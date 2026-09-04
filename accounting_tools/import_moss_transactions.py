#!/usr/bin/env -S uv run
"""Import the Moss exports into the unified three-level model
(moss_transactions / moss_expenses / moss_bookings).

Replaces import_moss_card_transactions.py and import_moss_balance_movements.py.
Design: doc/plans/2026-08_moss-transaction-unification.md in the wagon.

THE THREE LEVELS AND WHERE THEY COME FROM
-----------------------------------------
  L1 moss_transactions  one Moss transaction (a card payment is one; an invoice,
     reimbursement or top-up has exactly one payment in Moss). Card rows come from
     the card export, everything else from the balance-movements export, whose
     "Linked Reimbursement/Invoice ID" decides the kind (neither set = a wallet
     top-up).
  L2 moss_expenses      one expense of a reimbursement (one per balance row).
     Card, invoice and top-up get exactly one SHELL row -- the transaction IS the
     expense, as in Moss's own model, where those kinds have no middle level --
     and nothing kind-specific is stored on it.
  L3 moss_bookings      one split -- the grain DATEV books at. Card splits come
     from the card export, reimbursement splits from the reimbursement export
     and invoice lines from the invoice export; only a top-up's single booking
     is taken from its balance row.

The detail exports are not decoration: balance-movements is LOSSY. It collapses
a reimbursement expense's internal split into ONE row -- so an expense that Moss
really booked across two expense accounts arrives as a single amount on one of
them, and DATEV cannot match it. It also carries no cost center for invoices and
reports a bogus conversion rate of 1.0 on foreign-currency payments.

DETAIL GATE (per transaction, never per file)
---------------------------------------------
A reimbursement/invoice transaction is imported only when its detail rows exist
and line up with the balance rows. If the four files have slightly different
data states, the INTERSECTION of fully covered transactions is imported and the
rest is skipped with a log line -- a partially exported transaction is never
guessed at, and a missing detail file never blocks the other kinds.

WHAT IS NEVER OVERWRITTEN
-------------------------
The app owns `comment`, `additional_info`, `contribution_subject_*`, `status`,
the DATEV / camt / recipient links with their `*_link_meta`, and
`manually_paid` / `manually_booked`. None of them appears in a diffed value set,
so a re-import cannot touch them (the plan is column-granular). `fin_account_id`
is written on INSERT only, `source_file` only on rows that change anyway.

KEYS
----
  L1 moss_transaction_uuid
  L2 (moss_transaction_uuid, expense_number)
  L3 booking_unique_item_number -- CONSTRUCTED, never the CSV "Unique Item
     Number", whose suffix is a file-position counter and therefore unstable:
     card/invoice/top-up "<transaction uuid>_<Sub-row Number>",
     reimbursement "<Unique Expense ID>_<Sub-row Number>". The raw Moss value is
     kept in other_moss_columns["Unique Item Number"].

Moss CSV format: ";" separated, "." as the decimal point, UTF-8 with a BOM --
the exact opposite of the DATEV Buchungsstapel (cp1252, German decimals). The
two must never share a number parser.

USAGE
    ./accounting_tools/import_moss_transactions.py \
        External_Data/Moss_Exports/transactions_*.csv \
        External_Data/Moss_Exports/balance-movements_*.csv \
        External_Data/Moss_Exports/reimbursements_*.csv \
        External_Data/Moss_Exports/invoices_*.csv
    ... --dry-run              plan only, write nothing
    ... --rollback-for-testing apply, then ROLLBACK
Idempotent: re-importing the same files reports nothing to write.
"""

from __future__ import annotations

import collections as _collections
import csv as _csv
import datetime as _datetime
import decimal as _decimal
import logging as _logging
import pathlib as _pathlib
import sys as _sys
import uuid as _uuid

import wsjrdp2027
from wsjrdp2027._internal.single_table_upsert_plan import SingleTableUpsertPlanBuilder


_LOGGER = _logging.getLogger(__name__)
_SELF_NAME = _pathlib.Path(__file__).stem

_CSV_DELIMITER = ";"
_CSV_ENCODING = "utf-8-sig"

_TABLE_TRANSACTIONS = "moss_transactions"
_TABLE_EXPENSES = "moss_expenses"
_TABLE_BOOKINGS = "moss_bookings"

#: Detail-export columns that describe the whole payment, not one expense --
#: constant per reimbursement / invoice -- and therefore land on the
#: transaction's other_moss_columns. The reimbursement export's Reimbursement
#: Name (Moss website: "Name") is also the column transaction_name; it is kept
#: here as well, by decision.
DETAIL_TX_OTHER: tuple[str, ...] = (
    "Supplier Vat ID",
    "Reimbursement Payment Status",
    "Reimbursement Name",
)

#: Which export a file is. Detected from its columns, never from its name.
_KIND_CARD = "card"
_KIND_BALANCE = "balance"
_KIND_REIMBURSEMENT = "reimbursement"
_KIND_INVOICE = "invoice"

#: A "Cost Carrier - Number" of only zeros means "no sphere".
_EMPTY_SPHERE = frozenset({"", "000", "0"})

#: The fin account the Moss transactions belong to (the Moss wallet). Set on
#: INSERT only, so a manual reassignment in the app survives a re-import.
_WALLET_TRANSACTION_TYPE = "MossBalanceMovement"

#: Where a row was read from. Provenance, not content -- see _apply().
_SOURCE_FILES: dict[tuple, str | None] = {}


# =========================================================== column mappings
# One block per export, in the shape the card importer established:
#   *_COLUMN_MAP     CSV header -> DB column (1:1, coerced by _coerce)
#   *_OTHER_*        mostly-empty or archival columns -> other_moss_columns UNDER
#                    THEIR CSV HEADER, stored only where the cell has a value;
#                    the models map the header to a snake_case accessor
#   *_MIRROR_*       every amount/currency/rate column, mirrored verbatim under
#                    its Moss name even when a house column holds the same
#                    value (source fidelity); an empty cell is mirrored as ""
#   IGNORED_*        present in the CSV, deliberately never stored

# --- card export (transactions_*.csv): one row per SPLIT --------------------
CARD_TX_COLUMN_MAP: dict[str, str] = {
    "Transaction ID": "moss_transaction_uuid",
    "Transaction State": "moss_transaction_state",
    "Transaction Type": "transaction_type",
    "Payment Date": "payment_date",
    "Booking Date": "booking_date",
    "Settlement Date": "settlement_date",
    "First Export Date": "first_export_date",
    "Last Export Date": "last_export_date",
    "Receipt Date": "receipt_date",
    "Service Date": "service_date",
    "Approval Date": "approval_date",
    "Total Amount": "signed_total_base_amount",
    "Total Original Amount": "signed_total_transaction_amount",
    "Home Currency": "currency",
    "Original Currency": "currency_original",
    "Fees Amount": "fees_amount",
    # "Transaction Amount Excluding Fees" is per SPLIT in the card export (per
    # transaction in the balance export), so the L1 total is summed over the
    # splits in _card_records().
    "Conversion Rate Including Fees": "conversion_rate_including_fees",
    "Supplier Account": "supplier_account_number",
    "Moss Balance Account": "moss_balance_account_number",
    "Cash in Transit Account": "cash_in_transit_account_number",
    "Cardholder": "card_holder_name",
    "Card Used": "card_used",
    "Card Purpose": "card_purpose",
    "Team Name": "card_holder_team_name",
    "Approver Name": "approver_name",
    "Post Spend Approval Status": "post_spend_approval_status",
    "Parent Booking Text": "transaction_posting_text",
    "Invoice Number": "invoice_number",
    # The merchant of the card payment. In Moss it is part of the header's
    # CardTransactionMetadata (merchantDetails): the card header IS the expense.
    "Merchant Name": "merchant_name",
    "Merchant City": "merchant_city",
    "Merchant Country": "merchant_country",
}
CARD_BOOKING_COLUMN_MAP: dict[str, str] = {
    "Home Amount": "signed_base_amount",
    "Original Amount": "signed_transaction_amount",
    "Account Number": "account_number",
    "Cost Center - Number": "cost_center_number",
    "Cost Carrier - Number": "sphere_number",
    "Distribution combination": "distribution_combination",
    "Note": "booking_posting_text",
}
#: Mostly-empty card columns -> other_moss_columns of the TRANSACTION, under
#: their CSV header, only when the cell has a value. MossTransaction names them
#: through its accessors. All of them are header facts in Moss: the card header
#: is the expense (CardTransactionMetadata), so the receipt file name, the
#: merchant as the card issuer knows it, the ticket number and the deferral
#: fields live on L1 as well; the L2 shell of a card payment stays empty.
CARD_OTHER_TX: tuple[str, ...] = (
    # the card holder's name as printed on the card (the column
    # card_holder_name is the CSV Cardholder) and the two constant labels
    "Card Holder Name",
    "Card Holder Label",
    "Card Label",
    # the payment
    "Reason for Purchase",
    "General Transaction Type",
    "Sage Payment Type",
    "Sage Transaction Type",
    "Supplier Vat ID",
    # the expense (= the header, in Moss)
    "Invoice File Name",
    "Card Acceptor Name",
    "Airline Ticket Number",
    "Is Prepayment?",
    "Number of Months in Release Plan",
    "Prepayment Start Date",
    "Prepayment End Date",
)
#: Per split (the card export is one row per split); the VAT fields are where
#: Moss keeps them, on the line.
CARD_OTHER_BOOKING: tuple[str, ...] = (
    "Unit Price",
    "Quantity",
    "VAT Code",
    "VAT Name",
    "VAT Rate",
    "Client Number",
)
CARD_MIRROR_TX: tuple[str, ...] = (
    "Total Amount",
    "Total Amount (excl. VAT)",
    "Total Original Amount",
    "Total Original Amount (excl. VAT)",
    "VAT Amount",
    "Original VAT",
    "Fees Amount",
    "Currency",
    "Home Currency",
    "Original Currency",
    "Conversion Rate",
    "Conversion Rate Including Fees",
)
CARD_MIRROR_BOOKING: tuple[str, ...] = (
    "Amount",
    "Amount (excl. VAT)",
    "Home Amount",
    "Original Amount",
    "Original Amount (excl. VAT)",
    "Transaction Amount Excluding Fees",
)
# Derivable, structural or redundant -- see the design doc, section 6.
IGNORED_CARD = {
    # the creditor's standing data (name, IBAN, BIC) live in
    # wsjrdp_personal_accounts, reached through Supplier Account; no snapshot
    "Supplier Name",
    "Supplier IBAN",
    "Supplier BIC",
    # the ledger account's name, keyed by the account number that is a column
    "Category",
    "Name of Expense Account",
    "Original Expense Account",  # undocumented; == Account Number on every row
    "Cost Center - Name",
    "Cost Carrier - Name",
    "Card Name",
    "Record Type",
    "Period",
    "Period Day",
    "Period Month (double-digit)",
    "Month end date",
    "Accounting Period",
    "CSV Line Type",
    "Row Number",
    "Transaction Ordinal",
    "Sub Item Row Number",  # the split index is Sub-row Number (design doc, section 5)
    "Merchant and Card Description",
    "Sub-row Number",  # builds the booking key
    "Unique Item Number",  # mirrored into other_moss_columns explicitly
    "Moss Record URL",  # derivable from the uuid; the model computes it
    "Moss Attachment URL",
    "Transaction ID PDF filename",
    # Soll/Haben direction, new in the WSJ27 export from 2026-08-30: redundant
    # with the signed amount and the generated debit_credit column.
    "Account Debit/Credit",
    "Account Debit/Credit Reverse",
}

# --- balance-movements export: one row per EXPENSE (reimbursement) / LINE ---
BALANCE_TX_COLUMN_MAP: dict[str, str] = {
    "Transaction ID": "moss_transaction_uuid",
    "Transaction State": "moss_transaction_state",
    "Transaction Type": "transaction_type",
    "Payment Date": "payment_date",
    "Booking Date": "booking_date",
    "First Export Date": "first_export_date",
    "Currency": "currency",
    "Original Currency": "currency_original",
    "Payment Fee": "payment_fee",
    "Fees Amount": "fees_amount",
    # Per TRANSACTION in this export, unlike the card export's per-split column of that name.
    "Transaction Amount Excluding Fees": "total_amount_excluding_fees",
    "Conversion Rate Including Fees": "conversion_rate_including_fees",
    "Supplier Account": "supplier_account_number",
    "Recipient Account Number": "recipient_iban",
    "Recipient Bank Code": "recipient_bic",
    "Moss Balance Account": "moss_balance_account_number",
    "Cash in Transit Account": "cash_in_transit_account_number",
    # the finance user who released the payout and that user's team; empty
    # on a top-up
    "Team Name": "payout_team_name",
    "Cardholder": "payout_user_name",
    # Both text columns are re-set from _payment_reference() afterwards: the
    # raw value carries our own organisation name as a suffix.
    "Payment Reference": "payment_reference",
    "Invoice Number": "invoice_number",
    "Linked Reimbursement ID": "moss_reimbursement_uuid",
    "Linked Invoice ID": "moss_invoice_uuid",
}
#: Constant per transaction.
BALANCE_OTHER_TX: tuple[str, ...] = (
    "Reason for Purchase",
    "Moss Attachment URL",
)
#: Per balance ROW -- per expense of a reimbursement, per line of an invoice --
#: so it lands on that row's level: the expense of a reimbursement, the booking
#: of an invoice or top-up. It is the ledger account's name (equal on every row
#: that has an account); a top-up's Category is the one value not derivable
#: from an account number.
BALANCE_OTHER_ROW: tuple[str, ...] = ("Category",)
#: Per balance row as well, but a BOOKING dimension (Moss's client code), so it
#: goes onto every booking of the row: the invoice line / top-up booking, each
#: split of a reimbursement expense. Empty on every row today.
BALANCE_OTHER_BOOKING: tuple[str, ...] = ("Client Number",)
#: The payment's own figures. The per-ROW amounts ("Amount", "Home Amount",
#: "Original Amount") are deliberately not mirrored here: they are the first
#: expense's, not the payment's, and feed the expense / booking columns.
BALANCE_MIRROR_TX: tuple[str, ...] = (
    "Currency",
    "Home Currency",
    "Original Currency",
    "Conversion Rate",
    "Conversion Rate Including Fees",
    "Fees Amount",
    "Payment Fee",
    "Transaction Amount Excluding Fees",
)
IGNORED_BALANCE = {
    "Supplier Name",  # standing data, see IGNORED_CARD
    "Record Type",
    "Name of Expense Account",  # the ledger account's name; the number is a column
    "Period",
    "CSV Line Type",
    "Sub-row Number",  # the expense index / part of the booking key
    "Unique Item Number",
    "Account Number",  # the booking account comes from the detail export
    "Amount",  # the row's amounts feed the expense / booking columns
    "Home Amount",
    "Original Amount",
    "Amount (excl. VAT)",
    "Original Amount (excl. VAT)",
    "Note",  # the booking text comes from the detail export
}

# --- reimbursement export: the L3 splits, one row per split ----------------
REIMBURSEMENT_EXPENSE_COLUMN_MAP: dict[str, str] = {
    "Expense Name": "expense_name",
    "Expense type": "moss_expense_type",
    "Purchased On": "purchased_on",
    "Parent Booking Text": "expense_posting_text",
}
#: The reimbursement's own header facts -> columns of the TRANSACTION (all
#: constant per reimbursement): Submitted On is Moss's expenseTime of
#: the reimbursement header and the reimbursement's DATEV Belegdatum anchor
#: (matched against datev_bookings.booking_date).
REIMBURSEMENT_TX_COLUMN_MAP: dict[str, str] = {
    "Submitted On": "submitted_on",
    # the claim's title (website label "Name"); its Buchungstext, the
    # Reimbursement Description, is set by _transaction_posting_text()
    "Reimbursement Name": "transaction_name",
    "Creation date": "created_in_moss_on",
}
REIMBURSEMENT_BOOKING_COLUMN_MAP: dict[str, str] = {
    "Expense Account": "account_number",
    # Moss mislabels the cost-center NUMBER as a name here. The numbers are
    # alphanumeric (3100, D5, ...), so nothing may constrain them to digits.
    "Cost Center - Name": "cost_center_number",
    "Cost Carrier - Number": "sphere_number",
    "Distribution combination": "distribution_combination",
    "Expense Description": "booking_posting_text",
}
#: Everything else the export carries per expense -> other_moss_columns, each
#: under its Moss header, only when the cell has a value (MossExpense names them
#: through its accessors).
REIMBURSEMENT_OTHER_EXPENSE: tuple[str, ...] = (
    "Attached File Name",
    # mileage claims
    "KM Expense Type",
    "Start Location",
    "Destination Location",
    "Travel Route",
    "Trip Type",
    "Trip Distance In Unit",
    "Reimbursable Distance In Unit",
    "Commute Deduction In Unit",
    "Distance Unit",
    "Vehicle Type",
)
REIMBURSEMENT_OTHER_BOOKING: tuple[str, ...] = (
    "VAT Code",
    "VAT Name",
    "VAT Rate",
)
REIMBURSEMENT_MIRROR_BOOKING: tuple[str, ...] = (
    "Amount",
    "Amount (excl. VAT)",
    "Amount in Original Currency",
    "VAT Amount",
)
IGNORED_REIMBURSEMENT = {
    "Record Type",
    "CSV Line Type",
    "Row Number",  # a file-global counter -- never stored (design doc, section 5)
    "Sub-row Number",  # the split index; part of the booking key
    "Sub Item Row Number",  # ignored entirely (design doc, section 5)
    "Reimbursement Ordinal",
    "Unique Item Number",  # mirrored into other_moss_columns explicitly
    "Unique Expense ID",
    "Unique Reimbursement ID",
    "Unique Reimbursement ID PDF filename",
    "Moss Record URL",
    "Moss Attachment URL",
    "Moss Balance Account",
    "Combined Description",
    "Cost Carrier - Name",
    "Payment Date",
    "Team Name",
    "Total Amount",  # the expense total; recomputed from its splits
    "Total Amount (excl. VAT)",
    "Total Amount in Original Currency",
    "Original Currency",  # the currency lives on L1
    "Conversion Rate",  # read directly for the L1 exchange_rate
    "Supplier IBAN",  # standing data, see IGNORED_CARD
    "Supplier BIC",
    "Supplier Name",
    "Approver Name",
    "Approval Date",
    "Supplier account",  # the creditor lives on L1 (balance export Supplier Account)
    "Name of Expense Account",  # the ledger account's name; the number is a column
    "Submitted By",  # read directly for the L1 submitted_by column
    "Reimbursement Name",  # read directly: the L1 transaction_name (+ kept as a key)
    # read directly for L1 other_moss_columns (constant per reimbursement)
    "Reimbursement Payment Status",
    "User IBAN",
    "User BIC",
}

# --- invoice export: the L3 lines, one row per line ------------------------
INVOICE_BOOKING_COLUMN_MAP: dict[str, str] = {
    "Expense Account - Number": "account_number",
    "Cost Center - Number": "cost_center_number",
    "Cost Carrier - Number": "sphere_number",
    "Distribution combination": "distribution_combination",
    "Booking Text": "booking_posting_text",
}
#: The invoice's own facts -> columns of the TRANSACTION: in Moss the invoice
#: header is the expense (InvoiceMetadata: due / net due / delivery date,
#: payment status, terms, approvers; expenseTime = the invoice date), so the L2
#: shell of an invoice stays empty. Read from the first line of the invoice
#: export (all constant per invoice).
INVOICE_TX_COLUMN_MAP: dict[str, str] = {
    # the Buchungstext of the invoice (Moss: Expense.bookingText); the balance
    # export's Payment Reference is the SEPA Verwendungszweck and stays in
    # payment_reference (never equal on an invoice)
    "Parent Booking Text": "transaction_posting_text",
    "Delivery Date": "delivery_date",
    # Invoice Date is the invoice's DATEV Belegdatum anchor (matched against
    # datev_bookings.booking_date),
    # read through MossInvoice#datev_date_anchor.
    "Invoice Date": "invoice_date",
    # the invoice's workflow status (Moss Expense.status, e.g. Completed); the
    # payment state of every kind is moss_transaction_state (Transaction State)
    "Invoice Status": "invoice_status",
    "Due Date": "due_date",
    "Submitted Date": "submitted_date",
}
#: Mostly-empty invoice columns -> other_moss_columns of the TRANSACTION, under
#: their CSV header, only when the cell has a value (MossTransaction names
#: them). The shared names with the card export (Invoice File Name, Is
#: Prepayment?, release plan) mean the same there.
INVOICE_OTHER_TX: tuple[str, ...] = (
    "Net Due Date",
    "Invoice Payment Status",
    "General Invoice Type",
    "Is Prepayment?",
    "Prepayment Start Date",
    "Prepayment End Date",
    "Number of Months in Release Plan",
    "Payment term - Description",
    "Payment term - Number",
    "Discount 1 percentage",
    "Discount 1 due date",
    "Discount 2 percentage",
    "Discount 2 due date",
    "Invoice File Name",
    "Reviewed by",
    "Last reviewed",
    "Verified By Name",
    "Verifier Names",
    "Sage Transaction Type",
)
INVOICE_OTHER_BOOKING: tuple[str, ...] = (
    "Unit Price",
    "Quantity",
    "VAT Code",
    "VAT Name",
    "VAT Rate",
)
#: The line's amounts. Currency, Home Currency and Conversion Rate are facts
#: of the one payment and live on L1 (columns and the balance export's
#: mirrors); the invoice export's real rate feeds `exchange_rate` directly.
INVOICE_MIRROR_BOOKING: tuple[str, ...] = (
    "Amount",
    "Amount in Home Currency",
    "Net Amount",
    "Net Amount in Home Currency",
    "Net Amount Negated",
    "VAT Amount",
    "VAT Amount in Home Currency",
)
IGNORED_INVOICE = {
    "Record Type",
    "Accounting Period",
    "Row Number",
    "Sub-row Number",
    "Sub Item Row Number",
    "Ordinal",
    "Invoice ID",  # read directly for moss_expense_uuid
    "Invoice Number",  # the invoice number lives on L1
    "Supplier IBAN",  # standing data, see IGNORED_CARD
    "Supplier BIC",
    "Supplier Vat ID",  # read directly for L1 other_moss_columns["Supplier Vat ID"]
    "Approver Name",  # read directly for the L1 approver_name / approval_date
    "Approval Date",
    "Submitted By",  # read directly for the L1 submitted_by column
    "Currency",  # the payment's currency and rate live on L1
    "Home Currency",
    "Conversion Rate",  # read directly for the L1 exchange_rate
    # the ledger account's name; the number is a column
    "Expense Account - Name",
    "Original Expense Account",  # undocumented; == the account number on every line
    "PO Number",  # read directly for the L1 po_number / pr_number columns
    "PR Number",
    "Last Export Date",  # read directly for the L1 last_export_date column
    # == the balance export's Supplier Account on every imported invoice, i.e.
    # the L1 supplier_account_number; not stored a second time.
    "Supplier Number",
    "Moss Record URL",
    "Moss Attachment URL",
    "Moss Balance Account",
    "Cost Center - Name",
    "Cost Carrier - Name",
    "Category",
    "Supplier Name",  # standing data, see IGNORED_CARD
    "Team Name",
    "Payment Date",
    "Total Amount",
    "Total Amount in Home Currency",
    "Total Net Amount",
    "Total Net Amount in Home Currency",
}

#: Coercion by DB column (the unified schema; see the migration).
_DATE_COLS = frozenset(
    {
        "payment_date",
        "booking_date",
        "settlement_date",
        "first_export_date",
        "last_export_date",
        "receipt_date",
        "service_date",
        "approval_date",
        "purchased_on",
        "submitted_on",
        "delivery_date",
        "invoice_date",
        "due_date",
        "submitted_date",
        "created_in_moss_on",
    }
)
_DECIMAL_COLS = frozenset(
    {
        "signed_total_base_amount",
        "signed_total_transaction_amount",
        "signed_expense_base_amount",
        "signed_expense_transaction_amount",
        "signed_base_amount",
        "signed_transaction_amount",
        "exchange_rate",
        "payment_fee",
        "fees_amount",
        "total_amount_excluding_fees",
        "conversion_rate_including_fees",
    }
)
_INT_COLS = frozenset({"expense_number"})
#: Postgres `uuid` columns. They must travel as uuid.UUID, not as text: the
#: comparison in the plan's key lookup is typed, and `uuid = text` has no
#: operator in Postgres.
_UUID_COLS = frozenset(
    {
        "moss_transaction_uuid",
        "moss_expense_uuid",
        "moss_reimbursement_uuid",
        "moss_invoice_uuid",
    }
)
#: NOT NULL string columns: an empty CSV cell becomes "", never NULL.
_NOT_NULL_STR_COLS = frozenset({"transaction_posting_text", "booking_posting_text"})


# ================================================================== helpers


def _coerce(column: str, raw):
    """One CSV cell -> the value for its DB column.

    >>> _coerce("payment_date", "2026-05-29")
    datetime.date(2026, 5, 29)
    >>> _coerce("signed_base_amount", "-8.48")
    Decimal('-8.48')
    >>> _coerce("booking_posting_text", "") == ""
    True
    >>> _coerce("recipient_name", "  ") is None
    True
    >>> _coerce("moss_invoice_uuid", "abcdef01-2345-6789-abcd-ef0123456790")
    UUID('abcdef01-2345-6789-abcd-ef0123456790')
    """
    value = (raw or "").strip() if isinstance(raw, str) else raw
    if value in (None, ""):
        return "" if column in _NOT_NULL_STR_COLS else None
    if column in _DATE_COLS:
        return _parse_date(value)
    if column in _DECIMAL_COLS:
        return _decimal.Decimal(str(value))
    if column in _INT_COLS:
        return int(value)
    if column in _UUID_COLS:
        return _as_uuid(value)
    return value


def _as_uuid(value) -> _uuid.UUID | None:
    if value in (None, "") or isinstance(value, _uuid.UUID):
        return value or None
    return _uuid.UUID(str(value))


def _parse_date(raw) -> _datetime.date | None:
    text = (raw or "").strip()[:10]
    if not text:
        return None
    for pattern in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return _datetime.datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"unparseable Moss date {raw!r}")


def _mapped(row: dict, column_map: dict[str, str]) -> dict:
    return {
        column: _coerce(column, row.get(header, ""))
        for header, column in column_map.items()
    }


def _verbatim(row: dict, headers: tuple[str, ...]) -> dict:
    """Mostly-empty columns kept under their CSV header, only where the cell
    has a value. Every other_moss_columns key is a CSV header; the models map
    them to snake_case accessors."""
    return {
        h: value.strip() for h in headers if (value := row.get(h, "")) and value.strip()
    }


def _mirror(row: dict, headers: tuple[str, ...]) -> dict:
    """Amount/currency/rate columns, verbatim under their Moss name. An empty
    cell IS stored as "" -- that the field was empty is itself information."""
    return {header: (row.get(header) or "").strip() for header in headers}


def _text(row: dict, header: str) -> str | None:
    value = (row.get(header) or "").strip()
    return value or None


def _decimal_or_none(raw) -> _decimal.Decimal | None:
    text = (raw or "").strip() if isinstance(raw, str) else raw
    if text in (None, ""):
        return None
    return _decimal.Decimal(str(text))


def _int_or_zero(raw) -> int:
    return int((raw or "0").strip() or 0)


def _account_kind(number) -> str | None:
    """Kontenart of a (2026 chart) account number. NULL-safe, because a top-up
    booking has no expense account at all."""
    if not number:
        return None
    return wsjrdp2027.datev.account_kind_for_account_number(str(number))


def _sphere(raw) -> str | None:
    value = (raw or "").strip()
    return None if value in _EMPTY_SPHERE else value


def _payment_reference(row: dict) -> str | None:
    """The Verwendungszweck of the outgoing payment, house-normalised: Moss
    appends our own organisation name (often truncated mid-word) to every
    reference, which says nothing in our own books."""
    return wsjrdp2027.moss.normalize_payment_reference(_text(row, "Payment Reference"))


def _transaction_posting_text(kind: str, head: dict, detail) -> str:
    """The Buchungstext of the payment (website label "Buchungstext"; Moss:
    Expense.bookingText): the reimbursement export's Reimbursement Description
    (often empty, which is then an empty text -- the claim's
    title is transaction_name), the invoice export's Parent Booking Text; a
    top-up has none. The balance export's Payment Reference is the SEPA
    Verwendungszweck and stays in payment_reference; it is used here only
    while a detail export is missing, which the detail gate prevents for the
    imported kinds."""
    header = {
        "MossReimbursement": "Reimbursement Description",
        "MossInvoice": "Parent Booking Text",
    }.get(kind)
    if header and detail:
        return _text(_detail_head(kind, detail), header) or ""
    return _payment_reference(head) or ""


def _payee_name(reason_for_purchase: str | None) -> str | None:
    """Reimbursement rows carry the payee as "<Name>; ; -" in Reason for
    Purchase (the recipient name from the SEPA data of the Moss profile).

    >>> _payee_name("Alexander Beispiel; ; -")
    'Alexander Beispiel'
    >>> _payee_name("") is None
    True
    """
    if not reason_for_purchase:
        return None
    return reason_for_purchase.split(";")[0].strip() or None


def _sum(rows, header: str) -> _decimal.Decimal:
    total = _decimal.Decimal(0)
    for row in rows:
        total += _decimal_or_none(row.get(header)) or _decimal.Decimal(0)
    return total


def _sum_or_none(rows, header: str) -> _decimal.Decimal | None:
    """Like _sum, but None when no row carries a value -- for a column that is
    optional in the export and NULLable in the table.

    >>> _sum_or_none([{"x": "1.50"}, {"x": ""}, {"x": "2"}], "x")
    Decimal('3.50')
    >>> _sum_or_none([{"x": ""}], "x") is None
    True
    """
    values = [_decimal_or_none(row.get(header)) for row in rows]
    if all(value is None for value in values):
        return None
    return sum((value for value in values if value is not None), _decimal.Decimal(0))


def _group(
    rows: list[dict], key_header: str, order_header: str
) -> dict[str, list[dict]]:
    """Rows by their key column, each group ordered by a numeric column."""
    grouped: dict[str, list[dict]] = _collections.defaultdict(list)
    for row in rows:
        key = (row.get(key_header) or "").strip()
        if key:
            grouped[key].append(row)
    for group in grouped.values():
        group.sort(key=lambda row: _int_or_zero(row.get(order_header)))
    return dict(grouped)


# ===================================================================== input


def _read_csv(path: _pathlib.Path) -> list[dict[str, str]]:
    with path.open(encoding=_CSV_ENCODING, newline="") as handle:
        return list(_csv.DictReader(handle, delimiter=_CSV_DELIMITER))


def _detect_kind(headers: list[str], source: str = "") -> str:
    """Which Moss export this is, from its columns; `source` names the file in the error.

    >>> _detect_kind(["Unique Reimbursement ID", "Amount"])
    'reimbursement'
    >>> _detect_kind(["Invoice ID", "Amount"])
    'invoice'
    >>> _detect_kind(["Linked Reimbursement ID", "Amount"])
    'balance'
    >>> _detect_kind(["Merchant Name", "Home Amount"])
    'card'
    """
    header_set = set(headers)
    if "Unique Reimbursement ID" in header_set:
        return _KIND_REIMBURSEMENT
    if "Invoice ID" in header_set:
        return _KIND_INVOICE
    if "Linked Reimbursement ID" in header_set:
        return _KIND_BALANCE
    if "Merchant Name" in header_set:
        return _KIND_CARD
    where = f"{source}: " if source else ""
    raise SystemExit(
        f"{where}unrecognised Moss export -- none of the four Moss CSV layouts "
        f"(columns: {sorted(header_set)[:8]} ...)"
    )


_KNOWN_COLUMNS: dict[str, set[str]] = {
    _KIND_CARD: {
        *CARD_TX_COLUMN_MAP,
        *CARD_BOOKING_COLUMN_MAP,
        *CARD_OTHER_TX,
        *CARD_OTHER_BOOKING,
        *CARD_MIRROR_TX,
        *CARD_MIRROR_BOOKING,
        *IGNORED_CARD,
    },
    _KIND_BALANCE: {
        *BALANCE_TX_COLUMN_MAP,
        *BALANCE_OTHER_TX,
        *BALANCE_OTHER_ROW,
        *BALANCE_OTHER_BOOKING,
        *BALANCE_MIRROR_TX,
        *IGNORED_BALANCE,
    },
    _KIND_REIMBURSEMENT: {
        *REIMBURSEMENT_TX_COLUMN_MAP,
        *REIMBURSEMENT_EXPENSE_COLUMN_MAP,
        *REIMBURSEMENT_BOOKING_COLUMN_MAP,
        *REIMBURSEMENT_OTHER_EXPENSE,
        *REIMBURSEMENT_OTHER_BOOKING,
        *REIMBURSEMENT_MIRROR_BOOKING,
        *IGNORED_REIMBURSEMENT,
    },
    _KIND_INVOICE: {
        *INVOICE_BOOKING_COLUMN_MAP,
        *INVOICE_TX_COLUMN_MAP,
        *INVOICE_OTHER_TX,
        *INVOICE_OTHER_BOOKING,
        *INVOICE_MIRROR_BOOKING,
        *IGNORED_INVOICE,
    },
}


def _report_unknown_columns(kind: str, headers: list[str], name: str) -> None:
    """Warn about columns no map mentions. Not fatal -- only classified columns
    are ever read, so an unknown one is simply ignored; but a NEW Moss column is
    worth knowing about."""
    unknown = [h for h in dict.fromkeys(headers) if h and h not in _KNOWN_COLUMNS[kind]]
    if unknown:
        _LOGGER.warning(
            "%s: unclassified CSV column(s), ignored: %s", name, ", ".join(unknown)
        )


def _read_all(paths: list[str]) -> dict[str, list[dict]]:
    by_kind: dict[str, list[dict]] = {}
    for raw_path in paths:
        path = _pathlib.Path(raw_path)
        rows = _read_csv(path)
        if not rows:
            _LOGGER.warning("%s: empty export, skipped.", path.name)
            continue
        kind = _detect_kind(list(rows[0]), source=str(path))
        _report_unknown_columns(kind, list(rows[0]), path.name)
        for row in rows:
            row["__source_file__"] = path.name
        by_kind.setdefault(kind, []).extend(rows)
        _LOGGER.info("%s: %d rows (%s export).", path.name, len(rows), kind)
    return by_kind


# ========================================================== record building
# Each builder appends to (transactions, expenses, bookings). A booking carries
# the private key "_expense_number" naming its parent expense; it is stripped
# again before planning.


def _card_records(rows: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """The card export: a card payment IS its expense, so every transaction gets
    exactly one SHELL expense (nothing kind-specific on it) and one booking per
    CSV row (each row is one split)."""
    transactions: list[dict] = []
    expenses: list[dict] = []
    bookings: list[dict] = []
    for key, split_rows in _group(rows, "Transaction ID", "Sub-row Number").items():
        uuid = _as_uuid(key)
        head = split_rows[0]
        transaction = _mapped(head, CARD_TX_COLUMN_MAP)
        transaction.update(
            type="MossCardTransaction",
            supplier_account_kind=_account_kind(transaction["supplier_account_number"]),
            moss_balance_account_kind=_account_kind(
                transaction["moss_balance_account_number"]
            ),
            cash_in_transit_account_kind=_account_kind(
                transaction["cash_in_transit_account_number"]
            ),
            exchange_rate=_foreign_rate(head, transaction),
            # Per split in this export (per transaction in the balance one).
            total_amount_excluding_fees=_sum_or_none(
                split_rows, "Transaction Amount Excluding Fees"
            ),
            other_moss_columns=_verbatim(head, CARD_OTHER_TX)
            | _mirror(head, CARD_MIRROR_TX),
        )
        transactions.append(transaction)
        expenses.append(
            {
                "moss_transaction_uuid": uuid,
                "expense_number": 1,
                "type": "MossCardTransactionExpense",
                # The card export has no expense id of its own.
                "moss_expense_uuid": uuid,
                "signed_expense_base_amount": _sum(split_rows, "Home Amount"),
                "signed_expense_transaction_amount": _sum(
                    split_rows, "Original Amount"
                ),
                # A shell: merchant, receipt file and dates are transaction
                # columns / keys, as in Moss's card header.
            }
        )
        for split in split_rows:
            booking = _mapped(split, CARD_BOOKING_COLUMN_MAP)
            booking.update(
                booking_unique_item_number=f"{uuid}_{_text(split, 'Sub-row Number')}",
                moss_transaction_uuid=uuid,
                _expense_number=1,
                account_kind=_account_kind(booking["account_number"]),
                sphere_number=_sphere(booking["sphere_number"]),
                other_moss_columns=_verbatim(split, CARD_OTHER_BOOKING)
                | _mirror(split, CARD_MIRROR_BOOKING)
                | _mirror(split, ("Unique Item Number",)),
            )
            bookings.append(booking)
    return transactions, expenses, bookings


def _balance_records(
    rows: list[dict],
    reimbursements: dict[str, list[dict]],
    invoices: dict[str, list[dict]],
) -> tuple[list[dict], list[dict], list[dict], list[str]]:
    """The balance-movements export plus its two detail exports.

    Applies the per-transaction detail gate: a reimbursement or invoice whose
    detail rows are missing or do not line up is skipped entirely (and
    reported), never guessed at."""
    transactions: list[dict] = []
    expenses: list[dict] = []
    bookings: list[dict] = []
    skipped: list[str] = []
    for key, balance_rows in _group(rows, "Transaction ID", "Sub-row Number").items():
        uuid = _as_uuid(key)
        head = balance_rows[0]
        kind = _balance_kind(head)

        detail: list[dict] | None = None
        if kind != "MossTopUp":
            reimbursing = kind == "MossReimbursement"
            link = "Linked Reimbursement ID" if reimbursing else "Linked Invoice ID"
            detail_uuid = _text(head, link) or ""
            detail = (reimbursements if reimbursing else invoices).get(detail_uuid)
            reason = _detail_gate(kind, balance_rows, detail, detail_uuid)
            if reason:
                skipped.append(f"{uuid} ({kind}): {reason}")
                continue

        transaction = _mapped(head, BALANCE_TX_COLUMN_MAP)
        transaction.update(
            type=kind,
            # The balance export has no transaction-total column. `Amount` is
            # what left the wallet (fees included), so it is the base amount;
            # `Home Amount` can differ on a foreign-currency payment and is
            # mirrored into other_moss_columns instead.
            signed_total_base_amount=_sum(balance_rows, "Amount"),
            signed_total_transaction_amount=_sum(balance_rows, "Original Amount"),
            supplier_account_kind=_account_kind(transaction["supplier_account_number"]),
            moss_balance_account_kind=_account_kind(
                transaction["moss_balance_account_number"]
            ),
            cash_in_transit_account_kind=_account_kind(
                transaction["cash_in_transit_account_number"]
            ),
            # The balance export reports a bogus 1.0; the real rate is in the
            # detail export (design doc, section 4).
            exchange_rate=_detail_exchange_rate(kind, detail, transaction),
            # Reason for Purchase of a balance row is kind-specific: on a
            # reimbursement and an invoice it names the account holder paid
            # ("<Name>; ; -": the recipient from the SEPA data of the Moss
            # profile, or what the finance team typed for the transfer), on a
            # top-up it names the funding account (top_up_sender). The raw
            # key is kept on every kind.
            recipient_name=(
                _payee_name(_text(head, "Reason for Purchase"))
                if kind in ("MossReimbursement", "MossInvoice")
                else None
            ),
            # "<organisation> - <IBAN>", cut off by Moss after 60 characters;
            # kept verbatim.
            top_up_sender=(
                _text(head, "Reason for Purchase") if kind == "MossTopUp" else None
            ),
            approver_name=_text(_detail_head(kind, detail), "Approver Name"),
            approval_date=_parse_date(_detail_head(kind, detail).get("Approval Date")),
            po_number=_text(_detail_head(kind, detail), "PO Number"),
            pr_number=_text(_detail_head(kind, detail), "PR Number"),
            # Who created the claim / uploaded the invoice: a different person
            # from the payee on a third of the reimbursements.
            submitted_by=_text(_detail_head(kind, detail), "Submitted By"),
            # The balance export knows only the FIRST export date; the invoice
            # export carries the last one (constant per invoice).
            last_export_date=_parse_date(
                _detail_head(kind, detail).get("Last Export Date")
            ),
            payment_reference=_payment_reference(head),
            transaction_posting_text=_transaction_posting_text(kind, head, detail),
            other_moss_columns=_verbatim(head, BALANCE_OTHER_TX)
            | _mirror(head, BALANCE_MIRROR_TX)
            # the detail export's facts about the whole payment (constant per
            # reimbursement / invoice), kept here rather than on each expense
            | _verbatim(_detail_head(kind, detail), DETAIL_TX_OTHER)
            | _verbatim(_detail_head(kind, detail), INVOICE_OTHER_TX),
        )
        # The detail export's header facts of the kind (dates, texts); the
        # other kinds' columns stay NULL, so the update touches nothing there.
        if kind == "MossInvoice":
            transaction.update(
                _mapped(_detail_head(kind, detail), INVOICE_TX_COLUMN_MAP)
            )
        elif kind == "MossReimbursement":
            transaction.update(
                _mapped(_detail_head(kind, detail), REIMBURSEMENT_TX_COLUMN_MAP)
            )
        transactions.append(transaction)

        if kind == "MossReimbursement":
            _reimbursement_levels(uuid, balance_rows, detail, expenses, bookings)
        elif kind == "MossInvoice":
            _invoice_levels(uuid, balance_rows, detail, expenses, bookings)
        else:
            _top_up_levels(uuid, balance_rows, expenses, bookings)
    return transactions, expenses, bookings, skipped


def _balance_kind(row: dict) -> str:
    """A balance row is a reimbursement or an invoice when it links to one; with
    neither link it is a wallet top-up (positive, no expense account).

    >>> _balance_kind({"Linked Reimbursement ID": "x"})
    'MossReimbursement'
    >>> _balance_kind({"Linked Invoice ID": "x"})
    'MossInvoice'
    >>> _balance_kind({})
    'MossTopUp'
    """
    if _text(row, "Linked Reimbursement ID"):
        return "MossReimbursement"
    if _text(row, "Linked Invoice ID"):
        return "MossInvoice"
    return "MossTopUp"


def _detail_gate(kind, balance_rows, detail, detail_uuid) -> str | None:
    """Why this transaction cannot be imported, or None when it can."""
    if not detail:
        return f"{detail_uuid} has no rows in the detail export"
    if len(detail) != len(balance_rows):
        item = "expense" if kind == "MossReimbursement" else "line"
        return (
            f"{len(balance_rows)} balance row(s) but {len(detail)} detail "
            f"{item}(s) in {detail_uuid}"
        )
    if kind != "MossReimbursement":
        # An invoice line's detail amount is in the INVOICE currency, so only
        # the reimbursement side can be compared to the cent here; the invoice
        # amounts are guarded by the sum invariant instead.
        return None
    for balance, expense in zip(balance_rows, detail):
        if abs(_decimal_or_none(balance.get("Amount")) or 0) != abs(
            _sum(expense["rows"], "Amount")
        ):
            return f"amounts do not line up with the balance rows in {detail_uuid}"
    return None


def _reimbursement_levels(uuid, balance_rows, detail, expenses, bookings) -> None:
    """One expense per balance row, its bookings being the reimbursement's
    splits. The two sides correspond 1:1 IN ORDER -- the
    balance row carries no expense id to join on."""
    for balance, expense in zip(balance_rows, detail):
        expense_number = _int_or_zero(balance.get("Sub-row Number"))
        head = expense["rows"][0]
        # The balance row's sign is authoritative: the detail export reports
        # unsigned split amounts.
        sign = -1 if (_decimal_or_none(balance.get("Amount")) or 0) < 0 else 1
        record = _mapped(head, REIMBURSEMENT_EXPENSE_COLUMN_MAP)
        record.update(
            moss_transaction_uuid=uuid,
            expense_number=expense_number,
            type="MossReimbursementExpense",
            moss_expense_uuid=_as_uuid(expense["uuid"]),
            signed_expense_base_amount=_coerce(
                "signed_expense_base_amount", balance.get("Amount")
            ),
            signed_expense_transaction_amount=(
                sign * _sum(expense["rows"], "Amount in Original Currency")
            ),
            other_moss_columns=_verbatim(head, REIMBURSEMENT_OTHER_EXPENSE)
            | _verbatim(balance, BALANCE_OTHER_ROW),
        )
        expenses.append(record)
        for split in expense["rows"]:
            booking = _mapped(split, REIMBURSEMENT_BOOKING_COLUMN_MAP)
            amount = _decimal_or_none(split.get("Amount"))
            original = _decimal_or_none(split.get("Amount in Original Currency"))
            booking.update(
                booking_unique_item_number=(
                    f"{expense['uuid']}_{_text(split, 'Sub-row Number')}"
                ),
                moss_transaction_uuid=uuid,
                _expense_number=expense_number,
                signed_base_amount=None if amount is None else sign * abs(amount),
                signed_transaction_amount=(
                    None if original is None else sign * abs(original)
                ),
                account_kind=_account_kind(booking["account_number"]),
                sphere_number=_sphere(booking["sphere_number"]),
                other_moss_columns=_verbatim(split, REIMBURSEMENT_OTHER_BOOKING)
                | _mirror(split, REIMBURSEMENT_MIRROR_BOOKING)
                | _mirror(split, ("Unique Item Number",))
                | _verbatim(balance, BALANCE_OTHER_BOOKING),
            )
            bookings.append(booking)


def _invoice_levels(uuid, balance_rows, detail, expenses, bookings) -> None:
    """The invoice IS the expense: ONE shell expense, one booking per line. Balance
    rows and invoice lines correspond 1:1 in order, which is
    what gives an invoice line its cost center -- balance-movements has no
    cost-center column at all."""
    head = detail[0]
    # A shell: the invoice's dates, texts and terms are transaction columns /
    # keys, as in Moss's invoice header.
    expenses.append(
        {
            "moss_transaction_uuid": uuid,
            "expense_number": 1,
            "type": "MossInvoiceExpense",
            "moss_expense_uuid": _as_uuid(_text(head, "Invoice ID")),
            "signed_expense_base_amount": _sum(balance_rows, "Amount"),
            "signed_expense_transaction_amount": _sum(balance_rows, "Original Amount"),
        }
    )
    for balance, line in zip(balance_rows, detail):
        booking = _mapped(line, INVOICE_BOOKING_COLUMN_MAP)
        booking.update(
            # The key follows the balance row (design doc, section 5); that row's raw
            # Unique Item Number is kept in other_moss_columns.
            booking_unique_item_number=f"{uuid}_{_text(balance, 'Sub-row Number')}",
            moss_transaction_uuid=uuid,
            _expense_number=1,
            # The paid EUR comes from the balance row, the foreign amount from
            # the invoice: on a PLN invoice each side computes its own EUR.
            signed_base_amount=_coerce("signed_base_amount", balance.get("Amount")),
            signed_transaction_amount=_coerce(
                "signed_transaction_amount", balance.get("Original Amount")
            ),
            account_kind=_account_kind(booking["account_number"]),
            sphere_number=_sphere(booking["sphere_number"]),
            other_moss_columns=_verbatim(line, INVOICE_OTHER_BOOKING)
            | _mirror(line, INVOICE_MIRROR_BOOKING)
            | _mirror(balance, ("Unique Item Number",))
            | _verbatim(balance, BALANCE_OTHER_ROW)
            | _verbatim(balance, BALANCE_OTHER_BOOKING),
        )
        bookings.append(booking)


def _top_up_levels(uuid, balance_rows, expenses, bookings) -> None:
    """A wallet top-up has no expense and no expense account, but still gets one
    expense and one booking, so the sum invariant holds for every kind."""
    expenses.append(
        {
            "moss_transaction_uuid": uuid,
            "expense_number": 1,
            "type": "MossTopUpExpense",
            "moss_expense_uuid": uuid,
            "signed_expense_base_amount": _sum(balance_rows, "Amount"),
            "signed_expense_transaction_amount": _sum(balance_rows, "Original Amount"),
        }
    )
    for row in balance_rows:
        bookings.append(
            {
                "booking_unique_item_number": f"{uuid}_{_text(row, 'Sub-row Number')}",
                "moss_transaction_uuid": uuid,
                "_expense_number": 1,
                # Money INTO the wallet: positive, and touching only the wallet
                # (36100) and transit (13720) accounts.
                "signed_base_amount": _coerce("signed_base_amount", row.get("Amount")),
                "signed_transaction_amount": _coerce(
                    "signed_transaction_amount", row.get("Original Amount")
                ),
                "account_number": None,
                "account_kind": None,
                "booking_posting_text": _coerce(
                    "booking_posting_text", row.get("Note")
                ),
                "other_moss_columns": _mirror(
                    row, ("Unique Item Number", "Amount (excl. VAT)")
                )
                | _verbatim(row, BALANCE_OTHER_ROW)
                | _verbatim(row, BALANCE_OTHER_BOOKING),
            }
        )


def _reimbursement_expenses(rows: list[dict]) -> dict[str, list[dict]]:
    """reimbursement uuid -> its expenses in file order, each with its splits."""
    grouped: dict[str, list[dict]] = _collections.defaultdict(list)
    seen: dict[tuple[str, str], dict] = {}
    for row in sorted(rows, key=lambda r: _int_or_zero(r.get("Row Number"))):
        reimbursement = (row.get("Unique Reimbursement ID") or "").strip()
        expense_uuid = (row.get("Unique Expense ID") or "").strip()
        if not reimbursement or not expense_uuid:
            continue
        key = (reimbursement, expense_uuid)
        if key not in seen:
            seen[key] = {"uuid": expense_uuid, "rows": []}
            grouped[reimbursement].append(seen[key])
        seen[key]["rows"].append(row)
    for expenses in grouped.values():
        for expense in expenses:
            expense["rows"].sort(key=lambda r: _int_or_zero(r.get("Sub-row Number")))
    return dict(grouped)


def _invoice_lines(rows: list[dict]) -> dict[str, list[dict]]:
    """invoice uuid -> its lines in file order (the invoice's bookings)."""
    return _group(rows, "Invoice ID", "Row Number")


def _foreign_rate(row: dict, transaction: dict):
    """The exchange rate, but only where the payment really is in a foreign
    currency -- Moss reports 1.0 on every EUR row, which says nothing."""
    if transaction.get("currency_original") in (None, transaction.get("currency")):
        return None
    return _decimal_or_none(row.get("Conversion Rate"))


def _detail_head(kind: str, detail) -> dict:
    """The first detail row of a transaction -- an invoice's first line, a
    reimbursement's first split -- which carries everything that is constant
    per transaction (rate, supplier bank details). Empty for a top-up."""
    if kind == "MossTopUp" or not detail:
        return {}
    return detail[0] if kind == "MossInvoice" else detail[0]["rows"][0]


def _detail_exchange_rate(kind: str, detail, transaction: dict):
    """The real FX rate, taken from the detail export (invoice line /
    reimbursement split); NULL when the transaction is not foreign."""
    head = _detail_head(kind, detail)
    return _foreign_rate(head, transaction) if head else None


def _build_records(by_kind: dict[str, list[dict]]):
    transactions: list[dict] = []
    expenses: list[dict] = []
    bookings: list[dict] = []

    if _KIND_CARD in by_kind:
        card = _card_records(by_kind[_KIND_CARD])
        _LOGGER.info(
            "card: %d transactions, %d expenses, %d bookings.", *map(len, card)
        )
        for target, produced in zip((transactions, expenses, bookings), card):
            target.extend(produced)

    if _KIND_BALANCE in by_kind:
        *balance, skipped = _balance_records(
            by_kind[_KIND_BALANCE],
            _reimbursement_expenses(by_kind.get(_KIND_REIMBURSEMENT, [])),
            _invoice_lines(by_kind.get(_KIND_INVOICE, [])),
        )
        _LOGGER.info(
            "balance: %d transactions, %d expenses, %d bookings.", *map(len, balance)
        )
        for target, produced in zip((transactions, expenses, bookings), balance):
            target.extend(produced)
        for reason in skipped:
            _LOGGER.warning("detail gate -- SKIPPED %s", reason)
        if skipped:
            _LOGGER.warning(
                "%d transaction(s) skipped by the detail gate; everything else is imported.",
                len(skipped),
            )
    return transactions, expenses, bookings


# ===================================================== invariant & planning


def _verify_sum_invariant(transactions, expenses, bookings) -> None:
    """transaction total == SUM(its expenses) == SUM(its bookings), checked
    before anything is written. The importer never INTRODUCES an inconsistency,
    even though it tolerates one that other actions left behind."""
    expense_sums: dict[str, _decimal.Decimal] = _collections.defaultdict(
        _decimal.Decimal
    )
    for row in expenses:
        expense_sums[row["moss_transaction_uuid"]] += (
            row["signed_expense_base_amount"] or 0
        )
    booking_sums: dict[str, _decimal.Decimal] = _collections.defaultdict(
        _decimal.Decimal
    )
    for row in bookings:
        booking_sums[row["moss_transaction_uuid"]] += row["signed_base_amount"] or 0

    broken = [
        row
        for row in transactions
        if row["signed_total_base_amount"] != expense_sums[row["moss_transaction_uuid"]]
        or row["signed_total_base_amount"] != booking_sums[row["moss_transaction_uuid"]]
    ]
    if broken:
        for row in broken[:10]:
            uuid = row["moss_transaction_uuid"]
            _LOGGER.error(
                "sum invariant: %s total=%s expenses=%s bookings=%s",
                uuid,
                row["signed_total_base_amount"],
                expense_sums[uuid],
                booking_sums[uuid],
            )
        raise SystemExit(
            f"sum invariant violated for {len(broken)} transaction(s); nothing written"
        )
    _LOGGER.info(
        "Sum invariant holds for all %d transactions (%d expenses, %d bookings).",
        len(transactions),
        len(expenses),
        len(bookings),
    )


def _natural_key(row: dict, table: str) -> tuple:
    if table == _TABLE_TRANSACTIONS:
        return (str(row["moss_transaction_uuid"]),)
    if table == _TABLE_EXPENSES:
        return (str(row["moss_transaction_uuid"]), int(row["expense_number"]))
    return (str(row["booking_unique_item_number"]),)


def _remember_source_files(by_kind, transactions, expenses, bookings) -> None:
    """Which export each row came from -- the card/balance file that carried its
    transaction (the detail files only refine rows that file already produced)."""
    origin = {
        (row.get("Transaction ID") or "").strip(): row["__source_file__"]
        for rows in (by_kind.get(_KIND_CARD, []), by_kind.get(_KIND_BALANCE, []))
        for row in rows
    }
    for table, records in (
        (_TABLE_TRANSACTIONS, transactions),
        (_TABLE_EXPENSES, expenses),
        (_TABLE_BOOKINGS, bookings),
    ):
        for row in records:
            _SOURCE_FILES[_natural_key(row, table)] = origin.get(
                str(row["moss_transaction_uuid"])
            )


def _log_plan_summary(table: str, planned) -> None:
    """What an approval would apply -- logged BEFORE the approval is asked for."""
    for counts in planned.operation_counts().values():
        _LOGGER.info(
            "Planned for %s: %d INSERTs, %d UPDATEs, %d DELETEs (%d untouched).",
            table,
            counts.inserts,
            counts.updates,
            counts.deletes,
            len(planned.untouched_keys),
        )
    for label, rows in (("INSERT", planned.inserts), ("UPDATE", planned.updates)):
        if not rows:
            continue
        columns = sorted({column for row in rows for column in row})
        _LOGGER.info(
            "  %s %d row(s); columns: %s", label, len(rows), ", ".join(columns)
        )


def _plan_for(connection, ctx, table: str, key, values):
    builder = SingleTableUpsertPlanBuilder(
        table, key, values, time_zone=ctx.hitobito_time_zone
    )
    builder.load_existing(connection)
    planned = builder.plan()
    _log_plan_summary(table, planned)
    return planned


def _apply(
    planned, connection, table: str, now, *, insert_only: dict | None = None
) -> None:
    """Apply one plan, adding the provenance and insert-only columns first.

    `source_file` is metadata about WHERE a row came from: a renamed export must
    never turn an otherwise identical row into an UPDATE, so it stays out of the
    diff and is refreshed only on rows that are written anyway. `insert_only`
    columns (the wallet link) are set once and then left to the app."""
    if not planned.inserts and not planned.updates:
        _LOGGER.info(
            "%s: nothing to write (%d untouched).", table, len(planned.untouched_keys)
        )
        return
    for row in (*planned.inserts, *planned.updates):
        row["source_file"] = _SOURCE_FILES.get(_natural_key(row, table))
    for row in planned.inserts:
        row.update(insert_only or {})
    inserted, updated = planned.apply(connection, now=now)
    _LOGGER.info(
        "%s: %d inserted, %d updated, %d untouched.",
        table,
        len(inserted),
        len(updated),
        len(planned.untouched_keys),
    )


def _id_map(connection, table: str, key_columns: list[str]) -> dict:
    """{natural key -> id} of a table, to resolve the surrogate foreign keys.
    Keys are stringified, because uuid/int columns come back typed."""
    columns = ", ".join(key_columns)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT id, {columns} FROM {table}")  # noqa: S608 - fixed identifiers
        return {
            tuple(str(part) for part in row[1:]): row[0] for row in cursor.fetchall()
        }


def _wallet_fin_account_id(connection) -> int | None:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT id FROM wsjrdp_fin_accounts WHERE transaction_type = %s ORDER BY id LIMIT 1",
            (_WALLET_TRANSACTION_TYPE,),
        )
        row = cursor.fetchone()
    return row[0] if row else None


def _transaction_key(row: dict) -> tuple[str]:
    """The L1 lookup key of a row -- an _id_map key, hence stringified."""
    return (str(row["moss_transaction_uuid"]),)


def _expense_key(row: dict, parents: dict) -> tuple[str, str]:
    """The L2 lookup key of a booking: the expense it belongs to."""
    return (
        str(row["moss_transaction_uuid"]),
        str(parents[row["booking_unique_item_number"]]),
    )


# ======================================================================= CLI


def create_argument_parser():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "files",
        nargs="+",
        help="Moss exports (card, balance-movements, reimbursements, invoices); "
        "the kind of each file is detected from its columns.",
    )
    # --dry-run comes from the WsjRdpContext base parser (ctx.dry_run).
    parser.add_argument(
        "--rollback-for-testing",
        action="store_true",
        default=False,
        help="Apply the plan, then ROLLBACK instead of committing (testing).",
    )
    return parser


def _dry_run_lower_levels(connection, ctx, known, expenses, bookings, parents) -> None:
    """Plan L2/L3 for the transactions that already exist, and report how much
    is deferred because its transaction would have to be inserted first."""
    ready_expenses = [row for row in expenses if _transaction_key(row) in known]
    for row in ready_expenses:
        row["moss_transaction_id"] = known[_transaction_key(row)]
    if ready_expenses:
        _plan_for(
            connection,
            ctx,
            _TABLE_EXPENSES,
            ["moss_transaction_uuid", "expense_number"],
            ready_expenses,
        )
    expense_ids = _id_map(
        connection, _TABLE_EXPENSES, ["moss_transaction_uuid", "expense_number"]
    )
    ready_bookings = []
    for row in bookings:
        key = _expense_key(row, parents)
        if key not in expense_ids:
            continue
        row["moss_expense_id"] = expense_ids[key]
        row["moss_transaction_id"] = known[_transaction_key(row)]
        ready_bookings.append(row)
    if ready_bookings:
        _plan_for(
            connection,
            ctx,
            _TABLE_BOOKINGS,
            "booking_unique_item_number",
            ready_bookings,
        )
    deferred = (
        len(expenses) - len(ready_expenses),
        len(bookings) - len(ready_bookings),
    )
    if any(deferred):
        _LOGGER.warning(
            "[--dry-run] %d expense(s) and %d booking(s) not planned: their transaction "
            "does not exist yet.",
            *deferred,
        )


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    )
    out_base = ctx.make_out_path(_SELF_NAME + "_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    # Read and check everything first: the database is not touched before the
    # whole four-file picture is consistent.
    by_kind = _read_all(ctx.parsed_args.files)
    transactions, expenses, bookings = _build_records(by_kind)
    if not transactions:
        _LOGGER.warning("No Moss transactions in the given files; nothing to do.")
        return 0
    _verify_sum_invariant(transactions, expenses, bookings)
    _remember_source_files(by_kind, transactions, expenses, bookings)
    parents = {
        row["booking_unique_item_number"]: row.pop("_expense_number")
        for row in bookings
    }

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        transaction_plan = _plan_for(
            ro_conn, ctx, _TABLE_TRANSACTIONS, "moss_transaction_uuid", transactions
        )

        # L2/L3 are keyed on their own natural keys but need the surrogate
        # parent ids, which exist only after L1 has been applied. Planning them
        # up front is therefore possible only for already-known transactions --
        # enough to make --dry-run informative.
        if ctx.dry_run:
            known = _id_map(ro_conn, _TABLE_TRANSACTIONS, ["moss_transaction_uuid"])
            _dry_run_lower_levels(ro_conn, ctx, known, expenses, bookings, parents)
            _LOGGER.warning("[--dry-run] Nothing applied.")
            return 0

        # The plan summaries above show exactly what this approval applies.
        ctx.require_approval_to_run_in_prod()
        rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
        wallet_id = _wallet_fin_account_id(rw_conn)
        if wallet_id is None:
            _LOGGER.warning(
                "No Moss wallet fin account found; fin_account_id stays NULL."
            )
        _apply(
            transaction_plan,
            rw_conn,
            _TABLE_TRANSACTIONS,
            ctx.start_time,
            insert_only={"fin_account_id": wallet_id} if wallet_id else None,
        )

        transaction_ids = _id_map(
            rw_conn, _TABLE_TRANSACTIONS, ["moss_transaction_uuid"]
        )
        for row in expenses:
            row["moss_transaction_id"] = transaction_ids[_transaction_key(row)]
        expense_plan = _plan_for(
            rw_conn,
            ctx,
            _TABLE_EXPENSES,
            ["moss_transaction_uuid", "expense_number"],
            expenses,
        )
        _apply(expense_plan, rw_conn, _TABLE_EXPENSES, ctx.start_time)

        expense_ids = _id_map(
            rw_conn, _TABLE_EXPENSES, ["moss_transaction_uuid", "expense_number"]
        )
        for row in bookings:
            row["moss_expense_id"] = expense_ids[_expense_key(row, parents)]
            row["moss_transaction_id"] = transaction_ids[_transaction_key(row)]
        booking_plan = _plan_for(
            rw_conn, ctx, _TABLE_BOOKINGS, "booking_unique_item_number", bookings
        )
        _apply(booking_plan, rw_conn, _TABLE_BOOKINGS, ctx.start_time)

        if ctx.parsed_args.rollback_for_testing:
            _LOGGER.warning(
                "ROLLBACK (--rollback-for-testing given) - nothing committed."
            )
            rw_conn.rollback()
        # The commit happens when the `with ctx:` block exits cleanly; an
        # exception before that leaves the database untouched.
    return 0


if __name__ == "__main__":
    _sys.exit(main())
