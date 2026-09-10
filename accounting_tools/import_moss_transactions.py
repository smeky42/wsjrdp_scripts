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
is written on INSERT only, `source_file` only on rows that change anyway. What
an export profile may leave empty is protected on top of that, see the next
section.

PROFILE-DEPENDENT COLUMNS
-------------------------
Moss export profiles disagree about what a balance movement carries: the payout
account (`Recipient Account Number`, `Recipient Bank Code`), the finance user
who released it (`Cardholder`), that user's team (`Team Name`) and a `Payment
Date` of its own are filled by one profile and empty -- or a copy of `Booking
Date` -- in the next. A balance export whose payout rows carry none of them is
recognised as such and logged once when it is read. Hence:
  * `recipient_iban`, `recipient_bic` and `recipient_name` come from the
    balance export, and a BLANK cell never overwrites a stored value: every
    plan of moss_transactions protects the profile-dependent columns the
    planned records carry and reports per column what it kept. There is no
    fallback source: a `User IBAN` / `Supplier IBAN` is master data, not the
    account that was paid.
  * `payout_user_name` and `payout_team_name` are never written; the raw
    `Cardholder` and `Team Name` of a balance row go to the transaction's
    other_moss_columns wherever the cell has a value.
  * `payment_date` is written for card payments and top-ups; a reimbursement
    and an invoice carry none at all, so the column is not compared either.
  * `top_up_sender` is the organisation that funded the wallet -- the text
    before the first " - " or ";" -- and the raw line stays in
    other_moss_columns["Reason for Purchase"].

KEYS
----
  L1 moss_object_uuid -- the object the payment settles. The database GENERATES
     it as COALESCE(moss_reimbursement_uuid, moss_invoice_uuid,
     moss_transaction_uuid) and the importer computes the same expression, so a
     card payment and a top-up are identified by their Transaction ID, a
     reimbursement by its Linked Reimbursement ID and an invoice by its Linked
     Invoice ID.
  L2 moss_expense_uuid -- a reimbursement expense carries the CSV "Unique
     Expense ID"; the SHELL expense of a card, invoice or top-up carries its
     transaction's moss_object_uuid. `expense_number` stays an attribute
     (unique per transaction), not a key.
  L3 (moss_expense_id, sub_row_number) -- the split's own "Sub-row Number"
     inside its expense, read from the export that carries the split (the card
     and reimbursement exports, the invoice line, the balance row of a top-up).
     Never the CSV "Unique Item Number", whose suffix is a file-position
     counter and therefore unstable; that raw value is kept in
     other_moss_columns["Unique Item Number"].

TRANSACTION ID CHANGES
----------------------
Moss export profiles do not agree on the `Transaction ID` of a balance
movement: the same payout arrives under a different id in another profile.
`moss_transaction_uuid` is therefore the FIRST id a row was ever seen under and
is never overwritten; EVERY id it has been seen under is collected in
`all_moss_transaction_uuids`, so a lookup by any of them still finds the row.
Before anything is planned, every transaction of the files is resolved against
the stored rows:

  card / reimbursement / invoice  by moss_object_uuid; the stored row under it
     must carry the matching type.
  top-up  by its Transaction ID in `all_moss_transaction_uuids`; failing that
     by the HEURISTIC (booking_date, signed_total_base_amount), which survives
     a profile change. Every heuristic match is logged, several candidates are
     a conflict, none means a new transaction.

Anything ambiguous -- a stored row of the wrong type, an id already claimed by
another row, two records resolving to one row -- is a conflict: each one is
logged and the run stops with exit code 1 before the first plan. An id that
merely changed needs no confirmation; it is appended to the array.

After the write the run verifies that no id is in two rows' arrays, that every
row's `moss_transaction_uuid` is in its own array, that every non-reimbursement
expense carries its transaction's `moss_object_uuid`, and that the sum
invariant holds for every transaction the run touched.

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
import dataclasses as _dataclasses
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
    # Both text columns are re-set from _payment_reference() afterwards: the
    # raw value carries our own organisation name as a suffix.
    "Payment Reference": "payment_reference",
    "Invoice Number": "invoice_number",
    "Linked Reimbursement ID": "moss_reimbursement_uuid",
    "Linked Invoice ID": "moss_invoice_uuid",
}
#: Constant per transaction. `Cardholder` is the finance user who released a
#: payout, `Team Name` that user's team in one profile and the invoice's own
#: team in another -- which of the two a cell means depends on the export, so
#: both are kept RAW only, under their CSV header and only where filled (a
#: top-up has neither).
BALANCE_OTHER_TX: tuple[str, ...] = (
    "Reason for Purchase",
    "Moss Attachment URL",
    "Cardholder",
    "Team Name",
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
    # read directly for the L1 transaction_posting_text (the Buchungstext)
    "Reimbursement Description",
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
_INT_COLS = frozenset({"expense_number", "sub_row_number"})
#: Postgres `uuid` columns. They must travel as uuid.UUID, not as text: the
#: comparison in the plan's key lookup is typed, and `uuid = text` has no
#: operator in Postgres.
_UUID_COLS = frozenset(
    {
        "moss_object_uuid",
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


def _top_up_sender(reason_for_purchase: str | None) -> str | None:
    """The organisation that funded the wallet: Moss appends the account it
    came from -- an IBAN behind " - " in one export profile, a short code
    behind ";" in the next, and cut off at 60 characters either way -- so only
    the text before that separator is a fact of the top-up.

    >>> _top_up_sender("Some Organisation e.V. - DE00 0000 0000 0000 0000 00")
    'Some Organisation e.V.'
    >>> _top_up_sender("Some Organisation; X1234")
    'Some Organisation'
    >>> _top_up_sender("") is None
    True
    """
    if not reason_for_purchase:
        return None
    text = reason_for_purchase
    for separator in (" - ", ";"):
        text = text.split(separator)[0]
    return text.strip() or None


#: The kinds the balance export pays OUT (a card payment and a top-up are the
#: other two).
_PAYOUT_KINDS = frozenset({"MossReimbursement", "MossInvoice"})


def _drop_payout_payment_date(kind: str, transaction: dict) -> dict:
    """A reimbursement and an invoice carry NO payment_date: the column is
    neither written nor compared for them, because the balance export reports
    the booking date there in some profiles and the real payout day in others.
    A card payment and a top-up keep theirs.

    >>> _drop_payout_payment_date("MossInvoice", {"payment_date": 1, "x": 2})
    {'x': 2}
    >>> _drop_payout_payment_date("MossTopUp", {"payment_date": 1})
    {'payment_date': 1}
    """
    if kind in _PAYOUT_KINDS:
        transaction.pop("payment_date", None)
    return transaction


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


def _without_payout_details(rows: list[dict]) -> bool:
    """Whether a balance export leaves the payout details empty: no payout row
    -- a reimbursement or an invoice -- carries a recipient account or a
    cardholder, and every one of them repeats its `Booking Date` as the
    `Payment Date`. All three conditions describe the payouts, so a top-up's
    own payment day never decides this (see PROFILE-DEPENDENT COLUMNS).

    >>> _without_payout_details([{"Linked Invoice ID": "i",
    ...     "Payment Date": "2026-05-01", "Booking Date": "2026-05-01"}])
    True
    >>> _without_payout_details([{"Linked Invoice ID": "i",
    ...     "Cardholder": "Some One", "Payment Date": "2026-05-01",
    ...     "Booking Date": "2026-05-01"}])
    False
    >>> _without_payout_details([{"Booking Date": "2026-05-01"}])
    False

    A top-up settling on another day than it books is a fact of that payment,
    not a profile trait:

    >>> _without_payout_details([{"Linked Invoice ID": "i",
    ...     "Payment Date": "2026-05-01", "Booking Date": "2026-05-01"},
    ...     {"Payment Date": "2026-05-02", "Booking Date": "2026-05-04"}])
    True
    """
    payouts = [row for row in rows if _balance_kind(row) in _PAYOUT_KINDS]
    if not payouts:
        return False
    if any(
        _text(row, "Recipient Account Number") or _text(row, "Cardholder")
        for row in payouts
    ):
        return False
    return all(
        _text(row, "Payment Date") == _text(row, "Booking Date") for row in payouts
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
        if kind == _KIND_BALANCE and _without_payout_details(rows):
            _LOGGER.info(
                "%s: balance export without payout details: no recipient account, "
                "no cardholder, payout day equal to the booking day; "
                "recipient_iban/recipient_bic keep their stored values, "
                "payment_date is not imported for reimbursements and invoices.",
                path.name,
            )
        for row in rows:
            row["__source_file__"] = path.name
        by_kind.setdefault(kind, []).extend(rows)
        _LOGGER.info("%s: %d rows (%s export).", path.name, len(rows), kind)
    return by_kind


# ========================================================== record building
# Each builder appends to (transactions, expenses, bookings). Every record
# carries the private key "_transaction_ref" naming its transaction and every
# booking additionally "_expense_ref" naming its expense; both start out as the
# CSV values, are rewritten to the resolved identity (see _apply_resolution)
# and are stripped again before planning.


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
            _transaction_ref=key,
        )
        transactions.append(transaction)
        expenses.append(
            {
                "_transaction_ref": key,
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
                sub_row_number=_int_or_zero(split.get("Sub-row Number")),
                _transaction_ref=key,
                _expense_ref=uuid,
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

        transaction = _drop_payout_payment_date(
            kind, _mapped(head, BALANCE_TX_COLUMN_MAP)
        )
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
            # Only the funding organisation; the account behind it is spelled
            # differently per export profile, and the raw line stays in
            # other_moss_columns["Reason for Purchase"].
            top_up_sender=(
                _top_up_sender(_text(head, "Reason for Purchase"))
                if kind == "MossTopUp"
                else None
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
            _transaction_ref=key,
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
            _reimbursement_levels(key, balance_rows, detail, expenses, bookings)
        elif kind == "MossInvoice":
            _invoice_levels(key, balance_rows, detail, expenses, bookings)
        else:
            _top_up_levels(key, uuid, balance_rows, expenses, bookings)
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


def _reimbursement_levels(ref, balance_rows, detail, expenses, bookings) -> None:
    """One expense per balance row, its bookings being the reimbursement's
    splits. The two sides correspond 1:1 IN ORDER -- the
    balance row carries no expense id to join on."""
    for balance, expense in zip(balance_rows, detail):
        expense_number = _int_or_zero(balance.get("Sub-row Number"))
        head = expense["rows"][0]
        # The balance row's sign is authoritative: the detail export reports
        # unsigned split amounts.
        sign = -1 if (_decimal_or_none(balance.get("Amount")) or 0) < 0 else 1
        expense_uuid = _as_uuid(expense["uuid"])
        record = _mapped(head, REIMBURSEMENT_EXPENSE_COLUMN_MAP)
        record.update(
            _transaction_ref=ref,
            expense_number=expense_number,
            type="MossReimbursementExpense",
            moss_expense_uuid=expense_uuid,
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
                sub_row_number=_int_or_zero(split.get("Sub-row Number")),
                _transaction_ref=ref,
                _expense_ref=expense_uuid,
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


def _invoice_levels(ref, balance_rows, detail, expenses, bookings) -> None:
    """The invoice IS the expense: ONE shell expense, one booking per line. Balance
    rows and invoice lines correspond 1:1 in order, which is
    what gives an invoice line its cost center -- balance-movements has no
    cost-center column at all."""
    head = detail[0]
    invoice_uuid = _as_uuid(_text(head, "Invoice ID"))
    # A shell: the invoice's dates, texts and terms are transaction columns /
    # keys, as in Moss's invoice header.
    expenses.append(
        {
            "_transaction_ref": ref,
            "expense_number": 1,
            "type": "MossInvoiceExpense",
            "moss_expense_uuid": invoice_uuid,
            "signed_expense_base_amount": _sum(balance_rows, "Amount"),
            "signed_expense_transaction_amount": _sum(balance_rows, "Original Amount"),
        }
    )
    for balance, line in zip(balance_rows, detail):
        booking = _mapped(line, INVOICE_BOOKING_COLUMN_MAP)
        booking.update(
            # The line's own split number; it equals the paired balance row's.
            sub_row_number=_int_or_zero(line.get("Sub-row Number")),
            _transaction_ref=ref,
            _expense_ref=invoice_uuid,
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


def _top_up_levels(ref, uuid, balance_rows, expenses, bookings) -> None:
    """A wallet top-up has no expense and no expense account, but still gets one
    expense and one booking, so the sum invariant holds for every kind."""
    expenses.append(
        {
            "_transaction_ref": ref,
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
                "sub_row_number": _int_or_zero(row.get("Sub-row Number")),
                "_transaction_ref": ref,
                "_expense_ref": uuid,
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
        expense_sums[row["_transaction_ref"]] += row["signed_expense_base_amount"] or 0
    booking_sums: dict[str, _decimal.Decimal] = _collections.defaultdict(
        _decimal.Decimal
    )
    for row in bookings:
        booking_sums[row["_transaction_ref"]] += row["signed_base_amount"] or 0

    broken = [
        row
        for row in transactions
        if row["signed_total_base_amount"] != expense_sums[row["_transaction_ref"]]
        or row["signed_total_base_amount"] != booking_sums[row["_transaction_ref"]]
    ]
    if broken:
        for row in broken[:10]:
            uuid = row["_transaction_ref"]
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


# =================================================================== identity
# WHICH stored row a CSV transaction is. The CSV `Transaction ID` is not stable
# across Moss export profiles, so every transaction is resolved to its identity
# -- moss_object_uuid -- before anything is planned.

_STATUS_MATCHED = "matched"
_STATUS_VIA_ARRAY = "matched via array"
_STATUS_HEURISTIC = "matched by heuristic"
_STATUS_NEW = "new"
_STATUSES: tuple[str, ...] = (
    _STATUS_MATCHED,
    _STATUS_VIA_ARRAY,
    _STATUS_HEURISTIC,
    _STATUS_NEW,
)


@_dataclasses.dataclass(frozen=True, kw_only=True)
class _StoredTransaction:
    """A stored moss_transactions row, reduced to what identity needs."""

    id: int
    type: str
    object_uuid: _uuid.UUID
    all_transaction_uuids: tuple[_uuid.UUID, ...]
    booking_date: _datetime.date | None
    signed_total_base_amount: _decimal.Decimal | None


@_dataclasses.dataclass(frozen=True, kw_only=True)
class _StoredIndex:
    """The stored transactions, indexed the three ways a resolution needs:
    by identity, by EVERY Transaction ID a row has been seen under, and -- for
    top-ups, whose id is all the export gives them -- by booking date and
    amount."""

    by_object: dict[str, _StoredTransaction]
    by_any_id: dict[str, _StoredTransaction]
    top_ups: dict[tuple, list[_StoredTransaction]]


@_dataclasses.dataclass(frozen=True, kw_only=True)
class _Resolved:
    """What one CSV transaction turned out to be."""

    object_uuid: _uuid.UUID
    stored_id: int | None
    status: str
    kind: str
    appends_id: bool


def _require_object_uuid_column(connection) -> None:
    """The generated identity column is the contract with the wagon schema; an
    older database would silently key the plan on a column that is not there."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = %s AND column_name = %s",
            (_TABLE_TRANSACTIONS, "moss_object_uuid"),
        )
        if cursor.fetchone() is not None:
            return
    _LOGGER.error(
        "%s has no column moss_object_uuid: the wagon migration 20260910100000 "
        "is not applied to this database.",
        _TABLE_TRANSACTIONS,
    )
    raise SystemExit(1)


def _load_stored_transactions(connection) -> list[_StoredTransaction]:
    """The whole transaction table, identity columns only -- it is small, and a
    lookup per record would be one round trip each."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT id, type, moss_object_uuid, moss_transaction_uuid, "
            "all_moss_transaction_uuids, booking_date, signed_total_base_amount "
            "FROM moss_transactions"
        )
        return [
            _StoredTransaction(
                id=row[0],
                type=row[1],
                object_uuid=row[2],
                all_transaction_uuids=(row[3], *(row[4] or ())),
                booking_date=row[5],
                signed_total_base_amount=row[6],
            )
            for row in cursor.fetchall()
        ]


def _top_up_match_key(booking_date, amount) -> tuple:
    """The heuristic identity of a wallet top-up: the two facts that survive a
    change of the export profile, while its Transaction ID does not.

    >>> import datetime, decimal
    >>> _top_up_match_key(datetime.date(2026, 5, 1), decimal.Decimal("2.500"))
    (datetime.date(2026, 5, 1), Decimal('2.5'))
    >>> _top_up_match_key(None, None)
    (None, None)
    """
    return (
        booking_date,
        None if amount is None else _decimal.Decimal(amount).normalize(),
    )


def _index_stored(
    stored: list[_StoredTransaction], conflicts: list[str]
) -> _StoredIndex:
    by_object: dict[str, _StoredTransaction] = {}
    by_any_id: dict[str, _StoredTransaction] = {}
    top_ups: dict[tuple, list[_StoredTransaction]] = _collections.defaultdict(list)
    for row in stored:
        by_object[str(row.object_uuid)] = row
        for seen in row.all_transaction_uuids:
            other = by_any_id.setdefault(str(seen), row)
            if other.id != row.id:
                conflicts.append(
                    f"the Transaction ID {seen} is stored on two transactions "
                    f"({other.object_uuid} and {row.object_uuid})"
                )
        if row.type == "MossTopUp":
            key = _top_up_match_key(row.booking_date, row.signed_total_base_amount)
            top_ups[key].append(row)
    return _StoredIndex(by_object=by_object, by_any_id=by_any_id, top_ups=top_ups)


def _resolve_top_up(
    record: dict, csv_id: str, index: _StoredIndex, conflicts: list[str]
):
    """A top-up has no second id at all, so its own Transaction ID is tried
    first and booking date plus amount are the fallback."""
    row = index.by_any_id.get(csv_id)
    if row is not None:
        if row.type != "MossTopUp":
            conflicts.append(
                f"MossTopUp {csv_id}: the transaction stored under that id is a "
                f"{row.type}"
            )
        return row.object_uuid, row, _STATUS_VIA_ARRAY
    key = _top_up_match_key(record["booking_date"], record["signed_total_base_amount"])
    candidates = index.top_ups.get(key, [])
    if len(candidates) > 1:
        conflicts.append(
            f"MossTopUp {csv_id}: {len(candidates)} stored top-ups share its "
            "booking date and amount -- the heuristic cannot decide"
        )
        return _as_uuid(csv_id), None, _STATUS_NEW
    if candidates:
        row = candidates[0]
        _LOGGER.warning(
            "top-up matched by booking date and amount: Transaction ID %s is new, "
            "the stored transaction is %s (booking date %s).",
            csv_id,
            row.object_uuid,
            row.booking_date,
        )
        return row.object_uuid, row, _STATUS_HEURISTIC
    return _as_uuid(csv_id), None, _STATUS_NEW


def _resolve_one(record: dict, index: _StoredIndex, conflicts: list[str]):
    """(identity, the stored row or None, status) of one L1 record."""
    csv_id = record["_transaction_ref"]
    kind = record["type"]
    if kind == "MossTopUp":
        return _resolve_top_up(record, csv_id, index, conflicts)
    if kind == "MossCardTransaction":
        object_uuid = _as_uuid(csv_id)
    else:
        column = (
            "moss_reimbursement_uuid"
            if kind == "MossReimbursement"
            else "moss_invoice_uuid"
        )
        object_uuid = record[column]
        # The paid object identifies the transaction, so a Transaction ID that
        # belongs to a DIFFERENT transaction is a contradiction, not a change.
        claimed = index.by_any_id.get(csv_id)
        if claimed is not None and str(claimed.object_uuid) != str(object_uuid):
            conflicts.append(
                f"{kind} {object_uuid}: its Transaction ID {csv_id} is stored on "
                f"the transaction {claimed.object_uuid}"
            )
    row = index.by_object.get(str(object_uuid))
    if row is None:
        return object_uuid, None, _STATUS_NEW
    if row.type != kind:
        conflicts.append(
            f"{kind} {object_uuid}: the stored transaction is a {row.type}"
        )
    return object_uuid, row, _STATUS_MATCHED


def _resolve_transactions(
    transactions: list[dict], stored: list[_StoredTransaction]
) -> dict[str, _Resolved]:
    """CSV Transaction ID -> identity, for every L1 record. Every conflict is
    logged and the run then stops: an ambiguous identity is never guessed at,
    and at this point nothing has been planned."""
    conflicts: list[str] = []
    index = _index_stored(stored, conflicts)
    resolved: dict[str, _Resolved] = {}
    claimed_rows: dict[int, str] = {}
    claimed_new: dict[str, str] = {}
    for record in transactions:
        csv_id = record["_transaction_ref"]
        if csv_id in resolved:
            conflicts.append(
                f"the Transaction ID {csv_id} occurs in two of the given exports"
            )
            continue
        object_uuid, row, status = _resolve_one(record, index, conflicts)
        if row is None:
            first = claimed_new.setdefault(str(object_uuid), csv_id)
            if first != csv_id:
                conflicts.append(
                    f"two new transactions share the identity {object_uuid} "
                    f"(Transaction IDs {first} and {csv_id})"
                )
        else:
            first = claimed_rows.setdefault(row.id, csv_id)
            if first != csv_id:
                conflicts.append(
                    f"the stored transaction {object_uuid} is claimed by two "
                    f"Transaction IDs ({first} and {csv_id})"
                )
        resolved[csv_id] = _Resolved(
            object_uuid=object_uuid,
            stored_id=None if row is None else row.id,
            status=status,
            kind=record["type"],
            appends_id=row is not None
            and _as_uuid(csv_id) not in row.all_transaction_uuids,
        )
    if conflicts:
        for line in dict.fromkeys(conflicts):
            _LOGGER.error("identity conflict -- %s", line)
        _LOGGER.error(
            "%d identity conflict(s); nothing planned, nothing written.",
            len(conflicts),
        )
        raise SystemExit(1)
    _log_resolution_summary(resolved)
    return resolved


def _log_resolution_summary(resolved: dict[str, _Resolved]) -> None:
    per_kind: dict[str, _collections.Counter] = _collections.defaultdict(
        _collections.Counter
    )
    for entry in resolved.values():
        per_kind[entry.kind][entry.status] += 1
    for kind in sorted(per_kind):
        counts = per_kind[kind]
        _LOGGER.info(
            "identity %-20s %s",
            kind,
            ", ".join(f"{status} {counts[status]}" for status in _STATUSES),
        )
    _LOGGER.info(
        "%d transaction(s) resolved; %d Transaction ID(s) not yet in the stored "
        "array (they are appended).",
        len(resolved),
        sum(1 for entry in resolved.values() if entry.appends_id),
    )


def _apply_resolution(
    resolved: dict[str, _Resolved],
    transactions: list[dict],
    expenses: list[dict],
    bookings: list[dict],
    source_files: dict[str, str | None],
) -> dict[str, str | None]:
    """Rewrite the records with the resolved identity and return the provenance
    keyed by it.

    L1 gets its key `moss_object_uuid` and, on an EXISTING row, loses
    `moss_transaction_uuid`: the first id a row was seen under stays what it
    is. Every record collects its CSV id in `all_moss_transaction_uuids`, where
    an already stored id produces no delta at all. The shell expense of a card,
    invoice or top-up follows the identity, and every cross-level reference is
    re-pointed from the CSV id to it."""
    origin: dict[str, str | None] = {}
    expense_refs: dict[str, _uuid.UUID] = {}
    for row in transactions:
        csv_id = row["_transaction_ref"]
        entry = resolved[csv_id]
        row["moss_object_uuid"] = entry.object_uuid
        if entry.stored_id is not None:
            row.pop("moss_transaction_uuid", None)
        row["all_moss_transaction_uuids"] = wsjrdp2027.PgArray(
            [csv_id],
            wsjrdp2027.ArrayElementType.UUID,
            mode=wsjrdp2027.ArrayMode.APPEND,
        )
        row["_transaction_ref"] = str(entry.object_uuid)
        origin[str(entry.object_uuid)] = source_files.get(csv_id)
    for row in expenses:
        object_uuid = resolved[row["_transaction_ref"]].object_uuid
        if row["type"] != "MossReimbursementExpense":
            expense_refs[str(row["moss_expense_uuid"])] = object_uuid
            row["moss_expense_uuid"] = object_uuid
        row["_transaction_ref"] = str(object_uuid)
    for row in bookings:
        row["_transaction_ref"] = str(resolved[row["_transaction_ref"]].object_uuid)
        row["_expense_ref"] = expense_refs.get(
            str(row["_expense_ref"]), row["_expense_ref"]
        )
    return origin


# ===================================================== planning & applying


def _natural_key(row: dict, table: str) -> tuple:
    if table == _TABLE_TRANSACTIONS:
        return (str(row["moss_object_uuid"]),)
    if table == _TABLE_EXPENSES:
        return (str(row["moss_expense_uuid"]),)
    return (str(row["moss_expense_id"]), int(row["sub_row_number"]))


def _source_file_by_transaction(
    by_kind: dict[str, list[dict]],
) -> dict[str, str | None]:
    """CSV Transaction ID -> the export file that carried it (the card/balance
    file; the detail files only refine rows that file already produced)."""
    return {
        (row.get("Transaction ID") or "").strip(): row["__source_file__"]
        for rows in (by_kind.get(_KIND_CARD, []), by_kind.get(_KIND_BALANCE, []))
        for row in rows
    }


def _remember_source_files(origin: dict, table: str, records: list[dict]) -> None:
    """Provenance for one level, keyed by the natural key the plan uses. Called
    once the key columns of the level are final -- for L3 that is after
    `moss_expense_id` is known."""
    for row in records:
        _SOURCE_FILES[_natural_key(row, table)] = origin.get(
            str(row["_transaction_ref"])
        )


def _column_update_counts(rows: list[dict], key_names: tuple[str, ...]) -> list[tuple]:
    """How many of the planned UPDATE rows carry each non-key column,
    most-written column first. A profile switch that rewrites one column on
    hundreds of rows is visible in the preview as exactly that.

    >>> _column_update_counts([{"k": 1, "a": 1, "b": 2}, {"k": 2, "b": 3}], ("k",))
    [('b', 2), ('a', 1)]
    >>> _column_update_counts([], ("k",))
    []
    """
    counted = _collections.Counter(
        column for row in rows for column in row if column not in key_names
    )
    return sorted(counted.items(), key=lambda item: (-item[1], item[0]))


def _log_plan_summary(table: str, planned, key) -> None:
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
    key_names = (key,) if isinstance(key, str) else tuple(key)
    per_column = _column_update_counts(planned.updates, key_names)
    if per_column:
        _LOGGER.info(
            "  UPDATE columns: %s",
            ", ".join(f"{column} ({count})" for column, count in per_column),
        )
    kept = sorted(
        planned.kept_blank_counts.items(), key=lambda item: (-item[1], item[0])
    )
    if kept:
        _LOGGER.info(
            "  kept stored values for blank input: %s",
            ", ".join(f"{column} ({count})" for column, count in kept),
        )


def _keep_stored_when_blank(records: list[dict]) -> tuple[str, ...]:
    """The blank-protected transaction columns the given L1 records carry. The
    plan builder rejects a column no value set mentions, and which of them a
    run carries depends on its files -- this importer never writes the wallet
    statement's sender_* columns at all.

    >>> _keep_stored_when_blank([{"recipient_bic": ""}, {"recipient_iban": None}])
    ('recipient_iban', 'recipient_bic')
    >>> _keep_stored_when_blank([{"booking_date": None}])
    ()
    """
    columns = {column for record in records for column in record}
    return tuple(
        column
        for column in wsjrdp2027.moss.MOSS_TRANSACTION_KEEP_STORED_WHEN_BLANK
        if column in columns
    )


def _plan_for(
    connection,
    ctx,
    table: str,
    key,
    values,
    *,
    generated_key_columns=(),
):
    """Plan one table against its stored state and log the preview.

    The blank protection belongs to the TABLE, not to the caller: every plan of
    moss_transactions protects the profile-dependent columns the given records
    carry, so no caller can forget it. The other two levels have none."""
    builder = SingleTableUpsertPlanBuilder(
        table,
        key,
        values,
        time_zone=ctx.hitobito_time_zone,
        generated_key_columns=generated_key_columns,
    )
    builder.load_existing(connection)
    keep = _keep_stored_when_blank(values) if table == _TABLE_TRANSACTIONS else ()
    planned = builder.plan(keep_stored_when_blank=keep)
    _log_plan_summary(table, planned, key)
    return planned


def _plannable(records: list[dict], *private: str) -> list[dict]:
    """The records without their private cross-level references -- they name no
    database column and would otherwise be planned as one."""
    for row in records:
        for name in private:
            row.pop(name, None)
    return records


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
    """The L1 lookup key of a record -- an _id_map key, hence stringified."""
    return (str(row["_transaction_ref"]),)


def _expense_key(row: dict) -> tuple[str]:
    """The L2 lookup key of a booking: the expense it belongs to."""
    return (str(row["_expense_ref"]),)


def _owner_ids(records: list[dict], table: str) -> dict[tuple, int]:
    """{natural key -> moss_transaction_id} of one level, built while the
    records still carry both -- a plan row carries only its key.

    >>> _owner_ids([{"moss_expense_uuid": "u", "moss_transaction_id": 7}],
    ...            _TABLE_EXPENSES)
    {('u',): 7}
    """
    return {_natural_key(row, table): row["moss_transaction_id"] for row in records}


def _touched_transaction_ids(*levels) -> set[int]:
    """Every transaction this run wrote something on -- its own row or one of
    its expenses / bookings. `levels` are (plan, owner ids, table) triples."""
    touched: set[int] = set()
    for planned, owners, table in levels:
        for row in (*planned.inserts, *planned.updates):
            owner = owners.get(_natural_key(row, table))
            if owner is not None:
                touched.add(owner)
    return touched


def _post_run_checks(connection, touched: set[int]) -> None:
    """What no single plan can see, verified before the run is committed: the
    identity columns across all three levels and the sum invariant on every
    transaction the run touched. A violation stops the run, so leaving
    `with ctx:` rolls the whole import back."""
    failures: list[str] = []
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM (SELECT u FROM (SELECT "
            "unnest(all_moss_transaction_uuids) AS u FROM moss_transactions) e "
            "GROUP BY u HAVING count(*) > 1) s"
        )
        shared = cursor.fetchone()[0]
        if shared:
            failures.append(f"{shared} Transaction ID(s) belong to more than one row")

        cursor.execute(
            "SELECT count(*), COALESCE(sum(cardinality(all_moss_transaction_uuids)), 0) "
            "FROM moss_transactions"
        )
        rows, elements = cursor.fetchone()

        cursor.execute(
            "SELECT count(*) FROM moss_transactions "
            "WHERE NOT (moss_transaction_uuid = ANY (all_moss_transaction_uuids))"
        )
        unlisted = cursor.fetchone()[0]
        if unlisted:
            failures.append(
                f"{unlisted} row(s) whose moss_transaction_uuid is not in their "
                "all_moss_transaction_uuids"
            )

        cursor.execute(
            "SELECT count(*), count(*) FILTER (WHERE e.moss_expense_uuid "
            "<> t.moss_object_uuid) FROM moss_expenses e "
            "JOIN moss_transactions t ON t.id = e.moss_transaction_id "
            "WHERE e.type <> 'MossReimbursementExpense'"
        )
        shells, wrong_shells = cursor.fetchone()
        if wrong_shells:
            failures.append(
                f"{wrong_shells} shell expense(s) do not carry their "
                "transaction's moss_object_uuid"
            )

        cursor.execute(
            "SELECT t.id, t.signed_total_base_amount,"
            " COALESCE((SELECT sum(e.signed_expense_base_amount) FROM moss_expenses e"
            "  WHERE e.moss_transaction_id = t.id), 0),"
            " COALESCE((SELECT sum(b.signed_base_amount) FROM moss_bookings b"
            "  WHERE b.moss_transaction_id = t.id), 0)"
            " FROM moss_transactions t WHERE t.id = ANY(%s)",
            (sorted(touched),),
        )
        broken = [
            row for row in cursor.fetchall() if row[1] != row[2] or row[1] != row[3]
        ]
    if broken:
        for row in broken[:10]:
            _LOGGER.error(
                "post-run sum invariant: transaction #%s total=%s expenses=%s bookings=%s",
                *row,
            )
        failures.append(
            f"the sum invariant is broken on {len(broken)} touched transaction(s)"
        )
    if failures:
        for line in failures:
            _LOGGER.error("post-run check FAILED -- %s", line)
        raise SystemExit(1)
    _LOGGER.info(
        "Post-run checks passed: %d transaction(s) with %d Transaction ID(s), "
        "%d shell expense(s), sum invariant on %d touched transaction(s).",
        rows,
        elements,
        shells,
        len(touched),
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


def _dry_run_lower_levels(connection, ctx, known, expenses, bookings) -> None:
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
            "moss_expense_uuid",
            _plannable(ready_expenses, "_transaction_ref"),
        )
    expense_ids = _id_map(connection, _TABLE_EXPENSES, ["moss_expense_uuid"])
    ready_bookings = []
    for row in bookings:
        key = _expense_key(row)
        if key not in expense_ids or _transaction_key(row) not in known:
            continue
        row["moss_expense_id"] = expense_ids[key]
        row["moss_transaction_id"] = known[_transaction_key(row)]
        ready_bookings.append(row)
    if ready_bookings:
        _plan_for(
            connection,
            ctx,
            _TABLE_BOOKINGS,
            ["moss_expense_id", "sub_row_number"],
            _plannable(ready_bookings, "_transaction_ref", "_expense_ref"),
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
    source_files = _source_file_by_transaction(by_kind)

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        _require_object_uuid_column(ro_conn)

        # WHICH stored row each transaction is -- before any plan, because a
        # Transaction ID that changed would otherwise look like a new row.
        resolved = _resolve_transactions(
            transactions, _load_stored_transactions(ro_conn)
        )
        origin = _apply_resolution(
            resolved, transactions, expenses, bookings, source_files
        )
        _remember_source_files(origin, _TABLE_TRANSACTIONS, transactions)
        _remember_source_files(origin, _TABLE_EXPENSES, expenses)

        transaction_plan = _plan_for(
            ro_conn,
            ctx,
            _TABLE_TRANSACTIONS,
            "moss_object_uuid",
            _plannable(transactions, "_transaction_ref"),
            generated_key_columns=("moss_object_uuid",),
        )

        # L2/L3 are keyed on their own natural keys but need the surrogate
        # parent ids, which exist only after L1 has been applied. Planning them
        # up front is therefore possible only for already-known transactions --
        # enough to make --dry-run informative.
        if ctx.dry_run:
            known = _id_map(ro_conn, _TABLE_TRANSACTIONS, ["moss_object_uuid"])
            _dry_run_lower_levels(ro_conn, ctx, known, expenses, bookings)
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

        transaction_ids = _id_map(rw_conn, _TABLE_TRANSACTIONS, ["moss_object_uuid"])
        for row in expenses:
            row["moss_transaction_id"] = transaction_ids[_transaction_key(row)]
        expense_owners = _owner_ids(expenses, _TABLE_EXPENSES)
        expense_plan = _plan_for(
            rw_conn,
            ctx,
            _TABLE_EXPENSES,
            "moss_expense_uuid",
            _plannable(expenses, "_transaction_ref"),
        )
        _apply(expense_plan, rw_conn, _TABLE_EXPENSES, ctx.start_time)

        expense_ids = _id_map(rw_conn, _TABLE_EXPENSES, ["moss_expense_uuid"])
        for row in bookings:
            row["moss_expense_id"] = expense_ids[_expense_key(row)]
            row["moss_transaction_id"] = transaction_ids[_transaction_key(row)]
        _remember_source_files(origin, _TABLE_BOOKINGS, bookings)
        booking_owners = _owner_ids(bookings, _TABLE_BOOKINGS)
        booking_plan = _plan_for(
            rw_conn,
            ctx,
            _TABLE_BOOKINGS,
            ["moss_expense_id", "sub_row_number"],
            _plannable(bookings, "_transaction_ref", "_expense_ref"),
        )
        _apply(booking_plan, rw_conn, _TABLE_BOOKINGS, ctx.start_time)

        _post_run_checks(
            rw_conn,
            _touched_transaction_ids(
                (transaction_plan, transaction_ids, _TABLE_TRANSACTIONS),
                (expense_plan, expense_owners, _TABLE_EXPENSES),
                (booking_plan, booking_owners, _TABLE_BOOKINGS),
            ),
        )

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
