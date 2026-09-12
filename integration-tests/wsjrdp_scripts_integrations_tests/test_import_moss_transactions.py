"""Integration tests for accounting_tools/import_moss_transactions.py -- what
survives a Moss export-profile change: the STABLE KEYS of the three-level Moss
model (moss_object_uuid on L1, moss_expense_uuid on L2, (moss_expense_id,
sub_row_number) on L3), the identity resolution behind them and the
PROFILE-DEPENDENT COLUMNS, which a leaner export layout must not clear.

These tests WRITE to the independent integration-testing database
``hitobito_wsjrdp_scripts_integration_testing`` (see the ``ctx`` /
``integration_testing_ctx`` fixtures in integration-tests/conftest.py and the
AGENTS.md section on integration tests). Every test body runs inside ONE
transaction that is ALWAYS rolled back, so nothing survives a test -- never
assert absolute ``id`` values, the sequences advance regardless.

The three Moss tables do not come from a production dump; the ``moss_tables``
fixture creates them from fixtures/moss_tables_schema.sql (committed once, so
the subprocess tests see them too) when they are missing. Regenerate that file
after a wagon migration touching the Moss tables:

    docker exec development-postgres-1 pg_dump -U hitobito -d hitobito_development \\
        --schema-only --no-owner --no-privileges --no-comments \\
        -t moss_transactions -t moss_expenses -t moss_bookings

The Moss exports are synthetic: invented uuid5 ids, placeholder names, tiny
amounts and dates in 2099 -- no production data is involved.
"""

from __future__ import annotations

import csv
import datetime
import decimal
import importlib.util
import logging
import pathlib
import re
import subprocess
import sys
import uuid

import psycopg
import pytest
import pytest_wsjrdp2027


ROOT = pathlib.Path(__file__).resolve().parents[2]
IMPORTER_PATH = ROOT / "accounting_tools" / "import_moss_transactions.py"
SCHEMA_SQL = pathlib.Path(__file__).parent / "fixtures" / "moss_tables_schema.sql"


def _load_importer():
    """The importer as a module. Its module level only defines things -- the
    CLI sits behind ``if __name__ == "__main__"`` -- so importing it is free."""
    spec = importlib.util.spec_from_file_location(
        "import_moss_transactions", IMPORTER_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {IMPORTER_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


importer = _load_importer()

TABLE_TRANSACTIONS = importer._TABLE_TRANSACTIONS
TABLE_EXPENSES = importer._TABLE_EXPENSES
TABLE_BOOKINGS = importer._TABLE_BOOKINGS
#: The deferrable unique constraint that carries the L3 key.
SUB_ROW_CONSTRAINT = importer._SUB_ROW_CONSTRAINT
SUB_ROW_CONSTRAINT_MIGRATION = importer._SUB_ROW_CONSTRAINT_MIGRATION
IMPORTER_LOGGER = "import_moss_transactions"
#: The plan builder reports the stored values it kept under its own name.
PLAN_LOGGER = importer.SingleTableUpsertPlanBuilder.__module__

#: The two per-column lines of a plan preview (see _log_plan_summary).
UPDATE_COLUMNS = "UPDATE columns"
KEPT_BLANK = "kept stored values for blank input"

#: What one full import of the synthetic exports produces (see write_exports).
L1_ROWS = 4  # card, reimbursement, invoice, top-up
L2_ROWS = 5  # 3 shells + the reimbursement's 2 expenses
L3_ROWS = 8  # 2 card splits + 3 reimbursement splits + 2 invoice lines + 1 top-up

# Naive on purpose, like the timestamp columns; see test_single_table_upsert_plan.
NOW = datetime.datetime(2099, 3, 10, 12, 0, 0)  # noqa: DTZ001
LATER = datetime.datetime(2099, 3, 11, 12, 0, 0)  # noqa: DTZ001


# ============================================================ synthetic ids
# A card payment, a reimbursement and an invoice are identified by the OBJECT
# they settle, so those ids are the same in every export profile. The wallet
# payouts (reimbursement, invoice, top-up) are the ones Moss re-numbers per
# profile -- exactly what the identity resolution has to survive.

NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://example.invalid/moss-import-tests")


def fixed_uuid(name: str) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, name)


def wallet_uuid(profile: str, name: str) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, f"{profile}:{name}")


CARD_UUID = fixed_uuid("card-transaction")
REIMBURSEMENT_UUID = fixed_uuid("reimbursement")
EXPENSE_A_UUID = fixed_uuid("reimbursement-expense-a")
EXPENSE_B_UUID = fixed_uuid("reimbursement-expense-b")
INVOICE_UUID = fixed_uuid("invoice")

CARD_PAYMENT_DATE = datetime.date(2099, 2, 27)
CARD_BOOKING_DATE = datetime.date(2099, 2, 28)
REIMBURSEMENT_PAYMENT_DATE = datetime.date(2099, 3, 1)
REIMBURSEMENT_BOOKING_DATE = datetime.date(2099, 3, 2)
INVOICE_PAYMENT_DATE = datetime.date(2099, 3, 3)
INVOICE_BOOKING_DATE = datetime.date(2099, 3, 4)
#: A top-up settles on the day it books, in every profile -- the payment day
#: the profiles disagree about is the two payouts'.
TOP_UP_BOOKING_DATE = datetime.date(2099, 3, 6)
TOP_UP_AMOUNT = "25.00"
#: A second top-up amount: a content change on the single booking of a top-up
#: expense, which is what puts that expense on the reorder candidate list.
OTHER_TOP_UP_AMOUNT = "26.00"

MOSS_BALANCE_ACCOUNT = "36100"  # CLEARING
CASH_IN_TRANSIT_ACCOUNT = "13720"  # TRANSIT
EXPENSE_ACCOUNT_ONE = "61000"  # EXPENSE
EXPENSE_ACCOUNT_TWO = "62000"  # EXPENSE
EXPENSE_ACCOUNT_THREE = "63000"  # EXPENSE
CREDITOR_ACCOUNT_ONE = "700099"  # CREDITOR
CREDITOR_ACCOUNT_TWO = "700098"  # CREDITOR
COST_CENTER_ONE = "3100"
COST_CENTER_TWO = "3200"
#: The expense account's NAME, which only the balance export carries (its
#: `Category`). It is mirrored onto the booking of the line the balance row is
#: paired with, so a wrong pairing makes it contradict that booking's account.
ACCOUNT_NAMES = {
    EXPENSE_ACCOUNT_ONE: "Test expense account one",
    EXPENSE_ACCOUNT_TWO: "Test expense account two",
    EXPENSE_ACCOUNT_THREE: "Test expense account three",
}
#: The invoice number both exports carry; the log names an invoice by it.
INVOICE_NUMBER = "TEST-99.99.99"

# ------------------------------------------------- the reimbursement splits
# What the SPLIT REORDER heuristic works on: the splits of ONE reimbursement
# expense, which Moss may export in another order next time. Every variant
# below is that expense's split list, the rest of the export unchanged -- the
# balance row of an expense is its splits' total, so the sum invariant holds
# in every one of them. The card payment's splits and the invoice's lines are
# keyed the same way and have a reorder of their own, written as the
# ``swap_card_splits`` / ``swap_invoice_lines`` arguments of write_exports.

#: The stored order: two splits that differ in account, cost center and amount.
SPLITS_NORMAL = "normal"
#: The same two splits, exchanged in the CSV -- a reorder, and nothing else.
SPLITS_SWAPPED = "swapped"
#: Two splits sharing cost center, account AND amount: which row is which
#: cannot be decided, so they must never be renumbered.
SPLITS_AMBIGUOUS = "ambiguous"
#: One split really moves to another expense account: a content change, not a
#: permutation of the stored splits.
SPLITS_CHANGED_ACCOUNT = "changed account"
#: The swapped order plus a third split: the two sides no longer have the same
#: count, so the positional plan applies.
SPLITS_SWAPPED_PLUS_ONE = "swapped plus one"

# ---------------------------------------------- the reimbursement's expenses
# One level up from the splits: WHICH expense a balance row belongs to. The
# reimbursement export carries the expense with its splits, the balance export
# one row per expense -- what was paid for it and the number it is stored
# under -- and the two are paired by amount and text, never by position (see
# the REIMBURSEMENT EXPENSE PAIRING section of the importer).

#: Two expenses that differ in amount AND in text: every pairing step can tell
#: them apart.
EXPENSES_NORMAL = "normal"
#: The same two expenses, exchanged in BOTH exports -- what Moss reordering the
#: expenses themselves looks like: the pairing is the export order again, but
#: each expense comes back under the other's number.
EXPENSES_SWAPPED = "swapped"
#: Two expenses sharing their amount AND their text: which balance row belongs
#: to which of them cannot be decided on any step.
EXPENSES_AMBIGUOUS = "ambiguous"

#: The text an expense and its balance row carry: the expense's own Parent
#: Booking Text, which the balance export repeats in its Note.
EXPENSE_A_TEXT = "Test expense A"
EXPENSE_B_TEXT = "Test expense B"
#: What both expenses say in the ambiguous variant.
SAME_EXPENSE_TEXT = "Test expense"
#: What a balance export saying something else in its Note looks like: a text
#: the reimbursement export does not have, on every row.
OTHER_EXPENSE_TEXT = "Test other expense"

#: What each of the two expenses costs, as the expense row stores it: negative,
#: because the money leaves the wallet. Expense A has one split, expense B two.
EXPENSE_AMOUNTS = {
    EXPENSE_A_UUID: decimal.Decimal("-4.00"),
    EXPENSE_B_UUID: decimal.Decimal("-9.00"),
}
EXPENSE_TEXTS = {
    EXPENSE_A_UUID: EXPENSE_A_TEXT,
    EXPENSE_B_UUID: EXPENSE_B_TEXT,
}

# ------------------------------------------------------- the payout details
# What the export profiles disagree about besides the ids (the importer's
# PROFILE-DEPENDENT COLUMNS): the account a payout went to, the finance user
# who released it with their team, and the account that funded the wallet.

#: The account the reimbursement is paid to, per profile. "P4" is the P1
#: layout with a CORRECTED account -- a real value meeting a protected one.
REIMBURSEMENT_RECIPIENT: dict[str, tuple[str, str]] = {
    "P1": ("TEST-ACCOUNT-1", "TESTBIC1"),
    # never exported by the lean layout, and exactly what it must not clear
    "P2": ("TEST-ACCOUNT-1", "TESTBIC1"),
    "P3": ("TEST-ACCOUNT-1", "TESTBIC1"),
    "P4": ("TEST-ACCOUNT-4", "TESTBIC4"),
}
INVOICE_RECIPIENT = ("TEST-ACCOUNT-2", "TESTBIC2")
#: The account holder each payout names in its "Reason for Purchase".
REIMBURSEMENT_PAYEE = "Test Person"
INVOICE_PAYEE = "Test Supplier"
#: Cardholder / Team Name of a payout row in the rich layout: the finance user
#: who released the payment and that user's team.
PAYOUT_USER_NAME = "Test Finance Person"
PAYOUT_TEAM_NAME = "Test Finance Team"
#: What the lean layout puts in the same Team Name cell of an invoice payout:
#: the invoice's OWN team. Which of the two a cell means is unknowable, which
#: is why both stay raw.
OTHER_TEAM_NAME = "Test Other Team"
#: The wallet's funding account: the same organisation in both layouts, the
#: account behind it spelled differently.
TOP_UP_ORGANISATION = "Test Organisation"
TOP_UP_REASON_RICH = f"{TOP_UP_ORGANISATION} - TEST-IBAN-0000-0000"
TOP_UP_REASON_LEAN = f"{TOP_UP_ORGANISATION}; TESTCODE0"
#: The profile whose export layout leaves the payout details out.
LEAN_PROFILE = "P2"


# ====================================================== synthetic Moss exports
# Only the columns the importer actually reads are written; every one of them
# is classified in the importer's column maps, so a run logs no "unclassified
# CSV column" warning and `caplog` stays readable.

CARD_HEADERS = (
    "Transaction ID",
    "Sub-row Number",
    "Transaction State",
    "Transaction Type",
    "Payment Date",
    "Booking Date",
    "Total Amount",
    "Home Currency",
    "Home Amount",
    "Account Number",
    "Cost Center - Number",
    "Note",
    "Parent Booking Text",
    "Merchant Name",
    "Cardholder",
    "Moss Balance Account",
    "Cash in Transit Account",
)

BALANCE_HEADERS = (
    "Transaction ID",
    "Sub-row Number",
    "Transaction State",
    "Transaction Type",
    "Payment Date",
    "Booking Date",
    "Amount",
    # What an invoice line is paired on: the account and the amount in the home
    # currency, next to the account's name in "Category".
    "Home Amount",
    "Account Number",
    "Invoice Number",
    "Currency",
    "Note",
    "Payment Reference",
    "Recipient Account Number",
    "Recipient Bank Code",
    "Cardholder",
    "Team Name",
    "Supplier Account",
    "Moss Balance Account",
    "Cash in Transit Account",
    "Reason for Purchase",
    "Category",
    "Linked Reimbursement ID",
    "Linked Invoice ID",
)

REIMBURSEMENT_HEADERS = (
    "Unique Reimbursement ID",
    "Unique Expense ID",
    "Row Number",
    "Sub-row Number",
    "Expense Name",
    "Expense type",
    "Purchased On",
    "Parent Booking Text",
    "Expense Account",
    "Cost Center - Name",
    "Cost Carrier - Number",
    "Expense Description",
    "Amount",
    "Submitted On",
    "Reimbursement Name",
    "Creation date",
    "Submitted By",
)

INVOICE_HEADERS = (
    "Invoice ID",
    "Invoice Number",
    "Row Number",
    "Sub-row Number",
    "Expense Account - Number",
    "Cost Center - Number",
    "Cost Carrier - Number",
    "Booking Text",
    "Parent Booking Text",
    "Amount",
    "Amount in Home Currency",
    "Invoice Date",
    "Due Date",
    "Delivery Date",
    "Submitted Date",
    "Invoice Status",
    "Submitted By",
)


def _write_csv(path: pathlib.Path, headers, rows) -> str:
    """One Moss export: ";" separated, "." decimals, UTF-8 with a BOM."""
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(headers), delimiter=";", lineterminator="\r\n"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in headers})
    return str(path)


def _card_rows(*, swapped: bool = False) -> list[dict]:
    """One card payment with two splits; profile-independent in every column.

    ``swapped`` lists the two splits in the other order, so each gets the
    other's Sub-row Number while account, amount and text travel with it: the
    card export's own reorder. Nothing above the splits moves with them -- the
    transaction's columns are all in ``shared``."""
    shared = {
        "Transaction ID": str(CARD_UUID),
        "Transaction State": "Completed",
        "Transaction Type": "Card Payment",
        "Payment Date": CARD_PAYMENT_DATE.isoformat(),
        "Booking Date": CARD_BOOKING_DATE.isoformat(),
        "Total Amount": "-3.00",
        "Home Currency": "EUR",
        "Parent Booking Text": "Test card payment",
        "Merchant Name": "Test Merchant",
        "Cardholder": "Test Person",
        "Moss Balance Account": MOSS_BALANCE_ACCOUNT,
        "Cash in Transit Account": CASH_IN_TRANSIT_ACCOUNT,
        "Cost Center - Number": "3100",
    }
    splits = [
        {
            "Home Amount": "-1.00",
            "Account Number": EXPENSE_ACCOUNT_ONE,
            "Note": "Test card split one",
        },
        {
            "Home Amount": "-2.00",
            "Account Number": EXPENSE_ACCOUNT_TWO,
            "Note": "Test card split two",
        },
    ]
    if swapped:
        splits.reverse()
    return [
        shared | split | {"Sub-row Number": str(number)}
        for number, split in enumerate(splits, start=1)
    ]


def _balance_rows(
    profile: str,
    *,
    reimbursement_payout: uuid.UUID,
    rich_profile: bool,
    reimbursement_recipient: tuple[str, str],
    reimbursement_expense_rows: list[dict],
    invoice_rows: list[dict],
    top_up_amount: str,
) -> list[dict]:
    """The wallet movements: the reimbursement payout (one row per expense,
    see _expense_payout_rows), the invoice payout (one row per line, see
    _invoice_balance_rows) and the top-up.

    ``rich_profile`` is the layout of every profile but the lean one: both
    payouts carry the account that was paid, the finance user who released the
    payment with that user's team, a payment date of their own, and the
    top-up's funding account as an IBAN-like text. The lean layout leaves
    recipient account and cardholder empty, names the invoice's OWN team in
    the same Team Name cell, repeats the booking date as the payment date and
    spells the funding account as a short code. A top-up carries none of the
    payout details in either layout."""
    invoice_payout = wallet_uuid(profile, "invoice-payout")
    top_up = wallet_uuid(profile, "top-up")

    def movement(uuid_value, sub_row, amount, booking, **extra) -> dict:
        return {
            "Transaction ID": str(uuid_value),
            "Sub-row Number": str(sub_row),
            "Transaction State": "Completed",
            "Transaction Type": "Balance Movement",
            "Booking Date": booking.isoformat(),
            "Amount": amount,
            "Currency": "EUR",
            "Moss Balance Account": MOSS_BALANCE_ACCOUNT,
            "Cash in Transit Account": CASH_IN_TRANSIT_ACCOUNT,
            **extra,
        }

    def payout(
        uuid_value, sub_row, amount, payment, booking, account, lean_team, **extra
    ) -> dict:
        details = (
            {
                "Payment Date": payment.isoformat(),
                "Recipient Account Number": account[0],
                "Recipient Bank Code": account[1],
                "Cardholder": PAYOUT_USER_NAME,
                "Team Name": PAYOUT_TEAM_NAME,
            }
            if rich_profile
            else {"Payment Date": booking.isoformat(), "Team Name": lean_team}
        )
        return movement(uuid_value, sub_row, amount, booking, **details, **extra)

    reimbursement = {
        "Linked Reimbursement ID": str(REIMBURSEMENT_UUID),
        "Supplier Account": CREDITOR_ACCOUNT_ONE,
        "Payment Reference": "Test reimbursement payout",
        "Reason for Purchase": f"{REIMBURSEMENT_PAYEE}; ; -",
        "Category": "Test expense account",
    }
    invoice = {
        "Linked Invoice ID": str(INVOICE_UUID),
        "Invoice Number": INVOICE_NUMBER,
        "Supplier Account": CREDITOR_ACCOUNT_TWO,
        "Payment Reference": "Test invoice payout",
        "Reason for Purchase": f"{INVOICE_PAYEE}; ; -",
    }
    return [
        *(
            payout(
                reimbursement_payout,
                sub_row,
                row["Amount"],
                REIMBURSEMENT_PAYMENT_DATE,
                REIMBURSEMENT_BOOKING_DATE,
                reimbursement_recipient,
                # the lean layout knows no team for a reimbursement payout
                "",
                **reimbursement,
                **{name: value for name, value in row.items() if name != "Amount"},
            )
            for sub_row, row in enumerate(reimbursement_expense_rows, start=1)
        ),
        *(
            payout(
                invoice_payout,
                sub_row,
                row["Amount"],
                INVOICE_PAYMENT_DATE,
                INVOICE_BOOKING_DATE,
                INVOICE_RECIPIENT,
                OTHER_TEAM_NAME,
                **invoice,
                **{name: value for name, value in row.items() if name != "Amount"},
            )
            for sub_row, row in enumerate(invoice_rows, start=1)
        ),
        movement(
            top_up,
            1,
            top_up_amount,
            TOP_UP_BOOKING_DATE,
            **{
                "Payment Date": TOP_UP_BOOKING_DATE.isoformat(),
                "Note": "Test wallet top-up",
                "Reason for Purchase": (
                    TOP_UP_REASON_RICH if rich_profile else TOP_UP_REASON_LEAN
                ),
                "Category": "Test wallet top-up",
            },
        ),
    ]


def _expense_b_splits(variant: str) -> list[dict]:
    """The splits of expense B, in the order the export lists them: account,
    cost center, amount and the split's own text. `variant` is one of the
    SPLITS_* constants above; every variant keeps the expense's total, except
    the one that adds a split."""
    one = {
        "Expense Account": EXPENSE_ACCOUNT_ONE,
        "Cost Center - Name": COST_CENTER_ONE,
        "Expense Description": "Test expense B split one",
        "Amount": "4.00",
    }
    two = {
        "Expense Account": EXPENSE_ACCOUNT_TWO,
        "Cost Center - Name": COST_CENTER_TWO,
        "Expense Description": "Test expense B split two",
        "Amount": "5.00",
    }
    if variant == SPLITS_SWAPPED:
        return [two, one]
    if variant == SPLITS_SWAPPED_PLUS_ONE:
        third = {
            "Expense Account": EXPENSE_ACCOUNT_THREE,
            "Cost Center - Name": COST_CENTER_TWO,
            "Expense Description": "Test expense B split three",
            "Amount": "1.00",
        }
        return [two, one, third]
    if variant == SPLITS_AMBIGUOUS:
        same = {
            "Expense Account": EXPENSE_ACCOUNT_ONE,
            "Cost Center - Name": COST_CENTER_ONE,
            "Amount": "4.50",
        }
        return [
            same | {"Expense Description": "Test expense B split two"},
            same | {"Expense Description": "Test expense B split one"},
        ]
    if variant == SPLITS_CHANGED_ACCOUNT:
        return [one, two | {"Expense Account": EXPENSE_ACCOUNT_THREE}]
    return [one, two]


def _reimbursement_rows(
    variant: str = SPLITS_NORMAL, expenses: str = EXPENSES_NORMAL
) -> list[dict]:
    """Expense A with one split, expense B with several -- the level the
    balance export cannot see. Row Number is the file-global counter, Sub-row
    Number the split's position inside its own expense: the L3 key.

    ``expenses`` is one of the EXPENSES_* constants: which expenses the
    reimbursement consists of and in which order it lists them."""
    shared = {
        "Unique Reimbursement ID": str(REIMBURSEMENT_UUID),
        "Submitted On": "2099-02-25",
        "Creation date": "2099-02-20",
        "Reimbursement Name": "Test reimbursement",
        "Submitted By": "Test Person",
        "Expense type": "General",
    }
    expense_a = {
        "Unique Expense ID": str(EXPENSE_A_UUID),
        "Expense Name": "Test expense A",
        "Purchased On": "2099-02-18",
        "Parent Booking Text": EXPENSE_A_TEXT,
    }
    expense_b = {
        "Unique Expense ID": str(EXPENSE_B_UUID),
        "Expense Name": "Test expense B",
        "Purchased On": "2099-02-19",
        "Parent Booking Text": EXPENSE_B_TEXT,
    }
    a_splits = [
        {
            "Expense Account": EXPENSE_ACCOUNT_ONE,
            "Cost Center - Name": COST_CENTER_ONE,
            "Expense Description": "Test expense A split one",
            "Amount": "4.00",
        }
    ]
    b_splits = _expense_b_splits(variant)
    if expenses == EXPENSES_AMBIGUOUS:
        # The one thing a balance row could tell them apart by -- what the
        # expense cost and what it is called -- is the same on both.
        expense_a = expense_a | {"Parent Booking Text": SAME_EXPENSE_TEXT}
        expense_b = expense_b | {"Parent Booking Text": SAME_EXPENSE_TEXT}
        b_splits = [dict(a_splits[0], **{"Expense Description": "Test split"})]
    ordered = [(expense_a, a_splits), (expense_b, b_splits)]
    if expenses == EXPENSES_SWAPPED:
        ordered.reverse()
    rows: list[dict] = []
    for header, splits in ordered:
        for number, split in enumerate(splits, start=1):
            rows.append(
                shared
                | header
                | split
                | {"Row Number": str(len(rows) + 1), "Sub-row Number": str(number)}
            )
    return rows


def _expense_payout_rows(rows: list[dict]) -> list[dict]:
    """The balance export's view of the reimbursement: ONE row per expense, in
    export order, carrying what the expense cost (negative, because the money
    leaves the wallet) and the expense's own text in its Note. Keeping the two
    sides in step here is what makes every variant satisfy the sum invariant --
    and what gives the pairing the two keys it works on."""
    totals: dict[str, dict] = {}
    for row in rows:
        entry = totals.setdefault(
            row["Unique Expense ID"],
            {"total": decimal.Decimal(0), "Note": row["Parent Booking Text"]},
        )
        entry["total"] += decimal.Decimal(row["Amount"])
    return [
        {"Amount": f"{-entry['total']:.2f}", "Note": entry["Note"]}
        for entry in totals.values()
    ]


#: What the invoice's two lines SAY, without where they sit. An invoice arrives
#: in two exports at once -- the line contributes account, cost center and text,
#: the balance row the amount that was paid and the account's NAME -- and the
#: two are paired by content, so either export may list them in its own order
#: (``swap_invoice_lines`` resp. ``swap_invoice_balance_rows``).
INVOICE_LINES: tuple[dict, ...] = (
    {
        "Expense Account - Number": EXPENSE_ACCOUNT_ONE,
        "Cost Center - Number": COST_CENTER_ONE,
        "Booking Text": "Test invoice line one",
        "Amount": "5.00",
    },
    {
        "Expense Account - Number": EXPENSE_ACCOUNT_TWO,
        "Cost Center - Number": COST_CENTER_TWO,
        "Booking Text": "Test invoice line two",
        "Amount": "6.00",
    },
)
#: Two lines on the SAME expense account, told apart only by their cost center
#: -- the one attribute the balance export has no column for. Written with
#: ``balance_names_the_account=False``, so that neither the account nor the
#: (account, amount) step can decide their pairing and only the stored bookings
#: can.
INVOICE_LINES_SAME_ACCOUNT: tuple[dict, ...] = (
    {
        "Expense Account - Number": EXPENSE_ACCOUNT_ONE,
        "Cost Center - Number": COST_CENTER_ONE,
        "Booking Text": "Test invoice line one",
        "Amount": "5.00",
    },
    {
        "Expense Account - Number": EXPENSE_ACCOUNT_ONE,
        "Cost Center - Number": COST_CENTER_TWO,
        "Booking Text": "Test invoice line two",
        "Amount": "6.00",
    },
)
#: Two lines sharing account AND amount: which balance row belongs to which
#: line is genuinely undecidable, on every step.
INVOICE_LINES_AMBIGUOUS: tuple[dict, ...] = (
    INVOICE_LINES_SAME_ACCOUNT[0],
    INVOICE_LINES_SAME_ACCOUNT[1] | {"Amount": "5.00"},
)


def _line_amounts(lines) -> list[decimal.Decimal]:
    """What each line costs, as the booking stores it: negative, because the
    money leaves the wallet."""
    return [-decimal.Decimal(line["Amount"]) for line in lines]


def _invoice_rows(lines, *, swapped: bool = False) -> list[dict]:
    """The invoice export: one row per line -- where an invoice's cost centers
    come from. Row Number and Sub-row Number follow the export order, so
    ``swapped`` gives the two lines each other's number."""
    shared = {
        "Invoice ID": str(INVOICE_UUID),
        "Invoice Number": INVOICE_NUMBER,
        "Parent Booking Text": "Test invoice",
        "Invoice Date": "2099-02-15",
        "Due Date": "2099-03-15",
        "Delivery Date": "2099-02-14",
        "Submitted Date": "2099-02-16",
        "Invoice Status": "Completed",
        "Submitted By": "Test Person",
    }
    ordered = list(reversed(lines)) if swapped else list(lines)
    return [
        shared
        | line
        | {
            "Row Number": str(number),
            "Sub-row Number": str(number),
            # EUR here, so the amount pairs a balance row to this line.
            "Amount in Home Currency": line["Amount"],
        }
        for number, line in enumerate(ordered, start=1)
    ]


def _invoice_balance_rows(lines, *, swapped: bool, with_account: bool) -> list[dict]:
    """The balance export: one row per invoice line, carrying what was paid for
    it and -- where the profile names it -- that line's expense account with
    its NAME. ``swapped`` lists the rows in the other order while the invoice
    export keeps its own: the reordering one profile does to one export only."""
    ordered = list(reversed(lines)) if swapped else list(lines)
    return [
        {
            "Amount": f"{-decimal.Decimal(line['Amount']):.2f}",
            "Home Amount": f"{-decimal.Decimal(line['Amount']):.2f}",
            "Account Number": (
                line["Expense Account - Number"] if with_account else ""
            ),
            "Category": (
                ACCOUNT_NAMES[line["Expense Account - Number"]] if with_account else ""
            ),
        }
        for line in ordered
    ]


def write_exports(
    directory: pathlib.Path,
    profile: str,
    *,
    reimbursement_payout: uuid.UUID | None = None,
    splits: str = SPLITS_NORMAL,
    reimbursement_expenses: str = EXPENSES_NORMAL,
    swap_reimbursement_balance_rows: bool = False,
    reimbursement_notes_disagree: bool = False,
    swap_card_splits: bool = False,
    swap_invoice_lines: bool = False,
    swap_invoice_balance_rows: bool = False,
    invoice_lines: tuple[dict, ...] = INVOICE_LINES,
    balance_names_the_account: bool = True,
    top_up_amount: str = TOP_UP_AMOUNT,
) -> list[str]:
    """The four Moss exports of ONE id profile, as file paths in the order the
    CLI takes them (card, balance, reimbursement, invoice; the importer detects
    each kind from its columns anyway).

    ``profile`` names the wallet id set ("P1" ... "P4"). "P2" is also the lean
    export layout (no recipient account, no cardholder, payment date ==
    booking date); "P1", "P3" and "P4" share the rich layout. "P1" and "P3"
    differ from each other in their wallet ids ONLY, which is what makes a P1
    re-import after P3 a genuine no-op; "P4" additionally pays the
    reimbursement to another account.

    ``splits`` is what the reimbursement's second expense looks like -- one of
    the SPLITS_* constants; the balance rows follow it, so every variant is a
    consistent export.

    ``reimbursement_expenses`` is which expenses the reimbursement consists of
    -- one of the EXPENSES_* constants, EXPENSES_SWAPPED being a reorder BOTH
    exports follow. ``swap_reimbursement_balance_rows`` instead reorders only
    the balance rows of the reimbursement payout, so each expense's amount and
    Sub-row Number arrive at the other position while the reimbursement export
    keeps its order. ``reimbursement_notes_disagree`` makes those balance rows
    name a text the reimbursement export does not have.

    ``swap_card_splits`` and ``swap_invoice_lines`` are the same reorder on the
    other two kinds: the card payment's two splits, resp. the invoice's two
    lines, exchange their Sub-row Number -- an invoice's balance rows follow
    that order. ``swap_invoice_balance_rows`` instead reorders ONLY the balance
    export, which says nothing about the lines and must therefore change
    nothing at all.

    ``invoice_lines`` is what the invoice consists of (one of the INVOICE_LINES*
    above) and ``balance_names_the_account`` whether its balance rows name the
    expense account. ``top_up_amount`` is what the wallet was funded with -- the
    only content a top-up's single booking has.
    """
    directory.mkdir(parents=True, exist_ok=True)
    payout = reimbursement_payout or wallet_uuid(profile, "reimbursement-payout")
    reimbursement_rows = _reimbursement_rows(splits, reimbursement_expenses)
    expense_rows = _expense_payout_rows(reimbursement_rows)
    if reimbursement_notes_disagree:
        expense_rows = [dict(row, Note=OTHER_EXPENSE_TEXT) for row in expense_rows]
    if swap_reimbursement_balance_rows:
        expense_rows.reverse()
    balance_rows = _balance_rows(
        profile,
        reimbursement_payout=payout,
        rich_profile=profile != LEAN_PROFILE,
        reimbursement_recipient=REIMBURSEMENT_RECIPIENT[profile],
        reimbursement_expense_rows=expense_rows,
        invoice_rows=_invoice_balance_rows(
            invoice_lines,
            swapped=swap_invoice_lines != swap_invoice_balance_rows,
            with_account=balance_names_the_account,
        ),
        top_up_amount=top_up_amount,
    )
    return [
        _write_csv(
            directory / f"transactions_{profile}.csv",
            CARD_HEADERS,
            _card_rows(swapped=swap_card_splits),
        ),
        _write_csv(
            directory / f"balance-movements_{profile}.csv",
            BALANCE_HEADERS,
            balance_rows,
        ),
        _write_csv(
            directory / f"reimbursements_{profile}.csv",
            REIMBURSEMENT_HEADERS,
            reimbursement_rows,
        ),
        _write_csv(
            directory / f"invoices_{profile}.csv",
            INVOICE_HEADERS,
            _invoice_rows(invoice_lines, swapped=swap_invoice_lines),
        ),
    ]


# =================================================================== fixtures


@pytest.fixture
def moss_tables(integration_testing_ctx):
    """The three Moss tables in the integration-testing DB, created from the
    DDL fixture and COMMITTED once -- the subprocess tests need them in the
    committed state, and every test body rolls its own writes back."""
    conn = integration_testing_ctx.hitobito_psycopg_connection(read_only=False)
    found = conn.execute("SELECT to_regclass('public.moss_transactions')").fetchone()
    if found[0] is None:
        conn.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
        conn.commit()
        return
    has_identity = conn.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name = 'moss_transactions'"
        " AND column_name = 'moss_object_uuid'"
    ).fetchone()
    if has_identity is None:
        pytest.skip(
            "integration DB has moss tables without moss_object_uuid; "
            "drop them or restore a newer dump"
        )


@pytest.fixture
def rw_conn(moss_tables, integration_testing_ctx):
    """The cached read-write connection of the verified integration-testing
    context. The test body runs in ONE transaction that is always rolled
    back -- so every assertion has to read through THIS connection, another
    session cannot see the uncommitted rows."""
    conn = integration_testing_ctx.hitobito_psycopg_connection(read_only=False)
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


# ==================================================================== harness
# main() cannot be reused here: it builds its own WsjRdpContext and its own
# connections, and it commits. These two mirror its in-transaction steps with
# the importer's own functions on a GIVEN connection.


def _plan_transactions(conn, ctx, files):
    """Everything main() does before it branches on ctx.dry_run: read, build,
    verify the sum invariant, resolve the identities, remember the provenance
    of L1/L2 and plan L1. The blank protection of the profile-dependent columns
    comes with the table, so this harness asks for it as little as main() does.
    Returns (L1 plan, expenses, bookings, origin)."""
    by_kind = importer._read_all(list(files))
    transactions, expenses, bookings, pairings, reimbursements = (
        importer._build_records(by_kind)
    )
    importer._verify_sum_invariant(transactions, expenses, bookings)
    source_files = importer._source_file_by_transaction(by_kind)

    importer._require_wagon_schema(conn)
    resolved = importer._resolve_transactions(
        transactions, importer._load_stored_transactions(conn)
    )
    origin = importer._apply_resolution(
        resolved, transactions, expenses, bookings, source_files
    )
    importer._resolve_invoice_pairings(conn, pairings)
    importer._resolve_reimbursement_pairings(conn, reimbursements)
    importer._remember_source_files(origin, TABLE_TRANSACTIONS, transactions)
    importer._remember_source_files(origin, TABLE_EXPENSES, expenses)

    transaction_plan = importer._plan_for(
        conn,
        ctx,
        TABLE_TRANSACTIONS,
        "moss_object_uuid",
        importer._plannable(transactions, "_transaction_ref"),
        generated_key_columns=("moss_object_uuid",),
    )
    return transaction_plan, expenses, bookings, origin


def run_import(conn, ctx, files, *, now, no_updated_at=False):
    """The apply path of main(), step for step, on `conn`. Returns the three
    plans (L1, L2, L3) -- the L3 one being the plan that was APPLIED, i.e. the
    recomputed plan when splits had to be renumbered.

    main() previews L2/L3 before this path (see run_preview and the CLI test of
    the order); the harness leaves that out, so each level appears in the log
    of one run exactly once and a test reads the plan that was applied.

    `no_updated_at` is the CLI flag of that name, which main() reads from its
    parsed arguments and hands to the same four calls."""
    transaction_plan, expenses, bookings, origin = _plan_transactions(conn, ctx, files)
    wallet_id = importer._wallet_fin_account_id(conn)
    importer._apply(
        transaction_plan,
        conn,
        TABLE_TRANSACTIONS,
        now,
        insert_only={"fin_account_id": wallet_id} if wallet_id else None,
        no_updated_at=no_updated_at,
    )

    transaction_ids = importer._id_map(conn, TABLE_TRANSACTIONS, ["moss_object_uuid"])
    for row in expenses:
        row["moss_transaction_id"] = transaction_ids[importer._transaction_key(row)]
    expense_owners = importer._owner_ids(expenses, TABLE_EXPENSES)
    expense_plan = importer._plan_for(
        conn,
        ctx,
        TABLE_EXPENSES,
        "moss_expense_uuid",
        importer._plannable(expenses, "_transaction_ref"),
    )
    importer._apply(
        expense_plan, conn, TABLE_EXPENSES, now, no_updated_at=no_updated_at
    )
    importer._check_expense_numbers_now(conn)

    expense_ids = importer._id_map(conn, TABLE_EXPENSES, ["moss_expense_uuid"])
    for row in bookings:
        row["moss_expense_id"] = expense_ids[importer._expense_key(row)]
        row["moss_transaction_id"] = transaction_ids[importer._transaction_key(row)]
    importer._remember_source_files(origin, TABLE_BOOKINGS, bookings)
    booking_owners = importer._owner_ids(bookings, TABLE_BOOKINGS)
    booking_plan = importer._plan_for(
        conn,
        ctx,
        TABLE_BOOKINGS,
        importer._BOOKING_KEY,
        importer._plannable(bookings, "_transaction_ref", "_expense_ref"),
    )
    booking_plan, renumbered = importer._apply_split_reorders(
        conn, ctx, booking_plan, bookings, now, no_updated_at=no_updated_at
    )
    importer._apply(
        booking_plan, conn, TABLE_BOOKINGS, now, no_updated_at=no_updated_at
    )

    importer._post_run_checks(
        conn,
        importer._touched_transaction_ids(
            (transaction_plan, transaction_ids, TABLE_TRANSACTIONS),
            (expense_plan, expense_owners, TABLE_EXPENSES),
            (booking_plan, booking_owners, TABLE_BOOKINGS),
        )
        | renumbered,
    )
    return transaction_plan, expense_plan, booking_plan


def run_dry(conn, ctx, files):
    """The --dry-run branch of main(): L1 is planned, L2/L3 only for the
    transactions that already exist, nothing is applied. Returns the L1 plan --
    the preview returns none of its own, so what it planned for L2/L3 is only
    visible in the log. The preview words its lines after ctx.dry_run, which
    the CLI sets from the flag and this harness therefore sets for the call."""
    transaction_plan, expenses, bookings, _ = _plan_transactions(conn, ctx, files)
    known = importer._id_map(conn, TABLE_TRANSACTIONS, ["moss_object_uuid"])
    was_dry = ctx.dry_run_or_none
    ctx.dry_run = True
    try:
        importer._preview_lower_levels(conn, ctx, known, expenses, bookings)
    finally:
        ctx.dry_run = was_dry
    return transaction_plan


def run_preview(conn, ctx, files):
    """The preview of a REAL run: everything main() logs before it asks for the
    approval, with ctx.dry_run left alone. Returns the L1 plan, and the records
    it was given -- which planning must have left usable for the apply path."""
    transaction_plan, expenses, bookings, _ = _plan_transactions(conn, ctx, files)
    known = importer._id_map(conn, TABLE_TRANSACTIONS, ["moss_object_uuid"])
    importer._preview_lower_levels(conn, ctx, known, expenses, bookings)
    return transaction_plan, expenses, bookings


# ================================================================ read helpers


def counts(conn) -> tuple[int, int, int]:
    """(transactions, expenses, bookings) as THIS session sees them."""
    row = conn.execute(
        "SELECT (SELECT count(*) FROM moss_transactions),"
        " (SELECT count(*) FROM moss_expenses),"
        " (SELECT count(*) FROM moss_bookings)"
    ).fetchone()
    return (row[0], row[1], row[2])


def timestamps(conn, table: str) -> dict[int, tuple]:
    """{row id -> (created_at, updated_at)} of one Moss table. A row written by
    a plan carries its created_at from the INSERT and its updated_at only from a
    later run that really changed it -- which is what --no-updated-at leaves
    alone."""
    rows = conn.execute(
        f"SELECT id, created_at, updated_at FROM {table} ORDER BY id"  # noqa: S608 - fixed identifiers
    ).fetchall()
    return {row[0]: (row[1], row[2]) for row in rows}


def all_timestamps(conn) -> dict[str, dict[int, tuple]]:
    """The same for all three levels, keyed by table name."""
    return {
        table: timestamps(conn, table)
        for table in (TABLE_TRANSACTIONS, TABLE_EXPENSES, TABLE_BOOKINGS)
    }


def stamped_row_ids(stamps: dict[str, dict[int, tuple]]) -> dict[str, list[int]]:
    """{table -> the ids whose updated_at is set}: which rows a run stamped."""
    return {
        table: sorted(
            row_id
            for row_id, (_created, updated) in rows.items()
            if updated is not None
        )
        for table, rows in stamps.items()
    }


def transactions_by_type(conn) -> dict[str, dict]:
    """{type -> {column: value}} of the stored transactions -- the identity
    columns, the profile-dependent ones and what the app owns."""
    cursor = conn.execute(
        "SELECT type, moss_object_uuid, moss_transaction_uuid,"
        " all_moss_transaction_uuids, payment_date, booking_date, recipient_iban,"
        " recipient_bic, recipient_name, top_up_sender, payout_user_name,"
        " payout_team_name, other_moss_columns, comment, signed_total_base_amount"
        " FROM moss_transactions ORDER BY type"
    )
    columns = [column.name for column in cursor.description or ()]
    return {row[0]: dict(zip(columns, row)) for row in cursor.fetchall()}


def recipient(row: dict) -> tuple:
    """(iban, bic, name) of a stored transaction: the account that was paid."""
    return (row["recipient_iban"], row["recipient_bic"], row["recipient_name"])


def expenses_by_type(conn) -> dict[tuple[str, int], dict]:
    rows = conn.execute(
        "SELECT e.type, e.expense_number, e.moss_expense_uuid, t.moss_object_uuid"
        " FROM moss_expenses e JOIN moss_transactions t ON t.id = e.moss_transaction_id"
        " ORDER BY e.type, e.expense_number"
    ).fetchall()
    return {
        (row[0], row[1]): {"moss_expense_uuid": row[2], "object_uuid": row[3]}
        for row in rows
    }


def sub_row_numbers(conn) -> dict[tuple[str, int], list[int]]:
    """{(expense type, expense number) -> its bookings' sub_row_number}."""
    rows = conn.execute(
        "SELECT e.type, e.expense_number, b.sub_row_number"
        " FROM moss_bookings b JOIN moss_expenses e ON e.id = b.moss_expense_id"
        " ORDER BY e.type, e.expense_number, b.sub_row_number"
    ).fetchall()
    result: dict[tuple[str, int], list[int]] = {}
    for kind, number, sub_row in rows:
        result.setdefault((kind, number), []).append(sub_row)
    return result


def reimbursement_expense_rows(conn) -> list[dict]:
    """The reimbursement's stored expenses by expense number: which uuid sits
    under which number, what it cost and what it is called. A crossed pairing
    is visible here -- an expense holding another expense's amount."""
    rows = conn.execute(
        "SELECT e.expense_number, e.moss_expense_uuid, e.signed_expense_base_amount,"
        " e.expense_posting_text FROM moss_expenses e"
        " WHERE e.type = 'MossReimbursementExpense' ORDER BY e.expense_number"
    ).fetchall()
    return [
        {
            "expense_number": row[0],
            "moss_expense_uuid": row[1],
            "signed_expense_base_amount": row[2],
            "expense_posting_text": row[3],
        }
        for row in rows
    ]


def expected_reimbursement_expenses(order) -> list[dict]:
    """What the stored expenses of the NORMAL two have to be: the uuids in
    `order`, numbered from 1 in that order, each one on its OWN amount and its
    own text -- whichever order the two exports listed them in."""
    return [
        {
            "expense_number": number,
            "moss_expense_uuid": expense_uuid,
            "signed_expense_base_amount": EXPENSE_AMOUNTS[expense_uuid],
            "expense_posting_text": EXPENSE_TEXTS[expense_uuid],
        }
        for number, expense_uuid in enumerate(order, start=1)
    ]


def bookings_of(conn, expense_uuid) -> list[dict]:
    """The stored splits of ONE expense, by sub-row number: the row's own id,
    where it sits and what it says."""
    cursor = conn.execute(
        "SELECT b.id, b.sub_row_number, b.account_number, b.cost_center_number,"
        " b.signed_base_amount, b.booking_posting_text, b.comment, b.updated_at"
        " FROM moss_bookings b JOIN moss_expenses e ON e.id = b.moss_expense_id"
        " WHERE e.moss_expense_uuid = %s ORDER BY b.sub_row_number",
        (expense_uuid,),
    )
    columns = [column.name for column in cursor.description or ()]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def expense_b_bookings(conn) -> list[dict]:
    """The stored splits of the reimbursement's second expense."""
    return bookings_of(conn, EXPENSE_B_UUID)


def invoice_bookings(conn) -> list[dict]:
    """The invoice's stored lines by sub-row number: (account, cost center,
    amount, the account NAME the balance row carried). The name is what makes a
    crossed pairing visible -- it contradicts the account it sits next to."""
    rows = conn.execute(
        "SELECT b.sub_row_number, b.account_number, b.cost_center_number,"
        " b.signed_base_amount, b.other_moss_columns->>'Category'"
        " FROM moss_bookings b JOIN moss_expenses e ON e.id = b.moss_expense_id"
        " WHERE e.moss_expense_uuid = %s ORDER BY b.sub_row_number",
        (INVOICE_UUID,),
    ).fetchall()
    return [
        {
            "sub_row_number": row[0],
            "account_number": row[1],
            "cost_center_number": row[2],
            "signed_base_amount": row[3],
            "category": row[4],
        }
        for row in rows
    ]


def expected_invoice_bookings(lines, *, with_account_names: bool = True) -> list[dict]:
    """What those stored lines have to be: every line on its own amount, and
    the account name next to the account it belongs to."""
    return [
        {
            "sub_row_number": number,
            "account_number": line["Expense Account - Number"],
            "cost_center_number": line["Cost Center - Number"],
            "signed_base_amount": -decimal.Decimal(line["Amount"]),
            "category": (
                ACCOUNT_NAMES[line["Expense Account - Number"]]
                if with_account_names
                else None
            ),
        }
        for number, line in enumerate(lines, start=1)
    ]


def cross_the_stored_invoice_pairing(conn) -> None:
    """Make the stored invoice lines what a POSITIONAL pairing of crossed
    exports produces: the two rows keep their account and cost center and hold
    each other's amount and account name. That is the defect the content
    pairing has to repair."""
    rows = conn.execute(
        "SELECT b.id, b.signed_base_amount, b.other_moss_columns->>'Category'"
        " FROM moss_bookings b JOIN moss_expenses e ON e.id = b.moss_expense_id"
        " WHERE e.moss_expense_uuid = %s ORDER BY b.sub_row_number",
        (INVOICE_UUID,),
    ).fetchall()
    assert len(rows) == 2
    for row, other in ((rows[0], rows[1]), (rows[1], rows[0])):
        conn.execute(
            "UPDATE moss_bookings SET signed_base_amount = %s, other_moss_columns ="
            " jsonb_set(other_moss_columns, '{Category}', to_jsonb(%s::text))"
            " WHERE id = %s",
            (other[1], other[2], row[0]),
        )


def booking_content(row: dict) -> tuple:
    """What a booking row SAYS -- everything but where it sits and when it was
    last written. Two rows that exchange their sub-row numbers keep this."""
    return (
        row["account_number"],
        row["cost_center_number"],
        row["signed_base_amount"],
        row["booking_posting_text"],
    )


def assert_rows_exchanged_their_positions(
    before: list[dict], after: list[dict]
) -> None:
    """The two stored rows of an expense are still the same rows, each with the
    content it had, and they hold each other's sub-row number: the rows moved,
    nothing was rewritten."""
    by_id = {row["id"]: row for row in after}
    assert set(by_id) == {row["id"] for row in before}
    for row in before:
        assert booking_content(by_id[row["id"]]) == booking_content(row)
    assert {row["id"]: row["sub_row_number"] for row in after} == {
        before[0]["id"]: 2,
        before[1]["id"]: 1,
    }


def watch_booking_updates(conn) -> None:
    """Record every UPDATE on moss_bookings from here on: one row per
    STATEMENT and one per updated row, carrying the sub_row_number that
    statement wrote. That makes the INTERMEDIATE state observable -- a
    renumbering that parked its rows somewhere before setting the targets
    would show up as a second statement and as numbers nobody ever asked for.
    All of it is DDL inside the test transaction, rolled back with the rest."""
    conn.execute("CREATE TEMP TABLE booking_update_audit (kind text, sub_row integer)")
    conn.execute(
        "CREATE FUNCTION pg_temp.record_booking_update() RETURNS trigger AS $$"
        " BEGIN"
        "   IF TG_LEVEL = 'STATEMENT' THEN"
        "     INSERT INTO pg_temp.booking_update_audit VALUES ('statement', NULL);"
        "   ELSE"
        "     INSERT INTO pg_temp.booking_update_audit VALUES ('row', NEW.sub_row_number);"
        "   END IF;"
        "   RETURN NULL;"
        " END $$ LANGUAGE plpgsql"
    )
    for level in ("STATEMENT", "ROW"):
        conn.execute(
            f"CREATE TRIGGER audit_booking_update_{level.lower()}"
            " AFTER UPDATE ON moss_bookings"
            f" FOR EACH {level} EXECUTE FUNCTION pg_temp.record_booking_update()"
        )


def booking_update_audit(conn) -> tuple[int, list[int]]:
    """(number of UPDATE statements on moss_bookings, every sub_row_number they
    wrote) since watch_booking_updates."""
    rows = conn.execute("SELECT kind, sub_row FROM booking_update_audit").fetchall()
    return (
        sum(1 for kind, _ in rows if kind == "statement"),
        sorted(sub_row for kind, sub_row in rows if kind == "row"),
    )


def replace_sub_row_constraint_with_plain_index(conn) -> None:
    """Give the L3 key what an older database has: a plain unique INDEX instead
    of the deferrable constraint the wagon migration created."""
    conn.execute(f"ALTER TABLE moss_bookings DROP CONSTRAINT {SUB_ROW_CONSTRAINT}")
    conn.execute(
        "CREATE UNIQUE INDEX index_moss_bookings_expense_sub_row"
        " ON moss_bookings (moss_expense_id, sub_row_number)"
    )


def restore_sub_row_constraint(conn) -> None:
    """Put the deferrable constraint back, so the tables are as they were even
    before this transaction is rolled back."""
    conn.execute("DROP INDEX IF EXISTS index_moss_bookings_expense_sub_row")
    conn.execute(
        f"ALTER TABLE moss_bookings ADD CONSTRAINT {SUB_ROW_CONSTRAINT}"
        " UNIQUE (moss_expense_id, sub_row_number) DEFERRABLE INITIALLY DEFERRED"
    )


def sub_row_constraint_definition(conn) -> str | None:
    """What pg_constraint says about the L3 key, None when it is not a unique
    constraint at all."""
    row = conn.execute(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint"
        " WHERE conrelid = 'moss_bookings'::regclass AND conname = %s"
        " AND contype = 'u'",
        (SUB_ROW_CONSTRAINT,),
    ).fetchone()
    return None if row is None else row[0]


def _records(caplog, logger_name: str, level: int) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == logger_name and record.levelno == level
    ]


def importer_records(caplog, level: int) -> list[str]:
    return _records(caplog, IMPORTER_LOGGER, level)


def plan_builder_records(caplog, level: int) -> list[str]:
    return _records(caplog, PLAN_LOGGER, level)


def plan_preview(caplog, table: str) -> list[str]:
    """The plan preview of ONE table: its "Planned for <table>" line and the
    indented lines below it. The LAST preview of that table, so a test that
    asserts on a second run clears `caplog` before it."""
    block: list[str] = []
    inside = False
    for message in importer_records(caplog, logging.INFO):
        if message.startswith("Planned for "):
            inside = message.startswith(f"Planned for {table}:")
            if inside:
                block = [message]
        elif inside and message.startswith("  "):
            block.append(message)
    return block


_PREVIEW_COUNT = re.compile(r"(\w+) \((\d+)\)")


def preview_counts(caplog, table: str, label: str) -> dict[str, int]:
    """{column -> row count} of one "<label>: column (n), ..." preview line;
    empty when the plan logged no such line."""
    for line in plan_preview(caplog, table):
        if line.startswith(f"  {label}:"):
            return {name: int(count) for name, count in _PREVIEW_COUNT.findall(line)}
    return {}


def plan_counts(plan) -> tuple[int, int]:
    return len(plan.inserts), len(plan.updates)


#: The invoice-pairing lines, verbatim (the fixture's invoice has two lines).
UNPAIRABLE_INVOICE = (
    f"invoice {INVOICE_NUMBER}: its 2 lines cannot be paired with their balance "
    "rows by content; the export order decides."
)
AMOUNTS_MOVED = (
    f"invoice {INVOICE_NUMBER}: 2 of its stored booking(s) keep their account "
    "and cost center but change their amount."
)


def paired_in_another_order(step: str) -> str:
    """The line logged for an invoice whose balance rows arrive in another
    order than its lines, naming the step that decided the pairing."""
    return (
        f"invoice {INVOICE_NUMBER}: its 2 balance rows arrive in another order "
        f"than its lines; paired by {step}."
    )


def pairing_lines(caplog) -> list[str]:
    """The lines the invoice pairing logs: one per invoice whose balance rows
    arrive in another order than its lines (INFO), one per invoice nothing
    could pair (WARNING) and one per stored invoice whose amounts moved."""
    return [
        message
        for level in (logging.INFO, logging.WARNING)
        for message in importer_records(caplog, level)
        if message.startswith("invoice ")
    ]


#: The reimbursement-pairing lines, verbatim (the fixture's reimbursement has
#: two expenses).
UNPAIRABLE_REIMBURSEMENT = (
    f"reimbursement {REIMBURSEMENT_UUID}: its 2 expenses cannot be paired with "
    "their balance rows by content; the export order decides."
)
EXPENSE_NUMBERS_MOVED = (
    f"reimbursement {REIMBURSEMENT_UUID}: 2 of its stored expense(s) keep their "
    "uuid but change their expense number."
)


def reimbursement_paired_in_another_order(step: str) -> str:
    """The line logged for a reimbursement whose balance rows arrive in another
    order than its expenses, naming the step that decided the pairing."""
    return (
        f"reimbursement {REIMBURSEMENT_UUID}: its 2 balance rows arrive in "
        f"another order than its expenses; paired by {step}."
    )


def reimbursement_pairing_lines(caplog) -> list[str]:
    """The lines the reimbursement pairing logs: one per reimbursement whose
    balance rows arrive in another order than its expenses (INFO), one per
    reimbursement nothing could pair (WARNING) and one per stored
    reimbursement whose expense numbers moved."""
    return [
        message
        for level in (logging.INFO, logging.WARNING)
        for message in importer_records(caplog, level)
        if message.startswith("reimbursement ")
    ]


def detail_gate_lines(caplog) -> list[str]:
    """What the detail gate refused, one line per transaction."""
    return [
        message
        for message in importer_records(caplog, logging.WARNING)
        if message.startswith("detail gate -- SKIPPED ")
    ]


#: How the preview labels its own lines: a dry run applies nothing at all, a
#: real run is about to decide (see _preview_lower_levels).
DRY_RUN_LABEL = "[--dry-run] "
PREVIEW_LABEL = "[preview] "


def deferred_line(label: str, expenses: int, bookings: int) -> str:
    """The preview's line about what it cannot plan: everything hanging off a
    transaction the run would have to INSERT first."""
    return (
        f"{label}{expenses} expense(s) and {bookings} booking(s) belong to "
        "transactions that do not exist yet and are not planned above: the "
        "level-2 and level-3 plans are a lower bound."
    )


def preview_lines(caplog) -> list[str]:
    """The preview's own warnings -- what it could not plan and what a
    renumbering would move -- each carrying the label of its run."""
    return [
        message
        for message in importer_records(caplog, logging.WARNING)
        if message.startswith((DRY_RUN_LABEL, PREVIEW_LABEL))
    ]


def reorder_lines(caplog) -> list[str]:
    """The per-expense lines the split-reorder detection logs -- one per
    expense whose splits Moss reordered, naming the kind it belongs to."""
    return [
        message
        for message in importer_records(caplog, logging.INFO)
        if message.startswith("splits reordered in Moss:")
    ]


# ===================================================================== tests


class Test_import_moss_transactions_keys:
    """One import of the synthetic exports is 4 transactions / 5 expenses /
    8 bookings; every test below starts from an empty (rolled-back) database."""

    def test_first_import_writes_the_three_levels_on_their_stable_keys(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """P1: what the keys are after a first import."""
        files = write_exports(tmp_path / "P1", "P1")
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, files, now=NOW)

        assert [plan_counts(plan) for plan in plans] == [
            (L1_ROWS, 0),
            (L2_ROWS, 0),
            (L3_ROWS, 0),
        ]
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

        # L1: the identity is the OBJECT the payment settles.
        stored = transactions_by_type(rw_conn)
        assert stored["MossCardTransaction"]["moss_object_uuid"] == CARD_UUID
        assert stored["MossReimbursement"]["moss_object_uuid"] == REIMBURSEMENT_UUID
        assert stored["MossInvoice"]["moss_object_uuid"] == INVOICE_UUID
        assert stored["MossTopUp"]["moss_object_uuid"] == wallet_uuid("P1", "top-up")
        # ... while every row still remembers the Transaction ID it came under.
        assert {
            kind: row["all_moss_transaction_uuids"] for kind, row in stored.items()
        } == {
            "MossCardTransaction": [CARD_UUID],
            "MossReimbursement": [wallet_uuid("P1", "reimbursement-payout")],
            "MossInvoice": [wallet_uuid("P1", "invoice-payout")],
            "MossTopUp": [wallet_uuid("P1", "top-up")],
        }
        for row in stored.values():
            assert row["moss_transaction_uuid"] in row["all_moss_transaction_uuids"]

        # L2: a shell expense IS its transaction, a reimbursement expense has
        # an id of its own.
        expenses = expenses_by_type(rw_conn)
        for key in (
            ("MossCardTransactionExpense", 1),
            ("MossInvoiceExpense", 1),
            ("MossTopUpExpense", 1),
        ):
            assert expenses[key]["moss_expense_uuid"] == expenses[key]["object_uuid"]
        assert expenses[("MossReimbursementExpense", 1)]["moss_expense_uuid"] == (
            EXPENSE_A_UUID
        )
        assert expenses[("MossReimbursementExpense", 2)]["moss_expense_uuid"] == (
            EXPENSE_B_UUID
        )

        # L3: the split index inside its own expense, never a file position.
        assert sub_row_numbers(rw_conn) == {
            ("MossCardTransactionExpense", 1): [1, 2],
            ("MossReimbursementExpense", 1): [1],
            ("MossReimbursementExpense", 2): [1, 2],
            ("MossInvoiceExpense", 1): [1, 2],
            ("MossTopUpExpense", 1): [1],
        }

        assert any(
            message.startswith("Post-run checks passed")
            for message in importer_records(caplog, logging.INFO)
        )

    def test_first_import_writes_the_profile_dependent_columns(
        self, rw_conn, ctx, tmp_path
    ):
        """P1: what a rich export layout makes of the columns the profiles
        disagree about -- the payout account and the funding organisation are
        stored, the finance user and their team stay raw, and only a card
        payment and a top-up have a payment date."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        stored = transactions_by_type(rw_conn)

        # The two columns the importer never writes, however full the export:
        # the raw cells stay readable under their CSV header instead.
        for row in stored.values():
            assert row["payout_user_name"] is None
            assert row["payout_team_name"] is None
        for kind in ("MossReimbursement", "MossInvoice"):
            raw = stored[kind]["other_moss_columns"]
            assert raw["Cardholder"] == PAYOUT_USER_NAME
            assert raw["Team Name"] == PAYOUT_TEAM_NAME

        # A payout's payment date is a profile artefact and is not imported;
        # a card payment's and a top-up's is a fact of the payment.
        assert stored["MossReimbursement"]["payment_date"] is None
        assert stored["MossInvoice"]["payment_date"] is None
        assert stored["MossCardTransaction"]["payment_date"] == CARD_PAYMENT_DATE
        assert stored["MossTopUp"]["payment_date"] == TOP_UP_BOOKING_DATE

        # The account each payout went to.
        assert recipient(stored["MossReimbursement"]) == (
            *REIMBURSEMENT_RECIPIENT["P1"],
            REIMBURSEMENT_PAYEE,
        )
        assert recipient(stored["MossInvoice"]) == (*INVOICE_RECIPIENT, INVOICE_PAYEE)

        # The organisation that funded the wallet -- without the account
        # spelling behind it, which the raw line keeps.
        assert stored["MossTopUp"]["top_up_sender"] == TOP_UP_ORGANISATION
        assert (
            stored["MossTopUp"]["other_moss_columns"]["Reason for Purchase"]
            == TOP_UP_REASON_RICH
        )

    def test_reimporting_the_same_profile_writes_nothing(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """P1 twice: the whole point of the keys -- a re-import is a no-op."""
        files = write_exports(tmp_path / "P1", "P1")
        run_import(rw_conn, ctx, files, now=NOW)

        again = run_import(rw_conn, ctx, files, now=LATER)
        assert [plan_counts(plan) for plan in again] == [(0, 0), (0, 0), (0, 0)]
        assert [len(plan.untouched_keys) for plan in again] == [
            L1_ROWS,
            L2_ROWS,
            L3_ROWS,
        ]
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

        # The --dry-run branch says the same about all three levels.
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            dry = run_dry(rw_conn, ctx, files)
        assert plan_counts(dry) == (0, 0)
        planned = importer_records(caplog, logging.INFO)
        assert f"Planned for {TABLE_EXPENSES}: 0 INSERTs, 0 UPDATEs" in " ".join(
            planned
        )
        assert f"Planned for {TABLE_BOOKINGS}: 0 INSERTs, 0 UPDATEs" in " ".join(
            planned
        )

    def test_second_profile_appends_its_ids_and_keeps_the_identity(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """P2 after P1: the wallet payouts arrive under NEW Transaction IDs,
        and the lean layout -- which the run recognises as one -- rewrites
        neither what the app owns nor what it does not carry itself."""
        first = write_exports(tmp_path / "P1", "P1")
        run_import(rw_conn, ctx, first, now=NOW)
        rw_conn.execute(
            "UPDATE moss_transactions SET comment = %s WHERE type = 'MossReimbursement'",
            ("Test comment written by the app",),
        )

        second = write_exports(tmp_path / "P2", "P2")
        caplog.clear()
        with caplog.at_level(logging.INFO):
            transaction_plan, expense_plan, booking_plan = run_import(
                rw_conn, ctx, second, now=LATER
            )

        # Nothing new anywhere: the same four transactions, five expenses,
        # eight bookings.
        assert plan_counts(transaction_plan)[0] == 0
        assert plan_counts(expense_plan) == (0, 0)
        assert plan_counts(booking_plan) == (0, 0)
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

        stored = transactions_by_type(rw_conn)
        for kind, name in (
            ("MossReimbursement", "reimbursement-payout"),
            ("MossInvoice", "invoice-payout"),
            ("MossTopUp", "top-up"),
        ):
            row = stored[kind]
            assert row["all_moss_transaction_uuids"] == [
                wallet_uuid("P1", name),
                wallet_uuid("P2", name),
            ]
            # The FIRST id a row was seen under is never rewritten.
            assert row["moss_transaction_uuid"] == wallet_uuid("P1", name)
        # A card payment is identified by its own id, which never changed.
        assert stored["MossCardTransaction"]["all_moss_transaction_uuids"] == [
            CARD_UUID
        ]

        # The top-up has no second id at all -- only the heuristic finds it.
        heuristic = [
            message
            for message in importer_records(caplog, logging.WARNING)
            if "matched by booking date and amount" in message
        ]
        assert len(heuristic) == 1
        assert len(importer_records(caplog, logging.WARNING)) == 1

        # What the app owns survives...
        assert (
            stored["MossReimbursement"]["comment"] == "Test comment written by the app"
        )
        # ... and so does every payout detail this layout leaves blank: the
        # preview names them per column, next to the plan builder's own line.
        assert preview_counts(caplog, TABLE_TRANSACTIONS, KEPT_BLANK) == {
            "recipient_iban": 2,
            "recipient_bic": 2,
        }
        assert [
            message
            for message in plan_builder_records(caplog, logging.INFO)
            if message.startswith("Blank input kept the stored value:")
        ] == [
            "Blank input kept the stored value: recipient_bic (2), recipient_iban (2)."
        ]

        # The file that carries none of them says so once, when it is read.
        without_details = [
            message
            for message in importer_records(caplog, logging.INFO)
            if "balance export without payout details" in message
        ]
        assert len(without_details) == 1
        assert without_details[0].startswith("balance-movements_P2.csv:")

    def test_second_profile_keeps_the_columns_it_does_not_carry(
        self, rw_conn, ctx, tmp_path
    ):
        """P2 after P1, column by column: a blank cell of the lean layout
        never clears what P1 stored, a filled one is followed."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = transactions_by_type(rw_conn)
        run_import(rw_conn, ctx, write_exports(tmp_path / "P2", "P2"), now=LATER)
        stored = transactions_by_type(rw_conn)

        for kind, row in stored.items():
            assert recipient(row) == recipient(before[kind])
            assert row["payment_date"] == before[kind]["payment_date"]
            assert row["payout_user_name"] is None
            assert row["payout_team_name"] is None
        # The same organisation, however the lean layout spells its account.
        assert stored["MossTopUp"]["top_up_sender"] == TOP_UP_ORGANISATION

        # The raw mirror follows the file: the reimbursement payout has
        # neither cell in P2, so its whole raw set is the one P1 wrote ...
        assert (
            stored["MossReimbursement"]["other_moss_columns"]
            == before["MossReimbursement"]["other_moss_columns"]
        )
        # ... while the invoice payout's Team Name, filled here with the
        # invoice's own team, is taken as it stands (its Cardholder is not).
        assert stored["MossInvoice"]["other_moss_columns"]["Team Name"] == (
            OTHER_TEAM_NAME
        )
        assert stored["MossInvoice"]["other_moss_columns"]["Cardholder"] == (
            PAYOUT_USER_NAME
        )
        assert stored["MossTopUp"]["other_moss_columns"]["Reason for Purchase"] == (
            TOP_UP_REASON_LEAN
        )

    def test_a_filled_cell_overwrites_a_kept_payout_account(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """P1 -> P2 -> P4: the protection is about BLANK cells only. P2 keeps
        the stored payout account, P4 names another one and wins."""
        base = tmp_path
        run_import(rw_conn, ctx, write_exports(base / "P1", "P1"), now=NOW)

        caplog.clear()
        with caplog.at_level(logging.INFO):
            run_import(rw_conn, ctx, write_exports(base / "P2", "P2"), now=LATER)
        # Only the two blank columns are kept -- recipient_name, which the
        # lean layout does carry, is compared like any other column.
        assert preview_counts(caplog, TABLE_TRANSACTIONS, KEPT_BLANK) == {
            "recipient_iban": 2,
            "recipient_bic": 2,
        }
        assert recipient(transactions_by_type(rw_conn)["MossReimbursement"]) == (
            *REIMBURSEMENT_RECIPIENT["P1"],
            REIMBURSEMENT_PAYEE,
        )

        caplog.clear()
        with caplog.at_level(logging.INFO):
            run_import(rw_conn, ctx, write_exports(base / "P4", "P4"), now=LATER)
        stored = transactions_by_type(rw_conn)
        assert recipient(stored["MossReimbursement"]) == (
            *REIMBURSEMENT_RECIPIENT["P4"],
            REIMBURSEMENT_PAYEE,
        )
        # The payout P4 does not correct keeps the account it had.
        assert recipient(stored["MossInvoice"]) == (*INVOICE_RECIPIENT, INVOICE_PAYEE)
        # Nothing is blank in this layout, so no stored value is kept at all,
        # and the two corrected columns are planned as the updates they are.
        assert preview_counts(caplog, TABLE_TRANSACTIONS, KEPT_BLANK) == {}
        updated = preview_counts(caplog, TABLE_TRANSACTIONS, UPDATE_COLUMNS)
        assert updated["recipient_iban"] == 1
        assert updated["recipient_bic"] == 1

    def test_dry_run_of_the_second_profile_plans_no_protected_column(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The --dry-run preview of P2 after P1 names what an import would
        rewrite -- the new Transaction IDs and the raw cells the lean layout
        spells differently, and none of the columns it does not carry."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        second = write_exports(tmp_path / "P2", "P2")

        caplog.clear()
        with caplog.at_level(logging.INFO):
            plan = run_dry(rw_conn, ctx, second)

        assert plan_counts(plan) == (0, 3)
        assert preview_counts(caplog, TABLE_TRANSACTIONS, KEPT_BLANK) == {
            "recipient_iban": 2,
            "recipient_bic": 2,
        }
        updated = preview_counts(caplog, TABLE_TRANSACTIONS, UPDATE_COLUMNS)
        assert set(updated).isdisjoint(
            {
                "recipient_iban",
                "recipient_bic",
                "recipient_name",
                "payout_user_name",
                "payout_team_name",
                "payment_date",
            }
        )
        assert updated == {"all_moss_transaction_uuids": 3, "other_moss_columns": 2}
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_reimporting_the_second_profile_writes_nothing(
        self, rw_conn, ctx, tmp_path
    ):
        """P2 twice: idempotent on the appended ids as well."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        second = write_exports(tmp_path / "P2", "P2")
        run_import(rw_conn, ctx, second, now=LATER)

        again = run_import(rw_conn, ctx, second, now=LATER)
        assert [plan_counts(plan) for plan in again] == [(0, 0), (0, 0), (0, 0)]
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_third_profile_appends_in_discovery_order(self, rw_conn, ctx, tmp_path):
        """P1 -> P2 -> P3: the array grows in the order the ids were seen, and
        going back to P1 afterwards changes nothing at all."""
        base = tmp_path
        first = write_exports(base / "P1", "P1")
        run_import(rw_conn, ctx, first, now=NOW)
        run_import(rw_conn, ctx, write_exports(base / "P2", "P2"), now=LATER)
        run_import(rw_conn, ctx, write_exports(base / "P3", "P3"), now=LATER)

        stored = transactions_by_type(rw_conn)
        for kind, name in (
            ("MossReimbursement", "reimbursement-payout"),
            ("MossInvoice", "invoice-payout"),
            ("MossTopUp", "top-up"),
        ):
            assert stored[kind]["all_moss_transaction_uuids"] == [
                wallet_uuid("P1", name),
                wallet_uuid("P2", name),
                wallet_uuid("P3", name),
            ]

        # P3 carries the same layout as P1, so re-importing P1 is a full no-op:
        # its id is already in the array, and no column differs.
        back = run_import(rw_conn, ctx, first, now=LATER)
        assert [plan_counts(plan) for plan in back] == [(0, 0), (0, 0), (0, 0)]
        assert transactions_by_type(rw_conn) == stored
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_ambiguous_top_up_heuristic_is_a_conflict(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """Two stored top-ups with the same booking date and amount: the
        heuristic must refuse to decide instead of guessing."""
        base = tmp_path
        run_import(rw_conn, ctx, write_exports(base / "P1", "P1"), now=NOW)
        rw_conn.execute(
            "INSERT INTO moss_transactions (type, moss_transaction_uuid,"
            " all_moss_transaction_uuids, booking_date, signed_total_base_amount)"
            " VALUES ('MossTopUp', %s, ARRAY[%s]::uuid[], %s, %s)",
            (
                fixed_uuid("second-top-up"),
                fixed_uuid("second-top-up"),
                TOP_UP_BOOKING_DATE,
                decimal.Decimal(TOP_UP_AMOUNT),
            ),
        )
        before = counts(rw_conn)
        assert before == (L1_ROWS + 1, L2_ROWS, L3_ROWS)

        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            with pytest.raises(SystemExit) as excinfo:
                run_import(rw_conn, ctx, write_exports(base / "P2", "P2"), now=LATER)
        assert excinfo.value.code == 1
        conflicts = [
            message
            for message in importer_records(caplog, logging.ERROR)
            if message.startswith("identity conflict")
        ]
        assert any("the heuristic cannot decide" in message for message in conflicts)
        # The run stops BEFORE the first plan: nothing was written.
        assert counts(rw_conn) == before

    def test_transaction_id_of_another_kind_is_a_conflict(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """A reimbursement payout arriving under the stored top-up's
        Transaction ID is a contradiction, not a profile change."""
        base = tmp_path
        run_import(rw_conn, ctx, write_exports(base / "P1", "P1"), now=NOW)
        before = counts(rw_conn)

        hostile = write_exports(
            base / "P2-hostile",
            "P2",
            reimbursement_payout=wallet_uuid("P1", "top-up"),
        )
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            with pytest.raises(SystemExit) as excinfo:
                run_import(rw_conn, ctx, hostile, now=LATER)
        assert excinfo.value.code == 1
        conflicts = [
            message
            for message in importer_records(caplog, logging.ERROR)
            if message.startswith("identity conflict")
        ]
        assert any(
            "is stored on the transaction" in message for message in conflicts
        ), conflicts
        assert counts(rw_conn) == before

    def test_post_run_checks_catch_a_broken_shell_expense(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The L2 invariant no single plan can see: a shell expense must carry
        its transaction's moss_object_uuid."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        rw_conn.execute(
            "UPDATE moss_expenses SET moss_expense_uuid = %s"
            " WHERE type = 'MossTopUpExpense'",
            (fixed_uuid("rogue-shell-expense"),),
        )

        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            with pytest.raises(SystemExit) as excinfo:
                importer._post_run_checks(rw_conn, set())
        assert excinfo.value.code == 1
        assert any(
            "shell expense" in message
            for message in importer_records(caplog, logging.ERROR)
        )

    def test_cli_dry_run_plans_four_transactions_and_writes_nothing(
        self, rw_conn, integration_testing_ctx, tmp_path
    ):
        """The real CLI in a subprocess. It has its own session, so it sees only
        COMMITTED state -- the three tables exist and are empty, because every
        test in this class rolls its writes back."""
        rw_conn.rollback()
        assert counts(rw_conn) == (0, 0, 0)
        files = write_exports(tmp_path / "P1", "P1")

        result = pytest_wsjrdp2027.uv_run(
            [str(IMPORTER_PATH), "--dry-run", *files],
            ctx=integration_testing_ctx,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert result.returncode == 0
        assert f"Planned for {TABLE_TRANSACTIONS}: {L1_ROWS} INSERTs" in result.stdout
        assert "[--dry-run] Nothing applied." in result.stdout
        assert counts(rw_conn) == (0, 0, 0)

    def test_cli_rollback_for_testing_applies_and_rolls_back(
        self, rw_conn, integration_testing_ctx, tmp_path
    ):
        """The same CLI with --rollback-for-testing: it really writes all three
        levels, passes its post-run checks and then commits nothing."""
        rw_conn.rollback()
        assert counts(rw_conn) == (0, 0, 0)
        files = write_exports(tmp_path / "P1", "P1")

        result = pytest_wsjrdp2027.uv_run(
            [str(IMPORTER_PATH), "--rollback-for-testing", *files],
            ctx=integration_testing_ctx,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert result.returncode == 0
        assert f"{L1_ROWS} inserted" in result.stdout
        assert "Post-run checks passed" in result.stdout
        assert "ROLLBACK" in result.stdout
        assert counts(rw_conn) == (0, 0, 0)


class Test_import_moss_transactions_preview:
    """What the run shows before it writes anything -- in production before the
    approval: all three levels, the two lower ones for the records whose
    transaction is already stored, and the counts of everything that hangs off
    a transaction this run would have to insert first."""

    def test_the_preview_reports_what_it_cannot_plan(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """A first import: none of its transactions is stored yet, so no
        expense and no booking can be keyed against a parent id. The preview
        names both counts, which is what makes the level-1 plan readable as a
        lower bound instead of as the whole run."""
        files = write_exports(tmp_path / "P1", "P1")

        caplog.clear()
        with caplog.at_level(logging.INFO):
            plan = run_dry(rw_conn, ctx, files)

        assert plan_counts(plan) == (L1_ROWS, 0)
        assert preview_lines(caplog) == [deferred_line(DRY_RUN_LABEL, L2_ROWS, L3_ROWS)]
        # There was nothing to plan on either lower level.
        assert plan_preview(caplog, TABLE_EXPENSES) == []
        assert plan_preview(caplog, TABLE_BOOKINGS) == []
        assert counts(rw_conn) == (0, 0, 0)

    def test_the_preview_of_a_real_run_claims_no_dry_run(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The same preview in a run that is about to write: the same deferred
        counts, and no line claiming that nothing is applied. It plans on
        copies, so the records keep the private cross-level references the
        apply path needs afterwards."""
        files = write_exports(tmp_path / "P1", "P1")

        caplog.clear()
        with caplog.at_level(logging.INFO):
            plan, expenses, bookings = run_preview(rw_conn, ctx, files)

        assert plan_counts(plan) == (L1_ROWS, 0)
        assert preview_lines(caplog) == [deferred_line(PREVIEW_LABEL, L2_ROWS, L3_ROWS)]
        assert all("_transaction_ref" in row for row in expenses)
        assert all("_expense_ref" in row for row in bookings)
        assert counts(rw_conn) == (0, 0, 0)

    def test_the_preview_plans_both_lower_levels_of_stored_transactions(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """P2 after P1: every transaction is stored, so the preview plans all
        five expenses and all eight bookings and defers nothing -- the approval
        would see the run in full."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        second = write_exports(tmp_path / "P2", "P2")

        caplog.clear()
        with caplog.at_level(logging.INFO):
            run_preview(rw_conn, ctx, second)

        assert (
            f"Planned for {TABLE_EXPENSES}: 0 INSERTs, 0 UPDATEs, 0 DELETEs "
            f"({L2_ROWS} untouched)." in plan_preview(caplog, TABLE_EXPENSES)
        )
        assert (
            f"Planned for {TABLE_BOOKINGS}: 0 INSERTs, 0 UPDATEs, 0 DELETEs "
            f"({L3_ROWS} untouched)." in plan_preview(caplog, TABLE_BOOKINGS)
        )
        assert preview_lines(caplog) == []
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_the_preview_announces_a_renumbering(self, rw_conn, ctx, tmp_path, caplog):
        """The reorder detection runs read-only in the preview of a real run as
        well: a renumbering is named before anything is decided, and the
        preview itself moves no row."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = expense_b_bookings(rw_conn)
        swapped = write_exports(tmp_path / "P1-swapped", "P1", splits=SPLITS_SWAPPED)

        caplog.clear()
        with caplog.at_level(logging.INFO):
            run_preview(rw_conn, ctx, swapped)

        assert len(reorder_lines(caplog)) == 1
        assert preview_lines(caplog) == [
            (
                f"{PREVIEW_LABEL}2 booking row(s) in 1 expense(s) would be "
                "renumbered; the plan above is the one BEFORE that."
            )
        ]
        assert expense_b_bookings(rw_conn) == before
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_cli_previews_the_lower_levels_before_the_first_write(
        self, rw_conn, integration_testing_ctx, tmp_path
    ):
        """The real CLI against a database that already holds the transactions.
        In production the approval sits between the preview and the first
        write; this environment is not production, so
        require_approval_to_run_in_prod logs nothing and the ORDER of the lines
        is what can be read: both lower-level plan summaries appear before the
        first row is written.

        The first import has to be COMMITTED for the subprocess to see it, so
        the three tables are emptied again afterwards -- every other test of
        this module starts from an empty database."""
        rw_conn.rollback()
        assert counts(rw_conn) == (0, 0, 0)
        first = write_exports(tmp_path / "P1", "P1")
        second = write_exports(tmp_path / "P2", "P2")

        try:
            run_import(rw_conn, integration_testing_ctx, first, now=NOW)
            rw_conn.commit()
            result = pytest_wsjrdp2027.uv_run(
                [str(IMPORTER_PATH), "--rollback-for-testing", *second],
                ctx=integration_testing_ctx,
                stdout=subprocess.PIPE,
                text=True,
            )
        finally:
            for table in (TABLE_BOOKINGS, TABLE_EXPENSES, TABLE_TRANSACTIONS):
                rw_conn.execute(f"DELETE FROM {table}")  # noqa: S608 - fixed identifiers
            rw_conn.commit()

        assert result.returncode == 0
        first_write = re.search(r"\d+ inserted, \d+ updated", result.stdout)
        assert first_write is not None
        for table in (TABLE_EXPENSES, TABLE_BOOKINGS):
            assert result.stdout.index(f"Planned for {table}:") < first_write.start()
            # Once in the preview, once for the plan that is then applied.
            assert result.stdout.count(f"Planned for {table}:") == 2
        # A run that is about to write never labels a line as a dry run.
        assert DRY_RUN_LABEL not in result.stdout
        assert "ROLLBACK" in result.stdout
        assert counts(rw_conn) == (0, 0, 0)


class Test_import_moss_transactions_reimbursement_pairing:
    """A reimbursement arrives in two exports at once, and a profile may order
    them differently: the reimbursement export carries the expense with its
    splits, the balance export what was paid for it and the number the expense
    is stored under. The two are paired by CONTENT -- see the REIMBURSEMENT
    EXPENSE PAIRING section of the importer -- so that no expense ends up with
    another expense's amount, sign or number."""

    def test_swapped_balance_rows_keep_every_expense_on_its_own_amount(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The balance export lists its two rows in the other order while the
        reimbursement export keeps its own. Pairing them by position would put
        each expense's amount and number on the OTHER expense's uuid; pairing
        them by amount and text puts every expense on its own -- under the
        number of the balance row that really paid for it."""
        crossed = write_exports(
            tmp_path / "P1-crossed", "P1", swap_reimbursement_balance_rows=True
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, crossed, now=NOW)

        assert [plan_counts(plan) for plan in plans] == [
            (L1_ROWS, 0),
            (L2_ROWS, 0),
            (L3_ROWS, 0),
        ]
        assert reimbursement_expense_rows(rw_conn) == expected_reimbursement_expenses(
            [EXPENSE_B_UUID, EXPENSE_A_UUID]
        )

        # The order of the balance export says nothing about the splits, so
        # nothing is renumbered -- only the pairing is reported.
        assert reorder_lines(caplog) == []
        assert reimbursement_pairing_lines(caplog) == [
            reimbursement_paired_in_another_order("amount and text")
        ]

        # And the same files a second time have nothing left to say.
        again = run_import(rw_conn, ctx, crossed, now=LATER)
        assert [plan_counts(plan) for plan in again] == [(0, 0), (0, 0), (0, 0)]
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_a_crossed_stored_reimbursement_is_corrected_and_the_change_reported(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The same reimbursement, already stored under the numbers its first
        export gave it. The crossed balance export pays the same expenses under
        the other numbers, so both expenses move -- one plan exchanging 1 and 2,
        which only the DEFERRABLE constraint on (transaction, expense number)
        lets pass. The amounts stay where they belong, and the run says that the
        numbering moved before the plan is applied."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        assert reimbursement_expense_rows(rw_conn) == expected_reimbursement_expenses(
            [EXPENSE_A_UUID, EXPENSE_B_UUID]
        )

        crossed = write_exports(
            tmp_path / "P1-crossed", "P1", swap_reimbursement_balance_rows=True
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, crossed, now=LATER)

        assert [plan_counts(plan) for plan in plans] == [(0, 0), (0, 2), (0, 0)]
        assert preview_counts(caplog, TABLE_EXPENSES, UPDATE_COLUMNS) == {
            "expense_number": 2
        }
        assert reimbursement_expense_rows(rw_conn) == expected_reimbursement_expenses(
            [EXPENSE_B_UUID, EXPENSE_A_UUID]
        )
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

        assert reorder_lines(caplog) == []
        assert reimbursement_pairing_lines(caplog) == [
            reimbursement_paired_in_another_order("amount and text"),
            EXPENSE_NUMBERS_MOVED,
        ]

    def test_expenses_moss_reordered_exchange_their_numbers(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """Moss reorders the EXPENSES themselves, and both exports follow: the
        pairing is the export order again and says nothing, but the same two
        uuids come back under each other's number. The plan exchanges them in
        one go -- the constraint is deferred until the level-2 apply is
        through -- and the run reports the move."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = expense_b_bookings(rw_conn)

        reordered = write_exports(
            tmp_path / "P1-expenses-swapped",
            "P1",
            reimbursement_expenses=EXPENSES_SWAPPED,
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, reordered, now=LATER)

        assert [plan_counts(plan) for plan in plans] == [(0, 0), (0, 2), (0, 0)]
        assert reimbursement_expense_rows(rw_conn) == expected_reimbursement_expenses(
            [EXPENSE_B_UUID, EXPENSE_A_UUID]
        )
        # The expenses moved, their splits did not: an expense's own bookings
        # keep their rows, their numbers and their content.
        assert expense_b_bookings(rw_conn) == before
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

        assert reorder_lines(caplog) == []
        assert reimbursement_pairing_lines(caplog) == [EXPENSE_NUMBERS_MOVED]

    def test_an_undecidable_reimbursement_keeps_the_export_order(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """Two expenses sharing their amount AND their text: which balance row
        belongs to which of them is undecidable on every step. The export order
        stands, which is what the warning is for, and the run stays
        idempotent."""
        files = write_exports(
            tmp_path / "P1-ambiguous",
            "P1",
            reimbursement_expenses=EXPENSES_AMBIGUOUS,
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            run_import(rw_conn, ctx, files, now=NOW)

        stored = [
            {
                "expense_number": number,
                "moss_expense_uuid": expense_uuid,
                "signed_expense_base_amount": decimal.Decimal("-4.00"),
                "expense_posting_text": SAME_EXPENSE_TEXT,
            }
            for number, expense_uuid in enumerate(
                (EXPENSE_A_UUID, EXPENSE_B_UUID), start=1
            )
        ]
        assert reimbursement_expense_rows(rw_conn) == stored
        assert reimbursement_pairing_lines(caplog) == [UNPAIRABLE_REIMBURSEMENT]

        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            again = run_import(rw_conn, ctx, files, now=LATER)

        assert reimbursement_pairing_lines(caplog) == [UNPAIRABLE_REIMBURSEMENT]
        assert [plan_counts(plan) for plan in again] == [(0, 0), (0, 0), (0, 0)]
        assert reimbursement_expense_rows(rw_conn) == stored
        # One split per expense here, so the reimbursement has one booking less.
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS - 1)

    def test_a_reimbursement_whose_texts_disagree_is_skipped(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The balance rows name a text the reimbursement export does not have.
        The amounts alone still pair them, but a pairing whose two sides
        disagree about what they describe is one the importer cannot vouch for:
        the detail gate refuses the whole transaction and names it. The other
        three kinds are imported regardless."""
        files = write_exports(
            tmp_path / "P1-texts-disagree", "P1", reimbursement_notes_disagree=True
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            run_import(rw_conn, ctx, files, now=NOW)

        payout = wallet_uuid("P1", "reimbursement-payout")
        assert detail_gate_lines(caplog) == [
            (
                f"detail gate -- SKIPPED {payout} (MossReimbursement): texts do "
                f"not line up with the balance rows in {REIMBURSEMENT_UUID}"
            )
        ]
        assert reimbursement_pairing_lines(caplog) == []
        assert reimbursement_expense_rows(rw_conn) == []
        # The card payment, the invoice and the top-up, with their own rows.
        assert counts(rw_conn) == (L1_ROWS - 1, L2_ROWS - 2, L3_ROWS - 3)


class Test_import_moss_transactions_invoice_pairing:
    """An invoice arrives in two exports at once, and a profile may order them
    differently: the invoice export carries the line with its account and cost
    center, the balance export what was paid for it and the account's NAME. The
    two are paired by CONTENT -- see the INVOICE LINE PAIRING section of the
    importer -- so that no booking ends up with one line's amount next to
    another line's account."""

    def test_swapped_balance_rows_keep_every_line_on_its_own_amount(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The balance export lists its two rows in the other order while the
        invoice export keeps its own. Pairing them by position would put each
        line's amount and account name on the OTHER line's booking; pairing
        them by account and amount puts every line on its own."""
        crossed = write_exports(
            tmp_path / "P1-crossed", "P1", swap_invoice_balance_rows=True
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, crossed, now=NOW)

        assert [plan_counts(plan) for plan in plans] == [
            (L1_ROWS, 0),
            (L2_ROWS, 0),
            (L3_ROWS, 0),
        ]
        assert invoice_bookings(rw_conn) == expected_invoice_bookings(INVOICE_LINES)

        # The order of the balance export says nothing about the splits, so
        # nothing is renumbered -- only the pairing is reported.
        assert reorder_lines(caplog) == []
        assert pairing_lines(caplog) == [
            paired_in_another_order("expense account and amount")
        ]

        # And the same files a second time have nothing left to say.
        again = run_import(rw_conn, ctx, crossed, now=LATER)
        assert [plan_counts(plan) for plan in again] == [(0, 0), (0, 0), (0, 0)]
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_a_crossed_stored_invoice_is_corrected_and_the_change_reported(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The same invoice, but already stored as a positional pairing left
        it: the two rows hold each other's amount and account name. The import
        moves both back onto their own line -- two columns on two rows -- and
        says so before the plan is applied, because a plan alone shows only
        that two amounts changed."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        cross_the_stored_invoice_pairing(rw_conn)
        assert invoice_bookings(rw_conn) != expected_invoice_bookings(INVOICE_LINES)

        crossed = write_exports(
            tmp_path / "P1-crossed", "P1", swap_invoice_balance_rows=True
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, crossed, now=LATER)

        assert plan_counts(plans[2]) == (0, 2)
        assert preview_counts(caplog, TABLE_BOOKINGS, UPDATE_COLUMNS) == {
            "signed_base_amount": 2,
            "other_moss_columns": 2,
        }
        assert invoice_bookings(rw_conn) == expected_invoice_bookings(INVOICE_LINES)
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

        assert reorder_lines(caplog) == []
        assert pairing_lines(caplog) == [
            paired_in_another_order("expense account and amount"),
            AMOUNTS_MOVED,
        ]

    def test_lines_on_one_account_are_paired_through_the_stored_bookings(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """Two lines on the SAME expense account, told apart only by their cost
        center -- which the balance export has no column for -- and balance rows
        that name no account either. Nothing in the two files can pair them, so
        the first import falls back to the export order and warns. Once the
        invoice is stored, the stored bookings decide it: they carry the cost
        center, and their amounts say which balance row is which."""
        first = write_exports(
            tmp_path / "P1-one-account",
            "P1",
            invoice_lines=INVOICE_LINES_SAME_ACCOUNT,
            balance_names_the_account=False,
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            run_import(rw_conn, ctx, first, now=NOW)

        stored = expected_invoice_bookings(
            INVOICE_LINES_SAME_ACCOUNT, with_account_names=False
        )
        assert invoice_bookings(rw_conn) == stored
        assert pairing_lines(caplog) == [UNPAIRABLE_INVOICE]

        crossed = write_exports(
            tmp_path / "P1-one-account-crossed",
            "P1",
            invoice_lines=INVOICE_LINES_SAME_ACCOUNT,
            balance_names_the_account=False,
            swap_invoice_balance_rows=True,
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, crossed, now=LATER)

        assert pairing_lines(caplog) == [paired_in_another_order("the stored bookings")]
        # Every line is back on the amount it already had: nothing to write.
        assert [plan_counts(plan) for plan in plans] == [(0, 0), (0, 0), (0, 0)]
        assert invoice_bookings(rw_conn) == stored
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_an_undecidable_invoice_keeps_the_export_order(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """Two lines sharing their expense account AND their amount: which
        balance row belongs to which line is undecidable on every step, the
        stored bookings included -- their amounts are equal as well. The export
        order stands, which is what the warning is for, and the run stays
        idempotent."""
        files = write_exports(
            tmp_path / "P1-ambiguous",
            "P1",
            invoice_lines=INVOICE_LINES_AMBIGUOUS,
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            run_import(rw_conn, ctx, files, now=NOW)

        assert pairing_lines(caplog) == [UNPAIRABLE_INVOICE]
        stored = expected_invoice_bookings(INVOICE_LINES_AMBIGUOUS)
        assert invoice_bookings(rw_conn) == stored

        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            again = run_import(rw_conn, ctx, files, now=LATER)

        assert pairing_lines(caplog) == [UNPAIRABLE_INVOICE]
        assert [plan_counts(plan) for plan in again] == [(0, 0), (0, 0), (0, 0)]
        assert invoice_bookings(rw_conn) == stored
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)


class Test_import_moss_transactions_split_reorder:
    """The L3 key is POSITIONAL, and Moss reorders the splits of an expense
    between exports -- a card payment's splits, a reimbursement expense's
    splits and an invoice's lines alike. Every test here starts from a P1
    import and then imports the same objects in another shape."""

    def test_reordered_splits_are_renumbered_instead_of_rewritten(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The two splits of expense B swap places in the CSV: the booking ROWS
        move to each other's sub-row number and keep every content column --
        which is what protects what hangs off a row id (its DATEV booking, the
        Beitragsbuchung pointing at it). A positional key alone would instead
        have rewritten both rows' account, cost center, amount and text."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = expense_b_bookings(rw_conn)
        assert [row["sub_row_number"] for row in before] == [1, 2]
        assert all(row["updated_at"] is None for row in before)

        swapped = write_exports(tmp_path / "P1-swapped", "P1", splits=SPLITS_SWAPPED)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, swapped, now=LATER)

        after = {row["id"]: row for row in expense_b_bookings(rw_conn)}
        # The same rows, each with the content it had ...
        assert set(after) == {row["id"] for row in before}
        for row in before:
            assert booking_content(after[row["id"]]) == booking_content(row)
            assert after[row["id"]]["updated_at"] is not None
        # ... and the two of them exchanged their positions.
        assert {row["id"]: row["sub_row_number"] for row in after.values()} == {
            before[0]["id"]: 2,
            before[1]["id"]: 1,
        }

        # The recomputed plan has nothing left to write, on any level.
        assert [plan_counts(plan) for plan in plans] == [(0, 0), (0, 0), (0, 0)]
        assert preview_counts(caplog, TABLE_BOOKINGS, UPDATE_COLUMNS) == {}
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

        messages = importer_records(caplog, logging.INFO)
        assert len(reorder_lines(caplog)) == 1
        assert "reimbursement, expense" in reorder_lines(caplog)[0]
        assert "sub-row 1 -> 2, 2 -> 1" in reorder_lines(caplog)[0]
        assert any(
            message.startswith("Renumbered 2 booking row(s) in 1 expense(s)")
            for message in messages
        )
        assert any("plan recomputed" in message for message in messages)
        assert any(message.startswith("Post-run checks passed") for message in messages)

    def test_a_reorder_leaves_the_other_splits_alone(self, rw_conn, ctx, tmp_path):
        """Only the reordered expense is touched: expense A's single split, the
        card splits, the invoice lines and the top-up booking keep their
        sub-row numbers and are never written."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = sub_row_numbers(rw_conn)
        untouched = rw_conn.execute(
            "SELECT b.id FROM moss_bookings b JOIN moss_expenses e"
            " ON e.id = b.moss_expense_id WHERE e.moss_expense_uuid <> %s"
            " ORDER BY b.id",
            (EXPENSE_B_UUID,),
        ).fetchall()

        swapped = write_exports(tmp_path / "P1-swapped", "P1", splits=SPLITS_SWAPPED)
        run_import(rw_conn, ctx, swapped, now=LATER)

        # The sub-row numbers of an expense are a set, so this is unchanged --
        # what moved is WHICH row carries which number inside expense B.
        assert sub_row_numbers(rw_conn) == before
        assert (
            rw_conn.execute(
                "SELECT b.id FROM moss_bookings b JOIN moss_expenses e"
                " ON e.id = b.moss_expense_id WHERE e.moss_expense_uuid <> %s"
                " AND b.updated_at IS NULL ORDER BY b.id",
                (EXPENSE_B_UUID,),
            ).fetchall()
            == untouched
        )

    def test_a_reorder_keeps_what_the_app_wrote_on_the_row(
        self, rw_conn, ctx, tmp_path
    ):
        """A booking row is not anonymous: the app writes on it and links to
        it. `comment` stands in for those links here (it needs no foreign key):
        it must stay with the split it was written for, and therefore move
        along to the other sub-row number."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        first = expense_b_bookings(rw_conn)[0]
        rw_conn.execute(
            "UPDATE moss_bookings SET comment = %s WHERE id = %s",
            ("Test comment written by the app", first["id"]),
        )

        swapped = write_exports(tmp_path / "P1-swapped", "P1", splits=SPLITS_SWAPPED)
        run_import(rw_conn, ctx, swapped, now=LATER)

        commented = [row for row in expense_b_bookings(rw_conn) if row["comment"] != ""]
        assert len(commented) == 1
        assert commented[0]["id"] == first["id"]
        assert booking_content(commented[0]) == booking_content(first)
        assert commented[0]["sub_row_number"] == 2

    def test_ambiguous_splits_are_never_renumbered(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """Two splits of one expense sharing cost center, account AND amount:
        which stored row is which cannot be decided, so the match is refused
        and the ordinary positional plan rewrites the columns as before."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        ambiguous = write_exports(
            tmp_path / "P1-ambiguous", "P1", splits=SPLITS_AMBIGUOUS
        )

        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, ambiguous, now=LATER)

        assert reorder_lines(caplog) == []
        assert plan_counts(plans[2]) == (0, 2)
        rows = expense_b_bookings(rw_conn)
        assert [row["sub_row_number"] for row in rows] == [1, 2]
        assert [row["account_number"] for row in rows] == [EXPENSE_ACCOUNT_ONE] * 2
        assert [row["signed_base_amount"] for row in rows] == [
            decimal.Decimal("-4.50")
        ] * 2
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_a_genuine_account_change_is_not_a_reorder(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """One split really moves to another expense account. The two sides are
        not the same splits in another order, so nothing is renumbered and the
        plan updates the one column, on the row that sits where it sat."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = expense_b_bookings(rw_conn)
        changed = write_exports(
            tmp_path / "P1-changed", "P1", splits=SPLITS_CHANGED_ACCOUNT
        )

        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, changed, now=LATER)

        assert reorder_lines(caplog) == []
        assert plan_counts(plans[2]) == (0, 1)
        assert preview_counts(caplog, TABLE_BOOKINGS, UPDATE_COLUMNS) == {
            "account_number": 1
        }
        after = expense_b_bookings(rw_conn)
        assert [row["id"] for row in after] == [row["id"] for row in before]
        assert [row["sub_row_number"] for row in after] == [1, 2]
        assert after[1]["account_number"] == EXPENSE_ACCOUNT_THREE
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_a_third_split_next_to_a_reorder_is_not_renumbered(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The swapped export gains a third split: stored and incoming are no
        longer the same count, so the match is refused. The positional plan
        rewrites the two rows and inserts the new split."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        grown = write_exports(
            tmp_path / "P1-grown", "P1", splits=SPLITS_SWAPPED_PLUS_ONE
        )

        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, grown, now=LATER)

        assert reorder_lines(caplog) == []
        assert plan_counts(plans[2]) == (1, 2)
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS + 1)
        rows = expense_b_bookings(rw_conn)
        assert [row["sub_row_number"] for row in rows] == [1, 2, 3]
        assert [row["account_number"] for row in rows] == [
            EXPENSE_ACCOUNT_TWO,
            EXPENSE_ACCOUNT_ONE,
            EXPENSE_ACCOUNT_THREE,
        ]

    def test_dry_run_reports_the_reorder_and_writes_nothing(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The --dry-run branch runs the same detection read-only: it names the
        expense and how many rows would be renumbered, and leaves the three
        tables exactly as they were."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = expense_b_bookings(rw_conn)
        swapped = write_exports(tmp_path / "P1-swapped", "P1", splits=SPLITS_SWAPPED)

        caplog.clear()
        with caplog.at_level(logging.INFO):
            plan = run_dry(rw_conn, ctx, swapped)

        assert plan_counts(plan) == (0, 0)
        assert len(reorder_lines(caplog)) == 1
        assert [
            message
            for message in importer_records(caplog, logging.WARNING)
            if "would be renumbered" in message
        ] == [
            (
                "[--dry-run] 2 booking row(s) in 1 expense(s) would be renumbered; "
                "the plan above is the one BEFORE that."
            )
        ]
        assert expense_b_bookings(rw_conn) == before
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_reordered_card_splits_are_renumbered_instead_of_rewritten(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """A card payment's splits hang off its shell expense on the same
        positional key, so Moss reordering them is the same situation: the two
        booking rows move to each other's sub-row number instead of having
        their account, amount and text rewritten."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = bookings_of(rw_conn, CARD_UUID)
        assert [row["sub_row_number"] for row in before] == [1, 2]

        swapped = write_exports(tmp_path / "P1-card", "P1", swap_card_splits=True)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, swapped, now=LATER)

        assert_rows_exchanged_their_positions(before, bookings_of(rw_conn, CARD_UUID))

        # The recomputed plan has nothing left to write, on any level.
        assert [plan_counts(plan) for plan in plans] == [(0, 0), (0, 0), (0, 0)]
        assert preview_counts(caplog, TABLE_BOOKINGS, UPDATE_COLUMNS) == {}
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

        assert len(reorder_lines(caplog)) == 1
        assert "card payment, expense" in reorder_lines(caplog)[0]
        assert "sub-row 1 -> 2, 2 -> 1" in reorder_lines(caplog)[0]

    def test_reordered_invoice_lines_are_renumbered_instead_of_rewritten(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """An invoice's lines are its shell expense's splits. A line takes its
        number, account and cost center from the invoice export and its amount
        from the balance row it is paired with, so a reorder shows up in both
        exports -- and is then recognised like any other."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = bookings_of(rw_conn, INVOICE_UUID)
        assert [row["sub_row_number"] for row in before] == [1, 2]

        swapped = write_exports(tmp_path / "P1-invoice", "P1", swap_invoice_lines=True)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, swapped, now=LATER)

        assert_rows_exchanged_their_positions(
            before, bookings_of(rw_conn, INVOICE_UUID)
        )

        assert [plan_counts(plan) for plan in plans] == [(0, 0), (0, 0), (0, 0)]
        assert preview_counts(caplog, TABLE_BOOKINGS, UPDATE_COLUMNS) == {}
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

        assert len(reorder_lines(caplog)) == 1
        assert "invoice, expense" in reorder_lines(caplog)[0]
        assert "sub-row 1 -> 2, 2 -> 1" in reorder_lines(caplog)[0]

    def test_the_renumbering_is_one_statement_and_uses_no_parking_numbers(
        self, rw_conn, ctx, tmp_path
    ):
        """The deferrable constraint takes the uniqueness check off the
        statement, so the two rows exchange their sub-row numbers inside ONE
        UPDATE: a trigger on moss_bookings sees a single statement, and the only
        numbers written are the two the expense has. No row is ever parked below
        sub-row 1."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = expense_b_bookings(rw_conn)
        assert [row["sub_row_number"] for row in before] == [1, 2]
        watch_booking_updates(rw_conn)

        swapped = write_exports(tmp_path / "P1-swapped", "P1", splits=SPLITS_SWAPPED)
        run_import(rw_conn, ctx, swapped, now=LATER)

        # The rows kept their ids and their content and swapped their positions.
        assert_rows_exchanged_their_positions(before, expense_b_bookings(rw_conn))
        statements, written_numbers = booking_update_audit(rw_conn)
        assert statements == 1
        assert written_numbers == [1, 2]
        assert (
            rw_conn.execute(
                "SELECT count(*) FROM moss_bookings WHERE sub_row_number < 1"
            ).fetchone()[0]
            == 0
        )
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

    def test_the_renumbering_ends_on_a_unique_numbering(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The one UPDATE passes THROUGH a numbering that is not unique: the row
        moving onto sub-row 2 takes it while the row leaving it still holds it.
        Only a deferred check tolerates that, and the importer asks for the
        check right after the statement -- so this import getting through at all
        is the proof that the renumbering ENDED on a unique assignment, and the
        expense's sub-row numbers are 1..n again."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        stored = expense_b_bookings(rw_conn)
        assert [row["sub_row_number"] for row in stored] == [1, 2]

        swapped = write_exports(tmp_path / "P1-swapped", "P1", splits=SPLITS_SWAPPED)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            run_import(rw_conn, ctx, swapped, now=LATER)

        after = expense_b_bookings(rw_conn)
        assert [row["sub_row_number"] for row in after] == list(
            range(1, len(stored) + 1)
        )
        assert len({row["sub_row_number"] for row in after}) == len(after)
        assert len(reorder_lines(caplog)) == 1
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)

        # And the check the importer asked for is in force from then on, on
        # this transaction: a duplicate now fails ON ITS STATEMENT, where a
        # still-deferred one would go unnoticed until a COMMIT this test never
        # reaches. The savepoint takes that duplicate back.
        with pytest.raises(psycopg.errors.UniqueViolation):
            with rw_conn.transaction():
                rw_conn.execute(
                    "UPDATE moss_bookings SET sub_row_number = %s WHERE id = %s",
                    (after[1]["sub_row_number"], after[0]["id"]),
                )
        assert expense_b_bookings(rw_conn) == after

    def test_a_database_without_the_deferrable_constraint_stops_the_run(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The single-statement renumbering needs the L3 key to be a deferrable
        constraint, so the importer requires it of the schema before it reads
        anything. Where the key is still a plain unique index, the run names the
        missing wagon migration and stops -- instead of failing on a bare SQL
        error halfway through. The constraint is put back inside this (rolled
        back) transaction."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = expense_b_bookings(rw_conn)
        swapped = write_exports(tmp_path / "P1-swapped", "P1", splits=SPLITS_SWAPPED)

        replace_sub_row_constraint_with_plain_index(rw_conn)
        assert sub_row_constraint_definition(rw_conn) is None
        caplog.clear()
        try:
            with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
                with pytest.raises(SystemExit) as raised:
                    run_import(rw_conn, ctx, swapped, now=LATER)
        finally:
            restore_sub_row_constraint(rw_conn)

        assert raised.value.code == 1
        assert importer_records(caplog, logging.ERROR) == [
            (
                f"{TABLE_BOOKINGS} has no deferrable unique constraint"
                f" {SUB_ROW_CONSTRAINT} on (moss_expense_id, sub_row_number):"
                f" the wagon migration {SUB_ROW_CONSTRAINT_MIGRATION} is not"
                " applied to this database, so reordered splits could not be"
                " renumbered."
            )
        ]
        # Nothing was written, and the fixture is whole again.
        assert expense_b_bookings(rw_conn) == before
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)
        assert sub_row_constraint_definition(rw_conn) == (
            "UNIQUE (moss_expense_id, sub_row_number) DEFERRABLE INITIALLY DEFERRED"
        )

    def test_a_top_up_is_never_renumbered(self, rw_conn, ctx, tmp_path, caplog):
        """A top-up's expense holds a single booking. The changed amount puts it
        on the candidate list, but one row admits no bijection other than the
        identity, so the ordinary positional plan writes the new amount onto the
        row that was already there."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        top_up_expense = wallet_uuid("P1", "top-up")
        before = bookings_of(rw_conn, top_up_expense)
        assert [row["sub_row_number"] for row in before] == [1]

        changed = write_exports(
            tmp_path / "P1-top-up", "P1", top_up_amount=OTHER_TOP_UP_AMOUNT
        )
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, changed, now=LATER)

        assert reorder_lines(caplog) == []
        assert plan_counts(plans[2]) == (0, 1)
        assert preview_counts(caplog, TABLE_BOOKINGS, UPDATE_COLUMNS) == {
            "signed_base_amount": 1
        }
        after = bookings_of(rw_conn, top_up_expense)
        assert [row["id"] for row in after] == [row["id"] for row in before]
        assert [row["sub_row_number"] for row in after] == [1]
        assert after[0]["signed_base_amount"] == decimal.Decimal(OTHER_TOP_UP_AMOUNT)
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)


class Test_import_moss_transactions_no_updated_at:
    """--no-updated-at: the run writes everything it would write anyway, but no
    `updated_at` -- a row that changes keeps the timestamp it has, an inserted
    row still gets its `created_at`. Every test here pairs the flag with the
    SAME sequence without it, so what is asserted is the flag and nothing else.

    Two places write a timestamp, and both are covered below: the three plan
    applies (``touch=False``) and the renumbering of reordered splits, whose
    statement drops the `updated_at` assignment."""

    @staticmethod
    def changed_exports(tmp_path) -> list[str]:
        """The P1 objects with the wallet funded by another amount: one UPDATE
        on each of the three levels -- the transaction's total, its expense's
        amount and the single booking of the top-up."""
        return write_exports(
            tmp_path / "P1-top-up", "P1", top_up_amount=OTHER_TOP_UP_AMOUNT
        )

    def test_without_the_flag_the_changed_rows_are_stamped(
        self, rw_conn, ctx, tmp_path
    ):
        """The control: one row per level changes and is stamped, every other
        row keeps its NULL, and no created_at is rewritten."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = all_timestamps(rw_conn)
        assert stamped_row_ids(before) == {
            TABLE_TRANSACTIONS: [],
            TABLE_EXPENSES: [],
            TABLE_BOOKINGS: [],
        }

        plans = run_import(rw_conn, ctx, self.changed_exports(tmp_path), now=LATER)
        assert [plan_counts(plan) for plan in plans] == [(0, 1), (0, 1), (0, 1)]

        after = all_timestamps(rw_conn)
        stamped = stamped_row_ids(after)
        assert [len(ids) for ids in stamped.values()] == [1, 1, 1]
        for table, rows in after.items():
            assert {row_id: created for row_id, (created, _) in rows.items()} == {
                row_id: created for row_id, (created, _) in before[table].items()
            }

    def test_with_the_flag_the_changed_rows_keep_their_updated_at(
        self, rw_conn, ctx, tmp_path
    ):
        """The same sequence with the flag: the change IS written, no row's
        `updated_at` moves, and the rows the first run inserted carry that run's
        `now` in `created_at` -- the column's database default would otherwise
        have filled it from the database clock."""
        run_import(
            rw_conn,
            ctx,
            write_exports(tmp_path / "P1", "P1"),
            now=NOW,
            no_updated_at=True,
        )
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)
        before = all_timestamps(rw_conn)
        for rows in before.values():
            for created, updated in rows.values():
                assert created == NOW
                assert updated is None

        plans = run_import(
            rw_conn,
            ctx,
            self.changed_exports(tmp_path),
            now=LATER,
            no_updated_at=True,
        )
        assert [plan_counts(plan) for plan in plans] == [(0, 1), (0, 1), (0, 1)]

        # The change itself reached all three levels ...
        changed_amount = decimal.Decimal(OTHER_TOP_UP_AMOUNT)
        assert (
            transactions_by_type(rw_conn)["MossTopUp"]["signed_total_base_amount"]
            == changed_amount
        )
        assert [
            row["signed_base_amount"]
            for row in bookings_of(rw_conn, wallet_uuid("P1", "top-up"))
        ] == [changed_amount]
        # ... and not one timestamp moved with it.
        assert all_timestamps(rw_conn) == before

    def test_with_the_flag_a_renumbering_leaves_updated_at_alone(
        self, rw_conn, ctx, tmp_path, caplog
    ):
        """The renumbering of reordered splits is a statement of its own, so it
        is the one place the flag changes the SQL: the two rows still exchange
        their sub-row numbers, and both keep the timestamp they have."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = expense_b_bookings(rw_conn)
        assert [row["updated_at"] for row in before] == [None, None]

        swapped = write_exports(tmp_path / "P1-swapped", "P1", splits=SPLITS_SWAPPED)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=IMPORTER_LOGGER):
            plans = run_import(rw_conn, ctx, swapped, now=LATER, no_updated_at=True)

        after = expense_b_bookings(rw_conn)
        assert_rows_exchanged_their_positions(before, after)
        assert [row["updated_at"] for row in after] == [None, None]

        # The recomputed plan has nothing left to write, on any level.
        assert [plan_counts(plan) for plan in plans] == [(0, 0), (0, 0), (0, 0)]
        assert counts(rw_conn) == (L1_ROWS, L2_ROWS, L3_ROWS)
        messages = importer_records(caplog, logging.INFO)
        assert len(reorder_lines(caplog)) == 1
        assert any(
            message.startswith("Renumbered 2 booking row(s) in 1 expense(s)")
            for message in messages
        )
        assert any(message.startswith("Post-run checks passed") for message in messages)

    def test_without_the_flag_a_renumbering_stamps_the_moved_rows(
        self, rw_conn, ctx, tmp_path
    ):
        """The control for that statement: the same reorder stamps both rows it
        moved, while every row it did not move keeps its NULL."""
        run_import(rw_conn, ctx, write_exports(tmp_path / "P1", "P1"), now=NOW)
        before = expense_b_bookings(rw_conn)
        assert [row["updated_at"] for row in before] == [None, None]

        swapped = write_exports(tmp_path / "P1-swapped", "P1", splits=SPLITS_SWAPPED)
        run_import(rw_conn, ctx, swapped, now=LATER)

        after = expense_b_bookings(rw_conn)
        assert_rows_exchanged_their_positions(before, after)
        assert [row["updated_at"] is not None for row in after] == [True, True]
        assert stamped_row_ids(all_timestamps(rw_conn))[TABLE_BOOKINGS] == sorted(
            row["id"] for row in before
        )
