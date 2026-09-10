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

MOSS_BALANCE_ACCOUNT = "36100"  # CLEARING
CASH_IN_TRANSIT_ACCOUNT = "13720"  # TRANSIT
EXPENSE_ACCOUNT_ONE = "61000"  # EXPENSE
EXPENSE_ACCOUNT_TWO = "62000"  # EXPENSE
CREDITOR_ACCOUNT_ONE = "700099"  # CREDITOR
CREDITOR_ACCOUNT_TWO = "700098"  # CREDITOR

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
    "Row Number",
    "Sub-row Number",
    "Expense Account - Number",
    "Cost Center - Number",
    "Cost Carrier - Number",
    "Booking Text",
    "Parent Booking Text",
    "Amount",
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


def _card_rows() -> list[dict]:
    """One card payment with two splits; profile-independent in every column."""
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
    return [
        shared
        | {
            "Sub-row Number": "1",
            "Home Amount": "-1.00",
            "Account Number": EXPENSE_ACCOUNT_ONE,
            "Note": "Test card split one",
        },
        shared
        | {
            "Sub-row Number": "2",
            "Home Amount": "-2.00",
            "Account Number": EXPENSE_ACCOUNT_TWO,
            "Note": "Test card split two",
        },
    ]


def _balance_rows(
    profile: str,
    *,
    reimbursement_payout: uuid.UUID,
    rich_profile: bool,
    reimbursement_recipient: tuple[str, str],
) -> list[dict]:
    """The wallet movements: the reimbursement payout (one row per expense),
    the invoice payout (one row per line) and the top-up.

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
        "Supplier Account": CREDITOR_ACCOUNT_TWO,
        "Payment Reference": "Test invoice payout",
        "Reason for Purchase": f"{INVOICE_PAYEE}; ; -",
        "Category": "Test expense account",
    }
    return [
        *(
            payout(
                reimbursement_payout,
                sub_row,
                amount,
                REIMBURSEMENT_PAYMENT_DATE,
                REIMBURSEMENT_BOOKING_DATE,
                reimbursement_recipient,
                # the lean layout knows no team for a reimbursement payout
                "",
                **reimbursement,
            )
            for sub_row, amount in ((1, "-4.00"), (2, "-9.00"))
        ),
        *(
            payout(
                invoice_payout,
                sub_row,
                amount,
                INVOICE_PAYMENT_DATE,
                INVOICE_BOOKING_DATE,
                INVOICE_RECIPIENT,
                OTHER_TEAM_NAME,
                **invoice,
            )
            for sub_row, amount in ((1, "-5.00"), (2, "-6.00"))
        ),
        movement(
            top_up,
            1,
            TOP_UP_AMOUNT,
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


def _reimbursement_rows() -> list[dict]:
    """Expense A with one split, expense B with two -- the level the balance
    export cannot see."""
    shared = {
        "Unique Reimbursement ID": str(REIMBURSEMENT_UUID),
        "Submitted On": "2099-02-25",
        "Creation date": "2099-02-20",
        "Reimbursement Name": "Test reimbursement",
        "Submitted By": "Test Person",
        "Expense type": "General",
        "Cost Center - Name": "3100",
    }
    return [
        shared
        | {
            "Unique Expense ID": str(EXPENSE_A_UUID),
            "Row Number": "1",
            "Sub-row Number": "1",
            "Expense Name": "Test expense A",
            "Purchased On": "2099-02-18",
            "Parent Booking Text": "Test expense A",
            "Expense Account": EXPENSE_ACCOUNT_ONE,
            "Expense Description": "Test expense A split one",
            "Amount": "4.00",
        },
        shared
        | {
            "Unique Expense ID": str(EXPENSE_B_UUID),
            "Row Number": "2",
            "Sub-row Number": "1",
            "Expense Name": "Test expense B",
            "Purchased On": "2099-02-19",
            "Parent Booking Text": "Test expense B",
            "Expense Account": EXPENSE_ACCOUNT_ONE,
            "Expense Description": "Test expense B split one",
            "Amount": "4.00",
        },
        shared
        | {
            "Unique Expense ID": str(EXPENSE_B_UUID),
            "Row Number": "3",
            "Sub-row Number": "2",
            "Expense Name": "Test expense B",
            "Purchased On": "2099-02-19",
            "Parent Booking Text": "Test expense B",
            "Expense Account": EXPENSE_ACCOUNT_TWO,
            "Expense Description": "Test expense B split two",
            "Amount": "5.00",
        },
    ]


def _invoice_rows() -> list[dict]:
    """Two invoice lines -- where an invoice's cost centers come from."""
    shared = {
        "Invoice ID": str(INVOICE_UUID),
        "Parent Booking Text": "Test invoice",
        "Invoice Date": "2099-02-15",
        "Due Date": "2099-03-15",
        "Delivery Date": "2099-02-14",
        "Submitted Date": "2099-02-16",
        "Invoice Status": "Completed",
        "Submitted By": "Test Person",
    }
    return [
        shared
        | {
            "Row Number": "1",
            "Sub-row Number": "1",
            "Expense Account - Number": EXPENSE_ACCOUNT_ONE,
            "Cost Center - Number": "3100",
            "Booking Text": "Test invoice line one",
            "Amount": "5.00",
        },
        shared
        | {
            "Row Number": "2",
            "Sub-row Number": "2",
            "Expense Account - Number": EXPENSE_ACCOUNT_TWO,
            "Cost Center - Number": "3200",
            "Booking Text": "Test invoice line two",
            "Amount": "6.00",
        },
    ]


def write_exports(
    directory: pathlib.Path,
    profile: str,
    *,
    reimbursement_payout: uuid.UUID | None = None,
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
    """
    directory.mkdir(parents=True, exist_ok=True)
    payout = reimbursement_payout or wallet_uuid(profile, "reimbursement-payout")
    balance_rows = _balance_rows(
        profile,
        reimbursement_payout=payout,
        rich_profile=profile != LEAN_PROFILE,
        reimbursement_recipient=REIMBURSEMENT_RECIPIENT[profile],
    )
    return [
        _write_csv(
            directory / f"transactions_{profile}.csv", CARD_HEADERS, _card_rows()
        ),
        _write_csv(
            directory / f"balance-movements_{profile}.csv",
            BALANCE_HEADERS,
            balance_rows,
        ),
        _write_csv(
            directory / f"reimbursements_{profile}.csv",
            REIMBURSEMENT_HEADERS,
            _reimbursement_rows(),
        ),
        _write_csv(
            directory / f"invoices_{profile}.csv", INVOICE_HEADERS, _invoice_rows()
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
    transactions, expenses, bookings = importer._build_records(by_kind)
    importer._verify_sum_invariant(transactions, expenses, bookings)
    source_files = importer._source_file_by_transaction(by_kind)

    importer._require_object_uuid_column(conn)
    resolved = importer._resolve_transactions(
        transactions, importer._load_stored_transactions(conn)
    )
    origin = importer._apply_resolution(
        resolved, transactions, expenses, bookings, source_files
    )
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


def run_import(conn, ctx, files, *, now):
    """The apply path of main(), step for step, on `conn`. Returns the three
    plans (L1, L2, L3)."""
    transaction_plan, expenses, bookings, origin = _plan_transactions(conn, ctx, files)
    wallet_id = importer._wallet_fin_account_id(conn)
    importer._apply(
        transaction_plan,
        conn,
        TABLE_TRANSACTIONS,
        now,
        insert_only={"fin_account_id": wallet_id} if wallet_id else None,
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
    importer._apply(expense_plan, conn, TABLE_EXPENSES, now)

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
        ["moss_expense_id", "sub_row_number"],
        importer._plannable(bookings, "_transaction_ref", "_expense_ref"),
    )
    importer._apply(booking_plan, conn, TABLE_BOOKINGS, now)

    importer._post_run_checks(
        conn,
        importer._touched_transaction_ids(
            (transaction_plan, transaction_ids, TABLE_TRANSACTIONS),
            (expense_plan, expense_owners, TABLE_EXPENSES),
            (booking_plan, booking_owners, TABLE_BOOKINGS),
        ),
    )
    return transaction_plan, expense_plan, booking_plan


def run_dry(conn, ctx, files):
    """The --dry-run branch of main(): L1 is planned, L2/L3 only for the
    transactions that already exist, nothing is applied. Returns the L1 plan --
    _dry_run_lower_levels returns none of its own, so what it planned for L2/L3
    is only visible in the log."""
    transaction_plan, expenses, bookings, _ = _plan_transactions(conn, ctx, files)
    known = importer._id_map(conn, TABLE_TRANSACTIONS, ["moss_object_uuid"])
    importer._dry_run_lower_levels(conn, ctx, known, expenses, bookings)
    return transaction_plan


# ================================================================ read helpers


def counts(conn) -> tuple[int, int, int]:
    """(transactions, expenses, bookings) as THIS session sees them."""
    row = conn.execute(
        "SELECT (SELECT count(*) FROM moss_transactions),"
        " (SELECT count(*) FROM moss_expenses),"
        " (SELECT count(*) FROM moss_bookings)"
    ).fetchone()
    return (row[0], row[1], row[2])


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
