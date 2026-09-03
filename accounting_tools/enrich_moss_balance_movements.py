#!/usr/bin/env -S uv run
"""Enrich moss_balance_movements with the reimbursement booking detail needed by
the Moss-unification migration (doc/plans/2026-08_moss-transaction-unification.md).

WHY THIS EXISTS
---------------
The balance-movements export is *lossy*: it collapses each reimbursement
expense's internal Sachkonto/Kostenstelle/Sphaere split into ONE row. The real
splits live only in the reimbursements export. The unification migration runs
against the DATABASE ALONE (it never reads a CSV), so the split detail has to be
in the database BEFORE the migration -- otherwise the migration could only
create one (partly wrong) booking per expense. This script puts it there; the
migration's guard aborts before its first schema change when it is missing.

WHAT IT WRITES
--------------
For EVERY reimbursement row of moss_balance_movements (not just the split ones,
so the migration always finds a complete list) two keys in additional_info:

  moss_bookings      -- JSON array of complete booking rows, one object per L3
                        booking of that expense, each with
                          booking_unique_item_number  <expense uuid>_<Sub-row Number>
                          signed_base_amount          signed like the balance row
                          account_number              "Expense Account"
                          cost_center_number          "Cost Center - Name" (!) -- the
                                                      Moss export mislabels this: it
                                                      holds the cost-center NUMBER
                                                      (verified == distribution field 3)
                          sphere_number               "Cost Carrier - Number" ("000" -> None)
                          distribution_combination    "Distribution combination"
                          booking_posting_text        "Expense Description"
                          moss_unique_item_number     "Unique Item Number" (the raw CSV
                                                      value; the migration stores it on
                                                      the booking under that header)
  moss_expense_uuid  -- the reimbursement's "Unique Expense ID" for that expense

Everything else in additional_info (notably denylist_subject_candidates) is kept
untouched; the two keys are overwritten on a re-run, so the script is idempotent.

HOW BALANCE ROWS ARE MATCHED TO REIMBURSEMENT EXPENSES
------------------------------------------------------
The balance export has no expense id. The match is by ORDER, which is verified
here for every reimbursement before anything is written: the balance rows of a
transaction (ordered by sub_row_number) correspond 1:1 to the reimbursement's
expenses (ordered by the file-global "Row Number"), and the amounts must agree
to the cent. A reimbursement whose sequence does not match is SKIPPED and logged
-- never guessed.

Card, invoice and top-up rows are not touched: their bookings derive 1:1 from
the row itself (an invoice's several balance rows are already its lines).

USAGE
    ./accounting_tools/enrich_moss_balance_movements.py \
        External_Data/Moss_Exports/reimbursements_2026-09-01--04-50_WSJ27.csv
    ... --dry-run              plan only, write nothing
    ... --rollback-for-testing apply, then ROLLBACK
"""

from __future__ import annotations

import collections as _collections
import csv as _csv
import decimal as _decimal
import json as _json
import logging as _logging
import pathlib as _pathlib
import sys as _sys

import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)
_SELF_NAME = _pathlib.Path(__file__).stem

#: Moss CSV dialect: ";" separated, "." decimal point, UTF-8 with BOM.
_CSV_DELIMITER = ";"
_CSV_ENCODING = "utf-8-sig"

#: The keys this script owns in additional_info (overwritten on every run).
_KEY_BOOKINGS = "moss_bookings"
_KEY_EXPENSE_UUID = "moss_expense_uuid"

#: A "Cost Carrier - Number" of all zeros means "no sphere".
_EMPTY_SPHERE = {"", "000", "0"}


def _decimal_or_none(raw: str | None) -> _decimal.Decimal | None:
    """Parse a Moss amount ("." decimal point). Empty -> None."""
    text = (raw or "").strip()
    if not text:
        return None
    return _decimal.Decimal(text)


def _read_reimbursement_expenses(path: _pathlib.Path) -> dict[str, list[dict]]:
    """Read the reimbursements export, grouped reimbursement uuid -> expenses.

    Each expense is ``{"uuid": ..., "total": Decimal, "rows": [csv rows]}``; the
    expenses keep the file order ("Row Number"), the rows inside an expense are
    ordered by "Sub-row Number" (the split index within that expense).
    """
    with path.open(encoding=_CSV_ENCODING, newline="") as handle:
        rows = list(_csv.DictReader(handle, delimiter=_CSV_DELIMITER))

    per_reimbursement: dict[str, list[dict]] = _collections.defaultdict(list)
    seen: dict[tuple[str, str], dict] = {}
    for row in sorted(
        rows, key=lambda r: int((r.get("Row Number") or "0").strip() or 0)
    ):
        reimbursement_uuid = (row.get("Unique Reimbursement ID") or "").strip()
        expense_uuid = (row.get("Unique Expense ID") or "").strip()
        if not reimbursement_uuid or not expense_uuid:
            continue
        key = (reimbursement_uuid, expense_uuid)
        expense = seen.get(key)
        if expense is None:
            expense = {"uuid": expense_uuid, "rows": []}
            seen[key] = expense
            per_reimbursement[reimbursement_uuid].append(expense)
        expense["rows"].append(row)

    for expenses in per_reimbursement.values():
        for expense in expenses:
            expense["rows"].sort(
                key=lambda r: int((r.get("Sub-row Number") or "0").strip() or 0)
            )
            # "Total Amount" is the expense total and equals the sum of its
            # splits; recompute so a divergence surfaces.
            expense["total"] = sum(
                (_decimal_or_none(r.get("Amount")) or _decimal.Decimal(0))
                for r in expense["rows"]
            )
    return dict(per_reimbursement)


def _booking_objects(expense: dict, sign: int) -> list[dict]:
    """Build the additional_info->'moss_bookings' objects for one expense.

    ``sign`` is taken from the balance row (-1 for the usual outgoing money), so
    the stored amounts carry the same sign as the row they replace; the
    reimbursement export lists them unsigned.
    """
    bookings = []
    for row in expense["rows"]:
        amount = _decimal_or_none(row.get("Amount"))
        sphere = (row.get("Cost Carrier - Number") or "").strip()
        bookings.append(
            {
                # <expense uuid>_<Sub-row Number> -- constructed, NOT the CSV
                # "Unique Item Number" (whose suffix is a file-global counter).
                "booking_unique_item_number": (
                    f"{expense['uuid']}_{(row.get('Sub-row Number') or '').strip()}"
                ),
                "signed_base_amount": str(amount * sign)
                if amount is not None
                else None,
                # The Moss export mislabels the cost-center NUMBER as a name.
                "account_number": (row.get("Expense Account") or "").strip() or None,
                "cost_center_number": (row.get("Cost Center - Name") or "").strip()
                or None,
                "sphere_number": None if sphere in _EMPTY_SPHERE else sphere,
                "distribution_combination": (
                    row.get("Distribution combination") or ""
                ).strip()
                or None,
                "booking_posting_text": (row.get("Expense Description") or "").strip(),
                # Kept for traceability -- the raw, position-dependent Moss value.
                "moss_unique_item_number": (row.get("Unique Item Number") or "").strip()
                or None,
            }
        )
    return bookings


def _load_balance_rows(connection) -> dict[str, list[dict]]:
    """Reimbursement rows of moss_balance_movements, grouped by transaction."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, moss_transaction_id, sub_row_number, amount,
                   moss_reimbursement_id, unique_item_number, additional_info
              FROM moss_balance_movements
             WHERE coalesce(moss_reimbursement_id, '') <> ''
             ORDER BY moss_transaction_id, sub_row_number
            """
        )
        columns = [c.name for c in cursor.description]
        rows = [dict(zip(columns, values)) for values in cursor.fetchall()]

    grouped: dict[str, list[dict]] = _collections.defaultdict(list)
    for row in rows:
        grouped[row["moss_transaction_id"]].append(row)
    return dict(grouped)


def _plan_updates(
    balance_by_transaction: dict, expenses_by_reimbursement: dict
) -> tuple[list, list]:
    """Match balance rows to reimbursement expenses and build the UPDATE plan.

    Returns ``(updates, problems)``; ``updates`` are
    ``(balance_row_id, additional_info_dict)`` pairs.
    """
    updates: list[tuple[int, dict]] = []
    problems: list[str] = []

    for transaction_uuid, balance_rows in sorted(balance_by_transaction.items()):
        reimbursement_uuid = (balance_rows[0]["moss_reimbursement_id"] or "").strip()
        expenses = expenses_by_reimbursement.get(reimbursement_uuid)
        if not expenses:
            problems.append(
                f"{transaction_uuid}: reimbursement {reimbursement_uuid} not in the export "
                f"-- {len(balance_rows)} row(s) left unenriched"
            )
            continue
        if len(expenses) != len(balance_rows):
            problems.append(
                f"{transaction_uuid}: {len(balance_rows)} balance row(s) but "
                f"{len(expenses)} expense(s) in reimbursement {reimbursement_uuid} -- skipped"
            )
            continue

        # Verify the ordinal correspondence on the amounts before trusting it.
        mismatch = next(
            (
                (row, expense)
                for row, expense in zip(balance_rows, expenses)
                if abs(row["amount"]) != expense["total"]
            ),
            None,
        )
        if mismatch is not None:
            row, expense = mismatch
            problems.append(
                f"{transaction_uuid}: sub-row {row['sub_row_number']} is {row['amount']} but "
                f"expense {expense['uuid']} totals {expense['total']} -- reimbursement skipped"
            )
            continue

        for row, expense in zip(balance_rows, expenses):
            sign = -1 if row["amount"] < 0 else 1
            additional_info = dict(row["additional_info"] or {})
            additional_info[_KEY_BOOKINGS] = _booking_objects(expense, sign)
            additional_info[_KEY_EXPENSE_UUID] = expense["uuid"]
            updates.append((row["id"], additional_info))

    return updates, problems


def _apply_updates(connection, updates: list[tuple[int, dict]]) -> int:
    """Write the enriched additional_info back, one statement for all rows."""
    if not updates:
        return 0
    with connection.cursor() as cursor:
        cursor.executemany(
            "UPDATE moss_balance_movements SET additional_info = %s::jsonb WHERE id = %s",
            [
                (_json.dumps(info, ensure_ascii=False), row_id)
                for row_id, info in updates
            ],
        )
    return len(updates)


def create_argument_parser():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "files",
        nargs="+",
        help="Moss reimbursements export(s) (CSV-Builder WSJ27 template).",
    )
    # NB: --dry-run comes from the WsjRdpContext base parser (ctx.dry_run).
    parser.add_argument(
        "--rollback-for-testing",
        action="store_true",
        default=False,
        help="Apply the plan, then ROLLBACK instead of committing (testing).",
    )
    return parser


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        __file__=__file__,
    )
    out_base = ctx.make_out_path(_SELF_NAME + "_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    expenses_by_reimbursement: dict[str, list[dict]] = {}
    for raw_path in ctx.parsed_args.files:
        path = _pathlib.Path(raw_path)
        per_file = _read_reimbursement_expenses(path)
        expenses_by_reimbursement.update(per_file)
        _LOGGER.info(
            "%s: %d reimbursements, %d expenses, %d booking rows",
            path.name,
            len(per_file),
            sum(len(e) for e in per_file.values()),
            sum(len(x["rows"]) for e in per_file.values() for x in e),
        )

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        balance_by_transaction = _load_balance_rows(ro_conn)
        _LOGGER.info(
            "Database: %d reimbursement transactions, %d balance rows to enrich.",
            len(balance_by_transaction),
            sum(len(v) for v in balance_by_transaction.values()),
        )

        updates, problems = _plan_updates(
            balance_by_transaction, expenses_by_reimbursement
        )
        split_count = sum(
            1 for _, info in updates if len(info.get(_KEY_BOOKINGS) or []) > 1
        )
        _LOGGER.info(
            "Planned: %d rows enriched (%d of them with more than one booking).",
            len(updates),
            split_count,
        )
        for problem in problems:
            _LOGGER.warning("NOT enriched -- %s", problem)
        if problems:
            _LOGGER.warning(
                "%d reimbursement(s) could not be matched; the migration guard will "
                "refuse to run until they are resolved.",
                len(problems),
            )

        if ctx.dry_run:
            _LOGGER.warning("[--dry-run] Nothing written.")
            return 0
        if not updates:
            _LOGGER.info("Nothing to write.")
            return 0

        ctx.require_approval_to_run_in_prod()
        rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
        written = _apply_updates(rw_conn, updates)
        _LOGGER.info("Enriched %d moss_balance_movements rows.", written)

        if ctx.parsed_args.rollback_for_testing:
            _LOGGER.warning(
                "ROLLBACK (--rollback-for-testing given) - no changes committed"
            )
            rw_conn.rollback()
        # The commit happens implicitly when the `with ctx:` block exits cleanly.
    return 0


if __name__ == "__main__":
    _sys.exit(main())
