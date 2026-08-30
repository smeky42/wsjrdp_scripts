#!/usr/bin/env -S uv run
"""Import CAMT bank statements (camt.052/.053/.054) into wsjrdp_camt_transactions.

Source: ISO 20022 CAMT XML files (one or more, any of the report/statement/
notification flavours the loader ``wsjrdp2027.CamtMessage`` understands). Only
BOOKED entries are imported; the same booked transaction may appear in several
overlapping exports -- it is deduplicated on its key, and re-importing the same
files changes nothing.

Key (composite): ``camt_type``, ``account_identification``,
``account_servicer_reference``, ``transaction_details_index`` -- the bank's
account-servicer reference makes a booked entry unique per account and report
type; the details index separates the splits of a batch entry.

Multiple files are processed OLDEST FIRST (by ``GrpHdr/CreDtTm``,
``CamtMessage.creation_date_time``) and deduplicated on the composite key, so
the NEWEST export wins per key.

Columns written from the CAMT file: the amount and value/booking dates,
description, status, the counterparty fields (cdtr_*/dbtr_*), the SEPA
references (endtoend_id, mandate_id, ``references`` JSONB), the bank transaction
codes, return_reason, and the raw ``ntry`` / ``tx_dtls`` JSONB snapshots.
``references``/``ntry``/``tx_dtls`` are snapshot columns (the file is the full
truth; stale keys are dropped).

Recency guard: an existing row is UPDATEd only from an export NEWER than the one
that produced its current values (``message_creation_date_time``). An
older-or-equal export may only INSERT a new key, never overwrite -- regardless
of the values. A key not in the database yet is always INSERTed.

AcctSvcrRef: this equals the ``account_servicer_reference`` key column, so a
snapshot merely GAINING it is identity backfill, not a content change. Such a
row is written regardless of recency and does NOT bump ``updated_at`` / the
change stamps (when it is the only change on the row). A change from one
non-null AcctSvcrRef to a DIFFERENT non-null value can never legitimately
happen and is a HARD ERROR -- the import aborts before writing anything.

The derived person: ``imported_subject_id`` / ``imported_subject_type`` (the
person the SEPA mandate id points at, matched case-insensitively) is the
importer's own record, refreshed on every import, with base provenance in
``imported_subject_link_meta`` (a ``*_link_meta`` jsonb: ``{}`` when unlinked,
else automatic / score 1.0 / classification "sepa_mandate"; no ``created_at`` --
it is re-derived every import). The effective ``subject_id`` / ``subject_type``
(+ ``subject_link_meta``) is SEEDED from the same derived person on INSERT only,
and NEVER touched on UPDATE -- it is editable in the app, so a manual correction
survives a re-import, while imported_subject_* keeps showing what the file
derives (a divergence is thus visible).

Stamped on INSERT and on a genuine (substantive) change only -- never on a
no-op re-import: ``source_file`` (the winning export's file name),
``message_identification`` and ``message_creation_date_time`` (its GrpHdr
MsgId / CreDtTm, so message_creation_date_time always describes the export that
produced the row's current values -- the timestamp the recency guard reads).

Written ONLY on INSERT, never overwritten on re-import: ``fin_account_id``
(resolved from ``account_identification`` via wsjrdp_fin_accounts) and the
report ENVELOPE columns (report_identification, report_*_sequence_number,
report_page_number, report_creation_date_time), captured once as provenance.

Never touched by this importer (manual / downstream): ``comment``,
``additional_info``, ``category`` / ``sub_category``, the accounting links
(``payment_initiation_id``, ``direct_debit_payment_info_id``, the reversal
``*_id`` columns) and everything DATEV/booking-side (``datev_*``,
``account_id``/``account_type``, ``offsetting_*``, ``cost_center_number``,
``sphere_number``). New rows get their database defaults there.

Safety: the money/identity columns amount_cents, amount_currency, value_date
are immutable for a given key -- an incoming file that would change them on an
existing row aborts the import before writing (a booked transaction's amount
must not silently change under the same bank reference). Likewise
``fin_account_id``: since ``account_identification`` is part of the key it can
never change for a key, and a mismatch aborts.

An ``account_identification`` unknown to wsjrdp_fin_accounts is created from
``wsjrdp2027.bank_accounts`` when known there (needs the production approval),
otherwise the import aborts before any write.

--dry-run shows the plan without writing; --rollback-for-testing applies and
rolls back; --no-updated-at applies every change but never bumps ``updated_at``
(created_at on INSERT is still set) -- for cleaning up existing data without
making the rows look freshly changed.
"""

from __future__ import annotations

import datetime as _datetime
import logging as _logging
import pathlib as _pathlib
import sys as _sys
import typing as _typing

import wsjrdp2027
from wsjrdp2027._internal.single_table_upsert_plan import (
    SingleTableUpsertPlanBuilder,
)


if _typing.TYPE_CHECKING:
    from wsjrdp2027 import CamtTransactionDetails

_SELF_NAME = _pathlib.Path(__file__).stem
_LOGGER = _logging.getLogger(__name__)

_TABLE_NAME = "wsjrdp_camt_transactions"

# Composite plan key (the CamtTxUniqueDbKey columns).
_KEY = (
    "camt_type",
    "account_identification",
    "account_servicer_reference",
    "transaction_details_index",
)
# Snapshot JSONB columns: the CAMT file is the full truth, stale keys are dropped.
_JSONB_SNAPSHOT = ["references", "ntry", "tx_dtls"]
# Immutable money/identity columns: a change on an existing key aborts the run.
_IMMUTABLE = ("amount_cents", "amount_currency", "value_date")
# Report ENVELOPE columns: they describe the export a transaction was first
# captured from, NOT the transaction itself, so the SAME booked entry carries
# different values across overlapping exports. Written on INSERT only
# (provenance, set once) -- never refreshed.
#
# message_identification / message_creation_date_time are NOT here: they are
# stamped on INSERT *and* whenever the export genuinely changes the row (see
# _STAMP_ON_CHANGE), so message_creation_date_time always tracks the CreDtTm of
# the export that produced the row's current values -- that is the timestamp the
# recency guard compares against.
_ENVELOPE = (
    "report_identification",
    "report_electronic_sequence_number",
    "report_legal_sequence_number",
    "report_page_number",
    "report_creation_date_time",
)
# Provenance stamped on INSERT and on a genuine (substantive) change only, never
# on a no-op re-import -- same convention as datev_bookings.source_file. The
# values come from the winning export's GrpHdr (source_file = its file name).
_STAMP_ON_CHANGE = (
    "source_file",
    "message_identification",
    "message_creation_date_time",
)


def _to_date_or_none(value):
    return wsjrdp2027.to_date(value) if value is not None else None


def _derived_person(tx: CamtTransactionDetails) -> tuple[int | None, str | None]:
    """The person the CAMT import derives from the SEPA mandate id (as
    pg_insert_camt_transaction_from_tx does), or (None, None).

    Case-insensitive: a bank echoes the mandate id back in whatever case it
    likes (e.g. a SEPA Retoure carrying ``WSJRDP2027189``), so match it against
    the lower-cased ``wsjrdp2027<id>`` scheme rather than dropping it."""
    mandate_id = tx.mandate_id
    hitobito_id = wsjrdp2027.hitobito_id_from_sepa_mandate_id(
        mandate_id.lower() if isinstance(mandate_id, str) else mandate_id
    )
    return (hitobito_id, "Person") if hitobito_id is not None else (None, None)


def _derived_link_meta(hitobito_id: int | None) -> dict:
    """Base provenance for a subject link derived from the SEPA mandate,
    following the ``*_link_meta`` convention (doc/fin/recon_linking.md in the
    wagon): one jsonb object, ``{}`` when unlinked. The derivation is a
    deterministic automatic match.

    ``created_at`` is deliberately omitted: ``imported_subject_link_meta`` is
    refreshed on EVERY import, so a per-run timestamp would make it churn (and
    break the importer's idempotency); ``subject_link_meta`` mirrors it on
    INSERT. (Manual links set author_id / automatic_manual='manual' /
    classification_string=null in the app -- untouched here.)"""
    if hitobito_id is None:
        return {}
    return {
        "author_id": None,
        "score": 1.0,
        "automatic_manual": "automatic",
        "classification_string": "sepa_mandate",
    }


def _tx_value_row(tx: CamtTransactionDetails) -> dict:
    """The value set written from a CAMT transaction: the composite key plus the
    immutable CAMT-derived columns. Transforms mirror
    ``wsjrdp2027.pg_insert_camt_transaction`` exactly, so a re-import of data an
    earlier run wrote diffs to nothing. Manual/downstream columns are left out
    on purpose (see the module docstring); ``fin_account_id`` and the effective
    ``subject_*`` are added on INSERT only, elsewhere."""
    imported_subject_id, imported_subject_type = _derived_person(tx)
    return {
        # key
        "camt_type": tx.camt_type,
        "account_identification": tx.account_identification,
        "account_servicer_reference": tx.account_servicer_reference,
        "transaction_details_index": tx.transaction_details_index or 0,
        # amounts / dates (immutable for a key)
        "credit_debit_indication": tx.credit_debit_indication,
        "amount_cents": tx.amount_cents,
        "amount_currency": tx.amount_currency or "EUR",
        "value_date": wsjrdp2027.to_date(tx.value_date),
        "booking_date": _to_date_or_none(tx.booking_date),
        # description / metadata
        "description": tx.description,
        "status": tx.status or "NULL",
        "additional_entry_info": tx.additional_entry_info,
        "number_of_transactions": 1,
        "entry_or_details": "entry",
        # references / counterparties
        "references": tx.references or {},
        "endtoend_id": tx.endtoend_id,
        "mandate_id": tx.mandate_id,
        "bank_transaction_code": tx.bank_transaction_code,
        "bank_transaction_code_dk": tx.bank_transaction_code_dk,
        "return_reason": tx.return_reason,
        "cdtr_name": tx.cdtr_name,
        "cdtr_iban": tx.cdtr_iban,
        "cdtr_bic": tx.cdtr_bic,
        "cdtr_address": tx.cdtr_address,
        "dbtr_name": tx.dbtr_name,
        "dbtr_iban": tx.dbtr_iban,
        "dbtr_bic": tx.dbtr_bic,
        "dbtr_address": tx.dbtr_address,
        # raw JSONB snapshots
        "ntry": tx.Ntry or None,
        "tx_dtls": tx.TxDtls or None,
        # the derived person, refreshed on every import (the effective
        # subject_* is set from the same values on INSERT only, see below)
        "imported_subject_id": imported_subject_id,
        "imported_subject_type": imported_subject_type,
        "imported_subject_link_meta": _derived_link_meta(imported_subject_id),
    }


def _tx_insert_only(tx: CamtTransactionDetails) -> dict:
    """Values written on INSERT only, never refreshed on re-import: the effective
    ``subject_*`` (+ ``subject_link_meta``), the report envelope (_ENVELOPE,
    export provenance) and the account_identification used to resolve
    fin_account_id later.

    The effective ``subject_id`` / ``subject_type`` (+ ``subject_link_meta``,
    mirroring ``imported_subject_link_meta``) is SEEDED from the derived person
    on INSERT, but NEVER touched on UPDATE, because it is editable in the app: a
    manual correction survives a re-import, while ``imported_subject_*`` (in the
    value row) keeps tracking what the file derives, so a divergence stays
    visible.

    message_identification / message_creation_date_time are NOT here -- they are
    stamped on INSERT and on a genuine change (see _STAMP_ON_CHANGE)."""
    subject_id, subject_type = _derived_person(tx)
    return {
        "account_identification": tx.account_identification,
        "subject_id": subject_id,
        "subject_type": subject_type,
        "subject_link_meta": _derived_link_meta(subject_id),
        "report_identification": tx.report_identification,
        "report_electronic_sequence_number": tx.report_electronic_sequence_number,
        "report_legal_sequence_number": tx.report_legal_sequence_number,
        "report_page_number": tx.report_page_number,
        "report_creation_date_time": wsjrdp2027.to_datetime_or_none(
            tx.report_creation_date_time
        ),
    }


def _strip_acct_svcr_ref(value: object) -> object:
    """A deep copy of a JSONB snapshot with every ``AcctSvcrRef`` key removed
    (at any nesting depth). AcctSvcrRef equals the ``account_servicer_reference``
    key column, so a snapshot merely GAINING it is identity backfill, not a
    content change -- stripping it lets :func:`_is_acct_svcr_ref_only` tell such
    a row apart from a real edit."""
    if isinstance(value, dict):
        return {
            k: _strip_acct_svcr_ref(v) for k, v in value.items() if k != "AcctSvcrRef"
        }
    if isinstance(value, list):
        return [_strip_acct_svcr_ref(v) for v in value]
    return value


def _is_acct_svcr_ref_only(update_row: dict, incoming: dict, stored: dict) -> bool:
    """True when the only thing this UPDATE changes is snapshot columns GAINING
    ``AcctSvcrRef`` (no other column, no other JSONB key differs). Such a row is
    pure identity backfill: it is written regardless of export recency and never
    bumps ``updated_at`` / the change stamps (per the AcctSvcrRef rule)."""
    changed_columns = set(update_row) - set(_KEY)
    if not changed_columns:
        return False
    if not changed_columns <= set(_JSONB_SNAPSHOT):
        return False  # a scalar / non-snapshot column changed -> substantive
    for column in changed_columns:
        if _strip_acct_svcr_ref(incoming.get(column)) != _strip_acct_svcr_ref(
            stored.get(column)
        ):
            return False  # a snapshot changed beyond AcctSvcrRef -> substantive
    return True


def _acct_svcr_ref_values(row: dict) -> set:
    """Every non-null ``AcctSvcrRef`` value found (at any depth) across the
    snapshot columns of a row. AcctSvcrRef equals the account_servicer_reference
    key column, so per row this is normally the empty set or a single value."""
    found: set = set()

    def walk(value: object) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                if k == "AcctSvcrRef":
                    if v is not None:
                        found.add(v)
                else:
                    walk(v)
        elif isinstance(value, list):
            for v in value:
                walk(v)

    for column in _JSONB_SNAPSHOT:
        walk(row.get(column))
    return found


def _assert_acct_svcr_ref_unchanged(value_rows: dict, existing: dict) -> None:
    """A booked entry's AcctSvcrRef is its account_servicer_reference (the key),
    so it can never legitimately change. Adding it (stored has none) is fine, but
    a change from one non-null value to a DIFFERENT non-null value signals a
    corrupted / mismatched file -- abort before writing anything."""
    problems = []
    for key, incoming in value_rows.items():
        stored = existing.get(key)
        if stored is None:
            continue
        stored_refs = _acct_svcr_ref_values(stored)
        incoming_refs = _acct_svcr_ref_values(incoming)
        if stored_refs and incoming_refs and stored_refs != incoming_refs:
            problems.append(
                f"{key}: AcctSvcrRef {sorted(stored_refs)} -> {sorted(incoming_refs)}"
            )
    if problems:
        shown = "\n  ".join(problems[:10])
        raise SystemExit(
            f"Refusing to change AcctSvcrRef (== account_servicer_reference) on "
            f"{len(problems)} existing transaction(s):\n  {shown}"
        )


def _to_utc(dt: _datetime.datetime | None) -> _datetime.datetime | None:
    """Normalize to a tz-aware UTC datetime for the recency comparison. A naive
    value is read as UTC -- that is how Rails stores the ``timestamp without time
    zone`` column ``message_creation_date_time`` (both prod and this importer
    write it via ``to_datetime_or_none`` through the same UTC session)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=_datetime.UTC)
    return dt.astimezone(_datetime.UTC)


def _key_of(row: dict) -> tuple:
    return tuple(row[c] for c in _KEY)


def _log_plan_summary(planned) -> None:
    """Show what an approval would apply -- logged BEFORE the approval."""
    for table, counts in planned.operation_counts().items():
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
        shown = [
            f"{r['account_identification']}/{r['account_servicer_reference']}"
            for r in rows[:10]
        ]
        more = f", ... (+{len(rows) - 10} more)" if len(rows) > 10 else ""
        _LOGGER.info("  %s: %s%s", label, ", ".join(shown), more)


def _load_fin_accounts(conn) -> dict[str, int]:
    rows = wsjrdp2027.pg_select_dict_rows(
        conn,
        t"""SELECT "id", "account_identification" FROM wsjrdp_fin_accounts""",
        show_result=False,
    )
    return {
        r["account_identification"]: r["id"]
        for r in rows
        if r["account_identification"]
    }


def _assert_fin_account_stable(
    existing: dict, account_map: dict[str, int], insert_only: dict[tuple, dict]
) -> None:
    """A booked transaction cannot move to a different bank account:
    ``account_identification`` is part of the key, so ``fin_account_id`` (a
    function of it) is structurally fixed for a key. Assert it anyway -- a
    mismatch would mean the fin_account mapping changed under us -> abort.

    ``fin_account_id`` is written on INSERT only, so it is loaded via
    ``load_existing(read_only_columns=["fin_account_id"])`` rather than being a
    value-set column."""
    problems = []
    for key, stored in existing.items():
        current = stored.get("fin_account_id")
        if current is None:
            continue
        derived = account_map.get(insert_only[key]["account_identification"])
        if current != derived:
            problems.append(f"{key}: fin_account_id {current!r} -> {derived!r}")
    if problems:
        shown = "\n  ".join(problems[:10])
        raise SystemExit(
            f"fin_account_id would change on {len(problems)} existing "
            f"transaction(s) -- refusing to import:\n  {shown}"
        )


def _assert_immutable_unchanged(planned, existing: dict) -> None:
    """A booked transaction's amount/currency/value_date must not change under
    the same key -- abort (before writing) if an UPDATE would touch them."""
    problems = []
    for update in planned.updates:
        touched = [c for c in _IMMUTABLE if c in update]
        if touched:
            key = _key_of(update)
            stored = existing.get(key, {})
            for c in touched:
                problems.append(f"{key}: {c} {stored.get(c)!r} -> {update[c]!r}")
    if problems:
        shown = "\n  ".join(problems[:10])
        raise SystemExit(
            f"Refusing to change immutable columns on {len(problems)} existing "
            f"transaction(s):\n  {shown}"
        )


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "files",
        nargs="+",
        help="CAMT XML files (camt.052/.053/.054); booked entries are imported.",
    )
    p.add_argument(
        "--rollback-for-testing",
        action="store_true",
        default=False,
        help="Apply the plan, then ROLLBACK instead of committing (testing).",
    )
    p.add_argument(
        "--no-updated-at",
        action="store_true",
        default=False,
        help=(
            "Do NOT bump updated_at on any UPDATE (created_at on INSERT is still "
            "set). Use it to back-fill / clean up the existing (prod) data "
            "without making the rows look freshly changed."
        ),
    )
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    )
    out_base = ctx.make_out_path(_SELF_NAME + "_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    no_updated_at = ctx.parsed_args.no_updated_at

    # 1. Read + parse every file first (no database yet). Process the exports
    #    OLDEST FIRST (by GrpHdr/CreDtTm) so that when we dedup the overlapping
    #    exports on the composite key, the NEWEST export wins per key. Two
    #    occurrences of one key must agree on the immutable columns.
    messages = []  # (creation_date_time, path, camt), sorted oldest -> newest
    for raw_path in ctx.parsed_args.files:
        path = _pathlib.Path(raw_path)
        camt = wsjrdp2027.CamtMessage.load(path)
        messages.append((camt.creation_date_time, path, camt))
    messages.sort(key=lambda m: m[0])

    value_rows: dict[tuple, dict] = {}
    insert_only: dict[tuple, dict] = {}
    # Provenance of the winning (newest) export per key: the source file name,
    # its GrpHdr MsgId, and its CreDtTm. creation_dt drives the recency guard.
    stamp: dict[tuple, dict] = {}
    creation_dt: dict[tuple, _datetime.datetime | None] = {}
    account_idents: set[str] = set()
    for _creation_date_time, path, camt in messages:
        norm_credttm = wsjrdp2027.to_datetime_or_none(camt.creation_date_time)
        n = 0
        for tx in camt.booked_transaction_details:
            row = _tx_value_row(tx)
            key = _key_of(row)
            prev = value_rows.get(key)
            if prev is not None:
                clash = [c for c in _IMMUTABLE if prev[c] != row[c]]
                if clash:
                    raise SystemExit(
                        f"{path.name}: transaction {key} disagrees with an "
                        f"earlier file on {clash} -- refusing to import."
                    )
            value_rows[key] = row
            insert_only[key] = _tx_insert_only(tx)
            stamp[key] = {
                "source_file": path.name,
                "message_identification": camt.message_identification,
                "message_creation_date_time": norm_credttm,
            }
            creation_dt[key] = norm_credttm
            account_idents.add(tx.account_identification)
            n += 1
        _LOGGER.info(
            "%s (CreDtTm %s): %d booked transactions",
            path.name,
            camt.creation_date_time.isoformat(),
            n,
        )
    _LOGGER.info(
        "%d file(s), %d distinct transactions across %d account(s).",
        len(messages),
        len(value_rows),
        len(account_idents),
    )

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)

        # Resolve the fin accounts; partition any missing ones into
        # known-in-bank_accounts (creatable) vs unknown (fatal, before writing).
        account_map = _load_fin_accounts(ro_conn)
        known_bank = (
            wsjrdp2027.bank_accounts.WSJ27_ACCOUNT_IDENTIFICATION_TO_BANK_ACCOUNT_DICT
        )
        missing = sorted(account_idents - account_map.keys())
        unknown = [a for a in missing if a not in known_bank]
        if unknown:
            raise SystemExit(
                f"Unknown bank account(s), not in wsjrdp_fin_accounts and not in "
                f"wsjrdp2027.bank_accounts: {unknown}. Create them first."
            )
        creatable = [a for a in missing if a in known_bank]

        builder = SingleTableUpsertPlanBuilder(
            _TABLE_NAME,
            list(_KEY),
            list(value_rows.values()),
            time_zone=ctx.hitobito_time_zone,
        )
        # fin_account_id and message_creation_date_time are not value columns:
        # load them read-only for the fin-account stability guard resp. the
        # recency guard (the export that produced a row's current values).
        builder.load_existing(
            ro_conn,
            read_only_columns=["fin_account_id", "message_creation_date_time"],
        )
        # imported_subject_link_meta is a full-state jsonb (like the snapshots):
        # the incoming dict IS the target, so an unlinked row's {} clears a
        # stale meta rather than merging into it.
        planned = builder.plan(
            replace_dict_columns=[*_JSONB_SNAPSHOT, "imported_subject_link_meta"]
        )
        _assert_immutable_unchanged(planned, builder.existing)
        _assert_fin_account_stable(builder.existing, account_map, insert_only)
        _assert_acct_svcr_ref_unchanged(value_rows, builder.existing)

        # 2. Classify the planned UPDATEs and enforce the recency guard:
        #      * AcctSvcrRef-only  -> pure identity backfill (the AcctSvcrRef ==
        #        the account_servicer_reference key column): ALWAYS applied,
        #        never stamped, never bumps updated_at, ignores recency;
        #      * substantive + a NEWER export than the stored row -> applied,
        #        stamped (source_file / message_*), updated_at bumped unless
        #        --no-updated-at;
        #      * substantive + an OLDER-or-equal export -> DROPPED (an older
        #        export may only INSERT, never overwrite -- regardless of value).
        substantive_keys: set = set()
        acct_only_keys: set = set()
        kept_updates: list[dict] = []
        recency_blocked = 0
        for update_row in planned.updates:
            key = _key_of(update_row)
            if _is_acct_svcr_ref_only(
                update_row, value_rows[key], builder.existing[key]
            ):
                acct_only_keys.add(key)
                kept_updates.append(update_row)
                continue
            stored_credttm = _to_utc(
                builder.existing[key].get("message_creation_date_time")
            )
            incoming_credttm = _to_utc(creation_dt[key])
            if stored_credttm is not None and (
                incoming_credttm is None or incoming_credttm <= stored_credttm
            ):
                recency_blocked += 1
                continue
            substantive_keys.add(key)
            kept_updates.append(update_row)
        planned.updates = kept_updates

        _log_plan_summary(planned)
        _LOGGER.info(
            "  UPDATE breakdown: %d substantive (newer export), %d AcctSvcrRef-only "
            "backfill, %d skipped (older/equal export).",
            len(substantive_keys),
            len(acct_only_keys),
            recency_blocked,
        )
        if no_updated_at:
            _LOGGER.info("  --no-updated-at: UPDATEs do NOT bump updated_at.")
        if creatable:
            _LOGGER.info(
                "Will create %d new fin_account(s): %s", len(creatable), creatable
            )

        if not planned.inserts and not planned.updates and not creatable:
            _LOGGER.info(
                "nothing to write (%d untouched).", len(planned.untouched_keys)
            )
            return
        if ctx.dry_run:
            _LOGGER.info("[dry-run] Not applying the plan.")
            return

        # The plan summary above shows exactly what this approval applies.
        ctx.require_approval_to_run_in_prod()
        rw_conn = ctx.hitobito_psycopg_connection(read_only=False)

        for account_identification in creatable:
            acc = known_bank[account_identification]
            account_map[account_identification] = wsjrdp2027.pg_insert_fin_account(
                rw_conn, **acc.asdict()
            )
            _LOGGER.info(
                "Created fin_account %s -> id %s",
                account_identification,
                account_map[account_identification],
            )

        # INSERT rows: attach the INSERT-only columns (fin_account_id, the
        # effective subject_*, the report envelope), created_at, and the change
        # stamp. subject_id/subject_type are seeded here on INSERT only -- an
        # UPDATE never touches them, so a manual correction survives a re-import.
        now = ctx.start_time
        for insert_row in planned.inserts:
            key = _key_of(insert_row)
            extra = insert_only[key]
            insert_row["fin_account_id"] = account_map[extra["account_identification"]]
            if extra["subject_id"] is not None:
                insert_row["subject_id"] = extra["subject_id"]
                insert_row["subject_type"] = extra["subject_type"]
                insert_row["subject_link_meta"] = extra["subject_link_meta"]
            for column in _ENVELOPE:
                insert_row[column] = extra[column]
            for column in _STAMP_ON_CHANGE:
                insert_row[column] = stamp[key][column]
            insert_row["created_at"] = now

        # UPDATE rows: stamp the substantive ones (source_file / message_*, and
        # updated_at unless --no-updated-at). AcctSvcrRef-only rows get no stamp
        # and no updated_at -- invisible identity backfill.
        for update_row in planned.updates:
            key = _key_of(update_row)
            if key in substantive_keys:
                for column in _STAMP_ON_CHANGE:
                    update_row[column] = stamp[key][column]
                if not no_updated_at:
                    update_row["updated_at"] = now

        # touch=False: created_at / updated_at are set explicitly per row above,
        # so there is no blanket stamp -- that is what lets AcctSvcrRef-only rows
        # (and, with --no-updated-at, every UPDATE) keep their stored updated_at.
        inserted, updated = planned.apply(rw_conn, now=now, touch=False)
        _LOGGER.info(
            "CAMT transactions: %d inserted, %d updated (%d substantive + %d "
            "AcctSvcrRef-only), %d untouched, %d skipped (older export).",
            len(inserted),
            len(updated),
            len(substantive_keys),
            len(acct_only_keys),
            len(planned.untouched_keys),
            recency_blocked,
        )
        if ctx.parsed_args.rollback_for_testing:
            _LOGGER.warning(
                "ROLLBACK (--rollback-for-testing given) - no changes committed"
            )
            rw_conn.rollback()
        # The commit happens implicitly when the `with ctx:` block exits
        # cleanly; an exception before that leaves the database untouched.


if __name__ == "__main__":
    _sys.exit(main())
