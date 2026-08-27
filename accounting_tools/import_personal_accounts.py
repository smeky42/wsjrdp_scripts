#!/usr/bin/env -S uv run
"""Import personal accounts (Debitoren/Kreditoren) into wsjrdp_personal_accounts,
auto-detecting the source of each given file.

Supported sources (detected by header):

  * Moss supplier export (.csv, column "Supplier Number") -- the system of
    record for suppliers. Writes the shared fields (name, bank details,
    address) plus every moss_* column; extra CSV columns go to
    other_moss_columns. In its regular mode it also CLEARS moss_status (to
    NULL = unknown to Moss) for every stored account that is no longer in the
    CSV -- only where moss_status is not NULL already; those rows are merged
    into the same plan as ordinary updates (no separate UPDATE query).
  * DATEV Debitoren/Kreditoren export (DTVF format category 16, CP1252, ";").
    Writes the shared fields plus datev_short_name and
    datev_nummer_fremdsystem; extra populated fields go to
    other_datev_columns. It never touches moss_* columns.

CP1252: the DATEV export cannot represent characters outside CP1252 (Polish
"Gdansk" instead of "Gdańsk"). For the free-text fields the DATEV data
therefore only overwrites a value when it differs BEYOND that transliteration
-- both when merging the files (keep_existing_for_cp1252_equality) and when
diffing against the stored rows (skip_update_for_cp1252_equality; inside
other_datev_columns key by key) -- see doc/fin/datev_cp1252.md in the
hitobito_wsjrdp_2027 wagon. The Moss data stores Unicode and overwrites
whenever the value differs. For the shared columns (name, bank details,
address) an EMPTY DATEV field never clears a stored value (the column is
simply not mentioned; Moss is the system of record there); only the
DATEV-owned columns (datev_short_name, datev_nummer_fremdsystem) are cleared
by an empty DATEV field.

All files are merged (in argument order -- pass the Moss CSV BEFORE the DATEV
export so a mere transliteration never degrades a Unicode value) into ONE
SingleTableUpsertPlanBuilder. The plan then splits the accounts into explicit
INSERTs (unknown number) and column-granular UPDATEs (no upsert); accounts
whose target state equals the stored state are NOT touched, so a re-run of
the same files touches nothing. The other_*_columns JSONB is authoritative
per covered key: keys the export carries no value for are DELETED
(SpecialValue.DELETE), keys from other sources are preserved, and only
genuinely changing keys are written. Inserts stamp created_at only --
updated_at stays NULL until a row is really updated. Neither source touches
the Hitobito-specific short_name, aliases, description, comment, visibility,
represented_person_id and additional_info (display_short_name is
database-generated). The production approval is only requested when there is
actually something to write.
"""

from __future__ import annotations

import csv as _csv
import logging as _logging
import pathlib as _pathlib
import re as _re
import sys as _sys
import typing as _typing

import wsjrdp2027
from wsjrdp2027 import SpecialValue
from wsjrdp2027._internal.single_table_upsert_plan import (
    SingleTableUpsertPlanBuilder,
)
from wsjrdp2027._pg import create_select_query, in_expr


_SELF_NAME = _pathlib.Path(__file__).stem
_LOGGER = _logging.getLogger(__name__)

_TABLE_NAME = "wsjrdp_personal_accounts"

_STATUS_ACTIVE = "active"
_STATUS_DEACTIVATED = "deactivated"

# Free-text columns the DATEV export writes. It may carry only the
# transliterated CP1252 form of their content, so they are compared
# transliteration-aware (see doc/fin/datev_cp1252.md): when MERGING the DATEV
# file over the Moss values and when DIFFING the plan against the stored rows
# (other_datev_columns is covered key by key by the plan's minimal deltas).
# Excluded: codes and technical identifiers (number, iban, bic, post_code,
# country, datev_nummer_fremdsystem) and enums (account_type).
_MERGE_TRANSLIT_COLUMNS = ("name", "street", "address_second_line", "city")
_PLAN_TRANSLIT_COLUMNS = (
    "name",
    "street",
    "address_second_line",
    "city",
    "datev_short_name",
    "other_datev_columns",
)

# Shared columns for which Moss is the system of record. An EMPTY field in the
# DATEV export means "not maintained there" (e.g. the BIC is optional in DATEV
# for SEPA), NOT "delete" -- the DATEV reader simply OMITS the column then (an
# unmentioned column stays untouched). DATEV-owned columns (datev_short_name,
# datev_nummer_fremdsystem) are exempt: there DATEV is the master and an empty
# export field clears the stored value (None).
_DATEV_KEEP_STORED_IF_EMPTY = (
    "name",
    "iban",
    "bic",
    "street",
    "address_second_line",
    "post_code",
    "city",
    "country",
)

# --- Moss supplier export ----------------------------------------------------

_MOSS_KEY_COLUMN = "Supplier Number"

_MOSS_TO_COLUMN = {
    "Supplier Number": "number",
    "Supplier Name": "name",
    "Account Holder Name": "moss_account_holder_name",
    "Type": "moss_type",
    "Status": "moss_status",
    "Vat ID": "moss_vat_id",
    "IBAN": "iban",
    "SWIFT-Code": "bic",
    "Currency": "moss_default_currency",
    "Payment Method": "moss_default_payment_method",
    "Expense account code": "moss_default_ledger_account_number",
    "Cost Center Number": "moss_default_cost_center_number",
    "Cost Carrier Number": "moss_default_sphere_number",
    "Team Name": "moss_default_team_name",
    "Country": "country",
    "Street": "street",
    "Second line": "address_second_line",
    "Post code": "post_code",
    "City": "city",
}

# Moss is the system of record for all of these: every row mentions every
# column, an empty CSV field writes NULL.
_MOSS_SCALAR_COLUMNS = [
    "number",
    "name",
    "account_type",
    "moss_account_holder_name",
    "moss_type",
    "moss_status",
    "moss_vat_id",
    "moss_default_currency",
    "moss_default_payment_method",
    "moss_default_ledger_account_number",
    "moss_default_cost_center_number",
    "moss_default_sphere_number",
    "moss_default_team_name",
    "iban",
    "bic",
    "country",
    "street",
    "address_second_line",
    "post_code",
    "city",
]

# --- DATEV Debitoren/Kreditoren export (DTVF, format category 16) ------------

_DATEV_FORMAT_NAME = "Debitoren/Kreditoren"

_DATEV_TO_COLUMN = {
    "Konto": "number",
    "Name (Adressattyp Unternehmen)": "name",
    "Kurzbezeichnung": "datev_short_name",
    "Nummer Fremdsystem": "datev_nummer_fremdsystem",
    "IBAN-Nr. 1": "iban",
    "SWIFT-Code 1": "bic",
    "Straße (Rechnungsadresse)": "street",
    "Adresszusatz (Rechnungsadresse)": "address_second_line",
    "Postleitzahl (Rechnungsadresse)": "post_code",
    "Ort (Rechnungsadresse)": "city",
    "Land (Rechnungsadresse)": "country",
}

# Values that carry no information in the DATEV export (structurally filled).
_DATEV_EMPTY_VALUES = {"", "0", "0,00"}


def _norm(value: object) -> str:
    return "" if value is None else str(value).strip()


def _detect_source(path: _pathlib.Path) -> str:
    """Return "moss" or "datev" for a personal-accounts file."""
    with path.open(encoding="cp1252", newline="", errors="replace") as f:
        first = f.readline()
        second = f.readline()
    if _MOSS_KEY_COLUMN in first:
        return "moss"
    if _DATEV_FORMAT_NAME in first and "Konto" in second:
        return "datev"
    raise SystemExit(
        f"{path}: unknown personal-accounts format (expected a Moss supplier "
        f"export with a {_MOSS_KEY_COLUMN!r} column or a DATEV "
        f"{_DATEV_FORMAT_NAME!r} DTVF export)."
    )


def _read_moss(path: _pathlib.Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in _csv.DictReader(f):
            number = _norm(row.get(_MOSS_KEY_COLUMN))
            if not number:
                continue
            record: dict[str, object] = dict.fromkeys(_MOSS_SCALAR_COLUMNS)
            for header, column in _MOSS_TO_COLUMN.items():
                record[column] = _norm(row.get(header)) or None
            record["number"] = number
            record["account_type"] = wsjrdp2027.datev.account_type_for_account_number(
                number
            )
            record["moss_status"] = _normalize_status(row.get("Status"))
            # The CSV is authoritative for every header it carries: a value is
            # set, an empty covered field DELETES the key; keys from other
            # sources inside the JSONB stay untouched.
            record["other_moss_columns"] = {
                header: (value or SpecialValue.DELETE)
                for header, raw in row.items()
                if header and header not in _MOSS_TO_COLUMN
                for value in [_norm(raw)]
            }
            rows.append(record)
    return rows


def _read_datev(path: _pathlib.Path) -> list[dict[str, object]]:
    with path.open(encoding="cp1252", newline="") as f:
        raw_rows = list(_csv.reader(f, delimiter=";"))
    if len(raw_rows) < 2:
        raise SystemExit(f"{path}: too few lines for a DTVF export")
    header = [_norm(h) for h in raw_rows[1]]
    index = {h: i for i, h in enumerate(header)}
    missing = [h for h in _DATEV_TO_COLUMN if h not in index]
    if missing:
        raise SystemExit(f"{path}: DTVF header lacks expected field(s): {missing}")

    rows: list[dict[str, object]] = []
    for raw in raw_rows[2:]:
        raw = list(raw) + [""] * (len(header) - len(raw))
        number = _norm(raw[index["Konto"]])
        if not number:
            continue
        record: dict[str, object] = {
            "number": number,
            "account_type": wsjrdp2027.datev.account_type_for_account_number(number),
        }
        for field, column in _DATEV_TO_COLUMN.items():
            value = _norm(raw[index[field]]) or None
            if value is None and column in _DATEV_KEEP_STORED_IF_EMPTY:
                # Empty shared field: not maintained in DATEV -- leave the
                # column unmentioned so the stored (Moss) value survives.
                continue
            record[column] = value
        # The export is authoritative for every field of its header: a
        # meaningful value is set, a structurally empty one DELETES the key;
        # keys from other sources inside the JSONB stay untouched.
        record["other_datev_columns"] = {
            field: (value if value not in _DATEV_EMPTY_VALUES else SpecialValue.DELETE)
            for field, i in index.items()
            if field and field not in _DATEV_TO_COLUMN
            for value in [_norm(raw[i])]
        }
        rows.append(record)
    return rows


def _normalize_status(raw: str | None) -> str:
    value = _norm(raw).lower()
    if value in (_STATUS_ACTIVE, _STATUS_DEACTIVATED):
        return value
    if value == "":
        return _STATUS_ACTIVE
    if value.startswith("deactiv") or value in ("inactive", "inaktiv"):
        return _STATUS_DEACTIVATED
    return value


def _check_numbers(rows: list[dict[str, object]], source: str) -> None:
    """Personal accounts are 6-digit (CHECK constraint); abort on anything else
    rather than failing mid-transaction."""
    bad = sorted(
        {
            str(r["number"])
            for r in rows
            if not _re.fullmatch(r"[1-9]\d{5}", str(r["number"]))
        }
    )
    if bad:
        raise SystemExit(
            f"Import aborted ({source}): {len(bad)} account number(s) are not "
            f"6-digit personal accounts: {', '.join(bad[:10])}"
        )


def _warn_name_drift(updates: list[dict], existing: dict[object, dict]) -> None:
    """A name inside an update delta is a genuine change (transliteration-equal
    values never reach the delta): where a name was stored before, the
    Moss<->DATEV sync (or the master data) drifted."""
    drift = [
        (u["number"], existing.get(u["number"], {}).get("name"), u["name"])
        for u in updates
        if u.get("name") and existing.get(u["number"], {}).get("name") is not None
    ]
    for number, old, new in drift[:20]:
        _LOGGER.warning(
            "Name drift for %s: stored %r != incoming %r (incoming wins)",
            number,
            old,
            new,
        )
    if drift:
        _LOGGER.warning("%d name(s) differ (the incoming value wins).", len(drift))


def _select_moss_removed_numbers(conn, numbers: _typing.Collection[str]) -> list[str]:
    """Numbers of stored accounts that are NO LONGER in the Moss export and
    still carry a moss_status. Their moss_status is cleared to NULL (NULL =
    unknown to Moss); rows already at NULL stay untouched. The query is
    composed via the _pg helpers create_select_query/in_expr -- identifiers
    and literals are quoted there, nothing is written by hand."""
    import psycopg.sql

    where = psycopg.sql.SQL("moss_status IS NOT NULL AND NOT {numbers_in}").format(
        numbers_in=in_expr(psycopg.sql.Identifier("number"), sorted(numbers)),
    )
    query = create_select_query(_TABLE_NAME, ["number"], where=where)
    return [row["number"] for row in wsjrdp2027.pg_select_dict_rows(conn, query)]


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "files",
        nargs="+",
        help="Moss supplier CSV and/or DATEV Debitoren/Kreditoren DTVF export; "
        "the source is detected per file. Pass the Moss CSV first.",
    )
    p.add_argument(
        "--no-deactivate",
        action="store_true",
        help="Moss branch only: do not clear moss_status (to NULL) for "
        "accounts missing from the CSV.",
    )
    p.add_argument(
        "--rollback-for-testing",
        action="store_true",
        default=False,
        help="Apply the plan, then ROLLBACK instead of committing (testing).",
    )
    return p


def _log_plan_summary(planned, moss_removed: list[str]) -> None:
    """Show what an approval would apply -- logged BEFORE the production
    approval is requested."""
    for table, counts in planned.operation_counts().items():
        _LOGGER.info(
            "Planned for %s: %d INSERTs, %d UPDATEs (%d of them clearing "
            "moss_status), %d DELETEs (%d untouched).",
            table,
            counts.inserts,
            counts.updates,
            len(moss_removed),
            counts.deletes,
            len(planned.untouched_keys),
        )
    for label, rows in (("INSERT", planned.inserts), ("UPDATE", planned.updates)):
        if not rows:
            continue
        keys = [str(row["number"]) for row in rows]
        shown = ", ".join(keys[:10])
        if len(keys) > 10:
            shown += f", ... (+{len(keys) - 10} more)"
        _LOGGER.info("  %s: %s", label, shown)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    )
    out_base = ctx.make_out_path(_SELF_NAME + "_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    builder = SingleTableUpsertPlanBuilder(
        _TABLE_NAME, "number", [], time_zone=ctx.hitobito_time_zone
    )
    moss_numbers: set[str] | None = None
    datev_present = False
    all_columns: set[str] = set()
    for raw_path in ctx.parsed_args.files:
        path = _pathlib.Path(raw_path)
        source = _detect_source(path)
        rows = _read_moss(path) if source == "moss" else _read_datev(path)
        _check_numbers(rows, f"{source}:{path.name}")
        _LOGGER.info(
            "%s: %d personal accounts (source: %s)", path.name, len(rows), source
        )
        columns = {column for row in rows for column in row}
        all_columns |= columns
        if source == "moss":
            builder.merge_values(rows)
            moss_numbers = (moss_numbers or set()) | {str(r["number"]) for r in rows}
        else:
            datev_present = True
            # A mere CP1252 transliteration in the DATEV file never overwrites
            # an earlier (Moss Unicode) value.
            builder.merge_values(
                rows,
                keep_existing_for_cp1252_equality=[
                    c for c in _MERGE_TRANSLIT_COLUMNS if c in columns
                ],
            )

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        clear_moss = moss_numbers is not None and not ctx.parsed_args.no_deactivate
        moss_removed: list[str] = []
        if clear_moss:
            moss_removed = _select_moss_removed_numbers(ro_conn, moss_numbers)
        if moss_removed:
            _LOGGER.info(
                "%d account(s) no longer in the Moss export: clearing "
                "moss_status to NULL.",
                len(moss_removed),
            )
            # Merged BEFORE load_existing, so these rows travel through the
            # same plan/apply as every other update.
            builder.merge_values(
                {"number": number, "moss_status": None} for number in moss_removed
            )
        builder.load_existing(ro_conn)
        cp1252: list[str] | bool = (
            sorted(c for c in _PLAN_TRANSLIT_COLUMNS if c in all_columns)
            if datev_present
            else False
        )
        planned = builder.plan(skip_update_for_cp1252_equality=cp1252 or False)
        _warn_name_drift(planned.updates, builder.existing)

        _log_plan_summary(planned, moss_removed)

        if not planned.inserts and not planned.updates:
            _LOGGER.info(
                "Personal accounts: nothing to write (%d untouched); skipping write.",
                len(planned.untouched_keys),
            )
            return

        if ctx.dry_run:
            _LOGGER.info("[dry-run] Not applying the plan.")
            return

        # The plan summary above shows exactly what this approval applies.
        ctx.require_approval_to_run_in_prod()
        rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
        inserted, updated = planned.apply(rw_conn, now=ctx.start_time)
        _LOGGER.info(
            "Personal accounts: %d inserted, %d updated (%d moss_status "
            "cleared), %d untouched (identical).",
            len(inserted),
            len(updated),
            len(moss_removed),
            len(planned.untouched_keys),
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
