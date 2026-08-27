#!/usr/bin/env -S uv run
"""Import cost-center master data into wsjrdp_cost_centers, auto-detecting the
source of each given file.

Supported sources (detected per file):

  * Moss cost-center export (.csv, column "Cost Center Number") -- writes name,
    moss_status and manager_name (with the manager-person linking below).
  * DATEV KOST-Stammdaten report (.xlsx, header row containing
    "Kostenstelle/-traeger") -- writes ONLY number, name (Langbezeichnung) and
    short_name (Kurzbezeichnung). The DATEV report is CP1252-limited: name and
    short_name are compared transliteration-aware, so a stored Unicode value is
    never degraded by its DATEV transliteration (see doc/fin/datev_cp1252.md in
    the wagon); an empty DATEV field never clears a stored value.

Before writing, the stored rows are loaded (read-only) and diffed against the
CSV. Cost centers whose target state (name, moss_status, manager_name) equals
the stored state are NOT touched; the remainder is split into explicit INSERTs
(unknown number) and UPDATEs (no upsert), so a re-run of the same CSV touches
nothing. The ``status`` column is taken from the CSV "Status" column,
normalised to active/deactivated (the Moss vocabulary, shared with suppliers). created_at / updated_at are set here (created_at
only on insert). additional_info is left at its default (empty) and reserved for
future data.

The CSV "Manager" column is stored verbatim in manager_name. Whenever that name
CHANGES, the person link (manager_person_id) is cleared, because it may still
point at the previous manager. Afterwards the still-unlinked names are resolved
to a Person when they match exactly one -- on the full first name, else on its
first token (Moss uses the everyday first name, hitobito the full official one);
see _link_manager_persons; use --no-link-managers Only rows whose manager_person_id is still NULL are resolved, so a manual
correction in the app survives a re-import as long as the name stays the same.

Expected Moss CSV columns: "Cost Center Number", "Cost Center Name",
"Status", "Manager". Both sources can be passed in one call: all files are
merged (in argument order; for the DATEV file a mere CP1252 transliteration
never overwrites an earlier Unicode value) into ONE plan with a single
apply() at the end. The production approval is only requested when there is
actually something to insert or update.
"""

from __future__ import annotations

import csv as _csv
import logging as _logging
import pathlib as _pathlib
import sys as _sys
import typing as _typing

import wsjrdp2027
from wsjrdp2027._internal.single_table_upsert_plan import (
    SingleTableUpsertPlanBuilder,
)


if _typing.TYPE_CHECKING:
    import psycopg as _psycopg


_SELF_NAME = _pathlib.Path(__file__).stem
_LOGGER = _logging.getLogger(__name__)

_TABLE_NAME = "wsjrdp_cost_centers"

_STATUS_ACTIVE = "active"
_STATUS_DEACTIVATED = "deactivated"


def _normalize_status(raw: str | None) -> str:
    value = (raw or "").strip().lower()
    if value in ("", "active", "aktiv"):
        return _STATUS_ACTIVE
    if value.startswith("inactiv") or value in ("inaktiv", "deactivated"):
        return _STATUS_DEACTIVATED
    return value


def _read_cost_centers(csv_path: str | _pathlib.Path) -> list[dict]:
    """Read the cost centers from the Moss CSV; empty text fields become None."""
    cost_centers: list[dict] = []
    with _pathlib.Path(csv_path).open(encoding="utf-8-sig", newline="") as f:
        for row in _csv.DictReader(f):
            number = (row.get("Cost Center Number") or "").strip()
            if not number:
                continue
            cost_centers.append(
                {
                    "number": number,
                    "name": (row.get("Cost Center Name") or "").strip() or None,
                    "moss_status": _normalize_status(row.get("Status")),
                    "manager_name": (row.get("Manager") or "").strip() or None,
                }
            )
    return cost_centers


def _detect_source(path: _pathlib.Path) -> str:
    """Return "moss" (CSV) or "datev" (KOST-Stammdaten xlsx) for a file."""
    if path.suffix.lower() in (".xlsx", ".xlsm", ".xls"):
        return "datev"
    with path.open(encoding="utf-8-sig", newline="") as f:
        header = f.readline()
    if "Cost Center Number" in header:
        return "moss"
    raise SystemExit(
        f"{path}: unbekanntes Kostenstellen-Format (erwartet Moss-CSV mit "
        f"'Cost Center Number' oder DATEV KOST-Stammdaten .xlsx)."
    )


_DATEV_KOST_HEADER = ("Kostenstelle/-träger", "Langbezeichnung", "Kurzbezeichnung")


def _read_datev_kost(path: _pathlib.Path) -> list[dict]:
    """Read the DATEV KOST-Stammdaten report (.xlsx). The report carries a
    few metadata lines before the actual header row; locate the header by its
    "Kostenstelle/-träger" cell and take only number, Langbezeichnung and
    Kurzbezeichnung from the rows below."""
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        rows = list(wb[wb.sheetnames[0]].iter_rows(values_only=True))
    finally:
        wb.close()

    def norm(value: object) -> str:
        return "" if value is None else str(value).strip()

    header_index = columns = None
    for i, row in enumerate(rows):
        cells = [norm(c) for c in (row or [])]
        if _DATEV_KOST_HEADER[0] in cells:
            try:
                columns = tuple(cells.index(h) for h in _DATEV_KOST_HEADER)
            except ValueError as e:
                raise SystemExit(f"{path}: KOST-Headerzeile unvollständig: {e}")
            header_index = i
            break
    if header_index is None or columns is None:
        raise SystemExit(
            f"{path}: keine Headerzeile mit {_DATEV_KOST_HEADER[0]!r} gefunden."
        )

    out: list[dict] = []
    for row in rows[header_index + 1 :]:
        if not row:
            continue
        number = norm(row[columns[0]] if columns[0] < len(row) else None)
        if not number:
            continue
        # An empty report field is OMITTED: not mentioning a column leaves the
        # stored value untouched (an empty DATEV field never clears).
        record: dict = {"number": number}
        name = norm(row[columns[1]] if columns[1] < len(row) else None)
        if name:
            record["name"] = name
        short_name = norm(row[columns[2]] if columns[2] < len(row) else None)
        if short_name:
            record["short_name"] = short_name
        out.append(record)
    return out


# How a person's name is spelled out for the manager match. Two passes, tried in
# order: the full first name, then only its first token -- Moss carries the
# everyday first name while hitobito stores the full official one (middle names).
_MANAGER_NAME_EXPRS = (
    ("voller Vorname", "coalesce(p.first_name, '')"),
    ("erster Vorname", "split_part(coalesce(p.first_name, ''), ' ', 1)"),
)

# Case-insensitive, whitespace-collapsed form of a name expression.
_NORMALIZED = "lower(regexp_replace(btrim({expr}), '\\s+', ' ', 'g'))"


def _link_manager_pass(cur, first_name_expr: str) -> int:
    """One matching pass: link every still-unresolved manager_name that matches
    EXACTLY ONE person on "<first_name_expr> <last_name>". Returns the number of
    rows linked."""
    person_name = _NORMALIZED.format(
        expr=f"{first_name_expr} || ' ' || coalesce(p.last_name, '')"
    )
    cost_center_name = _NORMALIZED.format(expr="cc.manager_name")
    import psycopg.sql

    # No updated_at stamp: resolving the link is part of the import itself --
    # a freshly imported row must keep updated_at = NULL.
    query = psycopg.sql.SQL(
        "UPDATE {table} cc SET manager_person_id = m.person_id"
        " FROM ("
        "   SELECT {person_name} AS full_name, min(p.id) AS person_id, count(*) AS n"
        "   FROM people p GROUP BY 1"
        " ) m"
        " WHERE m.n = 1"
        "   AND cc.manager_person_id IS NULL"
        "   AND cc.manager_name IS NOT NULL"
        "   AND {cost_center_name} = m.full_name"
    ).format(
        table=psycopg.sql.Identifier(_TABLE_NAME),
        # Constant SQL expression fragments, composed from module constants
        # only (no external values inside) -- hence the LiteralString casts.
        person_name=psycopg.sql.SQL(_typing.cast("_typing.LiteralString", person_name)),
        cost_center_name=psycopg.sql.SQL(cost_center_name),
    )
    cur.execute(query)
    return cur.rowcount


def _link_manager_persons(conn: _psycopg.Connection) -> None:
    """Resolve manager_name to a Person (manager_person_id).

    Runs the passes in _MANAGER_NAME_EXPRS in order; a name is only linked when
    it matches EXACTLY ONE person, so an ambiguous or unknown name is left
    unresolved rather than guessed. Only rows whose manager_person_id is still
    NULL are touched, so a manual correction in the app is never overwritten and
    an earlier (more exact) pass always wins. Idempotent."""
    with conn.cursor() as cur:
        total = 0
        for label, expr in _MANAGER_NAME_EXPRS:
            linked = _link_manager_pass(cur, expr)
            total += linked
            _LOGGER.info("Manager-Abgleich (%s): %d verknuepft.", label, linked)
        import psycopg.sql

        cur.execute(
            psycopg.sql.SQL(
                "SELECT count(*) FROM {table}"
                " WHERE manager_name IS NOT NULL AND manager_person_id IS NULL"
            ).format(table=psycopg.sql.Identifier(_TABLE_NAME))
        )
        row = cur.fetchone()
        unresolved = row[0] if row else 0
    _LOGGER.info(
        "Manager: %d Kostenstelle(n) mit einer Person verknuepft, %d ohne eindeutigen Treffer.",
        total,
        unresolved,
    )


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "files",
        nargs="+",
        help="Kostenstellen-Dateien (Moss .csv und/oder DATEV KOST .xlsx); "
        "die Quelle wird je Datei automatisch erkannt.",
    )
    p.add_argument(
        "--no-link-managers",
        action="store_true",
        help="Do not resolve manager_name to a Person (leave manager_person_id untouched).",
    )
    p.add_argument(
        "--rollback-for-testing",
        action="store_true",
        default=False,
        help="Apply the plan, then ROLLBACK instead of committing (testing).",
    )
    return p


def _log_plan_summary(planned) -> None:
    """Show what an approval would apply -- logged BEFORE the production
    approval is requested."""
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
        keys = [str(row["number"]) for row in rows]
        shown = ", ".join(keys[:10])
        if len(keys) > 10:
            shown += f", ... (+{len(keys) - 10} more)"
        _LOGGER.info("  %s: %s", label, shown)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        __file__=__file__,
    )
    out_base = ctx.make_out_path(_SELF_NAME + "_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    # All files are merged into ONE plan builder (in argument order; a later
    # file wins on colliding columns), so there is exactly one plan and at
    # most one write() at the end.
    builder = SingleTableUpsertPlanBuilder(
        _TABLE_NAME, "number", [], time_zone=ctx.hitobito_time_zone
    )
    cp1252_columns: set[str] = set()
    for raw_path in ctx.parsed_args.files:
        path = _pathlib.Path(raw_path)
        source = _detect_source(path)
        if source == "moss":
            rows: list = _read_cost_centers(path)
            builder.merge_values(rows)
        else:
            rows = _read_datev_kost(path)
            # DATEV report text is CP1252-limited: when it merges over values
            # from an earlier (Moss) file, a mere transliteration keeps the
            # existing Unicode value; the same columns are later compared
            # transliteration-aware against the database (datev_cp1252.md).
            file_columns = sorted({c for r in rows for c in r if c != "number"})
            cp1252_columns.update(file_columns)
            builder.merge_values(rows, keep_existing_for_cp1252_equality=file_columns)
        _LOGGER.info(
            "%s: %d Kostenstellen aus %s (Quelle: %s)",
            path.name,
            len(rows),
            path,
            source,
        )

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        builder.load_existing(ro_conn)
        planned = builder.plan(
            skip_update_for_cp1252_equality=sorted(cp1252_columns) or False
        )
        # A CHANGED manager_name invalidates the person link (it may point at
        # the previous manager): clear it, so that _link_manager_persons
        # resolves it anew. This rule is general -- thanks to the
        # column-granular plan, "manager_name present in the update" means
        # exactly "manager_name changed"; an untouched name keeps the link,
        # including manual corrections. (A DATEV file never sets manager_name,
        # so it can never trigger this.)
        for update in planned.updates:
            if "manager_name" in update:
                update["manager_person_id"] = None

        _log_plan_summary(planned)

        if not planned.inserts and not planned.updates:
            _LOGGER.info(
                "Cost centers: nothing to write (%d untouched); skipping "
                "write and manager linking.",
                len(planned.untouched_keys),
            )
            return

        if ctx.dry_run:
            _LOGGER.info("[dry-run] Not applying the plan.")
            return

        # The plan summary above shows exactly what this approval applies.
        ctx.require_approval_to_run_in_prod()
        rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
        # Aware start time; the pg_table_* helpers turn it into
        # Rails-compatible UTC-naive created_at/updated_at stamps via the
        # session time zone.
        inserted, updated = planned.apply(rw_conn, now=ctx.start_time)
        _LOGGER.info(
            "Cost centers: %d inserted, %d updated, %d untouched (identical).",
            len(inserted),
            len(updated),
            len(planned.untouched_keys),
        )
        if ctx.parsed_args.no_link_managers:
            _LOGGER.info("[--no-link-managers] Skipped manager person linking.")
        else:
            _link_manager_persons(rw_conn)
        if ctx.parsed_args.rollback_for_testing:
            _LOGGER.warning(
                "ROLLBACK (--rollback-for-testing given) - no changes committed"
            )
            rw_conn.rollback()
        # The commit happens implicitly when the `with ctx:` block exits
        # cleanly; an exception before that leaves the database untouched.


if __name__ == "__main__":
    _sys.exit(main())
