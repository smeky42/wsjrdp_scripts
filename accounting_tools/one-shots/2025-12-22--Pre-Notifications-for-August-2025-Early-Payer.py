#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import pathlib
import pprint
import re
import sys
import textwrap

import psycopg.rows
import wsjrdp2027


SELF_STEM_NAME = pathlib.Path(__file__).stem

L = logging.getLogger(__name__)


def _parse_description(description) -> dict:
    # SEPA Lastschrifteinzug wsjrdp202714-0-91f6e56989 zum 15.08.2025 (Kontoinhaber*in: Ines Höfig, IBAN: DE22120300001036882692, Sequenz: OOFF)
    d = {}
    if m := re.search(r" (wsjrdp2027[a-zA-Z0-9-]+) ", description):
        d["endtoend_id"] = m[1]
    if m := re.search(r"IBAN: ([A-Za-z0-9]+)", description):
        d["sepa_iban"] = m[1]
    return d


def _iban(p_row, ae_row):
    p_iban = p_row["sepa_iban"].upper().replace(" ", "")
    if ae_row["sepa_iban"] != p_iban:
        L.warning(
            "%s: person sepa_iban=%s, accounting sepa_iban=%s",
            p_row["id_and_name"],
            p_row["sepa_iban"],
            ae_row["sepa_iban"],
        )
    return ae_row["sepa_iban"]


def _sepa_dd_description(row):
    id = row["id"]
    short_full_name = row["short_full_name"]
    short_role_name = row["payment_role"].short_role_name
    return f"WSJ 2027 Beitrag {short_full_name} {short_role_name} {id}"


def _insert_pn(conn, row) -> int:
    from wsjrdp2027._payment import insert_direct_debit_pre_notification_from_row

    with conn.cursor() as cursor:
        return insert_direct_debit_pre_notification_from_row(
            cursor,
            row,
            payment_initiation_id=1,
            direct_debit_payment_info_id=1,
            sepa_dd_config=wsjrdp2027.WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG,
            payment_status="xml_generated",
        )


def _execute(conn, query):
    import psycopg.sql

    if isinstance(query, str):
        query_str = query.strip("\r\n")
    else:
        query_str = psycopg.sql.as_string(query)
    if "\n" in query_str:
        query_log = "\n" + textwrap.indent(query_str, "  | ")
    else:
        query_log = " " + query_str
    L.debug("Execute SQL query:%s", query_log)
    conn.execute(query)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(argv=argv)
    log_filename = ctx.make_out_path(SELF_STEM_NAME + "_{{ filename_suffix }}.log")
    ctx.configure_log_file(log_filename)
    with ctx.psycopg_connect() as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            # SELECT * FROM accounting_entries WHERE payment_initiation_id IS NOT NULL AND direct_debit_pre_notification_id IS NULL
            cur.execute(
                """SELECT id, subject_id, description, amount_cents, value_date
FROM accounting_entries
WHERE payment_initiation_id IS NOT NULL AND direct_debit_pre_notification_id IS NULL"""
            )
            results = cur.fetchall()

        for row in results:
            row.update(_parse_description(row["description"]))

        id2ae = {r["subject_id"]: r for r in results}
        L.debug("id2ae:\n%s", textwrap.indent(pprint.pformat(id2ae), "  | "))
        subject_ids = sorted(set(r["subject_id"] for r in results))
        assert len(subject_ids) in (0, 217)

        if len(subject_ids) == 0:
            L.info("New pre-notification entries already written.")
            sys.exit(0)

        df = wsjrdp2027.load_people_dataframe(
            conn,
            query=wsjrdp2027.PeopleQuery(
                where=wsjrdp2027.PeopleWhere(id=subject_ids),
                collection_date="2025-08-15",
            ),
        )

        df["open_amount_cents"] = df["id"].map(
            lambda person_id: id2ae[person_id]["amount_cents"]
        )
        df["sepa_iban"] = df.apply(lambda row: _iban(row, id2ae[row["id"]]), axis=1)
        df["sepa_dd_description"] = df.apply(_sepa_dd_description, axis=1)
        df["sepa_dd_sequence_type"] = "OOFF"
        df["sepa_dd_endtoend_id"] = df["id"].map(
            lambda person_id: id2ae[person_id]["endtoend_id"]
        )

        L.info("Finished patching dataframe")

        df["amount_eur"] = df["open_amount_cents"] / 100.0

        df2 = df[
            [
                "id",
                "short_full_name",
                "amount_eur",
                # "sepa_dd_sequence_type",
                # "collection_date",
                "sepa_iban",
                "sepa_dd_description",
                "sepa_dd_endtoend_id",
            ]
        ]
        L.info(
            "Resulting data (excerpt):\n%s",
            textwrap.indent(df2.to_string(max_rows=220, max_cols=0), "  | "),
        )

        ctx.require_approval_to_run_in_prod(
            "Write new direct debit pre-notifications and accounting_entry updates to DB?"
        )

        if True:
            L.info("Write Pre-Notifications and update accounting_entries accordingly")
            with conn.transaction():
                for _, row in df.iterrows():
                    pn_id = _insert_pn(conn, row)
                    ae_id = id2ae[row["id"]]["id"]
                    _execute(
                        conn,
                        t"UPDATE accounting_entries SET direct_debit_pre_notification_id = {pn_id:l} WHERE id = {ae_id:l}",
                    )
                _execute(
                    conn,
                    """
UPDATE accounting_entries
SET
  value_date = pn.collection_date,
  cdtr_name = pn.cdtr_name,
  cdtr_iban = pn.cdtr_iban,
  cdtr_bic = pn.cdtr_bic,
  cdtr_address = pn.cdtr_address,
  creditor_id = pn.creditor_id,
  dbtr_name = pn.dbtr_name,
  dbtr_iban = pn.dbtr_iban,
  dbtr_bic = pn.dbtr_bic,
  dbtr_address = pn.dbtr_address,
  debit_sequence_type = pn.debit_sequence_type,
  mandate_id = pn.mandate_id,
  mandate_date = pn.mandate_date,
  endtoend_id = pn.endtoend_id,
  pre_notified_amount_cents = pn.pre_notified_amount_cents
FROM wsjrdp_direct_debit_pre_notifications AS pn
WHERE pn.id = accounting_entries.direct_debit_pre_notification_id
  AND accounting_entries.payment_initiation_id = 1;
""",
                )

    L.info("")
    L.info("Output directory: %s", ctx.out_dir)
    L.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
