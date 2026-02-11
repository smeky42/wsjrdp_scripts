#!/usr/bin/env -S uv run
from __future__ import annotations

import io as _io
import logging as _logging
import pathlib as _pathlib

import psycopg as _psycopg
import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


def write_datev_csv_for_pain_id(
    *, ctx: wsjrdp2027.WsjRdpContext, conn: _psycopg.Connection, pain_id: int
):
    import csv
    import re

    df = wsjrdp2027.pg_select_dataframe(
        conn,
        t"""SELECT
  id, payment_status,
  dbtr_name, dbtr_iban, dbtr_bic,
  amount_cents, debit_sequence_type, collection_date,
  mandate_id, mandate_date,
  description, endtoend_id
FROM wsjrdp_direct_debit_pre_notifications
WHERE payment_status = 'xml_generated'
  AND payment_initiation_id = {pain_id}
""",
    )

    COLS = [
        "Umsatz",
        "BU_Gegenkonto_H",
        "Belegfeld1",
        "Datum",
        "Konto_S",
        "KOST1",
        "Buchungstext",
    ]

    row = df.iloc[0]
    collection_date = row['collection_date']

    csv_filename = ctx.make_out_path(f"sepa_direct_debit_datev_csv_{collection_date.strftime('%Y-%m')}_pain{pain_id}.csv")
    with open(csv_filename, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=COLS, delimiter=";")
        writer.writeheader()
        for _, row in df.iterrows():
            amount_eur = int(round(row["amount_cents"])) / 100
            amount_de = str(f"{amount_eur:.2f}").replace(".", ",")
            collection_date = row["collection_date"]
            belegfeld = f"{wsjrdp2027.to_month_year_de(collection_date)} Einzug {row['debit_sequence_type']} (id={pain_id})"
            buchungstext = row["description"].replace(" WSJ 2027 ", " ")
            buchungstext = buchungstext.removeprefix("WSJ 2027 ")
            buchungstext = re.sub(
                r"Beitrag *(?P<descr>.*) +(?P<role>CMT|YP|UL|IST) (?P<id>[0-9]+)",
                r"\g<role> \g<id> \g<descr> / Beitrag",
                buchungstext,
            )
            buchungstext = re.sub(
                r"(?P<role>CMT|YP|UL|IST) Beitrag *(?P<descr>.*) +(?P<id>[0-9]+)",
                r"\g<role> \g<id> \g<descr> / Beitrag",
                buchungstext,
            )
            buchungstext = re.sub(
                r"(?P<installment>[0-9]+. Rate \w+ 202[567]) (?P<role>CMT|YP|UL|IST) *(?P<descr>.*) +\(id (?P<id>[0-9]+)\)",
                r"\g<role> \g<id> \g<descr> / \g<installment>",
                buchungstext,
            )
            writer.writerow(
                {
                    "Umsatz": amount_de,
                    "BU_Gegenkonto_H": "8116",
                    "Belegfeld1": belegfeld,
                    "Datum": collection_date.strftime("%d.%m.%Y"),
                    "Konto_S": "1200",
                    "KOST1": "9500",  # Kostenstelle für Beiträge
                    "Buchungstext": buchungstext,
                }
            )
    _LOGGER.info("    %s rows in dataframe", len(df))
    _LOGGER.info(f"  wrote {csv_filename}")


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("pain_ids", nargs="*", type=lambda s: int(s, base=10))
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        __file__=__file__,
    )
    out_base = ctx.make_out_path(_pathlib.Path(__file__).stem)
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    with ctx.psycopg_connect() as conn:
        for pain_id in ctx.parsed_args.pain_ids:
            write_datev_csv_for_pain_id(ctx=ctx, conn=conn, pain_id=pain_id)


if __name__ == "__main__":
    import sys

    sys.exit(main())
