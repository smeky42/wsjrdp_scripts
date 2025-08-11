#!/usr/bin/env -S uv run
"""SEPA direct debit to collect dues of Jamboree participants

* Create a SEPA direct debit XML file
* Update the accounting_entries database table
* Write a summary Excel xlsx file
* TODO: Create pre-notification email messages and send them

"""

from __future__ import annotations

import datetime
import logging
import sys

import wsjrdp2027

_LOGGER = logging.getLogger()

DEFAULT_COLLECTION_DATE = datetime.date(2025, 8, 15)


def main(argv=None):
    import textwrap

    now = datetime.datetime.now()
    # now = datetime.datetime(2025, 8, 11, 8, 11, 0)

    now_str = now.strftime("%Y%m%d-%H%M%S")
    out_dir = wsjrdp2027.create_dir("data/sepa_direct_debit.%(now)s", now=now)
    out_filename_base = out_dir / f"sepa_direct_debit.{now_str}"
    log_filename = out_filename_base.with_suffix(".log")
    xml_filename = out_filename_base.with_suffix(".xml")
    html_filename = out_filename_base.with_suffix(".html")
    xlsx_filename = out_filename_base.with_suffix(".xlsx")

    wsjrdp2027.ConnectionContext(log_file=log_filename)

    ctx = wsjrdp2027.ConnectionContext()

    collection_date = DEFAULT_COLLECTION_DATE

    with ctx.psycopg2_connect() as conn:
        df = wsjrdp2027.load_payment_dataframe(
            conn,
            early_payer=True,
            collection_date=collection_date,
            booking_at=now,
        )

        sum_amount = df["amount"].sum()
        _LOGGER.info("SUM(amount): %s EUR", sum_amount / 100)

        wsjrdp2027.write_accounting_dataframe_to_sepa_dd(
            df,
            xml_filename,
            config=wsjrdp2027.WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG,
            pedantic=True,
        )

        wsjrdp2027.write_payment_dataframe_to_db(conn, df)

        wsjrdp2027.write_payment_dataframe_to_html(df, html_filename)
        wsjrdp2027.write_payment_dataframe_to_xlsx(df, xlsx_filename)

    _LOGGER.info("finish writing output")

    df_ok = df[df["payment_status"] == "ok"]
    df_not_ok = df[df["payment_status"] != "ok"]

    _LOGGER.info("")
    _LOGGER.info("==== Skipped payments")
    _LOGGER.info("  Number of skipped payments: %s", len(df_not_ok))
    _LOGGER.info(
        "  Skipped payments DataFrame (payment_status != 'ok'):\n%s",
        textwrap.indent(str(df_not_ok), "  | "),
    )
    _LOGGER.info("  SUM(amount): %s EUR", df_not_ok["amount"].sum() / 100)

    _LOGGER.info("")
    _LOGGER.info("==== Payments")
    _LOGGER.info("  Number of payments: %s", len(df_ok))
    _LOGGER.info(
        "  Payments DataFrame (payment_status == 'ok'):\n%s",
        textwrap.indent(str(df_ok), "  | "),
    )
    _LOGGER.info("  SUM(amount): %s EUR", df_ok["amount"].sum() / 100)
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", out_dir)
    _LOGGER.info("  SEPA XML: %s", xml_filename)
    _LOGGER.info("  Excel: %s", xlsx_filename)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
