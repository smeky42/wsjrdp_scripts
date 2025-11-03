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


DEFAULT_COLLECTION_DATE = datetime.date(2025, 1, 1)


def parse_args(argv=None):
    import argparse
    import sys

    from wsjrdp2027._util import to_date

    if argv is None:
        argv = sys.argv
    p = argparse.ArgumentParser()
    p.add_argument("--accounting", action="store_true", default=True)
    p.add_argument("--no-accounting", dest="accounting", action="store_false")
    p.add_argument("--collection-date", type=to_date, default=DEFAULT_COLLECTION_DATE)
    p.add_argument("--payment-initiation-id", type=int)
    return p.parse_args(argv[1:])


def main(argv=None):
    import textwrap

    args = parse_args(argv=argv)

    start_time = None
    # start_time = datetime.datetime(2025, 8, 15, 10, 30, 27).astimezone()

    ctx = wsjrdp2027.WsjRdpContext(
        setup_logging=True,
        start_time=start_time,
        out_dir="data/sepa_direct_debit_{{ filename_suffix }}",
    )
    out_base = ctx.make_out_path("sepa_direct_debit_{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    xml_filename = out_base.with_suffix(".xml")
    html_filename = out_base.with_suffix(".html")
    xlsx_filename = out_base.with_suffix(".xlsx")

    ctx.configure_log_file(log_filename)
    ctx.require_approval_to_run_in_prod()

    with ctx.psycopg_connect() as conn:
        if args.payment_initiation_id:
            df = wsjrdp2027.load_payment_dataframe_from_payment_initiation(
                conn,
                payment_initiation_id=args.payment_initiation_id,
                pedantic=False,
                # booking_at=ctx.start_time,
                # today=ctx.start_time.date(),
            )
        else:
            df = wsjrdp2027.load_payment_dataframe(
                conn,
                collection_date=args.collection_date,
                booking_at=ctx.start_time,
                pedantic=False,
                today=ctx.start_time.date(),
            )

        sum_amount = df["amount"].sum()

        for _, row in df.iterrows():
            if not isinstance(row["amount"], (int, float)):
                print(row.to_string())
                raise RuntimeError("Invalid row: 'amount' value is not int or float")

        _LOGGER.info("SUM(amount): %s EUR", sum_amount / 100)

        wsjrdp2027.write_accounting_dataframe_to_sepa_dd(
            df,
            xml_filename,
            config=wsjrdp2027.WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG,
            pedantic=True,
        )

        if args.accounting:
            wsjrdp2027.write_payment_dataframe_to_db(conn, df)
        else:
            _LOGGER.info("")
            _LOGGER.info("SKIP ACCOUNTING (--no-accounting given)")
            _LOGGER.info("")

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
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  SEPA XML: %s", xml_filename)
    _LOGGER.info("  Excel: %s", xlsx_filename)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
