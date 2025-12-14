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


def create_argument_parser():
    import argparse

    from wsjrdp2027 import to_date_or_none

    p = argparse.ArgumentParser()
    p.add_argument("--accounting", action="store_true", default=True)
    p.add_argument("--no-accounting", dest="accounting", action="store_false")
    p.add_argument(
        "--collection-date", type=to_date_or_none, default=DEFAULT_COLLECTION_DATE
    )
    p.add_argument("--payment-initiation-id", type=int)
    return p


def main(argv=None):
    import textwrap

    ctx = wsjrdp2027.WsjRdpContext(
        argv=argv,
        argument_parser=create_argument_parser(),
        out_dir="data/sepa_direct_debit_{{ filename_suffix }}",
    )
    args = ctx.parsed_args
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
                query=wsjrdp2027.PeopleQuery(
                    collection_date=args.collection_date, now=ctx.start_time
                ),
                booking_at=ctx.start_time,
            )

        sum_amount = df["open_amount_cents"].sum()

        for _, row in df.iterrows():
            if not isinstance(row["open_amount_cents"], (int, float)):
                err_msg = "Invalid row: 'open_amount_cents' value is not int or float"
                _LOGGER.error(
                    "%s:\n%s", err_msg, textwrap.indent(row.to_string(), "  | ")
                )
                raise RuntimeError(err_msg)

        _LOGGER.info(
            "SUM(open_amount_cents): %s", wsjrdp2027.format_cents_as_eur_de(sum_amount)
        )

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
    _LOGGER.info(
        "  SUM(open_amount_cents): %s",
        wsjrdp2027.format_cents_as_eur_de(df_not_ok["open_amount_cents"].sum()),
    )

    _LOGGER.info("")
    _LOGGER.info("==== Payments")
    _LOGGER.info("  Number of payments: %s", len(df_ok))
    _LOGGER.info(
        "  Payments DataFrame (payment_status == 'ok'):\n%s",
        textwrap.indent(str(df_ok), "  | "),
    )
    _LOGGER.info(
        "  SUM(open_amount_cents): %s",
        wsjrdp2027.format_cents_as_eur_de(df_ok["open_amount_cents"].sum()),
    )
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  SEPA XML: %s", xml_filename)
    _LOGGER.info("  Excel: %s", xlsx_filename)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
