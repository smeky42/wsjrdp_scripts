#!/usr/bin/env -S uv run
"""Example showing how to create a SEPA Direct Debit file."""

from __future__ import annotations

import datetime as _datetime
import logging as _logging
import sys

import wsjrdp2027

_LOGGER = _logging.getLogger(__name__)


COLLECTION_DATE = _datetime.date(2025, 12, 15)


def main():
    start_time = None
    # start_time = datetime.datetime(2025, 8, 15, 10, 30, 27).astimezone()

    ctx = wsjrdp2027.WsjRdpContext(
        start_time=start_time,
        out_dir="data/example_sepa_direct_debit_{{ filename_suffix }}",
    )
    out_base = ctx.make_out_path("example_sepa_direct_debit_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))
    with ctx.psycopg2_connect() as conn:
        df = wsjrdp2027.load_payment_dataframe(
            conn,
            status=wsjrdp2027.DB_PEOPLE_ALL_STATUS,
            sepa_status=wsjrdp2027.DB_PEOPLE_ALL_SEPA_STATUS,
            collection_date=COLLECTION_DATE,
            booking_at=ctx.start_time,
            pedantic=True,
        )

    _LOGGER.info("Registered: %s", len(df))
    df = df[
        ~df["people_status"].isin(
            ["registered", "deregistration_noted", "deregistered"]
        )
    ]
    _LOGGER.info("Printed or further: %s", len(df))
    df = df[df["sepa_status"].isin(["OK"])]
    _LOGGER.info("Printed or further and sepa_status OK: %s", len(df))

    wsjrdp2027.write_payment_dataframe_to_xlsx(df, out_base.with_suffix(".xlsx"))
    wsjrdp2027.write_accounting_dataframe_to_sepa_dd(
        df,
        out_base.with_suffix(".xml"),
        config=wsjrdp2027.WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG,
        pedantic=True,
    )


if __name__ == "__main__":
    sys.exit(main())
