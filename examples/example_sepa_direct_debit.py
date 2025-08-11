#!/usr/bin/env -S uv run
"""Example showing how to create a SEPA Direct Debit file."""

from __future__ import annotations

import datetime as _datetime
import logging as _logging
import sys

import wsjrdp2027

_LOGGER = _logging.getLogger(__name__)


COLLECTION_DATE = _datetime.date(2025, 8, 15)


def main():
    now = _datetime.datetime.now()
    now = _datetime.datetime(2025, 8, 11, 8, 11, 0)
    out_dir = wsjrdp2027.create_dir("data/example_sepa_direct_debit.%(now)s", now=now)

    ctx = wsjrdp2027.ConnectionContext(log_file=out_dir / "example_dd.log")
    with ctx.psycopg2_connect() as conn:
        df = wsjrdp2027.load_payment_dataframe(
            conn,
            status=wsjrdp2027.DB_PEOPLE_ALL_STATUS,
            sepa_status=wsjrdp2027.DB_PEOPLE_ALL_SEPA_STATUS,
            collection_date=COLLECTION_DATE,
            booking_at=now,
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
    # df = df[df["amount"] > 0]
    # _LOGGER.info("Printed or further and sepa_status OK and amount > 0: %s", len(df))

    wsjrdp2027.write_payment_dataframe_to_xlsx(df, out_dir / "example_dd.xlsx")
    wsjrdp2027.write_accounting_dataframe_to_sepa_dd(
        df,
        out_dir / "example_dd.xml",
        config=wsjrdp2027.WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG,
        pedantic=True,
    )


if __name__ == "__main__":
    sys.exit(main())
