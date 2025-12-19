#!/usr/bin/env -S uv run
from __future__ import annotations

import logging as _logging
import random
import sys

import pandas as _pandas
import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(argv=argv, out_dir="data")
    out_base = ctx.make_out_path("move_longitude_latitude_{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    api_key = ctx.config.geo_api_key

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(conn)

    _LOGGER.info("Found %s people", len(df))

    for index, row in df.iterrows():  # .iloc[500:1500].iterrows()
        longitude = row["longitude"] if _pandas.notna(row["longitude"]) else "0"
        latitude = row["latitude"] if _pandas.notna(row["latitude"]) else "0"
        _LOGGER.info(f"id: {row['id']} Coords: {longitude}, {latitude}")

        if not (longitude == "0" or latitude == "0"):
            offset = random.randint(0, 9)
            longitude = str(float(longitude) + (offset / 10000))
            latitude = str(float(latitude) + (offset / 10000))
            _LOGGER.info(f"id: {row['id']} New Coords: {longitude}, {latitude}")

            if ctx.dry_run:
                _LOGGER.info("Skip (dry_run)")
                continue
            with ctx.psycopg_connect() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE people
                            SET longitude = %s, latitude = %s
                            WHERE id = %s
                        """,
                            (longitude, latitude, row["id"]),
                        )

                    conn.commit()
                    _LOGGER.info(
                        f"id: {row['id']} Updated! {row['first_name']}, {row['last_name']}"
                    )

                except Exception as update_error:
                    conn.rollback()
                    _LOGGER.info(
                        f"id: {row['id']} Error updating coordinates for {row['first_name']} {row['last_name']}: {update_error}"
                    )
                    raise
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
