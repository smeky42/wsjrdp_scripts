from __future__ import annotations

import logging as _logging
import sys

import wsjrdp2027
import pandas as _pandas

import random
from requests.structures import CaseInsensitiveDict
from urllib.parse import quote

_LOGGER = _logging.getLogger(__name__)

def main():
    start_time = None

    ctx = wsjrdp2027.WsjRdpContext(
        start_time=start_time,
        out_dir="data",
    )
    out_base = ctx.make_out_path("example_people_dataframe_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    api_key= ctx.config.geo_api_key

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(conn, exclude_deregistered=False)

    _LOGGER.info("Found %s people", len(df))

    for index, row in df.iterrows(): # .iloc[500:1500].iterrows()
        longitude = row['longitude'] if _pandas.notna(row['longitude']) else "0"
        latitude = row['latitude'] if _pandas.notna(row['latitude']) else "0"
        print(f"id: {row['id']} Coords: {longitude}, {latitude}")

        if not (longitude == "0" or latitude == "0"):
            offset = random.randint(0, 9)
            longitude = str(float(longitude) + (offset / 10000))
            latitude = str(float(latitude) + (offset / 10000))
            print(f"id: {row['id']} New Coords: {longitude}, {latitude}")


            with ctx.psycopg_connect() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE people
                            SET longitude = %s, latitude = %s
                            WHERE id = %s
                        """, (longitude, latitude, row['id']))

                    conn.commit()
                    print(f"id: {row['id']} Updated! {row['first_name']}, {row['last_name']}")

                except Exception as update_error:
                    conn.rollback()
                    print(f"id: {row['id']} Error updating coordinates for {row['first_name']} {row['last_name']}: {update_error}")
                    raise

if __name__ == "__main__":
    sys.exit(main())
