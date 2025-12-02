#!/usr/bin/env -S uv run
from __future__ import annotations

import logging as _logging
import sys

import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


def main():
    start_time = None
    # start_time = datetime.datetime(2025, 8, 15, 10, 30, 27).astimezone()

    ctx = wsjrdp2027.WsjRdpContext(
        start_time=start_time,
        out_dir="data",
    )
    out_base = ctx.make_out_path("example_people_dataframe_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(
            conn,
            query=wsjrdp2027.PeopleQuery(
                where=wsjrdp2027.PeopleWhere(exclude_deregistered=False)
            ),
        )

    _LOGGER.info("Found %s people", len(df))
    wsjrdp2027.write_people_dataframe_to_xlsx(df, out_base.with_suffix(".xlsx"))


if __name__ == "__main__":
    sys.exit(main())
