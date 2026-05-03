#!/usr/bin/env -S uv run

import logging as _logging

import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(out_dir="data")
    out_base = ctx.make_out_path("example_people_dataframe_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    df = ctx.load_people_dataframe(
        query=wsjrdp2027.PeopleQuery(
            where=wsjrdp2027.PeopleWhere(exclude_deregistered=False)
        )
    )

    _LOGGER.info("Found %s people", len(df))
    wsjrdp2027.write_people_dataframe_to_xlsx(df, out_base.with_suffix(".xlsx"))


if __name__ == "__main__":
    __import__("sys").exit(main())
