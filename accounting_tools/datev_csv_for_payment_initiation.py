#!/usr/bin/env -S uv run
from __future__ import annotations

import logging as _logging
import pathlib as _pathlib

import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--csv-encoding", choices=["cp1252", "utf-8"], default="utf-8")
    p.add_argument("--limit", type=lambda s: int(s, base=10))
    p.add_argument("--offset", type=lambda s: int(s, base=10))
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
            wsjrdp2027.datev.write_datev_csv_for_pain_id(
                ctx=ctx,
                conn=conn,
                pain_id=pain_id,
                csv_encoding=ctx.parsed_args.csv_encoding,
                limit=ctx.parsed_args.limit,
                offset=ctx.parsed_args.offset,
            )


if __name__ == "__main__":
    __import__("sys").exit(main())
