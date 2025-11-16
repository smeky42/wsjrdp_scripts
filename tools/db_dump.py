#!/usr/bin/env -S uv run
"""Dump the database."""

from __future__ import annotations

import argparse
import sys

import wsjrdp2027


_FORMAT_TO_EXT = {
    "p": ".sql",
    "plain": ".sql",
    "c": ".dump",
    "custom": ".dump",
    "t": ".tar",
    "tar": ".tar",
}


def main(argv=None):
    if argv is None:
        argv = sys.argv
    p = argparse.ArgumentParser()
    p.add_argument(
        "--column-inserts",
        action="store_true",
        default=False,
        help="Dump data as INSERT commands with explicit column names. Restoration will be very slow.",
    )
    p.add_argument(
        "--format",
        help="Selects the format of the output",
        default="custom",
        choices=["p", "plain", "c", "custom", "d", "directory", "t", "tar"],
    )
    p.add_argument("dump_path", nargs="?")
    args = p.parse_args(argv[1:])

    ctx = wsjrdp2027.WsjRdpContext(out_dir="data")

    if args.dump_path:
        dump_path = args.dump_path
    else:
        dump_path = ctx.make_out_path(
            f"{ctx.config.db_name}_{ctx.start_time_for_filename}", relative=False
        )
        if ext := _FORMAT_TO_EXT.get(args.format, ""):
            dump_path = dump_path.with_name(dump_path.name + ext)

    ctx.pg_dump(dump_path=dump_path, format=args.format, column_inserts=args.column_inserts)


if __name__ == "__main__":
    sys.exit(main())
