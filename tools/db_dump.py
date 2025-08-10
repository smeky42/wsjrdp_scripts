#!/usr/bin/env python
"""Dump the database."""

from __future__ import annotations

import argparse
import datetime
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
        "--format",
        help="Selects the format of the output",
        default="custom",
        choices=["p", "plain", "c", "custom", "d", "directory", "t", "tar"],
    )
    p.add_argument("dump_path", nargs="?")
    args = p.parse_args(argv[1:])

    ctx = wsjrdp2027.ConnectionContext()

    if args.dump_path:
        dump_path = args.dump_path
    else:
        now_str = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        ext = _FORMAT_TO_EXT.get(args.format, "")
        dump_path = f"data/{ctx.config.db_name}.{now_str}{ext}"

    ctx.pg_dump(dump_path=dump_path, format=args.format)


if __name__ == "__main__":
    sys.exit(main())
