#!/usr/bin/env -S uv run
"""Dumps the database and restores it into dev (config-dev.yml)."""

from __future__ import annotations

import argparse
import datetime
import sys

import wsjrdp2027


def main(argv=None):
    if argv is None:
        argv = sys.argv
    p = argparse.ArgumentParser()
    p.add_argument("dump_path", nargs="?")
    args = p.parse_args(argv[1:])

    ctx = wsjrdp2027.ConnectionContext()

    if args.dump_path:
        dump_path = args.dump_path
    else:
        now_str = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        dump_path = f"data/{ctx.config.db_name}.{now_str}.dump"

    ctx.pg_dump(dump_path=dump_path, format="custom")

    dev_ctx = wsjrdp2027.ConnectionContext(config_path="config-dev.yml")
    dev_ctx.pg_restore(dump_path=dump_path, restore_into_production=False)


if __name__ == "__main__":
    sys.exit(main())
