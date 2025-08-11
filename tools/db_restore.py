#!/usr/bin/env -S uv run
"""Restore the database.

..important:: Restoring into production is by default not allowed.
"""

from __future__ import annotations

import argparse
import sys

import wsjrdp2027


def main(argv=None):
    if argv is None:
        argv = sys.argv
    p = argparse.ArgumentParser()
    p.add_argument("dump_path")
    args = p.parse_args(argv[1:])
    dump_path = args.dump_path

    ctx = wsjrdp2027.ConnectionContext()
    ctx.pg_restore(dump_path=dump_path)


if __name__ == "__main__":
    sys.exit(main())
