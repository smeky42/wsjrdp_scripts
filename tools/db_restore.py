#!/usr/bin/env -S uv run
"""Restore the database.

..important:: Restoring into production is by default not allowed.
"""

from __future__ import annotations

import sys

import wsjrdp2027


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("dump_path", nargs="?")
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(argument_parser=create_argument_parser(), argv=argv)
    dump_path = ctx.parsed_args.dump_path

    if ctx.is_production or ctx.config.db_name in ["hitobito_production"]:
        ctx.require_approval_to_run_in_prod()
    ctx.pg_restore(dump_path=dump_path, restore_into_production=False)


if __name__ == "__main__":
    sys.exit(main())
