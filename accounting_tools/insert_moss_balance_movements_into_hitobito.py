#!/usr/bin/env -S uv run
from __future__ import annotations

import logging as _logging
import pathlib as _pathlib
import sys as _sys

import wsjrdp2027


_SELF_NAME = _pathlib.Path(__file__).stem

_LOGGER = _logging.getLogger(__name__)


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("csv_files", nargs="+")
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
    )
    out_base = ctx.make_out_path(_SELF_NAME)
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    balance_movements = []
    for csv_filename in ctx.parsed_args.csv_files:
        _LOGGER.info("")
        _LOGGER.info("Moss balance movement CSV file: %s", csv_filename)
        balance_movements.extend(
            wsjrdp2027.moss.MossBalanceMovement.iter_from_path(csv_filename)
        )

    with ctx.psycopg_connect() as conn:
        ctx.require_approval_to_run_in_prod()
        wsjrdp2027.pg.insert_moss_balance_movement(conn, balance_movements)

    # with ctx.psycopg_connect() as conn:
    #     ctx.require_approval_to_run_in_prod()
    #     account_identification2fin_account_id = _load_fin_accounts(conn)
    #
    #     for camt_file in ctx.parsed_args.camt_files:
    #         _LOGGER.info("")
    #         _LOGGER.info("CAMT file: %s", camt_file)
    #         _read_camt_and_write_to_db(
    #             ctx,
    #             conn,
    #             camt_file,
    #             account_identification2fin_account_id=account_identification2fin_account_id,
    #         )
    #         _LOGGER.info("%s", "=" * 60)

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    _sys.exit(main())
