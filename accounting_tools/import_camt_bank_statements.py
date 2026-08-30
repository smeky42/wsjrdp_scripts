#!/usr/bin/env -S uv run
from __future__ import annotations

import logging as _logging
import pathlib as _pathlib
import sys as _sys

import wsjrdp2027


_SELF_NAME = _pathlib.Path(__file__).stem

_LOGGER = _logging.getLogger(__name__)


def _load_fin_accounts(conn, /) -> dict[str, int]:
    fin_accounts = wsjrdp2027.pg_select_dict_rows(
        conn, t"""SELECT "id", "account_identification" FROM wsjrdp_fin_accounts;"""
    )
    return {row["account_identification"]: row["id"] for row in fin_accounts}


def get_fin_account_id(
    ctx,
    conn,
    account_identification: str,
    account_identification2fin_account_id: dict[str, int],
) -> int:
    try:
        return account_identification2fin_account_id[account_identification]
    except KeyError:
        _LOGGER.warning(f"Account {account_identification} not in DB")

        acc = wsjrdp2027.bank_accounts.WSJ27_ACCOUNT_IDENTIFICATION_TO_BANK_ACCOUNT_DICT.get(
            account_identification
        )
        if acc:
            _LOGGER.info(f"Need to insert new account into db:\n{acc}")
            ctx.require_approval_to_run_in_prod("Insert new account into db")

            acc_id = wsjrdp2027.pg_insert_fin_account(conn, **acc.asdict())
            account_identification2fin_account_id[account_identification] = acc_id
            return acc_id
        else:
            _LOGGER.error("  Account unknown -> exit")
            raise RuntimeError(f"Unknown account {account_identification!r}")


def _read_camt_and_write_to_db(
    ctx,
    conn,
    /,
    path: _pathlib.Path | str,
    *,
    account_identification2fin_account_id: dict[str, int],
) -> None:
    from wsjrdp2027 import (
        pg_insert_camt_transaction_from_tx,
        pg_select_camt_tx_unique_db_key2row,
    )

    camt = wsjrdp2027.CamtMessage.load(path)
    _LOGGER.info(camt)

    tx_unique_db_key2row = pg_select_camt_tx_unique_db_key2row(conn, show_result=False)

    for tx in camt.booked_transaction_details:
        _LOGGER.info("  %s", "-" * 40)
        _LOGGER.info(f"  account_identification: {tx.account_identification}")
        _LOGGER.info(f"  amount: {wsjrdp2027.format_cents_as_eur_de(tx.amount_cents)}")
        _LOGGER.info(f"  description: {tx.description}")
        if tx.return_reason:
            _LOGGER.info(f"  return_reason: {tx.return_reason}")
        _LOGGER.info(f"  booking_date: {tx.booking_date}")
        if refs := tx.references:
            _LOGGER.info("  references:")
            for k, v in refs.items():
                _LOGGER.info(f"    {k}: {v}")
        # TODO: Handle new accounts appearing in the CAMT file
        fin_account_id = get_fin_account_id(
            ctx,
            conn,
            tx.account_identification,
            account_identification2fin_account_id=account_identification2fin_account_id,
        )
        if tx_row := tx_unique_db_key2row.get(tx.unique_db_key):
            errors = []
            for key in ["amount_cents", "amount_currency", "value_date"]:
                row_val = tx_row[key]
                tx_val = getattr(tx, key, None)
                if row_val != tx_val:
                    err_msg = f"Tx {tx.account_servicer_reference}: Mismatch of {key}: tx={tx_val!r} row={row_val!r}"
                    errors.append(err_msg)
                    _LOGGER.error(err_msg)
            if errors:
                raise RuntimeError("\n".join(errors))
            else:
                _LOGGER.info(f"  ->  {tx_row['id']} (already in DB)")
        else:
            camt_tx_id = pg_insert_camt_transaction_from_tx(
                conn,
                tx,
                fin_account_id=fin_account_id,
                upsert=True,
            )
            _LOGGER.info(f"  ->  {camt_tx_id} (newly inserted)")


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("camt_files", nargs="+")
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
    )
    out_base = ctx.make_out_path(_SELF_NAME)
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    with ctx.psycopg_connect() as conn:
        ctx.require_approval_to_run_in_prod()
        account_identification2fin_account_id = _load_fin_accounts(conn)

        for camt_file in ctx.parsed_args.camt_files:
            _LOGGER.info("")
            _LOGGER.info("CAMT file: %s", camt_file)
            _read_camt_and_write_to_db(
                ctx,
                conn,
                camt_file,
                account_identification2fin_account_id=account_identification2fin_account_id,
            )
            _LOGGER.info("%s", "=" * 60)

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    _sys.exit(main())
