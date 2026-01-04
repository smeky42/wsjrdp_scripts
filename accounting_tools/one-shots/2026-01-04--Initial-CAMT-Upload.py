#!/usr/bin/env -S uv run
from __future__ import annotations

import logging as _logging
import pathlib as _pathlib
import sys as _sys

import wsjrdp2027


_SELF_NAME = _pathlib.Path(__file__).stem

_LOGGER = _logging.getLogger(__name__)

_PAX_DD = wsjrdp2027.WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG
_SKAT_DD = wsjrdp2027.WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG


def upsert_accounts(conn) -> dict[str, int]:
    from wsjrdp2027 import pg_insert_fin_account

    accounting_identification2fin_account_id = {}
    skat_id = pg_insert_fin_account(
        conn,
        account_identification=_SKAT_DD["IBAN"],
        opening_balance_cents=0,
        opening_balance_currency="EUR",
        opening_balance_date="2025-07-02",
        short_name="Skatbank",
        iban=_SKAT_DD["IBAN"],
        owner_name=_SKAT_DD["name"],
        owner_address=_SKAT_DD["address_as_single_line"],
        servicer_name="VR-Bank Altenburger Land eG",
        servicer_bic="GENODEF1SLR",
        servicer_address="Altenburger Str. 13, 04626 Schmölln",
        upsert=True,
    )
    accounting_identification2fin_account_id[_SKAT_DD["IBAN"]] = skat_id
    pax_id = pg_insert_fin_account(
        conn,
        account_identification=_PAX_DD["IBAN"],
        opening_balance_cents=73579,
        opening_balance_currency="EUR",
        opening_balance_date="2025-08-15",
        short_name="Pax-Bank Konto 44",
        iban=_PAX_DD["IBAN"],
        owner_name=_PAX_DD["name"],
        owner_address=_PAX_DD["address_as_single_line"],
        servicer_name="Pax-Bank für Kirche und Caritas eG",
        servicer_bic="GENODED1PAX",
        servicer_address="Kamp 17, 33098 Paderborn",
        upsert=True,
    )
    accounting_identification2fin_account_id[_PAX_DD["IBAN"]] = pax_id
    return accounting_identification2fin_account_id


def read_camt_and_write_to_db(
    conn,
    /,
    path: _pathlib.Path | str,
    *,
    accounting_identification2fin_account_id: dict[str, int],
) -> None:
    from wsjrdp2027 import pg_insert_camt_transaction_from_tx

    camt = wsjrdp2027.CamtMessage.load(path)
    _LOGGER.info(camt)

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
        fin_account_id = accounting_identification2fin_account_id[
            tx.account_identification
        ]
        camt_tx_id = pg_insert_camt_transaction_from_tx(
            conn,
            tx,
            fin_account_id=fin_account_id,
            upsert=True,
        )
        _LOGGER.info(f"  ->  {camt_tx_id}")


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
        accounting_identification2fin_account_id = upsert_accounts(conn)

        for camt_file in ctx.parsed_args.camt_files:
            _LOGGER.info("")
            _LOGGER.info("CAMT file: %s", camt_file)
            read_camt_and_write_to_db(
                conn,
                camt_file,
                accounting_identification2fin_account_id=accounting_identification2fin_account_id,
            )
            _LOGGER.info("%s", "=" * 60)

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    _sys.exit(main())
