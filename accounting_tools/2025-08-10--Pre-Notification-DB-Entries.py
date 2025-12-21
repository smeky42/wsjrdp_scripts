#!/usr/bin/env -S uv run
from __future__ import annotations

import datetime
import logging
import sys

import psycopg
import wsjrdp2027


_LOGGER = logging.getLogger(__name__)

COLLECTION_DATE = datetime.date(2025, 8, 15)


_AUG2025_PAIN_ID = 1
_AUG2025_DD_PYMNT_INF_ID = 1


def insert_payment_initiation_and_dd_payment_info(
    cursor: psycopg.Cursor, *, sepa_dd_config: wsjrdp2027.SepaDirectDebitConfig
) -> None:
    _LOGGER.info("Insert payment initiation and direct debit payment info")
    pain_id = wsjrdp2027.pg_insert_payment_initiation(
        cursor=cursor,
        sepa_dd_config=sepa_dd_config,
    )
    _LOGGER.info("payment initiation id: %s", pain_id)
    pymnt_inf_id = wsjrdp2027.pg_insert_direct_debit_payment_info(
        cursor,
        payment_initiation_id=pain_id,
        sepa_dd_config=sepa_dd_config,
        creditor_id=wsjrdp2027.CREDITOR_ID,
    )
    _LOGGER.info("direct debit payment info id: %s", pymnt_inf_id)


def main():
    ctx = wsjrdp2027.WsjRdpContext(
        setup_logging=True,
        log_level=logging.DEBUG,
    )

    ctx.require_approval_to_run_in_prod(
        "Do you want to store pre-notification data in the PRODUCTION Hitobito database?"
    )

    sepa_dd_config: wsjrdp2027.SepaDirectDebitConfig = (  # noqa  # type: ignore
        wsjrdp2027.WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG
    )

    pain_id = _AUG2025_PAIN_ID
    pymnt_inf_id = _AUG2025_DD_PYMNT_INF_ID

    with ctx.psycopg_connect() as conn:
        with conn.cursor() as _:
            # insert_payment_initiation_and_dd_payment_info(
            #     cur, sepa_dd_config=sepa_dd_config
            # )
            _LOGGER.info("payment initiation id: %s", pain_id)
            _LOGGER.info("direct debit payment info id: %s", pymnt_inf_id)


if __name__ == "__main__":
    sys.exit(main())
