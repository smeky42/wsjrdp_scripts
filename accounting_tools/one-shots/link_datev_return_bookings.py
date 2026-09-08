#!/usr/bin/env -S uv run
"""Apply the Retouren fee-link rule to the WHOLE database (retroactive).

The rule lives in `wsjrdp2027.datev_fee_links`: it links a returned ("Retoure")
fee booking to the accounting entry of the returned bank transaction
(match_return_fee_entries, classification_string
`retoure_matching_camt_return_by_amount_and_date`) and then mirrors that link
onto the camt transaction. accounting_tools/import_datev_buchungsstapel.py runs
it on the bookings of the file it has just imported; this one-shot runs exactly
the same rule -- from the same package module, so there stays exactly ONE
definition of what a Retoure link is -- once over EVERY booking of the
database. That is what a rule added after the Buchungsstapel were already
imported needs, instead of a full reload.

Read-only until the preview has been logged (the match query writes nothing),
then one bundled UPDATE per step. Idempotent: a second run links nothing.

Usage:
  ./accounting_tools/one-shots/link_datev_return_bookings.py
  ./accounting_tools/one-shots/link_datev_return_bookings.py --dry-run
  ./accounting_tools/one-shots/link_datev_return_bookings.py --rollback-for-testing
"""

from __future__ import annotations

import logging as _logging
import pathlib as _pathlib
import sys as _sys

import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)
_SELF_NAME = _pathlib.Path(__file__).stem


def create_argument_parser():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    # NB: --dry-run comes from the WsjRdpContext base parser (ctx.dry_run).
    parser.add_argument(
        "--rollback-for-testing",
        action="store_true",
        default=False,
        help="Apply the links, then ROLLBACK instead of committing (testing).",
    )
    return parser


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        __file__=__file__,
    )
    out_base = ctx.make_out_path(_SELF_NAME + "_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    fee_links = wsjrdp2027.datev_fee_links
    with ctx:
        # Preview on the read-only connection: the rule's own match query,
        # without the UPDATE behind it.
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        guids = fee_links.all_booking_guids(ro_conn)
        rows = fee_links.select_return_fee_matches(ro_conn, guids)
        matched = sum(1 for _booking_id, entry_id in rows if entry_id is not None)
        _LOGGER.info(
            "%d Buchung(en) in der Datenbank. Retouren-Regel: %d eindeutige "
            "Verknuepfung(en), %d Retoure-Buchung(en) ohne eindeutigen Treffer.",
            len(guids),
            matched,
            len(rows) - matched,
        )
        if not matched:
            _LOGGER.info("Nichts zu verknuepfen.")
            return 0
        if ctx.dry_run:
            _LOGGER.warning("[--dry-run] Nichts geschrieben.")
            return 0

        ctx.require_approval_to_run_in_prod()
        rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
        with rw_conn.cursor() as cur:
            fee_links.match_return_fee_entries(cur, guids, now=ctx.start_time)
            fee_links.mirror_camt_links(cur, guids)

        if ctx.parsed_args.rollback_for_testing:
            _LOGGER.warning(
                "ROLLBACK (--rollback-for-testing given) - no changes committed"
            )
            rw_conn.rollback()
        # The commit happens implicitly when the `with ctx:` block exits cleanly.
    return 0


if __name__ == "__main__":
    _sys.exit(main())
