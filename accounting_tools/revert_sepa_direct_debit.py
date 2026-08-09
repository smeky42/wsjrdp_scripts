#!/usr/bin/env -S uv run
"""UNDO a SEPA direct debit run for a given payment initiation.

This is the inverse of ``accounting_tools/sepa_direct_debit.py``. That script,
for a given ``payment_initiation_id`` (P), performs these database changes
(all inside one transaction):

1. INSERT rows into ``accounting_entries`` (``payment_initiation_id = P``).
2. INSERT rows into ``wsjrdp_direct_debit_payment_infos``
   (``payment_initiation_id = P``).
3. UPDATE ``wsjrdp_direct_debit_pre_notifications.payment_status`` from
   ``pre_notified`` to ``xml_generated`` (or to ``skipped`` for
   non-ok / try_skip rows), for rows with ``payment_initiation_id = P``.
4. UPDATE ``wsjrdp_payment_initiations`` row P: ``status`` from ``planned`` to
   ``xml_generated`` and stamp ``message_identification``,
   ``number_of_transactions``, ``control_sum_cents``, ``initiating_party_name``,
   ``sepa_schema``.

The DATEV CSV export and the dataframe loading are read-only and leave no
database trace.

This UNDO script reverses exactly those changes.
:meth:`RevertSepaDirectDebit.from_payment_initiation_id` gathers the affected row
ids and builds the SQL statements; :meth:`RevertSepaDirectDebit.apply` runs them
in this order (chosen so the foreign keys never block a step):

1. DELETE the collected ``accounting_entries`` (by id).
2. UPDATE the collected ``wsjrdp_direct_debit_pre_notifications`` (by id, guarded
   on ``payment_status = 'xml_generated'``): set ``payment_status`` back to
   ``pre_notified`` and clear ``direct_debit_payment_info_id`` to NULL. This runs
   before step 3 because that FK would otherwise block the payment-info delete.
   Rows in status ``skipped`` are left untouched on purpose.
3. DELETE the collected ``wsjrdp_direct_debit_payment_infos`` (by id).
4. UPDATE ``wsjrdp_payment_initiations`` row P: unconditionally set ``status``
   to ``planned`` and clear the stamped metadata columns. The old status is
   captured beforehand so the undo-the-undo SQL can restore it.

:meth:`RevertSepaDirectDebit.apply` reports what each statement changed (via its
``RETURNING`` clause) and emits the SQL that would revert this revert (undo the
undo): the re-INSERTs are rebuilt from the full-column rows collected up front,
the reverse UPDATEs from the captured old values. It is written next to the log
file as ``*.undo-the-undo.sql``.

ALL changes are collected first and shown as a summary. They are executed only
after a SINGLE confirmation, in one transaction.

The ``wsjrdp_payment_initiations`` row is never deleted (the pre-notifications
still reference it via ``payment_initiation_id``); it is reset to ``planned``.

Typical usage (always test first, dev + rollback):

    ./accounting_tools/revert_sepa_direct_debit.py --payment-initiation-id 42 --dry-run
    ./accounting_tools/revert_sepa_direct_debit.py --payment-initiation-id 42

"""

from __future__ import annotations

import logging
import sys
import typing as _typing

import wsjrdp2027


if _typing.TYPE_CHECKING:
    import argparse as _argparse

    import psycopg as _psycopg

_LOGGER = logging.getLogger()


def create_argument_parser() -> _argparse.ArgumentParser:
    import argparse

    p = argparse.ArgumentParser(
        description="Undo the database changes of a SEPA direct debit run."
    )
    p.add_argument(
        "--payment-initiation-id",
        type=int,
        default=None,
        help="""Id of the wsjrdp_payment_initiations row to revert. If omitted,
        a list is shown for interactive selection.""",
    )
    p.add_argument(
        "-y",
        "--yes",
        action="store_true",
        default=False,
        help="""Skip the interactive confirmation in NON-production. Production
        always asks for approval regardless of this flag.""",
    )
    p.add_argument("--rollback-for-testing", action="store_true", default=False)
    return p


def _to_eur(cents) -> str:
    if cents is None:
        return "-"
    return wsjrdp2027.format_cents_as_eur_de(int(cents), zero_cents=",00")


def _fmt_created_at(value) -> str:
    if value is None:
        return ""
    try:
        return value.strftime("%Y-%m-%d %H:%M")
    except AttributeError:
        return str(value)


def _fmt_date(value) -> str:
    if value is None:
        return ""
    try:
        return value.strftime("%Y-%m-%d")
    except AttributeError:
        return str(value)


def _make_selector_app_class():
    """Build the textual App class lazily so textual is only imported when a
    selection is actually needed."""
    from rich.text import Text
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.widgets import DataTable, Footer, Header

    def _right(value) -> Text:
        return Text("" if value is None else str(value), justify="right")

    class PaymentInitiationSelectorApp(App):
        TITLE = "revert_sepa_direct_debit"
        SUB_TITLE = "Select the payment initiation to revert (Enter), or cancel (q/Esc)"
        BINDINGS = [
            Binding("escape", "cancel", "Cancel"),
            Binding("q", "cancel", "Cancel"),
        ]

        def __init__(self, rows):
            super().__init__()
            self._rows = rows

        def compose(self) -> ComposeResult:
            yield Header()
            yield DataTable(cursor_type="row", zebra_stripes=True)
            yield Footer()

        def on_mount(self) -> None:
            table = self.query_one(DataTable)
            table.add_column("id", key="id")
            table.add_column("status")
            table.add_column("created_at")
            table.add_column("collection_date")
            table.add_column(Text("n_tx", justify="right"))
            table.add_column(Text("control_sum", justify="right"))
            table.add_column(Text("infos", justify="right"))
            table.add_column(Text("pre_notif", justify="right"))
            table.add_column(Text("pn_xml", justify="right"))
            table.add_column(Text("acc", justify="right"))
            table.add_column("message_identification")
            for r in self._rows:
                n_tx = r["number_of_transactions"]
                table.add_row(
                    str(r["id"]),
                    r["status"] or "",
                    _fmt_created_at(r["created_at"]),
                    _fmt_date(r["collection_date"]),
                    _right("" if n_tx is None else n_tx),
                    _right(_to_eur(r["control_sum_cents"])),
                    _right(r["n_payment_infos"]),
                    _right(r["n_pre_notifications"]),
                    _right(r["n_pre_notifications_xml"]),
                    _right(r["n_accounting_entries"]),
                    r["message_identification"] or "",
                    key=str(r["id"]),
                )
            table.focus()

        def on_data_table_row_selected(self, event) -> None:
            self.exit(int(event.row_key.value))

        def action_cancel(self) -> None:
            self.exit(None)

    return PaymentInitiationSelectorApp


def _select_payment_initiation_interactively(conn: _psycopg.Connection) -> int | None:
    """Show a textual list of payment initiations and return the chosen id
    (or None if the user cancelled)."""
    rows = wsjrdp2027.pg_select_dict_rows(
        conn,
        t"""SELECT
              pi.id,
              pi.status,
              pi.created_at,
              pi.number_of_transactions,
              pi.control_sum_cents,
              pi.message_identification,
              (SELECT count(*) FROM wsjrdp_direct_debit_payment_infos pmi
                 WHERE pmi.payment_initiation_id = pi.id) AS n_payment_infos,
              (SELECT count(*) FROM wsjrdp_direct_debit_pre_notifications pn
                 WHERE pn.payment_initiation_id = pi.id) AS n_pre_notifications,
              (SELECT count(*) FROM wsjrdp_direct_debit_pre_notifications pn
                 WHERE pn.payment_initiation_id = pi.id
                   AND pn.payment_status = 'xml_generated')
                 AS n_pre_notifications_xml,
              (SELECT count(*) FROM accounting_entries ae
                 WHERE ae.payment_initiation_id = pi.id) AS n_accounting_entries,
              COALESCE(
                (SELECT max(pmi.requested_collection_date)
                   FROM wsjrdp_direct_debit_payment_infos pmi
                   WHERE pmi.payment_initiation_id = pi.id),
                (SELECT max(pn.collection_date)
                   FROM wsjrdp_direct_debit_pre_notifications pn
                   WHERE pn.payment_initiation_id = pi.id)
              ) AS collection_date
            FROM wsjrdp_payment_initiations pi
            ORDER BY pi.id DESC""",
    )
    if not rows:
        _LOGGER.error("No payment initiations found in the database.")
        raise SystemExit(1)

    app_class = _make_selector_app_class()
    selected_id = app_class(rows).run()
    if selected_id is None:
        _LOGGER.info("Interactive selection cancelled - no payment initiation chosen.")
    else:
        _LOGGER.info("Selected payment_initiation_id=%s", selected_id)
    return selected_id


def _confirm(
    ctx: wsjrdp2027.WsjRdpContext, args, *, already_confirmed: bool = False
) -> bool:
    prompt = (
        f"Revert ALL of the above database changes for payment_initiation_id="
        f"{args.payment_initiation_id}?"
    )
    if ctx.is_production:
        # In production this is the single confirmation gate; it raises
        # SystemExit(0) when the user declines. Production always re-confirms,
        # even if the interactive preview was confirmed.
        ctx.require_approval_to_run_in_prod(prompt=prompt)
        return True
    if already_confirmed:
        _LOGGER.info("Confirmed in the interactive preview.")
        return True
    if args.yes:
        _LOGGER.info("Skipping interactive confirmation (--yes given, non-production).")
        return True
    return wsjrdp2027.console_confirm(prompt, default=False)


def main(argv=None) -> int:
    ctx = wsjrdp2027.WsjRdpContext(
        argv=argv,
        argument_parser=create_argument_parser(),
        __file__=__file__,
        # out_dir="data/revert_sepa_direct_debit_{{ filename_suffix }}",
    )
    args = ctx.parsed_args

    out_base = ctx.make_out_path("revert_sepa_direct_debit_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        if args.payment_initiation_id is None:
            args.payment_initiation_id = _select_payment_initiation_interactively(
                ro_conn
            )
            if args.payment_initiation_id is None:
                return 0

        plan = wsjrdp2027.RevertSepaDirectDebit.from_payment_initiation_id(
            ro_conn, pain_id=args.payment_initiation_id
        )
        confirmed_in_preview = plan.preview()

        if not plan.has_changes:
            return 0

        if not _confirm(ctx, args, already_confirmed=bool(confirmed_in_preview)):
            _LOGGER.info("Aborted: no confirmation given. No changes made.")
            return 0

        rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
        result = plan.apply(rw_conn)
        _LOGGER.info(
            "Applied revert: deleted %s accounting_entries, %s payment_infos;"
            " updated %s pre-notifications; payment_initiation_reset=%s",
            result.deleted_accounting_entries,
            result.deleted_payment_infos,
            result.updated_pre_notifications,
            result.payment_initiation_reset,
        )

        ctx.register_output_file(
            "Apply SQL", out_base.with_suffix(".apply.sql")
        ).write_text(result.apply_sql, encoding="utf-8")
        ctx.register_output_file(
            "Undo SQL", out_base.with_suffix(".undo.sql")
        ).write_text(result.revert_sql, encoding="utf-8")

        if ctx.dry_run or args.rollback_for_testing:
            reason = "--dry-run" if ctx.dry_run else "--rollback-for-testing"
            _LOGGER.warning("")
            _LOGGER.warning("ROLLBACK (%s given) - no changes committed", reason)
            _LOGGER.warning("")
            rw_conn.rollback()

    return 0


if __name__ == "__main__":
    sys.exit(main())
