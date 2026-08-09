from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import logging as _logging
import pathlib as _pathlib
import typing as _typing

import sepaxml as _sepaxml

from . import _pg, _report_tree, _types


if _typing.TYPE_CHECKING:
    import pandas as _pandas
    import psycopg.sql as _psycopg_sql


_LOGGER = _logging.getLogger(__name__)


CREDITOR_ID = "DE81WSJ00002017275"


WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG: _types.SepaDirectDebitConfig = {
    "name": "Ring deutscher Pfadfinder*innenverbände e.V.",
    "IBAN": "DE70830654080005498201",
    "BIC": "GENODEF1SLR",
    "creditor_id": CREDITOR_ID,
    "currency": "EUR",
    "address_as_single_line": "Chausseestraße 128/129, 10115 Berlin",
}

WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG: _types.SepaDirectDebitConfig = {
    "name": "Ring deutscher Pfadfinder*innenverbände e.V.",
    "IBAN": "DE13370601932001939044",
    "BIC": "GENODED1PAX",
    "creditor_id": CREDITOR_ID,
    "currency": "EUR",
    "address_as_single_line": "Chausseestraße 128/129, 10115 Berlin",
}


class SepaDirectDebitPayment(_typing.TypedDict, total=False):
    name: str
    IBAN: str
    BIC: str
    amount: int
    type: str
    collection_date: _datetime.date | _datetime.datetime | str
    mandate_id: str
    mandate_date: _datetime.date | _datetime.datetime | str
    description: str
    endtoend_id: str


class SepaDirectDebit:
    _num_payments: int

    def __init__(
        self, config: _types.SepaDirectDebitConfig, *, schema: str = "pain.008.001.02"
    ) -> None:
        raw_config: dict = config.copy()  # type: ignore
        raw_config.pop("address_as_single_line", None)  # not to be seen by sepaxml
        raw_config.setdefault("currency", "EUR")
        raw_config.setdefault("batch", True)
        for key in ["name"]:
            if key in raw_config:
                raw_config[key] = _german_transliterate(raw_config[key])

        self._dd = _sepaxml.SepaDD(raw_config, schema=schema, clean=True)
        self._num_payments = 0

    @property
    def num_payments(self) -> int:
        """Number of payments added to this SEPA direct debit."""
        return self._num_payments

    def add_payment(
        self, payment: SepaDirectDebitPayment, *, pedantic: bool = True
    ) -> SepaDirectDebitPayment:
        raw_payment: SepaDirectDebitPayment = payment.copy()
        raw_payment["amount"] = raw_payment.pop("amount")

        raw_payment["IBAN"] = raw_payment.get("IBAN", "").replace(" ", "").upper()

        # We assume that the BIC is not required (which it is not for
        # most if not all EUR SEPA direct debit payments) and hence
        # skip it as the quality of user entered BIC data is not good
        # enough to transmit it unless required.
        raw_payment.pop("BIC")

        for key in ["name", "description"]:
            if key in raw_payment:
                raw_payment[key] = _german_transliterate(raw_payment[key])

        self._dd.add_payment(raw_payment)
        self._num_payments += 1
        return raw_payment

    def add_payment_from_accounting_row(
        self, row: _pandas.Series, *, pedantic: bool = True
    ) -> SepaDirectDebitPayment:
        payment: SepaDirectDebitPayment = {
            "name": row["sepa_name"],
            "IBAN": row["sepa_iban"],
            "BIC": row["sepa_bic"],
            "amount": row["open_amount_cents"],
            "type": row.get("sepa_dd_sequence_type", "OOFF"),  # FRST,RCUR,OOFF,FNAL
            "collection_date": row["collection_date"],
            "mandate_id": row["sepa_mandate_id"],
            "mandate_date": row["sepa_mandate_date"],
            "description": row["sepa_dd_description"],
        }
        if endtoend_id := row.get("sepa_dd_endtoend_id"):
            payment["endtoend_id"] = endtoend_id
        return self.add_payment(payment, pedantic=pedantic)

    def export(self, *, pretty_print: bool = True) -> str:
        return self.export_bytes(pretty_print=pretty_print).decode("utf-8")

    def export_bytes(self, *, pretty_print: bool = True) -> bytes:
        return self._dd.export(validate=True, pretty_print=pretty_print)

    def export_file(self, path: str | _pathlib.Path, pretty_print: bool = True) -> None:
        xml_bytes = self.export_bytes(pretty_print=pretty_print)
        with open(path, "wb") as f:
            f.write(xml_bytes)


def _german_transliterate(s: str) -> str:
    import unicodedata

    s = unicodedata.normalize("NFC", s)

    replacements = {
        "Ä": "Ae",
        "Ö": "Oe",
        "Ü": "Ue",
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
    }
    for key, replacement in replacements.items():
        s = s.replace(key, replacement)
    return s


def write_accounting_dataframe_to_sepa_dd(
    df: _pandas.DataFrame,
    path: str | _pathlib.Path,
    *,
    config: _types.SepaDirectDebitConfig,
    pedantic: bool = True,
    print_progress_message=None,
) -> int:
    from . import _util

    if print_progress_message is None:
        print_progress_message = _util.print_progress_message

    dd = SepaDirectDebit(config)

    already_not_ok = len(df[df["payment_status"] != "ok"])

    idx: int
    df_len = len(df)
    written_payments = 0
    for i, (idx, row) in enumerate(df.iterrows()):  # type: ignore
        if row["payment_status"] != "ok":
            if row.get("open_amount_cents", 0) == 0:
                continue  # silently skip non-ok row with amount=0
            _LOGGER.debug(
                "[SDD] Skip non-ok row id=%s payment_status=%s payment_status_reason=%r",
                row["id"],
                row["payment_status"],
                row["payment_status_reason"],
            )
            continue

        skip_reasons = []
        if row.get("open_amount_cents", 0) == 0:
            skip_reasons.append("open_amount_cents = 0")
        if not row.get("payment_role", None):
            skip_reasons.append("payment_role IS NULL")
        if not row.get("sepa_iban", None):
            skip_reasons.append("sepa_iban IS NULL")

        if skip_reasons:
            skip_reason = ", ".join(skip_reasons)
            df.at[idx, "payment_status"] = "skipped"
            df.at[idx, "payment_status_reason"] = skip_reason
            _LOGGER.warning(
                "[SDD] Skip row id=%s payment_status_reason=%r", row["id"], skip_reason
            )
            continue

        progress_msg = (
            f"[SDD]"
            f" {row.get('id')} {row.get('short_full_name')}"
            f" sepa_name={row.get('sepa_name')!r}"
            f" {row.get('payment_role')}"
            f" open_amount_cents={row.get('open_amount_cents')}"
            f" {row.get('sepa_iban')}"
        )
        print_progress_message(i, df_len, progress_msg, logger=_LOGGER)
        try:
            dd.add_payment_from_accounting_row(row, pedantic=pedantic)
            written_payments += 1
        except (KeyError, ValueError) as exc:
            df.at[idx, "payment_status"] = "skipped"
            reason = f"{type(exc).__qualname__}: {exc}"
            df.at[idx, "payment_status_reason"] = reason
            _LOGGER.warning("[SDD] Caught exception: %s", reason)

    now_not_ok = len(df[df["payment_status"] != "ok"])
    _LOGGER.info("[SDD] Newly skipped rows: %s", now_not_ok - already_not_ok)
    _LOGGER.info("[SDD] Already not ok: %s", already_not_ok)
    _LOGGER.info("[SDD] Written payments: %s", written_payments)

    if dd.num_payments == 0:
        _LOGGER.warning("[SDD] No payments added to Direct Debit => No file written")
    else:
        _LOGGER.info("[SDD] Write %s", path)
        dd.export_file(path)

    return dd.num_payments


# =============================================================================
# Reverting a SEPA direct debit run (see accounting_tools/revert_sepa_direct_debit.py)
# =============================================================================

# The columns that the SEPA direct debit run stamps onto the payment initiation
# when it flips the row from 'planned' to 'xml_generated'. Reverting means
# setting 'status' back to 'planned' and clearing these.
_PAIN_STAMPED_COLS = [
    "message_identification",
    "number_of_transactions",
    "control_sum_cents",
    "initiating_party_name",
]


def _to_eur(cents) -> str:
    from . import _util

    if cents is None:
        return "-"
    return _util.format_cents_as_eur_de(int(cents), zero_cents=",00")


def _delete_by_ids_sql(table: str, ids: list[int]) -> _psycopg_sql.Composed | None:
    """``DELETE FROM <table> WHERE id = ANY(<ids>) RETURNING id`` or None.

    Only the ids are returned (for the deleted-row count); the undo-the-undo
    INSERTs are built from the full rows collected up front, not from here.
    """
    from psycopg import sql

    if not ids:
        return None
    return sql.SQL("DELETE FROM {table} WHERE id = ANY({ids}) RETURNING id").format(
        table=sql.Identifier(table), ids=sql.Literal(ids)
    )


def _update_pre_notifications_sql(
    ids: list[int],
) -> _psycopg_sql.Composed | None:
    """Restore payment_status and clear the payment-info link, guarded so rows
    whose status changed meanwhile are left alone."""
    from psycopg import sql

    if not ids:
        return None
    return sql.SQL(
        "UPDATE wsjrdp_direct_debit_pre_notifications"
        " SET payment_status = 'pre_notified', direct_debit_payment_info_id = NULL"
        " WHERE id = ANY({ids}) AND payment_status = 'xml_generated'"
        " RETURNING id"
    ).format(ids=sql.Literal(ids))


def _reset_payment_initiation_sql(pain_id: int) -> _psycopg_sql.Composed:
    """Unconditionally reset status to ``planned`` and clear stamped columns."""
    from psycopg import sql

    set_clauses: list[sql.Composable] = [sql.SQL("status = 'planned'")]
    set_clauses += [
        sql.SQL("{col} = NULL").format(col=sql.Identifier(col))
        for col in _PAIN_STAMPED_COLS
    ]
    return sql.SQL(
        "UPDATE wsjrdp_payment_initiations SET {sets} WHERE id = {id} RETURNING *"
    ).format(
        sets=sql.SQL(", ").join(set_clauses),
        id=sql.Literal(pain_id),
    )


def _insert_sql_for_rows(conn, table: str, rows: list[dict]) -> list[str]:
    """Render ``INSERT`` statements that would re-create *rows* (the full-column
    rows collected in :meth:`RevertSepaDirectDebit.from_payment_initiation_id`)."""
    stmts = []
    for row in rows:
        query = _pg.col_val_pairs_to_insert_sql_query(
            table, list(row.items()), returning=None
        )
        stmts.append(query.as_string(conn) + ";")
    return stmts


def _reverse_pre_notification_sql(conn, plan: RevertSepaDirectDebit) -> list[str]:
    """Render ``UPDATE`` statements restoring the pre-notifications that step 2
    changed (payment_status back to 'xml_generated', original payment-info id)."""
    import psycopg.sql as _psycopg_sql

    stmts = []
    for pn in plan.old_pre_notifications:
        query = _psycopg_sql.SQL(
            "UPDATE wsjrdp_direct_debit_pre_notifications"
            " SET payment_status = {payment_status},"
            " direct_debit_payment_info_id = {direct_debit_payment_info_id}"
            " WHERE id = {id};"
        ).format(**pn)
        stmts.append(query.as_string(conn))
    return stmts


def _reverse_payment_initiation_sql(conn, plan: RevertSepaDirectDebit) -> list[str]:
    """Render the ``UPDATE`` restoring the payment initiation's old values."""
    from psycopg import sql

    old = plan.old_payment_initiation
    set_clauses = [
        sql.SQL("status = {}").format(sql.Literal(plan.old_payment_initiation_status))
    ]
    set_clauses += [
        sql.SQL("{col} = {val}").format(
            col=sql.Identifier(col), val=sql.Literal(old[col])
        )
        for col in _PAIN_STAMPED_COLS
    ]
    query = sql.SQL(
        "UPDATE wsjrdp_payment_initiations SET {sets} WHERE id = {id}"
    ).format(
        sets=sql.SQL(", ").join(set_clauses),
        id=sql.Literal(plan.pain_id),
    )
    return [query.as_string(conn) + ";"]


@_dataclasses.dataclass(kw_only=True, frozen=True)
class RevertSepaDirectDebitResult:
    """Outcome of :meth:`RevertSepaDirectDebit.apply`.

    Bundles the statistics gathered while the revert ran with the SQL that would
    reinstate everything it changed.
    """

    pain_id: int
    """The reverted payment initiation row id."""

    deleted_accounting_entries: int
    """Number of ``accounting_entries`` rows deleted."""

    deleted_payment_infos: int
    """Number of ``wsjrdp_direct_debit_payment_infos`` rows deleted."""

    updated_pre_notifications: int
    """Number of pre-notifications set back to ``pre_notified`` and unlinked."""

    payment_initiation_reset: bool
    """Whether the ``wsjrdp_payment_initiations`` row was actually reset."""

    apply_sql: str
    """The SQL statements that were executed by :meth:`RevertSepaDirectDebit.apply`
    (the DELETEs and UPDATEs of the revert), in execution order."""

    revert_sql: str
    """SQL that would reinstate the deleted/changed rows in the DB (undo the
    undo): re-INSERTs of the deleted rows and reverse UPDATEs of the changed
    ones, in an FK-safe replay order."""


@_dataclasses.dataclass(kw_only=True, frozen=True)
class RevertSepaDirectDebit:
    """A collected, not-yet-executed revert of one SEPA direct debit run.

    The fields fall into two deliberately separated concerns:

    * **Execution** fields (no prefix) are the *only* data used to APPLY the
      revert in :meth:`apply`. The ``*_ids`` lists say which rows are affected,
      and the ``*_sql`` fields are the ready-to-run statements (each with a
      ``RETURNING`` clause) that :meth:`apply` merely executes. A ``*_sql`` field
      is ``None`` when that step has nothing to do (except
      ``reset_payment_initiation_sql``, which always runs).
    * **Preview** fields (``preview_*`` prefix) are used *only* to render a
      human-readable summary. They are never used to change the database, so they
      can hold as much descriptive detail as is useful without affecting what
      gets executed.
    """

    # --- Execution: which rows are affected ----------------------------------
    pain_id: int
    """Target payment initiation row id."""

    accounting_entry_ids: list[int]
    """Ids of the ``accounting_entries`` rows to delete."""

    payment_info_ids: list[int]
    """Ids of the ``wsjrdp_direct_debit_payment_infos`` rows to delete."""

    pre_notification_ids: list[int]
    """Ids of the pre-notifications (currently ``xml_generated``) whose
    ``payment_status`` is set to ``pre_notified`` and whose
    ``direct_debit_payment_info_id`` is cleared to NULL."""

    # --- Execution: the exact statements apply() runs (with RETURNING) -------
    delete_accounting_entries_sql: _psycopg_sql.Composed | None
    delete_payment_infos_sql: _psycopg_sql.Composed | None
    update_pre_notifications_sql: _psycopg_sql.Composed | None
    reset_payment_initiation_sql: _psycopg_sql.Composed
    """The payment initiation is always reset to ``planned`` (unconditionally)."""

    # --- Old state: rows as they are before the revert (for preview and for
    #     building the undo-the-undo SQL) ---------------------------------------
    old_payment_initiation: dict
    """Current ``wsjrdp_payment_initiations`` row (the old values used to build
    the reverse SQL for the reset; see :attr:`old_payment_initiation_status`)."""

    old_accounting_entries: list[dict]
    """``accounting_entries`` rows that will be deleted, with *all* columns so
    :meth:`apply` can rebuild them as INSERTs for the undo-the-undo SQL."""

    old_payment_infos: list[dict]
    """``wsjrdp_direct_debit_payment_infos`` rows that will be deleted, with
    *all* columns (used the same way as :attr:`old_accounting_entries`)."""

    old_pre_notifications: list[dict]
    """Pre-notification rows that will be restored/unlinked; includes their old
    ``direct_debit_payment_info_id`` so the reverse SQL can restore the link."""

    @classmethod
    def from_payment_initiation_id(
        cls,
        ro_conn: _pg.PgConnectionLike,
        *,
        pain_id: int,
    ) -> _typing.Self:
        """Collect the changes needed to revert SEPA DD payment initiation *pain_id*.

        Reads (read-only) the payment initiation row and its dependent
        ``accounting_entries``, ``wsjrdp_direct_debit_payment_infos`` and
        ``wsjrdp_direct_debit_pre_notifications`` rows, and builds the
        ready-to-run SQL statements (each with a ``RETURNING`` clause) that
        :meth:`apply` later executes. Nothing is written to the database by this
        method.

        Args:
            ro_conn: A connection-like object (``psycopg.Connection``,
                ``PsycopgClient`` or ``WsjRdpContext``); it is resolved to a
                read-only connection via ``_pg.to_connection``.
            pain_id: Id of the ``wsjrdp_payment_initiations`` row to revert.

        Returns:
            A populated, not-yet-executed plan instance.

        Raises:
            RuntimeError: If no ``wsjrdp_payment_initiations`` row with id
                *pain_id* exists.
            psycopg.Error: If resolving the connection or executing any of the
                SELECT queries fails (for example a lost connection or a query
                error).
        """
        ro_conn = _pg.to_connection(ro_conn, read_only=True)

        pain_rows = _pg.pg_select_dict_rows(
            ro_conn,
            t"""SELECT *
                FROM wsjrdp_payment_initiations
                WHERE id = {pain_id}""",
        )
        if not pain_rows:
            raise RuntimeError(
                f"No wsjrdp_payment_initiations row with id={pain_id} found."
            )
        pain_row = pain_rows[0]

        accounting_entries = _pg.pg_select_dict_rows(
            ro_conn,
            t"""SELECT *
                FROM accounting_entries
                WHERE payment_initiation_id = {pain_id}
                ORDER BY id""",
        )

        payment_infos = _pg.pg_select_dict_rows(
            ro_conn,
            t"""SELECT *
                FROM wsjrdp_direct_debit_payment_infos
                WHERE payment_initiation_id = {pain_id}
                ORDER BY id""",
        )

        pre_notifications = _pg.pg_select_dict_rows(
            ro_conn,
            t"""SELECT *
                FROM wsjrdp_direct_debit_pre_notifications
                WHERE payment_initiation_id = {pain_id}
                  AND payment_status = 'xml_generated'
                ORDER BY id""",
        )

        accounting_entry_ids = [r["id"] for r in accounting_entries]
        payment_info_ids = [r["id"] for r in payment_infos]
        pre_notification_ids = [r["id"] for r in pre_notifications]

        return cls(
            pain_id=pain_id,
            accounting_entry_ids=accounting_entry_ids,
            payment_info_ids=payment_info_ids,
            pre_notification_ids=pre_notification_ids,
            delete_accounting_entries_sql=_delete_by_ids_sql(
                "accounting_entries", accounting_entry_ids
            ),
            delete_payment_infos_sql=_delete_by_ids_sql(
                "wsjrdp_direct_debit_payment_infos", payment_info_ids
            ),
            update_pre_notifications_sql=_update_pre_notifications_sql(
                pre_notification_ids
            ),
            reset_payment_initiation_sql=_reset_payment_initiation_sql(pain_id),
            old_payment_initiation=pain_row,
            old_accounting_entries=accounting_entries,
            old_payment_infos=payment_infos,
            old_pre_notifications=pre_notifications,
        )

    @property
    def old_payment_initiation_status(self) -> str:
        """The payment initiation's ``status`` before the reset (the row is
        always reset to ``planned``, so this is what the reverse SQL restores)."""
        return self.old_payment_initiation["status"]

    @property
    def has_changes(self) -> bool:
        """True if applying the plan would change anything in the database.

        The payment initiation is always reset, so an initiation that is not yet
        back at ``planned`` already counts as a change on its own."""
        return bool(
            self.accounting_entry_ids
            or self.payment_info_ids
            or self.pre_notification_ids
            or self.old_payment_initiation_status != "planned"
        )

    def get_report_tree(self) -> _report_tree.ReportTree:
        """Return this revert plan as a generic :class:`~wsjrdp2027.ReportTree`.

        Each node is made of parts whose *last* part is the node's label (the line
        that owns the subtree). A step's earlier parts are the actual SQL statement
        (``kind="code"``); the root's earlier part is the current payment
        initiation row. The affected rows are leaf child nodes.
        """
        from . import _report_tree

        ReportNode = _report_tree.ReportNode
        ReportContent = _report_tree.ReportContent

        pain_id = self.pain_id
        pain_row = self.old_payment_initiation
        acc = self.old_accounting_entries
        pmt_infos = self.old_payment_infos
        pns = self.old_pre_notifications
        acc_sum = sum(int(r["amount_cents"] or 0) for r in acc)
        pmi_sum = sum(int(r["control_sum_cents"] or 0) for r in pmt_infos)
        pn_sum = sum(int(r["amount_cents"] or 0) for r in pns)

        control_sum = pain_row["control_sum_cents"]
        control_sum = int(control_sum) if control_sum is not None else None

        def sum_part(label: str, total: int, n_rows: int) -> ReportContent:
            text = f"{label}: {_to_eur(total)} ({n_rows} rows)"
            # Only warn when the payment initiation has a control sum and this
            # (non-empty) sum disagrees with it.
            if control_sum is not None and n_rows and total != control_sum:
                return ReportContent(
                    f"{text}  ⚠ does not match control_sum {_to_eur(control_sum)}",
                    kind="warning",
                )
            return ReportContent(text, kind="text")

        def sql_part(stmt) -> _report_tree.ReportContent:
            if stmt is None:
                return ReportContent("-- (no statement — nothing to do)", kind="text")
            return ReportContent(stmt.as_string(), kind="code", language="sql")

        info_node = _report_tree.ReportNode(
            f"status:                 {pain_row['status']}",
            f"message_identification: {pain_row['message_identification']}",
            f"number_of_transactions: {pain_row['number_of_transactions']}",
            f"control_sum_cents:      {pain_row['control_sum_cents']}"
            f" ({_to_eur(pain_row['control_sum_cents'])})",
            f"initiating_party_name:  {pain_row['initiating_party_name']}",
            sum_part("sum accounting_entries", acc_sum, len(acc)),
            sum_part("sum payment_infos     ", pmi_sum, len(pmt_infos)),
            sum_part("sum pre_notifications ", pn_sum, len(pns)),
        )
        step1 = ReportNode(
            "",
            f"(1) DELETE accounting_entries — {len(acc)} rows, SUM {_to_eur(acc_sum)}",
            sql_part(self.delete_accounting_entries_sql),
            children=tuple(
                ReportNode(
                    f"id={r['id']} subject_id={r['subject_id']} "
                    f"{_to_eur(r['amount_cents'])} | {r['description']}"
                )
                for r in acc
            ),
        )
        step2 = ReportNode(
            "",
            "(2) UPDATE wsjrdp_direct_debit_pre_notifications"
            " (payment_status -> 'pre_notified', direct_debit_payment_info_id ->"
            f" NULL) — {len(pns)} rows",
            sql_part(self.update_pre_notifications_sql),
            children=tuple(
                ReportNode(
                    f"id={r['id']} subject_id={r['subject_id']} "
                    f"payment_status={r['payment_status']} -> pre_notified; "
                    f"info_id {r['direct_debit_payment_info_id']} -> NULL"
                )
                for r in pns
            ),
        )
        step3 = ReportNode(
            "",
            f"(3) DELETE wsjrdp_direct_debit_payment_infos — {len(pmt_infos)} rows",
            sql_part(self.delete_payment_infos_sql),
            children=tuple(
                ReportNode(
                    f"id={r['id']} seq={r['debit_sequence_type']} "
                    f"n_tx={r['number_of_transactions']} "
                    f"sum={_to_eur(r['control_sum_cents'])} "
                    f"collect={r['requested_collection_date']}"
                )
                for r in pmt_infos
            ),
        )
        step4 = ReportNode(
            "",
            "(4) UPDATE wsjrdp_payment_initiations: reset status to 'planned'"
            " and clear the stamped columns",
            sql_part(self.reset_payment_initiation_sql),
            ReportContent(
                f"Note: current status {self.old_payment_initiation_status!r} is"
                " restored if this revert is itself reverted",
                kind="text",
            ),
        )
        children = [info_node, step1, step2, step3, step4]
        if not self.has_changes:
            children.append(ReportNode("Nothing to revert - no matching rows found."))
        root = ReportNode(
            f"REVERT SEPA Direct Debit XML export for payment_initiation_id = {pain_id}",
            children=children,
        )
        return _report_tree.ReportTree(root=root)

    def get_description(self) -> str:
        """Return the full plain-text description of this revert plan."""
        return self.get_report_tree().full_description()

    def preview(
        self,
        *,
        logger: _logging.Logger | _logging.LoggerAdapter | None = None,
        log_level: int = _logging.DEBUG,
        expand_below_children: int = 10,
    ) -> bool | None:
        """Show the collected plan to the user.

        The full description (:meth:`get_description`) is always logged at
        *log_level*. When stdout is an interactive console the plan is shown in a
        collapsible textual tree (nodes with fewer than *expand_below_children*
        children start expanded, larger ones collapsed; expand with Enter/Space);
        otherwise it is printed.

        Returns:
            In an interactive console, ``True`` if the user confirmed (``c``) or
            ``False`` if they left without confirming (``q`` / ``Ctrl-Q`` /
            ``Esc``); ``None`` when the plan was only printed (no interactive
            console).
        """
        import sys as _sys

        tree = self.get_report_tree()
        description = tree.full_description()
        (logger or _LOGGER).log(log_level, description)
        if _sys.stdout.isatty():
            return _report_tree.show_report_tree(
                tree,
                title="revert_sepa_direct_debit",
                subtitle=(
                    "Review the revert plan — Enter/Space expand/collapse,"
                    " c to confirm, q/Ctrl-Q/Esc to cancel"
                ),
                expand_below_children=expand_below_children,
                collapsible_root=False,
            )
        print(description)
        return None

    def apply(self, rw_conn: _pg.PgConnectionLike) -> RevertSepaDirectDebitResult:
        """Execute the collected statements against a read-write connection.

        The statements run in an order that respects the foreign keys (delete
        accounting entries, then restore/unlink pre-notifications, then delete
        payment infos, then reset the payment initiation). The changes are not
        committed here; the caller controls commit/rollback.

        Args:
            rw_conn: A connection-like object (``psycopg.Connection``,
                ``PsycopgClient`` or ``WsjRdpContext``); it is resolved to a
                read-write connection via ``_pg.to_connection``.

        Returns:
            A :class:`RevertSepaDirectDebitResult` with the counts of what
            changed, the SQL that was executed (``apply_sql``), and the SQL that
            would reinstate everything again (``revert_sql``, in an FK-safe
            replay order: re-INSERTs rebuilt from the full-column rows collected
            up front, reverse UPDATEs from the captured old values).

        Raises:
            psycopg.Error: If resolving the connection or executing any of the
                statements fails (for example a foreign key violation or a lost
                connection).
        """
        import psycopg.rows

        conn = _pg.to_connection(rw_conn, read_only=False)

        reinsert_payment_infos: list[str] = []
        reinsert_accounting_entries: list[str] = []
        reverse_pre_notifications: list[str] = []
        reverse_payment_initiation: list[str] = []
        executed_statements: list = []

        n_deleted_accounting_entries = 0
        n_restored_pre_notifications = 0
        n_deleted_payment_infos = 0
        payment_initiation_reset = False

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
            # (1) delete accounting entries (before payment infos: FK). The
            #     re-insert SQL is built from the full rows collected up front.
            if self.delete_accounting_entries_sql is not None:
                rows = cursor.execute(self.delete_accounting_entries_sql).fetchall()
                executed_statements.append(self.delete_accounting_entries_sql)
                n_deleted_accounting_entries = len(rows)
                _LOGGER.debug("Deleted %s accounting_entries", len(rows))
                reinsert_accounting_entries = _insert_sql_for_rows(
                    conn, "accounting_entries", self.old_accounting_entries
                )
            else:
                _LOGGER.debug("No accounting_entries to delete")

            # (2) restore + unlink pre-notifications (before deleting payment
            #     infos: the cleared direct_debit_payment_info_id FK would block
            #     the delete).
            if self.update_pre_notifications_sql is not None:
                rows = cursor.execute(self.update_pre_notifications_sql).fetchall()
                executed_statements.append(self.update_pre_notifications_sql)
                restored_ids = [r["id"] for r in rows]
                n_restored_pre_notifications = len(restored_ids)
                _LOGGER.debug(
                    "Updated %s pre-notifications"
                    " (payment_status -> 'pre_notified',"
                    " direct_debit_payment_info_id -> NULL)",
                    len(restored_ids),
                )
                reverse_pre_notifications = _reverse_pre_notification_sql(conn, self)
            else:
                _LOGGER.debug("No pre-notifications to restore/unlink")

            # (3) delete payment infos. The re-insert SQL is built from the full
            #     rows collected up front.
            if self.delete_payment_infos_sql is not None:
                rows = cursor.execute(self.delete_payment_infos_sql).fetchall()
                executed_statements.append(self.delete_payment_infos_sql)
                n_deleted_payment_infos = len(rows)
                _LOGGER.debug("Deleted %s wsjrdp_direct_debit_payment_infos", len(rows))
                reinsert_payment_infos = _insert_sql_for_rows(
                    conn,
                    "wsjrdp_direct_debit_payment_infos",
                    self.old_payment_infos,
                )
            else:
                _LOGGER.debug("No wsjrdp_direct_debit_payment_infos to delete")

            # (4) unconditionally reset the payment initiation row to 'planned'.
            rows = cursor.execute(self.reset_payment_initiation_sql).fetchall()
            executed_statements.append(self.reset_payment_initiation_sql)
            payment_initiation_reset = bool(rows)
            _LOGGER.debug(
                "Reset wsjrdp_payment_initiations id=%s to status 'planned' (was %r)",
                self.pain_id,
                self.old_payment_initiation_status,
            )
            if rows:
                reverse_payment_initiation = _reverse_payment_initiation_sql(conn, self)

        # Summarize the SQL that was executed (in execution order).
        apply_lines = [
            f"-- SQL executed to revert payment_initiation_id = {self.pain_id}",
            *(stmt.as_string(conn) + ";" for stmt in executed_statements),
        ]
        apply_sql = "\n".join(apply_lines) + "\n"
        _LOGGER.debug("Executed SQL:\n%s", apply_sql)

        # Assemble the undo-the-undo SQL in FK-safe replay order: payment infos
        # first, then accounting entries, then the pre-notification and
        # payment-initiation restores.
        revert_lines = [
            f"-- SQL to revert this revert of payment_initiation_id = {self.pain_id}",
            *reinsert_payment_infos,
            *reinsert_accounting_entries,
            *reverse_pre_notifications,
            *reverse_payment_initiation,
        ]
        revert_sql = "\n".join(revert_lines) + "\n"
        _LOGGER.debug("SQL to revert this revert:\n%s", revert_sql)
        return RevertSepaDirectDebitResult(
            pain_id=self.pain_id,
            deleted_accounting_entries=n_deleted_accounting_entries,
            deleted_payment_infos=n_deleted_payment_infos,
            updated_pre_notifications=n_restored_pre_notifications,
            payment_initiation_reset=payment_initiation_reset,
            apply_sql=apply_sql,
            revert_sql=revert_sql,
        )
