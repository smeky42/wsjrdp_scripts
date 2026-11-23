from __future__ import annotations

import collections.abc as _collections_abc
import contextlib as _contextlib
import dataclasses as _dataclasses
import datetime as _datetime
import logging as _logging
import pathlib as _pathlib
import typing as _typing

from . import _types, _util


if _typing.TYPE_CHECKING:
    import email.message as _email_message
    import email.policy as _email_policy

    import pandas as _pandas
    import psycopg as _psycopg

    from . import _context, _mail_client, _people_query
    from ._models import person as _person


__all__ = [
    "BatchConfig",
    "PreparedBatch",
    "PreparedEmailMessage",
]


_LOGGER = _logging.getLogger(__name__)


class _MissingType:
    pass


_MISSING = _MissingType()


@_dataclasses.dataclass(kw_only=True)
class PreparedEmailMessage:
    mailing_name: str
    message: _email_message.EmailMessage | None = None
    content: str | None = None
    person: _person.Person | None = None
    _row: _pandas.Series | None = None
    summary: str
    eml_name: str
    _eml: bytes | None = None
    # The log entry of this mail (``log_note`` of the batch): its item, the
    # data a script adds to it, and the id of its versions row once written.
    log_note_item: tuple[str, int] | None = None
    log_note_data: dict[str, _typing.Any]
    log_version_id: int | None = None

    def __init__(
        self,
        *,
        mailing_name: str,
        message: _email_message.EmailMessage | None = None,
        content: str | None = None,
        person: _person.Person | None = None,
        row: _pandas.Series | None = None,
        summary: str,
        eml_name: str | None = None,
        eml: bytes | None = None,
        log_note_item: tuple[str, int] | None = None,
        log_note_data: dict[str, _typing.Any] | None = None,
    ) -> None:
        self.mailing_name = mailing_name
        self.message = message
        self.content = content
        self.person = person
        self._row = row
        self.summary = summary
        self.eml_name = eml_name if eml_name else f"{self.mailing_name}.eml"
        self._eml = eml
        self.log_note_item = log_note_item
        self.log_note_data = dict(log_note_data or {})
        self.log_version_id = None

    @property
    def eml(self) -> bytes:
        if self._eml is None:
            if self.message is not None:
                self._eml = self.message.as_bytes()
            else:
                self._eml = b""
        return self._eml

    @eml.setter
    def eml(self, value: bytes) -> None:
        self._eml = value

    @property
    def row(self) -> _pandas.Series | None:
        if (row := self._row) is not None:
            return row
        elif (p := self.person) is not None:
            try:
                return p.row
            except Exception:
                return None
        else:
            return None


def _person_updates_for_display(
    row: _pandas.Series | _collections_abc.Mapping[str, _typing.Any],
    *,
    today: _datetime.date | None = None,
) -> dict[str, _typing.Any]:
    """The changes of one person (``row["person_changes"]``) as an update
    reads them: ``tag_list`` as ``add_tags`` / ``remove_tags``, the note of
    ``add_note``, the roles of the primary group that end, are deleted (only
    known with *today*) and are created as ``roles``
    (:meth:`RoleChange.for_display`), every other column as ``[old, new]``.

    >>> _person_updates_for_display({
    ...     "person_changes": {
    ...         "status": ["reviewed", "confirmed"],
    ...         "tag_list": [["A", "Warteliste"], ["A", "B"]],
    ...     },
    ...     "add_note": "Bestätigt",
    ... })
    {'add_note': 'Bestätigt', 'add_tags': ['B'], 'remove_tags': ['Warteliste'], 'status': ['reviewed', 'confirmed']}
    >>> _person_updates_for_display({"person_changes": {}, "add_note": None})
    {}
    """
    from . import _people

    changes = dict(row.get("person_changes") or {})
    updates: dict[str, _typing.Any] = {}
    if changes and (role_change := _people.role_change_for_row(row)):
        updates["roles"] = role_change.for_display(today=today)
    if note := _util.nan_to_none(row.get("add_note")):
        updates["add_note"] = note
    if (tag_list := changes.pop("tag_list", None)) is not None:
        old_tags, new_tags = (set(tags or ()) for tags in tag_list)
        if added := sorted(new_tags - old_tags):
            updates["add_tags"] = added
        if removed := sorted(old_tags - new_tags):
            updates["remove_tags"] = removed
    updates.update(changes)
    return dict(sorted(updates.items()))


@_dataclasses.dataclass(kw_only=True)
class PreparedBatch:
    name: str = "batch"
    df: _pandas.DataFrame
    unfiltered_df: _pandas.DataFrame | None = None
    messages: tuple[PreparedEmailMessage, ...] = ()
    action_arguments: dict = _dataclasses.field(default_factory=dict)
    updates: dict = _dataclasses.field(default_factory=dict)
    email_from: str = ""
    from_addr: str | None = None
    config_yaml: bytes | None
    out_dir: _pathlib.Path | None = None
    now: _datetime.datetime = _typing.cast(_datetime.datetime, None)
    dry_run: bool = False
    skip_email: bool = False
    skip_db_updates: bool = False
    results: dict = _dataclasses.field(default_factory=dict)
    person_updates_logged: bool = False
    # ``category`` and ``message`` of the log entry each mail gets (see
    # insert_log_notes); None for none.
    log_note: dict | None = None
    # The script named in the log entries; the running one by default.
    script: str | None = None
    eml_zip_name: str | None = None

    def __post_init__(self) -> None:
        import sys

        if self.now is None:
            self.now = _datetime.datetime.now().astimezone()
        if self.script is None:
            self.script = _pathlib.Path(sys.argv[0]).name or None

    def iter_people(self) -> _typing.Generator[_person.Person]:
        for msg in self.messages:
            if (p := msg.person) is not None:
                yield p

    def log_person_updates(
        self,
        *,
        logger: _logging.Logger | _logging.LoggerAdapter | None = None,
        level: int = _logging.INFO,
    ) -> None:
        """Logs one block per person with every change the DB update will
        write (see :func:`person_updates_for_display`), the derived ones
        included."""
        import pprint

        logger = logger or _LOGGER
        if "person_changes" not in self.df:
            logger.log(level, "Updates: none")
            return
        for _, row in self.df.iterrows():
            name = row.get("id_and_name", row["id"])
            skip_db_updates = bool(_util.nan_to_none(row.get("skip_db_updates")))
            skip_db_suffix = " [skip_db_updates]" if skip_db_updates else ""
            updates = _person_updates_for_display(row, today=self.now.date())
            if updates:
                logger.log(
                    level,
                    "Updates for %s%s:\n%s",
                    name,
                    skip_db_suffix,
                    pprint.pformat(updates),
                )
            else:
                logger.log(level, "Updates for %s%s: none", name, skip_db_suffix)
        self.person_updates_logged = True

    def _get_out_dir(self, out_dir: _pathlib.Path | str | None = None) -> _pathlib.Path:
        if out_dir:
            return _pathlib.Path(out_dir)
        elif self.out_dir:
            return self.out_dir
        else:
            return _pathlib.Path("data") / "batch"

    def write_data(
        self,
        *,
        out_dir: _pathlib.Path | None = None,
        zip_eml: bool | None = None,
        silent_skip_email: bool | None = None,
    ) -> _pathlib.Path:
        from . import _people

        if zip_eml is None:
            zip_eml = True
        out_dir = self._get_out_dir(out_dir=out_dir)

        self.__collect_default_results()

        out_dir.mkdir(exist_ok=True, parents=True)
        _LOGGER.info("  batch output directory: %s", out_dir)

        xlsx_path = out_dir / f"{self.name}.xlsx"
        unfiltered_xlsx_path = out_dir / f"{self.name}.unfiltered.xlsx"
        yml_path = out_dir / f"{self.name}.yml"

        self.__write_eml(
            out_dir=out_dir, zip_eml=zip_eml, silent_skip_email=silent_skip_email
        )

        _people.write_people_dataframe_to_xlsx(
            self.df, xlsx_path, log_level=_logging.DEBUG
        )
        _LOGGER.info("  wrote df xlsx %s", xlsx_path)

        if self.unfiltered_df is not None and self.unfiltered_df is not self.df:
            _people.write_people_dataframe_to_xlsx(
                self.unfiltered_df, unfiltered_xlsx_path, log_level=_logging.DEBUG
            )
            _LOGGER.info("  wrote unfiltered_df xlsx %s", unfiltered_xlsx_path)

        if self.config_yaml:
            with open(yml_path, "wb") as f:
                f.write(self.config_yaml)
            _LOGGER.info("  wrote yml %s", yml_path)

        return out_dir

    def __collect_default_results(self):
        if self.unfiltered_df is not None:
            unfiltered_ids_set = frozenset(self.unfiltered_df["id"])
        else:
            unfiltered_ids_set = frozenset()
        ids_set = frozenset(self.df[self.df["skip_db_updates"] == False]["id"])
        email_only_ids_set = frozenset(
            self.df[self.df["skip_db_updates"] != False]["id"]
        )
        self.results["ids"] = sorted(ids_set)
        self.results["email_only_ids"] = sorted(email_only_ids_set)
        skipped_ids_set = unfiltered_ids_set - ids_set - email_only_ids_set
        self.results["skipped_ids"] = sorted(skipped_ids_set)

    def __write_eml(
        self,
        *,
        out_dir: _pathlib.Path,
        zip_eml: bool,
        silent_skip_email: bool | None = None,
    ) -> None:
        import io
        import zipfile

        skipped_messages = [pm for pm in self.messages if not pm.message]
        prepared_messages = [pm for pm in self.messages if pm.message]
        if not silent_skip_email:
            for prep_msg in skipped_messages:
                _LOGGER.debug("  skip empty email message - %s", prep_msg.summary)
        if not prepared_messages:
            if not silent_skip_email:
                _LOGGER.info("  only empty messages - no eml output")
            return

        if zip_eml:
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w") as zf:
                for prep_msg in prepared_messages:
                    zf.writestr(prep_msg.eml_name, data=prep_msg.eml)
            eml_zip_path = out_dir / f"{self.name}.zip"
            with open(eml_zip_path, "wb") as f:
                f.write(zip_buf.getvalue())
            self.eml_zip_name = eml_zip_path.name
            _LOGGER.info("  wrote eml zip %s", eml_zip_path)
        else:
            for prep_msg in prepared_messages:
                eml_path = out_dir / prep_msg.eml_name
                eml_path.write_bytes(prep_msg.eml)
                _LOGGER.info("  wrote eml %s", eml_path)

    def write_results(
        self, *, out_dir: _pathlib.Path | None = None, json_indent: int | None = None
    ) -> None:
        import json

        out_dir = self._get_out_dir(out_dir=out_dir)
        json_path = out_dir / f"{self.name}.json"

        with open(json_path, "w") as f:
            json_d = {"results": self.results}
            json.dump(json_d, f, indent=json_indent)
        _LOGGER.info("  wrote json %s", json_path)

    def get_skip_email_reasons(self, *, dry_run: bool | None = None) -> list[str]:
        reasons = []
        if dry_run:
            reasons.append("dry_run")
        if self.skip_email:
            reasons.append("skip_email=True")
        if all(m.message is None for m in self.messages):
            reasons.append("no message")
        return reasons

    @property
    def writes_log_notes(self) -> bool:
        """Whether the mails get log entries: with ``log_note``, unless
        nothing is sent or nothing written to the DB."""
        return bool(self.log_note) and not (
            self.dry_run or self.skip_email or self.skip_db_updates
        )

    def insert_log_notes(self, conn: _psycopg.Connection, /) -> list[int]:
        """INSERTs the log entry of every mail, ``email_status`` ``prepared``.

        One entry per mail with a message: a note (``log_note``) in the log
        of the person, whose item is ``log_note_item`` of the message (the
        person by default). Its data: the script, run and batch, what the
        script put into ``log_note_data`` and what the mail says
        (:func:`wsjrdp2027._mail_log.prepared_data`). :meth:`send` adds
        the outcome. Never commits: the entries belong to the transaction
        of the change they log. Returns the ids of the versions rows."""
        from . import _log_entry, _mail_log

        if not self.writes_log_notes:
            _LOGGER.info("Skip log entries of the mails")
            return []
        assert self.log_note is not None
        prepared = [pm for pm in self.messages if pm.message and pm.person is not None]
        entries = []
        for prep_msg in prepared:
            assert prep_msg.message is not None and prep_msg.person is not None
            data = {
                "script": self.script,
                "run_started_at": self.now,
                "batch_name": self.name,
                **prep_msg.log_note_data,
                **_mail_log.prepared_data(
                    prep_msg.message,
                    eml=prep_msg.eml,
                    eml_name=prep_msg.eml_name,
                    eml_zip_name=self.eml_zip_name,
                    envelope_from=self.from_addr,
                ),
            }
            item = (
                {
                    "item_type": prep_msg.log_note_item[0],
                    "item_id": prep_msg.log_note_item[1],
                }
                if prep_msg.log_note_item
                else {}
            )
            entries.append(
                _log_entry.LogEntry.note(
                    main_id=int(prep_msg.person.id),
                    category=self.log_note["category"],
                    message=self._log_note_message(prep_msg, _mail_log.PREPARED, data),
                    data=data,
                    **item,
                )
            )
        ids = _log_entry.insert_log_entries(
            conn, entries, now=_datetime.datetime.now().astimezone()
        )
        for prep_msg, version_id in zip(prepared, ids, strict=True):
            prep_msg.log_version_id = version_id
        _LOGGER.info("  wrote %s log entries (email_status: prepared)", len(ids))
        return ids

    def _log_note_message(
        self, prep_msg: PreparedEmailMessage, status: str, data: dict
    ) -> str:
        from . import _mail_log

        assert self.log_note is not None
        person = prep_msg.person
        return " ".join(
            _util.render_template(
                self.log_note["message"],
                {
                    "p": person,
                    "person": person,
                    "row": person,
                    "msg": prep_msg.message,
                    "data": data,
                    "email_status": status,
                    "email_status_text": _mail_log.status_text(status),
                },
            ).split()
        )

    def send(
        self,
        mail_client: _mail_client.MailClient,
        /,
        *,
        log_conn: _psycopg.Connection | None = None,
        log_flush_every: int = 100,
    ) -> None:
        """Sends the mails. A mail with a log entry (:meth:`insert_log_notes`)
        gets its outcome added to it, through ``log_conn``: in blocks of
        ``log_flush_every`` mails, each block committed, the last one also
        when sending fails."""
        if log_conn is None and any(m.log_version_id for m in self.messages):
            raise RuntimeError("send: log entries written, but no log_conn given")
        pending: list[dict] = []
        try:
            self._send(
                mail_client,
                pending=pending,
                log_conn=log_conn,
                flush_every=log_flush_every,
            )
        finally:
            if log_conn is not None:
                self._flush_log_updates(log_conn, pending)

    def _flush_log_updates(
        self, conn: _psycopg.Connection, pending: list[dict]
    ) -> None:
        from . import _pg

        if not pending:
            return
        _pg.pg_table_updatemany(conn, "versions", pending, touch=False)
        conn.commit()
        _LOGGER.debug("  updated %s log entries", len(pending))
        pending.clear()

    def _log_update(
        self, prep_msg: PreparedEmailMessage, sent: dict, pending: list[dict]
    ) -> None:
        if prep_msg.log_version_id is None:
            return
        status = sent["email_status"]
        pending.append(
            {
                "id": prep_msg.log_version_id,
                "wsjrdp_data": {
                    **sent,
                    "message": self._log_note_message(prep_msg, status, sent),
                },
            }
        )

    def _send(
        self,
        mail_client: _mail_client.MailClient,
        /,
        *,
        pending: list[dict],
        log_conn: _psycopg.Connection | None,
        flush_every: int,
    ) -> None:
        import pprint
        import textwrap

        from . import _mail_client as _mail_client_mod, _mail_log

        num_messages = len(self.messages)
        if self.dry_run:
            if self.skip_email:
                _LOGGER.info(
                    "Skip sending %s messages (dry_run is True and skip_email is True)",
                    num_messages,
                )
            else:
                _LOGGER.info("Skip sending %s messages (dry_run is True)", num_messages)
            return
        elif self.skip_email:
            _LOGGER.info("Skip sending %s messages (skip_email is True)", num_messages)
            return
        for i, prep_msg in enumerate(self.messages, start=1):
            pcnt = (i / num_messages) * 100.0
            _LOGGER.info("%s %s", f"{i}/{num_messages} ({pcnt:.1f}%)", prep_msg.summary)
            if prep_msg.row is not None:
                row_d = dict(prep_msg.row)
                row_d.pop("person_dict", None)
                row_d.pop("primary_group_roles", None)
                _LOGGER.debug(
                    "  person:\n%s", textwrap.indent(pprint.pformat(row_d), "   | ")
                )
            if prep_msg.message is None:
                _LOGGER.debug("  Skip: No actual email message")
                continue
            if not _mail_client_mod.get_to_addrs(prep_msg.message):
                _LOGGER.info("  Skip: No recipients")
                self._log_update(
                    prep_msg, _mail_log.sent_data(_mail_log.NO_RECIPIENTS), pending
                )
                continue
            try:
                smtp_result, imap_result = mail_client.send_message(
                    prep_msg.message, from_addr=self.from_addr
                )
            except Exception as exc:
                _LOGGER.error("  Exception raised during email sending: %s", str(exc))
                self._log_update(
                    prep_msg,
                    _mail_log.sent_data(
                        _mail_log.FAILED, error=f"{type(exc).__name__}: {exc}"
                    ),
                    pending,
                )
                raise
            sent = _mail_log.sent_data(
                _mail_log.SENT,
                sent_at=_datetime.datetime.now().astimezone(),
                smtp_refused=smtp_result,
                imap_mailbox=_sent_mailbox_name(mail_client) if imap_result else None,
                imap_result=imap_result,
            )
            self._log_update(prep_msg, sent, pending)
            if log_conn is not None and len(pending) >= flush_every:
                self._flush_log_updates(log_conn, pending)


def _normalize_log_note(
    log_note: _collections_abc.Mapping | None,
) -> dict | None:
    """``log_note`` of a batch: ``category`` and ``message`` (a template
    with ``row``, ``msg``, ``data``, ``email_status`` and
    ``email_status_text``), both required.

    >>> _normalize_log_note({"category": "c", "message": "m"})
    {'category': 'c', 'message': 'm'}
    >>> _normalize_log_note(None) is None
    True
    >>> _normalize_log_note({"category": "c"})
    Traceback (most recent call last):
    ...
    ValueError: log_note needs 'category' and 'message', got ['category']
    """
    if not log_note:
        return None
    log_note = dict(log_note)
    if not (log_note.get("category") and log_note.get("message")) or set(log_note) - {
        "category",
        "message",
    }:
        raise ValueError(
            f"log_note needs 'category' and 'message', got {sorted(log_note)}"
        )
    return log_note


def _sent_mailbox_name(mail_client: _mail_client.MailClient) -> str | None:
    try:
        mailbox = mail_client.get_imap_sent_mailbox()
    except Exception:
        return None
    return mailbox[0] if mailbox else None


_DEFAULT_SUMMARY = "{{ row.role_id_name }}; status: {{ row.status }}{% if msg %}; To: {{ msg.to }}; Cc: {{ msg.cc }}{% if msg.bcc %}; Bcc: {{msg.bcc }}{% endif %}{% endif %}"


@_dataclasses.dataclass(kw_only=True)
class BatchConfig:
    query: _people_query.PeopleQuery
    email_subject: str = ""
    email_from: str = ""
    email_reply_to: list[str] | None = None
    extra_email_to: list[str] | None = None
    extra_email_cc: list[str] | None = None
    extra_email_bcc: list[str] | None = None
    from_addr: str | None = None
    signature: str = ""
    content: str | None = None
    content_file: _pathlib.Path | None = None
    html_content: str | None = None
    html_content_file: _pathlib.Path | None = None
    name: str = "batch"
    summary: str = _DEFAULT_SUMMARY
    jinja_extra_globals: dict | None = None
    action_arguments: dict
    updates: dict
    log_note: dict | None = None
    dry_run: bool = False
    skip_email: bool
    skip_db_updates: bool | None = None
    raw_yaml: bytes | None = None
    base_dir: _pathlib.Path | None = None
    _effective_base_dir: _pathlib.Path

    def __init__(
        self,
        /,
        *,
        query: _people_query.PeopleQuery | dict | None = None,
        where: _people_query.PeopleWhere | dict | str | None = None,
        email_subject: str | None = None,
        from_addr: str | None = None,
        email_from: str | None = None,
        email_reply_to: str | None = None,
        extra_email_to: _collections_abc.Iterable[str] | str | None = None,
        extra_email_cc: _collections_abc.Iterable[str] | str | None = None,
        extra_email_bcc: _collections_abc.Iterable[str] | str | None = None,
        signature: str | None = None,
        content: str | None = None,
        content_file: _pathlib.Path | str | None = None,
        html_content: str | None = None,
        html_content_file: _pathlib.Path | str | None = None,
        name: str | None = None,
        summary: str | None = None,
        jinja_extra_globals: dict | None = None,
        action_arguments: dict | None = None,
        updates: _collections_abc.Mapping | None = None,
        log_note: _collections_abc.Mapping | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
        raw_yaml: bytes | None = None,
        base_dir: _pathlib.Path | None = None,
        effective_base_dir: _pathlib.Path | None = None,
    ) -> None:
        import email.utils

        from . import _util

        if effective_base_dir:
            self._effective_base_dir = _pathlib.Path(effective_base_dir).resolve()
            self.base_dir = _pathlib.Path(base_dir) if base_dir else None
        elif base_dir:
            self.base_dir = _pathlib.Path(base_dir)
            self._effective_base_dir = self.base_dir.resolve()
        else:
            self._effective_base_dir = _pathlib.Path.cwd()
            self.base_dir = None

        def normalize_path(p: _pathlib.Path | str) -> _pathlib.Path:
            p = _pathlib.Path(p)
            if p.is_absolute():
                return p
            else:
                return (self._effective_base_dir / p).relative_to(
                    self._effective_base_dir, walk_up=True
                )

        self.name = name or "batch"
        self.email_subject = email_subject or ""
        self.query = _normalize_query_where(query, where, logger=None)
        if from_addr:
            from_addr = email.utils.parseaddr(from_addr)[1]
            self.from_addr = from_addr
        else:
            self.from_addr = "anmeldung@worldscoutjamboree.de"
        self.email_from = email_from or self.from_addr
        self.email_reply_to = _util.to_str_list_or_none(email_reply_to)
        self.extra_email_to = _util.to_str_list_or_none(extra_email_to)
        self.extra_email_cc = _util.to_str_list_or_none(extra_email_cc)
        self.extra_email_bcc = _util.to_str_list_or_none(extra_email_bcc)
        self.signature = signature or ""
        self.content = content or None
        self.html_content = html_content or None
        if content and content_file:
            raise RuntimeError("Only one of 'content' and 'content_file' allowed")
        if html_content and html_content_file:
            raise RuntimeError(
                "Only one of 'html_content' and 'html_content_file' allowed"
            )
        if content_file:
            self.content_file = normalize_path(content_file)
            self.content = self._slurp(content_file)
        else:
            self.content_file = None
        if html_content_file:
            self.html_content_file = normalize_path(html_content_file)
            self.html_content = self._slurp(self.html_content_file)
        elif html_content and html_content.endswith(".html"):
            self.html_content_file = normalize_path(html_content)
            self.html_content = self._slurp(self.html_content_file)
        else:
            self.html_content_file = None

        self.summary = summary or _DEFAULT_SUMMARY
        self.jinja_extra_globals = jinja_extra_globals
        self.action_arguments = action_arguments if action_arguments is not None else {}
        self.dry_run = dry_run if dry_run is not None else False
        self.skip_db_updates = skip_db_updates if skip_db_updates is not None else False

        if self.content is None and self.html_content is None and skip_email is None:
            self.skip_email = True
        elif skip_email is None:
            self.skip_email = False
        else:
            self.skip_email = skip_email

        updates = dict(**updates) if updates else {}
        for key in ["add_tags", "remove_tags", "new_primary_group_role_types"]:
            if key not in updates:
                continue
            val = _util.to_str_list(updates.get(key)) or None
            if val is None:
                updates.pop(key, None)
            else:
                updates[key] = val
        self.updates = updates
        self.log_note = _normalize_log_note(log_note)

        if raw_yaml is None:
            self.raw_yaml = self.__to_yaml__().encode("utf-8")
        else:
            self.raw_yaml = raw_yaml

    @classmethod
    def from_dict(
        cls,
        config: _collections_abc.Mapping | None = None,
        /,
        *,
        name: str | None = None,
        query: _people_query.PeopleQuery | dict | None = None,
        where: _people_query.PeopleWhere | dict | str | None = None,
        jinja_extra_globals: dict | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
        path: _pathlib.Path | str | None = None,
        effective_base_dir: _pathlib.Path | str | None = None,
        raw_yaml: bytes | str | None = None,
        updates: _collections_abc.Mapping | None = None,
    ) -> _typing.Self:
        from . import _people_query

        config = dict(config.items()) if config else {}
        path = _pathlib.Path(path) if path else None

        _INVALID_KEYS = ["raw_yaml", "effective_base_dir"]
        try:
            invalid_keys = sorted(k for k in _INVALID_KEYS if k in config)
            if invalid_keys:
                raise ValueError(
                    f"Parameter(s) {', '.join(repr(k) for k in invalid_keys)} not allowed in {path}"
                )
        except Exception as exc:
            _LOGGER.exception("Failed to read %s: %s", str(path), str(exc))
            raise

        missing = object()
        try:
            if query is None:
                query = _people_query.PeopleQuery.normalize(config.get("query"))
                if where is not None:
                    query = query.replace(
                        where=_people_query.PeopleWhere.normalize(where)
                    )
            else:
                query = _normalize_query_where_or_none(query, where, logger=None)
            if query:
                config["query"] = query
            if updates is not None:
                config["updates"] = updates
            if jinja_extra_globals is not None:
                config["jinja_extra_globals"] = jinja_extra_globals
            if name:
                config["name"] = name
            elif not config.get("name") and path and path.stem:
                config["name"] = path.stem
            if raw_yaml:
                if isinstance(raw_yaml, bytes):
                    config["raw_yaml"] = raw_yaml
                else:
                    config["raw_yaml"] = raw_yaml.encode("utf-8")
            if effective_base_dir:
                effective_base_dir = _pathlib.Path(effective_base_dir).resolve()
            elif "base_dir" in config:
                effective_base_dir = _pathlib.Path(config["base_dir"]).resolve()
            elif path:
                effective_base_dir = path.parent
            else:
                effective_base_dir = None
            if dry_run is not None:
                config["dry_run"] = dry_run
            if skip_email is not None:
                config["skip_email"] = skip_email
            if skip_db_updates is not None:
                config["skip_db_updates"] = skip_db_updates
            self = cls(**config, effective_base_dir=effective_base_dir)
            return self
        except Exception as exc:
            _LOGGER.exception("Failed to parse yaml %s: %s", path, str(exc))
            raise

    @classmethod
    def from_yaml_string(
        cls,
        raw_yaml: bytes | str,
        /,
        *,
        name: str | None = None,
        query: _people_query.PeopleQuery | dict | None = None,
        where: _people_query.PeopleWhere | dict | str | None = None,
        jinja_extra_globals: dict | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
        path: _pathlib.Path | str | None = None,
        effective_base_dir: _pathlib.Path | str | None = None,
    ) -> _typing.Self:
        import yaml as _yaml

        try:
            config = _yaml.load(raw_yaml, Loader=_yaml.FullLoader)
        except Exception as exc:
            _LOGGER.exception("Failed to read %s: %s", str(path), str(exc))
            raise

        return cls.from_dict(
            config,
            name=name,
            query=query,
            where=where,
            jinja_extra_globals=jinja_extra_globals,
            dry_run=dry_run,
            skip_email=skip_email,
            skip_db_updates=skip_db_updates,
            path=path,
            effective_base_dir=effective_base_dir,
            raw_yaml=raw_yaml,
        )

    @classmethod
    def from_yaml(
        cls,
        path: str | _pathlib.Path,
        /,
        *,
        name: str | None = None,
        query: _people_query.PeopleQuery | dict | None = None,
        where: _people_query.PeopleWhere | dict | str | None = None,
        jinja_extra_globals: dict | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
        effective_base_dir: _pathlib.Path | str | None = None,
    ) -> _typing.Self:
        path = _pathlib.Path(path)
        _LOGGER.info("Read batch config %s", str(path))
        try:
            with open(path, "r", encoding="utf-8", newline="\n") as f:
                raw_yaml = f.read()
        except Exception as exc:
            _LOGGER.exception("Failed to read %s: %s", str(path), str(exc))
            raise

        return cls.from_yaml_string(
            raw_yaml,
            name=name,
            query=query,
            where=where,
            jinja_extra_globals=jinja_extra_globals,
            dry_run=dry_run,
            skip_email=skip_email,
            skip_db_updates=skip_db_updates,
            path=path,
            effective_base_dir=effective_base_dir,
        )

    def copy(self) -> _typing.Self:
        import copy

        return copy.deepcopy(self)

    def replace(
        self,
        *,
        name: str | None = None,
        query: _people_query.PeopleQuery | None = None,
        jinja_extra_globals: dict | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
    ) -> _typing.Self:
        import copy

        obj = copy.deepcopy(self)
        if name is not None:
            obj.name = name
        if query is not None:
            obj.query = query
        if jinja_extra_globals is not None:
            obj.jinja_extra_globals = jinja_extra_globals
        if dry_run is not None:
            obj.dry_run = dry_run
        if skip_email is not None:
            obj.skip_email = skip_email
        if skip_db_updates is not None:
            obj.skip_db_updates = skip_db_updates
        return obj

    def extend_extra_email_bcc(
        self, *args: str | _typing.Iterable[str] | None
    ) -> list[str] | None:
        from . import _util

        self.extra_email_bcc = _util.to_str_list_or_none(self.extra_email_bcc, *args)
        return self.extra_email_bcc

    def __to_yaml__(
        self,
        content: str | None | _MissingType = _MISSING,
        signature: str | None | _MissingType = _MISSING,
    ) -> str:
        from . import _util

        if content is _MISSING:
            content = self.content
        if signature is _MISSING:
            signature = self.signature

        d = {
            "dry_run": self.dry_run,
            "skip_db_updates": self.skip_db_updates,
            "skip_email": self.skip_email,
            "action_arguments": (self.action_arguments or None),
            "updates": (self.updates or None),
            "log_note": self.log_note,
            "query": (self.query.__to_dict__() if self.query is not None else None),
            "summary": self.summary,
            "email_subject": self.email_subject,
            "email_from": self.email_from,
            "email_reply_to": self.email_reply_to,
            "extra_email_to": self.extra_email_to,
            "extra_email_cc": self.extra_email_cc,
            "extra_email_bcc": self.extra_email_bcc,
            "content": (None if self.content_file else content),
            "html_content": (None if self.html_content_file else self.html_content),
            "signature": signature,
            **{
                k: str(v)
                for k in ["base_dir", "content_file", "html_content_file"]
                if (v := getattr(self, k, None))
            },
        }
        d = {k: v for k, v in d.items() if v is not None}

        return _util.to_yaml_str(d)

    def _slurp(
        self,
        path: _pathlib.Path | str,
        /,
        *,
        encoding: str = "utf-8",
        newline: str = "\n",
    ) -> str:
        """Slurp *path* and return string."""
        real_path = self._effective_base_dir / _pathlib.Path(path)
        with open(real_path, encoding=encoding, newline=newline) as f:
            return f.read()

    def update_raw_yaml(self, raw_yaml: bytes | str | None = None) -> None:
        if raw_yaml is None:
            raw_yaml = self.__to_yaml__().encode("utf-8")
        elif isinstance(raw_yaml, str):
            raw_yaml = raw_yaml.encode("utf-8")
        self.raw_yaml = raw_yaml

    @property
    def person_updates(self) -> dict:
        from . import _person_pg

        keys_set = set(self.updates) & _person_pg.VALID_PERSON_UPDATE_KEYS
        return {k: v for k, v in self.updates.items() if k in keys_set}

    def update_dataframe_for_updates(
        self,
        df: _pandas.DataFrame,
        /,
        *,
        inplace: bool = True,
        now: _datetime.datetime | _datetime.date | str | float | None = None,
    ) -> _pandas.DataFrame:
        from . import _people

        if not inplace:
            df = df.copy(deep=True)

        _people.update_dataframe_for_updates(df, updates=self.person_updates, now=now)
        return df

    def load_people_dataframe(
        self,
        *,
        ctx: _context.WsjRdpContext,
        conn: _psycopg.Connection | None = None,
        collection_date: _datetime.date | str | None = None,
        extra_static_df_cols: dict[str, _typing.Any] | None = None,
        extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
        log_resulting_data_frame: bool | None = None,
        limit: int | None | _types.MissingType = _types.MISSING,
        now: _datetime.datetime
        | _datetime.date
        | str
        | float
        | None
        | _types.MissingType = _types.MISSING,
    ) -> _pandas.DataFrame:
        import textwrap

        import pandas as _pandas

        from . import _people, _people_query, _util

        limit = _util.coalesce_missing(limit, self.query.limit)
        now = _util.coalesce_missing(collection_date, self.query.collection_date)

        query = self.query.replace(
            limit=limit,
            collection_date=_util.to_date_or_none(collection_date),
            now=_util.to_datetime_or_none(now),
        )
        _LOGGER.debug("BatchConfig.load_people_dataframe")
        _LOGGER.debug("  collection_date: %s", query.collection_date)
        _LOGGER.debug("  now: %s", query.now)

        with _contextlib.ExitStack() as exit_stack:
            if conn is None:
                conn = exit_stack.enter_context(ctx.psycopg_connect())

            df = _people.load_people_dataframe(
                conn,
                query=query,
                extra_static_df_cols=extra_static_df_cols,
                extra_mailing_bcc=extra_mailing_bcc,
                log_resulting_data_frame=False,
            )
            if self.query.email_only_where:
                email_only_where = self.query.email_only_where
                exclude_found_ids = _people_query.PeopleWhere(exclude_id=list(df["id"]))
                email_only_query = query.replace(
                    where=_people_query.PeopleWhere(
                        and_=[email_only_where, exclude_found_ids]
                    )
                )
                email_only_query.email_only_where = None
                df2 = _people.load_people_dataframe(
                    conn,
                    query=email_only_query,
                    extra_static_df_cols=extra_static_df_cols,
                    extra_mailing_bcc=extra_mailing_bcc,
                    log_resulting_data_frame=False,
                    skip_db_updates=True,
                )
                if not df2.empty:
                    self.__mark_df_for_skip_db_updates(df2)
                    df = _pandas.concat(
                        [df, df2], axis=0, ignore_index=True, sort=False
                    )

        if log_resulting_data_frame or (log_resulting_data_frame is None):
            _LOGGER.info(
                "Resulting pandas DataFrame:\n%s", textwrap.indent(str(df), "  ")
            )

        return df

    @staticmethod
    def __mark_df_for_skip_db_updates(df: _pandas.DataFrame) -> None:
        from ._payment import _skip_payment

        for idx, row in df.iterrows():
            _skip_payment(df, idx, "skip_db_updates is True")

    @_util.log_exception_decorator
    def query_people_and_prepare_batch(
        self,
        *,
        ctx: _context.WsjRdpContext,
        conn: _psycopg.Connection | None = None,
        collection_date: _datetime.date | str | None = None,
        extra_static_df_cols: dict[str, _typing.Any] | None = None,
        extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
        df_cb: _collections_abc.Callable[[_pandas.DataFrame], _pandas.DataFrame]
        | None = None,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        out_dir: _pathlib.Path | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
        log_resulting_data_frame: bool | None = None,
        limit: int | None | _types.MissingType = _types.MISSING,
        now: _datetime.datetime
        | _datetime.date
        | str
        | float
        | None
        | _types.MissingType = _types.MISSING,
        report_all_updates: bool | None = None,
    ) -> PreparedBatch:
        from . import _util

        limit = _util.coalesce_missing(limit, self.query.limit)
        now = _util.to_datetime(_util.coalesce_missing(now, self.query.now))

        with _contextlib.ExitStack() as exit_stack:
            if conn is None:
                conn = ctx.hitobito_psycopg_connection(read_only=True)

            df = unfiltered_df = self.load_people_dataframe(
                ctx=ctx,
                conn=conn,
                collection_date=collection_date,
                extra_static_df_cols=extra_static_df_cols,
                extra_mailing_bcc=extra_mailing_bcc,
                log_resulting_data_frame=log_resulting_data_frame,
                limit=limit,
                now=now,
            )
            if df_cb is not None:
                _LOGGER.info("Update dataframe using callback %s", str(df_cb))
                df = df_cb(df)
            if out_dir is None:
                out_dir = ctx.out_dir
            return self.prepare(
                df,
                unfiltered_df=unfiltered_df,
                msg_cb=msg_cb,
                out_dir=out_dir,
                msgid_idstring=msgid_idstring,
                msgid_domain=msgid_domain,
                now=now,
                report_all_updates=report_all_updates,
            )

    def prepare(
        self,
        data: _pandas.DataFrame | _person.Person | _typing.Iterable[_person.Person],
        /,
        *,
        unfiltered_df: _pandas.DataFrame | None = None,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        out_dir: _pathlib.Path | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
        now: _datetime.datetime | _datetime.date | str | float | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
        open_editor: bool | None = None,
        report_all_updates: bool | None = None,
    ) -> PreparedBatch:
        import time

        import pandas as _pandas

        from . import _util
        from ._models import person as _person

        data_is_dataframe = False
        if isinstance(data, _person.Person):
            df = data.df
            people = [data]
        elif isinstance(data, _pandas.DataFrame):
            df = data
            people = list(_person.iter_people_dataframe(data))
            data_is_dataframe = True
        else:
            people = list(data)
            df = next(iter(people)).df

        if dry_run is None:
            dry_run = self.dry_run
        if skip_email is None:
            skip_email = self.skip_email
        if skip_db_updates is None:
            skip_db_updates = self.skip_db_updates

        now = _util.to_datetime(now)
        _LOGGER.info("Update dataframe for updates and action arguments...")
        unfiltered_df_is_df = unfiltered_df is df
        if self.person_updates:
            if not data_is_dataframe:
                raise RuntimeError(
                    "Updates only possible when data is a Pandas DataFrame"
                )
            df = self.update_dataframe_for_updates(df, now=now, inplace=False)
        if unfiltered_df_is_df:
            unfiltered_df = df
        if self.query.use_sepa_mailing:
            no_sepa_mail = [int(p["id"]) for p in people if not p["sepa_mailing_to"]]
            if no_sepa_mail:
                raise ValueError(
                    f"use_sepa_mailing: no SEPA mail address for {no_sepa_mail}"
                )
        if len(people) > 1:
            _LOGGER.info("Prepare mailing...")
        tic = time.monotonic()
        messages = tuple(
            self._prepare_email_message_for_person(
                p,
                msg_cb=msg_cb,
                msgid_idstring=msgid_idstring,
                msgid_domain=msgid_domain,
                open_editor=open_editor,
            )
            for p in people
        )
        toc = time.monotonic()
        if len(people) > 5 or (toc - tic) > 0.1:
            _LOGGER.info("  finished preparation of mailing (%g seconds)", toc - tic)
        if len(messages) == 1 and messages[0].content:
            new_content = messages[0].content
            config_yaml = self.__to_yaml__(content=new_content, signature=None).encode(
                "utf-8"
            )
        else:
            config_yaml = self.__to_yaml__().encode("utf-8")
        prepared_batch = PreparedBatch(
            name=self.name,
            df=df,
            unfiltered_df=unfiltered_df,
            messages=messages,
            config_yaml=config_yaml,
            action_arguments=self.action_arguments,
            updates=self.updates,
            log_note=self.log_note,
            email_from=self.email_from,
            from_addr=self.from_addr,
            out_dir=out_dir,
            now=now,
            dry_run=dry_run,
            skip_email=skip_email,
            skip_db_updates=bool(_util.coalesce(skip_db_updates, False)),
        )
        if report_all_updates:
            prepared_batch.log_person_updates()
        return prepared_batch

    def _prepare_email_message_for_person(
        self,
        person: _person.Person,
        *,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        policy: _email_policy.EmailPolicy | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
        open_editor: bool | None = None,
    ) -> PreparedEmailMessage:
        from . import _util

        prefix = "sepa_mailing" if self.query.use_sepa_mailing else "mailing"
        msg_for_row = _email_message_from_person(
            person,
            content=self.content,
            html_content=self.html_content,
            signature=self.signature,
            email_subject=self.email_subject,
            email_from=self.email_from or person[f"{prefix}_from"],
            email_to=_util.merge_mail_addresses(
                person[f"{prefix}_to"], self.extra_email_to
            ),
            email_cc=_util.merge_mail_addresses(
                person[f"{prefix}_cc"], self.extra_email_cc
            ),
            email_bcc=_util.merge_mail_addresses(
                person[f"{prefix}_bcc"], self.extra_email_bcc
            ),
            email_reply_to=self.email_reply_to or person[f"{prefix}_reply_to"],
            policy=policy,
            msgid_idstring=msgid_idstring,
            msgid_domain=msgid_domain,
            context={"query": self},
            extra_context=self.jinja_extra_globals,
            open_editor=open_editor,
        )
        prepared = PreparedEmailMessage(
            mailing_name=self.name,
            message=msg_for_row.msg,
            content=msg_for_row.content,
            person=person,
            row=person.row,
            eml_name=f"{self.name}.{person.id}.eml",
            summary=_util.render_template(
                self.summary,
                {"p": person, "person": person, "row": person, "msg": msg_for_row.msg},
            ),
        )
        if msg_cb:
            msg_cb(prepared)
        return prepared


_KEEP_NAN_KEYS = {"amount_paid_cents", "amount_unpaid_cents", "open_amount_cents"}


def _row_to_row_dict(row: _pandas.Series) -> dict[str, _typing.Any]:
    import math

    def _maybe_to_none(key, val):
        if (key in _KEEP_NAN_KEYS) or not isinstance(val, float) or not math.isnan(val):
            return val
        else:
            return None

    row_dict = {str(key): _maybe_to_none(key, val) for key, val in row.items()}
    return row_dict


def _strip_html_tags_builtin(html_content: str) -> str:
    import re
    from html import unescape

    html_content = re.sub(
        "<style.*?</style>", "", html_content, flags=re.DOTALL | re.IGNORECASE
    )
    html_content = re.sub("<br\\s*/?>", "\n", html_content, flags=re.IGNORECASE)
    html_content = re.sub("</p>", "\n", html_content, flags=re.IGNORECASE)
    text_without_tags = re.sub("<[^<]+?>", "", html_content)
    clean_text = unescape(text_without_tags)
    clean_lines = [line.strip() for line in clean_text.splitlines()]
    clean_text = "\n".join(line for line in clean_lines if line)

    return clean_text


def _strip_html_tags_html2text(html_content: str) -> str:
    import html2text

    return html2text.html2text(html_content)


@_dataclasses.dataclass(kw_only=True)
class EmailMessageForPerson:
    person: _person.Person
    row: _pandas.Series | dict[str, _typing.Any]
    content: str | None
    msg: _email_message.EmailMessage | None


def _email_message_from_person(
    person: _person.Person,
    *,
    email_subject: str,
    content: str | None = None,
    html_content: str | None,
    signature: str | None = None,
    email_from: str = "anmeldung@worldscoutjamboree.de",
    email_to: str | _collections_abc.Iterable[str] | None = None,
    email_cc: str | _collections_abc.Iterable[str] | None = None,
    email_bcc: str | _collections_abc.Iterable[str] | None = None,
    email_reply_to: str | _collections_abc.Iterable[str] | None = None,
    policy: _email_policy.EmailPolicy | None = None,
    email_date: _datetime.datetime | _datetime.date | str | float | None = None,
    message_id: str | None = None,
    msgid_idstring: str | None = None,
    msgid_domain: str | None = None,
    context: dict | None = None,
    extra_context: dict | None = None,
    open_editor: bool | None = None,
) -> EmailMessageForPerson:
    import email.message
    import email.utils

    import wsjrdp2027

    from . import DEFAULT_MSGID_DOMAIN, DEFAULT_MSGID_IDSTRING, _util
    from ._internal import signatures

    SIGNATURES = {
        k: v for k, v in wsjrdp2027.__dict__.items() if k.startswith("EMAIL_SIGNATURE_")
    } | {
        k: v for k, v in signatures.__dict__.items() if k.startswith("EMAIL_SIGNATURE_")
    }
    context = context or {}

    row = _row_to_row_dict(person.row)

    if content is None and html_content is None:
        return EmailMessageForPerson(person=person, row=row, msg=None, content=None)

    email_to = _util.to_str_list_or_none(email_to)
    email_cc = _util.to_str_list_or_none(email_cc)
    email_bcc = _util.to_str_list_or_none(email_bcc)
    email_reply_to = _util.to_str_list_or_none(email_reply_to)

    def render_template(template):
        local_context = context.copy()
        local_context |= {
            "p": person,
            "person": person,
            "row": person,
            "email_from": email_from,
            "email_to": email_to,
            "email_cc": email_cc,
            "email_bcc": email_bcc,
            "email_reply_to": email_reply_to,
            **SIGNATURES,
        }
        return _util.render_template(
            template,
            context=local_context,
            extra_context=extra_context,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    if message_id is None:
        message_id = email.utils.make_msgid(
            idstring=msgid_idstring or DEFAULT_MSGID_IDSTRING,
            domain=msgid_domain or DEFAULT_MSGID_DOMAIN,
        )
    email_date = email_date or row.get("email_date", None)
    email_date = _util.to_datetime(email_date)
    msg_date = email.utils.format_datetime(email_date)

    email_subject = render_template(email_subject).rstrip()

    if policy is None:
        policy = _util.get_default_email_policy()

    msg = email.message.EmailMessage(policy=policy)
    msg["Subject"] = email_subject
    msg["From"] = email_from
    if email_to is not None:
        msg["To"] = _util.to_str_list_or_none(email_to)
    if email_cc is not None:
        msg["Cc"] = _util.to_str_list_or_none(email_cc)
    if email_bcc is not None:
        msg["Bcc"] = _util.to_str_list_or_none(email_bcc)
    if email_reply_to is not None:
        msg["Reply-To"] = _util.to_str_list_or_none(email_reply_to)
    msg["Date"] = msg_date
    msg["Message-ID"] = message_id

    if not content:
        if html_content:
            content = _strip_html_tags_html2text(html_content)
        else:
            return EmailMessageForPerson(person=person, row=row, msg=None, content=None)
    content = render_template(content)
    if signature:
        signature = render_template(signature).lstrip()
        if not signature.startswith("-- \n"):
            signature = "-- \n" + signature
        content = content.rstrip() + "\n\n" + signature

    content = _maybe_edit_content(content, open_editor=open_editor)

    msg.set_content(content)

    if html_content:
        html_content = render_template(html_content)
        msg.add_alternative(html_content, subtype="html")

    return EmailMessageForPerson(person=person, row=row, msg=msg, content=content)


def _maybe_edit_content(content: str, *, open_editor: bool | None = None) -> str:
    import textwrap

    import editor as _editor

    if open_editor:
        new_content_bytes = _editor.edit(contents=content.encode("utf-8"))
        new_content = new_content_bytes.decode("utf-8")
        _LOGGER.info(
            "Replace Email content:\nOLD:\n%s\nNEW:\n%s",
            textwrap.indent(content, "  | "),
            textwrap.indent(new_content, "  | "),
        )
        return new_content
    else:
        return content


def write_data_and_send_mailing(
    ctx: _context.WsjRdpContext,
    mailing: PreparedBatch,
    *,
    dry_run: bool | None = None,
    out_dir: str | _pathlib.Path | None = None,
    zip_eml: bool | None = True,
    silent_skip_email: bool | None = None,
) -> None:
    if zip_eml is None:
        zip_eml = True

    if out_dir:
        out_dir = _pathlib.Path(out_dir)
    elif mailing.out_dir:
        out_dir = mailing.out_dir
    else:
        out_dir_base = _pathlib.Path(ctx.out_dir or "data/mailings")
        out_dir_tpl = str(
            out_dir_base / (mailing.name + "__{{ filename_suffix }}")
        ).replace("\\", "/")
        out_dir = ctx.make_out_path(out_dir_tpl)

    mailing.write_data(
        out_dir=out_dir, zip_eml=zip_eml, silent_skip_email=silent_skip_email
    )

    with ctx.mail_login(
        from_addr=mailing.from_addr,
        dry_run=dry_run,
    ) as mail_client:
        mailing.send(mail_client)


def _normalize_query_where_or_none(
    query: _people_query.PeopleQuery | dict | None = None,
    where: _people_query.PeopleWhere | dict | str | None = None,
    *,
    logger: _logging.Logger | _logging.LoggerAdapter | None = None,
) -> _people_query.PeopleQuery | None:
    from . import _people_query

    if query:
        query = _people_query.PeopleQuery.normalize(query)
        if where:
            if query and query.where:
                err_msg = "Arguments 'where' and 'query.where' are mutually exclusive"
                if logger:
                    logger.error(err_msg)
                raise RuntimeError(err_msg)
            else:
                where = _people_query.PeopleWhere.normalize(where)
                return query.replace(where=where)
        else:
            return query
    else:
        if where:
            return _people_query.PeopleQuery(where=where)
        else:
            return None


def _normalize_query_where(
    query: _people_query.PeopleQuery | dict | None = None,
    where: _people_query.PeopleWhere | dict | str | None = None,
    *,
    logger: _logging.Logger | _logging.LoggerAdapter | None = None,
) -> _people_query.PeopleQuery:
    from . import _people_query

    query = _normalize_query_where_or_none(query, where, logger=logger)
    return query if query is not None else _people_query.PeopleQuery()
