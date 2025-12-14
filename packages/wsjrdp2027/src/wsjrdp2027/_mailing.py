from __future__ import annotations

import collections.abc as _collections_abc
import contextlib as _contextlib
import dataclasses as _dataclasses
import datetime as _datetime
import logging as _logging
import pathlib as _pathlib
import typing as _typing

from . import _people_query, _util


if _typing.TYPE_CHECKING:
    import email.message as _email_message
    import email.policy as _email_policy

    import pandas as _pandas
    import psycopg as _psycopg

    from . import _context, _mail_client, _people_query


__all__ = [
    "MailingConfig",
    "PreparedEmailMessage",
    "PreparedMailing",
]


_LOGGER = _logging.getLogger(__name__)


@_dataclasses.dataclass(kw_only=True)
class PreparedEmailMessage:
    mailing_name: str
    message: _email_message.EmailMessage
    row: _pandas.Series | None = None
    summary: str
    eml_name: str
    _eml: bytes | None = None

    def __init__(
        self,
        *,
        mailing_name: str,
        message: _email_message.EmailMessage,
        row: _pandas.Series | None = None,
        summary: str,
        eml_name: str | None = None,
        eml: bytes | None = None,
    ) -> None:
        self.mailing_name = mailing_name
        self.message = message
        self.row = row
        self.summary = summary
        self.eml_name = eml_name if eml_name else f"{self.mailing_name}.eml"
        self._eml = eml

    @property
    def eml(self) -> bytes:
        if self._eml is None:
            self._eml = self.message.as_bytes()
        return self._eml

    @eml.setter
    def eml(self, value: bytes) -> None:
        self._eml = value


@_dataclasses.dataclass(kw_only=True)
class PreparedMailing:
    name: str = "mailing"
    df: _pandas.DataFrame
    messages: tuple[PreparedEmailMessage, ...] = ()
    action_arguments: dict = _dataclasses.field(default_factory=lambda: {})
    updates: dict = _dataclasses.field(default_factory=lambda: {})
    email_from: str = ""
    from_addr: str | None = None
    config_yaml: bytes | None
    out_dir: _pathlib.Path | None = None
    now: _datetime.datetime = _typing.cast(_datetime.datetime, None)
    dry_run: bool = False
    skip_email: bool = False
    skip_db_updates: bool = False

    def __post_init__(self) -> None:
        if self.now is None:
            self.now = _datetime.datetime.now().astimezone()

    def _get_out_dir(self, out_dir: _pathlib.Path | str | None = None) -> _pathlib.Path:
        if out_dir:
            return _pathlib.Path(out_dir)
        elif self.out_dir:
            return self.out_dir
        else:
            return _pathlib.Path("data") / "mailings"

    def write_data(
        self, *, out_dir: _pathlib.Path | None = None, zip_eml: bool | None = None
    ) -> _pathlib.Path:
        import io
        import zipfile

        from . import _people

        if zip_eml is None:
            zip_eml = True
        out_dir = self._get_out_dir(out_dir=out_dir)

        out_dir.mkdir(exist_ok=True, parents=True)
        _LOGGER.info("  mailing output directory: %s", out_dir)
        xlsx_path = out_dir / f"{self.name}.xlsx"
        yml_path = out_dir / f"{self.name}.yml"
        if zip_eml:
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w") as zf:
                for prep_msg in self.messages:
                    zf.writestr(prep_msg.eml_name, data=prep_msg.eml)
            eml_zip_path = out_dir / f"{self.name}.zip"
            with open(eml_zip_path, "wb") as f:
                f.write(zip_buf.getvalue())
            _LOGGER.info("  wrote eml zip %s", eml_zip_path)
        else:
            for prep_msg in self.messages:
                eml_path = out_dir / prep_msg.eml_name
                eml_path.write_bytes(prep_msg.eml)
                _LOGGER.info("  wrote eml %s", eml_path)
        _people.write_people_dataframe_to_xlsx(
            self.df, xlsx_path, log_level=_logging.DEBUG
        )
        _LOGGER.info("  wrote xlsx %s", xlsx_path)
        if self.config_yaml:
            with open(yml_path, "wb") as f:
                f.write(self.config_yaml)
            _LOGGER.info("  wrote yml %s", yml_path)
        return out_dir

    def send(self, mail_client: _mail_client.MailClient, /) -> None:
        import textwrap

        num_messages = len(self.messages)
        if self.dry_run:
            if self.skip_email:
                _LOGGER.info("Skip sending %s messages (dry_run is True and skip_email is True)", num_messages)
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
                _LOGGER.debug(
                    "  row:\n%s", textwrap.indent(prep_msg.row.to_string(), "   | ")
                )
            try:
                mail_client.send_message(prep_msg.message, from_addr=self.from_addr)
            except Exception as exc:
                _LOGGER.error("  Exception raised during email sending: %s", str(exc))
                raise


@_dataclasses.dataclass(kw_only=True)
class MailingConfig:
    query: _people_query.PeopleQuery = _dataclasses.field(
        default_factory=lambda: _people_query.PeopleQuery()
    )
    where: _dataclasses.InitVar[_people_query.PeopleWhere | dict | None] = None
    email_subject: str = ""
    email_from: str = ""
    email_reply_to: str = ""
    extra_email_to: list[str] | None = None
    extra_email_cc: list[str] | None = None
    extra_email_bcc: list[str] | None = None
    from_addr: str | None = None
    signature: str = ""
    content: str = ""
    name: str = "mailing"
    summary: str = "{{ row.id }} {{ row.short_full_name }}; role: {{ row.payment_role }}; status: {{ row.status }}; To: {{ msg.to }}; Cc: {{ msg.cc }}"
    action_arguments: dict = _dataclasses.field(default_factory=lambda: {})
    updates: dict = _dataclasses.field(default_factory=lambda: {})
    dry_run: bool = False
    skip_email: bool = None  # type: ignore
    skip_db_updates: bool = False
    raw_yaml: bytes | None = None

    def __post_init__(
        self, where: _people_query.PeopleWhere | dict | None = None
    ) -> None:
        import email.utils

        from . import _util

        if where is not None:
            if self.query.where:
                raise RuntimeError("Only one of 'where' and 'query.where' is allowed")
            if isinstance(where, dict):
                where = _people_query.PeopleWhere.from_dict(where)
            self.query.where = where

        if self.raw_yaml is None:
            self.raw_yaml = self.__to_yaml__().encode("utf-8")
        if self.email_from and not self.from_addr:
            from_addr = email.utils.parseaddr(self.email_from)[1]
            object.__setattr__(self, "from_addr", from_addr)
        elif self.from_addr:
            from_addr = email.utils.parseaddr(self.from_addr)[1]
            object.__setattr__(self, "from_addr", from_addr)

        if self.skip_email is None:
            self.skip_email = False
        updates = self.updates
        for key in ["add_tags", "new_primary_group_role_types"]:
            if key not in updates:
                continue
            val = _util.to_str_list(updates.get(key)) or None
            if val is None:
                updates.pop(key, None)
            else:
                updates[key] = val

    @classmethod
    def from_yaml(
        cls,
        path: str | _pathlib.Path,
        /,
        *,
        name: str | None = None,
        query: _people_query.PeopleQuery | None = None,
        where: _people_query.PeopleWhere | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
    ) -> _typing.Self:
        import yaml as _yaml

        from . import _util

        if where:
            if query:
                raise RuntimeError(
                    "Arguments 'query' and 'where' are mutually exclusive"
                )
            else:
                query = _people_query.PeopleQuery(where=where)

        path = _pathlib.Path(path)

        with open(path, "r", encoding="utf-8") as f:
            config = _yaml.load(f, Loader=_yaml.FullLoader)
        with open(path, "rb") as f:
            config["raw_yaml"] = f.read()

        _LOGGER.info("Read mailing config %s", path)
        missing = object()
        if (v := config.get("query", missing)) is not missing:
            config["query"] = _people_query.PeopleQuery(**v)
        if "name" not in config and path.stem:
            config["name"] = path.stem
        for k in ["extra_email_to", "extra_email_cc", "extra_email_bcc"]:
            if (extra := config.pop(k, None)) is not None:
                config[k] = _util.to_str_list_or_none(extra)
        self = cls(**config)
        self = self.replace(
            name=name,
            query=query,
            dry_run=dry_run,
            skip_email=skip_email,
            skip_db_updates=skip_db_updates,
        )
        return self

    def copy(self) -> _typing.Self:
        import copy

        return copy.deepcopy(self)

    def replace(
        self,
        *,
        name: str | None = None,
        query: _people_query.PeopleQuery | None = None,
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
        if dry_run is not None:
            obj.dry_run = dry_run
        if skip_email is not None:
            obj.skip_email = skip_email
        if skip_db_updates is not None:
            obj.skip_db_updates = skip_db_updates
        return obj

    def __to_yaml__(self) -> str:
        from . import _util

        d = {
            "action_arguments": (self.action_arguments or None),
            "updates": (self.updates or None),
            "query": (self.query.__to_dict__() if self.query is not None else None),
            "summary": self.summary,
            "email_subject": self.email_subject,
            "email_from": self.email_from,
            "email_reply_to": self.email_reply_to,
            "extra_email_to": self.extra_email_to,
            "extra_email_cc": self.extra_email_cc,
            "extra_email_bcc": self.extra_email_bcc,
            "content": self.content,
            "signature": self.signature,
        }
        d = {k: v for k, v in d.items() if v is not None}

        return _util.to_yaml_str(d)

    def update_raw_yaml(self, raw_yaml: bytes | str | None = None) -> None:
        if raw_yaml is None:
            raw_yaml = self.__to_yaml__().encode("utf-8")
        elif isinstance(raw_yaml, str):
            raw_yaml = raw_yaml.encode("utf-8")
        self.raw_yaml = raw_yaml

    def update_dataframe_for_updates(
        self,
        df: _pandas.DataFrame,
        /,
        *,
        conn: _psycopg.Connection,
        inplace: bool = True,
        now: _datetime.datetime | _datetime.date | str | int | float | None = None,
    ) -> _pandas.DataFrame:
        from . import _people, _person_pg

        if not inplace:
            df = df.copy(deep=True)

        keys_set = set(self.updates) & _person_pg.VALID_PERSON_UPDATE_KEYS
        _people.update_dataframe_for_updates(
            df,
            conn=conn,
            updates={k: v for k, v in self.updates.items() if k in keys_set},
            now=now,
        )
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
        limit: int | None = None,
        now: _datetime.datetime | _datetime.date | str | int | float | None = None,
    ) -> _pandas.DataFrame:
        import textwrap

        import pandas as _pandas

        from . import _people, _util

        query = self.query.replace(
            limit=limit,
            collection_date=_util.to_date_or_none(collection_date),
            now=_util.to_datetime_or_none(now),
        )

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
            df["skip_db_updates"] = False
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
                    limit=limit,
                    now=now,
                )
                if not df2.empty:
                    df2["skip_db_updates"] = True
                    df = _pandas.concat(
                        [df, df2], axis=0, ignore_index=True, sort=False
                    )

        if log_resulting_data_frame or (log_resulting_data_frame is None):
            _LOGGER.info(
                "Resulting pandas DataFrame:\n%s", textwrap.indent(str(df), "  ")
            )

        return df

    @_util.log_exception_decorator
    def query_people_and_prepare_mailing(
        self,
        *,
        ctx: _context.WsjRdpContext,
        conn: _psycopg.Connection | None = None,
        collection_date: _datetime.date | str | None = None,
        extra_static_df_cols: dict[str, _typing.Any] | None = None,
        extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        out_dir: _pathlib.Path | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
        log_resulting_data_frame: bool | None = None,
        limit: int | None = None,
        now: _datetime.datetime | _datetime.date | str | int | float | None = None,
    ) -> PreparedMailing:
        from . import _util

        now = _util.to_datetime(now)

        with _contextlib.ExitStack() as exit_stack:
            if conn is None:
                conn = exit_stack.enter_context(ctx.psycopg_connect())

            df = self.load_people_dataframe(
                ctx=ctx,
                conn=conn,
                collection_date=collection_date,
                extra_static_df_cols=extra_static_df_cols,
                extra_mailing_bcc=extra_mailing_bcc,
                log_resulting_data_frame=log_resulting_data_frame,
                limit=limit,
                now=now,
            )
            if out_dir is None:
                out_dir = ctx.out_dir
            return self.prepare_mailing_for_dataframe(
                df,
                conn=conn,
                msg_cb=msg_cb,
                out_dir=out_dir,
                msgid_idstring=msgid_idstring,
                msgid_domain=msgid_domain,
                now=now,
            )

    def prepare_mailing_for_dataframe(
        self,
        df: _pandas.DataFrame,
        *,
        conn: _psycopg.Connection,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        out_dir: _pathlib.Path | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
        now: _datetime.datetime | _datetime.date | str | int | float | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
    ) -> PreparedMailing:
        import time

        from . import _util

        if dry_run is None:
            dry_run = self.dry_run
        if skip_email is None:
            skip_email = self.skip_email
        if skip_db_updates is None:
            skip_db_updates = self.skip_db_updates

        now = _util.to_datetime(now)
        _LOGGER.info("Update dataframe for updates and action arguments...")
        df = self.update_dataframe_for_updates(df, conn=conn, now=now, inplace=False)
        _LOGGER.info("Prepare mailing...")
        tic = time.monotonic()
        messages = tuple(
            self.prepare_email_message_for_row(
                row,
                msg_cb=msg_cb,
                msgid_idstring=msgid_idstring,
                msgid_domain=msgid_domain,
            )
            for _, row in df.iterrows()
        )
        toc = time.monotonic()
        _LOGGER.info("  finished preparation of mailing (%g seconds)", toc - tic)
        config_yaml = self.__to_yaml__().encode("utf-8")
        return PreparedMailing(
            name=self.name,
            df=df,
            messages=messages,
            config_yaml=config_yaml,
            action_arguments=self.action_arguments,
            updates=self.updates,
            email_from=self.email_from,
            from_addr=self.from_addr,
            out_dir=out_dir,
            now=now,
            dry_run=dry_run,
            skip_email=skip_email,
            skip_db_updates=skip_db_updates,
        )

    def prepare_email_message_for_row(
        self,
        row: _pandas.Series,
        *,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        policy: _email_policy.EmailPolicy | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
    ) -> PreparedEmailMessage:
        from . import _util

        row_dict = _row_to_row_dict(row)

        id = row["id"]
        msg = email_message_from_row(
            row_dict,
            content=self.content,
            signature=self.signature,
            email_subject=self.email_subject,
            email_from=self.email_from or row["mailing_from"],
            email_to=_util.merge_mail_addresses(row["mailing_to"], self.extra_email_to),
            email_cc=_util.merge_mail_addresses(row["mailing_cc"], self.extra_email_cc),
            email_bcc=_util.merge_mail_addresses(
                row["mailing_bcc"], self.extra_email_bcc
            ),
            email_reply_to=self.email_reply_to or row["mailing_reply_to"],
            policy=policy,
            msgid_idstring=msgid_idstring,
            msgid_domain=msgid_domain,
        )
        prepared = PreparedEmailMessage(
            mailing_name=self.name,
            message=msg,
            row=row,
            eml_name=f"{self.name}.{id}.eml",
            summary=_util.render_template(self.summary, {"row": row_dict, "msg": msg}),
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


def email_message_from_row(
    row: _pandas.Series | dict[str, _typing.Any],
    *,
    content: str,
    signature: str | None = None,
    email_subject: str,
    email_from: str = "anmeldung@worldscoutjamboree.de",
    email_to: str | _collections_abc.Iterable[str] | None = None,
    email_cc: str | _collections_abc.Iterable[str] | None = None,
    email_bcc: str | _collections_abc.Iterable[str] | None = None,
    email_reply_to: str | _collections_abc.Iterable[str] | None = None,
    policy: _email_policy.EmailPolicy | None = None,
    email_date: _datetime.datetime | _datetime.date | str | float | int | None = None,
    message_id: str | None = None,
    msgid_idstring: str | None = None,
    msgid_domain: str | None = None,
) -> _email_message.EmailMessage:
    import email.message
    import email.utils

    import wsjrdp2027

    from . import DEFAULT_MSGID_DOMAIN, DEFAULT_MSGID_IDSTRING, _util

    SIGNATURES = {
        k: v for k, v in wsjrdp2027.__dict__.items() if k.startswith("EMAIL_SIGNATURE_")
    }

    email_to = _util.to_str_list_or_none(email_to)
    email_cc = _util.to_str_list_or_none(email_cc)
    email_bcc = _util.to_str_list_or_none(email_bcc)
    email_reply_to = _util.to_str_list_or_none(email_reply_to)

    def render_template(template):
        context = {
            "row": row,
            "email_from": email_from,
            "email_to": email_to,
            "email_cc": email_cc,
            "email_bcc": email_bcc,
            "email_reply_to": email_reply_to,
            **SIGNATURES,
        }
        return _util.render_template(
            template, context, trim_blocks=True, lstrip_blocks=True
        )

    if message_id is None:
        message_id = email.utils.make_msgid(
            idstring=msgid_idstring or DEFAULT_MSGID_IDSTRING,
            domain=msgid_domain or DEFAULT_MSGID_DOMAIN,
        )
    email_date = email_date or row.get("email_date", None)
    email_date = _util.to_datetime(email_date)
    msg_date = email.utils.format_datetime(email_date)

    email_subject = render_template(email_subject)
    content = render_template(content)
    if signature:
        signature = render_template(signature).lstrip()
        if not signature.startswith("-- \n"):
            signature = "-- \n" + signature
        content = content.rstrip() + "\n\n" + signature

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
    msg.set_content(content)
    return msg


def write_data_and_send_mailing(
    ctx: _context.WsjRdpContext,
    mailing: PreparedMailing,
    *,
    dry_run: bool | None = None,
    out_dir: str | _pathlib.Path | None = None,
    zip_eml: bool | None = True,
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

    mailing.write_data(out_dir=out_dir, zip_eml=zip_eml)

    with ctx.mail_login(
        from_addr=mailing.from_addr,
        dry_run=dry_run,
    ) as mail_client:
        mailing.send(mail_client)
