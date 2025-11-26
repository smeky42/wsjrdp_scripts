from __future__ import annotations

import collections.abc as _collections_abc
import contextlib as _contextlib
import dataclasses as _dataclasses
import logging as _logging
import pathlib as _pathlib
import typing as _typing

from . import _people_where
from ._mail_config import WsjRdpMailConfig


if _typing.TYPE_CHECKING:
    import datetime as _datetime
    import email.message as _email_message
    import email.policy as _email_policy
    import imaplib as _imaplib
    import smtplib as _smtplib

    import pandas as _pandas
    import psycopg as _psycopg

    import wsjrdp2027 as _wsjrdp2027

    from . import _context, _people, _people_where


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
    email_from: str = ""
    from_addr: str | None = None
    config_yaml: bytes | None
    out_dir: _pathlib.Path | None = None


@_dataclasses.dataclass(kw_only=True)
class MailingConfig:
    where: _people_where.PeopleWhere | None = None
    email_subject: str = ""
    email_from: str = ""
    email_reply_to: str = ""
    from_addr: str | None = None
    signature: str = ""
    content: str = ""
    name: str = "mailing"
    summary: str = "{{ row.id }} {{ row.short_full_name }}; role: {{ row.payment_role }}; status: {{ row.status }}; To: {{ msg.to }}; Cc: {{ msg.cc }}"

    action_arguments: dict = _dataclasses.field(default_factory=lambda: {})
    raw_yaml: bytes | None = None

    def __post_init__(self) -> None:
        import email.utils

        if self.raw_yaml is None:
            self.raw_yaml = self.__to_yaml__().encode("utf-8")
        if self.email_from and not self.from_addr:
            from_addr = email.utils.parseaddr(self.email_from)[1]
            object.__setattr__(self, "from_addr", from_addr)
        elif self.from_addr:
            from_addr = email.utils.parseaddr(self.from_addr)[1]
            object.__setattr__(self, "from_addr", from_addr)

    @classmethod
    def from_yaml(
        cls,
        path: str | _pathlib.Path,
        /,
        *,
        name: str | None = None,
        where: _people_where.PeopleWhere | None = None,
    ) -> _typing.Self:
        import yaml as _yaml

        from . import _people

        path = _pathlib.Path(path)

        with open(path, "r", encoding="utf-8") as f:
            config = _yaml.load(f, Loader=_yaml.FullLoader)
        with open(path, "rb") as f:
            config["raw_yaml"] = f.read()

        _LOGGER.info("Read mailing config %s", path)
        if "where" in config:
            config["where"] = _people_where.PeopleWhere.from_dict(config["where"])
        if "name" not in config and path.stem:
            config["name"] = path.stem
        self = cls(**config)
        self = self.replace(name=name, where=where)
        return self

    def copy(self) -> _typing.Self:
        import copy

        return copy.deepcopy(self)

    def replace(
        self,
        *,
        name: str | None = None,
        where: _people_where.PeopleWhere | None = None,
    ) -> _typing.Self:
        import copy

        updated = False

        obj = copy.deepcopy(self)
        if name is not None and obj.name != name:
            obj.name = name
            updated = True
        if where is not None and obj.where != where:
            obj.where = where
            updated = True
        if updated:
            obj.raw_yaml = obj.__to_yaml__().encode("utf-8")
        return obj

    def __to_yaml__(self) -> str:
        import io

        import ruamel.yaml as _ruamel_yaml
        from ruamel.yaml.scalarstring import LiteralScalarString as lss

        def maybe_to_lss(s: str) -> str | lss:
            return lss(s) if "\n" in s else s

        d = {
            "where": (self.where.to_dict() if self.where is not None else None),
            "summary": self.summary,
            "email_subject": self.email_subject,
            "email_from": self.email_from,
            "email_reply_to": self.email_reply_to,
            "content": maybe_to_lss(self.content),
            "signature": maybe_to_lss(self.signature),
        }
        d = {k: v for k, v in d.items() if v is not None}
        yaml = _ruamel_yaml.YAML(typ="rt")
        yaml.width = 200
        with io.StringIO() as str_io:
            yaml.dump(d, stream=str_io)
            return str_io.getvalue()

    def load_people_dataframe(
        self,
        *,
        ctx: _wsjrdp2027.WsjRdpContext,
        conn: _psycopg.Connection | None = None,
        extra_static_df_cols: dict[str, _typing.Any] | None = None,
        extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
        log_resulting_data_frame: bool | None = None,
        limit: int | None = None,
    ) -> _pandas.DataFrame:
        from . import load_people_dataframe

        with _contextlib.ExitStack() as exit_stack:
            if conn is None:
                conn = exit_stack.enter_context(ctx.psycopg_connect())

            df = load_people_dataframe(
                conn,
                where=self.where,
                exclude_deregistered=False,
                extra_static_df_cols=extra_static_df_cols,
                extra_mailing_bcc=extra_mailing_bcc,
                log_resulting_data_frame=log_resulting_data_frame,
                limit=limit,
            )

        return df

    def query_people_and_prepare_mailing(
        self,
        *,
        ctx: _wsjrdp2027.WsjRdpContext,
        conn: _psycopg.Connection | None = None,
        extra_static_df_cols: dict[str, _typing.Any] | None = None,
        extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        out_dir: _pathlib.Path | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
        log_resulting_data_frame: bool | None = None,
        limit: int | None = None,
    ) -> PreparedMailing:
        df = self.load_people_dataframe(
            ctx=ctx,
            conn=conn,
            extra_static_df_cols=extra_static_df_cols,
            extra_mailing_bcc=extra_mailing_bcc,
            log_resulting_data_frame=log_resulting_data_frame,
            limit=limit,
        )
        if out_dir is None:
            out_dir = ctx.out_dir
        return self.prepare_mailing_for_dataframe(
            df,
            msg_cb=msg_cb,
            out_dir=out_dir,
            msgid_idstring=msgid_idstring,
            msgid_domain=msgid_domain,
        )

    def prepare_mailing_for_dataframe(
        self,
        df: _pandas.DataFrame,
        *,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        out_dir: _pathlib.Path | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
    ) -> PreparedMailing:
        import time

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
        return PreparedMailing(
            name=self.name,
            df=df,
            messages=messages,
            config_yaml=self.raw_yaml,
            action_arguments=self.action_arguments,
            email_from=self.email_from,
            from_addr=self.from_addr,
            out_dir=out_dir,
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

        id = row["id"]
        msg = email_message_from_row(
            row,
            content=self.content,
            signature=self.signature,
            email_subject=self.email_subject,
            email_from=self.email_from or row["mailing_from"],
            email_to=row["mailing_to"],
            email_cc=row["mailing_cc"],
            email_bcc=row["mailing_bcc"],
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
            summary=_util.render_template(self.summary, {"row": row, "msg": msg}),
        )
        if msg_cb:
            msg_cb(prepared)
        return prepared


def email_message_from_row(
    row: _pandas.Series,
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
    import time

    import wsjrdp2027

    from . import DEFAULT_MSGID_DOMAIN, DEFAULT_MSGID_IDSTRING, _util

    SIGNATURES = {
        k: v for k, v in wsjrdp2027.__dict__.items() if k.startswith("EMAIL_SIGNATURE_")
    }

    email_to = _util.to_str_list(email_to)
    email_cc = _util.to_str_list(email_cc)
    email_bcc = _util.to_str_list(email_bcc)
    email_reply_to = _util.to_str_list(email_reply_to)

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
        msg["To"] = _util.to_str_list(email_to)
    if email_cc is not None:
        msg["Cc"] = _util.to_str_list(email_cc)
    if email_bcc is not None:
        msg["Bcc"] = _util.to_str_list(email_bcc)
    if email_reply_to is not None:
        msg["Reply-To"] = _util.to_str_list(email_reply_to)
    msg["Date"] = msg_date
    msg["Message-ID"] = message_id
    msg.set_content(content)
    return msg


def send_mailing(
    ctx: _wsjrdp2027.WsjRdpContext,
    mailing: PreparedMailing,
    *,
    dry_run: bool | None = None,
    out_dir: str | _pathlib.Path | None = None,
    zip_eml: bool | None = True,
) -> None:
    import io
    import zipfile

    from . import _people

    if zip_eml is None:
        zip_eml = True
    out_dir = _pathlib.Path(out_dir or ctx.out_dir or "data/mailings")
    num_messages = len(mailing.messages)

    if not mailing.out_dir:
        out_dir_tpl = str(out_dir / (mailing.name + "__{{ filename_suffix }}")).replace(
            "\\", "/"
        )
        mailing.out_dir = ctx.make_out_path(out_dir_tpl)
    mailing.out_dir.mkdir(exist_ok=True, parents=True)
    xlsx_path = mailing.out_dir / f"{mailing.name}.xlsx"
    yml_path = mailing.out_dir / f"{mailing.name}.yml"
    if zip_eml:
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            for prep_msg in mailing.messages:
                zf.writestr(prep_msg.eml_name, data=prep_msg.eml)
        eml_zip_path = mailing.out_dir / f"{mailing.name}.zip"
        with open(eml_zip_path, "wb") as f:
            f.write(zip_buf.getvalue())
        _LOGGER.info("  wrote eml zip %s", eml_zip_path)
    else:
        for prep_msg in mailing.messages:
            eml_path = mailing.out_dir / prep_msg.eml_name
            eml_path.write_bytes(prep_msg.eml)
            _LOGGER.info("  wrote eml %s", eml_path)
    _people.write_people_dataframe_to_xlsx(
        mailing.df, xlsx_path, log_level=_logging.DEBUG
    )
    _LOGGER.info("  wrote xlsx %s", xlsx_path)
    if mailing.config_yaml:
        with open(yml_path, "wb") as f:
            f.write(mailing.config_yaml)
        _LOGGER.info("  wrote yml %s", yml_path)

    with ctx.mail_login(
        from_addr=mailing.from_addr,
        dry_run=dry_run,
    ) as mail_client:
        for i, prep_msg in enumerate(mailing.messages, start=1):
            pcnt = (i / num_messages) * 100.0
            mail_client.send_message(prep_msg.message, from_addr=mailing.from_addr)
            _LOGGER.info("%s %s", f"{i}/{num_messages} ({pcnt:.1f}%)", prep_msg.summary)


def send_mailings(
    ctx: _wsjrdp2027.WsjRdpContext,
    *,
    mailings: PreparedMailing | _collections_abc.Iterable[PreparedMailing],
    dry_run: bool | None = None,
    out_dir: str | _pathlib.Path | None = None,
    zip_eml: bool = True,
) -> None:
    mailings = [mailings] if isinstance(mailings, PreparedMailing) else list(mailings)
    for mailing in mailings:
        send_mailing(ctx, mailing, dry_run=dry_run, out_dir=out_dir, zip_eml=zip_eml)


def _iter_mailings(
    mailings: _collections_abc.Iterable[PreparedMailing],
) -> _collections_abc.Iterator[tuple[PreparedMailing, PreparedEmailMessage]]:
    for mailing in mailings:
        for prep_msg in mailing.messages:
            yield mailing, prep_msg
