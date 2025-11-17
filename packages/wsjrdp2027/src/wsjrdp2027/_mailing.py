from __future__ import annotations

import contextlib as _contextlib
import dataclasses as _dataclasses
import logging as _logging
import pathlib as _pathlib
import typing as _typing


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc
    import email.message as _email_message

    import pandas as _pandas
    import psycopg as _psycopg

    import wsjrdp2027 as _wsjrdp2027

    from . import _people


__all__ = [
    "MailingConfig",
    "PreparedMailing",
]

_LOGGER = _logging.getLogger(__name__)


@_dataclasses.dataclass(kw_only=True)
class PreparedEmailMessage:
    summary: str
    mailing_name: str
    message: _email_message.EmailMessage
    eml_name: str
    eml: bytes


@_dataclasses.dataclass(kw_only=True)
class PreparedMailing:
    name: str = "mailing"
    df: _pandas.DataFrame
    messages: tuple[PreparedEmailMessage, ...] = ()
    action_arguments: dict = _dataclasses.field(default_factory=lambda: {})
    config_yaml: bytes | None
    out_dir: _pathlib.Path | None = None


@_dataclasses.dataclass(kw_only=True)
class MailingConfig:
    select: _people.SelectPeopleConfig | None = None
    email_subject: str = ""
    email_from: str = ""
    email_reply_to: str = ""
    signature: str = ""
    content: str = ""
    name: str = "mailing"
    summary: str = "To: {{msg.To}}; Cc: {{msg.Cc}}"

    action_arguments: dict = _dataclasses.field(default_factory=lambda: {})
    raw_yaml: bytes | None = None

    @classmethod
    def from_yaml(cls, path: str | _pathlib.Path) -> _typing.Self:
        import yaml as _yaml

        from . import _people

        path = _pathlib.Path(path)

        with open(path, "r", encoding="utf-8") as f:
            config = _yaml.load(f, Loader=_yaml.FullLoader)
        with open(path, "rb") as f:
            config["raw_yaml"] = f.read()

        _LOGGER.info("Read mailing config %s", path)
        if "select" in config:
            config["select"] = _people.SelectPeopleConfig.from_dict(config["select"])
        if "name" not in config and path.stem:
            config["name"] = path.stem
        self = cls(**config)
        return self

    def load_people_dataframe(
        self,
        *,
        ctx: _wsjrdp2027.WsjRdpContext,
        conn: _psycopg.Connection | None = None,
    ) -> _pandas.DataFrame:
        from . import load_people_dataframe

        with _contextlib.ExitStack() as exit_stack:
            if conn is None:
                conn = exit_stack.enter_context(ctx.psycopg_connect())

            df = load_people_dataframe(
                conn, where=self.select, exclude_deregistered=False
            )

        return df

    def query_people_and_prepare_mailing(
        self,
        *,
        ctx: _wsjrdp2027.WsjRdpContext,
        conn: _psycopg.Connection | None = None,
    ) -> PreparedMailing:
        import time

        df = self.load_people_dataframe(ctx=ctx, conn=conn)
        _LOGGER.info("Prepare mailing...")
        tic = time.monotonic()
        messages = tuple(
            self.prepare_email_message_for_row(row) for _, row in df.iterrows()
        )
        toc = time.monotonic()
        _LOGGER.info("  finished preperation of mailing (%g seconds)", toc - tic)
        return PreparedMailing(
            name=self.name,
            df=df,
            messages=messages,
            config_yaml=self.raw_yaml,
            action_arguments=self.action_arguments,
        )

    def prepare_email_message_for_row(
        self, row: _pandas.Series
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
        )
        return PreparedEmailMessage(
            mailing_name=self.name,
            message=msg,
            eml_name=f"{self.name}.{id}.eml",
            eml=msg.as_bytes(),
            summary=_util.render_template(self.summary, {"row": row, "msg": msg}),
        )


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
) -> _email_message.EmailMessage:
    import email.message
    import email.utils

    import wsjrdp2027

    from . import _util

    email_to = _util.to_str_list(email_to)
    email_cc = _util.to_str_list(email_cc)
    email_bcc = _util.to_str_list(email_bcc)
    email_reply_to = _util.to_str_list(email_reply_to)

    def render_template(template):
        context = {
            "row": row,
            "EMAIL_SIGNATURE_CMT": wsjrdp2027.EMAIL_SIGNATURE_CMT,
            "EMAIL_SIGNATURE_HOC": wsjrdp2027.EMAIL_SIGNATURE_HOC,
            "EMAIL_SIGNATURE_ORG": wsjrdp2027.EMAIL_SIGNATURE_ORG,
            "email_from": email_from,
            "email_to": email_to,
            "email_cc": email_cc,
            "email_bcc": email_bcc,
            "email_reply_to": email_reply_to,
        }
        return _util.render_template(
            template, context, trim_blocks=True, lstrip_blocks=True
        )

    email_subject = render_template(email_subject)
    content = render_template(content)
    if signature:
        signature = render_template(signature).lstrip()
        if not signature.startswith("-- \n"):
            signature = "-- \n" + signature
        content = content.rstrip() + "\n\n" + signature

    msg = email.message.EmailMessage()
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
    msg["Date"] = email.utils.formatdate(localtime=True)
    msg.set_content(content)

    return msg


def send_mailings(
    ctx: _wsjrdp2027.WsjRdpContext,
    *,
    mailings: PreparedMailing | _collections_abc.Iterable[PreparedMailing],
    send_message: bool = True,
    out_dir: str | _pathlib.Path | None = None,
) -> None:
    import io
    import zipfile

    import wsjrdp2027

    if isinstance(mailings, PreparedMailing):
        mailings = [mailings]
    else:
        mailings = list(mailings)

    out_dir = _pathlib.Path(out_dir or ctx.out_dir or "data/mailings")
    num_messages = sum(len(m.messages) for m in mailings)

    for mailing in mailings:
        zip_buf = io.BytesIO()

        if not mailing.out_dir:
            out_dir_tpl = str(
                out_dir / (mailing.name + "__{{ filename_suffix }}")
            ).replace("\\", "/")
            mailing.out_dir = ctx.make_out_path(out_dir_tpl)
        mailing.out_dir.mkdir(exist_ok=True, parents=True)
        with zipfile.ZipFile(zip_buf, "w") as zf:
            for prep_msg in mailing.messages:
                zf.writestr(prep_msg.eml_name, data=prep_msg.eml)
        eml_zip_path = mailing.out_dir / f"{mailing.name}.zip"
        xlsx_path = mailing.out_dir / f"{mailing.name}.xlsx"
        yml_path = mailing.out_dir / f"{mailing.name}.yml"
        with open(eml_zip_path, "wb") as f:
            f.write(zip_buf.getvalue())
        _LOGGER.info("  wrote eml zip %s", eml_zip_path)
        wsjrdp2027.write_people_dataframe_to_xlsx(mailing.df, xlsx_path)
        _LOGGER.info("  wrote xlsx %s", eml_zip_path)
        if mailing.config_yaml:
            with open(yml_path, "wb") as f:
                f.write(mailing.config_yaml)
            _LOGGER.info("  wrote yml %s", yml_path)

    if send_message:
        ctx.require_approval_to_send_email_in_prod()

    with ctx.smtp_login() as smtp_client:
        for i, (mailing, prep_msg) in enumerate(_iter_mailings(mailings), start=1):
            pcnt = (i / num_messages) * 100.0
            if send_message:
                smtp_client.send_message(prep_msg.message)
            else:
                _LOGGER.info("Skip actual email sending")
            _LOGGER.info("%s %s", f"{i}/{num_messages} ({pcnt:.1f}%)", prep_msg.summary)


def _iter_mailings(
    mailings: _collections_abc.Iterable[PreparedMailing],
) -> _collections_abc.Iterator[tuple[PreparedMailing, PreparedEmailMessage]]:
    for mailing in mailings:
        for prep_msg in mailing.messages:
            yield mailing, prep_msg
