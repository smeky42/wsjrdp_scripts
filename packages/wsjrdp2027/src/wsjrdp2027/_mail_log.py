"""The data of one sent mail, as a log entry keeps it (``email_*`` keys).

A batch with ``log_note`` writes one log entry per mail
(:meth:`wsjrdp2027.PreparedBatch.insert_log_notes`): first with what the
prepared mail already says (:func:`prepared_data`, ``email_status`` is
``prepared``), then, once sent, with what the sending adds
(:func:`sent_data`). An entry still ``prepared`` after the run belongs to a
run that stopped before this mail.
"""

from __future__ import annotations

import datetime as _datetime
import typing as _typing


if _typing.TYPE_CHECKING:
    import email.message as _email_message


PREPARED = "prepared"
SENT = "sent"
FAILED = "failed"
NO_RECIPIENTS = "no_recipients"

# How a log line says the status (``email_status_text`` in the message).
STATUS_TEXTS = {
    PREPARED: "nicht verschickt (abgebrochen)",
    SENT: "verschickt",
    FAILED: "nicht verschickt (Fehler)",
    NO_RECIPIENTS: "nicht verschickt (keine Empfänger)",
}


def status_text(status: str) -> str:
    return STATUS_TEXTS.get(status, status)


def addresses(msg: _email_message.EmailMessage, header: str) -> list[str]:
    """The addresses of a header, ``Name <address>`` or the bare address.

    >>> import email.message
    >>> msg = email.message.EmailMessage()
    >>> msg["To"] = "Jörg Müller <jm@example.org>, other@example.org"
    >>> addresses(msg, "To")
    ['Jörg Müller <jm@example.org>', 'other@example.org']
    >>> addresses(msg, "Cc")
    []
    """
    import email.utils

    values = [str(v) for v in (msg.get_all(header) or [])]
    return [
        f"{name} <{addr}>" if name else addr
        for name, addr in email.utils.getaddresses(values)
        if addr
    ]


def prepared_data(
    msg: _email_message.EmailMessage,
    *,
    eml: bytes,
    eml_name: str | None = None,
    eml_zip_name: str | None = None,
    envelope_from: str | None = None,
) -> dict[str, _typing.Any]:
    """What the mail says before it is sent."""
    import email.utils
    import hashlib

    from . import _mail_client

    date = email.utils.parsedate_to_datetime(msg["Date"]) if msg["Date"] else None
    data: dict[str, _typing.Any] = {
        "email_status": PREPARED,
        "email_message_id": msg["Message-ID"],
        "email_subject": msg["Subject"],
        "email_date": date.isoformat() if date else None,
        "email_from": addresses(msg, "From"),
        "email_to": addresses(msg, "To"),
        "email_cc": addresses(msg, "Cc"),
        "email_bcc": addresses(msg, "Bcc"),
        "email_reply_to": addresses(msg, "Reply-To"),
        "email_envelope_from": envelope_from,
        "email_envelope_to": _mail_client.get_to_addrs(msg),
        "email_eml_size": len(eml),
        "email_eml_sha256": hashlib.sha256(eml).hexdigest(),
        "email_eml_name": eml_name,
    }
    if eml_zip_name:
        data["email_eml_zip_name"] = eml_zip_name
    return data


def sent_data(
    status: str,
    *,
    sent_at: _datetime.datetime | None = None,
    smtp_refused: _typing.Mapping | None = None,
    imap_mailbox: str | None = None,
    imap_result: _typing.Any = None,
    error: str | None = None,
) -> dict[str, _typing.Any]:
    """What the sending adds: the status and, once sent, when and where to.

    >>> sent_data("sent", sent_at=_datetime.datetime(2026, 11, 1, 9, 0),
    ...           smtp_refused={}, imap_mailbox="Sent",
    ...           imap_result=("OK", [b"[APPENDUID 1 4711] Append completed."]))
    ... # doctest: +NORMALIZE_WHITESPACE
    {'email_status': 'sent', 'email_sent_at': '2026-11-01T09:00:00',
     'email_smtp_refused': {}, 'email_imap_mailbox': 'Sent',
     'email_imap_uid': 4711}
    >>> sent_data("failed", error="boom")
    {'email_status': 'failed', 'email_error': 'boom'}
    """
    data: dict[str, _typing.Any] = {"email_status": status}
    if sent_at is not None:
        data["email_sent_at"] = sent_at.isoformat()
    if status == SENT:
        data["email_smtp_refused"] = {
            str(addr): list(reply) if isinstance(reply, tuple) else reply
            for addr, reply in (smtp_refused or {}).items()
        }
        data["email_imap_mailbox"] = imap_mailbox
        data["email_imap_uid"] = imap_append_uid(imap_result)
    if error is not None:
        data["email_error"] = error
    return _jsonable_bytes(data)


def imap_append_uid(imap_result: _typing.Any) -> int | None:
    """The UID an IMAP APPEND reports (``APPENDUID``), if the server does.

    >>> imap_append_uid(("OK", [b"[APPENDUID 1700000000 123] Append completed."]))
    123
    >>> imap_append_uid(("OK", [b"Append completed."])) is None
    True
    >>> imap_append_uid(None) is None
    True
    """
    import re

    if not imap_result or len(imap_result) < 2:
        return None
    for line in imap_result[1] or []:
        text = line.decode("utf-8", "replace") if isinstance(line, bytes) else str(line)
        if m := re.search(r"APPENDUID\s+\d+\s+(\d+)", text):
            return int(m.group(1))
    return None


def _jsonable_bytes(value: _typing.Any) -> _typing.Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, dict):
        return {k: _jsonable_bytes(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable_bytes(v) for v in value]
    return value
