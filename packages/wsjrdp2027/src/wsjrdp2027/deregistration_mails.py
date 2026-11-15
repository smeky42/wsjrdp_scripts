"""What a script tells Hitobito about a deregistration mail it sent.

The wagon records such a report (``form_sent.json`` for the Abmelde-Formular,
``confirmation_sent.json`` for the confirmation) as a log entry and an event
of the deregistration (``WsjrdpDeregistration``); the report names the
deregistration it was built from by sending its ``deregistration.json`` back.
Users: ``registration_tools/create_deregistration_form.py`` and
``registration_tools/bestätigung_abmeldung.py``.
"""

from __future__ import annotations

import base64 as _base64
import datetime as _datetime
import email.utils as _email_utils
import json as _json
import logging as _logging
import typing as _typing


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc
    import email.message as _email_message
    import pathlib as _pathlib


_LOGGER = _logging.getLogger(__name__)


def addresses(msg: _email_message.EmailMessage, header: str) -> list[str]:
    """The plain addresses of *header*, without display names.

    >>> import email.message
    >>> m = email.message.EmailMessage()
    >>> m["To"] = "Max <max@example.org>, erika@example.org"
    >>> addresses(m, "To"), addresses(m, "Cc")
    (['max@example.org', 'erika@example.org'], [])
    """
    values = msg.get_all(header) or []
    return [addr for _name, addr in _email_utils.getaddresses(values) if addr]


def literal_subject(subject: str) -> str:
    """*subject* for a mailing's email_subject, which is a Jinja template:
    taken as it is, whatever braces it holds.

    >>> literal_subject("WSJ27 Abmeldung YP 1 Max {{x}}")
    '{% raw %}WSJ27 Abmeldung YP 1 Max {{x}}{% endraw %}'
    """
    return "{% raw %}" + subject.replace("{% endraw %}", "") + "{% endraw %}"


def build_report(
    msg: _email_message.EmailMessage,
    deregistration: _collections_abc.Mapping[str, _typing.Any],
    *,
    sent_at: _datetime.datetime,
    script: str,
) -> dict[str, _typing.Any]:
    """The report of a mail that went out, the mail itself included as an EML
    (``email_eml_base64``) -- Hitobito keeps it as a document of the
    deregistration.

    >>> import email.message, datetime as dt
    >>> m = email.message.EmailMessage()
    >>> m["To"] = "max@example.org"; m["Cc"] = "erika@example.org"
    >>> m["Bcc"] = "um@example.org"; m["Subject"] = "WSJ27 Abmeldung YP 1 Max"
    >>> m["Message-ID"] = "<1@example.org>"
    >>> at = dt.datetime(2026, 11, 15, 14, 3, 12, tzinfo=dt.timezone(dt.timedelta(hours=1)))
    >>> r = build_report(m, {"deregistration_id": 7}, sent_at=at, script="x.py")
    >>> r["email_sent_at"], r["email_to"], r["email_cc"], r["email_bcc"], r["email_message_id"]
    ('2026-11-15T14:03:12+01:00', ['max@example.org'], ['erika@example.org'], ['um@example.org'], '<1@example.org>')
    >>> r["deregistration"], r["script"]
    ({'deregistration_id': 7}, 'x.py')
    >>> import base64
    >>> base64.b64decode(r["email_eml_base64"]) == m.as_bytes()
    True
    """
    return {
        "email_sent_at": sent_at.isoformat(timespec="seconds"),
        "email_to": addresses(msg, "To"),
        "email_cc": addresses(msg, "Cc"),
        "email_bcc": addresses(msg, "Bcc"),
        "email_subject": str(msg["Subject"] or ""),
        "email_message_id": str(msg["Message-ID"] or ""),
        "script": script,
        "deregistration": dict(deregistration),
        "email_eml_base64": _base64.b64encode(msg.as_bytes()).decode("ascii"),
    }


def send_report(
    report_cb: _collections_abc.Callable[[dict], dict | None],
    report: dict,
    *,
    report_path: _pathlib.Path,
    person_id: int,
    mailing_key: str,
) -> None:
    """Send *report* through *report_cb* (a ``HitobitoClient`` method); if
    that fails the mail is out already, so the report is kept in
    *report_path* to be sent again (``--report-sent``) and the script ends
    with status 1."""
    try:
        result = report_cb(report)
    except Exception as exc:
        report_path.write_text(_json.dumps(report, indent=2, ensure_ascii=False))
        _LOGGER.error(
            "The mail went out, but Hitobito did not record it: %s\n"
            "  Report kept in %s -- send it again with: --report-sent %s %s",
            exc,
            report_path,
            report_path,
            person_id,
        )
        raise SystemExit(1) from exc
    if result is None:
        _LOGGER.info("[dry-run] Report of the mail not sent to Hitobito")
        return
    mailing = result.get(mailing_key) or {}
    _LOGGER.info(
        "Hitobito %s the mail: log entry %s%s",
        "already knew" if result.get("duplicate") else "recorded",
        mailing.get("log_version_id"),
        f", person note {mailing['note_id']}" if mailing.get("note_id") else "",
    )
