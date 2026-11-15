#!/usr/bin/env -S uv run
"""Send a person the Abmelde-Formular (the declaration of a withdrawal) by e-mail.

Hitobito makes the form and answers everything the mail needs -- who signs,
the role and name for the subject, the deadline, the amounts, the sender,
To, Cc and Bcc (for a youth participant or unit leader the Support-CMT
addresses of the unit, the last unit for a person on a waiting list). A form
without an agreed compensation is not sent. The script builds the mail from
that answer, attaches the PDF Hitobito made, sends it and reports the mail
back, which leaves a person note, a log entry and an element of the person's
form mailings in Hitobito.

API key: purpose ``deregistration`` (config key
``hitobito_api_token_deregistration``), scopes ``people`` and ``people:log``,
an acting person who may see the person's log. The wagon's
``doc/wsjrdp/deregistration.md`` describes the endpoints.

If the report fails after the mail went out, the report is kept as a JSON
file next to the EML; ``--report-sent FILE`` sends it again.
"""

from __future__ import annotations

import datetime as _datetime
import email.utils as _email_utils
import json
import logging
import pathlib as _pathlib
import sys
import typing as _typing

import wsjrdp2027


if _typing.TYPE_CHECKING:
    import email.message as _email_message


SELFDIR = _pathlib.Path(__file__).parent.resolve()
_SELF_NAME = _pathlib.Path(__file__).stem
_SCRIPT = f"registration_tools/{_SELF_NAME}.py"
_API_PURPOSE = "deregistration"


_LOGGER = logging.getLogger()


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--skip-email", action="store_true", default=None)
    p.add_argument(
        "--open-editor",
        action="store_true",
        default=False,
        help="Open E-Mail body content in editor before preparing EML file",
    )
    p.add_argument(
        "--report-sent",
        metavar="FILE",
        type=_pathlib.Path,
        default=None,
        help="Only report a mail that went out already: FILE is the report "
        "a failed run kept (…form_sent.json).",
    )
    p.add_argument("person_id")
    return p


def _addresses(msg: _email_message.EmailMessage, header: str) -> list[str]:
    """The plain addresses of *header*, without display names.

    >>> import email.message
    >>> m = email.message.EmailMessage()
    >>> m["To"] = "Max <max@example.org>, erika@example.org"
    >>> _addresses(m, "To"), _addresses(m, "Cc")
    (['max@example.org', 'erika@example.org'], [])
    """
    values = msg.get_all(header) or []
    return [addr for _name, addr in _email_utils.getaddresses(values) if addr]


def build_report(
    msg: _email_message.EmailMessage,
    deregistration: dict,
    *,
    sent_at: _datetime.datetime,
) -> dict[str, _typing.Any]:
    """What ``form_sent.json`` is told about a mail that went out.

    >>> import email.message, datetime as dt
    >>> m = email.message.EmailMessage()
    >>> m["To"] = "max@example.org"; m["Cc"] = "erika@example.org"
    >>> m["Bcc"] = "um@example.org"; m["Subject"] = "WSJ27 Abmeldung YP 1 Max"
    >>> m["Message-ID"] = "<1@example.org>"
    >>> at = dt.datetime(2026, 11, 15, 14, 3, 12, tzinfo=dt.timezone(dt.timedelta(hours=1)))
    >>> r = build_report(m, {"person_id": 1}, sent_at=at)
    >>> r["email_sent_at"], r["email_to"], r["email_cc"], r["email_bcc"], r["email_message_id"]
    ('2026-11-15T14:03:12+01:00', ['max@example.org'], ['erika@example.org'], ['um@example.org'], '<1@example.org>')
    """
    return {
        "email_sent_at": sent_at.isoformat(timespec="seconds"),
        "email_to": _addresses(msg, "To"),
        "email_cc": _addresses(msg, "Cc"),
        "email_bcc": _addresses(msg, "Bcc"),
        "email_subject": str(msg["Subject"] or ""),
        "email_message_id": str(msg["Message-ID"] or ""),
        "script": _SCRIPT,
        "deregistration": deregistration,
    }


def _de_date(iso: str | None) -> str:
    """
    >>> _de_date("2026-11-29"), _de_date(None)
    ('29.11.2026', '')
    """
    return _datetime.date.fromisoformat(iso).strftime("%d.%m.%Y") if iso else ""


def _compensation_error(deregistration: dict) -> str | None:
    """Why the form must not go out: without an agreed compensation
    (Entschädigung on the Abmeldung page) it leaves the refund and the claim to
    be filled in by hand.

    >>> _compensation_error({"amounts": {"deregistration_actual_compensation": None}})
    'no compensation is entered (Entschädigung on the Abmeldung page): enter it before the form is sent'
    >>> _compensation_error({"amounts": {"deregistration_actual_compensation": 0}}) is None
    True
    """
    amounts = deregistration.get("amounts") or {}
    if amounts.get("deregistration_actual_compensation") is None:
        return (
            "no compensation is entered (Entschädigung on the Abmeldung page): "
            "enter it before the form is sent"
        )
    return None


def _eur(amount: float | None, currency: str) -> str:
    """
    >>> _eur(402.95, "EUR"), _eur(1440, "EUR"), _eur(None, "EUR")
    ('402.95 EUR', '1440.00 EUR', '-')
    """
    return "-" if amount is None else f"{amount:.2f} {currency}"


def _checks(deregistration: dict, *, today: _datetime.date) -> list[str]:
    """Why sending may be a mistake: no helpdesk ticket is entered (the mail
    lands in no ticket), the deadline is over, or the form went out before.

    >>> import datetime as dt
    >>> d = {"issue": None, "form": {"created_date": "2026-11-01",
    ...      "reply_due_date": "2026-11-14"}, "form_mailings": []}
    >>> _checks(d, today=dt.date(2026, 11, 15))
    ['no helpdesk ticket is entered', "the form's deadline 14.11.2026 is over"]
    >>> _checks(d | {"issue": "HELP-1"}, today=dt.date(2026, 11, 14))
    []
    """
    reasons = []
    if not deregistration.get("issue"):
        reasons.append("no helpdesk ticket is entered")
    form = deregistration.get("form") or {}
    if form.get("created_date") and (due := form.get("reply_due_date")):
        if _datetime.date.fromisoformat(due) < today:
            reasons.append(f"the form's deadline {_de_date(due)} is over")
    for mailing in deregistration.get("form_mailings") or []:
        reasons.append(
            f"the form went out before ({mailing.get('email_sent_at')} to "
            f"{', '.join(mailing.get('email_to') or [])})"
        )
    return reasons


def report_sent(
    client, person_id: int, report: dict, *, report_path: _pathlib.Path
) -> None:
    """Report the mail; keep the report in *report_path* if that fails."""
    try:
        result = client.report_deregistration_form_sent(person_id, report)
    except Exception as exc:
        report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
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
    mailing = result.get("form_mailing") or {}
    _LOGGER.info(
        "Hitobito %s the mail: log entry %s, person note %s",
        "already knew" if result.get("duplicate") else "recorded",
        mailing.get("log_version_id"),
        mailing.get("note_id"),
    )


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    )
    person_id = int(ctx.parsed_args.person_id)
    # Fetched first: without the API key nothing else is done.
    client = ctx.get_hitobito_client(_API_PURPOSE)

    if report_file := ctx.parsed_args.report_sent:
        report = json.loads(report_file.read_text())
        report_sent(client, person_id, report, report_path=report_file)
        return 0

    deregistration = client.get_deregistration(person_id)
    _LOGGER.info(
        "Deregistration %s: kind %s, issue %s, status %s, contract %s, form made %s",
        deregistration["role_id_name"],
        deregistration["kind"],
        deregistration["issue"] or "-",
        deregistration["status"],
        deregistration["contract_status"],
        deregistration["form"]["created_date"] or "not yet",
    )
    if not deregistration["available"]:
        _LOGGER.error("No Abmelde-Formular: %s", deregistration["not_available_reason"])
        return 1
    if (error := _compensation_error(deregistration)) is not None:
        _LOGGER.error("%s: %s", deregistration["role_id_name"], error)
        return 1
    if reasons := _checks(deregistration, today=ctx.today):
        for reason in reasons:
            _LOGGER.warning("%s: %s", deregistration["role_id_name"], reason)
        if not ctx.console_confirm(
            f"{deregistration['role_id_name']}: {'; '.join(reasons)}. Send anyway?",
            default=False,
        ):
            _LOGGER.info("Ending script: not confirmed")
            return 0
    if not deregistration["form"]["created_date"]:
        if ctx.dry_run:
            _LOGGER.error(
                "The form is not made yet, and a dry run does not make it: "
                "make it on the Abmeldung page or run without --dry-run"
            )
            return 1
        deregistration = client.create_deregistration_form(person_id)
        _LOGGER.info("Made the form on %s", deregistration["form"]["created_date"])

    amounts = deregistration["amounts"]
    currency = amounts["base_currency"]
    _LOGGER.info(
        "Fee %s, paid %s, compensation %s (T&R %s), refund %s, open %s",
        *(
            _eur(amounts[key], currency)
            for key in (
                "total_fee",
                "amount_paid",
                "deregistration_actual_compensation",
                "deregistration_contractual_compensation",
                "deregistration_refund",
                "deregistration_open",
            )
        ),
    )
    mail = deregistration["mail"]
    unit = mail.get("unit")
    _LOGGER.info("From: %s", mail["from"])
    _LOGGER.info("To: %s", ", ".join(mail["to"]) or "-")
    _LOGGER.info("Cc: %s", ", ".join(mail["cc"]) or "-")
    _LOGGER.info(
        "Bcc: %s%s",
        ", ".join(mail["bcc"]) or "-",
        f" (unit {unit['name']}, {unit['source']})" if unit else "",
    )

    pdf_name = deregistration["form"]["file_name"]
    pdf_bytes = client.get_deregistration_form_pdf(person_id)

    with ctx.psycopg_connect() as conn:
        p = ctx.load_person_for_id(person_id, conn=conn)
        assert p.df is not None
        # The recipients the app names, not the ones the people query works out.
        p["mailing_to"] = list(mail["to"]) or None
        p["mailing_cc"] = list(mail["cc"]) or None
        ctx.out_dir = ctx.out_dir / p.id_and_name

        batch_config = wsjrdp2027.BatchConfig.from_yaml(
            SELFDIR / f"{_SELF_NAME}.yml",
            name=f"WSJ27 Abmeldung {deregistration['role_id_name']}",
            where=wsjrdp2027.PeopleWhere(id=p.id, exclude_deregistered=False),
        ).replace(dry_run=ctx.dry_run, skip_email=ctx.parsed_args.skip_email)
        batch_config.email_from = mail["from"]
        batch_config.extend_extra_email_bcc(mail["bcc"])
        batch_config.jinja_extra_globals = {
            "deregistration": deregistration,
            "reply_due_date_de": _de_date(deregistration["form"]["reply_due_date"]),
        }

        out_base = ctx.make_out_path(batch_config.name)
        ctx.configure_log_file(out_base.with_suffix(".log"))
        pdf_path = ctx.make_out_path(pdf_name)
        pdf_path.write_bytes(pdf_bytes)
        _LOGGER.info("Wrote %s", pdf_path)

        def attach(prepared: wsjrdp2027.PreparedEmailMessage) -> None:
            assert prepared.message is not None
            prepared.message.add_attachment(
                pdf_bytes, maintype="application", subtype="pdf", filename=pdf_name
            )
            prepared.eml_name = f"{prepared.mailing_name}.eml"

        mailing = batch_config.prepare(
            p,
            msg_cb=attach,
            out_dir=ctx.out_dir,
            open_editor=ctx.parsed_args.open_editor,
        )
        ctx.update_db_and_send_mailing(mailing, conn=conn, zip_eml=False)

    if skip := mailing.get_skip_email_reasons(dry_run=ctx.dry_run):
        _LOGGER.info("Nothing reported to Hitobito (%s)", ", ".join(skip))
        return 0
    msg = mailing.messages[0].message
    assert msg is not None
    report = build_report(
        msg, deregistration, sent_at=_datetime.datetime.now().astimezone()
    )
    report_sent(
        client,
        person_id,
        report,
        report_path=out_base.with_name(out_base.name + ".form_sent.json"),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
