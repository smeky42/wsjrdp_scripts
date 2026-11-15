#!/usr/bin/env -S uv run
"""Send a person the Abmelde-Formular (the declaration of a withdrawal) by e-mail.

Hitobito notes the deregistration on the person (status
``deregistration_noted``), makes the form and answers everything the mail
needs -- who signs, the role and name for the subject, the deadline, the
amounts, the sender, To, Cc and Bcc (for a youth participant or unit
leader the Support-CMT addresses of the unit, the last unit for a person on
a waiting list). A form without an agreed compensation is not sent. The script builds the mail from
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
import json
import logging
import pathlib as _pathlib
import sys

import wsjrdp2027


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


def _de_date(iso: str | None) -> str:
    """
    >>> _de_date("2026-11-29"), _de_date(None)
    ('29.11.2026', '')
    """
    return _datetime.date.fromisoformat(iso).strftime("%d.%m.%Y") if iso else ""


def _problems(
    deregistration: dict, *, dry_run: bool = False
) -> tuple[list[dict], list[dict]]:
    """The errors and warnings Hitobito found for the form
    (``deregistration.form_check``), and the one a dry run adds: it cannot make
    the form.

    >>> d = {"form": {"created_date": None}, "form_check": {
    ...     "errors": [{"code": "no_birthday", "message": "Geburtsdatum fehlt."}],
    ...     "warnings": [{"code": "no_issue", "message": "Kein Ticket."}]}}
    >>> errors, warnings = _problems(d, dry_run=True)
    >>> [e["code"] for e in errors], [w["code"] for w in warnings]
    (['no_birthday', 'dry_run_no_form'], ['no_issue'])
    """
    check = deregistration.get("form_check") or {}
    errors = list(check.get("errors") or [])
    warnings = list(check.get("warnings") or [])
    if dry_run and not (deregistration.get("form") or {}).get("created_date"):
        errors.append(
            {
                "code": "dry_run_no_form",
                "message": "the form is not made yet, and a dry run does not make it: "
                "make it on the Abmeldung page or run without --dry-run",
            }
        )
    return errors, warnings


def _log_problems(name: str, errors: list[dict], warnings: list[dict]) -> None:
    for error in errors:
        _LOGGER.error("%s: %s (%s)", name, error.get("message"), error.get("code"))
    for warning in warnings:
        _LOGGER.warning(
            "%s: %s (%s)", name, warning.get("message"), warning.get("code")
        )


def _eur(amount: float | None, currency: str) -> str:
    """
    >>> _eur(402.95, "EUR"), _eur(1440, "EUR"), _eur(None, "EUR")
    ('402.95 EUR', '1440.00 EUR', '-')
    """
    return "-" if amount is None else f"{amount:.2f} {currency}"


def report_sent(
    client, person_id: int, report: dict, *, report_path: _pathlib.Path
) -> None:
    """Report the mail; keep the report in *report_path* if that fails."""
    wsjrdp2027.deregistration_mails.send_report(
        lambda r: client.report_deregistration_form_sent(person_id, r),
        report,
        report_path=report_path,
        person_id=person_id,
        mailing_key="form_mailing",
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
    _LOGGER.info("Reply-To: %s", ", ".join(mail.get("reply_to") or []) or "-")
    _LOGGER.info("To: %s", ", ".join(mail["to"]) or "-")
    _LOGGER.info("Cc: %s", ", ".join(mail["cc"]) or "-")
    _LOGGER.info(
        "Bcc: %s%s",
        ", ".join(mail["bcc"]) or "-",
        f" (unit {unit['name']}, {unit['source']})" if unit else "",
    )

    # Everything that stands in the way, and everything worth a second look,
    # in one go -- as Hitobito checked them -- then the script ends, or asks.
    name = deregistration["role_id_name"]
    errors, warnings = _problems(deregistration, dry_run=ctx.dry_run)
    _log_problems(name, errors, warnings)
    if errors:
        _LOGGER.error(
            "Ending script: %s to fix first%s",
            f"{len(errors)} problem{'s' if len(errors) > 1 else ''}",
            f" ({len(warnings)} warning{'s' if len(warnings) > 1 else ''} above as well)"
            if warnings
            else "",
        )
        return 1
    if warnings and not ctx.console_confirm(
        f"{name}: {'; '.join(w['message'] for w in warnings)} Send anyway?",
        default=False,
    ):
        _LOGGER.info("Ending script: not confirmed")
        return 0

    # The app notes the deregistration on the person and makes the form -- or,
    # if the form would not be complete after all, refuses with every error
    # and warning; a dry run sends no POST and keeps what it read.
    try:
        answer = client.prepare_deregistration_form(person_id)
    except wsjrdp2027.HitobitoApiError as exc:
        _log_problems(name, exc.errors, exc.warnings)
        _LOGGER.error("Ending script: Hitobito refused to prepare the form (%s)", exc)
        return 1
    if answer is not None:
        prepared = answer["deregistration"]
        for key, label in (("status", "Status"), ("deregistration_status", "Step")):
            if prepared[key] != deregistration[key]:
                _LOGGER.info("%s: %s -> %s", label, deregistration[key], prepared[key])
        if not deregistration["form"]["created_date"]:
            _LOGGER.info("Made the form on %s", prepared["form"]["created_date"])
        deregistration = prepared
        mail = deregistration["mail"]

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
        # Reply-To as Hitobito names it; none means replies go to From.
        batch_config.email_reply_to = list(mail.get("reply_to") or []) or None
        # The subject is Hitobito's, taken as it is.
        batch_config.email_subject = wsjrdp2027.deregistration_mails.literal_subject(
            mail["subject"]
        )
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
    report = wsjrdp2027.deregistration_mails.build_report(
        msg,
        deregistration,
        sent_at=_datetime.datetime.now().astimezone(),
        script=_SCRIPT,
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
