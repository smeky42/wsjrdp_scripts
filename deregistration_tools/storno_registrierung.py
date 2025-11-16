#!/usr/bin/env -S uv run
"""Tool to produce a cancellation request letter."""

from __future__ import annotations

import contextlib as _contextlib
import logging
import pathlib as _pathlib
import smtplib as _smtplib
import sys
import textwrap as _textwrap

import pandas as pd
import wsjrdp2027


SELFDIR = _pathlib.Path(__file__).parent.resolve()


_LOGGER = logging.getLogger()

_MAIL_CC = []
_MAIL_BCC = []


def parse_args(argv=None):
    import argparse
    import sys

    if argv is None:
        argv = sys.argv
    p = argparse.ArgumentParser()
    p.add_argument(
        "--email",
        action="store_true",
        default=False,
        help="Send the created PDF out. Uses mailings_to, mailings_cc. Copies",
    )
    p.add_argument(
        "--no-email",
        dest="email",
        action="store_false",
        help="Create eml files, but do not send out the special agreement via SMTP.",
    )
    p.add_argument(
        "--today",
        metavar="TODAY",
        default="TODAY",
        help="Run as if the current date is TODAY",
    )
    p.add_argument(
        "--print-at",
        metavar="DATE",
        default=None,
        help="Run as if all people had print_at = DATE",
    )
    p.add_argument("--issue")
    p.add_argument("--greeting-name")
    p.add_argument("id")
    return p.parse_args(argv[1:])


def _merge_mail_to(*args) -> list[str] | None:
    from wsjrdp2027 import _util

    if all(arg is None for arg in args):
        return None
    addrs = []
    for arg in args:
        addrs.extend(_util.to_str_list(arg) or [])
    return _util.dedup(addrs)  # type: ignore


def send_pdf_via_email(
    args,
    ctx: wsjrdp2027.WsjRdpContext,
    smtp_client: _smtplib.SMTP | None,
    row: pd.Series,
    pdf_path: _pathlib.Path,
    *,
    pdf_filename: str | None = None,
    eml_path: _pathlib.Path | None = None,
) -> None:
    import email.message
    import email.utils

    id = row["id"]
    issue = args.issue
    short_full_name = row["short_full_name"]

    pdf_bytes = pdf_path.read_bytes()
    msg = email.message.EmailMessage()
    subject = f"WSJ27 Storno Registrierung {short_full_name} (id {id})"
    cc = _merge_mail_to(row["mailing_cc"], _MAIL_CC)
    bcc = _MAIL_BCC
    if issue:
        subject = f"{subject} {issue}"
        bcc = _merge_mail_to(bcc, "info@worldscoutjamboree.de")
    msg["Subject"] = subject
    msg["From"] = "anmeldung@worldscoutjamboree.de"
    if row["mailing_to"]:
        msg["To"] = row["mailing_to"]
    if cc:
        msg["Cc"] = cc
    if bcc:
        msg["Bcc"] = bcc
    msg["Reply-To"] = ["info@worldscoutjamboree.de"]
    msg["Date"] = email.utils.formatdate(localtime=True)

    names = get_names_from_row(row)

    hello_line = ", ".join(f"Hallo {n}" for n in names)

    if row["status"] in ["deregistration_noted", "deregistered"]:
        cancellation_noted = (
            " Wir haben die Stornierung auch schon im Anmeldesystem vermerkt."
        )
    else:
        cancellation_noted = ""

    msg.set_content(
        f"""{hello_line},

schade, dass {row["greeting_name"] if len(names) > 1 else "Du"} nicht mit zum Jamboree {"kommt" if len(names) > 1 else "kommst"}. Um die Abmeldung vornehmen zu können, benötigen wir das angehängte Stornierungs-Formular unterschreiben zurück.{cancellation_noted} Wenn wir das unterschriebene Formular per E-Mail zurück erhalten haben, setzen wir die Registrierung auf Abgemeldet und {"ihr bekommt für eure" if len(names) > 1 else "du bekommst für deine"} Unterlagen eine Bestätigung der Stornierung.

Liebe Grüße und Gut Pfad
Daffi

-- """
        + """
World Scout Jamboree 2027 Poland
German Contingent
Head of Finance Team

Ring deutscher Pfadfinder*innenverbände e.V. (rdp)
Chausseestr. 128/129
10115 Berlin

"""
    )
    if not pdf_filename:
        pdf_filename = pdf_path.name
    msg.add_attachment(
        pdf_bytes, maintype="application", subtype="pdf", filename=pdf_filename
    )

    if not eml_path:
        eml_path = ctx.make_out_path(
            f"storno_registrierung_{{{{ filename_suffix }}}}.{id}.eml"
        )
    with open(eml_path, "wb") as f:
        f.write(msg.as_bytes())
    _LOGGER.info("Wrote %s", eml_path)
    if smtp_client:
        ctx.require_approval_to_send_email_in_prod()
        smtp_client.send_message(msg)
    else:
        _LOGGER.warning("Skip actual email sending (--no-email given)")


def get_names_from_row(row: pd.Series) -> list[str]:
    of_legal_age = row["age"] >= 18
    full_name = row["full_name"]
    if of_legal_age:
        names = [full_name]
    else:
        names = [row["additional_contact_name_a"]]
        if not row["additional_contact_single"]:
            names.append(row["additional_contact_name_b"])
        names.append(full_name)
    return names


def create_cancellation_request(
    args,
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    row: pd.Series,
    smtp_client: _smtplib.SMTP | None = None,
    eml_path: _pathlib.Path | None = None,
) -> None:
    import json

    id = row["id"]
    short_full_name = row["short_full_name"]
    full_name = row["full_name"]

    _LOGGER.info(
        "id: %s, full_name: %s, row:\n%s",
        id,
        full_name,
        _textwrap.indent(row.to_string(), "  | "),
    )

    id_name = f"{id} {short_full_name}"
    out_name_base = f"WSJ27 Storno Registrierung {id_name}"
    if args.issue:
        out_name_base += f" {args.issue}"
    pdf_filename = f"{out_name_base}.pdf"
    pdf_path = ctx.make_out_path(out_name_base + " {{ filename_suffix }}.pdf")

    issue = args.issue or ""

    person_id_line = (
        f"(Anmeldungs-ID {id} / Vorgang Stornierung: {issue})"
        if issue
        else f"(Anmeldungs-Nummer {id})"
    )

    names = get_names_from_row(row)

    wsjrdp2027.typst_compile(
        SELFDIR / "cancellation_request.typ",
        output=pdf_path,
        sys_inputs={
            "hitobitoid": str(row["id"]),
            "full_name": full_name,
            "birthday_de": row["birthday_de"],
            "issue": issue,
            "person_id_line": person_id_line,
            "names": json.dumps(names),
        },
    )
    _LOGGER.info(f"Wrote {pdf_path}")
    send_pdf_via_email(
        args,
        ctx,
        smtp_client,
        row,
        pdf_path,
        pdf_filename=pdf_filename,
        eml_path=eml_path,
    )


def main(argv=None):
    args = parse_args(argv)
    ctx = wsjrdp2027.WsjRdpContext(
        setup_logging=True,
        out_dir="data/storno_registrierung{{ kind | omit_unless_prod | upper | to_ext }}",
    )
    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(
            conn,
            status=None,
            where=f"""people.id = {args.id}""",
            today=args.today,
            print_at=args.print_at,
            exclude_deregistered=False,
        )

    row = df.iloc[0]
    id = row["id"]
    id_and_name = f"{id} {row['short_full_name']}"
    ctx.out_dir = ctx.out_dir / id_and_name

    out_base = ctx.make_out_path("storno_registrierung_{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    eml_path = out_base.with_suffix(f".{id}.eml")
    ctx.configure_log_file(log_filename)

    with _contextlib.ExitStack() as exit_stack:
        if args.email:
            smtp_client = exit_stack.enter_context(ctx.smtp_login())
        else:
            smtp_client = None

        create_cancellation_request(
            args, ctx=ctx, row=row, smtp_client=smtp_client, eml_path=eml_path
        )


if __name__ == "__main__":
    sys.exit(main())
