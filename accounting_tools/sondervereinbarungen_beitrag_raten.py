#!/usr/bin/env -S uv run
"""Tool to produce a financtial Sondervereinbarung.

Uses the rules stored in the table wsj27_rdp_fee_rules.
"""

from __future__ import annotations

import contextlib as _contextlib
import logging
import pathlib as _pathlib
import re
import shutil as _shutil
import smtplib as _smtplib
import sys
import textwrap as _textwrap
import typing

import pandas as pd
import wsjrdp2027


if typing.TYPE_CHECKING:
    import docx as _docx


_LOGGER = logging.getLogger()

_MAIL_CC = [
    "kl@worldscoutjamboree.de",
    "sebastian.becker@rdp-bund.de",
]
_MAIL_BCC = [
    "finance-team@worldscoutjamboree.de",
]


def parse_args(argv=None):
    import argparse
    import sys

    if argv is None:
        argv = sys.argv
    p = argparse.ArgumentParser()
    p.add_argument("--rdp-representative", choices=["DF"], required=False)
    p.add_argument(
        "--pdf",
        action="store_true",
        default=False,
        help="Create a PDF of the created docx. Required to send the PDF via email",
    )
    p.add_argument("--no-pdf", dest="pdf", action="store_false")
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
    p.add_argument("id", nargs="+")
    return p.parse_args(argv[1:])


_RDP_REPRESENTATIVE_TO_TOWN = {
    "CW": "Bamberg",
    "DF": "München",
    "FU": "München",
    "IH": "Bad Kreuznach",
}

_RDP_REPRESENTATIVE_TO_NAME = {
    "CW": "Carolin Windisch",
    "DF": "David Fritzsche",
    "FU": "Felix Unger",
    "IH": "Ines Höfig",
}


def load_docx(name: str) -> _docx.Document:
    import os

    import docx as _docx

    p = os.path.join(os.path.dirname(__file__), name)
    return _docx.Document(p)  # type: ignore


def convert_docx_to_pdf(docx_path, /, pdf_name=None) -> _pathlib.Path:
    import os

    import docx2pdf

    docx_path = _pathlib.Path(docx_path)
    if pdf_name:
        pdf_path = docx_path.parent / pdf_name
    else:
        pdf_path = os.path.splitext(docx_path)[0] + ".pdf"
    docx2pdf.convert(docx_path, output_path=pdf_path)
    return _pathlib.Path(pdf_path)


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
) -> None:
    import email.message

    id = row["id"]
    issue = row["custom_installments_issue"]
    short_full_name = row["short_full_name"]

    pdf_bytes = pdf_path.read_bytes()
    msg = email.message.EmailMessage()
    subject = f"WSJ27 Sondervereinbarung {short_full_name} (id {id})"
    cc = _merge_mail_to(row["mailing_cc"], _MAIL_CC)
    bcc = _MAIL_BCC
    if issue:
        subject = f"{subject} {issue}"
        bcc = _merge_mail_to(bcc, "info@worldscoutjamboree.de")
    msg["Subject"] = subject
    msg["From"] = "anmeldung@worldscoutjamboree.de"
    msg["To"] = row["mailing_to"]
    if cc:
        msg["Cc"] = cc
    if bcc:
        msg["Bcc"] = bcc
    msg["Reply-To"] = ["info@worldscoutjamboree.de"]
    msg.set_content(
        f"""Hallo {row["greeting_name"]},

in der angehängten PDF-Datei findest du die Sondervereinbarung zum Vertrag für deinen Ratenplan und ein entsprechend angepasstes SEPA-Mandat.

Du musst jetzt beide Seiten des angehängten Dokuments unterschreiben und als eine Datei anstelle des regulären SEPA-Mandats im Anmeldesystem hochladen. Wenn du dann auch die restlichen Dokumente (Anmeldung, Medizinbogen, Fotoeinwilligung, usw.) hochgeladen hast, werden wir deine Anmeldung überprüfen - genau wie bei allen anderen Anmeldungen.

Wir bestätigen deine Anmeldung nachdem der Gastgeber in Polen uns die Anmeldung des gesamten Kontingents bestätigt hat.


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

Email: david.fritzsche@worldscoutjamboree.de
Web: www.worldscoutjamboree.de
Instagram: @wsjrdp
"""
    )
    if not pdf_filename:
        pdf_filename = pdf_path.name
    msg.add_attachment(
        pdf_bytes, maintype="application", subtype="pdf", filename=pdf_filename
    )

    person_out_dir = f"{id} {short_full_name}"
    eml_file = ctx.make_out_path(
        f"{person_out_dir}/sondervereinbarung_beitrag_raten_{{{{ filename_suffix }}}}.{id}.eml"
    )
    with open(eml_file, "wb") as f:
        f.write(msg.as_bytes())
    _LOGGER.info("Wrote %s", eml_file)
    if args.email:
        assert smtp_client is not None
        smtp_client.send_message(msg)
    else:
        _LOGGER.warning("Skip actual email sending (--no-email given)")


def installments_replacements_from_row(row: pd.Series, keys) -> dict[str, str]:
    import re

    def year_month_to_key(year, month):
        return f"i{str(year)[-2:]}_{month}"

    def to_eur(cents):
        return wsjrdp2027.format_cents_as_eur_de(cents, zero_cents="")

    keys_set = set(keys)
    installments_cents = row["installments_cents"]
    replacements = {
        key: to_eur(cents)
        for (year, month), cents in installments_cents.items()
        if (key := year_month_to_key(year, month)) in keys_set
    }
    for key in sorted(set(keys) - set(replacements)):
        if m := re.fullmatch("i[0-9]+_[0-9]+", key):
            replacements[key] = to_eur(0)
    replacements = {
        k: v for k, v in sorted(replacements.items(), key=lambda item: item[0])
    }
    missing = {
        f"{year}-{month:02d}": to_eur(cents)
        for (year, month), cents in installments_cents.items()
        if (key := year_month_to_key(year, month)) not in keys_set
    }
    if missing:
        err_msg = f"Einige Raten werden nicht gedruckt: {missing}"
        _LOGGER.error("%s", err_msg)
        raise RuntimeError(err_msg)
    return replacements


def log_replacements(row, replacements):
    max_key_len = max(len(k) for k in replacements)
    replacements_str = "  " + "\n  ".join(
        f"{k.rjust(max_key_len)}: {v!r}" for k, v in replacements.items()
    )
    _LOGGER.info(
        "Replacements for %s %s:\n%s", row["id"], row["full_name"], replacements_str
    )


def create_special_agreement(
    args,
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    row: pd.Series,
    smtp_client: _smtplib.SMTP | None = None,
) -> None:
    import python_docx_replace

    rdp_representative_town = _RDP_REPRESENTATIVE_TO_TOWN.get(args.rdp_representative)
    rdp_representative_name = _RDP_REPRESENTATIVE_TO_NAME.get(args.rdp_representative, "")  # fmt: skip

    id = row["id"]
    short_full_name = row["short_full_name"]
    full_name = row["full_name"]
    _LOGGER.info(
        "id: %s, full_name: %s, row:\n%s",
        id,
        full_name,
        _textwrap.indent(row.to_string(), "  | "),
    )
    issue = row["custom_installments_issue"]
    total_fee_cents = row["total_fee_cents"]
    installments_cents = row["installments_cents"]
    sum_installments_cents = sum((installments_cents or {}).values())
    if total_fee_cents != sum_installments_cents:
        err_msg = f"""Raten ergeben nicht zu zahlenden Betrag:
  Teilnahmebeitrag (in cents): {total_fee_cents}
  Summe der Raten (in cents): {sum_installments_cents}
  Raten (in cents): {installments_cents}
"""
        _LOGGER.error("%s", err_msg)
        raise RuntimeError(err_msg)
    is_jsf = row["total_fee_reduction_comment"] == "JSF"
    if is_jsf:
        special_agreement_title = (
            "Sondervereinbarung Ratenzahlungen & Jamboree Solidarity Fund (JSF)"
        )
    elif row["total_fee_reduction_cents"] > 0:
        special_agreement_title = "Sondervereinbarung Ratenzahlungen & Teilnahmebeitrag"
    else:
        special_agreement_title = "Sondervereinbarung Ratenzahlungen"
    person_id_line = (
        f"(Anmeldungs-Nummer {id} / Vorgang Sondervereinbarung: {issue})"
        if issue
        else f"(Anmeldungs-Nummer {id})"
    )
    total_fee_reduction_cents = int(row["total_fee_reduction_cents"])
    rdp_representative_town_date = f"{rdp_representative_town}, {row['today_de']}" if rdp_representative_town else ""  # fmt: skip

    if args.rdp_representative:
        template_filename = (
            f"WSJ27-Sondervereinbarung-Beitrag-Raten-{args.rdp_representative}.docx"
        )
    else:
        template_filename = f"WSJ27-Sondervereinbarung-Beitrag-Raten.docx"
    doc = load_docx(template_filename)
    doc_keys = python_docx_replace.docx_get_keys(doc)
    replacements = installments_replacements_from_row(row, keys=doc_keys)
    replacements |= {
        "creditor_id": wsjrdp2027.CREDITOR_ID,
        "total_fee": wsjrdp2027.format_cents_as_eur_de(row["total_fee_cents"]),
        "total_fee_reduction": wsjrdp2027.format_cents_as_eur_de(
            total_fee_reduction_cents
        ),
        "rdp_representative_name": rdp_representative_name,
        "rdp_representative_town_date": rdp_representative_town_date,
        "special_agreement_title": special_agreement_title,
        "sepa_mandate_title": f"SEPA-Mandat – Teilnahmebeitrag {full_name} (id {id})",
        "footer_special_agreement": f"Sondervereinbarung Ratenzahlung {id} – {full_name}",
        "footer_sepa_mandate": f"SEPA-Mandat {id} – {full_name}",
        "person_id_line": person_id_line,
    }
    hide_contact_b = bool(row["additional_contact_single"])
    for key in (k for k in doc_keys if k not in replacements):
        if (val := row.get(key)) is not None:
            replacements[key] = val
        elif hide_contact_b and re.fullmatch("additional_contact_.*_b", key):
            replacements[key] = "LEER"
    log_replacements(row, replacements)
    if missing := sorted(set(doc_keys) - set(replacements)):
        _LOGGER.error("Missing keys in document: %s", missing)
        raise RuntimeError(f"Missing keys in document: {', '.join(missing)}")

    python_docx_replace.docx_replace(doc, **replacements)
    blocks_kwargs = {
        "contact_b": not hide_contact_b,
        "vereinbarung_beitrag": (total_fee_reduction_cents > 0),
    }
    python_docx_replace.docx_blocks(doc, **blocks_kwargs)
    id_name = f"{id} {short_full_name}"
    out_name_base = "WSJ27-Sondervereinbarung-"
    out_name_base += "JSF" if is_jsf else "Ratenplan"
    out_name_base += f" {id_name}"
    pdf_filename = f"{out_name_base}.pdf"
    out_name_base = f"{id_name}/{out_name_base}"
    out_name = ctx.make_out_path(out_name_base + " {{ filename_suffix }}.docx")
    pdf_name = ctx.make_out_path(out_name_base + " {{ filename_suffix }}.pdf")
    tmp_docx = ctx.make_out_path("WSJ27-Sondervereinbarung.temp.docx")
    doc.save(tmp_docx)
    try:
        _LOGGER.info(f"Wrote temporary docx to {tmp_docx}")
        _shutil.copy(tmp_docx, out_name)
        _LOGGER.info(f"Wrote {out_name}")
        if args.pdf:
            tmp_pdf = convert_docx_to_pdf(tmp_docx)
            tmp_pdf.rename(pdf_name)
            send_pdf_via_email(
                args, ctx, smtp_client, row, pdf_name, pdf_filename=pdf_filename
            )
        elif args.email:
            _LOGGER.warning("Cannot send email without producing PDFs")
    finally:
        tmp_docx.unlink(missing_ok=True)


def main(argv=None):
    args = parse_args(argv)

    start_time = None
    # start_time = datetime.datetime(2025, 8, 15, 10, 30, 27).astimezone()

    ctx = wsjrdp2027.WsjRdpContext(
        setup_logging=True,
        start_time=start_time,
        out_dir="data/sondervereinbarungen{{ kind | omit_unless_prod | upper | to_ext }}",
    )
    out_base = ctx.make_out_path(
        "sondervereinbarung_beitrag_raten_{{ filename_suffix }}"
    )
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(
            conn,
            status=None,
            where=f"""people.id IN ({", ".join(str(x) for x in args.id)})""",
            fee_rules=["planned", "active"],
            today=args.today,
            print_at=args.print_at,
        )

    if args.email:
        ctx.require_approval_to_send_email_in_prod()

    with _contextlib.ExitStack() as exit_stack:
        if args.email and args.pdf:
            smtp_client = exit_stack.enter_context(ctx.smtp_login())
        else:
            smtp_client = None

        for _, row in df.iterrows():
            id = row["id"]
            id_and_name = f"{id} {row['short_full_name']}"
            person_log = ctx.make_out_path(
                f"{id_and_name}/sondervereinbarung_beitrag_raten_{{{{ filename_suffix }}}}.{id}.log"
            )
            logger = logging.getLogger()
            handler = wsjrdp2027.configure_file_logging(
                person_log, level=logging.DEBUG, logger=logger
            )
            try:
                create_special_agreement(
                    args, ctx=ctx, row=row, smtp_client=smtp_client
                )
            finally:
                logger.removeHandler(handler)
                handler.flush()
                handler.close()


if __name__ == "__main__":
    sys.exit(main())
