#!/usr/bin/env -S uv run
from __future__ import annotations

import email.message
import logging
import sys
import textwrap

import pandas as pd
import wsjrdp2027


_LOGGER = logging.getLogger()


def send_delay_mail(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    mail_client: wsjrdp2027.MailClient,
    row: pd.Series,
) -> email.message.EmailMessage:
    msg = email.message.EmailMessage()
    msg["Subject"] = (
        f"WSJ 2027 - {row['short_full_name']} (id {row['id']}) - Verspätung des SEPA Lastschrifteinzugs"
    )
    msg["From"] = row["sepa_mailing_from"] or "anmeldung@worldscoutjamboree.de"
    if row["sepa_mailing_to"]:
        msg["To"] = row["sepa_mailing_to"]
    if row["sepa_mailing_cc"]:
        msg["Cc"] = row["sepa_mailing_cc"]
    if row["sepa_mailing_bcc"]:
        msg["Bcc"] = row["sepa_mailing_bcc"]
    msg["Reply-To"] = row["sepa_mailing_reply_to"] or ["info@worldscoutjamboree.de"]

    sepa_name = row["sepa_name"]
    short_full_name = row["short_full_name"]

    if sepa_name == short_full_name:
        hallo_string = f"Hallo {sepa_name}"
    else:
        hallo_string = f"Hallo {sepa_name}, Hallo {short_full_name}"

    msg.set_content(
        f"""{hallo_string},

aufgrund von technischen Schwierigkeiten bei unserer Bank verzögert sich der für den 05.11.2025 angekündigte SEPA Lastschrifteinzug.  Der Einzug wird voraussichtlich kommende Woche durchgeführt werden (also zwischen dem 10.11. und 14.11.).  Falls durch die Verzögerung der Einzug bei euch nicht mehr passt, könnt ihr euch per E-Mail bei uns melden. Wir wollen Gebühren für Rücklastschriften vermeiden.

Du nimmst mit folgenden Daten an diesem verzögerten Lastschrifteinzug teil:

Teilnehmer*in: {row["full_name"]}
Betrag: {wsjrdp2027.format_cents_as_eur_de(row["pre_notified_amount"])}

Kontoinhaber*in: {row["sepa_name"]}
IBAN: {row["sepa_iban"]}
Mandatsreferenz: {row["sepa_mandate_id"]}
Verwendungszweck: {row["sepa_dd_description"]}


Falls du Fragen hast, schau auf unserer Homepage https://worldscoutjamboree.de/ vorbei oder wende dich an info@worldscoutjamboree.de.

Dein WSJ-Orga-Team

Daffi und Peter
"""
        + wsjrdp2027.EMAIL_SIGNATURE_ORG
    )

    eml_file = ctx.make_out_path(f"sepa_delay_2025_11.{row['id']}.eml")

    with open(eml_file, "wb") as f:
        f.write(msg.as_bytes())

    mail_client.send_message(msg)
    return msg


def send_delay_mails(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    mail_client: wsjrdp2027.MailClient,
    df: pd.DataFrame,
) -> None:
    df_len = len(df)
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        _LOGGER.debug(
            "id: %s, full_name: %s, row:\n%s",
            row["id"],
            row["full_name"],
            textwrap.indent(row.to_string(), "  | "),
        )
        send_delay_mail(ctx=ctx, mail_client=mail_client, row=row)
        _LOGGER.info(
            "%s id: %s; To: %s; Cc: %s; status: %s %s; payment_role: %s; fee: %s; paid: %s; due: %s",
            f"{i}/{df_len} ({i / df_len * 100.0:.1f}%)",
            row["id"],
            row["sepa_mailing_to"],
            row["sepa_mailing_cc"],
            row["status"],
            row["sepa_status"],
            row["payment_role"],
            wsjrdp2027.format_cents_as_eur_de(row["total_fee_cents"]),
            wsjrdp2027.format_cents_as_eur_de(row["amount_paid_cents"]),
            wsjrdp2027.format_cents_as_eur_de(row["open_amount_cents"]),
        )


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        out_dir="data/sepa_delay_{{ filename_suffix }}",
        parse_arguments=True,
        argv=argv,
    )
    out_base = ctx.make_out_path("sepa_delay")
    log_filename = out_base.with_suffix(".log")
    xlsx_filename = out_base.with_suffix(".xlsx")
    ctx.configure_log_file(log_filename)

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_payment_dataframe_from_payment_initiation(
            conn,
            payment_initiation_id=2,
            pedantic=False,
            report_amount_differences=False,
        )
    wsjrdp2027.write_payment_dataframe_to_xlsx(df, xlsx_filename)

    with ctx.mail_login() as mail_client:
        send_delay_mails(ctx=ctx, mail_client=mail_client, df=df)

    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Excel: %s", xlsx_filename)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
