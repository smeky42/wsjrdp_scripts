#!/usr/bin/env -S uv run
from __future__ import annotations

import datetime
import email.message
import logging
import smtplib as _smtplib
import sys
import textwrap

import pandas as pd
import wsjrdp2027


_LOGGER = logging.getLogger(__name__)


COLLECTION_DATE = datetime.date(2025, 12, 5)


def parse_args(argv=None):
    import argparse
    import sys

    if argv is None:
        argv = sys.argv
    p = argparse.ArgumentParser()
    p.add_argument("--accounting", action="store_true", default=True)
    p.add_argument("--no-accounting", dest="accounting", action="store_false")
    p.add_argument(
        "--email",
        action="store_true",
        default=True,
        help="Send out the created pre-notifications. Uses sepa_mailing_from, sepa_mailing_to, sepa_mailing_cc, sepa_mailing_bcc, sepa_mailing_reply_to",
    )
    p.add_argument(
        "--no-email",
        dest="email",
        action="store_false",
        help="Do not send out the created pre-notifications.",
    )
    p.add_argument(
        "--today",
        metavar="TODAY",
        default="TODAY",
        help="Run as if the current date is TODAY",
    )
    p.add_argument(
        "--collection-date",
        metavar="DATE",
        default=COLLECTION_DATE.strftime("%Y-%m-%d"),
        help="The collection date",
    )
    args = p.parse_args(argv[1:])
    args.today = wsjrdp2027.to_date(args.today)
    if args.collection_date:
        args.collection_date = wsjrdp2027.to_date(args.collection_date)
    else:
        args.collection_date = COLLECTION_DATE
    return args


def send_pre_notification_mail(
    args, *, ctx: wsjrdp2027.WsjRdpContext, smtp_client: _smtplib.SMTP, row: pd.Series
) -> email.message.EmailMessage:
    import email.message

    collection_date = row["collection_date"]
    collection_date_de = collection_date.strftime("%d.%m.%Y")

    msg = email.message.EmailMessage()
    msg["Subject"] = (
        f"WSJ 2027 - {row['short_full_name']} (id {row['id']}) - Ankündigung SEPA Lastschrifteinzug ab {collection_date_de}"
    )
    msg["From"] = row["sepa_mailing_from"] or "anmeldung@worldscoutjamboree.de"
    if row["sepa_mailing_to"]:
        msg["To"] = row["sepa_mailing_to"]
    if row["sepa_mailing_cc"]:
        msg["Cc"] = row["sepa_mailing_cc"]
    if row["sepa_mailing_bcc"]:
        msg["Bcc"] = row["sepa_mailing_bcc"]
    msg["Reply-To"] = row["sepa_mailing_reply_to"] or ["info@worldscoutjamboree.de"]
    msg.set_content(
        f"""Hallo {row["sepa_name"]}, Hallo {row["greeting_name"]},

wir werden den nächsten SEPA Lastschrifteinzug ab dem {collection_date_de} durchführen.

Du nimmst mit folgenden Daten an diesem Lastschrifteinzug teil:

Teilnehmer*in: {row["full_name"]}
Betrag: {wsjrdp2027.format_cents_as_eur_de(row["amount"])}

Kontoinhaber*in: {row["sepa_name"]}
IBAN: {row["sepa_iban"]}
Mandatsreferenz: {row["sepa_mandate_id"]}
Verwendungszweck: {row["sepa_dd_description"]}
Kundenreferenz: {row["sepa_dd_endtoend_id"]}


Falls du Fragen hast, schau auf unserer Homepage https://worldscoutjamboree.de/ vorbei oder wende dich an info@worldscoutjamboree.de.

Dein WSJ-Orga-Team

Daffi und Peter
"""
        + wsjrdp2027.EMAIL_SIGNATURE_ORG
    )

    eml_file = ctx.make_out_path(f"pre_notification.{row['id']}.eml")

    with open(eml_file, "wb") as f:
        f.write(msg.as_bytes())

    if args.email:
        smtp_client.send_message(msg)
    else:
        _LOGGER.warning("Skip actual email sending (--no-email given)")
    return msg


def send_pre_notification_mails(
    args, *, ctx: wsjrdp2027.WsjRdpContext, smtp_client: _smtplib.SMTP, df: pd.DataFrame
) -> None:
    df_len = len(df)
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        _LOGGER.debug(
            "id: %s, full_name: %s, row:\n%s",
            row["id"],
            row["full_name"],
            textwrap.indent(row.to_string(), "  | "),
        )
        send_pre_notification_mail(args, ctx=ctx, smtp_client=smtp_client, row=row)
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
            wsjrdp2027.format_cents_as_eur_de(row["amount_paid"]),
            wsjrdp2027.format_cents_as_eur_de(row["amount"]),
        )


def insert_pre_notifications_into_db(
    args,
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    df: pd.DataFrame,
    sepa_dd_config: wsjrdp2027.SepaDirectDebitConfig,
) -> None:
    if not args.accounting:
        _LOGGER.info("")
        _LOGGER.info("SKIP ACCOUNTING (--no-accounting given)")
        _LOGGER.info("")
        return

    with ctx.psycopg_connect() as conn:
        with conn.cursor() as cur:
            pain_id = wsjrdp2027.insert_payment_initiation(
                cursor=cur,
                sepa_dd_config=sepa_dd_config,
            )
            _LOGGER.info("payment initiation id: %s", pain_id)
            pymnt_inf_id = wsjrdp2027.insert_direct_debit_payment_info(
                cur,
                payment_initiation_id=pain_id,
                sepa_dd_config=sepa_dd_config,
            )
            _LOGGER.info("direct debit payment info id: %s", pymnt_inf_id)

            for _, row in df.iterrows():
                wsjrdp2027.insert_direct_debit_pre_notification_from_row(
                    cur,
                    row=row,
                    payment_initiation_id=pain_id,
                    direct_debit_payment_info_id=pymnt_inf_id,
                    creditor_id=wsjrdp2027.CREDITOR_ID,
                )


def main(argv=None):
    args = parse_args(argv=argv)

    start_time = None
    # start_time = datetime.datetime(2025, 8, 15, 10, 30, 27).astimezone()

    ctx = wsjrdp2027.WsjRdpContext(
        setup_logging=True,
        start_time=start_time,
        out_dir="data/sepa_direct_debit_pre_notifications_{{ filename_suffix }}",
        # log_level=logging.DEBUG,
    )
    out_base = ctx.make_out_path("sepa_direct_debit_{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    xml_filename = out_base.with_suffix(".xml")
    ctx.configure_log_file(log_filename)

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_payment_dataframe(
            conn,
            # early_payer=True,
            pedantic=False,
            max_print_at=args.collection_date,
            collection_date=args.collection_date,
            # where="people.payment_role NOT LIKE '%::Unit::Leader'",
        )

    df_ok = df[df["payment_status"] == "ok"]
    df_not_ok = df[df["payment_status"] != "ok"]

    if len(df_not_ok):
        _LOGGER.info("")
        _LOGGER.info("==== Skipped payments")
        _LOGGER.info("  Number of skipped payments: %s", len(df_not_ok))
        _LOGGER.info(
            "  Skipped payments DataFrame (payment_status != 'ok'):\n%s",
            textwrap.indent(str(df_not_ok), "  | "),
        )
        sum_not_ok = int(df_not_ok["amount"].sum())
        _LOGGER.info("  SUM(amount): %s", wsjrdp2027.format_cents_as_eur_de(sum_not_ok))
    else:
        sum_not_ok = 0

    _LOGGER.info("")
    _LOGGER.info("==== Payments")
    _LOGGER.info("  Number of payments: %s", len(df_ok))
    _LOGGER.info(
        "  Payments DataFrame (payment_status == 'ok'):\n%s",
        textwrap.indent(str(df_ok), "  | "),
    )

    sum_ok = int(df_ok["amount"].sum())
    _LOGGER.info("  SUM(amount): %s", wsjrdp2027.format_cents_as_eur_de(sum_ok))
    _LOGGER.info("")

    if sum_not_ok > 0:
        _LOGGER.error("Would skip amounts. Exit")
        sys.exit(1)
    if sum_ok == 0:
        _LOGGER.warning("No amount to transfer. Exit")
        sys.exit(0)

    ids = df_ok["id"].tolist()
    overlapping_ids = frozenset(ids) & frozenset(
        wsjrdp2027.EARLY_PAYER_AUGUST_IDS_SUPERSET
    )
    overlapping_ids -= set([623, 671])  # fehlgeschalgene August Einzüge
    overlapping_ids -= set([204, 208])  # Auf Ratenzahlung umgestellt
    if overlapping_ids:
        df_overlap = df_ok[df["id"].isin(overlapping_ids)]
        _LOGGER.error("")
        _LOGGER.error(
            "Found %s overlapping id's: %s",
            len(overlapping_ids),
            sorted(overlapping_ids),
        )
        _LOGGER.error("df_overlap:\n%s", str(df_overlap))
        for _, row in df_overlap.iterrows():
            _LOGGER.error(
                "id: %s, full_name: %s, row:\n%s",
                row["id"],
                row["full_name"],
                textwrap.indent(row.to_string(), "  | "),
            )
        sys.exit(1)

    del df

    sepa_dd_config: wsjrdp2027.SepaDirectDebitConfig = (  # type: ignore
        wsjrdp2027.WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG
    )

    wsjrdp2027.write_accounting_dataframe_to_sepa_dd(
        df_ok, path=xml_filename, config=sepa_dd_config, pedantic=True
    )

    ctx.require_approval_to_run_in_prod(
        "Do you want to store pre-notification data in the PRODUCTION Hitobito database?"
    )

    insert_pre_notifications_into_db(
        args, ctx=ctx, df=df_ok, sepa_dd_config=sepa_dd_config
    )

    ctx.require_approval_to_send_email_in_prod()
    with ctx.smtp_login() as smtp_client:
        send_pre_notification_mails(args, ctx=ctx, smtp_client=smtp_client, df=df_ok)

    _LOGGER.info("")
    _LOGGER.info(
        "SUM(amount): %s", wsjrdp2027.format_cents_as_eur_de(df_ok["amount"].sum())
    )
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  SEPA XML: %s", xml_filename)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
