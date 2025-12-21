#!/usr/bin/env -S uv run
from __future__ import annotations

import datetime
import email.message
import logging
import sys
import textwrap

import pandas as pd
import wsjrdp2027


_LOGGER = logging.getLogger(__name__)


COLLECTION_DATE = datetime.date(2025, 12, 5)


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--accounting", action="store_true", default=True)
    p.add_argument("--no-accounting", dest="accounting", action="store_false")
    p.add_argument(
        "--collection-date",
        metavar="DATE",
        default=COLLECTION_DATE.strftime("%Y-%m-%d"),
        help="The collection date",
    )
    return p


def send_pre_notification_mail(
    mail_client: wsjrdp2027.MailClient,
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    row: pd.Series,
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
Betrag: {wsjrdp2027.format_cents_as_eur_de(row["open_amount_cents"])}

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

    mail_client.send_message(msg)
    return msg


def send_pre_notification_mails(
    mail_client: wsjrdp2027.MailClient,
    *,
    ctx: wsjrdp2027.WsjRdpContext,
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
        send_pre_notification_mail(mail_client, ctx=ctx, row=row)
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
            pain_id = wsjrdp2027.pg_insert_payment_initiation(
                cursor=cur,
                sepa_dd_config=sepa_dd_config,
            )
            _LOGGER.info("payment initiation id: %s", pain_id)
            pymnt_inf_id = wsjrdp2027.pg_insert_direct_debit_payment_info(
                cur,
                payment_initiation_id=pain_id,
                sepa_dd_config=sepa_dd_config,
                creditor_id=wsjrdp2027.CREDITOR_ID,
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
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        setup_logging=True,
        out_dir="data/sepa_direct_debit_pre_notifications_{{ filename_suffix }}",
        # log_level=logging.DEBUG,
    )
    args = ctx.parsed_args
    if args.collection_date:
        args.collection_date = wsjrdp2027.to_date_or_none(args.collection_date)
    else:
        args.collection_date = COLLECTION_DATE

    out_base = ctx.make_out_path("sepa_direct_debit_{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    xml_filename = out_base.with_suffix(".xml")
    ctx.configure_log_file(log_filename)

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_payment_dataframe(
            conn,
            query=wsjrdp2027.PeopleQuery(
                where=wsjrdp2027.PeopleWhere(
                    status=["reviewed", "confirmed"],
                    sepa_status="ok",
                    role=["CMT", "IST", "YP", "UL"],
                ),
                collection_date=args.collection_date,
            ),
        )

    _LOGGER.info("")
    _LOGGER.info("==== Overall payments: %s", len(df))
    _LOGGER.info("")
    df_ok = df[df["payment_status"] == "ok"]
    df_not_ok = df[df["payment_status"] != "ok"]

    if len(df_not_ok):
        _LOGGER.info("")
        _LOGGER.info("==== Skipped payments (payment_status != 'ok')")
        _LOGGER.info("  Number of skipped payments: %s", len(df_not_ok))
        _LOGGER.info(
            "  Skipped payments DataFrame (payment_status != 'ok'):\n%s",
            textwrap.indent(str(df_not_ok), "  | "),
        )
        for _, row in df_not_ok.iterrows():
            _LOGGER.debug(
                "    %5d %s / %s / %s",
                row["id"],
                row["short_full_name"],
                row["payment_status"],
                row["payment_status_reason"],
            )
        sum_not_ok = int(df_not_ok["open_amount_cents"].sum())
        _LOGGER.info(
            "  NOT OK payments: SUM(open_amount_cents): %s",
            wsjrdp2027.format_cents_as_eur_de(sum_not_ok),
        )
    else:
        sum_not_ok = 0

    _LOGGER.info("")
    _LOGGER.info("==== Payments (payment_status == 'ok')")
    _LOGGER.info("  Number of payments: %s", len(df_ok))
    _LOGGER.info(
        "  Payments DataFrame (payment_status == 'ok'):\n%s",
        textwrap.indent(str(df_ok), "  | "),
    )

    sum_ok = int(df_ok["open_amount_cents"].sum())
    _LOGGER.info(
        "  OK payments: SUM(open_amount_cents): %s",
        wsjrdp2027.format_cents_as_eur_de(sum_ok),
    )
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

    with ctx.mail_login() as mail_client:
        send_pre_notification_mails(mail_client, ctx=ctx, df=df_ok)

    _LOGGER.info("")
    _LOGGER.info(
        "SUM(open_amount_cents): %s",
        wsjrdp2027.format_cents_as_eur_de(df_ok["open_amount_cents"].sum()),
    )
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  SEPA XML: %s", xml_filename)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
