#!/usr/bin/env -S uv run
from __future__ import annotations

import datetime
import email.message
import pathlib
import sys

import wsjrdp2027


COLLECTION_DATE = datetime.date(2025, 8, 15)


def main():
    ctx = wsjrdp2027.WsjRdpContext()

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_payment_dataframe(
            conn,
            collection_date=COLLECTION_DATE,
            where=wsjrdp2027.SelectPeopleConfig(
                role=["YP", "IST", "CMT"],
                exclude_deregistered=True,
                early_payer=True,
                max_print_at="2025-07-31",
            ),
        )

    ids = df["id"].tolist()
    if unexpected_ids := frozenset(ids) - frozenset(
        wsjrdp2027.EARLY_PAYER_AUGUST_IDS_SUPERSET
    ):
        print("Unexpected IDs:")
        print(sorted(unexpected_ids))
        sys.exit(1)

    collection_date_de = COLLECTION_DATE.strftime("%d.%m.%Y")

    ctx.require_approval_to_send_email_in_prod()

    now_str = ctx.start_time.strftime("%Y%m%d-%H%M%S")
    out_dir = pathlib.Path(f"data/sepa_direct_debit.{now_str}")
    out_dir.mkdir(exist_ok=True)

    with ctx.mail_login() as client:
        for _, row in df.iterrows():
            msg = email.message.EmailMessage()
            msg["Subject"] = (
                f"WSJ 2027 - {row['short_full_name']} (id {row['id']}) - Ankündigung SEPA Lastschrifteinzug ab {collection_date_de}"
            )
            msg["From"] = "anmeldung@worldscoutjamboree.de"
            msg["To"] = row["email"]
            if row["sepa_mail"] != row["email"]:
                msg["Cc"] = row["sepa_mail"]
            msg["Reply-To"] = "info@worldscoutjamboree.de"
            msg.set_content(
                f"""Hallo {row["greeting_name"]},

wir werden den verzögerten SEPA Lastschrifteinzug ab dem {collection_date_de} durchführen.

Du nimmst mit folgenden Daten am Lastschriftverfahren teil:

Teilnehmer*in: {row["full_name"]}
Betrag: {row["amount"] // 100} €

Kontoinhaber*in: {row["sepa_name"]}
IBAN: {row["sepa_iban"]}
Mandatsreferenz: {row["sepa_mandate_id"]}


Falls du Fragen hast, schau auf unserer Homepage https://worldscoutjamboree.de/ vorbei oder wende dich an info@worldscoutjamboree.de.

Dein WSJ-Orga-Team

Daffi und Peter
"""
                + wsjrdp2027.EMAIL_SIGNATURE_ORG
            )

            eml_file = out_dir / f"{row['id']}.pre_notification.eml"
            print(
                f"id: {row['id']}; To: {msg['To']}; Cc: {msg['Cc']}; payment_role: {row['payment_role']}; Amount: {row['amount'] // 100} €"
            )

            with open(eml_file, "wb") as f:
                f.write(msg.as_bytes())

            client.send_message(msg)


if __name__ == "__main__":
    sys.exit(main())
