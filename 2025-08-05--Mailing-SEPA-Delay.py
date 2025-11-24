#!/usr/bin/env -S uv run
from __future__ import annotations

import email.message
import sys

import wsjrdp2027
from wsjrdp2027._people_where import PeopleWhere


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(argv=argv)

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_payment_dataframe(
            conn,
            where=PeopleWhere(
                early_payer=True,
                status=["reviewed"],
                max_print_at="2025-07-31",
            ),
        )

    # Verschicke die Mail an alle Mailadressen
    with ctx.mail_login() as client:
        for _, row in df.iterrows():
            msg = email.message.EmailMessage()
            msg.set_content(
                f"""Hallo {row["greeting_name"]},

es hilft uns sehr, dass du am SEPA-Lastschriftverfahren für das World Scout Jamboree 2027 als Early Payer teilnimmst!
Ursprünglich sollte heute der erste Einzug stattfinden. Vielleicht ist dir bereits aufgefallen, dass wir noch nichts eingezogen haben.

Die Einrichtung der Konten und die Erteilung der Erlaubnis für das Lastschriftverfahren haben länger gedauert als erwartet. Daher müssen wir den geplanten Einzug leider verschieben. Wir gehen davon aus, dass wir die Erlaubnis Mitte des Monats erhalten.

Den verschobenen Einzug kündigen wir natürlich rechtzeitig per E-Mail an.
Du nimmst mit folgendem Konto am Lastschriftverfahren teil:
Kontoinhaber: {row["sepa_name"]}
IBAN: {row["sepa_iban"]}

Falls du Fragen hast, schau auf unserer Homepage https://worldscoutjamboree.de/ vorbei oder wende dich an info@worldscoutjamboree.de.

Vielen Dank für dein Verständnis!

Dein WSJ-Orga-Team

Daffi und Peter
"""
                + wsjrdp2027.EMAIL_SIGNATURE_ORG
            )

            msg["Subject"] = (
                "WSJ 2027 - Early Payer SEPA Lastschrifteinzug wird verschoben"
            )
            msg["From"] = "anmeldung@worldscoutjamboree.de"
            msg["To"] = row["email"]
            if row["sepa_mail"] != row["email"]:
                msg["Cc"] = row["sepa_mail"]
            msg["Reply-To"] = "info@worldscoutjamboree.de"

            print(
                f"Send email to "
                f"id: {row['id']}; "
                f"name: {row['short_full_name']}; "
                f"To: {msg['To']}; "
                f"Cc: {msg['Cc']}"
            )
            client.send_message(msg)


if __name__ == "__main__":
    sys.exit(main())
