#!/usr/bin/env -S uv run
from __future__ import annotations

import email.message
import email.utils
import logging as _logging
import sys

import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


def main():
    ctx = wsjrdp2027.WsjRdpContext(
        out_dir="data/2025-09-19__Anmelde-Turbo__IST-Mailing__{{ filename_suffix }}",
    )
    out_base = ctx.make_out_path("Anmelde-Turbo__IST-Mailing__{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(
            conn, where="primary_group_id = 4", exclude_deregistered=True
        )

    _LOGGER.info("Found %s IST", len(df))
    df_len = len(df)

    ctx.require_approval_to_send_email_in_prod()

    with ctx.smtp_login() as client:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            msg = email.message.EmailMessage()
            msg["Subject"] = (
                f"Dein nächster Schritt als IST: Anmeldung checken & andere begeistern (id {row['id']})"
            )
            msg["From"] = "anmeldung@worldscoutjamboree.de"
            msg["To"] = row["mailing_to"]
            if row["mailing_cc"]:
                msg["Cc"] = row["mailing_cc"]
            msg["Reply-To"] = "ist@worldscoutjamboree.de"
            msg["Date"] = email.utils.formatdate(localtime=True)
            msg.set_content(
                f"""Hallo {row["greeting_name"]},

wir freuen uns riesig, dass du dich für das World Scout Jamboree 2027 in Polen als IST angemeldet hast. Mit großen Schritten laufen wir auf den Anmeldeschluss am 15.10.2025 zu.

Bis es soweit ist kannst du uns noch aktiv unterstützen. Zum Einen, indem du überprüfst, dass deine Anmeldung vollständig abgegeben ist. Denn erst dann kannst du am Jamboree teilnehmen. Auf deiner Startseite in unserem Anmeldetool (https://anmeldung.worldscoutjamboree.de) kannst du jederzeit ganz einfach den Status deiner Anmeldung überprüfen.

Der aktuelle Status deiner Anmeldung ist „{row["status_de"]}“.

Sollte der Status noch auf „Registriert“ oder „Anmeldung gedruckt“ stehen, hast du noch nicht alle Dokumente vollständig hochgeladen, oder bei der Überprüfung deiner Anmeldung sind uns Unstimmigkeiten aufgefallen und du wurdest gebeten eines oder mehrere Dokumente erneut hochzuladen. Ist der Status „Upload vollständig“, musst du nichts weiter tun. Wir werden deine Anmeldung prüfen und bei einer erfolgreichen Prüfung den Status ändern zu „Dokumente vollständig überprüft“.  Dies ist dein Zeichen, dass bei deiner Anmeldung alles stimmig ist. Achtung! Dies ist noch kein offizieller Vertragsschluss. Dieser geschieht erst sobald wir aus Polen die offizielle Bestätigung unserer Anmeldung als Kontingent erhalten haben.

Falls ihr Probleme oder Herausforderungen bei der Anmeldung habt wendet euch vertrauensvoll an IST@worldscoutjamboree.de (z.B. mit einer Antwort auf diese Mail).


Außerdem möchten wir dich darum bitten aktiv Werbung für das Jamboree zu machen. Es ist super wichtig jetzt nochmal die Begeisterung für das Jamboree bei den YPs (=Jugendlichen Teilnehmenden) zu wecken, damit wir möglichst allen Interessierten die Möglichkeit bieten können an der once-in-lifetime Erfahrung Jamboree teilzunehmen. Begeisterte Youth Participants und größere Anmeldezahlen sind sind eine Bereicherung für das gesamte Kontingent!


Um nochmal Werbung zu machen und direkt Eltern und YPs und mögliche andere ISTs zu erreichen, bietet es sich an Anfang Oktober einen Elternabend zu veranstalten. Dafür benötigen wir deine Hilfe!

Für die Durchführung der Infoveranstaltungen haben wir unter https://www.worldscoutjamboree.de/infos-fuer-unit-leader-ul/ ein kleines Download Center auf unserer Website eingerichtet, wo du Infomaterial, Logos, usw. findest. Dort findet sich auch eine Info-Präsentation, die es dir erleichtern soll Elternabende für interessierte Youth Participants aus deiner Ortsgruppe zu veranstalten.

Eine Infoveranstaltung lohnt sich, um auf das Jamboree aufmerksam zu machen und gibt die Möglichkeit viele bereits vorhandene Fragen rund um das Jamboree zu klären. Gerade für Eltern oder Pfadis, die noch nie ein Jamboree erlebt haben, ist es außerdem hilfreich sich persönlich ein Bild zu machen.

Wenn du planst eine Infoveranstaltung anzubieten, kannst du uns das gerne in dem Formular https://cloud.worldscoutjamboree.de/apps/forms/s/PckSZZKSHDx37LNwN5tBMAJn rückmelden. Dann werden wir dieses Angebot über unsere Kanäle verbreiten. So sollen flächendeckend möglichst viele potentielle Teilnehmer*innen erreicht werden.


Vielen Dank für deine Unterstützung
Dein Contingent Management Team
"""
                + wsjrdp2027.EMAIL_SIGNATURE_CMT
            )
            eml_file = out_base.with_name(f"{out_base.name}--{row['id']}.eml")

            _LOGGER.info(
                "%s id: %s-%s; To: %s; CC: %s; status: %s",
                f"{i}/{df_len} ({i / df_len * 100.0:.1f}%)",
                row["primary_group_id"],
                row["id"],
                msg["To"],
                msg["Cc"],
                row["status"],
            )

            with open(eml_file, "wb") as f:
                f.write(msg.as_bytes())

            client.send_message(msg)


if __name__ == "__main__":
    sys.exit(main())
