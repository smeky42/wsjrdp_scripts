#!/usr/bin/env -S uv run
from __future__ import annotations

import email.message
import email.utils
import logging as _logging
import sys

import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argv=argv,
        out_dir="data/2025-09-19__Anmelde-Turbo__UL-Mailing__{{ filename_suffix }}",
    )
    out_base = ctx.make_out_path("Anmelde-Turbo__UL-Mailing__{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(
            conn,
            query=wsjrdp2027.PeopleQuery(
                where=wsjrdp2027.PeopleWhere(
                    primary_group_id=2, exclude_deregistered=True
                )
            ),
        )

    _LOGGER.info("Found %s UL", len(df))
    df_len = len(df)

    with ctx.mail_login() as client:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            msg = email.message.EmailMessage()
            msg["Subject"] = (
                "Dein nächster Schritt als Unit Leader: Anmeldung checken & YPs begeistern"
            )
            msg["From"] = "anmeldung@worldscoutjamboree.de"
            msg["To"] = email.utils.formataddr((row["short_full_name"], row["email"]))
            msg["Reply-To"] = "info@worldscoutjamboree.de"
            msg.set_content(
                f"""Hallo {row["greeting_name"]},

wir freuen uns riesig, dass du dich für das World Scout Jamboree 2027 in Polen als Unit Leader angemeldet hast. Mit großen Schritten laufen wir auf den Anmeldeschluss am 15.10.2025 zu und damit auch auf die Zusammenstellung der Units.

Bis es soweit ist kannst du uns noch aktiv unterstützen. Zum Einen, indem du überprüfst, dass deine Anmeldung vollständig abgegeben ist. Denn erst dann kannst du am Jamboree teilnehmen. Auf deiner Startseite in unserem Anmeldetool kannst du jederzeit ganz einfach den Status deiner Anmeldung überprüfen.

Der aktuelle Status deiner Anmeldung ist „{row["status_de"]}“.

Sollte der Status noch auf „Registriert“ oder „Anmeldung gedruckt“ stehen, hast du noch nicht alle Dokumente vollständig hochgeladen, oder bei der Überprüfung deiner Anmeldung sind uns Unstimmigkeiten aufgefallen und du wurdest gebeten eines oder mehrere Dokumente erneut hochzuladen. Ist der Status „Upload vollständig“, musst du nichts weiter tun. Wir werden deine Anmeldung prüfen und bei einer erfolgreichen Prüfung den Status ändern zu „Dokumente vollständig überprüft“.  Dies ist dein Zeichen, dass bei deiner Anmeldung alles stimmig ist und wir dich nach dem Anmeldeschluss in der Unit-Einteilung berücksichtigen. Achtung! Dies ist noch kein offizieller Vertragsschluss. Dieser geschieht erst sobald wir aus Polen die offizielle Bestätigung unserer Anmeldung als Kontingent erhalten haben.


Bereits jetzt kannst und sollst du uns noch weiter unterstützen, indem du **aktiv** Werbung machst.

Da wir nur mit vollständigen Units auf das Jamboree fahren, müssen wir bei der Unit Bildung auch das Verhältnis von Unit Leader*innen und Youth Participants berücksichtigen. Sollte der Fall eintreten, dass wir zu viele UL Anmeldungen haben werden wir einigen von euch absagen müssen. Selbstverständlich wollen wir euch aber alle dabei haben. Daher ist es umso wichtiger jetzt nochmal die Begeisterung für das Jamboree bei den Youth Participants (YPs) zu wecken.

Begeisterte Youth Participants und größere Anmeldezahlen sind auch in deinem Interesse. Denn wir werden insbesondere darauf achten, mit wem die YPs in einer Unit sein wollen (Buddy-ID).


Um nochmal Werbung zu machen und direkt Eltern und YPs zu erreichen, bietet es sich an bis Anfang Oktober einen Elternabend zu veranstalten. Dafür benötigen wir deine Hilfe!

Für die Durchführung der Infoveranstaltungen haben wir ein kleines Download Center auf unserer Website eingerichtet, wo du Infomaterial, Logos, usw. findest.

Den Link zu dieser Seite findest du hier: https://www.worldscoutjamboree.de/infos-fuer-unit-leader-ul/

Dort findet sich auch eine Info Präsentation, die es dir erleichtern soll Elternabende für interessierte Youth Participants aus deiner Ortsgruppe zu veranstalten.

Das lohnt sich, um auf das Jamboree aufmerksam zu machen und gibt die Möglichkeit viele bereits vorhandene Fragen rund um das Jamboree klären zu können. Gerade für Eltern ist es außerdem hilfreich die ULs kennenzulernen und sich persönlich ein Bild zu machen.

Wenn du planst einen Elternabend anzubieten, kannst du uns das gerne rückmelden. Dann werden wir dieses Angebot über unsere Kanäle verbreiten. So sollen flächendeckend möglichst viele potentielle Teilnehmer*innen erreicht werden. Um uns über deinen Elternabend zu informieren, kannst du ganz einfach das folgende Formular ausfüllen: https://cloud.worldscoutjamboree.de/apps/forms/s/PckSZZKSHDx37LNwN5tBMAJn


Vielen Dank für deine Unterstützung

Dein Contingent Management Team
"""
                + wsjrdp2027.EMAIL_SIGNATURE_CMT
            )
            eml_file = out_base.with_name(f"{out_base.name}--{row['id']}.eml")
            _LOGGER.info(
                "%s id: %s-%s; To: %s; status: %s",
                f"{i}/{df_len} ({i / df_len * 100.0:.1f}%)",
                row["primary_group_id"],
                row["id"],
                msg["To"],
                row["status"],
            )

            with open(eml_file, "wb") as f:
                f.write(msg.as_bytes())

            client.send_message(msg)


if __name__ == "__main__":
    sys.exit(main())
