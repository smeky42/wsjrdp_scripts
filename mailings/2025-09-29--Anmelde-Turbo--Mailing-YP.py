#!/usr/bin/env -S uv run
from __future__ import annotations

import email.message
import logging as _logging
import sys

import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argv=argv,
        out_dir="data/2025-09-29__Anmelde-Turbo__YP-Mailing__{{ filename_suffix }}",
    )
    out_base = ctx.make_out_path("Anmelde-Turbo__YP-Mailing__{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(
            conn,
            query=wsjrdp2027.PeopleQuery(
                where=wsjrdp2027.PeopleWhere(
                    primary_group_id=3,
                    exclude_deregistered=True,
                )
            ),
        )

    _LOGGER.info("Found %s YP", len(df))
    df_len = len(df)

    with ctx.mail_login() as client:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            msg = email.message.EmailMessage()
            msg["Subject"] = (
                f"Dein nächster Schritt als Youth Participant: Anmeldung checken & andere begeistern (id {row['id']})"
            )
            msg["From"] = "anmeldung@worldscoutjamboree.de"
            msg["To"] = row["mailing_to"]
            if row["mailing_cc"]:
                msg["Cc"] = row["mailing_cc"]
            msg["Reply-To"] = "info@worldscoutjamboree.de"
            msg.set_content(
                f"""Hallo {row["greeting_name"]},

wir freuen uns riesig, dass du dich für das World Scout Jamboree 2027 in Polen als Youth Participant (YP) angemeldet hast. Mit großen Schritten laufen wir auf den Anmeldeschluss am 15.10.2025 zu und damit auch auf die Zusammenstellung der Units.

Bis es so weit ist kannst du uns noch aktiv unterstützen. Zum einen, indem du überprüfst, dass deine Anmeldung vollständig abgegeben ist. Ohne vollständige Anmeldung kannst du nicht am Jamboree teilnehmen. Auf deiner Startseite in unserem Anmeldetool (https://anmeldung.worldscoutjamboree.de) kannst du ganz einfach den Status deiner Anmeldung überprüfen.

Der aktuelle Status deiner Anmeldung ist „{row["status_de"]}“ (Stand {row["today_de"]}).

Sollte der Status noch auf „Registriert“ oder „Anmeldung gedruckt“ stehen, hast du noch nicht alle Dokumente vollständig hochgeladen, oder bei der Überprüfung deiner Anmeldung sind uns Unstimmigkeiten aufgefallen und du wurdest gebeten, eines oder mehrere Dokumente erneut hochzuladen. Ist der Status „Upload vollständig“, musst du nichts weiter tun. Wir werden deine Anmeldung prüfen und bei einer erfolgreichen Prüfung den Status ändern zu „Dokumente vollständig überprüft“.  Dies ist dein Zeichen, dass bei deiner Anmeldung alles stimmig ist. Achtung! Dies ist noch kein offizieller Vertragsschluss. Dieser geschieht erst, wenn wir aus Polen die offizielle Bestätigung unserer Anmeldung als Kontingent erhalten haben. Dies wird voraussichtlich Ende November oder Anfang Dezember sein. Solange müssen auch wir im Vorbereitungsteam warten, ob wir zum Jamboree fahren dürfen.


*** Und jetzt kommt der Teil, bei dem du wirklich mitgestalten kannst: ***

Erzähl in deinem Stamm, deiner Sippe oder deinem Gau vom Jamboree! Vielleicht gibt es dort Leute, die noch nichts von dieser einmaligen Gelegenheit gehört haben – und genau auf so ein Abenteuer warten. Das World Scout Jamboree ist die perfekte Chance, gemeinsam mit Pfadfinder*innen aus aller Welt neue Freundschaften zu schließen, spannende Herausforderungen zu meistern und unvergessliche Erinnerungen zu sammeln.

Sprich deine Freund*innen und Leiter*innen an oder teile den Anmeldelink (https://www.worldscoutjamboree.de/howto-anmeldung/) in euren Gruppen. Falls du bei deiner Anmeldung eine Buddy-ID angegeben hast, um mit einer bestimmten Person zusammen in eine Unit zu kommen, erinnere diese bitte auch daran, ihre Anmeldung zu vervollständigen. Zusammen macht es doppelt Spaß – und wer weiß, vielleicht reist ihr 2027 mit einer ganzen Freundesgruppe nach Polen!


Wir freuen uns auf eine unvergessliche Zeit

Dein Contingent Management Team
"""
                + wsjrdp2027.EMAIL_SIGNATURE_CMT
            )
            eml_file = out_base.with_name(f"{out_base.name}--{row['id']}.eml")
            _LOGGER.info(
                "%s id: %s-%s; To: %s; Cc: %s; status: %s",
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
