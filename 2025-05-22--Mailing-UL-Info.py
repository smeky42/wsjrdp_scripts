#!/usr/bin/env -S uv run
from __future__ import annotations

import sys
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import psycopg2
import yaml
import smtplib
from email.mime.text import MIMEText


def main():
    with open("config-dev.yml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("read config.ycml successful")

    # PostgreSQL-Datenbank-Einstellungen
    db_host = config["db_host"]
    db_port = config["db_port"]
    db_username = config["db_username"]
    db_password = config["db_password"]
    db_name = config["db_name"]

    # SMTP-Einstellungen
    smtp_server = config["smtp_server"]
    smtp_port = config["smtp_port"]
    smtp_username = config.get("smtp_username", "")
    smtp_password = config.get("smtp_password", "")

    # SSH-Tunnel-Einstellungen
    use_ssh_tunnel = config.get("use_ssh_tunnel")
    if use_ssh_tunnel:
        ssh_host = config["ssh_host"]
        ssh_port = config["ssh_port"]
        ssh_username = config["ssh_username"]
        ssh_private_key = config["ssh_private_key"]

        # Erstelle einen SSH-Tunnel
        tunnel = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_username,
            ssh_pkey=ssh_private_key,
            remote_bind_address=(db_host, db_port),
        )

        print("start ssh tunnel")
        tunnel.start()
    else:
        tunnel = None

    print("connect postgres")
    if tunnel:
        conn = psycopg2.connect(
            host="localhost",
            port=tunnel.local_bind_port,
            user=db_username,
            password=db_password,
            dbname=db_name,
        )
    else:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_username,
            password=db_password,
            dbname=db_name,
        )

    print("query db")
    cur = conn.cursor()
    cur.execute("SELECT first_name, email, nickname FROM people ORDER BY id")
    rows = cur.fetchall()

    df = pd.DataFrame(rows)

    cur.close()
    conn.close()
    if tunnel is not None:
        tunnel.stop()

    print("print pandas frame")
    print(df)

    # Verbinde dich mit dem SMTP-Server
    server = smtplib.SMTP(smtp_server, smtp_port)
    if server.has_extn("STARTTLS"):
        server.starttls()
    if smtp_username and smtp_password:
        server.login(smtp_username, smtp_password)

    # Verschicke die Mail an alle Mailadressen
    for row in rows:
        name = row[0]
        mailadress = row[1]
        nickname = row[2]

        if nickname:
            name = nickname

        print(f"Send Mail to Name: {name}, Mailadresse: {row[1]}")
        msg = MIMEText(
            f"""Hallo {name},

wir freuen uns sehr, dass du dich als Unit Leader engagieren möchtest und heißen dich herzlich willkommen!
Dein Engagement ist für uns von großer Bedeutung und wir sind begeistert, dich für das Projekt Jamboree 2027 an Board zu haben.

Am nächsten Sonntag, den 25.05.2025 ab 17 Uhr wollen wir mit Dir und anderen interessierten Unit Leadern über das Jamboree 2027 in Polen sprechen.

Ursprünglich hatten wir geplant, an diesem Tag einen großen Livestream für alle zu veranstalten und diesen in einem Brief an die jugendlichen Teilnehmer*innen (Youth Participants) anzukündigen.
Leider konnten wir den Brief noch nicht versenden, weshalb wir den großen Livestream auf den 29.06.2025 verschieben.
Mehr Infos dazu folgen im Brief, auf Social Media und auf unserer Homepage.

Wir halten den 25.05.  weiterhin für einen hervorragenden Termin, um über das Jamboree zu sprechen.
Daher laden wir Dich herzlich zu unserer ersten Online UL Info Veranstaltung  ein!

Einen Zugang zu unserem Konferenzraum findest du spätestens am Wochenende unter: https://www.worldscoutjamboree.de/2025/05/20/infoveranstaltung-fuer-unit-leader/

Wir werden über das Jamboree informieren und Materialien für Elternabende vorstellen, die auch von euch Unit Leadern genutzt werden können.
Die Info Veranstaltung wird eine wertvolle Gelegenheit sein, um Ideen auszutauschen und euch für eure Elternabende zu unterstützen.

Wir freuen uns auf deine Teilnahme und darauf, gemeinsam auf das große Ziel Polen 2027 hin zu arbeiten!

Herzliche Grüße und Gut Pfad!

Caro, Fox, Ines

"""
            + "-- \n"
            + """World Scout Jamboree 2027 Poland
Head of Contingent

Ring deutscher Pfadfinder*innenverbände e.V. (rdp)
Chausseestr. 128/129
10115 Berlin

info@worldscoutjamboree.de
https://worldscoutjamboree.de"""
        )

        msg["From"] = smtp_username
        msg["To"] = mailadress
        msg["Reply-To"] = "info@worldscoutjamboree.de"
        msg["Subject"] = (
            "Einladung zur ersten UL Online Info Veranstaltung - 25.05. 17 Uhr"
        )

        server.sendmail(smtp_username, mailadress, msg.as_string())

    server.quit()


if __name__ == "__main__":
    sys.exit(main())
