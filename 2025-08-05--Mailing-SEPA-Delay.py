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
        print("read config.yml successful")

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
    cur.execute("""
    SELECT id, first_name, last_name, email, gender, status, nickname, primary_group_id,
                    sepa_name, sepa_mail, sepa_iban, upload_sepa_pdf,
                    payment_role, early_payer, print_at
    FROM people
    WHERE early_payer = TRUE
    AND print_at < '2025-08-01'
    AND status = 'reviewed'
    ORDER BY id
    """)
    rows = cur.fetchall()
    columns = [
        "id",
        "first_name",
        "last_name",
        "email",
        "gender",
        "status",
        "nickname",
        "primary_group_id",
        "sepa_name",
        "sepa_mail",
        "sepa_iban",
        "upload_sepa_pdf",
        "payment_role",
        "early_payer",
        "print_at",
    ]

    df = pd.DataFrame(rows, columns=columns)

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
    for idx, row in df.iterrows():
        name = row["first_name"]
        mailadress = row["email"]
        nickname = row["nickname"]
        cc_mailadress = row["sepa_mail"]

        if nickname:
            name = nickname

        print(
            f"Send Mail to Name: {name}, Mailadresse: {mailadress} and {cc_mailadress}"
        )
        msg = MIMEText(f"""Hallo {name},

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
--
World Scout Jamboree 2027 Poland
Head of Organisation

Ring deutscher Pfadfinder*innenverbände e.V. (rdp)
Chausseestr. 128/129
10115 Berlin

info@worldscoutjamboree.de
https://worldscoutjamboree.de""")

        from_email = smtp_username or "anmeldung@worldscoutjamboree.de"
        msg["From"] = from_email
        msg["To"] = mailadress
        msg["Cc"] = cc_mailadress
        msg["Reply-To"] = "info@worldscoutjamboree.de"
        msg["Subject"] = "WSJ 2027 - Early Payer SEPA Lastschrifteinzug wird verschoben"

        server.sendmail(from_email, [mailadress, cc_mailadress], msg.as_string())

    server.quit()


if __name__ == "__main__":
    sys.exit(main())
