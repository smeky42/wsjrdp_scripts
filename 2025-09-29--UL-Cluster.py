#!/usr/bin/env -S uv run
from __future__ import annotations

import sys
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import psycopg
import yaml
import smtplib
from email.mime.text import MIMEText


def main():
    with open("config.yml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("read config.yaml successful")

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
        conn = psycopg.connect(
            host="localhost",
            port=tunnel.local_bind_port,
            user=db_username,
            password=db_password,
            dbname=db_name,
        )
    else:
        conn = psycopg.connect(
            host=db_host,
            port=db_port,
            user=db_username,
            password=db_password,
            dbname=db_name,
        )

    print("query db")
    cur = conn.cursor()
    cur.execute(f'''SELECT first_name, email, nickname, primary_group_id, zip_code, status, rdp_association, rdp_association_region, payment_role FROM people 
    WHERE status = 'printed' OR status = 'uploaded' OR status = 'reviewed'
    ORDER BY id''')
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=['first_name', 'email', 'nickname', 'primary_group_id', 'zip_code', 'status', 'rdp_association', 'rdp_association_region', 'payment_role'])



    cur.close()
    # conn.close()
    if tunnel is not None:
        tunnel.stop()


    cur.close()
    conn.close()
    if tunnel is not None:
        tunnel.stop()

    print("print pandas frame")
    print(df)

    for zip in range(10):
        ul_df = df[(df['zip_code'].str.startswith(str(zip))) & (df['payment_role'].str.endswith('Unit::Leader'))]
        yp_df = df[(df['zip_code'].str.startswith(str(zip))) & (df['payment_role'].str.endswith('Unit::Member'))]
        ul_count = len(ul_df)
        yp_count = len(yp_df)

        print(f"PLZ: {zip}*****")
        print(f" \t UL: {ul_count} YP: {yp_count}")

        ul_untis = ul_count / 4
        yp_units = yp_count / 36
        print(f" \t UL Units: {ul_untis:.2f} YP Units: {yp_units:.2f} \t Faktor YP/UL: {yp_units/ul_untis:.2f}")


if __name__ == "__main__":
    sys.exit(main())
    
