import sys
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import psycopg2
import yaml
import smtplib
from email.mime.text import MIMEText
from sepaxml import SepaDD
from datetime import datetime, date 

read_only = True

payment_array = [
    ["Rolle", "Gesamt", "Dez 2025", "Jan 2026", "Feb 2026",
        "Mär 2026", "Aug 2026", "Nov 2026", "Feb 2027", "Mai 2027"],
    ["RegularPayer::Group::Unit::Member", "3400", "300",
        "500", "500", "500", "400", "400", "400", "400"],
    ["RegularPayer::Group::Unit::Leader", "2400", "150",
        "350", "350", "350", "300", "300", "300", "300"],
    ["RegularPayer::Group::Ist::Member", "2600", "200",
        "400", "400", "400", "300", "300", "300", "300"],
    ["RegularPayer::Group::Root::Member", "1600", "50",
        "250", "250", "250", "200", "200", "200", "200"],
    ["EarlyPayer::Group::Unit::Member", "3400", "", "", "", "", "", "", "", ""],
    ["EarlyPayer::Group::Unit::Leader", "2400", "", "", "", "", "", "", "", ""],
    ["EarlyPayer::Group::Ist::Member", "2600", "", "", "", "", "", "", "", ""],
    ["EarlyPayer::Group::Root::Member", "1600", "", "", "", "", "", "", "", ""]
]


def main():
    with open("config.yml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("read config.ycml successful")

    # SSH-Tunnel-Einstellungen
    ssh_host = config["ssh_host"]
    ssh_port = config["ssh_port"]
    ssh_username = config["ssh_username"]
    ssh_private_key = config["ssh_private_key"]

    # PostgreSQL-Datenbank-Einstellungen
    db_host = config["db_host"]
    db_port = config["db_port"]
    db_username = config["db_username"]
    db_password = config["db_password"]
    db_name = config["db_name"]

    # SMTP-Einstellungen
    smtp_server = config["smtp_server"]
    smtp_port = config["smtp_port"]
    smtp_username = config["smtp_username"]
    smtp_password = config["smtp_password"]

    # Erstelle einen SSH-Tunnel
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_username,
        ssh_pkey=ssh_private_key,
        remote_bind_address=(db_host, db_port)
    )

    print("start ssh tunnel")
    tunnel.start()

    print("connect postgres")
    conn = psycopg2.connect(
        host='localhost',
        port=tunnel.local_bind_port,
        user=db_username,
        password=db_password,
        dbname=db_name
    )

    print("query db")
    cur = conn.cursor()
    cur.execute(f'''SELECT id, first_name, last_name, email, gender, status, nickname, primary_group_id, 
                    sepa_name, sepa_mail, sepa_iban, upload_sepa_pdf, 
                    payment_role, early_payer, print_at 
              FROM people 
              ORDER BY id''')

    rows = cur.fetchall()
    columns = ['id', 'first_name', 'last_name', 'email', 'gender', 'status', 'nickname', 'primary_group_id',
               'sepa_name', 'sepa_mail', 'sepa_iban', 'upload_sepa_pdf',
               'payment_role', 'early_payer', 'print_at']
    df = pd.DataFrame(rows, columns=columns)

    cur.close()
    conn.close()
    tunnel.stop()

#   print("print pandas frame")
#   print(df)
    print(f"Registered: {len(df)}")

    df_reviewed = df[df['status'] == 'reviewed']
    print(f"Reviewed: {len(df_reviewed)}")

    df_reviewed_early_payers = df[(
        df['status'] == 'reviewed') & (df['early_payer'] == True)]
    print(f"Reviewed Early Payers: {len(df_reviewed_early_payers)}")

    payment_dict = {row[0]: int(row[1]) for row in payment_array[1:]}
    role_counts = df_reviewed_early_payers['payment_role'].value_counts()

    total_amount = 0
    print("Role Breakdown:")
    for role, count in role_counts.items():
        # Find the corresponding amount for the role
        amount = payment_dict.get(role, 0)
        role_total = count * amount
        total_amount += role_total
        print(f"{role}: {count} x {amount} = {role_total} €")

    print(f"\nTotal Reviewed Early Payers: {len(df_reviewed_early_payers)}")
    print(f"Total Amount: {total_amount} €")

    config = {
        "name": "Ring deutscher Pfadfinder*innenverbände e.V",
        "IBAN": "DE34520900000077228802",
        "BIC": "GENODE51KS1",
        "batch": True,
        "creditor_id": "",  # supplied by your bank or financial authority
        "currency": "EUR"
    }
    sepa = SepaDD(config, schema="pain.008.001.02", clean=True)

    for idx, row in df_reviewed_early_payers.iterrows():
        try: 
          role = row['payment_role']
          # Default to 0 if the role isn't found
          amount = payment_dict.get(role, 0)
          print(
              f"{row['first_name']} {row['last_name']} ({role}): {amount}€ to pay")
          
          name = row['sepa_name']
          iban = row['sepa_iban']
          # if not name or not iban:
          #   raise ValueError("Missing SEPA name or IBAN")

          # if not isinstance(row['print_at'], (datetime, datetime.date)):
          #     raise TypeError("Invalid mandate date format")

          payment = {
              "name": name,
              "IBAN": iban,
              # "BIC": row['sepa_bic'],
              "amount": amount * 100,  # in cents
              "type": "FRST",  # FRST,RCUR,OOFF,FNAL
              "collection_date": date(2025, 8, 5),
              "mandate_id": f"wsjrdp27-{row['id']}",
              "mandate_date": row['print_at'],
              "description": f"Early Payer WSJ 2027 Gesamtbetrag für {role}"
          }
          sepa.add_payment(payment)
        except (ValueError, TypeError) as e:
          print(f"Skipping row {row['first_name']} {row['last_name']} ({role}): {amount}€ to pay")
          print(f"{row['sepa_name']} - {row['sepa_iban']} - {row['print_at']}")
          print(e)
    
    filename = datetime.now().strftime("%Y-%m-%d--%H-%M-%S")
    with open(f"{filename}--sepa.xml", 'w', encoding='utf-8') as f:
        f.write(sepa.export(validate=False).decode('utf-8'))

    print("SEPA Direct Debit XML generated successfully.")


if __name__ == "__main__":
    sys.exit(main())
