#!/usr/bin/env python

from __future__ import annotations

import datetime
import email.message
import pathlib
import sys

import wsjrdp2027

COLLECTION_DATE = datetime.date(2025, 8, 14)


SIGNATURE = """World Scout Jamboree 2027 Poland
Head of Organisation

Ring deutscher Pfadfinder*innenverbände e.V. (rdp)
Chausseestr. 128/129
10115 Berlin

info@worldscoutjamboree.de
https://worldscoutjamboree.de"""


EXPECTED_ID_S = frozenset([
    760, 59, 732, 82, 16, 14, 65, 1153, 1094, 1133, 832, 1132, 998, 765, 738, 1129, 681,
    1128, 1126, 712, 879, 571, 870, 673, 1097, 1011, 640, 702, 409, 1025, 751, 1014,
    1068, 217, 207, 829, 1050, 748, 895, 1055, 1036, 1039, 789, 1029, 295, 1002, 192,
    597, 220, 1015, 1007, 999, 586, 428, 908, 891, 645, 606, 466, 167, 498, 728, 685,
    684, 292, 298, 878, 886, 588, 864, 865, 620, 392, 868, 854, 615, 455, 808, 813, 822,
    532, 820, 430, 725, 726, 819, 807, 812, 788, 567, 787, 784, 761, 721, 755, 785, 782,
    783, 158, 623, 779, 608, 759, 603, 610, 671, 585, 737, 745, 631, 358, 691, 682, 723,
    692, 675, 642, 546, 643, 646, 678, 572, 632, 625, 527, 594, 568, 214, 402, 172, 619,
    518, 519, 444, 451, 389, 596, 601, 415, 448, 308, 587, 508, 539, 264, 556, 357, 555,
    526, 566, 417, 399, 547, 384, 433, 534, 545, 533, 535, 317, 503, 208, 170, 160, 269,
    326, 148, 149, 471, 374, 447, 456, 147, 463, 315, 329, 443, 442, 414, 300, 287, 310,
    275, 231, 144, 161, 150, 188, 204, 166, 252, 263, 206, 288, 200, 198, 311, 190, 302,
    176, 219, 159, 156, 141, 1142, 1012, 1013, 799, 271, 747, 703, 355, 528, 251, 371,
    297, 253, 235, 241])  # fmt: skip


def main():
    ctx = wsjrdp2027.ConnectionContext()

    df = wsjrdp2027.load_people_dataframe(
        ctx,
        where="early_payer = TRUE AND print_at < '2025-08-01' AND status IN ('reviewed', 'confirmed')",
    )

    print("Pandas DataFrame:")
    print(df)
    ids = df["id"].tolist()
    assert frozenset(ids) == EXPECTED_ID_S

    collection_date_de = COLLECTION_DATE.strftime("%d.%m.%Y")

    now_str = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    out_dir = pathlib.Path(f"data/sepa_direct_debit.{now_str}")
    out_dir.mkdir(exist_ok=True)

    with ctx.smtp_login() as client:
        for _, row in df.iterrows():
            msg = email.message.EmailMessage()
            payment_role = wsjrdp2027.PaymentRole(row["payment_role"])
            amount_eur = payment_role.full_fee_eur
            mandate_id = wsjrdp2027.mandate_id_from_hitobito_id(row["id"])
            nickname = row["nickname"]
            full_name = row["first_name"] + " " + row["last_name"]
            short_first_name = row["first_name"].split(" ", 1)[0]
            short_full_name = short_first_name + " " + row["last_name"]
            greeting_name = nickname if nickname else short_first_name

            msg["Subject"] = (
                f"WSJ 2027 - {short_full_name} (id {row['id']}) - Ankündigung SEPA Lastschrifteinzug ab {collection_date_de}"
            )
            msg["From"] = "anmeldung@worldscoutjamboree.de"
            msg["To"] = row["email"]
            if row["sepa_mail"] != row["email"]:
                msg["Cc"] = row["sepa_mail"]
            msg["Reply-To"] = "info@worldscoutjamboree.de"
            msg.set_content(
                f"""Hallo {greeting_name},

wir werden den verzögerten SEPA Lastschrifteinzug ab dem {collection_date_de} durchführen.

Du nimmst mit folgendem Konto am Lastschriftverfahren teil:

Betrag: {amount_eur} €
Kontoinhaber*in: {row["sepa_name"]}
IBAN: {row["sepa_iban"]}
Mandats-ID: {mandate_id}
Teilnehmer*in: {full_name}


Falls du Fragen hast, schau auf unserer Homepage https://worldscoutjamboree.de/ vorbei oder wende dich an info@worldscoutjamboree.de.

Dein WSJ-Orga-Team

Daffi und Peter
"""
                + "\n-- \n"
                + SIGNATURE
            )

            eml_file = out_dir / f"{row['id']}.pre_notification.eml"
            print(
                f"To: {msg['To']}; Cc: msg['Cc']; Amount: {amount_eur} € -> {eml_file}"
            )

            with open(eml_file, "wb") as f:
                f.write(msg.as_bytes())

            client.send_message(msg)


if __name__ == "__main__":
    sys.exit(main())
