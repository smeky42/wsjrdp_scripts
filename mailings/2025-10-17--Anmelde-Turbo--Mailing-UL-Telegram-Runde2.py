#!/usr/bin/env -S uv run
from __future__ import annotations

import email.message
import email.utils
import logging as _logging
import sys

import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


ids_of_previous_mailing = frozenset(
    [
        4,
        5,
        6,
        7,
        15,
        18,
        20,
        21,
        22,
        23,
        25,
        26,
        27,
        28,
        29,
        30,
        31,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        46,
        47,
        49,
        50,
        51,
        52,
        53,
        54,
        55,
        56,
        57,
        58,
        62,
        67,
        69,
        70,
        71,
        72,
        73,
        75,
        77,
        78,
        79,
        80,
        81,
        83,
        84,
        85,
        86,
        87,
        88,
        89,
        91,
        92,
        93,
        95,
        96,
        98,
        99,
        100,
        102,
        104,
        105,
        106,
        107,
        108,
        110,
        111,
        113,
        115,
        116,
        117,
        118,
        119,
        120,
        122,
        125,
        127,
        128,
        129,
        130,
        132,
        133,
        134,
        136,
        137,
        138,
        139,
        209,
        236,
        325,
        334,
        340,
        404,
        410,
        411,
        460,
        475,
        483,
        484,
        489,
        497,
        501,
        541,
        561,
        574,
        612,
        741,
        768,
        795,
        826,
        892,
        907,
        915,
        1038,
        1040,
        1042,
        1058,
        1089,
        1103,
        1118,
        1141,
        1156,
        1157,
        1164,
        1168,
        1223,
        1226,
        1264,
        1289,
        1295,
        1322,
        1326,
        1370,
        1371,
        1401,
        1422,
        1443,
        1446,
        1485,
        1487,
        1533,
        1540,
        1541,
        1564,
        1592,
        1599,
        1610,
        1628,
        1635,
        1643,
        1648,
        1665,
        1666,
        1668,
        1669,
        1670,
        1713,
        1742,
        1756,
        1794,
        1814,
        1835,
        1839,
        1840,
        1841,
        1847,
        1850,
        1858,
        1896,
        1912,
        1931,
        1934,
        2035,
        2037,
        2045,
    ]
)


def link_by_association_region(association: str, region: str) -> str:
    if """BdP - Bayern
DPSG - München
PSG - München
DPSG - Passau
DPSG - Regensburg
PSG - Regensburg
DPSG - Bamberg
PSG - Bamberg
DPSG - Würzburg
PSG - Würzburg
DPSG - Augsburg
PSG - Augsburg
DPSG - Eichstätt
VCP - Bayern""".__contains__(f"{association} - {region}"):
        return f"Für {association} - {region} die Region Bayern: https://t.me/+WR-pFkSWYfY4OThi"
    elif """BdP - Baden-Württemberg
BdP - Rheinland-Pfalz/Saar
DPSG - Freiburg
PSG - Freiburg
DPSG - Rottenburg-Stuttgart
PSG - Rottenburg-Stuttgart
DPSG - Mainz
PSG - Mainz
DPSG - Speyer
PSG - Speyer
VCP - Baden
VCP - Württemberg
VCP - Rheinland-Pfalz/Saar""".__contains__(f"{association} - {region}"):
        return f"Für {association} - {region} die Region Bawü RPS: https://t.me/+1OKNSxcYdeExNzgy"
    elif """BdP - Hessen
BdP - Nordrhein-Westfalen
DPSG - Aachen
PSG - Aachen
DPSG - Essen
PSG - Essen
DPSG - Münster
PSG - Münster
DPSG - Köln
PSG - Köln
DPSG - Fulda
DPSG - Limburg
DPSG - Trier
PSG - Trier
VCP - Nordrhein
VCP - Westfalen
VCP - Hessen""".__contains__(f"{association} - {region}"):
        return f"Für {association} - {region} die Region NRW + Hessen: https://t.me/+TuXAtzz0R6syZTEy"
    elif """BdP - Bremen
BdP - Niedersachsen
BdP - Schleswig-Holstein/Hamburg
DPSG - Hamburg
PSG - Hamburg
DPSG - Hildesheim
PSG - Hildesheim
DPSG - Osnabrück
PSG - Osnabrück
DPSG - Paderborn
PSG - Paderborn
VCP - Hamburg
VCP - Niedersachsen
VCP - Schleswig-Holstein
""".__contains__(f"{association} - {region}"):
        return f"Für {association} - {region} die Region Norddeutschland: https://t.me/+uQMKzodcyZZkODNi"
    elif """BMPPD - Keine Landesebene
BdP - Berlin/Brandenburg
BdP - Sachsen
BdP - Sachsen-Anhalt
BdP - Thüringen
DPSG - Berlin
DPSG - Erfurt
DPSG - Magdeburg
PSG - Dresden-Meissen
PSG - Wiesbaden
VCP - Berlin-Brandenburg
VCP - Mecklenburg-Vorpommern
VCP - Mitteldeutschland
VCP - Sachsen
""".__contains__(f"{association} - {region}"):
        return f"Für {association} - {region} die Region Ostdeutschland: https://t.me/+y1Jg8bx7abg1NTAy"
    else:
        return "Wir konnten dir keine Region zuordnen. Trete gerne der Gruppe bei die dir am Besten passt."


def main():
    ctx = wsjrdp2027.WsjRdpContext(
        out_dir="data/2025-10-17__Anmelde-Turbo__UL-Mailing-Telegram2__{{ filename_suffix }}",
    )
    out_base = ctx.make_out_path(
        "Anmelde-Turbo__UL-Mailing-Telegram2__{{ filename_suffix }}"
    )
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(
            conn, where="primary_group_id = 2", exclude_deregistered=True
        )

    _LOGGER.info("Found total %s UL", len(df))

    df = df[~df["id"].isin(ids_of_previous_mailing)]

    df_len = len(df)
    _LOGGER.info("Found %s UL who did not got a mailing before", df_len)
    _LOGGER.info("remaining df:\n%s", str(df))

    ctx.require_approval_to_send_email_in_prod()

    with ctx.smtp_login() as client:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            msg = email.message.EmailMessage()
            msg["Subject"] = (
                f"Dein nächster Schritt als Unit Leader: Vernetzung (id {row['id']})"
            )
            msg["From"] = "anmeldung@worldscoutjamboree.de"
            msg["To"] = email.utils.formataddr(
                (str(row["short_full_name"]), row["email"])
            )
            msg["Reply-To"] = "info@worldscoutjamboree.de"
            msg["Date"] = email.utils.formatdate(localtime=True)
            msg.set_content(
                f"""Hallo {row["greeting_name"]},

wir nähern uns dem Anmeldeschluss (mehr dazu gleich) und gehen jetzt in die entscheidende Phase: die Bildung der Units!

Wie ihr wisst, braucht es für eine Unit zwei Komponenten: ein Team aus vier ULs – davon mindestens eine weibliche und ein männlicher UL – und 36 YPs aus einer Region. Die bisherigen Anmeldungen zusammenzuführen ist unsere Aufgabe für die kommenden Wochen. Und dafür brauchen wir euch!

Vernetzung zu UL-Teams:
Uns ist im Anmeldetool aufgefallen, dass wir zwar schon viele UL-Anmeldungen haben, aber bisher kaum vollständige Vierer-Teams aus den Buddy-IDs erkennen können. Deshalb möchten wir euch aktiv bei der Vernetzung unterstützen. Wir haben regionale Telegram-Gruppen eingerichtet, basierend auf eurer Postleitzahl im Anmeldetool. Die Links findet ihr am Ende dieser Mail. Dort könnt ihr andere ULs kennenlernen und euch zu Teams zusammenschließen. Achtet dabei unbedingt auf den Teamfit – ihr werdet in den kommenden 2,5 Jahren viel Zeit miteinander verbringen, und das Jamboree ist eine intensive Erfahrung. Unsere Unit Manager sind ebenfalls in den Gruppen und helfen euch bei diesem Prozess. Sprecht ehrlich mit ihnen, wenn ihr euch in einem Team nicht wohlfühlt. Ganz wichtig: Tragt eure Buddy-ID ins Anmeldetool ein, sobald ihr ein Team gebildet habt – nur so können wir eure Wünsche bei der Unit-Zusammenstellung berücksichtigen.

Eine Sidenote zum Messenger Telegram:
Bitte tretet zeitnah einer der Gruppen bei, damit wir direkt starten können. Wir wissen, dass viele von euch andere Messenger bevorzugen, aber Telegram ist für die Kommunikation im Jamboree und in vielen Pfadigremien zentral. Sobald ihr euer Team gefunden habt, könnt ihr natürlich zusätzlich jede Plattform nutzen, um euch auszutauschen.

Werbung von Young Participants:
Neben den UL-Teams braucht jede Unit 36 YPs. Aktuell haben wir deutschlandweit einen Überschuss an ULs, regional sind die Unterschiede noch deutlicher. Eine Live-Übersicht zu den Anmeldezahlen findet ihr hier:
https://anmeldung.worldscoutjamboree.de/public/statistics

Sollten wir ein Ungleichgewicht zwischen ULs und YPs haben, müssen wir einer Personengruppe leider absagen – wir zahlen an das Jamboree einen Fixpreis pro Unit und können keine leeren Plätze finanzieren. Im Falle einer Absage könnt ihr zu den IST wechseln oder auf die Nachrückerliste aufgenommen werden. Je mehr YPs eure Buddy-ID im Anmeldetool vermerken, desto wahrscheinlicher können wir euch einen Platz als Unitleitung anbieten.
Ihr als ULs seid maßgeblich dafür verantwortlich, YPs für eure Unit zu gewinnen. Unsere Erfahrung zeigt: Am meisten überzeugt ein Elternabend in eurer Region. Nutzt die Zeit bis zum Anmeldeschluss, um die Werbetrommel zu rühren. Ein früher Austausch mit Eltern und Jugendlichen hilft enorm, Vertrauen aufzubauen und weitere YPs für eure Unit zu begeistern. Hilfreiche Materialien für einen Elternabend findet ihr hier:
https://www.worldscoutjamboree.de/downloads-2/

Verschiebung Anmeldeschluss
Um euch mehr Zeit zu geben, haben wir den Anmeldeschluss um einen Monat verschoben: neuer Stichtag ist der 16.11.2025. Diese Info findet ihr auch auf unserer Website. Nutzt diese Chance! Je früher ihr euch vernetzt und zusammenfindet, desto reibungsloser könnt ihr starten. Ihr habt mehr Zeit für Vorbereitungen, könnt Jugendliche einbinden, die ihr schon kennt, und startet das Abenteuer bestmöglich vorbereitet. Verwendet die Buddy-IDs strategisch und bindet euren UM in die Planung ein – sie helfen euch gerne.

Ihr seid mit all diesen Aufgaben nicht allein: unsere Unit Manager*innen Berni, David, Hannah, Ki, Magda, Philipp und Teamlead Valle unterstützen euch. Bei Fragen meldet euch über Telegram oder wie gewohnt unter info@worldscoutjamboree.de.

Deine Telegram Gruppen für die Vernetzung haben wir anhand deiner Verbandsdaten ausgewählt.
{link_by_association_region(row["rdp_association"], row["rdp_association_region"])}

Falls du in einer anderen Region tätig bist, kannst du natürlich auch einer anderen Gruppe beitreten:

Bayern: https://t.me/+WR-pFkSWYfY4OThi
BaWü + RPS: https://t.me/+1OKNSxcYdeExNzgy
NRW + Hessen: https://t.me/+TuXAtzz0R6syZTEy
Norddeutschland: https://t.me/+uQMKzodcyZZkODNi
Ostdeutschland: https://t.me/+y1Jg8bx7abg1NTAy


Wir freuen uns schon riesig, euch endlich in den Telegram Gruppen kennenzulernen.
"""
                + wsjrdp2027.EMAIL_SIGNATURE_CMT
            )
            eml_file = out_base.with_name(f"{out_base.name}--{row['id']}.eml")
            _LOGGER.info(
                "%s id: %s-%s; To: %s; status: %s region: %s - %s",
                f"{i}/{df_len} ({i / df_len * 100.0:.1f}%)",
                row["primary_group_id"],
                row["id"],
                msg["To"],
                row["status"],
                row["rdp_association"],
                row["rdp_association_region"],
            )

            with open(eml_file, "wb") as f:
                f.write(msg.as_bytes())

            client.send_message(msg)


if __name__ == "__main__":
    sys.exit(main())
