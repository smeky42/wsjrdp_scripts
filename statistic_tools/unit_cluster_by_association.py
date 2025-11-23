#!/usr/bin/env -S uv run
from __future__ import annotations

import sys

import pandas as pd
import wsjrdp2027


all_regions = '''BMPPD - Keine Landesebene
BdP - Baden-Württemberg
BdP - Bayern
BdP - Berlin/Brandenburg
BdP - Bremen
BdP - Hessen
BdP - Niedersachsen
BdP - Nordrhein-Westfalen
BdP - Rheinland-Pfalz/Saar
BdP - Sachsen
BdP - Sachsen-Anhalt
BdP - Schleswig-Holstein/Hamburg
BdP - Thüringen
DPSG - Aachen
DPSG - Augsburg
DPSG - Bamberg
DPSG - Berlin
DPSG - Eichstätt
DPSG - Erfurt
DPSG - Essen
DPSG - Freiburg
DPSG - Fulda
DPSG - Hamburg
DPSG - Hildesheim
DPSG - Köln
DPSG - Limburg
DPSG - Magdeburg
DPSG - Mainz
DPSG - München
DPSG - Münster
DPSG - Osnabrück
DPSG - Paderborn
DPSG - Passau
DPSG - Regensburg
DPSG - Rottenburg-Stuttgart
DPSG - Speyer
DPSG - Trier
DPSG - Würzburg
PSG - Aachen
PSG - Augsburg
PSG - Bamberg
PSG - Dresden-Meissen
PSG - Essen
PSG - Freiburg
PSG - Hamburg
PSG - Hildesheim
PSG - Köln
PSG - Mainz
PSG - München
PSG - Münster
PSG - Osnabrück
PSG - Paderborn
PSG - Regensburg
PSG - Rottenburg-Stuttgart
PSG - Speyer
PSG - Trier
PSG - Wiesbaden
PSG - Würzburg
VCP - Baden
VCP - Bayern
VCP - Berlin-Brandenburg
VCP - Hamburg
VCP - Hessen
VCP - Mecklenburg-Vorpommern
VCP - Mitteldeutschland
VCP - Niedersachsen
VCP - Nordrhein
VCP - Rheinland-Pfalz/Saar
VCP - Sachsen
VCP - Schleswig-Holstein
VCP - Westfalen
VCP - Württemberg
'''


def data_frame_by_association(df: pd.DataFrame, role: str, association: str, region: str) -> pd.DataFrame:
    return df[(df["rdp_association"] == association) & (df["rdp_association_region"] == region) & (df["payment_role"].str.endswith(role))]


def print_frame_by_association(df: pd.DataFrame, association_region: str) -> tuple[int, int]:
    association, region = association_region.split(" - ")[0].lstrip(), association_region.split(" - ")[1].lstrip()
    print(f"\n### {association} - {region}" )

    ul_df = data_frame_by_association(df, "Unit::Leader", association, region)
    yp_df = data_frame_by_association(df, "Unit::Member", association, region)

    ul_count = len(ul_df)
    yp_count = len(yp_df)

    print(f"Total: {ul_count + yp_count} \t UL: {ul_count} \t YP: {yp_count}")

    if ul_count == 0:
        print("No ULs, cannot calculate factor")
        return [ul_count, yp_count]

    if yp_count == 0:
        print("No YPs, cannot calculate factor")
        return [ul_count, yp_count]

    ul_units = ul_count / 4
    yp_units = yp_count / 36
    print(
        f"UL Units: {ul_units:.2f} \t YP Units: {yp_units:.2f} \t Faktor YP/UL: {yp_units / ul_units:.2f}"
    )
    return [ul_count, yp_count]

def print_frame_by_region(df: pd.DataFrame, regions: str) -> None:
    ul_total = 0
    yp_total = 0
    for region in regions.splitlines():
        unit_count = print_frame_by_association(df, region)
        ul_total += unit_count[0]
        yp_total += unit_count[1]

    print(f"\n**Total: {ul_total + yp_total} \t UL: {ul_total} \t YP: {yp_total}**")
    ul_units = ul_total / 4
    yp_units = yp_total / 36
    print(
        f"\n**UL Units: {ul_units:.2f} \t YP Units: {yp_units:.2f} \t Faktor YP/UL: {yp_units / ul_units:.2f}**"
    )


def main():
    ctx = wsjrdp2027.WsjRdpContext()

    with ctx.psycopg_connect() as conn:
        cur = conn.cursor()
        cur.execute(f"""SELECT first_name, email, nickname, primary_group_id, zip_code, status, rdp_association, rdp_association_region, payment_role FROM people
    WHERE status = 'printed' OR status = 'upload' OR status = 'in_review' OR status = 'reviewed' OR status = 'confirmed'
    ORDER BY id""")
        rows = cur.fetchall()
        df = pd.DataFrame(
            rows,
            columns=[
                "first_name",
                "email",
                "nickname",
                "primary_group_id",
                "zip_code",
                "status",
                "rdp_association",
                "rdp_association_region",
                "payment_role",
            ],
        )

    # print_frame_by_region(df, all_regions)

    regions_bavaria = '''BdP - Bayern
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
VCP - Bayern'''

    print("\n## Bayern")
    print_frame_by_region(df, regions_bavaria)

    regions_bawue_rps = '''BdP - Baden-Württemberg
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
VCP - Rheinland-Pfalz/Saar'''
    print("\n# Baden-Württemberg + RPS")
    print_frame_by_region(df, regions_bawue_rps)


    regions_nrw_hessen = '''BdP - Hessen
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
VCP - Hessen'''
    print("\n# NRW + Hessen")
    print_frame_by_region(df, regions_nrw_hessen)

    regions_nord = '''BdP - Bremen
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
'''
    print("\n# Norddeutschland")
    print_frame_by_region(df, regions_nord)

    regions_east = '''BMPPD - Keine Landesebene
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
'''

    print("\n# Ostdeutschland")
    print_frame_by_region(df, regions_east)


if __name__ == "__main__":
    sys.exit(main())
