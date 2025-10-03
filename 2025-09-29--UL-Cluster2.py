#!/usr/bin/env -S uv run
from __future__ import annotations

import sys

import pandas as pd
import wsjrdp2027


def main():
    ctx = wsjrdp2027.WsjRdpContext()

    with ctx.psycopg_connect() as conn:
        print("query db")
        cur = conn.cursor()
        cur.execute(f"""SELECT first_name, email, nickname, primary_group_id, zip_code, status, rdp_association, rdp_association_region, payment_role FROM people 
    WHERE status = 'printed' OR status = 'uploaded' OR status = 'reviewed'
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

    print("print pandas frame")
    print(df)

    for zip in range(10):
        ul_df = df[
            (df["zip_code"].str.startswith(str(zip)))
            & (df["payment_role"].str.endswith("Unit::Leader"))
        ]
        yp_df = df[
            (df["zip_code"].str.startswith(str(zip)))
            & (df["payment_role"].str.endswith("Unit::Member"))
        ]
        ul_count = len(ul_df)
        yp_count = len(yp_df)

        print(f"PLZ: {zip}*****")
        print(f" \t UL: {ul_count} YP: {yp_count}")

        ul_untis = ul_count / 4
        yp_units = yp_count / 36
        print(
            f" \t UL Units: {ul_untis:.2f} YP Units: {yp_units:.2f} \t Faktor YP/UL: {yp_units / ul_untis:.2f}"
        )


if __name__ == "__main__":
    sys.exit(main())
