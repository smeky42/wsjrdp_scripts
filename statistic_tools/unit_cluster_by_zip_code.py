#!/usr/bin/env -S uv run
from __future__ import annotations

import sys

import pandas as pd
import wsjrdp2027


def data_frame_by_role(
    role: str, df: pd.DataFrame, zip_codes: list[str]
) -> pd.DataFrame:
    return df[
        (df["zip_code"].str.startswith(tuple(zip_codes)))
        & (df["payment_role"].str.endswith(role))
    ]


def data_frame_by_zip_code(zip_codes: list[str], df: pd.DataFrame) -> None:
    ul_df = data_frame_by_role("Unit::Leader", df, zip_codes)
    yp_df = data_frame_by_role("Unit::Member", df, zip_codes)

    ul_count = len(ul_df)
    yp_count = len(yp_df)

    print(f"PLZ: {zip_codes}")
    print(f"SUM: {ul_count + yp_count} \t UL: {ul_count} YP: {yp_count}")

    if ul_count == 0:
        print("No ULs, cannot calculate factor")
        return

    if yp_count == 0:
        print("No YPs, cannot calculate factor")
        return

    ul_untis = ul_count / 4
    yp_units = yp_count / 36
    print(
        f"UL Units: {ul_untis:.2f} YP Units: {yp_units:.2f} \t Faktor YP/UL: {yp_units / ul_untis:.2f}"
    )


def main():
    ctx = wsjrdp2027.WsjRdpContext()

    with ctx.psycopg_connect() as conn:
        print("query db")
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

    print("print pandas frame")
    print(df)

    print(f"\n == PLZ 0-9 ===")
    for zip in range(10):
        data_frame_by_zip_code([str(zip)], df)

    print(f"\n == PLZ 10-99 ===")
    for zip in range(10, 100):
        data_frame_by_zip_code([str(zip)], df)

    unit_len = len(df[df["payment_role"].str.endswith("Unit::Leader")]) + len(
        df[df["payment_role"].str.endswith("Unit::Member")]
    )
    print(f"\n == {unit_len} People in Query  ===")
    print("\n Nord")
    data_frame_by_zip_code(["0", "1", "2", "3"], df)

    print("\n Cluster Mitte West")
    data_frame_by_zip_code(["4", "5"], df)

    print("\n Cluster Bawü")
    data_frame_by_zip_code(["6", "7", "88", "89"], df)

    print("\n Cluster München")
    data_frame_by_zip_code(["81", "82"], df)


if __name__ == "__main__":
    sys.exit(main())
