#!/usr/bin/env -S uv run
from __future__ import annotations

import sys

import pandas as pd
import wsjrdp2027

def last_half(role: str) -> str:
    parts = role.split("::Group::")
    return parts[1] if len(parts) > 1 else role


def main():
    ctx = wsjrdp2027.WsjRdpContext()

    with ctx.psycopg_connect() as conn:
        cur = conn.cursor()
        cur.execute(f"""SELECT first_name, email, nickname, primary_group_id, zip_code, status, rdp_association, payment_role FROM people
    WHERE status = 'confirmed'
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
                "payment_role",
            ],
        )


    df = df.copy()
    df["role_last_half"] = df["payment_role"].apply(last_half)

    stats = df.groupby(["role_last_half", "rdp_association"]).size().reset_index(name="count")
    print(stats)

if __name__ == "__main__":
    sys.exit(main())
