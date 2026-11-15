#!/usr/bin/env -S uv run
"""Count confirmed people per role by the state of their Jamboree data.

For everybody with ``status = 'confirmed'`` this shows, per role, how
many have filled in the Jamboree data query (``people.jamboree_data``
is not empty) and how many of them have ``jamboree_data_confirmed`` set
to true or false.
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd
import wsjrdp2027


ROLE_ORDER = ["CMT", "UL", "YP", "IST", "BMT", "?"]


def create_argument_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description=__doc__)


def _role_sql() -> str:
    bmt_groups = ", ".join(str(g) for g in wsjrdp2027.Group.BMT_GROUPS)
    return f"""CASE
      WHEN payment_role LIKE '%::Group::Root::Member' THEN 'CMT'
      WHEN payment_role LIKE '%::Group::Unit::Leader' THEN 'UL'
      WHEN payment_role LIKE '%::Group::Unit::Member' THEN 'YP'
      WHEN payment_role LIKE '%::Group::Ist::Member'
           AND primary_group_id IN ({bmt_groups}) THEN 'BMT'
      WHEN payment_role LIKE '%::Group::Ist::Member' THEN 'IST'
      ELSE '?'
    END"""


def load_dataframe(conn) -> pd.DataFrame:
    query = f"""SELECT
      {_role_sql()} AS role,
      (jamboree_data IS NOT NULL AND jamboree_data <> '{{}}'::jsonb) AS filled,
      jamboree_data_confirmed AS confirmed_flag
    FROM people
    WHERE status = 'confirmed'"""
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["role", "filled", "confirmed_flag"])


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    def count(mask: pd.Series) -> pd.Series:
        return mask.groupby(df["role"]).sum()

    filled = df["filled"]
    flag = df["confirmed_flag"]
    summary = pd.DataFrame(
        {
            "gesamt": df.groupby("role").size(),
            "ausgefüllt": count(filled),
            "ausgefüllt & bestätigt=true": count(filled & flag),
            "ausgefüllt & bestätigt=false": count(filled & ~flag),
            "nicht ausgefüllt": count(~filled),
            "nicht ausgefüllt & bestätigt=true": count(~filled & flag),
            "nicht ausgefüllt & bestätigt=false": count(~filled & ~flag),
        }
    )
    order = [r for r in ROLE_ORDER if r in summary.index]
    summary = summary.reindex(order).fillna(0).astype(int)
    summary.loc["Summe"] = summary.sum()
    summary.index.name = "Rolle"
    return summary


def main(argv=None) -> int:
    ctx = wsjrdp2027.WsjRdpContext(
        argv=argv,
        argument_parser=create_argument_parser(),
        __file__=__file__,
    )
    out_base = ctx.make_out_path("jamboree_data_by_role_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        df = load_dataframe(ro_conn)

        if df.empty:
            print("Keine Personen mit status = 'confirmed' gefunden.")
            return 0

        summary = build_summary(df)
        with pd.option_context("display.width", 200, "display.max_columns", None):
            print(summary.to_string())

        csv_path = out_base.with_suffix(".csv")
        summary.to_csv(csv_path)
        ctx.register_output_file("Summary CSV", csv_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
