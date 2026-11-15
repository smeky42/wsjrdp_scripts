#!/usr/bin/env -S uv run
"""Excel list of everybody with a confirmed contract who has not confirmed
the Jamboree data (``people.jamboree_data_confirmed`` is false).

Columns: group_name, id, name, link (to the hitobito profile),
sorted by primary_group_id.

Run with:
  WSJRDP_SCRIPTS_CONFIG=config-prod.yml uv run statistic_tools/jamboree_data_unconfirmed_list.py
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd
import wsjrdp2027


BASE_URL = "https://anmeldung.worldscoutjamboree.de"


def create_argument_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description=__doc__)


def load_dataframe(conn) -> pd.DataFrame:
    query = """SELECT
      g.name AS group_name,
      p.id,
      CONCAT_WS(' ', p.first_name, p.last_name) AS name,
      p.primary_group_id
    FROM people p
    LEFT JOIN groups g ON g.id = p.primary_group_id
    WHERE p.contract_status = 'confirmed'
      AND COALESCE(p.jamboree_data_confirmed, FALSE) = FALSE
    ORDER BY p.primary_group_id, p.last_name, p.first_name, p.id"""
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["group_name", "id", "name", "primary_group_id"])
    df["link"] = [
        f"{BASE_URL}/groups/{gid}/people/{pid}"
        for gid, pid in zip(df["primary_group_id"], df["id"])
    ]
    return df[["group_name", "id", "name", "link"]]


def main(argv=None) -> int:
    ctx = wsjrdp2027.WsjRdpContext(
        argv=argv,
        argument_parser=create_argument_parser(),
        __file__=__file__,
    )
    out_base = ctx.make_out_path("jamboree_data_unconfirmed_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        df = load_dataframe(ro_conn)
        ctx.logger.info(
            "%d people with contract_status=confirmed and jamboree_data_confirmed=false",
            len(df),
        )

        xlsx_path = out_base.with_suffix(".xlsx")
        wsjrdp2027.write_dataframe_to_xlsx(df, xlsx_path, sheet_name="Nicht bestaetigt")
        ctx.register_output_file("Excel", xlsx_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
