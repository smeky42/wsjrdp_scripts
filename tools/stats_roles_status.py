#!/usr/bin/env -S uv run
"""Dumps the database and restores it into dev (config-dev.yml)."""

from __future__ import annotations

import logging
import math
import sys
import textwrap

import pandas as pd
import psycopg.rows
import wsjrdp2027


_LOGGER = logging.getLogger(__name__)


STATS_ROLES_STATUS_SQL_STMT = """
WITH group_names (id, name) AS (VALUES (1, 'CMT'),(2, 'UL'),(3, 'YP'),(4, 'IST'))

SELECT group_names.name AS role, status, COUNT(people.id) AS num_people FROM people
JOIN group_names ON people.primary_group_id = group_names.id
WHERE primary_group_id IN (1, 2, 3, 4)
AND status NOT IN ('deregistration_noted', 'deregistered')

GROUP BY group_names.name, people.primary_group_id, status
ORDER BY
  primary_group_id ASC,
  array_position(array['registered','printed','upload','in_review','reviewed','confirmed','deregistration_noted', 'deregistered'], status)
"""

ROLES = ["CMT", "UL", "YP", "IST"]

COLUMNS = [
    "registered",
    "printed",
    "upload",
    "in_review",
    "reviewed",
    "confirmed",
]


def main(argv=None):
    start_time = None
    # start_time = datetime.datetime(2025, 8, 15, 10, 30, 27).astimezone()
    ctx = wsjrdp2027.WsjRdpContext(
        setup_logging=True,
        start_time=start_time,
        out_dir="data",
    )
    out_base = ctx.make_out_path("stats_roles_status_{{ filename_suffix }}")
    xlsx_filename = out_base.with_suffix(".xlsx")

    with ctx.psycopg_connect() as conn:
        cur = conn.cursor(row_factory=psycopg.rows.dict_row)
        cur.execute(STATS_ROLES_STATUS_SQL_STMT)  # type: ignore
        rows = cur.fetchall()
        cur.close()
        base_df = pd.DataFrame(rows)
    _LOGGER.info(
        "Resulting base pandas DataFrame:\n%s", textwrap.indent(str(base_df), "  ")
    )

    NO_MIN_COLS = ["in_review", "confirmed"]

    df = pd.DataFrame(
        columns=[
            *COLUMNS,
            *(f"min_{col}" for col in COLUMNS if col not in NO_MIN_COLS),
        ]
    )
    for role in ROLES:
        for col in df.columns[1:]:
            df.at[role, col] = 0
    for _, row in base_df.iterrows():
        df.at[row["role"], row["status"]] = int(row["num_people"])
    for idx, row in df.iterrows():
        for j, col in enumerate(COLUMNS):
            if col not in NO_MIN_COLS:
                sum_range = slice(j, len(COLUMNS))
                df.at[idx, f"min_{col}"] = sum(row[sum_range])

    df.loc["Alle"] = sum(df.loc[role] for role in ROLES)
    yp_row = df.loc["YP"]
    ul_row = df.loc["UL"]
    df.loc["YP+UL"] = yp_row + ul_row
    for col in df.columns:
        yp_val = float(yp_row[col])  # type: ignore
        ul_val = float(ul_row[col])  # type: ignore
        if ul_val != 0:
            df.at["YP:UL", col] = yp_val / ul_val

    _LOGGER.info("Resulting base pandas DataFrame:\n%s", textwrap.indent(str(df), "  "))

    export_df = df.copy()
    export_df.insert(0, "role", list(df.index))
    wsjrdp2027.write_dataframe_to_xlsx(export_df, xlsx_filename)


if __name__ == "__main__":
    sys.exit(main())
