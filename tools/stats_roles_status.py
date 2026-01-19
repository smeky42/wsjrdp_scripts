#!/usr/bin/env -S uv run
"""Dumps the database and restores it into dev (config-dev.yml)."""

from __future__ import annotations

import logging
import sys
import textwrap

import pandas as pd
import psycopg.rows
import wsjrdp2027


_LOGGER = logging.getLogger(__name__)


STATS_ROLES_STATUS_SQL_STMT = """
WITH group_names (id, name) AS (VALUES
 (1, 'CMT'),
 (2, 'UL'),
 (3, 'YP'),
 (4, 'IST'),
 (5, 'UL Warteliste'),
 (6, 'YP Warteliste'),
 (7, 'IST Warteliste'),
 (45, 'BMT')
)

SELECT group_names.name AS role, status, COUNT(people.id) AS num_people FROM people
JOIN group_names ON people.primary_group_id = group_names.id
WHERE primary_group_id IN (1, 2, 3, 4, 5, 6, 7, 45)

GROUP BY group_names.name, people.primary_group_id, status
ORDER BY
  primary_group_id ASC,
  array_position(array['registered','printed','upload','in_review','reviewed','confirmed','deregistration_noted', 'deregistered'], status)
"""

REG_ROLES = ["CMT", "UL", "YP", "IST"]
WAIT_ROLES = ["UL Warteliste", "YP Warteliste", "IST Warteliste", "BMT"]
ALL_ROLES = REG_ROLES + WAIT_ROLES

COLUMNS = [
    "deregistered",
    "deregistration_noted",
    "registered",
    "printed",
    "upload",
    "in_review",
    "reviewed",
    "confirmed",
]


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(setup_logging=True, out_dir="data")
    out_base = ctx.make_out_path("stats_roles_status_{{ filename_suffix }}")
    xlsx_filename = out_base.with_suffix(".xlsx")

    with ctx.psycopg_connect() as conn:
        cur = conn.cursor(row_factory=psycopg.rows.dict_row)
        cur.execute(STATS_ROLES_STATUS_SQL_STMT)
        rows = cur.fetchall()
        cur.close()
        base_df = pd.DataFrame(rows)
    _LOGGER.debug(
        "Resulting base pandas DataFrame:\n%s", textwrap.indent(str(base_df), "  ")
    )

    NO_MIN_COLS = [
        "deregistered",
        "deregistration_noted",
        "in_review",
        "confirmed",
    ]
    add_min_cols = True

    if add_min_cols:
        df = pd.DataFrame(
            columns=[
                *COLUMNS,
                *(f"min_{col}" for col in COLUMNS if col not in NO_MIN_COLS),
            ]
        )
    else:
        df = pd.DataFrame(columns=COLUMNS.copy())
    for role in REG_ROLES:
        for col in df.columns[0:]:
            df.at[role, col] = 0
    df.loc[len(df)] = [""] * len(df.columns)
    for role in WAIT_ROLES:
        for col in df.columns[0:]:
            df.at[role, col] = 0
    for _, row in base_df.iterrows():
        df.at[row["role"], row["status"]] = int(row["num_people"])
    if add_min_cols:
        for idx, row in df.iterrows():
            if not isinstance(idx, str):
                continue
            for j, col in enumerate(COLUMNS):
                if col not in NO_MIN_COLS:
                    sum_range = slice(j, len(COLUMNS))
                    df.at[idx, f"min_{col}"] = sum(
                        x for x in row[sum_range] if isinstance(x, (float, int))
                    )

    df.loc[len(df)] = [""] * len(df.columns)
    df.loc["Alle"] = sum(df.loc[role] for role in REG_ROLES)
    df.loc["Alle (inkl. Warteliste)"] = sum(df.loc[role] for role in ALL_ROLES)
    df.loc[len(df)] = [""] * len(df.columns)
    # yp_row = df.loc["YP"]
    # ul_row = df.loc["UL"]
    # df.loc["YP+UL"] = yp_row + ul_row
    # for col in df.columns:
    #     yp_val = float(yp_row[col])
    #     ul_val = float(ul_row[col])
    #     if ul_val != 0:
    #         df.at["YP:UL", col] = yp_val / ul_val

    _LOGGER.info("Resulting base pandas DataFrame:\n%s", textwrap.indent(str(df), "  "))

    export_df = df.copy()
    export_df.insert(len(COLUMNS), "-", "")
    export_df.insert(
        0, "role", [(idx if isinstance(idx, str) else "") for idx in df.index]
    )
    header = [("" if col.startswith("-") else col) for col in export_df.columns]
    wsjrdp2027.write_dataframe_to_xlsx(
        export_df,
        xlsx_filename,
        add_autofilter=False,
        float_format="%.2f",
        header=header,
    )


if __name__ == "__main__":
    sys.exit(main())
