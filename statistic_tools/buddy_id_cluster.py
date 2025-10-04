#!/usr/bin/env -S uv run
from __future__ import annotations

import logging as _logging
import re
import sys

import networkx as nx
import pandas as pd
import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


BUDDY_ID_CLUSTER_COLUMNS = [
    "cluster_id",
    "primary_group_id",
    "id",
    "status",
    "full_name",
    "greeting_name",
    "rdp_association",
    "rdp_association_region",
    "rdp_association_sub_region",
    "rdp_association_group",
    "zip_code",
    "buddy_id",
    "buddy_id_ul",
    "buddy_id_yp",
    "email",
    "birthday",
    "gender",
]


def _rdp_path(row) -> str:
    if row is None:
        return "n/a"
    else:
        return f"{row['rdp_association']}: {row['rdp_association_region']}: {row['rdp_association_sub_region']}: {row['rdp_association_group']}"


def _is_deregistered(row) -> bool:
    if row is None:
        return True
    else:
        return row.get("status") in ("deregistration_noted", "deregistered")


def _is_registered(row) -> bool:
    if row is None:
        return False
    else:
        return row.get("status") in ("registered")


def create_buddy_id_graph(df: pd.DataFrame) -> nx.Graph:
    id2row = {row["id"]: row for _, row in df.iterrows()}

    def buddy_id_to_id(id: int, s: str | None) -> int | None:
        try:
            return int(s.rsplit("-", 1)[1]) if s else None
        except Exception as exc:
            _LOGGER.warning("id=%s, invalid buddy-id:%s", id, s)

    def is_valid_printed_or_higher_id(id: int | None) -> bool:
        if id is None:
            return False
        else:
            row = id2row.get(id)
            return not _is_deregistered(row) and not _is_registered(row)

    G = nx.Graph()
    for _, row in df.iterrows():
        if _is_deregistered(row) or _is_registered(row):
            continue
        id = row["id"]
        maybe_other_ids = [
            buddy_id_to_id(id, row["buddy_id_yp"]),
            buddy_id_to_id(id, row["buddy_id_ul"]),
        ]
        other_ids = sorted(
            set([n for n in maybe_other_ids if is_valid_printed_or_higher_id(n)])
        )

        for other_id in other_ids:
            G.add_edge(id, other_id)
    return G


def _is_ul(row) -> bool:
    if row is None:
        return False
    else:
        return row["primary_group_id"] == 2


def main():
    ctx = wsjrdp2027.WsjRdpContext(out_dir="data")
    out_base = ctx.make_out_path("buddy_id_cluster_{{ filename_suffix }}")
    xlsx_filename = out_base.with_suffix(".xlsx")

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(
            conn,
            extra_cols=[
                "buddy_id",
                "buddy_id_ul",
                "buddy_id_yp",
                "rdp_association",
                "rdp_association_region",
                "rdp_association_sub_region",
                "rdp_association_group",
                "zip_code",
            ],
            where="primary_group_id in (2, 3)",
            exclude_deregistered=False,
        )
    for idx, row in df.iterrows():
        buddy_id = f"{row['buddy_id']}-{row['id']}"
        df.at[idx, "buddy_id"] = buddy_id

    _LOGGER.info("Found %s YP and UL", len(df))

    id2row = {row["id"]: row for _, row in df.iterrows()}
    G = create_buddy_id_graph(df)
    all_conn_comps = sorted(nx.connected_components(G), key=len, reverse=True)

    conn_comps = [cc for cc in all_conn_comps if any(_is_ul(id2row[id]) for id in cc)]

    def id2key(id: int):
        try:
            row = id2row[id]
            return (row["primary_group_id"], _rdp_path(row), row["zip_code"], id)
        except KeyError:
            return (0, "n/a", "", id)

    id_in_cluster = set()
    ul_in_cluster = set()
    yp_in_cluster = set()

    comp_dfs = []

    for comp_idx, cc in enumerate(conn_comps):
        num_ul = sum(1 for id in cc if _is_ul(id2row.get(id)))
        num_yp = len(cc) - num_ul
        _LOGGER.info("#### %s Personen = %s ULs + %s YPs", len(cc), num_ul, num_yp)
        ids = sorted(list(cc), key=id2key)
        rows = []
        for id in ids:
            if (row := id2row.get(id)) is None:
                _LOGGER.warning("   !!!! id=%s", id)
                continue
            rows.append(row)
            id_in_cluster.add(id)
            primary_group_id = row["primary_group_id"]
            if primary_group_id == 2:
                ul_in_cluster.add(id)
            elif primary_group_id == 3:
                yp_in_cluster.add(id)
            print(
                f"    {row['primary_group_id']} {row['id']:4d} "
                f"{row['zip_code']} {row['greeting_name']} "
                f"{_rdp_path(row)}"
            )
        comp_df = pd.concat([r.to_frame().T for r in rows], ignore_index=True)
        comp_df.insert(loc=0, column="cluster_id", value=[comp_idx] * len(cc))
        comp_df = comp_df.reindex(columns=BUDDY_ID_CLUSTER_COLUMNS)

        comp_dfs.append(comp_df)

    out_df = pd.concat(comp_dfs, keys=list(range(len(comp_dfs))))
    df_ul = df[df["primary_group_id"] == 2]
    df_yp = df[df["primary_group_id"] == 3]
    _LOGGER.info("Anzahl aller Buddy-ID Cluster: %s", len(all_conn_comps))
    _LOGGER.info("Anzahl der Buddy-ID Cluster mit ULs: %s", len(conn_comps))
    _LOGGER.info("Anzahl Personen: %s (inkl. registered!)", len(df))
    _LOGGER.info("Anzahl ULs: %s", len(df_ul))
    _LOGGER.info("Anzahl YPs: %s", len(df_yp))
    _LOGGER.info("Anzahl Personen in einem Cluster mit ULs: %s", len(id_in_cluster))
    _LOGGER.info("Anzahl ULs in einem Cluster mit ULs: %s", len(ul_in_cluster))
    _LOGGER.info("Anzahl YPs in einem Cluster mit ULs: %s", len(yp_in_cluster))

    print(out_df)

    out_df.reindex(columns=BUDDY_ID_CLUSTER_COLUMNS)

    wsjrdp2027.write_dataframe_to_xlsx(out_df, path=xlsx_filename)


if __name__ == "__main__":
    sys.exit(main())
