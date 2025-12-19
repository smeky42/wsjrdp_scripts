#!/usr/bin/env -S uv run
from __future__ import annotations

import logging as _logging
import sys
import typing

import networkx as nx
import pandas as pd
import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


BUDDY_ID_CLUSTER_COLUMNS = [
    "cluster_id",
    "primary_group_id",
    "id",
    "unit_code",
    "cluster_code",
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


def create_buddy_id_graph(
    df: pd.DataFrame, include_registered: bool = False
) -> nx.Graph[int]:
    id2row = {row["id"]: row for _, row in df.iterrows()}

    def buddy_id_to_id(id: int, s: str | None) -> int | None:
        try:
            return int(s.rsplit("-", 1)[1]) if s else None
        except Exception as exc:
            _LOGGER.warning("id=%s, invalid buddy-id:%s", id, s)

    def is_cluster_member_id(id: int | None) -> bool:
        if id is None:
            return False
        else:
            row = id2row.get(id)
            if _is_deregistered(row):
                return False
            elif not include_registered and _is_registered(row):
                return False
            else:
                return True

    G = nx.Graph[int]()
    for _, row in df.iterrows():
        if _is_deregistered(row):
            continue
        if not include_registered and _is_registered(row):
            continue
        id = row["id"]
        maybe_other_ids = [
            buddy_id_to_id(id, row["buddy_id_yp"]),
            buddy_id_to_id(id, row["buddy_id_ul"]),
        ]
        other_ids = sorted(
            set(n for n in maybe_other_ids if n is not None and is_cluster_member_id(n))
        )

        for other_id in other_ids:
            G.add_edge(id, other_id)
    return G


def _is_ul(row) -> bool:
    if row is None:
        return False
    else:
        return row["primary_group_id"] == 2


def _write_cluster_codes(ctx: wsjrdp2027.WsjRdpContext, df: pd.DataFrame) -> None:
    _LOGGER.info("Write cluster-codes to database")
    ctx.require_approval_to_run_in_prod()

    query = (
        """UPDATE people SET cluster_code = %(new_cluster_code)s WHERE id = %(id)s"""
    )
    values = [
        {
            "id": row["id"],
            "new_cluster_code": row["new_cluster_code"],
        }
        for _, row in df.iterrows()
        if row["new_cluster_code"]
        and row["new_cluster_code"] != row["old_cluster_code"]
    ]
    _LOGGER.info("  query: %s", query)
    _LOGGER.info("  %s rows to be updated", len(values))

    if len(values) > 0:
        with ctx.psycopg_connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, values)
                conn.commit()
                _LOGGER.info("  updated %s rows", cur.rowcount)
                _LOGGER.info("  status: %s", cur.statusmessage)
        _LOGGER.info("Finished cluster_code update")
    else:
        _LOGGER.info("Skipped cluster_code update (nothing to do)")


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument(
        "--write-cluster-codes",
        help="Write out the determined cluster codes (default False)",
        action="store_true",
        default=False,
    )
    p.add_argument(
        "--include-registered",
        help="""Include people in status 'registered' """
        """when searching clusters and writing cluster-codes. (default False)""",
        action="store_true",
        default=False,
    )
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        out_dir="data", argument_parser=create_argument_parser(), argv=argv
    )
    args = ctx.parsed_args
    out_base = ctx.make_out_path("buddy_id_cluster_{{ filename_suffix }}")
    xlsx_filename = out_base.with_suffix(".xlsx")

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(
            conn,
            extra_cols=[
                "buddy_id",
                "buddy_id_ul",
                "buddy_id_yp",
                "unit_code",
                "cluster_code",
            ],
            query=wsjrdp2027.PeopleQuery(
                where=wsjrdp2027.PeopleWhere(
                    primary_group_id=[2, 3], exclude_deregistered=False
                )
            ),
        )
        df["old_cluster_code"] = df["cluster_code"]
        df["new_cluster_code"] = None
    idx: int
    for idx, row in df.iterrows():  # type: ignore
        buddy_id = f"{row['buddy_id']}-{row['id']}"
        df.at[idx, "buddy_id"] = buddy_id

    _LOGGER.info("Found %s YP and UL", len(df))

    id2row = {row["id"]: row for _, row in df.iterrows()}
    id2idx = {row["id"]: typing.cast(int, idx) for idx, row in df.iterrows()}
    G = create_buddy_id_graph(df, include_registered=args.include_registered)
    all_conn_comps: list[set[int]] = sorted(  # ty: ignore
        nx.connected_components(G),  # ty: ignore
        key=len,
        reverse=True,
    )

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

    problematic_clusters = {}

    for comp_idx, cc in enumerate(conn_comps):
        num_ul = sum(1 for id in cc if _is_ul(id2row.get(id)))
        num_yp = len(cc) - num_ul
        _LOGGER.info("#### %s Personen = %s ULs + %s YPs", len(cc), num_ul, num_yp)
        ids = sorted(list(cc), key=id2key)
        rows = []
        ul_unit_codes = set()
        ul_in_comp = []
        for id in ids:
            if (row := id2row.get(id)) is None:
                _LOGGER.warning("   !!!! id=%s", id)
                continue
            rows.append(row)
            id_in_cluster.add(id)
            primary_group_id = row["primary_group_id"]
            if primary_group_id == 2:
                ul_in_comp.append(id)
                ul_in_cluster.add(id)
                if row["unit_code"]:
                    ul_unit_codes.add(row["unit_code"])
            elif primary_group_id == 3:
                yp_in_cluster.add(id)
            print(
                f"    {row['primary_group_id']} {row['id']:4d} "
                f"{row['zip_code']} {row['greeting_name']} "
                f"{row['unit_code'] or ''} "
                f"{_rdp_path(row)}"
            )
        if len(ul_unit_codes) == 1:
            cluster_code = next(iter(ul_unit_codes))
            print(f"    => cluster_code: {cluster_code}")
            for id in ids:
                df.at[id2idx[id], "cluster_code"] = cluster_code
                df.at[id2idx[id], "new_cluster_code"] = cluster_code
            for row in rows:
                row["cluster_code"] = cluster_code
                row["new_cluster_code"] = cluster_code
        else:
            print("    WARNUNG: Mehr als ein unit_code oder kein unit_code bei den ULs")
            problematic_clusters[comp_idx] = {
                "ul": ul_in_comp,
                "ul_unit_codes": ul_unit_codes,
                "ids": ids,
            }
        comp_df = pd.concat([r.to_frame().T for r in rows], ignore_index=True)
        comp_df.insert(loc=0, column="cluster_id", value=[comp_idx] * len(cc))
        comp_df = comp_df.reindex(columns=BUDDY_ID_CLUSTER_COLUMNS)

        comp_dfs.append(comp_df)

    if args.write_cluster_codes:
        _write_cluster_codes(ctx, df)

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

    def link_to_hitobito(id):
        hitobito_url = ctx.config.hitobito_url
        group_id = id2row[id]["primary_group_id"]
        tags = id2row[id]["tags"]
        tags = [t for t in tags if t.startswith("Region")]
        return f"{hitobito_url}/groups/{group_id}/people/{id}/unit - {tags}"

    if problematic_clusters:
        for comp_idx, pc in problematic_clusters.items():
            ids = pc["ids"]
            prefix = f"pax {len(ids)}"
            if len(pc["ul"]) == 0:
                id = ids[0]
                print(f"{prefix} Keine UL im Cluster - {link_to_hitobito(id)}")
            elif len(pc["ul_unit_codes"]) == 0:
                id = pc["ul"][0]
                print(f"{prefix} Keine UL mit Unit-code - {link_to_hitobito(id)}")
            else:
                id = pc["ul"][0]
                ul_unit_codes = pc["ul_unit_codes"]
                ul_unit_codes_str = " ".join(ul_unit_codes)
                print(
                    f"{prefix} Mehrere Unit-codes ({ul_unit_codes_str}) bei ULs im Cluster - {link_to_hitobito(id)}"
                )

    # print(out_df)
    out_df.reindex(columns=BUDDY_ID_CLUSTER_COLUMNS)

    wsjrdp2027.write_dataframe_to_xlsx(out_df, path=xlsx_filename)


if __name__ == "__main__":
    sys.exit(main())
