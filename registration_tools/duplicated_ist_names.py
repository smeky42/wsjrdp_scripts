#!/usr/bin/env -S uv run
from __future__ import annotations

import argparse
import re
import sys

import pandas as pd
import wsjrdp2027
from psycopg.sql import SQL


DISPLAY_COLUMNS = [
    "group",
    "id",
    "first_name",
    "last_name",
    "status",
    "status_de",
]


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Findet Personen mit identischem Nachnamen in BMT und allen mit IST "
            "beginnenden Gruppen. "
            "Abgemeldete Anmeldungen werden mitgeprüft."
        )
    )
    return parser


def normalize_name(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value).strip()).casefold()


def find_duplicate_names(people: pd.DataFrame) -> pd.DataFrame:
    people = people.copy()
    people["_last_name"] = people["last_name"].map(normalize_name)

    has_name = people["_last_name"] != ""
    duplicate = people.duplicated(subset=["_last_name"], keep=False)

    return (
        people.loc[has_name & duplicate, DISPLAY_COLUMNS + ["_last_name"]]
        .sort_values(["_last_name", "first_name", "id"])
        .drop(columns=["_last_name"])
        .reset_index(drop=True)
    )


def main(argv: list[str] | None = None) -> int:
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    )

    with ctx.psycopg_connect() as conn:
        group_dicts = wsjrdp2027.pg_select_groups_dicts_for_where(
            conn,
            where=SQL(
                "id = 45 OR name ILIKE 'IST%' OR short_name ILIKE 'IST%' "
                "OR additional_info->>'group_code' ILIKE 'IST%'"
            ),
        )
        groups = [wsjrdp2027.Group(**group) for group in group_dicts]
        where = wsjrdp2027.PeopleWhere(
            primary_group_id=[group.id for group in groups],
            exclude_deregistered=False,
        )

        people = wsjrdp2027.load_people_dataframe(
            conn,
            query=wsjrdp2027.PeopleQuery(
                where=where,
                limit=None,
                offset=None,
            ),
            log_resulting_data_frame=False,
        )

    group_names = {group.id: group.name for group in groups}
    people["group"] = people["primary_group_id"].map(group_names)
    duplicates = find_duplicate_names(people)

    if duplicates.empty:
        print("Keine mehrfach vorkommenden Nachnamen in BMT/IST-Gruppen gefunden.")
        return 0

    duplicate_name_count = len(
        {normalize_name(last_name) for last_name in duplicates["last_name"]}
    )
    print(duplicates.to_string(index=False))
    print(
        f"\n{len(duplicates)} Anmeldungen in "
        f"{duplicate_name_count} Nachnamensgruppen gefunden."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
