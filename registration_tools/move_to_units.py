#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import re
import sys

import pandas as _pandas
import psycopg as _psycopg
import wsjrdp2027
import wsjrdp2027.keycloak
import wsjrdp2027.mailbox


_LOGGER = logging.getLogger(__name__)


def create_argument_parser():
    import argparse

    from wsjrdp2027 import to_date_or_none

    p = argparse.ArgumentParser()
    p.add_argument(
        "--tag",
        "-t",
        dest="tags",
        action="append",
        default=[],
        help="Add tag, can be specified multiple times",
    )
    p.add_argument(
        "--limit",
        type=int,
        help="Limitiert die Anzahl der Personen, die berücksichtigt werden (für Testzwecke).",
    )
    p.add_argument("--skip-email", action="store_true", default=None)
    p.add_argument("--skip-db-updates", action="store_true", default=None)
    p.add_argument("--zip-eml", action="store_true", default=None)
    p.add_argument("--no-zip-eml", dest="zip_eml", action="store_false")
    p.add_argument(
        "--collection-date",
        type=to_date_or_none,
        default=None,
        help="Collection date of the next SEPA direct debit. "
        "Computes SEPA direct debit information if set. "
        "Setting the collection date does not imply writing of payment information.",
    )
    p.add_argument("yaml_file")
    return p


def update_config_from_ctx(
    config: wsjrdp2027.BatchConfig,
    new_primary_group_id: str,
    where_unit_code: str,
) -> wsjrdp2027.BatchConfig:
    _LOGGER.info(
        "Setting new_primary_group_id to %s, where Unit Code %s",
        new_primary_group_id,
        where_unit_code,
    )

    config.updates["new_primary_group_id"] = new_primary_group_id
    config.query.where.unit_code = where_unit_code

    return config


_HEX_COLOR_RE = re.compile(r"#([0-9A-Fa-f]{6})\b")


def extract_unit_code(description: str) -> str | None:
    if not description:
        return None
    m = _HEX_COLOR_RE.search(str(description))
    return f"#{m.group(1).upper()}" if m else None


def filter_people_by_unit_code(
    groups_df: _pandas.DataFrame,
    people_df: _pandas.DataFrame,
    people_unit_col: str = "unit_code",
    group_id_col: str = "group_id",
    description_col: str = "description",
) -> dict[str, _pandas.DataFrame]:
    result: dict[str, _pandas.DataFrame] = {}
    for _, grow in groups_df.iterrows():
        gid = grow.get(group_id_col)
        desc = grow.get(description_col, "")
        unit = extract_unit_code(desc)
        if unit is None:
            # store empty DataFrame for groups without a unit_code (optional)
            result[str(gid)] = people_df.iloc[0:0].copy()
            continue
        filtered = people_df[people_df[people_unit_col].astype(str) == str(unit)].copy()
        result[str(gid)] = filtered
    return result


def update_and_mail(
    batch_config,
    ctx: wsjrdp2027.WsjRdpContext,
    gid: str,
    unit: str,
    df: _pandas.DataFrame,
):
    batch_config = update_config_from_ctx(batch_config, gid, unit)

    prepared_batch = ctx.load_people_and_prepare_batch(batch_config, df_cb=lambda _: df)
    ctx.update_db_and_send_mailing(prepared_batch, zip_eml=ctx.parsed_args.zip_eml)

    _LOGGER.info("Update and mailing done for group %s (unit %s)", gid, unit)


def update_group_description(con: _psycopg.Connection, gid: str, df: _pandas.DataFrame):
    new_description = "Die Unit Leader sind:"
    for _, row in df.iterrows():
        new_description += f"  {row['short_first_name']} ({row['username']}@units.worldscoutjamboree.de),"

    new_description = new_description.rstrip(",")

    update_group_sql = """
        UPDATE groups
        SET description = %s
        WHERE id = %s
    """
    cur = con.cursor()
    cur.execute(update_group_sql, (new_description, gid))
    con.commit()
    cur.close()

    _LOGGER.info("Updating group %s description to:\n%s", gid, new_description)


def mail_and_move_to_units(conn, pdf: _pandas.DataFrame, ctx: wsjrdp2027.WsjRdpContext):
    groups_sql = """
            SELECT id AS group_id, name, short_name, description
            FROM groups ORDER BY name ASC
        """
    gdf = _pandas.read_sql(groups_sql, conn)
    _LOGGER.info("Found %s groups", len(gdf))
    # _LOGGER.info("Groups preview:\n%s", gdf.head().to_string())

    batch_config_ul = wsjrdp2027.BatchConfig.from_yaml(
        ctx.parsed_args.yaml_file + "-UL.yml"
    )
    batch_config_yp = wsjrdp2027.BatchConfig.from_yaml(
        ctx.parsed_args.yaml_file + "-YP.yml"
    )

    batch_config_name_ul = batch_config_ul.name
    batch_config_name_yp = batch_config_yp.name

    for _, grow in gdf.iterrows():
        gid = grow.get("group_id")
        desc = grow.get("description", "")
        unit = extract_unit_code(desc)

        batch_config_ul.name = f"{batch_config_name_ul}_group_{gid}"
        batch_config_yp.name = f"{batch_config_name_yp}_group_{gid}"

        if unit is None:
            continue
        filtered = pdf[pdf["unit_code"].astype(str) == str(unit)].copy()
        filtered = filtered[filtered["primary_group_id"].isin([2, 3])]
        filtered_ul = filtered[
            filtered["payment_role"].fillna("").astype(str).str.endswith("UL")
        ]
        filtered_yp = filtered[
            filtered["payment_role"].fillna("").astype(str).str.endswith("YP")
        ]
        _LOGGER.info(
            f"{grow.get('name')} -> {len(filtered_ul)} UL + {len(filtered_yp)} YP = {len(filtered)}"
        )

        try:
            # if(gid == 8): # Testgruppe
            create_ul_accounts(ctx, filtered_ul)
            update_group_description(con=conn, gid=gid, df=filtered_ul)
            update_and_mail(
                batch_config=batch_config_ul,
                ctx=ctx,
                gid=gid,
                unit=unit,
                df=filtered_ul,
            )
            update_and_mail(
                batch_config=batch_config_yp,
                ctx=ctx,
                gid=gid,
                unit=unit,
                df=filtered_yp,
            )
        except Exception as e:
            _LOGGER.error(
                "Error creating accounts for group %s (unit %s): %s", gid, unit, e
            )


def find_duplicate_usernames(df: _pandas.DataFrame) -> bool:
    counts = df["username"].value_counts()
    df["duplicate_count"] = df["username"].map(counts)
    return df["duplicate_count"] > 1


def create_ul_accounts(ctx: wsjrdp2027.WsjRdpContext, df: _pandas.DataFrame):
    for _, row in df.iterrows():
        username = row["username"]
        password = row["password"]
        firstname = row["first_name"]
        lastname = row["last_name"]
        email = username + "@units.worldscoutjamboree.de"

        _LOGGER.info(
            "Creating account for %s %s (%s %s)",
            firstname,
            lastname,
            username,
            password,
        )
        wsjrdp2027.keycloak.add_user(ctx, email, firstname, lastname, password)
        wsjrdp2027.keycloak.add_user_to_group(ctx, email, "UL")
        wsjrdp2027.mailbox.add_mailbox(
            ctx,
            username,
            "units.worldscoutjamboree.de",
            f"{firstname} {lastname}",
            password,
        )


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        out_dir="data/move_to_units{{ kind | omit_unless_prod | upper | to_ext }}",
    )

    out_base = ctx.make_out_path("move_to_units__{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    # _LOGGER.info("Test Password %s", wsjrdp2027._util.generate_password())
    # _LOGGER.info("Test Username %s",wsjrdp2027._util.generate_mail_username("Möbius-Walter mit coolem Zweitnamen", "von und zu Späßchen mit …	Uni汉字字符集cohànzìde¿Æ"))

    # test_firstname = "Test Firstname"
    # test_lastname = "Test Lastname"
    # test_mail = "test11"
    # wsjrdp2027.keycloak.add_user(ctx, f"{test_mail}@units.worldscoutjamboree.de", test_firstname, test_lastname, "password1234")
    # wsjrdp2027.keycloak.add_user_to_group(ctx, f"{test_mail}@units.worldscoutjamboree.de", "UL")
    # wsjrdp2027.mailbox.add_mailbox(ctx, test_mail, "units.worldscoutjamboree.de", f"Test Email", "password1234")

    # return

    with ctx.psycopg_connect() as conn:
        pdf = wsjrdp2027.load_people_dataframe(
            conn,
            query=wsjrdp2027.PeopleQuery(
                where=wsjrdp2027.PeopleWhere(exclude_deregistered=True)
            ),
        )
        _LOGGER.info("Found %s people", len(pdf))
        # _LOGGER.info("People preview:\n%s", pdf.head().to_string())

        pdf["skip_db_updates"] = False
        pdf["username"] = pdf.apply(
            lambda r: wsjrdp2027._util.generate_mail_username(
                str(r["first_name"]), str(r["last_name"])
            ),
            axis=1,
        )
        pdf["password"] = pdf.apply(
            lambda r: wsjrdp2027._util.generate_password(), axis=1
        )

        uldf = pdf[pdf["payment_role"].fillna("").astype(str).str.endswith("UL")]
        _LOGGER.info("Found %s Unit Leader", len(uldf))
        # _LOGGER.info("People preview:\n%s", pdf.head().to_string())

        # if(find_duplicate_usernames(uldf)).any():
        #     _LOGGER.error("Duplicate usernames found in UL dataset!")
        #     return

        mail_and_move_to_units(conn, pdf, ctx)

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
