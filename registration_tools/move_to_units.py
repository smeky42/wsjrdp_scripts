#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import sys
import pandas as _pandas
import re

import wsjrdp2027

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


def update_batch_config_from_ctx(
    config: wsjrdp2027.BatchConfig,
    ctx: wsjrdp2027.WsjRdpContext,
    new_primary_group_id: str,
    where_unit_code: str,
) -> wsjrdp2027.BatchConfig:
    config = config.replace(
        dry_run=ctx.dry_run,
        skip_email=ctx.parsed_args.skip_email,
        skip_db_updates=ctx.parsed_args.skip_db_updates,
    )
    args = ctx.parsed_args

    _LOGGER.info("Setting new_primary_group_id to %s", new_primary_group_id)
    _LOGGER.info("Updates %s", config.updates)
    config.updates["new_primary_group_id"] = new_primary_group_id

    query = config.query
    query.now = ctx.start_time
    query.where.unit_code = where_unit_code

    if (limit := args.limit) is not None:
        query.limit = limit
    if (collection_date := args.collection_date) is not None:
        query.collection_date = wsjrdp2027.to_date(collection_date)
    if (dry_run := ctx.dry_run) is not None:
        config.dry_run = dry_run
    if tags := getattr(args, "tags"):
        config.updates.setdefault("add_tags", []).extend(tags)
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
) -> Dict[str, _pandas.DataFrame]:
    result: Dict[str, _pandas.DataFrame] = {}
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


def update_and_mail(batch_config, ctx: wsjrdp2027.WsjRdpContext, gid: str, unit: str):
    batch_config = update_batch_config_from_ctx(batch_config, ctx, gid, unit)

    out_base = ctx.make_out_path(batch_config.name)
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    prepared_batch = ctx.load_people_and_prepare_batch(batch_config)
    ctx.update_db_and_send_mailing(prepared_batch, zip_eml=ctx.parsed_args.zip_eml)

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


def mail_and_move_to_units(conn, pdf: _pandas.DataFrame, ctx: wsjrdp2027.WsjRdpContext):

    groups_sql = """
            SELECT id AS group_id, name, short_name, description
            FROM groups ORDER BY name ASC
        """
    gdf = _pandas.read_sql(groups_sql, conn)
    _LOGGER.info("Found %s groups", len(gdf))
    # _LOGGER.info("Groups preview:\n%s", gdf.head().to_string())

    batch_config_ul = wsjrdp2027.BatchConfig.from_yaml(ctx.parsed_args.yaml_file + "-UL.yml")
    batch_config_yp = wsjrdp2027.BatchConfig.from_yaml(ctx.parsed_args.yaml_file + "-YP.yml")

    ctx.out_dir = ctx.make_out_path(batch_config_ul.name + "__{{ filename_suffix }}")


    for _, grow in gdf.iterrows():
        gid = grow.get("group_id")
        desc = grow.get("description", "")
        unit = extract_unit_code(desc)
        if unit is None:
            continue
        filtered = pdf[pdf["unit_code"].astype(str) == str(unit)].copy()
        filtered_ul = filtered[filtered["payment_role"].fillna("").astype(str).str.endswith("UL")]
        filtered_yp = filtered[filtered["payment_role"].fillna("").astype(str).str.endswith("YP")]
        _LOGGER.info(grow.get("name")," -> " , len(filtered_ul), "UL + ", len(filtered_yp), "YP = ", len(filtered))

        update_and_mail(batch_config_ul,ctx, gid, unit)

        update_and_mail(batch_config_yp,ctx, gid, unit)
        

def find_duplicate_usernames(df: _pandas.DataFrame) -> bool:
    counts = df['username'].value_counts()
    df['duplicate_count'] = df['username'].map(counts)
    return df['duplicate_count'] > 1


def create_accounts(ctx: wsjrdp2027.WsjRdpContext, df: _pandas.DataFrame):
    for _, row in df.iterrows():
        username = row['username']
        password = row['password']
        firstname = row['first_name']
        lastname = row['last_name']
        email = username + "@units.worldscoutjamboree.de"

        _LOGGER.info("Creating account for %s (%s %s)", username, firstname, lastname)
        # wsjrdp2027.Keycloak.add_user(ctx,email, firstname, lastname, password)
        # wsjrdp2027.Mailbox.add_mailbox(ctx, username, "units.worldscoutjamboree.de", f"{firstname} {lastname}", password)

def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        out_dir="data/mailings{{ kind | omit_unless_prod | upper | to_ext }}",
    )

    _LOGGER.info("Test Password %s", wsjrdp2027._util.generate_password())
    _LOGGER.info("Test Username %s",wsjrdp2027._util.generate_mail_username("Möbius-Walter mit coolem Zweitnamen", "von und zu Späßchen mit …	Uni汉字字符集cohànzìde¿Æ"))
    

    wsjrdp2027.Keycloak.add_user(ctx, "test.email@units.worldscoutjamboree.de", "firstname", "lastname", "password")
    wsjrdp2027.Mailbox.add_mailbox(ctx, "test.email", "units.worldscoutjamboree.de", f"Test Email", "password")
    
    with ctx.psycopg_connect() as conn:
        pdf = wsjrdp2027.load_people_dataframe(
            conn,
            query=wsjrdp2027.PeopleQuery(
                where=wsjrdp2027.PeopleWhere(exclude_deregistered=True)
            ),
        )
        _LOGGER.info("Found %s people", len(pdf))
        # _LOGGER.info("People preview:\n%s", pdf.head().to_string())
        
        pdf['username'] = pdf.apply(lambda r: wsjrdp2027._util.generate_mail_username(str(r["first_name"]), str(r["last_name"])), axis=1)
        pdf['password'] = pdf.apply(lambda r: wsjrdp2027._util.generate_password(), axis=1)

        uldf = pdf[pdf["payment_role"].fillna("").astype(str).str.endswith("UL")]
        _LOGGER.info("Found %s Unit Leader", len(uldf))
        # _LOGGER.info("People preview:\n%s", pdf.head().to_string())

        if(find_duplicate_usernames(uldf)).any():
            _LOGGER.error("Duplicate usernames found in UL dataset!")
            return
        
        if(not ctx.dry_run):
            _LOGGER.warning("Creating accounts for Unit Leaders")
            #create_accounts(ctx, uldf)

        mail_and_move_to_units(conn, pdf, ctx)


if __name__ == "__main__":
    sys.exit(main())
