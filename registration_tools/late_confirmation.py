#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import pathlib as _pathlib
import pprint
import sys

import pandas as _pandas
import psycopg as _psycopg
import wsjrdp2027


_SELFDIR = _pathlib.Path(__file__).parent
_SELF_NAME = _pathlib.Path(__file__).stem

_LOGGER = logging.getLogger(__name__)


def create_argument_parser():
    import argparse

    from wsjrdp2027 import to_date_or_none

    p = argparse.ArgumentParser()
    p.add_argument("--skip-email", action="store_true", default=None)
    p.add_argument("--skip-db-updates", action="store_true", default=None)
    p.add_argument(
        "--collection-date",
        type=to_date_or_none,
        default=None,
        help="Collection date of the next SEPA direct debit. "
        "Computes SEPA direct debit information if set. "
        "Setting the collection date does not imply writing of payment information.",
        required=True,
    )
    p.add_argument(
        "--unit-id",
        help="""e.g., A1 or K3 - required to confirm YP or UL
        unless --new-primary-group-id is given""",
    )
    p.add_argument(
        "--new-primary-group-id",
        type=int,
        default=None,
        help="New primary_group_id to use",
    )
    p.add_argument("--bcc", action="append")
    p.add_argument("id", type=int)
    return p


def update_batch_config_from_ctx(
    config: wsjrdp2027.BatchConfig,
    ctx: wsjrdp2027.WsjRdpContext,
) -> wsjrdp2027.BatchConfig:
    config = config.replace(
        dry_run=ctx.dry_run,
        skip_email=ctx.parsed_args.skip_email,
        skip_db_updates=ctx.parsed_args.skip_db_updates,
    )
    args = ctx.parsed_args
    query = config.query
    query.now = ctx.start_time
    if (limit := args.limit) is not None:
        query.limit = limit
    if (collection_date := args.collection_date) is not None:
        query.collection_date = wsjrdp2027.to_date(collection_date)
    if (dry_run := ctx.dry_run) is not None:
        config.dry_run = dry_run
    return config


def _load_person_row(
    ctx: wsjrdp2027.WsjRdpContext, conn: _psycopg.Connection, person_id: int
) -> _pandas.Series:
    df = wsjrdp2027.load_people_dataframe(
        conn, where=wsjrdp2027.PeopleWhere(id=person_id), log_resulting_data_frame=False
    )
    if df.empty:
        _LOGGER.error(f"Could not load person with id {person_id}")
        raise SystemExit(1)
    row = df.iloc[0]
    if row["status"] not in ("reviewed", "confirmed"):
        _LOGGER.error(
            f"Person {row['id_and_name']} has status {row['status']!r}, expected 'reviewed' or 'confirmed'"
        )
        if ctx.is_production:
            raise SystemExit(1)
        else:
            _LOGGER.error("NOT IN PROD => continue")
    return row


def _select_group_id_for_group_name(conn, group_name: str) -> int:
    list_of_rows = wsjrdp2027.pg_select_dict_rows(
        conn, t'SELECT * FROM "groups" WHERE "name" = {group_name}'
    )
    if len(list_of_rows) == 0:
        _LOGGER.error(f"Found no group with name {group_name!r}")
        raise SystemExit(1)
    elif len(list_of_rows) != 1:
        _LOGGER.error(f"Expected to find ONE group with name {group_name!r}, found:")
        for row in list_of_rows:
            _LOGGER.error(f"  id={row['id']} name={row['id']}")
    return list_of_rows[0]["id"]


def _select_group_name_for_group_id(conn, group_id: int) -> str:
    list_of_rows = wsjrdp2027.pg_select_dict_rows(
        conn, t'SELECT * FROM "groups" WHERE "id" = {group_id}'
    )
    if len(list_of_rows) == 0:
        _LOGGER.error(f"Found no group with id {group_id!r}")
        raise SystemExit(1)
    elif len(list_of_rows) != 1:
        _LOGGER.error(f"Expected to find ONE group with id {group_id!r}, found:")
        for row in list_of_rows:
            _LOGGER.error(f"  id={row['id']} name={row['id']}")
    return list_of_rows[0]["name"]


def _confirm_person(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    person_row: _pandas.Series,
    wsj_role: str,
    batch_name: str,
) -> None:
    is_yp_or_ul = wsj_role in ["YP", "UL"]

    person_id: int = int(person_row["id"])
    role_id_name = f"{wsj_role} {person_row['id_and_name']}"
    unit_id: str | None = ctx.parsed_args.unit_id
    new_primary_group_id: int | None = ctx.parsed_args.new_primary_group_id
    bcc = wsjrdp2027.to_str_list(ctx.parsed_args.bcc)

    # Check unit_id and new_primary_group_id
    if new_primary_group_id is not None:
        _LOGGER.info(
            f"Use new_primary_group_id={new_primary_group_id} (from --new-primary-group-id)"
        )
        if is_yp_or_ul:
            new_unit_id = _select_group_name_for_group_id(conn, new_primary_group_id)
            if unit_id is not None and new_unit_id != unit_id:
                _LOGGER.error(
                    f"Inconsistent arguments: --new-primary-group-id={new_primary_group_id} "
                    f"(=> unit_id={new_unit_id}), but --unit-id={unit_id} given"
                )
                raise SystemExit(1)
            unit_id = new_unit_id
    elif is_yp_or_ul:
        if unit_id:
            new_primary_group_id = _select_group_id_for_group_name(conn, unit_id)
            _LOGGER.info(
                f"Use new_primary_group_id={new_primary_group_id} (from --unit-id={unit_id})"
            )
        else:
            _LOGGER.error(
                f"Missing --unit-id (or --new-primary-group-id) for confirmation of {role_id_name}"
            )
            raise SystemExit(1)

    batch_config = wsjrdp2027.BatchConfig.from_yaml(
        _SELFDIR / f"late_confirmation_{wsj_role}.yml",
        name=batch_name,
    )
    if bcc:
        batch_config.extra_email_bcc = bcc

    batch_config.query = batch_config.query.replace(
        where=wsjrdp2027.PeopleWhere(id=person_id),
        collection_date=ctx.parsed_args.collection_date,
    )
    batch_config.updates.update(
        {
            "add_tags": f"{wsj_role}-Confirmation-Mail",
            "remove_tags": ["Warteliste"],
        }
    )
    if new_primary_group_id is not None:
        batch_config.updates["new_primary_group_id"] = new_primary_group_id

    _LOGGER.info("Query:\n%s", batch_config.query)
    _LOGGER.info("Updates:\n%s", pprint.pformat(batch_config.updates))

    prepared_batch = ctx.load_people_and_prepare_batch(batch_config)
    ctx.update_db_and_send_mailing(prepared_batch, zip_eml=False)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        out_dir="data/" + _SELF_NAME + "{{ kind | omit_unless_prod | upper | to_ext }}",
    )
    person_id = int(ctx.parsed_args.id)
    print(flush=True)
    _LOGGER.info(f"Confirm person {person_id}")
    with ctx.psycopg_connect() as conn:
        person_row = _load_person_row(ctx, conn, person_id=person_id)
        short_role_name = person_row["payment_role"].short_role_name
        role_id_name = "-".join(
            [
                short_role_name,
                str(person_id),
                person_row["short_full_name"].replace(" ", "_"),
            ]
        )
        batch_name = f"late_confirmation_{role_id_name}"
        log_filename = ctx.make_out_path(batch_name).with_suffix(".log")
        ctx.configure_log_file(log_filename)

        _confirm_person(
            ctx=ctx,
            conn=conn,
            person_row=person_row,
            wsj_role=short_role_name,
            batch_name=batch_name,
        )

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
