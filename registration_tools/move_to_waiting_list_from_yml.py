#!/usr/bin/env -S uv run
from __future__ import annotations

import logging as _logging
import sys

import pandas as pd
import psycopg as _psycopg
import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


def parse_args(argv=None):
    import argparse
    import sys

    if argv is None:
        argv = sys.argv
    p = argparse.ArgumentParser()
    p.add_argument(
        "--email",
        action="store_true",
        default=True,
        help="Actually send out email messages",
    )
    p.add_argument(
        "--no-email",
        dest="email",
        action="store_false",
        help="Do not send out email messages",
    )
    p.add_argument(
        "--today",
        metavar="TODAY",
        default="TODAY",
        help="Run as if the current date is TODAY",
    )
    p.add_argument("yaml_file")
    args = p.parse_args(argv[1:])
    args.today = wsjrdp2027.to_date(args.today)
    return args


def change_primary_group_id(conn: _psycopg.Connection, *, df: pd.DataFrame) -> None:
    update_people_query = (
        "UPDATE people SET primary_group_id = %(new_primary_group_id)s "
        "WHERE id = %(id)s AND primary_group_id = %(primary_group_id)s "
        "RETURNING id"
    )
    update_roles_query = (
        "UPDATE roles SET group_id = %(new_primary_group_id)s "
        "WHERE person_id = %(id)s AND group_id = %(primary_group_id)s "
        "RETURNING id"
    )

    skipped_ids = set()
    failed_ids = set()

    df_len = len(df)

    with conn.cursor() as cur:
        with conn.transaction() as db_tx:
            for i, (_, row) in enumerate(df.iterrows(), start=1):
                pcnt = (i / df_len) * 100.0

                summary = (
                    f"{i}/{df_len} ({pcnt:.1f}) {row['id']} {row['short_full_name']}"
                )
                if row["primary_group_id"] == row["new_primary_group_id"]:
                    _LOGGER.info(
                        "%s SKIPPED primary_group_id stays at %s",
                        summary,
                        row["primary_group_id"],
                    )
                    skipped_ids.add(row["id"])
                    continue
                params = {
                    "id": row["id"],
                    "primary_group_id": row["primary_group_id"],
                    "new_primary_group_id": row["new_primary_group_id"],
                }
                _LOGGER.debug("  update_people_query: %s", update_people_query)
                cur.execute(update_people_query, params)
                update_people_results = cur.fetchall()
                _LOGGER.debug(
                    "  update people results: %s", repr(update_people_results)
                )
                updated_ids = set(r[0] for r in update_people_results)
                if updated_ids != set([row["id"]]):
                    _LOGGER.error("%s - failed to update people table", summary)
                    _LOGGER.error("  query: %s", update_people_query)
                    _LOGGER.error("  params: %s", repr(params))
                    _LOGGER.error("  results: %s", repr(update_people_results))
                    _LOGGER.error("  expected to find updated id %s", row["id"])
                    skipped_ids.add(row["id"])
                    failed_ids.add(row["id"])
                _LOGGER.debug("  update_roles_query: %s", update_roles_query)
                cur.execute(update_roles_query, params)
                update_roles_results = cur.fetchall()
                _LOGGER.debug("  update roles results: %s", repr(update_roles_results))
                updated_roles_ids = [r[0] for r in update_roles_results]
                if len(updated_roles_ids) != 1:
                    _LOGGER.error("%s - failed to update roles table", summary)
                    _LOGGER.error("  query: %s", update_roles_query)
                    _LOGGER.error("  params: %s", repr(params))
                    _LOGGER.error("  results: %s", repr(update_roles_results))
                    _LOGGER.error("  expected to find _one_ updated id")
                    skipped_ids.add(row["id"])
                    failed_ids.add(row["id"])
                _LOGGER.info(
                    "%s updated primary_group_id: %s -> %s",
                    summary,
                    row["primary_group_id"],
                    row["new_primary_group_id"],
                )
            if skipped_ids or failed_ids:
                raise _psycopg.Rollback(db_tx)
    if skipped_ids or failed_ids:
        _LOGGER.error("")
        _LOGGER.error("Failed to update all people: ROLLBACK")
        _LOGGER.error("  skipped_ids: %s", sorted(skipped_ids))
        _LOGGER.error("  failed_ids: %s", sorted(failed_ids))
        raise RuntimeError("Failed to update all people: ROLLBACK")


def main(argv=None):
    args = parse_args(argv=argv)
    ctx = wsjrdp2027.WsjRdpContext(
        setup_logging=True,
        out_dir="data/move_to_waiting_list{{ kind | omit_unless_prod | upper | to_ext }}",
    )
    mailing_config = wsjrdp2027.MailingConfig.from_yaml(args.yaml_file)

    ctx.out_dir = ctx.make_out_path(mailing_config.name + "__{{ filename_suffix }}")
    out_base = ctx.make_out_path(mailing_config.name)
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    prepared_mailing = mailing_config.query_people_and_prepare_mailing(ctx=ctx)
    prepared_mailing.out_dir = ctx.out_dir

    df = prepared_mailing.df

    df["new_primary_group_id"] = int(
        mailing_config.action_arguments["new_primary_group_id"]
    )

    if not mailing_config.action_arguments.get("email", True):
        args.email = False

    ctx.require_approval_to_run_in_prod()

    with ctx.psycopg_connect() as conn:
        change_primary_group_id(conn, df=prepared_mailing.df)

    wsjrdp2027.send_mailings(
        ctx,
        mailings=prepared_mailing,
        send_message=args.email,
        out_dir=ctx.out_dir,
    )
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
