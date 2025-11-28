#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import sys
import typing as _typing

import wsjrdp2027


if _typing.TYPE_CHECKING:
    import psycopg as _psycopg

_LOGGER = logging.getLogger(__name__)

ALLOWED_ACTION_ARGUMENTS = [
    "add_tags",
    # "new_status",
]


def create_argument_parser():
    import argparse

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
    p.add_argument("yaml_file")
    return p


def update_action_arguments_from_args(config: wsjrdp2027.MailingConfig, args) -> None:
    action_arguments = config.action_arguments
    updated = False
    if tags := getattr(args, "tags"):
        action_arguments.setdefault("add_tags", []).extend(tags)
        updated = True
    if updated:
        config.update_raw_yaml()


def update_db_for_action_arguments(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    prepared_mailing: wsjrdp2027.PreparedMailing,
) -> None:
    df = prepared_mailing.df
    df_len = len(df)
    with conn.cursor() as cursor:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            pcnt = (i / df_len) * 100.0

            summary = f"{i}/{df_len} ({pcnt:.1f}) {row['id']} {row['short_full_name']}"
            _LOGGER.info(summary)

            if tags := row.get("add_tags"):
                for tag in tags:
                    wsjrdp2027.pg_add_person_tag(cursor, person_id=row["id"], tag=tag)


def update_df_and_db_for_action_arguments(
    ctx: wsjrdp2027.WsjRdpContext, prepared_mailing: wsjrdp2027.PreparedMailing
) -> None:
    action_arguments = prepared_mailing.action_arguments

    db_updates = False
    df = prepared_mailing.df
    for k in ALLOWED_ACTION_ARGUMENTS:
        if v := action_arguments.get(k):
            db_updates = True
            df[k] = df.apply(lambda r: v, axis=1)

    if db_updates:
        if ctx.dry_run:
            _LOGGER.info("Skip DB updates (dry-run)")
        else:
            _LOGGER.info("Update DB")
            ctx.require_approval_to_run_in_prod()
            with ctx.psycopg_connect() as conn:
                update_db_for_action_arguments(
                    ctx=ctx, conn=conn, prepared_mailing=prepared_mailing
                )
    else:
        _LOGGER.info(
            "No DB updates given (neither as action_arguments in yaml nor as command line args)"
        )


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        out_dir="data/mailings{{ kind | omit_unless_prod | upper | to_ext }}",
    )
    mailing_config = wsjrdp2027.MailingConfig.from_yaml(ctx.parsed_args.yaml_file)
    ctx.out_dir = ctx.make_out_path(mailing_config.name + "__{{ filename_suffix }}")
    out_base = ctx.make_out_path(mailing_config.name)
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    update_action_arguments_from_args(mailing_config, ctx.parsed_args)
    prepared_mailing = mailing_config.query_people_and_prepare_mailing(
        ctx=ctx, limit=ctx.parsed_args.limit
    )
    prepared_mailing.out_dir = ctx.out_dir

    update_df_and_db_for_action_arguments(ctx, prepared_mailing)

    ctx.send_mailing(prepared_mailing)
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
