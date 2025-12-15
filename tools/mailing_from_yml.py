#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import sys

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
    if tags := getattr(args, "tags"):
        config.updates.setdefault("add_tags", []).extend(tags)
    return config


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        out_dir="data/mailings{{ kind | omit_unless_prod | upper | to_ext }}",
    )
    batch_config = wsjrdp2027.BatchConfig.from_yaml(ctx.parsed_args.yaml_file)
    batch_config = update_batch_config_from_ctx(batch_config, ctx)

    ctx.out_dir = ctx.make_out_path(batch_config.name + "__{{ filename_suffix }}")
    out_base = ctx.make_out_path(batch_config.name)
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    prepared_batch = ctx.load_people_and_prepare_batch(batch_config)

    ctx.update_db_and_send_mailing(prepared_batch, zip_eml=ctx.parsed_args.zip_eml)

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
