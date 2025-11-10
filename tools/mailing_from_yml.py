#!/usr/bin/env -S uv run
from __future__ import annotations

import datetime
import email.message
import logging
import smtplib as _smtplib
import sys
import textwrap

import pandas as pd
import wsjrdp2027
import yaml


_LOGGER = logging.getLogger(__name__)


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
    p.add_argument("--start-time")
    p.add_argument("yaml_files", nargs="+")
    args = p.parse_args(argv[1:])
    args.today = wsjrdp2027.to_date(args.today)
    args.start_time = (
        wsjrdp2027.to_datetime(args.start_time) if args.start_time else None
    )
    return args


def main(argv=None):
    args = parse_args(argv=argv)
    ctx = wsjrdp2027.WsjRdpContext(
        setup_logging=True,
        start_time=args.start_time,
        out_dir="data/mailings{{ kind | omit_unless_prod | upper | to_ext }}",
        # log_level=logging.DEBUG,
    )
    out_base = ctx.make_out_path("mailing_{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    mailings = []

    for yaml_file in args.yaml_files:
        config = wsjrdp2027.MailingConfig.from_yaml(yaml_file)
        mailings.append(config.query_people_and_prepare_mailing(ctx=ctx))

    wsjrdp2027.send_mailings(
        ctx,
        mailings=mailings,
        send_message=args.email,
        out_dir=ctx.out_dir,
    )
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
