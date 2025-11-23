#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import sys

import wsjrdp2027


_LOGGER = logging.getLogger(__name__)


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("yaml_files", nargs="+")
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        out_dir="data/mailings{{ kind | omit_unless_prod | upper | to_ext }}",
        argument_parser=create_argument_parser(),
        argv=argv,
    )
    out_base = ctx.make_out_path("mailing_{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    mailings = []

    for yaml_file in ctx.parsed_args.yaml_files:
        config = wsjrdp2027.MailingConfig.from_yaml(yaml_file)
        mailings.append(config.query_people_and_prepare_mailing(ctx=ctx))

    ctx.send_mailings(mailings)
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
