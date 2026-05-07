#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import pathlib as _pathlib
import sys

import wsjrdp2027


_SELF_NAME = _pathlib.Path(__file__).stem

_LOGGER = logging.getLogger(__name__)


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--skip-db-updates", action="store_true", default=None)
    p.add_argument(
        "--group",
        "-g",
        required=True,
        help="""Name or id of the group the confirmed person should be moved to.""",
    )
    p.add_argument("id", type=int)
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        skip_email=True,
        __file__=__file__,
    )
    person_id = int(ctx.parsed_args.id)

    print(flush=True)
    _LOGGER.info(f"Move person {person_id}")
    with ctx.psycopg_connect() as conn:
        person = ctx.load_person_for_id(person_id, conn=conn)
        out_base = ctx.make_out_path(
            f"{_SELF_NAME}_{person.id_and_name}".replace(" ", "_")
            + "_{{ filename_suffix }}"
        )
        batch_name = out_base.name
        log_filename = out_base.with_suffix(".log")
        ctx.configure_log_file(log_filename)

        new_group: str | int | None = ctx.parsed_args.group
        if new_group is None:
            _LOGGER.error(f"Missing --group to move to: {person.role_id_name}")
            raise SystemExit(1)

        person.move_to_group(new_group, ctx=ctx, batch_name=batch_name)

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
