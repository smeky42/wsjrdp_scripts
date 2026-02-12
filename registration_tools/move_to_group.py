#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import pathlib as _pathlib
import pprint
import sys

import pandas as _pandas
import psycopg as _psycopg
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


def _confirmation_note(
    ctx: wsjrdp2027.WsjRdpContext,
    *,
    old_group: wsjrdp2027.Group | None,
    new_group: wsjrdp2027.Group,
) -> str:
    date_str = ctx.today.strftime("%d.%m.%Y")
    old_group_name = (old_group.short_name or old_group.name) if old_group else ""
    new_group_name = new_group.short_name or new_group.name

    if old_group is not None:
        if old_group.id != new_group.id:
            move_str = f"von {old_group_name} nach {new_group_name} verschoben"
        else:
            move_str = ""
    else:
        move_str = f"nach {new_group_name} verschoben"

    if move_str:
        return f"Am {date_str} {move_str}"
    else:
        return ""


def _move_person(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    person_row: _pandas.Series,
    wsj_role: str,
    batch_name: str,
) -> None:
    is_yp_or_ul = wsj_role in ["YP", "UL"]
    person_id: int = int(person_row["id"])
    old_primary_group_id = wsjrdp2027.to_int_or_none(person_row.get("primary_group_id"))
    old_group = (
        wsjrdp2027.Group.db_load(conn, group_arg=old_primary_group_id)
        if old_primary_group_id is not None
        else None
    )

    role_id_name = f"{wsj_role} {person_row['id_and_name']}"
    group_arg: str | int | None = ctx.parsed_args.group
    if group_arg is None:
        _LOGGER.error(f"Missing --group to move to: {role_id_name}")
        raise SystemExit(1)
    new_group = wsjrdp2027.Group.db_load(
        conn, group_arg, auto_group_id=old_primary_group_id
    )
    unit_code: str | None = new_group.unit_code if new_group else None

    # create new batch config
    batch_config = ctx.new_batch_config(
        name=batch_name,
        where=wsjrdp2027.PeopleWhere(id=person_id),
    )

    if new_group.id != old_primary_group_id:
        _LOGGER.info(
            f"Set new_primary_group_id={new_group.id} (derived from --group={group_arg})"
        )
        batch_config.updates["new_primary_group_id"] = new_group.id
    if note := _confirmation_note(ctx, old_group=old_group, new_group=new_group):
        batch_config.updates["add_note"] = note
    if is_yp_or_ul and (unit_code := new_group.unit_code):
        _LOGGER.info(
            f"Set new_unit_code={unit_code!r} (derived from --group={group_arg})"
        )
        if unit_code != person_row["unit_code"]:
            batch_config.updates["new_unit_code"] = unit_code
    _LOGGER.info("Query:\n%s", batch_config.query)

    print(flush=True)
    _LOGGER.info(role_id_name)
    if batch_config.updates:
        _LOGGER.info("Updates:\n%s", pprint.pformat(batch_config.updates))
    else:
        _LOGGER.info("Updates: %r", batch_config.updates)
    print(flush=True)

    prepared_batch = ctx.load_people_and_prepare_batch(
        batch_config, log_resulting_data_frame=False
    )
    ctx.update_db_and_send_mailing(prepared_batch, silent_skip_email=True)


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
        person_row = wsjrdp2027.load_person_row(conn, person_id=person_id)
        short_role_name = person_row["payment_role"].short_role_name
        role_id_name = "-".join(
            [
                short_role_name,
                str(person_id),
                person_row["short_full_name"].replace(" ", "_"),
            ]
        )
        batch_name = f"{_SELF_NAME}_{role_id_name}"
        out_base = ctx.make_out_path(
            f"{_SELF_NAME}_{role_id_name}_{{{{ filename_suffix }}}}"
        )
        batch_name = out_base.name
        log_filename = out_base.with_suffix(".log")
        ctx.configure_log_file(log_filename)

        _move_person(
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
