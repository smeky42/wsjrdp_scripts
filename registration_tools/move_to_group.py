#!/usr/bin/env -S uv run
from __future__ import annotations

import dataclasses as _dataclasses
import logging
import pathlib as _pathlib
import pprint
import sys
import typing as _typing

import pandas as _pandas
import psycopg as _psycopg
import psycopg.sql as _psycopg_sql
import wsjrdp2027


if _typing.TYPE_CHECKING:
    import string.templatelib as _string_templatelib

    import psycopg.sql as _psycopg_sql


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


@_dataclasses.dataclass(kw_only=True)
class Group:
    id: int
    parent_id: int | None = None
    short_name: str | None = None
    name: str
    type: str | None = None
    email: str | None = None
    description: str
    additional_info: dict

    @property
    def unit_code(self) -> str | None:
        return self.additional_info.get("unit_code")

    @property
    def group_code(self) -> str | None:
        return self.additional_info.get("group_code")

    def __getitem__(self, key: str) -> _typing.Any:
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key) from None


def _select_group_for_where(
    conn, where: _psycopg_sql.Composable | _string_templatelib.Template
) -> Group:
    return Group(**wsjrdp2027.pg_select_group_dict_for_where(conn, where=where))


def _select_group_for_group_name(conn, group_name: str) -> Group:
    return _select_group_for_where(
        conn,
        t'"name" = {group_name} OR "short_name" = {group_name} OR "additional_info"->>\'group_code\' = {group_name}',
    )


def _select_group_for_group_id(conn, group_id: int) -> Group:
    return _select_group_for_where(conn, t'"id" = {group_id}')


def _select_group(
    conn, group_arg: str | int, *, auto_group_id: int | None = None
) -> Group:
    import re

    if isinstance(group_arg, int):
        return _select_group_for_group_id(conn, group_arg)
    elif re.fullmatch(group_arg, "[0-9]+"):
        return _select_group_for_group_id(conn, int(group_arg, base=10))
    elif group_arg == "auto":
        if auto_group_id is None:
            raise RuntimeError("group='auto' and auto_group_id=None not supported")
        return _select_group_for_group_id(conn, auto_group_id)
    else:
        return _select_group_for_group_name(conn, group_arg)


def _confirmation_note(
    ctx: wsjrdp2027.WsjRdpContext, *, old_group: Group | None, new_group: Group
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
        _select_group(conn, group_arg=old_primary_group_id)
        if old_primary_group_id is not None
        else None
    )

    role_id_name = f"{wsj_role} {person_row['id_and_name']}"
    group_arg: str | int | None = ctx.parsed_args.group
    if group_arg is None:
        _LOGGER.error(f"Missing --group to move to: {role_id_name}")
        raise SystemExit(1)
    new_group = _select_group(conn, group_arg, auto_group_id=old_primary_group_id)
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
        person_row = _load_person_row(ctx, conn, person_id=person_id)
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
