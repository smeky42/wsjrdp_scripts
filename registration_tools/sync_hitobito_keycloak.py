#!/usr/bin/env -S uv run
from __future__ import annotations

import argparse as _argparse
import dataclasses as _dataclasses
import logging as _logging
import pathlib as _pathlib

import pandas as _pandas
import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)

_SELF_NAME = _pathlib.Path(__file__).stem


GROUPNAMES_TO_SYNC = ["CMT", "UL", "IST", "BMT", "EXT"]
GROUPNAMES_TO_SYNC = []


KEYCLOAK_GROUPNAMES_TO_FETCH = ["CMT", "UL", "IST", "BMT", "EXT"]


@_dataclasses.dataclass(kw_only=True)
class SyncOptions:
    groups: list[str] = _dataclasses.field(default_factory=lambda: [])
    limit: int | None = None
    status: list[str] = _dataclasses.field(default_factory=lambda: [])
    create_missing_keycloak_user: bool = True


def _parse_args(args: _argparse.Namespace) -> SyncOptions:
    kwargs = {}
    if args.groups:
        groups = []
        for g in args.groups:
            if g == "all":
                groups.extend(KEYCLOAK_GROUPNAMES_TO_FETCH)
            elif g in KEYCLOAK_GROUPNAMES_TO_FETCH:
                groups.append(g)
            else:
                raise RuntimeError(f"Invalid groupname {g}")
        kwargs["groups"] = wsjrdp2027.dedup(groups)
    if args.limit:
        kwargs["limit"] = int(args.limit)
    if args.status:
        kwargs["status"] = args.status
    else:
        kwargs["status"] = ["confirmed"]
    if args.create_missing_keycloak_user is not None:
        kwargs["create_missing_keycloak_user"] = bool(args.create_missing_keycloak_user)
    return SyncOptions(**kwargs)


def _create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--skip-email", action="store_true", default=None)
    p.add_argument("--skip-db-updates", action="store_true", default=None)
    p.add_argument("--keycloak-dry-run", action="store_true", default=None)
    p.add_argument("--mailcow-dry-run", action="store_true", default=None)
    p.add_argument(
        "--open-editor",
        action="store_true",
        default=False,
        help="Open E-Mail body content in editor before preparing EML file",
    )
    p.add_argument(
        "--group",
        "-g",
        dest="groups",
        required=False,
        action="append",
        default=None,
        help="""Name or id of the group the deregistered person should be moved to.""",
        choices=KEYCLOAK_GROUPNAMES_TO_FETCH + ["all"],
    )
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--status", action="append", default=None)
    p.add_argument("--create-missing-keycloak-user", action="store_true", default=None)
    p.add_argument(
        "--no-create-missing-keycloak-user",
        dest="create_missing_keycloak_user",
        action="store_false",
    )
    return p


def load_people_dataframe_for_query(
    ctx: wsjrdp2027.WsjRdpContext, *, query: wsjrdp2027.PeopleQuery
) -> _pandas.DataFrame:

    print(flush=True)
    _LOGGER.info(f"{query.where=}")
    if query.where:
        _LOGGER.info(f"WHERE: {query.where.as_where_condition()}")
    _LOGGER.info(f"{query=}")
    print(flush=True)

    df = wsjrdp2027.load_people_dataframe(
        conn=ctx.hitobito_psycopg_connection(read_only=True),
        query=query,
        log_resulting_data_frame=False,
    )
    _LOGGER.info(f"Found {len(df)} contingent members in Hitobito for given query")
    return df


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=_create_argument_parser(),
        argv=argv,
        __file__=__file__,
        # dry_run=True,
        # log_level=_logging.DEBUG,
    )
    with ctx:
        args = _parse_args(ctx.parsed_args)

        out_base = ctx.make_out_path(_SELF_NAME + "__{{ filename_suffix }}")
        log_filename = out_base.with_suffix(".log")
        ctx.configure_log_file(log_filename)

        _LOGGER.info("Load mailcow aliases to fill cache")
        _aliases = ctx.mailcow().get_alias_list()
        _LOGGER.info(f"  ... loaded {len(_aliases)} aliases from mailcow")

        _LOGGER.info("Load keycloak users to fill cache")
        _users = ctx.keycloak().get_user_list(allow_cached=False)
        _LOGGER.info(f"  ... loaded {len(_users)} users from keycloak")

        for groupname in args.groups:
            where = wsjrdp2027.PeopleWhere.from_keycloak_group(
                groupname=groupname, status=args.status
            )
            query = wsjrdp2027.PeopleQuery(where=where, limit=args.limit)
            people = load_people_dataframe_for_query(ctx, query=query)
            if not ctx.sync_hitobito_keycloak_mailcow(
                people=people,
                keycloak_groupname=groupname,
                create_missing_keycloak_user=args.create_missing_keycloak_user,
                self_name=_SELF_NAME,
            ):
                return 1


if __name__ == "__main__":
    __import__("sys").exit(main())
