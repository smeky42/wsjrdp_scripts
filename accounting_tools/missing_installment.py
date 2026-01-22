#!/usr/bin/env -S uv run
from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
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
_SELFDIR = _pathlib.Path(__file__).parent

_LOGGER = logging.getLogger(__name__)


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


def _load_person_row(
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    person_id: int,
    collection_date=None,
) -> _pandas.Series:
    df = wsjrdp2027.load_people_dataframe(
        conn,
        query=wsjrdp2027.PeopleQuery(
            where=wsjrdp2027.PeopleWhere(id=person_id), collection_date=collection_date
        ),
        log_resulting_data_frame=False,
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


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--skip-db-updates", action="store_true", default=None)
    p.add_argument("--skip-email", action="store_true", default=None)
    # p.add_argument("--issue", required=True)
    p.add_argument("id", type=int)
    return p


def _send_missing_installment_notification(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    person_row: _pandas.Series,
    wsj_role: str,
    batch_name: str,
) -> None:
    person_id: int = int(person_row["id"])
    primary_group_id = int(person_row["primary_group_id"])
    group = _select_group(conn, group_arg=primary_group_id)
    role_id_name = f"{wsj_role} {person_row['id_and_name']}"

    # create new batch config
    missing_installment_issue = person_row.get("additional_info", {}).get(
        "missing_installment_issue", "FIN-000"
    )
    batch_config = ctx.load_batch_config_from_yaml(
        _SELFDIR / f"{_SELF_NAME}.yml",
        name=batch_name,
        where=wsjrdp2027.PeopleWhere(id=person_id),
        jinja_extra_globals={
            "role_id_name": role_id_name,
            "missing_installment_issue": missing_installment_issue,
        },
    )
    assert batch_config.jinja_extra_globals is not None
    collection_date = batch_config.query.collection_date
    assert collection_date is not None
    person_row = _load_person_row(
        ctx, conn, person_id=person_id, collection_date=collection_date
    )
    prev_month_person_row = _load_person_row(
        ctx,
        conn,
        person_id=person_id,
        collection_date=collection_date - _datetime.timedelta(days=30),
    )
    retoure_cents = prev_month_person_row["open_amount_cents"]
    missing_installment_cents = prev_month_person_row[
        "amount_due_in_collection_date_month_cents"
    ]
    bank_fees_cents = retoure_cents - missing_installment_cents
    batch_config.jinja_extra_globals.update(
        {
            "missing_installment_year_month": prev_month_person_row[
                "collection_date"
            ].strftime("%Y-%m"),
            "retoure_cents": retoure_cents,
            "missing_installment_cents": missing_installment_cents,
            "bank_fees_cents": bank_fees_cents,
        }
    )

    support_cmt_mail_addresses = group.additional_info.get("support_cmt_mail_addresses")
    batch_config.extend_extra_email_bcc(support_cmt_mail_addresses)

    if missing_installment_issue == "FIN-000":
        from wsjrdp2027 import _pg

        prep = ctx.load_people_and_prepare_batch(
            batch_config, log_resulting_data_frame=False
        )
        assert prep.messages
        msg = prep.messages[0].message
        assert msg
        _LOGGER.info("Subject:\n\n%s\n\n", msg["Subject"])

        actual_issue = input("Actual FIN issue: ")
        missing_installment_issue = actual_issue
        batch_config.jinja_extra_globals["missing_installment_issue"] = actual_issue
        _pg._execute_query_fetch_id(
            conn,
            t"UPDATE people SET additional_info['missing_installment_issue'] = to_jsonb({missing_installment_issue}::text) WHERE id = {person_id} RETURNING id;",
        )

    prepared_batch = ctx.load_people_and_prepare_batch(
        batch_config, log_resulting_data_frame=False
    )
    ctx.update_db_and_send_mailing(prepared_batch, zip_eml=False)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        __file__=__file__,
    )

    person_id = int(ctx.parsed_args.id)

    print(flush=True)
    _LOGGER.info(f"Send missing installment notification to {person_id}")
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

        _send_missing_installment_notification(
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
