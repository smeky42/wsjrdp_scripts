#!/usr/bin/env -S uv run
from __future__ import annotations

import datetime as _datetime
import logging
import pathlib as _pathlib
import textwrap

import pandas as _pandas
import psycopg as _psycopg
import wsjrdp2027


_SELF_NAME = _pathlib.Path(__file__).stem
_SELFDIR = _pathlib.Path(__file__).parent

_LOGGER = logging.getLogger(__name__)


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--skip-db-updates", action="store_true", default=None)
    p.add_argument("--skip-email", action="store_true", default=None)
    p.add_argument("id", type=int)
    return p


def _find_retoure_row(
    conn: _psycopg.Connection,
    *,
    person_id: int,
    retoure_cents: int,
    estimated_collection_date: _datetime.date,
) -> _pandas.Series | None:
    start_date = estimated_collection_date.replace(day=1)
    end_date = (estimated_collection_date + _datetime.timedelta(days=31)).replace(
        day=1
    ) - _datetime.timedelta(days=1)
    df = wsjrdp2027.pg_select_dataframe(
        conn,
        t"""SELECT * FROM accounting_entries
WHERE subject_id = {person_id}
  AND value_date >= {start_date} AND value_date <= {end_date}
  AND amount_cents = {-retoure_cents}
""",
    )
    if len(df) == 1:
        return df.iloc[0]
    else:
        return None


def _find_tx_row(conn: _psycopg.Connection, *, id: int | None) -> _pandas.Series | None:
    if id is None:
        return None
    df = wsjrdp2027.pg_select_dataframe(
        conn, t"SELECT * FROM wsjrdp_camt_transactions WHERE id = {id}"
    )
    if len(df) == 1:
        return df.iloc[0]
    else:
        return None


def _create_fin_issue(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    person_row: _pandas.Series,
    estimated_collection_date: _datetime.date,
    upcoming_collection_date: _datetime.date,
    batch_config: wsjrdp2027.BatchConfig,
) -> str:
    from wsjrdp2027 import _pg

    person_id = person_row["id"]

    collection_year_month = estimated_collection_date.strftime("%Y-%m")
    summary = f"Einzug {collection_year_month} Retoure {person_row['role_id_name']}"
    description = f"""{person_row["greeting_name"]} in Hitobito: [https://anmeldung.worldscoutjamboree.de/people/{person_id}]
"""
    labels = ["Retoure", "Rücklastschrift", f"Einzug-{collection_year_month}"]

    with ctx.login_helpdesk() as helpdesk:
        customer_request = helpdesk.create_fin_customer_request(
            summary=summary,
            description=description,
            labels=labels,
        )

        missing_installment_issue = customer_request["issueKey"]
        _LOGGER.info(f"Created customer request {missing_installment_issue}")

    _pg._execute_query_fetch_id(
        conn,
        t"UPDATE people SET additional_info['missing_installment_issue'] = to_jsonb({missing_installment_issue}::text) WHERE id = {person_id} RETURNING id;",
    )
    return missing_installment_issue


def _send_missing_installment_notification(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    person_row: _pandas.Series,
    wsj_role: str,
    batch_name: str,
) -> None:
    from wsjrdp2027 import _pg

    person_id: int = int(person_row["id"])
    primary_group_id = int(person_row["primary_group_id"])
    group = wsjrdp2027.Group.db_load(conn, group_arg=primary_group_id)
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
            "response_due_date": ctx.today + _datetime.timedelta(days=7),
        },
    )
    assert batch_config.jinja_extra_globals is not None
    upcoming_collection_date = batch_config.query.collection_date
    assert upcoming_collection_date is not None
    person_row = wsjrdp2027.load_person_row(
        conn, person_id=person_id, collection_date=upcoming_collection_date
    )

    estimated_collection_date = (
        upcoming_collection_date - _datetime.timedelta(days=30)
    ).replace(day=5)

    prev_month_person_row = wsjrdp2027.load_person_row(
        conn,
        person_id=person_id,
        collection_date=estimated_collection_date,
    )
    retoure_cents = prev_month_person_row["open_amount_cents"]
    missing_installment_cents = prev_month_person_row[
        "amount_due_in_collection_date_month_cents"
    ]
    bank_fees_cents = retoure_cents - missing_installment_cents

    batch_config.jinja_extra_globals.update(
        {
            "missing_installment_year_month": estimated_collection_date.strftime(
                "%Y-%m"
            ),
            "retoure_cents": retoure_cents,
            "missing_installment_cents": missing_installment_cents,
            "bank_fees_cents": bank_fees_cents,
            "upcoming_collection_date": upcoming_collection_date,
            "estimated_collection_date": estimated_collection_date,
        }
    )

    ae_row = _find_retoure_row(
        conn,
        person_id=person_id,
        retoure_cents=retoure_cents,
        estimated_collection_date=estimated_collection_date,
    )
    tx_row = _find_tx_row(
        conn, id=(ae_row["camt_transaction_id"] if ae_row is not None else None)
    )

    print(flush=True)
    if ae_row is not None:
        _LOGGER.info(
            "retoure accounting entry:\n%s", textwrap.indent(ae_row.to_string(), "  | ")
        )
    if tx_row is not None:
        _LOGGER.info(
            "retoure camt tx:\n%s", textwrap.indent(tx_row.to_string(), "  | ")
        )
    _LOGGER.info(f"retoure_cents: {retoure_cents} (prev month open_amount_cents)")
    _LOGGER.info(f"missing_installment_cents: {missing_installment_cents} (prev month)")
    _LOGGER.info(f"missing_installment_issue: {missing_installment_issue}")
    print(flush=True)

    ctx.require_approval_to_run_in_prod()

    # support_cmt_mail_addresses = group.additional_info.get("support_cmt_mail_addresses")
    # batch_config.extend_extra_email_bcc(support_cmt_mail_addresses)

    if missing_installment_issue in (None, "", "FIN-000"):
        missing_installment_issue = _create_fin_issue(
            ctx=ctx,
            conn=conn,
            batch_config=batch_config,
            estimated_collection_date=estimated_collection_date,
            upcoming_collection_date=upcoming_collection_date,
            person_row=person_row,
        )
        batch_config.jinja_extra_globals["missing_installment_issue"] = (
            missing_installment_issue
        )

    if ae_row is not None and not ae_row["comment"]:
        _pg._execute_query_fetch_id(
            conn,
            t"""UPDATE accounting_entries SET comment = {missing_installment_issue} WHERE id = {ae_row["id"]} RETURNING id;""",
        )
        _LOGGER.info(
            f"Updated comment of accounting_entry {ae_row['id']} to {missing_installment_issue!r}"
        )

    if tx_row is not None and not tx_row["comment"]:
        _pg._execute_query_fetch_id(
            conn,
            t"""UPDATE wsjrdp_camt_transactions SET comment = {missing_installment_issue} WHERE id = {tx_row["id"]} RETURNING id;""",
        )
        _LOGGER.info(
            f"Updated comment of camt tx {tx_row['id']} to {missing_installment_issue!r}"
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
    __import__("sys").exit(main())
