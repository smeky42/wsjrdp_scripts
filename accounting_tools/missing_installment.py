#!/usr/bin/env -S uv run
from __future__ import annotations

import datetime as _datetime
import logging
import pathlib as _pathlib
import pprint
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
    p.add_argument("--skip-create-fin-issue", action="store_true", default=False)
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


def _truish_list(*args):
    return [x for x in args if x]


def _update_ae(
    ctx: wsjrdp2027.WsjRdpContext,
    ae_row: _pandas.Series | None,
    person: wsjrdp2027.Person,
) -> None:
    from wsjrdp2027 import _pg

    if ae_row is None:
        return

    if skip_reasons := _truish_list(
        f"existing comment {ae_row['comment']!r}" if ae_row["comment"] else None,
        "skip_db_updates set" if ctx.skip_db_updates else None,
        "dry_run set" if ctx.dry_run else None,
        "no debit_return_issue" if not person.debit_return_issue else None,
    ):
        _LOGGER.info(
            f"Skip updating accounting_entry {ae_row['id']}: {','.join(skip_reasons)}"
        )
    else:
        _pg._execute_query_fetch_id(
            ctx.hitobito_psycopg_connection(read_only=False),
            t"""UPDATE accounting_entries SET comment = {person.debit_return_issue} WHERE id = {ae_row["id"]} RETURNING id;""",
        )
        _LOGGER.info(
            f"Updated comment of accounting_entry {ae_row['id']} to {person.debit_return_issue!r}"
        )


def _update_tx(
    ctx: wsjrdp2027.WsjRdpContext,
    tx_row: _pandas.Series | None,
    person: wsjrdp2027.Person,
) -> None:
    from wsjrdp2027 import _pg

    if tx_row is None:
        return

    if skip_reasons := _truish_list(
        f"existing camt tx comment {tx_row['comment']!r}"
        if tx_row["comment"]
        else None,
        "skip_db_updates set" if ctx.skip_db_updates else None,
        "dry_run set" if ctx.dry_run else None,
        "no debit_return_issue" if not person.debit_return_issue else None,
    ):
        _LOGGER.info(f"Skip updating camt tx {tx_row['id']}: {','.join(skip_reasons)}")
    else:
        _pg._execute_query_fetch_id(
            ctx.hitobito_psycopg_connection(read_only=False),
            t"""UPDATE wsjrdp_camt_transactions SET comment = {person.debit_return_issue} WHERE id = {tx_row["id"]} RETURNING id;""",
        )
        _LOGGER.info(
            f"Updated comment of camt tx {tx_row['id']} to {person.debit_return_issue!r}"
        )


def _create_fin_issue(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    person: wsjrdp2027.Person,
    estimated_collection_date: _datetime.date,
    upcoming_collection_date: _datetime.date,
    batch_config: wsjrdp2027.BatchConfig,
) -> str:
    from wsjrdp2027 import _pg

    collection_year_month = estimated_collection_date.strftime("%Y-%m")
    summary = f"Einzug {collection_year_month} Retoure {person.role_id_name}"
    email_addrs = wsjrdp2027.merge_mail_addresses(
        person.get("sepa_mailing_to"), person.get("sepa_mailing_cc"), default=[]
    )
    email_addrs_list_text = "\n".join(
        f"* [{addr}|mailto:{addr}]" for addr in email_addrs
    )
    description = f"""{person.greeting_name} in Hitobito: [https://anmeldung.worldscoutjamboree.de/people/{person.id}]

E-Mail Adressen:
{email_addrs_list_text}
""".strip()

    labels = ["Retoure", "Rücklastschrift", f"Einzug-{collection_year_month}"]

    reporter = person.get("sepa_mail") or person.email
    participants = [addr for addr in email_addrs if addr != reporter]

    with ctx.login_helpdesk() as helpdesk:
        for email in email_addrs:
            helpdesk.create_customer(email, email)

        customer_request = helpdesk.create_fin_customer_request(
            summary=summary,
            description=description,
            labels=labels,
            raise_on_behalf_of=reporter,
            request_participants=participants,
        )

        debit_return_issue = customer_request["issueKey"]

        _LOGGER.info(
            f"Created customer request {debit_return_issue}\n"
            f"{textwrap.indent(pprint.pformat(helpdesk.get_customer_request(debit_return_issue)), '  | ')}"
        )

    _pg._execute_query_fetch_id(
        ctx.hitobito_psycopg_connection(read_only=False),
        t"UPDATE people SET additional_info['debit_return_issue'] = to_jsonb({debit_return_issue}::text) WHERE id = {person.id} RETURNING id;",
    )
    return debit_return_issue


def _send_missing_installment_notification(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    person: wsjrdp2027.Person,
    batch_name: str,
    skip_create_fin_issue: bool = False,
) -> None:

    batch_config = ctx.load_batch_config_from_yaml(
        _SELFDIR / f"{_SELF_NAME}.yml",
        name=batch_name,
        where=wsjrdp2027.PeopleWhere(id=person.id),
        jinja_extra_globals={
            "response_due_date": ctx.today + _datetime.timedelta(days=7),
        },
    )
    assert batch_config.jinja_extra_globals is not None
    upcoming_collection_date = batch_config.query.collection_date
    assert upcoming_collection_date is not None
    person = ctx.load_person_for_id(person.id, collection_date=upcoming_collection_date)

    estimated_collection_date = (
        upcoming_collection_date - _datetime.timedelta(days=30)
    ).replace(day=5)

    prev_month_person = ctx.load_person_for_id(
        person.id, collection_date=estimated_collection_date
    )
    retoure_cents = prev_month_person.open_amount_cents
    missing_installment_cents = prev_month_person[
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
        ctx.hitobito_psycopg_connection(read_only=True),
        person_id=person.id,
        retoure_cents=retoure_cents,
        estimated_collection_date=estimated_collection_date,
    )
    tx_row = _find_tx_row(
        ctx.hitobito_psycopg_connection(read_only=True),
        id=(ae_row["camt_transaction_id"] if ae_row is not None else None),
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
    _LOGGER.info("")
    _LOGGER.info(f"role_id_name: {person.role_id_name}")
    _LOGGER.info(f"retoure_cents: {retoure_cents} (prev month open_amount_cents)")
    _LOGGER.info(f"missing_installment_cents: {missing_installment_cents} (prev month)")
    _LOGGER.info(f"debit_return_issue: {person.debit_return_issue}")
    print(flush=True)

    ctx.require_approval_to_run_in_prod()

    # support_cmt_mail_addresses = group.additional_info.get("support_cmt_mail_addresses")
    # batch_config.extend_extra_email_bcc(support_cmt_mail_addresses)

    if person.debit_return_issue in (None, "", "FIN-000"):
        if not skip_create_fin_issue:
            person.debit_return_issue = _create_fin_issue(
                ctx=ctx,
                batch_config=batch_config,
                estimated_collection_date=estimated_collection_date,
                upcoming_collection_date=upcoming_collection_date,
                person=person,
            )
            prev_month_person.debit_return_issue = person.debit_return_issue

    _update_ae(ctx, ae_row, person)
    _update_tx(ctx, tx_row, person)

    prepared = batch_config.prepare(
        person, dry_run=ctx.dry_run, skip_email=ctx.skip_email
    )
    ctx.send_mailing(prepared, zip_eml=False)


def main(argv=None):
    with wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    ) as ctx:
        person_id = int(ctx.parsed_args.id)
        print(flush=True)
        _LOGGER.info(f"Send missing installment notification to {person_id}")

        person = ctx.load_person_for_id(person_id)
        out_base = ctx.make_out_path(
            f"{_SELF_NAME}_{person.role_id_name_for_filename}"
            + "_{{ filename_suffix }}"
        )
        ctx.configure_log_file(out_base.with_suffix(".log"))

        _send_missing_installment_notification(
            ctx=ctx,
            person=person,
            batch_name=out_base.name,
            skip_create_fin_issue=ctx.dry_run or ctx.parsed_args.skip_create_fin_issue,
        )


if __name__ == "__main__":
    __import__("sys").exit(main())
