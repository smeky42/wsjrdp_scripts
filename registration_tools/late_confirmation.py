#!/usr/bin/env -S uv run
from __future__ import annotations

import dataclasses as _dataclasses
import logging
import pathlib as _pathlib
import pprint
import sys
import typing as _typing

import jinja2
import pandas as _pandas
import psycopg as _psycopg
import psycopg.sql as _psycopg_sql
import wsjrdp2027


_SELFDIR = _pathlib.Path(__file__).parent

_LOGGER = logging.getLogger(__name__)

if _typing.TYPE_CHECKING:
    import string.templatelib as _string_templatelib

    import psycopg.sql as _psycopg_sql


def create_argument_parser():
    import argparse

    from wsjrdp2027 import to_date_or_none

    p = argparse.ArgumentParser()
    p.add_argument("--skip-email", action="store_true", default=None)
    p.add_argument("--skip-db-updates", action="store_true", default=None)
    p.add_argument(
        "--payment-info",
        dest="show_payment_info",
        action="store_true",
        default=True,
        help="""Show payment information
        (amount already paid, next installment).
        On by default.""",
    )
    p.add_argument(
        "--no-payment-info",
        dest="show_payment_info",
        action="store_false",
        help="""Do not show payment information
        (amount already paid, next installment).""",
    )
    p.add_argument(
        "--collection-date",
        type=to_date_or_none,
        help="""Collection date of the next SEPA direct debit.
        Required unless --no-payment-info is given.""",
    )
    p.add_argument(
        "--group",
        "-g",
        required=True,
        help="""Name or id of the group the confirmed person should be moved to.""",
    )
    p.add_argument("--bcc", action="append", help="Additional mail addresses to Bcc")
    p.add_argument(
        "--allow-reconfirmation",
        action="store_true",
        default=False,
        help="""Allow a reconfirmation.""",
    )
    p.add_argument(
        "--skip-status-update",
        dest="skip_status_update",
        default=False,
        action="store_true",
    )
    p.add_argument("id", type=int)
    return p


_FULLY_PAID = """
Herzlichen Dank, dass du den Teilnahmebetrag in Höhe von {{ row.total_fee_cents | format_cents_as_eur_de }} bereits vollständig bezahlt hast. Das ist keine Kleinigkeit und wir wollen uns dafür bei dir bedanken! Ohne deine Unterstützung wäre das Jamboree für alle noch teurer geworden.
"""

_SEPA_STATUS_NOT_OK = """
Wir prüfen gerade deine Beitragszahlung und führen solange keinen SEPA Lastschrifteinzug durch.
Schreibe uns bitte eine Antwort auf diese E-Mail, wenn du davon noch nichts weißt.
"""

_NEXT_INSTALLMENT = """
Wir werden {% if row.early_payer or (row.installments_cents_dict | length) == 1 %}deinen Teilnahmebetrag{% else %}die {% if row.amount_paid_cents == 0 %}erste{% else %}nächste{% endif %} Rate deines Teilnahmebetrags{% endif %} voraussichtlich am {{ row.collection_date | date_de }} per SEPA Lastschrift einziehen. Du nimmst mit folgenden Daten am Einzug teil:

Teilnehmer*in: {{ row.full_name }}
Betrag: {{ row.open_amount_cents | format_cents_as_eur_de }}
Kontoinhaber*in: {{ row.sepa_name }}
IBAN: {{ row.sepa_iban | format_iban }}
{% if (row.sepa_bank_name is defined) and (row.sepa_bank_name) %}Bank: {{ row.sepa_bank_name }}
{% endif %}
Mandatsreferenz: {{ row.sepa_mandate_id }}
Verwendungszweck: {{ row.sepa_dd_description }}
"""

_ANNOUNCE_UPCOMING_INSTALLMENT = """
{% set next_installment_year_month_de = (row.installments_cents_dict.keys() | sort | first | month_year_de) %}
{% if row.early_payer or (row.installments_cents_dict | length) == 1 %}
Deinen Teilnahmebetrag in Höhe von {{ row.total_fee_cents | format_cents_as_eur_de }} ziehen wir im {{ next_installment_year_month_de }} per SEPA Lastschrift ein. Wir werden dir ein paar Tage vor dem Einzug eine E-Mail mit weiteren Details schicken.
{% else %}
Die erste Rate deines Teilnahmebetrags ziehen wir im {{ next_installment_year_month_de }} per SEPA Lastschrift ein. Wir werden dir ein paar Tage vor dem Einzug eine E-Mail mit weiteren Details schicken.
{% endif %}
"""


@jinja2.pass_environment
def render_upcoming_payment_text(env: jinja2.Environment) -> str:
    show_payment_info = _typing.cast(bool, env.globals.get("show_payment_info", True))
    if not show_payment_info:
        return ""
    row = _typing.cast(dict, env.globals.get("row", {}))
    if not row.get("collection_date"):
        raise RuntimeError("collection_date fehlt")
    if row.get("amount_unpaid_cents") == 0:
        source = _FULLY_PAID
    elif row.get("sepa_status") not in ("ok", "missing"):
        source = _SEPA_STATUS_NOT_OK
    elif (row.get("open_amount_cents") or 0) > 0:
        source = _NEXT_INSTALLMENT
    elif row.get("installments_cents_dict"):
        source = _ANNOUNCE_UPCOMING_INSTALLMENT
    else:
        raise RuntimeError(
            "Unsupported financial configuration -- cannot confirm and send email"
        )
    source = f"\n{source.strip()}\n\n\n"
    return env.from_string(source).render()


@jinja2.pass_environment
def render_confirmation_info(env: jinja2.Environment) -> str:
    _TEMPLATE = """
Name: {{ row.full_name }}
Anmeldungs-ID: {{ row.id }}
Rolle: {{ row.payment_role.full_role_name }}
Teilnahmebetrag: {{ row.total_fee_cents | format_cents_as_eur_de }}
{% if show_payment_info and (row.amount_paid_cents > 0) %}Davon bereits bezahlt: {{ row.amount_paid_cents | format_cents_as_eur_de }}
{% endif %}
"""
    return env.from_string(_TEMPLATE).render().strip()


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
    return config


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


def _select_group(conn, group_arg: str | int, *, auto_group_id: int | None) -> Group:
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


def _confirm_person(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    person_row: _pandas.Series,
    wsj_role: str,
    batch_name: str,
) -> None:
    import re

    is_yp_or_ul = wsj_role in ["YP", "UL"]
    allow_reconfirmation = bool(ctx.parsed_args.allow_reconfirmation)
    confirmation_tag = f"{wsj_role}-Confirmation-Mail"
    person_id: int = int(person_row["id"])
    old_primary_group_id = wsjrdp2027.to_int_or_none(person_row.get("primary_group_id"))
    role_id_name = f"{wsj_role} {person_row['id_and_name']}"
    group_arg: str | int | None = ctx.parsed_args.group
    if group_arg is None:
        if is_yp_or_ul or old_primary_group_id is None:
            _LOGGER.error(f"Missing --group for confirmation of {role_id_name}")
            raise SystemExit(1)
        else:
            target_group = _select_group_for_group_id(conn, old_primary_group_id)
    else:
        target_group = _select_group(
            conn, group_arg, auto_group_id=old_primary_group_id
        )
    if is_yp_or_ul and not target_group:
        _LOGGER.error(f"Missing --group for confirmation of {role_id_name}")
        raise SystemExit(1)
    unit_code: str | None = target_group.unit_code if target_group else None
    confirmation_email_bcc = target_group.additional_info.get("confirmation_email_bcc")
    if person_row["status"] == "confirmed":
        err_msg = f"Already status = 'confirmed' for {role_id_name}"
        if allow_reconfirmation:
            _LOGGER.warning(err_msg)
            _LOGGER.warning(f"  continue due to --allow-reconfirmation")
        else:
            print(flush=True)
            _LOGGER.error(err_msg)
            raise SystemExit(1)
    if confirmation_tag in (person_row.get("tag_list") or []):
        err_msg = f"Tag {confirmation_tag} already set for {role_id_name}"
        if allow_reconfirmation:
            _LOGGER.warning(err_msg)
            _LOGGER.warning(f"  continue due to --allow-reconfirmation")
        else:
            print(flush=True)
            _LOGGER.error(err_msg)
            raise SystemExit(1)

    # load batch config
    batch_config = ctx.load_batch_config_from_yaml(
        _SELFDIR / f"late_confirmation_{wsj_role}.yml",
        name=batch_name,
        jinja_extra_globals={
            render_confirmation_info.__name__: render_confirmation_info,
            render_upcoming_payment_text.__name__: render_upcoming_payment_text,
            "show_payment_info": ctx.parsed_args.show_payment_info,
        },
    )

    # Add to extra_email_bcc
    if bcc := ctx.parsed_args.bcc:
        _LOGGER.info(f"Add extra_email_bcc (from --bcc): {bcc}")
        batch_config.extend_extra_email_bcc(bcc)
    if confirmation_email_bcc:
        _LOGGER.info(
            f"Add extra_email_bcc (from additional_info['confirmation_email_bcc'] of group): {confirmation_email_bcc}",
        )
        batch_config.extend_extra_email_bcc(confirmation_email_bcc)

    batch_config.query = batch_config.query.replace(
        where=wsjrdp2027.PeopleWhere(id=person_id),
        collection_date=ctx.parsed_args.collection_date,
        include_sepa_mail_in_mailing_to=True,
    )
    batch_config.updates.update(
        {
            "new_status": "confirmed",
            "add_tags": confirmation_tag,
            "remove_tags": ["Warteliste"],
            "add_note": f"Bestätigungs-E-Mail am {ctx.today.strftime('%d.%m.%Y')} verschickt",
        }
    )
    if ctx.parsed_args.skip_status_update:
        _LOGGER.warning(f"Skipping status update (--skip-status-update given)")
        batch_config.updates.pop("new_status", None)
    if target_group.id != old_primary_group_id:
        _LOGGER.info(
            f"Set new_primary_group_id={target_group.id} (derived from --group={group_arg})"
        )
        batch_config.updates["new_primary_group_id"] = target_group.id
    if is_yp_or_ul and (unit_code := target_group.unit_code):
        _LOGGER.info(
            f"Set new_unit_code={unit_code!r} (derived from --group={group_arg})"
        )
        batch_config.updates["new_unit_code"] = unit_code

    _LOGGER.info("Query:\n%s", batch_config.query)
    _LOGGER.info("Updates:\n%s", pprint.pformat(batch_config.updates))

    prepared_batch = ctx.load_people_and_prepare_batch(batch_config)
    ctx.update_db_and_send_mailing(prepared_batch, zip_eml=False)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    )
    if ctx.parsed_args.show_payment_info:
        if not ctx.parsed_args.collection_date:
            raise RuntimeError(
                "--collection-date is required unless --no-payment-info is given"
            )
    person_id = int(ctx.parsed_args.id)
    print(flush=True)
    _LOGGER.info(f"Confirm person {person_id}")
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
        batch_name = f"late_confirmation_{role_id_name}"
        log_filename = ctx.make_out_path(batch_name).with_suffix(".log")
        ctx.configure_log_file(log_filename)

        _confirm_person(
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
