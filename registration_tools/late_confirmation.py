#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import pathlib as _pathlib
import pprint
import sys
import typing as _typing

import jinja2
import pandas as _pandas
import psycopg as _psycopg
import wsjrdp2027


_SELFDIR = _pathlib.Path(__file__).parent

_LOGGER = logging.getLogger(__name__)


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


def _load_person_row(
    ctx: wsjrdp2027.WsjRdpContext, conn: _psycopg.Connection, person_id: int
) -> _pandas.Series:
    row = wsjrdp2027.load_person_row(conn, person_id=person_id)
    if row["status"] not in ("reviewed", "confirmed"):
        _LOGGER.error(
            f"Person {row['id_and_name']} has status {row['status']!r}, expected 'reviewed' or 'confirmed'"
        )
        if ctx.is_production:
            raise SystemExit(1)
        else:
            _LOGGER.error("NOT IN PROD => continue")
    return row


def _confirmation_note(
    ctx: wsjrdp2027.WsjRdpContext,
    *,
    batch_config: wsjrdp2027.BatchConfig,
    old_group: wsjrdp2027.Group | None,
    new_group: wsjrdp2027.Group,
    old_status: str | None,
    new_status: str,
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
    confirmed_str = (
        "bestätigt" if (new_status != old_status and new_status == "confirmed") else ""
    )
    changes_str = " und ".join(filter(None, [confirmed_str, move_str]))
    email_str = ("ohne" if batch_config.skip_email else "mit") + " E-Mail-Versand"

    if changes_str:
        return f"Am {date_str} {email_str} {changes_str}"
    elif not batch_config.skip_email:
        return f"Bestätigungs-E-Mail am {date_str} verschickt"
    else:
        return ""


def _confirm_person(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    person_row: _pandas.Series,
    wsj_role: str,
    batch_name: str,
    new_status: str = "confirmed",
) -> None:
    is_yp_or_ul = wsj_role in ["YP", "UL"]
    allow_reconfirmation = bool(ctx.parsed_args.allow_reconfirmation)
    skip_status_update = bool(ctx.parsed_args.skip_status_update)
    person_id: int = int(person_row["id"])
    old_status = person_row["status"]
    old_primary_group_id = wsjrdp2027.to_int_or_none(person_row.get("primary_group_id"))
    old_group = (
        wsjrdp2027.Group.db_load(conn, group_arg=old_primary_group_id)
        if old_primary_group_id is not None
        else None
    )
    person_additional_info = person_row.get("additional_info", {}) or {}
    late_confirmation_issue = person_additional_info.get(
        "late_confirmation_issue", None
    )

    old_tag_list = person_row.get("tag_list") or []
    role_id_name = f"{wsj_role} {person_row['id_and_name']}"
    group_arg: str | int | None = ctx.parsed_args.group
    if group_arg is None:
        if is_yp_or_ul or old_primary_group_id is None:
            _LOGGER.error(f"Missing --group for confirmation of {role_id_name}")
            raise SystemExit(1)
        else:
            new_group = wsjrdp2027.Group.db_load_for_group_id(
                conn, old_primary_group_id
            )
    else:
        new_group = wsjrdp2027.Group.db_load(
            conn, group_arg, auto_group_id=old_primary_group_id
        )
    if is_yp_or_ul and not new_group:
        _LOGGER.error(f"Missing --group for confirmation of {role_id_name}")
        raise SystemExit(1)
    unit_code: str | None = new_group.unit_code if new_group else None
    support_cmt_mail_addresses = new_group.additional_info.get(
        "support_cmt_mail_addresses"
    )
    if skip_status_update:
        _LOGGER.warning(f"Skipping status update (--skip-status-update given)")
        new_status = old_status
        confirmation_tag = None
    else:
        if new_status == "confirmed":
            confirmation_tag = f"{wsj_role}-Confirmation-Mail"
        else:
            confirmation_tag = None

        if new_status == old_status:
            err_msg = f"Already status = {new_status!r} for {role_id_name}"
            if allow_reconfirmation:
                _LOGGER.warning(err_msg)
                _LOGGER.warning(f"  continue due to --allow-reconfirmation")
            else:
                print(flush=True)
                _LOGGER.error(err_msg)
                raise SystemExit(1)

    if confirmation_tag in old_tag_list:
        err_msg = f"Tag {confirmation_tag} already set for {role_id_name}"
        if allow_reconfirmation:
            confirmation_tag = None
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
    if support_cmt_mail_addresses:
        _LOGGER.info(
            f"Add extra_email_bcc (from additional_info['support_cmt_mail_addresses'] of group): {support_cmt_mail_addresses}",
        )
        batch_config.extend_extra_email_bcc(support_cmt_mail_addresses)

    # Handle late_confirmation_issue
    if late_confirmation_issue:
        batch_config.email_subject += f" {late_confirmation_issue}"
        match wsj_role:
            case "YP" | "UL":
                batch_config.extend_extra_email_bcc(
                    "unit-management@worldscoutjamboree.de"
                )
            case "IST":
                batch_config.extend_extra_email_bcc("ist@worldscoutjamboree.de")
            case _:
                batch_config.extend_extra_email_bcc("info@worldscoutjamboree.de")

    batch_config.query.where = wsjrdp2027.PeopleWhere(id=person_id)
    if ctx.parsed_args.collection_date:
        batch_config.query = batch_config.query.replace(
            collection_date=ctx.parsed_args.collection_date,
            include_sepa_mail_in_mailing_to=True,
        )

    if new_status != old_status:
        batch_config.updates["new_status"] = new_status
    if "Warteliste" in old_tag_list:
        batch_config.updates.setdefault("remove_tags", []).append("Warteliste")
    if confirmation_tag and confirmation_tag not in old_tag_list:
        batch_config.updates["add_tags"] = confirmation_tag
    if new_group.id != old_primary_group_id:
        _LOGGER.info(
            f"Set new_primary_group_id={new_group.id} (derived from --group={group_arg})"
        )
        batch_config.updates["new_primary_group_id"] = new_group.id
    if note := _confirmation_note(
        ctx,
        batch_config=batch_config,
        old_group=old_group,
        new_group=new_group,
        old_status=person_row["status"],
        new_status=new_status,
    ):
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
