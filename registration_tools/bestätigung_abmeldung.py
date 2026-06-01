#!/usr/bin/env -S uv run
"""Tool to produce a cancellation request letter."""

from __future__ import annotations

import json
import logging
import pathlib as _pathlib
import sys

import wsjrdp2027


SELFDIR = _pathlib.Path(__file__).parent.resolve()
_SELF_NAME = _pathlib.Path(__file__).stem


_LOGGER = logging.getLogger()


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--skip-email", action="store_true", default=None)
    p.add_argument("--skip-db-updates", action="store_true", default=None)
    p.add_argument(
        "--open-editor",
        action="store_true",
        default=False,
        help="Open E-Mail body content in editor before preparing EML file",
    )
    p.add_argument(
        "--group",
        "-g",
        required=False,
        default=None,
        help="""Name or id of the group the deregistered person should be moved to.""",
    )
    p.add_argument("person_id")
    return p


def attach_cancellation_confirmation(
    ctx: wsjrdp2027.WsjRdpContext, prepared: wsjrdp2027.PreparedEmailMessage
) -> None:
    import textwrap

    assert prepared.message is not None
    assert prepared.person
    person = prepared.person
    row = prepared.row
    assert row is not None

    _LOGGER.debug(
        "id: %s, full_name: %s, row:\n%s",
        row.id,
        row.get("full_name", None),
        textwrap.indent(row.to_string(), "  | "),
    )

    pdf_filename = f"WSJ27 Bestätigung Abmeldung {person.role_id_name}.pdf"
    pdf_path = ctx.make_out_path(f"{prepared.mailing_name}.pdf")

    acc_entries = sorted(
        person.load_accounting_entries(conn=ctx.hitobito_psycopg_connection()),
        key=lambda acc: (acc.value_date, acc.booking_date, acc.id),
        reverse=True,
    )
    accounting_entry_sum_cents = sum(acc.amount_cents for acc in acc_entries)

    cancellation_date = wsjrdp2027.to_date_or_none(
        person.additional_info.get("deregistration_effective_date")
    )
    cancellation_date_de = (
        cancellation_date.strftime("%d.%m.%Y") if cancellation_date else None
    )

    acc_entries_for_sys_inputs = [
        {
            "amount_de": acc.amount_de,
            "description": acc.description,
            "short_dbtr": acc.short_dbtr,
            "value_date": acc.value_date.strftime("%d.%m.%Y"),
        }
        for acc in acc_entries
    ]

    sys_inputs = {
        "hitobitoid": str(person.id),
        "today_de": ctx.today.strftime("%d.%m.%Y"),
        "cancellation_date_de": cancellation_date_de,
        "role_id_name": person.role_id_name,
        "full_name": person.full_name,
        "short_full_name": person.short_full_name,
        "birthday_de": row["birthday_de"],
        "deregistration_issue": (person.deregistration_issue or ""),
        "accounting_entries": json.dumps(acc_entries_for_sys_inputs),
        "accounting_entry_sum_de": wsjrdp2027.format_cents_as_eur_de(
            accounting_entry_sum_cents
        ),
    }
    wsjrdp2027.typst_compile(
        SELFDIR / f"{_pathlib.Path(__file__).stem}.typ",
        output=pdf_path,
        sys_inputs=sys_inputs,
    )
    pdf_bytes = pdf_path.read_bytes()
    prepared.message.add_attachment(
        pdf_bytes, maintype="application", subtype="pdf", filename=pdf_filename
    )
    prepared.eml_name = f"{prepared.mailing_name}.eml"


def _deregistration_note(
    ctx: wsjrdp2027.WsjRdpContext,
    batch_config: wsjrdp2027.BatchConfig,
    person: wsjrdp2027.Person,
    new_status: str = "deregistered",
    new_group: wsjrdp2027.Group | None = None,
) -> str:
    date_str = ctx.today.strftime("%d.%m.%Y")
    confirmed_str = (
        "abgemeldet"
        if (new_status != person.status and new_status == "deregistered")
        else ""
    )
    if new_group is not None and new_group.id != person.primary_group_id:
        old_group = person.primary_group
        old_group_name = (old_group.short_name or old_group.name) if old_group else ""
        new_group_name = new_group.short_name or new_group.name
        move_str = f"von {old_group_name} nach {new_group_name} verschoben"
        pass
    else:
        move_str = ""
    changes_str = " und ".join(filter(None, [confirmed_str, move_str]))
    email_str = ("ohne" if batch_config.skip_email else "mit") + " E-Mail-Versand"
    if changes_str:
        return f"Am {date_str} {email_str} {changes_str}"
    elif not batch_config.skip_email:
        return f"Abmelde-Bestätigungs-E-Mail am {date_str} verschickt"
    else:
        return ""


def main(argv=None):
    with wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    ) as ctx:
        person_id = int(ctx.parsed_args.person_id)
        new_group_id: str | int | None = ctx.parsed_args.group

        # conn = ctx.hitobito_psycopg_connection()
        person = ctx.load_person_for_id(person_id)
        ctx.out_dir = ctx.out_dir / person.id_and_name
        try:
            primary_group = person.primary_group
        except Exception as exc:
            _LOGGER.info("Could not load associated group: %s", str(exc))
            primary_group = None

        batch_config = wsjrdp2027.BatchConfig.from_yaml(
            SELFDIR / f"{_SELF_NAME}_{person.short_role_name}.yml",
            name=f"WSJ27 Bestätigung Abmeldung {person.role_id_name} {ctx.filename_suffix}",
            where=wsjrdp2027.PeopleWhere(
                id=person_id,
                exclude_deregistered=False,
            ),
        ).replace(
            dry_run=ctx.dry_run,
            skip_email=ctx.parsed_args.skip_email,
            skip_db_updates=ctx.parsed_args.skip_db_updates,
        )
        if primary_group:
            batch_config.extend_extra_email_bcc(
                primary_group.support_cmt_mail_addresses
            )
        if person.deregistration_issue:
            batch_config.email_subject += f" {person.deregistration_issue}"
            batch_config.extend_extra_email_bcc(person.helpdesk_email)
        if new_group_id:
            new_group = wsjrdp2027.Group.db_load(
                ctx.hitobito_psycopg_connection(), new_group_id
            )
            person.move_to_group(new_group, ctx=ctx, batch_config=batch_config)
        else:
            new_group = None

        new_status = "deregistered"
        if person.status != new_status:
            batch_config.updates["new_status"] = new_status
        if note := _deregistration_note(
            ctx, batch_config=batch_config, person=person, new_group=new_group
        ):
            batch_config.updates["add_note"] = note

        _LOGGER.info(f"Primary group: {primary_group}")
        _LOGGER.info(f"Deregistration issue: {person.deregistration_issue}")
        _LOGGER.info("Bcc: %s", batch_config.extra_email_bcc)

        ctx.configure_log_file(ctx.make_out_path(batch_config.name).with_suffix(".log"))

        mailing = batch_config.prepare(
            person.df,
            msg_cb=lambda prepared: attach_cancellation_confirmation(ctx, prepared),
            out_dir=ctx.out_dir,
            open_editor=ctx.parsed_args.open_editor,
            report_all_updates=True,
        )

        ctx.update_db_and_send_mailing(
            mailing,
            conn=ctx.hitobito_psycopg_connection(read_only=False),
            zip_eml=False,
        )


if __name__ == "__main__":
    sys.exit(main())
