#!/usr/bin/env -S uv run
"""Tool to produce a cancellation request letter."""

from __future__ import annotations

import datetime as _datetime
import logging
import pathlib as _pathlib
import sys

import wsjrdp2027


SELFDIR = _pathlib.Path(__file__).parent.resolve()
_SELF_NAME = _pathlib.Path(__file__).stem


_LOGGER = logging.getLogger()


def compute_contractual_compensation_cents(
    cents: int, today: _datetime.date | str | None = None
):
    today = wsjrdp2027.to_date(today)

    today_i = int(today.strftime("%Y%m%d"))

    if today_i >= 20270331:  # 31.03.2027
        return cents
    elif today_i >= 20263112:  # 31.12.2026
        return int(0.9 * cents)
    elif today_i >= 20263105:  # 31.05.2026
        return int(0.75 * cents)
    else:
        return int(0.5 * cents)


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--issue")
    p.add_argument("--refund-amount", type=wsjrdp2027.to_int_or_none, default=None)
    p.add_argument("person_id")
    return p


def attach_cancellation_request(
    ctx: wsjrdp2027.WsjRdpContext, prepared: wsjrdp2027.PreparedEmailMessage
) -> None:
    import json
    import pprint
    import textwrap

    assert prepared.message is not None
    row = prepared.row
    assert row is not None

    _LOGGER.info(
        "id: %s, full_name: %s, row:\n%s",
        row["id"],
        row.get("full_name", None),
        textwrap.indent(row.to_string(), "  | "),
    )

    pdf_filename = f"{prepared.mailing_name}.pdf"
    pdf_path = ctx.make_out_path(pdf_filename)

    additional_info = row.get("additional_info", {})
    deregistration_issue = additional_info.get("deregistration_issue", "")
    amount_paid_cents = row["amount_paid_cents"]

    #
    # BEGIN: manual values
    #
    total_fee_cents = row["total_fee_cents"]
    contractual_compensation_cents = compute_contractual_compensation_cents(
        total_fee_cents, today=ctx.today
    )
    # amount_paid_cents = None

    #
    # END: manual values
    #
    actual_compensation_cents = additional_info.get(
        "deregistration_actual_compensation_cents"
    )

    if actual_compensation_cents is not None and amount_paid_cents is not None:
        refund_amount_cents = max(amount_paid_cents - actual_compensation_cents, 0)
        missing_amount_cents = max(actual_compensation_cents - amount_paid_cents, 0)
    else:
        refund_amount_cents = None
        missing_amount_cents = None

    cancellation_date = wsjrdp2027.to_date_or_none(
        additional_info.get("deregistration_effective_date")
    )
    cancellation_date_de = (
        cancellation_date.strftime("%d.%m.%Y") if cancellation_date else None
    )

    def cents2eur(cents):
        return wsjrdp2027.format_cents_as_eur_de(cents).replace(" ", "\xa0")

    sys_inputs = {
        "hitobitoid": str(row["id"]),
        "role_id_name": row["role_id_name"],
        "full_name": row["full_name"],
        "birthday_de": row["birthday_de"],
        "deregistration_issue": deregistration_issue or "",
        "contract_names": json.dumps(row["contract_names"]),
        "amount_paid_cents": amount_paid_cents,
        "refund_amount_cents": refund_amount_cents,
        "missing_amount_cents": missing_amount_cents,
        "refund_iban": row["sepa_iban"].upper().replace(" ", ""),
        "refund_account_holder": row["sepa_name"],
        "contractual_compensation_cents": contractual_compensation_cents,
        "actual_compensation_cents": actual_compensation_cents,
        "cancellation_date_de": cancellation_date_de,
    }
    sys_inputs.update(
        {
            f"{k.removesuffix('cents')}display": cents2eur(v)
            for k, v in sys_inputs.items()
            if k.endswith("_cents") and v is not None
        }
    )
    sys_inputs = {k: str(v) for k, v in sys_inputs.items() if v is not None}

    _LOGGER.info("sys_inputs:\n%s", pprint.pformat(sys_inputs))

    wsjrdp2027.typst_compile(
        SELFDIR / f"{_SELF_NAME}.typ", output=pdf_path, sys_inputs=sys_inputs
    )
    _LOGGER.info(f"Wrote {pdf_path}")
    pdf_bytes = pdf_path.read_bytes()
    prepared.message.add_attachment(
        pdf_bytes, maintype="application", subtype="pdf", filename=pdf_filename
    )
    prepared.eml_name = f"{prepared.mailing_name}.eml"


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    )
    with ctx.psycopg_connect() as conn:
        p = ctx.load_person_for_id(ctx.parsed_args.person_id, conn=conn)
        assert p.df is not None
        ctx.out_dir = ctx.out_dir / p.id_and_name

        batch_config = wsjrdp2027.BatchConfig.from_yaml(
            SELFDIR / f"{_SELF_NAME}.yml",
            name=f"WSJ27 Abmeldung {p.role_id_name}",
            where=wsjrdp2027.PeopleWhere(id=p.id, exclude_deregistered=False),
        )
        if ctx.parsed_args.issue:
            p.deregistration_issue = ctx.parsed_args.issue
        if p.deregistration_issue:
            batch_config.extend_extra_email_bcc(p.helpdesk_email)
        batch_config.email_from = p.helpdesk_email

        out_base = ctx.make_out_path(batch_config.name)
        ctx.configure_log_file(out_base.with_suffix(".log"))

        mailing = batch_config.prepare(
            p,
            msg_cb=lambda p: attach_cancellation_request(ctx, p),
            out_dir=ctx.out_dir,
        )
        ctx.update_db_and_send_mailing(mailing, conn=conn, zip_eml=False)


if __name__ == "__main__":
    sys.exit(main())
