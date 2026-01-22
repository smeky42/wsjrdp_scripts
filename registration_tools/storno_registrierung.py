#!/usr/bin/env -S uv run
"""Tool to produce a cancellation request letter."""

from __future__ import annotations

import logging
import pathlib as _pathlib
import sys

import wsjrdp2027


SELFDIR = _pathlib.Path(__file__).parent.resolve()


_LOGGER = logging.getLogger()


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--issue")
    p.add_argument("--refund-amount", type=wsjrdp2027.to_int_or_none, default=None)
    p.add_argument("person_id")
    return p


def attach_cancellation_request(ctx, prepared: wsjrdp2027.PreparedEmailMessage) -> None:
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

    if (refund_amount := getattr(ctx.parsed_args, "refund_amount", None)) is not None:
        refund_amount_cents = refund_amount * 100
    else:
        refund_amount_cents = row["amount_paid_cents"]

    sys_inputs = {
        "hitobitoid": str(row["id"]),
        "role_id_name": row['role_id_name'],
        "full_name": row["full_name"],
        "birthday_de": row["birthday_de"],
        "deregistration_issue": (row["deregistration_issue"] or ""),
        "contract_names": json.dumps(row["contract_names"]),
        "amount_paid": wsjrdp2027.format_cents_as_eur_de(row["amount_paid_cents"]),
        "refund_amount": wsjrdp2027.format_cents_as_eur_de(refund_amount_cents),
        "refund_iban": row["sepa_iban"].upper().replace(" ", ""),
        "refund_account_holder": row["sepa_name"],
        "refund_show": str(refund_amount_cents > 0).lower(),
    }

    _LOGGER.info("sys_inputs:\n%s", pprint.pformat(sys_inputs))

    wsjrdp2027.typst_compile(
        SELFDIR / "storno_registrierung.typ", output=pdf_path, sys_inputs=sys_inputs
    )
    _LOGGER.info(f"Wrote {pdf_path}")
    pdf_bytes = pdf_path.read_bytes()
    prepared.message.add_attachment(
        pdf_bytes, maintype="application", subtype="pdf", filename=pdf_filename
    )
    prepared.eml_name = f"{prepared.mailing_name}.eml"


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        out_dir="data/storno_registrierung{{ kind | omit_unless_prod | upper | to_ext }}",
        argument_parser=create_argument_parser(),
        argv=argv,
    )

    batch_config = wsjrdp2027.BatchConfig.from_yaml(
        SELFDIR / "storno_registrierung.yml",
        where=wsjrdp2027.PeopleWhere(
            id=ctx.parsed_args.person_id,
            exclude_deregistered=False,
        ),
    )
    issue = ctx.parsed_args.issue or ""
    df = ctx.load_person_dataframe_for_batch(
        batch_config,
        extra_static_df_cols={"deregistration_issue": issue},
        extra_mailing_bcc=("unit-management@worldscoutjamboree.de" if issue else None),
    )

    row = df.iloc[0]
    id_and_name = f"{row['id']} {row['short_full_name']}"
    ctx.out_dir = ctx.out_dir / id_and_name
    batch_config.name = f"WSJ27 Storno Registrierung {id_and_name}"

    out_base = ctx.make_out_path(batch_config.name)
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx.psycopg_connect() as conn:
        mailing = batch_config.prepare_batch_for_dataframe(
            df,
            msg_cb=lambda p: attach_cancellation_request(ctx, p),
            out_dir=ctx.out_dir,
        )
        ctx.update_db_and_send_mailing(mailing, conn=conn, zip_eml=False)


if __name__ == "__main__":
    sys.exit(main())
