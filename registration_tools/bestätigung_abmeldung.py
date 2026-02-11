#!/usr/bin/env -S uv run
"""Tool to produce a cancellation request letter."""

from __future__ import annotations

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
    p.add_argument(
        "--open-editor",
        action="store_true",
        default=False,
        help="Open E-Mail body content in editor before preparing EML file",
    )
    p.add_argument("person_id")
    return p


def attach_cancellation_confirmation(
    ctx, prepared: wsjrdp2027.PreparedEmailMessage
) -> None:
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

    sys_inputs = {
        "hitobitoid": str(row["id"]),
        "role_id_name": row["role_id_name"],
        "full_name": row["full_name"],
        "birthday_de": row["birthday_de"],
        "deregistration_issue": (row["deregistration_issue"] or ""),
    }

    wsjrdp2027.typst_compile(
        SELFDIR / f"{_pathlib.Path(__file__).stem}.typ",
        output=pdf_path,
        sys_inputs=sys_inputs,
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
    person_id = int(ctx.parsed_args.person_id)
    with ctx.psycopg_connect() as conn:
        row = wsjrdp2027.load_person_row(conn, person_id=person_id)
        short_role_name = row["payment_role"].short_role_name
        id_and_name = row["role_id_name"]
        deregistration_issue = row["additional_info"]["deregistration_issue"]
        try:
            group = wsjrdp2027.Group.db_load(
                conn, group_arg=int(row["primary_group_id"])
            )
        except Exception as exc:
            _LOGGER.info("Could not load associated group: %s", str(exc))
            group = None

        batch_config = wsjrdp2027.BatchConfig.from_yaml(
            SELFDIR / f"{_SELF_NAME}_{short_role_name}.yml",
            name=f"WSJ27 Bestätigung Abmeldung {id_and_name}",
            where=wsjrdp2027.PeopleWhere(
                id=person_id,
                exclude_deregistered=False,
            ),
        )
        if group:
            batch_config.extend_extra_email_bcc(group.support_cmt_mail_addresses)
            _LOGGER.info("BCC: %s", batch_config.extra_email_bcc)
        ctx.out_dir = ctx.out_dir / id_and_name
        out_base = ctx.make_out_path(batch_config.name)
        log_filename = out_base.with_suffix(".log")
        ctx.configure_log_file(log_filename)

        df = ctx.load_person_dataframe_for_batch(
            batch_config,
            extra_static_df_cols={"deregistration_issue": deregistration_issue},
        )

        mailing = batch_config.prepare_batch_for_dataframe(
            df,
            msg_cb=lambda p: attach_cancellation_confirmation(ctx, p),
            out_dir=ctx.out_dir,
            open_editor=ctx.parsed_args.open_editor,
        )
        ctx.update_db_and_send_mailing(mailing, conn=conn, zip_eml=False)

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
