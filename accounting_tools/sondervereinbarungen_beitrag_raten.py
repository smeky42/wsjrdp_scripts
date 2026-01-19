#!/usr/bin/env -S uv run
"""Tool to produce a financtial Sondervereinbarung.

Uses the rules stored in the table wsj27_rdp_fee_rules.
"""

from __future__ import annotations

import logging
import pathlib as _pathlib
import re
import shutil as _shutil
import sys
import typing

import pandas as pd
import wsjrdp2027


if typing.TYPE_CHECKING:
    import docx as _docx


SELFDIR = _pathlib.Path(__file__).parent.resolve()


_LOGGER = logging.getLogger()


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--rdp-representative", choices=["DF"], required=False)
    p.add_argument(
        "--pdf",
        action="store_true",
        default=False,
        help="Create a PDF of the created docx. Required to send the PDF via email",
    )
    p.add_argument("--no-pdf", dest="pdf", action="store_false")
    p.add_argument("person_id")
    return p


_RDP_REPRESENTATIVE_TO_TOWN = {
    "CW": "Bamberg",
    "DF": "München",
    "FU": "München",
    "IH": "Bad Kreuznach",
}

_RDP_REPRESENTATIVE_TO_NAME = {
    "CW": "Carolin Windisch",
    "DF": "David Fritzsche",
    "FU": "Felix Unger",
    "IH": "Ines Höfig",
}


def load_docx(name: str) -> _docx.Document:  # type: ignore
    import os

    import docx as _docx

    p = os.path.join(os.path.dirname(__file__), name)
    return _docx.Document(p)


def convert_docx_to_pdf(docx_path, /, pdf_name=None) -> _pathlib.Path:
    import os

    import docx2pdf

    docx_path = _pathlib.Path(docx_path)
    if pdf_name:
        pdf_path = docx_path.parent / pdf_name
    else:
        pdf_path = os.path.splitext(docx_path)[0] + ".pdf"
    docx2pdf.convert(docx_path, output_path=pdf_path)
    return _pathlib.Path(pdf_path)


def installments_replacements_from_row(row: pd.Series, keys) -> dict[str, str]:
    import re

    def year_month_to_key(year, month):
        return f"i{str(year)[-2:]}_{month}"

    def to_eur(cents):
        return wsjrdp2027.format_cents_as_eur_de(cents, zero_cents="")

    keys_set = set(keys)
    installments_cents = row["installments_cents_dict"]
    replacements = {
        key: to_eur(cents)
        for (year, month), cents in installments_cents.items()
        if (key := year_month_to_key(year, month)) in keys_set
    }
    for key in sorted(set(keys) - set(replacements)):
        if re.fullmatch("i[0-9]+_[0-9]+", key):
            replacements[key] = to_eur(0)
    replacements = {
        k: v for k, v in sorted(replacements.items(), key=lambda item: item[0])
    }
    missing = {
        f"{year}-{month:02d}": to_eur(cents)
        for (year, month), cents in installments_cents.items()
        if (key := year_month_to_key(year, month)) not in keys_set
    }
    if missing:
        err_msg = f"Einige Raten werden nicht gedruckt: {missing}"
        _LOGGER.error("%s", err_msg)
        raise RuntimeError(err_msg)
    return replacements


def log_replacements(row, replacements):
    max_key_len = max(len(k) for k in replacements)
    replacements_str = "  " + "\n  ".join(
        f"{k.rjust(max_key_len)}: {v!r}" for k, v in replacements.items()
    )
    _LOGGER.info(
        "Replacements for %s %s:\n%s", row["id"], row["full_name"], replacements_str
    )


def attach_sondervereinbarung_raten(
    ctx, prepared: wsjrdp2027.PreparedEmailMessage
) -> None:
    import textwrap

    row = prepared.row
    assert row is not None

    _LOGGER.info(
        "id: %s, full_name: %s, row:\n%s",
        row["id"],
        row.get("full_name", None),
        textwrap.indent(row.to_string(), "  | "),
    )

    if not prepared.message:
        _LOGGER.warning("Cannot attach file to non-existing message")
        return

    import python_docx_replace

    rdp_representative_town = _RDP_REPRESENTATIVE_TO_TOWN.get(
        ctx.parsed_args.rdp_representative
    )
    rdp_representative_name = _RDP_REPRESENTATIVE_TO_NAME.get(ctx.parsed_args.rdp_representative, "")  # fmt: skip

    id = row["id"]
    full_name = row["full_name"]
    issue = row["custom_installments_issue"]
    total_fee_cents = row["total_fee_cents"]
    installments_cents = row["installments_cents_dict"]
    sum_installments_cents = sum((installments_cents or {}).values())
    if total_fee_cents != sum_installments_cents:
        err_msg = f"""Raten ergeben nicht zu zahlenden Betrag:
  Teilnahmebeitrag (in cents): {total_fee_cents}
  Summe der Raten (in cents): {sum_installments_cents}
  Raten (in cents): {installments_cents}
"""
        _LOGGER.error("%s", err_msg)
        raise RuntimeError(err_msg)
    is_jsf = row["total_fee_reduction_comment"] == "JSF"
    if is_jsf:
        special_agreement_title = (
            "Sondervereinbarung Ratenzahlungen & Jamboree Solidarity Fund (JSF)"
        )
    elif row["total_fee_reduction_cents"] > 0:
        special_agreement_title = "Sondervereinbarung Ratenzahlungen & Teilnahmebeitrag"
    else:
        special_agreement_title = "Sondervereinbarung Ratenzahlungen"
    person_id_line = (
        f"(Anmeldungs-Nummer {id} / Vorgang Sondervereinbarung: {issue})"
        if issue
        else f"(Anmeldungs-Nummer {id})"
    )
    total_fee_reduction_cents = int(row["total_fee_reduction_cents"])
    rdp_representative_town_date = f"{rdp_representative_town}, {row['today_de']}" if rdp_representative_town else ""  # fmt: skip

    if ctx.parsed_args.rdp_representative:
        template_filename = f"WSJ27-Sondervereinbarung-Beitrag-Raten-{ctx.parsed_args.rdp_representative}.docx"
    else:
        template_filename = f"WSJ27-Sondervereinbarung-Beitrag-Raten.docx"
    doc = load_docx(template_filename)
    doc_keys = python_docx_replace.docx_get_keys(doc)
    replacements = installments_replacements_from_row(row, keys=doc_keys)
    replacements |= {
        "creditor_id": wsjrdp2027.CREDITOR_ID,
        "total_fee": wsjrdp2027.format_cents_as_eur_de(row["total_fee_cents"]),
        "total_fee_reduction": wsjrdp2027.format_cents_as_eur_de(
            total_fee_reduction_cents
        ),
        "rdp_representative_name": rdp_representative_name,
        "rdp_representative_town_date": rdp_representative_town_date,
        "special_agreement_title": special_agreement_title,
        "sepa_mandate_title": f"SEPA-Mandat – Teilnahmebeitrag {full_name} (id {id})",
        "footer_special_agreement": f"Sondervereinbarung Ratenzahlung {id} – {full_name}",
        "footer_sepa_mandate": f"SEPA-Mandat {id} – {full_name}",
        "person_id_line": person_id_line,
    }
    hide_contact_b = bool(row["additional_contact_single"])
    for key in (k for k in doc_keys if k not in replacements):
        if (val := row.get(key)) is not None:
            replacements[key] = val
        elif hide_contact_b and re.fullmatch("additional_contact_.*_b", key):
            replacements[key] = "LEER"
    log_replacements(row, replacements)
    if missing := sorted(set(doc_keys) - set(replacements)):
        _LOGGER.error("Missing keys in document: %s", missing)
        raise RuntimeError(f"Missing keys in document: {', '.join(missing)}")

    python_docx_replace.docx_replace(doc, **replacements)
    blocks_kwargs = {
        "contact_b": not hide_contact_b,
        "vereinbarung_beitrag": (total_fee_reduction_cents > 0),
    }
    python_docx_replace.docx_blocks(doc, **blocks_kwargs)

    pdf_filename = f"{prepared.mailing_name}.pdf"
    pdf_name = ctx.make_out_path(pdf_filename)
    tmp_docx = ctx.out_dir.parent / "WSJ27-Sondervereinbarung.temp.docx"
    out_name = ctx.make_out_path(f"{prepared.mailing_name}.docx")

    doc.save(tmp_docx)
    try:
        _LOGGER.info(f"Wrote temporary docx to {tmp_docx}")
        _shutil.copy(tmp_docx, out_name)
        _LOGGER.info(f"Wrote {out_name}")
        if ctx.parsed_args.pdf:
            tmp_pdf = convert_docx_to_pdf(tmp_docx)
            tmp_pdf.rename(pdf_name)
            pdf_bytes = pdf_name.read_bytes()
            prepared.message.add_attachment(
                pdf_bytes, maintype="application", subtype="pdf", filename=pdf_filename
            )
            prepared.eml_name = f"{prepared.mailing_name}.eml"
        elif not ctx.dry_run:
            _LOGGER.warning("Cannot send email without producing PDFs")
    finally:
        tmp_docx.unlink(missing_ok=True)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        setup_logging=True,
        out_dir="data/sondervereinbarungen{{ kind | omit_unless_prod | upper | to_ext }}",
    )
    if not ctx.parsed_args.pdf:
        ctx.dry_run = True

    batch_config = wsjrdp2027.BatchConfig.from_yaml(
        SELFDIR / "sondervereinbarungen_beitrag_raten.yml",
        where=wsjrdp2027.PeopleWhere(
            id=ctx.parsed_args.person_id,
            exclude_deregistered=False,
            fee_rules=["planned", "active"],
        ),
    )
    df = ctx.load_person_dataframe_for_batch(batch_config)
    row = df.iloc[0]
    id_and_name = f"{row['id']} {row['short_full_name']}"
    ctx.out_dir = ctx.out_dir / id_and_name
    batch_config.name = f"WSJ27 Sondervereinbarung Ratenplan {id_and_name}"
    if row["custom_installments_issue"]:
        batch_config.extra_email_bcc = wsjrdp2027.merge_mail_addresses(
            batch_config.extra_email_bcc, "unit-management@worldscoutjamboree.de"
        )
    batch_config.update_raw_yaml()

    out_base = ctx.make_out_path(batch_config.name)
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx.psycopg_connect() as conn:
        mailing = batch_config.prepare_batch_for_dataframe(
            df,
            msg_cb=lambda p: attach_sondervereinbarung_raten(ctx, p),
            out_dir=ctx.out_dir,
        )
        ctx.update_db_and_send_mailing(mailing, conn=conn, zip_eml=False)


if __name__ == "__main__":
    sys.exit(main())
