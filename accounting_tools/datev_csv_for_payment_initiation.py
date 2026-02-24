#!/usr/bin/env -S uv run
from __future__ import annotations

import datetime as _datetime
import logging as _logging
import pathlib as _pathlib

import psycopg as _psycopg
import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)


# See https://developer.datev.de/de/file-format/details/datev-format/format-description/booking-batch


def to_win1252_compatible(string: str) -> str:
    import unicodedata

    result = []
    for char in string:
        try:
            char.encode("cp1252")
            result.append(char)
        except UnicodeEncodeError:
            normalized = unicodedata.normalize("NFD", char)
            base_char = "".join(c for c in normalized if not unicodedata.combining(c))
            try:
                base_char.encode("cp1252")
            except:
                _LOGGER.error(f"Cannot encode {base_char!r}")
                base_char = "?"
            result.append(base_char)
    return "".join(result)


def prepare_description(
    string: str, *, encoding: str = "utf-8", max_bytes_length: int = 60
) -> str:
    if encoding in ("cp1252", "win1252", "windows-1252"):
        string = to_win1252_compatible(string)
    if max_bytes_length:
        max_str_length = max_bytes_length
        string = string[:max_str_length]
        while len(string.encode(encoding)) > max_bytes_length:
            max_str_length -= 1
            string = string[:max_str_length]
    return string


def write_datev_csv_for_pain_id(
    *,
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    pain_id: int,
    csv_encoding: str = "cp1252",
    limit: int | None = None,
    offset: int | None = None,
    now: _datetime.date | None = None,
):
    import re

    now = wsjrdp2027.to_datetime(now, now=ctx.start_time)

    df = wsjrdp2027.pg_select_dataframe(
        conn,
        t"""SELECT
  id, payment_status,
  dbtr_name, dbtr_iban, dbtr_bic,
  amount_cents, amount_currency,
  debit_sequence_type, collection_date,
  mandate_id, mandate_date,
  description, endtoend_id
FROM wsjrdp_direct_debit_pre_notifications
WHERE payment_status = 'xml_generated'
  AND payment_initiation_id = {pain_id}
LIMIT {limit}
OFFSET {offset or 0}
""",
    )

    row = df.iloc[0]
    collection_date = row["collection_date"]

    match csv_encoding.lower():
        case "cp1252" | "win1252" | "windows-1252":
            encoding = "cp1252"
            csv_file_encoding = "cp1252"
            encoding_filename_suffix = "cp1252"
        case "utf-8" | "utf-8-sig":
            encoding = "utf-8"
            csv_file_encoding = "utf-8-sig"
            encoding_filename_suffix = "utf8"
        case _:
            raise RuntimeError("Unsupported CSV encoding: {csv_encoding!r}")

    base_csv_filename = ctx.make_out_path(
        f"EXTF_sammeleinzug_wsj27_{collection_date.strftime('%Y-%m')}_pain{pain_id}_{encoding_filename_suffix}"
    )
    if limit is not None:
        base_csv_filename = base_csv_filename.with_name(
            name=base_csv_filename.name + f"_limit{limit}"
        )
    if offset is not None:
        base_csv_filename = base_csv_filename.with_name(
            name=base_csv_filename.name + f"_offset{offset}"
        )
    csv_filename = base_csv_filename.with_name(base_csv_filename.name + ".csv")

    beraternummer = ctx.config.datev_beraternummer
    mandantennummer = ctx.config.datev_mandantennummer
    if collection_date.year < 2026:
        konto = "1200"
        gegenkonto = "8116"
        kost1 = "9500"
        kost2 = None
        sachkontenlaenge = 4
        sachkontenrahmen = "03"
    else:
        konto = "18000"
        gegenkonto = "41030"
        kost1 = None
        kost2 = "9500"
        sachkontenlaenge = 5
        sachkontenrahmen = "42"

    with open(csv_filename, "w", encoding=csv_file_encoding, newline="\r\n") as csvfile:
        d_writer = wsjrdp2027.datev.DatevExtfWriter(csvfile)
        datev_header = [
            '"EXTF"',  # 1 - Kennzeichen
            700,  # 2 - Versionsnummer
            21,  # 3 - Formatkategorie
            '"Buchungsstapel"',  # 4 - Formatname
            13,  # 5 - Formatversion
            now.strftime("%Y%m%d%H%M%S%f")[:-3],  # 6 - Erzeugt am YYYYMMDDHHMMSSFFF
            # 20240130140440439
            # 20260220024353259
            None,  # 7 - Importiert
            '""',  # 8 - Herkunft
            '"wsjrdp"',  # 9 - Exportiert von
            '""',  # 10 - Importiert von
            beraternummer,  # 11 - Beraternummer
            mandantennummer,  # 12 - Mandantennummer
            collection_date.replace(month=1, day=1).strftime(
                "%Y%m%d"
            ),  # 13 - WJ-Beginn YYYYMMDD
            sachkontenlaenge,  # 14 - Sachkontenlänge
            collection_date.strftime("%Y%m%d"),  # 15 - Datum von
            collection_date.strftime("%Y%m%d"),  # 16 - Datum bis
            f'"Sammeleinzug {collection_date.strftime("%Y-%m")}"',  # 17 - Bezeichnung
            '""',  # 18 - Diktatkürzel
            1,  # 19 - Buchungstyp
            0,  # 20 - Rechungslegungszweck
            0,  # 21 - Festschreibung
            '"EUR"',  # 22 - WKZ
            None,  # 23 - Reserviert
            '""',  # 24 - Derivatskennzeichen
            None,  # 25 - Reserviert
            None,  # 26 - Reserviert
            f'"{sachkontenrahmen}"',  # 27 - Sachkontenrahmen
            None,  # 28 - ID der Branchenlösung
            None,  # 29 - Reserviert
            '""',  # 30 - Reserviert
            '""',  # 31 - Anwendungsinformation
        ]
        csvfile.write(
            ";".join(str(s if s is not None else "") for s in datev_header) + "\n"
        )
        d_writer.writeheader()
        for _, row in df.iterrows():
            amount_currency = row.get("amount_currencty") or "EUR"
            amount_cents = int(round(row["amount_cents"]))
            amount_eur = amount_cents / 100
            amount_de = str(f"{amount_eur:.2f}").replace(".", ",").replace(",00", "")
            collection_date = row["collection_date"]
            belegfeld = f"Einzug-{collection_date.strftime('%Y-%m')}-{row['debit_sequence_type']}-{pain_id}-{row['id']}"
            buchungstext = row["description"].replace(" WSJ 2027 ", " ")
            buchungstext = buchungstext.removeprefix("WSJ 2027 ")
            buchungstext = re.sub(
                r"Beitrag *(?P<descr>.*) +(?P<role>CMT|YP|UL|IST) (?P<id>[0-9]+)",
                r"\g<role> \g<id> \g<descr> / Beitrag",
                buchungstext,
            )
            buchungstext = re.sub(
                r"(?P<role>CMT|YP|UL|IST) Beitrag *(?P<descr>.*) +(?P<id>[0-9]+)",
                r"\g<role> \g<id> \g<descr> / Beitrag",
                buchungstext,
            )
            buchungstext = re.sub(
                r"(?P<installment>[0-9]+. Rate \w+ 202[567]) (?P<role>CMT|YP|UL|IST) *(?P<descr>.*) +\(id (?P<id>[0-9]+)\)",
                r"\g<role> \g<id> \g<descr> / \g<installment>",
                buchungstext,
            )
            buchungstext = prepare_description(
                buchungstext, encoding=encoding, max_bytes_length=60
            )
            d_writer.writerow(
                {
                    "Umsatz (ohne Soll/Haben-Kz)": amount_de.lstrip("-"),
                    "Soll/Haben-Kennzeichen": "S" if amount_cents > 0 else "H",
                    "WKZ Umsatz": amount_currency,
                    "Konto": konto,
                    "Gegenkonto (ohne BU-Schlüssel)": gegenkonto,
                    "Belegdatum": collection_date.strftime("%d%m"),
                    "Belegfeld 1": belegfeld[:36],
                    "Buchungstext": buchungstext,
                    "KOST1 - Kostenstelle": kost1,
                    "KOST2 - Kostenstelle": kost2,
                    "Festschreibung": "0",
                }
            )
    _LOGGER.info("    %s rows in dataframe", len(df))
    _LOGGER.info(f"  wrote {csv_filename}")


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--csv-encoding", choices=["cp1252", "utf-8"], default="utf-8")
    p.add_argument("--limit", type=lambda s: int(s, base=10))
    p.add_argument("--offset", type=lambda s: int(s, base=10))
    p.add_argument("pain_ids", nargs="*", type=lambda s: int(s, base=10))
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        __file__=__file__,
    )
    out_base = ctx.make_out_path(_pathlib.Path(__file__).stem)
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    with ctx.psycopg_connect() as conn:
        for pain_id in ctx.parsed_args.pain_ids:
            write_datev_csv_for_pain_id(
                ctx=ctx,
                conn=conn,
                pain_id=pain_id,
                csv_encoding=ctx.parsed_args.csv_encoding,
                limit=ctx.parsed_args.limit,
                offset=ctx.parsed_args.offset,
            )


if __name__ == "__main__":
    import sys

    sys.exit(main())
