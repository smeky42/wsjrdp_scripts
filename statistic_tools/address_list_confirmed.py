#!/usr/bin/env -S uv run
"""Adressliste aller Personen mit status = 'confirmed' als Excel-Datei.

Spalten wie in der Vorlage; befüllt werden nur Anrede, Vorname, Nachname,
Straße, Hausnummer, PLZ und Ort. Die Anrede wird aus dem Geschlecht abgeleitet
(m -> Herr, w -> Frau, sonst leer). Als Vorname wird der Nickname verwendet,
falls vorhanden, sonst nur der erste Vorname.

Run with:
  WSJRDP_SCRIPTS_CONFIG=config-prod.yml uv run statistic_tools/address_list_confirmed.py
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd
import wsjrdp2027


COLUMNS = [
    "Firma (optional)",
    "Firma Zusatz (optional)",
    "Anrede",
    "Titel (optional)",
    "Vorname (optional)",
    "Nachname",
    "Straße",
    "Hausnummer",
    "Postfach (wenn vorhanden)",
    "PLZ",
    "Ort",
    "Land",
]
SALUTATION = {"m": "Herr", "w": "Frau"}


def create_argument_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description=__doc__)


def load_dataframe(conn) -> pd.DataFrame:
    query = """SELECT
      p.gender, p.first_name, p.nickname, p.last_name,
      p.street, p.housenumber, p.zip_code, p.town
    FROM people p
    WHERE p.status = 'confirmed'
    ORDER BY p.last_name, p.first_name, p.id"""
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=columns)


def _clean(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def build_address_list(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame("", index=df.index, columns=COLUMNS)
    out["Anrede"] = df["gender"].map(lambda g: SALUTATION.get(g, ""))
    nickname = _clean(df["nickname"])
    first_name = _clean(df["first_name"]).str.split().str[0].fillna("")
    out["Vorname (optional)"] = nickname.where(nickname != "", first_name)
    out["Nachname"] = _clean(df["last_name"])
    out["Straße"] = _clean(df["street"])
    out["Hausnummer"] = _clean(df["housenumber"])
    out["PLZ"] = _clean(df["zip_code"])
    out["Ort"] = _clean(df["town"])
    return out


def write_xlsx(df: pd.DataFrame, path) -> None:
    sheet = "Adressen"
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name=sheet, index=False)
        ws = writer.sheets[sheet]
        header = writer.book.add_format({"bold": True, "border": 1})
        text = writer.book.add_format({"num_format": "@"})
        for col, name in enumerate(df.columns):
            ws.write(0, col, name, header)
        # Keep leading zeros in PLZ / Hausnummer
        for name in ("Hausnummer", "PLZ"):
            col = df.columns.get_loc(name)
            for row, value in enumerate(df[name], start=1):
                ws.write_string(row, col, value, text)
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(df), len(df.columns) - 1)
        ws.autofit()


def main(argv=None) -> int:
    ctx = wsjrdp2027.WsjRdpContext(
        argv=argv,
        argument_parser=create_argument_parser(),
        __file__=__file__,
    )
    out_base = ctx.make_out_path("address_list_confirmed_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        df = load_dataframe(ro_conn)
        ctx.logger.info("%d people with status=confirmed", len(df))

        addresses = build_address_list(df)
        xlsx_path = out_base.with_suffix(".xlsx")
        write_xlsx(addresses, xlsx_path)
        ctx.register_output_file("Excel", xlsx_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
