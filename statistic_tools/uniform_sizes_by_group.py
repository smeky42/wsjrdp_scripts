#!/usr/bin/env -S uv run
"""Summe der bestellten Kluftgrößen pro Unit/Gruppe (status = 'confirmed').

Die Excel-Datei enthält
  * als erstes Blatt die Zusammenfassung: eine Zeile pro Gruppe (sortiert nach
    primary_group_id) mit der Anzahl je Kluftgröße, Ärmellänge und Verzicht auf
    die Kluft sowie eine Summenzeile, und
  * danach ein Blatt pro Gruppe mit den Daten der einzelnen Personen.

Ohne Angabe zur Ärmellänge wird "lang" angenommen (Vorauswahl im Formular).

In den Größen-Spalten der Zusammenfassung werden nur Personen gezählt, die eine
Kluft bekommen (uniform_order ist nicht false). Wer auf die Kluft verzichtet,
steht in der Spalte "verzichtet".

Die Spalten "Cappy", "Beanie" und "Kopfbedeckung ohne Angabe" zählen die
gewählte Kopfbedeckung aller Personen der Gruppe (Summe = "Gesamt").

Run with:
  WSJRDP_SCRIPTS_CONFIG=config-prod.yml uv run statistic_tools/uniform_sizes_by_group.py
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd
import wsjrdp2027


BASE_URL = "https://anmeldung.worldscoutjamboree.de"

# Order as in hitobito_wsjrdp_2027/config/settings.yml (uniform_size)
UNIFORM_SIZES = [
    "Klufthemd XS 35/36",
    "Klufthemd S 37/38",
    "Klufthemd M 39/40",
    "Klufthemd L 41/42",
    "Klufthemd XL 43/44",
    "Klufthemd XXL 45/46",
    "Klufthemd XXXL 47/48",
    "Klufthemd S-Schlank 38S",
    "Klufthemd M-Schlank 40S",
    "Klufthemd L-Schlank 42S",
    "Klufthemd XL-Schlank 44S",
    "Kluftbluse 34",
    "Kluftbluse 36",
    "Kluftbluse 38",
    "Kluftbluse 40",
    "Kluftbluse 42",
    "Kluftbluse 44",
    "Kluftbluse 46",
    "Kluftbluse 48",
]
NO_SIZE = "keine Angabe"
HEADGEAR = {"cappy": "Cappy", "beanie": "Beanie"}
NO_HEADGEAR = "Kopfbedeckung ohne Angabe"


def create_argument_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description=__doc__)


def load_dataframe(conn) -> pd.DataFrame:
    query = """SELECT
      p.primary_group_id,
      g.name AS group_name,
      p.id,
      CONCAT_WS(' ', p.first_name, p.last_name) AS name,
      p.uniform_size,
      p.uniform_long_sleeve,
      p.uniform_order,
      p.headgear
    FROM people p
    LEFT JOIN groups g ON g.id = p.primary_group_id
    WHERE p.status = 'confirmed'
    ORDER BY p.primary_group_id, p.last_name, p.first_name, p.id"""
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=columns)


def _sleeve_label(val) -> str:
    # Without an answer the form preselects long sleeves, so treat it as "lang".
    return "kurz" if val is False else "lang"


def _order_label(val) -> str:
    if val is False:
        return "verzichtet"
    elif val is True:
        return "ja"
    else:
        return ""


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    sizes = UNIFORM_SIZES + sorted(
        s for s in df["uniform_size"].dropna().unique() if s not in UNIFORM_SIZES
    )
    wants_uniform = df["uniform_order"].map(lambda v: v is not False)
    size = df["uniform_size"].fillna(NO_SIZE)
    sleeve = df["uniform_long_sleeve"].map(_sleeve_label)
    group_key = ["primary_group_id", "group_name"]

    summary = df.groupby(group_key).size().rename("Gesamt").to_frame()
    size_sleeve = size + " " + sleeve
    size_columns = [f"{s} {sl}" for s in sizes + [NO_SIZE] for sl in ("lang", "kurz")]
    size_counts = pd.crosstab(
        [df.loc[wants_uniform, k] for k in group_key], size_sleeve[wants_uniform]
    ).reindex(columns=size_columns, fill_value=0)
    sleeve_counts = (
        pd.crosstab(
            [df.loc[wants_uniform, k] for k in group_key], sleeve[wants_uniform]
        )
        .reindex(columns=["lang", "kurz"], fill_value=0)
        .rename(columns={"lang": "Ärmel lang", "kurz": "Ärmel kurz"})
    )
    declined = (~wants_uniform).groupby([df[k] for k in group_key]).sum()
    headgear = df["headgear"].map(lambda v: HEADGEAR.get(v, v) if v else NO_HEADGEAR)
    headgear_columns = list(HEADGEAR.values()) + [NO_HEADGEAR]
    headgear_counts = pd.crosstab([df[k] for k in group_key], headgear).reindex(
        columns=headgear_columns, fill_value=0
    )
    summary = summary.join(size_counts).join(sleeve_counts)
    summary["verzichtet"] = declined
    summary = summary.join(headgear_counts)
    summary = summary.fillna(0).astype(int)

    # Drop sizes nobody chose to keep the summary narrow, but always keep both
    # "lang" and "kurz" of every chosen size.
    empty_columns = [
        c
        for s in sizes + [NO_SIZE]
        if summary[[f"{s} lang", f"{s} kurz"]].sum().sum() == 0
        for c in (f"{s} lang", f"{s} kurz")
    ]
    summary = summary.drop(columns=empty_columns)

    summary = summary.reset_index().rename(
        columns={"primary_group_id": "Gruppen-ID", "group_name": "Gruppe"}
    )
    total = summary.drop(columns=["Gruppen-ID", "Gruppe"]).sum()
    total_row = {"Gruppen-ID": "", "Gruppe": "Summe", **total.to_dict()}
    return pd.concat([summary, pd.DataFrame([total_row])], ignore_index=True)


def build_details(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Gruppen-ID": df["primary_group_id"],
            "Gruppe": df["group_name"],
            "id": df["id"],
            "Name": df["name"],
            "Kluftgröße": df["uniform_size"].fillna(""),
            "Ärmellänge": df["uniform_long_sleeve"].map(_sleeve_label),
            "Kluft bestellen": df["uniform_order"].map(_order_label),
            "Kopfbedeckung": df["headgear"].map(lambda v: HEADGEAR.get(v, v or "")),
            "Link": [
                f"{BASE_URL}/groups/{gid}/people/{pid}"
                for gid, pid in zip(df["primary_group_id"], df["id"])
            ],
        }
    )


def _sheet_name(group_id, group_name, used: set[str]) -> str:
    """Excel sheet names: max. 31 chars, no []:*?/\\, unique."""
    name = str(group_name or f"Gruppe {group_id}")
    for ch in "[]:*?/\\":
        name = name.replace(ch, "-")
    name = name.strip("'")[:31] or f"Gruppe {group_id}"
    if name.lower() in used:
        suffix = f" ({group_id})"
        name = name[: 31 - len(suffix)] + suffix
    used.add(name.lower())
    return name


def write_xlsx(summary: pd.DataFrame, details: pd.DataFrame, path) -> None:
    sheet = "Zusammenfassung"
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        workbook = writer.book
        bold = workbook.add_format({"bold": True})
        title = workbook.add_format({"bold": True, "font_size": 14})
        header = workbook.add_format(
            {"bold": True, "text_wrap": True, "valign": "top", "bottom": 1}
        )

        summary_start = 1
        summary.to_excel(writer, sheet_name=sheet, startrow=summary_start, index=False)
        ws = writer.sheets[sheet]
        ws.write(
            0, 0, "Bestellte Kluftgrößen pro Unit/Gruppe (status = confirmed)", title
        )
        for col, name in enumerate(summary.columns):
            ws.write(summary_start, col, name, header)
        ws.set_row(summary_start, 60)
        total_row = summary_start + len(summary)
        ws.set_row(total_row, None, bold)

        ws.freeze_panes(summary_start + 1, 2)
        ws.autofit()
        ws.set_column(0, 0, 11)
        ws.set_column(2, len(summary.columns) - 1, 11)

        used = {sheet.lower()}
        for (group_id, group_name), group_df in details.groupby(
            ["Gruppen-ID", "Gruppe"], sort=True, dropna=False
        ):
            group_sheet = _sheet_name(group_id, group_name, used)
            group_df.to_excel(writer, sheet_name=group_sheet, index=False)
            gws = writer.sheets[group_sheet]
            for col, name in enumerate(group_df.columns):
                gws.write(0, col, name, header)
            gws.freeze_panes(1, 0)
            gws.autofilter(0, 0, len(group_df), len(group_df.columns) - 1)
            gws.autofit()


def main(argv=None) -> int:
    ctx = wsjrdp2027.WsjRdpContext(
        argv=argv,
        argument_parser=create_argument_parser(),
        __file__=__file__,
    )
    out_base = ctx.make_out_path("uniform_sizes_by_group_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        df = load_dataframe(ro_conn)
        ctx.logger.info("%d people with status=confirmed", len(df))
        if df.empty:
            return 0

        summary = build_summary(df)
        details = build_details(df)
        with pd.option_context("display.width", 250, "display.max_columns", None):
            print(summary.to_string(index=False))

        xlsx_path = out_base.with_suffix(".xlsx")
        write_xlsx(summary, details, xlsx_path)
        ctx.register_output_file("Excel", xlsx_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
