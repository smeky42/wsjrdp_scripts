#!/usr/bin/env -S uv run
from __future__ import annotations

import datetime
import sys

import pandas as pd
import wsjrdp2027
import xlsxwriter


def main():
    ctx = wsjrdp2027.WsjRdpContext()

    with ctx.psycopg2_connect() as conn:
        cur = conn.cursor()
        cur.execute("""
SELECT id, first_name, last_name, status,
       sepa_name, sepa_iban,
       payment_role, early_payer, print_at
FROM people
WHERE print_at < '2025-08-01'
ORDER BY last_name ASC, first_name ASC
""")

        rows = cur.fetchall()
        columns = [
            "id",
            "first_name",
            "last_name",
            "status",
            "Kontoinhaber*in",
            "IBAN",
            "payment_role",
            "early_payer",
            "Datum SEPA-Mandat",
        ]
        df = pd.DataFrame(rows, columns=columns)

        cur.close()

    print(f"Registered: {len(df)}")

    df = df[df["status"].isin(["reviewed", "upload"])]
    print(f"Reviewed & Uploaded: {len(df)}")

    df = df[df["early_payer"] == True]  # noqa: E712
    print(f"Reviewed & Uploaded Early Payers: {len(df)}")

    # reset indexing
    df.reset_index(drop=True, inplace=True)

    df["Mandatsreferenz"] = df["id"].map(wsjrdp2027.mandate_id_from_hitobito_id)
    df["IBAN"] = df["IBAN"].map(lambda s: s.upper().replace(" ", ""))

    def to_initial(name: str):
        return f"{name[0].upper()}." if name else ""

    df["Teilnehmer*in"] = (
        df["first_name"].map(to_initial) + " " + df["last_name"].map(to_initial)
    )
    df["Betrag"] = df["payment_role"].map(
        lambda role: wsjrdp2027.PaymentRole(role).full_fee_eur
    )
    sum_eur = sum(df["Betrag"])
    print(f"Sum EUR: {sum_eur}")

    filename_prefix = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    xslx_filename = f"{filename_prefix}--Einzug-Preview-Lastschriftvereinbarung.xlsx"
    writer = pd.ExcelWriter(xslx_filename, engine="xlsxwriter")
    sheet_name = "Einzug August 2025"
    df.to_excel(
        writer,
        engine="xlsxwriter",
        index=False,
        columns=[
            "Teilnehmer*in",
            "Kontoinhaber*in",
            "IBAN",
            "Mandatsreferenz",
            "Datum SEPA-Mandat",
            "Betrag",
        ],
        sheet_name=sheet_name,
    )
    (max_row, _) = df.shape
    workbook: xlsxwriter.Workbook = writer.book  # type: ignore
    eur_format = workbook.add_format({"num_format": "#,##0.00 [$€-407]"})
    left_align = workbook.add_format({"align": "left"})

    worksheet = writer.sheets[sheet_name]

    worksheet.freeze_panes(1, 0)

    worksheet.set_column(0, 3, None, left_align)
    worksheet.autofit()
    worksheet.set_column(5, 5, 12, eur_format)
    worksheet.write_formula(
        f"F{max_row + 2}", f"=SUM(F2:F{max_row + 1})", eur_format, sum_eur
    )

    writer.close()
    print(f"Finished writing {xslx_filename}")


if __name__ == "__main__":
    sys.exit(main())
