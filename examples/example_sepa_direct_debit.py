#!/usr/bin/env python
"""Example showing how to create a SEPA Direct Debit file."""

from __future__ import annotations

import datetime as _datetime
import sys

import pandas as pd
import wsjrdp2027

COLLECTION_DATE = _datetime.date(2025, 8, 14)


def main():
    ctx = wsjrdp2027.ConnectionContext()
    df = wsjrdp2027.load_people_dataframe(ctx)
    print(f"Registered: {len(df)}")

    df = df[~df["status"].isin(("registered", "deregistration_noted", "deregistered"))]
    print(f"Printed or Further: {len(df)}")

    dd = wsjrdp2027.SepaDirectDebit(wsjrdp2027.WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG)

    skipped_list = []

    for _, row in df.iterrows():
        if not all(row[col] for col in ["payment_role", "sepa_iban"]):
            skipped_list.append(row)
            print(
                "skip",
                row["id"],
                row["first_name"],
                row["last_name"],
                row["payment_role"],
                row["sepa_iban"],
            )
            continue
        print(row["id"], row["first_name"], row["last_name"])
        payment_role = wsjrdp2027.PaymentRole(row["payment_role"])
        people_id = row["id"]
        paid_for_name = row["first_name"] + " " + row["last_name"]

        description = (
            f"WSJ 2027 Gesamtbetrag Einmalzahlung für {paid_for_name} ({people_id})"
        )

        payment = {
            "name": row["sepa_name"],
            "IBAN": row["sepa_iban"],
            "BIC": row["sepa_bic"],
            "amount": payment_role.fee_due_by_date_in_cent(COLLECTION_DATE),
            "type": "OOFF",  # FRST,RCUR,OOFF,FNAL
            "collection_date": COLLECTION_DATE,
            "mandate_id": wsjrdp2027.mandate_id_from_hitobito_id(people_id),
            "mandate_date": row["print_at"],
            "description": description,
        }
        try:
            dd.add_payment(payment)
        except ValueError as exc:
            skipped_list.append(row)
            print(f"Skipped, due to error: {exc}")

    now_str = _datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    xml_file = f"data/example_sepa_direct_debit.{now_str}.xml"
    dd.export_file(xml_file)
    print(f"Wrote {xml_file}")

    skipped = pd.DataFrame(skipped_list)


if __name__ == "__main__":
    sys.exit(main())
