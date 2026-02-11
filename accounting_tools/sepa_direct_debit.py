#!/usr/bin/env -S uv run
"""SEPA direct debit to collect dues of Jamboree participants

* Create a SEPA direct debit XML file
* Update the accounting_entries database table
* Write a summary Excel xlsx file
* TODO: Create pre-notification email messages and send them

"""

from __future__ import annotations

import datetime
import logging
import sys
import typing as _typing

import wsjrdp2027


if _typing.TYPE_CHECKING:
    import pandas as _pandas

_LOGGER = logging.getLogger()


DEFAULT_COLLECTION_DATE = datetime.date(2025, 1, 1)

_PMT_INF_UPSERT_COLS = [
    "payment_information_identification",
    "batch_booking",
    "number_of_transactions",
    "control_sum_cents",
    "payment_type_instrument",
    "debit_sequence_type",
    "requested_collection_date",
    "cdtr_name",
    "cdtr_iban",
    "cdtr_bic",
    "cdtr_address",
    "creditor_id",
]


def _find_planned_payment_initiation(
    ctx: wsjrdp2027.WsjRdpContext, args, conn
) -> _pandas.Series:
    import textwrap

    pain_df = wsjrdp2027.pg_select_dataframe(
        conn,
        t"SELECT * FROM wsjrdp_payment_initiations WHERE status = 'planned'",
    )
    if pain_df.empty:
        _LOGGER.error("Could not find a planned payment initiation.")
        raise SystemExit(1)
    if len(pain_df) > 1:
        _LOGGER.error("Found more than one planned payment initiation.")
        for _, row in pain_df.iterrows():
            _LOGGER.error(
                "* planned payment initiation %s:\n%s",
                row["id"],
                textwrap.indent(row.to_string(), "  | "),
            )
        raise SystemExit(1)
    else:
        assert len(pain_df) == 1
        row = pain_df.iloc[0]
        args.payment_initiation_id = row.id
        _LOGGER.info(
            "Found planned payment initiation %s:\n%s",
            row["id"],
            textwrap.indent(row.to_string(), "  | "),
        )
    return row


def _upsert_payment_initiation_and_df_and_create_payment_infos(
    conn,
    *,
    df: _pandas.DataFrame,
    pain_id: int | None,
    pain_message: wsjrdp2027.PainMessage,
) -> None:
    pain_updates = dict(
        status="xml_generated",
        sepa_schema=pain_message.sepa_schema,
        message_identification=pain_message.message_identification,
        number_of_transactions=pain_message.number_of_transactions,
        control_sum_cents=pain_message.control_sum_cents,
        initiating_party_name=pain_message.initiating_party_name,
    )
    if pain_id is None:
        pain_id = wsjrdp2027.pg_insert_payment_initiation(conn, **pain_updates)  # type: ignore
        df["pn_payment_initiation_id"] = pain_id
        df["sepa_dd_payment_initiation_id"] = pain_id
        _LOGGER.info("Finished INSERT wsjrdp_payment_initiations id=%s", pain_id)
    else:
        wsjrdp2027.pg_update_payment_initiation(conn, id=pain_id, updates=pain_updates)
        _LOGGER.info("Finished UPDATE wsjrdp_payment_initiations id=%s", pain_id)

    endtoend_id2pmt_inf_id = {}
    seq_tp2pmt_inf_id = {}

    for pmt_inf in pain_message.payment_infos:
        insert_pmt_inf_kwargs = {k: getattr(pmt_inf, k) for k in _PMT_INF_UPSERT_COLS}
        pmt_inf_id = wsjrdp2027.pg_insert_direct_debit_payment_info(
            conn, payment_initiation_id=pain_id, **insert_pmt_inf_kwargs
        )
        seq_tp2pmt_inf_id[pmt_inf.debit_sequence_type] = pmt_inf_id
        endtoend_id2pmt_inf_id.update(
            {tx_inf.endtoend_id: pmt_inf_id for tx_inf in pmt_inf.direct_debit_tx_infs}
        )
        _LOGGER.info(
            "Finished INSERT wsjrdp_direct_debit_payment_infos id=%s", pmt_inf_id
        )

    df["pn_direct_debit_payment_info_id"] = df["sepa_dd_endtoend_id"].map(
        lambda id: endtoend_id2pmt_inf_id.get(id)
    )
    df["sepa_dd_direct_debit_payment_info_id"] = df["pn_direct_debit_payment_info_id"]


def _report_pain_message(pain_message):
    _LOGGER.info("")
    _LOGGER.info("Parsed PAIN message:")
    _LOGGER.info("  message_identification: %s", pain_message.message_identification)
    _LOGGER.info("  creation_date_time: %s", pain_message.creation_date_time)
    _LOGGER.info("  number_of_transactions: %s", pain_message.number_of_transactions)
    _LOGGER.info("  control_sum_cents: %s", pain_message.control_sum_cents)
    _LOGGER.info("  initiating_party_name: %s", pain_message.initiating_party_name)
    _LOGGER.info("  len(payment_infos): %s", len(pain_message.payment_infos))
    pmt_inf_sum = sum(
        pmt_inf.control_sum_cents for pmt_inf in pain_message.payment_infos
    )
    if pmt_inf_sum != pain_message.control_sum_cents:
        raise RuntimeError("Payment Initiation Message control sum mismatch!")
    for i, pmt_inf in enumerate(pain_message.payment_infos, start=1):
        _LOGGER.info("  PmtInf %s", i)
        for key in _PMT_INF_UPSERT_COLS:
            _LOGGER.info("    %s: %s", key, getattr(pmt_inf, key, None))
        tx_inf_sum = sum(tx_inf.amount_cents for tx_inf in pmt_inf.direct_debit_tx_infs)
        tx_infs_len = len(pmt_inf.direct_debit_tx_infs)
        if tx_infs_len == 0:
            _LOGGER.info("    transactions: []")
        else:
            _LOGGER.info("    transactions:")
        for i, tx_inf in enumerate(pmt_inf.direct_debit_tx_infs):
            if i < 10 or (tx_infs_len - i) < 5 or tx_infs_len < 20:
                tx_amount = wsjrdp2027.format_cents_as_eur_de(
                    tx_inf.amount_cents, zero_cents=",00"
                )
                tx_inf_msg = f"      {i + 1:>4} {tx_amount:>10} | {tx_inf.endtoend_id} | {tx_inf.description}"
                _LOGGER.info(tx_inf_msg)
            elif i == 10:
                _LOGGER.info("                    ...")
        if tx_inf_sum != pmt_inf.control_sum_cents:
            raise RuntimeError("Payment Info control sum mismatch!")


def _report_df(
    df: _pandas.DataFrame, *, pain_message: wsjrdp2027.PainMessage | None = None
) -> None:
    import textwrap

    def to_eur(cents):
        return wsjrdp2027.format_cents_as_eur_de(cents, zero_cents=",00")

    df_ok = df[df["payment_status"] == "ok"]
    df_not_ok = df[df["payment_status"] != "ok"]

    if len(df_not_ok) > 0:
        _LOGGER.info("")
        _LOGGER.info("==== Skipped payments")
        _LOGGER.info("  Number of skipped payments: %s", len(df_not_ok))
        print(flush=True)
        for _, row in df_not_ok.iterrows():
            skipped_msg = (
                f"Found payment_status != 'ok'\n"
                f"  {row['payment_role'].short_role_name} {row['id']} {row['short_full_name']}\n"
                f"  payment_status: {row['payment_status']}\n"
                f"  payment_status_reason: {row['payment_status_reason']}\n"
                f"  open_amount_cents: {to_eur(row['open_amount_cents'])}\n"
                f"  pn_comment: {row['pn_comment']}\n"
            )
            _LOGGER.info(skipped_msg)
            _LOGGER.debug("row:\n%s", textwrap.indent(row.to_string(), "  | "))
        _LOGGER.info(
            "  Skipped payments: SUM(open_amount_cents): %s",
            wsjrdp2027.format_cents_as_eur_de(df_not_ok["open_amount_cents"].sum()),
        )
    else:
        _LOGGER.info("")
        _LOGGER.info("==== No Skipped payments")

    _LOGGER.info("")
    _LOGGER.info("==== Payments")
    _LOGGER.info("  Number of payments: %s", len(df_ok))
    _LOGGER.info(
        "  Payments: SUM(open_amount_cents): %s",
        wsjrdp2027.format_cents_as_eur_de(df_ok["open_amount_cents"].sum()),
    )
    _LOGGER.info("")
    if pain_message:
        sum_amounts_df_ok = df_ok["open_amount_cents"].sum()
        if sum_amounts_df_ok != pain_message.control_sum_cents:
            err_msg = (
                f"pain message sum = {pain_message.control_sum_cents}"
                f" inconsistent with"
                f" sum(open_amount_cents) = {sum_amounts_df_ok} (where payment_status == 'ok')"
            )
            _LOGGER.error(err_msg)
            raise RuntimeError(err_msg)
        else:
            ok_msg = (
                f"OK: pain message sum = {pain_message.control_sum_cents}"
                f" equals"
                f" sum(open_amount_cents) = {sum_amounts_df_ok} (where payment_status == 'ok')"
            )
            _LOGGER.info(ok_msg)


def _write_mail_txt(path, pain_message: wsjrdp2027.PainMessage) -> None:
    pmt_inf = pain_message.payment_infos[0]
    collection_date = pmt_inf.requested_collection_date
    amount_cents = pain_message.control_sum_cents

    msg = wsjrdp2027.render_template(
        """Hallo Jessi,

die SEPA Lastschrift Datei für den {{ collection_date | month_year_de }} Jamboree Einzug findest du hier: <NEXTCLOUD-LINK>

Kannst du sie bitte einspielen. Einzugsdatum ist der {{ collection_date | date_de }}, die E-Mail Ankündigungen haben wir am <DD.MM.YYYY> verschickt.

Die Datei enthält einen Batch mit {{ pain_message.number_of_transactions }} Buchungen und der Gesamt-Betrag ist {{ amount_cents | format_cents_as_eur_de }}:

Zahl der Buchungen: {{ pain_message.number_of_transactions }}
Gesamt-Betrag: {{ amount_cents | format_cents_as_eur_de }}
Empfänger-Konto:
  IBAN {{ pmt_inf.cdtr_iban }}
  BIC {{ pmt_inf.cdtr_bic }}
Gläubiger-Identifikationsnummer: {{ pmt_inf.creditor_id }}
Einzugsdatum: {{ collection_date | date_de }}


Liebe Grüße und Gut Pfad
Daffi
""",
        context=dict(
            pain_message=pain_message,
            pmt_inf=pmt_inf,
            amount_cents=amount_cents,
            collection_date=collection_date,
        ),
    )

    with open(path, "w", encoding="utf-8", newline="\n") as f:
        print(msg, file=f)


def _write_datev_csv(
    path, pain_message: wsjrdp2027.PainMessage, df: _pandas.DataFrame
) -> None:
    import csv

    COLS = [
        "Umsatz",
        "BU_Gegenkonto_H",
        "Belegfeld1",
        "Datum",
        "Konto_S",
        "KOST1",
        "Buchungstext",
    ]

    count = 0
    skipped = 0
    with open(path, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=COLS, delimiter=";")
        writer.writeheader()

        for _, row in df.iterrows():
            if row["payment_status"] != "ok":
                skipped += 1
                continue

            if row.get("pn_id") is not None:
                pn_payment_status: str | None = row.get("pn_payment_status")
                if pn_payment_status != "pre_notified" or row.get("pn_try_skip"):
                    skipped += 1
                    continue
            amount_eur = int(round(row["open_amount_cents"])) / 100
            amount_de = str(f"{amount_eur:.2f}").replace(".", ",")
            collection_date = row["collection_date"]
            belegfeld = (
                f"{wsjrdp2027.to_month_year_de(collection_date)} Einzug"
                f" (id={row['sepa_dd_payment_initiation_id']})"
                f" MsgId {pain_message.message_identification}"
            )
            buchungstext = row["sepa_dd_description"].replace(" WSJ 2027 ", " ")
            buchungstext = buchungstext.removeprefix("WSJ 2027 ")
            writer.writerow(
                {
                    "Umsatz": amount_de,
                    "BU_Gegenkonto_H": "8116",
                    "Belegfeld1": belegfeld,
                    "Datum": collection_date.strftime("%d.%m.%Y"),
                    "Konto_S": "1200",
                    "KOST1": "9500",  # Kostenstelle für Beiträge
                    "Buchungstext": buchungstext,
                }
            )
            count += 1
    _LOGGER.info("Wrote DATEV CSV %s", path)
    _LOGGER.info("    %s rows in dataframe", len(df))
    _LOGGER.info("    %s rows written to CSV", count)
    _LOGGER.info("    %s rows skipped", skipped)


def create_argument_parser():
    import argparse

    from wsjrdp2027 import to_date_or_none

    p = argparse.ArgumentParser()
    p.add_argument("--accounting", action="store_true", default=True)
    p.add_argument("--no-accounting", dest="accounting", action="store_false")
    p.add_argument(
        "--collection-date",
        type=to_date_or_none,
        default=None,
        help="Collection date of the SEPA direct debit",
    )
    p.add_argument("--payment-initiation-id", type=int)
    p.add_argument(
        "--find-planned-payment-initiation", action="store_true", default=False
    )
    p.add_argument(
        "--allow-xml-generated",
        action="store_true",
        default=False,
        help="""Allow pre notifications in status xml_generated when reading
        from an existing payment initiation.""",
    )
    p.add_argument(
        "--ignore-accounting-entries-for-payment-initiation",
        action="store_true",
        default=False,
    )
    p.add_argument("--exclude-id", action="append")
    p.add_argument("--end-to-end-id-suffix", default=None)
    p.add_argument("--rollback-for-testing", action="store_true", default=False)
    return p


def _excluded_ids_from_parsed_arg(arg: list[str] | None) -> list[int]:
    if arg is None:
        return []
    excluded_ids = []
    for a in arg:
        excluded_ids.extend(int(x) for x in a.replace(" ", ",").split(",") if x)
    return excluded_ids


def main(argv=None):
    import textwrap

    ctx = wsjrdp2027.WsjRdpContext(
        argv=argv,
        argument_parser=create_argument_parser(),
        out_dir="data/sepa_direct_debit_{{ filename_suffix }}",
    )
    args = ctx.parsed_args
    if ctx.dry_run:
        if args.accounting:
            _LOGGER.info("Dry run: Deactivate accounting as-if --no-accounting")
        args.accounting = False
    out_base = ctx.make_out_path("sepa_direct_debit_{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    xml_filename = out_base.with_suffix(".xml")
    xlsx_filename = out_base.with_suffix(".xlsx")
    mail_txt_filename = out_base.with_suffix(".mail.txt")
    datev_csv_filename = out_base.with_suffix(".datev.csv")

    ctx.configure_log_file(log_filename)

    pain_row: _pandas.Series | None = None
    pain_id: int | None = None

    if ctx.parsed_args.allow_xml_generated:
        allowed_pre_notification_status = ("pre_notified", "xml_generated")
    else:
        allowed_pre_notification_status = "pre_notified"

    excluded_ids = _excluded_ids_from_parsed_arg(ctx.parsed_args.exclude_id)

    with ctx.psycopg_connect() as conn:
        if args.find_planned_payment_initiation:
            pain_row = _find_planned_payment_initiation(ctx, args, conn)
        elif args.payment_initiation_id:
            pain_df = wsjrdp2027.pg_select_dataframe(
                conn,
                t"""SELECT * FROM wsjrdp_payment_initiations WHERE "id" = {args.payment_initiation_id}""",
            )
            assert len(pain_df) == 1
            pain_row = pain_df.iloc[0]

        if pain_row is not None:
            pain_id = int(pain_row["id"])
            if ctx.parsed_args.ignore_accounting_entries_for_payment_initiation:
                accounting_entry_exclude_payment_initiation_id = pain_id
            else:
                accounting_entry_exclude_payment_initiation_id = None
            df = wsjrdp2027.load_payment_dataframe_from_payment_initiation(
                conn,
                payment_initiation_id=pain_id,
                pedantic=False,
                report_amount_differences=False,
                booking_at=ctx.start_time,
                allowed_pre_notification_status=allowed_pre_notification_status,
                accounting_entry_exclude_payment_initiation_id=accounting_entry_exclude_payment_initiation_id,
                excluded_ids=excluded_ids,
            )
            if args.collection_date is not None:
                df["collection_date"] = args.collection_date
        else:
            if args.collection_date is None:
                collection_date = DEFAULT_COLLECTION_DATE
            else:
                collection_date = args.collection_date
            df = wsjrdp2027.load_payment_dataframe(
                conn,
                query=wsjrdp2027.PeopleQuery(
                    collection_date=collection_date, now=ctx.start_time
                ),
                booking_at=ctx.start_time,
            )

        if ctx.parsed_args.end_to_end_id_suffix:
            df["sepa_dd_endtoend_id"] = df["sepa_dd_endtoend_id"].map(
                lambda s: s + ctx.parsed_args.end_to_end_id_suffix
            )

        sum_amount = df["open_amount_cents"].sum()

        for _, row in df.iterrows():
            if not isinstance(row["open_amount_cents"], (int, float)):
                err_msg = "Invalid row: 'open_amount_cents' value is not int or float"
                _LOGGER.error(
                    "%s:\n%s", err_msg, textwrap.indent(row.to_string(), "  | ")
                )
                raise RuntimeError(err_msg)

        _LOGGER.info(
            "SUM(open_amount_cents): %s", wsjrdp2027.format_cents_as_eur_de(sum_amount)
        )

        wsjrdp2027.write_accounting_dataframe_to_sepa_dd(
            df,
            xml_filename,
            config=wsjrdp2027.WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG,
            pedantic=True,
        )

        # ==================================================================================

        if sum_amount == 0:
            _LOGGER.warning("")
            _LOGGER.warning("No Direct Debit (sum_amount == 0)")
            _LOGGER.warning("")
        else:
            pain_message = wsjrdp2027.PainMessage.load(xml_filename)
            _LOGGER.info("Parsed %s", xml_filename)
            _write_mail_txt(mail_txt_filename, pain_message)
            _write_datev_csv(datev_csv_filename, pain_message=pain_message, df=df)
            _report_pain_message(pain_message)
            _report_df(df, pain_message=pain_message)
            wsjrdp2027.report_direct_debit_amount_differences(df, logger=_LOGGER)

            #
            # ==============================================================================

            ctx.require_approval_to_run_in_prod()

            # ==============================================================================
            #

            if args.accounting:
                wsjrdp2027.write_payment_dataframe_to_db(
                    conn, df, print_progress_message=ctx.print_progress_message
                )
                _upsert_payment_initiation_and_df_and_create_payment_infos(
                    conn, pain_id=pain_id, df=df, pain_message=pain_message
                )
            else:
                _LOGGER.info("")
                _LOGGER.info("SKIP ACCOUNTING (--no-accounting given)")
                _LOGGER.info("")

        # ==================================================================================

        wsjrdp2027.write_payment_dataframe_to_xlsx(df, xlsx_filename)
        if args.rollback_for_testing:
            print(flush=True)
            _LOGGER.warning("")
            _LOGGER.warning("ROLLBACK (--rollback-for-testing given)")
            conn.rollback()
            _LOGGER.warning("")
            print(flush=True)

    _LOGGER.info("finish writing output")

    if sum_amount == 0:
        _LOGGER.warning("")
        _LOGGER.warning("No Direct Debit (sum_amount == 0)")
        _LOGGER.warning("")
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  SEPA XML: %s", xml_filename)
    _LOGGER.info("  Excel: %s", xlsx_filename)
    _LOGGER.info("  DATEV CSV: %s", datev_csv_filename)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
