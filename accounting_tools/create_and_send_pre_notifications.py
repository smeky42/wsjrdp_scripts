#!/usr/bin/env -S uv run
from __future__ import annotations

import datetime
import logging
import pathlib as _pathlib
import sys
import textwrap
import typing as _typing

import pandas as pd
import wsjrdp2027
from wsjrdp2027._people_query import PeopleQuery


if _typing.TYPE_CHECKING:
    import psycopg as _psycopg


_SELFDIR = _pathlib.Path(__file__).parent.resolve()


_LOGGER = logging.getLogger(__name__)


COLLECTION_DATE = datetime.date(2025, 12, 5)


def _create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--skip-email", action="store_true", default=None)
    p.add_argument("--skip-db-updates", action="store_true", default=None)
    p.add_argument("--accounting", dest="skip_db_updates", action="store_false")
    p.add_argument("--no-accounting", dest="skip_db_updates", action="store_true")
    p.add_argument("--zip-eml", action="store_true", default=None)
    p.add_argument("--no-zip-eml", dest="zip_eml", action="store_false")
    p.add_argument(
        "--limit",
        type=int,
        help="Limitiert die Anzahl der Personen, die berücksichtigt werden (für Testzwecke).",
    )
    p.add_argument(
        "--tag",
        "-t",
        dest="tags",
        action="append",
        default=[],
        help="Add tag, can be specified multiple times",
    )
    p.add_argument(
        "--collection-date",
        metavar="DATE",
        default=COLLECTION_DATE.strftime("%Y-%m-%d"),
        help="The collection date",
    )
    p.add_argument("--query", default=None, help="YAML oder JSON query")
    return p


def _update_batch_config_from_ctx(
    config: wsjrdp2027.MailingConfig,
    ctx: wsjrdp2027.WsjRdpContext,
) -> wsjrdp2027.MailingConfig:
    if ctx.parsed_args.dry_run is not None:
        _LOGGER.debug("set dry_run = %s (from cli args)", ctx.parsed_args.dry_run)
    if ctx.parsed_args.skip_email is not None:
        _LOGGER.debug("set skip_email = %s (from cli args)", ctx.parsed_args.skip_email)
    if ctx.parsed_args.skip_db_updates is not None:
        _LOGGER.debug(
            "set skip_db_updates = %s (from cli args)", ctx.parsed_args.skip_db_updates
        )
    config = config.replace(
        dry_run=ctx.dry_run,
        skip_email=ctx.parsed_args.skip_email,
        skip_db_updates=ctx.parsed_args.skip_db_updates,
    )
    args = ctx.parsed_args
    if (query_str := getattr(args, "query")) is not None:
        query = _load_query_from_string(query_str)
        _LOGGER.info("set query = %s (from cli_args)", str(query))
        config.query = query
    else:
        query = config.query
    query.now = ctx.start_time
    if (limit := args.limit) is not None:
        _LOGGER.debug("set limit = %s (from cli args)", limit)
        query.limit = limit
    if (collection_date := args.collection_date) is not None:
        _LOGGER.debug("set collection_date = %s (from cli args)", collection_date)
        query.collection_date = wsjrdp2027.to_date(collection_date)
    if (dry_run := ctx.dry_run) is not None:
        _LOGGER.debug("set dry_run = %s (from cli args)", dry_run)
        config.dry_run = dry_run
    if tags := getattr(args, "tags"):
        _LOGGER.debug("add to add_tags = %s (from cli args)", tags)
        config.updates.setdefault("add_tags", []).extend(tags)
    return config


def _load_query_from_string(query_str: str) -> wsjrdp2027.PeopleQuery:
    import io

    import yaml

    f = io.StringIO(query_str)
    d = yaml.load(f, Loader=yaml.FullLoader)
    return wsjrdp2027.PeopleQuery(**d)


def _insert_pre_notifications_into_db(
    *,
    conn: _psycopg.Connection,
    df: pd.DataFrame,
    sepa_dd_config: wsjrdp2027.SepaDirectDebitConfig,
    dry_run: bool | None = None,
    skip_db_updates: bool | None = None,
) -> int | None:
    skip_reasons = []
    if dry_run:
        skip_reasons.append("dry_run is True")
    if skip_db_updates:
        skip_reasons.append("skip_db_updates is true")
    if skip_reasons:
        _LOGGER.info("Skip DB updates (%s)", ", ".join(skip_reasons))
        return None
    _LOGGER.info("Update DB - INSERT pre notifications")

    with conn.cursor() as cur:
        pain_id = wsjrdp2027.insert_payment_initiation(
            cursor=cur,
            sepa_dd_config=sepa_dd_config,
        )
        _LOGGER.info("payment initiation id: %s", pain_id)
        pymnt_inf_id = wsjrdp2027.insert_direct_debit_payment_info(
            cur,
            payment_initiation_id=pain_id,
            sepa_dd_config=sepa_dd_config,
        )
        _LOGGER.info("direct debit payment info id: %s", pymnt_inf_id)

        for _, row in df.iterrows():
            wsjrdp2027.insert_direct_debit_pre_notification_from_row(
                cur,
                row=row,
                payment_initiation_id=pain_id,
                direct_debit_payment_info_id=pymnt_inf_id,
                creditor_id=wsjrdp2027.CREDITOR_ID,
            )
    return pain_id


def handle_df(df: pd.DataFrame) -> pd.DataFrame:
    _LOGGER.info("")
    _LOGGER.info("==== Overall payments: %s", len(df))
    _LOGGER.info("")
    df_ok = df[df["payment_status"] == "ok"].copy()
    df_not_ok = df[df["payment_status"] != "ok"].copy()
    if len(df_not_ok):
        _LOGGER.info("")
        _LOGGER.info("==== Skipped payments (payment_status != 'ok')")
        _LOGGER.info("  Number of skipped payments: %s", len(df_not_ok))
        _LOGGER.info(
            "  Skipped payments DataFrame (payment_status != 'ok'):\n%s",
            textwrap.indent(str(df_not_ok), "  | "),
        )
        for _, row in df_not_ok.iterrows():
            _LOGGER.debug(
                "    %5d %s / %s / %s",
                row["id"],
                row["short_full_name"],
                row["payment_status"],
                row["payment_status_reason"],
            )
        sum_not_ok = int(df_not_ok["open_amount_cents"].sum())
        _LOGGER.info(
            "  NOT OK payments: SUM(open_amount_cents): %s",
            wsjrdp2027.format_cents_as_eur_de(sum_not_ok),
        )
    else:
        sum_not_ok = 0

    _LOGGER.info("")
    _LOGGER.info("==== Payments (payment_status == 'ok')")
    _LOGGER.info("  Number of payments: %s", len(df_ok))
    _LOGGER.info(
        "  Payments DataFrame (payment_status == 'ok'):\n%s",
        textwrap.indent(str(df_ok), "  | "),
    )

    sum_ok = int(df_ok["open_amount_cents"].sum())
    _LOGGER.info(
        "  OK payments: SUM(open_amount_cents): %s",
        wsjrdp2027.format_cents_as_eur_de(sum_ok),
    )
    _LOGGER.info("")

    if sum_not_ok > 0:
        _LOGGER.error("Would skip amounts. Exit")
        raise SystemExit(1)
    if sum_ok == 0:
        _LOGGER.warning("No amount to transfer. Exit")
        raise SystemExit(0)

    payment_finished_ids = (
        frozenset(wsjrdp2027.EARLY_PAYER_AUGUST_IDS_SUPERSET)
        - frozenset([623, 671])  # fehlgeschalgene August Einzüge
        - frozenset([204, 208])  # Auf Ratenzahlung umgestellt
    )

    ids = df_ok["id"].tolist()
    overlapping_ids = frozenset(ids) & payment_finished_ids
    if overlapping_ids:
        df_overlap = df_ok[df["id"].isin(overlapping_ids)]
        _LOGGER.error("")
        _LOGGER.error(
            "Found %s overlapping id's: %s",
            len(overlapping_ids),
            sorted(overlapping_ids),
        )
        _LOGGER.error("df_overlap:\n%s", str(df_overlap))
        for _, row in df_overlap.iterrows():
            _LOGGER.error(
                "id: %s, full_name: %s, row:\n%s",
                row["id"],
                row["full_name"],
                textwrap.indent(row.to_string(), "  | "),
            )
        raise SystemExit(1)

    return df_ok


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=_create_argument_parser(),
        argv=argv,
        out_dir="data/sepa_direct_debit_pre_notifications_{{ filename_suffix }}",
    )
    sepa_dd_config: wsjrdp2027.SepaDirectDebitConfig = (  # type: ignore
        wsjrdp2027.WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG
    )
    batch_config = wsjrdp2027.MailingConfig.from_yaml(
        _SELFDIR / "create_and_send_pre_notifications.yml"
    )
    batch_config = _update_batch_config_from_ctx(batch_config, ctx)
    out_base = ctx.make_out_path(batch_config.name)
    log_filename = out_base.with_suffix(".log")
    xml_filename = out_base.with_suffix(".xml")
    ctx.configure_log_file(log_filename)

    assert batch_config.query.collection_date is not None

    prepared_batch = ctx.load_people_and_prepare_mailing(batch_config, df_cb=handle_df)
    prepared_batch.write_data(zip_eml=ctx.parsed_args.zip_eml)
    df_ok = prepared_batch.df
    wsjrdp2027.write_accounting_dataframe_to_sepa_dd(
        df_ok, path=xml_filename, config=sepa_dd_config, pedantic=True
    )
    with ctx.psycopg_connect() as conn:
        ctx.update_db_for_dataframe(
            df_ok,
            conn=conn,
            now=prepared_batch.now,
            skip_db_updates=prepared_batch.skip_db_updates,
        )
        maybe_pain_id = _insert_pre_notifications_into_db(
            conn=conn,
            df=df_ok,
            sepa_dd_config=sepa_dd_config,
            dry_run=prepared_batch.dry_run,
            skip_db_updates=prepared_batch.skip_db_updates,
        )

    with ctx.mail_login(from_addr=prepared_batch.from_addr) as mail_client:
        prepared_batch.send(mail_client)

    # ======================================================================

    _LOGGER.info("")
    _LOGGER.info(
        "SUM(open_amount_cents): %s",
        wsjrdp2027.format_cents_as_eur_de(df_ok["open_amount_cents"].sum()),
    )
    if maybe_pain_id is None:
        _LOGGER.info("No payment initiation written")
    else:
        _LOGGER.info("Wrote payment initiation with id=%s", maybe_pain_id)
    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  SEPA XML: %s", xml_filename)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
