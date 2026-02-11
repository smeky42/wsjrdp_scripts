#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import re
import sys

import pandas as _pandas
import psycopg as _psycopg
import wsjrdp2027
import wsjrdp2027.keycloak
import wsjrdp2027.mailbox


_LOGGER = logging.getLogger(__name__)


def create_argument_parser():
    import argparse

    from wsjrdp2027 import to_date_or_none

    p = argparse.ArgumentParser()
    p.add_argument(
        "--tag",
        "-t",
        dest="tags",
        action="append",
        default=[],
        help="Add tag, can be specified multiple times",
    )
    p.add_argument(
        "--limit",
        type=int,
        help="Limitiert die Anzahl der Personen, die berücksichtigt werden (für Testzwecke).",
    )
    p.add_argument("--skip-email", action="store_true", default=None)
    p.add_argument("--skip-db-updates", action="store_true", default=None)
    p.add_argument("--zip-eml", action="store_true", default=None)
    p.add_argument("--no-zip-eml", dest="zip_eml", action="store_false")
    p.add_argument(
        "--collection-date",
        type=to_date_or_none,
        default=None,
        help="Collection date of the next SEPA direct debit. "
        "Computes SEPA direct debit information if set. "
        "Setting the collection date does not imply writing of payment information.",
    )
    p.add_argument("yaml_file")
    return p

def update_and_mail(
    batch_config,
    ctx: wsjrdp2027.WsjRdpContext,
    df: _pandas.DataFrame,
):
    prepared_batch = ctx.load_people_and_prepare_batch(batch_config, df_cb=lambda _: df)
    ctx.update_db_and_send_mailing(prepared_batch, zip_eml=ctx.parsed_args.zip_eml)


def onboarding_and_mail(conn, pdf: _pandas.DataFrame, ctx: wsjrdp2027.WsjRdpContext):
    batch_config = wsjrdp2027.BatchConfig.from_yaml(
        ctx.parsed_args.yaml_file
    )

    filtered = pdf[pdf["primary_group_id"].isin([45])] # BMT

    try:
        # create_accounts(ctx, filtered)

        update_and_mail(
                batch_config=batch_config,
                ctx=ctx,
                df=filtered,
            )

    except Exception as e:
        _LOGGER.error(
            "Error creating accounts: %s", e
            )

def create_accounts(ctx: wsjrdp2027.WsjRdpContext, df: _pandas.DataFrame):
    for _, row in df.iterrows():
        username = row["username"]
        password = row["password"]
        first_name = row["first_name"]
        last_name = row["last_name"]
        role = row["payment_role"]
        email_alias = row["email_alias"]
        private_email = row["email"]
        moss_email = str("wsj2027" + str(row["id"]) + "@worldscoutjamboree.de")

        _LOGGER.info(
            "Creating account for %s %s (%s %s) - %s",
            first_name,
            last_name,
            username,
            password,
            email_alias,
        )
        wsjrdp2027.keycloak.add_user(
            ctx,
            email=email_alias,
            first_name=first_name,
            last_name=last_name,
            password=password,
            attributes={"mossEmail": moss_email},
        )
        wsjrdp2027.keycloak.add_user_to_group(ctx, username=email_alias, group_name=role)
        wsjrdp2027.mailbox.add_alias(ctx, email=email_alias, goto=private_email)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        out_dir="data/move_to_units{{ kind | omit_unless_prod | upper | to_ext }}",
    )

    out_base = ctx.make_out_path("onboarding__{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    with ctx.psycopg_connect() as conn:
        pdf = wsjrdp2027.load_people_dataframe(
            conn,
            query=wsjrdp2027.PeopleQuery(
                where=wsjrdp2027.PeopleWhere(exclude_deregistered=True)
            ),
        )
        _LOGGER.info("Found %s people", len(pdf))
        # _LOGGER.info("People preview:\n%s", pdf.head().to_string())

        pdf["skip_db_updates"] = False
        pdf["username"] = pdf.apply(
            lambda r: wsjrdp2027._util.generate_mail_username(
                str(r["first_name"]), str(r["last_name"])
            ),
            axis=1,
        )

        pdf["email_alias"] = pdf.apply(
            lambda r: str(r["username"] + "@bmt.worldscoutjamboree.de").lower(),
            axis=1,
        )

        pdf["password"] = pdf.apply(
            lambda r: wsjrdp2027._util.generate_password(), axis=1
        )



        wsjrdp2027.keycloak.add_user(
            ctx,
            email="testbmt@bmt.worldscoutjamboree.de",
            first_name="Test",
            last_name="BMT",
            password=wsjrdp2027._util.generate_password(),
            username="testbmt@bmt.worldscoutjamboree.de",
            enabled=True,
            # attributes=[{"name": "mossEmail", "value": "wsjTest-2000@worldscoutjamboree.de"}],
        )
        wsjrdp2027.keycloak.add_user_to_group(ctx, username="testbmt@bmt.worldscoutjamboree.de", group_name="BMT")
        wsjrdp2027.mailbox.add_alias(ctx, email="testbmt@bmt.worldscoutjamboree.de", goto="bmt@smeky.de")

        # onboarding_and_mail(conn, pdf, ctx)

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info(" Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
