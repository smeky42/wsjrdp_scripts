#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import sys
import pandas as _pandas
import re

import wsjrdp2027

_LOGGER = logging.getLogger(__name__)

def insert_mailing_list(gid: int, mail_name: str, name: str, description: str, additional_sender: str, anyone_may_post: bool, delivery_report: bool, main_email: bool, con):
    insert_mailing_list_sql = """
    INSERT INTO mailing_lists
    ("name", group_id, description, publisher, mail_name, additional_sender,
    subscribers_may_post, anyone_may_post, preferred_labels, delivery_report,
    main_email, mailchimp_api_key, mailchimp_list_id, mailchimp_syncing,
    mailchimp_last_synced_at, mailchimp_result, mailchimp_include_additional_emails,
    filter_chain, subscribable_for, subscribable_mode, mailchimp_forgotten_emails)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    """
    cur = con.cursor()
    cur.execute(insert_mailing_list_sql, (
        name,                       # name
        gid,                        # group_id
        description,                # description
        "CMT - IT",                 # publisher
        mail_name.lower(),          # mail_name
        additional_sender,          # additional_sender
        False,                      # subscribers_may_post
        anyone_may_post,            # anyone_may_post
        None,                       # preferred_labels
        delivery_report,            # delivery_report
        main_email,                 # main_email
        "",                         # mailchimp_api_key
        "",                         # mailchimp_list_id
        False,                      # mailchimp_syncing
        None,                       # mailchimp_last_synced_at
        "{}" ,                      # mailchimp_result (pass dict for json/jsonb column)
        False,                      # mailchimp_include_additional_emails
        '--- {}\n',                 # filter_chain
        'nobody',                   # subscribable_for
        None,                       # subscribable_mode
        None                        # mailchimp_forgotten_emails
    ))
    new_id = cur.fetchone()[0]
    con.commit()
    cur.close()

    _LOGGER.info("Inserted mailing list %s for unit %s (group id %s) with db_id %s and description %s", mail_name, name, gid, new_id, description)
    _LOGGER.info("  Additional sender: %s", additional_sender)

    return new_id

def subscribe_to_mailing_list_sql(mailing_list_id: int, subscriber_id: int, con):
    subscribe_sql = """
    INSERT INTO subscriptions
    (mailing_list_id, subscriber_type, subscriber_id)
    VALUES (%s, %s, %s)
    RETURNING id
    """
    cur = con.cursor()
    cur.execute(subscribe_sql, (
        mailing_list_id,
        "Group",
        subscriber_id
    ))
    new_id = cur.fetchone()[0]
    con.commit()
    cur.close()

    _LOGGER.info("Subscribed group id %s to mailing list id %s in db_id %s", subscriber_id, mailing_list_id, new_id)
    return new_id

def add_related_role_types_sql(relation_id: int, role_type: str, con):
    subscribe_sql = """
    INSERT INTO related_role_types
    (relation_id, role_type, relation_type)
    VALUES (%s, %s, %s)
    RETURNING id
    """
    cur = con.cursor()
    cur.execute(subscribe_sql, (
        relation_id,
        role_type,
        'Subscription'
    ))
    new_id = cur.fetchone()[0]
    con.commit()
    cur.close()

    _LOGGER.info("Add related role type %s to relation id %s in db_id %s", role_type, relation_id, new_id)
    return new_id   

def main():
    ctx = wsjrdp2027.WsjRdpContext(
        out_dir="data/units_mailing_list{{ kind | omit_unless_prod | upper | to_ext }}",
    )

    out_base = ctx.make_out_path("units_mailing_list__{{ filename_suffix }}")
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
        

        uldf = pdf[pdf["payment_role"].fillna("").astype(str).str.endswith("UL")]
        uldf['username'] = uldf.apply(lambda r: wsjrdp2027._util.generate_mail_username(str(r["first_name"]), str(r["last_name"])), axis=1)

        groups_sql = """
                SELECT id AS group_id, name, short_name, description
                FROM groups ORDER BY name ASC
            """
        gdf = _pandas.read_sql(groups_sql, conn)
        _LOGGER.info("Found %s groups", len(gdf))
        # _LOGGER.info("Groups preview:\n%s", gdf.head().to_string())

        for _, row in gdf.iterrows():
            if len(row['name']) == 2:
                _LOGGER.info("Group %s: %s", row['group_id'], row['name'])

                name = f"Unit {row['name']} - Alle"
                description = f"Schreibe an alle Mitglieder (UL und YP) der Unit {row['name']}"
                mail_name = f"{row['name'].lower()}"
                additional_sender = f"*@worldscoutjamboree.de"
                for _, urow in uldf.iterrows():
                    if urow['primary_group_id'] == row['group_id']:
                        additional_sender += f", {urow['username']}@units.worldscoutjamboree.de"

                mailing_list_id = insert_mailing_list(
                    gid=row['group_id'],
                    mail_name=mail_name,
                    name=name,
                    description=description,
                    additional_sender=additional_sender,
                    anyone_may_post=False,
                    delivery_report=True,
                    main_email=False,
                    con=conn)
                
                subcribe_id = subscribe_to_mailing_list_sql(mailing_list_id=mailing_list_id, subscriber_id=row['group_id'], con=conn)
                add_related_role_types_sql(relation_id=subcribe_id, role_type='Group::Unit::Leader', con=conn)
                add_related_role_types_sql(relation_id=subcribe_id, role_type='Group::Unit::Member', con=conn)
                
                name = f"Unit {row['name']} - Unit Leader"
                description = f"Schreibe an Unit Leader der Unit {row['name']}"
                mail_name = f"ul-{row['name'].lower()}"
                additional_sender = f"*@worldscoutjamboree.de"

                mailing_list_id =insert_mailing_list(
                    gid=row['group_id'],
                    mail_name=mail_name,
                    name=name,
                    description=description,
                    additional_sender=additional_sender,
                    anyone_may_post=True,
                    delivery_report=False,
                    main_email=True,
                    con=conn)
                subcribe_id = subscribe_to_mailing_list_sql(mailing_list_id=mailing_list_id, subscriber_id=row['group_id'], con=conn)
                add_related_role_types_sql(relation_id=subcribe_id, role_type='Group::Unit::Leader', con=conn)



    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    sys.exit(main())
