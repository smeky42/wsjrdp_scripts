#!/usr/bin/env -S uv run
from __future__ import annotations

import logging
import sys

import wsjrdp2027


_LOGGER = logging.getLogger(__name__)


# Change value here if you want to actually change the database
ALWAYS_ROLLBACK = True


def main():
    ctx = wsjrdp2027.WsjRdpContext(__file__=__file__)

    with ctx.psycopg_connect() as conn:
        group_dicts = wsjrdp2027.pg_select_groups_dicts_for_where(conn, where=t"TRUE")
        group_id2group_dict = {
            group_dict["id"]: group_dict for group_dict in group_dicts
        }

        df = wsjrdp2027.load_people_dataframe(
            conn,
            where=wsjrdp2027.PeopleWhere(
                role="UL",
                exclude_deregistered=True,
                exclude_primary_group_id=[2, 5, 46],
                # status="confirmed",
                # exclude_id=[117, 1040],
            ),
        )
        df["username"] = df.apply(
            lambda row: wsjrdp2027.generate_mail_username(
                row["first_name"], row["last_name"]
            ),
            axis=1,
        )
        df["wsjrdp_email"] = df["username"].map(
            lambda name: f"{name}@units.worldscoutjamboree.de"
        )

        rows_with_errors = 0
        for _, row in df.iterrows():
            group_dict = group_id2group_dict[row["primary_group_id"]]
            group_description = group_dict["description"]
            _LOGGER.info(
                f"{row['payment_role'].short_role_name} {row['id_and_name']} :: "
                f"{row['first_name']} + {row['last_name']} -> {row['wsjrdp_email']}"
            )
            if row["wsjrdp_email"] not in group_description:
                rows_with_errors += 1
                _LOGGER.error(f"  Not in group description: {group_description!r}")
        query = """
UPDATE people
SET
   additional_info['wsjrdp_email'] = to_jsonb(%(wsjrdp_email)s::text),
   additional_info['wsjrdp_email_created_at'] = to_jsonb('2026-01-07 16:30:00'::text)
WHERE id = %(id)s
""".strip()
        values = [
            {"id": row["id"], "wsjrdp_email": row["wsjrdp_email"]}
            for _, row in df.iterrows()
        ]
        _LOGGER.info("query: %s", query)
        _LOGGER.info("%s rows to be updated", len(values))

        print(flush=True)
        if rows_with_errors:
            _LOGGER.error("Exit due to earlier errors")
            raise SystemExit(1)
        else:
            _LOGGER.info(
                "All wsjrdp_email addresses consistent with group descriptions"
            )
            print(flush=True)

        ctx.require_approval_to_run_in_prod("Update (overwrite) people additional_info")

        with conn.cursor() as cur:
            cur.executemany(query, values)

        if ALWAYS_ROLLBACK:
            _LOGGER.warning("Will ROLLBACK as this was a one-shot script")
            _LOGGER.warning(
                "Change value of ALWAYS_ROLLBACK variable to change this behavior"
            )
            conn.rollback()
            _LOGGER.warning("Rollback finished")
        else:
            _LOGGER.info("COMMIT")
            conn.commit()
            _LOGGER.info("Update finished")


if __name__ == "__main__":
    sys.exit(main())
