#!/usr/bin/env -S uv run
from __future__ import annotations

import pprint

import wsjrdp2027


_LOGGER = __import__("logging").getLogger(__name__)


_EXT_EHOC_USERNAMES = [
    "ihoefig",
    "cwindisch",
    "funger",
    "pneubauer",
    "dfritzsche",
    "cespeter",
    "eguldenfels",
    "kwistal",
    "mgriwatz",
    "klangenberg",
    "lriesner",
    "vreichenspurner",
    "hrieger",
]


def main():
    ctx = wsjrdp2027.WsjRdpContext(parse_arguments=False, __file__=__file__)
    out_base = ctx.make_out_path("sync_cmt_keycloak__{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    keycloak_users_cmt = wsjrdp2027.keycloak.get_users_in_group(ctx, "CMT")
    keycloak_users_cmt = [user for user in keycloak_users_cmt if user["enabled"]]

    email2keycloak_user = {
        user["email"]: user for user in keycloak_users_cmt if user["enabled"]
    }
    username2keycloak_user = {
        user["username"]: user for user in keycloak_users_cmt if user["enabled"]
    }
    _LOGGER.debug(f"email2keycloak_user = {pprint.pformat(email2keycloak_user)}")
    _LOGGER.info(f"Found {len(keycloak_users_cmt)} enabled CMT users in Keycloak")

    errors = []

    email2row = {}

    wsjrdp_email_values = []
    moss_email_values = []

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(
            conn,
            where=wsjrdp2027.PeopleWhere(primary_group_id=1, exclude_deregistered=True),
            log_resulting_data_frame=False,
        )
        _LOGGER.info(f"Found {len(df)} not-deregistered CMT members in Hitobito")

        for _, row in df.iterrows():
            id = row["id"]
            role_id_name = row["role_id_name"]
            additional_info: dict = row.get("additional_info") or {}
            wsjrdp_email = additional_info.get("wsjrdp_email")
            if not wsjrdp_email:
                username_prefix = wsjrdp2027.generate_mail_username(
                    row["first_name"], row["last_name"]
                )
                wsjrdp_email = f"{username_prefix}@worldscoutjamboree.de"
            email2row[wsjrdp_email] = row
            moss_email = additional_info.get("moss_email")

            keycloak_user = email2keycloak_user.get(wsjrdp_email)
            if not keycloak_user:
                if row["id"] == 1871:
                    _LOGGER.debug(
                        f"{role_id_name}: Ignore missing keycloak user (Till Sanders)"
                    )
                else:
                    errors.append(f"{role_id_name}: Could not find Keycloak user")
                continue
            moss_email = wsjrdp_email
            keycloak_moss_email = keycloak_user.get("attributes", {}).get(
                "mossEmail", [None]
            )[0]
            expected_moss_email = (moss_email or wsjrdp_email).lower()
            if keycloak_moss_email is None:
                _LOGGER.info(
                    f"{role_id_name}: Missing mossEmail, will set to {expected_moss_email}"
                )
                wsjrdp2027.keycloak.update_user(
                    ctx,
                    keycloak_user["username"],
                    {"attributes": {"mossEmail": [expected_moss_email]}},
                )
            elif keycloak_moss_email != expected_moss_email:
                if keycloak_moss_email.lower() == expected_moss_email:
                    _LOGGER.info(
                        f"{role_id_name}: Fix mossEmail {keycloak_moss_email!r} -> {expected_moss_email!r}"
                    )
                    wsjrdp2027.keycloak.update_user(
                        ctx,
                        keycloak_user["username"],
                        {"attributes": {"mossEmail": [expected_moss_email]}},
                    )
                else:
                    errors.append(
                        f"{role_id_name}: Unexpected mossEmail={keycloak_moss_email!r}"
                    )
                    continue
            if "wsjrdp_email" not in additional_info:
                wsjrdp_email_values.append(
                    {"id": row["id"], "wsjrdp_email": wsjrdp_email}
                )
            if "moss_email" not in additional_info:
                moss_email_values.append(
                    {"id": row["id"], "moss_email": moss_email or wsjrdp_email}
                )

        if wsjrdp_email_values:
            _LOGGER.info(f"Update wsjrdp_email for {len(wsjrdp_email_values)} people")
            query = """UPDATE people
SET
   additional_info['wsjrdp_email'] = to_jsonb(%(wsjrdp_email)s::text),
   additional_info['wsjrdp_email_created_at'] = to_jsonb(NOW()::text),
   additional_info['wsjrdp_email_updated_at'] = to_jsonb(NOW()::text)
WHERE id = %(id)s"""
            with conn.cursor() as cur:
                cur.executemany(query, wsjrdp_email_values)
        else:
            _LOGGER.info("No need to update wsjrdp_email for CMT people")

        if moss_email_values:
            _LOGGER.info(f"Update moss_email for {len(moss_email_values)} people")
            query = """UPDATE people
SET
   additional_info['moss_email'] = to_jsonb(%(moss_email)s::text),
   additional_info['moss_email_created_at'] = to_jsonb(NOW()::text),
   additional_info['moss_email_updated_at'] = to_jsonb(NOW()::text)
WHERE id = %(id)s"""
            with conn.cursor() as cur:
                cur.executemany(query, moss_email_values)
        else:
            _LOGGER.info("No need to update wsjrdp_email for CMT people")

    for email, user in email2keycloak_user.items():
        keycloak_moss_email = user.get("attributes", {}).get("mossEmail", [None])[0]
        assert email == user["email"]
        if email not in email2row:
            if user["username"] in ("umanagement", "bpowell"):
                _LOGGER.info(
                    f"{email}: Ignore, not expected to have associated Hitobito account"
                )
            elif user["username"] in ["aothmer", "lriesner", "mdenzer", "vvorholz"]:
                if keycloak_moss_email == email:
                    _LOGGER.info(
                        f"{email}: No Hitobito account yet, mossEmail == email == {email}"
                    )
                elif keycloak_moss_email is None:
                    _LOGGER.info(
                        f"{email}: No Hitobito account yet, set mossEmail to {email}"
                    )
                    wsjrdp2027.keycloak.update_user(
                        ctx,
                        user["username"],
                        {"attributes": {"mossEmail": [user["email"]]}},
                    )
                else:
                    errors.append(
                        f"Unexpected mossEmail == {keycloak_moss_email!r} where email = {email!r}"
                    )
            else:
                errors.append(
                    f"No Hitobito user for Keycloak user {user['username']}, email={user['email']}"
                )

    if errors:
        for err in errors:
            _LOGGER.error(err)
        return 1

    _LOGGER.info("")
    _LOGGER.info(
        f"Moss E-Mail Adresses for eHOC+ ({len(_EXT_EHOC_USERNAMES)} members in list)"
    )
    for username in _EXT_EHOC_USERNAMES:
        user = username2keycloak_user[username]
        print(user["email"])
    print()

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    __import__("sys").exit(main())
