#!/usr/bin/env -S uv run
from __future__ import annotations

import pprint

import pandas as _pandas
import psycopg as _psycopg
import wsjrdp2027


_LOGGER = __import__("logging").getLogger(__name__)

WAITING_LIST_GROUPS = [5, 6, 7]
BMT_GROUPS = [45]


def load_people_dataframe_for_groupname(
    conn: _psycopg.Connection, groupname: str
) -> _pandas.DataFrame:
    primary_group_id = None
    exclude_primary_group_id = None
    match groupname:
        case "BMT":
            role = "IST"
            primary_group_id = [45]
        case "IST":
            role = "IST"
            primary_group_id = [4]
        case "CMT":
            role = "CMT"
            primary_group_id = [1]
        case "UL" | "CMT" | "YP":
            role = groupname
            exclude_primary_group_id = WAITING_LIST_GROUPS
        case _:
            raise RuntimeError(f"Unsupported {groupname=}")

    df = wsjrdp2027.load_people_dataframe(
        conn,
        where=wsjrdp2027.PeopleWhere(
            role=role,
            # id=484,
            exclude_deregistered=True,
            primary_group_id=primary_group_id,
            exclude_primary_group_id=exclude_primary_group_id,
        ),
        # where=wsjrdp2027.PeopleWhere(primary_group_id=1, exclude_deregistered=True),
        # where=wsjrdp2027.PeopleWhere(id=[21, 65, 413], exclude_deregistered=True),
        # where=wsjrdp2027.PeopleWhere(id=[482], exclude_deregistered=True),
        log_resulting_data_frame=False,
    )
    _LOGGER.info(
        f"Found {len(df)} not-deregistered contingent members in role {role=} in Hitobito"
    )
    return df


def update_additional_info(
    conn: _psycopg.Connection, updates: list[dict], *, console_confirm: bool = False
) -> None:
    if not updates:
        _LOGGER.info("No updates to people additional_info")
        return
    if console_confirm:
        _LOGGER.info(f"Updates to additional_info to apply:\n{pprint.pformat(updates)}")
        if not wsjrdp2027.console_confirm("Update additional_info in DB?"):
            _LOGGER.info("!! Skipped updates to additional_info")
            return

    updates_by_key: dict[str, list[dict]] = {}

    for upd in updates:
        p_id = upd["id"]
        for k, v in upd.items():
            if k != "id":
                updates_by_key.setdefault(k, []).append({"id": p_id, k: v})
    for key, values in updates_by_key.items():
        if key in ("wsjrdp_email", "moss_email"):
            _LOGGER.info(f"Update {key} for {len(values)} people")
            wsjrdp2027.pg.pg_update_people_additional_info_email(conn, key, values)
        elif key in ("keycloak_username",):
            _LOGGER.info(f"Update {key} for {len(values)} people")
            wsjrdp2027.pg.pg_update_people_additional_info_strings(conn, values)
        else:
            raise RuntimeError(f"Unsupported {key=} to update")


def sync(
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    groupname: str,
    keycloak_users: list[wsjrdp2027.keycloak.KeycloakUserDict],
) -> bool:
    errors = []
    email2row = {}

    email2keycloak_user = {
        user.get("email"): user
        for user in keycloak_users
        if user.get("enabled") and user.get("email")
    }
    username2keycloak_user = {
        user["username"]: user for user in keycloak_users if user.get("enabled")
    }

    additional_info_updates = []
    keycloak_updates = []

    df = load_people_dataframe_for_groupname(conn, groupname=groupname)

    for p in wsjrdp2027.iter_people_dataframe(df):
        email2row[p.wsjrdp_email] = p

        if not (keycloak_user := email2keycloak_user.get(p.wsjrdp_email)):
            if p["id"] == 1871:
                _LOGGER.debug(
                    f"{p.role_id_name}: Ignore missing keycloak user (Till Sanders)"
                )
            else:
                errors.append(
                    f"{p.role_id_name}: Could not find Keycloak user\n"
                    f"  wsjrdp_email_or_none={p.wsjrdp_email_or_none}\n"
                    f"  wsjrdp_email={p.wsjrdp_email}\n"
                    f"  additional_info={p.additional_info}"
                )
            continue

        keycloak_username = keycloak_user["username"]
        keycloak_attributes = keycloak_user.get("attributes", {})

        for k_key, p_key, k_val, p_val, fix_if_lower_equal in [
            (
                "mossEmail",
                "moss_email",
                keycloak_attributes.get("mossEmail", [None])[0],
                p.moss_email,
                True,
            ),
            (
                "hitobitoId",
                "id",
                keycloak_attributes.get("hitobitoId", [None])[0],
                str(p.id),
                False,
            ),
        ]:
            if p_val is None:
                errors.append(f"{p.role_id_name}: Missing {p_key}")
                continue
            elif k_val is None:
                _LOGGER.info(
                    f"{p.role_id_name}: Missing {k_key} in Keycloak, will set to {p_val}"
                )
                keycloak_updates.append(
                    {"username": keycloak_username, "attributes": {k_key: [str(p_val)]}}
                )
            elif k_val != p_val:
                if fix_if_lower_equal and (k_val.lower() == p_val):
                    _LOGGER.info(
                        f"{p.role_id_name}: Fix Keycloak {k_key}: {k_val!r} -> {p_key!r}"
                    )
                    keycloak_updates.append(
                        {
                            "username": keycloak_username,
                            "attributes": {k_key: [str(p_val)]},
                        }
                    )
                else:
                    errors.append(
                        f"{p.role_id_name}: Unexpected Keycloak {k_key}={k_val!r}"
                    )
                    continue

        for key, val in [
            ("keycloak_username", keycloak_username),
            ("wsjrdp_email", p.wsjrdp_email),
            ("moss_email", p.moss_email),
        ]:
            if key not in p.additional_info:
                additional_info_updates.append({"id": p.id, key: val})

    if errors:
        for err in errors:
            _LOGGER.error(err)
        return False

    update_additional_info(conn, additional_info_updates, console_confirm=True)
    ctx.keycloak.update_users(keycloak_updates, console_confirm=True)
    return True


def main():
    ctx = wsjrdp2027.WsjRdpContext(
        parse_arguments=False,
        __file__=__file__,
        # dry_run=True,
    )
    out_base = ctx.make_out_path("sync_cmt_keycloak__{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    with ctx.psycopg_connect() as conn:
        for groupname in ["IST"]:
            _LOGGER.info(f"Load keycloak {groupname=} users")
            keycloak_users = ctx.keycloak.get_users_in_group(groupname, enabled=True)
            _LOGGER.info(
                f"Found {len(keycloak_users)} enabled {groupname=} users in Keycloak"
            )
            if not sync(ctx, conn, groupname=groupname, keycloak_users=keycloak_users):
                return 1

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    __import__("sys").exit(main())
