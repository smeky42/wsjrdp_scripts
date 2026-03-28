#!/usr/bin/env -S uv run
from __future__ import annotations

import itertools
import logging as _logging
import pprint

import pandas as _pandas
import psycopg as _psycopg
import wsjrdp2027


_LOGGER = _logging.getLogger(__name__)

WAITING_LIST_GROUPS = [5, 6, 7]
BMT_GROUPS = [45]
EXT_GROUPS = [48]


GROUPNAMES_TO_SYNC = ["CMT", "UL", "IST", "BMT", "EXT"]
GROUPNAMES_TO_SYNC = ["BMT"]


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
        case "EXT":
            role = None
            primary_group_id = EXT_GROUPS
        case _:
            raise RuntimeError(f"Unsupported {groupname=}")

    df = wsjrdp2027.load_people_dataframe(
        conn,
        where=wsjrdp2027.PeopleWhere(
            role=role,
            # tag="Finanzverantwortlich",
            # role=role,
            # id=[484,486],
            # id=2480,
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
        elif key in (
            "keycloak_initial_password",
            "keycloak_username",
            "moss_email_goto",
            "moss_email_is_mailbox",
            "wsjrdp_email_goto",
            "wsjrdp_email_is_mailbox",
        ):
            _LOGGER.info(f"Update {key} for {len(values)} people")
            wsjrdp2027.pg.pg_update_people_additional_info(conn, values)
        else:
            raise RuntimeError(f"Unsupported {key=} to update")


def _check_for_keycloak_user(
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    person: wsjrdp2027.Person,
    groupname: str,
    email2keycloak_user: dict[str | None, dict],
    keycloak_users: list[wsjrdp2027.keycloak.KeycloakUserDict],
    errors: list[str],
    additional_info_updates: list[dict],
    address2goto: dict[str, list[str]],
) -> bool:
    def _update_alias(email, add_goto):
        wsjrdp2027.mailbox.update_alias(ctx, email=email, add_goto=add_goto)
        goto_list = address2goto.setdefault(email, [])
        if add_goto not in goto_list:
            goto_list.append(add_goto)

    if email2keycloak_user.get(person.wsjrdp_email):
        return True

    password = wsjrdp2027.generate_password()
    keycloak_username = person.wsjrdp_email
    keycloak_email = person.wsjrdp_email

    if person.id == 1871:
        _LOGGER.debug(
            f"{person.role_id_name}: Ignore missing keycloak user (Till Sanders)"
        )
        return False
    elif (
        keycloak_username
        and keycloak_email
        and person.moss_email
        and not person.wsjrdp_email_should_be_mailbox
        and wsjrdp2027.console_confirm(
            f"No keycloak user for E-Mail {keycloak_email}\n"
            f"  username: {keycloak_username}\n"
            f"  password: {password}\n"
            f"  email: {keycloak_email}\n"
            f"  first_name: {person.first_name}\n"
            f"  last_name: {person.last_name}\n"
            f"  attributes:\n"
            f"    mossEmail: {person.moss_email}\n"
            f"    hitobitoId: {person.id}\n"
            f"Create missing Keycloak user {keycloak_username} for group {groupname}?"
        )
    ):
        if person.wsjrdp_email_should_be_mailbox:
            raise RuntimeError("Cannot setup mailbox - not supported yet")
        else:
            _update_alias(email=keycloak_email, add_goto=person.email)
            _update_alias(email=person.moss_email, add_goto=person.email)

        wsjrdp2027.keycloak.add_user(
            ctx,
            email=keycloak_email,
            username=keycloak_username,
            first_name=person.first_name,
            last_name=person.last_name,
            password=password,
            attributes={
                "mossEmail": [person.moss_email],
                "hitobitoId": [str(person.id)],
            },
        )
        wsjrdp2027.keycloak.add_user_to_group(
            ctx, username=keycloak_username, group_name=groupname
        )
        additional_info_updates.append(
            {"id": person.id, "keycloak_initial_password": password}
        )
        return True
    else:
        errors.append(
            f"{person.role_id_name}: Could not find Keycloak user\n"
            f"  wsjrdp_email_or_none={person.wsjrdp_email_or_none}\n"
            f"  wsjrdp_email={person.wsjrdp_email}\n"
            f"  moss_email={person.moss_email}\n"
            f"  additional_info={person.additional_info}"
        )
        return False


def _load_address2goto(ctx: wsjrdp2027.WsjRdpContext) -> dict[str, list[str]]:
    _LOGGER.debug("Load mailcow aliases to fill address2goto")
    aliases = wsjrdp2027.mailbox.get_aliases(ctx)
    result = {alias["address"]: alias["goto"].split(",") for alias in aliases}
    _LOGGER.info(f"Loaded {len(result)} aliases from mailcow")
    return result


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

    address2goto = _load_address2goto(ctx)

    for p in wsjrdp2027.iter_people_dataframe(df):
        email2row[p.wsjrdp_email] = p

        if not _check_for_keycloak_user(
            ctx,
            conn,
            person=p,
            groupname=groupname,
            keycloak_users=keycloak_users,
            errors=errors,
            email2keycloak_user=email2keycloak_user,
            additional_info_updates=additional_info_updates,
            address2goto=address2goto,
        ):
            continue

        keycloak_user = email2keycloak_user[p.wsjrdp_email]
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
            ("wsjrdp_email_is_mailbox", p.wsjrdp_email_should_be_mailbox),
        ]:
            if key not in p.additional_info and val is not None:
                additional_info_updates.append({"id": p.id, key: val})
        for key, email in [
            ("moss_email_goto", p.moss_email),
            ("wsjrdp_email_goto", p.wsjrdp_email),
        ]:
            _LOGGER.debug(f"{p.role_id_name} Check {key=} for {email=}")
            if email is None:
                continue
            old_goto_list = []
            goto_list = address2goto.get(email, [])
            while goto_list != old_goto_list:
                old_goto_list, goto_list = (
                    goto_list,
                    list(
                        itertools.chain.from_iterable(
                            address2goto.get(a, [a]) for a in goto_list
                        )
                    ),
                )
            _LOGGER.debug(f"{p.role_id_name} {goto_list=}")
            _LOGGER.debug(f"{p.role_id_name} {p.additional_info.get(key)=}")
            if goto_list and goto_list != p.additional_info.get(key):
                additional_info_updates.append({"id": p.id, key: goto_list or None})

    if errors:
        for err in errors:
            _LOGGER.error(err)
        return False

    update_additional_info(conn, additional_info_updates, console_confirm=True)
    ctx.keycloak.update_users(keycloak_updates, console_confirm=True)
    return True


def _load_keycloak_users_in_groups(ctx: wsjrdp2027.WsjRdpContext, groupnames):
    all_users = []
    for groupname in groupnames:
        _LOGGER.info(f"Load keycloak {groupname=} users")
        users = ctx.keycloak.get_users_in_group(groupname, enabled=True)
        _LOGGER.info(f"Found {len(users)} enabled {groupname=} users in Keycloak")
        all_users.extend(users)
    if len(groupnames) > 1:
        _LOGGER.info(f"Found {len(all_users)} enabled users in Keycloak")
    return all_users


def main():
    ctx = wsjrdp2027.WsjRdpContext(
        __file__=__file__,
        # dry_run=True,
        # log_level=_logging.DEBUG,
    )
    out_base = ctx.make_out_path("sync_cmt_keycloak__{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    keycloak_users = _load_keycloak_users_in_groups(ctx, GROUPNAMES_TO_SYNC)

    with ctx.psycopg_connect() as conn:
        for groupname in GROUPNAMES_TO_SYNC:
            if not sync(ctx, conn, groupname=groupname, keycloak_users=keycloak_users):
                return 1

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    __import__("sys").exit(main())
