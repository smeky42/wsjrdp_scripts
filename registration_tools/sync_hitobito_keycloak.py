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


KEYCLOAK_GROUPNAMES_TO_FETCH = ["CMT", "UL", "IST", "BMT", "EXT"]


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

    where = wsjrdp2027.PeopleWhere(
        role=role,
        # tag="Finanzverantwortlich",
        # role=role,
        # id=[484,486],
        # id=2480,
        exclude_deregistered=True,
        primary_group_id=primary_group_id,
        exclude_primary_group_id=exclude_primary_group_id,
    )
    df = wsjrdp2027.load_people_dataframe(
        conn,
        query=wsjrdp2027.PeopleQuery(
            where=where,
            limit=None,
            offset=None,
        ),
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
    *,
    errors: list[str],
    additional_info_updates: list[dict],
) -> bool:
    if ctx.keycloak().get_user_for_person_or_none(
        person,
        allow_cached=True,
        check_role_consistency=True,
        additional_info_updates=additional_info_updates,
    ):
        return True

    keycloak_username = person.keycloak_username or person.wsjrdp_email
    keycloak_email = person.wsjrdp_email

    if (
        keycloak_username
        and keycloak_email
        and person.moss_email
        and person.wsjrdp_email
        and ctx.console_confirm(
            f"No keycloak user for E-Mail {keycloak_email}\n"
            f"  username: {keycloak_username}\n"
            f"  email: {keycloak_email}\n"
            f"  first_name: {person.first_name}\n"
            f"  last_name: {person.last_name}\n"
            f"  attributes:\n"
            f"    mossEmail: {person.moss_email}\n"
            f"    hitobitoId: {person.id}\n"
            f"Create missing Keycloak user {keycloak_username} for group {groupname}?",
            cache_key="create_missing_keycloak_user",
            cache_hint="create missing Keycloak user",
        )
    ):
        _create_keycloak_user(
            ctx,
            conn,
            person,
            groupname=groupname,
            keycloak_username=keycloak_username,
            keycloak_email=keycloak_email,
            errors=errors,
            additional_info_updates=additional_info_updates,
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


def _create_keycloak_user(
    ctx: wsjrdp2027.WsjRdpContext,
    conn: _psycopg.Connection,
    person: wsjrdp2027.Person,
    *,
    groupname: str,
    keycloak_username: str,
    keycloak_email: str,
    errors: list[str],
    additional_info_updates: list[dict],
) -> None:
    assert person.wsjrdp_email
    assert person.moss_email

    password = wsjrdp2027.generate_password()

    _LOGGER.info(
        f"Create keycloak user {keycloak_username} with email {keycloak_email}"
    )
    ctx.keycloak().create_user(
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
    ctx.keycloak().add_user_to_group(username=keycloak_username, groupname=groupname)
    additional_info_updates.append(
        {"id": person.id, "keycloak_initial_password": password}
    )
    person.set_additional_info("keycloak_initial_password", password)

    new_mailbox = False
    if person.wsjrdp_email_should_be_mailbox:
        mb = ctx.mailcow().get_mailbox_or_none_by_username(person.wsjrdp_email)
        if mb:
            _LOGGER.info(f"Found Mailcow mailbox for {person.wsjrdp_email}")
        elif ctx.console_confirm(
            f"Create Mailcow mailbox for {person.wsjrdp_email}?",
            cache_key="create_missing_mailcow_user",
            cache_hint="create missing mailcow user",
        ):
            ctx.mailcow().add_mailbox(
                person.wsjrdp_email,
                name=person.full_name,
                password=password,
                authsource="keycloak",
            )
            new_mailbox = True
        else:
            errors.append(
                f"{person.role_id_name}: missing mailcow mailbox for {person.wsjrdp_email}"
            )
    else:
        _LOGGER.info(f"Create/Update mailcow alias {keycloak_email} -> {person.email}")
        ctx.mailcow().add_alias(keycloak_email, add_goto=person.email)
        _LOGGER.info(
            f"Create/Update mailcow alias {person.moss_email} -> {person.email}"
        )
        ctx.mailcow().add_alias(person.moss_email, add_goto=person.email)

    if new_mailbox:
        content = _NEW_ACCOUNT_AND_MAILBOX_MAIL_CONTENT
        subject = "Dein Jamboree 2027 Mailaccount - {{ person.short_full_name }} (id {{person.id }})"
    else:
        content = _NEW_ACCOUNT_MAIL_CONTENT
        subject = "Dein Jamboree 2027 Account - {{ person.short_full_name }} (id {{person.id }})"

    batch_config = wsjrdp2027.BatchConfig(
        where=wsjrdp2027.PeopleWhere(id=[]),
        email_from=person.helpdesk_email,
        email_subject=subject,
        extra_email_bcc="david.fritzsche@worldscoutjamboree.de",
        from_addr="anmeldung@worldscoutjamboree.de",
        signature=wsjrdp2027.EMAIL_SIGNATURE_CMT,
        content=content,
    )
    prepared = batch_config.prepare(
        person, dry_run=ctx.dry_run, skip_email=ctx.skip_email, skip_db_updates=True
    )
    ctx.send_mailing(prepared, zip_eml=False)
    ctx.load_people_and_prepare_batch(batch_config)


_NEW_ACCOUNT_AND_MAILBOX_MAIL_CONTENT = """
Hallo {{ p.greeting_name }},

wir haben dir eine Mailadresse eingerichtet:


https://mail.worldscoutjamboree.de
Mailadresse: {{ p.wsjrdp_email }}
Passwort: {{ p.additional_info.keycloak_initial_password }}


Bitte logge dich, über Single Sign-On, dort ein.
Wir wünschen uns, dass du diese Mailadresse aktiv nutzt, um per E-Mail zu Jamboree-Angelegenheiten zu kommunizieren.
So sehen die YPs und das CMT, dass du zum German Contingent gehörst.

Falls es technische Probleme oder Fragen gibt schreib uns gerne an helpdesk@worldscoutjamboree.de.


Gut Pfad und bis bald
Dein Contingent Management Team
""".strip()


_NEW_ACCOUNT_MAIL_CONTENT = """
Hallo {{ p.greeting_name }},

wir haben einen neuen Account für dich eingerichtet.

All unsere Zugänge werden von einer zentralen Instanz Keycloak verwaltet.
D.h. für dich, dass du dich überall mit dem Button "WSJ Login" oder "Single Sign On" anmeldest.
Von dort wirst du weiter auf login.worldscoutjamboree.de geleitet wo du deinen Username und Passwort angibst.


Nutzer: {{ p.wsjrdp_email }}
Passwort: {{ p.additional_info.keycloak_initial_password }}


Gut Pfad und bis bald
Dein Contingent Management Team
""".strip()


def sync(
    ctx: wsjrdp2027.WsjRdpContext, conn: _psycopg.Connection, groupname: str
) -> bool:
    errors = []
    email2row = {}

    additional_info_updates = []
    keycloak_updates = []

    df = load_people_dataframe_for_groupname(conn, groupname=groupname)

    for p in wsjrdp2027.iter_people_dataframe(df):
        if not p.wsjrdp_email:
            errors.append(f"{p.role_id_name} has no wsjrdp_email")
            continue
        email2row[p.wsjrdp_email] = p

        if not _check_for_keycloak_user(
            ctx,
            conn,
            person=p,
            groupname=groupname,
            errors=errors,
            additional_info_updates=additional_info_updates,
        ):
            continue

        keycloak_user = ctx.keycloak().get_user_by_email(
            p.wsjrdp_email, allow_cached=True
        )
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
            goto_list = _get_expanded_goto_list(ctx, email)
            _LOGGER.debug(f"{p.role_id_name} {goto_list=}")
            _LOGGER.debug(f"{p.role_id_name} {p.additional_info.get(key)=}")
            if goto_list and goto_list != p.additional_info.get(key):
                additional_info_updates.append({"id": p.id, key: goto_list or None})

    if errors:
        for err in errors:
            _LOGGER.error(err)
        return False

    update_additional_info(conn, additional_info_updates, console_confirm=True)
    ctx.keycloak().update_users(keycloak_updates, console_confirm=True)
    return True


def _get_expanded_goto_list(ctx: wsjrdp2027.WsjRdpContext, email: str) -> list[str]:
    import itertools

    def get_goto_list(email, default=None) -> list[str]:
        user_dict = ctx.keycloak().get_user_or_none_by_email(email, allow_cached=True)
        if not user_dict:
            return default or []
        else:
            goto = user_dict.get("goto")
            return goto.split(",") if goto else []

    old_goto_list = []
    goto_list = get_goto_list(email, [])
    while goto_list != old_goto_list:
        old_goto_list, goto_list = (
            goto_list,
            list(
                itertools.chain.from_iterable(get_goto_list(a, [a]) for a in goto_list)
            ),
        )
    return goto_list


def main():
    ctx = wsjrdp2027.WsjRdpContext(
        __file__=__file__,
        # dry_run=True,
        # log_level=_logging.DEBUG,
    )
    out_base = ctx.make_out_path("sync_cmt_keycloak__{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    _LOGGER.info("Load mailcow aliases to fill cache")
    _aliases = ctx.mailcow().get_alias_list()
    _LOGGER.info(f"  ... loaded {len(_aliases)} aliases from mailcow")

    _LOGGER.info("Load keycloak users to fill cache")
    _users = ctx.keycloak().get_user_list()
    _LOGGER.info(f"  ... loaded {len(_users)} users from keycloak")

    with ctx.psycopg_connect() as conn:
        for groupname in GROUPNAMES_TO_SYNC:
            if not sync(ctx, conn, groupname=groupname):
                return 1

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    __import__("sys").exit(main())
