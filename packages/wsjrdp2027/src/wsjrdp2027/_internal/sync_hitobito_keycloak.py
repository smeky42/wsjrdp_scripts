from __future__ import annotations

import collections.abc as _collections_abc
import logging as _logging

import pandas as _pandas

from .. import _context, _people_query, _person, _util


_LOGGER = _logging.getLogger(__name__)


def _check_for_keycloak_user(
    ctx: _context.WsjRdpContext,
    person: _person.Person,
    keycloak_groupname: str,
    *,
    errors: list[str],
    additional_info_updates: list[dict],
    create_missing_keycloak_user: bool = False,
    self_name: str | None = None,
    batch_name_suffix: str = "",
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

    if not create_missing_keycloak_user:
        return False
    elif (
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
            f"Create missing Keycloak user {keycloak_username} for group {keycloak_groupname}?",
            cache_key="create_missing_keycloak_user",
            cache_hint="create missing Keycloak user",
        )
    ):
        _create_keycloak_user(
            ctx,
            person=person,
            keycloak_groupname=keycloak_groupname,
            keycloak_username=keycloak_username,
            keycloak_email=keycloak_email,
            errors=errors,
            additional_info_updates=additional_info_updates,
            self_name=self_name,
            batch_name_suffix=batch_name_suffix
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
    ctx: _context.WsjRdpContext,
    person: _person.Person,
    *,
    keycloak_groupname: str,
    keycloak_username: str,
    keycloak_email: str,
    errors: list[str],
    additional_info_updates: list[dict],
    self_name: str | None = None,
    batch_name_suffix: str = "",
) -> None:
    from .. import _batch, _people_query, _util
    from . import signatures

    assert person.wsjrdp_email
    assert person.moss_email

    password = _util.generate_password()

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
    ctx.keycloak().add_user_to_group(
        username=keycloak_username, groupname=keycloak_groupname
    )
    additional_info_updates.append(
        {
            "id": person.id,
            "keycloak_initial_password": [person.keycloak_initial_password, password],
        }
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
        signature = signatures.EMAIL_SIGNATURE_CMT
    else:
        match keycloak_groupname:
            case "BMT":
                content = _NEW_BMT_ACCOUNT_MAIL_CONTENT
                subject = "Bestätigung deiner Black Magic-Team Anmeldung - {{ person.short_full_name }} (id {{person.id }})"
                signature = signatures.EMAIL_SIGNATURE_CMT
            case _:
                content = _NEW_ACCOUNT_MAIL_CONTENT
                subject = "Dein Jamboree 2027 Account - {{ person.short_full_name }} (id {{person.id }})"
                signature = signatures.EMAIL_SIGNATURE_CMT

    if self_name is None:
        self_name = "sync_hitobito_keycloak"
    batch_config = _batch.BatchConfig(
        name=f"{self_name}_{person.id_and_name}{batch_name_suffix}".replace(" ", "_"),
        where=_people_query.PeopleWhere(id=[]),
        email_from=person.helpdesk_email,
        email_subject=subject,
        extra_email_bcc=person.primary_group.support_cmt_mail_addresses,
        from_addr="anmeldung@worldscoutjamboree.de",
        signature=signature,
        content=content,
    )
    prepared = batch_config.prepare(
        person, dry_run=ctx.dry_run, skip_email=ctx.skip_email, skip_db_updates=True
    )
    ctx.send_mailing(prepared, zip_eml=False)


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

wir haben einen neuen Keycloak Account für dich eingerichtet, damit du auf weitere Dienste wie Confluence zugreifen kannst.
All unsere Zugänge (außer dem Anmeldesystem) werden von einer zentralen Keycloak Instanz verwaltet.
D.h. für dich, dass du dich überall mit dem Button "WSJ Login" oder "Single Sign On" anmeldest.
Von dort wirst du weiter auf login.worldscoutjamboree.de geleitet wo du deinen Username und Passwort angibst.


Nutzer: {{ p.wsjrdp_email }}
Passwort: {{ p.additional_info.keycloak_initial_password }}


Confluence: https://wiki.worldscoutjamboree.de/


Gut Pfad und bis bald
Dein Contingent Management Team
""".strip()


_NEW_BMT_ACCOUNT_MAIL_CONTENT = """
Hallo {{ p.greeting_name }},

wir freuen uns sehr, deine Teilnahme als Black-Magic-Tent IST am World Scout Jamboree 2027 in Polen hiermit zu bestätigen!

Ein aufregendes Abenteuer liegt vor uns, und wir danken dir für dein Engagement.


Wie geht es nun für dich weiter:

* Deine Anmeldung wurde von IST zu Black-Magic-Tent IST umgezogen.

* Wir haben einen neuen BMT Keycloak Account für dich eingerichtet, damit du auf weitere Dienste wie Confluence zugreifen kannst. All unsere Zugänge (außer dem Anmeldesystem) werden von einer zentralen Keycloak Instanz verwaltet. D.h. für dich, dass du dich überall mit dem Button "WSJ Login" oder "Single Sign On" anmeldest. Von dort wirst du weiter auf login.worldscoutjamboree.de geleitet wo du deinen Username und Passwort angibst.

    Nutzer: {{ p.wsjrdp_email }}
    Passwort: {{ p.additional_info.keycloak_initial_password }}

* Dein Zugang zum Ausgaben-Management-System Moss für Fahrtkosten usw. ist jetzt mit dem neuen BMT Keycloak Account verbunden. Wenn du bisher keinen Moss Zugang hattest, bekommst du in den nächsten Tagen eine Einladung zu Moss. Infos zu den ersten Schritten mit Moss gibt es hier: https://unithub.worldscoutjamboree.de/finanzen/moss/ (als BMT müsst ihr mit <name>@bmt.worldscoutjamboree.de anstelle von <name>@units.worldscoutjamboree.de arbeiten)

* Stelle bitte sicher, dass unsere E-Mails nicht im Spam landen: https://www.worldscoutjamboree.de/2026/03/16/whitelisting/

* Damit du keine wichtigen Informationen verpasst und dich mit anderen BMT-ISTs vernetzten kannst, trete bitte in folgende beiden Telegram-Gruppen bei:

    Black Magic all2all zum Vernetzen und Austauschen: https://t.me/+CWbJtVlW1yYyMjMy
    Black Magic Broadcast zur internen Verteilung von BMT Informationen: https://t.me/+33ZwVZpBMKRiYWYy

* Um auch außerhalb des Black Magic wichtige allgemeine Informationen für IST zu bekommen und dich auch mit den anderen ISTs zu vernetzen, laden wir dich auch in die folgenden Telegram-Gruppen ein:

    IST-Infokanal https://t.me/+_c_Glv8DtGVhZTg6 (Hier erhältst du wichtige Informationen und Updates direkt von uns)
    IST-Austauschgruppe: https://t.me/+EYdlumDfq4NjYmUy (Hier kannst du dich mit anderen IST-Mitgliedern austauschen und vernetzen)


Und hier noch mal alle wichtigen Daten zum Black Magic auf einen Blick:

- Unser erstes Vorbereitungstreffen findet vom 20.11.-22.11.2026 statt, eine Einladung mit Informationen folgt.
- Unser zweites Vorbereitungstreffen findet vom 16.04.-18.04.2027 statt, eine Einladung mit Informationen folgt.
- Das Kontingentslager findet vom 27.05.-30.05.2027 in Westernohe statt.
- Der Jamboree Zeitraum für uns als Black Magic-Team ist vom 23.07.–13.08.2027

Wir freuen uns schon jetzt, dich an unserem ersten VBT persönlich willkommen zu heißen.


Bis dahin Gut Pfad
Flo und Markus
""".strip()


def sync(
    ctx: _context.WsjRdpContext,
    people: _pandas.DataFrame
    | _person.Person
    | _collections_abc.Iterable[_person.Person],
    *,
    keycloak_groupname: str,
    create_missing_keycloak_user: bool = False,
    self_name: str | None = None,
    batch_name_suffix: str = "",
) -> bool:
    errors = []
    email2row = {}

    additional_info_updates = []
    keycloak_updates = []

    for p in _person.iter_people(people):
        if not p.wsjrdp_email:
            errors.append(f"{p.role_id_name} has no wsjrdp_email")
            continue
        email2row[p.wsjrdp_email] = p

        if not _check_for_keycloak_user(
            ctx,
            person=p,
            keycloak_groupname=keycloak_groupname,
            errors=errors,
            additional_info_updates=additional_info_updates,
            create_missing_keycloak_user=create_missing_keycloak_user,
            self_name=self_name,
            batch_name_suffix=batch_name_suffix,
        ):
            _LOGGER.info(f"Skip {p.role_id_name}, no keycloak user found")
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
                additional_info_updates.append({"id": p.id, key: [None, val]})
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
            old_goto_list = p.additional_info.get(key)
            if goto_list and goto_list != old_goto_list:
                additional_info_updates.append(
                    {"id": p.id, key: [old_goto_list, goto_list or None]}
                )

    if errors:
        for err in errors:
            _LOGGER.error(err)
        return False

    ctx.update_people_additional_info(additional_info_updates, console_confirm=True)
    ctx.keycloak().update_users(keycloak_updates, console_confirm=True)
    return True


def _get_expanded_goto_list(ctx: _context.WsjRdpContext, email: str) -> list[str]:
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
