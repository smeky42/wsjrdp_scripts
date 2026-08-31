#!/usr/bin/env -S uv run
"""One-shot: resend the IST -> BMT role-change confirmation mail to a single person.

Background: the confirmation mail is normally only sent when the BMT Keycloak
user is first created, inside `sync_hitobito_keycloak._create_keycloak_user`.
If that user already exists (e.g. because `sync_hitobito_keycloak.py -g BMT` was
run in the meantime), a further sync run creates nothing and therefore sends no
mail. This throwaway script rebuilds just the mail-sending tail of
`_create_keycloak_user`, using the role-change template, for one person by id.

It does NOT touch Keycloak or Mailcow. The `keycloak_initial_password` shown in
the mail is read from the person's stored additional_info (set when the user was
created), so it matches the account that already exists.

Test in dev against Mailcatcher first:
    ./registration_tools/one-shots/resend_bmt_rolechange_mail.py --id <PERSON_ID>
Use --skip-email to render the EML without sending.
"""

from __future__ import annotations

import argparse

import wsjrdp2027
from wsjrdp2027._internal import (
    signatures as _signatures,
    sync_hitobito_keycloak as _shk,
)


def _create_argument_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--id", type=int, required=True, help="Hitobito person id")
    p.add_argument("--skip-email", action="store_true", default=None)
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=_create_argument_parser(), argv=argv, __file__=__file__
    )
    with ctx:
        person_id = ctx.parsed_args.id

        df = ctx.load_people_dataframe(where=wsjrdp2027.PeopleWhere(id=[person_id]))
        people = list(wsjrdp2027.iter_people_dataframe(df))
        if len(people) != 1:
            raise SystemExit(
                f"Expected exactly one person for id {person_id}, got {len(people)}"
            )
        person = people[0]

        ctx.logger.info(f"Resend BMT role-change mail to {person.role_id_name}")
        ctx.logger.info(
            f"  wsjrdp_email={person.wsjrdp_email}  goto(email)={person.email}"
        )

        # Mirror the role-change branch of _create_keycloak_user().
        content = _shk._NEW_BMT_ACCOUNT_MAIL_CONTENT_ROLE_CHANGE
        subject = (
            "Bestätigung deiner Black Magic-Team Anmeldung - "
            "{{ person.short_full_name }} (id {{person.id }})"
        )
        signature = _signatures.EMAIL_SIGNATURE_BMT

        batch_config = wsjrdp2027.BatchConfig(
            name=f"resend_bmt_rolechange_{person.id_and_name}".replace(" ", "_"),
            where=wsjrdp2027.PeopleWhere(id=[]),
            email_from=person.helpdesk_email,
            email_subject=subject,
            extra_email_bcc=[
                *person.primary_group.support_cmt_mail_addresses,
                "ist@worldscoutjamboree.de",
            ],
            from_addr="anmeldung@worldscoutjamboree.de",
            signature=signature,
            content=content,
        )
        prepared = batch_config.prepare(
            person,
            dry_run=ctx.dry_run,
            skip_email=ctx.skip_email,
            skip_db_updates=True,
        )
        ctx.send_mailing(prepared, zip_eml=False)


if __name__ == "__main__":
    __import__("sys").exit(main())
