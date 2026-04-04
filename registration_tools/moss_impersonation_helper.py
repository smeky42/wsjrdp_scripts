#!/usr/bin/env -S uv run
from __future__ import annotations

import pprint

import wsjrdp2027


_LOGGER = __import__("logging").getLogger(__name__)


def start_moss_private_window(ctx, person):
    import subprocess

    import requests

    response = requests.get(
        "https://getmoss.com/api/v1/get_login_type", params={"email": person.moss_email}
    )
    _LOGGER.info(f"Response: {response}")
    _LOGGER.info(f"   {response.text}")
    response_data = response.json()
    assert response_data["loginType"] == "SSO_SAML"
    url = response_data["ssoLoginLink"]
    cmd = ["/Applications/Firefox.app/Contents/MacOS/firefox", "--private-window", url]
    subprocess.run(cmd, check=True)


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()

    p.add_argument("person_id", help="""Hitobito-ID der Person""")
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    )
    out_base = ctx.make_out_path("moss_impersonation__{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)
    with ctx.psycopg_connect() as conn:
        p = ctx.load_person_for_id(ctx.parsed_args.person_id, conn=conn)
    assert p.keycloak_username
    assert p.moss_email
    keycloak_user = ctx.keycloak().get_user_by_name(p.keycloak_username)
    _LOGGER.info(f"{p.role_id_name} keycloak user:\n{pprint.pformat(keycloak_user)}")
    impersonation_username = "moss-impersonation@worldscoutjamboree.de"
    impersonation_payload = {
        "firstName": p.first_name,
        "lastName": p.last_name,
        "attributes": {"mossEmail": p.moss_email},
    }
    _LOGGER.info(f"Update {impersonation_username} using {impersonation_payload}")
    ctx.keycloak().update_user(impersonation_username, impersonation_payload)
    moss_impersonation_user = ctx.keycloak().get_user_by_name(impersonation_username)
    _LOGGER.info(
        f"{impersonation_username} keycloak user:\n{pprint.pformat(moss_impersonation_user)}"
    )
    _LOGGER.info(f"  moss_email: {p.moss_email}")
    _LOGGER.info(f"  wsjrdp_email: {p.wsjrdp_email}")
    redirect_moss_email = p.moss_email.startswith("wsj27")
    _LOGGER.info(f"  redirect_moss_email: {redirect_moss_email}")
    old_moss_goto = ""
    if redirect_moss_email:
        old_moss_alias = wsjrdp2027.mailbox.get_aliases(ctx, id=p.moss_email)[0]
        old_moss_goto = old_moss_alias["goto"]
        _LOGGER.info(f"Old alias: {old_moss_alias}")
        wsjrdp2027.mailbox.update_alias(
            ctx,
            p.moss_email,
            add_goto=impersonation_username,
            remove_existing_goto=True,
        )
    start_moss_private_window(ctx, person=p)

    if redirect_moss_email:
        if ctx.console_confirm(
            f"Reset E-Mail alias to {p.moss_email} -> {old_moss_goto}?", default=True
        ):
            wsjrdp2027.mailbox.update_alias(
                ctx,
                p.moss_email,
                add_goto=old_moss_goto,
                remove_existing_goto=True,
            )


if __name__ == "__main__":
    __import__("sys").exit(main())
