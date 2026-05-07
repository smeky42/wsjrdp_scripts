#!/usr/bin/env -S uv run
from __future__ import annotations

import logging as _logging
import os as _os
import pathlib as _pathlib
import typing as _typing


if _typing.TYPE_CHECKING:
    import wsjrdp2027


_SELFDIR = _pathlib.Path(__file__).parent.resolve()
_SELF_NAME = _pathlib.Path(__file__).stem
if "WSJRDP_SCRIPTS_CONFIG_FOR_INTEGRATION_TESTS" in _os.environ:
    _WSJRDP_SCRIPTS_CONFIG = _pathlib.Path(
        _os.environ["WSJRDP_SCRIPTS_CONFIG_FOR_INTEGRATION_TESTS"]
    ).resolve()
else:
    _WSJRDP_SCRIPTS_CONFIG = _SELFDIR / "config-integration-tests.yml"
_OUT_DIR = (_SELFDIR / ".." / "data" / "integration-tests").resolve()


def new_ctx() -> wsjrdp2027.WsjRdpContext:
    import wsjrdp2027

    wsjrdp_ctx = wsjrdp2027.WsjRdpContext(
        config=_WSJRDP_SCRIPTS_CONFIG,
        setup_logging=True,
        log_level=_logging.DEBUG,
        out_dir=_OUT_DIR / _SELF_NAME,
        argv=["app"],
    )
    return wsjrdp_ctx


_KEYCLOAK_GROUPS = ["CMT", "UL", "IST", "BMT", "EXT", "YP"]
_WSJRDP_DOMAINS = [
    "worldscoutjamboree.de",
    "units.worldscoutjamboree.de",
    "ist.worldscoutjamboree.de",
    "bmt.worldscoutjamboree.de",
]

_CLEAR_DATA = True


def _clear_mailcow(mailcow_client: wsjrdp2027.MailcowClient):
    alias_list = mailcow_client.get_alias_list()
    mailcow_client.delete_alias([a["address"] for a in alias_list])
    mailbox_list = mailcow_client.get_mailbox_list()
    mailcow_client.delete_mailbox([a["username"] for a in mailbox_list])


def main():

    ctx = new_ctx()

    for k in _KEYCLOAK_GROUPS:
        ctx.keycloak().create_group(k, exist_ok=True)
    for k in _WSJRDP_DOMAINS:
        ctx.mailcow().edit_domain(
            k,
            aliases=2000,
            mailboxes=2000,
            default_mailbox_quota_mib=1024,
            max_mailbox_quota_mib=10 * 1024,
            domain_quota_mib=5_000_000,
        )

    if _CLEAR_DATA:
        import pytest_wsjrdp2027

        print()
        print("========== Remove mailcow aliases and mailboxes")
        _clear_mailcow(ctx.mailcow())

        print()
        print("========== Remove keycloak users")
        users = ctx.keycloak().get_user_list()
        for user in users:
            ctx.keycloak().delete_user_by_id(user["id"])

        print()
        print("========== Restore integration tests DB")
        pytest_wsjrdp2027.restore_integration_tests_db()


if __name__ == "__main__":
    __import__("sys").exit(main())
