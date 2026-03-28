from __future__ import annotations

import logging as _logging
import typing as _typing

import requests


if _typing.TYPE_CHECKING:
    from . import _context


_LOGGER = _logging.getLogger(__name__)


def get_all_domains(ctx):
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": ctx._config.mail_api_key,
    }
    base_url = ctx.config.mail_api_url or "https://mail.worldscoutjamboree.de"
    url = f"{base_url}/api/v1/get/domain/all"
    response = requests.get(url, headers=headers, timeout=30)
    _LOGGER.debug(f"GET {url} -> {response}")
    response.raise_for_status()
    return response.json()


def create_domain(ctx, domain: str):
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": ctx._config.mail_api_key,
    }

    payload = {"domain": domain}
    base_url = ctx.config.mail_api_url or "https://mail.worldscoutjamboree.de"
    print(f"{payload=}")
    print(f"{headers=}")
    resp = requests.post(
        f"{base_url}/api/v1/add/domain",
        json=payload,
        headers=headers,
        timeout=30,
    )
    # resp.raise_for_status()  # optional: raise exception for HTTP error codes
    _LOGGER.info("Add Domain Response: %s", resp.text)


def add_mailbox(ctx, local_part, domain, name, password):
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": ctx._config.mail_api_key,
    }

    payload = {
        "local_part": local_part,
        "domain": domain,
        "name": name,
        "quota": "1024",
        "password": password,
        "password2": password,
        "active": "1",
        "force_pw_update": "0",
        # "authsource": "keycloak",  # Only Keycloak login
        "tls_enforce_in": "1",
        "tls_enforce_out": "1",
    }

    base_url = ctx.config.mail_api_url or "https://mail.worldscoutjamboree.de"
    _LOGGER.info(f"{payload=}")
    _LOGGER.info(f"{headers=}")
    resp = requests.post(
        f"{base_url}/api/v1/add/mailbox",
        json=payload,
        headers=headers,
        timeout=30,
    )
    # resp.raise_for_status()  # optional: raise exception for HTTP error codes
    _LOGGER.info("Add Response: %s", resp.text)

    # edit_payload = {
    #     "items": [
    #     f"{local_part}@{domain}".format(local_part=local_part, domain=domain)
    #     ],
    #     "attr": {
    #     "active": "1",
    #     "authsource": "keycloak" # Only Keycloak login
    #     }
    # }
    # edit_resp = requests.post("https://mail.worldscoutjamboree.de/api/v1/edit/mailbox", json=edit_payload, headers=headers, timeout=30)
    # _LOGGER.info("Update Response: %s", edit_resp.text)


def add_alias(ctx, email, goto):
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": ctx._config.mail_api_key,
    }

    payload = {"address": email, "goto": goto, "active": "1"}

    base_url = ctx.config.mail_api_url or "https://mail.worldscoutjamboree.de"
    _LOGGER.info(f"{payload=}")
    _LOGGER.info(f"{headers=}")
    resp = requests.post(
        f"{base_url}/api/v1/add/alias",
        json=payload,
        headers=headers,
        timeout=30,
    )
    # resp.raise_for_status()  # optional: raise exception for HTTP error codes
    _LOGGER.info("Add Alias Response: %s", resp.text)


def update_alias(ctx, email, add_goto: str):
    from . import _util

    aliases = get_aliases(ctx, id=email)
    print(f"{aliases=}")
    if not aliases or not aliases[0]:
        _LOGGER.info(f"No existing alias for {email}, will create new one")
        return add_alias(ctx, email, goto=add_goto)
    old_alias = aliases[0]
    _LOGGER.info(f"Found existing alias: {old_alias}")
    old_goto_list = old_alias.get("goto", "").split(",")
    add_goto_list = [a.strip() for a in add_goto.split(",")]
    new_goto_list = list(_util.dedup(old_goto_list + add_goto_list))
    if new_goto_list == old_goto_list:
        _LOGGER.info("No update required, goto list unchanged")
        return old_alias
    EDIT_ALIAS_ATTRS = [
        "address",
        "goto",
        "goto_null",
        "goto_spam",
        "goto_ham",
        "private_comment",
        "public_comment",
        "active",
    ]
    new_alias = {k: v for k, v in old_alias.items() if k in EDIT_ALIAS_ATTRS}
    new_alias["goto"] = ",".join(new_goto_list)

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": ctx._config.mail_api_key,
    }

    payload = {"items": [old_alias["id"]], "attr": new_alias}

    base_url = ctx.config.mail_api_url or "https://mail.worldscoutjamboree.de"
    _LOGGER.info(f"{payload=}")
    _LOGGER.info(f"{headers=}")
    resp = requests.post(
        f"{base_url}/api/v1/edit/alias",
        json=payload,
        headers=headers,
        timeout=30,
    )
    # resp.raise_for_status()  # optional: raise exception for HTTP error codes
    _LOGGER.info("Edit Alias Response: %s", resp.text)


def get_aliases(ctx: _context.WsjRdpContext, *, id: str = "all") -> list[dict]:
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": ctx._config.mail_api_key,
    }
    base_url = ctx.config.mail_api_url or "https://mail.worldscoutjamboree.de"
    _LOGGER.info(f"{headers=}")
    url = f"{base_url}/api/v1/get/alias/{id}"
    response = requests.get(url, headers=headers, timeout=30)
    _LOGGER.debug(f"GET {url} -> {response}")
    response.raise_for_status()
    result = response.json()
    if isinstance(result, dict):
        return [result]
    else:
        return result


def get_mailboxes(ctx: _context.WsjRdpContext, *, id: str = "all") -> list[dict]:
    headers = {"Content-Type": "application/json", "X-API-Key": ctx.config.mail_api_key}
    base_url = ctx.config.mail_api_url or "https://mail.worldscoutjamboree.de"
    _LOGGER.info(f"{headers=}")
    url = f"{base_url}/api/v1/get/mailbox/{id}"
    response = requests.get(url, headers=headers, timeout=30)
    _LOGGER.debug(f"GET {url} -> {response}")
    response.raise_for_status()
    return response.json()
