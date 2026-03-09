from __future__ import annotations

import typing as _typing

from . import _keycloak_client, _weakref_util


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc

    import keycloak as _keycloak_python

    from . import _context


_LOGGER = __import__("logging").getLogger(__name__)


class WsjRdpKeycloakAdapter:
    _ctx = _weakref_util.WeakrefAttr["_context.WsjRdpContext"]()
    _client: _keycloak_client.KeycloakClient

    _username2user: dict[str, _keycloak_client.KeycloakUserDict]
    _keycloak_admin: _keycloak_python.KeycloakAdmin | None = None
    _username2id: dict[str, str]

    def __init__(
        self, context: _context.WsjRdpContext, /, *, verify: bool = True
    ) -> None:
        self._ctx = context
        config = context.config
        self._client = _keycloak_client.KeycloakClient(
            server_url=config.keycloak_url,
            username=config.keycloak_admin,
            password=config.keycloak_admin_password,
            realm_name=config.keycloak_realm,
            user_realm_name=config.keycloak_user_realm or config.keycloak_realm,
            verify=verify,
        )

    def get_realms(self) -> list[_keycloak_client.KeycloakRealmDict]:
        return self._client.get_realms()

    def create_group(
        self,
        name: str,
        *,
        payload: dict | None = None,
        parent: str | None = None,
        exist_ok: bool | None = None,
    ) -> _keycloak_client.KeycloakGroupDict:
        return self._client.create_group(
            name,
            payload=payload,
            parent=parent,
            exist_ok=exist_ok,
        )

    def update_user(
        self,
        username: str,
        payload: dict[str, _typing.Any],
        *,
        dry_run: bool | None = None,
    ) -> None:
        if dry_run is None:
            dry_run = self.dry_run
        self._client.update_user(
            username=username,
            payload=payload,
            dry_run=dry_run,
            audit=lambda s: self.__audit("update_user", s),
        )

    def update_users(
        self,
        updates: _collections_abc.Iterable[dict],
        *,
        dry_run: bool | None = None,
        console_confirm: bool = False,
    ) -> None:
        import pprint

        from . import _util

        if not updates:
            _LOGGER.debug("No updates to Keycloak users")
            return
        if console_confirm:
            _LOGGER.info(
                f"Updates to Keycloak users to apply:\n{pprint.pformat(updates)}"
            )
            if not _util.console_confirm("Update Keycloak users?"):
                _LOGGER.info("!! Skipped updates to Keycloak users")
                return
        for update in updates:
            username = update["username"]
            self.update_user(username, update, dry_run=dry_run)

    def __audit(self, action, description):
        self._ctx.require_approval_to_run_in_prod(
            "Do you want to update Keycloak in a PRODUCTION environment",
            category="keycloak",
            action=action,
            description=description,
        )

    @property
    def dry_run(self) -> bool:
        return self._ctx.dry_run

    def get_users_in_group(
        self, groupname: str, *, enabled: bool | None = None
    ) -> list[_keycloak_client.KeycloakUserDict]:
        return self._client.get_users_in_group(groupname, enabled=enabled)

    def get_user(
        self, username: str, *, user_profile_metadata: bool = False
    ) -> _keycloak_client.KeycloakUserDict:
        return self._client.get_user(
            username, user_profile_metadata=user_profile_metadata
        )
