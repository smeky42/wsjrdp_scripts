from __future__ import annotations

import typing as _typing

from . import _keycloak_client, _weakref_util


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc

    import keycloak as _keycloak_python

    from . import _context, _person


_LOGGER = __import__("logging").getLogger(__name__)


class WsjRdpKeycloakAdapter:
    _ctx = _weakref_util.WeakrefAttr["_context.WsjRdpContext"]()
    _dry_run: bool | None = None
    _client: _keycloak_client.KeycloakClient

    _username2user: dict[str, _keycloak_client.KeycloakUserDict]
    _keycloak_admin: _keycloak_python.KeycloakAdmin | None = None
    _username2id: dict[str, str]

    def __init__(
        self,
        context: _context.WsjRdpContext,
        /,
        *,
        verify: bool = True,
        dry_run: bool | None = None,
    ) -> None:
        self._ctx = context
        self._dry_run = dry_run
        config = context.config
        self._client = _keycloak_client.KeycloakClient(
            server_url=config.keycloak_url,
            username=config.keycloak_admin,
            password=config.keycloak_admin_password,
            realm_name=config.keycloak_realm,
            user_realm_name=config.keycloak_user_realm or config.keycloak_realm,
            verify=verify,
        )

    def close(self):
        if client := getattr(self, "_client", None):
            client.close()
            del self._client

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

    def add_user_to_group(
        self, username: str, groupname: str, dry_run: bool | None = None
    ) -> None:
        if dry_run is None:
            dry_run = self.dry_run
        self._client.add_user_to_group(
            username,
            groupname,
            dry_run=dry_run,
            audit=lambda s: self.__audit("add_user_to_group", s),
        )

    def create_user(
        self,
        email: str,
        password: str,
        *,
        username: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        enabled: bool | None = None,
        attributes: dict[str, list[str]] | None = None,
        payload: dict | None = None,
        exist_ok: bool | None = None,
        dry_run: bool | None = None,
    ) -> _keycloak_client.KeycloakUserDict:
        if dry_run is None:
            dry_run = self.dry_run
        return self._client.create_user(
            email=email,
            password=password,
            username=username,
            first_name=first_name,
            last_name=last_name,
            enabled=enabled,
            attributes=attributes,
            payload=payload,
            exist_ok=exist_ok,
            dry_run=dry_run,
            audit=lambda s: self.__audit("create_user", s),
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

    def create_or_update_user(
        self,
        email: str,
        password: str,
        *,
        username: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        enabled: bool | None = None,
        attributes: dict[str, list[str]] | None = None,
        payload: dict | None = None,
        dry_run: bool | None = None,
    ) -> _keycloak_client.KeycloakUserDict:
        if dry_run is None:
            dry_run = self.dry_run
        return self._client.create_or_update_user(
            email=email,
            password=password,
            username=username,
            first_name=first_name,
            last_name=last_name,
            enabled=enabled,
            attributes=attributes,
            payload=payload,
            dry_run=dry_run,
            audit=lambda s: self.__audit("create_or_update_user", s),
        )

    def disable_user(self, username: str, dry_run: bool | None = None) -> None:
        if dry_run is None:
            dry_run = self.dry_run
        self._client.update_user(
            username=username,
            payload={"enabled": False},
            dry_run=dry_run,
            audit=lambda s: self.__audit("disable_user", s),
        )

    def enable_user(self, username: str, dry_run: bool | None = None) -> None:
        if dry_run is None:
            dry_run = self.dry_run
        self._client.update_user(
            username=username,
            payload={"enabled": True},
            dry_run=dry_run,
            audit=lambda s: self.__audit("enable_user", s),
        )

    def delete_user_by_username(
        self,
        username: str,
        *,
        raise_on_missing: bool | None = None,
        dry_run: bool | None = None,
    ) -> None:
        if dry_run is None:
            dry_run = self.dry_run
        self._client.delete_user_by_username(
            username,
            raise_on_missing=raise_on_missing,
            audit=lambda s: self.__audit("delete_user_by_username", s),
            dry_run=dry_run,
        )

    def delete_user_by_id(
        self,
        user_id: str,
        *,
        raise_on_missing: bool | None = None,
        dry_run: bool | None = None,
    ) -> None:
        if dry_run is None:
            dry_run = self.dry_run
        self._client.delete_user_by_id(
            user_id,
            raise_on_missing=raise_on_missing,
            audit=lambda s: self.__audit("delete_user_by_id", s),
            dry_run=dry_run,
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
        return self._dry_run or self._ctx.dry_run

    def get_users_in_group(
        self, groupname: str, *, enabled: bool | None = None
    ) -> list[_keycloak_client.KeycloakUserDict]:
        return self._client.get_users_in_group(groupname, enabled=enabled)

    def get_user_by_username(
        self,
        username: str,
        *,
        user_profile_metadata: bool = False,
        omit_keys: _collections_abc.Iterable[str] = (),
        allow_cached: bool = False,
    ) -> _keycloak_client.KeycloakUserDict:
        return self._client.get_user_by_username(
            username,
            user_profile_metadata=user_profile_metadata,
            omit_keys=omit_keys,
            allow_cached=allow_cached,
        )

    def get_user_or_none_by_name(
        self,
        username: str,
        *,
        user_profile_metadata: bool = False,
        omit_keys: _collections_abc.Iterable[str] = (),
        allow_cached: bool = False,
    ) -> _keycloak_client.KeycloakUserDict | None:
        try:
            return self.get_user_by_username(
                username,
                user_profile_metadata=user_profile_metadata,
                omit_keys=omit_keys,
                allow_cached=allow_cached,
            )
        except Exception as exc:
            _LOGGER.debug(f"Failed to fetch user with {username=}: {exc}")
            return None

    def get_user_or_none_by_email(
        self,
        email: str,
        *,
        allow_cached: bool = False,
        user_profile_metadata: bool | None = None,
        omit_keys: _collections_abc.Iterable[str] | None = None,
    ) -> _keycloak_client.KeycloakUserDict | None:
        return self._client.get_user_or_none_by_email(
            email,
            allow_cached=allow_cached,
            user_profile_metadata=user_profile_metadata,
            omit_keys=omit_keys,
        )

    def get_user_by_email(
        self,
        email: str,
        *,
        allow_cached: bool = False,
        user_profile_metadata: bool | None = None,
        omit_keys: _collections_abc.Iterable[str] | None = None,
    ) -> _keycloak_client.KeycloakUserDict:
        user_dict = self._client.get_user_or_none_by_email(
            email,
            allow_cached=allow_cached,
            user_profile_metadata=user_profile_metadata,
            omit_keys=omit_keys,
        )
        if user_dict is not None:
            return user_dict
        else:
            raise RuntimeError(f"Found no keycloak user for {email=}")

    def get_user_list(
        self,
        *,
        omit_keys: _collections_abc.Iterable[str] | None = None,
        allow_cached: bool = True,
    ) -> list[_keycloak_client.KeycloakUserDict]:
        return self._client.get_user_list(
            omit_keys=omit_keys,
            allow_cached=allow_cached,
        )

    def get_user_for_person_or_none(
        self,
        person: _person.Person,
        *,
        additional_info_updates: list[dict] | None = None,
        allow_cached: bool = False,
        check_role_consistency: bool = True,
        autofix_role_consistency: bool | None = None,
    ) -> _keycloak_client.KeycloakUserDict | None:
        if check_role_consistency:
            self.__check_person_is_consistent(
                person,
                additional_info_updates=additional_info_updates,
                autofix_role_consistency=autofix_role_consistency,
            )
        if username := person.get_keycloak_username_expected():
            if user_dict := self.get_user_or_none_by_name(
                username, allow_cached=allow_cached
            ):
                return user_dict
        if wsjrdp_email := person.get_wsjrdp_email_expected():
            if user_dict := self.get_user_or_none_by_email(
                wsjrdp_email, allow_cached=allow_cached
            ):
                return user_dict
        return None

    def __check_person_is_consistent(
        self,
        person: _person.Person,
        *,
        additional_info_updates: list[dict] | None = None,
        autofix_role_consistency: bool | None = None,
    ) -> None:
        updates = person.find_role_consistency_updates()
        if not updates:
            return
        msg = f"Found inconsistent attributes for {person.role_id_name}:\n"
        for key, (old, new) in updates.items():
            msg += f"  {key}: {old} -> {new}\n"
        if autofix_role_consistency or self._ctx.console_confirm(
            f"{msg}Adjust {person.role_id_name}?"
        ):
            if additional_info_updates is None:
                additional_info_updates = []
            for key, old_new in updates.items():
                setattr(person, key, old_new[1])
                additional_info_updates.append({"id": person.id, key: old_new})
