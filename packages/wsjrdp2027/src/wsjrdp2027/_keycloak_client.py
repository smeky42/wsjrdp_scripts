from __future__ import annotations

import collections.abc as _collections_abc
import copy as _copy
import logging as _logging
import typing as _typing

import keycloak as _keycloak_python


if _typing.TYPE_CHECKING:
    from . import _context


_LOGGER = _logging.getLogger(__name__)


class KeycloakRealmDict(_typing.TypedDict, total=False):
    id: _typing.Required[str]
    realm: _typing.Required[str]


class KeycloakUserDict(_typing.TypedDict, total=False):
    id: _typing.Required[str]
    username: _typing.Required[str]
    email: _typing.Required[str]
    enabled: _typing.Required[bool]
    userProfileMetadata: _typing.NotRequired[dict]
    access: _typing.NotRequired[dict]
    attributes: _typing.NotRequired[dict]
    firstName: _typing.NotRequired[str]
    lastName: _typing.NotRequired[str]


class KeycloakGroupDict(_typing.TypedDict, total=False):
    id: _typing.Required[str]
    name: _typing.Required[str]
    path: _typing.Required[str]


class _KeycloakCache:
    _id2username: dict[str, str]
    _username2id: dict[str, str]
    _id2user: dict[str, KeycloakUserDict]

    _groupname2group: dict[str, KeycloakGroupDict]
    _groupname2id: dict[str, str]

    def __init__(self) -> None:
        self._username2id = {}
        self._id2username = {}
        self._id2user = {}

        self._groupname2group = {}
        self._groupname2id = {}

    def get_group_id(self, groupname: str) -> str | None:
        return self._groupname2id.get(groupname)

    def get_group_by_name(self, groupname: str) -> KeycloakGroupDict | None:
        return self._groupname2group.get(groupname)

    def add_group(self, group: KeycloakGroupDict) -> None:
        group = _copy.deepcopy(group)
        group_id = group["id"]
        groupname = group.get("name")
        if groupname:
            self._groupname2group[groupname] = group
            self._groupname2id[groupname] = group_id

    def remove_group_by_name(self, groupname: str) -> None:
        self._groupname2group.pop(groupname, None)
        self._groupname2id.pop(groupname, None)

    def get_user_id(self, username: str) -> str | None:
        return self._username2id.get(username)

    def get_username(self, user_id: str) -> str | None:
        return self._id2username.get(user_id)

    def get_user_by_name(
        self, username: str, *, copy: bool = True
    ) -> KeycloakUserDict | None:
        maybe_user_id = self._username2id.get(username)
        if maybe_user_id:
            return self.get_user_by_id(maybe_user_id, copy=copy)
        else:
            return None

    def get_user_by_id(
        self, user_id: str, *, copy: bool = True
    ) -> KeycloakUserDict | None:
        maybe_user_dict = self._id2user.get(user_id)
        if maybe_user_dict is not None and copy:
            return _copy.deepcopy(maybe_user_dict)
        else:
            return maybe_user_dict

    def add_user(self, user: KeycloakUserDict) -> None:
        user_id = user["id"]
        username = user.get("username")
        if not user_id:
            raise ValueError(f"Missing key 'id'")
        if not username:
            raise ValueError("Missing key 'username'")
        user = _copy.deepcopy(user)
        if (old_user := self._id2user.get(user_id)) is not None:
            import dictdiffer

            for key in ("access", "userProfileMetadata"):
                old_val = old_user.get(key)
                if old_val is not None and key not in user:
                    user[key] = old_val

            _LOGGER.debug(f"{self.__class__.__qualname__}.add_user(<{username}>)")
            if dict_diff := list(dictdiffer.diff(old_user, user)):
                _LOGGER.debug(f"  {dict_diff=}")
            old_user_id = old_user["id"]
            old_username = old_user["username"]
            self._id2username.pop(old_user_id, None)
            self._username2id.pop(old_username, None)
        self._id2user[user_id] = user
        self._id2username[user_id] = username
        self._username2id[username] = user_id

    def delete_user_by_id(self, user_id: str) -> None:
        _LOGGER.debug(f"{self.__class__.__qualname__}.delete_user_by_id({user_id!r})")
        maybe_user = self._id2user.pop(user_id, None)
        maybe_username = self._id2username.pop(user_id, None)
        if username := (maybe_username or maybe_user.get("username")):
            self._username2id.pop(username, None)

    def set_user_id_for_username(self, *, username: str, user_id: str) -> None:
        if user_dict := self._id2user.get(user_id):
            if user_dict["username"] != username:
                raise RuntimeError(
                    f"Cannot link {username=} and {user_id=}, conflict with cached user data"
                )
        self._username2id[username] = user_id
        self._id2username[user_id] = username


class KeycloakClient:
    _cache: _KeycloakCache
    _admin: _keycloak_python.KeycloakAdmin

    def __init__(
        self,
        server_url: str,
        username: str,
        password: str,
        realm_name: str,
        user_realm_name: str,
        verify: bool = True,
    ) -> None:
        self._cache = _KeycloakCache()
        self._admin = _keycloak_python.KeycloakAdmin(
            server_url=server_url,
            username=username,
            password=password,
            realm_name=realm_name,
            user_realm_name=user_realm_name,
            verify=verify,
        )

    @classmethod
    def from_wsjrdp_context(
        cls, ctx: _context.WsjRdpContext, *, verify: bool = True
    ) -> _typing.Self:
        return cls(
            server_url=ctx.config.keycloak_url,
            username=ctx.config.keycloak_admin,
            password=ctx.config.keycloak_admin_password,
            realm_name=ctx.config.keycloak_realm,
            user_realm_name=ctx.config.keycloak_user_realm or ctx.config.keycloak_realm,
            verify=verify,
        )

    def get_realms(self) -> list[KeycloakRealmDict]:
        return self._admin.get_realms()

    def create_group(
        self,
        name: str,
        *,
        payload: dict | None = None,
        parent: str | None = None,
        exist_ok: bool | None = None,
    ) -> KeycloakGroupDict:
        skip_exists = True if exist_ok is None else exist_ok
        payload = payload.copy() if payload else {}
        payload["name"] = name
        new_id = self._admin.create_group(
            payload=payload, parent=parent, skip_exists=skip_exists
        )
        if new_id:
            return self.get_group_by_id(new_id)
        else:
            return self.get_group(name)

    def delete_group(
        self,
        groupname: str,
        raise_on_missing: bool | None = None,
        audit: _collections_abc.Callable[[str | None], object] | None = None,
    ) -> None:
        if raise_on_missing is None:
            raise_on_missing = False
        try:
            group_id = self.get_group_id(groupname)
        except Exception:
            if not raise_on_missing:
                return  # group does not exist
            raise
        if group_id:
            cls_name = self.__class__.__qualname__
            description = f"{cls_name}.delete_group({groupname!r}, raise_on_missing={raise_on_missing!r})"
            audit(description) if audit else None
            self._admin.delete_group(group_id)

    def get_group_by_id(self, group_id: str) -> KeycloakGroupDict:
        group: KeycloakGroupDict | None
        group = self._admin.get_group(group_id)  # type: ignore
        if not group:
            raise RuntimeError(f"Failed to get group with id {group_id=}")
        else:
            self._cache.add_group(group)
            return group

    def get_group(self, groupname: str) -> KeycloakGroupDict:
        group: KeycloakGroupDict | None
        if group := self._cache.get_group_by_name(groupname):
            return group
        group = self._admin.get_group_by_path(groupname)  # type: ignore
        if not group:
            raise RuntimeError(f"Failed to get group with name {groupname=}")
        self._cache.add_group(group)
        return group

    def get_group_id(self, groupname: str) -> str:
        if group_id := self._cache.get_group_id(groupname):
            return group_id
        return self.get_group(groupname)["id"]

    def get_users_in_group(
        self,
        groupname: str | _collections_abc.Iterable[str],
        *,
        enabled: bool | None = None,
    ) -> list[KeycloakUserDict]:
        if isinstance(groupname, str):
            groupnames = [groupname]
        else:
            groupnames = [str(x) for x in groupname]

        all_users = []
        for groupname in groupnames:
            group_id = self.get_group_id(groupname)
            users = self._admin.get_group_members(group_id)
            for user in users:
                self._cache.add_user(user)
            if enabled is not None:
                users = [user for user in users if user.get("enabled") == enabled]
            all_users.extend(users)
        return all_users

    def get_user_id(self, username: str) -> str:
        user_id = self.get_user_id_or_none(username)
        if user_id is None:
            raise RuntimeError(f"Failed to get a user_id for {username=}")
        else:
            return user_id

    def get_user_id_or_none(self, username: str) -> str | None:
        if user_id := self._cache.get_user_id(username):
            return user_id
        user_id = self._admin.get_user_id(username)
        if user_id:
            self._cache.set_user_id_for_username(username=username, user_id=user_id)
            return user_id
        else:
            return None

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
        audit: _collections_abc.Callable[[str | None], object] | None = None,
    ) -> KeycloakUserDict:
        """Create and return new user.

        If *exist_ok* is `True` and a user with *username* already
        exists, the existing user is not updated.
        """
        dry_run = bool(dry_run)
        if exist_ok is None:
            exist_ok = True
        payload = self._mk_create_or_update_user_payload(
            email=email,
            password=password,
            username=username,
            first_name=first_name,
            last_name=last_name,
            enabled=enabled,
            attributes=attributes,
            payload=payload,
        )
        username = payload.get("username")
        assert username and isinstance(username, str)
        cls_name = self.__class__.__qualname__
        description = f"{cls_name}.create_user({email!r}, {password!r}, ...)"
        maybe_user_id = self.get_user_id_or_none(username)
        if maybe_user_id:
            user_id = maybe_user_id
            _LOGGER.debug(
                f"{description}"
                f" :: Found existing {user_id=} {username=}, return existing user"
            )
            return self.get_user_by_id(user_id)
        elif dry_run:
            _LOGGER.debug(
                f"[dry-run] {description}"
                f" :: skip call to keycloak_admin.create_user(payload={payload}, exist_ok={exist_ok})"
            )
        else:
            audit(description) if audit else None
            _LOGGER.debug(
                f"{description}\n"  #
                f"  {payload=}\n"
                f"  {exist_ok=}"
            )
            user_id = self._admin.create_user(payload, exist_ok=exist_ok)
            return self.get_user_by_id(user_id)

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
        audit: _collections_abc.Callable[[str | None], object] | None = None,
    ) -> KeycloakUserDict:
        dry_run = bool(dry_run)
        payload = self._mk_create_or_update_user_payload(
            email=email,
            password=password,
            username=username,
            first_name=first_name,
            last_name=last_name,
            enabled=enabled,
            attributes=attributes,
            payload=payload,
        )
        username = payload["username"]
        cls_name = self.__class__.__qualname__
        description = f"{cls_name}.create_or_update_user({email!r}, {password!r}, ...)"
        maybe_user_id = self.get_user_id_or_none(username)
        if maybe_user_id:
            user_id = maybe_user_id
            username = payload["username"]
            _LOGGER.debug(
                f"{description}"
                f" :: Found existing user {user_id=} {username=}, will update"
            )
            self.update_user(username, payload=payload, dry_run=dry_run, audit=audit)
            return self.get_user_by_id(user_id)
        elif dry_run:
            _LOGGER.debug(
                f"[dry-run] {description} :: Skip keycloak_admin.create_user(...)"
            )
            return payload
        else:
            audit(description) if audit else None
            _LOGGER.debug(
                f"{description}\n"  #
                f"  {payload=}\n"
                f"  exist_ok=False"
            )
            user_id = self._admin.create_user(payload, exist_ok=False)
            return self.get_user_by_id(user_id)

    def _mk_create_or_update_user_payload(
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
    ) -> dict:
        import email.utils as _email_utils

        email_addr = _email_utils.parseaddr(email, strict=True)[1]
        if not email_addr:
            raise ValueError(f"Invalid {email=}")
        if payload is None:
            payload = {}
        if enabled is None:
            enabled = True
        user_dict = {
            "username": username or payload.get("username") or email_addr,
            "email": email_addr,
            "enabled": enabled,
            "firstName": first_name or None,
            "lastName": last_name or None,
            "credentials": [{"type": "password", "value": password}],
            "attributes": attributes,
        }
        user_dict = {k: v for k, v in user_dict.items() if v is not None}
        payload = _merge_user_dicts(payload, user_dict)
        return payload

    def delete_user(
        self,
        username: str,
        raise_on_missing: bool | None = None,
        audit: _collections_abc.Callable[[str | None], object] | None = None,
    ) -> None:
        if raise_on_missing is None:
            raise_on_missing = False
        try:
            user_id = self.get_user_id(username)
        except Exception:
            if not raise_on_missing:
                return  # group does not exist
            raise
        if user_id:
            cls_name = self.__class__.__qualname__
            description = f"{cls_name}.delete_user({username!r}, raise_on_missing={raise_on_missing!r})"
            self.__delete_user_by_id(
                user_id,
                raise_on_missing=raise_on_missing,
                description=description,
                audit=audit,
            )

    def delete_user_by_id(
        self,
        user_id: str,
        raise_on_missing: bool | None = None,
        audit: _collections_abc.Callable[[str | None], object] | None = None,
    ) -> None:
        if raise_on_missing is None:
            raise_on_missing = False
        cls_name = self.__class__.__qualname__
        description = f"{cls_name}.delete_user_by_id({user_id!r}, raise_on_missing={raise_on_missing!r})"
        self.__delete_user_by_id(
            user_id, raise_on_missing, description=description, audit=audit
        )

    def __delete_user_by_id(
        self,
        user_id: str,
        raise_on_missing: bool,
        description: str,
        audit: _collections_abc.Callable[[str | None], object] | None = None,
    ) -> None:
        audit(description) if audit else None
        self._cache.delete_user_by_id(user_id)
        try:
            self._admin.delete_user(user_id)
        except _keycloak_python.KeycloakDeleteError:
            if raise_on_missing:
                raise

    def get_user_by_id(
        self,
        user_id: str,
        *,
        omit_keys: _collections_abc.Iterable[str] = (),
        user_profile_metadata: bool = False,
        prefer_cached: bool = False,
    ) -> KeycloakUserDict:
        if prefer_cached and (cached_user := self._cache.get_user_by_id(user_id)):
            return cached_user
        user_dict: KeycloakUserDict | None = self._admin.get_user(  # type: ignore
            user_id, user_profile_metadata=user_profile_metadata
        )
        if user_dict:
            self._cache.add_user(user_dict)
        else:
            raise RuntimeError(f"Failed to get user for {user_id=}")
        if omit_keys:
            omit_keys = frozenset(omit_keys)
            return {k: v for k, v in user_dict.items() if k not in omit_keys}  # type: ignore
        else:
            return user_dict

    def get_user(
        self,
        username: str,
        *,
        omit_keys: _collections_abc.Iterable[str] = (),
        user_profile_metadata: bool = False,
        prefer_cached: bool = False,
    ) -> KeycloakUserDict:
        if prefer_cached and (cached_user := self._cache.get_user_by_name(username)):
            return cached_user
        user_id = self.get_user_id(username)
        user_dict: KeycloakUserDict | None = self._admin.get_user(  # type: ignore
            user_id, user_profile_metadata=user_profile_metadata
        )
        if user_dict:
            self._cache.add_user(user_dict)
        else:
            raise RuntimeError(f"Failed to get user for {username=}")
        if omit_keys:
            omit_keys = frozenset(omit_keys)
            return {k: v for k, v in user_dict.items() if k not in omit_keys}  # type: ignore
        else:
            return user_dict

    def update_user(
        self,
        username: str,
        payload: dict[str, _typing.Any],
        *,
        prefer_cached: bool = False,
        dry_run: bool | None = None,
        audit: _collections_abc.Callable[[str | None], object] | None = None,
    ) -> None:
        """High-level user update. Merges with existing values."""
        omit_keys = frozenset(["createdTimestamp"])
        dry_run = bool(dry_run)

        original_payload = self.get_user(
            username,
            omit_keys=omit_keys,
            prefer_cached=True,
            user_profile_metadata=True,
        )
        user_id = original_payload["id"]
        new_payload = _merge_user_dicts(original_payload, payload, omit_keys=omit_keys)

        cls_name = self.__class__.__qualname__
        description = f"{cls_name}.update_user({username!r}, payload={new_payload!r})"

        if dry_run:
            _LOGGER.debug(
                f"{description}\n"
                f"  [dry-run] skip call to keycloak_admin.update_user(user_id={user_id!r}, ...)"
            )
        elif new_payload == original_payload:
            _LOGGER.debug(f"{description}\n  No diff to current user, no update")
        else:
            if not prefer_cached:
                original_payload = self.get_user(
                    username,
                    omit_keys=omit_keys,
                    prefer_cached=False,
                    user_profile_metadata=True,
                )
                new_payload = _merge_user_dicts(
                    original_payload, payload, omit_keys=omit_keys
                )
                description = (
                    f"{cls_name}.update_user({username!r}, payload={new_payload!r})"
                )

            _LOGGER.debug(
                f"{description}\n"
                f"  {original_payload=}\n"
                f"  diff :: {_dictdiff_s(original_payload, new_payload)}"
            )
            audit(description) if audit else None
            if new_payload:
                self._admin.update_user(user_id=user_id, payload=new_payload)


def _dictdiff_s(a: dict, b: dict, *, linesep: str = "\n  ") -> str:
    import dictdiffer

    changes = list(dictdiffer.diff(a, b))
    lines = []
    for verb, path, change in changes:
        lines.append(f"{verb} {path!r} {change}")
    return linesep.join(lines)


def _merge_user_dicts(
    a: dict | KeycloakUserDict,
    b: dict | KeycloakUserDict,
    /,
    omit_keys: _typing.Iterable[str] | None = None,
) -> dict[str, _typing.Any]:
    def _to_str_list(obj):
        if isinstance(obj, str):
            return [obj]
        elif isinstance(obj, _collections_abc.Iterable):
            return [str(item) for item in obj]
        else:
            return [str(obj)]

    def _dict_merge(key, old, new):
        d = old.copy() if old else {}
        d.update(new)
        d = {k: v for k, v in d.items() if v is not None}
        if key == "attributes":
            # Normalize values to be list of strings
            d = {k: _to_str_list(v) for k, v in d.items()}
        return d

    def _merge(key, old, new):
        if isinstance(old, dict) or key in ("access", "attributes"):
            return _dict_merge(key, old, new)
        else:
            return new

    # check for invalid merges
    for k in ["id", "username"]:
        if (b_val := b.get(k)) is not None:
            a_val = a.get(k)
            if (a_val is not None) and (a_val != b_val):
                raise RuntimeError(
                    f"Cannot merge user dicts where {k} does not match:\n"
                    f"  a[{k!r}] == {a_val!r}"
                    f"  b[{k!r}] == {b_val!r}"
                )

    omit_keys = frozenset(omit_keys or [])
    updates = {k: _merge(k, a.get(k), v) for k, v in b.items() if k not in omit_keys}
    merged = {k: v for k, v in a.items() if k not in omit_keys}
    merged.update(updates)
    return merged
