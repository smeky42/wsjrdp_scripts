from __future__ import annotations

import collections.abc as _collections_abc
import dataclasses as _dataclasses
import logging as _logging
import typing as _typing

from . import _logging_util, _util


if _typing.TYPE_CHECKING:
    import requests as _requests


_LOGGER = _logging.getLogger(__name__)


_EDIT_ALIAS_ATTRS = [
    "address",
    "goto",
    "goto_null",
    "goto_spam",
    "goto_ham",
    "private_comment",
    "public_comment",
    "active",
]


class _MailcowCache:
    _logger: _logging.Logger | _logging.LoggerAdapter

    _domains: dict[str | None, MailcowDomain]
    _all_domains_cached: bool = False

    _mailboxes: dict[str, dict]

    _address2alias: dict[str | None, MailcowAlias]
    _aid2address: dict[int | None, str]
    _all_aliases_cached: bool = False
    _max_alias_id: int = 0

    def __init__(
        self, *, logger: _logging.Logger | _logging.LoggerAdapter | bool = False
    ) -> None:
        self._logger = _logging_util.to_logger_or_adapter(
            logger, prefix=f"Mailcow-Cache-{id(self)}"
        )
        self._domains = {}
        self._mailboxes = {}
        self._aid2address = {}
        self._address2alias = {}

    def clear(self) -> None:
        self._domains.clear()
        self._all_aliases_cached = False
        self._mailboxes.clear()
        self._aid2address.clear()
        self._address2alias.clear()
        self._all_aliases_cached = False
        self._max_alias_id = 0

    # ==================================================================================
    # Domains Cache
    # ==================================================================================

    @property
    def all_domains_cached(self) -> bool:
        return self._all_domains_cached

    def add_domain(self, domain: MailcowDomain | dict) -> None:
        domain = MailcowDomain.fromdict(domain)
        name = domain.get("domain_name")
        if not name:
            raise ValueError(f"Domain must have domain_name, got {domain=}")
        self._domains[name] = domain

    def add_domains(
        self, domains: _typing.Iterable[MailcowDomain | dict], *, all: bool = False
    ) -> None:
        domains = list(domains)
        self._logger.debug(f"add {len(domains)} domains to cache, {all=}")
        for domain in domains:
            self.add_domain(domain)
        if all:
            self._all_domains_cached = True

    def get_domain_by_name(
        self, name: str | None, *, client: MailcowClient | None
    ) -> MailcowDomain | None:
        if domain := self._domains.get(name):
            return domain.__with_client__(client)
        else:
            return None

    def get_domain_list(
        self, key: str, *, client: MailcowClient | None, allow_cached: bool = True
    ) -> list[MailcowDomain] | None:
        if not allow_cached:
            return None
        elif key == "all":
            if self._all_domains_cached:
                return [
                    domain.__with_client__(client) for domain in self._domains.values()
                ]
            else:
                return None
        else:
            domain = self.get_domain_by_name(key, client=client)
            return [domain] if domain is not None else None

    # ==================================================================================
    # Aliases Cache
    # ==================================================================================

    @property
    def max_alias_id(self) -> int:
        return self._max_alias_id

    @property
    def all_aliases_cached(self) -> bool:
        return self._all_aliases_cached

    def add_alias(self, alias: MailcowAlias | dict) -> None:
        alias = MailcowAlias.fromdict(alias)
        address = alias.get("address")
        if not address:
            raise ValueError(f"Alias must have address, got {alias=}")
        id = alias.get("id")
        if not isinstance(id, int):
            raise ValueError(f"Alias must have integer id, got {alias=}")
        self._max_alias_id = max(self._max_alias_id, id)

        if old_alias := self._address2alias.pop(address, None):
            old_id = old_alias.get("id")
            self._aid2address.pop(old_id, None)

        self._address2alias[address] = alias
        self._aid2address[id] = address

    def add_aliases(
        self, aliases: _typing.Iterable[MailcowAlias], *, all: bool = False
    ) -> None:
        for alias in aliases:
            self.add_alias(alias)
        if all:
            self._all_aliases_cached = True

    def get_alias_by_address(
        self, address: str | None, *, client: MailcowClient | None
    ) -> MailcowAlias | None:
        if alias := self._address2alias.get(address):
            return alias.__with_client__(client)
        else:
            return None

    def get_alias_by_id(
        self, id: int, *, client: MailcowClient | None
    ) -> MailcowAlias | None:
        address = self._aid2address.get(id)
        return self.get_alias_by_address(address, client=client)

    def get_alias_list(
        self, key: str | int, *, client: MailcowClient | None
    ) -> list[MailcowAlias] | None:
        if key == "all":
            if self._all_aliases_cached:
                return [
                    alias.__with_client__(client)
                    for alias in self._address2alias.values()
                ]
            else:
                return None
        else:
            if isinstance(key, int):
                alias = self.get_alias_by_id(key, client=client)
            else:
                alias = self.get_alias_by_address(key, client=client)
            return [alias] if alias is not None else None

    def delete_alias(
        self, address_or_id: str | int | _collections_abc.Iterable[str | int], /
    ) -> None:
        for key in _util.to_int_or_str_list(address_or_id):
            if isinstance(key, int):
                address = self._aid2address.pop(key, None)
                self._address2alias.pop(address, None)
            else:
                alias = self._address2alias.pop(key, None)
                if alias:
                    self._aid2address.pop(alias.get("id"))


class MailcowError(RuntimeError):
    pass


class _MailcowDict(dict):
    __slots__ = ("_client_ref",)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._client_ref = None

    @classmethod
    def fromdict(
        cls, obj: dict, /, *, client: MailcowClient | None = None
    ) -> _typing.Self:
        import weakref

        self = cls(obj)
        if client is not None:
            self._client_ref = weakref.ref(client)
        return self

    def _client_or_none(self) -> MailcowClient | None:
        if (ref := self._client_ref) is None:
            return None
        elif (client := ref()) is None:
            return None
        else:
            return client

    def _client(self) -> MailcowClient:
        if (ref := self._client_ref) is None:
            raise RuntimeError("No associated MailcowClient")
        elif (client := ref()) is None:
            raise RuntimeError("Associated MailcowClient got deleted")
        else:
            return client

    def __without_client__(self) -> _typing.Self:
        return self.fromdict(self, client=None)

    def __with_client__(self, client: MailcowClient | None) -> _typing.Self:
        return self.fromdict(self, client=client)

    def copy(self) -> _typing.Self:
        return self.fromdict(super().copy(), client=self._client_or_none())

    def __str__(self) -> str:
        cls_name = self.__class__.__qualname__
        return f"{cls_name}({super().__str__()})"

    def __repr__(self) -> str:
        cls_name = self.__class__.__qualname__
        return f"{cls_name}({super().__repr__()})"


class MailcowDomain(_MailcowDict):
    @property
    def domain_name(self) -> str:
        return self["domain_name"]


class MailcowAlias(_MailcowDict):
    @classmethod
    def fromdict(
        cls, obj: dict, /, *, client: MailcowClient | None = None
    ) -> _typing.Self:
        return super().fromdict(
            _normalize_alias_dict_add_special_goto(obj), client=client
        )

    @property
    def id(self) -> int:
        return self["id"]

    @property
    def address(self) -> str:
        return self["address"]

    @property
    def goto(self) -> list[str]:
        """List of goto addresses.

        >>> MailcowAlias({"goto": "foo"}).goto
        ['foo']
        >>> MailcowAlias({"goto": "foo,bar"}).goto
        ['foo', 'bar']
        >>> MailcowAlias({"goto": "null@localhost"}).goto
        []
        >>> MailcowAlias({"goto": "null@localhost"}).goto_null
        True
        """
        if goto := self.get("goto"):
            if goto in ("null@localhost", "spam@localhost", "ham@localhost"):
                return []
            else:
                return goto.split(",")
        else:
            return []

    def _check_special_goto(self, key: str) -> bool:
        expected_goto = object()
        for goto_key, goto_addr in _SPECIAL_ALIAS_GOTO_ARGS:
            val = self.get(goto_key)
            if goto_key == key:
                expected_goto = goto_addr
                if val:
                    return True
            elif val:
                return False
        return self.get("goto", "") == expected_goto

    @property
    def goto_null(self) -> bool:
        """`True` if *self* is a goto null alias.

        >>> MailcowAlias({"goto": "null@localhost"}).goto_null
        True
        >>> MailcowAlias({"goto_null": 1}).goto_null
        True
        >>> MailcowAlias({"goto_null": True}).goto_null
        True
        >>> MailcowAlias({"goto_null": True, "goto_spam": True, "goto_ham": True}).goto_null
        True
        """
        return self._check_special_goto("goto_null")

    @property
    def goto_spam(self) -> bool:
        """`True` if *self* is a goto spam alias.

        >>> MailcowAlias({"goto": "spam@localhost"}).goto_spam
        True
        >>> MailcowAlias({"goto_spam": 1}).goto_spam
        True
        >>> MailcowAlias({"goto_spam": True}).goto_spam
        True
        >>> MailcowAlias({"goto_spam": True, "goto_ham": True}).goto_spam
        True
        """
        return self._check_special_goto("goto_spam")

    @property
    def goto_ham(self) -> bool:
        """`True` if *self* is a goto ham alias.

        >>> MailcowAlias({"goto": "ham@localhost"}).goto_ham
        True
        >>> MailcowAlias({"goto_ham": 1}).goto_ham
        True
        >>> MailcowAlias({"goto_ham": True}).goto_ham
        True
        """
        return self._check_special_goto("goto_ham")

    def delete(self) -> None:
        self._client().delete_alias(self["id"])


@_dataclasses.dataclass
class MailcowConfig:
    server: str
    api_key: str
    timeout: float = 30.0


class MailcowAudithook(_typing.Protocol):
    def __call__(self, action, /, **kwargs) -> object: ...


class MailcowClient:
    _config: MailcowConfig
    _cache: _MailcowCache
    _dry_run: bool = False
    __session: _requests.Session | None = None
    __is_closed: bool = False
    _audithook: MailcowAudithook

    def __init__(
        self,
        config: MailcowConfig,
        *,
        dry_run: bool | None = None,
        logger: _logging.Logger | _logging.LoggerAdapter | bool = True,
        cache_logger: _logging.Logger | _logging.LoggerAdapter | bool = False,
        audithook: MailcowAudithook | None = None,
    ) -> None:
        self._logger = _logging_util.to_logger_or_adapter(
            logger, prefix=f"Mailcow-{id(self)}"
        )
        self._config = config
        self._cache = _MailcowCache(logger=cache_logger)
        self._dry_run = bool(dry_run)
        self.audithook = audithook

    def close(self) -> None:
        self.clear()
        if (session := self.__session) is not None:
            session.close()
            self.__session = None
        self.__is_closed = True

    @property
    def closed(self) -> bool:
        """`True` if the underlying HTTP session is already closed."""
        return self.__is_closed

    def clear(self) -> None:
        """Clears the cache."""
        self._cache.clear()

    def _session(self) -> _requests.Session:
        import requests as _requests

        if self.__session is None:
            if self.__is_closed:
                raise RuntimeError("Underlying HTTP session already closed")
            self.__session = _requests.Session()
            self.__session.headers.update(
                {
                    "Content-Type": "application/json",
                    "X-API-Key": self._config.api_key,
                }
            )
        return self.__session

    @property
    def audithook(self) -> MailcowAudithook | None:
        return self._audithook if self._audithook is not self.__null_audithook else None

    @audithook.setter
    def audithook(self, value: MailcowAudithook | None) -> None:
        self._audithook = value if value is not None else self.__null_audithook

    @staticmethod
    def __null_audithook(action, /, **kwargs):
        return

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    def _log_notes_and_raise_for_danger_or_error(
        self,
        obj,
        *,
        allow_danger: str | None = None,
        logger: _logging.Logger | _logging.LoggerAdapter,
    ) -> None:
        if notes := list(_iter_notes_from_response(obj, allow_danger=allow_danger)):
            ok = True
            note_texts = []
            for note in notes:
                match note["type"]:
                    case "error":
                        ok = False
                        log_level = _logging.ERROR
                    case "danger":
                        ok = False
                        log_level = _logging.WARNING
                    case _:
                        log_level = _logging.DEBUG
                note_text = _format_note_dict(note, with_action=False)
                note_texts.append(note_text)
                for line in note_text.splitlines(keepends=False):
                    logger.log(log_level, f"  | {line}")
            if not ok:
                raise MailcowError("\n".join(note_texts))
        else:
            if isinstance(obj, dict):
                logger.debug("  | payload is single JSON object")
            if isinstance(obj, list):
                logger.debug(f"  | payload is list of {len(obj)} JSON object(s)")

    def _request(
        self,
        method,
        path: str,
        allow_danger: str | None = None,
        read_only: bool | None = None,
        json=None,
        dry_run=None,
    ) -> _typing.Any:
        if read_only is None:
            read_only = method.upper() in ("GET", "HEAD")
        dry_run = self._dry_run or bool(dry_run)
        if dry_run and read_only:
            dry_run = False
            logger = _logging_util.PrefixLoggerAdapter(
                self._logger, prefix="[dry-run ignored (read-only)]"
            )
        elif dry_run:
            logger = _logging_util.PrefixLoggerAdapter(
                self._logger, prefix="[dry-run] [skipped]"
            )
        else:
            logger = self._logger

        url = f"{self._config.server}{path}"
        audit_args = {
            "read_only": read_only,
            "json": json,
            "allow_danger": allow_danger,
        }
        audit_args = {k: v for k, v in audit_args.items() if v is not None}
        audit_args_msg = " ".join(f"{k}={v!r}" for k, v in audit_args.items())
        logger.debug(f"{method} {url} {audit_args_msg}")
        if dry_run:
            response = _DryRunResponse(200)
        else:
            action = path.lstrip("/").removeprefix("api/v1").lstrip("/")
            self._audithook(action, method=method, url=url, **audit_args)
            response = self._session().request(method, url, json=json)
        logger.debug(f"  => {response}")
        response.raise_for_status()
        response_json = response.json()
        self._log_notes_and_raise_for_danger_or_error(
            response_json, allow_danger=allow_danger, logger=logger
        )
        return response_json

    # ==================================================================================
    # Domains
    # ==================================================================================

    def get_domain_list(
        self,
        id: str = "all",
        /,
        *,
        allow_cached: bool = True,
    ) -> list[MailcowDomain]:
        if (
            domain_list := self._cache.get_domain_list(
                id, client=self, allow_cached=allow_cached
            )
        ) is not None:
            return domain_list
        result = self._request("GET", f"/api/v1/get/domain/{id}")
        if not result:
            result = []
        elif isinstance(result, dict):
            result = [result]
        domains = [MailcowDomain.fromdict(d, client=self) for d in result]
        self._cache.add_domains(domains, all=(id == "all"))
        return domains

    def get_domain_or_none_by_name(
        self, domain_name: str, /, *, allow_cached: bool = True
    ) -> MailcowDomain | None:
        domain_list = self.get_domain_list(domain_name, allow_cached=allow_cached)
        if not domain_list:
            return None
        elif len(domain_list) > 1:
            err_msg = f"Found more than one domain for {domain_name=}"
            self._logger.error(err_msg)
            raise RuntimeError(err_msg)
        else:
            return domain_list[0]

    def get_domain_by_name(
        self, domain_name: str, /, *, allow_cached: bool = True
    ) -> MailcowDomain:
        if domain := self.get_domain_or_none_by_name(
            domain_name, allow_cached=allow_cached
        ):
            return domain
        else:
            err_msg = f"Found no domain with name {domain_name=}"
            self._logger.error(err_msg)
            raise RuntimeError(err_msg)

    def add_domain(
        self, domain_name: str, *, exist_ok: bool = True, dry_run: bool | None = None
    ) -> MailcowDomain:
        dry_run = self.dry_run or bool(dry_run)
        if dry_run:
            return self._add_domain_dry_run(domain_name, exist_ok=exist_ok)
        else:
            payload = {"domain": domain_name}
            self._request(
                "POST",
                "/api/v1/add/domain",
                json=payload,
                allow_danger=("domain_exists" if exist_ok else None),
            )
            return self.get_domain_by_name(domain_name)

    def _add_domain_dry_run(self, domain_name: str, exist_ok: bool) -> MailcowDomain:
        self._logger.debug(f"[dry-run] load all domains into cache")
        self.get_domain_list(allow_cached=True)  # ensure we have all domains cached
        cached_domain = self._cache.get_domain_by_name(domain_name, client=self)
        if cached_domain:
            if exist_ok:
                return cached_domain
            else:
                raise MailcowError(f"dry-run error: {domain_name=} already exists")
        else:
            fake_domain = MailcowDomain.fromdict(
                {"domain_name": domain_name}, client=self
            )
            self._logger.debug(f"[dry-run] add fake domain {fake_domain} to cache")
            self._cache.add_domain(fake_domain)
            return fake_domain

    def delete_domain(self, domain: str | _typing.Iterable[str]) -> None:
        payload = _util.to_str_list(domain)
        if not payload:
            return
        self._request("POST", "/api/v1/delete/domain", json=payload)

    # ==================================================================================
    # Aliases
    # ==================================================================================

    def get_alias_list(
        self,
        id: str | int = "all",
        *,
        allow_cached: bool = True,
    ) -> list[MailcowAlias]:
        if (
            allow_cached
            and (alias_list := self._cache.get_alias_list(id, client=self)) is not None
        ):
            return alias_list
        result = self._request("GET", f"/api/v1/get/alias/{id}")
        if not result:
            # note: mailcow seems to send b'{}' if there is an empty list
            result = []
        elif isinstance(result, dict):
            result = [result]
        aliases = [MailcowAlias.fromdict(d, client=self) for d in result]
        self._cache.add_aliases(aliases, all=(id == "all"))
        return aliases

    def get_alias_or_none_by_address(self, address: str) -> MailcowAlias | None:
        alias_list = self.get_alias_list(address)
        if not alias_list:
            return None
        elif len(alias_list) > 1:
            err_msg = f"Found more than one alias for {address=}"
            self._logger.error(err_msg)
            raise RuntimeError(err_msg)
        else:
            return alias_list[0]

    def get_alias_by_address(self, address: str) -> MailcowAlias:
        alias = self.get_alias_or_none_by_address(address)
        if not alias:
            err_msg = f"Found no alias for {address=}"
            self._logger.error(err_msg)
            raise RuntimeError(err_msg)
        else:
            return alias

    def add_alias(
        self,
        address: str,
        *,
        goto: str | _collections_abc.Iterable[str] | None = None,
        add_goto: str | _collections_abc.Iterable[str] | None = None,
        remove_goto: str | _collections_abc.Iterable[str] | None = None,
        goto_null: bool | None = None,
        goto_ham: bool | None = None,
        goto_spam: bool | None = None,
        active: bool | None = None,
        dry_run: bool | None = None,
    ) -> MailcowAlias:
        if self.dry_run or bool(dry_run):
            self._logger.debug(f"[dry-run] load all aliases into cache")
            self.get_alias_list(allow_cached=True)  # ensure we have all aliases cached
        if goto_null:
            goto = "null@localhost"
            add_goto = remove_goto = None
        elif goto_spam:
            goto = "spam@localhost"
            add_goto = remove_goto = None
        elif goto_ham:
            goto = "ham@localhost"
            add_goto = remove_goto = None

        old_alias = self.get_alias_or_none_by_address(address)
        if old_alias:
            return self._edit_alias(
                address,
                old_alias=old_alias,
                goto=goto,
                add_goto=add_goto,
                remove_goto=remove_goto,
                active=active,
                dry_run=dry_run,
            )
        else:
            self._add_alias(
                address,
                goto=goto,
                add_goto=add_goto,
                remove_goto=remove_goto,
                active=active,
                dry_run=dry_run,
            )
            return self.get_alias_by_address(address)

    def _add_alias(
        self,
        address: str,
        *,
        goto: str | _collections_abc.Iterable[str] | None = None,
        add_goto: str | _collections_abc.Iterable[str] | None = None,
        remove_goto: str | _collections_abc.Iterable[str] | None = None,
        active: bool | None = None,
        dry_run: bool | None = None,
    ) -> None:
        if active is None:
            active = True
        goto_list = _normalize_goto(goto, add=add_goto, remove=remove_goto)
        new_alias = {"address": address, "goto": ",".join(goto_list), "active": active}
        new_alias = _normalize_alias_dict_add_special_goto(new_alias)
        dry_run = self.dry_run or bool(dry_run)
        if dry_run:
            new_alias.setdefault("id", self._cache.max_alias_id + 1)
            self._logger.debug(f"[dry-run] add fake alias {new_alias} to cache")
            self._cache.add_alias(new_alias)
        else:
            self._request("POST", "/api/v1/add/alias", json=new_alias, read_only=False)

    def _edit_alias(
        self,
        address: str,
        *,
        old_alias: dict,
        goto: str | _collections_abc.Iterable[str] | None = None,
        add_goto: str | _collections_abc.Iterable[str] | None = None,
        remove_goto: str | _collections_abc.Iterable[str] | None = None,
        active: bool | None = None,
        dry_run: bool | None = None,
    ) -> MailcowAlias:
        if old_alias.get("address", address) != address:
            raise ValueError(
                f"Inconsistent address, {address=} != {old_alias.get('address')}"
            )
        new_alias = _normalize_alias_dict_remove_special_goto(old_alias)
        if goto is None:
            goto = new_alias.get("goto")
        new_alias["goto"] = ",".join(
            _normalize_goto(goto, add=add_goto, remove=remove_goto)
        )
        if active is not None:
            new_alias["active"] = active
        new_alias = _normalize_alias_dict_add_special_goto(new_alias)
        self._cache.add_alias(new_alias)

        attr = {k: v for k, v in new_alias.items() if k in _EDIT_ALIAS_ATTRS}
        payload = {"items": [old_alias["id"]], "attr": attr}
        self._request(
            "POST", "/api/v1/edit/alias", json=payload, read_only=False, dry_run=dry_run
        )
        return MailcowAlias.fromdict(new_alias, client=self)

    def delete_alias(self, address: str | _collections_abc.Iterable[str]) -> None:
        payload = _util.to_int_or_str_list(address)
        if not payload:
            return
        self._cache.delete_alias(payload)
        self._request("POST", "/api/v1/delete/alias", json=payload, read_only=False)


def _to_note_dict(obj, allow_danger: str | None = None) -> dict | None:
    if not isinstance(obj, dict):
        return None
    err_type = obj.get("type")
    msg = obj.pop("msg", None)
    if msg:
        if isinstance(msg, str):
            msg = [msg]
        obj["msg"] = msg
    if err_type == "danger" and allow_danger:
        if any(allow_danger in str(x) for x in obj.get("msg", [])):
            obj["type"] = "info"

    if err_type in ("error", "success", "danger", "info"):
        return obj
    else:
        return None


def _iter_notes_from_response(
    obj, *, allow_danger: str | None = None
) -> _collections_abc.Iterator[dict]:
    if d := _to_note_dict(obj, allow_danger=allow_danger):
        yield d
    elif isinstance(obj, list) and len(obj) > 0:
        for item in obj:
            if d := _to_note_dict(item, allow_danger=allow_danger):
                yield d
            else:
                break


def _format_note_dict(obj: dict, *, with_action: bool = True) -> str:
    note_type = str(obj.get("type") or "unknown")
    msg = " ".join(str(x) for x in obj.get("msg", [])).strip()
    if with_action:
        log = " ".join(str(x) for x in obj.get("log", [])).strip()
        if log:
            action_prefix = "Failed" if note_type in ("danger", "error") else "Action"
            action = f"{action_prefix}: {log}"
        else:
            action = None
        note = "\n  ".join(filter(None, [msg, action]))
    else:
        note = msg or "???"
    return f"{note_type}: {note}"


def _normalize_goto(
    goto: str | _collections_abc.Iterable[str] | None,
    /,
    *,
    add: str | _collections_abc.Iterable[str] | None = None,
    remove: str | _collections_abc.Iterable[str] | None = None,
) -> list[str]:
    goto = list(_iter_gotos(goto))
    goto.extend(_iter_gotos(add))
    remove = set(_iter_gotos(remove))
    if remove:
        goto = [x for x in goto if x not in remove]
    return _util.dedup(goto)


def _iter_gotos(
    obj: str | _collections_abc.Iterable[str] | None,
) -> _collections_abc.Iterator[str]:
    if obj is None:
        return
    elif isinstance(obj, str):
        yield from (stripped for item in obj.split(",") if (stripped := item.strip()))
    else:
        for gotos in obj:
            yield from _iter_gotos(gotos)


_SPECIAL_ALIAS_GOTO_ARGS = [
    ("goto_null", "null@localhost"),
    ("goto_spam", "spam@localhost"),
    ("goto_ham", "ham@localhost"),
]


def _normalize_alias_dict_add_special_goto(d: dict, /) -> dict:
    has_special_goto = False
    for key, _ in _SPECIAL_ALIAS_GOTO_ARGS:
        if d.get(key):
            d[key] = has_special_goto = True
            d.pop("goto", None)
            for rm_key, _ in _SPECIAL_ALIAS_GOTO_ARGS:
                if rm_key != key:
                    d.pop(rm_key, None)
            break
    for key, val in _SPECIAL_ALIAS_GOTO_ARGS:
        if d.get("goto") == val:
            d[key] = has_special_goto = True
            d.pop("goto", None)
            break
    if not has_special_goto and not d.get("goto"):
        d["goto_null"] = has_special_goto = True
        d.pop("goto", None)
    return d


def _normalize_alias_dict_remove_special_goto(d: dict, /) -> dict:
    for key, val in _SPECIAL_ALIAS_GOTO_ARGS:
        if d.get(key):
            d["goto"] = val
            for rm_key, _ in _SPECIAL_ALIAS_GOTO_ARGS:
                d.pop(rm_key, None)
            break
    return d


class _DryRunResponse:
    def __init__(self, status_code=200, json=None):
        self.status_code = status_code
        self._json = json or {}

    def raise_for_status(self):
        pass

    def json(self):
        return self._json

    def __repr__(self):
        return f"<{self.__class__.__qualname__} [{self.status_code}]>"
