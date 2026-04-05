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

    _aliases: dict[str, list[str]]
    _mailboxes: dict[str, dict]

    def __init__(
        self, *, logger: _logging.Logger | _logging.LoggerAdapter | bool = False
    ) -> None:
        self._logger = _logging_util.to_logger_or_adapter(
            logger, prefix=f"Mailcow-Cache-{id(self)}"
        )
        self._aliases = {}
        self._mailboxes = {}

    def clear(self) -> None:
        self._aliases.clear()
        self._mailboxes.clear()

    def add_alias(
        self, email: str, goto: str | _collections_abc.Iterable[str] | None
    ) -> None:
        goto = _util.to_str_list_or_none(goto)
        if goto is None:
            self._aliases.pop(email)
        else:
            self._aliases[email] = goto


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

    def copy(self) -> _typing.Self:
        return self.fromdict(super().copy(), client=self._client_or_none())

    def __str__(self) -> str:
        cls_name = self.__class__.__qualname__
        return f"{cls_name}({super().__str__()})"

    def __repr__(self) -> str:
        cls_name = self.__class__.__qualname__
        return f"{cls_name}({super().__repr__()})"


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


class MailcowClient:
    _config: MailcowConfig
    _cache: _MailcowCache
    _dry_run: bool = False
    __session: _requests.Session | None = None

    def __init__(
        self,
        config: MailcowConfig,
        *,
        dry_run: bool | None = None,
        logger: _logging.Logger | _logging.LoggerAdapter | bool = True,
        cache_logger: _logging.Logger | _logging.LoggerAdapter | bool = False,
    ) -> None:
        self._logger = _logging_util.to_logger_or_adapter(
            logger, prefix=f"Mailcow-{id(self)}"
        )
        self._config = config
        self._dry_run = bool(dry_run)
        self._cache = _MailcowCache(logger=cache_logger)

    def close(self) -> None:
        self.clear_cache()

    def clear_cache(self) -> None:
        self._cache.clear()
        if (session := self.__session) is not None:
            session.close()

    def _session(self) -> _requests.Session:
        import requests as _requests

        if self.__session is None:
            self.__session = _requests.Session()
            self.__session.headers.update(
                {
                    "Content-Type": "application/json",
                    "X-API-Key": self._config.api_key,
                }
            )
        return self.__session

    def _log_notes_and_raise_for_danger_or_error(
        self, obj, *, allow_danger: str | None = None
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
                    self._logger.log(log_level, f"  | {line}")
            if not ok:
                raise MailcowError("\n".join(note_texts))
        else:
            if isinstance(obj, dict):
                self._logger.debug("  | payload is single JSON object")
            if isinstance(obj, list):
                self._logger.debug(f"  | payload is list of {len(obj)} JSON object(s)")

    def _request(
        self, method, path: str, allow_danger: str | None = None, **kwargs
    ) -> _typing.Any:
        url = f"{self._config.server}{path}"
        self._logger.debug(f"{method} {url} {kwargs}")
        response = self._session().request(method, url, **kwargs)
        self._logger.debug(f"  => {response}")
        response.raise_for_status()
        response_json = response.json()
        self._log_notes_and_raise_for_danger_or_error(
            response_json, allow_danger=allow_danger
        )
        # note: mailcow seems to send b'{}' if there is an empty list...
        return response_json or []

    # ==================================================================================
    # Domains
    # ==================================================================================

    def get_all_domains(self) -> list[dict]:
        return self._request("GET", "/api/v1/get/domain/all") or []

    def create_domain(self, domain: str, *, exist_ok: bool = True) -> None:
        payload = {"domain": domain}
        self._request(
            "POST",
            "/api/v1/add/domain",
            json=payload,
            allow_danger=("domain_exists" if exist_ok else None),
        )

    def delete_domain(self, domain: str | _typing.Iterable[str]) -> None:
        payload = _util.to_str_list(domain)
        if not payload:
            return
        self._request("POST", "/api/v1/delete/domain", json=payload)

    # ==================================================================================
    # Aliases
    # ==================================================================================

    def get_alias_or_none_by_address(self, address: str) -> MailcowAlias | None:
        alias_list = self.get_all_aliases(address)
        if not alias_list:
            return None
        elif len(alias_list) > 1:
            err_msg = f"Found more than one alias for {address=}"
            self._logger.error(err_msg)
            raise RuntimeError(err_msg)
        else:
            return MailcowAlias.fromdict(alias_list[0], client=self)

    def get_alias_by_address(self, address: str) -> MailcowAlias:
        alias = self.get_alias_or_none_by_address(address)
        if not alias:
            err_msg = f"Found no alias for {address=}"
            self._logger.error(err_msg)
            raise RuntimeError(err_msg)
        else:
            return alias

    def get_all_aliases(self, id: str = "all") -> list[MailcowAlias]:
        result = self._request("GET", f"/api/v1/get/alias/{id}")
        if isinstance(result, dict):
            result = [result]
        return [MailcowAlias.fromdict(d, client=self) for d in result]

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
    ) -> MailcowAlias:
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
            self._edit_alias(
                address,
                old_alias=old_alias,
                goto=goto,
                add_goto=add_goto,
                remove_goto=remove_goto,
                active=active,
            )
        else:
            self._add_alias(
                address,
                goto=goto,
                add_goto=add_goto,
                remove_goto=remove_goto,
                active=active,
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
    ) -> None:
        if active is None:
            active = True
        goto_list = _normalize_goto(goto, add=add_goto, remove=remove_goto)
        new_alias = {"address": address, "goto": ",".join(goto_list), "active": active}
        new_alias = _normalize_alias_dict_add_special_goto(new_alias)
        self._request("POST", "/api/v1/add/alias", json=new_alias)

    def _edit_alias(
        self,
        address: str,
        *,
        old_alias: dict,
        goto: str | _collections_abc.Iterable[str] | None = None,
        add_goto: str | _collections_abc.Iterable[str] | None = None,
        remove_goto: str | _collections_abc.Iterable[str] | None = None,
        active: bool | None = None,
    ) -> None:
        assert old_alias.get("address", address) == address
        old_alias = _normalize_alias_dict_remove_special_goto(old_alias)
        if goto is None:
            goto = old_alias.get("goto")
        new_alias = {k: v for k, v in old_alias.items() if k in _EDIT_ALIAS_ATTRS}
        new_alias["goto"] = ",".join(
            _normalize_goto(goto, add=add_goto, remove=remove_goto)
        )
        if active is not None:
            new_alias["active"] = active
        new_alias = _normalize_alias_dict_add_special_goto(new_alias)
        self._request(
            "POST",
            "/api/v1/edit/alias",
            json={"items": [old_alias["id"]], "attr": new_alias},
        )

    def delete_alias(self, address: str | _collections_abc.Iterable[str]) -> None:
        payload = _util.to_str_list(address)
        if not payload:
            return
        self._request("POST", "/api/v1/delete/alias", json=payload)


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
