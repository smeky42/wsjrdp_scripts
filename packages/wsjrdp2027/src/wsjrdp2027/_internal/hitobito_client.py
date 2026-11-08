"""A small HTTP client for the Hitobito API, authenticated with one API key."""

from __future__ import annotations

import collections.abc as _collections_abc
import dataclasses as _dataclasses
import logging as _logging
import typing as _typing

from .. import _logging_util


if _typing.TYPE_CHECKING:
    import requests as _requests

    from .._models import person as _person


__all__ = [
    "HitobitoAudithook",
    "HitobitoClient",
    "HitobitoConfig",
]


_JSON_API_MEDIA_TYPE = "application/vnd.api+json"


@_dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class HitobitoConfig:
    """Where to reach Hitobito and with which API key (*purpose* names it)."""

    hitobito_url: str
    purpose: str
    token: str = _dataclasses.field(repr=False)
    timeout: float = 60.0


class HitobitoAudithook(_typing.Protocol):
    def __call__(self, action, /, **kwargs) -> object: ...


class HitobitoClient:
    """Requests against the Hitobito API, the token in the ``X-Token`` header.

    In a dry run only GET requests are sent; any other request is logged and
    skipped (it returns ``None``). Every request that is sent passes the
    audithook first.
    """

    _config: HitobitoConfig
    _dry_run: bool = False
    __session: _requests.Session | None = None
    __is_closed: bool = False
    _audithook: HitobitoAudithook

    def __init__(
        self,
        config: HitobitoConfig,
        *,
        dry_run: bool | None = None,
        logger: _logging.Logger | _logging.LoggerAdapter | bool = True,
        audithook: HitobitoAudithook | None = None,
    ) -> None:
        self._logger = _logging_util.to_logger_or_adapter(
            logger, prefix=f"Hitobito-{config.purpose}"
        )
        self._config = config
        self._dry_run = bool(dry_run)
        self.audithook = audithook

    def __repr__(self) -> str:
        # Never the token.
        return (
            f"<{type(self).__qualname__} {self._config.hitobito_url} "
            f"purpose={self._config.purpose} dry_run={self._dry_run}>"
        )

    def close(self) -> None:
        """Close the HTTP session; the client cannot be used afterwards."""
        if (session := self.__session) is not None:
            session.close()
            self.__session = None
        self.__is_closed = True

    @property
    def closed(self) -> bool:
        """`True` once :meth:`close` was called."""
        return self.__is_closed

    @property
    def _session(self) -> _requests.Session:
        import requests as _requests

        if self.__session is None:
            if self.__is_closed:
                raise RuntimeError("Hitobito client already closed")
            session = _requests.Session()
            session.headers.update(
                {"X-Token": self._config.token, "Accept": "application/json"}
            )
            self.__session = session
        return self.__session

    @property
    def audithook(self) -> HitobitoAudithook | None:
        return self._audithook if self._audithook is not self.__null_audithook else None

    @audithook.setter
    def audithook(self, value: HitobitoAudithook | None) -> None:
        self._audithook = value if value is not None else self.__null_audithook

    @staticmethod
    def __null_audithook(action, /, **kwargs):
        return

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    @property
    def purpose(self) -> str:
        """The purpose = the name of the API key in Hitobito."""
        return self._config.purpose

    def request(self, method: str, path: str, **kwargs) -> _requests.Response | None:
        """Send *method* to *path* (relative to the Hitobito URL); *kwargs* go to
        :meth:`requests.Session.request`. ``None`` for a request a dry run skips."""
        return self._request(method, path, **kwargs)

    def get(self, path: str, **kwargs) -> _requests.Response | None:
        return self._request("GET", path, **kwargs)

    def post(self, path: str, **kwargs) -> _requests.Response | None:
        return self._request("POST", path, **kwargs)

    def put(self, path: str, **kwargs) -> _requests.Response | None:
        return self._request("PUT", path, **kwargs)

    def patch(self, path: str, **kwargs) -> _requests.Response | None:
        return self._request("PATCH", path, **kwargs)

    # ==================================================================================
    # People (JSON:API, /api/people)
    # ==================================================================================

    # The Person variants build on the resource variants (the JSON:API
    # resource objects as dicts) and convert with Person.from_hitobito_resource.

    def get_person_resource_list(
        self,
        *,
        filter: _collections_abc.Mapping[str, _typing.Any] | None = None,  # noqa: A002
        fields: _collections_abc.Iterable[str] | None = None,
        include: _collections_abc.Iterable[str] | None = None,
        page_size: int = 100,
    ) -> list[dict[str, _typing.Any]]:
        """The JSON:API resource objects of all people the token may see, page by page.

        *filter* maps an attribute to a value (``{"id": "1,2"}``) or to operators
        (``{"updated_at": {"gte": "2026-01-01"}}``).
        """
        people: list[dict[str, _typing.Any]] = []
        number = 1
        while True:
            params = _json_api_params(filter=filter, fields=fields, include=include)
            params.update({"page[number]": number, "page[size]": page_size})
            data = self._get_json_api("api/people", params)["data"]
            people.extend(data)
            if len(data) < page_size:
                return people
            number += 1

    def get_person_resource_by_id(
        self,
        person_id: int,
        *,
        fields: _collections_abc.Iterable[str] | None = None,
        include: _collections_abc.Iterable[str] | None = None,
    ) -> dict[str, _typing.Any]:
        """The JSON:API resource object (``id``, ``attributes``, ...) of one person."""
        params = _json_api_params(fields=fields, include=include)
        return self._get_json_api(f"api/people/{int(person_id)}", params)["data"]

    def get_person_resource_or_none_by_id(
        self,
        person_id: int,
        *,
        fields: _collections_abc.Iterable[str] | None = None,
        include: _collections_abc.Iterable[str] | None = None,
    ) -> dict[str, _typing.Any] | None:
        """Like :meth:`get_person_resource_by_id`, ``None`` for an unknown id."""
        import requests as _requests

        try:
            return self.get_person_resource_by_id(
                person_id, fields=fields, include=include
            )
        except _requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    def get_person_list(self, **kwargs) -> list[_person.Person]:
        """:meth:`get_person_resource_list` as Person objects (API fields only)."""
        from .._models import person as _person

        return [
            _person.Person.from_hitobito_resource(resource)
            for resource in self.get_person_resource_list(**kwargs)
        ]

    def get_person_by_id(self, person_id: int, **kwargs) -> _person.Person:
        """:meth:`get_person_resource_by_id` as a Person object (API fields only)."""
        from .._models import person as _person

        resource = self.get_person_resource_by_id(person_id, **kwargs)
        return _person.Person.from_hitobito_resource(resource)

    def get_person_or_none_by_id(
        self, person_id: int, **kwargs
    ) -> _person.Person | None:
        """Like :meth:`get_person_by_id`, ``None`` for an unknown id."""
        from .._models import person as _person

        resource = self.get_person_resource_or_none_by_id(person_id, **kwargs)
        return (
            None
            if resource is None
            else _person.Person.from_hitobito_resource(resource)
        )

    def _get_json_api(self, path: str, params: dict[str, _typing.Any]) -> dict:
        response = self.get(
            path, params=params, headers={"Accept": _JSON_API_MEDIA_TYPE}
        )
        if response is None:  # cannot happen: a dry run sends GET too
            raise RuntimeError(f"GET {path} was not sent")
        response.raise_for_status()
        return response.json()

    def _request(self, method: str, path: str, **kwargs) -> _requests.Response | None:
        method = method.upper()
        # The token goes along with every request, so only to the configured host.
        if "://" in path:
            raise ValueError(f"expected a path relative to the Hitobito URL: {path!r}")
        url = self._config.hitobito_url.rstrip("/") + "/" + path.lstrip("/")
        read_only = method == "GET"
        if self._dry_run and not read_only:
            self._logger.info(f"[dry-run] [skipped] {method} {url}")
            return None
        self._audithook(path, method=method, url=url, read_only=read_only)
        kwargs.setdefault("timeout", self._config.timeout)
        response = self._session.request(method, url, **kwargs)
        self._logger.debug(f"{method} {url} => {response.status_code}")
        return response


def _json_api_params(
    *,
    filter: _collections_abc.Mapping[str, _typing.Any] | None = None,  # noqa: A002
    fields: _collections_abc.Iterable[str] | None = None,
    include: _collections_abc.Iterable[str] | None = None,
) -> dict[str, _typing.Any]:
    """The query parameters of a JSON:API request on people.

    >>> _json_api_params(filter={"id": "1,2", "updated_at": {"gte": "2026-01-01"}},
    ...                  fields=["first_name", "last_name"], include=["roles"])
    {'filter[id]': '1,2', 'filter[updated_at][gte]': '2026-01-01', 'fields[people]': 'first_name,last_name', 'include': 'roles'}
    """
    params: dict[str, _typing.Any] = {}
    for key, value in (filter or {}).items():
        if isinstance(value, _collections_abc.Mapping):
            for operator, operand in value.items():
                params[f"filter[{key}][{operator}]"] = operand
        else:
            params[f"filter[{key}]"] = value
    if fields is not None:
        params["fields[people]"] = ",".join(fields)
    if include is not None:
        params["include"] = ",".join(include)
    return params
