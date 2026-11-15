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
    "HitobitoApiError",
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

    # ==================================================================================
    # Finance: Umbuchungen (internal transfers)
    # ==================================================================================

    LINK_INTERNAL_TRANSFERS_PATH = "fin/acc/internal_transfers/link_unique.json"

    def link_internal_transfers(self) -> dict[str, _typing.Any] | None:
        """Ask the app to link every Umbuchung suggestion that is unique.

        The app works out which bank and Moss transactions are transfers between
        the contingent's own accounts (``Fin::InternalTransferDetection`` in the
        wagon), so the detection rule lives in one place only. The import scripts
        call this after a committed import, with the client of the purpose
        ``finance_import`` (scope ``finance:write``, acting person on the
        finance write tier).

        Returns the app's counts (``linked``, ``ambiguous``, ``waiting``,
        ``without_counterpart``) and logs them, or ``None`` in a dry run (the
        POST is skipped). An HTTP error raises :class:`requests.HTTPError`;
        what a caller does about it is its own business (the importers only log
        it -- their import is committed already).
        """
        response = self.post(self.LINK_INTERNAL_TRANSFERS_PATH)
        if response is None:
            return None
        response.raise_for_status()
        result = response.json()
        self._logger.info(
            f"Umbuchungen: {result.get('linked')} linked, "
            f"{result.get('ambiguous')} ambiguous, "
            f"{result.get('waiting')} waiting for their counterpart, "
            f"{result.get('without_counterpart')} without counterpart."
        )
        return result

    # ==================================================================================
    # Abmeldung (Person::DeregistrationController in the wagon)
    # ==================================================================================

    # The API key needs the scopes people and people:log and an acting person
    # who may :log the person (purpose ``deregistration``). The app builds the
    # Abmelde-Formular and answers what its mail needs, so the form and the
    # mail say the same (doc/wsjrdp/deregistration.md in the wagon).

    @staticmethod
    def deregistration_path(person_id: int, action: str | None = None) -> str:
        """
        >>> HitobitoClient.deregistration_path(12)
        'people/12/deregistration.json'
        >>> HitobitoClient.deregistration_path(12, "form.pdf")
        'people/12/deregistration/form.pdf'
        """
        base = f"people/{int(person_id)}/deregistration"
        return f"{base}.json" if action is None else f"{base}/{action}"

    def get_deregistration(self, person_id: int) -> dict[str, _typing.Any]:
        """The person's deregistration as the app answers it: ``kind``,
        ``available``, ``not_available_reason``, ``issue``, ``status``,
        ``contract_status``, ``role_id_name``, ``contract_names``, ``form``
        (``created_date``, ``reply_due_date``, ``file_name``), ``mail``
        (``from``, ``bcc``, ``unit``) and ``form_mailings``."""
        response = self.get(self.deregistration_path(person_id))
        assert response is not None  # a dry run sends GET too
        _raise_for_status(response)
        return response.json()

    def create_deregistration_form(
        self, person_id: int
    ) -> dict[str, _typing.Any] | None:
        """Have the app make the Abmelde-Formular -- like "PDF erzeugen" on the
        page: its day, its deadline and the captured data are fixed. A form
        made already stays as it is. Answers ``{"deregistration", "errors",
        "warnings"}``, ``None`` in a dry run (the POST is skipped); an
        incomplete form raises :class:`HitobitoApiError` (422) with every
        error and warning, and nothing is written."""
        response = self.post(self.deregistration_path(person_id, "create_form.json"))
        if response is None:
            return None
        _raise_for_status(response)
        return response.json()

    def prepare_deregistration_form(
        self, person_id: int
    ) -> dict[str, _typing.Any] | None:
        """Have the app get the deregistration ready for the form's mail, in
        one go: the person's registration status moves on to
        ``deregistration_noted`` (unless it is that or ``deregistered``
        already; PaperTrail writes it down with the API key), the form is made
        where it is not yet. Answers ``{"deregistration", "errors",
        "warnings"}`` (the deregistration as it then stands), ``None`` in a
        dry run (the POST is skipped). A form that would not be complete
        raises :class:`HitobitoApiError` (422) with every error and warning,
        and nothing is written."""
        response = self.post(self.deregistration_path(person_id, "prepare_form.json"))
        if response is None:
            return None
        _raise_for_status(response)
        return response.json()

    def get_deregistration_form_pdf(self, person_id: int) -> bytes:
        """The made Abmelde-Formular as PDF bytes: built by the app on request,
        or -- once the form went out -- the PDF that was sent, as stored."""
        response = self.get(
            self.deregistration_path(person_id, "form.pdf"),
            headers={"Accept": "application/pdf"},
        )
        assert response is not None
        _raise_for_status(response)
        if not response.content.startswith(b"%PDF"):
            raise ValueError(
                f"expected a PDF, got {response.headers.get('Content-Type')!r}"
            )
        return response.content

    def report_deregistration_form_sent(
        self, person_id: int, report: _collections_abc.Mapping[str, _typing.Any]
    ) -> dict[str, _typing.Any] | None:
        """Tell the app the form went out by e-mail: it writes a person note, a
        log entry and an element of the person's form_mailings. *report* holds
        ``email_sent_at``, ``email_to``, ``email_cc``, ``email_bcc``,
        ``email_subject``, ``email_message_id``, ``script`` and
        ``deregistration`` (the answer the mail was built from). A Message-ID
        reported before writes nothing again. Answers the deregistration with
        ``form_mailing`` and ``duplicate``, ``None`` in a dry run."""
        response = self.post(
            self.deregistration_path(person_id, "form_sent.json"), json=dict(report)
        )
        if response is None:
            return None
        _raise_for_status(response)
        return response.json()

    def report_deregistration_confirmation_sent(
        self, person_id: int, report: _collections_abc.Mapping[str, _typing.Any]
    ) -> dict[str, _typing.Any] | None:
        """Tell the app the confirmation of the deregistration went out by
        e-mail: it writes a log entry and an event, and the deregistration
        moves on to ``confirmation_sent`` (no person note -- the confirmation
        script writes its own). *report* as for
        :meth:`report_deregistration_form_sent`. Answers the deregistration
        with ``confirmation_mailing`` and ``duplicate``, ``None`` in a dry run."""
        response = self.post(
            self.deregistration_path(person_id, "confirmation_sent.json"),
            json=dict(report),
        )
        if response is None:
            return None
        _raise_for_status(response)
        return response.json()

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


class HitobitoApiError(Exception):
    """An answer of the Hitobito API with an error status. The wagon's answers
    carry ``errors`` and ``warnings`` (``{"code", "message"}`` each) and, for
    the deregistration API, the ``deregistration`` as it stands; a script can
    log them all (:meth:`problems`).

    >>> e = HitobitoApiError(422, {"errors": [{"code": "no_birthday", "message": "Geburtsdatum fehlt."}],
    ...                            "warnings": [{"code": "no_issue", "message": "Kein Ticket."}]})
    >>> str(e)
    'HTTP 422: Geburtsdatum fehlt. (no_birthday)'
    >>> e.problems()
    [('error', 'no_birthday', 'Geburtsdatum fehlt.'), ('warning', 'no_issue', 'Kein Ticket.')]
    """

    def __init__(self, status: int, body: _typing.Any = None, *, url: str = "") -> None:
        self.status = status
        self.body = body if isinstance(body, dict) else {}
        self.url = url
        self.errors: list[dict] = list(self.body.get("errors") or [])
        self.warnings: list[dict] = list(self.body.get("warnings") or [])
        self.deregistration: dict | None = self.body.get("deregistration")
        reasons = "; ".join(
            f"{e.get('message')} ({e.get('code')})" for e in self.errors
        )
        super().__init__(
            f"HTTP {status}"
            + (f": {reasons}" if reasons else (f" {url}" if url else ""))
        )

    def problems(self) -> list[tuple[str, str, str]]:
        """Every error and warning as (kind, code, message), errors first."""
        return [
            ("error", e.get("code", ""), e.get("message", "")) for e in self.errors
        ] + [
            ("warning", w.get("code", ""), w.get("message", "")) for w in self.warnings
        ]


def _raise_for_status(response: _requests.Response) -> None:
    """Raise :class:`HitobitoApiError` for an error status, with the errors and
    warnings the app answered with."""
    if response.status_code < 400:
        return
    try:
        body = response.json()
    except ValueError:
        body = None
    raise HitobitoApiError(response.status_code, body, url=str(response.url))


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
