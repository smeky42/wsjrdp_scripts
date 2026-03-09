from __future__ import annotations

import typing as _typing

from . import _util


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc

    import atlassian as _atlassian


_LOGGER = __import__("logging").getLogger(__name__)


class Helpdesk:
    _service_desk: _atlassian.ServiceDesk

    fin_service_desk_id: str | int
    fin_request_type_id: str | int

    def __init__(
        self,
        url: str,
        username: str,
        password: str,
        *,
        fin_service_desk_id: str | int | None = None,
        fin_request_type_id: str | int | None = None,
    ) -> None:
        import atlassian as _atlassian

        self._service_desk = _atlassian.ServiceDesk(
            url=url, username=username, password=password
        )
        if fin_service_desk_id is None:
            fin_service_desk_id = 3
        self.fin_service_desk_id = fin_service_desk_id
        if fin_request_type_id is None:
            fin_request_type_id = 33
        self.fin_request_type_id = fin_request_type_id

    def __enter__(self):
        return self

    def __exit__(self, *args):
        del self._service_desk

    @_util.log_call
    def create_customer(self, full_name: str, email: str) -> None:
        try:
            new_customer = self._service_desk.create_customer(full_name, email)
        except Exception as exc:
            exc_msg = str(exc)
            if "A user with that username already exists" in exc_msg:
                return
            else:
                raise
        if new_customer:
            _LOGGER.debug(f"new_customer: {_pformat_for_log(new_customer)}")

    def __normalize_service_desk_id(self, service_desk_id: str, /) -> str:
        match service_desk_id:
            case "FIN":
                return str(self.fin_service_desk_id)
            case _:
                return service_desk_id

    def create_fin_customer_request(
        self,
        *,
        summary: str,
        description: str = "",
        labels: _collections_abc.Iterable[str] | None = None,
        raise_on_behalf_of=None,
        request_participants=None,
    ) -> dict:
        from . import _util

        labels = _util.to_str_list(labels)
        values_dict = {
            "summary": summary,
            "description": description,
            "labels": labels,
        }
        new_request = self._service_desk.create_customer_request(
            service_desk_id=self.fin_service_desk_id,
            request_type_id=self.fin_request_type_id,
            values_dict=values_dict,
            raise_on_behalf_of=raise_on_behalf_of,
            request_participants=request_participants,
        )
        if new_request:
            _LOGGER.debug(f"new_request: {_pformat_for_log(new_request)}")
        return new_request

    def get_customer_request(self, issue_id_or_key: str, /) -> dict:
        return self._service_desk.get_customer_request(issue_id_or_key)


def _pformat_for_log(x):
    import pprint
    import textwrap

    textwrap.indent(pprint.pformat(x), "  | ")
