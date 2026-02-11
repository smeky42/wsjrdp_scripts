from __future__ import annotations

import typing as _typing


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc

    import atlassian as _atlassian


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

    def create_fin_customer_request(
        self,
        *,
        summary: str,
        description: str = "",
        labels: _collections_abc.Iterable[str] | None = None,
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
        )

        return new_request
