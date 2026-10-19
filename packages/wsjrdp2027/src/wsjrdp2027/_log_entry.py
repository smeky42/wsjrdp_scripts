"""Log entries of the Hitobito wagon, written into ``versions``.

The wagon shows a row of ``versions`` whose ``event`` starts with
``wsjrdp_`` by its own rules (``Wsjrdp::LogEvents`` and
``doc/wsjrdp/log_entries.md`` in the wagon): its data lies in the jsonb
column ``wsjrdp_data``, metadata of any entry in ``additional_info``. It shows
in the log of its ``main`` (a person's or a group's log page), whatever its
``item`` is.

A note (event ``wsjrdp_note``) has ``category`` and ``message`` in its data and
any further keys; the log shows ``<category label>: <message>``. Nothing is
checked -- a category the wagon has no label for is shown as it is.

Write the entries in the transaction of the change they log, e.g.::

    wsjrdp2027.insert_log_entries(conn, [
        wsjrdp2027.LogEntry.note(
            main_id=person_id,
            category="pre_notification",
            message="Über den Lastschrifteinzug am 05.11.2026 informiert.",
            data={"collection_date": collection_date},
        )
    ])

The columns ``wsjrdp_data`` and ``additional_info`` exist from the wagon's
migration 20261020000100 on.
"""

from __future__ import annotations

import collections.abc as _collections_abc
import dataclasses as _dataclasses
import datetime as _datetime
import typing as _typing


if _typing.TYPE_CHECKING:
    from ._pg import PgConnectionLike


NOTE_EVENT = "wsjrdp_note"

# The author of an entry a script writes, as pg_insert_version has it.
ADMINISTRATOR_ID = 1


@_dataclasses.dataclass(frozen=True, kw_only=True)
class LogEntry:
    """One log entry: a row of ``versions``.

    ``item_type``/``item_id`` default to ``main_type``/``main_id``. ``data``
    becomes ``wsjrdp_data``; dates and datetimes in ``data`` and
    ``additional_info`` are written as ISO strings, everything else must be
    JSON-serializable. ``created_at`` defaults to the ``now`` of
    :func:`insert_log_entries`."""

    event: str
    main_id: int
    main_type: str = "Person"
    item_id: int | None = None
    item_type: str | None = None
    data: _collections_abc.Mapping[str, _typing.Any] = _dataclasses.field(
        default_factory=dict
    )
    additional_info: _collections_abc.Mapping[str, _typing.Any] = _dataclasses.field(
        default_factory=dict
    )
    whodunnit_id: int | None = ADMINISTRATOR_ID
    whodunnit_type: str = "Person"
    created_at: _datetime.datetime | None = None

    @classmethod
    def note(
        cls,
        *,
        main_id: int,
        category: str,
        message: str,
        data: _collections_abc.Mapping[str, _typing.Any] | None = None,
        **kwargs: _typing.Any,
    ) -> LogEntry:
        """A note: ``category`` and ``message`` over whatever ``data`` holds."""
        return cls(
            event=NOTE_EVENT,
            main_id=main_id,
            data={**(data or {}), "category": category, "message": message},
            **kwargs,
        )

    def to_row(self) -> dict[str, _typing.Any]:
        """The columns of the ``versions`` row."""
        row: dict[str, _typing.Any] = {
            "event": self.event,
            "item_type": self.item_type or self.main_type,
            "item_id": self.main_id if self.item_id is None else self.item_id,
            "main_type": self.main_type,
            "main_id": self.main_id,
            "whodunnit_type": self.whodunnit_type,
            "whodunnit": None if self.whodunnit_id is None else str(self.whodunnit_id),
            "wsjrdp_data": _jsonable(dict(self.data)),
            "additional_info": _jsonable(dict(self.additional_info)),
        }
        if self.created_at is not None:
            row["created_at"] = self.created_at
        return row


def insert_log_entries(
    conn: PgConnectionLike,
    entries: _collections_abc.Iterable[LogEntry],
    *,
    now: _datetime.datetime | _datetime.date | str | float | None = None,
) -> list[int]:
    """INSERT the entries into ``versions`` in one pipeline; returns their ids.

    Never commits: the entries belong to the transaction of the change they
    log. ``now`` is the ``created_at`` of an entry without one (see
    :func:`wsjrdp2027.pg_table_insertmany`)."""
    from ._pg import pg_table_insertmany

    rows = [entry.to_row() for entry in entries]
    return list(pg_table_insertmany(conn, "versions", rows, now=now).inserted_ids)


def _jsonable(value: _typing.Any) -> _typing.Any:
    """``value`` with every date and datetime as its ISO string."""
    if isinstance(value, (_datetime.date, _datetime.datetime)):
        return value.isoformat()
    if isinstance(value, _collections_abc.Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value
