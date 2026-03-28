from __future__ import annotations

import datetime as _datetime
import math as _math
import typing as _typing

from . import _weakref_util


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc

    import pandas as _pandas
    import psycopg as _psycopg

    from . import _groups


class Person:
    _KEEP_NAN_KEYS = {"amount_paid_cents", "amount_unpaid_cents", "open_amount_cents"}

    _df = _weakref_util.OptionalWeakrefAttr["_pandas.DataFrame"]()
    _df_ref: _pandas.DataFrame | None
    _index = None
    _row: _pandas.Series | None = None
    _data: dict[str, _typing.Any] = _typing.cast(dict, None)
    _cls_keys: frozenset[str]
    _data_keys: frozenset[str] = frozenset([])
    _primary_group: _groups.Group | None = None

    def __init__(self, **kwargs) -> None:
        self._index = None
        self._data = kwargs.copy()
        self._data_keys = frozenset(self._data.keys())

    @classmethod
    def from_pandas_row_tuple(
        cls,
        data: tuple,
        *,
        columns: _collections_abc.Iterable[str] | None = None,
        dataframe: _pandas.DataFrame | None = None,
    ) -> _typing.Self:
        if columns is None:
            if dataframe is None:
                raise TypeError("One of 'columns' or 'dataframe' must be given")
            else:
                columns = dataframe.columns

        index = data[0]
        d = {x[0]: x[1] for x in zip(columns, data[1:])}
        self = cls(**d)
        if dataframe is not None:
            self._df = dataframe
            self._df_ref = dataframe
            self._index = index
        return self

    @classmethod
    def from_pandas_row(
        cls,
        row: _pandas.Series,
        *,
        dataframe: _pandas.DataFrame | None = None,
        index=None,
    ) -> _typing.Self:
        d = row.to_dict()
        self = cls(**d)  # type: ignore
        self._row = row
        if dataframe is not None and index is not None:
            self._df = dataframe
            self._df_ref = dataframe
            self._index = index
        return self

    id: int
    primary_group_id: int

    @property
    def df(self) -> _pandas.DataFrame:
        if (dataframe := self._df) is not None:
            return dataframe
        else:
            raise RuntimeError("This Person object has no underlying Pandas dataframe")

    @property
    def row(self) -> _pandas.Series:
        if (row := self._row) is not None:
            return row
        elif self._df is None or self._index is None:
            raise RuntimeError(
                "This Person object has no underlying Pandas dataframe row"
            )
        else:
            return self._df.iloc[self._index]  # type: ignore

    def __normalize_val(self, key, val) -> _typing.Any:
        if (
            (key in self._KEEP_NAN_KEYS)
            or not isinstance(val, float)
            or not _math.isnan(val)
        ):
            return val
        else:
            return None

    def __getattr__(self, key, /):
        data = self.__dict__.get("_data", {})
        try:
            val = data[key]
            return self.__normalize_val(key, val)
        except KeyError:
            raise AttributeError(name=key, obj=self) from None

    def get(self, key, default=None, /):
        val = self._data.get(key, default)
        return self.__normalize_val(key, val)

    def __setattr__(self, name: str, value: _typing.Any, /) -> None:
        if name == "_data" or name in Person._cls_keys:
            object.__setattr__(self, name, value)
        else:
            data = self.__dict__.get("_data", {})
            if name in data:
                data[name] = value
            else:
                object.__setattr__(self, name, value)

    def __getitem__(self, key, /):
        data = self.__dict__.get("_data", {})
        val = data[key]
        return self.__normalize_val(key, val)

    def __setitem__(self, key, value, /):
        data = self.__dict__.get("_data", {})
        data[key] = value

    def __str__(self) -> str:
        cls_name = self.__class__.__qualname__
        args = [f"{k}={v!r}" for k, v in self._data.items()]
        return f"{cls_name}({', '.join(args)})"

    @property
    def primary_group(self) -> _groups.Group:
        if self._primary_group is None:
            raise RuntimeError("Missing primary group object")
        return self._primary_group

    @primary_group.setter
    def primary_group(self, value: _groups.Group) -> None:
        assert value.id == self.primary_group_id
        self._primary_group = value

    @property
    def short_role_name(self) -> str | None:
        payment_role = self._data.get("payment_role")
        if payment_role:
            return payment_role.short_role_name
        else:
            match self.primary_group_id:
                case 1 | 47:
                    return "CMT"
                case 2 | 5:
                    return "UL"
                case 3 | 6:
                    return "YP"
                case 4 | 7 | 45:
                    return "IST"
                case 48:
                    return "EXT"
                case _:
                    return None

    @property
    def is_bmt(self) -> bool:
        return self.short_role_name == "IST" and self.primary_group_id == 45

    @property
    def is_cmt(self) -> bool:
        return self.short_role_name == "CMT"

    @property
    def is_ul(self) -> bool:
        return self.short_role_name == "UL"

    @property
    def unit_or_role(self) -> str | None:
        if self.is_ul:
            return self._primary_group.name if self._primary_group is not None else None
        else:
            return self.short_role_name

    @property
    def helpdesk_email(self) -> str:
        match self.short_role_name:
            case "YP" | "UL":
                return "unit-management@worldscoutjamboree.de"
            case "IST":
                return "ist@worldscoutjamboree.de"
            case _:
                return "info@worldscoutjamboree.de"

    @property
    def id_and_name(self) -> str:
        return _filtered_join(self.id, self.short_full_name)

    @property
    def role_id_name(self) -> str:
        return _filtered_join(self.short_role_name, self.id, self.short_full_name)

    @property
    def additional_info(self) -> dict[str, _typing.Any]:
        return self._data.get("additional_info", {})

    @property
    def wsjrdp_email_or_none(self) -> str | None:
        return self.additional_info.get("wsjrdp_email")

    @property
    def wsjrdp_email(self) -> str | None:
        from . import _util

        if email := self.wsjrdp_email_or_none:
            return email
        match self.short_role_name:
            case "CMT":
                username = _util.generate_mail_username(self.first_name, self.last_name)
                return f"{username}@worldscoutjamboree.de"
            case "UL":
                username = _util.generate_mail_username(self.first_name, self.last_name)
                return f"{username}@units.worldscoutjamboree.de"
            case "IST":
                subdomain = "bmt" if self.is_bmt else "ist"
                username = _util.generate_mail_username(self.first_name, self.last_name)
                return f"{username}@{subdomain}.worldscoutjamboree.de"
            case "EXT":
                return f"wsj27-{self.id}@worldscoutjamboree.de"
            case _:
                return None

    @property
    def wsjrdp_email_should_be_mailbox(self) -> bool:
        match self.short_role_name:
            case "CMT" | "UL":
                return True
            case _:
                return False

    @property
    def wsjrdp_email_is_mailbox(self) -> bool | None:
        return self.additional_info.get("wsjrdp_email_is_mailbox")

    @wsjrdp_email_is_mailbox.setter
    def wsjrdp_email_is_mailbox(self, value: bool | None) -> None:
        if value is None:
            self.additional_info.pop("wsjrdp_email_is_mailbox", None)
        else:
            additional_info = self._data.setdefault("additional_info", {})
            additional_info["wsjrdp_email_is_mailbox"] = value

    @property
    def moss_email_or_none(self) -> str | None:
        return self.additional_info.get("moss_email")

    @property
    def moss_email(self) -> str | None:
        from . import _util

        if email := self.moss_email_or_none:
            return email

        match self.short_role_name:
            case "CMT":
                username = _util.generate_mail_username(self.first_name, self.last_name)
                return f"{username}@worldscoutjamboree.de"
            case "UL" | "IST" | "YP" | "EXT":
                return f"wsj27-{self.id}@worldscoutjamboree.de"
            case _:
                return None

    @property
    def moss_email_expected_goto(self) -> str | None:
        match self.short_role_name:
            case "CMT":
                return None
            case "UL":
                return self.wsjrdp_email
            case "IST" | "BMT" | "YP":
                return self.email
            case _:
                raise RuntimeError(
                    f"Cannot determine moss_email_expected_goto for role {self.short_role_name}"
                )

    @property
    def deregistration_issue(self) -> str | None:
        return self.additional_info.get("deregistration_issue")

    @deregistration_issue.setter
    def deregistration_issue(self, value: str | None) -> None:
        if not value:
            self.additional_info.pop("deregistration_issue", None)
        else:
            additional_info = self._data.setdefault("additional_info", {})
            additional_info["deregistration_issue"] = value

    @property
    def keycloak_username(self) -> str | None:
        return self.additional_info.get("keycloak_username")

    @keycloak_username.setter
    def keycloak_username(self, value: str | None) -> None:
        if not value:
            self.additional_info.pop("keycloak_username", None)
        else:
            additional_info = self._data.setdefault("additional_info", {})
            additional_info["keycloak_username"] = value

    @property
    def moss_invited_at(self) -> _datetime.datetime | None:
        dt = self.additional_info.get("moss_invited_at")
        return _datetime.datetime.fromisoformat(dt) if dt else None

    @moss_invited_at.setter
    def moss_invited_at(
        self, value: _datetime.datetime | _datetime.date | str | float | int | None
    ) -> None:
        from ._util import to_datetime_or_none

        value = to_datetime_or_none(value)
        if not value:
            self.additional_info.pop("moss_invited_at", None)
        else:
            additional_info = self._data.setdefault("additional_info", {})
            additional_info["moss_invited_at"] = value.isoformat()


Person._cls_keys = frozenset(Person.__dict__.keys())


def iter_people_dataframe(
    df: _pandas.DataFrame,
) -> _collections_abc.Iterator[Person]:
    for idx, row in df.iterrows():
        yield Person.from_pandas_row(row, dataframe=df, index=idx)


def _filtered_join(*args, sep=" "):
    import math

    return sep.join(
        str(a)
        for a in args
        if not (
            a is None  # filter out None
            or (isinstance(a, float) and math.isnan(a))  # filter out NaN
        )
    )


def load_primary_groups_for_people(
    conn: _psycopg.Connection, *, people: list[Person]
) -> None:
    from . import _groups

    groups_list = _groups.Group.load_for_group_ids(
        conn, (p.primary_group_id for p in people)
    )
    groups = {g.id: g for g in groups_list}
    for p in people:
        p.primary_group = groups[p.primary_group_id]
