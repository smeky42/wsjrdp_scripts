from __future__ import annotations

import typing as _typing

from . import _weakref_util


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc

    import pandas as _pandas


class Person:
    _df = _weakref_util.OptionalWeakrefAttr["_pandas.DataFrame"]()
    _index = None
    _data: dict[str, _typing.Any] = _typing.cast(dict, None)
    _cls_keys: frozenset[str]
    _data_keys: frozenset[str] = frozenset([])

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
            self._index = index
        return self

    id: int
    primary_group_id: int
    role_id_name: str

    def __getattr__(self, key, /):
        data = self.__dict__.get("_data", {})
        try:
            return data[key]
        except KeyError:
            raise AttributeError(name=key, obj=self) from None

    def get(self, key, default=None, /):
        return self._data.get(key, default)

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
        return self._data[key]

    def __str__(self) -> str:
        cls_name = self.__class__.__qualname__
        args = [f"{k}={v!r}" for k, v in self._data.items()]
        return f"{cls_name}({', '.join(args)})"

    @property
    def short_role_name(self) -> str | None:
        payment_role = self._data.get("payment_role")
        if payment_role:
            return payment_role.short_role_name
        else:
            match self.primary_group_id:
                case 1:
                    return "CMT"
                case 2:
                    return "UL"
                case 3:
                    return "YP"
                case 4 | 45:
                    return "IST"
                case _:
                    return None

    @property
    def is_bmt(self) -> bool:
        return self.short_role_name == "IST" and self.primary_group_id == 45

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
            case _:
                return None

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
            case "UL" | "IST" | "YP":
                return f"wsj27-{self.id}@worldscoutjamboree.de"
            case _:
                return None


Person._cls_keys = frozenset(Person.__dict__.keys())


def iter_people_dataframe(
    df: _pandas.DataFrame,
) -> _collections_abc.Iterator[Person]:
    for row in df.itertuples():
        yield Person.from_pandas_row_tuple(row, dataframe=df)
