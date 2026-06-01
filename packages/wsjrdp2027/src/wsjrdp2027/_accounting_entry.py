import datetime as _datetime
import math as _math
import typing as _typing


if _typing.TYPE_CHECKING:
    from . import _pg


class AccountingEntry:
    _KEEP_NAN_KEYS = {"amount_cents"}

    _cls_keys: frozenset[str]
    _data: dict[str, _typing.Any] = _typing.cast(dict, None)
    _data_keys: frozenset[str] = frozenset([])

    id: int
    subject_id: int
    author_id: int
    amount_cents: int
    amount_currency: str
    description: str
    comment: str | None
    booking_date: _datetime.date
    value_date: _datetime.date

    def __init__(self, **kwargs) -> None:
        self._index = None
        self._data = kwargs.copy()
        self._data_keys = frozenset(self._data.keys())

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
        if name == "_data" or name in self.__class__._cls_keys:
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

    __repr__ = __str__

    def __normalize_val(self, key, val) -> _typing.Any:
        if (
            (key in self._KEEP_NAN_KEYS)
            or not isinstance(val, float)
            or not _math.isnan(val)
        ):
            return val
        else:
            return None

    @property
    def amount_de(self) -> str:
        from . import _util

        return _util.format_cents_as_eur_de(self.amount_cents)

    @property
    def short_dbtr(self) -> str:
        return ", ".join(
            filter(None, [self.dbtr_name, self.dbtr_address, self.dbtr_iban])
        )

    @classmethod
    def load_entries_for_where(
        cls, *, where, conn: _pg.ConnectionLike | None = None
    ) -> list[_typing.Self]:

        from psycopg.sql import SQL

        from . import _pg

        if isinstance(where, str):
            where = SQL(where)  # type: ignore

        results = _pg.pg_select_dict_rows(
            conn=conn, query=t"SELECT * FROM accounting_entries WHERE {where:q}"
        )
        return [cls(**d) for d in results]


AccountingEntry._cls_keys = frozenset(AccountingEntry.__dict__.keys())
