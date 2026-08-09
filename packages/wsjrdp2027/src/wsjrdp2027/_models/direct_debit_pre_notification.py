"""Thin wrapper around a SEPA direct-debit pre-notification row.

:class:`DirectDebitPreNotification` mirrors :class:`wsjrdp2027.Person`: it is a
thin wrapper around a ``dict`` (optionally backed by a ``pandas`` DataFrame row)
that exposes the columns of a ``wsjrdp_direct_debit_pre_notifications`` row as
attributes / items. It only holds and exposes data; querying and attaching the
rows to people is left to the caller.
"""

from __future__ import annotations

import decimal as _decimal
import math as _math
import typing as _typing

from .. import _weakref_util


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc

    import pandas as _pandas


# Columns of a ``wsjrdp_direct_debit_pre_notifications`` row. Documentation only;
# the actual values live in ``self._data``.
PRE_NOTIFICATION_COLUMNS = (
    "id",
    "created_at",
    "updated_at",
    "payment_initiation_id",
    "direct_debit_payment_info_id",
    "subject_id",
    "subject_type",
    "author_id",
    "author_type",
    "try_skip",
    "payment_status",
    "email_from",
    "email_to",
    "email_cc",
    "email_bcc",
    "email_reply_to",
    "dbtr_name",
    "dbtr_iban",
    "dbtr_bic",
    "dbtr_address",
    "amount_currency",
    "amount_cents",
    "pre_notified_amount_cents",
    "debit_sequence_type",
    "collection_date",
    "mandate_id",
    "mandate_date",
    "description",
    "comment",
    "endtoend_id",
    "payment_role",
    "creditor_id",
    "cdtr_name",
    "cdtr_iban",
    "cdtr_bic",
    "cdtr_address",
    "additional_info",
)


class DirectDebitPreNotification:
    """A single ``wsjrdp_direct_debit_pre_notifications`` row as an object."""

    _KEEP_NAN_KEYS = {"amount_cents", "pre_notified_amount_cents"}

    _df = _weakref_util.OptionalWeakrefAttr["_pandas.DataFrame"]()
    _df_ref: _pandas.DataFrame | None
    _index = None
    _row: _pandas.Series | None = None
    _data: dict[str, _typing.Any] = _typing.cast(dict, None)
    _cls_keys: frozenset[str]
    _data_keys: frozenset[str] = frozenset([])

    # Documented data keys (values live in ``self._data``).
    id: int
    payment_initiation_id: int
    subject_id: int
    subject_type: str
    payment_status: str
    amount_cents: int
    amount_currency: str
    collection_date: _typing.Any
    endtoend_id: str | None
    debit_sequence_type: str

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

    def get_pandas_df(self) -> _pandas.DataFrame:
        if (dataframe := self._df) is not None:
            return dataframe
        else:
            raise RuntimeError(
                "This DirectDebitPreNotification object has no underlying "
                "Pandas dataframe"
            )

    def get_pandas_series(self) -> _pandas.Series:
        if (row := self._row) is not None:
            return row
        elif self._df is None or self._index is None:
            raise RuntimeError(
                "This DirectDebitPreNotification object has no underlying "
                "Pandas dataframe row"
            )
        else:
            return self._df.iloc[self._index]

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
        if name == "_data" or name in DirectDebitPreNotification._cls_keys:
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
    def amount(self) -> _decimal.Decimal:
        """:attr:`amount_cents` as a Decimal amount of the currency's main unit."""
        return _decimal.Decimal(self.amount_cents) / 100

    @property
    def pre_notified_amount(self) -> _decimal.Decimal | None:
        cents = self.get("pre_notified_amount_cents")
        return None if cents is None else _decimal.Decimal(cents) / 100

    @property
    def is_pre_notified(self) -> bool:
        return self.get("payment_status") == "pre_notified"

    @property
    def is_skipped(self) -> bool:
        return self.get("payment_status") == "skipped"


DirectDebitPreNotification._cls_keys = frozenset(
    DirectDebitPreNotification.__dict__.keys()
)


def iter_direct_debit_pre_notifications_dataframe(
    df: _pandas.DataFrame,
) -> _collections_abc.Iterator[DirectDebitPreNotification]:
    for idx, row in df.iterrows():
        yield DirectDebitPreNotification.from_pandas_row(row, dataframe=df, index=idx)
