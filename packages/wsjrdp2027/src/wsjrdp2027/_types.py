from __future__ import annotations

import typing as _typing


__all__ = [
    "MISSING",
    "MissingType",
    "NOT_NULL",
    "NULL",
    "SepaDirectDebitConfig",
]


class SepaDirectDebitConfig(_typing.TypedDict, total=False):
    name: str
    IBAN: str
    BIC: str
    creditor_id: str
    currency: str
    address_as_single_line: str


class NullOrNotType:
    def __init__(self, *, name: str, sql_literal: str, bool_value: bool) -> None:
        self._name = name
        self._sql_literal = sql_literal
        self._bool_value = bool_value

    def __repr__(self) -> str:
        return self._name

    def __str__(self) -> str:
        return self._name

    @property
    def sql_literal(self) -> str:
        return self._sql_literal

    @property
    def bool_value(self) -> bool:
        return self._bool_value

    def __copy__(self) -> _typing.Self:
        return self

    def __deepcopy__(self, memo) -> _typing.Self:
        return self


NULL = NullOrNotType(name="NULL", sql_literal="NULL", bool_value=False)
NOT_NULL = NullOrNotType(name="NOT_NULL", sql_literal="NOT NULL", bool_value=True)


class MissingType:
    def __copy__(self) -> _typing.Self:
        return self

    def __deepcopy__(self, memo) -> _typing.Self:
        return self

    def __repr__(self) -> str:
        return "MISSING"

    __str__ = __repr__


MISSING = MissingType()
