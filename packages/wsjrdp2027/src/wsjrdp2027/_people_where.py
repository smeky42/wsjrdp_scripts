from __future__ import annotations

import collections.abc as _collections_abc
import dataclasses as _dataclasses
import datetime as _datetime
import logging as _logging
import typing as _typing


if _typing.TYPE_CHECKING:
    from . import _role


_LOGGER = _logging.getLogger(__name__)


@_dataclasses.dataclass(kw_only=True)
class PeopleWhere:
    exclude_deregistered: bool = True
    role: tuple[_role.WsjRole, ...] | None = None
    status: _collections_abc.Sequence[str] | None = None
    id: _collections_abc.Sequence[int] | None = None
    primary_group_id: _collections_abc.Sequence[int] | None = None
    early_payer: bool | None = None
    max_print_at: _datetime.date | None = None
    fee_rules: tuple[str, ...] = ("active",)

    def __init__(
        self,
        *,
        exclude_deregistered: bool = True,
        role: str
        | _role.WsjRole
        | _collections_abc.Iterable[str | _role.WsjRole]
        | None = None,
        status: str | _collections_abc.Iterable[str] | None = None,
        id: str | int | _collections_abc.Iterable[str | int] | None = None,
        primary_group_id: str
        | int
        | _collections_abc.Sequence[str | int]
        | None = None,
        early_payer: bool | None = None,
        max_print_at: _datetime.date | str | None = None,
        fee_rules: str | _collections_abc.Iterable[str] = "active",
    ) -> None:
        from . import _role, _util

        if exclude_deregistered is not None:
            self.exclude_deregistered = exclude_deregistered
        if role is not None:
            if isinstance(role, (str, _role.WsjRole)):
                self.role = (_role.WsjRole.from_any(role),)
            else:
                self.role = tuple(_role.WsjRole.from_any(r) for r in role)
        if status is not None:
            self.status = _util.to_str_list(status)
        if id is not None:
            self.id = _util.to_int_list(id)
        if primary_group_id is not None:
            self.primary_group_id = _util.to_int_list(primary_group_id)
        if early_payer is not None:
            self.early_payer = early_payer
        if max_print_at is not None:
            self.max_print_at = _util.to_date(max_print_at)
        if isinstance(fee_rules, str):
            self.fee_rules = (fee_rules,)
        else:
            self.fee_rules = tuple(fee_rules)

    @classmethod
    def from_dict(cls, d: dict | None, /) -> _typing.Self:
        d = d.copy() if d else {}
        return cls(**d)

    def to_dict(self) -> dict:
        def to_out(elts: _collections_abc.Sequence | None, map=None):
            if elts is None:
                return None
            if map:
                elts = [map(item) for item in elts]
            if len(elts) == 1:
                return elts[0]
            else:
                return elts

        def iso_or_none(d: _datetime.date | _datetime.datetime | None) -> str | None:
            if d is not None:
                return d.isoformat()
            else:
                return None

        if self.fee_rules and list(self.fee_rules) != ["active"]:
            fee_rules = list(self.fee_rules)
        else:
            fee_rules = None

        d = {
            "exclude_deregistered": self.exclude_deregistered,
            "role": to_out(self.role, map=str),
            "status": to_out(self.status),
            "id": to_out(self.id),
            "primary_group_id": to_out(self.primary_group_id),
            "early_payer": self.early_payer,
            "max_print_at": iso_or_none(self.max_print_at),
            "fee_rules": fee_rules,
        }
        return {k: v for k, v in d.items() if v is not None}

    def as_where_condition(self, *, people_table: str = "people") -> str:
        from ._util import combine_where, in_expr

        where = ""
        if self.exclude_deregistered:
            where = combine_where(
                where,
                f"{people_table}.status NOT IN ('deregistration_noted', 'deregistered')",
            )
        if self.role is not None:
            payment_roles = []
            for role in self.role:
                payment_roles.extend(
                    [role.regular_payer_payment_role, role.early_payer_payment_role]
                )
            where = combine_where(
                where, in_expr(f"{people_table}.payment_role", payment_roles)
            )
        if self.id is not None:
            where = combine_where(where, in_expr(f"{people_table}.id", self.id))
        if self.primary_group_id is not None:
            where = combine_where(
                where,
                in_expr(f"{people_table}.primary_group_id", self.primary_group_id),
            )
        if self.status is not None:
            where = combine_where(where, in_expr(f"{people_table}.status", self.status))
        if self.early_payer is not None:
            early_payer_cond = (
                f"{people_table}.early_payer = TRUE"
                if self.early_payer
                else f"({people_table}.early_payer = FALSE OR {people_table}.early_payer IS NULL)"
            )
            where = combine_where(where, early_payer_cond)
        if self.max_print_at is not None:
            where = combine_where(
                where, f"{people_table}.print_at <= '{self.max_print_at.isoformat()}'"
            )
        return where
