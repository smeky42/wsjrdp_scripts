from __future__ import annotations

import collections.abc as _collections_abc
import dataclasses as _dataclasses
import datetime as _datetime
import logging as _logging
import typing as _typing

from . import _types


if _typing.TYPE_CHECKING:
    from . import _role


_LOGGER = _logging.getLogger(__name__)


_StrOrIterable = str | _collections_abc.Iterable[str]
_StrOrNullIterable = (
    str
    | bool
    | _types.NullOrNotType
    | _collections_abc.Iterable[str | bool | _types.NullOrNotType]
)
_StrIntOrIterable = str | int | bool | _collections_abc.Iterable[str | int | bool]


@_dataclasses.dataclass(kw_only=True)
class PeopleWhere:
    exclude_deregistered: bool | None = None
    role: tuple[_role.WsjRole, ...] | None = None
    status: _collections_abc.Sequence[str | _types.NullOrNotType] | None = None
    exclude_status: _collections_abc.Sequence[str | _types.NullOrNotType] | None = None
    sepa_status: _collections_abc.Sequence[str | _types.NullOrNotType] | None = None
    exclude_sepa_status: (
        _collections_abc.Sequence[str | _types.NullOrNotType] | None
    ) = None
    id: _collections_abc.Sequence[int] | None = None
    exclude_id: _collections_abc.Sequence[int] | None = None
    primary_group_id: _collections_abc.Sequence[int] | None = None
    exclude_primary_group_id: _collections_abc.Sequence[int] | None = None
    early_payer: bool | None = None
    max_print_at: _datetime.date | None = None
    fee_rules: tuple[str, ...] = ("active",)
    unit_code: _collections_abc.Sequence[str | _types.NullOrNotType] | None = None
    exclude_unit_code: _collections_abc.Sequence[str | _types.NullOrNotType] | None = (
        None
    )
    tag: _collections_abc.Sequence[str] | None = None
    exclude_tag: _collections_abc.Sequence[str] | None = None
    not_: _typing.Self | None = None
    or_: _collections_abc.Sequence[_typing.Self] = ()
    and_: _collections_abc.Sequence[_typing.Self] = ()

    def __init__(
        self,
        *,
        exclude_deregistered: bool | None = None,
        role: str
        | _role.WsjRole
        | _collections_abc.Iterable[str | _role.WsjRole]
        | None = None,
        status: _StrOrNullIterable | None = None,
        exclude_status: _StrOrNullIterable | None = None,
        sepa_status: _StrOrNullIterable | None = None,
        exclude_sepa_status: _StrOrNullIterable | None = None,
        id: _StrIntOrIterable | None = None,
        exclude_id: str | int | _collections_abc.Iterable[str | int] | None = None,
        primary_group_id: _StrIntOrIterable | None = None,
        exclude_primary_group_id: _StrIntOrIterable | None = None,
        early_payer: bool | None = None,
        max_print_at: _datetime.date | str | None = None,
        fee_rules: _StrOrIterable = "active",
        unit_code: _StrOrNullIterable | None = None,
        exclude_unit_code: _StrOrNullIterable | None = None,
        tag: _StrOrIterable | None = None,
        exclude_tag: _StrOrIterable | None = None,
        not_: _typing.Self | None = None,
        or_: _collections_abc.Iterable[_typing.Self] | _typing.Self | None = None,
        and_: _collections_abc.Iterable[_typing.Self] | _typing.Self | None = None,
    ) -> None:
        from . import _role, _util

        if not_ is not None:
            self.not_ = not_
        if or_ is not None:
            if isinstance(or_, PeopleWhere):
                or_ = [or_]
            self.or_ = tuple(or_)
        if and_ is not None:
            if isinstance(and_, PeopleWhere):
                and_ = [and_]
            self.and_ = tuple(and_)

        if exclude_deregistered is not None:
            self.exclude_deregistered = exclude_deregistered
        if role is not None:
            if isinstance(role, (str, _role.WsjRole)):
                self.role = (_role.WsjRole.from_any(role),)
            else:
                self.role = tuple(_role.WsjRole.from_any(r) for r in role)
        self.status = _to_str_or_null_list(status)
        self.exclude_status = _to_str_or_null_list(exclude_status)
        self.sepa_status = _to_str_or_null_list(sepa_status)
        self.exclude_sepa_status = _to_str_or_null_list(exclude_sepa_status)
        self.id = _util.to_int_list_or_none(id)
        self.exclude_id = _util.to_int_list_or_none(exclude_id)
        self.primary_group_id = _util.to_int_list_or_none(primary_group_id)
        self.exclude_primary_group_id = _util.to_int_list_or_none(
            exclude_primary_group_id
        )
        if early_payer is not None:
            self.early_payer = early_payer
        if max_print_at is not None:
            self.max_print_at = _util.to_date(max_print_at)
        if isinstance(fee_rules, str):
            self.fee_rules = (fee_rules,)
        else:
            self.fee_rules = tuple(fee_rules)
        self.unit_code = _to_str_or_null_list(unit_code)
        self.exclude_unit_code = _to_str_or_null_list(exclude_unit_code)
        self.tag = _util.to_str_list_or_none(tag)
        self.exclude_tag = _util.to_str_list_or_none(exclude_tag)

    @classmethod
    def from_dict(cls, d: dict | None, /) -> _typing.Self:
        d = d.copy() if d else {}
        if not_dict := d.pop("not", None):
            d["not_"] = cls.from_dict(not_dict)
        if and_list := d.pop("and", None):
            if isinstance(and_list, dict):
                d["and_"] = [cls.from_dict(and_list)]
            else:
                d["and_"] = [cls.from_dict(d) for d in and_list]
        if or_list := d.pop("or", None):
            if isinstance(or_list, dict):
                d["or_"] = [cls.from_dict(or_list)]
            else:
                d["or_"] = [cls.from_dict(d) for d in or_list]
        return cls(**d)

    def to_dict(self) -> dict:
        def null_or_not_to_bool(x):
            if isinstance(x, _types.NullOrNotType):
                return x.bool_value
            else:
                return x

        def to_out(elts: _collections_abc.Sequence | None, map=None):
            if elts is None:
                return None
            elts = [null_or_not_to_bool(item) for item in elts]
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

        def recursive_to_dict(obj):
            if not obj:
                return None
            elif isinstance(obj, PeopleWhere):
                return obj.to_dict()
            elif isinstance(obj, (list, tuple)):
                if len(obj) == 1:
                    return obj[0].to_dict()
                else:
                    return [x.to_dict() for x in obj]
            else:
                raise RuntimeError

        if self.fee_rules and list(self.fee_rules) != ["active"]:
            fee_rules = list(self.fee_rules)
        else:
            fee_rules = None

        regular_to_out_keys = [
            "status",
            "sepa_status",
            "id",
            "primary_group_id",
            "unit_code",
            "tag",
        ]
        exclude_to_out_keys = [f"exclude_{k}" for k in regular_to_out_keys]
        to_out_keys = regular_to_out_keys + exclude_to_out_keys

        d = {
            "exclude_deregistered": self.exclude_deregistered,
            "role": to_out(self.role, map=str),
            "early_payer": self.early_payer,
            "max_print_at": iso_or_none(self.max_print_at),
            "fee_rules": fee_rules,
            "not": recursive_to_dict(self.not_),
            "or": recursive_to_dict(self.or_),
            "and": recursive_to_dict(self.and_),
            **{k: to_out(getattr(self, k, None)) for k in to_out_keys},
        }
        return {k: v for k, v in d.items() if v is not None}

    __to_dict__ = to_dict

    def __repr__(self) -> str:
        """Represenation of this people where object.

        >>> PeopleWhere()
        PeopleWhere()
        >>> PeopleWhere(id=3)
        PeopleWhere(id=[3])
        """
        d = {
            key: val
            for fld in _dataclasses.fields(self)
            if (val := getattr(self, (key := fld.name), None)) is not None
        }
        if list(d.get("fee_rules", [])) == ["active"]:
            d.pop("fee_rules", None)
        for k in ["and_", "or_"]:
            if not d.get(k):
                d.pop(k, None)
        args = [f"{key}={val!r}" for key, val in d.items()]
        return f"{self.__class__.__qualname__}({', '.join(args)})"

    __str__ = __repr__

    def as_where_condition(self, *, people_table: str = "people") -> str:
        from . import _util
        from ._util import combine_where, in_expr, not_in_expr

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

        for key in ["id", "primary_group_id", "sepa_status", "status", "unit_code"]:
            val = getattr(self, key, None)
            if val is not None:
                where = combine_where(where, in_expr(f"{people_table}.{key}", val))
            exclude_key = f"exclude_{key}"
            exclude_val = getattr(self, exclude_key, None)
            if exclude_val is not None:
                where = combine_where(
                    where, not_in_expr(f"{people_table}.{key}", exclude_val)
                )
        for key, col_name in [("tag", "tag_list")]:
            val = getattr(self, key, None)
            if val is not None:
                where = combine_where(
                    where, _util.all_in_array_expr(f"{people_table}.{col_name}", *val)
                )
            exclude_key = f"exclude_{key}"
            exclude_val = getattr(self, exclude_key, None)
            if exclude_val is not None:
                where = combine_where(
                    where,
                    _util.all_in_array_expr(
                        f"{people_table}.{col_name}",
                        *exclude_val,
                        op="<>",
                        array_comp_func="ALL",
                    ),
                )

        if self.not_ is not None:
            not_where = self.not_.as_where_condition(people_table=people_table)
            if not_where:
                where = combine_where(
                    where,
                    f"NOT ({not_where})",
                )
        if self.and_:
            and_where = combine_where(
                "",
                *(w.as_where_condition(people_table=people_table) for w in self.and_),
                op="AND",
            )
            if and_where:
                where = combine_where(where, f"({and_where})")
        if self.or_:
            or_where = combine_where(
                "",
                *(w.as_where_condition(people_table=people_table) for w in self.or_),
                op="OR",
            )
            if or_where:
                where = combine_where(where, f"({or_where})")

        return where


class PeopleQueryDict(_typing.TypedDict, total=False):
    where: PeopleWhere | None
    email_only_where: PeopleWhere | None
    limit: int | None
    now: _datetime.datetime | None
    collection_date: _datetime.date | None


@_dataclasses.dataclass(kw_only=True)
class PeopleQuery:
    where: PeopleWhere | None = None
    email_only_where: PeopleWhere | None = None
    limit: int | None = None
    now: _datetime.datetime
    collection_date: _datetime.date | None = None

    def __init__(
        self,
        *,
        where: PeopleWhere | dict | None = None,
        email_only_where: PeopleWhere | dict | None = None,
        limit: int | None = None,
        now: _datetime.datetime | _datetime.date | str | int | float | None = None,
        collection_date: _datetime.date | str | None = None,
    ) -> None:
        from . import _util

        if isinstance(where, dict):
            where = PeopleWhere.from_dict(where)
        if isinstance(email_only_where, dict):
            email_only_where = PeopleWhere.from_dict(email_only_where)
        self.where = where
        self.email_only_where = email_only_where
        self.limit = int(limit) if (isinstance(limit, (int, float)) or limit) else None
        self.now = _util.to_datetime(now)
        self.collection_date = _util.to_date_or_none(collection_date)

    def __to_dict__(self) -> dict[str, _typing.Any]:
        def _map(k, v):
            if isinstance(v, PeopleWhere):
                return v.__to_dict__()
            else:
                return v

        field_names = [f.name for f in _dataclasses.fields(self)]
        d = {
            k: _map(k, v)
            for k in field_names
            if (v := getattr(self, k, None)) is not None
        }
        return d

    def replace(self, **kwargs: _typing.Unpack[PeopleQueryDict]) -> _typing.Self:
        import copy

        obj = copy.deepcopy(self)
        for k, v in kwargs.items():
            if v is not None:
                setattr(obj, k, v)
        return obj


_T = _typing.TypeVar("_T")
_R = _typing.TypeVar("_R")


def _to_list(
    *args: _T | _collections_abc.Iterable[_T],
    map: _collections_abc.Callable[[_T], _R] = str,
) -> list[_R]:
    result = []
    for arg in args:
        if arg is None:
            continue
        if isinstance(arg, str):
            result.append(map(_typing.cast(_T, arg)))
        elif isinstance(arg, _collections_abc.Iterable):
            result.extend(map(x) for x in arg)
        else:
            result.append(map(arg))
    return result


def _to_str_or_null_type(x: _typing.Any) -> str | _types.NullOrNotType:
    if x is None:
        return _types.NULL
    elif isinstance(x, bool):
        return _types.NOT_NULL if x else _types.NULL
    else:
        return str(x)


def _to_str_or_null_list(*args) -> list[str | _types.NullOrNotType] | None:
    return _to_list(*args, map=_to_str_or_null_type) or None
