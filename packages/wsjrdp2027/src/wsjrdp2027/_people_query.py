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


class ArrayMatchDict(_typing.TypedDict, total=False):
    expr: str | _collections_abc.Iterable[str]
    op: str
    func: str


@_dataclasses.dataclass(kw_only=True)
class ArrayMatchExpr:
    expr: list[str]
    _op: str | None = None
    _func: str | None = None

    default_op: str = "="
    default_func: str = "ANY"

    def __init__(
        self,
        *,
        expr: str | _collections_abc.Iterable[str],
        op: str | None = None,
        func: str | None = None,
        default_op: str | None = None,
        default_func: str | None = None,
    ) -> None:
        if isinstance(expr, str):
            self.expr = [expr]
        else:
            self.expr = list(expr)
        if default_op:
            self.default_op = default_op.strip().upper()
        if default_func:
            self.default_func = default_func.strip().upper()
        if op:
            self._op = op.strip().upper()
        if func:
            self._func = func.strip().upper()

    @property
    def op(self) -> str:
        return self._op or self.default_op

    @property
    def func(self) -> str:
        return self._func or self.default_func

    @classmethod
    def normalize(
        cls,
        obj: _ArrayMatchLike,
        *,
        default_op: str = "=",
        default_func: str = "ANY",
        negate: bool = False,
    ) -> ArrayMatchExpr:
        maybe_negate = lambda e: e.negate() if negate else e
        if isinstance(obj, ArrayMatchExpr):
            return maybe_negate(obj)
        elif isinstance(obj, dict):
            d: dict[str, _typing.Any] = obj.copy()  # type: ignore
            d.setdefault("op", default_op)
            d.setdefault("func", default_func)
            return maybe_negate(
                cls(**d, default_op=default_op, default_func=default_func)
            )
        elif isinstance(obj, str):
            return maybe_negate(
                cls(expr=obj, default_op=default_op, default_func=default_func)
            )
        else:
            return maybe_negate(
                cls(expr=list(obj), default_op=default_op, default_func=default_func)
            )

    @classmethod
    def normalize_or_none(
        cls,
        obj: _ArrayMatchLike | None,
        *,
        default_op: str = "=",
        default_func: str = "ANY",
        negate: bool = False,
    ) -> ArrayMatchExpr | None:
        if obj is None:
            return None
        else:
            return cls.normalize(
                obj, default_op=default_op, default_func=default_func, negate=negate
            )

    _FUNC_NEG = {
        None: None,
        "ANY": "ALL",
        "SOME": "ALL",
        "ALL": "ANY",
    }

    def negate(self) -> _typing.Self:
        import copy

        from . import _util

        negate_op = lambda op: _util.negate_sql_comparison_op(op)
        negate_func = lambda func: self._FUNC_NEG[func]

        return self.__class__(
            expr=copy.copy(self.expr),
            op=negate_op(self._op),
            func=negate_func(self._func),
            default_op=negate_op(self.default_op),
            default_func=negate_func(self.default_func),
        )

    def as_where_condition(self, *, array: str) -> str:
        from . import _util

        if not self.expr:
            return ""
        return _util.all_in_array_expr(
            array, *self.expr, op=self.op, join_op="AND", array_comp_func=self.func
        )

    def to_out(self) -> ArrayMatchDict | list[str] | str | None:
        if not self.expr:
            return None
        expr = self.expr[0] if len(self.expr) == 1 else self.expr[:]
        if (
            self.op in (self.default_op, None)  #
            and self.func in (self.default_func, None)
        ):
            return expr
        else:
            d: ArrayMatchDict = {"expr": expr}
            if self.op and (self.op != self.default_op):
                d["op"] = self.op
            if self.func and (self.func != self.default_func):
                d["func"] = self.func
            return d


_ArrayMatchLike = ArrayMatchExpr | ArrayMatchDict | str | _collections_abc.Iterable[str]


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
    tag: ArrayMatchExpr | None = None
    exclude_tag: ArrayMatchExpr | None = None
    note: ArrayMatchExpr | None = None
    exclude_note: ArrayMatchExpr | None = None
    raw_sql: str | None = None
    not_: PeopleWhere | None = None
    or_: _collections_abc.Sequence[PeopleWhere] = ()
    and_: _collections_abc.Sequence[PeopleWhere] = ()

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
        tag: _ArrayMatchLike | None = None,
        exclude_tag: _ArrayMatchLike | None = None,
        note: _ArrayMatchLike | None = None,
        exclude_note: _ArrayMatchLike | None = None,
        raw_sql: str | None = None,
        not_: PeopleWhere | None = None,
        or_: _collections_abc.Iterable[PeopleWhere] | PeopleWhere | None = None,
        and_: _collections_abc.Iterable[PeopleWhere] | PeopleWhere | None = None,
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
        self.tag = ArrayMatchExpr.normalize_or_none(tag)
        self.exclude_tag = ArrayMatchExpr.normalize_or_none(exclude_tag, negate=True)
        self.note = ArrayMatchExpr.normalize_or_none(note)
        self.exclude_note = ArrayMatchExpr.normalize_or_none(exclude_note, negate=True)
        self.raw_sql = raw_sql or None

    @classmethod
    def normalize(cls, obj: _PeopleWhereLike, /) -> PeopleWhere:
        if isinstance(obj, PeopleWhere):
            return obj
        elif isinstance(obj, dict):
            return cls.from_dict(obj)
        else:
            return cls(raw_sql=str(obj))

    @classmethod
    def normalize_or_none(cls, obj: _PeopleWhereLike | None, /) -> PeopleWhere | None:
        if not obj:
            return None
        else:
            return cls.normalize(obj)

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

        def to_out(elts: _collections_abc.Sequence | ArrayMatchExpr | None, map=None):
            if elts is None:
                return None
            elif isinstance(elts, ArrayMatchExpr):
                return elts.to_out()
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
            "note",
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
            "raw_sql": self.raw_sql or None,
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
                else f"COALESCE({people_table}.early_payer, FALSE) = FALSE"
            )
            where = combine_where(where, early_payer_cond)
        if self.max_print_at is not None:
            where = combine_where(
                where, f"{people_table}.print_at <= '{self.max_print_at.isoformat()}'"
            )

        for key in ["id", "primary_group_id", "sepa_status", "status", "unit_code"]:
            expr = f"{people_table}.{key}"
            if key == "sepa_status":
                expr = f"COALESCE({expr}, 'ok')"
            val = getattr(self, key, None)
            if val is not None:
                where = combine_where(where, in_expr(expr, val))
            exclude_key = f"exclude_{key}"
            exclude_val = getattr(self, exclude_key, None)
            if exclude_val is not None:
                where = combine_where(where, not_in_expr(expr, exclude_val))
        for key, col_name in [("tag", "tag_list"), ("note", "note_list")]:
            val: ArrayMatchExpr | None = getattr(self, key, None)
            if val is not None:
                where = combine_where(
                    where, val.as_where_condition(array=f"{people_table}.{col_name}")
                )
            exclude_key = f"exclude_{key}"
            exclude_val = getattr(self, exclude_key, None)
            if exclude_val is not None:
                where = combine_where(
                    where,
                    exclude_val.as_where_condition(array=f"{people_table}.{col_name}"),
                )
        if self.raw_sql is not None:
            where = combine_where(where, self.raw_sql)
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


_PeopleWhereLike = PeopleWhere | dict | str


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
    include_sepa_mail_in_mailing_to: bool | None = None

    def __init__(
        self,
        *,
        where: PeopleWhere | dict | str | None = None,
        email_only_where: PeopleWhere | dict | None = None,
        limit: int | None = None,
        now: _datetime.datetime | _datetime.date | str | int | float | None = None,
        collection_date: _datetime.date | str | None = None,
        include_sepa_mail_in_mailing_to: bool | None = None,
    ) -> None:
        from . import _util

        self.where = PeopleWhere.normalize_or_none(where)
        self.email_only_where = PeopleWhere.normalize_or_none(email_only_where)
        self.limit = int(limit) if (isinstance(limit, (int, float)) or limit) else None
        self.now = _util.to_datetime(now)
        self.collection_date = _util.to_date_or_none(collection_date)
        self.include_sepa_mail_in_mailing_to = include_sepa_mail_in_mailing_to

    @property
    def today(self) -> _datetime.date:
        return self.now.date()

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
        for f in _dataclasses.fields(self):
            key = f.name
            val = getattr(self, key)
            if key not in kwargs:
                kwargs[key] = val
        return self.__class__(**kwargs)


_T = _typing.TypeVar("_T")
_R = _typing.TypeVar("_R")


def _to_list(
    *args: _T | _collections_abc.Iterable[_T],
    map: _collections_abc.Callable[[_T], _R] = str,  # type: ignore
) -> list[_R]:
    result = []
    for arg in args:
        if arg is None:
            continue
        if isinstance(arg, str):
            result.append(map(_typing.cast(_T, arg)))
        elif isinstance(arg, _collections_abc.Iterable):
            result.extend(map(x) for x in arg)  # ty: ignore
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
