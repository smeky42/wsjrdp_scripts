from __future__ import annotations

import dataclasses as _dataclasses
import typing as _typing


if _typing.TYPE_CHECKING:
    import string.templatelib as _string_templatelib

    import psycopg.sql as _psycopg_sql


@_dataclasses.dataclass(kw_only=True)
class Group:
    id: int
    parent_id: int | None = None
    short_name: str | None = None
    name: str
    type: str | None = None
    email: str | None = None
    description: str
    additional_info: dict

    def __post_init__(self):
        if self.additional_info is None:
            self.additional_info = {}

    @property
    def unit_code(self) -> str | None:
        return self.additional_info.get("unit_code")

    @property
    def group_code(self) -> str | None:
        return self.additional_info.get("group_code")

    @property
    def support_cmt_mail_addresses(self) -> list[str]:
        addr = self.additional_info.get("support_cmt_mail_addresses", [])
        return addr

    def __getitem__(self, key: str) -> _typing.Any:
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key) from None

    @classmethod
    def db_load_for_where(
        cls, conn, where: _psycopg_sql.Composable | _string_templatelib.Template
    ) -> _typing.Self:
        from . import _pg

        return cls(**_pg.pg_select_group_dict_for_where(conn, where=where))

    @classmethod
    def db_load_for_group_name(cls, conn, group_name: str) -> _typing.Self:
        return cls.db_load_for_where(
            conn,
            t'"name" = {group_name} OR "short_name" = {group_name} OR "additional_info"->>\'group_code\' = {group_name}',
        )

    @classmethod
    def db_load_for_group_id(cls, conn, group_id: int) -> _typing.Self:
        return cls.db_load_for_where(conn, t'"id" = {group_id}')

    @classmethod
    def db_load(
        cls, conn, group_arg: str | int, *, auto_group_id: int | None = None
    ) -> _typing.Self:
        import re

        if isinstance(group_arg, int):
            return cls.db_load_for_group_id(conn, group_arg)
        elif re.fullmatch("[0-9]+", str(group_arg)):
            return cls.db_load_for_group_id(conn, int(group_arg, base=10))
        elif group_arg == "auto":
            if auto_group_id is None:
                raise RuntimeError("group='auto' and auto_group_id=None not supported")
            return cls.db_load_for_group_id(conn, auto_group_id)
        else:
            return cls.db_load_for_group_name(conn, group_arg)
