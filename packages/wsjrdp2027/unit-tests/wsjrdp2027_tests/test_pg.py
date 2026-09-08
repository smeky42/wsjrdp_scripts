from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import string.templatelib as _string_templatelib
import unittest.mock as _unittest_mock
import uuid as _uuid

import psycopg.sql as _psycopg_sql
import pytest
from wsjrdp2027 import _pg


class Test_as_identifier_str:
    @pytest.mark.parametrize("name", ["id", "number", "weird name", 'a"b', ""])
    def test_str_passthrough(self, name):
        assert _pg.as_identifier_str(name) == name

    @pytest.mark.parametrize("name", ["id", "number", "weird name", 'a"b', "select"])
    def test_identifier_roundtrip(self, name):
        assert _pg.as_identifier_str(_psycopg_sql.Identifier(name)) == name

    def test_multi_part_identifier_raises(self):
        with pytest.raises(TypeError, match="single-part"):
            _pg.as_identifier_str(_psycopg_sql.Identifier("schema", "table"))

    @pytest.mark.parametrize("bad", [42, None, _psycopg_sql.SQL("id")])
    def test_other_types_raise(self, bad):
        with pytest.raises(TypeError):
            _pg.as_identifier_str(bad)


class Test_table_identifier:
    """A table name must never reach a statement unquoted: only ``str`` and
    ``Identifier`` are accepted (defence in depth -- every caller today passes
    a ``str``)."""

    def test_str_becomes_quoted_identifier(self):
        got = _pg._table_identifier("people")
        assert isinstance(got, _psycopg_sql.Identifier)
        assert got.as_string() == '"people"'

    def test_identifier_passes_through(self):
        ident = _psycopg_sql.Identifier("people")
        assert _pg._table_identifier(ident) is ident

    def test_multi_part_identifier_passes_through(self):
        ident = _psycopg_sql.Identifier("public", "people")
        assert _pg._table_identifier(ident) is ident
        assert ident.as_string() == '"public"."people"'

    def test_hostile_str_is_quoted_not_executed(self):
        got = _pg._table_identifier('people"; DROP TABLE x; --')
        assert got.as_string() == '"people""; DROP TABLE x; --"'

    @pytest.mark.parametrize(
        "bad",
        [
            _psycopg_sql.SQL("people; DROP TABLE x"),
            _psycopg_sql.SQL("people"),
            _psycopg_sql.Literal("people"),
            42,
            None,
        ],
    )
    def test_other_types_raise(self, bad):
        with pytest.raises(TypeError, match="str or psycopg.sql.Identifier"):
            _pg._table_identifier(bad)


U1 = _uuid.UUID("11111111-1111-4111-8111-111111111111")
U2 = _uuid.UUID("22222222-2222-4222-8222-222222222222")


class Test_PgArray:
    """The value marker for NATIVE PostgreSQL ARRAY columns. The names are
    reached through ``wsjrdp2027._pg`` -- they are not re-exported (yet)."""

    def test_elements_are_normalized_and_stored_as_tuple(self):
        array = _pg.PgArray([str(U1), U2], _pg.ArrayElementType.UUID)
        assert array.elements == (U1, U2)
        assert isinstance(array.elements, tuple)

    def test_duplicates_collapse_keeping_the_first_occurrence(self):
        # The str and the UUID spelling of U1 are the SAME element; the
        # normalized form decides, and the FIRST position is kept.
        array = _pg.PgArray([str(U1), U2, U1, str(U2), U1], _pg.ArrayElementType.UUID)
        assert array.elements == (U1, U2)

    def test_empty_elements(self):
        assert _pg.PgArray([], _pg.ArrayElementType.TEXT).elements == ()

    @pytest.mark.parametrize("raw", [[str(U1), U2], (str(U1), U2), iter([str(U1), U2])])
    def test_any_iterable_is_accepted(self, raw):
        assert _pg.PgArray(raw, _pg.ArrayElementType.UUID).elements == (U1, U2)

    @pytest.mark.parametrize("raw", ["abc", b"abc", bytearray(b"abc")])
    def test_str_and_bytes_are_not_element_iterables(self, raw):
        with pytest.raises(TypeError, match="iterable of characters/bytes"):
            _pg.PgArray(raw, _pg.ArrayElementType.TEXT)

    @pytest.mark.parametrize("raw", [42, None, 4.2, U1])
    def test_non_iterable_raises(self, raw):
        with pytest.raises(TypeError, match="must be iterable"):
            _pg.PgArray(raw, _pg.ArrayElementType.INTEGER)

    def test_none_element_raises(self):
        with pytest.raises(ValueError, match=r"elements\[1\].*SQL NULL"):
            _pg.PgArray([U1, None], _pg.ArrayElementType.UUID)

    @pytest.mark.parametrize(
        "marker",
        [_pg.SpecialValue.DELETE, _pg.SpecialValue.NOW, _pg.SpecialValue.TODAY],
    )
    def test_special_value_element_raises(self, marker):
        with pytest.raises(ValueError, match=r"elements\[0\].*SpecialValue"):
            _pg.PgArray([marker], _pg.ArrayElementType.UUID)

    @pytest.mark.parametrize(
        "bad_type", ["uuid", "uuid[]; --", "text", None, 42, _pg.ArrayMode.REPLACE]
    )
    def test_non_member_element_type_raises(self, bad_type):
        # The cast fragment is built from the enum member, so a caller string
        # (hostile or not) can never become one.
        with pytest.raises(TypeError, match="element_type must be an ArrayElementType"):
            _pg.PgArray([], bad_type)

    @pytest.mark.parametrize(
        "bad_mode", ["APPEND", "append", None, 1, _pg.ArrayElementType.TEXT]
    )
    def test_non_member_mode_raises(self, bad_mode):
        with pytest.raises(TypeError, match="mode must be an ArrayMode"):
            _pg.PgArray([], _pg.ArrayElementType.TEXT, bad_mode)

    def test_default_mode_is_replace(self):
        assert _pg.PgArray([], _pg.ArrayElementType.TEXT).mode is _pg.ArrayMode.REPLACE

    def test_is_not_a_list(self):
        # The distinction that keeps a bare list meaning what it always meant.
        array = _pg.PgArray(["a"], _pg.ArrayElementType.TEXT)
        assert not isinstance(array, list)
        assert isinstance(array, _pg.PgArray)

    def test_frozen(self):
        array = _pg.PgArray(["a"], _pg.ArrayElementType.TEXT)
        with pytest.raises(_dataclasses.FrozenInstanceError):
            array.mode = _pg.ArrayMode.APPEND  # ty: ignore[invalid-assignment]

    def test_equality_is_by_value(self):
        assert _pg.PgArray([str(U1)], _pg.ArrayElementType.UUID) == _pg.PgArray(
            [U1], _pg.ArrayElementType.UUID
        )
        assert _pg.PgArray([U1], _pg.ArrayElementType.UUID) != _pg.PgArray(
            [U1], _pg.ArrayElementType.UUID, _pg.ArrayMode.APPEND
        )


class Test_ArrayElementType_cast_sql:
    @pytest.mark.parametrize(
        "element_type, expected",
        [
            (_pg.ArrayElementType.UUID, "::uuid[]"),
            (_pg.ArrayElementType.TEXT, "::text[]"),
            (_pg.ArrayElementType.INTEGER, "::integer[]"),
            (_pg.ArrayElementType.BIGINT, "::bigint[]"),
            (_pg.ArrayElementType.DATE, "::date[]"),
        ],
    )
    def test_cast_sql(self, element_type, expected):
        cast = element_type.cast_sql
        assert isinstance(cast, _psycopg_sql.SQL)
        assert cast.as_string() == expected


class Test_ArrayElementType_normalize:
    @pytest.mark.parametrize("raw", [U1, str(U1), str(U1).upper()])
    def test_uuid_accepts_uuid_and_str(self, raw):
        assert _pg.PgArray([raw], _pg.ArrayElementType.UUID).elements == (U1,)

    @pytest.mark.parametrize("bad", [42, 4.2, True, _datetime.date(2027, 8, 1)])
    def test_uuid_rejects_other_types(self, bad):
        with pytest.raises(TypeError, match=r"not a valid uuid\[\] element"):
            _pg.PgArray([bad], _pg.ArrayElementType.UUID)

    def test_uuid_rejects_a_non_uuid_str(self):
        with pytest.raises(ValueError, match="badly formed"):
            _pg.PgArray(["not-a-uuid"], _pg.ArrayElementType.UUID)

    def test_text_accepts_str_only(self):
        assert _pg.PgArray(["a", ""], _pg.ArrayElementType.TEXT).elements == ("a", "")

    @pytest.mark.parametrize("bad", [1, True, U1, _datetime.date(2027, 8, 1), 4.2])
    def test_text_rejects_non_str(self, bad):
        with pytest.raises(TypeError, match=r"not a valid text\[\] element"):
            _pg.PgArray([bad], _pg.ArrayElementType.TEXT)

    @pytest.mark.parametrize(
        "element_type", [_pg.ArrayElementType.INTEGER, _pg.ArrayElementType.BIGINT]
    )
    def test_integer_accepts_int(self, element_type):
        assert _pg.PgArray([1, -2, 0], element_type).elements == (1, -2, 0)

    @pytest.mark.parametrize(
        "element_type", [_pg.ArrayElementType.INTEGER, _pg.ArrayElementType.BIGINT]
    )
    @pytest.mark.parametrize("bad", [True, False, "1", 1.0, U1])
    def test_integer_rejects_bool_and_other_types(self, element_type, bad):
        with pytest.raises(
            TypeError, match=r"not a valid (integer|bigint)\[\] element"
        ):
            _pg.PgArray([bad], element_type)

    def test_date_accepts_date_and_iso_str(self):
        array = _pg.PgArray(
            [_datetime.date(2027, 8, 1), "2027-08-02"], _pg.ArrayElementType.DATE
        )
        assert array.elements == (
            _datetime.date(2027, 8, 1),
            _datetime.date(2027, 8, 2),
        )

    def test_date_rejects_datetime_as_ambiguous(self):
        # A datetime's day depends on the time zone -- the caller must decide.
        naive = _datetime.datetime(2027, 8, 1, 23, 30)  # noqa: DTZ001
        aware = _datetime.datetime(2027, 8, 1, 23, 30, tzinfo=_datetime.UTC)
        for value in (naive, aware):
            with pytest.raises(TypeError, match="datetime is ambiguous"):
                _pg.PgArray([value], _pg.ArrayElementType.DATE)

    @pytest.mark.parametrize("bad", [1, True, U1, 4.2])
    def test_date_rejects_other_types(self, bad):
        with pytest.raises(TypeError, match=r"not a valid date\[\] element"):
            _pg.PgArray([bad], _pg.ArrayElementType.DATE)

    def test_date_rejects_a_non_iso_str(self):
        with pytest.raises(ValueError, match="isoformat"):
            _pg.PgArray(["01.08.2027"], _pg.ArrayElementType.DATE)


class Test_in_expr_as_string:
    @pytest.mark.parametrize("ids", [[4], [4, 7], [7, 4, 4], []])
    def test_returns_sql_composable(self, ids):
        got = _pg.in_expr(_psycopg_sql.Identifier("id"), ids)
        assert isinstance(got, _psycopg_sql.SQL)

    @pytest.mark.parametrize(
        "ids, expected",
        [
            ([4], '"id" = 4'),
            ([4, 7], '"id" IN (4, 7)'),
            ([7, 4, 4], '"id" IN (7, 4, 4)'),
            ([], "FALSE"),
        ],
    )
    def test_identifier_as_string(self, ids, expected):
        got = _pg.in_expr(_psycopg_sql.Identifier("id"), ids).as_string()
        assert got == expected

    def test_identifier_empty_with_custom_empty_expr(self):
        got = _pg.in_expr(_psycopg_sql.Identifier("id"), [], empty_expr="").as_string()
        assert got == ""


class Test_in_expr_inside_t_string:
    @pytest.mark.parametrize(
        "ids, expected_where",
        [
            ([4], '"id" = 4'),
            ([4, 7], '"id" IN (4, 7)'),
            ([7, 4, 4], '"id" IN (7, 4, 4)'),
            ([], "FALSE"),
        ],
    )
    def test_identifier_ids_resolve(self, ids, expected_where):
        where = _pg.in_expr(_psycopg_sql.Identifier("id"), ids)
        query = t'SELECT id FROM "groups" WHERE {where:q}'
        resolved = _psycopg_sql.as_string(query, context=None)
        assert resolved == f'SELECT id FROM "groups" WHERE {expected_where}'

    @pytest.mark.parametrize(
        "ids, expected_where",
        [
            (["a"], "'foo' = 'a'"),
            (["a", "b"], "'foo' IN ('a', 'b')"),
            ([4, "a"], "'foo' IN (4, 'a')"),
            ([], "FALSE"),
        ],
    )
    def test_literal_regression_yields_string_literal(self, ids, expected_where):
        where = _pg.in_expr(_psycopg_sql.Literal("foo"), ids)
        query = t'SELECT id FROM "groups" WHERE {where:q}'
        resolved = _psycopg_sql.as_string(query, context=None)
        assert resolved == f'SELECT id FROM "groups" WHERE {expected_where}'


_GROUPS_SELECT = (
    "SELECT id, parent_id, name, short_name, type, email, "
    'description, additional_info FROM "groups" WHERE '
)


class Test_pg_select_groups_dicts_for_where:
    """The real path building that ``{where:q}`` template, with the executor
    ``_execute_query_fetchall_dicts`` replaced by a ``Mock`` so no connection
    is opened."""

    @pytest.mark.parametrize(
        "ids, expected_query",
        [
            ([4], _GROUPS_SELECT + '"id" = 4'),
            ([7, 4], _GROUPS_SELECT + '"id" IN (7, 4)'),
            ([], _GROUPS_SELECT + "FALSE"),
        ],
    )
    def test_builds_template_with_quoted_identifier(
        self, mock_execute_query_fetchall_dicts, ids, expected_query
    ):
        mock_execute_query_fetchall_dicts.return_value = []

        where = _pg.in_expr(_psycopg_sql.Identifier("id"), ids)
        result = _pg.pg_select_groups_dicts_for_where(
            _unittest_mock.Mock(), where=where
        )

        assert result == []
        _, query = mock_execute_query_fetchall_dicts.call_args.args
        assert isinstance(query, _string_templatelib.Template)
        resolved = _psycopg_sql.as_string(query, context=None)
        assert resolved == expected_query

    def test_empty_ids_resolve_to_false(self, mock_execute_query_fetchall_dicts):
        mock_execute_query_fetchall_dicts.return_value = []

        result = _pg.pg_select_groups_dicts_for_where(
            _unittest_mock.Mock(), where=_pg.in_expr(_psycopg_sql.Identifier("id"), [])
        )

        assert result == []
        query = mock_execute_query_fetchall_dicts.call_args.args[1]
        resolved = _psycopg_sql.as_string(query, context=None)
        assert "WHERE FALSE" in resolved

    def test_executor_receives_resolved_query(self, mock_execute_query_fetchall_dicts):

        expected_rows = [{"id": 4}]
        mock_execute_query_fetchall_dicts.return_value = expected_rows

        where = _pg.in_expr(_psycopg_sql.Identifier("id"), [4])
        conn_mock = _unittest_mock.Mock()
        result = _pg.pg_select_groups_dicts_for_where(conn_mock, where=where)

        assert result == expected_rows

        assert mock_execute_query_fetchall_dicts.call_count == 1
        call_args_1 = mock_execute_query_fetchall_dicts.call_args_list[0].args[1]
        got_query = _psycopg_sql.as_string(call_args_1, context=None)
        expected_query = _GROUPS_SELECT + '"id" = 4'

        assert got_query == expected_query
