from __future__ import annotations

import string.templatelib as _string_templatelib
import unittest.mock as _unittest_mock

import psycopg.sql as _psycopg_sql
import pytest
from wsjrdp2027 import _pg


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
