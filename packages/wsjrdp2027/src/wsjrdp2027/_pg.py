from __future__ import annotations

import logging as _logging
import textwrap as _textwrap
import typing as _typing


if _typing.TYPE_CHECKING:
    import datetime as _datetime

    import psycopg as _psycopg
    import psycopg.sql as _psycopg_sql


_LOGGER = _logging.getLogger(__name__)


def col_val_pairs_to_insert_sql_query(
    table_name: str | _psycopg_sql.Identifier,
    colval_pairs,
    returning: str | _psycopg_sql.Identifier | None = "id",
    on_conflict: _psycopg_sql.Composed | _psycopg_sql.SQL | str | None = None,
) -> _psycopg_sql.Composed:
    r"""Return a composed INSERT query.

    >>> col_val_pairs_to_insert_sql_query("tags", [("name", "Tag")]).as_string()
    'INSERT INTO "tags" ("name") VALUES (\'Tag\') RETURNING "id"'
    """
    from psycopg.sql import SQL, Composed, Identifier, Literal

    if isinstance(table_name, str):
        table_name = Identifier(table_name)

    cols = [*(Identifier(col_val[0]) for col_val in colval_pairs)]
    vals = [*(Literal(col_val[1]) for col_val in colval_pairs)]
    sql_cols = SQL(", ").join(cols)
    sql_vals = SQL(", ").join(vals)
    query = SQL("INSERT INTO {table_name} ({sql_cols}) VALUES ({sql_vals})").format(
        table_name=table_name,
        sql_cols=sql_cols,
        sql_vals=sql_vals,
        on_conflict=on_conflict,
        returning=returning,
    )
    if on_conflict is not None:
        query = Composed([*query, SQL(" ON CONFLICT {}").format(on_conflict)])
    if returning is not None:
        if isinstance(returning, str):
            returning = Identifier(returning)
        query = Composed([*query, SQL(" RETURNING {}").format(returning)])
    return query


def col_val_pairs_to_insert_do_nothing_sql_query(
    table_name: str | _psycopg_sql.Identifier,
    matching_colval_pairs,
    other_colval_pairs=None,
    *,
    returning: str | _psycopg_sql.Identifier = "id",
) -> _psycopg_sql.Composed:
    r"""Return a composed INSERT query.

    >>> col_val_pairs_to_insert_sql_query("tags", [("name", "Tag")]).as_string()
    'INSERT INTO "tags" ("name") VALUES (\'Tag\') RETURNING "id"'
    """
    from psycopg.sql import SQL, Identifier, Literal

    if isinstance(table_name, str):
        table_name = Identifier(table_name)
    if isinstance(returning, str):
        returning = Identifier(returning)

    all_colval_pairs = list(matching_colval_pairs) + list(other_colval_pairs or [])

    def sql_cmp(k, v):
        key = Identifier(k)
        val = Literal(v)
        if v is None:
            return SQL("{key} IS {val}").format(key=key, val=val)
        else:
            return SQL("{key} = {val}").format(key=key, val=val)

    where_clause = SQL(" AND ").join(sql_cmp(k, v) for k, v in matching_colval_pairs)

    insert_query = col_val_pairs_to_insert_sql_query(
        table_name, all_colval_pairs, returning=returning, on_conflict=SQL("DO NOTHING")
    )
    query = SQL("""WITH t AS ({insert_query})
SELECT * FROM t
UNION
SELECT {returning} FROM {table_name} WHERE {where_clause}""").format(
        table_name=table_name,
        insert_query=insert_query,
        returning=returning,
        where_clause=where_clause,
    )
    return query


def _execute_query_fetchone(cursor: _psycopg.Cursor, query):
    query_str = _textwrap.indent(query.as_string(context=cursor), "  | ")
    try:
        cursor.execute(query)
        result = cursor.fetchone()
    except Exception:
        _LOGGER.error("failed to execute\n%s", query_str)
        raise
    _LOGGER.debug("execute\n%s\n  -> %s", query_str, str(result))
    return result


def _upsert_tag(cursor: _psycopg.Cursor, /, tag: str) -> int:
    """Upserts tag with name *name* and returns the id of the row."""
    query = col_val_pairs_to_insert_do_nothing_sql_query("tags", [("name", tag)])
    return _execute_query_fetchone(cursor, query)[0]


def _find_tagging_id(
    cursor: _psycopg.Cursor,
    /,
    *,
    tag_id: int,
    taggable_type: str = "Person",
    taggable_id: int,
    context: str = "tags",
) -> int | None:
    from psycopg.sql import SQL

    query = SQL(
        """SELECT "id" FROM "taggings" WHERE "tag_id" = {tag_id} AND "taggable_type" = {taggable_type} AND "taggable_id" = {taggable_id} AND "context" = {context} LIMIT 1"""
    ).format(
        tag_id=tag_id,
        taggable_type=taggable_type,
        taggable_id=taggable_id,
        context=context,
    )
    result = _execute_query_fetchone(cursor, query)
    return result[0] if result is not None else None


def _upsert_tagging(
    cursor: _psycopg.Cursor,
    /,
    *,
    tag_id: int,
    taggable_type: str = "Person",
    taggable_id: int,
    tagger_type: str | None = None,
    tagger_id: str | None = None,
    context: str = "tags",
    hitobito_tooltip: str | None = None,
    tenant: str | None = None,
    created_at: _datetime.datetime | _datetime.date | int | float | str | None = None,
) -> int:
    from . import _util

    query = col_val_pairs_to_insert_do_nothing_sql_query(
        "taggings",
        [
            ("tag_id", tag_id),
            ("taggable_id", taggable_id),
            ("taggable_type", taggable_type),
            ("context", context),
            ("tagger_type", tagger_type),
            ("tagger_id", tagger_id),
        ],
        [
            ("hitobito_tooltip", hitobito_tooltip),
            ("tenant", tenant),
            ("created_at", _util.to_datetime(created_at)),
        ],
    )
    return _execute_query_fetchone(cursor, query)[0]


def pg_add_person_tag(cursor: _psycopg.Cursor, /, person_id: int, tag: str) -> int:
    from psycopg.sql import SQL

    tag_id = _upsert_tag(cursor, tag=tag)
    tagging_id = _find_tagging_id(cursor, tag_id=tag_id, taggable_id=person_id)
    if tagging_id is not None:
        return tagging_id
    else:
        tagging_id = _upsert_tagging(cursor, tag_id=tag_id, taggable_id=person_id)
        _execute_query_fetchone(
            cursor,
            SQL(
                'UPDATE "tags" SET "taggings_count" = "taggings_count" + 1 WHERE "id" = {tag_id} RETURNING "taggings_count"'
            ).format(tag_id=tag_id),
        )
        return tagging_id
