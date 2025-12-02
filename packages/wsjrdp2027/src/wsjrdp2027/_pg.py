from __future__ import annotations

import logging as _logging
import textwrap as _textwrap
import typing as _typing

from . import _person_pg


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc
    import datetime as _datetime

    import psycopg as _psycopg
    import psycopg.sql as _psycopg_sql


_LOGGER = _logging.getLogger(__name__)


def create_select_query(
    table_name: str | _psycopg_sql.Identifier,
    columns: _collections_abc.Iterable[_psycopg_sql.Composable | str],
    *,
    where: _psycopg_sql.Composable | str | None = None,
    limit: int | None = None,
) -> _psycopg_sql.Composed:
    from psycopg.sql import SQL, Composable, Composed, Identifier, Literal

    if isinstance(table_name, str):
        table_name = Identifier(table_name)

    col_specs = [
        col if isinstance(col, Composable) else Identifier(col) for col in columns
    ]

    query = SQL("SELECT {col_specs} FROM {table_name}").format(
        col_specs=SQL(", ").join(col_specs), table_name=table_name
    )
    if where:
        if isinstance(where, str):
            where = SQL(where)  # type: ignore
        query = Composed([*query, SQL(" WHERE {}").format(where)])
    if limit is not None:
        limit = Literal(limit)  # type: ignore
        query = Composed([*query, SQL(" LIMIT {}").format(limit)])
    return query


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


def _execute_query_fetchall(
    cursor: _psycopg.Cursor[_typing.Any], query, *, show_result: bool = False
):
    query_str = _textwrap.indent(query.as_string(context=cursor), "  | ")
    try:
        cursor.execute(query)
        result = cursor.fetchall()
    except Exception:
        _LOGGER.error("failed to execute\n%s", query_str)
        raise
    if show_result:
        _LOGGER.debug("execute\n%s\n  -> %s", query_str, str(result))
    else:
        _LOGGER.debug("execute\n%s\n  -> %s row(s)", query_str, len(result))
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


def pg_fetch_person_dict_for_id(
    cursor: _psycopg.Cursor,
    /,
    id: int,
) -> dict[str, _typing.Any]:
    import psycopg.rows
    from psycopg.sql import SQL, Identifier, Literal

    select_cursor = cursor.connection.cursor(row_factory=psycopg.rows.dict_row)
    select_query = create_select_query(
        "people",
        columns=[Identifier(col) for col in _person_pg.PERSON_VERSION_COLS],
        where=SQL("{table_name}.{id_col} = {id}").format(
            table_name=Identifier("people"), id_col=Identifier("id"), id=Literal(id)
        ),
    )
    with select_cursor:
        d = _execute_query_fetchall(select_cursor, select_query, show_result=False)[0]
    d.setdefault("tag_list", None)
    return d


def pg_fetch_person_dicts_for_ids(
    conn: _psycopg.Connection,
    /,
    ids: _collections_abc.Iterable[int],
) -> dict[int, _typing.Any]:
    import psycopg.rows
    from psycopg.sql import SQL, Identifier

    from . import _util

    where = SQL(_util.in_expr('"id"', ids))  # type: ignore

    tag_list_sql = SQL("""ARRAY(
    SELECT tags.name
    FROM taggings
    LEFT JOIN tags ON taggings.tag_id = tags.id
      AND taggings.taggable_type = 'Person'
    WHERE taggings.taggable_id = people.id
  ) AS tag_list""")

    select_query = create_select_query(
        "people",
        columns=[
            Identifier("id"),
            *(Identifier(col) for col in _person_pg.PERSON_VERSION_COLS),
            tag_list_sql,
        ],
        where=where,
    )
    with conn.cursor(row_factory=psycopg.rows.dict_row) as select_cursor:
        results = _execute_query_fetchall(
            select_cursor, select_query, show_result=False
        )
    d = {row["id"]: row for row in results}
    return d


_ROLE_COLS = [
    "id",
    "person_id",
    "group_id",
    "type",
    "label",
    "created_at",
    "archived_at",
    "terminated",
    "start_on",
    "end_on",
]


def pg_fetch_role_dicts_for_person_ids(
    conn: _psycopg.Connection,
    /,
    ids: _collections_abc.Iterable[int],
    today: _datetime.date | str | None = None,
) -> dict[int, list[dict[str, _typing.Any]]]:
    import psycopg.rows
    from psycopg.sql import SQL, Identifier

    from . import _util

    where = SQL(_util.in_expr('"person_id"', ids))  # type: ignore
    if today is not None:
        today = _util.to_date(today)
        where_end_on = SQL('"end_on" IS NULL OR "end_on" >= {today}').format(
            today=today
        )
        where = SQL("({where_end_on}) AND ({where})").format(
            where_end_on=where_end_on, where=where
        )

    select_query = create_select_query(
        "roles",
        columns=[Identifier(col) for col in _ROLE_COLS],
        where=where,
    )
    with conn.cursor(row_factory=psycopg.rows.dict_row) as select_cursor:
        results = _execute_query_fetchall(
            select_cursor, select_query, show_result=False
        )
    d = {}
    for row in results:
        d.setdefault(row["person_id"], []).append(row)
    return d


def pg_update_person(
    cursor: _psycopg.Cursor, /, *, id: int, updates: _collections_abc.Iterable
) -> None:
    from psycopg.sql import SQL, Identifier, Literal

    updates = list(updates)
    if not updates:
        _LOGGER.debug('Skip executing UPDATE "people" ... as now updates were given')
        return

    sql_updates = SQL(", ").join(
        SQL("{key} = {val}").format(key=Identifier(key), val=Literal(val))
        for key, val in updates
    )
    result = _execute_query_fetchone(
        cursor,
        SQL(
            'UPDATE "people" SET {sql_updates} WHERE "id" = {id} RETURNING "id"'
        ).format(sql_updates=sql_updates, id=Literal(id)),
    )


def pg_insert_version(
    cursor: _psycopg.Cursor,
    /,
    *,
    item_type: str = "Person",
    item_id: int | None = None,
    main_type: str = "Person",
    main_id: int,
    object_dict: dict | None = None,
    changes: dict[str, _collections_abc.Iterable],
    created_at: _datetime.datetime | _datetime.date | str | int | float | None = None,
    mutation_id: str | None = None,
    whodunnit_type: str = "Person",
    whodunnit_id: int | None = None,
    event: str = "update",
) -> int:
    from . import _util

    if item_id is None and item_type == main_type:
        item_id = main_id
    if whodunnit_id is None:
        whodunnit_id = 1  # Administrator
    if object_dict is not None:
        object_str = _util.to_yaml_str(object_dict, explicit_start=True)
    else:
        object_str = None

    colval_pairs = [
        ("item_type", item_type),
        ("item_id", item_id),
        ("main_type", main_type),
        ("main_id", main_id),
        ("whodunnit_type", whodunnit_type),
        ("whodunnit", whodunnit_id),
        ("event", event),
        ("object", object_str),
        ("object_changes", _util.to_yaml_str(changes, explicit_start=True)),
        ("mutation_id", mutation_id),
        ("created_at", _util.to_datetime(created_at)),
    ]
    insert_query = col_val_pairs_to_insert_sql_query(
        "versions", colval_pairs=colval_pairs, returning="id"
    )
    return _execute_query_fetchone(cursor, insert_query)[0]


def pg_from_roles_select_id_and_type_for_person_and_group_id(
    cursor: _psycopg.Cursor,
    /,
    *,
    person_id: int,
    group_id: int,
    today: _datetime.date | str | None = None,
) -> list[tuple[int, str]]:
    from psycopg.sql import SQL

    from . import _util

    today = _util.to_date(today)
    select_query = create_select_query(
        "roles",
        columns=["id", "type"],
        where=SQL(
            '"roles"."person_id" = {person_id} AND "roles"."group_id" = {group_id} AND ("roles"."end_on" IS NULL OR "roles"."end_on" >= {today})'
        ).format(person_id=person_id, group_id=group_id, today=today),
    )
    results = _execute_query_fetchall(cursor, select_query, show_result=True)
    return [tuple(row) for row in results]


def pg_find_role_type(
    cursor: _psycopg.Cursor,
    /,
    *,
    person_id: int,
    group_id: int,
    today: _datetime.date | str | None = None,
) -> str | None:
    from psycopg.sql import SQL

    from . import _util

    today = _util.to_date(today)
    select_query = create_select_query(
        "roles",
        columns=["type"],
        where=SQL(
            '"roles"."person_id" = {person_id} AND "roles"."group_id" = {group_id} AND ("roles"."end_on" IS NULL OR "roles"."end_on" >= {today})'
        ).format(person_id=person_id, group_id=group_id, today=today),
        limit=1,
    )
    return _execute_query_fetchone(cursor, select_query)[0]


def pg_update_role_set_end_on_for_ids(
    cursor: _psycopg.Cursor,
    /,
    *,
    ids: _collections_abc.Iterable[int] | None = None,
    end_on: _datetime.date | str | None = None,
) -> list[int]:
    from psycopg.sql import SQL

    from . import _util

    end_on = _util.to_date_or_none(end_on)
    ids = _util.to_int_list_or_none(ids)
    if not ids:
        _LOGGER.debug("No rows in roles to update (ids=%s)", ids)
        return []
    where = SQL(_util.in_expr('"id"', ids))  # type: ignore
    update_query = SQL(
        'UPDATE "roles" SET "end_on" = {end_on} WHERE {where} RETURNING "roles"."id"'
    ).format(end_on=end_on, where=where)
    update_results = _execute_query_fetchall(cursor, update_query, show_result=True)
    updated_ids = [row[0] for row in update_results]
    delete_query = SQL(
        'DELETE FROM "roles" WHERE {where} AND "end_on" <= "start_on" RETURNING "roles"."id"'
    ).format(where=where)
    delete_results = _execute_query_fetchall(cursor, delete_query, show_result=True)
    deleted_ids = [row[0] for row in delete_results]
    ids = sorted(set(updated_ids) - set(deleted_ids))
    return ids


def pg_insert_role(
    cursor: _psycopg.Cursor,
    /,
    *,
    person_id: int,
    group_id: int,
    type: str,
    label: str | None = None,
    created_at: _datetime.datetime | _datetime.date | str | int | float | None = None,
    updated_at: _datetime.datetime | _datetime.date | str | int | float | None = None,
    archived_at: _datetime.datetime | _datetime.date | str | int | float | None = None,
    terminated: bool = False,
    start_on: _datetime.date | str | None = None,
    end_on: _datetime.date | str | None = None,
    now: _datetime.datetime | None = None,
) -> int:
    from . import _util

    today = now.date() if now is not None else None
    created_at = _util.to_datetime(created_at, now=now)
    updated_at = _util.to_datetime(updated_at, now=now)
    start_on = _util.to_date(start_on, today=today)
    end_on = _util.to_date_or_none(end_on)

    colval_pairs = [
        ("person_id", person_id),
        ("group_id", group_id),
        ("type", type),
        ("label", label),
        ("created_at", created_at),
        ("updated_at", updated_at),
        ("archived_at", _util.to_datetime_or_none(archived_at)),
        ("terminated", terminated),
        ("start_on", start_on),
        ("end_on", end_on),
    ]
    insert_query = col_val_pairs_to_insert_sql_query(
        "roles", colval_pairs=colval_pairs, returning="id"
    )
    return _execute_query_fetchone(cursor, insert_query)[0]
