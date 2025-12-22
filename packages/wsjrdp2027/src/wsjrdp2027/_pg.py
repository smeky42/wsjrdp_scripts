from __future__ import annotations

import collections.abc as _collections_abc
import logging as _logging
import textwrap as _textwrap
import typing as _typing

from . import _person_pg, _types


if _typing.TYPE_CHECKING:
    import datetime as _datetime

    import psycopg as _psycopg
    import psycopg.sql as _psycopg_sql

    from . import _payment_role


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

    >>> col_val_pairs_to_insert_do_nothing_sql_query("tags", [("name", "Tag")]).as_string()
    'WITH t AS (INSERT INTO "tags" ("name") VALUES (\'Tag\') ON CONFLICT DO NOTHING RETURNING "id")\nSELECT * FROM t\nUNION\nSELECT "id" FROM "tags" WHERE "name" = \'Tag\''
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


def _execute_query_fetch_id(cursor: _psycopg.Cursor, query) -> int:
    result = _execute_query_fetchone(cursor, query)
    if result is None:
        raise RuntimeError("Query did not return any result")
    if len(result) < 1:
        raise RuntimeError(f"Query result is empty: result={result}")
    return result[0]


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
    return _execute_query_fetch_id(cursor, query)


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
    return _execute_query_fetch_id(cursor, query)


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


_UpdatesType = (
    _collections_abc.Iterable[_collections_abc.Iterable]
    | _collections_abc.Mapping[str, _typing.Any]
)


def _normalize_updates(updates: _UpdatesType, /) -> list[tuple[str, _typing.Any]]:
    if isinstance(updates, _collections_abc.Mapping):
        return [(k, v) for k, v in updates.items()]  # type: ignore
    else:
        return [tuple(x) for x in updates]  # type: ignore


def _pg_update_table(
    cursor: _psycopg.Cursor,
    /,
    *,
    table_name: str | _psycopg_sql.Identifier,
    id: int,
    updates: _UpdatesType,
    id_col: str | _psycopg_sql.Identifier = "id",
) -> list:
    from psycopg.sql import SQL, Identifier, Literal

    updates = _normalize_updates(updates)
    if isinstance(table_name, str):
        table_name = Identifier(table_name)
    if isinstance(id_col, str):
        id_col = Identifier(id_col)

    if not updates:
        _LOGGER.debug(
            "Skip executing UPDATE %s ... as no updates were given", table_name
        )
        return []

    sql_updates = SQL(", ").join(
        SQL("{key} = {val}").format(key=Identifier(key), val=Literal(val))
        for key, val in updates
    )
    result = _execute_query_fetchall(
        cursor,
        SQL(
            "UPDATE {table_name} SET {sql_updates} WHERE {id_col} = {id} RETURNING {id_col}"
        ).format(
            table_name=table_name,
            sql_updates=sql_updates,
            id_col=id_col,
            id=Literal(id),
        ),
    )
    return result


def pg_update_person(
    cursor: _psycopg.Cursor, /, *, id: int, updates: _UpdatesType
) -> int | None:
    result = _pg_update_table(
        cursor, id=id, updates=updates, table_name="people", id_col="id"
    )
    return result[0][0] if result else None


def pg_update_direct_debit_pre_notification(
    cursor: _psycopg.Cursor, /, *, id: int, updates: _UpdatesType
) -> int | None:
    result = _pg_update_table(
        cursor,
        id=id,
        updates=updates,
        table_name="wsjrdp_direct_debit_pre_notifications",
        id_col="id",
    )
    return result[0][0] if result else None


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
    return _execute_query_fetch_id(cursor, insert_query)


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
    result = _execute_query_fetchone(cursor, select_query)
    return result[0] if result else None


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
    return _execute_query_fetch_id(cursor, insert_query)


def pg_insert_note(
    cursor: _psycopg.Cursor,
    /,
    *,
    subject_id: int,
    subject_type: str = "Person",
    author_id: int = 1,
    text: str,
    created_at: _datetime.datetime | _datetime.date | str | int | float | None = None,
    updated_at: _datetime.datetime | _datetime.date | str | int | float | None = None,
    now: _datetime.datetime | None = None,
) -> int:
    from . import _util

    created_at = _util.to_datetime(created_at, now=now)
    updated_at = _util.to_datetime(updated_at, now=created_at)
    colval_pairs = [
        ("subject_id", subject_id),
        ("subject_type", subject_type),
        ("author_id", author_id),
        ("created_at", created_at),
        ("updated_at", updated_at),
        ("text", text),
    ]
    insert_query = col_val_pairs_to_insert_sql_query(
        "notes", colval_pairs=colval_pairs, returning="id"
    )
    return _execute_query_fetch_id(cursor, insert_query)


def pg_insert_direct_debit_pre_notification(
    cursor: _psycopg.Cursor,
    *,
    created_at: _datetime.datetime | str | None = "NOW",
    updated_at: _datetime.datetime | str | None = None,
    payment_initiation_id: int | None = None,
    direct_debit_payment_info_id: int | None = None,
    subject_id: int | None = None,
    subject_type: str | None = "Person",
    author_id: int | None = None,
    author_type: str | None = "Person",
    try_skip: bool | None = None,
    payment_status: str | None = None,
    email_from: str = "anmeldung@worldscoutjamboree.de",
    email_to: _collections_abc.Iterable[str] | str | None = None,
    email_cc: _collections_abc.Iterable[str] | str | None = None,
    email_bcc: _collections_abc.Iterable[str] | str | None = None,
    email_reply_to: _collections_abc.Iterable[str] | str | None = None,
    dbtr_name: str,
    dbtr_iban: str,
    dbtr_bic: str | None = None,
    dbtr_address: str | None = None,
    amount_currency: str = "EUR",
    amount_cents: int,
    pre_notified_amount_cents: int | None = None,
    debit_sequence_type: str = "OOFF",
    collection_date: _datetime.date | str | None = None,
    mandate_id: str | None = None,
    mandate_date: _datetime.date | str | None = None,
    description: str | None = None,
    comment: str | None = None,
    endtoend_id: str | None = None,
    payment_role: str | _payment_role.PaymentRole | None = None,
    early_payer: bool | None = None,
    cdtr_name: str | None = None,
    cdtr_iban: str | None = None,
    cdtr_bic: str | None = None,
    cdtr_address: str | None = None,
    additional_info: dict | None = None,
    creditor_id: str | None = None,
    sepa_dd_config: _types.SepaDirectDebitConfig | None = None,
) -> int:
    from . import _payment_role, _pg, _util

    if sepa_dd_config:
        if not cdtr_name:
            cdtr_name = sepa_dd_config.get("name")
        if not cdtr_iban:
            cdtr_iban = sepa_dd_config.get("IBAN")
        if not cdtr_bic:
            cdtr_bic = sepa_dd_config.get("BIC")
        if not cdtr_address:
            cdtr_address = sepa_dd_config.get("address_as_single_line")
        if not creditor_id:
            creditor_id = sepa_dd_config.get("creditor_id")

    if not creditor_id:
        raise ValueError("Missing creditor_id (also not present in sepa_dd_config")

    if pre_notified_amount_cents is None:
        pre_notified_amount_cents = amount_cents
    if payment_status is None:
        payment_status = "pre_notified"

    if isinstance(payment_role, _payment_role.PaymentRole):
        payment_role = payment_role.get_db_payment_role(early_payer=early_payer)

    cols_vals = [
        ("created_at", _util.to_datetime_or_none(created_at)),
        ("updated_at", _util.to_datetime_or_none(updated_at)),
        ("payment_initiation_id", _util.to_int_or_none(payment_initiation_id)),
        (
            "direct_debit_payment_info_id",
            _util.to_int_or_none(direct_debit_payment_info_id),
        ),
        ("subject_id", subject_id),
        ("subject_type", subject_type),
        ("author_id", author_id),
        ("author_type", author_type),
        ("try_skip", bool(try_skip)),
        ("payment_status", payment_status),
        ("email_from", email_from or None),
        ("email_to", _util.to_str_list_or_none(email_to)),
        ("email_cc", _util.to_str_list_or_none(email_cc)),
        ("email_bcc", _util.to_str_list_or_none(email_bcc)),
        ("email_reply_to", _util.to_str_list_or_none(email_reply_to)),
        ("dbtr_name", dbtr_name),
        ("dbtr_iban", dbtr_iban),
        ("dbtr_bic", dbtr_bic),
        ("dbtr_address", dbtr_address),
        ("amount_currency", amount_currency),
        ("amount_cents", amount_cents),
        ("pre_notified_amount_cents", pre_notified_amount_cents),
        ("debit_sequence_type", debit_sequence_type),
        ("collection_date", _util.to_date_or_none(collection_date)),
        ("mandate_id", mandate_id),
        ("mandate_date", _util.to_date_or_none(mandate_date)),
        ("description", description),
        ("comment", comment or ""),
        ("endtoend_id", endtoend_id),
        ("payment_role", payment_role),
        ("cdtr_name", cdtr_name),
        ("cdtr_iban", cdtr_iban),
        ("cdtr_bic", cdtr_bic),
        ("cdtr_address", cdtr_address),
        ("creditor_id", creditor_id),
    ]
    if additional_info:
        cols_vals.append(("additional_info", additional_info))
    query = _pg.col_val_pairs_to_insert_sql_query(
        "wsjrdp_direct_debit_pre_notifications", cols_vals, "id"
    )
    return _execute_query_fetch_id(cursor, query)


def pg_insert_payment_initiation(
    cursor: _psycopg.Cursor,
    *,
    created_at: _datetime.datetime | str = "NOW",
    updated_at: _datetime.datetime | str | None = None,
    status: str = "planned",
    sepa_schema: str = "pain.008.001.02",
    message_identification: str | None = None,
    number_of_transactions: int | None = None,
    control_sum_cents: int | None = None,
    initiating_party_name: str | None = None,
    initiating_party_iban: str | None = None,
    initiating_party_bic: str | None = None,
    sepa_dd_config: _types.SepaDirectDebitConfig | None = None,
) -> int:
    from . import _pg, _util

    if sepa_dd_config:
        if not initiating_party_name:
            initiating_party_name = sepa_dd_config.get("name")
        if not initiating_party_iban:
            initiating_party_iban = sepa_dd_config.get("IBAN")
        if not initiating_party_bic:
            initiating_party_bic = sepa_dd_config.get("BIC")

    cols_vals = [
        ("created_at", _util.to_datetime_or_none(created_at)),
        ("updated_at", _util.to_datetime_or_none(updated_at)),
        ("status", status),
        ("sepa_schema", sepa_schema),
        ("message_identification", message_identification),
        ("number_of_transactions", number_of_transactions),
        ("control_sum_cents", control_sum_cents),
        ("initiating_party_name", initiating_party_name),
        ("initiating_party_iban", initiating_party_iban),
        ("initiating_party_bic", initiating_party_bic),
    ]
    query = _pg.col_val_pairs_to_insert_sql_query(
        "wsjrdp_payment_initiations", cols_vals, "id"
    )
    return _execute_query_fetch_id(cursor, query)


def pg_insert_direct_debit_payment_info(
    cursor: _psycopg.Cursor,
    *,
    created_at: _datetime.datetime | str | None = "NOW",
    updated_at: _datetime.datetime | str | None = None,
    payment_initiation_id: int | None = None,
    payment_information_identification: str | None = None,
    batch_booking: bool = True,
    number_of_transactions: int | None = None,
    control_sum_cents: int | None = None,
    payment_type_instrument: str = "CORE",
    debit_sequence_type: str = "OOFF",
    requested_collection_date: _datetime.date | str = "TODAY",
    cdtr_name: str | None = None,
    cdtr_iban: str | None = None,
    cdtr_bic: str | None = None,
    creditor_id: str | None = None,
    sepa_dd_config: _types.SepaDirectDebitConfig | None = None,
) -> int:
    from . import _pg, _util

    if sepa_dd_config:
        if not cdtr_name:
            cdtr_name = sepa_dd_config.get("name")
        if not cdtr_iban:
            cdtr_iban = sepa_dd_config.get("IBAN")
        if not cdtr_bic:
            cdtr_bic = sepa_dd_config.get("BIC")
        if not creditor_id:
            creditor_id = sepa_dd_config.get("creditor_id")

    if not creditor_id:
        raise ValueError("Missing creditor_id (also not present in sepa_dd_config")

    cols_vals = [
        ("created_at", _util.to_datetime_or_none(created_at)),
        ("updated_at", _util.to_datetime_or_none(updated_at)),
        ("payment_initiation_id", _util.to_int_or_none(payment_initiation_id)),
        ("payment_information_identification", payment_information_identification),
        ("batch_booking", batch_booking),
        ("number_of_transactions", number_of_transactions),
        ("control_sum_cents", control_sum_cents),
        ("payment_type_instrument", payment_type_instrument),
        ("debit_sequence_type", debit_sequence_type),
        ("requested_collection_date", _util.to_date_or_none(requested_collection_date)),
        ("cdtr_name", cdtr_name),
        ("cdtr_iban", cdtr_iban),
        ("cdtr_bic", cdtr_bic),
        ("creditor_id", creditor_id),
    ]
    query = _pg.col_val_pairs_to_insert_sql_query(
        "wsjrdp_direct_debit_payment_infos", cols_vals, "id"
    )
    return _execute_query_fetch_id(cursor, query)


def pg_insert_accounting_entry(
    cursor,
    subject_id: int | str,
    author_id: int | str,
    amount_cents: int,
    description: str,
    subject_type: str = "Person",
    author_type: str = "Person",
    created_at: _datetime.datetime | str | None = None,
    updated_at: _datetime.datetime | str | None = None,
    payment_initiation_id: int | None = None,
    direct_debit_payment_info_id: int | None = None,
    direct_debit_pre_notification_id: int | None = None,
    endtoend_id: str | None = None,
    mandate_id: str | None = None,
    mandate_date: _datetime.date | str | None = None,
    debit_sequence_type: str | None = None,
    value_date: _datetime.date | str | None = None,
    new_sepa_status: str | None = None,
    cdtr_name: str | None = None,
    cdtr_iban: str | None = None,
    cdtr_bic: str | None = None,
    cdtr_address: str | None = None,
    dbtr_name: str | None = None,
    dbtr_iban: str | None = None,
    dbtr_bic: str | None = None,
    dbtr_address: str | None = None,
) -> int:
    from . import _pg, _util

    created_at = _util.to_datetime(created_at)
    updated_at = _util.to_datetime_or_none(updated_at)

    cols_vals = [
        ("created_at", created_at),
        ("updated_at", updated_at),
        ("subject_type", subject_type),
        ("subject_id", int(subject_id)),
        ("author_type", author_type),
        ("author_id", int(author_id)),
        ("amount_currency", "EUR"),
        ("amount_cents", int(amount_cents)),
        ("description", description),
        ("payment_initiation_id", _util.to_int_or_none(payment_initiation_id)),
        (
            "direct_debit_payment_info_id",
            _util.to_int_or_none(direct_debit_payment_info_id),
        ),
        (
            "direct_debit_pre_notification_id",
            _util.to_int_or_none(direct_debit_pre_notification_id),
        ),
        ("endtoend_id", endtoend_id),
        ("mandate_id", mandate_id),
        ("mandate_date", _util.to_date_or_none(mandate_date)),
        ("debit_sequence_type", debit_sequence_type),
        ("value_date", _util.to_date_or_none(value_date)),
        ("new_sepa_status", new_sepa_status),
        ("cdtr_name", cdtr_name),
        ("cdtr_iban", cdtr_iban),
        ("cdtr_bic", cdtr_bic),
        ("cdtr_address", cdtr_address),
        ("dbtr_name", dbtr_name),
        ("dbtr_iban", dbtr_iban),
        ("dbtr_bic", dbtr_bic),
        ("dbtr_address", dbtr_address),
    ]
    query = _pg.col_val_pairs_to_insert_sql_query("accounting_entries", cols_vals, "id")
    return _execute_query_fetch_id(cursor, query)
