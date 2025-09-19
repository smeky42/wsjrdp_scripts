from __future__ import annotations

import collections.abc as _collections_abc
import logging as _logging
import typing as _typing


if _typing.TYPE_CHECKING:
    import pandas as _pandas
    import psycopg as _psycopg


_LOGGER = _logging.getLogger(__name__)


def load_people_dataframe(
    conn: _psycopg.Connection,
    *,
    extra_cols: str | list[str] | None = None,
    join: str = "",
    where: str = "",
    group_by: str = "",
    status: str | _collections_abc.Iterable[str] | None = None,
    exclude_deregistered: bool = True,
    log_resulting_data_frame: bool = True,
) -> _pandas.DataFrame:
    import re
    import textwrap

    import pandas as pd
    import psycopg.rows

    from . import _util

    if extra_cols is not None:
        if isinstance(extra_cols, str):
            extra_cols = extra_cols.strip()
        else:
            extra_cols = ",\n  ".join(extra_cols)
    extra_cols_clause = f",\n  {extra_cols}" if extra_cols else ""

    join_clause = join

    status = _util.to_str_list(status)
    if status is not None:
        where = _util.combine_where(where, _util.in_expr("people.status", status))
    elif exclude_deregistered:
        where = _util.combine_where(
            where, "people.status NOT IN ('deregistration_noted', 'deregistered')"
        )
    where_clause = f"WHERE {where}" if where else ""
    group_by_clause = f"GROUP BY {group_by}" if group_by else ""

    with conn:
        cur = conn.cursor(row_factory=psycopg.rows.dict_row)

        sql_stmt = f"""
SELECT
  people.id, first_name, last_name, nickname, email, gender, people.status, nickname, primary_group_id,
  sepa_name, sepa_mail, sepa_iban, sepa_bic,
  payment_role,
  COALESCE(people.early_payer, FALSE) AS early_payer,
  print_at{extra_cols_clause}
FROM people
{join_clause}
{where_clause}
{group_by_clause}
ORDER BY people.id
        """
        sql_stmt = re.sub(r"\n+", "\n", textwrap.dedent(sql_stmt).strip())

        _LOGGER.info("SQL Query:\n%s", textwrap.indent(sql_stmt, "  "))
        cur.execute(sql_stmt)  # type: ignore

        rows = cur.fetchall()
        cur.close()
        df = pd.DataFrame(rows)

        df["short_first_name"] = df["first_name"].map(lambda s: s.split(" ", 1)[0])
        df["greeting_name"] = df.apply(
            lambda row: row["nickname"] or row["short_first_name"], axis=1
        )
        df["full_name"] = df["first_name"] + " " + df["last_name"]
        df["short_full_name"] = df["short_first_name"] + " " + df["last_name"]

        if log_resulting_data_frame:
            _LOGGER.info(
                "Resulting pandas DataFrame:\n%s", textwrap.indent(str(df), "  ")
            )
        return df
