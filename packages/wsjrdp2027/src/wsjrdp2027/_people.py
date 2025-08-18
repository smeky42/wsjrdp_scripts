from __future__ import annotations

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
    log_resulting_data_frame: bool = True,
) -> _pandas.DataFrame:
    import re
    import textwrap

    import pandas as pd
    import psycopg.rows

    if extra_cols is not None:
        if isinstance(extra_cols, str):
            extra_cols = extra_cols.strip()
        else:
            extra_cols = ",\n  ".join(extra_cols)
    extra_cols_clause = f",\n  {extra_cols}" if extra_cols else ""

    join_clause = join
    where_clause = f"WHERE {where}" if where else ""
    group_by_clause = f"GROUP BY {group_by}" if group_by else ""

    with conn:
        cur = conn.cursor(row_factory=psycopg.rows.dict_row)

        sql_stmt = f"""
SELECT
  people.id, first_name, last_name, nickname, email, gender, status, nickname, primary_group_id,
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
        if log_resulting_data_frame:
            _LOGGER.info(
                "Resulting pandas DataFrame:\n%s", textwrap.indent(str(df), "  ")
            )
        return df
