from __future__ import annotations

import logging as _logging
import typing as _typing

if _typing.TYPE_CHECKING:
    import pandas as _pandas

    from . import _connection


_LOGGER = _logging.getLogger(__name__)


def load_people_dataframe(
    ctx: _connection.ConnectionContext, *, where: str = ""
) -> _pandas.DataFrame:
    import textwrap
    import psycopg2.extras
    import pandas as pd

    if where:
        where_clause = f'WHERE {where}'
    else:
        where_clause = ''
    with ctx.psycopg2_connect() as conn:
        cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

        sql_stmt = f"""
            SELECT id, first_name, last_name, nickname, email, gender, status, nickname, primary_group_id,
                sepa_name, sepa_mail, sepa_iban, sepa_bic, upload_sepa_pdf,
                payment_role, early_payer, print_at
            FROM people
            {where_clause}
            ORDER BY id
        """
        sql_stmt = textwrap.dedent(sql_stmt)

        _LOGGER.info("SQL Query:\n%s", sql_stmt)
        cur.execute(sql_stmt)

        rows = cur.fetchall()
        return pd.DataFrame(rows)
