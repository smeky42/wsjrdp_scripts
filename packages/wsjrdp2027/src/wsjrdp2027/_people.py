from __future__ import annotations

import typing as _typing

if _typing.TYPE_CHECKING:
    import pandas as _pandas

    from . import _connection


def load_people_dataframe(
    ctx: _connection.ConnectionContext, *, where: str = ""
) -> _pandas.DataFrame:
    import psycopg2.extras
    import pandas as pd

    if where:
        where_clause = f'WHERE {where}'
    else:
        where_clause = ''
    with ctx.psycopg2_connect() as conn:
        cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        cur.execute(f"""
            SELECT id, first_name, last_name, email, gender, status, nickname, primary_group_id,
                sepa_name, sepa_mail, sepa_iban, sepa_bic, upload_sepa_pdf,
                payment_role, early_payer, print_at
            FROM people
            {where_clause}
            ORDER BY id
        """)

        rows = cur.fetchall()
        return pd.DataFrame(rows)
