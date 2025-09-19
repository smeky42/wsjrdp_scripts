from __future__ import annotations

import collections.abc as _collections_abc
import datetime as _datetime
import logging as _logging
import typing as _typing


if _typing.TYPE_CHECKING:
    import pathlib as _pathlib

    import pandas as _pandas
    import psycopg as _psycopg


_LOGGER = _logging.getLogger(__name__)


_STATUS_TO_DE = {
    "registered": "Registriert",
    "printed": "Anmeldung gedruckt",
    "upload": "Upload vollständig",
    "in_review": "Dokumente in Überprüfung durch CMT",
    "reviewed": "Dokumente vollständig überprüft",
    "confirmed": "Bestätigt durch CMT",
    "deregistration_noted": "Abmeldung Vermerkt",
    "deregistered": "Abgemeldet",
}



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
    today: _datetime.date | str | None = None,
) -> _pandas.DataFrame:
    import re
    import textwrap

    import pandas as pd
    import psycopg.rows

    from . import _util

    today = _datetime.date.today() if today is None else _util.to_date(today)

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
  people.id, people.primary_group_id,
  people.status,
  people.first_name, people.last_name, people.nickname,
  people.birthday,
  people.email,
  people.additional_contact_email_a,
  people.additional_contact_email_b,
  ARRAY(
    SELECT a.email
    FROM additional_emails a
    WHERE a.contactable_type='Person'
      AND a.contactable_id=people.id
      AND a.mailings = TRUE
  ) AS additional_emails_mailings,
  ARRAY(
    SELECT tags.name
    FROM taggings
    LEFT JOIN tags ON taggings.tag_id = tags.id
      AND taggings.taggable_type = 'Person'
    WHERE taggings.taggable_id = people.id
  ) AS tags,
  people.gender,
  people.payment_role,
  people.sepa_name, people.sepa_mail, people.sepa_iban, people.sepa_bic,
  COALESCE(people.early_payer, FALSE) AS early_payer,
  people.print_at{extra_cols_clause}
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

        def row_to_mailing_cc(row):
            other = set(row["additional_emails_mailings"])
            if row["primary_group_id"] == 3 or row.get("years", 18) < 18:
                for k in ("additional_contact_email_a", "additional_contact_email_b"):
                    if row[k]:
                        other.add(row[k])
            for s in row["mailing_to"] or []:
                other.discard(s)
            return sorted(other)

        def row_to_sepa_cc(row):
            other = set(row["additional_emails_mailings"])
            other.add(row["email"])
            if row["primary_group_id"] == 3 or row.get("years", 18) < 18:
                for k in ("additional_contact_email_a", "additional_contact_email_b"):
                    if row[k]:
                        other.add(row[k])
            for s in row["sepa_to"] or []:
                other.discard(s)
            return sorted(other)

        df["today"] = today
        df["age"] = df["birthday"].map(
            lambda bday: _util.compute_age(bday, today) if bday is not None else None
        )
        df["mailing_to"] = df["email"].map(lambda s: [s])
        df["mailing_cc"] = df.apply(row_to_mailing_cc, axis=1)
        df["sepa_to"] = df["sepa_mail"].map(lambda s: [s] if s else None)
        df["sepa_cc"] = df.apply(row_to_sepa_cc, axis=1)
        df["status_de"] = df["status"].map(_STATUS_TO_DE.get)
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


def write_people_dataframe_to_xlsx(
    df: _pandas.DataFrame, path: str | _pathlib.Path, *, sheet_name: str = "Sheet 1"
) -> None:
    import pandas as pd

    from . import _util

    df = _util.dataframe_copy_for_xlsx(df)

    _LOGGER.info("Write %s", path)
    writer = pd.ExcelWriter(
        path, engine="xlsxwriter", engine_kwargs={"options": {"remove_timezone": True}}
    )
    df.to_excel(writer, engine="xlsxwriter", index=False, sheet_name=sheet_name)
    (max_row, max_col) = df.shape

    # workbook: xlsxwriter.Workbook = writer.book  # type: ignore
    worksheet = writer.sheets[sheet_name]
    worksheet.freeze_panes(1, 0)
    worksheet.autofilter(0, 0, max_row, max_col - 1)
    worksheet.autofit()

    writer.close()
