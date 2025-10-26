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


def sepa_mandate_id_from_hitobito_id(hitobito_id: str | int) -> str:
    return f"wsjrdp2027{hitobito_id}"


def row_to_mailing_cc(row) -> list[str] | None:
    other = set(row["additional_emails_for_mailings"])
    if row["primary_group_id"] == 3 or row.get("years", 18) < 18:
        for k in ("additional_contact_email_a", "additional_contact_email_b"):
            if row[k]:
                other.add(row[k])
    for s in row["mailing_to"] or []:
        other.discard(s)
    return sorted(other)


def row_to_sepa_cc(row) -> list[str] | None:
    other = set(row["additional_emails_for_mailings"])
    if email := row["email"]:
        other.add(email)
    if row["primary_group_id"] == 3 or row.get("years", 18) < 18:
        for k in ("additional_contact_email_a", "additional_contact_email_b"):
            if row[k]:
                other.add(row[k])
    for s in row["sepa_to"] or []:
        other.discard(s)
    return sorted(filter(None, other))


def find_short_first_name(row) -> str:
    nickname = row["nickname"]
    first_names = row["first_name"].split(" ")
    if nickname and nickname in first_names:
        return nickname
    else:
        return first_names[0]


def compute_installments_cents_from_row(
    row, id2fee_rules
) -> dict[tuple[int, int], int] | None:
    from . import _payment_role, _util

    id = row["id"]
    payment_role: _payment_role.PaymentRole = row["payment_role"]
    early_payer = bool(row["early_payer"])
    print_at = _util.to_date(row["print_at"])
    today = _util.to_date(row["today"])
    fee_rules = id2fee_rules.get(id, {})
    year = _util.to_int_or_none(fee_rules.get("custom_installments_starting_year"))
    custom_installments_cents = fee_rules.get("custom_installments_cents")
    if payment_role is None:
        return None
    elif year is None or custom_installments_cents is None:
        return payment_role.get_installments_cents(
            early_payer=early_payer,
            print_at=print_at,
            today=today,
        )
    else:
        return {
            (year + (i // 12), (i % 12) + 1): cents_as_int
            for i, cents in enumerate(custom_installments_cents)
            if (cents_as_int := int(cents)) != 0
        }


def fetch_id2fee_rules(
    conn: _psycopg.Connection,
    fee_rules: str | _collections_abc.Iterable[str] = "active",
) -> dict:
    import re
    import textwrap

    import psycopg

    if isinstance(fee_rules, str):
        fee_rules = [fee_rules]
    else:
        fee_rules = list(fee_rules)
    fee_rules_str = ", ".join(f"'{s}'" for s in fee_rules)

    fee_rules_sql_stmt = f"""
SELECT
  "id",
  "people_id",
  "status",
  custom_installments_comment,
  custom_installments_issue,
  custom_installments_starting_year,
  custom_installments_cents,
  (SELECT SUM(cents) FROM UNNEST(custom_installments_cents) cents) AS custom_installments_sum_cents,
  total_fee_reduction_comment,
  COALESCE(total_fee_reduction_cents, 0) AS total_fee_reduction_cents
FROM wsj27_rdp_fee_rules
WHERE status IN ({fee_rules_str}) AND deleted_at IS NULL
ORDER BY array_position(ARRAY[{fee_rules_str}], status) ASC
        """
    fee_rules_sql_stmt = re.sub(
        r"\n+", "\n", textwrap.dedent(fee_rules_sql_stmt).strip()
    )

    _LOGGER.info(
        "Fetch wsj27_rdp_fee_rules SQL Query:\n%s",
        textwrap.indent(fee_rules_sql_stmt, "  "),
    )
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(fee_rules_sql_stmt)  # type: ignore
        fee_rules_rows = cur.fetchall()
        cur.close()

    id2fee_rules = {}
    for row in fee_rules_rows:
        id2fee_rules.setdefault(row["people_id"], row)

    return id2fee_rules


def load_people_dataframe(
    conn: _psycopg.Connection,
    *,
    extra_cols: str | list[str] | None = None,
    join: str = "",
    where: str = "",
    group_by: str = "",
    status: str | _collections_abc.Iterable[str] | None = None,
    fee_rules: str | _collections_abc.Iterable[str] = "active",
    exclude_deregistered: bool = True,
    log_resulting_data_frame: bool = True,
    today: _datetime.date | str | None = None,
) -> _pandas.DataFrame:
    import re
    import textwrap

    import pandas as pd
    import psycopg.rows

    from . import _util
    from ._payment_role import PaymentRole

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

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        sql_stmt = f"""
SELECT
  people.id, people.primary_group_id,
  people.print_at,
  people.status,
  people.first_name, people.last_name, people.nickname,
  people.birthday,
  people.email,
  people.street, people.housenumber, people.town, people.zip_code, people.country,
  people.rdp_association,
  people.rdp_association_region,
  people.rdp_association_sub_region,
  people.rdp_association_group,
  people.longitude, people.latitude,
  people.additional_contact_name_a,
  people.additional_contact_adress_a,
  people.additional_contact_email_a,
  people.additional_contact_phone_a,
  people.additional_contact_name_b,
  people.additional_contact_adress_b,
  people.additional_contact_email_b,
  people.additional_contact_phone_b,
  COALESCE(people.additional_contact_single, FALSE) AS additional_contact_single,
  ARRAY(
    SELECT a.email
    FROM additional_emails a
    WHERE a.contactable_type='Person'
      AND a.contactable_id=people.id
      AND a.mailings = TRUE
  ) AS additional_emails_for_mailings,
  ARRAY(
    SELECT tags.name
    FROM taggings
    LEFT JOIN tags ON taggings.tag_id = tags.id
      AND taggings.taggable_type = 'Person'
    WHERE taggings.taggable_id = people.id
  ) AS tags,
  people.gender,
  people.payment_role,
  COALESCE(people.sepa_status, 'ok') AS sepa_status,
  people.sepa_name,
  people.sepa_address,
  people.sepa_mail,
  people.sepa_iban,
  people.sepa_bic,
  COALESCE(people.early_payer, FALSE) AS early_payer{extra_cols_clause}
FROM people
{join_clause}
{where_clause}
{group_by_clause}
ORDER BY people.id
        """
        sql_stmt = re.sub(r"\n+", "\n", textwrap.dedent(sql_stmt).strip())

        _LOGGER.info("Fetch people SQL Query:\n%s", textwrap.indent(sql_stmt, "  "))
        cur.execute(sql_stmt)  # type: ignore
        rows = cur.fetchall()
        cur.close()

    id2fee_rules = fetch_id2fee_rules(conn, fee_rules=fee_rules)

    df = pd.DataFrame(rows)

    df["today"] = today
    df["today_de"] = df["today"].map(lambda d: d.strftime("%d.%m.%Y"))
    df["birthday_de"] = df["birthday"].map(lambda d: d.strftime("%d.%m.%Y"))
    df["age"] = df["birthday"].map(
        lambda bday: _util.compute_age(bday, today) if bday is not None else None
    )
    df["mailing_to"] = df["email"].map(lambda s: ([s] if s else None))
    df["mailing_cc"] = df.apply(row_to_mailing_cc, axis=1)
    df["sepa_to"] = df["sepa_mail"].map(lambda s: [s] if s else None)
    df["sepa_cc"] = df.apply(row_to_sepa_cc, axis=1)

    df["sepa_mandate_id"] = df["id"].map(sepa_mandate_id_from_hitobito_id)
    df["early_payer"] = df["early_payer"].map(lambda x: bool(x))
    df["payment_role"] = df["payment_role"].map(lambda s: PaymentRole(s) if s else None)  # fmt: skip
    df["total_fee_regular_cents"] = df["payment_role"].map(lambda p: (p.regular_full_fee_cents if p else None))  # fmt: skip

    def col_from_fee_rules(
        col_name, *, fee_rules_col_name=None, f=lambda val: val
    ) -> None:
        if not fee_rules_col_name:
            fee_rules_col_name = col_name
        df[col_name] = df["id"].map(
            lambda id: f(id2fee_rules.get(id, {}).get(fee_rules_col_name))
        )

    col_from_fee_rules("fee_rule_id", fee_rules_col_name="id")
    col_from_fee_rules("fee_rule_status", fee_rules_col_name="status")
    col_from_fee_rules("custom_installments_comment")
    col_from_fee_rules("custom_installments_issue")
    col_from_fee_rules("custom_installments_sum_cents")
    df["installments_cents"] = df.apply(lambda row: compute_installments_cents_from_row(row, id2fee_rules), axis=1)  # fmt: skip
    col_from_fee_rules("total_fee_reduction_cents", f=lambda val: val or 0)
    col_from_fee_rules("total_fee_reduction_comment")

    df["total_fee_cents"] = df.apply(lambda r: r["total_fee_regular_cents"] - r["total_fee_reduction_cents"], axis=1)  # fmt: skip

    df["status_de"] = df["status"].map(_STATUS_TO_DE.get)
    df["short_first_name"] = df.apply(find_short_first_name, axis=1)
    df["greeting_name"] = df.apply(
        lambda row: row["nickname"] or row["short_first_name"], axis=1
    )
    df["full_name"] = df["first_name"] + " " + df["last_name"]
    df["short_full_name"] = df["short_first_name"] + " " + df["last_name"]

    if log_resulting_data_frame:
        _LOGGER.info("Resulting pandas DataFrame:\n%s", textwrap.indent(str(df), "  "))
    return df


def write_people_dataframe_to_xlsx(
    df: _pandas.DataFrame, path: str | _pathlib.Path, *, sheet_name: str = "Sheet 1"
) -> None:
    from . import _util

    _util.write_dataframe_to_xlsx(df, path, sheet_name=sheet_name)
