from __future__ import annotations

import collections.abc as _collections_abc
import datetime as _datetime
import logging as _logging
import typing as _typing

from ._people_where import PeopleWhere as SelectPeopleConfig


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

PEOPLE_DATAFRAME_COLUMNS = [
    "id",
    "status",
    "status_de",
    "first_name",
    "last_name",
    "short_first_name",
    "nickname",
    "greeting_name",
    "full_name",
    "short_full_name",
    "street",
    "housenumber",
    "town",
    "zip_code",
    "country",
    "longitude",
    "latitude",
    "email",
    "birthday",
    "birthday_de",
    "age",
    "gender",
    "primary_group_id",
    "contract_additional_emails",
    "contract_additional_names",
    "contract_names",
    #
    "rdp_association",
    "rdp_association_region",
    "rdp_association_sub_region",
    "rdp_association_group",
    "additional_contact_name_a",
    "additional_contact_adress_a",
    "additional_contact_email_a",
    "additional_contact_phone_a",
    "additional_contact_name_b",
    "additional_contact_adress_b",
    "additional_contact_email_b",
    "additional_contact_phone_b",
    "additional_contact_single",
    "tags",
    "mailing_from",
    "mailing_to",
    "mailing_cc",
    "mailing_bcc",
    "mailing_reply_to",
    "created_at",
    "updated_at",
    "print_at",
    "contract_upload_at",
    "complete_document_upload_at",
    "today",
    "today_de",
    #
    "fee_rule_id",
    "fee_rule_status",
    "payment_role",
    "early_payer",
    "sepa_name",
    "sepa_mail",
    "sepa_address",
    "sepa_mailing_from",
    "sepa_mailing_to",
    "sepa_mailing_cc",
    "sepa_mailing_bcc",
    "sepa_mailing_reply_to",
    "sepa_iban",
    "sepa_bic",
    "sepa_bic_status",
    "sepa_bic_status_reason",
    "sepa_status",
    "collection_date",
    "sepa_mandate_id",
    "sepa_mandate_date",
    "sepa_dd_sequence_type",
    "accounting_entries_count",
    "accounting_entries_amounts_cents",
    "regular_full_fee_cents",
    "total_fee_cents",
    "total_fee_reduction_comment",
    "total_fee_reduction_cents",
    "installments_cents_dict",
    "installments_cents_sum",
    "custom_installments_comment",
    "custom_installments_issue",
    "pre_notified_amount",
    "amount_paid",
    "amount_due",
    "amount",
    "payment_status_reason",
    "payment_status",
]


def sepa_mandate_id_from_hitobito_id(hitobito_id: str | int) -> str:
    return f"wsjrdp2027{hitobito_id}"


def is_minor_or_yp(row: _pandas.Series) -> bool:
    is_minor = row["age"] < 18
    payment_role = row["payment_role"]
    if payment_role is None:
        is_yp = None
    elif isinstance(payment_role, str):
        is_yp = payment_role.endswith("::Group::Unit::Member")
    else:
        is_yp = payment_role.is_yp
    return is_yp or (row.get("primary_group_id", 0) in (3, 6)) or is_minor


def get_contract_additional_names(row: _pandas.Series) -> list[str]:
    if is_minor_or_yp(row):
        if row.get("additional_contact_single", False):
            keys = ["additional_contact_name_a"]
        else:
            keys = ["additional_contact_name_a", "additional_contact_name_b"]
        return [addr for k in keys if (addr := row.get(k, None))]
    else:
        return []


def get_contract_additional_emails(row: _pandas.Series) -> list[str]:
    if is_minor_or_yp(row):
        if row.get("additional_contact_single", False):
            keys = ["additional_contact_email_a"]
        else:
            keys = ["additional_contact_email_a", "additional_contact_email_b"]
        return [addr for k in keys if (addr := row.get(k, None))]
    else:
        return []


def get_contract_names(row: _pandas.Series) -> list[str]:
    return [
        row["full_name"],
        *(get_contract_additional_names(row) if is_minor_or_yp(row) else []),
    ]


def row_to_mailing_cc(row) -> list[str] | None:
    other = set(row["additional_emails_for_mailings"])
    other.update(get_contract_additional_emails(row))
    for s in row["mailing_to"] or []:
        other.discard(s)
    other = sorted(other)
    return other or None


def row_to_sepa_cc(row) -> list[str] | None:
    other = set(row["additional_emails_for_mailings"])
    if email := row["email"]:
        other.add(email)
    other.update(get_contract_additional_emails(row))
    for s in row["sepa_mailing_to"] or []:
        other.discard(s)
    other = sorted(filter(None, other))
    return other or None


def find_short_first_name(row) -> str:
    nickname = row["nickname"]
    first_names = row["first_name"].split(" ")
    if nickname and (
        (nickname in first_names)
        or any(nickname in f_name.split("-") for f_name in first_names)
    ):
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
    print_at = _util.to_date_or_none(row["print_at"])
    today = _util.to_date_or_none(row["today"])
    fee_rules = id2fee_rules.get(id, {})
    year = _util.to_int_or_none(fee_rules.get("custom_installments_starting_year"))
    custom_installments_cents = fee_rules.get("custom_installments_cents")
    fee_reduction_cents = fee_rules.get("total_fee_reduction_cents") or 0
    if payment_role is None:
        return None
    elif year is None or custom_installments_cents is None:
        return payment_role.get_installments_cents(
            early_payer=early_payer,
            print_at=print_at,
            today=today,
            fee_reduction_cents=fee_reduction_cents,
        )
    else:
        return {
            (year + (i // 12), (i % 12) + 1): cents_as_int
            for i, cents in enumerate(custom_installments_cents)
            if (cents_as_int := int(cents)) != 0
        }


def _compute_total_fee_cents(row: _pandas.Series) -> float | None:
    regular_full_fee_cents = row.get("regular_full_fee_cents", None)
    if regular_full_fee_cents is not None:
        return regular_full_fee_cents - row.get("total_fee_reduction_cents", 0)
    else:
        return None


def _sepa_dd_sequence_type_from_row(row) -> str:
    # FRST, RCUR, OOFF, FNAL
    if row["early_payer"]:
        return "OOFF"
    else:
        # It seems that it is OK to always use RCUR for recurring
        # payments, even if FRST or FNAL would be somewhat more
        # correct.
        return "RCUR"


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

    _LOGGER.debug(
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


def _enrich_people_dataframe(
    df: _pandas.DataFrame,
    *,
    id2fee_rules: dict,
    today: _datetime.date,
    print_at: _datetime.date | None = None,
    collection_date: _datetime.date,
    extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
) -> None:
    from . import _util
    from ._payment_role import PaymentRole

    df["today"] = today
    df["today_de"] = df["today"].map(lambda d: d.strftime("%d.%m.%Y"))
    df["birthday_de"] = df["birthday"].map(
        lambda d: d.strftime("%d.%m.%Y") if d is not None else None
    )
    if print_at is not None:
        df["print_at"] = print_at
    df["age"] = df["birthday"].map(
        lambda bday: _util.compute_age(bday, today) if bday is not None else None
    )
    df["mailing_from"] = "anmeldung@worldscoutjamboree.de"
    df["mailing_to"] = df["email"].map(lambda s: ([s] if s else None))
    df["mailing_cc"] = df.apply(row_to_mailing_cc, axis=1)
    df["mailing_bcc"] = df["id"].map(
        lambda _: _util.merge_mail_addresses(extra_mailing_bcc)
    )
    df["mailing_reply_to"] = None
    df["sepa_mailing_from"] = "anmeldung@worldscoutjamboree.de"
    df["sepa_mailing_to"] = df["sepa_mail"].map(lambda s: [s] if s else None)
    df["sepa_mailing_cc"] = df.apply(row_to_sepa_cc, axis=1)
    df["sepa_mailing_bcc"] = df["id"].map(
        lambda _: _util.merge_mail_addresses(extra_mailing_bcc)
    )
    df["sepa_mailing_reply_to"] = None

    df["sepa_mandate_id"] = df["id"].map(sepa_mandate_id_from_hitobito_id)
    df["sepa_mandate_date"] = df["print_at"].map(lambda d: d if d else collection_date)

    df["early_payer"] = df["early_payer"].map(lambda x: bool(x))
    df["payment_role"] = df["payment_role"].map(lambda s: PaymentRole(s) if s else None)  # fmt: skip
    df["regular_full_fee_cents"] = df["payment_role"].map(lambda p: (p.regular_full_fee_cents if p else None))  # fmt: skip

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
    df["installments_cents_dict"] = df.apply(lambda row: compute_installments_cents_from_row(row, id2fee_rules), axis=1)  # fmt: skip
    df["installments_cents_sum"] = df["installments_cents_dict"].map(
        lambda d: sum(d.values()) if d is not None else None
    )
    col_from_fee_rules("total_fee_reduction_cents", f=lambda val: val or 0)
    col_from_fee_rules("total_fee_reduction_comment")

    df["total_fee_cents"] = df.apply(_compute_total_fee_cents, axis=1)  # fmt: skip
    df["accounting_entries_count"] = df["accounting_entries_amounts_cents"].map(lambda amounts: len(amounts))  # fmt: skip
    df["amount_paid"] = df["accounting_entries_amounts_cents"].map(sum)
    df["sepa_dd_sequence_type"] = df.apply(_sepa_dd_sequence_type_from_row, axis=1)

    df["status_de"] = df["status"].map(_STATUS_TO_DE.get)
    df["short_first_name"] = df.apply(find_short_first_name, axis=1)
    df["greeting_name"] = df.apply(
        lambda row: row["nickname"] or row["short_first_name"], axis=1
    )
    df["full_name"] = df["first_name"] + " " + df["last_name"]
    df["short_full_name"] = df["short_first_name"] + " " + df["last_name"]
    df["contract_additional_emails"] = df.apply(get_contract_additional_emails, axis=1)
    df["contract_additional_names"] = df.apply(get_contract_additional_names, axis=1)
    df["contract_names"] = df.apply(get_contract_names, axis=1)

    assert_all_people_rows_consistent(df)
    df.drop(
        columns=[
            "additional_emails_for_mailings",
            "custom_installments_sum_cents",
        ],
        inplace=True,
    )


def _extract_col_name(col: str) -> str | None:
    if col.isidentifier():
        return col
    else:
        return None


def load_people_dataframe(
    conn: _psycopg.Connection,
    *,
    extra_cols: str | list[str] | None = None,
    join: str = "",
    where: str | SelectPeopleConfig | None = "",
    group_by: str = "",
    status: str | _collections_abc.Iterable[str] | None = None,
    early_payer: bool | None = None,
    sepa_status: str | _collections_abc.Iterable[str] | None = None,
    fee_rules: str | _collections_abc.Iterable[str] = "active",
    exclude_deregistered: bool | None = None,
    log_resulting_data_frame: bool | None = None,
    today: _datetime.date | str | None = None,
    collection_date: _datetime.date | str | None = None,
    print_at: _datetime.date | str | None = None,
    extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
    extra_static_df_cols: dict[str, _typing.Any] | None = None,
) -> _pandas.DataFrame:
    import re
    import textwrap

    import pandas as pd
    import psycopg.rows

    from . import _util

    today = _util.to_date_or_none(today) or _datetime.date.today()
    print_at = _util.to_date_or_none(print_at)
    collection_date = _util.to_date_or_none(collection_date) or today

    extra_out_cols = []
    if extra_cols is not None:
        if isinstance(extra_cols, str):
            extra_cols = [extra_cols.strip()]
        else:
            extra_cols = [x.strip() for x in extra_cols] or None
    if extra_cols:
        extra_out_cols = [
            col_name for col in extra_cols if (col_name := _extract_col_name(col))
        ]
        extra_cols_clause = ",\n  " + ",\n  ".join(extra_cols)
    else:
        extra_cols_clause = ""

    join_clause = join

    if where is None:
        where = ""
    elif isinstance(where, SelectPeopleConfig):
        if exclude_deregistered is None:
            exclude_deregistered = where.exclude_deregistered
        where = where.as_where_condition(people_table="people")

    if exclude_deregistered is None:
        exclude_deregistered = True

    status = _util.to_str_list(status)
    if status is not None:
        where = _util.combine_where(where, _util.in_expr("people.status", status))
    elif exclude_deregistered:
        where = _util.combine_where(
            where, "people.status NOT IN ('deregistration_noted', 'deregistered')"
        )
    if early_payer is not None:
        if early_payer:
            where = _util.combine_where(where, "people.early_payer = TRUE")
        else:
            where = _util.combine_where(
                where, "(people.early_payer = FALSE OR people.early_payer IS NULL)"
            )
    if sepa_status is not None:
        where = _util.combine_where(
            where, _util.in_expr("COALESCE(people.sepa_status, 'ok')", sepa_status)
        )

    where_clause = f"WHERE {where}" if where else ""
    group_by_clause = f"GROUP BY {group_by}" if group_by else ""

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        sql_stmt = f"""
SELECT
  people.id,
  people.primary_group_id,
  people.created_at,
  people.updated_at,
  people.print_at,
  people.contract_upload_at,
  people.complete_document_upload_at,
  people.status,
  people.first_name, people.last_name, people.nickname,
  people.birthday,
  people.email,
  people.street, people.housenumber, people.town, people.zip_code, people.country,
  people.longitude,
  people.latitude,
  people.gender,
  people.rdp_association,
  people.rdp_association_region,
  people.rdp_association_sub_region,
  people.rdp_association_group,
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
  ARRAY(
    SELECT COALESCE(e.amount_cents, 0)
    FROM accounting_entries AS e
    WHERE e.subject_type = 'Person' AND e.subject_id = people."id" AND COALESCE(e.amount_currency, 'EUR') = 'EUR'
  ) AS accounting_entries_amounts_cents,
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

        if "\n" in where_clause:
            _LOGGER.info(
                "Fetch people SQL where clause:\n%s",
                textwrap.indent(where_clause, "  "),
            )
        else:
            _LOGGER.info("Fetch people %s", where_clause)
        _LOGGER.debug("Fetch people SQL Query:\n%s", textwrap.indent(sql_stmt, "  "))
        cur.execute(sql_stmt)  # type: ignore
        rows = cur.fetchall()
        cur.close()

    df = pd.DataFrame(rows)

    if len(df) != 0:
        id2fee_rules = fetch_id2fee_rules(conn, fee_rules=fee_rules)
        _enrich_people_dataframe(
            df,
            id2fee_rules=id2fee_rules,
            today=today,
            print_at=print_at,
            collection_date=collection_date,
            extra_mailing_bcc=extra_mailing_bcc,
        )
    if not (set(df) <= set(PEOPLE_DATAFRAME_COLUMNS)):
        warn_msg = "Some columns of the resulting dataframe are not listed in PEOPLE_DATAFRAME_COLUMNS"
        for col_name in list(df):
            if col_name not in PEOPLE_DATAFRAME_COLUMNS:
                warn_msg += (
                    f'\n  column "{col_name}" not present in PEOPLE_DATAFRAME_COLUMNS'
                )
        _LOGGER.warning(warn_msg)
    columns = PEOPLE_DATAFRAME_COLUMNS[:]
    if extra_out_cols:
        columns.extend(extra_out_cols)
    if extra_static_df_cols:
        columns.extend(extra_static_df_cols.keys())
        for key, val in extra_static_df_cols.items():
            df[key] = val
    df = df.reindex(columns=columns)

    if log_resulting_data_frame or (log_resulting_data_frame is None):
        _LOGGER.info("Resulting pandas DataFrame:\n%s", textwrap.indent(str(df), "  "))
    return df


def assert_all_people_rows_consistent(df: _pandas.DataFrame) -> None:
    import textwrap

    def nan_to_none(num: float | None) -> float | None:
        import math

        if num is None:
            return None
        if not isinstance(num, float):
            try:
                num = float(num)
            except Exception:
                return None
        if math.isnan(num):
            return None
        else:
            return num

    inconsistent_ids_set = set()
    for _, row in df.iterrows():
        installments_cents_sum = nan_to_none(row["installments_cents_sum"])
        total_fee_cents = nan_to_none(row["total_fee_cents"])
        if installments_cents_sum != total_fee_cents:
            inconsistent_ids_set.add(row["id"])
            _LOGGER.warning(
                "Inconsistent row (installments_cents_sum != total_fee_cents):\n%s",
                textwrap.indent(row.to_string(), "    | "),
            )
            _LOGGER.warning(
                "  id: %s short_full_name: %s", row["id"], row["short_full_name"]
            )
            _LOGGER.warning(
                "  installments_cents_sum: %s", row["installments_cents_sum"]
            )
            _LOGGER.warning("  total_fee_cents: %s", row["total_fee_cents"])
    inconsistent_ids = sorted(inconsistent_ids_set)
    inconsistent_df = df[df["id"].isin(inconsistent_ids_set)]
    if inconsistent_ids:
        err_msg = (
            f"Found {len(inconsistent_ids)} inconsistent rows:\n"
            + textwrap.indent(str(inconsistent_df), "  | ")
        )
        _LOGGER.error("%s", err_msg)
        raise RuntimeError(err_msg)


def write_people_dataframe_to_xlsx(
    df: _pandas.DataFrame,
    path: str | _pathlib.Path,
    *,
    sheet_name: str = "Sheet 1",
    log_level: int | None = None,
) -> None:
    from . import _util

    _util.write_dataframe_to_xlsx(df, path, sheet_name=sheet_name, log_level=log_level)
