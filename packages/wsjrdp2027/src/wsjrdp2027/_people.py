from __future__ import annotations

import collections.abc as _collections_abc
import datetime as _datetime
import logging as _logging
import typing as _typing

from . import _people_query, _util


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
    "id_and_name",
    "role_id_name",
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
    "unit_code",
    "roles",
    "primary_group_roles",
    "primary_group_role_types",
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
    "tag_list",
    "note_list",
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
    "sepa_bank_name",
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
    "pre_notified_amount_cents",
    "amount_paid_cents",  # bereits bezahlt
    "amount_unpaid_cents",  # insgesamt offen
    "amount_due_cents",  # fällig gesamt (at collection_date)
    "open_amount_cents",  # Betrag der offen ist (at collection_date)
    "amount_due_in_collection_date_month_cents",
    "additional_info",
    "payment_status_reason",
    "payment_status",
    "person_dict",
    #
    "skip_db_updates",
]


def is_minor_or_yp(row: _pandas.Series) -> bool:
    age = int(row["age"])
    is_minor = age < 18
    payment_role = row["payment_role"]
    if payment_role is None:
        is_yp = None
    elif isinstance(payment_role, str):
        is_yp = payment_role.endswith("::Group::Unit::Member")
    else:
        is_yp = payment_role.is_yp
    return is_yp or is_minor


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


def _row_to_mailing_to(
    row: _pandas.Series, query: _people_query.PeopleQuery
) -> list[str] | None:
    from . import _util

    wsjrdp_email = row.get("additional_info", {}).get("wsjrdp_email")
    if query.include_sepa_mail_in_mailing_to:
        candidates = [row.get("email"), wsjrdp_email, row.get("sepa_mail")]
    else:
        candidates = [row.get("email"), wsjrdp_email]
    return _util.merge_mail_addresses(*candidates)


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


def _compute_installments_cents_dict_from_row(
    row, id2fee_rules
) -> dict[tuple[int, int], int] | None:
    from . import _payment_role, _util

    id = row["id"]
    payment_role: _payment_role.PaymentRole = row["payment_role"]
    if payment_role is None and row.get("status") in [
        "registered",
        "deregistration_noted",
        "deregistered",
    ]:
        return {(2025, 1): 0}
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


def _compute_regular_full_fee_cents(row: _pandas.Series) -> float:
    payment_role = row.get("payment_role")
    if payment_role:
        return payment_role.regular_full_fee_cents
    else:
        status = row.get("status")
        if status in ["registered", "deregistration_noted", "deregistered"]:
            return 0
        else:
            return 10_000_000_00


def _compute_total_fee_cents(row: _pandas.Series) -> float | None:
    regular_full_fee_cents = _util.nan_to_none(row.get("regular_full_fee_cents", None))
    if regular_full_fee_cents is not None:
        return regular_full_fee_cents - row.get("total_fee_reduction_cents", 0)
    else:
        return None


def _sepa_dd_sequence_type_from_row(row) -> str:
    # FRST, RCUR, OOFF, FNAL
    installments_dict = row.get("installments_cents_dict") or {}
    if row["early_payer"] or len(installments_dict) == 1:
        return "OOFF"
    else:
        # It seems that it is OK to always use RCUR for recurring
        # payments, even if FRST or FNAL would be more correct.
        return "RCUR"


def fee_due_by_date_in_cent_from_plan(
    date: _datetime.date, installments_cents: dict[tuple[int, int], int], *, row=None
) -> int:
    import bisect
    import itertools
    import textwrap

    plan = [
        (_datetime.date(year, month, 5), cents)
        for (year, month), cents in sorted(
            installments_cents.items(), key=lambda item: item[0]
        )
    ]
    dates = [_datetime.date.min, *(x[0] for x in plan), _datetime.date.max]
    installments = [0, *(x[1] for x in plan), 0]
    accumulated = list(itertools.accumulate(installments))
    pos = max(bisect.bisect_right(dates, date) - 1, 0)
    cents_due = accumulated[pos]
    if row is not None:
        installments_str = "\n".join(
            f"{'*' if idx == pos else ' '} {d}: {i / 100:9.2f} EUR  / {a / 100:9.2f} EUR"
            for idx, (d, i, a) in enumerate(zip(dates, installments, accumulated))
        )
        _LOGGER.debug(
            "%s fee due by %s: %s EUR\n%s\n  | -> pos=%s",
            row["id_and_name"],
            date,
            cents_due / 100,
            textwrap.indent(installments_str, "  | "),
            pos,
        )
    return cents_due


def _compute_amount_due_cents(row) -> int:
    installments_cents = _util.nan_to_none(row["installments_cents_dict"])
    collection_date: _datetime.date | None = _util.nan_to_none(row["collection_date"])
    if installments_cents is None or collection_date is None:
        return 0
    else:
        return fee_due_by_date_in_cent_from_plan(
            collection_date, installments_cents, row=row
        )


def _compute_amount_due_in_collection_date_month_centsamount_due_cent(row) -> int:
    from . import _util

    installments_cents = _util.nan_to_none(row["installments_cents_dict"])
    collection_date: _datetime.date | None = _util.nan_to_none(row["collection_date"])
    if installments_cents is None or collection_date is None:
        return 0
    else:
        collection_date_ym = _util.to_year_month(collection_date)
        return sum(
            cents
            for ym, cents in installments_cents.items()
            if ym == collection_date_ym
        )


def _compute_open_amount_cents(row: _pandas.Series) -> int:
    return max(row["amount_due_cents"] - row["amount_paid_cents"], 0)


def _iban_to_bank_name(iban: str) -> str | None:
    import schwifty

    try:
        return schwifty.IBAN(iban).bank_name
    except Exception as exc:
        _LOGGER.debug("Could not determine bank name for IBAN %r: %s", iban, str(exc))
    return None


def _fetch_id2fee_rules(
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


def _fetch_id2roles(
    conn: _psycopg.Connection,
    /,
    *,
    df: _pandas.DataFrame,
    today: _datetime.date | str | None = None,
) -> dict[int, list[dict[str, _typing.Any]]]:
    from . import _pg

    ids = list(df["id"])

    return _pg.pg_fetch_role_dicts_for_person_ids(conn, ids=ids, today=today)


def _fetch_id2person_dicts(
    conn: _psycopg.Connection, /, *, df: _pandas.DataFrame
) -> dict[int, dict[str, _typing.Any]]:
    from . import _pg

    ids = list(df["id"])
    id2p = _pg.pg_fetch_person_dicts_for_ids(conn, ids=ids)
    return id2p


def _select_primary_group_roles(row: _pandas.Series) -> list[dict[str, _typing.Any]]:
    id: int = row["id"]
    primary_group_id: int = row["primary_group_id"]
    roles: list[dict[str, _typing.Any]] = row.get("roles", [])
    assert all(role["person_id"] == id for role in roles)
    return [r for r in roles if r["group_id"] == primary_group_id]


def _roles_to_primary_group_role_types(row: _pandas.Series) -> list[str]:
    roles: list[dict[str, _typing.Any]] = row.get("primary_group_roles", [])
    return [r["type"] for r in roles]


def _filtered_join(*args, sep=" "):
    import math

    return sep.join(
        str(a)
        for a in args
        if not (
            a is None  # filter out None
            or (isinstance(a, float) and math.isnan(a))  # filter out NaN
        )
    )


def _compute_short_full_name(row) -> str:
    maybe_short_last_name = row.get("additional_info", {}).get("short_last_name")
    if maybe_short_last_name:
        return _filtered_join(row["short_first_name"], str(maybe_short_last_name))
    else:
        return _filtered_join(row["short_first_name"], row["last_name"])


def _compute_role_id_name(row) -> str:
    short_role_name = getattr(row.get("payment_role"), "short_role_name", None)
    return _filtered_join(short_role_name, row["id"], row["short_full_name"])


def _enrich_people_dataframe(
    df: _pandas.DataFrame,
    *,
    query: _people_query.PeopleQuery,
    id2fee_rules: dict,
    id2roles: dict[int, list[dict[str, _typing.Any]]],
    id2person_dicts: dict[int, dict],
    today: _datetime.date,
    collection_date: _datetime.date | None = None,
    extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
    skip_db_updates: bool | None = None,
) -> None:
    from . import _util
    from ._payment_role import PaymentRole

    df["short_first_name"] = df.apply(find_short_first_name, axis=1)
    df["greeting_name"] = df.apply(
        lambda row: row["nickname"] or row["short_first_name"], axis=1
    )
    df["full_name"] = df.apply(
        lambda r: _filtered_join(r["first_name"], r["last_name"]), axis=1
    )
    df["short_full_name"] = df.apply(_compute_short_full_name, axis=1)
    df["id_and_name"] = df.apply(
        lambda r: _filtered_join(r["id"], r["short_full_name"]), axis=1
    )
    df["today"] = today
    df["age"] = df["birthday"].map(
        lambda bday: _util.compute_age(bday, today) if bday is not None else None
    )

    df["roles"] = df.apply(lambda r: id2roles.get(r["id"], []), axis=1)
    df["person_dict"] = df.apply(lambda r: id2person_dicts.get(r["id"], {}), axis=1)
    df["primary_group_roles"] = df.apply(_select_primary_group_roles, axis=1)
    df["primary_group_role_types"] = df.apply(
        _roles_to_primary_group_role_types, axis=1
    )
    df["today_de"] = df["today"].map(lambda d: d.strftime("%d.%m.%Y"))
    df["birthday_de"] = df["birthday"].map(
        lambda d: d.strftime("%d.%m.%Y") if d is not None else None
    )
    df["mailing_from"] = "anmeldung@worldscoutjamboree.de"
    df["mailing_to"] = df.apply(lambda r: _row_to_mailing_to(r, query), axis=1)
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

    df["sepa_bank_name"] = df["sepa_iban"].map(_iban_to_bank_name)
    df["sepa_mandate_id"] = df["id"].map(_util.sepa_mandate_id_from_hitobito_id)
    df["sepa_mandate_date"] = df["print_at"].map(lambda d: d if d else collection_date)

    df["early_payer"] = df["early_payer"].map(lambda x: bool(x))
    df["payment_role"] = df["payment_role"].map(lambda s: PaymentRole(s) if s else None)  # fmt: skip
    df["role_id_name"] = df.apply(_compute_role_id_name, axis=1)
    df["regular_full_fee_cents"] = df.apply(_compute_regular_full_fee_cents, axis=1)

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
    df["installments_cents_dict"] = df.apply(lambda row: _compute_installments_cents_dict_from_row(row, id2fee_rules), axis=1)  # fmt: skip
    df["installments_cents_sum"] = df["installments_cents_dict"].map(
        lambda d: sum(d.values()) if d is not None else None
    )
    col_from_fee_rules("total_fee_reduction_cents", f=lambda val: val or 0)
    col_from_fee_rules("total_fee_reduction_comment")

    df["total_fee_cents"] = df.apply(_compute_total_fee_cents, axis=1)  # fmt: skip
    df["accounting_entries_count"] = df["accounting_entries_amounts_cents"].map(lambda amounts: len(amounts))  # fmt: skip
    df["collection_date"] = collection_date
    df["amount_paid_cents"] = df["accounting_entries_amounts_cents"].map(sum)
    df["amount_unpaid_cents"] = df.apply(lambda r: max(r["total_fee_cents"] - r["amount_paid_cents"], 0), axis=1)  # fmt: skip
    df["amount_due_cents"] = df.apply(_compute_amount_due_cents, axis=1)
    df["amount_due_in_collection_date_month_cents"] = df.apply(
        _compute_amount_due_in_collection_date_month_centsamount_due_cent, axis=1
    )
    df["open_amount_cents"] = df.apply(_compute_open_amount_cents, axis=1)

    df["sepa_dd_sequence_type"] = df.apply(_sepa_dd_sequence_type_from_row, axis=1)

    df["status_de"] = df["status"].map(_STATUS_TO_DE.get)
    df["contract_additional_emails"] = df.apply(get_contract_additional_emails, axis=1)
    df["contract_additional_names"] = df.apply(get_contract_additional_names, axis=1)
    df["contract_names"] = df.apply(get_contract_names, axis=1)

    df["skip_db_updates"] = bool(skip_db_updates)  # None => False

    assert_all_people_rows_consistent(df)
    df.drop(
        columns=[
            "additional_emails_for_mailings",
            "custom_installments_sum_cents",
        ],
        inplace=True,
    )


def load_people_dataframe(
    conn: _psycopg.Connection,
    *,
    extra_cols: str | list[str] | None = None,
    join: str = "",
    query: _people_query.PeopleQuery | None = None,
    where: str | _people_query.PeopleWhere | None = "",
    group_by: str = "",
    fee_rules: str | _collections_abc.Iterable[str] | None = None,
    log_resulting_data_frame: bool | None = None,
    now: _datetime.datetime | _datetime.date | str | int | float | None = None,
    print_at: _datetime.date | str | None = None,
    extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
    extra_static_df_cols: dict[str, _typing.Any] | None = None,
    skip_db_updates: bool | None = None,
    accounting_entry_exclude_payment_initiation_id: _collections_abc.Iterable[int]
    | int
    | None = None,
) -> _pandas.DataFrame:
    import re
    import textwrap

    import pandas as pd
    import psycopg.rows

    from . import _people_query, _util

    if query:
        if where:
            raise ValueError("Only one of 'query' and 'where' is allowed")
        else:
            where = query.where
        if now is None:
            now = query.now
    else:
        query = _people_query.PeopleQuery(where=where)

    now = _util.to_datetime(now)
    today = now.date()
    print_at = _util.to_date_or_none(print_at)

    if extra_cols is not None:
        if isinstance(extra_cols, str):
            extra_cols = [extra_cols.strip()]
        else:
            extra_cols = [x.strip() for x in extra_cols] or None
    if extra_cols:
        extra_cols_clause = ",\n  " + ",\n  ".join(extra_cols)
    else:
        extra_cols_clause = ""

    join_clause = join

    if where is None:
        where = ""
    elif isinstance(where, _people_query.PeopleWhere):
        if fee_rules is None:
            fee_rules = where.fee_rules
        where = where.as_where_condition(people_table="people")

    where_clause = f"WHERE {where}" if where else ""
    group_by_clause = f"GROUP BY {group_by}" if group_by else ""

    if query.limit is not None:
        limit_clause = f"\nLIMIT {query.limit}"
    else:
        limit_clause = ""

    accounting_entry_exclude_payment_initiation_id = _util.to_int_list_or_none(
        accounting_entry_exclude_payment_initiation_id
    )
    if accounting_entry_exclude_payment_initiation_id:
        accounting_entry_where_extra = (
            ' AND ("e".payment_initiation_id IS NULL OR "e".'
            + _util.not_in_expr(
                "payment_initiation_id", accounting_entry_exclude_payment_initiation_id
            )
            + ")"
        )
    else:
        accounting_entry_where_extra = ""

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        sql_stmt = f"""
WITH "people" AS (
  SELECT
    *,
    ARRAY(SELECT "tags".name FROM taggings
          LEFT JOIN "tags" ON taggings.tag_id = "tags".id AND taggings.taggable_type = 'Person'
          WHERE taggings.taggable_id = people.id ORDER BY "tags"."name"
    ) AS tag_list,
    ARRAY(SELECT "a".email FROM additional_emails "a"
          WHERE "a".contactable_type='Person' AND "a".contactable_id = people.id AND "a".mailings = TRUE
    ) AS additional_emails_for_mailings,
    ARRAY(SELECT COALESCE("e".amount_cents, 0) FROM accounting_entries AS "e"
          WHERE "e".subject_type = 'Person' AND "e".subject_id = people."id" AND COALESCE("e".amount_currency, 'EUR') = 'EUR'{accounting_entry_where_extra}
    ) AS accounting_entries_amounts_cents,
    ARRAY(SELECT "n".text FROM "notes" AS "n"
          WHERE "n".subject_type = 'Person' AND "n".subject_id = people."id"
    ) AS "note_list"
  FROM "people")
SELECT
  people.id, people.primary_group_id, people.unit_code,
  people.created_at, people.updated_at,
  people.print_at, people.contract_upload_at, people.complete_document_upload_at,
  people.status,
  people.first_name, people.last_name, people.nickname, people.birthday,
  people.email,
  people.street, people.housenumber, people.town, people.zip_code, people.country,
  people.longitude, people.latitude,
  people.gender,
  people.rdp_association, people.rdp_association_region, people.rdp_association_sub_region, people.rdp_association_group,
  people.additional_contact_name_a, people.additional_contact_adress_a,
  people.additional_contact_email_a, people.additional_contact_phone_a,
  people.additional_contact_name_b, people.additional_contact_adress_b,
  people.additional_contact_email_b, people.additional_contact_phone_b,
  COALESCE(people.additional_contact_single, FALSE) AS additional_contact_single,
  people.additional_emails_for_mailings,
  people.tag_list, people.note_list,
  people.payment_role,
  people.accounting_entries_amounts_cents,
  COALESCE(people.sepa_status, 'ok') AS sepa_status,
  people.sepa_name, people.sepa_address, people.sepa_mail, people.sepa_iban, people.sepa_bic,
  COALESCE(people.early_payer, FALSE) AS early_payer,
  people.additional_info{extra_cols_clause}
FROM people
{join_clause}
{where_clause}
{group_by_clause}
ORDER BY people.id{limit_clause}
        """
        sql_stmt = re.sub(r"\n+", "\n", textwrap.dedent(sql_stmt).strip())

        if "\n" in where_clause:
            _LOGGER.info("load_people_dataframe: Fetch people...")
        elif len(where_clause) > 80:
            _LOGGER.info(
                "load_people_dataframe: Fetch people %s ...[shortened]...  %s",
                where_clause[:50],
                where_clause[-20:],
            )
        else:
            _LOGGER.info("load_people_dataframe: Fetch people %s", where_clause)
        _LOGGER.debug(
            "load_people_dataframe: Fetch people SQL Query:\n%s",
            textwrap.indent(sql_stmt, "  "),
        )
        cur.execute(sql_stmt)  # type: ignore
        rows = cur.fetchall()
        cur.close()

    df = pd.DataFrame(rows)

    if len(df) != 0:
        if fee_rules is None:
            fee_rules = "active"
        id2fee_rules = _fetch_id2fee_rules(conn, fee_rules=fee_rules)
        id2roles = _fetch_id2roles(conn, df=df, today=today)
        id2person_dicts = _fetch_id2person_dicts(conn, df=df)
        _enrich_people_dataframe(
            df,
            query=query,
            id2fee_rules=id2fee_rules,
            id2roles=id2roles,
            id2person_dicts=id2person_dicts,
            today=today,
            collection_date=query.collection_date,
            extra_mailing_bcc=extra_mailing_bcc,
            skip_db_updates=skip_db_updates,
        )
        df_columns = set(df.columns)
        for key, val in (extra_static_df_cols or {}).items():
            assert key not in df_columns, f"Cannot overwrite existing column {key}"
            df[key] = df.apply(lambda r: val, axis=1)
    if not (set(df) <= set(PEOPLE_DATAFRAME_COLUMNS)):
        warn_msg = "load_people_dataframe: Some columns of the resulting dataframe are not listed in PEOPLE_DATAFRAME_COLUMNS"
        for col_name in list(df):
            if col_name not in PEOPLE_DATAFRAME_COLUMNS:
                warn_msg += (
                    f'\n  column "{col_name}" not present in PEOPLE_DATAFRAME_COLUMNS'
                )
        _LOGGER.warning(warn_msg)
    extra_columns = [
        col for col in df.columns if col not in frozenset(PEOPLE_DATAFRAME_COLUMNS)
    ]
    _LOGGER.debug("load_people_dataframe: detected extra columns: %s", extra_columns)
    columns = PEOPLE_DATAFRAME_COLUMNS[:] + extra_columns
    df = df.reindex(columns=columns)

    if query.collection_date is not None:
        _LOGGER.info(
            "load_people_dataframe: query.collection_date = %s given => enrich with payment information",
            query.collection_date,
        )
        from . import _payment

        df = _payment.enrich_people_dataframe_for_payments(
            df,
            collection_date=query.collection_date,
            pedantic=False,
            reindex=False,
        )
    else:
        _LOGGER.debug("load_people_dataframe: query.collection_date is None")

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
            f"Found {len(inconsistent_ids)} inconsistent row(s):\n"
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
    drop_columns: _collections_abc.Iterable[str] = (
        "id_and_name",
        "person_dict",
        "status_de",
        "birthday_de",
        "today_de",
    ),
) -> None:
    from . import _util

    _util.write_dataframe_to_xlsx(
        df, path, sheet_name=sheet_name, log_level=log_level, drop_columns=drop_columns
    )


def update_dataframe_for_updates(
    df: _pandas.DataFrame,
    *,
    updates: dict | None = None,
    now: _datetime.datetime | _datetime.date | str | int | float | None = None,
):
    import collections

    from . import _person_pg, _util

    now = _util.to_datetime(now)
    updates = updates or {}

    used_changes_set = set()
    used_changes = []

    new_cols = []

    for key, new_val in updates.items():
        if key not in _person_pg.VALID_PERSON_UPDATE_KEYS:
            raise TypeError(f"Invalid keyword argument {key!r}")

        chg = _person_pg.UPDATE_KEY_TO_CHANGE[key]
        if chg not in used_changes_set:
            used_changes.append(chg)
        used_changes_set.add(chg)
        if len(df) == 0:
            new_cols.append(key)
        else:
            df[key] = df.apply(
                lambda row: chg.compute_df_val(
                    row, column=key, value=new_val, updates=updates
                ),
                axis=1,
            )

    if len(df) == 0:
        if used_changes:
            new_cols.extend(["db_changes", "person_changes"])
        return df.reindex(columns=list(df.columns) + new_cols)

    if used_changes:
        df["db_changes"] = False
        df["person_changes"] = df.apply(lambda _: {}, axis=1)

        idx: int
        for idx, row in df.iterrows():  # type: ignore
            id = row["id"]
            person_dict = collections.ChainMap(row["person_dict"], row.to_dict())
            changed = False
            object_changes = {}
            for chg in used_changes:
                if not chg.old_col:
                    new_val = chg.get_new_val(row)
                    if new_val or (chg.new_col != "add_note"):
                        changed = True
                    _LOGGER.debug("{%s} %s", id, chg.col_name)
                    _LOGGER.debug("{%s} + %s", id, new_val)
                    continue
                else:
                    old_val = chg.get_old_val(person_dict)
                    new_val = chg.get_new_val(row)
                    _LOGGER.debug("{%s} compare %s", id, chg.old_col)
                    _LOGGER.debug("{%s} - %s", id, old_val)
                    _LOGGER.debug("{%s} + %s", id, new_val)
                    if new_val != old_val:
                        changed = True
                        object_changes[chg.old_col] = [old_val, new_val]
            df.at[idx, "db_changes"] = changed
            df.at[idx, "person_changes"] = object_changes  # type: ignore

    return df


def update_postgres_db_for_dataframe(
    cursor: _psycopg.Cursor,
    df: _pandas.DataFrame,
    /,
    write_versions: bool | None = None,
    dry_run: bool | None = None,
    skip_db_updates: bool | None = None,
    now: _datetime.datetime | _datetime.date | str | int | float | None = None,
    logger: _logging.Logger | _logging.LoggerAdapter = _LOGGER,
    ctx=None,
) -> None:
    import psycopg as _psycopg

    from . import _util

    if dry_run is None:
        dry_run = False
    if write_versions is None:
        write_versions = True
    now = _util.to_datetime(now)

    db_changes = any(df.get("db_changes", [False]))
    skip_reasons = []
    if not db_changes:
        skip_reasons.append("No DB updates")
    if dry_run:
        skip_reasons.append("dry_run is True")
    if skip_db_updates:
        skip_reasons.append("skip_db_updates is true")
    if skip_reasons:
        logger.info("Skip DB updates (%s)", ", ".join(skip_reasons))
        return
    logger.info("Update DB")
    if ctx:
        ctx.require_approval_to_run_in_prod()

    skipped_ids = set()
    failed_ids = set()
    df_len = len(df)
    with cursor.connection.transaction() as db_tx:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            pcnt = (i / df_len) * 100.0
            id = row["id"]
            id_and_name = row.get("id_and_name", str(id))
            db_changes = row["db_changes"]
            person_changes = row["person_changes"]
            summary = f"{i}/{df_len} ({pcnt:.1f}) {id_and_name}: db_changes={db_changes} person_changes={person_changes}"
            logger.info(summary)
            if not db_changes:
                skipped_ids.add(id)
                logger.debug("  Skip %s (no changes)", id_and_name)
                continue
            elif _util.nan_to_none(row.get("skip_db_updates")):
                skipped_ids.add(id)
                logger.info("  Skip %s (due to row['skip_db_updates'])", id_and_name)
                continue
            try:
                _update_person_from_row(
                    cursor,
                    row=row,
                    write_versions=write_versions,
                    now=now,
                    logger=logger,
                    transaction=db_tx,
                )
            except Exception as exc:
                logger.exception("Failed to update %s: %s", id_and_name, str(exc))
                failed_ids.add(id)
        if failed_ids:
            logger.error("")
            logger.error("ROLLBACK: Failed to update people")
            logger.error("  failed_ids: %s", sorted(failed_ids))
            logger.error("")
            raise _psycopg.Rollback(db_tx)
    if failed_ids:
        raise RuntimeError(
            f"DB Transaction ROLLBACK: Failed to update people: failed_ids={sorted(failed_ids)}"
        )


def _update_person_from_row(
    cursor: _psycopg.Cursor,
    /,
    *,
    row: _pandas.Series,
    write_versions: bool,
    logger: _logging.Logger | _logging.LoggerAdapter,
    now: _datetime.datetime | _datetime.date | str | int | float | None = None,
    transaction: _psycopg.Transaction,
) -> None:
    from . import _person_pg, _pg, _util

    now = _util.to_datetime(now)

    _update_roles(
        cursor, row=row, now=now, write_versions=write_versions, logger=logger
    )

    if write_versions:
        if person_changes_for_version := row["person_changes"]:
            person_changes_for_version = person_changes_for_version.copy()
            person_changes_for_version.pop("primary_group_role_types", None)
            _pg.pg_insert_version(
                cursor,
                main_id=row["id"],
                changes=person_changes_for_version,
                created_at=now,
            )
    # Note: Writing a versions row for a person requires reading the
    # respective row from the people table, so we update the people
    # table only after writing the versions entries.
    person_updates = []
    person_changes = row["person_changes"]
    if "tag_list" in person_changes:
        for tag_name in _util.to_str_list(row.get("add_tags")):
            _pg.pg_add_person_tag(cursor, person_id=row["id"], tag_name=tag_name)
        for tag_name in _util.to_str_list(row.get("remove_tags")):
            _pg.pg_remove_person_tag(cursor, person_id=row["id"], tag_name=tag_name)
    if (add_note := row.get("add_note")) is not None:
        _pg.pg_insert_note(cursor, subject_id=row["id"], text=add_note)
    for chg in _person_pg.PERSON_CHANGES:
        if chg.old_col in ["add_note", "primary_group_role_types", "tag_list"]:
            continue
        if chg.col_name in row.index and chg.old_col in person_changes:
            person_updates.append((chg.old_col, row.get(chg.col_name)))
    _pg.pg_update_person(cursor, id=row["id"], updates=person_updates)


def _update_roles(
    cursor: _psycopg.Cursor,
    /,
    *,
    row: _pandas.Series,
    now: _datetime.datetime,
    write_versions: bool = True,
    logger: _logging.Logger | _logging.LoggerAdapter = _LOGGER,
) -> None:
    from . import _pg

    today = now.date()
    yesterday = today - _datetime.timedelta(days=1)
    person_changes = row["person_changes"]

    primary_group_id = row["primary_group_id"]
    roles = row["primary_group_roles"]
    id2role = {role["id"]: role for role in roles}
    role_types = row["primary_group_role_types"]

    if "new_primary_group_id" in row.index and "primary_group_id" in person_changes:
        new_primary_group_id = row["new_primary_group_id"]
        new_role_types = row.get("new_primary_group_role_types") or role_types
    elif (
        "new_primary_group_role_types" in row.index
        and "primary_group_role_types" in person_changes
    ):
        new_primary_group_id = primary_group_id
        new_role_types = row["new_primary_group_role_types"]
    else:
        logger.debug("No role change")
        return
    logger.debug("  primary_group_id: %s -> %s", primary_group_id, new_primary_group_id)
    logger.debug("  primary_group_role_types: %s -> %s", role_types, new_role_types)

    if new_primary_group_id != primary_group_id:
        if len(new_role_types) != len(role_types):
            err_msg = "Cannot move person to different primary group while changing number of role types"
            logger.error("  %s", err_msg)
            raise RuntimeError(err_msg)

    roles_ids = [role["id"] for role in roles]
    updated_ids = _pg.pg_update_role_set_end_on_for_ids(
        cursor, ids=roles_ids, end_on=yesterday
    )
    if write_versions:
        for role_id in updated_ids:
            role = id2role[role_id]
            update_role_changes = {
                "end_on": [role["end_on"], yesterday],
            }
            _pg.pg_insert_version(
                cursor,
                item_type="Role",
                item_id=role_id,
                main_id=row["id"],
                object_dict=role,
                changes=update_role_changes,
                event="update",
                created_at=now,
            )
        for role_id in set(roles_ids) - set(updated_ids):
            role = id2role[role_id]
            destroy_role_changes = {k: [v, None] for k, v in role.items()}
            _pg.pg_insert_version(
                cursor,
                item_type="Role",
                item_id=role_id,
                main_id=row["id"],
                object_dict=role,
                changes=destroy_role_changes,
                event="destroy",
                created_at=now,
            )

    for new_role_type in new_role_types:
        role_id = _pg.pg_insert_role(
            cursor,
            person_id=row["id"],
            group_id=new_primary_group_id,
            type=new_role_type,
            label=None,
            created_at=now,
            start_on=today,
            now=now,
        )
        if write_versions:
            role_changes = {
                "id": [None, role_id],
                "person_id": [None, row["id"]],
                "group_id": [None, new_primary_group_id],
                "type": [None, new_role_type],
                "label": [None, None],
                "created_at": [None, now.isoformat(sep=" ")],
                "start_on": [None, today],
            }
            _pg.pg_insert_version(
                cursor,
                item_type="Role",
                item_id=role_id,
                main_id=row["id"],
                object_dict=row["person_dict"],
                changes=role_changes,
                event="create",
                created_at=now,
            )


def load_person_row(
    conn: _psycopg.Connection,
    person_id: int,
    *,
    collection_date: _datetime.date | str | None = None,
) -> _pandas.Series:
    from . import _people_query

    df = load_people_dataframe(
        conn,
        query=_people_query.PeopleQuery(
            where=_people_query.PeopleWhere(id=person_id),
            collection_date=collection_date,
        ),
        log_resulting_data_frame=False,
    )
    if df.empty:
        err_msg = f"Could not load person with id {person_id}"
        _LOGGER.error(err_msg)
        raise RuntimeError(err_msg)
    row = df.iloc[0]
    return row
