from __future__ import annotations

import datetime as _datetime
import itertools as _itertools
import logging as _logging
import typing as _typing
from collections import abc as _collections_abc

from . import _payment_role


if _typing.TYPE_CHECKING:
    import pathlib as _pathlib

    import pandas as _pandas
    import psycopg as _psycopg
    import psycopg.sql as _psycopg_sql

    from . import _people, _people_query, _sepa_direct_debit


_LOGGER = _logging.getLogger(__name__)


DB_PEOPLE_ALL_STATUS = [
    "registered",
    "printed",
    "upload",
    "in_review",
    "reviewed",
    "confirmed",
    "deregistration_noted",
    "deregistered",
]


DB_PEOPLE_ALL_SEPA_STATUS = [
    "ok",
    "in_review",
    "invalid_account",
    "missing",
    "paused",
]


PAYMENT_DATAFRAME_COLUMNS = [
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
    "sepa_dd_description",
    "sepa_dd_endtoend_id",
    "sepa_dd_payment_initiation_id",
    "sepa_dd_direct_debit_payment_info_id",
    "sepa_dd_pre_notification_id",
    "accounting_entries_count",
    "accounting_entries_amounts_cents",
    "accounting_entry_id",
    "accounting_author_id",
    "accounting_value_date",
    "accounting_booking_at",
    "accounting_description",
    "regular_full_fee_cents",
    "total_fee_cents",
    "total_fee_reduction_comment",
    "total_fee_reduction_cents",
    "installments_cents_dict",
    "installments_cents_sum",
    "custom_installments_comment",
    "custom_installments_issue",
    "pre_notified_amount",
    "amount_paid_cents",
    "amount_unpaid_cents",
    "amount_due_cents",
    "open_amount_cents",
    "payment_status_reason",
    "payment_status",
    "person_dict",
]


def _dd_description_from_row(row) -> str:
    """Compute the direct debit description.

    .. important::

       The character set should be limited to the allowed one::

           Zulässiger SEPA Zeichensatz im DFÜ Abkommen (Deutsche Kreditwirtschaft)

           a - z          a b c d e f g h i j k l m n o p q r s t u v w x y z
           A - Z          A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
           0 - 9          0 1 2 3 4 5 6 7 8 9
           Sonderzeichen  /  ? : ( ) . , ' + -
           Leerzeichen    Space
    """
    from . import _util

    prefix = "WSJ 2027"
    payment_role = row.get("payment_role")
    installments_dict = row.get("installments_cents_dict") or {}
    collection_date: _datetime.date | None = row.get("collection_date")
    if not payment_role or not installments_dict or not collection_date:
        return ""
    prefix = f"{prefix} {payment_role.short_role_name}"
    name_and_id = f"{row['short_full_name']} (id {row['id']})"
    if row["early_payer"] or len(installments_dict) < 2:
        return f"{prefix} Beitrag {name_and_id}"
    else:
        collection_ym = _util.to_year_month(collection_date)
        installment_num = len([ym for ym in installments_dict if ym <= collection_ym])
        # TODO: Check if amount is larger than usual
        return f"{prefix} {name_and_id} / {installment_num}. Rate {_util.to_month_year_de(collection_ym)}"


def _dd_endtoend_id_from_row(row, *, endtoend_ids: dict[int, str] | None = None) -> str:
    import uuid

    from . import _util

    if endtoend_id := (endtoend_ids or {}).get(row["id"]):
        return endtoend_id

    mandate_id = _util.sepa_mandate_id_from_hitobito_id(row["id"])
    count_accounting_entries = (
        _util.nan_to_none(row.get("accounting_entries_count")) or 0
    )
    random_hex = uuid.uuid4().hex[:10]
    endtoend_id = f"{mandate_id}-{count_accounting_entries}-{random_hex}"
    return endtoend_id[:35]


def _accounting_description_from_row(row: _pandas.Series) -> str:
    endtoend_id = row["sepa_dd_endtoend_id"]
    collection_date = row["collection_date"]
    collection_date_de = collection_date.strftime("%d.%m.%Y")
    sepa_name = row.get("sepa_name")
    sepa_iban = row.get("sepa_iban")
    sepa_dd_sequence_type = row.get("sepa_dd_sequence_type")
    return (
        f"SEPA Lastschrifteinzug {endtoend_id} zum {collection_date_de} "
        f"(Kontoinhaber*in: {sepa_name}, IBAN: {sepa_iban}, Sequenz: {sepa_dd_sequence_type})"
    )


def enrich_people_dataframe_for_payments(
    df: _pandas.DataFrame,
    collection_date: _datetime.date | str = "2025-01-01",
    booking_at: _datetime.datetime | None = None,
    pedantic: bool = True,
    endtoend_ids: dict[int, str] | None = None,
    reindex: bool = True,
) -> _pandas.DataFrame:
    import datetime

    from . import _util

    collection_date = _util.to_date_or_none(collection_date)
    if booking_at is None:
        booking_at = datetime.datetime.now()

    if len(df):
        df["sepa_iban"] = df["sepa_iban"].map(lambda s: s.replace(" ", "").upper() if s else None)  # fmt: skip
        df["sepa_bic"] = df["sepa_bic"].map(lambda s: s.replace(" ", "").upper() if s else None)  # fmt: skip
        df["sepa_bic_status"] = None
        df["sepa_bic_status_reason"] = ""

        df["sepa_dd_description"] = df.apply(_dd_description_from_row, axis=1)
        df["sepa_dd_endtoend_id"] = df.apply(
            lambda row: _dd_endtoend_id_from_row(row, endtoend_ids=endtoend_ids), axis=1
        )
        df["payment_status_reason"] = df["open_amount_cents"].map(
            lambda amt: "" if amt > 0 else "amount = 0"
        )
        df["payment_status"] = df["payment_status_reason"].map(
            lambda rsn: "ok" if not rsn else "skipped"
        )
        df["accounting_entry_id"] = None  # accounting_entries.id
        df["accounting_author_id"] = 65  # TODO: maybe (2 - Peter or 65 - Daffi)
        df["accounting_value_date"] = df["collection_date"]  # best guess we can do
        df["accounting_booking_at"] = booking_at
        df["accounting_description"] = df.apply(
            _accounting_description_from_row, axis=1
        )

        _check_iban_bic_in_payment_dataframe(df, pedantic=pedantic)

    if reindex:
        df = df.reindex(columns=PAYMENT_DATAFRAME_COLUMNS)
    return df


def to_int_or_none(obj: object) -> int | None:
    try:
        return int(obj)
    except Exception:
        return None


def _check_iban_bic_in_payment_dataframe(df, pedantic: bool = True):
    import schwifty

    def _check_iban(df, idx, row: _pandas.Series) -> schwifty.IBAN | None:
        # Check for IBAN and if present check that IBAN is valid
        raw_iban = row.get("sepa_iban", None)
        if not raw_iban:
            _skip_payment(df, idx, "No IBAN")
            return None
        else:
            try:
                return schwifty.IBAN(raw_iban, validate_bban=pedantic)
            except Exception as exc:
                _skip_payment(df, idx, f"sepa_iban: {exc}")
                return None

    def _check_bic(df, idx, row: _pandas.Series, iban: schwifty.IBAN | None) -> None:
        # Check for BIC and check that BIC is valid
        bic: schwifty.BIC | None = None
        raw_bic = row.get("sepa_bic", None)
        if raw_bic:
            try:
                bic = schwifty.BIC(raw_bic)
                raw_bic = str(bic)
                df.at[idx, "sepa_bic_status"] = "valid"
            except Exception as exc:
                df.at[idx, "sepa_bic_status"] = "invalid"
                df.at[idx, "sepa_bic_status_reason"] = str(exc)
                if pedantic:
                    _skip_payment(df, idx, f"sepa_bic: {exc}")
                raw_bic = None
        else:
            if iban and (auto_bic := iban.bic):
                df.at[idx, "sepa_bic"] = str(auto_bic)
                df.at[idx, "sepa_bic_status"] = "from_iban"
                df.at[idx, "sepa_bic_status_reason"] = "sepa_bic empty"
            else:
                df.at[idx, "sepa_bic_status"] = "not_present"
            return

        if not (bic and iban and (auto_bic := iban.bic)):
            return

        # here we now have iban, bic and auto_bic
        if not _is_bic_compatible(str(bic), str(auto_bic)):
            reason = (
                f"sepa_bic {bic} not consistent with {auto_bic} derived from sepa_iban"
            )
            df.at[idx, "sepa_bic"] = str(auto_bic)
            df.at[idx, "sepa_bic_status"] = "inconsistent"
            df.at[idx, "sepa_bic_status_reason"] = reason
            if pedantic:
                _skip_payment(df, idx, f"sepa_bic: {reason}")

    for idx, row in df.iterrows():
        iban = _check_iban(df, idx, row)
        _check_bic(df, idx, row, iban=iban)


def _is_bic_compatible(bic_a: str | None, bic_b: str | None) -> bool:
    if (
        (bic_a is None or bic_b is None)
        or (bic_a == bic_b)
        or (bic_a + "XXX" == bic_b)
        or (bic_a == bic_b + "XXX")
    ):
        return True
    else:
        return False


def _skip_payment(df: _pandas.DataFrame, idx, reason: str = "") -> None:
    row = df.loc[idx]
    _LOGGER.warning("Skip payment id=%s reason=%r", row.get("id", "??"), reason)
    df.at[idx, "payment_status"] = "skipped"
    reason_parts = filter(None, [df.at[idx, "payment_status_reason"], reason])
    df.at[idx, "payment_status_reason"] = ", ".join(filter(None, reason_parts))


def load_accounting_balance_in_cent(conn: _psycopg.Connection, id: int | str) -> int:
    from psycopg.sql import SQL, Literal

    with conn:
        cur = conn.cursor()
        query = "SELECT COALESCE(SUM(amount_cents), 0) FROM accounting_entries WHERE subject_id = {id} AND subject_type = 'Person' AND amount_currency = 'EUR'"
        cur.execute(SQL(query).format(id=Literal(int(id))))
        rows = cur.fetchall()
        cur.close()
    return rows[0][0]


def insert_accounting_entry(
    cursor,
    subject_id: int | str,
    author_id: int | str,
    amount: int,
    description: str,
    created_at: _datetime.date | str | None = None,
    payment_initiation_id: int | None = None,
    direct_debit_payment_info_id: int | None = None,
    direct_debit_pre_notification_id: int | None = None,
    end_to_end_identifier: str | None = None,
    mandate_id: str | None = None,
    mandate_date: _datetime.date | str | None = None,
    debit_sequence_type: str | None = None,
    value_date: _datetime.date | str | None = None,
) -> int:
    import datetime

    from . import _pg, _util

    if created_at is None:
        created_at = datetime.datetime.now().date()
    else:
        created_at = _util.to_date_or_none(created_at)

    cols_vals = [
        ("subject_type", "Person"),
        ("subject_id", int(subject_id)),
        ("author_type", "Person"),
        ("author_id", int(author_id)),
        ("amount_currency", "EUR"),
        ("amount_cents", int(amount)),
        ("description", description),
        ("created_at", created_at),
        ("payment_initiation_id", to_int_or_none(payment_initiation_id)),
        ("direct_debit_payment_info_id", to_int_or_none(direct_debit_payment_info_id)),
        (
            "direct_debit_pre_notification_id",
            to_int_or_none(direct_debit_pre_notification_id),
        ),
        ("end_to_end_identifier", end_to_end_identifier),
        ("mandate_id", mandate_id),
        ("mandate_date", _util.to_date_or_none(mandate_date)),
        ("debit_sequence_type", debit_sequence_type),
        ("value_date", _util.to_date_or_none(value_date)),
    ]
    query = _pg.col_val_pairs_to_insert_sql_query("accounting_entries", cols_vals, "id")
    _LOGGER.debug("[ACC] execute %s", query.as_string(context=cursor))
    cursor.execute(query)
    return cursor.fetchone()[0]


def insert_accounting_entry_from_row(cursor, row: _pandas.Series) -> int:
    # Convert booking_at to a Python datetime.datetime object and if
    # it is naive add the local timezone. Otherwise we might end up
    # with a timezone difference when inserting the date into the
    # database.
    booking_at = row["accounting_booking_at"]
    booking_at = booking_at.to_pydatetime()
    if not booking_at.tzinfo:
        booking_at = booking_at.astimezone()
    return insert_accounting_entry(
        cursor,
        subject_id=row["id"],
        author_id=row["accounting_author_id"],
        amount=int(row.get("amount", 0)),
        description=row["accounting_description"],
        created_at=booking_at,
        payment_initiation_id=row.get("sepa_dd_payment_initiation_id"),
        direct_debit_payment_info_id=row.get("sepa_dd_direct_debit_payment_info_id"),
        direct_debit_pre_notification_id=row.get("sepa_dd_pre_notification_id"),
        end_to_end_identifier=row.get("sepa_dd_endtoend_id"),
        debit_sequence_type=row.get("sepa_dd_sequence_type"),
        mandate_id=row.get("sepa_mandate_id"),
        mandate_date=row.get("sepa_mandate_date"),
        value_date=row.get("collection_date"),
    )


def insert_payment_initiation(
    cursor: _psycopg.Cursor,
    *,
    created_at: _datetime.datetime | str = "NOW",
    updated_at: _datetime.datetime | str | None = None,
    status: str = "planned",
    sepa_schema: str = "pain.008.001.02",
    message_identification: str | None = None,
    number_of_transactions: int | None = None,
    control_sum: int | None = None,
    initiating_party_name: str | None = None,
    initiating_party_iban: str | None = None,
    initiating_party_bic: str | None = None,
    sepa_dd_config: _sepa_direct_debit.SepaDirectDebitConfig | None = None,
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
        ("control_sum", control_sum),
        ("initiating_party_name", initiating_party_name),
        ("initiating_party_iban", initiating_party_iban),
        ("initiating_party_bic", initiating_party_bic),
    ]
    query = _pg.col_val_pairs_to_insert_sql_query(
        "wsjrdp_payment_initiations", cols_vals, "id"
    )
    _LOGGER.debug("execute %s", query.as_string(context=cursor))
    cursor.execute(query)
    return cursor.fetchone()[0]  # type: ignore


def insert_direct_debit_payment_info(
    cursor: _psycopg.Cursor,
    *,
    created_at: _datetime.datetime | str | None = "NOW",
    updated_at: _datetime.datetime | str | None = None,
    payment_initiation_id: int | None = None,
    payment_information_identification: str | None = None,
    batch_booking: bool = True,
    number_of_transactions: int | None = None,
    control_sum: int | None = None,
    payment_type_instrument: str = "CORE",
    sequence_type: str = "OOFF",
    requested_collection_date: _datetime.date | str = "TODAY",
    cdtr_name: str | None = None,
    cdtr_iban: str | None = None,
    cdtr_bic: str | None = None,
    creditor_id: str | None = None,
    sepa_dd_config: _sepa_direct_debit.SepaDirectDebitConfig | None = None,
) -> int:
    import wsjrdp2027

    from . import _pg, _util

    creditor_id = creditor_id or wsjrdp2027.CREDITOR_ID

    if sepa_dd_config:
        if not cdtr_name:
            cdtr_name = sepa_dd_config.get("name")
        if not cdtr_iban:
            cdtr_iban = sepa_dd_config.get("IBAN")
        if not cdtr_bic:
            cdtr_bic = sepa_dd_config.get("BIC")

    cols_vals = [
        ("created_at", _util.to_datetime_or_none(created_at)),
        ("updated_at", _util.to_datetime_or_none(updated_at)),
        ("payment_initiation_id", to_int_or_none(payment_initiation_id)),
        ("payment_information_identification", payment_information_identification),
        ("batch_booking", batch_booking),
        ("number_of_transactions", number_of_transactions),
        ("control_sum", control_sum),
        ("payment_type_instrument", payment_type_instrument),
        ("sequence_type", sequence_type),
        ("requested_collection_date", _util.to_date_or_none(requested_collection_date)),
        ("cdtr_name", cdtr_name),
        ("cdtr_iban", cdtr_iban),
        ("cdtr_bic", cdtr_bic),
        ("creditor_id", creditor_id),
    ]
    query = _pg.col_val_pairs_to_insert_sql_query(
        "wsjrdp_direct_debit_payment_infos", cols_vals, "id"
    )
    _LOGGER.debug("execute %s", query.as_string(context=cursor))
    cursor.execute(query)
    return cursor.fetchone()[0]  # type: ignore


def insert_direct_debit_pre_notification(
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
    payment_status: str = "pre_notified",
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
    sequence_type: str = "OOFF",
    collection_date: _datetime.date | str | None = None,
    mandate_id: str | None = None,
    mandate_date: _datetime.date | str | None = None,
    description: str | None = None,
    endtoend_id: str | None = None,
    payment_role: str | _payment_role.PaymentRole | None = None,
    early_payer: bool | None = None,
    creditor_id: str | None = None,
) -> int:
    import wsjrdp2027

    from . import _pg, _util

    creditor_id = creditor_id or wsjrdp2027.CREDITOR_ID

    if isinstance(payment_role, _payment_role.PaymentRole):
        payment_role = payment_role.get_db_payment_role(early_payer=early_payer)

    cols_vals = [
        ("created_at", _util.to_datetime_or_none(created_at)),
        ("updated_at", _util.to_datetime_or_none(updated_at)),
        ("payment_initiation_id", to_int_or_none(payment_initiation_id)),
        ("direct_debit_payment_info_id", to_int_or_none(direct_debit_payment_info_id)),
        ("subject_id", subject_id),
        ("subject_type", subject_type),
        ("author_id", author_id),
        ("author_type", author_type),
        ("try_skip", try_skip),
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
        ("sequence_type", sequence_type),
        ("collection_date", _util.to_date_or_none(collection_date)),
        ("mandate_id", mandate_id),
        ("mandate_date", _util.to_date_or_none(mandate_date)),
        ("description", description),
        ("endtoend_id", endtoend_id),
        ("payment_role", payment_role),
        ("creditor_id", creditor_id),
    ]
    query = _pg.col_val_pairs_to_insert_sql_query(
        "wsjrdp_direct_debit_pre_notifications", cols_vals, "id"
    )
    _LOGGER.debug("execute %s", query.as_string(context=cursor))
    cursor.execute(query)
    return cursor.fetchone()[0]  # type: ignore


def insert_direct_debit_pre_notification_from_row(
    cursor: _psycopg.Cursor,
    row: _pandas.Series,
    *,
    created_at: _datetime.datetime | str | None = "NOW",
    updated_at: _datetime.datetime | str | None = None,
    payment_initiation_id: int | None = None,
    direct_debit_payment_info_id: int | None = None,
    creditor_id: str | None = None,
) -> int:
    from . import _util

    return insert_direct_debit_pre_notification(
        cursor,
        created_at=created_at,
        updated_at=updated_at,
        payment_initiation_id=payment_initiation_id,
        direct_debit_payment_info_id=direct_debit_payment_info_id,
        subject_id=row["id"],
        subject_type="Person",
        author_id=row["accounting_author_id"],
        author_type="Person",
        email_from=row["sepa_mailing_from"],
        email_to=row["sepa_mailing_to"],
        email_cc=row["sepa_mailing_cc"],
        email_bcc=row["sepa_mailing_bcc"],
        email_reply_to=row["sepa_mailing_reply_to"],
        dbtr_name=row["sepa_name"],
        dbtr_iban=row["sepa_iban"],
        dbtr_bic=row["sepa_bic"],
        dbtr_address=row["sepa_address"],
        amount_currency="EUR",
        amount_cents=row["open_amount_cents"],
        sequence_type=row["sepa_dd_sequence_type"],
        collection_date=row["collection_date"],
        mandate_id=row["sepa_mandate_id"],
        mandate_date=_util.to_date_or_none(row["sepa_mandate_date"]),
        description=row["sepa_dd_description"],
        endtoend_id=row["sepa_dd_endtoend_id"],
        payment_role=row["payment_role"],
        early_payer=row["early_payer"],
        creditor_id=(creditor_id or None),
    )


def write_payment_dataframe_to_db(
    conn: _psycopg.Connection, df: _pandas.DataFrame
) -> None:
    with conn.cursor() as cursor:
        for idx, row in df.iterrows():
            if row["payment_status"] != "ok":
                _LOGGER.debug(
                    "[ACC] Skip non-ok row id=%s payment_status=%s payment_status_reason=%s",
                    row.get("id", "??"),
                    row.get("payment_status", "??"),
                    row.get("payment_status_reason", "??"),
                )
                continue
            accounting_entry_id = insert_accounting_entry_from_row(cursor, row)
            _LOGGER.info(
                "[ACC] subject_id=%s sepa_name=%r %r %s print_at=%s open_amount_cents=%s -> id=%s",
                row.get("id"),
                row.get("sepa_name"),
                row.get("short_full_name"),
                row.get("payment_role"),
                row.get("print_at"),
                int(row.get("open_amount_cents", 0)),
                accounting_entry_id,
            )
            df.at[idx, "accounting_entry_id"] = accounting_entry_id
        _LOGGER.info("COMMIT")
    conn.commit()


def write_payment_dataframe_to_html(
    df: _pandas.DataFrame, path: str | _pathlib.Path
) -> None:
    html_str = df.to_html()
    with open(path, "w", encoding="utf-8") as f:
        _LOGGER.info("Write %s", path)
        f.write(html_str)


def write_payment_dataframe_to_xlsx(
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


def load_payment_dataframe_from_payment_initiation(
    conn: _psycopg.Connection,
    *,
    payment_initiation_id: int,
    pedantic: bool = True,
    where: str = "",
    report_amount_differences: bool = True,
) -> _pandas.DataFrame:
    import re
    import textwrap

    import pandas as pd
    import psycopg.rows
    from psycopg.sql import SQL, Identifier, Literal

    from . import _util

    raw_query = """
SELECT
  wsjrdp_direct_debit_pre_notifications.id,
  direct_debit_payment_info_id,
  subject_id,
  author_id,
  author_type,
  COALESCE(try_skip, FALSE) AS try_skip,
  payment_status,
  email_from,
  email_to,
  email_cc,
  email_bcc,
  email_reply_to,
  dbtr_name,
  dbtr_iban,
  dbtr_bic,
  dbtr_address,
  amount_currency,
  amount_cents,
  sequence_type,
  collection_date,
  mandate_id,
  mandate_date,
  description,
  endtoend_id,
  people.first_name,
  people.last_name
FROM wsjrdp_direct_debit_pre_notifications
LEFT OUTER JOIN people
  ON people.id = wsjrdp_direct_debit_pre_notifications.subject_id
     AND wsjrdp_direct_debit_pre_notifications.subject_type = 'Person'
WHERE
  wsjrdp_direct_debit_pre_notifications.payment_initiation_id = {payment_initiation_id}
  AND wsjrdp_direct_debit_pre_notifications.subject_type = 'Person'
"""

    query = SQL(raw_query).format(
        payment_initiation_id=Literal(payment_initiation_id),
    )

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        sql_stmt = re.sub(
            r"\n+", "\n", textwrap.dedent(query.as_string(context=cur)).strip()
        )

        _LOGGER.info(
            "Fetch pre notifications SQL Query:\n%s", textwrap.indent(sql_stmt, "  ")
        )
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()

    raw_pn_df = pd.DataFrame(rows)
    raw_pn_df["full_name"] = raw_pn_df["first_name"] + " " + raw_pn_df["last_name"]

    id_to_pn_row = {row["subject_id"]: row for _, row in raw_pn_df.iterrows()}

    _LOGGER.info("all pre notifications:\n%s", textwrap.indent(str(raw_pn_df), "  | "))
    all_pn_ids = set(raw_pn_df["id"])
    _LOGGER.info(
        "all pre notification id's (%s):\n  | all_pn_ids = %s",
        len(all_pn_ids),
        sorted(all_pn_ids),
    )

    pn_df = raw_pn_df[raw_pn_df["try_skip"] == False]
    keep_pn_ids = set(pn_df["id"])

    _LOGGER.info(
        "not skipped pre notification id's (%s):\n  | keep_pn_ids = %s",
        len(keep_pn_ids),
        sorted(keep_pn_ids),
    )
    skipped_pn_df = raw_pn_df[~raw_pn_df["id"].isin(keep_pn_ids)]

    if len(skipped_pn_df):
        for _, row in skipped_pn_df.iterrows():
            _LOGGER.info(
                """skipped pre notification (do not collect) for %s %s (pre notification id: %s):
  try_skip: %s
  payment_status: %s
  row:
%s""",
                row["subject_id"],
                row["full_name"],
                row["id"],
                row["try_skip"],
                row["payment_status"],
                textwrap.indent(row.to_string(), "    | "),
            )

        _LOGGER.info(
            "skipped pre notifications:\n%s",
            textwrap.indent(str(skipped_pn_df), "  | "),
        )
    else:
        _LOGGER.info("No pre notifications have been skipped")

    ids = list(pn_df["subject_id"])
    collection_dates = set(pn_df["collection_date"])
    assert len(collection_dates) == 1
    collection_date = list(collection_dates)[0]
    endtoend_ids = {k: v["endtoend_id"] for k, v in id_to_pn_row.items()}

    where = _util.combine_where(where, _util.in_expr("people.id", ids))

    df = load_payment_dataframe(
        conn,
        where=where,
        pedantic=pedantic,
        collection_date=collection_date,
        endtoend_ids=endtoend_ids,
    )
    df["sepa_dd_payment_initiation_id"] = payment_initiation_id
    df["sepa_dd_direct_debit_payment_info_id"] = df["id"].map(
        lambda person_id: id_to_pn_row[person_id]["direct_debit_payment_info_id"]
    )
    df["sepa_dd_pre_notification_id"] = df["id"].map(
        lambda person_id: id_to_pn_row[person_id]["id"]
    )
    df["pre_notified_amount"] = df["id"].map(
        lambda person_id: id_to_pn_row[person_id]["amount_cents"]
    )

    amount_changed_df = df[df["open_amount_cents"] != df["pre_notified_amount"]]
    if report_amount_differences:
        if len(amount_changed_df):
            for _, row in amount_changed_df.iterrows():
                _LOGGER.info(
                    "amount different between pre notification and current computation for %s %s:\n%s",
                    row["id"],
                    row["full_name"],
                    textwrap.indent(row.to_string(), "  | "),
                )
        else:
            _LOGGER.info(
                "All due amounts are the same between pre notification and current computation"
            )

    return df


def load_payment_dataframe(
    conn: _psycopg.Connection,
    *,
    collection_date: _datetime.date | str = "2025-01-01",
    booking_at: _datetime.datetime | None = None,
    pedantic: bool = True,
    where: str | _people_query.PeopleWhere | None = "",
    early_payer: bool | None = None,
    max_print_at: str | _datetime.date | None = None,
    status: str | _collections_abc.Iterable[str] | None = ("reviewed", "confirmed"),
    fee_rules: str | _collections_abc.Iterable[str] = "active",
    sepa_status: str | _collections_abc.Iterable[str] = "ok",
    now: _datetime.datetime | _datetime.date | str | int | float | None = None,
    endtoend_ids: dict[int, str] | None = None,
) -> _pandas.DataFrame:
    import textwrap

    from . import _people, _people_query, _util

    now = _util.to_datetime(now)
    today = now.date()

    if where is None:
        where = ""
    elif isinstance(where, _people_query.PeopleWhere):
        where = where.as_where_condition(people_table="people")

    if max_print_at is not None:
        where = _util.combine_where(
            where,
            f"people.print_at <= '{_util.to_date_or_none(max_print_at).isoformat()}'",
        )

    df = _people.load_people_dataframe(
        conn,
        where=where,
        status=status,
        early_payer=early_payer,
        sepa_status=sepa_status,
        fee_rules=fee_rules,
        log_resulting_data_frame=False,
        now=now,
        collection_date=collection_date,
    )

    collection_date = _util.to_date_or_none(collection_date)
    today = collection_date if today is None else min(today, collection_date)
    df = enrich_people_dataframe_for_payments(
        df,
        collection_date=collection_date,
        booking_at=booking_at,
        pedantic=pedantic,
        endtoend_ids=endtoend_ids,
    )
    _LOGGER.info("Resulting pandas DataFrame:\n%s", textwrap.indent(str(df), "  "))
    return df
