from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import logging as _logging
import typing as _typing
from collections import abc as _collections_abc

import wsjrdp2027

from . import _util


if _typing.TYPE_CHECKING:
    import pathlib as _pathlib

    import pandas as _pandas
    import psycopg as _psycopg

    from . import _people_query, _types


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
    "sepa_bank_name",
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
    "pre_notified_amount_cents",
    "amount_paid_cents",
    "amount_unpaid_cents",
    "amount_due_cents",
    "open_amount_cents",
    "payment_status_reason",
    "payment_status",
    "person_dict",
]

PRE_NOTIFICATION_COLUMNS = [
    "id",
    "payment_initiation_id",
    "direct_debit_payment_info_id",
    "subject_id",
    "subject_type",
    "author_id",
    "author_type",
    "try_skip",
    "payment_status",
    "email_from",
    "email_to",
    "email_cc",
    "email_bcc",
    "email_reply_to",
    "dbtr_name",
    "dbtr_iban",
    "dbtr_bic",
    "dbtr_address",
    "amount_currency",
    "amount_cents",
    "pre_notified_amount_cents",
    "debit_sequence_type",
    "collection_date",
    "mandate_id",
    "mandate_date",
    "description",
    "comment",
    "endtoend_id",
    "creditor_id",
    "cdtr_name",
    "cdtr_iban",
    "cdtr_bic",
    "cdtr_address",
]

_SAVE_AS_CALCULATED_COLUMNS = [
    "open_amount_cents",
    "sepa_name",
    "sepa_iban",
    "sepa_bic",
    "sepa_address",
    "sepa_dd_sequence_type",
    "sepa_mandate_id",
    "sepa_mandate_date",
    "sepa_dd_description",
    "sepa_dd_endtoend_id",
    "collection_date",
]


def _dd_description_from_row(row) -> str:
    """Compute the direct debit description.

    .. important::

       The character set should be limited to the allowed one::

           Zulässiger SEPA Zeichensatz im DFÜ Abkommen (Deutsche Kreditwirtschaft)

           a - z          a b c d e f g h i j k l m n o p q r s t u v w x y z
           A - Z          A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
           0 - 9          0 1 2 3 4 5 6 7 8 9
           Sonderzeichen  / ? : ( ) . , ' + -
           Leerzeichen    Space
    """
    from . import _util

    payment_role = row.get("payment_role")
    installments_dict = row.get("installments_cents_dict") or {}
    collection_date: _datetime.date | None = row.get("collection_date")
    if not payment_role or not installments_dict or not collection_date:
        return ""

    role_short_name = payment_role.short_role_name if payment_role else None
    subject_ident = " ".join(
        str(x) for x in [role_short_name, row["id"], row["short_full_name"]] if x
    )

    purpose = "WSJ 2027"

    if row["early_payer"] or len(installments_dict) < 2:
        purpose += " Beitrag"
    else:
        collection_ym = _util.to_year_month(collection_date)
        installment_num = len([ym for ym in installments_dict if ym <= collection_ym])
        purpose += f" {installment_num}. Rate {_util.to_month_year_de(collection_ym)}"

    def cents_to_eur(cents) -> str:
        return _util.format_cents_as_eur_de(
            cents,
            zero_cents="",
            format="#,##0.00 ¤¤",
            currency="EUR",
        )

    open_amount_cents = row["open_amount_cents"]
    this_month_cents = row["amount_due_in_collection_date_month_cents"]
    if open_amount_cents != this_month_cents:
        this_month_eur = cents_to_eur(this_month_cents)
        purpose += f" ({this_month_eur})"
        difference_eur = cents_to_eur(abs(open_amount_cents - this_month_cents))
        if open_amount_cents < this_month_cents:
            purpose += f", davon bereits {difference_eur} bezahlt"
        elif open_amount_cents > this_month_cents:
            purpose += f" + Zahlungsrückstand ({difference_eur})"

    return f"{subject_ident} / {purpose}"


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
    booking_at: _datetime.datetime | _datetime.date | str | int | None = None,
    pedantic: bool = False,
    endtoend_ids: dict[int, str] | None = None,
    reindex: bool = True,
    now: _datetime.datetime | _datetime.date | str | int | float | None = None,
) -> _pandas.DataFrame:
    from . import _util

    now = _util.to_datetime(now)

    collection_date = _util.to_date_or_none(collection_date)
    booking_at = _util.to_datetime(booking_at, now=now)

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
        df["accounting_author_id"] = 1  # TODO: maybe (2 - Peter or 65 - Daffi)
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
        return int(obj)  # type: ignore
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


def _skip_payment(
    df: _pandas.DataFrame, idx, reason: str = "", *, log_level=_logging.WARNING
) -> None:
    row = df.loc[idx]
    _LOGGER.log(log_level, "Skip payment id=%s reason=%r", row.get("id", "??"), reason)
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


def insert_accounting_entry_from_row(cursor, row: _pandas.Series) -> int:
    # Convert booking_at to a Python datetime.datetime object and if
    # it is naive add the local timezone. Otherwise we might end up
    # with a timezone difference when inserting the date into the
    # database.
    from . import _pg

    booking_at = row["accounting_booking_at"]
    booking_at = booking_at.to_pydatetime()
    if not booking_at.tzinfo:
        booking_at = booking_at.astimezone()
    booking_date = booking_at.date()

    pn_id = row.get("pn_id")
    if pn_id is not None:
        return _pg.pg_insert_accounting_entry(
            cursor,
            subject_id=row["id"],
            author_id=row["pn_author_id"],
            author_type=row["pn_author_type"],
            amount_cents=int(row.get("pn_amount_cents", 0)),
            description=row["pn_description"],
            comment=row["pn_comment"],
            created_at=booking_at,
            payment_initiation_id=row.get("pn_payment_initiation_id"),
            direct_debit_payment_info_id=row.get("pn_direct_debit_payment_info_id"),
            direct_debit_pre_notification_id=pn_id,
            endtoend_id=row.get("pn_endtoend_id"),
            debit_sequence_type=row.get("pn_debit_sequence_type"),
            mandate_id=row.get("pn_mandate_id"),
            mandate_date=row.get("pn_mandate_date"),
            value_date=row.get("pn_collection_date"),
            booking_date=booking_date,
            dbtr_name=row.get("pn_dbtr_name"),
            dbtr_iban=row.get("pn_dbtr_iban"),
            dbtr_bic=row.get("pn_dbtr_bic"),
            dbtr_address=row.get("pn_dbtr_address"),
            cdtr_name=row.get("pn_cdtr_name"),
            cdtr_iban=row.get("pn_cdtr_iban"),
            cdtr_bic=row.get("pn_cdtr_bic"),
            cdtr_address=row.get("pn_cdtr_address"),
        )
    else:
        return _pg.pg_insert_accounting_entry(
            cursor,
            subject_id=row["id"],
            author_id=row["accounting_author_id"],
            amount_cents=int(row.get("open_amount_cents", 0)),
            description=row["accounting_description"],
            created_at=booking_at,
            payment_initiation_id=row.get("sepa_dd_payment_initiation_id"),
            direct_debit_payment_info_id=row.get(
                "sepa_dd_direct_debit_payment_info_id"
            ),
            direct_debit_pre_notification_id=row.get("sepa_dd_pre_notification_id"),
            endtoend_id=row.get("sepa_dd_endtoend_id"),
            debit_sequence_type=row.get("sepa_dd_sequence_type"),
            mandate_id=row.get("sepa_mandate_id"),
            mandate_date=row.get("sepa_mandate_date"),
            value_date=row.get("collection_date"),
            booking_date=booking_date,
        )


def insert_direct_debit_pre_notification_from_row(
    cursor: _psycopg.Cursor,
    row: _pandas.Series,
    *,
    created_at: _datetime.datetime | str | None = "NOW",
    updated_at: _datetime.datetime | str | None = None,
    payment_initiation_id: int | None = None,
    direct_debit_payment_info_id: int | None = None,
    creditor_id: str | None = None,
    payment_status: str | None = None,
    sepa_dd_config: _types.SepaDirectDebitConfig | None = None,
) -> int:
    from . import _pg, _util

    return _pg.pg_insert_direct_debit_pre_notification(
        cursor,
        created_at=created_at,
        updated_at=updated_at,
        payment_initiation_id=payment_initiation_id,
        direct_debit_payment_info_id=direct_debit_payment_info_id,
        subject_id=row["id"],
        subject_type="Person",
        author_id=row["accounting_author_id"],
        author_type="Person",
        payment_status=payment_status,
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
        debit_sequence_type=row["sepa_dd_sequence_type"],
        collection_date=row["collection_date"],
        mandate_id=row["sepa_mandate_id"],
        mandate_date=_util.to_date_or_none(row["sepa_mandate_date"]),
        description=row["sepa_dd_description"],
        endtoend_id=row["sepa_dd_endtoend_id"],
        payment_role=row["payment_role"],
        early_payer=row["early_payer"],
        creditor_id=(creditor_id or None),
        sepa_dd_config=sepa_dd_config,
    )


def write_payment_dataframe_to_db(
    conn: _psycopg.Connection, df: _pandas.DataFrame, *, print_progress_message=None
) -> None:
    from . import _pg, _util

    if print_progress_message is None:
        print_progress_message = _util.print_progress_message

    df_len = len(df)
    count = 0
    try_skip_ids = []
    _LOGGER.info("Write (up to) %s accounting entries to DB", df_len)
    with conn.cursor() as cursor:
        idx: int
        for i, (idx, row) in enumerate(df.iterrows()):  # type: ignore
            pn_id = row.get("pn_id")
            pn_payment_status: str | None = row.get("pn_payment_status")
            if row["payment_status"] != "ok":
                _LOGGER.debug(
                    "[ACC] Skip non-ok row id=%s payment_status=%s payment_status_reason=%s",
                    row.get("id", "??"),
                    row.get("payment_status", "??"),
                    row.get("payment_status_reason", "??"),
                )
                if pn_id is not None and pn_payment_status == "pre_notified":
                    _pg.pg_update_direct_debit_pre_notification(
                        cursor, id=pn_id, updates={"payment_status": "skipped"}
                    )
                    _LOGGER.debug(
                        "[ACC]     SET pn.payment_status = 'skipped' WHERE pn.id=%s (pn.payment_status was %s)",
                        pn_id,
                        pn_payment_status,
                    )
                continue
            if pn_id is not None:
                if pn_payment_status != "pre_notified":
                    _LOGGER.debug(
                        "[ACC] Skip pre-notification due to payment_status: people.id=%s pn.id=%s pn.payment_status=%s",
                        row.get("id"),
                        pn_id,
                        pn_payment_status,
                    )
                    continue
                elif row.get("pn_try_skip"):
                    try_skip_ids.append(pn_id)
                    _pg.pg_update_direct_debit_pre_notification(
                        cursor, id=pn_id, updates={"payment_status": "skipped"}
                    )
                    _LOGGER.debug(
                        "[ACC] Skip pre-notification due to try_skip=True: people.id=%s pn.id=%s pn.payment_status=%s",
                        row.get("id"),
                        pn_id,
                        pn_payment_status,
                    )
                    continue
                else:
                    _pg.pg_update_direct_debit_pre_notification(
                        cursor, id=pn_id, updates={"payment_status": "xml_generated"}
                    )

            count += 1
            accounting_entry_id = insert_accounting_entry_from_row(cursor, row)
            progress_msg = (
                f"[ACC] subject_id={row.get('id')}"
                f" sepa_name={row.get('sepa_name')!r} {row.get('short_full_name')!r}"
                f" {row.get('payment_role')}"
                f" {int(row.get('open_amount_cents', 0))}"
                f" -> id={accounting_entry_id}"
            )
            print_progress_message(i, df_len, progress_msg, logger=_LOGGER)
            df.at[idx, "accounting_entry_id"] = accounting_entry_id
    _LOGGER.info("... Finished inserting %s accounting entries", count)
    if try_skip_ids:
        _LOGGER.info(
            "    - Skipped %s pre notifications due to try_skip: pn_ids=%s",
            len(try_skip_ids),
            try_skip_ids,
        )


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


@_dataclasses.dataclass(kw_only=True)
class _PreNotificationInfo:
    ids: list[int] = _dataclasses.field(default_factory=lambda: [])
    collection_date: _datetime.date = _datetime.date.min
    endtoend_ids: dict = _dataclasses.field(default_factory=lambda: {})
    id_to_pn_row: dict[int, _pandas.Series] = _dataclasses.field(
        default_factory=lambda: {}
    )

    @classmethod
    def from_pn_df(cls, raw_pn_df) -> _typing.Self:
        if len(raw_pn_df) == 0:
            return cls.__from_empty()
        else:
            return cls.__from_raw_pn_df(raw_pn_df)

    @classmethod
    def __from_empty(cls) -> _typing.Self:
        return cls()

    @classmethod
    def __from_raw_pn_df(cls, raw_pn_df: _pandas.DataFrame) -> _typing.Self:
        import textwrap

        raw_pn_df["full_name"] = raw_pn_df["first_name"] + " " + raw_pn_df["last_name"]
        id_to_pn_row = {row["subject_id"]: row for _, row in raw_pn_df.iterrows()}
        all_pn_ids = set(raw_pn_df["id"])

        _LOGGER.info(
            "all pre notifications:\n%s", textwrap.indent(str(raw_pn_df), "  | ")
        )
        _LOGGER.debug(
            "all pre notification id's (%s):\n  | all_pn_ids = %s",
            len(all_pn_ids),
            sorted(all_pn_ids),
        )

        pn_df = raw_pn_df

        ids = list(pn_df["subject_id"])
        collection_dates = set(pn_df["collection_date"])
        assert len(collection_dates) == 1, (
            f"Can handle only one collection_date, found {sorted(collection_dates)}"
        )
        collection_date = list(collection_dates)[0]
        endtoend_ids = {k: v["endtoend_id"] for k, v in id_to_pn_row.items()}

        return cls(
            ids=ids,
            collection_date=collection_date,
            endtoend_ids=endtoend_ids,
            id_to_pn_row=id_to_pn_row,
        )


def load_payment_dataframe_from_payment_initiation(
    conn: _psycopg.Connection,
    *,
    payment_initiation_id: int,
    pedantic: bool = False,
    where: str = "",
    report_amount_differences: bool = True,
    booking_at: _datetime.datetime | _datetime.date | str | int | None = None,
    now: _datetime.datetime | _datetime.date | str | int | float | None = None,
) -> _pandas.DataFrame:
    import re
    import textwrap

    import pandas as pd
    import psycopg.rows
    from psycopg.sql import SQL, Literal

    from . import _people_query, _util

    raw_query = """
SELECT
  wsjrdp_direct_debit_pre_notifications.id,
  wsjrdp_direct_debit_pre_notifications.payment_initiation_id,
  wsjrdp_direct_debit_pre_notifications.direct_debit_payment_info_id,
  subject_id,
  subject_type,
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
  pre_notified_amount_cents,
  debit_sequence_type,
  collection_date,
  mandate_id,
  mandate_date,
  description,
  wsjrdp_direct_debit_pre_notifications.comment,
  endtoend_id,
  creditor_id,
  cdtr_name,
  cdtr_iban,
  cdtr_bic,
  cdtr_address,
  wsjrdp_direct_debit_pre_notifications.additional_info,
  people.first_name,
  people.last_name,
  COALESCE(people.sepa_status, 'ok') as new_sepa_status
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

        _LOGGER.info("fetch pre notifications...")
        _LOGGER.debug(
            "SQL Query to fetch pre notifications:\n%s", textwrap.indent(sql_stmt, "  ")
        )
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()

    raw_pn_df = pd.DataFrame(rows)

    pninf = _PreNotificationInfo.from_pn_df(raw_pn_df=raw_pn_df)

    where = _util.combine_where(where, _util.in_expr("people.id", pninf.ids))

    df = load_payment_dataframe(
        conn,
        query=_people_query.PeopleQuery(
            where=where, collection_date=pninf.collection_date, now=now
        ),
        pedantic=pedantic,
        endtoend_ids=pninf.endtoend_ids,
        booking_at=booking_at,
    )

    if len(df):
        df = df.copy()  # just for de-fragmentation
        for key in _SAVE_AS_CALCULATED_COLUMNS:
            df[f"calculated_{key}"] = df[key]
        df["sepa_dd_payment_initiation_id"] = payment_initiation_id
        df["sepa_dd_direct_debit_payment_info_id"] = df["id"].map(
            lambda person_id: pninf.id_to_pn_row[person_id][
                "direct_debit_payment_info_id"
            ]
        )
        df["sepa_dd_pre_notification_id"] = df["id"].map(
            lambda person_id: pninf.id_to_pn_row[person_id]["id"]
        )
        df["pre_notified_amount_cents"] = df["id"].map(
            lambda person_id: pninf.id_to_pn_row[person_id]["pre_notified_amount_cents"]
        )
        df = df.copy()  # just for de-fragmentation
        for key in PRE_NOTIFICATION_COLUMNS:
            df[f"pn_{key}"] = df["id"].map(
                lambda person_id: pninf.id_to_pn_row[person_id][key]
            )
        df = df.copy()  # just for de-fragmentation
        df["open_amount_cents"] = df["pn_amount_cents"]
        df["sepa_name"] = df["pn_dbtr_name"]
        df["sepa_iban"] = df["pn_dbtr_iban"]
        df["sepa_bic"] = df["pn_dbtr_bic"]
        df["sepa_address"] = df["pn_dbtr_address"]
        df["sepa_dd_sequence_type"] = df["pn_debit_sequence_type"]
        df["sepa_mandate_id"] = df["pn_mandate_id"]
        df["sepa_mandate_date"] = df["pn_mandate_date"]
        df["sepa_dd_description"] = df["pn_description"]
        df["sepa_dd_endtoend_id"] = df["pn_endtoend_id"]
        df["collection_date"] = df["pn_collection_date"]

        if report_amount_differences:
            report_direct_debit_amount_differences(df)

        # Skip payments in df if
        # (a) the pn.payment_status is not pre_notified (e.g., already skipped or xml_generated)
        # (b) the pn.try_skip flag is set, i.e., we are asked to skip the payment
        for idx, row in df.iterrows():
            pn_id = row.get("pn_id")
            pn_payment_status: str | None = row.get("pn_payment_status")
            if pn_payment_status != 'pre_notified':
                _LOGGER.info(
                    "Notice: Skip payment due to pn.payment_status=%s: people.id=%s pn.id=%s",
                    pn_payment_status,
                    row.get("id"),
                    pn_id,
                )
                _LOGGER.info("    %s %s %s", row['payment_role'].short_role_name, row['id'], row['short_full_name'])
                _LOGGER.info("    pn_comment: %s", row['pn_comment'])
                _skip_payment(df, idx, f"pn.payment_status={pn_payment_status}", log_level=_logging.DEBUG)
            elif row.get("pn_try_skip"):
                _LOGGER.info(
                    "Notice: Skip payment due to pn.try_skip=True: people.id=%s pn.id=%s pn.payment_status=%s",
                    row.get("id"),
                    pn_id,
                    pn_payment_status,
                )
                _LOGGER.info("    %s %s %s", row['payment_role'].short_role_name, row['id'], row['short_full_name'])
                _LOGGER.info("    pn_comment: %s", row['pn_comment'])
                _skip_payment(df, idx, "pn.try_skip=True", log_level=_logging.DEBUG)
    else:
        new_cols = [
            "sepa_dd_payment_initiation_id",
            "sepa_dd_direct_debit_payment_info_id",
            "sepa_dd_pre_notification_id",
            "pre_notified_amount_cents",
            *(f"pn_{k}" for k in PRE_NOTIFICATION_COLUMNS),
            *(f"calculated_{k}" for k in _SAVE_AS_CALCULATED_COLUMNS),
        ]
        columns = list(df.columns) + [
            col for col in new_cols if col not in set(df.columns)
        ]
        df = df.reindex(columns=columns)

    return df


def report_direct_debit_amount_differences(
    df: _pandas.DataFrame,
    *,
    logger: _logging.Logger | _logging.LoggerAdapter | None = None,
) -> None:
    import textwrap

    from ._util import format_cents_as_eur_de

    def to_eur(cents):
        return format_cents_as_eur_de(cents, zero_cents=",00")

    if logger is None:
        logger = _LOGGER

    amount_changed_df = df[
        (df["calculated_open_amount_cents"] != df["pn_amount_cents"])
        | (df["pn_pre_notified_amount_cents"] != df["pn_amount_cents"])
        | (df["pn_debit_sequence_type"] != df["calculated_sepa_dd_sequence_type"])
    ]
    print(flush=True)
    if amount_changed_df.empty:
        _LOGGER.info(
            "All due amounts are the same between pre notification and current computation"
        )
        return

    _LOGGER.info("==== Found %s payments with differences between amounts:", len(amount_changed_df))
    for _, row in amount_changed_df.iterrows():
        diff_msg = (
            f"amount difference between pre-notification, amount in pre-notification and current computation:\n"
            f"  {row['payment_role'].short_role_name} {row['id']} {row['short_full_name']}\n"
            f"  pn_amount_cents:              {to_eur(row['pn_amount_cents'])} (used amount)\n"
            f"  pn_pre_notified_amount_cents: {to_eur(row['pn_pre_notified_amount_cents'])} (originally notified)\n"
            f"  calculated_open_amount_cents: {to_eur(row['calculated_open_amount_cents'])} (newly calculated)\n"
            f"  pn_debit_sequence_type: {row['pn_debit_sequence_type']} (used sequence type)\n"
            f"  calculated_sepa_dd_sequence_type: {row['sepa_dd_sequence_type']} (newly calculated)\n"
            f"  pn_comment: {row['pn_comment']}\n"
        )
        _LOGGER.info(diff_msg)
        _LOGGER.debug("row:\n%s", textwrap.indent(row.to_string(), "  | "))
    print(flush=True)


def load_payment_dataframe(
    conn: _psycopg.Connection,
    *,
    booking_at: _datetime.datetime | _datetime.date | str | int | None = None,
    pedantic: bool = False,
    query: _people_query.PeopleQuery | None = None,
    where: str | _people_query.PeopleWhere | None = "",
    fee_rules: str | _collections_abc.Iterable[str] = "active",
    endtoend_ids: dict[int, str] | None = None,
    now: _datetime.datetime | _datetime.date | str | int | float | None = None,
) -> _pandas.DataFrame:
    import textwrap

    from . import _people, _people_query

    if query:
        if where:
            raise ValueError("Only one of 'query' and 'where' is allowed")
        if now is None:
            now = query.now
    else:
        query = _people_query.PeopleQuery(where=where)
    if query.collection_date is None:
        raise ValueError("query.collection_date must not be None")
    now = _util.to_datetime(now)

    df = _people.load_people_dataframe(
        conn, query=query, fee_rules=fee_rules, log_resulting_data_frame=False, now=now
    )
    df = enrich_people_dataframe_for_payments(
        df,
        collection_date=query.collection_date,
        booking_at=booking_at,
        pedantic=pedantic,
        endtoend_ids=endtoend_ids,
        now=now,
    )
    _LOGGER.info("Resulting pandas DataFrame:\n%s", textwrap.indent(str(df), "  "))
    return df
