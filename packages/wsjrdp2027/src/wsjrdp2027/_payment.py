from __future__ import annotations

import datetime as _datetime
import enum as _enum
import itertools as _itertools
import logging as _logging
import typing as _typing
from collections import abc as _collections_abc


if _typing.TYPE_CHECKING:
    import pathlib as _pathlib

    import pandas as _pandas
    import psycopg as _psycopg


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
    "OK",
    "In Überprüfung",
    "Zahlung ausstehend",
]


PAYMENT_DATAFRAME_COLUMNS = [
    "id",
    "primary_group_id",
    "first_name",
    "last_name",
    "status",
    "short_first_name",
    "nickname",
    "greeting_name",
    "full_name",
    "short_full_name",
    "email",
    "gender",
    "payment_role",
    "early_payer",
    "sepa_name",
    "sepa_mail",
    "sepa_iban",
    "sepa_bic",
    "sepa_bic_status",
    "sepa_bic_status_reason",
    "sepa_status",
    "print_at",
    "collection_date",
    "mandate_id",
    "mandate_date",
    "sepa_debit_type",
    "sepa_debit_description",
    "sepa_debit_endtoend_id",
    "accounting_entries_count",
    "accounting_entry_id",
    "accounting_author_id",
    "accounting_value_date",
    "accounting_booking_at",
    "accounting_comment",
    "total_fee_regular_cents",
    "total_fee_reduction_comment",
    "total_fee_reduction_cents",
    "amount_paid",
    "amount_due",
    "amount",
    "payment_status_reason",
    "payment_status",
]


def mandate_id_from_hitobito_id(hitobito_id: str | int) -> str:
    return f"wsjrdp2027{hitobito_id}"


def enrich_dataframe_for_payments(
    df: _pandas.DataFrame,
    collection_date: _datetime.date | str = "2025-01-01",
    booking_at: _datetime.datetime | None = None,
    pedantic: bool = True,
) -> _pandas.DataFrame:
    import datetime

    from ._util import to_date

    def dd_type_from_row(row) -> str:
        # FRST, RCUR, OOFF, FNAL
        if row["early_payer"] or row["amount_due"] == 0:
            return "OOFF"
        else:
            # It seems that it is OK to always use RCUR for recurring
            # payments, even if FRST or FNAL would be somewhat more
            # correct.
            return "RCUR"

    def dd_description_from_row(row) -> str:
        prefix = "WSJ 2027"
        payment_role = row["payment_role"]
        short_role_name = payment_role.short_role_name if payment_role else "UNKNOWN"
        suffix = f"{row['short_full_name']} {short_role_name} {row['id']}"
        if row["early_payer"]:
            return f"{prefix} Beitrag {suffix}"
        else:
            year_month = row["collection_date"].strftime("%Y-%m")
            # TODO: Check if amount is larger than usual
            # TODO: improve format of year_month
            # TODO: determine Ratenzahlungsmonat from collection_date
            return f"{prefix} {year_month} Rate {suffix}"

    def dd_endtoend_id_from_row(row) -> str:
        import uuid

        from ._payment import mandate_id_from_hitobito_id

        mandate_id = mandate_id_from_hitobito_id(row["id"])
        count_accounting_entries = row.get("accounting_entries_count", "0")
        random_hex = uuid.uuid4().hex[:10]
        endtoend_id = f"{mandate_id}-{count_accounting_entries}-{random_hex}"
        return endtoend_id[:35]

    def accounting_comment_from_row(row: _pandas.Series) -> str:
        endtoend_id = row["sepa_debit_endtoend_id"]
        collection_date = row["collection_date"]
        collection_date_de = collection_date.strftime("%d.%m.%Y")
        sepa_name = row.get("sepa_name")
        sepa_iban = row.get("sepa_iban")
        sepa_debit_type = row.get("sepa_debit_type")
        return (
            f"SEPA Lastschrifteinzug {endtoend_id} zum {collection_date_de} "
            f"(Kontoinhaber*in: {sepa_name}, IBAN: {sepa_iban}, Sequenz: {sepa_debit_type})"
        )

    collection_date = to_date(collection_date)
    if booking_at is None:
        booking_at = datetime.datetime.now()

    df["sepa_iban"] = df["sepa_iban"].map(lambda s: s.replace(" ", "").upper() if s else None)  # fmt: skip
    df["sepa_bic"] = df["sepa_bic"].map(lambda s: s.replace(" ", "").upper() if s else None)  # fmt: skip
    df["sepa_bic_status"] = None
    df["sepa_bic_status_reason"] = ""

    df["collection_date"] = collection_date
    df["mandate_id"] = df["id"].map(mandate_id_from_hitobito_id)
    df["mandate_date"] = df["print_at"].map(lambda d: d if d else collection_date)

    df["accounting_entries_count"] = df["accounting_entries_amounts_cents"].map(lambda amounts: len(amounts))  # fmt: skip
    df["amount_paid"] = df["accounting_entries_amounts_cents"].map(lambda amounts: sum(amounts))  # fmt: skip
    df["amount_due"] = df.apply(compute_total_fee_due, axis=1)
    df["amount"] = df.apply(
        lambda row: max(row["amount_due"] - row["amount_paid"], 0), axis=1
    )
    df["sepa_debit_type"] = df.apply(dd_type_from_row, axis=1)
    df["sepa_debit_description"] = df.apply(dd_description_from_row, axis=1)
    df["sepa_debit_endtoend_id"] = df.apply(dd_endtoend_id_from_row, axis=1)
    df["payment_status_reason"] = df["amount"].map(
        lambda amt: "" if amt > 0 else "amount = 0"
    )
    df["payment_status"] = df["payment_status_reason"].map(
        lambda rsn: "ok" if not rsn else "skipped"
    )
    df["accounting_entry_id"] = None  # accounting_entries.id
    df["accounting_author_id"] = 65  # TODO: maybe (2 - Peter or 65 - Daffi)
    df["accounting_value_date"] = df["collection_date"]  # best guess we can do
    df["accounting_booking_at"] = booking_at
    df["accounting_comment"] = df.apply(accounting_comment_from_row, axis=1)

    _check_iban_bic_in_payment_dataframe(df, pedantic=pedantic)

    return df.reindex(columns=PAYMENT_DATAFRAME_COLUMNS)


def format_cents_as_eur_de(cents: int, zero_cents: str = ",—") -> str:
    from babel.numbers import format_currency

    return format_currency(cents / 100, "EUR", locale="de_DE").replace(
        ",00", zero_cents
    )


def to_int_or_none(obj: object) -> int | None:
    try:
        return int(obj)
    except Exception:
        return None


def mk_installments_plan(row) -> list[tuple[_datetime.date, int]] | None:
    year = to_int_or_none(row["custom_installments_starting_year"])
    custom_installments_cents = row["custom_installments_cents"]
    if year is None or custom_installments_cents is None:
        return None
    plan = []
    for i, cents in enumerate(custom_installments_cents):
        print(i, cents)
        #  0   ->  year + 0,   1,  5
        #  11  ->  year + 0,  12,  5
        #  12  ->  year + 1,   1,  5
        print((year + i // 12, (i % 12) + 1, 5))
        d = _datetime.date(year + i // 12, (i % 12) + 1, 5)
        plan.append((d, cents))
    return plan


def fee_due_by_date_in_cent_from_plan(
    date: _datetime.date, installments_cents: dict[tuple[int, int], int]
) -> int:
    import bisect

    plan = [
        (_datetime.date(year, month, 5), cents)
        for (year, month), cents in sorted(
            installments_cents.items(), key=lambda item: item[0]
        )
    ]
    dates = [_datetime.date.min, *(x[0] for x in plan), _datetime.date.max]
    installments = [0, *(x[1] for x in plan), 0]
    accumulated = list(_itertools.accumulate(installments))
    pos = max(bisect.bisect_right(dates, date) - 1, 0)
    return accumulated[pos]


def compute_total_fee_due(row) -> int:
    installments_cents = row["installments_cents"]
    collection_date: _datetime.date = row["collection_date"]
    if installments_cents is None:
        return 0
    else:
        return fee_due_by_date_in_cent_from_plan(collection_date, installments_cents)


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
) -> int:
    import datetime

    from psycopg.sql import SQL, Identifier, Literal

    from ._util import to_date

    if created_at is None:
        created_at = datetime.datetime.now().date()
    else:
        created_at = to_date(created_at)

    cols_vals = [
        ("subject_type", "Person"),
        ("subject_id", int(subject_id)),
        ("author_type", "Person"),
        ("author_id", int(author_id)),
        ("amount_currency", "EUR"),
        ("amount_cents", int(amount)),
        ("description", description),
        ("created_at", created_at),
    ]
    cols = [*(Identifier(col_val[0]) for col_val in cols_vals)]
    vals = [*(Literal(col_val[1]) for col_val in cols_vals)]
    sql_string = (
        SQL("INSERT INTO accounting_entries ({}) VALUES ({}) RETURNING {}")
        .format(SQL(", ").join(cols), SQL(", ").join(vals), Identifier("id"))
        .as_string()
    )
    _LOGGER.debug("[ACC] execute %s", sql_string)
    cursor.execute(sql_string)

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
        description=row["accounting_comment"],
        created_at=booking_at,
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
                "[ACC] subject_id=%s sepa_name=%r %r %s print_at=%s amount=%s -> id=%s",
                row.get("id"),
                row.get("sepa_name"),
                row.get("short_full_name"),
                row.get("payment_role"),
                row.get("print_at"),
                int(row.get("amount", 0)),
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


def load_payment_dataframe(
    conn: _psycopg.Connection,
    *,
    collection_date: _datetime.date | str = "2025-01-01",
    booking_at: _datetime.datetime | None = None,
    pedantic: bool = True,
    where: str = "",
    early_payer: bool | None = None,
    max_print_at: str | _datetime.date | None = None,
    status: str | _collections_abc.Iterable[str] | None = ("reviewed", "confirmed"),
    fee_rules: str | _collections_abc.Iterable[str] = "active",
    sepa_status: str | _collections_abc.Iterable[str] = "ok",
    today: _datetime.date | str | None = None,
) -> _pandas.DataFrame:
    import textwrap

    from . import _people, _util

    where = _util.combine_where(
        where, _util.in_expr("COALESCE(people.sepa_status, 'ok')", sepa_status)
    )
    if early_payer is not None:
        if early_payer:
            where = _util.combine_where(where, "people.early_payer = TRUE")
        else:
            where = _util.combine_where(
                where, "(people.early_payer = FALSE OR people.early_payer IS NULL)"
            )
    if max_print_at is not None:
        where = _util.combine_where(
            where, f"people.print_at <= '{_util.to_date(max_print_at).isoformat()}'"
        )

    df = _people.load_people_dataframe(
        conn,
        where=where,
        extra_cols=[
            """ARRAY(
    SELECT COALESCE(e.amount_cents, 0)
    FROM accounting_entries AS e
    WHERE e.subject_type = 'Person' AND e.subject_id = people."id" AND e.amount_currency = 'EUR'
  ) AS accounting_entries_amounts_cents""",
        ],
        status=status,
        fee_rules=fee_rules,
        log_resulting_data_frame=False,
        today=today,
    )

    collection_date = _util.to_date(collection_date)
    today = collection_date if today is None else min(today, collection_date)
    df = enrich_dataframe_for_payments(
        df,
        collection_date=collection_date,
        booking_at=booking_at,
        pedantic=pedantic,
    )
    _LOGGER.info("Resulting pandas DataFrame:\n%s", textwrap.indent(str(df), "  "))
    return df
