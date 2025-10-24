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
    "amount_paid",
    "amount_due",
    "amount",
    "payment_status_reason",
    "payment_status",
]


# input: [dt.datetime.strptime(d, '%b %Y').date() for d in _PAYMENT_ARRAY[0][2:]]
_PAYMENT_DATES = [
    _datetime.date.min,
    _datetime.date(2025, 8, 1),  # cut-off for earliest Early-Payer
    _datetime.date(2025, 11, 1),  # cut-off for middle Early-Payer
    _datetime.date(2025, 12, 1),
    _datetime.date(2026, 1, 1),
    _datetime.date(2026, 2, 1),
    _datetime.date(2026, 3, 1),
    _datetime.date(2026, 8, 1),
    _datetime.date(2026, 11, 1),
    _datetime.date(2027, 2, 1),
    _datetime.date(2027, 5, 1),
    _datetime.date.max,
]

_AUG_25 = _datetime.date(2025, 8, 1)
_NOV_25 = _datetime.date(2025, 11, 1)


_PAYMENT_ARRAY = [
    [
        "Rolle",
        "Gesamt",
        "Dez 2025",
        "Jan 2026",
        "Feb 2026",
        "Mär 2026",
        "Aug 2026",
        "Nov 2026",
        "Feb 2027",
        "Mai 2027",
    ],
    [
        "RegularPayer::Group::Unit::Member",
        "3400",
        "300",
        "500",
        "500",
        "500",
        "400",
        "400",
        "400",
        "400",
    ],
    [
        "RegularPayer::Group::Unit::Leader",
        "2400",
        "150",
        "350",
        "350",
        "350",
        "300",
        "300",
        "300",
        "300",
    ],
    [
        "RegularPayer::Group::Ist::Member",
        "2600",
        "200",
        "400",
        "400",
        "400",
        "300",
        "300",
        "300",
        "300",
    ],
    [
        "RegularPayer::Group::Root::Member",
        "1600",
        "50",
        "250",
        "250",
        "250",
        "200",
        "200",
        "200",
        "200",
    ],
    ["EarlyPayer::Group::Unit::Member", "3400", "", "", "", "", "", "", "", ""],
    ["EarlyPayer::Group::Unit::Leader", "2400", "", "", "", "", "", "", "", ""],
    ["EarlyPayer::Group::Ist::Member", "2600", "", "", "", "", "", "", "", ""],
    ["EarlyPayer::Group::Root::Member", "1600", "", "", "", "", "", "", "", ""],
]


class PaymentRole(_enum.Enum):
    REGULAR_PAYER_CMT = "RegularPayer::Group::Root::Member"
    REGULAR_PAYER_YP = "RegularPayer::Group::Unit::Member"
    REGULAR_PAYER_UL = "RegularPayer::Group::Unit::Leader"
    REGULAR_PAYER_IST = "RegularPayer::Group::Ist::Member"

    EARLY_PAYER_CMT = "EarlyPayer::Group::Root::Member"
    EARLY_PAYER_YP = "EarlyPayer::Group::Unit::Member"
    EARLY_PAYER_UL = "EarlyPayer::Group::Unit::Leader"
    EARLY_PAYER_IST = "EarlyPayer::Group::Ist::Member"

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}.{self.name}"

    @property
    def is_early_payer(self) -> bool:
        return self in _EARLY_PAYER_ROLES

    @property
    def is_regular_payer(self) -> bool:
        return self in _REGULAR_PAYER_ROLES

    @property
    def db_payment_role(self) -> str:
        """The string for the payment_role column in the database.

        >>> PaymentRole.REGULAR_PAYER_CMT.db_payment_role
        'RegularPayer::Group::Root::Member'
        """
        return self.value

    @property
    def short_role_name(self) -> str:
        return self.name.rsplit("_", 1)[1]

    @property
    def full_fee_eur(self) -> int:
        """Full amount due for this role in EUR.

        >>> PaymentRole.REGULAR_PAYER_CMT.full_fee_eur
        1600
        """
        return _PAYMENT_ROLE_TO_FULL_FEE_EUR[self]

    @property
    def full_fee_cent(self) -> int:
        """Full amount due for this role in EUR-cents.

        >>> PaymentRole.REGULAR_PAYER_CMT.full_fee_cent
        160000
        """
        return self.full_fee_eur * 100

    def fee_due_by_date_in_eur(
        self,
        date: _datetime.date | str,
        *,
        print_at: _datetime.date | str | None = None,
    ) -> int:
        """Return the accumulated fees due for this role by *date* in EUR.

        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('1900-01-01')
        0
        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('1900-07-31')
        0
        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('2025-08-01')
        0
        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('2025-11-30')
        0
        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('2025-12-01')
        300
        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('2031-01-01')
        3400


        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('1900-01-01')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('1900-07-31')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-08-01')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-11-30')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-12-01')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2031-01-01')
        3400

        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('1900-07-31', print_at='2025-07-31')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-08-01', print_at='2025-07-31')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-10-31', print_at='2025-07-31')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-11-01', print_at='2025-07-31')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2031-01-01', print_at='2025-07-31')
        3400

        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('1900-07-31', print_at='2025-08-01')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-08-01', print_at='2025-08-01')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-10-31', print_at='2025-08-01')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-11-01', print_at='2025-08-01')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2031-01-01', print_at='2025-08-01')
        3400
        """
        import bisect

        from ._util import to_date

        date = to_date(date)
        pos = max(bisect.bisect_right(_PAYMENT_DATES, date) - 1, 0)
        amount = _PAYMENT_ROLE_TO_ACCUMULATED_INSTALLMENTS[self][pos]
        if self.is_early_payer and print_at is not None:
            print_at = to_date(print_at)
            if date < _NOV_25 and print_at >= _AUG_25:
                return 0
        return amount

    def fee_due_by_date_in_cent(
        self,
        date: _datetime.date | str,
        *,
        print_at: _datetime.date | str | None = None,
    ) -> int:
        return self.fee_due_by_date_in_eur(date, print_at=print_at) * 100


_ALL_PAYMENT_ROLES = frozenset(role.value for role in PaymentRole)


_PAYMENT_ROLE_TO_FULL_FEE_EUR: dict[str | PaymentRole, int] = {
    x[0]: int(x[1]) for x in _PAYMENT_ARRAY[1:]
}
assert _ALL_PAYMENT_ROLES == frozenset([x[0] for x in _PAYMENT_ARRAY[1:]])

_PAYMENT_ROLE_TO_FULL_FEE_EUR.update(
    {role: _PAYMENT_ROLE_TO_FULL_FEE_EUR[role.value] for role in PaymentRole}
)


_EARLY_PAYER_ROLES = frozenset(
    [role for role in PaymentRole if role.db_payment_role.startswith("EarlyPayer::")]
)
_REGULAR_PAYER_ROLES = frozenset(
    [role for role in PaymentRole if role.db_payment_role.startswith("RegularPayer::")]
)
assert all(role.is_early_payer or role.is_regular_payer for role in PaymentRole)
assert not any(role.is_early_payer and role.is_regular_payer for role in PaymentRole)

_PAYMENT_ROLE_TO_INSTALLMENTS: dict[str | PaymentRole, list[int]] = {
    x[0]: (
        [0]
        + [int(x[1]) if x[0].startswith("EarlyPayer::") else 0]
        + [0]
        + [int(s or "0") for s in x[2:]]
        + [0]
    )
    for x in _PAYMENT_ARRAY[1:]
}
_PAYMENT_ROLE_TO_INSTALLMENTS.update(
    {role: _PAYMENT_ROLE_TO_INSTALLMENTS[role.value] for role in PaymentRole}
)

_PAYMENT_ROLE_TO_ACCUMULATED_INSTALLMENTS = {
    k: list(_itertools.accumulate(v)) for k, v in _PAYMENT_ROLE_TO_INSTALLMENTS.items()
}


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

    df["early_payer"] = df["early_payer"].map(lambda x: bool(x))
    df["payment_role"] = df["payment_role"].map(lambda s: PaymentRole(s) if s else None)
    df["sepa_iban"] = df["sepa_iban"].map(lambda s: s.replace(" ", "").upper() if s else None)  # fmt: skip
    df["sepa_bic"] = df["sepa_bic"].map(lambda s: s.replace(" ", "").upper() if s else None)  # fmt: skip
    df["sepa_bic_status"] = None
    df["sepa_bic_status_reason"] = ""

    df["collection_date"] = collection_date
    df["mandate_id"] = df["id"].map(mandate_id_from_hitobito_id)
    df["mandate_date"] = df["print_at"].map(lambda d: d if d else collection_date)

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


def compute_total_fee_due(row) -> int:
    payment_role = row["payment_role"]
    if payment_role:
        return payment_role.fee_due_by_date_in_cent(
            row["collection_date"], print_at=row["print_at"]
        )
    else:
        return 0


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
    status: str | _collections_abc.Iterable[str] = ("reviewed", "confirmed"),
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
            "people.print_at",
            "COALESCE(people.sepa_status, 'ok') AS sepa_status",
            "COUNT(accounting_entries.id) AS accounting_entries_count",
            "SUM(COALESCE(accounting_entries.amount_cents, 0)) AS amount_paid",
        ],
        join="LEFT OUTER JOIN accounting_entries ON people.id = accounting_entries.subject_id AND accounting_entries.subject_type = 'Person' AND accounting_entries.amount_currency = 'EUR'",
        group_by="people.id",
        status=status,
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
