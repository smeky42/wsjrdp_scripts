from __future__ import annotations

import collections.abc as _collections_abc
import contextlib as _contextlib
import dataclasses as _dataclasses
import datetime as _datetime
import decimal as _decimal
import pathlib as _pathlib
import re as _re
import typing as _typing


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc
    import csv as _csv

    from . import _context, _person


def ensure_moss_email_mailbox_or_alias(
    ctx: _context.WsjRdpContext, *, people: _collections_abc.Iterable[_person.Person]
) -> None:
    from . import mailbox
    from ._util import console_confirm

    all_mail_aliases = mailbox.get_aliases(ctx)
    addr2goto = {
        a["address"]: frozenset(a["goto"].split(",")) for a in all_mail_aliases
    }

    for p in people:
        expected_goto = p.moss_email_expected_goto
        if expected_goto and (expected_goto != p.moss_email):
            print(p.unit_or_role, p.moss_email, "->", expected_goto)
            goto_addrs = set(s.lower() for s in addr2goto.get(p.moss_email, set()))
            if expected_goto.lower() not in goto_addrs:
                print(f"    ! Missing alias {p.moss_email} -> {expected_goto}")
                print(f"    {p.primary_group.name}  status: {p.status}")
                if console_confirm("Add missing mail alias?"):
                    mailbox.add_alias(ctx, p.moss_email, goto=expected_goto)


def moss_email_with_expected_goto(p: _person.Person, /) -> str:
    assert p.moss_email
    expected_goto = p.moss_email_expected_goto
    if expected_goto and (expected_goto != p.moss_email):
        return f"{p.moss_email} -> {expected_goto}"
    else:
        return p.moss_email


# ======================================================================================
# MossBalanceMovement
# ======================================================================================


@_dataclasses.dataclass(kw_only=True)
class MossBalanceMovement:
    created_at: _datetime.datetime | None = None
    updated_at: _datetime.datetime | None = None
    fin_account_id: int | None = None
    subject_id: int | None = None
    subject_type: str | None = None
    comment: str = ""
    status: str | None = None
    additional_info: dict = _dataclasses.field(default_factory=lambda: {})

    unique_item_number: str
    moss_transaction_id: str
    sub_row_number: int = 0
    transaction_state: str | None = None
    transaction_type: str | None = None
    payment_date: _datetime.date
    booking_date: _datetime.date
    amount_excl_vat: _decimal.Decimal | None = None
    amount: _decimal.Decimal
    currency: str = "EUR"
    original_amount_excl_vat: _decimal.Decimal | None = None
    original_amount: _decimal.Decimal | None = None
    original_currency: str | None = None
    conversion_rate: _decimal.Decimal | None = None
    conversion_rate_including_fees: _decimal.Decimal | None = None
    fees_amount: _decimal.Decimal | None = None
    payment_fee: _decimal.Decimal | None = None
    transaction_amount_excluding_fees: _decimal.Decimal | None = None
    supplier_account: str | None = None
    supplier_name: str | None = None
    account_number: str | None = None
    name_of_expense_account: str | None = None
    category: str | None = None
    moss_balance_account: str | None = None
    cash_in_transit_account: str | None = None
    reason_for_purchase: str | None = None
    note: str = ""
    recipient_account_number: str | None = None
    recipient_bank_code: str | None = None
    payment_reference: str | None = None
    invoice_number: str | None = None
    team_name: str | None = None
    cardholder: str | None = None
    client_number: str | None = None
    first_export_date: _datetime.date | None = None
    moss_expense_id: str | None = None
    moss_invoice_id: str | None = None
    moss_reimbursement_id: str | None = None
    moss_attachment_url: str | None = None

    _FIELD_NAMES_SET: _typing.ClassVar[frozenset[str]]
    _CSV_COLUMN_NAME_TO_FIELD_NAME: _typing.ClassVar[dict[str, str]] = {
        "Transaction ID": "moss_transaction_id",
        "Linked Invoice ID": "moss_invoice_id",
        "Linked Reimbursement ID": "moss_reimbursement_id",
    }

    def __init__(
        self,
        *,
        created_at: _datetime.datetime | str | int | None = None,
        updated_at: _datetime.datetime | str | int | None = None,
        fin_account_id: int | str | None = None,
        subject_id: int | str | None = None,
        subject_type: str | None = None,
        comment: str | None = None,
        status: str | None = None,
        additional_info: dict | None = None,
        unique_item_number: str,
        moss_transaction_id: str,
        sub_row_number: int | str | None = None,
        transaction_state: str | None = None,
        transaction_type: str | None = None,
        payment_date: _datetime.date | str,
        booking_date: _datetime.date | str,
        amount_excl_vat: _decimal.Decimal | float | str | None = None,
        amount: _decimal.Decimal | float | str,
        currency: str | None = None,
        original_amount_excl_vat: _decimal.Decimal | float | str | None = None,
        original_amount: _decimal.Decimal | float | str,
        original_currency: str | None = None,
        conversion_rate: _decimal.Decimal | float | str | None = None,
        conversion_rate_including_fees: _decimal.Decimal | float | str | None = None,
        fees_amount: _decimal.Decimal | float | str | None = None,
        payment_fee: _decimal.Decimal | float | str | None = None,
        transaction_amount_excluding_fees: _decimal.Decimal | float | str | None = None,
        supplier_account: str | None = None,
        supplier_name: str | None = None,
        account_number: str | None = None,
        name_of_expense_account: str | None = None,
        category: str | None = None,
        moss_balance_account: str | None = None,
        cash_in_transit_account: str | None = None,
        reason_for_purchase: str | None = None,
        note: str | None = None,
        recipient_account_number: str | None = None,
        recipient_bank_code: str | None = None,
        payment_reference: str | None = None,
        invoice_number: str | None = None,
        team_name: str | None = None,
        cardholder: str | None = None,
        client_number: str | None = None,
        first_export_date: _datetime.date | str | None = None,
        moss_expense_id: str | None = None,
        moss_invoice_id: str | None = None,
        moss_reimbursement_id: str | None = None,
        moss_attachment_url: str | None = None,
    ) -> None:
        from . import _util

        self.created_at = _util.to_datetime_or_none(created_at)
        self.updated_at = _util.to_datetime_or_none(updated_at)
        self.fin_account_id = _util.to_int_or_none(fin_account_id)
        self.subject_id = _util.to_int_or_none(subject_id)
        self.subject_type = subject_type or None
        self.comment = comment or ""
        self.status = status or None
        self.additional_info = additional_info or {}
        self.unique_item_number = unique_item_number
        self.moss_transaction_id = moss_transaction_id
        self.sub_row_number = _util.to_int(sub_row_number or 0)
        self.transaction_state = transaction_state
        self.transaction_type = transaction_type
        self.payment_date = _util.to_date(payment_date)
        self.booking_date = _util.to_date(booking_date)
        self.amount_excl_vat = _util.to_decimal_or_none(amount_excl_vat)
        self.amount = _util.to_decimal(amount)
        self.currency = currency or "EUR"
        self.original_amount_excl_vat = _util.to_decimal_or_none(
            original_amount_excl_vat
        )
        self.original_amount = _util.to_decimal(original_amount)
        self.original_currency = original_currency or "EUR"

        self.conversion_rate = _util.to_decimal_or_none(conversion_rate)
        self.conversion_rate_including_fees = _util.to_decimal_or_none(
            conversion_rate_including_fees
        )
        self.fees_amount = _util.to_decimal_or_none(fees_amount)
        self.payment_fee = _util.to_decimal_or_none(payment_fee)
        self.transaction_amount_excluding_fees = _util.to_decimal_or_none(
            transaction_amount_excluding_fees
        )
        self.supplier_account = supplier_account or None
        self.supplier_name = supplier_name or None
        self.account_number = account_number or None
        self.name_of_expense_account = name_of_expense_account or None
        self.category = category or None
        self.moss_balance_account = moss_balance_account or None
        self.cash_in_transit_account = cash_in_transit_account or None
        self.reason_for_purchase = reason_for_purchase or None
        self.note = note or ""
        self.recipient_account_number = recipient_account_number or None
        self.recipient_bank_code = recipient_bank_code or None
        self.payment_reference = _normalize_payment_reference(payment_reference)
        self.invoice_number = invoice_number or None
        self.team_name = team_name or None
        self.cardholder = cardholder or None
        self.client_number = client_number or None
        self.first_export_date = _util.to_date_or_none(first_export_date)
        self.moss_expense_id = moss_expense_id or None
        self.moss_invoice_id = moss_invoice_id or None
        self.moss_reimbursement_id = moss_reimbursement_id or None
        self.moss_attachment_url = moss_attachment_url or None
        if self.moss_attachment_url and self.moss_attachment_url.startswith(
            "getmoss.com/"
        ):
            self.moss_attachment_url = f"https://{self.moss_attachment_url}"

    @classmethod
    def iter_from_path(
        cls, path: _pathlib.Path | str, /
    ) -> _collections_abc.Iterator[_typing.Self]:

        with _csv_dict_reader(path) as reader:
            for row in reader:
                kwargs = {
                    fld_name: val
                    for col, val in row.items()
                    if (fld_name := cls.__csv_col2fld_name(col)) is not None
                }
                bm = cls(**kwargs)
                if bm.moss_transaction_id == "2039d32d-64c8-4758-b31f-21886f4404d4":
                    bm.amount = _decimal.Decimal("-10.24")
                    bm.original_currency = "PLN"
                    bm.conversion_rate = None
                yield bm

    def __getitem__(self, key: str, /) -> _typing.Any:
        if key in self._FIELD_NAMES_SET:
            return getattr(self, key)
        else:
            raise KeyError(key)

    def asdict(self) -> dict:
        return _dataclasses.asdict(self)

    @classmethod
    def __csv_col2fld_name(cls, k: str) -> str | None:
        if new_k := cls._CSV_COLUMN_NAME_TO_FIELD_NAME.get(k):
            k = new_k
        else:
            k = _re.sub("[^a-z0-9]+", "_", k.lower()).strip("_")
        return k if k in cls._FIELD_NAMES_SET else None


MossBalanceMovement._FIELD_NAMES_SET = frozenset(
    fld.name for fld in _dataclasses.fields(MossBalanceMovement)
)


# ======================================================================================
# Local Helper Functions
# ======================================================================================


@_contextlib.contextmanager
def _csv_dict_reader(
    path: _pathlib.Path | str, /
) -> _collections_abc.Iterator[_csv.DictReader]:
    import csv as _csv

    with open(path, newline="") as csvfile:
        head = csvfile.read(1024)
        delimiter = _sniff_delimiter(head)
        csvfile.seek(0)

        reader = _csv.DictReader(csvfile, delimiter=delimiter)
        yield reader


def _sniff_delimiter(head) -> str:
    count_comma = head.count(",")
    count_semi = head.count(";")

    if count_comma > 10 and count_comma > 2 * count_semi:
        return ","

    elif count_semi > 10 and count_semi > 2 * count_comma:
        return ";"

    else:
        raise RuntimeError("Failed to detect delimiter")


_PAYMENT_REFERENCE_RDP_RE = _re.compile(
    r"\s*(?:-|- )?[Rr]ing deutscher [Pp]fadfinder[.':*]innenverb(?:ä|ae|a|)nde e[.]?[Vv][.]?"
)

_PAYMENT_REFERENCE_RDP_SUFFIX = "Ring deutscher Pfadfinder.innenverbände e.V."


def _normalize_payment_reference(text: str | None, /, length: int = 140) -> str | None:
    """Normalize payment reference

    >>> _normalize_payment_reference("Kundennummer: 123456 / Belegnummer: 345678 / WSJ27 Unit T1 / abcdef01-2345-6789-abcd-ef0123456790 - Ring deutscher Pfadfinder.innenverbände")
    'Kundennummer: 123456 / Belegnummer: 345678 / WSJ27 Unit T1 / abcdef01-2345-6789-abcd-ef0123456790'
    >>> _normalize_payment_reference("foo - Ring deutscher Pfadfinder.innenverbände e.V.")
    'foo'
    >>> _normalize_payment_reference("foo ring deutscher Pfadfinder.innenverbände e.V.")
    'foo'
    >>> _normalize_payment_reference("foo ring deutscher Pfadfinder*innenverbände e.V.")
    'foo'
    >>> _normalize_payment_reference("foo ring deutscher Pfadfinder'innenverbaende e.V.")
    'foo'
    >>> _normalize_payment_reference("foo   - Ring deutscher Pfadfinder'innenverbaende eV")
    'foo'

    >>> _normalize_payment_reference("foo - Ring deutscher Pfadfinder.innenverbände", length=4)
    'foo'
    >>> _normalize_payment_reference("foo - Ring deutscher Pfadfinder.innenverb", length=4)
    'foo'
    >>> _normalize_payment_reference("foo - Ring deutscher Pfadfinder.", length=4)
    'foo'
    >>> _normalize_payment_reference("foo - Ring deutsche", length=4)
    'foo'
    """
    text = text or ""
    # Note: Due to stripped trailing whitespace before this function
    # was called, len(text) might be less than *length* even if
    # truncation of the rdp suffix happened. Therefore we compare
    # `len(text) + 1` with length.
    if len(text) + 1 >= length:
        suffix_len = len(_PAYMENT_REFERENCE_RDP_SUFFIX)
        for i in range(suffix_len - 2):
            candidate = _PAYMENT_REFERENCE_RDP_SUFFIX[: suffix_len - i]
            if text.endswith(candidate):
                text = text[: -len(candidate)].rstrip(" -")
                break
    else:
        text = _PAYMENT_REFERENCE_RDP_RE.sub("", text).rstrip(" -")
    return text[:length] or None
