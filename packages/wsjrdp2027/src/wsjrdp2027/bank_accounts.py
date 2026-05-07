from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import typing as _typing


_MISSING = object()


class BankAccountDict(_typing.TypedDict, total=False):
    # mandatory
    account_identification: _typing.Required[str]
    opening_balance_cents: _typing.Required[int]
    opening_balance_date: _typing.Required[_datetime.date | str]
    opening_balance_currency: _typing.Required[str]
    # optional
    short_name: str | None
    description: str | None
    iban: str | None
    owner_name: str | None
    owner_address: str | None
    servicer_name: str | None
    servicer_bic: str | None
    servicer_address: str | None


@_dataclasses.dataclass(kw_only=True, eq=True, frozen=True)
class BankAccount:
    # mandatory
    account_identification: str
    opening_balance_cents: int
    opening_balance_date: _datetime.date
    opening_balance_currency: str
    # optional
    short_name: str | None = None
    description: str | None = None
    iban: str | None = None
    owner_name: str | None = None
    owner_address: str | None = None
    servicer_name: str | None = None
    servicer_bic: str | None = None
    servicer_address: str | None = None

    def __init__(self, **kwargs: _typing.Unpack[BankAccountDict]) -> None:
        from . import _util

        for field in _dataclasses.fields(self):
            key = field.name
            if (val := kwargs.get(key, _MISSING)) is not _MISSING:
                if key == "opening_balance_date":
                    val = _util.to_date_or_none(val)
                object.__setattr__(self, field.name, val)

    def asdict(self) -> BankAccountDict:
        return _dataclasses.asdict(self)  # type: ignore


WSJ27_PAX_BANK_GIRO_ACCOUNT = BankAccount(
    account_identification="DE13370601932001939044",
    opening_balance_cents=73579,
    opening_balance_currency="EUR",
    opening_balance_date="2025-08-15",
    short_name="Pax-Bank Girokonto 44",
    iban="DE13370601932001939044",
    owner_name="Ring deutscher Pfadfinder*innenverbände e.V.",
    owner_address="Chausseestraße 128/129, 10115 Berlin",
    servicer_name="Pax-Bank für Kirche und Caritas eG",
    servicer_bic="GENODED1PAX",
    servicer_address="Kamp 17, 33098 Paderborn",
)

WSJ27_PAX_BANK_MONEY_MARKET_ACCOUNT = BankAccount(
    account_identification="DE57370601932001939028",
    opening_balance_cents=0,
    opening_balance_currency="EUR",
    opening_balance_date="2025-11-17",
    short_name="Pax-Bank Tagesgeldkonto 28",
    iban="DE57370601932001939028",
    owner_name="Ring deutscher Pfadfinder*innenverbände e.V.",
    owner_address="Chausseestraße 128/129, 10115 Berlin",
    servicer_name="Pax-Bank für Kirche und Caritas eG",
    servicer_bic="GENODED1PAX",
    servicer_address="Kamp 17, 33098 Paderborn",
)

WSJ27_SKATBANK_GIRO_ACCOUNT = BankAccount(
    account_identification="DE70830654080005498201",
    opening_balance_cents=0,
    opening_balance_currency="EUR",
    opening_balance_date="2025-07-02",
    short_name="Skatbank",
    iban="DE70830654080005498201",
    owner_name="Ring deutscher Pfadfinder*innenverbände e.V.",
    owner_address="Chausseestraße 128/129, 10115 Berlin",
    servicer_name="VR-Bank Altenburger Land eG",
    servicer_bic="GENODEF1SLR",
    servicer_address="Altenburger Str. 13, 04626 Schmölln",
)


WSJ27_ACCOUNTS: list[BankAccount] = [
    WSJ27_PAX_BANK_GIRO_ACCOUNT,
    WSJ27_PAX_BANK_MONEY_MARKET_ACCOUNT,
    WSJ27_SKATBANK_GIRO_ACCOUNT,
]

WSJ27_ACCOUNT_IDENTIFICATION_TO_BANK_ACCOUNT_DICT = {
    acc.account_identification: acc for acc in WSJ27_ACCOUNTS
}
