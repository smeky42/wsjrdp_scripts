from __future__ import annotations

import datetime as _datetime
import pathlib as _pathlib
import typing as _typing

import sepaxml as _sepaxml

import wsjrdp2027

WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG = {
    "name": "Ring deutscher Pfadfinder*innenverbände e.V",
    "IBAN": "DE34520900000077228802",
    "BIC": "GENODE51KS1",
    "creditor_id": "DE81WSJ00002017275",
    "currency": "EUR",
}


class SepaDirectDebitConfig(_typing.TypedDict, total=False):
    name: str
    IBAN: str
    BIC: str
    creditor_id: str


class SepaDirectDebitPayment(_typing.TypedDict, total=False):
    name: str
    IBAN: str
    BIC: str
    amount_cents: int  # amount in cents
    type: str
    collection_date: _datetime.date | _datetime.datetime | str
    mandate_id: str
    mandate_date: _datetime.date | _datetime.datetime | str
    description: str


class SepaDirectDebit:
    def __init__(
        self, config: SepaDirectDebitConfig, *, schema: str = "pain.008.001.02"
    ) -> None:
        raw_config: dict = config.copy()  # type: ignore
        raw_config.setdefault("currency", "EUR")
        raw_config.setdefault("batch", True)
        for key in ["name"]:
            if key in raw_config:
                raw_config[key] = _german_transliterate(raw_config[key])

        self._dd = _sepaxml.SepaDD(raw_config, schema=schema, clean=True)

    def add_payment(self, payment: SepaDirectDebitPayment) -> None:
        from . import _kontocheck as kontocheck

        raw_payment: dict = payment.copy()  # type: ignore
        raw_payment["amount"] = raw_payment.pop("amount")

        raw_iban = raw_payment["IBAN"]
        iban = raw_iban.replace(" ", "").upper()
        raw_bic = raw_payment.pop("BIC", None) or None
        if raw_bic is not None:
            bic = raw_bic.replace(" ", "").upper() or None
        else:
            bic = None

        if not kontocheck.check_iban(iban):
            raise ValueError(f"Invalid IBAN {iban} (from {raw_iban!r})")
        else:
            raw_payment["IBAN"] = iban

        auto_bic = kontocheck.get_bic(iban)
        if raw_bic is None:
            raw_payment["BIC"] = auto_bic
        elif kontocheck.is_bic_compatible(bic, auto_bic):
            raw_payment["BIC"] = auto_bic
        else:
            raise ValueError(f"Inconsistent BIC {bic!r} does not match {auto_bic!r} (derived from IBAN)")

        for key in ["name", "description"]:
            if key in raw_payment:
                raw_payment[key] = _german_transliterate(raw_payment[key])

        self._dd.add_payment(raw_payment)

    def export(self, *, pretty_print: bool = True) -> str:
        return self.export_bytes(pretty_print=pretty_print).decode("utf-8")

    def export_bytes(self, *, pretty_print: bool = True) -> bytes:
        return self._dd.export(validate=True, pretty_print=pretty_print)

    def export_file(self, path: str | _pathlib.Path, pretty_print: bool = True) -> None:
        xml_bytes = self.export_bytes(pretty_print=pretty_print)
        with open(path, "wb") as f:
            f.write(xml_bytes)


def _german_transliterate(s: str) -> str:
    import unicodedata

    s = unicodedata.normalize("NFC", s)

    replacements = {
        "Ä": "Ae",
        "Ö": "Oe",
        "Ü": "Ue",
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
    }
    for key, replacement in replacements.items():
        s = s.replace(key, replacement)
    return s
