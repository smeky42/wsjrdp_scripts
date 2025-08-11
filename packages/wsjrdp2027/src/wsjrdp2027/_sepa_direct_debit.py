from __future__ import annotations

import datetime as _datetime
import logging as _logging
import pathlib as _pathlib
import typing as _typing

import sepaxml as _sepaxml

if _typing.TYPE_CHECKING:
    import pandas as _pandas


_LOGGER = _logging.getLogger(__name__)


WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG = {
    "name": "Ring deutscher Pfadfinder*innenverbände e.V.",
    "IBAN": "DE34520900000077228802",
    "BIC": "GENODE51KS1",
    "creditor_id": "DE81WSJ00002017275",
    "currency": "EUR",
}

WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG = {
    "name": "Ring deutscher Pfadfinder*innenverbände e.V.",
    "IBAN": "DE13370601932001939044",
    "BIC": "GENODED1PAX",
    "creditor_id": "DE81WSJ00002017275",
    "currency": "EUR",
}


class SepaDirectDebitConfig(_typing.TypedDict, total=False):
    name: str
    IBAN: str
    BIC: str
    creditor_id: str
    currency: str


class SepaDirectDebitPayment(_typing.TypedDict, total=False):
    name: str
    IBAN: str
    BIC: str
    amount: int
    type: str
    collection_date: _datetime.date | _datetime.datetime | str
    mandate_id: str
    mandate_date: _datetime.date | _datetime.datetime | str
    description: str
    endtoend_id: str


class SepaDirectDebit:
    _num_payments: int

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
        self._num_payments = 0

    @property
    def num_payments(self) -> int:
        """Number of payments added to this SEPA direct debit."""
        return self._num_payments

    def __pedantic_payment_check(self, payment: SepaDirectDebitPayment) -> None:
        from . import _kontocheck as kontocheck

        raw_iban = payment.get("IBAN", "")
        raw_bic = payment.get("BIC", None)
        iban = raw_iban.replace(" ", "").upper()
        bic = raw_bic.replace(" ", "").upper() if raw_bic else None

        if not kontocheck.check_iban(iban):
            raise ValueError(f"Invalid IBAN {iban} (from {raw_iban!r})")
        else:
            payment["IBAN"] = iban

        auto_bic = kontocheck.get_bic(iban)
        if bic is None:
            payment["BIC"] = auto_bic
        elif kontocheck.is_bic_compatible(bic, auto_bic):
            payment["BIC"] = auto_bic
        else:
            if bic is not None and len(bic) not in (8, 11):
                raise ValueError(
                    f"Invalid BIC {bic!r} (got length={len(bic)}, must be 8 or 11); BIC from IBAN: {auto_bic}"
                )
            else:
                raise ValueError(
                    f"Inconsistent BIC {bic!r} does not match {auto_bic} (derived from IBAN)"
                )

    def add_payment(
        self, payment: SepaDirectDebitPayment, *, pedantic: bool = True
    ) -> SepaDirectDebitPayment:
        raw_payment: SepaDirectDebitPayment = payment.copy()  # type: ignore
        raw_payment["amount"] = raw_payment.pop("amount")
        if pedantic:
            self.__pedantic_payment_check(raw_payment)
        else:
            raw_payment["IBAN"] = raw_payment.get("IBAN", "").replace(" ", "").upper()
            raw_payment.pop("BIC")

        for key in ["name", "description"]:
            if key in raw_payment:
                raw_payment[key] = _german_transliterate(raw_payment[key])

        self._dd.add_payment(raw_payment)
        self._num_payments += 1
        return raw_payment

    def add_payment_from_accounting_row(
        self, row: _pandas.Series, *, pedantic: bool = True
    ) -> SepaDirectDebitPayment:
        payment: SepaDirectDebitPayment = {
            "name": row["sepa_name"],
            "IBAN": row["sepa_iban"],
            "BIC": row["sepa_bic"],
            "amount": row["amount"],
            "type": row.get("sepa_debit_type", "OOFF"),  # FRST,RCUR,OOFF,FNAL
            "collection_date": row["collection_date"],
            "mandate_id": row["mandate_id"],
            "mandate_date": row["mandate_date"],
            "description": row["sepa_debit_description"],
        }
        if endtoend_id := row.get("sepa_debit_endtoend_id"):
            payment["endtoend_id"] = endtoend_id
        return self.add_payment(payment, pedantic=pedantic)

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


def write_accounting_dataframe_to_sepa_dd(
    df: _pandas.DataFrame,
    path: str | _pathlib.Path,
    *,
    config: SepaDirectDebitConfig,
    pedantic: bool = True,
) -> int:
    dd = SepaDirectDebit(config)

    already_not_ok = len(df[df["payment_status"] != "ok"])

    for idx, row in df.iterrows():
        if row["payment_status"] != "ok":
            _LOGGER.debug(
                "Skip non-ok row id=%s payment_status=%s payment_status_reason=%r",
                row["id"],
                row["payment_status"],
                row["payment_status_reason"],
            )
            continue

        skip_reasons = []
        if row.get("amount", 0) == 0:
            skip_reasons.append("amount = 0")
        if not row.get("payment_role", None):
            skip_reasons.append("payment_role IS NULL")
        if not row.get("sepa_iban", None):
            skip_reasons.append("sepa_iban IS NULL")

        if skip_reasons:
            skip_reason = ", ".join(skip_reasons)
            df.at[idx, "payment_status"] = "skipped"
            df.at[idx, "payment_status_reason"] = skip_reason
            _LOGGER.warning(
                "Skip row id=%s payment_status_reason=%r", row["id"], skip_reason
            )
            continue

        row_msg = " ".join(
            str(x)
            for x in [
                row.get("id"),
                row.get("short_full_name"),
                row.get("payment_role"),
                row.get("amount"),
                row.get("sepa_iban"),
                row.get("print_at"),
            ]
        )
        _LOGGER.info("%s", row_msg)
        try:
            dd.add_payment_from_accounting_row(row, pedantic=pedantic)
        except (KeyError, ValueError) as exc:
            df.at[idx, "payment_status"] = "skipped"
            reason = f"{type(exc).__qualname__}: {exc}"
            df.at[idx, "payment_status_reason"] = reason
            _LOGGER.warning("Caught exception: %s", reason)

    now_not_ok = len(df[df["payment_status"] != "ok"])
    _LOGGER.info("Newly skipped rows: %s", now_not_ok - already_not_ok)

    if dd.num_payments == 0:
        _LOGGER.warning("No payments added to Direct Debit => No file written")
    else:
        _LOGGER.info("Write %s", path)
        dd.export_file(path)

    return dd.num_payments
