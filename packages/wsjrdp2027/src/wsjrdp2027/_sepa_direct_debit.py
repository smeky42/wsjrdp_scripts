from __future__ import annotations

import datetime as _datetime
import logging as _logging
import pathlib as _pathlib
import typing as _typing

import sepaxml as _sepaxml

from . import _types


if _typing.TYPE_CHECKING:
    import pandas as _pandas


_LOGGER = _logging.getLogger(__name__)


CREDITOR_ID = "DE81WSJ00002017275"


WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG: _types.SepaDirectDebitConfig = {
    "name": "Ring deutscher Pfadfinder*innenverbände e.V.",
    "IBAN": "DE70830654080005498201",
    "BIC": "GENODEF1SLR",
    "creditor_id": CREDITOR_ID,
    "currency": "EUR",
    "address_as_single_line": "Chausseestraße 128/129, 10115 Berlin",
}

WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG: _types.SepaDirectDebitConfig = {
    "name": "Ring deutscher Pfadfinder*innenverbände e.V.",
    "IBAN": "DE13370601932001939044",
    "BIC": "GENODED1PAX",
    "creditor_id": CREDITOR_ID,
    "currency": "EUR",
    "address_as_single_line": "Chausseestraße 128/129, 10115 Berlin",
}


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
        self, config: _types.SepaDirectDebitConfig, *, schema: str = "pain.008.001.02"
    ) -> None:
        raw_config: dict = config.copy()  # type: ignore
        raw_config.pop("address_as_single_line", None)  # not to be seen by sepaxml
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

    def add_payment(
        self, payment: SepaDirectDebitPayment, *, pedantic: bool = True
    ) -> SepaDirectDebitPayment:
        raw_payment: SepaDirectDebitPayment = payment.copy()
        raw_payment["amount"] = raw_payment.pop("amount")

        raw_payment["IBAN"] = raw_payment.get("IBAN", "").replace(" ", "").upper()

        # We assume that the BIC is not required (which it is not for
        # most if not all EUR SEPA direct debit payments) and hence
        # skip it as the quality of user entered BIC data is not good
        # enough to transmit it unless required.
        raw_payment.pop("BIC")

        for key in ["name", "description"]:
            if key in raw_payment:
                raw_payment[key] = _german_transliterate(raw_payment[key])  # ty: ignore

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
            "amount": row["open_amount_cents"],
            "type": row.get("sepa_dd_sequence_type", "OOFF"),  # FRST,RCUR,OOFF,FNAL
            "collection_date": row["collection_date"],
            "mandate_id": row["sepa_mandate_id"],
            "mandate_date": row["sepa_mandate_date"],
            "description": row["sepa_dd_description"],
        }
        if endtoend_id := row.get("sepa_dd_endtoend_id"):
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
    config: _types.SepaDirectDebitConfig,
    pedantic: bool = True,
    print_progress_message=None,
) -> int:
    from . import _util

    if print_progress_message is None:
        print_progress_message = _util.print_progress_message

    dd = SepaDirectDebit(config)

    already_not_ok = len(df[df["payment_status"] != "ok"])

    idx: int
    df_len = len(df)
    written_payments = 0
    for i, (idx, row) in enumerate(df.iterrows()):  # type: ignore
        if row["payment_status"] != "ok":
            if row.get("open_amount_cents", 0) == 0:
                continue  # silently skip non-ok row with amount=0
            _LOGGER.debug(
                "[SDD] Skip non-ok row id=%s payment_status=%s payment_status_reason=%r",
                row["id"],
                row["payment_status"],
                row["payment_status_reason"],
            )
            continue

        skip_reasons = []
        if row.get("open_amount_cents", 0) == 0:
            skip_reasons.append("open_amount_cents = 0")
        if not row.get("payment_role", None):
            skip_reasons.append("payment_role IS NULL")
        if not row.get("sepa_iban", None):
            skip_reasons.append("sepa_iban IS NULL")

        if skip_reasons:
            skip_reason = ", ".join(skip_reasons)
            df.at[idx, "payment_status"] = "skipped"
            df.at[idx, "payment_status_reason"] = skip_reason
            _LOGGER.warning(
                "[SDD] Skip row id=%s payment_status_reason=%r", row["id"], skip_reason
            )
            continue

        progress_msg = (
            f"[SDD]"
            f" {row.get('id')} {row.get('short_full_name')}"
            f" sepa_name={row.get('sepa_name')!r}"
            f" {row.get('payment_role')}"
            f" open_amount_cents={row.get('open_amount_cents')}"
            f" {row.get('sepa_iban')}"
        )
        print_progress_message(i, df_len, progress_msg, logger=_LOGGER)
        try:
            dd.add_payment_from_accounting_row(row, pedantic=pedantic)
            written_payments += 1
        except (KeyError, ValueError) as exc:
            df.at[idx, "payment_status"] = "skipped"
            reason = f"{type(exc).__qualname__}: {exc}"
            df.at[idx, "payment_status_reason"] = reason
            _LOGGER.warning("[SDD] Caught exception: %s", reason)

    now_not_ok = len(df[df["payment_status"] != "ok"])
    _LOGGER.info("[SDD] Newly skipped rows: %s", now_not_ok - already_not_ok)
    _LOGGER.info("[SDD] Already not ok: %s", already_not_ok)
    _LOGGER.info("[SDD] Written payments: %s", written_payments)

    if dd.num_payments == 0:
        _LOGGER.warning("[SDD] No payments added to Direct Debit => No file written")
    else:
        _LOGGER.info("[SDD] Write %s", path)
        dd.export_file(path)

    return dd.num_payments
