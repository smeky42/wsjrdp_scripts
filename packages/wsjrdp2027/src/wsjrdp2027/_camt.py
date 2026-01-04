from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import functools as _functools
import itertools as _itertools
import typing as _typing

from . import _weakref_util
from ._iso20022 import element_or_list_to_list as _to_list


if _typing.TYPE_CHECKING:
    import io as _io

    from . import _file


@_dataclasses.dataclass(kw_only=True)
class CamtTransactionDetails:
    entry_details = _weakref_util.WeakrefAttr["CamtEntryDetails"]()

    _root: dict = _dataclasses.field(repr=False)

    __transaction_details_index: int = _dataclasses.field(default=0, repr=False)

    def __init__(
        self, *, TxDtls: dict, entry_details: CamtEntryDetails, index: int | None = None
    ) -> None:
        self.entry_details = entry_details
        self._root = TxDtls
        if index is not None:
            self.__transaction_details_index = index

    @property
    def entry(self) -> CamtEntry:
        return self.entry_details.entry

    @property
    def report(self) -> CamtReport:
        return self.entry.report

    @property
    def message(self) -> CamtMessage:
        return self.entry.report.message

    # ==================================================================================

    @property
    def camt_type(self) -> str:
        return self.message.camt_type

    @property
    def message_identification(self) -> str:
        return self.message.message_identification

    @property
    def message_creation_date_time(self) -> _datetime.datetime:
        return self.message.creation_date_time

    # ==================================================================================

    @property
    def report_identification(self) -> str:
        return self.report.identification

    @property
    def report_electronic_sequence_number(self) -> int | None:
        return self.report.electronic_sequence_number

    @property
    def report_legal_sequence_number(self) -> int | None:
        return self.report.legal_sequence_number

    @property
    def report_page_number(self) -> int | None:
        return self.report.page_number

    @property
    def report_creation_date_time(self) -> _datetime.datetime:
        return self.report.creation_date_time

    @property
    def account_identification(self) -> str:
        return self.report.account_identification

    # ==================================================================================

    @property
    def account_servicer_reference(self) -> str:
        return self.entry.account_servicer_reference

    @property
    def Ntry(self) -> dict:
        return self.entry.Ntry

    @property
    def value_date(self) -> _datetime.date:
        return self.entry.value_date

    @property
    def booking_date(self) -> _datetime.date:
        return self.entry.booking_date

    @property
    def credit_debit_indication(self) -> str:
        return self.entry.credit_debit_indication

    @property
    def is_credit(self) -> bool:
        return self.entry.is_credit

    @property
    def is_debit(self) -> bool:
        return self.entry.is_debit

    @property
    def status(self) -> str:
        return self.entry.status

    @property
    def additional_entry_info(self) -> str | None:
        return self.entry.additional_entry_info

    # ==================================================================================

    @property
    def TxDtls(self) -> dict:
        return self._root

    @property
    def Amt(self) -> dict:
        return self._root["Amt"]

    @property
    def BkTxCd(self) -> dict:
        return self._root["BkTxCd"]

    @property
    def RtrInf(self) -> dict:
        return self._root.get("RtrInf", {})

    @property
    def RltdPties(self) -> dict:
        return self._root.get("RltdPties", {})

    @property
    def RltdAgts(self) -> dict:
        return self._root.get("RltdAgts", {})

    @property
    def RmtInf(self) -> dict:
        return self._root.get("RmtInf", {})

    @property
    def transaction_details_index(self) -> int:
        return self.__transaction_details_index

    @property
    def bank_transaction_code(self) -> str:
        domn = self.BkTxCd.get("Domn")
        if domn:
            return "+".join([domn["Cd"], domn["Fmly"]["Cd"], domn["Fmly"]["SubFmlyCd"]])
        else:
            return ""

    @property
    def bank_transaction_code_dk(self) -> str:
        prtry = self.BkTxCd.get("Prtry", {})
        return prtry.get("Cd", "") if (prtry.get("Issr") == "DK") else ""

    @_functools.cached_property
    def amount_cents(self) -> int:
        return _amount_string_to_cents(
            self.Amt["amt"], currency=self.amount_currency, is_credit=self.is_credit
        )

    @_functools.cached_property
    def amount_currency(self) -> str:
        return self.Amt["Ccy"]

    @property
    def description(self) -> str:
        return "".join(rmt_inf["Ustrd"]) if (rmt_inf := self.RmtInf) else ""

    @_functools.cached_property
    def references(self) -> dict[str, str]:
        return {
            k: v for k, v in self.TxDtls.get("Refs", {}).items() if v != "NOTPROVIDED"
        }

    @property
    def mandate_id(self) -> str | None:
        return self.references.get("MndtId")

    @property
    def endtoend_id(self) -> str | None:
        return self.references.get("EndToEndId")

    @property
    def cdtr_name(self) -> str | None:
        return self.RltdPties.get("Cdtr", {}).get("Pty", {}).get("Nm")

    @property
    def cdtr_iban(self) -> str | None:
        return self.RltdPties.get("CdtrAcct", {}).get("Id", {}).get("IBAN")

    @property
    def cdtr_bic(self) -> str | None:
        return self.RltdAgts.get("CdtrAgt", {}).get("FinInstnId", {}).get("BICFI")

    @property
    def cdtr_address(self) -> str | None:
        return None

    @property
    def dbtr_name(self) -> str | None:
        return self.RltdPties.get("Dbtr", {}).get("Pty", {}).get("Nm")

    @property
    def dbtr_iban(self) -> str | None:
        return self.RltdPties.get("DbtrAcct", {}).get("Id", {}).get("IBAN")

    @property
    def dbtr_bic(self) -> str | None:
        return self.RltdAgts.get("DbtrAgt", {}).get("FinInstnId", {}).get("BICFI")

    @property
    def dbtr_address(self) -> str | None:
        return None

    @property
    def return_reason(self) -> str | None:
        return self.RtrInf.get("Rsn", {}).get("Cd")


@_dataclasses.dataclass(kw_only=True)
class CamtEntryDetails:
    entry = _weakref_util.WeakrefAttr["CamtEntry"]()
    NtryDtls: dict = _dataclasses.field(repr=False)
    __TxDtls: list[dict] = _dataclasses.field(repr=False)

    def __init__(self, *, NtryDtls: dict, entry: CamtEntry) -> None:
        self.entry = entry
        self.NtryDtls = NtryDtls
        self.__TxDtls = _to_list(self.NtryDtls.get("TxDtls"))

    @property
    def report(self) -> CamtReport:
        return self.entry.report

    @property
    def message(self) -> CamtMessage:
        return self.entry.report.message

    @_functools.cached_property
    def transaction_details(self) -> list[CamtTransactionDetails]:
        counter = self.entry._index_count
        return [
            CamtTransactionDetails(TxDtls=d, entry_details=self, index=next(counter))
            for d in self.__TxDtls
        ]


@_dataclasses.dataclass(kw_only=True)
class CamtEntry:
    report = _weakref_util.WeakrefAttr["CamtReport"]()

    _root: dict = _dataclasses.field(repr=False)
    _index_count: _itertools.count = _dataclasses.field(repr=False)

    def __init__(self, *, Ntry: dict, report: CamtReport) -> None:
        self.report = report
        self._root = Ntry
        self._index_count = _itertools.count(start=0)

    @property
    def Ntry(self) -> dict:
        return self._root

    @property
    def Amt(self) -> dict:
        return self._root["Amt"]

    @property
    def NtryDtls(self) -> list[dict]:
        dtls = self._root["NtryDtls"]
        return [dtls] if isinstance(dtls, dict) else dtls

    @property
    def BkTxCd(self) -> dict:
        return self._root.get("BkTxCd", {})

    @property
    def message(self) -> CamtMessage:
        return self.report.message

    @property
    def camt_type(self) -> str:
        return self.message.camt_type

    @_functools.cached_property
    def credit_debit_indication(self) -> str:
        return self._root["CdtDbtInd"]

    @property
    def is_credit(self) -> bool:
        return self._root["CdtDbtInd"] == "CRDT"

    @property
    def is_debit(self) -> bool:
        return self._root["CdtDbtInd"] == "DBIT"

    @property
    def bank_transaction_code(self) -> str:
        domn = self.BkTxCd.get("Domn")
        if domn:
            return "+".join([domn["Cd"], domn["Fmly"]["Cd"], domn["Fmly"]["SubFmlyCd"]])
        else:
            return ""

    @property
    def bank_transaction_code_dk(self) -> str:
        prtry = self.BkTxCd.get("Prtry", {})
        return prtry.get("Cd", "") if (prtry.get("Issr") == "DK") else ""

    @_functools.cached_property
    def amount_cents(self) -> int:
        return _amount_string_to_cents(
            self.Amt["amt"], currency=self.amount_currency, is_credit=self.is_credit
        )

    @_functools.cached_property
    def amount_currency(self) -> str:
        return self.Amt["Ccy"]

    @_functools.cached_property
    def entry_details(self) -> list[CamtEntryDetails]:
        return [
            CamtEntryDetails(NtryDtls=d, entry=self)
            for d in _to_list(self._root.get("NtryDtls", []))
        ]

    @_functools.cached_property
    def transaction_details(self) -> list[CamtTransactionDetails]:
        tx_dtls = []
        for ntry_dtls in self.entry_details:
            tx_dtls.extend(ntry_dtls.transaction_details)
        return tx_dtls

    @property
    def _tx_dtls(self) -> CamtTransactionDetails | None:
        for ntry_dtls in self.entry_details:
            for tx_dtls in ntry_dtls.transaction_details:
                return tx_dtls
        return None

    @property
    def is_booked(self) -> bool:
        return self._root.get("Sts", {}).get("Cd", None) == "BOOK"

    @property
    def description(self) -> str:
        return tx_dtls.description if (tx_dtls := self._tx_dtls) else ""

    @property
    def references(self) -> dict[str, str]:
        return tx_dtls.references if (tx_dtls := self._tx_dtls) else {}

    @property
    def value_date(self) -> _datetime.date:
        from . import _util

        return _util.to_date(self.Ntry["ValDt"]["Dt"])

    @property
    def booking_date(self) -> _datetime.date:
        from . import _util

        return _util.to_date(self.Ntry["BookgDt"]["Dt"])

    @property
    def additional_entry_info(self) -> str | None:
        return self._root.get("AddtlNtryInf")

    @property
    def account_servicer_reference(self) -> str:
        return self._root.get("AcctSvcrRef", "")

    @property
    def status(self) -> str:
        return self._root.get("Sts", {}).get("Cd", "INFO")

    @property
    def mandate_id(self) -> str | None:
        return self.references.get("MndtId")

    @property
    def endtoend_id(self) -> str | None:
        return self.references.get("EndToEndId")

    @property
    def cdtr_name(self) -> str | None:
        return tx_dtls.cdtr_name if (tx_dtls := self._tx_dtls) else None

    @property
    def cdtr_iban(self) -> str | None:
        return tx_dtls.cdtr_iban if (tx_dtls := self._tx_dtls) else None

    @property
    def cdtr_bic(self) -> str | None:
        return tx_dtls.cdtr_bic if (tx_dtls := self._tx_dtls) else None

    @property
    def cdtr_address(self) -> str | None:
        return tx_dtls.cdtr_address if (tx_dtls := self._tx_dtls) else None

    @property
    def dbtr_name(self) -> str | None:
        return tx_dtls.dbtr_name if (tx_dtls := self._tx_dtls) else None

    @property
    def dbtr_iban(self) -> str | None:
        return tx_dtls.dbtr_iban if (tx_dtls := self._tx_dtls) else None

    @property
    def dbtr_bic(self) -> str | None:
        return tx_dtls.dbtr_bic if (tx_dtls := self._tx_dtls) else None

    @property
    def dbtr_address(self) -> str | None:
        return tx_dtls.dbtr_address if (tx_dtls := self._tx_dtls) else None


@_dataclasses.dataclass(kw_only=True)
class CamtReport:
    _CAMT_TYPE_TO_PGNTN_TAG = {
        "camt.052": "RptPgntn",
        "camt.053": "StmtPgntn",
        "camt.054": "NtfctnPgntn",
    }

    message = _weakref_util.WeakrefAttr["CamtMessage"]()
    _root: dict = _dataclasses.field(repr=False)

    def __init__(self, *, Rpt: dict, message: CamtMessage) -> None:
        self._root = Rpt
        self.message = message

    @property
    def camt_type(self) -> str:
        return self.message.camt_type

    @property
    def Rpt(self) -> dict:
        return self._root

    @property
    def Pgntn(self) -> dict:
        pgntn_tag = self._CAMT_TYPE_TO_PGNTN_TAG[self.camt_type]
        return self._root.get(pgntn_tag, {})

    @property
    def Acct(self) -> dict:
        return self._root.get("Acct", {})

    @property
    def identification(self) -> str:
        return self._root["Id"]

    @property
    def page_number(self) -> int | None:
        return int(pg_nb, base=10) if (pg_nb := self.Pgntn.get("PgNb")) else None

    @property
    def creation_date_time(self) -> _datetime.datetime:
        return _datetime.datetime.fromisoformat(self.Rpt["CreDtTm"])

    @property
    def electronic_sequence_number(self) -> int | None:
        return (
            int(seq_nb, base=10) if (seq_nb := self.Rpt.get("ElctrncSeqNb")) else None
        )

    @property
    def legal_sequence_number(self) -> int | None:
        return int(seq_nb, base=10) if (seq_nb := self.Rpt.get("LglSeqNb")) else None

    @property
    def account_identification(self) -> str:
        return self.Acct["Id"]["IBAN"]

    @_functools.cached_property
    def entries(self) -> list[CamtEntry]:
        return [
            CamtEntry(Ntry=d, report=self) for d in _to_list(self.Rpt.get("Ntry", []))
        ]

    @property
    def booked_entries(self) -> _typing.Iterator[CamtEntry]:
        for ntry in self.entries:
            if ntry.is_booked:
                yield ntry

    @property
    def transaction_details(self) -> _typing.Iterator[CamtTransactionDetails]:
        for ntry in self.entries:
            yield from ntry.transaction_details

    @property
    def booked_transaction_details(self) -> _typing.Iterator[CamtTransactionDetails]:
        for ntry in self.booked_entries:
            yield from ntry.transaction_details


@_dataclasses.dataclass(kw_only=True)
class CamtMessage:
    _CAMT_TYPE_TO_ROOT_TAG = {
        "camt.052": "BkToCstmrAcctRpt",
        "camt.053": "BkToCstmrStmt",
        "camt.054": "BkToCstmrDbtCdtNtfctn",
    }
    _CAMT_TYPE_TO_STATEMENT_TAG = {
        "camt.052": "Rpt",
        "camt.053": "Stmt",
        "camt.054": "Ntfctn",
    }

    sepa_schema: str
    Document: dict = _dataclasses.field(repr=False)
    _root: dict = _dataclasses.field(repr=False)

    def __init__(self, *, Document: dict, sepa_schema: str) -> None:
        self.Document = Document
        self.sepa_schema = sepa_schema
        self.__camt_type = ".".join(sepa_schema.split(".")[:2])
        if self.__camt_type not in ("camt.052", "camt.053", "camt.054"):
            raise ValueError(f"Unsupported camt_type={self.__camt_type!r}")
        self._root = self.Document[self._CAMT_TYPE_TO_ROOT_TAG[self.__camt_type]]

    @classmethod
    def load(cls, file_or_path: _file.PathLike | _io.Reader[bytes], /) -> _typing.Self:
        from . import _iso20022

        d = _iso20022.iso20022_xml_file_to_dict(
            file_or_path, expected_format="camt.052"
        )
        return cls(sepa_schema=d["sepa_schema"], Document=d["Document"])

    @classmethod
    def loads(cls, content: str | bytes, /) -> _typing.Self:
        from . import _iso20022

        d = _iso20022.iso20022_xml_text_to_dict(content, expected_format="camt.052")
        return cls(sepa_schema=d["sepa_schema"], Document=d["Document"])

    @property
    def GrpHdr(self) -> dict:
        return self._root["GrpHdr"]

    @property
    def message_identification(self) -> str:
        return self.GrpHdr["MsgId"]

    @property
    def creation_date_time(self) -> _datetime.datetime:
        return _datetime.datetime.fromisoformat(self.GrpHdr["CreDtTm"])

    @property
    def camt_type(self) -> str:
        return self.__camt_type

    @property
    def is_camt052(self) -> bool:
        return self.__camt_type == "camt.052"

    @property
    def is_camt053(self) -> bool:
        return self.__camt_type == "camt.053"

    @property
    def is_camt054(self) -> bool:
        return self.__camt_type == "camt.054"

    @_functools.cached_property
    def reports(self) -> list[CamtReport]:
        stmt_tag = self._CAMT_TYPE_TO_STATEMENT_TAG[self.__camt_type]
        return [CamtReport(Rpt=d, message=self) for d in _to_list(self._root[stmt_tag])]

    @property
    def entries(self) -> _typing.Iterator[CamtEntry]:
        for rpt in self.reports:
            yield from rpt.entries

    @property
    def booked_entries(self) -> _typing.Iterator[CamtEntry]:
        for rpt in self.reports:
            yield from rpt.booked_entries

    @property
    def transaction_details(self) -> _typing.Iterator[CamtTransactionDetails]:
        for ntry in self.entries:
            yield from ntry.transaction_details

    @property
    def booked_transaction_details(self) -> _typing.Iterator[CamtTransactionDetails]:
        for ntry in self.booked_entries:
            yield from ntry.transaction_details


def _amount_string_to_cents(amount: str, currency: str, is_credit: bool) -> int:
    import iso4217

    cur = iso4217.Currency(currency)
    exponent = cur.exponent or 0
    cents = int(round(float(amount) * (10**exponent)))
    return cents if is_credit else -cents
