from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import functools as _functools
import typing as _typing

from . import _weakref_util


if _typing.TYPE_CHECKING:
    import io as _io

    from . import _file


@_dataclasses.dataclass(kw_only=True)
class PainDirectDebitTxInf:
    # Direct Debit Transaction Information

    payment_info = _weakref_util.WeakrefAttr["PainPaymentInformation"]()
    _root: dict = _dataclasses.field(repr=False)

    def __init__(
        self, *, DrctDbtTxInf: dict, payment_info: PainPaymentInformation
    ) -> None:
        self._root = DrctDbtTxInf
        self.payment_info = payment_info

    @property
    def _Amt(self) -> dict:
        return self._root["InstdAmt"]

    @property
    def message(self) -> PainMessage:
        return self.payment_info.message

    @property
    def endtoend_id(self) -> str:
        return self._root["PmtId"]["EndToEndId"]

    @_functools.cached_property
    def amount_cents(self) -> int:
        from . import _iso20022

        return _iso20022.amount_string_to_cents(
            self._Amt["amt"], currency=self.amount_currency, is_credit=True
        )

    @_functools.cached_property
    def amount_currency(self) -> str:
        return self._Amt["Ccy"]

    @property
    def description(self) -> str:
        return self._root["RmtInf"]["Ustrd"]


@_dataclasses.dataclass(kw_only=True)
class PainPaymentInformation:
    message = _weakref_util.WeakrefAttr["PainMessage"]()
    _root: dict = _dataclasses.field(repr=False)

    def __init__(self, *, PmtInf: dict, message: PainMessage) -> None:
        self._root = PmtInf
        self.message = message

    @property
    def PmtInf(self) -> dict:
        return self._root

    @property
    def PmtTpInf(self) -> dict:
        return self._root["PmtTpInf"]

    @property
    def payment_information_identification(self) -> str:
        return self.PmtInf.get("PmtInfId", "")

    @property
    def batch_booking(self) -> bool:
        return self.PmtInf.get("BtchBookg", "true") == "true"

    @property
    def number_of_transactions(self) -> int:
        return int(self.PmtInf["NbOfTxs"], base=10)

    @property
    def control_sum_string(self) -> str:
        return self.PmtInf["CtrlSum"]

    @property
    def control_sum_cents(self) -> int:
        return int(round(float(self.control_sum_string) * 100))

    @property
    def payment_type_instrument(self) -> str:
        return self.PmtTpInf["LclInstrm"]["Cd"]

    @property
    def debit_sequence_type(self) -> str:
        return self.PmtTpInf["SeqTp"]

    @property
    def requested_collection_date(self) -> _datetime.date | None:
        from . import _util

        return _util.to_date_or_none(self.PmtInf.get("ReqdColltnDt"))

    @property
    def cdtr_name(self) -> str:
        return self._root["Cdtr"]["Nm"]

    @property
    def cdtr_iban(self) -> str:
        return self._root["CdtrAcct"]["Id"]["IBAN"]

    @property
    def cdtr_bic(self) -> str | None:
        fin_inst = self._root["CdtrAgt"]["FinInstnId"]
        for key in ("BIC", "BICFI"):
            if (val := fin_inst.get(key)) is not None:
                return val
        return None

    @property
    def cdtr_address(self) -> str | None:
        return None

    @property
    def creditor_id(self) -> str | None:
        return (
            self.PmtInf.get("CdtrSchmeId", {})
            .get("Id", {})
            .get("PrvtId", {})
            .get("Othr", {})
            .get("Id")
        )

    @_functools.cached_property
    def direct_debit_tx_infs(self) -> list[PainDirectDebitTxInf]:
        from . import _iso20022

        txs = _iso20022.element_or_list_to_list(self._root.get("DrctDbtTxInf", []))
        return [PainDirectDebitTxInf(DrctDbtTxInf=d, payment_info=self) for d in txs]


@_dataclasses.dataclass(kw_only=True)
class PainMessage:
    _MESSAGE_TYPE_TO_ROOT_TAG = {
        "pain.008": "CstmrDrctDbtInitn",
    }

    sepa_schema: str
    Document: dict = _dataclasses.field(repr=False)
    _root: dict = _dataclasses.field(repr=False)

    def __init__(self, *, Document: dict, sepa_schema: str) -> None:
        self.Document = Document
        self.sepa_schema = sepa_schema
        self.__message_type = ".".join(sepa_schema.split(".")[:2])
        if self.__message_type not in ("pain.008",):
            raise ValueError(f"Unsupported camt_type={self.__message_type!r}")
        self._root = self.Document[self._MESSAGE_TYPE_TO_ROOT_TAG[self.__message_type]]

    @classmethod
    def load(cls, file_or_path: _file.PathLike | _io.Reader[bytes], /) -> _typing.Self:
        from . import _iso20022

        d = _iso20022.iso20022_xml_file_to_dict(
            file_or_path, expected_format="pain.008"
        )
        return cls(sepa_schema=d["sepa_schema"], Document=d["Document"])

    @classmethod
    def loads(cls, content: str | bytes, /) -> _typing.Self:
        from . import _iso20022

        d = _iso20022.iso20022_xml_text_to_dict(content, expected_format="pain.008")
        return cls(sepa_schema=d["sepa_schema"], Document=d["Document"])

    @property
    def GrpHdr(self) -> dict:
        return self._root["GrpHdr"]

    @property
    def InitgPty(self) -> dict:
        return self.GrpHdr.get("InitgPty", {})

    @property
    def message_identification(self) -> str:
        return self.GrpHdr["MsgId"]

    @property
    def creation_date_time(self) -> _datetime.datetime:
        return _datetime.datetime.fromisoformat(self.GrpHdr["CreDtTm"])

    @property
    def number_of_transactions(self) -> int:
        return int(self.GrpHdr["NbOfTxs"], base=10)

    @property
    def control_sum_string(self) -> str:
        return self.GrpHdr["CtrlSum"]

    @property
    def control_sum_cents(self) -> int:
        return int(round(float(self.control_sum_string) * 100))

    @property
    def initiating_party_name(self) -> str:
        return self.InitgPty["Nm"]

    @_functools.cached_property
    def payment_infos(self) -> list[PainPaymentInformation]:
        from . import _iso20022

        pmt_infs = _iso20022.element_or_list_to_list(self._root["PmtInf"])
        return [PainPaymentInformation(PmtInf=d, message=self) for d in pmt_infs]
