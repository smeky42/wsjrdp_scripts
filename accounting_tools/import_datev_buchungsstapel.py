#!/usr/bin/env -S uv run
"""Import DATEV DTVF "Buchungsstapel" (Primanota) CSV files into the database.

Reads the CP1252/CRLF DTVF export files (DTVF_Buchungsstapel_*.csv, incl. the
fake 2025 files produced by accounting_tools/fake_datev_buchungsstapel.py) and
writes them to two tables:

  * datev_booking_batches -- one row per Buchungsstapel (= one Primanota = one
    file). Identified by the STABLE header coordinates
    (Berater, Mandant, Datum von, Datum bis, Bezeichnung); found-or-created
    (UPSERT) per file, so a re-export of the same Stapel updates the existing
    batch instead of creating a duplicate. See "Woran erkenne ich den Stapel?"
    below.
  * datev_bookings -- one row per booking, keyed on the DATEV "Buchungs GUID"
    (field 103, unique + NOT NULL): a booking whose GUID already exists is
    UPDATEd (all DATEV-derived fields refreshed, the hand-editable
    `posting_text` and `secondary_cost_center_number` preserved); a new GUID
    INSERTs a new row. The booking<->entry and booking<->camt links live on the
    OTHER side (accounting_entries.datev_booking_id /
    wsjrdp_camt_transactions.datev_booking_id) and are (re)established by the
    auto-linking below, not by the upsert.

Like the master-data importers (import_cost_centers.py & co) this is a
plan/apply CLI on SingleTableUpsertPlanBuilder: the stored rows are loaded
read-only and diffed per row AND per column, a plan preview is logged BEFORE
anything is written (and before the production approval), a re-run of the same
files touches nothing, and created_at/updated_at follow the import convention
(created_at only on INSERT, updated_at only on a genuine UPDATE -- a fresh
import leaves updated_at NULL, auto-linking included). --dry-run shows the
plan(s) without writing; --rollback-for-testing applies and rolls back.

The import REFUSES to run (before touching the database) when

  * a Buchungsstapel's currency (DTVF header field 22) is not EUR, or
  * any booking's BASE currency would not be EUR (a foreign-currency Umsatz
    without Basis-Umsatz): the accounting keeps all base figures in EUR and
    the generated amount columns rely on that.

Woran der Buchungsstapel eindeutig zu erkennen ist
--------------------------------------------------
Der Stapel wird identifiziert durch das Tupel aus dem DTVF-Header:
    (Berater [11], Mandant [12], 'Datum von' [15], 'Datum bis' [16], Bezeichnung [17]).
Diese Felder sind über wiederholte Exporte desselben Stapels stabil. NICHT
Teil der Identität sind die rekonstruierte Primanota-/Stapelnummer und die
_NNNNN-Dateisequenz -- beide hängen vom Export als Ganzem ab, nicht vom Stapel.

Field mapping and the 2025->2026 account remap reuse the Primanota Excel
importer (import_datev_primanota). Belegdatum is TTMM (no year) -- the year comes
from the header WJ-Beginn. Any populated DTVF record column without a dedicated
DB column (raw Beleglink, BU-Schlüssel, ...) is captured in the JSONB column
other_datev_columns, so
no exported information is silently dropped.

Usage:
  ./accounting_tools/import_datev_buchungsstapel.py \
      External_Data/DATEV_FY2026_Export_20260821/DTVF_Buchungsstapel_*.csv
  WSJRDP_SCRIPTS_CONFIG=config-prod.yml uv run accounting_tools/import_datev_buchungsstapel.py ...   # PROD (nur auf Wunsch)
"""

from __future__ import annotations

import csv as _csv
import datetime as _datetime
import decimal as _decimal
import logging as _logging
import pathlib as _pathlib
import re as _re
import sys as _sys
import typing as _typing
import uuid as _uuid

import wsjrdp2027
from wsjrdp2027._internal.single_table_upsert_plan import (
    SingleTableUpsertPlanBuilder,
)


# Reuse the account map + description derivation from the Excel importer (same
# directory). Its module top level is import-safe (openpyxl is imported lazily
# inside functions and main() is guarded).
_sys.path.insert(0, str(_pathlib.Path(__file__).resolve().parent))
from import_datev_primanota import (  # noqa: E402
    ACCOUNT_MAP_2025_TO_2026 as _ACCOUNT_MAP,
    _derive_description,
)


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc

_SELF_NAME = _pathlib.Path(__file__).stem
_LOGGER = _logging.getLogger(__name__)

_BOOKINGS_TABLE = "datev_bookings"
_BATCHES_TABLE = "datev_booking_batches"

# _NNNNN sequence in the export file name, e.g. DTVF_Buchungsstapel_20260821_110220_00003.csv
_RE_FILE_SEQ = _re.compile(r"_(\d{5})\.csv$", _re.IGNORECASE)

# --- DTVF record columns we map to a dedicated datev_bookings column. Every
# OTHER populated column is captured in other_datev_columns. ---
_COL_UMSATZ = "Umsatz (ohne Soll/Haben-Kz)"
_COL_SH = "Soll/Haben-Kennzeichen"
_COL_WKZ = "WKZ Umsatz"
_COL_KURS = "Kurs"
_COL_BASIS_UMSATZ = "Basis-Umsatz"
_COL_WKZ_BASIS = "WKZ Basis-Umsatz"
_COL_KONTO = "Konto"
_COL_GEGENKONTO = "Gegenkonto (ohne BU-Schlüssel)"
_COL_BELEGDATUM = "Belegdatum"
_COL_BELEGFELD1 = "Belegfeld 1"
_COL_BELEGFELD2 = "Belegfeld 2"
_COL_BUCHUNGSTEXT = "Buchungstext"
_COL_KOST1 = "KOST1 - Kostenstelle"
_COL_KOST2 = "KOST2 - Kostenstelle"
_COL_HERKUNFT = "Herkunft-Kz"
_COL_FESTSCHREIBUNG = "Festschreibung"
_COL_GENERALUMKEHR = "Generalumkehr (GU)"
_COL_GUID = "Buchungs GUID"
_COL_LEISTUNGSDATUM = "Leistungsdatum"
_COL_BELEGLINK = "Beleglink"

# Beleginfo (Felder 21-36) and Zusatzinformation (Felder 48-87) come as numbered
# Art/Inhalt slot pairs. DATEV's own header spelling is inconsistent (e.g.
# "Beleginfo - Inhalt 1" has spaces, "Zusatzinformation- Inhalt 1" has none), so
# the slots are matched by a tolerant regex against the file's real headers
# rather than by fixed names.
_RE_BELEGINFO_ART = _re.compile(r"^Beleginfo\s*-\s*Art\s+(\d+)$")
_RE_BELEGINFO_INHALT = _re.compile(r"^Beleginfo\s*-\s*Inhalt\s+(\d+)$")
_RE_ZUSATZINFO_ART = _re.compile(r"^Zusatzinformation\s*-\s*Art\s+(\d+)$")
_RE_ZUSATZINFO_INHALT = _re.compile(r"^Zusatzinformation\s*-\s*Inhalt\s+(\d+)$")
# Belegbild reference inside the Beleglink field: BEDI "<uuid>".
_RE_BEDI_GUID = _re.compile(
    r'BEDI\s+"?\{?'
    r"([0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})"
    r'\}?"?'
)

_MAPPED_COLUMNS = frozenset(
    {
        _COL_UMSATZ,
        _COL_SH,
        _COL_WKZ,
        _COL_KURS,
        _COL_BASIS_UMSATZ,
        _COL_WKZ_BASIS,
        _COL_KONTO,
        _COL_GEGENKONTO,
        _COL_BELEGDATUM,
        _COL_BELEGFELD1,
        _COL_BELEGFELD2,
        _COL_BUCHUNGSTEXT,
        _COL_KOST1,
        _COL_KOST2,
        _COL_HERKUNFT,
        _COL_FESTSCHREIBUNG,
        _COL_GENERALUMKEHR,
        _COL_GUID,
        _COL_LEISTUNGSDATUM,
    }
)

# The datev_bookings plan covers exactly the columns present in the value sets
# from _iter_bookings; every DATEV-derived column is diffed and refreshed on a
# re-import. Preserved implicitly: secondary_cost_center_number and
# additional_info are never part of the values, and the hand-editable
# `posting_text` is split off and written on INSERT only (see
# _build_bookings_plan). The booking<->entry / booking<->camt links are not
# booking columns any more -- they live on accounting_entries /
# wsjrdp_camt_transactions and are (re)set by the auto-linking step.

# The datev_booking_batches identity (the composite plan key; see the module
# docstring). The remaining batch columns are simply the non-identity keys of
# _batch_values -- diffed/refreshed on re-import. Exception: `source_file` is
# provenance metadata, kept OUT of the diffed values and only written when a
# batch is inserted/updated for other reasons (see _add_source_file).
_BATCH_IDENTITY = [
    "consultant_number",
    "client_number",
    "period_from",
    "period_to",
    "label",
]


def _cell(value: str | None) -> str | None:
    """Trim a CSV cell; empty -> None."""
    if value is None:
        return None
    s = value.strip()
    return s or None


def _parse_yyyymmdd(value: str | None) -> _datetime.date | None:
    s = _cell(value)
    if not s or len(s) != 8 or not s.isdigit():
        return None
    return _datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def _parse_erzeugt_am(value: str | None) -> _datetime.datetime | None:
    """Header 'Erzeugt am', YYYYMMDDHHMMSSFFF (17 digits). Stored, not relied on."""
    s = _cell(value)
    if not s or len(s) < 14 or not s[:14].isdigit():
        return None
    return _datetime.datetime(  # noqa: DTZ001 -- naive, like DATEV's own timestamps
        int(s[0:4]),
        int(s[4:6]),
        int(s[6:8]),
        int(s[8:10]),
        int(s[10:12]),
        int(s[12:14]),
    )


def _parse_belegdatum(value: str | None, *, year: int) -> _datetime.date | None:
    """DTVF Belegdatum is TTMM (day, month); the year comes from the header
    (WJ-Beginn / fiscal_year -- our fiscal year is the calendar year)."""
    s = _cell(value)
    if not s or len(s) != 4 or not s.isdigit():
        return None
    return _datetime.date(year, int(s[2:4]), int(s[0:2]))


def _parse_ttmmjjjj(value: str | None) -> _datetime.date | None:
    """DTVF Leistungsdatum / KOST-Datum, TTMMJJJJ."""
    s = _cell(value)
    if not s or len(s) != 8 or not s.isdigit():
        return None
    return _datetime.date(int(s[4:8]), int(s[2:4]), int(s[0:2]))


def _parse_amount_de(value: str | None) -> _decimal.Decimal | None:
    """German decimal '1234,56' -> Decimal('1234.56')."""
    s = _cell(value)
    if not s:
        return None
    return _decimal.Decimal(s.replace(".", "").replace(",", "."))


def _extract_bedi_guid(beleglink: str | None) -> _uuid.UUID | None:
    """Pull the Belegbild GUID out of a Beleglink cell (BEDI "<uuid>").

    Returned as uuid.UUID: the bedi_guid column is of type uuid (DATEV's
    upper-case spelling is normalized; the verbatim Beleglink stays in
    other_datev_columns), and a typed value keeps the plan diff comparing
    like with like."""
    if not beleglink:
        return None
    m = _RE_BEDI_GUID.search(beleglink)
    return _uuid.UUID(m.group(1)) if m else None


def _build_info_slots(
    column_names: list[str],
    row: _typing.Sequence[str],
    art_re: _re.Pattern[str],
    inhalt_re: _re.Pattern[str],
) -> list[dict[str, object]]:
    """Collect numbered Art/Inhalt slot pairs into [{num, key, value}], keeping
    the DATEV slot number and order and skipping wholly empty slots (gaps are
    allowed). `key` is the Art, `value` the Inhalt."""
    arts: dict[int, str] = {}
    inhalts: dict[int, str] = {}
    for name, raw in zip(column_names, row):
        m = art_re.match(name)
        if m:
            arts[int(m.group(1))] = (raw or "").strip()
            continue
        m = inhalt_re.match(name)
        if m:
            inhalts[int(m.group(1))] = (raw or "").strip()
    slots: list[dict[str, object]] = []
    for num in sorted(arts.keys() | inhalts.keys()):
        key = arts.get(num, "")
        value = inhalts.get(num, "")
        if not key and not value:
            continue
        slots.append({"num": num, "key": key, "value": value})
    return slots


def _info_value(slots: list[dict[str, object]], key: str) -> str | None:
    """Value (Inhalt) of the first slot whose key (Art) matches, or None."""
    for slot in slots:
        if slot.get("key") == key:
            value = slot.get("value")
            return value if isinstance(value, str) and value else None
    return None


def _is_info_column(name: str) -> bool:
    """True for Beleginfo / Zusatzinformation slot columns (they get their own
    typed columns, so they must NOT also land in other_datev_columns)."""
    return bool(
        _RE_BELEGINFO_ART.match(name)
        or _RE_BELEGINFO_INHALT.match(name)
        or _RE_ZUSATZINFO_ART.match(name)
        or _RE_ZUSATZINFO_INHALT.match(name)
    )


class _BatchHeader(_typing.NamedTuple):
    consultant_number: str
    client_number: str
    period_from: _datetime.date
    period_to: _datetime.date
    label: str
    fiscal_year: int | None
    fiscal_year_start: _datetime.date | None
    origin_indicator: str | None
    festschreibung: bool
    booking_type: int | None
    account_number_length: int | None
    chart_of_accounts: str | None
    currency: str
    datev_created_at: _datetime.datetime | None
    source_file: str
    file_sequence: int | None
    header_raw: dict[str, str]


def _parse_header(fields: list[str], *, source_file: str) -> _BatchHeader:
    """Parse the DTVF metadata header line (fields already CSV-unquoted)."""

    def f(i: int) -> str | None:
        return _cell(fields[i]) if i < len(fields) else None

    kennzeichen = f(0)
    if kennzeichen not in ("DTVF", "EXTF"):
        raise ValueError(
            f"{source_file}: not a DTVF/EXTF header (field 1 = {kennzeichen!r})"
        )
    formatname = f(3)
    if formatname != "Buchungsstapel":
        raise ValueError(
            f"{source_file}: not a Buchungsstapel (field 4 = {formatname!r})"
        )

    period_from = _parse_yyyymmdd(f(14))
    period_to = _parse_yyyymmdd(f(15))
    label = f(16)
    wj_beginn = _parse_yyyymmdd(f(12))
    consultant = f(10)
    client = f(11)
    if not (consultant and client and period_from and period_to and label):
        raise ValueError(
            f"{source_file}: incomplete Stapel identity in header "
            f"(Berater={consultant!r} Mandant={client!r} von={period_from} bis={period_to} Bez={label!r})"
        )
    m = _RE_FILE_SEQ.search(_pathlib.Path(source_file).name)
    buchungstyp, sachkontenlaenge = f(18), f(13)
    return _BatchHeader(
        consultant_number=consultant,
        client_number=client,
        period_from=period_from,
        period_to=period_to,
        label=label,
        fiscal_year=wj_beginn.year if wj_beginn else None,
        fiscal_year_start=wj_beginn,
        origin_indicator=f(7),
        festschreibung=(f(20) == "1"),
        booking_type=int(buchungstyp)
        if (buchungstyp and buchungstyp.isdigit())
        else None,
        account_number_length=int(sachkontenlaenge)
        if (sachkontenlaenge and sachkontenlaenge.isdigit())
        else None,
        chart_of_accounts=f(26),
        currency=f(21) or "EUR",
        datev_created_at=_parse_erzeugt_am(f(5)),
        source_file=_pathlib.Path(source_file).name,
        file_sequence=int(m.group(1)) if m else None,
        header_raw={str(i + 1): v for i, v in enumerate(fields)},
    )


def _read_dtvf(path: _pathlib.Path) -> tuple[list[str], list[str], list[list[str]]]:
    with open(path, encoding="cp1252", newline="") as fh:
        rows = list(_csv.reader(fh, delimiter=";"))
    if len(rows) < 2:
        raise ValueError(f"{path}: too few lines for a Buchungsstapel")
    header_fields = rows[0]
    column_names = [c.strip() for c in rows[1]]
    data_rows = [r for r in rows[2:] if any(c.strip() for c in r)]
    return header_fields, column_names, data_rows


def _reconstruct_primanota_numbers(headers: list[_BatchHeader]) -> dict[str, str]:
    """Stapelnummer 'MM-YYYY/NNNN' per DATEV's documented rule: month/year from
    'Datum bis', running number = position among the export's files (sorted by
    the _NNNNN sequence) within that month. Best-effort Anzeigewert; keyed by
    source_file. See datev_import.md 5.2 (10/10 verified on the real export)."""
    ordered = sorted(
        headers,
        key=lambda h: (
            h.file_sequence if h.file_sequence is not None else 1 << 30,
            h.source_file,
        ),
    )
    counter: dict[tuple[int, int], int] = {}
    numbers: dict[str, str] = {}
    for h in ordered:
        key = (h.period_to.year, h.period_to.month)
        counter[key] = counter.get(key, 0) + 1
        numbers[h.source_file] = (
            f"{h.period_to.month:02d}-{h.period_to.year}/{counter[key]:04d}"
        )
    return numbers


def _iter_bookings(
    *,
    column_names: list[str],
    data_rows: list[list[str]],
    header: _BatchHeader,
) -> _collections_abc.Iterator[dict[str, object]]:
    idx = {name: i for i, name in enumerate(column_names)}
    year = header.fiscal_year

    def col(row: list[str], name: str) -> str | None:
        i = idx.get(name)
        return _cell(row[i]) if (i is not None and i < len(row)) else None

    for ordinal, row in enumerate(data_rows, start=1):
        raw_guid = col(row, _COL_GUID)
        if not raw_guid:
            raise ValueError(
                f"{header.source_file}: row {ordinal} has no Buchungs GUID "
                "(required; the buchungs_guid column is NOT NULL)"
            )
        try:
            # The column is of type uuid; keep the value typed so the plan
            # diff compares like with like (a str key would never match the
            # UUID the database returns).
            guid = _uuid.UUID(raw_guid)
        except ValueError:
            raise ValueError(
                f"{header.source_file}: row {ordinal} has a malformed "
                f"Buchungs GUID {raw_guid!r}"
            ) from None
        konto = col(row, _COL_KONTO)
        gegenkonto = col(row, _COL_GEGENKONTO)
        original_account_number = konto
        original_offsetting_account_number = gegenkonto
        if year is not None and year >= 2026:
            account_number = konto
            offsetting_account_number = gegenkonto
        else:
            account_number = _ACCOUNT_MAP.get(konto) if konto else None
            offsetting_account_number = (
                _ACCOUNT_MAP.get(gegenkonto) if gegenkonto else None
            )

        account_kind = wsjrdp2027.datev.account_kind_for_account_number(account_number)
        offsetting_account_kind = wsjrdp2027.datev.account_kind_for_account_number(
            offsetting_account_number
        )

        sh = col(row, _COL_SH)
        debit_credit = "C" if (sh and sh.upper() == "H") else "D"

        kost1 = col(row, _COL_KOST1)
        kost2 = col(row, _COL_KOST2)
        cost_center_number = kost2 if (year is not None and year >= 2026) else kost1
        sphere_number = kost1 if (year is not None and year >= 2026) else "3"

        original_posting_text = col(row, _COL_BUCHUNGSTEXT)
        posting_text = _derive_description(
            original_posting_text,
            fiscal_year=year or 0,
            datev_kost1=kost1,
            account_number=original_account_number,
        )

        # Beleginfo / Zusatzinformation now have dedicated columns; the Beleglink
        # GUID is lifted into bedi_guid (the raw Beleglink still flows into
        # other_datev_columns).
        beleginfo = _build_info_slots(
            column_names, row, _RE_BELEGINFO_ART, _RE_BELEGINFO_INHALT
        )
        zusatzinformation = _build_info_slots(
            column_names, row, _RE_ZUSATZINFO_ART, _RE_ZUSATZINFO_INHALT
        )
        bedi_guid = _extract_bedi_guid(col(row, _COL_BELEGLINK))

        # Belegdatum is a mandatory DTVF field and booking_date is NOT NULL;
        # fail with file/row context instead of a mid-batch DB error.
        booking_date = (
            _parse_belegdatum(col(row, _COL_BELEGDATUM), year=year) if year else None
        )
        if booking_date is None:
            raise ValueError(
                f"{header.source_file}: row {ordinal} has no valid Belegdatum "
                "(required; the booking_date column is NOT NULL)"
            )

        # DATEV truncates the Buchungstext to 60 chars (often ending in "..."); the
        # full SEPA Verwendungszweck usually survives in the Zusatzinformation
        # "D_Nachricht". When the Buchungstext -- with any trailing dots removed --
        # is a prefix of that D_Nachricht, prefer the longer, complete text as the
        # posting_text.
        d_nachricht = _info_value(zusatzinformation, "D_Nachricht")
        posting_prefix = (
            original_posting_text.rstrip(". ") if original_posting_text else ""
        )
        if (
            posting_prefix
            and d_nachricht
            and d_nachricht != original_posting_text
            and d_nachricht.startswith(posting_prefix)
        ):
            posting_text = d_nachricht

        # Every other populated column without a dedicated mapping -- keeps the
        # raw Beleglink, Festschreibung=1, Generalumkehr=1, Kurs, Basis-Umsatz,
        # ... Beleginfo/Zusatzinformation slots are excluded (own columns).
        # Trivial "0"/empty technical fields are dropped.
        unmapped = {}
        for name, raw in zip(column_names, row):
            if name in _MAPPED_COLUMNS or _is_info_column(name):
                continue
            val = (raw or "").strip()
            if val and val != "0":
                unmapped[name] = val

        # Amounts: accounting is kept in the base currency (EUR). DATEV fills the
        # Basis-Umsatz / WKZ Basis-Umsatz only for foreign-currency bookings, so
        # for a plain EUR booking the Umsatz already IS the base amount. The
        # as-booked transaction figures are stored only when the booking is in a
        # foreign currency.
        umsatz = _parse_amount_de(col(row, _COL_UMSATZ))
        wkz_umsatz = col(row, _COL_WKZ) or "EUR"
        basis_umsatz = _parse_amount_de(col(row, _COL_BASIS_UMSATZ))
        wkz_basis = col(row, _COL_WKZ_BASIS)
        kurs = _parse_amount_de(col(row, _COL_KURS))
        # The transaction figures (transaction_amount = DATEV Umsatz,
        # transaction_currency = WKZ Umsatz) are stored for EVERY row, so both are
        # filterable and the signed signed_transaction_amount / signed_offsetting_transaction_amount
        # (generated) exist for all rows. The base figures equal them for EUR
        # bookings; DATEV fills the Basis-Umsatz / WKZ Basis-Umsatz (and Kurs) only
        # for foreign-currency bookings, and then the base side diverges.
        transaction_amount = umsatz
        transaction_currency = wkz_umsatz
        if basis_umsatz is not None:  # foreign-currency booking
            base_amount = basis_umsatz
            base_currency = wkz_basis or "EUR"
            exchange_rate = kurs
        else:
            base_amount = umsatz
            base_currency = wkz_umsatz
            exchange_rate = None

        yield {
            "buchungs_guid": guid,
            "datev_booking_batch_id": None,  # set after the batch upsert
            "base_amount": base_amount,
            "debit_credit": debit_credit,
            "base_currency": base_currency,
            # As-booked transaction figures (stored for every row; the signed
            # signed_transaction_amount / signed_offsetting_transaction_amount are generated).
            "transaction_amount": transaction_amount,
            "transaction_currency": transaction_currency,
            "exchange_rate": exchange_rate,
            "account_number": account_number,
            "offsetting_account_number": offsetting_account_number,
            "account_kind": account_kind,
            "offsetting_account_kind": offsetting_account_kind,
            "original_account_number": original_account_number,
            "original_offsetting_account_number": original_offsetting_account_number,
            "document_field_1": col(row, _COL_BELEGFELD1),
            "document_field_2": col(row, _COL_BELEGFELD2),
            "booking_date": booking_date,
            "service_date": _parse_ttmmjjjj(col(row, _COL_LEISTUNGSDATUM)),
            "cost_center_number": cost_center_number,
            "original_kost1": kost1,
            "original_kost2": kost2,
            "sphere_number": sphere_number,
            "original_posting_text": original_posting_text,
            "posting_text": posting_text,
            "origin_indicator": col(row, _COL_HERKUNFT),
            # Record-level Festschreibung / Generalumkehr: true only when the
            # export explicitly flags the row (empty/0 -> false; DATEV allows
            # G or 1 for Generalumkehr).
            "is_finalized": col(row, _COL_FESTSCHREIBUNG) == "1",
            "is_general_reversal": col(row, _COL_GENERALUMKEHR) in ("1", "G"),
            "bedi_guid": bedi_guid,
            "beleginfo": beleginfo,
            "zusatzinformation": zusatzinformation,
            "other_datev_columns": unmapped,
        }


def _require_complete_accounts(bookings: list[dict[str, object]]) -> None:
    """account_number / offsetting_account_number are NOT NULL. Abort with a
    clear message (unmapped 2025 account) rather than a mid-batch DB error."""
    missing: dict[str, int] = {}
    for b in bookings:
        for side, raw_side in (
            ("account_number", "original_account_number"),
            ("offsetting_account_number", "original_offsetting_account_number"),
        ):
            if b[side] is None:
                missing[str(b[raw_side])] = missing.get(str(b[raw_side]), 0) + 1
    if missing:
        detail = ", ".join(f"{acc!r}: {n}" for acc, n in sorted(missing.items()))
        raise SystemExit(
            "Unmapped/empty DATEV account numbers (add them to "
            f"ACCOUNT_MAP_2025_TO_2026 in import_datev_primanota.py): {detail}"
        )


def _require_eur_batch_currency(headers: list[_BatchHeader]) -> None:
    """The Stapel currency (DTVF header field 22, WKZ) must be EUR: it is the
    base currency of the whole batch, and the accounting (generated amount
    columns, reconciliation) keeps all base figures in EUR. Refuse otherwise."""
    bad = [(h.source_file, h.currency) for h in headers if h.currency != "EUR"]
    if bad:
        detail = ", ".join(f"{name}: {currency!r}" for name, currency in bad)
        raise SystemExit(f"Import refused: non-EUR Buchungsstapel currency in {detail}")


def _require_eur_base_currency(
    per_file: list[tuple[_BatchHeader, list[dict[str, object]]]],
) -> None:
    """Every booking's BASE currency must be EUR. A non-EUR base arises from a
    foreign-currency Umsatz WITHOUT Basis-Umsatz (DATEV normally fills the
    Basis-Umsatz for foreign-currency bookings); importing it would put a
    non-EUR figure into the EUR-based amount columns. Refuse the import."""
    bad = []
    for header, rows in per_file:
        for ordinal, row in enumerate(rows, start=1):
            if row["base_currency"] != "EUR":
                bad.append(
                    f"{header.source_file} row {ordinal} "
                    f"(GUID {row['buchungs_guid']}): {row['base_currency']!r}"
                )
    if bad:
        shown = "; ".join(bad[:10])
        more = f"; ... (+{len(bad) - 10} more)" if len(bad) > 10 else ""
        raise SystemExit(
            f"Import refused: {len(bad)} booking(s) whose base currency is "
            f"not EUR: {shown}{more}"
        )


def _batch_identity(header: _BatchHeader) -> tuple:
    """The stable Stapel identity (the composite plan key), in
    _BATCH_IDENTITY order."""
    return (
        header.consultant_number,
        header.client_number,
        header.period_from,
        header.period_to,
        header.label,
    )


def _batch_values(header: _BatchHeader, primanota_number: str | None) -> dict:
    """One datev_booking_batches value set (identity + payload columns)."""
    return {
        "consultant_number": header.consultant_number,
        "client_number": header.client_number,
        "period_from": header.period_from,
        "period_to": header.period_to,
        "label": header.label,
        "financial_year_start": header.fiscal_year_start,
        "primanota_number": primanota_number,
        "origin_indicator": header.origin_indicator,
        "is_finalized": header.festschreibung,
        "booking_type": header.booking_type,
        "ledger_account_number_length": header.account_number_length,
        "datev_chart_of_accounts_number": header.chart_of_accounts,
        "base_currency": header.currency,
        "datev_created_at": header.datev_created_at,
        # This importer reads a DATEV export INTO Hitobito (never the reverse).
        "import_export": "import",
        "header_raw": header.header_raw,
    }


def _add_source_file(batches_plan, headers: list[_BatchHeader]) -> None:
    """source_file is provenance metadata, not batch content: a changed file
    name alone must never turn an otherwise identical Stapel into an UPDATE.
    The column is therefore kept OUT of the diffed value sets (see
    _batch_values) and only added to rows the plan already INSERTs or UPDATEs
    for other reasons -- refreshing it whenever the row is written anyway."""
    by_identity = {_batch_identity(h): h.source_file for h in headers}
    for row in (*batches_plan.inserts, *batches_plan.updates):
        row["source_file"] = by_identity[tuple(row[name] for name in _BATCH_IDENTITY)]


def _load_batch_ids(conn, headers: list[_BatchHeader]) -> dict[tuple, int]:
    """Map every header's identity tuple to its datev_booking_batches.id (the
    table is small; loaded wholesale). All batches must exist -- called after
    the batches plan has been applied, or when it plans no INSERTs."""
    rows = conn.execute(
        f"SELECT id, {', '.join(_BATCH_IDENTITY)} FROM {_BATCHES_TABLE}"
    ).fetchall()
    by_identity = {tuple(row[1:]): row[0] for row in rows}
    ids: dict[tuple, int] = {}
    for header in headers:
        identity = _batch_identity(header)
        if identity not in by_identity:
            raise RuntimeError(
                f"batch {header.label!r} ({header.source_file}) not found in "
                f"{_BATCHES_TABLE} after the batches plan was applied"
            )
        ids[identity] = by_identity[identity]
    return ids


def _build_bookings_plan(
    conn,
    ctx,
    per_file: list[tuple[_BatchHeader, list[dict[str, object]]]],
    batch_ids: dict[tuple, int],
):
    """Build + compute the datev_bookings plan (keyed on buchungs_guid) with
    the final datev_booking_batch_id filled in.

    Two columns stay OUT of the diffed values and are added to the plan rows
    afterwards: the hand-editable `posting_text` goes onto INSERT rows only
    (an UPDATE never touches it, so a manual edit in the app survives every
    re-import), and `source_file` is change provenance -- stamped onto INSERT
    rows and onto genuinely CHANGED rows, so a re-import that changes nothing
    (also under a new file name) touches nobody's source_file."""
    values: list[dict[str, object]] = []
    posting_texts: dict[object, object] = {}
    source_files: dict[object, object] = {}
    for header, rows in per_file:
        batch_id = batch_ids[_batch_identity(header)]
        for row in rows:
            posting_texts[row["buchungs_guid"]] = row["posting_text"]
            source_files[row["buchungs_guid"]] = header.source_file
            value = {k: v for k, v in row.items() if k != "posting_text"}
            value["datev_booking_batch_id"] = batch_id
            values.append(value)
    builder = SingleTableUpsertPlanBuilder(
        _BOOKINGS_TABLE, "buchungs_guid", values, time_zone=ctx.hitobito_time_zone
    )
    builder.load_existing(conn)
    # other_datev_columns mirrors the export file (snapshot): keys DATEV no
    # longer sends are deleted on re-import.
    planned = builder.plan(replace_dict_columns=["other_datev_columns"])
    for insert_row in planned.inserts:
        insert_row["posting_text"] = posting_texts[insert_row["buchungs_guid"]]
        insert_row["source_file"] = source_files[insert_row["buchungs_guid"]]
    for update_row in planned.updates:
        update_row["source_file"] = source_files[update_row["buchungs_guid"]]
    return planned


def _log_plan_summary(planned, *, key_of) -> None:
    """Show what an approval would apply -- logged BEFORE the production
    approval is requested. ``key_of`` renders one plan row for the log."""
    for table, counts in planned.operation_counts().items():
        _LOGGER.info(
            "Planned for %s: %d INSERTs, %d UPDATEs (%d untouched).",
            table,
            counts.inserts,
            counts.updates,
            len(planned.untouched_keys),
        )
    for label, rows in (("INSERT", planned.inserts), ("UPDATE", planned.updates)):
        if not rows:
            continue
        keys = [str(key_of(row)) for row in rows]
        shown = ", ".join(keys[:10])
        if len(keys) > 10:
            shown += f", ... (+{len(keys) - 10} more)"
        _LOGGER.info("  %s: %s", label, shown)


# Person number embedded in a fee Buchungstext, e.g. "CMT 11" / "YP 4711".
_PERSON_IN_TEXT_RE = _re.compile(r"\b(?:CMT|YP|UL|IST)\s+(\d+)\b")

# Regular fee Belegfeld 1, e.g. "Einzug-2026-01-RCUR-4-1717"; the trailing block
# is the wsjrdp_direct_debit_pre_notifications id.
_RE_EINZUG_PRENOTIF = _re.compile(r"^Einzug-\d{4}-\d{2}-[A-Z]{4}-\d+-(\d+)$")


def _link_entry(cur, *, booking_id, entry_id, link_type, now) -> None:
    """Link a booking to its accounting entry -- the link lives ON THE ENTRY now
    (accounting_entries.datev_booking_id + the datev_booking_link_meta JSON).
    A booking has no own person column any more (its person is the entry's
    subject). The importer's two rules are its deterministic import-equivalent
    cases, so the meta is: automatic_manual = 'automatic', score = 1.0 (100 %),
    author_id = 1 (system person), classification_string = link_type. ``now`` is
    the AWARE ctx.start_time; created_at stores its ISO 8601 form (and the UTC
    session writes updated_at Rails-conventionally as UTC-naive). The entry IS
    modified, so its updated_at is bumped."""
    from psycopg.types.json import Jsonb

    meta = {
        "created_at": now.isoformat(),
        "author_id": 1,
        "score": 1.0,
        "automatic_manual": "automatic",
        "classification_string": link_type,
    }
    cur.execute(
        "UPDATE accounting_entries SET datev_booking_id = %s,"
        " datev_booking_link_meta = %s, updated_at = %s WHERE id = %s",
        (booking_id, Jsonb(meta), now, entry_id),
    )


def _match_2025_fee_entries(cur, bookings, ctx) -> None:
    """Link 2025 participant-fee bookings (of the batch just imported) to their
    accounting entry.

    Scope: the passed-in bookings that are 2025, KOST 9500 and Gegenkonto 41030
    (the mapped SKR42 fee account) and not yet linked. Match rule (verified exact
    on the real data): the person id from the Buchungstext, the same amount
    INCLUDING THE SIGN (the fee-side signed_offsetting_base_amount equals the entry's
    amount_cents on every historical pair) and the EXACT booking date
    (accounting_entries.value_date = datev_bookings.booking_date). Only
    unambiguous hits against not-yet-linked entries are connected; sets the
    entry's datev_booking_id + link_meta (classification_string =
    '2025_fee_booking') -- the link lives on the entry now. Idempotent."""
    now = ctx.start_time
    guids = [b["buchungs_guid"] for b in bookings]
    cur.execute(
        "SELECT db.id, db.original_posting_text, db.signed_offsetting_base_amount,"
        " db.booking_date"
        f" FROM {_BOOKINGS_TABLE} db"
        f" JOIN {_BATCHES_TABLE} b ON b.id = db.datev_booking_batch_id"
        " WHERE EXTRACT(YEAR FROM b.financial_year_start) = 2025"
        " AND db.cost_center_number = '9500'"
        " AND db.offsetting_account_number = '41030'"
        " AND NOT EXISTS (SELECT 1 FROM accounting_entries ae"
        "                 WHERE ae.datev_booking_id = db.id)"
        " AND db.buchungs_guid = ANY(%s)",
        (guids,),
    )
    rows = cur.fetchall()
    linked = 0
    skipped = 0
    for booking_id, text, amount, booking_date in rows:
        match = _PERSON_IN_TEXT_RE.search(text or "")
        if match is None or booking_date is None:
            skipped += 1
            continue
        person_id = int(match.group(1))
        cents = int(round(amount * 100))
        cur.execute(
            "SELECT ae.id FROM accounting_entries ae"
            " WHERE ae.subject_type = 'Person' AND ae.subject_id = %s"
            " AND ae.amount_cents = %s AND ae.value_date = %s"
            " AND ae.datev_booking_id IS NULL"
            " AND COALESCE((ae.additional_info ->>"
            " 'excluded_from_fee_reconciliation')::boolean, false) = false",
            (person_id, cents, booking_date),
        )
        hits = cur.fetchall()
        if len(hits) != 1:
            skipped += 1
            continue
        _link_entry(
            cur,
            booking_id=booking_id,
            entry_id=hits[0][0],
            link_type="2025_fee_booking",
            now=now,
        )
        linked += 1
    if rows:
        _LOGGER.info(
            "2025 TN-Beitraege: %d von %d unverknuepften Buchungen mit ihrer "
            "Beitragsbuchung verknuepft (%d ohne eindeutigen Treffer).",
            linked,
            len(rows),
            skipped,
        )


def _match_pre_notification_fee_entries(cur, bookings, ctx) -> None:
    """Link regular fee bookings (of the batch just imported) to their accounting
    entry via the pre-notification id in Belegfeld 1.

    Scope: the passed-in bookings with Gegenkonto 41030, KOST 9500 and a
    Belegfeld 1 of the form 'Einzug-YYYY-MM-<SEQ>-<n>-<prenotif_id>' that are not
    yet linked. The trailing block is the wsjrdp_direct_debit_pre_notifications id;
    the accounting entry to link points at it via
    accounting_entries.direct_debit_pre_notification_id. The person id parsed from
    the Buchungstext must match the entry's subject (verification). Only
    unambiguous hits against not-yet-linked entries are connected; sets the
    entry's datev_booking_id + link_meta (classification_string =
    'document_field_1_pre_notification'). Idempotent."""
    now = ctx.start_time
    guids = [b["buchungs_guid"] for b in bookings]
    cur.execute(
        "SELECT db.id, db.document_field_1, db.original_posting_text"
        f" FROM {_BOOKINGS_TABLE} db"
        " WHERE db.offsetting_account_number = '41030'"
        " AND db.cost_center_number = '9500'"
        " AND NOT EXISTS (SELECT 1 FROM accounting_entries ae"
        "                 WHERE ae.datev_booking_id = db.id)"
        " AND db.buchungs_guid = ANY(%s)",
        (guids,),
    )
    considered = 0
    linked = 0
    skipped = 0
    for booking_id, document_field_1, text in cur.fetchall():
        m = _RE_EINZUG_PRENOTIF.match(document_field_1 or "")
        if m is None:
            continue  # not a pre-notification fee booking
        considered += 1
        pre_notification_id = int(m.group(1))
        person_match = _PERSON_IN_TEXT_RE.search(text or "")
        if person_match is None:
            skipped += 1
            continue
        person_id = int(person_match.group(1))
        cur.execute(
            "SELECT ae.id FROM accounting_entries ae"
            " WHERE ae.direct_debit_pre_notification_id = %s"
            " AND ae.subject_type = 'Person' AND ae.subject_id = %s"
            " AND ae.datev_booking_id IS NULL"
            " AND COALESCE((ae.additional_info ->>"
            " 'excluded_from_fee_reconciliation')::boolean, false) = false",
            (pre_notification_id, person_id),
        )
        hits = cur.fetchall()
        if len(hits) != 1:
            skipped += 1
            continue
        _link_entry(
            cur,
            booking_id=booking_id,
            entry_id=hits[0][0],
            link_type="document_field_1_pre_notification",
            now=now,
        )
        linked += 1
    if considered:
        _LOGGER.info(
            "Pre-Notification-Beitraege: %d von %d Einzug-Buchungen mit ihrer "
            "Beitragsbuchung verknuepft (%d ohne eindeutigen Treffer).",
            linked,
            considered,
            skipped,
        )


def _mirror_camt_links(cur, bookings) -> None:
    """Mirror each linked entry's bank-statement (camt) transaction onto the
    camt side: wsjrdp_camt_transactions.datev_booking_id = the entry's booking,
    for the batch just imported. Idempotent."""
    guids = [b["buchungs_guid"] for b in bookings]
    cur.execute(
        "UPDATE wsjrdp_camt_transactions c SET datev_booking_id = ae.datev_booking_id"
        " FROM accounting_entries ae"
        f" JOIN {_BOOKINGS_TABLE} db ON db.id = ae.datev_booking_id"
        " WHERE ae.camt_transaction_id = c.id"
        " AND ae.datev_booking_id IS NOT NULL"
        " AND c.datev_booking_id IS DISTINCT FROM ae.datev_booking_id"
        " AND db.buchungs_guid = ANY(%s)",
        (guids,),
    )
    if cur.rowcount:
        _LOGGER.info("camt-Verknuepfung auf %d Buchung(en) gespiegelt.", cur.rowcount)


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("dtvf_files", nargs="+", help="DATEV DTVF Buchungsstapel .csv files")
    p.add_argument(
        "--truncate",
        action="store_true",
        help=f"Wipe {_BOOKINGS_TABLE} + {_BATCHES_TABLE} before importing (clean full reload).",
    )
    p.add_argument(
        "--no-auto-link",
        action="store_true",
        help="Do not auto-link accounting entries (skip the fee/pre-notification matching).",
    )
    p.add_argument(
        "--rollback-for-testing",
        action="store_true",
        default=False,
        help="Apply the plan, then ROLLBACK instead of committing (testing).",
    )
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        __file__=__file__,
    )
    out_base = ctx.make_out_path(_SELF_NAME + "_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    paths = [_pathlib.Path(p) for p in ctx.parsed_args.dtvf_files]
    parsed = []  # (header, column_names, data_rows)
    for path in paths:
        header_fields, column_names, data_rows = _read_dtvf(path)
        header = _parse_header(header_fields, source_file=str(path))
        parsed.append((header, column_names, data_rows))
        _LOGGER.info(
            "%s  Stapel %r (%s .. %s, Herkunft %s, %d Buchungen)",
            path.name,
            header.label,
            header.period_from,
            header.period_to,
            header.origin_indicator,
            len(data_rows),
        )
    headers = [h for h, _, _ in parsed]
    _require_eur_batch_currency(headers)

    primanota_numbers = _reconstruct_primanota_numbers(headers)

    all_bookings: list[dict[str, object]] = []
    per_file: list[tuple[_BatchHeader, list[dict[str, object]]]] = []
    for header, column_names, data_rows in parsed:
        rows = list(
            _iter_bookings(
                column_names=column_names,
                data_rows=data_rows,
                header=header,
            )
        )
        per_file.append((header, rows))
        all_bookings.extend(rows)

    _LOGGER.info("")
    _LOGGER.info(
        "Total bookings parsed: %d in %d batches", len(all_bookings), len(per_file)
    )
    _require_eur_base_currency(per_file)
    _require_complete_accounts(all_bookings)

    batches_builder = SingleTableUpsertPlanBuilder(
        _BATCHES_TABLE,
        tuple(_BATCH_IDENTITY),
        [_batch_values(h, primanota_numbers.get(h.source_file)) for h in headers],
        time_zone=ctx.hitobito_time_zone,
    )

    truncate = ctx.parsed_args.truncate
    auto_link = not ctx.parsed_args.no_auto_link
    with ctx:
        bookings_plan = None
        if truncate:
            # A clean reload wipes both tables first, so the plans can only be
            # computed AFTER the truncate (inside the write transaction):
            # everything becomes an INSERT.
            _LOGGER.info(
                "--truncate: TRUNCATE %s, %s + full reload of %d batches / "
                "%d bookings.",
                _BOOKINGS_TABLE,
                _BATCHES_TABLE,
                len(headers),
                len(all_bookings),
            )
            if ctx.dry_run:
                _LOGGER.info("[dry-run] Not truncating / writing.")
                return
            ctx.require_approval_to_run_in_prod()
            rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
            # The booking<->entry and booking<->camt links live on the entry /
            # camt side now, i.e. the PRODUCTION accounting_entries and
            # wsjrdp_camt_transactions reference datev_bookings. TRUNCATE CASCADE
            # would therefore wipe those tables -- so null the (now stale) links
            # first and DELETE (which respects the FKs by checking actual rows,
            # and leaves the reference tables untouched) instead. The Moss
            # tables reference datev_bookings the same way (moss_bookings'
            # expense leg, moss_transactions' clearing leg; both FKs are ON
            # DELETE SET NULL): their ids would be nulled by the DELETE anyway,
            # so clear them together with their provenance meta, which the FK
            # would leave behind stale.
            _LOGGER.info(
                "Clear reversed links on accounting_entries / "
                "wsjrdp_camt_transactions / moss_bookings / moss_transactions, "
                "then DELETE %s, %s",
                _BOOKINGS_TABLE,
                _BATCHES_TABLE,
            )
            rw_conn.execute(
                "UPDATE accounting_entries SET datev_booking_id = NULL,"
                " datev_booking_link_meta = '{}'::jsonb"
                " WHERE datev_booking_id IS NOT NULL"
            )
            rw_conn.execute(
                "UPDATE wsjrdp_camt_transactions SET datev_booking_id = NULL"
                " WHERE datev_booking_id IS NOT NULL"
            )
            rw_conn.execute(
                "UPDATE moss_bookings SET expense_datev_booking_id = NULL,"
                " expense_datev_booking_link_meta = '{}'::jsonb"
                " WHERE expense_datev_booking_id IS NOT NULL"
            )
            rw_conn.execute(
                "UPDATE moss_transactions SET clearing_datev_booking_id = NULL,"
                " clearing_datev_booking_link_meta = '{}'::jsonb"
                " WHERE clearing_datev_booking_id IS NOT NULL"
            )
            rw_conn.execute(f"DELETE FROM {_BOOKINGS_TABLE}")
            rw_conn.execute(f"DELETE FROM {_BATCHES_TABLE}")
            rw_conn.execute(f"ALTER SEQUENCE {_BOOKINGS_TABLE}_id_seq RESTART WITH 1")
            rw_conn.execute(f"ALTER SEQUENCE {_BATCHES_TABLE}_id_seq RESTART WITH 1")
            plan_conn = rw_conn
        else:
            plan_conn = ctx.hitobito_psycopg_connection(read_only=True)

        batches_builder.load_existing(plan_conn)
        # header_raw mirrors the export file's header line (snapshot): fields
        # DATEV no longer sends are deleted on re-import.
        batches_plan = batches_builder.plan(replace_dict_columns=["header_raw"])
        _add_source_file(batches_plan, headers)
        _log_plan_summary(batches_plan, key_of=lambda row: row["label"])

        if not truncate:
            if not batches_plan.inserts:
                # Every Stapel already exists: the booking-level plan is
                # computable read-only, BEFORE the approval.
                batch_ids = _load_batch_ids(plan_conn, headers)
                bookings_plan = _build_bookings_plan(
                    plan_conn, ctx, per_file, batch_ids
                )
                _log_plan_summary(
                    bookings_plan, key_of=lambda row: row["buchungs_guid"]
                )
            else:
                _LOGGER.info(
                    "%d new Stapel: the booking-level plan follows after "
                    "their INSERT (needs the new batch ids).",
                    len(batches_plan.inserts),
                )
            if ctx.dry_run:
                _LOGGER.info("[dry-run] Not applying the plan.")
                return
            # The plan summary above shows what this approval applies (plus
            # the auto-linking on the freshly imported bookings).
            ctx.require_approval_to_run_in_prod()
            rw_conn = ctx.hitobito_psycopg_connection(read_only=False)

        inserted, updated = batches_plan.apply(rw_conn, now=ctx.start_time)
        _LOGGER.info(
            "Batches: %d inserted, %d updated, %d untouched (identical).",
            len(inserted),
            len(updated),
            len(batches_plan.untouched_keys),
        )

        if bookings_plan is None:
            batch_ids = _load_batch_ids(rw_conn, headers)
            bookings_plan = _build_bookings_plan(rw_conn, ctx, per_file, batch_ids)
            _log_plan_summary(bookings_plan, key_of=lambda row: row["buchungs_guid"])
        inserted, updated = bookings_plan.apply(rw_conn, now=ctx.start_time)
        _LOGGER.info(
            "Bookings: %d inserted, %d updated, %d untouched (identical).",
            len(inserted),
            len(updated),
            len(bookings_plan.untouched_keys),
        )

        if auto_link:
            # Auto-link the imported fee bookings to their accounting entry /
            # person / camt transaction (idempotent; only not-yet-linked
            # bookings of this run are considered).
            with rw_conn.cursor() as cur:
                for header, rows in per_file:
                    _match_2025_fee_entries(cur, rows, ctx)
                    _match_pre_notification_fee_entries(cur, rows, ctx)
                    _mirror_camt_links(cur, rows)
        else:
            _LOGGER.info("[--no-auto-link] Skipped accounting-entry linking.")

        if ctx.parsed_args.rollback_for_testing:
            _LOGGER.warning(
                "ROLLBACK (--rollback-for-testing given) - no changes committed"
            )
            rw_conn.rollback()
        # The commit happens implicitly when the `with ctx:` block exits
        # cleanly; an exception before that leaves the database untouched.

    _LOGGER.info("Output directory: %s", ctx.out_dir)


if __name__ == "__main__":
    _sys.exit(main())
