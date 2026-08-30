from __future__ import annotations

import logging as _logging
import pathlib as _pathlib
import typing as _typing


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc
    import datetime as _datetime

    import psycopg as _psycopg

    from . import _context


_LOGGER = _logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Account type (DATEV "Kontenart") classification
# --------------------------------------------------------------------------
# Short enum-style account-type codes stored on datev_accounts and copied onto
# each booking; they represent the DATEV "Kontenart" of an account. PLACEHOLDER:
# derived purely from the (mapped, SKR42) account number until a real DATEV
# account export carrying the actual Kontenart is available.
ACCOUNT_KIND_BANK = "BANK"  # Bank-/Kassenkonten (Aktiva), z. B. 18xxx
ACCOUNT_KIND_TRANSIT = "TRANSIT"  # Geldtransit (Aktiva), z. B. 13xxx
ACCOUNT_KIND_CLEARING = "CLEARING"  # Verrechnungskonten, z. B. 36xxx
ACCOUNT_KIND_LIABILITY = "LIABILITY"  # Verbindlichkeiten, z. B. 33xxx/37xxx
ACCOUNT_KIND_CREDITOR = "CREDITOR"  # Kreditoren / Lieferanten (Personenkonten), 700xxx
ACCOUNT_KIND_INCOME = "INCOME"  # Ertraege / Einnahmen, z. B. 4xxxx, 71000
ACCOUNT_KIND_EXPENSE = "EXPENSE"  # Aufwendungen, z. B. 6xxxx
ACCOUNT_KIND_EQUITY = "EQUITY"  # Eigenkapital / Saldenvortrag, z. B. 9xxxx
ACCOUNT_KIND_UNKNOWN = "UNKNOWN"

# Account types whose bookings are P&L (GuV) postings. For these the stored
# amount sign is flipped relative to the account-centric (Soll +, Haben -) value,
# so that income comes out positive and expense negative -- i.e. incoming money
# is positive and outgoing money is negative across every account type.
PROFIT_LOSS_ACCOUNT_KINDS = frozenset({ACCOUNT_KIND_INCOME, ACCOUNT_KIND_EXPENSE})


def account_kind_for_account_number(number: str | None) -> str:
    """Classify a (mapped, SKR42) DATEV account number into a short account-type
    code (the DATEV "Kontenart"). Placeholder number-based mapping until a real
    DATEV account export is available; see the ``ACCOUNT_KIND_*`` constants."""
    if not number:
        return ACCOUNT_KIND_UNKNOWN
    n = str(number).strip()
    if len(n) == 6 and n.startswith("7"):
        return ACCOUNT_KIND_CREDITOR  # 700xxx personal accounts (Kreditoren)
    if len(n) == 5:
        if n.startswith("13"):
            return ACCOUNT_KIND_TRANSIT
        if n.startswith("18"):
            return ACCOUNT_KIND_BANK
        if n.startswith("36"):
            return ACCOUNT_KIND_CLEARING
        first = n[0]
        if first == "3":  # 33xxx, 37xxx Verbindlichkeiten
            return ACCOUNT_KIND_LIABILITY
        if first == "4":  # 4xxxx Ertraege/Einnahmen
            return ACCOUNT_KIND_INCOME
        if first == "6":  # 6xxxx Aufwendungen
            return ACCOUNT_KIND_EXPENSE
        if first == "7":  # 71000 Zinsertraege etc.
            return ACCOUNT_KIND_INCOME
        if first == "9":  # 90000 Saldenvortrag
            return ACCOUNT_KIND_EQUITY
    return ACCOUNT_KIND_UNKNOWN


def account_kind_is_profit_loss(account_kind: str | None) -> bool:
    """Whether an account type (Kontenart) is a P&L (GuV) account (income/
    expense), i.e. one whose sign is flipped so income is positive and expense
    negative."""
    return account_kind in PROFIT_LOSS_ACCOUNT_KINDS


_DATEV_EXTF_BUCHUNGSSTAPEL_COLUMNS = [
    "Umsatz (ohne Soll/Haben-Kz)",
    "Soll/Haben-Kennzeichen",
    "WKZ Umsatz",
    "Kurs",
    "Basis-Umsatz",
    "WKZ Basis-Umsatz",
    "Konto",
    "Gegenkonto (ohne BU-Schlüssel)",
    "BU-Schlüssel",
    "Belegdatum",
    "Belegfeld 1",
    "Belegfeld 2",
    "Skonto",
    "Buchungstext",
    "Postensperre",
    "Diverse Adressnummer",
    "Geschäftspartnerbank",
    "Sachverhalt",
    "Zinssperre",
    "Beleglink",
    "Beleginfo - Art 1",
    "Beleginfo - Inhalt 1",
    "Beleginfo - Art 2",
    "Beleginfo - Inhalt 2",
    "Beleginfo - Art 3",
    "Beleginfo - Inhalt 3",
    "Beleginfo - Art 4",
    "Beleginfo - Inhalt 4",
    "Beleginfo - Art 5",
    "Beleginfo - Inhalt 5",
    "Beleginfo - Art 6",
    "Beleginfo - Inhalt 6",
    "Beleginfo - Art 7",
    "Beleginfo - Inhalt 7",
    "Beleginfo - Art 8",
    "Beleginfo - Inhalt 8",
    "KOST1 - Kostenstelle",
    "KOST2 - Kostenstelle",
    "Kost-Menge",
    "EU-Land u. UStID (Bestimmung)",
    "EU-Steuersatz (Bestimmung)",
    "Abw. Versteuerungsart",
    "Sachverhalt L+L",
    "Funktionsergänzung L+L",
    "BU 49 Hauptfunktionstyp",
    "BU 49 Hauptfunktionsnummer",
    "BU 49 Funktionsergänzung",
    "Zusatzinformation - Art 1",
    "Zusatzinformation- Inhalt 1",
    "Zusatzinformation - Art 2",
    "Zusatzinformation- Inhalt 2",
    "Zusatzinformation - Art 3",
    "Zusatzinformation- Inhalt 3",
    "Zusatzinformation - Art 4",
    "Zusatzinformation- Inhalt 4",
    "Zusatzinformation - Art 5",
    "Zusatzinformation- Inhalt 5",
    "Zusatzinformation - Art 6",
    "Zusatzinformation- Inhalt 6",
    "Zusatzinformation - Art 7",
    "Zusatzinformation- Inhalt 7",
    "Zusatzinformation - Art 8",
    "Zusatzinformation- Inhalt 8",
    "Zusatzinformation - Art 9",
    "Zusatzinformation- Inhalt 9",
    "Zusatzinformation - Art 10",
    "Zusatzinformation- Inhalt 10",
    "Zusatzinformation - Art 11",
    "Zusatzinformation- Inhalt 11",
    "Zusatzinformation - Art 12",
    "Zusatzinformation- Inhalt 12",
    "Zusatzinformation - Art 13",
    "Zusatzinformation- Inhalt 13",
    "Zusatzinformation - Art 14",
    "Zusatzinformation- Inhalt 14",
    "Zusatzinformation - Art 15",
    "Zusatzinformation- Inhalt 15",
    "Zusatzinformation - Art 16",
    "Zusatzinformation- Inhalt 16",
    "Zusatzinformation - Art 17",
    "Zusatzinformation- Inhalt 17",
    "Zusatzinformation - Art 18",
    "Zusatzinformation- Inhalt 18",
    "Zusatzinformation - Art 19",
    "Zusatzinformation- Inhalt 19",
    "Zusatzinformation - Art 20",
    "Zusatzinformation- Inhalt 20",
    "Stück",
    "Gewicht",
    "Zahlweise",
    "Forderungsart",
    "Veranlagungsjahr",
    "Zugeordnete Fälligkeit",
    "Skontotyp",
    "Auftragsnummer",
    "Buchungstyp",
    "USt-Schlüssel (Anzahlungen)",
    "EU-Land (Anzahlungen)",
    "Sachverhalt L+L (Anzahlungen)",
    "EU-Steuersatz (Anzahlungen)",
    "Erlöskonto (Anzahlungen)",
    "Herkunft-Kz",
    "Buchungs GUID",
    "KOST-Datum",
    "SEPA-Mandatsreferenz",
    "Skontosperre",
    "Gesellschaftername",
    "Beteiligtennummer",
    "Identifikationsnummer",
    "Zeichnernummer",
    "Postensperre bis",
    "Bezeichnung SoBil-Sachverhalt",
    "Kennzeichen SoBil-Buchung",
    "Festschreibung",
    "Leistungsdatum",
    "Datum Zuord. Steuerperiode",
    "Fälligkeit",
    "Generalumkehr (GU)",
    "Steuersatz",
    "Land",
    "Abrechnungsreferenz",
    "BVV-Position",
    "EU-Land u. UStID (Ursprung)",
    "EU-Steuersatz (Ursprung)",
    "Abw. Skontokonto",
]


_EXTF_TYPES = {
    "Umsatz (ohne Soll/Haben-Kz)": "Betrag",
    "Soll/Haben-Kennzeichen": "Text",
    "WKZ Umsatz": "Text",
    "Kurs": "Zahl",
    "Basis-Umsatz": "Betrag",
    "WKZ Basis-Umsatz": "Text",
    "Kontonummer": "Konto",
    "Konto": "Konto",
    "Gegenkonto (ohne BU-Schlüssel)": "Konto",
    "BU-Schlüssel": "Text",
    "Belegdatum": "Datum",
    "Belegfeld 1": "Text",
    "Belegfeld 2": "Text",
    "Skonto": "Betrag",
    "Buchungstext": "Text",
    "Postensperre": "Zahl",
    "Diverse Adressnummer": "Text",
    "Geschäftspartnerbank": "Zahl",
    "Sachverhalt": "Zahl",
    "Zinssperre": "Zahl",
    "Beleglink": "Text",
    "Beleginfo - Art 1": "Text",
    "Beleginfo - Inhalt 1": "Text",
    "Beleginfo - Art 2": "Text",
    "Beleginfo - Inhalt 2": "Text",
    "Beleginfo - Art 3": "Text",
    "Beleginfo - Inhalt 3": "Text",
    "Beleginfo - Art 4": "Text",
    "Beleginfo - Inhalt 4": "Text",
    "Beleginfo - Art 5": "Text",
    "Beleginfo - Inhalt 5": "Text",
    "Beleginfo - Art 6": "Text",
    "Beleginfo - Inhalt 6": "Text",
    "Beleginfo - Art 7": "Text",
    "Beleginfo - Inhalt 7": "Text",
    "Beleginfo - Art 8": "Text",
    "Beleginfo - Inhalt 8": "Text",
    "Kost 1 - Kostenstelle": "Text",
    "Kost 2 - Kostenstelle": "Text",
    "KOST1 - Kostenstelle": "Text",
    "KOST2 - Kostenstelle": "Text",
    "Kost-Menge": "Zahl",
    "EU-Land u. UStID (Bestimmung)": "Text",
    "EU-Steuersatz (Bestimmung)": "Zahl",
    "Abw. Versteuerungsart": "Text",
    "Sachverhalt L+L": "Zahl",
    "Funktionsergänzung L+L": "Zahl",
    "BU 49 Hauptfunktionstyp": "Zahl",
    "BU 49 Hauptfunktionsnummer": "Zahl",
    "BU 49 Funktionsergänzung": "Zahl",
    "Zusatzinformation - Art 1": "Text",
    "Zusatzinformation- Inhalt 1": "Text",
    "Zusatzinformation - Art 2": "Text",
    "Zusatzinformation- Inhalt 2": "Text",
    "Zusatzinformation - Art 3": "Text",
    "Zusatzinformation- Inhalt 3": "Text",
    "Zusatzinformation - Art 4": "Text",
    "Zusatzinformation- Inhalt 4": "Text",
    "Zusatzinformation - Art 5": "Text",
    "Zusatzinformation- Inhalt 5": "Text",
    "Zusatzinformation - Art 6": "Text",
    "Zusatzinformation- Inhalt 6": "Text",
    "Zusatzinformation - Art 7": "Text",
    "Zusatzinformation- Inhalt 7": "Text",
    "Zusatzinformation - Art 8": "Text",
    "Zusatzinformation- Inhalt 8": "Text",
    "Zusatzinformation - Art 9": "Text",
    "Zusatzinformation- Inhalt 9": "Text",
    "Zusatzinformation - Art 10": "Text",
    "Zusatzinformation- Inhalt 10": "Text",
    "Zusatzinformation - Art 11": "Text",
    "Zusatzinformation- Inhalt 11": "Text",
    "Zusatzinformation - Art 12": "Text",
    "Zusatzinformation- Inhalt 12": "Text",
    "Zusatzinformation - Art 13": "Text",
    "Zusatzinformation- Inhalt 13": "Text",
    "Zusatzinformation - Art 14": "Text",
    "Zusatzinformation- Inhalt 14": "Text",
    "Zusatzinformation - Art 15": "Text",
    "Zusatzinformation- Inhalt 15": "Text",
    "Zusatzinformation - Art 16": "Text",
    "Zusatzinformation- Inhalt 16": "Text",
    "Zusatzinformation - Art 17": "Text",
    "Zusatzinformation- Inhalt 17": "Text",
    "Zusatzinformation - Art 18": "Text",
    "Zusatzinformation- Inhalt 18": "Text",
    "Zusatzinformation - Art 19": "Text",
    "Zusatzinformation- Inhalt 19": "Text",
    "Zusatzinformation - Art 20": "Text",
    "Zusatzinformation- Inhalt 20": "Text",
    "Stück": "Zahl",
    "Gewicht": "Zahl",
    "Zahlweise": "Zahl",
    "Forderungsart": "Text",
    "Veranlagungsjahr": "Zahl",
    "Zugeordnete Fälligkeit": "Datum",
    "Skontotyp": "Zahl",
    "Auftragsnummer": "Text",
    "Buchungstyp (Anzahlungen)": "Text",
    "Buchungstyp": "Text",
    "USt-Schlüssel (Anzahlungen)": "Zahl",
    "EU-Land (Anzahlungen)": "Text",
    "Sachverhalt L+L (Anzahlungen)": "Zahl",
    "EU-Steuersatz (Anzahlungen)": "Zahl",
    "Erlöskonto (Anzahlungen)": "Konto",
    "Herkunft-Kz": "Text",
    "Buchungs GUID": "Text",
    "Kost-Datum": "Datum",
    "KOST-Datum": "Datum",
    "SEPA-Mandatsreferenz": "Text",
    "Skontosperre": "Zahl",
    "Gesellschaftername": "Text",
    "Beteiligtennummer": "Zahl",
    "Identifikationsnummer": "Text",
    "Zeichnernummer": "Text",
    "Postensperre bis": "Datum",
    "Bezeichnung SoBil-Sachverhalt": "Text",
    "Kennzeichen SoBil-Buchung": "Zahl",
    "Festschreibung": "Zahl",
    "Leistungsdatum": "Datum",
    "Datum Zuord. Steuerperiode": "Datum",
    "Fälligkeit": "Datum",
    "Generalumkehr (GU)": "Text",
    "Steuersatz": "Zahl",
    "Land": "Text",
    "Abrechnungsreferenz": "Text",
    "BVV-Position": "Zahl",
    "EU-Land u. UStID (Ursprung)": "Text",
    "EU-Steuersatz (Ursprung)": "Zahl",
    "Abw. Skontokonto": "Konto",
}


class DatevExtfWriter:
    def __init__(
        self,
        file,
        *,
        fieldnames: _collections_abc.Iterable[str] | None = None,
        type_overrides: dict[str, str] | None = None,
    ) -> None:
        """CSV writer for the 125-column DATEV Buchungsstapel record format.

        `type_overrides` maps field names to a serialization type ("Text",
        "Zahl", ...) overriding _EXTF_TYPES for this writer only. Used by the
        fake-DTVF generator to match DATEV's own serialization exactly, where
        it deviates from our defaults (observed on the real 2026 export:
        "Beteiligtennummer" is quoted-empty i.e. Text, "Generalumkehr (GU)" is
        an unquoted number).
        """
        if fieldnames is None:
            fieldnames = _DATEV_EXTF_BUCHUNGSSTAPEL_COLUMNS
        else:
            fieldnames = list(fieldnames)

        self._file = file
        self._fieldnames = fieldnames
        self._types = dict(_EXTF_TYPES)
        if type_overrides:
            unknown = [f for f in type_overrides if f not in self._types]
            if unknown:
                raise RuntimeError(f"Unknown fields in type_overrides: {unknown}")
            self._types.update(type_overrides)
        unsupported_fields = []
        for spalte in self._fieldnames:
            if spalte not in self._types:
                unsupported_fields.append(spalte)
        if unsupported_fields:
            raise RuntimeError(
                f"Unsupported fields: {'\n    '.join(str(f) for f in unsupported_fields)}"
            )

    def _serialize_field(self, field_name: str, value) -> str:
        field_type = self._types[field_name]
        if value is None:
            value = ""
        if field_type == "Text":
            value = '"' + str(value).replace('"', '""') + '"'
        else:
            value = str(value)
            assert '"' not in value
        return value

    def writeheader(self) -> None:
        self._file.write(";".join(field_name for field_name in self._fieldnames))
        self._file.write("\n")

    def writerow(self, rowdict, /) -> None:
        d = {field_name: "" for field_name in self._fieldnames}
        d.update(rowdict)
        line = ";".join(
            self._serialize_field(field_name, rowdict.get(field_name, ""))
            for field_name in self._fieldnames
        )
        self._file.write(line)
        self._file.write("\n")


def win1252_matches_stored(datev: str | None, stored: str | None) -> bool:
    """Whether a text from DATEV still denotes the value we already store.

    DATEV exports are CP1252 (see doc/fin/datev_cp1252.md in the
    hitobito_wsjrdp_2027 wagon), so an unchanged value may reach us reduced to
    its base characters. The check is **deliberately asymmetric**: the incoming
    DATEV text is compared against the stored text *and* against its
    transliteration, but never the other way round.

    >>> win1252_matches_stored("Gdansk", "Gdańsk")   # unchanged, just reduced
    True
    >>> win1252_matches_stored("Gdańsk", "Gdańsk")   # unchanged, full Unicode
    True
    >>> win1252_matches_stored("Danzig", "Gdańsk")   # genuinely renamed
    False

    The asymmetry is the point: should DATEV ever export the full character set
    (an xlsx or a UTF-8 Buchungsstapel), an incoming ``Gdańsk`` against a stored
    ``Gdansk`` counts as a CHANGE, so the import upgrades our value instead of
    silently keeping the poorer one.

    >>> win1252_matches_stored("Gdańsk", "Gdansk")   # richer than what we hold
    False

    ``None`` matches only ``None``.
    """
    if datev == stored:
        return True
    if datev is None or stored is None:
        return False
    return datev == to_win1252_compatible(stored)


def to_win1252_compatible(string: str) -> str:
    """Transliterate `string` the way DATEV does when it writes CP1252 exports.

    Characters representable in CP1252 (umlauts, accents, ...) pass through;
    everything else is reduced to its base character (Polish ``ą`` -> ``a``,
    ``ń`` -> ``n``, ...). Besides producing DATEV-safe output this is the
    canonical form for COMPARING a value from a DATEV export against a Unicode
    value already stored in the database -- see doc/fin/datev_cp1252.md in the
    hitobito_wsjrdp_2027 wagon.
    """
    return _to_win1252_compatible(string)


# Latin letters whose base form NFD does NOT expose (the diacritic is part of
# the glyph, not a combining mark), so they would otherwise degrade to "?".
_WIN1252_FALLBACKS = {
    "\u0141": "L", "\u0142": "l",  # L/l with stroke (Polish)
    "\u0110": "D", "\u0111": "d",  # D/d with stroke
    "\u00d8": "O", "\u00f8": "o",  # O/o with stroke (Nordic)
    "\u0126": "H", "\u0127": "h",  # H/h with stroke
    "\u0166": "T", "\u0167": "t",  # T/t with stroke
    "\u0131": "i",                  # dotless i (Turkish)
    "\u1e9e": "SS",                 # capital sharp s (German); ss.upper() convention
}  # fmt: skip


def win1252_equivalent(left: str | None, right: str | None) -> bool:
    """Whether two texts denote the same value, allowing for CP1252 transliteration.

    DATEV exports are CP1252 (see doc/fin/datev_cp1252.md in the
    hitobito_wsjrdp_2027 wagon), so the same value may reach us either in full
    Unicode or reduced to its base characters. Two strings are considered equal
    when they are equal outright, or when they are equal after transliterating
    BOTH sides -- which makes the check independent of which side happens to
    carry the richer encoding:

    >>> win1252_equivalent("Gdansk", "Gdansk")
    True
    >>> win1252_equivalent("Gdańsk", "Gdansk")   # stored Unicode vs CP1252 export
    True
    >>> win1252_equivalent("Gdansk", "Gdańsk")   # and the other way round
    True
    >>> win1252_equivalent("Gdańsk", "Danzig")
    False

    ``None`` equals only ``None``.
    """
    if left == right:
        return True
    if left is None or right is None:
        return False
    return to_win1252_compatible(left) == to_win1252_compatible(right)


def _to_win1252_compatible(string: str) -> str:
    import unicodedata

    result = []
    for char in string:
        try:
            char.encode("cp1252")
            result.append(char)
        except UnicodeEncodeError:
            normalized = unicodedata.normalize("NFD", char)
            base_char = "".join(c for c in normalized if not unicodedata.combining(c))
            try:
                base_char.encode("cp1252")
            except:  # noqa: E722
                base_char = _WIN1252_FALLBACKS.get(char, "")
                if not base_char:
                    _LOGGER.error(f"Cannot encode {char!r}")
                    base_char = "?"
            result.append(base_char)
    return "".join(result)


def _prepare_description(
    string: str, *, encoding: str = "utf-8", max_bytes_length: int = 60
) -> str:
    if encoding in ("cp1252", "win1252", "windows-1252"):
        string = _to_win1252_compatible(string)
    if max_bytes_length:
        max_str_length = max_bytes_length
        string = string[:max_str_length]
        while len(string.encode(encoding)) > max_bytes_length:
            max_str_length -= 1
            string = string[:max_str_length]
    return string


def build_buchungsstapel_header_line(
    *,
    kennzeichen: str = "EXTF",
    erzeugt_am: _datetime.datetime,
    herkunft: str = "",
    exportiert_von: str = "wsjrdp",
    beraternummer: int | str,
    mandantennummer: int | str,
    wj_beginn: _datetime.date,
    sachkontenlaenge: int,
    datum_von: _datetime.date,
    datum_bis: _datetime.date,
    bezeichnung: str,
    diktatkuerzel: str = "",
    buchungstyp: int = 1,
    rechnungslegungszweck: int = 0,
    festschreibung: int = 0,
    wkz: str = "EUR",
    derivatskennzeichen: str = "",
    reserviert_26: int | str | None = None,
    sachkontenrahmen: str,
    branchenloesung_id: int | None = None,
) -> str:
    """Metadata header (line 1) of a DATEV-Format "Buchungsstapel" file.

    Field names and positions follow the official format description
    (https://developer.datev.de/de/file-format/details/datev-format/format-description/header),
    Versionsnummer 700 / Formatversion 13. Returns the line WITHOUT a trailing
    newline. `kennzeichen` is "EXTF" for files we hand to DATEV for import and
    "DTVF" when mimicking a DATEV-generated export (fake test fixtures).
    `derivatskennzeichen` (position 24) and `reserviert_26` (position 26) are
    officially empty/reserved, but DATEV's own exporter fills them ("MP" and a
    per-batch number); they are parameters so fakes can mirror that.
    """
    datev_header = [
        f'"{kennzeichen}"',  # 1 - Kennzeichen
        700,  # 2 - Versionsnummer
        21,  # 3 - Formatkategorie (21 = Buchungsstapel)
        '"Buchungsstapel"',  # 4 - Formatname
        13,  # 5 - Formatversion
        erzeugt_am.strftime("%Y%m%d%H%M%S%f")[:-3],  # 6 - Erzeugt am YYYYMMDDHHMMSSFFF
        None,  # 7 - Importiert (Leerfeld)
        f'"{herkunft}"',  # 8 - Herkunft
        f'"{exportiert_von}"',  # 9 - Exportiert von
        '""',  # 10 - Importiert von
        beraternummer,  # 11 - Beraternummer
        mandantennummer,  # 12 - Mandantennummer
        wj_beginn.strftime("%Y%m%d"),  # 13 - WJ-Beginn YYYYMMDD
        sachkontenlaenge,  # 14 - Sachkontenlänge
        datum_von.strftime("%Y%m%d"),  # 15 - Datum von
        datum_bis.strftime("%Y%m%d"),  # 16 - Datum bis
        f'"{bezeichnung}"',  # 17 - Bezeichnung
        f'"{diktatkuerzel}"',  # 18 - Diktatkürzel
        buchungstyp,  # 19 - Buchungstyp (1 = Finanzbuchführung)
        rechnungslegungszweck,  # 20 - Rechnungslegungszweck
        festschreibung,  # 21 - Festschreibung
        f'"{wkz}"',  # 22 - WKZ
        None,  # 23 - Reserviert
        f'"{derivatskennzeichen}"',  # 24 - Derivatskennzeichen
        None,  # 25 - Reserviert
        reserviert_26,  # 26 - Reserviert (DATEV: per-batch number)
        f'"{sachkontenrahmen}"',  # 27 - Sachkontenrahmen
        branchenloesung_id,  # 28 - ID der Branchenlösung
        None,  # 29 - Reserviert
        '""',  # 30 - Reserviert
        '""',  # 31 - Anwendungsinformation
    ]
    return ";".join(str(s if s is not None else "") for s in datev_header)


def write_datev_csv_for_pain_id(
    *,
    ctx: _context.WsjRdpContext,
    conn: _psycopg.Connection,
    pain_id: int,
    csv_encoding: str = "utf-8",
    limit: int | None = None,
    offset: int | None = None,
    now: _datetime.date | None = None,
) -> _pathlib.Path:
    # See https://developer.datev.de/de/file-format/details/datev-format/format-description/booking-batch

    import re

    from . import _pg, _util

    now = _util.to_datetime(now, now=ctx.start_time)

    df = _pg.pg_select_dataframe(
        conn,
        t"""SELECT
  id, payment_status,
  dbtr_name, dbtr_iban, dbtr_bic,
  amount_cents, amount_currency,
  debit_sequence_type, collection_date,
  mandate_id, mandate_date,
  description, endtoend_id
FROM wsjrdp_direct_debit_pre_notifications
WHERE payment_status = 'xml_generated'
  AND payment_initiation_id = {pain_id}
LIMIT {limit}
OFFSET {offset or 0}
""",
    )

    row = df.iloc[0]
    collection_date = row["collection_date"]

    match csv_encoding.lower():
        case "cp1252" | "win1252" | "windows-1252":
            encoding = "cp1252"
            csv_file_encoding = "cp1252"
            encoding_filename_suffix = "cp1252"
        case "utf-8" | "utf-8-sig":
            encoding = "utf-8"
            csv_file_encoding = "utf-8-sig"
            encoding_filename_suffix = "utf8"
        case _:
            raise RuntimeError("Unsupported CSV encoding: {csv_encoding!r}")

    base_csv_filename = ctx.make_out_path(
        f"EXTF_sammeleinzug_wsj27_{collection_date.strftime('%Y-%m')}_pain{pain_id}_{encoding_filename_suffix}"
    )
    if limit is not None:
        base_csv_filename = base_csv_filename.with_name(
            name=base_csv_filename.name + f"_limit{limit}"
        )
    if offset is not None:
        base_csv_filename = base_csv_filename.with_name(
            name=base_csv_filename.name + f"_offset{offset}"
        )
    csv_filename = base_csv_filename.with_name(base_csv_filename.name + ".csv")

    beraternummer = ctx.config.datev_beraternummer
    mandantennummer = ctx.config.datev_mandantennummer
    if collection_date.year < 2026:
        konto = "1200"
        gegenkonto = "8116"
        kost1 = "9500"
        kost2 = None
        sachkontenlaenge = 4
        sachkontenrahmen = "03"
    else:
        konto = "18000"
        gegenkonto = "41030"
        kost1 = None
        kost2 = "9500"
        sachkontenlaenge = 5
        sachkontenrahmen = "42"

    with open(csv_filename, "w", encoding=csv_file_encoding, newline="\r\n") as csvfile:
        d_writer = DatevExtfWriter(csvfile)
        header_line = build_buchungsstapel_header_line(
            kennzeichen="EXTF",
            erzeugt_am=now,
            herkunft="",
            exportiert_von="wsjrdp",
            beraternummer=beraternummer,
            mandantennummer=mandantennummer,
            wj_beginn=collection_date.replace(month=1, day=1),
            sachkontenlaenge=sachkontenlaenge,
            datum_von=collection_date,
            datum_bis=collection_date,
            bezeichnung=f"Sammeleinzug {collection_date.strftime('%Y-%m')}",
            sachkontenrahmen=sachkontenrahmen,
        )
        csvfile.write(header_line + "\n")
        d_writer.writeheader()
        sum_cents = 0
        for _, row in df.iterrows():
            amount_currency = row.get("amount_currency") or "EUR"
            amount_cents = int(round(row["amount_cents"]))
            amount_eur = amount_cents / 100
            amount_de = str(f"{amount_eur:.2f}").replace(".", ",").replace(",00", "")
            sum_cents += amount_cents
            collection_date = row["collection_date"]
            belegfeld = f"Einzug-{collection_date.strftime('%Y-%m')}-{row['debit_sequence_type']}-{pain_id}-{row['id']}"
            buchungstext = row["description"].replace(" WSJ 2027 ", " ")
            buchungstext = buchungstext.removeprefix("WSJ 2027 ")
            buchungstext = re.sub(
                r"Beitrag *(?P<descr>.*) +(?P<role>CMT|YP|UL|IST) (?P<id>[0-9]+)",
                r"\g<role> \g<id> \g<descr> / Beitrag",
                buchungstext,
            )
            buchungstext = re.sub(
                r"(?P<role>CMT|YP|UL|IST) Beitrag *(?P<descr>.*) +(?P<id>[0-9]+)",
                r"\g<role> \g<id> \g<descr> / Beitrag",
                buchungstext,
            )
            buchungstext = re.sub(
                r"(?P<installment>[0-9]+. Rate \w+ 202[567]) (?P<role>CMT|YP|UL|IST) *(?P<descr>.*) +\(id (?P<id>[0-9]+)\)",
                r"\g<role> \g<id> \g<descr> / \g<installment>",
                buchungstext,
            )
            buchungstext = _prepare_description(
                buchungstext, encoding=encoding, max_bytes_length=60
            )
            d_writer.writerow(
                {
                    "Umsatz (ohne Soll/Haben-Kz)": amount_de.lstrip("-"),
                    "Soll/Haben-Kennzeichen": "S" if amount_cents > 0 else "H",
                    "WKZ Umsatz": amount_currency,
                    "Konto": konto,
                    "Gegenkonto (ohne BU-Schlüssel)": gegenkonto,
                    "Belegdatum": collection_date.strftime("%d%m"),
                    "Belegfeld 1": belegfeld[:36],
                    "Buchungstext": buchungstext,
                    "KOST1 - Kostenstelle": kost1,
                    "KOST2 - Kostenstelle": kost2,
                    "Festschreibung": "0",
                }
            )
    _LOGGER.info("    %s rows in dataframe", len(df))
    _LOGGER.info("    %s cents in dataframe", sum_cents)
    _LOGGER.info(f"  wrote {csv_filename}")
    return csv_filename
