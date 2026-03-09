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
        self, file, *, fieldnames: _collections_abc.Iterable[str] | None = None
    ) -> None:
        if fieldnames is None:
            fieldnames = _DATEV_EXTF_BUCHUNGSSTAPEL_COLUMNS
        else:
            fieldnames = list(fieldnames)

        self._file = file
        self._fieldnames = fieldnames
        unsupported_fields = []
        for spalte in self._fieldnames:
            if spalte not in _EXTF_TYPES:
                unsupported_fields.append(spalte)
        if unsupported_fields:
            raise RuntimeError(
                f"Unsupported fields: {'\n    '.join(str(f) for f in unsupported_fields)}"
            )

    def _serialize_field(self, field_name: str, value) -> str:
        field_type = _EXTF_TYPES[field_name]
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
                _LOGGER.error(f"Cannot encode {base_char!r}")
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
        datev_header = [
            '"EXTF"',  # 1 - Kennzeichen
            700,  # 2 - Versionsnummer
            21,  # 3 - Formatkategorie
            '"Buchungsstapel"',  # 4 - Formatname
            13,  # 5 - Formatversion
            now.strftime("%Y%m%d%H%M%S%f")[:-3],  # 6 - Erzeugt am YYYYMMDDHHMMSSFFF
            # 20240130140440439
            # 20260220024353259
            None,  # 7 - Importiert
            '""',  # 8 - Herkunft
            '"wsjrdp"',  # 9 - Exportiert von
            '""',  # 10 - Importiert von
            beraternummer,  # 11 - Beraternummer
            mandantennummer,  # 12 - Mandantennummer
            collection_date.replace(month=1, day=1).strftime(
                "%Y%m%d"
            ),  # 13 - WJ-Beginn YYYYMMDD
            sachkontenlaenge,  # 14 - Sachkontenlänge
            collection_date.strftime("%Y%m%d"),  # 15 - Datum von
            collection_date.strftime("%Y%m%d"),  # 16 - Datum bis
            f'"Sammeleinzug {collection_date.strftime("%Y-%m")}"',  # 17 - Bezeichnung
            '""',  # 18 - Diktatkürzel
            1,  # 19 - Buchungstyp
            0,  # 20 - Rechungslegungszweck
            0,  # 21 - Festschreibung
            '"EUR"',  # 22 - WKZ
            None,  # 23 - Reserviert
            '""',  # 24 - Derivatskennzeichen
            None,  # 25 - Reserviert
            None,  # 26 - Reserviert
            f'"{sachkontenrahmen}"',  # 27 - Sachkontenrahmen
            None,  # 28 - ID der Branchenlösung
            None,  # 29 - Reserviert
            '""',  # 30 - Reserviert
            '""',  # 31 - Anwendungsinformation
        ]
        csvfile.write(
            ";".join(str(s if s is not None else "") for s in datev_header) + "\n"
        )
        d_writer.writeheader()
        sum_cents = 0
        for _, row in df.iterrows():
            amount_currency = row.get("amount_currencty") or "EUR"
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
