"""Tests for the DATEV CP1252 transliteration.

DATEV exports are Windows-1252. `to_win1252_compatible` reproduces the
transliteration DATEV applies, so it is also the canonical form for comparing a
value read from a DATEV export against a Unicode value in our database (see
doc/fin/datev_cp1252.md in the hitobito_wsjrdp_2027 wagon).
"""

import pytest
import wsjrdp2027


to_win1252_compatible = wsjrdp2027.datev.to_win1252_compatible
win1252_matches_stored = wsjrdp2027.datev.win1252_matches_stored


# (language, alphabet, expected). Characters contained in CP1252 must pass
# through unchanged; everything else degrades to its base letter.
LANGUAGE_ALPHABETS = [
    # Fully representable in CP1252 -> unchanged.
    ("German", "äöüÄÖÜß", "äöüÄÖÜß"),
    ("Danish", "æøåÆØÅ", "æøåÆØÅ"),
    ("Swedish", "åäöÅÄÖ", "åäöÅÄÖ"),
    ("Finnish", "äöåÄÖÅšžŠŽ", "äöåÄÖÅšžŠŽ"),
    ("Icelandic", "áéíóúýþðæöÁÉÍÓÚÝÞÐÆÖ", "áéíóúýþðæöÁÉÍÓÚÝÞÐÆÖ"),
    # Partly outside CP1252 -> those characters lose their diacritic.
    ("Polish", "ąćęłńóśźżĄĆĘŁŃÓŚŹŻ", "acelnószzACELNÓSZZ"),
    ("Czech", "áčďéěíňóřšťúůýžÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ", "ácdéeínórštúuýžÁCDÉEÍNÓRŠTÚUÝŽ"),
    ("Hungarian", "áéíóöőúüűÁÉÍÓÖŐÚÜŰ", "áéíóöoúüuÁÉÍÓÖOÚÜU"),
]


@pytest.mark.parametrize("language,alphabet,expected", LANGUAGE_ALPHABETS)
def test_alphabet(language: str, alphabet: str, expected: str):
    assert to_win1252_compatible(alphabet) == expected


@pytest.mark.parametrize("language,alphabet,expected", LANGUAGE_ALPHABETS)
def test_alphabet_survives_a_cp1252_roundtrip(
    language: str, alphabet: str, expected: str
):
    """The whole point: the result must be writable to a CP1252 file."""
    assert to_win1252_compatible(alphabet).encode("cp1252").decode("cp1252") == expected


@pytest.mark.parametrize(
    "char,expected",
    [
        # NFD exposes no base character for these (the stroke/bar is part of the
        # glyph), so they need the explicit fallback table.
        ("ł", "l"),
        ("Ł", "L"),
        ("đ", "d"),
        ("Đ", "D"),
        ("ħ", "h"),
        ("ŧ", "t"),
        ("ı", "i"),
        ("ẞ", "SS"),  # capital sharp s: no single CP1252 upper-case form
    ],
)
def test_character_without_nfd_base(char: str, expected: str):
    assert to_win1252_compatible(char) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("", ""),
        ("Konto 700026", "Konto 700026"),  # ASCII is never touched
        ("ZHP Chorągiew Gdańska", "ZHP Choragiew Gdanska"),
        ("Łódź", "Lódz"),  # ó is CP1252 and stays, Ł/ź do not
        ("STRAẞE", "STRASSE"),
        ("Müller & Co. KG", "Müller & Co. KG"),  # German umlaut stays
    ],
)
def test_text(text: str, expected: str):
    assert to_win1252_compatible(text) == expected


@pytest.mark.parametrize(
    "datev,stored,expected",
    [
        # Unchanged: identical, or the stored value reduced to CP1252.
        ("Müller", "Müller", True),
        ("", "", True),
        (None, None, True),
        ("Gdansk", "Gdańsk", True),
        ("Lódz", "Łódź", True),
        ("STRASSE", "STRAẞE", True),
        # Genuinely different.
        ("Danzig", "Gdańsk", False),
        ("Meier", "Müller", False),
        ("Gdansk ", "Gdańsk", False),  # trailing space is a real difference
        # DATEV delivers MORE than we hold -> a change, so the import upgrades.
        ("Gdańsk", "Gdansk", False),
        # None matches only None.
        (None, "Gdansk", False),
        ("Gdansk", None, False),
    ],
)
def test_win1252_matches_stored(datev: str | None, stored: str | None, expected: bool):
    """The importers' check: has this DATEV text changed against what we store?"""
    assert win1252_matches_stored(datev, stored) is expected


@pytest.mark.parametrize("rich,reduced", [("Gdańsk", "Gdansk"), ("Łódź", "Lódz")])
def test_win1252_matches_stored_is_deliberately_asymmetric(rich: str, reduced: str):
    """A reduced DATEV text against a rich stored one is "unchanged" (keep ours),
    but a rich DATEV text against a reduced stored one is a CHANGE -- that is how
    a future full-charset export upgrades our data."""
    assert win1252_matches_stored(reduced, rich) is True
    assert win1252_matches_stored(rich, reduced) is False


@pytest.mark.parametrize("rich,reduced", [("Gdańsk", "Gdansk"), ("Łódź", "Lódz")])
def test_stored_value_only_ever_ratchets_up(rich: str, reduced: str):
    """Repeated imports never fall back: once the rich form is stored, a reduced
    export leaves it alone; a reduced store is lifted by a rich export and then
    stays put."""
    stored = reduced
    for datev in (rich, reduced, rich, reduced):  # alternating export encodings
        if not win1252_matches_stored(datev, stored):
            stored = datev  # importer would update
    assert stored == rich
