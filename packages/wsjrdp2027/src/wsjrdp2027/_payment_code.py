"""Payment codes: RF creditor references around a Reed-Solomon codeword.

A payment code identifies a payment notice (wsjrdp_payment_notices) in the
remittance information of a transfer, so a bank statement import can link
the payment even when a person typed the code by hand.

Format (see docs/payment_code.md)::

    RFpp DDDD DDDD CCCC     printed, e.g. RF75 K7M3 QX9P NFK3
    RFppDDDDDDDDCCCC        electronic form, 16 characters

- ``DDDDDDDD``: 8 random symbols of Crockford base32
  (``0-9 A-Z`` without ``I L O U``).
- ``CCCC``: 4 Reed-Solomon parity symbols over the 8 data symbols:
  RS(12,8), shortened from RS(31,27) over GF(32), primitive polynomial
  x^5 + x^2 + 1 (0x25), generator 2, first consecutive root 1 (fcr=1),
  systematic (data first). Any two codewords differ in at least 5 symbols.
- ``RFpp``: ISO 11649 creditor reference prefix, ``pp`` = ISO 7064
  MOD 97-10 check digits over the 12-character codeword.

The codeword is ``DDDDDDDDCCCC``: the data symbols followed by the parity
symbols, a word of the Reed-Solomon code. The codeword functions
(:func:`encode_rs_codeword`, :func:`generate_rs_codeword`, ...) work without the
RF prefix and with other lengths.

Issuing a code (:func:`generate_distinct_payment_code`) also keeps it far
from every code issued before: by default a Hamming distance of at least
:data:`MIN_HAMMING_DISTANCE` and a Levenshtein distance of at least
:data:`MIN_LEVENSHTEIN_DISTANCE` between the codewords, drawing again up to
:data:`MAX_GENERATION_ATTEMPTS` times and then taking the farthest draw.
The Hitobito wagon issues codes with the same rules
(``Wsjrdp2027::PaymentCode``).

Matching a typed code (:func:`match_payment_code`) tries, in this order, an
exact codeword, a Reed-Solomon correction (up to 2 wrong symbols), the
nearest issued codeword by Hamming distance (up to 3 wrong symbols) and the
nearest by
Damerau-Levenshtein distance (up to 2 edits, a missing or extra symbol
included). A correction is accepted only when it leads to exactly one issued
code.
"""

from __future__ import annotations

import dataclasses as _dataclasses
import functools as _functools
import re as _re
import secrets as _secrets
import typing as _typing

import reedsolo as _reedsolo  # type: ignore[import-untyped]
from rapidfuzz.distance import (
    DamerauLevenshtein as _DamerauLevenshtein,
    Hamming as _Hamming,
    Levenshtein as _Levenshtein,
)
from stdnum import iso11649 as _iso11649  # type: ignore[import-untyped]
from stdnum.iso7064 import mod_97_10 as _mod_97_10  # type: ignore[import-untyped]


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc


ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
"""Crockford base32: a symbol's value is its index."""

DATA_LENGTH = 8
PARITY_LENGTH = 4
CODEWORD_LENGTH = DATA_LENGTH + PARITY_LENGTH
CODE_LENGTH = 4 + CODEWORD_LENGTH
RF_MAX_CODEWORD_LENGTH = 21
"""The most symbols an ISO 11649 reference holds after RFpp."""

RS_PRIMITIVE_POLYNOMIAL = 0x25
RS_GENERATOR = 2
RS_FCR = 1

MIN_HAMMING_DISTANCE = 8
"""Minimum Hamming distance between the codewords of two issued codes."""

MIN_LEVENSHTEIN_DISTANCE = 4
"""Minimum Levenshtein distance between the codewords of two issued codes."""

MAX_GENERATION_ATTEMPTS = 10

MAX_HAMMING_CORRECTION = 3
MAX_EDIT_CORRECTION = 2

_LOOKALIKES = str.maketrans({"O": "0", "I": "1", "L": "1"})
_SEPARATORS = _re.compile(r"[\s\-]+")
_RF_START = _re.compile(r"R\s*F\s*([0-9OIL])\s*([0-9OIL])", _re.IGNORECASE)
_CODEWORD_CHAR = _re.compile(r"[0-9A-Z]")

RS_MAX_LENGTH = len(ALPHABET) - 1
"""The longest Reed-Solomon code over GF(32): data and parity together."""


@_functools.cache
def _codec(parity_length: int) -> _typing.Any:
    """The Reed-Solomon codec with parity_length parity symbols.

    reedsolo keeps its Galois field tables in module globals, so all codecs
    of the process use the one field GF(32); they differ only in the number
    of parity symbols.
    """
    if not 1 <= parity_length < RS_MAX_LENGTH:
        raise ValueError(
            f"parity_length must be 1 to {RS_MAX_LENGTH - 1}, got {parity_length}"
        )
    return _reedsolo.RSCodec(
        nsym=parity_length,
        c_exp=5,
        prim=RS_PRIMITIVE_POLYNOMIAL,
        generator=RS_GENERATOR,
        fcr=RS_FCR,
    )


class PaymentCodeError(Exception):
    """No payment code could be issued."""


def _symbols(codeword: str) -> bytearray:
    return bytearray(ALPHABET.index(char) for char in codeword)


def _codeword_from_symbols(symbols: _collections_abc.Iterable[int]) -> str:
    return "".join(ALPHABET[value] for value in symbols)


def encode_rs_codeword(data: str, *, parity_length: int = PARITY_LENGTH) -> str:
    """The data symbols followed by parity_length Reed-Solomon parity symbols.

    >>> encode_rs_codeword("K7M3QX9P")
    'K7M3QX9PNFK3'
    >>> encode_rs_codeword("K7M3", parity_length=2)
    'K7M395'
    """
    if (
        not data
        or len(data) + parity_length > RS_MAX_LENGTH
        or any(char not in ALPHABET for char in data)
    ):
        raise ValueError(
            f"expected 1 to {RS_MAX_LENGTH - parity_length} symbols of {ALPHABET}, got {data!r}"
        )
    return _codeword_from_symbols(_codec(parity_length).encode(_symbols(data)))


def check_digits(codeword: str) -> str:
    """The ISO 11649 check digits for a codeword."""
    return _mod_97_10.calc_check_digits(codeword + "RF")


def code_from_codeword(codeword: str) -> str:
    """The electronic form ``RFpp`` + codeword."""
    return "RF" + check_digits(codeword) + codeword


def format_payment_code(code: str) -> str:
    """The printed form: groups of four.

    >>> format_payment_code("rf75k7m3-qx9p-nfk3")
    'RF75 K7M3 QX9P NFK3'
    """
    compact = compact_payment_code(code)
    return " ".join(compact[i : i + 4] for i in range(0, len(compact), 4))


def compact_payment_code(code: str) -> str:
    """Upper case, without spaces and hyphens, look-alikes mapped in the codeword
    and the check digits (``O`` to ``0``, ``I``/``L`` to ``1``)."""
    compact = _SEPARATORS.sub("", code).upper()
    if compact.startswith("RF"):
        return "RF" + compact[2:].translate(_LOOKALIKES)
    return compact.translate(_LOOKALIKES)


def codeword_of(code: str) -> str:
    """The codeword of a code in electronic form."""
    return compact_payment_code(code)[4:]


def is_valid_rs_codeword(
    code: str, *, data_length: int = DATA_LENGTH, parity_length: int = PARITY_LENGTH
) -> bool:
    """Whether code is a Reed-Solomon codeword of data_length data and
    parity_length parity symbols."""
    if len(code) != data_length + parity_length or any(
        char not in ALPHABET for char in code
    ):
        return False
    return bool(_codec(parity_length).check(_symbols(code))[0])


def is_valid_payment_code(
    code: str, *, data_length: int = DATA_LENGTH, parity_length: int = PARITY_LENGTH
) -> bool:
    """Whether a code has valid RF check digits and a valid codeword of
    data_length data and parity_length parity symbols."""
    compact = compact_payment_code(code)
    return (
        len(compact) == 4 + data_length + parity_length
        and _iso11649.is_valid(compact)
        and is_valid_rs_codeword(
            compact[4:], data_length=data_length, parity_length=parity_length
        )
    )


def _distances(code: str, issued: _collections_abc.Iterable[str]) -> tuple[int, int]:
    """The smallest Hamming and Levenshtein distances of code to the issued
    codes; one more than the code's length where there is none to compare
    with. Hamming counts only codes of the same length."""
    hamming = levenshtein = len(code) + 1
    for other in issued:
        if len(other) == len(code):
            hamming = min(hamming, _Hamming.distance(code, other))
        levenshtein = min(levenshtein, _Levenshtein.distance(code, other))
    return hamming, levenshtein


def generate_rs_codeword(
    *,
    data_length: int = DATA_LENGTH,
    parity_length: int = PARITY_LENGTH,
    random_data: _collections_abc.Callable[[], str] | None = None,
) -> str:
    """data_length random Crockford base32 symbols followed by parity_length
    Reed-Solomon parity symbols, without the RF prefix.

    ``random_data`` returns the data symbols instead of a random draw.
    """
    data = (
        random_data()
        if random_data
        else "".join(_secrets.choice(ALPHABET) for _ in range(data_length))
    )
    if len(data) != data_length:
        raise ValueError(f"expected {data_length} data symbols, got {data!r}")
    return encode_rs_codeword(data, parity_length=parity_length)


def generate_distinct_rs_codeword(
    issued_codes: _collections_abc.Iterable[str] = (),
    *,
    data_length: int = DATA_LENGTH,
    parity_length: int = PARITY_LENGTH,
    min_hamming_distance: int = MIN_HAMMING_DISTANCE,
    min_levenshtein_distance: int = MIN_LEVENSHTEIN_DISTANCE,
    max_attempts: int = MAX_GENERATION_ATTEMPTS,
    raise_when_too_close: bool = False,
    random_data: _collections_abc.Callable[[], str] | None = None,
) -> str:
    """A new code as :func:`generate_rs_codeword` makes it, far enough from every
    issued code.

    Draws until the code has at least ``min_hamming_distance`` and
    ``min_levenshtein_distance`` to every code in ``issued_codes``. After
    ``max_attempts`` draws without such a code it returns the draw farthest
    from the issued codes (largest Hamming distance first, then largest
    Levenshtein distance), or raises :class:`PaymentCodeError` with
    ``raise_when_too_close``. A code equal to an issued one is never
    returned: then it raises in any case.

    The default distances suit the default lengths; a Hamming distance above
    the code length can never be reached.
    """
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be at least 1, got {max_attempts}")
    issued = [_SEPARATORS.sub("", code).upper() for code in issued_codes]
    best_code = ""
    best_distances = (-1, -1)
    for _ in range(max_attempts):
        code = generate_rs_codeword(
            data_length=data_length,
            parity_length=parity_length,
            random_data=random_data,
        )
        distances = _distances(code, issued)
        if (
            distances[0] >= min_hamming_distance
            and distances[1] >= min_levenshtein_distance
        ):
            return code
        if distances > best_distances:
            best_code, best_distances = code, distances
    if raise_when_too_close or best_distances[1] == 0:
        raise PaymentCodeError(
            f"no code far enough from {len(issued)} issued codes in {max_attempts} attempts"
        )
    return best_code


def generate_distinct_payment_code(
    issued_codes: _collections_abc.Iterable[str] = (),
    *,
    data_length: int = DATA_LENGTH,
    parity_length: int = PARITY_LENGTH,
    min_hamming_distance: int = MIN_HAMMING_DISTANCE,
    min_levenshtein_distance: int = MIN_LEVENSHTEIN_DISTANCE,
    max_attempts: int = MAX_GENERATION_ATTEMPTS,
    raise_when_too_close: bool = False,
    random_data: _collections_abc.Callable[[], str] | None = None,
) -> str:
    """A new payment code far enough from every issued code: the codeword comes
    from :func:`generate_distinct_rs_codeword`, compared with the codewords of
    ``issued_codes``, with the RF prefix around it.

    An RF reference holds at most :data:`RF_MAX_CODEWORD_LENGTH` symbols after
    its check digits, so data and parity together may not be longer.
    Finding and matching typed codes (:func:`find_payment_codes`,
    :func:`match_payment_code`, :func:`is_valid_payment_code`) know the
    default lengths only.
    """
    if data_length + parity_length > RF_MAX_CODEWORD_LENGTH:
        raise ValueError(
            f"an RF reference holds at most {RF_MAX_CODEWORD_LENGTH} symbols, "
            f"got {data_length} + {parity_length}"
        )
    codeword = generate_distinct_rs_codeword(
        [codeword_of(code) for code in issued_codes],
        data_length=data_length,
        parity_length=parity_length,
        min_hamming_distance=min_hamming_distance,
        min_levenshtein_distance=min_levenshtein_distance,
        max_attempts=max_attempts,
        raise_when_too_close=raise_when_too_close,
        random_data=random_data,
    )
    return code_from_codeword(codeword)


@_dataclasses.dataclass(frozen=True)
class TypedPaymentCode:
    """A payment code as found in a text: the check digits and the codeword
    candidates after the ``RF``, the one of the expected length first, then
    the shorter and longer ones (a missing or extra symbol)."""

    text: str
    check_digits: str
    codewords: tuple[str, ...]


def find_payment_codes(
    text: str,
    *,
    data_length: int = DATA_LENGTH,
    parity_length: int = PARITY_LENGTH,
    max_edit_correction: int = MAX_EDIT_CORRECTION,
) -> list[TypedPaymentCode]:
    """The payment codes in a remittance text, tolerating spaces, hyphens,
    lower case and look-alike characters.

    Each found code offers its codeword of ``data_length + parity_length``
    symbols and codewords up to ``max_edit_correction`` symbols shorter or
    longer.
    """
    codeword_length = data_length + parity_length
    found: list[TypedPaymentCode] = []
    for match in _RF_START.finditer(text):
        symbols: list[str] = []
        end = match.end()
        for index in range(match.end(), len(text)):
            char = text[index]
            if _SEPARATORS.fullmatch(char):
                continue
            upper = char.upper().translate(_LOOKALIKES)
            if (
                not _CODEWORD_CHAR.fullmatch(upper)
                or len(symbols) == codeword_length + max_edit_correction
            ):
                break
            symbols.append(upper)
            end = index + 1
        joined = "".join(symbols)
        lengths = [codeword_length]
        for difference in range(1, max_edit_correction + 1):
            lengths += [codeword_length - difference, codeword_length + difference]
        codewords = tuple(
            joined[:length] for length in lengths if 0 < length <= len(joined)
        )
        if codewords:
            digits = (match.group(1) + match.group(2)).upper().translate(_LOOKALIKES)
            found.append(
                TypedPaymentCode(
                    text=text[match.start() : end],
                    check_digits=digits,
                    codewords=codewords,
                )
            )
    return found


MatchMethod = _typing.Literal["exact", "reed_solomon", "hamming", "edit"]


@_dataclasses.dataclass(frozen=True)
class PaymentCodeMatch:
    """The issued code a typed code stands for.

    ``code`` is the issued code (electronic form) or ``None``; ``method``
    says how it was found and ``corrections`` how many symbols were changed.
    ``ambiguous`` lists the issued codes that fit equally well where more
    than one did. ``corrected`` matches are to be reviewed by a person.
    """

    typed: str
    code: str | None
    method: MatchMethod | None
    corrections: int = 0
    check_digits_ok: bool = False
    ambiguous: tuple[str, ...] = ()

    @property
    def corrected(self) -> bool:
        return self.method not in (None, "exact")


def _nearest(
    codeword: str,
    issued: dict[str, str],
    distance: _collections_abc.Callable[[str, str], int],
    limit: int,
    same_length: bool,
) -> tuple[int, list[str]]:
    best = limit + 1
    nearest: list[str] = []
    for other in issued:
        if same_length and len(other) != len(codeword):
            continue
        value = distance(codeword, other)
        if value < best:
            best, nearest = value, [other]
        elif value == best:
            nearest.append(other)
    return best, nearest


def match_payment_code(
    typed: str | TypedPaymentCode,
    issued_codes: _collections_abc.Iterable[str],
    *,
    data_length: int = DATA_LENGTH,
    parity_length: int = PARITY_LENGTH,
    max_hamming_correction: int = MAX_HAMMING_CORRECTION,
    max_edit_correction: int = MAX_EDIT_CORRECTION,
) -> PaymentCodeMatch:
    """The issued code a typed payment code stands for.

    ``typed`` is a code as typed (``RF75 K7M3 …``) or one found by
    :func:`find_payment_codes`. The lengths are those the codes were issued
    with; Reed-Solomon corrects up to ``parity_length // 2`` wrong symbols,
    the nearest issued code is taken up to ``max_hamming_correction`` wrong
    symbols or ``max_edit_correction`` edits. The distances kept when issuing
    decide how many of these corrections are unique.
    """
    codeword_length = data_length + parity_length
    if isinstance(typed, str):
        found = find_payment_codes(
            typed,
            data_length=data_length,
            parity_length=parity_length,
            max_edit_correction=max_edit_correction,
        )
        if not found:
            return PaymentCodeMatch(typed=typed, code=None, method=None)
        typed = found[0]
    issued = {codeword_of(code): compact_payment_code(code) for code in issued_codes}

    def result(
        codeword: str, method: MatchMethod, corrections: int
    ) -> PaymentCodeMatch:
        return PaymentCodeMatch(
            typed=typed.text,
            code=issued[codeword],
            method=method,
            corrections=corrections,
            check_digits_ok=typed.check_digits == check_digits(codeword),
        )

    def ambiguous(codewords: list[str]) -> PaymentCodeMatch:
        return PaymentCodeMatch(
            typed=typed.text,
            code=None,
            method=None,
            ambiguous=tuple(issued[other] for other in codewords),
        )

    codeword = typed.codewords[0]
    full_length = len(codeword) == codeword_length
    if full_length and codeword in issued:
        return result(codeword, "exact", 0)

    if full_length and all(char in ALPHABET for char in codeword):
        try:
            _, corrected_symbols, errata = _codec(parity_length).decode(
                _symbols(codeword)
            )
        except _reedsolo.ReedSolomonError:
            pass
        else:
            corrected = _codeword_from_symbols(corrected_symbols)
            if corrected in issued:
                return result(corrected, "reed_solomon", len(errata))

    if full_length:
        best, nearest = _nearest(
            codeword,
            issued,
            _Hamming.distance,
            max_hamming_correction,
            same_length=True,
        )
        if len(nearest) == 1:
            return result(nearest[0], "hamming", best)
        if len(nearest) > 1:
            return ambiguous(nearest)

    candidates: dict[str, int] = {}
    for candidate_codeword in typed.codewords:
        best, nearest = _nearest(
            candidate_codeword,
            issued,
            _DamerauLevenshtein.distance,
            max_edit_correction,
            same_length=False,
        )
        for other in nearest:
            candidates[other] = min(best, candidates.get(other, best))
    if candidates:
        best = min(candidates.values())
        nearest = [other for other, value in candidates.items() if value == best]
        if len(nearest) == 1:
            return result(nearest[0], "edit", best)
        return ambiguous(nearest)
    return PaymentCodeMatch(typed=typed.text, code=None, method=None)


def payment_code_test_vectors(count: int = 40) -> list[dict[str, str]]:
    """Deterministic data, codeword, code and printed form for cross-checking
    other implementations (the wagon's ``Wsjrdp2027::PaymentCode``)."""
    data_list = ["00000000", "ZZZZZZZZ", "K7M3QX9P", "0123456Z"]
    state = 20271
    for _ in range(count):
        symbols = []
        for _ in range(DATA_LENGTH):
            state = (state * 1103515245 + 12345) % 2**31
            symbols.append(ALPHABET[(state >> 16) % len(ALPHABET)])
        data_list.append("".join(symbols))
    vectors = []
    for data in data_list:
        codeword = encode_rs_codeword(data)
        code = code_from_codeword(codeword)
        vectors.append(
            {
                "data": data,
                "codeword": codeword,
                "code": code,
                "printed": format_payment_code(code),
            }
        )
    return vectors
