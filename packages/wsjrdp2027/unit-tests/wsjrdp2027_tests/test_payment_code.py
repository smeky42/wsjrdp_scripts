"""Payment codes (wsjrdp2027._payment_code): format, issuing with minimum
distances, and matching typed codes against issued ones."""

import json
import pathlib
import random

import pytest
from wsjrdp2027 import _payment_code as pc


VECTORS_FILE = pathlib.Path(__file__).parent / "data" / "payment_code_vectors.json"


def _code(data: str) -> str:
    return pc.code_from_codeword(pc.encode_rs_codeword(data))


def _change(code: str, positions: dict[int, str]) -> str:
    """The code with the codeword symbols at positions replaced."""
    codeword = list(pc.codeword_of(code))
    for position, symbol in positions.items():
        codeword[position] = symbol
    return code[:4] + "".join(codeword)


def _other(symbol: str) -> str:
    return pc.ALPHABET[(pc.ALPHABET.index(symbol) + 7) % len(pc.ALPHABET)]


class TestFormat:
    def test_vectors_file_matches_the_implementation(self) -> None:
        assert json.loads(VECTORS_FILE.read_text()) == pc.payment_code_test_vectors()

    def test_codes_are_valid_rf_references_with_a_valid_codeword(self) -> None:
        for vector in pc.payment_code_test_vectors():
            assert pc.is_valid_payment_code(vector["code"])
            assert pc.is_valid_payment_code(vector["printed"])
            assert len(vector["code"]) == pc.CODE_LENGTH
            assert vector["codeword"].startswith(vector["data"])

    def test_compact_maps_look_alikes_and_drops_separators(self) -> None:
        assert pc.compact_payment_code("rf75 k7m3-qx9p nfk3") == "RF75K7M3QX9PNFK3"
        assert pc.compact_payment_code("RFO4 OOOO OOOO OOOO") == "RF04000000000000"
        assert pc.compact_payment_code("RFI5") == "RF15"

    def test_a_wrong_symbol_or_check_digit_is_not_valid(self) -> None:
        code = _code("K7M3QX9P")
        assert not pc.is_valid_payment_code(
            _change(code, {3: _other(pc.codeword_of(code)[3])})
        )
        assert not pc.is_valid_payment_code("RF00" + pc.codeword_of(code))
        assert not pc.is_valid_payment_code(code[:-1])

    def test_bodies_of_two_codes_differ_in_at_least_five_symbols(self) -> None:
        codewords = [vector["codeword"] for vector in pc.payment_code_test_vectors()]
        for i, first in enumerate(codewords):
            for second in codewords[i + 1 :]:
                assert sum(a != b for a, b in zip(first, second, strict=True)) >= 5

    def test_encode_refuses_wrong_data(self) -> None:
        with pytest.raises(ValueError):
            pc.encode_rs_codeword("")
        with pytest.raises(ValueError):
            pc.encode_rs_codeword("K" * 28)
        with pytest.raises(ValueError):
            pc.encode_rs_codeword("K7M3QX9U")


class TestGenerate:
    def test_a_new_code_keeps_the_distances_to_the_issued_ones(self) -> None:
        rng = random.Random(1)
        issued: list[str] = []
        for _ in range(200):
            draw = lambda: "".join(
                rng.choice(pc.ALPHABET) for _ in range(pc.DATA_LENGTH)
            )  # noqa: E731
            issued.append(pc.generate_distinct_payment_code(issued, random_data=draw))
        codewords = [pc.codeword_of(code) for code in issued]
        for i, first in enumerate(codewords):
            for second in codewords[i + 1 :]:
                assert (
                    sum(a != b for a, b in zip(first, second, strict=True))
                    >= pc.MIN_HAMMING_DISTANCE
                )

    def test_a_draw_too_close_to_an_issued_code_is_drawn_again(self) -> None:
        issued = _code("K7M3QX9P")
        draws = iter(["K7M3QX9P", "K7M3QX9Q", "00000000"])
        assert pc.generate_distinct_payment_code(
            [issued], random_data=lambda: next(draws)
        ) == _code("00000000")

    def test_after_the_last_draw_it_takes_the_farthest_one(self) -> None:
        issued = _code("K7M3QX9P")
        draws = iter(["K7M3QX9Q", "K7M3Q000", "K7M3QX90"])
        code = pc.generate_distinct_payment_code(
            [issued], max_attempts=3, random_data=lambda: next(draws)
        )
        assert code == _code("K7M3Q000")

    def test_after_the_last_draw_it_raises_when_asked_to(self) -> None:
        issued = _code("K7M3QX9P")
        with pytest.raises(pc.PaymentCodeError):
            pc.generate_distinct_payment_code(
                [issued],
                max_attempts=3,
                raise_when_too_close=True,
                random_data=lambda: "K7M3QX9Q",
            )

    def test_it_never_returns_an_issued_code(self) -> None:
        issued = _code("K7M3QX9P")
        with pytest.raises(pc.PaymentCodeError):
            pc.generate_distinct_payment_code([issued], random_data=lambda: "K7M3QX9P")

    def test_the_distances_are_configurable(self) -> None:
        issued = _code("K7M3QX9P")
        draws = iter(["K7M3QX9Q", "00000000"])
        code = pc.generate_distinct_payment_code(
            [issued],
            min_hamming_distance=1,
            min_levenshtein_distance=1,
            raise_when_too_close=True,
            random_data=lambda: next(draws),
        )
        assert code == _code("K7M3QX9Q")

    def test_other_lengths(self) -> None:
        code = pc.generate_distinct_payment_code(data_length=10, parity_length=6)
        assert len(code) == 4 + 16
        assert pc.is_valid_rs_codeword(
            pc.codeword_of(code), data_length=10, parity_length=6
        )
        assert pc.compact_payment_code(code)[2:4] == pc.check_digits(
            pc.codeword_of(code)
        )

    def test_the_codeword_fits_an_rf_reference(self) -> None:
        with pytest.raises(ValueError):
            pc.generate_distinct_payment_code(data_length=16, parity_length=6)

    def test_it_needs_at_least_one_attempt(self) -> None:
        with pytest.raises(ValueError):
            pc.generate_distinct_payment_code(max_attempts=0)

    def test_without_issued_codes_any_draw_is_taken(self) -> None:
        assert pc.is_valid_payment_code(pc.generate_distinct_payment_code())


class TestFind:
    def test_it_finds_a_code_between_other_text(self) -> None:
        code = _code("K7M3QX9P")
        found = pc.find_payment_codes(
            f"TN 791 AB1 RZ1 {pc.format_payment_code(code)} Rueckzahlung"
        )
        assert [item.codewords[0] for item in found] == [pc.codeword_of(code)]
        assert found[0].check_digits == code[2:4]

    def test_it_tolerates_case_hyphens_and_look_alikes(self) -> None:
        found = pc.find_payment_codes("rf75-k7m3-qx9p-nfk3")
        assert found[0].codewords[0] == "K7M3QX9PNFK3"

    def test_it_offers_shorter_and_longer_codewords(self) -> None:
        found = pc.find_payment_codes("RF75 K7M3 QX9P NFK3 X")
        assert found[0].codewords == (
            "K7M3QX9PNFK3",
            "K7M3QX9PNFK",
            "K7M3QX9PNFK3X",
            "K7M3QX9PNF",
        )

    def test_it_takes_other_lengths(self) -> None:
        found = pc.find_payment_codes(
            "RF12 ABCD EFGH J X", data_length=6, parity_length=3, max_edit_correction=1
        )
        assert found[0].codewords == ("ABCDEFGHJ", "ABCDEFGH", "ABCDEFGHJX")

    def test_nothing_without_rf(self) -> None:
        assert pc.find_payment_codes("Beitrag 2027") == []


class TestMatch:
    @pytest.fixture
    def issued(self) -> list[str]:
        rng = random.Random(7)
        codes: list[str] = []
        for _ in range(300):
            codes.append(
                pc.generate_distinct_payment_code(
                    codes,
                    random_data=lambda: "".join(
                        rng.choice(pc.ALPHABET) for _ in range(pc.DATA_LENGTH)
                    ),
                )
            )
        return codes

    def test_exact(self, issued: list[str]) -> None:
        match = pc.match_payment_code(pc.format_payment_code(issued[5]), issued)
        assert (match.code, match.method, match.corrected, match.check_digits_ok) == (
            issued[5],
            "exact",
            False,
            True,
        )

    def test_wrong_check_digits_still_find_the_codeword(
        self, issued: list[str]
    ) -> None:
        match = pc.match_payment_code("RF00" + pc.codeword_of(issued[5]), issued)
        assert (match.code, match.method, match.check_digits_ok) == (
            issued[5],
            "exact",
            False,
        )

    def test_two_wrong_symbols_are_corrected_by_reed_solomon(
        self, issued: list[str]
    ) -> None:
        codeword = pc.codeword_of(issued[9])
        typed = _change(issued[9], {1: _other(codeword[1]), 10: _other(codeword[10])})
        match = pc.match_payment_code(typed, issued)
        assert (match.code, match.method, match.corrections, match.corrected) == (
            issued[9],
            "reed_solomon",
            2,
            True,
        )

    def test_a_swap_is_corrected(self, issued: list[str]) -> None:
        codeword = pc.codeword_of(issued[11])
        swapped = next(
            i for i in range(len(codeword) - 1) if codeword[i] != codeword[i + 1]
        )
        typed = _change(
            issued[11], {swapped: codeword[swapped + 1], swapped + 1: codeword[swapped]}
        )
        assert pc.match_payment_code(typed, issued).code == issued[11]

    def test_three_wrong_symbols_are_corrected_by_the_nearest_issued_code(
        self, issued: list[str]
    ) -> None:
        codeword = pc.codeword_of(issued[20])
        typed = _change(
            issued[20],
            {0: _other(codeword[0]), 5: _other(codeword[5]), 11: _other(codeword[11])},
        )
        match = pc.match_payment_code(typed, issued)
        assert (match.code, match.method, match.corrections) == (
            issued[20],
            "hamming",
            3,
        )

    def test_a_missing_or_extra_symbol_is_corrected_by_edits(
        self, issued: list[str]
    ) -> None:
        codeword = pc.codeword_of(issued[30])
        missing = pc.match_payment_code(
            "RF" + issued[30][2:4] + codeword[:4] + codeword[5:], issued
        )
        extra = pc.match_payment_code(
            "RF" + issued[30][2:4] + codeword[:6] + "X" + codeword[6:], issued
        )
        assert (missing.code, missing.method) == (issued[30], "edit")
        assert (extra.code, extra.method) == (issued[30], "edit")

    def test_too_many_errors_match_nothing(self, issued: list[str]) -> None:
        codeword = pc.codeword_of(issued[40])
        typed = _change(issued[40], {i: _other(codeword[i]) for i in range(0, 12, 2)})
        match = pc.match_payment_code(typed, issued)
        assert (match.code, match.method) == (None, None)

    def test_a_code_equally_near_to_two_issued_codes_is_ambiguous(self) -> None:
        first = _code("00000000")
        codeword = pc.codeword_of(first)
        second = first[:4] + "".join(
            _other(char) if i < 6 else char for i, char in enumerate(codeword)
        )
        typed = first[:4] + "".join(
            _other(char) if i < 3 else char for i, char in enumerate(codeword)
        )
        match = pc.match_payment_code(typed, [first, second])
        assert match.code is None
        assert set(match.ambiguous) == {first, second}

    def test_no_code_in_the_text(self) -> None:
        assert pc.match_payment_code("Beitrag 2027", []).method is None


class TestRsCode:
    def test_default_lengths_are_those_of_a_payment_code_codeword(self) -> None:
        code = pc.generate_rs_codeword(random_data=lambda: "K7M3QX9P")
        assert code == pc.encode_rs_codeword("K7M3QX9P")
        assert pc.is_valid_rs_codeword(code)

    @pytest.mark.parametrize(
        ("data_length", "parity_length"), [(4, 2), (6, 3), (10, 6), (20, 11)]
    )
    def test_other_lengths(self, data_length: int, parity_length: int) -> None:
        code = pc.generate_rs_codeword(
            data_length=data_length, parity_length=parity_length
        )
        assert len(code) == data_length + parity_length
        assert pc.is_valid_rs_codeword(
            code, data_length=data_length, parity_length=parity_length
        )
        broken = _other(code[0]) + code[1:]
        assert not pc.is_valid_rs_codeword(
            broken, data_length=data_length, parity_length=parity_length
        )

    def test_codecs_of_other_lengths_leave_the_payment_code_unchanged(self) -> None:
        pc.generate_rs_codeword(data_length=6, parity_length=3)
        assert json.loads(VECTORS_FILE.read_text()) == pc.payment_code_test_vectors()

    def test_lengths_beyond_the_field_are_refused(self) -> None:
        with pytest.raises(ValueError):
            pc.generate_rs_codeword(data_length=28, parity_length=4)
        with pytest.raises(ValueError):
            pc.generate_rs_codeword(data_length=8, parity_length=0)
        with pytest.raises(ValueError):
            pc.generate_rs_codeword(data_length=8, random_data=lambda: "K7M3")

    def test_distinct_codes_keep_the_distances(self) -> None:
        rng = random.Random(3)
        issued: list[str] = []
        for _ in range(100):
            issued.append(
                pc.generate_distinct_rs_codeword(
                    issued,
                    data_length=6,
                    parity_length=3,
                    min_hamming_distance=5,
                    min_levenshtein_distance=3,
                    raise_when_too_close=True,
                    random_data=lambda: "".join(
                        rng.choice(pc.ALPHABET) for _ in range(6)
                    ),
                )
            )
        for i, first in enumerate(issued):
            assert pc.is_valid_rs_codeword(first, data_length=6, parity_length=3)
            for second in issued[i + 1 :]:
                assert sum(a != b for a, b in zip(first, second, strict=True)) >= 5

    def test_distinct_code_falls_back_to_the_farthest_draw(self) -> None:
        issued = pc.encode_rs_codeword("K7M3", parity_length=2)
        draws = iter(["K7M4", "00Z0"])
        code = pc.generate_distinct_rs_codeword(
            [issued],
            data_length=4,
            parity_length=2,
            max_attempts=2,
            random_data=lambda: next(draws),
        )
        assert code == max(
            (pc.encode_rs_codeword(d, parity_length=2) for d in ["K7M4", "00Z0"]),
            key=lambda c: (sum(a != b for a, b in zip(c, issued, strict=True)),),
        )


class TestOtherLengths:
    DATA = 10
    PARITY = 6

    @pytest.fixture
    def issued(self) -> list[str]:
        rng = random.Random(11)
        codes: list[str] = []
        for _ in range(100):
            codes.append(
                pc.generate_distinct_payment_code(
                    codes,
                    data_length=self.DATA,
                    parity_length=self.PARITY,
                    min_hamming_distance=10,
                    min_levenshtein_distance=6,
                    raise_when_too_close=True,
                    random_data=lambda: "".join(
                        rng.choice(pc.ALPHABET) for _ in range(self.DATA)
                    ),
                )
            )
        return codes

    def lengths(self) -> dict[str, int]:
        return {"data_length": self.DATA, "parity_length": self.PARITY}

    def test_valid(self, issued: list[str]) -> None:
        assert all(pc.is_valid_payment_code(code, **self.lengths()) for code in issued)
        assert not pc.is_valid_payment_code(issued[0])

    def test_exact(self, issued: list[str]) -> None:
        match = pc.match_payment_code(
            pc.format_payment_code(issued[3]), issued, **self.lengths()
        )
        assert (match.code, match.method) == (issued[3], "exact")

    def test_reed_solomon_corrects_three_symbols(self, issued: list[str]) -> None:
        codeword = pc.codeword_of(issued[4])
        typed = _change(
            issued[4],
            {0: _other(codeword[0]), 7: _other(codeword[7]), 15: _other(codeword[15])},
        )
        match = pc.match_payment_code(typed, issued, **self.lengths())
        assert (match.code, match.method, match.corrections) == (
            issued[4],
            "reed_solomon",
            3,
        )

    def test_nearest_corrects_four_symbols(self, issued: list[str]) -> None:
        codeword = pc.codeword_of(issued[5])
        typed = _change(issued[5], {i: _other(codeword[i]) for i in (1, 5, 9, 13)})
        match = pc.match_payment_code(
            typed, issued, max_hamming_correction=4, **self.lengths()
        )
        assert (match.code, match.method, match.corrections) == (
            issued[5],
            "hamming",
            4,
        )

    def test_a_missing_symbol(self, issued: list[str]) -> None:
        codeword = pc.codeword_of(issued[6])
        typed = issued[6][:4] + codeword[:8] + codeword[9:]
        match = pc.match_payment_code(typed, issued, **self.lengths())
        assert (match.code, match.method) == (issued[6], "edit")
