"""The fee comes from the generated columns of people (wsjrdp_regular_full_fee,
wsjrdp_total_fee), and the standard installment plan follows it."""

import datetime
from decimal import Decimal

import pandas
import pytest
from wsjrdp2027 import _people
from wsjrdp2027._payment_role import PaymentRole


class Test_Fee_Cents_From_The_Columns:
    def test_regular_full_fee_is_the_column_in_cents(self):
        row = pandas.Series({"regular_full_fee_eur": Decimal("3400.000")})

        assert _people._compute_regular_full_fee_cents(row) == 340_000

    def test_total_fee_is_the_column_in_cents_rounded_half_up(self):
        assert (
            _people._compute_total_fee_cents(
                pandas.Series({"total_fee_eur": Decimal("3387.655")})
            )
            == 338_766
        )
        assert (
            _people._compute_total_fee_cents(
                pandas.Series({"total_fee_eur": Decimal("3387.654")})
            )
            == 338_765
        )
        assert (
            _people._compute_total_fee_cents(
                pandas.Series({"total_fee_eur": Decimal("0.000")})
            )
            == 0
        )

    def test_a_row_without_the_column_fails_loudly(self):
        with pytest.raises(KeyError):
            _people._compute_total_fee_cents(
                pandas.Series({"payment_role": PaymentRole.REGULAR_PAYER_YP})
            )


class Test_Effective_Fee_Reduction:
    def test_is_the_gap_between_the_tariff_and_the_fee_to_be_paid(self):
        row = pandas.Series({"total_fee_cents": 300_000})

        assert (
            _people._effective_fee_reduction_cents(row, PaymentRole.REGULAR_PAYER_YP)
            == 40_000
        )

    def test_a_fee_above_the_tariff_is_not_spread_over_the_installments(self):
        row = pandas.Series({"total_fee_cents": 350_000})

        assert (
            _people._effective_fee_reduction_cents(row, PaymentRole.REGULAR_PAYER_YP)
            == 0
        )

    def test_the_standard_plan_brings_in_the_fee_to_be_paid(self):
        row = pandas.Series(
            {
                "id": 1,
                "payment_role": PaymentRole.REGULAR_PAYER_YP,
                "status": "confirmed",
                "early_payer": False,
                "print_at": datetime.date(2025, 7, 1),
                "today": datetime.date(2026, 10, 8),
                "total_fee_cents": 330_000,
            }
        )

        installments = _people._compute_installments_cents_dict_from_row(row, {})

        assert installments is not None
        assert sum(installments.values()) == 330_000
        assert (
            installments[(2027, 5)] == 30_000
        )  # the last installment, 400 less the 100 taken off
