"""Where a person's individual installment plan comes from, and that a plan
paid by credit transfer is never collected."""

import datetime
from decimal import Decimal

import pandas
import wsjrdp2027
from wsjrdp2027 import _payment, _people
from wsjrdp2027._payment_role import PaymentRole


def _people_df() -> pandas.DataFrame:
    return pandas.DataFrame(
        [
            # 1: an active plan of their own, paid by credit transfer.
            {
                "id": 1,
                "wsjrdp_raw_installments_eur": [
                    Decimal(2026),
                    Decimal(0),
                    Decimal("312.500"),
                ],
                "wsjrdp_installments_issue": "HELP-1",
                "wsjrdp_installments_comment": "aktiv",
                "wsjrdp_installments_payment_method": "credit_transfer",
            },
            # 2: an active plan by direct debit.
            {
                "id": 2,
                "wsjrdp_raw_installments_eur": [Decimal(2026), Decimal("500.000")],
                "wsjrdp_installments_issue": None,
                "wsjrdp_installments_comment": None,
                "wsjrdp_installments_payment_method": "direct_debit",
            },
            # 3: no plan of their own.
            {
                "id": 3,
                "wsjrdp_raw_installments_eur": None,
                "wsjrdp_installments_issue": None,
                "wsjrdp_installments_comment": None,
                "wsjrdp_installments_payment_method": None,
            },
        ]
    )


class Test_Id2Active_Plans:
    def test_active_plans_come_from_the_people(self):
        plans = _people._id2active_plans(_people_df())

        assert sorted(plans) == [1, 2]
        assert plans[1] == {
            "id": None,
            "people_id": 1,
            "status": "active",
            "custom_installments_comment": "aktiv",
            "custom_installments_issue": "HELP-1",
            "custom_installments_starting_year": 2026,
            "custom_installments_cents": [0, 31_250],
            "custom_installments_sum_cents": 31_250,
            "custom_installments_payment_method": "credit_transfer",
        }
        assert plans[2]["custom_installments_payment_method"] == "direct_debit"


class Test_Installments_Cents_From_Plan:
    def test_months_from_january_of_the_starting_year_without_empty_ones(self):
        cents = [0] * 11 + [20_000, 0, 31_250] + [0] * 10 + [1]

        assert wsjrdp2027.installments_cents_from_plan(2025, cents) == {
            (2025, 12): 20_000,
            (2026, 2): 31_250,
            (2027, 1): 1,
        }

    def test_no_installment_at_all(self):
        assert wsjrdp2027.installments_cents_from_plan(2026, [0, 0]) == {}


class Test_Installments_From_A_Persons_Plan:
    def _row(self, plan):
        return pandas.Series(
            {
                "id": 1,
                "payment_role": PaymentRole.REGULAR_PAYER_YP,
                "status": "confirmed",
                "early_payer": False,
                "print_at": datetime.date(2025, 7, 1),
                "today": datetime.date(2026, 10, 8),
                "total_fee_cents": 340_000,
            }
        ), {1: plan}

    def test_the_active_plan_gives_the_same_installments_as_its_fee_rule(self):
        person = _people_df().iloc[0]
        active = _people._active_plan_from_person_row(person)
        fee_rule = {
            "custom_installments_starting_year": 2026,
            "custom_installments_cents": [0, 31_250],
        }

        from_person = _people._compute_installments_cents_dict_from_row(
            *self._row(active)
        )
        from_fee_rule = _people._compute_installments_cents_dict_from_row(
            *self._row(fee_rule)
        )

        assert from_person == from_fee_rule == {(2026, 2): 31_250}

    def test_cents_survive_the_euros_exactly(self):
        raw = [
            Decimal(2025),
            *(Decimal(cents) / 100 for cents in range(0, 100_000, 997)),
        ]
        person = pandas.Series({"id": 1, "wsjrdp_raw_installments_eur": raw})

        plan = _people._active_plan_from_person_row(person)

        assert plan is not None
        assert plan["custom_installments_cents"] == list(range(0, 100_000, 997))


class Test_Skip_Credit_Transfer_Payments:
    def test_skips_a_plan_paid_by_credit_transfer_only(self):
        df = pandas.DataFrame(
            {
                "id": [1, 2, 3],
                "installments_payment_method": [
                    "credit_transfer",
                    "direct_debit",
                    "credit_transfer",
                ],
                "payment_status": ["ok", "ok", "skipped"],
                "payment_status_reason": ["", "", "amount = 0"],
            }
        )

        _payment._skip_credit_transfer_payments(df)

        assert list(df["payment_status"]) == ["skipped", "ok", "skipped"]
        assert list(df["payment_status_reason"]) == [
            "payment_method=credit_transfer",
            "",
            "amount = 0, payment_method=credit_transfer",
        ]
