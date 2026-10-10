"""Where a person's individual installment plan comes from, and that a plan
paid by credit transfer is never collected."""

import datetime
from decimal import Decimal

import pandas
import pytest
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


class Test_Skip_Payments_Not_To_Collect:
    @staticmethod
    def _df(*people):
        defaults = {
            "installments_payment_method": "direct_debit",
            "sepa_status": "ok",
            "contract_status": "confirmed",
            "status": "confirmed",
            "payment_status": "ok",
            "payment_status_reason": "",
        }
        return pandas.DataFrame(
            [{"id": i, **defaults, **p} for i, p in enumerate(people, start=1)]
        )

    def test_collects_a_contract_in_force_by_direct_debit_with_an_ok_mandate(self):
        df = self._df({}, {"status": "printed"}, {"sepa_status": None})

        _payment._skip_payments_not_to_collect(df)

        assert list(df["payment_status"]) == ["ok", "ok", "ok"]

    @pytest.mark.parametrize(
        "person, reason",
        [
            (
                {"installments_payment_method": "credit_transfer"},
                "payment_method=credit_transfer",
            ),
            ({"installments_payment_method": None}, "payment_method=None"),
            ({"sepa_status": "in_review"}, "sepa_status=in_review"),
            ({"contract_status": "none"}, "contract_status=none"),
            ({"contract_status": "ended"}, "contract_status=ended"),
            ({"status": "deregistration_noted"}, "status=deregistration_noted"),
            ({"status": "deregistered"}, "status=deregistered"),
        ],
    )
    def test_skips(self, person, reason):
        df = self._df(person)

        _payment._skip_payments_not_to_collect(df)

        assert list(df["payment_status"]) == ["skipped"]
        assert list(df["payment_status_reason"]) == [reason]

    def test_keeps_an_earlier_reason_and_joins_several(self):
        df = self._df(
            {
                "payment_status": "skipped",
                "payment_status_reason": "amount = 0",
                "installments_payment_method": "credit_transfer",
                "sepa_status": "in_review",
            }
        )

        _payment._skip_payments_not_to_collect(df)

        assert list(df["payment_status_reason"]) == [
            "amount = 0, payment_method=credit_transfer, sepa_status=in_review"
        ]


class Test_Report_Contract_Without_Confirmed_Status:
    def test_lists_collected_people_whose_status_is_not_confirmed(self):
        df = pandas.DataFrame(
            {
                "id": [1, 2, 3],
                "status": ["confirmed", "printed", "printed"],
                "contract_status": ["confirmed", "confirmed", "confirmed"],
                "payment_status": ["ok", "ok", "skipped"],
                "open_amount_cents": [100, 200, 300],
            }
        )

        rows = _payment.report_contract_without_confirmed_status(df)

        assert list(rows["id"]) == [2]
