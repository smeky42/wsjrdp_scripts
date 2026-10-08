import pytest
from wsjrdp2027._payment_role import PaymentRole


REGULAR_PAYER_ROLES = [role for role in PaymentRole if role.is_regular_payer]


@pytest.mark.parametrize("role", REGULAR_PAYER_ROLES, ids=lambda role: role.name)
def test_reduction_leaves_the_role_plan_alone(role):
    """A reduction takes installments off one person's plan only: a later call
    without a reduction gets the role's full plan again."""
    full = role.get_installments_eur()
    if not full:
        pytest.skip("role without installments")

    reduced = role.get_installments_eur(fee_reduction_eur=sum(full.values()) // 2)

    assert sum(reduced.values()) < sum(full.values())
    assert role.get_installments_eur() == full


def test_reduction_takes_installments_off_from_the_end():
    role = PaymentRole.REGULAR_PAYER_IST
    full = role.get_installments_eur()

    assert role.get_installments_eur(fee_reduction_eur=1250) == {
        (2025, 12): 200,
        (2026, 1): 400,
        (2026, 2): 400,
        (2026, 3): 350,
    }
    assert sum(full.values()) - 1250 == 1350
    assert role.get_installments_eur() == full


def test_reduction_of_the_whole_fee_leaves_no_installment_and_the_plan_alone():
    role = PaymentRole.REGULAR_PAYER_YP
    full = role.get_installments_eur()

    assert role.get_installments_eur(fee_reduction_eur=sum(full.values())) == {}
    assert role.get_installments_eur() == full


def test_reduction_in_cents_leaves_the_role_plan_alone():
    role = PaymentRole.REGULAR_PAYER_UL
    full = role.get_installments_cents()

    role.get_installments_cents(fee_reduction_cents=50_050)

    assert role.get_installments_cents() == full
