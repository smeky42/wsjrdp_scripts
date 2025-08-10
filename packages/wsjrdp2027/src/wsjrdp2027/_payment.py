from __future__ import annotations

import enum as _enum

_PAYMENT_ARRAY = [
    [
        "Rolle",
        "Gesamt",
        "Dez 2025",
        "Jan 2026",
        "Feb 2026",
        "Mär 2026",
        "Aug 2026",
        "Nov 2026",
        "Feb 2027",
        "Mai 2027",
    ],
    [
        "RegularPayer::Group::Unit::Member",
        "3400",
        "300",
        "500",
        "500",
        "500",
        "400",
        "400",
        "400",
        "400",
    ],
    [
        "RegularPayer::Group::Unit::Leader",
        "2400",
        "150",
        "350",
        "350",
        "350",
        "300",
        "300",
        "300",
        "300",
    ],
    [
        "RegularPayer::Group::Ist::Member",
        "2600",
        "200",
        "400",
        "400",
        "400",
        "300",
        "300",
        "300",
        "300",
    ],
    [
        "RegularPayer::Group::Root::Member",
        "1600",
        "50",
        "250",
        "250",
        "250",
        "200",
        "200",
        "200",
        "200",
    ],
    ["EarlyPayer::Group::Unit::Member", "3400", "", "", "", "", "", "", "", ""],
    ["EarlyPayer::Group::Unit::Leader", "2400", "", "", "", "", "", "", "", ""],
    ["EarlyPayer::Group::Ist::Member", "2600", "", "", "", "", "", "", "", ""],
    ["EarlyPayer::Group::Root::Member", "1600", "", "", "", "", "", "", "", ""],
]


class PaymentRole(_enum.Enum):
    REGULAR_PAYER_CMT = "RegularPayer::Group::Root::Member"
    REGULAR_PAYER_YP = "RegularPayer::Group::Unit::Member"
    REGULAR_PAYER_UL = "RegularPayer::Group::Unit::Leader"
    REGULAR_PAYER_IST = "RegularPayer::Group::Ist::Member"

    EARLY_PAYER_CMT = "EarlyPayer::Group::Root::Member"
    EARLY_PAYER_YP = "EarlyPayer::Group::Unit::Member"
    EARLY_PAYER_UL = "EarlyPayer::Group::Unit::Leader"
    EARLY_PAYER_IST = "EarlyPayer::Group::Ist::Member"

    @property
    def payment_role(self) -> str:
        return self.value

    @property
    def full_cost_cents(self) -> int:
        """Full cost for this role in EUR-cents.

        >>> PaymentRole.REGULAR_PAYER_CMT.full_cost_cents
        160000
        """
        return _PAYMENT_ROLE_TO_FULL_COST_EUR[self] * 100

    @property
    def full_cost_eur(self) -> int:
        """Full cost for this role in EUR.

        >>> PaymentRole.REGULAR_PAYER_CMT.full_cost_eur
        1600
        """
        return _PAYMENT_ROLE_TO_FULL_COST_EUR[self]


_ALL_PAYMENT_ROLES = frozenset(role.value for role in PaymentRole)


_PAYMENT_ROLE_TO_FULL_COST_EUR: dict[str | PaymentRole, int] = {
    x[0]: int(x[1]) for x in _PAYMENT_ARRAY[1:]
}
assert _ALL_PAYMENT_ROLES == frozenset([x[0] for x in _PAYMENT_ARRAY[1:]])

_PAYMENT_ROLE_TO_FULL_COST_EUR.update(
    {role: _PAYMENT_ROLE_TO_FULL_COST_EUR[role.value] for role in PaymentRole}
)
_PAYMENT_ROLE_TO_FULL_COST_EUR.update(
    {role.name: _PAYMENT_ROLE_TO_FULL_COST_EUR[role.value] for role in PaymentRole}
)


def mandate_id_from_hitobito_id(hitobito_id: str | int) -> str:
    return f"wsjrdp2027{hitobito_id}"
