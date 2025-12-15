from __future__ import annotations

import datetime as _datetime
import enum as _enum
import itertools as _itertools


# input: [dt.datetime.strptime(d, '%b %Y').date() for d in _PAYMENT_ARRAY[0][2:]]
_PAYMENT_DATES = [
    _datetime.date.min,
    _datetime.date(2025, 8, 1),  # cut-off for earliest Early-Payer
    _datetime.date(2025, 11, 1),  # cut-off for middle Early-Payer
    _datetime.date(2025, 12, 1),
    _datetime.date(2026, 1, 1),
    _datetime.date(2026, 2, 1),
    _datetime.date(2026, 3, 1),
    _datetime.date(2026, 8, 1),
    _datetime.date(2026, 11, 1),
    _datetime.date(2027, 2, 1),
    _datetime.date(2027, 5, 1),
    _datetime.date.max,
]

_AUG_25 = _datetime.date(2025, 8, 1)
_NOV_25 = _datetime.date(2025, 11, 1)


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
    """Payment role."""

    REGULAR_PAYER_CMT = "RegularPayer::Group::Root::Member"
    REGULAR_PAYER_YP = "RegularPayer::Group::Unit::Member"
    REGULAR_PAYER_UL = "RegularPayer::Group::Unit::Leader"
    REGULAR_PAYER_IST = "RegularPayer::Group::Ist::Member"

    EARLY_PAYER_CMT = "EarlyPayer::Group::Root::Member"
    EARLY_PAYER_YP = "EarlyPayer::Group::Unit::Member"
    EARLY_PAYER_UL = "EarlyPayer::Group::Unit::Leader"
    EARLY_PAYER_IST = "EarlyPayer::Group::Ist::Member"

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}.{self.name}"

    @property
    def is_early_payer(self) -> bool:
        return self in _EARLY_PAYER_ROLES

    @property
    def is_regular_payer(self) -> bool:
        return self in _REGULAR_PAYER_ROLES

    @property
    def is_yp(self) -> bool:
        """`True` if this is a YP payment role.

        >>> PaymentRole.EARLY_PAYER_YP.is_yp
        True
        >>> PaymentRole.REGULAR_PAYER_YP.is_yp
        True
        >>> PaymentRole.EARLY_PAYER_IST.is_yp
        False
        """
        return self in (PaymentRole.EARLY_PAYER_YP, PaymentRole.REGULAR_PAYER_YP)

    def get_db_payment_role(self, *, early_payer: bool | None = None) -> str:
        """The string for the payment_role column in the database.

        >>> PaymentRole.REGULAR_PAYER_CMT.get_db_payment_role()
        'RegularPayer::Group::Root::Member'
        >>> PaymentRole.REGULAR_PAYER_CMT.get_db_payment_role(early_payer=True)
        'EarlyPayer::Group::Root::Member'
        >>> PaymentRole.REGULAR_PAYER_CMT.get_db_payment_role(early_payer=False)
        'RegularPayer::Group::Root::Member'
        """
        if early_payer is None:
            return self.value
        elif early_payer:
            return getattr(self, f"EARLY_PAYER_{self.short_role_name}").value
        else:
            return getattr(self, f"REGULAR_PAYER_{self.short_role_name}").value

    @property
    def short_role_name(self) -> str:
        """The short role name.

        >>> PaymentRole.REGULAR_PAYER_CMT.short_role_name
        'CMT'
        >>> PaymentRole.EARLY_PAYER_CMT.short_role_name
        'CMT'
        """
        return self.name.rsplit("_", 1)[1]

    @property
    def regular_full_fee_eur(self) -> int:
        """Full amount due for this role in EUR.

        >>> PaymentRole.REGULAR_PAYER_CMT.regular_full_fee_eur
        1600
        """
        return _PAYMENT_ROLE_TO_FULL_FEE_EUR[self]

    @property
    def regular_full_fee_cents(self) -> int:
        """Full amount due for this role in EUR-cents.

        >>> PaymentRole.REGULAR_PAYER_CMT.regular_full_fee_cents
        160000
        """
        return self.regular_full_fee_eur * 100

    def fee_due_by_date_in_eur(
        self,
        date: _datetime.date | str,
        *,
        print_at: _datetime.date | str | None = None,
    ) -> int:
        """Return the accumulated fees due for this role by *date* in EUR.

        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('1900-01-01')
        0
        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('1900-07-31')
        0
        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('2025-08-01')
        0
        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('2025-11-30')
        0
        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('2025-12-01')
        300
        >>> PaymentRole.REGULAR_PAYER_YP.fee_due_by_date_in_eur('2031-01-01')
        3400


        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('1900-01-01')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('1900-07-31')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-08-01')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-11-30')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-12-01')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2031-01-01')
        3400

        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('1900-07-31', print_at='2025-07-31')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-08-01', print_at='2025-07-31')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-10-31', print_at='2025-07-31')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-11-01', print_at='2025-07-31')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2031-01-01', print_at='2025-07-31')
        3400

        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('1900-07-31', print_at='2025-08-01')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-08-01', print_at='2025-08-01')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-10-31', print_at='2025-08-01')
        0
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2025-11-01', print_at='2025-08-01')
        3400
        >>> PaymentRole.EARLY_PAYER_YP.fee_due_by_date_in_eur('2031-01-01', print_at='2025-08-01')
        3400
        """
        import bisect

        from ._util import to_date_or_none

        date = to_date_or_none(date)
        pos = max(bisect.bisect_right(_PAYMENT_DATES, date) - 1, 0)
        amount = _PAYMENT_ROLE_TO_ACCUMULATED_INSTALLMENTS[self][pos]
        if self.is_early_payer and print_at is not None:
            print_at = to_date_or_none(print_at)
            if date < _NOV_25 and print_at >= _AUG_25:
                return 0
        return amount

    def get_installments_eur(
        self,
        *,
        early_payer: bool | None = None,
        print_at: _datetime.date | str | None = None,
        today: _datetime.date | str = "TODAY",
        fee_reduction_eur: float = 0,
    ) -> dict[tuple[int, int], float]:
        """Dictionary mapping year-month tuples to installments.

        >>> PaymentRole.REGULAR_PAYER_CMT.get_installments_eur()
        {(2025, 12): 50, (2026, 1): 250, (2026, 2): 250, (2026, 3): 250, (2026, 8): 200, (2026, 11): 200, (2027, 2): 200, (2027, 5): 200}
        >>> PaymentRole.REGULAR_PAYER_YP.get_installments_eur()
        {(2025, 12): 300, (2026, 1): 500, (2026, 2): 500, (2026, 3): 500, (2026, 8): 400, (2026, 11): 400, (2027, 2): 400, (2027, 5): 400}
        >>> PaymentRole.REGULAR_PAYER_UL.get_installments_eur()
        {(2025, 12): 150, (2026, 1): 350, (2026, 2): 350, (2026, 3): 350, (2026, 8): 300, (2026, 11): 300, (2027, 2): 300, (2027, 5): 300}
        >>> PaymentRole.REGULAR_PAYER_IST.get_installments_eur()
        {(2025, 12): 200, (2026, 1): 400, (2026, 2): 400, (2026, 3): 400, (2026, 8): 300, (2026, 11): 300, (2027, 2): 300, (2027, 5): 300}

        >>> PaymentRole.REGULAR_PAYER_IST.get_installments_eur(fee_reduction_eur=1250)
        {(2025, 12): 200, (2026, 1): 400, (2026, 2): 400, (2026, 3): 350}

        >>> PaymentRole.EARLY_PAYER_CMT.get_installments_eur(today="2025-07-31")
        {(2026, 1): 1600}
        >>> PaymentRole.EARLY_PAYER_YP.get_installments_eur(today="2025-07-31")
        {(2026, 1): 3400}
        >>> PaymentRole.EARLY_PAYER_UL.get_installments_eur(today="2025-07-31")
        {(2026, 1): 2400}
        >>> PaymentRole.EARLY_PAYER_IST.get_installments_eur(today="2025-07-31")
        {(2026, 1): 2600}
        >>> PaymentRole.EARLY_PAYER_YP.get_installments_eur(today="2025-07-31", fee_reduction_eur=1700)
        {(2026, 1): 1700}
        """
        from . import _util

        today = _util.to_date(today)
        if print_at is None:
            single_payment_at = today
        else:
            print_at = _util.to_date(print_at)
            print_at_plus_notification_days = print_at + _datetime.timedelta(days=4)
            if today < _datetime.date(2026, 1, 1):
                single_payment_at = max(today, print_at_plus_notification_days)
            else:
                single_payment_at = print_at_plus_notification_days

        if early_payer is None:
            early_payer = self.is_early_payer
        if early_payer:
            start_of_next_month = (
                single_payment_at.replace(day=1) + _datetime.timedelta(days=32)
            ).replace(day=1)
            ym = (start_of_next_month.year, start_of_next_month.month)
            total_fee = max(self.regular_full_fee_eur - fee_reduction_eur, 0)
            if ym <= (2025, 12):
                return {(2026, 1): total_fee}
            elif ym in ((2025, 9), (2025, 10)):
                return {(2025, 11): total_fee}
            else:
                return {ym: total_fee}

        dates = _PAYMENT_DATES
        raw_installments: list[float] = _PAYMENT_ROLE_TO_INSTALLMENTS[self]  # type: ignore
        if fee_reduction_eur > 0:
            for i, eur in reversed(list(enumerate(raw_installments))):
                if fee_reduction_eur > eur:
                    raw_installments[i] = 0
                    fee_reduction_eur -= eur
                else:  # fee_reduction_eur <= eur
                    raw_installments[i] -= fee_reduction_eur
                    fee_reduction_eur = 0
                    break
        installments = {
            (date.year, date.month): cents
            for date, cents in zip(dates, raw_installments)
            if cents != 0
        }
        return installments

    def get_installments_cents(
        self,
        *,
        early_payer: bool | None = None,
        print_at: _datetime.date | str | None = None,
        today: _datetime.date | str = "TODAY",
        fee_reduction_cents: int = 0,
    ) -> dict[tuple[int, int], int]:
        """Dictionary of year-month tuple to installments.

        >>> PaymentRole.EARLY_PAYER_CMT.get_installments_cents(today="2025-07-31")
        {(2026, 1): 160000}
        """
        fee_reduction_eur = fee_reduction_cents / 100
        if fee_reduction_eur == round(fee_reduction_eur):
            fee_reduction_eur = int(fee_reduction_eur)

        return {
            key: int(round(eur * 100))
            for key, eur in self.get_installments_eur(
                early_payer=early_payer,
                print_at=print_at,
                today=today,
                fee_reduction_eur=fee_reduction_eur,
            ).items()
        }

    def fee_due_by_date_in_cent(
        self,
        date: _datetime.date | str,
        *,
        print_at: _datetime.date | str | None = None,
    ) -> int:
        return self.fee_due_by_date_in_eur(date, print_at=print_at) * 100


_ALL_PAYMENT_ROLES = frozenset(role.value for role in PaymentRole)


_PAYMENT_ROLE_TO_FULL_FEE_EUR: dict[str | PaymentRole, int] = {
    x[0]: int(x[1]) for x in _PAYMENT_ARRAY[1:]
}
assert _ALL_PAYMENT_ROLES == frozenset([x[0] for x in _PAYMENT_ARRAY[1:]])

_PAYMENT_ROLE_TO_FULL_FEE_EUR.update(
    {role: _PAYMENT_ROLE_TO_FULL_FEE_EUR[role.value] for role in PaymentRole}
)


_EARLY_PAYER_ROLES = frozenset(
    [
        role
        for role in PaymentRole
        if role.get_db_payment_role().startswith("EarlyPayer::")
    ]
)
_REGULAR_PAYER_ROLES = frozenset(
    [
        role
        for role in PaymentRole
        if role.get_db_payment_role().startswith("RegularPayer::")
    ]
)
assert all(role.is_early_payer or role.is_regular_payer for role in PaymentRole)
assert not any(role.is_early_payer and role.is_regular_payer for role in PaymentRole)

_PAYMENT_ROLE_TO_INSTALLMENTS: dict[str | PaymentRole, list[int]] = {
    x[0]: (
        [0]
        + [int(x[1]) if x[0].startswith("EarlyPayer::") else 0]
        + [0]
        + [int(s or "0") for s in x[2:]]
        + [0]
    )
    for x in _PAYMENT_ARRAY[1:]
}
_PAYMENT_ROLE_TO_INSTALLMENTS.update(
    {role: _PAYMENT_ROLE_TO_INSTALLMENTS[role.value] for role in PaymentRole}
)

_PAYMENT_ROLE_TO_ACCUMULATED_INSTALLMENTS = {
    k: list(_itertools.accumulate(v)) for k, v in _PAYMENT_ROLE_TO_INSTALLMENTS.items()
}
