from __future__ import annotations

import enum as _enum
import typing as _typing


class _WsjRoleProps(_typing.NamedTuple):
    reg_primary_group: int
    regular_payer_payment_role: str
    early_payer_payment_role: str


class WsjRole(_enum.Enum):
    # fmt: off
    CMT = _WsjRoleProps(1, "RegularPayer::Group::Root::Member", "EarlyPayer::Group::Root::Member")
    UL = _WsjRoleProps(2, "RegularPayer::Group::Unit::Leader", "EarlyPayer::Group::Unit::Leader")
    YP = _WsjRoleProps(3, "RegularPayer::Group::Unit::Member", "EarlyPayer::Group::Unit::Member")
    IST = _WsjRoleProps(4, "RegularPayer::Group::Ist::Member", "EarlyPayer::Group::Ist::Member")
    # fmt: on

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}.{self.name}"

    @property
    def regular_payer_payment_role(self) -> str:
        return self.value.regular_payer_payment_role

    @property
    def early_payer_payment_role(self) -> str:
        return self.value.early_payer_payment_role

    @property
    def regular_total_fee_eur(self) -> int:
        """The regular total fee in EUR.

        >>> WsjRole.CMT.regular_total_fee_eur
        1600
        """
        from . import _payment_role

        return _payment_role._PAYMENT_ROLE_TO_FULL_FEE_EUR[
            self.value.regular_payer_payment_role
        ]

    @property
    def regular_total_fee_cents(self) -> int:
        return self.regular_total_fee_eur * 100

    @classmethod
    def from_any(cls, obj: str | WsjRole) -> _typing.Self:
        if isinstance(obj, WsjRole):
            return cls(obj)
        elif isinstance(obj, str):
            return cls[obj]
        else:
            raise RuntimeError(f"Cannot convert to WsjRole: {obj!r}")
