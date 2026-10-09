"""The participation contract of a person (people.contract_status and friends,
wagon migration 20261006200006), as the scripts keep it when they change a
person.

The contract follows status the way the wagon's Person#track_contract does:

- a change to ``confirmed`` starts a contract unless one is in force (a
  re-confirmation keeps the first date);
- a change to ``deregistered`` ends the contract in force;
- every other change of status leaves it as it is.

On top of that, a change of role that changes the fee (the payment role type:
UL to IST, YP to UL, ...) needs a new contract: the contract in force ends as
if the person deregistered, and the next confirmation starts a new one. A
move between groups of the same payment role type (IST to BMT and back, a
waiting list to a unit, an unapproved leader becoming a leader) keeps it.

The scripts write as the Administrator (versions.whodunnit 1), so a contract
they start is recorded as confirmed by that person.
"""

from __future__ import annotations

import collections.abc as _collections_abc
import dataclasses as _dataclasses
import datetime as _datetime
import logging as _logging
import typing as _typing


_LOGGER = _logging.getLogger(__name__)


CONTRACT_COLS = (
    "contract_status",
    "contract_confirmed_at",
    "contract_ended_at",
    "contract_confirmed_by_type",
    "contract_confirmed_by_id",
)

# Who confirms a contract that the scripts start: the Administrator, the
# author of every version the scripts write.
SCRIPT_CONFIRMED_BY_TYPE = "Person"
SCRIPT_CONFIRMED_BY_ID = 1

# The role types that carry a fee, and the payment role type of each, as the
# wagon maps them (Wsjrdp2027::Person::WSJRDP_ROLE_TYPE_TO_PAYMENT_ROLE_TYPE_MAP).
ROLE_TYPE_TO_PAYMENT_ROLE_TYPE = {
    "Group::Extern::Member": "Group::Extern::Member",
    "Group::Ist::Leader": "Group::Ist::Member",
    "Group::Ist::Member": "Group::Ist::Member",
    "Group::Root::Leader": "Group::Root::Member",
    "Group::Root::Member": "Group::Root::Member",
    "Group::Unit::Leader": "Group::Unit::Leader",
    "Group::Unit::Member": "Group::Unit::Member",
    "Group::Unit::UnapprovedLeader": "Group::Unit::Leader",
}


class ContractError(ValueError):
    """A change the contract rules do not allow."""


def payment_role_type(
    role_types: str | _collections_abc.Iterable[str] | None,
) -> str | None:
    """The payment role type of the role types of the primary group; None
    when none of them carries a fee.

    >>> payment_role_type(["Group::Root::Finance", "Group::Root::Leader"])
    'Group::Root::Member'
    >>> payment_role_type("Group::Unit::UnapprovedLeader")
    'Group::Unit::Leader'
    >>> payment_role_type(["Group::Root::Admin"]) is None
    True
    """
    from . import _util

    for role_type in _util.to_str_list(role_types):
        if mapped := ROLE_TYPE_TO_PAYMENT_ROLE_TYPE.get(role_type):
            return mapped
    return None


def role_change_needs_new_contract(
    old_role_types: str | _collections_abc.Iterable[str] | None,
    new_role_types: str | _collections_abc.Iterable[str] | None,
) -> bool:
    """Whether a change of the role types of the primary group changes the
    fee, so that the contract in force cannot go on.

    >>> role_change_needs_new_contract(["Group::Unit::Leader"], "Group::Ist::Member")
    True
    >>> role_change_needs_new_contract(["Group::Ist::Member"], ["Group::Ist::Member"])
    False
    >>> role_change_needs_new_contract(["Group::Unit::UnapprovedLeader"], "Group::Unit::Leader")
    False
    """
    old = payment_role_type(old_role_types)
    new = payment_role_type(new_role_types)
    return old is not None and new is not None and old != new


def contract_changes(
    old: _collections_abc.Mapping[str, _typing.Any],
    *,
    new_status: str | None,
    role_change: bool = False,
    now: _datetime.datetime,
) -> dict[str, list[_typing.Any]]:
    """The changes of the contract columns, ``{column: [old, new]}``, for a
    change of a person whose stored values are *old* (status and the contract
    columns): to *new_status* (None: status stays) and, with *role_change*,
    to a role with another fee. Empty when the contract stays as it is.

    Raises ContractError for a change of role of a person with a contract in
    force whose status stays confirmed: the new role needs a new contract,
    which takes a new confirmation.
    """
    old_status = old.get("status")
    before = {col: old.get(col) for col in CONTRACT_COLS}
    before["contract_status"] = before["contract_status"] or "none"
    after = dict(before)

    status_changes = new_status is not None and new_status != old_status

    if role_change and after["contract_status"] == "confirmed":
        if not status_changes or new_status == "confirmed":
            raise ContractError(
                "A change of role to another fee ends the contract in force; "
                "status must leave 'confirmed' with it (the new role needs a "
                f"new confirmation), but it stays {old_status!r}"
            )
        after["contract_status"] = "ended"
        after["contract_ended_at"] = now

    if status_changes:
        if new_status == "confirmed" and after["contract_status"] != "confirmed":
            after["contract_status"] = "confirmed"
            after["contract_confirmed_at"] = now
            after["contract_ended_at"] = None
            after["contract_confirmed_by_type"] = SCRIPT_CONFIRMED_BY_TYPE
            after["contract_confirmed_by_id"] = SCRIPT_CONFIRMED_BY_ID
        elif new_status == "deregistered" and after["contract_status"] == "confirmed":
            after["contract_status"] = "ended"
            after["contract_ended_at"] = now

    return {
        col: [before[col], after[col]]
        for col in CONTRACT_COLS
        if before[col] != after[col]
    }


# ---------------------------------------------------------------------------
# Consistency of status and contract, checked before a payment run.
#
# An error is a state the rules above never produce: a write that bypassed
# them (SQL by hand, an older script) or a broken contract row. A note is a
# legal state a payment run should know of: a contract in force while status
# stepped back (a document missing, a deregistration noted) -- a selection by
# status does not collect from such a person.

CONTRACT_STATUSES = ("none", "confirmed", "ended")

_CONSISTENCY_COLS = ("id", "status", *CONTRACT_COLS)


@_dataclasses.dataclass(frozen=True)
class ContractFinding:
    person_id: int
    status: str | None
    contract_status: str | None
    problem: str

    def __str__(self) -> str:
        return (
            f"{self.person_id:5} status={self.status} "
            f"contract_status={self.contract_status}: {self.problem}"
        )


def contract_problems(
    person: _collections_abc.Mapping[str, _typing.Any],
) -> tuple[list[str], list[str]]:
    """The errors and the notes for one person's status and contract columns.

    >>> contract_problems({"status": "confirmed", "contract_status": "none"})
    (["status is 'confirmed' without a contract in force"], [])
    """
    status = person.get("status")
    contract = person.get("contract_status")
    confirmed_at = person.get("contract_confirmed_at")
    ended_at = person.get("contract_ended_at")
    errors: list[str] = []
    notes: list[str] = []

    if contract not in CONTRACT_STATUSES:
        errors.append(f"unknown contract_status {contract!r}")
    if status == "confirmed" and contract != "confirmed":
        errors.append("status is 'confirmed' without a contract in force")
    if status == "deregistered" and contract == "confirmed":
        errors.append("status is 'deregistered' with a contract in force")
    if contract == "confirmed" and (confirmed_at is None or ended_at is not None):
        errors.append(
            "contract in force needs contract_confirmed_at and no contract_ended_at"
        )
    if contract == "ended" and (confirmed_at is None or ended_at is None):
        errors.append(
            "ended contract needs contract_confirmed_at and contract_ended_at"
        )
    if contract == "none" and any(
        person.get(col) is not None for col in CONTRACT_COLS if col != "contract_status"
    ):
        errors.append("no contract, but contract dates or confirmed_by are set")
    if confirmed_at is not None and ended_at is not None and ended_at < confirmed_at:
        errors.append("contract_ended_at before contract_confirmed_at")
    if contract == "confirmed" and status not in ("confirmed", "deregistered"):
        notes.append(
            f"contract in force, status {status!r}: not collected by a selection on status"
        )
    return errors, notes


def find_contract_findings(
    people: _collections_abc.Iterable[_collections_abc.Mapping[str, _typing.Any]],
) -> tuple[list[ContractFinding], list[ContractFinding]]:
    """The errors and the notes of all *people* (contract_problems)."""
    errors: list[ContractFinding] = []
    notes: list[ContractFinding] = []
    for person in people:
        person_errors, person_notes = contract_problems(person)
        for target, problems in ((errors, person_errors), (notes, person_notes)):
            target.extend(
                ContractFinding(
                    person_id=person["id"],
                    status=person.get("status"),
                    contract_status=person.get("contract_status"),
                    problem=problem,
                )
                for problem in problems
            )
    return errors, notes


def load_contract_findings(
    conn,
) -> tuple[list[ContractFinding], list[ContractFinding]]:
    """The errors and the notes of every person in the database."""
    from . import _pg

    columns = ", ".join(_CONSISTENCY_COLS)
    rows = _pg.pg_select_dict_rows(conn, f"SELECT {columns} FROM people ORDER BY id")
    return find_contract_findings(rows)


def check_contract_consistency(ctx, conn) -> list[ContractFinding]:
    """Logs the errors and the notes (load_contract_findings) and, with any
    error, asks for approval to go on in production. Returns the errors."""
    errors, notes = load_contract_findings(conn)
    _LOGGER.info("==== Contract consistency")
    _LOGGER.info("  errors: %s, notes: %s", len(errors), len(notes))
    for finding in notes:
        _LOGGER.info("  note:  %s", finding)
    for finding in errors:
        _LOGGER.error("  error: %s", finding)
    if errors:
        ctx.require_approval_to_run_in_prod(
            f"{len(errors)} contract inconsistencies (see log). Continue anyway?"
        )
    return errors
