"""What a script leaves behind once it has told a person about a returned
direct debit (Rücklastschrift) by e-mail.

Two traces, written in the transaction of the run and only after the mail has
gone out:

- a note in the person's log (:func:`log_entry`): a ``versions`` row with
  event ``wsjrdp_note`` and category :data:`NOTE_CATEGORY`, shown on the
  person's log page (not among the person's notes). Its item is the
  accounting entry of the return, where one is known, so the entry and the
  note point at each other.
- an element of the list ``debit_return_notifications`` in the
  ``additional_info`` of that accounting entry (:func:`append_notification`):
  when the mail went out, to whom, its subject and Message-ID, the FIN ticket
  (only when there is one -- not FIN-000) and the id of the log note. A list, because a second mail about the same return
  (a reminder) adds to it.

The amounts are in cents; :func:`log_message` renders them for the log line.
User: ``accounting_tools/missing_installment.py``.
"""

from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import typing as _typing

from . import _log_entry, _util


if _typing.TYPE_CHECKING:
    import email.message as _email_message

    import psycopg as _psycopg


#: The category of the log note; the wagon labels it (version.wsjrdp_note_categories).
NOTE_CATEGORY = "debit_return"

#: The key of the list in ``accounting_entries.additional_info``.
NOTIFICATIONS_KEY = "debit_return_notifications"

#: Values of ``people.additional_info['debit_return_issue']`` that name no
#: ticket: none yet, or the placeholder FIN-000.
NO_ISSUE = ("", "FIN-000")


@_dataclasses.dataclass(frozen=True, kw_only=True)
class DebitReturnNotice:
    """One mail about one returned direct debit, as it was sent."""

    person_id: int
    #: The month of the failed collection, ``YYYY-MM``.
    collection_month: str
    retoure_cents: int
    missing_installment_cents: int
    bank_fees_cents: int
    #: The collection that is to take the missing amount along.
    upcoming_collection_date: _datetime.date
    #: What that collection is to take in all.
    upcoming_amount_cents: int | None = None
    #: The FIN ticket; a placeholder (:data:`NO_ISSUE`) is taken as none.
    debit_return_issue: str | None = None
    accounting_entry_id: int | None = None
    camt_transaction_id: int | None = None
    sent_at: _datetime.datetime
    email_to: tuple[str, ...] = ()
    email_cc: tuple[str, ...] = ()
    email_subject: str | None = None
    email_message_id: str | None = None
    script: str | None = None

    def __post_init__(self) -> None:
        if (
            self.debit_return_issue is not None
            and self.debit_return_issue.strip() in NO_ISSUE
        ):
            object.__setattr__(self, "debit_return_issue", None)

    @classmethod
    def from_email_message(
        cls, message: _email_message.EmailMessage, **kwargs: _typing.Any
    ) -> DebitReturnNotice:
        """The notice with the recipients, subject and Message-ID of ``message``."""
        return cls(
            email_to=_addresses(message.get_all("To")),
            email_cc=_addresses(message.get_all("Cc")),
            email_subject=_header(message.get("Subject")),
            email_message_id=_header(message.get("Message-ID")),
            **kwargs,
        )


def log_message(notice: DebitReturnNotice) -> str:
    """The one line the person's log shows after the category label."""

    def eur(cents: int) -> str:
        return _util.format_cents_as_eur_de(cents, zero_cents=",00")

    parts = [
        (
            f"Per E-Mail über die Rücklastschrift des Einzugs {notice.collection_month}"
            f" informiert: {eur(notice.retoure_cents)}"
        )
    ]
    if notice.bank_fees_cents > 0:
        parts[0] += f", davon {eur(notice.bank_fees_cents)} Gebühren"
    upcoming = (
        f"Nachholung mit dem Einzug am {notice.upcoming_collection_date:%d.%m.%Y}"
    )
    if notice.upcoming_amount_cents is not None:
        upcoming += f" ({eur(notice.upcoming_amount_cents)})"
    parts.append(upcoming)
    if notice.debit_return_issue:
        parts.append(f"Ticket {notice.debit_return_issue}")
    return "; ".join(parts) + "."


def log_entry(notice: DebitReturnNotice) -> _log_entry.LogEntry:
    """The note in the person's log; its item is the accounting entry, if known."""
    item = (
        {"item_type": "AccountingEntry", "item_id": notice.accounting_entry_id}
        if notice.accounting_entry_id is not None
        else {}
    )
    return _log_entry.LogEntry.note(
        main_id=notice.person_id,
        category=NOTE_CATEGORY,
        message=log_message(notice),
        data={
            "collection_month": notice.collection_month,
            "retoure_cents": notice.retoure_cents,
            "missing_installment_cents": notice.missing_installment_cents,
            "bank_fees_cents": notice.bank_fees_cents,
            "upcoming_collection_date": notice.upcoming_collection_date,
            "upcoming_amount_cents": notice.upcoming_amount_cents,
            "debit_return_issue": notice.debit_return_issue,
            "accounting_entry_id": notice.accounting_entry_id,
            "camt_transaction_id": notice.camt_transaction_id,
            "email_to": list(notice.email_to),
            "email_cc": list(notice.email_cc),
            "email_subject": notice.email_subject,
            "email_message_id": notice.email_message_id,
        },
        additional_info=({"script": notice.script} if notice.script else {}),
        created_at=notice.sent_at,
        **item,
    )


def notification(
    notice: DebitReturnNotice, *, log_version_id: int | None
) -> dict[str, _typing.Any]:
    """The element appended to the entry's ``debit_return_notifications``;
    ``debit_return_issue`` only with a ticket (not without one, not FIN-000)."""
    element: dict[str, _typing.Any] = {
        "sent_at": notice.sent_at.isoformat(),
        "email_to": list(notice.email_to),
        "email_cc": list(notice.email_cc),
        "email_subject": notice.email_subject,
        "email_message_id": notice.email_message_id,
    }
    if notice.debit_return_issue:
        element["debit_return_issue"] = notice.debit_return_issue
    element["log_version_id"] = log_version_id
    return element


_APPEND_SQL = f"""UPDATE accounting_entries
SET additional_info = COALESCE(additional_info, '{{}}'::jsonb)
  || jsonb_build_object('{NOTIFICATIONS_KEY}',
       COALESCE(additional_info->'{NOTIFICATIONS_KEY}', '[]'::jsonb)
       || jsonb_build_array(%(notification)s::jsonb)),
    updated_at = %(now)s
WHERE id = %(id)s
RETURNING id"""


def append_notification(
    conn: _psycopg.Connection,
    accounting_entry_id: int,
    element: _typing.Mapping[str, _typing.Any],
    *,
    now: _datetime.datetime,
) -> None:
    """Appends ``element`` to the entry's ``debit_return_notifications``.

    Never commits. Raises if the entry does not exist."""
    import json

    row = conn.execute(
        _APPEND_SQL,
        {"notification": json.dumps(element), "now": now, "id": accounting_entry_id},
    ).fetchone()
    if row is None:
        raise RuntimeError(f"accounting_entries row {accounting_entry_id} not found")


def record(conn: _psycopg.Connection, notice: DebitReturnNotice) -> int:
    """Writes both traces of ``notice``; returns the id of the log note.

    Never commits: they belong to the transaction of the run."""
    (version_id,) = _log_entry.insert_log_entries(conn, [log_entry(notice)])
    if notice.accounting_entry_id is not None:
        append_notification(
            conn,
            notice.accounting_entry_id,
            notification(notice, log_version_id=version_id),
            now=notice.sent_at,
        )
    return version_id


def _header(value: object) -> str | None:
    return None if value is None else str(value)


def _addresses(values: _typing.Iterable[object] | None) -> tuple[str, ...]:
    """The bare addresses of address headers (``Name <a@b>`` → ``a@b``)."""
    import email.utils

    return tuple(
        addr
        for _name, addr in email.utils.getaddresses([str(v) for v in values or ()])
        if addr
    )
