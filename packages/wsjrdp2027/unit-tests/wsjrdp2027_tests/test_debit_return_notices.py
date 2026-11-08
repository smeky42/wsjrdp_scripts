from __future__ import annotations

import datetime
import email.message

import pytest
import wsjrdp2027


notices = wsjrdp2027.debit_return_notices

SENT_AT = datetime.datetime(2026, 11, 8, 10, 30, tzinfo=datetime.UTC)


def _message() -> email.message.EmailMessage:
    msg = email.message.EmailMessage()
    msg["To"] = "Kim Example <kim@example.org>, parent@example.org"
    msg["Cc"] = "Other <other@example.org>"
    msg["Subject"] = "WSJ27 Einzug 2026-10 Retoure YP-1 FIN-1"
    msg["Message-ID"] = "<abc@example.org>"
    return msg


def _plain(text: str) -> str:
    """The amounts come with a no-break space before the euro sign."""
    return text.replace("\N{NO-BREAK SPACE}", " ")


def _notice(**kwargs) -> notices.DebitReturnNotice:
    defaults = dict(
        person_id=42,
        collection_month="2026-10",
        retoure_cents=31500,
        missing_installment_cents=30000,
        bank_fees_cents=1500,
        upcoming_collection_date=datetime.date(2026, 11, 5),
        upcoming_amount_cents=61500,
        debit_return_issue="FIN-1",
        accounting_entry_id=7,
        camt_transaction_id=9,
        sent_at=SENT_AT,
        script="accounting_tools/missing_installment.py",
    )
    defaults.update(kwargs)
    return notices.DebitReturnNotice.from_email_message(_message(), **defaults)


def test_from_email_message_takes_bare_addresses_subject_and_message_id():
    notice = _notice()
    assert notice.email_to == ("kim@example.org", "parent@example.org")
    assert notice.email_cc == ("other@example.org",)
    assert notice.email_subject == "WSJ27 Einzug 2026-10 Retoure YP-1 FIN-1"
    assert notice.email_message_id == "<abc@example.org>"


def test_log_message():
    assert _plain(notices.log_message(_notice())) == (
        "Per E-Mail über die Rücklastschrift des Einzugs 2026-10 informiert:"
        " 315,00 €, davon 15,00 € Gebühren;"
        " Nachholung mit dem Einzug am 05.11.2026 (615,00 €); Ticket FIN-1."
    )


def test_log_message_without_fees_amount_and_ticket():
    notice = _notice(
        bank_fees_cents=0, upcoming_amount_cents=None, debit_return_issue=None
    )
    assert _plain(notices.log_message(notice)) == (
        "Per E-Mail über die Rücklastschrift des Einzugs 2026-10 informiert:"
        " 315,00 €; Nachholung mit dem Einzug am 05.11.2026."
    )


def test_log_entry_is_a_note_on_the_accounting_entry():
    row = notices.log_entry(_notice()).to_row()
    assert row["event"] == "wsjrdp_note"
    assert (row["main_type"], row["main_id"]) == ("Person", 42)
    assert (row["item_type"], row["item_id"]) == ("AccountingEntry", 7)
    assert row["created_at"] == SENT_AT
    assert row["additional_info"] == {
        "script": "accounting_tools/missing_installment.py"
    }
    data = row["wsjrdp_data"]
    assert data["category"] == "debit_return"
    assert data["message"].startswith("Per E-Mail")
    assert data["upcoming_collection_date"] == "2026-11-05"
    assert data["retoure_cents"] == 31500
    assert data["camt_transaction_id"] == 9
    assert data["email_to"] == ["kim@example.org", "parent@example.org"]
    assert data["email_message_id"] == "<abc@example.org>"


def test_log_entry_without_accounting_entry_is_on_the_person():
    row = notices.log_entry(_notice(accounting_entry_id=None)).to_row()
    assert (row["item_type"], row["item_id"]) == ("Person", 42)


def test_notification():
    assert notices.notification(_notice(), log_version_id=123) == {
        "sent_at": "2026-11-08T10:30:00+00:00",
        "email_to": ["kim@example.org", "parent@example.org"],
        "email_cc": ["other@example.org"],
        "email_subject": "WSJ27 Einzug 2026-10 Retoure YP-1 FIN-1",
        "email_message_id": "<abc@example.org>",
        "debit_return_issue": "FIN-1",
        "log_version_id": 123,
    }


@pytest.mark.parametrize("issue", [None, "", "FIN-000"])
def test_notification_without_a_ticket_has_no_issue_key(issue):
    notice = _notice(debit_return_issue=issue)

    assert notice.debit_return_issue is None
    assert "debit_return_issue" not in notices.notification(notice, log_version_id=1)
    assert "Ticket" not in notices.log_message(notice)


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self):
        self.calls = []

    def execute(self, query, params=None):
        self.calls.append((query, params))
        return _FakeResult((params["id"],))


def test_append_notification_appends_to_the_list(monkeypatch):
    conn = _FakeConn()
    notices.append_notification(conn, 7, {"log_version_id": 1}, now=SENT_AT)
    ((query, params),) = conn.calls
    assert "debit_return_notifications" in query
    assert "jsonb_build_array" in query
    assert params == {"notification": '{"log_version_id": 1}', "now": SENT_AT, "id": 7}


def test_record_writes_note_then_notification(monkeypatch):
    inserted = []

    def fake_insert(conn, entries, *, now=None):
        inserted.extend(entries)
        return [555]

    monkeypatch.setattr(wsjrdp2027._log_entry, "insert_log_entries", fake_insert)
    conn = _FakeConn()
    assert notices.record(conn, _notice()) == 555
    assert inserted[0].data["category"] == "debit_return"
    ((_query, params),) = conn.calls
    assert '"log_version_id": 555' in params["notification"]


def test_record_without_accounting_entry_writes_only_the_note(monkeypatch):
    monkeypatch.setattr(
        wsjrdp2027._log_entry, "insert_log_entries", lambda conn, entries, now=None: [1]
    )
    conn = _FakeConn()
    assert notices.record(conn, _notice(accounting_entry_id=None)) == 1
    assert conn.calls == []
