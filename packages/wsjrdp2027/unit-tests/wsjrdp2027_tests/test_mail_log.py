from __future__ import annotations

import email.message
import email.utils
import hashlib
import types

import pandas as pd
import pytest
from wsjrdp2027 import _log_entry, _mail_log, _pg
from wsjrdp2027._batch import BatchConfig, PreparedBatch, PreparedEmailMessage


def _msg(
    *, to="Anna Beispiel <anna@example.org>", bcc=None
) -> email.message.EmailMessage:
    msg = email.message.EmailMessage()
    msg["Subject"] = "Betreff"
    msg["From"] = "info@example.org"
    if to:
        msg["To"] = to
    msg["Cc"] = "cc@example.org"
    if bcc:
        msg["Bcc"] = bcc
    msg["Reply-To"] = "reply@example.org"
    msg["Date"] = "Sun, 01 Nov 2026 09:00:00 +0100"
    msg["Message-ID"] = "<1@example.org>"
    msg.set_content("Hallo")
    return msg


class Test_prepared_data:
    def test_keys(self):
        msg = _msg(bcc="bcc@example.org")
        eml = msg.as_bytes()
        data = _mail_log.prepared_data(
            msg, eml=eml, eml_name="b.7.eml", envelope_from="env@example.org"
        )
        assert data == {
            "email_status": "prepared",
            "email_message_id": "<1@example.org>",
            "email_subject": "Betreff",
            "email_date": "2026-11-01T09:00:00+01:00",
            "email_from": ["info@example.org"],
            "email_to": ["Anna Beispiel <anna@example.org>"],
            "email_cc": ["cc@example.org"],
            "email_bcc": ["bcc@example.org"],
            "email_reply_to": ["reply@example.org"],
            "email_envelope_from": "env@example.org",
            "email_envelope_to": [
                "anna@example.org",
                "cc@example.org",
                "bcc@example.org",
            ],
            "email_eml_size": len(eml),
            "email_eml_sha256": hashlib.sha256(eml).hexdigest(),
            "email_eml_name": "b.7.eml",
        }

    def test_zip_name_only_when_zipped(self):
        msg = _msg()
        data = _mail_log.prepared_data(msg, eml=b"x", eml_zip_name="b.zip")
        assert data["email_eml_zip_name"] == "b.zip"
        assert "email_eml_zip_name" not in _mail_log.prepared_data(msg, eml=b"x")


class _Conn:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


class _MailClient:
    def __init__(self, fail_on: set[str] = frozenset()):
        self.sent: list[str] = []
        self.fail_on = fail_on

    def send_message(self, msg, *, from_addr=None):
        if msg["Message-ID"] in self.fail_on:
            raise RuntimeError("smtp down")
        self.sent.append(msg["Message-ID"])
        return {}, ("OK", [b"[APPENDUID 1 " + str(len(self.sent)).encode() + b"] ok"])

    def get_imap_sent_mailbox(self):
        return ("Sent", "/")


@pytest.fixture
def recorded(monkeypatch):
    rec = {"entries": [], "updates": []}

    def fake_insert(conn, entries, *, now=None):
        entries = list(entries)
        rec["entries"].extend(entries)
        return [100 + i for i in range(len(entries))]

    def fake_updatemany(conn, table, updates, *, touch=None):
        assert table == "versions" and touch is False
        rec["updates"].append([dict(u) for u in updates])

    monkeypatch.setattr(_log_entry, "insert_log_entries", fake_insert)
    monkeypatch.setattr(_pg, "pg_table_updatemany", fake_updatemany)
    return rec


def _batch(messages, **kwargs) -> PreparedBatch:
    return PreparedBatch(
        name="pn",
        df=pd.DataFrame(),
        messages=tuple(messages),
        config_yaml=None,
        from_addr="env@example.org",
        log_note={
            "category": "pre_notification",
            "message": "Mail an {{ p.id }} {{ email_status_text }}",
        },
        script="test.py",
        **kwargs,
    )


def _prep(person_id: int, msg_id: str, **kwargs) -> PreparedEmailMessage:
    msg = _msg()
    msg.replace_header("Message-ID", msg_id)
    return PreparedEmailMessage(
        mailing_name="pn",
        message=msg,
        person=types.SimpleNamespace(id=person_id),
        summary=str(person_id),
        eml_name=f"pn.{person_id}.eml",
        **kwargs,
    )


class Test_PreparedBatch_log_notes:
    def test_insert_then_send(self, recorded):
        msgs = [
            _prep(7, "<a@x>", log_note_item=("WsjrdpDirectDebitPreNotification", 55)),
            _prep(8, "<b@x>", log_note_data={"pre_notification_recorded": False}),
        ]
        batch = _batch(msgs)
        assert batch.insert_log_notes(_Conn()) == [100, 101]
        first, second = recorded["entries"]
        assert (first.item_type, first.item_id, first.main_id) == (
            "WsjrdpDirectDebitPreNotification",
            55,
            7,
        )
        assert first.data["message"] == "Mail an 7 nicht verschickt (abgebrochen)"
        assert first.data["email_status"] == "prepared"
        assert first.data["batch_name"] == "pn"
        assert first.data["script"] == "test.py"
        assert second.item_type is None
        assert second.data["pre_notification_recorded"] is False

        conn = _Conn()
        batch.send(_MailClient(), log_conn=conn, log_flush_every=1)
        assert conn.commits == 2
        flat = [u for block in recorded["updates"] for u in block]
        assert [u["id"] for u in flat] == [100, 101]
        data = flat[0]["wsjrdp_data"]
        assert data["email_status"] == "sent"
        assert data["message"] == "Mail an 7 verschickt"
        assert data["email_imap_uid"] == 1
        assert data["email_imap_mailbox"] == "Sent"
        assert data["email_smtp_refused"] == {}
        assert "email_sent_at" in data

    def test_failure_flushes_and_reraises(self, recorded):
        msgs = [_prep(7, "<a@x>"), _prep(8, "<b@x>"), _prep(9, "<c@x>")]
        batch = _batch(msgs)
        batch.insert_log_notes(_Conn())
        conn = _Conn()
        with pytest.raises(RuntimeError, match="smtp down"):
            batch.send(_MailClient(fail_on={"<b@x>"}), log_conn=conn)
        assert conn.commits == 1
        (block,) = recorded["updates"]
        assert [(u["id"], u["wsjrdp_data"]["email_status"]) for u in block] == [
            (100, "sent"),
            (101, "failed"),
        ]
        assert (
            block[1]["wsjrdp_data"]["message"] == "Mail an 8 nicht verschickt (Fehler)"
        )
        assert block[1]["wsjrdp_data"]["email_error"] == "RuntimeError: smtp down"

    def test_no_recipients(self, recorded):
        msg = _prep(7, "<a@x>")
        for header in ("To", "Cc"):
            del msg.message[header]
        batch = _batch([msg])
        batch.insert_log_notes(_Conn())
        client = _MailClient()
        batch.send(client, log_conn=_Conn())
        assert client.sent == []
        (block,) = recorded["updates"]
        assert block[0]["wsjrdp_data"]["email_status"] == "no_recipients"
        assert block[0]["wsjrdp_data"]["message"] == (
            "Mail an 7 nicht verschickt (keine Empfänger)"
        )

    @pytest.mark.parametrize("flag", ["dry_run", "skip_email", "skip_db_updates"])
    def test_no_entries_without_sending_or_db_writes(self, recorded, flag):
        batch = _batch([_prep(7, "<a@x>")], **{flag: True})
        assert batch.insert_log_notes(_Conn()) == []
        assert recorded["entries"] == []

    def test_send_needs_log_conn_once_entries_exist(self, recorded):
        batch = _batch([_prep(7, "<a@x>")])
        batch.insert_log_notes(_Conn())
        with pytest.raises(RuntimeError, match="log_conn"):
            batch.send(_MailClient())


class Test_BatchConfig_log_note:
    def test_passed_to_prepared_batch_yaml(self):
        bc = BatchConfig(log_note={"category": "c", "message": "m"}, skip_email=True)
        assert bc.log_note == {"category": "c", "message": "m"}
        assert "log_note" in bc.raw_yaml.decode()

    def test_invalid(self):
        with pytest.raises(ValueError, match="log_note"):
            BatchConfig(log_note={"category": "c", "message": "m", "x": 1})
