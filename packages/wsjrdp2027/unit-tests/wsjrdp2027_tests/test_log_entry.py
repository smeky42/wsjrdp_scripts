from __future__ import annotations

import datetime

from wsjrdp2027 import LogEntry


class Test_LogEntry:
    def test_note_row(self):
        entry = LogEntry.note(
            main_id=42,
            category="pre_notification",
            message="informiert",
            data={"category": "lost", "collection_date": datetime.date(2026, 11, 5)},
        )
        assert entry.to_row() == {
            "event": "wsjrdp_note",
            "item_type": "Person",
            "item_id": 42,
            "main_type": "Person",
            "main_id": 42,
            "whodunnit_type": "Person",
            "whodunnit": "1",
            "wsjrdp_data": {
                "category": "pre_notification",
                "message": "informiert",
                "collection_date": "2026-11-05",
            },
            "additional_info": {},
        }

    def test_item_other_than_main_and_no_author(self):
        created_at = datetime.datetime(2026, 10, 20, 12, 0, tzinfo=datetime.UTC)
        entry = LogEntry(
            event="wsjrdp_something",
            main_id=42,
            item_type="Group",
            item_id=7,
            whodunnit_id=None,
            additional_info={"source": "script", "at": [created_at]},
            created_at=created_at,
        )
        row = entry.to_row()
        assert (row["item_type"], row["item_id"]) == ("Group", 7)
        assert (row["main_type"], row["main_id"]) == ("Person", 42)
        assert row["whodunnit"] is None
        assert row["wsjrdp_data"] == {}
        assert row["additional_info"] == {
            "source": "script",
            "at": ["2026-10-20T12:00:00+00:00"],
        }
        assert row["created_at"] == created_at
