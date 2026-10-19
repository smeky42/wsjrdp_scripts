"""Integration test for wsjrdp2027.insert_log_entries.

It WRITES to the independent integration-testing database
``hitobito_wsjrdp_scripts_integration_testing`` (guarded by the
``integration_testing_ctx`` fixture) and rolls everything back: a dump that
predates the wagon migration 20261020000100 gets the columns ``wsjrdp_data``
and ``additional_info`` inside the test's transaction only.
"""

from __future__ import annotations

import datetime

import pytest
import wsjrdp2027


NOW = datetime.datetime(2026, 10, 20, 12, 0, tzinfo=datetime.UTC)


@pytest.fixture
def conn(integration_testing_ctx):
    connection = integration_testing_ctx.hitobito_psycopg_connection(
        force_new=True, read_only=False
    )
    try:
        for column in ("wsjrdp_data", "additional_info"):
            connection.execute(
                f"ALTER TABLE versions ADD COLUMN IF NOT EXISTS {column} "
                "jsonb NOT NULL DEFAULT '{}'"
            )
        yield connection
    finally:
        connection.rollback()
        connection.close()


def test_insert_log_entries(conn):
    person_id = conn.execute("SELECT id FROM people ORDER BY id LIMIT 1").fetchone()[0]

    ids = wsjrdp2027.insert_log_entries(
        conn,
        [
            wsjrdp2027.LogEntry.note(
                main_id=person_id,
                category="pre_notification",
                message="informiert",
                data={"collection_date": datetime.date(2026, 11, 5)},
            ),
            wsjrdp2027.LogEntry(
                event="wsjrdp_something",
                main_id=person_id,
                additional_info={"source": "test"},
            ),
        ],
        now=NOW,
    )

    rows = conn.execute(
        "SELECT event, item_type, item_id, main_type, main_id, whodunnit,"
        " whodunnit_type, wsjrdp_data, additional_info, created_at, object_changes"
        " FROM versions WHERE id = ANY(%s) ORDER BY id",
        [ids],
    ).fetchall()
    assert len(ids) == 2
    assert rows[0][:9] == (
        "wsjrdp_note",
        "Person",
        person_id,
        "Person",
        person_id,
        "1",
        "Person",
        {
            "collection_date": "2026-11-05",
            "category": "pre_notification",
            "message": "informiert",
        },
        {},
    )
    assert rows[0][10] is None
    assert rows[1][0] == "wsjrdp_something"
    assert rows[1][7:9] == ({}, {"source": "test"})

    found = conn.execute(
        "SELECT id FROM versions WHERE wsjrdp_data @> %s::jsonb AND id = ANY(%s)",
        ['{"category": "pre_notification"}', ids],
    ).fetchall()
    assert [row[0] for row in found] == [ids[0]]
