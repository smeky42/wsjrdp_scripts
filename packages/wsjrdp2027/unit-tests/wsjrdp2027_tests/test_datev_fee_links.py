"""Tests for the pure helpers and the SQL of ``wsjrdp2027.datev_fee_links``.

The rules themselves are exercised against a real database (dev / integration);
what is unit-testable is the parsing of the DATEV text fields, the cents
conversion, and the SQL the set-based helpers build -- above all that every
rule keeps an already-linked or ``excluded_from_fee_reconciliation`` entry out
of reach.
"""

from __future__ import annotations

import datetime
import decimal
from typing import Any

import pytest
from wsjrdp2027 import datev_fee_links


class FakeCursor:
    """Records what a rule executes; ``rowcount``/``fetchall`` are canned."""

    def __init__(self, rows=(), rowcount=0):
        self.calls: list[tuple[Any, Any]] = []  # (sql, params) per execute()
        self.rows = list(rows)
        self.rowcount = rowcount

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self

    def fetchall(self):
        return list(self.rows)


# --------------------------------------------------------------- constants --


def test_classification_strings():
    """The classification_string values are stored in the database and read by
    the app -- they are part of the contract, not an implementation detail."""
    assert datev_fee_links.LINK_TYPE_2025_FEE == "2025_fee_booking"
    assert (
        datev_fee_links.LINK_TYPE_PRE_NOTIFICATION
        == "document_field_1_pre_notification"
    )
    assert (
        datev_fee_links.LINK_TYPE_RETURN
        == "retoure_matching_camt_return_by_amount_and_date"
    )
    assert datev_fee_links.LINK_SCORE == 1.0
    assert datev_fee_links.LINK_AUTHOR_ID == 1
    assert datev_fee_links.RETURN_LINK_WINDOW_DAYS == 14


# ---------------------------------------------------------- parse_person_id --


@pytest.mark.parametrize(
    "text,expected",
    [
        # Every role prefix the Buchungstext can carry.
        ("Beitrag BMT 42 Rate 1", 42),
        ("Beitrag CMT 11 Rate 2", 11),
        ("Beitrag IST 1234 Rate 3", 1234),
        ("Beitrag TN 4711 Rate 3", 4711),
        ("Beitrag UL 7 Rate 4", 7),
        ("Beitrag YP 4711 Rate 3", 4711),
        # The id is found anywhere in the text, also at its very start/end.
        ("YP 815", 815),
        ("Retoure Einzug Beitrag YP 815", 815),
        # Several spaces / a tab between role and number.
        ("Beitrag YP  99 Rate 1", 99),
        ("Beitrag YP\t99 Rate 1", 99),
        # No role prefix at all, or one that is not a role.
        ("Sammelbuchung Beitraege", None),
        ("Beitrag ABC 4711", None),
        # The prefix must be a whole word: "MYP 5" is not a YP.
        ("Beitrag MYP 5", None),
        # A number without a role prefix is not a person id.
        ("Rechnung 4711", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_person_id(text, expected):
    assert datev_fee_links.parse_person_id(text) == expected


def test_parse_person_id_takes_the_first_match():
    assert datev_fee_links.parse_person_id("YP 1 und YP 2") == 1


# ------------------------------------------------- parse_pre_notification_id --


@pytest.mark.parametrize(
    "document_field_1,expected",
    [
        ("Einzug-2026-01-RCUR-4-1717", 1717),
        ("Einzug-2025-11-FRST-12-3", 3),
        # The whole field must have the shape (no prefix/suffix, no id block).
        ("Einzug-2026-01-RCUR-4", None),
        ("X-Einzug-2026-01-RCUR-4-1717", None),
        ("Einzug-2026-01-RCUR-4-1717x", None),
        ("Einzug-2026-1-RCUR-4-1717", None),
        ("Einzug-2026-01-Rcur-4-1717", None),
        ("Rechnung 4711", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_pre_notification_id(document_field_1, expected):
    assert datev_fee_links.parse_pre_notification_id(document_field_1) == expected


# --------------------------------------------------------- amount_to_cents --


@pytest.mark.parametrize(
    "amount,expected",
    [
        ("-123.45", -12345),
        ("123.45", 12345),
        ("57", 5700),
        ("0", 0),
        ("-0.01", -1),
        ("1234567.89", 123456789),
        # A DATEV base amount never has more than two decimals; if it did, it
        # would be rounded (Decimal round-half-to-even), not truncated.
        ("0.004", 0),
        ("0.005", 0),
        ("-0.005", 0),
        ("0.006", 1),
        ("0.015", 2),
        ("0.025", 2),
    ],
)
def test_amount_to_cents(amount, expected):
    assert datev_fee_links.amount_to_cents(decimal.Decimal(amount)) == expected


# --------------------------------------------------------------- SQL shape --


def test_free_entry_condition_covers_both_gates():
    """A rule may only touch an entry that is unlinked AND not excluded."""
    condition = datev_fee_links.FREE_ENTRY_CONDITION
    assert "ae.datev_booking_id IS NULL" in condition
    assert "excluded_from_fee_reconciliation" in condition
    assert "false) = false" in condition


def test_return_match_sql_applies_the_free_entry_condition_to_the_pool():
    """The Retouren pool uses the same gate as the other two rules, so an
    excluded entry is neither matched nor counted in the +/- 14 day window
    (the window counts read the pool)."""
    sql = datev_fee_links.RETURN_MATCH_SQL
    assert datev_fee_links.FREE_ENTRY_CONDITION in sql
    assert sql.count("FROM pool w") == 1
    assert sql.count("FROM scope w") == 1


def test_return_match_sql_is_year_agnostic_and_windowed():
    sql = datev_fee_links.RETURN_MATCH_SQL
    assert "financial_year_start" not in sql  # every financial year
    assert sql.count("%s") == 1  # exactly one parameter: the guid array
    assert f"- {datev_fee_links.RETURN_LINK_WINDOW_DAYS}" in sql
    assert f"+ {datev_fee_links.RETURN_LINK_WINDOW_DAYS}" in sql
    assert "c.return_reason IS NOT NULL" in sql
    assert "c.deleted_at IS NULL" in sql
    assert "p.booking_date = b.booking_date" in sql  # booking date, not value date


# --------------------------------------------------------------- helpers ---


def test_select_unique_pairs_issues_no_statement_for_an_empty_batch():
    cur = FakeCursor()
    assert (
        datev_fee_links.select_unique_pairs(
            cur,
            {"booking_id": ("bigint", []), "person_id": ("integer", [])},
            join="TRUE",
        )
        == []
    )
    assert cur.calls == []


def test_select_unique_pairs_builds_one_symmetric_query():
    cur = FakeCursor(rows=[(7, 3)])
    pairs = datev_fee_links.select_unique_pairs(
        cur,
        {
            "booking_id": ("bigint", [3]),
            "person_id": ("integer", [11]),
            "value_date": ("date", [datetime.date(2026, 1, 2)]),
        },
        join="ae.subject_id = c.person_id",
    )
    assert pairs == [(7, 3)]
    ((sql, params),) = cur.calls
    # The candidate arrays travel in the given order, typed, through unnest.
    assert "unnest(%s::bigint[], %s::integer[], %s::date[])" in sql
    assert "cand (booking_id, person_id, value_date)" in sql
    assert params == [[3], [11], [datetime.date(2026, 1, 2)]]
    # The rule's own join plus the shared free-entry gate.
    assert "JOIN accounting_entries ae ON ae.subject_id = c.person_id" in sql
    assert datev_fee_links.FREE_ENTRY_CONDITION in sql
    # Uniqueness on BOTH sides.
    assert "count(*) OVER (PARTITION BY booking_id) AS per_booking" in sql
    assert "count(*) OVER (PARTITION BY entry_id) AS per_entry" in sql
    assert "WHERE per_booking = 1 AND per_entry = 1" in sql


def test_link_entries_issues_no_statement_for_an_empty_batch():
    cur = FakeCursor()
    datev_fee_links.link_entries(
        cur, [], link_type="whatever", now=datetime.datetime.now(datetime.UTC)
    )
    assert cur.calls == []


def test_link_entries_writes_one_update_with_the_full_meta():
    now = datetime.datetime(2026, 2, 3, 4, 5, 6, tzinfo=datetime.UTC)
    cur = FakeCursor(rowcount=2)
    datev_fee_links.link_entries(
        cur, [(7, 3), (8, 4)], link_type=datev_fee_links.LINK_TYPE_RETURN, now=now
    )
    ((sql, params),) = cur.calls
    assert sql.startswith("UPDATE accounting_entries ae SET datev_booking_id")
    # The UPDATE re-checks the entry is still free, so a link written between
    # the match query and here survives.
    assert "AND ae.datev_booking_id IS NULL" in sql
    meta, updated_at, entry_ids, booking_ids = params
    assert meta.obj == {
        "created_at": now.isoformat(),
        "author_id": 1,
        "score": 1.0,
        "automatic_manual": "automatic",
        "classification_string": ("retoure_matching_camt_return_by_amount_and_date"),
    }
    assert updated_at == now
    assert entry_ids == [7, 8]
    assert booking_ids == [3, 4]


def test_link_entries_score_is_overridable():
    cur = FakeCursor(rowcount=1)
    datev_fee_links.link_entries(
        cur,
        [(7, 3)],
        link_type="x",
        now=datetime.datetime.now(datetime.UTC),
        score=0.5,
    )
    assert cur.calls[0][1][0].obj["score"] == 0.5


def test_link_entries_warns_when_fewer_rows_were_written(caplog):
    cur = FakeCursor(rowcount=1)
    with caplog.at_level("WARNING", logger="wsjrdp2027.datev_fee_links"):
        datev_fee_links.link_entries(
            cur,
            [(7, 3), (8, 4)],
            link_type="x",
            now=datetime.datetime.now(datetime.UTC),
        )
    assert "1 Zeile(n) geschrieben" in caplog.text


def test_select_return_fee_matches_issues_no_statement_without_guids():
    cur = FakeCursor()
    assert datev_fee_links.select_return_fee_matches(cur, []) == []
    assert cur.calls == []


def test_match_return_fee_entries_issues_no_statement_without_guids():
    cur = FakeCursor()
    datev_fee_links.match_return_fee_entries(
        cur, [], now=datetime.datetime.now(datetime.UTC)
    )
    assert cur.calls == []


def test_mirror_camt_links_mirrors_only_changed_rows():
    cur = FakeCursor(rowcount=0)
    datev_fee_links.mirror_camt_links(cur, [])
    ((sql, params),) = cur.calls
    assert sql.startswith("UPDATE wsjrdp_camt_transactions c SET datev_booking_id")
    assert "IS DISTINCT FROM" in sql  # idempotent: no no-op writes
    assert params == ([],)
