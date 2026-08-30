"""Integration tests for wsjrdp2027._pg.pg_table_updatemany.

These tests WRITE to the database -- they run exclusively against the
independent integration-testing database
``hitobito_wsjrdp_scripts_integration_testing`` (see the ``ctx`` fixture in
integration-tests/conftest.py and the AGENTS.md section on integration tests).
A guard verifies the database identity before any write.

Unlike test_single_table_upsert_plan.py, the scratch table here DELIBERATELY survives
the test run: the fixture drops and re-seeds it BEFORE each test and the tests
commit, so after a failure the table state can be inspected interactively in
the integration-testing database.
"""

from __future__ import annotations

import datetime

import psycopg
import psycopg.sql
import pytest
import wsjrdp2027


EXPECTED_DATABASE = "hitobito_wsjrdp_scripts_integration_testing"
TABLE = "test_pg_updatemany"

SEED = [
    # (id, name, short_name, amount, flag, extra)
    (1, "Alpha", "A", 10, True, '{"a": 1}'),
    (2, "Beta", "B", 20, False, "{}"),
    (3, "Gamma", None, 30, None, '{"g": "x"}'),
    (4, "Delta", "D", 40, True, "{}"),
]


@pytest.fixture
def conn(ctx):
    """Connection with a freshly re-seeded, PERSISTENT scratch table.

    Cleanup happens BEFORE the test (drop + create + seed + commit); nothing
    is cleaned up afterwards, so the final state stays inspectable."""
    with ctx.psycopg_connect() as connection:
        dbname = connection.execute("SELECT current_database()").fetchone()[0]
        if dbname != EXPECTED_DATABASE:
            pytest.fail(
                f"SAFETY STOP: connected to {dbname!r}, expected "
                f"{EXPECTED_DATABASE!r} -- refusing to write."
            )
        table = psycopg.sql.Identifier(TABLE)
        connection.execute(psycopg.sql.SQL("DROP TABLE IF EXISTS {}").format(table))
        connection.execute(
            psycopg.sql.SQL(
                "CREATE TABLE {} ("
                " id bigserial PRIMARY KEY,"
                " name varchar,"
                " short_name varchar,"
                " amount integer,"
                " flag boolean,"
                " extra jsonb DEFAULT '{{}}',"
                " ts timestamptz,"
                " d date,"
                " created_at timestamp,"
                " updated_at timestamp)"
            ).format(table)
        )
        with connection.cursor() as cur:
            cur.executemany(
                psycopg.sql.SQL(
                    "INSERT INTO {} (id, name, short_name, amount, flag, extra) "
                    "VALUES (%s, %s, %s, %s, %s, %s::jsonb)"
                ).format(table),
                SEED,
            )
        connection.execute(
            psycopg.sql.SQL("SELECT setval(pg_get_serial_sequence(%s, 'id'), 50)"),
            (TABLE,),
        )
        connection.commit()
        yield connection


def fetch_all(conn):
    rows = conn.execute(
        psycopg.sql.SQL(
            "SELECT id, name, short_name, amount, flag, extra FROM {} ORDER BY id"
        ).format(psycopg.sql.Identifier(TABLE))
    ).fetchall()
    return {
        r[0]: {
            "name": r[1],
            "short_name": r[2],
            "amount": r[3],
            "flag": r[4],
            "extra": r[5],
        }
        for r in rows
    }


def seed_state():
    return {
        1: {
            "name": "Alpha",
            "short_name": "A",
            "amount": 10,
            "flag": True,
            "extra": {"a": 1},
        },
        2: {
            "name": "Beta",
            "short_name": "B",
            "amount": 20,
            "flag": False,
            "extra": {},
        },
        3: {
            "name": "Gamma",
            "short_name": None,
            "amount": 30,
            "flag": None,
            "extra": {"g": "x"},
        },
        4: {
            "name": "Delta",
            "short_name": "D",
            "amount": 40,
            "flag": True,
            "extra": {},
        },
    }


class Test_pg_table_updatemany:
    def test_different_columns_per_row(self, conn):
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {"id": 1, "name": "Alpha!"},
                {"id": 2, "amount": 22, "flag": True},
                {"id": 3, "short_name": "G", "extra": {"g": "y", "new": 1}},
            ],
        )
        conn.commit()
        assert updated.updated_ids == [1, 2, 3]
        expected = seed_state()
        expected[1]["name"] = "Alpha!"
        expected[2]["amount"] = 22
        expected[2]["flag"] = True
        expected[3]["short_name"] = "G"
        expected[3]["extra"] = {"g": "y", "new": 1}
        assert fetch_all(conn) == expected  # row 4 untouched

    def test_pair_iterable_form_and_identifier_args(self, conn):
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            psycopg.sql.Identifier(TABLE),
            [
                [("id", 4), ("name", "Delta!")],
                (("id", 1), ("amount", 11)),
            ],
            id_col=psycopg.sql.Identifier("id"),
        )
        conn.commit()
        assert updated.updated_ids == [4, 1]
        state = fetch_all(conn)
        assert state[4]["name"] == "Delta!"
        assert state[1]["amount"] == 11

    def test_custom_id_col(self, conn):
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [{"name": "Beta", "amount": 99}],
            key_col="name",
        )
        conn.commit()
        assert updated.updated_ids == [2]  # id_col default "id"
        assert fetch_all(conn)[2]["amount"] == 99

    def test_dict_becomes_jsonb_and_none_becomes_null(self, conn):
        hostile_extra = {"k'ey %s": 'va"lue', "nested": {"ünïcode": ["ą", 1, None]}}
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {"id": 1, "extra": hostile_extra, "short_name": None},
                {"id": 2, "name": 'O\'Reilly 100% "%s" {}'},
            ],
        )
        conn.commit()
        assert updated.updated_ids == [1, 2]
        state = fetch_all(conn)
        # dict values are PARTIAL updates now: the seeded key survives.
        assert state[1]["extra"] == {"a": 1, **hostile_extra}
        assert state[1]["short_name"] is None
        assert state[2]["name"] == 'O\'Reilly 100% "%s" {}'

    def test_missing_id_column_raises_before_writing(self, conn):
        with pytest.raises(ValueError, match=r"updates\[1\].*key column 'id'"):
            wsjrdp2027.pg_table_updatemany(
                conn,
                TABLE,
                [{"id": 1, "name": "x"}, {"name": "no id here"}],
            )
        conn.rollback()
        assert fetch_all(conn) == seed_state()  # nothing was written

    def test_id_only_update_set_is_skipped(self, conn):
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [{"id": 1}, {"id": 2, "amount": 21}],
        )
        conn.commit()
        assert updated.updated_ids == [2]
        state = fetch_all(conn)
        assert state[1] == seed_state()[1]
        assert state[2]["amount"] == 21

    def test_empty_updates(self, conn):
        result = wsjrdp2027.pg_table_updatemany(conn, TABLE, [])
        assert (result.inserted_ids, result.updated_ids) == ([], [])
        assert result.id_col == "id"
        assert fetch_all(conn) == seed_state()

    def test_duplicate_id_last_wins(self, conn):
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {"id": 1, "name": "first"},
                {"id": 1, "name": "second", "amount": 111},
            ],
        )
        conn.commit()
        assert updated.updated_ids == [1, 1]
        state = fetch_all(conn)
        assert state[1]["name"] == "second"
        assert state[1]["amount"] == 111

    def test_unknown_id_raises_after_pipeline(self, conn):
        with pytest.raises(ValueError, match=r"2 update\(s\) hit no row.*999"):
            wsjrdp2027.pg_table_updatemany(
                conn,
                TABLE,
                [
                    {"id": 1, "name": "applied-but-rolled-back"},
                    {"id": 999, "name": "nope"},
                    {"id": 1000, "name": "nope"},
                ],
            )
        # The caller owns the transaction and rolls back on the error.
        conn.rollback()
        assert fetch_all(conn) == seed_state()

    def test_unknown_column_aborts_transaction(self, conn):
        with pytest.raises(psycopg.errors.UndefinedColumn):
            wsjrdp2027.pg_table_updatemany(
                conn,
                TABLE,
                [{"id": 1, "no_such_column": 1}],
            )
        conn.rollback()
        assert fetch_all(conn) == seed_state()

    def test_generator_input(self, conn):
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            ({"id": i, "amount": i * 100} for i in (1, 2, 3, 4)),
        )
        conn.commit()
        assert updated.updated_ids == [1, 2, 3, 4]
        assert [row["amount"] for row in fetch_all(conn).values()] == [
            100,
            200,
            300,
            400,
        ]

    def test_non_unique_key_updates_all_matching_rows(self, conn):
        # Two extra rows sharing one name; key_col="name" is NOT unique.
        table = psycopg.sql.Identifier(TABLE)
        with conn.cursor() as cur:
            cur.executemany(
                psycopg.sql.SQL(
                    "INSERT INTO {} (id, name, amount) VALUES (%s, %s, %s)"
                ).format(table),
                [(10, "Dup", 1), (11, "Dup", 2)],
            )
        conn.commit()

        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {"name": "Dup", "amount": 77},
                {"name": "Alpha", "flag": False},
            ],
            key_col="name",
        )
        conn.commit()
        # One key hit two rows -> two ids; RETURNING order within one
        # statement is database-determined, hence the sorted comparison.
        assert sorted(updated.updated_ids) == [1, 10, 11]
        state = fetch_all(conn)
        assert state[10]["amount"] == state[11]["amount"] == 77
        assert state[1]["flag"] is False

    def test_non_unique_key_duplicate_sets_last_wins_on_all_rows(self, conn):
        table = psycopg.sql.Identifier(TABLE)
        with conn.cursor() as cur:
            cur.executemany(
                psycopg.sql.SQL(
                    "INSERT INTO {} (id, name, amount) VALUES (%s, %s, %s)"
                ).format(table),
                [(10, "Dup", 1), (11, "Dup", 2)],
            )
        conn.commit()

        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {"name": "Dup", "amount": 50},
                {"name": "Dup", "amount": 60},
            ],
            key_col="name",
        )
        conn.commit()
        # Both statements each hit both rows: 4 ids, 2 keys.
        assert sorted(updated.updated_ids) == [10, 10, 11, 11]
        state = fetch_all(conn)
        assert state[10]["amount"] == state[11]["amount"] == 60

    def test_distinct_id_and_key_col(self, conn):
        # key_col selects by name, id_col reports the short_name values.
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [{"name": "Alpha", "amount": 5}, {"name": "Beta", "amount": 6}],
            key_col="name",
            id_col="short_name",
        )
        conn.commit()
        assert updated.updated_ids == ["A", "B"]
        assert updated.id_col == "short_name"

    def test_larger_batch(self, conn):
        table = psycopg.sql.Identifier(TABLE)
        with conn.cursor() as cur:
            cur.executemany(
                psycopg.sql.SQL("INSERT INTO {} (id, name) VALUES (%s, %s)").format(
                    table
                ),
                [(i, f"row {i}") for i in range(100, 400)],
            )
        conn.commit()
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {"id": i, "name": f"updated {i}", "extra": {"i": i}}
                for i in range(100, 400)
            ],
        )
        conn.commit()
        assert updated.updated_ids == list(range(100, 400))
        state = fetch_all(conn)
        assert state[100]["name"] == "updated 100"
        assert state[399]["extra"] == {"i": 399}


class Test_pg_table_updatemany_composite_key:
    """key_col as a SEQUENCE of column names: the update set must contain all
    of them and a row is selected by matching every one (the shape of the
    DATEV Buchungsstapel identity tuple)."""

    def test_matches_all_key_columns(self, conn):
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [{"name": "Alpha", "short_name": "A", "amount": 99}],
            key_col=("name", "short_name"),
        )
        conn.commit()
        assert updated.updated_ids == [1]
        state = fetch_all(conn)
        assert state[1]["amount"] == 99
        # Key columns select, they are not written.
        assert state[1]["name"] == "Alpha"

    def test_missing_key_column_raises(self, conn):
        with pytest.raises(ValueError, match=r"updates\[0\].*'short_name'"):
            wsjrdp2027.pg_table_updatemany(
                conn,
                TABLE,
                [{"name": "Alpha", "amount": 1}],
                key_col=("name", "short_name"),
            )
        conn.rollback()
        assert fetch_all(conn) == seed_state()

    def test_missed_tuple_raises_with_tuple_in_message(self, conn):
        with pytest.raises(
            ValueError, match=r"\(name, short_name\) = \('Alpha', 'nope'\)"
        ):
            wsjrdp2027.pg_table_updatemany(
                conn,
                TABLE,
                # Half-matching tuple: name exists, the short_name does not.
                [{"name": "Alpha", "short_name": "nope", "amount": 1}],
                key_col=("name", "short_name"),
            )
        conn.rollback()
        assert fetch_all(conn) == seed_state()

    def test_key_only_set_is_skipped(self, conn):
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {"name": "Alpha", "short_name": "A"},
                {"name": "Beta", "short_name": "B", "amount": 21},
            ],
            key_col=("name", "short_name"),
        )
        conn.commit()
        assert updated.updated_ids == [2]
        assert fetch_all(conn)[1] == seed_state()[1]

    def test_empty_key_col_sequence_raises(self, conn):
        with pytest.raises(ValueError, match="at least one column"):
            wsjrdp2027.pg_table_updatemany(
                conn, TABLE, [{"id": 1, "amount": 1}], key_col=()
            )
        conn.rollback()


class Test_pg_table_insertmany:
    def test_heterogeneous_inserts_with_generated_ids(self, conn):
        result = wsjrdp2027.pg_table_insertmany(
            conn,
            TABLE,
            [
                {"name": "Neu1", "amount": 1, "extra": {"a": 1}},
                {"name": "Neu2", "flag": True},
            ],
        )
        conn.commit()
        # bigserial: the sequence was set to 50 -> generated ids 51, 52.
        assert result.inserted_ids == [51, 52]
        assert result.updated_ids == []
        assert result.id_col == "id"
        state = fetch_all(conn)
        assert state[51]["name"] == "Neu1"
        assert state[51]["extra"] == {"a": 1}
        assert state[52]["flag"] is True
        assert state[52]["amount"] is None  # column absent -> default

    def test_empty_value_set_inserts_default_row(self, conn):
        result = wsjrdp2027.pg_table_insertmany(conn, TABLE, [{}, {"name": "X"}])
        conn.commit()
        assert result.inserted_ids == [51, 52]
        state = fetch_all(conn)
        assert state[51]["name"] is None  # all defaults
        assert state[51]["extra"] == {}
        assert state[52]["name"] == "X"

    def test_duplicate_primary_key_raises(self, conn):
        with pytest.raises(psycopg.errors.UniqueViolation):
            wsjrdp2027.pg_table_insertmany(conn, TABLE, [{"id": 1, "name": "dup"}])
        conn.rollback()
        assert fetch_all(conn) == seed_state()

    def test_empty_inserts(self, conn):
        result = wsjrdp2027.pg_table_insertmany(conn, TABLE, [])
        assert (result.inserted_ids, result.updated_ids) == ([], [])
        assert result.id_col == "id"
        assert fetch_all(conn) == seed_state()


AWARE_NOW = datetime.datetime(2027, 8, 1, 8, 0, 0, tzinfo=datetime.UTC)


class Test_SpecialValue_markers:
    def test_scalar_delete_sets_null_and_dict_delete_removes_key(self, conn):
        updated = wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {
                    "id": 1,
                    "short_name": wsjrdp2027.SpecialValue.DELETE,
                    "extra": {"a": wsjrdp2027.SpecialValue.DELETE, "neu": 5},
                }
            ],
        )
        conn.commit()
        assert updated.updated_ids == [1]
        state = fetch_all(conn)[1]
        assert state["short_name"] is None  # scalar DELETE -> NULL
        assert state["extra"] == {"neu": 5}  # "a" deleted, "neu" set

    def test_jsonb_partial_update_preserves_unaddressed_keys(self, conn):
        conn.execute(
            psycopg.sql.SQL("UPDATE {} SET extra = %s::jsonb WHERE id = 3").format(
                psycopg.sql.Identifier(TABLE)
            ),
            ('{"keep": 1, "old": 2, "g": "x"}',),
        )
        conn.commit()
        wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [{"id": 3, "extra": {"old": wsjrdp2027.SpecialValue.DELETE, "g": "y"}}],
        )
        conn.commit()
        assert fetch_all(conn)[3]["extra"] == {"keep": 1, "g": "y"}

    def test_jsonb_merge_robust_against_stored_null(self, conn):
        conn.execute(
            psycopg.sql.SQL("UPDATE {} SET extra = NULL WHERE id = 2").format(
                psycopg.sql.Identifier(TABLE)
            )
        )
        conn.commit()
        wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [{"id": 2, "extra": {"neu": 1, "weg": wsjrdp2027.SpecialValue.DELETE}}],
        )
        conn.commit()
        assert fetch_all(conn)[2]["extra"] == {"neu": 1}

    def test_now_and_today_in_updatemany(self, conn):
        wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {
                    "id": 1,
                    "ts": wsjrdp2027.SpecialValue.NOW,
                    "d": wsjrdp2027.SpecialValue.TODAY,
                }
            ],
            now=AWARE_NOW,
        )
        conn.commit()
        ts, d = conn.execute(
            psycopg.sql.SQL("SELECT ts, d FROM {} WHERE id = 1").format(
                psycopg.sql.Identifier(TABLE)
            )
        ).fetchone()
        assert ts == AWARE_NOW
        assert d == AWARE_NOW.date()

    def test_now_and_dict_delete_in_insertmany(self, conn):
        result = wsjrdp2027.pg_table_insertmany(
            conn,
            TABLE,
            [
                {
                    "name": "Markiert",
                    "ts": wsjrdp2027.SpecialValue.NOW,
                    "d": wsjrdp2027.SpecialValue.TODAY,
                    "amount": wsjrdp2027.SpecialValue.DELETE,
                    "extra": {"a": 1, "weg": wsjrdp2027.SpecialValue.DELETE},
                }
            ],
            now=AWARE_NOW,
        )
        conn.commit()
        (new_id,) = result.inserted_ids
        name, ts, d, amount, extra = conn.execute(
            psycopg.sql.SQL(
                "SELECT name, ts, d, amount, extra FROM {} WHERE id = %s"
            ).format(psycopg.sql.Identifier(TABLE)),
            (new_id,),
        ).fetchone()
        assert (name, ts, d, amount) == ("Markiert", AWARE_NOW, AWARE_NOW.date(), None)
        assert extra == {"a": 1}  # DELETE inside dict: key not set

    def test_datetime_date_now_today_in_dict_become_iso_strings(self, conn):
        naive = datetime.datetime(2027, 8, 1, 8, 0, 0)  # noqa: DTZ001
        wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {
                    "id": 1,
                    "extra": {
                        "ts": wsjrdp2027.SpecialValue.NOW,
                        "d": wsjrdp2027.SpecialValue.TODAY,
                        "aware": AWARE_NOW,
                        "naive": naive,
                        "date": datetime.date(2027, 8, 3),
                        "nested": {"list": [AWARE_NOW]},
                    },
                }
            ],
            now=AWARE_NOW,
        )
        conn.commit()
        extra = fetch_all(conn)[1]["extra"]
        # Pinned Europe/Zurich, Rails as_json shape (.SSS+HH:MM); the UTC
        # AWARE_NOW renders as 10:00+02:00, the naive value is read as
        # Zurich wall time.
        assert extra["ts"] == "2027-08-01T10:00:00.000+02:00"
        assert extra["aware"] == "2027-08-01T10:00:00.000+02:00"
        assert extra["naive"] == "2027-08-01T08:00:00.000+02:00"
        assert extra["d"] == "2027-08-01"
        assert extra["date"] == "2027-08-03"
        assert extra["nested"] == {"list": ["2027-08-01T10:00:00.000+02:00"]}
        assert extra["a"] == 1  # unaddressed key untouched

    def test_time_zone_override_and_ctx_default(self, ctx, conn):
        assert str(ctx.hitobito_time_zone) == "Europe/Zurich"
        wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [{"id": 2, "extra": {"ts": wsjrdp2027.SpecialValue.NOW}}],
            now=AWARE_NOW,
            time_zone="America/New_York",
        )
        conn.commit()
        assert fetch_all(conn)[2]["extra"]["ts"] == "2027-08-01T04:00:00.000-04:00"

    def test_nested_delete_in_dict_raises(self, conn):
        with pytest.raises(ValueError, match="TOP-LEVEL"):
            wsjrdp2027.pg_table_updatemany(
                conn,
                TABLE,
                [{"id": 1, "extra": {"n": {"x": wsjrdp2027.SpecialValue.DELETE}}}],
            )
        conn.rollback()

    def test_now_accepts_to_datetime_forms(self, conn):
        # A str form accepted by wsjrdp2027._util.to_datetime.
        wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [{"id": 1, "ts": wsjrdp2027.SpecialValue.NOW}],
            now="2027-08-01 08:00:00+00:00",
        )
        conn.commit()
        (ts,) = conn.execute(
            psycopg.sql.SQL("SELECT ts FROM {} WHERE id = 1").format(
                psycopg.sql.Identifier(TABLE)
            )
        ).fetchone()
        assert ts == AWARE_NOW


def fetch_stamps(conn, row_id):
    return conn.execute(
        psycopg.sql.SQL("SELECT created_at, updated_at FROM {} WHERE id = %s").format(
            psycopg.sql.Identifier(TABLE)
        ),
        (row_id,),
    ).fetchone()


class Test_touch:
    """`touch` (None = not set = on) stamps created_at in insertmany and
    updated_at in updatemany with the resolved `now`. The columns are naive
    timestamps and the test session runs in Etc/UTC, so an aware `now` lands
    UTC-naive -- the Rails convention."""

    def test_updatemany_default_stamps_updated_at(self, conn):
        wsjrdp2027.pg_table_updatemany(
            conn, TABLE, [{"id": 1, "name": "Neu"}], now=AWARE_NOW
        )
        conn.commit()
        created_at, updated_at = fetch_stamps(conn, 1)
        assert updated_at == datetime.datetime(2027, 8, 1, 8, 0)  # noqa: DTZ001
        assert created_at is None  # updates never stamp created_at

    def test_updatemany_touch_false_and_explicit_value(self, conn):
        wsjrdp2027.pg_table_updatemany(
            conn, TABLE, [{"id": 1, "name": "a"}], now=AWARE_NOW, touch=False
        )
        explicit = datetime.datetime(2030, 1, 1, 12, 0)  # noqa: DTZ001
        wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [{"id": 2, "name": "b", "updated_at": explicit}],
            now=AWARE_NOW,
        )
        conn.commit()
        assert fetch_stamps(conn, 1) == (None, None)  # touch=False: no stamp
        # An explicit updated_at wins over the stamp (and does not clash with
        # it -- no duplicate SET column).
        assert fetch_stamps(conn, 2) == (None, explicit)

    def test_updatemany_key_only_set_stays_skipped(self, conn):
        result = wsjrdp2027.pg_table_updatemany(conn, TABLE, [{"id": 1}], now=AWARE_NOW)
        conn.commit()
        # touch never turns a key-only skip into a pure updated_at write.
        assert result.updated_ids == []
        assert fetch_stamps(conn, 1) == (None, None)

    def test_insertmany_default_stamps_created_at(self, conn):
        result = wsjrdp2027.pg_table_insertmany(
            conn, TABLE, [{"name": "frisch"}], now=AWARE_NOW
        )
        conn.commit()
        (new_id,) = result.inserted_ids
        created_at, updated_at = fetch_stamps(conn, new_id)
        assert created_at == datetime.datetime(2027, 8, 1, 8, 0)  # noqa: DTZ001
        assert updated_at is None  # inserts never stamp updated_at

    def test_insertmany_touch_false_and_explicit_created_at(self, conn):
        explicit = datetime.datetime(2030, 1, 1, 12, 0)  # noqa: DTZ001
        off = wsjrdp2027.pg_table_insertmany(
            conn, TABLE, [{"name": "aus"}], now=AWARE_NOW, touch=False
        )
        explicit_result = wsjrdp2027.pg_table_insertmany(
            conn, TABLE, [{"name": "explizit", "created_at": explicit}], now=AWARE_NOW
        )
        conn.commit()
        assert fetch_stamps(conn, off.inserted_ids[0]) == (None, None)
        # An explicit created_at wins over the stamp (no duplicate column).
        assert fetch_stamps(conn, explicit_result.inserted_ids[0]) == (explicit, None)

    def test_insertmany_empty_value_set(self, conn):
        stamped = wsjrdp2027.pg_table_insertmany(conn, TABLE, [{}], now=AWARE_NOW)
        plain = wsjrdp2027.pg_table_insertmany(conn, TABLE, [{}], touch=False)
        conn.commit()
        # With touch, even an otherwise-empty row gets created_at; touch=False
        # keeps the pure DEFAULT VALUES insert.
        assert fetch_stamps(conn, stamped.inserted_ids[0]) == (
            datetime.datetime(2027, 8, 1, 8, 0),  # noqa: DTZ001
            None,
        )
        assert fetch_stamps(conn, plain.inserted_ids[0]) == (None, None)

    def test_naive_now_is_read_in_time_zone(self, conn):
        # 23:30 naive is read as wall time in the (default Europe/Zurich)
        # time_zone: the instant 21:30Z, date 2027-08-01. A system-zone
        # reading would shift TODAY to 2027-08-02 on a UTC runner.
        naive_now = datetime.datetime(2027, 8, 1, 23, 30)  # noqa: DTZ001
        wsjrdp2027.pg_table_updatemany(
            conn,
            TABLE,
            [
                {
                    "id": 3,
                    "ts": wsjrdp2027.SpecialValue.NOW,
                    "d": wsjrdp2027.SpecialValue.TODAY,
                }
            ],
            now=naive_now,
        )
        conn.commit()
        ts, d = conn.execute(
            psycopg.sql.SQL("SELECT ts, d FROM {} WHERE id = 3").format(
                psycopg.sql.Identifier(TABLE)
            )
        ).fetchone()
        assert ts == datetime.datetime(2027, 8, 1, 21, 30, tzinfo=datetime.UTC)
        assert d == datetime.date(2027, 8, 1)
