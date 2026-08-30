"""Integration tests for wsjrdp2027._internal.single_table_upsert_plan.

These tests WRITE to the database -- they run exclusively against the
independent integration-testing database
``hitobito_wsjrdp_scripts_integration_testing`` (see the ``ctx`` fixture in
integration-tests/conftest.py and the AGENTS.md section on integration tests).
The fixture verifies the database identity before any write and hard-fails
otherwise.

All work happens in a scratch table owned by this module; no hitobito table is
touched. Every test runs inside a transaction that is rolled back.
"""

from __future__ import annotations

import datetime

import psycopg
import psycopg.sql
import pytest
from wsjrdp2027 import SpecialValue
from wsjrdp2027._internal.single_table_upsert_plan import (
    OperationCounts,
    SingleTableUpsertPlanBuilder,
)


EXPECTED_DATABASE = "hitobito_wsjrdp_scripts_integration_testing"
SCRATCH_TABLE = "test_single_table_upsert_plan_scratch"


# Naive datetimes on purpose: the pg_table_* helpers read them as wall time
# in the (default Europe/Zurich) time zone; the timestamp columns are
# "timestamp without time zone".
NOW = datetime.datetime(2027, 8, 1, 8, 0, 0)  # noqa: DTZ001
LATER = datetime.datetime(2027, 8, 2, 9, 30, 0)  # noqa: DTZ001


@pytest.fixture
def rw_conn(integration_testing_ctx):
    """Connection to the integration-testing DB with a scratch table; the
    whole fixture runs in one transaction that is ALWAYS rolled back."""
    ctx = integration_testing_ctx
    new_rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
    new_rw_conn.execute(t"DROP TABLE IF EXISTS {SCRATCH_TABLE:i};")
    new_rw_conn.execute(
        t"""CREATE TABLE {SCRATCH_TABLE:i} (
            number varchar PRIMARY KEY,
            name varchar,
            short_name varchar,
            amount integer,
            extra jsonb NOT NULL DEFAULT '{{}}',
            created_at timestamp,
            updated_at timestamp)"""
    )
    new_rw_conn.commit()
    try:
        yield new_rw_conn
    finally:
        new_rw_conn.close()


def fetch_all(conn):
    rows = conn.execute(
        psycopg.sql.SQL(
            "SELECT number, name, short_name, amount, extra, created_at, "
            "updated_at FROM {} ORDER BY number"
        ).format(psycopg.sql.Identifier(SCRATCH_TABLE))
    ).fetchall()
    return {
        r[0]: {
            "name": r[1],
            "short_name": r[2],
            "amount": r[3],
            "extra": r[4],
            "created_at": r[5],
            "updated_at": r[6],
        }
        for r in rows
    }


def run_cycle(conn, values, *, cp1252=False, now=NOW, touch=None):
    builder = SingleTableUpsertPlanBuilder(SCRATCH_TABLE, "number", values)
    builder.load_existing(conn)
    planned = builder.plan(skip_update_for_cp1252_equality=cp1252)
    inserted, updated = planned.apply(conn, now=now, touch=touch)
    return planned, inserted, updated


class Test_SingleTableUpsertPlan_lifecycle:
    def test_plan_before_load_raises(self):
        upsert = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1"}]
        )
        with pytest.raises(RuntimeError, match="load_existing"):
            upsert.plan()

    def test_apply_twice_raises_and_new_cycle_works(self, rw_conn):
        builder = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1", "name": "x"}]
        )
        builder.load_existing(rw_conn)
        planned = builder.plan()
        planned.apply(rw_conn, now=NOW)
        with pytest.raises(RuntimeError, match="already been applied"):
            planned.apply(rw_conn, now=NOW)

        # New cycle: re-load + re-plan gives a FRESH plan; now everything is
        # untouched.
        builder.load_existing(rw_conn)
        planned = builder.plan()
        assert planned.untouched_keys == ["1"]
        assert planned.apply(rw_conn, now=LATER) == ([], [])

    def test_duplicate_key_in_constructor_raises(self, rw_conn):
        with pytest.raises(ValueError, match=r"number = '1' twice"):
            SingleTableUpsertPlanBuilder(
                SCRATCH_TABLE,
                "number",
                [{"number": "1", "name": "a"}, {"number": "1", "name": "b"}],
            )
        # merge_values stays the way to COMBINE sources for the same key.
        upsert = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1", "name": "a"}]
        )
        upsert.merge_values([{"number": "1", "name": "b"}])
        upsert.load_existing(rw_conn)
        planned = upsert.plan()
        assert planned.inserts == [{"number": "1", "name": "b"}]

    def test_missing_key_and_bad_key_col_raise_at_construction(self, rw_conn):
        with pytest.raises(ValueError, match=r"values\[1\].*'number'"):
            SingleTableUpsertPlanBuilder(
                SCRATCH_TABLE, "number", [{"number": "1"}, {"name": "x"}]
            )
        with pytest.raises(TypeError, match="plain column name"):
            # Deliberate type violation: the runtime TypeError is the test.
            SingleTableUpsertPlanBuilder(
                SCRATCH_TABLE,
                psycopg.sql.Identifier("number"),  # ty: ignore[invalid-argument-type]
                [],
            )


class Test_SingleTableUpsertPlan_merge_values:
    def test_merge_after_load_raises(self, rw_conn):
        upsert = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1"}]
        )
        upsert.load_existing(rw_conn)
        with pytest.raises(RuntimeError, match="load_existing"):
            upsert.merge_values([{"number": "2"}])

    def test_merge_new_keys_and_column_overwrite(self, rw_conn):
        upsert = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1", "name": "Moss", "amount": 1}]
        )
        upsert.merge_values(
            [{"number": "1", "name": "Later"}, {"number": "2", "name": "New"}]
        )
        upsert.load_existing(rw_conn)
        planned = upsert.plan()
        inserted, _ = planned.apply(rw_conn, now=NOW)
        assert inserted == ["1", "2"]
        state = fetch_all(rw_conn)
        assert state["1"]["name"] == "Later"  # later file wins
        assert state["1"]["amount"] == 1  # non-colliding column kept
        assert state["2"]["name"] == "New"

    def test_merge_translit_keeps_existing_unicode(self, rw_conn):
        upsert = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE,
            "number",
            [{"number": "1", "name": "Gdańsk", "extra": {"Ort": "Łódź"}}],
        )
        # A DATEV-style file merged over the Moss Unicode values: mere
        # transliterations keep the existing values, a genuine change wins.
        upsert.merge_values(
            [
                {
                    "number": "1",
                    "name": "Gdansk",
                    "short_name": "Neu",
                    "extra": {"Ort": "Lódz", "Zusatz": "x"},
                }
            ],
            keep_existing_for_cp1252_equality=("name", "extra"),
        )
        upsert.load_existing(rw_conn)
        upsert.plan().apply(rw_conn, now=NOW)
        state = fetch_all(rw_conn)["1"]
        assert state["name"] == "Gdańsk"  # translit kept
        assert state["short_name"] == "Neu"  # new column merged in
        assert state["extra"] == {"Ort": "Łódź", "Zusatz": "x"}

    def test_merge_delete_key_marker_survives_merge(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "extra": {"a": 1, "b": 2}}])
        upsert = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1", "extra": {"a": 10}}]
        )
        upsert.merge_values([{"number": "1", "extra": {"b": SpecialValue.DELETE}}])
        upsert.load_existing(rw_conn)
        upsert.plan().apply(rw_conn, now=LATER)
        assert fetch_all(rw_conn)["1"]["extra"] == {"a": 10}

    def test_merge_unknown_translit_column_raises(self, rw_conn):
        upsert = SingleTableUpsertPlanBuilder(SCRATCH_TABLE, "number", [])
        with pytest.raises(ValueError, match="absent"):
            upsert.merge_values(
                [{"number": "1", "name": "x"}],
                keep_existing_for_cp1252_equality=("nope",),
            )

    def test_single_write_over_two_merged_files(self, rw_conn):
        # End-to-end shape of the cost-center import: two sources, ONE write.
        run_cycle(rw_conn, [{"number": "1", "name": "Alt", "amount": 1}])
        upsert = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE,
            "number",
            [{"number": "1", "name": "Neu", "amount": 1}],
        )
        upsert.merge_values(
            [{"number": "1", "short_name": "K"}, {"number": "2", "name": "Zwei"}]
        )
        upsert.load_existing(rw_conn)
        planned = upsert.plan()
        inserted, updated = planned.apply(rw_conn, now=LATER)
        assert (inserted, updated) == (["2"], ["1"])
        assert planned.updates == [{"name": "Neu", "short_name": "K", "number": "1"}]


class Test_SingleTableUpsertPlan_planning:
    def test_insert_update_untouched_with_per_row_columns(self, rw_conn):
        planned, inserted, updated = run_cycle(
            rw_conn,
            [
                {"number": "1", "name": "Alpha", "amount": 10, "extra": {"a": 1}},
                {"number": "2", "name": "Beta"},
            ],
        )
        assert (inserted, updated) == (["1", "2"], [])
        state = fetch_all(rw_conn)
        assert state["1"]["extra"] == {"a": 1}
        assert state["2"]["amount"] is None  # column absent -> DB default
        # The helpers stamp with the RESOLVED now: the naive NOW is read as
        # Zurich wall time (+02:00 in August) and lands in the naive
        # timestamp column through the UTC test session -- UTC-naive, the
        # Rails convention.
        assert state["1"]["created_at"] == NOW - datetime.timedelta(hours=2)
        assert state["1"]["updated_at"] is None  # fresh row: never updated

        # Second cycle: one row untouched, one row with ONE changed column.
        planned, inserted, updated = run_cycle(
            rw_conn,
            [
                {"number": "1", "name": "Alpha", "amount": 11, "extra": {"a": 1}},
                {"number": "2", "name": "Beta"},
            ],
            now=LATER,
        )
        assert (inserted, updated) == ([], ["1"])
        assert planned.untouched_keys == ["2"]
        # Column-granular: only the changed column (plus key) is in the update.
        assert planned.updates == [{"amount": 11, "number": "1"}]
        state = fetch_all(rw_conn)
        assert state["1"]["amount"] == 11
        assert state["1"]["updated_at"] == LATER - datetime.timedelta(hours=2)
        assert state["2"]["updated_at"] is None  # still never updated

    def test_absent_column_untouched_none_sets_null(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "name": "N", "short_name": "S"}])
        # name absent -> stays; short_name None -> NULL.
        planned, _, updated = run_cycle(
            rw_conn, [{"number": "1", "short_name": None}], now=LATER
        )
        assert updated == ["1"]
        assert planned.updates == [{"short_name": None, "number": "1"}]
        state = fetch_all(rw_conn)["1"]
        assert state["name"] == "N"
        assert state["short_name"] is None

    def test_jsonb_merge_set_keep_delete(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "extra": {"keep": 1, "old": 2, "upd": 3}}])
        planned, _, updated = run_cycle(
            rw_conn,
            [
                {
                    "number": "1",
                    "extra": {"old": SpecialValue.DELETE, "upd": 30, "new": 4},
                }
            ],
            now=LATER,
        )
        assert updated == ["1"]
        assert fetch_all(rw_conn)["1"]["extra"] == {"keep": 1, "upd": 30, "new": 4}

        # Deleting a missing key + re-setting identical values: untouched.
        planned, _, _ = run_cycle(
            rw_conn,
            [{"number": "1", "extra": {"old": SpecialValue.DELETE, "upd": 30}}],
            now=LATER,
        )
        assert planned.untouched_keys == ["1"]

    def test_jsonb_delta_is_minimal(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "extra": {"same": 1, "upd": 2}}])
        planned, _, updated = run_cycle(
            rw_conn,
            [
                {
                    "number": "1",
                    "extra": {"same": 1, "upd": 20, "gone": SpecialValue.DELETE},
                }
            ],
            now=LATER,
        )
        assert updated == ["1"]
        # Only the genuinely changing key is written: the equal value and the
        # DELETE of an absent key are dropped from the delta.
        assert planned.updates == [{"extra": {"upd": 20}, "number": "1"}]
        assert fetch_all(rw_conn)["1"]["extra"] == {"same": 1, "upd": 20}

    def test_jsonb_translit_key_survives_mixed_update(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "extra": {"Ort": "Łódź", "n": 1}}])
        # One key genuinely changes, the other is a mere CP1252
        # transliteration of the stored Unicode value: only the real change
        # is written, the Unicode value survives.
        planned, _, updated = run_cycle(
            rw_conn,
            [{"number": "1", "extra": {"Ort": "Lódz", "n": 2}}],
            cp1252=["extra"],
            now=LATER,
        )
        assert updated == ["1"]
        assert planned.updates == [{"extra": {"n": 2}, "number": "1"}]
        assert fetch_all(rw_conn)["1"]["extra"] == {"Ort": "Łódź", "n": 2}

    def test_sentinel_on_insert_is_dropped(self, rw_conn):
        planned, inserted, _ = run_cycle(
            rw_conn, [{"number": "9", "extra": {"a": 1, "b": SpecialValue.DELETE}}]
        )
        assert inserted == ["9"]
        assert fetch_all(rw_conn)["9"]["extra"] == {"a": 1}

    def test_affected_tables_and_operation_counts(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "name": "Alt"}])
        builder = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE,
            "number",
            [
                {"number": "1", "name": "Neu"},
                {"number": "2", "name": "Zwei"},
                {"number": "3", "name": "Drei"},
            ],
        )
        builder.load_existing(rw_conn)
        planned = builder.plan()
        assert planned.affected_tables == (SCRATCH_TABLE,)
        assert planned.operation_counts() == {
            SCRATCH_TABLE: OperationCounts(inserts=2, updates=1, deletes=0)
        }


class Test_SingleTableUpsertPlan_cp1252:
    def seed(self, rw_conn):
        run_cycle(
            rw_conn,
            [{"number": "1", "name": "Gdańsk", "extra": {"Ort": "Łódź", "n": 1}}],
        )

    def test_translit_equal_counts_as_untouched(self, rw_conn):
        self.seed(rw_conn)
        # Incoming DATEV transliterations of the stored Unicode values.
        planned, _, _ = run_cycle(
            rw_conn,
            [{"number": "1", "name": "Gdansk", "extra": {"Ort": "Lódz", "n": 1}}],
            cp1252=True,
            now=LATER,
        )
        assert planned.untouched_keys == ["1"]
        assert fetch_all(rw_conn)["1"]["name"] == "Gdańsk"  # Unicode kept

    def test_translit_equal_column_dropped_from_changed_row(self, rw_conn):
        self.seed(rw_conn)
        planned, _, updated = run_cycle(
            rw_conn,
            [{"number": "1", "name": "Gdansk", "amount": 5}],
            cp1252=True,
            now=LATER,
        )
        assert updated == ["1"]
        # name is translit-equal -> NOT part of the update; amount is.
        assert planned.updates == [{"amount": 5, "number": "1"}]
        assert fetch_all(rw_conn)["1"]["name"] == "Gdańsk"

    def test_asymmetry_unicode_incoming_updates(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "name": "Gdansk"}])
        # Stored transliteration, incoming Unicode: NOT equal -> update.
        planned, _, updated = run_cycle(
            rw_conn, [{"number": "1", "name": "Gdańsk"}], cp1252=True, now=LATER
        )
        assert updated == ["1"]
        assert fetch_all(rw_conn)["1"]["name"] == "Gdańsk"

    def test_column_list_mode(self, rw_conn):
        self.seed(rw_conn)
        run_cycle(rw_conn, [{"number": "1", "short_name": "Łuk"}], now=NOW)
        # Only short_name is translit-protected; name gets the plain compare.
        planned, _, updated = run_cycle(
            rw_conn,
            [{"number": "1", "name": "Gdansk", "short_name": "Luk"}],
            cp1252=["short_name"],
            now=LATER,
        )
        assert updated == ["1"]
        assert planned.updates == [{"name": "Gdansk", "number": "1"}]
        state = fetch_all(rw_conn)["1"]
        assert state["name"] == "Gdansk"  # plainly compared -> overwritten
        assert state["short_name"] == "Łuk"  # translit-protected

    def test_unknown_column_in_list_raises(self, rw_conn):
        upsert = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1", "name": "x"}]
        )
        upsert.load_existing(rw_conn)
        with pytest.raises(ValueError, match="unknown"):
            upsert.plan(skip_update_for_cp1252_equality=["nope"])


class Test_SingleTableUpsertPlan_markers:
    def test_delete_key_is_enum_member(self):
        assert SpecialValue.DELETE is SpecialValue.DELETE
        assert isinstance(SpecialValue.DELETE, SpecialValue)

    def test_now_and_today_resolve_on_insert_and_update(self, rw_conn):
        _, inserted, _ = run_cycle(
            rw_conn,
            [{"number": "1", "created_at": SpecialValue.NOW, "name": "x"}],
            now=NOW,
        )
        assert inserted == ["1"]
        # SpecialValue handling lives in the *many helpers: a naive `now` is
        # interpreted in the configured zone (Zurich, +02:00 in August) and
        # the NOW marker resolves to that AWARE instant; storing it in the
        # naive timestamp column goes through the session time zone (UTC on
        # the test instance), hence 08:00+02:00 -> 06:00.
        assert fetch_all(rw_conn)["1"]["created_at"] == NOW - datetime.timedelta(
            hours=2
        )

        # Update path: the marker row always counts as changed, even though
        # nothing else differs.
        planned, _, updated = run_cycle(
            rw_conn,
            [{"number": "1", "created_at": SpecialValue.NOW, "name": "x"}],
            now=LATER,
        )
        assert updated == ["1"]
        assert planned.updates == [{"created_at": SpecialValue.NOW, "number": "1"}]
        assert fetch_all(rw_conn)["1"]["created_at"] == LATER - datetime.timedelta(
            hours=2
        )

    def test_today_resolves_to_date_of_now(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "name": "x"}])
        _, _, updated = run_cycle(
            rw_conn,
            [{"number": "1", "updated_at": SpecialValue.TODAY}],
            now=LATER,
        )
        assert updated == ["1"]
        # timestamp column: midnight of LATER's date.
        assert fetch_all(rw_conn)["1"]["updated_at"] == datetime.datetime(2027, 8, 2)  # noqa: DTZ001

    def test_markers_without_now_use_wall_clock(self, rw_conn):
        upsert = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1", "amount": 1}]
        )
        upsert.load_existing(rw_conn)
        upsert.plan().apply(rw_conn, now=None)
        # Without `now` the helpers fall back to the current wall clock (the
        # column is naive, the test session Etc/UTC -> UTC-naive value).
        created_at = fetch_all(rw_conn)["1"]["created_at"]
        wall_clock = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
        assert abs(created_at - wall_clock) < datetime.timedelta(minutes=10)

    def test_scalar_delete_sets_null_and_is_null_idempotent(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "name": "N", "short_name": "S"}])
        # Scalar DELETE == None: sets NULL...
        planned, _, updated = run_cycle(
            rw_conn,
            [{"number": "1", "short_name": SpecialValue.DELETE}],
            now=LATER,
        )
        assert updated == ["1"]
        assert planned.updates == [{"short_name": None, "number": "1"}]
        assert fetch_all(rw_conn)["1"]["short_name"] is None
        # ...and is untouched-idempotent once the column is NULL.
        planned, _, _ = run_cycle(
            rw_conn, [{"number": "1", "short_name": SpecialValue.DELETE}], now=LATER
        )
        assert planned.untouched_keys == ["1"]

    def test_scalar_delete_on_insert_and_in_merge(self, rw_conn):
        upsert = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "9", "name": "wird-null"}]
        )
        # A later file may DELETE a scalar column set by an earlier one.
        upsert.merge_values([{"number": "9", "name": SpecialValue.DELETE}])
        upsert.load_existing(rw_conn)
        inserted, _ = upsert.plan().apply(rw_conn, now=NOW)
        assert inserted == ["9"]
        assert fetch_all(rw_conn)["9"]["name"] is None

    def test_now_and_datetime_in_dict_serialize_and_stay_idempotent(self, rw_conn):
        aware = datetime.datetime(2027, 8, 1, 6, 0, 0, tzinfo=datetime.UTC)
        # datetime values are serialized at intake; NOW stays a marker until
        # apply and therefore always marks the row as changed.
        _, inserted, _ = run_cycle(
            rw_conn,
            [{"number": "1", "extra": {"ts": SpecialValue.NOW, "fix": aware}}],
            now=aware,
        )
        assert inserted == ["1"]
        extra = fetch_all(rw_conn)["1"]["extra"]
        assert extra["ts"] == "2027-08-01T08:00:00.000+02:00"  # Zurich
        assert extra["fix"] == "2027-08-01T08:00:00.000+02:00"

        # Without NOW: the intake-serialized datetime equals the stored
        # string -> untouched.
        planned, _, _ = run_cycle(
            rw_conn, [{"number": "1", "extra": {"fix": aware}}], now=aware
        )
        assert planned.untouched_keys == ["1"]

    def test_nested_delete_in_dict_raises(self, rw_conn):
        with pytest.raises(ValueError, match="TOP-LEVEL"):
            SingleTableUpsertPlanBuilder(
                SCRATCH_TABLE,
                "number",
                [{"number": "1", "extra": {"n": {"x": SpecialValue.DELETE}}}],
            )


class Test_SingleTableUpsertPlan_write:
    def test_hostile_values(self, rw_conn):
        hostile = 'O\'Reilly 100% "%s" {}; DROP TABLE x; --'
        planned, inserted, _ = run_cycle(
            rw_conn, [{"number": "1", "name": hostile, "extra": {"k'%s": 'v"'}}]
        )
        assert inserted == ["1"]
        state = fetch_all(rw_conn)["1"]
        assert state["name"] == hostile
        assert state["extra"] == {"k'%s": 'v"'}
        planned, _, _ = run_cycle(
            rw_conn,
            [{"number": "1", "name": hostile, "extra": {"k'%s": 'v"'}}],
            now=LATER,
        )
        assert planned.untouched_keys == ["1"]

    def test_write_touch_false_skips_timestamps(self, rw_conn):
        _, inserted, _ = run_cycle(rw_conn, [{"number": "1", "name": "x"}], touch=False)
        assert inserted == ["1"]
        _, _, updated = run_cycle(
            rw_conn, [{"number": "1", "name": "y"}], now=LATER, touch=False
        )
        assert updated == ["1"]
        state = fetch_all(rw_conn)["1"]
        assert state["created_at"] is None and state["updated_at"] is None

    def test_heterogeneous_inserts_grouped(self, rw_conn):
        planned, inserted, _ = run_cycle(
            rw_conn,
            [
                {"number": "1", "name": "a"},
                {"number": "2", "amount": 2},
                {"number": "3", "name": "c"},
            ],
        )
        assert inserted == ["1", "2", "3"]
        state = fetch_all(rw_conn)
        assert state["2"]["name"] is None
        assert state["3"]["name"] == "c"

    def test_empty_values(self, rw_conn):
        builder = SingleTableUpsertPlanBuilder(SCRATCH_TABLE, "number", [])
        builder.load_existing(rw_conn)
        planned = builder.plan()
        assert (planned.inserts, planned.updates, planned.untouched_keys) == (
            [],
            [],
            [],
        )
        assert planned.operation_counts() == {
            SCRATCH_TABLE: OperationCounts(inserts=0, updates=0, deletes=0)
        }
        assert planned.apply(rw_conn, now=NOW) == ([], [])


def run_composite_cycle(conn, values, *, now=NOW):
    """run_cycle with the COMPOSITE key (number, name) -- key values are
    tuples in exactly that order."""
    builder = SingleTableUpsertPlanBuilder(SCRATCH_TABLE, ("number", "name"), values)
    builder.load_existing(conn)
    planned = builder.plan()
    inserted, updated = planned.apply(conn, now=now)
    return planned, inserted, updated


class Test_SingleTableUpsertPlan_composite_key:
    """key_col as a sequence of column names (the shape of the DATEV
    Buchungsstapel identity tuple): key values are tuples everywhere."""

    def test_insert_update_untouched_cycle(self, rw_conn):
        planned, inserted, updated = run_composite_cycle(
            rw_conn,
            [
                {"number": "1", "name": "a", "amount": 1},
                {"number": "2", "name": "b", "amount": 2},
            ],
        )
        assert (inserted, updated) == ([("1", "a"), ("2", "b")], [])
        state = fetch_all(rw_conn)
        assert state["1"]["amount"] == 1 and state["1"]["name"] == "a"
        assert state["1"]["created_at"] == NOW - datetime.timedelta(hours=2)
        assert state["1"]["updated_at"] is None

        planned, inserted, updated = run_composite_cycle(
            rw_conn,
            [
                {"number": "1", "name": "a", "amount": 10},
                {"number": "2", "name": "b", "amount": 2},
            ],
            now=LATER,
        )
        assert (inserted, updated) == ([], [("1", "a")])
        assert planned.untouched_keys == [("2", "b")]
        # The update row carries ALL key columns plus the changed column.
        assert planned.updates == [{"amount": 10, "number": "1", "name": "a"}]
        state = fetch_all(rw_conn)
        assert state["1"]["amount"] == 10
        assert state["2"]["updated_at"] is None

    def test_duplicate_composite_key_raises(self, rw_conn):
        with pytest.raises(ValueError, match=r"\(number, name\) = \('1', 'a'\) twice"):
            SingleTableUpsertPlanBuilder(
                SCRATCH_TABLE,
                ("number", "name"),
                [
                    {"number": "1", "name": "a", "amount": 1},
                    {"number": "1", "name": "a", "amount": 2},
                ],
            )
        # Same first column with a DIFFERENT second column is a distinct key.
        SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE,
            ("number", "name"),
            [
                {"number": "1", "name": "a", "amount": 1},
                {"number": "1", "name": "b", "amount": 2},
            ],
        )

    def test_missing_key_column_and_bad_key_col_raise(self, rw_conn):
        with pytest.raises(ValueError, match=r"values\[0\].*'name'"):
            SingleTableUpsertPlanBuilder(
                SCRATCH_TABLE, ("number", "name"), [{"number": "1", "amount": 1}]
            )
        with pytest.raises(TypeError, match="non-empty sequence"):
            SingleTableUpsertPlanBuilder(SCRATCH_TABLE, (), [])
        with pytest.raises(ValueError, match="duplicate"):
            SingleTableUpsertPlanBuilder(SCRATCH_TABLE, ("number", "number"), [])

    def test_operation_counts_and_merge_values(self, rw_conn):
        run_composite_cycle(rw_conn, [{"number": "1", "name": "a", "amount": 1}])
        builder = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE,
            ("number", "name"),
            [{"number": "1", "name": "a", "amount": 5}],
        )
        # merge_values keys on the same tuples.
        builder.merge_values(
            [
                {"number": "1", "name": "a", "short_name": "S"},
                {"number": "2", "name": "b"},
            ]
        )
        builder.load_existing(rw_conn)
        planned = builder.plan()
        assert planned.operation_counts() == {
            SCRATCH_TABLE: OperationCounts(inserts=1, updates=1, deletes=0)
        }
        inserted, updated = planned.apply(rw_conn, now=LATER)
        assert (inserted, updated) == ([("2", "b")], [("1", "a")])
        state = fetch_all(rw_conn)
        assert state["1"]["amount"] == 5 and state["1"]["short_name"] == "S"


class Test_SingleTableUpsertPlan_list_columns:
    """A list value is a JSONB array column: diffed and replaced as a whole."""

    def test_insert_rerun_untouched_then_replace(self, rw_conn):
        slots = [{"num": 1, "key": "Konto", "value": "18000"}, "x", 2]
        _, inserted, _ = run_cycle(rw_conn, [{"number": "1", "extra": slots}])
        assert inserted == ["1"]
        assert fetch_all(rw_conn)["1"]["extra"] == slots

        # Identical list -> untouched (no write at all).
        planned, _, _ = run_cycle(rw_conn, [{"number": "1", "extra": slots}], now=LATER)
        assert planned.untouched_keys == ["1"]
        assert fetch_all(rw_conn)["1"]["updated_at"] is None

        # A changed list is REPLACED as a whole (no per-element merge).
        changed = [{"num": 1, "key": "Konto", "value": "27400"}]
        planned, _, updated = run_cycle(
            rw_conn, [{"number": "1", "extra": changed}], now=LATER
        )
        assert updated == ["1"]
        assert planned.updates == [{"extra": changed, "number": "1"}]
        assert fetch_all(rw_conn)["1"]["extra"] == changed

    def test_dates_inside_lists_serialize_and_stay_idempotent(self, rw_conn):
        aware = datetime.datetime(2027, 8, 1, 6, 0, 0, tzinfo=datetime.UTC)
        values = [{"number": "1", "extra": [datetime.date(2027, 8, 1), aware]}]
        run_cycle(rw_conn, values)
        assert fetch_all(rw_conn)["1"]["extra"] == [
            "2027-08-01",
            "2027-08-01T08:00:00.000+02:00",  # Zurich, like inside dicts
        ]
        planned, _, _ = run_cycle(rw_conn, values, now=LATER)
        assert planned.untouched_keys == ["1"]

    def test_delete_marker_inside_list_raises(self, rw_conn):
        with pytest.raises(ValueError, match="TOP-LEVEL"):
            SingleTableUpsertPlanBuilder(
                SCRATCH_TABLE,
                "number",
                [{"number": "1", "extra": [SpecialValue.DELETE]}],
            )


class Test_SingleTableUpsertPlan_read_only_columns:
    def test_read_only_column_is_exposed_but_not_diffed(self, rw_conn):
        # Seed a row, then set `amount` behind the plan's back (as an
        # INSERT-only / app-maintained column would be).
        run_cycle(rw_conn, [{"number": "1", "name": "N"}])
        rw_conn.execute(
            psycopg.sql.SQL("UPDATE {} SET amount = 42 WHERE number = '1'").format(
                psycopg.sql.Identifier(SCRATCH_TABLE)
            )
        )
        builder = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1", "name": "N"}]
        )
        builder.load_existing(rw_conn, read_only_columns=["amount"])
        # Exposed via `existing` for the caller...
        assert builder.existing["1"]["amount"] == 42
        # ...but never diffed: the row is untouched and `amount` survives.
        planned = builder.plan()
        assert planned.untouched_keys == ["1"]
        assert planned.apply(rw_conn, now=NOW) == ([], [])
        assert fetch_all(rw_conn)["1"]["amount"] == 42

    def test_read_only_column_already_auto_loaded_is_deduped(self, rw_conn):
        # Naming a value-set column (or the key) as read-only is harmless.
        run_cycle(rw_conn, [{"number": "1", "name": "N", "amount": 7}])
        builder = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1", "name": "N", "amount": 7}]
        )
        builder.load_existing(rw_conn, read_only_columns=["name", "amount", "number"])
        assert builder.existing["1"]["name"] == "N"
        assert builder.existing["1"]["amount"] == 7
        planned = builder.plan()
        assert planned.untouched_keys == ["1"]


class Test_SingleTableUpsertPlan_replace_dict_columns:
    """plan(replace_dict_columns=...): the incoming dict is the FULL target
    state (snapshot semantics) -- stored-only keys are deleted."""

    def run_replace_cycle(self, conn, values, *, now=NOW):
        builder = SingleTableUpsertPlanBuilder(SCRATCH_TABLE, "number", values)
        builder.load_existing(conn)
        planned = builder.plan(replace_dict_columns=["extra"])
        inserted, updated = planned.apply(conn, now=now)
        return planned, inserted, updated

    def test_stored_only_key_is_deleted_with_minimal_delta(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "extra": {"a": 1, "b": 2}}])
        planned, _, updated = self.run_replace_cycle(
            rw_conn, [{"number": "1", "extra": {"a": 1, "c": 3}}], now=LATER
        )
        assert updated == ["1"]
        # Minimal delta: the equal key "a" is not rewritten; "b" is deleted.
        assert planned.updates == [
            {"extra": {"c": 3, "b": SpecialValue.DELETE}, "number": "1"}
        ]
        assert fetch_all(rw_conn)["1"]["extra"] == {"a": 1, "c": 3}

    def test_equal_snapshot_is_untouched(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "extra": {"a": 1}}])
        planned, _, _ = self.run_replace_cycle(
            rw_conn, [{"number": "1", "extra": {"a": 1}}], now=LATER
        )
        assert planned.untouched_keys == ["1"]

    def test_default_merge_still_keeps_stored_only_keys(self, rw_conn):
        run_cycle(rw_conn, [{"number": "1", "extra": {"a": 1, "b": 2}}])
        planned, _, _ = run_cycle(
            rw_conn, [{"number": "1", "extra": {"a": 1}}], now=LATER
        )
        # Without replace semantics the same input is a no-op: "b" survives.
        assert planned.untouched_keys == ["1"]
        assert fetch_all(rw_conn)["1"]["extra"] == {"a": 1, "b": 2}

    def test_unknown_replace_column_raises(self, rw_conn):
        builder = SingleTableUpsertPlanBuilder(
            SCRATCH_TABLE, "number", [{"number": "1", "name": "x"}]
        )
        builder.load_existing(rw_conn)
        with pytest.raises(ValueError, match="replace_dict_columns.*unknown"):
            builder.plan(replace_dict_columns=["nope"])
