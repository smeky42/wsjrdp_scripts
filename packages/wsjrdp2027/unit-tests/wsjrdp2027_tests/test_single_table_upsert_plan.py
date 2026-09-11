"""Tests for the parts of ``wsjrdp2027._internal.single_table_upsert_plan``
that need no database.

The plan/apply behaviour itself is exercised against a real PostgreSQL in
``integration-tests/wsjrdp_scripts_integrations_tests/test_single_table_upsert_plan.py``;
what is unit-testable is the validation that runs before anything is opened --
the constructor's, and the one ``plan()`` does on its options.
"""

from __future__ import annotations

import pytest
from wsjrdp2027._internal.single_table_upsert_plan import (
    SingleTableUpsertPlanBuilder,
    _is_blank,
)
from wsjrdp2027._pg import ArrayElementType, PgArray


TABLE = "a_table"


class Test_SingleTableUpsertPlanBuilder_generated_key_columns:
    """``generated_key_columns`` names key columns the DATABASE derives; the
    declaration is checked at construction, before any connection exists."""

    def test_default_is_empty(self):
        builder = SingleTableUpsertPlanBuilder(TABLE, "number", [])
        assert builder._generated_key_columns == ()

    def test_single_key_column_is_stored_as_tuple(self):
        builder = SingleTableUpsertPlanBuilder(
            TABLE, "object_key", [], generated_key_columns=["object_key"]
        )
        assert builder._generated_key_columns == ("object_key",)

    def test_composite_key_columns_keep_the_given_order(self):
        builder = SingleTableUpsertPlanBuilder(
            TABLE,
            ("object_key", "number", "name"),
            [],
            generated_key_columns=("name", "object_key"),
        )
        assert builder._generated_key_columns == ("name", "object_key")

    def test_non_key_column_raises(self):
        with pytest.raises(ValueError, match=r"'name'.*not a key column"):
            SingleTableUpsertPlanBuilder(
                TABLE, "number", [], generated_key_columns=["name"]
            )
        # Same for a composite key: only its own columns qualify.
        with pytest.raises(ValueError, match=r"'name'.*not a key column"):
            SingleTableUpsertPlanBuilder(
                TABLE, ("number", "object_key"), [], generated_key_columns=["name"]
            )

    def test_duplicate_column_raises(self):
        with pytest.raises(ValueError, match="duplicate"):
            SingleTableUpsertPlanBuilder(
                TABLE,
                "object_key",
                [],
                generated_key_columns=["object_key", "object_key"],
            )


class Test_SingleTableUpsertPlanBuilder_keep_stored_when_blank:
    """``plan(keep_stored_when_blank=...)`` names columns whose BLANK incoming
    value means "not carried by this source". The declaration is checked by
    ``_keep_blank_columns()``, which :meth:`plan` calls right after its
    ``load_existing()`` guard and which needs no loaded state -- so it is
    exercised here directly; the effect on a plan is an integration test."""

    def builder(self):
        return SingleTableUpsertPlanBuilder(
            TABLE,
            "number",
            [
                {
                    "number": "1",
                    "short_name": "S",
                    "extra": {"a": 1},
                    "tags": ["a"],
                    "uuids": PgArray(
                        ["11111111-1111-4111-8111-111111111111"],
                        ArrayElementType.UUID,
                    ),
                }
            ],
        )

    def test_empty_default_and_value_set_columns_are_accepted(self):
        builder = self.builder()
        assert builder._keep_blank_columns(()) == frozenset()
        assert builder._keep_blank_columns(["short_name"]) == frozenset({"short_name"})

    def test_key_column_raises(self):
        with pytest.raises(ValueError, match=r"key column.*'number'"):
            self.builder()._keep_blank_columns(["number"])

    def test_unknown_column_raises(self):
        with pytest.raises(ValueError, match=r"unknown column\(s\): \['nope'\]"):
            self.builder()._keep_blank_columns(["nope"])

    def test_dict_list_and_array_columns_raise(self):
        for column, type_name in (
            ("extra", "dict"),
            ("tags", "list"),
            ("uuids", "PgArray"),
        ):
            with pytest.raises(ValueError, match=f"{type_name} value"):
                self.builder()._keep_blank_columns([column])

    def test_blank_is_none_or_a_whitespace_only_string(self):
        assert [_is_blank(v) for v in (None, "", "   ", "\t\n")] == [True] * 4
        # A value is a value -- also a falsy one.
        assert [_is_blank(v) for v in ("x", " x ", 0, False, [], {})] == [False] * 6
