"""Tests for the parts of ``wsjrdp2027._internal.single_table_upsert_plan``
that need no database.

The plan/apply behaviour itself is exercised against a real PostgreSQL in
``integration-tests/wsjrdp_scripts_integrations_tests/test_single_table_upsert_plan.py``;
what is unit-testable is the validation the constructor does before anything
is opened.
"""

from __future__ import annotations

import pytest
from wsjrdp2027._internal.single_table_upsert_plan import SingleTableUpsertPlanBuilder


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
