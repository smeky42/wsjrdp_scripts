import pytest
from wsjrdp2027._people import update_dataframe_for_updates


def _create_df():
    import pandas

    df = pandas.DataFrame()

    people = {
        1: {"status": "registered", "tag_list": []},
        2: {"status": "upload", "tag_list": ["bar"]},
        3: {"status": "reviewed", "tag_list": ["baz"]},
        4: {"status": "confirmed", "tag_list": ["bar", "foo"]},
    }

    ids = sorted(people.keys())
    df["id"] = ids
    for key in ["status", "tag_list"]:
        df[key] = [people[id][key] for id in ids]
    df["person_dict"] = df.apply(lambda row: people[row["id"]], axis=1)
    return df


@pytest.fixture
def df():
    return _create_df()


class Test_Update_DataFrame_For_Updates:
    def test_new_status(self, df):
        updates = {"new_status": "confirmed"}

        update_dataframe_for_updates(df, updates=updates)
        assert list(df["new_status"]) == ["confirmed"] * len(df)
        assert list(df["db_changes"]) == [True, True, True, False]
        assert list(df["person_changes"]) == [
            {"status": ["registered", "confirmed"]},
            {"status": ["upload", "confirmed"]},
            {"status": ["reviewed", "confirmed"]},
            {},
        ]

    def test_add_note(self, df):
        updates = {"add_note": "Note for {{ row.id }}"}

        update_dataframe_for_updates(df, updates=updates)
        assert list(df["add_note"]) == [f"Note for {i}" for i in range(1, len(df) + 1)]
        assert list(df["db_changes"]) == [True] * len(df)
        assert list(df["person_changes"]) == [{}] * len(df)

    @pytest.mark.parametrize("note", [None, ""])
    def test_add_empty_note(self, df, note):
        updates = {"add_note": note}

        update_dataframe_for_updates(df, updates=updates)
        assert "add_note" in df.columns
        assert list(df["add_note"]) == [""] * len(df)
        assert list(df["db_changes"]) == [False] * len(df)
        assert list(df["person_changes"]) == [{}] * len(df)

    def test_add_tags(self, df):
        updates = {"add_tags": "bar"}

        update_dataframe_for_updates(df, updates=updates)
        assert list(df["add_tags"]) == [["bar"]] * len(df)
        assert list(df["db_changes"]) == [True, False, True, False]
        assert list(df["person_changes"]) == [
            {"tag_list": [[], ["bar"]]},
            {},
            {"tag_list": [["baz"], ["bar", "baz"]]},
            {},
        ]

    def test_add_tags_remove_tags__overlapping(self, df):
        updates = {"add_tags": ["bar", "baz"], "remove_tags": ["baz"]}

        update_dataframe_for_updates(df, updates=updates)
        assert list(df["add_tags"]) == [["bar"]] * len(df)
        assert list(df["remove_tags"]) == [["baz"]] * len(df)
        assert list(df["db_changes"]) == [True, False, True, False]
        assert list(df["person_changes"]) == [
            {"tag_list": [[], ["bar"]]},
            {},
            {"tag_list": [["baz"], ["bar"]]},
            {},
        ]

    def test_add_tags_remove_tags__non_overlapping(self, df):
        updates = {"add_tags": ["bar", "baz"], "remove_tags": ["foo"]}

        update_dataframe_for_updates(df, updates=updates)
        assert list(df["add_tags"]) == [["bar", "baz"]] * len(df)
        assert list(df["remove_tags"]) == [["foo"]] * len(df)
        assert list(df["db_changes"]) == [True, True, True, True]
        assert list(df["person_changes"]) == [
            {"tag_list": [[], ["bar", "baz"]]},
            {"tag_list": [["bar"], ["bar", "baz"]]},
            {"tag_list": [["baz"], ["bar", "baz"]]},
            {"tag_list": [["bar", "foo"], ["bar", "baz"]]},
        ]
