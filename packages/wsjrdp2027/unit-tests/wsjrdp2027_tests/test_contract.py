"""The participation contract as the scripts keep it: the rules
(_contract.contract_changes) and how a batch update applies them
(update_dataframe_for_updates)."""

import datetime

import pandas
import pytest
from wsjrdp2027 import _contract, _person_pg
from wsjrdp2027._people import update_dataframe_for_updates


NOW = datetime.datetime(2026, 10, 10, 12, 0, tzinfo=datetime.UTC)
# The stored timestamps are without a time zone, as Rails writes them.
CONFIRMED_AT = datetime.datetime.fromisoformat("2025-12-16T20:00:00")
ENDED_AT = datetime.datetime.fromisoformat("2026-03-01T10:00:00")

NO_CONTRACT = {
    "contract_status": "none",
    "contract_confirmed_at": None,
    "contract_ended_at": None,
    "contract_confirmed_by_type": None,
    "contract_confirmed_by_id": None,
}
IN_FORCE = {
    "contract_status": "confirmed",
    "contract_confirmed_at": CONFIRMED_AT,
    "contract_ended_at": None,
    "contract_confirmed_by_type": "Person",
    "contract_confirmed_by_id": 65,
}
ENDED = {**IN_FORCE, "contract_status": "ended", "contract_ended_at": ENDED_AT}


def changes(old, status, new_status, *, role_change=False):
    return _contract.contract_changes(
        {"status": status, **old},
        new_status=new_status,
        role_change=role_change,
        now=NOW,
    )


class Test_Contract_Changes_By_Status:
    def test_the_first_confirmation_starts_a_contract(self):
        assert changes(NO_CONTRACT, "reviewed", "confirmed") == {
            "contract_status": ["none", "confirmed"],
            "contract_confirmed_at": [None, NOW],
            "contract_confirmed_by_type": [None, "Person"],
            "contract_confirmed_by_id": [None, 1],
        }

    def test_a_confirmation_with_a_contract_in_force_keeps_it(self):
        # Back to upload for a document and confirmed again: the first date stays.
        assert changes(IN_FORCE, "upload", "confirmed") == {}

    def test_a_confirmation_after_the_end_starts_a_new_contract(self):
        assert changes(ENDED, "reviewed", "confirmed") == {
            "contract_status": ["ended", "confirmed"],
            "contract_confirmed_at": [CONFIRMED_AT, NOW],
            "contract_ended_at": [ENDED_AT, None],
            "contract_confirmed_by_id": [65, 1],
        }

    def test_a_deregistration_ends_the_contract_in_force(self):
        assert changes(IN_FORCE, "deregistration_noted", "deregistered") == {
            "contract_status": ["confirmed", "ended"],
            "contract_ended_at": [None, NOW],
        }

    @pytest.mark.parametrize("old", [NO_CONTRACT, ENDED])
    def test_a_deregistration_without_a_contract_in_force_changes_nothing(self, old):
        assert changes(old, "registered", "deregistered") == {}

    @pytest.mark.parametrize(
        "status, new_status",
        [
            ("confirmed", "upload"),
            ("confirmed", "deregistration_noted"),
            ("confirmed", "registered"),
            ("confirmed", None),
            ("confirmed", "confirmed"),
        ],
    )
    def test_every_other_change_leaves_the_contract(self, status, new_status):
        assert changes(IN_FORCE, status, new_status) == {}


class Test_Contract_Changes_By_Role:
    def test_a_role_with_another_fee_ends_the_contract_in_force(self):
        # UL to IST: back to registered, the new role needs a new contract.
        assert changes(IN_FORCE, "confirmed", "registered", role_change=True) == {
            "contract_status": ["confirmed", "ended"],
            "contract_ended_at": [None, NOW],
        }

    def test_the_new_contract_starts_with_the_next_confirmation(self):
        ended = {**IN_FORCE, "contract_status": "ended", "contract_ended_at": NOW}
        assert changes(ended, "reviewed", "confirmed")["contract_status"] == [
            "ended",
            "confirmed",
        ]

    @pytest.mark.parametrize("new_status", [None, "confirmed"])
    def test_a_role_change_that_stays_confirmed_is_refused(self, new_status):
        with pytest.raises(_contract.ContractError, match="new confirmation"):
            changes(IN_FORCE, "confirmed", new_status, role_change=True)

    @pytest.mark.parametrize("old", [NO_CONTRACT, ENDED])
    def test_a_role_change_without_a_contract_in_force_changes_nothing(self, old):
        assert changes(old, "printed", "registered", role_change=True) == {}

    @pytest.mark.parametrize(
        "old_role_types, new_role_types, needs_new_contract",
        [
            (["Group::Unit::Leader"], "Group::Ist::Member", True),
            (["Group::Unit::Member"], "Group::Unit::Leader", True),
            (["Group::Ist::Member"], "Group::Root::Member", True),
            # IST to BMT and back: the same role type in another group.
            (["Group::Ist::Member"], ["Group::Ist::Member"], False),
            (["Group::Unit::UnapprovedLeader"], "Group::Unit::Leader", False),
            (["Group::Ist::Leader"], "Group::Ist::Member", False),
            (
                ["Group::Root::Finance", "Group::Root::Member"],
                "Group::Root::Leader",
                False,
            ),
            # No fee on one side: nothing to compare.
            ([], "Group::Ist::Member", False),
            (["Group::Root::Admin"], "Group::Unit::Member", False),
        ],
    )
    def test_which_role_changes_need_a_new_contract(
        self, old_role_types, new_role_types, needs_new_contract
    ):
        assert (
            _contract.role_change_needs_new_contract(old_role_types, new_role_types)
            is needs_new_contract
        )


def _df(people: dict[int, dict]) -> pandas.DataFrame:
    """A data frame as load_people_dataframe makes it, with the stored values
    of each person (person_dict) that the update compares with."""
    rows = []
    for id, person in people.items():
        primary_group_id = person.get("primary_group_id", 2)
        rows.append(
            {
                "id": id,
                "status": person["status"],
                "primary_group_id": primary_group_id,
                "primary_group_role_types": person.get(
                    "role_types", ["Group::Unit::Leader"]
                ),
                "tag_list": [],
                "person_dict": {
                    "status": person["status"],
                    "primary_group_id": primary_group_id,
                    **person["contract"],
                },
            }
        )
    return pandas.DataFrame(rows)


class Test_Update_DataFrame_Derives_The_Contract:
    def test_confirmation_and_deregistration_write_the_contract_columns(self):
        df = _df({1: {"status": "reviewed", "contract": NO_CONTRACT}})

        update_dataframe_for_updates(df, updates={"new_status": "confirmed"}, now=NOW)

        row = df.iloc[0]
        assert row["db_changes"]
        assert row["person_changes"]["contract_status"] == ["none", "confirmed"]
        assert row["new_contract_status"] == "confirmed"
        assert row["new_contract_confirmed_at"] == NOW
        assert row["new_contract_confirmed_by_id"] == 1

    def test_the_ul_to_ist_role_change_ends_the_contract(self):
        # registration_tools/2025-11-25__UL-Rollenwechsel-zu-IST.yml
        df = _df({29: {"status": "confirmed", "contract": IN_FORCE}})
        updates = {
            "new_status": "registered",
            "new_primary_group_id": 4,
            "new_primary_group_role_types": "Group::Ist::Member",
        }

        update_dataframe_for_updates(df, updates=updates, now=NOW)

        person_changes = df.iloc[0]["person_changes"]
        assert person_changes["status"] == ["confirmed", "registered"]
        assert person_changes["contract_status"] == ["confirmed", "ended"]
        assert person_changes["contract_ended_at"] == [None, NOW]

    def test_a_move_to_the_bmt_keeps_the_contract(self):
        df = _df(
            {
                7: {
                    "status": "confirmed",
                    "primary_group_id": 4,
                    "role_types": ["Group::Ist::Member"],
                    "contract": IN_FORCE,
                }
            }
        )
        updates = {
            "new_primary_group_id": 45,
            "new_primary_group_role_types": "Group::Ist::Member",
        }

        update_dataframe_for_updates(df, updates=updates, now=NOW)

        assert not any(
            col.startswith("contract_") for col in df.iloc[0]["person_changes"]
        )

    def test_a_role_change_of_a_confirmed_person_without_new_status_is_refused(self):
        df = _df({29: {"status": "confirmed", "contract": IN_FORCE}})

        with pytest.raises(_contract.ContractError, match="29"):
            update_dataframe_for_updates(
                df,
                updates={"new_primary_group_role_types": "Group::Ist::Member"},
                now=NOW,
            )

    def test_an_update_without_status_or_role_leaves_the_contract_alone(self):
        df = _df({1: {"status": "reviewed", "contract": NO_CONTRACT}})

        update_dataframe_for_updates(df, updates={"add_tags": "x"}, now=NOW)

        assert "new_contract_status" not in df.columns

    def test_the_contract_columns_cannot_be_set_by_an_update(self):
        assert "new_contract_status" not in _person_pg.VALID_PERSON_UPDATE_KEYS
        df = _df({1: {"status": "reviewed", "contract": NO_CONTRACT}})

        with pytest.raises(TypeError, match="new_contract_status"):
            update_dataframe_for_updates(df, updates={"new_contract_status": "ended"})


class Test_New_Primary_Group_Role_Types:
    """A batch YAML gives the role types as a string; the update compares
    and writes them as a list (as _update_roles counts and walks them)."""

    def test_a_string_becomes_a_list(self):
        df = _df({29: {"status": "registered", "contract": NO_CONTRACT}})

        update_dataframe_for_updates(
            df, updates={"new_primary_group_role_types": "Group::Ist::Member"}, now=NOW
        )

        row = df.iloc[0]
        assert row["new_primary_group_role_types"] == ["Group::Ist::Member"]
        assert row["person_changes"]["primary_group_role_types"] == [
            ["Group::Unit::Leader"],
            ["Group::Ist::Member"],
        ]

    def test_the_same_role_types_are_no_change(self):
        df = _df({29: {"status": "confirmed", "contract": IN_FORCE}})

        update_dataframe_for_updates(
            df, updates={"new_primary_group_role_types": "Group::Unit::Leader"}, now=NOW
        )

        assert df.iloc[0]["person_changes"] == {}
        assert not df.iloc[0]["db_changes"]


class Test_Contract_Consistency:
    """The check before a payment run (_contract.find_contract_findings)."""

    @staticmethod
    def person(status, contract, id=1):
        return {"id": id, "status": status, **contract}

    @pytest.mark.parametrize(
        "status, contract",
        [
            ("confirmed", IN_FORCE),
            ("registered", NO_CONTRACT),
            ("deregistered", ENDED),
            ("deregistered", NO_CONTRACT),
            ("reviewed", ENDED),
        ],
    )
    def test_states_the_rules_produce_are_fine(self, status, contract):
        assert _contract.find_contract_findings([self.person(status, contract)]) == (
            [],
            [],
        )

    @pytest.mark.parametrize(
        "status, contract, problem",
        [
            ("confirmed", NO_CONTRACT, "without a contract in force"),
            ("confirmed", ENDED, "without a contract in force"),
            ("deregistered", IN_FORCE, "with a contract in force"),
            (
                "confirmed",
                {**IN_FORCE, "contract_confirmed_at": None},
                "needs contract_confirmed_at",
            ),
            (
                "confirmed",
                {**IN_FORCE, "contract_ended_at": ENDED_AT},
                "no contract_ended_at",
            ),
            (
                "deregistered",
                {**ENDED, "contract_ended_at": None},
                "needs contract_confirmed_at and contract_ended_at",
            ),
            (
                "registered",
                {**NO_CONTRACT, "contract_confirmed_by_id": 1},
                "confirmed_by are set",
            ),
            (
                "registered",
                {**NO_CONTRACT, "contract_status": "active"},
                "unknown contract_status",
            ),
            (
                "deregistered",
                {
                    **ENDED,
                    "contract_ended_at": datetime.datetime.fromisoformat(
                        "2025-01-01T00:00:00"
                    ),
                },
                "before contract_confirmed_at",
            ),
        ],
    )
    def test_errors(self, status, contract, problem):
        errors, notes = _contract.find_contract_findings(
            [self.person(status, contract, id=7)]
        )

        assert [e.person_id for e in errors] and {e.person_id for e in errors} == {7}
        assert any(problem in e.problem for e in errors), errors

    @pytest.mark.parametrize("status", ["printed", "upload", "deregistration_noted"])
    def test_a_contract_in_force_with_a_stepped_back_status_is_a_note(self, status):
        errors, notes = _contract.find_contract_findings(
            [self.person(status, IN_FORCE)]
        )

        assert errors == []
        assert [n.problem for n in notes] == [
            f"contract in force, status {status!r}: not collected by a selection on status"
        ]

    def test_check_asks_for_approval_only_with_errors(self, monkeypatch):
        class Ctx:
            prompts: list[str] = []

            def require_approval_to_run_in_prod(self, prompt):
                self.prompts.append(prompt)

        people = [
            self.person("confirmed", IN_FORCE, id=1),
            self.person("printed", IN_FORCE, id=2),
        ]
        monkeypatch.setattr(
            _contract,
            "load_contract_findings",
            lambda conn: _contract.find_contract_findings(people),
        )
        ctx = Ctx()
        assert _contract.check_contract_consistency(ctx, conn=None) == []
        assert ctx.prompts == []

        people.append(self.person("confirmed", NO_CONTRACT, id=3))
        errors = _contract.check_contract_consistency(ctx, conn=None)
        assert [e.person_id for e in errors] == [3]
        assert len(ctx.prompts) == 1
