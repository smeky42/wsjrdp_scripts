import pytest
from wsjrdp2027 import PeopleWhere


class Test_PeopleWhere__as_where_condition:
    def test_status(self):
        where_str = PeopleWhere(status="confirmed").as_where_condition()
        assert where_str == "people.status = 'confirmed'"

    def test_exclude_status(self):
        where_str = PeopleWhere(exclude_status="registered").as_where_condition()
        assert where_str == "people.status <> 'registered'"

    def test_sepa_status(self):
        where_str = PeopleWhere(sepa_status="ok").as_where_condition()
        assert where_str == "people.sepa_status = 'ok'"

    def test_exclude_sepa_status(self):
        where_str = PeopleWhere(exclude_sepa_status="ok").as_where_condition()
        assert where_str == "people.sepa_status <> 'ok'"

    def test_id(self):
        where_str = PeopleWhere(id=123).as_where_condition()
        assert where_str == "people.id = 123"

    def test_exclude_id(self):
        where_str = PeopleWhere(exclude_id=123).as_where_condition()
        assert where_str == "people.id <> 123"

    def test_primary_group_id(self):
        where_str = PeopleWhere(primary_group_id=123).as_where_condition()
        assert where_str == "people.primary_group_id = 123"

    def test_exclude_primary_group_id(self):
        where_str = PeopleWhere(exclude_primary_group_id=123).as_where_condition()
        assert where_str == "people.primary_group_id <> 123"

    def test_unit_code(self):
        where_str = PeopleWhere(unit_code="000000").as_where_condition()
        assert where_str == "people.unit_code = '000000'"

    def test_exclude_unit_code(self):
        where_str = PeopleWhere(exclude_unit_code="000000").as_where_condition()
        assert where_str == "people.unit_code <> '000000'"

    @pytest.mark.parametrize(
        "arg,expected",
        [
            ("Region Nord", "('Region Nord' == ANY(people.tag_list))"),
            (["Region Nord"], "('Region Nord' == ANY(people.tag_list))"),
            (
                ["a", "b"],
                "('a' == ANY(people.tag_list) AND 'b' == ANY(people.tag_list))",
            ),
        ],
    )
    def test_tag(self, arg, expected):
        where_str = PeopleWhere(tag=arg).as_where_condition()
        assert where_str == expected

    @pytest.mark.parametrize(
        "arg,expected",
        [
            ("Region Nord", "('Region Nord' <> ALL(people.tag_list))"),
            (["Region Nord"], "('Region Nord' <> ALL(people.tag_list))"),
            (
                ["a", "b"],
                "('a' <> ALL(people.tag_list) AND 'b' <> ALL(people.tag_list))",
            ),
        ],
    )
    def test_exclude_tag(self, arg, expected):
        where_str = PeopleWhere(exclude_tag=arg).as_where_condition()
        assert where_str == expected


class Test_PeopleWhere__to_dict:
    def test_status(self):
        where_dict = PeopleWhere(status="confirmed").to_dict()
        assert where_dict == {"status": "confirmed"}

    def test_exclude_status(self):
        where_dict = PeopleWhere(exclude_status="registered").to_dict()
        assert where_dict == {"exclude_status": "registered"}

    def test_sepa_status(self):
        where_dict = PeopleWhere(sepa_status="ok").to_dict()
        assert where_dict == {"sepa_status": "ok"}

    def test_exclude_sepa_status(self):
        where_dict = PeopleWhere(exclude_sepa_status="ok").to_dict()
        assert where_dict == {"exclude_sepa_status": "ok"}

    def test_id(self):
        where_dict = PeopleWhere(id=123).to_dict()
        assert where_dict == {"id": 123}

    def test_exclude_id(self):
        where_dict = PeopleWhere(exclude_id=123).to_dict()
        assert where_dict == {"exclude_id": 123}

    def test_primary_group_id(self):
        where_dict = PeopleWhere(primary_group_id=123).to_dict()
        assert where_dict == {"primary_group_id": 123}

    def test_exclude_primary_group_id(self):
        where_dict = PeopleWhere(exclude_primary_group_id=123).to_dict()
        assert where_dict == {"exclude_primary_group_id": 123}

    def test_unit_code(self):
        where_dict = PeopleWhere(unit_code="000000").to_dict()
        assert where_dict == {"unit_code": "000000"}

    def test_exclude_unit_code(self):
        where_dict = PeopleWhere(exclude_unit_code="000000").to_dict()
        assert where_dict == {"exclude_unit_code": "000000"}

    def test_tag(self):
        where_dict = PeopleWhere(tag="Region Nord").to_dict()
        assert where_dict == {"tag": "Region Nord"}

    def test_exclude_tag(self):
        where_dict = PeopleWhere(exclude_tag="Region Nord").to_dict()
        assert where_dict == {"exclude_tag": "Region Nord"}
