import pytest
from wsjrdp2027 import PeopleWhere


def _yaml_str_to_dict(yaml_str: str) -> dict:
    import io

    import yaml

    f = io.StringIO(yaml_str)

    return yaml.load(f, Loader=yaml.FullLoader)


class Test_PeopleWhere:
    def test_empty_where(self):
        where = PeopleWhere()
        assert where.status is None
        assert where.exclude_status is None


class Test_PeopleWhere_Yaml_To_Dict:
    @pytest.mark.parametrize(
        "yaml_str, expected_dict",
        [
            ("""status:""", {}),
            ("""status: false""", {"status": False}),
            ("""status: [false, 'registered']""", {"status": [False, "registered"]}),
        ],
    )
    def test_from_yaml(self, yaml_str, expected_dict):
        yaml_dict = _yaml_str_to_dict(yaml_str)
        where = PeopleWhere.from_dict(yaml_dict)
        where_dict = where.to_dict()
        assert where_dict == expected_dict


class Test_PeopleWhere__as_where_condition:
    def test_status(self):
        where_str = PeopleWhere(status="confirmed").as_where_condition()
        assert where_str == "people.status = 'confirmed'"

    def test_exclude_status(self):
        where_str = PeopleWhere(exclude_status="registered").as_where_condition()
        assert where_str == "people.status <> 'registered'"

    def test_sepa_status(self):
        where_str = PeopleWhere(sepa_status="ok").as_where_condition()
        assert where_str == "COALESCE(people.sepa_status, 'ok') = 'ok'"

    def test_exclude_sepa_status(self):
        where_str = PeopleWhere(exclude_sepa_status="ok").as_where_condition()
        assert where_str == "COALESCE(people.sepa_status, 'ok') <> 'ok'"

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

    def test_unit_code_is_null(self):
        where_str = PeopleWhere(unit_code=False).as_where_condition()
        assert where_str == "people.unit_code IS NULL"

    def test_unit_code_is_null_or_000000(self):
        where_str = PeopleWhere(unit_code=[False, "000000"]).as_where_condition()
        assert where_str == "(people.unit_code = '000000' OR people.unit_code IS NULL)"

    def test_exclude_unit_code(self):
        where_str = PeopleWhere(exclude_unit_code="000000").as_where_condition()
        assert where_str == "people.unit_code <> '000000'"

    @pytest.mark.parametrize(
        "note,expected",
        [
            (
                "foo",
                "'foo' = ANY(people.note_list)",
            ),
            (
                ["foo", "bar"],
                "('foo' = ANY(people.note_list) AND 'bar' = ANY(people.note_list))",
            ),
            (
                {"expr": "foo", "op": "like"},
                "EXISTS(WITH t AS (SELECT UNNEST(people.note_list) AS r) SELECT FROM t WHERE r LIKE 'foo')",
            ),
        ],
    )
    def test_note(self, note, expected):
        where_str = PeopleWhere(note=note).as_where_condition()
        assert where_str == expected

    @pytest.mark.parametrize(
        "exclude_note,expected",
        [
            (
                "foo",
                "'foo' <> ALL(people.note_list)",
            ),
            (
                ["foo", "bar"],
                "('foo' <> ALL(people.note_list) AND 'bar' <> ALL(people.note_list))",
            ),
            (
                {"expr": "foo", "op": "like"},
                "NOT EXISTS(WITH t AS (SELECT UNNEST(people.note_list) AS r) SELECT FROM t WHERE r LIKE 'foo')",
            ),
        ],
    )
    def test_exclude_note(self, exclude_note, expected):
        where = PeopleWhere(exclude_note=exclude_note)
        where_str = where.as_where_condition()
        assert where_str == expected

    @pytest.mark.parametrize(
        "arg,expected",
        [
            ("Region Nord", "'Region Nord' = ANY(people.tag_list)"),
            (["Region Nord"], "'Region Nord' = ANY(people.tag_list)"),
            (
                ["a", "b"],
                "('a' = ANY(people.tag_list) AND 'b' = ANY(people.tag_list))",
            ),
        ],
    )
    def test_tag(self, arg, expected):
        where_str = PeopleWhere(tag=arg).as_where_condition()
        assert where_str == expected

    @pytest.mark.parametrize(
        "arg,expected",
        [
            ("Region Nord", "'Region Nord' <> ALL(people.tag_list)"),
            (["Region Nord"], "'Region Nord' <> ALL(people.tag_list)"),
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

    @staticmethod
    def __extract_op(obj, default: str) -> str:
        if isinstance(obj, dict):
            return obj.get("op", default)
        else:
            return default

    @pytest.mark.parametrize(
        "note,expected",
        [
            ("foo", "foo"),
            (["foo"], "foo"),
            ((x for x in ["foo"]), "foo"),
            ({"expr": "foo"}, "foo"),
            ({"expr": "foo", "func": "ANY", "op": "="}, "foo"),
            (
                {"expr": "foo", "func": "ANY", "op": "LIKE"},
                {"expr": "foo", "op": "LIKE"},
            ),
        ],
    )
    def test_note(self, note, expected):
        where = PeopleWhere(note=note)
        where_dict = where.to_dict()
        expected_where_dict = {"note": expected}
        assert where_dict == expected_where_dict
        assert where.note
        assert where.note.op == self.__extract_op(expected, "=")

    @pytest.mark.parametrize(
        "exclude_note,expected",
        [
            ("foo", "foo"),
            (["foo"], "foo"),
            ((x for x in ["foo"]), "foo"),
            ({"expr": "foo"}, "foo"),
            ({"expr": "foo", "func": "ANY", "op": "="}, "foo"),
            (
                {"expr": "foo", "func": "ANY", "op": "LIKE"},
                {"expr": "foo", "op": "NOT LIKE"},
            ),
        ],
    )
    def test_exclude_note(self, exclude_note, expected):
        where = PeopleWhere(exclude_note=exclude_note)
        where_dict = where.to_dict()
        expected_where_dict = {"exclude_note": expected}
        assert where_dict == expected_where_dict
        assert where.exclude_note
        assert where.exclude_note.op == self.__extract_op(expected, "<>")
