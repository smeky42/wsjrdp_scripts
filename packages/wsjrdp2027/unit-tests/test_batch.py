import pathlib as _pathlib

import pytest
from wsjrdp2027 import PeopleQuery, PeopleWhere
from wsjrdp2027._batch import BatchConfig, _strip_html_tags_builtin


_SELFDIR = _pathlib.Path(__file__).parent.resolve()


@pytest.mark.parametrize(
    "html,expected",
    [
        ("<style></style><html><body>Foo</body></html>", "Foo"),
    ],
)
def test___strip_html_tags_builtin(html: str, expected: str):
    text = _strip_html_tags_builtin(html)
    assert text == expected


@pytest.mark.parametrize(
    "html,expected",
    [
        ("<style></style><html><body>Foo</body></html>", "Foo"),
    ],
)
def test___strip_html_tags_html2text(html: str, expected: str):
    text = _strip_html_tags_builtin(html)
    assert text == expected


class Test_BatchConfig_from_dict:
    def test_empty(self):
        bc = BatchConfig.from_dict()
        assert bc.base_dir is None
        assert bc.name == "batch"

    def test_with_path(self):
        bc = BatchConfig.from_dict(path=_SELFDIR / "foo.yml")
        assert bc.base_dir is None
        assert bc._effective_base_dir == _SELFDIR
        assert bc.name == "foo"

    def test_query__override_with_query(self):
        config_query = PeopleQuery(where=PeopleWhere(id=2))
        new_query = PeopleQuery(where=PeopleWhere(id=3))
        config = {"query": config_query}
        bc = BatchConfig.from_dict(config, query=new_query)
        assert bc.query == new_query
        assert bc.query.get_where_condition() == "people.id = 3"

    def test_query__override_with_where(self):
        config_query = PeopleQuery(where=PeopleWhere(id=2))
        new_where = PeopleWhere(id=3)
        config = {"query": config_query}
        bc = BatchConfig.from_dict(config, where=new_where)
        assert bc.query.where == new_where
        assert bc.query.get_where_condition() == "people.id = 3"


class Test_BatchConfig___init__:
    def test_empty(self):
        bc = BatchConfig()
        assert bc.base_dir is None
        assert bc.name == "batch"

    def test_where_to_fill_query(self):
        WHERE = "x IS NULL"
        bc = BatchConfig(where=WHERE)
        assert bc.query.get_where_condition() == WHERE
        assert bc.query.where
        assert bc.query.where.raw_sql == WHERE
