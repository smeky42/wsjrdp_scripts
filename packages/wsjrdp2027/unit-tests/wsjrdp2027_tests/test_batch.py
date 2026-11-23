import pathlib as _pathlib

import pandas as pd
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


class Test_use_sepa_mailing:
    def _headers(self, sepa_mail="s@x.de", **kwargs):
        row = {"id": 7, "role_id_name": "YP 7", "status": "confirmed"}
        for prefix, to in [("mailing", "p@x.de"), ("sepa_mailing", sepa_mail)]:
            row[f"{prefix}_from"] = "from@x.de"
            row[f"{prefix}_to"] = [to] if to else None
            row[f"{prefix}_cc"] = [f"{prefix}-cc@x.de"]
            row[f"{prefix}_bcc"] = None
            row[f"{prefix}_reply_to"] = [f"{prefix}-reply@x.de"]
        bc = BatchConfig(content="x", email_subject="s", **kwargs)
        batch = bc.prepare(pd.DataFrame([row]))
        msg = batch.messages[0].message
        return {k: msg[k] for k in ("To", "Cc", "Reply-To")}

    def test_off(self):
        assert self._headers() == {
            "To": "p@x.de",
            "Cc": "mailing-cc@x.de",
            "Reply-To": "mailing-reply@x.de",
        }

    def test_on(self):
        assert self._headers(query=PeopleQuery(use_sepa_mailing=True)) == {
            "To": "s@x.de",
            "Cc": "sepa_mailing-cc@x.de",
            "Reply-To": "sepa_mailing-reply@x.de",
        }

    def test_batch_values_win(self):
        headers = self._headers(
            query=PeopleQuery(use_sepa_mailing=True),
            email_reply_to="batch@x.de",
            extra_email_cc="extra@x.de",
        )
        assert headers["Reply-To"] == "batch@x.de"
        assert headers["Cc"] == "sepa_mailing-cc@x.de, extra@x.de"

    def test_no_sepa_mail(self):
        with pytest.raises(ValueError, match=r"no SEPA mail address for \[7\]"):
            self._headers(sepa_mail=None, query=PeopleQuery(use_sepa_mailing=True))

    def test_both_options(self):
        with pytest.raises(ValueError, match="Only one of"):
            PeopleQuery(use_sepa_mailing=True, include_sepa_mail_in_mailing_to=True)

    def test_replace_and_to_dict_keep_it(self):
        query = PeopleQuery(use_sepa_mailing=True).replace(limit=1)
        assert query.use_sepa_mailing is True
        assert query.__to_dict__()["use_sepa_mailing"] is True
