from __future__ import annotations

import pathlib as _pathlib

import pytest
import pytest_wsjrdp2027
import wsjrdp2027


_SELFDIR = _pathlib.Path(__file__).parent.resolve()


@pytest.fixture
def mailing_from_yml(ctx: wsjrdp2027.WsjRdpContext):
    def run(*args):
        pytest_wsjrdp2027.uv_run(["./tools/mailing_from_yml.py", *args], ctx=ctx)

    return run


def test_query_html_content_file():
    config = wsjrdp2027.BatchConfig.from_yaml(_SELFDIR / "html_content_file.yml")


class Test_Html_Mail:
    def test__html_content_inline(self, mailing_from_yml):
        mailing_from_yml("--no-zip-eml", _SELFDIR / "html_content__inline.yml")

    def test__html_content_file(self, mailing_from_yml):
        mailing_from_yml("--no-zip-eml", _SELFDIR / "html_content_file.yml")

    def test__html_content_is_path(self, mailing_from_yml):
        mailing_from_yml("--no-zip-eml", _SELFDIR / "html_content_is_filename.yml")
