"""Tests of the Hitobito <-> Keycloak sync.

Everything talking to Keycloak, Mailcow or the database is a mock here,
there is deliberately no real ``WsjRdpContext``.
"""

import logging as _logging
import unittest.mock as _mock

import pytest
from wsjrdp2027 import _batch, _keycloak_wsjrdp_adapter, _util
from wsjrdp2027._internal import sync_hitobito_keycloak as _sync
from wsjrdp2027._models.person import Person


USERNAME = "first.last@ist.worldscoutjamboree.de"
MOSS_DEFAULT = "wsj27-7@worldscoutjamboree.de"
MOSS_EXTERNAL = "someone@example.org"


def _person(id=7, **additional_info) -> Person:
    return Person(
        id=id,
        first_name="First",
        nickname=None,
        primary_group_id=4,
        last_name="Last",
        email="private@example.org",
        additional_info={
            "wsjrdp_role": "IST",
            "wsjrdp_email": USERNAME,
            "keycloak_username": USERNAME,
            **additional_info,
        },
    )


def _no_sso_person(**additional_info) -> Person:
    return _person(
        moss_email=MOSS_EXTERNAL, moss_login_without_sso=True, **additional_info
    )


def _keycloak_user(*, id=7, moss_email: str | None = None) -> dict:
    attributes = {"hitobitoId": [str(id)]}
    if moss_email:
        attributes["mossEmail"] = [moss_email]
    return {"username": USERNAME, "attributes": attributes}


def _mock_ctx(keycloak_user: dict | None = None):
    ctx = _mock.MagicMock()
    adapter = ctx.keycloak.return_value
    adapter.get_user_for_person_or_none.return_value = keycloak_user
    adapter.get_user_by_email.return_value = keycloak_user
    adapter.get_user_or_none_by_email.return_value = None
    return ctx, adapter


def _answer(monkeypatch, *answers: str) -> None:
    it = iter(answers)
    monkeypatch.setattr(_util, "clear_stdin", lambda: None)
    monkeypatch.setattr("builtins.input", lambda prompt: next(it))


def _adapter() -> _keycloak_wsjrdp_adapter.WsjRdpKeycloakAdapter:
    adapter = _keycloak_wsjrdp_adapter.WsjRdpKeycloakAdapter.__new__(
        _keycloak_wsjrdp_adapter.WsjRdpKeycloakAdapter
    )
    adapter._client = _mock.MagicMock()
    return adapter


class Test_Person_moss_login_without_sso:
    def test_defaults_to_false(self):
        p = _person(moss_email=MOSS_DEFAULT)
        assert p.moss_login_without_sso is False
        assert p.keycloak_moss_email == MOSS_DEFAULT
        assert p.moss_email_expected_goto() == "private@example.org"

    def test_external_address_needs_the_flag(self):
        p = _person(moss_email=MOSS_EXTERNAL)
        assert p.find_role_consistency_updates() == {
            "moss_email": (MOSS_EXTERNAL, MOSS_DEFAULT)
        }

    def test_flag_keeps_address_out_of_keycloak_and_mailcow(self):
        p = _no_sso_person()
        assert p.moss_login_without_sso is True
        assert p.moss_email == MOSS_EXTERNAL
        assert p.find_role_consistency_updates() == {}
        assert p.keycloak_moss_email is None
        assert p.moss_email_expected_goto() is None

    def test_setter_removes_key_for_false(self):
        p = _no_sso_person()
        p.moss_login_without_sso = False
        assert "moss_login_without_sso" not in p.additional_info


class Test_console_choice:
    CHOICES = {"y": "yes", "n": "no", "i": "ignore"}

    @pytest.mark.parametrize(
        "answers,expected",
        [
            ([""], "i"),
            (["y"], "y"),
            (["NO"], "n"),
            (["ignore"], "i"),
            (["x", "n"], "n"),
        ],
    )
    def test_answers(self, monkeypatch, answers, expected):
        _answer(monkeypatch, *answers)
        assert _util.console_choice("?", choices=self.CHOICES, default="i") == expected

    def test_prompt_marks_default(self, monkeypatch):
        prompts = []
        monkeypatch.setattr(_util, "clear_stdin", lambda: None)
        monkeypatch.setattr("builtins.input", lambda p: prompts.append(p) or "")
        _util.console_choice("Q?", choices=self.CHOICES, default="i")
        assert prompts == ["Q? [y/n/I] "]


class Test_adapter_consistency_check:
    def test_ignore_is_default(self, monkeypatch):
        _answer(monkeypatch, "")
        with pytest.raises(_keycloak_wsjrdp_adapter.PersonInconsistencyIgnored):
            _adapter().get_user_for_person_or_none(_person(moss_email=MOSS_EXTERNAL))

    def test_no_aborts(self, monkeypatch):
        _answer(monkeypatch, "n")
        with pytest.raises(_keycloak_wsjrdp_adapter.PersonInconsistencyAborted):
            _adapter().get_user_for_person_or_none(_person(moss_email=MOSS_EXTERNAL))

    def test_yes_adjusts(self, monkeypatch):
        _answer(monkeypatch, "y")
        p = _person(moss_email=MOSS_EXTERNAL)
        updates = []
        _adapter().get_user_for_person_or_none(p, additional_info_updates=updates)
        assert p.moss_email == MOSS_DEFAULT
        assert updates == [{"id": 7, "moss_email": (MOSS_EXTERNAL, MOSS_DEFAULT)}]

    def test_flag_asks_nothing(self, monkeypatch):
        monkeypatch.setattr("builtins.input", _mock.Mock(side_effect=AssertionError))
        _adapter().get_user_for_person_or_none(_no_sso_person())


class Test_sync:
    def _sync(self, ctx, people) -> bool:
        return _sync.sync(ctx, people, keycloak_groupname="IST")

    def test_no_sso_without_keycloak_attribute(self, caplog):
        ctx, adapter = _mock_ctx(_keycloak_user())
        with caplog.at_level(_logging.WARNING):
            assert self._sync(ctx, [_no_sso_person()])
        adapter.update_users.assert_called_once_with([], console_confirm=True)
        assert not caplog.records

    def test_no_sso_with_keycloak_attribute_only_warns(self, caplog):
        ctx, adapter = _mock_ctx(_keycloak_user(moss_email=MOSS_DEFAULT))
        with caplog.at_level(_logging.WARNING):
            assert self._sync(ctx, [_no_sso_person()])
        adapter.update_users.assert_called_once_with([], console_confirm=True)
        assert [r.levelname for r in caplog.records] == ["WARNING"]
        assert "mossEmail" in caplog.text

    def test_regular_person_still_gets_missing_attribute(self):
        ctx, adapter = _mock_ctx(_keycloak_user())
        assert self._sync(ctx, [_person(moss_email=MOSS_DEFAULT)])
        adapter.update_users.assert_called_once_with(
            [{"username": USERNAME, "attributes": {"mossEmail": [MOSS_DEFAULT]}}],
            console_confirm=True,
        )

    def test_regular_person_mismatch_is_an_error(self):
        ctx, adapter = _mock_ctx(_keycloak_user(moss_email=MOSS_EXTERNAL))
        assert not self._sync(ctx, [_person(moss_email=MOSS_DEFAULT)])
        adapter.update_users.assert_not_called()

    def test_ignored_person_does_not_block_others(self, caplog):
        ctx, adapter = _mock_ctx(_keycloak_user())
        adapter.get_user_for_person_or_none.side_effect = [
            # first loop: only logging
            _keycloak_user(),
            _keycloak_user(),
            # second loop
            _keycloak_wsjrdp_adapter.PersonInconsistencyIgnored(),
            _keycloak_user(),
        ]
        people = [_person(moss_email=MOSS_EXTERNAL), _person(moss_email=MOSS_DEFAULT)]
        with caplog.at_level(_logging.WARNING):
            assert self._sync(ctx, people)
        ctx.update_people_additional_info.assert_called_once()
        adapter.update_users.assert_called_once()
        assert "Skip" in caplog.text

    def test_abort_writes_nothing(self):
        ctx, adapter = _mock_ctx(_keycloak_user())
        adapter.get_user_for_person_or_none.side_effect = [
            _keycloak_user(),
            _keycloak_wsjrdp_adapter.PersonInconsistencyAborted(),
        ]
        assert not self._sync(ctx, [_person(moss_email=MOSS_EXTERNAL)])
        ctx.update_people_additional_info.assert_not_called()
        adapter.update_users.assert_not_called()


class Test_create_keycloak_user:
    def _create(self, monkeypatch, person):
        monkeypatch.setattr(_batch, "BatchConfig", _mock.MagicMock())
        monkeypatch.setattr(Person, "primary_group", _mock.MagicMock(), raising=False)
        ctx, adapter = _mock_ctx()
        _sync._create_keycloak_user(
            ctx,
            person,
            keycloak_adapter=adapter,
            keycloak_groupname="IST",
            keycloak_username=USERNAME,
            keycloak_email=USERNAME,
            errors=[],
            additional_info_updates=[],
        )
        return ctx.mailcow.return_value, adapter

    def test_regular_person(self, monkeypatch):
        mailcow, adapter = self._create(monkeypatch, _person(moss_email=MOSS_DEFAULT))
        assert adapter.create_user.call_args.kwargs["attributes"] == {
            "hitobitoId": ["7"],
            "mossEmail": [MOSS_DEFAULT],
        }
        assert [c.args[0] for c in mailcow.add_alias.call_args_list] == [
            USERNAME,
            MOSS_DEFAULT,
        ]

    def test_no_sso_person(self, monkeypatch):
        mailcow, adapter = self._create(monkeypatch, _no_sso_person())
        assert adapter.create_user.call_args.kwargs["attributes"] == {
            "hitobitoId": ["7"]
        }
        assert [c.args[0] for c in mailcow.add_alias.call_args_list] == [USERNAME]
