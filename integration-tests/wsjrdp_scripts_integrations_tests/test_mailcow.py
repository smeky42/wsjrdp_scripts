from __future__ import annotations

import logging

import pytest
import wsjrdp2027
from wsjrdp2027 import MailcowClient, MailcowError


_LOGGER = logging.getLogger()


WSJRDP_DOMAINS = [
    "worldscoutjamboree.de",
    "units.worldscoutjamboree.de",
    "ist.worldscoutjamboree.de",
    "bmt.worldscoutjamboree.de",
    "wsj.local",
]


@pytest.fixture
def mailcow_client(ctx: wsjrdp2027.WsjRdpContext) -> MailcowClient:
    return ctx.mailcow()


@pytest.fixture
def clear_mailcow(mailcow_client: MailcowClient):
    try:
        yield
    finally:
        domain_list = mailcow_client.get_all_domains()
        mailcow_client.delete_domain([d["domain_name"] for d in domain_list])
        alias_list = mailcow_client.get_all_aliases()
        mailcow_client.delete_alias([a["address"] for a in alias_list])


class Test_MailcowClient_Bare:
    @pytest.fixture(autouse=True)
    def _auto_fixture(self, mailcow_client: MailcowClient, clear_mailcow):
        pass

    def test_create_domain(self, mailcow_client: MailcowClient):
        mailcow_client.create_domain("foo.local")

        domain_list = mailcow_client.get_all_domains()
        assert len(domain_list) == 1
        domain_dict = domain_list[0]
        assert domain_dict["domain_name"] == "foo.local"

    def test_create_domain__fails_on_existing(self, mailcow_client: MailcowClient):
        mailcow_client.create_domain("foo.local")
        with pytest.raises(MailcowError, match="domain_exists"):
            mailcow_client.create_domain("foo.local", exist_ok=False)
        domain_list = mailcow_client.get_all_domains()
        assert len(domain_list) == 1

    def test_create_domain__exist_ok(self, mailcow_client: MailcowClient):
        mailcow_client.create_domain("foo.local")
        mailcow_client.create_domain("foo.local", exist_ok=True)
        domain_list = mailcow_client.get_all_domains()
        assert len(domain_list) == 1

    def test_create_delete_domain(self, mailcow_client: MailcowClient):
        mailcow_client.create_domain("foo.local")
        mailcow_client.delete_domain(["foo.local"])
        domain_list = mailcow_client.get_all_domains()
        assert domain_list == []

    def test_delete_domain__non_existing(self, mailcow_client: MailcowClient):
        mailcow_client.create_domain("foo.local")
        mailcow_client.delete_domain(["foo.local", "foo2.local"])
        mailcow_client.delete_domain(["foo.local", "foo2.local"])
        domain_list = mailcow_client.get_all_domains()
        assert domain_list == []

    def test_get_all_domains__no_domains(self, mailcow_client: MailcowClient):
        domain_list = mailcow_client.get_all_domains()
        assert domain_list == []

    def test_get_all_domains__one_domain(self, mailcow_client: MailcowClient):
        mailcow_client.create_domain("foo.local")
        domain_list = mailcow_client.get_all_domains()
        assert len(domain_list) == 1
        domain_dict = domain_list[0]
        assert domain_dict["domain_name"] == "foo.local"

    def test_get_aliases__empty(self, mailcow_client: MailcowClient):
        alias_list = mailcow_client.get_all_aliases()
        assert alias_list == []

    def test_add_delete_alias__delete_using_client(self, mailcow_client: MailcowClient):
        mailcow_client.create_domain("foo.local")

        mailcow_client.add_alias("foo@foo.local", goto="bar@example.org")
        mailcow_client.delete_alias("foo@foo.local")

        alias_list = mailcow_client.get_all_aliases()
        assert alias_list == []

    def test_add_delete_alias__delete_using_alias(self, mailcow_client: MailcowClient):
        mailcow_client.create_domain("foo.local")

        alias = mailcow_client.add_alias("foo@foo.local", goto="bar@example.org")
        alias.delete()

        alias_list = mailcow_client.get_all_aliases()
        assert alias_list == []


class Test_MailcowClietnt_WSJRDP:
    @pytest.fixture(autouse=True)
    def _auto_fixture(self, mailcow_client: MailcowClient, clear_mailcow):
        for domain in WSJRDP_DOMAINS:
            mailcow_client.create_domain(domain)

    def test_get_all_domains__wsjrdp_domains(self, mailcow_client: MailcowClient):
        domain_list = mailcow_client.get_all_domains()
        assert len(domain_list) == len(WSJRDP_DOMAINS)

    def test_add_alias__invalid_goto(self, mailcow_client: MailcowClient):
        with pytest.raises(MailcowError, match="goto_invalid bar@bar"):
            mailcow_client.add_alias("foo@bar", goto="bar@bar")

    def test_add_alias__invalid_address(self, mailcow_client: MailcowClient):
        with pytest.raises(MailcowError, match="domain_not_found bar"):
            mailcow_client.add_alias("foo@bar", goto="bar@example.org")

    def test_get_aliases__one_alias(self, mailcow_client: MailcowClient):
        mailcow_client.add_alias("foo@wsj.local", goto="bar@example.org")
        alias_list = mailcow_client.get_all_aliases()
        assert len(alias_list) == 1
        alias = alias_list[0]
        assert alias["address"] == "foo@wsj.local"
        assert alias["goto"] == "bar@example.org"

    def test_get_aliases__two_aliases(self, mailcow_client: MailcowClient):
        mailcow_client.add_alias("foo@wsj.local", goto="bar@example.org")
        mailcow_client.add_alias("bar@wsj.local", goto="bar@example.org")

        alias_list = mailcow_client.get_all_aliases()
        assert len(alias_list) == 2
        addresses = {a["address"] for a in alias_list}
        assert addresses == {"foo@wsj.local", "bar@wsj.local"}

    def test_delete_alias__error__not_existing(self, mailcow_client: MailcowClient):
        with pytest.raises(MailcowError, match="access_denied"):
            mailcow_client.delete_alias("foo@wsj.local")

    def test_delete_alias__error__invalid_domain2(self, mailcow_client: MailcowClient):
        with pytest.raises(MailcowError, match="access_denied"):
            mailcow_client.delete_alias("foo@wsj.local")

    def test_add_alias__add_goto(self, mailcow_client: MailcowClient):
        mailcow_client.add_alias("foo@wsj.local", add_goto="foo@example.org")
        mailcow_client.add_alias("foo@wsj.local", add_goto="bar@example.org")

        alias = mailcow_client.get_alias_by_address("foo@wsj.local")
        assert alias.goto == ["foo@example.org", "bar@example.org"]

    def test_add_alias__add_goto__remove_goto(self, mailcow_client: MailcowClient):
        mailcow_client.add_alias("foo@wsj.local", add_goto="foo@example.org")
        mailcow_client.add_alias("foo@wsj.local", remove_goto="foo@example.org")

        alias = mailcow_client.get_alias_by_address("foo@wsj.local")
        assert alias.goto == []
        assert alias.goto_null

    def test_add_alias__add_goto_null(self, mailcow_client: MailcowClient):
        mailcow_client.add_alias("foo@wsj.local", goto_null=True)

        alias = mailcow_client.get_alias_by_address("foo@wsj.local")
        assert alias.goto == []
        assert alias.goto_null
        assert not alias.goto_spam
        assert not alias.goto_ham

    def test_add_alias__add_goto_null__add_goto_ham(
        self, mailcow_client: MailcowClient
    ):
        mailcow_client.add_alias("foo@wsj.local", goto_null=True)
        _LOGGER.info("=" * 40)
        mailcow_client.add_alias("foo@wsj.local", goto_ham=True)

        alias = mailcow_client.get_alias_by_address("foo@wsj.local")
        assert alias.goto == []
        assert not alias.goto_null
        assert not alias.goto_spam
        assert alias.goto_ham
