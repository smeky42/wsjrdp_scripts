from __future__ import annotations

import contextlib as _contextlib
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


def _clear_mailcow(mailcow_client: MailcowClient):
    alias_list = mailcow_client.get_alias_list()
    mailcow_client.delete_alias([a["address"] for a in alias_list])
    mailbox_list = mailcow_client.get_mailbox_list()
    mailcow_client.delete_mailbox([a["username"] for a in mailbox_list])
    domain_list = mailcow_client.get_domain_list()
    mailcow_client.delete_domain([d["domain_name"] for d in domain_list])


class Test_MailcowClient_Bare:
    @pytest.fixture
    def mailcow_client(self, ctx: wsjrdp2027.WsjRdpContext) -> MailcowClient:
        return ctx.mailcow()

    @pytest.fixture(autouse=True)
    def _auto_fixture(self, mailcow_client: MailcowClient):
        _clear_mailcow(mailcow_client)

    def test_add_domain(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")

        domain_list = mailcow_client.get_domain_list()
        assert len(domain_list) == 1
        domain_dict = domain_list[0]
        assert domain_dict["domain_name"] == "foo.local"

    @pytest.mark.parametrize("allow_cached", [True, False])
    def test_add_domain__not_updating_on_existing(
        self, mailcow_client: MailcowClient, allow_cached
    ):
        mailcow_client.add_domain("foo.local", aliases=17)

        domain_dict = mailcow_client.get_domain_by_name(
            "foo.local", allow_cached=allow_cached
        )
        assert domain_dict["domain_name"] == "foo.local"
        assert domain_dict["max_num_aliases_for_domain"] == 17

        mailcow_client.add_domain("foo.local", aliases=19, exist_ok=True)

        domain_dict = mailcow_client.get_domain_by_name(
            "foo.local", allow_cached=allow_cached
        )
        assert domain_dict["max_num_aliases_for_domain"] == 17

    def test_add_domain__fails_on_existing(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")
        with pytest.raises(MailcowError, match="domain_exists"):
            mailcow_client.add_domain("foo.local", exist_ok=False)
        domain_list = mailcow_client.get_domain_list()
        assert len(domain_list) == 1

    def test_add_domain__exist_ok(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")
        mailcow_client.add_domain("foo.local", exist_ok=True)
        domain_list = mailcow_client.get_domain_list()
        assert len(domain_list) == 1

    def test_add_delete_domain(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")
        mailcow_client.delete_domain(["foo.local"])
        domain_list = mailcow_client.get_domain_list(allow_cached=False)
        assert domain_list == []

    def test_add_delete_domain__cached(self, mailcow_client: MailcowClient):
        domain_list = mailcow_client.get_domain_list()  # load all domains
        mailcow_client.add_domain("foo.local")
        mailcow_client.delete_domain(["foo.local"])
        domain_list = mailcow_client.get_domain_list(allow_cached=True)
        assert domain_list == []

    def test_edit_domain__non_existing_domain__create_ok(
        self, mailcow_client: MailcowClient
    ):
        dm = mailcow_client.get_domain_or_none_by_name("foo.local")
        assert not dm

        mailcow_client.edit_domain("foo.local", active=False, create_ok=True)
        dm = mailcow_client.get_domain_by_name("foo.local")
        assert dm.domain_name == "foo.local"

    def test_edit_domain__non_existing_domain__failure(
        self, mailcow_client: MailcowClient
    ):
        dm = mailcow_client.get_domain_or_none_by_name("foo.local")
        assert not dm

        with pytest.raises(MailcowError):
            mailcow_client.edit_domain("foo.local", active=False, create_ok=False)

    def test_edit_domain__updating(self, mailcow_client: MailcowClient):
        dm = mailcow_client.add_domain("foo.local", exist_ok=True)
        assert dm.active
        assert dm.max_num_aliases_for_domain == 400
        assert dm.max_num_mboxes_for_domain == 10
        assert dm.max_quota_for_domain == 10737418240
        assert dm.max_quota_for_mbox == 10737418240
        assert dm.def_quota_for_mbox == 3221225472

        dm = mailcow_client.edit_domain(
            "foo.local",
            active=False,
            aliases=27,
            mailboxes=29,
            description="buzz",
            domain_quota_mib=239,
            max_mailbox_quota_mib=237,
            default_mailbox_quota_mib=235,
        )
        assert not dm.active
        assert dm.max_num_aliases_for_domain == 27
        assert dm.max_num_mboxes_for_domain == 29
        assert dm.max_quota_for_domain == 239 * 1024 * 1024
        assert dm.max_quota_for_mbox == 237 * 1024 * 1024
        assert dm.def_quota_for_mbox == 235 * 1024 * 1024
        assert dm.max_quota_for_mbox == dm["max_new_mailbox_quota"]
        assert dm.def_quota_for_mbox == dm["def_new_mailbox_quota"]
        assert dm.description == "buzz"

    def test_delete_domain__non_existing(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")
        mailcow_client.delete_domain(["foo.local", "foo2.local"])
        mailcow_client.delete_domain(["foo.local", "foo2.local"])
        domain_list = mailcow_client.get_domain_list()
        assert domain_list == []

    def test_get_all_domains__no_domains(self, mailcow_client: MailcowClient):
        domain_list = mailcow_client.get_domain_list()
        assert domain_list == []

    def test_get_all_domains__one_domain(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")
        domain_list = mailcow_client.get_domain_list()
        assert len(domain_list) == 1
        domain_dict = domain_list[0]
        assert domain_dict["domain_name"] == "foo.local"

    def test_get_aliases__empty(self, mailcow_client: MailcowClient):
        alias_list = mailcow_client.get_alias_list()
        assert alias_list == []

    def test_add_delete_alias__delete_using_client(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")

        mailcow_client.add_alias("foo@foo.local", goto="bar@example.org")
        mailcow_client.delete_alias("foo@foo.local")

        alias_list = mailcow_client.get_alias_list()
        assert alias_list == []

    def test_add_delete_alias__delete_using_alias(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")

        alias = mailcow_client.add_alias("foo@foo.local", goto="bar@example.org")
        alias.delete()

        alias_list = mailcow_client.get_alias_list()
        assert alias_list == []

    def test_add_mailbox__defaults(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")

        mb = mailcow_client.add_mailbox("foo@foo.local", password="foobarbaz")

        mailbox_list = mailcow_client.get_mailbox_list()
        assert len(mailbox_list) == 1
        mailbox = mailbox_list[0]
        assert mb == mailbox
        assert mb.username == "foo@foo.local"
        assert mb.quota_mib == 1024
        assert mb.authsource == "mailcow"
        assert mb.tls_enforce_in
        assert mb.tls_enforce_out
        assert not mb.force_pw_update

    def test_add_mailbox__quota(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")

        mb = mailcow_client.add_mailbox(
            "foo@foo.local", password="foobarbaz", quota_mib=12
        )
        assert mb.quota_b == 12 * 1024 * 1024

    def test_update_mailbox(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")

        mb1 = mailcow_client.add_mailbox(
            "foo@foo.local", password="foobarbaz", quota_mib=12, force_pw_update=False
        )
        assert mb1.quota_b == 12 * 1024 * 1024
        assert mb1.force_pw_update is False

        mb2 = mailcow_client.add_mailbox(
            "foo@foo.local", quota_mib=3, force_pw_update=True
        )
        assert mb2.quota_b == 3 * 1024 * 1024
        assert mb2.force_pw_update is True

    def test_update_mailbox__fails(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")

        mailcow_client.add_mailbox("foo@foo.local", password="foobarbaz")
        with pytest.raises(MailcowError, match="mailbox_exists"):
            mailcow_client.add_mailbox("foo@foo.local", exist_ok=False)

    def test_add_delete_mailbox__uncached(self, mailcow_client: MailcowClient):
        mailcow_client.add_domain("foo.local")

        mailcow_client.add_mailbox("foo@foo.local", password="foobarbaz")
        mailbox_list = mailcow_client.get_mailbox_list(allow_cached=False)
        assert len(mailbox_list) == 1

        mailcow_client.delete_mailbox("foo@foo.local")
        mailbox_list = mailcow_client.get_mailbox_list(allow_cached=False)
        assert len(mailbox_list) == 0

    def test_add_delete_mailbox__cached(self, mailcow_client: MailcowClient):
        mailbox_list = mailcow_client.get_mailbox_list(allow_cached=True)
        mailcow_client.add_domain("foo.local")

        mailcow_client.add_mailbox("foo@foo.local", password="foobarbaz")
        mailbox_list = mailcow_client.get_mailbox_list(allow_cached=True)
        assert len(mailbox_list) == 1

        mailcow_client.delete_mailbox("foo@foo.local")
        mailbox_list = mailcow_client.get_mailbox_list(allow_cached=True)
        assert len(mailbox_list) == 0


class Test_MailcowClient_Bare_DryRun:
    @pytest.fixture
    def dry_run_client(self, ctx: wsjrdp2027.WsjRdpContext) -> MailcowClient:
        client = ctx.mailcow(dry_run=True)
        assert client.dry_run
        return client

    @pytest.fixture(autouse=True)
    def _auto_fixture(self, dry_run_client: MailcowClient):
        _clear_mailcow(dry_run_client)

    def test_add_domain(self, dry_run_client: MailcowClient):
        dry_run_client.add_domain("foo.local")
        _LOGGER.info("get domain list")
        domain_list = dry_run_client.get_domain_list()
        assert len(domain_list) == 1
        domain = domain_list[0]
        assert domain.domain_name == "foo.local"

    def test_add_alias(self, dry_run_client: MailcowClient):
        dry_run_client.add_domain("foo.local")
        dry_run_client.add_alias("foo@foo.local", goto="bar@example.org")
        alias_list = dry_run_client.get_alias_list()
        assert len(alias_list) == 1
        alias = alias_list[0]
        assert alias.address == "foo@foo.local"
        assert alias.goto == ["bar@example.org"]

    def test_add_alias__update_alias(self, dry_run_client: MailcowClient):
        dry_run_client.add_domain("foo.local")
        dry_run_client.add_alias("foo@foo.local", goto="bar@example.org")
        dry_run_client.add_alias("foo@foo.local", add_goto="foo@example.org")
        alias_list = dry_run_client.get_alias_list()
        assert len(alias_list) == 1
        alias = alias_list[0]
        assert alias.address == "foo@foo.local"
        assert alias.goto == ["bar@example.org", "foo@example.org"]

    def test_add_mailbox(self, dry_run_client: MailcowClient):
        dry_run_client.add_domain("foo.local")
        dry_run_client.add_mailbox("foo@foo.local")
        mailbox_list = dry_run_client.get_mailbox_list()
        assert len(mailbox_list) == 1
        mailbox = mailbox_list[0]
        assert mailbox.username == "foo@foo.local"

    def test_add_mailbox__defaults(self, dry_run_client: MailcowClient):
        dry_run_client.add_domain("foo.local")

        mb = dry_run_client.add_mailbox("foo@foo.local", password="foobarbaz")

        mailbox_list = dry_run_client.get_mailbox_list()
        assert mailbox_list == [mb]

        assert mb.username == "foo@foo.local"
        assert mb.quota_mib == 1024
        assert mb.authsource == "mailcow"
        assert mb.tls_enforce_in
        assert mb.tls_enforce_out
        assert not mb.force_pw_update


class Test_MailcowClient_WSJRDP:
    @pytest.fixture
    def mailcow_client(self, ctx: wsjrdp2027.WsjRdpContext) -> MailcowClient:
        return ctx.mailcow()

    @pytest.fixture(autouse=True)
    def _auto_fixture(self, mailcow_client: MailcowClient):
        _clear_mailcow(mailcow_client)
        for domain in WSJRDP_DOMAINS:
            mailcow_client.add_domain(domain)

    def test_get_all_domains__wsjrdp_domains(self, mailcow_client: MailcowClient):
        domain_list = mailcow_client.get_domain_list()
        assert len(domain_list) == len(WSJRDP_DOMAINS)

    def test_add_alias__invalid_goto(self, mailcow_client: MailcowClient):
        with pytest.raises(MailcowError, match="goto_invalid bar@bar"):
            mailcow_client.add_alias("foo@bar", goto="bar@bar")

    def test_add_alias__invalid_address(self, mailcow_client: MailcowClient):
        with pytest.raises(MailcowError, match="domain_not_found bar"):
            mailcow_client.add_alias("foo@bar", goto="bar@example.org")

    def test_get_aliases__one_alias(self, mailcow_client: MailcowClient):
        mailcow_client.add_alias("foo@wsj.local", goto="bar@example.org")
        alias_list = mailcow_client.get_alias_list()
        assert len(alias_list) == 1
        alias = alias_list[0]
        assert alias["address"] == "foo@wsj.local"
        assert alias["goto"] == "bar@example.org"

    def test_get_aliases__two_aliases(self, mailcow_client: MailcowClient):
        mailcow_client.add_alias("foo@wsj.local", goto="bar@example.org")
        mailcow_client.add_alias("bar@wsj.local", goto="bar@example.org")

        alias_list = mailcow_client.get_alias_list()
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
