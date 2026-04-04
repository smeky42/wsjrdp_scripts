from __future__ import annotations

import unittest.mock

import pytest
import wsjrdp2027
from wsjrdp2027 import KeycloakClient


@pytest.fixture
def keycloak_client(ctx: wsjrdp2027.WsjRdpContext):
    return ctx.keycloak()._client


class Test_KeyCloakClient:
    @pytest.fixture(autouse=True)
    def _setup(self, keycloak_client: KeycloakClient):
        keycloak_client.create_group("UL", exist_ok=True)
        try:
            yield
        finally:
            pass

    def test_remove_nonexisting_group(self, keycloak_client: KeycloakClient):
        keycloak_client.delete_group("non-existing-group")

    def test_remove_nonexisting_group__raise_on_missing(
        self, keycloak_client: KeycloakClient
    ):
        with pytest.raises(Exception, match="Group path does not exist"):
            keycloak_client.delete_group("non-existing-group", raise_on_missing=True)

    def test_get_realms(self, keycloak_client: KeycloakClient):
        realms = keycloak_client.get_realms()
        assert len(realms) == 2
        realm_names = sorted(realm["realm"] for realm in realms)
        assert realm_names == ["WSJ_development", "master"]

    def test_create_group(self, keycloak_client: KeycloakClient):
        try:
            group = keycloak_client.create_group("CMT")
            assert group["name"] == "CMT"
        finally:
            keycloak_client.delete_group("CMT")

    @pytest.mark.parametrize(
        "args,kwargs,expected_payload",
        [
            (
                ("foo@example.org", "pw"),
                {},
                {
                    "username": "foo@example.org",
                    "email": "foo@example.org",
                    "enabled": True,
                    "credentials": [{"type": "password", "value": "pw"}],
                },
            ),
        ],
    )
    def test_create_user__payload(
        self,
        keycloak_client: KeycloakClient,
        args,
        kwargs,
        expected_payload,
        monkeypatch,
    ):
        mock = unittest.mock.Mock(wraps=keycloak_client._admin.create_user)
        monkeypatch.setattr(keycloak_client._admin, "create_user", mock)

        user_dict = keycloak_client.create_user(*args, **kwargs)
        keycloak_client.delete_user(user_dict["username"])

        assert mock.call_count == 1
        assert mock.call_args == unittest.mock.call(expected_payload, exist_ok=True)

    def test_create_user(self, keycloak_client: KeycloakClient):
        user1 = keycloak_client.create_user("foo@foo", "pw123")
        user2 = keycloak_client.create_user("foo@foo", "pw123")
        assert user1 == user2
        assert user1["username"] == user1["email"]

        keycloak_client.delete_user("foo@foo")

    def test_create_or_update_user(self, keycloak_client: KeycloakClient):
        user1 = keycloak_client.create_or_update_user(
            "foo@baz", "pw", first_name="first"
        )
        user2 = keycloak_client.create_or_update_user(
            "foo@baz", "pw", first_name="second"
        )
        assert user1 != user2
        assert user1["username"] == user1["email"]
        assert user1.get("firstName") == "first"
        assert user2.get("firstName") == "second"

        keycloak_client.delete_user("foo@baz")
        keycloak_client.delete_user("foo@baz", raise_on_missing=False)

    def test_add_user_to_group(self, keycloak_client: KeycloakClient):
        keycloak_client.create_group("FOO")
        keycloak_client.create_user("foo@baz", "pw")
        keycloak_client.add_user_to_group("foo@baz", "FOO")
        user_dict_list = keycloak_client.get_users_in_group("FOO")
        assert len(user_dict_list) == 1
        user_dict = user_dict_list[0]
        assert user_dict["username"] == "foo@baz"

        keycloak_client.delete_user("foo@baz", raise_on_missing=True)
        keycloak_client.delete_group("FOO", raise_on_missing=True)


class Test_Keycloak:
    def test_create_group_and_user(self, ctx):
        adm = wsjrdp2027.keycloak.admin_login(ctx)
        adm.create_group({"name": "UL"}, skip_exists=True)

        username = "test11@noreply.worldscoutjamboree.de"
        email = username
        wsjrdp2027.keycloak.add_user(ctx, email, "Firstname", "Lastname", "pw123")

        user_dict = wsjrdp2027.keycloak.get_user(ctx, username)
        assert "firstName" in user_dict
        assert "lastName" in user_dict
        assert "email" in user_dict
        assert user_dict["firstName"] == "Firstname"
        assert user_dict["lastName"] == "Lastname"
        assert user_dict["email"] == email
        assert wsjrdp2027.keycloak.is_user_enabled(ctx, username)

        wsjrdp2027.keycloak.disable_user(ctx, username)
        assert not wsjrdp2027.keycloak.is_user_enabled(ctx, username)

        wsjrdp2027.keycloak.enable_user(ctx, username)
        assert wsjrdp2027.keycloak.is_user_enabled(ctx, username)

        wsjrdp2027.keycloak.add_user_to_group(ctx, email, "UL")
