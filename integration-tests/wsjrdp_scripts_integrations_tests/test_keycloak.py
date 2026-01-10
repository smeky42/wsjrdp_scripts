from __future__ import annotations

import wsjrdp2027


class Test_Keycloak:
    def test_get_realms(self, ctx):
        adm = wsjrdp2027.keycloak.admin_login(ctx)
        realms = adm.get_realms()
        assert len(realms) == 2
        realm_names = sorted(realm["realm"] for realm in realms)
        assert realm_names == ["WSJ_development", "master"]

    def test_create_group_and_user(self, ctx):
        adm = wsjrdp2027.keycloak.admin_login(ctx)
        adm.create_group({"name": "UL"}, skip_exists=True)

        username = "test11@noreply.worldscoutjamboree.de"
        email = username
        wsjrdp2027.keycloak.add_user(ctx, email, "Firstname", "Lastname", "pw123")

        user_dict = wsjrdp2027.keycloak.get_user(ctx, username)
        assert user_dict["firstName"] == "Firstname"
        assert user_dict["lastName"] == "Lastname"
        assert user_dict["email"] == email
        assert wsjrdp2027.keycloak.is_user_enabled(ctx, username)

        wsjrdp2027.keycloak.disable_user(ctx, username)
        assert not wsjrdp2027.keycloak.is_user_enabled(ctx, username)

        wsjrdp2027.keycloak.enable_user(ctx, username)
        assert wsjrdp2027.keycloak.is_user_enabled(ctx, username)

        wsjrdp2027.keycloak.add_user_to_group(ctx, email, "UL")
