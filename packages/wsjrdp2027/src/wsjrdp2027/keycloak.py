import collections.abc as _collections_abc
import logging
import typing as _typing

import keycloak as _keycloak
from keycloak.exceptions import KeycloakAuthenticationError


if _typing.TYPE_CHECKING:
    from . import _context

_LOGGER = logging.getLogger(__name__)


def log_and_reraise[F: _collections_abc.Callable[..., _typing.Any]](func: F) -> F:
    import functools

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            call_args = [repr(a) for a in args]
            call_args.extend(f"{k}={v}" for k, v in kwargs.items())
            _LOGGER.error(f"{func.__qualname__}({', '.join(call_args)}) failed: {exc}")
            raise

    return wrapped  # type: ignore


@log_and_reraise
def admin_login(
    ctx: _context.WsjRdpContext, *, verify: bool = True
) -> _keycloak.KeycloakAdmin:
    return _keycloak.KeycloakAdmin(
        server_url=ctx.config.keycloak_url,
        username=ctx.config.keycloak_admin,
        password=ctx.config.keycloak_admin_password,
        realm_name=ctx.config.keycloak_realm,
        user_realm_name=ctx.config.keycloak_user_realm or ctx.config.keycloak_realm,
        verify=verify,
    )


@log_and_reraise
def __get_realm(ctx: _context.WsjRdpContext) -> _keycloak.KeycloakOpenID:
    return _keycloak.KeycloakOpenID(
        server_url=ctx.config.keycloak_url,
        client_id="",
        realm_name=ctx.config.keycloak_realm,
        client_secret_key="",
    )


@log_and_reraise
def __get_user_id(ctx: _context.WsjRdpContext, username: str) -> str:
    keycloak_admin = admin_login(ctx)
    user_id = keycloak_admin.get_user_id(username)
    if not user_id:
        raise RuntimeError(f"Failed to get a user_id for {username=}")
    return user_id


def add_user(
    ctx: _context.WsjRdpContext,
    email: str,
    first_name: str,
    last_name: str,
    password: str,
    username: str | None = None,
    enabled: bool = True,
    attributes: list[dict[str, str]] | None = None,
) -> None:
    keycloak_admin = admin_login(ctx)
    _LOGGER.info("add_user: trying to create user with: mail=%s, firstname=%s, lastname=%s", email, first_name, last_name)

    try:
        keycloak_admin.create_user(
            {
                "username": username or email,
                "email": email,
                "enabled": enabled,
                "firstName": first_name,
                "lastName": last_name,
                "credentials": [{"type": "password", "value": password}],
                "attributes": attributes or [],
            }
        )
    except Exception as e:
        _LOGGER.error(
            "add_user(mail=%s, firstname=%s, lastname=%s) failed: %s",
            email,
            first_name,
            last_name,
            e,
        )
        _LOGGER.error(
            "add_user: trying to update user with: mail=%s, firstname=%s, lastname=%s",
            email,
            first_name,
            last_name,
        )
        edit_user(ctx, email, first_name, last_name, email)


def add_user_to_group(ctx: _context.WsjRdpContext, username: str, group_name: str):
    try:
        keycloak_admin = admin_login(ctx)
        user_id = __get_user_id(ctx, username)
        groups = keycloak_admin.get_groups()
        group_id = None
        for group in groups:
            if group["name"] == group_name:
                group_id = group["id"]
                break
        if group_id is not None:
            _LOGGER.info(
                "add_user_to_group: adding user_id=%s (%s) to group_name=%s",
                user_id,
                username,
                group_name,
            )
            return keycloak_admin.group_user_add(user_id, group_id)
        else:
            _LOGGER.error(
                "add_user_to_group(username=%s, group_name=%s) failed: Group not found",
                username,
                group_name,
            )
    except Exception as exc:
        _LOGGER.error(
            "add_user_to_group(username=%s, group_name=%s) failed: %s",
            username,
            group_name,
            exc,
        )


@log_and_reraise
def set_user_password(
    ctx: _context.WsjRdpContext, username: str, password: str
) -> dict:
    keycloak_admin = admin_login(ctx)
    user_id = __get_user_id(ctx, username)
    return keycloak_admin.set_user_password(
        user_id=user_id, password=password, temporary=True
    )


@log_and_reraise
def get_user(
    ctx: _context.WsjRdpContext, username: str, user_profile_metadata: bool = False
) -> dict:
    keycloak_admin = admin_login(ctx)
    user_id = __get_user_id(ctx, username)
    return keycloak_admin.get_user(user_id, user_profile_metadata=user_profile_metadata)


@log_and_reraise
def edit_user(
    ctx: _context.WsjRdpContext,
    username: str,
    first_name: str | None = None,
    last_name: str | None = None,
    email: str | None = None,
) -> None:
    keycloak_admin = admin_login(ctx)
    user_id = __get_user_id(ctx, username)
    payload = {"firstName": first_name, "lastName": last_name, "email": email}
    payload = {k: v for k, v in payload.items() if v is not None}
    if payload:
        keycloak_admin.update_user(user_id=user_id, payload=payload)


@log_and_reraise
def is_user_enabled(ctx: _context.WsjRdpContext, username: str) -> bool:
    user_dict = get_user(ctx, username, user_profile_metadata=False)
    return user_dict["enabled"]


@log_and_reraise
def enable_user(ctx: _context.WsjRdpContext, username: str) -> None:
    keycloak_admin = admin_login(ctx)
    userID = __get_user_id(ctx, username)
    keycloak_admin.enable_user(userID)


@log_and_reraise
def disable_user(ctx: _context.WsjRdpContext, username: str):
    keycloak_admin = admin_login(ctx)
    userID = __get_user_id(ctx, username)
    return keycloak_admin.disable_user(userID)


@log_and_reraise
def delete_user(ctx: _context.WsjRdpContext, user_id):
    keycloak_admin = admin_login(ctx)
    return keycloak_admin.delete_user(user_id)


@log_and_reraise
def get_users(ctx: _context.WsjRdpContext):
    keycloak_admin = admin_login(ctx)
    return keycloak_admin.get_users({})


def is_admin(ctx, userID):
    try:
        keycloak_admin = admin_login(ctx)
        if str(keycloak_admin.get_user(userID)["email"]).endswith("esh.essen.de"):
            return True
        else:
            return False
    except Exception as e:
        _LOGGER.error("is_admin(userID=%s) failed: %s", userID, e)
        return True  # temp fix. Users are deleted if keycloak api call fails -> main: remove non admin users from keycloak if missing in db


def verify_token(ctx, headers):
    try:
        keycloak_openid = __get_realm(ctx)
        # keycloakPub = "-----BEGIN PUBLIC KEY-----\n" + keycloak_openid.public_key() + "\n-----END PUBLIC KEY-----"
        # options = {"verify_signature": True, "verify_aud": False, "verify_exp": True}
        # tokenInfo = keycloak_openid.decode_token(headers['Authorization'][7:], key=keycloakPub, options=options)
        tokenInfo = keycloak_openid.decode_token(headers["Authorization"][7:])
        return 200

    except KeycloakAuthenticationError as e:
        _LOGGER.error("verify_token(headers=%s) failed: %s", headers, e)
        return 401

    except KeyError as e:
        _LOGGER.error("verify_token(headers=%s) failed: %s", headers, e)
        return 401


def get_user_of_token(ctx, headers):
    try:
        keycloak_openid = __get_realm(ctx)
        keycloak_admin = admin_login(ctx)
        # keycloakPub = "-----BEGIN PUBLIC KEY-----\n" + keycloak_openid.public_key() + "\n-----END PUBLIC KEY-----"
        # options = {"verify_signature": True, "verify_aud": False, "verify_exp": True}
        # tokenInfo = keycloak_openid.decode_token(headers['Authorization'][7:], key=keycloakPub, options=options)
        tokenInfo = keycloak_openid.decode_token(headers["Authorization"][7:])

        user = keycloak_admin.get_user(tokenInfo["sub"])
        return user["username"]

    except Exception as e:
        _LOGGER.error("get_user_of_token(headers=%s) failed: %s", headers, e)
        return ""


def is_user_in_roles(ctx, headers):
    try:
        keycloak_openid = __get_realm(ctx)
        # keycloak_admin = __login()
        # keycloakPub = "-----BEGIN PUBLIC KEY-----\n" + keycloak_openid.public_key() + "\n-----END PUBLIC KEY-----"
        # options = {"verify_signature": True, "verify_aud": False, "verify_exp": True}
        # tokenInfo = keycloak_openid.decode_token(headers['Authorization'][7:], key=keycloakPub, options=options)
        tokenInfo = keycloak_openid.decode_token(headers["Authorization"][7:])

        # roles = keycloak_admin.get_user(tokenInfo['realm_access']['roles'])
        roles = tokenInfo["realm_access"]["roles"]
        if "GAP-GUACAMOLE-USER" in roles or "GAP-GUACAMOLE-ADMIN" in roles:
            return True
        else:
            return False
    except Exception as e:
        _LOGGER.error("is_user_in_roles(headers=%s) failed: %s", headers, e)
        return False
