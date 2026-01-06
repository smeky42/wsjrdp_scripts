from keycloak import KeycloakAdmin, KeycloakOpenID
from keycloak.exceptions import KeycloakAuthenticationError
import logging

_LOGGER = logging.getLogger(__name__)

class Keycloak():
    def __login(ctx):
        try:
            return KeycloakAdmin(
                server_url="https://login.worldscoutjamboree.de",
                username=ctx._config.keycloak_admin,
                password=ctx._config.keycloak_admin_password,
                realm_name=ctx._config.keycloak_realm,
                verify=True
            )
        except Exception as e:
            _LOGGER.error('Keycloak.__login() failed: %s', e)

    def __get_realm():
        try:
            return KeycloakOpenID(server_url="https://login.worldscoutjamboree.de",
                                client_id="",
                                realm_name="WSJ",
                                client_secret_key="")
        except Exception as e:
            _LOGGER.error('Keycloak.__get_realm() failed: %s', e)

    def __get_user_id(username):
        try:
            keycloak_admin = Keycloak.__login()
            return keycloak_admin.get_user_id(username)
        except Exception as e:
            _LOGGER.error('Keycloak.__get_user_id(username=%s) failed: %s', username, e)

    def add_user(ctx, mail, firstname, lastname, pw):
        try:
            keycloak_admin = Keycloak.__login(ctx)
            _LOGGER.info('Keycloak.add_user: trying to create user with: mail=%s, firstname=%s, lastname=%s', mail, firstname, lastname)
            return keycloak_admin.create_user({"email": mail,
                                                "username": mail,
                                                "enabled": True,
                                                "firstName": firstname,
                                                "lastName": lastname,
                                                "requiredActions": ["UPDATE_PASSWORD"],
                                                "credentials": [{"value": pw,"type": "password",}]})
        except Exception as e:
            _LOGGER.error('Keycloak.add_user(mail=%s, firstname=%s, lastname=%s) failed: %s', mail, firstname, lastname, e)
            _LOGGER.error('Keycloak.add_user: trying to update user with: mail=%s, firstname=%s, lastname=%s', mail, firstname, lastname)
            Keycloak.edit_user(ctx, mail, firstname, lastname, mail)

    def set_user_pw(ctx, username, pw):
        try:
            keycloak_admin = Keycloak.__login(ctx)
            userID = Keycloak.__get_user_id(username)
            return keycloak_admin.set_user_password(
                user_id=userID,
                password=pw,
                temporary=True
            )
        except Exception as e:
            _LOGGER.error('Keycloak.set_user_pw(username=%s) failed: %s', username, e)


    def edit_user(ctx, username, firstname, lastname, mail):
        try:
            keycloak_admin = Keycloak.__login(ctx)
            userID = Keycloak.__get_user_id(ctx,username)
            return keycloak_admin.update_user(
                user_id = userID,
                payload = {
                'firstName': firstname,
                'lastName': lastname,
                'email': mail
                }
            )
        except Exception as e:
            _LOGGER.error('Keycloak.edit_user(username=%s, firstname=%s, lastname=%s, mail=%s) failed: %s', username, firstname, lastname, mail, e)

    def enable_user(ctx, username):
        try:
            keycloak_admin = Keycloak.__login(ctx)
            userID = Keycloak.__get_user_id(ctx,username)
            return keycloak_admin.enable_user(userID)
        except Exception as e:
            _LOGGER.error('Keycloak.enable_user(username=%s) failed: %s', username, e)

    def disable_user(ctx, username):
        try:
            keycloak_admin = Keycloak.__login(ctx)
            userID = Keycloak.__get_user_id(ctx,username)
            return keycloak_admin.disable_user(userID)
        except Exception as e:
            _LOGGER.error('Keycloak.disable_user(username=%s) failed: %s', username, e)

    def delete_user(ctx,userID):
        try:
            keycloak_admin = Keycloak.__login(ctx)
            return keycloak_admin.delete_user(ctx, userID)
        except Exception as e:
            _LOGGER.error('Keycloak.delete_user(userID=%s) failed: %s', userID, e)

    def get_users(ctx):
        try:
            keycloak_admin = Keycloak.__login(ctx)
            return keycloak_admin.get_users({})
        except Exception as e:
            _LOGGER.error('Keycloak.get_users() failed: %s', e)

    def is_admin(ctx, userID):
        try:
            keycloak_admin = Keycloak.__login(ctx)
            if str(keycloak_admin.get_user(userID)['email']).endswith('esh.essen.de'):
                return True
            else:
                return False
        except Exception as e:
            _LOGGER.error('Keycloak.is_admin(userID=%s) failed: %s', userID, e)
            return True #temp fix. Users are deleted if keycloak api call fails -> main: remove non admin users from keycloak if missing in db

    def verify_token(headers):
        try:
            keycloak_openid = Keycloak.__get_realm()
            # keycloakPub = "-----BEGIN PUBLIC KEY-----\n" + keycloak_openid.public_key() + "\n-----END PUBLIC KEY-----"
            # options = {"verify_signature": True, "verify_aud": False, "verify_exp": True}
            # tokenInfo = keycloak_openid.decode_token(headers['Authorization'][7:], key=keycloakPub, options=options)
            tokenInfo = keycloak_openid.decode_token(headers['Authorization'][7:])
            return 200

        except KeycloakAuthenticationError as e:
            _LOGGER.error('Keycloak.verify_token(headers=%s) failed: %s', headers, e)
            return 401

        except KeyError as e:
            _LOGGER.error('Keycloak.verify_token(headers=%s) failed: %s', headers, e)
            return 401
    
    def get_user_of_token(headers):
        try:
            keycloak_openid = Keycloak.__get_realm()
            keycloak_admin = Keycloak.__login()
            # keycloakPub = "-----BEGIN PUBLIC KEY-----\n" + keycloak_openid.public_key() + "\n-----END PUBLIC KEY-----"
            # options = {"verify_signature": True, "verify_aud": False, "verify_exp": True}
            # tokenInfo = keycloak_openid.decode_token(headers['Authorization'][7:], key=keycloakPub, options=options)
            tokenInfo = keycloak_openid.decode_token(headers['Authorization'][7:])
        
            user = keycloak_admin.get_user(tokenInfo['sub'])
            return user['username']

        except Exception as e:
            _LOGGER.error('Keycloak.get_user_of_token(headers=%s) failed: %s', headers, e)
            return ''
        
    def is_user_in_roles(headers):
        try:
            keycloak_openid = Keycloak.__get_realm()
        # keycloak_admin = Keycloak.__login()
            # keycloakPub = "-----BEGIN PUBLIC KEY-----\n" + keycloak_openid.public_key() + "\n-----END PUBLIC KEY-----"
            # options = {"verify_signature": True, "verify_aud": False, "verify_exp": True}
            # tokenInfo = keycloak_openid.decode_token(headers['Authorization'][7:], key=keycloakPub, options=options)
            tokenInfo = keycloak_openid.decode_token(headers['Authorization'][7:])
        
            # roles = keycloak_admin.get_user(tokenInfo['realm_access']['roles'])
            roles = tokenInfo['realm_access']['roles']
            if 'GAP-GUACAMOLE-USER' in roles or 'GAP-GUACAMOLE-ADMIN' in roles:
                return True
            else:
                return False
        except Exception as e:
            _LOGGER.error('Keycloak.is_user_in_roles(headers=%s) failed: %s', headers, e)
            return False
        

