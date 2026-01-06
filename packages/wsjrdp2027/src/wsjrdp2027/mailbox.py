

import logging
import requests

_LOGGER = logging.getLogger(__name__)

def add_mailbox(ctx, local_part, domain, name, password):
    headers = {
        'Content-Type': 'application/json',
        'X-API-Key': ctx._config.mail_api_key
    }

    payload = {
        "local_part": local_part,
        "domain": domain,
        "name": name,
        "quota": "1024",
        "password": password,
        "password2": password,
        "active": "1",
        "force_pw_update": "1",
        "tls_enforce_in": "1",
        "tls_enforce_out": "1"
    }

    resp = requests.post("https://mail.worldscoutjamboree.de/api/v1/add/mailbox", json=payload, headers=headers, timeout=30)
    #resp.raise_for_status()  # optional: raise exception for HTTP error codes

    _LOGGER.info("Response: %s", resp.text)