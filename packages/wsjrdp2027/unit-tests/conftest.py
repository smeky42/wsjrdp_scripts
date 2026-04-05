import pytest


@pytest.fixture
def wsjrdp_config():
    from wsjrdp2027 import WsjRdpContextConfig

    return WsjRdpContextConfig(is_production=False, keycloak_url="", helpdesk_url="")


@pytest.fixture
def wsjrdp_context(wsjrdp_config):
    from wsjrdp2027 import WsjRdpContext

    return WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)
