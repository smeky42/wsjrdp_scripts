import logging as _logging
import os as _os
import pathlib as _pathlib

import pytest


_SELFDIR = _pathlib.Path(__file__).parent.resolve()
if "WSJRDP_SCRIPTS_CONFIG_FOR_INTEGRATION_TESTS" in _os.environ:
    _WSJRDP_SCRIPTS_CONFIG = _pathlib.Path(
        _os.environ["WSJRDP_SCRIPTS_CONFIG_FOR_INTEGRATION_TESTS"]
    )
else:
    _WSJRDP_SCRIPTS_CONFIG = _SELFDIR / "config-integration-tests.yml"
_OUT_DIR = (_SELFDIR / ".." / "data" / "integration-tests").resolve()


@pytest.fixture(scope="session")
def docker_compose_project_name() -> str:
    return "wsjrdp_scripts-tests"


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    _logging.debug("pytestconfig.rootdir: %s", pytestconfig.rootdir)
    return _os.path.join(str(pytestconfig.rootdir), "docker-compose.yml")


@pytest.fixture(scope="session")
def docker_cleanup():
    # return ["down -v"]
    return False


@pytest.fixture
def ctx():
    from wsjrdp2027 import WsjRdpContext

    return WsjRdpContext(
        config=_WSJRDP_SCRIPTS_CONFIG,
        setup_logging=False,
        log_level=_logging.DEBUG,
        out_dir=_OUT_DIR,
        argv=["app"],
    )


@pytest.fixture(scope="session", autouse=True)
def wsjrdp_scripts_integration_test_defaults(docker_services):
    _os.environ["WSJRDP_SCRIPTS_CONFIG"] = str(_WSJRDP_SCRIPTS_CONFIG)
    _os.environ["WSJRDP_SCRIPTS_OUTPUT_DIR__OVERRIDE"] = str(_OUT_DIR)
