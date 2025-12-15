import logging as _logging
import os as _os
import pathlib as _pathlib

import pytest


_SELFDIR = _pathlib.Path(__file__).parent.resolve()
if "WSJRDP_SCRIPTS_CONFIG_FOR_INTEGRATION_TESTS" in _os.environ:
    _WSJRDP_SCRIPTS_CONFIG = _pathlib.Path(
        _os.environ["WSJRDP_SCRIPTS_CONFIG_FOR_INTEGRATION_TESTS"]
    ).resolve()
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
def ctx(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    from wsjrdp2027 import WsjRdpContext

    test_name = request.node.name
    out_dir = _OUT_DIR / test_name

    with monkeypatch.context() as m:
        _logging.debug("Delete ENV WSJRDP_SCRIPTS_OUTPUT_DIR__OVERRIDE")
        m.delenv("WSJRDP_SCRIPTS_OUTPUT_DIR__OVERRIDE", raising=False)

        wsjrdp_ctx = WsjRdpContext(
            config=_WSJRDP_SCRIPTS_CONFIG,
            setup_logging=False,
            log_level=_logging.DEBUG,
            out_dir=out_dir,
            argv=["app"],
        )
        wsjrdp_ctx.configure_log_file(out_dir / f"{test_name}.log")
        yield wsjrdp_ctx


@pytest.fixture
def run_wsjrdp_script_out_dir(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
):
    test_name = request.node.name
    out_dir = _OUT_DIR / test_name
    out_dir.mkdir(exist_ok=True, parents=True)
    with monkeypatch.context() as m:
        _logging.debug("Delete ENV WSJRDP_SCRIPTS_OUTPUT_DIR__OVERRIDE")
        m.delenv("WSJRDP_SCRIPTS_OUTPUT_DIR__OVERRIDE", raising=False)
        _logging.debug("cd %s", out_dir)
        m.chdir(out_dir)
        yield out_dir


@pytest.fixture
def run_wsjrdp_script(run_wsjrdp_script_out_dir):
    from pytest_wsjrdp2027 import run_script

    return run_script


@pytest.fixture(scope="session", autouse=True)
def wsjrdp_scripts_integration_test_defaults(docker_services):
    _os.environ["WSJRDP_SCRIPTS_START_TIME"] = "2027-08-01 08:00:00"
    _os.environ["WSJRDP_SCRIPTS_CONFIG"] = str(_WSJRDP_SCRIPTS_CONFIG)
    _os.environ["WSJRDP_SCRIPTS_OUTPUT_DIR__OVERRIDE"] = str(_OUT_DIR)
