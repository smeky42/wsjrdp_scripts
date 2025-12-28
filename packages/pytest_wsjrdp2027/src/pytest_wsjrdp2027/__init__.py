from __future__ import annotations

import logging as _logging
import os as _os
import pathlib as _pathlib
import shlex as _shlex
import subprocess as _subprocess


_LOGGER = _logging.getLogger(__name__)
_SELFDIR = _pathlib.Path(__file__).parent.resolve()
_ROOT_DIR = (_SELFDIR / ".." / ".." / ".." / "..").resolve()


def uv_run(
    args,
    stderr=_subprocess.STDOUT,
    env=None,
    check=True,
    env_update=None,
    out_dir_override=None,
    cwd=None,
    ctx=None,
    **kwargs,
):
    if env is None:
        env = _os.environ.copy()
    else:
        env = env.copy()
    env["WSJRDP_SCRIPTS_CONFIG"] = str(
        _ROOT_DIR / "integration-tests" / "config-integration-tests.yml"
    )
    if env_update is not None:
        env.update(env_update)
    if ctx is not None:
        if out_dir_override is None:
            out_dir_override = str(ctx.out_dir)
    if out_dir_override:
        env["WSJRDP_SCRIPTS_OUTPUT_DIR__OVERRIDE"] = str(out_dir_override)
    if isinstance(args, str):
        args = f"uv run {args}"
        cmd_string = args
    else:
        args = ["uv", "run", *(str(a) for a in args)]
        cmd_string = " ".join(_shlex.quote(a) for a in args)
    _LOGGER.info("Run %s", cmd_string)
    return _subprocess.run(args, **kwargs, stderr=stderr, env=env, check=check, cwd=cwd)


def restore_integration_tests_db():
    uv_run(["./tools/db_restore.py", "./integration-tests/hitobito_production.dump"])


def run_script(script, *args):
    if not _os.path.isabs(script):
        script = _os.path.join(_ROOT_DIR, script)

    _subprocess.run(
        ["uv", "run", script, *args],
        stderr=_subprocess.STDOUT,
        check=True,
    )


def parse_sdd_xml(filename: _pathlib.Path | str) -> dict:
    import lxml.etree

    with open(filename, "r") as f:
        x_tree = lxml.etree.parse(f)

    x = x_tree.getroot()
    ns = {"sepa": "urn:iso:std:iso:20022:tech:xsd:pain.008.001.02"}
    grp_hdr_elt = x.xpath(
        "//sepa:Document/sepa:CstmrDrctDbtInitn/sepa:GrpHdr", namespaces=ns
    )[0]
    ctrl_sum_elt = grp_hdr_elt.xpath("sepa:CtrlSum", namespaces=ns)[0]
    ctrl_sum_cents = int(float(ctrl_sum_elt.text) * 100)

    return {
        "etree": x,
        "grp_hdr_elt": grp_hdr_elt,
        "ctrl_sum_cents": ctrl_sum_cents,
        "ctrl_sum_elt": ctrl_sum_elt,
    }
