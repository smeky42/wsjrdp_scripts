from __future__ import annotations

import os as _os
import pathlib as _pathlib
import subprocess as _subprocess


_SELFDIR = _pathlib.Path(__file__).parent.resolve()
_ROOT_DIR = (_SELFDIR / ".." / "..").resolve()


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
