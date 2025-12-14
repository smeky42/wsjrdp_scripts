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
        check=True,
    )
