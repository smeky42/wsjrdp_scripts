from __future__ import annotations

import collections.abc as _collections_abc
import logging as _logging
import pathlib as _pathlib
import typing as _typing


if _typing.TYPE_CHECKING:
    import typst as _typst


_SELFDIR = _pathlib.Path(__file__).parent.resolve()

_LOGGER = _logging.getLogger(__name__)

_PathLike = str | _pathlib.Path


def get_typst_font_paths() -> list[_pathlib.Path]:
    return [_SELFDIR / "_fonts"]


def typst_compile(
    input: _PathLike,
    output: _PathLike,
    *,
    font_paths: _collections_abc.Iterable[_PathLike] = (),
    format: _typst.OutputFormat | None = None,
    sys_inputs: dict[str, str] | None = None,
) -> None:
    import typst

    font_paths = list(font_paths)
    font_paths.extend(get_typst_font_paths())

    typst.compile(
        input,
        output,
        font_paths=font_paths,
        format=format,
        sys_inputs=(sys_inputs or {}),
    )
    _LOGGER.info(f"Wrote {output}")
