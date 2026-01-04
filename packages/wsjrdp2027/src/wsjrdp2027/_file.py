from __future__ import annotations

import io as _io
import os as _os
import pathlib as _pathlib
import typing as _typing


if _typing.TYPE_CHECKING:
    import io as _io
    import os as _os
    import pathlib as _pathlib


PathLike = _typing.Union["_pathlib.Path", "_os.PathLike", str, bytes]


def slurp(
    path_or_file: PathLike | _io.Reader[str],
    encoding: str = "utf-8",
    newline: str | None = None,
) -> str:
    import os as _os
    import pathlib as _pathlib

    if isinstance(path_or_file, (_pathlib.Path, str, bytes, _os.PathLike)):
        with open(path_or_file, encoding=encoding, newline=newline) as f:  # ty: ignore
            return f.read()
    else:
        return path_or_file.read()


def slurp_bytes(path_or_file: PathLike | _io.Reader[bytes]) -> bytes:
    import os as _os
    import pathlib as _pathlib

    if isinstance(path_or_file, (_pathlib.Path, str, bytes, _os.PathLike)):
        with open(path_or_file, "rb") as f:  # ty: ignore
            return f.read()
    else:
        return path_or_file.read()


def detect_xml_encoding(content: bytes) -> str:
    import contextlib
    from xml.parsers import expat

    class EncodingDetector:
        def __init__(self):
            self.encoding = "utf-8"

        def xml_decl_handler(self, version, encoding, standalone):
            self.encoding = encoding

        def start_namespace(self, prefix, uri):
            print(f"Detected Namespace: Prefix='{prefix}', URI='{uri}'")

    detector = EncodingDetector()
    parser = expat.ParserCreate(namespace_separator=" ")
    parser.XmlDeclHandler = detector.xml_decl_handler
    parser.StartNamespaceDeclHandler = detector.start_namespace
    with contextlib.suppress(expat.ExpatError):
        parser.Parse(content, False)
    return detector.encoding or "utf-8"
