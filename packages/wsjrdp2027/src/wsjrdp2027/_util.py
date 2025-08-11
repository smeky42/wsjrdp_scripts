from __future__ import annotations

import typing as _typing

if _typing.TYPE_CHECKING:
    import datetime as _datetime
    import logging as _logging
    import pathlib as _pathlib


_CONSOLE_CONFIRM_DEFAULT_TO_CHOICE_DISPLAY = {
    None: "y/n",
    True: "Y/n",
    False: "y/N",
}

_CONSOLE_CONFIRM_INPUT_TO_VALUE = {
    "yes": True,
    "ye": True,
    "y": True,
    "no": False,
    "n": False,
}


def console_confirm(question, *, default: bool | None = False) -> bool:
    allowed_choices = _CONSOLE_CONFIRM_DEFAULT_TO_CHOICE_DISPLAY.get(default, "y/n")
    while True:
        raw_user_input = input(f"{question} [{allowed_choices}]? ")
        user_input = raw_user_input.strip().lower()
        if not user_input and default is not None:
            return default
        elif (val := _CONSOLE_CONFIRM_INPUT_TO_VALUE.get(user_input, None)) is not None:
            return val
        else:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n", flush=True)


def create_dir(
    path_pattern: str,
    *,
    parents: bool = True,
    exist_ok: bool = True,
    now: _datetime.datetime | None = None,
) -> _pathlib.Path:
    import datetime
    import pathlib

    if now is None:
        now = datetime.datetime.now()
    now_str = now.strftime("%Y%m%d-%H%M%S")
    path_str = path_pattern % {"now": now_str}
    path = pathlib.Path(path_str)
    path.mkdir(exist_ok=exist_ok, parents=parents)
    return path


def to_log_level(level: int | str | None, default: int | None = None) -> int:
    import logging

    if default is None:
        default = logging.DEBUG
    if level is None:
        return default
    elif isinstance(level, str):
        return logging.getLevelNamesMapping()[level]
    else:
        return level


def configure_file_logging(
    filename: str | _pathlib.Path,
    *,
    level: int | str | None,
    logger: _logging.Logger | str | None = None,
) -> _logging.Handler:
    import logging

    if logger is None:
        logger = logging.getLogger()
    elif isinstance(logger, str):
        logger = logging.getLogger(logger)
    level = to_log_level(level, default=logging.NOTSET)

    formatter = logging.Formatter("%(asctime)s %(levelname)-1s %(message)s")
    handler = logging.FileHandler(filename, encoding="utf-8")
    handler.setFormatter(formatter)
    handler.setLevel(level)
    logger.addHandler(handler)
    return handler


def to_date(date: _datetime.date | str) -> _datetime.date:
    import datetime
    import re

    if isinstance(date, str):
        if re.fullmatch("[0-9]+-[0-9]+-[0-9]+", date):
            return datetime.datetime.strptime(date, "%Y-%m-%d").date()
        elif re.fullmatch("[0-9]+[.][0-9]+[.][0-9]+", date):
            return datetime.datetime.strptime(date, "%d.%m.%Y").date()
        else:
            raise ValueError(f"Unsupported date format: {date!r}")
    else:
        return date
