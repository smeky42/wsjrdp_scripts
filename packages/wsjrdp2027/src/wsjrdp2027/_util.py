from __future__ import annotations

import collections.abc as _collections_abc
import logging as _logging
import math as _math
import typing as _typing

from . import _types


if _typing.TYPE_CHECKING:
    import datetime as _datetime
    import email.policy as _email_policy
    import logging as _logging
    import pathlib as _pathlib

    import pandas as _pandas


_LOGGER = _logging.getLogger(__name__)


__all__ = [
    "PrefixLoggerAdapter",
    "combine_where",
    "configure_file_logging",
    "console_confirm",
    "create_dir",
    "dataframe_copy_for_xlsx",
    "date_to_datetime",
    "dedup",
    "dedup_iter",
    "format_cents_as_eur_de",
    "get_default_email_policy",
    "in_expr",
    "log_exception_decorator",
    "merge_mail_addresses",
    "render_template",
    "slurp",
    "to_date_or_none",
    "to_datetime",
    "to_datetime_or_none",
    "to_int_list_or_none",
    "to_int_or_none",
    "to_log_level",
    "to_str_list_or_none",
    "to_yaml_str",
]

_T = _typing.TypeVar("_T")
_R = _typing.TypeVar("_R")


class PrefixLoggerAdapter(_logging.LoggerAdapter):
    def __init__(
        self,
        logger: _logging.Logger | _logging.LoggerAdapter,
        *,
        prefix: str,
    ) -> None:
        self._prefix = prefix
        super().__init__(logger)

    def process(self, msg, kwargs):
        return (f"{self._prefix} {msg}", kwargs)


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
        raw_user_input = input(f"{question} [{allowed_choices}] ")
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
    import logging as _logging
    import pathlib as _pathlib

    if logger is None:
        logger = _logging.getLogger()
    elif isinstance(logger, str):
        logger = _logging.getLogger(logger)
    level = to_log_level(level, default=_logging.NOTSET)
    filename = _pathlib.Path(filename).resolve()
    filename.parent.mkdir(exist_ok=True, parents=True)

    formatter = _logging.Formatter("%(asctime)s %(levelname)-1s %(message)s")
    handler = _logging.FileHandler(filename, encoding="utf-8")
    handler.setFormatter(formatter)
    handler.setLevel(level)
    logger.addHandler(handler)
    return handler


def log_exception_decorator[F: _collections_abc.Callable[..., _typing.Any]](
    func: F,
) -> F:
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            _LOGGER.exception("%s failed: %s", func.__name__, str(exc))
            raise

    return _typing.cast(F, wrapper)


def date_to_datetime(
    date: _datetime.date, *, tz: _datetime.tzinfo | None = None
) -> _datetime.datetime:
    """Convert *date* to an aware datetime.

    The returned :obj:`~datetime.datetime` is aware in time zone *tz*
    (or local time if *tz* is `None`) with the hours of the day chosen
    such that the date part of the returned datetime converted to UTC
    is *date*.

    >>> import datetime, zoneinfo
    >>> date = datetime.date(2025, 8, 15)
    >>> date_to_datetime(date).date() == date
    True
    >>> date_to_datetime(date).astimezone(datetime.timezone.utc).date() == date
    True

    >>> date_to_datetime(date, tz=zoneinfo.ZoneInfo("Etc/GMT+12"))
    datetime.datetime(2025, 8, 15, 6, 0, tzinfo=zoneinfo.ZoneInfo(key='Etc/GMT+12'))

    >>> date_to_datetime(date, tz=zoneinfo.ZoneInfo("America/Los_Angeles"))
    datetime.datetime(2025, 8, 15, 8, 0, tzinfo=zoneinfo.ZoneInfo(key='America/Los_Angeles'))

    >>> date_to_datetime(date, tz=datetime.timezone.utc)
    datetime.datetime(2025, 8, 15, 12, 0, tzinfo=datetime.timezone.utc)

    >>> date_to_datetime(date, tz=zoneinfo.ZoneInfo("Europe/Berlin"))
    datetime.datetime(2025, 8, 15, 13, 0, tzinfo=zoneinfo.ZoneInfo(key='Europe/Berlin'))

    >>> date_to_datetime(date, tz=zoneinfo.ZoneInfo("Asia/Calcutta"))
    datetime.datetime(2025, 8, 15, 14, 30, tzinfo=zoneinfo.ZoneInfo(key='Asia/Calcutta'))

    >>> date_to_datetime(date, tz=zoneinfo.ZoneInfo("Asia/Katmandu"))
    datetime.datetime(2025, 8, 15, 14, 45, tzinfo=zoneinfo.ZoneInfo(key='Asia/Katmandu'))

    >>> date_to_datetime(date, tz=zoneinfo.ZoneInfo("Asia/Tokyo"))
    datetime.datetime(2025, 8, 15, 16, 0, tzinfo=zoneinfo.ZoneInfo(key='Asia/Tokyo'))

    >>> date_to_datetime(date, tz=zoneinfo.ZoneInfo("Pacific/Kiritimati"))
    datetime.datetime(2025, 8, 15, 19, 0, tzinfo=zoneinfo.ZoneInfo(key='Pacific/Kiritimati'))
    """

    def to_hours(delta: datetime.timedelta) -> datetime.timedelta:
        return datetime.timedelta(hours=delta.total_seconds() // 3600)

    import datetime

    naive = datetime.datetime.combine(date, datetime.time())
    aware = naive.astimezone(tz=tz)
    utcoffset: datetime.timedelta = aware.utcoffset()  # type: ignore
    h24 = datetime.timedelta(hours=24)
    if utcoffset.total_seconds() >= 0:
        offset = utcoffset + to_hours((h24 - utcoffset) // 2)
    else:
        offset = to_hours((h24 + utcoffset) // 2)
    return (naive + offset).replace(tzinfo=aware.tzinfo)


@_typing.overload
def to_datetime_or_none(
    dt: _datetime.datetime | _datetime.date | str | float | int, /
) -> _datetime.datetime: ...


@_typing.overload
def to_datetime_or_none(dt: None, /) -> None: ...


def to_datetime_or_none(
    dt: _datetime.datetime | _datetime.date | str | float | int | None, /
) -> _datetime.datetime | None:
    """Return a datetime.

    >>> import datetime as dt, zoneinfo as zi

    >>> to_datetime_or_none(None) is None
    True
    >>> to_datetime_or_none("0")
    datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)

    """
    return None if dt is None else to_datetime(dt)


def to_datetime(
    dt: _datetime.datetime | _datetime.date | str | float | int | None,
    /,
    *,
    now: _datetime.datetime | None = None,
) -> _datetime.datetime:
    """Return a datetime.

    >>> import datetime as dt, zoneinfo as zi

    ..
       >>> import datetime
       >>> import datetime as _dt, zoneinfo as _zi
       >>> _tm = getfixture("time_machine")
       >>> _tm.move_to(_dt.datetime(2025, 8, 15, 10, 30, 27, tzinfo=_zi.ZoneInfo("Europe/Berlin")))

    >>> to_datetime("0")
    datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    >>> to_datetime(0)
    datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    >>> to_datetime(1755246627)
    datetime.datetime(2025, 8, 15, 8, 30, 27, tzinfo=datetime.timezone.utc)
    >>> to_datetime("2025-02-01")
    datetime.datetime(2025, 2, 1, 12, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=3600), 'CET'))
    >>> to_datetime("01.06.2025")
    datetime.datetime(2025, 6, 1, 13, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200), 'CEST'))

    ..
       >>> _tm.move_to(_dt.datetime(2025, 8, 15, 10, 30, 27, tzinfo=_zi.ZoneInfo("Europe/Berlin")))

    >>> to_datetime("NOW")
    datetime.datetime(2025, 8, 15, 10, 30, 27, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200), 'CEST'))
    >>> to_datetime("TODAY")
    datetime.datetime(2025, 8, 15, 13, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200), 'CEST'))

    ..
       >>> _tm.move_to(_dt.datetime(2025, 8, 15, 10, 30, 27, tzinfo=_zi.ZoneInfo("Europe/Berlin")))

    >>> to_datetime(None)
    datetime.datetime(2025, 8, 15, 10, 30, 27, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200), 'CEST'))

    >>> some_dt = dt.datetime(2025, 8, 15, 10, 30, 27, 1234, tzinfo=zi.ZoneInfo("Europe/Berlin"))
    >>> to_datetime(some_dt) == some_dt
    True

    If the local time zone is "America/Los_Angeles":

    ..
       >>> _tm.move_to(_dt.datetime(2025, 8, 15, 1, 30, 27, tzinfo=_zi.ZoneInfo(key='America/Los_Angeles')))

    >>> to_datetime("01.06.2025")
    datetime.datetime(2025, 6, 1, 8, 0, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=61200), 'PDT'))
    >>> to_datetime("01.06.2025").astimezone(datetime.timezone.utc)
    datetime.datetime(2025, 6, 1, 15, 0, tzinfo=datetime.timezone.utc)


    If the local time zone is "Asia/Tokyo":

    ..
       >>> _tm.move_to(_dt.datetime(2025, 8, 15, 1, 30, 27, tzinfo=_zi.ZoneInfo(key='Asia/Tokyo')))

    >>> to_datetime("01.06.2025")
    datetime.datetime(2025, 6, 1, 16, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=32400), 'JST'))
    >>> to_datetime("01.06.2025").astimezone(datetime.timezone.utc)
    datetime.datetime(2025, 6, 1, 7, 0, tzinfo=datetime.timezone.utc)


    If the local time zone is "Pacific/Kiritimati":

    ..
       >>> _tm.move_to(_dt.datetime(2025, 8, 15, 1, 30, 27, tzinfo=_zi.ZoneInfo(key='Pacific/Kiritimati')))

    >>> to_datetime("01.06.2025")
    datetime.datetime(2025, 6, 1, 19, 0, tzinfo=datetime.timezone(datetime.timedelta(seconds=50400), '+14'))
    >>> to_datetime("01.06.2025").astimezone(datetime.timezone.utc)
    datetime.datetime(2025, 6, 1, 5, 0, tzinfo=datetime.timezone.utc)

    """
    import datetime
    import re

    def normalize_tz(dt: datetime.datetime) -> datetime.datetime:
        if not dt.tzinfo:
            return dt.astimezone()
        else:
            return dt

    def parse_date(dt: str, pattern: str) -> datetime.datetime:
        """Return a aware datetime object for date *dt*.

        >>> dt = parse_date("01.01.2025", "%d.%m.%Y")
        >>> dt.tzinfo is not None
        >>> dt.date() == dt.astimezone(datetime.timezone.utc)
        """
        naive = datetime.datetime.strptime(dt, pattern)
        return date_to_datetime(naive)

    if dt is None:
        return datetime.datetime.now().astimezone() if now is None else now
    elif isinstance(dt, datetime.datetime):
        return normalize_tz(dt)
    elif isinstance(dt, datetime.date):
        return date_to_datetime(dt)
    elif isinstance(dt, (float, int)):
        return datetime.datetime.fromtimestamp(dt, tz=datetime.timezone.utc)
    elif isinstance(dt, str):
        if dt.upper() == "NOW":
            return datetime.datetime.now().astimezone()
        elif dt.upper() == "TODAY":
            return date_to_datetime(to_date_or_none("TODAY"))
        elif re.fullmatch("[0-9]+", dt):
            return datetime.datetime.fromtimestamp(int(dt), tz=datetime.timezone.utc)
        elif re.fullmatch("[0-9]+[.][0-9]+[.][0-9]+", dt):
            return parse_date(dt, "%d.%m.%Y")
        elif re.fullmatch("[0-9]+-[0-9]+-[0-9]+", dt):
            return parse_date(dt, "%Y-%m-%d")
        elif re.fullmatch("[0-9]+[.][0-9]+[.][0-9]+ [0-9]+:[0-9]+:[0-9]+", dt):
            return datetime.datetime.strptime(dt, "%d.%m.%Y %H:%M:%S").astimezone()
        else:
            try:
                iso_dt = datetime.datetime.fromisoformat(dt)
                return normalize_tz(iso_dt)
            except ValueError:
                raise ValueError(f"Unsupported datetime format: {dt!r}") from None
    else:
        raise ValueError(
            f"Cannot convert {type(dt).__qualname__!r} value {dt!r} to datetime"
        )


@_typing.overload
def to_date_or_none(date: _datetime.date | str, /) -> _datetime.date: ...


@_typing.overload
def to_date_or_none(date: None, /) -> None: ...


def to_date_or_none(date: _datetime.date | str | None, /) -> _datetime.date | None:
    """Return a date.

    Examples:

      >>> to_date_or_none(None) is None
      True
      >>> to_date_or_none("2025-06-01")
      datetime.date(2025, 6, 1)
      >>> to_date_or_none("01.06.2025")
      datetime.date(2025, 6, 1)
    """
    import datetime
    import re

    if date is None:
        return None
    elif isinstance(date, str):
        if date.upper() == "TODAY":
            return datetime.date.today()
        elif re.fullmatch("[0-9]+[.][0-9]+[.][0-9]+", date):
            return datetime.datetime.strptime(date, "%d.%m.%Y").date()
        else:
            try:
                return datetime.date.fromisoformat(date)
            except ValueError:
                raise ValueError(f"Unsupported date format: {date!r}") from None
    else:
        return date


def to_date(
    date: _datetime.date | str | None, /, *, today: _datetime.date | None = None
) -> _datetime.date:
    """Return a datetime date object for *date*.

    ..
       >>> import datetime
       >>> import datetime as _dt, zoneinfo as _zi
       >>> _tm = getfixture("time_machine")
       >>> _tm.move_to(_dt.datetime(2025, 8, 15, 10, 30, 27, tzinfo=_zi.ZoneInfo("Europe/Berlin")))

    Examples:

      >>> to_date(None)
      datetime.date(2025, 8, 15)
      >>> to_date("2025-06-01")
      datetime.date(2025, 6, 1)
      >>> to_date("01.06.2025")
      datetime.date(2025, 6, 1)
    """
    import datetime

    if date is None:
        return datetime.date.today() if today is None else today
    else:
        return to_date_or_none(date)


def compute_age(birthday: _datetime.date, today: _datetime.date) -> int:
    """Compute age.

    >>> from datetime import date
    >>> compute_age(date(1970, 1, 1), date(2024, 12, 31))
    54
    >>> compute_age(date(1970, 1, 1), date(2025, 1, 1))
    55
    >>> compute_age(date(1970, 1, 1), date(2025, 1, 2))
    55
    """
    birthday_passed = (today.month > birthday.month) or (
        (today.month == birthday.month) and (today.day >= birthday.day)
    )
    return (today.year - birthday.year) - (0 if birthday_passed else 1)


def format_cents_as_eur_de(
    cents: int,
    *,
    zero_cents: str = ",—",
    format: str | None = None,
    currency: str = "EUR",
) -> str:
    from babel.numbers import format_currency

    return format_currency(
        int(round(cents)) / 100,
        format=format,
        currency=currency,
        locale="de_DE",
    ).replace(",00", zero_cents)


def render_template(
    template: str,
    context: dict | None,
    *,
    extra_context: dict | None = None,
    extra_filters: dict[str, _typing.Callable] | None = None,
    trim_blocks: bool = False,
    lstrip_blocks: bool = False,
) -> str:
    """Render the Jinja2 *template* using *vars*.

    >>> render_template("Hello {{ name }}", dict(name="World"))
    'Hello World'

    >>> import datetime as dt, zoneinfo as zi
    >>> now = dt.datetime(2025, 8, 15, 10, 30, 27, 1234, tzinfo=zi.ZoneInfo("Europe/Berlin"))
    >>> render_template("Now is {{ now }}", {'now': now})
    'Now is 2025-08-15 10:30:27.001234+02:00'
    >>> render_template("Now is {{ now | isoformat }}", {'now': now})
    'Now is 2025-08-15 10:30:27+02:00'
    >>> render_template("Now is {{ now | isoformat('T') }}", {'now': now})
    'Now is 2025-08-15T10:30:27+02:00'
    >>> render_template("Now is {{ now | isoformat(' ', 'hours') }}", {'now': now})
    'Now is 2025-08-15 10+02:00'
    >>> render_template("Now is {{ now | isoformat(' ', 'milliseconds') }}", {'now': now})
    'Now is 2025-08-15 10:30:27.001+02:00'
    >>> render_template("Now is {{ now | strftime('%Y%m%d-%H%M%S') }}", {'now': now})
    'Now is 20250815-103027'

    Jinja2 builtin filters: https://jinja.palletsprojects.com/en/stable/templates/#builtin-filters

    Available filters:

    * ``strftime`` - calls :obj:`datetime.datetime.strftime`
    * ``isoformat`` - calls :obj:`datetime.datetime.isoformat`
    * ``to_ext`` - adds leading dot (``.``) if non-empty

    """
    import jinja2 as _jinja2

    def _strftime(dt: _datetime.datetime, format="%Y-%m-%d %H:%M:%S") -> str:
        return dt.strftime(format)

    def _isoformat(dt: _datetime.datetime, sep=" ", timespec="seconds") -> str:
        return dt.isoformat(sep=sep, timespec=timespec)

    jinja_env = _jinja2.Environment(
        undefined=_jinja2.StrictUndefined,
        trim_blocks=trim_blocks,
        lstrip_blocks=lstrip_blocks,
    )
    jinja_env.filters.update(
        {
            "strftime": _strftime,
            "isoformat": _isoformat,
            "to_ext": (lambda s: f".{s}" if s else ""),
            "format_cents_as_eur_de": format_cents_as_eur_de,
            "date_de": (lambda s: to_date(s).strftime("%d.%m.%Y")),
            "month_de": to_month_de,
            "month_year_de": to_month_year_de,
            "raise_runtime_error": _raise_runtime_error,
            "log_info": (lambda s: _LOGGER.info("%s", s)),
            "tee_log_info": (lambda s: _tee_log(_logging.INFO, s)),
            "format_iban": format_iban,
        }
    )
    jinja_env.filters.update(extra_filters or {})
    jinja_template = jinja_env.from_string(template)
    context = (context or {}).copy()
    context.update(extra_context or {})
    out = jinja_template.render(context)
    return out


def _tee_log(level, obj):
    _LOGGER.log(level, "%s", str(obj))
    return obj


_MONTH_NAME_DE = {
    1: "Januar",
    2: "Februar",
    3: "März",
    4: "April",
    5: "Mai",
    6: "Juni",
    7: "Juli",
    8: "August",
    9: "September",
    10: "Oktober",
    11: "November",
    12: "Dezember",
}

_MONTH_SHORT_NAME_DE = {
    1: "Jan",
    2: "Feb",
    3: "Mär",
    4: "Apr",
    5: "Mai",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Okt",
    11: "Nov",
    12: "Dez",
}


def to_month_de(month: _datetime.datetime | _datetime.date | str | int):
    if not isinstance(month, int):
        month = to_date(month).month
    return _MONTH_NAME_DE[month]


def to_short_month_de(month: _datetime.datetime | _datetime.date | str | int):
    if not isinstance(month, int):
        month = to_date(month).month
    return _MONTH_SHORT_NAME_DE[month]


def to_year_month(
    ym: _datetime.datetime | _datetime.date | str | tuple | list, /
) -> tuple[int, int]:
    if not isinstance(ym, (tuple, list)):
        date = to_date(ym)
        return (date.year, date.month)
    else:
        year, month = ym
        return (year, month)


def to_month_year_de(
    year_month: _datetime.datetime | _datetime.date | str | tuple | list,
) -> str:
    year, month = to_year_month(year_month)
    return f"{to_month_de(month)} {year}"


def to_short_month_year_de(
    year_month: _datetime.datetime | _datetime.date | str | tuple | list,
) -> str:
    """Return a short German month year.

    >>> to_short_month_year_de("2026-01-05")
    'Jan 26'
    """
    year, month = to_year_month(year_month)
    short_month_de = to_short_month_de(month)
    short_year = str(year)[2:]
    return f"{short_month_de} {short_year}"


def _raise_runtime_error(s):
    raise RuntimeError(s)


def format_iban(iban: str, /) -> str:
    def mask(s: str, left: int, right: int, expected_length: int | None = None) -> str:
        if expected_length is not None and len(s) != expected_length:
            return s
        mid_len = len(s) - left - right
        if mid_len <= 0:
            return s
        else:
            lft = s[:left]
            rgt = s[-right:]
            return lft + ("*" * mid_len) + rgt

    iban = str(iban or "").upper().strip().replace(" ", "")
    match iban[:2]:
        case "AT":
            return mask(iban, 4, 4, expected_length=20)
        case "CH":
            return mask(iban, 4, 4, expected_length=21)
        case "DE":
            return mask(iban, 4, 4, expected_length=22)
        case "NL":
            return mask(iban, 5, 4, expected_length=18)
        case "IT":
            return mask(iban, 5, 4, expected_length=27)
        case _:
            return iban


def coalesce(*args: _T | None) -> _T | None:
    for arg in args:
        if arg is not None:
            return arg
    else:
        return None


def coalesce_missing(*args: _T | _types.MissingType) -> _T | None:
    for arg in args:
        if not isinstance(arg, _types.MissingType):
            return arg
    else:
        return None


def slurp(
    path: _pathlib.Path | str, encoding: str = "utf-8", newline: str | None = None
) -> str:
    with open(path, encoding=encoding, newline=newline) as f:
        return f.read()


def dedup_iter(iterable):
    """Deduplicate *iterable*."""
    memo = set()
    for item in iterable:
        if item not in memo:
            yield item
        memo.add(item)


def dedup(iterable):
    """Deduplicate *iterable*."""
    return list(dedup_iter(iterable))


def is_nan_or_none(obj) -> bool:
    return bool(obj is None or (isinstance(obj, float) and _math.isnan(obj)))


def nan_to_none(obj: _T) -> _T | None:
    if isinstance(obj, float) and _math.isnan(obj):
        return None
    else:
        return obj


@_typing.overload
def to_int_list_or_none(int_or_list: None, /) -> None: ...


@_typing.overload
def to_int_list_or_none(
    int_or_list: int | str | _collections_abc.Iterable[int | str], /
) -> list[int]: ...


def to_int_list_or_none(int_or_list, /) -> list[int] | None:
    if int_or_list is None:
        return None
    if isinstance(int_or_list, (int, str)):
        int_or_list = [int_or_list]
    return [int(x) for x in int_or_list if x]


@_typing.overload
def to_str_list_or_none(str_or_list: None, /) -> None: ...


@_typing.overload
def to_str_list_or_none(
    str_or_list: str | _collections_abc.Iterable[str], /
) -> list[str]: ...


def to_str_list_or_none(str_or_list, /) -> list[str] | None:
    if str_or_list is None:
        return None
    if isinstance(str_or_list, str):
        str_or_list = [str_or_list]
    return [str(x) for x in str_or_list if x]


def to_str_list(*args: str | _collections_abc.Iterable[str] | None) -> list[str]:
    result = []
    for arg in args:
        if arg is not None:
            result.extend(to_str_list_or_none(arg))
    return result


@_typing.overload
def to_str_set_or_none(str_or_iter: None, /) -> None: ...


@_typing.overload
def to_str_set_or_none(
    str_or_iter: str | _collections_abc.Iterable[str], /
) -> set[str]: ...


def to_str_set_or_none(str_or_iter, /) -> set[str] | None:
    if str_or_iter is None:
        return None
    if isinstance(str_or_iter, str):
        str_or_iter = [str_or_iter]
    return set(str(x) for x in str_or_iter if x)


def to_str_set(*args: str | _collections_abc.Iterable[str] | None) -> set[str]:
    result = set()
    for arg in args:
        if arg is not None:
            result.update(to_str_set_or_none(arg))
    return result


@_typing.overload
def to_int_or_none(obj: int) -> int: ...


@_typing.overload
def to_int_or_none(obj: object) -> int | None: ...


def to_int_or_none(obj):
    try:
        return int(obj)
    except Exception:
        return None


def to_yaml_str(
    obj: dict | list, *, width: int = 200, explicit_start: bool = False
) -> str:
    import io

    import ruamel.yaml as _ruamel_yaml
    from ruamel.yaml.scalarstring import LiteralScalarString as lss

    def maybe_to_lss(s: str) -> str | lss:
        return lss(s) if "\n" in s else s

    if isinstance(obj, dict):
        obj = {k: maybe_to_lss(v) if isinstance(v, str) else v for k, v in obj.items()}

    yaml = _ruamel_yaml.YAML(typ="rt")
    yaml.width = width
    yaml.explicit_start = explicit_start
    with io.StringIO() as str_io:
        yaml.dump(obj, stream=str_io)
        return str_io.getvalue()


@_typing.overload
def merge_mail_addresses(*args, default: list[str]) -> list[str]: ...
@_typing.overload
def merge_mail_addresses(*args, default: None = None) -> list[str] | None: ...
def merge_mail_addresses(*args, default: list[str] | None = None) -> list[str] | None:
    if all(arg is None for arg in args):
        return default
    addrs = []
    for arg in args:
        addrs.extend(to_str_list_or_none(arg) or [])
    addrs = list(dedup(addrs))
    return addrs or default


def get_default_email_policy() -> _email_policy.EmailPolicy:
    import email.policy

    policy = email.policy.SMTP.clone(
        raise_on_defect=True, cte_type="7bit", verify_generated_headers=True
    )
    return policy  # type: ignore


# ==============================================================================
# WSJRDP
# ==============================================================================


def sepa_mandate_id_from_hitobito_id(hitobito_id: str | int) -> str:
    return f"wsjrdp2027{hitobito_id}"


# ==============================================================================
# SQL
# ==============================================================================

_LIKE_OPS = frozenset(
    [
        "LIKE",
        "ILIKE",
        "NOT LIKE",
        "NOT ILIKE",
        "SIMILAR TO",
        "NOT SIMILAR TO",
        "~",
        "~*",
        "!~",
        "!~*",
    ]
)

_OP_NEG = {
    None: None,
    "=": "<>",
    "<>": "=",
    ">=": "<",
    ">": "<=",
    "<=": ">",
    "<": ">=",
    "~": "!~",
    "!~": "~",
    "~*": "!~*",
    "!~*": "~*",
    "LIKE": "NOT LIKE",
    "ILIKE": "NOT ILIKE",
    "NOT LIKE": "LIKE",
    "NOT ILIKE": "ILIKE",
    "SIMILAR TO": "NOT SIMILAR TO",
    "NOT SIMILAR TO": "SIMILAR TO",
}


def combine_where(where: str, *exprs: str, op="AND") -> str:
    for expr in exprs:
        where = f"{where}\n    {op} {expr}" if where else expr
    return where


def sql_literal(x):
    if isinstance(x, bool):
        return "TRUE" if x else "FALSE"
    elif isinstance(x, _types.NullOrNotType):
        return x.sql_literal
    elif isinstance(x, (int, float)):
        return repr(x)
    else:
        x_escaped = x.replace("'", "''")
        return f"'{x_escaped}'"


@_typing.overload
def negate_sql_comparison_op(op: None, /) -> None: ...
@_typing.overload
def negate_sql_comparison_op(op: str, /) -> str: ...
def negate_sql_comparison_op(op: str | None, /) -> str | None:
    import re

    if op is None:
        return None
    op = re.sub(r"\s+", " ", op.strip().upper())
    return _OP_NEG[op]


def all_in_array_expr(
    array, *vals, op="=", join_op="AND", array_comp_func="ANY"
) -> str:
    if not vals:
        return ""
    if op in _LIKE_OPS:
        if array_comp_func.upper() in ("ANY", "SOME"):
            val_str_list = [
                f"EXISTS(WITH t AS (SELECT UNNEST({array}) AS r) SELECT FROM t WHERE r {op} {sql_literal(val)})"
                for val in vals
            ]
        elif array_comp_func.upper() == "ALL":
            neg_op = negate_sql_comparison_op(op)
            val_str_list = [
                f"NOT EXISTS(WITH t AS (SELECT UNNEST({array}) AS r) SELECT FROM t WHERE r {neg_op} {sql_literal(val)})"
                for val in vals
            ]
        else:
            raise RuntimeError(
                f"No support for '{array_comp_func}' with LIKE-like op '{op}'"
            )
    else:
        val_str_list = [
            f"{sql_literal(val)} {op} {array_comp_func}({array})" for val in vals
        ]
    if len(val_str_list) == 0:
        return ""
    elif len(val_str_list) == 1:
        return val_str_list[0]
    else:
        return "(" + f" {join_op} ".join(val_str_list) + ")"


def in_expr(expr, elts) -> str:
    """

    ..
       >>> from ._types import NULL, NOT_NULL

    >>> in_expr("x", [1, 2])
    'x IN (1, 2)'
    >>> in_expr("x", [1])
    'x = 1'
    >>> in_expr("x", [NOT_NULL])
    'x IS NOT NULL'
    >>> in_expr("x", [NULL])
    'x IS NULL'
    >>> in_expr("x", [None])
    'x IS NULL'
    >>> in_expr("x", [])
    'FALSE'
    >>> in_expr("x", [1, 2, None])
    '(x IN (1, 2) OR x IS NULL)'
    >>> in_expr("x", None)
    'FALSE'
    """

    if elts is None:
        return "FALSE"
    elif isinstance(elts, (int, float, str, bool)):
        elts = [elts]
    if not elts:
        return "FALSE"
    elif any(x in (_types.NULL, None) for x in elts):
        elts_wo_none = [x for x in elts if x not in (_types.NULL, None)]
        if not elts_wo_none:
            return f"{expr} IS NULL"
        else:
            return f"({in_expr(expr, elts_wo_none)} OR {expr} IS NULL)"
    elif any(x is _types.NOT_NULL for x in elts):
        elts_wo_not_null = [x for x in elts if x is not _types.NOT_NULL]
        if not elts_wo_not_null:
            return f"{expr} IS NOT NULL"
        else:
            return f"({in_expr(expr, elts_wo_not_null)} OR {expr} IS NOT NULL)"
    else:
        if len(elts) == 1:
            return f"{expr} = {sql_literal(elts[0])}"
        else:
            elts_list_str = ", ".join(sql_literal(x) for x in elts)
            return f"{expr} IN ({elts_list_str})"


def not_in_expr(expr, elts) -> str:
    """

    ..
       >>> from ._types import NULL, NOT_NULL

    >>> not_in_expr("x", [1, 2])
    'x NOT IN (1, 2)'
    >>> not_in_expr("x", [1])
    'x <> 1'
    >>> not_in_expr("x", [])
    'TRUE'
    >>> not_in_expr("x", [None])
    'x IS NOT NULL'
    >>> not_in_expr("x", [NULL])
    'x IS NOT NULL'
    >>> not_in_expr("x", [NOT_NULL])
    'x IS NULL'
    >>> not_in_expr("x", [1, 2, None])
    '(x NOT IN (1, 2) AND x IS NOT NULL)'
    >>> not_in_expr("x", [1, 2, NOT_NULL])
    '(x NOT IN (1, 2) AND x IS NULL)'
    >>> not_in_expr("x", None)
    'TRUE'
    """

    if elts is None:
        return "TRUE"
    elif isinstance(elts, (int, float, str, bool)):
        elts = [elts]
    if not elts:
        return "TRUE"
    elif any(x in (_types.NULL, None) for x in elts):
        elts_wo_none = [x for x in elts if x not in (_types.NULL, None)]
        if not elts_wo_none:
            return f"{expr} IS NOT NULL"
        else:
            return f"({not_in_expr(expr, elts_wo_none)} AND {expr} IS NOT NULL)"
    elif any(x is _types.NOT_NULL for x in elts):
        elts_wo_not_null = [x for x in elts if x is not _types.NOT_NULL]
        if not elts_wo_not_null:
            return f"{expr} IS NULL"
        else:
            return f"({not_in_expr(expr, elts_wo_not_null)} AND {expr} IS NULL)"
    else:
        if len(elts) == 1:
            return f"{expr} <> {sql_literal(elts[0])}"
        else:
            elts_list_str = ", ".join(sql_literal(x) for x in elts)
            return f"{expr} NOT IN ({elts_list_str})"


def dataframe_copy_for_xlsx(df: _pandas.DataFrame) -> _pandas.DataFrame:
    df = df.copy()

    # Excel does not support timestamps with timezones, so we remove
    # them here.
    df = df.copy()
    datetime_cols = df.select_dtypes(include=["datetimetz"]).columns  # ty: ignore
    for col in datetime_cols:
        df[col] = df[col].dt.tz_localize(None)

    def str_or_repr(obj):
        obj_str = str(obj)
        obj_str_repr = repr(obj_str)
        require_repr = (
            (len(obj_str_repr) > len(obj_str) + 2)  #
            or "," in obj_str
            or obj_str.strip() != obj_str
        )
        return repr(obj) if require_repr else obj_str

    def to_excel_val(obj):
        if isinstance(obj, list):
            return ", ".join(str_or_repr(x) for x in obj)
        else:
            return obj

    for col in df.columns:
        df[col] = df[col].map(to_excel_val)
    return df


# ==============================================================================
# Dataframes
# ==============================================================================


def write_dataframe_to_xlsx(
    df: _pandas.DataFrame,
    path: str | _pathlib.Path,
    *,
    add_autofilter: bool = True,
    columns: _typing.Sequence[_typing.Hashable] | None = None,
    float_format: str | None = None,
    header: _typing.Sequence[_typing.Hashable] | bool = True,
    index: bool = False,
    merge_cells: bool = True,
    na_rep: str = "",
    sheet_name: str = "Sheet 1",
    log_level: int | None = None,
    drop_columns: _collections_abc.Iterable[str] | None = None,
) -> None:
    import pandas as pd

    from . import _util

    df = _util.dataframe_copy_for_xlsx(df)
    if drop_columns is not None:
        df.drop(columns=list(drop_columns), inplace=True)

    if log_level is None:
        log_level = _logging.INFO
    _LOGGER.log(log_level, "Write %s", path)
    writer = pd.ExcelWriter(
        path, engine="xlsxwriter", engine_kwargs={"options": {"remove_timezone": True}}
    )
    df.to_excel(
        writer,
        engine="xlsxwriter",
        columns=columns,  # type: ignore
        float_format=float_format,
        header=header,  # type: ignore
        index=index,
        merge_cells=merge_cells,
        na_rep=na_rep,
        sheet_name=sheet_name,
    )
    (max_row, max_col) = df.shape

    # workbook: xlsxwriter.Workbook = writer.book  # type: ignore
    worksheet = writer.sheets[sheet_name]
    worksheet.freeze_panes(1, 0)
    if add_autofilter:
        worksheet.autofilter(0, 0, max_row, max_col - 1)
    worksheet.autofit()

    writer.close()


def print_progress_message(count, size, message, *, logger=_LOGGER) -> None:
    import sys

    count_p_1 = count + 1
    pcnt = f"({count_p_1 / size * 100.0:.1f}%)"
    prefix = f"{count_p_1:>4}/{size} {pcnt:>8} "
    message = prefix + message
    logger.debug(message)
    if count < 15 or (size - count < 5):
        print(f"  | {message}", file=sys.stderr, flush=True)
    elif count == 15:
        print(f"  | ...", file=sys.stderr, flush=True)


# ==============================================================================
# Accounts
# ==============================================================================


def generate_password():
    import secrets
    import string

    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for i in range(20))


def generate_mail_username(firstname, lastname):
    import re
    import unicodedata

    # _LOGGER.info("Generated username from: %s, %s", str(firstname), str(lastname))
    firstname = firstname.split(" ")[0]
    username = firstname + "." + lastname
    username = username.lower()
    username = (
        username.replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    username = unicodedata.normalize("NFKD", username)
    username = "".join(c for c in username if not unicodedata.combining(c))
    username = re.sub(r"[^a-z0-9._-]+", "-", username)
    username = re.sub(r"[_-]+", "-", username)
    username = username.strip(".").strip("-")

    if len(username) > 64:
        # try shrinking last username part
        parts = username.split(".", 1)
        first = parts[0][:30]  # keep some of first
        last = parts[1][: max(1, 63 - len(first) - 1)]
        username = f"{first}.{last}".strip(".")

    # _LOGGER.info("Generated username: %s", username)
    return username
