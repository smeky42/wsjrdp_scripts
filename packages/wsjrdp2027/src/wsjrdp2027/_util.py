from __future__ import annotations

import collections.abc as _collections_abc
import logging as _logging
import typing as _typing


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
    "format_cents_as_eur_de",
    "get_default_email_policy",
    "in_expr",
    "merge_mail_addresses",
    "render_template",
    "to_date_or_none",
    "to_datetime",
    "to_datetime_or_none",
    "to_int_list",
    "to_int_or_none",
    "to_log_level",
    "to_str_list",
]

_T = _typing.TypeVar("_T")


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
    utcoffset = aware.utcoffset()
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
    dt: _datetime.datetime | _datetime.date | str | float | int | None, /
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
        return datetime.datetime.now().astimezone()
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


def to_date(date: _datetime.date | str | None, /) -> _datetime.date:
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

    return datetime.date.today() if date is None else to_date_or_none(date)


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


def format_cents_as_eur_de(cents: int, zero_cents: str = ",—") -> str:
    from babel.numbers import format_currency

    return format_currency(int(round(cents)) / 100, "EUR", locale="de_DE").replace(
        ",00", zero_cents
    )


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
    jinja_env.filters["strftime"] = _strftime
    jinja_env.filters["isoformat"] = _isoformat
    jinja_env.filters["to_ext"] = lambda s: f".{s}" if s else ""
    jinja_env.filters.update(extra_filters or {})
    jinja_template = jinja_env.from_string(template)
    context = (context or {}).copy()
    context.update(extra_context or {})
    out = jinja_template.render(context)
    return out


def dedup(iterable):
    """Deduplicate *iterable*."""
    memo = set()
    for item in iterable:
        if item not in memo:
            yield item
        memo.add(item)


@_typing.overload
def to_int_list(int_or_list: None) -> None: ...


@_typing.overload
def to_int_list(
    int_or_list: int | str | _collections_abc.Iterable[int | str],
) -> list[int]: ...


def to_int_list(int_or_list) -> list[int] | None:
    if int_or_list is None:
        return None
    if isinstance(int_or_list, (int, str)):
        int_or_list = [int_or_list]
    return [int(x) for x in int_or_list if x]


@_typing.overload
def to_str_list(str_or_list: None) -> None: ...


@_typing.overload
def to_str_list(str_or_list: str | _collections_abc.Iterable[str]) -> list[str]: ...


def to_str_list(str_or_list) -> list[str] | None:
    if str_or_list is None:
        return None
    if isinstance(str_or_list, str):
        str_or_list = [str_or_list]
    return [str(x) for x in str_or_list if x]


@_typing.overload
def to_int_or_none(obj: int) -> int: ...


@_typing.overload
def to_int_or_none(obj: object) -> int | None: ...


def to_int_or_none(obj):
    try:
        return int(obj)
    except Exception:
        return None


def merge_mail_addresses(*args) -> list[str] | None:
    if all(arg is None for arg in args):
        return None
    addrs = []
    for arg in args:
        addrs.extend(to_str_list(arg) or [])
    addrs = list(dedup(addrs))
    return addrs or None


def get_default_email_policy() -> _email_policy.EmailPolicy:
    import email.policy

    policy = email.policy.SMTP.clone(
        raise_on_defect=True, cte_type="7bit", verify_generated_headers=True
    )
    return policy  # type: ignore


# ==============================================================================
# SQL
# ==============================================================================


def combine_where(where: str, expr: str) -> str:
    return f"{where}\n    AND {expr}" if where else expr


def in_expr(expr, elts) -> str:
    """

    >>> in_expr("x", [1, 2])
    'x IN (1, 2)'
    >>> in_expr("x", [1])
    'x = 1'
    >>> in_expr("x", [])
    'FALSE'
    >>> in_expr("x", [1, 2, None])
    '(x IN (1, 2) OR x IS NULL)'
    >>> in_expr("x", None)
    'FALSE'
    """

    def sql_repr(x):
        if isinstance(x, (int, float)):
            return repr(x)
        else:
            return f"'{x}'"

    if elts is None:
        return "FALSE"
    elif isinstance(elts, (int, float, str)):
        elts = [elts]
    if not elts:
        return "FALSE"
    elif any(x is None for x in elts):
        elts_wo_none = [x for x in elts if x is not None]
        if not elts_wo_none:
            return f"{expr} IS NULL"
        else:
            return f"({in_expr(expr, elts_wo_none)} OR {expr} IS NULL)"
    else:
        if len(elts) == 1:
            return f"{expr} = {sql_repr(elts[0])}"
        else:
            elts_list_str = ", ".join(sql_repr(x) for x in elts)
            return f"{expr} IN ({elts_list_str})"


def dataframe_copy_for_xlsx(df: _pandas.DataFrame) -> _pandas.DataFrame:
    df = df.copy()

    # Excel does not support timestamps with timezones, so we remove
    # them here.
    df = df.copy()
    datetime_cols = df.select_dtypes(include=["datetimetz"]).columns
    for col in datetime_cols:
        print(col)
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
) -> None:
    import pandas as pd

    from . import _util

    df = _util.dataframe_copy_for_xlsx(df)

    if log_level is None:
        log_level = _logging.INFO
    _LOGGER.log(log_level, "Write %s", path)
    writer = pd.ExcelWriter(
        path, engine="xlsxwriter", engine_kwargs={"options": {"remove_timezone": True}}
    )
    df.to_excel(
        writer,
        engine="xlsxwriter",
        columns=columns,
        float_format=float_format,
        header=header,
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
