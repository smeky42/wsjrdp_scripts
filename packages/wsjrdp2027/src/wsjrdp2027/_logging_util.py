from __future__ import annotations

import logging as _logging


_BASE_LOGGER = _logging.getLogger(__name__.rsplit(".", 1)[0])


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


class NullLoggerAdapter(_logging.LoggerAdapter):
    def __init__(self):
        super().__init__(_logging.getLogger())

    def process(self, msg, kwargs):
        return msg, kwargs

    def log(self, level, msg, *args, **kwargs) -> None:
        pass

    def _log(self, level, msg, args, **kwargs) -> None:
        pass


def to_logger_or_adapter(
    logger: _logging.Logger | _logging.LoggerAdapter | bool,
    *,
    prefix: str | None = None,
) -> _logging.Logger | _logging.LoggerAdapter:
    if isinstance(logger, bool):
        if logger:
            if prefix:
                return PrefixLoggerAdapter(_BASE_LOGGER, prefix=prefix)
            else:
                return _BASE_LOGGER
        else:
            return NullLoggerAdapter()
    else:
        return logger
