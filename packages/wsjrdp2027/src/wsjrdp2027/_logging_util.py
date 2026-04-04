from __future__ import annotations

import logging as _logging


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
