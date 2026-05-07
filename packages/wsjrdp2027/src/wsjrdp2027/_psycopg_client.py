from __future__ import annotations

import contextlib as _contextlib
import dataclasses as _dataclasses
import logging as _logging
import typing as _typing


if _typing.TYPE_CHECKING:
    import psycopg as _psycopg
    import psycopg.rows as _psycopg_rows
    import sshtunnel as _sshtunnel


_Row = _typing.TypeVar("_Row", covariant=True, default="_psycopg_rows.TupleRow")
_CursorRow = _typing.TypeVar("_CursorRow")


@_dataclasses.dataclass
class PsycopgConfig:
    host: str
    port: int
    user: str
    password: str
    dbname: str
    autocommit: bool = False
    read_only: bool = False
    ssh_forwarder: _sshtunnel.SSHTunnelForwarder | None = None


class PsycopgAudithook(_typing.Protocol):
    def __call__(self, action, /, **kwargs) -> object: ...


class PsycopgClient:
    _config: PsycopgConfig
    _dry_run: bool = False
    __exit_stack: _contextlib.ExitStack
    __connection: _psycopg.Connection | None = None
    __is_closed: bool = False
    _audithook: PsycopgAudithook | None = None

    def __init__(
        self,
        config: PsycopgConfig,
        *,
        dry_run: bool | None = None,
        logger: _logging.Logger | _logging.LoggerAdapter | bool = True,
        audithook: PsycopgAudithook | None = None,
    ) -> None:
        from . import _logging_util

        self._logger = _logging_util.to_logger_or_adapter(
            logger, prefix=f"Psycopg-{id(self)}"
        )
        self._config = config
        self._dry_run = bool(dry_run)
        self._audithook = audithook
        self.__exit_stack = _contextlib.ExitStack()

    @property
    def config(self) -> PsycopgConfig:
        return self._config

    def close(self) -> None:
        if (connection := self.__connection) is not None:
            connection.close()
            self.__connection = None
        self.__is_closed = True

    @property
    def closed(self) -> bool:
        """`True` if the underlying HTTP session is already closed."""
        return self.__is_closed

    @property
    def adapters(self):
        conn = self.__connection
        if conn:
            return conn.adapters
        else:
            from psycopg.adapt import AdaptersMap

            return AdaptersMap()

    @property
    def connection(self) -> _psycopg.Connection | None:
        return self.__connection

    def get_connection(self) -> _psycopg.Connection:
        if self._audithook:
            self._audithook("get connection")
        return self._get_connection()

    def __enter__(self) -> _typing.Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__exit_stack.__exit__(exc_type, exc_val, exc_tb)

    @_typing.overload
    def cursor(self, *, binary: bool = False) -> _psycopg.Cursor[_Row]: ...
    @_typing.overload
    def cursor(
        self, *, binary: bool = False, row_factory: _psycopg_rows.RowFactory[_CursorRow]
    ) -> _psycopg.Cursor[_CursorRow]: ...
    def cursor(
        self,
        *,
        binary: bool = False,
        row_factory: _typing.Any = None,
    ):
        conn = self._get_connection()
        if self._audithook:
            self._audithook("create cursor")
        return conn.cursor(row_factory=row_factory, binary=binary)

    def _get_connection(self) -> _psycopg.Connection:
        if self.__connection is None:
            if self.__is_closed:
                raise RuntimeError("PsycopgClient already closed")
            self.__connection = conn = self.__create_connection()
            self.__exit_stack.enter_context(conn)
        return self.__connection

    def __create_connection(self) -> _psycopg.Connection:
        import psycopg
        import psycopg.types
        import psycopg.types.hstore

        from . import _logging_util

        conn = psycopg.connect(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            password=self._config.password,
            dbname=self._config.dbname,
            autocommit=self._config.autocommit,
        )
        logger = _logging_util.PrefixLoggerAdapter(
            self._logger, prefix=f"{self.__class__.__qualname__}.__create_connection: "
        )
        if self._config.read_only:
            if conn.autocommit:
                conn.execute("SET default_transaction_read_only TO TRUE;")
                logger.info("SET default_transaction_read_only TO TRUE;")
            else:
                conn.set_read_only(True)
                logger.info("Set connection to be READ ONLY")
        hstore_info = psycopg.types.TypeInfo.fetch(conn, "hstore")
        if hstore_info is not None:
            logger.debug(f"hstore_info: {hstore_info}")
            logger.debug(f"Register HSTORE type info")
            psycopg.types.hstore.register_hstore(hstore_info, conn)
        else:
            logger.debug(f"No HSTORE type info found => HSTORE not registered")
        return conn
