from __future__ import annotations

import contextlib as _contextlib
import dataclasses as _dataclasses
import logging as _logging
import pathlib as _pathlib
import typing as _typing

import sshtunnel as _sshtunnel


@_dataclasses.dataclass(kw_only=True, frozen=True)
class SSHTunnelConfig:
    host: str
    port: int
    username: str
    private_key_path: _pathlib.Path | str
    remote_bind_address: tuple[str, int] | None = None


class SSHTunnel:
    config: SSHTunnelConfig
    _forwarder: _sshtunnel.SSHTunnelForwarder | None = None

    def __init__(
        self,
        config: SSHTunnelConfig,
        *,
        logger: _logging.Logger | _logging.LoggerAdapter | bool = True,
    ) -> None:
        from . import _logging_util

        self._logger = _logging_util.to_logger_or_adapter(
            logger, prefix=f"SSHTunnel-{id(self)}"
        )
        self.config = config
        self._forwarder = self.__create_ssh_forwarder()
        self._forwarder.__enter__()

    def close(self) -> None:
        self.__exit__(None, None, None)

    @property
    def local_bind_host(self) -> str:
        if (forwarder := self._forwarder) is None:
            return ""
        else:
            return forwarder.local_bind_host

    @property
    def local_bind_port(self) -> int:
        if (forwarder := self._forwarder) is None:
            return 0
        else:
            return forwarder.local_bind_port

    def __enter__(self) -> _typing.Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        forwarder, self._forwarder = self._forwarder, None
        if forwarder is not None:
            forwarder.__exit__(exc_type, exc_val, exc_tb)

    def __create_ssh_forwarder(self) -> _sshtunnel.SSHTunnelForwarder:
        import logging

        import sshtunnel

        logger = logging.getLogger(f"{self._logger.name}.ssh_tunnel_logger")
        logger.propagate = False
        logger.setLevel(logging.ERROR)

        forwarder = sshtunnel.SSHTunnelForwarder(
            (self.config.host, self.config.port),
            ssh_username=self.config.username,
            ssh_pkey=str(self.config.private_key_path),
            local_bind_address=("127.0.0.1", 0),
            remote_bind_address=self.config.remote_bind_address,
            logger=logger,
        )
        return forwarder

    def __str__(self) -> str:
        if (forwarder := self._forwarder) is None:
            return "Closed SSH-Tunnel"
        else:
            msg = f"""
\thost: {forwarder.ssh_host}
\tport: {forwarder.ssh_port}
\tusername: {forwarder.ssh_username}
\tlocal_binds: {forwarder._local_binds}
\tremote_binds: {forwarder._remote_binds}"""
            with _contextlib.suppress(Exception):
                msg += f"\n\tlocal_bind_host: {forwarder.local_bind_host}"
                msg += f"\n\tlocal_bind_port: {forwarder.local_bind_port}"
            return msg.strip("\n ")
