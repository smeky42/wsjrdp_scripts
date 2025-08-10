from __future__ import annotations

import contextlib as _contextlib
import logging as _logging
import pathlib as _pathlib
import typing as _typing

from . import _config

if _typing.TYPE_CHECKING:
    import smtplib as _smtplib

    import psycopg2 as _psycopg2
    import sshtunnel as _sshtunnel


_LOGGER = _logging.getLogger(__name__)


class ConnectionContext:
    config: _config.Config

    def __init__(
        self,
        *,
        config: _config.Config | None = None,
        config_path: str | _pathlib.Path | None = None,
        log_level: int | str | None = None,
    ) -> None:
        # Default basic logging config
        if log_level is None:
            log_level = _logging.INFO
        elif isinstance(log_level, str):
            log_level = _logging.getLevelNamesMapping()[log_level]
        _logging.basicConfig(
            level=log_level,
            format="%(asctime)s %(levelname)-1s %(message)s",
        )

        # wsjrdp_scripts config
        if config is None:
            if config_path is not None:
                raise ValueError("Only one of 'config' and 'config_path' is allowed.")
            config = _config.Config.from_file(config_path)
        self.config = config

    def __create_ssh_forwarder(
        self, *, remote_bind_address: tuple[str, int]
    ) -> _sshtunnel.SSHTunnelForwarder:
        import sshtunnel

        return sshtunnel.SSHTunnelForwarder(
            (self.config.ssh_host, self.config.ssh_port),
            ssh_username=self.config.ssh_username,
            ssh_pkey=self.config.ssh_private_key,
            remote_bind_address=remote_bind_address,
        )

    def __ssh_forwarder_to_str(self, forwarder: _sshtunnel.SSHTunnelForwarder) -> str:
        return f"""
\thost: {forwarder.ssh_host}
\tport: {forwarder.ssh_port}
\tusername: {forwarder.ssh_username}
\tlocal_binds: {forwarder._local_binds}
\tremote_binds: {forwarder._remote_binds}
""".strip("\n ")

    @_contextlib.contextmanager
    def psycopg2_connect(self) -> _typing.Iterator[_psycopg2.connection]:
        import contextlib

        import psycopg2

        with contextlib.ExitStack() as exit_stack:
            if self.config.use_ssh_tunnel:
                forwarder = self.__create_ssh_forwarder(
                    remote_bind_address=(self.config.db_host, self.config.db_port)
                )
                _LOGGER.info(
                    "Start SSH tunnel:\n%s", self.__ssh_forwarder_to_str(forwarder)
                )
                exit_stack.enter_context(forwarder)

                db_host = forwarder.local_bind_host
                db_port = forwarder.local_bind_port
            else:
                db_host = self.config.db_host
                db_port = self.config.db_port

            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                user=self.config.db_username,
                password=self.config.db_password,
                dbname=self.config.db_name,
            )
            exit_stack.callback(conn.close)
            yield conn

    @_contextlib.contextmanager
    def smtp_login(self) -> _typing.Iterator[_smtplib.SMTP]:
        import smtplib

        _LOGGER.info(
            "[SMTP] Connect to server %s:%s",
            self.config.smtp_server,
            self.config.smtp_port,
        )

        client = smtplib.SMTP(self.config.smtp_server, self.config.smtp_port)

        client.ehlo()

        has_starttls = client.has_extn("STARTTLS")
        _LOGGER.info("[SMTP] thas STARTTLS? %s", has_starttls)

        if has_starttls:
            _LOGGER.debug("[SMTP] try STARTTLS")
            try:
                client.starttls()
            except Exception as exc:
                _LOGGER.error("[SMTP] STARTTLS failed: %s", str(exc))
                raise

        if self.config.smtp_username and self.config.smtp_password:
            _LOGGER.info("[SMTP] login as %s", self.config.smtp_username)
            client.login(self.config.smtp_username, self.config.smtp_password)
        else:
            _LOGGER.info("[SMTP] Skip login (credentials empty)")

        try:
            yield client
        finally:
            _LOGGER.info("[SMTP] QUIT")
            client.quit()
