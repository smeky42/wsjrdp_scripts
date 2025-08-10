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
            config = _config.Config.from_file(config_path)
        else:
            if config_path is not None:
                raise ValueError("Only one of 'config' and 'config_path' is allowed.")
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

    def pg_dump(
        self,
        *,
        dump_path: str | _pathlib.Path,
        format: _typing.Literal[
            "p", "plain", "c", "custom", "d", "directory", "t", "tar"
        ] = "plain",
    ) -> None:
        import contextlib
        import os
        import shlex
        import subprocess

        env = os.environ.copy()
        env["PGPASSWORD"] = self.config.db_password

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

            pg_dump_cmd = [
                "pg_dump",
                f"--host={db_host}",
                f"--username={self.config.db_username}",
                f"--port={db_port}",
                f"--dbname={self.config.db_name}",
                f"--format={format}",
                f"--file={dump_path}",
            ]
            pg_dump_cmd_str = " ".join(shlex.quote(a) for a in pg_dump_cmd)

            _LOGGER.info("Run %s", pg_dump_cmd_str)
            subprocess.run(pg_dump_cmd, env=env, check=True)
        p = os.path.abspath(dump_path)
        _LOGGER.info("Wrote %s (%s bytes)", p, os.path.getsize(p))

    def pg_restore(
        self, *, dump_path: str | _pathlib.Path, restore_into_production: bool = False
    ) -> None:
        import contextlib
        import os
        import shlex
        import subprocess

        db_name = self.config.db_name

        # By default we do not restore into a production database
        is_production = self.config.is_production or db_name in ("hitobito_production")
        if is_production and not restore_into_production:
            raise RuntimeError(
                "No restore into a production config unless 'restore_into_production' is True"
            )

        env = os.environ.copy()
        env["PGPASSWORD"] = self.config.db_password

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

            def run_psql(command):
                cmd = [
                    "psql",
                    f"--host={db_host}",
                    f"--username={self.config.db_username}",
                    f"--port={db_port}",
                    # f"--dbname={self.config.db_name}",
                    "-c",
                    str(command),
                ]
                cmd_str = " ".join(shlex.quote(a) for a in cmd)
                _LOGGER.info("Run %s", cmd_str)
                subprocess.run(cmd, env=env, check=True)

            def run_pg_restore(*args):
                cmd = [
                    "pg_restore",
                    f"--host={db_host}",
                    f"--username={self.config.db_username}",
                    f"--port={db_port}",
                    f"--dbname={self.config.db_name}",
                    *args,
                ]
                cmd_str = " ".join(shlex.quote(str(a)) for a in cmd)
                _LOGGER.info("Run %s", cmd_str)
                subprocess.run(cmd, env=env, check=True)

            quoted_db_name = f'"{db_name}"'
            run_psql(f"DROP DATABASE IF EXISTS {quoted_db_name};")
            run_psql(f"CREATE DATABASE {quoted_db_name};")
            run_pg_restore("--format=custom", "--clean", "--if-exists", "--no-owner", dump_path)
            _LOGGER.info("Finished restore")

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
