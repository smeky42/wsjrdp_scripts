from __future__ import annotations

import contextlib as _contextlib
import dataclasses as _dataclasses
import enum as _enum
import logging as _logging
import logging.handlers as _logging_handlers
import os as _os
import pathlib as _pathlib
import typing as _typing


if _typing.TYPE_CHECKING:
    import datetime as _datetime
    import smtplib as _smtplib

    import psycopg as _psycopg
    import sshtunnel as _sshtunnel


__all__ = [
    "WsjRdpContext",
    "WsjRdpContextConfig",
]


_LOGGER = _logging.getLogger(__name__)


@_dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class WsjRdpContextConfig:
    is_production: bool = True

    use_ssh_tunnel: bool = True
    ssh_host: str = ""
    ssh_port: int = 0
    ssh_username: str = ""
    ssh_private_key: str = ""

    db_host: str = ""
    db_port: int = 0
    db_username: str = ""
    db_password: str = ""
    db_name: str = ""

    smtp_server: str = ""
    smtp_port: int = 0
    smtp_username: str = ""
    smtp_password: str = ""

    geo_api_key: str = ""

    @staticmethod
    def __to_bool(name: str, value: bool | str) -> bool:
        if isinstance(value, bool):
            return value
        s_low = value.lower()
        if s_low in {"true", "t", "1", "yes"}:
            return True
        elif s_low in {"false", "f", "0", "no"}:
            return False
        else:
            raise ValueError(f"Invalid config {name}! Expected bool, got {value!r}")

    @classmethod
    def from_file(cls, path: str | _pathlib.Path | None = None) -> _typing.Self:
        import yaml as _yaml

        if path is None:
            _LOGGER.debug("[config] Check if env WSJRDP_SCRIPTS_CONFIG is set")
            if (path_from_env := _os.environ.get("WSJRDP_SCRIPTS_CONFIG")) is not None:
                _LOGGER.info("[config] Use env WSJRDP_SCRIPTS_CONFIG=%s", path_from_env)
                path = path_from_env
            else:
                path = "config-dev.yml"
        _LOGGER.info("[config] Read config file %s", path)
        path = _pathlib.Path(path)
        with open(path, "r", encoding="utf-8") as f:
            config = _yaml.load(f, Loader=_yaml.FullLoader)

        kwargs: dict[str, str | _typing.Any] = {}

        is_production = config.get("is_production")
        if is_production is None:
            is_production = path.name in ("config.yml", "config-prod.yml")
            _LOGGER.debug(
                "[config] assume is_production=%s based on filename %s %s",
                is_production,
                path.name,
                "(is_production not present in config)",
            )
        else:
            _LOGGER.info("[config] use is_production=%s (from %s)", is_production, path)

        use_ssh_tunnel = cls.__to_bool(
            "use_ssh_tunnel", config.get("use_ssh_tunnel", "true")
        )
        if use_ssh_tunnel:
            # SSH-Tunnel-Einstellungen
            ssh_host = config["ssh_host"]
            ssh_port = int(config["ssh_port"])
            ssh_username = config["ssh_username"]
            ssh_private_key = config["ssh_private_key"]
            kwargs.update(
                ssh_host=ssh_host,
                ssh_port=ssh_port,
                ssh_username=ssh_username,
                ssh_private_key=ssh_private_key,
            )

        self = cls(
            is_production=is_production,
            use_ssh_tunnel=use_ssh_tunnel,
            # PostgreSQL-Datenbank-Einstellungen
            db_host=config["db_host"],
            db_port=config["db_port"],
            db_username=config["db_username"],
            db_password=config["db_password"],
            db_name=config["db_name"],
            # SMTP-Einstellungen
            smtp_server=config["smtp_server"],
            smtp_port=config["smtp_port"],
            smtp_username=str(config.get("smtp_username", "")),
            smtp_password=str(config.get("smtp_password", "")),
            geo_api_key=str(config.get("geo_api_key","")),
            **kwargs,  # type: ignore
        )
        return self


class WsjRdpContextKind(_enum.StrEnum):
    """
    >>> str(WsjRdpContextKind.PRODUCTION)
    'prod'

    >>> repr(WsjRdpContextKind.PRODUCTION)
    'WsjRdpContextKind.PRODUCTION'
    """

    PRODUCTION = "prod"
    DEVELOPMENT = "dev"
    STAGING = "staging"

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}.{self.name}"


class WsjRdpContext:
    """Context (prod or dev?, hosts, ports, ...) of a script execution."""

    _config: WsjRdpContextConfig
    _buffering_handler: _UnlimitedBufferingHandler | None = None
    _kind: WsjRdpContextKind
    _start_time: _datetime.datetime
    _out_dir: _pathlib.Path = _typing.cast("_pathlib.Path", None)

    def __init__(
        self,
        config: WsjRdpContextConfig | _pathlib.Path | str | None = None,
        *,
        setup_logging: bool = True,
        log_level: int | str | None = None,
        start_time: _datetime.datetime | str | None = None,
        out_dir: _pathlib.Path | str | None = "data",
    ) -> None:
        """Initialize this context.

        Args:
          config: The config as a config object or filename
          setup_logging: Do some initial console logging config if
            `True` (default `True`)
          log_level: Console logging level (default INFO)
          start_time: Start time of script execution (default now)
          out_dir: Output directory (default ``"data"``)

        ..
           >>> import datetime, zoneinfo, pathlib
           >>> from pathlib import Path
           >>> from zoneinfo import ZoneInfo
           >>> monkeypatch = getfixture("monkeypatch")
           >>> tmp_path = getfixture("tmp_path")
           >>> monkeypatch.chdir(tmp_path)

        When giving *out_dir* as a :obj:`str`, (Jinja2) templating is
        applied, see also :obj:`render_template`:

        >>> prod_conf = WsjRdpContextConfig(is_production=True)
        >>> now = datetime.datetime(2025, 8, 15, 10, 30, 27, 1234, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin"))
        >>> prod_ctx = WsjRdpContext(prod_conf, setup_logging=False, out_dir="data/foo_{{ filename_suffix }}", start_time=now)
        >>> str(prod_ctx.out_dir.relative_to(Path.cwd()))
        'data/foo_20250815-103027_PROD'
        """
        from . import _util

        # Default basic logging config
        if setup_logging:
            log_level = _util.to_log_level(log_level, default=_logging.INFO)
            self._buffering_handler = _UnlimitedBufferingHandler()
            self._buffering_handler.setLevel(_logging.DEBUG)
            stream_handler = _logging.StreamHandler()
            stream_handler.setLevel(log_level)
            _logging.basicConfig(
                level=_logging.DEBUG,
                format="%(asctime)s %(levelname)-1s %(message)s",
                handlers=[stream_handler, self._buffering_handler],
            )

        # wsjrdp_scripts config
        if config is None:
            config = WsjRdpContextConfig.from_file()
        elif not isinstance(config, WsjRdpContextConfig):
            config = WsjRdpContextConfig.from_file(config)
        self._config = config
        self._kind = (
            WsjRdpContextKind.PRODUCTION
            if self._config.is_production
            else WsjRdpContextKind.DEVELOPMENT
        )

        # start_time, output_directory, ...
        self._start_time = self._determine_start_time(start_time=start_time)
        self._out_dir = self._determine_out_dir(out_dir)

    def _determine_start_time(
        self, start_time: _datetime.datetime | str | None = None, *, env=None
    ) -> _datetime.datetime:
        import datetime as _datetime
        import os as _os

        from . import _util

        if env is None:
            env = _os.environ
        env_name = "WSJRDP_SCRIPTS_START_TIME"
        env_val = env.get(f"{env_name}")
        _LOGGER.debug(
            "[ctx] found %s%s",
            env_name,
            (" not set" if env_val is None else f"={env_val}"),
        )
        if self._config.is_production and env_val is not None:
            _LOGGER.warning("[ctx] Production run: Ignore %s", env_name)
            env_val = None

        if start_time is not None:
            result = _util.to_datetime(start_time)
            _LOGGER.info("[ctx] start_time=%s (explicitly given)", result.isoformat())
        elif env_val:
            result = _util.to_datetime(env_val)
            _LOGGER.info("[ctx] start_time=%s (from %s)", result.isoformat(), env_name)
        else:
            result = _datetime.datetime.now().astimezone()
            _LOGGER.info("[ctx] start_time=%s (current time)", result.isoformat())
        return result

    def _determine_out_dir(self, p: _pathlib.Path | str | None = None) -> _pathlib.Path:
        import os as _os
        import pathlib as _pathlib

        out_dir: _pathlib.Path
        if p is None:
            out_dir = _pathlib.Path(".")
        elif isinstance(p, str):
            out_dir = _pathlib.Path(
                self.render_template(
                    p, extra_context={"filename_suffix": self.filename_suffix}
                )
            )
        else:
            out_dir = p
        out_dir = out_dir.resolve()
        _LOGGER.info("[ctx] output_directory=%s", _os.path.relpath(out_dir, "."))
        return out_dir

    @property
    def config(self) -> WsjRdpContextConfig:
        return self._config

    @property
    def is_production(self) -> bool:
        """`True` in production config, `False` otherwise."""
        return self._config.is_production

    @property
    def start_time(self) -> _datetime.datetime:
        return self._start_time

    @property
    def out_dir(self) -> _pathlib.Path:
        return self._out_dir

    @property
    def relative_out_dir(self) -> _pathlib.Path:
        import pathlib as _pathlib

        return self._out_dir.relative_to(_pathlib.Path.cwd(), walk_up=True)

    @property
    def kind(self) -> WsjRdpContextKind:
        return self._kind

    @property
    def start_time_for_filename(self) -> str:
        return self.start_time.strftime("%Y%m%d-%H%M%S")

    @property
    def filename_suffix(self) -> str:
        suffix = self.start_time_for_filename
        if self.is_production:
            suffix += f"_{str(self.kind).upper()}"
        return suffix

    def make_out_path(
        self, template: str, *, relative: bool = True, mkdir: bool = True
    ) -> _pathlib.Path:
        import pathlib as _pathlib

        rendered_template = self.render_template(template)
        path = self.out_dir / rendered_template
        if not path.is_relative_to(self.out_dir):
            raise RuntimeError(
                f"Invalid out_path, not under out_dir={self.relative_out_dir}"
            )
        if mkdir:
            path.parent.mkdir(parents=True, exist_ok=True)
        return path.relative_to(_pathlib.Path.cwd(), walk_up=True) if relative else path

    def configure_log_file(
        self, filename: str | _pathlib.Path, level: int | str | None = None
    ) -> _logging.Handler:
        from . import _util

        _LOGGER.info("[ctx] Writing log file %s", filename)
        file_handler = _util.configure_file_logging(filename, level=level)
        if self._buffering_handler is not None:
            buffering_handler, self._buffering_handler = self._buffering_handler, None
            buffering_handler.setTarget(file_handler)
            buffering_handler.flush()
            _logging.getLogger().removeHandler(buffering_handler)
        return file_handler

    def require_approval_to_run_in_prod(self, prompt: str | None = None) -> None:
        from ._util import console_confirm

        if self._config.is_production:
            _LOGGER.warning(
                "[ctx] Running in production - asking for user consent in console"
            )
            if not prompt:
                prompt = "Do you want to continue running this script in a PRODUCTION environment?"
            print()
            print()
            if not console_confirm(prompt, default=False):
                _LOGGER.info("[ctx] Ending script: No user approval given")
                raise SystemExit(0)
            else:
                _LOGGER.debug("[ctx] User approved to continue")
                return
        else:
            _LOGGER.debug(
                "[ctx] Not running in production - no special approval required"
            )
            return

    def require_approval_to_send_email_in_prod(self) -> None:
        prompt = (
            f"Do you want to send email messages in a PRODUCTION environment "
            f"via SMTP server {self.config.smtp_server}:{self.config.smtp_port}?"
        )
        self.require_approval_to_run_in_prod(prompt=prompt)

    def __create_ssh_forwarder(
        self, *, remote_bind_address: tuple[str, int]
    ) -> _sshtunnel.SSHTunnelForwarder:
        import sshtunnel

        return sshtunnel.SSHTunnelForwarder(
            (self._config.ssh_host, self._config.ssh_port),
            ssh_username=self._config.ssh_username,
            ssh_pkey=self._config.ssh_private_key,
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
    def psycopg_connect(self) -> _typing.Iterator[_psycopg.Connection]:
        import contextlib

        import psycopg

        with contextlib.ExitStack() as exit_stack:
            if self._config.use_ssh_tunnel:
                forwarder = self.__create_ssh_forwarder(
                    remote_bind_address=(self._config.db_host, self._config.db_port)
                )
                _LOGGER.info(
                    "Start SSH tunnel:\n%s", self.__ssh_forwarder_to_str(forwarder)
                )
                exit_stack.enter_context(forwarder)

                db_host = forwarder.local_bind_host
                db_port = forwarder.local_bind_port
            else:
                db_host = self._config.db_host
                db_port = self._config.db_port

            conn = psycopg.connect(
                host=db_host,
                port=db_port,
                user=self._config.db_username,
                password=self._config.db_password,
                dbname=self._config.db_name,
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
        import os as _os
        import pathlib as _pathlib
        import shlex
        import subprocess

        import humanfriendly as _humanfriendly

        env = _os.environ.copy()
        env["PGPASSWORD"] = self._config.db_password

        dump_path = _pathlib.Path(dump_path).resolve()
        rel_dump_path = dump_path.relative_to(_pathlib.Path.cwd(), walk_up=True)

        with contextlib.ExitStack() as exit_stack:
            if self._config.use_ssh_tunnel:
                forwarder = self.__create_ssh_forwarder(
                    remote_bind_address=(self._config.db_host, self._config.db_port)
                )
                _LOGGER.info(
                    "[pg_dump] Start SSH tunnel:\n%s",
                    self.__ssh_forwarder_to_str(forwarder),
                )
                exit_stack.enter_context(forwarder)

                db_host = forwarder.local_bind_host
                db_port = forwarder.local_bind_port
            else:
                db_host = self._config.db_host
                db_port = self._config.db_port

            pg_dump_cmd = [
                "pg_dump",
                f"--host={db_host}",
                f"--username={self._config.db_username}",
                f"--port={db_port}",
                f"--dbname={self._config.db_name}",
                f"--format={format}",
                f"--file={rel_dump_path}",
            ]
            pg_dump_cmd_str = " ".join(shlex.quote(a) for a in pg_dump_cmd)

            _LOGGER.info(
                "[pg_dump] Run %s (passing db_password in env PGPASSWORD)",
                pg_dump_cmd_str,
            )
            subprocess.run(pg_dump_cmd, env=env, check=True)
        size = _os.path.getsize(dump_path)
        _LOGGER.info(
            "[pg_dump] Wrote %s (%s / %s bytes)",
            rel_dump_path,
            _humanfriendly.format_size(size, binary=True),
            size,
        )

    def pg_restore(
        self, *, dump_path: str | _pathlib.Path, restore_into_production: bool = False
    ) -> None:
        import contextlib
        import os as _os
        import pathlib as _pathlib
        import shlex
        import subprocess

        db_name = self._config.db_name

        # By default we do not restore into a production database
        is_production = self._config.is_production or db_name in ["hitobito_production"]
        if is_production and not restore_into_production:
            raise RuntimeError(
                "No restore into a production config unless 'restore_into_production' is True"
            )

        env = _os.environ.copy()
        env["PGPASSWORD"] = self._config.db_password

        dump_path = _pathlib.Path(dump_path).resolve()
        rel_dump_path = dump_path.relative_to(_pathlib.Path.cwd(), walk_up=True)

        with contextlib.ExitStack() as exit_stack:
            if self._config.use_ssh_tunnel:
                forwarder = self.__create_ssh_forwarder(
                    remote_bind_address=(self._config.db_host, self._config.db_port)
                )
                _LOGGER.info(
                    "[pg_restore] Start SSH tunnel:\n%s",
                    self.__ssh_forwarder_to_str(forwarder),
                )
                exit_stack.enter_context(forwarder)

                db_host = forwarder.local_bind_host
                db_port = forwarder.local_bind_port
            else:
                db_host = self._config.db_host
                db_port = self._config.db_port

            def run_psql(command):
                cmd = [
                    "psql",
                    f"--host={db_host}",
                    f"--username={self._config.db_username}",
                    f"--port={db_port}",
                    "-c",
                    str(command),
                ]
                cmd_str = " ".join(shlex.quote(a) for a in cmd)
                _LOGGER.info(
                    "[pg_restore] Run %s (passing db_password in env PGPASSWORD)",
                    cmd_str,
                )
                subprocess.run(cmd, env=env, check=True)

            def run_pg_restore(*args):
                cmd = [
                    "pg_restore",
                    f"--host={db_host}",
                    f"--username={self._config.db_username}",
                    f"--port={db_port}",
                    f"--dbname={self._config.db_name}",
                    *args,
                ]
                cmd_str = " ".join(shlex.quote(str(a)) for a in cmd)
                _LOGGER.info("[pg_restore] Run %s", cmd_str)
                subprocess.run(cmd, env=env, check=True)

            quoted_db_name = f'"{db_name}"'
            run_psql(f"DROP DATABASE IF EXISTS {quoted_db_name};")
            run_psql(f"CREATE DATABASE {quoted_db_name};")
            run_pg_restore(
                "--format=custom", "--clean", "--if-exists", "--no-owner", rel_dump_path
            )
            _LOGGER.info("[pg_restore] Finished restore")

    @_contextlib.contextmanager
    def smtp_login(self) -> _typing.Iterator[_smtplib.SMTP]:
        import smtplib

        _LOGGER.info(
            "[SMTP] Connect to server %s:%s",
            self._config.smtp_server,
            self._config.smtp_port,
        )

        client = smtplib.SMTP(self._config.smtp_server, self._config.smtp_port)

        client.ehlo()

        has_starttls = client.has_extn("STARTTLS")
        _LOGGER.info("[SMTP] has STARTTLS? %s", has_starttls)

        if has_starttls:
            _LOGGER.debug("[SMTP] try STARTTLS")
            try:
                client.starttls()
            except Exception as exc:
                _LOGGER.error("[SMTP] STARTTLS failed: %s", str(exc))
                raise

        if self._config.smtp_username and self._config.smtp_password:
            _LOGGER.info("[SMTP] login as %s", self._config.smtp_username)
            client.login(self._config.smtp_username, self._config.smtp_password)
        else:
            _LOGGER.info("[SMTP] Skip login (credentials empty)")

        try:
            yield client
        finally:
            _LOGGER.info("[SMTP] QUIT")
            client.quit()

    def render_template(
        self,
        template: str,
        *,
        extra_context: dict | None = None,
        extra_filters: dict[str, _typing_Callable] | None = None,
    ) -> str:
        """Render *template*.

        Available variables:

        * ``filename_suffix`` - a string encoding :obj:`start_time`
          and :obj:`kind` (if in production), suitable as a filename
          suffix.
        * ``kind`` - context kind (:obj:`WsjRdpContextKind`)
        * ``is_production`` - `True` if production config, `False`
          otherwise
        * ``out_dir`` - :obj:`output_directory` (if already defined)
        * ``start_time`` - :obj:`start_time`

        Available extra filters:

        * ``strftime`` - calls :obj:`datetime.datetime.strftime`
        * ``isoformat`` - calls :obj:`datetime.datetime.isoformat`
        * ``to_ext`` - adds leading dot (``.``) if non-empty
        * ``omit_if_prod`` - replace with empty string (``""``) if in
          production
        * ``omit_unless_prod`` - replace with empty string (``""``)
          unless in production

        >>> prod = WsjRdpContextConfig(is_production=True)
        >>> dev = WsjRdpContextConfig(is_production=False)
        >>> import datetime as dt, zoneinfo as zi
        >>> now = dt.datetime(2025, 8, 15, 10, 30, 27, 1234, tzinfo=zi.ZoneInfo("Europe/Berlin"))
        >>> now.strftime("%Y%m%d-%H%M%S")
        '20250815-103027'

        >>> template = "data/my_out.{{ start_time | strftime('%Y%m%d-%H%M%S') }}{{ kind | omit_unless_prod | upper | to_ext }}"

        >>> prod_ctx = WsjRdpContext(prod, setup_logging=False, out_dir=".", start_time=now)
        >>> prod_ctx.render_template(template)
        'data/my_out.20250815-103027.PROD'

        >>> dev_ctx = WsjRdpContext(dev, setup_logging=False, out_dir=".", start_time=now)
        >>> dev_ctx.render_template(template)
        'data/my_out.20250815-103027'
        """
        from . import _util

        is_prod = self.is_production
        context = {
            "filename_suffix": self.filename_suffix,
            "is_production": is_prod,
            "kind": self.kind,
            "start_time": self._start_time,
            "start_time_for_filename": self.start_time_for_filename,
        }
        if out_dir := getattr(self, "out_dir", None):
            context["out_dir"] = out_dir
        filters = {
            "omit_unless_prod": lambda obj: obj if self.is_production else "",
            "omit_if_prod": lambda obj: "" if self.is_production else obj,
        }
        filters.update(extra_filters or {})
        return _util.render_template(
            template, context, extra_context=extra_context, extra_filters=filters
        )


class _UnlimitedBufferingHandler(_logging_handlers.MemoryHandler):
    def __init__(self, target=None, flushOnClose: bool = True) -> None:
        super().__init__(
            10_000_000_000_000,
            flushLevel=_logging.CRITICAL + 10,
            target=target,
            flushOnClose=flushOnClose,
        )

    def shouldFlush(self, record) -> bool:
        return False
