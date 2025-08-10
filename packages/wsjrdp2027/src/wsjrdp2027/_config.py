from __future__ import annotations

import dataclasses as _dataclasses
import logging as _logging
import os as _os
import pathlib as _pathlib
import typing as _typing

_LOGGER = _logging.getLogger(__name__)


@_dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class Config:
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
            _LOGGER.debug("Check if env WSJRDP_SCRIPTS_CONFIG is set")
            if (path_from_env := _os.environ.get("WSJRDP_SCRIPTS_CONFIG")) is not None:
                _LOGGER.info("Use env WSJRDP_SCRIPTS_CONFIG=%s", path_from_env)
                path = path_from_env
            else:
                path = "config-dev.yml"
        _LOGGER.info("Read config file %s", path)
        path = _pathlib.Path(path)
        with open(path, "r", encoding="utf-8") as f:
            config = _yaml.load(f, Loader=_yaml.FullLoader)

        kwargs: dict[str, str | _typing.Any] = {}

        is_production_from_name = path.name in ("config.yml", "config-prod.yml")
        is_production = cls.__to_bool(
            "is_production", config.get("is_production", is_production_from_name)
        )

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
            **kwargs,  # type: ignore
        )
        return self
