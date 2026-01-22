from __future__ import annotations

import collections.abc as _collections_abc
import contextlib as _contextlib
import dataclasses as _dataclasses
import datetime as _datetime
import logging as _logging
import pathlib as _pathlib
import typing as _typing

from . import _people_query, _types, _util


if _typing.TYPE_CHECKING:
    import email.message as _email_message
    import email.policy as _email_policy

    import pandas as _pandas
    import psycopg as _psycopg

    from . import _context, _mail_client, _people_query


__all__ = [
    "BatchConfig",
    "PreparedEmailMessage",
    "PreparedBatch",
]


_LOGGER = _logging.getLogger(__name__)


@_dataclasses.dataclass(kw_only=True)
class PreparedEmailMessage:
    mailing_name: str
    message: _email_message.EmailMessage | None = None
    row: _pandas.Series | None = None
    summary: str
    eml_name: str
    _eml: bytes | None = None

    def __init__(
        self,
        *,
        mailing_name: str,
        message: _email_message.EmailMessage | None = None,
        row: _pandas.Series | None = None,
        summary: str,
        eml_name: str | None = None,
        eml: bytes | None = None,
    ) -> None:
        self.mailing_name = mailing_name
        self.message = message
        self.row = row
        self.summary = summary
        self.eml_name = eml_name if eml_name else f"{self.mailing_name}.eml"
        self._eml = eml

    @property
    def eml(self) -> bytes:
        if self._eml is None:
            if self.message is not None:
                self._eml = self.message.as_bytes()
            else:
                self._eml = b""
        return self._eml

    @eml.setter
    def eml(self, value: bytes) -> None:
        self._eml = value


@_dataclasses.dataclass(kw_only=True)
class PreparedBatch:
    name: str = "batch"
    df: _pandas.DataFrame
    unfiltered_df: _pandas.DataFrame | None = None
    messages: tuple[PreparedEmailMessage, ...] = ()
    action_arguments: dict = _dataclasses.field(default_factory=lambda: {})
    updates: dict = _dataclasses.field(default_factory=lambda: {})
    email_from: str = ""
    from_addr: str | None = None
    config_yaml: bytes | None
    out_dir: _pathlib.Path | None = None
    now: _datetime.datetime = _typing.cast(_datetime.datetime, None)
    dry_run: bool = False
    skip_email: bool = False
    skip_db_updates: bool = False
    results: dict = _dataclasses.field(default_factory=lambda: {})

    def __post_init__(self) -> None:
        if self.now is None:
            self.now = _datetime.datetime.now().astimezone()

    def _get_out_dir(self, out_dir: _pathlib.Path | str | None = None) -> _pathlib.Path:
        if out_dir:
            return _pathlib.Path(out_dir)
        elif self.out_dir:
            return self.out_dir
        else:
            return _pathlib.Path("data") / "batch"

    def write_data(
        self,
        *,
        out_dir: _pathlib.Path | None = None,
        zip_eml: bool | None = None,
        silent_skip_email: bool | None = None,
    ) -> _pathlib.Path:
        from . import _people

        if zip_eml is None:
            zip_eml = True
        out_dir = self._get_out_dir(out_dir=out_dir)

        self.__collect_default_results()

        out_dir.mkdir(exist_ok=True, parents=True)
        _LOGGER.info("  batch output directory: %s", out_dir)

        xlsx_path = out_dir / f"{self.name}.xlsx"
        unfiltered_xlsx_path = out_dir / f"{self.name}.unfiltered.xlsx"
        yml_path = out_dir / f"{self.name}.yml"

        self.__write_eml(
            out_dir=out_dir, zip_eml=zip_eml, silent_skip_email=silent_skip_email
        )

        _people.write_people_dataframe_to_xlsx(
            self.df, xlsx_path, log_level=_logging.DEBUG
        )
        _LOGGER.info("  wrote df xlsx %s", xlsx_path)

        if self.unfiltered_df is not None and self.unfiltered_df is not self.df:
            _people.write_people_dataframe_to_xlsx(
                self.unfiltered_df, unfiltered_xlsx_path, log_level=_logging.DEBUG
            )
            _LOGGER.info("  wrote unfiltered_df xlsx %s", unfiltered_xlsx_path)

        if self.config_yaml:
            with open(yml_path, "wb") as f:
                f.write(self.config_yaml)
            _LOGGER.info("  wrote yml %s", yml_path)

        return out_dir

    def __collect_default_results(self):
        if self.unfiltered_df is not None:
            unfiltered_ids_set = frozenset(self.unfiltered_df["id"])
        else:
            unfiltered_ids_set = frozenset()
        ids_set = frozenset(self.df[self.df["skip_db_updates"] == False]["id"])
        email_only_ids_set = frozenset(
            self.df[self.df["skip_db_updates"] != False]["id"]
        )
        self.results["ids"] = sorted(ids_set)
        self.results["email_only_ids"] = sorted(email_only_ids_set)
        skipped_ids_set = unfiltered_ids_set - ids_set - email_only_ids_set
        self.results["skipped_ids"] = sorted(skipped_ids_set)

    def __write_eml(
        self,
        *,
        out_dir: _pathlib.Path,
        zip_eml: bool,
        silent_skip_email: bool | None = None,
    ) -> None:
        import io
        import zipfile

        skipped_messages = [pm for pm in self.messages if not pm.message]
        prepared_messages = [pm for pm in self.messages if pm.message]
        if not silent_skip_email:
            for prep_msg in skipped_messages:
                _LOGGER.debug("  skip empty email message - %s", prep_msg.summary)
        if not prepared_messages:
            if not silent_skip_email:
                _LOGGER.info("  only empty messages - no eml output")
            return

        if zip_eml:
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w") as zf:
                for prep_msg in prepared_messages:
                    zf.writestr(prep_msg.eml_name, data=prep_msg.eml)
            eml_zip_path = out_dir / f"{self.name}.zip"
            with open(eml_zip_path, "wb") as f:
                f.write(zip_buf.getvalue())
            _LOGGER.info("  wrote eml zip %s", eml_zip_path)
        else:
            for prep_msg in prepared_messages:
                eml_path = out_dir / prep_msg.eml_name
                eml_path.write_bytes(prep_msg.eml)
                _LOGGER.info("  wrote eml %s", eml_path)

    def write_results(
        self, *, out_dir: _pathlib.Path | None = None, json_indent: int | None = None
    ) -> None:
        import json

        out_dir = self._get_out_dir(out_dir=out_dir)
        json_path = out_dir / f"{self.name}.json"

        with open(json_path, "w") as f:
            json_d = {"results": self.results}
            json.dump(json_d, f, indent=json_indent)
        _LOGGER.info("  wrote json %s", json_path)

    def get_skip_email_reasons(self, *, dry_run: bool | None = None) -> list[str]:
        reasons = []
        if dry_run:
            reasons.append("dry_run")
        if self.skip_email:
            reasons.append("skip_email=True")
        if all(m.message is None for m in self.messages):
            reasons.append("no message")
        return reasons

    def send(self, mail_client: _mail_client.MailClient, /) -> None:
        import pprint
        import textwrap

        num_messages = len(self.messages)
        if self.dry_run:
            if self.skip_email:
                _LOGGER.info(
                    "Skip sending %s messages (dry_run is True and skip_email is True)",
                    num_messages,
                )
            else:
                _LOGGER.info("Skip sending %s messages (dry_run is True)", num_messages)
            return
        elif self.skip_email:
            _LOGGER.info("Skip sending %s messages (skip_email is True)", num_messages)
            return
        for i, prep_msg in enumerate(self.messages, start=1):
            pcnt = (i / num_messages) * 100.0
            _LOGGER.info("%s %s", f"{i}/{num_messages} ({pcnt:.1f}%)", prep_msg.summary)
            if prep_msg.row is not None:
                row_d = dict(prep_msg.row)
                row_d.pop("person_dict", None)
                row_d.pop("primary_group_roles", None)
                _LOGGER.debug(
                    "  person:\n%s", textwrap.indent(pprint.pformat(row_d), "   | ")
                )
            if prep_msg.message is None:
                _LOGGER.debug("  Skip: No actual email message")
            else:
                try:
                    mail_client.send_message(prep_msg.message, from_addr=self.from_addr)
                except Exception as exc:
                    _LOGGER.error(
                        "  Exception raised during email sending: %s", str(exc)
                    )
                    raise


@_dataclasses.dataclass(kw_only=True)
class BatchConfig:
    query: _people_query.PeopleQuery
    email_subject: str = ""
    email_from: str = ""
    email_reply_to: list[str] | None = None
    extra_email_to: list[str] | None = None
    extra_email_cc: list[str] | None = None
    extra_email_bcc: list[str] | None = None
    from_addr: str | None = None
    signature: str = ""
    content: str | None = None
    content_file: _pathlib.Path | None = None
    html_content: str | None = None
    html_content_file: _pathlib.Path | None = None
    name: str = "batch"
    summary: str = "{{ row.id }} {{ row.short_full_name }}; role: {{ row.payment_role }}; status: {{ row.status }}{% if msg %}; To: {{ msg.to }}; Cc: {{ msg.cc }}{% endif %}"
    jinja_extra_globals: dict | None = None
    action_arguments: dict
    updates: dict
    dry_run: bool = False
    skip_email: bool
    skip_db_updates: bool | None = None
    raw_yaml: bytes | None = None
    base_dir: _pathlib.Path | None = None
    _effective_base_dir: _pathlib.Path

    def __init__(
        self,
        /,
        *,
        query: _people_query.PeopleQuery | dict | None = None,
        where: _people_query.PeopleWhere | dict | str | None = None,
        email_subject: str | None = None,
        from_addr: str | None = None,
        email_from: str | None = None,
        email_reply_to: str | None = None,
        extra_email_to: _collections_abc.Iterable[str] | str | None = None,
        extra_email_cc: _collections_abc.Iterable[str] | str | None = None,
        extra_email_bcc: _collections_abc.Iterable[str] | str | None = None,
        signature: str | None = None,
        content: str | None = None,
        content_file: _pathlib.Path | str | None = None,
        html_content: str | None = None,
        html_content_file: _pathlib.Path | str | None = None,
        name: str | None = None,
        summary: str | None = None,
        jinja_extra_globals: dict | None = None,
        action_arguments: dict | None = None,
        updates: dict | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
        raw_yaml: bytes | None = None,
        base_dir: _pathlib.Path | None = None,
        effective_base_dir: _pathlib.Path | None = None,
    ) -> None:
        import email.utils

        from . import _util

        if effective_base_dir:
            self._effective_base_dir = _pathlib.Path(effective_base_dir).resolve()
            self.base_dir = _pathlib.Path(base_dir) if base_dir else None
        elif base_dir:
            self.base_dir = _pathlib.Path(base_dir)
            self._effective_base_dir = self.base_dir.resolve()
        else:
            self._effective_base_dir = _pathlib.Path.cwd()
            self.base_dir = None

        def normalize_path(p: _pathlib.Path | str) -> _pathlib.Path:
            p = _pathlib.Path(p)
            if p.is_absolute():
                return p
            else:
                return (self._effective_base_dir / p).relative_to(
                    self._effective_base_dir, walk_up=True
                )

        self.name = name or "batch"
        self.email_subject = email_subject or ""
        self.query = _normalize_query_where(query, where, logger=None)
        if from_addr:
            from_addr = email.utils.parseaddr(from_addr)[1]
            self.from_addr = from_addr
        else:
            self.from_addr = "anmeldung@worldscoutjamboree.de"
        self.email_from = email_from or self.from_addr
        self.email_reply_to = _util.to_str_list_or_none(email_reply_to)
        self.extra_email_to = _util.to_str_list_or_none(extra_email_to)
        self.extra_email_cc = _util.to_str_list_or_none(extra_email_cc)
        self.extra_email_bcc = _util.to_str_list_or_none(extra_email_bcc)
        self.signature = signature or ""
        self.content = content or None
        self.html_content = html_content or None
        if content and content_file:
            raise RuntimeError("Only one of 'content' and 'content_file' allowed")
        if html_content and html_content_file:
            raise RuntimeError(
                "Only one of 'html_content' and 'html_content_file' allowed"
            )
        if content_file:
            self.content_file = normalize_path(content_file)
            self.content = self._slurp(content_file)
        else:
            self.content_file = None
        if html_content_file:
            self.html_content_file = normalize_path(html_content_file)
            self.html_content = self._slurp(self.html_content_file)
        elif html_content and html_content.endswith(".html"):
            self.html_content_file = normalize_path(html_content)
            self.html_content = self._slurp(self.html_content_file)
        else:
            self.html_content_file = None

        self.summary = (
            summary
            or "{{ row.payment_role.short_role_name }} {{ row.id_and_name }}; status: {{ row.status }}{% if msg %}; To: {{ msg.to }}; Cc: {{ msg.cc }}{% endif %}"
        )
        self.jinja_extra_globals = jinja_extra_globals
        self.action_arguments = action_arguments if action_arguments is not None else {}
        self.dry_run = dry_run if dry_run is not None else False
        self.skip_db_updates = skip_db_updates if skip_db_updates is not None else False

        if self.content is None and self.html_content is None and skip_email is None:
            self.skip_email = True
        elif skip_email is None:
            self.skip_email = False
        else:
            self.skip_email = skip_email

        updates = updates.copy() if updates else {}
        for key in ["add_tags", "remove_tags", "new_primary_group_role_types"]:
            if key not in updates:
                continue
            val = _util.to_str_list(updates.get(key)) or None
            if val is None:
                updates.pop(key, None)
            else:
                updates[key] = val
        self.updates = updates

        if raw_yaml is None:
            self.raw_yaml = self.__to_yaml__().encode("utf-8")
        else:
            self.raw_yaml = raw_yaml

    @classmethod
    def from_dict(
        cls,
        config: _collections_abc.Mapping | None = None,
        /,
        *,
        name: str | None = None,
        query: _people_query.PeopleQuery | dict | None = None,
        where: _people_query.PeopleWhere | dict | str | None = None,
        jinja_extra_globals: dict | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
        path: _pathlib.Path | str | None = None,
        effective_base_dir: _pathlib.Path | str | None = None,
        raw_yaml: bytes | str | None = None,
    ) -> _typing.Self:
        config = dict(config.items()) if config else {}
        path = _pathlib.Path(path) if path else None

        _INVALID_KEYS = ["raw_yaml", "effective_base_dir"]
        try:
            invalid_keys = sorted(k for k in _INVALID_KEYS if k in config)
            if invalid_keys:
                raise ValueError(
                    f"Parameter(s) {', '.join(repr(k) for k in invalid_keys)} not allowed in {path}"
                )
        except Exception as exc:
            _LOGGER.exception("Failed to read %s: %s", str(path), str(exc))
            raise

        missing = object()
        try:
            if query is None:
                query =  _people_query.PeopleQuery.normalize(config.get("query"))
                if where is not None:
                    query = query.replace(where=_people_query.PeopleWhere.normalize(where))
            else:
                query = _normalize_query_where_or_none(query, where, logger=None)
            if query:
                config["query"] = query
            if jinja_extra_globals is not None:
                config["jinja_extra_globals"] = jinja_extra_globals
            if name:
                config["name"] = name
            elif not config.get("name") and path and path.stem:
                config["name"] = path.stem
            if raw_yaml:
                if isinstance(raw_yaml, bytes):
                    config["raw_yaml"] = raw_yaml
                else:
                    config["raw_yaml"] = raw_yaml.encode("utf-8")
            if effective_base_dir:
                effective_base_dir = _pathlib.Path(effective_base_dir).resolve()
            elif "base_dir" in config:
                effective_base_dir = _pathlib.Path(config["base_dir"]).resolve()
            elif path:
                effective_base_dir = path.parent
            else:
                effective_base_dir = None
            if dry_run is not None:
                config["dry_run"] = dry_run
            if skip_email is not None:
                config["skip_email"] = skip_email
            if skip_db_updates is not None:
                config["skip_db_updates"] = skip_db_updates
            self = cls(**config, effective_base_dir=effective_base_dir)
            return self
        except Exception as exc:
            _LOGGER.exception("Failed to parse yaml %s: %s", path, str(exc))
            raise

    @classmethod
    def from_yaml_string(
        cls,
        raw_yaml: bytes | str,
        /,
        *,
        name: str | None = None,
        query: _people_query.PeopleQuery | dict | None = None,
        where: _people_query.PeopleWhere | dict | str | None = None,
        jinja_extra_globals: dict | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
        path: _pathlib.Path | str | None = None,
        effective_base_dir: _pathlib.Path | str | None = None,
    ) -> _typing.Self:
        import yaml as _yaml

        try:
            config = _yaml.load(raw_yaml, Loader=_yaml.FullLoader)
        except Exception as exc:
            _LOGGER.exception("Failed to read %s: %s", str(path), str(exc))
            raise

        return cls.from_dict(
            config,
            name=name,
            query=query,
            where=where,
            jinja_extra_globals=jinja_extra_globals,
            dry_run=dry_run,
            skip_email=skip_email,
            skip_db_updates=skip_db_updates,
            path=path,
            effective_base_dir=effective_base_dir,
            raw_yaml=raw_yaml,
        )

    @classmethod
    def from_yaml(
        cls,
        path: str | _pathlib.Path,
        /,
        *,
        name: str | None = None,
        query: _people_query.PeopleQuery | dict | None = None,
        where: _people_query.PeopleWhere | dict | str | None = None,
        jinja_extra_globals: dict | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
        effective_base_dir: _pathlib.Path | str | None = None,
    ) -> _typing.Self:
        path = _pathlib.Path(path)
        _LOGGER.info("Read batch config %s", str(path))
        try:
            with open(path, "r", encoding="utf-8", newline="\n") as f:
                raw_yaml = f.read()
        except Exception as exc:
            _LOGGER.exception("Failed to read %s: %s", str(path), str(exc))
            raise

        return cls.from_yaml_string(
            raw_yaml,
            name=name,
            query=query,
            where=where,
            jinja_extra_globals=jinja_extra_globals,
            dry_run=dry_run,
            skip_email=skip_email,
            skip_db_updates=skip_db_updates,
            path=path,
            effective_base_dir=effective_base_dir,
        )

    def copy(self) -> _typing.Self:
        import copy

        return copy.deepcopy(self)

    def replace(
        self,
        *,
        name: str | None = None,
        query: _people_query.PeopleQuery | None = None,
        jinja_extra_globals: dict | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
    ) -> _typing.Self:
        import copy

        obj = copy.deepcopy(self)
        if name is not None:
            obj.name = name
        if query is not None:
            obj.query = query
        if jinja_extra_globals is not None:
            obj.jinja_extra_globals = jinja_extra_globals
        if dry_run is not None:
            obj.dry_run = dry_run
        if skip_email is not None:
            obj.skip_email = skip_email
        if skip_db_updates is not None:
            obj.skip_db_updates = skip_db_updates
        return obj

    def extend_extra_email_bcc(
        self, *args: str | _typing.Iterable[str] | None
    ) -> list[str] | None:
        from . import _util

        self.extra_email_bcc = _util.to_str_list_or_none(self.extra_email_bcc, *args)
        return self.extra_email_bcc

    def __to_yaml__(self) -> str:
        from . import _util

        d = {
            "dry_run": self.dry_run,
            "skip_db_updates": self.skip_db_updates,
            "skip_email": self.skip_email,
            "action_arguments": (self.action_arguments or None),
            "updates": (self.updates or None),
            "query": (self.query.__to_dict__() if self.query is not None else None),
            "summary": self.summary,
            "email_subject": self.email_subject,
            "email_from": self.email_from,
            "email_reply_to": self.email_reply_to,
            "extra_email_to": self.extra_email_to,
            "extra_email_cc": self.extra_email_cc,
            "extra_email_bcc": self.extra_email_bcc,
            "content": (None if self.content_file else self.content),
            "html_content": (None if self.html_content_file else self.html_content),
            "signature": self.signature,
            **{
                k: str(v)
                for k in ["base_dir", "content_file", "html_content_file"]
                if (v := getattr(self, k, None))
            },
        }
        d = {k: v for k, v in d.items() if v is not None}

        return _util.to_yaml_str(d)

    def _slurp(
        self,
        path: _pathlib.Path | str,
        /,
        *,
        encoding: str = "utf-8",
        newline: str = "\n",
    ) -> str:
        """Slurp *path* and return string."""
        real_path = self._effective_base_dir / _pathlib.Path(path)
        with open(real_path, encoding=encoding, newline=newline) as f:
            return f.read()

    def update_raw_yaml(self, raw_yaml: bytes | str | None = None) -> None:
        if raw_yaml is None:
            raw_yaml = self.__to_yaml__().encode("utf-8")
        elif isinstance(raw_yaml, str):
            raw_yaml = raw_yaml.encode("utf-8")
        self.raw_yaml = raw_yaml

    def update_dataframe_for_updates(
        self,
        df: _pandas.DataFrame,
        /,
        *,
        inplace: bool = True,
        now: _datetime.datetime | _datetime.date | str | int | float | None = None,
    ) -> _pandas.DataFrame:
        from . import _people, _person_pg

        if not inplace:
            df = df.copy(deep=True)

        keys_set = set(self.updates) & _person_pg.VALID_PERSON_UPDATE_KEYS
        _people.update_dataframe_for_updates(
            df,
            updates={k: v for k, v in self.updates.items() if k in keys_set},
            now=now,
        )
        return df

    def load_people_dataframe(
        self,
        *,
        ctx: _context.WsjRdpContext,
        conn: _psycopg.Connection | None = None,
        collection_date: _datetime.date | str | None = None,
        extra_static_df_cols: dict[str, _typing.Any] | None = None,
        extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
        log_resulting_data_frame: bool | None = None,
        limit: int | None | _types.MissingType = _types.MISSING,
        now: _datetime.datetime
        | _datetime.date
        | str
        | int
        | float
        | None
        | _types.MissingType = _types.MISSING,
    ) -> _pandas.DataFrame:
        import textwrap

        import pandas as _pandas

        from . import _people, _util

        limit = _util.coalesce_missing(limit, self.query.limit)
        now = _util.coalesce_missing(collection_date, self.query.collection_date)

        query = self.query.replace(
            limit=limit,
            collection_date=_util.to_date_or_none(collection_date),
            now=_util.to_datetime_or_none(now),
        )
        _LOGGER.debug("BatchConfig.load_people_dataframe")
        _LOGGER.debug("  collection_date: %s", query.collection_date)
        _LOGGER.debug("  now: %s", query.now)

        with _contextlib.ExitStack() as exit_stack:
            if conn is None:
                conn = exit_stack.enter_context(ctx.psycopg_connect())

            df = _people.load_people_dataframe(
                conn,
                query=query,
                extra_static_df_cols=extra_static_df_cols,
                extra_mailing_bcc=extra_mailing_bcc,
                log_resulting_data_frame=False,
            )
            if self.query.email_only_where:
                email_only_where = self.query.email_only_where
                exclude_found_ids = _people_query.PeopleWhere(exclude_id=list(df["id"]))
                email_only_query = query.replace(
                    where=_people_query.PeopleWhere(
                        and_=[email_only_where, exclude_found_ids]
                    )
                )
                email_only_query.email_only_where = None
                df2 = _people.load_people_dataframe(
                    conn,
                    query=email_only_query,
                    extra_static_df_cols=extra_static_df_cols,
                    extra_mailing_bcc=extra_mailing_bcc,
                    log_resulting_data_frame=False,
                    skip_db_updates=True,
                )
                if not df2.empty:
                    self.__mark_df_for_skip_db_updates(df2)
                    df = _pandas.concat(
                        [df, df2], axis=0, ignore_index=True, sort=False
                    )

        if log_resulting_data_frame or (log_resulting_data_frame is None):
            _LOGGER.info(
                "Resulting pandas DataFrame:\n%s", textwrap.indent(str(df), "  ")
            )

        return df

    @staticmethod
    def __mark_df_for_skip_db_updates(df: _pandas.DataFrame) -> None:
        from ._payment import _skip_payment

        for idx, row in df.iterrows():
            _skip_payment(df, idx, "skip_db_updates is True")

    @_util.log_exception_decorator
    def query_people_and_prepare_batch(
        self,
        *,
        ctx: _context.WsjRdpContext,
        conn: _psycopg.Connection | None = None,
        collection_date: _datetime.date | str | None = None,
        extra_static_df_cols: dict[str, _typing.Any] | None = None,
        extra_mailing_bcc: str | _collections_abc.Iterable[str] | None = None,
        df_cb: _collections_abc.Callable[[_pandas.DataFrame], _pandas.DataFrame]
        | None = None,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        out_dir: _pathlib.Path | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
        log_resulting_data_frame: bool | None = None,
        limit: int | None | _types.MissingType = _types.MISSING,
        now: _datetime.datetime
        | _datetime.date
        | str
        | int
        | float
        | None
        | _types.MissingType = _types.MISSING,
    ) -> PreparedBatch:
        from . import _util

        limit = _util.coalesce_missing(limit, self.query.limit)
        now = _util.to_datetime(_util.coalesce_missing(now, self.query.now))

        with _contextlib.ExitStack() as exit_stack:
            if conn is None:
                conn = exit_stack.enter_context(ctx.psycopg_connect())

            df = unfiltered_df = self.load_people_dataframe(
                ctx=ctx,
                conn=conn,
                collection_date=collection_date,
                extra_static_df_cols=extra_static_df_cols,
                extra_mailing_bcc=extra_mailing_bcc,
                log_resulting_data_frame=log_resulting_data_frame,
                limit=limit,
                now=now,
            )
            if df_cb is not None:
                _LOGGER.info("Update dataframe using callback %s", str(df_cb))
                df = df_cb(df)
            if out_dir is None:
                out_dir = ctx.out_dir
            return self.prepare_batch_for_dataframe(
                df,
                unfiltered_df=unfiltered_df,
                msg_cb=msg_cb,
                out_dir=out_dir,
                msgid_idstring=msgid_idstring,
                msgid_domain=msgid_domain,
                now=now,
            )

    def prepare_batch_for_dataframe(
        self,
        df: _pandas.DataFrame,
        *,
        unfiltered_df: _pandas.DataFrame | None = None,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        out_dir: _pathlib.Path | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
        now: _datetime.datetime | _datetime.date | str | int | float | None = None,
        dry_run: bool | None = None,
        skip_email: bool | None = None,
        skip_db_updates: bool | None = None,
    ) -> PreparedBatch:
        import time

        from . import _util

        if dry_run is None:
            dry_run = self.dry_run
        if skip_email is None:
            skip_email = self.skip_email
        if skip_db_updates is None:
            skip_db_updates = self.skip_db_updates

        now = _util.to_datetime(now)
        _LOGGER.info("Update dataframe for updates and action arguments...")
        unfiltered_df_is_df = unfiltered_df is df
        df = self.update_dataframe_for_updates(df, now=now, inplace=False)
        if unfiltered_df_is_df:
            unfiltered_df = df
        if len(df) > 1:
            _LOGGER.info("Prepare mailing...")
        tic = time.monotonic()
        messages = tuple(
            self.prepare_email_message_for_row(
                row,
                msg_cb=msg_cb,
                msgid_idstring=msgid_idstring,
                msgid_domain=msgid_domain,
            )
            for _, row in df.iterrows()
        )
        toc = time.monotonic()
        if len(df) > 5 or (toc - tic) > 0.1:
            _LOGGER.info("  finished preparation of mailing (%g seconds)", toc - tic)
        config_yaml = self.__to_yaml__().encode("utf-8")
        return PreparedBatch(
            name=self.name,
            df=df,
            unfiltered_df=unfiltered_df,
            messages=messages,
            config_yaml=config_yaml,
            action_arguments=self.action_arguments,
            updates=self.updates,
            email_from=self.email_from,
            from_addr=self.from_addr,
            out_dir=out_dir,
            now=now,
            dry_run=dry_run,
            skip_email=skip_email,
            skip_db_updates=bool(_util.coalesce(skip_db_updates, False)),
        )

    def prepare_email_message_for_row(
        self,
        row: _pandas.Series,
        *,
        msg_cb: _collections_abc.Callable[[PreparedEmailMessage], None] | None = None,
        policy: _email_policy.EmailPolicy | None = None,
        msgid_idstring: str | None = None,
        msgid_domain: str | None = None,
    ) -> PreparedEmailMessage:
        from . import _util

        row_dict = _row_to_row_dict(row)

        id = row["id"]
        msg = email_message_from_row(
            row_dict,
            content=self.content,
            html_content=self.html_content,
            signature=self.signature,
            email_subject=self.email_subject,
            email_from=self.email_from or row["mailing_from"],
            email_to=_util.merge_mail_addresses(row["mailing_to"], self.extra_email_to),
            email_cc=_util.merge_mail_addresses(row["mailing_cc"], self.extra_email_cc),
            email_bcc=_util.merge_mail_addresses(
                row["mailing_bcc"], self.extra_email_bcc
            ),
            email_reply_to=self.email_reply_to or row["mailing_reply_to"],
            policy=policy,
            msgid_idstring=msgid_idstring,
            msgid_domain=msgid_domain,
            context={"query": self},
            extra_context=self.jinja_extra_globals,
        )
        prepared = PreparedEmailMessage(
            mailing_name=self.name,
            message=msg,
            row=row,
            eml_name=f"{self.name}.{id}.eml",
            summary=_util.render_template(self.summary, {"row": row_dict, "msg": msg}),
        )
        if msg_cb:
            msg_cb(prepared)
        return prepared


_KEEP_NAN_KEYS = {"amount_paid_cents", "amount_unpaid_cents", "open_amount_cents"}


def _row_to_row_dict(row: _pandas.Series) -> dict[str, _typing.Any]:
    import math

    def _maybe_to_none(key, val):
        if (key in _KEEP_NAN_KEYS) or not isinstance(val, float) or not math.isnan(val):
            return val
        else:
            return None

    row_dict = {str(key): _maybe_to_none(key, val) for key, val in row.items()}
    return row_dict


def _strip_html_tags_builtin(html_content: str) -> str:
    import re
    from html import unescape

    html_content = re.sub(
        "<style.*?</style>", "", html_content, flags=re.DOTALL | re.IGNORECASE
    )
    html_content = re.sub("<br\\s*/?>", "\n", html_content, flags=re.IGNORECASE)
    html_content = re.sub("</p>", "\n", html_content, flags=re.IGNORECASE)
    text_without_tags = re.sub("<[^<]+?>", "", html_content)
    clean_text = unescape(text_without_tags)
    clean_lines = [line.strip() for line in clean_text.splitlines()]
    clean_text = "\n".join(line for line in clean_lines if line)

    return clean_text


def _strip_html_tags_html2text(html_content: str) -> str:
    import html2text

    return html2text.html2text(html_content)


def email_message_from_row(
    row: _pandas.Series | dict[str, _typing.Any],
    *,
    email_subject: str,
    content: str | None = None,
    html_content: str | None,
    signature: str | None = None,
    email_from: str = "anmeldung@worldscoutjamboree.de",
    email_to: str | _collections_abc.Iterable[str] | None = None,
    email_cc: str | _collections_abc.Iterable[str] | None = None,
    email_bcc: str | _collections_abc.Iterable[str] | None = None,
    email_reply_to: str | _collections_abc.Iterable[str] | None = None,
    policy: _email_policy.EmailPolicy | None = None,
    email_date: _datetime.datetime | _datetime.date | str | float | int | None = None,
    message_id: str | None = None,
    msgid_idstring: str | None = None,
    msgid_domain: str | None = None,
    context: dict | None = None,
    extra_context: dict | None = None,
) -> _email_message.EmailMessage | None:
    import email.message
    import email.utils

    import wsjrdp2027

    from . import DEFAULT_MSGID_DOMAIN, DEFAULT_MSGID_IDSTRING, _util

    SIGNATURES = {
        k: v for k, v in wsjrdp2027.__dict__.items() if k.startswith("EMAIL_SIGNATURE_")
    }
    context = context or {}

    if content is None and html_content is None:
        return None

    email_to = _util.to_str_list_or_none(email_to)
    email_cc = _util.to_str_list_or_none(email_cc)
    email_bcc = _util.to_str_list_or_none(email_bcc)
    email_reply_to = _util.to_str_list_or_none(email_reply_to)

    def render_template(template):
        local_context = context.copy()
        local_context |= {
            "row": row,
            "email_from": email_from,
            "email_to": email_to,
            "email_cc": email_cc,
            "email_bcc": email_bcc,
            "email_reply_to": email_reply_to,
            **SIGNATURES,
        }
        return _util.render_template(
            template,
            context=local_context,
            extra_context=extra_context,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    if message_id is None:
        message_id = email.utils.make_msgid(
            idstring=msgid_idstring or DEFAULT_MSGID_IDSTRING,
            domain=msgid_domain or DEFAULT_MSGID_DOMAIN,
        )
    email_date = email_date or row.get("email_date", None)
    email_date = _util.to_datetime(email_date)
    msg_date = email.utils.format_datetime(email_date)

    email_subject = render_template(email_subject)

    if policy is None:
        policy = _util.get_default_email_policy()

    msg = email.message.EmailMessage(policy=policy)
    msg["Subject"] = email_subject
    msg["From"] = email_from
    if email_to is not None:
        msg["To"] = _util.to_str_list_or_none(email_to)
    if email_cc is not None:
        msg["Cc"] = _util.to_str_list_or_none(email_cc)
    if email_bcc is not None:
        msg["Bcc"] = _util.to_str_list_or_none(email_bcc)
    if email_reply_to is not None:
        msg["Reply-To"] = _util.to_str_list_or_none(email_reply_to)
    msg["Date"] = msg_date
    msg["Message-ID"] = message_id

    if not content:
        if html_content:
            content = _strip_html_tags_html2text(html_content)
        else:
            return None
    content = render_template(content)
    if signature:
        signature = render_template(signature).lstrip()
        if not signature.startswith("-- \n"):
            signature = "-- \n" + signature
        content = content.rstrip() + "\n\n" + signature
    msg.set_content(content)

    if html_content:
        html_content = render_template(html_content)
        msg.add_alternative(html_content, subtype="html")

    return msg


def write_data_and_send_mailing(
    ctx: _context.WsjRdpContext,
    mailing: PreparedBatch,
    *,
    dry_run: bool | None = None,
    out_dir: str | _pathlib.Path | None = None,
    zip_eml: bool | None = True,
    silent_skip_email: bool | None = None,
) -> None:
    if zip_eml is None:
        zip_eml = True

    if out_dir:
        out_dir = _pathlib.Path(out_dir)
    elif mailing.out_dir:
        out_dir = mailing.out_dir
    else:
        out_dir_base = _pathlib.Path(ctx.out_dir or "data/mailings")
        out_dir_tpl = str(
            out_dir_base / (mailing.name + "__{{ filename_suffix }}")
        ).replace("\\", "/")
        out_dir = ctx.make_out_path(out_dir_tpl)

    mailing.write_data(
        out_dir=out_dir, zip_eml=zip_eml, silent_skip_email=silent_skip_email
    )

    with ctx.mail_login(
        from_addr=mailing.from_addr,
        dry_run=dry_run,
    ) as mail_client:
        mailing.send(mail_client)


def _normalize_query_where_or_none(
    query: _people_query.PeopleQuery | dict | None = None,
    where: _people_query.PeopleWhere | dict | str | None = None,
    *,
    logger: _logging.Logger | _logging.LoggerAdapter | None = None,
) -> _people_query.PeopleQuery | None:
    if query:
        query = _people_query.PeopleQuery.normalize(query)
        if where:
            if query and query.where:
                err_msg = "Arguments 'where' and 'query.where' are mutually exclusive"
                if logger:
                    logger.error(err_msg)
                raise RuntimeError(err_msg)
            else:
                where = _people_query.PeopleWhere.normalize(where)
                return query.replace(where=where)
        else:
            return query
    else:
        if where:
            return _people_query.PeopleQuery(where=where)
        else:
            return None


def _normalize_query_where(
    query: _people_query.PeopleQuery | dict | None = None,
    where: _people_query.PeopleWhere | dict | str | None = None,
    *,
    logger: _logging.Logger | _logging.LoggerAdapter | None = None,
) -> _people_query.PeopleQuery:
    query = _normalize_query_where_or_none(query, where, logger=logger)
    return query if query is not None else _people_query.PeopleQuery()
