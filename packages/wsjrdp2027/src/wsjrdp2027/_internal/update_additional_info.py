from __future__ import annotations

import collections.abc as _collections_abc
import logging as _logging

import pandas as _pandas

from .. import _context, _people_query, _person, _util


_LOGGER = _logging.getLogger(__name__)


def update_people_additional_info(
    ctx: _context.WsjRdpContext,
    updates: _collections_abc.Iterable[_collections_abc.Mapping],
    *,
    console_confirm: bool = False,
    write_versions: bool = True,
) -> None:
    import pprint

    from .. import _pg, _util

    updates = list(updates)

    if not updates:
        _LOGGER.info("No updates to people additional_info")
        return
    if console_confirm:
        _LOGGER.info(f"Updates to additional_info to apply:\n{pprint.pformat(updates)}")
        if not _util.console_confirm("Update additional_info in DB?"):
            _LOGGER.info("!! Skipped updates to additional_info")
            return
    updates_by_key: dict[str, list[dict]] = {}
    versions: list[tuple[int, dict]] = []
    for upd in updates:
        p_id = upd["id"]
        for k, v in upd.items():
            if k != "id":
                _, new = v
                updates_by_key.setdefault(k, []).append({"id": p_id, k: new})
                if k not in ("keycloak_initial_password",):
                    versions.append((p_id, {k: v}))

    conn = ctx.hitobito_psycopg_connection(read_only=False)
    for key, values in updates_by_key.items():
        if key in ("wsjrdp_email", "moss_email"):
            _LOGGER.debug(f"Update {key} for {len(values)} people")
            _pg.pg_update_people_additional_info_email(conn, key, values)
        else:
            _LOGGER.debug(f"Update {key} for {len(values)} people")
            _pg.pg_update_people_additional_info(conn, values)
    if write_versions and versions:
        with conn.cursor() as cursor:
            for main_id, changes in versions:
                _pg.pg_insert_version(cursor, main_id=main_id, changes=changes)
