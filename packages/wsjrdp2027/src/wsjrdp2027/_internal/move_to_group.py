from __future__ import annotations

import collections.abc as _collections_abc
import logging as _logging
import typing as _typing

from .. import _context, _groups, _people_query, _person


if _typing.TYPE_CHECKING:
    from .. import _batch


_LOGGER = _logging.getLogger(__name__)


def move_person_to_group(
    ctx: _context.WsjRdpContext | None,
    *,
    person: _person.Person,
    new_group: str | int | _groups.Group,
    batch_name: str | None = None,
    updates: _collections_abc.Mapping | None = None,
    batch_config: _batch.BatchConfig | None = None,
) -> None:
    if ctx is None:
        ctx = _context.get_thread_local_ctx_or_raise()
    if not batch_name:
        batch_name = f"move_to_group_{person.id_and_name}".replace(" ", "_")

    new_group_arg = new_group

    with ctx.as_thread_local_ctx():
        is_yp_or_ul = person.short_role_name in ["YP", "UL"]
        old_group = person.primary_group

        new_group = _groups.Group.to_group(new_group, conn=ctx)
        unit_code: str | None = new_group.unit_code if new_group else None

        if batch_config is not None:
            local_batch_config = batch_config
        else:
            local_batch_config = ctx.new_batch_config(
                name=batch_name,
                where=_people_query.PeopleWhere(id=person.id),
                updates=updates,
            )

        if new_group.id != person.primary_group_id:
            _LOGGER.info(
                f"Set new_primary_group_id={new_group.id} (derived from new_group={new_group_arg})"
            )
            local_batch_config.updates["new_primary_group_id"] = new_group.id
            person.primary_group = new_group
        if note := _confirmation_note(ctx, old_group=old_group, new_group=new_group):
            local_batch_config.updates["add_note"] = note
        if is_yp_or_ul and (unit_code := new_group.unit_code):
            _LOGGER.info(
                f"Set new_unit_code={unit_code!r} (derived from new_group={new_group_arg})"
            )
            if unit_code != person.unit_code:
                local_batch_config.updates["new_unit_code"] = unit_code
        _LOGGER.debug("Query:\n%s", local_batch_config.query)

        if batch_config is None:
            prepared_batch = ctx.load_people_and_prepare_batch(
                local_batch_config,
                log_resulting_data_frame=False,
                report_all_updates=True,
            )
            ctx.update_db_and_send_mailing(prepared_batch, silent_skip_email=True)


def _confirmation_note(
    ctx: _context.WsjRdpContext,
    *,
    old_group: _groups.Group | None,
    new_group: _groups.Group,
) -> str:
    date_str = ctx.today.strftime("%d.%m.%Y")
    old_group_name = (old_group.short_name or old_group.name) if old_group else ""
    new_group_name = new_group.short_name or new_group.name

    if old_group is not None:
        if old_group.id != new_group.id:
            move_str = f"von {old_group_name} nach {new_group_name} verschoben"
        else:
            move_str = ""
    else:
        move_str = f"nach {new_group_name} verschoben"

    if move_str:
        return f"Am {date_str} {move_str}"
    else:
        return ""
