from __future__ import annotations

import logging as _logging
import pprint as _pprint

from .. import _context, _groups, _people_query, _person


_LOGGER = _logging.getLogger(__name__)


def move_person_to_group(
    ctx: _context.WsjRdpContext | None,
    *,
    person: _person.Person,
    new_group: str | int | _groups.Group,
    batch_name: str | None = None,
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

        # create new batch config
        batch_config = ctx.new_batch_config(
            name=batch_name,
            where=_people_query.PeopleWhere(id=person.id),
        )

        if new_group.id != person.primary_group_id:
            _LOGGER.info(
                f"Set new_primary_group_id={new_group.id} (derived from new_group={new_group_arg})"
            )
            batch_config.updates["new_primary_group_id"] = new_group.id
        if note := _confirmation_note(ctx, old_group=old_group, new_group=new_group):
            batch_config.updates["add_note"] = note
        if is_yp_or_ul and (unit_code := new_group.unit_code):
            _LOGGER.info(
                f"Set new_unit_code={unit_code!r} (derived from new_group={new_group_arg})"
            )
            if unit_code != person.unit_code:
                batch_config.updates["new_unit_code"] = unit_code
        _LOGGER.info("Query:\n%s", batch_config.query)

        print(flush=True)
        _LOGGER.info(person.role_id_name)
        if batch_config.updates:
            _LOGGER.info("Updates:\n%s", _pprint.pformat(batch_config.updates))
        else:
            _LOGGER.info("Updates: %r", batch_config.updates)
        print(flush=True)

        prepared_batch = ctx.load_people_and_prepare_batch(
            batch_config, log_resulting_data_frame=False
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
