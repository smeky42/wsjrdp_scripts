#!/usr/bin/env -S uv run
from __future__ import annotations

import wsjrdp2027


_LOGGER = __import__("logging").getLogger(__name__)


def main():
    ctx = wsjrdp2027.WsjRdpContext(
        parse_arguments=False,
        __file__=__file__,
        # start_time="2026-01-12",
        # dry_run=True,
    )
    out_base = ctx.make_out_path("moss_invitations__{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)
    batch_config = wsjrdp2027.BatchConfig(
        where=wsjrdp2027.PeopleWhere(
            # id=2480,
            role="UL", tag="Finanzverantwortlich", status="confirmed"
            exclude_primary_group_id=[5, 6, 7],
        )
    )

    all_mail_aliases = wsjrdp2027.mailbox.get_aliases(ctx)
    addr2goto = {
        a["address"]: frozenset(a["goto"].split(",")) for a in all_mail_aliases
    }
    with ctx.psycopg_connect() as conn:
        df = batch_config.load_people_dataframe(
            ctx=ctx, conn=conn, log_resulting_data_frame=False
        )

        people = list(wsjrdp2027.iter_people_dataframe(df))
        _groups_list = wsjrdp2027.Group.load_for_group_ids(
            conn, (p.primary_group_id for p in people)
        )
        groups = {g.id: g for g in _groups_list}

    people = sorted(people, key=lambda p: groups[p.primary_group_id].name)
    print()
    for p in people:
        now = ctx.start_time
        group = groups[p.primary_group_id]
        group_or_role = group.name if p.is_ul else p.short_role_name
        if expected_goto := p.moss_email_expected_goto:
            print(group_or_role, p.moss_email, "->", expected_goto)
            goto_addrs = set(s.lower() for s in addr2goto.get(p.moss_email, set()))
            if expected_goto.lower() not in goto_addrs:
                print(f"    ! Missing alias {p.moss_email} -> {expected_goto}")
                print(f"    {group.name}  status: {p.status}")
                if wsjrdp2027.console_confirm("Add missing mail alias?"):
                    wsjrdp2027.mailbox.add_alias(ctx, p.moss_email, goto=expected_goto)
        else:
            print(group_or_role, p.moss_email)
        if p.moss_invited_at:
            print(f"    moss_invited_at={p.moss_invited_at.isoformat()}")
        elif wsjrdp2027.console_confirm(
            f"Invited to Moss (moss_invited_at={now.date().isoformat()})?"
        ):
            with ctx.psycopg_connect() as conn:
                wsjrdp2027.pg.pg_update_people_additional_info(
                    conn, [{"id": p.id, "moss_invited_at": now}]
                )
                conn.commit()
        print()

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    __import__("sys").exit(main())
