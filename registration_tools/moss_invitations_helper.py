#!/usr/bin/env -S uv run
from __future__ import annotations

import itertools

import wsjrdp2027


_LOGGER = __import__("logging").getLogger(__name__)


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()

    p.add_argument("--role")
    p.add_argument(
        "--group",
        help="""Name or id of the group the confirmed person should be moved to.""",
    )
    p.add_argument(
        "--skip-uninvited",
        action="store_true",
        default=False,
        help="""Skip people still uninvited (useful to check query before actual invitation).""",
    )
    return p


IST_ID_FIRST_MEETING = [
    5, 6, 136, 223, 228, 237, 238, 241, 244, 245, 249, 251, 253, 255, 256, 257, 258,
    271, 277, 291, 297, 309, 314, 323, 324, 327, 330, 331, 332, 350, 351, 355, 365, 369,
    371, 378, 380, 381, 385, 395, 401, 418, 421, 432, 435, 437, 439, 473, 474, 528, 557,
    578, 584, 652, 688, 700, 703, 704, 714, 715, 716, 724, 727, 747, 750, 771, 799, 830,
    841, 843, 855, 857, 859, 871, 887, 903, 905, 906, 1012, 1013, 1024, 1026, 1033,
    1035, 1063, 1065, 1079, 1106, 1112, 1163, 1182, 1195, 1196, 1202, 1208, 1218, 1292,
    1294, 1336, 1350, 1360, 1362, 1387, 1393, 1422, 1462, 1560, 1575, 1587, 1606, 1625,
    1658, 1697, 1715, 1748, 1792, 1820, 1821, 1854, 1863, 1870, 1878, 1886, 1890, 1902,
    1957, 1967, 1976, 1993, 1994, 1995, 2006, 2096, 2107, 2131, 2154, 2181, 2189, 2190,
    2191, 2208, 2231, 2247, 2268, 2281, 2289, 2304, 2312, 2317, 2334, 2353, 2354, 2358,
    2359, 2365, 2367, 2368, 2372, 2374, 2377, 2380, 2385, 2387, 2403, 2415, 2437, 2440,
    2447, 2449, 2451, 2453
]  # fmt: skip


def build_people_where(ctx: wsjrdp2027.WsjRdpContext, conn) -> wsjrdp2027.PeopleWhere:
    if ctx.parsed_args.group:
        group = wsjrdp2027.Group.db_load(conn, ctx.parsed_args.group)
        primary_group_id = group.id
    else:
        primary_group_id = None
    if ctx.parsed_args.role:
        role = ctx.parsed_args.role
    else:
        role = None
    return wsjrdp2027.PeopleWhere(
        status="confirmed",
        exclude_deregistered=True,
        exclude_primary_group_id=[5, 6, 7],
        role=role,
        primary_group_id=primary_group_id,
        # role="CMT",
        # tag="Finanzverantwortlich",
        # role="UL",
        # status="confirmed",
        # tag='eHoC',
        # tag='Unit-Manager',
        # role="IST",
        # primary_group_id=10,
        # id=[2508, 2509],
        # primary_group_id=[9, 24],  # 9 - A2, 24 - A8
        # status="confirmed"
        # role="IST",
        # id=IST_ID_FIRST_MEETING,
    )


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        __file__=__file__,
        # start_time="2026-01-12",
        # dry_run=True,
    )
    skip_uninvited = bool(ctx.parsed_args.skip_uninvited)

    out_base = ctx.make_out_path("moss_invitations__{{ filename_suffix }}")
    log_filename = out_base.with_suffix(".log")
    ctx.configure_log_file(log_filename)

    with ctx.psycopg_connect() as conn:
        batch_config = wsjrdp2027.BatchConfig(where=build_people_where(ctx, conn))
        df = batch_config.load_people_dataframe(
            ctx=ctx, conn=conn, log_resulting_data_frame=False
        )

        people = list(wsjrdp2027.iter_people_dataframe(df))
        print(len(people))
        wsjrdp2027.load_primary_groups_for_people(conn, people=people)

    people = sorted(people, key=lambda p: (p.unit_or_role, p.id))
    print()
    wsjrdp2027.moss.ensure_moss_email_mailbox_or_alias(ctx, people=people)

    for unit_or_role, people_in_group in itertools.groupby(
        people, key=lambda p: p.unit_or_role
    ):
        people_in_group = list(people_in_group)
        invited = []
        uninvited = []
        for p in people_in_group:
            if p.moss_invited_at:
                invited.append(p)
            else:
                uninvited.append(p)
        print()
        print("===", unit_or_role, "===")

        for p in people_in_group:
            print(
                f"{unit_or_role} {p.role_id_name} / {wsjrdp2027.moss.moss_email_with_expected_goto(p)}"
            )
            if p.moss_invited_at:
                print(f"    moss_invited_at={p.moss_invited_at.isoformat()}")

        if uninvited:
            print(f"")
            print(f"    E-Mails to invite({len(uninvited)}):")
            for i_outer, outer_batch in enumerate(itertools.batched(uninvited, 50)):
                if i_outer > 0:
                    print()
                for uninvited_part in itertools.batched(outer_batch, 2):
                    print(f"   ", " ".join(p.moss_email for p in uninvited_part))
            if skip_uninvited:
                print("    **SKIP** (--skip-uninvited given)")
            else:
                now = ctx.start_time
                print()
                if wsjrdp2027.console_confirm(
                    f"    Invited to Moss (moss_invited_at={now.date().isoformat()})?"
                ):
                    with ctx.psycopg_connect() as conn:
                        wsjrdp2027.pg.pg_update_people_additional_info(
                            conn,
                            [{"id": p.id, "moss_invited_at": now} for p in uninvited],
                        )
                        conn.commit()

    _LOGGER.info("")
    _LOGGER.info("Output directory: %s", ctx.out_dir)
    _LOGGER.info("  Log file: %s", log_filename)


if __name__ == "__main__":
    __import__("sys").exit(main())
