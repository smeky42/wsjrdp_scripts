#!/usr/bin/env -S uv run
from __future__ import annotations

import pathlib as _pathlib

import wsjrdp2027


_SELF_NAME = _pathlib.Path(__file__).stem


_TO_BMT_TAG = "zu-BMT-umziehen"
_IST_TO_BMT_TAG = "von-IST-zu-BMT-umgezogen"

# IST noch nicht BMT und nicht auf Warteliste
_IST_GROUPS_WO_BMT_WO_WAITING = [4, 49, 50, 51, 52]


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--keycloak-dry-run", action="store_true", default=None)
    return p


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    )

    with ctx:
        out_base = ctx.make_out_path(_SELF_NAME + "__{{ filename_suffix }}")
        log_filename = out_base.with_suffix(".log")
        ctx.configure_log_file(log_filename)

        where = wsjrdp2027.PeopleWhere(
            tag=_TO_BMT_TAG,
            role="IST",
            status="confirmed",
            primary_group_id=_IST_GROUPS_WO_BMT_WO_WAITING,
        )
        query = wsjrdp2027.PeopleQuery(where=where)
        updates = {
            "add_tags": _IST_TO_BMT_TAG,
            "remove_tags": _TO_BMT_TAG,
        }
        batch_config = ctx.new_batch_config(
            query=query,
            updates=updates,
        )
        prepared_batch = ctx.load_people_and_prepare_batch(
            batch_config, log_resulting_data_frame=False
        )

        num_people = len(prepared_batch.messages)

        ctx.logger.info("")
        ctx.logger.info(f"Found {num_people} IST to be moved to BMT")
        ctx.logger.info("")

        for p in prepared_batch.iter_people():
            ctx.logger.info(f"Move {p.role_id_name}")
            p.move_to_group(
                "BMT",
                updates=updates,
                batch_name=f"{out_base.name}_{p.id_and_name}_move".replace(" ", "_"),
            )

        # ctx.logger.info("Load mailcow aliases to fill cache")
        # _aliases = ctx.mailcow().get_alias_list()
        # ctx.logger.info(f"  ... loaded {len(_aliases)} aliases from mailcow")

        ctx.logger.info("Load keycloak users to fill cache")
        _users = ctx.keycloak().get_user_list(allow_cached=False)
        ctx.logger.info(f"  ... loaded {len(_users)} users from keycloak")

        additional_info_updates = []
        for p in prepared_batch.iter_people():
            expected_bmt_keycloak_username = p.get_keycloak_username_expected("BMT")
            expected_ist_keycloak_username = p.get_keycloak_username_expected("IST")
            if (
                p.keycloak_username
                and p.keycloak_username == expected_ist_keycloak_username
                and p.keycloak_username != expected_bmt_keycloak_username
            ):
                user_dict = ctx.keycloak().get_user_or_none_by_name(p.keycloak_username)
                if user_dict and user_dict["enabled"]:
                    ctx.logger.info(
                        f"Disable old IST keycloak account {p.keycloak_username}"
                    )
                    ctx.keycloak().disable_user(p.keycloak_username)
                p.keycloak_username = expected_bmt_keycloak_username
            expected_bmt_wsjrdp_email = p.get_wsjrdp_email_expected("BMT")
            expected_ist_wsjrdp_email = p.get_wsjrdp_email_expected("IST")
            if (
                p.wsjrdp_email
                and p.wsjrdp_email == expected_ist_wsjrdp_email
                and p.wsjrdp_email != expected_bmt_wsjrdp_email
            ):
                p.wsjrdp_email = expected_bmt_wsjrdp_email
            additional_info_updates.extend(p.additional_info_updates_list())
        ctx.update_people_additional_info(additional_info_updates, console_confirm=True)

        ctx.sync_hitobito_keycloak_mailcow(
            prepared_batch.iter_people(),
            keycloak_groupname="BMT",
            create_missing_keycloak_user=True,
            self_name=f"{out_base.name}",
            batch_name_suffix="_sync",
        )
        ctx.logger.info("")
        ctx.logger.info(f"Moved {num_people} IST to BMT")
        ctx.logger.info("")


if __name__ == "__main__":
    __import__("sys").exit(main())
