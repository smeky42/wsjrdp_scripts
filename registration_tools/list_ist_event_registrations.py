#!/usr/bin/env -S uv run
from __future__ import annotations

import json

import pandas as pd
import wsjrdp2027


def second_ist_meeting(events: list[str | None]) -> str:
    return next(
        (event for event in events if event and event.startswith("2. IST Vorbereitungstreffen")),
        "",
    )


def main() -> None:
    ctx = wsjrdp2027.WsjRdpContext(__file__=__file__)
    with ctx:
        with ctx.hitobito_psycopg_connection(read_only=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                """
                SELECT people.id, people.first_name, people.last_name,
                       people.status, groups.id, groups.name,
                       COALESCE((
                           SELECT array_agg(t.name ORDER BY t.name)
                           FROM event_participations AS ep
                           JOIN event_translations AS t
                             ON t.event_id = ep.event_id AND t.locale = 'de'
                           WHERE ep.person_id = people.id AND ep.active = TRUE
                         ), ARRAY[]::text[])
                FROM people
                JOIN groups ON groups.id = people.primary_group_id
                WHERE groups.type = 'Group::Ist'
                                    AND EXISTS (
                                            SELECT 1 FROM roles
                                            WHERE roles.person_id = people.id
                                                AND roles.group_id = groups.id
                                                AND roles.type = 'Group::Ist::Member'
                                                AND roles.archived_at IS NULL
                                                AND roles.terminated = FALSE
                                    )
                ORDER BY people.last_name, people.first_name, people.id
                """
                
                )
                rows = cursor.fetchall()

        data = [
            (
                person_id,
                first_name,
                last_name,
                status,
                f"{ctx.config.hitobito_url.rstrip('/')}/groups/{group_id}/people/{person_id}",
                group_id,
                group_name,
                json.dumps(events, ensure_ascii=False),
                second_ist_meeting(events),
            )
            for person_id, first_name, last_name, status, group_id, group_name, events in rows
        ]
        columns = [
            "ID", "Vorname", "Nachname", "Status", "Profillink", "GruppenID", "Gruppe", "Events",
            "2. IST Vorbereitungstreffen",
        ]
        out_path = ctx.make_out_path("list_ist_event_registrations__{{ filename_suffix }}.xlsx")
        with pd.ExcelWriter(
            out_path, engine="xlsxwriter", engine_kwargs={"options": {"strings_to_formulas": False}}
        ) as writer:
            pd.DataFrame(data, columns=columns).to_excel(writer, index=False)
        print(f"Excel-Datei erstellt: {out_path}")


if __name__ == "__main__":
    main()