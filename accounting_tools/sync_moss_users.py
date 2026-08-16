#!/usr/bin/env -S uv run
"""Sync Moss user attributes into Hitobito ``people.additional_info``.

Reads a Moss user-export CSV with the columns

    name, status, email, phone, team, role

and, for every row whose *email* matches a Hitobito person's
``additional_info->>'moss_email'`` (case-insensitive), sets/updates these
``additional_info`` fields on that person:

  * ``moss_status``  <- ``status``  (e.g. ACTIVE / INVITED / DEACTIVATED)
  * ``moss_phone``   <- ``phone``
  * ``moss_team``    <- ``team``
  * ``moss_roles``   <- ``role``    (whitespace-split into a list of tokens,
                                     e.g. ``"TEAMLEAD USER"`` -> ``["TEAMLEAD",
                                     "USER"]``)

The CSV is read as cp1252 (the Moss export encoding). An empty CSV value clears
the corresponding field (the fields mirror the export). Only actually-changed
fields are written, and a PaperTrail version is recorded per change. CSV rows
whose email matches no Hitobito person are reported and skipped.

Safety:
  * ``--dry-run`` computes and reports the changes but writes nothing.
  * Non-production runs ask for confirmation before writing.
  * Production runs additionally require the ``require_approval_to_run_in_prod``
    gate.

This tool reads from and (unless dry-run) writes ``additional_info`` back to the
Hitobito database; it changes nothing else.
"""

from __future__ import annotations

import collections
import csv
import dataclasses
import logging
import pathlib
import sys
import typing as _typing

import wsjrdp2027


if _typing.TYPE_CHECKING:
    from wsjrdp2027 import PgConnectionLike

_SELF_NAME = pathlib.Path(__file__).stem
_LOGGER = logging.getLogger(__name__)

# The Moss export is Windows-1252 encoded (e.g. "ö" as byte 0xF6).
_CSV_ENCODING = "cp1252"

_CSV_COLUMNS = ("name", "status", "email", "phone", "team", "role")

# additional_info field <- Moss user attribute.
_FIELDS = ("moss_status", "moss_phone", "moss_team", "moss_roles")


@dataclasses.dataclass(frozen=True)
class _MossUser:
    """One row of the Moss user-export CSV."""

    name: str
    status: str
    email: str
    phone: str
    team: str
    roles: list[str]

    @classmethod
    def from_row(cls, row: dict) -> _MossUser:
        def s(key: str) -> str:
            return (row.get(key) or "").strip()

        return cls(
            name=s("name"),
            status=s("status"),
            email=s("email"),
            phone=s("phone"),
            team=s("team"),
            roles=s("role").split(),
        )

    @property
    def email_key(self) -> str:
        return self.email.lower()


@dataclasses.dataclass
class _Stats:
    matched: int = 0
    unmatched: int = 0
    ambiguous: int = 0
    changed: int = 0
    unchanged: int = 0
    per_field: collections.Counter = dataclasses.field(
        default_factory=collections.Counter
    )


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser(description="Sync a Moss user CSV into Hitobito.")
    p.add_argument("csv_file", help="Moss user-export CSV file (cp1252).")
    p.add_argument(
        "--limit",
        type=lambda s: int(s, base=10),
        default=None,
        help="Only process the first N CSV rows (useful for testing).",
    )
    return p


def read_moss_users(path: str | pathlib.Path) -> list[_MossUser]:
    with open(path, encoding=_CSV_ENCODING, newline="") as f:
        reader = csv.DictReader(f)
        missing = set(_CSV_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(
                f"CSV {path} is missing column(s): {sorted(missing)} "
                f"(found {reader.fieldnames})"
            )
        users = [_MossUser.from_row(row) for row in reader]
    return [u for u in users if u.email]


def load_people_by_moss_email(
    conn: PgConnectionLike,
) -> dict[str, list[wsjrdp2027.Person]]:
    """Map lower-cased ``moss_email`` -> Hitobito people that carry it."""
    rows = wsjrdp2027.pg_select_dict_rows(
        conn,
        t"SELECT id, first_name, last_name, status, additional_info "
        t"FROM people WHERE additional_info->>'moss_email' IS NOT NULL",
    )
    by_email: dict[str, list[wsjrdp2027.Person]] = collections.defaultdict(list)
    for row in rows:
        person = wsjrdp2027.Person(**row)
        email = (person.moss_email_or_none or "").strip().lower()
        if email:
            by_email[email].append(person)
    return by_email


def compute_updates(
    users: list[_MossUser],
    people_by_email: dict[str, list[wsjrdp2027.Person]],
) -> tuple[list[dict], _Stats]:
    """Apply the desired Moss fields to matched people and collect the changes."""
    stats = _Stats()
    updates: list[dict] = []
    for user in users:
        people = people_by_email.get(user.email_key)
        if not people:
            stats.unmatched += 1
            _LOGGER.warning(
                "No Hitobito person with moss_email=%s (Moss user %r)",
                user.email,
                user.name,
            )
            continue
        if len(people) > 1:
            stats.ambiguous += 1
            _LOGGER.warning(
                "moss_email=%s matches %s Hitobito people %s - skipping",
                user.email,
                len(people),
                [p.id for p in people],
            )
            continue

        stats.matched += 1
        person = people[0]
        # Empty values become None, which clears the field (mirror the CSV).
        person.moss_status = user.status or None
        person.moss_phone = user.phone or None
        person.moss_team = user.team or None
        person.moss_roles = user.roles or None

        person_updates = person.additional_info_updates_list()
        if not person_updates:
            stats.unchanged += 1
            continue
        stats.changed += 1
        updates.extend(person_updates)
        changed = {
            key: old_new
            for upd in person_updates
            for key, old_new in upd.items()
            if key != "id"
        }
        for key in changed:
            stats.per_field[key] += 1
        _LOGGER.info("%s %s: %s", person.id, person.full_name, changed)
    return updates, stats


def _log_stats(stats: _Stats) -> None:
    _LOGGER.info("")
    _LOGGER.info(
        "Matched %s, changed %s, unchanged %s, unmatched %s, ambiguous %s",
        stats.matched,
        stats.changed,
        stats.unchanged,
        stats.unmatched,
        stats.ambiguous,
    )
    for field in _FIELDS:
        _LOGGER.info("  %-12s changes: %s", field, stats.per_field.get(field, 0))


def main(argv=None) -> int:
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        __file__=__file__,
    )
    out_base = ctx.make_out_path(_SELF_NAME + "__{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    csv_file = ctx.parsed_args.csv_file
    users = read_moss_users(csv_file)
    if ctx.parsed_args.limit is not None:
        users = users[: ctx.parsed_args.limit]
    _LOGGER.info("Read %s Moss user(s) from %s", len(users), csv_file)

    with ctx:
        conn = ctx.hitobito_psycopg_connection(read_only=True)
        people_by_email = load_people_by_moss_email(conn)
        _LOGGER.info(
            "Loaded %s Hitobito person/people with a moss_email",
            sum(len(v) for v in people_by_email.values()),
        )

        updates, stats = compute_updates(users, people_by_email)
        _log_stats(stats)

        if not updates:
            _LOGGER.info("Nothing to write.")
            return 0

        if ctx.dry_run:
            _LOGGER.warning(
                "DRY RUN - not writing %s field update(s) across %s people.",
                len(updates),
                stats.changed,
            )
            return 0

        ctx.require_approval_to_run_in_prod(
            prompt=(
                f"Apply {len(updates)} additional_info field update(s) "
                f"to {stats.changed} people?"
            )
        )
        ctx.update_people_additional_info(updates, console_confirm=True)
        _LOGGER.info(
            "Applied %s field update(s) across %s people.", len(updates), stats.changed
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
