"""Automatic linking of DATEV fee bookings to their accounting entry.

A participant fee exists twice in the database: as an ``accounting_entries``
row (the "Beitragsbuchung" -- its subject is the person, it may point at a
pre-notification and/or at the bank transaction it was paid by) and as a
``datev_bookings`` row imported from the tax office's DTVF Buchungsstapel
export. This module holds the rules that pair the two automatically, the SQL
they are made of, and the batched UPDATE that writes the link.

The link lives ON THE ENTRY: ``accounting_entries.datev_booking_id`` plus the
JSON ``datev_booking_link_meta`` (``created_at``, ``author_id`` = 1 (system),
``score``, ``automatic_manual`` = ``"automatic"``, ``classification_string``).
A booking carries no person of its own -- its person is the entry's subject.

Three rules, applied in this order, each seeing only what its predecessors
left unlinked:

1. :func:`match_2025_fee_entries` (:data:`LINK_TYPE_2025_FEE`) -- 2025 fee
   bookings, by the person id in the Buchungstext + signed amount + exact
   booking date.
2. :func:`match_pre_notification_fee_entries`
   (:data:`LINK_TYPE_PRE_NOTIFICATION`) -- fee bookings after 2025, by the
   pre-notification id in Belegfeld 1, with the person id as verification.
3. :func:`match_return_fee_entries` (:data:`LINK_TYPE_RETURN`) -- returned
   ("Retoure") fee bookings of EVERY financial year, against the accounting
   entry of a returned bank transaction by exact booking date + signed amount.

:func:`mirror_camt_links` then mirrors the linked entry's bank transaction onto
``wsjrdp_camt_transactions.datev_booking_id``.

All three rules are set-based: the bookings handed to them are matched in ONE
query and written in ONE bundled UPDATE, so a rule costs a fixed handful of
statements regardless of how many bookings it links. All three link only what
is unambiguous on BOTH sides, all three ignore an entry that is already linked
or flagged ``excluded_from_fee_reconciliation`` (see
:data:`FREE_ENTRY_CONDITION`), and all three are idempotent: a second run over
the same bookings links nothing more.

Users: ``accounting_tools/import_datev_buchungsstapel.py`` (per imported file)
and ``accounting_tools/one-shots/link_datev_return_bookings.py`` (the Retouren
rule over every booking of the database).
"""

from __future__ import annotations

import logging as _logging
import re as _re
import typing as _typing


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc
    import datetime as _datetime
    import decimal as _decimal
    import uuid as _uuid

    _Guids = _collections_abc.Sequence[_uuid.UUID]
    _Candidates = _collections_abc.Mapping[
        str, tuple[str, _collections_abc.Sequence[_typing.Any]]
    ]


_LOGGER = _logging.getLogger(__name__)


class SqlResult(_typing.Protocol):
    """What running a statement gives back: rows to fetch."""

    def fetchall(self) -> list[_typing.Any]: ...


class SqlExecutor(_typing.Protocol):
    """Anything that runs one of this module's statements -- a psycopg cursor
    or connection. The queries are composed from the module's own literals, so
    the ``query`` argument is deliberately untyped (as elsewhere in the
    package); a structural protocol keeps the rules testable with a recording
    fake."""

    def execute(
        self, query: _typing.Any, params: _typing.Any = ..., /
    ) -> SqlResult: ...


class SqlCursor(SqlExecutor, SqlResult, _typing.Protocol):
    """An executor that also reports how many rows the last statement touched
    -- what the linking UPDATEs need."""

    @property
    def rowcount(self) -> int: ...


BOOKINGS_TABLE = "datev_bookings"
BATCHES_TABLE = "datev_booking_batches"

#: The SKR42 participant-fee account (Gegenkonto) and cost center every fee
#: booking of the three rules carries.
FEE_OFFSETTING_ACCOUNT_NUMBER = "41030"
FEE_COST_CENTER_NUMBER = "9500"

#: classification_string of :func:`match_2025_fee_entries`.
LINK_TYPE_2025_FEE = "2025_fee_booking"

#: classification_string of :func:`match_pre_notification_fee_entries`.
LINK_TYPE_PRE_NOTIFICATION = "document_field_1_pre_notification"

#: classification_string of :func:`match_return_fee_entries`.
LINK_TYPE_RETURN = "retoure_matching_camt_return_by_amount_and_date"

#: Confidence written into datev_booking_link_meta.score. The three rules are
#: deterministic and import-equivalent, hence 1.0 (100 %); a rule may pass its
#: own confidence to :func:`link_entries`.
LINK_SCORE = 1.0

#: author_id of an automatic link: the system person.
LINK_AUTHOR_ID = 1

#: Half-width (in days) of the uniqueness window of the Retouren rule: a pair
#: only survives when its amount is unique on BOTH sides within +/- this many
#: days around the booking date (inclusive).
RETURN_LINK_WINDOW_DAYS = 14

#: What makes an accounting entry available to a rule, as an SQL condition on
#: an ``accounting_entries`` row aliased ``ae``: not linked yet, and not
#: excluded from the fee reconciliation by hand. All three rules use it -- the
#: Retouren rule also for its window counts, so an excluded entry neither
#: matches nor makes another pair ambiguous.
FREE_ENTRY_CONDITION = (
    "ae.datev_booking_id IS NULL"
    " AND COALESCE((ae.additional_info ->>"
    " 'excluded_from_fee_reconciliation')::boolean, false) = false"
)

# Person number embedded in a fee Buchungstext, e.g. "CMT 11" / "YP 4711".
_PERSON_IN_TEXT_RE = _re.compile(r"\b(?:BMT|CMT|IST|TN|UL|YP)\s+(\d+)\b")

# Regular fee Belegfeld 1, e.g. "Einzug-2026-01-RCUR-4-1717"; the trailing block
# is the wsjrdp_direct_debit_pre_notifications id.
_RE_EINZUG_PRENOTIF = _re.compile(r"^Einzug-\d{4}-\d{2}-[A-Z]{4}-\d+-(\d+)$")


def parse_person_id(text: str | None) -> int | None:
    """The person id embedded in a fee Buchungstext, or None when the text
    carries none.

    >>> parse_person_id("Beitrag YP 4711 Rate 3")
    4711
    >>> parse_person_id("Beitrag TN 4711 Rate 3")
    4711
    >>> parse_person_id("Beitrag BMT 42 Rate 1")
    42
    >>> parse_person_id("Sammelbuchung Beitraege")
    >>> parse_person_id(None)
    """
    match = _PERSON_IN_TEXT_RE.search(text or "")
    return int(match.group(1)) if match else None


def parse_pre_notification_id(document_field_1: str | None) -> int | None:
    """The wsjrdp_direct_debit_pre_notifications id in a regular fee Belegfeld 1
    ('Einzug-YYYY-MM-<SEQ>-<n>-<prenotif_id>'), or None for a Belegfeld 1 of any
    other shape -- which is what marks a booking as NOT a pre-notification fee
    booking.

    >>> parse_pre_notification_id("Einzug-2026-01-RCUR-4-1717")
    1717
    >>> parse_pre_notification_id("Einzug-2026-01-RCUR-4")
    >>> parse_pre_notification_id("Rechnung 4711")
    >>> parse_pre_notification_id(None)
    """
    match = _RE_EINZUG_PRENOTIF.match(document_field_1 or "")
    return int(match.group(1)) if match else None


def amount_to_cents(amount: _decimal.Decimal) -> int:
    """A DATEV base amount (signed EUR Decimal) as integer cents -- the unit of
    accounting_entries.amount_cents.

    >>> from decimal import Decimal
    >>> amount_to_cents(Decimal("-123.45"))
    -12345
    >>> amount_to_cents(Decimal("57"))
    5700
    """
    return int(round(amount * 100))


def all_booking_guids(conn: SqlExecutor) -> list[_uuid.UUID]:
    """The buchungs_guid of every booking of the database -- the scope of a
    retroactive run of a rule over the whole table (as opposed to the file an
    importer just read). Read-only, so a read-only connection suffices."""
    rows = conn.execute(f"SELECT buchungs_guid FROM {BOOKINGS_TABLE}").fetchall()
    return [guid for (guid,) in rows]


def select_unique_pairs(
    cur: SqlCursor, candidates: _Candidates, *, join: str
) -> list[tuple[int, int]]:
    """Match a whole candidate batch against accounting_entries in ONE query and
    return the UNAMBIGUOUS (entry_id, booking_id) pairs.

    ``candidates`` is an ordered ``{column: (pg_type, values)}`` mapping of the
    equally long candidate arrays, one of them named ``booking_id``. They are
    handed to the database as ``unnest(%s::<type>[], ...)``, aliased ``c``, and
    joined against accounting_entries by the rule's own ``join`` condition. The
    entry must additionally be free (:data:`FREE_ENTRY_CONDITION`).

    Uniqueness is SYMMETRIC: a pair survives only when its booking hits exactly
    one entry AND its entry is hit by exactly one booking of the batch, so two
    bookings competing for one entry leave BOTH unlinked instead of awarding the
    entry to whichever booking a sequential first-wins match happened to reach
    first. On the real data no such competition occurs, so both readings select
    the same pairs.

    Issues no statement (and returns no pair) for an empty batch."""
    columns = list(candidates)
    arrays = [values for _pg_type, values in candidates.values()]
    if not arrays or not arrays[0]:
        return []
    unnest = ", ".join(f"%s::{candidates[name][0]}[]" for name in columns)
    cur.execute(
        f"WITH cand ({', '.join(columns)}) AS (SELECT * FROM unnest({unnest}))"
        ", hit AS ("
        " SELECT c.booking_id, ae.id AS entry_id FROM cand c"
        f" JOIN accounting_entries ae ON {join}"
        f" WHERE {FREE_ENTRY_CONDITION})"
        ", counted AS ("
        " SELECT booking_id, entry_id,"
        " count(*) OVER (PARTITION BY booking_id) AS per_booking,"
        " count(*) OVER (PARTITION BY entry_id) AS per_entry"
        " FROM hit)"
        " SELECT entry_id, booking_id FROM counted"
        " WHERE per_booking = 1 AND per_entry = 1",
        arrays,
    )
    return cur.fetchall()


def link_entries(
    cur: SqlCursor,
    pairs: _collections_abc.Sequence[tuple[int, int]],
    *,
    link_type: str,
    now: _datetime.datetime,
    score: float = LINK_SCORE,
) -> None:
    """Write a whole batch of booking<->entry links in ONE UPDATE.

    The link lives ON THE ENTRY (accounting_entries.datev_booking_id + the
    datev_booking_link_meta JSON); a booking has no own person column (its
    person is the entry's subject). Every pair of one call gets the same meta:
    automatic_manual = 'automatic', author_id = :data:`LINK_AUTHOR_ID` (system
    person), classification_string = link_type and ``score`` --
    :data:`LINK_SCORE` (100 %) for the deterministic import-equivalent rules,
    which a rule may override with its own confidence. ``now`` is the AWARE
    ctx.start_time; created_at stores its ISO 8601 form (and the UTC session
    writes updated_at Rails-conventionally as UTC-naive). The entries ARE
    modified, so their updated_at is bumped.

    ``pairs`` is a sequence of (entry_id, booking_id); it travels as two arrays
    joined via ``unnest``. The UPDATE re-checks ``datev_booking_id IS NULL``, so
    an entry that got a link between the match query and here keeps it -- a
    rowcount below the number of pairs is logged as a warning. An empty batch
    issues no statement."""
    from psycopg.types.json import Jsonb

    if not pairs:
        return
    meta = {
        "created_at": now.isoformat(),
        "author_id": LINK_AUTHOR_ID,
        "score": score,
        "automatic_manual": "automatic",
        "classification_string": link_type,
    }
    cur.execute(
        "UPDATE accounting_entries ae SET datev_booking_id = v.booking_id,"
        " datev_booking_link_meta = %s, updated_at = %s"
        " FROM unnest(%s::bigint[], %s::bigint[]) AS v(entry_id, booking_id)"
        " WHERE ae.id = v.entry_id AND ae.datev_booking_id IS NULL",
        (
            Jsonb(meta),
            now,
            [entry_id for entry_id, _ in pairs],
            [booking_id for _, booking_id in pairs],
        ),
    )
    if cur.rowcount != len(pairs):
        _LOGGER.warning(
            "%s: %d Verknuepfung(en) geplant, aber %d Zeile(n) geschrieben.",
            link_type,
            len(pairs),
            cur.rowcount,
        )


def match_2025_fee_entries(
    cur: SqlCursor, guids: _Guids, *, now: _datetime.datetime
) -> None:
    """Link 2025 participant-fee bookings to their accounting entry.

    Scope: the bookings named by ``guids`` that are 2025, KOST
    :data:`FEE_COST_CENTER_NUMBER` and Gegenkonto
    :data:`FEE_OFFSETTING_ACCOUNT_NUMBER` (the mapped SKR42 fee account) and
    not yet linked. Match rule (verified exact on the real data): the person id
    from the Buchungstext, the same amount INCLUDING THE SIGN (the fee-side
    signed_offsetting_base_amount equals the entry's amount_cents on every
    historical pair) and the EXACT booking date
    (accounting_entries.value_date = datev_bookings.booking_date).

    The batch is matched set-based in one query (see :func:`select_unique_pairs`
    for the symmetric uniqueness) and written in one UPDATE (see
    :func:`link_entries`), which sets the entry's datev_booking_id + link_meta
    (classification_string = :data:`LINK_TYPE_2025_FEE`) -- the link lives on
    the entry. Idempotent: a linked booking is out of scope, a linked entry out
    of reach."""
    cur.execute(
        "SELECT db.id, db.original_posting_text, db.signed_offsetting_base_amount,"
        " db.booking_date"
        f" FROM {BOOKINGS_TABLE} db"
        f" JOIN {BATCHES_TABLE} b ON b.id = db.datev_booking_batch_id"
        " WHERE EXTRACT(YEAR FROM b.financial_year_start) = 2025"
        f" AND db.cost_center_number = '{FEE_COST_CENTER_NUMBER}'"
        f" AND db.offsetting_account_number = '{FEE_OFFSETTING_ACCOUNT_NUMBER}'"
        " AND NOT EXISTS (SELECT 1 FROM accounting_entries ae"
        "                 WHERE ae.datev_booking_id = db.id)"
        " AND db.buchungs_guid = ANY(%s)",
        (list(guids),),
    )
    rows = cur.fetchall()
    booking_ids: list[int] = []
    person_ids: list[int] = []
    amounts_cents: list[int] = []
    value_dates: list[_datetime.date] = []
    for booking_id, text, amount, booking_date in rows:
        person_id = parse_person_id(text)
        if person_id is None or booking_date is None:
            continue
        booking_ids.append(booking_id)
        person_ids.append(person_id)
        amounts_cents.append(amount_to_cents(amount))
        value_dates.append(booking_date)
    pairs = select_unique_pairs(
        cur,
        {
            "booking_id": ("bigint", booking_ids),
            "person_id": ("integer", person_ids),
            "amount_cents": ("integer", amounts_cents),
            "value_date": ("date", value_dates),
        },
        join="ae.subject_type = 'Person' AND ae.subject_id = c.person_id"
        " AND ae.amount_cents = c.amount_cents"
        " AND ae.value_date = c.value_date",
    )
    link_entries(cur, pairs, link_type=LINK_TYPE_2025_FEE, now=now)
    if rows:
        _LOGGER.info(
            "2025 TN-Beitraege: %d von %d unverknuepften Buchungen mit ihrer "
            "Beitragsbuchung verknuepft (%d ohne eindeutigen Treffer).",
            len(pairs),
            len(rows),
            len(rows) - len(pairs),
        )
    _LOGGER.debug(
        "2025 TN-Beitraege: %d Statement(s) fuer %d Kandidat(en).",
        1 + bool(booking_ids) + bool(pairs),
        len(rows),
    )


def match_pre_notification_fee_entries(
    cur: SqlCursor, guids: _Guids, *, now: _datetime.datetime
) -> None:
    """Link regular fee bookings to their accounting entry via the
    pre-notification id in Belegfeld 1.

    Scope: the bookings named by ``guids`` of a 2026-or-later Stapel (financial
    year after 2025 -- the 2025 fee bookings are the 2025 rule's, see
    :func:`match_2025_fee_entries`) with Gegenkonto
    :data:`FEE_OFFSETTING_ACCOUNT_NUMBER`, KOST :data:`FEE_COST_CENTER_NUMBER`
    and a Belegfeld 1 of the form 'Einzug-YYYY-MM-<SEQ>-<n>-<prenotif_id>' that
    are not yet linked. The trailing block is the
    wsjrdp_direct_debit_pre_notifications id; the accounting entry to link
    points at it via accounting_entries.direct_debit_pre_notification_id. The
    person id parsed from the Buchungstext must match the entry's subject
    (verification).

    The batch is matched set-based in one query (see :func:`select_unique_pairs`
    for the symmetric uniqueness) and written in one UPDATE (see
    :func:`link_entries`), which sets the entry's datev_booking_id + link_meta
    (classification_string = :data:`LINK_TYPE_PRE_NOTIFICATION`). Idempotent: a
    linked booking is out of scope, a linked entry out of reach."""
    cur.execute(
        "SELECT db.id, db.document_field_1, db.original_posting_text"
        f" FROM {BOOKINGS_TABLE} db"
        f" JOIN {BATCHES_TABLE} b ON b.id = db.datev_booking_batch_id"
        " WHERE EXTRACT(YEAR FROM b.financial_year_start) > 2025"
        f" AND db.offsetting_account_number = '{FEE_OFFSETTING_ACCOUNT_NUMBER}'"
        f" AND db.cost_center_number = '{FEE_COST_CENTER_NUMBER}'"
        " AND NOT EXISTS (SELECT 1 FROM accounting_entries ae"
        "                 WHERE ae.datev_booking_id = db.id)"
        " AND db.buchungs_guid = ANY(%s)",
        (list(guids),),
    )
    considered = 0
    booking_ids: list[int] = []
    pre_notification_ids: list[int] = []
    person_ids: list[int] = []
    for booking_id, document_field_1, text in cur.fetchall():
        pre_notification_id = parse_pre_notification_id(document_field_1)
        if pre_notification_id is None:
            continue  # not a pre-notification fee booking
        considered += 1
        person_id = parse_person_id(text)
        if person_id is None:
            continue
        booking_ids.append(booking_id)
        pre_notification_ids.append(pre_notification_id)
        person_ids.append(person_id)
    pairs = select_unique_pairs(
        cur,
        {
            "booking_id": ("bigint", booking_ids),
            "pre_notification_id": ("bigint", pre_notification_ids),
            "person_id": ("integer", person_ids),
        },
        join="ae.direct_debit_pre_notification_id = c.pre_notification_id"
        " AND ae.subject_type = 'Person' AND ae.subject_id = c.person_id",
    )
    link_entries(cur, pairs, link_type=LINK_TYPE_PRE_NOTIFICATION, now=now)
    if considered:
        _LOGGER.info(
            "Pre-Notification-Beitraege: %d von %d Einzug-Buchungen mit ihrer "
            "Beitragsbuchung verknuepft (%d ohne eindeutigen Treffer).",
            len(pairs),
            considered,
            considered - len(pairs),
        )
    _LOGGER.debug(
        "Pre-Notification-Beitraege: %d Statement(s) fuer %d Einzug-Buchung(en).",
        1 + bool(booking_ids) + bool(pairs),
        considered,
    )


# The Retouren rule as ONE query -- scope, pool, the exact-date candidates and
# both window counts. Takes one parameter: the buchungs_guid array of the
# bookings in scope. Returns one row per in-scope Retoure booking, entry_id
# non-NULL exactly for the unambiguous ones (so the caller gets the pairs AND
# the number of bookings considered from a single statement).
#
#   scope  every not-yet-linked Retoure fee booking of the WHOLE table, of any
#          financial year -- the reference set of the second window count, so a
#          competing booking in another file still blocks the link.
#   pool   the accounting entry of every returned (return_reason) bank
#          transaction that is free (FREE_ENTRY_CONDITION); an already linked
#          or excluded entry is neither matchable nor counted.
#   batch  the scope restricted to the bookings handed to the rule.
#   hit    exact-date + exact-cents candidates that are unique on BOTH sides
#          within the +/- RETURN_LINK_WINDOW_DAYS window.
RETURN_MATCH_SQL = f"""
WITH scope AS (
  SELECT db.id AS booking_id, db.buchungs_guid, db.booking_date,
         round(db.signed_offsetting_base_amount * 100)::bigint AS cents
    FROM {BOOKINGS_TABLE} db
   WHERE db.offsetting_account_number = '{FEE_OFFSETTING_ACCOUNT_NUMBER}'
     AND db.cost_center_number = '{FEE_COST_CENTER_NUMBER}'
     AND db.original_posting_text ~* 'retoure'
     AND db.signed_offsetting_base_amount < 0
     AND NOT EXISTS (SELECT 1 FROM accounting_entries ae
                      WHERE ae.datev_booking_id = db.id)
), pool AS (
  SELECT ae.id AS entry_id, c.booking_date,
         round(c.signed_base_amount * 100)::bigint AS cents
    FROM wsjrdp_camt_transactions c
    JOIN accounting_entries ae
      ON ae.camt_transaction_id = c.id AND {FREE_ENTRY_CONDITION}
   WHERE c.deleted_at IS NULL
     AND c.return_reason IS NOT NULL
     AND c.return_reason <> ''
), batch AS (
  SELECT * FROM scope WHERE buchungs_guid = ANY(%s)
), hit AS (
  SELECT b.booking_id, p.entry_id
    FROM batch b
    JOIN pool p ON p.cents = b.cents AND p.booking_date = b.booking_date
   WHERE (SELECT count(*) FROM pool w
           WHERE w.cents = b.cents
             AND w.booking_date BETWEEN b.booking_date - {RETURN_LINK_WINDOW_DAYS}
                                    AND b.booking_date + {RETURN_LINK_WINDOW_DAYS}) = 1
     AND (SELECT count(*) FROM scope w
           WHERE w.cents = p.cents
             AND w.booking_date BETWEEN p.booking_date - {RETURN_LINK_WINDOW_DAYS}
                                    AND p.booking_date + {RETURN_LINK_WINDOW_DAYS}) = 1
)
SELECT b.booking_id, h.entry_id
  FROM batch b
  LEFT JOIN hit h ON h.booking_id = b.booking_id
"""


def select_return_fee_matches(
    conn: SqlExecutor, guids: _Guids
) -> list[tuple[int, int | None]]:
    """Run :data:`RETURN_MATCH_SQL` WITHOUT writing anything and return one
    ``(booking_id, entry_id)`` row per in-scope Retoure booking of ``guids``;
    ``entry_id`` is None for a booking without an unambiguous counterpart.

    Read-only, so this is the preview of :func:`match_return_fee_entries` on a
    read-only connection. An empty ``guids`` issues no statement."""
    if not guids:
        return []
    return conn.execute(RETURN_MATCH_SQL, (list(guids),)).fetchall()


def match_return_fee_entries(
    cur: SqlCursor, guids: _Guids, *, now: _datetime.datetime
) -> None:
    """Link returned ("Retoure") fee bookings to the accounting entry of the
    returned bank transaction.

    Scope: the bookings named by ``guids`` with Gegenkonto
    :data:`FEE_OFFSETTING_ACCOUNT_NUMBER`, KOST :data:`FEE_COST_CENTER_NUMBER`,
    'Retoure' in the Buchungstext and a NEGATIVE fee-side amount
    (signed_offsetting_base_amount < 0 -- the return gives the fee back), that
    are not yet linked. EVERY financial year, 2025 included: the 2025 and the
    pre-notification rule run first, so everything they claimed is already out
    of scope.

    Counterpart pool: the accounting entry of every returned bank transaction
    (wsjrdp_camt_transactions with a return_reason, not soft-deleted) that is
    free (:data:`FREE_ENTRY_CONDITION`). Match rule: the SAME amount in cents
    including the sign (the fee-side signed_offsetting_base_amount against the
    transaction's signed_base_amount, which is the entry's amount_cents) AND
    the EXACT date (wsjrdp_camt_transactions.booking_date =
    datev_bookings.booking_date). The VALUE date is deliberately not used: for
    a return it is the value date of the original collection and can be two
    weeks away from the day the return was booked.

    Uniqueness is symmetric AND windowed, which is stricter than the exact-date
    match alone: the pair only survives when, within +/-
    :data:`RETURN_LINK_WINDOW_DAYS` days around the date, exactly ONE free
    return transaction of that amount and exactly ONE unlinked Retoure booking
    of that amount exist -- across the whole tables, not just the bookings
    handed in. Two returns of the same amount in the same fortnight therefore
    leave BOTH unlinked, even when only one of them falls on the exact date:
    with returns of identical amounts the date alone is not evidence enough for
    an automatic link.

    Matched in ONE query (:data:`RETURN_MATCH_SQL`) and written in one UPDATE
    (see :func:`link_entries`), which sets the entry's datev_booking_id +
    link_meta (classification_string = :data:`LINK_TYPE_RETURN`, score =
    :data:`LINK_SCORE`). Idempotent: a linked booking is out of scope, a linked
    entry out of the pool."""
    if not guids:
        return
    rows = select_return_fee_matches(cur, guids)
    pairs = [
        (entry_id, booking_id) for booking_id, entry_id in rows if entry_id is not None
    ]
    link_entries(cur, pairs, link_type=LINK_TYPE_RETURN, now=now)
    if rows:
        _LOGGER.info(
            "Retouren: %d von %d Retoure-Buchungen mit ihrer Beitragsbuchung "
            "verknuepft (%d ohne eindeutigen Treffer).",
            len(pairs),
            len(rows),
            len(rows) - len(pairs),
        )
    _LOGGER.debug(
        "Retouren: %d Statement(s) fuer %d Retoure-Buchung(en).",
        1 + bool(pairs),
        len(rows),
    )


def mirror_camt_links(cur: SqlCursor, guids: _Guids) -> None:
    """Mirror each linked entry's bank-statement (camt) transaction onto the
    camt side: wsjrdp_camt_transactions.datev_booking_id = the entry's booking,
    for the bookings named by ``guids``. Idempotent."""
    cur.execute(
        "UPDATE wsjrdp_camt_transactions c SET datev_booking_id = ae.datev_booking_id"
        " FROM accounting_entries ae"
        f" JOIN {BOOKINGS_TABLE} db ON db.id = ae.datev_booking_id"
        " WHERE ae.camt_transaction_id = c.id"
        " AND ae.datev_booking_id IS NOT NULL"
        " AND c.datev_booking_id IS DISTINCT FROM ae.datev_booking_id"
        " AND db.buchungs_guid = ANY(%s)",
        (list(guids),),
    )
    if cur.rowcount:
        _LOGGER.info("camt-Verknuepfung auf %d Buchung(en) gespiegelt.", cur.rowcount)
