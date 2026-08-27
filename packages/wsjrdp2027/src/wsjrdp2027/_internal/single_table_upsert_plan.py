"""Plan-based insert/update helper for master-data import scripts.

:class:`SingleTableUpsertPlanBuilder` prepares the work around ONE table:

1. ``SingleTableUpsertPlanBuilder(table_name, key_col, values)`` -- bind the
   incoming rows (same element shape as ``pg_table_updatemany``: mappings or
   ``(column, value)`` pair iterables; every set must contain ``key_col``, and
   a key appearing twice raises ``ValueError`` -- combining sources is
   :meth:`~SingleTableUpsertPlanBuilder.merge_values`' job).
2. :meth:`~SingleTableUpsertPlanBuilder.load_existing` -- read the current
   state of the addressed rows (read-only; the column set is the union of the
   columns appearing in ``values``).
3. :meth:`~SingleTableUpsertPlanBuilder.plan` -- compute what actually has to
   change, per row AND per column; optionally treating CP1252-transliterated
   strings as equal. Returns a :class:`SingleTableUpsertPlan`, writes nothing.

The :class:`SingleTableUpsertPlan` reports which tables it affects
(:attr:`~SingleTableUpsertPlan.affected_tables`) and the number of planned
INSERT/UPDATE/DELETE operations per table
(:meth:`~SingleTableUpsertPlan.operation_counts`; DELETE is currently always
0), and :meth:`~SingleTableUpsertPlan.apply` executes it through
:func:`wsjrdp2027._pg.pg_table_insertmany` and
:func:`wsjrdp2027._pg.pg_table_updatemany` (one psycopg pipeline each,
per-row column sets; all SpecialValue and created_at/updated_at ``touch``
handling lives there). Never commits; transaction control stays with the
caller.

Column semantics (uniform for scalar columns and JSONB keys):

* column/key **not mentioned** in a value set -> stays untouched;
* column/key with a **value** -> is set (``dict`` values are written as JSONB);
* scalar column = ``None`` -> is set to SQL ``NULL`` (a scalar
  ``SpecialValue.DELETE`` is equivalent);
* JSONB key = ``SpecialValue.DELETE`` -> the key is
  DELETED from the stored dict. ``dict`` values are merged into the stored
  dict at the top level (see :func:`merge_jsonb`); nested dicts replace their
  key's value entirely;
* scalar column = ``SpecialValue.NOW`` / ``SpecialValue.TODAY`` -> resolved at
  apply time to the ``now`` timestamp resp. its date.

Identifiers are composed with ``psycopg.sql``, values travel as bound
parameters, dicts through ``psycopg.types.json.Jsonb`` (per
https://www.psycopg.org/psycopg3/docs/).
"""

from __future__ import annotations

import dataclasses as _dataclasses
import datetime as _datetime
import logging as _logging
import typing as _typing
import zoneinfo as _zoneinfo

from .._pg import SpecialValue


if _typing.TYPE_CHECKING:
    import psycopg.sql as _psycopg_sql

    from .._pg import PgConnectionLike, _UpdatesType


_LOGGER = _logging.getLogger(__name__)


@_dataclasses.dataclass(frozen=True, kw_only=True)
class OperationCounts:
    """Number of operations a plan intends per table (see
    :meth:`SingleTableUpsertPlan.operation_counts`)."""

    inserts: int
    updates: int
    deletes: int = 0


class SingleTableUpsertPlan:
    """Result of :meth:`SingleTableUpsertPlanBuilder.plan`: the concrete
    per-row, per-column work for ONE table. ``inserts``/``updates`` contain
    the key column plus (for updates) ONLY the genuinely changed columns --
    and inside dict (JSONB) columns only the genuinely changing keys (see
    :func:`_minimal_jsonb_delta`); ``untouched_keys`` lists the keys whose
    target state already equals the stored state. Instances come from the
    builder, not from user code.

    :meth:`apply` executes the plan; a plan can be applied at most once."""

    def __init__(
        self,
        *,
        table_name: str | _psycopg_sql.Identifier,
        key_name: str,
        time_zone: _zoneinfo.ZoneInfo,
        inserts: list[dict],
        updates: list[dict],
        untouched_keys: list,
    ) -> None:
        self._table_name = table_name
        self._key_name = key_name
        self._tz = time_zone
        self.inserts = inserts
        self.updates = updates
        self.untouched_keys = untouched_keys
        self._applied = False

    @property
    def affected_tables(self) -> tuple[str, ...]:
        """Plain names of the tables this plan touches -- currently always
        exactly one."""
        from .._pg import as_identifier_str

        table = self._table_name
        return (table if isinstance(table, str) else as_identifier_str(table),)

    def operation_counts(self) -> dict[str, OperationCounts]:
        """Planned operations per affected table. ``deletes`` is currently
        always 0: a plan never deletes rows."""
        (table,) = self.affected_tables
        return {
            table: OperationCounts(
                inserts=len(self.inserts), updates=len(self.updates), deletes=0
            )
        }

    def apply(
        self,
        conn: PgConnectionLike,
        *,
        now: _datetime.datetime | _datetime.date | str | float | None = None,
        touch: bool | None = None,
    ) -> tuple[list, list]:
        """Execute the plan through :func:`pg_table_insertmany` /
        :func:`pg_table_updatemany` (one psycopg pipeline each). ALL
        SpecialValue and timestamp handling lives in those helpers; ``now``
        and ``touch`` are passed down verbatim. The helpers resolve NOW/TODAY
        markers against ``now``, fall back to the current wall clock when it
        is None, and read a naive datetime as wall time in the configured
        hitobito_time_zone -- pass an explicit ``now`` for deterministic
        timestamps. Via ``touch`` (default ``None`` = not set = on) inserts
        get ``created_at = now`` (``updated_at`` stays NULL: the row was
        never updated) and updates ``updated_at = now``; ``touch=False``
        turns that off, an explicit value in a row always wins over the
        stamp.

        Returns ``(inserted_keys, updated_keys)`` in input order. Never
        commits. A plan can be applied at most once -- afterwards it is
        stale; for another round, build a fresh plan via
        :meth:`SingleTableUpsertPlanBuilder.load_existing` +
        :meth:`SingleTableUpsertPlanBuilder.plan`."""
        if self._applied:
            raise RuntimeError(
                "this plan has already been applied; build a fresh one via "
                "load_existing() + plan()"
            )
        from .._pg import pg_table_insertmany, pg_table_updatemany, to_connection

        connection = to_connection(conn, read_only=False)

        pg_table_insertmany(
            connection,
            self._table_name,
            self.inserts,
            id_col=self._key_name,
            now=now,
            time_zone=self._tz,
            touch=touch,
        )
        inserted_keys = [row[self._key_name] for row in self.inserts]

        pg_table_updatemany(
            connection,
            self._table_name,
            self.updates,
            key_col=self._key_name,
            id_col=self._key_name,
            now=now,
            time_zone=self._tz,
            touch=touch,
        )
        # pg_table_updatemany raises when any update set hits no row, so at
        # this point every planned update has been applied.
        updated_keys = [row[self._key_name] for row in self.updates]

        self._applied = True
        return inserted_keys, updated_keys


def merge_jsonb(stored_value: dict | None, incoming: dict | None) -> dict:
    """Merge ``incoming`` into ``stored_value`` at the top level: keys not
    mentioned in ``incoming`` are preserved, ``SpecialValue.DELETE`` values delete
    the key, everything else is set."""
    merged = dict(stored_value or {})
    for key, value in (incoming or {}).items():
        if value is SpecialValue.DELETE:
            merged.pop(key, None)
        else:
            merged[key] = value
    return merged


def _values_equal(incoming: object, stored: object, *, translit: bool) -> bool:
    """Recursive equality; with ``translit`` a string pair also counts as
    equal when ``incoming`` is merely the CP1252 transliteration of the
    stored Unicode value (asymmetric on purpose, see doc/fin/datev_cp1252.md
    in the hitobito_wsjrdp_2027 wagon)."""
    if isinstance(incoming, str) and isinstance(stored, str):
        if incoming == stored:
            return True
        if translit:
            from .._datev import win1252_matches_stored

            return win1252_matches_stored(incoming, stored)
        return False
    if isinstance(incoming, dict) and isinstance(stored, dict):
        return incoming.keys() == stored.keys() and all(
            _values_equal(incoming[k], stored[k], translit=translit) for k in incoming
        )
    if isinstance(incoming, list) and isinstance(stored, list):
        return len(incoming) == len(stored) and all(
            _values_equal(a, b, translit=translit) for a, b in zip(incoming, stored)
        )
    return bool(incoming == stored)


def _strip_delete_keys(value: dict) -> dict:
    return {k: v for k, v in value.items() if v is not SpecialValue.DELETE}


def _minimal_jsonb_delta(delta: dict, stored: dict, *, translit: bool) -> dict:
    """Reduce a JSONB update delta to the keys that actually CHANGE the stored
    dict: a DELETE of an absent key is dropped, and so is a value equal to the
    stored one -- transliteration-aware when ``translit`` is set, so a mere
    CP1252 transliteration never overwrites the stored Unicode value even when
    OTHER keys of the same column genuinely change."""
    minimal: dict = {}
    for key, value in delta.items():
        if value is SpecialValue.DELETE:
            if key in stored:
                minimal[key] = value
        elif not (
            key in stored and _values_equal(value, stored[key], translit=translit)
        ):
            minimal[key] = value
    return minimal


def _intake_serialize(value: object, *, tz: _zoneinfo.ZoneInfo, where: str) -> object:
    """Serialize datetime/date values inside a JSONB dict to their ISO 8601
    string form (see _pg._resolve_time_zone for the format decision) so the
    plan diff compares the written representation. NOW/TODAY markers pass
    through (they are resolved later, in write, against `now`);
    SpecialValue.DELETE is not allowed below the top level of the column
    dict -- the caller handles that level."""
    if isinstance(value, SpecialValue):
        if value is SpecialValue.DELETE:
            raise ValueError(
                f"{where}: SpecialValue.DELETE is only allowed as a "
                "TOP-LEVEL value of a dict (JSONB) column"
            )
        return value
    if isinstance(value, _datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=tz)
        return value.astimezone(tz).isoformat(timespec="milliseconds")
    if isinstance(value, _datetime.date):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            k: _intake_serialize(v, tz=tz, where=f"{where}[{k!r}]")
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [
            _intake_serialize(v, tz=tz, where=f"{where}[{i}]")
            for i, v in enumerate(value)
        ]
    return value


def _intake_serialize_column_dict(
    value: dict, *, tz: _zoneinfo.ZoneInfo, where: str
) -> dict:
    """Intake serialization for a whole JSONB column dict: its top-level
    values may be SpecialValue.DELETE (kept as marker), everything else runs
    through :func:`_intake_serialize`."""
    return {
        k: (
            v
            if v is SpecialValue.DELETE
            else _intake_serialize(v, tz=tz, where=f"{where}[{k!r}]")
        )
        for k, v in value.items()
    }


class SingleTableUpsertPlanBuilder:
    """See the module docstring for the three-phase lifecycle: bind values,
    :meth:`load_existing`, :meth:`plan` -- the returned
    :class:`SingleTableUpsertPlan` is then applied. After an apply the loaded
    state is stale; a new cycle starts with :meth:`load_existing` again."""

    def __init__(
        self,
        table_name: str | _psycopg_sql.Identifier,
        key_col: str,
        values: _typing.Iterable[_UpdatesType],
        *,
        time_zone: str | _zoneinfo.ZoneInfo | None = None,
    ) -> None:
        from .._pg import _resolve_time_zone

        self.table_name = table_name
        self.key_col = key_col
        # Zone for serializing date/time values inside JSONB dicts; pass
        # ctx.hitobito_time_zone (see _pg._resolve_time_zone for the format
        # decision and the Europe/Zurich default).
        self._tz = _resolve_time_zone(time_zone)
        if not isinstance(key_col, str):
            # The plain name is needed to look up the key inside the value
            # sets; an Identifier cannot provide it reliably.
            raise TypeError("key_col must be given as str (the plain column name)")
        key_name = key_col

        self._key_name = key_name
        # {key value: {column: value}} -- insertion-ordered, mirroring the
        # shape of the loaded state (self._existing).
        self._rows: dict = {}
        self._column_set: set[str] = set()

        self._existing: dict | None = None

        for key_value, row in self._normalized_value_sets(values):
            if key_value in self._rows:
                raise ValueError(
                    f"values contain the key {key_name} = {key_value!r} twice"
                )
            self._column_set.update(row)
            self._rows[key_value] = row

    def _normalized_value_sets(
        self, values: _typing.Iterable[_UpdatesType]
    ) -> _typing.Iterator[tuple[object, dict]]:
        from .._pg import _normalize_updates

        key_name = self._key_name
        for index, value_set in enumerate(values):
            row = dict(_normalize_updates(value_set))
            if key_name not in row:
                raise ValueError(
                    f"values[{index}] does not contain the key column {key_name!r}"
                )
            for column, column_value in list(row.items()):
                if column_value is SpecialValue.DELETE:
                    # A scalar DELETE means "set the column to SQL NULL" --
                    # exactly what a scalar None means here, so normalize it
                    # right away (plan/diff and write then need no marker
                    # handling; NULL-idempotency falls out for free).
                    row[column] = None
                elif isinstance(column_value, dict):
                    # Serialize date/time values to their ISO 8601 JSONB form
                    # right away, so plan()'s diff compares what would be
                    # written. NOW/TODAY stay markers until apply()
                    # (resolved there against `now`, wall clock by default)
                    # -- a marker never equals a stored string, so such a
                    # row always counts as changed.
                    row[column] = _intake_serialize_column_dict(
                        column_value,
                        tz=self._tz,
                        where=f"values[{index}][{column!r}]",
                    )
            key_value = row.pop(key_name)
            yield key_value, row

    @property
    def _columns(self) -> tuple[str, ...]:
        return tuple(sorted(self._column_set))

    def merge_values(
        self,
        values: _typing.Iterable[_UpdatesType],
        *,
        keep_existing_for_cp1252_equality: bool | _typing.Sequence[str] = False,
    ) -> None:
        """Merge additional value sets into the bound ones (e.g. a second
        import file). Only allowed BEFORE :meth:`load_existing` -- afterwards
        the column union (and therefore the loaded state) would be stale.

        Per key: an unknown key appends a new row; for a known key the
        incoming columns are merged into the existing set (an incoming column
        wins; ``dict`` values merge per top-level key, ``SpecialValue.DELETE`` markers
        included). With ``keep_existing_for_cp1252_equality`` (``True`` = all
        columns, sequence = exactly those) an incoming string that is merely
        the CP1252 transliteration of the existing value KEEPS the existing
        value -- e.g. a DATEV file merged over Moss Unicode data."""
        if self._existing is not None:
            raise RuntimeError(
                "merge_values() is not allowed after load_existing(): the "
                "loaded state would be stale"
            )
        incoming = list(self._normalized_value_sets(values))
        incoming_columns: set[str] = set()
        for _, row in incoming:
            incoming_columns.update(row)
        if keep_existing_for_cp1252_equality is True:
            translit_columns = incoming_columns
        elif keep_existing_for_cp1252_equality is False:
            translit_columns = set()
        else:
            translit_columns = set(keep_existing_for_cp1252_equality)
            unknown = translit_columns.difference(incoming_columns)
            if unknown:
                raise ValueError(
                    "keep_existing_for_cp1252_equality names column(s) absent "
                    f"from the merged values: {sorted(unknown)}"
                )

        for key_value, row in incoming:
            existing = self._rows.get(key_value)
            if existing is None:
                self._rows[key_value] = row
            else:
                for column, value in row.items():
                    existing[column] = self._merged_column_value(
                        existing.get(column),
                        value,
                        translit=column in translit_columns,
                    )
            self._column_set.update(row)

    @staticmethod
    def _merged_column_value(
        existing: object, incoming: object, *, translit: bool
    ) -> object:
        def keep_existing(old: object, new: object) -> bool:
            if not (translit and isinstance(old, str) and isinstance(new, str)):
                return False
            from .._datev import win1252_matches_stored

            return new != old and win1252_matches_stored(new, old)

        if isinstance(existing, dict) and isinstance(incoming, dict):
            merged = dict(existing)
            for key, value in incoming.items():
                if keep_existing(merged.get(key), value):
                    continue
                merged[key] = value  # SpecialValue.DELETE markers pass through
            return merged
        if keep_existing(existing, incoming):
            return existing
        return incoming

    # -- phase 1: read -------------------------------------------------------

    @property
    def existing(self) -> dict[object, dict]:
        """The loaded state ({key: {column: value}}); raises before
        :meth:`load_existing`."""
        if self._existing is None:
            raise RuntimeError("load_existing() has not been called yet")
        return self._existing

    def load_existing(self, conn: PgConnectionLike) -> None:
        """Read-only load of the addressed rows, restricted to the union of
        the columns appearing in the value sets."""
        import psycopg.sql

        from .._pg import to_connection

        connection = to_connection(conn, read_only=False)
        table = (
            psycopg.sql.Identifier(self.table_name)
            if isinstance(self.table_name, str)
            else self.table_name
        )
        keys = list(self._rows)
        if not keys or not self._columns:
            self._existing = {}
            return
        query = psycopg.sql.SQL(
            "SELECT {columns} FROM {table} WHERE {key} = ANY(%s)"
        ).format(
            columns=psycopg.sql.SQL(", ").join(
                psycopg.sql.Identifier(c) for c in (self._key_name, *self._columns)
            ),
            table=table,
            key=psycopg.sql.Identifier(self._key_name),
        )
        import psycopg.rows

        with connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(query, (keys,))
            self._existing = {row.pop(self._key_name): row for row in cur.fetchall()}

    # -- phase 2: plan -------------------------------------------------------

    def plan(
        self,
        *,
        skip_update_for_cp1252_equality: bool | _typing.Sequence[str] = False,
    ) -> SingleTableUpsertPlan:
        """Compute the per-row, per-column changes against the loaded state.

        ``skip_update_for_cp1252_equality``: ``True`` applies the
        CP1252-transliteration-aware string comparison to every column
        (recursively inside dicts), ``False`` to none, a sequence of column
        names to exactly those columns. Raises if :meth:`load_existing` has
        not run. Returns a :class:`SingleTableUpsertPlan`, writes nothing."""
        if self._existing is None:
            raise RuntimeError("plan() requires load_existing() first")
        if skip_update_for_cp1252_equality is True:
            translit_columns = set(self._columns)
        elif skip_update_for_cp1252_equality is False:
            translit_columns = set()
        else:
            translit_columns = set(skip_update_for_cp1252_equality)
            unknown = translit_columns.difference(self._columns)
            if unknown:
                raise ValueError(
                    "skip_update_for_cp1252_equality names unknown "
                    f"column(s): {sorted(unknown)}"
                )

        inserts: list[dict] = []
        updates: list[dict] = []
        untouched_keys: list = []
        translit_kept = 0
        for key_value, row in self._rows.items():
            current = self._existing.get(key_value)
            if current is None:
                insert_row = {self._key_name: key_value}
                for column, value in row.items():
                    insert_row[column] = (
                        _strip_delete_keys(value) if isinstance(value, dict) else value
                    )
                inserts.append(insert_row)
                continue
            changed: dict = {}
            for column, value in row.items():
                stored_value = current.get(column)
                translit = column in translit_columns
                if isinstance(value, dict):
                    stored_dict = stored_value if isinstance(stored_value, dict) else {}
                    target: object = merge_jsonb(stored_dict, value)
                    if _values_equal(target, stored_value, translit=translit):
                        if translit and target != stored_value:
                            translit_kept += 1
                        continue
                    # Keep a MINIMAL partial value set (SpecialValue.DELETE
                    # markers included): only the keys that genuinely change
                    # the stored dict are written -- pg_table_updatemany
                    # merges them server-side; keys equal to the stored value
                    # (transliteration-aware where enabled) stay untouched.
                    minimal = _minimal_jsonb_delta(
                        value, stored_dict, translit=translit
                    )
                    if not minimal:
                        continue
                    changed[column] = minimal
                    continue
                if _values_equal(value, stored_value, translit=translit):
                    if translit and value != stored_value:
                        translit_kept += 1
                    continue
                changed[column] = value
            if changed:
                changed[self._key_name] = key_value
                updates.append(changed)
            else:
                untouched_keys.append(key_value)
        if translit_kept:
            _LOGGER.info(
                "CP1252: %d field(s) differ from the stored value only by "
                "transliteration and stay untouched.",
                translit_kept,
            )
        return SingleTableUpsertPlan(
            table_name=self.table_name,
            key_name=self._key_name,
            time_zone=self._tz,
            inserts=inserts,
            updates=updates,
            untouched_keys=untouched_keys,
        )
