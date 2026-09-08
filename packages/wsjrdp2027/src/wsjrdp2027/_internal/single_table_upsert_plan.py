"""Plan-based insert/update helper for master-data import scripts.

:class:`SingleTableUpsertPlanBuilder` prepares the work around ONE table:

1. ``SingleTableUpsertPlanBuilder(table_name, key_col, values)`` -- bind the
   incoming rows (same element shape as ``pg_table_updatemany``: mappings or
   ``(column, value)`` pair iterables; every set must contain ``key_col``, and
   a key appearing twice raises ``ValueError`` -- combining sources is
   :meth:`~SingleTableUpsertPlanBuilder.merge_values`' job). ``key_col`` may
   also be a sequence of column names -- a COMPOSITE key (e.g. the identity
   tuple of a DATEV Buchungsstapel): every value set must then contain ALL of
   those columns, and everywhere a key value appears (``untouched_keys``, the
   return of :meth:`~SingleTableUpsertPlan.apply`, ...) it is the tuple of the
   values in ``key_col`` order.
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
  key's value entirely. Columns named in ``plan(replace_dict_columns=...)``
  REPLACE the stored dict instead (the incoming dict is the full target
  state; stored-only keys are deleted) -- for snapshot-style columns like a
  raw import record;
* a **list** value is a JSONB array: compared and written as a WHOLE (no
  per-element merge; date/datetime elements are serialized like inside
  dicts);
* a :class:`~wsjrdp2027._pg.PgArray` value is a NATIVE PostgreSQL ARRAY
  column (``uuid[]``, ``text[]``, ...); a bare ``list`` stays a JSONB array.
  On INSERT the array is written as given (an empty one writes ``{}``, not
  ``NULL``), on UPDATE its ``ArrayMode`` decides:

  - ``REPLACE``: the incoming elements ARE the target state -- compared
    element-wise IN ORDER, and written as a whole array. A stored SQL
    ``NULL`` is never equal, not even to an empty incoming array: that
    writes ``{}`` exactly once and is idempotent afterwards;
  - ``APPEND``: only the elements the LOADED array does not carry yet are
    written (that delta is appended, order kept, so no duplicates arise); an
    empty delta writes no row at all, hence a stored ``NULL`` stays ``NULL``.

  A ``PgArray`` is only valid as a TOP-LEVEL column value (inside a
  dict/list it raises) and never in a key column. :meth:`merge_values`
  combines two ``PgArray`` values of the SAME mode and element type
  (``REPLACE``: the later one wins, ``APPEND``: the ordered union); a
  different mode or element type, or a ``PgArray`` meeting a bare
  list/scalar, raises -- a column never silently switches between ARRAY and
  JSONB semantics. An explicit ``None`` sets ``NULL`` as for any scalar.

  Known limit: the APPEND delta is computed against the state
  :meth:`~SingleTableUpsertPlanBuilder.load_existing` read, so a concurrent
  appender writing between load and apply could make an element appear
  twice (the same window every column has);
* scalar column = ``SpecialValue.NOW`` / ``SpecialValue.TODAY`` -> resolved at
  apply time to the ``now`` timestamp resp. its date.

Identifiers are composed with ``psycopg.sql``, values travel as bound
parameters, dicts through ``psycopg.types.json.Jsonb`` (per
https://www.psycopg.org/psycopg3/docs/).
"""

from __future__ import annotations

import collections.abc as _collections_abc
import dataclasses as _dataclasses
import datetime as _datetime
import logging as _logging
import typing as _typing
import zoneinfo as _zoneinfo

from .._pg import ArrayMode, PgArray, SpecialValue, _table_identifier


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
    :func:`_minimal_jsonb_delta`), inside an APPEND
    :class:`~wsjrdp2027._pg.PgArray` column only the missing elements (see
    :func:`_array_delta`); ``untouched_keys`` lists the keys whose
    target state already equals the stored state. Instances come from the
    builder, not from user code.

    :meth:`apply` executes the plan; a plan can be applied at most once."""

    def __init__(
        self,
        *,
        table_name: str | _psycopg_sql.Identifier,
        key_names: tuple[str, ...],
        composite_key: bool,
        time_zone: _zoneinfo.ZoneInfo,
        inserts: list[dict],
        updates: list[dict],
        untouched_keys: list,
    ) -> None:
        self._table_name = table_name
        self._key_names = key_names
        self._composite_key = composite_key
        self._tz = time_zone
        self.inserts = inserts
        self.updates = updates
        self.untouched_keys = untouched_keys
        self._applied = False

    def _row_key(self, row: dict) -> object:
        """The key value of a plan row -- a scalar, or the tuple of the key
        columns for a composite key."""
        if self._composite_key:
            return tuple(row[name] for name in self._key_names)
        return row[self._key_names[0]]

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
        from psycopg.types.json import Jsonb

        from .._pg import pg_table_insertmany, pg_table_updatemany, to_connection

        connection = to_connection(conn, read_only=False)

        def rows_for_write(rows: list[dict]) -> list[dict]:
            # A list value is a JSONB array column: the *many helpers would
            # bind a bare Python list as a PostgreSQL ARRAY, so wrap it here
            # (dicts go through unwrapped -- their partial-merge handling
            # lives in the helpers, and so does the native-ARRAY handling of a
            # PgArray, which passes through unwrapped as well: the helpers
            # render its cast resp. its array_cat template). The plan rows
            # themselves stay unwrapped.
            return [
                {
                    column: Jsonb(value) if isinstance(value, list) else value
                    for column, value in row.items()
                }
                for row in rows
            ]

        # id_col is only used for RETURNING; any present column works, so the
        # first key column serves both the single and the composite case.
        first_key = self._key_names[0]
        pg_table_insertmany(
            connection,
            self._table_name,
            rows_for_write(self.inserts),
            id_col=first_key,
            now=now,
            time_zone=self._tz,
            touch=touch,
        )
        inserted_keys = [self._row_key(row) for row in self.inserts]

        pg_table_updatemany(
            connection,
            self._table_name,
            rows_for_write(self.updates),
            key_col=self._key_names if self._composite_key else first_key,
            id_col=first_key,
            now=now,
            time_zone=self._tz,
            touch=touch,
        )
        # pg_table_updatemany raises when any update set hits no row, so at
        # this point every planned update has been applied.
        updated_keys = [self._row_key(row) for row in self.updates]

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


def _array_delta(value: PgArray, stored: list | None, *, translit: bool) -> tuple:
    """The elements of an ``ArrayMode.APPEND`` :class:`PgArray` that the
    stored array does not carry yet -- in the incoming order, which is exactly
    what has to be appended.

    A stored ``NULL`` (``None``) counts as the empty array. Membership runs
    through :func:`_values_equal`, so with ``translit`` a ``text[]`` element
    that is merely the CP1252 transliteration of a stored element counts as
    present (and is therefore NOT appended again)."""
    stored_elements = stored if stored is not None else []
    return tuple(
        element
        for element in value.elements
        if not any(
            _values_equal(element, stored_element, translit=translit)
            for stored_element in stored_elements
        )
    )


def _intake_serialize(value: object, *, tz: _zoneinfo.ZoneInfo, where: str) -> object:
    """Serialize datetime/date values inside a JSONB dict to their ISO 8601
    string form (see _pg._resolve_time_zone for the format decision) so the
    plan diff compares the written representation. NOW/TODAY markers pass
    through (they are resolved later, in write, against `now`);
    SpecialValue.DELETE is not allowed below the top level of the column
    dict -- the caller handles that level. A PgArray is a whole COLUMN value
    (a native ARRAY column), never a JSONB element, and raises here."""
    if isinstance(value, SpecialValue):
        if value is SpecialValue.DELETE:
            raise ValueError(
                f"{where}: SpecialValue.DELETE is only allowed as a "
                "TOP-LEVEL value of a dict (JSONB) column"
            )
        return value
    if isinstance(value, PgArray):
        # Same wording as _pg._serialize_jsonb_value: a PgArray selects a
        # native ARRAY column, it has no meaning inside JSONB. ValueError,
        # not TypeError: like the DELETE marker above it is a well-typed
        # object used in the wrong place.
        raise ValueError(  # noqa: TRY004
            f"{where}: PgArray is only allowed as a TOP-LEVEL column value"
        )
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
        key_col: str | _typing.Sequence[str],
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
        # The plain names are needed to look the key up inside the value
        # sets; an Identifier cannot provide them reliably. A sequence of
        # names is a COMPOSITE key: key values are then tuples in this order.
        key_names: tuple[str, ...]
        if isinstance(key_col, str):
            key_names = (key_col,)
            self._composite_key = False
        else:
            if not isinstance(key_col, _collections_abc.Sequence):
                raise TypeError(
                    "key_col must be given as str or a non-empty sequence of "
                    "str (the plain column names)"
                )
            key_names = tuple(key_col)
            if not key_names or not all(isinstance(k, str) for k in key_names):
                raise TypeError(
                    "key_col must be given as str or a non-empty sequence of "
                    "str (the plain column names)"
                )
            if len(set(key_names)) != len(key_names):
                raise ValueError(f"key_col contains duplicate columns: {key_col!r}")
            self._composite_key = True

        self._key_names = key_names
        self._key_desc = (
            "(" + ", ".join(key_names) + ")" if self._composite_key else key_names[0]
        )
        # {key value: {column: value}} -- insertion-ordered, mirroring the
        # shape of the loaded state (self._existing).
        self._rows: dict = {}
        self._column_set: set[str] = set()

        self._existing: dict | None = None

        for key_value, row in self._normalized_value_sets(values):
            if key_value in self._rows:
                raise ValueError(
                    f"values contain the key {self._key_desc} = {key_value!r} twice"
                )
            self._column_set.update(row)
            self._rows[key_value] = row

    def _normalized_value_sets(
        self, values: _typing.Iterable[_UpdatesType]
    ) -> _typing.Iterator[tuple[object, dict]]:
        from .._pg import _normalize_updates

        for index, value_set in enumerate(values):
            row = dict(_normalize_updates(value_set))
            for key_name in self._key_names:
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
                elif isinstance(column_value, PgArray):
                    # A native ARRAY column value: already canonical (its
                    # constructor normalized the elements per element type,
                    # dropped duplicates and froze the result), so it passes
                    # through UNCHANGED -- no ISO serialization, which is for
                    # JSONB. The explicit branch also keeps the `list` branch
                    # below from ever seeing a PgArray.
                    if column in self._key_names:
                        raise ValueError(
                            f"values[{index}][{column!r}]: a PgArray is not a "
                            "valid value for the key column "
                            f"{self._key_desc} -- key values are scalars"
                        )
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
                elif isinstance(column_value, list):
                    # A list is a JSONB array column: serialize date/time
                    # elements the same way (a DELETE marker inside raises --
                    # arrays have no per-element merge).
                    row[column] = _intake_serialize(
                        column_value,
                        tz=self._tz,
                        where=f"values[{index}][{column!r}]",
                    )
            if self._composite_key:
                key_value: object = tuple(row.pop(k) for k in self._key_names)
            else:
                key_value = row.pop(self._key_names[0])
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
        included; two :class:`~wsjrdp2027._pg.PgArray` values of the same mode
        and element type combine -- ``REPLACE``: the incoming one wins,
        ``APPEND``: the ordered union -- while a different mode or element
        type, or a ``PgArray`` meeting a bare list/scalar, raises
        ``ValueError``). With ``keep_existing_for_cp1252_equality`` (``True`` = all
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

        if isinstance(existing, PgArray) and isinstance(incoming, PgArray):
            if (
                existing.mode is not incoming.mode
                or existing.element_type is not incoming.element_type
            ):
                raise ValueError(
                    "cannot merge PgArray values with a different mode or "
                    f"element type: {existing.mode.name}/"
                    f"{existing.element_type.name} vs {incoming.mode.name}/"
                    f"{incoming.element_type.name}"
                )
            if existing.mode is ArrayMode.APPEND:
                # An accumulating column: the ordered union of both sources
                # (the PgArray constructor drops duplicates, first occurrence
                # winning).
                return _dataclasses.replace(
                    existing,
                    elements=(*existing.elements, *incoming.elements),
                )
            # REPLACE: the later source IS the target state, like a scalar.
            return incoming
        if isinstance(existing, PgArray) or isinstance(incoming, PgArray):
            # Exactly one side is a native ARRAY value. Only an explicit None
            # (SQL NULL, the scalar rule) and an absent counterpart may meet
            # it -- a bare list or a scalar would silently switch the column
            # between ARRAY and JSONB semantics.
            if existing is None or incoming is None:
                return incoming
            raise ValueError(
                "cannot merge a PgArray (a native ARRAY column) with a "
                f"non-PgArray value: {existing!r} vs {incoming!r}"
            )
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

    def load_existing(
        self, conn: PgConnectionLike, *, read_only_columns: _typing.Sequence[str] = ()
    ) -> None:
        """Read-only load of the addressed rows, restricted to the union of the
        columns appearing in the value sets.

        ``read_only_columns`` adds further columns to that SELECT: they are
        exposed via :attr:`existing` for the caller's own use (e.g. a stability
        check on an INSERT-only column) but are NEVER diffed or written by
        :meth:`plan`/:meth:`apply`. Columns already among the value-set columns
        (or the key) are merged/deduplicated, so naming one that is auto-loaded
        anyway is harmless."""
        import psycopg.rows
        import psycopg.sql

        from .._pg import to_connection

        connection = to_connection(conn, read_only=False)
        # Defence in depth: a bare psycopg.sql.SQL would otherwise reach the
        # statement unquoted (the write path hardens the name the same way).
        table = _table_identifier(self.table_name)
        keys = list(self._rows)
        # Value-set columns first, then any read-only extras not already covered
        # by the value set or the key (deduplicated, order preserved).
        load_columns = list(
            dict.fromkeys(
                c
                for c in (*self._columns, *read_only_columns)
                if c not in self._key_names
            )
        )
        if not keys or not load_columns:
            self._existing = {}
            return
        columns_sql = psycopg.sql.SQL(", ").join(
            psycopg.sql.Identifier(c) for c in (*self._key_names, *load_columns)
        )
        if self._composite_key:
            # Composite key: match the key tuples against a VALUES list
            # ((k1, ..., kn) IN (VALUES (...), ...)); psycopg sends typed
            # parameters, so the VALUES columns compare cleanly against the
            # table columns.
            key_tuple_sql = psycopg.sql.SQL("({})").format(
                psycopg.sql.SQL(", ").join(
                    psycopg.sql.Placeholder() for _ in self._key_names
                )
            )
            query = psycopg.sql.SQL(
                "SELECT {columns} FROM {table} WHERE ({key_columns}) IN "
                "(VALUES {key_tuples})"
            ).format(
                columns=columns_sql,
                table=table,
                key_columns=psycopg.sql.SQL(", ").join(
                    psycopg.sql.Identifier(k) for k in self._key_names
                ),
                key_tuples=psycopg.sql.SQL(", ").join(key_tuple_sql for _ in keys),
            )
            params: list = [value for key in keys for value in key]
        else:
            query = psycopg.sql.SQL(
                "SELECT {columns} FROM {table} WHERE {key} = ANY(%s)"
            ).format(
                columns=columns_sql,
                table=table,
                key=psycopg.sql.Identifier(self._key_names[0]),
            )
            params = [keys]

        with connection.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(query, params)
            if self._composite_key:
                self._existing = {
                    tuple(row.pop(k) for k in self._key_names): row
                    for row in cur.fetchall()
                }
            else:
                self._existing = {
                    row.pop(self._key_names[0]): row for row in cur.fetchall()
                }

    # -- phase 2: plan -------------------------------------------------------

    def _key_columns_dict(self, key_value: object) -> dict:
        """The key value spelled out as {column: value} (in ``key_col``
        order), for insert rows and for putting the key back into update
        rows."""
        if self._composite_key:
            return dict(zip(self._key_names, _typing.cast("tuple", key_value)))
        return {self._key_names[0]: key_value}

    def plan(
        self,
        *,
        skip_update_for_cp1252_equality: bool | _typing.Sequence[str] = False,
        replace_dict_columns: _typing.Sequence[str] = (),
    ) -> SingleTableUpsertPlan:
        """Compute the per-row, per-column changes against the loaded state.

        ``skip_update_for_cp1252_equality``: ``True`` applies the
        CP1252-transliteration-aware string comparison to every column
        (recursively inside dicts, and per element of a ``text[]``
        :class:`~wsjrdp2027._pg.PgArray`), ``False`` to none, a sequence of
        column names to exactly those columns.

        ``replace_dict_columns``: dict (JSONB) columns whose incoming dict is
        the FULL target state (snapshot semantics) instead of a partial
        top-level merge: keys present only in the stored dict are DELETED.
        The written delta is still minimal (only genuinely changing keys,
        transliteration-aware where enabled).

        Raises if :meth:`load_existing` has not run. Returns a
        :class:`SingleTableUpsertPlan`, writes nothing."""
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
        replace_columns = set(replace_dict_columns)
        unknown = replace_columns.difference(self._columns)
        if unknown:
            raise ValueError(
                f"replace_dict_columns names unknown column(s): {sorted(unknown)}"
            )

        inserts: list[dict] = []
        updates: list[dict] = []
        untouched_keys: list = []
        translit_kept = 0
        for key_value, row in self._rows.items():
            current = self._existing.get(key_value)
            if current is None:
                insert_row = self._key_columns_dict(key_value)
                for column, value in row.items():
                    # A PgArray passes through as given: on INSERT both modes
                    # write the whole array (there is no stored state to
                    # append to).
                    insert_row[column] = (
                        _strip_delete_keys(value) if isinstance(value, dict) else value
                    )
                inserts.append(insert_row)
                continue
            changed: dict = {}
            for column, value in row.items():
                stored_value = current.get(column)
                translit = column in translit_columns
                if isinstance(value, PgArray):
                    # Native ARRAY column; the value carries the semantics.
                    stored_list = (
                        stored_value if isinstance(stored_value, list) else None
                    )
                    if value.mode is ArrayMode.APPEND:
                        # Only what the stored array is missing is written --
                        # that delta is what makes a re-import idempotent
                        # (the statement itself appends verbatim). An empty
                        # delta writes NO row for this column, so a stored
                        # NULL stays NULL: an APPEND never claims the column.
                        delta = _array_delta(value, stored_list, translit=translit)
                        if delta:
                            changed[column] = _dataclasses.replace(
                                value, elements=delta
                            )
                        continue
                    # REPLACE: the incoming elements are the target state,
                    # compared IN ORDER. A stored NULL is never equal, not
                    # even to an empty incoming array -- that writes `{}` once
                    # and is untouched from then on.
                    incoming_elements = list(value.elements)
                    if stored_list is not None and _values_equal(
                        incoming_elements, stored_list, translit=translit
                    ):
                        if translit and incoming_elements != stored_list:
                            translit_kept += 1
                        continue
                    changed[column] = value
                    continue
                if isinstance(value, dict):
                    stored_dict = stored_value if isinstance(stored_value, dict) else {}
                    if column in replace_columns:
                        # Snapshot semantics: the incoming dict IS the target
                        # state; stored-only keys are deleted.
                        target: object = _strip_delete_keys(value)
                    else:
                        target = merge_jsonb(stored_dict, value)
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
                    if column in replace_columns:
                        for stored_key in stored_dict:
                            if stored_key not in value:
                                minimal[stored_key] = SpecialValue.DELETE
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
                changed.update(self._key_columns_dict(key_value))
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
            key_names=self._key_names,
            composite_key=self._composite_key,
            time_zone=self._tz,
            inserts=inserts,
            updates=updates,
            untouched_keys=untouched_keys,
        )
