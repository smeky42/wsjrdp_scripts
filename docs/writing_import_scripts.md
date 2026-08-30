# Writing import scripts

Conventions for CLIs that read files (or an API export) and write them into the
Hitobito database: the importers for standing-data
(`accounting_tools/import_cost_centers.py`, `import_ledger_accounts.py`,
`import_personal_accounts.py`) and the DATEV booking importer
(`import_datev_buchungsstapel.py`) were used as the reference implementations.
`import_camt_bank_statements.py` (CAMT bank statements) has since been migrated
to the same shape.

Older scripts (`import_moss_balance_movements.py`,
`import_moss_card_transactions.py`) predate these conventions. They still work;
migrate them when you touch them anyway (checklist at the end).

> ⚠ **`--dry-run` might not be safe in the old scripts.** The legacy
> `ctx.psycopg_connect()` opens a read-write session regardless of
> `ctx.dry_run` (verified: `SHOW default_transaction_read_only` -> `off`), so
> a script that does not check `ctx.dry_run` itself -- as
> `import_moss_balance_movements.py` does not -- **writes to the
> database even with `--dry-run`**. The modern
> `hitobito_psycopg_connection(read_only=False)` is downgraded to read-only
> under `--dry-run` by the context, so the database itself refuses the write.
> Until such a script is migrated, treat `--dry-run` there as "no protection".

The runtime harness itself -- `with ctx:`, the read-only/read-write
connections, the approval layers, one log file per run -- is described in
**[wsjrdp_context.md](wsjrdp_context.md)**. This document is about what an
*import* script does on top of it.

## The reference shape

```python
def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(), argv=argv, __file__=__file__
    )
    out_base = ctx.make_out_path(_SELF_NAME + "_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    # 1. Read every input file FIRST (no database yet): parse, validate,
    #    abort on bad input before anything is opened for writing.
    builder = SingleTableUpsertPlanBuilder(
        _TABLE_NAME, "number", [], time_zone=ctx.hitobito_time_zone
    )
    for raw_path in ctx.parsed_args.files:
        builder.merge_values(_read(pathlib.Path(raw_path)))

    with ctx:
        # 2. Load + plan on a READ-ONLY connection.
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        builder.load_existing(ro_conn)
        planned = builder.plan()

        # 3. Show what an approval would apply -- BEFORE asking for it.
        _log_plan_summary(planned)

        if not planned.inserts and not planned.updates:
            _LOGGER.info("nothing to write (%d untouched)",
                         len(planned.untouched_keys))
            return
        if ctx.dry_run:
            _LOGGER.info("[dry-run] Not applying the plan.")
            return

        # 4. Approval, THEN the read-write connection, then apply.
        ctx.require_approval_to_run_in_prod()
        rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
        inserted, updated = planned.apply(rw_conn, now=ctx.start_time)
        _LOGGER.info("%d inserted, %d updated, %d untouched.",
                     len(inserted), len(updated), len(planned.untouched_keys))

        if ctx.parsed_args.rollback_for_testing:
            _LOGGER.warning("ROLLBACK (--rollback-for-testing) - nothing committed")
            rw_conn.rollback()
        # No conn.commit(): leaving `with ctx:` cleanly IS the commit.
```

## The conventions

### 1. The module docstring is the contract

Start with one summary line (the CLI reuses it, see 2), then document: which
**sources** are recognised and how they are detected, which **columns** each
source writes, which columns it deliberately never touches, the **key**, and
the special rules (transliteration, "empty means keep", deletion semantics).
Somebody debugging a surprising UPDATE should find the answer there without
reading the code.

### 2. CLI

```python
p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
p.add_argument("files", nargs="+", help="…; the source is detected per file.")
p.add_argument("--rollback-for-testing", action="store_true", default=False,
               help="Apply the plan, then ROLLBACK instead of committing (testing).")
```

`--dry-run/-n`, `--start-time` and `--today` come from the context itself --
never define them again. Input files are positional and variadic: one call
imports every source of a table, the reader is picked per file by
header/extension, never by a `--type` flag.

### 3. One log file (and output directory) per run

`__file__=__file__` puts the outputs under `data/<script>/` instead of the
shared `data/`; `_SELF_NAME + "_{{ filename_suffix }}"` gives every run its
own timestamped log (with `_PROD` appended in production). Do not print a
closing "Output directory: …" summary by hand -- leaving `with ctx:` logs it,
including every file registered with `ctx.register_output_file(...)`.

### 4. Read files first, database second

Parse, normalise and validate every input file before opening a connection.
Structural errors (unknown format, missing header field, a 6-digit number
where a ledger account is expected) must abort with `SystemExit` while
nothing is open for writing.

### 5. Plan/apply instead of row-by-row writes

Build the target rows and hand them to
`SingleTableUpsertPlanBuilder` (`wsjrdp2027._internal.single_table_upsert_plan`);
`load_existing()` reads the stored state read-only, `plan()` diffs it per row
**and per column**, `apply()` executes it through
`pg_table_insertmany`/`pg_table_updatemany` in psycopg pipeline mode. What you
get for free: a preview before writing, column-granular UPDATEs (manual edits
in untouched columns survive), minimal JSONB deltas, idempotency, and one
round trip per statement batch instead of per row.

Useful knobs: `key_col` accepts a sequence for a **composite key**;
`merge_values()` combines several files into one plan (later file wins,
`keep_existing_for_cp1252_equality` protects Unicode against a CP1252
transliteration); `plan(skip_update_for_cp1252_equality=…)` for the same
comparison against the database; `plan(replace_dict_columns=…)` for
snapshot-style JSONB columns.

### 6. Show the plan before asking for approval

`_log_plan_summary()` logs `planned.operation_counts()` per table plus the
first ~10 affected keys, and it runs **before**
`require_approval_to_run_in_prod()`. Whoever confirms in production must be
able to see what they are confirming.

### 7. Ask for approval only when there is something to write

Return early on an empty plan (`nothing to write (N untouched)`) -- an
idempotent re-run must not prompt at all, and must never open a read-write
connection. Same for `--dry-run`: log the plan, then return before the
approval.

### 8. No manual `commit()`

The commit happens when the `with ctx:` block exits cleanly; an exception
before that leaves the database untouched. `--rollback-for-testing` calls
`rw_conn.rollback()` at the end, so the automatic commit finds an empty
transaction. A script that calls `conn.commit()` itself has (at least) two
places that decide durability -- there should be one.

### 9. Timestamps: `created_at` on INSERT, `updated_at` only on a real UPDATE

`apply(..., now=ctx.start_time)` stamps `created_at` on inserted rows and
`updated_at` on updated ones (the `touch` mechanics live in the `pg_table_*`
helpers). A fresh import therefore leaves `updated_at` NULL everywhere --
that is the Rails convention and a useful signal: a non-NULL `updated_at`
means the row genuinely changed after its import. Pass the aware
`ctx.start_time`, not a naive value.

### 10. Provenance columns stay out of the diff

Metadata about *where* a row came from (`source_file`) must not turn an
otherwise identical row into an UPDATE just because the export file was
renamed. Keep such columns out of the diffed value sets and add them, after
`plan()`, only to the rows that are inserted or updated anyway (see
`_add_source_file()` in `import_datev_buchungsstapel.py`).

### 11. Idempotency is an acceptance criterion, not a hope

Re-running the same files must report `0 inserted, 0 updated` / "nothing to
write". Verify it (see the checklist) -- it is the property everything else
rests on: the re-import runbook, `--rollback-for-testing`, and the ability to
re-run a failed import without thinking.

## Old vs. new at a glance

| Aspect | Old scripts | Convention |
|---|---|---|
| Connection | `with ctx.psycopg_connect() as conn` (always read-write, no audit hook, no dry-run downgrade) | `with ctx:` + `hitobito_psycopg_connection(read_only=True/False)` |
| Approval | `require_approval_to_run_in_prod()` right after connecting, before anything is known | after the plan preview, and only when the plan is non-empty |
| Preview | none -- the log shows what happened, afterwards | `_log_plan_summary()` before the approval |
| Writes | per row (`pg_insert_*`, `cur.execute` in a loop) with `upsert=True`, or one batch INSERT with `ON CONFLICT DO NOTHING` | one plan, `apply()` in pipeline mode |
| Diff | none, or whole-row comparison | per row and per column, minimal JSONB deltas |
| Commit | manual `conn.commit()` / `conn.rollback()` | implicit at the `with ctx:` exit |
| Log file | `data/<script>.log`, all runs appended | `data/<script>/<script>_<timestamp>[_PROD].log` |
| `--dry-run` | ignored -- and the connection stays writable (see the warning above) | plan preview, no write; the connection is read-only anyway |
| `--rollback-for-testing` | absent | present |
| Docstring | often none | the contract (sources, columns, rules) |

## Migration checklist

For each script, in this order:

1. **Docstring**: write the contract (sources, columns written and never
   touched, key, special rules). `description=__doc__.splitlines()[0]`.
2. **Context**: add `__file__=__file__`; log to
   `_SELF_NAME + "_{{ filename_suffix }}"`; delete the hand-written closing
   "Output directory" lines.
3. **Split reading from writing**: move all parsing/validation ahead of the
   database block.
4. **`with ctx:`**: replace `with ctx.psycopg_connect() as conn:`; use
   `ro_conn` for loading/planning, request `rw_conn` only after the approval.
5. **Plan/apply**: replace the row loop with a
   `SingleTableUpsertPlanBuilder`. Match the old write semantics
   deliberately -- which source owns which column, does an empty field clear
   or keep, are JSONB keys merged or replaced -- and write it into the
   docstring.

   Watch out for the **change in update semantics**, the one thing a
   migration can silently get wrong:

   * `ON CONFLICT DO NOTHING` (e.g. `insert_moss_balance_movement`) means an
     already-imported row is never touched again -- a re-import with
     corrected data does nothing. The plan/apply flow instead aligns the row
     with the file. That is usually what you want, but it now overwrites
     values that used to be frozen after the first import: decide per column
     whether the file or the database owns it, and keep manually maintained
     columns out of the value sets entirely.
   * `upsert=True` helpers usually rewrite the whole row; the plan writes
     only genuinely changed columns. Anything a person edits in the app
     therefore survives -- as long as no source lists that column.
6. **Preview + approval order**: add `_log_plan_summary()`, return early on
   an empty plan and on `ctx.dry_run`, then approve, then apply.
7. **Timestamps**: `apply(..., now=ctx.start_time)`; drop hand-set
   `created_at`/`updated_at` values.
8. **Commit**: delete `conn.commit()`; add `--rollback-for-testing`.
9. **Provenance**: take `source_file` (and friends) out of the diff.
10. **Clean up**: remove commented-out leftovers.

### Verifying a migration

Run against **dev** (never production) and require all four:

1. **Equivalence** -- run the migrated script against the data the old one
   wrote: it must report *nothing to write*. This is the strongest evidence
   that the rewrite computes the same target state.
2. **Fresh import** -- `TRUNCATE` the table, import, then compare the
   business columns against the pre-truncate state (e.g. an MD5 over all
   columns except `id`/`created_at`/`updated_at`, ordered by the key). It
   must be identical, and `updated_at` must be NULL everywhere.
3. **Idempotency** -- run again: *nothing to write*.
4. **Rollback** -- `--rollback-for-testing` on an emptied table leaves it
   empty.

Plus `uv run ruff check`, `uv run ruff format --check`, `uv run ty check`
(no new diagnostics) and the unit tests.
