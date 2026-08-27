# The WsjRdpContext pattern for CLI scripts

`wsjrdp2027.WsjRdpContext` is the runtime harness every script in this
repository builds on: it loads the selected config, owns the start time,
the output directory, logging, database/mail/SSH resources -- and, most
importantly, the safety layers that stand between a script and the
production database. This document describes the canonical `main()`
pattern as implemented by `accounting_tools/revert_sepa_direct_debit.py`,
and why new scripts should follow it.

## The canonical `main()` shape

```python
def main(argv=None) -> int:
    ctx = wsjrdp2027.WsjRdpContext(
        argv=argv,
        argument_parser=create_argument_parser(),
        __file__=__file__,
    )
    args = ctx.parsed_args

    out_base = ctx.make_out_path("my_script_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        # ... gather data, build a plan, show a preview (read-only) ...

        # ... interactive confirmation / ctx.require_approval_to_run_in_prod ...

        rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
        # ... apply the plan; write output files via
        #     ctx.register_output_file(...) ...
    return 0
```

Three ideas carry the pattern: **one output basename per run**, **a
read-only connection for everything up to the decision**, and **a
read/write connection only after the decision** -- with commit and
cleanup handled by the context.

## One log file (and output basename) per run

* `ctx.filename_suffix` renders `start_time` as `YYYYMMDD-HHMMSS`; in
  production the config "kind" is appended in upper case (e.g.
  `..._PROD`). `start_time` is fixed once per process (overridable
  outside production via `WSJRDP_SCRIPTS_START_TIME`), so every
  execution gets a distinct suffix, second-precise, and every artifact
  of one run shares one basename:

  ```
  data/my_script_20260827-193042.log
  data/my_script_20260827-193042.apply.sql
  data/my_script_20260827-193042.undo.sql
  ```

* `ctx.make_out_path("my_script_{{ filename_suffix }}")` renders the
  Jinja template, verifies the result stays under `ctx.out_dir` (no
  path traversal) and creates the parent directories. Derive sibling
  files with `out_base.with_suffix(...)`.

* `ctx.configure_log_file(path)` attaches a `FileHandler` AND flushes a
  buffering handler that has captured every log record since process
  start -- the log file therefore contains the early `[config]`/`[ctx]`
  lines emitted before the file existed. The log file is registered as
  an output file.

* `ctx.register_output_file(description, path)` records further
  artifacts. When the outermost `with ctx:` block exits, the context
  logs a summary of the output directory and every registered file.

Without the `{{ filename_suffix }}` template (e.g.
`make_out_path("my_script").with_suffix(".log")`) all runs append to
one shared file -- workable, but runs are hard to separate and
concurrent runs interleave.

## `with ctx:` -- resource lifecycle and commit semantics

`WsjRdpContext.__enter__` pushes an `ExitStack` (and sets the context
as thread-local). Every resource the context creates on demand --
psycopg clients, SSH tunnels, mail connections -- is entered into that
stack. On `__exit__`:

* Each cached `PsycopgClient` is closed. Its own `__exit__` **commits
  the open transaction if the block is left without an exception**
  (logged as `COMMIT`, at INFO level for read/write connections) and
  merely closes -- i.e. effectively rolls back -- when an exception is
  propagating. A script following the pattern therefore does not call
  `conn.commit()` at all: reaching the end of the `with ctx:` block IS
  the commit, and any crash before that leaves the database untouched.
* SSH tunnels and other resources are torn down in reverse order.
* The outermost exit prints the output-file report described above.

`ctx.dry_run` and flags like `--rollback-for-testing` compose with
this: calling `rw_conn.rollback()` before the block ends discards the
work, and the final automatic COMMIT then commits an empty transaction.

## Read-only vs. read/write connections

`ctx.hitobito_psycopg_connection(read_only=...)` returns a connection
from a per-key cached `PsycopgClient` (`ro`, `rw`, plus autocommit
variants; repeated calls with the same flags return the same client).

* **`read_only=True`** -- the connection is switched to read-only at
  the PostgreSQL level (`Connection.set_read_only(True)`, or
  `SET default_transaction_read_only TO TRUE` for autocommit clients).
  Any write through this connection fails in the database, whatever
  the Python code does. No approval hook is attached: reading is
  always allowed.
* **`read_only=False`** -- a separate client. Two safety layers attach
  automatically:
  * **dry-run downgrade:** when `ctx.dry_run` is set, the request is
    downgraded to a read-only client (logged). A dry run cannot write,
    even through code paths that ask for `read_only=False`.
  * **the audit hook:** the client is created with
    `ctx.create_audithook("Hitobito DB")`. Every `get_connection()` /
    `cursor()` call first invokes
    `require_approval_to_run_in_prod(category="Hitobito DB", ...)`.

The recommended flow -- fetch `ro_conn` first, do ALL selection,
planning and preview work there, and request `rw_conn` only after the
user has confirmed -- keeps the window in which a writable connection
exists as small as possible, and makes "the script only read so far"
a property enforced by the database, not by code review.

## How production writes are gated

Several independent layers must all agree before anything is written
in production:

1. `is_production` comes from the loaded config file; development and
   integration configs never trigger the production paths.
2. **Explicit script-level approval:**
   `ctx.require_approval_to_run_in_prod(prompt=...)` asks on the
   console and raises `SystemExit(0)` when declined. Outside
   production (or for `read_only=True` actions) it returns
   immediately, so scripts can call it unconditionally. With a
   `category`, a given approval is cached and repeated calls for the
   same category pass silently.
3. **The audit hook as a safety net:** even a script that forgets step
   2 cannot obtain a usable read/write connection in production -- the
   first `get_connection()`/`cursor()` on the `rw` client triggers the
   same console approval (category `"Hitobito DB"`). In non-production
   the hook is a no-op. (Because the explicit prompt in step 2 is
   usually category-less, production runs typically confirm twice:
   once for the concrete action, once for the database write access.
   That is deliberate belt-and-suspenders.)
4. **dry-run downgrade** (see above): `--dry-run` makes writable
   connections unobtainable altogether.
5. Read-only connections are enforced by PostgreSQL itself.

## Contrast: the legacy `ctx.psycopg_connect()`

Older scripts use `with ctx.psycopg_connect() as conn:`. That helper
opens a single **read/write, audit-hook-free** client outside the
resource cache, does not participate in the dry-run downgrade, and
leaves commits to the script. All safety then rests on the script
remembering `require_approval_to_run_in_prod()` and checking
`ctx.dry_run` itself. New scripts should prefer the
`with ctx:` + `hitobito_psycopg_connection(read_only=...)` pattern.
