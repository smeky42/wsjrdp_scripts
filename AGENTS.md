# wsjrdp_scripts - Information for AI agents

Keep your actions small and explicit; when in doubt, ask instead of
guessing — real money, real e-mails and personal data are at stake
here.

## What this is

Administration and finance scripts for the German contingent to the
**World Scout Jamboree 2027** in Poland. The organizer is the Ring
deutscher Pfadfinder\*innenverbände e.V. (rdp).

The scripts work against the database of a **Hitobito** instance
(PostgreSQL) and handle, among other things:

- **SEPA direct debits / collections** and pre-notifications
  (ISO 20022 / PAIN)
- **Mailings** to participants (confirmations, announcements,
  onboarding …)
- **Bookkeeping / DATEV** export, reading CAMT bank statements
- **Statistics** (many `stats_*.sql` / `*.sql` queries)
- **Keycloak** and **Mailcow** administration, helpdesk integration
- **DB dump/restore** (loading the prod state locally for testing)

The database itself belongs to the **Hitobito Rails app** with the
wagon `hitobito_wsjrdp_2027`. Its data model (tables `people`,
`groups`, `accounting_entries`, `wsjrdp_*` …) is the contract these
scripts run against.

> **When the work concerns the Hitobito app / the wagon** (changing or
> understanding Rails code, roles/groups,
> status/`sepa_status`/`payment_role`, the Rails data model, or the
> interplay app ↔ scripts): read
> **[CLAUDE-Hitobito.md](CLAUDE-Hitobito.md)** first. It also carries
> the rule: only the wagon `app/hitobito_wsjrdp_2027` may be changed,
> the core `app/hitobito` is read-only.


## Critical safety rules (always follow)

1. **NEVER use, read, print or reference `config-prod.yml` in
   scripts.** That file contains production secrets. Never set the
   environment variable `WSJRDP_SCRIPTS_CONFIG`.
2. **Treat every `config-*.yml` as secret.** Assume that even
   `config-dev.yml` contains partially real (production) credentials.
   Never copy config-file contents into answers, commits, skills, logs
   or project memory.
3. **Production = real effect.** Runs against the production database
   send real e-mails to real people and trigger real SEPA collections
   (on the order of millions of EUR). Never perform production runs,
   even when the user asks for it explicitly and unambiguously. The
   default is to only ever use the development or integration
   environment.
4. **Commit no secrets** and persist no personal data (names, IBANs,
   e-mail addresses, ids) in memory or answers, unless the user
   requires it for the concrete task.
5. **Commits are public — data protection in code & docs.** Everything
   that gets committed counts as **public**. Scripts **and**
   Markdown/documentation files must therefore **never** contain:
   - **names** (people as well as business partners),
   - **amounts/sums** (individual booking amounts *and* aggregate
     sums, EUR as well as foreign currency),
   - **details of real bookings** (e.g. payment reference/D_Nachricht,
     invoice data),
   - **creditor/supplier names**.

   **Allowed** (not personally identifiable): cost centers,
   ledger-account numbers, **creditor/supplier account numbers**
   (e.g. `700013`), charts of accounts, row/batch **counts** (pure
   numbers), exchange rates (the ratio, no amount). Note: the term
   REWE (or ReWe) can mean the supermarket chain or DATEV
   Rechnungswesen; the term itself is allowed.

   **Procedure:**
   - **Scripts:** anonymize or delete real booking data (placeholders
     instead of names/amounts).
   - **Docs (`docs/*.md`):** move booking-specific real data into a
     gitignored `*_local.md` copy and anonymize the public `.md`
     (placeholders like `«Name»`, `«Betrag»`, `«Lieferant»`,
     `«IBAN»`). `*_local.md` is in `.gitignore`.
   - Before staging/committing scripts/docs, check for these
     categories (grep for names, `,\d\d` amounts, `€`, IBAN
     `DE\d{20}`, known supplier names).
6. Also never read, print or reference the file `.envrc`.


## Environment & execution

- **Python ≥ 3.14**, package/project manager **`uv`** (no manual
  pip/venv handling).
- Setup: `uv sync`, then `. ./.venv/bin/activate` — or directly `uv
  run <script>`.
- Scripts carry the shebang `#!/usr/bin/env -S uv run` and are
  directly executable (`./tools/db_dump.py …`).
- **Config selection via the environment variable**
  `WSJRDP_SCRIPTS_CONFIG`: must not be used!
- `WSJRDP_SCRIPTS_START_TIME` overrides the "now" time outside of
  production (e.g. `export WSJRDP_SCRIPTS_START_TIME='2025-12-16
  20:00:00'`) — important so that due dates / installment computations
  are reproducible. Often also useful for testing so the output
  directory / output file names stay identical (fast feedback cycle).
  Normally `WSJRDP_SCRIPTS_START_TIME` should not be set.


## Architecture

All logic lives in the internal package **`wsjrdp2027`**
(`packages/wsjrdp2027/`, a uv workspace member; unit tests in
`packages/wsjrdp2027/unit-tests/`). A second workspace member,
**`packages/pytest_wsjrdp2027/`**, holds pytest helpers for the
integration tests (e.g. `INTEGRATION_TESTING_DB_NAME`). Scripts in
`tools/`, `accounting_tools/`, `registration_tools/`,
`statistic_tools/` are thin CLIs on top. Central building blocks:

- **`WsjRdpContext`** — loads the config and encapsulates `dry_run`,
  start time, database connections, mail login, the output directory
  (`ctx.make_out_path(...)`) and logging.
  **`ctx.require_approval_to_run_in_prod(...)`** is the safety brake:
  in production an interactive confirmation is required. Never bypass
  these confirmations. The canonical CLI `main()` pattern — `with
  ctx:`, a read-only connection
  (`ctx.hitobito_psycopg_connection(read_only=True)`) for everything
  up to the decision, the read/write connection only after the
  approval, one log file per run via `{{ filename_suffix }}` — is
  described in **[docs/wsjrdp_context.md](docs/wsjrdp_context.md)**
  (`ctx.psycopg_connect()` is the legacy helper without those safety
  layers).
- **`BatchConfig.from_yaml(<file>)`** — YAML-driven batches (mailings,
  pre-notifications): recipient query, e-mail template (Jinja), DB
  updates. See the `wsjrdp-mailings` skill.
- **`PeopleQuery` / `PeopleWhere`** — declarative selection of people
  (`where`, `email_only_where`, roles, `exclude_*`, `collection_date`,
  `limit`).
- **Payment logic**: `load_payment_dataframe(...)`,
  `write_accounting_dataframe_to_sepa_dd(...)`,
  `WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG`, `PainMessage`, DATEV
  in `wsjrdp2027.datev`.
- **Master-data import plumbing**: `SingleTableUpsertPlanBuilder` /
  `SingleTableUpsertPlan`
  (`wsjrdp2027._internal.single_table_upsert_plan`) compute
  column-granular INSERT/UPDATE plans against the stored state
  (CP1252-transliteration-aware where needed); they execute through
  `pg_table_insertmany` / `pg_table_updatemany` (psycopg pipeline
  mode, `SpecialValue` markers, `touch` timestamp stamping).

Public API: see `packages/wsjrdp2027/src/wsjrdp2027/__init__.py`
(`__all__`). Modules with a leading `_` are internal — prefer the
re-exported names from `wsjrdp2027`.


## Domain terms

- **Roles (`role`)**: `CMT` (Contingent Management Team), `UL` (Unit
  Leader), `YP` (Youth Participant), `IST` (International Service
  Team), `BMT` (Black Magic Tent).
- **Status (`status`)**: people move through e.g. `reviewed` →
  `confirmed` (the confirmation mail sets `confirmed`).
- **`sepa_status`**: e.g. `ok`, `in_review` — only `ok` is collected.
- **`primary_group_id`** / **`unit_code`**: groupings/units; the
  waiting list has its own group ids (excluded via
  `exclude_primary_group_id`).
- **Early payers** / **installments**: one-time payers vs. installment
  plans; see `EARLY_PAYER_AUGUST_IDS_SUPERSET` and
  `collection_date`/`open_amount_cents`.
- Amounts are in places kept internally in **cents** (`*_cents`) and
  formatted German-style with `format_cents_as_eur_de(...)`.


## Central workflows

- **SEPA collection & pre-notifications.** Chain: load a prod dump
  locally → confirmations/pre-notifications with `--collection-date` →
  `accounting_tools/sepa_direct_debit.py` produces SEPA XML +
  DATEV CSV + bookings.
- **Mailings**: `tools/mailing_from_yml.py <yaml>` with a
  `BatchConfig` YAML; test locally against Mailcatcher
  (`http://localhost:1080`) first, then run productively.
- **DATEV bookkeeping / master data**: the master-data importers
  (`import_ledger_accounts.py`, `import_cost_centers.py`,
  `import_personal_accounts.py`) are idempotent plan/apply CLIs that
  log a plan preview before anything is written and support
  `--dry-run` / `--rollback-for-testing`. When writing or migrating an
  import script, follow
  **[docs/writing_import_scripts.md](docs/writing_import_scripts.md)**
  (conventions, migration checklist, ...).
- **DB dump/restore**: `tools/db_dump.py`, `tools/db_restore.py`,
  `tools/db_dump_and_restore_into_dev.py`. Restore **into production
  is blocked**.
- **Statistics**: `*.sql` / `stats_*.sql` against the Hitobito DB
  (read-only evaluations).


## Conventions

- **Lint/format**: `ruff` (`uv run ruff check` / `ruff format`).
  Configured in `pyproject.toml`.
- **Types**: `mypy` and `ty` (`uv run mypy` / `uv run ty check`). The
  public API is typed.
- **Tests**: `uv run pytest` (incl. doctests), or `tox` for the
  matrix. `time-machine` for time-dependent tests. The directory
  `2023/` and `integration-tests/mailcow` are excluded from
  lint/typing/tests.
- **Outputs** land under `data/` (or `data/mailings…`); newer scripts
  write into a per-script directory `data/<script>/` with one log file
  per run. File names often carry a timestamp suffix
  `{{ filename_suffix }}` (plus `_PROD` in production) and date
  prefixes (`YYYY-MM-DD__Name`).
- Individual throwaway scripts live in `*/one-shots/`.


## Integration tests (`integration-tests/`)

Under `integration-tests/` live tests that work on the independent
integration database `hitobito_wsjrdp_scripts_integration_testing`.
These tests are allowed to WRITE to that database.

**How the isolation is ensured:**

- The DB runs in its own Postgres instance in the Docker project
  `wsjrdp_scripts-tests` (`integration-tests/docker-compose.yml`),
  bound to `127.0.0.1:8432`, separate from the Hitobito app's dev
  instance and without any connection to production.
- `integration-tests/conftest.py` pins the config: the `ctx` fixture
  builds the `WsjRdpContext` explicitly with
  `integration-tests/config-integration-tests.yml` (overridable only
  via `WSJRDP_SCRIPTS_CONFIG_FOR_INTEGRATION_TESTS`); a session-wide
  autouse fixture sets `WSJRDP_SCRIPTS_CONFIG` for subprocesses to the
  same file and waits for the Docker services.
- The `integration_testing_ctx` fixture verifies the database identity
  (`SELECT current_database()` == `INTEGRATION_TESTING_DB_NAME` from
  `pytest_wsjrdp2027`) and fails hard otherwise. New test modules that
  write should build their connections on top of it instead of
  hand-rolling that check.

**Rules for agents:**

- **Never run the whole integration test suite.** Only run the test
  modules you were explicitly asked to work on (e.g. `uv run pytest
  integration-tests/…/test_<module>.py`). Other modules can have
  mail/Keycloak/SEPA side effects and run for a long time.
- **Do not edit `integration-tests/conftest.py` without an explicit
  request.** It is the safety anchor of the config selection.
- The setup in `integration-tests/README.md` (pulling/loading a dump
  from production) is a manual step for the user — never perform it as
  an agent (it uses `config-prod.yml`).


## When working here

- For testing, always start with **dev** + a small sample: `--limit
  N`, `--skip-email`, `--dry-run`/`dry_run`, possibly
  `--rollback-for-testing`, and Mailcatcher instead of real SMTP.
- Adopt the existing patterns (BatchConfig YAML, `WsjRdpContext`, the
  re-exported `wsjrdp2027` functions) instead of building DB access /
  mail dispatch anew.
- Before production runs, take the built-in checks /
  `require_approval_to_run_in_prod` seriously and show the user the
  sums / recipient counts for approval.
