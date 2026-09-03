# CLAUDE-Hitobito.md — Hitobito Rails app (wagon `hitobito_wsjrdp_2027`)

Notes for Claude for working on the **Ruby on Rails Hitobito application**
whose database the Python scripts in this repo (`wsjrdp_scripts`) run
against. This file is referenced from `CLAUDE.md` and should be read as soon
as changes to the Rails/Hitobito app are involved.

> Read this file whenever the user talks about the Hitobito app, the wagon,
> roles/groups, the Rails data model (people/groups/accounting …) or the
> relationship between the Python scripts and the DB. For pure Python script
> work `CLAUDE.md` is enough. Also read and follow the wagon repo's own
> `AGENTS.md` (in the wagon working tree, see below).

## Where things live

- **Additional working directory:** a local checkout of the Hitobito dev
  setup (corresponds to `github.com/hitobito/development`); its location is
  machine-specific — ask the user if it is not known. Paths below are given
  relative to that root as `development/…`.
- **Hitobito core:** `development/app/hitobito/` — **READ ONLY, NEVER
  CHANGE.** (Explicit user rule. The core is a separate upstream repo.)
- **Our wagon (changes go here):**
  `development/app/hitobito_wsjrdp_2027/` — its own git repo
  (`github.com/smeky42/hitobito_wsjrdp_2027`).
- Further wagons may live under `development/app/hitobito_*`.

**Rule:** changes exclusively in the wagon `hitobito_wsjrdp_2027`. The core
(`app/hitobito`) and other `app/hitobito_*` wagons are read-only.

## What a Hitobito wagon is (in short)

Hitobito is a Rails app for member administration. A **wagon** is a
Rails::Engine (via the `wagons` gem) that **extends the core instead of
forking it**. Instead of editing core classes, the wagon defines modules and
mixes them into the core classes at runtime via `include`/`prepend`.

Central file: **`lib/hitobito_wsjrdp_2027/wagon.rb`**. The
`config.to_prepare` block registers all extensions, among them:

- **Models:** `Person.include Wsjrdp2027::Person`,
  `Group.include Wsjrdp2027::Group`, `Event`, `AdditionalEmail`,
  `ActsAsTaggableOn::Tagging`.
- **Controllers:** e.g. `PeopleController.prepend Wsjrdp2027::PeopleController`,
  `GroupsController`, `MailingListsController`, `Person::QueryController`.
- **Decorators:** `PersonDecorator`, `ContactableDecorator`, `VersionDecorator`.
- **Helpers/sheets:** `Sheet::Base`, `NavigationHelper`, `StandardFormBuilder`.
- **Abilities (CanCan):** `PersonAbility`, `GroupAbility`, `EventAbility`,
  `MailingListAbility`, `RoleAbility`, `SubscriptionAbility`, `VariousAbility`.
- **Settings:** `initializer "wsjrdp_2027.add_settings"` additionally loads
  `config/settings.yml` (overriding core settings), reachable via `Settings.*`.

Rule of thumb: **create new code as a `Wsjrdp2027::*` module and hook it in
via the `to_prepare` block**, never touch core classes directly.

## Directory layout of the wagon

```
app/
  models/            # Person/Group extensions (wsjrdp_2027/*), own models
    wsjrdp_2027/     #   person.rb, group.rb, event.rb, wizards/, paper_trail/ …
    group/           #   root.rb, unit.rb, ist.rb, extern.rb  (group hierarchy + roles)
    concerns/        #   wsjrdp_transaction.rb (shared booking logic)
    accounting_entry.rb, wsjrdp_*.rb, moss_transaction.rb (+ moss_expense.rb, moss_booking.rb
    and their STI subclasses), wsj27_rdp_fee_rule.rb
  controllers/       # fin/ (finance), person/ (detail tabs), contingent/, wsjrdp_2027/, public/
  abilities/wsjrdp_2027/   # permissions
  decorators/wsjrdp_2027/  # presentation logic
  domain/wsjrdp_2027/      # export/pdf/registration/* (PDF generation), year_month*, payment_plan_conversion_helper
  helpers/           # many wsjrdp_*_helper.rb + sheet/* (UI structure/navigation)
  serializers/wsjrdp_2027/ # JSON API
  views/             # *.haml/.erb overrides
config/
  routes.rb          # additional routes (additive to the core)
  settings.yml       # app settings (status labels, sepa_status, roles, diets …)
  locales/wsjrdp_2027.de.yml  # German i18n strings (app is de-only)
  rdp_groups*.yml    # association/group structure (dev/new/prod)
db/
  migrate/           # wagon migrations, timestamp prefix — details below
  schema.rb          # COMBINED schema (core + wagon) — auto-generated, the data-model reference
  schema.rb.diff     # auto-generated diff: ONLY the wagon's additions over the core schema
  seeds/             # groups.rb + development/{0_groups,1_people,events}.rb
lib/hitobito_wsjrdp_2027/  # wagon.rb, version.rb
spec/                # RSpec (abilities/, features/, models/, api/, fixtures/, support/)
```

## Data model / domain

### Roles & group hierarchy (`app/models/group/*.rb`)
Groups are Rails STI (`groups.type`). Each group class is a `layer` and
defines its role classes with CanCan `permissions`:

- **`Group::Root`** (CMT / contingent root): roles `Admin`
  (`layer_and_below_full, admin, finance`), `Leader`, `Finance`, `Member`.
- **`Group::Unit`**: `Manager`, `Leader` (`group_full`), `UnapprovedLeader`,
  `Member` (YP).
- **`Group::Ist`**: `Leader` (MIST), `Member`.
- **`Group::Extern`**: `Member` (does not attend the Jamboree).

**WSJ roles** (domain-level, derived — see `Wsjrdp2027::Person`'s
`default_wsj_role` / `wsjrdp_role_by_primary_group`): `CMT`, `UL` (Unit
Leader), `YP` (Youth Participant), `IST`, `BMT`, `EXT`, `JPT/JDT`. The
mapping role type → WSJ role resp. payment role lives in `Wsjrdp2027::Person`
(`WSJRDP_ROLE_TYPE_TO_WSJ_ROLE_MAP`, `WSJRDP_ROLE_TYPE_TO_PAYMENT_ROLE_TYPE_MAP`).

**Concrete primary_group_ids in the dev DB** (may differ in prod — never
rely on them hard, but useful for orientation; cf.
`Person#wsjrdp_role_by_primary_group`): `1` = Group::Root "German
Contingent…" (CMT), `47` = "CMT Warteliste", `4` = "IST Registration",
`7` = "IST Warteliste", `45` = "BMT", `48`/`53` = Extern, `49–52` = IST
regions, `Group::Unit` ids (8, 9, 12, 13, 17–24 …) = units A*/B*. Groups
carry `groups.is_wsjrdp` (bool, default true) to separate "real" contingent
groups from the waiting list etc.

### Person (`app/models/wsjrdp_2027/person.rb`)
The largest extension. Key points:

- **Status** (`people.status`, string, see `Settings.status`):
  `registered → printed → upload → in_review → reviewed → confirmed`; besides
  those `deregistration_noted`, `deregistered`. (The confirmation mail sets
  `confirmed`.)
- **`sepa_status`** (`Settings.sepa_status`): `ok`, `in_review`,
  `invalid_account`, `missing`, `individual_ok`, `individual_missing`. Only
  `ok`/`individual_ok` mean a clean collection.
- **Payment roles:** `payment_role` (e.g. `EarlyPayer::Group::Unit::Member`),
  `early_payer` (one-time payer vs. installments). Helpers: `cmt?/ul?/yp?/ist?`,
  `short_payment_role` → `CMT/UL/YP/IST/EXT`, `ensure_payment_role`.
- **Fees (kept in cents):** `total_fee_cents`/`_eur`, `amount_paid_cents`
  (= sum of the `accounting_entries`), `regular_full_fee_cents`,
  `wsjrdp_total_fee_reduction` (+ `_hint`/`_comment`), default full fee
  340000 ct.
- **Installments/payment plans:** `yme_list` (list of `YearMonthEur`),
  `installments_string`, `active_fee_rule`/`planned_fee_rule`
  (→ `Wsj27RdpFeeRule`), otherwise fallback to `WsjrdpPaymentPlan` by
  `wsjrdp_role` + `single_payment`.
- **SEPA:** `sepa_iban/bic/name/address/mail`, `sepa_mandate_id` (default
  `"wsjrdp2027#{id}"`), IBAN validation via `iban-tools`.
- **`additional_info` (jsonb):** catch-all for many attributes via
  `jsonb_accessor` — among them `moss_*` (MOSS account/spend),
  `wsjrdp_email*`, `keycloak_username`, `deregistration_*`,
  `short_last_name`, `raw_wsjrdp_role`, `planned_total_fee_reduction*`.
  **Careful:** such store-accessor fields must **not** be added to
  `FILTER_ATTRS`/`PUBLIC_ATTRS` (see the comments in the code).
- **Buddy ids:** `buddy_id`, `buddy_id_ul`, `buddy_id_yp` (format
  `<tag>-<id>`, validated against a real person with a matching role).
- **Uploads/documents:** `upload_*_pdf` (contract, sepa, medical, passport,
  photo_permission, good_conduct, data_agreement, recommendation);
  `upload_complete?` checks role-dependently.
- **Geocoding:** address → `latitude/longitude` (before_save, `geocoder`).
- **PaperTrail:** internal attrs (`WSJRDP_INTERNAL_ATTRS`) are excluded from
  the audit trail.
- Callbacks: `maybe_update_payment_or_wsj_role`, `geocode_full_address`,
  `tag_good_conduct_missing` (sets the tag `eFZ-Einsicht-fehlt` when the
  certificate of good conduct is missing), `_save_planned_fee_rule`.
- `zero_padded_id` is a **generated** DB column (t.virtual) — not writable,
  not searchable via SEARCHABLE_ATTRS.

### Finance / accounting (own models + `fin/` controllers)
- **`AccountingEntry`** (`accounting_entries`): booking, `belongs_to :subject`
  (polymorphic, usually Person), `amount_cents`/`_eur`, linkable to
  `direct_debit_payment_info`, `direct_debit_pre_notification`,
  `payment_initiation`, `camt_transaction`, `moss_booking`;
  reversal via `reverses`/`reversed_by`.
- **`WsjrdpDirectDebitPreNotification`** (`pn`): SEPA pre-notification
  (`collection_date`, `amount_cents`, `payment_status`, debtor/creditor
  fields).
- **`WsjrdpPaymentInitiation`**, **`WsjrdpDirectDebitPaymentInfo`**: a SEPA
  collection run.
- **`WsjrdpCamtTransaction`** (`tx`): imported bank-statement entries
  (camt.05x); the `WsjrdpTransaction` concern provides candidate matching
  (person from the payment reference via `CMT/UL/IST/YP … <id>` patterns)
  and deny lists.
- **`WsjrdpFinAccount`** (`acc`): bank accounts (IBAN, opening balance …).
- **`WsjrdpPaymentPlan`** / **`Wsj27RdpFeeRule`**: installment plans
  (standard vs. individual).
- **`MossTransaction`** / **`MossExpense`** / **`MossBooking`** (`moss_booking`):
  the three levels of the unified Moss model (one transaction -> its expenses ->
  its splits; STI per kind: card transaction, invoice, reimbursement, top-up).
  Design: the wagon's `doc/plans/2026-08_moss-transaction-unification.md`.
- **Master data:** `WsjrdpLedgerAccount`
  (`wsjrdp_ledger_accounts`), `WsjrdpPersonalAccount`
  (`wsjrdp_personal_accounts`, Debitoren/Kreditoren),
  `WsjrdpCostCenter` (`wsjrdp_cost_centers`), `WsjrdpSphere`
  (`wsjrdp_spheres`) — filled/reconciled by the Python master-data
  importers in `accounting_tools/`.
- **`WsjrdpNote`**, **`WsjrdpDocument`**, **`WsjrdpConfig`**:
  notes/documents/config.
- **Money always in cents** (`*_cents`); EUR views via
  `eur_attribute`/helpers.

### Routes (`config/routes.rb`, additive)
- Person detail tabs: `.../people/:id/{print,upload,medical,status,unit,accounting}`,
  plus `finance/fee/spend/deregistration/debit_return`.
- Finance area under `scope "fin"`: `ae` (accounting_entries), `pn`, `tx`,
  `acc`, `moss_booking`, `payment_plans`, `person_fees`.
- `namespace :contingent`: overviews `cmt`, `ist`, contingent.
- `public/statistics` (public), `groups/:id/map`, `groups/:id/statistics/data`.

### Permissions (`app/abilities/wsjrdp_2027/*`)
CanCan via `permission(...).may(...).<constraint>`. The wagon **tightens**
the core defaults (lots of `.nobody`), e.g. in `PersonAbility`:
`history/log/security` → nobody; the `finance` role on Root may `fin_admin`;
`admin` may `update_wsjrdp_email`, `update_moss_email`, Keycloak username.
When changing permissions, start here, not in the core.

## Database & migrations (`db/`)

> **Ground rule for schema changes:** creating or changing tables/columns
> always means **writing a new migration** — never touch the live schema
> directly and never edit `schema.rb` by hand (the file is auto-generated).
>
> **The dividing line is the `main` branch:**
> - **Migrations that are already on `main` are NEVER touched again** (they
>   count as "baked" and are already applied in dev and prod). Corrections
>   happen exclusively through a **further** migration with a newer
>   timestamp.
> - **As long as a migration is NOT yet on `main`** (fresh, in progress), it
>   is edited in place instead of adding another migration. In particular:
>   **DB changes to a table that is newly created in a not-yet-merged
>   migration belong INTO exactly that migration** — no separate second
>   migration for such a freshly created table.

### Layout of `db/`
- **`db/migrate/`** — currently **41 migrations**, file name
  `YYYYMMDDHHMMSS_snake_case_name.rb` with the class name in CamelCase. The
  14-digit **timestamp = version = order**. All of them are already `up`
  (applied); latest version `20260823000300`.
- **`db/schema.rb`** — auto-generated, **combined** snapshot (core **+**
  wagon). `ActiveRecord::Schema[7.1].define(version: 2026_08_23_000300)`.
  Best reference for the real data model. **Do not edit by hand.**
- **`db/schema.rb.diff`** — auto-generated diff showing **only the wagon's
  additions** over the core schema (handy to see "what is ours").
- **`db/seeds/`** — `groups.rb`, `development/{0_groups,1_people,events}.rb`
  (dev test data; run via `db:seed:all` / `wagon:seed`).

### Procedure when creating or changing a table/column
1. **New file** in `db/migrate/` with a **fresh, ascending timestamp**
   (greater than `20260823000300`; no collision with existing versions).
2. Write the migration (conventions below).
3. Apply it (the core's wagon migration runner):
   ```bash
   # recommended:
   docker exec development-rails-1 bash -lc \
     'cd /usr/src/app/hitobito && bundle exec rails wagon:migrate'
   # a single version up/down:
   #   rails app:db:migrate:up   VERSION=<ts>
   #   rails app:db:migrate:down VERSION=<ts>
   ```
   Wagon migrations are recorded in the **same** `schema_migrations` table
   as core migrations; check the status with `rails wagon:migrate:status`
   resp. `rails db:migrate:status_all` (shows the source wagon
   `[wsjrdp2027]`).
4. **Re-dump the schema** so `schema.rb` + `schema.rb.diff` are current:
   ```bash
   docker exec development-rails-1 bash -lc \
     'cd /usr/src/app/hitobito && bundle exec rails wagon:schema_dump'
   ```
   (The generated `schema.rb`/`schema.rb.diff` belong into the commit.)
5. **Keep it reversible:** prefer `def change`; for raw SQL / data
   migrations use `reversible do |direction| … end` with `up`/`down` (see
   `change_accounting.rb`, `add_extern_group_type.rb`), so a single `down`
   works cleanly.

> ⚠️ **Do NOT use `wagon:revert` for "one migration back"!** `wagon:revert`
> rolls back the **whole wagon** (all migrations, `down` from newest to
> oldest) and thereby **drops tables/columns including data** in the dev DB.
> To take back exactly **one** migration, always use the single version:
> ```bash
> docker exec development-rails-1 bash -lc \
>   'cd /usr/src/app/hitobito && bundle exec rails app:db:migrate:down VERSION=<ts>'
> ```
> For a **brand-new, not-yet-merged** table whose migration you are
> reworking: take exactly that one version `:down`, edit the migration, then
> `wagon:migrate` (forward) — never `wagon:revert`.

> Useful rake tasks (from the core, acting on loaded wagons):
> `wagon:migrate` (forward, safe) · `wagon:migrate:status` ·
> `wagon:schema_dump` · `app:db:migrate:up|down VERSION=<ts>` (single
> migration) · `hitobito_wsjrdp2027_wagon:install:migrations` (copies wagon
> migrations into the app) · `db:reset:all` / `db:setup:all` / `db:seed:all`.
> **Dangerous (drops data):** `wagon:revert` (whole wagon), `db:reset:all`.

> **Dev DB broken / data lost?** Full restore of a **fresh prod dump into
> the dev DB** (no prod access, just a local dump file; `config-dev.yml` is
> the default config):
> ```bash
> PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH" \
>   uv run tools/db_restore.py --terminate-other-clients <data/hitobito_production_*.dump>
> ```
> Afterwards `wagon:migrate` (brings the wagon migrations back up) + re-import
> if needed. Pulling a **fresh** prod dump is user-only (needs `config-prod`,
> off-limits for me).

### Migration conventions (follow the existing ones)
- **Header:** `# frozen_string_literal: true` + AGPL copyright header
  ("German Contingent for the World Scout Jamboree 2027 … COPYING …") as in
  the newer migrations. (Do not imitate old 2025 migrations without the
  header.)
- **Base class:** new migrations inherit from
  **`ActiveRecord::Migration[7.1]`** (Rails 7.1; older ones used `[4.2]`).
- **Naming pattern:** `Add<Table>Attrs<Topic>` / `AddWsjrdp<Table>` /
  `Change<Table>…` / `Move…` depending on purpose; file name in matching
  snake_case.
- **Own tables** are named `wsjrdp_*` (resp. `wsj27_rdp_*`,
  `accounting_entries`, `moss_transactions` / `moss_expenses` /
  `moss_bookings`). Person/group attributes
  are attached **directly to the core tables** `people` / `groups`
  (`add_column :people, …`).
- **Money amounts** always as `*_cents` integers (never float/decimal euros
  in the DB).
- **Enums/status** as `:string` with a sensible `default:`/`comment:`; the
  human-readable labels live in `config/settings.yml`, not in the DB.
- **The catch-all column `additional_info` (`:jsonb`, default `{}`)** exists
  on `people`, `groups` and many `wsjrdp_*` tables; prefer putting new
  "soft" fields there via `jsonb_accessor` in the model instead of adding a
  dedicated column (cf. `Wsjrdp2027::Person`).
- **Further patterns in the code base:** array columns (`array: true`),
  generated stored columns (`as: %q{…}, stored: true`, e.g.
  `zero_padded_id`), references via `add_reference`/`t.belongs_to` with
  `foreign_key: { to_table: … }`, raw SQL via `execute <<-SQL … SQL`.
- **Data migrations** (rewriting existing data) as their own migration with
  `reversible`/`execute` (example: `move_data_from_fee_rules.rb`).

### Careful: the contract with the Python side
Every schema change can affect the Python scripts (`packages/wsjrdp2027`,
SEPA/mailing/DATEV) that query the same DB. Renaming/removing columns,
changing enums or defaults → check whether Python queries must be adapted,
and tell the user about the impact. See the section "Relationship to the
Python scripts".

## Dev environment & execution

Runs in **Docker Compose** (from the dev setup root). The containers are
already running: `development-rails-1` (app, port **3000**), `-worker-1`
(jobs), `-webpack-1` (3035), `-postgres-1` (**Postgres 16, port 5432**),
`-mailcatcher-1` (SMTP 1025 / UI **1080**).

- **App in the browser:** http://localhost:3000/ (302 → login). **Dev DB →
  writes allowed, but carefully** (real effect on the local state, no prod
  effect).
- **Interactive dev shell:** `./bin/dev-env.sh` opens the `hit` shell; inside
  it `hit up|down|ps`, `hit rails console|bash|routes|logs`, `hit test …`.
- **Without the hit shell (most practical for me), read-only evaluation:**
  ```bash
  docker exec development-rails-1 bash -lc \
    'cd /usr/src/app/hitobito && bundle exec rails runner "puts Person.count"'
  ```
  (Container workdir: `/usr/src/app/hitobito`; the wagon is mounted at
  `/usr/src/app/hitobito_wsjrdp_2027`. `RAILS_ENV=development`.) For
  **writing** runner calls, ask the user first.
- **Rails console:** `hit rails console` (or
  `docker compose run --rm -e SKIP_INIT=1 rails bash -l -c "bundle exec rails c"`).
- **Migrations:** see the dedicated section **"Database & migrations
  (`db/`)"** above — schema change = always a new migration; migrations on
  `main` are baked, not-yet-merged ones are edited in place.
- **Mails in dev** land in Mailcatcher: http://localhost:1080/.

## Tests, lint, conventions (wagon)

- **RSpec:** `bin/rspec` (uses Spring, unless `DISABLE_SPRING`). Specs under
  `spec/` (`abilities/`, `features/` (Capybara), `models/`, `api/`),
  Fabrication + fixtures. Via the hit shell: `hit test …`.
- **Rubocop:** `.rubocop.yml` inherits from the core
  (`../hitobito/.rubocop.yml`) + `.rubocop_todo.yml`. `db/`, `config/`,
  `bin/` are excluded.
- **Language:** the app is **de-only** (`config/settings.yml` → languages:
  de). New UI strings go to `config/locales/wsjrdp_2027.de.yml`.
- **Frozen string literals**, 2-space indent, HAML/ERB views.
- **Copyright header** as in existing files (AGPL-3.0, "German Contingent…").
- **Always test a small, local change first** (dev DB) before anything moves
  towards prod.

## Relationship to the Python scripts (`wsjrdp_scripts`)

The Python scripts in this repo access **the very same Hitobito PostgreSQL
DB directly** (dev: `localhost:5432`, container `development-postgres-1`).
The wagon schema described here (tables `people`, `groups`,
`accounting_entries`, `wsjrdp_direct_debit_pre_notifications`,
`wsjrdp_payment_*`, `wsjrdp_ledger_accounts`, `wsjrdp_personal_accounts`,
`wsjrdp_cost_centers`, `wsjrdp_spheres`, `moss_transactions` /
`moss_expenses` / `moss_bookings`,
`wsj27_rdp_fee_rules`, `roles` …) is therefore the **contract** between the
app and the scripts:

- If I change a field/enum here (e.g. `status`, `sepa_status`,
  `payment_role`, `additional_info` keys, fee/installment logic), that can
  affect the Python side (`packages/wsjrdp2027`, SEPA/mailing/DATEV tools) —
  and vice versa.
- Amounts are in **cents** on both sides; German formatting.
- Before DB schema changes, check whether Python queries are affected (and
  whether a fresh prod-dump restore is needed to test consistently on dev).

## Key rules

1. **Only change `app/hitobito_wsjrdp_2027/`. `app/hitobito/` and other
   wagons: read-only.**
2. Always extend core behavior via a wagon module (`Wsjrdp2027::*`) hooked
   in through `wagon.rb`, never patch the core.
3. Dev DB = local, writing allowed, but ask before destructive/mass actions.
   Prod is off-limits (real people, real money) — prod runs are driven by
   the user through the Python side, not by me through the app.
4. **Schema change = always a new migration** under `db/migrate/` (new
   timestamp, `ActiveRecord::Migration[7.1]`, reversible). **Migrations that
   are already on `main` are never changed** — corrections only through a
   further migration. A not-yet-merged migration, in contrast, is edited in
   place: DB changes for a table **newly created** in it belong into that
   same migration, no second one. `schema.rb`/`schema.rb.diff` are
   auto-generated (via `wagon:schema_dump`), do not edit by hand.
5. The schema/enums are the contract with the Python side — change with
   care.
