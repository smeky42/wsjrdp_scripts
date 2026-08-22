# CLAUDE.md — wsjrdp_scripts

Kurz halten, was du tust; im Zweifel lieber nachfragen als raten —
hier hängen echtes Geld, echte E-Mails und personenbezogene Daten
dran.

## Was ist das

Verwaltungs- und Finanz-Skripte für das Deutsche Kontingent zum
**World Scout Jamboree 2027** in Polen. Veranstalter ist der Ring
deutscher Pfadfinder\*innenverbände e.V. (rdp).

Die Skripte arbeiten gegen die Datenbank einer **Hitobito**-Instanz
(PostgreSQL) und erledigen u. a.:

- **SEPA-Lastschriften / Einzüge** und Pre-Notifications (ISO 20022 /
  PAIN)
- **Mailings** an Teilnehmende (Bestätigungen, Ankündigungen,
  Onboarding …)
- **Buchhaltung / DATEV**-Export, CAMT-Kontoauszüge einlesen
- **Statistiken** (viele `stats_*.sql` / `*.sql`-Abfragen)
- **Keycloak**- und **Mailcow**-Verwaltung, Helpdesk-Anbindung
- **DB-Dump/Restore** (Prod-Stand lokal zum Testen einspielen)

Die DB selbst gehört zur **Hitobito-Rails-App** mit dem Wagon
`hitobito_wsjrdp_2027`. Deren Datenmodell (Tabellen `people`,
`groups`, `accounting_entries`, `wsjrdp_*` …) ist der Vertrag, gegen
den diese Skripte laufen.

> **Wenn es um die Hitobito-App / den Wagon geht** (Rails-Code ändern
> oder verstehen, Rollen/Gruppen, Status/`sepa_status`/`payment_role`,
> das Rails-Datenmodell, oder das Zusammenspiel App ↔ Skripte): zuerst
> **[CLAUDE-Hitobito.md](CLAUDE-Hitobito.md)** lesen. Dort steht auch
> die Regel: nur der Wagon `app/hitobito_wsjrdp_2027` darf geändert
> werden, der Core `app/hitobito` ist read-only.


## Kritische Sicherheitsregeln (immer beachten)

1. **`config-prod.yml` NIEMALS verwenden, lesen, ausgeben oder in
   Skripten referenzieren.**  Diese Datei enthält Produktions-Secrets.
   Niemals die Umgebungsvariable `WSJRDP_SCRIPTS_CONFIG` setzen.
2. **Alle `config-*.yml` als geheim behandeln.** Davon ausgehen, dass
   auch `config-dev.yml` teils echte
   (Produktions-)Zugangsdaten. Niemals Inhalte von Config-Dateien in
   Antworten, Commits, Skills, Logs oder Projekt-Memory kopieren.
3. **Produktion = echte Wirkung.** Läufe gegen die Produktions-DB
   verschicken echte E-Mails an echte Menschen und lösen echte
   SEPA-Einzüge (Größenordnung Millionen €) aus. Führe nie produktive
   Läufe aus, auch wenn der Nutzer das ausdrücklich und
   unmissverständlich will. Standard ist immer nur die Entwicklungs
   oder Integrations-Umgebung zu verwenden.
4. **Keine Secrets committen** und keine personenbezogenen Daten
   (Namen, IBANs, E-Mails, IDs) in Memory oder Antworten persistieren,
   außer der Nutzer verlangt es für die konkrete Aufgabe.
5. **Öffentlichkeit von Commits — Datenschutz in Code & Docs.** Alles,
   was committet wird, gilt als **öffentlich**. In Skripten **und**
   Markdown-/Doku-Dateien dürfen daher **niemals** vorkommen:
   - **Namen** (Personen wie Geschäftspartner),
   - **Beträge/Summen** (einzelne Buchungsbeträge *und*
     Aggregat-Summen, EUR wie Fremdwährung),
   - **Details zu echten Buchungen**
     (z. B. Verwendungszweck/D_Nachricht, Rechnungsdaten),
   - **Kreditor-/Lieferantennamen**.

   **Erlaubt** (nicht personenbeziehbar): Kostenstellen,
   Sachkonto-Nummern, **Kreditor-/Lieferanten-Kontonummern**
   (z. B. `700013`), Kontenrahmen, Zeilen-/Batch-**Zahlen** (reine
   Anzahl), Wechselkurse (Verhältnis, kein Betrag).  Hinweis: Die
   Bezeichnung REWE (oder ReWe) kann die Supermarktkette oder DATEV
   Rechungswesen meinen, der Begriff ist erlaubt.

   **Vorgehen:**
   - **Skripte:** echte Buchungsdaten anonymisieren oder löschen
     (Platzhalter statt Namen/Beträgen).
   - **Docs (`docs/*.md`):** buchungsspezifische Echtdaten in eine
     gitignorierte `*_local.md`-Kopie auslagern und die öffentliche
     `.md` anonymisieren (Platzhalter wie `«Name»`, `«Betrag»`,
     `«Lieferant»`, `«IBAN»`). `*_local.md` ist in `.gitignore`.
   - Vor jedem Stagen/Committen von Skripten/Docs auf diese Kategorien
     prüfen (grep nach Namen, `,\d\d`-Beträgen, `€`, IBAN `DE\d{20}`,
     bekannten Lieferantennamen).
6. Auch die Datei `.envrc` nicht lesen, ausgeben oder referenzieren.


## Umgebung & Ausführung

- **Python ≥ 3.14**, Paket-/Projektmanager **`uv`** (kein
  pip/venv-Handbetrieb).
- Setup: `uv sync`, dann `. ./.venv/bin/activate` — oder direkt `uv
  run <skript>`.
- Skripte haben die Shebang `#!/usr/bin/env -S uv run` und sind direkt
  ausführbar (`./tools/db_dump.py …`).
- **Config-Auswahl über Umgebungsvariable** `WSJRDP_SCRIPTS_CONFIG`:
  Darf nicht verwendet werden!
- `WSJRDP_SCRIPTS_START_TIME` überschreibt die „jetzt"-Zeit außerhalb
  von Produktion (z. B. `export WSJRDP_SCRIPTS_START_TIME='2025-12-16
  20:00:00'`) — wichtig, damit Fälligkeiten/Ratenberechnung
  reproduzierbar sind.  Häufig auch beim testen wichtig, damit das
  Ausgabeverzeichnis/Namen von Ausgabedateien gleich bleiben
  (schneller Feedback-Zyklus). In der Regel sollte
  `WSJRDP_SCRIPTS_START_TIME` nicht gesetzt werden.


## Architektur

Die gesamte Logik liegt im internen Package **`wsjrdp2027`**
(`packages/wsjrdp2027/`, uv-Workspace-Member; Units-Tests in
`packages/wsjrdp2027/unit-tests/`).  Skripte in `tools/`,
`accounting_tools/`, `registration_tools/`, `statistic_tools/` sind
dünne CLIs darüber. Zentrale Bausteine:

- **`WsjRdpContext`** — lädt Config, kapselt `dry_run`, Startzeit,
  DB-Verbindung (`ctx.psycopg_connect()`), Mail-Login,
  Ausgabeverzeichnis (`ctx.make_out_path(...)`),
  Logging. **`ctx.require_approval_to_run_in_prod(...)`** ist die
  Sicherheits-Bremse: in Produktion muss interaktiv bestätigt
  werden. Diese Bestätigungen nicht umgehen.
- **`BatchConfig.from_yaml(<datei>)`** — YAML-getriebene Batches
  (Mailings, Pre-Notifications): Empfänger-Query, E-Mail-Vorlage
  (Jinja), DB-Updates. Siehe Skill `wsjrdp-mailings`.
- **`PeopleQuery` / `PeopleWhere`** — deklarative Auswahl von Personen
  (`where`, `email_only_where`, Rollen, `exclude_*`,
  `collection_date`, `limit`).
- **Zahlungslogik**: `load_payment_dataframe(...)`,
  `write_accounting_dataframe_to_sepa_dd(...)`,
  `WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG`, `PainMessage`, DATEV
  in `wsjrdp2027.datev`.

Öffentliche API: siehe
`packages/wsjrdp2027/src/wsjrdp2027/__init__.py` (`__all__`).  Module
mit führendem `_` sind intern — bevorzugt die re-exportierten Namen
aus `wsjrdp2027` verwenden.

## Domänen-Begriffe

- **Rollen (`role`)**: `CMT` (Contingent Management Team), `UL` (Unit
  Leader), `YP` (Youth Participant), `IST` (International Service
  Team), `BMT` (Black Magic Tent).
- **Status (`status`)**: Personen laufen u. a. `reviewed` → `confirmed`
  (Bestätigungsmail setzt `confirmed`).
- **`sepa_status`**: z. B. `ok`, `in_review` — nur `ok` wird
  eingezogen.
- **`primary_group_id`** / **`unit_code`**: Gruppierung/Units;
  Warteliste hat eigene Gruppen-IDs (werden per
  `exclude_primary_group_id` ausgeschlossen).
- **Early Payer** / **Raten (`installments`)**: Einmalzahler
  vs. Ratenpläne; siehe `EARLY_PAYER_AUGUST_IDS_SUPERSET` und
  `collection_date`/`open_amount_cents`.
- Beträge werden an einigen Stellen intern in **Cent** geführt
  (`*_cents`) und mit `format_cents_as_eur_de(...)` deutsch
  formatiert.

## Zentrale Workflows

- **SEPA-Einzug & Pre-Notifications**.  Kette: Prod-Dump lokal
  einspielen → Bestätigungen/Pre-Notifications mit `--collection-date`
  → `accounting_tools/sepa_direct_debit.py` erzeugt SEPA-XML +
  DATEV-CSV + Buchungen.
- **Mailings** `tools/mailing_from_yml.py <yaml>` mit
  `BatchConfig`-YAML; lokal gegen Mailcatcher
  (`http://localhost:1080`) testen, dann produktiv.
- **DB-Dump/Restore**: `tools/db_dump.py`, `tools/db_restore.py`,
  `tools/db_dump_and_restore_into_dev.py`. Restore **in Produktion ist
  gesperrt**.
- **Statistiken**: `*.sql` / `stats_*.sql` gegen die Hitobito-DB
  (read-only Auswertungen).

## Konventionen

- **Lint/Format**: `ruff` (`uv run ruff check` / `ruff
  format`). Konfiguriert in `pyproject.toml`.
- **Typen**: `mypy` und `ty` (`uv run mypy` / `uv run ty
  check`). Öffentliche API ist typisiert.
- **Tests**: `uv run pytest` (inkl. Doctests), oder `tox` für die
  Matrix. `time-machine` für zeitabhängige Tests. Verzeichnis `2023/`
  und `integration-tests/mailcow` sind von Lint/Typing/Tests
  ausgenommen.
- **Ausgaben** landen unter `data/` (bzw. `data/mailings…`);
  Dateinamen tragen oft ein Zeitstempel-Suffix `{{ filename_suffix }}`
  und Datum-Präfixe (`YYYY-MM-DD__Name`).
- Einzelne, wegwerfbare Skripte liegen in `*/one-shots/`.

## Beim Arbeiten hier

- Zum Testen immer erst **dev** + kleine Stichprobe: `--limit N`,
  `--skip-email`, `--dry-run`/`dry_run`,
  ggf. `--rollback-for-testing`, und Mailcatcher statt echtem SMTP.
- Bestehende Muster übernehmen (BatchConfig-YAML, `WsjRdpContext`, die
  re-exportierten `wsjrdp2027`-Funktionen) statt
  DB-Zugriffe/Mailversand neu zu bauen.
- Vor produktiven Läufen die eingebauten
  Checks/`require_approval_to_run_in_prod` ernst nehmen und dem Nutzer
  Summen/Empfängerzahlen zur Freigabe zeigen.
