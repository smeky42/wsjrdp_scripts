# wsjrdp Skripte

Siehe auch [CONTRIBUTING.md](./CONTRIBUTING.md) mit Hinweisen zum Ausführen.

## Tools

### Datenbank Dump

Einen dump der konfigurierten Datenbank in eine Datei schreiben. Wenn
`<dump_path>` nicht angegeben ist, dann wird in
`data/<db-name>.<timestamp>.dump` geschrieben.

```sh
uv run tools/db_dump.py [--format=[p,c,t,d]] [dump_path]
WSJRDP_SCRIPTS_CONFIG=config-prod.yml uv run tools/db_dump.py [--format=[p,c,t,d]] [dump_path]
```

### Datenbank Restore

Einen Datenbank-dump wieder einstellen. Aktuell ist es nicht möglich,
einen Datenbank-dump in Produktion einzuspielen!

```sh
uv run tools/db_restore.py [dump_path]
```

### Produktions-Datenbank in Development-Umgebung einspielen

Hinweis: Für den restore wird `config-dev.yml` gelesen.

```sh
WSJRDP_SCRIPTS_CONFIG=config-prod.yml uv run tools/db_dump_and_restore_into_dev.py
```


## Beispiel-Skripte

Beispiel Skripte finden sich im Unterverzeichnis
[`examples`](./examples/). Um z.B. eine Test-E-Mail zu schicken, kann
man

```sh
uv run examples/example_send_plaintext_email.py
```

aufrufen.


## Zugriff auf Hitobito Test (hit) Umgebung

Siehe auch https://github.com/hitobito/development/blob/master/docker-compose.yml


### PostgreSQL

* Host: `localhost`
* Port: `5432`
* User: `hitobito`
* Password: `hitobito`
* Database: `hitobito_development`


### Mailcatcher

* Host: `localhost`
* WebUI: [`http://localhost:1080`](http://localhost:1080)
* SMTP-Server: `1025`
* SMTP-Port: `1025`


### Hitobito

* URL: [`http://localhost:3000`](http://localhost:3000)
