# wsjrdp Skripte

Siehe auch [CONTRIBUTING.md](./CONTRIBUTING.md) mit Hinweisen zum Ausführen.


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
