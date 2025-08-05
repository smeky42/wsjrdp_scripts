# Arbeit mit den Python-Skripten

## Python-Umgebung für wsjrdp Skripte

Eine Python virtual environment zum Ausführen der Python Skripte in
diesem Repo kann mit dem Python Paket/Projekt-Manager
[uv](https://docs.astral.sh/uv/) angelegt werden:

```
uv sync
. ./.venv/bin/activate
```

oder auch direkt ausgeführt werden (uv erstellt dann im Hintergrund die virtual environment):

```
uv run python <skript>
```

[uv installieren](https://docs.astral.sh/uv/getting-started/installation/#installing-uv)
