# Integration Tests

## Prepare

In the top-level `wsjrdp_scripts` execute the following:

```
docker compose -f ./integration-tests/docker-compose.yml up --wait
WSJRDP_SCRIPTS_CONFIG=config-prod.yml ./tools/db_dump.py ./integration-tests/hitobito_production.dump
WSJRDP_SCRIPTS_CONFIG=integration-tests/config-integration-tests.yml ./tools/db_restore.py ./integration-tests/hitobito_production.dump
```

## Run Tests

```
uv run pytest integration-tests
```
