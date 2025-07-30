#!/bin/bash

# Check if yq is installed
if ! command -v yq &> /dev/null; then
    echo "yq could not be found. Please install yq to parse YAML files."
    exit 1
fi

# Read credentials from config.yml
SSH_HOST=$(yq eval '.ssh_host' config.yml)
SSH_PORT=$(yq eval '.ssh_port' config.yml)
SSH_USERNAME=$(yq eval '.ssh_username' config.yml)
SSH_PRIVATE_KEY=$(yq eval '.ssh_private_key' config.yml)

DB_HOST=$(yq eval '.db_host' config.yml)
DB_PORT=$(yq eval '.db_port' config.yml)
DB_PORT_TUNNEL="5433"
DB_USERNAME=$(yq eval '.db_username' config.yml)
DB_PASSWORD=$(yq eval '.db_password' config.yml)
DB_NAME=$(yq eval '.db_name' config.yml)

TIMESTAMP=$(date +"%Y-%m-%d--%H-%M-%S")
DUMP_FILE="$TIMESTAMP--hitobito_production_dump.sql"  # Output dump file name

# Create an SSH tunnel and run pg_dump
ssh -i "$SSH_PRIVATE_KEY" -L $DB_PORT_TUNNEL:$DB_HOST:$DB_PORT -p $SSH_PORT $SSH_USERNAME@$SSH_HOST -N &

# Wait for the SSH tunnel to establish
sleep 5

# Run pg_dump through the SSH tunnel
# PGPASSWORD=$DB_PASSWORD pg_dump -U $DB_USERNAME -h localhost -p $DB_PORT_TUNNEL -d $DB_NAME -F c -f $DUMP_FILE

# Run PostgreSQL query through the SSH tunnel
# PGPASSWORD=$DB_PASSWORD psql -h localhost -p $DB_PORT_TUNNEL -U hitobito_production -d hitobito_production -c "SELECT id, first_name, generated_registration_pdf FROM people WHERE generated_registration_pdf LIKE '/app-src/private/uploads/person/pdf/%';"

PGPASSWORD=$DB_PASSWORD psql -h localhost -p $DB_PORT_TUNNEL -U hitobito_production -d hitobito_production -c "SELECT id, first_name, generated_registration_pdf FROM people WHERE generated_registration_pdf LIKE '/app-src/private/uploads/person/pdf/%';" -o "$TIMESTAMP--generated-old-path.csv"

PGPASSWORD=$DB_PASSWORD psql -h localhost -p $DB_PORT_TUNNEL -U hitobito_production -d hitobito_production -c "UPDATE people SET generated_registration_pdf = REPLACE(generated_registration_pdf, '/app-src/private/', '/app-src/vendor/wagons/hitobito_wsjrdp_2027/private/') WHERE generated_registration_pdf LIKE '/app-src/private/uploads/person/pdf/%';"

PGPASSWORD=$DB_PASSWORD psql -h localhost -p $DB_PORT_TUNNEL -U hitobito_production -d hitobito_production -c "SELECT id, first_name, generated_registration_pdf FROM people WHERE generated_registration_pdf LIKE '/app-src/vendor/wagons/hitobito_wsjrdp_2027/private/%';" -o "$TIMESTAMP--generated-new-path.csv"

# Kill the SSH tunnel process
kill $!
