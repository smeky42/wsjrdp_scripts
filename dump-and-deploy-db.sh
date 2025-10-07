#!/bin/bash

# Check if yq is installed
if ! command -v yq &> /dev/null; then
    echo "yq could not be found. Please install yq to parse YAML files."
    exit 1
fi

# Read credentials from config-prod.yml
SSH_HOST=$(yq eval '.ssh_host' config-prod.yml)
SSH_PORT=$(yq eval '.ssh_port' config-prod.yml)
SSH_USERNAME=$(yq eval '.ssh_username' config-prod.yml)
SSH_PRIVATE_KEY=$(yq eval '.ssh_private_key' config-prod.yml)

DB_HOST=$(yq eval '.db_host' config-prod.yml)
DB_PORT=$(yq eval '.db_port' config-prod.yml)
DB_PORT_TUNNEL="5433"
DB_USERNAME=$(yq eval '.db_username' config-prod.yml)
DB_PASSWORD=$(yq eval '.db_password' config-prod.yml)
DB_NAME=$(yq eval '.db_name' config-prod.yml)

TIMESTAMP=$(date +"%Y-%m-%d--%H-%M-%S")
DUMP_FILE="$TIMESTAMP--hitobito_production_dump.sql"  # Output dump file name

# Create an SSH tunnel and run pg_dump
ssh -i "$SSH_PRIVATE_KEY" -L $DB_PORT_TUNNEL:$DB_HOST:$DB_PORT -p $SSH_PORT $SSH_USERNAME@$SSH_HOST -N &

# Wait for the SSH tunnel to establish
sleep 5

# Run pg_dump through the SSH tunnel
PGPASSWORD=$DB_PASSWORD pg_dump -U $DB_USERNAME -h localhost -p $DB_PORT_TUNNEL -d $DB_NAME -F c -f $DUMP_FILE

# Kill the SSH tunnel process
kill $!

echo "Database dump completed: $DUMP_FILE"


# Rename all appearances of hitobito_production to hitobito_development in the dump file
echo "Renaming database references in the dump file..."
sed -i '' 's/hitobito_production/hitobito_development/g' "$DUMP_FILE"

LOCAL_DB_PASSWORD="hitobito"
LOCAL_DB_USERNAME="hitobito"
LOCAL_DB_NAME="hitobito_development"

# Drop the local database before reimporting
echo "Dropping the local database..."
PGPASSWORD=$LOCAL_DB_PASSWORD psql -U $LOCAL_DB_USERNAME -h localhost -p $DB_PORT -c "DROP DATABASE IF EXISTS $LOCAL_DB_NAME;"

# Recreate the local database
echo "Recreating the local database..."
PGPASSWORD=$LOCAL_DB_PASSWORD psql -U $LOCAL_DB_USERNAME -h localhost -p $DB_PORT -c "CREATE DATABASE $LOCAL_DB_NAME;"

# Reimport the dump into the local database
echo "Reimporting the dump into the local database..."
PGPASSWORD=$LOCAL_DB_PASSWORD pg_restore -U $LOCAL_DB_USERNAME -h localhost -p $DB_PORT -d $LOCAL_DB_NAME -F c --clean --no-owner "$DUMP_FILE"

echo "Reimport completed."
