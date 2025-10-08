#!/bin/bash
set -e # Exit on error
set -o pipefail # Exit if any command in a pipeline fails

{ 
  echo "Finding the UID and GID of the provided user '"$APP_USER"' and group '"$APP_GROUP"'."
  APP_UID=$(id -u $APP_USER) && 
  APP_GID=$(grep "^$APP_GROUP:" /etc/group | cut -d: -f3) 

} || {

  echo "Defaulting to the user and group of the current directory's owner because the provided user or group was not found."
  APP_UID=$(ls -ldn `pwd` | awk '{print $3}') &&
  APP_GID=$(ls -ldn `pwd` | awk '{print $4}')
}

echo "APP_UID:$APP_UID,APP_GID:$APP_GID"

# Check if .env file exists
if [ ! -f .env ]; then
  echo "Creating .env file..."
  # If it doesn't exist, create it
  touch .env
fi

# Check if APP_UID exists in the .env file
if grep -q "APP_UID" .env; then
  echo "APP_UID already exists in .env file."
  echo "OSTYPE: $OSTYPE"
  # If it exists, update it
  if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "darwin"
    sed -i "" "s/^APP_UID=.*/APP_UID=$APP_UID/" .env
  else
    echo "not darwin"
    sed -i "s/^APP_UID=.*/APP_UID=$APP_UID/" .env
  fi
else
  # If it doesn't exist, add it
  echo "APP_UID does not exist in .env file."
  echo "" >> .env
  echo "APP_UID=$APP_UID" >> .env
  echo "wrote APP_UID=$APP_UID to .env file."
fi

# Check if APP_GID exists in the .env file
if grep -q "APP_GID" .env; then
  echo "APP_GID already exists in .env file."
  echo "OSTYPE: $OSTYPE"
  # If it exists, update it
  if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "darwin"
    sed -i "" "s/^APP_GID=.*/APP_GID=$APP_GID/" .env
  else
    echo "not darwin"
    sed -i "s/^APP_GID=.*/APP_GID=$APP_GID/" .env
  fi
else
  echo "APP_GID does not exist in .env file."
  # If it doesn't exist, add it
  echo "APP_GID=$APP_GID" >> .env
  echo "wrote APP_GID=$APP_GID to .env file."
fi

# Check if GRAFANA_ADMIN_PASSWORD exists in the .env file
if grep -q "GRAFANA_ADMIN_PASSWORD" .env; then
  echo "GRAFANA_ADMIN_PASSWORD already exists in .env file."
else
  echo "GRAFANA_ADMIN_PASSWORD does not exist in .env file."
  # If it doesn't exist, add it with a random password
  GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 16)
  echo "GRAFANA_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD" >> .env
  echo "wrote GRAFANA_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD to .env file."
fi

# build API
echo "Building API..."
cd api
sudo docker build --network=host --build-arg APP_UID=${APP_UID} --build-arg APP_GID=${APP_GID} -t bioloop-api .
cd ..
echo "API built."

# starts postgres if it is not running
echo "Starting Postgres..."
sudo docker compose -f "docker-compose-prod.yml" up -d postgres
echo "Postgres started."

# recreates and starts api and ui
echo "Recreating and starting UI and API..."
sudo docker compose -f "docker-compose-prod.yml" up -d --force-recreate ui api
echo "UI and API recreated and started."

# start grafana if it is not running
# this will also start prometheus and postgres-exporter if they are not running
echo "Starting Grafana..."
sudo docker compose -f "docker-compose-prod.yml" up -d grafana
echo "Grafana started."
