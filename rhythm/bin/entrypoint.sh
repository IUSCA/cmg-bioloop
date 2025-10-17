#!/bin/bash

echo "Running entrypoint script for rhythm container"

set -e

api_env="api/.env"

# Check if .env file exists in api directory
if [ -f "$api_env" ]; then
  echo ".env file exists in api directory."
else
  echo "Creating .env file in api directory..."
  touch $api_env
fi

# remove all content from .env file

# Generate keys in the API directory so API can find them
if [ -f "api/keys/auth.key" ] && [ -f "api/keys/auth.pub" ]; then 
  echo "Keys already exist in API directory. Skipping key generation."
else
  echo "Generating keys in API keys directory"
  mkdir -p api/keys/
  cd api/keys/
  openssl genrsa -out auth.key 2048
  chmod 600 auth.key
  openssl rsa -in auth.key -pubout > auth.pub
  cd ../../
  echo "Generated keys in API keys directory"
fi

# Also copy keys to rhythm's keys directory for rhythm_api to use
mkdir -p keys/
cp api/keys/auth.key keys/auth.key
cp api/keys/auth.pub keys/auth.pub
echo "Copied API keys to rhythm keys directory for rhythm_api"

# Check if the string exists in the file
api_token="WORKFLOW_AUTH_TOKEN"
echo "Checking if the string '${api_token}' exists in the file '$api_env'..."
if grep -q "^${api_token}=" "$api_env"; then
  value=$(grep "^${api_token}=" "$api_env" | cut -d'=' -f2)
  if [ -n "$value" ]; then
    echo "The file contains the string '${api_token}' with a value: $value"
  else
    echo "The string '${api_token}' exists but has no value."
    sed -i '/^WORKFLOW_AUTH_TOKEN/d' $api_env
    echo "WORKFLOW_AUTH_TOKEN=$(python -m rhythm_api.scripts.issue_token --sub bioloop-dev.sca.iu.edu)" >> $api_env
    echo "Created new API token."
    echo "INFO: You MUST RESTART THE API in order to use the new token."
  fi
else
  echo "The string '${api_token}' does not exist in the file."
  echo "WORKFLOW_AUTH_TOKEN=$(python -m rhythm_api.scripts.issue_token --sub bioloop-dev.sca.iu.edu)" >> $api_env
  echo "Created new API token."
  echo "INFO: You MUST RESTART THE API in order to use the new token."
fi

echo "Completed entrypoint script for rhythm container"

$*
