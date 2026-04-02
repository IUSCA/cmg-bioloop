#!/bin/bash

##
# Reclaim resources for bigbang (app DB + sandbox DB)
#
# Destructive reset for both targets used by bigbang:
# - main app PostgreSQL data (target-db=app)
# - data_sync sandbox PostgreSQL data (target-db=sandbox)
#
# What this script does (important side effects):
# - Stops app services `api`, `ui`, and `postgres` in the main compose stack.
# - Stops/removes the data_sync `db_sandbox` container.
# - Deletes the sandbox Postgres volume (`bioloop_sandbox_pgdata`).
# - Deletes contents of the main app Postgres host data directory.
# - Aggressively prunes unused Docker cache/images/containers/volumes.
# - Starts `db_sandbox` again with fresh empty Postgres storage.
#
# Usage:
#   ./bin/reclaim_bigbang_container_resources.sh
#
##

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_SYNC_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

CHECK_PATH="/opt/sca"
MIN_FREE_MB=2048
SANDBOX_VOLUME_NAME="bioloop_sandbox_pgdata"
LOG_DIR="/tmp/data_sync_logs"

if ! command -v docker &> /dev/null; then
  echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
  exit 1
fi

source "$SCRIPT_DIR/init.sh"

SANDBOX_COMPOSE_FILE="$(resolve_compose_file)"
SANDBOX_COMPOSE_PATH="${DATA_SYNC_ROOT}/${SANDBOX_COMPOSE_FILE}"

if [[ "${SANDBOX_COMPOSE_FILE}" == "docker-compose.prod.yml" ]]; then
  APP_COMPOSE_PATH="${REPO_ROOT}/docker-compose-prod.yml"
  APP_DB_HOST_DIR="${REPO_ROOT}/db/prod/db_postgres"
else
  APP_COMPOSE_PATH="${REPO_ROOT}/docker-compose.yml"
  APP_DB_HOST_DIR="${REPO_ROOT}/db/postgres/data"
fi

get_free_mb() {
  df -Pm "$CHECK_PATH" | awk 'NR==2 {print $4}'
}

reclaim_docker_space() {
  echo -e "${YELLOW}Reclaiming Docker space (unused images/cache/containers/volumes)...${NC}"
  docker builder prune -af >/dev/null 2>&1 || true
  docker image prune -af >/dev/null 2>&1 || true
  docker container prune -f >/dev/null 2>&1 || true
  docker volume prune -f >/dev/null 2>&1 || true
}

require_app_postgres_env() {
  local rendered
  rendered="$(docker compose -f "$APP_COMPOSE_PATH" config 2>/dev/null || true)"
  local has_db has_user has_pass

  has_db=$(printf "%s\n" "$rendered" | awk '/POSTGRES_DB:/ { if ($2 != "\"\"" && $2 != "") print "yes" }' | head -n 1)
  has_user=$(printf "%s\n" "$rendered" | awk '/POSTGRES_USER:/ { if ($2 != "\"\"" && $2 != "") print "yes" }' | head -n 1)
  has_pass=$(printf "%s\n" "$rendered" | awk '/POSTGRES_PASSWORD:/ { if ($2 != "\"\"" && $2 != "") print "yes" }' | head -n 1)

  if [[ "$has_db" != "yes" || "$has_user" != "yes" || "$has_pass" != "yes" ]]; then
    echo -e "${RED}ERROR: Main app Postgres env vars are not fully set for ${APP_COMPOSE_PATH}.${NC}"
    echo -e "${RED}Set POSTGRES_DB, POSTGRES_USER, and POSTGRES_PASSWORD in repo .env, then rerun.${NC}"
    exit 1
  fi
}

clear_app_db_dir() {
  if [[ ! -d "$APP_DB_HOST_DIR" ]]; then
    echo -e "${YELLOW}Main app DB directory not found — creating.${NC}"
    mkdir -p "$APP_DB_HOST_DIR"
    return 0
  fi

  # Try direct delete first.
  if rm -rf "${APP_DB_HOST_DIR:?}/"* 2>/dev/null; then
    return 0
  fi

  # Fallback for root-owned Postgres files: remove via ephemeral container.
  echo -e "${YELLOW}Direct delete failed (permissions). Retrying via container...${NC}"
  docker run --rm -v "${APP_DB_HOST_DIR}:/target" postgres:14.5 sh -c "rm -rf /target/* /target/.[!.]* /target/..?* || true" >/dev/null
}

echo -e "${YELLOW}Sandbox compose: ${SANDBOX_COMPOSE_PATH}${NC}"
echo -e "${YELLOW}App compose: ${APP_COMPOSE_PATH}${NC}"
echo -e "${YELLOW}Main app DB dir: ${APP_DB_HOST_DIR}${NC}"
require_app_postgres_env

if ls "${LOG_DIR}"/bigbang*.log &>/dev/null; then
  echo -e "${YELLOW}Clearing old bigbang logs from ${LOG_DIR}...${NC}"
  rm -f "${LOG_DIR}"/bigbang*.log
  echo -e "${GREEN}Logs cleared.${NC}"
else
  echo -e "${YELLOW}No bigbang logs to clear.${NC}"
fi

free_before=$(get_free_mb)
echo -e "${YELLOW}Free space before cleanup on ${CHECK_PATH}: ${free_before}MB${NC}"
reclaim_docker_space
free_after_prune=$(get_free_mb)
echo -e "${YELLOW}Free space after initial Docker cleanup: ${free_after_prune}MB${NC}"

echo -e "${YELLOW}Stopping app services (api/ui/postgres) without tearing down networks...${NC}"
# Keep network objects intact to avoid IP reallocation collisions.
docker compose -f "$APP_COMPOSE_PATH" stop api ui postgres >/dev/null 2>&1 || true
echo -e "${YELLOW}Removing postgres container so it is recreated with current .env values...${NC}"
docker compose -f "$APP_COMPOSE_PATH" rm -f postgres >/dev/null 2>&1 || true

echo -e "${YELLOW}Stopping/removing db_sandbox container...${NC}"
docker compose -f "$SANDBOX_COMPOSE_PATH" stop db_sandbox >/dev/null 2>&1 || true
docker compose -f "$SANDBOX_COMPOSE_PATH" rm -f db_sandbox >/dev/null 2>&1 || true

echo -e "${YELLOW}Removing sandbox PostgreSQL volume (${SANDBOX_VOLUME_NAME})...${NC}"
if docker volume inspect "$SANDBOX_VOLUME_NAME" &>/dev/null; then
  docker volume rm "$SANDBOX_VOLUME_NAME" >/dev/null
  echo -e "${GREEN}Sandbox volume removed.${NC}"
else
  echo -e "${YELLOW}Sandbox volume not present — skipping.${NC}"
fi

echo -e "${YELLOW}Clearing main app PostgreSQL data directory...${NC}"
clear_app_db_dir
echo -e "${GREEN}Main app DB directory cleared.${NC}"

reclaim_docker_space
free_after_reset=$(get_free_mb)
echo -e "${YELLOW}Free space after reset cleanup: ${free_after_reset}MB${NC}"

if [ "$free_after_reset" -lt "$MIN_FREE_MB" ]; then
  echo -e "${RED}ERROR: Need at least ${MIN_FREE_MB}MB free on ${CHECK_PATH}, have ${free_after_reset}MB.${NC}"
  exit 1
fi

echo -e "${YELLOW}Starting fresh db_sandbox container...${NC}"
# Brings back data_sync runtime shell with a fresh sandbox Postgres volume.
docker compose -f "$SANDBOX_COMPOSE_PATH" up -d db_sandbox

echo ""
echo -e "${GREEN}Done. Reclaimed resources for app + sandbox bigbang targets.${NC}"
echo -e "${YELLOW}Next:${NC}"
echo -e "${YELLOW}  1. Run ./bin/deploy.sh${NC}"
echo -e "${YELLOW}  2. Run ./data_sync/bin/bigbang.sh ...${NC}"
