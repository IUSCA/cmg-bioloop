#!/bin/bash

##
# Reset db_sandbox PostgreSQL data volume
#
# Clears old bigbang log files, stops the db_sandbox container, removes its
# PostgreSQL data volume, and brings the container back up with a fresh
# empty database.
#
# Use this when the sandbox database needs to be wiped entirely,
# e.g. after "no space left on device" errors or before a clean bigbang rerun.
#
# IMPORTANT: After running this script, run bin/deploy.sh (from the repo root)
# to ensure the main app stack (api, postgres, etc.) is healthy before running
# bigbang. The reset can disrupt container networking on the shared Docker
# network, so a deploy re-establishes the correct state.
#
# Wrapper behavior:
#   Host-side script only. Uses docker compose to manage the db_sandbox service.
#   Compose file is selected from APP_ENV/NODE_ENV (production -> prod compose;
#   otherwise localhost compose).
#
# Usage:
#   ./bin/reset_sandbox_db.sh
#
# Exit Codes:
#   0 - Success
#   1 - Failure
#
##

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

if ! command -v docker &> /dev/null; then
  echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
  exit 1
fi

source "$SCRIPT_DIR/init.sh"

COMPOSE_FILE="$(resolve_compose_file)"
VOLUME_NAME="bioloop_sandbox_pgdata"
LOG_DIR="/tmp/data_sync_logs"

# Clear old bigbang logs
if ls "${LOG_DIR}"/bigbang*.log &>/dev/null; then
  echo -e "${YELLOW}Clearing old bigbang logs from ${LOG_DIR}...${NC}"
  rm -f "${LOG_DIR}"/bigbang*.log
  echo -e "${GREEN}Logs cleared.${NC}"
else
  echo -e "${YELLOW}No bigbang logs to clear.${NC}"
fi

# Reclaim space from dangling Docker images and build cache
echo -e "${YELLOW}Pruning dangling Docker images and build cache...${NC}"
docker image prune -f 2>/dev/null || true
docker builder prune -f 2>/dev/null || true

echo -e "${YELLOW}Stopping db_sandbox...${NC}"
docker compose -f "$COMPOSE_FILE" stop db_sandbox

echo -e "${YELLOW}Removing db_sandbox container...${NC}"
docker compose -f "$COMPOSE_FILE" rm -f db_sandbox

echo -e "${YELLOW}Removing PostgreSQL data volume (${VOLUME_NAME})...${NC}"
if docker volume inspect "$VOLUME_NAME" &>/dev/null; then
  docker volume rm "$VOLUME_NAME"
  echo -e "${GREEN}Volume removed.${NC}"
else
  echo -e "${YELLOW}Volume ${VOLUME_NAME} does not exist — nothing to remove.${NC}"
fi

echo -e "${YELLOW}Starting db_sandbox with fresh database...${NC}"
docker compose -f "$COMPOSE_FILE" up -d db_sandbox

echo ""
echo -e "${GREEN}Done. db_sandbox is running with an empty PostgreSQL database.${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo -e "${YELLOW}  1. Run bin/deploy.sh (from repo root) to ensure the main app stack is healthy${NC}"
echo -e "${YELLOW}  2. Run bigbang to re-populate data${NC}"
