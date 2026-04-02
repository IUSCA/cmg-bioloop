#!/bin/bash

##
# CMG to Bioloop Big-Bang Database Migration (CMG source)
#
# Performs one-time historical data migration from CMG (MongoDB) to Bioloop (PostgreSQL).
# This script migrates all existing data and initializes cursor positions for future
# incremental sync via pollers.
#
# Wrapper behavior:
#   Host-side launcher only. Executes Node.js script inside db_sandbox container.
#   Compose file is selected from APP_ENV/NODE_ENV (production -> prod compose;
#   otherwise localhost compose).
#
# Usage:
#   ./bin/bigbang_cmg.sh [options]
#
# Options:
#   --target-db DB         Target database: 'sandbox' (default), 'app', or 'custom'
#   --clear-target-db      Clear all CMG + Xenium migration data from target DB before migration
#   --clear-locks          Force release all existing process locks
#   --skip-sessions        Skip genome browser session conversion
#   -h, --help             Show this help message
#
# Environment Variables (CMG source):
#   CMG_MONGO_HOST
#   CMG_MONGO_PORT
#   CMG_MONGO_DB
#   CMG_MONGO_USERNAME
#   CMG_MONGO_PASSWORD
#
# Examples:
#
#   # Migrate to sandbox database (default)
#   ./bin/bigbang_cmg.sh
#
#   # Migrate to main app database
#   ./bin/bigbang_cmg.sh --target-db app
#
#   # Clear all legacy migration data before migration
#   ./bin/bigbang_cmg.sh --target-db sandbox --clear-target-db
#
#   # Skip genome browser sessions (faster migration for testing)
#   ./bin/bigbang_cmg.sh --skip-sessions
#
#   # Clear stuck process locks from previous failed run
#   ./bin/bigbang_cmg.sh --clear-locks
#
# Migration Steps (12 total):
#   1.  Create roles (admin, operator, user)
#   2.  Create CMG system user
#   3.  Populate pipeline definitions (conversion workflows)
#   4.  Convert users
#   5.  Convert datasets (raw data)
#   6.  Convert dataset audit logs
#   7.  Convert CMG upload history to import logs
#   8.  Convert dataset hierarchies (raw → derived relationships)
#   9.  Convert projects
#   10. Convert conversions (pipeline runs)
#   11. Convert genome browser sessions
#   12. Initialize poller cursors
#
# Process Locking:
#   This script uses database-level locking to prevent multiple instances
#   from running simultaneously. If a previous run crashed, use --clear-locks.
#
# Idempotency:
#   This migration is idempotent - it can be safely re-run multiple times.
#   Existing records are skipped based on cmg_id or unique constraints.
#   Data inserted before any error is retained in the database.
#
# Logs:
#   Detailed logs are written to: data_sync/logs/bigbang_sync_*.log
#   On production host, logs are in: /tmp/data_sync_logs/
#
# Exit Codes:
#   0 - Success
#   1 - Migration failed (check logs)
#   2 - Another bigbang process is already running
#
##

set -e

# Get script directory and change to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"
cd "$SCRIPT_DIR/.."

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Help function
show_help() {
  grep '^#' "$SCRIPT_PATH" | sed 's/^# \?//' | sed 's/^##$//'
  exit 0
}

# Parse arguments
for arg in "$@"; do
  case $arg in
    -h|--help)
      show_help
      ;;
  esac
done

should_migrate_app_db() {
  local prev=""
  for arg in "$@"; do
    case "$arg" in
      --target-db=app) return 0 ;;
      --target-db=*) return 1 ;;
      app)
        if [ "$prev" = "--target-db" ]; then
          return 0
        fi
        ;;
    esac
    prev="$arg"
  done
  return 1
}

# Check if Docker is available
if ! command -v docker &> /dev/null; then
  echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
  exit 1
fi

source "$SCRIPT_DIR/init.sh"

COMPOSE_FILE="$(resolve_compose_file)"

if ! docker compose -f "$COMPOSE_FILE" ps db_sandbox | grep -q "Up"; then
  echo -e "${RED}Error: db_sandbox container is not running for ${COMPOSE_FILE}.${NC}"
  echo -e "${YELLOW}  Start it with: docker compose -f ${COMPOSE_FILE} up -d${NC}"
  exit 1
fi

if should_migrate_app_db "$@"; then
  echo -e "${YELLOW}Applying Prisma migrations to app database (preflight)...${NC}"
  docker compose -f "$COMPOSE_FILE" exec db_sandbox sh -lc 'DATABASE_URL="$(node -e "process.stdout.write(require(\"/opt/sca/app/src/utils/db_config\").getDatabaseUrl(\"app\"))")" npx prisma migrate deploy --schema /opt/sca/api/prisma/schema.prisma'
fi

# Run the Node.js bigbang script
echo -e "${YELLOW}Starting big-bang migration...${NC}"
echo ""

docker compose -f "$COMPOSE_FILE" exec db_sandbox node /opt/sca/app/src/bigbang_cmg_sync.js "$@"
exit_code=$?

echo ""
if [ $exit_code -eq 0 ]; then
  echo -e "${GREEN}✓ Big-bang migration completed successfully${NC}"
elif [ $exit_code -eq 2 ]; then
  echo -e "${RED}✗ Another bigbang process is already running${NC}"
  echo -e "${YELLOW}  Use --clear-locks to force release locks${NC}"
else
  echo -e "${RED}✗ Big-bang migration failed${NC}"
  echo -e "${YELLOW}  Check logs in data_sync/logs/ for details${NC}"
fi

exit $exit_code

