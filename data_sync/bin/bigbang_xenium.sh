#!/bin/bash

##
# Xenium to Bioloop Big-Bang Database Migration
#
# Performs one-time historical data migration from the Xenium PostgreSQL
# database into cmg-bioloop's PostgreSQL database.
#
# Unlike the CMG bigbang (MongoDB → PostgreSQL), this is a
# PostgreSQL-to-PostgreSQL migration.
#
# Wrapper behavior:
#   Host-side launcher only. Executes Node.js script inside db_sandbox container.
#   Compose file is selected from APP_ENV/NODE_ENV (production -> prod compose;
#   otherwise localhost compose).
#
# Usage:
#   ./bin/bigbang_xenium.sh [options]
#
# Options:
#   --target-db DB         Target database: 'sandbox' (default), 'app', or 'custom'
#   --clear-target-db      Clear all CMG + Xenium migration data from target DB before migration
#   --clear-locks          Force release all existing xenium process locks
#   -h, --help             Show this help message
#
# Environment Variables:
#   XENIUM_PG_HOST         Xenium source PostgreSQL host
#   XENIUM_PG_PORT         Xenium source PostgreSQL port
#   XENIUM_PG_DATABASE     Xenium source PostgreSQL database name
#   XENIUM_PG_USERNAME     Xenium source PostgreSQL username
#   XENIUM_PG_PASSWORD     Xenium source PostgreSQL password
#
# Examples:
#
#   # Migrate to sandbox database (default)
#   ./bin/bigbang_xenium.sh
#
#   # Migrate to main app database
#   ./bin/bigbang_xenium.sh --target-db app
#
#   # Clear all legacy migration data before migration
#   ./bin/bigbang_xenium.sh --target-db sandbox --clear-target-db
#
#   # Clear stuck process locks from previous failed run
#   ./bin/bigbang_xenium.sh --clear-locks
#
# Migration Steps (8 total):
#   1. Seed constants (roles, xenium system user, analysis types, import sources)
#   2. Sync users
#   3. Sync datasets (RAW_DATA and DATA_PRODUCT)
#   4. Sync dataset audit logs
#   5. Sync dataset import logs
#   6. Sync dataset hierarchies (RAW_DATA → DATA_PRODUCT)
#   7. Sync projects
#   8. Initialize xenium poller cursors
#
# Process Locking:
#   Uses xenium_sync_process_lock table to prevent concurrent runs.
#   If a previous run crashed, use --clear-locks to reset.
#
# Idempotency:
#   This migration is idempotent. Existing records are skipped based on
#   xenium_id or unique constraints. Data inserted before any error is retained.
#
# Logs:
#   Detailed logs are written to: data_sync/logs/bigbang_xenium_sync_*.log
#   On production host, logs are in: /tmp/data_sync_logs/
#
# Exit Codes:
#   0 - Success
#   1 - Migration failed (check logs)
#   2 - Another bigbang process is already running
#
##

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"
cd "$SCRIPT_DIR/.."

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

show_help() {
  grep '^#' "$SCRIPT_PATH" | sed 's/^# \?//' | sed 's/^##$//'
  exit 0
}

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

echo -e "${BLUE}${BOLD}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}${BOLD}║   Xenium → Bioloop Big-Bang Migration                    ║${NC}"
echo -e "${BLUE}${BOLD}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}Starting xenium big-bang migration...${NC}"
echo ""

docker compose -f "$COMPOSE_FILE" exec db_sandbox node /opt/sca/app/src/bigbang_xenium_sync.js "$@"
exit_code=$?

echo ""
if [ $exit_code -eq 0 ]; then
  echo -e "${GREEN}✓ Xenium big-bang migration completed successfully${NC}"
  echo ""
  echo "Next steps:"
  echo "  1. Verify data integrity in Bioloop database"
  echo "  2. Start xenium pollers: ./bin/start_pollers_xenium.sh --target-db=<your-target>"
elif [ $exit_code -eq 2 ]; then
  echo -e "${RED}✗ Another xenium bigbang process is already running${NC}"
  echo -e "${YELLOW}  Use --clear-locks to force release locks${NC}"
else
  echo -e "${RED}✗ Xenium big-bang migration failed${NC}"
  echo -e "${YELLOW}  Check logs in data_sync/logs/ for details${NC}"
fi

exit $exit_code
