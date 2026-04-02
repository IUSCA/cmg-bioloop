#!/bin/bash

##
# Bioloop Big-Bang Wrapper (Central — runs CMG then Xenium)
#
# Runs both legacy migrations sequentially:
#   1. CMG bigbang  (MongoDB → Bioloop PostgreSQL)
#   2. Xenium bigbang (Xenium PostgreSQL → Bioloop PostgreSQL)
#
# If CMG bigbang fails, the script aborts and Xenium bigbang is NOT run.
#
# Wrapper behavior:
#   Host-side launcher only. Executes Node.js scripts inside db_sandbox container.
#   Compose file is selected from APP_ENV/NODE_ENV (production -> prod compose;
#   otherwise localhost compose).
#
# Usage:
#   ./bin/bigbang.sh [options]
#
# Options:
#   --target-db DB         Target database: 'sandbox' (default), 'app', or 'custom'
#   --clear-target-db      Clear all CMG + Xenium migration data from target DB before migration
#   --clear-locks          Force release all existing process locks
#   --skip-sessions        Skip genome browser session conversion (CMG only)
#   -h, --help             Show this help message
#
# Environment Variables:
# - CMG source:
#   CMG_MONGO_HOST, CMG_MONGO_PORT, CMG_MONGO_DB,
#   CMG_MONGO_USERNAME, CMG_MONGO_PASSWORD
# - Xenium source:
#   XENIUM_PG_HOST, XENIUM_PG_PORT, XENIUM_PG_DATABASE,
#   XENIUM_PG_USERNAME, XENIUM_PG_PASSWORD
#
# Examples:
#
#   # Migrate both apps to sandbox database (default)
#   ./bin/bigbang.sh
#
#   # Migrate both apps to main app database
#   ./bin/bigbang.sh --target-db app
#
#   # Clear existing data before migration
#   ./bin/bigbang.sh --target-db sandbox --clear-target-db
#
#   # Skip genome browser sessions (faster migration for testing)
#   ./bin/bigbang.sh --skip-sessions
#
#   # Clear stuck process locks from previous failed run
#   ./bin/bigbang.sh --clear-locks
#
# Process Locking:
#   Each bigbang acquires its own process lock (cmg_sync_process_lock /
#   xenium_sync_process_lock). If a previous run crashed, use --clear-locks.
#
# Idempotency:
#   Both migrations are idempotent — they can be safely re-run.
#   Existing records are skipped based on cmg_id / xenium_id or unique constraints.
#
# Logs:
#   CMG logs:    /tmp/data_sync_logs/bigbang_cmg_sync_*.log
#   Xenium logs: /tmp/data_sync_logs/bigbang_xenium_sync_*.log
#
# Exit Codes:
#   0 - Both migrations succeeded
#   1 - Migration failed (check logs)
#   2 - Another bigbang process is already running
#
##

set -e

# Get script directory and change to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Help function
show_help() {
  grep '^#' "$0" | sed 's/^# \?//' | sed 's/^##$//'
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

# Build per-phase arg lists:
# - CMG gets all user flags.
# - Xenium gets all flags EXCEPT --clear-target-db so we don't run the
#   expensive legacy-data clear twice in one wrapper invocation.
CMG_ARGS=("$@")
XENIUM_ARGS=()
for arg in "$@"; do
  if [ "$arg" = "--clear-target-db" ]; then
    continue
  fi
  XENIUM_ARGS+=("$arg")
done

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

# ── Phase 1: CMG bigbang ──────────────────────────────────────────────────────

echo -e "${YELLOW}[1/2] Starting CMG big-bang migration...${NC}"
echo ""

docker compose -f "$COMPOSE_FILE" exec db_sandbox node /opt/sca/app/src/bigbang_cmg_sync.js "${CMG_ARGS[@]}"
cmg_exit=$?

echo ""
if [ $cmg_exit -ne 0 ]; then
  if [ $cmg_exit -eq 2 ]; then
    echo -e "${RED}✗ CMG bigbang: another process is already running${NC}"
    echo -e "${YELLOW}  Use --clear-locks to force release locks${NC}"
  else
    echo -e "${RED}✗ CMG big-bang migration failed — aborting (Xenium bigbang will NOT run)${NC}"
    echo -e "${YELLOW}  Check logs in /tmp/data_sync_logs/ for details${NC}"
  fi
  exit $cmg_exit
fi

echo -e "${GREEN}✓ CMG big-bang migration completed successfully${NC}"
echo ""

# ── Phase 2: Xenium bigbang ───────────────────────────────────────────────────

echo -e "${YELLOW}[2/2] Starting Xenium big-bang migration...${NC}"
echo ""

docker compose -f "$COMPOSE_FILE" exec db_sandbox node /opt/sca/app/src/bigbang_xenium_sync.js "${XENIUM_ARGS[@]}"
xenium_exit=$?

echo ""
if [ $xenium_exit -ne 0 ]; then
  if [ $xenium_exit -eq 2 ]; then
    echo -e "${RED}✗ Xenium bigbang: another process is already running${NC}"
    echo -e "${YELLOW}  Use --clear-locks to force release locks${NC}"
  else
    echo -e "${RED}✗ Xenium big-bang migration failed${NC}"
    echo -e "${YELLOW}  Check logs in /tmp/data_sync_logs/ for details${NC}"
  fi
  exit $xenium_exit
fi

echo -e "${GREEN}✓ Both CMG and Xenium big-bang migrations completed successfully${NC}"
echo ""
echo "Next steps:"
echo "  1. Verify data integrity in Bioloop database"
echo "  2. Start pollers: ./bin/start_pollers.sh"

exit 0

