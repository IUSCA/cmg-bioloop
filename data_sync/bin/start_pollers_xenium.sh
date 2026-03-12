#!/bin/bash

##
# Xenium → Bioloop Continuous Sync Pollers
#
# Starts continuous polling processes that watch the Xenium PostgreSQL database
# for changes and incrementally sync them into cmg-bioloop's PostgreSQL.
#
# Unlike the CMG pollers (MongoDB source), these are PostgreSQL-to-PostgreSQL
# pollers using timestamp + integer ID cursors.
#
# Prerequisites:
#   - Xenium bigbang migration must have been run first (initializes cursors)
#   - Cursor positions must exist in xenium_sync_cursor table
#
# Usage:
#   ./bin/start_pollers_xenium.sh [options]
#
# Options:
#   --target-db DB         Target database: 'sandbox' (default), 'app', or 'custom'
#   --clear-locks          Force release existing locks before starting
#   -h, --help             Show this help message
#
# Environment Variables:
#   XENIUM_DATABASE_URL    PostgreSQL connection URL for the Xenium source database
#
# Active Pollers (4 total):
#   1. xenium_user_roles      - User role assignment changes
#   2. xenium_project_acl     - Project user + dataset ACL changes
#   3. xenium_dataset_metadata - Dataset description changes
#   4. xenium_project_metadata - Project metadata changes
#
# Polling Behavior:
#   - Each poller runs independently every 10 seconds
#   - Processes up to 200 rows per round
#   - Uses cursor-based sync (updated_at + integer ID)
#   - Failed rows are logged to xenium_sync_retry for future retry
#
# Process Locking:
#   Uses xenium_sync_process_lock table to prevent concurrent poller runs.
#
# Stopping:
#   Press Ctrl+C to gracefully stop all pollers.
#   Cursor positions are saved; pollers resume from where they stopped.
#
# Logs:
#   Detailed logs written to: data_sync/logs/poller_xenium_sync_*.log
#   On production host, logs are in: /tmp/data_sync_logs/
#
# Exit Codes:
#   0 - Pollers stopped gracefully (Ctrl+C)
#   1 - Error during startup or runtime
#   2 - Another poller process is already running
#   3 - Cursors not initialized (run xenium bigbang first)
#
##

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"
cd "$SCRIPT_DIR/.."

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
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

if ! command -v node &> /dev/null; then
  echo -e "${RED}Error: Node.js is not installed or not in PATH${NC}"
  exit 1
fi

if [ -z "$XENIUM_DATABASE_URL" ]; then
  echo -e "${YELLOW}Warning: XENIUM_DATABASE_URL is not set.${NC}"
  echo -e "${YELLOW}  Set it in data_sync/.env or export it before running this script.${NC}"
  echo ""
fi

echo -e "${BLUE}${BOLD}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}${BOLD}║   Xenium → Bioloop Continuous Sync Pollers               ║${NC}"
echo -e "${BLUE}${BOLD}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}Starting 4 xenium pollers (PostgreSQL → PostgreSQL)...${NC}"
echo ""
echo "Pollers will check for changes every 10 seconds."
echo "Press Ctrl+C to stop."
echo ""

node src/poller_xenium_sync.js "$@"
exit_code=$?

echo ""
if [ $exit_code -eq 0 ]; then
  echo -e "${GREEN}✓ Xenium pollers stopped gracefully${NC}"
elif [ $exit_code -eq 2 ]; then
  echo -e "${RED}✗ Another xenium poller process is already running${NC}"
  echo -e "${YELLOW}  Wait for it to finish or use --clear-locks${NC}"
elif [ $exit_code -eq 3 ]; then
  echo -e "${RED}✗ Xenium cursors not initialized${NC}"
  echo -e "${YELLOW}  Run xenium bigbang migration first: ./bin/bigbang_xenium.sh${NC}"
else
  echo -e "${RED}✗ Xenium poller process failed${NC}"
  echo -e "${YELLOW}  Check logs in data_sync/logs/ for details${NC}"
fi

exit $exit_code
