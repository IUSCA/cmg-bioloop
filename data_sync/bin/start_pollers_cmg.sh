#!/bin/bash

##
# CMG to Bioloop Continuous Sync Pollers
#
# Starts continuous polling processes that watch CMG (MongoDB) for changes
# and incrementally sync them to Bioloop (PostgreSQL).
#
# Prerequisites:
#   - Bigbang migration must have been run first (to initialize cursors)
#   - Cursor positions must exist in cmg_sync_cursor table
#
# Usage:
#   ./bin/start_pollers.sh [options]
#
# Options:
#   --target-db DB         Target database: 'sandbox' (default), 'app', or 'custom'
#   -h, --help             Show this help message
#
# Environment Variables:
#   See migrate.sh for full list of environment variables
#
# Examples:
#
#   # Start pollers for sandbox database (default)
#   ./bin/start_pollers.sh
#
#   # Start pollers for main app database
#   ./bin/start_pollers.sh --target-db app
#
# Active Pollers:
#   The script starts 6 concurrent pollers, each watching a specific aspect:
#
#   1. user_roles          - User role assignments
#   2. project_acl         - Project access control lists
#   3. dataset_activity    - Dataset lifecycle flags (archived, staged, etc.)
#   4. dataset_metadata    - Dataset metadata changes
#   5. project_metadata    - Project metadata changes
#   6. session_metadata    - Genome browser session metadata
#
# Polling Behavior:
#   - Each poller runs independently every 10 seconds
#   - Processes up to 200 documents per round (batch size)
#   - Uses cursor-based sync to track progress
#   - Failed documents are logged to cmg_sync_retry for future retry
#   - Uses bounded window to prevent "moving target" issues
#
# Process Locking:
#   - Uses database-level lock to prevent multiple poller processes
#   - Individual cursor locks prevent concurrent poller conflicts
#   - Lock TTL: 2 minutes (auto-releases if process crashes)
#
# Stopping Pollers:
#   Press Ctrl+C to gracefully stop all pollers.
#   Cursor positions are saved, so pollers can resume from where they left off.
#
# Monitoring:
#   Check cursor status in the database:
#   SELECT * FROM cmg_sync_cursor ORDER BY poller_name;
#
#   Check retry queue:
#   SELECT * FROM cmg_sync_retry ORDER BY next_retry_at;
#
# Logs:
#   Detailed logs are written to: data_sync/logs/poller_sync_*.log
#   On production host, logs are in: /tmp/data_sync_logs/
#
# Exit Codes:
#   0 - Pollers stopped gracefully (Ctrl+C)
#   1 - Error during startup or runtime
#   2 - Another poller process is already running
#   3 - Cursors not initialized (run bigbang first)
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
BLUE='\033[0;34m'
RED='\033[0;31m'
BOLD='\033[1m'
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

# Check if Node.js is available
if ! command -v node &> /dev/null; then
  echo -e "${RED}Error: Node.js is not installed or not in PATH${NC}"
  exit 1
fi

# Display header
echo -e "${BLUE}${BOLD}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}${BOLD}║   CMG → Bioloop Continuous Sync Pollers (CMG source)                  ║${NC}"
echo -e "${BLUE}${BOLD}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}Starting 6 concurrent pollers...${NC}"
echo ""
echo "Pollers will check for changes every 10 seconds."
echo "Press Ctrl+C to stop."
echo ""

# Run the Node.js poller script
node src/poller_cmg_sync.js "$@"
exit_code=$?

echo ""
if [ $exit_code -eq 0 ]; then
  echo -e "${GREEN}✓ Pollers stopped gracefully${NC}"
elif [ $exit_code -eq 2 ]; then
  echo -e "${RED}✗ Another poller process is already running${NC}"
  echo -e "${YELLOW}  Wait for it to finish or manually clear locks${NC}"
elif [ $exit_code -eq 3 ]; then
  echo -e "${RED}✗ Cursors not initialized${NC}"
  echo -e "${YELLOW}  Run bigbang migration first: ./bin/bigbang.sh${NC}"
else
  echo -e "${RED}✗ Poller process failed${NC}"
  echo -e "${YELLOW}  Check logs in data_sync/logs/ for details${NC}"
fi

exit $exit_code

