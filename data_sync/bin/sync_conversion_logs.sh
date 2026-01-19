#!/bin/bash

##
# CMG Conversion Logs Sync - Standalone Script
#
# Populates historic CMG conversion logs from filesystem into Bioloop's
# worker_process and log tables.
#
# This script MUST be run from the host (not inside the container).
# It will automatically exec into the db_sandbox container to run the sync.
#
# Usage (from host):
#   cd /opt/sca/cmg/data_sync
#   ./bin/sync_conversion_logs.sh [options]
#
# Options:
#   --target-db <app|sandbox|custom>  Target database (default: sandbox)
#   --dry-run                         Discover logs without inserting to DB
#   --overwrite-existing              Re-process conversions with existing logs
#   --help                            Show this help message
#
# Target Databases:
#   app     - Main application database (reads from api/.env)
#   sandbox - Isolated sync database (for testing)
#   custom  - Use DATABASE_URL environment variable
#
# Examples:
#   # Dry run to see what logs would be processed
#   ./bin/sync_conversion_logs.sh --dry-run
#
#   # Sync logs to sandbox database (testing)
#   ./bin/sync_conversion_logs.sh --target-db sandbox
#   ./bin/sync_conversion_logs.sh --target-db=sandbox
#
#   # Sync logs to main application database (production)
#   ./bin/sync_conversion_logs.sh --target-db app
#   ./bin/sync_conversion_logs.sh --target-db=app
#
#   # Re-process all conversions (overwrite existing logs)
#   ./bin/sync_conversion_logs.sh --target-db=app --overwrite-existing
#
# Environment Variables:
#   CMG_LEGACY_CONVERSIONS_LOGS_DIR   Path to CMG conversion logs
#                                      Container: /opt/sca/project/ingestion_source_dir/CMG-SCA/production/runlogs
#                                      Host: /N/project/CMG-SCA/production/runlogs
#
# Alternative: Run Inside Container
#   If you're already inside the db_sandbox container, you can run directly:
#     node /opt/sca/app/src/standalone_sync_conversion_logs.js [options]
#
# Notes:
#   - This script is idempotent (safe to re-run)
#   - Skips conversions that already have logs populated
#   - Each conversion gets its own worker_process record
#   - Multiple conversions on same dataset share one log file,
#     but each gets duplicate log entries in the database
##

set -e

# Get script directory (data_sync/bin)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_SYNC_DIR="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if we're inside the container (check for container-specific env var or path)
if [[ -d "/opt/sca/app" && -f "/opt/sca/app/src/standalone_sync_conversion_logs.js" ]]; then
  # We're inside the container - run directly
  echo -e "${BLUE}Detected running inside container. Executing directly...${NC}"
  echo ""
  exec node /opt/sca/app/src/standalone_sync_conversion_logs.js "$@"
fi

# We're on the host - need to exec into container

# Parse command line arguments to check for --help
SHOW_HELP=false
for arg in "$@"; do
  if [[ "$arg" == "--help" ]]; then
    SHOW_HELP=true
  fi
done

if $SHOW_HELP; then
  grep '^#' "$0" | sed 's/^# \?//'
  exit 0
fi

echo ""
echo -e "${GREEN}===========================================================${NC}"
echo -e "${GREEN}CMG Conversion Logs Sync - Standalone${NC}"
echo -e "${GREEN}===========================================================${NC}"
echo ""
echo -e "${BLUE}This script will exec into the db_sandbox container to run the sync.${NC}"
echo ""

# Change to data_sync directory
cd "$DATA_SYNC_DIR"

# Check if docker compose is available
if ! command -v docker &> /dev/null; then
  echo -e "${RED}Error: Docker is not installed or not in PATH${NC}"
  exit 1
fi

# Check if container is running
if ! docker compose -f docker-compose.sandbox.yml ps | grep -q "bioloop_db_sandbox.*Up"; then
  echo -e "${RED}Error: db_sandbox container is not running${NC}"
  echo ""
  echo "Start the container first:"
  echo -e "  ${YELLOW}cd /opt/sca/cmg/data_sync${NC}"
  echo -e "  ${YELLOW}docker compose -f docker-compose.sandbox.yml up -d${NC}"
  echo ""
  exit 1
fi

# Execute inside container (pass all arguments)
echo -e "${YELLOW}Executing inside db_sandbox container...${NC}"
echo ""

docker compose -f docker-compose.sandbox.yml exec db_sandbox node /opt/sca/app/src/standalone_sync_conversion_logs.js "$@"

EXIT_CODE=$?

echo ""
if [[ $EXIT_CODE -eq 0 ]]; then
  echo -e "${GREEN}✓ Conversion logs sync completed successfully${NC}"
else
  echo -e "${RED}✗ Conversion logs sync failed (exit code: $EXIT_CODE)${NC}"
fi
echo ""

exit $EXIT_CODE
