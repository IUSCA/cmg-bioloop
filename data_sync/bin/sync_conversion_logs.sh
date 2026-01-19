#!/bin/bash

##
# CMG Conversion Logs Sync - Standalone Script
#
# Populates historic CMG conversion logs from filesystem into Bioloop's
# worker_process and log tables.
#
# This script can be run independently of the bigbang migration.
#
# Usage:
#   ./bin/sync_conversion_logs.sh [options]
#
# Options:
#   --target-db <app|sandbox|custom>  Target database (default: sandbox)
#   --dry-run                         Discover logs without inserting to DB
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
#
#   # Sync logs to main application database (production)
#   ./bin/sync_conversion_logs.sh --target-db app
#
# Environment Variables:
#   CMG_LEGACY_CONVERSIONS_LOGS_DIR   Path to CMG conversion logs
#                                      (default: /N/project/CMG-SCA/runlogs)
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

# Default values
TARGET_DB="sandbox"
DRY_RUN=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --target-db)
      TARGET_DB="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN="--dry-run"
      shift
      ;;
    --help)
      grep '^#' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *)
      echo -e "${RED}Error: Unknown option $1${NC}"
      echo "Run with --help for usage information"
      exit 1
      ;;
  esac
done

# Validate target-db
if [[ "$TARGET_DB" != "app" && "$TARGET_DB" != "sandbox" && "$TARGET_DB" != "custom" ]]; then
  echo -e "${RED}Error: Invalid --target-db value: $TARGET_DB${NC}"
  echo "Must be one of: app, sandbox, custom"
  exit 1
fi

echo -e "${GREEN}===========================================================${NC}"
echo -e "${GREEN}CMG Conversion Logs Sync - Standalone${NC}"
echo -e "${GREEN}===========================================================${NC}"
echo ""

if [[ -n "$DRY_RUN" ]]; then
  echo -e "${YELLOW}MODE: DRY RUN - Discovery only, no database writes${NC}"
else
  echo -e "${YELLOW}MODE: SYNC - Will populate worker_process and log tables${NC}"
fi

echo "Target Database: ${TARGET_DB}"
echo ""

# Change to data_sync directory
cd "$DATA_SYNC_DIR"

# Check if node is available
if ! command -v node &> /dev/null; then
  echo -e "${RED}Error: Node.js is not installed or not in PATH${NC}"
  exit 1
fi

# Check if .env file exists (optional but recommended)
if [[ ! -f .env ]]; then
  echo -e "${YELLOW}Warning: .env file not found in data_sync directory${NC}"
  echo -e "${YELLOW}Using default configuration from .env.default${NC}"
  echo ""
fi

# Build node command
NODE_CMD="node src/standalone_sync_conversion_logs.js --target-db=$TARGET_DB"

if [[ -n "$DRY_RUN" ]]; then
  NODE_CMD="$NODE_CMD --dry-run"
fi

# Run the script
echo -e "${YELLOW}Executing: $NODE_CMD${NC}"
echo ""

$NODE_CMD

EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
  echo ""
  echo -e "${GREEN}✓ Conversion logs sync completed successfully${NC}"
else
  echo ""
  echo -e "${RED}✗ Conversion logs sync failed (exit code: $EXIT_CODE)${NC}"
fi

exit $EXIT_CODE

