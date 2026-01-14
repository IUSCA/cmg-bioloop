#!/bin/bash

##
# Pull Database Sync Logs from CMG Production Host
#
# This script pulls sync logs from the production host where the data_sync
# container is running. Logs are stored in the host's data_sync/logs directory.
#
# Usage:
#   ./bin/pull_logs_from_prod.sh [options]
#
# Options:
#   -h, --host HOST        Production host (default: cmg-bioloop)
#                          Uses SSH alias from ~/.ssh/config
#   -u, --user USER        SSH user (default: from SSH config - cmguser)
#   -o, --output DIR       Local output directory (default: ./logs_from_prod)
#   -n, --last N           Download last N log files (default: 1)
#   --all                  Download all log files
#   -l, --list             List available log files without downloading
#   --help                 Show this help message
#
# Note: Remote logs are always read from /tmp/data_sync_logs/ on the host
#       Files are sorted by timestamp (newest first)
#
# Examples:
#   # Pull latest log file (default)
#   ./bin/pull_logs_from_prod.sh
#
#   # List available logs
#   ./bin/pull_logs_from_prod.sh --list
#
#   # Pull last 3 log files
#   ./bin/pull_logs_from_prod.sh --last 3
#
#   # Pull all log files
#   ./bin/pull_logs_from_prod.sh --all
#
#   # Pull from different host
#   ./bin/pull_logs_from_prod.sh --host bioloop
#
#   # Pull last 5 logs to specific directory
#   ./bin/pull_logs_from_prod.sh --last 5 --output /path/to/local/logs
##

set -e

# Default values
PROD_HOST="cmg-bioloop"  # SSH alias from ~/.ssh/config
SSH_USER=""  # Leave empty to use SSH config default (cmguser)
REMOTE_LOGS_DIR="/tmp/data_sync_logs"  # Direct path to logs on host
LOCAL_OUTPUT="./logs_from_prod"
LIST_ONLY=false
LAST_N=1  # Default: download last 1 file
ALL_FILES=false

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--host)
      PROD_HOST="$2"
      shift 2
      ;;
    -u|--user)
      SSH_USER="$2"
      shift 2
      ;;
    -o|--output)
      LOCAL_OUTPUT="$2"
      shift 2
      ;;
    -n|--last)
      LAST_N="$2"
      shift 2
      ;;
    --all)
      ALL_FILES=true
      shift
      ;;
    -l|--list)
      LIST_ONLY=true
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

# Construct SSH connection string
if [ -n "$SSH_USER" ]; then
  SSH_CONN="${SSH_USER}@${PROD_HOST}"
  DISPLAY_USER="${SSH_USER}"
else
  SSH_CONN="${PROD_HOST}"
  DISPLAY_USER="(from SSH config)"
fi

# SSH options to avoid port forwarding conflicts
SSH_OPTS="-o ClearAllForwardings=yes"

echo -e "${GREEN}===========================================================${NC}"
echo -e "${GREEN}Pull Database Sync Logs from Production${NC}"
echo -e "${GREEN}===========================================================${NC}"
echo ""
echo "Production Host: ${PROD_HOST}"
echo "SSH User: ${DISPLAY_USER}"
echo "Remote Path: ${REMOTE_LOGS_DIR}"
echo ""

# Test SSH connection
echo -e "${YELLOW}Testing SSH connection...${NC}"
if ! ssh ${SSH_OPTS} -o ConnectTimeout=5 -o BatchMode=yes "${SSH_CONN}" "echo 'Connection successful'" >/dev/null 2>&1; then
  echo -e "${RED}Error: Cannot connect to ${SSH_CONN}${NC}"
  echo "Please check:"
  echo "  - Host is reachable"
  echo "  - SSH keys are configured"
  echo "  - User has access to the host"
  exit 1
fi
echo -e "${GREEN}✓ SSH connection successful${NC}"
echo ""

# Check if remote logs directory exists
echo -e "${YELLOW}Checking remote logs directory...${NC}"
if ! ssh ${SSH_OPTS} "${SSH_CONN}" "test -d ${REMOTE_LOGS_DIR}"; then
  echo -e "${RED}Error: Remote logs directory does not exist: ${REMOTE_LOGS_DIR}${NC}"
  echo ""
  echo "The logs directory should be created when the sync scripts run."
  echo "Have you run the bigbang or poller scripts on production yet?"
  exit 1
fi
echo -e "${GREEN}✓ Remote logs directory exists${NC}"
echo ""

# Get list of log files sorted by timestamp (newest first)
LOG_FILES=$(ssh ${SSH_OPTS} "${SSH_CONN}" "ls -t ${REMOTE_LOGS_DIR}/*.log 2>/dev/null" || echo "")

if [ -z "$LOG_FILES" ]; then
  echo -e "${YELLOW}No log files found on production${NC}"
  echo ""
  echo "This could mean:"
  echo "  - No sync scripts have been run yet"
  echo "  - Logs were cleaned up"
  echo "  - Logs are in a different location"
  exit 0
fi

# Apply filter based on --all or --last N
if [ "$ALL_FILES" = true ]; then
  FILTERED_FILES="$LOG_FILES"
  FILE_DESCRIPTION="all"
else
  FILTERED_FILES=$(echo "$LOG_FILES" | head -n "$LAST_N")
  FILE_DESCRIPTION="last ${LAST_N}"
fi

FILTERED_COUNT=$(echo "$FILTERED_FILES" | wc -l | tr -d ' ')

# List or pull logs
if [ "$LIST_ONLY" = true ]; then
  echo -e "${YELLOW}Available log files on production (showing ${FILE_DESCRIPTION}):${NC}"
  echo ""
  for file in $FILTERED_FILES; do
    ssh ${SSH_OPTS} "${SSH_CONN}" "ls -lh $file"
  done
  echo ""
  echo "To download logs, run without --list flag"
  echo "Use --all to see all files, or --last N to see last N files"
else
  # Create local output directory
  mkdir -p "${LOCAL_OUTPUT}"
  
  echo -e "${YELLOW}Pulling ${FILE_DESCRIPTION} (${FILTERED_COUNT} file(s))...${NC}"
  echo ""
  
  # Download each file
  for file in $FILTERED_FILES; do
    filename=$(basename "$file")
    echo "Downloading: $filename"
    scp ${SSH_OPTS} "${SSH_CONN}:${file}" "${LOCAL_OUTPUT}/" 2>&1 | grep -v "Sink: "
  done
  
  echo ""
  echo -e "${GREEN}✓ Logs successfully pulled to: ${LOCAL_OUTPUT}${NC}"
  echo ""
  echo "Downloaded files:"
  ls -lh "${LOCAL_OUTPUT}"/*.log 2>/dev/null | tail -n "$FILTERED_COUNT"
fi

echo ""
echo -e "${GREEN}Done!${NC}"

