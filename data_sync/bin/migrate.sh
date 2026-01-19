#!/bin/bash

##
# CMG to Bioloop Database Migration - Interactive Wrapper
#
# This script provides an interactive interface for database migration from
# CMG (legacy MongoDB) to Bioloop (PostgreSQL). Users can choose between:
#
#   1. One-time database population (bigbang migration only)
#   2. Database population + continuous sync (bigbang + pollers)
#
# The underlying scripts can still be run independently:
#   - ./bin/bigbang.sh       - Run bigbang migration only
#   - ./bin/start_pollers.sh - Run continuous sync pollers only
#
# Usage:
#   ./bin/migrate.sh [options]
#
# Options:
#   --target-db DB         Target database: 'sandbox' (default), 'app', or 'custom'
#   --clear-target-db      Clear all data from target DB before migration
#   --clear-locks          Force release all existing process locks
#   --skip-sessions        Skip genome browser session conversion
#   -h, --help             Show this help message
#
# Environment Variables (optional):
#   CMG_MONGO_HOST         CMG MongoDB host (default: from config)
#   CMG_MONGO_PORT         CMG MongoDB port (default: from config)
#   CMG_MONGO_DB           CMG MongoDB database (default: from config)
#   CMG_MONGO_USERNAME     CMG MongoDB username (default: from config)
#   CMG_MONGO_PASSWORD     CMG MongoDB password (default: from config)
#
# Examples:
#
#   # Interactive mode - shows menu to choose migration type
#   ./bin/migrate.sh
#
#   # Migrate to sandbox database (default), interactive
#   ./bin/migrate.sh --target-db sandbox
#
#   # Migrate to main app database, clear existing data first
#   ./bin/migrate.sh --target-db app --clear-target-db
#
#   # Skip genome browser sessions during migration
#   ./bin/migrate.sh --skip-sessions
#
#   # Clear stuck locks from previous failed run
#   ./bin/migrate.sh --clear-locks
#
# Migration Modes:
#
#   Mode 1: Populate Database Only
#   --------------------------------
#   - Performs one-time historical data migration from CMG to Bioloop
#   - Initializes cursor positions for future incremental sync
#   - Does NOT start continuous polling
#   - Use this for: testing, sandbox environments, or when you want to
#     manually control when continuous sync starts
#
#   Mode 2: Populate + Start Continuous Sync
#   -----------------------------------------
#   - Performs historical data migration (same as Mode 1)
#   - Then automatically starts continuous sync pollers
#   - Pollers will watch CMG for changes and sync them to Bioloop
#   - Use this for: production deployments, automated setups
#
#   Mode 3: Start Continuous Sync Only (Skip Population)
#   -----------------------------------------------------
#   - Starts continuous sync pollers WITHOUT running bigbang migration
#   - REQUIRES that bigbang was already run previously
#   - Pollers will fail if cursor positions are not initialized
#   - Use this for: restarting pollers after they were stopped, or
#     starting pollers separately from initial migration
#
# Database Targeting:
#
#   --target-db sandbox (default)
#   - Writes to isolated sandbox database (defined in docker-compose.sandbox.yml)
#   - Safe for testing, will not affect production data
#
#   --target-db app
#   - Writes to main application database (reads from api/.env)
#   - Use for production migration
#   - Requires api/.env to be properly configured
#
#   --target-db custom
#   - Uses DATABASE_URL from environment variable
#   - For advanced/custom database configurations
#
# Process Lock Management:
#
#   The migration uses database locks to prevent concurrent runs.
#   If a previous run crashed, you may need to clear locks:
#
#   ./bin/migrate.sh --clear-locks
#
# Notes:
#   - The bigbang migration is idempotent (can be run multiple times)
#   - Data inserted before any error is retained in the database
#   - Pollers will start from where bigbang left off (seamless handoff)
#   - Both underlying scripts write detailed logs to data_sync/logs/
#
##

set -e

# Get script directory and change to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Help function
show_help() {
  grep '^#' "$0" | sed 's/^# \?//' | sed 's/^##$//'
  exit 0
}

# Parse arguments
ARGS=()
for arg in "$@"; do
  case $arg in
    -h|--help)
      show_help
      ;;
    *)
      ARGS+=("$arg")
      ;;
  esac
done

# Display header
echo ""
echo -e "${BLUE}${BOLD}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}${BOLD}║   CMG → Bioloop Database Migration System                ║${NC}"
echo -e "${BLUE}${BOLD}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}Choose migration mode (use ↑↓ arrows and Enter):${NC}"
echo ""

# Select menu
PS3="$(echo -e ${BOLD}Your choice:${NC} )"
options=(
  "Populate database only (one-time historical migration)"
  "Populate database + start continuous sync pollers"
  "Start continuous sync pollers only (skip database population)"
  "Exit"
)

select opt in "${options[@]}"
do
  case $REPLY in
    1)
      echo ""
      echo -e "${GREEN}✓ Selected: Database population only${NC}"
      echo -e "${YELLOW}  Mode: Bigbang migration${NC}"
      echo ""
      
      # Run bigbang only
      ./bin/bigbang.sh "${ARGS[@]}"
      exit_code=$?
      
      echo ""
      if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}${BOLD}✓ Migration complete!${NC}"
        echo ""
        echo "Database has been populated with historical CMG data."
        echo "Cursor positions have been initialized for future sync."
        echo ""
        echo -e "To start continuous sync later, run:"
        echo -e "  ${BLUE}./bin/start_pollers.sh${NC}"
      else
        echo -e "${RED}${BOLD}✗ Migration failed with exit code $exit_code${NC}"
        echo ""
        echo "Check the logs in data_sync/logs/ for details."
      fi
      echo ""
      exit $exit_code
      ;;
    
    2)
      echo ""
      echo -e "${GREEN}✓ Selected: Database population + continuous sync${NC}"
      echo -e "${YELLOW}  Mode: Bigbang migration → Continuous pollers${NC}"
      echo ""
      
      # Run bigbang
      echo -e "${BLUE}[Step 1/2] Running bigbang migration...${NC}"
      echo ""
      ./bin/bigbang.sh "${ARGS[@]}"
      bigbang_exit=$?
      
      if [ $bigbang_exit -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✓ Database population complete!${NC}"
        echo ""
        echo -e "${BLUE}[Step 2/2] Starting continuous sync pollers...${NC}"
        echo ""
        
        # Remove flags that don't apply to pollers
        POLLER_ARGS=()
        for arg in "${ARGS[@]}"; do
          case $arg in
            --clear-target-db|--skip-sessions|--clear-locks)
              # Skip these flags (bigbang-only)
              ;;
            *)
              POLLER_ARGS+=("$arg")
              ;;
          esac
        done
        
        ./bin/start_pollers.sh "${POLLER_ARGS[@]}"
        poller_exit=$?
        exit $poller_exit
      else
        echo ""
        echo -e "${RED}${BOLD}✗ Database population failed. Not starting pollers.${NC}"
        echo ""
        echo "Check the logs in data_sync/logs/ for details."
        echo ""
        exit $bigbang_exit
      fi
      ;;
    
    3)
      echo ""
      echo -e "${YELLOW}⚠️  Selected: Start continuous sync pollers only${NC}"
      echo -e "${YELLOW}  Mode: Pollers without bigbang${NC}"
      echo ""
      echo -e "${RED}${BOLD}WARNING:${NC} This assumes the database has already been populated!"
      echo ""
      echo "Prerequisites:"
      echo "  • Bigbang migration must have been run previously"
      echo "  • Cursor positions must be initialized in the database"
      echo "  • If cursors don't exist, pollers will fail immediately"
      echo ""
      echo "If you haven't run bigbang yet, press Ctrl+C now and choose option 1 or 2."
      echo ""
      read -p "$(echo -e ${BOLD}Continue anyway? [y/N]:${NC} )" -n 1 -r
      echo ""
      
      if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "Cancelled. Returning to menu..."
        echo ""
        exec "$0" "${ARGS[@]}"
      fi
      
      echo ""
      echo -e "${GREEN}✓ Confirmed. Starting continuous sync pollers...${NC}"
      echo ""
      
      # Remove flags that don't apply to pollers
      POLLER_ARGS=()
      for arg in "${ARGS[@]}"; do
        case $arg in
          --clear-target-db|--skip-sessions|--clear-locks|--skip-conversion-logs)
            # Skip these flags (bigbang-only)
            ;;
          *)
            POLLER_ARGS+=("$arg")
            ;;
        esac
      done
      
      ./bin/start_pollers.sh "${POLLER_ARGS[@]}"
      poller_exit=$?
      exit $poller_exit
      ;;
    
    4)
      echo ""
      echo "Exiting..."
      echo ""
      exit 0
      ;;
    
    *)
      echo -e "${RED}Invalid option $REPLY. Please enter 1, 2, 3, or 4.${NC}"
      ;;
  esac
done

