#!/bin/bash

#===============================================================================
# Script: sync_cmg_schema_to_app.sh
# Description: Backs up cmg schema from data_sync postgres and restores it to 
#              the main app's postgres database.
#
# Usage: ./sync_cmg_schema_to_app.sh [--force]
#
# Options:
#   --force    Skip production check (use with extreme caution)
#
# Requirements:
#   - Must be run from the project root or data_sync directory
#   - Both docker compose stacks must be running
#   - Uses docker compose commands (not docker commands)
#===============================================================================

set -e  # Exit on error
set -u  # Exit on undefined variable

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
FORCE_MODE=false
while [[ $# -gt 0 ]]; do
  case $1 in
    --force)
      FORCE_MODE=true
      shift
      ;;
    *)
      echo -e "${RED}Error: Unknown option: $1${NC}"
      echo "Usage: $0 [--force]"
      exit 1
      ;;
  esac
done

# Function to print colored messages
info() {
  echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
  echo -e "${GREEN}[OK]${NC} $1"
}

warning() {
  echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

#===============================================================================
# Production Environment Check
#===============================================================================

info "Checking environment..."

# Check hostname
CURRENT_HOSTNAME=$(hostname)
if [[ "$CURRENT_HOSTNAME" == "cmg-new-service1.sca.iu.edu" ]] && [[ "$FORCE_MODE" != "true" ]]; then
  error "PRODUCTION ENVIRONMENT DETECTED!"
  error "This script is intended for development/testing only."
  error ""
  error "Hostname: $CURRENT_HOSTNAME"
  error ""
  error "⚠️  Running this script in production could overwrite production data!"
  error ""
  error "If you absolutely must run this in production, use: $0 --force"
  error "However, it is strongly recommended to test in development first."
  exit 1
fi

# Check NODE_ENV from api/.env if it exists
if [[ -f "api/.env" ]]; then
  NODE_ENV=$(grep -E "^NODE_ENV=" api/.env | cut -d '=' -f2 | tr -d '"' | tr -d "'" || echo "")
  if [[ "$NODE_ENV" == "production" ]] && [[ "$FORCE_MODE" != "true" ]]; then
    error "PRODUCTION ENVIRONMENT DETECTED in api/.env!"
    error "NODE_ENV=production"
    error ""
    error "⚠️  Running this script could overwrite production data!"
    error ""
    error "If you absolutely must proceed, use: $0 --force"
    exit 1
  fi
fi

if [[ "$FORCE_MODE" == "true" ]]; then
  warning "⚠️  FORCE MODE ENABLED - Production checks bypassed!"
  warning "Hostname: $CURRENT_HOSTNAME"
  warning ""
  read -p "Are you ABSOLUTELY sure you want to continue? (type 'YES' to proceed): " confirmation
  if [[ "$confirmation" != "YES" ]]; then
    info "Operation cancelled."
    exit 0
  fi
fi

success "Environment check passed (development mode)"

#===============================================================================
# Path Setup
#===============================================================================

info "Detecting project paths..."

# Determine script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT=""

# Check if we're in data_sync/bin
if [[ "$(basename "$(dirname "$SCRIPT_DIR")")" == "data_sync" ]]; then
  PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
  info "Detected data_sync/bin directory, project root: $PROJECT_ROOT"
# Check if we're in data_sync
elif [[ "$(basename "$SCRIPT_DIR")" == "bin" ]] && [[ "$(basename "$(dirname "$SCRIPT_DIR")")" == "data_sync" ]]; then
  PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
  info "Detected data_sync directory, project root: $PROJECT_ROOT"
# Check if we're in project root
elif [[ -d "$SCRIPT_DIR/data_sync" ]] && [[ -d "$SCRIPT_DIR/api" ]]; then
  PROJECT_ROOT="$SCRIPT_DIR"
  info "Detected project root directory"
else
  error "Cannot determine project root directory!"
  error "Please run this script from:"
  error "  - Project root"
  error "  - data_sync directory"
  error "  - data_sync/bin directory"
  exit 1
fi

DATA_SYNC_DIR="$PROJECT_ROOT/data_sync"
BACKUP_DIR="/tmp"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="$BACKUP_DIR/cmg_schema_backup_${TIMESTAMP}.sql"

success "Project root: $PROJECT_ROOT"
success "Data sync directory: $DATA_SYNC_DIR"
success "Backup file: $BACKUP_FILE"

#===============================================================================
# Container Status Check
#===============================================================================

info "Checking docker containers..."

# Check main app postgres container
cd "$PROJECT_ROOT"
if ! docker compose ps postgres | grep -q "Up"; then
  error "Main app postgres container is not running!"
  error "Start it with: cd $PROJECT_ROOT && docker compose up -d postgres"
  exit 1
fi
success "Main app postgres container is running"

# Check data_sync postgres container
cd "$DATA_SYNC_DIR"
if ! docker compose -f docker-compose.sandbox.yml ps db_sandbox | grep -q "Up"; then
  error "Data sync postgres container (db_sandbox) is not running!"
  error "Start it with: cd $DATA_SYNC_DIR && docker compose -f docker-compose.sandbox.yml up -d"
  exit 1
fi
success "Data sync postgres container is running"

#===============================================================================
# Database Connection Info
#===============================================================================

info "Reading database connection info..."

# Get sandbox database info from data_sync/.env.default
SANDBOX_DB_NAME="bioloop_sync"
SANDBOX_DB_USER="appuser"

# Get main app database info from db/postgres/.env.default
APP_DB_NAME="app"
APP_DB_USER="appuser"

success "Sandbox DB: $SANDBOX_DB_NAME (user: $SANDBOX_DB_USER)"
success "App DB: $APP_DB_NAME (user: $APP_DB_USER)"

#===============================================================================
# Backup cmg Schema from Sandbox DB
#===============================================================================

info "Backing up 'cmg' schema from data_sync postgres..."

cd "$DATA_SYNC_DIR"

# Export cmg schema only (data and structure)
if docker compose -f docker-compose.sandbox.yml exec -T db_sandbox \
  pg_dump -U "$SANDBOX_DB_USER" -d "$SANDBOX_DB_NAME" \
  --schema=cmg \
  --no-owner \
  --no-acl \
  > "$BACKUP_FILE"; then
  success "Backup created: $BACKUP_FILE"
  BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
  info "Backup size: $BACKUP_SIZE"
else
  error "Failed to create backup!"
  exit 1
fi

# Verify backup file is not empty
if [[ ! -s "$BACKUP_FILE" ]]; then
  error "Backup file is empty! cmg schema may not exist in sandbox DB."
  error "Run bigbang sync first to populate the sandbox database."
  rm "$BACKUP_FILE"
  exit 1
fi

#===============================================================================
# Drop Existing cmg Schema in App DB (if exists)
#===============================================================================

info "Dropping existing 'cmg' schema in main app postgres (if exists)..."

cd "$PROJECT_ROOT"

if docker compose exec -T postgres \
  psql -U "$APP_DB_USER" -d "$APP_DB_NAME" \
  -c "DROP SCHEMA IF EXISTS cmg CASCADE;" > /dev/null 2>&1; then
  success "Existing cmg schema dropped (or didn't exist)"
else
  warning "Could not drop cmg schema (may not exist, continuing...)"
fi

#===============================================================================
# Restore cmg Schema to App DB
#===============================================================================

info "Restoring 'cmg' schema to main app postgres..."

cd "$PROJECT_ROOT"

if cat "$BACKUP_FILE" | docker compose exec -T postgres \
  psql -U "$APP_DB_USER" -d "$APP_DB_NAME"; then
  success "cmg schema restored successfully to app database!"
else
  error "Failed to restore cmg schema to app database!"
  error "Backup file preserved at: $BACKUP_FILE"
  exit 1
fi

#===============================================================================
# Verify Restoration
#===============================================================================

info "Verifying restoration..."

cd "$PROJECT_ROOT"

# Check if cmg schema exists
SCHEMA_CHECK=$(docker compose exec -T postgres \
  psql -U "$APP_DB_USER" -d "$APP_DB_NAME" \
  -t -c "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'cmg';" \
  | tr -d '[:space:]')

if [[ "$SCHEMA_CHECK" == "1" ]]; then
  success "cmg schema exists in app database"
  
  # Count tables in cmg schema
  TABLE_COUNT=$(docker compose exec -T postgres \
    psql -U "$APP_DB_USER" -d "$APP_DB_NAME" \
    -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'cmg';" \
    | tr -d '[:space:]')
  
  info "Tables in cmg schema: $TABLE_COUNT"
  
  # List tables
  info "Tables in cmg schema:"
  docker compose exec -T postgres \
    psql -U "$APP_DB_USER" -d "$APP_DB_NAME" \
    -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'cmg' ORDER BY table_name;"
else
  error "cmg schema not found in app database after restoration!"
  error "Backup file preserved at: $BACKUP_FILE"
  exit 1
fi

#===============================================================================
# Cleanup
#===============================================================================

info "Cleaning up..."

if [[ -f "$BACKUP_FILE" ]]; then
  rm "$BACKUP_FILE"
  success "Temporary backup file deleted"
fi

#===============================================================================
# Summary
#===============================================================================

echo ""
echo "═══════════════════════════════════════════════════════════════"
success "cmg Schema Sync Complete!"
echo "═══════════════════════════════════════════════════════════════"
echo ""
info "Source: data_sync postgres (db_sandbox) - database: $SANDBOX_DB_NAME"
info "Target: main app postgres - database: $APP_DB_NAME"
info "Schema: cmg"
info "Tables: $TABLE_COUNT"
echo ""
warning "The cmg schema in the main app database has been replaced with"
warning "data from the data_sync container."
echo ""
info "Next steps:"
info "  1. Verify the data in the app database"
info "  2. Test the application to ensure everything works"
info "  3. Run any necessary migrations if schema structure changed"
echo ""
echo "═══════════════════════════════════════════════════════════════"


