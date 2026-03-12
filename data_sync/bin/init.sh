#!/bin/bash

##
# Bioloop Legacy Migration — Explicit Per-App Orchestrator
#
# This wrapper never infers app selection. You must explicitly choose each action:
#   - CMG bigbang / pollers
#   - Xenium bigbang / pollers
#
# Usage:
#   ./bin/init.sh [action flags] [app-specific options]
#
# Action Flags (explicit; at least one required):
#   --cmg-run-bigbang
#   --cmg-run-pollers
#   --xenium-run-bigbang
#   --xenium-run-pollers
#
# CMG Options:
#   --cmg-target-db DB               Target DB for CMG actions (default: sandbox)
#   --cmg-clear-locks                Pass --clear-locks to CMG script(s)
#   --cmg-clear-target-data          Clear CMG-originated rows before CMG bigbang
#   --cmg-skip-sessions              Pass-through to CMG bigbang
#   --cmg-skip-conversion-logs       Pass-through to CMG bigbang
#   --cmg-uri URI                    Pass-through to CMG bigbang
#
# Xenium Options:
#   --xenium-target-db DB            Target DB for Xenium actions (default: sandbox)
#   --xenium-clear-locks             Pass --clear-locks to Xenium script(s)
#   --xenium-clear-target-data       Clear Xenium-originated rows before Xenium bigbang
#
# General:
#   --dry-run                        Print resolved commands; do not execute
#   -h, --help                       Show this help message
#
# Examples:
#   ./bin/init.sh --cmg-run-bigbang
#   ./bin/init.sh --xenium-run-bigbang --xenium-run-pollers
#   ./bin/init.sh --cmg-run-bigbang --xenium-run-bigbang
#   ./bin/init.sh --cmg-run-bigbang --xenium-run-bigbang --cmg-target-db app --xenium-target-db sandbox
#   ./bin/init.sh --cmg-run-pollers --xenium-run-pollers
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
DIM='\033[2m'
NC='\033[0m'

RUN_CMG_BIGBANG=false
RUN_CMG_POLLERS=false
RUN_XENIUM_BIGBANG=false
RUN_XENIUM_POLLERS=false

CMG_TARGET_DB="sandbox"
XENIUM_TARGET_DB="sandbox"
CMG_CLEAR_LOCKS=false
XENIUM_CLEAR_LOCKS=false
CMG_CLEAR_TARGET_DATA=false
XENIUM_CLEAR_TARGET_DATA=false
DRY_RUN=false

CMG_EXTRA_BIGBANG_ARGS=()

show_help() {
  grep '^#' "$SCRIPT_PATH" | sed 's/^# \?//' | sed 's/^##$//'
  exit 0
}

log() {
  echo -e "$1"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $(echo "$1" | sed 's/\x1B\[[0-9;]*m//g')" >> "$LOG_FILE"
}

join_args() {
  local joined=""
  for arg in "$@"; do
    if [[ -z "$joined" ]]; then
      joined="$arg"
    else
      joined="$joined $arg"
    fi
  done
  echo "$joined"
}

header() {
  log ""
  log "${BLUE}${BOLD}═══════════════════════════════════════════════════════════${NC}"
  log "${BLUE}${BOLD}  $1${NC}"
  log "${BLUE}${BOLD}═══════════════════════════════════════════════════════════${NC}"
  log ""
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --cmg-run-bigbang) RUN_CMG_BIGBANG=true ;;
      --cmg-run-pollers) RUN_CMG_POLLERS=true ;;
      --xenium-run-bigbang) RUN_XENIUM_BIGBANG=true ;;
      --xenium-run-pollers) RUN_XENIUM_POLLERS=true ;;

      --cmg-target-db=*) CMG_TARGET_DB="${1#*=}" ;;
      --cmg-target-db) CMG_TARGET_DB="$2" ; shift ;;
      --xenium-target-db=*) XENIUM_TARGET_DB="${1#*=}" ;;
      --xenium-target-db) XENIUM_TARGET_DB="$2" ; shift ;;

      --cmg-clear-locks) CMG_CLEAR_LOCKS=true ;;
      --xenium-clear-locks) XENIUM_CLEAR_LOCKS=true ;;
      --cmg-clear-target-data) CMG_CLEAR_TARGET_DATA=true ;;
      --xenium-clear-target-data) XENIUM_CLEAR_TARGET_DATA=true ;;

      --cmg-skip-sessions) CMG_EXTRA_BIGBANG_ARGS+=("--skip-sessions") ;;
      --cmg-skip-conversion-logs) CMG_EXTRA_BIGBANG_ARGS+=("--skip-conversion-logs") ;;
      --cmg-uri=*) CMG_EXTRA_BIGBANG_ARGS+=("$1") ;;
      --cmg-uri)
        CMG_EXTRA_BIGBANG_ARGS+=("$1" "$2")
        shift
        ;;

      --target-db=*|--target-db|--clear-locks|--clear-cmg-db|--clear-xenium-db|--cmg|--xenium|--all|--bigbang|--pollers|--both)
        echo -e "${RED}Deprecated/ambiguous flag: $1${NC}"
        echo -e "${RED}Use explicit per-app flags (run with --help).${NC}"
        exit 1
        ;;

      --dry-run) DRY_RUN=true ;;
      -h|--help) show_help ;;
      *)
        echo -e "${RED}Unknown argument: $1${NC}"
        echo "Run with --help for usage."
        exit 1
        ;;
    esac
    shift
  done
}

build_cmg_bigbang_args() {
  local args=("--target-db=${CMG_TARGET_DB}")
  if [[ "$CMG_CLEAR_LOCKS" == true ]]; then args+=("--clear-locks"); fi
  if [[ "$CMG_CLEAR_TARGET_DATA" == true ]]; then args+=("--clear-cmg-target-data"); fi
  args+=("${CMG_EXTRA_BIGBANG_ARGS[@]}")
  echo "${args[@]}"
}

build_xenium_bigbang_args() {
  local args=("--target-db=${XENIUM_TARGET_DB}")
  if [[ "$XENIUM_CLEAR_LOCKS" == true ]]; then args+=("--clear-locks"); fi
  if [[ "$XENIUM_CLEAR_TARGET_DATA" == true ]]; then args+=("--clear-xenium-target-data"); fi
  echo "${args[@]}"
}

build_cmg_poller_args() {
  local args=("--target-db=${CMG_TARGET_DB}")
  if [[ "$CMG_CLEAR_LOCKS" == true ]]; then args+=("--clear-locks"); fi
  echo "${args[@]}"
}

build_xenium_poller_args() {
  local args=("--target-db=${XENIUM_TARGET_DB}")
  if [[ "$XENIUM_CLEAR_LOCKS" == true ]]; then args+=("--clear-locks"); fi
  echo "${args[@]}"
}

validate_args() {
  if [[ "$CMG_CLEAR_TARGET_DATA" == true && "$RUN_CMG_BIGBANG" != true ]]; then
    echo -e "${RED}Error: --cmg-clear-target-data requires --cmg-run-bigbang.${NC}"
    exit 1
  fi
  if [[ "$XENIUM_CLEAR_TARGET_DATA" == true && "$RUN_XENIUM_BIGBANG" != true ]]; then
    echo -e "${RED}Error: --xenium-clear-target-data requires --xenium-run-bigbang.${NC}"
    exit 1
  fi

  if [[ ${#CMG_EXTRA_BIGBANG_ARGS[@]} -gt 0 && "$RUN_CMG_BIGBANG" != true ]]; then
    echo -e "${RED}Error: CMG bigbang options were provided but --cmg-run-bigbang is not selected.${NC}"
    exit 1
  fi

  if [[ "$RUN_CMG_BIGBANG" != true && "$RUN_CMG_POLLERS" != true && "$RUN_XENIUM_BIGBANG" != true && "$RUN_XENIUM_POLLERS" != true ]]; then
    echo -e "${RED}Error: select at least one action flag (e.g., --cmg-run-bigbang).${NC}"
    echo "Run with --help for usage."
    exit 1
  fi

  if [[ "$RUN_CMG_BIGBANG" == true || "$RUN_CMG_POLLERS" == true ]]; then
    if [[ -z "$MONGO_URI" ]]; then
      echo -e "${YELLOW}Warning: MONGO_URI is not set (required for CMG operations).${NC}"
    fi
  fi
  if [[ "$RUN_XENIUM_BIGBANG" == true || "$RUN_XENIUM_POLLERS" == true ]]; then
    if [[ -z "$XENIUM_DATABASE_URL" ]]; then
      echo -e "${YELLOW}Warning: XENIUM_DATABASE_URL is not set (required for Xenium operations).${NC}"
    fi
  fi
}

run_cmg_bigbang() {
  header "CMG Bigbang Migration (MongoDB → PostgreSQL)"
  read -r -a args <<< "$(build_cmg_bigbang_args)"
  log "${YELLOW}Running: ./bin/bigbang_cmg.sh $(join_args "${args[@]}")${NC}"
  ./bin/bigbang_cmg.sh "${args[@]}"
}

run_xenium_bigbang() {
  header "Xenium Bigbang Migration (PostgreSQL → PostgreSQL)"
  read -r -a args <<< "$(build_xenium_bigbang_args)"
  log "${YELLOW}Running: ./bin/bigbang_xenium.sh $(join_args "${args[@]}")${NC}"
  ./bin/bigbang_xenium.sh "${args[@]}"
}

run_cmg_pollers() {
  header "CMG Pollers (MongoDB → PostgreSQL)"
  read -r -a args <<< "$(build_cmg_poller_args)"
  log "${YELLOW}Running: ./bin/start_pollers_cmg.sh $(join_args "${args[@]}")${NC}"
  ./bin/start_pollers_cmg.sh "${args[@]}"
}

run_xenium_pollers() {
  header "Xenium Pollers (PostgreSQL → PostgreSQL)"
  read -r -a args <<< "$(build_xenium_poller_args)"
  log "${YELLOW}Running: ./bin/start_pollers_xenium.sh $(join_args "${args[@]}")${NC}"
  ./bin/start_pollers_xenium.sh "${args[@]}"
}

main() {
  mkdir -p logs
  LOG_FILE="logs/init_$(date '+%Y%m%d_%H%M%S')_$$.log"

  parse_args "$@"
  validate_args

  echo -e "${BLUE}${BOLD}"
  echo "╔═══════════════════════════════════════════════════════════╗"
  echo "║   Bioloop Legacy Migration Orchestrator                   ║"
  echo "╚═══════════════════════════════════════════════════════════╝"
  echo -e "${NC}"

  log "${DIM}Log file: ${LOG_FILE}${NC}"
  log ""
  log "${BOLD}Resolved Actions:${NC}"
  if [[ "$RUN_CMG_BIGBANG" == true ]]; then
    read -r -a cmg_bigbang_args <<< "$(build_cmg_bigbang_args)"
    log "  ● CMG bigbang  ./bin/bigbang_cmg.sh $(join_args "${cmg_bigbang_args[@]}")"
  fi
  if [[ "$RUN_XENIUM_BIGBANG" == true ]]; then
    read -r -a xenium_bigbang_args <<< "$(build_xenium_bigbang_args)"
    log "  ● Xenium bigbang  ./bin/bigbang_xenium.sh $(join_args "${xenium_bigbang_args[@]}")"
  fi
  if [[ "$RUN_CMG_POLLERS" == true ]]; then
    read -r -a cmg_poller_args <<< "$(build_cmg_poller_args)"
    log "  ● CMG pollers  ./bin/start_pollers_cmg.sh $(join_args "${cmg_poller_args[@]}")"
  fi
  if [[ "$RUN_XENIUM_POLLERS" == true ]]; then
    read -r -a xenium_poller_args <<< "$(build_xenium_poller_args)"
    log "  ● Xenium pollers  ./bin/start_pollers_xenium.sh $(join_args "${xenium_poller_args[@]}")"
  fi
  log ""

  if [[ "$DRY_RUN" == true ]]; then
    log "${YELLOW}[DRY-RUN] No commands executed.${NC}"
    log "${GREEN}${BOLD}✓ init.sh dry-run completed${NC}"
    exit 0
  fi

  # Run bigbang actions first, then pollers.
  if [[ "$RUN_CMG_BIGBANG" == true ]]; then run_cmg_bigbang; fi
  if [[ "$RUN_XENIUM_BIGBANG" == true ]]; then run_xenium_bigbang; fi

  if [[ "$RUN_CMG_POLLERS" == true && "$RUN_XENIUM_POLLERS" == true ]]; then
    log "${YELLOW}Starting both poller processes in background (Ctrl+C to stop both)...${NC}"
    run_cmg_pollers &
    CMG_PID=$!
    run_xenium_pollers &
    XENIUM_PID=$!
    trap "kill $CMG_PID $XENIUM_PID 2>/dev/null" SIGINT SIGTERM
    wait $CMG_PID
    wait $XENIUM_PID
  else
    if [[ "$RUN_CMG_POLLERS" == true ]]; then run_cmg_pollers; fi
    if [[ "$RUN_XENIUM_POLLERS" == true ]]; then run_xenium_pollers; fi
  fi

  log ""
  log "${GREEN}${BOLD}✓ init.sh completed${NC}"
}

main "$@"
