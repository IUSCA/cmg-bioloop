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
#   --cmg-start-pollers
#   --cmg-stop-pollers
#   --cmg-restart-pollers
#   --xenium-run-bigbang
#   --xenium-start-pollers
#   --xenium-stop-pollers
#   --xenium-restart-pollers
#
# CMG Options:
#   --cmg-clear-locks                Pass --clear-locks to CMG script(s)
#   --cmg-skip-sessions              Pass-through to CMG bigbang
#   --cmg-skip-conversion-logs       Pass-through to CMG bigbang
#
# Xenium Options:
#   --xenium-clear-locks             Pass --clear-locks to Xenium script(s)
#
# General:
#   --clear-target-db                Clear all CMG + Xenium migration rows before any selected bigbang(s)
#   --target-db DB                   Shared target DB for all actions: sandbox (default), app, or custom
#   --dry-run                        Print resolved commands; do not execute
#   -h, --help                       Show this help message
#
# Examples:
#   ./bin/init.sh --cmg-run-bigbang
#   ./bin/init.sh --cmg-run-bigbang --xenium-run-bigbang
#   ./bin/init.sh --cmg-run-bigbang --xenium-run-bigbang --target-db app
#   ./bin/init.sh --cmg-start-pollers
#   ./bin/init.sh --cmg-restart-pollers --xenium-stop-pollers
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
START_CMG_POLLERS=false
STOP_CMG_POLLERS=false
RESTART_CMG_POLLERS=false
RUN_XENIUM_BIGBANG=false
START_XENIUM_POLLERS=false
STOP_XENIUM_POLLERS=false
RESTART_XENIUM_POLLERS=false

TARGET_DB="sandbox"
CMG_CLEAR_LOCKS=false
XENIUM_CLEAR_LOCKS=false
CLEAR_TARGET_DB=false
DRY_RUN=false

CMG_EXTRA_BIGBANG_ARGS=()

show_help() {
  grep '^# ' "$SCRIPT_PATH" | sed 's/^# \?//' | sed 's/^##$//'
  exit 0
}

log() {
  echo -e "$1"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $(echo "$1" | sed 's/\x1B\[[0-9;]*m//g')" >> "$LOG_FILE"
}

resolve_compose_file() {
  local app_env="${APP_ENV:-}"
  local node_env="${NODE_ENV:-}"

  if [[ -z "$app_env" || -z "$node_env" ]] && [[ -f ".env" ]]; then
    local env_app env_node
    env_app="$(sed -n 's/^APP_ENV=//p' .env | head -n 1 | tr -d '"' | tr -d "'")"
    env_node="$(sed -n 's/^NODE_ENV=//p' .env | head -n 1 | tr -d '"' | tr -d "'")"
    if [[ -z "$app_env" ]]; then app_env="$env_app"; fi
    if [[ -z "$node_env" ]]; then node_env="$env_node"; fi
  fi

  if [[ "$app_env" == "production" || "$node_env" == "production" ]]; then
    echo "docker-compose.prod.yml"
  else
    echo "docker-compose.localhost.yml"
  fi
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
      --cmg-start-pollers) START_CMG_POLLERS=true ;;
      --cmg-stop-pollers) STOP_CMG_POLLERS=true ;;
      --cmg-restart-pollers) RESTART_CMG_POLLERS=true ;;
      --xenium-run-bigbang) RUN_XENIUM_BIGBANG=true ;;
      --xenium-start-pollers) START_XENIUM_POLLERS=true ;;
      --xenium-stop-pollers) STOP_XENIUM_POLLERS=true ;;
      --xenium-restart-pollers) RESTART_XENIUM_POLLERS=true ;;

      --target-db=*) TARGET_DB="${1#*=}" ;;
      --target-db) TARGET_DB="$2" ; shift ;;

      --cmg-clear-locks) CMG_CLEAR_LOCKS=true ;;
      --xenium-clear-locks) XENIUM_CLEAR_LOCKS=true ;;
      --clear-target-db) CLEAR_TARGET_DB=true ;;

      --cmg-skip-sessions) CMG_EXTRA_BIGBANG_ARGS+=("--skip-sessions") ;;
      --cmg-skip-conversion-logs) CMG_EXTRA_BIGBANG_ARGS+=("--skip-conversion-logs") ;;
      --cmg-run-pollers|--xenium-run-pollers)
        echo -e "${RED}Removed flag: $1${NC}"
        echo -e "${RED}Use explicit lifecycle flags: --*-start-pollers / --*-stop-pollers / --*-restart-pollers${NC}"
        exit 1
        ;;

      --cmg-target-db=*|--cmg-target-db|--xenium-target-db=*|--xenium-target-db|--clear-locks|--clear-cmg-db|--clear-xenium-db|--cmg|--xenium|--all|--bigbang|--pollers|--both)
        echo -e "${RED}Deprecated/ambiguous flag: $1${NC}"
        echo -e "${RED}Use explicit per-app action flags and shared --target-db (run with --help).${NC}"
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
  local args=("--target-db=${TARGET_DB}")
  if [[ "$CMG_CLEAR_LOCKS" == true ]]; then args+=("--clear-locks"); fi
  if [[ "$CLEAR_TARGET_DB" == true ]]; then args+=("--clear-target-db"); fi
  args+=("${CMG_EXTRA_BIGBANG_ARGS[@]}")
  echo "${args[@]}"
}

build_xenium_bigbang_args() {
  local args=("--target-db=${TARGET_DB}")
  if [[ "$XENIUM_CLEAR_LOCKS" == true ]]; then args+=("--clear-locks"); fi
  if [[ "$CLEAR_TARGET_DB" == true ]]; then args+=("--clear-target-db"); fi
  echo "${args[@]}"
}

build_cmg_poller_args() {
  local args=("--target-db=${TARGET_DB}")
  if [[ "$CMG_CLEAR_LOCKS" == true ]]; then args+=("--clear-locks"); fi
  echo "${args[@]}"
}

build_xenium_poller_args() {
  local args=("--target-db=${TARGET_DB}")
  if [[ "$XENIUM_CLEAR_LOCKS" == true ]]; then args+=("--clear-locks"); fi
  echo "${args[@]}"
}

cmg_pid_file() {
  echo "run/cmg_poller.pid"
}

xenium_pid_file() {
  echo "run/xenium_poller.pid"
}

is_pid_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

stop_pid_set() {
  local source_label="$1"
  shift
  local pids=("$@")
  local attempts=0

  if [[ ${#pids[@]} -eq 0 ]]; then
    return 0
  fi

  log "${YELLOW}Stopping ${source_label} poller process(es): ${pids[*]}${NC}"
  for pid in "${pids[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done

  while [[ $attempts -lt 15 ]]; do
    local any_running=false
    for pid in "${pids[@]}"; do
      if is_pid_running "$pid"; then
        any_running=true
        break
      fi
    done
    if [[ "$any_running" == false ]]; then
      break
    fi
    sleep 1
    attempts=$((attempts + 1))
  done

  local forced=false
  for pid in "${pids[@]}"; do
    if is_pid_running "$pid"; then
      forced=true
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done

  if [[ "$forced" == true ]]; then
    log "${YELLOW}${source_label} poller required SIGKILL after timeout.${NC}"
  else
    log "${GREEN}${source_label} poller stopped gracefully.${NC}"
  fi
}

validate_args() {
  if [[ "$CLEAR_TARGET_DB" == true && "$RUN_CMG_BIGBANG" != true && "$RUN_XENIUM_BIGBANG" != true ]]; then
    echo -e "${RED}Error: --clear-target-db requires --cmg-run-bigbang and/or --xenium-run-bigbang.${NC}"
    exit 1
  fi

  if [[ ${#CMG_EXTRA_BIGBANG_ARGS[@]} -gt 0 && "$RUN_CMG_BIGBANG" != true ]]; then
    echo -e "${RED}Error: CMG bigbang options were provided but --cmg-run-bigbang is not selected.${NC}"
    exit 1
  fi

  if [[ "$RESTART_CMG_POLLERS" == true && "$STOP_CMG_POLLERS" == true ]]; then
    echo -e "${RED}Error: Use either --cmg-stop-pollers or --cmg-restart-pollers, not both.${NC}"
    exit 1
  fi
  if [[ "$RESTART_XENIUM_POLLERS" == true && "$STOP_XENIUM_POLLERS" == true ]]; then
    echo -e "${RED}Error: Use either --xenium-stop-pollers or --xenium-restart-pollers, not both.${NC}"
    exit 1
  fi

  if [[ "$RUN_CMG_BIGBANG" != true && "$START_CMG_POLLERS" != true && "$STOP_CMG_POLLERS" != true && "$RESTART_CMG_POLLERS" != true && "$RUN_XENIUM_BIGBANG" != true && "$START_XENIUM_POLLERS" != true && "$STOP_XENIUM_POLLERS" != true && "$RESTART_XENIUM_POLLERS" != true ]]; then
    echo -e "${RED}Error: select at least one action flag (e.g., --cmg-run-bigbang).${NC}"
    echo "Run with --help for usage."
    exit 1
  fi

  if [[ "$RUN_CMG_BIGBANG" == true || "$START_CMG_POLLERS" == true || "$RESTART_CMG_POLLERS" == true ]]; then
    if [[ -z "${MONGO_URI:-}" && -z "${CMG_MONGO_HOST:-}" ]]; then
      echo -e "${YELLOW}Warning: CMG MongoDB connection env looks unset (no MONGO_URI or CMG_MONGO_HOST).${NC}"
    fi
  fi
  if [[ "$RUN_XENIUM_BIGBANG" == true || "$START_XENIUM_POLLERS" == true || "$RESTART_XENIUM_POLLERS" == true ]]; then
    if [[ -z "${XENIUM_DATABASE_URL:-}" && -z "${XENIUM_PG_HOST:-}" ]]; then
      echo -e "${YELLOW}Warning: Xenium source connection env looks unset (no XENIUM_DATABASE_URL or XENIUM_PG_HOST).${NC}"
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

start_cmg_pollers_managed() {
  header "CMG Pollers Start (managed background mode)"
  mkdir -p run logs
  local pid_file
  pid_file="$(cmg_pid_file)"
  local existing_pid=""
  if [[ -f "$pid_file" ]]; then
    existing_pid="$(<"$pid_file")"
    if is_pid_running "$existing_pid"; then
      echo -e "${RED}Error: CMG poller already running with PID ${existing_pid} (pid file: ${pid_file}).${NC}"
      exit 1
    fi
    rm -f "$pid_file"
  fi

  read -r -a args <<< "$(build_cmg_poller_args)"
  local poller_log="logs/cmg_poller_${TARGET_DB}_$(date '+%Y%m%d_%H%M%S').log"
  log "${YELLOW}Running (background): node src/poller_cmg_sync.js $(join_args "${args[@]}")${NC}"
  log "${DIM}Output log: ${poller_log}${NC}"
  nohup node src/poller_cmg_sync.js "${args[@]}" >> "$poller_log" 2>&1 &
  local pid=$!
  echo "$pid" > "$pid_file"
  sleep 1
  if ! is_pid_running "$pid"; then
    rm -f "$pid_file"
    echo -e "${RED}Error: CMG poller failed to stay running. Check ${poller_log}.${NC}"
    exit 1
  fi
  log "${GREEN}CMG poller started (PID ${pid}).${NC}"
}

stop_cmg_pollers_managed() {
  header "CMG Pollers Stop"
  mkdir -p run
  local pid_file
  pid_file="$(cmg_pid_file)"
  local pids=()

  if [[ -f "$pid_file" ]]; then
    local pid_from_file
    pid_from_file="$(<"$pid_file")"
    if is_pid_running "$pid_from_file"; then
      pids+=("$pid_from_file")
    fi
  fi

  if [[ ${#pids[@]} -eq 0 ]]; then
    mapfile -t pids < <(pgrep -f "poller_cmg_sync\\.js" || true)
  fi

  if [[ ${#pids[@]} -eq 0 ]]; then
    log "${YELLOW}No CMG poller process found.${NC}"
    rm -f "$pid_file"
    return 0
  fi

  stop_pid_set "CMG" "${pids[@]}"
  rm -f "$pid_file"
}

start_xenium_pollers_managed() {
  header "Xenium Pollers Start (managed background mode)"
  mkdir -p run logs
  local pid_file
  pid_file="$(xenium_pid_file)"
  local existing_pid=""
  if [[ -f "$pid_file" ]]; then
    existing_pid="$(<"$pid_file")"
    if is_pid_running "$existing_pid"; then
      echo -e "${RED}Error: Xenium poller already running with PID ${existing_pid} (pid file: ${pid_file}).${NC}"
      exit 1
    fi
    rm -f "$pid_file"
  fi

  read -r -a args <<< "$(build_xenium_poller_args)"
  local poller_log="logs/xenium_poller_${TARGET_DB}_$(date '+%Y%m%d_%H%M%S').log"
  log "${YELLOW}Running (background): node src/poller_xenium_sync.js $(join_args "${args[@]}")${NC}"
  log "${DIM}Output log: ${poller_log}${NC}"
  nohup node src/poller_xenium_sync.js "${args[@]}" >> "$poller_log" 2>&1 &
  local pid=$!
  echo "$pid" > "$pid_file"
  sleep 1
  if ! is_pid_running "$pid"; then
    rm -f "$pid_file"
    echo -e "${RED}Error: Xenium poller failed to stay running. Check ${poller_log}.${NC}"
    exit 1
  fi
  log "${GREEN}Xenium poller started (PID ${pid}).${NC}"
}

stop_xenium_pollers_managed() {
  header "Xenium Pollers Stop"
  mkdir -p run
  local pid_file
  pid_file="$(xenium_pid_file)"
  local pids=()

  if [[ -f "$pid_file" ]]; then
    local pid_from_file
    pid_from_file="$(<"$pid_file")"
    if is_pid_running "$pid_from_file"; then
      pids+=("$pid_from_file")
    fi
  fi

  if [[ ${#pids[@]} -eq 0 ]]; then
    mapfile -t pids < <(pgrep -f "poller_xenium_sync\\.js" || true)
  fi

  if [[ ${#pids[@]} -eq 0 ]]; then
    log "${YELLOW}No Xenium poller process found.${NC}"
    rm -f "$pid_file"
    return 0
  fi

  stop_pid_set "Xenium" "${pids[@]}"
  rm -f "$pid_file"
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
  log "Shared target DB: ${TARGET_DB}"
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
  if [[ "$START_CMG_POLLERS" == true ]]; then
    read -r -a cmg_start_args <<< "$(build_cmg_poller_args)"
    log "  ● CMG pollers start (managed)  node src/poller_cmg_sync.js $(join_args "${cmg_start_args[@]}")"
  fi
  if [[ "$STOP_CMG_POLLERS" == true ]]; then
    log "  ● CMG pollers stop (managed)"
  fi
  if [[ "$RESTART_CMG_POLLERS" == true ]]; then
    read -r -a cmg_restart_args <<< "$(build_cmg_poller_args)"
    log "  ● CMG pollers restart (managed)  node src/poller_cmg_sync.js $(join_args "${cmg_restart_args[@]}")"
  fi
  if [[ "$START_XENIUM_POLLERS" == true ]]; then
    read -r -a xenium_start_args <<< "$(build_xenium_poller_args)"
    log "  ● Xenium pollers start (managed)  node src/poller_xenium_sync.js $(join_args "${xenium_start_args[@]}")"
  fi
  if [[ "$STOP_XENIUM_POLLERS" == true ]]; then
    log "  ● Xenium pollers stop (managed)"
  fi
  if [[ "$RESTART_XENIUM_POLLERS" == true ]]; then
    read -r -a xenium_restart_args <<< "$(build_xenium_poller_args)"
    log "  ● Xenium pollers restart (managed)  node src/poller_xenium_sync.js $(join_args "${xenium_restart_args[@]}")"
  fi
  log ""

  if [[ "$DRY_RUN" == true ]]; then
    log "${YELLOW}[DRY-RUN] No commands executed.${NC}"
    log "${GREEN}${BOLD}✓ init.sh dry-run completed${NC}"
    exit 0
  fi

  # Stop/restart actions happen before bigbang.
  if [[ "$STOP_CMG_POLLERS" == true || "$RESTART_CMG_POLLERS" == true ]]; then stop_cmg_pollers_managed; fi
  if [[ "$STOP_XENIUM_POLLERS" == true || "$RESTART_XENIUM_POLLERS" == true ]]; then stop_xenium_pollers_managed; fi

  # Run bigbang actions first, then pollers.
  if [[ "$RUN_CMG_BIGBANG" == true ]]; then run_cmg_bigbang; fi
  if [[ "$RUN_XENIUM_BIGBANG" == true ]]; then run_xenium_bigbang; fi

  # Managed background starts/restarts.
  if [[ "$START_CMG_POLLERS" == true || "$RESTART_CMG_POLLERS" == true ]]; then start_cmg_pollers_managed; fi
  if [[ "$START_XENIUM_POLLERS" == true || "$RESTART_XENIUM_POLLERS" == true ]]; then start_xenium_pollers_managed; fi

  log ""
  log "${GREEN}${BOLD}✓ init.sh completed${NC}"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
