#!/bin/bash

# Script to manage the secure_download API service in production docker-compose
# Usage: ./deploy_dev.sh [-d] [-u] [-l] [-h]

set -e

COMPOSE_FILE="docker-compose-prod.yml"
SERVICE="api"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default behavior
DO_DOWN=false
DO_UP=false
SHOW_LOGS=false

# Parse command line arguments
while getopts "dulh" opt; do
  case ${opt} in
    d )
      DO_DOWN=true
      ;;
    u )
      DO_UP=true
      ;;
    l )
      SHOW_LOGS=true
      ;;
    h )
      echo "Usage: $0 [-d] [-u] [-l] [-h]"
      echo ""
      echo "Options:"
      echo "  -d    Only bring down the service"
      echo "  -u    Only bring up the service"
      echo "  -l    Show logs after operation"
      echo "  -h    Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0          # Restart (down + up)"
      echo "  $0 -l       # Restart and show logs"
      echo "  $0 -d       # Only bring down"
      echo "  $0 -u       # Only bring up"
      echo "  $0 -u -l    # Bring up and show logs"
      exit 0
      ;;
    \? )
      echo "Invalid option: -$OPTARG" 1>&2
      echo "Use -h for help"
      exit 1
      ;;
  esac
done

# If no flags provided, do both down and up (restart)
if [ "$DO_DOWN" = false ] && [ "$DO_UP" = false ]; then
  DO_DOWN=true
  DO_UP=true
fi

# Execute commands
if [ "$DO_DOWN" = true ]; then
  echo -e "${YELLOW}Bringing down ${SERVICE}...${NC}"
  sudo docker compose -f "$COMPOSE_FILE" down "$SERVICE"
  echo -e "${GREEN}✓ Service stopped${NC}"
fi

if [ "$DO_UP" = true ]; then
  echo -e "${YELLOW}Bringing up ${SERVICE}...${NC}"
  sudo docker compose -f "$COMPOSE_FILE" up "$SERVICE" -d
  echo -e "${GREEN}✓ Service started${NC}"
fi

if [ "$SHOW_LOGS" = true ]; then
  echo -e "${YELLOW}Showing logs (Ctrl+C to exit)...${NC}"
  echo ""
  sudo docker compose -f "$COMPOSE_FILE" logs -tf "$SERVICE"
fi

