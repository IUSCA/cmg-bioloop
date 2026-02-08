#!/bin/bash
# Helper script to run upload verification tests

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}====================================================================${NC}"
echo -e "${BLUE}Upload Verification Integration Tests${NC}"
echo -e "${BLUE}====================================================================${NC}"
echo ""

# Check if running in Docker
if [ -d "/opt/sca/app" ]; then
    echo -e "${GREEN}✓ Running in Docker container${NC}"
    WORK_DIR="/opt/sca/app"
else
    echo -e "${YELLOW}⚠ Not in Docker, using current directory${NC}"
    WORK_DIR="$(dirname "$(dirname "$(dirname "$(realpath "$0")")")")"
fi

cd "$WORK_DIR"

# Check if poetry is available
if ! command -v poetry &> /dev/null; then
    echo -e "${RED}✗ Poetry not found${NC}"
    echo "  Install: curl -sSL https://install.python-poetry.org | python3 -"
    exit 1
fi

echo -e "${GREEN}✓ Poetry found${NC}"

# Parse arguments
TEST_ARGS=()
RUN_SLOW=false
RUN_ALL=false

for arg in "$@"; do
    case $arg in
        --all)
            RUN_ALL=true
            ;;
        --slow)
            RUN_SLOW=true
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS] [PYTEST_ARGS]"
            echo ""
            echo "Options:"
            echo "  --all       Run all tests (including slow)"
            echo "  --slow      Run only slow tests"
            echo "  --help      Show this help"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Run all fast tests"
            echo "  $0 --all                              # Run all tests"
            echo "  $0 --slow                             # Run only slow tests"
            echo "  $0 test_happy_path.py                 # Run specific file"
            echo "  $0 test_happy_path.py::test_name      # Run specific test"
            exit 0
            ;;
        *)
            TEST_ARGS+=("$arg")
            ;;
    esac
done

# Build pytest command
PYTEST_CMD="poetry run pytest tests/upload_verification/"

if [ "$RUN_ALL" = true ]; then
    echo -e "${YELLOW}Running all tests (including slow)${NC}"
elif [ "$RUN_SLOW" = true ]; then
    echo -e "${YELLOW}Running only slow tests${NC}"
    PYTEST_CMD="$PYTEST_CMD -m slow"
else
    echo -e "${YELLOW}Running fast tests (excluding slow)${NC}"
    PYTEST_CMD="$PYTEST_CMD -m 'not slow'"
fi

# Add user arguments
if [ ${#TEST_ARGS[@]} -gt 0 ]; then
    PYTEST_CMD="$PYTEST_CMD ${TEST_ARGS[@]}"
fi

# Add verbose flag
PYTEST_CMD="$PYTEST_CMD -v"

echo ""
echo -e "${BLUE}Command: $PYTEST_CMD${NC}"
echo ""

# Run tests
if $PYTEST_CMD; then
    echo ""
    echo -e "${GREEN}====================================================================${NC}"
    echo -e "${GREEN}✓ All tests passed!${NC}"
    echo -e "${GREEN}====================================================================${NC}"
    
    # Show log location
    LATEST_LOG=$(ls -t test_logs/test_run_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_LOG" ]; then
        echo -e "${BLUE}Test logs: $LATEST_LOG${NC}"
    fi
else
    echo ""
    echo -e "${RED}====================================================================${NC}"
    echo -e "${RED}✗ Tests failed${NC}"
    echo -e "${RED}====================================================================${NC}"
    
    # Show log location
    LATEST_LOG=$(ls -t test_logs/test_run_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_LOG" ]; then
        echo -e "${BLUE}Test logs: $LATEST_LOG${NC}"
        echo -e "${YELLOW}Check logs for details:${NC}"
        echo "  tail -f $LATEST_LOG"
    fi
    
    exit 1
fi
