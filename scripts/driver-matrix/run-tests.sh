#!/bin/bash
# Driver Compatibility Matrix Test Runner
# Runs tests for all MongoDB drivers against OxideDB

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OXIDEDB_URL="${OXIDEDB_URL:-mongodb://localhost:27017}"

# Test results
PASSED=0
FAILED=0
TOTAL=0

# Test function
run_test() {
    local driver=$1
    local test_file=$2
    local command=$3

    echo ""
    echo "========================================"
    echo "Testing: $driver"
    echo "========================================"

    if [ ! -f "$test_file" ]; then
        echo -e "${YELLOW}⚠ Skipped: $driver (test file not found)${NC}"
        return 0
    fi

    export OXIDEDB_URL

    if eval "$command"; then
        echo -e "${GREEN}✓ $driver tests passed${NC}"
        ((PASSED++))
    else
        echo -e "${RED}✗ $driver tests failed${NC}"
        ((FAILED++))
    fi
    ((TOTAL++))
}

# Check if Node.js is available and run tests
if command -v node &> /dev/null; then
    # Check if mongodb package is installed
    if node -e "require('mongodb')" 2>/dev/null; then
        run_test "Node.js (mongodb)" "$SCRIPT_DIR/test-nodejs.js" "node $SCRIPT_DIR/test-nodejs.js"
    else
        echo -e "${YELLOW}⚠ Skipped: Node.js (mongodb package not installed)${NC}"
        echo "  Install with: npm install mongodb"
    fi
else
    echo -e "${YELLOW}⚠ Skipped: Node.js (not installed)${NC}"
fi

# Check if Python is available and run tests
if command -v python3 &> /dev/null; then
    # Check if pymongo is installed
    if python3 -c "import pymongo" 2>/dev/null; then
        run_test "Python (pymongo)" "$SCRIPT_DIR/test-python.py" "python3 $SCRIPT_DIR/test-python.py"
    else
        echo -e "${YELLOW}⚠ Skipped: Python (pymongo not installed)${NC}"
        echo "  Install with: pip install pymongo"
    fi
else
    echo -e "${YELLOW}⚠ Skipped: Python (not installed)${NC}"
fi

# Check if Go is available and run tests
if command -v go &> /dev/null; then
    # Check if mongo-driver is available
    if go list go.mongodb.org/mongo-driver/mongo 2>/dev/null; then
        run_test "Go (mongo-driver)" "$SCRIPT_DIR/test-go.go" "cd $SCRIPT_DIR && go run test-go.go"
    else
        echo -e "${YELLOW}⚠ Skipped: Go (mongo-driver not installed)${NC}"
        echo "  Install with: go get go.mongodb.org/mongo-driver/mongo"
    fi
else
    echo -e "${YELLOW}⚠ Skipped: Go (not installed)${NC}"
fi

# Check if Rust is available and run tests
if command -v cargo &> /dev/null; then
    # For Rust, we need a proper Cargo project
    echo -e "${YELLOW}⚠ Skipped: Rust (requires Cargo project setup)${NC}"
    echo "  Manual test: cd scripts/driver-matrix/rust && cargo run"
else
    echo -e "${YELLOW}⚠ Skipped: Rust (not installed)${NC}"
fi

# Summary
echo ""
echo "========================================"
echo "Driver Compatibility Matrix Summary"
echo "========================================"
echo -e "Passed: ${GREEN}$PASSED${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"
echo "Total:  $TOTAL"
echo "========================================"

if [ $FAILED -gt 0 ]; then
    exit 1
fi
