#!/bin/bash
# Test runner script for RDM Pytest framework

set -e  # Exit on error

echo "========================================"
echo "RDM Party Role Testing Framework"
echo "========================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed"
    exit 1
fi

print_status "Python 3 found: $(python3 --version)"

# Check if pytest is installed
if ! python3 -c "import pytest" &> /dev/null; then
    print_warning "pytest not found. Installing dependencies..."
    pip install -r requirements-test.txt
fi

print_status "pytest found: $(pytest --version)"

# Parse command line arguments
TEST_TYPE="${1:-all}"
VERBOSE="${2:-false}"

echo ""
echo "Test Type: $TEST_TYPE"
echo ""

# Run tests based on type
case $TEST_TYPE in
    "unit")
        print_status "Running unit tests..."
        if [ "$VERBOSE" = "true" ]; then
            pytest unit/ -v --tb=short
        else
            pytest unit/ --tb=short
        fi
        ;;
    
    "integration")
        print_status "Running integration tests..."
        if [ "$VERBOSE" = "true" ]; then
            pytest integration/ -v --tb=short
        else
            pytest integration/ --tb=short
        fi
        ;;
    
    "coverage")
        print_status "Running tests with coverage..."
        pytest --cov=../ --cov-report=html --cov-report=term-missing
        print_status "Coverage report generated at: coverage_report/index.html"
        ;;
    
    "smoke")
        print_status "Running smoke tests..."
        pytest -m smoke -v --tb=short
        ;;
    
    "quick")
        print_status "Running quick tests (unit only, no coverage)..."
        pytest unit/ --tb=line -q
        ;;
    
    "all")
        print_status "Running all tests..."
        if [ "$VERBOSE" = "true" ]; then
            pytest -v --tb=short
        else
            pytest --tb=short
        fi
        ;;
    
    "ci")
        print_status "Running CI pipeline tests..."
        pytest -v --junitxml=test-results.xml --cov=../ --cov-report=xml --cov-report=term-missing --cov-fail-under=80
        ;;
    
    "help")
        echo "Usage: ./run_tests.sh [TEST_TYPE] [VERBOSE]"
        echo ""
        echo "TEST_TYPE options:"
        echo "  all          - Run all tests (default)"
        echo "  unit         - Run unit tests only"
        echo "  integration  - Run integration tests only"
        echo "  coverage     - Run tests with coverage report"
        echo "  smoke        - Run smoke tests only"
        echo "  quick        - Run quick unit tests"
        echo "  ci           - Run CI pipeline tests (with coverage threshold)"
        echo "  help         - Show this help message"
        echo ""
        echo "VERBOSE options:"
        echo "  true         - Verbose output"
        echo "  false        - Normal output (default)"
        echo ""
        echo "Examples:"
        echo "  ./run_tests.sh unit"
        echo "  ./run_tests.sh integration true"
        echo "  ./run_tests.sh coverage"
        exit 0
        ;;
    
    *)
        print_error "Invalid test type: $TEST_TYPE"
        echo "Run './run_tests.sh help' for usage information"
        exit 1
        ;;
esac

echo ""
if [ $? -eq 0 ]; then
    print_status "All tests passed!"
    exit 0
else
    print_error "Some tests failed!"
    exit 1
fi