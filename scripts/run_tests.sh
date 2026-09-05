#!/bin/bash
# Run all tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"

# Build if needed
if [ ! -d "$BUILD_DIR" ]; then
    echo "Building project..."
    cmake -B "$BUILD_DIR" -S "$PROJECT_DIR" -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON
    cmake --build "$BUILD_DIR" --parallel
fi

echo "Running unit tests..."
cd "$BUILD_DIR"

# Run ctest if GTest is available
if ctest --output-on-failure; then
    echo ""
    echo "All tests passed!"
else
    echo ""
    echo "Some tests failed."
    exit 1
fi

# Run integration tests if requested
if [ "$1" = "--integration" ]; then
    echo ""
    echo "Running integration tests..."
    ./tests/test_storage_engine
fi

# Run benchmarks if requested
if [ "$1" = "--bench" ]; then
    echo ""
    echo "Running benchmarks..."
    ./tests/bench_throughput --ops 50000 --threads 4
fi
