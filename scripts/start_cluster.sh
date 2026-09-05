#!/bin/bash
# Start a local 3-node cluster for development

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"

# Ensure build exists
if [ ! -f "$BUILD_DIR/shard-kv" ]; then
    echo "Build not found. Building..."
    cmake -B "$BUILD_DIR" -S "$PROJECT_DIR" -DCMAKE_BUILD_TYPE=Debug
    cmake --build "$BUILD_DIR" --parallel
fi

# Create data directories
mkdir -p /tmp/shard/node1/{data,wal}
mkdir -p /tmp/shard/node2/{data,wal}
mkdir -p /tmp/shard/node3/{data,wal}

# Function to cleanup on exit
cleanup() {
    echo "Stopping nodes..."
    kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
}
trap cleanup EXIT

# Start nodes
echo "Starting node 1..."
SHARD_NODE_ID=1 \
SHARD_LISTEN_ADDR=127.0.0.1:7071 \
SHARD_PEERS=127.0.0.1:7072,127.0.0.1:7073 \
SHARD_DATA_DIR=/tmp/shard/node1/data \
SHARD_WAL_DIR=/tmp/shard/node1/wal \
SHARD_LOG_LEVEL=INFO \
"$BUILD_DIR/shard-kv" &
NODE1_PID=$!

sleep 1

echo "Starting node 2..."
SHARD_NODE_ID=2 \
SHARD_LISTEN_ADDR=127.0.0.1:7072 \
SHARD_PEERS=127.0.0.1:7071,127.0.0.1:7073 \
SHARD_DATA_DIR=/tmp/shard/node2/data \
SHARD_WAL_DIR=/tmp/shard/node2/wal \
SHARD_LOG_LEVEL=INFO \
"$BUILD_DIR/shard-kv" &
NODE2_PID=$!

sleep 1

echo "Starting node 3..."
SHARD_NODE_ID=3 \
SHARD_LISTEN_ADDR=127.0.0.1:7073 \
SHARD_PEERS=127.0.0.1:7071,127.0.0.1:7072 \
SHARD_DATA_DIR=/tmp/shard/node3/data \
SHARD_WAL_DIR=/tmp/shard/node3/wal \
SHARD_LOG_LEVEL=INFO \
"$BUILD_DIR/shard-kv" &
NODE3_PID=$!

echo ""
echo "Cluster started!"
echo "  Node 1: 127.0.0.1:7071"
echo "  Node 2: 127.0.0.1:7072"
echo "  Node 3: 127.0.0.1:7073"
echo ""
echo "Press Ctrl+C to stop all nodes"

wait
