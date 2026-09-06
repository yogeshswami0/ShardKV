#!/bin/bash
set -e

# Base directory for data and WAL logs
mkdir -p /tmp/shard/node1/data /tmp/shard/node1/wal
mkdir -p /tmp/shard/node2/data /tmp/shard/node2/wal
mkdir -p /tmp/shard/node3/data /tmp/shard/node3/wal

# Locate the binary (falls back to PATH or build directory)
BIN_PATH="${BINARY_PATH:-/app/build/shard-kv}"
if [ ! -f "$BIN_PATH" ] && command -v shard-kv >/dev/null 2>&1; then
    BIN_PATH="$(command -v shard-kv)"
fi

cleanup() {
    echo "Stopping nodes and dashboard..."
    kill $NODE1_PID $NODE2_PID $NODE3_PID $DASH_PID 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Start Node 1
echo "Starting Node 1 on 127.0.0.1:7071..."
SHARD_NODE_ID=1 \
SHARD_LISTEN_ADDR=127.0.0.1:7071 \
SHARD_PEERS=127.0.0.1:7072,127.0.0.1:7073 \
SHARD_DATA_DIR=/tmp/shard/node1/data \
SHARD_WAL_DIR=/tmp/shard/node1/wal \
SHARD_LOG_LEVEL=INFO \
"$BIN_PATH" &
NODE1_PID=$!

sleep 1

# Start Node 2
echo "Starting Node 2 on 127.0.0.1:7072..."
SHARD_NODE_ID=2 \
SHARD_LISTEN_ADDR=127.0.0.1:7072 \
SHARD_PEERS=127.0.0.1:7071,127.0.0.1:7073 \
SHARD_DATA_DIR=/tmp/shard/node2/data \
SHARD_WAL_DIR=/tmp/shard/node2/wal \
SHARD_LOG_LEVEL=INFO \
"$BIN_PATH" &
NODE2_PID=$!

sleep 1

# Start Node 3
echo "Starting Node 3 on 127.0.0.1:7073..."
SHARD_NODE_ID=3 \
SHARD_LISTEN_ADDR=127.0.0.1:7073 \
SHARD_PEERS=127.0.0.1:7071,127.0.0.1:7072 \
SHARD_DATA_DIR=/tmp/shard/node3/data \
SHARD_WAL_DIR=/tmp/shard/node3/wal \
SHARD_LOG_LEVEL=INFO \
"$BIN_PATH" &
NODE3_PID=$!

echo "Cluster nodes started. Launching Dashboard on port ${PORT:-8006}..."

# Check if dashboard is a Python app or Node/compiled binary and run it
export FORCE_LOCAL_NODES=1
if [ -f "/app/dashboard/server.py" ] || [ -f "./dashboard/server.py" ]; then
    PORT=${PORT:-8006} python3 dashboard/server.py &
    DASH_PID=$!
else
    # Fallback to direct binary/executable in dashboard folder
    PORT=${PORT:-8006} ./dashboard/dashboard &
    DASH_PID=$!
fi

# Keep container alive and forward process signals
wait $DASH_PID
