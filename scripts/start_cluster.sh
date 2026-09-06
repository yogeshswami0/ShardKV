# #!/bin/bash
# # Start a local 3-node cluster for development

# set -e

# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
# BUILD_DIR="$PROJECT_DIR/build"

# # Ensure build exists
# if [ ! -f "$BUILD_DIR/shard-kv" ]; then
#     echo "Build not found. Building..."
#     cmake -B "$BUILD_DIR" -S "$PROJECT_DIR" -DCMAKE_BUILD_TYPE=Debug
#     cmake --build "$BUILD_DIR" --parallel
# fi

# # Create data directories
# mkdir -p /tmp/shard/node1/{data,wal}
# mkdir -p /tmp/shard/node2/{data,wal}
# mkdir -p /tmp/shard/node3/{data,wal}

# # Function to cleanup on exit
# cleanup() {
#     echo "Stopping nodes..."
#     kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null || true
# }
# trap cleanup EXIT

# # Start nodes
# echo "Starting node 1..."
# SHARD_NODE_ID=1 \
# SHARD_LISTEN_ADDR=127.0.0.1:7071 \
# SHARD_PEERS=127.0.0.1:7072,127.0.0.1:7073 \
# SHARD_DATA_DIR=/tmp/shard/node1/data \
# SHARD_WAL_DIR=/tmp/shard/node1/wal \
# SHARD_LOG_LEVEL=INFO \
# "$BUILD_DIR/shard-kv" &
# NODE1_PID=$!

# sleep 1

# echo "Starting node 2..."
# SHARD_NODE_ID=2 \
# SHARD_LISTEN_ADDR=127.0.0.1:7072 \
# SHARD_PEERS=127.0.0.1:7071,127.0.0.1:7073 \
# SHARD_DATA_DIR=/tmp/shard/node2/data \
# SHARD_WAL_DIR=/tmp/shard/node2/wal \
# SHARD_LOG_LEVEL=INFO \
# "$BUILD_DIR/shard-kv" &
# NODE2_PID=$!

# sleep 1

# echo "Starting node 3..."
# SHARD_NODE_ID=3 \
# SHARD_LISTEN_ADDR=127.0.0.1:7073 \
# SHARD_PEERS=127.0.0.1:7071,127.0.0.1:7072 \
# SHARD_DATA_DIR=/tmp/shard/node3/data \
# SHARD_WAL_DIR=/tmp/shard/node3/wal \
# SHARD_LOG_LEVEL=INFO \
# "$BUILD_DIR/shard-kv" &
# NODE3_PID=$!

# echo ""
# echo "Cluster started!"
# echo "  Node 1: 127.0.0.1:7071"
# echo "  Node 2: 127.0.0.1:7072"
# echo "  Node 3: 127.0.0.1:7073"
# echo ""
# echo "Press Ctrl+C to stop all nodes"

# wait








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
