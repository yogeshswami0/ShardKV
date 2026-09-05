#!/bin/bash
# Generate protobuf and gRPC code manually (for development)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$PROJECT_DIR/proto"
OUT_DIR="$PROJECT_DIR/generated"

mkdir -p "$OUT_DIR"

echo "Generating protobuf and gRPC code..."

for proto_file in "$PROTO_DIR"/*.proto; do
    echo "Processing $(basename "$proto_file")..."
    
    protoc \
        --cpp_out="$OUT_DIR" \
        --grpc_out="$OUT_DIR" \
        --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin) \
        -I"$PROTO_DIR" \
        "$proto_file"
done

echo "Done! Generated files in $OUT_DIR"
