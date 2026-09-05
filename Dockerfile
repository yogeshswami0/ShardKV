# Build stage
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY CMakeLists.txt .
COPY src/ src/

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF -DBUILD_BENCHMARKS=OFF && \
    cmake --build build --parallel $(nproc)

# Runtime stage
FROM ubuntu:22.04

RUN useradd -m -s /bin/bash shard && \
    mkdir -p /var/shard/data /var/shard/wal && \
    chown -R shard:shard /var/shard

COPY --from=builder /app/build/shard-kv /usr/local/bin/

USER shard

ENV SHARD_NODE_ID=1
ENV SHARD_LISTEN_ADDR=0.0.0.0:7070
ENV SHARD_DATA_DIR=/var/shard/data
ENV SHARD_WAL_DIR=/var/shard/wal
ENV SHARD_LOG_LEVEL=INFO

EXPOSE 7070

ENTRYPOINT ["shard-kv"]
