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

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash shard && \
    mkdir -p /var/shard/data /var/shard/wal && \
    chown -R shard:shard /var/shard

WORKDIR /app

# Copy the built binary
COPY --from=builder /app/build/shard-kv /app/build/shard-kv
RUN ln -s /app/build/shard-kv /usr/local/bin/shard-kv

# Copy Python dashboard
COPY dashboard/ dashboard/
RUN pip3 install --no-cache-dir -r dashboard/requirements.txt

# Copy startup script
COPY scripts/start_cluster.sh /app/start_cluster.sh
RUN chmod +x /app/start_cluster.sh

# The dashboard expects SHARD_IN_DOCKER to point to Docker DNS if using compose, 
# but since we are running everything in ONE container via start_cluster.sh, 
# the nodes will literally be on 127.0.0.1 just like local dev!
# We do NOT set SHARD_IN_DOCKER=1 so the dashboard connects to 127.0.0.1.

EXPOSE 8006

CMD ["/app/start_cluster.sh"]
