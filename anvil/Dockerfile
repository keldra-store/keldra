# syntax=docker/dockerfile:1.7

# Stage 1: Build the binaries with the release toolchain used by CI.
FROM rust:1.96-trixie AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    libclang-dev \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/anvil

COPY . .

# Build the 0.5 server and CLI in release mode. BuildKit cache mounts keep
# Cargo's registry, git checkout cache, and target directory outside the image
# layer so repeat local/CI Docker builds reuse compiled dependencies.
RUN --mount=type=cache,id=anvil-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=anvil-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=anvil-cargo-target-release,target=/usr/src/anvil/target,sharing=locked \
    cargo build --release --locked \
        -p anvil-server --bin anvil-server \
        -p anvil-storage-cli --bin anvil \
    && mkdir -p /usr/local/anvil-build \
    && cp \
        target/release/anvil-server \
        target/release/anvil \
        /usr/local/anvil-build/

# Stage 2: Create the final runtime image. Rust tooling is deliberately not
# present in the shipped server image.
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10001 --no-create-home --home-dir /var/lib/anvil --shell /usr/sbin/nologin anvil \
    && install -d -o anvil -g anvil /var/lib/anvil

# Copy the compiled binaries from the builder stage.
COPY --from=builder /usr/local/anvil-build/anvil-server /usr/local/bin/anvil-server
COPY --from=builder /usr/local/anvil-build/anvil /usr/local/bin/anvil

ENV ANVIL_LISTEN=0.0.0.0:50051 \
    ANVIL_DATA_DIR=/var/lib/anvil \
    ANVIL_NODE_ID=1 \
    ANVIL_MAX_ATOMIC_COMMIT_ENTRIES=4096 \
    ANVIL_MAX_ATOMIC_COMMIT_BYTES=16777216
USER anvil
WORKDIR /var/lib/anvil

# The bearer token must be supplied as ANVIL_API_TOKEN when the container starts.
EXPOSE 50051

CMD ["anvil-server"]
