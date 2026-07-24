# syntax=docker/dockerfile:1.7

# Stage 1: Build the binaries with the release toolchain used by CI.
FROM rust:1.96-bookworm AS builder

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

# Build the server, public CLI, and admin CLI in release mode. BuildKit cache
# mounts keep Cargo's registry, git checkout cache, and target directory outside
# the image layer so repeat local/CI Docker builds reuse compiled dependencies.
RUN --mount=type=cache,id=anvil-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=anvil-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=anvil-cargo-target-release,target=/usr/src/anvil/target,sharing=locked \
    cargo build --release --locked \
        -p anvil-server --bin anvil-server \
        -p anvil-storage-cli --bin anvil --bin anvil-admin \
    && mkdir -p /usr/local/anvil-build \
    && cp \
        target/release/anvil-server \
        target/release/anvil \
        target/release/anvil-admin \
        /usr/local/anvil-build/

# Stage 2: Create the final runtime image. Rust tooling is deliberately not
# present in the shipped server image.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10001 --create-home --home-dir /var/lib/anvil --shell /usr/sbin/nologin anvil

# Copy the compiled binaries from the builder stage.
COPY --from=builder /usr/local/anvil-build/anvil-server /usr/local/bin/anvil-server
COPY --from=builder /usr/local/anvil-build/anvil /usr/local/bin/anvil
COPY --from=builder /usr/local/anvil-build/anvil-admin /usr/local/bin/anvil-admin

RUN mkdir -p /var/lib/anvil-bootstrap \
    && chown -R anvil:anvil /var/lib/anvil /var/lib/anvil-bootstrap

ENV STORAGE_PATH=/var/lib/anvil
USER anvil
WORKDIR /var/lib/anvil

# Expose the public and administrative gRPC listeners.
EXPOSE 50051
EXPOSE 50052

HEALTHCHECK --interval=30s --timeout=5s --retries=5 CMD curl -fsS http://127.0.0.1:50051/ready || exit 1

# Set the default command to run the anvil server
CMD ["anvil-server"]
