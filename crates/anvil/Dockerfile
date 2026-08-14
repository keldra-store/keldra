# syntax=docker/dockerfile:1.7

# Build inside the requested image platform. An amd64 image is compiled in the
# amd64 trixie builder and an arm64 image is compiled in the arm64 builder.
FROM --platform=$TARGETPLATFORM rust:1.96-trixie AS builder
ARG TARGETARCH
ARG CARGO_BUILD_JOBS=1

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    cmake \
    libclang-dev \
    libprotobuf-dev \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/anvil

COPY . .

# Build the 0.9 server and CLI in release mode. BuildKit cache mounts keep
# Cargo's registry, git checkout cache, and target directory outside the image
# layer so repeat local/CI Docker builds reuse compiled dependencies.
RUN --mount=type=cache,id=anvil-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=anvil-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=anvil-cargo-target-${TARGETARCH}-release,target=/usr/src/anvil/target,sharing=locked \
    cargo build --release --locked --jobs "${CARGO_BUILD_JOBS}" \
        -p anvil-server --bin anvil-server \
        -p anvil-storage-cli --bin anvil \
    && mkdir -p /usr/local/anvil-build \
    && cp \
        target/release/anvil-server \
        target/release/anvil \
        /usr/local/anvil-build/

# Rust tooling is deliberately absent from the matching target-platform
# runtime image.
FROM --platform=$TARGETPLATFORM debian:trixie-slim
ARG ANVIL_SOURCE_REVISION=unknown

LABEL org.opencontainers.image.revision="${ANVIL_SOURCE_REVISION}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10001 --no-create-home --home-dir /var/lib/anvil --shell /usr/sbin/nologin anvil \
    && install -d -o anvil -g anvil /var/lib/anvil \
    && install -d /usr/share/doc/anvil

# Copy the compiled binaries from the builder stage.
COPY --from=builder /usr/local/anvil-build/anvil-server /usr/local/bin/anvil-server
COPY --from=builder /usr/local/anvil-build/anvil /usr/local/bin/anvil
COPY --from=builder /usr/src/anvil/LICENSE /usr/share/doc/anvil/LICENSE
COPY --from=builder /usr/src/anvil/docs/dependency-licenses.md /usr/share/doc/anvil/dependency-licenses.md

ENV ANVIL_LISTEN=0.0.0.0:50051 \
    ANVIL_DATA_DIR=/var/lib/anvil \
    ANVIL_NODE_ID=1 \
    ANVIL_TOKEN_SIGNING_KEY_FILE=/run/secrets/anvil-token-signing-key \
    ANVIL_MAX_ATOMIC_COMMIT_ENTRIES=4096 \
    ANVIL_MAX_ATOMIC_COMMIT_BYTES=16777216 \
    ANVIL_AWAITING_PUBLISH_TTL_SECONDS=86400 \
    ANVIL_MUTATION_RECEIPT_RETENTION_SECONDS=86400 \
    ANVIL_MAX_MUTATION_RECEIPT_ENTRIES=2000000 \
    ANVIL_MAX_MUTATION_RECEIPT_BYTES=536870912 \
    ANVIL_SOURCE_JOURNAL_MAX_ENTRIES=1000000 \
    ANVIL_SOURCE_JOURNAL_MAX_BYTES=536870912
USER anvil
WORKDIR /var/lib/anvil

# Mount a distinct mode-0600 signing key at the configured path. On the first
# start only, set ANVIL_RUN_SYSTEM_BOOTSTRAP=true and then copy and delete the
# generated /var/lib/anvil/system-bootstrap-credential.json file.
# Public gRPC, S3 and Git share 50051. Cluster-managed mTLS peers use 50052.
EXPOSE 50051 50052

CMD ["anvil-server"]
