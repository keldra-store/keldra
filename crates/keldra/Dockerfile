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

WORKDIR /usr/src/keldra

COPY . .

# Build the Keldra server and CLI in release mode. BuildKit cache mounts keep
# Cargo's registry, git checkout cache, and target directory outside the image
# layer so repeat local/CI Docker builds reuse compiled dependencies.
RUN --mount=type=cache,id=keldra-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=keldra-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=keldra-cargo-target-${TARGETARCH}-release,target=/usr/src/keldra/target,sharing=locked \
    cargo build --release --locked --jobs "${CARGO_BUILD_JOBS}" \
        -p keldra-server --bin keldra-server \
        -p keldra-cli --bin keldra \
    && mkdir -p /usr/local/keldra-build \
    && cp \
        target/release/keldra-server \
        target/release/keldra \
        /usr/local/keldra-build/

# Rust tooling is deliberately absent from the matching target-platform
# runtime image.
FROM --platform=$TARGETPLATFORM debian:trixie-slim
ARG KELDRA_SOURCE_REVISION=unknown

LABEL org.opencontainers.image.revision="${KELDRA_SOURCE_REVISION}"
LABEL org.opencontainers.image.title="Keldra"
LABEL org.opencontainers.image.source="https://github.com/keldra-store/keldra"

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 10001 --no-create-home --home-dir /var/lib/keldra --shell /usr/sbin/nologin keldra \
    && install -d -o keldra -g keldra /var/lib/keldra \
    && install -d /usr/share/doc/keldra

# Copy the compiled binaries from the builder stage.
COPY --from=builder /usr/local/keldra-build/keldra-server /usr/local/bin/keldra-server
COPY --from=builder /usr/local/keldra-build/keldra /usr/local/bin/keldra
COPY --from=builder /usr/src/keldra/LICENSE /usr/share/doc/keldra/LICENSE
COPY --from=builder /usr/src/keldra/docs/dependency-licenses.md /usr/share/doc/keldra/dependency-licenses.md

ENV KELDRA_LISTEN=0.0.0.0:50051 \
    KELDRA_DATA_DIR=/var/lib/keldra \
    KELDRA_NODE_ID=1 \
    KELDRA_TOKEN_SIGNING_KEY_FILE=/run/secrets/keldra-token-signing-key \
    KELDRA_MAX_ATOMIC_COMMIT_ENTRIES=4096 \
    KELDRA_MAX_ATOMIC_COMMIT_BYTES=16777216 \
    KELDRA_AWAITING_PUBLISH_TTL_SECONDS=86400 \
    KELDRA_MUTATION_RECEIPT_RETENTION_SECONDS=86400 \
    KELDRA_MAX_MUTATION_RECEIPT_ENTRIES=2000000 \
    KELDRA_MAX_MUTATION_RECEIPT_BYTES=536870912 \
    KELDRA_SOURCE_JOURNAL_MAX_ENTRIES=1000000 \
    KELDRA_SOURCE_JOURNAL_MAX_BYTES=536870912
USER keldra
WORKDIR /var/lib/keldra

# Mount a distinct mode-0600 signing key at the configured path. On the first
# start only, set KELDRA_RUN_SYSTEM_BOOTSTRAP=true and then copy and delete the
# generated /var/lib/keldra/system-bootstrap-credential.json file.
# Public gRPC, S3 and Git share 50051. Cluster-managed mTLS peers use 50052.
EXPOSE 50051 50052

CMD ["keldra-server"]
