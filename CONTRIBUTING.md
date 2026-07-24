# Contributing to Anvil

## Prerequisites

- Rust and Cargo
- Docker and Docker Compose for containerized cluster checks

## Local Development

Anvil uses native on-disk state below `STORAGE_PATH`; no external metadata database is required.

Run the canonical six-peer Docker cluster acceptance test:

```bash
ANVIL_IMAGE=anvil:test ./scripts/release-gates.sh docker-mesh
```

The Docker harness bootstraps the committed CoreMeta lifecycle topology and
uses each peer's authenticated gRPC endpoint. There is no separate discovery or
gossip process to start.

Run focused checks:

```bash
cargo check -p anvil-storage-core
cargo check -p anvil-storage
cargo check -p anvil-storage-test-utils
cargo check -p anvil-storage-storage --tests
```

Run tests with the shared Cargo target directory managed by Cargo locking. Do not create ad-hoc target directories unless the task explicitly requires isolation.
