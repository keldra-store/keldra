# Contributing to Anvil

## Prerequisites

- Rust 1.96.0 and Cargo
- A C/C++ build toolchain, CMake, Clang/libclang, `pkg-config`, and the
  Protobuf compiler and development headers
- Docker with Buildx for image builds and smoke tests; QEMU is required when
  building for a non-native architecture

## Workspace

All workspace packages currently share version `0.8.0`:

- server, CLI, and Rust client: `anvil-server`, `anvil-storage-cli`, and
  `anvil-storage`;
- core crates: `anvil-api`, `anvil-authz`, `anvil-atomic-program`,
  `anvil-consensus`, `anvil-index`, and `anvil-store`;
- qualification tooling: `anvil-osv-qualification`.

Anvil 0.8.0 runs as one flat cluster of capacity-weighted nodes with native
on-disk state, cluster-managed mTLS between peers, cluster-wide derived
streaming indexes, and no external metadata database, external PKI, or second
storage system. It includes PersonalDB, accounting, S3 and Git gateways, and
online growth from one node to the configured erasure width. Native gRPC, S3,
Git, and administrative APIs share one authorized public listener.

## Local Validation

Run the same static and Rust gates used by CI:

```sh
./scripts/release-gates.sh static
./scripts/release-gates.sh rust
# Equivalent combined invocation:
./scripts/release-gates.sh all
```

Rust source files have an absolute 2,000-line limit. Split larger units into
semantic modules; the static release gate enforces this policy.

For a focused server, client, and CLI test run:

```sh
./scripts/release-gates.sh server
```

Build and smoke-test a native-architecture image:

```sh
ANVIL_IMAGE=anvil:test ./scripts/build-image.sh
ANVIL_IMAGE=anvil:test ./scripts/release-gates.sh image
```

Before a release, repeat the image build and smoke test with
`ANVIL_DOCKER_PLATFORM=linux/amd64` and `linux/arm64`, using a distinct local
`ANVIL_IMAGE` tag for each architecture.

Inspect and verify the two publishable crate archives locally before tagging:

```sh
cargo package --locked -p anvil-api --list
cargo package --locked -p anvil-storage --list
cargo package --locked -p anvil-api
cargo package --locked -p anvil-storage
```

## Release

The release tag must be the exact, unprefixed workspace version. After the
validated commit is pushed, maintainers publish `0.8.0` with:

```sh
validated_commit="$(git rev-parse HEAD)"
git tag 0.8.0 "$validated_commit"
git push origin refs/tags/0.8.0
```

The tag-triggered workflow reruns the static, Rust, and per-architecture image
gates, then publishes the single multi-architecture image for the repository
and creates the GitHub release. Do not publish
public architecture-specific or `v`-prefixed image tags.

Publish the crates from the same validated commit. `anvil-storage` depends on
the exact `anvil-api` release, so publish and verify the API crate before the
client crate:

```sh
cargo publish --locked -p anvil-api
cargo info anvil-api@0.8.0

cargo publish --locked -p anvil-storage
cargo info anvil-storage@0.8.0
```

Do not publish `anvil-storage` until `cargo info anvil-api@0.8.0` resolves from
crates.io. After both commands succeed, run both `cargo info` checks again and
confirm that each reports version `0.8.0` from crates.io.

Use Cargo's shared target directory and locking. Do not create ad-hoc target
directories unless the task explicitly requires isolation.
