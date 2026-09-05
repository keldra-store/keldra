# Contributing to Keldra

## Prerequisites

- Rust 1.96.0 and Cargo
- A C/C++ build toolchain, CMake, Clang/libclang, `pkg-config`, and the
  Protobuf compiler and development headers
- Docker with Buildx for image builds and smoke tests; QEMU is required when
  building for a non-native architecture

## Workspace

All workspace packages currently share version `0.16.1`:

- server, CLI, and Rust client: `keldra-server`, `keldra-cli`, and `keldra`;
- core crates: `keldra-api`, `keldra-authz`, `keldra-atomic-program`,
  `keldra-consensus`, `keldra-index`, and `keldra-store`;
- qualification tooling: `keldra-osv-qualification`.

Keldra 0.16.1 runs as one flat cluster of capacity-weighted nodes with native
on-disk state, cluster-managed mTLS between peers, cluster-wide derived
streaming indices, and no external metadata database, external PKI, or second
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
KELDRA_IMAGE=keldra:test ./scripts/build-image.sh
KELDRA_IMAGE=keldra:test ./scripts/release-gates.sh image
```

Before a release, repeat the image build and smoke test with
`KELDRA_DOCKER_PLATFORM=linux/amd64` and `linux/arm64`, using a distinct local
`KELDRA_IMAGE` tag for each architecture.

Inspect and verify the two publishable crate archives locally before tagging:

```sh
cargo package --locked -p keldra-api --list
cargo package --locked -p keldra --list
cargo package --locked -p keldra-api
cargo package --locked -p keldra
```

## Release

### Initialize 0.16 at capability 2/2

Keldra 0.16 changes the cluster/data-peer protocols and introduces a clean-break
storage format and index architecture. Every 0.16 node must use fresh
authoritative and derived-index volumes; mixed 0.15/0.16 operation and in-place
upgrades from any earlier Keldra release are unsupported.

1. Initialize a fresh 0.16 cluster. Fresh clusters start with protocol/storage
   capability `2/2`, regardless of node count. If application data must move
   from an older cluster, import it through the public API as new writes.
2. Inspect cluster capabilities and require active and target protocol/storage
   capability `2/2` with no blocking ACTIVE node IDs.
3. Smoke clone independence, link write-through, target-delete fencing, unlink,
   and date queries before admitting production traffic.

Use the authenticated CLI surface to inspect the active capabilities:

```sh
keldra --endpoint "$KELDRA_ENDPOINT" get-cluster-capabilities
```

Never start an earlier Keldra binary against storage initialized or touched by
0.16.

The release tag must be the exact, unprefixed workspace version. After the
validated commit is pushed, maintainers publish `0.16.1` with:

```sh
validated_commit="$(git rev-parse HEAD)"
git tag 0.16.1 "$validated_commit"
git push origin refs/tags/0.16.1
```

The tag-triggered workflow reruns the static, Rust, and per-architecture image
gates, then publishes the single multi-architecture image for the repository
and creates the GitHub release. Do not publish
public architecture-specific or `v`-prefixed image tags.

Publish the crates from the same validated commit. `keldra` depends on
the exact `keldra-api` release, so publish and verify the API crate before the
client crate:

```sh
cargo publish --locked -p keldra-api
cargo info keldra-api@0.16.1

cargo publish --locked -p keldra
cargo info keldra@0.16.1
```

Do not publish `keldra` until `cargo info keldra-api@0.16.1` resolves from
crates.io. After both commands succeed, run both `cargo info` checks again and
confirm that each reports version `0.16.1` from crates.io.

Use Cargo's shared target directory and locking. Do not create ad-hoc target
directories unless the task explicitly requires isolation.
