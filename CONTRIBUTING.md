# Contributing to Keldra

## Prerequisites

- Rust 1.96.0 and Cargo
- A C/C++ build toolchain, CMake, Clang/libclang, `pkg-config`, and the
  Protobuf compiler and development headers
- Docker with Buildx for image builds and smoke tests; QEMU is required when
  building for a non-native architecture

## Workspace

All workspace packages currently share version `0.15.0`:

- server, CLI, and Rust client: `keldra-server`, `keldra-cli`, and `keldra`;
- core crates: `keldra-api`, `keldra-authz`, `keldra-atomic-program`,
  `keldra-consensus`, `keldra-index`, and `keldra-store`;
- qualification tooling: `keldra-osv-qualification`.

Keldra 0.15.0 runs as one flat cluster of capacity-weighted nodes with native
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

### Upgrade and activate capability 2/2

Keldra 0.15 changes the cluster and data-peer protocols. A 0.14 cluster must be
upgraded offline; mixed 0.14/0.15 operation is unsupported.

1. Quiesce mutations, atomic invocations, and membership changes, then take a
   consistent backup of every node's data and operational keys.
2. Stop every 0.14 node. Install 0.15 on every node before restarting the
   cluster.
3. Keep writes drained while ACTIVE nodes attest support. Inspect cluster
   capabilities and require active `1/1`, target `2/2`, no blocking ACTIVE node
   IDs, `activation_quiescent=true`, and
   `ready_for_target_activation=true`.
4. Activate protocol/storage `2/2` using the exact placement term and index from
   that status response. If placement changes, discard the old fence and inspect
   again.
5. Re-read status and require active `2/2`. Smoke clone independence, link
   write-through, target-delete fencing, unlink, and date queries before
   resuming traffic.

Use the authenticated CLI surface; the status command prints the exact safe
activation command when the cluster is ready:

```sh
keldra --endpoint "$KELDRA_ENDPOINT" get-cluster-capabilities
keldra --endpoint "$KELDRA_ENDPOINT" activate-cluster-capabilities \
  --protocol-version 2 --storage-format 2 \
  --expected-placement-term "$PLACEMENT_TERM" \
  --expected-placement-index "$PLACEMENT_INDEX"
```

Never force activation past a blocker. Rollback requires restoring the complete
pre-upgrade backup; do not start 0.14 against storage touched by 0.15.

The release tag must be the exact, unprefixed workspace version. After the
validated commit is pushed, maintainers publish `0.15.0` with:

```sh
validated_commit="$(git rev-parse HEAD)"
git tag 0.15.0 "$validated_commit"
git push origin refs/tags/0.15.0
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
cargo info keldra-api@0.15.0

cargo publish --locked -p keldra
cargo info keldra@0.15.0
```

Do not publish `keldra` until `cargo info keldra-api@0.15.0` resolves from
crates.io. After both commands succeed, run both `cargo info` checks again and
confirm that each reports version `0.15.0` from crates.io.

Use Cargo's shared target directory and locking. Do not create ad-hoc target
directories unless the task explicitly requires isolation.
