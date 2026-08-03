# Contributing to Anvil

## Prerequisites

- Rust 1.96.0 and Cargo
- A C/C++ build toolchain, CMake, Clang/libclang, `pkg-config`, and the
  Protobuf compiler and development headers
- Docker with Buildx for image builds and smoke tests; QEMU is required when
  building for a non-native architecture

## Workspace

All workspace packages currently share version `0.5.2`:

- server, CLI, and Rust client: `anvil-server`, `anvil-storage-cli`, and
  `anvil-storage`;
- core crates: `anvil-api`, `anvil-authz`, `anvil-atomic-program`,
  `anvil-consensus`, `anvil-index`, and `anvil-store`;
- qualification tooling: `anvil-osv-qualification`.

Anvil 0.5.2 runs as one flat cluster of capacity-weighted nodes with native
on-disk state, cluster-managed mTLS between peers, cluster-wide derived
indexes, and no external metadata database, external PKI, or second storage
system. PersonalDB is staged as the following 0.5.3 capability release.

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

## Release

The release tag must be the exact, unprefixed workspace version. After the
validated commit is pushed, maintainers publish `0.5.2` with:

```sh
validated_commit="$(git rev-parse HEAD)"
git tag 0.5.2 "$validated_commit"
git push origin refs/tags/0.5.2
```

The tag-triggered workflow reruns the static, Rust, and per-architecture image
gates, then publishes the single multi-architecture image for the repository
and creates the GitHub release. Do not publish
public architecture-specific or `v`-prefixed image tags.

Use Cargo's shared target directory and locking. Do not create ad-hoc target
directories unless the task explicitly requires isolation.
