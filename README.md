# Anvil

Anvil is a production object storage platform with indexing, search, relationship authorisation, durable watches, and PersonalDB witnessing built into the storage layer.

A normal object store can put bytes under a key and read them later. Product systems usually need much more: predictable path layouts, version history, metadata filters, full text search, vector retrieval, access checks on every result, live change streams, background repair evidence, static delivery, S3-compatible tooling, and a way to sync local-first SQLite applications without adding a separate database service. Anvil treats those capabilities as storage responsibilities instead of leaving every application to rebuild them.

## Current Capabilities

- **Object storage:** tenants, buckets, keys, object versions, current pointers, delete markers, checksums, range reads, copy/delete operations, multipart flows, native gRPC APIs, and S3-compatible object access.
- **CoreStore durability:** objects, metadata, indexes, authorisation records, PersonalDB state, mesh lifecycle records, gateway records, leases, audit events, and repair findings persist through immutable objects, ordered streams, and compare-and-swap refs.
- **Indexes and search:** path indexes, metadata-filter indexes, typed JSON indexes, full text indexes, vector indexes, and hybrid indexes with documented selector, extractor, build-policy, query, diagnostics, pagination, and catch-up shapes.
- **Authorisation:** public policy scopes for tenant app credentials, Zanzibar-style relationship authorisation for tenant product data, and private system-realm relations for the admin API.
- **Watch streams:** cursor-based streams for source changes and derived maintenance so applications and background workers can catch up without rescanning everything.
- **PersonalDB witnessing:** SQLite changeset validation, commit certificates, catch-up, snapshots, projection records, row metadata, and repair evidence for local-first applications.
- **Gateways:** native public API, S3-compatible object access, static host-alias routing, object links, and CoreStore-backed gateway foundation records.
- **Operations:** Docker-first server deployment, separate public/admin listeners, tenant and app provisioning, least-privilege grants, topology lifecycle, diagnostics, repair, audit listing, and release gates.

## Architecture

CoreStore is Anvil's durable boundary. It has three primitives:

| Primitive | Purpose |
| --- | --- |
| `CoreObject` | Immutable bytes: payloads, index segments, snapshots, source packs, gateway blobs. |
| `CoreStream` | Ordered facts: object mutations, authz tuple logs, audit events, append records, PersonalDB commits. |
| `CoreRef` | CAS heads: current object pointers, index generations, PersonalDB heads, routing state, leases. |

Feature-specific formats still exist where useful, but durable truth goes through those primitives. S3 is a gateway, not the storage model. The native API, S3 gateway, search, watches, PersonalDB, and admin workflows all resolve back to Anvil tenants, buckets, resources, authorisation, and CoreStore records.

CoreStore is split into a metadata plane and a byte plane. CoreMeta uses RocksDB column families for metadata, heads, versions, transactions, index definitions, segment locators, authz rows, mesh records, leases, and other small control records. Tiny payloads may be inlined according to the inline payload policy. Larger durable bytes are written through the CoreStore byte pipeline and stored as erasure-coded shard data. Index segments, stream payloads, PersonalDB pages, gateway blobs, and object bodies all follow that same rule.

## Release Surfaces

- **Server:** Docker image and release binaries. The server crate is not published to crates.io.
- **Rust client:** `anvil-storage = "0.3.0"` on crates.io.
- **Saga API:** reserved in the wire protocol and Rust client for forward compatibility; server-side saga execution is intentionally rejected in this release.
- **CLIs:** `anvil` for tenant/public operations and `anvil-admin` for private admin-plane operations.
- **Documentation:** Fission static site in `documentation/`, published by a separate docs workflow.
- **Protocol bindings:** generated gRPC bindings are packaged with the Rust client.

## Quick Start

Anvil is Docker-first. Set `ANVIL_IMAGE` to the image published for the release you want to run, preferably pinned by tag or digest:

```sh
export ANVIL_IMAGE="ghcr.io/worka-ai/anvil:v0.3.0"
docker pull "$ANVIL_IMAGE"
```

For a real local setup, use the Docker tutorial because it also covers server secret material, first-start bootstrap, public/admin listener separation, and token flow:

```sh
fission site serve --project-dir documentation
# then open /tutorials/setup-local-anvil/
```

Add the Rust client to an application:

```toml
[dependencies]
anvil-storage = "0.3.0"
```

Use the client with a bearer token minted by Anvil:

```rust
use anvil_storage::{proto::ListBucketsRequest, AnvilClient};

async fn example(
    endpoint: String,
    token: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = AnvilClient::connect_with_bearer(endpoint, token).await?;
    let response = client.buckets().list_buckets(ListBucketsRequest {}).await?;
    println!("{} buckets", response.into_inner().buckets.len());
    Ok(())
}
```

Existing S3-compatible tools can use the S3 gateway with Anvil-issued app credentials when the operation is object-shaped. Use the native API or Rust client for Anvil-specific features such as typed indexes, watches, PersonalDB, relationship authorisation, task leases, and repair workflows.

## Documentation

The documentation is organised as five books:

- `documentation/content/learn/` teaches the concepts from first principles.
- `documentation/content/architecture/` explains storage internals, CoreMeta, index formats, mesh transport, release status, and contributor rules.
- `documentation/content/tutorials/` walks through concrete operations.
- `documentation/content/operators/` covers deployment and production operation.
- `documentation/content/reference/` documents the CLIs, authorisation action/resource strings, and index/query JSON shapes.

Build and check the site locally:

```sh
fission site check --project-dir documentation --release
fission site build --project-dir documentation --release
```

## Development Checks

Run these before changing core behaviour or opening a release PR:

```sh
cargo fmt --all -- --check
ANVIL_BUILD_PROFILE=release ANVIL_IMAGE=anvil:test ./scripts/build-image.sh
./scripts/release-gates.sh
```

`release-gates.sh` includes storage hardening checks, docs hardening, release-note rendering, the Fission documentation build, the Rust client publish dry run, Rust unit tests, server core integration tests, and Docker-backed integration groups. Each gate step prints start/finish timings and, where GNU `timeout` is available, is bounded by `ANVIL_GATE_STEP_TIMEOUT_SECONDS` (default `1800`). `build-image.sh` defaults to a ci-profile image for PR and local test turnaround; set `ANVIL_BUILD_PROFILE=release` when building release evidence. Security-sensitive changes should also include focused tests for the affected path before the full gate.

PR CI runs `build-image.sh` once for a fast `linux/amd64` ci-profile test image. Release builds run `build-image.sh` for both `linux/amd64` and `linux/arm64` with `ANVIL_BUILD_PROFILE=release`. The script uses Zig/cargo-zigbuild by default so the Linux binaries are compatible with the runtime image instead of accidentally depending on the GitHub runner's newer glibc. Docker integration tests run against the `linux/amd64` artifact, while the `linux/arm64` artifact is built and smoke-checked before publication. The release workflow publishes architecture-specific GHCR tags and then creates the public multi-architecture tag from those tested artifacts.

## Release Process

The release flow is designed so PR and release testing use the same gates:

1. Open a PR containing source, docs, workflow, README, and blog/release-note changes.
2. Merge to `main` only after CI passes.
3. Tag the release.
4. Let the release workflow test the Docker image, publish the tested image, publish `anvil-storage` if the version is new, render release notes from the release blog post, and create the GitHub release.
5. Let the independent documentation workflow publish the Fission static site from `documentation/`.

See `documentation/content/operators/release-readiness-checklist.md` for the operator checklist and `documentation/content/architecture/release-status.md` for the release architecture status report.

## License

Anvil is licensed under the Apache 2.0 License. See `LICENSE`.

## Docker-first local run shape

A single-node local run is useful for learning the planes before building a larger topology. Keep the storage path on a volume, generate real secret material for anything you intend to keep, and remember that the admin listener is private even in local demos.

```sh
export ANVIL_IMAGE="ghcr.io/worka-ai/anvil:v0.3.0"
export ANVIL_SECRET_ENCRYPTION_KEY="$(anvil-admin key generate-secret-encryption-key)"

docker run --rm \
  --name anvil-local \
  -p 127.0.0.1:50051:50051 \
  -v anvil-local-data:/var/lib/anvil \
  -e STORAGE_PATH=/var/lib/anvil \
  -e MESH_ID=local \
  -e NODE_ID=anvil-local \
  -e REGION=local \
  -e CELL_ID=local \
  -e API_LISTEN_ADDR=0.0.0.0:50051 \
  -e PUBLIC_API_ADDR=http://127.0.0.1:50051 \
  -e ADMIN_LISTEN_ADDR=127.0.0.1:50052 \
  -e JWT_SECRET="local-jwt-secret-change-me" \
  -e ANVIL_SECRET_ENCRYPTION_KEY="$ANVIL_SECRET_ENCRYPTION_KEY" \
  -e BOOTSTRAP_SYSTEM_ADMIN_APP_NAME=ops-admin \
  -e BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH=/var/lib/anvil/first-admin.json \
  "$ANVIL_IMAGE"
```

PersonalDB asymmetric signing is an optional in-process capability of the Anvil
server. It requires no signer sidecars, socket mounts, or signing
manifest. A server with no PersonalDB signing keys starts normally and retains
its object, stream, and index behavior. PersonalDB uses Ed25519 evidence only;
PersonalDB operations that need a signature require an active, purpose-scoped
key provisioned through the authenticated admin plane with
`anvil-admin personal-db-signing-key`.
See [Secrets and Key Management](documentation/content/operators/secrets-and-key-management.md#personaldb-protocol-signing-keys)
for the custody and rotation model.

After the container is ready, the host can reach only the public plane at `http://127.0.0.1:50051`. The admin listener is bound to loopback inside the container and is deliberately not published to the host. For local admin smoke tests, run `anvil-admin` with `docker exec` so the command executes inside that private boundary; for example, pass `ANVIL_AUTH_TOKEN` into the container and let the in-container CLI call `http://127.0.0.1:50052`.

Tenant applications should use only the public endpoint and the `anvil` CLI or Rust client. If a README example requires the admin CLI to read or write tenant objects, treat that as a documentation bug.

## Plane quick reference

Use the public plane for tenant-owned resources: buckets, objects, object links, tenant application credentials, public policy delegation, relationship tuples, index definitions and queries, watches, append streams, task leases, PersonalDB groups, tenant diagnostics, and tenant repair. Use the admin plane for operator-owned resources: tenants, first application handover, server-side policy grants, secret-envelope rotation, regions, cells, nodes, routing projection repair, administrative diagnostics, and administrative audit. The two planes share authentication mechanics, but they do not share authorisation: public policy scopes do not grant system-realm admin relations.
