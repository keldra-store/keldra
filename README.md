# Anvil

Distributed object storage for application state.

Anvil stores opaque bytes at stable paths and gives applications the
coordination primitives they usually have to build around object storage:
streaming uploads, exact compare-and-swap, immutable namespaces, Zanzibar
authorization, opt-in atomic programs, and materialized search indexes. Run it
as one process for development or as a capacity-weighted cluster with
erasure-coded payloads.

- **Application-shaped primitives.** Use CAS, bulk writes, same-snapshot batch
  reads, prefix listing, watches, version retention, and bounded multi-object
  programs from one API.
- **One secure API on every node.** Any active node can accept a request. JWT
  authentication and Zanzibar authorization protect object, index, and
  administration operations.
- **Search without operating another database.** Anvil builds immutable,
  cluster-wide index generations and reports exactly how fresh every result is.

## Capabilities

| Capability | Status | What it provides |
| --- | --- | --- |
| Object storage | Available | Streaming puts, content-addressed deduplication, CAS, bulk writes, batch reads, deletes, optional version retention, and prefix listing |
| Authorization | Available | Credentials, application realms, Zanzibar schemas, tuples, checks, and protected administration |
| Application invariants | Available | Immutable path prefixes, `PROGRAM_ONLY` data, and bounded deterministic atomic programs |
| Change notification | Available | Bounded, resumable `WatchPrefix` invalidations |
| Distributed cluster | Available | Any-node ingress, capacity-weighted rendezvous placement, cluster-managed peer mTLS, replicated metadata, and configurable erasure coding |
| Derived indexes | Available | Path, object metadata, typed JSON, full text, vector, hybrid, Git-source, and tensor indexes |
| Rust client | Available | Authenticated transport, streaming upload helpers, and the complete generated gRPC API |
| Container image | Available | One `0.5.2` tag for Linux AMD64 and ARM64 |
| PersonalDB | Next: 0.5.3 | Authorized server-built projections, snapshots, catch-up, and synchronization |
| S3 and Git gateways | Planned | Native S3 clients and Git push/pull over the Anvil object core |
| Network plugins | Planned | Independently deployed integrations over public and authenticated peer APIs |

## Start a development node

The published image contains both `anvil-server` and the `anvil` CLI. From a
clone of this repository:

```sh
export ANVIL_IMAGE=ghcr.io/worka-ai/anvil:0.5.2

mkdir -p .anvil
head -c 64 /dev/urandom > .anvil/token-signing-key
chmod 0600 .anvil/token-signing-key

# The image runs as UID 10001 and the server requires this key to remain 0600.
docker run --rm --user 0 \
  -v "$PWD/.anvil/token-signing-key:/key" \
  "$ANVIL_IMAGE" chown 10001:10001 /key

ANVIL_TOKEN_SIGNING_KEY_FILE="$PWD/.anvil/token-signing-key" \
ANVIL_RUN_SYSTEM_BOOTSTRAP=true \
docker compose -f crates/anvil/docker-compose.yml up -d
```

Bootstrap creates a mode-`0600` operator credential inside the data volume.
Use it once to create the first tenant and application credential:

```sh
docker compose -f crates/anvil/docker-compose.yml exec \
  -e ANVIL_NEW_CLIENT_SECRET='replace-with-at-least-32-bytes' anvil \
  anvil --endpoint http://127.0.0.1:50051 \
  --credentials-file /var/lib/anvil/system-bootstrap-credential.json \
  provision-tenant example example-owner example-client

docker compose -f crates/anvil/docker-compose.yml exec \
  -e ANVIL_CLIENT_ID=example-client \
  -e ANVIL_CLIENT_SECRET='replace-with-at-least-32-bytes' anvil \
  anvil --endpoint http://127.0.0.1:50051 create-bucket objects
```

Copy the generated bootstrap credential to the operator's secret store, then
delete that generated copy. The application endpoint is now available at
`http://127.0.0.1:50051`.

## Use the Rust client

```sh
cargo add anvil-storage@0.5.2
cargo add tokio --features macros,rt-multi-thread
```

```rust,no_run
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut objects = anvil_storage::connect_with_credentials(
        "http://127.0.0.1:50051",
        "example-client",
        "replace-with-at-least-32-bytes",
    )
    .await?;

    // `objects` is an authenticated ObjectService client. Generated request
    // types and every public service client live under `anvil_storage::v1`.
    let _ = &mut objects;
    Ok(())
}
```

See the [Rust client guide](clients/rust/README.md) for object reads, streaming
uploads, CAS, and access to the generated authorization, administration, and
index clients.

## Run a three-node cluster

The local cluster qualification starts three real nodes, admits joiners with
one-use bundles, establishes peer mTLS, exercises replicated and erasure-coded
storage, queries every index type, and performs a rolling restart:

```sh
ANVIL_IMAGE=ghcr.io/worka-ai/anvil:0.5.2 \
  ./scripts/qualify-three-node.sh
```

Production formation uses the same short sequence:

1. Start the first node with `--run-system-bootstrap`, a stable
   `--peer-advertise` address, durable storage, and the operator-managed JWT
   signing key.
2. From an authorized active node, run
   `anvil prepare-node <node-id> <peer-address>`.
3. Copy the generated mode-`0600` join bundle to that node and start it with
   `anvil-server --join-bundle <path>`.
4. Repeat for additional nodes. Configure each node's storage weight to match
   its usable capacity; Anvil moves only ownership affected by the change.

The public gRPC endpoint can sit behind an ordinary TLS terminator. Peer
traffic uses certificates created and rotated by the cluster.

## Public API

Anvil publishes one versioned protobuf contract with five services:

| Service | Purpose |
| --- | --- |
| `ObjectService` | Streaming puts, CAS, bulk and batch operations, reads, versions, listing, watches, policies, and atomic programs |
| `IndexService` | Create, update, inspect, list, delete, and query cluster-wide indexes |
| `AuthzService` | Zanzibar realms, schemas, relationships, bindings, and authorization checks |
| `AdministrationService` | Protected tenant, bucket, credential, and cluster lifecycle operations |
| `CredentialService` | Exchange long-lived application credentials for short-lived bearer tokens |

The protobuf definitions are in
[`crates/anvil-api/proto/anvil.proto`](crates/anvil-api/proto/anvil.proto).

## Architecture

Every successful object write creates an immutable version and moves one
current exact-path head. Small values stay inline; large values are split into
content-addressed erasure-coded shards. Mutable records are replicated, while
compact cluster membership and atomic-publication decisions are agreed through
Raft. Object bodies, path inventories, locks, and index files never enter the
Raft log.

Atomic programs are explicitly selected for the small part of an application
that needs multi-path visibility. Ordinary uploads—including large media—stay
on the direct object path. Index builders consume the cluster's ordered source
journals, publish immutable index files as ordinary Anvil objects, and let up
to three query replicas materialize them through a shared bounded cache.

The architectural contracts are documented in
[ANVIL-0009](docs/rfcs/anvil_0009_atomic_programs.md) and
[ANVIL-0010](docs/rfcs/anvil_0010_cluster_distribution.md).

## Build and test

Anvil requires Rust 1.96 or newer.

```sh
cargo fmt --all -- --check
cargo test --workspace
```

Build and qualify the container locally with:

```sh
ANVIL_IMAGE=anvil:local ./scripts/build-image.sh
ANVIL_IMAGE=anvil:local ./scripts/release-gates.sh image
ANVIL_IMAGE=anvil:local ./scripts/qualify-three-node.sh
```

## Operational notes

Anvil 0.5 uses a new API and storage format; migrate 0.4 data by export and
import. The public gRPC endpoint currently relies on external TLS termination.
See [known limitations](docs/known-limitations.md) for the current operational
boundaries.

Apache-2.0 licensed.
