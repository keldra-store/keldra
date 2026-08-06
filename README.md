# Anvil

Anvil is distributed object storage for application state. It keeps opaque bytes
at stable paths and supplies the coordination primitives applications otherwise
have to assemble around a blob store: streaming writes, compare-and-swap,
immutable namespaces, Zanzibar authorization, bounded atomic programs, change
notification, and materialized search indexes.

Run one process while developing. Add nodes when you need capacity or
availability; clients keep using the same API, any active node can accept a
request, and Anvil places data across heterogeneous nodes with capacity-weighted
rendezvous hashing.

## What is available

| Capability | Release | Status |
| --- | --- | --- |
| Object storage | 0.5.0 | Streaming puts, deduplication, CAS, immutable puts, bulk writes, batch reads, deletes, optional version retention, prefix listing, and watches |
| Authorization | 0.5.0 | Application credentials, short-lived JWTs, protected administration, Zanzibar schemas, tuples, roles, and checks |
| Atomic programs | 0.5.0 | Explicitly selected, deterministic multi-path state transitions without routing ordinary uploads through a transaction system |
| Distributed clusters | 0.5.1 | Any-node ingress, peer mTLS, replicated metadata, weighted placement, and 2+1 erasure-coded payload durability |
| Materialized indexes | 0.5.2 | Path, object metadata, typed JSON, full text, vector, hybrid, Git-source, and tensor indexes |
| Rust client | 0.5.2 | Credential exchange, authenticated clients, streaming upload helpers, and the complete generated gRPC API |
| PersonalDB, public reads, accounting, S3 and Git | 0.5.3 | Protocol-native PersonalDB groups and projections, authorized usage aggregates, opt-in anonymous reads, and standard S3/Git gateways |
| Online cluster growth | 0.5.4 | Large objects use complete replicas below the configured erasure width, then move online to the fixed erasure profile as nodes join |
| Shared public listener | 0.5.5 | Native gRPC, S3, Git, and administrative APIs share one authorized public endpoint; peer mTLS remains isolated |
| Java client | — | TODO |
| Python client | — | TODO |
| Node.js client | — | TODO |
| Ruby client | — | TODO |
| Network plugins | — | Planned after 0.5.3 |

The published container is a single multi-platform image for Linux AMD64 and
ARM64.

## Your first object in five minutes

This walkthrough starts one node, creates the system administrator, provisions
a tenant and its first application, creates a bucket, then writes and reads an
object. It uses the CLI inside the published image, so only Docker and this
repository are required.

### 1. Start a development node

```sh
export ANVIL_IMAGE=ghcr.io/worka-ai/anvil:0.5.5
export ANVIL_TOKEN_SIGNING_KEY_FILE="$PWD/anvil-data/token-signing-key"

mkdir -p anvil-data
head -c 64 /dev/urandom > "$ANVIL_TOKEN_SIGNING_KEY_FILE"
chmod 0600 "$ANVIL_TOKEN_SIGNING_KEY_FILE"

# The container runs as UID 10001 and deliberately rejects a broadly readable
# signing key.
docker run --rm --user 0 \
  -v "$ANVIL_TOKEN_SIGNING_KEY_FILE:/key" \
  "$ANVIL_IMAGE" chown 10001:10001 /key

ANVIL_RUN_SYSTEM_BOOTSTRAP=true \
  docker compose -f crates/anvil/docker-compose.yml up -d
```

The first successful bootstrap creates one mode-`0600` credential at
`/var/lib/anvil/system-bootstrap-credential.json`. It belongs to the protected
system administrator application. Copy it to an operator secret store, then
remove the generated copy:

```sh
docker compose -f crates/anvil/docker-compose.yml cp \
  anvil:/var/lib/anvil/system-bootstrap-credential.json \
  anvil-data/system-bootstrap-credential.json
chmod 0600 anvil-data/system-bootstrap-credential.json
```

Bootstrap is explicit and one-shot. Recreate the process without the flag after
the first successful start; the named data volume is retained:

```sh
ANVIL_RUN_SYSTEM_BOOTSTRAP=false \
  docker compose -f crates/anvil/docker-compose.yml up -d --force-recreate
```

### 2. Create a tenant and its owner application

An application authenticates with a `client_id` and `client_secret`. Credential
exchange returns a short-lived bearer token; the CLI and Rust client perform
that exchange for you.

Choose and retain a strong application secret:

```sh
export ANVIL_OWNER_SECRET="$(openssl rand -hex 32)"
```

Use the system credential to create tenant `example`, owner application
`example-owner`, and client `example-client` in one operation:

```sh
docker compose -f crates/anvil/docker-compose.yml exec \
  -e ANVIL_NEW_CLIENT_SECRET="$ANVIL_OWNER_SECRET" anvil \
  anvil --endpoint http://127.0.0.1:50051 \
  --credentials-file /var/lib/anvil/system-bootstrap-credential.json \
  provision-tenant example example-owner example-client
```

The generated system credential should not remain on the server after you have
copied it and completed bootstrap:

```sh
docker compose -f crates/anvil/docker-compose.yml exec --user 0 anvil \
  rm /var/lib/anvil/system-bootstrap-credential.json
```

### 3. Create a bucket

The tenant owner may create buckets. The application that creates a bucket
becomes its owner:

```sh
docker compose -f crates/anvil/docker-compose.yml exec \
  -e ANVIL_CLIENT_ID=example-client \
  -e ANVIL_CLIENT_SECRET="$ANVIL_OWNER_SECRET" anvil \
  anvil --endpoint http://127.0.0.1:50051 \
  create-bucket objects
```

Buckets are unversioned by default: overwriting or deleting a current object
does not expose an older value later. Use `create-bucket objects --versioning
enabled` when retained versions are part of the application contract.

### 4. Upload and read an object

```sh
printf 'hello from Anvil\n' > anvil-data/hello.txt
docker compose -f crates/anvil/docker-compose.yml cp \
  anvil-data/hello.txt anvil:/tmp/hello.txt

docker compose -f crates/anvil/docker-compose.yml exec \
  -e ANVIL_CLIENT_ID=example-client \
  -e ANVIL_CLIENT_SECRET="$ANVIL_OWNER_SECRET" anvil \
  anvil --endpoint http://127.0.0.1:50051 \
  put example objects greetings/hello.txt /tmp/hello.txt \
  --content-type text/plain --command-id first-upload

docker compose -f crates/anvil/docker-compose.yml exec \
  -e ANVIL_CLIENT_ID=example-client \
  -e ANVIL_CLIENT_SECRET="$ANVIL_OWNER_SECRET" anvil \
  anvil --endpoint http://127.0.0.1:50051 \
  get example objects greetings/hello.txt
```

The last command prints `hello from Anvil`. You now have a tenant-isolated,
Zanzibar-authorized object addressed by `(tenant, bucket, path)`.

## Use the Rust client

```sh
cargo add anvil-storage@0.5.5
cargo add tokio --features macros,rt-multi-thread
```

The application needs the public endpoint and the client ID and secret created
above. Tenant and bucket are part of each object address, not connection-wide
state.

```rust,no_run
use anvil_storage::v1::{
    Durability, HeadObjectRequest, ObjectAddress, PutHeader, PutOperation,
    put_header,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut objects = anvil_storage::connect_with_credentials(
        "http://127.0.0.1:50051",
        "example-client",
        std::env::var("ANVIL_OWNER_SECRET")?,
    )
    .await?;

    let address = ObjectAddress {
        tenant: "example".into(),
        bucket: "objects".into(),
        path: "greetings/from-rust.txt".into(),
    };

    let receipt = anvil_storage::put_chunks(
        &mut objects,
        PutHeader {
            address: Some(address.clone()),
            content_type: "text/plain".into(),
            command_id: "rust-first-upload".into(),
            durability: Durability::Local as i32,
            operation: Some(put_header::Operation::Put(PutOperation {})),
        },
        [b"hello from Rust\n".to_vec()],
    )
    .await?;

    let head = objects
        .head_object(HeadObjectRequest {
            address: Some(address),
        })
        .await?
        .into_inner();

    println!("published version {}: {head:?}", receipt.version);
    Ok(())
}
```

`LOCAL` and `REPLICATED` describe when the client is acknowledged, not where
the object ultimately lives. `LOCAL` returns after the ingress node has durably
accepted the write while normal placement continues. `REPLICATED` waits for
the fixed 2+1 payload guarantee (or the corresponding mutable-record quorum).

For long-running processes, exchange credentials again when the returned token
expires. `connect_channel`, `exchange_client_credentials`, `BearerToken`, and
the generated service clients let an application share one transport across
object, index, authorization, and administration calls. The focused Rust guide
is at [clients/rust/README.md](clients/rust/README.md).

## Create a PersonalDB group

PersonalDB gives an application a witnessed, predecessor-linked log for SQLite
changesets. Source and standalone groups accept authorized appends; projection
groups are materialized explicitly from a source group. Group roles are
Zanzibar-authorized independently of ordinary object traffic.

Add the public client and canonical protocol types:

```sh
cargo add anvil-storage@0.5.5 personaldb-protocol@0.2.2 serde_json
```

Use the same application credential created above to create a source group and
verify Anvil's signed descriptor:

```rust,no_run
use anvil_storage::v1::{CreatePersonalDbGroupRequest, PersonalDbGroupKind};
use personaldb_protocol::{
    GroupDescriptor, PublicKeyTrustRecord, PublicKeyTrustStore, Sha256Digest,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = anvil_storage::connect_channel("http://127.0.0.1:50051").await?;
    let token = anvil_storage::exchange_client_credentials(
        channel.clone(),
        "example-client",
        std::env::var("ANVIL_OWNER_SECRET")?,
    )
    .await?;
    let mut personaldb = anvil_storage::personaldb_client(channel, &token.access_token)?;

    let group = personaldb
        .create_group(CreatePersonalDbGroupRequest {
            bucket: "objects".into(),
            database_id: "main".into(),
            group_id: "primary".into(),
            kind: PersonalDbGroupKind::Source as i32,
            schema_hash_sha256: Sha256Digest::hash(b"application-schema-v1")
                .as_bytes()
                .to_vec(),
            mirror_projection: None,
            command_id: "create-main-primary".into(),
        })
        .await?
        .into_inner();

    let records = group
        .trust_records_json
        .iter()
        .map(|bytes| serde_json::from_slice::<PublicKeyTrustRecord>(bytes))
        .collect::<Result<Vec<_>, _>>()?;
    let trust = PublicKeyTrustStore::from_records(records)?;
    let descriptor = GroupDescriptor::decode_canonical(&group.descriptor)?;
    descriptor.verify(&trust)?;
    println!("created {}", descriptor.group_id());
    Ok(())
}
```

`AppendEntry` advances the exact committed predecessor and carries the SQLite
changeset plus client, voter, and admission evidence. `CatchUp` streams
canonical `personaldb-protocol` frames, while `MaterializeProjection` derives
projection output server-side. The complete public workflow is executable in
[`crates/anvil/examples/personaldb_qualification.rs`](crates/anvil/examples/personaldb_qualification.rs).

## Track billable usage

Accounting is opt-in for a whole bucket or one path prefix. Anvil maintains the
aggregate asynchronously and authorizes both configuration and reads through
Zanzibar. Each snapshot reports current object count and logical stored bytes,
cumulative accepted inbound and served outbound bytes, and a source checkpoint
so billing code can judge freshness.

Use an empty `path_prefix` for the whole bucket, or a canonical prefix such as
`customers/acme` for one application's billing boundary:

```rust,no_run
use anvil_storage::{BearerToken, connect_channel, exchange_client_credentials};
use anvil_storage::v1::accounting_service_client::AccountingServiceClient;
use anvil_storage::v1::{EnableAccountingRequest, GetAccountingRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel("http://127.0.0.1:50051").await?;
    let token = exchange_client_credentials(
        channel.clone(),
        "example-client",
        std::env::var("ANVIL_OWNER_SECRET")?,
    )
    .await?;
    let mut accounting = AccountingServiceClient::with_interceptor(
        channel,
        BearerToken::new(&token.access_token)?,
    );

    accounting
        .enable_accounting(EnableAccountingRequest {
            bucket: "objects".into(),
            path_prefix: "customers/acme".into(),
            command_id: "enable-acme-accounting".into(),
        })
        .await?;

    // Query after application traffic; freshness identifies the exact source
    // journal checkpoints included by the asynchronous aggregate.
    let usage = accounting
        .get_accounting(GetAccountingRequest {
            bucket: "objects".into(),
            path_prefix: "customers/acme".into(),
        })
        .await?
        .into_inner();
    println!(
        "{} objects, {} stored bytes, {} bytes in, {} bytes out; freshness={:?}",
        usage.object_count,
        usage.logical_stored_bytes,
        usage.accepted_inbound_bytes,
        usage.served_outbound_bytes,
        usage.freshness,
    );
    Ok(())
}
```

The complete enable, traffic, convergence, and disable flow is executable in
[`crates/anvil/examples/accounting_qualification.rs`](crates/anvil/examples/accounting_qualification.rs).

## Use the S3 endpoint

The public endpoint on port `50051` accepts both native gRPC and standard
SigV4 path-style S3 requests. Use an Anvil application `client_id` as the AWS
access-key ID and its `client_secret` as the AWS secret-access key; Zanzibar
still decides what that application may do.

With the AWS CLI installed, the owner application created above can exercise
the minimum S3 surface directly:

```sh
export AWS_ACCESS_KEY_ID=example-client
export AWS_SECRET_ACCESS_KEY="$ANVIL_OWNER_SECRET"
export AWS_DEFAULT_REGION=eu-west-2
export ANVIL_S3_ENDPOINT=http://127.0.0.1:50051

aws --endpoint-url "$ANVIL_S3_ENDPOINT" s3api create-bucket \
  --bucket s3-demo
printf 'hello through S3\n' > anvil-data/s3-hello.txt
aws --endpoint-url "$ANVIL_S3_ENDPOINT" s3api put-object \
  --bucket s3-demo --key greetings/hello.txt \
  --content-type text/plain --body anvil-data/s3-hello.txt
aws --endpoint-url "$ANVIL_S3_ENDPOINT" s3api head-object \
  --bucket s3-demo --key greetings/hello.txt
aws --endpoint-url "$ANVIL_S3_ENDPOINT" s3api get-object \
  --bucket s3-demo --key greetings/hello.txt anvil-data/s3-downloaded.txt
cmp anvil-data/s3-hello.txt anvil-data/s3-downloaded.txt
aws --endpoint-url "$ANVIL_S3_ENDPOINT" s3api list-objects-v2 \
  --bucket s3-demo --prefix greetings/
aws --endpoint-url "$ANVIL_S3_ENDPOINT" s3api delete-object \
  --bucket s3-demo --key greetings/hello.txt
```

The same endpoint works with the official AWS SDKs. The Rust SDK qualification
uses one or three public endpoints and verifies every returned byte in
[`crates/anvil/examples/s3_qualification.rs`](crates/anvil/examples/s3_qualification.rs).

## Push and clone Git repositories

Anvil serves Git's smart HTTP protocol at
`/git/<tenant>/<bucket>/<repository>.git` on the same public port `50051`.
The first authenticated push creates the repository; subsequent pushes use CAS
when publishing its ordinary Anvil bundle object. Basic authentication accepts
the same application client ID and secret used by the gRPC and S3 APIs.

```sh
mkdir -p anvil-data/git-demo
git -C anvil-data/git-demo init --initial-branch=main
git -C anvil-data/git-demo config user.name "Anvil Example"
git -C anvil-data/git-demo config user.email "anvil@example.invalid"
printf '# Stored in Anvil\n' > anvil-data/git-demo/README.md
git -C anvil-data/git-demo add README.md
git -C anvil-data/git-demo commit -m initial

export ANVIL_GIT_URL=http://127.0.0.1:50051/git/example/objects/demo.git
export ANVIL_GIT_AUTH="$(printf '%s:%s' example-client "$ANVIL_OWNER_SECRET" | base64 | tr -d '\n')"
git -C anvil-data/git-demo \
  -c "http.extraHeader=Authorization: Basic $ANVIL_GIT_AUTH" \
  push "$ANVIL_GIT_URL" main
git -c "http.extraHeader=Authorization: Basic $ANVIL_GIT_AUTH" \
  clone --branch main "$ANVIL_GIT_URL" anvil-data/authenticated-clone
git -C anvil-data/authenticated-clone \
  -c "http.extraHeader=Authorization: Basic $ANVIL_GIT_AUTH" pull
```

To make pulls and clones public, enable public reads for the repository's
bucket through the authorized command shown below, then omit the authentication
header:

```sh
docker compose -f crates/anvil/docker-compose.yml exec \
  -e ANVIL_CLIENT_ID=example-client \
  -e ANVIL_CLIENT_SECRET="$ANVIL_OWNER_SECRET" anvil \
  anvil --endpoint http://127.0.0.1:50051 \
  set-bucket-public-read objects enabled

git clone --branch main "$ANVIL_GIT_URL" anvil-data/public-clone
```

Push always requires an authorized application. A real-client push, pull, and
public-clone qualification is kept in
[`crates/anvil/tests/git_gateway.rs`](crates/anvil/tests/git_gateway.rs).

## Authorization model

Authorization is enforced on the API; deployment topology is not a security
boundary.

1. The one unauthenticated credential RPC exchanges an application
   `client_id` and Argon2id-verified secret for a short-lived signed JWT.
2. The JWT identifies one stable application and tenant. It contains no trusted
   caller-supplied roles.
3. Every protected RPC evaluates that identity through Zanzibar relationships.
4. The protected `_anvil/system` realm governs Anvil administration. Customer
   realms use the same Zanzibar primitives for application-domain models, but
   cannot modify the system realm.

Tenant roles are `owner`, `admin`, `reader`, `manage_tenant`, `read_tenant`,
`manage_buckets`, and `manage_authz`. Bucket roles are `owner`, `admin`,
`reader`, `writer`, `get`, `put`, `delete`, and `manage_policy`. A bucket owner
may delegate the narrowest useful role to another application; index results
are authorization-filtered before they are returned.

A bucket owner can enable public reads with the CLI or
`AdministrationService.SetBucketPublicRead`:

```sh
docker compose -f crates/anvil/docker-compose.yml exec \
  -e ANVIL_CLIENT_ID=example-client \
  -e ANVIL_CLIENT_SECRET="$ANVIL_OWNER_SECRET" anvil \
  anvil --endpoint http://127.0.0.1:50051 \
  set-bucket-public-read objects enabled
```

A request with no bearer token is then evaluated as Anvil's built-in,
unmanageable anonymous application. It may read only where the owner
explicitly granted that bucket policy. Supplying an invalid token remains an
authentication error, and anonymous callers never bypass Zanzibar or gain
write, index-management, or administration access.

## Compare-and-swap and immutable data

Anvil exposes separate operations so intent is visible in code:

- `Put` always advances the path version.
- `PutIfAbsent` succeeds only if the path has never had a live value.
- `PutIfVersion(expected_version)` succeeds only against that exact current
  version.
- `PutImmutable` creates write-once content.
- `Delete` advances the path to a tombstone.
- `DeleteIfVersion(expected_version)` deletes only the expected current value.

Bucket policies can also reserve immutable or `PROGRAM_ONLY` path prefixes.
The latter may be changed only by an atomic program, preventing an ordinary
writer from racing a program dependency.

## Atomic programs

An atomic program is a small, bounded JSON state-transition definition—not
uploaded executable code and not a transaction wrapper around ordinary object
traffic. Use one where several paths represent one application invariant, such
as moving a balance while appending a corresponding immutable ledger entry.

The lifecycle is deliberately explicit:

1. Put an immutable definition at `_anvil/programs/<name>@<version>`.
2. `HeadObject` it and retain the returned BLAKE3 hash.
3. Mark every path the program may read or write as `PROGRAM_ONLY`.
4. Invoke the exact path and hash with a unique invocation ID and JSON bindings.
5. Anvil authorizes and locks the declared paths, evaluates the bounded DSL on
   the nominated executor, and publishes every resulting head atomically.

Ordinary uploads—including large media—continue to use `Put`; they never enter
this orchestration path. A complete two-document program and invocation is kept
executable in
[`scripts/qualify-three-node.sh`](scripts/qualify-three-node.sh).

## Create and query indexes

Indexes are bucket-local definitions scoped by an optional path prefix and
content type. One cluster writer consumes every node's ordered source journal,
publishes immutable index generations through the ordinary object store, and
up to three HRW-selected query replicas materialize them through a bounded
cache. Query responses always include freshness evidence; a newly created or
busy index may return a valid partial generation while it catches up.

Construct an authenticated index client from the same token used for objects:

```rust,no_run
use anvil_storage::{BearerToken, connect_channel, exchange_client_credentials};
use anvil_storage::v1::index_service_client::IndexServiceClient;

# async fn client() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let channel = connect_channel("http://127.0.0.1:50051").await?;
let token = exchange_client_credentials(
    channel.clone(),
    "example-client",
    std::env::var("ANVIL_OWNER_SECRET")?,
)
.await?;
let mut indexes = IndexServiceClient::with_interceptor(
    channel,
    BearerToken::new(&token.access_token)?,
);
# let _ = &mut indexes;
# Ok(())
# }
```

Call `CreateIndex` with a `CreateIndexRequest`, then `QueryIndex` with the
matching query variant. These are the eight engines and their minimal useful
definitions:

| Engine | Definition | Query | Source object shape |
| --- | --- | --- | --- |
| Path | `PathIndexSpec {}` | `PathIndexQuery { prefix, start_after }` | Any object |
| Metadata filter | fields such as `path`, `content_type`, `content_length` | Predicates over retained head fields | Any object |
| Typed JSON | named fields mapped to JSON Pointers, e.g. `status -> /status` | Canonical JSON scalar predicates and ordering | `{"status":"active"}` |
| Full text | named text fields mapped to JSON Pointers | Text or phrase query | `{"body":"durable journal delivery"}` |
| Vector | JSON Pointer, dimensions, cosine/dot/Euclidean metric | A vector with the declared dimensions | `{"embedding":[1.0,0.0,0.0]}` |
| Hybrid | Full-text and vector specifications plus weights | Text and vector together | `{"title":"rust search","embedding":[1.0,0.0,0.0]}` |
| Git source | Repository ID | Commit ID and exact/prefix tree path | Git manifest containing repository, commit, tree path, object ID, pack path/version, offset, and length |
| Tensor | Model ID | Tensor name | Tensor manifest containing model, tensor name, source path/version, offset, length, dtype, and shape |

For example, a typed JSON definition retains `/status` and queries it using one
canonical JSON string:

```rust,no_run
use anvil_storage::v1::index_query::Query as QueryValue;
use anvil_storage::v1::index_specification::Specification as SpecValue;
use anvil_storage::v1::*;

# async fn example(mut indexes: anvil_storage::v1::index_service_client::IndexServiceClient<tonic::service::interceptor::InterceptedService<tonic::transport::Channel, anvil_storage::BearerToken>>) -> Result<(), tonic::Status> {
indexes.create_index(CreateIndexRequest {
    bucket: "objects".into(),
    name: "active-documents".into(),
    path_prefix: "documents/".into(),
    content_type: "application/json".into(),
    specification: Some(IndexSpecification {
        specification: Some(SpecValue::TypedJson(TypedJsonIndexSpec {
            fields: vec![IndexField {
                name: "status".into(),
                json_pointer: "/status".into(),
            }],
        })),
    }),
    command_id: "create-active-documents".into(),
}).await?;

let response = indexes.query_index(QueryIndexRequest {
    bucket: "objects".into(),
    index_name: "active-documents".into(),
    query: Some(IndexQuery {
        query: Some(QueryValue::TypedJson(TypedJsonIndexQuery {
            predicates: vec![IndexPredicate {
                field: "status".into(),
                operator: IndexPredicateOperator::Equal as i32,
                values_json: vec![br#""active""#.to_vec()],
            }],
            order: vec![],
        })),
    }),
    limit: 100,
    page_token: vec![],
}).await?.into_inner();

for hit in response.hits {
    println!("{}", hit.address.unwrap().path);
}
println!("freshness: {:?}", response.freshness);
# Ok(())
# }
```

The repository's public-API qualification constructs, populates, and queries
all eight variants in
[`crates/anvil/examples/cluster_index_qualification.rs`](crates/anvil/examples/cluster_index_qualification.rs).

## Run a three-node cluster

The local qualification starts three real nodes, admits joiners with one-use
bundles, establishes peer mTLS, exercises replicated and erasure-coded storage,
queries every index type, and performs a rolling restart:

```sh
ANVIL_IMAGE=ghcr.io/worka-ai/anvil:0.5.5 \
  ./scripts/qualify-three-node.sh
```

Production formation uses the same sequence:

1. Start the first node with `--run-system-bootstrap`, a stable
   `--peer-advertise` address, durable storage, and the operator-managed JWT
   signing key.
2. From an authorized active node, run
   `anvil prepare-node <node-id> <peer-address>`.
3. Copy the generated mode-`0600` join bundle to the configured joining node,
   delete the source copy, and start `anvil-server --join-bundle <path>` there.
4. Repeat for additional nodes. Set each node's storage weight to its usable
   capacity; weighted HRW moves only ownership affected by membership changes.

The public gRPC endpoint may sit behind an ordinary TLS terminator. Peer traffic
uses mandatory certificates created and rotated by the cluster.

## Public services

| Service | Purpose |
| --- | --- |
| `CredentialService` | Exchange durable application credentials for short-lived bearer tokens |
| `ObjectService` | Streaming writes, CAS, bulk/batch operations, reads, versions, listing, watches, policies, and atomic programs |
| `IndexService` | Create, update, inspect, list, delete, and query materialized indexes |
| `PersonalDbService` | Create and authorize database groups, append protocol evidence, explicitly materialize projections, catch up, and transfer snapshots |
| `AccountingService` | Enable and query authorization-protected bucket or path-prefix usage aggregates |
| `AuthzService` | Manage customer Zanzibar realms, schemas, relationships, bindings, and checks |
| `AdministrationService` | Protected tenant, bucket, credential, role, and cluster lifecycle |

The versioned contracts are
[`anvil.proto`](crates/anvil-api/proto/anvil.proto) and
[`personaldb.proto`](crates/anvil-api/proto/personaldb.proto).

## Architecture in one page

Every successful write creates immutable content and advances one current
exact-path head. Values up to 64 KiB remain inline in a RocksDB column family;
larger values are content-addressed, use complete copies while the cluster is
smaller than its fixed erasure width, and move online to erasure-coded shards
as nodes join. Mutable records are replicated. Compact cluster membership and
atomic-publication decisions use Raft; object bodies, path inventories, program
locks, and index files do not.

Names resolve to stable numeric IDs, so renaming human-facing identifiers does
not rewrite every storage key. Ordered per-node source journals feed watches,
reference accounting, and the single writer for each materialized index. Index
artifacts are ordinary Anvil objects; local materialization is disposable
acceleration, not another authoritative storage plane.

The architecture contracts live in
[ANVIL-0009](docs/rfcs/anvil_0009_atomic_programs.md) and
[ANVIL-0010](docs/rfcs/anvil_0010_cluster_distribution.md).

## Build and qualify

Anvil requires Rust 1.96 or newer.

```sh
cargo fmt --all -- --check
cargo test --workspace
```

Build and qualify the container locally before CI:

```sh
ANVIL_IMAGE=anvil:local ./scripts/build-image.sh
ANVIL_IMAGE=anvil:local ./scripts/release-gates.sh image
ANVIL_IMAGE=anvil:local ./scripts/qualify-three-node.sh
```

Anvil 0.5 has a new API and storage format; migrate 0.4 data by export and
import. Current operational boundaries are collected in the
[known limitations](docs/known-limitations.md).

Apache-2.0 licensed.
