# Keldra

Keldra is distributed object storage for application state. It keeps opaque bytes
at stable paths and supplies the coordination primitives applications otherwise
have to assemble around a blob store: streaming writes, compare-and-swap,
immutable namespaces, Zanzibar authorization, bounded atomic programs, change
notification, and materialized search indices.

Run one process while developing. Add nodes when you need capacity or
availability; clients keep using the same API, any active node can accept a
request, and Keldra places data across heterogeneous nodes with capacity-weighted
rendezvous hashing.

## What is available

| Capability | Release | Status |
| --- | --- | --- |
| Object storage | 0.10.0 | Streaming puts, deduplication, CAS, immutable puts, bulk writes, batch reads, deletes, optional version retention, prefix listing, and watches |
| Authorization | 0.10.0 | Application credentials, short-lived JWTs, protected administration, Zanzibar schemas, tuples, roles, and checks |
| Atomic programs | 0.10.0 | Explicitly selected, deterministic multi-path state transitions without routing ordinary uploads through a transaction system |
| Distributed clusters | 0.10.0 | Any-node ingress, peer mTLS, replicated metadata, weighted placement, and 2+1 erasure-coded payload durability |
| Materialized Typed JSON indices | 0.15.0 | Memory-first partition-owned projection, logical catalog sharing, immutable segment/root publication, and bounded query materialization |
| Rust client | 0.10.0 | Credential exchange, authenticated clients, streaming upload helpers, and the complete generated gRPC API |
| PersonalDB, public reads, accounting, S3 and Git | 0.10.0 | Protocol-native PersonalDB groups and projections, authorized usage aggregates, opt-in anonymous reads, and standard S3/Git gateways |
| Online cluster growth | 0.10.0 | Large objects use complete replicas below the configured erasure width, then move online to the fixed erasure profile as nodes join |
| Shared public listener | 0.10.0 | Native gRPC, S3, Git, administrative APIs and configured HTTP plugins share one authorized public endpoint; peer mTLS remains isolated |
| Memory-first index pipeline | 0.15.0 | Bounded hot ingress, shared physical recipe routing, partition-local segment/root publication, journal recovery, and CPU/memory-first catch-up |
| Dates, zero-copy clones, and protected links | 0.15.0 | ISO 8601 or configured date parsing, independent clones sharing immutable payload bytes, and transparent mutable aliases with deletion fencing |
| General atomic paths | 0.15.0 | Durable path reservations let authorized ordinary paths participate in bounded atomic programs without requiring `PROGRAM_ONLY` policy |
| Java client | — | TODO |
| Python client | — | TODO |
| Node.js client | — | TODO |
| Ruby client | — | TODO |
| Authorized HTTP plugin broker | 0.10.0 | Host-routed private services receive short-lived, tenant/bucket/path-scoped object tokens after Zanzibar authorization |

The published container is a single multi-platform image for Linux AMD64 and
ARM64.

## Your first object in five minutes

This walkthrough starts one node, creates the system administrator, provisions
a tenant and its first application, creates a bucket, then writes and reads an
object. It uses the CLI inside the published image, so only Docker and this
repository are required.

### 1. Start a development node

```sh
export KELDRA_IMAGE=ghcr.io/keldra-store/keldra:0.15.0
export KELDRA_TOKEN_SIGNING_KEY_FILE="$PWD/keldra-data/token-signing-key"

mkdir -p keldra-data
head -c 64 /dev/urandom > "$KELDRA_TOKEN_SIGNING_KEY_FILE"
chmod 0600 "$KELDRA_TOKEN_SIGNING_KEY_FILE"

# The container runs as UID 10001 and deliberately rejects a broadly readable
# signing key.
docker run --rm --user 0 \
  -v "$KELDRA_TOKEN_SIGNING_KEY_FILE:/key" \
  "$KELDRA_IMAGE" chown 10001:10001 /key

KELDRA_RUN_SYSTEM_BOOTSTRAP=true \
  docker compose -f crates/keldra/docker-compose.yml up -d
```

The first successful bootstrap creates one mode-`0600` credential at
`/var/lib/keldra/system-bootstrap-credential.json`. It belongs to the protected
system administrator application. Copy it to an operator secret store, then
remove the generated copy:

```sh
docker compose -f crates/keldra/docker-compose.yml cp \
  keldra:/var/lib/keldra/system-bootstrap-credential.json \
  keldra-data/system-bootstrap-credential.json
chmod 0600 keldra-data/system-bootstrap-credential.json
```

Bootstrap is explicit and one-shot. Recreate the process without the flag after
the first successful start; the named data volume is retained:

```sh
KELDRA_RUN_SYSTEM_BOOTSTRAP=false \
  docker compose -f crates/keldra/docker-compose.yml up -d --force-recreate
```

### 2. Create a tenant and its owner application

An application authenticates with a `client_id` and `client_secret`. Credential
exchange returns a short-lived bearer token; the CLI and Rust client perform
that exchange for you.

Choose and retain a strong application secret:

```sh
export KELDRA_OWNER_SECRET="$(openssl rand -hex 32)"
```

Use the system credential to create tenant `example`, owner application
`example-owner`, and client `example-client` in one operation:

```sh
docker compose -f crates/keldra/docker-compose.yml exec \
  -e KELDRA_NEW_CLIENT_SECRET="$KELDRA_OWNER_SECRET" keldra \
  keldra --endpoint http://127.0.0.1:50051 \
  --credentials-file /var/lib/keldra/system-bootstrap-credential.json \
  provision-tenant example example-owner example-client
```

The generated system credential should not remain on the server after you have
copied it and completed bootstrap:

```sh
docker compose -f crates/keldra/docker-compose.yml exec --user 0 keldra \
  rm /var/lib/keldra/system-bootstrap-credential.json
```

### 3. Create a bucket

The tenant owner may create buckets. The application that creates a bucket
becomes its owner:

```sh
docker compose -f crates/keldra/docker-compose.yml exec \
  -e KELDRA_CLIENT_ID=example-client \
  -e KELDRA_CLIENT_SECRET="$KELDRA_OWNER_SECRET" keldra \
  keldra --endpoint http://127.0.0.1:50051 \
  create-bucket objects
```

Buckets are unversioned by default: overwriting or deleting a current object
does not expose an older value later. Use `create-bucket objects --versioning
enabled` when retained versions are part of the application contract.

### 4. Upload and read an object

```sh
printf 'hello from Keldra\n' > keldra-data/hello.txt
docker compose -f crates/keldra/docker-compose.yml cp \
  keldra-data/hello.txt keldra:/tmp/hello.txt

docker compose -f crates/keldra/docker-compose.yml exec \
  -e KELDRA_CLIENT_ID=example-client \
  -e KELDRA_CLIENT_SECRET="$KELDRA_OWNER_SECRET" keldra \
  keldra --endpoint http://127.0.0.1:50051 \
  put example objects greetings/hello.txt /tmp/hello.txt \
  --content-type text/plain --command-id first-upload

docker compose -f crates/keldra/docker-compose.yml exec \
  -e KELDRA_CLIENT_ID=example-client \
  -e KELDRA_CLIENT_SECRET="$KELDRA_OWNER_SECRET" keldra \
  keldra --endpoint http://127.0.0.1:50051 \
  get example objects greetings/hello.txt
```

The last command prints `hello from Keldra`. You now have a tenant-isolated,
Zanzibar-authorized object addressed by `(tenant, bucket, path)`.

## Use the Rust client

```sh
cargo add keldra@0.15.0
cargo add tokio --features macros,rt-multi-thread
```

The application needs the public endpoint and the client ID and secret created
above. Tenant and bucket are part of each object address, not connection-wide
state.

```rust,no_run
use keldra::v1::{
    Durability, HeadObjectRequest, ObjectAddress, PutHeader, PutOperation,
    put_header,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut objects = keldra::connect_with_credentials(
        "http://127.0.0.1:50051",
        "example-client",
        std::env::var("KELDRA_OWNER_SECRET")?,
    )
    .await?;

    let address = ObjectAddress {
        tenant: "example".into(),
        bucket: "objects".into(),
        path: "greetings/from-rust.txt".into(),
    };

    let receipt = keldra::put_chunks(
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

## Clone objects and link mutable names

`CloneObject` publishes an independent destination version using an exact
source version's existing payload reference; it does not copy the bytes.
`LinkObject` creates a transparent path alias to one canonical target instead.
Writes through any linked name update that target, deleting the link unlinks
it, and the target cannot be deleted until every inbound link is removed.

The complete Rust example is in
[clients/rust/README.md](clients/rust/README.md#clone-bytes-or-link-a-mutable-name).
Clone and link require cluster protocol/storage capability `2/2`; complete the
0.15 activation runbook below before using them.

## Create a PersonalDB group

PersonalDB gives an application a witnessed, predecessor-linked log for SQLite
changesets. Source and standalone groups accept authorized appends; projection
groups are materialized explicitly from a source group. Group roles are
Zanzibar-authorized independently of ordinary object traffic.

Add the public client and canonical protocol types:

```sh
cargo add keldra@0.15.0 personaldb-protocol@0.2.2 serde_json
```

Use the same application credential created above to create a source group and
verify Keldra's signed descriptor:

```rust,no_run
use keldra::v1::{CreatePersonalDbGroupRequest, PersonalDbGroupKind};
use personaldb_protocol::{
    GroupDescriptor, PublicKeyTrustRecord, PublicKeyTrustStore, Sha256Digest,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = keldra::connect_channel("http://127.0.0.1:50051").await?;
    let token = keldra::exchange_client_credentials(
        channel.clone(),
        "example-client",
        std::env::var("KELDRA_OWNER_SECRET")?,
    )
    .await?;
    let mut personaldb = keldra::personaldb_client(channel, &token.access_token)?;

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
[`crates/keldra/examples/personaldb_qualification.rs`](crates/keldra/examples/personaldb_qualification.rs).

## Track billable usage

Accounting is opt-in for a whole bucket or one path prefix. Keldra maintains the
aggregate asynchronously and authorizes both configuration and reads through
Zanzibar. Each snapshot reports current object count and logical stored bytes,
cumulative accepted inbound and served outbound bytes, and a source checkpoint
so billing code can judge freshness.

Use an empty `path_prefix` for the whole bucket, or a canonical prefix such as
`customers/acme` for one application's billing boundary:

```rust,no_run
use keldra::{BearerToken, connect_channel, exchange_client_credentials};
use keldra::v1::accounting_service_client::AccountingServiceClient;
use keldra::v1::{EnableAccountingRequest, GetAccountingRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = connect_channel("http://127.0.0.1:50051").await?;
    let token = exchange_client_credentials(
        channel.clone(),
        "example-client",
        std::env::var("KELDRA_OWNER_SECRET")?,
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
[`crates/keldra/examples/accounting_qualification.rs`](crates/keldra/examples/accounting_qualification.rs).

## Use the S3 endpoint

The public endpoint on port `50051` accepts both native gRPC and standard
SigV4 path-style S3 requests. Use an Keldra application `client_id` as the AWS
access-key ID and its `client_secret` as the AWS secret-access key; Zanzibar
still decides what that application may do.

With the AWS CLI installed, the owner application created above can exercise
the minimum S3 surface directly:

```sh
export AWS_ACCESS_KEY_ID=example-client
export AWS_SECRET_ACCESS_KEY="$KELDRA_OWNER_SECRET"
export AWS_DEFAULT_REGION=eu-west-2
export KELDRA_S3_ENDPOINT=http://127.0.0.1:50051

aws --endpoint-url "$KELDRA_S3_ENDPOINT" s3api create-bucket \
  --bucket s3-demo
printf 'hello through S3\n' > keldra-data/s3-hello.txt
aws --endpoint-url "$KELDRA_S3_ENDPOINT" s3api put-object \
  --bucket s3-demo --key greetings/hello.txt \
  --content-type text/plain --body keldra-data/s3-hello.txt
aws --endpoint-url "$KELDRA_S3_ENDPOINT" s3api head-object \
  --bucket s3-demo --key greetings/hello.txt
aws --endpoint-url "$KELDRA_S3_ENDPOINT" s3api get-object \
  --bucket s3-demo --key greetings/hello.txt keldra-data/s3-downloaded.txt
cmp keldra-data/s3-hello.txt keldra-data/s3-downloaded.txt
aws --endpoint-url "$KELDRA_S3_ENDPOINT" s3api list-objects-v2 \
  --bucket s3-demo --prefix greetings/
aws --endpoint-url "$KELDRA_S3_ENDPOINT" s3api delete-object \
  --bucket s3-demo --key greetings/hello.txt
```

The same endpoint works with the official AWS SDKs. The Rust SDK qualification
uses one or three public endpoints and verifies every returned byte in
[`crates/keldra/examples/s3_qualification.rs`](crates/keldra/examples/s3_qualification.rs).

## Push and clone Git repositories

Keldra serves Git's smart HTTP protocol at
`/git/<tenant>/<bucket>/<repository>.git` on the same public port `50051`.
The first authenticated push creates the repository; subsequent pushes use CAS
to publish an ordered repository generation. New Git packs are immutable
ordinary Keldra objects; small reference batches and checkpoints make a warm
native repository incremental rather than rewriting the repository after every
push. Basic authentication accepts the same application client ID and secret
used by the gRPC and S3 APIs.

```sh
mkdir -p keldra-data/git-demo
git -C keldra-data/git-demo init --initial-branch=main
git -C keldra-data/git-demo config user.name "Keldra Example"
git -C keldra-data/git-demo config user.email "keldra@example.invalid"
printf '# Stored in Keldra\n' > keldra-data/git-demo/README.md
git -C keldra-data/git-demo add README.md
git -C keldra-data/git-demo commit -m initial

export KELDRA_GIT_URL=http://127.0.0.1:50051/git/example/objects/demo.git
export KELDRA_GIT_AUTH="$(printf '%s:%s' example-client "$KELDRA_OWNER_SECRET" | base64 | tr -d '\n')"
git -C keldra-data/git-demo \
  -c "http.extraHeader=Authorization: Basic $KELDRA_GIT_AUTH" \
  push "$KELDRA_GIT_URL" main
git -c "http.extraHeader=Authorization: Basic $KELDRA_GIT_AUTH" \
  clone --branch main "$KELDRA_GIT_URL" keldra-data/authenticated-clone
git -C keldra-data/authenticated-clone \
  -c "http.extraHeader=Authorization: Basic $KELDRA_GIT_AUTH" pull
```

To make pulls and clones public, enable public reads for the repository's
bucket through the authorized command shown below, then omit the authentication
header:

```sh
docker compose -f crates/keldra/docker-compose.yml exec \
  -e KELDRA_CLIENT_ID=example-client \
  -e KELDRA_CLIENT_SECRET="$KELDRA_OWNER_SECRET" keldra \
  keldra --endpoint http://127.0.0.1:50051 \
  set-bucket-public-read objects enabled

git clone --branch main "$KELDRA_GIT_URL" keldra-data/public-clone
```

Push always requires an authorized application. A real-client push, pull, and
public-clone qualification is kept in
[`crates/keldra/tests/git_gateway.rs`](crates/keldra/tests/git_gateway.rs).

## Authorization model

Authorization is enforced on the API; deployment topology is not a security
boundary.

1. The one unauthenticated credential RPC exchanges an application
   `client_id` and Argon2id-verified secret for a short-lived signed JWT.
2. The JWT identifies one stable application and tenant. It contains no trusted
   caller-supplied roles.
3. Every protected RPC evaluates that identity through Zanzibar relationships.
4. The protected `_keldra/system` realm governs Keldra administration. Customer
   realms use the same Zanzibar primitives for application-domain models, but
   cannot modify the system realm.

`WatchPrefix` validates the access token when the stream opens rather than
limiting the connection to the token's one-hour lifetime. While the stream is
open, Keldra continues to evaluate the admitted application against current
Zanzibar relationships on every source poll. Revoking its applicable access
therefore terminates the established stream with `PERMISSION_DENIED`.

Tenant roles are `owner`, `admin`, `reader`, `manage_tenant`, `read_tenant`,
`manage_buckets`, and `manage_authz`. Bucket roles are `owner`, `admin`,
`reader`, `writer`, `get`, `put`, `delete`, and `manage_policy`. A bucket owner
may delegate the narrowest useful role to another application; index results
are authorization-filtered before they are returned.

Tenant administrators rotate credentials only for applications in their own
tenant. If an application has lost its usable credential, a protected `_keldra`
system administrator can recover that existing identity without recreating the
tenant, application, roles, buckets, or objects. Put the replacement in a
mode-`0600` file so it never appears in process arguments:

```sh
umask 077
printf '%s' "$(openssl rand -hex 32)" > replacement.secret

keldra --endpoint https://keldra.example.com \
  --credentials-file /run/secrets/keldra-system-admin.json \
  recover-application-credential \
  --storage-tenant example \
  --app-id example-owner \
  --client-id example-client \
  --client-secret-file replacement.secret
```

Recovery requires both a caller from the protected `_keldra` tenant and
`system#manage_system`; ordinary tenant credentials cannot target another
tenant. The old secret stops minting tokens immediately. Bearer tokens issued
before recovery remain valid until their existing expiry.

A bucket owner can enable public reads with the CLI or
`AdministrationService.SetBucketPublicRead`:

```sh
docker compose -f crates/keldra/docker-compose.yml exec \
  -e KELDRA_CLIENT_ID=example-client \
  -e KELDRA_CLIENT_SECRET="$KELDRA_OWNER_SECRET" keldra \
  keldra --endpoint http://127.0.0.1:50051 \
  set-bucket-public-read objects enabled
```

A request with no bearer token is then evaluated as Keldra's built-in,
unmanageable anonymous application. It may read only where the owner
explicitly granted that bucket policy. Supplying an invalid token remains an
authentication error, and anonymous callers never bypass Zanzibar or gain
write, index-management, or administration access.

`QueryIndex` is the one index RPC that also accepts a missing bearer. An
anonymous query must set `QueryIndexRequest.tenant`; an authenticated query may
leave it empty because the signed token supplies the tenant. Supplying a tenant
with an authenticated query never changes identity—it must match the token.
Index creation, updates, discovery, and deletion always require credentials.

## Compare-and-swap and immutable data

Keldra exposes separate operations so intent is visible in code:

- `Put` always advances the path version.
- `PutIfAbsent` succeeds only if the path has never had a live value.
- `PutIfVersion(expected_version)` succeeds only against that exact current
  version.
- `PutImmutable` creates write-once content.
- `Delete` advances the path to a tombstone.
- `DeleteIfVersion(expected_version)` deletes only the expected current value.

Bucket policies can also reserve immutable or `PROGRAM_ONLY` path prefixes.
`PROGRAM_ONLY` is optional application policy: it prevents ordinary mutation
while still allowing an authorized atomic program to change the path. Atomic
programs can also use ordinary mutable or immutable paths; Keldra's durable
path reservations prevent concurrent ordinary writers from racing the atomic
decision.

## Atomic programs

An atomic program is a small, bounded JSON state-transition definition—not
uploaded executable code and not a transaction wrapper around ordinary object
traffic. Use one where several paths represent one application invariant, such
as moving a balance while appending a corresponding immutable ledger entry.

The lifecycle is deliberately explicit:

1. Put an immutable definition at `_keldra/programs/<name>@<version>`.
2. `HeadObject` it and retain the returned BLAKE3 hash.
3. Invoke the exact path and hash with a unique invocation ID and JSON bindings.
4. Keldra authorizes and reserves the declared paths, evaluates the bounded DSL on
   the nominated executor, and publishes every resulting head atomically.

Ordinary uploads—including large media—continue to use `Put`; they never enter
this orchestration path. The retained historical three-node Docker qualification
contains a complete two-document program and invocation fixture.

## Create and query indices

Typed JSON is the current materialized-index surface. The clean-break v6
pipeline consumes each source partition in order, keeps bounded hot work in
memory, shares extraction and physical recipes across logically equivalent
definitions, and publishes immutable partition roots at durable checkpoints.
Logical definitions are catalog state and equivalent definitions share physical
recipes. Query nodes materialize the exact published root vector needed for
their atomic cut and perform the
authoritative object/version check before returning a hit. The pipeline recovers
from the durable source journal after a restart, so its in-memory queues are
not a second authority.

The current operational scale runbook is
[index contention qualification](docs/qualification/index-contention.md). It
defines the CPU- and memory-normalized SSD qualification and the v6 telemetry
required to claim ingestion/index catch-up. The architecture contract is
[KELDRA-0020](docs/rfcs/keldra_0020_logical_index_catalog_and_shared_physical_projections.md).

### Current v6 resource controls

The indexing pipeline is CPU- and memory-scalable.
`KELDRA_INDEXING_CORES` sets the process indexing-worker ceiling and
`KELDRA_INDEX_PIPELINE_MEMORY_BYTES` bounds aggregate pipeline memory. Plan on
256 MiB per indexing core unless measured evidence supports another allocation.
`KELDRA_INDEX_WORKING_MEMORY_BYTES` is the hard aggregate ceiling and must admit
the pipeline plus `KELDRA_INDEX_QUERY_MEMORY_BYTES`. Segment formation uses
`KELDRA_INDEX_FLUSH_BYTES`, `KELDRA_INDEX_FLUSH_MAX_AGE_MILLIS`, and
`KELDRA_INDEX_FLUSH_MAX_OPERATIONS`; LSM debt uses
`KELDRA_INDEX_LSM_MAX_RUNS_PER_LEVEL` and
`KELDRA_INDEX_LSM_MAX_UNMERGED_BYTES_PER_LEVEL`. These are node-wide controls.

Single-node mutation group commit can be tuned without rebuilding the server.
Pass a strict JSON file with `--config-file` or `KELDRA_CONFIG_FILE`:

```json
{
  "single_node_group_commit": {
    "max_requests": 10,
    "max_operations": 5000,
    "max_inline_bytes": 67108864,
    "max_queued_requests": 64,
    "max_queued_operations": 8000,
    "max_queued_inline_bytes": 134217728,
    "group_dwell_microseconds": 250
  }
}
```

Each field also has a `KELDRA_SINGLE_NODE_GROUP_COMMIT_*` environment variable
and a matching `--single-node-group-commit-*` argument. Precedence is command
line, then environment, then JSON file, then the compiled default. The server
rejects zero or contradictory bounds and logs the complete effective group
configuration at startup.

`BulkWrite` accepts at most 1,000 operations and 64 MiB of encoded protobuf.
Its independent server deadline defaults to ten minutes and is configured with
`KELDRA_BULK_WRITE_TIMEOUT_SECONDS`; a shorter client `grpc-timeout` still wins.
This deadline is separate from `KELDRA_ATOMIC_PROGRAM_TIMEOUT_SECONDS`.
`QueryIndex` uses its own startup-configured request limit,
`KELDRA_INDEX_QUERY_TIMEOUT_SECONDS`, subject to a shorter client deadline.

Construct an authenticated index client from the same token used for objects:

```rust,no_run
use keldra::{connect_channel, exchange_client_credentials, index_client};

# async fn client() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let channel = connect_channel("http://127.0.0.1:50051").await?;
let token = exchange_client_credentials(
    channel.clone(),
    "example-client",
    std::env::var("KELDRA_OWNER_SECRET")?,
)
.await?;
let mut indices = index_client(channel, &token.access_token)?;
# let _ = &mut indices;
# Ok(())
# }
```

Call `CreateIndex` with a Typed JSON `CreateIndexRequest`, then `QueryIndex`
with a `TypedJsonIndexQuery`. Typed fields map to JSON Pointers and explicitly
declare their exact, prefix, range, order, facet, aggregate, or full-text
capabilities.

### Typed JSON API

For example, the Rust builder makes the field type and its permitted operations
explicit. This definition indexes `/status` as an exact, uninterpreted keyword;
it neither tokenizes nor stores the source JSON:

```rust,no_run
use keldra::v1::index_query::Query as QueryValue;
use keldra::v1::*;
use keldra::{KeywordField, PredicateExpression, TypedJsonIndexBuilder};

# async fn example(mut indices: keldra::v1::index_service_client::IndexServiceClient<tonic::service::interceptor::InterceptedService<tonic::transport::Channel, keldra::BearerToken>>) -> Result<(), tonic::Status> {
let definition = TypedJsonIndexBuilder::new("objects", "active-documents")
    .path_prefix("documents/")
    .content_type("application/json")
    .field(KeywordField::single("status", "/status").exact())
    .finish("create-active-documents")
    .expect("valid static index definition");

indices.create_index(definition).await?;

let response = indices.query_index(QueryIndexRequest {
    bucket: "objects".into(),
    index_name: "active-documents".into(),
    query: Some(IndexQuery {
        query: Some(QueryValue::TypedJson(TypedJsonIndexQuery {
            predicate: Some(
                PredicateExpression::equal("status", "active")
                    .expect("valid predicate")
                    .into_proto(),
            ),
            order: vec![],
            facets: vec![],
            aggregates: vec![],
        })),
    }),
    limit: 100,
    page_token: vec![],
    tenant: String::new(), // inferred from the authenticated caller
}).await?.into_inner();

for hit in response.hits {
    println!("{}", hit.address.unwrap().path);
}
println!("freshness: {:?}", response.freshness);
# Ok(())
# }
```

Typed JSON Date fields accept ISO-8601 strings by default, or one validated
POSIX strftime pattern selected in their definition. Values with an explicit
offset are normalized to UTC; offset-less values are interpreted as UTC. Keldra
stores exactly signed Unix epoch milliseconds in points and doc values, rejects
inputs with finer-than-millisecond precision rather than truncating them, and
does not retain the source date string. Date fields support exact and range
predicates, ordering, and facets, but not aggregates. Date predicate literals
use the field's configured format, and facet bucket values are formatted back
with that same format.

The v6 public-API and SSD qualification constructs, populates, paginates,
updates, deletes, restart-recovers, and load-tests Typed JSON definitions. It
exercises every field type and declared capability,
including multi-valued exact/facet/aggregate semantics, fielded text and phrase
search, identity-only hits, facets, and all five numeric aggregate operations.
The production-shaped qualification binds
those result checks to per-query logical reads, candidate counts, cursor seeks,
cache fetches, CPU, memory, and elapsed time from Keldra's operational signals.

## Run a three-node non-index qualification

The Docker qualification starts three real nodes, admits joiners with one-use
bundles, establishes peer mTLS, exercises replicated and erasure-coded storage,
and performs a rolling restart. Index qualification is a separate SSD-kit
phase:

```sh
KELDRA_IMAGE=ghcr.io/keldra-store/keldra:0.15.0 \
  ./scripts/qualify-three-node.sh
```

Production formation uses the same sequence:

1. Start the first node with `--run-system-bootstrap`, a stable
   `--peer-advertise` address, durable storage, and the operator-managed JWT
   signing key.
2. From an authorized active node, run
   `keldra prepare-node <node-id> <peer-address>`.
3. Copy the generated mode-`0600` join bundle to the configured joining node,
   delete the source copy, and start `keldra-server --join-bundle <path>` there.
4. Repeat for additional nodes. Set each node's storage weight to its usable
   capacity; weighted HRW moves only ownership affected by membership changes.

The public gRPC endpoint may sit behind an ordinary TLS terminator. Peer traffic
uses mandatory certificates created and rotated by the cluster.

### Start 0.15 on fresh volumes and activate its capabilities

Keldra 0.15 is a clean storage-format break. Start every 0.15 node with fresh
authoritative volumes; it does not open, migrate, or reuse a 0.14 data volume.
Mixed 0.14/0.15 clusters are unsupported. If application data must move from an
older cluster, keep that cluster separate and import the data through the public
API as new writes.

Inspect cluster capabilities. Activation is safe only when it reports active
protocol/storage `1/1`, target `2/2`, no blocking ACTIVE node IDs,
`activation_quiescent=true`, and `ready_for_target_activation=true`. Activate
`2/2` using that same response's exact placement term and index, then re-read
status and require active `2/2`. If placement changes, discard the old fence and
inspect again. Never force activation past a blocker.

```sh
keldra --endpoint "$KELDRA_ENDPOINT" get-cluster-capabilities

keldra --endpoint "$KELDRA_ENDPOINT" activate-cluster-capabilities \
  --protocol-version 2 \
  --storage-format 2 \
  --expected-placement-term "$PLACEMENT_TERM" \
  --expected-placement-index "$PLACEMENT_INDEX"
```

Supply the same system-operator credentials used for other protected
administration commands. The status command prints the exact activation command
when the cluster is ready.

Before admitting production traffic, smoke clone independence, link
write-through, target-delete fencing, unlink, and date queries. Never start a
0.14 binary against a volume initialized or touched by 0.15.

### Place storage by workload

`KELDRA_DATA_DIR` supplies convenient single-filesystem defaults. A new node
logs a bootstrap warning for every authoritative path left at its default so an
operator can distribute I/O before that storage root is pinned:

| Setting | Workload | Lifecycle |
| --- | --- | --- |
| `KELDRA_STATE_DIR` | Node identity, peer certificates, and Raft decisions | Authoritative; pinned at node initialization |
| `KELDRA_METADATA_DIR` | RocksDB manifests, SSTs, object metadata, Zanzibar, and journals | Authoritative; pinned |
| `KELDRA_METADATA_WAL_DIR` | RocksDB synchronous write-ahead log | Authoritative; pinned; prefer low-latency durable storage |
| `KELDRA_MAX_TOTAL_WAL_BYTES` | Shared RocksDB WAL flush target | Defaults to 50 GiB; administrator configurable |
| `KELDRA_PAYLOAD_DIR` | Payload-artifact SSTs and integrated BlobDB files for complete values and EC shards | Authoritative; pinned; prefer capacity and sequential throughput |
| `KELDRA_SCRATCH_DIR` | Index sort and merge scratch | Disposable; may use tmpfs or local scratch storage |
| `KELDRA_CACHE_DIR` | Materialized index and gateway caches | Disposable; may be replaced between restarts |
| `KELDRA_PENDING_UPLOAD_MAX_BYTES` | Process-wide unfinished-upload admission | Defaults to the configured maximum object size; persisted chunks remain GC-owned in RocksDB |

Each authoritative root carries a node-local identity marker. Restarting with a
missing mount, an empty replacement directory, or another node's disk fails
before RocksDB, Raft, or payload storage opens. Moving the same marked storage
root to a different mount path is safe. Scratch and cache paths are deliberately
unpinned; losing them may abort active work or cause cache refetching, but cannot
lose an acknowledged object.

The WAL must not use tmpfs. Complete payloads, erasure shards, and full chunks
of unfinished uploads are written directly to the integrated RocksDB payload
column family. Their installation, lifecycle, and garbage-collection records
are in the same database. Keldra does not create filesystem upload-spool files;
abandoned persisted chunks are reclaimed by the ordinary due-index GC.

## Public services

| Service | Purpose |
| --- | --- |
| `CredentialService` | Exchange durable application credentials for short-lived bearer tokens |
| `ObjectService` | Streaming writes, CAS, bulk/batch operations, reads, versions, listing, watches, policies, and atomic programs |
| `IndexService` | Create, update, inspect, list, delete, and query materialized indices |
| `PersonalDbService` | Create and authorize database groups, append protocol evidence, explicitly materialize projections, catch up, and transfer snapshots |
| `AccountingService` | Enable and query authorization-protected bucket or path-prefix usage aggregates |
| `AuthzService` | Manage customer Zanzibar realms, schemas, relationships, bindings, and checks |
| `AdministrationService` | Protected tenant, bucket, credential, role, and cluster lifecycle |

The versioned contracts are
[`keldra.proto`](crates/keldra-api/proto/keldra.proto) and
[`personaldb.proto`](crates/keldra-api/proto/personaldb.proto).

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
reference accounting, and each node's assigned v6 index partitions. Immutable
index artifacts are ordinary Keldra objects; local materialization is
disposable acceleration, not another authoritative storage plane.

The architecture contracts live in
[KELDRA-0009](docs/rfcs/keldra_0009_atomic_programs.md) and
[KELDRA-0010](docs/rfcs/keldra_0010_cluster_distribution.md). The current
clean-break native-segment index architecture is specified by
[KELDRA-0014](docs/rfcs/keldra_0014_native_segment_indexes.md). The approved
clean-break 0.15 payload layout, lifecycle, GC, replication boundary, and WAL
contract is specified by
[KELDRA-0018](docs/rfcs/keldra_0018_integrated_payload_storage.md).

## Build and qualify

Keldra requires Rust 1.96 or newer.

```sh
cargo fmt --all -- --check
cargo test --workspace
```

Build and qualify the container locally before CI:

```sh
KELDRA_IMAGE=keldra:local ./scripts/build-image.sh
KELDRA_IMAGE=keldra:local ./scripts/release-gates.sh image
# On the attested SSD binary kit; see docs/qualification/index-contention.md.
cd ~/keldra_experiments/kit
KELDRA_V6_SCALE_MODE=sustained ./qualify-index-v6-ssd-scale.sh
```

The v6 qualification kit verifies its source revision and checksums before
running. It records offered, accepted, source, selected, prepared, projected,
sealed, and checkpointed throughput; source lag and drain; query latency; and
CPU, RSS, WAL/store-write evidence under `~/keldra_experiments`. The
single-node and three-node wrappers qualify non-index storage, authorization,
and cluster behavior only.

Keldra 0.15 deployments start on fresh authoritative and derived-index volumes.
Current operational boundaries are collected in the [known
limitations](docs/known-limitations.md).

Apache-2.0 licensed.
