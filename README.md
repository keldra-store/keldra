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
| Materialized indices | 0.10.0 | Path, object metadata, typed JSON, full text, vector, hybrid, Git-source, and tensor indices |
| Rust client | 0.10.0 | Credential exchange, authenticated clients, streaming upload helpers, and the complete generated gRPC API |
| PersonalDB, public reads, accounting, S3 and Git | 0.10.0 | Protocol-native PersonalDB groups and projections, authorized usage aggregates, opt-in anonymous reads, and standard S3/Git gateways |
| Online cluster growth | 0.10.0 | Large objects use complete replicas below the configured erasure width, then move online to the fixed erasure profile as nodes join |
| Shared public listener | 0.10.0 | Native gRPC, S3, Git, administrative APIs and configured HTTP plugins share one authorized public endpoint; peer mTLS remains isolated |
| Streaming succinct indices | 0.10.0 | Bounded per-kind construction memory, incremental immutable runs, streaming compaction, `sux`-based merged structures, and lazy block materialization |
| Sparse index coordination | 0.10.0 | Non-blocking startup, transactional definition locators, routed change journals, scoped recovery, resumable accounting, and budgeted maintenance |
| Scalable bulk indices | 0.10.0 | Direct bounded bulk builds, packed artifacts, stable compressed postings, authorized rebuilds, and lossless journal backpressure |
| Native segment indices | 0.10.0 | Keldra-owned immutable segments, exact predicate intersection, optional physical ordering, stable cursors, bounded arbitrary sorting, and shared query-memory admission |
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
this orchestration path. A complete two-document program and invocation is kept
executable in
[`scripts/qualify-three-node.sh`](scripts/qualify-three-node.sh).

## Create and query indices

Indices are bucket-local definitions scoped by an optional path prefix and
content type. Each definition's HRW-selected writer consumes every node's
ordered source journal, writes bounded immutable native segments, merges
deterministic size tiers, and publishes complete generations through the
ordinary object store. Seekable postings and numeric points intersect selective
predicates before typed doc values are read; an optional definition-time
physical order supports early termination and true search-after pagination.
Hits identify authoritative objects instead of copying source fields into the
index. Up to three HRW-selected query replicas materialize only the blocks a
query needs through the shared bounded cache.
Construction and query memory share one hard process-wide ceiling, so corpus
size does not become unaccounted heap. Per-kind construction settings and the
query setting remain fair-share planning targets; active work can borrow idle
capacity without crossing the aggregate ceiling. Query responses always
include freshness evidence; while a new complete generation is building,
Keldra continues serving the preceding complete generation rather than exposing
a partial one.

The startup default is 256 MiB of construction memory and at most four
compaction lanes for each index kind, shared by every local definition of that
kind, with four Rayon workers across the process. Set
`KELDRA_INDEX_BUILDER_MEMORY_BYTES_PER_KIND` for the common memory fallback and
`KELDRA_INDEX_RAYON_WORKERS` for the process-wide worker ceiling. Every kind can
override the fallback and lane cap independently with
`KELDRA_INDEX_<KIND>_BUILDER_MEMORY_BYTES` and
`KELDRA_INDEX_<KIND>_COMPACTION_MAX_LANES`, where `<KIND>` is `PATH`,
`METADATA_FILTER`, `TYPED_JSON`, `FULL_TEXT`, `VECTOR`, `HYBRID`, `GIT_SOURCE`,
or `TENSOR`.

Projection concurrency also defaults to four lanes per kind. Configure it with
`KELDRA_INDEX_PATH_PROJECTION_MAX_LANES`,
`KELDRA_INDEX_METADATA_FILTER_PROJECTION_MAX_LANES`,
`KELDRA_INDEX_TYPED_JSON_PROJECTION_MAX_LANES`,
`KELDRA_INDEX_FULL_TEXT_PROJECTION_MAX_LANES`,
`KELDRA_INDEX_VECTOR_PROJECTION_MAX_LANES`,
`KELDRA_INDEX_HYBRID_PROJECTION_MAX_LANES`,
`KELDRA_INDEX_GIT_SOURCE_PROJECTION_MAX_LANES`, and
`KELDRA_INDEX_TENSOR_PROJECTION_MAX_LANES`. These caps share the process Rayon
worker pool and the corresponding kind's construction-memory budget.

`KELDRA_INDEX_MAX_SEGMENTS_PER_TIER` and
`KELDRA_INDEX_MAX_UNMERGED_BYTES_PER_TIER` bound merge debt, with per-kind
overrides following the same naming pattern. `KELDRA_INDEX_QUERY_MEMORY_BYTES`
is the query fair share. `KELDRA_INDEX_WORKING_MEMORY_BYTES` optionally sets the
hard aggregate query/build/compaction ceiling; without it, Keldra uses the
checked sum of the query and eight per-kind shares (2.5 GiB with defaults).
Queries and builders can borrow idle bytes and derive their workspace from the
actual grant, while queued mandatory work retains FIFO priority.

`BulkWrite` accepts at most 1,000 operations and 64 MiB of encoded protobuf.
Its independent server deadline defaults to five minutes and is configured with
`KELDRA_BULK_WRITE_TIMEOUT_SECONDS`; a shorter client `grpc-timeout` still wins.
This deadline is separate from `KELDRA_ATOMIC_PROGRAM_TIMEOUT_SECONDS`, so
ordinary ingestion on slower durable storage does not weaken atomic-program
execution bounds.

Actual compaction concurrency is the minimum of that kind's configured cap,
the process worker ceiling, the number of deterministic key ranges, and the
memory admission `1 + floor((kind budget - shared workspace) / incremental
lane workspace)`. A cap of one preserves the sequential merge path. Cache
limits remain separate: `KELDRA_INDEX_DISK_CACHE_BYTES` controls the shared
disposable disk cache and `KELDRA_INDEX_MEMORY_PERCENT` caps aggregate in-flight
block materialization. Immutable cache files are read through mmap, allowing
the operating system to retain clean hot pages and reclaim them under pressure.
`QueryIndex` has a separate five-minute server maximum so a cold first page can
materialize the required blocks without inheriting the ordinary 30-second RPC
limit. Set `KELDRA_INDEX_QUERY_TIMEOUT_SECONDS` at startup to tune it; a shorter
client `grpc-timeout` always wins.

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

Call `CreateIndex` with a `CreateIndexRequest`, then `QueryIndex` with the
matching query variant. These are the eight engines and their minimal useful
definitions:

| Engine | Definition | Query | Source object shape |
| --- | --- | --- | --- |
| Path | `PathIndexSpec {}` | `PathIndexQuery { prefix, start_after }` | Any object |
| Metadata filter | fields such as `path`, `content_type`, `content_length` | Predicates over retained head fields | Any object |
| Typed JSON | typed fields mapped to JSON Pointers with explicit exact, prefix, range, order, facet, aggregate, or full-text capabilities | Capability-checked predicates, ordering, facets, and numeric aggregates | `{"status":"active"}` |
| Full text | named text fields mapped to JSON Pointers | Text or phrase query | `{"body":"durable journal delivery"}` |
| Vector | JSON Pointer, dimensions, cosine/dot/Euclidean metric | A vector with the declared dimensions | `{"embedding":[1.0,0.0,0.0]}` |
| Hybrid | Full-text and vector specifications plus weights | Text and vector together | `{"title":"rust search","embedding":[1.0,0.0,0.0]}` |
| Git source | Repository ID | Commit ID and exact/prefix tree path | Git manifest containing repository, commit, tree path, object ID, pack path/version, offset, and length |
| Tensor | Model ID | Tensor name | Tensor manifest containing model, tensor name, source path/version, offset, length, dtype, and shape |

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

The repository's public-API qualification constructs, populates, paginates,
updates, deletes, rebuilds, and restart-verifies all eight variants in
[`crates/keldra/examples/cluster_index_qualification.rs`](crates/keldra/examples/cluster_index_qualification.rs).
Tensor coverage separately removes a referenced result object while its source
manifest remains current, exercising the public no-stale-version boundary
rather than relying only on the next index generation.
Its Typed JSON case exercises every field type and declared capability,
including multi-valued exact/facet/aggregate semantics, fielded text and phrase
search, identity-only hits, facets, and all five numeric aggregate operations.
The production-shaped qualification binds
those result checks to per-query logical reads, candidate counts, cursor seeks,
cache fetches, CPU, memory, and elapsed time from Keldra's operational signals.

## Run a three-node cluster

The local qualification starts three real nodes, admits joiners with one-use
bundles, establishes peer mTLS, exercises replicated and erasure-coded storage,
queries every index type, and performs a rolling restart:

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

### Upgrade from 0.14 and activate 0.15 capabilities

The cluster and data-peer protocols changed in 0.15. Stop all 0.14 nodes before
starting any 0.15 node; mixed-version operation is unsupported. First quiesce
mutations, atomic invocations, and membership changes, and take a consistent
backup of every node's data and operational keys. Install 0.15 everywhere,
restart the complete cluster, and keep writes drained while ACTIVE nodes attest
support.

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

Before resuming traffic, smoke clone independence, link write-through,
target-delete fencing, unlink, and date queries. Rollback requires restoring the
complete pre-upgrade backup; never start 0.14 against storage touched by 0.15.

### Place storage by workload

`KELDRA_DATA_DIR` supplies convenient single-filesystem defaults. A new node
logs a bootstrap warning for every authoritative path left at its default so an
operator can distribute I/O before that storage root is pinned:

| Setting | Workload | Lifecycle |
| --- | --- | --- |
| `KELDRA_STATE_DIR` | Node identity, peer certificates, and Raft decisions | Authoritative; pinned at node initialization |
| `KELDRA_METADATA_DIR` | RocksDB manifests, SSTs, object metadata, Zanzibar, and journals | Authoritative; pinned |
| `KELDRA_METADATA_WAL_DIR` | RocksDB synchronous write-ahead log | Authoritative; pinned; prefer low-latency durable storage |
| `KELDRA_PAYLOAD_DIR` | Canonical blobs, EC shards, and authoritative index artifacts | Authoritative; pinned; prefer capacity and sequential throughput |
| `KELDRA_SCRATCH_DIR` | Index sort and merge scratch | Disposable; may use tmpfs or local scratch storage |
| `KELDRA_CACHE_DIR` | Materialized index and gateway caches | Disposable; may be replaced between restarts |
| `KELDRA_UPLOAD_SPOOL_DIR` | Unfinished, unacknowledged upload bytes | Disposable; may use bounded tmpfs |
| `KELDRA_UPLOAD_SPOOL_MAX_BYTES` | Process-wide unfinished-upload capacity | Defaults to the configured maximum object size |

Each authoritative root carries a node-local identity marker. Restarting with a
missing mount, an empty replacement directory, or another node's disk fails
before RocksDB, Raft, or payload storage opens. Moving the same marked storage
root to a different mount path is safe. Scratch, cache, and upload-spool paths
are deliberately unpinned; losing them may abort active work or cause cache
refetching, but cannot lose an acknowledged object.

The WAL must not use tmpfs. Identified blob/shard staging and garbage-collection
recovery remain under `KELDRA_PAYLOAD_DIR`; only raw uploads which have not
reached `PutEnd` use the disposable spool.

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
reference accounting, and the single writer for each materialized index. Index
artifacts are ordinary Keldra objects; local materialization is disposable
acceleration, not another authoritative storage plane.

The architecture contracts live in
[KELDRA-0009](docs/rfcs/keldra_0009_atomic_programs.md) and
[KELDRA-0010](docs/rfcs/keldra_0010_cluster_distribution.md). The current
clean-break native-segment index architecture is specified by
[KELDRA-0014](docs/rfcs/keldra_0014_native_segment_indexes.md).

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
KELDRA_QUALIFICATION_MODE=release \
KELDRA_QUALIFICATION_INDEX_KIND_BUDGET_BYTES=268435456 \
KELDRA_QUALIFICATION_INDEX_COMPACTION_MAX_LANES=4 \
KELDRA_QUALIFICATION_INDEX_RAYON_WORKERS=4 \
KELDRA_QUALIFICATION_INDEX_MAX_ANONYMOUS_GROWTH_BYTES=2147483648 \
  KELDRA_IMAGE=keldra:local ./scripts/qualify-single-node.sh
KELDRA_QUALIFICATION_MODE=release \
  KELDRA_IMAGE=keldra:local ./scripts/qualify-three-node.sh
```

The production-shaped index qualification generates 839,980 private-data-free
JSON objects with 12 indexed fields, verifies every live path and version after
updates and deletes, and records phase-by-phase RSS and anonymous-memory peaks.
The wrapper scripts above build the release-mode qualification client, bind the
evidence to the exact image revision, and run the client directly so Cargo does
not hold the shared build lock during the long qualification.

The pinned `sux` and Rayon dependencies, resolved versions, and license choices are
recorded in [the index dependency record](docs/dependency-licenses.md).

Keldra 0.11 deployments start with new volumes. Format-v4 index definitions and
native segment artifacts are built from authoritative source objects rather
than migrated from format v3. Current operational boundaries are collected in
the [known limitations](docs/known-limitations.md).

Apache-2.0 licensed.
