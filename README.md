# Anvil

Anvil 0.5 is a small, versioned object store with an explicit opt-in layer for
bounded atomic application commands.

The storage kernel understands paths and opaque bytes. Every successful write
creates an immutable version and moves one current head. It supports exact
compare-and-swap, create-once namespaces, idempotent commands, independent bulk
writes, content-addressed streaming uploads, and same-snapshot batch reads.
`ListObjects` provides stateless pages of current live paths directly from the
current-head keyspace.

Applications that need several exact paths to change together write a bounded
deterministic program as an immutable ordinary object below `_anvil/programs/`,
then explicitly invoke that pinned object. The singleton executor locks its
expanded paths locally in canonical order and evaluates against the latest
committed state. A compact Raft decision makes an already durable prepared
bundle visible. Raft never carries object bodies, path inventories, locks,
waiters, or program definitions.

`InvokeProgram` has one absolute execution budget covering lock acquisition,
evaluation, commit and finalization. The startup-only maximum defaults to 30
seconds and is configured with `ANVIL_ATOMIC_PROGRAM_TIMEOUT_SECONDS` or
`--atomic-program-timeout-seconds`. A standard client gRPC deadline may shorten
that budget but cannot extend it; expiry returns gRPC `DEADLINE_EXCEEDED`.

## Implementation status

The complete 0.5.0 capability is in release qualification. The production
surface includes authenticated object/CAS/bulk operations, immutable and
`PROGRAM_ONLY` policy, typed Zanzibar administration and application realms,
explicit bootstrap and credential exchange, public atomic-program invocation
and recovery, and bounded resumable `WatchPrefix`. Functional release evidence
includes the full local suite, crash/manual QA, and both container
architectures. The pinned Developer Defence OSV import is the immediate
post-release performance baseline; misses are profiled and corrected after the
immutable 0.5.0 tag rather than delaying its functional release.

## Deliberate 0.5 break

There is no compatibility with the 0.4 transaction or MVCC API and no in-place
upgrade from its storage format. The following concepts are gone:

- `BeginTransaction`, `CommitTransaction`, and `RollbackTransaction`;
- transaction drafts and staged public writes;
- certification, read sets, range observations, and snapshot versions;
- arbitrary cross-partition atomic batches;
- payloads or product rows in Raft.

Export data through the old release and import it into a new 0.5 store.

## API layers

| Layer | Operations | Guarantee |
| --- | --- | --- |
| Opaque object core | `StartPut` → `Put` → `PutEnd`, `Delete`, `DeleteIfVersion` | Streamed bytes remain invisible until `PutEnd`; each publication moves one exact-path head |
| Throughput | `BulkWrite`, `BatchGet` | Independent write outcomes; one read snapshot |
| Discovery | `ListObjects` | Zanzibar-authorized lexical pages of current live paths |
| Policy | create-once and `PROGRAM_ONLY` prefixes | Write-once children, or mutation admitted only through atomic programs |
| Atomic programs | immutable object under `_anvil/programs/`, then `InvokeProgram` | One bounded deterministic state transition orchestrated by the nominated executor |
| Invalidations | `WatchPrefix` | Bounded unordered at-least-once notice to reread current state |
| Authorization | schemas, realms, tuples, checks and typed administration | Zanzibar evaluation at one explicit current revision |
| Workflows | application-owned saga | External-service coordination |

Uploading an MP3 through `StartPut` and streaming `Put` is an ordinary blob
operation. It does not invoke a program, and the staged bytes remain invisible.
Only `PutEnd` publishes the object and changes its visible exact-path head.
Sealed uploads reserve their content-addressed bytes for 24 hours by default.
Anvil runs one full garbage-collection pass at startup and then hourly. Set
`ANVIL_AWAITING_PUBLISH_TTL_SECONDS` (or
`--awaiting-publish-ttl-seconds`) to change the unpublished inactivity limit;
the value must be non-zero. Sealing identical content refreshes its
`updated_at` value and therefore its inactivity deadline. Published content is
retained by its reference count. Count-zero and awaiting-publication content is
removed only after it has also been inactive for the configured threshold, so
a valid ready upload can still publish deduplicated bytes after an intervening
delete.

`ListObjects` uses a literal UTF-8 prefix, defaults to 100 paths per page, and
accepts at most 1,000. Pass the last returned path back as the exclusive
`start_after` cursor when `has_more` is true. Pages are read committed rather
than one snapshot held across requests. The RocksDB `heads` column family uses
`[format version][tenant ID][bucket ID][raw UTF-8 path]`, so the implementation
seeks directly to the requested literal path prefix and stops at the end of
that contiguous range. It maintains no duplicate listing projection or side
index.

## Authentication

Every protected RPC accepts a one-hour bearer token minted by
`ExchangeClientCredentials`. The exchange request contains a long-lived
application secret, so production deployments must put the gRPC endpoint
behind TLS termination. Plaintext transport is suitable only for a trusted
local development loop.

The server requires `--token-signing-key-file` (or
`ANVIL_TOKEN_SIGNING_KEY_FILE`) on every start. It must name a regular,
non-symlink file with mode `0600` containing between 32 and 4096 key bytes.
Anvil reads but never persists or logs this operator-managed key. The static
shared API token and unauthenticated server mode do not exist in 0.5.
Rotating or disabling an application credential stops future exchanges;
already-issued tokens remain valid until their one-hour expiry.

## Repository

- `crates/anvil-store`: opaque version/head storage, blobs, CAS, policies and bulk operations.
- `crates/anvil-atomic-program`: the one bounded JSON program interpreter.
- `crates/anvil-consensus`: compact executor nomination and publication decisions.
- `crates/anvil-api`: the single generated gRPC contract shared by server and clients.
- `anvil`: server transport and integration.
- `clients/rust`: thin authenticated Rust transport.
- `docs/rfcs/anvil_0009_atomic_programs.md`: complete 0.5 architecture and invariants.
- `docs/known-limitations.md`: explicit 0.5.0 limitations that operators and clients must account for.

## Development

```sh
cargo fmt --all -- --check
cargo test --workspace
```

Before creating the `0.5.0` Git tag, qualify both release images locally. The
architecture-specific names below remain in the local Docker daemon; they are
never pushed to GHCR.

```sh
ANVIL_DOCKER_PLATFORM=linux/amd64 \
ANVIL_IMAGE=anvil:0.5.0-local-amd64 \
./scripts/build-image.sh
ANVIL_IMAGE=anvil:0.5.0-local-amd64 ./scripts/release-gates.sh image

ANVIL_DOCKER_PLATFORM=linux/arm64 \
ANVIL_IMAGE=anvil:0.5.0-local-arm64 \
./scripts/build-image.sh
ANVIL_IMAGE=anvil:0.5.0-local-arm64 ./scripts/release-gates.sh image
```

Each image is compiled from source inside a `rust:1.96-trixie` builder for its
target platform and runs on `debian:trixie-slim`. Release publication rebuilds
the same Dockerfile as the single public multi-platform image
`ghcr.io/worka-ai/anvil:0.5.0`. There are no public architecture-specific or
`v`-prefixed image tags.

## First start

Anvil has no insecure or static-token mode. Create an operator-managed signing
key, keep it mode `0600`, and run bootstrap exactly once:

```sh
head -c 64 /dev/urandom > anvil-token-signing-key
chmod 0600 anvil-token-signing-key
anvil-server \
  --data-dir ./anvil-data \
  --token-signing-key-file ./anvil-token-signing-key \
  --run-system-bootstrap
```

The server prints the exact generated credential path, normally
`./anvil-data/system-bootstrap-credential.json`. Copy that mode-`0600` file to
the operator's secret store and delete the generated copy after provisioning.
The CLI exchanges it for a one-hour access token automatically:

```sh
ANVIL_NEW_CLIENT_SECRET='a-new-secret-containing-at-least-32-bytes' \
anvil --credentials-file ./copied-bootstrap-credential.json \
  provision-tenant acme acme-owner acme-owner-client

ANVIL_CLIENT_ID=acme-owner-client \
ANVIL_CLIENT_SECRET='a-new-secret-containing-at-least-32-bytes' \
anvil create-bucket objects
```

Credential exchange carries a long-lived secret and requires TLS termination
in production. The container runs as UID 10001; a bind-mounted signing key must
therefore be readable by that UID while remaining mode `0600`, or be supplied
by a secret mount that sets `uid=10001,mode=0600`.

Anvil is licensed under Apache-2.0.
