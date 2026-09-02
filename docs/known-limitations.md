# Keldra known limitations

## Current v6 index boundary

Keldra 0.16 starts on fresh authoritative and derived-index volumes. The
partition-owned Typed JSON pipeline, logical catalog, recovery, root-vector
query cut, retention, and materialization contracts are specified by
[KELDRA-0020](rfcs/keldra_0020_logical_index_catalog_and_shared_physical_projections.md).
Its active operational evidence and configuration are the v6 SSD runbook in
[index contention qualification](qualification/index-contention.md) and the
v6 metrics in [observability](observability.md).

| Area | Current v6 boundary |
| --- | --- |
| Index surface | Typed JSON fields can declare exact, prefix, range, order, facet, aggregate, and full-text capabilities. |
| Resource scaling | Indexing uses one aggregate memory-first pipeline per node, controlled by `KELDRA_INDEX_PIPELINE_MEMORY_BYTES` and `KELDRA_INDEXING_CORES`. `KELDRA_INDEX_WORKING_MEMORY_BYTES` must admit that pipeline plus `KELDRA_INDEX_QUERY_MEMORY_BYTES`. Memory is not reserved per logical definition. The sizing baseline is 256 MiB per indexing core; operators should change it only with workload evidence. |
| Recovery | Prepared queues and hot extraction state are intentionally volatile. After a restart, a producer reconstructs work from its durable source-journal checkpoint. A pipeline which was behind may therefore reread durable journal and object state before it catches up; no in-memory queue is authoritative. |
| Catalog scale | Logical definitions are compact catalog rows. Equivalent definitions share a physical recipe, extraction, and immutable segment work. Distinct physical recipes still require distinct projection and segment output, so P rather than logical definition count D is the steady-state indexing multiplier. |
| Query materialization | Queries execute against an exact published root vector for one atomic cut. A cold query node may need to fetch and materialize immutable blocks before execution, increasing first-query latency without weakening cut consistency or the authoritative object/version check. |
| Backpressure | Source journals remain bounded durable recovery authority. If indexing cannot advance its durable checkpoint before retained evidence reaches the configured journal capacity, ingress can be backpressured rather than silently dropping index work. |
| Qualification | Throughput and catch-up claims require `scripts/qualify-index-v6-ssd-scale.sh` on the attested SSD kit. The ordinary single-node and three-node wrappers intentionally contain no index phase. Sustained qualification covers D1/D64/D1K/D10K/D250K, P1/P4/P16/P64, worker/memory scaling, and 1 KiB plus 96 KiB objects. |

## Capabilities introduced in 0.5.x

The following limits are grouped by the release which introduced each
capability. Unless a newer section above explicitly replaces one, they remain
the current boundary.

## Usage accounting in 0.5.3

Accounting transfer totals describe payload bytes accepted for upload and
selected for download; they are not wire-exact cancellation counters. A native
`GetObject` records the selected object's declared payload length before the
response stream is fully consumed, so a client that disconnects early can be
charged for bytes it did not receive. The S3 gateway records inbound bytes only
after a successful `PutEnd`. Failed, disconnected, and replayed requests can
therefore differ from socket-level traffic. Applications should use these
figures for product usage bands rather than network-forensics reconciliation.

Each node buffers transfer deltas briefly before writing its cumulative source
object. A process failure in that interval can undercount traffic. Enabling an
accounting prefix is discovered asynchronously on other nodes, so traffic sent
there immediately after `EnableAccounting` can also precede the local meter.
The returned freshness structure describes object-journal coverage; it does
not claim wire-exact transfer capture.

An accounting worker uses a bounded-memory current-head scan only for its
initial baseline or after retained journal evidence is unavailable. The 0.5.3
scan is paged rather than one cluster-wide snapshot. Sustained writes racing a
cold multi-page baseline can therefore require a later operator restart during
a quiet interval to establish an exact base. The steady-state path is ordered,
incremental, and does not retain one in-memory entry per object.

## Request deadline coverage

Index definition create, update, inspect, list, delete, and rebuild requests use
one absolute deadline: the shorter of the client `grpc-timeout` and the
startup-configured 30-second maximum. `QueryIndex` instead uses the shorter of
the client deadline and `KELDRA_INDEX_QUERY_TIMEOUT_SECONDS`, whose default is
five minutes. `BulkWrite` independently uses the shorter client deadline and
`KELDRA_BULK_WRITE_TIMEOUT_SECONDS`, ten minutes by default, so a valid
64 MiB request does not inherit the atomic-program execution maximum on slow
storage. Atomic link operations embedded in a bulk retain the atomic-program
maximum. The same remaining budget is propagated across object and peer calls.
These maxima are deliberately not transport-wide timeouts because `Put` and
`WatchPrefix` are long-lived streams. Local authorization, administration, and
credential unary requests still rely on their client or external TLS terminator
to supply a deadline; extending the shared deadline wrapper to those existing
services is deferred.

Authorization-aware index pagination batches the common case where every
candidate is visible. If Zanzibar filters or reorders a candidate, Keldra falls
back to a one-candidate scan so a continuation never exposes an unauthorized
position. A large, heavily filtered cold query can therefore exhaust its
configured query request budget; the transport timeout is safely retryable.

## Minimum PersonalDB surface in 0.5.3

PersonalDB 0.5.3 provides source, standalone and bounded mirror-projection
groups; predecessor-linked append and catch-up; explicit projection
materialisation; snapshots; signed protocol evidence; and tenant-scoped group
roles. Projection materialisation supports only the deterministic mirror mode
in this release. More specialised application projections remain future
capabilities.

Client proposal, voter acknowledgement and admission evidence is retained in
each witnessed commit certificate, but 0.5.3 only checks that those opaque
values are present. It does not yet evaluate an application-specific voter or
admission trust policy before witnessing the commit.

`ListPersonalDbGroups` applies per-group Zanzibar authorization while scanning
ordinary manifest objects. It keeps scanning internally until the requested
authorized page is full or the source is exhausted, and continuation metadata
is derived only from an authorized result. A bucket containing many groups
that the caller cannot see can therefore require substantial scan and
authorization work before the call returns.

## Failed minority metadata candidates

A mutation can be durably present on fewer than its required logical-record
replicas if the remaining replicas become unavailable after the coordinator's
local commit. When no quorum can prove either that exact candidate or a
different valid successor, ordered reference delivery cannot determine whether
the candidate's journal effect is authoritative. Keldra fails closed at that
source position: destination cursors do not advance beyond it, the source
journal prefix is retained, and reference garbage collection remains disabled
for the affected work. If the retained journal reaches its configured bound,
later mutations that need to append a source event receive backpressure.

A quorum reporting only that the candidate is absent is not enough to discard
it because a previously issued replica apply may still complete. Mutations
already proven by a metadata quorum and other ACTIVE nodes remain unaffected;
Keldra does not trade reference correctness for recovery availability.

After metadata quorum is available again, retrying the unknown-outcome
operation with the same command ID allows the deterministic candidate to
complete. Restoring a replica that holds the required lineage can likewise
provide the missing proof. If neither the original candidate nor a
quorum-proven valid successor can be recovered, Keldra 0.6.0 has no unsafe
operator bypass; automatic lineage reconciliation for that case is deferred.

## Legacy one-node reference journal recovery

The upgrade recovery path accepts a proofless 0.5.3 object-head event only when
its source is local and that source is still the sole committed ACTIVE node. It
recognizes that the one-node fast path already applied the reference effect and
advances the cursor without applying the count again. Every other missing-proof
case continues to fail closed.

Consequently, an existing one-node installation must complete startup reference
reconciliation before beginning an online ADD. Once it has, large objects use
complete replicas while membership is undersized and the normal typed handoff
supports online growth from one to two to three ACTIVE nodes. Skipping that
ordered recovery boundary is unsupported; it does not cause Keldra to infer or
weaken reference-proof semantics.

## Online ADD boundaries in 0.5.4

An online ADD briefly pauses mutable public and peer operations for the final
handoff snapshot. A large upload may finish sending its bytes before that
pause, then receive retryable `UNAVAILABLE` when it attempts `PutEnd` during the
cutover. Retrying the upload after the membership operation completes is safe;
unpublished prepared bytes remain subject to the ordinary 24-hour GC grace.

ADD copies the new metadata replica set but does not proactively remove every
former metadata replica in 0.5.4. Those extra records are not authoritative and
do not affect reads or quorum decisions, but consume additional disk
proportional to moved records until a later maintenance capability retires
them.

Large complete-copy reads in an undersized membership use any valid ACTIVE
copy but do not proactively reconstruct a missing selected complete replica.
A successful `REPLICATED` write still proves two distinct durable copies, and
the typed ADD handoff restores the selected placement when another node joins.

Read repair can recreate an artifact after manual loss or corruption before
its local authoritative lifecycle record has arrived. That artifact remains
`AWAITING_PUBLISH` and is eligible for ordinary GC after 24 hours if reference
delivery or a membership handoff never reinstalls the lifecycle. Normal writes
and normal online growth install the lifecycle and are unaffected.

In a two-node cluster with object versioning enabled, an explicit deletion of
a non-current retained version can become unavailable if the coordinator's
local commit succeeds but its peer apply fails. That operation changes the
retained descriptor set without advancing the stamped head, so the `2/2` read
cannot prove which complete snapshot is the direct successor and fails closed
instead of guessing. Ordinary puts, overwrites, whole-object deletes, current
version deletion, one-node operation, and `2/3` clusters are unaffected.
Applications that require this maintenance operation should defer it while a
cluster has exactly two ACTIVE nodes; a later capability can add explicit
lineage for retained-history maintenance.

## First custom-realm binding in a multi-node 0.5.1 cluster

The first schema binding for a custom Zanzibar realm must atomically create
the realm binding and its protected-system ownership grant. Keldra 0.5.1 keeps
that guarantee on a one-node cluster, but rejects the first binding with
`UNAVAILABLE` when more than one node is active. Existing realms can be
rebound and used normally across the cluster. A later capability must add one
bounded cross-Zanzibar operation before enabling first binding on multi-node
clusters; 0.5.1 does not weaken the atomic ownership guarantee.

## Cluster lifecycle operations

Keldra supports genesis, authorized node preparation, learner catch-up, typed
ownership handoff, and online ADD activation for ordinary cluster formation.
The bounded Raft state machine also validates removal, capacity-reweight, and
peer-certificate-overlap transitions, but 0.5.4 does not expose public
operations that start those transitions because their corresponding online
ownership handoff and live TLS-reload orchestration are not yet complete.
Operators must not submit those internal Raft commands directly.

There is no public drain or detailed cluster-health RPC in 0.5.4. Public
listener availability is the readiness boundary: Keldra binds it only after
membership, serving-fence, bootstrap, authorization, atomic recovery, and
ordered reference startup checks complete. Normal process termination stops
the public listener before the peer runtime and flushes the local store; it
does not remove the node from committed membership.

## Transient reference-delivery cursor skew in 0.5.1

Under concurrent cluster activity, ordered reference delivery can transiently
observe a destination cursor one position beyond the source-tail snapshot and
pause that source until a later retry. Keldra fails closed while this condition
exists: blob garbage collection remains disabled and source-journal entries
remain retained, so acknowledged object data is not collected prematurely. A
persistent condition can delay reference-count convergence and eventually
apply bounded-journal write backpressure; it does not weaken `LOCAL` or
`REPLICATED` acknowledgement guarantees.

## Tenant schema catalogue handoff in 0.5.1

Node admission transfers each tenant's complete Zanzibar schema catalogue as
one typed private handoff unit; individual `TenantSchema` records are discovery
keys and are never independently repaired. The 0.5.1 private typed-message
limit bounds one encoded catalogue to 16 MiB. Admission fails closed if a
catalogue exceeds that size. A later release can stream unusually large
catalogues without changing their storage format or quorum semantics.

## Coordinator error detail in 0.5.1

Bulk writes correctly reject failed preconditions, but a per-item failure that
crosses the object-coordinator boundary is reported as `INVALID` rather than
the more specific `CONDITION_FAILED`. Other independent items in the same bulk
request retain their normal success or failure outcomes.

Deleting the current tombstone of a versioned object correctly returns
`FAILED_PRECONDITION` and leaves that tombstone unchanged, but the coordinator
path does not preserve the `CURRENT_TOMBSTONE_VERSION_CANNOT_BE_DELETED` text
prefix. Clients must use the gRPC status code rather than matching that text in
0.5.1.

## Per-object user metadata

Keldra 0.5.0 accepts the bounded `content_type` header but does not accept
arbitrary caller-defined metadata on an object version. Applications that need
descriptive or index input fields must currently carry them in their payload or
in an application-owned manifest.

## S3 gateway surface in 0.5.3

The shared public listener implements path-style CreateBucket, HeadBucket,
PutObject, GetObject, HeadObject, DeleteObject and ListObjectsV2 alongside the
native gRPC API.
It does not yet implement ListBuckets, DeleteBucket, multipart upload, copy,
delimiter/common-prefix grouping, presigned query authentication, virtual-host
bucket routing, object tags, ACLs, lifecycle configuration or website APIs.
Applications needing those operations must use Keldra's native API or wait for
a later gateway capability.

Authenticated requests derive the tenant from the client credential, so an
S3 client uses ordinary `/bucket/key` path-style requests. An unsigned public
read has no credential from which to derive a tenant and therefore uses
`/tenant/bucket/key`; public ListObjectsV2 similarly uses `/tenant/bucket`.

SigV4 verification needs plaintext-equivalent signing material, whereas
pre-0.5.3 Keldra credentials intentionally retained only an Argon2id verifier.
New and rotated credentials contain an AES-256-GCM envelope in their existing
replicated credential record. Its key is domain-separated from the cluster JWT
signing material and its associated data binds tenant, application and client
identity. Existing credentials continue to work unchanged with gRPC but must
be rotated once before S3 use. Keldra never stores this material in a separate
column family or side plane.

PutObject accepts a signed SHA-256 payload or the fully chained
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` aws-chunked form and verifies the decoded
bytes before publication. Other aws-chunked trailer/checksum variants are
reported as unsupported rather than accepting unbound payload bytes.

## Incremental Git smart HTTP gateway

The shared public listener supports Git smart HTTP push and pull at
`/git/{tenant}/{bucket}/{repository}.git`. Basic authentication exchanges the
application client ID and secret through Keldra's normal credential path;
Bearer authentication accepts a normal Keldra access token. Pulls may omit
credentials only when current Zanzibar policy permits anonymous reads for the
bucket. Every push is Zanzibar-authorized. Immutable Git packs, reference
batches, and checkpoints are ordinary objects below the protected
`_keldra/git/v2` namespace; one exact-version CAS of the small `current` object
publishes a complete generation. Native bare repositories under
`KELDRA_CACHE_DIR` are disposable persistent materializations and recover from
the current checkpoint plus its bounded batch tail when their cache is absent.
Fetch responses and request bodies stream without the old whole-object gateway
buffer.

The first incremental release publishes one accepted push per batch and
serializes receive-pack for that repository on one node. Unrelated repositories
do not share a process lock, and competing nodes remain protected by the exact
`current` CAS, but bounded multi-push group publication and preferred-writer
peer proxying are not yet enabled. A losing cross-node push returns a conflict
and must fetch then retry.

Checkpoint compaction is requested in the background at a fixed tail depth of
64. The additional pack-count, byte, age, loose-object, and authorized manual
triggers are not yet runtime settings. Obsolete immutable Git generations are
not yet removed by repository-scoped retention, and the Git materialization
directory does not yet enforce its own LRU byte budget; operators should size
and monitor `KELDRA_CACHE_DIR` accordingly. These affect write concurrency and
space reclamation, not ref atomicity, authorization, durability, or recovery.
The complete target architecture and remaining qualification gates are in
[`KELDRA-0015`](rfcs/keldra_0015_incremental_git_repositories.md).

## Atomic preparation and the blob inactivity clock

Staging an atomic program's prepared output blobs and bundle sets their
ordinary blob `updated_at` timestamps before `CommitBatch` is proposed. An
unusually long delay in staging, waiting for the commit gate, or recovering an
earlier commit therefore reduces the effective post-commit replay and recovery
retention by the length of that delay.

The normal path is expected to take milliseconds. Keldra 0.5.0 does not refresh
the prepared blobs immediately before `CommitBatch`, and it does not add a
special lease, side store, or second lifecycle clock for atomic programs.

## Permanent deletion from versioned program-only paths

A version-enabled `PROGRAM_ONLY` path retains its historical payload versions.
Path policy correctly prohibits the ordinary `DeleteVersion` API from mutating
that path, and the 0.5.0 atomic-program DSL has no operation for deleting one
exact retained version. Operators therefore cannot permanently prune that
history in Keldra 0.5.0.

## Fixed accounting traffic bounds in 0.7.0

The process-local accounting traffic queue, per-bucket matcher cache, and
traffic-batch limits use fixed bounded defaults in 0.7.0. A
sustained ingress rate above those bounds can drop bandwidth-accounting entries;
Keldra reports the dropped batches and bytes, and stored-byte and object-count
accounting remains exact. Operators can reduce the risk by sizing or scaling
ingress nodes so the supported workload records zero drops. Runtime and startup
configuration for these bounds is deferred to a later release.

The 0.7.0 bandwidth matcher loads one bucket's sparse accounting-definition
locators only on a cache miss. Definition delivery invalidates that exact
bucket synchronously, while true-gap reconciliation clears disposable matcher
caches before its checkpoint advances; there is no periodic matcher rescan. A
bucket with more than 65,536 accounting definitions or more than 64 MiB of
decoded matcher state cannot be loaded, so bandwidth entries for that bucket
are dropped and reported rather than consuming unbounded memory. Very large
definition sets within the bound can still make a cold load expensive. Stored-
byte and object-count rollups remain exact, and all ordinary object and
authorization behavior is unaffected. Operators should use coarse path
accounting boundaries and monitor the accounting drop metrics.

## Fixed maintenance budgets in 0.7.0

Blob collection and former-placement retirement use fixed bounded work budgets
in 0.7.0. Each hourly cycle advances through 100 ms-spaced bounded ticks until
it completes. If sustained churn or slow health probes outpace that bounded
progress, disk reclamation can lag and local storage can temporarily grow.
Object availability, reference safety, and acknowledged durability are
unaffected because maintenance fails closed. Operators should monitor and
provision disk headroom for high-churn deployments; startup configuration for
the maintenance budgets and cadence is deferred.

## Accounting baseline restart after a terminal stream failure in 0.7.0

A first accounting build or genuine retained-journal gap consumes a scoped,
snapshot-bound baseline stream. If that stream ends with a terminal peer error,
its held RocksDB snapshots cannot be resumed, so Keldra restarts the baseline for
that same accounting path scope. The last complete rollup remains readable;
ordinary restarts resume valid rollups, and no unrelated object heads or startup
inventory are scanned. Operators can retry after peer health returns. A
resumable cross-node snapshot protocol is deferred.
