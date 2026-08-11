# ANVIL-0012: Sparse, Bounded Index Coordination in Anvil 0.7.0

Status: Accepted architecture for Anvil 0.7.0.

Supersedes: ANVIL-0011 in full.

Audience: Anvil implementors, operators, client authors, and reviewers

Upgrade contract: Anvil 0.7.0 deployments start with new volumes. There is no
old-volume detection, migration, scan, backfill, dual-write, compatibility
reader, or rejection subsystem. Behavior when pointing 0.7.0 at a volume
created by an earlier release is outside the supported contract.

## 1. Decision

Anvil retains the bounded immutable-run index engines introduced in 0.6.0 and
replaces their discovery, change-routing, recovery, accounting, and maintenance
coordination. Index construction must be bounded, but bounded construction is
not sufficient if startup or recovery still scans the entire object corpus.

The 0.7.0 flow is:

```text
ordinary definition object
  + transactional definition locator
  + source-journal entry
  + sparse route keys sharing that journal offset
        |
        +--> top-three weighted-HRW assignment records
        |      rank zero builds; ranks one and two are ready query/failover owners
        |
        +--> relevant bucket consumers only
               index builder / accounting worker / WatchPrefix

snapshot-bound scoped source stream + routed journal suffix
  -> bounded per-kind mutable builder
  -> immutable L0 runs
  -> streaming size-tiered compaction
  -> ordinary Anvil objects
  -> one CAS-published generation manifest
```

Two small, transactional secondary access paths are added to RocksDB:

1. `definition_state` locates definitions and stores only the assignments and
   cursors needed by this node.
2. `journal_routes` points from a definition kind or bucket identity to entries
   in the existing source-local journal.

Neither is a new authority. The ordinary object is the authority for a
definition. The existing source journal remains the sole ordered source of
change evidence. Route records reuse its source epoch and offset and have no
independent sequence, acknowledgement, or retention policy.

Raft continues to contain only bounded cluster decisions. It contains no
definition, assignment, journal route, index byte, segment, generation,
checkpoint, accounting rollup, cache entry, tenant, bucket, or object record.

## 2. Motivation

The 0.6.1 index process performs a global current-head scan before it starts the
public server. It filters the scan for index definitions only after every head
has been read. On a production-shaped installation with more than 726,000
ordinary objects, that scan exceeded a hard-coded 30-second deadline and
terminated the process with:

```text
index journals did not reach a clear initial definition barrier
deadline has elapsed
```

An automatic restart policy repeated the scan approximately every 37 seconds.
The ordinary objects were present, but the process could not become available
and the required index generation was never published.

This is a design failure rather than a timeout-tuning problem. Raising the
deadline makes startup depend on a larger finite corpus. Removing only the
deadline leaves every restart proportional to all objects. Replaying the
bounded source journal from offset zero cannot reconstruct definitions whose
creation events have already been compacted.

The same pattern occurs elsewhere:

- every index builder reads every source-journal event and filters afterward;
- accounting rediscovers definitions and rebuilds baselines instead of
  resuming its durable rollup checkpoint;
- retained-object and generation cleanup use broad scans with fixed deadlines;
- cache startup deletes useful disposable state instead of validating it
  lazily; and
- some retry paths turn transient failures into another broad rebuild.

At the intended upper bound—millions of tenants, potentially billions of
buckets, hundreds of millions of objects per bucket, and eventually trillions
of objects across a mesh—no startup or periodic task may have work proportional
to the complete object population. Work must be proportional to a requested
scope, a changed scope, assigned definitions, or an explicit maintenance
budget.

## 3. Research and retained engine design

The index representation remains an immutable log-structured design behind a
mutable public API. A bounded mutable batch becomes an independently queryable
L0 run. Streaming compaction merges immutable runs into compact read-optimized
runs. Publication is one compare-and-swap of an immutable generation manifest.

This follows the useful construction boundary demonstrated by MG4J: bounded
batches become separately queryable subindexes and are merged later. Elias-Fano
and related quasi-succinct structures provide compact monotone sequences and
efficient successor operations in merged runs. The pure-Rust `sux` crate
provides those structures; Rayon provides fixed-pool bounded CPU parallelism.
Both remain approved dependencies. Rayon work must hold the same byte-budget
permits as synchronous work.

The authoritative durable format remains explicit, versioned, fixed-width
where applicable, and portable between AMD64 and ARM64. Native Rust layouts are
not persisted. `epserde` informs the owned-to-borrowed access model but is not
an authoritative storage format.

## 4. Required invariants

1. Core object, authorization, administration, and gateway readiness never
   waits for a global index or accounting inventory scan.
2. Normal startup performs zero reads of unrelated ordinary object heads for
   definition discovery.
3. A tenant or bucket without a definition contributes no definition-runtime
   work and no per-bucket journal tail or cursor.
4. The ordinary definition object is authoritative for definition content,
   object version, Zanzibar authorization, and deletion.
5. `definition_state` and `journal_routes` are transactional secondary access
   paths. They cannot authorize, publish, or resurrect a definition.
6. One source-local journal remains the only ordered change authority. Sparse
   routes share its exact offset, gap semantics, and retention.
7. Definition locators and route records are committed in the same synchronous
   RocksDB batch as the head/version/journal mutation they describe.
8. A replicated definition mutation carries a typed transition. Replicas never
   infer a definition by parsing opaque payload bytes.
9. A logical index has up to three weighted-HRW owners. Rank zero alone builds
   and publishes. The other owners hold the small assignment needed for query
   materialization and failover.
10. Assignment is derived from stable numeric tenant, bucket, and definition
    IDs plus committed ACTIVE membership. It is never persisted in Raft.
11. Every builder and accounting publication checkpoint is a complete barrier:
    membership fence, atomic-program finalized watermark, and one source epoch
    and next offset for every required source.
12. An atomic program is never partially represented by a published index or
    accounting rollup when its ordinary object results are atomically visible.
13. Peak mutable construction memory is bounded independently per `IndexKind`
    and shared fairly between all local definitions of that kind.
14. Caches, builder scratch, assigned state, locators, routes, and accounting
    projections are recoverable or disposable. Ordinary objects, source
    journals, and published current pointers remain their authorities.
15. Maintenance is incremental and budgeted. Startup performs no unbounded
    filesystem inventory, generation scan, blob scan, or cache purge.
16. Corruption or recovery failure affects the exact capability or definition;
    it does not create an unbounded retry loop or stop unrelated object service.
17. 0.7.0 deployments use new volumes. The implementation contains no
    old-volume detection, migration, compatibility reader, dual writer,
    backfill, or old-format fallback.

## 5. Terms

**Source journal** is one node-local monotonically sequenced change log. Its
identity is `(node_id, source_epoch)` and its positions are `u64` offsets. It is
not Raft, a cluster-wide total order, or a payload log.

**Complete barrier** is a committed membership fence, an atomic-program
finalized watermark, and a vector of `(source_id, next_offset)` values. It is a
complete vector cut, not a global RocksDB snapshot or a total order between
ordinary writes on different nodes.

**Definition locator** is the small secondary record which makes an ordinary
definition object discoverable without scanning unrelated heads.

**Assignment record** is the local durable projection saying that this node is
one of the current top-three owners of a definition. It is not placement
authority and must be revalidated against current membership and the ordinary
definition object.

**Route record** is an empty or fixed-small secondary key which names an
existing source-journal offset by definition kind or bucket identity. It does
not duplicate the event body.

**Builder** is the rank-zero weighted-HRW owner which consumes scoped change
evidence and publishes generations.

**Run** is one immutable independently queryable index segment. L0 runs are
write optimized; merged runs are read optimized.

**Component block** is an independently fetched, checksummed ordinary object
holding a bounded range of one run component. It is not an erasure-code shard;
the ordinary Anvil byte plane may erasure-code its payload.

## 6. Existing source journal

Each node stores a source-local journal in its `local_invalidations` RocksDB
column family. An ordinary coordinated mutation assigns the next local offset
and commits the head, version, receipt, reference evidence, journal event, and
journal counters in one synchronous `WriteBatch`. Metadata replicas do not
append duplicate source events.

The physical journal tail is not by itself a visibility guarantee. A
distributed coordinator can durably append its candidate before the complete
metadata quorum has acknowledged it. The journal therefore stores one durable
`settled_through` watermark alongside its existing counters. It advances only
over a contiguous prefix for which the selected mutation is proven at the
fixed metadata quorum. Public watches, index builders, accounting workers, and
scoped snapshot boundaries consume only through that watermark.

The coordinator retains one bounded proof value at each unsettled object
position. The proof contains the exact typed object, retained-version-delete,
or program-path metadata mutation and no payload bytes. Recovery can replay an
ordinary object or retained-version mutation to its current fixed replica
group and then re-read the quorum. Atomic-program mutations remain owned by the
nominated executor's existing committed-tail recovery; journal settlement only
verifies their resulting exact proofs. Missing or split proof evidence stops at
that position rather than stepping over an unknown outcome.

Single-node mutations whose metadata and reference effects share the original
RocksDB batch stage the new settled watermark in that same batch. Distributed
recovery writes it once per bounded proven page. A crash before the watermark
write safely repeats proof work; a crash after it trusts the already proven
prefix. Membership cutover requires every old source's settled watermark to
equal its physical tail before activating the new placement. Because the
watermark is durable source-journal state, restart and later node additions do
not require an inventory or transfer of historical proof records.

The journal carries invalidations and exact transition evidence, not object
payload bytes. It supports:

- crash-safe content-reference deltas;
- public `WatchPrefix` invalidations;
- index and accounting catch-up; and
- online membership handoff suffixes.

Reference-delivery cursors constrain journal compaction because losing those
deltas can make object garbage collection unsafe. Public watches, indexes, and
accounting do not constrain it. They report an expired cursor or rebuild their
own derived view after a gap.

Reference-delivery safety and public visibility are separate cuts. The former
is reconstructed from durable destination cursors after restart and alone
controls journal pruning. The durable settled watermark controls visibility
and never claims that physical reference effects have reached every shard
owner.

The journal remains bounded by configured entry and byte limits. This is
correct: retained change history scales with a configured window, not stored
object population. What changes in 0.7.0 is how consumers find the small subset
of offsets relevant to them.

## 7. Transactional definition state

### 7.1 Definition authority

Index and accounting definitions remain ordinary, Zanzibar-authorized objects
inside the tenant and bucket they govern. Existing exact create, get, update,
delete, and known-bucket prefix-list APIs continue to operate on those objects.
The locator never supplies definition content to a public API.

The trusted definition service supplies this typed evidence with the ordinary
object mutation:

```text
DefinitionTransition {
  kind: INDEX | ACCOUNTING
  tenant_id: u64
  bucket_id: u64
  definition_id: u64
  path: bytes
  object_version: u64
  operation: UPSERT | DELETE
}
```

The transition is carried by the replicated logical object mutation. The
storage layer validates that its tenant, bucket, path, version, and delete state
match the enclosing mutation. It never discovers a definition by deserializing
opaque object bytes or by treating an arbitrary reserved-looking path as a
trusted transition.

### 7.2 `definition_state` column family

The new column family contains four key domains. Keys begin with a storage
format byte and a domain byte; ordered numeric components are big-endian.
Values use an explicit versioned encoding and reject unknown required versions.

```text
LOCATOR
  [format][L][kind][tenant_id][bucket_id][definition_path]
    -> { definition_id, object_version }

ASSIGNED
  [format][A][kind][tenant_id][bucket_id][definition_id]
    -> { definition_path, object_version, observed_fence, rank }

CHECKPOINT
  [format][C][consumer_kind][source_node_id]
    -> { source_epoch, next_offset, observed_fence }

RECONCILED
  [format][R]
    -> { membership_fence }
```

An UPSERT transaction writes or replaces the locator. A DELETE removes it. The
locator change, authoritative object mutation, source-journal event, and sparse
route keys share one RocksDB batch on the coordinator; replicated object owners
apply the same validated locator transition with their authoritative replica
mutation.

Assignment and checkpoint records are local projections. Applying an
idempotent assignment delivery and advancing its source checkpoint is one local
RocksDB batch. A crash observes either both or neither. An assignment is used
only after exact-reading the ordinary definition at the recorded version and
rechecking current weighted-HRW placement.

The single local `RECONCILED` record is only crash-recovery progress for the
rare membership-triggered locator inventory. It is sync-written after both
definition kinds finish and the membership fence is rechecked. It contains no
definition identity, cursor, payload, ownership decision, or authority.

The column family never contains definition payloads. Deleting it can delay or
rebuild derived capabilities but cannot lose ordinary data or grant access.

### 7.3 Definition delivery and placement

For every typed definition transition, the source derives the top three owners
from `(tenant_id, bucket_id, definition_id)` and the committed ACTIVE membership.
It delivers an idempotent assignment upsert or remove only to those owners over
the authenticated peer path. Rank zero schedules the builder. Ranks one and two
retain the assignment for query materialization and failover.

Delivery resumes from the sparse definition route, not the ordinary event
stream. A destination validates the membership fence and transition version.
A later version wins; a replay is harmless; an older transition is ignored.
Failure to deliver does not block the original object mutation or source-journal
retention. If the route falls behind retention, the affected projection enters
`RECONCILING` and inventories definition locators rather than ordinary heads.

Because a deleted definition has no live locator and assignment records do not
carry a source identity, a true gap reconciles the affected definition kind as
a whole; it does not guess which stale assignment came from the gapped source.
Under one committed membership fence, the reconciler streams each ACTIVE
node's assignments and locators in bounded pages. Every assignment and locator
candidate is exact-read from the authoritative ordinary object. Stale,
deleted, or misplaced assignments receive an idempotent versioned removal;
live locators recompute the top-three owners and receive idempotent upserts with
the current rank and fence. Replica locators therefore cannot resurrect a
deleted definition. Source tails captured before the inventory are replayed
through the sparse definition routes before the destination checkpoints become
current. A membership or source-epoch change, or another expired suffix,
restarts reconciliation. Version and fence ordering lets a concurrent newer
delivery win. The process uses no atomic registry swap, assignment generation,
tombstone catalogue, new column family, or corpus-sized in-memory set; an
interrupted rebuild is safe to repeat and every assignment remains subject to
exact authoritative-object revalidation.

On membership change, current owners stream their local assignments, recompute
the top three, and transfer only assignments whose owner set changes. A new
rank-zero owner exact-reads the definition and the published current pointer
before it builds. The existing placement fence prevents an old builder from
publishing after cutover.

Assignment transfer alone cannot discover a definition committed immediately
before its source disappeared but before its first asynchronous assignment was
delivered. Every committed membership/source-set change therefore also queues
the existing bounded whole-kind locator reconciliation for both index and
accounting definitions. It exact-validates ordinary definition objects and
repairs only missing, stale, or misplaced assignments. This rare recovery work
is `O(definitions)`, proceeds in bounded pages after serving is available, and
never scans ordinary object heads or blocks normal startup.

The lowest ACTIVE node performs that inventory. On its first observation of a
fence, it skips the inventory only when its durable local `RECONCILED` record
equals that exact fence. A missing or older record starts the inventory; a
future or malformed record fails closed. The marker advances only after both
kinds finish and the fence is rechecked. A crash before or during the inventory
therefore leaves the old marker and repeats bounded work after restart instead
of permanently losing an unassigned locator. Other nodes continue their local
assignment transfer but never write this completion record. A fresh 0.7 volume
may perform one empty locator inventory on first start.

At one billion live definitions, rebuilding all ownership has an unavoidable
`O(definitions)` lower bound. The design distributes normal ownership across
nodes and guarantees that billions of buckets with no definitions contribute
zero work. It never makes recovery `O(objects)`.

## 8. Sparse journal routing

The new `journal_routes` column family has no independently sequenced values.
Each route key ends in the authoritative source-journal offset and points back
to that existing journal event:

```text
DEFINITION route
  [format][D][kind][source_epoch][offset]

BUCKET route
  [format][B][tenant_id][bucket_id][source_epoch][offset]
```

Definition mutations write both their definition route and bucket route.
Ordinary object-head and retained-version changes write only the bucket route.
Aggregate and content-lifecycle changes without a bucket write neither unless a
specific existing consumer requires a typed route in this RFC.

The route value is empty: the offset is already in the key. A routed read seeks
the requested prefix, obtains offsets in source order, and reads the event body
from `local_invalidations`. A missing primary offset is a gap, never an empty
page. Route records are deleted in the same retention batch as the primary
journal entry. Their valid range is therefore exactly the primary journal's
epoch and retained floor through tail.

One bucket route serves all definitions in that bucket. A process-local bucket
dispatcher may decode a page once and use a prefix trie to wake matching index,
accounting, and watch consumers. The dispatcher is a cache; durable consumers
advance only their own complete checkpoints. Anvil does not create one journal,
tail, or durable cursor per bucket or definition.

The resulting steady-state work is:

```text
ordinary unrelated mutation                    O(1) journal + one bucket route
definition mutation                            O(1) locator + routes + <=3 deliveries
one index catch-up                              O(changes in its bucket, then path filter)
all local indexes sharing a bucket              one decoded routed page + local demux
tenant/bucket with no active consumer            no polling or durable cursor
```

This removes the previous `all cluster changes × all consumers` multiplier
without introducing a second log.

## 9. Definition APIs and startup

Public definition operations never depend on a complete in-memory catalogue:

- `GetIndex` and accounting equivalents exact-read the ordinary object.
- create, update, and delete use exact object CAS operations plus the typed
  transition.
- `ListIndexes` seeks the reserved definition prefix inside the already-resolved
  numeric tenant and bucket identity.

Normal startup is:

1. open and validate the 0.7 storage format;
2. start the public object/auth/admin/gateway service;
3. stream this node's local `ASSIGNED` prefixes in bounded pages;
4. revalidate each selected definition and current placement;
5. resume sparse source checkpoints; and
6. schedule rank-zero builders fairly.

There is no startup definition barrier and no fixed deadline around definition
inventory. The index states exposed to queries and operations are:

```text
DISCOVERING  BUILDING  CURRENT  STALE  RECONCILING  FAILED
```

A query may serve a prior complete generation with freshness evidence while
the builder catches up. If no complete generation exists, that index—not the
server—reports its unavailable state.

## 10. Scoped baseline and complete barriers

An initial build or true journal-gap rebuild establishes a scoped coherent
boundary without a cluster-wide frozen snapshot:

1. bind to one committed ACTIVE membership fence and a clear atomic-program
   finalized watermark;
2. on every required source, wait until its durable settled watermark reaches
   its physical tail, acquire the source commit fence, and take one RocksDB
   snapshot which includes current heads and that same snapshot's source epoch
   and settled tail;
3. seek and stream only retained current heads matching the stable tenant,
   bucket, and path-prefix scope;
4. feed frames directly into the bounded builder or accounting accumulator;
5. continue accepting writes into later source-journal offsets;
6. replay the routed journal suffix after each captured tail;
7. reread affected exact heads so overlap is idempotent; and
8. publish only when membership, source identities, source positions, and the
   atomic watermark form one complete barrier.

The index scan API uses stable `(tenant_id, bucket_id, path)` ordering and a
snapshot-bound opaque cursor. It returns only current head versions, not
mutation receipts. A held RocksDB snapshot prevents later pages from changing
beneath the scan.

Accounting must include every retained payload version when bucket versioning
is enabled. Its scoped snapshot stream is therefore independently paged in
stable `(tenant_id, bucket_id, path, version)` order. Each frame carries one
version descriptor plus the small current-head state needed to count the path
as one live object. It never materializes all retained versions of one path in
a `Vec`: one path with millions of versions must obey the same frame and memory
limits as millions of paths. Unversioned buckets naturally expose only the
current retained descriptor through this stream.

A large scan uses a per-frame inactivity deadline. It has no wall-clock deadline
proportional to total corpus size. Membership or source-epoch change cancels the
candidate explicitly. Progress metrics expose records, bytes, last frame, and
estimated lag.

## 11. Accounting recovery

Accounting definitions use the same locator, assignment, bucket routes,
snapshot-bound scoped stream, and complete barrier as indexes. A published
rollup stores:

```text
membership fence
atomic-program finalized watermark
source epoch and next offset for every required source
definition object version
aggregate totals and freshness time
```

Normal restart opens the last valid rollup and resumes from its barrier. It does
not rebuild a baseline merely because the process restarted. Applying one page
of accounting transitions and advancing the working cursor is crash-safe and
idempotent. Publication of the next rollup happens only at a complete barrier.

The first build or a real journal gap performs the scoped baseline described in
Section 10, captures concurrent journal tails, catches up, and then publishes.
The preceding rollup remains available with stale freshness until replacement.

Normal accounting work is `O(relevant changes)`. First build and gap recovery
are `O(objects in the explicitly configured scope)`. Unrelated tenants and
buckets cost zero.

### 11.1 Ingress traffic matching

Stored-byte and object-count accounting is derived from authoritative object
state and routed journal evidence and remains exact at a reported complete
barrier. Inbound and outbound byte accounting is deliberately bounded
best-effort usage telemetry; it is not a financial-ledger guarantee.

An arbitrary public ingress must not hold every accounting definition merely to
match one request path. Weighted HRW therefore derives one **accounting matcher**
for each stable numeric `(tenant_id, bucket_id)` from committed ACTIVE
membership. The assignment is disposable and never enters Raft.

Each ingress:

1. records completed public ingress/egress byte deltas as bounded entries keyed
   by stable bucket IDs and exact path;
2. groups entries into bounded batches with one stable retry identity;
3. sends each batch to that bucket's current matcher over the authenticated peer
   path; and
4. retains it for bounded retry until the matcher acknowledges it or the local
   process loses its disposable queue.

The matcher lazily prefix-loads only that bucket's ordinary accounting
definitions and evicts idle bucket match state under a shared bound. Cached
definitions have no time-to-live and therefore never create periodic bucket or
corpus scans. After assignment delivery for an accounting-definition
transition, the source synchronously sends an idempotent bucket invalidation to
the current weighted-HRW matcher before advancing its delivery checkpoint. The
authenticated peer handler verifies the committed fence, ACTIVE caller, and
current matcher target, takes the existing traffic-delivery gate, and then
evicts exactly that bucket. A true route gap clears the disposable matcher
caches on every ACTIVE node before the reconciled delivery checkpoint or
membership completion marker advances. Duplicate invalidation and clear
delivery are harmless.

The matcher applies segment-aware path-prefix matching once, aggregates by
definition, and uses the existing idempotent per-definition traffic-source
publication path to the definition's rank-zero worker. A membership change
causes the ingress to rederive and retry against the new matcher. Duplicate
batch delivery cannot double count because the stable batch identity derives
stable per-definition flush identities.

This adds no authoritative traffic log, per-bucket Raft record, global
definition catalogue, or durable matcher assignment. The accepted consequence
is that a small amount of traffic may be absent from reported bandwidth during:

- ingress process failure before an in-memory batch is acknowledged;
- bounded queue exhaustion under sustained overload; or
- the short interval while an enable, disable, or matcher reassignment reaches
  the disposable bucket cache.

Those windows affect bandwidth totals only. They never alter stored-byte,
object-count, object, reference, authorization, or index correctness. Anvil
exports dropped batch and byte counts, pending bytes, oldest pending age,
matcher retries, cache generation/age, and definition-propagation lag. Queue
exhaustion is never silent. Release qualification must show zero drops under the
supported production-shaped workload; deployments requiring legally exact
network billing need an external request ledger rather than treating this
telemetry as one.

## 12. Builder memory, immutable runs, and compaction

One `TypeBuildPool` exists per enabled index kind. It owns a byte semaphore, a
fair ready queue, and access to one fixed process-owned Rayon pool. A builder
obtains a byte lease before it pulls source data or allocates a capacity-growing
collection. It yields after a fixed work quantum or sealed block.

An accepted source change becomes a versioned live record or tombstone. A
bounded mutable builder coalesces repeated exact paths and flushes a sorted L0
run. Every run contains a path/version change directory so later runs can shadow
older live values and deletes correctly.

Compaction is size tiered with four-input fan-in. It k-way merges immutable
component iterators while retaining only iterator state, decoded input blocks,
and one bounded output block. Merged monotone structures use appropriate `sux`
Elias-Fano/rank/select representations. Inputs remain queryable until every
replacement object has met artifact durability and a new generation wins its
current-pointer CAS.

The public API therefore appears mutable while durable index storage stays
immutable. Anvil does not add mutable distributed index pages, another WAL, or
an index-specific replication plane.

## 13. Ordinary-object publication and retention

Definitions, run components, component blocks, run roots, generation manifests,
and current pointers are ordinary Anvil objects under reserved paths. Small
objects use the inline path; larger ones use the ordinary erasure-coded byte
plane. Index publication requests `REPLICATED` acknowledgement when the cluster
can provide it and degrades to `LOCAL` on a one-node cluster, matching the
ordinary durability contract.

Publication is one current-pointer CAS checked against:

- expected pointer version;
- exact definition object version;
- current weighted-HRW rank-zero owner; and
- committed membership fence.

Obsolete generations are retained subject to the configured maximum generation
count, maximum age, and maximum authoritative bytes. The first exceeded bound
makes the oldest non-current generation eligible. In-flight queries are safe
because they are pinned to one immutable generation and the minimum retention
age exceeds the request deadline.

Retention never performs one full scan per index on a timer. One node-wide
bounded scheduler seeks only the reserved artifact prefixes for due definitions,
keeps cursors across scheduler ticks, staggers work, and enforces record, byte,
and time budgets. In 0.7.0 a process restart begins that disposable traversal
again at the exact definition's artifact prefix; it never widens to unrelated
objects and every tick remains bounded.

## 14. Query execution and authorization

One complete query executes on one top-three owner. Any public node may accept
the request and proxy it to one current owner. There is no scatter/gather query
plan or network result merge.

The query pins the definition version, generation, authorization revision,
membership fence, and page-token identity. It opens immutable component blocks
through the shared async index-cache handle and uses lazy iterators plus bounded
top-k state. It never materializes the complete result set merely to paginate.

Authorization proceeds from coarse to fine:

1. authenticate the caller or use the explicit anonymous principal;
2. authorize query access to the definition and its stable tenant/bucket/path
   boundary;
3. eliminate candidates outside that boundary in the index routing layer; and
4. exact-authorize remaining paths unless a Zanzibar relation proves the whole
   boundary visible at the pinned revision.

An index cannot grant access. Cache entries never carry a positive authorization
decision beyond its revision. Anonymous access works only through explicit
Zanzibar policy; missing or invalid credentials never silently bypass policy.

Results always include freshness evidence. Lag does not turn partial but valid
results into `INDEX_LAGGING`; the client decides whether the reported source
barrier is fresh enough.

## 15. Supported index kinds

All existing 0.6 public kinds remain in scope and use the common coordination,
bounded construction, publication, visibility, cache, and authorization paths.

### 15.1 Path

L0 sorts path, version, and tombstone state. Merged runs use prefix-compressed
path dictionaries and succinct offsets. Prefix queries seek the first matching
range and k-way merge visible live paths.

### 15.2 Metadata filter

The fixed head projection—path, version, content type, content length, content
hash, and commit time—becomes typed rows plus sorted routed-key components.
Equality, set, prefix, and range predicates seek relevant blocks and validate
the referenced live row.

### 15.3 Typed JSON

The builder parses only configured JSON pointers. Canonical scalar keys retain
their JSON type. Arrays may contribute multiple values while result identity
deduplicates by path/version. Range and ordered queries stream routed scalar
iterators.

### 15.4 Full text

Bounded tokenization emits term, document ordinal, and optional position. L0
uses term-sorted gap-coded postings; merged runs use succinct document and
position sequences. Boolean and phrase iterators retain only bounded state and
ranking uses a bounded top-k heap.

### 15.5 Vector

The initial engine remains exact. Vectors use fixed-width portable blocks and
succinct identity/offset structures. Queries stream blocks and maintain a
bounded top-k heap; they do not load the complete vector corpus or introduce a
mutable distributed ANN graph.

### 15.6 Hybrid

Text and vector components share one path/version document table and one source
barrier. Bounded candidate streams are fused deterministically; compaction
replaces both component families together.

### 15.7 Git source

Commit, tree-path, and object relationships use sorted tuples and routed lookup
keys. Object bodies remain ordinary Anvil objects and are never copied into the
index.

### 15.8 Tensor

Tensor name, shape, data type, model identity, and source location use routed
keys and succinct document ordinals. Tensor payload bytes remain in ordinary
objects.

## 16. Typed recovery policy

Recovery classifies failures instead of mapping every error to a rebuild:

| Failure | Behavior |
| --- | --- |
| Peer timeout, temporary unavailability, atomic program in progress | Retain the durable checkpoint, yield the process-local worker lease, and retry through bounded assignment rediscovery. |
| Membership fence changed before publication | Discard candidate, recompute placement, and resume from a compatible checkpoint when possible. |
| Source epoch changed or cursor fell below retention floor | Mark the exact definition `RECONCILING` and run one scoped baseline. |
| Corrupt definition, manifest, component, or unsupported version | Fail that definition closed and alert; do not loop a broad scan. |
| Mutable builder lost on crash | Resume from the last published complete barrier. |
| Candidate upload or CAS lost | Keep the prior generation; unreachable ordinary objects become retention candidates. |
| Cached block absent or corrupt | Evict and refetch from authoritative ordinary storage. |
| Accounting worker crash | Resume from the last published rollup barrier. |

Shared bounded queues coalesce retries and prevent many definitions from
starting simultaneous rebuilds after one peer or membership failure. A failed
definition never retains one of the bounded process-local worker leases while
waiting: the durable assignment and last published checkpoint are its resume
point, while incomplete mutable scratch is disposable.

## 17. Bounded garbage collection and cache recovery

Garbage collection starts only after serving and reference recovery are ready.
Each tick inspects at most configured record, byte, and time budgets and stores
its next cursor. Candidate discovery does not hold the process-wide mutation
commit lock. Before removal, a short critical section rereads flags, count, and
`updated_at`; only still-eligible content is removed from canonical reachability
while locked. Potentially slow filesystem unlink happens afterward.

Prepared or unpublished shards use the ordinary shard record, reference count,
awaiting-publish flag, and `updated_at` grace. There is no side storage or
special index-preparation plane.

The disposable index cache is not deleted at startup. A deterministic
hash-and-length path is validated lazily when opened or mmapped. Missing or
corrupt blocks are refetched. One bounded background reconciler accounts and
evicts zero-reference entries under the shared disk and memory budgets. Strict
filesystem quota enforcement is an operator/filesystem concern rather than a
reason to inventory all cache files before serving.

These rules make startup `O(1)` for maintenance, each tick `O(budget)`, and a
full local cleanup `O(local candidates)` spread over time.

## 18. New-volume release contract

Every 0.7.0 deployment starts with a new volume. This is an operator and release
contract, not another storage subsystem. The server does not probe an old
volume, classify its release, scan it, or provide a special rejection path.

0.7.0 contains no:

- one-time head scan for old definition discovery;
- resumable locator backfill;
- explicit re-registration bridge;
- old index definition or generation reader;
- dual-write or shadow-read mode;
- compatibility feature flag; or
- fallback to the 0.6 global in-memory catalogue.

The absence of migration code is what permits a hard guarantee that a populated
0.7 store never pays a legacy global scan. Starting 0.7.0 against an earlier
release's volume is unsupported and deliberately unspecified; no implementation
work is spent making that case fail in a particular way.

## 19. Complexity contract

Let `O` be all objects, `D` all definitions, `A_i` definitions assigned to node
`i`, `C_b` changes in one bucket, `S_p` live objects under one requested path
prefix, and `Q` one maintenance work budget.

| Operation | 0.6.1 behavior | 0.7.0 contract |
| --- | ---: | ---: |
| Core service startup | `O(O)` and fatal deadline | `O(1)` before serving |
| Definition runtime startup on node `i` | `O(O)` plus full catalogue | `O(A_i + pending sparse changes)` streamed |
| Definition mutation | Hidden among all changes | `O(1)` batch plus at most three deliveries |
| Index catch-up | `O(all cluster changes)` per index | `O(C_b)` with local prefix demux |
| First build or true gap | Broad retained scan | `O(S_p)` snapshot-bound stream |
| Accounting restart | Rebuild baseline | `O(relevant changes)` from rollup barrier |
| Public known-bucket definition list | Catalogue dependent | One RocksDB prefix seek plus page size |
| Maintenance tick | Broad scan or startup purge | `O(Q)` |
| Empty bucket | Contributes to global scan | Zero derived-runtime state |

An `O(D)` locator reconciliation is permitted only after loss or a true retained
journal gap. No ordinary restart performs it. Mesh-wide ownership, routing, and
reference recovery remain a future RFC; this RFC's barriers are one-cluster
barriers and must not be stretched into a vector of every future mesh node.

## 20. Observability

OTLP metrics and traces expose, without payloads or mutable names:

- definition assignments, sparse delivery cursor, route pages, gaps, and
  reconciliation reasons;
- startup head-scan count, which must remain zero;
- scoped snapshot records/bytes, last-progress time, and journal suffix lag;
- complete membership/source/atomic barrier state;
- configured, leased, peak, and waiting construction bytes by kind;
- builder flushes, run levels, compaction input/output, and publication CAS;
- accounting rollup age and source lag;
- accounting traffic pending/dropped batches and bytes, oldest pending age,
  matcher retries, bucket-cache age, and definition-propagation lag;
- maintenance cursor, candidates, records, bytes, lock time, and unlink time;
- cache hits, misses, lazy validations, refetches, and evictions; and
- query latency, fetched blocks, candidates, stale rejections, and authorization
  rejections by kind.

Logs may identify stable numeric tenant, bucket, and definition IDs. They never
log definition bodies, source payloads, selected JSON values, query vectors,
credentials, tokens, or proprietary path names.

## 21. Validation matrix

### 21.1 Transaction and format tests

- Definition UPSERT and DELETE commit the head, locator, source event, and both
  route keys atomically.
- Replica replay applies the typed locator transition idempotently and rejects a
  transition which disagrees with its enclosing mutation.
- Route prefixes return exact source-order offsets and are pruned with primary
  journal entries.
- Old, missing-middle, future, and wrong-epoch routed cursors fail distinctly.
- Raw tail, durable settled visibility, and reference-safe cuts advance
  independently; restart preserves only the proven settled prefix.
- A cancelled or restarted distributed mutation is replayed from its exact
  typed proof and cannot expose a split or absent quorum through Watch or a
  derived capability.
- A new 0.7 volume creates and reopens all required column families without
  scanning heads.

### 21.2 Definition and placement tests

- One million unrelated heads plus zero definitions cause zero head reads at
  startup.
- One million unrelated heads plus one definition start only that assigned
  capability.
- Definitions in another tenant/bucket never enter a scoped list or assigned
  runtime.
- Upsert, delete, duplicate replay, stale replay, membership change, and rank-zero
  failover preserve exactly one publisher and at most three owners.
- A crash before or during the lowest-ACTIVE membership locator inventory leaves
  its durable completion fence old; restart repeats it, while an exact completed
  fence skips it and a future or malformed fence fails closed.
- Public exact/list APIs read ordinary definitions and enforce Zanzibar even if
  local assignment state is absent or corrupt.

### 21.3 Index and accounting recovery tests

- Each supported index kind performs initial build, routed incremental update,
  overwrite, tombstone, compaction, pagination, and query after restart through
  the public API.
- Writes through all three cluster ingress nodes reach one builder.
- A real journal gap performs one scoped snapshot and never reads another bucket.
- Atomic-program paths publish in one complete generation/rollup barrier.
- Accounting resumes the persisted rollup cursor after restart and does not run
  a baseline.
- A mutation racing the last baseline page appears exactly once after catch-up.
- A single path with more retained versions than one page is counted through
  `(path, version)` cursors without a corpus-sized allocation.
- Large scoped baselines continue while frames make progress beyond the former
  fixed total deadline.
- Traffic entering through every node reaches the derived bucket matcher,
  overlapping accounting prefixes receive one idempotent delta each, retries do
  not double count, matcher failover drains pending batches, and supported-load
  qualification reports zero dropped batches/bytes.

### 21.4 Maintenance and cache tests

- Retention never scans another definition's ordinary paths.
- One scheduler tick cannot exceed its record, byte, or time budget.
- A concurrent reference increment prevents a selected blob from being removed.
- Crash before and after canonical removal preserves recoverable state.
- A warm valid cache survives restart; corrupt and absent blocks refetch lazily.
- Cache reconciliation and eviction remain bounded with more files than one
  tick can inspect.

### 21.5 Release qualification

Before tagging 0.7.0:

1. format and Clippy checks and the repository 2,000-line source limit pass;
2. locked workspace tests pass locally;
3. public single-node and three-node Docker tests cover object, authorization,
   gateway, accounting, and every index kind;
4. a populated restart proves no global head scan and no startup deadline;
5. the public approximately 840,000-record, 12-field qualification completes
   ingest and index publication under explicit CPU/memory/cache budgets;
6. rolling three-node restart and builder reassignment retain complete published
   generations;
7. AMD64 and ARM64 images are built from the exact candidate commit as one
   multi-platform image;
8. publishable crates, image contents, and the bounded outgoing commit range pass
   privacy and secret checks; and
9. the exact validated commit is pushed, tagged `0.7.0`, released, and its crate
   versions, image digest, and platform manifest are independently verified.

Performance tuning may follow functional release. Global startup scans,
unbounded construction memory, unauthorized results, incomplete atomic
visibility, stale/deleted hits, false durability, and inability to build a
supported kind are release blockers rather than known limitations.

## 22. Consequences

The new column families and typed mutation field make definition discovery and
routing explicit instead of attempting to recover type information from an
opaque global object space. Each ordinary object mutation pays for one compact
bucket route key; definitions pay for one additional route and locator. In
exchange, consumers seek directly to relevant changes and startup becomes
independent of total objects.

Assignment replicas add at most three small records per live definition. This
is bounded by actual enabled capabilities, not tenants or buckets. No whole
registry is swapped atomically; each definition is an independent transactional
entry.

The new-volume contract requires operators to create 0.7 data directories and
reingest or restore data through supported 0.7 APIs. That cost is explicit and
finite. The implementation gains no legacy corpus scan, compatibility branch,
format probe, or mixed-format behavior.

## 23. References

- MG4J manual, [building batches](https://vigna.di.unimi.it/MG4J/man/manual/ch02s03.html).
- MG4J manual, [combining batches](https://vigna.di.unimi.it/MG4J/man/manual/ch02s04.html).
- Sebastiano Vigna, [Quasi-Succinct Indices](https://vigna.di.unimi.it/ftp/papers/QuasiSuccinctIndices.pdf), WSDM 2013.
- Sebastiano Vigna et al., [`sux`](https://github.com/vigna/sux-rs).
- Tommaso Fontana, Sebastiano Vigna et al., [`epserde`](https://github.com/vigna/epserde-rs).
- Gonzalo Navarro and Veli Makinen, [Compressed Full-Text Indexes](https://users.dcc.uchile.cl/~gnavarro/ps/acmcs06.pdf), ACM Computing Surveys 39(1), 2007.
- Rayon developers, [Rayon](https://github.com/rayon-rs/rayon).
