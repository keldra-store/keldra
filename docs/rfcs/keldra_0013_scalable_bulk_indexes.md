# KELDRA-0013: Scalable Bulk and Incremental Indexes in Keldra 0.8.0

Status: Accepted architecture for Keldra 0.8.0

Supersedes: KELDRA-0012 in full

Audience: Keldra implementors, operators, client authors, and reviewers

## 1. Decision

Keldra 0.8.0 replaces the index artifact format and the initial-build execution
path. It keeps one authoritative object store, one ordered source-journal
mechanism, ordinary Keldra objects for durable index artifacts, weighted-HRW
builder and query placement, and one compare-and-swap publication point per
index generation.

The new design has two deliberately different construction paths:

```text
initial build or explicitly requested rebuild
  -> snapshot-bound scoped source stream
  -> bounded parallel projection
  -> range-partitioned bulk builder
  -> format-v3 packed base runs
  -> routed journal suffix
  -> one source-complete generation CAS

ordinary catch-up
  -> sparse routed journal pages
  -> bounded parallel projection
  -> immutable L0 runs
  -> one source-complete generation CAS
  -> asynchronous range-striped compaction
  -> replacement generation CAS at the same or newer source barrier
```

The bulk path writes a compact base generation directly. It does not simulate
an initial build by producing hundreds or thousands of small L0 runs and then
rewriting them through every compaction level.

Format v3 stores logical index blocks inside immutable 16 MiB target-size pack
objects. A logical block descriptor addresses an ordinary Keldra object plus a
checked byte offset and length. Pack objects, run roots, generation manifests,
and current pointers all use the ordinary inline or erasure-coded object path.
There is no index byte plane, index database, artifact registry, or index WAL.

Mutable source changes remain visible through immutable generations. A
generation may contain bounded compaction debt, but it is published only when
it represents a complete source barrier. Compaction improves query shape; it
is no longer a prerequisite for acknowledging that all source changes through
the barrier are indexed.

The source journal and mutation-receipt store remain bounded. Their entry and
byte limits are startup configuration which may change on restart. Reaching a
limit no longer drops required index evidence or turns a committed object into
an apparently missing indexed object. Keldra first prunes proven-safe state,
then applies write backpressure until consumers advance. If a request deadline
expires before admission, the mutation has not started and the client may
retry its command identity.

## 2. Motivation and measured baseline

The immutable-run model is sound, but the 0.7 construction path applies it at
the wrong granularity for a large initial corpus.

A production-shaped ARM64 qualification indexed 839,980 small JSON objects
with twelve Typed JSON fields. Its relevant results were:

| Phase | Result |
| --- | ---: |
| Object ingest | 471.8 seconds, approximately 1,780 objects/second |
| First complete index generation | approximately 27 minutes 45 seconds after ingest |
| End-to-end index rate | approximately 504 source objects/second |
| L0 runs generated | approximately 103 |
| Largest measured compaction | 563,725 source records, 242 seconds |
| Blocks published by that compaction | 4,463 ordinary objects |
| Peak process/cgroup memory during qualification | approximately 4.3 GiB |

The run did not lose data, exhaust memory, restart, or report RocksDB write
stalls. The result is nevertheless not viable at Keldra's intended scale.
Linear extrapolation alone puts 100 million objects at roughly 15.6 hours to
ingest and 55 hours to index. Size-tiered rewrite amplification makes the index
projection worse as levels are added. A 400 million-object corpus would take
days rather than hours.

The same qualification exposed four distinct multipliers:

1. A fixed 4 MiB source quantum produced many small runs even when the configured
   per-kind memory budget could safely admit a larger build unit.
2. Source projection awaited one head read, payload read, and Rayon job before
   starting the next object. A four-worker CPU pool therefore projected only
   one source object at a time.
3. Derived-key rebuild and final cross-range merge retained serial phases after
   range-local compaction had become parallel.
4. Each approximately 512 KiB logical block became a separate ordinary object.
   Thousands of sequential object publications added fixed metadata, journal,
   authorization, reference, and sync costs after the data merge was complete.

Ingest also repeated work at request granularity: stable tenant and bucket names
were resolved for each object, bucket-wide Zanzibar decisions were repeated,
local payloads were cloned for a remote path which was not used, placement was
recomputed per object on one-node deployments, and related RocksDB point reads
were issued independently instead of as one bounded multi-get.

Finally, equality queries traversed the complete routed-key tree instead of
seeking the requested half-open range. Once the full generation was published,
exact verification became CPU-bound and could delay the serving-fence renewal
task. An index optimization must not make cluster control-plane liveness depend
on query load.

These are architectural granularity failures. Raising deadlines, adding memory,
or creating a larger thread pool does not remove them.

## 3. Goals

Keldra 0.8.0 must:

- build a large initial or replacement generation in one bounded bulk pass plus
  a bounded journal suffix;
- keep mutable construction, external sort, compaction, cache, and query memory
  within explicit shared budgets;
- project and compact every supported index kind concurrently without allowing
  one index to multiply the kind-wide memory allowance;
- publish source-complete generations before optional compaction debt is fully
  repaid;
- keep journal and receipt guarantees under pressure by delaying new writes,
  never by silently discarding required state;
- use the source journal's hard entry and byte capacities as the sole
  lag-driven admission pressure; lag never switches an index into bulk rebuild;
- reduce durable artifact-object count by packing logical blocks;
- avoid corpus-wide dense document renumbering during each compaction;
- use compressed postings for Typed JSON and Metadata Filter predicates;
- make point, equality, prefix, and range queries seek their relevant key range;
- isolate serving-fence and membership progress from CPU-heavy index work;
- preserve Zanzibar authorization, atomic-program visibility, ordinary object
  durability, weighted-HRW placement, and current-pointer CAS publication;
- keep startup and periodic maintenance independent of total ordinary-object
  count; and
- provide enough metrics, traces, and logs to distinguish source reading,
  projection, sorting, packing, publication, compaction, cache, query, and
  backpressure costs while work is in progress.

## 4. Non-goals

This RFC does not add:

- a distributed scatter/gather query engine;
- a mutable distributed index-page protocol;
- a second authoritative event log, catalogue, registry, job store, or receipt
  system;
- index bytes in Raft;
- a format-v2 decoder, converter, backfill, dual writer, query fallback, or
  compatibility shim;
- a mutable ANN graph for vector search;
- mesh-wide ownership or cross-region barriers;
- automatic unbounded memory growth in response to lag; or
- runtime mutation of the new capacity settings without a process restart.

## 5. Terms

**Source journal** is one node-local, monotonically sequenced log of committed
object transitions. Its position is `(node_id, source_epoch, offset)`. It is
the ordered evidence used by reference delivery, `WatchPrefix`, indexing, and
accounting. It contains transition metadata, not payload bytes, and is not a
Raft log or a cluster-wide total order.

**Sparse route** is a secondary RocksDB key which maps a bucket or definition
kind to an existing source-journal offset. It has no event body, sequence, or
retention policy of its own.

**Complete barrier** is a committed membership fence, the atomic-program
finalized watermark, and one `(source_epoch, next_offset)` value for every
required source. It is a vector cut, not a global RocksDB snapshot.

**Source-complete generation** is an immutable generation whose runs represent
every relevant visible object transition through one complete barrier. It may
contain bounded compaction debt.

**Compaction debt** is the configured bounded number and encoded bytes of
immutable runs which are queryable but not yet merged into their target level.
It is not missing source data.

**Publication progress debt** is the exact journal entries and logical bytes
by which a trusted derived publication temporarily exceeds the configured
source-journal capacities so it can publish the source-complete generation or
complete accounting rollup which releases older retained evidence. It is
retained journal data, not permission to omit or truncate an event.

**Bulk rebuild** is the scoped, snapshot-bound base-generation construction path
used for a first build or an explicitly authorized rebuild request. Source lag
alone never selects this path.

**Logical block** is one independently checksummed and decoded leaf, routing,
dictionary, posting, document, or vector block in format v3.

**Pack object** is one immutable ordinary Keldra object containing multiple
logical blocks. Its target payload size is 16 MiB. It is not an erasure-code
shard; ordinary storage may erasure-code the pack payload.

**Stable version identity** is the exact source identity `(path, object_version)`
inside the already stable numeric tenant and bucket scope. It does not change
when a generation is compacted.

**Range-local identity** is a compact ordinal valid only inside one immutable
path-range component. A posting carries enough range context to resolve it back
to a stable version identity. No generation-wide document ordinal exists.

**Mutation receipt** is the bounded, retained command outcome which makes a
client mutation identity idempotently retryable for its configured retention
window. It is not an index artifact or an event-delivery acknowledgement.

## 6. Required invariants

1. Ordinary object mutation and its source-journal transition commit in the
   same synchronous RocksDB batch on the authoritative metadata coordinator.
2. A successful mutation is never omitted from every future complete index
   generation merely because a bounded journal became full. Capturing or
   completing an unpublished snapshot is never proof that its journal evidence
   may be discarded.
3. Except for the narrowly scoped trusted derived publication described in
   Section 8.5, a write which cannot reserve both its mutation receipt and
   source-journal entry does not begin. Capacity pressure occurs before the
   authoritative object mutation. The exception may create only retained
   publication progress debt; it cannot omit evidence or admit an ordinary
   source-producing write above either configured capacity.
4. The ordinary index definition object is the sole authority for definition
   content, version, scope, and Zanzibar authorization.
5. The existing source journal remains the sole ordered change authority.
   Sparse routes and local scheduler state cannot create or reorder events.
6. Format-v3 pack objects and manifests are ordinary immutable Keldra objects.
   Their durability and garbage collection use the ordinary data plane.
7. Publication remains one current-pointer compare-and-swap by the current
   weighted-HRW rank-zero builder.
8. A current pointer never names a pack, run, or manifest which has not already
   met artifact durability.
9. A published generation is source-complete. Compaction debt may affect query
   cost but never source visibility through its advertised barrier.
   Advancing that barrier is itself publication work even when none of the
   intervening source objects changes the index contents. Checkpoint-only
   progress is never left solely in disposable builder memory.
10. Atomic-program paths which are atomically visible through ordinary APIs
    become visible together in an index generation.
11. One `IndexKind` memory budget is shared fairly by every local definition of
    that kind. Projection or compaction lanes acquire workspace from it before
    allocating.
12. Parallel completion order cannot change logical ordered index contents.
13. Stable version identities are never replaced by a corpus-wide dense
    reassignment. Any compact ordinal is explicitly range-local.
14. Query execution never scans an entire routed component when the predicate
    defines a seekable half-open range.
15. CPU-heavy projection, decoding, ranking, sorting, and merging cannot run on
    or block the control-plane executor used for serving-fence progress.
16. Authentication failure never becomes anonymous access. Index creation,
    update, deletion, rebuild, and protected query remain Zanzibar authorized.
17. Startup performs no scan of unrelated ordinary object heads, artifacts, or
    cache files.
18. Caches, decoded pages, external-sort scratch, assignment caches, and
    in-progress builders are disposable. Their loss cannot lose ordinary data
    or grant access.
19. Raft contains no definition, source event, index block, pack, run,
    generation, cursor, receipt, accounting record, cache item, or query state.
20. Format v3 is the only index artifact format read or written by 0.8.0.

## 7. Definition discovery, placement, and authority

Index definitions remain Zanzibar-authorized ordinary objects in the tenant and
bucket they govern. A trusted definition transition is committed with the
ordinary mutation and maintains the existing transactional definition locator.
The locator makes definitions discoverable without reading unrelated heads; it
does not contain the definition payload and cannot authorize a request.

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

Storage validates the transition against the enclosing tenant, bucket, path,
version, and delete state. It never treats arbitrary opaque bytes or a
reserved-looking path as a trusted definition transition.

The existing `definition_state` RocksDB column family contains four bounded key
domains. Ordered numeric components are big-endian and every key/value begins
with an explicit storage-format tag:

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

An UPSERT writes or replaces its locator and a DELETE removes it in the same
synchronous RocksDB batch as the authoritative head/version, source event, and
sparse routes. `ASSIGNED`, `CHECKPOINT`, and `RECONCILED` are local recoverable
projections. Deleting them delays derived capability recovery but cannot lose
ordinary source data, publish a generation, or grant access.

The existing `journal_routes` RocksDB column family contains fixed-small keys
which end in the offset of an existing source event:

```text
DEFINITION route
  [format][D][kind][source_epoch][offset]

BUCKET route
  [format][B][tenant_id][bucket_id][source_epoch][offset]
```

A definition mutation writes both routes. An ordinary object transition writes
the bucket route. A route has no independently sequenced value; scanning it
returns offsets whose event bodies are read from the source journal. Primary
journal pruning deletes its route keys in the same bounded cleanup work.

For each stable `(tenant_id, bucket_id, definition_id)`, weighted HRW selects up
to three owners from committed ACTIVE membership:

- rank zero builds and publishes;
- ranks one and two materialize published packs for queries and are ready for
  builder failover; and
- any public node may accept a query and proxy it to one of those owners.

Assignments are local, bounded projections. They are never placed in Raft.
Before using one, a node exact-reads the ordinary definition at the recorded
version and recomputes placement under the current membership fence. A
membership cutover prevents the old rank-zero builder from winning publication.

Definition mutations use sparse definition routes for normal delivery. A true
route gap or membership loss runs the bounded locator reconciliation defined by
the existing coordination model: it scans definitions, not ordinary object
heads; exact-validates each ordinary definition; and repairs only the top-three
assignments. A billion definitions necessarily require `O(definitions)` work to
reassign, but billions of buckets with no definitions contribute no state or
work.

Normal startup opens storage, starts the public object/auth/admin/gateway
surface, streams only this node's assignments, revalidates them, resumes sparse
source cursors, and schedules work fairly. There is no global definition
barrier and no corpus-sized startup deadline.

## 8. Source journal, retention, and backpressure

### 8.1 Journal authority

Each metadata coordinator appends one typed transition to its source-local
journal in the same synchronous RocksDB batch as the head, version, receipt,
reference evidence, accounting transition, definition transition, and sparse
route keys which apply to that mutation. Replica metadata writes do not append
duplicate source events.

Consumers read only through the journal's durable `settled_through` watermark.
It advances over the contiguous prefix whose selected metadata mutation has
met the fixed metadata quorum. Reference-safe delivery and source visibility
remain separate cuts; neither may claim progress from an unproven tail.

Public `WatchPrefix` remains an invalidation subscription. A slow public watch
may receive `RESUME_EXPIRED` after retention. Internal index and exact accounting
consumers are different: only their last source-complete generation or complete
rollup can release required journal evidence, while active bulk work may pin an
additional routed suffix. Capturing or processing a scoped snapshot is not
retention proof. Only publication of a source-complete generation or complete
accounting rollup may advance the corresponding retention cursor.

### 8.2 Configurable bounds

The following values are non-zero startup configuration and may be changed on
every restart:

- mutation-receipt retention duration, default 24 hours;
- mutation-receipt maximum entries, default 2,000,000;
- mutation-receipt maximum logical bytes, default 512 MiB;
- source-journal maximum retained entries, default 1,000,000; and
- source-journal maximum retained logical bytes, default 512 MiB.

They are node-local resource bounds, not persistent placement decisions and not
Raft state. Reducing a bound below current occupancy is valid: reads and
consumer progress continue, while affected new mutations wait until occupancy
falls below the new bound. A single encoded receipt or journal transition larger
than its configured byte bound is rejected immediately; it cannot wait forever.
The journal capacities are hard admission bounds for every ordinary
source-producing write. Current retained occupancy may exceed them only by the
explicit publication progress debt in Section 8.5.

### 8.3 Receipt backpressure

Before a new command is evaluated, Keldra removes expired receipts within a
bounded work budget and computes the exact entry and logical-byte reservation
for the new receipt. If that reservation exceeds either bound because retained
receipts are still inside their guarantee window, the request waits in a
bounded, process-local admission queue. The queue is not durable and is not an
authority: no object mutation has started while the request is waiting.

Expiry pruning and completed capacity maintenance wake waiters. If the clamped
request deadline expires first, Keldra returns a retryable capacity failure and
does not commit the object, receipt, journal entry, or reference delta. Once a
request is admitted, its existing command identity and unknown-outcome retry
rules apply normally.

Keldra never shortens the configured retention promise, evicts an unexpired
receipt, or acknowledges a non-idempotently-retryable command merely to admit
more writes.

### 8.4 Journal backpressure

Before the object mutation, the coordinator reserves the exact entry and
logical bytes for its source event and routes. It first advances reference
delivery, derived-consumer cursors, and bounded pruning.

Derived-consumer progress is aggregated by source, consumer kind, and ACTIVE
consumer node, not recorded once per index or bucket. Each node's index manager
demultiplexes sparse bucket routes across the rank-zero definitions currently
assigned to that node. It advances its one index-consumer checkpoint for a
source only after every affected assigned definition has incorporated
the preceding offsets in a source-complete generation. Accounting uses one
separate aggregate consumer checkpoint and may advance only through a published
complete rollup. A node with no affected assigned definitions can advance
directly to the settled tail.

Demultiplexing chooses the cheaper of two bounded reads without adding another
log or persistent route format. When the node owns relatively few bucket
assignments, it scans those existing bucket routes. When the source interval
contains fewer records than the required bucket probes, it reads that existing
authoritative journal interval once in source order and filters its typed
bucket identities through the disposable assignment map. A node with no local
rank-zero assignments performs neither read. Work is therefore bounded by the
smaller of assigned-bucket probes and retained interval records rather than
their product. Only a matching bucket resolves definition evidence; an
irrelevant event cannot trigger payload or index work.

This generalizes the existing journal-metadata checkpoint mechanism; it does
not add a definition checkpoint catalogue. The consumer sends its monotone
checkpoint to the source over the authenticated peer path. The source durably
stores the bounded set in its existing journal metadata, keyed by membership
fence, consumer kind, and ACTIVE node ID. Delivery is idempotent. The safe
derived cursor is the minimum across the required ACTIVE consumer checkpoints.
The actual prune-through position is the minimum of that cursor, the durable
`settled_through` watermark, and the independent reference-safe cursor. Public
watch cursors do not pin it. Rank-one and rank-two query owners do not constrain
the source because they never build independently; they consume the rank-zero
node's published generation.

This costs `O(active nodes x consumer kinds)` checkpoint records per source,
not `O(indexes)`, `O(buckets)`, or `O(objects)`. A membership change fences the
old checkpoint set. A newly responsible rank-zero manager completes assignment
reconciliation and recovers each published barrier before the new fence can
release history; a removed node ceases to pin only after committed membership
cutover.

On restart, a consumer loads its local aggregate checkpoint and every assigned
definition's last published barrier. If the source still retains the suffix, it
resumes normal demultiplexing. If a required cursor is below the retained floor
or belongs to an unavailable source epoch, that definition fails closed until
an authorized principal explicitly requests a rebuild. Keldra does not capture
a replacement snapshot merely because history is missing. Only published
source-complete generations or complete accounting rollups justify the next
aggregate acknowledgement. A malformed, future-fence, or future-offset
checkpoint fails closed.

These aggregate checkpoints are retention evidence only. They cannot select a
builder, authorize a definition, supply query data, or publish a generation;
they are neither an index authority nor a registry.

A journal record is removable only when losing it cannot violate reference
correctness and every required aggregate derived-consumer cursor proves that
each affected index has published a source-complete barrier beyond the record
and each affected exact accounting consumer has published a complete rollup
beyond it. A captured, in-progress, completed-but-unpublished, failed, or
discarded scoped snapshot never permits pruning.

If safe pruning cannot satisfy the reservation, the request waits before its
object mutation. Source consumers receive scheduling priority while journal
backpressure is active. If the request deadline expires before admission, it
returns a retryable capacity failure and nothing from that mutation is
committed.

The journal event and object mutation therefore remain indivisible. There is
no mode which writes the object while dropping, overwriting, sampling, or
best-effort-delivering its index evidence.

A failed or deliberately disabled definition can eventually apply backpressure
to writes routed through a source it pins. This is intentional fail-closed
behavior. A principal authorized to update that definition can repair it,
request a scoped rebuild, or delete it through the public API. Keldra does not
silently make customer data disappear from indexed results to preserve write
throughput.

### 8.5 Trusted derived publication progress debt

Journal capacity can otherwise create one circular wait: an index generation
or accounting rollup has consumed the retained suffix and is ready to publish,
but writing its immutable artifacts and current pointer through the ordinary
object path also requires journal entries. Refusing those exact entries at the
capacity boundary would prevent the only durable publication which can advance
the consumer cursor and permit pruning.

Keldra therefore gives one narrow trusted path an admission exception. The
internal publisher of an already constructed source-complete index generation
or complete accounting rollup may reserve the exact journal entries and logical
bytes needed to durably write that publication even when the reservation takes
retained occupancy above a configured journal capacity. The excess entries and
bytes are publication progress debt. This exception applies only to the pack,
run, manifest, rollup, and current-pointer writes required to finish that
publication. It does not admit client writes, ordinary internal writes,
snapshots, rebuild initiation, compaction, cache materialization, or speculative
work, and it does not relax mutation-receipt capacity.

Every debt entry is appended normally and remains subject to the same ordering,
settlement, routing, integrity, and retention rules as any other journal entry.
No event is dropped, sampled, overwritten, hidden from a required consumer, or
declared represented by an unpublished snapshot. Only the durable CAS of the
source-complete generation, or durable publication of the complete accounting
rollup, advances the corresponding consumer checkpoint. Failed or incomplete
publication leaves its evidence retained and must be retried or repaired; it
does not manufacture a prune proof.

While either debt counter is non-zero, all ordinary source-producing mutations
remain under backpressure even if a concurrent prune briefly makes one nominal
reservation fit. Trusted publication work receives scheduling priority so it
can complete, advance the safe cursor, prune proven evidence, and repay the
debt. The observable journal occupancy may consequently exceed its configured
capacity by the exact debt of publication already needed to make progress. A
persistent or growing debt means publication is failing or slower than its
source and is an operational fault, not permission to truncate the journal or
accept more ordinary writes.

## 9. Authorized explicit bulk rebuild

`IndexService` adds one authenticated `RebuildIndex` operation with:

```text
bucket
index name
expected definition version
command identity
```

The caller's tenant comes from its authenticated identity. This is a public
Zanzibar-authorized capability. The same exact permission which permits
updating or putting that index definition permits rebuilding it; no hard-coded
role grants or denies the operation. It does not expose an internal builder
endpoint and it cannot select another tenant's definition.

The implementation exact-reads the ordinary definition and uses
`PutIfVersion(expected_definition_version)` to write the same semantic
definition plus one internal server-time acceptance timestamp as a new ordinary
object version. That timestamp is carried forward unchanged by later semantic
updates and is not exposed as another public resource. The normal typed
definition transition, source journal, assignment delivery, and current-pointer
rules then schedule a bulk build for that new definition version. The response
is the updated `IndexDefinition`; it does not invent a durable job ID.

Concurrent manual or semantic updates resolve through the ordinary definition
CAS. Repeated command identities use the ordinary mutation receipt. No rebuild
registry, job database, Raft entry, side file, or second definition authority is
added.

Admission is rate-limited by the exact stable
`(tenant_id, bucket_id, definition_id)` identity:

- at most one new rebuild command is accepted in any rolling one-hour interval;
- durable acceptance starts the full one-hour interval immediately; and
- success, failure, cancellation, process restart, builder reassignment, and an
  unknown outcome neither shorten nor reset that interval.

Authorization happens before replay, CAS, or rate decisions. When the supplied
expected version is not current, Keldra safely submits the unchanged current
definition through the ordinary mutation path: an exact retry of the immediately
preceding accepted rebuild replays its retained receipt, while another command
can only return the ordinary CAS or idempotency error and cannot create a build.
A fresh request against the current version then checks the one-hour timestamp.
It either writes the next definition version and timestamp together or returns
`RESOURCE_EXHAUSTED` without creating a definition version or build. Acceptance
evidence is durable and the limit is cluster-wide: it survives process restart
and builder reassignment, and sending the request to another public ingress node
cannot bypass it. Its evidence remains part of the ordinary definition authority
and does not create a registry, job database, Raft record, or process-local
authority.

Source lag never starts a bulk rebuild. A history gap, unavailable source epoch,
or malformed cursor fails the affected definition closed until an authorized
principal repairs or explicitly rebuilds it. Hard source-journal capacity is
the sole lag-driven write backpressure mechanism.

## 10. Scoped snapshot and bulk builder

A first build or authorized explicit rebuild uses the same path:

1. Pin the exact ordinary definition version, current ACTIVE membership fence,
   and a clear atomic-program finalized watermark.
2. On every required source, wait for the durable settled watermark to reach
   the captured physical tail, take the existing source commit fence, and open
   a RocksDB snapshot which contains current heads plus that exact source epoch
   and settled tail.
3. Seek only the stable numeric tenant, bucket, and segment-aware path-prefix
   scope. Stream current heads in canonical path order in bounded frames.
4. Group metadata reads by exact replica group and use bounded multi-get pages.
   Fetch only payloads required by that index definition.
5. Project byte-bounded waves concurrently under the kind-wide construction
   budget and process-owned Rayon pool.
6. Restore deterministic path/version order after projection. Partition output
   into deterministic non-overlapping path ranges.
7. Build primary path/version components directly. External-sort only the
   secondary keys whose natural projection order differs from their durable
   order.
8. Stream logical blocks into format-v3 pack objects. No complete component or
   corpus-sized identity map is retained in memory.
9. Continue accepting concurrent source changes into journal offsets after the
   captured tails.
10. Replay the routed suffix after every captured tail, coalescing exact paths
    and preserving atomic-program groups.
11. Publish only when the candidate has one complete membership/source/atomic
    barrier and every referenced pack has met artifact durability.

The snapshot cursor is bound to its RocksDB snapshot and stable
`(tenant_id, bucket_id, path)` ordering. It uses a per-frame inactivity
deadline, not a wall-clock deadline proportional to corpus size. Membership or
source-epoch change cancels the candidate explicitly.

The bulk path writes one or a bounded small number of base runs per deterministic
path range. It does not emit ordinary L0 runs for transport pages and does not
run the same records through every size-tiered level before first publication.

## 11. Bounded parallel source projection

One `TypeBuildPool` exists per supported `IndexKind`. It owns one byte semaphore,
a fair queue across definitions, configured projection and compaction lane
ceilings, and access to the process-owned Rayon pool.

Source processing uses waves bounded by both encoded source bytes and the
worst-case resident projection charge. A wave performs:

1. one deduplicated identity and head lookup plan;
2. bounded concurrent payload opens;
3. parallel pure projection on Rayon;
4. deterministic ordering of projection results; and
5. sequential admission of those ordered results into range-local writers.

The wave holds a kind-budget lease until every result and temporary allocation
is released. Payload sizes, decoded JSON, tokens, vectors, postings, sort runs,
and output buffers are charged. A definition yields after a configured work
quantum so another definition of the same kind cannot starve.

Projection concurrency and source quantum are independently configurable by
kind at startup. Their defaults are four lanes and a 16 MiB source quantum,
subject to the effective minimum of configured lanes, Rayon workers, available
work, and lanes affordable by the kind's byte budget. A smaller budget reduces
effective parallelism rather than allowing hidden allocation.

The default process profile remains four Rayon workers and 256 MiB of
construction memory per kind. With all eight kinds simultaneously saturated,
the explicit aggregate construction ceiling is 2 GiB. Operators may tune each
kind independently on restart.

## 12. Index format v3

### 12.1 Clean format break

`INDEX_FORMAT_VERSION` is 3. The 0.8.0 index engine reads and writes format v3
only. It contains no v2 block decoder, v2 manifest reader, converter, dual-read,
dual-write, fallback, or mixed-generation query path.

Ordinary source objects and ordinary definition objects remain authoritative
and are not an index format. A v3 generation is created from those objects
through the same scoped bulk builder used for any initial build. That builder
never opens v2 artifact bytes. Until a v3 generation publishes, the definition
reports its unavailable/building state rather than serving a v2 generation.

All persistent encodings use explicit field widths, byte order, bounds, and
version tags. Rust native layouts are never durable. Readers reject unknown
required tags, arithmetic overflow, an offset outside its pack, overlapping
logical blocks, invalid ordering, and digest mismatch.

### 12.2 Pack objects

Format-v3 logical blocks retain the existing bounded decoded-memory contract.
Multiple complete logical blocks are concatenated into one immutable pack with
a target payload size of exactly 16 MiB. The final pack of a component or
generation may be smaller. A logical block never straddles two packs, and a pack
is sealed before adding a block which would cross the target.

A logical block descriptor contains at least:

```text
ordinary pack object address and exact object version
byte offset within the pack
encoded byte length
decoded byte bound
component codec and block kind
first and last ordered key where the component is routed
BLAKE3 digest of the exact encoded logical block
```

Offsets and lengths address raw pack bytes. Whole-pack compression is forbidden
because it would turn one small range read into a complete-pack decode;
individual logical-block codecs remain free to compress their own content.

A pack belongs to one candidate generation and one writer lane. It contains
consecutive complete logical blocks emitted by that lane and never bytes owned
by another generation. Components need not coordinate a process-wide pack
buffer merely to fill every final pack. This keeps parallel writers independent
and retention and failed-publication cleanup generation-scoped. Run roots
remain individual ordinary objects and refer to logical block descriptors;
manifests refer to run roots and packs; the current pointer refers to one
manifest.

Pack writes use the ordinary object path with `REPLICATED` acknowledgement when
the cluster can provide it and the ordinary one-node `LOCAL` acknowledgement
otherwise. The pack's content address, reference count, inline/erasure-coded
placement, awaiting-publish grace, and garbage collection are exactly those of
any other ordinary object.

Compared with one ordinary object per approximately 512 KiB logical block, a
16 MiB target removes roughly thirty-two fixed object-publication operations
for a full pack. This reduces object heads, journal entries, reference updates,
authorization work, synchronous writes, and cache filenames without increasing
the maximum logical decode allocation.

### 12.3 Async cached access

The shared cache exposes Keldra's async file-like index handle, not a Rust
`File`. A read requests `(offset, max_length)` and returns an owned or
reference-counted immutable byte slice whose length is the available result.
The handle may materialize or range-fetch the required pack in the background.

Valid pack bytes may be mmap-backed through `memmap2`. Decoded immutable routing
and leaf pages may also be cached under the same shared memory budget and keyed
by pack version, offset, length, codec, and digest. Concurrent readers share
immutable pages; cache metadata locks are not held during I/O, decode, query
evaluation, or an `await`.

Memory and disk cache are disposable. A missing or corrupt entry is evicted and
refetched from authoritative ordinary storage. Cache startup performs no full
filesystem inventory or purge.

## 13. Stable identities and compressed postings

Every live or tombstoned index record retains the exact stable source identity
`(path, object_version)`. Compaction never replaces it with a generation-wide
dense ordinal.

Each immutable path-range component stores a sorted identity dictionary and may
assign dense local ordinals inside that range. Derived postings identify their
range plus local ordinal. When compaction rewrites a range, only that output
range's local dictionary is assigned; unrelated ranges and runs are neither
counted nor renumbered. An unchanged immutable run remains directly reusable.

Metadata Filter and Typed JSON format-v3 components use:

- a dictionary of configured field identities and canonical typed scalar keys;
- sorted compressed posting lists of range-local identities;
- explicit live/tombstone version state in the range's identity component; and
- succinct monotone offsets and posting sequences implemented with approved
  `sux` structures.

Equality and `IN` predicates seek exact dictionary ranges and intersect or
union compressed postings. Prefix and numeric/string range predicates seek the
first canonical key and stream only keys within the half-open predicate range.
`EXISTS` uses the field dictionary rather than walking every document. Final
hits resolve range-local identities and validate the winning live version.

This representation avoids one routed row for every
`(field, value, document)` while retaining canonical JSON type distinctions,
array multi-value semantics, result deduplication, deterministic ordering, and
exact tombstone handling.

Full Text and Hybrid retain compressed term postings but adopt the same stable
version and range-local identity boundary. Path, Vector, Git Source, and Tensor
components use the stable identity dictionary directly rather than requiring a
global ordinal pass.

## 14. Source-complete publication and bounded compaction debt

An incremental builder seals an immutable L0 run when its byte-budgeted source
quantum ends or its resident builder is full. Once that run and its pack objects
meet artifact durability, the rank-zero builder may publish a generation which
contains the previous complete runs plus the new L0 and advances the complete
source barrier.

Publication does not wait for every level to satisfy its preferred fan-in.
Queries apply newest-run shadowing and tombstones across the manifest's bounded
run set, so the generation is complete even while compaction is pending.

Every kind has startup-configured maximum uncompacted runs and maximum encoded
uncompacted bytes per level. The defaults are 64 runs and 1 GiB per kind and
level. The encoded-byte limit applies once a level has at least two runs: one
indivisible run may be larger than the configured limit, but it is never joined
by another published run until compaction has restored the bound. The scheduler
will not publish another
incremental run which would cross either bound. It prioritizes compaction for
that definition while retaining the last complete generation. Continued source
lag is protected by the journal backpressure rules in Section 8; Keldra does not
create an unbounded manifest to avoid applying pressure.

Compaction reads immutable runs, writes replacement packs, and CAS-publishes a
new generation. Its complete source barrier is identical to or newer than the
generation it replaces. Inputs remain readable until the replacement wins and
retention makes them eligible. A failed compaction leaves the current complete
generation untouched.

This separates two facts which 0.7 conflated:

- whether every source change through the barrier is represented; and
- whether the immutable representation has reached its preferred read shape.

Only the first gates freshness publication. The second is bounded operational
debt.

If a source quantum contains no mutation matching the definition, the builder
publishes a new manifest and current pointer with the unchanged immutable run
set and the advanced complete source barrier. This checkpoint-only generation
uses the same artifact validation and current-pointer CAS as every other
generation. An idle eviction, process restart, or builder reassignment can
therefore recover zero-lag evidence from ordinary authoritative objects rather
than an in-memory pseudo-checkpoint.

## 15. All-kind parallel compaction

Path, Metadata Filter, Typed JSON, Full Text, Vector, Hybrid, Git Source, and
Tensor all use deterministic range-striped compaction.

The coordinator divides the ordered key space into non-overlapping half-open
ranges. Each admitted lane acquires its complete reader, decoded-page,
projection, sort, and pack-writer workspace from the kind-wide byte budget. It
merges one range, emits format-v3 logical blocks into lane-local packs, and
returns only bounded descriptors and statistics.

Derived keys which require a different order use bounded external-sort runs.
Their merge is itself split by deterministic key range; no index kind feeds its
final routed-key component through one unbounded or serial whole-index writer.
The default external-sort source chunk is 16 MiB, reduced automatically when a
kind budget cannot afford it.

After all lanes finish, one coordinator validates disjoint range summaries and
assembles their roots in key order. It does not copy the lane payloads through a
serial buffer. Completion order cannot affect logical records. No range becomes
visible independently: publication remains one generation CAS.

Each kind has its own startup-configured maximum compaction lanes and byte
budget. Effective lanes are the minimum of:

- that kind's configured lane ceiling;
- process-owned Rayon workers;
- deterministic non-empty ranges;
- work admitted by the kind-wide fair scheduler; and
- lanes affordable under the kind's current byte budget.

The default ceiling is four lanes per kind. A one-lane configuration remains a
fully supported deterministic execution profile. No kind creates a private
Rayon pool or bypasses its shared budget.

## 16. Artifact publication and retention

Logical block emission only stages pack content. The publisher seals full packs
and publishes descendant packs through bounded ordinary `BulkWrite` requests,
then publishes run roots, the generation manifest, and finally the current
pointer. It never awaits thousands of independent descendant `Put` operations
in sequence.

Distributed publication groups pack operations by their exact metadata replica
group and preserves ordinary per-object receipt, durability, reference, and
authorization semantics. There is no cross-group atomic claim. A generation is
invisible until every descendant succeeds and the one current-pointer CAS wins.

Obsolete generations remain subject to the configured maximum generation
count, age, and authoritative bytes. The first exceeded bound makes the oldest
non-current generation eligible after the minimum in-flight-query safety age.
Deleting a generation deletes its ordinary manifest, roots, and generation-owned
packs through ordinary reference counting and garbage collection.

Retention is incremental and scoped to reserved artifact paths for due
definitions. Each tick has record, byte, and time limits plus a resumable local
cursor. It never runs one global object-head scan.

## 17. Query execution and control-plane scheduling

One query executes on one of the current top-three weighted-HRW owners. There is
no scatter/gather plan and no result merge between independent indexes.

The query pins definition version, format-v3 generation, authorization
revision, membership fence, and page-token identity. It opens immutable pack
slices lazily and retains bounded iterator or top-k state.

Routed cursors always receive the tightest known half-open key range:

- equality uses `[encoded_key, successor(encoded_key))`;
- prefix uses `[encoded_prefix, prefix_successor(encoded_prefix))`;
- range predicates use their canonical inclusive/exclusive bounds; and
- unbounded traversal is used only for an operation whose semantics truly
  request the whole ordered component.

A lookup therefore costs routing height plus matching leaves, visibility
validation, and Zanzibar filtering. It must not decode every routed page merely
to reject it after decoding.

Latest-live checks and exact authorization checks are grouped by stable bucket
and metadata replica group and executed as bounded multi-gets/batches. A query
does not issue one serialized distributed read per candidate when the same
authority can evaluate a page.

Keldra maintains a small process-owned control-plane executor reserved for
serving-fence renewal, membership progress, assignment fences, and peer-health
work. Index queries, projection, compression, sorting, merge, ranking, and
decoded-page construction never execute CPU work on that executor. Async index
tasks perform bounded I/O and submit CPU chunks to the shared Rayon pool; they
yield between chunks and never hold cache or scheduler locks across `await`.

This is scheduling isolation inside one Keldra process. It is not another
server, port, persistence plane, or authority.

Authorization proceeds coarse to fine:

1. authenticate the caller, or use the fixed anonymous principal only for an
   explicitly public query;
2. authorize access to the exact ordinary definition and its stable
   tenant/bucket/path boundary at one Zanzibar revision;
3. eliminate candidates outside that boundary in routing;
4. apply a proven whole-boundary Zanzibar grant when one exists; otherwise
   exact-authorize bounded candidate pages; and
5. return only hits which are both live and authorized at the pinned revision.

An index never grants access. Results include the complete freshness structure,
including source barriers, observed lag, generation, definition version,
placement fence, initial-build completion, and rebuild state. Lag returns valid
results plus evidence; it is not by itself an `INDEX_LAGGING` failure.

`QueryIndex` has a separate startup-configured server execution maximum,
defaulting to 300 seconds. A valid shorter client `grpc-timeout` still wins and
is propagated through downstream work. Other bounded unary APIs retain the
30-second server maximum. This separation allows cold pack materialization and
authorized first-page filtering to make bounded progress without weakening
deadlines for inexpensive control-plane operations.

## 18. Ingest, authorization, and routing path

`BulkWrite` remains a collection of independently reported object operations,
not a cross-object transaction. The public limit remains 1,000 operations and
64 MiB per request. Implementations and qualification clients use the largest
batch which fits both bounds instead of imposing a smaller hidden 256-operation
ceiling.

The server applies these request-scoped optimizations without changing object
semantics:

1. Resolve each distinct mutable tenant and bucket name to its stable numeric
   identity once. Reuse those IDs for authorization, accounting, key encoding,
   and placement.
2. Pin one Zanzibar realm revision. Deduplicate checks by
   `(tenant_id, bucket_id, action)`. When an exact bucket-wide relation proves
   every addressed object allowed, do not evaluate identical object fallbacks.
   If it does not, evaluate the exact object paths; denial is never cached as an
   anonymous request or bypass.
3. Compile and cache the disposable Zanzibar evaluation graph by authoritative
   realm revision. A revision change cannot reuse a positive decision from an
   older graph.
4. On a one-node topology, use the known local metadata authority without
   hashing every path. On a cluster, compute placement once per exact metadata
   replica-group key and group operations by that complete group, not merely by
   the first node.
5. Forward one bounded logical batch per replica group. The coordinator and
   replicas apply one physical RocksDB `WriteBatch` for the group while retaining
   an outcome and mutation receipt per public operation.
6. Preload receipts, heads, inline-blob existence, and blob-reference state with
   bounded RocksDB multi-get. Evaluate repeated paths in original request order
   against an in-memory pending map so `PutIf*` and delete CAS results are
   unchanged.
7. Hash and validate each payload once. Borrow or move the payload on a local
   route and serialize it only when a remote route is selected; do not clone
   every payload speculatively.
8. Accumulate accounting and journal counters for the complete physical batch
   and update them once in the same RocksDB batch rather than taking one global
   mutex per object.
9. Perform one synchronous RocksDB commit per local replica-group batch rather
   than one per object. Cross-group success is not falsely presented as atomic.

Returned operation results remain in client input order. Retry identity,
versioning, CAS, immutable-path, reference-counting, inline/erasure-coded
payload, `LOCAL`/`REPLICATED` acknowledgement, and Zanzibar behavior do not
change.

## 19. Supported index kinds

Every public kind uses the common definition, bulk/replay selection, bounded
projection, format-v3 packs, source-complete publication, compaction-debt,
query-placement, cache, authorization, and observability paths.

### 19.1 Path

The bulk builder streams canonical paths directly into range dictionaries.
Incremental runs carry path/version live state or tombstones. Prefix listing
seeks only intersecting ranges and k-way merges the bounded run set. Succinct
offsets and prefix-compressed path bytes remain appropriate; global document
ordinals are unnecessary.

### 19.2 Metadata Filter

The fixed head projection includes path, version, content type, content length,
content hash, and commit time. Bulk projection writes the stable range-local
identity table and external-sorts canonical field values into compressed
postings. Equality, set, prefix, existence, and range predicates seek
dictionaries and intersect postings before exact live/version validation.

### 19.3 Typed JSON

Only configured JSON pointers are parsed. Canonical scalar encodings preserve
JSON type; arrays may contribute multiple values while one stable version
identity deduplicates results. The bulk builder partitions documents by path,
then externally sorts field/value postings in bounded chunks. Equality and
range queries seek compressed postings rather than traversing the twelve-field
key space.

### 19.4 Full Text

Bounded parallel tokenization produces term postings and optional positions
against range-local identities. Bulk build externally sorts terms once;
incremental runs remain immutable. Merged postings use gap/quasi-succinct
encoding. Boolean and phrase iterators retain bounded state, and ranking uses a
bounded top-k heap on the shared CPU pool.

### 19.5 Vector

The 0.8 engine remains exact. Fixed-width portable vector blocks align with
path ranges and stable identities. Queries scan the selected vector blocks in
bounded parallel chunks and maintain a top-k heap; they do not load the corpus
or introduce a mutable ANN graph. Compaction packs blocks and identity
dictionaries without a global ordinal pass.

### 19.6 Hybrid

Text and vector components share one stable path/version identity table and one
source-complete barrier. Bulk build constructs both families under one kind
budget. Query fuses bounded candidate streams deterministically. Publication
and compaction replace both families together, never one partial side.

### 19.7 Git Source

Commit, tree-path, and object relationships use sorted tuples and compressed
routed postings over stable source identities. Object bodies remain ordinary
Keldra objects and are not copied into packs. Commit and tree-prefix queries seek
only matching tuple ranges.

### 19.8 Tensor

Tensor name, shape, data type, model identity, and source location use canonical
dictionaries and compressed routed postings. Tensor payload bytes remain
ordinary objects. Name/range queries seek postings; shape and type filters
intersect bounded candidate streams.

## 20. Atomic-program visibility

The complete barrier includes the nominated executor's finalized atomic-program
watermark. Projection may process program paths in parallel, but a generation
cannot publish with only part of one finalized program represented.

If a candidate encounters an in-progress program at its boundary, it retains
the previous complete generation and waits or advances to a later clear
barrier. Bulk rebuild suffix replay treats the program's path set as one
visibility unit. Format-v3 packing and range striping do not make individual
paths or ranges independently visible.

Ordinary non-program writes remain outside atomic-program orchestration. Index
atomicity follows ordinary API visibility; it does not force unrelated object
writes into a transaction.

## 21. Accounting

Accounting definitions retain the same ordinary-object authority, transactional
locator, top-three assignment, sparse bucket routes, scoped snapshot stream,
complete barrier, and current-rollup CAS as index definitions.

Exact stored bytes and object counts constrain journal pruning until a complete
rollup makes older events unnecessary. A captured or completed-but-unpublished
scoped snapshot is not retention evidence. Their receipt and journal pressure
follows Section 8. Normal restart resumes from the published rollup barrier;
first build or a true source-history gap uses a bounded scoped bulk baseline.
The hard journal cap prevents ordinary accounting lag from manufacturing that
gap; source-epoch replacement may still require the scoped baseline. Only its
complete published rollup can advance the accounting retention cursor.

Per-object ingress and egress accounting remains bounded best-effort telemetry,
not a financial ledger. Weighted HRW selects one disposable matcher for each
stable numeric bucket. It lazily loads only that bucket's definitions, applies
segment-aware prefixes, aggregates batches idempotently, and exports dropped
batch/byte signals. No globally exact traffic log or per-bucket Raft record is
introduced.

## 22. Recovery and maintenance

Recovery is typed and scoped:

| Failure | Required behavior |
| --- | --- |
| Process crash during bulk build or compaction | Discard disposable scratch. Keep serving the last source-complete generation and, for a first build or accepted explicit rebuild, restart construction for the same ordinary definition version. |
| Source lag reaches the configured journal entry or byte capacity | Prioritize consumers and hold new source-producing mutations before commit until published progress permits safe pruning. Never switch construction paths merely because of lag. |
| Index source cursor below retained floor or wrong epoch | Fail the affected index definition closed until an authorized principal explicitly requests a rebuild. Do not capture a replacement snapshot automatically. |
| Accounting source cursor below retained floor or wrong epoch | Capture only that accounting definition's bounded scoped baseline; retain source evidence until its complete rollup publishes. |
| Pack upload or current-pointer CAS failure | Keep the prior complete generation; ordinary unreachable packs become retention candidates. |
| Corrupt format-v3 pack, logical block, run, or manifest, or an unknown future required tag | Fail that definition closed and alert; never broaden into a corpus scan. Format-v2 artifacts are never opened. |
| Missing or corrupt disposable cache entry | Evict and refetch the exact ordinary pack. |
| Membership fence changes before publication | Cancel the candidate, recompute weighted-HRW ownership, and resume on the current rank-zero owner. |
| Receipt capacity exhausted | Wait before mutation admission until expiry pruning frees the configured reservation. |
| Journal capacity exhausted | Prioritize consumers and wait before mutation admission until the exact event reservation is safe. |
| Journal capacity blocks the trusted publication required to release retained evidence | Admit only the exact generation or rollup publication entries as progress debt, keep ordinary writes blocked, publish durably, then prune and repay the debt. |
| Compaction debt limit reached | Stop publishing more debt, compact within the kind budget, and allow journal backpressure if lag reaches its hard bound. |

Garbage collection, artifact retention, cache reconciliation, definition
reconciliation, and source proof cleanup use bounded record, byte, and time
budgets with resumable scoped cursors. Core object service starts before them.
No recovery failure triggers a global head scan, blob scan, artifact scan, or
cache purge.

## 23. Operational configuration

All values in this section are validated at startup and may be changed on
restart. They are not placed in Raft and do not change durable format:

- mutation-receipt retention seconds, maximum entries, and maximum bytes;
- source-journal maximum entries and maximum bytes;
- process Rayon worker count, default four;
- per-kind construction memory, default 256 MiB per kind;
- per-kind projection lanes, default four;
- per-kind source quantum, default 16 MiB;
- per-kind compaction lanes, default four;
- per-kind external-sort chunk bytes, default 16 MiB when affordable;
- per-kind compaction-debt run and encoded-byte limits, default 64 runs and
  1 GiB per level;
- global index-cache disk bytes and memory percentage;
- query concurrency, default 64, and a cache-read CPU-work quantum, default
  4 MiB between cooperative yields;
- index-query maximum execution time, default 300 seconds, independently
  clamped by a shorter client deadline; and
- generation count, age, and authoritative-byte retention limits.

The 16 MiB format-v3 pack target is part of format v3 rather than an operator
knob. Changing that packing contract belongs in a later format decision.

Invalid zero values and configurations whose fixed workspace cannot fit the
declared kind budget fail at startup with the exact setting named. Operators
may configure different local resource budgets on heterogeneous nodes; the
durable bytes and query results remain portable and deterministic.

## 24. Observability

Keldra exports OTLP metrics and traces for the complete critical path. Metrics
use low-cardinality labels such as operation, index kind, phase, level,
trigger, outcome, and limiting factor. Stable numeric IDs belong in trace and
structured-log fields, never metric labels. Mutable tenant, bucket, index,
path, field value, token, vector, payload, and credential data are never
exported.

### 24.1 Ingest and authorization

- public requests, operations, payload bytes, batches, batch size, latency, and
  outcomes;
- stable-name resolutions, cache hits, and lookups by identity type;
- bucket-wide and exact-object Zanzibar checks, compiled-revision cache hits,
  evaluation duration, and authorization revision;
- placement calculations, one-node fast paths, replica-group batches, local
  and remote operations;
- RocksDB multi-get keys/bytes/duration, write-batch operations/bytes, sync
  duration, and failures; and
- payload hash bytes/duration, clones avoided, reference work, and accounting
  aggregation duration.

### 24.2 Receipts, journals, and backpressure

- configured and current receipt entries/bytes, oldest retained age, expiry
  pruning rate, active/waiting writers, wait duration, deadline outcomes, and
  time to projected capacity;
- configured and current journal entries/bytes, settled tail, retention floor,
  safe-prune cursor, per-consumer lag, prune rate, active/waiting writers, wait
  duration, deadline outcomes, limiting bound, and source priority while
  pressured;
- current and peak publication progress-debt entries/bytes, plus the ordinary
  mutation backpressure wait and publication outcome signals which show whether
  debt is blocking writers or making progress; and
- public rebuild request outcomes, including authorization, stale-CAS, and
  one-hour rate-limit failures, plus builder rebuild progress and outcome.

### 24.3 Build and publication

- scoped snapshot frames, records, bytes, reads, payload fetches, elapsed time,
  throughput, last-progress age, and suffix lag;
- projection waves, input/output records/bytes, configured/effective/active/
  waiting lanes, byte-budget use, Rayon queue time, CPU time, and failures by
  kind;
- external-sort chunks, spill bytes, merge passes, transient versus
  authoritative bytes, and peak workspace;
- logical blocks, packs, fill ratio, bytes, publish batches, durability time,
  roots, manifests, current-pointer CAS, and failures;
- source-complete generation age, source barriers, publication rate, and
  rebuilding state; and
- compaction debt runs/bytes, admission stops, repayment rate, and oldest debt
  age.

### 24.4 Compaction and query

- compaction level, input records/runs/blocks/bytes, configured/effective/
  active/waiting lanes, ranges total/completed, output records/blocks/packs/
  bytes, phase duration, last-progress age, retries, failures, and limiting
  factor for every index kind;
- query routed pages, range seeks, decoded blocks, cache hits/misses/refetches,
  candidates, live-version checks, Zanzibar checks, result hits, CPU queue
  time, CPU time, latency, and outcomes by kind; and
- control-plane executor queue depth, task latency, serving-fence renewal
  lateness, lease margin, membership progress, and missed deadlines.

Long-running snapshot, projection, sort, pack, publication, compaction, and
query phases carry nested trace spans with stable numeric definition and
generation IDs. Progress is emitted as bounded span events and periodic
structured logs, not by repeatedly overwriting one span field. Failures name
the exact phase and durable barrier which remains valid. Ordinary successful
per-object work does not generate one log line per object. Publication debt is
sampled as current and process-lifetime peak entry/byte gauges without placing
tenant, bucket, definition, or path values in metric labels.

## 25. Complexity contract

Let `O` be every ordinary object, `D` every definition, `A_i` definitions
assigned to node `i`, `S_p` live objects in one definition scope, `C_b` relevant
changes in one bucket, `R` bounded runs in one current generation, `P` packs
touched by a query, and `Q` one maintenance budget.

| Operation | 0.8.0 bound |
| --- | ---: |
| Core service startup | `O(1)` before serving |
| Assigned runtime startup on node `i` | `O(A_i + pending sparse changes)` streamed |
| Definition mutation | `O(1)` transaction plus at most three assignment deliveries |
| Incremental catch-up | `O(C_b)` routed, then exact path-scope filter |
| Bulk first build or rebuild | `O(S_p)` source pass plus bounded external-sort I/O |
| Current-generation query | `O(routing height + matching blocks across R + returned-candidate validation)` |
| Exact equality lookup | independent of unrelated routed keys |
| Maintenance tick | `O(Q)` |
| Empty tenant or bucket | zero derived-runtime state |

`R` is bounded by configured compaction-debt limits. Memory is bounded by the
sum of explicitly configured kind, cache, query, and ingress budgets, not by
`S_p`. Pack/object metadata grows with approximately one ordinary artifact per
16 MiB of encoded index data rather than one per logical block.

The design removes known superlinear execution caused by routing every initial
record through all LSM levels and globally renumbering documents. External sort
remains `O(N log N)` I/O for component keys which do not arrive in durable
order. Exact vector search remains linear in selected vectors.

## 26. Validation matrix

### 26.1 Format and engine tests

- Format-v3 encode/decode round trips on AMD64 and ARM64 with byte-identical
  golden fixtures.
- V2 artifacts are not decoded, converted, mixed, or served.
- Pack offsets, lengths, digests, ordering, overlap, truncation, integer
  overflow, unknown codec, and decoded-size bounds fail closed.
- A full pack approaches 16 MiB without splitting a logical block; a final
  partial pack and one maximum logical block remain valid.
- Every kind supports first build, incremental update, overwrite, tombstone,
  compaction, pagination, restart, and query through format-v3 packs.
- Stable version and range-local identity tests prove no cross-range dense
  ordinal dependency and exact tombstone/version resolution.
- Metadata and Typed JSON compressed postings cover equality, `IN`, prefix,
  range, order, `EXISTS`, arrays, mixed JSON scalar types, and duplicate values.

### 26.2 Concurrency and resource tests

- One-lane and four-lane projection/compaction produce identical logical
  ordered results for all eight kinds.
- Deliberately reordered lane completion produces the same range summaries and
  root order.
- Every allocation is covered by the per-kind budget; lowering a budget reduces
  lanes without exceeding it.
- Multiple definitions of one kind share the budget fairly and a noisy
  definition cannot monopolize every turn.
- Publication with bounded debt returns complete results before compaction,
  after compaction, and during a failed replacement CAS.
- Debt limits prevent another run publication and eventually cause measured
  journal backpressure without growing memory or manifests unboundedly.
- CPU-saturating queries and compactions do not cause a serving-fence renewal to
  miss its lease.

### 26.3 Backpressure and rebuild tests

- Tiny receipt limits block a new mutation before commit, wake after expiry,
  preserve idempotent retry, and never evict an unexpired receipt.
- Tiny journal limits block a new mutation before commit, prioritize consumers,
  wake after safe cursor advance, and never commit an object without its event.
- With a journal exactly at its entry or byte bound, the trusted publication of
  an already constructed source-complete generation and a complete accounting
  rollup may each incur exact progress debt, publish durably, advance the safe
  cursor, prune, repay the debt, and wake the blocked ordinary writer.
- Publication progress debt survives publication failure and process restart
  as retained journal evidence. It never admits an ordinary or speculative
  write, drops an event, advances a cursor from a snapshot, or becomes an
  unreported capacity bypass.
- Lowering limits on restart begins in backpressure and drains safely.
- Entry and byte capacity independently block mutation admission only at their
  configured hard journal bounds; neither lag level starts a rebuild.
- An in-progress scoped snapshot never advances a derived-consumer retention
  cursor. Only a published source-complete generation or complete accounting
  rollup permits the corresponding event to be pruned.
- A missing retained cursor or unavailable source epoch fails the index definition
  closed and does not start a snapshot until an authorized rebuild is accepted.
- The public rebuild API rewrites the same ordinary definition by CAS, rejects
  stale versions and callers without definition-update permission, and creates
  no job or registry state.
- Any principal can rebuild exactly when Zanzibar grants the exact
  definition-update permission; no hard-coded role changes the result.
- One newly accepted rebuild blocks another for one full hour regardless of
  success, failure, cancellation, restart, reassignment, or unknown outcome.
  The result is identical across public ingress nodes and after process restart.
- Replaying the immediately preceding rebuild command identity returns its
  original outcome without a second definition version or build. After an
  intervening semantic definition update, the old rebuild input is no longer
  byte-identical and returns the ordinary idempotency-input-mismatch error;
  concurrent rebuild and semantic updates still resolve by the ordinary CAS and
  never schedule a second build from that retry.
- Concurrent source writes during a bulk snapshot appear exactly once after
  suffix replay.
- A finalized atomic program is entirely present or entirely absent from each
  published generation.
- Focused tiny-capacity tests delay the derived consumer and prove the exact
  entry and byte admission boundaries, trusted publication progress, safe
  pruning, debt repayment, and writer wake-up independently of the large-corpus
  performance qualification.

### 26.4 Public single-node and three-node tests

- Official public API clients create, rebuild, query, update, and delete every
  index kind with private and explicitly public Zanzibar policy.
- Writes through each cluster ingress node are grouped by replica group and
  reach the one rank-zero builder.
- Builder failure and membership cutover move ownership without dual
  publication or a global scan.
- Range-seeking equality returns exact results without reading unrelated routed
  leaves.
- Artifact publication exercises ordinary inline and erasure-coded paths,
  `LOCAL` and `REPLICATED` acknowledgement, restart, cache refetch, reference
  counting, and retention.
- Receipt and journal backpressure preserve correctness under concurrent public
  writes on all ingress nodes.

### 26.5 Production-shaped qualification

The exact candidate binaries are built first and qualification runs them
directly, without holding the shared Cargo build lock. The evidence record pins
the source commit, container digest, native architecture, hardware, corpus hash,
topology, durability, batch size, concurrency, every resource configuration,
timer boundaries, and correctness result.

On the documented reference hardware, the approximately 840,000-record,
twelve-field qualification must:

- use public `BulkWrite` requests up to the public 1,000-operation/64 MiB bound;
- sustain at least 3,000 accepted source objects per second over the measured
  ingest interval;
- sustain at least 1,000 source objects per second through Typed JSON
  projection and source-complete generation publication;
- return exact expected results for every partition before and after updates,
  deletes, restart, and compaction;
- show zero missing events, dropped required journal evidence, authorization
  violations, OOMs, restarts, serving-fence failures, or unbounded debt; and
- record pack fill, artifact count, physical bytes, CPU, memory, cache, journal,
  receipt, backpressure, phase throughput, and publication metrics.

A separate bounded scale run demonstrates that increasing corpus size does not
increase peak configured memory, create a generation beyond debt limits, or
degrade equality query work into a full routed-tree traversal. Performance is
reported separately for ingest, projection, sort, pack publication, suffix
catch-up, compaction, and verification; one aggregate duration is insufficient.
The focused capacity tests in Section 26.3, rather than this throughput run,
exercise deliberately delayed consumption at tiny journal bounds.

### 26.6 Release gates

Before tagging 0.8.0:

1. format, formatting, Clippy, dependency policy, and the repository 2,000-line
   handwritten source-file limit pass;
2. focused format, engine, backpressure, rebuild, control-plane, authorization,
   and persistence tests pass;
3. locked workspace/all-target tests pass locally;
4. public single-node and three-node Docker qualification passes every index
   kind and relevant ordinary object/gateway behavior;
5. the production-shaped performance and exact-result gates in Section 26.5
   pass from direct candidate binaries;
6. populated restart proves zero global object-head, artifact, blob, or cache
   scans before serving;
7. rolling three-node restart and builder reassignment preserve complete
   generations and backpressure guarantees;
8. AMD64 and ARM64 images are built from the exact candidate commit as one
   multi-platform image;
9. publishable crates, image contents, generated files, and the bounded outgoing
   commit range pass privacy and secret checks; and
10. the exact validated commit is pushed, tagged `0.8.0`, released, and every
    package version, image digest, child platform digest, and forge release is
    independently verified.

Data loss, an acknowledged mutation without retained/rebuildable index evidence,
unbounded construction memory, partial atomic-program visibility, unauthorized
results, failure of a supported index kind, serving-fence starvation under the
qualified load, and a global startup scan are release blockers. Optional
ranking quality, ANN search, and further throughput work are limitations rather
than reasons to weaken these gates.

## 27. Known limitations

- Vector search is exact and therefore linear in the selected vector scope. An
  ANN format requires a later explicit design.
- Typed JSON and Metadata Filter conjunctions choose one bounded posting range
  as the driver and evaluate the remaining predicates against those projected
  candidates. Every predicate remains exact and bounded, but 0.8.0 does not
  intersect multiple compressed posting streams; an unselective driver may
  therefore read more candidate rows than a range-local intersection.
- One index query executes on one owner. Very large indexes may fetch packs over
  the ordinary distributed object path, but Keldra does not scatter the query or
  merge network result sets.
- A permanently failed index or accounting definition can hold journal space
  and apply write backpressure. This preserves visibility correctness; the
  definition must be repaired, explicitly rebuilt, or deleted by a principal
  with the required Zanzibar permission.
- An exact rebuild-command retry replays its retained receipt while that rebuild
  remains the current definition version. If a semantic definition update has
  since changed the canonical body, retrying the older rebuild command returns
  the ordinary idempotency-input-mismatch error instead of reconstructing a
  historical response. The accepted rebuild and one-hour limit remain intact;
  use the current definition version and a fresh command after the window.
- Capacity and lane settings change on restart, not through a runtime control
  API in 0.8.0.
- Format-v2 index generations are not queryable, converted, or cleaned through
  a special compatibility path. Format-v3 rebuild reads authoritative ordinary
  source objects, never v2 artifact bytes.
- Pack range access can still require fetching a complete ordinary pack when
  the underlying object path cannot satisfy a smaller efficient range. The
  fixed 16 MiB bound makes that cost finite.
- External sort creates disposable local scratch I/O for secondary components;
  a crash repeats that bounded scoped work.
- Bandwidth accounting remains explicitly best effort. Stored-byte and object
  counts remain exact at their reported complete barrier.
- Mesh and region coordination are outside this single-cluster RFC.

## 28. Consequences

Format v3 is a clean break which removes millions of small artifact objects and
the global-ordinal constraint at the cost of rebuilding indexes from ordinary
source data. The same bulk path is used for first build and explicitly
authorized replacement or repair, so no migration-only subsystem is required.

Publishing complete generations ahead of compaction improves freshness while
the explicit debt limit prevents query and storage costs from becoming
unbounded. When configured resources cannot keep up, pressure moves to writes
before their object mutation. This makes overload visible and preserves the
promise that accepted customer data will appear in a future complete index.

Packed artifacts and batched ingest reduce fixed metadata work without changing
ordinary object authority, durability, erasure coding, reference counting,
Zanzibar, or garbage collection. Parallel projection and compaction use the
already approved Rayon execution model and the existing per-kind memory
budgets; no new execution or persistence dependency is introduced.

The resulting system remains deliberately simple at the authority boundary:
ordinary source objects are truth, the source journal orders their transitions,
ordinary format-v3 objects hold immutable derived bytes, and one CAS chooses the
current complete generation.

## 29. References

- MG4J manual, [building batches](https://vigna.di.unimi.it/MG4J/man/manual/ch02s03.html).
- MG4J manual, [combining batches](https://vigna.di.unimi.it/MG4J/man/manual/ch02s04.html).
- Sebastiano Vigna, [Quasi-Succinct Indices](https://vigna.di.unimi.it/ftp/papers/QuasiSuccinctIndices.pdf), WSDM 2013.
- Sebastiano Vigna et al., [`sux`](https://github.com/vigna/sux-rs).
- Tommaso Fontana, Sebastiano Vigna et al., [`epserde`](https://github.com/vigna/epserde-rs).
- Gonzalo Navarro and Veli Makinen, [Compressed Full-Text Indexes](https://users.dcc.uchile.cl/~gnavarro/ps/acmcs06.pdf), ACM Computing Surveys 39(1), 2007.
- Rayon developers, [Rayon](https://github.com/rayon-rs/rayon).
- RocksDB, [MultiGet API](https://github.com/facebook/rocksdb/wiki/MultiGet-Performance).
