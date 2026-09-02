# KELDRA-0016: Journal-Backed Partition Segment Publication

Status: Accepted architecture; implementation in progress

Amends: KELDRA-0014 sections 5, 7.2, 7.3, 9.5, 9.6, 14, 17, 19,
20, 21, and 23

KELDRA-0020 defines logical catalog sharing and the memory-first distributed
projection pipeline. This RFC defines the durable publication, recovery,
retention, and compaction protocol used by that pipeline.

Audience: Keldra implementors, operators, client authors, and reviewers

## 1. Decision

Keldra uses source-local durable journals and exact retained ordinary object
versions as replay authority for derived-index construction. Every data node
projects the source partitions assigned to it. There is no external builder,
rank-zero definition builder, per-definition consumer, or central assembler.

The normal path derives compact indexable state from object bytes already in
memory, holds it in bounded stage-specific memory, and writes only amortized
immutable segment or head-delta artifacts. RocksDB records the existing source
journal and the small durable partition publication metadata; it does not
receive a duplicate indexing WAL or one posting record per source mutation.

For each source partition the durable derived state contains:

- immutable physical segment and head-delta artifacts;
- immutable partition projection roots naming complete point-in-time views;
- one exact-version current pointer selecting the newest partition root; and
- one small durable family partition directory changed only by partition
  lifecycle transitions; and
- a contiguous source and atomic checkpoint in that selected root.

Independent partitions publish independently. No normal flush updates a global
manifest or every logical definition. Query nodes pin a vector of partition
roots and materialize their union.

Typed JSON is the only admitted index kind in this milestone. The other seven
previously advertised kinds fail definition admission and create no state until
implemented on this same partition publication protocol. None retains the old
builder as a fallback.

The canonical persistence is the format-v6 partition-root namespace and codec.
This is a clean break. No old manifest reader, builder scheduler, migration,
converter, dual writer, compatibility shim, fallback builder, or mixed
publication architecture is retained.

## 2. Authority model

```text
Authoritative source state
  durable ordinary object versions/blobs
  durable bounded source journals
                    |
                    | hot projection or exact replay
                    v
Disposable producer state
  resident payload/prepared-row FIFO cache
  token/posting workers
  segment/head accumulators and encoding scratch
                    |
                    | durable publication
                    v
Durable derived index
  immutable segment/head-delta artifacts
  immutable partition roots
  exact-version partition current pointers
                    |
                    | fetch and materialize
                    v
Disposable query state
  pinned root vectors, local artifacts, readers and caches
```

Ordinary objects are application-data authority. The selected partition root is
authority for acknowledged index progress and its represented source range.
Memory is never authority. A query may claim a root only after verifying every
required durable artifact.

Published derived artifacts receive ordinary Keldra placement, replication or
erasure coding, integrity, reference, accounting, repair, and GC semantics.
Irreparable loss is a durable-data failure and is never silently interpreted as
a cache miss.

## 3. Source and atomic ordering

Keldra retains source-local ordering. It does not invent a cluster-global source
sequence. Each assigned partition advances its own exact journal epoch and
contiguous position. A query freshness snapshot is therefore a vector of
partition checkpoints.

Every prepared mutation carries:

- source partition and placement epoch;
- source-journal epoch and position;
- stable source identity and exact version;
- exact catalog generation; and
- atomic-batch cursor and boundary where applicable.

Atomic program delivery remains one durable complete batch event. Its routed
mutations enter the partition pipeline as one indivisible publication unit.
Checkpoint advancement proves all or none of the relevant atomic batch.

CPU preparation may complete out of order inside a bounded FIFO cache keyed by
exact source identity/version. One ordered journal consumer remains the sole
source-order and checkpoint authority; cache completion cannot advance it.

## 4. Hot admission and replay

After an authoritative object commit, its still-resident request bytes may enter
a bounded FIFO cache keyed by exact source identity/version. CPU workers may
prepare the physical projection out of order. Pre-commit speculative extraction
is allowed, but failed commits discard it and only committed exact-version keys
enter the cache.

Hot admission is opportunistic:

- it acquires indexing CPU and memory credits before work;
- it never blocks a Store commit lock or serving fence;
- it contains only required normalized fields/tokens, not the full payload;
- it may be evicted before the journal consumer reaches it; and
- lack of credit causes an exact storage load later, not allocation beyond the
  bound.

The ordered journal consumer starts at selected root checkpoint plus one. At
each position it takes an exact prepared row or resident raw payload from the
cache. On miss or eviction it loads the exact source version named by retained
journal evidence. It may wait only for a bounded already-running preparation,
then uses raw memory or storage rather than block behind speculative work. Every
route invokes the same normalization, recipe semantics, and segment encoders.

There is no second authoritative hot queue, reorder checkpoint, or durable
indexing WAL: a crash loses only work after the selected
root checkpoint, and that suffix is reconstructible from the retained source
journal and objects.

## 5. Memory pipeline and flush boundaries

One process-wide hard indexing budget covers:

- extraction input;
- resident payload/prepared-row FIFO state;
- normalization/token/posting output;
- active partition accumulators;
- frozen encoding/compression work;
- publication descriptors; and
- compaction workspaces.

The default capacity-planning ratio is 256 MiB per indexing core. Administrators
may set the core count and total ceiling explicitly. Stage credits prevent a
producer from exhausting memory required to seal and release existing work.
Every intermediate expansion is charged by resident bytes.

The active partition accumulator freezes after finishing the current complete
mutation unit when the first configured condition holds:

- target accounted bytes;
- maximum age of non-empty work;
- document/operation safety cap;
- memory pressure;
- explicit freshness request; or
- placement/catalog fence transition.

Targets are qualified settings rather than durable format constants. One
complete source mutation or atomic batch may overshoot a soft boundary, subject
to public hard request/object bounds and explicit overshoot accounting.

Freezing is an in-memory ownership transfer. The partition begins accumulating
newer work immediately if downstream credits are available. A full downstream
queue pauses partition consumption and lets replay lag grow; it does not create
another uncharged buffer.

## 6. Immutable output

A frozen accumulator is sorted and encoded independently of newer work. Its
logical outputs are:

- multi-field physical segments for new or changed indexed material; and
- compact head-delta blocks for current-version, liveness, and
  material-version advances.

Each flush packages those physical segments as one immutable
**`ProjectionQueryRun`** for one `(family, source_partition)` at the same
contiguous source and atomic cut carried by the eventual partition root. The
run contains, for every declared physical recipe, its seekable term/posting and
position structures, typed points, declared order/facet/aggregate doc values,
and a stable-key/material-version/live gate. It is a complete query input: a
reader can seek and intersect its components without opening a preparation
stream.

Material changes publish sparse L0 query runs containing old-material removal
or tombstone evidence and new-material additions. Projection-preserving updates
publish only a head delta. Field-state deltas and document-key comparison runs
are preparation-only; they decide what to encode but are never query readers,
never scanned to find candidates, and never a query fallback.

Physical components are packed into integrated payload objects at bounded
sizes. Small terms, postings, columns, and recipe tails share packs. Keldra does
not create one file, inode, ordinary object, or RocksDB key per source object,
term, component, or logical definition.

Every artifact binds its exact source range, catalog/codec identity, checksums,
record counts, and encoded/logical bytes. Portable codecs never persist Rust
layout, pointers, `usize`, or third-party memory images.

A projection-preserving update creates a head delta only. A material-changing
update adds only changed physical recipe state. Replaced immutable data remains
hidden by newest head state until partition-local compaction reclaims it.

## 7. Partition roots

One immutable partition root contains at least:

```text
PartitionProjectionRoot {
    format
    tenant_bucket_family
    source_partition
    placement_epoch
    catalog_generation
    revision
    previous_retained_root
    segment_directory_root
    head_delta_directory_root
    source_through
    atomic_through
    integrity_metadata
    byte_and_document_accounting
}
```

Segment and delta directories use bounded-fanout immutable pages, not one
unbounded manifest array. A root is a complete searchable partition view and a
retention checkpoint. Retained roots preserve pagination and in-flight query
leases within configured count, age, and byte bounds.

One small mutable current object exists per physical family/source-partition
incarnation. The partition identity contains the family and source-partition
incarnation only; it excludes catalog generation. `current` binds the exact
root identity, revision, placement epoch, and catalog generation. Logical
definition records refer to shared recipes and readiness; they are not rewritten
at each flush.

A small durable family partition directory names every active and retiring
partition incarnation and its handoff lineage. It is discovery authority across
membership changes. It changes only on partition creation, handoff, split/merge,
or retirement—not on ordinary flushes—so it cannot become a per-segment global
CAS bottleneck.

## 8. Publication protocol

The assigned partition owner publishes in this order:

1. finish a contiguous set of complete mutation units;
2. encode and verify immutable segment/head-delta artifacts;
3. durably store every artifact through integrated payload storage;
4. write bounded immutable directory pages;
5. write and durably acknowledge the immutable partition root;
6. exact-version CAS the partition current pointer under the placement fence;
7. release journal and exact source-version retention through the root's
   checkpoints; and
8. notify query nodes of the new partition revision.

Steps 3 through 6 amortize durability over a bounded batch. They may use several
device synchronizations and replica acknowledgements; no claim of one `fsync`
is made.

Only the selected current pointer makes a root searchable. Durable artifacts or
roots which are not selected are unattached orphans. Ordinary reachability GC
must discover and reclaim them after the configured safety age. A failed
publication can retry exact content when identities and fences still match, but
may never attach partially durable output.

Immutable directory pages, packs, segments, head deltas, and roots are
family-scoped content-addressed artifacts. They may be reused by a compatible
catalog transition or a replacement partition's inherited predecessor lineage.
Only `current` is partition-incarnation scoped; the family directory remains at
one stable path. GC keeps every predecessor artifact needed by a live
activation, handoff, pinned continuation, or eligible atomic cut.

Independent partitions may execute this protocol concurrently. They do not
serialize through a cluster/global current pointer. A query composes their
selected roots after publication.

## 9. Readers and freshness

A query node resolves the authorized logical definition and the durable family
partition directory, then obtains the newest finalized root per active or
not-yet-covered retiring partition incarnation. It selects one common finalized
`through_atomic_position`; if a partition is ahead, it walks exact retained
predecessor roots to the newest root at or before that cut. The resulting root
vector identifies a point-in-time set of immutable candidate structures; newer
publications do not invalidate it.

Handoff roots may overlap. Directory lineage makes that overlap explicit;
query materialization resolves one stable document by the greatest covered
authoritative source position and exact identity tie validation. It neither
duplicates a result nor lets predecessor state supersede replacement state.

An ordinary query may use the newest verified root vector already open. A
freshness request supplies required source/atomic evidence and waits until every
affected partition root covers it or its deadline expires. It does not wait for
unrelated partitions, empty queues, or compaction.

Continuation evidence binds the logical definition version, catalog generation,
root vector, query fingerprint, order, authorization context, and search-after
state. Root retention keeps its artifacts reachable for the bounded continuation
lifetime.

Query-local merged readers and caches are disposable. Losing them refetches
published artifacts and never reindexes source data.

For a pinned root vector, the native reader opens only its `ProjectionQueryRun`
recipe directories. It seeks/intersects postings and stable-key/material/live
gates, uses points for ranges, and reads declared doc values for ordering,
facets, and aggregates. It does not broadly scan field-state deltas. Every
selected candidate then passes bounded exact-current head/object validation
before the result is returned.

## 10. Retention and backpressure

The source journal is finite. A source position may be pruned only after every
constraining physical partition stream has a selected durable root covering it.
Logical definitions and query replicas do not own journal cursors.

Required source versions/blobs remain reachable for every uncheckpointed journal
event. Journal pruning and source-version GC use the same projection checkpoint
proof.

If indexing cannot keep pace:

```text
hot buffer fills
  -> hot preparations are dropped
  -> replay lag grows
  -> retained journal occupancy grows
  -> configured journal capacity is reached
  -> new source-producing mutations receive bounded backpressure
```

Keldra never avoids this by dropping evidence, advancing a cache cursor,
claiming an unselected root, allocating unbounded memory, or silently rebuilding.
Existing progress-debt rules may admit only the exact bounded publication which
will durably advance the constraining root; they cannot become general reserve
capacity.

## 11. Placement and recovery

Only the owner of a source partition at the current committed placement epoch
may publish. Reassignment fences the former owner but does not remove its
committed root from discovery. The family directory marks that incarnation
retiring while the new owner loads and verifies its selected root, inherits or
references its exact segment/head lineage, reconstructs the pipeline from the
next journal position, and publishes a replacement root.

Only after that replacement root durably proves equivalent-or-later source and
atomic coverage may one directory CAS retire the predecessor incarnation. Until
then queries include the predecessor root. This lifecycle CAS is not on the
normal flush path and cannot forget committed segments because membership
changed.

| Failure point | Recovery |
| --- | --- |
| Prepared row/FIFO/active memory | Resume the ordered journal consumer after the selected root checkpoint |
| Frozen encoding | Discard local output and replay |
| Artifact durable, root absent | Orphan; exact reuse or eventual GC |
| Root durable, pointer old | Previous selected root remains authoritative |
| Pointer advanced, local cache old | Recover from selected root |
| Former owner continues | Placement fence rejects publication; its selected root remains discoverable until replacement coverage |
| Query-node local state lost | Fetch selected durable artifacts |
| Published artifact unavailable | Ordinary repair; fail closed while missing |
| Published artifact irreparable | Report durable-data loss; authorized rebuild |

Recovery work is proportional to the uncheckpointed suffix. It never invokes an
old builder, scans historical projection-state streams, or falls back to a
legacy format.

## 12. Initial build and new recipes

A new physical family or recipe pins its catalog generation and one source
barrier for every incarnation in the family partition directory. Each assigned
partition owner scans its authoritative current-object subset through the same
bounded prepared-row pipeline and segment encoders as live projection, then
catches the journal suffix after the pinned barrier.

The journal alone is insufficient for baseline construction because objects
which predate its retention floor need not have journal evidence. No external
builder or alternate rebuild path exists; the partition owner performs the
bounded current-object scan.

At most one non-serving root exists per partition/catalog-generation backfill.
Completed baseline segments may be durably retained there for restart, but no
logical definition becomes ready until every required directory partition
reaches a complete compatible barrier. Partial baseline state is never queried as a
complete index.

A catalog transition reuses old family-scoped roots for every semantically
unchanged recipe. It scans/backfills only genuinely new recipes, then activates
the new catalog generation only when the reused and new roots prove exact
coverage for every directory partition.

Live journal catch-up has priority over backfill. Equivalent logical definitions
bind to existing ready recipes and cause no build.

## 13. Partition-local compaction

Compaction selects immutable inputs from one partition and writes a verified
replacement segment/root. It resolves newest head/material state, removes
obsolete documents and tombstones when retention permits, preserves exact query
semantics and source checkpoints, and assigns any new segment-local DocIds.

The unit of merge is a bounded whole `ProjectionQueryRun`: all recipe
components and its stable-key/material/live gate are rewritten together. A
replacement therefore remains directly seekable and cannot require query-time
reconstruction from field-state preparation output.

Compaction has separate bounded CPU, memory, I/O, input, and output admission.
It yields priority to journal catch-up and publication. If its root CAS loses,
the current root remains authoritative and its output is either exactly rebased
or reclaimed. A lost compaction never discards prepared source work or causes
reprojection.

Cross-partition merging may later improve reads, but is a disposable query
materialization. It cannot replace partition roots as checkpoint or retention
authority.

## 14. Correctness and liveness invariants

1. Every selected root references only durably acknowledged immutable artifacts.
2. A partition pointer advances only by exact CAS under the current placement
   epoch.
3. Source/atomic checkpoints advance only in the selected root proving the
   corresponding complete work.
4. Retention never advances beyond the oldest constraining selected checkpoint.
5. Atomic batches are represented all-or-none.
6. Memory loss cannot lose acknowledged index progress.
7. Replay cannot make an older source version supersede a newer one.
8. Publishing one partition never requires or mutates a global manifest.
9. Logical-definition cardinality does not multiply publication state.
10. Unselected artifacts and roots are never searchable and are eventually
    reclaimed.
11. A saturated pipeline remains inside configured memory and eventually
    backpressures through the bounded journal rather than dropping work.
12. Queries pin compatible root vectors and never mix unverified artifacts.
13. Compaction changes physical shape, never the represented source checkpoint
    or exact logical results.
14. Authorization, accounting, integrity, placement, and durability semantics
    remain unchanged by physical sharing.
15. A placement transition cannot retire a predecessor root until the
    replacement durably proves equivalent-or-later coverage and directory CAS
    records that proof.

## 15. Observability

Keldra exposes low-cardinality metrics for:

- memory capacity, used, peak, waiting, and dropped-hot bytes by stage;
- indexing cores and CPU time by extraction, normalization, analysis, sort,
  encode, publication, and compaction;
- hot opportunities/admissions/discards and replay records/bytes;
- prepared/source byte ratio and values/tokens/postings produced;
- active/frozen accumulator bytes, records, oldest age, and flush reason;
- artifacts, roots, logical/encoded bytes, fill ratio, and durability latency;
- query-run counts/bytes by level, sparse L0 removals/additions, and component
  bytes for postings, positions, points, doc values, and stable-key/live gates;
- source/atomic checkpoint lag by partition;
- root CAS attempts/losses and placement-fence rejection;
- journal occupancy, time/bytes to backpressure, and progress debt;
- compaction debt, input/output/write amplification and throttling;
- orphan artifact count, bytes, age, reuse, and reclamation; and
- query root-vector size, fetch/open/reopen latency, freshness waits,
  recipe seeks/intersections, forbidden preparation-state broad scans (zero),
  and exact-current candidate-validation batches.

Periodic INFO summaries are sufficient for qualification without per-object
logs. Detailed identities belong in bounded DEBUG traces. Payloads, field values,
and credentials are never logged.

## 16. Qualification

Required qualification includes:

- 1/2/4/8 indexing cores at 256 MiB per core on SSD;
- hot-resident, replay-only, and mixed hot/replay ingestion;
- buffer byte, age, operation, memory-pressure, and freshness flushes;
- inserts, deletes, projection-preserving and material-changing updates;
- maximum admitted object and atomic-batch overshoot;
- concurrent independent partition publication with no global CAS;
- sustained ingestion until lag is demonstrably stationary;
- crash/restart at every recovery-table boundary;
- placement reassignment during preparation, sealing, and publication;
- compaction under ingestion and root-CAS races;
- journal capacity and authoritative write backpressure;
- orphan creation followed by bounded GC; and
- query materialization from a multi-partition root vector, with per-recipe
  run seeks/intersections and no field-state broad scan.

Reports record exact source commit and artifact digest, host/topology, durability,
corpus, offered/accepted/indexed rates, stage throughput per core and 256 MiB,
hot/replay ratio, memory, CPU, journal lag, segment bytes, RocksDB/artifact I/O,
publication/compaction/query latency, query-run source/atomic cuts, L0
removal/addition density, per-recipe seek/intersection counters,
exact-current validation batches, correctness, and duration.

Passing requires zero skipped mutations, zero partial atomic observations, exact
pre/post-recovery results, bounded memory, bounded or stationary lag at the
claimed rate, eventual orphan reclamation, and no definition-count-proportional
physical publication. It also requires zero query-time field-state preparation
scans.

## 17. Clean-break removal list

The implementation deletes rather than wraps:

- builder tasks, representatives, leases, due queues, and failover records;
- per-definition/per-family active builders and source cursor loops;
- global or family manifest/current publication on normal flush;
- historical projected-state streams required only by the builder model;
- format-v4 bridge assemblers operating as a second indexing pipeline;
- legacy manifest and component readers/writers superseded by partition roots;
- converters, migrations, dual-write paths, compatibility modes and feature
  flags; and
- tests or documentation whose only purpose is retaining those paths.

The public index/query API and source data remain. Derived artifacts are rebuilt
fresh through the one partition pipeline.

## 18. Consequences

Keldra deliberately accepts a bounded amount of uncommitted index work in RAM.
A crash replays it. Segment durability still writes disk, but it is amortized
over large immutable outputs rather than duplicated per mutation. Prolonged
index saturation still backpressures source writes because retained journals are
finite.

In return, normal indexing consumes resident bytes once, avoids a second WAL,
shares physical work, publishes independently by partition, scales with
allocated CPU and memory, and removes builder scheduling and accumulated-history
cost from the steady-state path.
