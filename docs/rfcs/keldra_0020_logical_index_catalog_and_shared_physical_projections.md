# KELDRA-0020: Logical Index Catalog and Distributed Physical Projections

Status: Accepted; implementation in progress

Supersedes: KELDRA-0019 in full

Amends: KELDRA-0013, KELDRA-0014, and KELDRA-0016 wherever they make a
logical definition, external builder, or global manifest the unit of source
consumption, buffering, segment construction, publication, checkpointing,
scheduling, rebuild, or retention

Audience: Keldra implementors, operators, client authors, and reviewers

## 1. Decision

Keldra separates public logical index definitions from shared physical
projections and builds those projections through a memory-first pipeline on
every data node which owns an assigned source partition.

A logical definition is durable catalog metadata. It owns no task, source
cursor, mutable buffer, segment tree, publication timer, checkpoint, or query
cache. Canonically equivalent definitions refer to the same physical recipes.

For each assigned source partition, the data node runs one bounded projection
pipeline:

```text
committed object request bytes -> bounded FIFO/preparation cache ---+
                                                                   |
durable source journal -> one ordered checkpoint consumer ----------+
                         |                                         |
                         +-- cache miss -> exact storage load ------+
                                                                   v
                                                   prepared-row pipeline
                                                                   |
                                  normalize/tokenize/derive components once
                                                                   |
                                                partition accumulators
                                                                   |
                                      immutable segments/head deltas
                                                                   |
                                      partition root + checkpoint
```

Memory and CPU are the normal indexing path. The source journal and immutable
ordinary source versions are the replay authority when memory is full or a
process restarts. Keldra adds no mutation-by-mutation index WAL or durable queue.

There is no external index builder, elected definition builder, fallback
builder, or central segment assembler. Every node can project the source
partitions assigned to it. Publication changes only the assigned partition's
root; normal flushes do not contend on a global manifest CAS.

Query nodes materialize a logical definition from a pinned vector of relevant
partition roots. Initially they may use the existing native reader strategy,
but write-path ownership and coordination do not depend on a future global
query layout.

Typed JSON is the only admitted index kind for this milestone. Path, Metadata
Filter, Full Text, Vector, Hybrid, Git Source, and Tensor fail definition
admission before durable catalog or physical state is created. Each kind returns
only after it implements this same partition-owned pipeline; none retains or
falls back to the removed builder architecture.

The canonical durable format and namespace are v6 partition roots, currents,
segments, head deltas, and checkpoints. This is a clean break. There are no
legacy readers, migrations, format converters, dual writers, compatibility
shims, feature flags selecting the old builder path, or mixed old/new
projection generations. Deployments start with fresh derived-index state and
rebuild it from authoritative objects and retained journals.

## 2. Motivation and evidence

The previous architecture improved sharing but retained an independently
scheduled builder lifecycle. Sustained SSD qualification failed even at an
offered 100 mutations/s with one definition: exact visibility reached a
24.445-second tail, concurrent-query p99 exceeded four seconds, and post-write
query p99 exceeded six seconds. Correctness held, but service time grew as
immutable projection history accumulated.

One defect reopened projection history once per source record. Batching reduced
that multiplicative cost, but the architecture still rediscovered accepted
mutations after commit, accumulated historical comparison streams, published
many small boundaries, and made freshness depend on a scheduler revisiting a
builder. Runtime compaction would bound one symptom without removing those
costs.

The replacement follows four principles:

1. Logical definitions are catalog entries, not workers.
2. Source bytes already resident at ingestion are the cheapest projection
   input and should be reduced immediately to required typed fields.
3. The existing durable journal is sufficient recovery authority; duplicating
   it into an indexing WAL adds write amplification without adding correctness.
4. Immutable segments and partition-local LSM compaction amortize persistence;
   per-mutation physical publication does not.

The target is work proportional to source bytes and distinct changed physical
recipes, with throughput increasing as administrators allocate indexing CPU and
memory. Logical-definition cardinality must not multiply physical work.

## 3. Vocabulary

**Logical definition** is the authorized public index contract: source scope,
public field names, query capabilities, definition version, and lifecycle.

**Physical recipe** is the canonical, complete semantic description of one
membership or field representation, including selection, type, normalization,
analysis, cardinality, collation, date handling, capabilities, and codec
version.

**Projection family** is the shared document universe and physical recipes for
one tenant/bucket source scope.

**Source partition** is an existing object-placement and source-journal scope
with one ordered authoritative mutation stream and one assigned projection
owner at a placement epoch.

**Prepared row** is the compact, byte-accounted, non-authoritative in-memory
result of extracting only required physical values from an exact source
mutation. It excludes unindexed payload bytes.

**Preparation cache** is a bounded FIFO keyed by exact source identity/version.
It holds resident payload bytes or an asynchronously derived prepared row until
the ordered journal consumer reaches that mutation. It is not a queue of
authoritative work and owns no checkpoint.

**Replay path** reconstructs the same prepared row from the durable journal and
exact retained ordinary object version.

**Partition projection root** is the immutable point-in-time directory of
segments and head deltas for one source partition, selected by one small mutable
partition pointer and carrying its contiguous source checkpoint.

**Partition identity** names only the physical family and source-partition
incarnation. It deliberately excludes catalog generation. A root and its
partition-scoped current pointer bind the catalog generation, placement epoch,
revision, and checkpoints. This lets one stable family directory path describe
handoff lineage without creating a new partition identity for every catalog
transition.

**Head delta** advances current source/result identity for stable documents. A
projection-preserving update needs no new membership, posting, point, doc-value,
facet, order, text, or vector material.

**Projection checkpoint** is the greatest contiguous source position fully
represented by a durable published partition root.

**Family partition directory** is the small durable lineage map from one
physical family to current and retiring source-partition incarnations. It
changes only on partition creation, handoff, split/merge, or retirement, never
on an ordinary segment flush.

## 4. Logical catalog and physical identity

The ordinary definition object remains the public mutation and authorization
authority. Compact durable catalog records bind each exact logical version to
canonical physical recipes:

```text
LogicalDefinitionRecord {
    tenant_id
    bucket_id
    index_id
    definition_object_version
    definition_version
    source_scope_id
    membership_recipe_id
    field_bindings[]       // public field name/id -> physical recipe id
    query_contract_hash
    state                  // building, ready, replacing, deleting, failed
}
```

Recipe sharing is permitted only when all result-affecting semantics match.
Stored canonical specifications are authoritative; hashes are content addresses
and lookup accelerators, never unchecked equality proofs. Sharing never crosses
tenant/bucket authorization authority.

The compiled physical catalog contains a source router and the union of active
recipes. It is immutable and replaced atomically. A source mutation performs
bounded prefix/content-type routing and never scans logical definitions.

Immutable directory pages, packs, segments, head deltas, and roots are
family-scoped content-addressed artifacts. They are reusable when a catalog
transition retains a recipe and when a replacement partition inherits a
predecessor's covered state. Only `current` is partition-incarnation scoped;
the family directory has a stable path and names those partition identities.

Catalog cardinality must not create a corresponding number of tasks, journal
cursors, buffers, segment streams, roots, checkpoint records, timers, files, or
reader instances. An equivalent definition creates only a binding. A genuinely
new recipe starts one physical backfill and joins live projection under an exact
catalog-generation barrier.

## 5. Ingestion-side hot extraction

The object ingress path already owns validated request bytes. After a successful
commit provides exact source identity/version, those bytes may enter the bounded
FIFO preparation cache. CPU workers may transform cached bytes out of order
against a pinned compiled catalog. The job:

1. parses the payload at most once;
2. visits only the union of selected paths;
3. validates and normalizes typed values;
4. performs analyzer/token work which is safe before commit;
5. discards every unindexed payload byte; and
6. returns one compact prepared row charged to the indexing budget.

Pre-commit speculative extraction is permitted, but a failed mutation discards
its result. Only exact committed identity/version keys enter the cache. A cache
entry carries its catalog generation and atomic identity when applicable, but
it cannot advance or stand in for the journal consumer.

Extraction never runs on a serving-fence, Store commit, or async reactor thread.
It uses finite CPU jobs. If CPU or memory credits are unavailable, ingress skips
hot preparation and commits normally; the replay path later derives identical
work. Index pressure therefore does not add an unbounded wait to object commit.

The cache is an optimization over the one durable authority, not a second
source of truth. Payloads and prepared rows may be evicted at any time before
the journal consumer takes them.

## 6. Ordered partition pipeline

One journal consumer per assigned source partition is the sole ordering and
checkpoint authority. Preparation may finish out of order, but it never creates
a second work queue, replay cursor, or checkpoint.

The pipeline:

1. starts at the durable partition checkpoint plus one;
2. reads the next contiguous journal mutation;
3. takes an exact-version prepared row or resident raw payload from the cache;
4. on cache miss or eviction, exact-loads the retained source version;
5. coalesces repeated mutations only where exact lifecycle and atomic semantics
   prove it equivalent;
6. routes one prepared value to every distinct physical recipe needing it;
7. accumulates column/posting/head output in partition-local memory; and
8. seals and publishes only contiguous represented source ranges.

The consumer may wait only for a bounded already-running preparation, then use
resident raw bytes or exact storage rather than stall behind speculative work.
Publication and checkpoint advancement remain contiguous and exact under the
placement fence.

Atomic-program batches remain indivisible publication units. The pipeline may
cross a soft memory target to finish one admitted atomic batch, but the public
hard operation limits remain enforced at ingress.

The normal complexity of one mutation is:

```text
O(route lookup + selected payload bytes + produced terms + changed recipes)
```

It is never proportional to total logical definitions or accumulated
projection history.

## 7. Stage-specific memory and CPU control

Each node has one hard indexing memory budget divided into credit pools:

```text
extraction input
resident payload/prepared-row FIFO
token/posting worker output
partition segment accumulators
encoding/compression scratch
publication descriptors
compaction workspace
```

All resident and transient allocations are conservatively charged before use.
One stage cannot consume the credits another stage needs to release memory.
Credits may be dynamically rebalanced inside the hard ceiling, but mandatory
publication/recovery work cannot be starved by speculative extraction or
compaction.

The default planning ratio is 256 MiB per configured indexing core, subject to
an explicit administrator-set total ceiling. The ratio is a qualification and
capacity-planning baseline, not a promise that every workload achieves the same
documents/s. Large scalar or token expansion is charged by produced bytes, not
source size. Streaming analyzers must not create an unbounded token vector.

CPU execution is batch oriented. Finite extraction, normalization, tokenization,
sorting, and encoding jobs run in the bounded CPU pool; partition orchestration
remains async and never occupies a CPU worker while awaiting I/O or another job.
Additional cores should increase throughput until memory bandwidth, analyzer
cost, durable segment bandwidth, or compaction becomes the measured limit.

When the FIFO is full, old cache entries are evicted and the journal consumer
loads exact versions from storage. When sustained projection falls behind the
journal, lag grows toward
the configured journal capacity and ultimately applies authoritative write
backpressure. Keldra never allocates beyond the memory limit or discards durable
journal evidence.

## 8. Physical output and HOT-equivalent updates

Each partition accumulator produces reusable multi-field segment components:

```text
PartitionSegment
  document identity/material-version table
  exact and text term dictionaries/postings
  numeric/date point values
  typed order/facet/aggregate columns
  existence and membership structures
  text statistics/positions/norms
  vector blocks where declared
  represented source range and catalog generation
```

Equivalent logical definitions select components through bindings. They do not
cause duplicate extraction, tokenization, posting construction, or segment
publication.

A **projection-preserving update** changes an ordinary source/result version but
not canonical indexed material. It emits only a compact head delta:

```text
stable_document_key
current_source_version
current_result_identity
source_position
material_source_version
live state
```

It never enters field accumulators. A material change emits a head delta plus
only the changed recipe components. Deletes emit head/membership tombstones.
Old immutable material is not rewritten synchronously; newest authoritative
head state wins and partition-local compaction later removes obsolete material.

Canonical projected values are compared exactly. Digests may reject differences
quickly but digest equality alone cannot authorize a projection-preserving
decision.

## 9. Segment flush and partition publication

Accumulators seal on the first applicable boundary after finishing the current
indivisible mutation unit:

- target accumulated bytes;
- maximum non-empty age;
- document/operation safety bound;
- memory pressure;
- explicit freshness request; or
- placement/catalog transition requiring a fence.

Sealing writes relatively large immutable integrated-payload artifacts. It does
not write one file or RocksDB value per term, document, logical definition, or
component tail.

### 9.1 Query-ready projection runs

Every successful flush emits one immutable **`ProjectionQueryRun`** for exactly
one `(projection_family, source_partition)` and exactly one contiguous
`through_source_position` / `through_atomic_position` cut. It is query-ready
at publication, not an intermediate field-state log:

```text
ProjectionQueryRun {
    family, source_partition, level, source_through, atomic_through
    stable_key_material_live_gate
    recipe_directories {
        exact/text terms, advanceable postings, and declared positions
        typed numeric/date points
        declared order/facet/aggregate doc values
        membership/existence and optional physical-order structures
    }
}
```

The stable-key/material/live gate binds every segment-local document to the
material version represented by that run and makes an old material version
ineligible before it can become a hit. A material-changing mutation emits a
sparse L0 run with removal/tombstone evidence for the old material plus additions
for the new material; unchanged recipes need no duplicate component. A
projection-preserving mutation emits only the head delta described above.

Field-state deltas, extracted values, and document-key comparison runs exist
only to prepare this output and to decide whether a mutation is
projection-preserving. They are never opened by a query reader, never scanned
to discover matching documents, and never a fallback query representation. A
root cannot claim its checkpoint until every run and head delta for that exact
source/atomic cut is durable and referenced by the root.

One partition root contains:

```text
PartitionProjectionRoot {
    tenant_bucket_family
    source_partition
    placement_epoch
    catalog_generation
    revision
    through_source_position
    through_atomic_position
    segment_directory_root
    head_delta_directory_root
    previous_retained_root
    byte_and_document_accounting
}
```

Publication orders durability as follows:

1. encode and verify the immutable segment/head-delta artifacts;
2. store them through integrated payload storage with required durability;
3. write the immutable partition root referencing those exact artifacts;
4. exact-version CAS that partition's small current pointer; and
5. only then release journal/source-version retention through the checkpoint.

Each source partition incarnation has its own pointer and placement fence. A
small family partition directory names active and retiring incarnations and
their handoff lineage. It changes only when placement topology changes, never
for segment publication. There is no global manifest CAS for normal flushes and
no logical-definition pointer update. Independent partitions may publish
concurrently.

Unattached durable artifacts are safe orphans, but not leaks. Ordinary artifact
reachability GC discovers and reclaims them after the publication safety age.
GC retains every predecessor root/page/pack needed by a live activation,
handoff lineage, pinned query/continuation, or an eligible atomic cut; it may
not reclaim merely because a newer `current` exists.

## 10. Query materialization

A query:

1. authenticates and authorizes the requested logical definition;
2. loads its exact binding and catalog generation;
3. resolves the durable family partition directory and obtains the newest
   finalized root for every active or not-yet-covered retiring partition
   incarnation;
4. chooses one common finalized `through_atomic_position` across that vector;
   when a partition is ahead of the common cut it walks exact retained
   predecessor roots until it finds the newest root at or before that cut;
5. pins that cut-consistent root vector before opening any reader;
6. maps public field names to physical recipes;
7. opens or reuses verified immutable segment readers;
8. executes predicates, ordering, facets, aggregates, text, and vectors;
9. applies newest head/liveness state and exact-current validation; and
10. returns authorized exact result identities.

For each pinned root, the reader seeks only the matching recipe directories in
its `ProjectionQueryRun`s. It intersects advanceable postings and membership /
stable-key live gates, uses points for ranges, and reads declared doc values only
for order, facets, and aggregates. It may merge a bounded number of run-local
iterators, but it never broad-scans document-key field-state output. Candidate
stable keys then undergo the mandatory bounded exact-current object/head
validation before a result is returned.

The root vector, logical definition version, query shape, order, and
search-after state are bound into continuation evidence. A later partition root
does not mutate an already pinned view.

During handoff, predecessor and replacement roots may cover an overlapping
source range. The directory records that lineage and the query resolves a stable
document exactly once: the highest covered authoritative source position wins,
with exact identity comparison on ties. Overlap can increase bounded read work
but cannot duplicate or resurrect a result.

Query nodes may cache root vectors, open readers, decoded blocks, and later
materialized cross-partition merges. These remain disposable. A future
distributed query engine may fan out or place merged replicas, but neither is a
condition of the write architecture.

Authorization is never inherited merely because physical bytes are shared.
Both definition admission and returned objects retain their existing checks.

## 11. Backfill and catalog changes

An equivalent definition binds immediately to ready recipes and performs no
source work. A new physical recipe:

1. installs a new immutable compiled catalog generation;
2. pins one source barrier for every incarnation in the family partition
   directory;
3. has each partition owner scan its authoritative current-object subset through
   the same bounded memory pipeline used by live projection;
4. catches journal mutations after the pinned barrier through ordered replay;
5. converges baseline and live checkpoints; and
6. marks logical bindings ready only after every directory partition proves a
   complete compatible barrier.

A catalog transition reuses every old family-scoped recipe root that remains
semantically identical. It backfills only genuinely new recipes, then performs
one exact-coverage activation binding the new logical catalog generation to the
reused and newly completed roots. It does not rewrite or clone retained recipe
artifacts merely because a logical definition changed.

Removing the last logical reference makes a recipe eligible for physical
retirement after retained queries and roots release it. Definition deletion
never directly deletes shared artifacts.

Backfill uses the same bounded memory-first preparation and segment encoders as
live projection. It is lower priority than journal catch-up and cannot create a
separate builder architecture. A retained journal suffix cannot reconstruct
objects which predate its retention floor; the authoritative partition-scoped
current-object scan is mandatory for a new recipe or full rebuild.

## 12. Distribution, recovery, and retention

Placement assigns source partitions, not logical definitions. Only the owner at
the current placement epoch may advance a partition pointer. Active membership
alone is not root discovery authority: removing a node must not make its
published segments disappear from queries.

For immutable `SourceId { source_node, source_epoch }`, its ACTIVE source node
is the producer, preserving normal payload locality. If that node has left the
current placement, the producer is rank zero from the existing capacity-weighted
`FutureIndex` HRW ranking over the domain-separated canonical key
`keldra/v6/source-producer/v1 || tenant_id || bucket_id || source_node ||
source_epoch`. Logical definitions and physical families are deliberately not
inputs, so every family follows one source handoff. The partition identity
records both immutable source identity and this fenced producer identity.

On handoff, the family partition directory retains the predecessor incarnation
as retiring. The replacement loads its selected root, inherits or references
the exact predecessor segment/head lineage, and replays from its checkpoint.
Only after the replacement root durably proves equivalent-or-later coverage may
one directory CAS retire the predecessor incarnation. Until then, query root
vectors include the predecessor root. The old producer is fenced immediately,
but its committed data remains discoverable. Directory CAS occurs only for this
placement lifecycle transition, never per segment flush.

Crash recovery is exact:

- lost prepared rows, reorder state, and accumulators replay from the journal;
- partially encoded local output is disposable;
- durable unattached artifacts are reclaimed by reachability GC;
- a durable root not selected by the partition pointer is unattached;
- a selected root is authoritative even if local checkpoint caches lag; and
- query-node local loss refetches durable segments without source reindexing.

Journal and source-version retention use the minimum durable projection
checkpoint across assigned physical partition streams. Logical definitions do
not own retention cursors. Required exact source versions remain reachable until
the relevant partition root proves them represented. Artifact GC additionally
retains predecessor chains required to reconstruct any live activation or
eligible common atomic cut, not just the newest root of each partition.

Irreparable loss of a published segment despite Keldra durability is a durable
data failure. Repair runs first; an authorized explicit rebuild is the fallback,
not silent query or builder compatibility behavior.

## 13. Partition-local compaction

Compaction is ordinary LSM maintenance over immutable segment and head-delta
levels. It:

- selects bounded inputs from one partition;
- resolves newest state by stable document key and source order;
- folds obsolete versions and tombstones when retention permits;
- emits verified replacement artifacts;
- atomically replaces only that partition's root; and
- leaves its represented source checkpoint unchanged.

It compacts bounded whole `ProjectionQueryRun` inputs for one family/partition,
not independent per-field state streams. The replacement run merges each
declared recipe's terms/postings/positions, points, doc values and
stable-key/material/live gate together, preserving the source/atomic cut and
exact query meaning. No compaction output may require a reader to reconstruct a
queryable posting or value by scanning preparation state.

Compaction is independently CPU-, memory-, and I/O-throttled. Journal catch-up
and required flush publication take priority. A failed compaction CAS does not
invalidate ingestion work or trigger source replay. Cross-partition compaction
is deferred as an optional read optimization and is never write authority.

## 14. Correctness invariants

1. Ordinary objects remain application-data authority.
2. Source journals plus retained exact object versions remain replay authority.
3. Memory is never durability, checkpoint, publication, retention, or recovery
   authority.
4. A checkpoint advances only with a durable selected partition root proving
   the complete contiguous source range.
5. Every relevant atomic batch is represented all-or-none in a published view.
6. Placement epochs fence former partition owners without hiding their
   published roots before a replacement durably inherits their coverage.
7. Equivalent logical definitions share physical work without sharing
   authorization authority.
8. Projection-preserving updates return the exact current result identity
   through unchanged indexed material.
9. Material changes cannot use stale postings to return an obsolete value.
10. No query mixes incompatible catalog generations or unpinned partition
    roots.
11. Every admitted allocation is bounded and charged before use.
12. Unpublished artifacts are not searchable and are eventually reclaimed.
13. Replay is idempotent and an older source position cannot supersede a newer
    one.
14. Loss of local memory or query caches cannot lose acknowledged index state.
15. Authorization and exact-current result validation remain fail closed.

## 15. Telemetry

Required low-cardinality telemetry includes:

- logical definitions and distinct membership/field recipes;
- assigned partitions and placement epochs;
- bytes used, queued, and peak for every memory-credit stage;
- indexing cores, CPU busy time, queue wait, and batch sizes by stage;
- hot-path opportunities, admissions, drops, and admission percentage;
- replay records, source payload reads, bytes, and reasons;
- source bytes parsed versus selected prepared bytes;
- documents, scalar values, tokens, terms, and postings per core-second;
- projection-preserving, changed-field, membership, insert, and delete counts;
- accumulator bytes/age, flush reason, fill ratio, and overshoot;
- segment/head-delta counts, bytes, encode time, durability time, and
  publication time;
- query-run counts/bytes by LSM level; sparse-L0 removals/additions; recipe
  component counts/bytes for postings, positions, points, doc values, and the
  stable-key/material/live gate;
- partition checkpoint lag in records, bytes, and wall time;
- root-CAS attempts, losses, and placement-fence failures;
- compaction debt, input/output bytes, CPU, duration, and write amplification;
- orphan artifact count/bytes/age/reclamation; and
- query root-vector size, segment readers, cache reuse, logical/physical bytes,
  latency, iterator seeks/intersections, broad-scan fallback attempts (which
  must remain zero), and exact-current candidate validation batches.

Periodic summaries provide qualification evidence. Stable tenant, definition,
partition, root, and segment identities are trace fields, not metric labels.
Payloads, credentials, and field values are never logged.

## 16. Qualification gates

Qualification uses fresh volumes and the same candidate artifact on the
8-core/32-GiB SSD and 16-core/64-GiB rotational hosts. All remote experiment
files and Keldra data remain beneath `~/keldra_experiments`.

### 16.1 Throughput scaling

Measure indexing with 1, 2, 4, and 8 cores and 256 MiB per core. Report:

- accepted object mutations/s;
- indexed documents, source bytes, values, tokens, and postings/s;
- each value normalized per core and per 256 MiB;
- CPU utilization and memory by stage;
- hot admission and replay percentages;
- segment durability bandwidth and publication frequency; and
- query-run publication rate, L0 removal/addition density, and query-side
  seek/intersection evidence showing no preparation-state scan; and
- lag slope during a sustained run.

The qualification corpus has two independent object-size shapes: a small
object floor of approximately 1 KiB and a pathological 96 KiB source-object
cell reflecting the production large-object failure shape. The latter is D1/P1
at the maximum resource cell, not a multiplier on logical catalog or physical
recipe cardinality. Reports distinguish accepted source bytes/s from
prepared/projected bytes/s; the latter is read only from a v6 runtime counter,
never estimated from payload length.

The first SSD floor for small-object projection is 10,000 accepted and indexed
mutations/s with stationary lag. This is a qualification target, not a claim
until measured.

### 16.2 Logical and physical scale

Create D1, D64, D1K, D10K, and D250K equivalent logical definitions. Physical
work and source-to-visible lag must remain near D1. Separately test P1, P4, P16,
and P64 genuinely distinct physical recipes so unavoidable recipe cost is not
misreported as catalog cost.

### 16.3 Workload shapes

Run inserts, deletes, projection-preserving updates, material-changing updates,
and a realistic mixture. Projection-preserving updates must produce head deltas
and zero field-component work. Large fields and token expansion must remain
inside charged bounds.

### 16.4 Sustained and recovery runs

Run the maximum stationary SSD cell for at least 30 minutes. Lag must converge
to a bounded distribution while ingestion continues; a final drain is not
keep-up evidence. Kill and restart producers at empty, partially filled,
sealing, durable-unattached, root-publication, and compaction points. Verify
exact results, replay bounds, and orphan reclamation.

### 16.5 Query isolation evidence

Record write-only qualification separately from concurrent-query cells. Write
architecture is accepted on projection throughput and correctness, while query
latency and root-vector materialization are measured independently so a read
optimization cannot conceal or constrain a better write path.

For each declared recipe capability, query a pinned multi-partition root vector
through a selective term/text predicate, numeric range, order, facet, and
aggregate as applicable. Evidence must bind every claimed root cut to its
`ProjectionQueryRun`s, show recipe seeks/intersections and bounded
exact-current candidate validation, and show zero field-state broad-scan or
field-state fallback attempts. Exercise sparse L0 old-material removals plus
new-material additions before and after bounded whole-run compaction.

Every report binds commit and artifact digest, host/topology, corpus hash,
durability, batch/concurrency, logical and physical recipe counts, offered and
accepted rates, latency distributions, checkpoints, CPU profile, memory,
RocksDB/artifact bytes, compaction, correctness, and duration.

## 17. Clean-break removal requirements

Implementation removes, rather than wraps:

- external and per-definition builders;
- elected representative builders and builder failover state;
- builder scheduler queues, due records, leases, and per-definition turns;
- per-definition or per-family journal rescans and checkpoints;
- historical projected-state streams used to rediscover predecessor state;
- global/per-family manifest CAS on the normal partition flush path;
- the format-v4 assembler bridge as a separately scheduled indexing path;
- dual writers, old-format readers, converters, migrations, feature flags, and
  fallback query/index paths;
- compatibility tests whose only purpose is preserving removed architecture;
  and
- stale documentation or configuration referring to those facilities.

Existing public index and query semantics are retained unless this RFC
explicitly changes their internal physical ownership. Fresh deployments rebuild
derived state; no legacy derived artifact is imported.

## 18. Non-goals and consequences

This RFC does not make genuinely different analysis free, make memory durable,
introduce a second journal, share data across authorization authorities, adopt
Lucene or RocksDB as the public index format, require query fanout, or promise
that increasing CPU always helps after another measured resource saturates.

It also does not preserve the seven v4-only index kinds in the current supported
surface. Their component semantics remain design input, but availability waits
for a partition-pipeline implementation and its full correctness qualification.

It deliberately accepts that:

- a crash discards recent in-memory projection work and replays it;
- a full hot buffer degrades into journal replay rather than blocking ingress
  solely for indexing;
- sustained inability to project eventually exhausts bounded journal capacity
  and backpressures authoritative writes;
- immutable segments still require durable writes, but those writes are large
  and amortized rather than per object; and
- partition-root vectors add query-planning work which read-side optimization
  must address separately.

In return, indexing capacity becomes a direct function of allocated CPU,
memory, physical recipe complexity, and segment bandwidth—not logical builder
count or accumulated projection history.
