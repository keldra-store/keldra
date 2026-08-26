# KELDRA-0016: Journal-Backed Durable Segment Publication

Status: Accepted architecture

Amends: KELDRA-0014 sections 5, 7.3, 9.5, 9.6, 14, 17, 19, 20, and 21

Audience: Keldra implementors, operators, client authors, and reviewers

## 1. Decision

Keldra will use its durable source journals and immutable source objects as the
replay authority for incremental derived-index construction. Index builders
accumulate complete source mutations in bounded RAM buffers and flush those
buffers into immutable, durable Keldra segment objects. One small commit
manifest atomically binds the searchable segment set to the exact source
journal positions it represents.

There is no heavyweight generation build in the steady-state incremental path.
The durable model contains only:

- immutable segment objects;
- immutable commit manifests describing point-in-time searchable views; and
- one exact-version current pointer selecting the newest committed manifest.

A commit manifest has a revision so readers can identify and pin an immutable
view. That revision replaces the overloaded use of **generation** as a unit of
building, scheduling, invalidation, retention, and publication. In the parts of
KELDRA-0014 amended by this RFC, **generation** means only a committed manifest
revision. It is not a mutable candidate or a reason to rebuild already prepared
work.

The durable derived index is not disposable operationally. Its segment objects
are acknowledged Keldra data with ordinary placement, integrity, reference,
and durability guarantees. Reconstructing all segments from authoritative
source objects remains possible as disaster recovery, but may be extremely
expensive and is not the normal response to node or process loss.

Only node-local materialization is disposable. This includes active and frozen
builder buffers, downloaded segment files, opened readers, decoded blocks, and
query caches. A builder or query node recovers ordinary local loss by fetching
the durable committed segments, not by rebuilding the index from source.

The default incremental flush boundaries are:

```text
active RAM target:           16 MiB
maximum buffered age:         1 second
operation-count safety cap:  configured and explicit
boundary overshoot:           one complete mutation unit
physical artifact target:    16 MiB
```

Crossing any enabled boundary freezes the active buffer. A mutation unit is an
ordinary source mutation or one complete atomic-program batch; it is never
split between committed views.

## 2. Motivation

The previous runtime treated derived-index progress as repeated construction of
replaceable candidate generations. Under sustained background materialization,
new source changes repeatedly displaced work that was already being built.
Publication and retention then raced over a moving current pointer while reads
waited for freshness.

The observed cycle was:

```text
source mutation
    -> construct or revise candidate generation
    -> publish a newer generation
    -> invalidate work held by derived consumers
    -> retention loses its current-pointer compare-and-swap
    -> discard, rebuild, and retry
    -> freshness-sensitive reads wait behind continuing background work
```

That model fails to exploit Keldra's existing durability. Source mutations are
already represented by bounded durable journals, and their payloads remain in
authoritative ordinary object versions and blobs. The index does not need a
second mutation-by-mutation write-ahead log or persistent representation of an
in-progress RAM buffer.

The corrected path is monotonic:

```text
source mutation
    -> append to bounded RAM buffer
    -> flush one immutable durable segment
    -> publish one commit manifest
    -> optionally reopen readers
```

Already encoded segments survive later source mutations unchanged. New work is
appended as new segments and small live-document metadata. Background merges
replace committed segments without changing the source checkpoint.

## 3. Scope and relationship to KELDRA-0014

KELDRA-0014 remains authoritative for:

- format-v4 component codecs and envelopes;
- field capabilities and schema fingerprints;
- segment-local document identifiers;
- term dictionaries, postings, points, doc values, positions, norms, vectors,
  identities, and live-document representation;
- query planning, execution, pagination, authorization, and result refill;
- ordinary object placement, erasure coding, accounting, and corruption
  handling; and
- the supported index kinds.

This RFC replaces the parts of KELDRA-0014 that model incremental catch-up,
publication, freshness, retention, and recovery as construction or replacement
of heavyweight generations.

This RFC does not remove immutable point-in-time reader views. It makes their
authority smaller and more precise: a reader view is the segment set and
live-document state selected by one committed manifest.

The initial baseline scan and an explicit full rebuild retain KELDRA-0014's
source-snapshot correctness requirements. They produce the same immutable
segments and commit manifests defined here. A full rebuild may retain one
bounded, non-serving build root so already durable rebuild segments survive a
builder restart; it must not create multiple competing candidates or affect the
serving view until its complete manifest is published.

## 4. Vocabulary

**Source journal** is one source-local, durable, ordered and bounded stream of
mutation evidence. It identifies changed stable source state but does not copy
complete ordinary object payloads.

**Source checkpoint** is the vector of source-local journal positions whose
mutations are completely represented by a committed manifest.

**Atomic checkpoint** is the greatest contiguous atomic-program cursor whose
complete relevant mutation batches are represented by a committed manifest.

**Replay authority** is the combination of retained source-journal evidence and
the immutable ordinary source versions or blobs needed to project that evidence
into an index.

**Mutation unit** is either one ordinary source mutation or one complete routed
atomic-program batch. Flush and publication boundaries never split a mutation
unit.

**Active buffer** is the mutable, heap-resident index-builder state receiving
new mutation units.

**Frozen buffer** is a detached, immutable builder buffer being sorted, encoded,
or flushed as a segment. It is local materialization and can be reconstructed by
replay until its segment is committed.

**Segment** is one logical immutable collection of format-v4 component files
built or merged together. A segment may occupy several physical Keldra artifact
packs.

**Commit manifest** is one immutable object naming a complete searchable
segment set, its selected live-document state, source checkpoint, atomic
checkpoint, schema identity, and integrity metadata.

**Current pointer** is the one small mutable object whose exact-version
compare-and-swap selects the newest committed manifest.

**Committed view** is the immutable point-in-time index view selected by one
commit manifest. It is the precise replacement for the overloaded term
"generation" in the amended architecture.

**Local materialization** is a disposable node-local representation of builder
or query state. It is never publication authority.

## 5. Ownership and durability model

The complete hierarchy is:

```text
Authoritative source state
  |-- durable ordinary object versions and blobs
  `-- bounded durable source journals
                    |
                    | replay and projection
                    v
Durable derived index
  |-- immutable segment objects
  |-- immutable commit manifests
  `-- exact-version current pointer
                    |
                    | fetch and materialize
                    v
Disposable node-local state
  |-- active and frozen builder buffers
  |-- encoded temporary files
  |-- downloaded segment files
  |-- opened readers
  `-- decoded-block and query caches
```

The source objects are authoritative for application data. The committed index
is authoritative for what an acknowledged index view contains and which source
positions it has consumed. A query node must not claim to serve a committed view
unless it can verify and open every durable segment component named by that
view.

Loss of disposable local state cannot lose acknowledged index progress. Loss of
durable segment objects despite Keldra's storage guarantees is a durable-data
failure and invokes explicit rebuild or repair. It is not treated as an ordinary
cache miss.

## 6. Replay model without global source ordering

Keldra deliberately retains source-local journals rather than inventing one
cluster-global journal order. A journal record identifies the stable source,
path, version, and mutation metadata required to discover changed state. The
builder fetches the referenced or barrier-correct authoritative object version
and projects it into native index structures.

```text
source A journal:  ... 711, 712, 713
source B journal:  ... 204, 205
source C journal:  ... 990, 991, 992, 993

committed source checkpoint:
    { A: 713, B: 205, C: 993 }
```

There is no semantic ordering between independent ordinary mutations from
different sources. Their checkpoint is a vector, and a segment may contain a
deterministic merge of mutation units read from several sources.

Correct replay nevertheless requires payload retention. For every uncommitted
journal event, at least one of these must remain true:

1. the event references an immutable source version or blob which remains
   reachable; or
2. the source retention horizon preserves the exact version needed by the
   consumer.

Source versions needed by an uncheckpointed mutation must not be reclaimed.
Journal retention, ordinary-object version retention, and committed index
checkpoints therefore form one correctness contract even though they use
different storage structures.

The builder may coalesce repeated ordinary changes to one stable source identity
when doing so is proven equivalent to projecting each intermediate change. It
must not coalesce across an atomic-program boundary or discard lifecycle effects
which the index definition observes.

## 7. Atomic-program delivery

Independent per-path finalization events cannot prove that a derived index has
consumed all paths of an atomic program. Physical path finalization may complete
out of source order and across multiple source journals.

After an atomic program becomes globally visible, its executor therefore emits
one durable, idempotent batch-publication event:

```text
AtomicBatchPublished {
    cursor,
    bundle_hash,
    affected_routes,
    mutations,
}
```

`mutations` contains the complete, sorted routed mutation descriptors and is
the durable derived-delivery authority. `bundle_hash` binds those descriptors
to the atomic program's prepared bundle for integrity lineage without retaining
that temporary bundle as a second durable ownership system. Authoritative
ordinary source versions and blobs remain the payload authority. Each derived
definition receives its complete routed subset as one mutation unit. Ordinary
per-path invalidations belonging to the atomic program are not independently
projected into the derived index.

The builder may begin a complete atomic batch while below a flush boundary. It
finishes that batch even if doing so exceeds the RAM, byte, or operation target,
then freezes the buffer immediately afterward.

```text
ordinary mutation
ordinary mutation
atomic batch containing all 240 relevant path changes
----------------------------------------------------- flush boundary
next ordinary mutation
```

The atomic checkpoint advances only when every relevant mutation in the batch
is represented by the segment set named in the committed manifest. This proves
that a committed view contains all or none of each relevant atomic program.

## 8. Active-buffer transition

The journal-consumer task which appends the mutation crossing the first enabled
boundary owns the buffer transition. Under the active-buffer lock it replaces
the active buffer with a new empty buffer and enqueues the detached buffer for
encoding.

Conceptually:

```text
lock active
append complete mutation unit

if first boundary reached:
    frozen = replace(active, empty_buffer)
    enqueue(frozen)

unlock active
continue consuming into the new active buffer
```

The replacement is an in-memory ownership transfer. It requires no journal
barrier, segment publication, reader coordination, or global quiet point.

```text
Journal consumer
      |
      | append mutation 998
      | append mutation 999
      | append mutation 1000  <--- crosses a boundary
      |          |
      |          `--- replace active buffer
      |
      | append mutation 1001 into new active buffer
      ` append mutation 1002

                         detached frozen buffer
                                  |
                                  v
                            sort and encode
```

Normal buffer replacement does not pause journal consumption. Consumption pauses
only when the explicitly bounded downstream pipeline has no capacity for another
frozen buffer. That is resource backpressure, not part of the flush algorithm.

## 9. Flush boundaries and resource bounds

The active buffer freezes after completing the first mutation unit for which
any enabled condition is true:

```text
estimated active-buffer RAM >= 16 MiB
oldest buffered mutation age >= 1 second
buffered mutation count      >= configured operation cap
explicit refresh requested
```

The 16 MiB boundary measures accounted builder memory, not encoded segment size.
Projection dictionaries, sort keys, postings builders, live-document changes,
and retained identities all contribute to the estimate. The output may be
smaller or larger after encoding.

The 1-second age boundary applies only to a non-empty active buffer. An idle
definition does not publish empty segments merely because time passes. The
timer starts when the first mutation enters an empty active buffer.

The operation cap is a safety boundary for mutation shapes whose temporary
memory is not well predicted by encoded source bytes. Its default must be
selected by qualification and exposed in configuration and telemetry.

One complete mutation unit may overshoot any soft target. Admission must still
enforce a hard maximum atomic-program and source-object shape. A mutation which
cannot fit within the documented hard bound fails closed at its authoritative
ingress rather than causing unbounded index-builder memory.

Every process has one global index-builder memory budget. Per-definition buffer
targets do not multiply without limit as definitions become active. Scheduler
admission, active buffers, frozen buffers, encoders, and pending publication
bytes all charge the global budget.

The minimum complete pipeline is:

```text
one active buffer per admitted definition
bounded frozen-buffer queue
bounded segment encoders
bounded durable segment publications
bounded background merges
```

When a downstream queue is full, the affected definition stops pulling new
journal pages. It does not allocate another unaccounted buffer and does not
discard frozen work in favour of a newer target.

## 10. Segment encoding and physical packing

A frozen buffer is sorted and encoded entirely independently of later source
mutations. It produces one logical immutable segment with:

- native postings, term dictionaries, points, doc values, positions, norms,
  vectors, identities, locators, and statistics required by the definition;
- new documents for creates and updates;
- live-document or tombstone effects for replaced and deleted identities;
- the source and atomic cursor ranges represented by the segment;
- schema, codec, and format identities; and
- checksums for every logical component.

The physical artifact target remains 16 MiB. A logical segment may contain
several independently fetchable physical packs, especially when an indivisible
mutation unit overshoots the RAM target or the index kind has several component
families.

```text
logical segment S42
  |-- postings-0        16 MiB artifact pack
  |-- postings-1         7 MiB artifact pack
  |-- doc-values         4 MiB artifact pack
  |-- live-docs        320 KiB component
  `-- segment metadata
```

All named components receive ordinary Keldra durability acknowledgement before
any committed manifest may reference the segment. Physical packing may combine
small component tails as already defined by KELDRA-0014, but cannot make one
logical range read require an unbounded artifact fetch.

## 11. Commit manifest and publication

The commit manifest is the durable checkpoint. It contains at least:

```text
CommitManifest {
    format,
    definition_id,
    definition_version,
    schema_fingerprint,
    revision,
    previous_manifest,
    segments,
    live_document_views,
    source_through,
    atomic_through,
    integrity_metadata,
}
```

Publication follows one ordered durability protocol:

```text
1. Accumulate complete mutation units in the active RAM buffer.
2. Replace the active buffer when the first flush boundary is reached.
3. Encode the frozen buffer as one immutable logical segment.
4. Flush every segment component into durable ordinary Keldra objects.
5. Construct a manifest naming the prior committed segment set plus the new
   segment and its complete source and atomic checkpoints.
6. Durably write the immutable manifest.
7. Exact-version CAS the current pointer to the new manifest.
8. Permit journal and source-version retention through the newly committed
   checkpoints.
9. Notify query nodes that a newer committed view is available.
```

Steps 4 through 7 constitute one logical index flush and publication. The
implementation may require multiple storage-device synchronizations, replica
acknowledgements, and metadata commits. The architectural property is that
these durability operations are amortized over one complete segment rather than
performed for every source mutation or builder intermediate.

Ready definitions may share bounded physical publication cohorts without
sharing logical authority. Pack objects are published first with one exact
outcome per item; only definitions whose packs are durable enter an immutable
manifest cohort; only definitions whose manifests are durable enter a guarded
current-pointer cohort. Each pointer keeps its own definition fence,
expected-version CAS, outcome, checkpoint, and retry state. A failed item never
rolls back or prevents an unrelated successful item, and no manifest names an
unacknowledged pack. Incremental and maintenance cohorts have independent
bounded admission, while all stages retain count, byte, age, and in-flight
bounds.

The manifest and current-pointer CAS are the only publication authority. A
durable segment which is not named by a committed manifest is not searchable
authority.

The authoritative retention checkpoint is read from the committed manifest.
Any separately cached consumer offset may lag and may be recreated; it must not
permit retention beyond the manifest's checkpoint.

## 12. Updates and deletes

Segment cores remain immutable. An update creates a new indexed document and
records that the prior document identity is no longer live in the committed
view. A delete records the corresponding liveness removal without inserting a
new searchable document.

Commit manifests select immutable live-document metadata alongside segment
cores. No query may combine a segment core from one manifest with live-document
state from another.

```text
manifest M17
  |-- segment S9  + live view L9.3
  |-- segment S12 + live view L12.1
  `-- segment S16 + live view L16.0
```

Updates and deletes may be represented by newly written live-document deltas or
replacement immutable live-document views according to KELDRA-0014's bounded
format. Delta-chain length and query application work must remain explicitly
bounded; background merging collapses accumulated liveness changes.

Stable source identity and version metadata make crash replay idempotent. A
replayed mutation cannot make an older source version supersede a newer one.

## 13. Readers and freshness

A query opens and pins one committed manifest. That manifest remains immutable
and valid while newer manifests are published.

```text
reader R1 ---> manifest M41 ---> [S10, S17, S22]
reader R2 ---> manifest M42 ---> [S10, S17, S22, S23]
```

Publication of `M42` never invalidates `R1` and never causes `R1` to rebuild its
work. The query node may reopen a reader over `M42` in the background and swap
the local serving pointer after all newly required segment materializations are
verified.

An ordinary query serves the newest already-open committed view immediately.
It does not wait for the active buffer, frozen buffer, segment flush, merge,
retention, or atomic-program tail to become empty.

A request requiring read-your-write freshness carries a source mutation token
or another exact required checkpoint. It waits only until a committed manifest
and locally open reader prove:

```text
manifest.source_through includes required source token
and
manifest.atomic_through includes required atomic token, when applicable
```

That wait is bounded by the request deadline and reports an explicit freshness
failure if publication cannot catch up. It does not discard existing reader
work and does not manufacture global ordering between independent source
journals.

Continuation tokens pin the committed manifest revision required by
KELDRA-0014. Retention keeps that manifest and its artifacts reachable for the
bounded continuation lifetime.

## 14. Background merge

Merging operates only on immutable segments named by a committed manifest. It
reads selected segment cores and liveness state, removes dead documents, and
writes one immutable replacement segment.

```text
S21 + S22 + S23 + selected live views
                  |
                  v
                 S31
```

A merge then proposes a new manifest which replaces its exact input segment
set with `S31`. It preserves the source and atomic checkpoints of the latest
manifest onto which it is validly rebased.

If its current-pointer CAS loses to a concurrent incremental publication, the
merge checks whether all exact input segments and required live views remain in
the new current manifest. If they do, it rebases the same immutable output with
the intervening segments retained. If they do not, the merge output is an
unpublished orphan and may be reclaimed after safety age.

A lost merge CAS never causes source reprojection, active-buffer invalidation,
or query delay. Merge concurrency, input bytes, output bytes, and I/O bandwidth
are bounded independently from incremental segment publication. Incremental
publication has priority whenever journal pressure approaches its configured
limit.

## 15. Retention and bounded-journal backpressure

The source journal is finite. Durable index publication is therefore part of
the source write-path capacity model.

```text
segment publication stalls
        |
        v
committed source checkpoint stalls
        |
        v
journal retention horizon stalls
        |
        v
bounded journal approaches capacity
        |
        v
authoritative source mutations receive backpressure
```

This is the required failure mode. Keldra must not avoid backpressure by:

- advancing an index consumer checkpoint before its manifest is committed;
- dropping journal evidence needed by the index;
- treating durable segment reconstruction as cheap;
- discarding an older frozen buffer in favour of newer source state; or
- allowing unbounded in-memory candidate accumulation.

The central retention invariant is:

```text
source checkpoint C may be released only if the current durable commit
manifest proves that every relevant mutation through C is represented in
durable segment objects referenced by that manifest.
```

The current pointer's retained manifest roots remain the reference-accounting
authority. Removal of an expired retained root occurs through a short exact
CAS. Ordinary Keldra object references and safety age protect segment artifacts;
retention does not traverse and delete a moving artifact graph independently of
the current-pointer mutation.

Metrics and admission must expose enough remaining journal bytes and time to
backpressure for operators to distinguish a temporarily slow index from an
imminent source-write stall.

## 16. Recovery

### 16.1 Builder process loss before segment durability

The active and frozen buffers are disposable. Recovery opens the current
manifest and replays every required journal after its committed checkpoint.

```text
current manifest checkpoint ---> replay retained journal suffix ---> new buffer
```

### 16.2 Segment durable but manifest unpublished

The segment is an orphan and has no searchable authority. Recovery may reuse it
only if its complete content identity, schema identity, source cursor range,
and mutation-unit boundaries prove an exact match. Otherwise it is reclaimed
after ordinary safety age and the mutations are replayed.

### 16.3 Manifest durable but current pointer unpublished

The manifest and newly named segments are unpublished orphans. The previous
current manifest remains authoritative.

### 16.4 Current pointer published but local checkpoint cache stale

Recovery reads the checkpoint from the committed manifest. Replaying an
already committed mutation suffix is safe but unnecessary; a local cached
consumer offset never overrides the manifest.

### 16.5 Query-node local loss

The query node reads the current manifest, fetches its durable segment objects,
verifies their identities and checksums, opens the reader, and begins serving.
No source reindexing occurs.

### 16.6 Durable derived-index loss

Missing committed segment objects despite successful Keldra durability are a
durable-data failure. Keldra first invokes ordinary object repair or replica
recovery. If the committed index cannot be repaired, an explicit full rebuild
scans authoritative source state and catches up retained journals.

This path may consume substantial CPU, I/O, network bandwidth, and time. It is
disaster recovery, not normal cache recovery, and must be observable as such.

### 16.7 Crash matrix

| Crash point | Authoritative recovery |
| --- | --- |
| Active RAM buffer only | Replay from current manifest checkpoint |
| Frozen buffer encoding | Delete partial local output and replay |
| Segment durable, manifest absent | Treat segment as orphan; prove reuse or replay |
| Manifest durable, current pointer old | Previous manifest remains current |
| Current pointer new, local offset old | Recover offset from current manifest |
| Merge in progress | Continue serving old segments; discard or retry merge |
| Query-node disk lost | Fetch committed durable segments |
| Committed segment irreparably lost | Report durable-data failure and explicitly rebuild |

## 17. Initial build and explicit rebuild

An initial build and an explicit rebuild use the same 16 MiB segment production
path. They differ from steady-state catch-up because no partial baseline may be
served as a complete index.

At most one non-serving build root exists per definition. It records the stable
source scan boundary, completed durable segment set, baseline progress, and
journal catch-up checkpoint. The build root exists solely to retain and resume
already durable rebuild segments; it is not a second serving authority and is
not visible to queries.

```text
current pointer
  |-- serving: committed manifest M41
  `-- building: bounded build root B7   (optional, at most one)
```

Each completed rebuild segment may advance `B7` through exact CAS. New rebuild
work extends the same build root rather than starting a newer candidate. When
the baseline and required journal suffix are complete, one current-pointer CAS
publishes the complete rebuilt manifest as `serving` and removes `building`.

If the rebuild cannot keep its required journal suffix within the bounded
journal, source mutations backpressure. Keldra must not silently abandon the
build root, start competing rebuilds, or publish a partial baseline to avoid
that consequence.

## 18. Correctness invariants

1. A committed manifest references only segment components which have received
   ordinary Keldra durability acknowledgement.
2. The current pointer advances only by exact expected-version CAS.
3. A source checkpoint advances only in the same committed manifest which
   proves the corresponding mutations are represented by durable segments.
4. Journal and required source-version retention never advance beyond the
   oldest constraining committed checkpoint.
5. A committed view contains all or none of every relevant atomic-program
   batch.
6. Flush boundaries never split an ordinary mutation record or atomic batch.
7. A reader uses segment cores and liveness state from exactly one pinned
   committed manifest.
8. Publishing a newer manifest does not change or invalidate an older retained
   manifest.
9. Replayed updates and deletes cannot make an older stable source version
   supersede a newer one.
10. A local materialization is never publication, checkpoint, retention, or
    durability authority.
11. Loss of every local materialization cannot lose acknowledged index state.
12. Irreparable loss of a committed segment is reported as durable-data loss;
    it is not silently treated as an ordinary cache miss.
13. At most one non-serving initial/rebuild root exists per definition.
14. Unpublished segments and manifests never become searchable merely because
    they exist.

## 19. Scale, performance, and liveness invariants

1. Normal mutation projection is CPU and bounded-memory work until one segment
   flush boundary is reached.
2. A normal mutation is projected at most once, excluding explicit recovery
   replay and merge rewriting.
3. A newer mutation never causes an already frozen or durable segment to be
   discarded and rebuilt.
4. Active-buffer replacement performs no storage or network operation.
5. A non-empty active buffer is frozen within 1 second unless the process is
   unable to run; publication delay is measured separately.
6. Ordinary query latency is independent of active-buffer, frozen-buffer,
   merge, retention, and journal-tail emptiness.
7. Publication rate is bounded by segment readiness and the one-second age
   target; empty commits are never generated.
8. Builder memory is bounded by the global budget, admitted active buffers,
   frozen-buffer queue, encoder workspaces, and permitted mutation-unit
   overshoot.
9. Segment-publication and merge concurrency and bytes are independently
   bounded. Cross-definition physical publication cohorts retain independent
   per-definition receipts, CAS outcomes, and checkpoints.
10. Journal pressure prioritizes incremental publication over merge and rebuild
    work.
11. A saturated derived pipeline backpressures journal consumption and
    eventually authoritative source mutations rather than consuming unbounded
    memory or dropping required work.
12. A query node recovers local loss by fetching committed segments, with work
    proportional to the committed view it materializes rather than the entire
    authoritative source corpus.
13. A lost retention or merge CAS recomputes only its bounded manifest mutation;
    it does not restart source projection or traverse a moving global artifact
    graph.

## 20. Observability

Per definition, Keldra exposes at least:

- active-buffer accounted bytes, mutation count, and oldest age;
- number and bytes of frozen buffers;
- segment encode, flush, durability, manifest-write, and pointer-CAS latency;
- publication cohort queue wait, logical item count, physical batch count,
  item/byte fill ratio, and incremental-versus-maintenance class;
- logical segment bytes and physical artifact-pack count;
- current source checkpoint vector and atomic checkpoint;
- journal head distance in records and bytes for every constraining source;
- estimated time and bytes remaining before journal backpressure;
- source fetch bytes and projection CPU time;
- publication CAS attempts and losses, classified as incremental, merge,
  retention, or rebuild;
- merge input, output, dead-document reclamation, and write amplification;
- local materialization fetches, bytes, verification failures, and reopen time;
- freshness-token wait latency and timeout count;
- orphan segment and manifest count, bytes, age, reuse, and reclamation;
- ordinary local recovery versus durable-data repair or full rebuild; and
- time spent backpressuring index consumption and authoritative source writes.

Logs describe state transitions once and attach stable definition, manifest,
segment, and cursor identities. Expected buffer swaps, reader reopens, CAS
losses, and retries are metrics or sampled debug events, not high-rate warning
logs.

## 21. Qualification

Qualification uses the public mutation and query paths with indexing
concurrency `1`, `4`, `16`, and the configured maximum. The mutation queue
remains non-empty for sustained runs so idle gaps do not hide contention.

Required cases include:

- small ordinary mutations reaching the one-second age boundary;
- high-volume mutations reaching the 16 MiB RAM boundary;
- the configured operation boundary;
- one maximum admitted mutation and one maximum atomic batch overshooting the
  soft target;
- updates, deletes, delete/recreate, and repeated updates to one identity;
- atomic programs crossing sources and index routes;
- simultaneous incremental publication, reader reopen, merge, retention, and
  explicit rebuild;
- builder crash at every row of the crash matrix;
- query-node local segment loss and redownload;
- injected segment replica loss followed by ordinary Keldra repair;
- journal capacity pressure and eventual source-write backpressure;
- current-pointer CAS races between incremental publication, merge, retention,
  and rebuild completion; and
- multiple definitions competing under the global builder-memory budget.

Pass criteria include:

1. zero partial atomic-program observations in every committed view;
2. zero skipped or falsely checkpointed source mutations;
3. exact result equivalence before and after every crash and merge;
4. no ordinary query retry caused by publication of a newer manifest;
5. query p95 and p99 statistically independent of sustained journal queue
   depth, excluding explicit freshness-token waits;
6. no rebuild or discard of a completed frozen segment because newer source
   mutations arrived;
7. stable memory within configured bounds, including overshoot accounting;
8. stable durable bytes within retention and merge-policy bounds;
9. measured backpressure only at documented journal or resource limits;
10. query-node recovery from durable segments without source reindexing;
11. source reindexing only after an explicitly reported irreparable durable
    index failure or operator-requested rebuild; and
12. bounded log volume under sustained indexing and merge traffic.

Performance reports record source mutation count and bytes, projected document
count, segment and artifact bytes, flush reason, buffer age, durability mode,
topology, journal capacity, builder memory budget, merge activity, publication
latency distribution, query latency distribution, and exact source revision.

## 22. Consequences

This architecture deliberately accepts:

- up to one second of incremental work may exist only in builder RAM before a
  time-triggered flush begins;
- a crash replays that uncommitted work from retained journals and source
  objects;
- segment durability or publication stalls can eventually backpressure source
  mutations because journals are bounded;
- a logical segment can occupy several physical artifact packs;
- merges rewrite durable derived bytes to bound query and liveness
  amplification; and
- explicit full rebuild remains expensive even though authoritative source data
  makes it possible.

In return, Keldra gains:

- no persistent mutation-by-mutation index-builder state;
- no repeated candidate-generation replacement during continuous ingestion;
- bounded, near-instant active-buffer transitions;
- durability work amortized over complete 16 MiB-class segments;
- exact journal recovery from the last committed manifest;
- stable point-in-time readers unaffected by newer publications;
- retention coupled to proven durable progress;
- ordinary node recovery from durable segment objects; and
- explicit backpressure instead of unbounded memory or silent data loss.

## 23. Non-goals

This RFC does not:

- make source journals byte-complete or globally ordered;
- make local segment files authoritative;
- treat durable segment loss as cheap;
- remove ordinary Keldra durability, placement, erasure coding, reference
  accounting, serving fences, authorization, or accounting;
- change format-v4 query semantics or public index APIs;
- permit a partial baseline or partial atomic program to become searchable;
- require exactly one kernel `fsync` or storage-device operation per segment;
- allow unbounded active, frozen, publication, merge, or rebuild work; or
- promise that a 16 MiB RAM buffer encodes to exactly one 16 MiB artifact.

## 24. Summary

Keldra's source journals and immutable ordinary source objects are the replay
authority for incremental indexing. Builders keep only bounded in-progress
state in memory, replace their active buffer immediately at the first 16 MiB,
one-second, operation-count, or explicit-refresh boundary, and flush the frozen
buffer as one immutable logical segment backed by durable Keldra objects.

One committed manifest binds the durable segment set to exact source and atomic
checkpoints. Only that manifest permits journal and source-version retention to
advance. Readers pin immutable committed manifests, while node-local segment
materializations remain disposable and recover by fetching durable segment
objects. Background merges replace segment sets without rebuilding source work.

The result follows Lucene's proven RAM-buffer, immutable-segment, commit-point,
reader-reopen, and background-merge model while using Keldra's own durable
objects, source-local journals, atomic programs, distributed publication, and
bounded backpressure where they provide the stronger architecture.
