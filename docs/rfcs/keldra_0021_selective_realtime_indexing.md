# KELDRA-0021: Selective Real-Time Indexing

Status: Core path implemented; admission/observability hardening and topology,
failure, and performance qualification pending

Amends: KELDRA-0016 sections 3, 5, 6, 7, 9, 10, 13, and 16

Audience: Keldra implementors, operators, client authors, and reviewers

## 1. Decision

Keldra provides selective real-time indexing as an explicitly requested service
for individual committed mutations. Ordinary mutations continue through the
high-throughput ordered base projection. A mutation marked real-time also enters
an independently admitted, partitioned real-time projection lane and receives a
visibility token.

Real-time processing is not enabled globally, per bucket, or per index. It is a
property of the mutation request. Mutations affecting the same logical index may
therefore use different visibility classes without forcing low-latency segment
publication for unrelated traffic.

The query view is the deterministic composition of:

1. the newest complete published base generation;
2. published real-time overlay generations not yet absorbed by that base; and
3. newest-version liveness records which remove replaced or deleted documents.

The durable object versions and source journal remain application-data
authority. Base roots remain authority for complete contiguous source-prefix
coverage. Real-time overlays are durable derived index data, but they never
claim that holes containing ordinary mutations have been projected.

Keldra uses its native immutable segment representation, object placement,
publication, authorization, continuation, accounting, integrity, and GC
machinery. It does not introduce Lucene, Tantivy, another mutable index, or a
second authoritative WAL.

## 2. Goals and non-goals

The design must:

- make a specifically requested committed mutation query-visible with bounded
  latency without flushing unrelated normal work;
- scale preparation and publication across cores and independent partitions;
- preserve exact stable-object/version replacement and deletion semantics;
- preserve atomic-program all-or-none visibility;
- calculate hits, ordering, facets, and aggregates over the same authorized
  composed view;
- survive process and node failure without losing an acknowledged real-time
  request;
- isolate real-time memory, queue, CPU, and publication pressure from normal
  indexing and queries; and
- retire overlay work once the base projection represents it.

The design does not:

- make every write synchronously publish an index generation;
- promise that ordinary mutations between two real-time mutations are visible;
- replace complete-prefix `required_freshness` semantics;
- let an in-memory queue or reader become storage authority; or
- weaken realm authorization or exact-current candidate validation.

## 3. Public semantics

Each mutation carries an `IndexingIntent`:

- `STANDARD` selects only normal asynchronous projection;
- `REALTIME` requests accelerated overlay projection and a visibility token.

The omitted/default value is `STANDARD`. Unknown numeric enum values are
rejected rather than silently interpreted as either lane.

A successful real-time mutation receipt contains an opaque, integrity-protected
`IndexVisibilityToken`. The token binds at least:

- tenant and bucket identity;
- authenticated application and stable object identity;
- committed object version or tombstone version;
- source node, source epoch, and the complete inclusive source-position range
  emitted by the mutation (canonical plus alias-visible changes);
- atomic-program position and membership when applicable;
- source placement identity needed to reject stale routing; and
- an expiry bounded by retained root and marker lifetimes.

The receipt always reports the committed object mutation even if later waiting
for index visibility reaches a deadline. SDKs may provide a `write_realtime`
convenience which performs the mutation and then waits, but the wire operation
does not hide a successful durable write behind a deadline error.

A query may supply the visibility token. It waits until its selected physical
projection has either:

- published an overlay containing the exact requested mutation; or
- published a base generation proving that it has absorbed the mutation.

The wait ends at the request deadline. Query admission resolves and pins the
current physical catalog because object mutations are index-agnostic. Invalid,
expired, cross-tenant, cross-bucket, or superseded-placement tokens fail
explicitly. A selected mutation which does not match an index still becomes
satisfied once that projection has durably processed its no-match outcome.

This contract differs from `required_freshness`:

| Requirement | Meaning |
| --- | --- |
| `required_freshness` | Every applicable source mutation through a contiguous checkpoint is represented. |
| real-time visibility token | The specifically named committed mutation is represented; unrelated ordinary holes may remain. |

A caller needing a complete prefix uses `required_freshness`. A caller needing
selected write visibility uses the real-time token. Supplying both requires both
conditions at one query deadline.

## 4. Authority and persistence

The source journal event records the indexing intent atomically with the object
mutation. A committed `REALTIME` mutation therefore remains discoverable after
failure.

The same store commit writes a compact real-time routing marker keyed by source
identity and position. The marker contains only routing and exact-version
evidence; it does not duplicate the object payload or extracted index material.
Markers are derived acceleration state:

- the source journal is their rebuild authority;
- losing markers may delay visibility but cannot change object truth;
- a marker is removed only after every relevant projection proves overlay
  publication or base absorption; and
- recovery prioritizes pending markers before ordinary backlog work.

The format-v1 namespace gains marker and overlay-current keys. This is the sole
supported format; no legacy reader, dual writer, migration path, or alternate
identity is introduced.

## 5. Routing and lanes

The committed journal feeds two paths:

```text
durable source journal
        |
        +-- ordered base projection -----------------> complete-prefix roots
        |
        +-- durable real-time routing markers
                    |
                    +-- family/partition lane 0 --+
                    +-- family/partition lane 1 --+--> overlay generations
                    +-- family/partition lane N --+
```

Real-time work is sharded by physical projection family and source partition.
Different partitions and families prepare concurrently. Mutations of the same
stable object retain source order. One mutation relevant to several shared
physical recipes is extracted once and fanned into the corresponding physical
components.

Each lane uses a bounded micro-batch. It freezes on the first of:

- a short maximum dwell;
- target accounted bytes;
- target operation count;
- a waiter for one of its contained tokens;
- placement or catalog transition; or
- memory pressure requiring already admitted work to publish.

The dwell is an amortization tool, not a visibility guarantee by itself. Token
waiters actively wake the owning lane. Unrelated base accumulators are not
flushed.

## 6. Overlay representation

An overlay is an immutable Keldra-native query run plus a compact liveness
delta. It uses the same dense segment-local document identifiers, term
dictionaries, postings, points, typed columns, stable identities, exact material
versions, range-readable object layout, and reusable `Arc<SegmentReader>`
instances as base segments.

For one stable object, publication atomically exposes:

- the new indexed document when its exact committed version matches the
  physical recipe;
- a tombstone when the mutation deletes it or makes it leave the recipe; and
- a liveness replacement hiding every older base or overlay document.

No old-field reconstruction or per-term subtraction is performed.

An overlay generation binds its included sparse source positions rather than a
contiguous source checkpoint. Sparse evidence is never reused as base freshness
evidence.

## 7. Query composition

A query pins one base root vector and the compatible overlay-current vector.
Both are bound into the query snapshot and continuation token. Publication after
the pin cannot change that query's view.

Candidate evaluation follows this order:

1. seek base and overlay recipe dictionaries;
2. intersect or advance postings independently;
3. merge by stable object identity and exact material version;
4. apply newest liveness/tombstone evidence;
5. validate exact current object heads in bounded batches;
6. enforce the index's application or realm authorization at one pinned
   authorization revision; and
7. calculate hits, ordering, pagination, facets, and aggregates only from the
   surviving authorized view.

A newer ordinary mutation which has not reached the base may supersede a
real-time overlay document. Exact-current validation removes the stale overlay
candidate. If that newer ordinary mutation introduces a new matching document,
it remains absent until normal projection reaches it; that is the documented
selective-visibility contract.

Counts and aggregates must never add base and overlay values independently.
They run after stable-identity replacement and authorization filtering so
replaced documents are not double counted and unauthorized cardinality is not
leaked.

## 8. Atomic programs

Real-time intent on any member of an atomic program applies to the complete
atomic publication unit. Keldra never publishes a sparse overlay containing
only part of an atomic program.

The aggregate visibility token binds the atomic position, complete-unit hash,
and canonical set of affected tenant/bucket routes. The same opaque token may
therefore be supplied to an index query in any affected bucket; admission
selects that bucket's bound route without weakening the all-or-none unit. A
query either selects overlay evidence for the complete atomic unit or waits.
Base absorption retires the complete unit together.

## 9. Absorption and garbage collection

When a complete-prefix base root covers an overlay mutation's exact source and
atomic position, that overlay entry is absorbed. Reconciliation publishes a new
overlay generation omitting absorbed entries and then removes satisfied routing
markers.

Artifacts remain retained while referenced by:

- the current overlay generation;
- a pinned query or continuation;
- an unexpired visibility token whose condition has not moved to the base; or
- recovery/publication state.

Normal object GC reclaims unreachable overlay generations and packs. Overlay
retirement is asynchronous and never blocks base advancement or real-time
publication.

## 10. Failure and recovery

Acknowledgement ordering is:

1. commit object state, source journal event, indexing intent, and routing
   marker atomically;
2. return the mutation receipt and visibility token;
3. prepare and publish the overlay asynchronously; and
4. notify waiters only after the overlay current is durable and readable.

A crash before step 1 returns no success. A crash after step 1 replays the
marker. A crash during preparation discards memory and retries from the exact
object version. A crash after artifact staging but before overlay-current CAS
leaves ordinary orphan artifacts for bounded GC.

Placement change transfers unresolved markers and overlay ownership using the
same authenticated partition ownership rules as base projection. A node never
serves an overlay after losing its serving fence.

Corrupt durable overlay artifacts are integrity failures. Admission refusal,
queue saturation, and temporary object-store unavailability are retryable and
must not halt the base producer.

## 11. Resource isolation and fairness

One accounted process working-memory authority contains distinct reclaimable
accounts for:

- normal projection preparation and publication;
- real-time preparation and publication;
- query readers and caches; and
- compaction and merge progress.

Reserved progress capacity prevents any account from consuming memory needed
to publish and release already admitted work. Unused shares may be borrowed
through the common authority; hidden independent caps are forbidden.

Real-time admission is bounded by:

- global and per-tenant queued mutations and bytes;
- global and per-tenant in-flight preparation;
- family/partition lane concurrency;
- micro-batch bytes, operations, and dwell;
- outstanding visibility tokens and waiters;
- overlay run count and unmerged bytes; and
- publication and object-write concurrency.

Admission uses fair queuing across tenants. A tenant which exhausts its
real-time allowance receives a retryable resource error for the real-time
request; Keldra does not silently downgrade it to standard indexing after
committing a promise it cannot meet.

Normal indexing cannot consume promised real-time publication capacity.
Real-time work cannot starve base projection, queries, or overlay/base merging.

## 12. Compaction

Overlay merging is bounded and asynchronous. It combines only compatible
overlay generations and removes entries already absorbed by the base. Base LSM
compaction remains independent and does not gate token satisfaction.

Real-time publication must not depend on a global store commit mutex. Parallel
preparation and independent partition publication remain concurrent; only the
required per-partition overlay-current CAS is serialized.

Repeated small real-time batches must remain bounded under sustained traffic.
The system reports and controls overlay run debt rather than relying on the
query executor's maximum-run guard as normal flow control.

## 13. Security and privacy

Real-time intent grants no additional storage or query authority. The mutation
is authorized exactly like a standard mutation. A token is usable only by a
caller already authorized to query the bound index and tenant/bucket.

Tokens are integrity protected with the existing operator token authority,
contain no object payload or field values, and are rejected on identity,
placement, expiry, or authorization mismatch. The query separately pins the
current catalog and verifies the exact source evidence against it. Logs and metrics never
emit token bytes, object payloads, indexed field values, or credentials.

Realm-authorized indexes evaluate overlay candidates against the same custom
realm, subject, relation, namespace mapping, and pinned authorization revision
as base candidates.

## 14. Observability

Required metrics distinguish base and real-time work:

- requested, admitted, refused, replayed, published, absorbed, and expired
  real-time mutations;
- receipt-to-overlay-visible latency and waiter latency;
- queue depth/bytes/oldest age by tenant and partition without tenant labels of
  unbounded cardinality;
- micro-batch size and dwell;
- preparation, artifact staging, overlay-current CAS, and reader-open latency;
- overlay runs, bytes, dead-document ratio, merge debt, and GC debt;
- memory owned by preparation, published readers, caches, and merging; and
- stale-version removals, base/overlay deduplication, and authorization-filter
  counts.

Freshness telemetry continues to describe complete base-prefix coverage.
Separate visibility telemetry describes sparse real-time token satisfaction.

## 15. Qualification

Release qualification must cover:

- standard-only, real-time-only, and mixed traffic on the same index;
- 1/2/3-node topologies and ownership reassignment;
- inserts, deletes, projection-preserving updates, material-changing updates,
  and repeated replacement of one stable object;
- atomic programs containing both matching and non-matching objects;
- concurrent real-time lanes scaling with indexing cores and memory;
- sustained traffic proving bounded overlay run and merge debt;
- base absorption without duplicate hits, facets, or aggregates;
- application and realm authorization, including continuation tokens;
- crash/restart before and after marker commit, artifact staging, and overlay
  current publication;
- expired, forged, cross-index, cross-tenant, and stale-placement tokens;
- overload fairness and retryable admission errors;
- exact-current validation when a later standard mutation supersedes a
  real-time document; and
- performance comparison showing that standard-only ingest does not pay the
  real-time publication cost.

Success requires correct final query results, zero lost acknowledged real-time
requests, stationary base and overlay debt under the admitted workload, and
positive throughput scaling when additional productive CPU, memory, and object
I/O capacity are supplied.
