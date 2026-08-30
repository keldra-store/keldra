# KELDRA-0020: Logical Index Catalog and Shared Physical Projections

Status: Accepted; implementation in progress

Supersedes: KELDRA-0019 in full

Amends: KELDRA-0013, KELDRA-0014, and KELDRA-0016 where they make one
logical definition the unit of source consumption, physical segment ownership,
manifest publication, checkpointing, scheduling, or rebuild

Audience: Keldra implementors, operators, client authors, and reviewers

## 1. Decision

Keldra separates a public logical index definition from the physical projection
components which satisfy it. A logical definition is durable catalog metadata.
It does not own an autonomous source-journal consumer, builder, mutable buffer,
segment tree, manifest stream, checkpoint, or query cache.

Assigned source partitions consume their source journal once. A bounded writer
projects each exact source version once, compares its canonical indexed state
with the preceding projected state, and updates each distinct changed physical
recipe once. Equivalent logical definitions reference the same physical state.

The physical model has four layers:

```text
logical definition catalog
        |
        v
canonical membership and field recipes
        |
        v
source-partition writer and projected-document state
        |
        v
immutable component segments + atomic projection generation
```

The public index-definition and query contracts remain logical. A query first
resolves its authorized definition to an exact physical generation and field
bindings. Shared bytes do not merge authorization domains or make another
definition visible.

This is a clean derived-index format and ownership change. There is no dual
writer, mixed physical generation, legacy physical reader, or artifact
conversion path. Ordinary objects and retained source journals remain the
rebuild authority. An upgrade discards incompatible derived index artifacts and
rebuilds the new projections; it does not migrate them as authoritative data.

## 2. Why the previous unit is wrong

KELDRA-0019 shared payload parsing but deliberately retained definition-local
assembly and publication. Production-shaped D1, D16, and D64 profiling proved
that parsing was not the dominant remaining cost. Equivalent definitions still
independently read prior artifacts, update locators and live masks, seal
segments, publish manifests, mutate references, materialize query cache files,
and advance checkpoints.

That makes work proportional to logical definition count even when additional
definitions introduce no new indexed semantics. It also creates many short
durable operations which queue behind the Store commit authority. The evidence
and exact profiles are recorded in
`../qualification/index-scale-investigation.md`.

The corrected separation follows three proven lessons:

1. PostgreSQL keeps index definitions and lifecycle state in catalogs. A
   catalog row is not a permanent worker. A row update maintains only the
   physical indexes it affects.
2. PostgreSQL HOT avoids new index entries when indexed values did not change,
   and follows a stable row indirection to the current tuple version.
3. Lucene uses bounded writers, in-memory indexing buffers, immutable segments,
   point-in-time reader generations, tombstones, and background merging.

Keldra adopts those ownership and lifecycle principles without adopting
PostgreSQL's heap format, Lucene's file format, or Elasticsearch's
logical-index-to-shard mapping.

## 3. Vocabulary

**Logical definition** is the public named index contract: source scope, public
field names, query capabilities, authorization, definition version, and
lifecycle state.

**Source partition** is the existing authoritative journal and object-placement
scope over which one assigned projection writer consumes ordered changes.

**Document universe** is the stable mapping between a source record identity
and a physical document identity within one source partition.

**Membership recipe** is the canonical semantic rule deciding whether a source
record belongs to a logical view. Identical membership semantics share one
physical membership component.

**Field recipe** is the canonical semantic rule selecting, normalizing, and
encoding one indexed value. Identical field semantics share one physical field
component.

**Projection family** is one source partition's document universe plus its set
of referenced membership and field recipes.

**Projected-document state** is disposable, rebuildable state containing the
last exact canonical membership and field values for a stable source record.
It is used to decide which physical recipes actually changed.

**Projection generation** is one immutable, query-visible root binding a
complete source barrier to document-state, membership, and field component
segments.

**Logical binding** maps one exact logical-definition version to a projection
family, the first generation where all its recipes were ready, its membership
and field recipes, and public field IDs. It follows later complete generations
of that family without a catalog rewrite.

## 4. Durable logical catalog

The ordinary index-definition object remains the public mutation and
authorization authority. The runtime derives compact durable catalog records:

```text
LogicalDefinitionRecord {
    tenant_id
    bucket_id
    index_id
    definition_object_version
    definition_version
    source_scope_id
    membership_recipe_id
    field_bindings[]       // public field ID/name -> field recipe ID
    query_contract_hash
    physical_family_id
    state                  // building, ready, replacing, deleting, failed
}
```

Catalog records are stored in the existing Store/RocksDB authority and are read
in bounded pages. Process memory holds compact immutable routing pages and
point-lookup caches, not one complete builder object per catalog entry.

Catalog cardinality must not create an equal number of:

- Tokio tasks;
- journal cursors;
- retained source pages;
- mutable segment writers;
- publication timers;
- manifest/current objects; or
- filesystem query-cache files.

Creating a definition whose complete physical semantics already exist adds a
logical binding with the existing ready revision. It performs no source rebuild.
Creating a genuinely new recipe schedules only the new physical recipe's
bounded backfill and publishes the binding after the first complete generation
containing that recipe.

Catalog changes compile a new immutable routing snapshot. A source mutation
never scans all logical definitions and never takes a process-wide mutable
catalog lock.

Process-local assignment notifications are wakeups, not a change journal. A
receiver which falls behind must retain already completed physical progress and
reconcile from an exact bounded-memory view of the durable catalog. It must not
discard a partially or completely reconstructed projection inventory and
restart from page one. The assignment scan and replacement receiver share the
Store's assignment mutation fence, so no change can fall between the exact
inventory and its catch-up stream. A dedicated lightweight collector drains
that receiver independently of source/index work and coalesces only the newest
committed mutation per definition. The pending map is bounded by catalog
cardinality. Full durable replacement is reserved for startup and genuine
collector lag; a long indexing turn cannot repeatedly trigger an `O(catalog)`
rescan. A later catalog-page format may reduce the duration of the snapshot
fence, but cannot weaken this boundary.

## 5. Canonical physical identities

Recipe sharing is allowed only when every result-affecting semantic input is
identical. The stored canonical specification is authoritative; hashes are
content addresses and lookup accelerators, not unchecked equality proofs.

A source-scope identity includes:

- tenant and bucket physical authority;
- canonical path-prefix membership;
- accepted content type;
- source-record expansion rules; and
- codec/result identity versions.

A membership-recipe identity includes its source-scope identity and canonical
membership predicate. A field-recipe identity includes:

- source selector;
- scalar/vector type and cardinality;
- missing and null semantics;
- analyzer, normalization, collation, and date format;
- exact, prefix, range, full-text, order, facet, aggregate, vector, scoring,
  positions, and norms capabilities; and
- physical codec and query-semantic versions.

Field IDs are logical binding IDs, not physical ownership. Two definitions can
expose the same physical field recipe under different public names or IDs.

The format-v5 projection-family ID is the full domain-separated digest of the
tenant ID, bucket ID, and canonical membership/source-scope recipe. It is not a
truncated logical index ID. Definitions with the same source universe but
different field subsets therefore share one family, while a tenant, bucket, or
membership change necessarily selects another family.

Sharing is limited to one tenant/bucket authority. Cross-authority sharing is
not permitted by this RFC.

## 6. Source-driven routing and projection

For each assigned source partition, the runtime maintains an immutable compiled
plan:

```text
path/content-type router
    -> applicable source scopes
    -> union selector trie
    -> membership recipe ordinals
    -> field recipe ordinals
```

The plan is rebuilt from catalog deltas outside source processing and swapped
atomically. The source hot path performs direct prefix/content-type routing. It
does not enumerate unrelated definitions.

For each source-journal mutation, the writer:

1. reads the journal evidence once;
2. resolves the exact source version once;
3. streams the payload once through the applicable union selector plan;
4. produces exact canonical membership and field values;
5. compares them with projected-document state;
6. updates only distinct changed recipes;
7. appends those deltas to bounded projection-family buffers; and
8. advances the physical source barrier only when all routed work is represented.

The complexity target for a mutation is:

```text
O(route lookup + payload bytes + distinct changed recipes)
```

It must not be `O(total definitions)`, `O(matching definitions squared)`, or
`O(matching aliases)`.

## 7. Keldra projection-preserving updates

Keldra's HOT equivalent is called a **projection-preserving update**.

Every projected source record has a stable physical document key and exact
projected state:

```text
ProjectedDocumentState {
    stable_document_key
    source_path
    source_record
    current_source_version
    live
    membership_values[]
    canonical_field_values[]
}
```

Canonical values are compared exactly. A bounded digest may reject obvious
differences quickly, but digest equality cannot by itself authorize skipping a
physical update.

Four outcomes are defined:

1. **No relevant change.** Only unindexed payload or source metadata changed.
   Update the shared document-head/source-version state and source barrier. Do
   not write postings, points, doc values, facets, order keys, vectors, or
   membership data.
2. **Field subset changed.** Update only those field recipes. Reuse every
   unchanged field and membership component.
3. **Membership only changed.** Update only the affected membership recipes.
   Reuse unchanged field components.
4. **Insert, delete, path, expansion, or material indexed change.** Append the
   required document, component, and liveness deltas. Obsolete immutable data is
   reclaimed by merge and reference GC.

Queries use:

```text
physical field/membership entry -> stable document key
stable document key -> generation-pinned live state and exact result identity
```

This is the analogue of an index entry continuing to reach a newer PostgreSQL
tuple through HOT indirection. An unchanged indexed value remains valid when an
ordinary object's version changes.

Projected-document state is not an object-data authority. Losing it causes
replay or bounded rebuild from ordinary objects and source journals.

## 8. Physical segments and generations

One source-partition writer owns bounded RAM across all dirty projection
families. Admission is byte based and shared; it does not reserve a fixed buffer
per logical definition. Flush boundaries use accounted memory, maximum wall
age, and explicit operation safety caps.

A flush writes immutable integrated-payload packs containing the changed
document, membership, and field components. It does not create a filesystem
file per component or per logical definition.

One projection generation contains:

```text
ProjectionGeneration {
    family_id
    revision
    source_barrier
    placement_fence
    document_state_segments[]
    membership_roots[]
    field_roots[]
    physical_order_roots[]
    previous_generation_hash
    encoded/logical byte accounting
}
```

The component lists in that illustration are logical lists, not one unbounded
manifest array. They are encoded as a content-addressed bounded-fanout Merkle
directory. Leaf pages contain canonical sorted component roots; branch pages
contain canonical key ranges and child hashes. The generation record contains
only the directory root, root count, barrier, and predecessor. This keeps
generation publication and point lookup bounded without creating one
filesystem inode per component.

An unchanged component root is referenced from the previous generation rather
than rewritten. Publication atomically installs the complete generation and
then makes eligible logical bindings visible. No query can combine component
roots from different source barriers.

### 8.1 Format-v5 stable document keys

Format v4 cannot be extended into the projection-preserving design by adding a
comparison cache alone. Its postings use segment-local `DocId` values and its
identity rows embed the source/result object version. Reusing those postings
after a source-version change would either return the old version or cause the
exact-current candidate gate to reject the result. Format v5 therefore makes a
stable document key the join key shared by every independently reusable
component.

The key is derived from the source-scope identity, canonical source path, and
deterministic expanded-record identity. The current expansion contract uses the
record ordinal; a change which shifts ordinals is consequently a material
delete/insert for those records. Hash collisions are never accepted as
equality: every head entry retains and validates the complete source path and
record identity.

```text
StableDocumentKey = H(
    format/domain,
    source_scope_id,
    source_path,
    source_record_identity
)
```

Format-v5 immutable streams are separated by authority:

```text
document-head delta:
    stable key -> source path, record identity, current source version,
                  current result identity, live/deleted state

membership delta for recipe R:
    stable key -> present/absent

field delta for recipe R:
    stable key -> exact canonical field state or tombstone

order delta for order recipe R:
    stable key -> canonical order tuple or tombstone
```

The newest entry at or below the pinned generation wins independently in each
stream. A query obtains stable keys from postings/points/order, resolves the
pinned membership and live head, then obtains the exact current result identity
from that head. No query reads a field component from a generation newer than
its pinned complete barrier.

### 8.2 Exact projected-state comparison

Projected-document state stores the complete canonical bytes for each recipe,
not only a digest. A digest is kept as a bounded negative-comparison
accelerator. Equality is authorized only after byte-for-byte comparison of the
canonical recipe state and exact membership state.

For one source mutation the writer constructs a sorted recipe-state vector and
merge-compares it with the previous vector. It emits:

- one document-head delta whenever the exact source/result version changes;
- no membership or field delta when all canonical indexed state is equal;
- a delta only for each added, removed, or byte-different recipe; and
- tombstones for records or recipe values removed by the mutation.

The head delta and every changed recipe delta are installed by one projection
generation. A crash may leave unattached immutable packs, but cannot publish a
head whose required changed recipe roots are absent. Recipe compaction folds
newest-by-stable-key state and may rewrite physical keys without changing the
generation barrier or logical result.

Segments are immutable. Updates and deletes append deltas/tombstones; automatic
tiered merges combine small segments, fold document-state chains, and reclaim
obsolete entries. Merge concurrency and I/O are globally bounded and
throttled. A merge changes physical shape without changing logical results or
the represented source barrier.

## 9. Query resolution

A query follows this exact sequence:

1. authorize and load the requested logical definition;
2. resolve its exact logical binding;
3. load and pin the family's current generation, requiring it to be at or
   beyond the binding's ready revision;
4. compile public field IDs through the binding to physical recipes;
5. execute predicates, ordering, facets, aggregates, text, and vectors against
   those component roots;
6. intersect candidates with the definition's membership component and the
   generation's live-document state;
7. map stable document keys to exact visible result identities; and
8. perform the existing result-object authorization/refill behavior.

Pagination binds logical definition version, projection generation, query
shape, order, and search-after state. A definition replacement cannot continue
an old cursor against a different binding.

The query cache keys physical component identity and generation. Equivalent
definitions reuse opened physical readers while retaining separate logical
authorization and query contracts.

## 10. Rebuild and definition changes

A new source scope or recipe is built from a source snapshot plus its retained
journal suffix. Work is scheduled by missing physical recipe/family, not by
logical definition.

Definition changes behave as follows:

- an identical physical contract changes only logical metadata;
- adding a field references an existing recipe when available, otherwise builds
  that recipe once;
- removing a field drops a logical reference without rewriting other fields;
- changing source membership creates or reuses the new membership recipe;
- changing incompatible analysis/type semantics creates a new field recipe;
- deletion removes the logical binding and decrements physical references.

A definition becomes ready only when every referenced physical component has an
exact complete source barrier compatible with the binding. Rebuild publication
is all-or-none at the logical binding.

## 11. Distribution, durability, and recovery

The source journal and ordinary objects remain input authority. Integrated
payload storage remains immutable artifact authority. Existing placement,
integrity, erasure/replication, reference delivery, and Store commit fencing
apply to projection packs and generation roots.

Assignment is by source partition/projection family rather than definition.
Only the assigned writer may publish that family's next generation under the
exact placement fence. Logical bindings are independently authorized catalog
state and cannot grant publication authority.

Crash points are recovered as follows:

- a lost RAM buffer replays its unadvanced source window;
- an uploaded but unattached pack is an ordinary bounded orphan and is reclaimed
  by artifact GC;
- a complete generation uploaded before current publication is unattached and
  reclaimed unless retry attaches the exact content identity;
- a published physical generation without a new logical binding remains valid
  physical state and can satisfy that binding on retry;
- a logical binding never becomes ready before a complete published generation
  contains every recipe it names;
- restart rebuilds immutable routing pages from the durable catalog in bounded
  pages and resumes source partitions from physical barriers.

GC deletes a pack only after no retained projection generation, logical
binding, in-flight query lease, rebuild root, or publication attempt references
it. Reference updates are batched per physical publication rather than repeated
for every logical definition.

## 12. Resource bounds and fairness

The node enforces explicit bounds for:

- catalog page and routing-snapshot residency;
- source windows and exact payload bytes;
- canonical projected-document state;
- dirty recipe/component buffers;
- immutable pack assembly;
- merge input/output and scratch;
- opened physical readers and query work;
- concurrently active source partitions and backfills; and
- queued catalog changes.

Inactive logical definitions consume durable catalog bytes and bounded cache
entries only. They do not reserve workers or construction memory.

Fair scheduling rotates source windows and backfills by source partition and
oldest wall-clock lag. A hot family cannot monopolize the writer, and a large
catalog cannot dilute service for the bounded set of dirty families.

## 13. Telemetry

Required counters and histograms are labelled by physical family/recipe class,
not by high-cardinality recipe IDs:

- catalog definitions, source scopes, membership recipes, and field recipes;
- resident routing pages and compiled-plan rebuild duration;
- source records read, exact objects fetched, and payloads parsed;
- projection-preserving updates;
- membership-only, field-subset, insert, and delete updates;
- distinct recipes considered and changed;
- buffer bytes, flush reasons, packs, components, and fill ratio;
- physical generations and logical bindings published;
- physical bytes and Store/reference mutations per accepted source mutation;
- source receipt-to-projection visibility lag;
- merge debt, bytes, duration, throttling, and reclaimed tombstones;
- opened physical readers, logical reader reuse, and cache bytes; and
- source-partition runnable, wait, I/O, Store-lock, and publication phase time.

Periodic INFO summaries provide qualification evidence without enabling
per-object logs. DEBUG spans retain exact phase attribution.

## 14. Required correctness tests

The implementation must prove:

1. equivalent definitions share one physical generation and return identical
   authorized query results;
2. different public field names can bind one physical recipe;
3. field subsets and predicates share only compatible components;
4. incompatible types, analyzers, formats, cardinality, or capabilities never
   share;
5. an unindexed payload update emits no field/membership physical mutation;
6. one changed field updates only that recipe;
7. membership-only changes preserve unchanged field components;
8. insert, overwrite, delete, path reuse, arrays, expanded records, aliases,
   dates, facets, aggregates, order, text, vectors, and pagination retain exact
   released semantics;
9. definition create/replace/delete racing source writes binds an exact complete
   generation or retries without partial visibility;
10. crash/restart at every publication boundary replays exactly and GC reclaims
    unattached packs;
11. catalog paging and restart do not create one task per definition; and
12. physical work counters are independent of equivalent logical-definition
    count.

## 15. Scale qualification gates

Qualification uses fresh volumes and the same exact candidate artifact on the
8-core/32-GiB SSD and 16-core/64-GiB rotational hosts. All experiment files and
Keldra data remain under `~/keldra_experiments`.

### 15.1 Catalog-250K

Create 250,000 live logical definitions, restart, point-read and page-list them,
then mutate objects matching zero, one, and a bounded set of recipes. Record
creation rate, restart time, RSS, task count, catalog lookup/list latency,
mutation work, and query correctness.

### 15.2 Equivalent D1 through D640

Run D1, D4, D16, D64, D256, and D640 identical definitions over one hot source
stream. Physical component items/bytes, payload parsing, Store/reference
mutations, source work, and visibility lag must remain close to D1. Logical
catalog/binding bytes may grow linearly.

### 15.3 Heterogeneous recipes

Vary source prefixes, predicates, field subsets, types, analyzers, dates,
facets, aggregates, ordering, text, and vectors. Demonstrate that physical work
tracks distinct matched changed recipes, not definitions, and verify every
public query against an independent expected result.

### 15.4 Projection-preserving update

Continuously mutate large unindexed properties while indexed values remain
constant, then change one indexed field and one membership predicate. Prove
zero physical field work in the first phase and exact component-local work in
the later phases.

### 15.5 Sustained keep-up

Offer a fixed realistic write rate for at least 30 minutes. Source-to-visible
lag must reach a stationary bounded distribution during ingestion. A final
drain is not evidence of keeping pace.

### 15.6 Churn and recovery

Create, replace, and delete definitions while ingesting; crash during routing
swap, buffer flush, generation publication, binding publication, merge, and GC.
Prove exact results, bounded replay, and eventual orphan reclamation.

Each result records source commit, binary/image digest, host/topology, corpus
hash, definitions and distinct recipes, offered/accepted rate, latency
distributions, visibility lag, CPU profile, async waits, Store-lock ownership,
RocksDB and artifact bytes, RSS, task/file counts, correctness, and end-to-end
duration.

## 16. Success criteria

The architecture is complete only when:

- 250,000 catalog definitions are operational without 250,000 runtime tasks or
  physical copies;
- equivalent D640 remains bounded near D1 physical work;
- projection-preserving updates avoid all unchanged component writes;
- heterogeneous work scales with distinct changed recipes;
- sustained ingestion reaches a stationary visibility lag on both hosts;
- crash/restart and churn preserve exact query/publication semantics; and
- no resource bound, authorization check, placement fence, durability, or GC
  guarantee is weakened to obtain the result.

## 17. Non-goals

This RFC does not:

- make 250,000 genuinely distinct changed recipes free;
- share physical bytes across tenant/bucket authorization authorities;
- make derived indexes an object-data authority;
- introduce synchronous index construction into ordinary object commits;
- adopt Lucene as a dependency or use its file format;
- create one Elasticsearch-style shard per logical definition;
- weaken exact-version result reads, query authorization, or freshness
  semantics; or
- preserve legacy derived-index physical formats.

## 18. Implementation milestones

The physical-format replacement is delivered in measured milestones. Passing
an earlier milestone does not weaken the completion criteria in section 16.

### 18.1 Milestone A: equivalent complete projections

Implemented on `feat/shared-index-projection`:

- a logical definition has a deterministic tenant/bucket-scoped physical
  projection identity derived from its complete canonical schema;
- every equivalent definition maps to one scheduler representative, builder,
  manifest/current stream, artifact owner, retention identity, and query reader;
- queries authorize and paginate using the requested logical definition, then
  resolve to the shared physical identity before execution;
- source placement is by tenant/bucket projection partition rather than logical
  definition ID;
- catalog changes compile immutable path-prefix/content-type selector routes;
  source processing performs direct route lookup and shares exact payload
  projection results rather than scanning all logical definitions; and
- the disposable projection cache has bounded logarithmic recency maintenance.

This milestone is intended to prove that D640 equivalent definitions remain
close to D1 physical work. It deliberately does not claim component sharing
between different schemas or projection-preserving updates.

Qualification of server commit `f3dc43afa795` with harness commit
`f1aa37322283` established that intended bound. On the SSD host, saturated
accepted throughput remained 1,000.13 operations/s at D1 and 973.84 at D640.
On rotational storage it remained 90.49 operations/s at D1 and 114.71 at D640.
Both D640 runs completed with zero mutation/query/correctness errors. The source
projection summaries recorded one projection request and parse per source
mutation rather than per logical definition.

The first sustained keep-up gate also passed on rotational storage at D640: a
fixed 40 operations/s for 30 minutes accepted 71,973 of 71,973 offered
operations, publication visibility p99 was 26.69 seconds, concurrent-query p99
was 401.92 milliseconds, and the final exact drain took 80.36 seconds. This is
a bounded in-load visibility result, not a saturation result inferred from an
eventual drain. A corresponding 180 operations/s SSD cell accepted all 323,994
offered mutations and completed all 37,200 scheduled queries without an
in-load error, but failed the keep-up gate: the target generation remained
invisible after the full 1,800-second drain allowance. Its archive SHA-256 is
`d761a73f78ad4b70af3d5c8558c9dd60fac85f8f7f1fe3fca8cfc86a3d314aca`.
This is an overload bound, not a successful SSD service-rate result.

A later SSD D640 cell at server commit `a5f3284cbf45` established a bounded
100 operations/s service point: all 59,994 offered mutations and 12,800 queries
completed, visibility p99 was 4.26 seconds, concurrent-query p99 was 200.19
milliseconds, and exact correctness passed. The derived-progress race produced
zero false inventory rebuilds after conservative proof deferral. The cell still
wrote 92.43 GB and drained in 59.35 seconds, so it proves keep-up and recovery
stability at that rate, not acceptable physical write efficiency. Its archive
SHA-256 is
`cfa78e3035034e3201c1b7904cf4311ad44b3aaba0214d94af9d37c8c052d817`.

Catalog scale was qualified separately at server commit `ecab0a14b49f`. On the
8-core SSD host, 250,000 definitions were created in 333.24 seconds at 750.21
definitions/s. A clean restart recovered and listed all 250,000 definitions and
served three exact sampled reads in 203.83 seconds. The independent assignment
collector performed zero lag-triggered snapshots, its coalescing map peaked at
1,905 pending definitions, and server residency peaked at 1,551,940 KiB RSS and
91 threads during creation (568,628 KiB and 58 threads after restart). The
archive SHA-256 is
`a725cbb768a8f3e83d154bc67046adc85270a8611771d3d8772b77f4f8e072c6`.
This proves the sparse logical-catalog requirement on SSD; it is not evidence
that arbitrary distinct physical recipes have zero maintenance cost.

Server commit `1ce4fa290d23` subsequently repeated the rotational gate with two
alternating logical schema variants: reordered fields and different public
aliases bound the same recipes. All 71,973 offered operations were accepted at
40 operations/s; all 36,000 concurrent queries completed; there were zero
mutation, query, timeout, scheduling, or correctness errors. Publication
visibility was p50 3.50 seconds, p95 5.81 seconds, p99 10.32 seconds, maximum
11.57 seconds; concurrent-query p99 was 346.62 milliseconds and exact drain was
107.42 seconds. The evidence archive SHA-256 is
`03cb408de54a2f1d9bcc848daa56ad93acf34c391eb13147480cf82707e29c3b`.

### 18.2 Milestone B: recipe catalog and component generations

Replace complete-schema physical ownership with canonical membership and field
recipe records. A projection-family generation references independently reusable
membership, field, document-head, and liveness components. Logical bindings map
public field IDs and names to those physical recipes. Qualification must include
different public names, field subsets, and incompatible-recipe isolation.

Implemented groundwork on `feat/shared-index-projection`:

- tenant/bucket-scoped membership and field recipe identities cover selectors,
  value semantics, capabilities, formats, and component codec versions;
- runtime catalog plans reference-count those recipes transactionally and emit
  bounded aggregate telemetry; and
- native field IDs are canonicalized by recipe identity while public names are
  excluded from the segment schema fingerprint. Complete schemas which differ
  only by field declaration order or public aliases now share the same actual
  projection, segments, and query reader; query compilation and response labels
  still use the authorized logical names;
- format-v5 projected-state and generation records use integrity-checked,
  bounded codecs; and
- component roots are represented by a canonical bounded-fanout Merkle
  directory rather than an unbounded manifest or one file per component. A
  70,000-component regression keeps every encoded directory page below 32 KiB;
  and
- each component root can now name an ordered immutable delta stream through a
  second bounded-fanout content-addressed directory. Readers verify every
  segment identity and resolve newest-by-stable-key values; compaction folds
  the stream into one exact self-contained segment while retaining tombstones
  required by concurrently pinned older generations. A 70,000-segment
  regression keeps directory pages below 32 KiB; and
- component-stream append is a persistent path-copy operation rather than a
  whole-directory rebuild. The generation retains only a small root identity;
  that identity includes the stream root hash, segment count, delta bytes,
  logical bytes, and directory bytes, so it can be reopened exactly without
  loading or guessing the full directory;
  immutable directory pages are loaded by hash and one append publishes only
  the rightmost leaf plus its `O(log_256 segments)` parent path. A 65,536 to
  65,537 segment boundary regression rewrites exactly three pages while
  preserving an exact complete decode; unreachable proof pages are rejected;
  and
- changed deltas are integrated into shared payload packs at the existing
  16 MiB physical bound. Stream descriptors bind pack hash, exact byte range,
  segment hash, record count, and byte accounting, so many tiny recipe changes
  do not become one payload object or filesystem inode each; and
- one storage-neutral publication preparation produces the complete set of
  delta packs, newly path-copied stream pages, component-directory pages, and
  exact next generation. Runtime publication must make those immutable values
  durable first and atomically install only the generation record last.

This does not yet satisfy Milestone B for field subsets. Independent component
generation ownership and bindings remain required before different complete
schema sets may share only their compatible fields.

### 18.3 Milestone C: projected-document state

Persist the disposable exact projected state and stable document indirection
described in section 7. The writer compares canonical prior/current state and
emits no component mutation for an unindexed change, or only the changed recipe
subset for a material change. This is the Keldra HOT-equivalent milestone.

The native format-v5 writer core is implemented. One byte-accounted
source-partition buffer admits a complete mutation across every dirty physical
recipe or leaves the buffer unchanged, coalesces repeated stable-document-key
updates, and seals canonical integrity-checked delta segments. Focused tests
prove that an unindexed update writes only the disposable exact projected state
and document head—no membership or query field—and that changing one indexed
field adds only that field recipe. Durable generation publication and
production query resolution still have to consume these records before
Milestone C is complete.

### 18.4 Milestone D: lifecycle and scale completion

Complete last-reference physical GC, incremental recipe backfill, bounded
catalog paging/restart, churn/crash recovery, and every scale gate in section
15. Only this milestone changes this RFC's status from implementation in
progress to implemented.
