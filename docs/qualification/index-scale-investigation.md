# Index scale investigation

Status: evidence-backed design investigation; replacement architecture approved
by KELDRA-0020 and implementation in progress.

This report explains why Keldra's current index runtime does not scale from one
to many simultaneously affected logical definitions, and defines the minimum
architecture and qualification needed for a tenfold scale increase. It is not
a claim that increasing a lane, lease, timeout, memory, or batch-size constant
will solve the problem.

## Scope and target

The investigation separates two scale axes which the current D1--D64 workload
previously conflated:

1. **catalog scale**: at least 250,000 live logical definitions on one node;
2. **active fan-out**: at least 640 logical definitions affected by one hot
   source stream while ingestion and queries continue.

A large catalog must not create one resident task, source cursor, manifest
publisher, or physical copy of equivalent index data per definition. Active
work necessarily grows with the number of distinct physical field recipes
which an accepted source mutation changes, but must not grow with aliases or
logically equivalent definitions.

The PostgreSQL comparison is useful when interpreted on those two axes.
PostgreSQL stores index definitions in catalogs and does not execute one
background journal consumer per catalog row. It can also avoid index updates
when an update does not change indexed columns through HOT. PostgreSQL does not
make maintenance of 250,000 genuinely distinct indexes on the same changed row
free; Keldra nevertheless needs the same separation between cheap catalog
cardinality and actual changed physical index work.

Relevant PostgreSQL contracts:

- <https://www.postgresql.org/docs/current/catalog-pg-index.html>
- <https://www.postgresql.org/docs/17/storage-hot.html>
- <https://www.postgresql.org/docs/current/indexes.html>

## Workload

The diagnostic workload creates independent public logical definitions on the
same tenant, bucket, path prefix, and content type. Every definition has the
same three exact fields:

- `/record_id` as an unsigned integer;
- `/class` as a keyword;
- `/generation` as an unsigned integer.

This is deliberately the most shareable case. Additional definition IDs add no
new physical projection recipe. An architecture whose physical work is keyed
by semantic requirements should process D64 approximately like D1, plus bounded
logical-view metadata and query routing.

The evidence below used the same x86-64 SSD host, fresh state for each cell,
four saturated mutation workers, batches of 32 data mutations plus the
visibility marker, a 20-second concurrent phase, and targeted DEBUG telemetry.
Debug results are diagnostic and are not compared directly with production-log
qualification results.

The server behavior was commit `fd87484511af25c005e4c802adfcfd06d74d7d95`.
The immutable result archives and their SHA-256 sidecars are retained in the
qualification evidence directory:

- D1 result archive, SHA-256
  `5171ab85c19c68f0d209fc52b98edbe5f5311f51341a248e1f3473541a419960`;
- D16 result archive, SHA-256
  `1e3fb5f9703a834f7d60638da8502e3e7856aa08e0ea72c3f36a2ca2829bb160`;
- D64 result archive, SHA-256
  `f729a04c83a539d3aada985d6f36079d9d8db6e2c655c397d764cf225e518685`.

| Metric | D1 | D16 | D64 |
| --- | ---: | ---: | ---: |
| Accepted source operations | 28,743 | 25,014 | 20,064 |
| Accepted operations/s | 1,369.7 | 1,162.8 | 920.9 |
| Visibility p99 | 3.11 s | 6.96 s | 50.36 s |
| Final drain | 15.77 s | 28.29 s | 101.33 s |
| Average server CPU | 0.89 cores | 2.19 cores | 2.89 cores |
| Process writes | 260.7 MiB | 1,051.6 MiB | 2,628.6 MiB |
| Incremental immutable items | 328 | 4,499 | 13,654 |
| Incremental immutable bytes/source op | 236 B | 3,579 B | 13,506 B |
| Manifest publications/source op | 0.00070 | 0.00868 | 0.02906 |
| Cache misses/materialisations | 421 | 4,314 | 13,199 |
| Aggregate catch-up records/s | 1,199.9 | 1,137.5 | 204.4 |
| Blob-reference commit wait/source op | 0.004 ms | 0.617 ms | 15.579 ms |
| Blob-reference commit hold/source op | 0.014 ms | 0.182 ms | 0.823 ms |

The definition expansion visible in the same runs was:

| Metric | D1 | D16 | D64 |
| --- | ---: | ---: | ---: |
| Catch-up records processed | 29,075 | 463,234 | 884,197 |
| Aggregate catch-up wall time | 24.2 s | 407.2 s | 4,325.4 s |
| Shared-projection requests in the last complete summary | 16,384 | 393,216 | 1,294,336 |
| Minimum definition inspections implied by that summary | 16,384 | 6.29 million | 82.84 million |

The D64 catch-up count is not the full `64 x source operations`: the workload
stopped accepting input and the runtime then spent 101 seconds draining an
already divergent set of builders. The incomplete expansion is itself the lag
symptom, not evidence that only a bounded subset of definitions was affected.

The host was not CPU-saturated at D64. The dominant symptom is that derived
work creates many independent futures which queue behind serialized durable
publication and reference updates. Aggregate lock wait can exceed wall time
because many definitions wait concurrently.

### Symbolized sampled profile

A second D64 diagnostic used the same server behavior with an x86-64 release
binary built with frame pointers and full DWARF. Samply recorded the late
concurrent phase and drain at 199 Hz, with context-switch markers and a
presymbolicated sidecar. The profile covers a deliberately profiled run and is
not used for throughput comparison. Evidence:

- `profile.json.gz`, SHA-256
  `6629b8a4894b09ecf8cff7407f5dfd8cbc942c73923e08b5a7273beae3539b81`;
- `profile.json.syms.json`, SHA-256
  `aa2275dca45ac26760317f4ec07c3c4c81511a36d8fb686f11fb17a6d70e3d68`;
- profiled D64 result archive, SHA-256
  `518462f8e2e034c1cc55218d31c50103276ad48b89f4f073a865e204ecedb70f`.

Inclusive sampled stacks overlap and must not be added together. The important
attribution was:

| Inclusive stack | Samples |
| --- | ---: |
| Definition builder catch-up | 69.8% |
| Per-definition source projection/application inside catch-up | 66.8--67.3% |
| Existing artifact component reads | 36.6% |
| Stable exact object snapshot path | 28.2% |
| Shared projection `plan` and its definition mutex | 3.7% |
| Segment freeze/seal | 1.8% |
| Manifest publication | 1.5% |

Locator validation, encode, decode and lookup together were also prominent
self CPU, as were allocation/free and system-call overhead. Publication has a
smaller on-CPU percentage because the expensive symptom is waiting on the Store
commit authority; the independent owner-tagged wait/hold telemetry captures
that off-CPU queue.

This profile changes the priority, not the architectural conclusion. Removing
the quadratic plan is necessary before increasing active definitions, but it
cannot make D640 viable. The dominant sampled work is each logical definition
reading its own prior artifacts, resolving source state, updating locators,
sealing segments and publishing its own generation. The first structural
milestone must therefore centralize physical catch-up for equivalent views,
not stop after replacing the planner data structure.

## Findings

### 1. The shared projection cache performs quadratic planning

`SharedProjectionMapper::project` calls `plan` before it checks the prepared
output cache. `plan` takes one process-wide synchronous mutex and scans every
currently registered definition, filters matching definitions, reconstructs a
selector set, and hashes that set.

For `N` equivalent definitions processing one source mutation:

- the source mutation enters `project` `N` times;
- every call scans the same `N` definitions;
- prepared cache hits occur only after that scan.

The resulting planning work is `O(N^2)` per source mutation and the global
mutex serializes it. The last periodic D64 summary alone recorded 1,294,336
projection requests and 1,273,951 prepared hits. Since 64 definitions were
registered, those requests imply at least 82.8 million definition inspections.
The cache saved JSON parsing, but it was placed behind the quadratic work.
Even a prepared hit then deep-clones the definition-local `MergeMutation` into
the requesting builder, so a hit is not a shared posting or segment update.

The mapper registry contains only active builder leases, which currently caps
the scan at 64. Raising the lease limit to 640 without replacing the planner
would increase this part of the work by roughly another factor of 100.

The projection cache has another cliff after it fills. Eviction finds the
oldest entry by scanning the complete `HashMap`, and repeats that scan for every
entry which must be removed. The default 256 MiB reservation gives half to the
cache, so small projected records can accumulate tens of thousands of entries
before a sustained workload reaches this `O(cache entries)` eviction path.
Replacing the planner must also replace this pseudo-LRU with a bounded
constant/logarithmic-time recency structure.

### 2. One aggregate demultiplexer wakes N independent source consumers

The derived-consumer runtime already scans the source interval once to discover
which bucket and definitions are affected. It then loops over every definition,
loads definition/current evidence, and wakes the definition when its published
barrier is behind.

After that shared wake step, every `BuilderJob` independently calls
`journal.next_page` over the same source interval. Each builder independently:

- reads exact object versions;
- performs locator lookups and invalidation bookkeeping;
- clones or constructs projected mutations;
- builds segment components;
- publishes artifact packs;
- publishes a manifest and current pointer;
- publishes its own derived checkpoint evidence.

The demultiplexer shares notification, not indexing work.

### 3. Equivalent logical definitions cannot share physical artifacts

Every segment identity embeds `index_id`, `definition_version`, schema
fingerprint, and a newly allocated segment ID. Component envelopes encode that
identity, and artifact paths are beneath the logical index ID. Equivalent
definitions therefore produce different component bytes, hashes, object paths,
manifests, references, and cache entries.

In the D64 trace, one 20-second workload produced 13,654 incremental immutable
items and 583 manifest publications. `blob_reference` acquired the Store commit
lock 14,531 times. Its aggregate wait was 312.6 seconds while aggregate hold
time was only 16.5 seconds. D1 incurred 468 acquisitions, 0.117 seconds of wait,
and 0.411 seconds of hold.

The other largest D64 owners show the same queueing shape: `bulk_mutation`
waited 35.5 seconds and held 6.2 seconds; derived membership, watch-journal and
checkpoint operations together waited 48.9 seconds while holding less than
0.5 seconds. These durations overlap across tasks, so they are not wall-clock
components to add. They prove that the derived fan-out continuously creates a
queue around short serialized commits, including the ordinary ingestion calls
which are supposed to remain independent of asynchronous index freshness.

This is why publication batching helped but did not fix scaling: batching can
reduce Store calls, but it cannot remove logically unnecessary physical data.

### 4. The 64-definition point is a production lease boundary

`MAX_ACTIVE_BUILDERS` is 64. The scheduler retains at most 64 definitions and
defers later assignments to rediscovery. Idle definitions release their lease,
so more than 64 definitions can eventually rotate through the runtime, but 640
simultaneously lagging definitions cannot be resident together. Under sustained
input, rotation adds a scheduling interval on top of duplicated catch-up work.

The process-local catalog handoff is separately capped at 1,024 pending changes.
Those are safety bounds, not a scalable catalog representation. Merely raising
them would expose more quadratic planning and publication contention.

### 5. Query materialisation multiplies filesystem objects

The authoritative artifact pack is copied into one disposable cache file and
memory-mapped. D64 recorded 13,199 cache misses in the diagnostic run; a sampled
profile observed more than 10,000 distinct cache paths. The cache charges at
least 4 KiB per file, which bounds retained files by configured cache bytes, but
high logical fan-out still causes inode churn, copy I/O, reconciliation work,
and cold-query materialisation.

This must not be solved by allowing millions of small cache files. Shared,
well-filled physical packs should reduce object count first. The query path
should then consume authoritative integrated payload ranges through bounded
memory/RocksDB block caching rather than copying every pack into a separate
filesystem cache file.

## Required architecture

### Source-driven physical indexing

```text
current
  one source journal window
       -> definition 1 builder -> segments -> manifest/current/checkpoint
       -> definition 2 builder -> segments -> manifest/current/checkpoint
       -> ...
       -> definition N builder -> segments -> manifest/current/checkpoint

replacement
  one source journal window
       -> compiled source/recipe router
       -> one physical projection update per changed recipe
       -> one physical generation
       -> N lightweight logical views
```

Replace definition-driven builders with one source-driven writer per assigned
tenant/bucket projection group:

1. read each source-journal page once;
2. resolve each exact object version once;
3. match the source through a compiled prefix/content-type routing structure;
4. parse each payload once for the union of required selectors;
5. compare the canonical projected values with the prior physical row and
   produce a recipe change mask;
6. update each changed distinct physical field recipe once;
7. assemble well-filled immutable packs once;
8. publish one complete physical generation/barrier;
9. make logical definitions lightweight views over that generation.

The unit of scheduled work becomes a changed source window and a set of unique
physical recipes, not one task per logical definition.

The change mask is the equivalent of avoiding an unnecessary PostgreSQL index
update when indexed values did not change. Keldra currently treats every source
revision as work for every matching definition. The replacement must preserve
enough per-physical-row projection identity to distinguish membership/live-state
changes from unchanged field values; an update to an unindexed JSON property
must not rewrite postings, points, facets or order data.

### Compiled physical recipe catalog

A field recipe is keyed by every byte-affecting semantic input, including:

- tenant and bucket physical ownership;
- source selector;
- scalar type and cardinality;
- analyzer/normalization/date format;
- exact, range, prefix, text, facet, order, aggregate, vector, and scoring
  capabilities;
- codec and result-affecting format versions.

Definition changes update a persistent recipe-reference catalog and an
immutable process-local routing snapshot. Source projection performs a prefix
trie/content-type lookup and direct recipe lookup. It never scans all
definitions and does not take a global mutable catalog lock per source.

Definitions with the same complete schema and source scope can share a whole
physical view. Definitions with different field subsets can share row identity,
live-state, locator, and compatible field components. Definition-specific
physical ordering or incompatible analyzers create additional recipes, not a
copy of every other component.

### Logical views, barriers, and GC

A logical definition owns its public name, authorization, public field names,
query capability contract, definition version, and mapping to physical recipe
IDs. It does not own duplicated component bytes.

One physical generation carries the complete source barrier and atomic
watermark. A logical view becomes visible by atomically binding its exact
definition version to one ready physical generation. Definition deletion or
replacement changes reference ownership; immutable physical packs are GC
eligible only after no retained logical view or in-flight query references
them.

Physical sharing is limited to the same tenant/bucket authority unless a later
approved security design proves a broader boundary. Query authorization still
checks the logical definition and returned source objects; shared bytes do not
grant access.

### Sparse catalog residency

The runtime must support 250,000 definitions without 250,000 tasks or cursors.
Durable definition and recipe-reference records remain in RocksDB. Memory holds
compact immutable routing/catalog pages and only bounded active source windows.
Point lookup by definition ID and incremental catalog changes must not perform
a whole-catalog scan. Restart work is paged and may rebuild disposable routing
snapshots without delaying ordinary object service.

The intended scaling variables are:

| Operation/resource | Current runtime | Replacement target |
| --- | --- | --- |
| Catalog definitions | resident builder/task pressure | paged logical view records |
| Route one source change | scan/replan per active definition | one compiled source lookup |
| Parse one exact payload | once on a warm cache, otherwise per definition | once per changed source version |
| Physical update | once per logical definition | once per distinct changed recipe |
| Barrier/checkpoint | once per logical definition | once per physical generation |
| Equivalent definition bytes | proportional to definitions | one physical view plus logical bindings |
| Query definition lookup | definition-local physical tree | direct logical-view-to-generation lookup |

If `D` is catalog definitions, `M` is definitions whose source predicate
matches one mutation, and `R` is distinct changed physical recipes among those
matches, source work must be `O(route lookup + payload bytes + R)`. It must not
be `O(D)`, `O(M^2)`, or `O(M)` when all `M` definitions are aliases of one
physical view.

## Rejected partial fixes

- Raising `MAX_ACTIVE_BUILDERS`: exposes quadratic planner work and increases
  publication queue depth.
- Increasing projection or publication lanes: adds contenders to the same
  Store commit authority and previously showed destructive concurrency.
- Lengthening flush/cohort windows: may reduce call count but increases
  freshness and retains N copies of the data.
- Caching only JSON parsing: already implemented; it leaves source scans,
  planning, exact reads, locators, segments, manifests, references, and cache
  files duplicated.
- Treating a larger timeout as throughput: changes only how long callers wait.

## Delivery stages

1. **Remove quadratic planning.** Build immutable, bucket-scoped routing and
   recipe snapshots on catalog changes. Put cache lookup before any catalog
   expansion, and replace whole-map pseudo-LRU eviction. Add an inspection
   counter regression proving projection work is independent of unrelated
   catalog size.
2. **Introduce physical projection groups.** Centralize source page and exact
   version reads. For identical definitions, build and publish one physical
   segment/generation and attach multiple logical views.
3. **Component-level sharing.** Split logical index ownership from physical row
   identity and field recipes so heterogeneous definitions reuse compatible
   components.
4. **Remove file-per-pack query caching.** Read bounded ranges from integrated
   payload storage into memory/block cache, preserving integrity checks and
   query memory admission.
5. **Replace active-definition scheduling.** Schedule bounded source windows
   and recipe work; make the definition catalog sparse and independently
   scalable.

Stages 2 and 3 change persistent index formats and physical ownership. Their
authority, recovery rules, projection-preserving update semantics, and complete
qualification gates are approved in KELDRA-0020. Stage 1 remains useful but
cannot by itself meet the scale target.

## Qualification gates

The replacement is not complete until all of these pass on SSD and rotational
storage with fresh volumes:

1. **Catalog-250K:** create 250,000 definitions, restart, point-inspect and list
   them, then mutate an object which matches only a bounded subset. Startup,
   mutation work, and resident tasks must not scale with unrelated definitions.
2. **Equivalent D640:** 640 identical definitions on one hot bucket. Physical
   artifact items/bytes and Store reference mutations must remain close to D1,
   not grow 640 times.
3. **Heterogeneous recipes:** vary field subsets, types, analyzers, source
   prefixes, facets, ordering, and dates. Work must track distinct matched
   recipes and emitted postings, with exact query parity for every definition.
4. **Sustained keep-up:** run a fixed offered rate for at least 30 minutes.
   Published lag must reach a stationary bound during ingestion; final drain
   alone is not evidence of keeping pace.
5. **Definition churn:** concurrently create, replace, and delete logical views;
   prove exact generation binding, reference accounting, and eventual orphan
   reclamation.
6. **Crash/restart:** interrupt source projection, physical publication, logical
   view binding, and GC independently; prove no partial visibility or leaked
   authoritative packs.
7. **Resource proof:** record CPU/on-CPU stacks, async/off-CPU waits, Store-lock
   owner wait/hold, RocksDB writes, physical item/byte counts, memory, cache
   objects, and journal lag per phase.

The primary success metric is physical work per accepted source mutation per
distinct matched recipe. Logical definition count is a control-plane dimension,
not permission to repeat equivalent physical indexing work.

## First replacement-architecture evidence

Server commit `f3dc43afa795` and bounded harness commit `f1aa37322283` were run
from fresh volumes with the same x86-64 candidate on the 8-core SSD and 16-core
rotational hosts.

Equivalent-definition saturation no longer exhibits definition-linear work:

| Host | D1 accepted ops/s | D640 accepted ops/s | D640 query p99 | D640 result |
| --- | ---: | ---: | ---: | --- |
| SSD | 1,000.13 | 973.84 | 351.74 ms | exact, zero errors |
| rotational | 90.49 | 114.71 | 350.98 ms | exact, zero errors |

The visibility gate failed in these saturation cells because the open-loop
offered rate exceeded the physical writer's sustained service rate. That is not
a definition-count scaling regression: periodic projection summaries showed
one physical request/parse stream at D640, comparable with D1.

The rotational D640 fixed-rate run then held 40 operations/s for 30 minutes. It
accepted all 71,973 operations with no mutation, query, timeout, scheduling, or
oracle error. In-load publication visibility was p50 5.43 seconds, p95 21.14
seconds, and p99 26.69 seconds; concurrent-query p99 was 401.92 milliseconds;
the exact final drain was 80.36 seconds. This passes the sustained keep-up gate
on the slower host. The SSD fixed-rate run and heterogeneous/HOT/churn gates
remain separate evidence requirements.

Server commit `1ce4fa290d23` then repeated that gate with 640 logical
definitions alternating between two declaration orders and two public field
name sets over the same physical recipes. It again accepted all 71,973 offered
operations at 40 operations/s and completed all 36,000 concurrent queries with
zero mutation, request, timeout, scheduling, or correctness error. Visibility
improved to p50 3.50 seconds, p95 5.81 seconds, p99 10.32 seconds, maximum
11.57 seconds; concurrent-query p99 was 346.62 milliseconds. Exact final drain
was 107.42 seconds. This proves alias/order-neutral sharing through the public
query contract, not merely identical serialized definitions. The archived
result SHA-256 is
`03cb408de54a2f1d9bcc848daa56ad93acf34c391eb13147480cf82707e29c3b`.

The corresponding SSD D640 run deliberately offered a much higher fixed rate:
180 operations/s for 30 minutes. It accepted all 323,994 operations and
completed all 37,200 scheduled queries with zero mutation, query, timeout, or
correctness errors during the offered-load phase. It did **not** pass keep-up:
after another 1,800 seconds the final canary still could not observe the target
generation and its query hit the 30-second request deadline. The terminal
report is therefore a failure, not an eventual-drain success. Its archive
SHA-256 is `d761a73f78ad4b70af3d5c8558c9dd60fac85f8f7f1fe3fca8cfc86a3d314aca`.
This bounds the current SSD service rate below 180 operations/s for this corpus
and D640 shape; a lower-rate sweep is required to locate the stationary limit.

The first Catalog-250K attempt also exposed a separate control-plane defect.
While definitions were being created, the derived consumer repeatedly lost its
bounded assignment notifications and discarded its entire reconstructed
inventory. The skipped batches grew 5, 57, 131, 357, 824, 1,854, then 3,305 as
each page-one restart took longer. That is direct superlinear recovery work,
not a host-capacity result. The diagnostic was stopped with its logs preserved.
The replacement uses an exact Store assignment snapshot plus a notification
receiver created under the same assignment lock; lag now replaces only the
disposable assignment inventory and preserves completed source/projection
progress. Catalog-250K must be rerun against that exact candidate before a pass
is claimed.
