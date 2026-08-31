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

The next SSD D640 cell offered 100 operations/s for ten minutes against server
commit `ecab0a14b49f`. It accepted all 59,994 operations, completed all 12,000
concurrent queries without a request, timeout, scheduling, or correctness
error, held concurrent-query p99 to 200.96 milliseconds, and observed every
one of 114 publication samples. Publication visibility was p50 2.28 seconds,
p95 3.64 seconds, p99 3.99 seconds, maximum 4.33 seconds; exact final drain was
33.32 seconds. The harness marked the cell `fail` only because its generic
workload-validity predicate required at least one query per definition during
each 30-second baseline/post phase, which can offer only 600 queries for 640
definitions at 20 queries/s. The deliberately paced producer queue being empty
is reported but is not a failure in fixed-rate mode. The responsiveness and
correctness subreports both passed. The evidence archive
SHA-256 is
`a41c3992cba38aded531b4f9c84afb6d89c2078c2bc1a63a53138980eeccc579`.

That run also exposed a distinct write-amplification defect rather than a
service-rate limit. It wrote 71.12 GB while accepting only 5.91 MB of request
payload and logged 142 `derived proof is beyond the settled source tail`
warnings, 116 inventory retries, and 26 full disposable-inventory rebuilds.
An asynchronous valid publication proof could arrive ahead of the derived
tracker's captured demultiplexing barrier. The tracker incorrectly classified
that ordering as corrupt evidence and rebuilt. The replacement defers an
ahead proof without trusting it, releasing an effect, or advancing retention;
the normal scan applies it only after the exact source barrier catches up. The
focused regression proves that a proof at offset 100 observed against a
settled tail of 5 leaves the effect and checkpoint at 1, then clears the effect
and advances to 100 only after the settled tail reaches 99. A same-shape
exact-candidate A/B at server commit `a5f3284cbf45` accepted all 59,994 offered
mutations at 100 operations/s, completed all 12,800 queries, and passed exact
final correctness with zero future-proof warnings, inventory retries, or
rebuild lines. Visibility was p50 2.44 seconds, p95 3.86 seconds, p99 4.26
seconds, maximum 4.30 seconds; concurrent-query p99 was 200.19 milliseconds.
The evidence archive SHA-256 is
`cfa78e3035034e3201c1b7904cf4311ad44b3aaba0214d94af9d37c8c052d817`.

That run does not support attributing the remaining write amplification to the
false rebuilds. It wrote 92.43 GB and drained in 59.35 seconds, versus 71.12 GB
and 33.32 seconds in the earlier same-rate cell. The fix is therefore a
correctness and recovery-stability result, not a storage-efficiency result.
The remaining amplifier is the ordinary format-v4 component, manifest,
reference, checkpoint, and compaction work which KELDRA-0020 replaces; a
higher SSD stationary rate is not claimed from this patch.

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

That first replacement was necessary but not sufficient. Candidate
`737c62e65f1f` reached 77,291 definitions at about 859 definitions/s on SSD,
but performed hundreds of full durable reconciliations in its first two
minutes. The HDD cell reached 1,908 definitions at about 21 definitions/s and
its skipped notification batches grew 4, 58, then 126. Both diagnostics were
stopped and preserved. Keeping projection progress removed the old restart
loop, but tying assignment receipt to long derived-consumer turns still made
catalog recovery work proportional to repeated catalog snapshots.

The second replacement gives assignment intake its own lightweight task. It
continuously drains the Store receiver and coalesces the newest committed
mutation per exact definition identity in a map bounded by catalog cardinality.
The derived runtime drains that map at its normal boundaries; only genuine lag
inside the collector invokes the exact durable snapshot. This candidate must
show zero collector-lag replacements during Catalog-250K before the control
plane is considered scalable.

Server commit `ecab0a14b49f` passed that Catalog-250K gate on the 8-core SSD
host. It created 250,000 definitions with concurrency 64 in 333.24 seconds
(750.21 definitions/s), then restarted the server from the same volume and
listed all 250,000 definitions plus three exact sampled reads in 203.83 seconds.
The assignment collector reported zero lag replacements. Its pending map
peaked at 1,905 entries and repeatedly returned to double digits or zero while
creation continued. The server peaked at 1,551,940 KiB RSS and 91 threads
during creation; after restart it peaked at 568,628 KiB and 58 threads. There
was no definition-proportional task or thread growth. The evidence archive
SHA-256 is
`a725cbb768a8f3e83d154bc67046adc85270a8611771d3d8772b77f4f8e072c6`.

This is a control-plane and recovery result. It proves that a large logical
catalog is cheap resident metadata; it does not claim that 250,000 distinct
recipes can all maintain changed physical fields for free. The rotational-host
Catalog-250K cell and the distinct-recipe/HOT gates remain in progress.

## Mixed field-subset bridge evidence and maintenance-admission defect

Server and harness commit `ed9041b68fd8` changed the production Typed JSON
bridge from complete-schema sharing to membership-family sharing. The bounded
qualification alternated two genuinely different logical field subsets: one
requested `record_id`, `class`, and `generation`; the other requested the same
physical `record_id` and `class` recipes under `document_id` and `category`
while omitting `generation`. Logical queries remained restricted to their own
declared fields.

On the 8-core SSD host, a fresh-volume 100 operations/s sweep produced:

| Definitions | Accepted | Accepted/s | Mutation p99 | Query p99 | Visibility p99 | Drain |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 11,979 | 99.93 | 97.54 ms | 143.36 ms | 3.03 s | 16.53 s |
| 256 | 11,979 | 99.94 | 82.88 ms | 141.82 ms | 3.01 s | 21.89 s |
| 640 | 11,979 | 99.97 | 72.26 ms | 127.10 ms | 4.20 s | 20.53 s |

Every cell completed 7,680 concurrent queries with zero mutation, query,
timeout, scheduling, or correctness error and exact final results for every
logical definition. After one zero-work transitional physical identity while
the recipe union changed, the server ran one steady family builder. The
definition-count sweep archive SHA-256 is
`343bd96d72f8926ecbada92b2abbd5fe34802af5e542ed21a6d58af5c40a3169`.
This proves that 640 mixed logical subsets do not create 640 physical writers;
it is not a 30-minute keep-up result.

The corresponding ten-minute D640 SSD cell accepted all 59,994 operations at
99.99 operations/s and completed 38,400 concurrent queries with zero errors,
timeouts, drops, or correctness failures. Mutation p99 was 127.81 ms and query
p99 was 892.93 ms. It nevertheless failed the publication-visibility gate:
95 of 96 samples remained in the ordinary 2--4 second range, but one sample
waited 95.29 seconds; exact drain was 149.75 seconds. The failure archive
SHA-256 is
`2b2e61b01ea24def8ef819526e6975a99259cc390738e8eb7827dd6e0ce72bfe`.

The server trace attributes that outlier to working-memory admission rather
than definition fan-out. Default index working memory was 2.75 GiB. Queries
reserved 512 MiB and the shared projection held 256 MiB, leaving 2 GiB of
background capacity. A four-lane Typed JSON compaction of four segments, 429
documents, and only 55,558 encoded input bytes admitted 256 MiB of actual
workspace but leased the complete 2 GiB remainder. Because that loan is
non-preemptible, a new source-writer turn could not obtain its mandatory
256 MiB. Its catch-up span emitted 30- and 60-second zero-progress heartbeats
and resumed only after 84.03 seconds when the merge released the oversized
lease.

The correction makes segment and locator compaction request exactly their
per-kind fair share. The default four lanes already fit that 256 MiB share, so
the observed merge loses no admitted lane while a later writer remains
admissible. A focused asynchronous regression holds the exact production
compaction permit and proves a second Typed JSON writer obtains its complete
mandatory turn without waiting for compaction to finish. A sustained exact-
candidate rerun on SSD and rotational storage is required before this defect is
closed; the failed cell remains part of the record.

Server commit `d25286ba0258` bounded every observed compaction lease to its
256 MiB Typed JSON share. The rotational host then accepted all 4,785 offered
operations at 40 operations/s with D640, returned exact results for all 640
definitions, held concurrent-query p99 to 328.70 milliseconds, observed
publication visibility p99/maximum of 23.30 seconds, and drained in 61.01
seconds. The archive SHA-256 is
`4329d9a294907b9e71a6175bd94bb8f43801f18cc5e290a026c31240ace30f8e`.
The same noisy host's D1 control had a worse 32.67-second visibility maximum,
so this is definition-scale evidence rather than a clean storage comparison.

The corresponding ten-minute SSD D640 run remained a failed keep-up cell. It
accepted all 59,994 offered operations at 99.99 operations/s, completed all
38,400 concurrent queries with zero errors, timeouts, drops, or correctness
failures, and held mutation p99 to 122.37 milliseconds. Visibility was p50
2.42 seconds and p95 5.19 seconds, but one of 97 samples reached 79.50 seconds;
drain was 114.16 seconds. Its archive SHA-256 is
`e368d1b6ba4e985e413d8c6aa17866c736889c593d33645afb46291e46bdb077`.
This improves the preceding outlier but does not pass the gate.

That trace exposed a separate cancellation-lifetime defect. Rapid catalog
creation repeatedly expands the membership family's field union and replaces
the representative builder. Catch-up and rebuild launched projection lanes as
detached Tokio tasks. Cancelling the obsolete builder therefore discarded its
result but not its child task. Before the SSD outlier, an active builder
processed one 436-record page and then spent 77.23 seconds without progress
behind obsolete work on the same per-source projection stripe. Another turn
lasted 87.19 seconds while making progress through the accumulated CPU queue.
The replacement makes projection lane tasks abort-on-owner-drop and makes
not-yet-started Rayon submissions cancellation-aware. Intermediate catalog
plans can no longer consume CPU or a source stripe after their only publisher
has been replaced. Focused tests prove a cancelled queued submission performs
zero work after the worker reaches its queue position. A new exact-candidate
SSD sustained cell is required to close this failure.

Server commit `492b96944b0a` closed the remaining task-ownership hole by
keeping an owned projection task attached while its result is joined. A
five-minute D640 cell on the rotational host accepted every offered mutation
at 39.97 operations/s, returned exact final results for all 640 definitions,
held concurrent-query p99 to 486 milliseconds, observed visibility maximum
6.28 seconds, and drained in 25.85 seconds. Its archive SHA-256 is
`a0acebe23c06787b3ff0cb8d94ce39a995e9e90aca85e9871f530f8b0cbfa241`.

The corresponding ten-minute SSD cell remained correctness-clean but exposed
one residual tail: all 59,994 mutations were accepted at 99.99 operations/s,
all 38,400 concurrent queries completed without error, query p99 was 737.79
milliseconds, and all 640 definitions were exact after drain. Visibility was
p50 2.48 seconds and p95 4.69 seconds, but one sample reached 94.77 seconds;
drain was 157.13 seconds. Its archive SHA-256 is
`6eaa7a5d7b60c4d8f92a36220619f34fc950df8fe0210f5996d3fbbb7f148467`.

Commit `ae77005fa6c6` added explicit source-read, projection/application,
segment-seal, pack-stage, and component-publication phase timing. Its exact
ten-minute SSD rerun again accepted all 59,994 mutations at 99.99 operations/s
and returned exact results for all 640 definitions. Concurrent-query p99 was
982.53 milliseconds with zero request, timeout, or correctness errors; 44 of
38,400 open-loop query offers were deliberately dropped by the bounded client
rather than queued. Of 98 visibility samples, 97 stayed in the normal band
(p50 2.42 seconds, p95 4.46 seconds) and one reached 89.72 seconds; drain was
53.17 seconds. Its archive SHA-256 is
`72b7660b39ad0066ff78f2393f29dce103188c74a3c3e0babb8387d7c923771d`.

That phase trace rules out exact source reads, shared projection parsing,
logical assembly, segment sealing, immutable pack staging, and component
publication as the long operation: none reported a slow completion during the
zero-progress interval. Process accounting initially appeared to show 9.53 GB
of writes during the 90-second interval, but live `/proc/<pid>/io` attribution
proved that almost all such bytes are `cancelled_write_bytes` from disposable
format-v4 merge scratch files. Host block I/O remained low and RocksDB recorded
no write stall; its flushes and compactions were sub-second and only tens of
megabytes. Conversely `rchar` advanced by hundreds of megabytes per second
with zero physical read bytes, demonstrating repeated page-cache reads of
existing v4 artifacts. Open descriptors during the reproduction continuously
named `.merge-*.tmp` files beneath the bounded scratch directory.

This establishes a separate physical-amplification defect: the storage device
was not servicing 9.53 GB of durable writes, but the process still spent CPU,
memory bandwidth, page-cache capacity, and allocator work repeatedly reading
and constructing disposable v4 merge state. A scheduler or timeout adjustment
cannot remove that work. The coherent correction remains the format-v5
delta/component generation in KELDRA-0020: stable document keys, exact
projected-state comparison, changed-recipe-only appends, and component-local
compaction. The v4 bridge remains useful evidence that logical catalog
cardinality and equivalent definitions can share one writer, but it cannot
establish the final physical write-amplification gate.

A second targeted run enabled only the builder/publication DEBUG spans needed
to place the discrete visibility outlier. It accepted all 59,994 mutations at
99.98 operations/s, returned exact final results for all 640 definitions, and
completed 38,365 of 38,400 open-loop query offers with no request, timeout, or
correctness error. Query p99 was 970.24 milliseconds. Visibility was p50 2.41
seconds and p95 4.96 seconds, but one sample reached 74.51 seconds; exact drain
was 51.55 seconds. The archive SHA-256 is
`12fbaae88f901bbcb9f69ed52db64c7af98d3f56a3f3d8e72843691bd6b7051d`.

The publication completion appeared 78.056 seconds before the logical catch-up
progress span ended. An initial interpretation blamed completion polling
through the global builder delayed queue. Candidate `931f46e6728d` changed an
otherwise-idle builder to join its owned publication directly and repeated the
exact ten-minute cell. That experiment falsified the interpretation: it again
accepted all 59,994 mutations at 99.99 operations/s, completed all 40,320
queries with zero errors, and returned exact results for all 640 definitions,
but one of 94 visibility samples still reached 76.35 seconds. Query p99 was
719.87 milliseconds and drain was 52.37 seconds. The archive SHA-256 is
`cc518777878e7f295f7687a13c8d315f42c584bda23e9b6263e3fcb493e7222a`.
The speculative join change was consequently removed rather than retained as
an unproven scheduler workaround.

The corrected reading is that one `BuilderProgress` object spans multiple
admitted scheduler turns. Publication revision 369 completed early, after
which the same catch-up state entered v4 mandatory maintenance before it could
advance or close the span. The exact comparison run stayed in ordinary
one-to-two-second, approximately 100-record turns during sustained ingestion.
At the end of the run it hit the v4 segment/locator bound: one maintenance turn
took 30.000 seconds while processing 344 records, the following turn consumed
the resulting 10,349-record backlog in 23.984 seconds, and a final zero-record
maintenance turn took 19.462 seconds. Those three turns account for the
visibility tail. This is why publication could be fast and the encompassing
catch-up span could still remain open for tens of seconds.

The discrete failure and the physical-amplification evidence therefore have
one architectural root: v4 eventually must read and rewrite accumulated
segment/locator state before source intake can proceed. Avoiding normal merge
debt on the journal critical path cannot avoid a hard segment/locator bound.
The final correction is not another timeout, lane, or wakeup rule; it is the v5
stable-key, changed-component-only stream and bounded path-copy generation
specified by KELDRA-0020.
