# Anvil known limitations

## Anvil 0.8 operational boundaries

Anvil 0.8 uses index format 3 exclusively and starts with new volumes. Index
definitions build fresh packed generations from authoritative ordinary source
objects; there is no format-2 decoder, converter, dual writer, or fallback.
This clean boundary keeps the serving and recovery paths small and predictable.

| Area | Current boundary |
| --- | --- |
| Vector search | Exact search is linear in the selected vector scope; an ANN format is deferred. |
| Typed/Metadata conjunctions | Every predicate is enforced and each individual equality, `IN`, prefix, range, or `EXISTS` lookup uses bounded posting ranges. A conjunction currently chooses one bounded posting lookup as its driver and checks the remaining predicates against those projected candidates instead of intersecting several compressed posting streams. Results and memory bounds are unchanged, but a poorly selective driver can read more candidates. Adding a selective equality or prefix predicate narrows that work; range-local multi-stream intersection is deferred to a later index format. |
| Query placement | One weighted-HRW owner executes a query. Large indexes may fetch ordinary pack objects, but there is no scatter/gather query engine. |
| Oversized source values | One selected payload and its fixed projection workspace must fit that index kind's configured construction budget. Other definitions and object operations remain available. |
| Failed definitions | A definition which cannot advance can eventually apply write backpressure rather than allow accepted objects to disappear from future indexed results. Repair, rebuild, or delete the authorized definition. |
| Runtime tuning | Journal, receipt, builder, compaction, query, cache, and retention limits are startup settings and change on restart. |
| Pack range reads | A cold logical-block read may fetch its complete ordinary pack where the underlying object path cannot provide an efficient range; packs have a fixed 16 MiB target. |
| Build scratch | Secondary-key external sort uses disposable bounded local scratch. A crash repeats the affected scoped build. |
| Traffic accounting | Inbound and outbound byte accounting is bounded best-effort telemetry. Stored bytes and object counts remain exact at their reported complete checkpoint. |
| Distribution | 0.8 is a single-cluster release; mesh and region coordination remain future capabilities. |

These limits do not weaken object durability, Zanzibar authorization, CAS,
atomic-program visibility, source-complete index publication, or journal
evidence retention. Operational pressure is surfaced through metrics, traces,
logs, and pre-mutation backpressure.

## Sparse index coordination in 0.7.0

Anvil 0.7.0 supersedes the 0.5.2 and 0.6.x cold definition-discovery
limitation. Startup and recovery read bounded pages from the transactional
definition locator and this node's sparse assignment state; they do not scan
ordinary object heads, the stored corpus, or every bucket. Normal serving
startup is therefore independent of tenant, bucket, object, and dormant-index
population. The ordinary definition object remains authoritative and each
selected locator is exact-read and revalidated before work starts.

Stored-byte and object-count accounting is derived from authoritative object
state and remains exact at each reported complete checkpoint. Inbound and
outbound byte totals are bounded usage telemetry rather than a financial
ledger. Each ingress node buffers small idempotent batches and sends them to
the weighted-HRW matcher for the affected bucket. A process loss before
acknowledgement, bounded-queue exhaustion, or the short propagation interval
after an accounting definition or matcher changes can omit traffic bytes.
Anvil reports dropped batches and bytes; supported-load qualification requires
both to remain zero. These windows do not affect stored objects, stored-byte
totals, object counts, authorization, references, or index correctness.

Index-retention traversal cursors are resumable across bounded scheduler ticks
but are not persisted across a process restart in 0.7.0. After restart, each
scheduled definition begins retention again from its current published
generation and the start of its reserved artifact prefix. Serving and startup
remain O(1), each maintenance tick remains bounded by its configured record,
byte, and time budget, and exact-version deletion is idempotent. Repeating
already-inspected maintenance work can delay reclamation; it cannot remove a
live object or change index results.

The credit-driven source snapshot used for a scoped index baseline is an
ephemeral peer stream, not a durable server-side lease. A terminal transport
error therefore restarts that one definition's tenant/bucket/path-prefix
baseline from its beginning. It never widens into an unrelated-object scan or
delays core serving, and the preceding complete generation remains current.
Catch-up and publication state use retryable cursors and are retained across
transient failures; only the in-progress scoped baseline work is repeated.

Catch-up source pages containing no changes for the selected bucket consume no
byte credit. If many ACTIVE source journals advance without any route for that
bucket, one builder turn can therefore retain its per-kind lease while it walks
those empty pages; fairness in that case is proportional to ACTIVE source count
rather than the byte quantum. Memory, checkpoints, publication, and index
results remain bounded and correct. Operators can avoid an extreme startup
burst by staging very large membership changes; a separate record-count credit
is deferred until production evidence justifies it.

## Streaming indexes in 0.6.0

Index format 2 is a clean break. Anvil 0.6 does not read, migrate, dual-write or
fall back to 0.5 index definitions, artifacts, cache entries or page tokens.
Authoritative source objects are unaffected; recreate each definition to build
its new index.

The first succinct engines deliberately keep their existing minimum query
surface. Full text has one Unicode-aware lowercase tokenizer but no stemming,
fuzzy matching, synonyms or configurable filters. Vector search remains an
exact scan, so its cost is linear in the selected vectors. Hybrid evaluates its
full-text and vector candidates before deterministic fusion. Git Source and
Tensor remain manifest projections rather than general Git or model-serving
engines. These are query-feature and performance limits, not weaker
authorization or visibility semantics.

In the historical 0.6 implementation, rebuilding a node's disposable
assignment cache scanned the reserved definition prefix. Anvil 0.7 replaces
that cold path with the sparse locator described above. Query materialization
remains disposable and cold queries may fetch index blocks before executing. A
query always includes freshness evidence and continues to serve the preceding
complete generation while its writer builds the next one.

One source mutation whose selected payload and required seal workspace cannot
fit the configured per-kind construction budget cannot be indexed. The writer
keeps the preceding complete generation and retries after configuration or
source data changes; other object and index kinds remain available.

Format-2 encoded data and routing blocks are capped at 512 KiB and routing
fanout is 32. A decoder may use approximately 4 MiB in aggregate for the owned
representation of one block. Full-text position lists are split across blocks,
but one Typed JSON scalar projection or one Git Source/Tensor record group must
fit one block in 0.6.0. The writer rejects an oversized row before it enters a
mutable run, does not advance the source checkpoint, and keeps serving the
preceding complete generation. Splitting that application record across
ordinary source objects is the current workaround.

Each level contains at most four runs. A fifth run triggers compaction of the
four oldest inputs into one run at the next level. Anvil 0.7 divides every
index kind's merge into deterministic, non-overlapping key ranges and prepares
up to four ranges concurrently by default. The actual lane count is capped by
that kind's configured lane limit, the process Rayon workers, the ranges
available in that component, and its shared construction-memory pool. A narrow
key space can therefore use fewer lanes, and a one-lane configuration preserves
the sequential path. Each lane owns a bounded writer and stages immutable
range-local blocks concurrently through the ordinary object path. Components
that need globally dense document ordinals first count each range, derive dense
bases with an ordered prefix sum, and then scan the ranges again while writing;
those components therefore pay a bounded two-pass input-read cost.

The coordinator validates completed range summaries and assembles their
subtrees in key order before the one complete-generation publication CAS. A
fixed range plan has deterministic ordered output, but independently sealed
ranges may have different leaf and routing block boundaries from a one-lane
run; logical records and query results remain equivalent. No range becomes
visible independently. A compaction still has to scan its inputs and exact
vector query cost remains linear; these are bounded latency costs rather than
unbounded memory.

The first merged representation uses `sux` Elias–Fano sequences and rear-coded
dictionaries where those structures directly fit an engine. It does not yet
emit ranked dense bit vectors. Full-text scoring uses bounded per-posting term
frequency and field length rather than generation-wide BM25 statistics, and
exact vector scoring is sequential within each fetched block. These affect
index size, ranking quality and query throughput, not path/version visibility,
freshness, durability or Zanzibar filtering.

Format-2 cache reads remain mmap-backed and do not copy immutable blocks into a
second managed heap cache. `ANVIL_INDEX_MEMORY_PERCENT` independently bounds
concurrent block-fetch buffers and retained mappings; each mapping is charged at
least one 4 KiB page so a large disk cache cannot retain an unbounded number of
tiny mappings. Live query slices pin their mappings and may temporarily exceed
the retained-mapping budget until the last slice is dropped. The first engines
use bounded demand reads and only explicit prefetch, without format-specific
cache-retention priorities. Full-text and hybrid queries accept at most 32 term
cursors, while typed-JSON and Git pages retain at most 4 MiB of projected
records. Concurrent query working sets are bounded per request but do not yet
share one aggregate byte-admission pool. These limits can reject a pathological
query or increase cold-query I/O, but cannot change authoritative index bytes or
results.

Stable index identity, definition version, source checkpoint, atomic watermark
and build diagnostics are bound once by the immutable generation manifest. A
run root contains its kind, level, path/version state, component descriptors,
hashes and statistics but does not duplicate those generation-level fields.
Consequently a run is opened only through its validated manifest; it is not a
standalone interchange artifact.

A not-yet-published run build or compaction must seal and publish its earliest
staged block before the ordinary awaiting-publish GC age, 24 hours by default.
If it exceeds that window, the writer retries from the last complete
generation. L0 work is bounded and the production qualification is far below
this interval; support for a single compaction lasting longer than the GC age
is deferred. No partial run becomes query-visible.

An index builder admits at most three outbound rebuild snapshots per index
kind, but a source node does not yet impose a second process-wide ceiling on
the total inbound snapshot sessions requested by all trusted cluster peers.
Each live source snapshot owns one bounded RocksDB snapshot thread and sends a
frame only after receiving pull credit, so corpus bytes do not queue in heap;
nevertheless, a cluster with many simultaneous cold definitions can retain
many thread stacks on one source. Operators should stage mass definition
creation or temporarily reduce the number of active builders. A source-wide
admission queue is deferred until production measurements justify its policy.

Unsupported or corrupt format-2 derived objects are rejected when discovery or
generation loading encounters them. Startup does not scan every dormant object
under the reserved index prefix. An invalid candidate cannot replace the
current generation. If an already-published current pointer or manifest is
later found corrupt, that definition is unavailable, reports the failure and
retries rather than preventing unrelated object storage from starting. Delete
and recreate that definition to rebuild it from authoritative source objects.

The implementation dependencies and their selected license options are
recorded in [the Anvil 0.6 index dependency record](dependency-licenses.md).

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
the client deadline and `ANVIL_INDEX_QUERY_TIMEOUT_SECONDS`, whose default is
five minutes. The same remaining budget is propagated across object and peer
calls. These maxima are deliberately not transport-wide timeouts because `Put`
and `WatchPrefix` are long-lived streams. Local authorization, administration,
and credential unary requests still rely on their client or external TLS
terminator to supply a deadline; extending the shared deadline wrapper to those
existing services is deferred.

Authorization-aware index pagination batches the common case where every
candidate is visible. If Zanzibar filters or reorders a candidate, Anvil falls
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

## Historical 0.5.2 index implementation (replaced in 0.6.0)

This section records the behavior of the released 0.5.2 index implementation.
Its whole-generation builder, page-map format and source router are removed in
0.6.0; the current boundaries are described above.

The first 0.5.2 index engines intentionally provide a small, usable query
surface rather than every optimization commonly found in a dedicated search
server:

- full-text search has one bounded Unicode-aware lowercase tokenizer, phrase
  positions, and BM25-style ranking; it does not yet provide language-specific
  stemming, fuzzy matching, synonyms, or configurable token filters;
- vector search is an exact page-streamed scan rather than HNSW, so it is
  correct for small indexes but query cost grows linearly with indexed vectors;
- hybrid search evaluates the complete full-text and vector candidate sets
  before deterministic weighted fusion; it has no threshold-pruning algorithm;
- Git-source and tensor/model indexes provide minimum manifest projections;
  Git supports exact and ordered tree lookup, while Tensor supports exact
  `(model_id, tensor_name)` lookup. Tensor input objects must contain one
  `TensorRecord` JSON value or an array of them, including the ordinary source
  object path and version used for result authorization. The retained Hugging
  Face manifest format has no public index definition or gateway in 0.5.2;
- typed JSON and metadata predicates use paged ordered postings without a
  compressed-bitmap accelerator in this release.

These are performance and feature limits, not weaker visibility semantics. A
query returns the latest published generation available to the serving node
together with freshness metadata describing its source checkpoints and known
lag. Before the first generation is published, it returns empty results with
generation `0`, `initial_build_complete=false`, and `rebuilding=true`. Index lag
never changes an otherwise valid query into an error; the client decides
whether the returned generation is fresh enough for its use.

Generation construction currently holds the selected source-object projection
and newly encoded engine files in builder memory. Source invalidations are
collected once per node and shared across local builders, but an affected index
is rebuilt as a complete immutable generation rather than incrementally
compacted. This favors the intended small and medium indexes; very large
indexes can create substantial temporary builder memory and write load.
Logical index files use a fixed 4 MiB segment target in 0.5.2. The cache has an
internal bounded prefetch operation, but format-specific read hints and writer
seal-boundary hints are not yet part of the engine interface.

The memory-cache budget is configured as a percentage in 0.5.2; there is no
absolute-byte memory option or separate materialisation-concurrency setting.

The disposable source-router history is fixed at 1,024 complete barriers or
1,000,000 changes per node in 0.5.2. A builder that falls behind those bounds,
or observes a source epoch or membership change, performs a fresh current-head
scan. This does not lose acknowledged changes, but catch-up can be expensive.
The minimum clustered runtime starts this node-level router on every ACTIVE
node, even when that node currently owns no index builder, so empty or lightly
indexed clusters perform more source-tail polling than necessary. The router
still reads each source only once per node and shares batches among that node's
builders; assignment-aware suspension is deferred as an optimization.

Initial index-router readiness requires a clear checkpoint from every ACTIVE
source. A restarting node therefore keeps its public listener closed while an
ACTIVE peer is unreachable, even if the remaining Raft voters otherwise have
quorum. Nodes that were already serving continue to serve their last published
index generations with freshness evidence. Relaxing cold-start discovery
without weakening checkpoint evidence is deferred.

Concurrent cold misses for the same immutable index segment are not yet
coalesced, so simultaneous queries can fetch the same bytes more than once.
The content hash and atomic cache-file installation keep the result correct.
Cache files left by a prior process are verified and admitted lazily when
reused; the 0.5.2 cache does not eagerly inventory those files at startup, so
the configured disk budget is restored as entries become known rather than by
one cold-start sweep. Cache eviction runs on cache activity rather than an idle
timer; if the last pinned handle is dropped while the cache is above budget,
the next cache access restores the configured bound.

Obsolete generation objects are deleted oldest-first under the configured
count, age, and authoritative-byte caps. An in-flight query remains safe through
the ordinary blob inactivity window, which is at least 24 hours while the
server request deadline is at most 30 seconds; 0.5.2 does not add a separate
distributed generation-lease protocol.

Index definition create, update, and delete use ordinary `LOCAL`
acknowledgement and then the normal metadata replication path. Immutable
generation segments, manifests, and current-pointer publication use
`REPLICATED` acknowledgement. Definition requests do not expose a durability
selector in 0.5.2.

Definition validation does not yet reject every syntactically unusable or
reserved path prefix. Such a definition is stored but its builder fails closed
without publishing a generation; it cannot expose reserved objects.

Deleting an index definition prevents further queries and builders, but its
already-published generation artifacts are not eagerly removed in this
release; normal blob reference accounting remains correct, and a later
maintenance capability will collect those unreachable index generations.

## Cold index-definition discovery in 0.5.2

This historical limitation is superseded by the sparse, bounded 0.7.0 locator
recovery described at the start of this document.

Index definitions are ordinary authoritative Anvil objects and there is no
separate registry or index-specific persistence plane. A node without its
disposable assignment cache scans the reserved definition paths at startup,
then applies weighted HRW to determine which indexes it builds or serves. The
cold-start work therefore grows with the number of index definitions. Anvil
0.5.2 accepts that startup cost rather than adding an unmeasured catalogue or
side plane; later releases can optimize discovery without changing the stored
definition format.

## Failed minority metadata candidates

A mutation can be durably present on fewer than its required logical-record
replicas if the remaining replicas become unavailable after the coordinator's
local commit. When no quorum can prove either that exact candidate or a
different valid successor, ordered reference delivery cannot determine whether
the candidate's journal effect is authoritative. Anvil fails closed at that
source position: destination cursors do not advance beyond it, the source
journal prefix is retained, and reference garbage collection remains disabled
for the affected work. If the retained journal reaches its configured bound,
later mutations that need to append a source event receive backpressure.

A quorum reporting only that the candidate is absent is not enough to discard
it because a previously issued replica apply may still complete. Mutations
already proven by a metadata quorum and other ACTIVE nodes remain unaffected;
Anvil does not trade reference correctness for recovery availability.

After metadata quorum is available again, retrying the unknown-outcome
operation with the same command ID allows the deterministic candidate to
complete. Restoring a replica that holds the required lineage can likewise
provide the missing proof. If neither the original candidate nor a
quorum-proven valid successor can be recovered, Anvil 0.6.0 has no unsafe
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
ordered recovery boundary is unsupported; it does not cause Anvil to infer or
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
the realm binding and its protected-system ownership grant. Anvil 0.5.1 keeps
that guarantee on a one-node cluster, but rejects the first binding with
`UNAVAILABLE` when more than one node is active. Existing realms can be
rebound and used normally across the cluster. A later capability must add one
bounded cross-Zanzibar operation before enabling first binding on multi-node
clusters; 0.5.1 does not weaken the atomic ownership guarantee.

## Cluster lifecycle operations

Anvil supports genesis, authorized node preparation, learner catch-up, typed
ownership handoff, and online ADD activation for ordinary cluster formation.
The bounded Raft state machine also validates removal, capacity-reweight, and
peer-certificate-overlap transitions, but 0.5.4 does not expose public
operations that start those transitions because their corresponding online
ownership handoff and live TLS-reload orchestration are not yet complete.
Operators must not submit those internal Raft commands directly.

There is no public drain or detailed cluster-health RPC in 0.5.4. Public
listener availability is the readiness boundary: Anvil binds it only after
membership, serving-fence, bootstrap, authorization, atomic recovery, and
ordered reference startup checks complete. Normal process termination stops
the public listener before the peer runtime and flushes the local store; it
does not remove the node from committed membership.

## Transient reference-delivery cursor skew in 0.5.1

Under concurrent cluster activity, ordered reference delivery can transiently
observe a destination cursor one position beyond the source-tail snapshot and
pause that source until a later retry. Anvil fails closed while this condition
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

Anvil 0.5.0 accepts the bounded `content_type` header but does not accept
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
Applications needing those operations must use Anvil's native API or wait for
a later gateway capability.

Authenticated requests derive the tenant from the client credential, so an
S3 client uses ordinary `/bucket/key` path-style requests. An unsigned public
read has no credential from which to derive a tenant and therefore uses
`/tenant/bucket/key`; public ListObjectsV2 similarly uses `/tenant/bucket`.

SigV4 verification needs plaintext-equivalent signing material, whereas
pre-0.5.3 Anvil credentials intentionally retained only an Argon2id verifier.
New and rotated credentials contain an AES-256-GCM envelope in their existing
replicated credential record. Its key is domain-separated from the cluster JWT
signing material and its associated data binds tenant, application and client
identity. Existing credentials continue to work unchanged with gRPC but must
be rotated once before S3 use. Anvil never stores this material in a separate
column family or side plane.

PutObject accepts a signed SHA-256 payload or the fully chained
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` aws-chunked form and verifies the decoded
bytes before publication. Other aws-chunked trailer/checksum variants are
reported as unsupported rather than accepting unbound payload bytes.

## Git smart HTTP gateway in 0.5.3

The shared public listener supports Git smart HTTP push and pull at
`/git/{tenant}/{bucket}/{repository}.git`. Basic authentication exchanges the
application client ID and secret through Anvil's normal credential path;
Bearer authentication accepts a normal Anvil access token. Pulls may omit
credentials only when current Zanzibar policy permits anonymous reads for the
bucket. Every push is Zanzibar-authorized and publishes one authoritative Git
bundle as an ordinary object below the protected `_anvil/git` namespace using
`PutIfAbsent` or `PutIfVersion`.

Anvil 0.5.3 deliberately uses `git-http-backend` and a disposable node-local
bare-repository cache. Each request rematerializes the repository from the
authoritative bundle, and each successful push rewrites the complete bundle.
The first implementation serializes Git CGI requests per node. Request bodies,
CGI responses and the bundle being published are buffered in memory and are
bounded by the configured object maximum where Anvil controls the input; Git's
CGI response itself does not yet have an independent streaming limit. These
costs make this surface suitable for ordinary small-repository workflows,
not yet for very large repositories or high-concurrency Git hosting.

Concurrent pushes on different gateway nodes are protected by the bundle
object CAS, so one cannot silently overwrite the other. The losing client gets
an HTTP conflict after its local receive-pack completes and must fetch then
retry; 0.5.3 does not translate that late CAS conflict into a polished Git
receive-pack status. Git gateway transfer bytes are not yet included in path
accounting, and internal derived bundle writes are intentionally not billed as
client object ingress.

## Atomic preparation and the blob inactivity clock

Staging an atomic program's prepared output blobs and bundle sets their
ordinary blob `updated_at` timestamps before `CommitBatch` is proposed. An
unusually long delay in staging, waiting for the commit gate, or recovering an
earlier commit therefore reduces the effective post-commit replay and recovery
retention by the length of that delay.

The normal path is expected to take milliseconds. Anvil 0.5.0 does not refresh
the prepared blobs immediately before `CommitBatch`, and it does not add a
special lease, side store, or second lifecycle clock for atomic programs.

## Permanent deletion from versioned program-only paths

A version-enabled `PROGRAM_ONLY` path retains its historical payload versions.
Path policy correctly prohibits the ordinary `DeleteVersion` API from mutating
that path, and the 0.5.0 atomic-program DSL has no operation for deleting one
exact retained version. Operators therefore cannot permanently prune that
history in Anvil 0.5.0.

## Fixed accounting traffic bounds in 0.7.0

The process-local accounting traffic queue, per-bucket matcher cache, and
traffic-batch limits use fixed bounded defaults in 0.7.0. A
sustained ingress rate above those bounds can drop bandwidth-accounting entries;
Anvil reports the dropped batches and bytes, and stored-byte and object-count
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
its held RocksDB snapshots cannot be resumed, so Anvil restarts the baseline for
that same accounting path scope. The last complete rollup remains readable;
ordinary restarts resume valid rollups, and no unrelated object heads or startup
inventory are scanned. Operators can retry after peer health returns. A
resumable cross-node snapshot protocol is deferred.
