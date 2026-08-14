# ANVIL-0014: Anvil-Native Segment Indexes

Status: Accepted

Supersedes: ANVIL-0013 in full

Audience: Anvil implementors, operators, client authors, and reviewers

## 1. Decision

Anvil will replace format-v3 indexes with an Anvil-owned format-v4 segment
engine. The engine adopts the proven execution contracts used by Lucene:

- immutable segment cores with mutable behavior presented through new segments
  and generation-bound live-document views;
- dense segment-local document identifiers;
- seekable term dictionaries and blocked, advanceable postings;
- cost-led Boolean iterator algebra instead of one-driver candidate scanning;
- typed, independently readable column values for filtering, ordering, and
  scoring;
- stored fields fetched only after filtering, liveness, pagination, and
  authorization have reduced the candidate set;
- optional definition-time physical ordering for workloads which repeatedly
  request the same leading order;
- true generation-bound search-after pagination; and
- background segment merging which preserves exact match membership while a
  newly published generation may recompute ranking statistics.

Lucene is the reference for these contracts and their operational maturity. It
is not Anvil's storage authority, file format, library dependency, or public
API. Anvil owns every durable byte and keeps its existing distributed object,
publication, authorization, and resource-control boundaries.

Format v4 is a clean break. It has no format-v3 reader, converter, backfill,
dual writer, query fallback, compatibility shim, or mixed-generation path. A
format-v4 deployment builds indexes from authoritative ordinary objects and
their retained source journals.

Apache Arrow is not adopted as an authoritative Anvil storage format and no
Arrow dependency is added by this RFC. Format-v4 postings, live masks, terms,
columns, stored fields, vectors, manifests, and pack envelopes use explicit
Anvil codecs. Existing RocksDB records, source journals, Raft state, opaque
payloads, erasure shards, atomic-program artifacts, accounting objects, and
gateway payloads also do not move to Arrow.

The engine does define a generation-pinned, column-oriented scan contract so a
future DataFusion gateway can push projection, predicates, ordering, and limits
into Anvil and convert bounded result batches to Arrow `RecordBatch` values.
Arrow belongs at that execution and interchange boundary, not beneath the
index or object store.

The release containing format v4 should be `0.9.0`, because the durable index
format and execution engine are intentionally incompatible with `0.8.x`.

## 2. Motivation

Format v3 fixed the unbounded construction-memory and small-artifact problems,
but its query model is not viable for selective compound queries over large
corpora.

One production-shaped corpus contained 729,917 documents and approximately
1.2 GiB of authoritative Typed JSON index bytes. A selective query requesting
four results:

```text
public = true
AND state = "active"
AND withdrawn = false
AND ecosystem IN ("cargo", "npm", "pypi")
ORDER BY modified_at DESC, source_record_id ASC
LIMIT 4
```

repeatedly reached a 300-second deadline after 255,000 to 329,000 logical reads
and 8.7 to 11.6 GiB of logical bytes. A 999-result pagination query read several
GiB per page, and consecutive pages performed nearly identical work. Reducing
the generation from eleven runs to two did not materially change the result.

The host remained mostly CPU-idle with negligible I/O wait while one worker
performed the serial candidate path. This rules out disk bandwidth, memory
pressure, and run fanout as the primary cause.

The cause is architectural:

1. format v3 chooses one predicate as a driver;
2. a broad equality such as `public = true` can become that driver;
3. other predicates are evaluated after candidate retrieval instead of by
   intersecting independently advanceable postings;
4. current-version liveness can require repeated cross-run probes;
5. ordering and continuation filtering occur after expensive candidate work;
   and
6. stored projected values are read too early.

Compaction cannot repair an execution model which reads most of the index for
a sparse result. More heuristics around the existing driver merely move the
failure to another predicate distribution. The durable structures and the
query engine must be designed together.

## 3. Goals

Format v4 must:

- make conjunctions use every useful predicate rather than one chosen driver;
- make iterator advancement and block skipping the normal execution path;
- make current-version liveness a local segment operation, not a distributed
  lookup per candidate;
- make continuation tokens seek before candidate decoding;
- make result ordering use fast columns or a declared physical order without
  loading stored fields;
- bound query, build, merge, cache, and authorization memory;
- retain complete source-journal, atomic-program, publication, durability,
  Zanzibar, and backpressure guarantees;
- support all eight public index kinds through one common segment foundation;
- keep authoritative artifacts in the ordinary inline or erasure-coded object
  path;
- make every persistent codec portable across AMD64 and ARM64 and independent
  of Rust memory layout;
- expose enough statistics and scan semantics for a future cost-based Anvil
  planner and DataFusion gateway; and
- prove logical read work, not merely wall-clock latency, in qualification.

## 4. Non-goals

This RFC does not add:

- a SQL public API or DataFusion runtime dependency;
- Arrow IPC, Arrow Flight, or Arrow files;
- a distributed scatter/gather query engine;
- a second index byte plane, registry, journal, job database, or authority;
- index data or definitions in Raft;
- a mutable on-disk segment core;
- an approximate-nearest-neighbour graph;
- mesh or cross-region coordination;
- automatic inference of a permanent public SQL schema from arbitrary JSON;
- a format-v3 compatibility path; or
- changes to opaque object, S3, Git, PersonalDB, or atomic-program payloads.

## 5. Terms

**Generation** is the immutable, source-complete view selected by one
compare-and-swap current pointer. It pins a definition version, schema
fingerprint, source barrier, segment set, and live-document view.

**Segment core** is an immutable collection of documents built or merged
together. Its document identifiers, term ordinals, block locations, and
statistics are private to that segment.

**DocId** is a dense `u32` ordinal local to one segment core. It is never an
object identity and never appears in a public response or continuation token.

**Stable object identity** is the exact `(path, object_version)` inside one
stable numeric tenant and bucket scope.

**Live-document view** is the generation-selected bitmap which says which
DocIds in one segment still represent current visible object heads.

**Term dictionary** maps a canonical field/value term to posting metadata and
supports exact seek, prefix range seek, and ordered enumeration.

**Posting iterator** enumerates matching DocIds in ascending order and supports
`next`, `advance(target)`, and a conservative remaining-work estimate.

**Fast column** is a typed, block-addressable value column keyed by segment
DocId. It is the Anvil equivalent of Lucene DocValues and is used for ordering,
range verification, scoring, and projected result values.

**Stored field** is a projected value retained for final result materialization
but not required to enumerate a predicate posting.

**Physical order** is the optional definition-time document order applied
inside every segment. It is not a promise that segments form one globally
contiguous file; a query merges their ordered iterators.

**Scan batch** is a bounded internal batch of selected logical columns and
stable object identities. It is an Anvil type, not an Arrow type.

## 6. Required invariants

1. Ordinary objects remain the sole source of truth. Index artifacts are
   reproducible materialized projections.
2. One source-local ordered journal transition commits atomically with its
   authoritative object mutation and sparse routes.
3. Reaching a configured journal entry or byte capacity applies admission
   backpressure before a new source-producing mutation. Required index evidence
   is never truncated or sampled.
4. Every published generation represents a complete membership, source, and
   atomic-program barrier. A partially built segment is never queryable.
5. One generation-pointer CAS is the only publication point. No segment or
   live-mask block becomes visible independently.
6. Every durable index artifact is an ordinary Anvil object and reaches the
   required artifact durability before publication.
7. Raft contains no definition, source event, segment, posting, column, live
   mask, manifest, cursor, query state, or cache entry.
8. Weighted HRW chooses up to three query owners. Rank zero builds and
   publishes; any public node may proxy a query to an owner. Query execution
   never scatters across owners.
9. Query correctness is independent of cache contents. Cache data, mmap views,
   decoded blocks, scratch files, assignment projections, and builders are
   disposable.
10. A segment DocId is meaningful only with its segment identity and generation
    schema. A merge assigns new DocIds.
11. Every exact predicate is evaluated exactly. A cost estimate may change
    execution order but cannot remove a predicate or weaken it to sampling.
12. Current-version and delete filtering uses the generation-pinned live view,
    a disposable post-barrier invalidation overlay, and bounded-batch
    exact-current validation. A physically ordered scan validates only
    selected/refill hits; arbitrary top-K may have to validate every exact
    Boolean match before heap admission, but never as serialized point reads.
13. A matching physical-order continuation seeks before predicate and stored
    value work. An arbitrary-order top-K continuation applies its bound while
    collecting and never materializes rejected earlier-page fields, although it
    may necessarily rescan exact Boolean matches.
14. Stored fields and ordinary payload bytes are not read before predicate,
    liveness, cursor, and the order plan's authorization/exact-current
    processing have selected likely results.
15. Authentication failure never degrades to anonymous access. Public reads
    still require the explicit Zanzibar public-read grant.
16. CPU-heavy build, merge, decoding, scoring, and collection do not run on or
    block the serving-fence and membership executor.
17. Startup work is proportional to this node's assigned definitions and
    retained cursors, never to all ordinary object heads, blobs, or cache files.
18. Builder and query memory is charged before use to the existing shared
    per-index-kind budgets. Parallelism cannot multiply the budget silently.
19. No Rust struct layout, crate-private enum discriminant, pointer, native
    `usize`, or third-party serializer memory image is durable format.
20. A malformed, unsupported, over-sized, or checksum-invalid artifact fails
    closed with a bounded error; it cannot drive an allocation from unchecked
    on-disk lengths.
21. Async lane orchestration never occupies a Rayon worker. Every Rayon task is
    a finite leaf CPU chunk and may not synchronously wait for another task,
    channel, future, or permit whose progress depends on the same pool.

## 7. Retained coordination and authority

### 7.1 Definitions and discovery

Index definitions remain Zanzibar-authorized ordinary objects in the tenant
and bucket they govern. Their ordinary mutation carries a trusted bounded
definition transition. On the source metadata coordinator, the transition
updates the existing definition locator in the same RocksDB batch as the head,
version, source event, and sparse routes. Metadata replicas apply their typed
object mutation without appending a second source event or route.

The locator contains only stable numeric scope, definition identity, path, and
object version. It is discovery evidence, not a definition payload, permission,
registry, or second source of truth. A node exact-reads and validates the
ordinary definition before acting on it.

Normal startup restores this node's bounded assignment projection and resumes
its sparse source cursors. A true assignment gap scans definition locators, not
ordinary object heads. Buckets without definitions impose no index startup
work.

### 7.2 Placement

For stable `(tenant_id, bucket_id, definition_id)`, weighted HRW over committed
ACTIVE membership chooses up to three owners:

- rank zero is the only builder and publisher;
- ranks one and two materialize published artifacts for query service and are
  candidates for builder failover; and
- any public ingress node proxies a query to an available owner.

Assignments are disposable local projections and are not recorded in Raft. A
membership fence prevents an old rank-zero builder from winning publication
after ownership changes.

### 7.3 Source journal, retention, and backpressure

Each metadata coordinator has one monotonically sequenced source journal. On
the coordinator, one versioned transition is appended in the same RocksDB batch
as the object mutation and sparse routes. It contains transition metadata, not
payload bytes. Metadata quorum settlement advances later. Sparse bucket and
definition routes contain only source positions.

Consumers may read only through the durable settled watermark. Settlement is
not itself a retention cursor. The source retains entries through the minimum
reference-safe, index-safe, and accounting-safe positions; public watch cursors
do not pin internal history.

Public `WatchPrefix` remains an invalidation subscription, not the internal
index delivery protocol or a cluster-wide total order. A slow subscriber may
receive `RESUME_EXPIRED` and recover through ordinary read/list APIs. This does
not weaken internal index retention, which is released only by complete
published barriers.

The index manager on each builder node demultiplexes assigned work without
multiplying a source scan by the number of buckets. For each retained source
interval it chooses the cheaper bounded access plan: seek the sparse routes for
the buckets for which this node owns rank-zero definitions, or read that source
interval once and filter stable bucket IDs through a disposable assignment map.
It never reads the same retained interval once per assigned bucket. Work is
therefore bounded by the cheaper of assigned-bucket route probes and retained
source records, plus the matching routed records themselves. A node with no
rank-zero assignment for that source performs neither scan. Only a matched
bucket causes definition resolution or payload projection.

The manager advances one aggregate index-consumer checkpoint per source only
after every affected rank-zero definition has published a complete barrier.
The durable checkpoint set is bounded by source, consumer kind, membership
fence, and ACTIVE consumer node, not by definitions, buckets, or objects.
Rank-one and rank-two query owners do not pin source history because they
consume the rank-zero builder's published generation.

A membership cutover fences the old checkpoint set. A new responsible builder
recovers every assigned definition's published barrier before its aggregate
checkpoint can release history; a removed node ceases to pin only after the
committed cutover. Malformed, future-fence, future-offset, unavailable-epoch,
or below-floor evidence fails closed and cannot authorize pruning.

Receipt retention duration and receipt/journal entry and logical-byte caps are
non-zero startup configuration:

- mutation-receipt retention defaults to 24 hours;
- mutation-receipt capacity defaults to 2,000,000 entries and 512 MiB;
- source-journal capacity defaults to 1,000,000 entries and 512 MiB; and
- source journals have no independent time-retention promise.

The values may change on restart. Reducing a bound below current occupancy is
valid: reads, pruning, and consumers continue while affected new writes wait.
A single encoded receipt or journal transition larger than its complete byte
cap is rejected immediately rather than waiting forever.

Before commit, the serialized mutation attempt computes the exact receipt,
journal, and route costs and performs bounded safe pruning. If the complete
RocksDB batch cannot fit, none of it is applied and admission waits until safe
progress frees capacity or the request deadline expires. Deadline expiry
returns a retryable capacity failure and commits no object, receipt, reference
delta, journal event, or route. Unexpired receipt guarantees are never shortened
to admit more work.

Publication-progress debt is available only for the exact encoded entries and
bytes needed to finish an already constructed, source-complete index generation
or a complete accounting rollup. Eligible writes are limited to the immutable
packs, segments, manifest, current pointer, or rollup object which complete that
specific publication. It excludes client writes, unrelated internal objects,
source snapshots, rebuild initiation, compaction, cache population, and
speculative work.

Every debt entry is appended through the normal journal path. Failed or
incomplete publication leaves that evidence retained and cannot manufacture a
prune proof; retry or recovery completes from ordinary artifacts and retained
source evidence. While any debt is non-zero, ordinary source-producing writes
remain backpressured. Only the successful generation-current or rollup CAS
advances the consumer cursor, permits safe pruning, and repays the debt. No
event is dropped and the exception cannot become a general reserve capacity.

Source lag never silently selects a rebuild. A missing required journal suffix
fails that definition closed until an authorized principal requests a rebuild
or repairs/deletes the definition.

### 7.4 Explicit rebuild

The existing public `RebuildIndex` operation remains Zanzibar authorized by
the same capability as definition mutation. It writes a new version of the
ordinary definition through compare-and-swap and remains limited to one
accepted rebuild per stable definition per rolling hour. There is no job
registry or Raft record.

### 7.5 Bulk ingestion and authorization

`BulkWrite` remains at most 1,000 independently reported object operations and
64 MiB per request. It is not a cross-object transaction. Results remain in
client input order, and batching does not change CAS, immutable-path,
versioning, command-retry, reference-count, payload-placement, durability, or
Zanzibar semantics.

One request resolves each distinct mutable tenant and bucket name once, pins
one Zanzibar realm revision, deduplicates equivalent authorization checks, and
groups writes by their complete metadata replica group. A bucket-wide grant may
prove exact object access; failure to prove it falls through to exact path
checks and never to anonymous access.

The coordinator uses bounded multi-get for receipts, heads, inline-blob
existence, and reference state, evaluates repeated paths in input order against
one pending map, and applies one RocksDB `WriteBatch` per local replica group
while retaining one public receipt/outcome per operation. Payloads are hashed
once and moved on local routes rather than cloned for a remote path which is not
used. A one-node topology uses its known local authority without hashing every
path; clustered placement is computed once per exact group.

### 7.6 Accounting

Accounting definitions retain ordinary-object authority, the transactional
definition locator, top-three HRW assignment, sparse bucket routes, bounded
scoped source snapshots, complete barriers, and one current-rollup CAS.

Exact stored bytes and object counts pin source history until a complete rollup
is published. A captured or completed-but-unpublished snapshot is not retention
evidence. Restart resumes from the published barrier; a true unavailable source
epoch may require that accounting definition's scoped baseline. Only its
complete publication advances the accounting-safe cursor.

Ingress and egress traffic remain bounded best-effort usage telemetry, not a
financial ledger. Weighted HRW chooses one disposable matcher for each stable
numeric bucket. It lazily loads only that bucket's enabled accounting prefixes,
aggregates batches idempotently, and exports dropped batch/byte signals. A
failure or reassignment window can therefore miss a bounded amount of billable
traffic; stored-byte and object totals remain exact at their reported complete
barrier. Anvil adds no global traffic log or per-bucket Raft record.

## 8. Public contract

The public `IndexService` retains create, update, get, list, rebuild, delete,
and query operations for these eight kinds:

1. Path;
2. Metadata Filter;
3. Typed JSON;
4. Full Text;
5. Vector;
6. Hybrid;
7. Git Source; and
8. Tensor.

Existing query predicates remain equality, membership, prefix, less-than,
less-than-or-equal, greater-than, greater-than-or-equal, and existence. Query
limits, result shapes, generation freshness evidence, and opaque page tokens
remain public concepts.

Format v4 adds one optional physical-order declaration to Typed JSON
definitions. It contains one or more existing indexed field names and
directions. It is definition-versioned. Adding, removing, or changing it
requires a new complete generation.

Typed JSON field definitions add an explicit `multi_valued` Boolean. False is
the default and declares zero or one scalar; true declares zero or more
scalars. Equality, membership, range, prefix, and existence predicates continue
to work on either cardinality. Public `IndexOrder` and physical order may name
only a field with `multi_valued = false`; query validation rejects an ordered
multi-valued field before opening index artifacts.

The public protobuf change is deliberately small and reuses `IndexOrder`:

```protobuf
message IndexField {
  string name = 1;
  string json_pointer = 2;
  bool multi_valued = 3;
}

message TypedJsonIndexSpec {
  repeated IndexField fields = 1;
  repeated IndexOrder physical_order = 2;
}
```

Physical-order fields must produce zero or one scalar value per document. A
builder likewise fails a candidate generation with a precise definition/data
error if any field declared single-valued produces multiple values; the
previous complete generation remains published. This avoids silently choosing
an array element and gives ordering one meaning.

A query matches the physical order only when its complete explicit field and
direction list exactly equals the definition's declaration; both then append
the same implicit stable-identity tie-break. A proper prefix does not match:
`(a, b, identity)` is not ordered as `(a, identity)`. An empty query order also
does not match a non-empty physical declaration. Every non-matching order uses
the arbitrary top-K path.

Format v4 defines a total tagged scalar order for Typed JSON:

```text
null < boolean < number < unsigned < string
```

Values compare normally within one tag. `NaN` and infinite JSON numbers cannot
occur, and numeric negative zero compares equal to positive zero. Strings use
unsigned UTF-8 byte order; locale collation is outside format v4. Missing values
sort last for ascending order and first for descending order, and remain
distinct from explicit JSON `null`. This fixed rule applies to arbitrary and
physical order because the current public `IndexOrder` has no missing-placement
mode. The stable object identity is the final ascending tie-break in every
physical or query order. Descending reverses the selected field comparison but
not the final uniqueness of that identity.

The tagged total order applies to result ordering only. Equality and range
predicates remain type-exact: a range bound enumerates only its own scalar tag,
so, for example, strings do not satisfy a numeric greater-than predicate merely
because strings sort after numbers.

Arbitrary query ordering remains correct without an exactly matching physical
order. It uses bounded top-K collection over fast columns and may inspect every
document which satisfies the Boolean predicate. Matching physical order
permits early termination and is therefore the preferred definition for
recurring sparse ordered queries.

No segment DocId, field ordinal, block address, or implementation structure is
added to the public API.

## 9. Format-v4 artifact envelope

### 9.1 Reserved namespace

Format v4 uses only these canonical reserved object paths:

```text
_anvil/indexes/v4/definitions/<index_name>
_anvil/indexes/v4/<index_id>/artifacts/<blake3_hex>
_anvil/indexes/v4/<index_id>/manifests/<blake3_hex>
_anvil/indexes/v4/<index_id>/current
```

Definition names remain one validated path segment. Numeric index IDs use their
canonical decimal spelling without leading zeroes. Digests are exactly 64
lower-case hexadecimal characters. Path parsers match the complete segment
shape; textual prefix matching cannot widen retention, authorization, or delete
scope.

Component packs, segment roots, live-mask roots/blocks, locator roots/blocks,
scoring-stat roots/blocks, and other immutable descendants use the `artifacts`
form and their complete object payload digest. A generation manifest uses the
`manifests` form. The mutable `current` object is the sole publication point.

A format-v4 process discovers only v4 definitions. The release starts from new
volumes and definitions are recreated through the public API. It does not scan,
interpret, convert, delete, or treat `_anvil/indexes/v3/` objects as definitions
or artifacts.

Every path segment exactly equal to `_anvil` is reserved. Source events for
reserved objects continue to participate in ordinary durability, reference,
retention, and accounting machinery, but no index definition may project them
as user documents. This rule applies even to a whole-bucket definition and
prevents definitions, artifacts, manifests, and current pointers from
recursively indexing themselves.

### 9.2 Component envelope

Every format-v4 component uses an explicit portable envelope:

```text
magic              [8]byte = "ANVLIDX4"
component_kind     u16 little-endian
codec_version      u16 little-endian
flags              u32 little-endian
index_id           u64 little-endian
definition_version u64 little-endian
schema_fingerprint [32]byte
segment_id         u64 little-endian
logical_length     u64 little-endian
encoded_length     u64 little-endian
payload_checksum   [32]byte BLAKE3
payload            [encoded_length]byte
```

The exact fixed header is shared by term, posting, column, stored-field,
position, norm, vector, live-mask, path-locator, and manifest components. A
component-specific payload has its own explicit bounds and codec version.
Readers validate the fixed header, checked lengths, declared upper bounds, and
checksum before allocating or decoding.

Logical components are packed into immutable ordinary Anvil objects. A checked
descriptor identifies the canonical reserved object address, exact object
version, object content hash, byte offset, encoded length, logical length,
component kind, codec version, and checksum. The address and version retain and
resolve the ordinary object; the content hash provides integrity and ordinary
payload deduplication but is not by itself an object reference. The existing
async index-handle/cache layer fetches the required range or pack and may mmap
local cache files. A cache never changes authoritative bytes.

### 9.3 Fixed format bounds

Format v4 retains these proven portable bounds:

- one encoded logical component block, including its envelope, is at most
  512 KiB;
- one component decoder may claim at most 4 MiB after validating the encoded
  header;
- one ordinary artifact pack targets and may not exceed 16 MiB;
- one routing key is at most 4,096 bytes;
- one routing node has at most 32 children and routing height is at most eight;
  and
- one segment contains at most `u32::MAX` documents and splits before assigning
  a DocId outside that range.

A posting, column, stored-field stream, or dictionary larger than one logical
block is split into independently checked blocks. No record may straddle an
artifact pack. Encoded counts and lengths use checked arithmetic and no decoded
allocation is derived from an unvalidated value. These limits are format
constants, not startup settings.

### 9.4 Schema fingerprint

The schema fingerprint is BLAKE3 with domain separator
`anvil.index.schema.v4`. Its input is one explicit length-prefixed canonical
binary encoding in this order:

1. index kind, path prefix, and content-type scope;
2. every field in canonical definition order, including FieldId, public name,
   source selector, permitted scalar domain, cardinality, null/missing policy,
   collation, and enabled components;
3. tokenizer/analyzer and full-text scoring semantics where applicable;
4. vector dimensions, metric, normalization, and hybrid weights where
   applicable;
5. Git repository or Tensor model scope where applicable;
6. physical order and stable tie-break semantics; and
7. every required component codec semantic version.

Integers use fixed-width little-endian encoding, enums use documented Anvil
tags, strings and byte fields use a `u32` byte length followed by exact bytes,
and lists use a `u32` count. Protobuf wire bytes, JSON serialization, map
iteration order, and observed segment statistics are not fingerprint inputs.
Any result-affecting definition or codec change therefore creates another
fingerprint and cannot merge with old segments. A rebuild which changes only
the definition object version may retain the same fingerprint; definition
version remains a separate merge-identity field.

### 9.5 Generation manifest and durability

The generation manifest contains:

- definition identity and version;
- format version and schema fingerprint;
- complete source and atomic-program barrier;
- physical-order declaration, if any;
- ordered segment descriptors;
- each segment's live-view root;
- path-locator roots used by the builder;
- per-segment statistics;
- artifact encoded and logical byte totals.

The segment ID is a normal cluster-unique Snowflake ID allocated by the builder.
It needs no registry, Raft entry, or managed counter. Generation retention is
represented by the bounded set of retained generation roots; manifests do not
form an unbounded predecessor chain.

The manifest and every referenced ordinary artifact request `REPLICATED`
acknowledgement before the generation current pointer can advance whenever the
ACTIVE topology has at least two nodes. On a one-node topology, publication
uses the ordinary `LOCAL` acknowledgement threshold: the node durably stores a
complete replica and the object remains subject to normal placement and online
convergence as nodes join. `LOCAL` never means an index-only side file or a
permanently local object.

An undersized one- or two-node cluster stores complete object replicas; once
the fixed cluster erasure profile is satisfiable, normal object placement uses
that profile without changing the index format. Artifact publication cannot
point at process staging bytes or a coordinator-only preparation file.
The current-pointer CAS itself uses the same topology-aware acknowledgement
threshold as the immutable artifacts it publishes.

### 9.6 Generation retention

The mutable current pointer contains the current manifest reference and a
bounded ordered set of retained manifest references. Retained generation count
has a format maximum of 64. The defaults retain at most three generations, at
most 24 hours, and at most 50 GiB of authoritative generation bytes per index.
The first exceeded count, age, or byte bound makes the oldest non-current
generation eligible after the minimum in-flight query safety age.

The byte bound counts each distinct ordinary artifact object version once
across the retained set, so immutable components shared by generations are not
double charged.

Dropping a retained reference makes that generation's manifest and
generation-owned artifacts eligible for ordinary reference-counted deletion
and GC. Maintenance is scoped to due v4 index paths and uses bounded record,
byte, and time budgets with a resumable local cursor; it never scans all object
heads. Manifests do not form an unbounded predecessor chain. A continuation for
a generation absent from the bounded current pointer fails with the existing
generation-no-longer-available result.

## 10. Schema and field identity

Each definition version has one canonical field catalogue. A field receives a
dense definition-local `u32 FieldId` from canonical specification order. There
is no global field registry, managed counter, or mutable name-to-ID authority.
A semantically changed definition has a new schema fingerprint and builds new
segments. A rebuild may create a new definition version without changing that
fingerprint. Segments merge only when the exact `(index_id,
definition_version, schema_fingerprint)` tuple matches.

For each field, the catalogue records:

- public field name and source selector;
- permitted logical value domain: boolean, finite IEEE-754 binary64 number,
  exact unsigned `u64`, UTF-8 string, or a declared subset;
- definition-declared cardinality and whether missing and explicit null are
  permitted;
- comparison and collation semantics;
- timestamp unit and timezone semantics when a future typed definition declares
  a timestamp;
- decimal precision and scale when a future typed definition declares a
  decimal; and
- whether the field has terms, fast columns, stored values, positions, norms,
  vectors, or physical ordering.

Current dynamically typed JSON definitions do not pretend to be one SQL scalar
type. They permit the complete tagged value domain and use the definition's
explicit single- or multi-valued cardinality. Their format-v4 columns retain
Anvil scalar tags and separate presence and null bitmaps. Null is a value state,
not a numeric or string scalar type. A future SQL API must either expose that
dynamic domain conservatively or require an explicitly typed definition; it
cannot silently coerce mixed values or collapse missing into null.

Observed occurrence counts, actual scalar tags, null counts, and multi-value
counts are segment statistics. They never alter the definition schema or its
fingerprint as new objects arrive.

Persistent type tags are Anvil constants. They are never Arrow, protobuf, Rust,
or third-party enum discriminants.

## 11. Segment identity and liveness

### 11.1 Segment-local DocIds

A builder orders one segment's accepted projected documents deterministically
and assigns dense DocIds from zero. All segment components use those DocIds:
postings, fast columns, stored fields, full-text positions and norms, vectors,
and live masks.

The segment identity table maps DocId to stable `(path, object_version)`. The
public result is reconstructed from this table only for selected hits.

A segment core never changes. A merge writes a new core and new DocIds, then a
new generation atomically replaces the old segment set.

### 11.2 Path locator

The builder maintains an immutable, seekable path-locator component mapping the
current path to either live `(segment_id, DocId, object_version)` or deleted
`(tombstone_version)`. New generations add locator deltas and bounded merging
folds them by exact object version. Retaining tombstone version evidence prevents
a stale or replayed delta from resurrecting an older object. This component lets
the builder find the predecessor document affected by an update or delete.

The ordinary query path does not consult the locator per candidate. It exists
to construct the next live view and to support exact path-oriented index work.

### 11.3 Live-document views

Each generation references one materialized immutable live bitmap per segment.
The bitmap is split into fixed DocId-range blocks. Publishing an update rewrites
only blocks containing changed DocIds and reuses every unchanged block
descriptor. A new segment begins with its exact initial bitmap; tombstones clear
the predecessor and do not become query hits.

This is a mutable liveness API over immutable objects. It avoids both a
generation-wide dense renumbering and an object-head point read for every
candidate. Query workers cache live blocks with the same disposable index cache
used for other components.

Query owners also maintain a disposable post-generation invalidation overlay.
They consume relevant settled journal routes after the generation barrier, use
the path locator to map an overwritten or deleted predecessor to its segment
DocId, and clear that DocId locally. The overlay never adds a new document and
cannot claim index freshness; it only prevents avoidably stale candidates while
the next complete generation is building. Losing it costs catch-up work, not
correctness or source data. Its memory is charged to the query/cache budget; if
that budget cannot retain it, Anvil drops or partially rebuilds the overlay and
relies on the mandatory bounded exact-current result check.

Before returning results, Anvil exact-reads current heads in bounded multi-get
batches and removes any object whose exact current version or delete state
differs. This preserves the existing no-stale-version result contract even when
the disposable overlay is behind or a mutation commits concurrently. A
physically ordered iterator validates selected candidates and continues from
its current position to refill. An arbitrary-order collector validates each
exact Boolean match before heap admission because any match may outrank the
current top K. It may therefore check all matches, but it does so in bounded
batches and never as one serialized point read per candidate.

This is read-committed index behavior, not snapshot isolation. The pinned
generation fixes the candidate structures and their order. Exact-current
validation may remove an object updated or deleted after that generation's
barrier, while the replacement version does not become a candidate until a
later complete generation. Freshness evidence tells the caller where that
candidate view ends.

A merge resolves locator deltas and live masks, writes only current live
documents into the replacement segment, and publishes a fresh all-live bitmap
for it. Old cores and masks remain retained while any permitted generation can
reference them, then ordinary object reference counting and GC reclaim them.

Atomic-program changes are projected as one source group. The generation CAS
publishes all affected segment and live-mask changes together, so an index
cannot expose only part of an atomic program whose ordinary results are not
partially visible.

## 12. Common segment components

### 12.1 Term dictionary

Terms use canonical bytes prefixed by FieldId and scalar or token type. The
dictionary supports:

- exact seek;
- seek to the first term greater than or equal to a key;
- bounded prefix and scalar range enumeration;
- term document frequency and posting descriptor lookup; and
- ordered iteration for range and prefix queries.

The implementation may use finite-state or succinct structures in memory, but
the durable bytes are an Anvil codec. `sux` structures may be reconstructed
from portable arrays or used behind an Anvil wrapper; their native serializer
is never the format contract.

### 12.2 Postings

One posting list contains sorted segment DocIds. Lists are split into checked
blocks with:

- first and last DocId;
- document count;
- encoded and decoded lengths;
- block maximum score or impact metadata when applicable;
- a skip directory; and
- compressed DocId gaps and optional frequencies.

Every iterator implements the semantic operations:

```text
doc_id()          -> current DocId or END
next()            -> next DocId
advance(target)   -> first DocId >= target
cost()            -> conservative remaining document count
```

The interface is internal and async at block boundaries. A fetched block is
decoded into a budgeted local cursor; callers do not hold mutable references
across a remote fetch.

Rare postings may use simple gap coding. Dense or monotone postings may use the
approved quasi-succinct/succinct codecs. The writer selects from a small fixed
set using deterministic thresholds recorded in the descriptor. Query semantics
do not depend on the codec.

### 12.3 Fast columns

Fast columns are independently addressable blocks keyed by DocId range. A
column stores:

- a presence bitmap;
- a separate explicit-null bitmap;
- fixed-width values, monotone values, dictionary ordinals, or offsets plus
  bytes according to its Anvil logical type;
- multi-value offsets where the definition permits arrays; and
- per-block count, null count, minimum, maximum, encoded bytes, and decoded
  bytes.

The planner can use these columns for range verification, ordering, scoring,
projection, and statistics without reading a stored JSON projection.

### 12.4 Stored fields

Stored projected values are grouped into independently compressed DocId-range
blocks with a bounded offset table. They are fetched only for candidates which
survive Boolean predicates, live masks, cursor bounds, and a bounded
authorization selection.

An index query never fetches the ordinary source payload merely to return fields
already declared as stored by that index. Fetching the source object remains an
explicit object API operation.

### 12.5 Full-text data

Full Text and Hybrid segments add token frequencies, positions, field lengths,
norms, and block impact bounds. BM25 scoring operates over these components.
Phrase and positional queries use a cheap term-conjunction approximation
followed by positional verification.

At query admission, BM25 aggregates document, field-length, and term-frequency
statistics from the bounded segment set of the pinned generation. Segment
statistics may conservatively include documents cleared by a later live mask
until those segments merge, matching the practical immutable-segment tradeoff:
exact match membership remains unchanged, while a newly published merged
generation may have slightly different scores and score order. A page token
continues on its retained generation, so ranking cannot change within one
pagination sequence.

Anvil does not add a generation-global term-stat registry, high-cardinality
delta dictionary, or per-document forward term list solely to freeze scores
across compaction. Impact blocks persist raw frequency/norm maxima from which a
conservative bound is computed using the pinned query statistics. An invalid
or understated bound is corruption and fails closed; it is never an advisory
estimate allowed to skip possible hits.

Impact-aware top-K skipping is part of the format contract even if its first
implementation supports only the exact subset needed by current public query
operators. It must be possible to add a newer posting codec without changing
stable object identity, generation publication, or the public API.

### 12.6 Vectors

Vector components use fixed-width numeric blocks aligned to segment DocIds.
Presence and liveness use the common bitmaps. Exact vector search first applies
all exact non-vector filters to obtain a DocId set, then scores only surviving
vectors.

A future approximate-nearest-neighbour sidecar may use the same DocIds and
generation lifecycle. It requires a separate accepted design and cannot weaken
filter or authorization correctness.

## 13. Planner and executor

### 13.1 Logical plan

The engine normalizes a query into:

```text
ScanPlan {
  generation
  authorization_scope
  required_fields
  must
  should
  must_not
  order
  after
  limit
}
```

Each leaf advertises its exactness, estimated cost, output order, and required
components. A predicate implementation is one of:

- `Exact`: no residual verification is needed;
- `Inexact`: it produces a safe superset and names the exact bounded verifier;
  or
- `Unsupported`: this index cannot execute it.

An exact public index query cannot drop an unsupported predicate. It returns a
clear unsupported-query error unless the public contract explicitly permits
bounded residual evaluation from indexed fast or stored fields.

### 13.2 Boolean iterator algebra

For conjunction, the planner orders posting iterators by estimated cost. The
cheapest iterator advances normally; every other iterator advances directly to
that candidate. If one moves beyond it, the lead iterator advances to the new
DocId. Evaluation continues until all iterators agree or one ends.

`IN` and `OR` merge posting iterators with a bounded heap and suppress duplicate
DocIds. `NOT` advances an exclusion iterator alongside the positive iterator.
The live bitmap is another exact DocId filter. Two-phase predicates first
intersect their cheap approximation and only then run positional, column, or
other exact verification.

The planner never falls back from a selective compound query to decoding every
document from a broad first predicate. If no useful indexed predicate exists,
it may perform an explicit bounded full-index scan only where the public API
already defines such a scan. The plan, reason, and estimated work are visible
in traces.

### 13.3 Ordering and top-K

When the complete explicit query order exactly equals the definition's physical
order, each segment produces candidates in that order. A bounded heap merges
segment iterators, tests the remaining predicates and live mask, and stops as
soon as it has enough authorized hits. A proper prefix, suffix, reordered list,
direction change, or empty order does not qualify.

Otherwise, Boolean execution produces matching DocIds and a bounded top-K
collector reads only the order fast columns. Its memory is proportional to the
requested limit plus a bounded authorization refill window, not total matches.
It may inspect all matching DocIds because an arbitrary exact order has no safe
early-termination rule.

The final stable identity tie-break makes ordering deterministic across
segments and merges.

### 13.4 Pagination

A page token remains opaque and is bound to caller, tenant, bucket, definition
identity and version, canonical query fingerprint, physical order, generation,
and relevant Zanzibar revision.

For physically ordered scans, search-after seeks each segment to the first key
strictly after the cursor. It does not reproduce page one's scan. For top-K
scans without a matching physical order, the collector rejects keys at or
before the cursor while reading fast columns. That avoids retaining or
materializing the earlier page but cannot avoid re-enumerating exact Boolean
matches: no ordered access path exists from which it could safely seek.

A token never contains a DocId. Compaction can change DocIds without changing
the token's stable sort values and `(path, object_version)` tie-break. The token
remains generation-bound so its candidate structures, scoring inputs, and
retained artifacts do not change during pagination. Exact-current validation is
still read committed and can remove an identity changed after that generation's
barrier.

### 13.5 Authorization and result refill

Management operations and query admission remain Zanzibar authorized before
definition or artifact work. Bucket, tenant, index scope, path prefix, and
public-read evidence are pushed into the scan wherever they can safely exclude
candidates.

Fine-grained authorization and exact-current checks use bounded batches of
stable object identities. If a candidate is denied or stale, a physically
ordered iterator continues to refill the result page. The engine does not issue
recursive `limit = 1` queries. Authorization cannot be represented as an
optional post-processing callback which a future SQL adapter can omit.

For arbitrary-order top-K, the engine authorizes and exact-current-validates
candidate batches before heap admission. It may check every exact Boolean match
to return the true top K visible and current for the caller. The batch and heap
memory remain bounded even when total work is `M`; the engine neither retains
an unbounded denied set nor falls back to one serialized request per candidate.

## 14. Build, incremental update, and merge

### 14.1 Initial and explicit rebuild

A first build or authorized explicit rebuild:

1. pins the ordinary definition version, membership fence, finalized atomic
   watermark, and settled source tails;
2. opens bounded source snapshots and seeks only the stable numeric tenant,
   bucket, and path scope;
3. streams current heads and required payloads in byte-bounded waves;
4. projects concurrently under the shared kind budget, with async orchestration
   outside the process CPU pool and only finite leaf CPU chunks submitted to it;
5. restores deterministic order;
6. creates one or a bounded number of format-v4 segments directly;
7. replays the routed journal suffix, preserving atomic groups;
8. writes all segment, locator, live-mask, and manifest artifacts through the
   ordinary object path using the topology-aware acknowledgement threshold in
   Section 9.5; and
9. publishes one complete generation CAS.

It does not simulate a rebuild by creating hundreds of tiny L0 segments and
rewriting them through every level before first publication.

The source snapshot cursor remains bound to its RocksDB snapshot, exact source
epoch, and canonical `(tenant_id, bucket_id, path)` ordering. A large build may
run for hours while bounded frames continue to make progress; only per-frame
inactivity is deadline-bound. A committed membership-fence change or any
source-epoch change cancels the candidate before publication. It never converts
a corpus-size-dependent wall-clock deadline into an index failure.

### 14.2 Incremental catch-up

Ordinary catch-up consumes routed journal records in source order, coalesces
safe repeated mutations of one path, projects a bounded wave into a new
immutable segment, updates affected locator and live-mask blocks, and publishes
one complete generation.

The published generation may contain bounded merge debt but may not omit a
settled relevant event. Journal capacity provides the required natural write
backpressure when builders cannot keep up.

### 14.3 Merge

Merge selection uses deterministic size tiers and explicit per-kind run and
byte thresholds. A merge:

- opens immutable inputs;
- rejects any input whose exact `(index_id, definition_version,
  schema_fingerprint)` differs;
- applies their generation-pinned live masks;
- merges term dictionaries, postings, columns, stored fields, positions,
  vectors, and locator entries in deterministic key ranges;
- assigns new segment-local DocIds;
- preserves the declared physical order;
- writes a replacement segment and all-live bitmap;
- verifies checksums, counts, ordering, exact match membership, and kind
  invariants; and
- publishes a replacement generation CAS at the same or a newer complete
  source barrier.

Range-striped merge lanes share the per-kind byte budget. Effective lanes are
the minimum of configured lanes, CPU workers, affordable workspaces, and
available non-overlapping ranges. Publication remains single and atomic; no
range is visible independently.

Queued work holds no construction-memory permit until selected to run. A task
cannot retain a partial permit while waiting for another task of the same kind
to acquire the full budget.

## 15. Index-kind dry runs

### 15.1 Path

The term dictionary stores canonical paths and supports exact and prefix range
seek. Postings identify the current segment documents for a path term; the
common live masks remove overwritten or deleted versions. Prefix listing merges
ordered path iterators and can seek a continuation before reading identities.

No JSON, stored payload, vector, or full-text component is needed.

### 15.2 Metadata Filter

Fixed object-head fields receive stable definition-local FieldIds. Equality and
membership use term postings. Numeric and time ranges use ordered terms and
fast columns. Requested metadata values come from fast columns; path and object
version come from the identity table.

Known metadata types avoid dynamic JSON coercion. A query intersects all useful
postings before reading result columns.

### 15.3 Typed JSON

The streaming projector extracts only configured JSON pointers. Each scalar
type has a canonical tagged term lane and typed fast column. Arrays contribute
multiple terms and multi-value column entries. Missing and explicit null remain
different.

Equality, membership, prefix, exists, and ordered scalar ranges create
advanceable posting iterators. Compound filters intersect them. Arbitrary order
uses fast-column top-K; a matching optional physical order permits early
termination. Final requested values are materialized only for selected hits.

### 15.4 Full Text

The projector tokenizes configured fields into term postings, frequencies,
positions, field lengths, and norms. Boolean term queries use the common
iterator algebra. Phrase queries use conjunction as their approximation and
positions as exact verification. BM25 uses term statistics and norms; impact
blocks permit safe top-K skipping.

Stored snippets or projected fields are fetched only for final hits. The
source payload is not repeatedly decoded during scoring.

### 15.5 Vector

Vectors are fixed-width blocks aligned with common DocIds. Metadata or Typed
JSON indexes have different definition-local DocIds and are not silently joined
to this index. The current public Vector query therefore scores every present
live vector in bounded blocks and keeps a bounded top-K heap. A future native or
SQL planner may compose indexes only through stable object identities under a
separately specified plan; it cannot compare DocIds across definitions.

### 15.6 Hybrid

Lexical and vector components share the same segment DocIds, identity table,
and live masks because both components belong to the same Hybrid definition.
Lexical scoring produces bounded candidates; vector scoring reranks those
candidates or participates in the publicly defined exact fusion. Weighting and
normalization are generation metadata, not a second index. The public Hybrid
query does not gain Metadata or Typed JSON predicates under this RFC.

A future approximate vector sidecar can participate only through the same
generation, liveness, and authorization boundary.

### 15.7 Git Source

Repository, commit, tree path, Git object ID, and source identity form sorted
composite terms and typed columns. Exact repository/commit lookups seek
directly; tree traversal uses prefix enumeration. Stable Git object and pack
bytes remain ordinary Anvil payloads and are not copied into the index unless
explicitly projected as a stored field.

### 15.8 Tensor

The definition's model and the requested tensor name form the exact lookup key.
Shape, element type, source offset/length, and stable object identity use typed
columns or late stored fields for the returned record. Tensor payload bytes
remain ordinary objects. Shape and type do not become new public query filters
under this RFC.

All eight kinds therefore use the same identity, liveness, publication, cache,
budget, iterator, pagination, authorization, and scan foundations. A kind adds
only the components its semantics require.

## 16. DataFusion-ready scan boundary

The format-v4 engine exposes one internal async contract independent of any SQL
library:

```text
ScanRequest {
  generation
  authorization_scope
  projected_field_ids
  predicate_expression
  required_order
  after
  limit
  target_batch_bytes
}

ScanCapabilities {
  predicate_pushdown: map<PredicateId, Exact | Inexact | Unsupported>
  residual_expression
  per_partition_order
  globally_merged_order
  partitions
  estimated_documents
  estimated_bytes
  estimated_cached_bytes
  estimated_remote_bytes
}

ScanStream -> async stream<ScanBatch>
```

`PredicateId` is scoped to one `ScanRequest`; it is neither durable nor drawn
from a registry.

`ScanBatch` contains definition-local FieldIds, Anvil logical types, presence
and null information, stable object identities, and bounded column buffers. It
does not expose internal block ownership or DocIds beyond the engine boundary.

Every kind exposes the same reserved system columns for stable numeric tenant,
stable numeric bucket, path, and object version. Tenant and bucket are virtual
constants supplied by the authorized scan/definition scope rather than values
repeated for every DocId. Path and object version come from the segment identity
component. A future native planner may compose two index scans only through
that stable identity under an explicit plan; segment DocIds from separate
definitions are never comparable. This common identity is enough to design a
future join/intersection operator without adding one to the format-v4 release.

The contract provides the information a future DataFusion `TableProvider` and
`ExecutionPlan` need:

- projection pushdown;
- exact/inexact/unsupported filter pushdown;
- limit and ordering pushdown;
- a generation-pinned candidate view with read-committed exact-current
  validation;
- declared output ordering and partition count;
- per-segment and per-block statistics;
- selected HRW-owner locality plus advisory cached and remote byte estimates;
- bounded asynchronous batches; and
- mandatory authorization and liveness operators.

Reported partitions are local execution partitions on the selected HRW owner,
such as disjoint segment groups. They do not authorize DataFusion to scatter a
query across Anvil nodes or bypass the owner, generation, cache, and Zanzibar
boundaries. Ordering is reported per partition. A physically ordered
multi-segment scan either adds one local k-way merge and returns one globally
ordered stream or exposes the per-partition ordering and requires an explicit
local merge operator; a future adapter may not treat several ordered partitions
as one globally ordered result.

Native query admission supplies an already selected generation. A future SQL
provider performs only metadata planning before execution; it does not fetch
or repeatedly resolve a generation while constructing the logical plan. At
execution admission, the selected HRW owner resolves one current complete
generation if the request did not already pin one, then every batch in that
execution remains pinned to it.

The future SQL adapter inherits this read-committed contract. A row replaced
after the generation barrier may be removed by exact-current validation before
return, while its replacement waits for a later published generation. The scan
does not claim SQL snapshot isolation, repeatable read, or time-travel
semantics.

The future adapter translates DataFusion expressions to `ScanRequest`, consumes
`ScanBatch` values, and constructs Arrow `RecordBatch` values for the user
projection plus any columns required by an inexact residual expression. The
residual is evaluated before the final user projection. If Anvil performs that
bounded residual exactly, it advertises the predicate as `Exact` and need not
return its private verification columns. The adapter can remain in a separate
SQL-facing crate, keeping Arrow and DataFusion out of the store, consensus,
program, gateway, and native index crates.

Format v4 promises semantic convertibility, not zero-copy Arrow compatibility.
If a future fast-column block can be wrapped by an Arrow buffer without copying,
that is an implementation optimization. Alignment, compression, variable-width
offsets, and null representation may still require conversion and never become
an authoritative-format promise.

## 17. Apache Arrow assessment across Anvil

Arrow is designed for tabular analytical locality, vectorized processing, and
interchange. Its record-batch representation does not match many Anvil
authority access patterns. The clean format break is not a reason to use one
representation where the access patterns do not match.

| Anvil bytes | Access pattern | Arrow decision | Reason |
| --- | --- | --- | --- |
| Terms and postings | Exact/range seek, skip, intersect, score | Do not use | Arrow supplies no term dictionary, posting advancement, skip data, positions, impacts, or liveness |
| Fast index columns | DocId block reads, range verification, top-K | Anvil codec on disk; Arrow adapter later | Specialized compression and small random block reads matter more than generic IPC; selected bounded columns convert naturally |
| Stored index fields | Late sparse materialization | Do not persist as Arrow | Sparse lookup needs per-DocId offsets plus independently compressed, checksummed, range-addressable blocks; IPC adds no useful locator |
| Live masks | Single-bit tests and bitmap algebra | Do not use | Anvil needs generation-rooted immutable block reuse, independent checksums, and range fetches; an IPC envelope adds no authority or query benefit |
| Segment identities, path locators, and pack descriptors | Stable identity resolution, overwrite/delete lookup, and checked range addressing | Do not use | These are seekable mappings and small descriptors with independent version and integrity rules, not analytical columns |
| Vector blocks | Fixed-width candidate scoring | Anvil fixed-vector codec | Blocks need Anvil range descriptors, integrity/versioning, and room for future quantization; selected runtime vectors can still be wrapped or converted |
| Segment/generation manifests | Small pointer-rich control records | Do not use | Explicit bounded binary records are smaller and independently versionable |
| Ordinary small blobs | Opaque content-addressed bytes up to 64 KiB | Never reinterpret | The payload belongs to the client and is deliberately stored directly in RocksDB |
| Large complete replicas, staging files, and erasure fragments | Opaque streaming bytes, hashing, coding, deduplication | Never reinterpret | Arrow cannot improve object identity, restart-safe staging, streaming, erasure coding, or reference counting |
| RocksDB heads and versions | Individual point read/update and prefix iteration | Do not use | Grouping records requires rewriting a batch, while one-record IPC batches impose disproportionate schema/envelope overhead |
| Name mappings and blob-reference counts | Tiny fixed or scalar values | Do not use | Existing fixed-width encoding is already the suitable shape |
| Tenant/bucket/options/policy/authentication records | Small exact control-plane point/range records | Do not use | Independent mutable authority and prefix lookup need bounded records, not analytical batches |
| Node identity, peer TLS overlap, and join/bootstrap artifacts | Small private identity and operator-carried security records | Do not use | Explicit bounded records and mode-0600 operator artifacts need exact validation and lifecycle, not batching |
| Zanzibar realm/tenant revisions, schemas, tuples, and bindings | Security-sensitive exact point/range evaluation | Do not use | Authorization authority needs independently versioned records and exact key access |
| Mutation/authz receipts and reference lifecycle evidence | Idempotency windows, exact proofs, counters, and bounded pruning | Do not use | Each proof has its own identity and lifecycle; columnar grouping adds no evaluation benefit |
| Definition locators, assignments, and consumer checkpoints | Exact discovery, disposable placement projection, and durable retention evidence | Do not use | Their independent keys, fences, and update cadence do not form an analytical batch |
| Mixed local control metadata | High-watermarks, source epochs/counters, bootstrap/program state, and bounded consumer evidence | Do not use | Independently updated scalar and small records in `CF_METADATA` need point access and atomic RocksDB batches |
| Source journal and sparse routes | Atomic append with mutation/routes; later settlement; per-offset pruning and prefix seek | Do not use | Primary records are heterogeneous versioned transitions and routes are empty ordered keys; RocksDB already expresses atomicity and retention |
| Raft protocol log/metadata, applied recovery journal, and bounded state-machine snapshots | Consensus, replay, and compact cluster/atomic state | Do not use | Arrow would inflate latency-sensitive non-tabular records and add no query benefit |
| Atomic programs | Bounded JSON DSL plus opaque ordinary-object results | Do not use | JSON Pointer/Patch semantics and opaque outputs are not columnar tables |
| Accounting definitions, rollups, and traffic-source checkpoints | A few exact counters, scope records, and freshness checkpoints | Do not use | Scalar point updates and small ordinary objects are already direct |
| S3, Git, PersonalDB, and public gRPC payloads | Native protocol or opaque bytes | Do not use | Gateways must preserve their public wire contracts |
| Disposable index cache, construction scratch/spills, and Git materialization cache | Exact artifact copies or reconstructible working data | No persistent Arrow contract | These bytes follow the component, sorter, or native Git access pattern which created them and never define storage authority |
| Peer snapshot transfer | Homogeneous bounded record stream | Defer pending evidence | Retain the current protocol until a representative transfer benchmark demonstrates a material compression, CPU, or transfer benefit |
| Future SQL execution/results | Bounded projected columns | Use at adapter boundary | This is Arrow's intended in-memory and interchange workload |
| Disposable SQL/query cache | Transient selected columns | Arrow is allowed at the future adapter | Cached Arrow arrays may accelerate one SQL process but remain disposable and never define storage authority |

Persistent Arrow would be justified during this clean break if it removed a
substantial custom subsystem or produced a measured material reduction in
stored bytes, remote ranges, decode CPU, or query memory without weakening
independent block replacement and checksumming. It does none of those for terms,
postings, liveness, sparse stored fields, point records, or opaque payloads.

Primitive fast-column and contiguous vector blocks are the closest candidates:
their decoded buffers can resemble Arrow arrays. Persisting Arrow IPC still
would not provide their term access paths, DocId-range routing, specialized
compression, generation identity, integrity envelope, or future vector
quantization. The native codec therefore stores only the buffers the native
engine needs. A future SQL-facing crate may depend on the modular `arrow-array`
and `arrow-schema` crates and wrap compatible uncompressed aligned buffers or
convert selected bounded blocks. That captures the practical Arrow benefit
without importing Arrow into the store or making its layout an Anvil durability
contract.

Several RocksDB column-family values currently use JSON serialization. Repeated
field names and byte arrays can cost CPU and uncompressed write bytes at very
large object counts, but Arrow is not a suitable replacement. RocksDB already
provides block storage and compaction; these records need compact individual
encoding, not record batches.

Changing every object, authorization, receipt, and journal record codec would
touch unrelated correctness and trust paths without fixing the measured query
failure. This RFC therefore does not expand the format-v4 index replacement into
a store-wide record rewrite. A separate proposal may define a store-owned,
versioned, bounded-record envelope informed by the consensus crate's private
framed fixed-integer encoding, but only after a representative benchmark
reports:

- encoded bytes before and after RocksDB compression;
- encode/decode CPU and allocation rate;
- point-read and write-batch latency;
- WAL and compaction bytes; and
- operational benefit large enough to justify changing those authority paths.

That evidence requirement is important even on new volumes: a clean break
removes migration cost, but it does not remove implementation and correctness
risk.

## 18. Resource control and cache

One shared construction budget remains configured per index kind for the whole
process, not per definition. Builders, projection waves, sorters, posting
writers, live-mask writers, pack buffers, and merge lanes acquire exact or
conservative byte permits from it. Fair scheduling prevents one tenant,
bucket, or definition from monopolizing a kind.

Long-lived projection and merge lanes are async orchestration tasks outside the
Rayon pool. They acquire work and memory, submit one finite leaf CPU chunk, await
its result without occupying a Rayon worker, and then schedule the next phase.
A leaf performs an admitted in-memory sort itself or returns before the
orchestrator submits later sorter/codec work. No Rayon leaf blocks on work sent
back to the same pool. Projection lanes may therefore equal Rayon workers
without relying on an operator to reserve one otherwise-idle worker.

One process index-cache pool retains its configured disk and memory budgets.
HRW owners materialize immutable ordinary artifacts into this disposable cache.
The async handle reads logical ranges and transparently fetches missing packs;
it may prefetch sequential posting, column, or stored-field blocks from planner
hints. Open handles pin only the exact cached entries they use. Eviction never
removes authoritative bytes.

Query execution has a separate bounded memory allowance for decoded posting
blocks, iterator heaps, top-K state, fast-column pages, authorization batches,
and returned fields. A query which cannot acquire its declared maximum waits or
returns a resource error within the normal server deadline; it does not grow
the process implicitly.

### 18.1 Startup defaults

Resource values are non-zero startup configuration, may change on restart, and
are not Raft or durable-format state. Defaults are:

- process Rayon workers: four;
- construction memory: 256 MiB shared by all definitions of each index kind;
- projection lanes per kind: four;
- source quantum per kind: 16 MiB;
- external-sort chunk per kind: 16 MiB when affordable;
- merge lanes per kind: four;
- merge debt per kind and level: 64 segments and 1 GiB encoded bytes;
- disposable index-cache disk: 10 GiB;
- disposable index-cache memory: 10 percent of process memory;
- concurrent queries: 64;
- query CPU work quantum between cooperative yields: 4 MiB;
- global query working memory shared by all kinds: 512 MiB;
- retained generations: three, 24 hours, or 50 GiB, whichever bound is reached
  first; and
- index-query server maximum: 300 seconds, clamped by a shorter valid client
  deadline; other bounded unary APIs retain 30 seconds.

Each kind's memory, projection, sort, merge, and debt values can be overridden
independently. Effective lanes are reduced to available workers, work, and
affordable workspace; a smaller budget never permits hidden allocation. With
all eight default construction pools saturated, their explicit aggregate is
2 GiB.

Invalid zero values, a memory percentage outside 1..=100, a segment/debt count
above a fixed format bound, or a budget which cannot fit one required workspace
fails startup and names the exact setting. Heterogeneous nodes may use different
local budgets without changing durable bytes or query results.

### 18.2 Recovery and maintenance

| Failure | Required behavior |
| --- | --- |
| Crash during build or merge | Discard disposable scratch, keep serving the last complete generation, and resume/restart work for the same ordinary definition version |
| Journal reaches its entry or byte cap | Prioritize consumers and hold new source-producing commits until published progress permits safe pruning; never truncate or switch to bulk rebuild |
| Required index cursor is below the retained floor or has an unavailable epoch | Fail that definition closed until an authorized principal explicitly rebuilds, repairs, or deletes it |
| Accounting cursor is below the retained floor or has an unavailable epoch | Build only that accounting definition's bounded scoped baseline; publish it before advancing retention |
| Artifact upload or current-pointer CAS fails | Keep the prior complete generation; unreachable immutable artifacts become retention candidates after the 24-hour safety age |
| A v4 component, pack, manifest, live mask, locator, or scoring-stat block is corrupt | Fail that definition closed and alert; never broaden into a corpus scan or an older format reader |
| A disposable cache entry is missing or corrupt | Evict and refetch the exact ordinary object/version and checked range |
| Membership fence changes before publication | Cancel the candidate, recompute weighted-HRW ownership, and resume on current rank zero |
| Receipt capacity is exhausted | Prune expired receipts within a bounded budget and wait before commit; preserve the configured retry window |
| Journal capacity blocks the complete publication which would release history | Admit only that exact trusted publication as progress debt, keep ordinary writes blocked, publish, then prune and repay |
| Merge debt reaches its configured bound | Stop adding debt, merge under the kind budget, and allow the journal cap to backpressure writes if catch-up cannot progress |

Artifact retention, cache reconciliation, definition assignment reconciliation,
source proof cleanup, receipt expiry, and journal pruning use bounded record,
byte, and time work with resumable scoped cursors. Core object service starts
before them. No recovery or maintenance path triggers a global object-head,
blob, artifact, definition-payload, or cache-file scan.

## 19. Statistics and cost model

Every segment records enough metadata to plan without opening every data block:

- total and live document counts;
- per-field presence, null, and value counts;
- per-term document frequency;
- per-column minimum and maximum by block;
- physical-order bounds;
- posting, position, vector, column, and stored-field encoded bytes;
- decoded byte upper bounds;
- vector dimensions and present count; and
- full-text token, frequency, norm, and impact summaries.

Correctness-bearing bounds are distinct from advisory cost estimates. Block
first/last keys, column minimum/maximum, physical-order bounds, live counts,
segment-core scoring inputs, and conservative impact maxima are checksummed
index data. Writers verify them against emitted records, readers validate their
ordering and range, and corruption fails the definition closed. Only these
exact or conservative bounds may prune a block, terminate an ordered scan, or
skip scoring work.

Advisory estimates include expected selectivity, cache cost, decoded CPU, and
prefetch benefit. They choose iterator order, top-K strategy, and scheduling.
An inaccurate advisory estimate may make a plan slower but cannot omit a block,
predicate, or candidate; exact iterators and verification remain authoritative.

Statistics are also exposed to the future native Anvil planner. A DataFusion
adapter consumes the same estimates rather than inventing a second topology or
index catalogue. The native cost includes selected HRW owner, local cache
residency, missing ordinary artifact ranges, expected remote bytes, and bounded
fetch concurrency. These locality values are advisory scheduling costs only;
they cannot move execution away from an authorized owner or change exact query
semantics.

## 20. Freshness and query result evidence

Queries use the latest complete generation available on the selected owner.
They return the existing freshness structure containing definition version,
generation identity, source checkpoint/barrier, publication time, and lag
evidence. Lag does not convert a valid partial-history result into
`INDEX_LAGGING`; the client decides whether freshness meets its needs.

The generation is the complete indexed candidate view through that barrier;
the exact-current result check is read committed. Consequently an update after
the barrier may temporarily remove the old identity without adding the new one
until the next generation. The returned freshness evidence makes that boundary
explicit rather than claiming a database snapshot.

Checkpoint-only progress is itself publishable generation evidence even when
no indexed document changes. Restart, reassignment, and compaction preserve that
zero-lag proof.

## 21. Observability

Low-cardinality metrics by index kind, phase, and level include:

- active, queued, waiting, completed, failed, and cancelled builds and merges;
- source records/bytes read and projected;
- posting blocks sought, advanced, decoded, skipped, and bytes read;
- term seeks and enumerated terms;
- candidate DocIds, conjunction advances, union heap operations, and two-phase
  verifications;
- live-mask blocks read and candidates rejected;
- invalidation-overlay offsets applied, resident bytes, cache drops, and
  candidates rejected;
- fast-column blocks/bytes read;
- stored-field blocks/bytes read;
- physical-order early termination and top-K documents inspected;
- cursor seeks and documents skipped before/after the cursor;
- authorization/exact-current batches, candidates checked, denied, stale, and
  refill work;
- cache hits, misses, fetched bytes, evictions, and pinned bytes;
- output hits, logical/physical read bytes, duration, timeout, and cancellation;
- generation age, source lag, publication time, and current merge debt;
- construction/query budget capacity, used bytes, waiting bytes, and permit
  duration; and
- journal occupancy, limits, backpressure, publication debt, and last consumer
  progress.

Stable tenant, bucket, definition, generation, and segment identities are trace
fields, never metric labels. One query trace records the normalized plan,
estimated costs, chosen iterator order, physical-order decision, residual
verification, logical bytes by component, cancellation progress, and actual
returned hits even when the caller deadline cancels the response.

Logs record definition failure, corrupt artifact identity, rebuild admission,
builder reassignment, generation publication, merge replacement, sustained
backpressure, and a concise slow-query plan summary. They do not print payloads,
credentials, private field values, or unbounded query text.

## 22. Complexity contract

For a segment with `D` documents, predicate posting sizes `P1..Pn`, `M` exact
Boolean matches, requested limit `K`, and returned stored bytes `R`:

- exact term lookup is logarithmic or succinct-dictionary seek plus one posting
  descriptor read;
- conjunction work is proportional to iterator advances and decoded posting
  blocks, led by the smallest useful posting rather than `D` or one arbitrarily
  chosen broad predicate;
- disjunction work is proportional to participating posting advances with a
  heap bounded by the number of terms;
- live filtering is constant-time per candidate after its bitmap block is
  present;
- a matching physical order may terminate after enough live authorized and
  current matches, but in the worst case may inspect every candidate; its
  working memory remains bounded;
- arbitrary ordering inspects and may authorization/current-check all `M`
  matches, reads their order fast-column values, and retains `O(K)` top-K state
  plus one bounded validation batch;
- stored-field decoding is proportional to selected/refill candidates and `R`,
  not a broad predicate's cardinality;
- physical-order page continuation seeks from the cursor and does not repeat
  the previous page's complete work; arbitrary-order top-K may re-enumerate
  `M` matches but does not reread their stored fields; and
- merge memory is bounded by admitted blocks and lanes, independent of total
  corpus size.

No fixed complexity promise can make an unindexed predicate or arbitrary exact
sort sublinear. The engine must identify that case explicitly instead of hiding
it behind a broad-scan fallback.

## 23. Validation

### 23.1 Codec and corruption tests

- golden vectors decode identically on AMD64 and ARM64;
- every component kind rejects bad magic, version, lengths, offsets, enum tags,
  checksums, and allocation claims;
- encode/decode round trips preserve missing versus null, dynamic scalar tags,
  arrays, Unicode, numeric boundaries, positions, vectors, and stable identity;
- no persisted byte stream depends on Rust or `sux` native layout; and
- fuzzed component readers remain within declared allocation bounds.

### 23.2 Query correctness

- every predicate works alone and in `AND`, `OR`/`IN`, and exclusion forms;
- conjunction results equal a trusted in-memory evaluator for varied predicate
  selectivity and order;
- updates and deletes clear old DocIds without per-candidate head reads;
- post-generation overwrite/delete overlays suppress stale candidates, and a
  missing/behind overlay still returns no stale version because the bounded
  exact-current result check refills correctly;
- pre- and post-merge exact match membership and stored values are identical;
  scores and score order may change only in the newly published generation as
  documented, while an active cursor remains stable on its retained generation;
- physical and top-K order agree, including missing, null, direction, and stable
  tie-breaks;
- second and later pages neither repeat nor omit results and seek before stored
  materialization;
- authorization denial refills from the same iterator without leakage;
- checkpoint-only progress preserves freshness across restart; and
- atomic-program paths become visible together at one generation CAS.

### 23.3 Production-shaped regression

A public-API qualification builds at least 800,000 Typed JSON objects with the
same selectivity shape as the incident query:

- one equality predicate matches nearly every document;
- two Boolean predicates narrow it;
- one three-value membership predicate narrows it further;
- two-field descending/ascending order requests four results; and
- pagination requests consecutive 999-result pages.

The verifier compares exact results and order with an independent evaluator.
It records ingest and index rates separately and reports posting advances,
candidate DocIds, liveness checks, fast-column reads, stored-field reads,
logical and physical bytes, cache state, CPU, memory, and elapsed time.

Acceptance requires:

- no 300-second timeout;
- no full stored-field or source-payload scan;
- no logical-read amplification approaching one complete index per small page;
- the regression definition declares its matching physical order, and page two
  performs a real cursor seek without repeating page one's complete candidate
  work;
- compaction changes performance shape only within documented bounds, never
  changes exact match membership, and changes scoring only for a newly
  published generation under the documented scoring semantics; and
- peak memory remains inside configured construction, query, and cache budgets.

Tests must also include zero-hit sparse conjunctions, a deliberately unselective
arbitrary sort, cold and warm cache, multiple segments, concurrent queries,
restart, builder failover, and source-journal backpressure. A forced-spill build
and merge with projection lanes exactly equal to Rayon workers proves that no
nested same-pool starvation is possible.

### 23.4 All-kind and distributed matrix

Each of the eight kinds is created, built, queried, updated, deleted, rebuilt,
restarted, and merged through the public API on one node and a three-node Docker
cluster. Tests use independent buckets in one cluster. They verify HRW proxying,
artifact durability, owner failover, Zanzibar denial/public-read behavior,
freshness evidence, pagination, and exact results.

## 24. Clean-break removal list

The format-v4 implementation deletes, rather than wraps:

- format-v3 component and manifest readers/writers;
- range-local ordinal persistence and cross-run latest-live probing;
- single-driver Typed JSON and Metadata query execution;
- broad ordered-query fallback and post-scan continuation filtering;
- per-candidate stored-row decoding before Boolean filtering;
- query paths whose pagination restarts from the beginning;
- format-v3 merge and compaction code;
- compatibility branches, feature flags, converters, and dual-write tests; and
- known limitations which excuse the absence of posting intersection.

Retained source-journal, definition, HRW, authorization, object publication,
cache, accounting, gateway, and Raft code is changed only where required to
connect to the new format-v4 engine.

## 25. Known limitations

- Vector search remains exact and linear in the live filtered vector set. ANN
  requires a later accepted design.
- One query executes on one HRW owner. Very large indexes may fetch ordinary
  artifact packs over the cluster, but Anvil does not scatter the query or merge
  network result sets.
- Arbitrary order without a matching physical order may inspect every exact
  Boolean match on every page and may authorize and exact-current-check every
  match, although it reads fast columns rather than stored fields or payloads.
- Full-text merge may slightly change scores and score order in the newly
  published generation because BM25 statistics are aggregated from that
  generation's immutable segment set. Exact Boolean membership is unchanged,
  and an active page token remains on its retained generation.
- Dynamically typed JSON does not automatically become a conventional SQL
  column. A future SQL capability must define its exposure explicitly.
- Arrow, DataFusion, SQL, Flight, and IPC are not user-visible capabilities in
  this release.
- A permanently failed definition may eventually hold journal capacity and
  backpressure affected writes until an authorized principal repairs, rebuilds,
  or deletes it. This preserves index visibility correctness.
- Capacity and lane configuration changes on restart.
- Mesh and regional coordination remain outside the single-cluster design.

## 26. Consequences

The new engine is a major internal refactor, but it removes the architectural
cause of multi-index-sized reads for sparse compound queries. Common DocIds,
live masks, advanceable postings, fast columns, late materialization, and one
planner serve every index kind instead of letting each rediscover filtering,
pagination, authorization, and cache behavior.

Anvil retains ownership of its durable format and can tune codecs, remote block
layout, cache hints, compaction, and future ANN structures for its
inline/erasure-coded object architecture. Following Lucene's stable execution
contracts avoids inventing an unproven search model without binding Anvil to a
foreign storage format.

The DataFusion-ready scan boundary avoids a future SQL gateway having to bypass
the native planner or rebuild authorization, liveness, and topology knowledge.
Deferring Arrow to that adapter keeps today’s core dependency and persistence
surface small while preserving bounded conversion to the industry-standard
analytical representation when it has an actual consumer.

The clean break costs one rebuild from authoritative ordinary objects. It adds
no migration subsystem and leaves existing source data, object durability,
Zanzibar authority, journals, Raft state, gateways, and accounting semantics
unchanged.

## 27. References

- Apache Lucene, [index package and segment model](https://lucene.apache.org/core/10_3_0/core/org/apache/lucene/index/package-summary.html).
- Apache Lucene, [`DocIdSetIterator`](https://lucene.apache.org/core/10_3_2/core/org/apache/lucene/search/DocIdSetIterator.html).
- Apache Lucene, [`DocValuesType`](https://lucene.apache.org/core/10_3_2/core/org/apache/lucene/index/DocValuesType.html).
- Apache Lucene, [`TwoPhaseIterator`](https://lucene.apache.org/core/10_3_2/core/org/apache/lucene/search/TwoPhaseIterator.html).
- Apache Lucene, [`IndexOrDocValuesQuery`](https://lucene.apache.org/core/10_3_2/core/org/apache/lucene/search/IndexOrDocValuesQuery.html).
- Apache Lucene, [`IndexWriterConfig` index sorting](https://lucene.apache.org/core/10_1_0/core/org/apache/lucene/index/IndexWriterConfig.html).
- Apache Lucene, [`IndexSearcher.searchAfter`](https://lucene.apache.org/core/10_3_2/core/org/apache/lucene/search/IndexSearcher.html).
- Apache Lucene, [`Lucene103PostingsFormat`](https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/codecs/lucene103/Lucene103PostingsFormat.html).
- Apache Lucene, [`ImpactsEnum`](https://lucene.apache.org/core/10_3_2/core/org/apache/lucene/index/ImpactsEnum.html).
- Apache Lucene, [`WANDScorer`](https://github.com/apache/lucene/blob/releases/lucene/10.3.2/lucene/core/src/java/org/apache/lucene/search/WANDScorer.java).
- Apache Arrow, [columnar format](https://arrow.apache.org/docs/format/Columnar.html).
- Apache Arrow Rust, [IPC support](https://arrow.apache.org/rust/arrow_ipc/index.html).
- Apache DataFusion, [custom data sources and table providers](https://datafusion.apache.org/library-user-guide/custom-table-providers.html).
- Gonzalo Navarro and Veli Mäkinen, [Compressed Full-Text Indexes](https://users.dcc.uchile.cl/~gnavarro/ps/acmcs06.pdf), ACM Computing Surveys 39(1), 2007.
- Sebastiano Vigna, [Quasi-Succinct Indices](https://vigna.di.unimi.it/ftp/papers/QuasiSuccinctIndices.pdf), WSDM 2013.
- Sebastiano Vigna et al., [`sux`](https://github.com/vigna/sux-rs).
