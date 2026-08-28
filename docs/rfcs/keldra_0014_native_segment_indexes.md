# KELDRA-0014: Keldra-Native Segment Indices

Status: Accepted

Supersedes: KELDRA-0013 in full

Audience: Keldra implementors, operators, client authors, and reviewers

## 1. Decision

Keldra will replace format-v3 indices with an Keldra-owned format-v4 segment
engine. The engine adopts the proven execution contracts used by Lucene:

- immutable segment cores with mutable behavior presented through new segments
  and generation-bound live-document views;
- dense segment-local document identifiers;
- seekable term dictionaries and blocked, advanceable postings;
- cost-led Boolean iterator algebra instead of one-driver candidate scanning;
- field types and explicitly declared query capabilities which compile only
  the postings, points, doc values, positions, norms, or vectors they need;
- typed, independently readable doc values for ordering, faceting,
  aggregation, and scoring;
- optional definition-time physical ordering for workloads which repeatedly
  request the same leading order;
- true generation-bound search-after pagination; and
- background segment merging which preserves exact match membership while a
  newly published generation may recompute ranking statistics.

Lucene is the reference for these contracts and their operational maturity. It
is not Keldra's storage authority, file format, library dependency, or public
API. Keldra owns every durable byte and keeps its existing distributed object,
publication, authorization, and resource-control boundaries.

Format v4 is a clean break. It has no format-v3 reader, converter, backfill,
dual writer, query fallback, compatibility shim, or mixed-generation path. A
format-v4 deployment builds indices from authoritative ordinary objects and
their retained source journals.

Format-v4 postings, points, live masks, terms, doc values, vectors, generation
manifests, and pack envelopes use explicit Keldra codecs matched to their native
access patterns. An index never stores a second copy of an ordinary source field
merely to return it in a hit; a client retrieves the authoritative object
through `GetObject` or `BatchGet`. The engine defines a generation-pinned scan
contract so a future SQL gateway can push predicates, ordering, aggregation,
and limits into the same authorized native planner rather than bypassing it.

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
6. every selected Typed JSON field is unnecessarily emitted as a term, a
   generic column, and a stored JSON value whether or not those capabilities
   were requested.

A later production qualification exposed a second format-bound cost on a
healthy four-disk rotational RAID10 array. While source ingestion supplied
about 6.5 MiB/s of encoded documents, Keldra issued about 32.7 MiB/s of
kernel-accounted writes, about 81 MiB/s of userspace reads, and roughly 671
write syscalls per second. The array remained 86--91 percent busy while index
progress fell substantially behind ingestion. Logs were negligible and the
filesystem had ample free space.

The native builder had accidentally made each logical component stream its
own ordinary-object durability boundary. A field-rich index therefore sealed
and synchronously published many underfilled tail packs: terms, postings,
points, doc values, identities, live masks, locators, norms, statistics, and
routing layers could each leave a separate object. This defeated the intended
16 MiB packing and multiplied staging-file synchronisation, RocksDB lifecycle
commits, directory synchronisation, ordinary-object commits, and journal
entries. Rotational storage made the regression especially visible, but the
write amplification is wrong on every medium.

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
- make result ordering use typed doc values or a declared physical order
  without loading ordinary payloads;
- bound query, build, merge, cache, and authorization memory;
- retain complete source-journal, atomic-program, publication, durability,
  Zanzibar, and backpressure guarantees;
- support all eight public index kinds through one common segment foundation;
- keep authoritative artifacts in the ordinary inline or erasure-coded object
  path;
- make every persistent codec portable across AMD64 and ARM64 and independent
  of Rust memory layout;
- expose enough statistics and scan semantics for a future cost-based Keldra
  planner and SQL gateway; and
- prove logical read work, not merely wall-clock latency, in qualification.

## 4. Non-goals

This RFC does not add:

- a SQL public API or SQL runtime dependency;
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

**Stable document identity** is the total internal identity
`(result_path, result_version, source_path, source_version, source_record)`.
Ordinary one-document objects normally have the same source and result
identity with source record zero. Git Source and Tensor manifests may project
several documents which intentionally share one result object; their source
identity and deterministic `u32` record ordinal make those documents distinct.
This identity survives compaction and is used only for total ordering and
pagination. It is not a DocId and does not widen the public result identity.

**Live-document view** is the generation-selected bitmap which says which
DocIds in one segment still represent current visible object heads.

**Term dictionary** maps a canonical field/value term to posting metadata and
supports exact seek, prefix range seek, and ordered enumeration.

**Posting iterator** enumerates matching DocIds in ascending order and supports
`next`, `advance(target)`, and a conservative remaining-work estimate.

**Point value** is a typed, order-preserving value in a seekable point tree used
to find exact or ranged numeric candidates. It is not a returned source value.

**Doc value** is a typed, block-addressable column keyed by segment DocId. It is
used only for capabilities such as ordering, faceting, grouping, aggregation,
and scoring. It is not a stored copy of the source document.

**Field type** defines how one selected source value is validated, encoded,
compared, and, for text, analyzed.

**Field capability** is an operation promised by one field definition. The
initial capabilities are exact matching, prefix matching, range matching,
ordering, faceting, aggregation, and full-text search. A capability exists only
when named by the definition and backed by its required component.

**Physical order** is the optional definition-time document order applied
inside every segment. It is not a promise that segments form one globally
contiguous file; a query merges their ordered iterators.

**Scan batch** is a bounded internal batch of selected logical columns and
stable object identities. It is an Keldra-owned type.

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
6. Every durable index artifact is an ordinary Keldra object and reaches the
   required artifact durability before publication.
7. Raft contains no definition, source event, segment, posting, point, doc
   value, live mask, manifest, cursor, query state, or cache entry.
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
13. A matching physical-order continuation seeks before predicate and doc-value
    work. An arbitrary-order top-K continuation applies its bound while
    collecting, although it may necessarily rescan exact Boolean matches.
14. An index query never reads ordinary payload bytes to reconstruct source
    fields for a result. Hits identify the authoritative object and clients use
    the ordinary object API when they require its payload.
15. Authentication failure never degrades to anonymous access. Public reads
    still require the explicit Zanzibar public-read grant.
16. CPU-heavy build, merge, decoding, scoring, and collection do not run on or
    block the serving-fence and membership executor.
17. Startup work is proportional to this node's assigned definitions and
    retained cursors, never to all ordinary object heads, blobs, or cache files.
18. Builder, compaction, and query working memory is charged before use to one
    hard process-wide ceiling. Query and per-index-kind settings are fair-share
    planning targets: work may borrow currently idle capacity, but it cannot
    bypass queued mandatory work or exceed the aggregate ceiling. Parallelism
    cannot multiply the admitted grant silently.
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

The locator contains only stable numeric scope, definition identity, path,
object version, and whether that exact version is live or deleted. Deletion
replaces the one locator value with a tombstone; recreation replaces that same
key rather than appending history. The locator is discovery evidence, not a
definition payload, permission, registry, or second source of truth. A node
exact-reads and validates the matching ordinary live or deleted head before
acting on it.

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
barrier. Keldra adds no global traffic log or per-bucket Raft record.

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

The plural noun in the public contract is **indices**. The list operation is
`ListIndices(ListIndicesRequest) -> ListIndicesResponse`, and its repeated
result field is `indices`. Singular names such as `IndexService`,
`IndexDefinition`, and `CreateIndex` remain singular because they address one
index or the index capability itself.

### 8.1 Field type and capability

An index definition declares both the logical type of a field and the
capabilities required from it. Type determines validation, canonical encoding,
comparison, and analysis. Capability determines which persistent structures
are built and which query operations are accepted. Selecting a JSON pointer
does not implicitly grant every capability.

The initial field types are:

- `BOOLEAN`;
- `SIGNED_INTEGER`, an exact JSON integer in `i64` range;
- `UNSIGNED_INTEGER`, an exact JSON integer in `u64` range;
- `FLOAT`, a finite IEEE-754 binary64 value;
- `KEYWORD`, an uninterpreted UTF-8 value with binary UTF-8 collation; and
- `TEXT`, a UTF-8 value processed by its declared analyzer.

The initial capabilities are:

- `EXACT`, which permits equality, `IN`, and existence;
- `PREFIX`, which permits raw keyword-prefix matching;
- `RANGE`, which permits type-correct ordered comparisons;
- `ORDER`, which permits result ordering through doc values;
- `FACET`, which permits categorical or numeric facet collection;
- `AGGREGATE`, which permits supported numeric aggregation; and
- `FULL_TEXT`, which permits analyzed term and phrase matching.

Existence is available through every non-empty capability and does not require
a separate stored representation. Capabilities do not imply one another:
`EXACT` does not permit prefix, range, order, facet, aggregate, or full-text
operations. The same source JSON pointer may appear under several distinct
public field names when an application intentionally needs different
representations, such as an analyzed title and an exact title.

The valid initial combinations are:

| Field type | Valid capabilities |
| --- | --- |
| `BOOLEAN` | `EXACT`, `FACET` |
| `SIGNED_INTEGER`, `UNSIGNED_INTEGER`, `FLOAT` | `EXACT`, `RANGE`, `ORDER`, `FACET`, `AGGREGATE` |
| `KEYWORD` | `EXACT`, `PREFIX`, `RANGE`, `ORDER`, `FACET` |
| `TEXT` | `FULL_TEXT` |

`AGGREGATE` initially supports count, minimum, maximum, sum, and average on
numeric fields. `FACET` uses sorted or sorted-set doc values for keywords and
compact typed doc values for Boolean or numeric values. A future capability or
type extends this table explicitly; it cannot be smuggled through an unknown
enum value or a generic payload.

Integer sums use checked `i128` or `u128` accumulation and must fit the field's
declared `i64` or `u64` result domain. Count is `u64`. Float sums and all
averages must produce a finite JSON number. Overflow is a query error; values
are never saturated, wrapped, or silently discarded.

A facet counts one matching document once for each distinct facet value.
Numeric aggregates count every array element, including repeated values;
missing and explicit null values do not contribute.

### 8.2 Representative wire declaration

The wire contract uses one type choice and an explicit capability list. The
following is representative protobuf; exact field numbers are fixed by the
implementation patch which realizes this accepted contract:

```protobuf
enum IndexFieldCapability {
  INDEX_FIELD_CAPABILITY_EXACT = 0;
  INDEX_FIELD_CAPABILITY_PREFIX = 1;
  INDEX_FIELD_CAPABILITY_RANGE = 2;
  INDEX_FIELD_CAPABILITY_ORDER = 3;
  INDEX_FIELD_CAPABILITY_FACET = 4;
  INDEX_FIELD_CAPABILITY_AGGREGATE = 5;
  INDEX_FIELD_CAPABILITY_FULL_TEXT = 6;
}

enum IndexFieldCardinality {
  INDEX_FIELD_CARDINALITY_SINGLE = 0;
  INDEX_FIELD_CARDINALITY_MULTI = 1;
}

message KeywordIndexField {}
message BooleanIndexField {}
message SignedIntegerIndexField {}
message UnsignedIntegerIndexField {}
message FloatIndexField {}

enum TextAnalyzer {
  TEXT_ANALYZER_UNICODE_ALPHANUMERIC_LOWERCASE = 0;
}

message TextIndexField {
  TextAnalyzer analyzer = 1;
}

message IndexField {
  string name = 1;
  string json_pointer = 2;
  IndexFieldCardinality cardinality = 3;
  repeated IndexFieldCapability capabilities = 4;
  oneof field_type {
    BooleanIndexField boolean = 10;
    SignedIntegerIndexField signed_integer = 11;
    UnsignedIntegerIndexField unsigned_integer = 12;
    FloatIndexField float = 13;
    KeywordIndexField keyword = 14;
    TextIndexField text = 15;
  }
}

message TypedJsonIndexSpec {
  repeated IndexField fields = 1;
  repeated IndexOrder physical_order = 2;
}
```

Using a `oneof` makes a field's logical type unambiguous on the wire. The
server rejects a missing type, an empty or duplicate capability list, an
invalid type/capability pair, duplicate public field names, duplicate physical
order entries, an analyzer on a non-text field, or an order entry which lacks
`ORDER`. Raw protobuf callers therefore fail before definition publication;
they cannot create a partially usable definition.

This protobuf text-format declaration shows the complete shape of a real
request rather than an invented index DSL:

```protobuf
bucket: "intelligence"
name: "advisories"
path_prefix: "advisories/"
content_type: "application/json"
specification {
  typed_json {
    fields {
      name: "advisory_id"
      json_pointer: "/id"
      cardinality: INDEX_FIELD_CARDINALITY_SINGLE
      capabilities: INDEX_FIELD_CAPABILITY_EXACT
      keyword {}
    }
    fields {
      name: "ecosystem"
      json_pointer: "/ecosystem"
      cardinality: INDEX_FIELD_CARDINALITY_SINGLE
      capabilities: INDEX_FIELD_CAPABILITY_EXACT
      capabilities: INDEX_FIELD_CAPABILITY_FACET
      keyword {}
    }
    fields {
      name: "modified_at"
      json_pointer: "/modified_at_unix_millis"
      cardinality: INDEX_FIELD_CARDINALITY_SINGLE
      capabilities: INDEX_FIELD_CAPABILITY_EXACT
      capabilities: INDEX_FIELD_CAPABILITY_RANGE
      capabilities: INDEX_FIELD_CAPABILITY_ORDER
      capabilities: INDEX_FIELD_CAPABILITY_AGGREGATE
      signed_integer {}
    }
    fields {
      name: "summary"
      json_pointer: "/summary"
      cardinality: INDEX_FIELD_CARDINALITY_SINGLE
      capabilities: INDEX_FIELD_CAPABILITY_FULL_TEXT
      text {
        analyzer: TEXT_ANALYZER_UNICODE_ALPHANUMERIC_LOWERCASE
      }
    }
    physical_order {
      field: "modified_at"
      direction: INDEX_ORDER_DIRECTION_DESCENDING
    }
  }
}
command_id: "01J..."
```

This definition does not tokenize `advisory_id`, cannot prefix-match or order
by it, and stores no source JSON in the index. It builds exact postings for the
ID, exact postings plus facet doc values for ecosystem, numeric point/doc-value
structures for `modified_at`, and analyzed postings for `summary`.

### 8.3 Rust client builder

The Rust client must not make callers assemble the protobuf structure by hand.
It provides concrete field builders and typed capability states. A capability
method exists only for a field type which supports it; `physical_order` accepts
only an order token produced by a single-valued field with `ORDER`; and
`finish` consumes only a non-empty definition whose public field names are
unique.

The representative user API is:

```rust
let advisory_id = KeywordField::single("advisory_id", "/id")
    .exact();

let ecosystem = KeywordField::single("ecosystem", "/ecosystem")
    .exact()
    .facet();

let modified_at = SignedIntegerField::single(
        "modified_at",
        "/modified_at_unix_millis",
    )
    .exact()
    .range()
    .order()
    .aggregate();

let modified_at_desc = modified_at.descending();

let summary = TextField::single("summary", "/summary")
    .analyzer(TextAnalyzer::UnicodeAlphanumericLowercase)
    .full_text();

let request = TypedJsonIndexBuilder::new("intelligence", "advisories")
    .path_prefix("advisories/")
    .content_type("application/json")
    .field(advisory_id)
    .field(ecosystem)
    .field(modified_at)
    .field(summary)
    .physical_order([modified_at_desc])
    .finish(command_id)?;

client.create_index(request).await?;
```

This API has no generic `.capability(...)` escape hatch. `TextField` does not
offer `.exact()`, `BooleanField` does not offer `.range()`, and a field without
`.order()` cannot create an ascending or descending order token. Multi-valued
ordering is absent from the initial client API; a later explicit `MIN` or `MAX`
selector requires another accepted contract rather than an arbitrary element
choice. Each capability transition is consuming, so it cannot be declared
twice. `finish` is the only operation which can produce a `CreateIndexRequest`,
and it exists only after at least one field has been added. String/path
validation and duplicate-name checks remain fallible because Rust's type
system cannot prove properties of runtime strings; `finish` rejects them before
producing a request. Invalid type/capability/cardinality combinations are
unrepresentable.

Other official clients provide the same concrete field concepts and validate
before sending. The server remains the authority and repeats all checks because
raw gRPC clients are always possible.

### 8.4 Query admission and physical order

Query predicates remain equality, membership, prefix, less-than,
less-than-or-equal, greater-than, greater-than-or-equal, existence, and the
existing full-text operations. Query limits, generation freshness evidence,
facets, aggregates, and opaque page tokens are public concepts. Query admission
maps every requested operator to its required capability and rejects a mismatch
before opening an artifact:

```text
==, IN       -> EXACT
PREFIX       -> PREFIX
<, <=, >, >= -> RANGE
ORDER BY     -> ORDER
FACET        -> FACET
aggregate    -> AGGREGATE
text/phrase  -> FULL_TEXT
EXISTS       -> any declared capability
```

Physical order is definition-versioned. Adding, removing, or changing it
requires a new complete generation. Physical-order fields must be
single-valued and have `ORDER`. A builder fails a candidate generation with a
precise definition/data error if a field declared single-valued produces
multiple values; the previous complete generation remains published.

A query matches the physical order only when its complete explicit field and
direction list exactly equals the definition's declaration; both then append
the same implicit stable-identity tie-break. A proper prefix does not match:
`(a, b, identity)` is not ordered as `(a, identity)`. An empty query order also
does not match a non-empty physical declaration. Every non-matching order uses
the arbitrary top-K path.

Each field has one declared logical type, so there is no cross-type tagged
order. Numbers compare numerically, keywords use unsigned UTF-8 byte order, and
locale collation is outside format v4. `NaN`, infinity, or an out-of-domain JSON
number is a projection error. Missing and explicit JSON `null` remain distinct;
missing sorts last ascending and first descending. The stable document identity
is the final ascending tie-break in every physical or query order. Descending
reverses the selected field comparison but not that final uniqueness.

Arbitrary query ordering remains correct without an exactly matching physical
order. It uses bounded top-K collection over the field's doc values and may
inspect every document which satisfies the Boolean predicate. Matching
physical order permits early termination and is therefore the preferred
definition for recurring sparse ordered queries.

### 8.5 Value-size contract

The 512 KiB component ceiling is a block bound, not a scalar-size promise.
Format v4 adopts Lucene's 32,766-byte maximum for one raw keyword term, one
sorted or sorted-set keyword doc value, and one analyzed token. This raises the
current approximately 4 KiB raw term ceiling while deliberately lowering the
accidental near-component-sized generic-column value ceiling. Numeric and Boolean
values have fixed widths.

`EXACT` remains available for a keyword longer than 32,766 bytes. Its canonical
term is the scalar type, exact `u64` byte length, and BLAKE3 digest of the exact
UTF-8 bytes. Equality and `IN` compare that representation. This is the only
large-exact representation: it is consistent with Keldra's existing
content-addressed identity model and does not create a source-payload
verification path in the query engine.

A keyword longer than 32,766 bytes cannot participate in `PREFIX`, `RANGE`,
`ORDER`, or `FACET`. An analyzed source text may be larger because the analyzer
streams it into bounded tokens; an individual token may not exceed the limit.
An out-of-contract value fails that definition precisely and the index cannot
claim a complete barrier which omits it. It is never truncated, sampled,
silently hashed into an ordered operation, or copied into an exceptional jumbo
component.

`IndexQueryHit` contains the result object address, exact object version, and
optional score. It contains no `fields_json` or other source projection. Facet
and aggregation results are derived from declared doc values and returned in
their explicit result structures; clients retrieve ordinary source data with
`GetObject` or `BatchGet`.

No segment DocId, field ordinal, block address, or implementation structure is
added to the public API.

## 9. Format-v4 artifact envelope

### 9.1 Reserved namespace

Format v4 uses only these canonical reserved object paths:

```text
_keldra/indices/v4/definitions/<index_name>
_keldra/indices/v4/<index_id>/artifacts/<blake3_hex>
_keldra/indices/v4/<index_id>/manifests/<blake3_hex>
_keldra/indices/v4/<index_id>/current
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
interpret, convert, delete, or treat `_keldra/indices/v3/` objects as definitions
or artifacts.

Every path segment exactly equal to `_keldra` is reserved. Source events for
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

The exact fixed header is shared by term, posting, point, doc-value, position,
norm, vector, live-mask, and path-locator components. A
component-specific payload has its own explicit bounds and codec version.
Readers validate the fixed header, checked lengths, declared upper bounds, and
checksum before allocating or decoding.

Logical components are packed into immutable ordinary Keldra objects. Packing is
segment-scoped rather than component-stream-scoped: leaves, routing nodes, and
other logical components from different streams share the same current pack
until adding the next complete component would exceed 16 MiB. A component may
not straddle packs.

A checked component descriptor contains a segment-local `u32` pack ordinal,
byte offset, encoded length, logical length, component kind, codec version, and
checksum. It deliberately does not embed an ordinary-object path or object
version. This indirection lets routing components refer to already assigned
component locations before the enclosing packs are sealed and durably
published; otherwise every stream tail would have to become a separate object
before its routing parent could be encoded.

After the complete segment or standalone locator tree is encoded, the builder
seals its packs in ordinal order and publishes them through the ordinary
grouped object-publication path. Its pack table then maps every ordinal to the
canonical reserved object address, exact object version, content hash, and
object length. The table is carried by the segment descriptor or standalone
locator root in the generation manifest. Pack ordinals are meaningful only
with that exact segment identity and table. They are never global identifiers,
Raft state, mutable names, or another storage plane.

Readers validate the ordinal and component range against the generation-bound
pack table before resolving the ordinary object. The address and version retain
and resolve that object; the content hash provides integrity and ordinary
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
- one raw keyword term, sorted keyword doc value, or analyzed token is at most
  32,766 bytes;
- one routing node has at most 32 children and routing height is at most eight;
  and
- one segment contains at most `u32::MAX` documents and splits before assigning
  a DocId outside that range.

A posting, point, doc-value stream, or dictionary larger than one logical block
is split into independently checked blocks. The term dictionary routes a long
keyword through bounded radix fragments; it never repeats a complete 32,766-byte
term in one 4,096-byte routing key. Eight maximum routing fragments cover the
complete term bound. No record may straddle an artifact pack. Encoded counts
and lengths use checked arithmetic and no decoded allocation is derived from an
unvalidated value. These limits are format constants, not startup settings.

### 9.4 Schema fingerprint

The schema fingerprint is BLAKE3 with domain separator
`keldra.index.schema.v4`. Its input is one explicit length-prefixed canonical
binary encoding in this order:

1. index kind, path prefix, and content-type scope;
2. every field in canonical definition order, including FieldId, public name,
   source selector, logical type, cardinality, null/missing policy, collation,
   declared capabilities, and compiled components;
3. tokenizer/analyzer and full-text scoring semantics where applicable;
4. vector dimensions, metric, normalization, and hybrid weights where
   applicable;
5. Git repository or Tensor model scope where applicable;
6. physical order and stable tie-break semantics; and
7. every required component codec semantic version.

Integers use fixed-width little-endian encoding, enums use documented Keldra
tags, strings and byte fields use a `u32` byte length followed by exact bytes,
and lists use a `u32` count. Protobuf wire bytes, JSON serialization, map
iteration order, and observed segment statistics are not fingerprint inputs.
Any result-affecting definition or codec change therefore creates another
fingerprint and cannot merge with old segments. A rebuild which changes only
the definition object version may retain the same fingerprint; definition
version remains a separate merge-identity field.

### 9.5 Generation manifest and durability

The generation manifest is one ordinary immutable Keldra object, not a logical
index component. It therefore has no 512 KiB component-block ceiling. Small
manifests use the ordinary inline path and larger manifests use the same
complete-replica or erasure-coded payload placement as every other object. The
manifest codec has its own magic, version, exact payload length, checked
collection counts, and structural validation.

The ordinary object `BlobRef` is the manifest's sole content checksum. Keldra
hashes the bytes when staging the content-addressed object and verifies that
identity when reading a complete replica or reconstructing erasure shards. The
manifest does not nest a second checksum over the same bytes, and callers do
not rehash a payload already verified by the ordinary object reader. This does
not weaken packed components: their individual checksums remain necessary
because a query may range-read one component without reading its complete
ordinary pack object.

The generation manifest contains:

- definition identity and version;
- format version and schema fingerprint;
- complete source and atomic-program barrier;
- physical-order declaration, if any;
- ordered segment descriptors;
- each segment's canonical ordinal-to-ordinary-object pack table;
- each segment's live-view root;
- path-locator roots used by the builder, including a pack table when the root
  is not owned by a segment descriptor;
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

All packs produced by one completed segment or standalone locator build are
staged before their metadata publication and are submitted through the existing
bounded grouped publication path. Grouping changes physical RocksDB and network
work only: each pack remains an independently content-addressed ordinary object
with its normal durability, reference counting, placement, and authorization
semantics. The generation manifest is not published until every pack outcome
has been resolved into its canonical pack table.

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

Each builder node keeps a sparse durable due index in the existing definition
state column family. One primary record per assigned index identifies the exact
definition/current version and reason; one time-ordered secondary key makes the
oldest due work seekable. Updating or completing work atomically replaces or
removes both keys. Only the currently executing bounded job is resident in
memory, and its due record remains until exact completion, so delayed work does
not consume an active-job slot and a crash can resume it without an inventory.

Deleting a definition installs a `deleted_definition` due record before its
delivery checkpoint advances. Cleanup exact-validates the tombstone and HRW
fence, then scans only `_keldra/indices/v4/<index_id>/` under the normal safety
age and exact-version deletion rules. It never deletes the ordinary definition
tombstone and never touches another index in the bucket.

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
- one logical type: Boolean, signed integer, unsigned integer, finite
  IEEE-754 binary64, keyword, or analyzed text;
- definition-declared cardinality and the effective missing and explicit-null
  policy;
- comparison and collation semantics;
- the exact declared capabilities and the minimum compiled components which
  realize them;
- timestamp unit and timezone semantics when a future typed definition declares
  a timestamp;
- decimal precision and scale when a future typed definition declares a
  decimal; and
- whether the field participates in physical ordering.

Typed JSON definitions are not dynamically typed inside the index. One field
has one declared type across every source object and segment. A value outside
that type is a precise projection error; Keldra never coerces a string to a
number, truncates an integer, treats a Boolean as a number, or invents a tagged
cross-type order. Null is a value state, not a numeric or string type, and
missing remains a separate presence state. A future SQL API therefore receives
real logical types without inferring them from observed documents.

The initial Typed JSON public definition keeps that policy deliberately small:
missing and explicit null are both permitted. The canonical field catalogue
records and fingerprints those effective values even though callers cannot vary
them in this release. A later stricter policy would be an explicit public API
extension, not an inference from observed documents.

Observed occurrence counts, actual scalar tags, null counts, and multi-value
counts are segment statistics. They never alter the definition schema or its
fingerprint as new objects arrive.

One complete segment-statistics record must fit one checked 512 KiB component.
Definition admission computes its exact schema-weighted worst-case encoding
before allocating builder state and rejects a larger definition with the
required and supported byte counts. The bound admits at least 1,702 fields with
every currently supported field component and up to 5,088 minimal fields. A
larger real requirement needs a separately designed routed statistics codec;
it does not silently create an unbounded record.

Persistent type tags are Keldra constants. They are never protobuf, Rust, or
third-party enum discriminants.

## 11. Segment identity and liveness

### 11.1 Segment-local DocIds

A builder orders one segment's accepted projected documents deterministically
and assigns dense DocIds from zero. All segment components use those DocIds:
postings, points, doc values, full-text positions and norms, vectors, and live
masks.

The segment identity table maps DocId to the stable document identity. The
public result's `(path, object_version)` is reconstructed from its result
identity only for selected hits. Source identity and record ordinal remain
internal cursor tie-break data.

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
that budget cannot retain it, Keldra drops or partially rebuilds the overlay and
relies on the mandatory bounded exact-current result check.

Before returning results, Keldra exact-reads current heads in bounded multi-get
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

An inline keyword or token is at most 32,766 bytes. Prefix-compressed leaves
store complete terms while bounded radix routing consumes long common prefixes
across levels; a routing node never repeats a complete long term. A keyword
longer than the limit with only `EXACT` uses its canonical length-and-BLAKE3
term instead. Hashed exact terms are not eligible for ordered enumeration,
prefix matching, range matching, sorting, or faceting.

The implementation may use finite-state or succinct structures in memory, but
the durable bytes are an Keldra codec. `sux` structures may be reconstructed
from portable arrays or used behind an Keldra wrapper; their native serializer
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

The term reference records the inclusive maximum DocId of every posting block.
An `advance(target)` binary-searches those bounds and opens the first block
which can contain the target; it does not replay preceding blocks merely to
reach a later page. The opened block's actual maximum must equal its declared
bound. This is one flat four-byte bound per block, not an independently
authoritative skip tree. Readers of the preceding term-reference codec retain
the exact sequential path until normal rebuild or compaction emits the bounds.

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

### 12.3 Point values

Numeric `EXACT` and `RANGE` capabilities use a block point tree inspired by
Lucene's point-value separation. Signed integers, unsigned integers, and finite
binary64 values have canonical fixed-width order-preserving encodings. Leaves
contain bounded sorted `(value, DocId)` entries; internal nodes contain checked
value bounds and child descriptors. Exact, set, and range traversal therefore
find candidate DocIds without scanning a per-document column.

The same point component has a reserved presence-record subrange containing
exactly one DocId for each present source field, including explicit null. It is
not a numeric value and is excluded from numeric bounds and range statistics.
`EXISTS` seeks that subrange without forcing an otherwise-unused term component.

Single-dimensional points are sufficient for the initial field types. A future
multi-dimensional or geospatial type requires an accepted extension rather than
changing the meaning of this format. Point components do not exist unless a
numeric field declares `EXACT` or `RANGE`.

### 12.4 Doc values

`ORDER`, `FACET`, and `AGGREGATE` compile to independently addressable typed doc
values keyed by DocId range:

- single numeric fields use fixed-width, delta, monotone, or bit-packed numeric
  blocks;
- multi-valued numeric fields add one checked DocId-to-value offset stream;
- single keyword fields use one sorted distinct-value dictionary and one
  ordinal per present document; and
- multi-valued keyword fields use a sorted distinct-value dictionary, sorted
  ordinal lists, and checked per-document offsets.

Every representation has separate presence and explicit-null bitmaps where the
definition permits those states. Block statistics retain counts and numeric or
ordinal bounds. Keyword minimum and maximum are ordinals into values already
stored in the dictionary; the codec never serializes duplicate minimum and
maximum strings. A singleton 179 KiB keyword would therefore contain one value,
not three copies, although it remains ineligible for `ORDER` or `FACET` because
it exceeds the 32,766-byte ordered-key contract.

One compatible doc-value representation serves every declared capability which
can share it. `ORDER` plus `FACET` does not create two sorted keyword doc-value
components, and `ORDER` plus `AGGREGATE` does not create two numeric doc-value
components. A field without one of those capabilities has no doc values. Doc
values never retain an otherwise unneeded source field for result
materialization.

### 12.5 Full-text data

Full Text and Hybrid segments add token frequencies, positions, field lengths,
norms, and block impact bounds. BM25 scoring operates over these components.
Phrase and positional queries use a cheap term-conjunction approximation
followed by positional verification.

Multi-valued text analyzes every value and inserts a position gap between
values, so a phrase never matches across two array elements.

At query admission, BM25 aggregates document, field-length, and term-frequency
statistics from the bounded segment set of the pinned generation. Segment
statistics may conservatively include documents cleared by a later live mask
until those segments merge, matching the practical immutable-segment tradeoff:
exact match membership remains unchanged, while a newly published merged
generation may have slightly different scores and score order. A page token
continues on its retained generation, so ranking cannot change within one
pagination sequence.

Keldra does not add a generation-global term-stat registry, high-cardinality
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
bounded residual evaluation from the field's declared point, doc-value,
position, or vector components.

### 13.2 Boolean iterator algebra

For conjunction, the planner orders posting iterators by estimated cost. The
cheapest iterator advances normally; every other iterator advances directly to
that candidate. If one moves beyond it, the lead iterator advances to the new
DocId. Evaluation continues until all iterators agree or one ends.

`IN` and `OR` merge posting iterators with a bounded heap and suppress duplicate
DocIds. `NOT` advances an exclusion iterator alongside the positive iterator.
The live bitmap is another exact DocId filter. Two-phase predicates first
intersect their cheap approximation and only then run positional, doc-value,
or other exact verification.

The planner never falls back from a selective compound query to decoding every
document from a broad first predicate. If no useful indexed predicate exists,
it may perform an explicit bounded full-index scan only where the public API
already defines such a scan. The plan, reason, and estimated work are visible
in traces.

### 13.3 Ordering and top-K

When the complete explicit query order exactly equals the definition's physical
order, each segment produces candidates in that order. A one-head-per-segment
min-heap merges segment iterators in `O(log segments)` per advance, tests the
remaining predicates and live mask, and stops as soon as it has enough
authorized hits. Decoded state is retained in an admitted query-local LRU;
eviction drops only disposable decoded blocks and preserves each cursor's exact
logical position. A proper prefix, suffix, reordered list, direction change,
or empty order does not qualify.

Otherwise, Boolean execution produces matching DocIds and a bounded top-K
collector reads only the declared order doc values. Its memory is proportional to the
requested limit plus a bounded authorization refill window, not total matches.
It may inspect all matching DocIds because an arbitrary exact order has no safe
early-termination rule.

The final stable document-identity tie-break makes ordering deterministic
across segments and merges, including when one manifest projects multiple
records which point at the same result object.

### 13.4 Pagination

A page token remains opaque and is bound to caller, tenant, bucket, definition
identity and version, canonical query fingerprint, physical order, generation,
and relevant Zanzibar revision.

For physically ordered scans, search-after seeks each segment to the first key
strictly after the cursor. It does not reproduce page one's scan. For top-K
scans without a matching physical order, the collector rejects keys at or
before the cursor while reading doc values. That avoids retaining the earlier
page but cannot avoid re-enumerating exact Boolean matches: no ordered access
path exists from which it could safely seek.

A token never contains a DocId. Compaction can change DocIds without changing
the token's stable sort values and stable document-identity tie-break. The token
remains generation-bound so its candidate structures, scoring inputs, and
retained artifacts do not change during pagination. Exact-current validation is
still read committed and can remove a result object changed after that
generation's barrier.

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
8. coalesces complete logical components across streams into segment-scoped
   packs, publishes each completed pack set through the bounded grouped
   ordinary-object path using the topology-aware acknowledgement threshold in
   Section 9.5, then writes the manifest; and
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

Merge selection uses deterministic size tiers and explicit per-kind segment and
byte thresholds. A merge:

- opens immutable inputs;
- rejects any input whose exact `(index_id, definition_version,
  schema_fingerprint)` differs;
- applies their generation-pinned live masks;
- merges term dictionaries, postings, point trees, doc values, positions,
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
membership use term postings or exact numeric points. Numeric and time ranges
use point trees. Ordering, facets, and aggregates use only the doc values
declared for those capabilities; path and object version come from the identity
table.

Known metadata types avoid dynamic JSON coercion. A query intersects all useful
postings before reading required doc values.

### 15.3 Typed JSON

The streaming projector extracts only JSON pointers named by typed field
definitions. It validates the declared type and emits only the components
required by that field's capabilities. An exact keyword ID produces one term
and posting rather than token, doc-value, and source-projection copies. A
numeric range-and-order field produces points and numeric doc values. A text
field produces analyzed terms and the full-text components selected by its
definition. Arrays contribute entries only to a declared multi-valued field.
Missing and explicit null remain different.

Equality, membership, prefix, existence, ordered scalar ranges, text matching,
faceting, and aggregation are admitted only when the field declares the
corresponding capability. Compound filters intersect their advanceable
iterators. Arbitrary order uses doc-value top-K; a matching optional physical
order permits early termination. Hits contain stable result identities, not
copies of selected JSON.

### 15.4 Full Text

The projector tokenizes configured fields into term postings, frequencies,
positions, field lengths, and norms. Boolean term queries use the common
iterator algebra. Phrase queries use conjunction as their approximation and
positions as exact verification. BM25 uses term statistics and norms; impact
blocks permit safe top-K skipping.

The source payload is not copied into the index or repeatedly decoded during
scoring. A caller retrieves a selected hit through the ordinary object API.

### 15.5 Vector

Vectors are fixed-width blocks aligned with common DocIds. Metadata or Typed
JSON indices have different definition-local DocIds and are not silently joined
to this index. The current public Vector query therefore scores every present
live vector in bounded blocks and keeps a bounded top-K heap. A future native or
SQL planner may compose indices only through stable object identities under a
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
composite terms and capability-selected doc values. Exact repository/commit
lookups seek directly; tree traversal uses prefix enumeration. Stable Git
object and pack bytes remain ordinary Keldra payloads and are never copied into
the index.

### 15.8 Tensor

The definition's model and the requested tensor name form the exact lookup key.
Shape, element type, and source offset/length use typed components only if a
declared query capability needs them; stable object identity identifies the
result. Tensor payload bytes remain ordinary objects. Shape and type do not
become new public query filters under this RFC.

All eight kinds therefore use the same identity, liveness, publication, cache,
budget, iterator, pagination, authorization, and scan foundations. A kind adds
only the components its semantics require.

## 16. Future native scan boundary

Format v4 defines the internal contract a future SQL capability must consume.
The contract is a design boundary in `0.9.0`, not a user-visible SQL API or an
executable adapter added by this release:

```text
ScanRequest {
  generation
  authorization_scope
  required_doc_value_field_ids
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

`ScanBatch` contains only requested definition-local doc values, their Keldra
logical types, presence and null information, stable object identities, and
bounded column buffers. It does not expose source JSON, internal block
ownership, or DocIds beyond the engine boundary.

Every kind exposes the same reserved system columns for stable numeric tenant,
stable numeric bucket, path, and object version. Tenant and bucket are virtual
constants supplied by the authorized scan/definition scope rather than values
repeated for every DocId. Path and object version come from the segment identity
component. A future native planner may compose two index scans only through
that stable identity under an explicit plan; segment DocIds from separate
definitions are never comparable. This common identity is enough to design a
future join/intersection operator without adding one to the format-v4 release.

The contract provides the information a future SQL planner and physical scan
need:

- identity and declared-doc-value selection;
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
such as disjoint segment groups. They do not authorize a SQL engine to scatter
a query across Keldra nodes or bypass the owner, generation, cache, and Zanzibar
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

The future adapter translates SQL expressions to `ScanRequest`, consumes
bounded `ScanBatch` values, and evaluates any inexact residual from requested
doc values. Source-field projection is a separate ordinary-object fetch stage;
format v4 does not turn an index into another copy of the source. If Keldra
performs a bounded residual exactly, it advertises the predicate as `Exact`.
The adapter remains outside the store, consensus, program, gateway, and native
index crates.

## 17. Resource control and cache

One hard aggregate working-memory ceiling covers query execution and all eight
index kinds. The query setting and each kind's construction setting remain
fair-share planning targets, not isolated heaps. Builders, projection waves,
sorters, posting writers, live-mask writers, pack buffers, merge lanes, and
queries acquire exact or conservative byte permits from the shared parent.
The global FIFO admits mandatory work before optional borrowing. An active
bounded loan is not revoked; it is returned at the existing query, build-turn,
or compaction boundary. Catch-up, rebuild, and segment compaction derive their
actual workspace from the granted bytes. Locator-root compaction is globally
charged but requests only its fixed fair-share workspace because a larger
grant cannot accelerate that path.

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
it may prefetch sequential posting, point, or doc-value blocks from planner
hints. Open handles pin only the exact cached entries they use. Eviction never
removes authoritative bytes.

Query admission distinguishes mandatory memory from preferred decoded-state
residency. A query waits for its mandatory cursor, heap, response, authorization,
and minimum decoded workspace, then borrows immediately idle aggregate capacity
up to its preferred segment residency. A physically ordered query evicts and
reloads decoded segment state within the grant when every segment cannot remain
resident. A mandatory request may exceed its query fair share when the aggregate
ceiling can admit it; only the aggregate ceiling is a hard rejection boundary.

### 17.1 Startup defaults

Resource values are non-zero startup configuration, may change on restart, and
are not Raft or durable-format state. Defaults are:

- process Rayon workers: four;
- construction memory: 256 MiB shared by all definitions of each index kind;
- projection lanes per kind: four;
- source quantum per kind: 16 MiB;
- external-sort chunk per kind: 16 MiB when affordable;
- merge lanes per kind: four;
- merge debt per kind and size tier: 64 segments and 1 GiB encoded bytes;
- disposable index-cache disk: 10 GiB;
- disposable index-cache memory: 10 percent of process memory;
- concurrent queries: 64;
- query CPU work quantum between cooperative yields: 4 MiB;
- query working-memory fair share: 512 MiB;
- aggregate query/build/compaction working-memory ceiling: the checked sum of
  the query and eight per-kind fair shares, 2.5 GiB with defaults, unless the
  administrator explicitly sets `KELDRA_INDEX_WORKING_MEMORY_BYTES`;
- retained generations: three, 24 hours, or 50 GiB, whichever bound is reached
  first; and
- index-query server maximum: 300 seconds, clamped by a shorter valid client
  deadline; other bounded unary APIs retain 30 seconds.

Each kind's memory, projection, sort, merge, and debt values can be overridden
independently. Effective lanes are reduced to available workers, work, and
affordable workspace; a smaller grant never permits hidden allocation. The
aggregate override must admit the largest configured share. Without an
override, the checked sum preserves the former maximum concurrent allowance
while allowing idle shares to accelerate active work.

Invalid zero values, a memory percentage outside 1..=100, a segment/debt count
above a fixed format bound, or a budget which cannot fit one required workspace
fails startup and names the exact setting. Heterogeneous nodes may use different
local budgets without changing durable bytes or query results.

### 17.2 Recovery and maintenance

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

Blob-reference lifecycle mutations atomically maintain a local derived
eligibility key ordered by `updated_at` for states which are unpublished or
have zero references. GC seeks only through the age cutoff, exact-rereads the
authoritative reference state, and then quarantines and removes the bytes.
Canonical blob directories are not inventoried. Crash-only staging and
quarantine reconciliation is bounded, resumable, restricted to those two
directories, and starts after serving. Cache reconciliation is likewise
explicitly started after serving; cold reads remain correct through exact lazy
validation and refetch.

## 18. Statistics and cost model

Every segment records enough metadata to plan without opening every data block:

- total and live document counts;
- per-field presence, null, and value counts;
- per-term document frequency;
- per-point-leaf and per-doc-value-block bounds;
- physical-order bounds;
- posting, point, position, vector, and doc-value encoded bytes;
- decoded byte upper bounds;
- vector dimensions and present count; and
- full-text token, frequency, norm, and impact summaries.

Correctness-bearing bounds are distinct from advisory cost estimates. Block
first/last keys, point/doc-value bounds, physical-order bounds, live counts,
segment-core scoring inputs, and conservative impact maxima are checksummed
index data. Writers verify them against emitted records, readers validate their
ordering and range, and corruption fails the definition closed. Only these
exact or conservative bounds may prune a block, terminate an ordered scan, or
skip scoring work.

Advisory estimates include expected selectivity, cache cost, decoded CPU, and
prefetch benefit. They choose iterator order, top-K strategy, and scheduling.
An inaccurate advisory estimate may make a plan slower but cannot omit a block,
predicate, or candidate; exact iterators and verification remain authoritative.

Statistics are also exposed to the future native Keldra planner. A future SQL
adapter consumes the same estimates rather than inventing a second topology or
index catalogue. The native cost includes selected HRW owner, local cache
residency, missing ordinary artifact ranges, expected remote bytes, and bounded
fetch concurrency. These locality values are advisory scheduling costs only;
they cannot move execution away from an authorized owner or change exact query
semantics.

## 19. Freshness and query result evidence

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

## 20. Observability

Low-cardinality metrics by index kind, phase, and size tier include:

- active, queued, waiting, completed, failed, and cancelled builds and merges;
- source records/bytes read and projected;
- posting blocks sought, advanced, decoded, skipped, and bytes read;
- term seeks and enumerated terms;
- candidate DocIds, conjunction advances, union heap operations, and two-phase
  verifications;
- live-mask blocks read and candidates rejected;
- invalidation-overlay offsets applied, resident bytes, cache drops, and
  candidates rejected;
- point nodes/leaves and bytes read;
- doc-value blocks/bytes read;
- facet and aggregate documents/values processed;
- physical-order early termination and top-K documents inspected;
- cursor seeks and documents skipped before/after the cursor;
- query phase time for planning, continuation seek, head initialization,
  physical merge/advance, candidate visibility, and response materialization;
- desired and granted query memory, resident segment slots/current/peak,
  conservatively charged retained decoded bytes, evictions, and reloads;
- authorization/exact-current batches, candidates checked, denied, stale, and
  refill work;
- cache hits, misses, fetched bytes, evictions, and pinned bytes;
- output hits, logical/physical read bytes, duration, timeout, and cancellation;
- generation age, source lag, publication time, and current merge debt;
- aggregate and per-class working-memory capacity, fair share, used, borrowed,
  waiting, desired, granted, peak, and permit duration; and
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

## 21. Complexity contract

For a segment with `D` documents, predicate posting sizes `P1..Pn`, `M` exact
Boolean matches, and requested limit `K`:

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
  matches, reads their order doc values, and retains `O(K)` top-K state
  plus one bounded validation batch;
- physical-order page continuation seeks from the cursor and does not repeat
  the previous page's complete work; arbitrary-order top-K may re-enumerate
  `M` matches and reread their order doc values; and
- merge memory is bounded by admitted blocks and lanes, independent of total
  corpus size.

Query execution never reads ordinary source payloads. Returning source fields
is an explicit `GetObject` or `BatchGet` operation after identity selection.

No fixed complexity promise can make an unindexed predicate or arbitrary exact
sort sublinear. The engine must identify that case explicitly instead of hiding
it behind a broad-scan fallback.

## 22. Validation

### 22.1 Codec and corruption tests

- golden vectors decode identically on AMD64 and ARM64;
- every component kind rejects bad magic, version, lengths, offsets, enum tags,
  checksums, and allocation claims;
- encode/decode round trips preserve every declared field type and capability,
  missing versus null, cardinality, Unicode, numeric boundaries, positions,
  vectors, stable result/source identities, and source-record ordinals;
- no persisted byte stream depends on Rust or `sux` native layout; and
- fuzzed component readers remain within declared allocation bounds.

### 22.2 Query correctness

- every predicate works alone and in `AND`, `OR`/`IN`, and exclusion forms;
- conjunction results equal a trusted in-memory evaluator for varied predicate
  selectivity and order;
- updates and deletes clear old DocIds without per-candidate head reads;
- post-generation overwrite/delete overlays suppress stale candidates, and a
  missing/behind overlay still returns no stale version because the bounded
  exact-current result check refills correctly;
- pre- and post-merge exact membership, doc values, facets, and aggregates are
  identical;
  scores and score order may change only in the newly published generation as
  documented, while an active cursor remains stable on its retained generation;
- physical and top-K order agree, including missing, null, direction, and stable
  document-identity tie-breaks;
- second and later pages neither repeat nor omit results and seek before
  rejected-candidate doc-value work;
- authorization denial refills from the same iterator without leakage;
- checkpoint-only progress preserves freshness across restart; and
- atomic-program paths become visible together at one generation CAS.

### 22.3 Production-shaped regression

A public-API qualification builds at least 800,000 Typed JSON objects with the
same selectivity shape as the incident query:

- one equality predicate matches nearly every document;
- two Boolean predicates narrow it;
- one three-value membership predicate narrows it further;
- two-field descending/ascending order requests four results; and
- pagination requests consecutive 999-result pages.

The verifier compares exact results and order with an independent evaluator.
It records ingest and index rates separately and reports posting advances,
candidate DocIds, liveness checks, point reads, doc-value reads,
logical and physical bytes, cache state, CPU, memory, and elapsed time.

Acceptance requires:

- no 300-second timeout;
- no ordinary source-payload read during a query;
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

### 22.4 All-kind and distributed matrix

Each of the eight kinds is created, built, queried, updated, deleted, rebuilt,
restarted, and merged through the public API on one node and a three-node Docker
cluster. Tests use independent buckets in one cluster. They verify HRW proxying,
artifact durability, owner failover, Zanzibar denial/public-read behavior,
freshness evidence, pagination, and exact results.

## 23. Clean-break removal list

The format-v4 implementation deletes, rather than wraps:

- format-v3 component and manifest readers/writers;
- range-local ordinal persistence and cross-run latest-live probing;
- single-driver Typed JSON and Metadata query execution;
- broad ordered-query fallback and post-scan continuation filtering;
- automatic terms-plus-generic-column-plus-stored-field projection for every
  selected Typed JSON field;
- query paths whose pagination restarts from the beginning;
- format-v3 merge and compaction code;
- compatibility branches, feature flags, converters, and dual-write tests; and
- known limitations which excuse the absence of posting intersection.

Retained source-journal, definition, HRW, authorization, object publication,
cache, accounting, gateway, and Raft code is changed only where required to
connect to the new format-v4 engine.

## 24. Known limitations

- Vector search remains exact and linear in the live filtered vector set. ANN
  requires a later accepted design.
- One query executes on one HRW owner. Very large indices may fetch ordinary
  artifact packs over the cluster, but Keldra does not scatter the query or merge
  network result sets.
- Arbitrary order without a matching physical order may inspect every exact
  Boolean match on every page and may authorize and exact-current-check every
  match and reread order doc values, but it never reads source payloads.
- Full-text merge may slightly change scores and score order in the newly
  published generation because BM25 statistics are aggregated from that
  generation's immutable segment set. Exact Boolean membership is unchanged,
  and an active page token remains on its retained generation.
- A keyword longer than 32,766 bytes supports only hashed `EXACT`; ordered,
  prefix, range, and facet capabilities reject it.
- Public Typed JSON and Metadata Filter predicate expressions are bounded to 32
  levels of depth and 256 total nodes. Empty Boolean operators are rejected;
  negation is set complement over indexed live documents rather than SQL
  three-valued logic.
- Metadata Filter and the dedicated Full Text kind retain fixed built-in field
  schemas and analysis rather than adding a second capability declaration API.
- SQL is not a user-visible capability in this release.
- A permanently failed definition may eventually hold journal capacity and
  backpressure affected writes until an authorized principal repairs, rebuilds,
  or deletes it. This preserves index visibility correctness.
- Capacity and lane configuration changes on restart.
- Mesh and regional coordination remain outside the single-cluster design.

## 25. Consequences

The new engine is a major internal refactor, but it removes the architectural
cause of multi-index-sized reads for sparse compound queries. Common DocIds,
live masks, advanceable postings, points, typed doc values, and one planner
serve every index kind instead of letting each rediscover filtering,
pagination, authorization, and cache behavior.

Keldra retains ownership of its durable format and can tune codecs, remote block
layout, cache hints, compaction, and future ANN structures for its
inline/erasure-coded object architecture. Following Lucene's stable execution
contracts avoids inventing an unproven search model without binding Keldra to a
foreign storage format.

The native scan boundary prevents a future SQL gateway from bypassing the
native planner or rebuilding authorization, liveness, and topology knowledge.
Its executable adapter remains deferred until there is an actual SQL consumer.

The clean break costs one rebuild from authoritative ordinary objects. It adds
no migration subsystem and leaves existing source data, object durability,
Zanzibar authority, journals, Raft state, gateways, and accounting semantics
unchanged.

## 26. References

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
- Apache DataFusion, [custom data sources and table providers](https://datafusion.apache.org/library-user-guide/custom-table-providers.html).
- Gonzalo Navarro and Veli Mäkinen, [Compressed Full-Text Indexes](https://users.dcc.uchile.cl/~gnavarro/ps/acmcs06.pdf), ACM Computing Surveys 39(1), 2007.
- Sebastiano Vigna, [Quasi-Succinct Indices](https://vigna.di.unimi.it/ftp/papers/QuasiSuccinctIndices.pdf), WSDM 2013.
- Sebastiano Vigna et al., [`sux`](https://github.com/vigna/sux-rs).
