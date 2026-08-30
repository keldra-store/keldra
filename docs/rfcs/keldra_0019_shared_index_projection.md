# KELDRA-0019: Shared source projection and definition-local assembly

Status: Implemented

Audience: Keldra implementors, operators, reviewers, and performance engineers

## 1. Decision

Every Keldra node runs one bounded projection mapper for the index definitions
assigned to that process. For an exact source-object version, the mapper forms
the union of JSON selectors required by matching active Typed JSON definitions,
streams the authoritative payload once, and retains definition-neutral typed
scalar facts. Definition-local assemblers consume those facts and continue to
own every definition-specific representation and publication decision.

The mapper may also retain a prepared mutation keyed by the exact schema
fingerprint. Definitions with the same compiled schema can therefore reuse not
only source selection but cardinality checks, date normalization, analysis,
terms, points, doc values, order keys, and field-length preparation.

The mapper is not authoritative. Source journals and exact object versions are
the replay authority; committed index manifests remain query authority. Every
mapper entry may be evicted or lost without changing results.

## 2. Motivation

Before this change, every definition independently performed the following work
for every matching source version:

1. read the exact payload from the object store;
2. parse and validate the complete JSON document;
3. walk selectors and retain selected values;
4. normalize and analyze those values; and
5. assemble a definition-local mutation.

That is a sound isolation model, but the expensive prefix scales with
`source objects × definitions`. With 64 definitions over the same stream, one
JSON document can be fetched and parsed 64 times before segment construction
begins. Increasing builder concurrency then increases duplicate payload reads,
parser work, allocation pressure, and competition for publication rather than
increasing useful indexing throughput.

The new boundary centralizes only work whose identity can be proven independent
of a definition. It deliberately does not centralize segment ownership or
publication authority.

## 3. Authority and ownership

```text
source journal + exact object version       authoritative input
                 |
                 v
bounded node-local projection mapper        disposable optimization
  - exact source identity
  - active-definition selector union
  - one streaming parse
  - canonical pointer -> typed scalar facts
  - optional schema-fingerprint output cache
                 |
                 v
definition-local assembler                  authoritative index construction
  - schema and Field IDs
  - terms, points, doc values and order keys
  - document IDs, locators and live masks
  - segment flush/merge policy
  - manifest and current publication
```

No posting list, segment, locator, live mask, manifest, checkpoint, or current
pointer is shared between definition writers. That preserves independent
definition rebuild, retention, fencing, recovery, and publication.

## 4. Exact cache identity

A selected-fact cache entry is identified by:

- tenant and bucket;
- canonical source path and exact object version;
- the exact source commit timestamp used by indexed metadata fields;
- content type;
- `BlobRef` hash and length; and
- the hash of the canonical sorted selector union.

Prepared outputs within that entry are additionally keyed by the compiled
schema fingerprint. A catalog edit changes either the selector-union hash or
schema fingerprint and cannot consume stale prepared state. Path reuse, version
reuse with different bytes, content-type changes, and definition changes are
therefore distinct cache identities.

The mapper registers only definitions assigned to the local process. In a
single-node cluster it can share across every active definition. In a
multi-node cluster each node can reuse one retained parse across its matching
local definition set. Eviction or an oversized-union fallback can deliberately
repeat a parse; the design does not introduce network RPCs or a new
cluster-wide coordinator.

## 5. Bounded memory and admission

`KELDRA_INDEX_SHARED_PROJECTION_MEMORY_BYTES` is a mandatory accounted share of
the existing process-wide index working-memory ceiling. It defaults to 256 MiB
and is divided equally between:

- the selected/prepared LRU cache; and
- one active selector-union mapping workspace.

The mapper acquires that share at runtime startup. Cache keys, selected scalar
vectors and strings, prepared mutations, and fixed entry overhead are charged
before retention. Entries larger than the cache half are used once and not
retained. Least-recently-used source entries are evicted until the cache is
within its hard bound.

Definition assembly remains charged to the requesting builder lane. Returning
a cached prepared mutation clones it into that admitted lane; mapper ownership
and builder ownership are therefore both explicit while both allocations are
live.

The default aggregate working-memory calculation is:

```text
query share
+ shared projection share
+ the eight per-kind builder shares
= 2.75 GiB with defaults
```

An explicit `KELDRA_INDEX_WORKING_MEMORY_BYTES` must admit the query share, the
permanent projection share, and the largest mandatory builder request at the
same time; otherwise startup rejects the configuration instead of admitting a
deadlocked runtime.

## 6. Oversized unions and fallback

The optimization cannot add a new indexing limit. A selector union across many
definitions can exceed the mapper workspace even when each individual
definition fits its admitted projection lane. In that case Keldra:

1. abandons the union attempt without caching partial facts;
2. reopens the same exact `BlobRef`;
3. invokes the released bounded per-definition projector; and
4. emits a union-bypass telemetry event.

If the individual projection also exceeds its existing lane bound, the
pre-existing resource-limit behavior applies. Malformed JSON retains the
existing tombstone/skip semantics and is safe to cache as a negative mapping.

## 7. Concurrency and ordering

Sixty-four striped asynchronous locks provide same-source singleflight without
serializing unrelated objects. A source miss is rechecked after acquiring its
stripe so concurrent definition writers cannot both parse it. Projection work
continues to use the bounded index CPU pool.

Catch-up and rebuild may request several source versions concurrently up to the
definition's existing projection-lane cap. Results are restored to source order
before they enter a definition assembler. The mapper therefore changes neither
journal settlement order nor deterministic segment input order.

The initial implementation shares Typed JSON scalar selection. Other index
kinds continue through their existing projectors. This is intentional: scalar
facts have a canonical definition-neutral representation, whereas tokenization,
vectors, Git records and tensor records need a separately proven reusable
identity before crossing definition boundaries. Identical Typed JSON schemas
already reuse their complete prepared mutations.

## 8. Telemetry

The mapper emits counters at the exact decision boundary:

- `keldra_index_shared_projection_payload_parses_total` — exact payloads
  streamed by the shared mapper;
- `keldra_index_shared_projection_selected_hits_total` — reuse of canonical
  pointer facts;
- `keldra_index_shared_projection_prepared_hits_total` — reuse of a complete
  schema-fingerprint mutation; and
- `keldra_index_shared_projection_union_bypasses_total` — fallback because the
  active selector union exceeded mapper workspace.

Qualification must compare these with accepted source objects and active
definition count. For identical schemas, parses should approach source-object
count rather than source-object count multiplied by definitions.

The exact events remain at debug level. At info level, the mapper emits one
cumulative summary after every 16,384 projection requests, including all four
counters plus cache entry and resident-byte counts. Production qualification
therefore obtains direct reuse evidence without enabling per-object logs.

## 9. Correctness tests

Required focused coverage is:

- one union parse feeding definitions with different field names/selectors is
  byte-for-byte equal to independent projection;
- arrays, nulls, unsigned/signed numbers, dates, analyzers, cardinality, points,
  facets, ordering and physical components retain their existing semantics;
- malformed JSON produces the same explicit tombstone/diagnostics;
- cache identities change with exact source or plan identity;
- prepared hits require the exact schema fingerprint;
- eviction stays within the configured bytes;
- an oversized union falls back and a genuinely oversized individual
  projection still fails with the original resource limit; and
- rebuild and catch-up preserve source order and final exact freshness.

## 10. Performance qualification

Qualification uses the released 0.15.0 image as baseline and an image built
from the exact candidate commit. Both run from fresh volumes with identical
fixed-rate workloads on the same machines:

- the 16-core/64-GiB spinning-disk host; and
- the 8-core/32-GiB SSD host.

For each host run D1, D4, D16 and D64 definition counts. Record accepted ingest
rate, mutation latency, source and publication lag, drain time, query latency,
payload parses, selected/prepared hits, union bypasses, CPU, RSS, RocksDB write
bytes, and final correctness. Experiment scripts, logs, data and artifacts must
remain under `~/keldra_experiments` on each host.

The change succeeds only if final results are exact and it materially reduces
normalized projection work and freshness lag at multi-definition scale without
regressing D1 ingestion/query behavior beyond normal run variance.

## 11. Non-goals

This RFC does not:

- make a projection cache authoritative or durable;
- change source-journal retention or object durability;
- share definition-local Field IDs, document IDs or segment state;
- introduce synchronous indexing work into object ingestion;
- add a cluster-wide indexing coordinator;
- weaken authorization or exact-version reads;
- change index format or public query semantics; or
- claim that all index kinds have definition-neutral reusable facts.
