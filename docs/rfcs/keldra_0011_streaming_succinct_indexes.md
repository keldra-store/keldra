# ANVIL-0011: Streaming Succinct Indexes in Anvil 0.6.0

Status: Superseded by ANVIL-0012.

Audience: Anvil implementors, operators, client authors, and reviewers

Compatibility: None for indexes. Anvil 0.6.0 does not read, convert, migrate,
dual-publish, or fall back to an Anvil 0.5.x index definition, artifact,
generation, cache entry, or page token. The index subsystem is implemented as
a clean replacement.

## 1. Decision

Anvil 0.6.0 replaces whole-generation index construction with a bounded,
streaming segment architecture:

```text
cluster source journals and snapshot barriers
  -> one weighted-HRW index builder
  -> bounded mutable L0 builder
  -> immutable write-optimized L0 segments
  -> streaming size-tiered compaction
  -> immutable quasi-succinct merged segments
  -> ordinary Anvil objects with the strongest available acknowledgement
  -> one CAS-published generation manifest
```

The public API presents a mutable, asynchronously updated index. The durable
representation is immutable. Updates and deletes create new change records;
compaction later removes superseded representations. No index page is modified
in place.

One complete query is always evaluated on one node. Weighted rendezvous
hashing (weighted HRW) selects one builder and up to three query replicas from
the active cluster membership. A query may enter through any node, but it is
proxied to one selected query replica. Anvil never scatter-queries independent
per-node index partitions and never merges network-distributed result sets.

Index components and their independently fetchable blocks are ordinary Anvil
objects. Small components use the ordinary inline path; larger components use
the ordinary erasure-coded byte plane. The index cache is disposable. Raft
contains neither index bytes, index paths, index assignments, index cursors nor
per-index coordination records.

Each index kind has one process-wide, operator-configured construction-memory
budget shared fairly by every local builder of that kind. A large index cannot
consume another index's allowance, and many indexes of one kind cannot multiply
that kind's configured limit. Work pauses or flushes before the limit is
exceeded; it does not buffer an unbounded backlog in memory.

Anvil adopts the pure-Rust [`sux`](https://github.com/vigna/sux-rs) crate for
succinct structures and adopts [Rayon](https://github.com/rayon-rs/rayon) for
bounded CPU parallelism. Both dependencies are approved for this design.
Rayon work must still hold the same byte-budget permits as synchronous work;
parallelism is never an escape from memory accounting.

The 0.6.0 dependency is pinned to `sux = 0.14.0` with default features disabled
and only its `rayon` feature enabled. Anvil uses the Apache-2.0 option of
`sux`'s `Apache-2.0 OR LGPL-2.1-or-later` license. This deliberately excludes
its unrelated Flate, Zstandard, Serde and epserde features. The resolved Rayon
version is 1.12.0 under its `MIT OR Apache-2.0` license. `Cargo.lock` is the
complete reproducible transitive dependency record.

## 2. Why the index must be replaced

The released implementation rebuilds a complete immutable generation after an
invalidation. It holds the selected current-object projection and newly encoded
engine files in builder memory before cutting those files into storage blocks.
Its 4 MiB storage-block target therefore limits only the final artifact shape;
it does not bound generation-construction memory.

A production capture demonstrated that this is not an acceptable scale limit.
The identifying application, tenant, bucket, paths, schema, payloads and
business behavior are deliberately excluded from this RFC. The relevant,
anonymized facts are:

- the host was x86-64 with 16 logical CPUs and 62.7 GiB of memory;
- the public input archive was about 1.43 GB and contained 839,980 small JSON
  records indexed over 12 fields;
- ingress used four concurrent batches of 256 items with a 60 MiB maximum
  encoded batch size;
- the run reached 687,395 records before Anvil became unavailable and the host
  ultimately required recovery after an out-of-memory event;
- four complete samples showed Anvil RSS grow from about 37.2 GiB to 45.7 GiB
  in 25.25 seconds, an increase of about 9.19 GiB during that short interval;
- the detailed memory map attributed about 46.3 GiB to anonymous RSS and only
  about 0.63 GiB to file-backed RSS, so approximately 98.7% of process RSS was
  anonymous rather than mapped index or RocksDB files;
- file cache was evicted while anonymous memory rose, and process CPU remained
  between roughly 213% and 312% in the complete samples;
- 1,660 successful bulk requests represented 403,207 operations and about
  7.83 GB of encoded request bytes before the terminal stall; and
- the stopped data directory contained about 3.98 GB of RocksDB metadata,
  8.53 GB of ordinary content-addressed large objects and 0.73 GB of disposable
  index cache.

The capture does not identify which Rust allocation types owned every anonymous
page, and the storage inventory cannot attribute every content hash to an index
component without private RocksDB data. It nevertheless proves the release
architecture permitted corpus-scale anonymous growth on a machine with ample
RAM while a complete generation was being assembled. The source path confirms
that complete source projections and encoded files were retained before
publication. A design which merely tunes allocator behavior, increases the
memory limit or changes the final block size would preserve the underlying
failure mode.

The replacement must make peak construction memory a configured invariant, not
an emergent property of corpus size.

## 3. Research basis

MG4J's construction process is a close match for Anvil's execution boundary.
MG4J accumulates a bounded batch, writes it as a separately queryable subindex,
and combines batches later. Its manual explicitly describes flushing a batch
when a document threshold is reached or available memory is low, and exposing
the batch set as one logical cluster. In MG4J, “cluster” means a composite index,
not distributed query execution. See [Scan: Building batches](https://vigna.di.unimi.it/MG4J/man/manual/ch02s03.html),
[Combining batches](https://vigna.di.unimi.it/MG4J/man/manual/ch02s04.html), and
[Clusters & Partitioning](https://vigna.di.unimi.it/MG4J/man/manual/ch04.html).

Vigna's [Quasi-Succinct Indices](https://vigna.di.unimi.it/ftp/papers/QuasiSuccinctIndices.pdf)
represents monotone sequences with Elias-Fano high and low bits. It provides
compact sequential traversal, direct access and efficient successor operations
used by posting-list intersections. The paper also describes the practical
incremental arrangement adopted here: small segments may use cheap gap coding,
while larger merged segments use the more query-efficient quasi-succinct
representation once their frequencies, occurrence counts and upper bounds are
known.

The modern Rust [`sux`](https://github.com/vigna/sux-rs) project supplies
Elias-Fano dictionaries, rank/select bit vectors, prefix-omission string lists,
static filters and related succinct structures. Anvil uses those structures in
merged index components instead of porting MG4J's Java implementation.

[`epserde`](https://github.com/vigna/epserde-rs) demonstrates a useful owned-to-
borrowed model for opening large immutable Rust structures with very little
copying. Its own documentation warns that validation and padding cleaning are
the caller's responsibility. Anvil adopts the design lesson—flat immutable
arrays behind pinned handles—but does not make epserde's native-layout format
the authoritative 0.6 storage format. Authoritative blocks use Anvil's explicit,
versioned, fixed-width encoding so AMD64 and ARM64 nodes can exchange them.

Navarro and Mäkinen's survey of
[Compressed Full-Text Indexes](https://users.dcc.uchile.cl/~gnavarro/ps/acmcs06.pdf)
explains compressed suffix arrays, FM indexes, wavelet trees, rank/select,
sampling and self-indexes for arbitrary substring `count`, `locate` and
`display`. Those techniques inform the future substring index described in
Section 23. They do not replace word-oriented inverted indexes used for fields,
BM25-style ranking and phrase queries.

## 4. Required invariants

1. Peak mutable construction memory is bounded independently for each
   `IndexKind` by startup configuration and is shared across every index of
   that kind on the node.
2. No input-corpus, generation-file, posting-list or event-backlog collection
   grows in heap in proportion to the complete index.
3. One logical index definition produces one cluster index, one builder and up
   to three disposable query materializations—not one authoritative index per
   node.
4. One query executes completely on one selected node. There is no distributed
   scatter/gather query plan.
5. Index definitions, segments, components, blocks, manifests and current
   pointers use ordinary Anvil objects and ordinary reference accounting.
6. Every artifact named by a published generation has completed the artifact
   acknowledgement defined in Section 14 before the current pointer can name
   it.
7. Publication is one compare-and-swap of the current pointer. Before that CAS,
   a partially uploaded segment or compaction output is invisible to queries.
8. A query is pinned to one immutable generation, definition version,
   authorization revision and page-token identity for its complete execution.
9. Every returned result names the current live object version represented by
   that generation. Superseded versions and tombstones never return as hits.
10. An atomic program becomes index-visible no less atomically than it becomes
    visible through ordinary object reads.
11. A journal gap, source-epoch change or unprovable snapshot boundary triggers
    a complete rebuild. Anvil never guesses past missing evidence.
12. Zanzibar authorization applies to definition management, querying and
    every exact result whose visibility is not proven by a coarser authorized
    boundary.
13. Caches, builder scratch and decoded structures are disposable. The
    published ordinary objects and current pointer are the only index authority.
14. Raft remains bounded and independent of index, term, posting, segment,
    generation and query cardinality.
15. Pre-0.6 index state is rejected, not interpreted through compatibility
    code.

## 5. Goals

1. Build every supported index using bounded memory regardless of source
   corpus size.
2. Apply object changes incrementally without rebuilding unaffected segments.
3. Let compact, read-optimized structures coexist with cheap ingestion.
4. Keep query execution local while transparently materializing cold blocks
   from the distributed object plane.
5. Share construction capacity fairly between unrelated indexes and prevent a
   noisy index from starving all peers of its kind.
6. Keep deletes, overwrites, bucket versioning, atomic visibility and index
   freshness correct across flushes and compactions.
7. Reuse one segment, cache, publication, recovery and observability framework
   across all index kinds without forcing every engine into one data structure.
8. Preserve explicit index boundaries by tenant, bucket and path prefix so work
   is removed before query-time filtering.
9. Validate the implementation against the public large-JSON workload shape
   which exposed the released failure.

## 6. Non-goals

Anvil 0.6.0 does not provide:

- a mutable in-place B-tree, posting file, HNSW graph or other authoritative
  index page;
- distributed query planning, network joins or per-node result merging;
- a second index database, column family, catalogue, WAL or reference-count
  plane;
- unbounded query-result or candidate materialization;
- an exact substring/FM index;
- approximate vector search or a mutable ANN graph;
- language-specific stemming, fuzzy matching, synonyms or a configurable text
  analysis pipeline unless already present in the public 0.6 API;
- a new authorization model or an authorization result cache that can outlive
  its Zanzibar revision;
- preservation of any 0.5.x index byte, page token or implementation module;
  or
- a claim that mmap virtual bytes alone constitute a hard RSS budget.

## 7. Terms

**Definition** is the authorized ordinary object describing one logical index:
its stable ID, tenant, bucket, path boundary, content-type boundary, kind and
kind-specific fields.

**Builder** is the one weighted-HRW-selected node which consumes cluster change
evidence and publishes generations for a definition.

**Mutable builder** is bounded temporary state used to invert or order a small
batch of changes. It is not authority and is lost safely on process failure.

**Run** (also called a segment in the indexing literature) is an immutable,
independently queryable set of changes or compacted state. An L0 run is write
optimized; a merged run is read optimized. This RFC uses “segment” when
discussing the general indexing technique, while format-2 paths and concrete
artifact identities use `run`.

**Component** is one semantic portion of a run, such as a term dictionary,
posting stream, path/version directory, positions, vector values or live-state
changes.

**Block** is the independently fetched, checksummed ordinary object holding a
bounded range of one component. An index block is not an erasure-code shard.
The ordinary Anvil byte plane may erasure-code the block's payload.

**Generation manifest** is the immutable description of the exact active
run set and source coverage presented to queries.

**Current pointer** is the small mutable ordinary object whose CAS publishes
one immutable generation manifest.

**L0** is the first, cheap immutable representation flushed from a mutable
builder. **L1+** are increasingly large quasi-succinct runs produced by
streaming compaction.

## 8. Placement and authority

An index identity is the stable tuple `(tenant_id, bucket_id, index_id)`.
Mutable names do not participate in placement. The existing weighted-HRW
ranking over committed ACTIVE membership selects:

```text
rank 0       builder and first query replica
rank 1..2    additional query replicas when present
```

The builder assignment is derived and disposable. It is not written to Raft or
an index registry. The committed membership fence carried by the current
generation prevents a stale former builder from publishing after reassignment.
The final current-pointer CAS is checked against both the expected object
version and current placement fence.

Query replicas materialize the same published generation. They never build or
publish competing indexes. Any selected replica can execute a complete query;
ordinary routing spreads queries among at most three replicas.

## 9. Cluster change input and rebuild boundaries

Every ACTIVE node retains its locally ordered, gap-detecting source journal.
The current builder pulls all required source journals over the authenticated
peer API. A source event is an invalidation, not trusted index content: the
builder rereads the current authoritative head and, when needed, its payload
through ordinary Anvil APIs.

The builder records one `(source_epoch, next_offset)` cursor per source. Events
from different sources need no global order. Exact-path versions are monotonic,
so a late event for version `V` is ignored when the active segment view already
knows version `>= V` for that path.

An initial build or gap recovery must establish a coherent boundary:

1. bind the operation to one committed membership fence;
2. capture every required source's epoch and tail;
3. scan that source's rank-zero current heads under a RocksDB snapshot bound to
   the captured source state;
4. stream the scan through bounded L0 builders;
5. replay journal entries strictly after each captured tail;
6. re-read current heads and compare exact-path versions so scan/replay overlap
   is idempotent; and
7. publish only after all source cursors and the atomic-program finalized
   watermark form one complete barrier.

A multi-page live scan that is not bound to a source snapshot cannot support a
manifest checkpoint and must not publish. Loss of a retained journal range,
source-epoch change, membership change during the build or an incomplete
atomic watermark abandons the candidate and restarts from a new complete
boundary.

The published freshness response exposes the generation and checkpoint vector.
Lag remains evidence for the client, not a query-admission error.

## 10. Per-kind construction budgets and fairness

Startup configuration supplies one positive byte limit for each enabled
`IndexKind`. For example, all full-text definitions assigned to a node share
the full-text limit; all vector definitions share a separate vector limit. The
sum of the configured per-kind limits is the maximum heap Anvil deliberately
offers to simultaneous index construction, apart from small fixed scheduler,
task and allocator overhead which must be measured and reported.

One `TypeBuildPool` exists per kind. It owns:

- a byte semaphore for mutable records, dictionaries, sort buffers, output
  blocks and engine-specific temporary arrays;
- a FIFO round-robin queue of definitions with available source work;
- access to the one fixed process-owned Rayon pool shared across kinds; and
- per-definition progress and waiting-time metrics.

Work is pull based. A builder first obtains a bounded byte lease and only then
pulls the source page or component block it can process with that lease. It
does not enqueue source payloads while waiting for memory. A definition yields
after one work quantum or sealed block so another ready definition of the same
kind can progress. A large definition may use unused capacity, but it cannot
reserve the whole pool across a yield when peers are waiting.

Every capacity-growing collection must be charged before reserving memory.
Variable-sized payloads are parsed or tokenized as streams. Sorting uses
bounded runs. A single hot term is split across independently encoded posting
blocks. A selected typed or projection group which has no unambiguous split
semantics in 0.6.0 is rejected before builder admission instead of forcing a
large allocation or silently changing its query meaning.

One source payload or one derived mutation can itself be larger than the
configured kind budget. The 0.6.0 KISS behavior is fail closed: the builder
does not skip the object, spill it into another storage plane, publish a cursor
past it, or exceed the hard cap. It keeps serving the preceding generation and
reports the required bytes, configured cap and lagging index identity. The
operator can raise the startup cap or change the index definition or source
data. Truly streaming projections may remove this limitation later without
changing authority or publication semantics.

When the budget cannot admit the minimum useful quantum, the builder waits and
exports the reason. When an L0 builder reaches its lease, configured record
target or publication interval, it coalesces same-path changes and flushes.
Each level admits at most four runs. When a fifth run makes a level overfull,
compaction of the four oldest runs takes priority and new events remain in their
bounded source journals. If that delay outlives journal retention, the correct
fallback is a bounded complete rebuild.

Rayon uses a process-owned fixed thread pool. In 0.6.0 it performs the
CPU-heavy, non-async source projection that can consume an entire admitted
payload. Seal and compaction keep their asynchronous ordinary-object sinks and
run on the index writer task, but their synchronous work is cut at the fixed
512 KiB block boundary and yields whenever a block is read or emitted. This
avoids an executor bridge which would either block a Rayon thread on async I/O
or introduce another buffering channel. Tasks submitted to Rayon hold the same
memory permits as the caller. No index builds a private unbounded Rayon pool,
and code does not rely on Rayon's global pool for resource isolation.

## 11. Immutable L0 segments

Every accepted source change becomes one of:

```text
LiveChange {
  path
  version
  segment_local_document
  kind-specific projection
}

DeleteChange {
  path
  tombstone_version
}
```

The mutable builder keeps only its bounded batch. Repeated changes to the same
path within the batch coalesce from the published baseline directly to the
greatest exact-path version; intermediate versions do not contribute temporary
postings or statistic deltas. The flush writes sorted, write-optimized
structures using simple fixed-width or gap-coded sequences. It does not pay the
two-pass construction cost of the final quasi-succinct representation.

Every L0 segment contains a path-change directory for every path changed by
the batch, including deletes. A live entry maps to one segment-local document
ordinal. Kind-specific components refer to that ordinal rather than repeating
the tenant, bucket, path and version in every posting.

The run root and its component descriptors record:

- format, engine kind and level;
- mutation, live-document and version-range statistics;
- minimum and maximum component routing keys; and
- component/block descriptors and hashes.

The immutable generation manifest binds those roots to the stable index ID,
definition version, complete input checkpoint vector, atomic finalized
watermark and accepted/skipped diagnostics once. Runs are never opened outside
that validated manifest. Keeping generation-level identity at that one
authority avoids duplicating mutable-generation context in every immutable run
without weakening validation.

An L0 segment is queryable immediately. It need not wait for compaction.

## 12. Streaming compaction and quasi-succinct segments

Compaction is size tiered with a fixed four-run fanout. A level may contain at
most four runs. When the fifth arrives, the builder selects the four oldest
contiguous runs from that level and streams them into one run at the next
level. One compaction therefore has exactly four inputs and one output. The
unselected newest run remains at its level. Cascading compaction applies the
same rule at every higher level, so the active manifest has a bounded number of
runs per level and logarithmic growth in levels.

Inputs remain immutable and queryable throughout compaction. The merger uses
k-way iterators over sorted component blocks. It retains only iterator state,
one bounded output block and engine-specific small statistics. It never opens
all input bytes into memory.

The fixed 0.6 profile caps each encoded component or routing block at 512 KiB
and uses routing fanout 32. Decoding one input block may expand its several
owned arrays, strings and records; their aggregate decoded-block allowance is
approximately 4 MiB. Fixed compaction admission therefore covers all four
inputs as `4 * (512 KiB encoded + approximately 4 MiB decoded)`, for a 16 MiB
aggregate decoded-input bound. Output and
routing construction remain covered by the same exported fixed-workspace plan,
and the compactor still holds the process-wide per-kind budget permit. It may
not admit a fifth input or treat mmap-backed bytes as uncharged memory.

For monotone sequences, compaction first obtains the element count and upper
bound from block headers or one bounded counting pass, then performs an encoding
pass into a `sux` Elias-Fano structure. Term/path dictionaries may use
prefix-omission string lists. The initial 0.6 profile uses those two structures
where they directly improve merged runs; ranked dense bit vectors remain a
format-2 optimization rather than a release requirement. Offset arrays,
cumulative counts, sparse live sets and posting document IDs use the suitable
codec rather than one universal representation.

Compaction removes older versions shadowed within its input span and preserves
the greatest path version, including a tombstone. It never drops the path-change
record needed to shadow a version in a segment outside the compacted span.
Compacting an older span while newer segments remain is safe because generation
visibility is still evaluated across the complete manifest.

Output blocks are sealed and uploaded incrementally. A term whose posting list
exceeds one work quantum is represented by multiple independently searchable
posting blocks. Directories are themselves blocked and have a small root whose
ranges identify the child blocks needed for a lookup.

Only after every replacement component and its segment manifest has completed
the artifact acknowledgement defined in Section 14 may the builder propose a
new generation which removes the input segments and adds the replacement.
Failed or losing CAS outputs are unreachable immutable objects handled by
ordinary retention and reference GC.

## 13. Format v2

The new family is index format `2`. Every generation manifest, run root,
component root and component block carries the format number or format-2 magic
and a typed component identifier. A component block has an unambiguous magic
value, fixed-width lengths, bounds, element counts and BLAKE3 integrity
identity.

The encoded block ceiling is 512 KiB, including the component envelope, and a
routing node has at most 32 descriptors. These constants keep even maximum
path-key routing nodes below the encoded ceiling and make the compaction
workspace independent of corpus size. Decoders validate both encoded bounds
and the approximately 4 MiB aggregate owned-decoding allowance before
reserving element arrays.

Durable numeric arrays use fixed-width unsigned or signed integers in little-
endian order. They never serialize `usize`, pointer values, Rust enum layout or
native padding. Succinct word arrays use `u64` as their durable word size.
Decoders validate lengths, monotonicity, range bounds, rank/select support data,
document ordinals and checksums before an unchecked `sux` query path can see the
data.

The small generation and segment manifests may use a self-describing encoding,
but `format = 2` and required fields are mandatory. The format records the
codec and codec revision of every component. A later incompatible codec writes
a new component-format revision and is introduced through normal compaction;
it does not reinterpret old bytes.

The format-2 reserved paths are:

```text
_keldra/indexes/v2/definitions/{index_name}
_keldra/indexes/v2/{index_id}/runs/{run_hash}/...
_keldra/indexes/v2/{index_id}/manifests/{manifest_hash}
_keldra/indexes/v2/{index_id}/current
```

The run hash is not known while its component blocks are being streamed. Each
bounded block is therefore first sealed with the existing `stage_blob`
primitive. That is the ordinary content-addressed byte plane and its existing
awaiting-publish lifecycle, not an index scratch store or side plane. The
builder retains only the returned hash, length and range in its bounded routing
tree. Once the final small root determines `run_hash`, the publisher traverses
that staged tree and publishes each existing `BlobRef`, without rereading the
source or retaining a block inventory, beneath the final run path with the
artifact acknowledgement defined in Section 14. It publishes the root last.
Different runs get different ordinary object paths even when block bytes
deduplicate, so ordinary reference counting remains exact and per-run retention
needs no global mark set. A failed unfinished build leaves only ordinary
awaiting-publish blobs or unreachable immutable run paths, handled by the
existing age-bounded GC.

Component, run and manifest paths are content addressed, so an abandoned
candidate cannot collide with a later retry of the same logical generation
number. Exact helper spelling below a run path remains an implementation
detail. The current pointer always names one immutable generation manifest.
Format-1 paths are never probed and there is no dual-write path.

## 14. Ordinary-object publication

Artifact publication requests `REPLICATED` acknowledgement whenever at least
two ACTIVE nodes exist. A one-node cluster has no remote node which can supply
that acknowledgement, so it requests `LOCAL` and proceeds only after the sole
node has durably stored the ordinary object. This is an acknowledgement-timing
exception, not a second placement or storage format: inline versus erasure-
coded storage, later replication and online cluster growth continue to follow
the ordinary object rules. Once a second node is ACTIVE, subsequent index
artifact publication uses `REPLICATED`.

Publishing a new L0 batch or compaction result follows one sequence:

1. encode bounded component blocks;
2. stage each block in the ordinary content-addressed byte plane;
3. once the root determines the run hash, traverse the staged tree and publish
   every block, component root and run root beneath the final run path with the
   selected artifact acknowledgement, publishing the root last;
4. write the immutable generation manifest containing the complete active
   segment set and complete source checkpoint vector with the selected artifact
   acknowledgement;
5. revalidate builder placement, definition version and atomic watermark; and
6. CAS `_keldra/indexes/v2/{index_id}/current` from the exact previously observed
   object version to the new generation, also using the selected artifact
   acknowledgement.

The CAS is the sole index-publication point. There is no index commit log,
receipt column family or Raft command. A request that observes the old pointer
uses the complete old generation; a request that observes the new pointer uses
the complete new generation.

The manifest contains at least:

```text
IndexGenerationManifestV2 {
  format = 2
  index_id
  generation
  definition_version
  placement_fence
  atomic_finalized_through
  source_checkpoint_vector
  ordered_active_runs
  visible_document_count
  accepted_and_skipped_diagnostics
  authoritative_bytes
}
```

The generation number is an identity, not a transaction ID or source order.
The source vector and atomic watermark are the freshness evidence.

## 15. Version and tombstone visibility

Every active segment has an exact path-change dictionary. To resolve a path in
one generation, the query view probes candidate segment dictionaries and
chooses the greatest exact-path version. A result is visible only when:

```text
latest(path).state == LIVE
and latest(path).version == candidate.version
```

A later tombstone therefore hides every earlier posting without rewriting it.
An overwrite hides the old representation even when the old payload is retained
by bucket versioning. Compaction eventually removes hidden data, but correctness
does not wait for compaction.

Segment routing filters and path ranges avoid most exact dictionary probes.
Static filters may reject absence but are never accepted as proof of membership;
an exact path and version comparison follows every positive filter result.

The initial full-text engine stores term frequency, field length and optional
positions in each posting and computes a deterministic bounded local score. It
does not hold a generation-wide statistics map while building. Forward term
vectors and signed generation-wide BM25 statistic deltas can be added as
format-2 components later; they are a ranking-quality optimization, not part of
path/version visibility or publication correctness.

An engine produces candidates lazily. Visibility filtering occurs before
authorization and before a hit is emitted. Top-k engines continue pulling until
no unseen candidate can outrank the retained visible hits or their iterator is
exhausted; they do not assume the first `k` raw candidates survive tombstone and
authorization filtering.

## 16. Atomic-program visibility

The pull-based journal collector exposes only a complete atomic-program
finalized watermark.
A mutable builder may observe partial invalidations, but it cannot publish a
generation whose source vector passes an atomic commit until every path from
that commit is visible in authoritative current state and represented by the
candidate segment set.

An ordinary-object query and an index query can therefore disagree only because
the index generation is explicitly older, as reported in freshness. They cannot
disagree by showing only part of an atomic program at one claimed checkpoint.

## 17. Query execution and lazy iterators

The selected query replica reads and validates one current pointer and pins its
immutable generation for the request. A generation directory exposes an async,
file-like Anvil API; it does not return `std::fs::File` and does not borrow a
caller-owned mutable slice across an async boundary. Reads return an immutable
reference-counted slice for at most the requested length together with its
logical offset.

The directory maps logical component ranges to content-addressed blocks. A
cache miss fetches the exact ordinary object, verifies its length and hash,
installs it atomically in the shared disk cache and returns a pinned handle.
Concurrent misses for one hash are coalesced. Dropping the last handle makes a
block eligible for eviction; it does not delete the authoritative object.

Engines first load small roots and routing data, then request only contributing
blocks. Iterators expose operations such as `next`, `advance_to`, `seek_key` and
bounded score information. AND, OR, range, phrase, prefix and cross-segment
operations compose those iterators rather than materializing complete posting
sets.

Mapped immutable blocks are useful, but a mapped virtual range is not counted
as free memory. The cache manager tracks pinned decoded bytes and open mappings,
keeps roots preferentially, and avoids pinning a whole index solely because a
query touches one term. Linux may reclaim clean file-backed pages; Anvil's hard
construction budgets still apply to anonymous builder allocations independently
of page-cache behavior.

## 18. Scope and Zanzibar authorization

An index definition is rooted in exactly one stable tenant ID and bucket ID and
optionally one slash-segment-aware path prefix and content type. Those bounds
are applied during construction and query routing before an engine examines
kind-specific data. A definition for `/tenant/123` does not include
`/tenant/1234`.

Every management request is authenticated and Zanzibar authorized. A query may
instead omit credentials, in which case it must name its tenant explicitly and
Anvil binds it to the fixed anonymous subject. Authenticated queries may omit
the tenant; a supplied tenant must match the signed identity. Anvil checks the
coarse index/bucket capability first. Segment path ranges, definition scope and
query predicates then eliminate impossible candidates. Remaining exact object
results are checked in bounded Zanzibar batches at the generation's
authorization revision. If a batch contains denials or reordered answers,
iteration continues without exposing an unauthorized path, count, continuation
position or score.

Anonymous callers remain the ordinary implicit anonymous subject and receive
results only where public-read relationships explicitly grant them. Failed or
malformed authentication never degrades to anonymous. When execution is routed
to another query replica, mandatory mTLS carries only the original signed JWT
or the fixed anonymous marker plus the requested tenant; the replica rebuilds
identity, validates the tenant against its stable ID and evaluates Zanzibar.

Authorization state is not copied into an authoritative per-index ACL plane.
Any future disposable authorization accelerator must be revision bound and
must fail closed; it is outside this release.

## 19. Cache and materialization policy

All locally assigned indexes share the existing node-wide index disk-cache and
in-flight materialization-memory budgets. Per-kind construction budgets do not
create per-index caches. The cache manager, not an engine, owns fetching,
verification, pinning, prefetch and eviction. Initial 0.6 reads remain
mmap-backed instead of copying a mapped block into a second managed heap tier;
the operating system may retain clean mapped pages and reclaim them under
pressure.

The cache API admits declarative hints for:

- root or routing block: retain and prefetch;
- sequential posting/position block: prefetch next after demonstrated
  sequential access;
- random vector/value block: do not speculative-prefetch;
- one-shot merge input: low retention priority; and
- output block being sealed: do not admit as a query-cache duplicate.

The initial engines perform bounded demand reads and explicit prefetch only;
format-specific retention priorities are a disposable-cache optimization and
may be wired without changing authoritative artifacts or query results.

Hints may improve cost but never alter correctness or authority. A complete
small index may remain warm in memory. A larger index transparently exchanges
component blocks over the ordinary object plane while the complete query still
runs on one node.

## 20. Failure and recovery

| Failure | Required behavior |
| --- | --- |
| Process exits with a mutable L0 builder | Lose the disposable batch and resume from the published source vector. |
| Crash while uploading blocks | Current pointer remains unchanged; unreachable ordinary objects are later removed by retention and normal reference GC. |
| Crash after immutable manifest upload but before CAS | Old generation remains current; the manifest is unreachable and safe to collect. |
| Lost current-pointer response | Reread current; matching generation means success, otherwise retry from observed state. |
| Current-pointer CAS loses | Never publish the candidate; reload assignment/current and discard or reuse only immutable content-identical blocks. |
| Builder assignment changes | Old builder fails the membership fence/CAS; new builder opens current and resumes from its source vector. |
| Source journal gap or epoch change | Keep serving the prior generation with stale freshness and run a complete snapshot-bound rebuild. |
| Corrupt/missing cached block | Evict and refetch from authoritative ordinary storage. |
| Authoritative block cannot meet durability/read recovery | Fail the build or query; never publish or return a partial result as complete. |
| Corrupt manifest or component | Fail closed with index identity and hash in diagnostics, excluding payload bytes. |
| Compaction failure | Input segments remain current and queryable. |
| Memory budget exhausted | Pause/yield/flush; never exceed the type limit by retaining more corpus state. |

An initial build or rebuild creates uncommitted L0 and merged segments through
the same bounded pipeline, but does not publish a partially covered generation.
The prior generation remains queryable until the full source barrier is ready.

Obsolete generations remain governed by configured maximum count, age and
authoritative bytes. The first bound reached selects oldest non-current
generations for deletion. An in-flight request is safe because it is generation
bound and the retention window exceeds the public request deadline.

## 21. Supported index-kind dry runs

This section proves that every supported index kind fits the common design. It
does not force engines to use one physical representation where their data
differs.

### 21.1 Path

The L0 builder sorts `(path, version, live-or-tombstone)` and coalesces repeated
paths. A merged segment stores a prefix-omission compressed path dictionary,
Elias-Fano offsets and compact state/version columns. Root blocks contain path
ranges. A prefix query seeks the first dictionary entry, k-way merges matching
segment iterators in UTF-8 byte order, applies greatest-version visibility and
returns authorized live paths. It never scans object heads once an initial
generation exists.

### 21.2 Metadata filter

The fixed head projection—path, version, content type, content length, content
hash and commit time—is streamed into a segment-local document table. The
initial format writes canonical typed rows by document ordinal and a second,
sorted routed-key component for configured field values. L1+ ordinal columns
use Elias-Fano; routed string keys use prefix-delta encoding. Equality, `IN`,
prefix and range predicates seek the relevant routed-key blocks and then
validate the referenced row. Ranked dense bit vectors remain a later format-2
optimization rather than an initial representation. Path/version visibility
and Zanzibar filtering happen before hits are emitted.

### 21.3 Typed JSON

The builder incrementally parses only configured JSON pointers and does not
retain complete documents. Canonical scalar keys include an explicit JSON type
so `1`, `"1"`, `true` and `null` do not collide. Arrays may contribute several
values but a document ordinal appears once in final results. L0 uses sorted
scalar runs. Merged runs store canonical selected values in ordinal rows,
Elias-Fano-compressed ordinal columns and prefix-delta routed scalar keys.
Range and ordered queries k-way merge bounded iterators without collecting the
whole result domain. One selected mutation whose canonical row cannot fit a
format-2 leaf is rejected before builder admission; the previous complete
generation and source checkpoint remain current.

### 21.4 Full text

The existing bounded tokenizer streams configured fields and emits
`(term, document_ordinal, position)`. L0 stores cheap term-sorted gap-coded
postings. Merged segments separate:

- term dictionary and posting-block routing;
- document pointers;
- per-posting term frequency and field length; and
- optional bounded positions for phrase queries.

Document pointers, counts, cumulative position offsets and positions use
quasi-succinct sequences where appropriate. A non-phrase Boolean query never
materializes positions. `advance_to` performs successor operations over
Elias-Fano postings. The initial score is deterministic and local to each
posting, using term frequency and field length, and retains only a bounded
top-k heap. Forward term vectors and signed generation-wide BM25 statistic
deltas are possible later format-2 components; they are not part of 0.6.0.

### 21.5 Vector

The vector engine remains an exact search in 0.6.0. The definition bounds
dimensions and metric. Each L0 segment stores validated fixed-width vectors,
segment-local document ordinals and path/version changes. Merged segments keep
vector blocks in a portable little-endian float representation while using
succinct ordinals, offsets and live-state structures around them.

A query streams vector blocks, computes cosine, dot-product or Euclidean score,
filters stale versions and maintains a bounded top-k heap. It does not load all
vectors or add a mutable HNSW graph. Initial scoring within a decoded block is
sequential and bounded; the process-owned Rayon pool is used for admitted
source projection work rather than as an unaccounted query allocator.
Approximate ANN or parallel scoring can be a later immutable segment component
without changing publication or cache authority.

### 21.6 Hybrid

A hybrid segment shares one path/version document table between its full-text
and vector components. It does not duplicate complete source projections.
Full-text iterators and exact vector block scans produce bounded candidate
streams; deterministic weighted fusion retains only required candidate state.
Visibility and authorization are applied to the shared document identity. A
compaction merges both component families under one replacement segment so a
generation never pairs text from one source barrier with vectors from another.

### 21.7 Git source

The fixed repository definition projects immutable commit, tree-path and object
relationships. L0 contains sorted commit/path/object tuples. Merged segments
store payload records under succinct document ordinals and use prefix-delta
routed keys for commit/path and object-ID lookup. Exact commit/path lookup seeks
one key range; prefix tree lookup streams the contiguous path range; object
lookup streams its locations. One source projection group must fit a format-2
leaf in 0.6.0 and is rejected before admission otherwise. Object bodies remain
ordinary Anvil objects and are not copied into the index.

### 21.8 Tensor

The fixed model definition projects tensor names, shapes, data types and source
object locations. L0 sorts `(model_id, tensor_name)` records. Merged segments
store canonical record groups under Elias-Fano-compressed document ordinals and
use prefix-delta routed tensor keys. Exact lookup loads the relevant routed key
and record block, then returns the authoritative source path/version subject to
visibility and authorization. One source projection group must fit a format-2
leaf in 0.6.0 and is rejected before admission otherwise. Tensor payload bytes
remain in their ordinary objects.

## 22. Mutable API versus truly mutable storage

A genuinely mutable compressed index would require in-place page updates,
write-ahead recovery, latches, split/merge coordination and erasure-stripe
rewrites. In Anvil it would also turn derived data into a new distributed
authority and make a small logical update rewrite parity-bearing storage.

Those costs are justified when a database index is part of the authoritative
transactional state. They are not justified for an asynchronous materialized
view which can be reconstructed from ordinary objects and ordered change
evidence. MG4J and modern search engines support the chosen boundary: mutable
operations are represented as immutable batches, then merged.

The design therefore does not reproduce PostgreSQL's mutable page tradeoffs.
It gives clients mutable semantics while keeping publication atomic at the
generation-manifest boundary.

## 23. Future exact substring/self-index type

An FM index or compressed suffix array serves arbitrary substring search,
whereas Anvil's full-text kind serves tokenized fields, phrase positions and
ranked document retrieval. Combining them under one kind would create ambiguous
query and scoring semantics.

A future `SUBSTRING` or `SEQUENCE` kind may use the Navarro–Mäkinen techniques:

- Burrows-Wheeler/FM backward search for substring counting;
- wavelet or rank/select structures for compact alphabet navigation;
- sampling for locate and display tradeoffs; and
- run-length representations for highly repetitive corpora.

It must still use bounded immutable segments, ordinary Anvil component blocks,
one-node query execution and generation CAS. Deletes and overwrites must pass
through the common path/version visibility layer because a static self-index
otherwise continues to contain deleted suffix occurrences. It is explicitly
outside 0.6.0.

## 24. Operational configuration

The 0.6 server exposes startup configuration for:

- construction-memory bytes per `IndexKind` (one value applied to each kind in
  0.6.0, with one independent pool per kind);
- fixed index Rayon worker count;
- index disk-cache bytes and in-flight materialization-memory percentage;
- retained generation count, age and authoritative-byte limits.

Builder and query replica routing remains derived from active membership and
is not a startup knob.

L0 byte targets, flush interval, per-level fanout and component-block size are
fixed versioned implementation constants in 0.6.0. Exposing knobs before the
production-shaped qualification establishes useful ranges would expand the
operator contract without improving correctness.

Limits are startup configured in 0.6.0. They are not mutable index definitions
and do not enter Raft. Configuration validation reports the sum of per-kind
construction budgets and rejects zero, overflow or internally inconsistent
values.

## 25. Observability

The existing tracing/OTLP path exports, without tenant payload or query text:

- configured, leased, peak and waiting construction bytes by kind;
- active, runnable and waiting definitions by kind;
- flush count, records, bytes, duration and reason;
- segment count and bytes by level and kind;
- compaction input/output bytes, amplification, duration and failures;
- source cursor, observed tail, lag, rebuild reason and initial-build state;
- generation, definition version, placement fence and publication CAS result;
- cache hit/miss/fetch/coalescing/verification/eviction bytes;
- query blocks fetched, iterators advanced, candidates considered, stale
  candidates rejected and authorization candidates rejected;
- query latency by kind and cold/warm status; and
- unreachable artifact and obsolete-generation collection.

Logs identify stable numeric tenant, bucket and index IDs where operationally
necessary. They never log source payloads, selected JSON values, query vectors,
credentials or proprietary path contents.

## 26. Validation matrix

### 26.1 Format and engine tests

The common format and the eight engines collectively test:

- empty, one-record, multi-block and multi-run generations where the engine
  supports a collection result;
- L0 query equivalence before and after compaction;
- overwrite, tombstone, duplicate replay and latest-version visibility;
- corrupt headers, hashes, bounds, counts, routing support and succinct select
  support;
- deterministic output from the same ordered logical input;
- native, generation-bound continuation for every pageable result shape; and
- rejection of format-1 definitions, generations and page-token positions.

The durable codec uses explicit fixed-width little-endian fields and tests
deterministic byte output. The release images and public qualification run on
both AMD64 and ARM64; no native Rust layout is persisted.

### 26.2 Resource tests

Shared budget, codec, routing and compaction tests prove the fixed encoded and
decoded ceilings, four-input fan-in, FIFO admission and three same-kind clients
making progress without multiplying the kind limit. Engine tests cover the
kind-specific high-risk cases: hot full-text positions, large typed values,
vectors and indivisible Git/Tensor projections.

The production-shaped qualification then samples process RSS and anonymous RSS
while a 12-field Typed JSON index consumes a corpus much larger than its
configured kind budget. It must produce multiple bounded runs, compact them,
and plateau below the explicitly accepted process-memory increase. This
representative process test complements, rather than duplicates, the public
functional matrix for all eight kinds.

### 26.3 Public single-node tests

Through the generated public client, create, build, query, update, delete and
page each of Path, Metadata Filter, Typed JSON, Full Text, Vector, Hybrid, Git
Source and Tensor. Validate returned path/version, score where applicable,
freshness and Zanzibar denial behavior.

### 26.4 Public three-node tests

Using one three-node Docker Compose cluster and isolated tenants/buckets:

- send source writes through all three ingress nodes;
- wait for the one HRW builder to consume every source journal;
- create, page, mutate and query every kind through public endpoints;
- verify Zanzibar denial, explicit public access and revocation;
- verify published index artifacts use `REPLICATED` acknowledgement when at
  least two ACTIVE nodes exist, and `LOCAL` acknowledgement on a one-node
  cluster; and
- perform a rolling restart while preserving complete published generations.

Unit tests independently cover journal gaps, incomplete atomic barriers,
stale placement fences, publication CAS loss and failure before current-pointer
publication. Exhaustive process-kill injection at every block-upload and CAS
instruction boundary is not a 0.6.0 release gate.

### 26.5 Production-shaped qualification

The release qualification uses a public or generated corpus with approximately
840,000 small JSON records and 12 indexed fields on a resource-capped node. It
must:

- complete ingest and initial publication without OOM, swap collapse or
  corpus-linear anonymous growth;
- remain below every configured per-kind construction budget plus measured
  fixed overhead;
- validate exact indexed object/version counts and representative queries;
- report ingest, initial-build and compaction wall time separately;
- report peak RSS, anonymous RSS, cache bytes, authoritative bytes, write
  amplification, segment counts and query cold/warm latency; and
- update/delete a bounded subset and demonstrate incremental L0 publication
  rather than complete generation rebuild.

No private application schema, fixture, name or production payload is checked
into the repository or release artifact.

## 27. Clean-break implementation rule

The existing index engine and whole-generation builder are deleted from the
Rust compilation graph before the replacement is considered complete. Code may
reuse ordinary object, HRW, source-journal, Zanzibar, cache-file and public
transport primitives where they satisfy this RFC. It must not preserve the old
index architecture behind adapters.

Specifically, 0.6.0 contains no:

- format-1 manifest, page-map or generation reader;
- old-to-new converter or background migration;
- dual-write or shadow-read mode;
- old generation fallback after a format-2 failure;
- compatibility feature flag;
- alternate current-pointer namespace for legacy indexes; or
- test whose purpose is to keep a 0.5.x index artifact working.

An installation which retains ordinary source objects through a separately
supported core-store upgrade may recreate index definitions and rebuild format
2 from authoritative current state. That is a new build, not index migration.
This RFC does not require retaining any 0.5.x index definition or permitting a
mixed 0.5/0.6 cluster.

Every reader rejects an unsupported format encountered inside the v2 namespace
explicitly. Discovery rejects unsupported definitions; generation loading
rejects unsupported current pointers, manifests, roots and blocks. An invalid
candidate is never CAS-published, so the preceding valid generation stays
current. Corruption of an already-published current pointer makes that one
definition unavailable and causes its builder to report and retry. Anvil does
not add a cluster-wide startup scan merely to discover dormant corrupt derived
objects. The implementation never probes a format-1 current pointer and never
mistakes it for an empty v2 index.

## 28. Release acceptance

Anvil 0.6.0 is ready to tag only when:

1. the old index implementation and dependencies used only by it are removed;
2. `sux` and Rayon licenses, features and locked dependency trees are recorded;
3. every supported kind passes its format/engine tests and the public
   single-node and public three-node matrices, while shared resource tests and
   the production-shaped run validate the common hard-memory machinery;
4. the clean-break rejection tests pass and no legacy index reader remains;
5. the production-shaped qualification passes under an explicit memory cap;
6. formatting, Clippy, file-size enforcement, locked workspace tests and both
   target architectures pass locally;
7. AMD64 and ARM64 container images are built from the exact candidate commit
   and exposed as one multi-platform image;
8. a manual public-API smoke test succeeds against the candidate image;
9. the README, feature matrix, known limitations and release notes describe the
   format break and new resource controls accurately; and
10. the validated commit is pushed, tagged `0.6.0`, released, and its published
    image digest/platform list is independently verified.

Performance tuning which does not affect correctness may follow the functional
release, but unbounded construction memory, stale/deleted hits, unauthorized
results, partial atomic visibility, false durability and inability to build any
supported kind are release blockers rather than known limitations.

## 29. References

- MG4J manual, [Scan: Building batches](https://vigna.di.unimi.it/MG4J/man/manual/ch02s03.html).
- MG4J manual, [Combining batches](https://vigna.di.unimi.it/MG4J/man/manual/ch02s04.html).
- MG4J manual, [Clusters & Partitioning](https://vigna.di.unimi.it/MG4J/man/manual/ch04.html).
- Sebastiano Vigna,
  [Quasi-Succinct Indices](https://vigna.di.unimi.it/ftp/papers/QuasiSuccinctIndices.pdf),
  WSDM 2013.
- Sebastiano Vigna et al., [`sux`: Rust implementations of succinct data structures](https://github.com/vigna/sux-rs).
- Tommaso Fontana, Sebastiano Vigna et al.,
  [`epserde`: an epsilon-copy serialization/deserialization framework](https://github.com/vigna/epserde-rs).
- Gonzalo Navarro and Veli Mäkinen,
  [Compressed Full-Text Indexes](https://users.dcc.uchile.cl/~gnavarro/ps/acmcs06.pdf),
  ACM Computing Surveys 39(1), 2007.
- Rayon developers, [Rayon: data parallelism in Rust](https://github.com/rayon-rs/rayon).
