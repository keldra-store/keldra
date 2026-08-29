# KELDRA-0018: Integrated RocksDB payload storage

Status: Implemented in Keldra 0.15.0

Audience: Keldra implementors, operators, reviewers, and performance engineers

Compatibility: This is a clean storage-format break. Keldra 0.15.0 clusters
using this format start from fresh authoritative volumes. There is no 0.14
reader, migration, fallback, dual-write period, legacy column family, or
filesystem payload compatibility path.

## 1. Decision

Keldra stores every complete payload and erasure-coded shard in one node-local
RocksDB database. A dedicated payload-artifact column family uses RocksDB's
integrated BlobDB support so large values are packed into managed blob files
instead of becoming one filesystem file per object or shard.

This changes physical persistence, not distributed ownership. Keldra continues
to replicate logical object mutations, complete payload streams, and erasure
shard streams. It never copies a live RocksDB database, WAL, SST, BlobDB file,
checkpoint, or compaction layout between nodes.

The design has one authoritative local lifecycle for every durable artifact.
Every payload value or chunk is either:

- committed atomically with a durable lifecycle and garbage-collection record;
  or
- owned by a durable, resumable installation record committed before that
  value or chunk.

No durable payload bytes may exist outside an enumerable Keldra lifecycle.
Failed publication may temporarily consume space through an explicit grace
period, but it cannot create permanently undiscoverable storage.

## 2. Motivation

The pre-0.15 layout stores values up to 64 KiB in RocksDB and stores each larger
complete payload or shard as an individual content-addressed filesystem file.
That makes a bulk containing hundreds of modest values larger than 64 KiB pay
hundreds of file creates, file synchronizations, renames, and directory
synchronizations. On spinning disks, requests well below the 64 MiB BulkWrite
limit can consequently take minutes even though metadata mutation itself is
fast.

The same layout also grows filesystem directory and inode cardinality with
payload and shard cardinality. Raising a request timeout or grouping directory
syncs does not fix that storage model.

Integrated BlobDB provides the required established machinery:

- many logical values are packed into larger managed blob files;
- keys, manifests, lifecycle state, and GC state remain in RocksDB's ordered
  and crash-recoverable model;
- one WAL orders writes across the database's column families;
- compaction rewrites live values and reclaims obsolete blob-file bytes; and
- each node remains free to compact and lay out its local database differently.

## 3. Required invariants

1. `BlobRef { hash, length }` remains the complete-payload identity.
2. `ShardIdentity` remains the erasure-fragment identity. Physical RocksDB keys
   and files never enter public or peer protocols.
3. Every durable payload value or chunk has one durable owning lifecycle.
4. Readers never expose an incomplete installation or a deleting artifact.
5. A successful `REPLICATED` response still proves the KELDRA-0010 payload
   placement requirement and the independent metadata quorum.
6. Payload preparation precedes metadata publication. Failure before metadata
   publication leaves only lifecycle-owned, age-gated artifacts.
7. Positive references are never collected while reference delivery may still
   be in flight.
8. Retrying an identical artifact is idempotent and never inflates its reference
   count.
9. A content key with missing or contradictory lifecycle state is corruption,
   not an orphan to adopt silently.
10. GC is restartable. Crashing during installation, retirement, logical
    deletion, or physical compaction cannot hide work from the next process.
11. RocksDB physical files are node-local implementation details. Repair and
    handoff transfer verified logical streams.
12. Metadata, WAL, and payload roots are one node failure domain even when they
    reside on separate devices.
13. The administrator controls the WAL target. The default total WAL target is
    50 GiB.
14. There is one 0.15 storage representation. No compatibility branch may
    become a second authority.

## 4. Non-goals

This RFC does not:

- replicate RocksDB databases or checkpoints;
- replace Keldra's metadata replica groups, payload placement, erasure profile,
  source journals, or reference-delivery protocol;
- make BlobDB files a peer protocol or backup format;
- add per-object placement records;
- add a second payload database or WAL;
- claim cross-node atomicity from a local RocksDB `WriteBatch`;
- make RocksDB compaction state authoritative;
- define a 0.14 migration or mixed-format rolling upgrade; or
- change the public `LOCAL` and `REPLICATED` durability contract.

## 5. Node-local RocksDB layout

One RocksDB database owns metadata and payload column families. The database
root, MANIFEST, OPTIONS, and WAL remain on the configured metadata roots. The
payload-artifact column family's `cf_paths` points to the configured payload
root.

```text
metadata root
  RocksDB CURRENT, MANIFEST, OPTIONS
  metadata column-family SSTs

metadata WAL root
  shared RocksDB WAL

payload root
  payload-artifact column-family SSTs
  integrated BlobDB files
```

The WAL is shared by the database. Payload values are therefore initially
recoverable from the WAL and memtable, then move to payload SST/blob files as
the payload column family flushes and compacts. This is deliberate: one local
WAL gives atomic ordering across lifecycle and payload records without making
payload and metadata devices separate authorities.

The payload root is still authoritative. Startup fails closed if any pinned
authoritative root is absent, belongs to another node, or cannot be opened.
Runtime I/O failure cannot be converted into a successful durability receipt.

Node-local backup or checkpoint tooling must include every configured RocksDB
path. A checkpoint is an operational backup of that node, never cluster
replication authority.

## 6. Payload-artifact keys and values

The payload-artifact column family contains tagged local physical keys. The
tags distinguish complete payloads from erasure shards and inline values from
chunk values.

Conceptually, a manifest maps the public logical identity to a node-local
physical storage identity:

```text
CompleteManifest = BlobRef -> ArtifactManifest(storage_id, ...)
ShardManifest    = ShardIdentity -> ArtifactManifest(storage_id, ...)

CompleteInline = complete-tag || storage_id
CompleteChunk  = complete-chunk-tag || storage_id || chunk_ordinal

ShardInline    = shard-tag || storage_id
ShardChunk     = shard-chunk-tag || storage_id || chunk_ordinal
```

Keys use the storage format's canonical binary encoding, not JSON, paths, or
hexadecimal filenames. `storage_id` is deterministic for ordinary complete and
shard installation, and random for an upload whose final `BlobRef` is not yet
known. The manifest is the only published mapping from logical identity to
physical values. Each storage identity determines one bounded prefix, allowing
bounded deletion without a filesystem inventory.

The local manifest records:

```text
ArtifactManifest {
  format
  identity_kind = Complete | Shard
  encoded_length
  layout = Inline | Chunked { chunk_bytes, chunk_count }
  integrity
  storage_id
}
```

`integrity` binds the complete BLAKE3 identity or the erasure fragment's frozen
format and checksum rules. The manifest does not replace `BlobRef` or
`ShardIdentity` in distributed protocols.

Values at or below the local chunk threshold use one logical RocksDB value.
Larger streams use fixed bounded chunks. The initial 0.15 storage format uses
an 8 MiB chunk size. Changing that constant requires an explicit later storage
format decision; it is not silently inferred from local hardware.

RocksDB integrated BlobDB is enabled for the payload-artifact column family.
Its minimum blob-value size is 64 KiB. Smaller payload values remain in SSTs;
larger inline values and chunks are stored in managed blob files. Both are
addressed through the same Keldra column-family API.

## 7. Why chunking is required

The public maximum object size defaults to 16 GiB. RocksDB `Put` and
`WriteBatch` accept complete value slices; they are not streaming object APIs.
Putting a multi-gigabyte object into one value would require an unbounded memory
allocation and an enormous WAL record.

Client streaming therefore remains bounded without a filesystem staging copy:

1. `StartPut` allocates a random node-local upload storage identity and hashes
   bytes while retaining at most one 8 MiB chunk in memory.
2. Every full chunk is written directly to the payload-artifact column family
   together with an `ArtifactInstall` update and a replaceable GC due entry.
3. `PutEnd` computes the final `BlobRef`, writes the final partial chunk, and
   verifies the complete stream from RocksDB.
4. One atomic RocksDB batch publishes the logical `BlobRef` manifest over the
   same physical storage identity, removes the upload installation/due record,
   and creates the awaiting-publication lifecycle.
5. No payload bytes are copied during promotion. Upload chunks become the
   complete artifact in place.

Streams no larger than one chunk remain in bounded memory and join the final
RocksDB transaction directly. Larger streams pay only the RocksDB WAL/memtable
write and the engine's eventual SST/BlobDB placement; there is no temporary
filesystem payload write before those engine writes.

Peer payload and shard RPCs already know the expected identity and may stream
directly into a lifecycle-owned installation. They still verify the complete
stream before sealing it.

If a stream is cancelled, fails validation, or the process crashes after any
chunk is durable, its `ArtifactInstall` and due entry remain enumerable. The
ordinary blob-GC worker deletes that exact storage prefix after the configured
grace. There is no request-finalizer-only cleanup and no unreachable orphan.

## 8. Artifact lifecycle

The 0.15 lifecycle is an explicit state machine rather than an implicit
filesystem convention. Its implementation deliberately reuses the existing
`BlobReferenceState` authority for awaiting/referenced/retired state and adds a
separate exact `ArtifactInstall` record for incomplete chunks; it does not
duplicate reference counts in a second codec:

```text
Uploading(random storage_id)
    | PutEnd identity verification and atomic promotion
    v
AwaitingPublication

Installing
    | complete verification
    v
AwaitingPublication
    | first committed positive reference
    v
Referenced(count > 0)
    | last committed negative reference
    v
Retired
    | grace elapsed and GC safety proven
    v
Deleting
    | all payload keys and metadata deleted
    v
Absent
```

Conceptually:

```text
ArtifactLifecycle {
  format
  identity
  revision
  created_at_unix_millis
  updated_at_unix_millis
  state =
    Uploading {
      storage_id,
      next_chunk_ordinal
    }
    | Installing {
      exact_manifest,
      next_chunk_ordinal
    }
    | AwaitingPublication
    | Referenced { ref_count }
    | Retired
    | Deleting { next_chunk }
}
```

The exact persisted codecs are closed and versioned. A sealed manifest is the
read-visibility boundary. Unknown formats, states, flags, impossible counts,
reversed timestamps, invalid progress, or identity mismatches fail closed.

Every transition runs under the local artifact lock and RocksDB commit fence.
Callers supply the expected lifecycle revision. A stale installer, publisher,
reference delivery, or collector cannot overwrite a newer state.

### 8.1 Installing

For a normal one-value artifact, Keldra can atomically write the value,
manifest, `AwaitingPublication` state, and GC due entry in one synchronous
batch; no observable `Installing` state is needed.

For a chunked artifact whose logical identity is already known, Keldra first
synchronously writes its awaiting-publication lifecycle, exact
`ArtifactInstall`, and lifecycle due entry before writing any chunk. Each
subsequent bounded batch atomically writes one chunk value and advances the
installation ordinal.

An ingress upload whose final hash is not yet known instead uses a random
storage identity. Its first bounded chunk transaction creates an upload-kind
`ArtifactInstall` and upload due entry; later transactions atomically advance
both the chunk ordinal and due revision. It has no `BlobReferenceState` and is
not readable as a logical artifact. At `PutEnd`, complete verification proves
the final `BlobRef`, and the promotion batch replaces upload authority with the
published manifest and awaiting-publication lifecycle without moving values.

Readers reject `Uploading` and `Installing`. Known-identity installation uses
the content identity plus its exact immutable manifest. Unknown-identity upload
uses its unguessable storage identity and can only be promoted by a complete
hash/length verification. A contradictory manifest or noncontiguous ordinal
cannot append to or seal either installation.

After all expected chunks are present, Keldra verifies the complete payload or
shard. One final synchronous batch writes the manifest and removes the
installation records. The already-durable awaiting lifecycle is unchanged;
the manifest makes the artifact readable.

### 8.2 Awaiting publication

`AwaitingPublication` means the bytes are durable but no delivered positive
reference has claimed them yet. It is not a public object reference and is not
counted as one.

Preparing identical already-awaiting content verifies and touches the existing
artifact without creating another reference. Preparing content already in
`Referenced` verifies and reuses it without downgrading its lifecycle.

The awaiting-publication TTL remains administrator configurable and defaults
to 24 hours. Server validation requires it to cover every publication,
lost-response, and atomic replay window which can legitimately consume prepared
evidence. A coordinator may not publish from expired evidence.

### 8.3 Referenced and retired

The first positive reference changes `AwaitingPublication` to
`Referenced { ref_count: 1 }`. Later positive references increment exactly
once. Negative references decrement exactly once. Same-content replacement has
zero net effect.

The last negative reference changes the state to `Retired` and writes a due
entry. Underflow, missing predecessor evidence, or a reference gap is
corruption and disables collection for the affected authority.

### 8.4 Deleting

The 0.15 key encoding gives every chunked artifact one bounded RocksDB key
prefix. Under the artifact/commit fence, GC exact-revalidates eligibility and
atomically writes a prefix range tombstone together with deletion of the
manifest, installation records, lifecycle, and due entries. There is no
multi-commit filesystem deletion to resume and therefore no redundant
persisted `Deleting` progress record in this format. The conceptual
`Deleting` state is the single atomic deletion batch.

## 9. Durable GC due index

GC discovery uses an ordered RocksDB column family, not payload-file scans.

Conceptually:

```text
GcDueKey = due_at_unix_millis || artifact_kind || artifact_identity
GcDueValue = expected_lifecycle_revision
```

Writing or changing an eligible lifecycle atomically replaces its prior due
entry. The collector scans due keys in order using bounded record, byte, and
wall-time budgets. It exact-reads the lifecycle again under the commit fence
before changing anything.

A stale due entry is deleted. An entry whose lifecycle revision changed is
never applied to the new state. A collector crash merely restarts an ordered
prefix scan; correctness does not depend on its in-memory cursor.

There is no canonical payload-directory inventory, upload staging directory,
startup payload scan, or orphan adoption. Incomplete uploads are found only
through their ordered GC due records and exact installation identities.

## 10. Orphans and failed publication

Payload preparation and metadata publication cannot be one cross-node
RocksDB transaction. Keldra therefore uses ordered durability:

1. selected payload owners durably install and verify their artifacts;
2. the path coordinator verifies placement evidence;
3. the metadata mutation commits to its logical replica quorum; and
4. positive reference effects reach the required payload owners.

Failure before step 3 can leave `AwaitingPublication` artifacts. They are safe
for correctness and bounded for operations:

- each is present in the durable due index;
- each is measured as awaiting bytes;
- retry may reuse it;
- after the grace and reference-safety checks, GC moves it to `Deleting`; and
- RocksDB key deletion makes it logically absent before BlobDB compaction
  reclaims its physical bytes.

There is no request-finalizer-only cleanup. Cancellation, timeout, panic,
process exit, and node restart all leave durable GC authority behind.

## 11. Reference-delivery safety

KELDRA-0010's source-journal and reference-delivery authority remains in force.
Artifact GC runs only when the local reference runtime proves it is current
through the relevant source prefixes. A delayed negative reference may retain
extra bytes. A missing or delayed positive reference must never allow live
bytes to be removed.

If a source journal is unavailable, has a gap, or requires count
reconstruction, collection pauses for the affected authority. Keldra prefers
temporary excess storage to deleting a referenced payload.

`REPLICATED` publication still waits for the required selected payload owners
to hold verified bytes and the required positive reference effects. `LOCAL`
may return after the upload source and metadata quorum are durable, then use
the same reference-delivery and placement convergence machinery in the
background.

## 12. Replication, repair, and handoff

RocksDB is never the replication unit.

Metadata replication sends typed logical mutations to the exact-path replica
group. Payload replication sends a verified complete payload stream or erasure
shard stream to the owners selected from `BlobRef` and committed membership.
The receiving node persists that stream into its own local payload-artifact
column family and returns evidence only after its synchronous local seal.

Nodes may have different:

- RocksDB sequence numbers;
- WAL boundaries;
- SST files and levels;
- BlobDB files;
- chunk installation timing; and
- compaction history.

None of those differences affect content identity or placement.

Repair obtains a valid logical artifact from surviving owners, verifies it
while streaming, and installs it through the same receiving lifecycle. Handoff
transfers logical metadata snapshots plus only the payloads or shards the new
placement selects. Neither operation copies database files.

## 13. Bulk and grouped installation

The storage design must remove per-object synchronization amplification, not
merely relocate it into RocksDB calls.

Single-node BulkWrite groups normal-sized payload seals into bounded RocksDB
`WriteBatch` commits. One batch may contain many independent payload values,
manifests, lifecycles, and due entries. The hard public BulkWrite bound remains
64 MiB encoded, while internal batch construction accounts for keys, values,
lifecycle records, WAL encoding, and configured resident memory.

One item that does not fit the current batch is installed alone or through the
chunked path. It is not rejected merely because it exceeds a normal grouping
target while remaining within the public object-size limit.

Distributed receivers may likewise accept bounded groups, but batching never
changes placement evidence: every returned item independently binds its exact
identity and durable outcome. A malformed or failed item cannot be hidden by
other successes.

## 14. WAL administration

The administrator configures:

```text
KELDRA_MAX_TOTAL_WAL_BYTES
```

The default is:

```text
53,687,091,200 bytes  # 50 GiB
```

The value must be non-zero and large enough for the maximum admitted internal
write batch plus recovery headroom. Keldra passes it to RocksDB's
`max_total_wal_size`.

RocksDB's option is a flush trigger, not an exact filesystem quota. Keldra
separately bounds each public and internal write batch; large objects use 8 MiB
installation commits rather than one object-sized WAL record. RocksDB applies
its normal write stall when flush/compaction cannot keep pace. Recovery,
reference delivery, GC state transitions, and metadata needed to make progress
remain bounded; an operator must still capacity-plan the WAL device rather
than treating this target as a disk quota.

The WAL root must be durable storage, never tmpfs. Startup logs the configured
limit and root. Metrics expose current WAL bytes, configured bytes, flush
pressure, stalls, and payload admissions delayed by the bound.

## 15. BlobDB reclamation

Deleting a Keldra payload key makes it logically absent. RocksDB may retain the
obsolete value in a blob file until compaction rewrites the live keys and drops
garbage.

The payload column family enables integrated BlobDB garbage collection.
Automatic compaction handles active workloads. A one-hour periodic-compaction
setting also makes RocksDB reconsider a quiescent payload column family which
still contains logically deleted garbage; it does not require Keldra to scan
individual blob files or manufacture liveness. Foreground writes retain
RocksDB's ordinary compaction scheduling and backpressure priority.

This distinction is operationally visible:

- `logical live bytes` are authoritative referenced payload bytes;
- `reclaimable bytes` are deleting/deleted values not yet compacted; and
- `physical payload bytes` are payload SST and BlobDB file usage.

Reclaimable bytes are engine-managed and recoverable by compaction. Growth
which exceeds compaction throughput is capacity pressure and must trigger
metrics/backpressure; it is never hidden as an unreachable Keldra orphan.

## 16. Crash and retry matrix

| Failure point | Durable result | Recovery |
| --- | --- | --- |
| Before the first upload-chunk transaction | No payload keys | Nothing to recover |
| After an upload chunk transaction, before `PutEnd` | Upload installation plus exact GC due entry | Age-gated GC deletes the upload storage prefix |
| After `Installing`, before all chunks | Lifecycle-owned partial chunks | Resume with exact generation or age-gated delete |
| After chunks, before verification | Still `Installing` | Verify and seal, or delete |
| After seal, before metadata commit | `AwaitingPublication` | Retry may reuse; otherwise due-index GC |
| Metadata committed, positive reference delayed | Prepared bytes plus source-journal authority | Reference delivery resumes; GC remains disabled for the gap |
| During positive reference transition | Old or new state atomically | Idempotent source-position replay |
| During artifact deletion batch | Old or absent state atomically | Retry exact GC selection if old |
| After RocksDB key deletion, before compaction | Logically absent, physically reclaimable bytes | BlobDB compaction reclaims bytes |
| During repair or handoff | Destination installation is incomplete or sealed | Resume/retry exact logical transfer |

## 17. Corruption and fail-closed behavior

Keldra reports data loss and refuses to publish or read when it observes:

- payload values without lifecycle authority;
- lifecycle or manifests without their required payload keys;
- an inline value whose length or hash is wrong;
- a chunk gap, duplicate ordinal, invalid encoded length, or invalid final hash;
- a shard whose frozen format or checksums do not validate;
- a lifecycle revision or installation generation mismatch;
- a reference underflow or source-position gap;
- an impossible GC due record; or
- required metadata, WAL, or payload storage paths which are unavailable.

GC never attempts to “repair” contradictory authority by guessing whether bytes
are live. Repair starts from an independently proven valid logical owner.

## 18. Configuration

| Setting | Default | Meaning |
| --- | ---: | --- |
| `KELDRA_MAX_TOTAL_WAL_BYTES` | 50 GiB | WAL flush/admission high-water target |
| `KELDRA_AWAITING_PUBLISH_TTL_SECONDS` | 24 hours | Minimum inactivity before unpublished or retired artifacts become eligible |
| `KELDRA_MAX_BLOB_BYTES` | 16 GiB | Maximum accepted complete object size |
| `KELDRA_PENDING_UPLOAD_MAX_BYTES` | maximum object size | Process-wide admission bound across unfinished uploads, including their lifecycle-owned RocksDB chunks |
| payload chunk size | 8 MiB fixed by storage format | Maximum logical value chunk before RocksDB encoding overhead |
| BlobDB minimum value | 64 KiB fixed by storage format | Values at or above this size are eligible for BlobDB files |

GC work budgets remain bounded by record count, logical bytes, and wall time.
They may be exposed separately, but changing a scheduling budget cannot change
liveness or eligibility semantics.

## 19. Observability

Required metrics and logs include:

- payload installs started, resumed, sealed, failed, and expired;
- inline values and chunk values written;
- complete and shard logical bytes;
- `Installing`, `AwaitingPublication`, `Referenced`, `Retired`, and `Deleting`
  artifact/byte gauges;
- oldest installation and awaiting-publication ages;
- GC due records inspected, deleted, stale, blocked, and failed;
- logical payload bytes deleted per tick;
- BlobDB live bytes, garbage bytes, file count, and compaction progress;
- payload-column-family SST bytes;
- total WAL bytes, configured WAL target, flushes, stalls, and admission waits;
- grouped seal items, bytes, batches, and synchronous write duration; and
- per-phase payload preparation, placement, metadata replication, reference
  delivery, and total public request duration.

A request timeout reports the last incomplete phase. Cancellation must not be
the only observer or cleanup owner for work already made durable.

## 20. Clean-break startup and capability

Keldra 0.15 using this storage contract accepts only fresh volumes initialized
with the new storage capability. It does not inspect, import, or delete a 0.14
payload directory.

Startup rejects:

- pre-0.15 storage capability markers;
- a `small_blobs` authoritative layout;
- canonical filesystem blob or shard roots from the prior format;
- missing new payload column families;
- an incompatible payload-artifact codec; and
- a node whose committed cluster storage capability differs from its local
  binary or volume.

There is no upgrade shim. An operator moving from 0.14 creates a fresh 0.15
cluster and imports application data through the public API as new writes, if
they choose to move data at all.

## 21. Implementation boundaries

The implementation should preserve these responsibilities:

- Store payload module: local artifact keys, manifests, lifecycle transitions,
  bounded installation, exact reads, and logical deletion.
- Store GC module: due-index scan, reference-safe retirement, restartable
  deletion, and BlobDB reclamation telemetry.
- Payload distribution: logical complete/shard streaming, placement evidence,
  and receiving-node durable seals.
- Object distribution: metadata quorum ordering and durability response rules.
- Reference delivery: source-prefix continuity and exact positive/negative
  effects.
- Handoff/repair: logical artifact transfer and verification.
- Server configuration: WAL limit validation, authoritative paths, and metrics.

No module may bypass the lifecycle by writing the payload-artifact column
family directly.

## 22. Required qualification

The release is not qualified by unit tests alone. Required evidence includes:

1. Fresh one-node and clustered initialization with the new storage capability.
2. Public Put/Get/Delete, retained versions, clone, link write-through, BulkWrite,
   atomic programs, watches, and indexing over payloads on both sides of 64 KiB.
3. A realistic bulk containing hundreds of values just above 64 KiB on SSD and
   spinning disks, proving grouped RocksDB persistence rather than per-item
   filesystem synchronization.
4. Objects above the inline threshold and multi-gigabyte streamed objects,
   proving bounded resident memory and WAL batches.
5. Complete-copy fallback in an undersized cluster and erasure shards at the
   configured width.
6. Node loss after payload preparation but before metadata commit.
7. Process crash after every lifecycle transition and during chunk installation
   and deletion.
8. Cancellation and deadline expiry with durable prepared artifacts, followed
   by expiry and verified GC reclamation.
9. Positive-reference delay and source-journal gap, proving GC remains disabled.
10. Repair, add-node handoff, removal, and restart with independently compacted
    RocksDB layouts.
11. Sustained orphan creation followed by a quiescent period, proving logical
    deletion and eventual BlobDB physical reclamation.
12. WAL high-water pressure using an administrator-selected value, proving
    bounded payload admission and continued recovery/GC progress.
13. Missing metadata, WAL, and payload mounts, each failing startup closed.
14. Corrupt manifests, chunks, lifecycle states, and payload values, each
    producing explicit failure without adoption or silent deletion.

Record source revision, topology, durability, storage devices, RocksDB options,
WAL target, object distribution, bytes, item count, concurrency, latency,
resident memory, physical writes, logical/physical payload size, GC outcome,
query responsiveness, and final exact-content verification.

The deterministic regression suite includes:

- a Store-level 807-operation bulk whose object payloads total 63,016,190
  bytes, every value is above 64 KiB, and a forced payload-CF flush
  proves 807 logical artifacts become only a handful of managed RocksDB files;
- the same exact 807-operation/63,016,190-byte request through the public gRPC
  `BulkWrite` boundary with the production 30-second server deadline;
- complete values and erasure shards on both sides of the 8 MiB chunk boundary;
- interrupted chunk installation, restart, age gating, and exact logical GC;
- cancellation and restart after one and several directly persisted upload chunks, proving due-index GC reclaims every physical value; and
- hard rejection of a missing payload root, missing format marker, nonempty
  fresh payload root, and the prior `small_blobs` column-family layout.

## 23. Supersession

This RFC supersedes only the physical payload persistence, lifecycle codec,
filesystem staging, and filesystem GC portions of KELDRA-0009 and KELDRA-0010.

KELDRA-0010 remains authoritative for:

- weighted-HRW metadata and payload placement;
- complete-copy behavior in undersized clusters;
- fixed erasure profiles and distinct shard ownership;
- `LOCAL` and `REPLICATED` response evidence;
- source-journal-based reference delivery;
- repair and membership handoff; and
- the prohibition on physical RocksDB replication.

Where those RFCs refer to `small_blobs`, canonical blob files, shard files,
filesystem quarantine, or directory inventory, this RFC's integrated RocksDB
artifact store and lifecycle replace that physical mechanism for 0.15.
