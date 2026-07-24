---
title: CoreStore Operations
description: Operate Anvil's recovery boundary: durable blobs, refs, streams, fences, source records, derived views, repair, and bypass detection.
---

# CoreStore Operations

CoreStore is Anvil's durable substrate. Tenants do not call it directly, and operators should not edit it directly, but almost every serious recovery question eventually comes back to it: which object version committed, which ref was current, which stream record was visible, which derived index generation was built from which cursor, and whether a repair can rebuild a view from source records.

For operators, CoreStore is the recovery boundary rather than just an implementation detail. If a node loses the durable records below `STORAGE_PATH`, Anvil may lose object bodies, metadata history, tenant credentials, authorisation records, task state, gateway records, PersonalDB commits, mesh lifecycle records, repair findings, and audit evidence. If a derived index is lost but the source records remain intact, Anvil can often rebuild. If the source records are lost, a derived index is not a substitute for recovery.

Read this page with [CoreStore](/learn/corestore/), [Backup and Recovery](/operators/backup-and-recovery/), [Repair and Diagnostics](/operators/repair-and-diagnostics/), [Observability](/operators/observability/), and [Security Hardening](/operators/security-hardening/). For consistency concepts, see [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/) and [Watches and Derived Data](/learn/watches-and-derived-data/).

## Source records and derived views

Anvil features are easier to operate when you separate source records from derived views. A source record is the durable evidence that something happened: an object version was written, an authz tuple changed, a bucket was created, an append record was accepted, a PersonalDB changeset was witnessed, or a mesh record changed. A derived view is a structure built from those records: a path listing, a full-text segment, a vector index, a userset index, a projection, or a routing projection.

CoreStore is where source records and the durable evidence for derived views are stored. The product features still have their own schemas, hashes, signatures, and validation rules; a full-text segment is not the same as an authz tuple. The operational discipline is that committed feature truth is written through CoreStore primitives rather than through private feature-owned side files.

That distinction matters during an incident. If a query result is wrong, first ask whether the source records are sound and whether the derived view has caught up. If the source is sound and the derived view is stale or damaged, repair should rebuild the derived state from the source. If source records are missing or corrupt, the response is backup restore, integrity investigation, or a product-specific recovery plan, not a blind index rebuild.

## The primitives operators need to recognise

CoreStore gives feature code a small vocabulary:

| Primitive | Operator meaning | Typical failure signal |
| --- | --- | --- |
| Blob, or durable object | Immutable bytes with a hash, logical size, manifest, encoding profile, and placement evidence. | Missing shard, hash mismatch, manifest mismatch, read quorum failure. |
| Ref | A named mutable pointer to a target, with a generation. | CAS generation mismatch, target mismatch, missing required ref, unexpected delete. |
| Stream | Ordered records with sequence, cursor, previous event hash, event hash, payload hash, and optional idempotency key. | Stream head mismatch, cursor expiry, idempotency conflict, lagging consumer. |
| Mutation batch | A group of ref updates and stream appends made visible under shared preconditions. | Cross-partition rejection, stale fence, failed stream-head precondition. |
| Fence | A time-bounded ownership token tied to an authenticated principal. | Stale worker rejection, expired owner, owner/principal mismatch. |
| Cursor | A resumable position in stream history. | Watch lag, expired checkpoint, derived state behind source. |

A ref CAS failure is not automatically a storage outage. It usually means another writer changed the head first. A stream-head mismatch is not the same as a missing disk file; it means the append was based on an older stream head. A fence failure is usually a correctness success: Anvil rejected a stale or unauthorised worker before it could publish state.

## What lives under `STORAGE_PATH`

`STORAGE_PATH` is the server-owned durable directory. The current server creates it if needed and also creates a `tmp/` directory for transient upload staging. CoreStore creates `_core/` below that path. The local backend currently stores:

- blob shards under local data-replica directories;
- object manifests under local control replicas;
- stream data and stream-name directories;
- ref values and ref-name directories;
- transaction records;
- staging and lock files used while writes are being coordinated.

Node identity and the Ed25519 receipt-signing private key are node-local CoreMeta records inside `STORAGE_PATH`. They are not tenant feature records, but they are operationally critical: losing the volume loses the identity, while duplicating the volume across live processes duplicates it. The signing public key committed in lifecycle topology must match the private identity held by that node.

Treat `STORAGE_PATH` as a stateful volume, not a cache. It needs persistent storage, backup, free-space alerts, inode alerts, permissions that prevent ordinary application containers from writing it, and placement on disks that match your durability expectations. It should not be shared as a writable directory between unrelated Anvil processes unless the deployment model is explicitly designed and tested for that access pattern.

## Current local durability model

The current CoreStore backend is a local backend. It is structured around placement, manifests, root catalogs, and quorum profiles, but those should not be read as a claim that full distributed erasure-coded production storage is complete today.

For blob bytes, the local backend encodes data with a Reed-Solomon `4+2` profile and writes six local shard files named as local nodes. Reads need enough valid shards to reconstruct the data, verify shard hashes and sizes, and verify the final object hash. That protects against some local missing/corrupt shard cases during reads, but it is not the same as remote multi-node shard placement with automated cluster-wide repair.

For control records such as manifests, refs, streams, transactions, stream directories, and ref directories, the local backend writes five local control replicas and requires a write/read quorum of three matching replicas. These replicas are local files in the same storage path. They give the implementation quorum semantics and test coverage, but they do not by themselves protect against losing the underlying volume.

The consequence for operators is simple: do not market or run the current local backend as if it were already a complete distributed storage fabric. Protect the volume, monitor it, back it up, and plan restore drills. Future distributed backends can sit behind the same API, but the current operational runbook must match the current implementation.

## Blobs, manifests, and payload recovery

When a tenant writes an object through the public API, the object body is staged temporarily, then written as a CoreStore blob. Metadata records refer to the resulting `CoreObjectRef`, which includes the content hash, logical size, and manifest reference. Reads use that reference to load the manifest, verify it, load shards, reconstruct if enough shards are present, and verify the final bytes.

This gives operators useful evidence. If a read fails with a manifest mismatch or blob hash mismatch, the problem is integrity and recovery, not authorisation. If a read fails because a bucket locator points to another region, the problem is routing or placement, not object bytes. If an object metadata record says an older object is not CoreStore-backed, that points at legacy or migration state that needs explicit handling.

Backups should preserve both the blob shards and the control records that point to them. Copying only files that look like payload bytes is not enough; without manifests, refs, streams, and metadata records, the bytes are not recoverable as Anvil objects.

## Refs, CAS, and mutable heads

Refs are the small mutable heads that tell Anvil which durable target is current. CoreStore refs are generationed and changed with compare-and-swap preconditions: require absent, require present, expected generation, expected target, optional fence, optional authz revision, and optional source watch cursor.

Operators will see CAS failures during races, stale controllers, duplicate automation, and repair attempts based on old reads. The safe reaction is not to force-write the ref. Reread the state, understand who changed it, and only retry if the product logic still holds. A stale CAS is often what prevented a lost update.

Ref update history is also stream-backed. That matters for recovery: if a current ref value is missing but the update stream is intact, code can reconstruct the current value from ref updates. Operators should still treat that as repair territory, not a reason to hand-edit JSON under `_core/replicas`.

## Streams, append records, and cursors

Streams are ordered history. Object metadata journals, bucket journals, control journals, authz tuple journals, append-stream records, task journals, gateway records, repair findings, diagnostics, and root catalog history all use stream-shaped CoreStore state in current code.

A stream record carries sequence and cursor information plus a hash chain. Appends can include idempotency keys. Replaying the same key with the same payload returns the original receipt; replaying the same key with different bytes is an idempotency conflict. This is the storage-level version of retry safety.

Streams can be sealed into segment blobs for compaction or archival. Segment sealing is storage maintenance; it is not logical stream closure. A logical append stream can continue after a segment is sealed unless the feature itself adds a separate closed state. Operators should therefore monitor compaction and stream lag without assuming a sealed segment means no more writes will appear.

Consumers of streams should store checkpoints only after their derived work is durable. If an indexer, projection builder, or audit exporter stores a cursor before its own output commits, a restart can skip records. If it stores the cursor after output commits, a restart may replay a record but can rely on idempotency and CAS to avoid publishing duplicate state.

## Compaction and lag

CoreStore streams and feature journals grow as source records accumulate. Anvil currently has object metadata compaction thresholds: `OBJECT_METADATA_COMPACTION_FRAME_THRESHOLD` defaults to `4096`, and `OBJECT_METADATA_COMPACTION_BYTES_THRESHOLD` defaults to `67108864`. When a bucket's uncompacted metadata frames or encoded bytes pass those thresholds, Anvil schedules an object metadata compaction task. The worker seals object metadata and directory segments and logs the sealed generation.

Compaction is not garbage collection of source truth. It packages history into segment evidence so reads, listings, and repairs can be efficient. If compaction falls behind, operators may see slower listings, larger backup deltas, more storage growth, or delayed repair checks. If compaction runs too aggressively on a busy deployment, it can compete with foreground work. Tune thresholds deliberately and watch the task queue and logs.

Lag appears in several forms: object and bucket watches behind source streams, index generations behind object metadata cursors, authz derived usersets behind tuple revisions, PersonalDB projections behind commits, and mesh routing projections behind lifecycle records. Current observability defines names such as `watch_stream_lag`, `compaction_backlog`, `full_text_indexing_lag`, `vector_indexing_lag`, `authz_derived_index_lag`, `personaldb_projection_lag`, and `repair_findings`, but export and dashboard integration are deployment work. Do not assume every metric has a turnkey public endpoint in the current repository.

## Repair and backup posture

Repair should rebuild or validate derived state from source records. It should not synthesize committed object versions or PersonalDB commits out of nothing. Current repair finding code explicitly rejects repair actions that would synthesize those kinds of committed source state. That is the right boundary: repairs can rebuild derived indexes, manifests, projections, and routing records from evidence, but source-record loss is a backup and recovery problem.

Use diagnostics before repair. For example:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source index \
  --tenant-id acme \
  --bucket-name documents \
  --severity error \
  --limit 50
```

This proves that the admin listener is reachable, the caller has `view_diagnostics`, and index diagnostic records for that tenant and bucket can be read. It does not scan every CoreStore blob, prove backup recoverability, or prove that all derived views are current.

A focused repair should name the smallest scope that matches the finding:

```bash
anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind directory-index \
  --tenant-id acme \
  --bucket-name documents \
  --audit-reason 'rebuild documents directory index after diagnostic DIAG-1842'
```

This asks the server to repair one derived directory index from source records and records an admin audit reason. It does not repair object payload shards, restore deleted source records, or make unrelated indexes current. After a repair, rerun the failing read, list, query, or diagnostic and check lag.

Backups should be volume-level or otherwise consistent across CoreStore blobs, refs, streams, transactions, feature records, node identity material, and the external secrets needed to decrypt server-side encrypted data. A backup of `STORAGE_PATH` without `ANVIL_SECRET_ENCRYPTION_KEY` and previous key history may be unreadable for stored secrets. A secret without storage is not a backup. Restore into an isolated environment and prove public reads, object writes, admin auth, index queries, watches, PersonalDB reads, and gateway access before treating a backup strategy as complete.

## Reserved namespaces and bypass attempts

Public object keys under Anvil-owned prefixes are reserved. Current code rejects keys at or below these prefixes: `_anvil/meta/`, `_anvil/index/`, `_anvil/authz/`, `_anvil/watch/`, `_anvil/personaldb/`, `_anvil/git/`, and `_anvil/tmp/`. Native object APIs and the S3 gateway use the reserved-namespace checks before treating those keys as tenant data.

Reserved namespace rejections are security signals. They may be an accidental client bug, a scanner, or an attempt to read or forge internal state. Monitor `UnauthorizedReservedNamespace` errors and the `reserved_namespace_rejection_count` signal where your telemetry export exposes it. A spike should lead to request-id review, tenant/app identification, and gateway log checks, not to relaxing the reserved namespace list.

The same bypass principle applies to the filesystem. Application services should not mount `STORAGE_PATH`, write `_core` records, or create product data files beside CoreStore. Admin automation should use `anvil-admin` and the private admin API, not direct storage edits. Direct writes bypass bearer-token authentication, system-realm authorisation, public policy checks, CAS, fences, stream hashes, audit records, and repair evidence.

## Detecting bypasses in code and operations

Bypass detection is part release review, part runtime hygiene. In code review, any new durable feature state should be easy to classify:

| Write path | Expected classification |
| --- | --- |
| `CoreStore::put_blob`, `compare_and_swap_ref`, `append_stream`, or `commit_mutation_batch` | Durable CoreStore-backed state. Review schema, preconditions, idempotency, and repair story. |
| Writes to `Storage::temp_dir_path()` or `_core/staging` | Temporary staging. Review cleanup and crash retry behaviour. |
| Node-local identity/signing rows and bootstrap credential output | Operator/bootstrap state. Review secret handling, lifecycle consistency, and backup implications. |
| `tokio::fs`, `std::fs`, or `OpenOptions` writing feature data elsewhere under `STORAGE_PATH` | Potential bypass. Require a clear reason or move the state behind CoreStore. |

A practical release review can search for filesystem writes and then classify each hit. That search proves only that you looked for suspicious write paths; it does not prove correctness by itself:

```bash
rg -n "tokio::fs|std::fs|OpenOptions|File::create|write_all" anvil-core/src anvil/src
```

At runtime, look for unexpected files under the durable volume. `tmp/` should contain transient upload staging, not long-lived feature truth. `_core/` should be owned by the Anvil server. Unknown top-level directories or rapidly growing non-CoreStore files deserve investigation. Do not delete them during an incident until you know whether they are temporary, identity material, or a bug-created side store.

## Readiness checks

Before declaring a deployment ready, check the recovery boundary deliberately:

| Check | What it proves | What it does not prove |
| --- | --- | --- |
| `STORAGE_PATH` is on a persistent volume with free byte and inode alerts. | The server has a place to keep durable state and operators will see capacity pressure. | That backup, restore, or shard integrity works. |
| The Anvil process user owns `STORAGE_PATH`; application containers do not. | Direct storage bypass is less likely. | That all code paths use CoreStore correctly. |
| A tenant smoke test creates a bucket, writes an object, reads it back, and lists it. | Public object path, metadata path, and current placement work for that bucket. | That every derived index or gateway protocol is healthy. |
| Admin diagnostics are readable. | The private admin listener, token, system-realm relation, and diagnostic backend are usable. | That diagnostics cover every CoreStore integrity condition. |
| Restore drill starts from backup in an isolated environment. | The backup boundary and secret material are sufficient to start and read data. | That production failover is automatic or data loss is impossible. |
| Release review classifies direct filesystem writes. | New feature state is less likely to bypass CoreStore. | That runtime operators can ignore logs, repairs, or dashboards. |

For a quick operator view of mesh and routing-derived health, a read-only admin diagnostic is appropriate:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source mesh \
  --limit 50
```

This proves admin diagnostic access for the mesh sources currently exposed by the service. It does not prove disk durability or inspect every blob. Pair it with filesystem capacity checks, feature smoke tests, and restore drills.

## Current surfaces and limits

The current public and admin CLIs expose feature diagnostics and repairs, not a general `corestore fsck` command. There is no single supported command that walks every CoreStore blob, stream, ref, transaction, shard, and manifest and certifies the volume. Operators should combine feature diagnostics, repair findings, logs, telemetry, backup validation, and restore drills.

The local backend's `4+2` shard layout and five local control replicas are useful correctness machinery, but they are still local to the configured storage path. There is no documented general-purpose remote shard rebalancer or full distributed erasure-coded production repair loop in the current source. Cross-region routing, proxying, activation checkpoints, and drain workflows have their own current limits; see [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/) and [Topology Planning](/operators/topology-planning/).

Some observability names exist in code before a deployment has exported dashboards for them. Treat the catalog as the signals to wire, not as proof that your environment already alerts on them. Some repairs rebuild derived state but intentionally refuse to synthesize source records. Some old or migrated data may require explicit migration handling if it is not CoreStore-backed.

The safe operational posture is conservative: protect the storage path, keep secrets and storage snapshots together, watch disk and lag, use public/admin APIs instead of direct files, repair derived views from source records, and prove recovery with restore drills.

## CoreStore change control

Any runbook that proposes editing files under `STORAGE_PATH` should stop for review. CoreStore records carry visibility, cursor, checksum, and compare-and-swap expectations that ad hoc file edits cannot satisfy. Prefer API-level repair because repair can write new derived state with the same validation and audit path as the rest of Anvil.

If storage corruption is suspected, preserve the volume before running repeated repair attempts. The first evidence often matters more than making a derived view temporarily green.
