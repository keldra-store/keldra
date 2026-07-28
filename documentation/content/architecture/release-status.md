---
title: Release Architecture Status
description: Public architecture and operational status for the Anvil 0.4.0 MVCC-under-Raft release.
---

# Release Architecture Status

Anvil 0.4.0 is the first release of the cluster-local MVCC-under-Raft
architecture, but its customer-qualified deployment profile is deliberately
narrow: one node in one cluster. This page states what the release does, where
its guarantees begin and end, and which operations fail closed. Read it before
putting customer data on this release.

Anvil remains an alpha-stage product. Version 0.4.0 is suitable for a controlled
deployment whose operator can pin the image, preserve and back up the node's
durable volume, monitor capacity, and restore that same volume and identity
after a failure. It is not yet a hands-off distributed storage service.

## Transaction and consensus boundary

The topology is a mesh containing regions, with one or more clusters in each
region. A cluster is the transaction, MVCC-version, conflict, durability-policy,
and consensus boundary.

Each cluster has one OpenRaft group that establishes a total order for compact
transaction certification decisions. Transactions may touch multiple keys,
tables, and physical partitions owned by the same cluster. They cannot span
clusters, regions, or the whole mesh. Commit versions are meaningful only
inside the cluster that assigned them.

Raft stores only the compact state needed to make deterministic decisions:

- transaction identity, snapshot version, requested durability, bundle hash,
  and a fixed-size caller binding used to fence retries;
- point and range observations, write-conflict keys, and the resulting compact
  committed or aborted outcome;
- cluster identity, fixed membership, node incarnations, partition assignments,
  and durability-policy state;
- OpenRaft vote, log, membership, and snapshot metadata.

Object bodies, transaction bundles, product rows, indexes, authorisation data,
tokens, erasure shards, and materialisation jobs are not carried in the Raft
log. They use Anvil's data and bundle replication paths.

## MVCC behaviour

Ordinary transactional reads use one cluster-local snapshot. Certification
rejects a commit when a point or range observed at that snapshot conflicts with
a newer committed write. A successful certification assigns one commit version
to the complete transaction; an aborted transaction publishes none of its
writes.

On the supported single-node topology, the public API is held unready during
startup until the local consensus barrier is available, ordered apply has
caught up to the confirmed version, and the system authorisation realm is
visible.

The release uses a single sequencer per cluster. That is an intentional
correctness-first implementation, not a promise that one cluster scales without
bound. Additional clusters are independent transaction domains.

## Physical durability

The architecture defines three durability levels:

| Level | Acknowledgement boundary | Node-loss posture |
| --- | --- | --- |
| `local` | The local representation and transaction bundle are durable on the coordinating node. | Loss of that holder before a durability upgrade may lose committed data. |
| `quorum` | Bundle and physical-holder evidence satisfy the configured quorum policy. | Object-payload transactions require multiple shard targets and therefore fail closed on the supported one-node topology. |
| `erasure` | Enough erasure shards are durably acknowledged across the configured placement to satisfy the erasure policy. | Multi-node erasure placement is not release-qualified in 0.4.0. |

The implementation contains direct streaming erasure placement, but 0.4.0 does
not qualify that path for customer durability. No durability label can turn one
machine and one volume into node-loss tolerance.

Server-created implicit object/Git transactions and the public CLI default to
`local` in 0.4.0. Raw `BeginTransaction` callers must send `LOCAL`;
`UNSPECIFIED` still resolves to `quorum`. Explicit `quorum` or `erasure`
requests are not silently downgraded: an object transaction using either level
fails when the required shard placement is not available.

Internal cluster connections use persistent gRPC streams with request IDs and
explicit acknowledgements. A write is not counted as durable merely because it
was submitted to a socket.

Back up the complete node volume and configuration. Loss of the sole node or
volume can lose committed data.

## Storage and restart status

OpenRaft durable state and Anvil's local MVCC state share the node's RocksDB
storage boundary. Persistent consensus values use Anvil-owned, bounded,
versioned binary encodings rather than a generic object serializer. Existing
specialised on-disk formats used by other subsystems remain unchanged.

An in-place process restart must reuse the same durable node volume and
identity. The component stores have reopen and atomicity coverage, but
product-level crash/restart recovery for object, PersonalDB, and Git workflows
is not release-qualified in 0.4.0. Use controlled restarts and a tested backup.

Back up every node's durable storage and deployment configuration before an
upgrade. Do not reuse a lost Raft identity on a blank volume.

## Deliberate 0.4.0 limitations

The following are release boundaries, not hidden background features:

- **Only a single-node cluster is supported.** Multi-node bootstrap/restart,
  quorum or erasure node-loss durability, coordinator recovery, and leader
  failover are deferred. A multi-node configuration may remain unready and is
  not a supported 0.4.0 deployment.
- **MVCC and physical garbage collection are disabled.** Old row versions,
  transaction recovery evidence, bundles, and physical payloads are retained.
  Disk consumption therefore grows with writes, overwrites, and deletes.
- **Clean-disk node replacement is unsupported.** Restart a node with its
  original durable volume and identity.
- **Transactions are cluster-local.** There is no cross-cluster two-phase
  commit, saga layer, or mesh-wide serial order.
- **Only local object durability is available.** Explicit `quorum` and
  `erasure` object writes fail closed because one node cannot provide the
  required shard placement. A lost sole volume can lose committed data.
- **Index reads are a limited preview.** Index create/update/delete/list/query
  and diagnostics remain available, including path/prefix query, but query
  completeness and freshness are not release-qualified. Do not use results for
  authorisation or workflow decisions. Index watch RPCs return `Unimplemented`,
  and `index:watch` cannot be delegated. Derived indexes are non-authoritative
  and may be rebuilt.
- **Git is an ingest subset.** A tenant administrator may ingest packs and read
  the stored objects through the object API or S3 gateway. Git object/tree/blob
  query and watch RPCs return `Unimplemented`, and Git actions cannot be
  delegated in this release.
- **Hugging Face ingestion is disabled.** Key storage remains available, but
  ingestion start/status/cancel RPCs return `Unimplemented`, and ingestion
  actions cannot be delegated in this release.
- **Boundary-schema retry responses are not byte-equivalent.** A retry of an
  already committed implicit write can return the same schema hash with a
  different textual prefix. The committed schema remains available; canonical
  retry-response equivalence is deferred.
- **Mixed-version rolling upgrades are not a supported guarantee yet.** Upgrade
  a controlled deployment using backups, release-pinned artifacts, and the
  release's documented validation sequence.
- **No performance target is claimed by this release.** The immediate gate is
  correctness for the supported single-node workflows.

Because garbage collection is off, capacity alerts are a correctness control
for this release. Leave enough headroom for MVCC history, Raft state, bundles,
and erasure shards for the full planned deployment window.

## Supported public subset

The customer-qualified subset is the native object API, the Rust client,
S3-compatible object operations, object versions and metadata, Zanzibar-style
relationship authorisation, core PersonalDB create/submit/catch-up/watch, Git
pack ingest for a tenant administrator, and the separate public/admin planes.
Their authoritative mutations pass through the single-node transaction path.

Applications should use the native API or Rust client for Anvil-specific
features. Existing S3-compatible tooling is appropriate for object-shaped
operations, but S3 remains a gateway rather than the storage model.

## Release evidence

The public 0.4.0 artifacts are:

- Rust client: `anvil-storage 0.4.0`;
- Docker image: `ghcr.io/worka-ai/anvil:v0.4.0`;
- GitHub release and source tag: `v0.4.0`.

The Docker tag must be a multi-platform manifest containing `linux/amd64` and
`linux/arm64`. Treat the tag as available only after the release workflow has
built and smoke-checked both images, published the manifest, verified its
platform list and digest, published the Rust client when necessary, and created
the GitHub release.

Before promotion, run a real object write/read/delete cycle, a PersonalDB
submit/catch-up cycle, Git pack ingest plus object/S3 readback, a
conflicting-transaction case, and a controlled restart over a backed-up copy of
the same volume. Do not use index results as correctness evidence, and do not
use Git query/watch or index watch RPCs as 0.4.0 validation steps.
