---
title: Release Architecture Status
description: Public architecture and operational status for the Anvil 0.4.0 MVCC-under-Raft release.
---

# Release Architecture Status

Anvil 0.4.0 is the first release of the cluster-local MVCC-under-Raft
architecture. This page states what the release does, where its guarantees
begin and end, and which operations are deliberately unavailable. It should be
read before putting customer data on this release.

Anvil remains an alpha-stage product. Version 0.4.0 is suitable for a controlled
deployment whose operator can pin the image, preserve every node's durable
volume, monitor capacity, and restore the tested topology after a failure. It
is not yet a hands-off storage service for arbitrary membership changes.

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

The public API is held unready during startup until the cluster consensus
barrier is available, ordered local apply has caught up to the confirmed
version, and the system authorisation realm is visible. Internal bootstrap
traffic and the readiness probe have narrowly scoped exceptions so a node can
recover without serving ordinary customer operations early.

The release uses a single sequencer per cluster. That is an intentional
correctness-first implementation, not a promise that one cluster scales without
bound. Additional clusters are independent transaction domains.

## Physical durability

The configured durability level controls which physical work must complete
before a transaction can be acknowledged:

| Level | Acknowledgement boundary | Node-loss posture |
| --- | --- | --- |
| `local` | The local representation and transaction bundle are durable on the coordinating node. | Loss of that holder before a durability upgrade may lose committed data. |
| `quorum` | Bundle and physical-holder evidence satisfy the cluster's configured quorum policy. | This is the default for public writes. |
| `erasure` | Enough erasure shards are durably acknowledged across the configured placement to satisfy the erasure policy. | Tolerates the failure-domain loss configured by that policy. |

For non-local object writes, the ingesting node forms erasure stripes while it
reads the upload and streams the resulting shards directly to their target
nodes. It does not first replicate the complete object to several nodes and
schedule a second full-file erasure conversion.

Internal cluster connections use persistent gRPC streams with request IDs and
explicit acknowledgements. A write is not counted as durable merely because it
was submitted to a socket.

Operators must configure enough distinct nodes and failure domains for their
chosen quorum and erasure policies. A single-node deployment can exercise the
API, but it cannot turn one physical machine into node-loss durability.

## Storage and restart status

OpenRaft durable state and Anvil's local MVCC state share the node's RocksDB
storage boundary. Persistent consensus values use Anvil-owned, bounded,
versioned binary encodings rather than a generic object serializer. Existing
specialised on-disk formats used by other subsystems remain unchanged.

An in-place process restart is expected to reuse the same durable node volume
and identity. Startup recovery replays compact decisions, fetches referenced
bundles when needed, advances ordered apply, and only then opens the public
plane.

Back up every node's durable storage and deployment configuration before an
upgrade. Do not reuse a lost Raft identity on a blank volume.

## Deliberate 0.4.0 limitations

The following are release boundaries, not hidden background features:

- **MVCC and physical garbage collection are disabled.** Old row versions,
  transaction recovery evidence, bundles, and physical payloads are retained.
  Disk consumption therefore grows with writes, overwrites, and deletes.
- **The voter set is fixed at initial cluster bootstrap.** Runtime voter or
  learner reconfiguration is not a supported operator action in 0.4.0.
- **Clean-disk node replacement is unsupported.** Restart a node with its
  original durable volume. Rebuilding a lost node into an existing cluster is
  deferred until checkpoint installation and replacement fencing are complete.
- **Transactions are cluster-local.** There is no cross-cluster two-phase
  commit, saga layer, or mesh-wide serial order.
- **`local` durability is explicitly lossy under holder failure.** Use the
  default `quorum` level for customer writes unless that risk is acceptable.
- **Mixed-version rolling upgrades are not a supported guarantee yet.** Upgrade
  a controlled deployment using backups, release-pinned artifacts, and the
  release's documented validation sequence.
- **No performance target is claimed by this release.** The immediate gate is
  end-to-end correctness and recovery for the supported fixed topology.

Because garbage collection is off, capacity alerts are a correctness control
for this release. Leave enough headroom for MVCC history, Raft state, bundles,
and erasure shards for the full planned deployment window.

## Public surfaces retained

The native gRPC API, Rust client, S3-compatible object gateway, object
versioning, metadata and search indexes, Zanzibar-style relationship
authorisation, watches, task leases, PersonalDB witnessing, and the separate
public/admin planes remain the customer-facing product surfaces. Internally,
their authoritative mutations now pass through the cluster-local transaction
path where the implementation has been converted.

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

Before promotion, run a real write/read/delete cycle, a conflicting-transaction
case, an index query with authorisation, a process restart over the same volume,
and a cluster failover within the fixed topology. A healthy process is not by
itself proof that ordered apply or physical durability is healthy.
