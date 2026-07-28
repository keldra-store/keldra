---
title: Anvil 0.4.0: single-node MVCC foundation
slug: /blog/mvcc-under-raft-0-4/
description: Anvil 0.4.0 ships a deliberately narrow single-node object, PersonalDB, and Git-ingest release on the new MVCC-under-Raft foundation.
release: v0.4.0
release_date: 2026-07-28
artifacts:
  rust_crate: anvil-storage 0.4.0
  docker_image: ghcr.io/worka-ai/anvil:v0.4.0
---

# Anvil 0.4.0: single-node MVCC foundation

Anvil 0.4.0 replaces the previous metadata commit path with one coherent
transaction model: MVCC provides snapshots and conflict detection, while one
OpenRaft group per cluster orders compact certification decisions. Object data
and product rows stay out of the Raft log.

This is an alpha release for controlled, single-node customer deployments. It
qualifies the object API, core PersonalDB workflows, and Git pack ingest on one
durable node. Multi-node operation, derived-index correctness, and Git
query/watch are not release-qualified in 0.4.0.

## What changed

A transaction is now bound to one cluster and one snapshot version. Within that
cluster it may touch multiple keys, tables, and physical partitions. At commit,
the cluster certifier checks its point and range observations against newer
writes and either assigns one commit version to the complete transaction or
aborts it without publishing a partial result.

The cluster boundary matters. An Anvil mesh may contain multiple regions, and a
region may contain multiple clusters. Transactions do not span clusters or
regions, and there is no mesh-wide serial order. Each cluster has its own
consensus group and commit-version sequence.

Raft carries only the information required to agree on a result:

- transaction identity, snapshot, durability level, and bundle hash;
- compact caller binding for safe retry recovery;
- point/range observations and write-conflict keys;
- committed or aborted certification outcome;
- cluster identity, fixed membership, assignments, and durability policy.

Object bodies, metadata rows, index data, authorisation tuples, transaction
bundles, and erasure shards are replicated by their own storage paths rather
than inflating the consensus log.

## Multi-node storage design

The implementation contains the direct-streaming erasure design for future
multi-node releases: the ingesting node forms bounded stripes and streams
shards to assigned nodes instead of first writing complete replicas. That path
is not release-qualified in 0.4.0 and must not be treated as node-loss
protection.

Internal node connections are persistent gRPC streams. Every operation has a
request identity and an explicit acknowledgement, so a socket that merely
appears connected is not counted as durable progress.

On the supported single-node topology, implicit object writes default to
`local`. Explicit `quorum` and `erasure` object transactions fail closed
because there are not enough physical shard targets; Anvil never silently
downgrades an explicit durability request. All committed 0.4.0 customer data
still depends on one node and one volume. Losing that volume can lose committed
data, so backups are mandatory.

## Startup and retry safety

Customer traffic remains closed while a node is recovering. The public plane
opens only after a cluster consensus barrier succeeds, ordered local apply has
reached the confirmed version, and the system authorisation realm is visible.

If the single-node coordinator loses a response after proposing a transaction,
an idempotent retry can recover the compact outcome. The retained outcome is
bound to the original caller without placing bearer tokens or authorisation
data in Raft.

OpenRaft state and local MVCC state use the node's CoreMeta RocksDB database.
Anvil owns the bounded, versioned binary encoding of its durable consensus
values; direct application use of `bincode` has been removed.

## Operational boundaries

Version 0.4.0 intentionally does not claim the following:

- The supported topology is exactly one Anvil node in one cluster. Multi-node
  bootstrap, restart, quorum/erasure durability, coordinator recovery, and
  leader failover are not supported.
- MVCC and physical garbage collection are disabled, so storage consumption
  grows with every write, overwrite, and delete.
- Clean-disk replacement is unsupported. Restart with the same durable volume
  and identity. Product-level crash/restart recovery is not yet
  release-qualified even though component storage reopen tests are present.
- Transactions cannot cross cluster boundaries.
- Index create/update/delete/list/query/diagnostics remain available, including
  path/prefix query, but query completeness and freshness are not
  release-qualified. Index watch RPCs fail closed with `Unimplemented`, and
  `index:watch` cannot be delegated.
- Git pack ingest and object/S3 readback are supported only for a tenant
  administrator. Git query/tree/blob lookup and watch RPCs fail closed with
  `Unimplemented`; Git actions cannot be delegated in 0.4.0.
- Hugging Face key storage remains available, but ingestion start, status, and
  cancellation RPCs fail closed with `Unimplemented`; ingestion actions cannot
  be delegated in 0.4.0.
- An idempotent retry of an already-committed boundary-schema write may return
  the same hash in a different textual representation. The committed schema is
  retained, but exact retry-response equivalence is deferred to 0.4.1.
- Explicit transactions carrying object payloads must request `local`
  durability. Server-created implicit object/Git transactions and the CLI
  `transaction begin` default use `local`. Raw `BeginTransaction` requests that
  send `UNSPECIFIED` still resolve to `quorum`; set `LOCAL` explicitly.
  Explicit `quorum` and `erasure` payload writes require multiple shard targets
  and are unavailable on the supported one-node topology.
- Mixed-version rolling upgrades are not supported.
- This release does not set a performance service-level objective.

Capacity monitoring is therefore part of operating 0.4.0 safely. Keep backups
of every node volume and the exact topology configuration, alert well before a
volume is full, and pin the Docker image by release tag or digest.

The new transaction architecture is a clean alpha-stage replacement. Anvil
0.4.0 does not provide an automatic migration for a 0.3.x data directory. Start
0.4.0 on new storage, import application data through supported APIs where
needed, and do not point a 0.4.0 node at a 0.3.x volume.

## Supported customer subset

The native gRPC API and Rust client remain the preferred integration surfaces,
and the S3-compatible gateway remains available for object-shaped operations.
The 0.4.0 release gate covers authenticated object mutation/readback,
transaction conflicts and retry, core PersonalDB create/submit/catch-up/watch,
Git pack ingest and object/S3 readback, Zanzibar enforcement, and the separate
public and admin planes on one node.

Index-derived data is not authoritative in this release. Applications must read
objects through the authorised object API or S3 gateway and must not use index
results for completeness, authorisation, or workflow decisions. Git consumers
must read stored pack objects rather than the unavailable Git query/watch RPCs.

Protocol messages remain available where they did not compromise the new
transaction model, but the fail-closed RPCs above are intentional capability
cuts. Because Anvil is still alpha, applications should pin the 0.4 client and
image together and treat compiler, protocol, or `Unimplemented` errors as
required upgrade work rather than depending on accidental compatibility.

## Release validation

The release pipeline builds and smoke-checks both `linux/amd64` and
`linux/arm64` images, publishes the two-platform GHCR manifest, verifies its
platform list and digest, publishes `anvil-storage 0.4.0` when it is not already
present, and creates the GitHub release from this post.

Before promoting the image to customer traffic, validate:

1. an authenticated native object write, read, range read, overwrite, and
   delete;
2. an S3-compatible write and read if the gateway is used;
3. PersonalDB group creation, submit, catch-up, and watch;
4. Git pack ingest followed by object or S3 readback;
5. a transaction conflict where exactly one incompatible writer commits;
6. a controlled in-place restart over a backed-up copy of the same durable
   volume.

The detailed guarantee and limitation matrix is in
[Release Architecture Status](/architecture/release-status/).

## Rollback

Do not downgrade a 0.4.0 data directory into a 0.3.x server. If deployment
validation fails, stop writes and restore the complete pre-deployment volumes
and matching server version together. Restoring only one RocksDB column family,
one node directory, or one subset of erasure shards is not a supported
rollback.
