---
title: Anvil 0.4.0: cluster-local MVCC under Raft
slug: /blog/mvcc-under-raft-0-4/
description: Anvil 0.4.0 introduces cluster-local MVCC certification, direct streaming erasure placement, and an explicit fixed-topology safety boundary.
release: v0.4.0
release_date: 2026-07-28
artifacts:
  rust_crate: anvil-storage 0.4.0
  docker_image: ghcr.io/worka-ai/anvil:v0.4.0
---

# Anvil 0.4.0: cluster-local MVCC under Raft

Anvil 0.4.0 replaces the previous metadata commit path with one coherent
transaction model: MVCC provides snapshots and conflict detection, while one
OpenRaft group per cluster orders compact certification decisions. Object data
and product rows stay out of the Raft log.

This is an alpha release for controlled customer deployments. It establishes
the correctness boundary needed by products building on Anvil, while making the
remaining operational gaps explicit instead of presenting unfinished recovery
paths as supported.

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

## Direct erasure placement

Non-local object writes no longer require several complete-file replicas
followed by a background erasure-coding pass. The ingesting node forms bounded
stripes as bytes arrive, erasure-codes each stripe, and streams the shards
directly to their assigned nodes. The foreground transaction records the
resulting physical evidence before certification.

Internal node connections are persistent gRPC streams. Every operation has a
request identity and an explicit acknowledgement, so a socket that merely
appears connected is not counted as durable progress.

The public default is `quorum` durability. `erasure` requires the configured
shard and failure-domain threshold before acknowledgement. `local` remains
available for workloads that deliberately accept that losing the sole holder
before an upgrade can lose committed data.

## Startup and retry safety

Customer traffic remains closed while a node is recovering. The public plane
opens only after a cluster consensus barrier succeeds, ordered local apply has
reached the confirmed version, and the system authorisation realm is visible.

If a coordinator loses its response after proposing a transaction, a retry can
recover the compact outcome from the current leader after a linearized barrier.
The retained outcome is bound to the original caller without placing bearer
tokens or authorisation data in Raft.

OpenRaft state and local MVCC state use the node's CoreMeta RocksDB database.
Anvil owns the bounded, versioned binary encoding of its durable consensus
values; direct application use of `bincode` has been removed.

## Operational boundaries

Version 0.4.0 intentionally does not claim the following:

- MVCC and physical garbage collection are disabled, so storage consumption
  grows with every write, overwrite, and delete.
- The voter set is fixed at initial cluster bootstrap. Runtime membership
  changes are unsupported.
- A node cannot be replaced from a blank disk. Restart it with the same durable
  volume and identity.
- Transactions cannot cross cluster boundaries.
- Mixed-version rolling upgrades are not supported.
- This release does not set a performance service-level objective.

Capacity monitoring is therefore part of operating 0.4.0 safely. Keep backups
of every node volume and the exact topology configuration, alert well before a
volume is full, and pin the Docker image by release tag or digest.

The new transaction architecture is a clean alpha-stage replacement. Anvil
0.4.0 does not provide an automatic migration for a 0.3.x data directory. Start
0.4.0 on new storage, import application data through supported APIs where
needed, and do not point a 0.4.0 node at a 0.3.x volume.

## What applications keep

The native gRPC API and Rust client remain the preferred integration surfaces.
The S3-compatible gateway remains available for object-shaped operations.
Object versions, metadata and search indexes, Zanzibar-style relationship
authorisation, watches, task leases, PersonalDB witnessing, and the separate
public and admin planes remain product capabilities.

Public API compatibility has been preserved where it did not compromise the new
transaction model. Because Anvil is still alpha, applications should pin the
0.4 client and image together and treat compiler or protocol errors as required
upgrade work rather than depending on accidental compatibility.

## Release validation

The release pipeline builds and smoke-checks both `linux/amd64` and
`linux/arm64` images, publishes the two-platform GHCR manifest, verifies its
platform list and digest, publishes `anvil-storage 0.4.0` when it is not already
present, and creates the GitHub release from this post.

Before promoting the image to customer traffic, validate:

1. an authenticated native object write, read, range read, overwrite, and
   delete;
2. an S3-compatible write and read if the gateway is used;
3. a transaction conflict where exactly one incompatible writer commits;
4. an authorised index query;
5. an in-place restart over the same durable volume;
6. leader failover in the configured fixed cluster.

The detailed guarantee and limitation matrix is in
[Release Architecture Status](/architecture/release-status/).

## Rollback

Do not downgrade a 0.4.0 data directory into a 0.3.x server. If deployment
validation fails, stop writes and restore the complete pre-deployment volumes
and matching server version together. Restoring only one RocksDB column family,
one node directory, or one subset of erasure shards is not a supported
rollback.
