# ANVIL-0007: CoreStore Test Strategy

Status: Superseded by `docs/rfcs/mvcc_under_raft.md`
Audience: Anvil implementors, reviewers, release engineers
Scope: correctness testing for CoreStore roots, generations, metadata
replication, erasure-coded byte replication, explicit transactions, and recovery

Historical RFC: `docs/rfcs/anvil_0007_corestore_unified_storage_manifest.md`

This strategy describes the discarded root-anchor, landed-bytes, and CoreMeta
quorum-certificate architecture. It is not an acceptance suite for the active
implementation.

## 0. Decision

Anvil accepts `anvil-corestore-model` plus Stateright as the first correctness
proof layer for ANVIL-0007.

The implementation must add a dedicated Rust model crate named
`anvil-corestore-model`. That crate models the CoreStore protocol as a small,
executable distributed state machine and uses Stateright to explore bounded
interleavings, crashes, message loss, stale owners, quorum edges, transaction
expiry, and recovery.

Jepsen remains part of the strategy, but it is intentionally not the first tool.
Jepsen is a post-deployment black-box validation layer. It should be added after
the public API, container image, deployment automation, and operational topology
are stable enough to create, fault, observe, and destroy real clusters. Jepsen
must validate the external guarantees Anvil actually exposes; it must not be
used to imply serialisable multi-root transactions unless a later RFC adds that
guarantee.

The accepted stack is therefore:

| Layer | Tooling | Purpose | Release role |
|---|---|---|---|
| Executable model | `anvil-corestore-model` + Stateright | Prove protocol invariants under bounded distributed interleavings | Required before ANVIL-0007 is considered implemented |
| Generated histories | `proptest` inside the model crate | Explore larger random histories and record failing seeds | Required in CI |
| Isolation matrix | Model tests and real harness tests | Prove read committed behaviour and document allowed anomalies | Required in CI |
| Real harness | In-process multi-node CoreStore harness | Prove the real RocksDB/byte-plane implementation refines the model | Required in CI |
| Docker smoke | Multi-process container tests | Prove packaging, ports, process restart, and basic deployment behaviour | Required for release |
| Jepsen | Post-deployment black-box tests | Prove deployed clusters preserve advertised guarantees under real faults | Required before production-grade distributed correctness claims |

## 1. Purpose

ANVIL-0007 defines a storage system with two very different correctness
surfaces:

- a logical metadata protocol built from CoreMeta rows, RocksDB local
  materialisation, quorum receipts, commit certificates, and root generation
  publication;
- a byte-plane protocol built from landed bytes, erasure-coded shards, shard
  receipts, manifests, repair obligations, and range reads.

Ordinary unit tests and Docker smoke tests are not sufficient to prove this
works. They can prove common paths, but they do not explore enough message
reordering, node crashes, stale replicas, lost acknowledgements, transaction
rollbacks, expiry races, or partial byte-plane writes.

This document defines the required layered test strategy. The first correctness
layer is an executable Rust model, implemented in a dedicated
`anvil-corestore-model` crate using Stateright. Jepsen is explicitly a later
post-deployment validation layer, not the first proof mechanism.

## 2. Core Terms Under Test

### 2.1 Root

A root is Anvil's commit pointer for one scoped slice of state.

The RFC defines:

```text
RootAnchorKey = realm_id "/" root_kind "/" partition_id
root_kind     = objects | streams | indexes | authz | personaldb | registry |
                mesh | core-control
```

Examples:

```text
realm_a/objects/42
realm_a/authz/0
realm_a/streams/7
```

Each root is independent. Anvil does not have one global root for the whole
cluster.

### 2.2 Generation

A generation is the monotonically increasing committed version of a root.

```text
realm_a/objects/42 generation 0 = genesis
realm_a/objects/42 generation 1 = first committed object mutation
realm_a/objects/42 generation 2 = next committed object mutation
```

A successful commit advances exactly one root from `N` to `N+1`. Normal readers
only observe committed generations.

### 2.3 Explicit Transaction Scope

An explicit transaction is scoped to exactly one root key. A transaction may
stage many writes, but every staged write must resolve to the same root.

If a write carrying `transaction_id` resolves to a different root, Anvil must
reject it with `TransactionScopeMismatch`.

This RFC does not claim multi-root ACID transactions.

### 2.4 Isolation Level

The public transaction isolation level for ANVIL-0007 is read committed.

Required properties:

- dirty reads are forbidden;
- dirty writes are forbidden by root preconditions, fencing, and commit
  conflict handling;
- committed reads observe only committed root generations;
- staged explicit transaction rows are invisible to ordinary readers;
- rollback and expiry never expose staged rows;
- a read pinned to `at_root_generation` observes that one committed root
  generation;
- reads of "latest committed" may observe different generations across repeated
  reads.

Allowed behaviours under read committed:

- non-repeatable reads when a client repeatedly reads latest committed;
- phantoms when a client lists/query latest committed while new generations are
  published;
- read skew across different roots, because ANVIL-0007 has no global multi-root
  snapshot.

Unsupported public claims:

- serialisable multi-operation transactions;
- snapshot isolation for open write transactions;
- atomic visibility across multiple root keys.

## 3. Testing Layers

### 3.1 Layer 1: Executable Protocol Model

The first required correctness artefact is a Rust crate:

```text
crates/anvil-corestore-model
```

The crate models the CoreStore protocol without RocksDB, files, sockets, async
runtimes, or real erasure coding. It models the logical state machine that the
real implementation must refine.

Required tool:

```text
stateright
```

Reason:

Stateright is a Rust-native model checker for distributed protocols. CoreStore's
hardest bugs are protocol bugs: message reordering, quorum gaps, stale fences,
ambiguous commits, root CAS races, and recovery after partial progress. These are
better found by exhaustive or bounded model exploration before a real RocksDB
cluster exists.

The model crate must be able to run as:

```text
cargo test -p anvil-corestore-model
cargo test -p anvil-corestore-model --features exhaustive-small
```

### 3.2 Layer 2: Property-Based History Generation

The model crate must also use property-based history generation for larger but
non-exhaustive workloads.

Recommended tool:

```text
proptest
```

Properties generated here should include random sequences of:

- begin transaction;
- write inside transaction;
- commit;
- rollback;
- expire transaction;
- implicit write;
- latest read;
- pinned generation read;
- replica crash/recover;
- root owner failover;
- shard loss and repair.

The generated tests must record the random seed on failure.

### 3.3 Layer 3: Hermitage-Style Isolation Matrix

Anvil must have an explicit anomaly matrix for read committed behaviour. The
matrix must not accidentally test for serialisability.

Required matrix:

| Anomaly | Expected under Anvil read committed |
|---|---|
| Dirty read | forbidden |
| Dirty write | forbidden |
| Lost update without precondition | API-specific; must be documented per operation |
| Lost update with object-version precondition | forbidden |
| Non-repeatable read of latest committed | allowed |
| Non-repeatable read pinned to one root generation | forbidden |
| Phantom list/query of latest committed | allowed |
| Phantom list/query pinned to one root generation | forbidden |
| Read skew across different roots | allowed |
| Read skew inside one pinned root generation | forbidden |
| Write skew across roots | allowed |
| Write skew inside one root with matching preconditions | forbidden |

Each row must become at least one automated test. The tests should live near the
model crate first, then be mirrored in the real in-process harness.

### 3.4 Layer 4: Real In-Process Multi-Node Harness

After the executable model passes, the same scenarios must run against real
Anvil components in one process.

This harness must use:

- real RocksDB instances in temporary directories;
- real CoreMeta row encoding;
- real root publication code;
- real transaction lifecycle code;
- real landed-byte files;
- fake deterministic network transport;
- fake deterministic clock;
- controllable crash/restart points.

The harness must not use production TCP or Docker as the primary mechanism. It
must be fast enough for local development and CI.

Required command shape:

```text
cargo test -p anvil-corestore-conformance
```

If the crate name differs, the command must be documented in this file before
release.

### 3.5 Layer 5: Docker Cluster Fault Smoke

Docker tests validate packaging, ports, environment variables, real process
startup, and basic multi-process behaviour. They do not replace the model or
in-process harness.

Docker tests must cover:

- three-node metadata profile;
- large payload write;
- inline payload write;
- node restart;
- stale replica catch-up;
- degraded byte read and repair;
- explicit transaction rollback;
- explicit transaction commit;
- transaction scope mismatch.

### 3.6 Layer 6: Jepsen After Deployment

Jepsen is a post-deployment validation layer.

It should be introduced after:

- the public API is stable enough to run black-box workloads;
- deployment automation can reliably create and destroy test clusters;
- the in-process harness has already proven the protocol invariants;
- the Docker fault suite is green.

Jepsen should use an Anvil-backed transactional key/value adapter and Elle-style
history checking where applicable. It must validate the external claims Anvil
actually makes: read committed, single-root linear commit order, and no dirty
reads. It must not be configured to expect serialisability unless a future RFC
adds that guarantee.

## 4. Model Design

### 4.1 Actors

The Stateright model must define these actor roles:

```text
Client
RootOwner
CoreMetaReplica
ShardNode
RepairWorker
```

The model may combine roles into one node when convenient, but the state machine
must preserve their logical responsibilities.

### 4.2 Model State

The model state must include:

```text
roots:
  root_key_hash -> {
    visible_generation,
    root_anchor_hash,
    owner_node,
    owner_epoch,
    owner_fence
  }

coremeta_replicas:
  node_id -> {
    pending_batches,
    committed_rows,
    persisted_commit_certificates,
    highest_generation_servable
  }

transactions:
  transaction_id -> {
    root_key_hash,
    state,
    staged_mutations,
    expiry_tick,
    preconditions
  }

shards:
  node_id -> {
    block_id,
    shard_index,
    shard_hash,
    fsynced
  }

manifests:
  manifest_hash -> {
    block_ids,
    shard_receipts,
    repair_obligations
  }
```

### 4.3 Model Messages

The model must include at least these messages:

```text
ReplicatePendingBatch
CoreMetaPrepareReceipt
PersistCommitCertificate
CoreMetaCertificatePersistReceipt
CompareAndSwapRoot
RootPublished
PutShard
ShardReceipt
BeginTransaction
StageMutation
CommitTransaction
RollbackTransaction
ExpireTransaction
ReadLatestCommitted
ReadAtRootGeneration
CatchUpPartition
RepairShard
```

### 4.4 Faults

The model must explore:

- message drop;
- message duplication;
- message reordering;
- node crash before local fsync;
- node crash after local fsync;
- owner crash after quorum prepare;
- owner crash after building commit certificate but before certificate
  persistence;
- owner crash after certificate persistence but before root CAS;
- owner crash after root CAS but before client acknowledgement;
- third metadata replica missing a batch;
- stale owner fence;
- stale placement epoch;
- shard receipt missing;
- shard receipt forged for the wrong placement epoch;
- transaction expiry racing with commit;
- rollback racing with staged writes.

## 5. Required Invariants

### 5.1 Root Invariants

- Root generations never regress.
- At most one root anchor is visible for `(root_key_hash, generation)`.
- A visible generation must reference a valid CoreMeta commit certificate.
- A visible generation must reference a commit certificate that has quorum
  certificate-persisted receipts.
- A reader must never observe pending rows as committed state.
- A stale root owner fence must never publish a generation.

### 5.2 CoreMeta Replication Invariants

- A CoreMeta commit certificate cannot exist without quorum prepare receipts.
- A root cannot publish with an owner-local-only commit certificate.
- A stale replica cannot serve current reads until it has caught up to the
  requested generation.
- If quorum is not reached, no visible root generation may be created.
- If quorum is reached and root publication succeeds, later recovery must be
  able to prove the commit from durable evidence.

### 5.3 Transaction Invariants

- A staged write is invisible before commit.
- A rolled-back transaction never becomes visible.
- An expired transaction cannot commit.
- A transaction that touches a second root fails with
  `TransactionScopeMismatch`.
- Commit publishes exactly one root generation.
- Lost client acknowledgement after successful root publication returns
  idempotent committed success.

### 5.4 Byte-Plane Invariants

- A manifest cannot become visible without the required shard receipt threshold.
- A shard receipt must match block id, shard index, placement epoch, shard hash,
  and node identity.
- A manifest committed below full shard count must carry repair obligations.
- Degraded reads require at least read quorum.
- Repair must not replace a valid shard with bytes whose hash or epoch differs
  from the manifest.

### 5.5 Isolation Invariants

- Dirty reads are impossible.
- Pinned generation reads are repeatable for one root.
- Latest committed reads may move forward but never backward for one root.
- Multi-root read skew is allowed and must be documented in test expectations.

## 6. Required Test Names

The implementation must include tests with these names or exact aliases
documented in the crate README:

```text
model_root_generation_single_winner
model_root_generation_never_regresses
model_owner_only_commit_certificate_never_publishes
model_commit_certificate_persisted_before_root
model_quorum_prepare_required
model_quorum_failure_no_visibility
model_stale_replica_cannot_serve_current_generation
model_stale_replica_catches_up
model_stale_owner_fence_rejected
model_lost_ack_returns_idempotent_success

model_explicit_transaction_staged_rows_invisible
model_explicit_transaction_commit_publishes_one_root
model_explicit_transaction_rollback_hides_rows
model_explicit_transaction_expiry_blocks_commit
model_explicit_transaction_scope_mismatch_rejected

model_read_committed_dirty_read_forbidden
model_read_committed_non_repeatable_latest_allowed
model_read_committed_pinned_generation_repeatable
model_read_committed_single_root_phantom_pinned_forbidden
model_read_committed_multi_root_skew_allowed

model_erasure_manifest_requires_publish_threshold
model_erasure_missing_shards_require_repair_obligation
model_erasure_degraded_read_requires_read_quorum
model_erasure_stale_placement_receipt_rejected
model_erasure_repair_preserves_manifest_hashes
```

## 7. Real Harness Fault Injection Points

The real in-process harness must expose named pause/crash points matching the
model:

```text
after_landed_byte_fsync
after_pending_coremeta_write_batch
after_one_prepare_receipt
after_prepare_quorum_before_certificate
after_certificate_built_before_persist
after_certificate_persist_quorum_before_root
after_root_cas_before_client_ack
after_staged_transaction_write
after_transaction_expiry_tick
after_shard_fsync_before_receipt
after_manifest_built_before_coremeta_commit
```

Each point must support:

- pause;
- crash current actor;
- drop next message;
- duplicate next message;
- resume.

## 8. Release Gating

ANVIL-0007 implementation is not complete until:

1. `anvil-corestore-model` passes the required Stateright model tests;
2. property-based history tests run with recorded seeds in CI;
3. the read committed anomaly matrix passes;
4. the real in-process harness passes the same core scenarios with real RocksDB;
5. Docker cluster smoke tests pass;
6. release notes state the exact public isolation level and explicitly say that
   multi-root serialisable transactions are not supported.

Jepsen is not required before the first local implementation merge, but it is
required before making production-grade public claims about distributed
transactional correctness.

## 9. Out of Scope for This Strategy

This document does not require a FoundationDB-style full deterministic simulator
for the first implementation. That remains a future quality target. The current
requirement is narrower: executable protocol model plus real in-process
fault-injected harness.

This document does not require Loom for CoreStore protocol tests. Loom is useful
for local lock-free or concurrent data-structure tests, but it does not model
distributed messages, crashes, quorums, or root publication.

This document does not require Jepsen before local implementation work starts.
Jepsen belongs after the public API and deployment shape are stable enough for
black-box testing.
