# MVCC-under-Raft acceptance audit

Status as of 2026-07-26 at current HEAD.

This is a static source audit. Cargo compilation and test execution were
deliberately deferred to avoid occupying the shared build directory. “Present,
unvalidated” therefore means that production code and focused test source exist;
it does not mean the behavior has passed on this revision.

## Section 32 criteria

| # | Requirement | Status | Current evidence |
|---|---|---|---|
| 1 | Transactions atomically modify arbitrary keys, tables, partitions and features inside one cluster. | Present, unvalidated | Product mutations form one canonical bundle and [`LocalMvccStore::apply_certified_bundle_at_decision`](../../anvil-core/src/mvcc_store.rs#L247) installs its rows, jobs, outbox state and watermarks in one RocksDB batch. Cross-feature tests exist in [`mvcc_cross_feature_tests.rs`](../../anvil-core/src/mvcc_cross_feature_tests.rs). |
| 2 | Cross-cluster, cross-region and mesh transactions are rejected. | Present, unvalidated | Cluster ownership is checked while staging in [`mvcc_open_transactions.rs`](../../anvil-core/src/mvcc_open_transactions.rs#L483) and again before preparation by the coordinator in [`mvcc_transaction.rs`](../../anvil-core/src/mvcc_transaction.rs#L980). Region and mesh are routing domains, not transaction participants. |
| 3 | Commit ordering is cluster-local. | Present, unvalidated | Each consensus instance is bound to one cluster hash; persisted state rejects a different cluster. Local MVCC uses a cluster scope in [`LocalMvccStore::from_db`](../../anvil-core/src/mvcc_store.rs#L204). |
| 4 | Raft contains only Section 9 certification/control state. | Present, unvalidated | [`CertifyTransaction`](../../crates/anvil-mvcc-consensus/src/types.rs#L91) and the closed [`ConsensusCommand`](../../crates/anvil-mvcc-consensus/src/types.rs#L114) contain compact conflict/durability evidence and permitted controls. |
| 5 | Raft uses the existing RocksDB database. | Present, unvalidated | Bootstrap passes the CoreMeta DB to [`RocksRaftStore::from_db`](../../anvil-core/src/mvcc_bootstrap.rs#L383), then constructs MVCC and transaction state from the same DB at [`mvcc_bootstrap.rs:530`](../../anvil-core/src/mvcc_bootstrap.rs#L530) and [`mvcc_bootstrap.rs:558`](../../anvil-core/src/mvcc_bootstrap.rs#L558). |
| 6 | Object bytes and bundle bodies never enter Raft. | Present, unvalidated | The Raft transaction type contains hashes, lengths and conflict metadata only ([`types.rs:91`](../../crates/anvil-mvcc-consensus/src/types.rs#L91)); physical bundles use the prepared-bundle/replication path. |
| 7 | Persistent gRPC replication uses durable application ACKs. | Present, unvalidated | The receiver syncs bytes before returning a persisted ACK in [`replication.rs`](../../anvil-core/src/replication.rs#L317); the service emits that ACK in [`services/replication.rs`](../../anvil-core/src/services/replication.rs#L466). |
| 8 | Node authorization occurs on connect/reconnect, not per frame. | Present, unvalidated | Authorization is performed during stream establishment in [`services/replication.rs`](../../anvil-core/src/services/replication.rs#L160); subsequent messages use the connection session. |
| 9 | `local`, `quorum` and `erasure` have distinct tested semantics. | Present, unvalidated | Coordinator and placement tests exercise the three thresholds. Local writes also persist an upgrade job in [`object_manager.rs`](../../anvil-core/src/object_manager.rs#L395). There is no public API for explicitly requesting or inspecting a durability upgrade; see remaining gaps. |
| 10 | Distributed ingest erasure-codes bounded stripes incrementally. | Present, unvalidated | [`StreamingErasureEncoder::encode`](../../anvil-core/src/streaming_erasure.rs#L64) is bounded; the object path invokes distributed ingest at [`object_manager.rs:455`](../../anvil-core/src/object_manager.rs#L455). |
| 11 | Shards stream directly to final targets. | Present, unvalidated | [`DistributedIngest::encode`](../../anvil-core/src/shard_placement.rs#L174) sends encoded shards through the final-target stream and requires application ACK evidence. |
| 12 | Distributed writes do not first persist complete remote replicas. | Present, unvalidated | The active distributed path feeds the incoming/local reader into `DistributedIngest`; only shard payloads cross the target stream. |
| 13 | Committed bundles apply atomically. | Present, unvalidated | Atomic batch construction starts at [`mvcc_store.rs:247`](../../anvil-core/src/mvcc_store.rs#L247). Restart and cross-feature tests are present but unrun. |
| 14 | Prepared bundles remain invisible. | Present, unvalidated | Prepared storage is separate from MVCC visibility; local application follows a committed decision in [`mvcc_node_runtime.rs`](../../anvil-core/src/mvcc_node_runtime.rs#L94). |
| 15 | Point and range certification prevent conflicts and phantoms. | Present, unvalidated | Compact commands contain point/range observations, and certification applies them in [`certification.rs`](../../crates/anvil-mvcc-consensus/src/certification.rs). Focused point/range model tests exist. |
| 16 | Recovery fetches a missing committed bundle from a durable holder. | Present, unvalidated | [`mvcc_apply_worker.rs`](../../anvil-core/src/mvcc_apply_worker.rs) stops at gaps, fetches and verifies the immutable bundle, applies it, then advances its watermark. |
| 17 | No discarded protocol remains on the active product-write path. | Present with audit risk, unvalidated | The source allowlist in [`direct_mutation_contract.rs`](../../anvil-core/src/direct_mutation_contract.rs) guards direct RocksDB mutation boundaries. This is a static contract test, not executed proof on current HEAD. |
| 18 | Model, adapter, replication, transaction, fault and performance suites demonstrate every invariant. | **Not yet accepted** | The suites and benchmark matrix exist, but they have not been compiled or run together. Several fault requirements still lack real process/topology execution, and some performance cases use lower-level fixtures rather than the complete public/network pipeline. |

## Fault and replication evidence

The real bidirectional gRPC service now applies deterministic frame actions at
[`services/replication.rs:95`](../../anvil-core/src/services/replication.rs#L95).
Focused TCP tests cover a dropped frame, duplicate delivery, dropped Complete
ACK, progress timeout and persisted-watermark reconnect at
[`replication_client.rs:1011`](../../anvil-core/src/replication_client.rs#L1011);
silent half-open recovery is covered at
[`replication_client.rs:1101`](../../anvil-core/src/replication_client.rs#L1101).
These tests are source-present but unrun.

This does **not** satisfy every Section 28.5 boundary. Most durability failpoints
currently return injected errors in one process. They do not kill and restart a
real process before and after every prepared-bundle, shard, Raft-WAL, MVCC-batch
and Complete-ACK boundary. Leader/follower isolation, simultaneous workers and
post-quorum node loss also need one repeatable multi-process or equivalent
multi-node harness.

## Section 29 benchmark audit

[`mvcc_rfc.rs`](../../anvil-core/benches/mvcc_rfc.rs) now contains distinct
transaction IDs and executable workloads for unrelated concurrency, same-key
conflicts, overlapping ranges, group commit, durability levels, receiver
restart/resume, retained history, MVCC GC and deferred erasure reconstruction.
`RaftCertification` measures only the inner certifier call, while `GroupCommit`
records concurrent wall time. [`run-mvcc-perf-benchmark.sh`](../../scripts/run-mvcc-perf-benchmark.sh)
emits an eight-column `report.csv` and rejects a report missing any required
workload.

Fidelity limitations remain:

- reconnect/resume measures a real `TransferReceiver` close/reopen and persisted
  watermark, not a forced disconnect through the complete gRPC client/server
  benchmark pipeline;
- deferred repair measures real bounded erasure reconstruction/placement work,
  not the complete durable `ShardRepairRunner` claim/retry/publish lifecycle;
- local/quorum/erasure run real coordinator and ingest code, but not a realistic
  multi-host network/disk topology;
- the single-node OpenRaft fixture can measure concurrent proposal pressure but
  cannot establish production multi-node WAL/group-commit throughput;
- no baseline can be accepted until the harness is compiled, executed and its
  CSV checked for non-zero, correctly labelled measurements.

## Remaining acceptance work

1. Compile current HEAD and run the focused model, storage, transaction,
   replication, fault and cross-feature suites.
2. Add process-kill/restart coverage on both sides of every durable boundary.
3. Add multi-node leader/follower isolation, worker-race and post-quorum-loss
   scenarios.
4. Run a full public write/read transaction across a real cluster fixture,
   including restart, missing-shard reconstruction and repair publication.
5. Upgrade the network-sensitive benchmarks to full gRPC and multi-node
   fixtures, then capture an accepted baseline.
6. Decide and implement a public durability-upgrade API if clients must promote
   an already committed local object explicitly. The internal runner is wired
   at [`mvcc_bootstrap.rs:700`](../../anvil-core/src/mvcc_bootstrap.rs#L700), but
   no stable client-facing request/status contract is exposed.

Until these steps pass, criteria 1–17 are implementation claims awaiting
validation and criterion 18 remains unsatisfied.
