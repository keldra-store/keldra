# MVCC under Raft implementation audit

Audit target: [`mvcc_under_raft.md`](mvcc_under_raft.md).
Status: static source review of current HEAD on 2026-07-26; no Cargo command was
run for this refresh.

Status meanings:

- **Implemented, unvalidated** — the production path and focused test source
  exist, but current HEAD has not been compiled or executed.
- **Partial** — a core path exists but a normative boundary, public contract or
  required realistic test is missing.
- **Missing** — no implementation evidence was found.

## Current implementation

| Area | Status | Evidence and qualification |
|---|---|---|
| One cluster-local OpenRaft certification/control group | Implemented, unvalidated | Bootstrap creates the cluster-bound store at [`mvcc_bootstrap.rs:383`](../../anvil-core/src/mvcc_bootstrap.rs#L383). `ConsensusCommand` is closed at [`types.rs:114`](../../crates/anvil-mvcc-consensus/src/types.rs#L114). |
| Shared CoreMeta RocksDB | Implemented, unvalidated | Raft, MVCC and open-transaction state receive the same DB ([`mvcc_bootstrap.rs:383`](../../anvil-core/src/mvcc_bootstrap.rs#L383), [`mvcc_bootstrap.rs:530`](../../anvil-core/src/mvcc_bootstrap.rs#L530), [`mvcc_bootstrap.rs:558`](../../anvil-core/src/mvcc_bootstrap.rs#L558)). |
| Compact consensus state; no bodies in Raft | Implemented, unvalidated | [`CertifyTransaction`](../../crates/anvil-mvcc-consensus/src/types.rs#L91) contains conflict hashes, bundle identity/length and durability holders, not product bytes. |
| Scope-free cluster-local transactions | Implemented, unvalidated | The registry commits canonical bundles at [`mvcc_open_transactions.rs:483`](../../anvil-core/src/mvcc_open_transactions.rs#L483); coordinator ordering begins at [`mvcc_transaction.rs:980`](../../anvil-core/src/mvcc_transaction.rs#L980). |
| Atomic MVCC application and historical reads | Implemented, unvalidated | Atomic apply is at [`mvcc_store.rs:247`](../../anvil-core/src/mvcc_store.rs#L247); GC is at [`mvcc_store.rs:1280`](../../anvil-core/src/mvcc_store.rs#L1280). |
| Persistent authenticated gRPC replication | Implemented, unvalidated | Client and service maintain connection sessions; durable ACK emission is at [`services/replication.rs:466`](../../anvil-core/src/services/replication.rs#L466). |
| ACK-progress timeout, retransmission and resume | Implemented, unvalidated | Operational frame faults are wired at [`services/replication.rs:95`](../../anvil-core/src/services/replication.rs#L95); real TCP recovery tests start at [`replication_client.rs:1011`](../../anvil-core/src/replication_client.rs#L1011). |
| Streaming erasure directly to final nodes | Implemented, unvalidated | Bounded encoding is at [`streaming_erasure.rs:64`](../../anvil-core/src/streaming_erasure.rs#L64); placement/ACK policy is at [`shard_placement.rs:174`](../../anvil-core/src/shard_placement.rs#L174). |
| Durable repair/rebalance state machine | Implemented, unvalidated | Jobs, claims and retry transitions exist in [`mvcc_shard_repair.rs`](../../anvil-core/src/mvcc_shard_repair.rs) and `LocalMvccStore`. |
| Local-durability automatic upgrade | Implemented internally, unvalidated | Local writes create an upgrade job at [`object_manager.rs:395`](../../anvil-core/src/object_manager.rs#L395); the worker is wired at [`mvcc_bootstrap.rs:700`](../../anvil-core/src/mvcc_bootstrap.rs#L700). |
| Public explicit durability upgrade/status API | Implemented, unvalidated | `ObjectService.PromoteObjectDurability` and `ObjectService.GetObjectDurabilityPromotion` are part of the public proto ([`anvil.proto:1415`](../../anvil-core/proto/anvil.proto#L1415), request/response messages at [`anvil.proto:1638`](../../anvil-core/proto/anvil.proto#L1638)). The RPC validates object write/read authorization, accepts an optional object version, returns the stable promotion ID and durable state, and is exercised from a real three-node fixture ([`services/object/rpc.rs:514`](../../anvil-core/src/services/object/rpc.rs#L514), [`mvcc_cluster_fixture.rs:247`](../../anvil/tests/mvcc_cluster_fixture.rs#L247)). |
| GC safety and observability | Partial | MVCC GC, pins and metrics exist, but current HEAD is unrun and realistic lagging-node/process-restart validation is incomplete. |
| Stable trace operation vocabulary | Partial | Core transaction, replication, consensus, ingest and repair spans exist. Public `request.receive`/`response.send` coverage is not consistently standardised. |
| Section 28 fault suite | Partial | Deterministic in-process faults and real-stream frame/ACK faults exist. Real process kills at every durability boundary and complete multi-node topology faults do not. |
| Section 29 benchmarks | Partial | The required workload/report matrix exists in [`mvcc_rfc.rs`](../../anvil-core/benches/mvcc_rfc.rs) and [`run-mvcc-perf-benchmark.sh`](../../scripts/run-mvcc-perf-benchmark.sh), but full gRPC/multi-node fidelity and an executed baseline are absent. |
| End-to-end acceptance | **Missing** | No current-HEAD build plus public API, multi-node, restart, recovery, reconstruction and repair run has been completed. |

## Discarded internals and write authority

The intended active product-write authority is the MVCC transaction/runtime
path. Direct storage is limited to:

- canonical MVCC state in [`mvcc_store.rs`](../../anvil-core/src/mvcc_store.rs);
- durable transaction drafts/results in
  [`mvcc_open_transactions.rs`](../../anvil-core/src/mvcc_open_transactions.rs);
- compact Raft state in
  [`storage.rs`](../../crates/anvil-mvcc-consensus/src/storage.rs);
- immutable prepared bundles, replication transfers and physical shards.

[`direct_mutation_contract.rs`](../../anvil-core/src/direct_mutation_contract.rs)
encodes this allowlist as a repository scan. Because it has not run on current
HEAD, this audit does not upgrade “no discarded protocol remains” from a
source-supported claim to validated acceptance.

## Required work before end-to-end testing can be called meaningful

1. Compile and execute the public durability-promotion API test on current
   HEAD, including authorization failures and an explicit version selection.
2. Compile once near the end, fix integration errors, then run focused suites.
3. Add a repeatable cluster fixture using real gRPC replication and real
   OpenRaft peers.
4. Exercise public transaction/object APIs across nodes, restart participants,
   lose a shard and verify reconstruction plus durable repair publication.
5. Kill processes before and after prepared-bundle sync, shard sync, Raft WAL
   append, MVCC batch write and Complete ACK; verify recovery invariants.
6. Run leader/follower isolation, simultaneous-worker and post-quorum-node-loss
   scenarios.
7. Run the benchmark report, verify every required row and establish a reviewed
   baseline. Replace lower-level reconnect/repair fixtures with full gRPC and
   worker lifecycle measurements where production fidelity is required.

The design is substantially represented in source, but the project is not yet
at RFC acceptance or credible full end-to-end sign-off. The next build should
be treated as integration discovery, not release validation.
