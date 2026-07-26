# MVCC Under Raft implementation audit

Audit target: `docs/rfcs/mvcc_under_raft.md`

Status values:

- **Implemented**: production code and focused tests provide direct evidence.
- **Partial**: the core mechanism exists, but a normative part or required test is missing.
- **Missing**: no production implementation was found.
- **Contradicted**: reachable production code retains behavior the RFC forbids.

## Consensus and local storage

| RFC requirement | Status | Evidence or gap |
|---|---|---|
| One OpenRaft certification/control group per cluster | Implemented | `mvcc_bootstrap.rs` constructs one cluster-bound `OpenRaftConsensus`; topology validation binds every configured peer to the same cluster. |
| Pin reviewed OpenRaft 0.9.x | Implemented | `crates/anvil-mvcc-consensus/Cargo.toml` pins `openraft = "=0.9.24"`. |
| Product code must not import OpenRaft types | Implemented | OpenRaft imports are contained by `crates/anvil-mvcc-consensus`; `mvcc_consensus_adapter.rs` converts product types through the Anvil-owned `Consensus` trait. |
| Raft state must use the existing CoreMeta RocksDB | Implemented | `MvccSubsystem::bootstrap` passes `core_meta_db` to `RocksRaftStore::from_db`. Required Raft column families are declared in `anvil-mvcc-consensus/src/storage.rs`. |
| Raft may contain only compact certification and cluster-control state | Implemented | `ConsensusCommand` contains certification hashes/evidence and the enumerated compact control changes. `OpenRaftStateMachine::apply` stores `MachineState`, not product bodies. |
| Object bytes, bundle bodies, ordinary rows, jobs and manifests must not enter Raft | Implemented | `mvcc_consensus_adapter::to_consensus_command` reduces a bundle request to hashes, lengths, conflict keys and holder incarnations. Bundle bytes are persisted by `AppendOnlyPreparedBundleStore`. |
| State-machine decision and last-applied update must be atomic | Implemented | `OpenRaftStateMachine::apply` updates certification/control state, decisions, membership and `last_applied_log_id`, then syncs one encoded machine state. Restart/snapshot tests live in the consensus crate. |
| A linearized read must wait for local application through the barrier | Implemented | `MvccNodeRuntime::snapshot` obtains the Raft barrier and waits for `LocalMvccStore::readable_version`; fixed by `00c7a04`. |
| Commit-version gaps from membership, control and abort entries must be safe | Implemented | The apply worker advances `decision_watermark` for decisions without a committed bundle; reads are bounded by `max(applied_version, decision_watermark)`. |

## Transactions, certification and visibility

| RFC requirement | Status | Evidence or gap |
|---|---|---|
| One immutable bundle and one certification decision per mutating transaction | Implemented | `TransactionCoordinator::commit`, `OpenTransactionRegistry::commit`, and `ConsensusTransactionCertifier` form one canonical bundle and submit one command. |
| Transaction IDs must have one retry-stable outcome | Implemented | Certification retains decisions by transaction ID; the open-transaction registry durably stores resolved state and retries reuse it. |
| Point observations must abort after a conflicting write | Implemented | `CertificationState::apply` validates `point_latest_write`; model and adapter tests cover point conflicts. |
| Range observations and writes must prevent phantoms | Implemented | `HierarchicalRangeStampScheme` deterministically maps reads/writes to stamps; certification validates and advances them; model tests cover insert/delete conflicts. |
| Canonical repeated fields must be sorted and unique | Implemented | `TransactionBundle::canonicalize`, adapter conversion, and consensus `validate_canonical` sort/deduplicate or reject malformed fields. |
| Bundle encoding must be deterministic and schema-versioned | Implemented | `TransactionBundle::canonical_bytes` canonicalizes before encoding and rejects an unknown schema. |
| Prepared bundles must remain invisible | Implemented | Prepared storage is separate from `LocalMvccStore`; only a committed decision invokes atomic application. |
| All writes in a bundle must become visible at one commit version | Implemented | `LocalMvccStore::apply_certified_bundle_at_decision` writes all versions, heads, jobs and watermarks in one RocksDB `WriteBatch`. |
| Tombstones must retain and hide older versions by snapshot | Implemented | `WriteOperation::Delete` stores a tombstone; snapshot/tombstone tests cover visibility before and after deletion. |
| Read-only transactions must not create an application log entry | Implemented | `OpenTransactionRegistry::commit` now resolves a read-only bundle at its snapshot without invoking the runtime; focused retry/no-proposal test added in `972391d`. |
| Transactions may cross arbitrary tables, partitions, tenants and features inside one cluster | Partial | Bundle and registry types are scope-free and cross-table tests exist. Several product paths have moved to logical MVCC mutations, but not every reachable write path has been converted. |
| Transactions must reject keys owned by another cluster before preparation | Partial | The transaction is permanently cluster-bound and foreign `cluster_id` RPCs/bundles are rejected. `LogicalKey` has no general ownership resolver, so arbitrary staged keys are not independently resolved to a cluster. |
| Explicit uniqueness/CAS predicates must certify deterministically | Partial | Point observations cover create/update conflicts, but the bundle and consensus command have no distinct `explicit_predicates` representation described by the RFC. |
| Define limits for observations, writes, command bytes, bundle bytes and raw payload bytes | Missing | No comprehensive transaction resource-limit policy is enforced. |

## Durability, replication and recovery

| RFC requirement | Status | Evidence or gap |
|---|---|---|
| Persistent authenticated bidirectional gRPC streams | Implemented | `replication_client.rs`, `replication_service.rs` and the bootstrap services maintain reusable sessions and multiplex transfers. |
| Authenticate/authorize on connect or reconnect, not every frame | Partial | `NodeConnectionAuthorizer` validates the opening identity/cluster/incarnation and frames are session-bound. Deployment-enforced TLS and a concrete Zanzibar graph check are not demonstrated by focused tests. |
| Persisted/Complete ACK only after durable storage/hash verification | Implemented | `replication.rs` syncs partial data before `Persisted`, verifies final length/hash before `Complete`, and rejects session/sequence/hash mismatches. |
| Reconnect must resume from persisted offset and deduplicate | Implemented | Client/server watermark exchange resumes incomplete transfers; tests cover reconnect, persisted offsets and duplicate completion. |
| Silent half-open connection must be detected from ACK progress | Partial | Transfer calls have deadlines and progress validation, but no clear continuous heartbeat/outstanding-window implementation matching Section 17.4 was found. |
| `local`, `quorum`, and `erasure` must have distinct honest thresholds | Partial | `DurabilityPolicy` validates bundle-holder and shard evidence with distinct levels. End-to-end failure-domain/reconstructability coverage remains incomplete. |
| Missing committed bundles must be fetched and verified before watermark advance | Implemented | `MvccApplyWorker` stops at gaps, fetches from peers, verifies canonical bytes/hash/length/cluster, applies, then advances. Restart/gap/foreign-cluster tests cover this. |
| Durable acknowledgements must name node incarnation | Implemented | `NodeIncarnation` is present in durability evidence and authenticated replication peer state; stale-incarnation validation is tested. |

## Object ingest, erasure coding and maintenance

| RFC requirement | Status | Evidence or gap |
|---|---|---|
| Distributed ingest must erasure-code bounded stripes while upload is arriving | Missing | Current canonical object mutation/materialisation path stages object data and a later materialisation job. No foreground bounded-stripe encoder streaming stripe zero before upload completion was found. |
| Quorum/erasure shards must stream directly to final targets | Missing | Bundle replication exists; the required foreground object-shard streaming pipeline and final-target placement protocol are not complete. |
| Distributed writes must not create complete remote replicas before encoding | Partial | The newer canonical path avoids putting payload bytes in Raft, but repository-wide legacy/raw object paths still need deletion and proof that no complete remote replica is used. |
| Missing optional shards must create durable repair jobs | Missing | Object materialisation jobs exist, but the RFC repair/rebalance job lifecycle and placement-update transaction are not implemented end to end. |
| Workers must claim jobs with MVCC CAS and be duplicate-safe | Partial | Materialisation lease/claim/retry state exists and is tested. It is a special materialisation CF transition rather than the full repair worker and MVCC placement flow specified by Section 19. |

## Garbage collection, observability and validation

| RFC requirement | Status | Evidence or gap |
|---|---|---|
| GC watermark must protect active snapshots, lagging replicas, history, backup and jobs | Missing | Consensus has a compact GC safety watermark and below-watermark reads fail, but complete MVCC version, prepared-bundle, shard and conflict-state GC policy is not implemented. |
| Required transaction/replication/MVCC/consensus/shard metrics | Missing | The exact metric set in Section 26 is not present. Existing performance guards do not satisfy the required counters, gauges and histograms. |
| Required stable trace operations | Partial | Some consensus/transaction/replication tracing exists, but the complete stable operation list is not implemented. |
| Required model/storage/transaction/replication tests | Partial | Strong unit and restart/recovery coverage exists for certification, storage, MVCC and replication. Several enumerated failure, repair, GC and cross-feature cases are absent. |
| Required fault-injection tests | Missing | The complete disk-full, process-kill-at-boundary, reordered-frame and minority-loss matrix is not present. |
| Required benchmark suite and separated phase timings | Missing | The Section 29 benchmark and metrics suite is not present. |

## Contradictions and discarded internals

| RFC requirement | Status | Evidence or gap |
|---|---|---|
| No migration machinery, dual reads/writes or shadow transaction protocol | Contradicted | Reachable product code still contains CoreStore explicit-transaction machinery and some MVCC-first/physical-row fallback reads. These must be deleted rather than retained as migration behavior. |
| No discarded internal protocol on the active write path | Contradicted | The cutover removed transaction overlays from topology, manifest, append, mesh routing, boundary, index and several materializers, but remaining `CoreStore` explicit transaction APIs and legacy receipt/publication paths are still reachable in parts of object/metadata handling. |
| Materialisation and outbox work are ordinary durable MVCC state | Partial | Materialisation jobs are applied atomically with a bundle, but `cf_outbox` is absent and event/job consumers are not uniformly modeled as ordinary versioned logical rows. |

## Highest-priority next work

1. Finish deleting every reachable CoreStore explicit-transaction and receipt/publication path; remove MVCC/physical fallback reads rather than treating them as migration support.
2. Implement the foreground bounded-stripe erasure pipeline and final-target shard streams before claiming `quorum` or `erasure` object durability.
3. Add a general logical-key-to-cluster ownership resolver and validate every observation, mutation, manifest, event and job before bundle persistence.
4. Define and enforce transaction/command/bundle/raw-payload resource limits.
5. Implement safe MVCC, bundle, shard and conflict-state GC with active-snapshot and catch-up protection.
6. Add the required observability, fault matrix and benchmark suite.
