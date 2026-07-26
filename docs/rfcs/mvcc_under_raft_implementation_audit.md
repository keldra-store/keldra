# MVCC Under Raft implementation audit

Audit target: `docs/rfcs/mvcc_under_raft.md`

Status values:

- **Implemented**: production code and focused tests provide direct evidence.
- **Partial**: the core mechanism exists, but a normative part or required test is missing.
- **Missing**: no production implementation was found.
- **Contradicted**: reachable production code retains behavior the RFC forbids.

Validation note: this audit is based on static inspection of current HEAD.
Focused tests and benchmark harnesses added during this implementation run are
identified as **present but unrun** because Cargo execution was explicitly
disabled; that is distinct from missing production implementation.

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
| Transactions may cross arbitrary tables, partitions, tenants and features inside one cluster | Implemented | Bundles, durable drafts and certification are scope-free; canonical product autocommit now groups mutations across tables/features, and focused cross-table/cross-feature tests are present but unrun in this audit. |
| Transactions must reject resources owned by another cluster before preparation | Implemented | Routing remains authoritative rather than deriving ownership from key hashes. Canonical bundles carry an exact `ClusterOwnershipClaim` set for every observed/written logical key, range, manifest, outbox event and materialisation job. Canonicalization rejects missing/forged coverage, and the coordinator invokes an injectable `ClusterOwnershipResolver` for every claim before bundle persistence or replication. Open transaction staging also retains its routing-issued owning-cluster check. |
| Explicit uniqueness/CAS predicates must certify deterministically | Implemented | `ExplicitPredicate` is canonical bundle state, is hashed into compact consensus `PredicateObservation` entries, and certification evaluates absent/version/value-hash predicates deterministically. Focused source tests are present but unrun. |
| Define limits for observations, writes, command bytes, bundle bytes and raw payload bytes | Implemented | `TransactionResourceLimits` provides validated configurable limits with conservative defaults. The coordinator rejects point/range/write counts, canonical bundle bytes and aggregate raw payload bytes before prepared persistence; the adapter independently rejects an oversized encoded certification command before Raft. Focused tests cover every dimension. |

## Durability, replication and recovery

| RFC requirement | Status | Evidence or gap |
|---|---|---|
| Persistent authenticated bidirectional gRPC streams | Implemented | `replication_client.rs`, `replication_service.rs` and the bootstrap services maintain reusable sessions and multiplex transfers. |
| Authenticate/authorize on connect or reconnect, not every frame | Implemented | `NodeConnectionAuthorizer` validates token-derived node identity, cluster and incarnation once when a session opens/reopens; subsequent frames are bound to the authorised session. `c02a187` removes per-frame authorisation. Focused tests are present but unrun. |
| Persisted/Complete ACK only after durable storage/hash verification | Implemented | `replication.rs` syncs partial data before `Persisted`, verifies final length/hash before `Complete`, and rejects session/sequence/hash mismatches. |
| Reconnect must resume from persisted offset and deduplicate | Implemented | Client/server watermark exchange resumes incomplete transfers; tests cover reconnect, persisted offsets and duplicate completion. |
| Silent half-open connection must be detected from ACK progress | Implemented | Persistent streams track outstanding progress, emit heartbeats and fail stalled ACK windows; reconnect/resume follows the durable transfer watermark (`ccf2118`). Deterministic half-open hooks/tests are present but unrun. |
| `local`, `quorum`, and `erasure` must have distinct honest thresholds | Implemented | `DurabilityPolicy` validates distinct bundle/shard thresholds. Public object write paths select local inline storage or bounded `DistributedIngest`; quorum requires a failure-domain-safe reconstructable set and erasure requires every `k+m` Complete ACK. Source tests/E2E are present but unrun. |
| Missing committed bundles must be fetched and verified before watermark advance | Implemented | `MvccApplyWorker` stops at gaps, fetches from peers, verifies canonical bytes/hash/length/cluster, applies, then advances. Restart/gap/foreign-cluster tests cover this. |
| Durable acknowledgements must name node incarnation | Implemented | `NodeIncarnation` is present in durability evidence and authenticated replication peer state; stale-incarnation validation is tested. |

## Object ingest, erasure coding and maintenance

| RFC requirement | Status | Evidence or gap |
|---|---|---|
| Distributed ingest must erasure-code bounded stripes while upload is arriving | Implemented | `StreamingErasureEncoder` retains one bounded stripe and emits before EOF with sink backpressure. Both `ObjectManager` and the native public put RPC invoke `DistributedIngest::encode` directly for distributed durability. Focused source tests are present but unrun. |
| Quorum/erasure shards must stream directly to final targets | Implemented | `ShardPlacementPolicy` deterministically selects distinct node incarnations/failure domains. `DistributedIngest` sends each encoded shard to the ordinal's final target through the `ShardTargetStream` trait; `TonicReplicationStreamManager` implements that trait with `ObjectShard` transfers and waits for matching Complete ACKs. The combined distributed E2E in `lib.rs` uses the real replication manager and final targets. |
| Distributed writes must not create complete remote replicas before encoding | Implemented | Public distributed writes stream encoded stripes directly to final shard targets and construct the committed physical manifest from Complete ACK evidence; no complete remote-object staging precedes encoding. |
| Committed object metadata must carry a canonical shard manifest and support verified reconstruction | Implemented | `PhysicalObjectShardManifest::from_ingest` binds placements, incarnation, failure domain, transfer identity and BLAKE3 shard hashes; its reference is added to the transaction bundle. Range reconstruction verifies the same BLAKE3 identity, fetches only shard transfers and Reed-Solomon reconstructs missing data. A focused hash/reconstruction test and the combined E2E cover missing-shard reconstruction. |
| Missing optional shards must create durable repair jobs | Implemented | Object ingest derives `ShardRepairJob` for missing optional placements and commits it with the originating bundle. `ShardRepairRunner` reconstructs, places replacements, and publishes a versioned placement overlay transaction (`a7bf14e`). Tests are present but unrun. |
| Workers must claim jobs with MVCC CAS and be duplicate-safe | Implemented | Durable materialisation/repair records use lease-owner transitions, expiry recovery and identity-stable overlay publication. Duplicate repair execution resolves the current overlay and has exactly-once effect. Focused tests and deterministic duplicate-repair hooks are present but unrun. |

## Garbage collection, observability and validation

| RFC requirement | Status | Evidence or gap |
|---|---|---|
| GC watermark must protect active snapshots, lagging replicas, history, backup and jobs | Implemented | Consensus candidates take the minimum of durable snapshots, replica apply positions, retention, backup/audit and unfinished-work pins. Followers collect only after applying `AdvanceGcWatermark`. MVCC preserves the value/tombstone visibility anchor; prepared bundles and shards use durable timestamps, reachability, Complete replacement ACKs, overlay-application evidence and grace windows. Tests are present but unrun. |
| Required transaction/replication/MVCC/consensus/shard metrics | Implemented | `perf.rs` defines the Section 26 transaction, replication, MVCC, consensus, ingest and repair measurements. Current GC work adds the previously absent MVCC GC watermark/bytes and shard GC measurements. Instrumentation is statically wired; runtime emission was not executed in this audit. |
| Required stable trace operations | Partial | Transaction, ingest encode/stripe, shard stream/fsync, replication, consensus, apply, repair and MVCC/shard GC operations are wired. Explicit `request.receive` and `response.send` operations remain to be standardised across public transports. |
| Required model/storage/transaction/replication tests | Partial | Strong unit and restart/recovery coverage exists for certification, storage, MVCC and replication. Several enumerated failure, repair, GC and cross-feature cases are absent. |
| Required fault-injection tests | Partial | Deterministic ordinal failpoints are wired at prepared/shard/MVCC/Raft writes, proposal/apply and Complete-ACK boundaries; frame drop/duplicate/reorder/half-open and the full named RFC scenario matrix are defined in `mvcc_fault_injection.rs`. Hooks/tests are present but unrun; several topology scenarios remain declarative rather than full multi-node executions. |
| Required benchmark suite and separated phase timings | Partial | `benches/mvcc_rfc.rs` defines every Section 29 transaction shape, proposal batching/WAL group commit, and separate phase columns. The concrete workloads are still skeletal and the harness is unrun. |

## Contradictions and discarded internals

| RFC requirement | Status | Evidence or gap |
|---|---|---|
| No migration machinery, dual reads/writes or shadow transaction protocol | Implemented | The legacy CoreStore explicit transaction engine was deleted in `744ac07`; metadata fallback reads were removed in `accae34`. Current product writes use MVCC transactions/autocommit without shadow receipts or dual visibility. |
| No discarded internal protocol on the active write path | Implemented | Manifest, append, index and product autocommit paths now stage canonical MVCC mutations. Remaining CoreStore root/publication code supports separate CoreMeta durability internals rather than a reachable shadow application transaction protocol. |
| Materialisation and outbox work are ordinary durable MVCC state | Implemented | Materialisation jobs and Section 11's required `cf_outbox` events are installed in the same durable RocksDB batch as product rows and the applied watermark. Outbox records carry commit version and deterministic identity; durable lease, reclaim and idempotent completion semantics are covered by focused tests. |

## Direct-mutation classification

Static call-chain inspection of current HEAD classifies production persistence as
follows:

| Boundary | Classification | Reachability/evidence |
|---|---|---|
| `mvcc_store.rs` | Canonical MVCC product state | The apply worker/runtime is the sole product-row RocksDB writer. One certified bundle atomically installs versions, heads, jobs, outbox rows and watermarks. Lease and GC transitions remain inside the same MVCC-owned database. |
| `mvcc_open_transactions.rs` | Canonical MVCC transaction state | Durable drafts and retry/idempotency records are transaction-coordinator state, never visible product rows. Public transaction RPCs enter here before certification. |
| `core_store/meta.rs` | Permitted internal CoreMeta state | Direct RocksDB access implements node-local/internal CoreMeta primitives and column-family ownership. Product APIs reach journals that now stage canonical MVCC mutations; they do not call this boundary to publish ordinary product visibility. |
| `anvil-mvcc-consensus/src/storage.rs` | Permitted Raft/control state | Stores OpenRaft logs, votes, membership, compact certification/conflict state and GC control. Bundle bodies and product values are excluded by type. |
| `bundle_replication.rs`, `replication.rs` | Permitted physical immutable bytes | Framed, checksummed prepared bundles and transfer files are invisible until a certified bundle is applied. |
| `shard_store.rs`, distributed ingest targets | Permitted physical shard representation | Stores checksummed provisional/final erasure shards. Canonical manifests/placement overlays determine logical reachability; proof-driven GC handles retirement. |
| Product service, journal, index, watch, authz and object modules | Canonical MVCC staging or permitted reads | No direct production RocksDB mutation primitive remains. Public writes stage into an explicit transaction or retry-stable product autocommit; control-plane-only topology operations remain separately classified consensus/CoreMeta control. |

No current production direct-write call chain was classified as an RFC
violation. `direct_mutation_contract.rs` makes the reviewed RocksDB writer
allowlist executable and fails when a new source file introduces a direct
mutation primitive without classification. Its test is present but unrun in
this audit.

## Highest-priority next work

1. Convert the declarative minority-loss, leader-change, lagging-follower-GC and restart boundaries into executable multi-node fault scenarios using the existing deterministic hooks.
2. Replace the skeletal Section 29 harness bodies with real local and multi-node workloads, including proposal batching and RocksDB WAL group commit.
3. Finish the stable trace-operation list at request/response, stripe and shard-fsync boundaries and validate metric emission/labels in focused tests.
4. Replace the benchmark's deterministic in-process certifier with a real OpenRaft WAL/group-commit fixture if release acceptance requires consensus-storage numbers; the current harness honestly measures coordinator pressure and real local/erasure paths, not OpenRaft batching.
