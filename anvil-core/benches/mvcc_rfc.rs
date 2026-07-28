use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anvil_core::{
    bundle_replication::{
        AppendOnlyPreparedBundleStore, BundleTarget, BundleTargetStream, ObjectEvidenceRegistry,
        StreamingBundleReplicator,
    },
    mvcc_consensus_adapter::ConsensusTransactionCertifier,
    mvcc_store::LocalMvccStore,
    mvcc_transaction::{
        BundleIdentity, CertificationRequest, CertificationResult, CommitVersion, DurabilityLevel,
        DurabilityPolicy, HierarchicalRangeStampScheme, LogicalKey, NodeIncarnation,
        ReadConsistency, TransactionBundle, TransactionBundleBuilder, TransactionCertifier,
        TransactionCoordinator,
    },
    replication::{AckStatus, AuthenticatedPeer, ReplicationAck},
    replication_client::{
        ReplicationPeer, ReplicationStreamOptions, TonicReplicationStreamManager,
        object_shard_transfer_id,
    },
    services::replication::{ReplicationConnectionAuthorizer, ReplicationServiceImpl},
    shard_placement::{
        DistributedIngest, ShardPlacementPlan, ShardPlacementPolicy, ShardTarget, ShardTargetStream,
    },
    streaming_erasure::{EncodedShard, ErasureProfile},
};
use anvil_mvcc_consensus::{
    ConsensusNode, ConsensusRpc, ConsensusRpcClient, ConsensusRpcError, ConsensusRpcFactory,
    NodeId as RaftNodeId, OpenRaftConsensus, RocksRaftStore,
};
use anyhow::Result;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Status, metadata::MetadataMap};
use uuid::Uuid;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    BundleBuild,
    StripeEncoding,
    ShardStreaming,
    RemotePersistenceWait,
    RaftCertification,
    GroupCommit,
    LocalMvccApply,
    MvccRead,
    DeferredRepair,
    ReplicationReconnect,
    MvccGc,
    EndToEnd,
}

#[derive(Debug, Clone, Copy)]
struct Shape {
    name: &'static str,
    logical_keys: usize,
    tables: usize,
    payload_bytes: usize,
    concurrency: usize,
}

const SHAPES: &[Shape] = &[
    Shape {
        name: "metadata_only",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 0,
        concurrency: 1,
    },
    Shape {
        name: "small_inline_object",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 4 * 1024,
        concurrency: 1,
    },
    Shape {
        name: "large_streaming_erasure",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 64 * 1024 * 1024,
        concurrency: 1,
    },
    Shape {
        name: "one_logical_key",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 1,
    },
    Shape {
        name: "ten_logical_keys",
        logical_keys: 10,
        tables: 1,
        payload_bytes: 1_280,
        concurrency: 1,
    },
    Shape {
        name: "cross_table_partition",
        logical_keys: 10,
        tables: 4,
        payload_bytes: 1_280,
        concurrency: 1,
    },
    Shape {
        name: "local_durability",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 4 * 1024,
        concurrency: 1,
    },
    Shape {
        name: "quorum_durability",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 4 * 1024,
        concurrency: 1,
    },
    Shape {
        name: "erasure_durability",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 4 * 1024,
        concurrency: 1,
    },
    Shape {
        name: "unrelated_concurrency",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 16,
    },
    Shape {
        name: "same_key_conflict",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 16,
    },
    Shape {
        name: "overlapping_range_conflict",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 16,
    },
    Shape {
        name: "group_commit",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 64,
    },
    Shape {
        name: "replication_reconnect_resume",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 1024 * 1024,
        concurrency: 1,
    },
    Shape {
        name: "retained_history_read",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 1,
    },
    Shape {
        name: "mvcc_garbage_collection",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 1,
    },
    Shape {
        name: "deferred_repair",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 4 * 1024 * 1024,
        concurrency: 1,
    },
];

#[derive(Default)]
struct PhaseTimings(BTreeMap<Phase, Duration>);

impl PhaseTimings {
    fn measure<T>(&mut self, phase: Phase, work: impl FnOnce() -> T) -> T {
        let started = Instant::now();
        let output = work();
        *self.0.entry(phase).or_default() += started.elapsed();
        output
    }
}

fn main() {
    // The concrete cluster fixture supplies these phase closures. Keeping the
    // shape and phase contract in one harness prevents end-to-end latency from
    // hiding encoding, persistence, consensus, apply, or repair regressions.
    println!("shape,keys,tables,payload_bytes,concurrency,phase,nanos");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build MVCC benchmark runtime");
    for shape in SHAPES {
        let mut timings = PhaseTimings::default();
        runtime.block_on(run_shape(*shape, &mut timings));
        for phase in [
            Phase::BundleBuild,
            Phase::StripeEncoding,
            Phase::ShardStreaming,
            Phase::RemotePersistenceWait,
            Phase::RaftCertification,
            Phase::GroupCommit,
            Phase::LocalMvccApply,
            Phase::MvccRead,
            Phase::DeferredRepair,
            Phase::ReplicationReconnect,
            Phase::MvccGc,
            Phase::EndToEnd,
        ] {
            println!(
                "{},{},{},{},{},{:?},{}",
                shape.name,
                shape.logical_keys,
                shape.tables,
                shape.payload_bytes,
                shape.concurrency,
                phase,
                timings
                    .0
                    .get(&phase)
                    .copied()
                    .unwrap_or_default()
                    .as_nanos()
            );
        }
    }
}

async fn run_shape(shape: Shape, timings: &mut PhaseTimings) {
    let directory = tempfile::tempdir().expect("create benchmark MVCC directory");
    let store = LocalMvccStore::open(directory.path()).expect("open benchmark MVCC store");
    let end_to_end = Instant::now();
    let logical_payload_bytes = if shape.name == "large_streaming_erasure" {
        128
    } else {
        shape.payload_bytes
    };
    let payload_per_key = logical_payload_bytes / shape.logical_keys.max(1);
    let bundles = timings.measure(Phase::BundleBuild, || {
        (0..shape.concurrency)
            .map(|worker| {
                build_bundle(shape, worker, payload_per_key).expect("build benchmark transaction")
            })
            .collect::<Vec<_>>()
    });
    let durability = match shape.name {
        "local_durability" => DurabilityLevel::Local,
        "erasure_durability" | "large_streaming_erasure" => DurabilityLevel::Erasure,
        _ => DurabilityLevel::Quorum,
    };
    if durability == DurabilityLevel::Erasure {
        let (encoding, streaming) = run_erasure(shape.payload_bytes.max(4 * 1024))
            .await
            .expect("run benchmark distributed ingest");
        *timings.0.entry(Phase::StripeEncoding).or_default() += encoding;
        *timings.0.entry(Phase::ShardStreaming).or_default() += streaming;
    }
    let replication_time = Arc::new(Mutex::new(Duration::ZERO));
    let prepared_directory = tempfile::tempdir().expect("create prepared bundle directory");
    let prepared = AppendOnlyPreparedBundleStore::open(
        prepared_directory.path(),
        "benchmark-cluster",
        NodeIncarnation {
            node_id: "node-1".into(),
            incarnation: 1,
        },
        "zone-1",
    )
    .expect("open durable prepared bundle store");
    let target_root = tempfile::tempdir().expect("create replication target root");
    let targets = vec![
        bundle_target("node-2", "zone-2"),
        bundle_target("node-3", "zone-3"),
    ];
    let replicator = StreamingBundleReplicator::new(
        DurableBundleTargets {
            root: target_root.path().to_path_buf(),
            elapsed: replication_time.clone(),
        },
        targets,
        ObjectEvidenceRegistry::default(),
    )
    .expect("build multi-target bundle replicator");
    let raft_directory = tempfile::tempdir().expect("create OpenRaft directory");
    let consensus = openraft_consensus(raft_directory.path()).await;
    let certification_time = Arc::new(Mutex::new(Duration::ZERO));
    let coordinator = TransactionCoordinator::new(
        prepared,
        replicator,
        TimedCertifier {
            inner: ConsensusTransactionCertifier::new(consensus.clone()),
            elapsed: certification_time.clone(),
        },
        DurabilityPolicy {
            bundle_quorum_holders: 2,
            tolerated_failure_domains: 1,
        },
    )
    .expect("build benchmark coordinator");
    let group_started = Instant::now();
    let results = futures::future::join_all(
        bundles
            .iter()
            .cloned()
            .map(|bundle| coordinator.commit(bundle, durability)),
    )
    .await;
    if shape.name == "group_commit" {
        timings
            .0
            .insert(Phase::GroupCommit, group_started.elapsed());
    }
    timings.0.insert(
        Phase::RaftCertification,
        *certification_time.lock().unwrap(),
    );
    timings.0.insert(
        Phase::RemotePersistenceWait,
        *replication_time.lock().unwrap(),
    );
    let (commit_version, bundle) = results
        .into_iter()
        .enumerate()
        .find_map(
            |(index, result)| match result.expect("coordinate benchmark transaction") {
                CertificationResult::Committed { commit_version } => {
                    Some((commit_version, bundles[index].clone()))
                }
                CertificationResult::Aborted { .. } => None,
            },
        )
        .expect("at least one benchmark transaction must commit");
    timings.measure(Phase::LocalMvccApply, || {
        store
            .apply_certified_bundle(commit_version, &bundle)
            .expect("apply benchmark transaction");
    });
    timings.measure(Phase::MvccRead, || {
        for write in &bundle.writes {
            store
                .read_at(write.key(), commit_version)
                .expect("read benchmark MVCC row");
        }
    });
    match shape.name {
        "replication_reconnect_resume" => {
            let started = Instant::now();
            run_reconnect_resume(shape.payload_bytes)
                .await
                .expect("benchmark replication reconnect");
            *timings.0.entry(Phase::ReplicationReconnect).or_default() += started.elapsed();
        }
        "retained_history_read" => {
            timings.measure(Phase::MvccRead, || {
                run_retained_history_read().expect("benchmark retained history");
            });
        }
        "mvcc_garbage_collection" => {
            timings.measure(Phase::MvccGc, || {
                run_mvcc_gc().expect("benchmark MVCC garbage collection");
            });
        }
        "deferred_repair" => {
            let (encoding, streaming) = run_erasure(shape.payload_bytes)
                .await
                .expect("benchmark deferred repair reconstruction");
            *timings.0.entry(Phase::DeferredRepair).or_default() += encoding + streaming;
        }
        _ => {}
    }
    consensus
        .shutdown()
        .await
        .expect("shutdown benchmark OpenRaft");
    timings.0.insert(Phase::EndToEnd, end_to_end.elapsed());
}

fn build_bundle(shape: Shape, worker: usize, payload_per_key: usize) -> Result<TransactionBundle> {
    let mut builder = TransactionBundleBuilder::new(
        "benchmark-cluster",
        format!("{}-tx-{worker}", shape.name),
        0,
        "benchmark-principal",
        HierarchicalRangeStampScheme::new(),
    );
    if shape.name == "overlapping_range_conflict" {
        builder.observe_range(
            1,
            b"partition-0/a".to_vec(),
            b"partition-0/z".to_vec(),
            None,
        )?;
    }
    for ordinal in 0..shape.logical_keys {
        let key_ordinal = if matches!(
            shape.name,
            "same_key_conflict" | "overlapping_range_conflict"
        ) {
            ordinal
        } else {
            worker * shape.logical_keys + ordinal
        };
        let key = LogicalKey {
            table_id: u16::try_from(ordinal % shape.tables.max(1) + 1)?,
            application_key: format!("partition-{}/key-{key_ordinal}", ordinal % 8).into_bytes(),
        };
        if shape.name == "same_key_conflict" {
            builder.observe_point(key.clone(), None);
        }
        builder.put(
            key,
            vec![u8::try_from((worker + ordinal) % 251)?; payload_per_key],
        );
    }
    let bundle = builder.build()?;
    bundle.canonical_bytes()?;
    Ok(bundle)
}

struct TimedCertifier<C> {
    inner: C,
    elapsed: Arc<Mutex<Duration>>,
}

#[async_trait]
impl<C: TransactionCertifier> TransactionCertifier for TimedCertifier<C> {
    async fn observed_commit_version(&self, consistency: ReadConsistency) -> Result<CommitVersion> {
        self.inner.observed_commit_version(consistency).await
    }

    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResult> {
        let started = Instant::now();
        let result = self.inner.certify(request).await;
        *self.elapsed.lock().unwrap() += started.elapsed();
        result
    }

    fn durability_policy(&self) -> Option<DurabilityPolicy> {
        self.inner.durability_policy()
    }
}

struct DurableBundleTargets {
    root: std::path::PathBuf,
    elapsed: Arc<Mutex<Duration>>,
}

#[async_trait]
impl BundleTargetStream for DurableBundleTargets {
    async fn send_bundle(
        &self,
        target: &BundleTarget,
        identity: &BundleIdentity,
        bytes: &[u8],
    ) -> Result<ReplicationAck> {
        let started = Instant::now();
        let directory = self.root.join(&target.node.node_id);
        std::fs::create_dir_all(&directory)?;
        let path = directory.join(identity.hash.trim_start_matches("sha256:"));
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        std::io::Write::write_all(&mut file, bytes)?;
        file.sync_all()?;
        *self.elapsed.lock().unwrap() += started.elapsed();
        let digest = bundle_digest(bytes);
        Ok(ReplicationAck {
            session_id: Uuid::nil(),
            acknowledged_sequence: 1,
            transfer_id: Uuid::new_v4(),
            persisted_through: bytes.len() as u64,
            completed_hash: Some(digest),
            status: AckStatus::Complete,
        })
    }
}

fn bundle_target(node: &str, domain: &str) -> BundleTarget {
    BundleTarget {
        cluster_id: "benchmark-cluster".into(),
        node: NodeIncarnation {
            node_id: node.into(),
            incarnation: 1,
        },
        failure_domain: domain.into(),
        voter: true,
    }
}

struct NoRemoteFactory;
struct NoRemoteClient;

#[async_trait]
impl ConsensusRpcClient for NoRemoteClient {
    async fn request(&mut self, _: ConsensusRpc) -> Result<Vec<u8>, ConsensusRpcError> {
        Err(ConsensusRpcError::Unreachable(
            "single-node benchmark has no remote peer".into(),
        ))
    }
}

impl ConsensusRpcFactory for NoRemoteFactory {
    fn client(&self, _: RaftNodeId, _: &ConsensusNode) -> Box<dyn ConsensusRpcClient> {
        Box::new(NoRemoteClient)
    }
}

async fn openraft_consensus(path: &std::path::Path) -> OpenRaftConsensus {
    let cluster_hash = domain_hash(b"anvil.mvcc.cluster-id.v1", &[&b"benchmark-cluster"[..]]);
    let consensus = OpenRaftConsensus::new(
        RaftNodeId(1),
        RocksRaftStore::open(path, 0).expect("open benchmark OpenRaft store"),
        cluster_hash,
        "benchmark-cluster",
        Arc::new(NoRemoteFactory),
    )
    .await
    .expect("start benchmark OpenRaft");
    consensus
        .initialize(BTreeMap::from([(
            RaftNodeId(1),
            ConsensusNode {
                address: "in-process".into(),
            },
        )]))
        .await
        .expect("initialize benchmark OpenRaft");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if anvil_mvcc_consensus::Consensus::linearized_read_barrier(&consensus)
            .await
            .is_ok()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "benchmark OpenRaft did not elect a leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for (node_id, raft_node_id, failure_domain) in [
        ("node-1", 1, "zone-1"),
        ("node-2", 2, "zone-2"),
        ("node-3", 3, "zone-3"),
    ] {
        consensus
            .install_node(
                cluster_hash,
                anvil_mvcc_consensus::NodeIncarnation {
                    node_id: consensus_control_node_id(node_id),
                    incarnation: 1,
                },
                RaftNodeId(raft_node_id),
                failure_domain.into(),
            )
            .await
            .expect("install benchmark durability holder");
    }
    consensus
        .set_durability_policy(cluster_hash, 1, 2, 1)
        .await
        .expect("install benchmark durability policy");
    consensus
}

fn domain_hash(domain: &[u8], fields: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    for field in fields {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    hasher.finalize().into()
}

fn consensus_control_node_id(node_id: &str) -> RaftNodeId {
    let digest = domain_hash(b"anvil.node-id.v1", &[node_id.as_bytes()]);
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    RaftNodeId(u64::from_be_bytes(bytes))
}

struct CompleteShardTarget(Arc<Mutex<Duration>>);

#[async_trait]
impl ShardTargetStream for CompleteShardTarget {
    async fn send(
        &self,
        _target: &ShardTarget,
        shard: &EncodedShard<'_>,
    ) -> Result<ReplicationAck> {
        let started = Instant::now();
        let acknowledged = ReplicationAck {
            session_id: Uuid::nil(),
            acknowledged_sequence: shard.stripe_ordinal + 1,
            transfer_id: object_shard_transfer_id(
                shard.object_identity,
                shard.encoding_generation,
                shard.stripe_ordinal,
                shard.shard_ordinal,
                shard.payload_hash,
                shard.payload.len() as u64,
            ),
            persisted_through: shard.payload.len() as u64,
            completed_hash: Some(shard.payload_hash),
            status: AckStatus::Complete,
        };
        *self.0.lock().unwrap() += started.elapsed();
        Ok(acknowledged)
    }
}

async fn run_erasure(payload_bytes: usize) -> Result<(Duration, Duration)> {
    let targets = (0..4)
        .map(|ordinal| ShardTarget {
            cluster_id: "benchmark-cluster".into(),
            node: NodeIncarnation {
                node_id: format!("shard-node-{ordinal}"),
                incarnation: 1,
            },
            failure_domain: format!("zone-{ordinal}"),
        })
        .collect();
    let plan = ShardPlacementPlan {
        targets_by_ordinal: targets,
    };
    let profile = ErasureProfile {
        data_shards: 2,
        parity_shards: 2,
        shard_bytes: 64 * 1024,
    };
    let payload = vec![7_u8; payload_bytes];
    let mut reader = payload.as_slice();
    let streaming = Arc::new(Mutex::new(Duration::ZERO));
    let started = Instant::now();
    DistributedIngest::encode(
        &CompleteShardTarget(streaming.clone()),
        &plan,
        ShardPlacementPolicy {
            tolerated_failure_domains: 1,
        },
        profile,
        DurabilityLevel::Erasure,
        &mut reader,
        "mvcc-rfc-benchmark",
        0,
        1,
        false,
        Uuid::from_u128(1),
        None,
        1,
    )
    .await?;
    let total = started.elapsed();
    let streaming = *streaming.lock().unwrap();
    Ok((total.saturating_sub(streaming), streaming))
}

#[derive(Clone)]
struct BenchmarkReplicationAuthorizer;

#[async_trait]
impl ReplicationConnectionAuthorizer for BenchmarkReplicationAuthorizer {
    async fn authorize(
        &self,
        _metadata: &MetadataMap,
        open: &anvil_core::anvil_api::ReplicationSessionOpen,
    ) -> Result<AuthenticatedPeer, Status> {
        AuthenticatedPeer::new_bound(
            open.node_id.clone(),
            open.node_incarnation,
            "benchmark-client",
        )
        .map_err(|error| Status::permission_denied(error.to_string()))
    }
}

async fn start_replication_server(
    receiver_directory: &std::path::Path,
) -> Result<(
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let service = ReplicationServiceImpl::open(BenchmarkReplicationAuthorizer, receiver_directory)?;
    let task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(
                anvil_core::anvil_api::replication_service_server::ReplicationServiceServer::new(
                    service,
                ),
            )
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    Ok((format!("http://{address}"), task))
}

async fn run_reconnect_resume(payload_bytes: usize) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bytes = vec![11_u8; payload_bytes];
    let identity = bundle_identity(&bytes);
    let target = bundle_target("benchmark-peer", "zone-2");
    let (first_endpoint, first_server) = start_replication_server(directory.path()).await?;
    let manager = TonicReplicationStreamManager::new(
        "benchmark-cluster",
        NodeIncarnation {
            node_id: "benchmark-client".into(),
            incarnation: 1,
        },
        "benchmark-token",
        [ReplicationPeer {
            cluster_id: "benchmark-cluster".into(),
            node: target.node.clone(),
            endpoint: first_endpoint,
        }],
        ReplicationStreamOptions {
            allow_insecure_transport_for_tests: true,
            frame_bytes: 64 * 1024,
            ..ReplicationStreamOptions::default()
        },
    )?;
    let first_ack = manager.send_bundle(&target, &identity, &bytes).await?;
    assert_eq!(first_ack.status, AckStatus::Complete);
    first_server.abort();

    // A replacement service reopens the same durable receiver directory. The
    // real client discards its old channel, authenticates a new gRPC stream,
    // queries the transfer watermark, and resumes the immutable transfer.
    let (second_endpoint, second_server) = start_replication_server(directory.path()).await?;
    manager
        .replace_peer_endpoint("benchmark-cluster", &target.node, second_endpoint)
        .await?;
    let resumed_ack = manager.send_bundle(&target, &identity, &bytes).await?;
    assert_eq!(resumed_ack.status, AckStatus::Complete);
    assert_eq!(resumed_ack.persisted_through, bytes.len() as u64);
    second_server.abort();
    Ok(())
}

fn bundle_identity(bytes: &[u8]) -> BundleIdentity {
    let digest = bundle_digest(bytes);
    BundleIdentity {
        hash: format!("sha256:{}", hex::encode(digest)),
        length: bytes.len() as u64,
    }
}

fn bundle_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"anvil.mvcc.transaction-bundle.v1");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    hash.finalize().into()
}

fn run_retained_history_read() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalMvccStore::open(directory.path())?;
    let key = LogicalKey {
        table_id: 1,
        application_key: b"partition-0/history-key".to_vec(),
    };
    for version in 1..=256_u64 {
        let mut builder = TransactionBundleBuilder::new(
            "benchmark-cluster",
            format!("history-{version}"),
            version.saturating_sub(1),
            "benchmark-principal",
            HierarchicalRangeStampScheme::new(),
        );
        builder.put(key.clone(), vec![version as u8; 128]);
        store.apply_certified_bundle(version, &builder.build()?)?;
    }
    let _ = store.read_at(&key, 128)?;
    Ok(())
}

fn run_mvcc_gc() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalMvccStore::open(directory.path())?;
    let key = LogicalKey {
        table_id: 1,
        application_key: b"partition-0/gc-key".to_vec(),
    };
    for version in 1..=256_u64 {
        let mut builder = TransactionBundleBuilder::new(
            "benchmark-cluster",
            format!("gc-{version}"),
            version.saturating_sub(1),
            "benchmark-principal",
            HierarchicalRangeStampScheme::new(),
        );
        builder.put(key.clone(), version.to_be_bytes().to_vec());
        store.apply_certified_bundle(version, &builder.build()?)?;
    }
    let deleted = store.garbage_collect(192)?;
    assert!(deleted > 0);
    Ok(())
}
