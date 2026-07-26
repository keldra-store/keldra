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
        BundleIdentity, CertificationResult, DurabilityLevel, DurabilityPolicy,
        HierarchicalRangeStampScheme, LogicalKey, NodeIncarnation, TransactionBundleBuilder,
        TransactionCoordinator,
    },
    replication::{AckStatus, ReplicationAck},
    replication_client::object_shard_transfer_id,
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
use uuid::Uuid;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    BundleBuild,
    StripeEncoding,
    ShardStreaming,
    RemotePersistenceWait,
    RaftCertification,
    LocalMvccApply,
    MvccRead,
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
            Phase::LocalMvccApply,
            Phase::MvccRead,
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
    let mut builder = TransactionBundleBuilder::new(
        "benchmark-cluster",
        format!("{}-tx", shape.name),
        0,
        "benchmark-principal",
        HierarchicalRangeStampScheme::new(),
    );
    timings.measure(Phase::BundleBuild, || {
        for ordinal in 0..shape.logical_keys {
            builder.put(
                LogicalKey {
                    table_id: u16::try_from(ordinal % shape.tables.max(1) + 1).unwrap(),
                    application_key: format!("partition-{}/key-{ordinal}", ordinal % 8)
                        .into_bytes(),
                },
                vec![u8::try_from(ordinal % 251).unwrap(); payload_per_key],
            );
        }
    });
    let bundle = timings.measure(Phase::BundleBuild, || {
        let bundle = builder.build().expect("build benchmark transaction");
        bundle.canonical_bytes().expect("encode benchmark bundle");
        bundle
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
    let coordinator = TransactionCoordinator::new(
        prepared,
        replicator,
        ConsensusTransactionCertifier::new(consensus.clone()),
        DurabilityPolicy {
            bundle_quorum_holders: 2,
            tolerated_failure_domains: 1,
        },
    )
    .expect("build benchmark coordinator");
    let certification_started = Instant::now();
    let results = futures::future::join_all(
        (0..shape.concurrency).map(|_| coordinator.commit(bundle.clone(), durability)),
    )
    .await;
    *timings.0.entry(Phase::RaftCertification).or_default() += certification_started.elapsed();
    timings.0.insert(
        Phase::RemotePersistenceWait,
        *replication_time.lock().unwrap(),
    );
    let commit_version = match results
        .into_iter()
        .next()
        .expect("benchmark has non-zero concurrency")
        .expect("coordinate benchmark transaction")
    {
        CertificationResult::Committed { commit_version } => commit_version,
        CertificationResult::Aborted { reason } => {
            panic!("benchmark transaction aborted: {reason:?}")
        }
    };
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
    consensus
        .shutdown()
        .await
        .expect("shutdown benchmark OpenRaft");
    timings.0.insert(Phase::EndToEnd, end_to_end.elapsed());
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
        let digest: [u8; 32] = Sha256::digest(bytes).into();
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
            return consensus;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "benchmark OpenRaft did not elect a leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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
        Uuid::from_u128(1),
        None,
        1,
    )
    .await?;
    let total = started.elapsed();
    let streaming = *streaming.lock().unwrap();
    Ok((total.saturating_sub(streaming), streaming))
}
