use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anvil_core::{
    mvcc_store::LocalMvccStore,
    mvcc_transaction::{
        BundleDurabilityEvidence, BundleIdentity, BundleReplicator, CertificationRequest,
        CertificationResult, DurabilityLevel, DurabilityPolicy, HierarchicalRangeStampScheme,
        LogicalKey, NodeIncarnation, PreparedBundleStore, ReadConsistency, ReplicationEvidence,
        TransactionBundleBuilder, TransactionCertifier, TransactionCoordinator,
    },
    replication::{AckStatus, ReplicationAck},
    replication_client::object_shard_transfer_id,
    shard_placement::{
        DistributedIngest, ShardPlacementPlan, ShardPlacementPolicy, ShardTarget, ShardTargetStream,
    },
    streaming_erasure::{EncodedShard, ErasureProfile},
};
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    StripeEncoding,
    ShardStreaming,
    RemotePersistenceWait,
    RaftCertification,
    LocalMvccApply,
    DeferredRepair,
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
        name: "unrelated_concurrency",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 32,
    },
    Shape {
        name: "same_key_conflict",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 32,
    },
    Shape {
        name: "overlapping_range_conflict",
        logical_keys: 10,
        tables: 1,
        payload_bytes: 1_280,
        concurrency: 32,
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
        name: "group_commit",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 64,
    },
    Shape {
        name: "proposal_batching",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 128,
    },
    Shape {
        name: "rocksdb_wal_group_commit",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 128,
    },
    Shape {
        name: "replication_reconnect_resume",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 8 * 1024 * 1024,
        concurrency: 1,
    },
    Shape {
        name: "mvcc_read_retained_history",
        logical_keys: 1,
        tables: 1,
        payload_bytes: 128,
        concurrency: 1,
    },
    Shape {
        name: "mvcc_garbage_collection",
        logical_keys: 10_000,
        tables: 4,
        payload_bytes: 1_280_000,
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
            Phase::StripeEncoding,
            Phase::ShardStreaming,
            Phase::RemotePersistenceWait,
            Phase::RaftCertification,
            Phase::LocalMvccApply,
            Phase::DeferredRepair,
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
    timings.measure(Phase::StripeEncoding, || {
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
    let bundle = timings.measure(Phase::StripeEncoding, || {
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
        let shard_started = Instant::now();
        run_erasure(shape.payload_bytes.max(4 * 1024))
            .await
            .expect("run benchmark distributed ingest");
        *timings.0.entry(Phase::ShardStreaming).or_default() += shard_started.elapsed();
    }
    let replication_time = Arc::new(Mutex::new(Duration::ZERO));
    let coordinator = TransactionCoordinator::new(
        MemoryPrepared,
        MemoryReplicator(replication_time.clone()),
        MemoryCertifier::default(),
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
    timings.measure(Phase::ShardStreaming, || {
        for write in &bundle.writes {
            store
                .read_at(write.key(), commit_version)
                .expect("read benchmark MVCC row");
        }
    });
    timings.measure(Phase::DeferredRepair, || {
        store
            .garbage_collect(commit_version)
            .expect("collect benchmark MVCC history");
    });
    timings.0.insert(Phase::EndToEnd, end_to_end.elapsed());
}

struct MemoryPrepared;

#[async_trait]
impl PreparedBundleStore for MemoryPrepared {
    async fn persist(&self, _: &BundleIdentity, _: &[u8]) -> Result<BundleDurabilityEvidence> {
        Ok(bundle_holder("node-1", "zone-1"))
    }
}

struct MemoryReplicator(Arc<Mutex<Duration>>);

#[async_trait]
impl BundleReplicator for MemoryReplicator {
    async fn replicate(
        &self,
        _: &BundleIdentity,
        _: &[u8],
        _: &[anvil_core::mvcc_transaction::ObjectShardManifestReference],
        _: DurabilityLevel,
    ) -> Result<ReplicationEvidence> {
        let started = Instant::now();
        let evidence = ReplicationEvidence {
            bundle_holders: vec![
                bundle_holder("node-2", "zone-2"),
                bundle_holder("node-3", "zone-3"),
            ],
            objects: Vec::new(),
        };
        *self.0.lock().unwrap() += started.elapsed();
        Ok(evidence)
    }
}

#[derive(Default)]
struct MemoryCertifier(AtomicU64);

#[async_trait]
impl TransactionCertifier for MemoryCertifier {
    async fn observed_commit_version(&self, _: ReadConsistency) -> Result<u64> {
        Ok(self.0.load(Ordering::Relaxed))
    }

    async fn certify(&self, _: CertificationRequest) -> Result<CertificationResult> {
        Ok(CertificationResult::Committed {
            commit_version: self.0.fetch_add(1, Ordering::Relaxed) + 1,
        })
    }
}

fn bundle_holder(node: &str, domain: &str) -> BundleDurabilityEvidence {
    BundleDurabilityEvidence {
        cluster_id: "benchmark-cluster".into(),
        node: NodeIncarnation {
            node_id: node.into(),
            incarnation: 1,
        },
        failure_domain: domain.into(),
        complete: true,
        hash_verified: true,
        fsynced: true,
    }
}

struct CompleteShardTarget;

#[async_trait]
impl ShardTargetStream for CompleteShardTarget {
    async fn send(
        &self,
        _target: &ShardTarget,
        shard: &EncodedShard<'_>,
    ) -> Result<ReplicationAck> {
        Ok(ReplicationAck {
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
        })
    }
}

async fn run_erasure(payload_bytes: usize) -> Result<()> {
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
    DistributedIngest::encode(
        &CompleteShardTarget,
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
    Ok(())
}
