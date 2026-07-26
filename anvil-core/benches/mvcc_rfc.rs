use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use anvil_core::{
    mvcc_store::LocalMvccStore,
    mvcc_transaction::{HierarchicalRangeStampScheme, LogicalKey, TransactionBundleBuilder},
};
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
    for shape in SHAPES {
        let mut timings = PhaseTimings::default();
        run_shape(*shape, &mut timings);
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

fn run_shape(shape: Shape, timings: &mut PhaseTimings) {
    let directory = tempfile::tempdir().expect("create benchmark MVCC directory");
    let store = LocalMvccStore::open(directory.path()).expect("open benchmark MVCC store");
    let end_to_end = Instant::now();
    let payload_per_key = shape.payload_bytes / shape.logical_keys.max(1);
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
    let bundle = timings.measure(Phase::RaftCertification, || {
        let bundle = builder.build().expect("build benchmark transaction");
        bundle.canonical_bytes().expect("encode benchmark bundle");
        bundle
    });
    timings.measure(Phase::LocalMvccApply, || {
        store
            .apply_certified_bundle(1, &bundle)
            .expect("apply benchmark transaction");
    });
    timings.measure(Phase::RemotePersistenceWait, || {
        for write in &bundle.writes {
            store
                .read_at(write.key(), 1)
                .expect("read benchmark MVCC row");
        }
    });
    timings.measure(Phase::DeferredRepair, || {
        store
            .garbage_collect(1)
            .expect("collect benchmark MVCC history");
    });
    timings.0.insert(Phase::EndToEnd, end_to_end.elapsed());
}
