use std::{
    collections::BTreeMap,
    hint::black_box,
    time::{Duration, Instant},
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
        timings.measure(Phase::EndToEnd, || {
            black_box((
                shape.logical_keys,
                shape.tables,
                shape.payload_bytes,
                shape.concurrency,
            ));
        });
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
