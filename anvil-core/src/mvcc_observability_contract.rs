//! Static release contract for the observability vocabulary in
//! `docs/rfcs/mvcc_under_raft.md`.
//!
//! These checks intentionally inspect production source rather than a metrics
//! recorder. A recorder-based test only proves that one exercised path emitted
//! a value; this contract also prevents an unexercised operation or metric from
//! being renamed or removed accidentally.

use std::{
    fs,
    path::{Path, PathBuf},
};

const REQUIRED_METRICS: &[&str] = &[
    "anvil_transaction_build_duration_ms",
    "anvil_transaction_replication_duration_ms",
    "anvil_transaction_certification_duration_ms",
    "anvil_transaction_apply_duration_ms",
    "anvil_transaction_total_duration_ms",
    "anvil_transaction_conflicts_total",
    "anvil_transaction_point_observations",
    "anvil_transaction_range_observations",
    "anvil_transaction_written_keys",
    "anvil_replication_stream_connected",
    "anvil_replication_ack_latency_ms",
    "anvil_replication_persist_latency_ms",
    "anvil_replication_unacked_bytes",
    "anvil_replication_reconnect_total",
    "anvil_replication_resume_bytes_total",
    "anvil_mvcc_applied_watermark",
    "anvil_mvcc_apply_lag_versions",
    "anvil_mvcc_versions_total",
    "anvil_mvcc_gc_watermark",
    "anvil_mvcc_gc_bytes_total",
    "anvil_local_durability_violations",
    "anvil_local_durability_violations_total",
    "anvil_consensus_proposal_duration_ms",
    "anvil_consensus_apply_duration_ms",
    "anvil_consensus_commit_index",
    "anvil_consensus_leader_changes_total",
    "anvil_consensus_log_entries",
    "anvil_consensus_snapshot_duration_ms",
    "anvil_ingest_stripe_encode_duration_ms",
    "anvil_ingest_shard_stream_duration_ms",
    "anvil_ingest_shard_ack_count",
    "anvil_repair_queue_depth",
    "anvil_repair_age_ms",
    "anvil_repair_duration_ms",
    "anvil_erasure_shard_bytes",
];

const REQUIRED_TRACE_OPERATIONS: &[&str] = &[
    "request.receive",
    "transaction.snapshot",
    "transaction.bundle_build",
    "ingest.stripe",
    "ingest.erasure_encode",
    "shard.stream",
    "shard.fsync",
    "replication.stream",
    "replication.persist_ack",
    "consensus.certify",
    "transaction.apply",
    "response.send",
    "repair.claim",
    "repair.reconstruct",
    "repair.place",
    "repair.commit",
    "gc.mvcc",
    "gc.shard",
];

#[test]
fn mvcc_under_raft_required_metrics_exist_in_production_source() {
    assert_vocabulary_is_present(REQUIRED_METRICS, "metrics");
}

#[test]
fn mvcc_under_raft_stable_trace_operations_exist_in_production_source() {
    assert_vocabulary_is_present(REQUIRED_TRACE_OPERATIONS, "trace operations");
}

fn assert_vocabulary_is_present(required: &[&str], vocabulary: &str) {
    let source = production_source();
    let missing = required
        .iter()
        .copied()
        .filter(|name| !source.contains(&format!("\"{name}\"")))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "RFC-required {vocabulary} missing from production source: {missing:?}"
    );
}

fn production_source() -> String {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut source = String::new();
    append_rust_source(&manifest_dir.join("src"), &mut source);
    append_rust_source(
        &manifest_dir
            .parent()
            .expect("anvil-core has a workspace parent")
            .join("crates/anvil-mvcc-consensus/src"),
        &mut source,
    );
    source
}

fn append_rust_source(directory: &Path, source: &mut String) {
    for entry in fs::read_dir(directory).expect("read production source directory") {
        let path = entry.expect("read production source entry").path();
        if path.is_dir() {
            append_rust_source(&path, source);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs")
            && !is_test_source(&path)
        {
            source.push_str(&fs::read_to_string(path).expect("read production Rust source"));
        }
    }
}

fn is_test_source(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "tests")
        || path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with("_tests.rs"))
}
