#![recursion_limit = "256"]

use std::{fs, path::Path};

fn repo_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path))
        .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
}

#[test]
fn performance_baseline_manifest_and_generator_are_release_artifacts() {
    let manifest = repo_file("ops/perf/baseline-manifest.json");
    for expected in [
        "anvil.perf.baseline_manifest.v1",
        "anvil-corestore-baseline-v1",
        "anvil-corestore-baseline-v1-seed-42",
        "release-10-node-nvme",
        "minio/minio:RELEASE.2026-06-13T11-33-47Z",
        "minimum_valid_samples_per_metric",
    ] {
        assert!(manifest.contains(expected), "missing {expected}");
    }

    let source = repo_file("anvil-core/src/perf_baseline.rs");
    for expected in [
        "pub struct BaselineManifest",
        "pub fn release_default()",
        "validate_release_contract",
        "manifest_hash",
        "generated_object_key",
        "generated_object_metadata",
        "deterministic_vector",
        "baseline_trace_context",
        "BaselineRunSummary",
        "BaselineScenarioSummary",
    ] {
        assert!(source.contains(expected), "missing {expected}");
    }
}

#[test]
fn corestore_perf_baseline_smoke() {
    let performance_test = repo_file("anvil/tests/performance_tests.rs");
    for expected in [
        "ANVIL_RUN_PERF_TESTS",
        "corestore_put_blob_64k",
        "corestore_get_blob_64k",
        "grpc_query_typed_json_caught_up",
        "BaselineRunSummary::smoke",
        "release-gate-step.json",
    ] {
        assert!(performance_test.contains(expected), "missing {expected}");
    }
}

#[test]
fn corestore_perf_baseline_release() {
    let manifest = repo_file("ops/perf/baseline-manifest.json");
    let baseline = repo_file("anvil-core/src/perf_baseline.rs");
    for expected in [
        "release-10-node-nvme",
        "minimum_valid_samples_per_metric",
        "object_thresholds",
        "non_object_thresholds",
        "regression_threshold_percent",
        "validate_release_contract",
        "BaselineRunSummary",
    ] {
        assert!(
            manifest.contains(expected) || baseline.contains(expected),
            "missing {expected}"
        );
    }
}

#[test]
fn mvcc_under_raft_performance_harness_reports_required_phase_boundaries() {
    let runner = repo_file("anvil-core/benches/mvcc_rfc.rs");
    for expected in [
        "metadata_only",
        "small_inline_object",
        "large_streaming_erasure",
        "one_logical_key",
        "ten_logical_keys",
        "cross_table_partition",
        "local_durability",
        "quorum_durability",
        "erasure_durability",
        "StripeEncoding",
        "ShardStreaming",
        "RemotePersistenceWait",
        "RaftCertification",
        "LocalMvccApply",
        "MvccRead",
    ] {
        assert!(runner.contains(expected), "missing {expected}");
    }

    let release_gates = repo_file("scripts/release-gates.sh");
    assert!(release_gates.contains("perf-quick"));
    assert!(release_gates.contains("perf-release"));
    assert!(release_gates.contains("run-mvcc-perf-benchmark.sh"));
}

#[test]
fn stable_trace_event_schema_and_operation_names_are_code_contract() {
    let perf = repo_file("anvil-core/src/perf.rs");
    for expected in [
        "pub struct TraceEvent",
        "anvil.trace_event.v1",
        "pub const TRACE_OPERATION_NAMES",
        "admission.rocksdb_write_batch",
        "coremeta.persist_commit_certificate",
        "byte_pipeline.erasure_encode",
        "block.shard_fsync",
        "query.authz_prune",
        "query.boundary_prune",
        "erasure.range_read",
        "record_trace_event",
        "anvil_trace_event",
    ] {
        assert!(perf.contains(expected), "missing {expected}");
    }

    let local_io = repo_file("anvil-core/src/core_store/local_io.rs");
    assert!(local_io.contains("record_corestore_trace_event"));
    assert!(local_io.contains("crate::perf::TraceEvent"));
}

#[test]
fn corestore_metrics_required_series_present() {
    let perf = repo_file("anvil-core/src/perf.rs");
    for expected in [
        "anvil_request_duration_ms",
        "anvil_fsync_duration_ms",
        "anvil_rocksdb_write_batch_duration_ms",
        "anvil_materialisation_lag_ms",
        "anvil_pending_rows",
        "anvil_pending_coremeta_bytes",
        "anvil_landed_bytes_backlog",
        "anvil_byte_pipeline_stage_duration_ms",
        "anvil_query_plan_duration_ms",
        "anvil_authz_candidate_prune_ratio",
        "anvil_boundary_prune_ratio",
        "anvil_compaction_duration_ms",
        "anvil_compaction_bytes_rewritten_total",
        "anvil_repair_duration_ms",
        "record_pending_state",
        "record_query_prune_ratio",
        "record_compaction_duration",
        "record_repair_duration",
    ] {
        assert!(perf.contains(expected), "missing {expected}");
    }

    let performance_test = repo_file("anvil/tests/performance_tests.rs");
    for expected in [
        "performance-summary.json",
        "anvil.line",
        "release-gate-step.json",
        "slow-spans.json",
        "BaselineRunSummary::smoke",
        "baseline-manifest.json",
    ] {
        assert!(performance_test.contains(expected), "missing {expected}");
    }
}

#[test]
fn corestore_grafana_dashboards_load_from_provisioning() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let dashboard_dir = root.join("ops/perf/grafana/dashboards");
    let required = [
        (
            "anvil-performance-overview.json",
            "Anvil Performance Overview",
            [
                "anvil_request_duration_ms",
                "anvil_pending_rows",
                "anvil_materialisation_lag_ms",
            ]
            .as_slice(),
        ),
        (
            "anvil-request-to-disk.json",
            "Request To Disk",
            [
                "anvil_trace_event",
                "anvil_rocksdb_write_batch_duration_ms",
                "anvil_fsync_duration_ms",
            ]
            .as_slice(),
        ),
        (
            "anvil-corestore-byte-pipeline.json",
            "CoreStore Byte Pipeline",
            [
                "anvil_byte_pipeline_stage_duration_ms",
                "anvil_dedupe_hit_ratio",
                "anvil_compression_ratio",
            ]
            .as_slice(),
        ),
        (
            "anvil-index-and-query.json",
            "Index And Query",
            [
                "anvil_query_plan_duration_ms",
                "anvil_authz_candidate_prune_ratio",
                "anvil_boundary_prune_ratio",
            ]
            .as_slice(),
        ),
        (
            "anvil-rocksdb-pending-state-and-recovery.json",
            "RocksDB Pending State And Recovery",
            [
                "anvil_pending_coremeta_bytes",
                "anvil_landed_bytes_backlog",
                "anvil_recovery_duration_ms",
            ]
            .as_slice(),
        ),
        (
            "anvil-compaction-and-repair.json",
            "Compaction And Repair",
            [
                "anvil_compaction_duration_ms",
                "anvil_compaction_bytes_rewritten_total",
                "anvil_repair_duration_ms",
            ]
            .as_slice(),
        ),
        (
            "anvil-coremeta-and-root-protocols.json",
            "CoreMeta And Root Protocols",
            [
                "anvil_coremeta_replication_duration_ms",
                "anvil_root_register_cas_duration_ms",
                "anvil_failover_vote_total",
            ]
            .as_slice(),
        ),
    ];

    for (file_name, title, metrics) in required {
        let path = dashboard_dir.join(file_name);
        let raw = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read dashboard {}: {error}", path.display()));
        let parsed: serde_json::Value = serde_json::from_str(&raw)
            .unwrap_or_else(|error| panic!("parse dashboard {}: {error}", path.display()));
        assert_eq!(
            parsed.get("title").and_then(|value| value.as_str()),
            Some(title)
        );
        assert!(
            parsed
                .get("panels")
                .and_then(|value| value.as_array())
                .is_some_and(|panels| panels.len() >= 5),
            "{file_name} must contain stable release panels"
        );
        for metric in metrics {
            assert!(raw.contains(metric), "{file_name} missing metric {metric}");
        }
    }

    let provisioning = repo_file("ops/perf/grafana/provisioning/dashboards/anvil.yml");
    assert!(
        provisioning.contains("ops/perf/grafana/dashboards")
            || provisioning.contains("/var/lib/grafana/dashboards")
    );
}
