use std::fmt::Write as _;
use std::future::Future;
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
struct PerfEvent {
    measurement: String,
    labels: Vec<(String, String)>,
    fields: Vec<(String, PerfField)>,
    timestamp_ns: u128,
}

#[derive(Debug, Clone)]
enum PerfField {
    I64(i64),
    U64(u64),
    U128(u128),
    F64(f64),
    Str(String),
}

#[derive(Clone)]
struct PerfSink {
    tx: mpsc::UnboundedSender<PerfMessage>,
}

static PERF_SINK: OnceLock<Option<PerfSink>> = OnceLock::new();

tokio::task_local! {
    static PERF_CONTEXT: Vec<(String, String)>;
}

enum PerfMessage {
    Event(PerfEvent),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug)]
pub struct PerfGuard {
    measurement: &'static str,
    labels: Vec<(String, String)>,
    started_at: Instant,
}

pub fn enabled() -> bool {
    std::env::var_os("ANVIL_PERF_TRACE").is_some()
        || std::env::var_os("ANVIL_PERF_GREPTIME_URL").is_some()
        || std::env::var_os("ANVIL_GREPTIME_WRITE_URL").is_some()
        || std::env::var_os("ANVIL_PERF_TRACE_FILE").is_some()
}

pub fn guard(measurement: &'static str, labels: &[(&str, &str)]) -> PerfGuard {
    PerfGuard {
        measurement,
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        started_at: Instant::now(),
    }
}

pub fn record_duration(measurement: &str, labels: &[(&str, &str)], duration: Duration) {
    if !enabled() {
        return;
    }
    let fields = vec![
        (
            "duration_nanos".to_string(),
            PerfField::U128(duration.as_nanos()),
        ),
        (
            "duration_ms".to_string(),
            PerfField::F64(duration.as_secs_f64() * 1000.0),
        ),
    ];
    emit(PerfEvent {
        measurement: measurement.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        fields,
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_histogram_duration_ms(
    measurement: &str,
    labels: &[(&str, &str)],
    duration: Duration,
) {
    record_duration(measurement, labels, duration);
}

pub fn record_request_duration(
    plane: &str,
    method: &str,
    path: &str,
    status: &str,
    tenant_present: bool,
    request_id_sampled: bool,
    duration: Duration,
) {
    record_histogram_duration_ms(
        "anvil_request_duration_ms",
        &[
            ("plane", plane),
            ("method", method),
            ("path", path),
            ("status", status),
            ("tenant_present", bool_label(tenant_present)),
            ("request_id_sampled", bool_label(request_id_sampled)),
        ],
        duration,
    );
}

pub fn record_fsync_duration(
    component: &str,
    file_class: &str,
    operation: &str,
    duration: Duration,
) {
    record_histogram_duration_ms(
        "anvil_fsync_duration_ms",
        &[
            ("component", component),
            ("file_class", file_class),
            ("operation", operation),
        ],
        duration,
    );
}

pub fn record_io_duration(
    component: &str,
    operation: &str,
    path: &Path,
    bytes: u64,
    duration: Duration,
) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: "anvil_file_io".to_string(),
        labels: vec![
            ("component".to_string(), component.to_string()),
            ("operation".to_string(), operation.to_string()),
            ("file_path".to_string(), path.to_string_lossy().into_owned()),
        ],
        fields: vec![
            (
                "duration_nanos".to_string(),
                PerfField::U128(duration.as_nanos()),
            ),
            (
                "duration_ms".to_string(),
                PerfField::F64(duration.as_secs_f64() * 1000.0),
            ),
            ("bytes".to_string(), PerfField::U64(bytes)),
        ],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_coremeta_duration(
    operation: &str,
    cf: &str,
    table_id: u16,
    row_count: u64,
    bytes: u64,
    duration: Duration,
) {
    if !enabled() {
        return;
    }
    if matches!(
        operation,
        "put" | "delete" | "write_batch" | "write_encoded_rows"
    ) {
        record_histogram_duration_ms(
            "anvil_rocksdb_write_batch_duration_ms",
            &[
                ("node_id", "local"),
                (
                    "column_family_group",
                    if cf == "multi" { "multi" } else { cf },
                ),
                ("fsync_mode", "wal-sync"),
                ("status", "ok"),
            ],
            duration,
        );
    }
    emit(PerfEvent {
        measurement: "anvil_coremeta_io".to_string(),
        labels: vec![
            ("operation".to_string(), operation.to_string()),
            ("column_family".to_string(), cf.to_string()),
            ("table_id".to_string(), format!("0x{table_id:04x}")),
        ],
        fields: vec![
            (
                "duration_nanos".to_string(),
                PerfField::U128(duration.as_nanos()),
            ),
            (
                "duration_ms".to_string(),
                PerfField::F64(duration.as_secs_f64() * 1000.0),
            ),
            ("row_count".to_string(), PerfField::U64(row_count)),
            ("bytes".to_string(), PerfField::U64(bytes)),
        ],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_counter(measurement: &str, labels: &[(&str, &str)], value: u64) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: measurement.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        fields: vec![("count".to_string(), PerfField::U64(value))],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_local_durability_violation(reason: &str) {
    record_counter(
        "anvil_local_durability_violations_total",
        &[("reason", reason)],
        1,
    );
}

pub fn record_local_durability_violation_status(active: u64) {
    record_gauge(
        "anvil_local_durability_violations",
        &[],
        active.min(i64::MAX as u64) as i64,
    );
}

pub fn record_gauge(measurement: &str, labels: &[(&str, &str)], value: i64) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: measurement.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        fields: vec![("value".to_string(), PerfField::I64(value))],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_float_gauge(measurement: &str, labels: &[(&str, &str)], value: f64) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: measurement.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        fields: vec![("value".to_string(), PerfField::F64(value))],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_text(measurement: &str, labels: &[(&str, &str)], field: &str, value: &str) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: measurement.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        fields: vec![(field.to_string(), PerfField::Str(value.to_string()))],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn write_non_authoritative_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

pub fn record_bytes_counter(measurement: &str, labels: &[(&str, &str)], bytes: u64) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: measurement.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        fields: vec![("bytes".to_string(), PerfField::U64(bytes))],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_materialisation_lag_ms(writer_family: &str, lag_ms: u64) {
    let labels = &[("writer_family", writer_family)];
    record_gauge(
        "anvil_materialisation_lag_ms",
        labels,
        lag_ms.min(i64::MAX as u64) as i64,
    );
    record_histogram_duration_ms(
        "anvil_materialisation_lag_ms",
        labels,
        Duration::from_millis(lag_ms),
    );
}

pub fn record_pending_state(
    node_id: &str,
    writer_family: &str,
    pending_rows: u64,
    pending_coremeta_bytes: u64,
    landed_bytes: u64,
) {
    record_gauge(
        "anvil_pending_rows",
        &[("node_id", node_id), ("writer_family", writer_family)],
        pending_rows.min(i64::MAX as u64) as i64,
    );
    record_gauge(
        "anvil_pending_coremeta_bytes",
        &[("node_id", node_id), ("writer_family", writer_family)],
        pending_coremeta_bytes.min(i64::MAX as u64) as i64,
    );
    record_gauge(
        "anvil_landed_bytes_backlog",
        &[("node_id", node_id), ("writer_family", writer_family)],
        landed_bytes.min(i64::MAX as u64) as i64,
    );
}

pub fn record_byte_pipeline_stage_duration(
    stage: &str,
    writer_family: &str,
    erasure_profile: &str,
    bytes_in: u64,
    bytes_out: u64,
    duration: Duration,
) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: "anvil_byte_pipeline_stage_duration_ms".to_string(),
        labels: vec![
            ("stage".to_string(), stage.to_string()),
            ("writer_family".to_string(), writer_family.to_string()),
            ("erasure_profile".to_string(), erasure_profile.to_string()),
        ],
        fields: vec![
            (
                "duration_nanos".to_string(),
                PerfField::U128(duration.as_nanos()),
            ),
            (
                "duration_ms".to_string(),
                PerfField::F64(duration.as_secs_f64() * 1000.0),
            ),
            ("bytes_in".to_string(), PerfField::U64(bytes_in)),
            ("bytes_out".to_string(), PerfField::U64(bytes_out)),
        ],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_dedupe_hit_ratio(writer_family: &str, erasure_profile: &str, ratio: f64) {
    record_float_gauge(
        "anvil_dedupe_hit_ratio",
        &[
            ("writer_family", writer_family),
            ("erasure_profile", erasure_profile),
        ],
        ratio.clamp(0.0, 1.0),
    );
}

pub fn record_compression_ratio(
    writer_family: &str,
    compression: &str,
    erasure_profile: &str,
    uncompressed_bytes: u64,
    stored_bytes: u64,
) {
    let ratio = if uncompressed_bytes == 0 {
        1.0
    } else {
        stored_bytes as f64 / uncompressed_bytes as f64
    };
    record_float_gauge(
        "anvil_compression_ratio",
        &[
            ("writer_family", writer_family),
            ("compression", compression),
            ("erasure_profile", erasure_profile),
        ],
        ratio,
    );
}

pub fn record_erasure_reconstruction_total(erasure_profile: &str, status: &str) {
    record_counter(
        "anvil_erasure_reconstruction_total",
        &[("erasure_profile", erasure_profile), ("status", status)],
        1,
    );
}

pub fn record_replication_heartbeat(status: &str) {
    record_counter(
        "anvil_replication_heartbeat_total",
        &[("status", status)],
        1,
    );
}

pub fn record_replication_reconnect(reason: &str) {
    record_counter(
        "anvil_replication_reconnect_total",
        &[("reason", reason)],
        1,
    );
}

pub fn record_replication_stream_connected(connected: bool) {
    record_gauge(
        "anvil_replication_stream_connected",
        &[],
        i64::from(connected),
    );
}

pub fn record_replication_ack_latency(status: &str, duration: Duration) {
    record_histogram_duration_ms(
        "anvil_replication_ack_latency_ms",
        &[("status", status)],
        duration,
    );
}

pub fn record_replication_persist_latency(status: &str, duration: Duration) {
    record_histogram_duration_ms(
        "anvil_replication_persist_latency_ms",
        &[("status", status)],
        duration,
    );
}

pub fn record_replication_resume_bytes(bytes: u64) {
    record_counter("anvil_replication_resume_bytes_total", &[], bytes);
}

pub fn record_ingest_stripe_encode(status: &str, duration: Duration) {
    record_histogram_duration_ms(
        "anvil_ingest_stripe_encode_duration_ms",
        &[("status", status)],
        duration,
    );
}

pub fn record_replication_unacked_bytes(kind: &str, bytes: u64) {
    record_gauge(
        "anvil_replication_unacked_bytes",
        &[("kind", kind)],
        bytes.min(i64::MAX as u64) as i64,
    );
}

pub fn record_ingest_shard_stream(
    durability: &str,
    status: &str,
    duration: Duration,
    shard_bytes: u64,
) {
    record_histogram_duration_ms(
        "anvil_ingest_shard_stream_duration_ms",
        &[("durability", durability), ("status", status)],
        duration,
    );
    record_counter(
        "anvil_ingest_shard_ack_count",
        &[("durability", durability), ("status", status)],
        1,
    );
    record_gauge(
        "anvil_erasure_shard_bytes",
        &[("durability", durability)],
        shard_bytes.min(i64::MAX as u64) as i64,
    );
}

pub fn record_repair_age(repair_kind: &str, age: Duration) {
    record_histogram_duration_ms("anvil_repair_age_ms", &[("repair_kind", repair_kind)], age);
}

pub fn record_transaction_duration(phase: &str, status: &str, duration: Duration) {
    let measurement = match phase {
        "build" => "anvil_transaction_build_duration_ms",
        "replication" => "anvil_transaction_replication_duration_ms",
        "certification" => "anvil_transaction_certification_duration_ms",
        "apply" => "anvil_transaction_apply_duration_ms",
        "total" => "anvil_transaction_total_duration_ms",
        _ => return,
    };
    record_histogram_duration_ms(measurement, &[("status", status)], duration);
}

pub fn record_transaction_shape(
    point_observations: u64,
    range_observations: u64,
    written_keys: u64,
) {
    record_gauge(
        "anvil_transaction_point_observations",
        &[],
        point_observations.min(i64::MAX as u64) as i64,
    );
    record_gauge(
        "anvil_transaction_range_observations",
        &[],
        range_observations.min(i64::MAX as u64) as i64,
    );
    record_gauge(
        "anvil_transaction_written_keys",
        &[],
        written_keys.min(i64::MAX as u64) as i64,
    );
}

pub fn record_transaction_conflict(reason: &str) {
    record_counter(
        "anvil_transaction_conflicts_total",
        &[("reason", reason)],
        1,
    );
}

pub fn record_mvcc_state(applied_watermark: u64, observed_commit: u64, versions_created: u64) {
    record_gauge(
        "anvil_mvcc_applied_watermark",
        &[],
        applied_watermark.min(i64::MAX as u64) as i64,
    );
    record_gauge(
        "anvil_mvcc_apply_lag_versions",
        &[],
        observed_commit
            .saturating_sub(applied_watermark)
            .min(i64::MAX as u64) as i64,
    );
    if versions_created > 0 {
        record_counter("anvil_mvcc_versions_total", &[], versions_created);
    }
}

pub fn record_mvcc_gc(watermark: u64, reclaimed_bytes: u64, duration: Duration) {
    record_gauge(
        "anvil_mvcc_gc_watermark",
        &[],
        watermark.min(i64::MAX as u64) as i64,
    );
    if reclaimed_bytes > 0 {
        record_bytes_counter("anvil_mvcc_gc_bytes_total", &[], reclaimed_bytes);
    }
    record_histogram_duration_ms("anvil_mvcc_gc_duration_ms", &[("status", "ok")], duration);
}

pub fn record_gc_coordination(status: &str, duration: Duration) {
    record_histogram_duration_ms(
        "anvil_mvcc_gc_coordination_duration_ms",
        &[("status", status)],
        duration,
    );
}

pub fn record_shard_gc(reclaimed_bytes: u64, duration: Duration) {
    if reclaimed_bytes > 0 {
        record_bytes_counter("anvil_shard_gc_bytes_total", &[], reclaimed_bytes);
    }
    record_histogram_duration_ms("anvil_shard_gc_duration_ms", &[("status", "ok")], duration);
}

pub fn record_consensus_phase(phase: &str, status: &str, duration: Duration) {
    let measurement = match phase {
        "proposal" => "anvil_consensus_proposal_duration_ms",
        "apply" => "anvil_consensus_apply_duration_ms",
        "snapshot" => "anvil_consensus_snapshot_duration_ms",
        _ => return,
    };
    record_histogram_duration_ms(measurement, &[("status", status)], duration);
}

pub fn record_consensus_commit(commit_index: u64) {
    record_gauge(
        "anvil_consensus_commit_index",
        &[],
        commit_index.min(i64::MAX as u64) as i64,
    );
    record_counter("anvil_consensus_log_entries", &[("kind", "proposal")], 1);
}

pub fn record_consensus_leader_change(reason: &str) {
    record_counter(
        "anvil_consensus_leader_changes_total",
        &[("reason", reason)],
        1,
    );
}

pub fn record_query_plan_duration(
    query_kind: &str,
    index_kind: &str,
    status: &str,
    ranges_read: u64,
    duration: Duration,
) {
    if !enabled() {
        return;
    }
    emit(PerfEvent {
        measurement: "anvil_query_plan_duration_ms".to_string(),
        labels: vec![
            ("query_kind".to_string(), query_kind.to_string()),
            ("index_kind".to_string(), index_kind.to_string()),
            ("status".to_string(), status.to_string()),
        ],
        fields: vec![
            (
                "duration_nanos".to_string(),
                PerfField::U128(duration.as_nanos()),
            ),
            (
                "duration_ms".to_string(),
                PerfField::F64(duration.as_secs_f64() * 1000.0),
            ),
            ("ranges_read".to_string(), PerfField::U64(ranges_read)),
        ],
        timestamp_ns: unix_timestamp_nanos(),
    });
}

pub fn record_query_prune_ratio(
    measurement: &str,
    query_kind: &str,
    index_kind: &str,
    before: u64,
    after: u64,
) {
    let ratio = if before == 0 {
        0.0
    } else {
        1.0 - (after.min(before) as f64 / before as f64)
    };
    record_float_gauge(
        measurement,
        &[("query_kind", query_kind), ("index_kind", index_kind)],
        ratio.clamp(0.0, 1.0),
    );
}

pub fn record_authz_candidate_prune_ratio(
    query_kind: &str,
    index_kind: &str,
    before: u64,
    after: u64,
) {
    record_query_prune_ratio(
        "anvil_authz_candidate_prune_ratio",
        query_kind,
        index_kind,
        before,
        after,
    );
}

pub fn record_boundary_prune_ratio(query_kind: &str, index_kind: &str, before: u64, after: u64) {
    record_query_prune_ratio(
        "anvil_boundary_prune_ratio",
        query_kind,
        index_kind,
        before,
        after,
    );
}

pub fn record_query_ranges_read_total(query_kind: &str, index_kind: &str, ranges: u64) {
    record_counter(
        "anvil_query_ranges_read_total",
        &[("query_kind", query_kind), ("index_kind", index_kind)],
        ranges,
    );
}

pub fn record_compaction_duration(
    compaction_kind: &str,
    writer_family: &str,
    status: &str,
    bytes_rewritten: u64,
    duration: Duration,
) {
    record_histogram_duration_ms(
        "anvil_compaction_duration_ms",
        &[
            ("compaction_kind", compaction_kind),
            ("writer_family", writer_family),
            ("status", status),
        ],
        duration,
    );
    record_bytes_counter(
        "anvil_compaction_bytes_rewritten_total",
        &[
            ("compaction_kind", compaction_kind),
            ("writer_family", writer_family),
        ],
        bytes_rewritten,
    );
}

pub fn record_repair_duration(
    repair_kind: &str,
    writer_family: &str,
    status: &str,
    duration: Duration,
) {
    record_histogram_duration_ms(
        "anvil_repair_duration_ms",
        &[
            ("repair_kind", repair_kind),
            ("writer_family", writer_family),
            ("status", status),
        ],
        duration,
    );
}

pub fn record_recovery_duration(recovery_kind: &str, status: &str, duration: Duration) {
    record_histogram_duration_ms(
        "anvil_recovery_duration_ms",
        &[("recovery_kind", recovery_kind), ("status", status)],
        duration,
    );
}

pub fn record_coremeta_replication_duration(
    phase: &str,
    profile: &str,
    quorum_outcome: &str,
    duration: Duration,
) {
    record_histogram_duration_ms(
        "anvil_coremeta_replication_duration_ms",
        &[
            ("phase", phase),
            ("profile", profile),
            ("quorum_outcome", quorum_outcome),
        ],
        duration,
    );
}

pub fn record_coremeta_quorum_total(phase: &str, profile: &str, outcome: &str) {
    record_counter(
        "anvil_coremeta_quorum_total",
        &[("phase", phase), ("profile", profile), ("outcome", outcome)],
        1,
    );
}

pub fn record_root_register_cas_duration(
    operation: &str,
    profile: &str,
    outcome: &str,
    duration: Duration,
) {
    record_histogram_duration_ms(
        "anvil_root_register_cas_duration_ms",
        &[
            ("operation", operation),
            ("profile", profile),
            ("outcome", outcome),
        ],
        duration,
    );
}

pub fn record_root_generation_in_doubt(root_kind: &str, partition_id: u64) {
    let partition = partition_id.to_string();
    record_counter(
        "anvil_root_generation_in_doubt_total",
        &[("root_kind", root_kind), ("partition_id", &partition)],
        1,
    );
}

pub fn record_partition_failover_duration(
    region_id: &str,
    cell_id: &str,
    status: &str,
    duration: Duration,
) {
    record_histogram_duration_ms(
        "anvil_partition_failover_duration_ms",
        &[
            ("region_id", region_id),
            ("cell_id", cell_id),
            ("status", status),
        ],
        duration,
    );
}

pub fn record_failover_vote_total(decision: &str, reason: &str) {
    record_counter(
        "anvil_failover_vote_total",
        &[("decision", decision), ("reason", reason)],
        1,
    );
}

pub fn record_anti_entropy_findings_total(
    finding_kind: &str,
    writer_family: &str,
    severity: &str,
    count: u64,
) {
    record_counter(
        "anvil_anti_entropy_findings_total",
        &[
            ("finding_kind", finding_kind),
            ("writer_family", writer_family),
            ("severity", severity),
        ],
        count,
    );
}

pub fn record_repair_queue_depth(repair_kind: &str, region_id: &str, cell_id: &str, depth: u64) {
    record_gauge(
        "anvil_repair_queue_depth",
        &[
            ("repair_kind", repair_kind),
            ("region_id", region_id),
            ("cell_id", cell_id),
        ],
        depth.min(i64::MAX as u64) as i64,
    );
}

pub fn record_tombstone_debt(writer_family: &str, debt: u64) {
    record_gauge(
        "anvil_tombstone_debt",
        &[("writer_family", writer_family)],
        debt.min(i64::MAX as u64) as i64,
    );
}

pub fn record_protocol_errors_total(operation: &str, code: &str) {
    record_counter(
        "anvil_protocol_errors_total",
        &[("operation", operation), ("code", code)],
        1,
    );
}

#[derive(Debug, Clone)]
pub struct TraceEvent<'a> {
    pub trace_id: &'a str,
    pub span_id: &'a str,
    pub parent_span_id: Option<&'a str>,
    pub request_id: Option<&'a str>,
    pub component: &'a str,
    pub operation: &'a str,
    pub writer_family: Option<&'a str>,
    pub bucket_hash: Option<&'a str>,
    pub boundary_schema_generation: Option<u64>,
    pub duration: Duration,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub fsync_count: u64,
    pub status: &'a str,
}

pub const TRACE_OPERATION_NAMES: &[&str] = &[
    "api.request",
    "admission.validate",
    "admission.boundary_extract",
    "admission.landed_write",
    "admission.landed_fsync",
    "admission.rocksdb_write_batch",
    "admission.rocksdb_fsync",
    "admission.commit_evidence_write_batch",
    "coremeta.replicate_pending",
    "coremeta.persist_commit_certificate",
    "coremeta.quorum_wait",
    "failover.vote_grant",
    "failover.vote_reject",
    "failover.in_doubt",
    "materialiser.plan",
    "writer.build",
    "byte_pipeline.chunk",
    "byte_pipeline.dedupe",
    "byte_pipeline.compress",
    "byte_pipeline.encrypt",
    "byte_pipeline.erasure_encode",
    "placement.plan",
    "block.shard_write",
    "block.shard_fsync",
    "root_register.cas_read",
    "root_register.cas_write",
    "root_register.quorum_wait",
    "root_register.failover_vote",
    "root_register.failover_publish",
    "manifest.publish",
    "query.plan",
    "query.authz_prune",
    "query.boundary_prune",
    "erasure.range_read",
    "erasure.decode",
    "repair.enqueue",
    "repair.reconstruct",
    "anti_entropy.scan",
    "response.stream",
];

pub fn record_trace_event(event: TraceEvent<'_>) {
    if !enabled() {
        return;
    }
    let mut labels = vec![
        ("schema".to_string(), "anvil.trace_event.v1".to_string()),
        ("trace_id".to_string(), event.trace_id.to_string()),
        ("span_id".to_string(), event.span_id.to_string()),
        ("component".to_string(), event.component.to_string()),
        ("operation".to_string(), event.operation.to_string()),
        ("status".to_string(), event.status.to_string()),
    ];
    push_optional_label(&mut labels, "parent_span_id", event.parent_span_id);
    push_optional_label(&mut labels, "request_id", event.request_id);
    push_optional_label(&mut labels, "writer_family", event.writer_family);
    push_optional_label(&mut labels, "bucket_hash", event.bucket_hash);
    let mut fields = vec![
        (
            "duration_micros".to_string(),
            PerfField::U128(event.duration.as_micros()),
        ),
        ("bytes_in".to_string(), PerfField::U64(event.bytes_in)),
        ("bytes_out".to_string(), PerfField::U64(event.bytes_out)),
        ("fsync_count".to_string(), PerfField::U64(event.fsync_count)),
    ];
    if let Some(generation) = event.boundary_schema_generation {
        fields.push((
            "boundary_schema_generation".to_string(),
            PerfField::U64(generation),
        ));
    }
    emit(PerfEvent {
        measurement: "anvil_trace_event".to_string(),
        labels,
        fields,
        timestamp_ns: unix_timestamp_nanos(),
    });
}

fn push_optional_label(labels: &mut Vec<(String, String)>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        labels.push((key.to_string(), value.to_string()));
    }
}

fn bool_label(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

pub async fn flush() {
    if let Some(sink) = PERF_SINK.get().and_then(|sink| sink.as_ref()) {
        let (ack_tx, ack_rx) = oneshot::channel();
        let _ = sink.tx.send(PerfMessage::Flush(ack_tx));
        let _ = tokio::time::timeout(Duration::from_secs(5), ack_rx).await;
    }
}

pub async fn with_context<F, T>(labels: Vec<(String, String)>, future: F) -> T
where
    F: Future<Output = T>,
{
    let mut combined = PERF_CONTEXT.try_with(Clone::clone).unwrap_or_default();
    merge_labels(&mut combined, labels);
    PERF_CONTEXT.scope(combined, future).await
}

impl Drop for PerfGuard {
    fn drop(&mut self) {
        let labels = self
            .labels
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        record_duration(self.measurement, &labels, self.started_at.elapsed());
    }
}

fn emit(mut event: PerfEvent) {
    let Some(sink) = PERF_SINK.get_or_init(init_sink).as_ref() else {
        return;
    };
    event.labels = labels_with_context(event.labels);
    let _ = sink.tx.send(PerfMessage::Event(event));
}

fn labels_with_context(mut labels: Vec<(String, String)>) -> Vec<(String, String)> {
    if let Ok(context) = PERF_CONTEXT.try_with(Clone::clone) {
        for (key, value) in context {
            if !labels.iter().any(|(existing, _)| existing == &key) {
                labels.push((key, value));
            }
        }
    }
    labels
}

fn merge_labels(target: &mut Vec<(String, String)>, labels: Vec<(String, String)>) {
    for (key, value) in labels {
        if let Some((_, existing)) = target
            .iter_mut()
            .find(|(existing_key, _)| existing_key == &key)
        {
            *existing = value;
        } else {
            target.push((key, value));
        }
    }
}

fn init_sink() -> Option<PerfSink> {
    let greptime_url = std::env::var("ANVIL_PERF_GREPTIME_URL")
        .ok()
        .or_else(|| std::env::var("ANVIL_GREPTIME_WRITE_URL").ok());
    let file_path = std::env::var("ANVIL_PERF_TRACE_FILE").ok();
    if greptime_url.is_none() && file_path.is_none() {
        return None;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return None;
    };
    let (tx, mut rx) = mpsc::unbounded_channel::<PerfMessage>();
    handle.spawn(async move {
        let client = reqwest::Client::new();
        let mut pending = Vec::new();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                maybe_message = rx.recv() => {
                    match maybe_message {
                        Some(PerfMessage::Event(event)) => {
                            pending.push(event);
                            if pending.len() >= 100 {
                                flush_events(&client, greptime_url.as_deref(), file_path.as_deref(), &mut pending).await;
                            }
                        }
                        Some(PerfMessage::Flush(ack)) => {
                            flush_events(&client, greptime_url.as_deref(), file_path.as_deref(), &mut pending).await;
                            let _ = ack.send(());
                        }
                        None => {
                            flush_events(&client, greptime_url.as_deref(), file_path.as_deref(), &mut pending).await;
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    flush_events(&client, greptime_url.as_deref(), file_path.as_deref(), &mut pending).await;
                }
            }
        }
    });
    Some(PerfSink { tx })
}

async fn flush_events(
    client: &reqwest::Client,
    greptime_url: Option<&str>,
    file_path: Option<&str>,
    pending: &mut Vec<PerfEvent>,
) {
    if pending.is_empty() {
        return;
    }
    let mut body = String::new();
    for event in pending.drain(..) {
        body.push_str(&to_line_protocol(&event));
        body.push('\n');
    }
    if let Some(path) = file_path {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        if let Ok(mut file) = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
        {
            use tokio::io::AsyncWriteExt;
            let _ = file.write_all(body.as_bytes()).await;
        }
    }
    if let Some(url) = greptime_url {
        let _ = client
            .post(url)
            .header("content-type", "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await;
    }
}

fn to_line_protocol(event: &PerfEvent) -> String {
    let mut line = String::new();
    line.push_str(&escape_key(&event.measurement));
    for (key, value) in &event.labels {
        let _ = write!(line, ",{}={}", escape_key(key), escape_key(value));
    }
    line.push(' ');
    for (idx, (key, value)) in event.fields.iter().enumerate() {
        if idx > 0 {
            line.push(',');
        }
        let _ = write!(line, "{}={}", escape_key(key), format_field(value));
    }
    let _ = write!(line, " {}", event.timestamp_ns);
    line
}

fn format_field(value: &PerfField) -> String {
    match value {
        PerfField::I64(value) => format!("{value}i"),
        PerfField::U64(value) => format!("{}i", (*value).min(i64::MAX as u64)),
        PerfField::U128(value) => format!("{}i", (*value).min(i64::MAX as u128)),
        PerfField::F64(value) => {
            if value.is_finite() {
                value.to_string()
            } else {
                "0".to_string()
            }
        }
        PerfField::Str(value) => {
            format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
        }
    }
}

fn escape_key(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace(' ', "\\ ")
        .replace(',', "\\,")
        .replace('=', "\\=")
}

fn unix_timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_protocol_escapes_tags_and_fields() {
        let event = PerfEvent {
            measurement: "anvil perf".to_string(),
            labels: vec![("path".to_string(), "/a,b=c".to_string())],
            fields: vec![
                ("duration_nanos".to_string(), PerfField::U64(42)),
                (
                    "note".to_string(),
                    PerfField::Str("a \"quoted\" value".to_string()),
                ),
            ],
            timestamp_ns: 7,
        };
        let line = to_line_protocol(&event);
        assert!(line.starts_with("anvil\\ perf,path=/a\\,b\\=c "));
        assert!(line.contains("duration_nanos=42i"));
        assert!(line.contains("note=\"a \\\"quoted\\\" value\""));
        assert!(line.ends_with(" 7"));
    }

    #[tokio::test]
    async fn context_labels_are_added_without_overriding_event_labels() {
        let event = with_context(
            vec![
                ("request_id".to_string(), "req-1".to_string()),
                ("path".to_string(), "/outer".to_string()),
            ],
            async {
                PerfEvent {
                    measurement: "anvil_request".to_string(),
                    labels: labels_with_context(vec![("path".to_string(), "/event".to_string())]),
                    fields: vec![("duration_nanos".to_string(), PerfField::U64(42))],
                    timestamp_ns: 7,
                }
            },
        )
        .await;

        assert!(
            event
                .labels
                .contains(&("request_id".to_string(), "req-1".to_string()))
        );
        assert!(
            event
                .labels
                .contains(&("path".to_string(), "/event".to_string()))
        );
        assert!(
            !event
                .labels
                .contains(&("path".to_string(), "/outer".to_string()))
        );
    }
}
