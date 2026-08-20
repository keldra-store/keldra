use std::time::Duration;

use serde::Serialize;

pub const QUALIFICATION_SCHEMA: &str = "keldra.perf.osv_import.v3";
pub const TARGET_SECONDS: f64 = 150.0;

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct LatencySummary {
    pub sample_count: u64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
}

impl LatencySummary {
    pub fn from_durations(samples: &[Duration]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        let mut millis = samples
            .iter()
            .map(|sample| sample.as_secs_f64() * 1_000.0)
            .collect::<Vec<_>>();
        millis.sort_by(f64::total_cmp);
        Self {
            sample_count: millis.len() as u64,
            mean_ms: millis.iter().sum::<f64>() / millis.len() as f64,
            p50_ms: nearest_rank(&millis, 50),
            p95_ms: nearest_rank(&millis, 95),
            p99_ms: nearest_rank(&millis, 99),
        }
    }
}

fn nearest_rank(sorted: &[f64], percentile: usize) -> f64 {
    debug_assert!(!sorted.is_empty());
    debug_assert!((1..=100).contains(&percentile));
    let rank = (percentile * sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunCounts {
    pub source_documents: u64,
    pub normalised_source_records: u64,
    pub shard_objects: u64,
    pub source_definition_payload_bytes: u64,
    pub shard_payload_bytes: u64,
    pub manifest_payload_bytes: u64,
    pub data_bulk_requests: u64,
    pub manifest_bulk_requests: u64,
    pub replayed_mutations: u64,
}

impl RunCounts {
    pub fn logical_mutations(&self) -> u64 {
        self.shard_objects.saturating_add(2)
    }

    pub fn payload_bytes(&self) -> u64 {
        self.source_definition_payload_bytes
            .saturating_add(self.shard_payload_bytes)
            .saturating_add(self.manifest_payload_bytes)
    }

    pub fn bulk_requests(&self) -> u64 {
        self.data_bulk_requests
            .saturating_add(self.manifest_bulk_requests)
    }
}

#[derive(Debug, Serialize)]
pub struct QualificationReport<'a> {
    pub schema: &'static str,
    pub measured: bool,
    pub passed: bool,
    pub target_seconds: f64,
    pub corpus: CorpusReport<'a>,
    pub schema_shape: SchemaShapeReport,
    pub software: SoftwareReport<'a>,
    pub workload: WorkloadReport<'a>,
    pub parsing: ParsingReport,
    pub result: ResultReport,
    pub verification: VerificationReport,
    pub limitations: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct CorpusReport<'a> {
    pub path: &'a str,
    pub expected_sha256: &'a str,
    pub observed_sha256: &'a str,
    pub archive_bytes: u64,
    pub snapshot_day: &'a str,
    pub snapshot_id: &'a str,
}

#[derive(Debug, Serialize)]
pub struct SchemaShapeReport {
    pub source_definition_schema: &'static str,
    pub source_record_schema: &'static str,
    pub snapshot_manifest_schema: &'static str,
    pub source: &'static str,
    pub bucket: &'static str,
    pub source_definition_path: &'static str,
    pub shard_path: &'static str,
    pub manifest_path: &'static str,
    pub shard_format: &'static str,
    pub shard_compression: &'static str,
    pub manifest_is_authoritative: bool,
    pub per_object_user_metadata: bool,
    pub phases: [&'static str; 3],
}

impl Default for SchemaShapeReport {
    fn default() -> Self {
        Self {
            source_definition_schema: "keldra.osv.source-definition.v1",
            source_record_schema: "keldra.osv.source-record.v1",
            snapshot_manifest_schema: "keldra.osv.snapshot-manifest.v1",
            source: "osv",
            bucket: "keldra-osv-qualification",
            source_definition_path: "entities/source-definition/{sha256(source-definition\\0osv)}/current.json",
            shard_path: "shards/v1/{records_sha256[0..2]}/{records_sha256}.ndjson.zst",
            manifest_path: "snapshots/{snapshot_id}/manifest.json",
            shard_format: "keldra.osv.source-record.ndjson.v1",
            shard_compression: "zstd-6",
            manifest_is_authoritative: true,
            per_object_user_metadata: false,
            phases: [
                "write immutable source definition and compressed content-addressed shards",
                "write immutable authoritative snapshot manifest with exact shard versions",
                "verify every exact current object head",
            ],
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SoftwareReport<'a> {
    pub keldra_commit: &'a str,
}

#[derive(Debug, Serialize)]
pub struct WorkloadReport<'a> {
    pub endpoint: &'a str,
    pub tenant: &'a str,
    pub bucket: &'a str,
    pub source_url: &'a str,
    pub source_cadence_hours: u16,
    pub durability_class: &'a str,
    pub node_count: u8,
    pub batch_size_operations: usize,
    pub maximum_batch_payload_bytes: usize,
    pub shard_uncompressed_target_bytes: usize,
    pub write_concurrency: usize,
    pub verification_concurrency: usize,
    pub clean_target_verified: bool,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ParsingReport {
    pub archive_entries: u64,
    pub json_documents: u64,
    pub accepted_source_documents: u64,
    pub normalised_source_records: u64,
    pub unscoped_documents: u64,
    pub malformed_documents: u64,
    pub oversized_documents: u64,
    pub decompressed_json_bytes: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ResultReport {
    pub source_documents: u64,
    pub normalised_source_records: u64,
    pub shard_objects: u64,
    pub logical_mutations: u64,
    pub source_definition_payload_bytes: u64,
    pub shard_payload_bytes: u64,
    pub manifest_payload_bytes: u64,
    pub total_payload_bytes: u64,
    pub data_bulk_requests: u64,
    pub manifest_bulk_requests: u64,
    pub bulk_requests: u64,
    pub batch_fill_average_operations: f64,
    pub replayed_mutations: u64,
    pub end_to_end_seconds: f64,
    pub source_documents_per_second: f64,
    pub normalised_source_records_per_second: f64,
    pub payload_mebibytes_per_second: f64,
    pub bulk_request_latency_ms: LatencySummary,
    pub data_request_latency_ms: LatencySummary,
    pub manifest_request_latency_ms: LatencySummary,
}

impl ResultReport {
    pub fn calculate(
        counts: &RunCounts,
        elapsed: Duration,
        data_latencies: &[Duration],
        manifest_latencies: &[Duration],
    ) -> Self {
        let seconds = elapsed.as_secs_f64();
        let logical_mutations = counts.logical_mutations();
        let bulk_requests = counts.bulk_requests();
        let mut all_latencies = Vec::with_capacity(data_latencies.len() + manifest_latencies.len());
        all_latencies.extend_from_slice(data_latencies);
        all_latencies.extend_from_slice(manifest_latencies);
        Self {
            source_documents: counts.source_documents,
            normalised_source_records: counts.normalised_source_records,
            shard_objects: counts.shard_objects,
            logical_mutations,
            source_definition_payload_bytes: counts.source_definition_payload_bytes,
            shard_payload_bytes: counts.shard_payload_bytes,
            manifest_payload_bytes: counts.manifest_payload_bytes,
            total_payload_bytes: counts.payload_bytes(),
            data_bulk_requests: counts.data_bulk_requests,
            manifest_bulk_requests: counts.manifest_bulk_requests,
            bulk_requests,
            batch_fill_average_operations: if bulk_requests == 0 {
                0.0
            } else {
                logical_mutations as f64 / bulk_requests as f64
            },
            replayed_mutations: counts.replayed_mutations,
            end_to_end_seconds: seconds,
            source_documents_per_second: rate(counts.source_documents, seconds),
            normalised_source_records_per_second: rate(counts.normalised_source_records, seconds),
            payload_mebibytes_per_second: if seconds == 0.0 {
                0.0
            } else {
                counts.payload_bytes() as f64 / (1024.0 * 1024.0) / seconds
            },
            bulk_request_latency_ms: LatencySummary::from_durations(&all_latencies),
            data_request_latency_ms: LatencySummary::from_durations(data_latencies),
            manifest_request_latency_ms: LatencySummary::from_durations(manifest_latencies),
        }
    }
}

fn rate(count: u64, seconds: f64) -> f64 {
    if seconds == 0.0 {
        0.0
    } else {
        count as f64 / seconds
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct VerificationReport {
    pub expected_object_count: u64,
    pub verified_object_count: u64,
    pub duration_seconds: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles_are_stable_for_small_samples() {
        let samples = (1..=100).map(Duration::from_millis).collect::<Vec<_>>();
        let summary = LatencySummary::from_durations(&samples);
        assert_eq!(summary.sample_count, 100);
        assert_eq!(summary.mean_ms, 50.5);
        assert_eq!(summary.p50_ms, 50.0);
        assert_eq!(summary.p95_ms, 95.0);
        assert_eq!(summary.p99_ms, 99.0);

        let single = LatencySummary::from_durations(&[Duration::from_millis(7)]);
        assert_eq!(single.p50_ms, 7.0);
        assert_eq!(single.mean_ms, 7.0);
        assert_eq!(single.p95_ms, 7.0);
        assert_eq!(single.p99_ms, 7.0);
    }

    #[test]
    fn result_calculation_counts_definition_shards_and_manifest() {
        let counts = RunCounts {
            source_documents: 300,
            normalised_source_records: 500,
            shard_objects: 8,
            source_definition_payload_bytes: 400,
            shard_payload_bytes: 12_000,
            manifest_payload_bytes: 3_000,
            data_bulk_requests: 2,
            manifest_bulk_requests: 1,
            replayed_mutations: 0,
        };
        let result = ResultReport::calculate(
            &counts,
            Duration::from_secs(2),
            &[Duration::from_millis(4), Duration::from_millis(8)],
            &[Duration::from_millis(6)],
        );
        assert_eq!(result.source_documents, 300);
        assert_eq!(result.normalised_source_records, 500);
        assert_eq!(result.logical_mutations, 10);
        assert_eq!(result.total_payload_bytes, 15_400);
        assert_eq!(result.bulk_requests, 3);
        assert_eq!(result.source_documents_per_second, 150.0);
        assert_eq!(result.normalised_source_records_per_second, 250.0);
        assert_eq!(result.bulk_request_latency_ms.p50_ms, 6.0);
        assert_eq!(result.bulk_request_latency_ms.p95_ms, 8.0);
    }

    #[test]
    fn report_shape_names_manifest_authority_and_metadata_omission() {
        let encoded = serde_json::to_value(SchemaShapeReport::default()).unwrap();
        assert_eq!(encoded["manifest_is_authoritative"], true);
        assert_eq!(encoded["per_object_user_metadata"], false);
        assert_eq!(
            encoded["snapshot_manifest_schema"],
            "keldra.osv.snapshot-manifest.v1"
        );
    }
}
