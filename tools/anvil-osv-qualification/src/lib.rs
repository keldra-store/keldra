use std::time::Duration;

use serde::Serialize;

pub const QUALIFICATION_SCHEMA: &str = "anvil.perf.osv_import.v2";
pub const DD_SCHEMA_VERSION: &str = "developer-defence.source-raw-record-head.v1";
pub const DD_SCHEMA_SOURCE_COMMIT: &str = "ac838a79e5b9fd4aed08d1ac7786e5374b01b733";
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
    pub source_records: u64,
    pub raw_payload_bytes: u64,
    pub head_payload_bytes: u64,
    pub leaf_bulk_requests: u64,
    pub head_bulk_requests: u64,
    pub replayed_mutations: u64,
}

impl RunCounts {
    pub fn logical_mutations(&self) -> u64 {
        self.source_records.saturating_mul(2)
    }

    pub fn payload_bytes(&self) -> u64 {
        self.raw_payload_bytes
            .saturating_add(self.head_payload_bytes)
    }

    pub fn bulk_requests(&self) -> u64 {
        self.leaf_bulk_requests
            .saturating_add(self.head_bulk_requests)
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
}

#[derive(Debug, Serialize)]
pub struct SchemaShapeReport {
    pub developer_defence_schema: &'static str,
    pub source: &'static str,
    pub bucket: &'static str,
    pub leaf_path: &'static str,
    pub head_path: &'static str,
    pub writes_per_source_record: u8,
    pub phases: [&'static str; 2],
}

impl Default for SchemaShapeReport {
    fn default() -> Self {
        Self {
            developer_defence_schema: DD_SCHEMA_VERSION,
            source: "osv",
            bucket: "dd-source-osv-raw",
            leaf_path: "raw/osv/{sha256(trimmed id)}/record.json",
            head_path: "raw/osv/{sha256(trimmed id)}/current.json",
            writes_per_source_record: 2,
            phases: [
                "bulk canonical raw leaves and retain returned versions",
                "bulk canonical heads referring to those versions",
            ],
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SoftwareReport<'a> {
    pub anvil_commit: &'a str,
    pub developer_defence_schema_source_commit: &'static str,
}

#[derive(Debug, Serialize)]
pub struct WorkloadReport<'a> {
    pub endpoint: &'a str,
    pub tenant: &'a str,
    pub bucket: &'a str,
    pub durability_class: &'a str,
    pub node_count: u8,
    pub batch_size_operations: usize,
    pub maximum_batch_payload_bytes: usize,
    pub write_concurrency: usize,
    pub verification_concurrency: usize,
    pub clean_target_asserted: bool,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ParsingReport {
    pub archive_entries: u64,
    pub json_documents: u64,
    pub accepted_source_records_n: u64,
    pub malformed_documents: u64,
    pub oversized_documents: u64,
    pub decompressed_json_bytes: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ResultReport {
    pub source_records_n: u64,
    pub logical_mutations_m: u64,
    pub raw_payload_bytes: u64,
    pub head_payload_bytes: u64,
    pub total_payload_bytes: u64,
    pub leaf_bulk_requests: u64,
    pub head_bulk_requests: u64,
    pub bulk_requests: u64,
    pub batch_fill_average_operations: f64,
    pub replayed_mutations: u64,
    pub end_to_end_seconds: f64,
    pub source_records_per_second: f64,
    pub logical_mutations_per_second: f64,
    pub bulk_request_latency_ms: LatencySummary,
    pub leaf_request_latency_ms: LatencySummary,
    pub head_request_latency_ms: LatencySummary,
}

impl ResultReport {
    pub fn calculate(
        counts: &RunCounts,
        elapsed: Duration,
        leaf_latencies: &[Duration],
        head_latencies: &[Duration],
    ) -> Self {
        let seconds = elapsed.as_secs_f64();
        let logical_mutations = counts.logical_mutations();
        let bulk_requests = counts.bulk_requests();
        let mut all_latencies = Vec::with_capacity(leaf_latencies.len() + head_latencies.len());
        all_latencies.extend_from_slice(leaf_latencies);
        all_latencies.extend_from_slice(head_latencies);
        Self {
            source_records_n: counts.source_records,
            logical_mutations_m: logical_mutations,
            raw_payload_bytes: counts.raw_payload_bytes,
            head_payload_bytes: counts.head_payload_bytes,
            total_payload_bytes: counts.payload_bytes(),
            leaf_bulk_requests: counts.leaf_bulk_requests,
            head_bulk_requests: counts.head_bulk_requests,
            bulk_requests,
            batch_fill_average_operations: if bulk_requests == 0 {
                0.0
            } else {
                logical_mutations as f64 / bulk_requests as f64
            },
            replayed_mutations: counts.replayed_mutations,
            end_to_end_seconds: seconds,
            source_records_per_second: rate(counts.source_records, seconds),
            logical_mutations_per_second: rate(logical_mutations, seconds),
            bulk_request_latency_ms: LatencySummary::from_durations(&all_latencies),
            leaf_request_latency_ms: LatencySummary::from_durations(leaf_latencies),
            head_request_latency_ms: LatencySummary::from_durations(head_latencies),
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
    fn result_calculation_reports_n_m_bytes_batches_and_rates() {
        let counts = RunCounts {
            source_records: 300,
            raw_payload_bytes: 12_000,
            head_payload_bytes: 3_000,
            leaf_bulk_requests: 2,
            head_bulk_requests: 2,
            replayed_mutations: 0,
        };
        let result = ResultReport::calculate(
            &counts,
            Duration::from_secs(2),
            &[Duration::from_millis(4), Duration::from_millis(8)],
            &[Duration::from_millis(6), Duration::from_millis(10)],
        );
        assert_eq!(result.source_records_n, 300);
        assert_eq!(result.logical_mutations_m, 600);
        assert_eq!(result.total_payload_bytes, 15_000);
        assert_eq!(result.bulk_requests, 4);
        assert_eq!(result.batch_fill_average_operations, 150.0);
        assert_eq!(result.source_records_per_second, 150.0);
        assert_eq!(result.logical_mutations_per_second, 300.0);
        assert_eq!(result.bulk_request_latency_ms.p50_ms, 6.0);
        assert_eq!(result.bulk_request_latency_ms.p95_ms, 10.0);
    }

    #[test]
    fn report_serialization_keeps_the_qualification_dimensions_named() {
        let counts = RunCounts {
            source_records: 2,
            raw_payload_bytes: 100,
            head_payload_bytes: 40,
            leaf_bulk_requests: 1,
            head_bulk_requests: 1,
            replayed_mutations: 0,
        };
        let result = ResultReport::calculate(
            &counts,
            Duration::from_secs(1),
            &[Duration::from_millis(1)],
            &[Duration::from_millis(2)],
        );
        let encoded = serde_json::to_value(result).unwrap();
        assert_eq!(encoded["source_records_n"], 2);
        assert_eq!(encoded["logical_mutations_m"], 4);
        assert_eq!(encoded["total_payload_bytes"], 140);
        assert_eq!(encoded["bulk_requests"], 2);
        assert_eq!(encoded["bulk_request_latency_ms"]["p99_ms"], 2.0);
    }
}
