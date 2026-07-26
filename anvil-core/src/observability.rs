use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const OBJECT_WRITE_LATENCY: &str = "object_write_latency";
pub const OBJECT_READ_LATENCY: &str = "object_read_latency";
pub const METADATA_JOURNAL_APPEND_LATENCY: &str = "metadata_journal_append_latency";
pub const MANIFEST_PUBLISH_LATENCY: &str = "manifest_publish_latency";
pub const PREFIX_LIST_LATENCY: &str = "prefix_list_latency";
pub const FULL_TEXT_INDEXING_LAG: &str = "full_text_indexing_lag";
pub const VECTOR_INDEXING_LAG: &str = "vector_indexing_lag";
pub const AUTHZ_TUPLE_WRITE_LATENCY: &str = "authz_tuple_write_latency";
pub const AUTHZ_DERIVED_INDEX_LAG: &str = "authz_derived_index_lag";
pub const RESERVED_NAMESPACE_REJECTION_COUNT: &str = "reserved_namespace_rejection_count";
pub const PERSONALDB_WITNESS_LATENCY: &str = "personaldb_witness_latency";
pub const PERSONALDB_COMMIT_REJECTION_REASONS: &str = "personaldb_commit_rejection_reasons";
pub const PERSONALDB_PROJECTION_LAG: &str = "personaldb_projection_lag";
pub const WATCH_STREAM_LAG: &str = "watch_stream_lag";
pub const PARTITION_RECOVERY_DURATION: &str = "partition_recovery_duration";
pub const COMPACTION_BACKLOG: &str = "compaction_backlog";
pub const REPAIR_FINDINGS: &str = "repair_findings";
pub const MVCC_OUTBOX_BACKLOG: &str = "mvcc_outbox_backlog";
pub const MVCC_OUTBOX_OLDEST_AGE_MS: &str = "mvcc_outbox_oldest_age_ms";
pub const MVCC_OUTBOX_FAILURES: &str = "mvcc_outbox_failures";

pub const REQUIRED_METRICS: &[&str] = &[
    OBJECT_WRITE_LATENCY,
    OBJECT_READ_LATENCY,
    METADATA_JOURNAL_APPEND_LATENCY,
    MANIFEST_PUBLISH_LATENCY,
    PREFIX_LIST_LATENCY,
    FULL_TEXT_INDEXING_LAG,
    VECTOR_INDEXING_LAG,
    AUTHZ_TUPLE_WRITE_LATENCY,
    AUTHZ_DERIVED_INDEX_LAG,
    RESERVED_NAMESPACE_REJECTION_COUNT,
    PERSONALDB_WITNESS_LATENCY,
    PERSONALDB_COMMIT_REJECTION_REASONS,
    PERSONALDB_PROJECTION_LAG,
    WATCH_STREAM_LAG,
    PARTITION_RECOVERY_DURATION,
    COMPACTION_BACKLOG,
    REPAIR_FINDINGS,
    MVCC_OUTBOX_BACKLOG,
    MVCC_OUTBOX_OLDEST_AGE_MS,
    MVCC_OUTBOX_FAILURES,
];

#[derive(Clone, Debug, Default)]
pub struct Observability {
    inner: Arc<Mutex<MetricState>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct MetricKey {
    pub name: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetricSnapshot {
    pub count: u64,
    pub value: i64,
    pub sum_nanos: u128,
    pub min_nanos: Option<u128>,
    pub max_nanos: Option<u128>,
}

#[derive(Debug, Default)]
struct MetricState {
    metrics: BTreeMap<MetricKey, MetricSnapshot>,
}

#[derive(Debug)]
pub struct LatencyGuard {
    observability: Observability,
    metric_name: &'static str,
    labels: Vec<(String, String)>,
    started_at: Instant,
}

impl Observability {
    pub fn increment_counter(&self, metric_name: &str, labels: &[(&str, &str)]) {
        let key = metric_key(metric_name, labels);
        let mut state = self.inner.lock().expect("observability mutex poisoned");
        let sample = state.metrics.entry(key).or_default();
        sample.count = sample.count.saturating_add(1);
        sample.value = sample.value.saturating_add(1);
    }

    pub fn record_latency(&self, metric_name: &str, labels: &[(&str, &str)], duration: Duration) {
        self.record_duration(metric_name, labels, duration);
    }

    pub fn record_duration(&self, metric_name: &str, labels: &[(&str, &str)], duration: Duration) {
        let elapsed = duration.as_nanos();
        let key = metric_key(metric_name, labels);
        let mut state = self.inner.lock().expect("observability mutex poisoned");
        let sample = state.metrics.entry(key).or_default();
        sample.count = sample.count.saturating_add(1);
        sample.sum_nanos = sample.sum_nanos.saturating_add(elapsed);
        sample.min_nanos = Some(
            sample
                .min_nanos
                .map_or(elapsed, |current| current.min(elapsed)),
        );
        sample.max_nanos = Some(
            sample
                .max_nanos
                .map_or(elapsed, |current| current.max(elapsed)),
        );
        sample.value = i64::try_from(elapsed).unwrap_or(i64::MAX);
    }

    pub fn set_gauge(&self, metric_name: &str, labels: &[(&str, &str)], value: i64) {
        let key = metric_key(metric_name, labels);
        let mut state = self.inner.lock().expect("observability mutex poisoned");
        let sample = state.metrics.entry(key).or_default();
        sample.count = sample.count.saturating_add(1);
        sample.value = value;
    }

    pub fn latency_guard(
        &self,
        metric_name: &'static str,
        labels: &[(&str, &str)],
    ) -> LatencyGuard {
        LatencyGuard {
            observability: self.clone(),
            metric_name,
            labels: labels
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            started_at: Instant::now(),
        }
    }

    pub fn snapshot(&self) -> BTreeMap<MetricKey, MetricSnapshot> {
        self.inner
            .lock()
            .expect("observability mutex poisoned")
            .metrics
            .clone()
    }
}

impl Drop for LatencyGuard {
    fn drop(&mut self) {
        let labels = self
            .labels
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        self.observability
            .record_duration(self.metric_name, &labels, self.started_at.elapsed());
    }
}

pub fn metric_is_required(metric_name: &str) -> bool {
    REQUIRED_METRICS.contains(&metric_name)
}

fn metric_key(metric_name: &str, labels: &[(&str, &str)]) -> MetricKey {
    MetricKey {
        name: metric_name.to_string(),
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn required_metrics_cover_rfc_observability_catalog() {
        let required = REQUIRED_METRICS.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(required.len(), REQUIRED_METRICS.len());
        for name in [
            "object_write_latency",
            "object_read_latency",
            "metadata_journal_append_latency",
            "manifest_publish_latency",
            "prefix_list_latency",
            "full_text_indexing_lag",
            "vector_indexing_lag",
            "authz_tuple_write_latency",
            "authz_derived_index_lag",
            "reserved_namespace_rejection_count",
            "PersonalDB witness latency",
            "PersonalDB commit rejection reasons",
            "PersonalDB projection lag",
            "watch_stream_lag",
            "partition_recovery_duration",
            "compaction_backlog",
            "repair_findings",
        ] {
            let canonical_name = name
                .replace("PersonalDB ", "personaldb_")
                .replace(' ', "_")
                .to_lowercase();
            assert!(
                required.contains(canonical_name.as_str()),
                "missing required metric {canonical_name}"
            );
        }
    }

    #[test]
    fn counters_and_gauges_are_snapshotted_by_name_and_labels() {
        let observability = Observability::default();
        observability.increment_counter(
            RESERVED_NAMESPACE_REJECTION_COUNT,
            &[("api", "native"), ("operation", "put_object")],
        );
        observability.increment_counter(
            RESERVED_NAMESPACE_REJECTION_COUNT,
            &[("operation", "put_object"), ("api", "native")],
        );
        observability.set_gauge(COMPACTION_BACKLOG, &[("family", "object_metadata")], 7);

        let snapshot = observability.snapshot();
        let reserved_key = metric_key(
            RESERVED_NAMESPACE_REJECTION_COUNT,
            &[("api", "native"), ("operation", "put_object")],
        );
        let compaction_key = metric_key(COMPACTION_BACKLOG, &[("family", "object_metadata")]);

        assert_eq!(snapshot[&reserved_key].count, 2);
        assert_eq!(snapshot[&reserved_key].value, 2);
        assert_eq!(snapshot[&compaction_key].count, 1);
        assert_eq!(snapshot[&compaction_key].value, 7);
    }

    #[test]
    fn latency_samples_record_count_bounds_and_sum() {
        let observability = Observability::default();
        observability.record_latency(
            OBJECT_READ_LATENCY,
            &[("api", "s3")],
            Duration::from_millis(4),
        );
        observability.record_latency(
            OBJECT_READ_LATENCY,
            &[("api", "s3")],
            Duration::from_millis(9),
        );

        let snapshot = observability.snapshot();
        let sample = &snapshot[&metric_key(OBJECT_READ_LATENCY, &[("api", "s3")])];
        assert_eq!(sample.count, 2);
        assert_eq!(sample.sum_nanos, Duration::from_millis(13).as_nanos());
        assert_eq!(sample.min_nanos, Some(Duration::from_millis(4).as_nanos()));
        assert_eq!(sample.max_nanos, Some(Duration::from_millis(9).as_nanos()));
    }

    #[test]
    fn latency_guard_records_on_drop() {
        let observability = Observability::default();
        {
            let _guard = observability.latency_guard(PREFIX_LIST_LATENCY, &[("api", "native")]);
        }

        let snapshot = observability.snapshot();
        let sample = &snapshot[&metric_key(PREFIX_LIST_LATENCY, &[("api", "native")])];
        assert_eq!(sample.count, 1);
        assert!(sample.max_nanos.unwrap_or_default() >= sample.min_nanos.unwrap_or_default());
    }
}
