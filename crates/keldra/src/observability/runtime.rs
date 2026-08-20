use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use keldra_store::{MetadataRuntimeMetrics, SourceJournalRuntimeMetrics, Store};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";
const CGROUP_V1_MEMORY_ROOT: &str = "/sys/fs/cgroup/memory";

/// Owns the periodic process and RocksDB metrics sampler.
pub(crate) struct RuntimeMetricsTask {
    task: JoinHandle<()>,
}

impl RuntimeMetricsTask {
    pub(crate) fn start(store: Store) -> Self {
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
            let mut failure_logs = FailureLogs::default();
            let mut receipt_capacity = ReceiptCapacityHistory::default();
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                sample_once(store.clone(), &mut failure_logs, &mut receipt_capacity).await;
            }
        });
        Self { task }
    }
}

impl Drop for RuntimeMetricsTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Default)]
struct FailureLogs {
    recorded: BTreeSet<&'static str>,
}

#[derive(Default)]
struct ReceiptCapacityHistory {
    previous: Option<ReceiptCapacitySample>,
}

#[derive(Clone, Copy)]
struct ReceiptCapacitySample {
    sampled_at: std::time::Instant,
    entries: u64,
    bytes: u64,
}

impl ReceiptCapacityHistory {
    fn observe(&mut self, metrics: &MetadataRuntimeMetrics) -> Option<f64> {
        let current = ReceiptCapacitySample {
            sampled_at: std::time::Instant::now(),
            entries: metrics.mutation_receipt_entries?,
            bytes: metrics.mutation_receipt_bytes?,
        };
        let projection = self.previous.and_then(|previous| {
            projected_receipt_capacity_seconds(
                previous,
                current,
                metrics.mutation_receipt_max_entries,
                metrics.mutation_receipt_max_bytes,
            )
        });
        self.previous = Some(current);
        projection
    }
}

async fn sample_once(
    store: Store,
    failure_logs: &mut FailureLogs,
    receipt_capacity: &mut ReceiptCapacityHistory,
) {
    let joined = tokio::task::spawn_blocking(move || {
        (
            read_process_memory(),
            read_cgroup_memory(),
            store.metadata_runtime_metrics(),
            store.source_journal_runtime_metrics(),
        )
    })
    .await;
    let (process, cgroup, rocksdb, source_journal) = match joined {
        Ok(sample) => sample,
        Err(error) => {
            record_collection_failure(failure_logs, "sampler", 1, &anyhow!(error));
            return;
        }
    };

    match process {
        Ok(process) => emit_process_metrics(process),
        Err(error) => {
            tracing::debug!(gauge.keldra_process_memory_metrics_available = 0_u64);
            record_collection_failure(failure_logs, "process", 1, &error);
        }
    }
    match cgroup {
        Ok(Some(cgroup)) => emit_cgroup_metrics(cgroup),
        Ok(None) => tracing::debug!(
            gauge.keldra_cgroup_memory_metrics_available = 0_u64,
            "cgroup memory metrics are unavailable"
        ),
        Err(error) => {
            tracing::debug!(gauge.keldra_cgroup_memory_metrics_available = 0_u64);
            record_collection_failure(failure_logs, "cgroup", 1, &error);
        }
    }
    emit_rocksdb_metrics(&rocksdb, receipt_capacity);
    match source_journal {
        Ok(source_journal) => emit_source_journal_metrics(source_journal),
        Err(error) => {
            tracing::debug!(gauge.keldra_source_journal_metrics_available = 0_u64);
            record_collection_failure(failure_logs, "source_journal", 1, &error);
        }
    }
    if rocksdb.property_collection_failures != 0 {
        let error = rocksdb
            .first_collection_error
            .as_deref()
            .unwrap_or("one or more RocksDB property probes failed");
        record_collection_failure(
            failure_logs,
            "rocksdb",
            rocksdb.property_collection_failures,
            &error,
        );
    }
    if rocksdb.unavailable_properties != 0 && failure_logs.recorded.insert("rocksdb_unavailable") {
        tracing::warn!(
            property = rocksdb.first_unavailable_property.unwrap_or("unknown"),
            unavailable_properties = rocksdb.unavailable_properties,
            "one or more optional RocksDB metrics are unsupported; supported signals remain active"
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProcessMemory {
    resident_bytes: u64,
    virtual_bytes: u64,
    threads: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CgroupMemoryEvents {
    low: u64,
    high: u64,
    max: u64,
    oom: u64,
    oom_kill: u64,
    oom_group_kill: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CgroupMemory {
    current_bytes: u64,
    limit_bytes: Option<u64>,
    peak_bytes: Option<u64>,
    events: CgroupMemoryEvents,
}

fn read_process_memory() -> Result<ProcessMemory> {
    let status = std::fs::read_to_string("/proc/self/status")
        .context("read /proc/self/status for process metrics")?;
    parse_process_status(&status)
}

fn parse_process_status(status: &str) -> Result<ProcessMemory> {
    let mut resident_bytes = None;
    let mut virtual_bytes = None;
    let mut threads = None;
    for line in status.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        match name {
            "VmRSS" => resident_bytes = Some(parse_status_kib(value, name)?),
            "VmSize" => virtual_bytes = Some(parse_status_kib(value, name)?),
            "Threads" => {
                threads = Some(
                    value
                        .split_whitespace()
                        .next()
                        .context("Threads has no value")?
                        .parse()
                        .context("Threads is not an integer")?,
                )
            }
            _ => {}
        }
    }
    Ok(ProcessMemory {
        resident_bytes: resident_bytes.context("VmRSS is absent from /proc/self/status")?,
        virtual_bytes: virtual_bytes.context("VmSize is absent from /proc/self/status")?,
        threads: threads.context("Threads is absent from /proc/self/status")?,
    })
}

fn parse_status_kib(value: &str, name: &str) -> Result<u64> {
    let mut fields = value.split_whitespace();
    let kib: u64 = fields
        .next()
        .with_context(|| format!("{name} has no value"))?
        .parse()
        .with_context(|| format!("{name} is not an integer"))?;
    if fields.next() != Some("kB") {
        return Err(anyhow!("{name} does not use kB units"));
    }
    kib.checked_mul(1024)
        .with_context(|| format!("{name} byte count overflowed u64"))
}

fn read_cgroup_memory() -> Result<Option<CgroupMemory>> {
    let membership = read_optional_text("/proc/self/cgroup")?;
    let paths = membership
        .as_deref()
        .map(parse_cgroup_membership)
        .transpose()?
        .unwrap_or_default();
    for root in cgroup_roots(CGROUP_V2_ROOT, paths.v2.as_deref())? {
        if let Some(memory) = read_cgroup_v2(&root)? {
            return Ok(Some(memory));
        }
    }

    for root in cgroup_roots(CGROUP_V1_MEMORY_ROOT, paths.v1_memory.as_deref())? {
        if let Some(memory) = read_cgroup_v1(&root)? {
            return Ok(Some(memory));
        }
    }
    Ok(None)
}

fn read_cgroup_v2(root: &Path) -> Result<Option<CgroupMemory>> {
    let Some(current) = read_optional_u64(root.join("memory.current"))? else {
        return Ok(None);
    };
    let limit = read_optional_limit(root.join("memory.max"))?;
    let peak = read_optional_u64(root.join("memory.peak"))?;
    let events = read_optional_text(root.join("memory.events"))?
        .map(|value| parse_cgroup_events(&value))
        .transpose()?
        .unwrap_or_default();
    Ok(Some(CgroupMemory {
        current_bytes: current,
        limit_bytes: limit,
        peak_bytes: peak,
        events,
    }))
}

fn read_cgroup_v1(root: &Path) -> Result<Option<CgroupMemory>> {
    let Some(current) = read_optional_u64(root.join("memory.usage_in_bytes"))? else {
        return Ok(None);
    };
    let limit = read_optional_u64(root.join("memory.limit_in_bytes"))?
        .filter(|value| *value < i64::MAX as u64 / 2);
    let peak = read_optional_u64(root.join("memory.max_usage_in_bytes"))?;
    Ok(Some(CgroupMemory {
        current_bytes: current,
        limit_bytes: limit,
        peak_bytes: peak,
        events: CgroupMemoryEvents::default(),
    }))
}

#[derive(Default, Debug, PartialEq, Eq)]
struct CgroupMembership {
    v2: Option<String>,
    v1_memory: Option<String>,
}

fn parse_cgroup_membership(value: &str) -> Result<CgroupMembership> {
    let mut membership = CgroupMembership::default();
    for line in value.lines() {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next().context("cgroup line has no hierarchy")?;
        let controllers = fields.next().context("cgroup line has no controllers")?;
        let path = fields.next().context("cgroup line has no path")?;
        if hierarchy == "0" && controllers.is_empty() {
            membership.v2 = Some(path.to_owned());
        }
        if controllers
            .split(',')
            .any(|controller| controller == "memory")
        {
            membership.v1_memory = Some(path.to_owned());
        }
    }
    Ok(membership)
}

fn cgroup_roots(base: &str, member_path: Option<&str>) -> Result<Vec<PathBuf>> {
    let base = PathBuf::from(base);
    let mut roots = Vec::with_capacity(2);
    if let Some(member_path) = member_path {
        let relative = member_path.trim_start_matches('/');
        if relative.split('/').any(|segment| segment == "..") {
            return Err(anyhow!("cgroup membership path contains a parent segment"));
        }
        let member_root = base.join(relative);
        roots.push(member_root);
    }
    if !roots.iter().any(|root| root == &base) {
        roots.push(base);
    }
    Ok(roots)
}

fn read_optional_text(path: impl AsRef<Path>) -> Result<Option<String>> {
    let path = path.as_ref();
    match std::fs::read_to_string(path) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

fn read_optional_u64(path: impl AsRef<Path>) -> Result<Option<u64>> {
    let path = path.as_ref();
    read_optional_text(path)?
        .map(|value| {
            value
                .trim()
                .parse()
                .with_context(|| format!("{} is not an integer", path.display()))
        })
        .transpose()
}

fn read_optional_limit(path: impl AsRef<Path>) -> Result<Option<u64>> {
    let path = path.as_ref();
    read_optional_text(path)?
        .map(|value| match value.trim() {
            "max" => Ok(None),
            value => value
                .parse()
                .map(Some)
                .with_context(|| format!("{} is not an integer or max", path.display())),
        })
        .transpose()
        .map(Option::flatten)
}

fn parse_cgroup_events(events: &str) -> Result<CgroupMemoryEvents> {
    let mut parsed = CgroupMemoryEvents::default();
    for line in events.lines() {
        let mut fields = line.split_whitespace();
        let Some(name) = fields.next() else {
            continue;
        };
        let value: u64 = fields
            .next()
            .with_context(|| format!("cgroup memory event {name} has no value"))?
            .parse()
            .with_context(|| format!("cgroup memory event {name} is not an integer"))?;
        match name {
            "low" => parsed.low = value,
            "high" => parsed.high = value,
            "max" => parsed.max = value,
            "oom" => parsed.oom = value,
            "oom_kill" => parsed.oom_kill = value,
            "oom_group_kill" => parsed.oom_group_kill = value,
            _ => {}
        }
    }
    Ok(parsed)
}

fn emit_process_metrics(process: ProcessMemory) {
    tracing::debug!(
        gauge.keldra_process_memory_metrics_available = 1_u64,
        gauge.keldra_process_resident_memory_bytes = process.resident_bytes,
        gauge.keldra_process_virtual_memory_bytes = process.virtual_bytes,
        gauge.keldra_process_threads = process.threads,
        "sampled process resources"
    );
}

fn emit_cgroup_metrics(cgroup: CgroupMemory) {
    tracing::debug!(
        gauge.keldra_cgroup_memory_metrics_available = 1_u64,
        gauge.keldra_cgroup_memory_current_bytes = cgroup.current_bytes,
        gauge.keldra_cgroup_memory_limit_bytes = cgroup.limit_bytes.unwrap_or(0),
        gauge.keldra_cgroup_memory_limited = u64::from(cgroup.limit_bytes.is_some()),
        gauge.keldra_cgroup_memory_peak_bytes = cgroup.peak_bytes.unwrap_or(0),
        gauge.keldra_cgroup_memory_low_events = cgroup.events.low,
        gauge.keldra_cgroup_memory_high_events = cgroup.events.high,
        gauge.keldra_cgroup_memory_max_events = cgroup.events.max,
        gauge.keldra_cgroup_memory_oom_events = cgroup.events.oom,
        gauge.keldra_cgroup_memory_oom_kill_events = cgroup.events.oom_kill,
        gauge.keldra_cgroup_memory_oom_group_kill_events = cgroup.events.oom_group_kill,
        "sampled cgroup memory resources"
    );
}

fn emit_rocksdb_metrics(
    rocksdb: &MetadataRuntimeMetrics,
    receipt_capacity: &mut ReceiptCapacityHistory,
) {
    tracing::debug!(
        gauge.keldra_rocksdb_block_cache_capacity_bytes = rocksdb.block_cache_capacity_bytes,
        gauge.keldra_rocksdb_block_cache_usage_bytes = rocksdb.block_cache_usage_bytes,
        gauge.keldra_rocksdb_block_cache_pinned_bytes = rocksdb.block_cache_pinned_bytes,
        gauge.keldra_rocksdb_write_buffer_capacity_bytes = rocksdb.write_buffer_capacity_bytes,
        gauge.keldra_rocksdb_write_buffer_usage_bytes = rocksdb.write_buffer_usage_bytes,
        gauge.keldra_rocksdb_unavailable_properties = rocksdb.unavailable_properties,
        "sampled RocksDB resources"
    );
    if let Some(value) = rocksdb.active_memtable_bytes {
        tracing::debug!(gauge.keldra_rocksdb_active_memtable_bytes = value);
    }
    if let Some(value) = rocksdb.all_memtable_bytes {
        tracing::debug!(gauge.keldra_rocksdb_all_memtable_bytes = value);
    }
    if let Some(value) = rocksdb.table_reader_bytes {
        tracing::debug!(gauge.keldra_rocksdb_table_reader_bytes = value);
    }
    if let Some(value) = rocksdb.pending_compaction_bytes {
        tracing::debug!(gauge.keldra_rocksdb_pending_compaction_bytes = value);
    }
    if let Some(value) = rocksdb.immutable_memtables {
        tracing::debug!(gauge.keldra_rocksdb_immutable_memtables = value);
    }
    if let Some(value) = rocksdb.running_compactions {
        tracing::debug!(gauge.keldra_rocksdb_running_compactions = value);
    }
    if let Some(value) = rocksdb.running_flushes {
        tracing::debug!(gauge.keldra_rocksdb_running_flushes = value);
    }
    if let Some(value) = rocksdb.compaction_pending_column_families {
        tracing::debug!(gauge.keldra_rocksdb_compaction_pending_column_families = value);
    }
    if let Some(value) = rocksdb.flush_pending_column_families {
        tracing::debug!(gauge.keldra_rocksdb_flush_pending_column_families = value);
    }
    if let Some(value) = rocksdb.actual_delayed_write_rate_bytes_per_second {
        tracing::debug!(gauge.keldra_rocksdb_actual_delayed_write_rate_bytes_per_second = value);
    }
    if let Some(value) = rocksdb.write_stopped {
        tracing::debug!(gauge.keldra_rocksdb_write_stopped = value);
    }
    if let Some(value) = rocksdb.background_errors {
        tracing::debug!(gauge.keldra_rocksdb_background_errors = value);
    }
    if rocksdb.write_stopped.is_some()
        || rocksdb.actual_delayed_write_rate_bytes_per_second.is_some()
    {
        tracing::debug!(
            gauge.keldra_rocksdb_write_stalled = u64::from(
                rocksdb.write_stopped.is_some_and(|value| value != 0)
                    || rocksdb
                        .actual_delayed_write_rate_bytes_per_second
                        .is_some_and(|value| value != 0)
            )
        );
    }
    let projected_capacity_seconds = receipt_capacity.observe(rocksdb);
    match (
        rocksdb.mutation_receipt_entries,
        rocksdb.mutation_receipt_bytes,
        rocksdb.mutation_receipt_oldest_age_seconds,
    ) {
        (Some(entries), Some(bytes), Some(oldest_age_seconds)) => tracing::debug!(
            gauge.keldra_mutation_receipt_metrics_available = 1_u64,
            gauge.keldra_mutation_receipt_entries = entries,
            gauge.keldra_mutation_receipt_bytes = bytes,
            gauge.keldra_mutation_receipt_max_entries = rocksdb.mutation_receipt_max_entries,
            gauge.keldra_mutation_receipt_max_bytes = rocksdb.mutation_receipt_max_bytes,
            gauge.keldra_mutation_receipt_entry_occupancy_ratio =
                entries as f64 / rocksdb.mutation_receipt_max_entries as f64,
            gauge.keldra_mutation_receipt_byte_occupancy_ratio =
                bytes as f64 / rocksdb.mutation_receipt_max_bytes as f64,
            gauge.keldra_mutation_receipt_oldest_retained_age_seconds = oldest_age_seconds,
            gauge.keldra_mutation_receipt_projected_capacity_available =
                u64::from(projected_capacity_seconds.is_some()),
            gauge.keldra_mutation_receipt_time_to_projected_capacity_seconds =
                projected_capacity_seconds.unwrap_or(0.0),
            "sampled mutation receipt capacity"
        ),
        _ => tracing::debug!(
            gauge.keldra_mutation_receipt_metrics_available = 0_u64,
            "mutation receipt capacity metrics are unavailable"
        ),
    }
}

fn projected_receipt_capacity_seconds(
    previous: ReceiptCapacitySample,
    current: ReceiptCapacitySample,
    maximum_entries: u64,
    maximum_bytes: u64,
) -> Option<f64> {
    if current.entries >= maximum_entries || current.bytes >= maximum_bytes {
        return Some(0.0);
    }
    let elapsed = current
        .sampled_at
        .saturating_duration_since(previous.sampled_at)
        .as_secs_f64();
    if elapsed == 0.0 {
        return None;
    }
    let entries = capacity_seconds(previous.entries, current.entries, maximum_entries, elapsed);
    let bytes = capacity_seconds(previous.bytes, current.bytes, maximum_bytes, elapsed);
    match (entries, bytes) {
        (Some(entries), Some(bytes)) => Some(entries.min(bytes)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn capacity_seconds(previous: u64, current: u64, maximum: u64, elapsed: f64) -> Option<f64> {
    let growth = current.checked_sub(previous)?;
    if growth == 0 {
        return None;
    }
    Some(maximum.saturating_sub(current) as f64 / (growth as f64 / elapsed))
}

fn emit_source_journal_metrics(source: SourceJournalRuntimeMetrics) {
    let prune_safe_through = source.prune_safe_through();
    tracing::debug!(
        gauge.keldra_source_journal_metrics_available = 1_u64,
        gauge.keldra_source_journal_tail = source.tail,
        gauge.keldra_source_journal_settled_through = source.settled_through,
        gauge.keldra_source_journal_retention_floor = source.retention_floor,
        gauge.keldra_source_journal_reference_safe_through = source.reference_safe_through,
        gauge.keldra_source_journal_index_safe_through = source.index_safe_through,
        gauge.keldra_source_journal_accounting_safe_through = source.accounting_safe_through,
        gauge.keldra_source_journal_prune_safe_through = prune_safe_through,
        gauge.keldra_source_journal_unsettled_entries =
            source.tail.saturating_sub(source.settled_through),
        gauge.keldra_source_journal_reference_lag_entries =
            source.tail.saturating_sub(source.reference_safe_through),
        gauge.keldra_source_journal_index_lag_entries =
            source.tail.saturating_sub(source.index_safe_through),
        gauge.keldra_source_journal_accounting_lag_entries =
            source.tail.saturating_sub(source.accounting_safe_through),
        gauge.keldra_source_journal_prune_lag_entries =
            source.tail.saturating_sub(prune_safe_through),
        gauge.keldra_source_journal_retained_entries = source.retained_entries,
        gauge.keldra_source_journal_retained_bytes = source.retained_bytes,
        gauge.keldra_source_journal_max_entries = source.max_entries,
        gauge.keldra_source_journal_max_bytes = source.max_bytes,
        gauge.keldra_source_journal_progress_debt_entries = source.progress_debt_entries(),
        gauge.keldra_source_journal_progress_debt_bytes = source.progress_debt_bytes(),
        gauge.keldra_source_journal_progress_debt_peak_entries = source.progress_debt_peak_entries,
        gauge.keldra_source_journal_progress_debt_peak_bytes = source.progress_debt_peak_bytes,
        gauge.keldra_source_journal_entry_occupancy_ratio =
            source.retained_entries as f64 / source.max_entries as f64,
        gauge.keldra_source_journal_byte_occupancy_ratio =
            source.retained_bytes as f64 / source.max_bytes as f64,
        "sampled source-journal safety and capacity"
    );
}

fn record_collection_failure(
    failure_logs: &mut FailureLogs,
    component: &'static str,
    failures: u64,
    error: &impl std::fmt::Display,
) {
    tracing::debug!(
        monotonic_counter.keldra_runtime_metrics_collection_failures_total = failures,
        component,
        "runtime metrics collection failed"
    );
    if failure_logs.recorded.insert(component) {
        tracing::warn!(component, error = %error, "runtime metrics collection error");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_capacity_uses_the_first_growing_bound() {
        let now = std::time::Instant::now();
        let previous = ReceiptCapacitySample {
            sampled_at: now - Duration::from_secs(10),
            entries: 10,
            bytes: 100,
        };
        let current = ReceiptCapacitySample {
            sampled_at: now,
            entries: 20,
            bytes: 300,
        };

        let seconds = projected_receipt_capacity_seconds(previous, current, 100, 1_000).unwrap();
        assert!((seconds - 35.0).abs() < f64::EPSILON);
        assert_eq!(
            projected_receipt_capacity_seconds(previous, current, 20, 1_000),
            Some(0.0)
        );
    }

    #[test]
    fn receipt_capacity_is_unknown_without_positive_growth() {
        let now = std::time::Instant::now();
        let previous = ReceiptCapacitySample {
            sampled_at: now - Duration::from_secs(10),
            entries: 20,
            bytes: 300,
        };
        let current = ReceiptCapacitySample {
            sampled_at: now,
            entries: 19,
            bytes: 250,
        };

        assert_eq!(
            projected_receipt_capacity_seconds(previous, current, 100, 1_000),
            None
        );
    }

    #[test]
    fn parses_linux_process_memory_without_exporting_names() {
        let parsed = parse_process_status(
            "Name:\tkeldra-server\nVmSize:\t1234 kB\nVmRSS:\t321 kB\nThreads:\t17\n",
        )
        .unwrap();
        assert_eq!(
            parsed,
            ProcessMemory {
                resident_bytes: 321 * 1024,
                virtual_bytes: 1234 * 1024,
                threads: 17,
            }
        );
    }

    #[test]
    fn rejects_incomplete_or_wrong_unit_process_status() {
        assert!(parse_process_status("VmSize: 1 MB\nVmRSS: 1 kB\nThreads: 1").is_err());
        assert!(parse_process_status("VmSize: 1 kB\nVmRSS: 1 kB").is_err());
    }

    #[test]
    fn parses_known_cgroup_events_and_ignores_future_fields() {
        let parsed = parse_cgroup_events(
            "low 2\nhigh 3\nmax 4\noom 5\noom_kill 6\noom_group_kill 7\nfuture 8\n",
        )
        .unwrap();
        assert_eq!(
            parsed,
            CgroupMemoryEvents {
                low: 2,
                high: 3,
                max: 4,
                oom: 5,
                oom_kill: 6,
                oom_group_kill: 7,
            }
        );
    }

    #[test]
    fn resolves_unified_and_legacy_cgroup_membership() {
        let membership = parse_cgroup_membership(
            "0::/user.slice/app.scope\n7:cpu,cpuacct:/apps\n6:memory:/memory/apps\n",
        )
        .unwrap();
        assert_eq!(
            membership,
            CgroupMembership {
                v2: Some("/user.slice/app.scope".to_owned()),
                v1_memory: Some("/memory/apps".to_owned()),
            }
        );
        assert_eq!(
            cgroup_roots("/sys/fs/cgroup", membership.v2.as_deref()).unwrap(),
            vec![
                PathBuf::from("/sys/fs/cgroup/user.slice/app.scope"),
                PathBuf::from("/sys/fs/cgroup"),
            ]
        );
        assert!(cgroup_roots("/sys/fs/cgroup", Some("/../../escape")).is_err());
    }
}
