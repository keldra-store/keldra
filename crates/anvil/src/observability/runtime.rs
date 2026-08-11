use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anvil_store::{MetadataRuntimeMetrics, Store};
use anyhow::{Context, Result, anyhow};
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
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                sample_once(store.clone(), &mut failure_logs).await;
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

async fn sample_once(store: Store, failure_logs: &mut FailureLogs) {
    let joined = tokio::task::spawn_blocking(move || {
        (
            read_process_memory(),
            read_cgroup_memory(),
            store.metadata_runtime_metrics(),
        )
    })
    .await;
    let (process, cgroup, rocksdb) = match joined {
        Ok(sample) => sample,
        Err(error) => {
            record_collection_failure(failure_logs, "sampler", 1, &anyhow!(error));
            return;
        }
    };

    match process {
        Ok(process) => emit_process_metrics(process),
        Err(error) => {
            tracing::debug!(gauge.anvil_process_memory_metrics_available = 0_u64);
            record_collection_failure(failure_logs, "process", 1, &error);
        }
    }
    match cgroup {
        Ok(Some(cgroup)) => emit_cgroup_metrics(cgroup),
        Ok(None) => tracing::debug!(
            gauge.anvil_cgroup_memory_metrics_available = 0_u64,
            "cgroup memory metrics are unavailable"
        ),
        Err(error) => {
            tracing::debug!(gauge.anvil_cgroup_memory_metrics_available = 0_u64);
            record_collection_failure(failure_logs, "cgroup", 1, &error);
        }
    }
    emit_rocksdb_metrics(&rocksdb);
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
        gauge.anvil_process_memory_metrics_available = 1_u64,
        gauge.anvil_process_resident_memory_bytes = process.resident_bytes,
        gauge.anvil_process_virtual_memory_bytes = process.virtual_bytes,
        gauge.anvil_process_threads = process.threads,
        "sampled process resources"
    );
}

fn emit_cgroup_metrics(cgroup: CgroupMemory) {
    tracing::debug!(
        gauge.anvil_cgroup_memory_metrics_available = 1_u64,
        gauge.anvil_cgroup_memory_current_bytes = cgroup.current_bytes,
        gauge.anvil_cgroup_memory_limit_bytes = cgroup.limit_bytes.unwrap_or(0),
        gauge.anvil_cgroup_memory_limited = u64::from(cgroup.limit_bytes.is_some()),
        gauge.anvil_cgroup_memory_peak_bytes = cgroup.peak_bytes.unwrap_or(0),
        gauge.anvil_cgroup_memory_low_events = cgroup.events.low,
        gauge.anvil_cgroup_memory_high_events = cgroup.events.high,
        gauge.anvil_cgroup_memory_max_events = cgroup.events.max,
        gauge.anvil_cgroup_memory_oom_events = cgroup.events.oom,
        gauge.anvil_cgroup_memory_oom_kill_events = cgroup.events.oom_kill,
        gauge.anvil_cgroup_memory_oom_group_kill_events = cgroup.events.oom_group_kill,
        "sampled cgroup memory resources"
    );
}

fn emit_rocksdb_metrics(rocksdb: &MetadataRuntimeMetrics) {
    tracing::debug!(
        gauge.anvil_rocksdb_block_cache_capacity_bytes = rocksdb.block_cache_capacity_bytes,
        gauge.anvil_rocksdb_block_cache_usage_bytes = rocksdb.block_cache_usage_bytes,
        gauge.anvil_rocksdb_block_cache_pinned_bytes = rocksdb.block_cache_pinned_bytes,
        gauge.anvil_rocksdb_write_buffer_capacity_bytes = rocksdb.write_buffer_capacity_bytes,
        gauge.anvil_rocksdb_write_buffer_usage_bytes = rocksdb.write_buffer_usage_bytes,
        gauge.anvil_rocksdb_unavailable_properties = rocksdb.unavailable_properties,
        "sampled RocksDB resources"
    );
    if let Some(value) = rocksdb.active_memtable_bytes {
        tracing::debug!(gauge.anvil_rocksdb_active_memtable_bytes = value);
    }
    if let Some(value) = rocksdb.all_memtable_bytes {
        tracing::debug!(gauge.anvil_rocksdb_all_memtable_bytes = value);
    }
    if let Some(value) = rocksdb.table_reader_bytes {
        tracing::debug!(gauge.anvil_rocksdb_table_reader_bytes = value);
    }
    if let Some(value) = rocksdb.pending_compaction_bytes {
        tracing::debug!(gauge.anvil_rocksdb_pending_compaction_bytes = value);
    }
    if let Some(value) = rocksdb.immutable_memtables {
        tracing::debug!(gauge.anvil_rocksdb_immutable_memtables = value);
    }
    if let Some(value) = rocksdb.running_compactions {
        tracing::debug!(gauge.anvil_rocksdb_running_compactions = value);
    }
    if let Some(value) = rocksdb.running_flushes {
        tracing::debug!(gauge.anvil_rocksdb_running_flushes = value);
    }
    if let Some(value) = rocksdb.compaction_pending_column_families {
        tracing::debug!(gauge.anvil_rocksdb_compaction_pending_column_families = value);
    }
    if let Some(value) = rocksdb.flush_pending_column_families {
        tracing::debug!(gauge.anvil_rocksdb_flush_pending_column_families = value);
    }
    if let Some(value) = rocksdb.actual_delayed_write_rate_bytes_per_second {
        tracing::debug!(gauge.anvil_rocksdb_actual_delayed_write_rate_bytes_per_second = value);
    }
    if let Some(value) = rocksdb.write_stopped {
        tracing::debug!(gauge.anvil_rocksdb_write_stopped = value);
    }
    if let Some(value) = rocksdb.background_errors {
        tracing::debug!(gauge.anvil_rocksdb_background_errors = value);
    }
    if rocksdb.write_stopped.is_some()
        || rocksdb.actual_delayed_write_rate_bytes_per_second.is_some()
    {
        tracing::debug!(
            gauge.anvil_rocksdb_write_stalled = u64::from(
                rocksdb.write_stopped.is_some_and(|value| value != 0)
                    || rocksdb
                        .actual_delayed_write_rate_bytes_per_second
                        .is_some_and(|value| value != 0)
            )
        );
    }
}

fn record_collection_failure(
    failure_logs: &mut FailureLogs,
    component: &'static str,
    failures: u64,
    error: &impl std::fmt::Display,
) {
    tracing::debug!(
        monotonic_counter.anvil_runtime_metrics_collection_failures_total = failures,
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
    fn parses_linux_process_memory_without_exporting_names() {
        let parsed = parse_process_status(
            "Name:\tanvil-server\nVmSize:\t1234 kB\nVmRSS:\t321 kB\nThreads:\t17\n",
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
