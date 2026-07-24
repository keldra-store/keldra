use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rocksdb::perf::{PerfContext, PerfMetric};
use serde::Serialize;

use crate::config::{GateManifest, ProfileSpec};

pub const REPORT_SCHEMA: &str = "anvil.perf.coremeta_gate_report.v1";

#[derive(Debug, Clone, Default, Serialize)]
pub struct WorkCounters {
    pub user_key_comparisons: u64,
    pub block_cache_hits: u64,
    pub block_reads: u64,
    pub block_read_bytes: u64,
    pub get_read_bytes: u64,
    pub get_from_memtable_count: u64,
    pub iterator_read_bytes: u64,
    pub internal_keys_skipped: u64,
    pub memtable_seeks: u64,
    pub memtable_nexts: u64,
    pub child_seeks: u64,
    pub write_wal_nanos: u64,
    pub write_memtable_nanos: u64,
    pub db_mutex_wait_nanos: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioEvidence {
    pub name: String,
    pub dataset_rows: u64,
    pub expected_items_per_operation: usize,
    pub returned_items: u64,
    pub item_count_mismatches: u64,
    pub sample_count: usize,
    pub latency_ns: Vec<u64>,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub throughput_per_second: f64,
    pub work: WorkCounters,
    pub logical_work_per_operation: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct GateEvidence {
    pub name: String,
    pub category: String,
    pub metric: String,
    pub expectation: String,
    pub observed: Option<f64>,
    pub configured_threshold: f64,
    pub effective_threshold: f64,
    pub unit: String,
    pub pass: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MachineEvidence {
    pub machine_class: String,
    pub operating_system: String,
    pub architecture: String,
    pub logical_cpus: usize,
    pub hostname: String,
}

#[derive(Debug, Serialize)]
pub struct GateReport {
    pub schema: String,
    pub pass: bool,
    pub profile: String,
    pub dataset_id: String,
    pub seed: String,
    pub gate_manifest_hash: String,
    pub gate_manifest_sha256: String,
    pub profile_spec: ProfileSpec,
    pub git_commit: String,
    pub started_at_unix_ms: u128,
    pub elapsed_ms: u128,
    pub work_counter_source: String,
    pub machine: MachineEvidence,
    pub scenarios: Vec<ScenarioEvidence>,
    pub gates: Vec<GateEvidence>,
}

impl WorkCounters {
    pub fn capture(context: &PerfContext) -> Self {
        Self {
            user_key_comparisons: context.metric(PerfMetric::UserKeyComparisonCount),
            block_cache_hits: context.metric(PerfMetric::BlockCacheHitCount),
            block_reads: context.metric(PerfMetric::BlockReadCount),
            block_read_bytes: context.metric(PerfMetric::BlockReadByte),
            get_read_bytes: context.metric(PerfMetric::GetReadBytes),
            get_from_memtable_count: context.metric(PerfMetric::GetFromMemtableCount),
            iterator_read_bytes: context.metric(PerfMetric::IterReadBytes),
            internal_keys_skipped: context.metric(PerfMetric::InternalKeySkippedCount),
            memtable_seeks: context.metric(PerfMetric::SeekOnMemtableCount),
            memtable_nexts: context.metric(PerfMetric::NextOnMemtableCount),
            child_seeks: context.metric(PerfMetric::SeekChildSeekCount),
            write_wal_nanos: context.metric(PerfMetric::WriteWalTime),
            write_memtable_nanos: context.metric(PerfMetric::WriteMemtableTime),
            db_mutex_wait_nanos: context.metric(PerfMetric::DbMutexLockNanos),
        }
    }

    pub fn logical_work(&self) -> u64 {
        self.user_key_comparisons
            .saturating_add(self.block_reads)
            .saturating_add(self.get_from_memtable_count)
            .saturating_add(self.internal_keys_skipped)
            .saturating_add(self.memtable_seeks)
            .saturating_add(self.memtable_nexts)
            .saturating_add(self.child_seeks)
    }
}

impl ScenarioEvidence {
    pub fn from_samples(
        name: impl Into<String>,
        dataset_rows: u64,
        expected_items_per_operation: usize,
        returned_items: u64,
        item_count_mismatches: u64,
        latency_ns: Vec<u64>,
        work: WorkCounters,
    ) -> Self {
        let sample_count = latency_ns.len();
        let total_ns = latency_ns
            .iter()
            .map(|value| u128::from(*value))
            .sum::<u128>();
        let logical_work_per_operation = if sample_count == 0 {
            0.0
        } else {
            work.logical_work() as f64 / sample_count as f64
        };
        let throughput_per_second = if total_ns == 0 {
            0.0
        } else {
            sample_count as f64 / (total_ns as f64 / 1_000_000_000.0)
        };
        Self {
            name: name.into(),
            dataset_rows,
            expected_items_per_operation,
            returned_items,
            item_count_mismatches,
            sample_count,
            p50_ms: percentile_ms(&latency_ns, 50),
            p95_ms: percentile_ms(&latency_ns, 95),
            p99_ms: percentile_ms(&latency_ns, 99),
            throughput_per_second,
            latency_ns,
            work,
            logical_work_per_operation,
        }
    }
}

impl GateReport {
    pub fn new(
        profile: impl Into<String>,
        manifest: &GateManifest,
        manifest_hash: impl Into<String>,
        manifest_sha256: impl Into<String>,
        profile_spec: ProfileSpec,
        started_at: SystemTime,
        elapsed: Duration,
        scenarios: Vec<ScenarioEvidence>,
        gates: Vec<GateEvidence>,
    ) -> Self {
        let pass = gates.iter().all(|gate| gate.pass);
        Self {
            schema: REPORT_SCHEMA.to_string(),
            pass,
            profile: profile.into(),
            dataset_id: manifest.dataset_id.clone(),
            seed: manifest.seed.clone(),
            gate_manifest_hash: manifest_hash.into(),
            gate_manifest_sha256: manifest_sha256.into(),
            profile_spec,
            git_commit: git_commit(),
            started_at_unix_ms: started_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            elapsed_ms: elapsed.as_millis(),
            work_counter_source: "RocksDB PerfContext EnableCount".to_string(),
            machine: machine_evidence(),
            scenarios,
            gates,
        }
    }

    pub fn write(&self, output_path: &Path, manifest_bytes: &[u8]) -> Result<()> {
        let parent = output_path
            .parent()
            .context("CoreMeta report output has no parent directory")?;
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        atomic_write_json(output_path, self)?;
        atomic_write(&parent.join("gate-manifest.json"), manifest_bytes)?;
        Ok(())
    }
}

fn percentile_ms(samples_ns: &[u64], percentile: usize) -> f64 {
    if samples_ns.is_empty() {
        return 0.0;
    }
    let mut ordered = samples_ns.to_vec();
    ordered.sort_unstable();
    let rank = ((ordered.len() * percentile).saturating_add(99) / 100).max(1);
    ordered[rank.min(ordered.len()) - 1] as f64 / 1_000_000.0
}

fn atomic_write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).context("encode performance evidence")?;
    atomic_write(path, &bytes)
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&temporary, bytes).with_context(|| format!("write {}", temporary.display()))?;
    std::fs::rename(&temporary, path).with_context(|| format!("publish {}", path.display()))
}

fn git_commit() -> String {
    std::env::var("GITHUB_SHA")
        .or_else(|_| std::env::var("ANVIL_GIT_COMMIT"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "local-unset".to_string())
}

fn machine_evidence() -> MachineEvidence {
    MachineEvidence {
        machine_class: std::env::var("ANVIL_MACHINE_CLASS")
            .unwrap_or_else(|_| "unclassified".to_string()),
        operating_system: std::env::consts::OS.to_string(),
        architecture: std::env::consts::ARCH.to_string(),
        logical_cpus: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        hostname: std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "unknown".to_string()),
    }
}
