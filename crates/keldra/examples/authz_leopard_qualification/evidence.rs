use std::fs;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

/// Exact before/after deltas collected by the qualification wrapper from
/// Keldra's OTLP counters and the host process/device sampler. These values are
/// deliberately not inferred from client behavior: missing instrumentation is
/// missing evidence and fails a normal qualification run.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(super) struct ResourceEvidence {
    pub schema: String,
    pub measurement_seconds: f64,
    pub process_cpu_percent: f64,
    pub process_peak_resident_memory_bytes: u64,
    pub storage_read_iops: f64,
    pub storage_write_iops: f64,
    pub storage_read_mebibytes_per_second: f64,
    pub storage_write_mebibytes_per_second: f64,
    pub storage_busy_percent: f64,
    pub leopard_visited_usersets: u64,
    pub leopard_loaded_direct_edges: u64,
    pub leopard_db_prefix_reads: u64,
    pub leopard_forward_prefix_reads: u64,
    pub leopard_forward_edges_loaded: u64,
    pub leopard_reverse_prefix_reads: u64,
    pub leopard_reverse_edges_loaded: u64,
    pub leopard_cache_hits: u64,
    pub leopard_cache_misses: u64,
    pub leopard_adjacency_cache_hits: u64,
    pub leopard_adjacency_cache_misses: u64,
    pub leopard_evaluation_steps: u64,
    pub leopard_limit_failures: u64,
}

impl ResourceEvidence {
    pub(super) fn load(path: Option<&Path>, required: bool) -> Result<Option<Self>> {
        let Some(path) = path else {
            ensure!(
                !required,
                "Leopard internal/resource telemetry evidence path is required"
            );
            return Ok(None);
        };
        let bytes = fs::read(path)
            .with_context(|| format!("read Leopard telemetry evidence {}", path.display()))?;
        let value: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode Leopard telemetry evidence {}", path.display()))?;
        value.validate()?;
        Ok(Some(value))
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == "keldra.authz-leopard-resource-evidence.v1",
            "wrong Leopard resource evidence schema"
        );
        ensure!(
            self.measurement_seconds > 0.0,
            "telemetry measurement window is empty"
        );
        ensure!(
            self.leopard_visited_usersets > 0,
            "telemetry observed no visited usersets"
        );
        ensure!(
            self.leopard_loaded_direct_edges > 0,
            "telemetry observed no loaded direct edges"
        );
        ensure!(
            self.leopard_db_prefix_reads > 0,
            "telemetry observed no DB prefix reads"
        );
        ensure!(
            self.leopard_reverse_prefix_reads > 0 && self.leopard_reverse_edges_loaded > 0,
            "telemetry did not prove reverse-ancestor userset evaluation"
        );
        ensure!(
            self.leopard_adjacency_cache_hits > 0,
            "telemetry observed no per-batch adjacency-cache reuse"
        );
        ensure!(
            self.leopard_adjacency_cache_misses > 0,
            "telemetry observed no persistent adjacency reads"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_rejects_missing_leopard_activity() {
        assert!(ResourceEvidence::default().validate().is_err());
        let complete = ResourceEvidence {
            schema: "keldra.authz-leopard-resource-evidence.v1".into(),
            measurement_seconds: 1.0,
            leopard_visited_usersets: 1,
            leopard_loaded_direct_edges: 1,
            leopard_db_prefix_reads: 1,
            leopard_reverse_prefix_reads: 1,
            leopard_reverse_edges_loaded: 1,
            leopard_adjacency_cache_hits: 1,
            leopard_adjacency_cache_misses: 1,
            ..ResourceEvidence::default()
        };
        assert!(complete.validate().is_ok());
    }
}
