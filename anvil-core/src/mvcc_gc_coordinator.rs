//! Leader-owned cluster garbage-collection watermark coordination.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anvil_mvcc_consensus::{
    CommitVersion, Consensus as _, GarbageCollectionPins, NodeIncarnation, OpenRaftConsensus,
};
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::{
    mvcc_gc::{advance_garbage_collection_watermark, plan_garbage_collection},
    mvcc_open_transactions::OpenTransactionRegistry,
    mvcc_store::LocalMvccStore,
    services::consensus_transport::{AppliedWatermarkReports, LocalGcSafetyReport},
};

pub struct MvccGarbageCollectionCoordinator {
    cluster_id: String,
    local_raft_node_id: anvil_mvcc_consensus::NodeId,
    consensus: Arc<OpenRaftConsensus>,
    open_transactions: Arc<OpenTransactionRegistry>,
    local: LocalMvccStore,
    reports: AppliedWatermarkReports,
    local_report: LocalGcSafetyReport,
    interval: Duration,
}

impl MvccGarbageCollectionCoordinator {
    pub fn new(
        cluster_id: impl Into<String>,
        local_raft_node_id: anvil_mvcc_consensus::NodeId,
        consensus: Arc<OpenRaftConsensus>,
        open_transactions: Arc<OpenTransactionRegistry>,
        local: LocalMvccStore,
        reports: AppliedWatermarkReports,
        local_report: LocalGcSafetyReport,
        interval: Duration,
    ) -> Result<Self> {
        if interval.is_zero() {
            bail!("MVCC GC coordinator interval must be non-zero");
        }
        Ok(Self {
            cluster_id: cluster_id.into(),
            local_raft_node_id,
            consensus,
            open_transactions,
            local,
            reports,
            local_report,
            interval,
        })
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            if let Err(error) = self.run_once().await {
                tracing::debug!(
                    operation = "gc.coordinate",
                    error = %error,
                    "MVCC GC coordination deferred"
                );
            }
            tokio::select! {
                _ = shutdown.changed() => {}
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
    }

    pub async fn run_once(&self) -> Result<Option<u64>> {
        let now = unix_time_ms()?;
        let local_snapshot_pin = self
            .open_transactions
            .active_snapshot_pins(now)?
            .into_iter()
            .next();
        let local_work_pin = self.local.unfinished_work_pins()?.all().into_iter().next();
        self.local_report.update(
            self.local.readable_version()?,
            local_snapshot_pin,
            local_work_pin,
        );
        if !self.consensus.is_leader() {
            return Ok(None);
        }
        let control = self.consensus.applied_control_snapshot()?;
        let reports = self.reports.snapshot();
        let mut replica_applied_watermarks = BTreeMap::new();
        let mut active_snapshots = std::collections::BTreeSet::new();
        let mut unfinished_work_pins = std::collections::BTreeSet::new();
        for (control_node_id, raft_node_id, incarnation, _) in &control.nodes {
            let report = if *raft_node_id == self.local_raft_node_id {
                let mut report = self.local_report.snapshot();
                report.incarnation = *incarnation;
                report
            } else {
                let report = reports
                    .get(raft_node_id)
                    .with_context(|| {
                        format!(
                            "node {} has not reported MVCC apply progress",
                            raft_node_id.0
                        )
                    })?;
                if report.incarnation != *incarnation {
                    bail!(
                        "node {} reported incarnation {}, installed incarnation is {}",
                        raft_node_id.0,
                        report.incarnation,
                        incarnation
                    );
                }
                *report
            };
            if let Some(pin) = report.oldest_active_snapshot {
                active_snapshots.insert(CommitVersion(pin));
            }
            if let Some(pin) = report.oldest_unfinished_work {
                unfinished_work_pins.insert(CommitVersion(pin));
            }
            replica_applied_watermarks.insert(
                NodeIncarnation {
                    node_id: *control_node_id,
                    incarnation: *incarnation,
                },
                CommitVersion(report.watermark),
            );
        }
        let current = self.local.gc_watermark()?;
        let head = self.consensus.observed_commit_version().0;
        let proposal = plan_garbage_collection(
            &self.open_transactions,
            &self.local,
            now,
            current,
            head,
            head,
            GarbageCollectionPins {
                replica_applied_watermarks,
                active_snapshots,
                unfinished_work_pins,
                ..Default::default()
            },
        )?;
        if proposal.watermark == current {
            return Ok(Some(current));
        }
        let started = std::time::Instant::now();
        let watermark = advance_garbage_collection_watermark(
            &self.consensus,
            cluster_id_hash(&self.cluster_id),
            &proposal,
        )
        .await?;
        crate::perf::record_gc_coordination("advanced", started.elapsed());
        tracing::info!(
            operation = "gc.coordinate",
            watermark,
            replica_count = proposal.pins.replica_applied_watermarks.len(),
            "advanced cluster MVCC garbage-collection watermark"
        );
        Ok(Some(watermark))
    }
}

fn cluster_id_hash(cluster_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let domain = b"anvil.mvcc.cluster-id.v1";
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    hasher.update((cluster_id.len() as u64).to_be_bytes());
    hasher.update(cluster_id.as_bytes());
    hasher.finalize().into()
}

fn unix_time_ms() -> Result<u64> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis()
        .try_into()
        .context("system time exceeds u64 milliseconds")?)
}
