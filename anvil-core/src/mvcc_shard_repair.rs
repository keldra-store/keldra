use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anvil_mvcc_consensus::Consensus;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::{
    mvcc_transaction::CommitVersion,
    object_shard_manifest::{PhysicalObjectShardManifest, PhysicalShardPlacement},
    shard_placement::{ShardPlacementPolicy, ShardTarget},
    streaming_erasure::{EncodedShard, ErasureProfile, ShardSink, StreamingErasureEncoder},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardRepairState {
    Pending,
    Running,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingShardTarget {
    pub stripe_ordinal: u64,
    pub shard_ordinal: u16,
    pub target: ShardTarget,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardMaintenanceKind {
    #[default]
    Repair,
    Rebalance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRepairJob {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    #[serde(default)]
    pub kind: ShardMaintenanceKind,
    pub target_logical_identity: String,
    pub source_manifest: PhysicalObjectShardManifest,
    pub source_manifest_hash: String,
    pub missing: Vec<MissingShardTarget>,
    pub retiring: Vec<PhysicalShardPlacement>,
    pub originating_snapshot_version: CommitVersion,
    pub requested_at_unix_ms: u64,
}

/// Detect placement/failure-domain drift in an already snapshot-resolved
/// manifest and build one canonical durable rebalance job.
///
/// The caller persists `canonical_bytes()` as a materialisation job in the
/// transaction that observed `originating_snapshot_version`. Identical inputs
/// produce an identical job ID, so registry insertion is naturally deduplicated.
pub fn plan_rebalance_job(
    manifest: &PhysicalObjectShardManifest,
    transaction_id: impl Into<String>,
    candidates: &[ShardTarget],
    policy: ShardPlacementPolicy,
    originating_snapshot_version: CommitVersion,
    requested_at_unix_ms: u64,
) -> Result<Option<ShardRepairJob>> {
    manifest.validate()?;
    let profile = ErasureProfile {
        data_shards: usize::from(manifest.data_shards),
        parity_shards: usize::from(manifest.parity_shards),
        shard_bytes: usize::try_from(manifest.shard_bytes)?,
    };
    let desired = policy.plan(
        manifest.object_identity,
        manifest.encoding_generation,
        profile,
        candidates,
    )?;
    let mut missing = Vec::new();
    let mut retiring = Vec::new();
    for stripe_ordinal in 0..manifest.stripe_count {
        for (ordinal, target) in desired.targets_by_ordinal.iter().enumerate() {
            let shard_ordinal = u16::try_from(ordinal)?;
            let current = manifest.placements.iter().find(|placement| {
                placement.stripe_ordinal == stripe_ordinal
                    && placement.shard_ordinal == shard_ordinal
            });
            let already_optimal = current.is_some_and(|placement| {
                placement.node_id == target.node.node_id
                    && placement.node_incarnation == target.node.incarnation
                    && placement.failure_domain == target.failure_domain
            });
            if already_optimal {
                continue;
            }
            missing.push(MissingShardTarget {
                stripe_ordinal,
                shard_ordinal,
                target: target.clone(),
            });
            if let Some(current) = current {
                retiring.push(current.clone());
            }
        }
    }
    if missing.is_empty() {
        return Ok(None);
    }
    missing.sort_by_key(|entry| (entry.stripe_ordinal, entry.shard_ordinal));
    retiring.sort_by_key(|entry| (entry.stripe_ordinal, entry.shard_ordinal));
    let target_logical_identity =
        String::from_utf8(placement_overlay_key(manifest).application_key)?;
    let job = ShardRepairJob {
        schema: ShardRepairJob::SCHEMA.to_string(),
        cluster_id: manifest.cluster_id.clone(),
        transaction_id: transaction_id.into(),
        kind: ShardMaintenanceKind::Rebalance,
        target_logical_identity,
        source_manifest: manifest.clone(),
        source_manifest_hash: hex::encode(blake3::hash(&manifest.canonical_bytes()?)),
        missing,
        retiring,
        originating_snapshot_version,
        requested_at_unix_ms,
    };
    job.validate()?;
    Ok(Some(job))
}

/// Snapshot-resolve, detect drift, and durably stage a deduplicated rebalance
/// job in an already-open MVCC transaction.
pub fn stage_rebalance_if_drift(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    source_manifest: &PhysicalObjectShardManifest,
    candidates: &[ShardTarget],
    policy: ShardPlacementPolicy,
    now_unix_ms: u64,
) -> Result<bool> {
    let binding = mvcc.open_transactions.binding(transaction_id, principal)?;
    if binding.cluster_id != source_manifest.cluster_id {
        bail!("rebalance source manifest belongs to another cluster");
    }
    let resolved = resolve_manifest_at_snapshot(
        mvcc.runtime.local_store(),
        source_manifest,
        binding.snapshot_version,
    )?;
    let Some(mut job) = plan_rebalance_job(
        &resolved,
        transaction_id,
        candidates,
        policy,
        binding.snapshot_version,
        now_unix_ms,
    )?
    else {
        return Ok(false);
    };
    job.source_manifest_hash = hex::encode(blake3::hash(&source_manifest.canonical_bytes()?));
    mvcc.open_transactions.add_job(
        transaction_id,
        &binding.cluster_id,
        job.canonical_bytes()?,
        now_unix_ms,
    )?;
    Ok(true)
}

impl ShardRepairJob {
    pub const SCHEMA: &'static str = "anvil.mvcc.shard-repair-job.v1";

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let job: Self = serde_json::from_slice(bytes)?;
        if job.canonical_bytes()? != bytes {
            bail!("shard repair job is not canonically encoded");
        }
        Ok(job)
    }

    pub fn job_id(&self) -> Result<String> {
        Ok(hex::encode(blake3::hash(&self.canonical_bytes()?)))
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema != Self::SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self.target_logical_identity.trim().is_empty()
            || self.source_manifest_hash.len() != 64
            || !self
                .source_manifest_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || self.missing.is_empty()
            || self.requested_at_unix_ms == 0
        {
            bail!("invalid shard repair job");
        }
        self.source_manifest.validate()?;
        let total = usize::from(
            self.source_manifest
                .data_shards
                .checked_add(self.source_manifest.parity_shards)
                .context("repair shard count overflow")?,
        );
        let mut identities = std::collections::BTreeSet::new();
        for missing in &self.missing {
            if missing.stripe_ordinal >= self.source_manifest.stripe_count
                || usize::from(missing.shard_ordinal) >= total
                || missing.target.cluster_id != self.cluster_id
                || !identities.insert((missing.stripe_ordinal, missing.shard_ordinal))
            {
                bail!("invalid or duplicate missing shard target");
            }
        }
        if self.missing.windows(2).any(|pair| {
            (pair[0].stripe_ordinal, pair[0].shard_ordinal)
                >= (pair[1].stripe_ordinal, pair[1].shard_ordinal)
        }) {
            bail!("shard maintenance targets must be canonically sorted");
        }
        let mut retired = std::collections::BTreeSet::new();
        for placement in &self.retiring {
            let identity = (placement.stripe_ordinal, placement.shard_ordinal);
            if !identities.contains(&identity) || !retired.insert(identity) {
                bail!("retiring placement must correspond to one replacement target");
            }
            let replacement = self
                .missing
                .iter()
                .find(|missing| (missing.stripe_ordinal, missing.shard_ordinal) == identity)
                .expect("checked replacement identity");
            if placement.node_id == replacement.target.node.node_id
                && placement.node_incarnation == replacement.target.node.incarnation
                && placement.failure_domain == replacement.target.failure_domain
            {
                bail!("replacement target must differ from retiring placement");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRepairRecord {
    pub job: ShardRepairJob,
    pub state: ShardRepairState,
    pub attempts: u32,
    pub next_attempt_unix_ms: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl ShardRepairRecord {
    pub fn pending(job: ShardRepairJob) -> Self {
        Self {
            next_attempt_unix_ms: job.requested_at_unix_ms,
            job,
            state: ShardRepairState::Pending,
            attempts: 0,
            lease_owner: None,
            lease_expires_unix_ms: None,
            last_error: None,
        }
    }

    pub fn claimable(&self, now_unix_ms: u64) -> bool {
        match self.state {
            ShardRepairState::Pending => self.next_attempt_unix_ms <= now_unix_ms,
            ShardRepairState::Running => self
                .lease_expires_unix_ms
                .is_some_and(|expiry| expiry <= now_unix_ms),
            ShardRepairState::Complete => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardPlacementOverlay {
    pub schema: String,
    pub cluster_id: String,
    pub target_logical_identity: String,
    pub source_manifest_hash: String,
    pub replacement_manifest: PhysicalObjectShardManifest,
    pub retired_after_commit: Vec<PhysicalShardPlacement>,
}

impl ShardPlacementOverlay {
    pub const SCHEMA: &'static str = "anvil.mvcc.shard-placement-overlay.v1";
    pub const TABLE_ID: u16 = 0x7f12;
}

pub const SHARD_MANIFEST_CATALOG_TABLE_ID: u16 = 0x7f13;
const SHARD_REBALANCE_CHECKPOINT_TABLE_ID: u16 = 0x7f14;
const REBALANCE_PAGE_SIZE: usize = 64;

pub fn manifest_catalog_key(
    manifest: &PhysicalObjectShardManifest,
) -> crate::mvcc_transaction::LogicalKey {
    crate::mvcc_transaction::LogicalKey {
        table_id: SHARD_MANIFEST_CATALOG_TABLE_ID,
        application_key: format!("manifest/{}", manifest.object_hash).into_bytes(),
    }
}

pub fn stage_manifest_catalog_entry(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    manifest: &PhysicalObjectShardManifest,
    now_unix_ms: u64,
) -> Result<()> {
    let binding = mvcc.open_transactions.binding(transaction_id, principal)?;
    if binding.cluster_id != manifest.cluster_id {
        bail!("manifest catalog entry belongs to another cluster");
    }
    mvcc.open_transactions.put(
        transaction_id,
        &binding.cluster_id,
        manifest_catalog_key(manifest),
        manifest.canonical_bytes()?,
        now_unix_ms,
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RebalanceCheckpoint {
    schema: String,
    topology_epoch: [u8; 32],
    snapshot_version: CommitVersion,
    after_application_key: Option<Vec<u8>>,
}

impl RebalanceCheckpoint {
    const SCHEMA: &'static str = "anvil.mvcc.shard-rebalance-checkpoint.v1";
}

pub struct ShardRebalanceReconciler {
    mvcc: Arc<crate::mvcc_bootstrap::MvccSubsystem>,
    worker_id: String,
}

impl ShardRebalanceReconciler {
    pub fn new(
        mvcc: Arc<crate::mvcc_bootstrap::MvccSubsystem>,
        worker_id: impl Into<String>,
    ) -> Result<Self> {
        let worker_id = worker_id.into();
        if worker_id.trim().is_empty() {
            bail!("shard rebalance reconciler worker ID is required");
        }
        Ok(Self { mvcc, worker_id })
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            if let Err(error) = self.run_once(now_unix_ms()).await {
                tracing::warn!(%error, worker_id = %self.worker_id, "shard rebalance reconciliation failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    pub async fn run_once(&self, now: u64) -> Result<bool> {
        if !self.mvcc.consensus.is_leader() {
            return Ok(false);
        }
        let epoch = topology_epoch(
            &self.mvcc.shard_candidates,
            self.mvcc.tolerated_failure_domains,
        )?;
        let checkpoint_key = crate::mvcc_transaction::LogicalKey {
            table_id: SHARD_REBALANCE_CHECKPOINT_TABLE_ID,
            application_key: b"reconciler/checkpoint".to_vec(),
        };
        let latest = self
            .mvcc
            .runtime
            .local_store()
            .read_latest(&checkpoint_key)?;
        let checkpoint = latest
            .as_ref()
            .map(|row| serde_json::from_slice::<RebalanceCheckpoint>(&row.value))
            .transpose()?
            .filter(|checkpoint| {
                checkpoint.schema == RebalanceCheckpoint::SCHEMA
                    && checkpoint.topology_epoch == epoch
            });
        let mut checkpoint = match checkpoint {
            Some(checkpoint) => checkpoint,
            None => RebalanceCheckpoint {
                schema: RebalanceCheckpoint::SCHEMA.to_string(),
                topology_epoch: epoch,
                snapshot_version: self
                    .mvcc
                    .runtime
                    .snapshot(crate::mvcc_transaction::ReadConsistency::Linearized)
                    .await?,
                after_application_key: None,
            },
        };
        if checkpoint.snapshot_version < self.mvcc.runtime.local_store().gc_watermark()? {
            checkpoint.snapshot_version = self
                .mvcc
                .runtime
                .snapshot(crate::mvcc_transaction::ReadConsistency::Linearized)
                .await?;
            checkpoint.after_application_key = None;
        }
        let rows = self.mvcc.runtime.scan_table_prefix_at(
            SHARD_MANIFEST_CATALOG_TABLE_ID,
            b"manifest/",
            checkpoint.snapshot_version,
        )?;
        let page = rows
            .into_iter()
            .filter(|(key, _)| {
                checkpoint
                    .after_application_key
                    .as_ref()
                    .is_none_or(|after| key.application_key > *after)
            })
            .take(REBALANCE_PAGE_SIZE)
            .collect::<Vec<_>>();
        let principal = format!("shard-rebalance/{}", self.worker_id);
        let cursor_hash = blake3::hash(
            checkpoint
                .after_application_key
                .as_deref()
                .unwrap_or_default(),
        );
        let handle = self
            .mvcc
            .open_transactions
            .begin(
                self.mvcc.runtime.as_ref(),
                self.mvcc.cluster_id().to_string(),
                &principal,
                format!(
                    "rebalance/{}/{}/{}",
                    hex::encode(epoch),
                    checkpoint.snapshot_version,
                    hex::encode(cursor_hash.as_bytes())
                ),
                Duration::from_secs(30),
                crate::mvcc_transaction::DurabilityLevel::Quorum,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                now,
            )
            .await?;
        for (key, row) in &page {
            let manifest: PhysicalObjectShardManifest = serde_json::from_slice(&row.value)?;
            let resolved = resolve_manifest_at_snapshot(
                self.mvcc.runtime.local_store(),
                &manifest,
                checkpoint.snapshot_version,
            )?;
            if let Some(mut job) = plan_rebalance_job(
                &resolved,
                &handle.transaction_id,
                &self.mvcc.shard_candidates,
                ShardPlacementPolicy {
                    tolerated_failure_domains: self.mvcc.tolerated_failure_domains,
                },
                checkpoint.snapshot_version,
                now,
            )? {
                job.source_manifest_hash = hex::encode(blake3::hash(&manifest.canonical_bytes()?));
                self.mvcc.open_transactions.add_job(
                    &handle.transaction_id,
                    self.mvcc.cluster_id(),
                    job.canonical_bytes()?,
                    now,
                )?;
            }
            checkpoint.after_application_key = Some(key.application_key.clone());
        }
        if page.len() < REBALANCE_PAGE_SIZE {
            checkpoint.snapshot_version = self
                .mvcc
                .runtime
                .snapshot(crate::mvcc_transaction::ReadConsistency::Linearized)
                .await?;
            checkpoint.after_application_key = None;
        }
        self.mvcc.open_transactions.put(
            &handle.transaction_id,
            self.mvcc.cluster_id(),
            checkpoint_key,
            serde_json::to_vec(&checkpoint)?,
            now,
        )?;
        let outcome = self
            .mvcc
            .open_transactions
            .commit(
                self.mvcc.runtime.as_ref(),
                &handle.transaction_id,
                &principal,
                now,
            )
            .await?;
        if !matches!(
            outcome.certification,
            crate::mvcc_transaction::CertificationResult::Committed { .. }
        ) {
            bail!("rebalance page and checkpoint transaction was not committed");
        }
        Ok(!page.is_empty())
    }
}

pub fn placement_overlay_key(
    manifest: &PhysicalObjectShardManifest,
) -> crate::mvcc_transaction::LogicalKey {
    crate::mvcc_transaction::LogicalKey {
        table_id: ShardPlacementOverlay::TABLE_ID,
        application_key: format!(
            "cluster/{}/object/{}",
            manifest.cluster_id, manifest.object_hash
        )
        .into_bytes(),
    }
}

/// Resolves a placement overlay using only versions visible at `snapshot`.
/// Callers performing historical or transactional reads must pass their
/// captured read snapshot rather than consulting the latest overlay.
pub fn resolve_manifest_at_snapshot(
    store: &crate::mvcc_store::LocalMvccStore,
    source: &PhysicalObjectShardManifest,
    snapshot: CommitVersion,
) -> Result<PhysicalObjectShardManifest> {
    let key = placement_overlay_key(source);
    let Some(row) = store.read_at(&key, snapshot)? else {
        return Ok(source.clone());
    };
    let overlay: ShardPlacementOverlay = serde_json::from_slice(&row.value)?;
    let source_hash = hex::encode(blake3::hash(&source.canonical_bytes()?));
    if overlay.cluster_id != source.cluster_id
        || overlay.target_logical_identity != String::from_utf8_lossy(&key.application_key)
        || overlay.source_manifest_hash != source_hash
    {
        bail!("shard placement overlay does not match its source manifest");
    }
    overlay.replacement_manifest.validate()?;
    Ok(overlay.replacement_manifest)
}

pub struct ShardRepairRunner {
    mvcc: Arc<crate::mvcc_bootstrap::MvccSubsystem>,
    worker_id: String,
    lease_ms: u64,
}

impl ShardRepairRunner {
    pub fn new(
        mvcc: Arc<crate::mvcc_bootstrap::MvccSubsystem>,
        worker_id: impl Into<String>,
    ) -> Result<Self> {
        let worker_id = worker_id.into();
        if worker_id.trim().is_empty() {
            bail!("shard repair worker ID is required");
        }
        Ok(Self {
            mvcc,
            worker_id,
            lease_ms: 30_000,
        })
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            if let Err(error) = self.run_once(now_unix_ms()).await {
                tracing::warn!(%error, worker_id = %self.worker_id, "shard repair attempt failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    pub async fn run_once(&self, now: u64) -> Result<bool> {
        self.mvcc.consensus.linearized_read_barrier().await?;
        let store = self.mvcc.runtime.local_store();
        let Some((job_id, record)) =
            store.claim_shard_repair(&self.worker_id, now, self.lease_ms)?
        else {
            return Ok(false);
        };
        let started_at = std::time::Instant::now();
        crate::perf::record_repair_age(
            maintenance_label(record.job.kind),
            Duration::from_millis(now.saturating_sub(record.job.requested_at_unix_ms)),
        );
        tracing::info!(
            operation = "repair.claim",
            job_id = %job_id,
            worker_id = %self.worker_id,
            attempts = record.attempts,
            "claimed durable shard repair job"
        );
        let timeout = Duration::from_millis(self.lease_ms - 1_000);
        match tokio::time::timeout(timeout, self.execute(&record.job)).await {
            Ok(Ok(())) => {
                crate::perf::record_repair_duration(
                    maintenance_label(record.job.kind),
                    "erasure",
                    "complete",
                    started_at.elapsed(),
                );
                store.complete_shard_repair(&job_id, &self.worker_id)?
            }
            Ok(Err(error)) => {
                crate::perf::record_repair_duration(
                    maintenance_label(record.job.kind),
                    "erasure",
                    "retry",
                    started_at.elapsed(),
                );
                store.retry_shard_repair(
                    &job_id,
                    &self.worker_id,
                    retry_at(now, record.attempts),
                    &error.to_string(),
                )?
            }
            Err(_) => {
                crate::perf::record_repair_duration(
                    maintenance_label(record.job.kind),
                    "erasure",
                    "timeout",
                    started_at.elapsed(),
                );
                store.retry_shard_repair(
                    &job_id,
                    &self.worker_id,
                    retry_at(now, record.attempts),
                    "shard repair exceeded lease-safe timeout",
                )?
            }
        }
        Ok(true)
    }

    async fn execute(&self, job: &ShardRepairJob) -> Result<()> {
        tracing::info!(
            operation = "repair.reconstruct",
            transaction_id = %job.transaction_id,
            missing_shards = job.missing.len(),
            "reconstructing immutable object for shard repair"
        );
        let overlay_key = crate::mvcc_transaction::LogicalKey {
            table_id: ShardPlacementOverlay::TABLE_ID,
            application_key: job.target_logical_identity.as_bytes().to_vec(),
        };
        if let Some(row) = self.mvcc.runtime.local_store().read_latest(&overlay_key)? {
            let overlay: ShardPlacementOverlay = serde_json::from_slice(&row.value)?;
            if job.missing.iter().all(|missing| {
                overlay
                    .replacement_manifest
                    .placements
                    .iter()
                    .any(|placement| {
                        placement.stripe_ordinal == missing.stripe_ordinal
                            && placement.shard_ordinal == missing.shard_ordinal
                            && placement.node_id == missing.target.node.node_id
                            && placement.node_incarnation == missing.target.node.incarnation
                    })
            }) {
                return Ok(());
            }
        }
        let payload = Arc::new(std::sync::Mutex::new(Vec::new()));
        job.source_manifest
            .read_range_chunks(
                &self.mvcc.replication_client,
                0,
                job.source_manifest.object_length,
                {
                    let payload = payload.clone();
                    move |chunk| {
                        let payload = payload.clone();
                        async move {
                            payload.lock().unwrap().extend_from_slice(&chunk);
                            Ok(())
                        }
                    }
                },
            )
            .await?;
        let payload = Arc::try_unwrap(payload).unwrap().into_inner().unwrap();
        let targets = job
            .missing
            .iter()
            .map(|missing| {
                (
                    (missing.stripe_ordinal, missing.shard_ordinal),
                    missing.target.clone(),
                )
            })
            .collect();
        let mut sink = RepairSink {
            transport: &self.mvcc.replication_client,
            targets,
            completed: Vec::new(),
        };
        let profile = ErasureProfile {
            data_shards: usize::from(job.source_manifest.data_shards),
            parity_shards: usize::from(job.source_manifest.parity_shards),
            shard_bytes: usize::try_from(job.source_manifest.shard_bytes)?,
        };
        let mut reader = std::io::Cursor::new(payload);
        let encoded = StreamingErasureEncoder::new(profile)?
            .encode(
                &mut reader,
                job.source_manifest.object_identity,
                job.source_manifest.encoding_generation,
                &mut sink,
            )
            .await?;
        let encoded_hash = format!("sha256:{}", hex::encode(encoded.content_hash));
        if encoded_hash != job.source_manifest.object_hash
            || sink.completed.len() != job.missing.len()
        {
            bail!("reconstructed repair payload does not match frozen manifest");
        }
        tracing::info!(
            operation = "repair.place",
            transaction_id = %job.transaction_id,
            placed_shards = sink.completed.len(),
            "replacement shards received verified durable ACKs"
        );
        let mut replacement = job.source_manifest.clone();
        for placement in sink.completed {
            replacement.placements.retain(|current| {
                (current.stripe_ordinal, current.shard_ordinal)
                    != (placement.stripe_ordinal, placement.shard_ordinal)
            });
            replacement.placements.push(placement);
        }
        replacement.placements.sort_by_key(|placement| {
            (
                placement.stripe_ordinal,
                placement.shard_ordinal,
                placement.node_id.clone(),
                placement.node_incarnation,
            )
        });
        replacement.validate()?;
        self.publish_overlay(job, replacement).await
    }

    async fn publish_overlay(
        &self,
        job: &ShardRepairJob,
        mut replacement_manifest: PhysicalObjectShardManifest,
    ) -> Result<()> {
        let principal = format!("shard-repair/{}", self.worker_id);
        let handle = self
            .mvcc
            .open_transactions
            .begin(
                self.mvcc.runtime.as_ref(),
                job.cluster_id.clone(),
                &principal,
                format!("shard-repair/{}", job.job_id()?),
                Duration::from_secs(30),
                crate::mvcc_transaction::DurabilityLevel::Quorum,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                now_unix_ms(),
            )
            .await?;
        let source_manifest_hash = job.source_manifest_hash.clone();
        let overlay_key = crate::mvcc_transaction::LogicalKey {
            table_id: ShardPlacementOverlay::TABLE_ID,
            application_key: job.target_logical_identity.as_bytes().to_vec(),
        };
        let observed = self
            .mvcc
            .runtime
            .local_store()
            .read_at(&overlay_key, handle.snapshot_version)?;
        let mut retired_after_commit = job.retiring.clone();
        if let Some(row) = &observed {
            let current: ShardPlacementOverlay = serde_json::from_slice(&row.value)?;
            if current.cluster_id != job.cluster_id
                || current.target_logical_identity != job.target_logical_identity
                || current.source_manifest_hash != source_manifest_hash
            {
                bail!("existing shard placement overlay has a different source manifest");
            }
            let repaired = job
                .missing
                .iter()
                .map(|missing| (missing.stripe_ordinal, missing.shard_ordinal))
                .collect::<std::collections::BTreeSet<_>>();
            replacement_manifest.placements.retain(|placement| {
                repaired.contains(&(placement.stripe_ordinal, placement.shard_ordinal))
            });
            replacement_manifest
                .placements
                .extend(current.replacement_manifest.placements);
            replacement_manifest.placements.sort_by_key(|placement| {
                (
                    placement.stripe_ordinal,
                    placement.shard_ordinal,
                    placement.node_id.clone(),
                    placement.node_incarnation,
                )
            });
            replacement_manifest
                .placements
                .dedup_by_key(|placement| (placement.stripe_ordinal, placement.shard_ordinal));
            replacement_manifest.validate()?;
            retired_after_commit.extend(current.retired_after_commit);
            retired_after_commit.sort_by_key(|placement| {
                (
                    placement.stripe_ordinal,
                    placement.shard_ordinal,
                    placement.node_id.clone(),
                    placement.node_incarnation,
                )
            });
            retired_after_commit.dedup();
        }
        let overlay = ShardPlacementOverlay {
            schema: ShardPlacementOverlay::SCHEMA.to_string(),
            cluster_id: job.cluster_id.clone(),
            target_logical_identity: job.target_logical_identity.clone(),
            source_manifest_hash,
            replacement_manifest,
            retired_after_commit,
        };
        self.mvcc.open_transactions.observe_point(
            &handle.transaction_id,
            &job.cluster_id,
            overlay_key.clone(),
            observed.as_ref().map(|row| row.commit_version),
            now_unix_ms(),
        )?;
        self.mvcc.open_transactions.put(
            &handle.transaction_id,
            &job.cluster_id,
            overlay_key,
            serde_json::to_vec(&overlay)?,
            now_unix_ms(),
        )?;
        let outcome = self
            .mvcc
            .open_transactions
            .commit(
                self.mvcc.runtime.as_ref(),
                &handle.transaction_id,
                &principal,
                now_unix_ms(),
            )
            .await?;
        if !matches!(
            outcome.certification,
            crate::mvcc_transaction::CertificationResult::Committed { .. }
        ) {
            bail!("replacement placement overlay transaction was not committed");
        }
        tracing::info!(
            operation = "repair.commit",
            transaction_id = %job.transaction_id,
            "committed replacement shard placement overlay"
        );
        Ok(())
    }
}

struct RepairSink<'a> {
    transport: &'a crate::replication_client::TonicReplicationStreamManager,
    targets: BTreeMap<(u64, u16), ShardTarget>,
    completed: Vec<PhysicalShardPlacement>,
}

#[async_trait]
impl ShardSink for RepairSink<'_> {
    async fn send(&mut self, shard: EncodedShard<'_>) -> Result<()> {
        let Some(target) = self
            .targets
            .get(&(shard.stripe_ordinal, shard.shard_ordinal))
        else {
            return Ok(());
        };
        let ack =
            crate::shard_placement::ShardTargetStream::send(self.transport, target, &shard).await?;
        if ack.status != crate::replication::AckStatus::Complete
            || ack.completed_hash != Some(shard.payload_hash)
        {
            bail!("replacement shard did not receive a verified Complete ACK");
        }
        self.completed.push(PhysicalShardPlacement {
            stripe_ordinal: shard.stripe_ordinal,
            shard_ordinal: shard.shard_ordinal,
            payload_length: shard.payload.len() as u64,
            payload_hash: shard.payload_hash,
            transfer_id: ack.transfer_id,
            node_id: target.node.node_id.clone(),
            node_incarnation: target.node.incarnation,
            failure_domain: target.failure_domain.clone(),
        });
        Ok(())
    }
}

fn retry_at(now: u64, attempts: u32) -> u64 {
    now.saturating_add(250_u64.saturating_mul(1_u64 << attempts.min(10)))
}

fn maintenance_label(kind: ShardMaintenanceKind) -> &'static str {
    match kind {
        ShardMaintenanceKind::Repair => "mvcc_shard_repair",
        ShardMaintenanceKind::Rebalance => "mvcc_shard_rebalance",
    }
}

fn topology_epoch(
    candidates: &[ShardTarget],
    tolerated_failure_domains: usize,
) -> Result<[u8; 32]> {
    let mut candidates = candidates.to_vec();
    candidates.sort_by(|left, right| {
        (&left.cluster_id, &left.node, &left.failure_domain).cmp(&(
            &right.cluster_id,
            &right.node,
            &right.failure_domain,
        ))
    });
    let bytes = serde_json::to_vec(&(candidates, tolerated_failure_domains))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mvcc_transaction::NodeIncarnation,
        object_shard_manifest::{OBJECT_SHARD_MANIFEST_SCHEMA, PhysicalShardPlacement},
    };
    use uuid::Uuid;

    fn job() -> ShardRepairJob {
        let source = PhysicalObjectShardManifest {
            schema_version: OBJECT_SHARD_MANIFEST_SCHEMA,
            cluster_id: "cluster".into(),
            object_identity: Uuid::from_u128(7),
            object_hash: format!("sha256:{}", hex::encode([7; 32])),
            object_length: 3,
            encoding_generation: 1,
            data_shards: 1,
            parity_shards: 1,
            shard_bytes: 3,
            stripe_count: 1,
            placements: vec![PhysicalShardPlacement {
                stripe_ordinal: 0,
                shard_ordinal: 0,
                payload_length: 3,
                payload_hash: [8; 32],
                transfer_id: Uuid::from_u128(8),
                node_id: "node-a".into(),
                node_incarnation: 1,
                failure_domain: "zone-a".into(),
            }],
        };
        let source_manifest_hash = hex::encode(blake3::hash(&source.canonical_bytes().unwrap()));
        ShardRepairJob {
            schema: ShardRepairJob::SCHEMA.into(),
            cluster_id: "cluster".into(),
            transaction_id: "tx".into(),
            kind: ShardMaintenanceKind::Repair,
            target_logical_identity: format!("cluster/cluster/object/{}", source.object_hash),
            source_manifest: source,
            source_manifest_hash,
            missing: vec![MissingShardTarget {
                stripe_ordinal: 0,
                shard_ordinal: 1,
                target: ShardTarget {
                    cluster_id: "cluster".into(),
                    node: NodeIncarnation {
                        node_id: "node-b".into(),
                        incarnation: 1,
                    },
                    failure_domain: "zone-b".into(),
                },
            }],
            retiring: Vec::new(),
            originating_snapshot_version: 4,
            requested_at_unix_ms: 10,
        }
    }

    #[test]
    fn repair_job_is_canonical_and_leases_expire() {
        let job = job();
        assert_eq!(
            ShardRepairJob::decode(&job.canonical_bytes().unwrap()).unwrap(),
            job
        );
        let mut record = ShardRepairRecord::pending(job);
        assert!(record.claimable(10));
        record.state = ShardRepairState::Running;
        record.lease_expires_unix_ms = Some(20);
        assert!(!record.claimable(19));
        assert!(record.claimable(20));
    }

    #[test]
    fn repair_job_rejects_duplicate_missing_ordinal() {
        let mut job = job();
        job.missing.push(job.missing[0].clone());
        assert!(job.validate().is_err());
    }

    #[test]
    fn placement_drift_plans_canonical_rebalance_and_post_cutover_retirement() {
        let source = job().source_manifest;
        let candidates = ["b", "c"]
            .into_iter()
            .enumerate()
            .map(|(index, suffix)| ShardTarget {
                cluster_id: "cluster".into(),
                node: NodeIncarnation {
                    node_id: format!("node-{suffix}"),
                    incarnation: 1,
                },
                failure_domain: format!("zone-{index}"),
            })
            .collect::<Vec<_>>();
        let planned = plan_rebalance_job(
            &source,
            "rebalance-tx",
            &candidates,
            ShardPlacementPolicy {
                tolerated_failure_domains: 1,
            },
            4,
            20,
        )
        .unwrap()
        .unwrap();
        assert_eq!(planned.kind, ShardMaintenanceKind::Rebalance);
        assert_eq!(planned.retiring, source.placements);
        assert_eq!(
            ShardRepairJob::decode(&planned.canonical_bytes().unwrap()).unwrap(),
            planned
        );
    }

    #[test]
    fn topology_epoch_is_order_independent_and_policy_sensitive() {
        let candidates = ["a", "b"]
            .into_iter()
            .map(|suffix| ShardTarget {
                cluster_id: "cluster".into(),
                node: NodeIncarnation {
                    node_id: format!("node-{suffix}"),
                    incarnation: 1,
                },
                failure_domain: format!("zone-{suffix}"),
            })
            .collect::<Vec<_>>();
        let mut reversed = candidates.clone();
        reversed.reverse();
        assert_eq!(
            topology_epoch(&candidates, 1).unwrap(),
            topology_epoch(&reversed, 1).unwrap()
        );
        assert_ne!(
            topology_epoch(&candidates, 0).unwrap(),
            topology_epoch(&candidates, 1).unwrap()
        );
    }
}
