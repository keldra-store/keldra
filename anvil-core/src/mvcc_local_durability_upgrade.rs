//! Durable post-commit promotion of `local` object representations.
//!
//! This module deliberately separates physical work from the compact Raft
//! transition. A runner may publish the transition only after all replacement
//! manifests and voter-safe bundle-holder ACKs are durable.

use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    bundle_replication::{AppendOnlyPreparedBundleStore, StreamingBundleReplicator},
    local_object_store::{LocalObjectManifest, LocalObjectStore},
    mvcc_transaction::{
        BundleDurabilityEvidence, BundleIdentity, BundleReplicator, DurabilityLevel,
        NodeIncarnation, ObjectDurabilityEvidence, PreparedBundleStore,
    },
    object_shard_manifest::PhysicalObjectShardManifest,
    shard_placement::{
        DistributedIngest, ShardPlacementPlan, ShardPlacementPolicy, ShardTargetStream,
    },
    streaming_erasure::ErasureProfile,
};

pub struct PreparedBundleUpgradePublisher<T> {
    pub store: AppendOnlyPreparedBundleStore,
    pub replicator: StreamingBundleReplicator<T>,
}

#[async_trait]
impl<T> LocalUpgradeBundlePublisher for PreparedBundleUpgradePublisher<T>
where
    T: crate::bundle_replication::BundleTargetStream,
{
    async fn establish_holders(
        &self,
        job: &LocalDurabilityUpgradeJob,
    ) -> Result<Vec<BundleDurabilityEvidence>> {
        let identity = job
            .bundle
            .as_ref()
            .context("committed bundle identity is missing")?;
        let bytes = self
            .store
            .read(identity)?
            .context("committed prepared bundle is unavailable locally")?;
        let local = self.store.persist(identity, &bytes).await?;
        let mut evidence = self
            .replicator
            .replicate(identity, &bytes, &[], job.target)
            .await?
            .bundle_holders;
        evidence.push(local);
        Ok(evidence)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalDurabilityUpgradeObject {
    /// Stable identity used to derive shard transfer IDs on every retry.
    pub object_identity: Uuid,
    pub local_manifest: LocalObjectManifest,
}

/// Immutable, content-addressed work created in the committing transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalDurabilityUpgradeJob {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    pub commit_version: u64,
    /// Bound atomically during certified MVCC apply. It is `None` in the
    /// pre-certification intent to avoid a self-referential bundle hash.
    pub bundle: Option<BundleIdentity>,
    pub target: DurabilityLevel,
    pub objects: Vec<LocalDurabilityUpgradeObject>,
    pub requested_at_unix_ms: u64,
}

impl LocalDurabilityUpgradeJob {
    pub const SCHEMA: &'static str = "anvil.mvcc.local-durability-upgrade-job.v1";

    pub fn validate(&self) -> Result<()> {
        if self.schema != Self::SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self
                .bundle
                .as_ref()
                .is_some_and(|bundle| bundle.length == 0 || !is_sha256(&bundle.hash))
            || self.target == DurabilityLevel::Local
            || self.objects.is_empty()
            || self.requested_at_unix_ms == 0
        {
            bail!("invalid local durability upgrade job");
        }
        let mut identities = BTreeSet::new();
        for object in &self.objects {
            if object.local_manifest.schema_version != 1
                || object.local_manifest.cluster_id != self.cluster_id
                || !is_sha256(&object.local_manifest.object_hash)
                || !identities.insert(object.object_identity)
            {
                bail!("invalid or duplicate local representation in durability upgrade job");
            }
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    pub fn job_id(&self) -> Result<String> {
        let mut intent = self.clone();
        // The commit version is assigned by certification, after the intent
        // has entered the immutable bundle. It is deliberately excluded from
        // the stable job identity and bound atomically during MVCC apply.
        intent.commit_version = 0;
        intent.bundle = None;
        Ok(hex::encode(Sha256::digest(intent.canonical_bytes()?)))
    }

    pub(crate) fn local_holder(&self) -> Result<NodeIncarnation> {
        let mut holders = self
            .objects
            .iter()
            .map(|object| object.local_manifest.node.clone());
        let holder = holders
            .next()
            .context("local durability upgrade has no local holder")?;
        if holders.any(|candidate| candidate != holder) {
            bail!("one local durability upgrade spans multiple holder incarnations");
        }
        Ok(holder)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalDurabilityUpgradeState {
    Pending,
    Running,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalDurabilityUpgradeRecord {
    pub schema: String,
    pub job: LocalDurabilityUpgradeJob,
    pub state: LocalDurabilityUpgradeState,
    pub attempts: u32,
    pub next_attempt_unix_ms: u64,
    pub last_error: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
}

impl LocalDurabilityUpgradeRecord {
    pub const SCHEMA: &'static str = "anvil.mvcc.local-durability-upgrade-record.v1";

    pub fn pending(job: LocalDurabilityUpgradeJob) -> Result<Self> {
        job.validate()?;
        Ok(Self {
            schema: Self::SCHEMA.into(),
            job,
            state: LocalDurabilityUpgradeState::Pending,
            attempts: 0,
            next_attempt_unix_ms: 0,
            last_error: None,
            lease_owner: None,
            lease_expires_unix_ms: None,
        })
    }

    pub fn claimable(&self, now_unix_ms: u64) -> bool {
        (self.state == LocalDurabilityUpgradeState::Pending
            && self.next_attempt_unix_ms <= now_unix_ms)
            || (self.state == LocalDurabilityUpgradeState::Running
                && self
                    .lease_expires_unix_ms
                    .is_some_and(|expiry| expiry <= now_unix_ms))
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        if self.schema != Self::SCHEMA {
            bail!("invalid local durability upgrade record schema");
        }
        self.job.validate()?;
        Ok(serde_json::to_vec(self)?)
    }
}

/// Current cluster placement, resolved on every attempt rather than frozen in
/// the job. This prevents a retry from targeting removed incarnations.
#[derive(Clone, Debug)]
pub struct LocalUpgradePlacement {
    pub plan: ShardPlacementPlan,
    pub policy: ShardPlacementPolicy,
    pub profile: ErasureProfile,
    pub encoding_generation: u64,
}

#[async_trait]
pub trait LocalUpgradePlacementProvider: Send + Sync {
    fn distributed_upgrade_available(&self) -> Result<bool> {
        Ok(true)
    }

    async fn assignment(
        &self,
        job: &LocalDurabilityUpgradeJob,
    ) -> Result<crate::mvcc_worker_authority::AssignmentGuard>;

    fn validate_assignment(
        &self,
        guard: &crate::mvcc_worker_authority::AssignmentGuard,
    ) -> Result<()>;

    async fn placement(
        &self,
        job: &LocalDurabilityUpgradeJob,
        object: &LocalDurabilityUpgradeObject,
    ) -> Result<LocalUpgradePlacement>;
}

#[async_trait]
impl LocalUpgradePlacementProvider for Arc<crate::mvcc_bootstrap::MvccSubsystem> {
    fn distributed_upgrade_available(&self) -> Result<bool> {
        self.live_shard_placement()
            .map(|(candidates, _, _)| candidates.len() >= 2)
    }

    async fn assignment(
        &self,
        job: &LocalDurabilityUpgradeJob,
    ) -> Result<crate::mvcc_worker_authority::AssignmentGuard> {
        self.reconcile_pinned_work_assignment(
            "local-durability-upgrade",
            &format!("transaction/{}", job.transaction_id),
            &job.local_holder()?,
        )
        .await?
        .context("local durability upgrade is assigned to another node")
    }

    fn validate_assignment(
        &self,
        guard: &crate::mvcc_worker_authority::AssignmentGuard,
    ) -> Result<()> {
        crate::mvcc_bootstrap::MvccSubsystem::validate_assignment(self, guard)
    }

    async fn placement(
        &self,
        _job: &LocalDurabilityUpgradeJob,
        _object: &LocalDurabilityUpgradeObject,
    ) -> Result<LocalUpgradePlacement> {
        let (candidates, tolerated_failure_domains, topology_epoch) =
            self.live_shard_placement()?;
        if candidates.len() < 2 {
            bail!("distributed durability upgrade requires at least two shard targets");
        }
        let parity_shards = tolerated_failure_domains.max(1).min(candidates.len() - 1);
        let profile = ErasureProfile {
            data_shards: candidates.len() - parity_shards,
            parity_shards,
            shard_bytes: 256 * 1024,
        };
        let policy = ShardPlacementPolicy {
            tolerated_failure_domains,
        };
        let generation = topology_epoch.max(1);
        Ok(LocalUpgradePlacement {
            plan: policy.plan(_object.object_identity, generation, profile, &candidates)?,
            policy,
            profile,
            encoding_generation: generation,
        })
    }
}

/// Publishes immutable physical-representation overlays durably and
/// idempotently. Implementations must fence their worker assignment.
#[async_trait]
pub trait LocalUpgradeManifestPublisher: Send + Sync {
    async fn publish(
        &self,
        job: &LocalDurabilityUpgradeJob,
        manifests: &[PhysicalObjectShardManifest],
        evidence: &[ObjectDurabilityEvidence],
    ) -> Result<()>;

    async fn publish_completion(&self, job: &LocalDurabilityUpgradeJob) -> Result<()>;
}

const LOCAL_UPGRADE_MANIFEST_TABLE_ID: u16 = 0x7f15;
const LOCAL_UPGRADE_STATUS_TABLE_ID: u16 = 0x7f16;

pub fn resolve_promoted_manifest(
    store: &crate::mvcc_store::LocalMvccStore,
    object_hash: &str,
) -> Result<Option<PhysicalObjectShardManifest>> {
    let key = crate::mvcc_transaction::LogicalKey {
        table_id: LOCAL_UPGRADE_MANIFEST_TABLE_ID,
        application_key: format!("object/{object_hash}").into_bytes(),
    };
    store
        .read_latest(&key)?
        .map(|row| serde_json::from_slice(&row.value).map_err(Into::into))
        .transpose()
}

pub fn promotion_complete(store: &crate::mvcc_store::LocalMvccStore, job_id: &str) -> Result<bool> {
    let key = crate::mvcc_transaction::LogicalKey {
        table_id: LOCAL_UPGRADE_STATUS_TABLE_ID,
        application_key: format!("promotion/{job_id}").into_bytes(),
    };
    Ok(store.read_latest(&key)?.is_some())
}

#[async_trait]
impl LocalUpgradeManifestPublisher for Arc<crate::mvcc_bootstrap::MvccSubsystem> {
    async fn publish(
        &self,
        job: &LocalDurabilityUpgradeJob,
        manifests: &[PhysicalObjectShardManifest],
        _evidence: &[ObjectDurabilityEvidence],
    ) -> Result<()> {
        let principal = format!("local-durability-upgrade/{}", self.local_node.node_id);
        let now = unix_time_ms()?;
        let logical_identity = format!("transaction/{}", job.transaction_id);
        let guard = self
            .reconcile_pinned_work_assignment(
                "local-durability-upgrade",
                &logical_identity,
                &job.local_holder()?,
            )
            .await?
            .context("local durability upgrade is assigned to another node")?;
        let handle = self
            .open_transactions
            .begin(
                self.runtime.as_ref(),
                job.cluster_id.clone(),
                &principal,
                format!("local-upgrade/{}", job.job_id()?),
                std::time::Duration::from_secs(30),
                DurabilityLevel::Quorum,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                now,
            )
            .await?;
        if resume_publication(
            self,
            &handle.transaction_id,
            &principal,
            now,
            "representation",
        )
        .await?
        {
            return Ok(());
        }
        self.stage_assignment_guard(&handle.transaction_id, &principal, &guard, now)?;
        let mut mutations = Vec::with_capacity(manifests.len().saturating_mul(2));
        for manifest in manifests {
            manifest.validate()?;
            let key = crate::mvcc_transaction::LogicalKey {
                table_id: LOCAL_UPGRADE_MANIFEST_TABLE_ID,
                application_key: format!("object/{}", manifest.object_hash).into_bytes(),
            };
            let observed = self
                .runtime
                .local_store()
                .read_point_at(&key, handle.snapshot_version)?;
            mutations.push(crate::mvcc_open_transactions::StagedLogicalMutation {
                key,
                observed_version: observed.observed_version(),
                value: Some(manifest.canonical_bytes()?),
            });
            let catalog_key = crate::mvcc_shard_repair::manifest_catalog_key(manifest);
            let catalog_observed = self
                .runtime
                .local_store()
                .read_point_at(&catalog_key, handle.snapshot_version)?;
            mutations.push(crate::mvcc_open_transactions::StagedLogicalMutation {
                key: catalog_key,
                observed_version: catalog_observed.observed_version(),
                value: Some(manifest.canonical_bytes()?),
            });
        }
        // One durable registry mutation makes a crash during staging
        // retry-safe. Re-entering an Open idempotent transaction replaces
        // writes by key instead of creating a non-canonical duplicate.
        self.open_transactions.stage_logical_mutations(
            &handle.transaction_id,
            &principal,
            &job.cluster_id,
            mutations,
            now,
        )?;
        let outcome = self
            .open_transactions
            .commit(
                self.runtime.as_ref(),
                &handle.transaction_id,
                &principal,
                unix_time_ms()?,
            )
            .await?;
        require_committed_publication(outcome, "representation")
    }

    async fn publish_completion(&self, job: &LocalDurabilityUpgradeJob) -> Result<()> {
        let job_id = job.job_id()?;
        let principal = format!("local-durability-upgrade/{}", self.local_node.node_id);
        let now = unix_time_ms()?;
        let logical_identity = format!("transaction/{}", job.transaction_id);
        let guard = self
            .reconcile_pinned_work_assignment(
                "local-durability-upgrade",
                &logical_identity,
                &job.local_holder()?,
            )
            .await?
            .context("local durability upgrade is assigned to another node")?;
        let handle = self
            .open_transactions
            .begin(
                self.runtime.as_ref(),
                job.cluster_id.clone(),
                &principal,
                format!("local-upgrade-complete/{job_id}"),
                std::time::Duration::from_secs(30),
                DurabilityLevel::Quorum,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                now,
            )
            .await?;
        if resume_publication(self, &handle.transaction_id, &principal, now, "completion").await? {
            return Ok(());
        }
        self.stage_assignment_guard(&handle.transaction_id, &principal, &guard, now)?;
        let key = crate::mvcc_transaction::LogicalKey {
            table_id: LOCAL_UPGRADE_STATUS_TABLE_ID,
            application_key: format!("promotion/{job_id}").into_bytes(),
        };
        let observed = self
            .runtime
            .local_store()
            .read_point_at(&key, handle.snapshot_version)?;
        self.open_transactions.stage_logical_mutations(
            &handle.transaction_id,
            &principal,
            &job.cluster_id,
            vec![crate::mvcc_open_transactions::StagedLogicalMutation {
                key,
                observed_version: observed.observed_version(),
                value: Some(job.commit_version.to_be_bytes().to_vec()),
            }],
            now,
        )?;
        let outcome = self
            .open_transactions
            .commit(
                self.runtime.as_ref(),
                &handle.transaction_id,
                &principal,
                unix_time_ms()?,
            )
            .await?;
        require_committed_publication(outcome, "completion")
    }
}

async fn resume_publication(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    now_unix_ms: u64,
    phase: &str,
) -> Result<bool> {
    let status = mvcc
        .open_transactions
        .status(transaction_id, principal, now_unix_ms)?;
    match status.state {
        "open" => Ok(false),
        "committing" => {
            let outcome = mvcc
                .open_transactions
                .commit(
                    mvcc.runtime.as_ref(),
                    transaction_id,
                    principal,
                    now_unix_ms,
                )
                .await?;
            require_committed_publication(outcome, phase)?;
            Ok(true)
        }
        "committed" => Ok(true),
        "aborted" => bail!("local durability {phase} publication conflicted"),
        state => bail!("local durability {phase} publication transaction is {state}"),
    }
}

fn require_committed_publication(
    outcome: crate::mvcc_node_runtime::CommitOutcome,
    phase: &str,
) -> Result<()> {
    if matches!(
        outcome.certification,
        crate::mvcc_transaction::CertificationResult::Committed { .. }
    ) {
        Ok(())
    } else {
        bail!("local durability {phase} publication conflicted")
    }
}

/// Re-reads the immutable prepared bundle, verifies its identity, streams it
/// to current targets, and returns only Complete/hash-verified/fsynced ACKs.
#[async_trait]
pub trait LocalUpgradeBundlePublisher: Send + Sync {
    async fn establish_holders(
        &self,
        job: &LocalDurabilityUpgradeJob,
    ) -> Result<Vec<BundleDurabilityEvidence>>;
}

/// Final compact consensus transition. This is intentionally the last step.
#[async_trait]
pub trait LocalUpgradeConsensus: Send + Sync {
    async fn publish_upgrade(
        &self,
        job: &LocalDurabilityUpgradeJob,
        holders: Vec<NodeIncarnation>,
    ) -> Result<()>;
}

#[async_trait]
impl LocalUpgradeConsensus for anvil_mvcc_consensus::OpenRaftConsensus {
    async fn publish_upgrade(
        &self,
        job: &LocalDurabilityUpgradeJob,
        holders: Vec<NodeIncarnation>,
    ) -> Result<()> {
        let cluster_id_hash =
            domain_hash(b"anvil.mvcc.cluster-id.v1", &[job.cluster_id.as_bytes()]);
        let bundle = job
            .bundle
            .as_ref()
            .context("durability upgrade intent has no committed bundle identity")?;
        let bundle_hash = parse_sha256(&bundle.hash)?;
        let durability = match job.target {
            DurabilityLevel::Local => bail!("local is not an upgrade target"),
            DurabilityLevel::Quorum => anvil_mvcc_consensus::DurabilityLevel::Quorum,
            DurabilityLevel::Erasure => anvil_mvcc_consensus::DurabilityLevel::Erasure,
        };
        let mut holders = holders
            .into_iter()
            .map(|holder| anvil_mvcc_consensus::NodeIncarnation {
                node_id: crate::mvcc_bootstrap::consensus_control_node_id(&holder.node_id),
                incarnation: holder.incarnation,
            })
            .collect::<Vec<_>>();
        // The application-level node IDs were canonical before this mapping,
        // but their compact-Raft IDs are hashes and therefore have a different
        // ordering. UpgradeDurability deliberately rejects non-canonical
        // holder evidence, so canonicalise in the command's identity domain.
        holders.sort_unstable();
        holders.dedup();
        self.upgrade_durability(
            cluster_id_hash,
            anvil_mvcc_consensus::CommitVersion(job.commit_version),
            anvil_mvcc_consensus::BundleHash(bundle_hash),
            durability,
            holders,
        )
        .await?;
        Ok(())
    }
}

/// Executes one already-leased durable job.
///
/// Retry safety comes from deterministic object identities/encoding generation,
/// idempotent shard transfer IDs, idempotent overlay publication, immutable
/// bundle identity, and the monotonic Raft transition.
pub struct LocalDurabilityUpgradeRunner<T, P, M, B, C> {
    local_objects: LocalObjectStore,
    shard_transport: T,
    placement: P,
    manifests: M,
    bundles: B,
    consensus: C,
}

impl<T, P, M, B, C> LocalDurabilityUpgradeRunner<T, P, M, B, C> {
    pub fn new(
        local_objects: LocalObjectStore,
        shard_transport: T,
        placement: P,
        manifests: M,
        bundles: B,
        consensus: C,
    ) -> Self {
        Self {
            local_objects,
            shard_transport,
            placement,
            manifests,
            bundles,
            consensus,
        }
    }
}

impl<T, P, M, B, C> LocalDurabilityUpgradeRunner<T, P, M, B, C>
where
    T: ShardTargetStream,
    P: LocalUpgradePlacementProvider,
    M: LocalUpgradeManifestPublisher,
    B: LocalUpgradeBundlePublisher,
    C: LocalUpgradeConsensus,
{
    pub async fn execute(&self, job: &LocalDurabilityUpgradeJob) -> Result<()> {
        job.validate()?;
        if job.commit_version == 0 || job.bundle.is_none() {
            bail!("durability upgrade intent has not been bound to a committed transaction");
        }
        let assignment = self.placement.assignment(job).await?;

        let mut manifests = Vec::with_capacity(job.objects.len());
        let mut object_evidence = Vec::new();
        for object in &job.objects {
            let placement = self.placement.placement(job, object).await?;
            if placement.encoding_generation == 0 {
                bail!("durability upgrade encoding generation must be non-zero");
            }
            let mut reader = self
                .local_objects
                .open_verified(&object.local_manifest)
                .await?;
            let ingest = DistributedIngest::encode(
                &self.shard_transport,
                &placement.plan,
                placement.policy,
                placement.profile,
                job.target,
                &mut reader,
                &job.transaction_id,
                job.commit_version,
                job.requested_at_unix_ms,
                true,
                object.object_identity,
                Some(&object.local_manifest.object_hash),
                placement.encoding_generation,
            )
            .await?;
            let manifest = PhysicalObjectShardManifest::from_ingest(
                &job.cluster_id,
                object.object_identity,
                placement.encoding_generation,
                placement.profile.data_shards,
                placement.profile.parity_shards,
                placement.profile.shard_bytes,
                &ingest,
            )?;
            object_evidence.extend(ingest.evidence);
            manifests.push(manifest);
        }

        // Physical representation selection must be durable before the local
        // representation ceases to be the authoritative representation.
        self.placement.validate_assignment(&assignment)?;
        self.manifests
            .publish(job, &manifests, &object_evidence)
            .await?;

        // Shard holders and transaction-bundle holders are different evidence.
        // Never infer the latter from the former.
        let holder_evidence = self.bundles.establish_holders(job).await?;
        let holders = validate_bundle_holders(job, holder_evidence)?;

        // This compact command is the sole consensus action in the workflow.
        self.placement.validate_assignment(&assignment)?;
        self.consensus.publish_upgrade(job, holders).await?;
        self.manifests.publish_completion(job).await
    }

    /// Claims and executes at most one durably persisted job. The caller's
    /// continuous worker loop supplies clock/backoff values and must use an
    /// assignment-fenced worker identity.
    pub async fn run_once(
        &self,
        store: &crate::mvcc_store::LocalMvccStore,
        local_node: &NodeIncarnation,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        retry_after_unix_ms: u64,
    ) -> Result<bool> {
        if !self.placement.distributed_upgrade_available()? {
            return Ok(false);
        }
        let Some((job_id, record)) = store.claim_local_durability_upgrade_where(
            worker_id,
            now_unix_ms,
            lease_ms,
            |record| {
                record
                    .job
                    .objects
                    .iter()
                    .all(|object| object.local_manifest.node == *local_node)
            },
        )?
        else {
            return Ok(false);
        };
        let assignment = match self.placement.assignment(&record.job).await {
            Ok(assignment) => assignment,
            Err(error) => {
                store.retry_local_durability_upgrade(
                    &job_id,
                    worker_id,
                    retry_after_unix_ms,
                    &error.to_string(),
                )?;
                return Err(error);
            }
        };
        let lease_owner = assignment.lease_owner(worker_id);
        store.rebind_local_durability_upgrade_lease(&job_id, worker_id, &lease_owner)?;
        match self.execute(&record.job).await {
            Ok(()) => {
                store.complete_local_durability_upgrade(&job_id, &lease_owner)?;
                Ok(true)
            }
            Err(error) => {
                let retry_after_unix_ms = if error
                    .to_string()
                    .contains("distributed durability upgrade requires at least two shard targets")
                {
                    now_unix_ms.saturating_add(60 * 60 * 1_000)
                } else {
                    retry_after_unix_ms
                };
                store.retry_local_durability_upgrade(
                    &job_id,
                    &lease_owner,
                    retry_after_unix_ms,
                    &error.to_string(),
                )?;
                Err(error)
            }
        }
    }

    pub async fn run(
        self,
        store: crate::mvcc_store::LocalMvccStore,
        local_node: NodeIncarnation,
        worker_id: String,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            let now = unix_time_ms().unwrap_or(1);
            if let Err(error) = self
                .run_once(
                    &store,
                    &local_node,
                    &worker_id,
                    now,
                    30_000,
                    now.saturating_add(1_000),
                )
                .await
            {
                tracing::warn!(%error, "local durability upgrade attempt will retry");
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
            }
        }
    }
}

fn validate_bundle_holders(
    job: &LocalDurabilityUpgradeJob,
    evidence: Vec<BundleDurabilityEvidence>,
) -> Result<Vec<NodeIncarnation>> {
    let mut holders = BTreeSet::new();
    for holder in evidence {
        if holder.cluster_id != job.cluster_id
            || holder.node.node_id.trim().is_empty()
            || holder.node.incarnation == 0
            || holder.failure_domain.trim().is_empty()
            || !holder.complete
            || !holder.hash_verified
            || !holder.fsynced
        {
            bail!("bundle publisher returned non-authoritative durability evidence");
        }
        holders.insert(holder.node);
    }
    if holders.is_empty() {
        bail!("durability upgrade has no authoritative bundle holder ACKs");
    }
    Ok(holders.into_iter().collect())
}

fn is_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn parse_sha256(value: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(
        value
            .strip_prefix("sha256:")
            .context("bundle hash must use sha256")?,
    )?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("bundle hash must contain 32 bytes"))
}

fn domain_hash(domain: &[u8], fields: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    for field in fields {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    hasher.finalize().into()
}

fn unix_time_ms() -> Result<u64> {
    Ok(u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?)
}
