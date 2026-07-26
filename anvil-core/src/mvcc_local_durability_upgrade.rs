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
    local_object_store::{LocalObjectManifest, LocalObjectStore},
    mvcc_transaction::{
        BundleDurabilityEvidence, BundleIdentity, DurabilityLevel, NodeIncarnation,
        ObjectDurabilityEvidence,
    },
    object_shard_manifest::PhysicalObjectShardManifest,
    shard_placement::{
        DistributedIngest, ShardPlacementPlan, ShardPlacementPolicy, ShardTargetStream,
    },
    streaming_erasure::ErasureProfile,
};

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
    pub bundle: BundleIdentity,
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
            || self.commit_version == 0
            || self.bundle.length == 0
            || !is_sha256(&self.bundle.hash)
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
        Ok(hex::encode(Sha256::digest(self.canonical_bytes()?)))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LocalDurabilityUpgradeState {
    Pending,
    Running {
        attempt: u32,
        worker: NodeIncarnation,
    },
    Complete {
        completed_at_unix_ms: u64,
    },
    Retryable {
        attempt: u32,
        error: String,
        retry_after_unix_ms: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalDurabilityUpgradeRecord {
    pub schema: String,
    pub job: LocalDurabilityUpgradeJob,
    pub state: LocalDurabilityUpgradeState,
}

impl LocalDurabilityUpgradeRecord {
    pub const SCHEMA: &'static str = "anvil.mvcc.local-durability-upgrade-record.v1";

    pub fn pending(job: LocalDurabilityUpgradeJob) -> Result<Self> {
        job.validate()?;
        Ok(Self {
            schema: Self::SCHEMA.into(),
            job,
            state: LocalDurabilityUpgradeState::Pending,
        })
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
    async fn placement(
        &self,
        job: &LocalDurabilityUpgradeJob,
        object: &LocalDurabilityUpgradeObject,
    ) -> Result<LocalUpgradePlacement>;
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
        let bundle_hash = parse_sha256(&job.bundle.hash)?;
        let durability = match job.target {
            DurabilityLevel::Local => bail!("local is not an upgrade target"),
            DurabilityLevel::Quorum => anvil_mvcc_consensus::DurabilityLevel::Quorum,
            DurabilityLevel::Erasure => anvil_mvcc_consensus::DurabilityLevel::Erasure,
        };
        let holders = holders
            .into_iter()
            .map(|holder| anvil_mvcc_consensus::NodeIncarnation {
                node_id: holder.node_id,
                incarnation: holder.incarnation,
            })
            .collect();
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
        self.manifests
            .publish(job, &manifests, &object_evidence)
            .await?;

        // Shard holders and transaction-bundle holders are different evidence.
        // Never infer the latter from the former.
        let holder_evidence = self.bundles.establish_holders(job).await?;
        let holders = validate_bundle_holders(job, holder_evidence)?;

        // This compact command is the sole consensus action in the workflow.
        self.consensus.publish_upgrade(job, holders).await
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
