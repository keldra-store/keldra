//! Transaction-coordinator boundary for the MVCC-under-Raft architecture.
//!
//! Product services build one immutable [`TransactionBundle`] containing every
//! logical mutation. Bundle persistence and replication happen outside Raft;
//! only [`CertificationRequest`] is submitted to consensus.

use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub type CommitVersion = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityLevel {
    Local,
    Quorum,
    Erasure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistency {
    LocalSnapshot,
    Linearized,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogicalKey {
    pub table_id: u16,
    pub application_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointObservation {
    pub key: LogicalKey,
    pub observed_version: Option<CommitVersion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeObservation {
    pub table_id: u16,
    pub start_application_key: Option<Vec<u8>>,
    pub end_application_key: Option<Vec<u8>>,
    pub conflict_key: RangeStampKey,
    pub observed_range_stamp: Option<CommitVersion>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RangeStampKey {
    pub scheme_version: u16,
    pub table_id: u16,
    pub key_prefix: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteOperation {
    Put { key: LogicalKey, value: Vec<u8> },
    Delete { key: LogicalKey },
}

impl WriteOperation {
    pub fn key(&self) -> &LogicalKey {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectShardManifestReference {
    pub object_hash: String,
    pub manifest_hash: String,
    pub object_length: u64,
    pub encoding_generation: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub stripe_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OwnedResource {
    LogicalKey(LogicalKey),
    Range {
        table_id: u16,
        start_application_key: Option<Vec<u8>>,
        end_application_key: Option<Vec<u8>>,
    },
    Manifest {
        object_hash: String,
        manifest_hash: String,
        encoding_generation: u64,
    },
    OutboxEvent {
        payload_hash: [u8; 32],
    },
    MaterialisationJob {
        payload_hash: [u8; 32],
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ClusterOwnershipClaim {
    cluster_id: String,
    resource: OwnedResource,
}

impl ClusterOwnershipClaim {
    fn new(cluster_id: impl Into<String>, resource: OwnedResource) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            resource,
        }
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn resource(&self) -> &OwnedResource {
        &self.resource
    }
}

pub trait ClusterOwnershipResolver: Send + Sync {
    fn validate_claim(
        &self,
        transaction_cluster_id: &str,
        claim: &ClusterOwnershipClaim,
    ) -> Result<()>;
}

#[derive(Debug)]
pub struct RoutingIssuedOwnership;

impl ClusterOwnershipResolver for RoutingIssuedOwnership {
    fn validate_claim(
        &self,
        transaction_cluster_id: &str,
        claim: &ClusterOwnershipClaim,
    ) -> Result<()> {
        if claim.cluster_id != transaction_cluster_id {
            bail!("transaction resource belongs to another cluster");
        }
        Ok(())
    }
}

/// Canonically encoded transaction data persisted and replicated outside Raft.
///
/// A bundle deliberately has no partition or publication scope. One bundle may
/// contain keys from unrelated tables and physical partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionBundle {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    pub snapshot_version: CommitVersion,
    pub authenticated_principal: String,
    pub point_observations: Vec<PointObservation>,
    pub range_observations: Vec<RangeObservation>,
    pub advanced_range_stamps: Vec<RangeStampKey>,
    pub writes: Vec<WriteOperation>,
    pub shard_manifests: Vec<ObjectShardManifestReference>,
    pub outbox_events: Vec<Vec<u8>>,
    pub materialisation_jobs: Vec<Vec<u8>>,
    pub ownership_claims: Vec<ClusterOwnershipClaim>,
}

impl TransactionBundle {
    pub const SCHEMA: &'static str = "anvil.mvcc.transaction-bundle.v1";

    pub fn canonicalize(&mut self) -> Result<()> {
        if self.schema != Self::SCHEMA {
            bail!("unsupported transaction bundle schema");
        }
        if self.cluster_id.trim().is_empty() {
            bail!("cluster ID must not be empty");
        }
        if self.transaction_id.is_empty() {
            bail!("transaction ID must not be empty");
        }
        if self.authenticated_principal.is_empty() {
            bail!("authenticated principal must not be empty");
        }

        self.point_observations
            .sort_by(|left, right| left.key.cmp(&right.key));
        ensure_unique(
            self.point_observations.iter().map(|entry| &entry.key),
            "point observation",
        )?;

        self.range_observations.sort_by(|left, right| {
            (
                left.table_id,
                &left.start_application_key,
                &left.end_application_key,
                &left.conflict_key,
            )
                .cmp(&(
                    right.table_id,
                    &right.start_application_key,
                    &right.end_application_key,
                    &right.conflict_key,
                ))
        });
        ensure_unique_by(
            self.range_observations.iter(),
            |entry| {
                (
                    entry.table_id,
                    entry.start_application_key.as_deref(),
                    entry.end_application_key.as_deref(),
                    &entry.conflict_key,
                )
            },
            "range observation",
        )?;
        for observation in &self.range_observations {
            if matches!(
                (
                    observation.start_application_key.as_deref(),
                    observation.end_application_key.as_deref(),
                ),
                (Some(start), Some(end)) if start >= end
            ) {
                bail!("range observation must be a non-empty half-open interval");
            }
            if observation.table_id != observation.conflict_key.table_id {
                bail!("range observation conflict key belongs to another table");
            }
            if observation.conflict_key.scheme_version
                != HierarchicalRangeStampScheme::SCHEME_VERSION
            {
                bail!("range observation uses an unsupported stamp scheme");
            }
        }
        self.advanced_range_stamps.sort();
        ensure_unique(
            self.advanced_range_stamps.iter(),
            "advanced range conflict stamp",
        )?;
        if self
            .advanced_range_stamps
            .iter()
            .any(|stamp| stamp.scheme_version != HierarchicalRangeStampScheme::SCHEME_VERSION)
        {
            bail!("advanced range stamp uses an unsupported scheme");
        }
        self.writes
            .sort_by(|left, right| left.key().cmp(right.key()));
        ensure_unique(self.writes.iter().map(WriteOperation::key), "written key")?;
        self.shard_manifests
            .sort_by(|left, right| left.object_hash.cmp(&right.object_hash));
        ensure_unique(
            self.shard_manifests.iter().map(|entry| &entry.object_hash),
            "object shard manifest",
        )?;
        for manifest in &self.shard_manifests {
            if !is_sha256_hash(&manifest.object_hash) || !is_sha256_hash(&manifest.manifest_hash) {
                bail!("object and shard manifest hashes must be sha256 hashes");
            }
            if manifest.data_shards == 0 || manifest.stripe_count == 0 {
                bail!("shard manifest must declare data shards and stripes");
            }
        }
        self.materialisation_jobs.sort();
        ensure_unique(self.materialisation_jobs.iter(), "materialisation job")?;
        for encoded_job in &self.materialisation_jobs {
            let job = crate::object_materialisation::ObjectMaterialisationJob::decode(encoded_job)?;
            if job.cluster_id != self.cluster_id || job.transaction_id != self.transaction_id {
                bail!("materialisation job belongs to another transaction or cluster");
            }
        }
        self.ownership_claims.sort();
        ensure_unique(self.ownership_claims.iter(), "ownership claim")?;
        if self.ownership_claims != expected_ownership_claims(self) {
            bail!("transaction bundle ownership claims do not cover its resources");
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let mut canonical = self.clone();
        canonical.canonicalize()?;
        serde_json::to_vec(&canonical).map_err(Into::into)
    }

    pub fn identity(&self) -> Result<BundleIdentity> {
        let bytes = self.canonical_bytes()?;
        let mut hasher = Sha256::new();
        hasher.update(b"anvil.mvcc.transaction-bundle.v1");
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(&bytes);
        Ok(BundleIdentity {
            hash: format!("sha256:{:x}", hasher.finalize()),
            length: u64::try_from(bytes.len()).map_err(|_| anyhow!("bundle is too large"))?,
        })
    }
}

/// Deterministic hierarchical range-stamp layout.
///
/// The empty prefix is the table-wide stamp. A write advances that stamp and
/// every byte-prefix ancestor of its key through [`Self::MAX_PREFIX_BYTES`]. A
/// scan observes the deepest scheme prefix shared by its half-open bounds.
/// Consequently every key inside the scan advances the observed stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HierarchicalRangeStampScheme;

impl HierarchicalRangeStampScheme {
    pub const SCHEME_VERSION: u16 = 1;
    pub const MAX_PREFIX_BYTES: usize = 8;

    pub const fn new() -> Self {
        Self
    }

    pub fn observation_key(
        self,
        table_id: u16,
        start_application_key: Option<&[u8]>,
        end_application_key: Option<&[u8]>,
    ) -> Result<RangeStampKey> {
        let shared = match (start_application_key, end_application_key) {
            (Some(start), Some(end)) => {
                if start >= end {
                    bail!("range observation must be a non-empty half-open interval");
                }
                start
                    .iter()
                    .zip(end)
                    .take(Self::MAX_PREFIX_BYTES)
                    .take_while(|(left, right)| left == right)
                    .count()
            }
            _ => 0,
        };
        Ok(RangeStampKey {
            scheme_version: Self::SCHEME_VERSION,
            table_id,
            key_prefix: start_application_key
                .map(|start| start[..shared].to_vec())
                .unwrap_or_default(),
        })
    }

    pub fn write_keys(self, key: &LogicalKey) -> Vec<RangeStampKey> {
        let depth = Self::MAX_PREFIX_BYTES.min(key.application_key.len());
        (0..=depth)
            .map(|prefix_len| RangeStampKey {
                scheme_version: Self::SCHEME_VERSION,
                table_id: key.table_id,
                key_prefix: key.application_key[..prefix_len].to_vec(),
            })
            .collect()
    }
}

/// Builds one scope-free transaction bundle while deriving all hierarchical
/// range conflict metadata from reads and writes.
pub struct TransactionBundleBuilder {
    bundle: TransactionBundle,
    range_scheme: HierarchicalRangeStampScheme,
    advanced_range_stamps: BTreeSet<RangeStampKey>,
}

impl TransactionBundleBuilder {
    pub fn new(
        cluster_id: impl Into<String>,
        transaction_id: impl Into<String>,
        snapshot_version: CommitVersion,
        authenticated_principal: impl Into<String>,
        range_scheme: HierarchicalRangeStampScheme,
    ) -> Self {
        Self {
            bundle: TransactionBundle {
                schema: TransactionBundle::SCHEMA.to_string(),
                cluster_id: cluster_id.into(),
                transaction_id: transaction_id.into(),
                snapshot_version,
                authenticated_principal: authenticated_principal.into(),
                point_observations: Vec::new(),
                range_observations: Vec::new(),
                advanced_range_stamps: Vec::new(),
                writes: Vec::new(),
                shard_manifests: Vec::new(),
                outbox_events: Vec::new(),
                materialisation_jobs: Vec::new(),
                ownership_claims: Vec::new(),
            },
            range_scheme,
            advanced_range_stamps: BTreeSet::new(),
        }
    }

    pub fn observe_point(
        &mut self,
        key: LogicalKey,
        observed_version: Option<CommitVersion>,
    ) -> &mut Self {
        self.bundle.point_observations.push(PointObservation {
            key: key.clone(),
            observed_version,
        });
        self.claim(OwnedResource::LogicalKey(key));
        self
    }

    pub fn observe_range(
        &mut self,
        table_id: u16,
        start_application_key: Vec<u8>,
        end_application_key: Vec<u8>,
        observed_range_stamp: Option<CommitVersion>,
    ) -> Result<&mut Self> {
        self.observe_scan(
            table_id,
            Some(start_application_key),
            Some(end_application_key),
            observed_range_stamp,
        )
    }

    pub fn observe_scan(
        &mut self,
        table_id: u16,
        start_application_key: Option<Vec<u8>>,
        end_application_key: Option<Vec<u8>>,
        observed_range_stamp: Option<CommitVersion>,
    ) -> Result<&mut Self> {
        let conflict_key = self.range_scheme.observation_key(
            table_id,
            start_application_key.as_deref(),
            end_application_key.as_deref(),
        )?;
        self.bundle.range_observations.push(RangeObservation {
            table_id,
            start_application_key: start_application_key.clone(),
            end_application_key: end_application_key.clone(),
            conflict_key,
            observed_range_stamp,
        });
        self.claim(OwnedResource::Range {
            table_id,
            start_application_key,
            end_application_key,
        });
        Ok(self)
    }

    pub fn put(&mut self, key: LogicalKey, value: Vec<u8>) -> &mut Self {
        self.claim(OwnedResource::LogicalKey(key.clone()));
        self.advance_write_stamps(&key);
        self.bundle.writes.push(WriteOperation::Put { key, value });
        self
    }

    pub fn delete(&mut self, key: LogicalKey) -> &mut Self {
        self.claim(OwnedResource::LogicalKey(key.clone()));
        self.advance_write_stamps(&key);
        self.bundle.writes.push(WriteOperation::Delete { key });
        self
    }

    /// Rename is one atomic delete plus put and may cross tables or partitions.
    pub fn rename(
        &mut self,
        old_key: LogicalKey,
        new_key: LogicalKey,
        value: Vec<u8>,
    ) -> &mut Self {
        self.delete(old_key);
        self.put(new_key, value);
        self
    }

    pub fn add_shard_manifest(&mut self, manifest: ObjectShardManifestReference) -> &mut Self {
        self.claim(manifest_resource(&manifest));
        self.bundle.shard_manifests.push(manifest);
        self
    }

    pub fn add_outbox_event(&mut self, event: Vec<u8>) -> &mut Self {
        self.claim(payload_resource(&event, true));
        self.bundle.outbox_events.push(event);
        self
    }

    pub fn add_materialisation_job(&mut self, job: Vec<u8>) -> &mut Self {
        self.claim(payload_resource(&job, false));
        self.bundle.materialisation_jobs.push(job);
        self
    }

    pub fn build(mut self) -> Result<TransactionBundle> {
        self.bundle.advanced_range_stamps = self.advanced_range_stamps.into_iter().collect();
        self.bundle.canonicalize()?;
        Ok(self.bundle)
    }

    fn advance_write_stamps(&mut self, key: &LogicalKey) {
        self.advanced_range_stamps
            .extend(self.range_scheme.write_keys(key));
    }

    fn claim(&mut self, resource: OwnedResource) {
        let claim = ClusterOwnershipClaim::new(self.bundle.cluster_id.clone(), resource);
        if !self.bundle.ownership_claims.contains(&claim) {
            self.bundle.ownership_claims.push(claim);
        }
    }
}

fn expected_ownership_claims(bundle: &TransactionBundle) -> Vec<ClusterOwnershipClaim> {
    let mut resources = BTreeSet::new();
    resources.extend(
        bundle
            .point_observations
            .iter()
            .map(|entry| OwnedResource::LogicalKey(entry.key.clone())),
    );
    resources.extend(
        bundle
            .writes
            .iter()
            .map(|entry| OwnedResource::LogicalKey(entry.key().clone())),
    );
    resources.extend(
        bundle
            .range_observations
            .iter()
            .map(|entry| OwnedResource::Range {
                table_id: entry.table_id,
                start_application_key: entry.start_application_key.clone(),
                end_application_key: entry.end_application_key.clone(),
            }),
    );
    resources.extend(bundle.shard_manifests.iter().map(manifest_resource));
    resources.extend(
        bundle
            .outbox_events
            .iter()
            .map(|payload| payload_resource(payload, true)),
    );
    resources.extend(
        bundle
            .materialisation_jobs
            .iter()
            .map(|payload| payload_resource(payload, false)),
    );
    resources
        .into_iter()
        .map(|resource| ClusterOwnershipClaim::new(bundle.cluster_id.clone(), resource))
        .collect()
}

fn manifest_resource(manifest: &ObjectShardManifestReference) -> OwnedResource {
    OwnedResource::Manifest {
        object_hash: manifest.object_hash.clone(),
        manifest_hash: manifest.manifest_hash.clone(),
        encoding_generation: manifest.encoding_generation,
    }
}

fn payload_resource(payload: &[u8], outbox: bool) -> OwnedResource {
    let payload_hash = *blake3::hash(payload).as_bytes();
    if outbox {
        OwnedResource::OutboxEvent { payload_hash }
    } else {
        OwnedResource::MaterialisationJob { payload_hash }
    }
}

fn ensure_unique_by<'a, T: 'a, K: PartialEq>(
    values: impl IntoIterator<Item = &'a T>,
    key: impl Fn(&'a T) -> K,
    description: &str,
) -> Result<()> {
    let mut previous = None;
    for value in values {
        let current = key(value);
        if previous.as_ref() == Some(&current) {
            bail!("transaction bundle contains duplicate {description}");
        }
        previous = Some(current);
    }
    Ok(())
}

fn is_sha256_hash(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn ensure_unique<'a, T: Ord + ?Sized + 'a>(
    values: impl IntoIterator<Item = &'a T>,
    description: &str,
) -> Result<()> {
    let mut previous: Option<&T> = None;
    for value in values {
        if previous == Some(value) {
            bail!("transaction bundle contains duplicate {description}");
        }
        previous = Some(value);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleIdentity {
    pub hash: String,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeIncarnation {
    pub node_id: String,
    pub incarnation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleDurabilityEvidence {
    pub cluster_id: String,
    pub node: NodeIncarnation,
    pub failure_domain: String,
    pub complete: bool,
    pub hash_verified: bool,
    pub fsynced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ObjectDurabilityEvidence {
    LocalRepresentation {
        cluster_id: String,
        object_hash: String,
        node: NodeIncarnation,
        failure_domain: String,
        complete: bool,
        hash_verified: bool,
        fsynced: bool,
    },
    ShardPlacement {
        cluster_id: String,
        object_hash: String,
        encoding_generation: u64,
        stripe_ordinal: u64,
        shard_ordinal: u16,
        data_shards: u16,
        parity_shards: u16,
        node: NodeIncarnation,
        failure_domain: String,
        complete: bool,
        hash_verified: bool,
        fsynced: bool,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplicationEvidence {
    pub bundle_holders: Vec<BundleDurabilityEvidence>,
    pub objects: Vec<ObjectDurabilityEvidence>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityPolicy {
    /// Number of distinct nodes that must hold the complete transaction bundle.
    pub bundle_quorum_holders: usize,
    /// Number of simultaneous failure-domain losses object placement must survive.
    pub tolerated_failure_domains: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionResourceLimits {
    pub max_point_observations: usize,
    pub max_range_observations: usize,
    pub max_written_keys: usize,
    pub max_certification_command_bytes: usize,
    pub max_bundle_bytes: usize,
    pub max_raw_payload_bytes: usize,
}

impl Default for TransactionResourceLimits {
    fn default() -> Self {
        Self {
            max_point_observations: 65_536,
            max_range_observations: 16_384,
            max_written_keys: 65_536,
            max_certification_command_bytes: 8 * 1024 * 1024,
            max_bundle_bytes: 64 * 1024 * 1024,
            max_raw_payload_bytes: 512 * 1024 * 1024,
        }
    }
}

impl TransactionResourceLimits {
    fn validate(self) -> Result<()> {
        if self.max_point_observations == 0
            || self.max_range_observations == 0
            || self.max_written_keys == 0
            || self.max_certification_command_bytes == 0
            || self.max_bundle_bytes == 0
            || self.max_raw_payload_bytes == 0
        {
            bail!("transaction resource limits must be non-zero");
        }
        Ok(())
    }

    fn validate_bundle(self, bundle: &TransactionBundle, canonical_bytes: &[u8]) -> Result<()> {
        self.validate()?;
        if bundle.point_observations.len() > self.max_point_observations {
            bail!("transaction exceeds point observation limit");
        }
        if bundle.range_observations.len() > self.max_range_observations {
            bail!("transaction exceeds range observation limit");
        }
        if bundle.writes.len() > self.max_written_keys {
            bail!("transaction exceeds written key limit");
        }
        if canonical_bytes.len() > self.max_bundle_bytes {
            bail!("transaction exceeds canonical bundle byte limit");
        }
        let mut raw_payload_bytes = bundle
            .writes
            .iter()
            .map(|write| match write {
                WriteOperation::Put { value, .. } => value.len(),
                WriteOperation::Delete { .. } => 0,
            })
            .chain(bundle.outbox_events.iter().map(Vec::len))
            .chain(bundle.materialisation_jobs.iter().map(Vec::len))
            .try_fold(0usize, |total, length| total.checked_add(length))
            .context("transaction raw payload byte count overflow")?;
        for manifest in &bundle.shard_manifests {
            raw_payload_bytes = raw_payload_bytes
                .checked_add(
                    usize::try_from(manifest.object_length)
                        .context("object payload length exceeds address space")?,
                )
                .context("transaction raw payload byte count overflow")?;
        }
        if raw_payload_bytes > self.max_raw_payload_bytes {
            bail!("transaction exceeds raw payload byte limit");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationRequest {
    pub cluster_id: String,
    pub transaction_id: String,
    pub snapshot_version: CommitVersion,
    pub bundle: BundleIdentity,
    pub durability: DurabilityLevel,
    pub bundle_holders: Vec<BundleDurabilityEvidence>,
    pub object_durability: Vec<ObjectDurabilityEvidence>,
    pub point_observations: Vec<PointObservation>,
    pub range_observations: Vec<RangeObservation>,
    pub advanced_range_stamps: Vec<RangeStampKey>,
    pub written_keys: Vec<LogicalKey>,
    pub max_command_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificationAbort {
    InvalidCommand(String),
    PointConflict { key_hash: [u8; 32] },
    RangeConflict { range_hash: [u8; 32] },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertificationResult {
    Committed { commit_version: CommitVersion },
    Aborted { reason: CertificationAbort },
}

#[async_trait]
pub trait PreparedBundleStore: Send + Sync {
    /// Persist and fsync the complete canonical bundle before returning.
    async fn persist(
        &self,
        identity: &BundleIdentity,
        bytes: &[u8],
    ) -> Result<BundleDurabilityEvidence>;
}

#[async_trait]
pub trait BundleReplicator: Send + Sync {
    /// Stream the bundle and final object shards, returning application-level
    /// durability evidence. Implementations must not report transport delivery
    /// as completed persistence.
    async fn replicate(
        &self,
        identity: &BundleIdentity,
        bytes: &[u8],
        objects: &[ObjectShardManifestReference],
        durability: DurabilityLevel,
    ) -> Result<ReplicationEvidence>;
}

#[async_trait]
pub trait TransactionCertifier: Send + Sync {
    async fn observed_commit_version(&self, consistency: ReadConsistency) -> Result<CommitVersion>;
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResult>;
}

/// Coordinates the data path and the minimal consensus decision.
pub struct TransactionCoordinator<S, R, C> {
    store: S,
    replicator: R,
    certifier: C,
    policy: DurabilityPolicy,
    resource_limits: TransactionResourceLimits,
    ownership_resolver: Arc<dyn ClusterOwnershipResolver>,
}

impl<S, R, C> TransactionCoordinator<S, R, C>
where
    S: PreparedBundleStore,
    R: BundleReplicator,
    C: TransactionCertifier,
{
    pub fn new(store: S, replicator: R, certifier: C, policy: DurabilityPolicy) -> Result<Self> {
        if policy.bundle_quorum_holders == 0 {
            bail!("bundle quorum holder threshold must be non-zero");
        }
        Ok(Self {
            store,
            replicator,
            certifier,
            policy,
            resource_limits: TransactionResourceLimits::default(),
            ownership_resolver: Arc::new(RoutingIssuedOwnership),
        })
    }

    pub fn with_resource_limits(
        mut self,
        resource_limits: TransactionResourceLimits,
    ) -> Result<Self> {
        resource_limits.validate()?;
        self.resource_limits = resource_limits;
        Ok(self)
    }

    pub fn with_ownership_resolver(
        mut self,
        ownership_resolver: Arc<dyn ClusterOwnershipResolver>,
    ) -> Self {
        self.ownership_resolver = ownership_resolver;
        self
    }

    pub async fn snapshot(&self, consistency: ReadConsistency) -> Result<CommitVersion> {
        self.certifier.observed_commit_version(consistency).await
    }

    pub async fn commit(
        &self,
        mut bundle: TransactionBundle,
        durability: DurabilityLevel,
    ) -> Result<CertificationResult> {
        bundle.canonicalize()?;
        for claim in &bundle.ownership_claims {
            self.ownership_resolver
                .validate_claim(&bundle.cluster_id, claim)?;
        }
        let bytes = bundle.canonical_bytes()?;
        self.resource_limits.validate_bundle(&bundle, &bytes)?;
        let identity = bundle.identity()?;

        let local_bundle = self.store.persist(&identity, &bytes).await?;
        let coordinator_incarnation = local_bundle.node.clone();
        let mut evidence = self
            .replicator
            .replicate(&identity, &bytes, &bundle.shard_manifests, durability)
            .await?;
        evidence.bundle_holders.push(local_bundle);
        evidence
            .bundle_holders
            .sort_by(|left, right| left.node.cmp(&right.node));
        evidence
            .bundle_holders
            .dedup_by(|left, right| left.node == right.node);
        self.validate_durability(
            &bundle.cluster_id,
            durability,
            &bundle.shard_manifests,
            &evidence,
            &coordinator_incarnation,
        )?;

        let written_keys = bundle
            .writes
            .iter()
            .map(|operation| operation.key().clone())
            .collect();
        self.certifier
            .certify(CertificationRequest {
                cluster_id: bundle.cluster_id,
                transaction_id: bundle.transaction_id,
                snapshot_version: bundle.snapshot_version,
                bundle: identity,
                durability,
                bundle_holders: evidence.bundle_holders,
                object_durability: evidence.objects,
                point_observations: bundle.point_observations,
                range_observations: bundle.range_observations,
                advanced_range_stamps: bundle.advanced_range_stamps,
                written_keys,
                max_command_bytes: self.resource_limits.max_certification_command_bytes,
            })
            .await
    }

    fn validate_durability(
        &self,
        cluster_id: &str,
        durability: DurabilityLevel,
        objects: &[ObjectShardManifestReference],
        evidence: &ReplicationEvidence,
        coordinator_incarnation: &NodeIncarnation,
    ) -> Result<()> {
        let durable_bundle_nodes = evidence
            .bundle_holders
            .iter()
            .filter(|holder| {
                holder.cluster_id == cluster_id
                    && holder.complete
                    && holder.hash_verified
                    && holder.fsynced
            })
            .map(|holder| &holder.node)
            .collect::<BTreeSet<_>>();
        let required_bundle_holders = match durability {
            DurabilityLevel::Local => 1,
            DurabilityLevel::Quorum | DurabilityLevel::Erasure => self.policy.bundle_quorum_holders,
        };
        if durable_bundle_nodes.len() < required_bundle_holders {
            bail!("transaction bundle durability was not satisfied");
        }

        for object in objects {
            match durability {
                DurabilityLevel::Local => {
                    let locally_durable = evidence.objects.iter().any(|entry| {
                        matches!(
                            entry,
                            ObjectDurabilityEvidence::LocalRepresentation {
                                cluster_id: evidence_cluster,
                                object_hash,
                                node,
                                complete: true,
                                hash_verified: true,
                                fsynced: true,
                                ..
                            } if object_hash == &object.object_hash
                                && evidence_cluster == cluster_id
                                && node == coordinator_incarnation
                        )
                    });
                    if !locally_durable {
                        bail!(
                            "local durability was not satisfied for object {}",
                            object.object_hash
                        );
                    }
                }
                DurabilityLevel::Quorum => {
                    self.validate_shard_placement(cluster_id, object, &evidence.objects, false)?
                }
                DurabilityLevel::Erasure => {
                    self.validate_shard_placement(cluster_id, object, &evidence.objects, true)?
                }
            }
        }
        Ok(())
    }

    fn validate_shard_placement(
        &self,
        cluster_id: &str,
        manifest: &ObjectShardManifestReference,
        evidence: &[ObjectDurabilityEvidence],
        require_complete_plan: bool,
    ) -> Result<()> {
        let object_hash = manifest.object_hash.as_str();
        let mut placements = Vec::new();
        for entry in evidence {
            let ObjectDurabilityEvidence::ShardPlacement {
                cluster_id: evidence_cluster,
                object_hash: placed_object,
                encoding_generation,
                stripe_ordinal,
                shard_ordinal,
                data_shards,
                parity_shards,
                node,
                failure_domain,
                complete,
                hash_verified,
                fsynced,
            } = entry
            else {
                continue;
            };
            if evidence_cluster == cluster_id
                && placed_object == object_hash
                && *complete
                && *hash_verified
                && *fsynced
            {
                placements.push(ShardEvidenceRef {
                    encoding_generation: *encoding_generation,
                    stripe_ordinal: *stripe_ordinal,
                    shard_ordinal: *shard_ordinal,
                    data_shards: *data_shards,
                    parity_shards: *parity_shards,
                    node,
                    failure_domain,
                });
            }
        }
        if placements.is_empty() {
            bail!("no durable shard placement for object {object_hash}");
        }

        let profile = (manifest.data_shards, manifest.parity_shards);
        if profile.0 == 0
            || placements
                .iter()
                .any(|entry| (entry.data_shards, entry.parity_shards) != profile)
        {
            bail!("object {object_hash} has inconsistent erasure profiles");
        }

        let generations = placements
            .iter()
            .map(|entry| entry.encoding_generation)
            .collect::<BTreeSet<_>>();
        if generations != BTreeSet::from([manifest.encoding_generation]) {
            bail!("object {object_hash} has mixed encoding generations");
        }

        let stripe_ordinals = placements
            .iter()
            .map(|entry| entry.stripe_ordinal)
            .collect::<BTreeSet<_>>();
        if stripe_ordinals != (0..manifest.stripe_count).collect::<BTreeSet<_>>() {
            bail!("object {object_hash} has incomplete or unexpected stripe evidence");
        }
        for stripe_ordinal in 0..manifest.stripe_count {
            let stripe = placements
                .iter()
                .copied()
                .filter(|entry| entry.stripe_ordinal == stripe_ordinal)
                .collect::<Vec<_>>();
            validate_unique_shard_targets(object_hash, stripe_ordinal, &stripe)?;
            let planned = profile.0.saturating_add(profile.1);
            if stripe.iter().any(|entry| entry.shard_ordinal >= planned) {
                bail!("object {object_hash} has a shard ordinal outside its k+m profile");
            }
            if require_complete_plan
                && (0..planned)
                    .any(|ordinal| !stripe.iter().any(|entry| entry.shard_ordinal == ordinal))
            {
                bail!(
                    "erasure durability requires complete k+m placement for object {object_hash}"
                );
            }
            if !survives_failure_domains(
                &stripe,
                profile.0 as usize,
                self.policy.tolerated_failure_domains,
            ) {
                bail!(
                    "shard placement for object {object_hash} is not reconstructable after configured failures"
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ShardEvidenceRef<'a> {
    encoding_generation: u64,
    stripe_ordinal: u64,
    shard_ordinal: u16,
    data_shards: u16,
    parity_shards: u16,
    node: &'a NodeIncarnation,
    failure_domain: &'a str,
}

fn validate_unique_shard_targets(
    object_hash: &str,
    stripe_ordinal: u64,
    placements: &[ShardEvidenceRef<'_>],
) -> Result<()> {
    let mut ordinals = BTreeSet::new();
    let mut nodes = BTreeSet::new();
    for placement in placements {
        if !ordinals.insert(placement.shard_ordinal) {
            bail!("object {object_hash} stripe {stripe_ordinal} has duplicate shard ordinal");
        }
        if !nodes.insert(placement.node) {
            bail!("object {object_hash} stripe {stripe_ordinal} places several shards on one node");
        }
    }
    Ok(())
}

fn survives_failure_domains(
    placements: &[ShardEvidenceRef<'_>],
    required_shards: usize,
    tolerated_failures: usize,
) -> bool {
    let domains = placements
        .iter()
        .map(|entry| entry.failure_domain)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let failures = tolerated_failures.min(domains.len());
    failure_combinations(&domains, failures)
        .into_iter()
        .all(|lost| {
            placements
                .iter()
                .filter(|entry| !lost.contains(entry.failure_domain))
                .map(|entry| entry.shard_ordinal)
                .collect::<BTreeSet<_>>()
                .len()
                >= required_shards
        })
}

fn failure_combinations<'a>(domains: &[&'a str], count: usize) -> Vec<BTreeSet<&'a str>> {
    fn visit<'a>(
        domains: &[&'a str],
        count: usize,
        offset: usize,
        current: &mut BTreeSet<&'a str>,
        output: &mut Vec<BTreeSet<&'a str>>,
    ) {
        if current.len() == count {
            output.push(current.clone());
            return;
        }
        for index in offset..domains.len() {
            current.insert(domains[index]);
            visit(domains, count, index + 1, current, output);
            current.remove(domains[index]);
        }
    }

    let mut output = Vec::new();
    visit(domains, count, 0, &mut BTreeSet::new(), &mut output);
    output
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    fn restrictive_limits() -> TransactionResourceLimits {
        TransactionResourceLimits {
            max_point_observations: 1,
            max_range_observations: 1,
            max_written_keys: 1,
            max_certification_command_bytes: 128,
            max_bundle_bytes: 1024,
            max_raw_payload_bytes: 1,
        }
    }

    #[test]
    fn resource_limits_cover_conflict_sets_bundle_and_raw_payload_bytes() {
        let mut builder = TransactionBundleBuilder::new(
            "cluster",
            "limited",
            0,
            "principal",
            HierarchicalRangeStampScheme::new(),
        );
        builder.put(
            LogicalKey {
                table_id: 1,
                application_key: b"key".to_vec(),
            },
            b"two".to_vec(),
        );
        let bundle = builder.build().unwrap();
        let bytes = bundle.canonical_bytes().unwrap();
        let limits = restrictive_limits();
        assert!(
            limits
                .validate_bundle(&bundle, &bytes)
                .unwrap_err()
                .to_string()
                .contains("raw payload byte limit")
        );

        let mut oversized_bundle = limits;
        oversized_bundle.max_raw_payload_bytes = usize::MAX;
        oversized_bundle.max_bundle_bytes = 1;
        assert!(
            oversized_bundle
                .validate_bundle(&bundle, &bytes)
                .unwrap_err()
                .to_string()
                .contains("canonical bundle byte limit")
        );

        let mut two_writes = TransactionBundleBuilder::new(
            "cluster",
            "two-writes",
            0,
            "principal",
            HierarchicalRangeStampScheme::new(),
        );
        two_writes.put(
            LogicalKey {
                table_id: 1,
                application_key: b"a".to_vec(),
            },
            Vec::new(),
        );
        two_writes.put(
            LogicalKey {
                table_id: 1,
                application_key: b"b".to_vec(),
            },
            Vec::new(),
        );
        let two_writes = two_writes.build().unwrap();
        let two_writes_bytes = two_writes.canonical_bytes().unwrap();
        let mut write_limit = TransactionResourceLimits::default();
        write_limit.max_written_keys = 1;
        assert!(
            write_limit
                .validate_bundle(&two_writes, &two_writes_bytes)
                .unwrap_err()
                .to_string()
                .contains("written key limit")
        );

        let mut observations = TransactionBundleBuilder::new(
            "cluster",
            "observations",
            0,
            "principal",
            HierarchicalRangeStampScheme::new(),
        );
        observations
            .observe_point(
                LogicalKey {
                    table_id: 1,
                    application_key: b"a".to_vec(),
                },
                None,
            )
            .observe_point(
                LogicalKey {
                    table_id: 1,
                    application_key: b"b".to_vec(),
                },
                None,
            );
        observations
            .observe_range(1, b"a".to_vec(), b"m".to_vec(), None)
            .unwrap();
        observations
            .observe_range(1, b"m".to_vec(), b"z".to_vec(), None)
            .unwrap();
        let observed = observations.build().unwrap();
        let observed_bytes = observed.canonical_bytes().unwrap();
        let mut point_limit = TransactionResourceLimits::default();
        point_limit.max_point_observations = 1;
        assert!(
            point_limit
                .validate_bundle(&observed, &observed_bytes)
                .unwrap_err()
                .to_string()
                .contains("point observation limit")
        );
        let mut range_limit = TransactionResourceLimits::default();
        range_limit.max_range_observations = 1;
        assert!(
            range_limit
                .validate_bundle(&observed, &observed_bytes)
                .unwrap_err()
                .to_string()
                .contains("range observation limit")
        );
    }

    #[test]
    fn canonical_bundle_rejects_missing_or_forged_ownership_claims() {
        let mut builder = TransactionBundleBuilder::new(
            "cluster",
            "ownership",
            0,
            "principal",
            HierarchicalRangeStampScheme::new(),
        );
        builder.put(
            LogicalKey {
                table_id: 1,
                application_key: b"key".to_vec(),
            },
            b"value".to_vec(),
        );
        let mut missing = builder.build().unwrap();
        missing.ownership_claims.clear();
        assert!(
            missing
                .canonicalize()
                .unwrap_err()
                .to_string()
                .contains("ownership claims")
        );
    }

    struct Store;

    #[async_trait]
    impl PreparedBundleStore for Store {
        async fn persist(
            &self,
            _identity: &BundleIdentity,
            _bytes: &[u8],
        ) -> Result<BundleDurabilityEvidence> {
            Ok(bundle_holder("a", "zone-a"))
        }
    }

    struct Replicator(ReplicationEvidence);

    #[async_trait]
    impl BundleReplicator for Replicator {
        async fn replicate(
            &self,
            _identity: &BundleIdentity,
            _bytes: &[u8],
            _objects: &[ObjectShardManifestReference],
            _durability: DurabilityLevel,
        ) -> Result<ReplicationEvidence> {
            Ok(self.0.clone())
        }
    }

    #[derive(Default)]
    struct Certifier {
        request: Mutex<Option<CertificationRequest>>,
    }

    #[async_trait]
    impl TransactionCertifier for Certifier {
        async fn observed_commit_version(
            &self,
            _consistency: ReadConsistency,
        ) -> Result<CommitVersion> {
            Ok(41)
        }

        async fn certify(&self, request: CertificationRequest) -> Result<CertificationResult> {
            *self.request.lock().unwrap() = Some(request);
            Ok(CertificationResult::Committed { commit_version: 42 })
        }
    }

    fn bundle_holder(node_id: &str, failure_domain: &str) -> BundleDurabilityEvidence {
        BundleDurabilityEvidence {
            cluster_id: "cluster".into(),
            node: NodeIncarnation {
                node_id: node_id.to_string(),
                incarnation: 1,
            },
            failure_domain: failure_domain.to_string(),
            complete: true,
            hash_verified: true,
            fsynced: true,
        }
    }

    fn shard(shard_ordinal: u16, node_id: &str, failure_domain: &str) -> ObjectDurabilityEvidence {
        ObjectDurabilityEvidence::ShardPlacement {
            cluster_id: "cluster".into(),
            object_hash: test_object_hash(),
            encoding_generation: 1,
            stripe_ordinal: 0,
            shard_ordinal,
            data_shards: 2,
            parity_shards: 2,
            node: NodeIncarnation {
                node_id: node_id.to_string(),
                incarnation: 1,
            },
            failure_domain: failure_domain.to_string(),
            complete: true,
            hash_verified: true,
            fsynced: true,
        }
    }

    fn test_object_hash() -> String {
        format!("sha256:{}", "a".repeat(64))
    }

    struct RejectTableNine;

    impl ClusterOwnershipResolver for RejectTableNine {
        fn validate_claim(
            &self,
            _transaction_cluster_id: &str,
            claim: &ClusterOwnershipClaim,
        ) -> Result<()> {
            if matches!(
                claim.resource(),
                OwnedResource::LogicalKey(LogicalKey { table_id: 9, .. })
            ) {
                bail!("routing resolved resource to another cluster");
            }
            Ok(())
        }
    }

    fn bundle(with_object: bool) -> TransactionBundle {
        let mut builder = TransactionBundleBuilder::new(
            "cluster",
            "tx-1",
            41,
            "tenant/1/principal/app",
            HierarchicalRangeStampScheme::new(),
        );
        builder.put(
            LogicalKey {
                table_id: 9,
                application_key: b"partition-b/key".to_vec(),
            },
            b"second".to_vec(),
        );
        builder.put(
            LogicalKey {
                table_id: 3,
                application_key: b"partition-a/key".to_vec(),
            },
            b"first".to_vec(),
        );
        if with_object {
            builder.add_shard_manifest(ObjectShardManifestReference {
                object_hash: test_object_hash(),
                manifest_hash: format!("sha256:{}", "b".repeat(64)),
                object_length: 1024,
                encoding_generation: 1,
                data_shards: 2,
                parity_shards: 2,
                stripe_count: 1,
            });
        }
        builder.build().unwrap()
    }

    #[tokio::test]
    async fn one_certification_covers_unrelated_tables_and_partitions() {
        let coordinator = TransactionCoordinator::new(
            Store,
            Replicator(ReplicationEvidence {
                bundle_holders: vec![bundle_holder("b", "zone-b")],
                objects: Vec::new(),
            }),
            Certifier::default(),
            DurabilityPolicy {
                bundle_quorum_holders: 2,
                tolerated_failure_domains: 1,
            },
        )
        .unwrap();

        let result = coordinator
            .commit(bundle(false), DurabilityLevel::Quorum)
            .await
            .unwrap();
        assert_eq!(
            result,
            CertificationResult::Committed { commit_version: 42 }
        );
        let request = coordinator.certifier.request.lock().unwrap();
        let request = request.as_ref().unwrap();
        assert_eq!(request.written_keys.len(), 2);
        assert_eq!(request.written_keys[0].table_id, 3);
        assert_eq!(request.written_keys[1].table_id, 9);
    }

    #[tokio::test]
    async fn routing_resolver_rejects_foreign_resource_before_preparation() {
        let coordinator = TransactionCoordinator::new(
            Store,
            Replicator(ReplicationEvidence::default()),
            Certifier::default(),
            DurabilityPolicy {
                bundle_quorum_holders: 1,
                tolerated_failure_domains: 0,
            },
        )
        .unwrap()
        .with_ownership_resolver(Arc::new(RejectTableNine));

        let error = coordinator
            .commit(bundle(false), DurabilityLevel::Local)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("another cluster"));
    }

    #[tokio::test]
    async fn consensus_is_not_called_before_quorum_durability() {
        let coordinator = TransactionCoordinator::new(
            Store,
            Replicator(ReplicationEvidence::default()),
            Certifier::default(),
            DurabilityPolicy {
                bundle_quorum_holders: 2,
                tolerated_failure_domains: 1,
            },
        )
        .unwrap();

        let error = coordinator
            .commit(bundle(false), DurabilityLevel::Quorum)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("bundle durability"));
        assert!(coordinator.certifier.request.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn quorum_requires_reconstruction_after_each_tolerated_domain_loss() {
        let unsafe_coordinator = TransactionCoordinator::new(
            Store,
            Replicator(ReplicationEvidence {
                bundle_holders: vec![bundle_holder("b", "zone-b")],
                objects: vec![
                    shard(0, "a", "zone-a"),
                    shard(1, "b", "zone-a"),
                    shard(2, "c", "zone-b"),
                ],
            }),
            Certifier::default(),
            DurabilityPolicy {
                bundle_quorum_holders: 2,
                tolerated_failure_domains: 1,
            },
        )
        .unwrap();
        let error = unsafe_coordinator
            .commit(bundle(true), DurabilityLevel::Quorum)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not reconstructable"));
        assert!(
            unsafe_coordinator
                .certifier
                .request
                .lock()
                .unwrap()
                .is_none()
        );

        let safe_coordinator = TransactionCoordinator::new(
            Store,
            Replicator(ReplicationEvidence {
                bundle_holders: vec![bundle_holder("b", "zone-b")],
                objects: vec![
                    shard(0, "a", "zone-a"),
                    shard(1, "b", "zone-b"),
                    shard(2, "c", "zone-c"),
                ],
            }),
            Certifier::default(),
            DurabilityPolicy {
                bundle_quorum_holders: 2,
                tolerated_failure_domains: 1,
            },
        )
        .unwrap();
        assert_eq!(
            safe_coordinator
                .commit(bundle(true), DurabilityLevel::Quorum)
                .await
                .unwrap(),
            CertificationResult::Committed { commit_version: 42 }
        );
    }

    #[tokio::test]
    async fn erasure_requires_every_planned_shard() {
        let coordinator = TransactionCoordinator::new(
            Store,
            Replicator(ReplicationEvidence {
                bundle_holders: vec![bundle_holder("b", "zone-b")],
                objects: vec![
                    shard(0, "a", "zone-a"),
                    shard(1, "b", "zone-b"),
                    shard(2, "c", "zone-c"),
                ],
            }),
            Certifier::default(),
            DurabilityPolicy {
                bundle_quorum_holders: 2,
                tolerated_failure_domains: 1,
            },
        )
        .unwrap();
        let error = coordinator
            .commit(bundle(true), DurabilityLevel::Erasure)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("complete k+m placement"));

        let complete = TransactionCoordinator::new(
            Store,
            Replicator(ReplicationEvidence {
                bundle_holders: vec![bundle_holder("b", "zone-b")],
                objects: vec![
                    shard(0, "a", "zone-a"),
                    shard(1, "b", "zone-b"),
                    shard(2, "c", "zone-c"),
                    shard(3, "d", "zone-d"),
                ],
            }),
            Certifier::default(),
            DurabilityPolicy {
                bundle_quorum_holders: 2,
                tolerated_failure_domains: 1,
            },
        )
        .unwrap();
        assert_eq!(
            complete
                .commit(bundle(true), DurabilityLevel::Erasure)
                .await
                .unwrap(),
            CertificationResult::Committed { commit_version: 42 }
        );
    }

    #[tokio::test]
    async fn local_requires_verified_fsynced_local_representation() {
        let coordinator = TransactionCoordinator::new(
            Store,
            Replicator(ReplicationEvidence {
                bundle_holders: Vec::new(),
                objects: vec![ObjectDurabilityEvidence::LocalRepresentation {
                    cluster_id: "cluster".into(),
                    object_hash: test_object_hash(),
                    node: NodeIncarnation {
                        node_id: "a".to_string(),
                        incarnation: 1,
                    },
                    failure_domain: "zone-a".to_string(),
                    complete: true,
                    hash_verified: true,
                    fsynced: true,
                }],
            }),
            Certifier::default(),
            DurabilityPolicy {
                bundle_quorum_holders: 2,
                tolerated_failure_domains: 1,
            },
        )
        .unwrap();
        assert_eq!(
            coordinator
                .commit(bundle(true), DurabilityLevel::Local)
                .await
                .unwrap(),
            CertificationResult::Committed { commit_version: 42 }
        );
    }

    #[test]
    fn canonical_identity_does_not_depend_on_input_write_order() {
        let first = bundle(false);
        let mut second = first.clone();
        second.writes.reverse();
        assert_eq!(first.identity().unwrap(), second.identity().unwrap());
    }

    #[test]
    fn hierarchical_scan_stamp_is_advanced_by_every_overlapping_write() {
        let scheme = HierarchicalRangeStampScheme::new();
        let observed = scheme
            .observation_key(7, Some(b"orders/a"), Some(b"orders/z"))
            .unwrap();
        assert_eq!(observed.key_prefix, b"orders/".to_vec());

        let overlapping = LogicalKey {
            table_id: 7,
            application_key: b"orders/m".to_vec(),
        };
        assert!(scheme.write_keys(&overlapping).contains(&observed));

        let unrelated = LogicalKey {
            table_id: 7,
            application_key: b"profiles/m".to_vec(),
        };
        assert!(!scheme.write_keys(&unrelated).contains(&observed));
        let full_table = scheme.observation_key(7, None, None).unwrap();
        assert!(full_table.key_prefix.is_empty());
        assert!(scheme.write_keys(&unrelated).contains(&full_table));
    }

    #[test]
    fn builder_advances_delete_and_cross_table_rename_stamps() {
        let scheme = HierarchicalRangeStampScheme::new();
        let old_key = LogicalKey {
            table_id: 3,
            application_key: b"old/key".to_vec(),
        };
        let new_key = LogicalKey {
            table_id: 9,
            application_key: b"new/key".to_vec(),
        };
        let mut builder = TransactionBundleBuilder::new(
            "cluster",
            "rename",
            10,
            "tenant/1/principal/app",
            scheme,
        );
        builder.rename(old_key.clone(), new_key.clone(), b"value".to_vec());
        let bundle = builder.build().unwrap();

        assert_eq!(bundle.writes.len(), 2);
        assert!(
            scheme
                .write_keys(&old_key)
                .iter()
                .all(|stamp| bundle.advanced_range_stamps.contains(stamp))
        );
        assert!(
            scheme
                .write_keys(&new_key)
                .iter()
                .all(|stamp| bundle.advanced_range_stamps.contains(stamp))
        );
        assert!(bundle.advanced_range_stamps.contains(&RangeStampKey {
            scheme_version: HierarchicalRangeStampScheme::SCHEME_VERSION,
            table_id: 3,
            key_prefix: Vec::new(),
        }));
        assert!(bundle.advanced_range_stamps.contains(&RangeStampKey {
            scheme_version: HierarchicalRangeStampScheme::SCHEME_VERSION,
            table_id: 9,
            key_prefix: Vec::new(),
        }));
    }
}
