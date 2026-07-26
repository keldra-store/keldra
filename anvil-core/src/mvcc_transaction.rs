//! Transaction-coordinator boundary for the MVCC-under-Raft architecture.
//!
//! Product services build one immutable [`TransactionBundle`] containing every
//! logical mutation. Bundle persistence and replication happen outside Raft;
//! only [`CertificationRequest`] is submitted to consensus.

use std::collections::BTreeSet;

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
    pub start_application_key: Vec<u8>,
    pub end_application_key: Vec<u8>,
    pub observed_range_stamp: CommitVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RangeConflict {
    pub table_id: u16,
    pub start_application_key: Vec<u8>,
    pub end_application_key: Vec<u8>,
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

/// Canonically encoded transaction data persisted and replicated outside Raft.
///
/// A bundle deliberately has no partition or publication scope. One bundle may
/// contain keys from unrelated tables and physical partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionBundle {
    pub schema: String,
    pub transaction_id: String,
    pub snapshot_version: CommitVersion,
    pub authenticated_principal: String,
    pub point_observations: Vec<PointObservation>,
    pub range_observations: Vec<RangeObservation>,
    pub advanced_range_stamps: Vec<RangeConflict>,
    pub writes: Vec<WriteOperation>,
    pub shard_manifests: Vec<ObjectShardManifestReference>,
    pub outbox_events: Vec<Vec<u8>>,
    pub materialisation_jobs: Vec<Vec<u8>>,
}

impl TransactionBundle {
    pub const SCHEMA: &'static str = "anvil.mvcc.transaction-bundle.v1";

    pub fn canonicalize(&mut self) -> Result<()> {
        if self.schema != Self::SCHEMA {
            bail!("unsupported transaction bundle schema");
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
            )
                .cmp(&(
                    right.table_id,
                    &right.start_application_key,
                    &right.end_application_key,
                ))
        });
        ensure_unique_by(
            self.range_observations.iter(),
            |entry| {
                (
                    entry.table_id,
                    entry.start_application_key.as_slice(),
                    entry.end_application_key.as_slice(),
                )
            },
            "range observation",
        )?;
        for observation in &self.range_observations {
            if observation.start_application_key >= observation.end_application_key {
                bail!("range observation must be a non-empty half-open interval");
            }
        }
        self.advanced_range_stamps.sort();
        ensure_unique(
            self.advanced_range_stamps.iter(),
            "advanced range conflict stamp",
        )?;
        for range in &self.advanced_range_stamps {
            if range.start_application_key >= range.end_application_key {
                bail!("advanced range stamp must be a non-empty half-open interval");
            }
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
        object_hash: String,
        node: NodeIncarnation,
        failure_domain: String,
        complete: bool,
        hash_verified: bool,
        fsynced: bool,
    },
    ShardPlacement {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificationRequest {
    pub transaction_id: String,
    pub snapshot_version: CommitVersion,
    pub bundle: BundleIdentity,
    pub durability: DurabilityLevel,
    pub bundle_holders: Vec<BundleDurabilityEvidence>,
    pub object_durability: Vec<ObjectDurabilityEvidence>,
    pub point_observations: Vec<PointObservation>,
    pub range_observations: Vec<RangeObservation>,
    pub advanced_range_stamps: Vec<RangeConflict>,
    pub written_keys: Vec<LogicalKey>,
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
        })
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
        let bytes = bundle.canonical_bytes()?;
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
            })
            .await
    }

    fn validate_durability(
        &self,
        durability: DurabilityLevel,
        objects: &[ObjectShardManifestReference],
        evidence: &ReplicationEvidence,
        coordinator_incarnation: &NodeIncarnation,
    ) -> Result<()> {
        let durable_bundle_nodes = evidence
            .bundle_holders
            .iter()
            .filter(|holder| holder.complete && holder.hash_verified && holder.fsynced)
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
                                object_hash,
                                node,
                                complete: true,
                                hash_verified: true,
                                fsynced: true,
                                ..
                            } if object_hash == &object.object_hash
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
                    self.validate_shard_placement(object, &evidence.objects, false)?
                }
                DurabilityLevel::Erasure => {
                    self.validate_shard_placement(object, &evidence.objects, true)?
                }
            }
        }
        Ok(())
    }

    fn validate_shard_placement(
        &self,
        manifest: &ObjectShardManifestReference,
        evidence: &[ObjectDurabilityEvidence],
        require_complete_plan: bool,
    ) -> Result<()> {
        let object_hash = manifest.object_hash.as_str();
        let mut placements = Vec::new();
        for entry in evidence {
            let ObjectDurabilityEvidence::ShardPlacement {
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
            if placed_object == object_hash && *complete && *hash_verified && *fsynced {
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

    fn bundle(with_object: bool) -> TransactionBundle {
        TransactionBundle {
            schema: TransactionBundle::SCHEMA.to_string(),
            transaction_id: "tx-1".to_string(),
            snapshot_version: 41,
            authenticated_principal: "tenant/1/principal/app".to_string(),
            point_observations: Vec::new(),
            range_observations: Vec::new(),
            advanced_range_stamps: Vec::new(),
            writes: vec![
                WriteOperation::Put {
                    key: LogicalKey {
                        table_id: 9,
                        application_key: b"partition-b/key".to_vec(),
                    },
                    value: b"second".to_vec(),
                },
                WriteOperation::Put {
                    key: LogicalKey {
                        table_id: 3,
                        application_key: b"partition-a/key".to_vec(),
                    },
                    value: b"first".to_vec(),
                },
            ],
            shard_manifests: if with_object {
                vec![ObjectShardManifestReference {
                    object_hash: test_object_hash(),
                    manifest_hash: format!("sha256:{}", "b".repeat(64)),
                    object_length: 1024,
                    encoding_generation: 1,
                    data_shards: 2,
                    parity_shards: 2,
                    stripe_count: 1,
                }]
            } else {
                Vec::new()
            },
            outbox_events: Vec::new(),
            materialisation_jobs: Vec::new(),
        }
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
}
