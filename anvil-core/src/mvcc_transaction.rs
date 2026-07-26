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
pub struct RawObjectReference {
    pub content_hash: String,
    pub payload_length: u64,
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
    pub writes: Vec<WriteOperation>,
    pub raw_objects: Vec<RawObjectReference>,
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

        self.writes
            .sort_by(|left, right| left.key().cmp(right.key()));
        ensure_unique(self.writes.iter().map(WriteOperation::key), "written key")?;
        self.raw_objects
            .sort_by(|left, right| left.content_hash.cmp(&right.content_hash));
        ensure_unique(
            self.raw_objects.iter().map(|entry| &entry.content_hash),
            "raw object",
        )?;
        for raw_object in &self.raw_objects {
            if !is_sha256_hash(&raw_object.content_hash) {
                bail!("raw object content hash must be a sha256 hash");
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleIdentity {
    pub hash: String,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeIncarnation {
    pub node_id: String,
    pub incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableRepresentation {
    Raw,
    Erasure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableHolder {
    pub node: NodeIncarnation,
    pub representation: DurableRepresentation,
}

#[derive(Debug, Clone)]
pub struct CertificationRequest {
    pub transaction_id: String,
    pub snapshot_version: CommitVersion,
    pub bundle: BundleIdentity,
    pub durability: DurabilityLevel,
    pub durable_holders: Vec<DurableHolder>,
    pub point_observations: Vec<PointObservation>,
    pub range_observations: Vec<RangeObservation>,
    pub written_keys: Vec<LogicalKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertificationResult {
    Committed { commit_version: CommitVersion },
    Aborted { conflicts: Vec<LogicalKey> },
}

#[async_trait]
pub trait PreparedBundleStore: Send + Sync {
    /// Persist and fsync the complete canonical bundle before returning.
    async fn persist(&self, identity: &BundleIdentity, bytes: &[u8]) -> Result<DurableHolder>;
}

#[async_trait]
pub trait BundleReplicator: Send + Sync {
    /// Return only complete, hash-verified, fsynced remote holders.
    async fn replicate(
        &self,
        identity: &BundleIdentity,
        bytes: &[u8],
        durability: DurabilityLevel,
    ) -> Result<Vec<DurableHolder>>;
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
    quorum_holders: usize,
    erasure_holders: usize,
}

impl<S, R, C> TransactionCoordinator<S, R, C>
where
    S: PreparedBundleStore,
    R: BundleReplicator,
    C: TransactionCertifier,
{
    pub fn new(
        store: S,
        replicator: R,
        certifier: C,
        quorum_holders: usize,
        erasure_holders: usize,
    ) -> Result<Self> {
        if quorum_holders == 0 || erasure_holders == 0 {
            bail!("durability holder thresholds must be non-zero");
        }
        Ok(Self {
            store,
            replicator,
            certifier,
            quorum_holders,
            erasure_holders,
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

        let local = self.store.persist(&identity, &bytes).await?;
        let mut holders = vec![local];
        holders.extend(
            self.replicator
                .replicate(&identity, &bytes, durability)
                .await?,
        );
        // Prefer erasure evidence when the same node reports both its raw
        // receipt and final shard receipt.
        holders.sort_by(|left, right| {
            left.node.cmp(&right.node).then_with(|| {
                representation_rank(right.representation)
                    .cmp(&representation_rank(left.representation))
            })
        });
        holders.dedup_by(|left, right| left.node == right.node);
        self.validate_durability(durability, &holders)?;

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
                durable_holders: holders,
                point_observations: bundle.point_observations,
                range_observations: bundle.range_observations,
                written_keys,
            })
            .await
    }

    fn validate_durability(
        &self,
        durability: DurabilityLevel,
        holders: &[DurableHolder],
    ) -> Result<()> {
        let distinct_nodes = holders
            .iter()
            .map(|holder| &holder.node)
            .collect::<BTreeSet<_>>()
            .len();
        match durability {
            DurabilityLevel::Local if distinct_nodes >= 1 => Ok(()),
            DurabilityLevel::Quorum if distinct_nodes >= self.quorum_holders => Ok(()),
            DurabilityLevel::Erasure
                if holders
                    .iter()
                    .filter(|holder| holder.representation == DurableRepresentation::Erasure)
                    .count()
                    >= self.erasure_holders =>
            {
                Ok(())
            }
            DurabilityLevel::Local => bail!("local durability was not satisfied"),
            DurabilityLevel::Quorum => bail!("quorum durability was not satisfied"),
            DurabilityLevel::Erasure => bail!("erasure durability was not satisfied"),
        }
    }
}

fn representation_rank(representation: DurableRepresentation) -> u8 {
    match representation {
        DurableRepresentation::Raw => 0,
        DurableRepresentation::Erasure => 1,
    }
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
        ) -> Result<DurableHolder> {
            Ok(holder("a", DurableRepresentation::Raw))
        }
    }

    struct Replicator(Vec<DurableHolder>);

    #[async_trait]
    impl BundleReplicator for Replicator {
        async fn replicate(
            &self,
            _identity: &BundleIdentity,
            _bytes: &[u8],
            _durability: DurabilityLevel,
        ) -> Result<Vec<DurableHolder>> {
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

    fn holder(node_id: &str, representation: DurableRepresentation) -> DurableHolder {
        DurableHolder {
            node: NodeIncarnation {
                node_id: node_id.to_string(),
                incarnation: 1,
            },
            representation,
        }
    }

    fn bundle() -> TransactionBundle {
        TransactionBundle {
            schema: TransactionBundle::SCHEMA.to_string(),
            transaction_id: "tx-1".to_string(),
            snapshot_version: 41,
            authenticated_principal: "tenant/1/principal/app".to_string(),
            point_observations: Vec::new(),
            range_observations: Vec::new(),
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
            raw_objects: Vec::new(),
            outbox_events: Vec::new(),
            materialisation_jobs: Vec::new(),
        }
    }

    #[tokio::test]
    async fn one_certification_covers_unrelated_tables_and_partitions() {
        let coordinator = TransactionCoordinator::new(
            Store,
            Replicator(vec![holder("b", DurableRepresentation::Raw)]),
            Certifier::default(),
            2,
            2,
        )
        .unwrap();

        let result = coordinator
            .commit(bundle(), DurabilityLevel::Quorum)
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
        let coordinator =
            TransactionCoordinator::new(Store, Replicator(Vec::new()), Certifier::default(), 2, 2)
                .unwrap();

        let error = coordinator
            .commit(bundle(), DurabilityLevel::Quorum)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("quorum durability"));
        assert!(coordinator.certifier.request.lock().unwrap().is_none());
    }

    #[test]
    fn canonical_identity_does_not_depend_on_input_write_order() {
        let first = bundle();
        let mut second = first.clone();
        second.writes.reverse();
        assert_eq!(first.identity().unwrap(), second.identity().unwrap());
    }
}
