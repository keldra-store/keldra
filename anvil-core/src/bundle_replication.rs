//! Concrete prepared-bundle persistence and replication adapters.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::{
    mvcc_transaction::{
        BundleDurabilityEvidence, BundleIdentity, BundleReplicator, DurabilityLevel,
        NodeIncarnation, ObjectDurabilityEvidence, ObjectShardManifestReference,
        PreparedBundleStore, ReplicationEvidence, TransactionBundle,
    },
    replication::{AckStatus, ReplicationAck},
    shard_placement::DistributedIngestResult,
};

const MAGIC: &[u8; 8] = b"ANVBND01";
const VERSION: u16 = 2;
const HEADER: usize = 8 + 2 + 32 + 8 + 8;
const TRAILER: usize = 32;

#[derive(Clone, Copy)]
struct BundleLocation {
    payload_offset: u64,
    payload_length: u64,
    prepared_at_unix_ms: u64,
}

struct PreparedBundleLog {
    path: std::path::PathBuf,
    file: File,
    index: BTreeMap<String, BundleLocation>,
}

impl PreparedBundleLog {
    fn open(directory: &Path) -> Result<Self> {
        fs::create_dir_all(directory)?;
        let path = directory.join("prepared-bundles.log");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let index = recover(&mut file)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self { path, file, index })
    }

    fn persist(&mut self, identity: &BundleIdentity, bytes: &[u8]) -> Result<()> {
        verify_identity(identity, bytes)?;
        if let Some(location) = self.index.get(&identity.hash).copied() {
            if location.payload_length != identity.length {
                bail!("prepared bundle hash was reused with a different length");
            }
            self.file.seek(SeekFrom::Start(location.payload_offset))?;
            let mut existing = vec![0; location.payload_length as usize];
            self.file.read_exact(&mut existing)?;
            verify_identity(identity, &existing)?;
            return Ok(());
        }
        let original_len = self.file.metadata()?.len();
        let append_result = append_record(
            &mut self.file,
            identity,
            bytes,
            unix_time_ms()?,
            &mut self.index,
        )
        .and_then(|()| {
            #[cfg(any(test, debug_assertions))]
            crate::mvcc_fault_injection::hit(
                crate::mvcc_fault_injection::FaultPoint::PreparedBundleWrite,
            )?;
            self.file.sync_data().map_err(Into::into)
        });
        if let Err(error) = append_result {
            self.index.remove(&identity.hash);
            self.file
                .set_len(original_len)
                .context("rollback failed prepared-bundle append")?;
            self.file
                .sync_data()
                .context("sync prepared-bundle append rollback")?;
            self.file
                .seek(SeekFrom::End(0))
                .context("restore prepared-bundle append cursor")?;
            return Err(error);
        }
        Ok(())
    }

    fn read(&mut self, identity: &BundleIdentity) -> Result<Option<Vec<u8>>> {
        let Some(location) = self.index.get(&identity.hash).copied() else {
            return Ok(None);
        };
        if location.payload_length != identity.length {
            bail!("prepared bundle length differs from immutable identity");
        }
        self.file.seek(SeekFrom::Start(location.payload_offset))?;
        let mut bytes = vec![0; location.payload_length as usize];
        self.file.read_exact(&mut bytes)?;
        verify_identity(identity, &bytes)?;
        Ok(Some(bytes))
    }

    fn compact(&mut self, retained_hashes: &BTreeSet<String>) -> Result<usize> {
        let removed = self
            .index
            .keys()
            .filter(|hash| !retained_hashes.contains(*hash))
            .count();
        if removed == 0 {
            return Ok(0);
        }

        let temporary_path = self.path.with_extension("log.compacting");
        let mut replacement = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&temporary_path)?;
        let mut replacement_index = BTreeMap::new();
        for (hash, location) in self.index.clone() {
            if !retained_hashes.contains(&hash) {
                continue;
            }
            self.file.seek(SeekFrom::Start(location.payload_offset))?;
            let mut bytes = vec![0; location.payload_length as usize];
            self.file.read_exact(&mut bytes)?;
            let identity = BundleIdentity {
                hash: hash.clone(),
                length: location.payload_length,
            };
            verify_identity(&identity, &bytes)?;
            append_record(
                &mut replacement,
                &identity,
                &bytes,
                location.prepared_at_unix_ms,
                &mut replacement_index,
            )?;
        }
        replacement.sync_all()?;
        fs::rename(&temporary_path, &self.path)?;
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.file.seek(SeekFrom::End(0))?;
        self.index = replacement_index;
        Ok(removed)
    }
}

fn append_record(
    file: &mut File,
    identity: &BundleIdentity,
    bytes: &[u8],
    prepared_at_unix_ms: u64,
    index: &mut BTreeMap<String, BundleLocation>,
) -> Result<()> {
    let hash = parse_sha256(&identity.hash)?;
    let offset = file.seek(SeekFrom::End(0))?;
    let mut record = Vec::with_capacity(HEADER + bytes.len() + TRAILER);
    record.extend_from_slice(MAGIC);
    record.extend_from_slice(&VERSION.to_be_bytes());
    record.extend_from_slice(&hash);
    record.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    record.extend_from_slice(&prepared_at_unix_ms.to_be_bytes());
    record.extend_from_slice(bytes);
    let checksum: [u8; 32] = Sha256::digest(&record).into();
    record.extend_from_slice(&checksum);
    file.write_all(&record)?;
    index.insert(
        identity.hash.clone(),
        BundleLocation {
            payload_offset: offset + HEADER as u64,
            payload_length: bytes.len() as u64,
            prepared_at_unix_ms,
        },
    );
    Ok(())
}

fn unix_time_ms() -> Result<u64> {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )
    .context("system time exceeds u64 milliseconds")
}

#[derive(Clone)]
pub struct AppendOnlyPreparedBundleStore {
    log: Arc<Mutex<PreparedBundleLog>>,
    cluster_id: String,
    node: NodeIncarnation,
    failure_domain: String,
}

impl AppendOnlyPreparedBundleStore {
    pub fn open(
        directory: impl AsRef<Path>,
        cluster_id: impl Into<String>,
        node: NodeIncarnation,
        failure_domain: impl Into<String>,
    ) -> Result<Self> {
        let cluster_id = cluster_id.into();
        let failure_domain = failure_domain.into();
        if cluster_id.trim().is_empty()
            || node.node_id.trim().is_empty()
            || node.incarnation == 0
            || failure_domain.trim().is_empty()
        {
            bail!("prepared bundle store requires a valid node incarnation and failure domain");
        }
        Ok(Self {
            log: Arc::new(Mutex::new(PreparedBundleLog::open(directory.as_ref())?)),
            cluster_id,
            node,
            failure_domain,
        })
    }

    pub fn read(&self, identity: &BundleIdentity) -> Result<Option<Vec<u8>>> {
        self.log
            .lock()
            .map_err(|_| anyhow::anyhow!("prepared bundle log lock poisoned"))?
            .read(identity)
    }

    pub(crate) fn identities(&self) -> Result<Vec<BundleIdentity>> {
        let log = self
            .log
            .lock()
            .map_err(|_| anyhow::anyhow!("prepared bundle log lock poisoned"))?;
        Ok(log
            .index
            .iter()
            .map(|(hash, location)| BundleIdentity {
                hash: hash.clone(),
                length: location.payload_length,
            })
            .collect())
    }

    /// Rewrites the append-only log retaining exactly the bundle identities
    /// authorised by a cluster-wide GC plan.
    ///
    /// This method deliberately accepts no age or watermark: a local node
    /// cannot prove that an unlisted prepared bundle has expired or that no
    /// active certification/catch-up attempt still references it.
    pub fn compact_authorised(&self, retained_identities: &[BundleIdentity]) -> Result<usize> {
        let mut log = self
            .log
            .lock()
            .map_err(|_| anyhow::anyhow!("prepared bundle log lock poisoned"))?;
        let mut retained_hashes = BTreeSet::new();
        for identity in retained_identities {
            if let Some(location) = log.index.get(&identity.hash)
                && location.payload_length != identity.length
            {
                bail!("authorised prepared bundle identity has the wrong length");
            }
            retained_hashes.insert(identity.hash.clone());
        }
        log.compact(&retained_hashes)
    }

    pub fn retain_plan(
        &self,
        committed: &[BundleIdentity],
        pinned_transaction_ids: &BTreeSet<String>,
        now_unix_ms: u64,
        preparation_grace_ms: u64,
    ) -> Result<Vec<BundleIdentity>> {
        let mut log = self
            .log
            .lock()
            .map_err(|_| anyhow::anyhow!("prepared bundle log lock poisoned"))?;
        let committed = committed
            .iter()
            .map(|identity| (identity.hash.as_str(), identity.length))
            .collect::<BTreeMap<_, _>>();
        let locations = log.index.clone();
        let mut retained = Vec::new();
        for (hash, location) in locations {
            let identity = BundleIdentity {
                hash,
                length: location.payload_length,
            };
            let explicitly_committed = match committed.get(identity.hash.as_str()) {
                Some(length) if *length == identity.length => true,
                Some(_) => bail!("committed bundle retain evidence has the wrong length"),
                None => false,
            };
            let grace_deadline = location
                .prepared_at_unix_ms
                .checked_add(preparation_grace_ms)
                .context("prepared bundle grace deadline overflow")?;
            let recent = now_unix_ms < grace_deadline;
            let pinned_or_ambiguous = match log.read(&identity) {
                Ok(Some(bytes)) => serde_json::from_slice::<TransactionBundle>(&bytes)
                    .map(|bundle| pinned_transaction_ids.contains(&bundle.transaction_id))
                    .unwrap_or(true),
                Ok(None) | Err(_) => true,
            };
            if explicitly_committed || recent || pinned_or_ambiguous {
                retained.push(identity);
            }
        }
        retained.sort_by(|left, right| left.hash.cmp(&right.hash));
        Ok(retained)
    }
}

#[async_trait]
impl PreparedBundleStore for AppendOnlyPreparedBundleStore {
    async fn persist(
        &self,
        identity: &BundleIdentity,
        bytes: &[u8],
    ) -> Result<BundleDurabilityEvidence> {
        let log = self.log.clone();
        let identity = identity.clone();
        let bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || {
            log.lock()
                .map_err(|_| anyhow::anyhow!("prepared bundle log lock poisoned"))?
                .persist(&identity, &bytes)
        })
        .await??;
        Ok(BundleDurabilityEvidence {
            cluster_id: self.cluster_id.clone(),
            node: self.node.clone(),
            failure_domain: self.failure_domain.clone(),
            complete: true,
            hash_verified: true,
            fsynced: true,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BundleTarget {
    pub cluster_id: String,
    pub node: NodeIncarnation,
    pub failure_domain: String,
    /// Whether this target is a voter in the membership used for selection.
    ///
    /// Learners may hold opportunistic copies, but voters are attempted first.
    /// The Raft state machine revalidates holder safety against its
    /// authoritative applied membership.
    pub voter: bool,
}

#[async_trait]
pub trait BundleTargetStream: Send + Sync {
    async fn send_bundle(
        &self,
        target: &BundleTarget,
        identity: &BundleIdentity,
        bytes: &[u8],
    ) -> Result<ReplicationAck>;
}

/// Holds only evidence produced by successful ingest/local persistence.
#[derive(Clone, Default)]
pub struct ObjectEvidenceRegistry {
    evidence: Arc<Mutex<BTreeMap<String, Vec<ObjectDurabilityEvidence>>>>,
}

impl ObjectEvidenceRegistry {
    pub fn record_ingest(&self, result: &DistributedIngestResult) -> Result<()> {
        for evidence in &result.evidence {
            let ObjectDurabilityEvidence::ShardPlacement { object_hash, .. } = evidence else {
                bail!("distributed ingest returned non-shard evidence");
            };
            self.record(object_hash, evidence.clone())?;
        }
        Ok(())
    }

    pub fn record(&self, object_hash: &str, evidence: ObjectDurabilityEvidence) -> Result<()> {
        let evidence_hash = match &evidence {
            ObjectDurabilityEvidence::LocalRepresentation { object_hash, .. }
            | ObjectDurabilityEvidence::ShardPlacement { object_hash, .. } => object_hash,
        };
        if evidence_hash != object_hash {
            bail!("object durability evidence was registered under another object hash");
        }
        let mut registry = self
            .evidence
            .lock()
            .map_err(|_| anyhow::anyhow!("object evidence registry lock poisoned"))?;
        let entries = registry.entry(object_hash.to_string()).or_default();
        if !entries.contains(&evidence) {
            entries.push(evidence);
        }
        Ok(())
    }

    fn evidence_for(
        &self,
        manifests: &[ObjectShardManifestReference],
    ) -> Result<Vec<ObjectDurabilityEvidence>> {
        let registry = self
            .evidence
            .lock()
            .map_err(|_| anyhow::anyhow!("object evidence registry lock poisoned"))?;
        let mut result = Vec::new();
        for manifest in manifests {
            let entries = registry.get(&manifest.object_hash).with_context(|| {
                format!("no ingest evidence for object {}", manifest.object_hash)
            })?;
            let matching = entries.iter().filter(|entry| match entry {
                ObjectDurabilityEvidence::LocalRepresentation { object_hash, .. } => {
                    object_hash == &manifest.object_hash
                }
                ObjectDurabilityEvidence::ShardPlacement {
                    object_hash,
                    encoding_generation,
                    data_shards,
                    parity_shards,
                    stripe_ordinal,
                    ..
                } => {
                    object_hash == &manifest.object_hash
                        && *encoding_generation == manifest.encoding_generation
                        && *data_shards == manifest.data_shards
                        && *parity_shards == manifest.parity_shards
                        && *stripe_ordinal < manifest.stripe_count
                }
            });
            result.extend(matching.cloned());
        }
        Ok(result)
    }
}

#[derive(Clone)]
pub struct StreamingBundleReplicator<T> {
    transport: T,
    targets: Vec<BundleTarget>,
    objects: ObjectEvidenceRegistry,
}

impl<T> StreamingBundleReplicator<T> {
    pub fn new(
        transport: T,
        mut targets: Vec<BundleTarget>,
        objects: ObjectEvidenceRegistry,
    ) -> Result<Self> {
        targets.sort_by(|left, right| {
            right
                .voter
                .cmp(&left.voter)
                .then_with(|| left.node.cmp(&right.node))
        });
        let mut nodes = BTreeSet::new();
        for target in &targets {
            if target.cluster_id.trim().is_empty()
                || target.node.node_id.trim().is_empty()
                || target.node.incarnation == 0
                || target.failure_domain.trim().is_empty()
                || !nodes.insert(target.node.clone())
            {
                bail!("bundle replication targets must be valid distinct node incarnations");
            }
        }
        Ok(Self {
            transport,
            targets,
            objects,
        })
    }
}

#[async_trait]
impl<T: BundleTargetStream> BundleReplicator for StreamingBundleReplicator<T> {
    async fn replicate(
        &self,
        identity: &BundleIdentity,
        bytes: &[u8],
        objects: &[ObjectShardManifestReference],
        durability: DurabilityLevel,
    ) -> Result<ReplicationEvidence> {
        verify_identity(identity, bytes)?;
        let object_evidence = self.objects.evidence_for(objects)?;
        let mut bundle_holders = Vec::new();
        if durability != DurabilityLevel::Local {
            let expected_hash = parse_sha256(&identity.hash)?;
            for target in &self.targets {
                let ack = match self.transport.send_bundle(target, identity, bytes).await {
                    Ok(ack) => ack,
                    Err(error) => {
                        tracing::warn!(
                            node_id = %target.node.node_id,
                            incarnation = target.node.incarnation,
                            voter = target.voter,
                            %error,
                            "bundle target failed before durable completion ACK"
                        );
                        continue;
                    }
                };
                if ack.status == AckStatus::Complete
                    && ack.completed_hash == Some(expected_hash)
                    && ack.persisted_through == identity.length
                {
                    bundle_holders.push(BundleDurabilityEvidence {
                        cluster_id: target.cluster_id.clone(),
                        node: target.node.clone(),
                        failure_domain: target.failure_domain.clone(),
                        complete: true,
                        hash_verified: true,
                        fsynced: true,
                    });
                }
            }
        }
        Ok(ReplicationEvidence {
            bundle_holders,
            objects: object_evidence,
        })
    }
}

fn recover(file: &mut File) -> Result<BTreeMap<String, BundleLocation>> {
    let file_len = file.metadata()?.len();
    let mut offset = 0_u64;
    let mut index = BTreeMap::new();
    while offset < file_len {
        let remaining = file_len - offset;
        if remaining < (HEADER + TRAILER) as u64 {
            truncate(file, offset)?;
            break;
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0; HEADER];
        file.read_exact(&mut header)?;
        if &header[..8] != MAGIC || u16::from_be_bytes(header[8..10].try_into().unwrap()) != VERSION
        {
            bail!("prepared bundle log contains invalid framing at offset {offset}");
        }
        let length = u64::from_be_bytes(header[42..50].try_into().unwrap());
        let prepared_at_unix_ms = u64::from_be_bytes(header[50..58].try_into().unwrap());
        let record_len = (HEADER as u64)
            .checked_add(length)
            .and_then(|value| value.checked_add(TRAILER as u64))
            .context("prepared bundle record length overflow")?;
        if record_len > remaining {
            truncate(file, offset)?;
            break;
        }
        let mut body = vec![0; HEADER + length as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut body)?;
        let mut checksum = [0; 32];
        file.read_exact(&mut checksum)?;
        let actual_checksum: [u8; 32] = Sha256::digest(&body).into();
        if actual_checksum != checksum {
            bail!("prepared bundle record checksum mismatch at offset {offset}");
        }
        let hash = format!("sha256:{}", hex::encode(&header[10..42]));
        let payload = &body[HEADER..];
        verify_identity(
            &BundleIdentity {
                hash: hash.clone(),
                length,
            },
            payload,
        )?;
        if index
            .insert(
                hash,
                BundleLocation {
                    payload_offset: offset + HEADER as u64,
                    payload_length: length,
                    prepared_at_unix_ms,
                },
            )
            .is_some()
        {
            bail!("prepared bundle log contains a duplicate bundle identity");
        }
        offset += record_len;
    }
    Ok(index)
}

fn truncate(file: &mut File, offset: u64) -> Result<()> {
    file.set_len(offset)?;
    file.sync_all()?;
    Ok(())
}

fn verify_identity(identity: &BundleIdentity, bytes: &[u8]) -> Result<()> {
    if identity.length != bytes.len() as u64 {
        bail!("prepared bundle length does not match identity");
    }
    let mut hash = Sha256::new();
    hash.update(b"anvil.mvcc.transaction-bundle.v1");
    hash.update(identity.length.to_be_bytes());
    hash.update(bytes);
    if parse_sha256(&identity.hash)? != <[u8; 32]>::from(hash.finalize()) {
        bail!("prepared bundle hash does not match canonical bytes");
    }
    Ok(())
}

fn parse_sha256(value: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(
        value
            .strip_prefix("sha256:")
            .context("bundle identity must use sha256")?,
    )?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("bundle hash must contain 32 bytes"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use uuid::Uuid;

    use super::*;
    use crate::mvcc_transaction::{
        HierarchicalRangeStampScheme, LogicalKey, TransactionBundleBuilder,
    };

    fn node(id: &str) -> NodeIncarnation {
        NodeIncarnation {
            node_id: id.to_string(),
            incarnation: 1,
        }
    }

    fn identity(bytes: &[u8]) -> BundleIdentity {
        let mut hash = Sha256::new();
        hash.update(b"anvil.mvcc.transaction-bundle.v1");
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
        BundleIdentity {
            hash: format!("sha256:{}", hex::encode(hash.finalize())),
            length: bytes.len() as u64,
        }
    }

    fn canonical_bundle(transaction_id: &str) -> (BundleIdentity, Vec<u8>) {
        let mut builder = TransactionBundleBuilder::new(
            "cluster-a",
            transaction_id,
            0,
            "principal",
            HierarchicalRangeStampScheme::new(),
        );
        builder.put(
            LogicalKey {
                table_id: 1,
                application_key: transaction_id.as_bytes().to_vec(),
            },
            b"value".to_vec(),
        );
        let bytes = builder.build().unwrap().canonical_bytes().unwrap();
        (identity(&bytes), bytes)
    }

    #[tokio::test]
    async fn append_log_retry_and_restart_reuse_one_verified_record() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = b"canonical bundle";
        let identity = identity(bytes);
        let store = AppendOnlyPreparedBundleStore::open(
            directory.path(),
            "cluster-a",
            node("node-a"),
            "zone-a",
        )
        .unwrap();
        let first = store.persist(&identity, bytes).await.unwrap();
        assert!(first.complete && first.hash_verified && first.fsynced);
        let path = directory.path().join("prepared-bundles.log");
        let length = path.metadata().unwrap().len();
        store.persist(&identity, bytes).await.unwrap();
        assert_eq!(path.metadata().unwrap().len(), length);
        drop(store);

        let reopened = AppendOnlyPreparedBundleStore::open(
            directory.path(),
            "cluster-a",
            node("node-a"),
            "zone-a",
        )
        .unwrap();
        reopened.persist(&identity, bytes).await.unwrap();
        assert_eq!(path.metadata().unwrap().len(), length);
    }

    #[tokio::test]
    async fn authorised_compaction_retains_only_explicitly_pinned_bundles() {
        let directory = tempfile::tempdir().unwrap();
        let keep_bytes = b"committed bundle needed by catch-up";
        let remove_bytes = b"expired uncommitted bundle";
        let keep = identity(keep_bytes);
        let remove = identity(remove_bytes);
        let store = AppendOnlyPreparedBundleStore::open(
            directory.path(),
            "cluster-a",
            node("node-a"),
            "zone-a",
        )
        .unwrap();
        store.persist(&keep, keep_bytes).await.unwrap();
        store.persist(&remove, remove_bytes).await.unwrap();

        assert_eq!(
            store
                .compact_authorised(std::slice::from_ref(&keep))
                .unwrap(),
            1
        );
        assert_eq!(store.read(&keep).unwrap().unwrap(), keep_bytes);
        assert!(store.read(&remove).unwrap().is_none());

        drop(store);
        let reopened = AppendOnlyPreparedBundleStore::open(
            directory.path(),
            "cluster-a",
            node("node-a"),
            "zone-a",
        )
        .unwrap();
        assert_eq!(reopened.read(&keep).unwrap().unwrap(), keep_bytes);
        assert!(reopened.read(&remove).unwrap().is_none());
    }

    #[tokio::test]
    async fn retain_plan_uses_commit_reachability_transaction_pins_and_grace() {
        let directory = tempfile::tempdir().unwrap();
        let store = AppendOnlyPreparedBundleStore::open(
            directory.path(),
            "cluster-a",
            node("node-a"),
            "zone-a",
        )
        .unwrap();
        let (committed, committed_bytes) = canonical_bundle("committed");
        let (pinned, pinned_bytes) = canonical_bundle("pinned");
        let (expired, expired_bytes) = canonical_bundle("expired");
        store.persist(&committed, &committed_bytes).await.unwrap();
        store.persist(&pinned, &pinned_bytes).await.unwrap();
        store.persist(&expired, &expired_bytes).await.unwrap();
        for location in store.log.lock().unwrap().index.values_mut() {
            location.prepared_at_unix_ms = 1;
        }

        let retain = store
            .retain_plan(
                std::slice::from_ref(&committed),
                &["pinned".to_string()].into_iter().collect(),
                100,
                10,
            )
            .unwrap();
        assert!(retain.contains(&committed));
        assert!(retain.contains(&pinned));
        assert!(!retain.contains(&expired));
        assert_eq!(store.compact_authorised(&retain).unwrap(), 1);
    }

    #[tokio::test]
    async fn recovery_truncates_crash_tail_before_accepting_retry() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = b"canonical bundle";
        let identity = identity(bytes);
        let store = AppendOnlyPreparedBundleStore::open(
            directory.path(),
            "cluster-a",
            node("node-a"),
            "zone-a",
        )
        .unwrap();
        store.persist(&identity, bytes).await.unwrap();
        let path = directory.path().join("prepared-bundles.log");
        let valid_length = path.metadata().unwrap().len();
        drop(store);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&MAGIC[..3]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let recovered = AppendOnlyPreparedBundleStore::open(
            directory.path(),
            "cluster-a",
            node("node-a"),
            "zone-a",
        )
        .unwrap();
        assert_eq!(path.metadata().unwrap().len(), valid_length);
        recovered.persist(&identity, bytes).await.unwrap();
        assert_eq!(path.metadata().unwrap().len(), valid_length);
    }

    #[tokio::test]
    async fn failed_durable_append_rolls_back_index_and_tail_before_retry() {
        struct ClearFaults;
        impl Drop for ClearFaults {
            fn drop(&mut self) {
                crate::mvcc_fault_injection::clear();
            }
        }

        let _clear = ClearFaults;
        let directory = tempfile::tempdir().unwrap();
        let bytes = b"bundle whose append reaches the durability boundary";
        let identity = identity(bytes);
        let store = AppendOnlyPreparedBundleStore::open(
            directory.path(),
            "cluster-a",
            node("node-a"),
            "zone-a",
        )
        .unwrap();
        let path = directory.path().join("prepared-bundles.log");
        let initial_len = path.metadata().unwrap().len();
        crate::mvcc_fault_injection::install(
            crate::mvcc_fault_injection::DeterministicFaults::default().fail_at(
                crate::mvcc_fault_injection::FaultPoint::PreparedBundleWrite,
                1,
            ),
        );

        let error = store.persist(&identity, bytes).await.unwrap_err();
        assert!(error.to_string().contains("PreparedBundleWrite"));
        assert!(store.read(&identity).unwrap().is_none());
        assert_eq!(path.metadata().unwrap().len(), initial_len);

        crate::mvcc_fault_injection::clear();
        store.persist(&identity, bytes).await.unwrap();
        assert_eq!(
            store.read(&identity).unwrap().as_deref(),
            Some(bytes.as_slice())
        );
        drop(store);
        let reopened = AppendOnlyPreparedBundleStore::open(
            directory.path(),
            "cluster-a",
            node("node-a"),
            "zone-a",
        )
        .unwrap();
        assert_eq!(
            reopened.read(&identity).unwrap().as_deref(),
            Some(bytes.as_slice())
        );
    }

    struct Transport {
        status_by_node: BTreeMap<String, AckStatus>,
        corrupt_hash_node: Option<String>,
        failed_nodes: BTreeSet<String>,
    }

    #[async_trait]
    impl BundleTargetStream for Transport {
        async fn send_bundle(
            &self,
            target: &BundleTarget,
            identity: &BundleIdentity,
            _bytes: &[u8],
        ) -> Result<ReplicationAck> {
            if self.failed_nodes.contains(&target.node.node_id) {
                bail!("target is unavailable");
            }
            let status = self
                .status_by_node
                .get(&target.node.node_id)
                .copied()
                .unwrap_or(AckStatus::Complete);
            let mut completed_hash =
                (status == AckStatus::Complete).then(|| parse_sha256(&identity.hash).unwrap());
            if self.corrupt_hash_node.as_deref() == Some(&target.node.node_id) {
                completed_hash = Some([9; 32]);
            }
            Ok(ReplicationAck {
                session_id: Uuid::new_v4(),
                acknowledged_sequence: 1,
                transfer_id: Uuid::new_v4(),
                persisted_through: identity.length,
                completed_hash,
                status,
            })
        }
    }

    fn target(id: &str, domain: &str) -> BundleTarget {
        BundleTarget {
            cluster_id: "cluster-a".into(),
            node: node(id),
            failure_domain: domain.to_string(),
            voter: true,
        }
    }

    fn manifest() -> ObjectShardManifestReference {
        ObjectShardManifestReference {
            object_hash: format!("sha256:{}", "a".repeat(64)),
            manifest_hash: format!("sha256:{}", "b".repeat(64)),
            object_length: 8,
            encoding_generation: 1,
            data_shards: 2,
            parity_shards: 1,
            stripe_count: 1,
        }
    }

    fn shard_evidence(manifest: &ObjectShardManifestReference) -> ObjectDurabilityEvidence {
        ObjectDurabilityEvidence::ShardPlacement {
            cluster_id: "cluster-a".into(),
            object_hash: manifest.object_hash.clone(),
            encoding_generation: manifest.encoding_generation,
            stripe_ordinal: 0,
            shard_ordinal: 0,
            data_shards: manifest.data_shards,
            parity_shards: manifest.parity_shards,
            node: node("shard-node"),
            failure_domain: "zone-s".into(),
            complete: true,
            hash_verified: true,
            fsynced: true,
        }
    }

    #[tokio::test]
    async fn replicator_combines_recorded_shards_with_only_matching_complete_bundle_acks() {
        let manifest = manifest();
        let objects = ObjectEvidenceRegistry::default();
        objects
            .record(&manifest.object_hash, shard_evidence(&manifest))
            .unwrap();
        let replicator = StreamingBundleReplicator::new(
            Transport {
                status_by_node: BTreeMap::from([("node-b".into(), AckStatus::Persisted)]),
                corrupt_hash_node: Some("node-c".into()),
                failed_nodes: BTreeSet::new(),
            },
            vec![
                target("node-a", "zone-a"),
                target("node-b", "zone-b"),
                target("node-c", "zone-c"),
            ],
            objects,
        )
        .unwrap();
        let bytes = b"bundle";
        let identity = identity(bytes);
        let evidence = replicator
            .replicate(
                &identity,
                bytes,
                std::slice::from_ref(&manifest),
                DurabilityLevel::Quorum,
            )
            .await
            .unwrap();
        assert_eq!(evidence.bundle_holders.len(), 1);
        assert_eq!(evidence.bundle_holders[0].node.node_id, "node-a");
        assert_eq!(evidence.objects, vec![shard_evidence(&manifest)]);
    }

    #[tokio::test]
    async fn local_does_not_contact_remote_targets_and_requires_recorded_object_evidence() {
        struct RejectTransport;
        #[async_trait]
        impl BundleTargetStream for RejectTransport {
            async fn send_bundle(
                &self,
                _target: &BundleTarget,
                _identity: &BundleIdentity,
                _bytes: &[u8],
            ) -> Result<ReplicationAck> {
                panic!("local durability must not contact remote targets")
            }
        }

        let manifest = manifest();
        let objects = ObjectEvidenceRegistry::default();
        let replicator = StreamingBundleReplicator::new(
            RejectTransport,
            vec![target("remote", "zone-b")],
            objects.clone(),
        )
        .unwrap();
        let bytes = b"bundle";
        let error = replicator
            .replicate(
                &identity(bytes),
                bytes,
                std::slice::from_ref(&manifest),
                DurabilityLevel::Local,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no ingest evidence"));

        let local = ObjectDurabilityEvidence::LocalRepresentation {
            cluster_id: "cluster-a".into(),
            object_hash: manifest.object_hash.clone(),
            node: node("local"),
            failure_domain: "zone-a".into(),
            complete: true,
            hash_verified: true,
            fsynced: true,
        };
        objects
            .record(&manifest.object_hash, local.clone())
            .unwrap();
        let evidence = replicator
            .replicate(
                &identity(bytes),
                bytes,
                std::slice::from_ref(&manifest),
                DurabilityLevel::Local,
            )
            .await
            .unwrap();
        assert!(evidence.bundle_holders.is_empty());
        assert_eq!(evidence.objects, vec![local]);
    }

    #[tokio::test]
    async fn quorum_collects_complete_acks_despite_optional_target_failure() {
        let replicator = StreamingBundleReplicator::new(
            Transport {
                status_by_node: BTreeMap::new(),
                corrupt_hash_node: None,
                failed_nodes: BTreeSet::from(["node-c".into()]),
            },
            vec![
                target("node-a", "zone-a"),
                target("node-b", "zone-b"),
                target("node-c", "zone-c"),
            ],
            ObjectEvidenceRegistry::default(),
        )
        .unwrap();
        let bytes = b"bundle";
        let evidence = replicator
            .replicate(&identity(bytes), bytes, &[], DurabilityLevel::Quorum)
            .await
            .unwrap();
        assert_eq!(
            evidence
                .bundle_holders
                .iter()
                .map(|holder| holder.node.node_id.as_str())
                .collect::<Vec<_>>(),
            ["node-a", "node-b"]
        );
    }

    #[test]
    fn voters_are_attempted_before_learners() {
        let mut learner = target("node-a", "zone-a");
        learner.voter = false;
        let voter = target("node-z", "zone-z");
        let replicator = StreamingBundleReplicator::new(
            Transport {
                status_by_node: BTreeMap::new(),
                corrupt_hash_node: None,
                failed_nodes: BTreeSet::new(),
            },
            vec![learner, voter],
            ObjectEvidenceRegistry::default(),
        )
        .unwrap();
        assert!(replicator.targets[0].voter);
        assert_eq!(replicator.targets[0].node.node_id, "node-z");
    }
}
