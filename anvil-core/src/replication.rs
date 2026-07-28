//! Durable application-level replication primitives.
//!
//! gRPC transports these values, but transport delivery is deliberately not
//! treated as persistence. A receiver emits [`AckStatus::Persisted`] only after
//! syncing the received bytes and [`AckStatus::Complete`] only after verifying
//! the complete transfer's content hash.

use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct NodeIncarnation {
    pub node_id: String,
    pub incarnation: u64,
}

/// Proof that connection-level token validation and node authorization passed.
///
/// This type is intentionally constructible only through [`AuthenticatedPeer::new`].
/// The network service is responsible for token validation and the Zanzibar
/// connection authorization check before creating it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedPeer {
    incarnation: NodeIncarnation,
    endpoint: Option<String>,
}

impl AuthenticatedPeer {
    pub fn new(node_id: impl Into<String>, incarnation: u64) -> Result<Self> {
        let node_id = node_id.into();
        if node_id.trim().is_empty() {
            bail!("authenticated node ID must not be empty");
        }
        if incarnation == 0 {
            bail!("authenticated node incarnation must be non-zero");
        }
        Ok(Self {
            incarnation: NodeIncarnation {
                node_id,
                incarnation,
            },
            endpoint: None,
        })
    }

    pub fn new_bound(
        node_id: impl Into<String>,
        incarnation: u64,
        endpoint: impl Into<String>,
    ) -> Result<Self> {
        let mut peer = Self::new(node_id, incarnation)?;
        let endpoint = endpoint.into();
        if endpoint.trim().is_empty() {
            bail!("authenticated node endpoint must not be empty");
        }
        peer.endpoint = Some(endpoint);
        Ok(peer)
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionSession {
    id: Uuid,
    cluster_id: String,
    peer: NodeIncarnation,
    peer_endpoint: Option<String>,
    last_sequence: u64,
    last_frame: Option<(Uuid, u64, [u8; 32])>,
}

impl ConnectionSession {
    pub fn establish(cluster_id: impl Into<String>, peer: AuthenticatedPeer) -> Result<Self> {
        let cluster_id = cluster_id.into();
        if cluster_id.trim().is_empty() {
            bail!("replication session cluster ID must not be empty");
        }
        Ok(Self {
            id: Uuid::new_v4(),
            cluster_id,
            peer: peer.incarnation,
            peer_endpoint: peer.endpoint,
            last_sequence: 0,
            last_frame: None,
        })
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn peer(&self) -> &NodeIncarnation {
        &self.peer
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn peer_endpoint(&self) -> Option<&str> {
        self.peer_endpoint.as_deref()
    }

    fn validate_sequence(&self, session_id: Uuid, sequence: u64) -> Result<()> {
        if session_id != self.id {
            bail!("replication frame belongs to a different connection session");
        }
        if sequence == 0 {
            bail!("replication frame sequence must be non-zero");
        }
        // An identical sequence may be retransmitted when its durable ACK was
        // lost. The receiver verifies the bytes before acknowledging it again.
        if sequence < self.last_sequence {
            bail!("replication frame sequence is stale");
        }
        Ok(())
    }

    fn validate_retransmission(&self, frame: &ReplicationFrame) -> Result<()> {
        if frame.sequence == self.last_sequence
            && self.last_frame != Some((frame.transfer_id, frame.offset, frame.payload_checksum))
        {
            bail!("replication frame reused a sequence with different bytes");
        }
        Ok(())
    }

    fn record_frame(&mut self, frame: &ReplicationFrame) {
        self.last_sequence = frame.sequence;
        self.last_frame = Some((frame.transfer_id, frame.offset, frame.payload_checksum));
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TransferKind {
    TransactionBundle,
    ObjectShard,
    MvccCatchUp,
    ConsensusSnapshot,
    Repair,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationFrame {
    pub session_id: Uuid,
    pub cluster_id: String,
    pub sequence: u64,
    pub partition: String,
    pub transfer_id: Uuid,
    pub kind: TransferKind,
    pub offset: u64,
    pub payload: Vec<u8>,
    pub payload_checksum: [u8; 32],
    pub total_length: u64,
    pub final_hash: [u8; 32],
    pub finish: bool,
    pub transaction_id: String,
    pub prepared_snapshot_version: u64,
    pub prepared_at_unix_ms: u64,
    pub provisional: bool,
}

impl ReplicationFrame {
    pub fn checksum(payload: &[u8]) -> [u8; 32] {
        *blake3::hash(payload).as_bytes()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckStatus {
    Received,
    Persisted,
    Complete,
    Applied,
    Rejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationAck {
    pub session_id: Uuid,
    pub acknowledged_sequence: u64,
    pub transfer_id: Uuid,
    pub persisted_through: u64,
    pub completed_hash: Option<[u8; 32]>,
    pub status: AckStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferWatermark {
    pub persisted_through: u64,
    pub complete: bool,
    pub completed_hash: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteTransferChunk {
    pub offset: u64,
    pub payload: Vec<u8>,
    pub total_length: u64,
    pub completed_hash: [u8; 32],
    pub finish: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct TransferMetadata {
    transfer_id: Uuid,
    cluster_id: String,
    partition: String,
    kind: TransferKind,
    total_length: u64,
    final_hash: [u8; 32],
    transaction_id: String,
    prepared_snapshot_version: u64,
    prepared_at_unix_ms: u64,
    provisional: bool,
    /// Local durable deadline armed only after the cluster GC watermark makes
    /// a replacement placement authoritative. This is deliberately mutable
    /// receiver state, not part of the immutable transfer identity.
    #[serde(default)]
    retirement_not_before_unix_ms: Option<u64>,
}

impl TransferMetadata {
    fn has_same_immutable_identity(&self, other: &Self) -> bool {
        self.transfer_id == other.transfer_id
            && self.cluster_id == other.cluster_id
            && self.partition == other.partition
            && self.kind == other.kind
            && self.total_length == other.total_length
            && self.final_hash == other.final_hash
            && self.transaction_id == other.transaction_id
            && self.prepared_snapshot_version == other.prepared_snapshot_version
            && self.prepared_at_unix_ms == other.prepared_at_unix_ms
            && self.provisional == other.provisional
    }
}

/// Disk-backed receiver for resumable immutable transfers.
///
/// Incomplete transfers remain as `.part` files. Their durable file length is
/// the resume watermark exchanged after reconnect. Completion atomically
/// renames the verified file to `.complete`.
pub struct TransferReceiver {
    directory: PathBuf,
    metadata: HashMap<Uuid, TransferMetadata>,
}

impl TransferReceiver {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)
            .with_context(|| format!("create replication directory {}", directory.display()))?;
        let mut metadata = HashMap::new();
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("meta") {
                continue;
            }
            let bytes = fs::read(&path)?;
            let item: TransferMetadata = serde_json::from_slice(&bytes)
                .with_context(|| format!("decode transfer metadata {}", path.display()))?;
            metadata.insert(item.transfer_id, item);
        }
        Ok(Self {
            directory,
            metadata,
        })
    }

    pub fn persisted_watermark(&self, transfer_id: Uuid) -> Result<Option<u64>> {
        Ok(self
            .watermark(transfer_id)?
            .map(|watermark| watermark.persisted_through))
    }

    pub fn watermark(&self, transfer_id: Uuid) -> Result<Option<TransferWatermark>> {
        let partial = self.partial_path(transfer_id);
        if partial.exists() {
            return Ok(Some(TransferWatermark {
                persisted_through: partial.metadata()?.len(),
                complete: false,
                completed_hash: None,
            }));
        }
        let complete = self.complete_path(transfer_id);
        if complete.exists() {
            let completed_hash = self
                .metadata
                .get(&transfer_id)
                .map(|metadata| metadata.final_hash);
            return Ok(Some(TransferWatermark {
                persisted_through: complete.metadata()?.len(),
                complete: true,
                completed_hash,
            }));
        }
        Ok(None)
    }

    pub fn read_complete_chunk(
        &self,
        transfer_id: Uuid,
        offset: u64,
        max_bytes: usize,
    ) -> Result<CompleteTransferChunk> {
        if max_bytes == 0 {
            bail!("replication read chunk size must be non-zero");
        }
        let metadata = self
            .metadata
            .get(&transfer_id)
            .context("replication transfer metadata is missing")?;
        let path = self.complete_path(transfer_id);
        let mut file = File::open(&path).context("replication transfer is not complete")?;
        let total_length = file.metadata()?.len();
        if total_length != metadata.total_length || offset > total_length {
            bail!("replication read offset or immutable length is invalid");
        }
        file.seek(SeekFrom::Start(offset))?;
        let remaining = total_length - offset;
        let read_len = usize::try_from(remaining.min(max_bytes as u64))
            .context("replication read length exceeds address space")?;
        let mut payload = vec![0; read_len];
        file.read_exact(&mut payload)?;
        Ok(CompleteTransferChunk {
            offset,
            payload,
            total_length,
            completed_hash: metadata.final_hash,
            finish: offset + read_len as u64 == total_length,
        })
    }

    /// Idempotently retires complete object-shard transfers selected by a
    /// cluster-authorised MVCC GC plan after a durable local rollback window.
    ///
    /// The deadline starts when this receiver first observes the transfer as
    /// authorised, rather than when the shard was originally prepared. If a
    /// newer placement or a work pin withdraws authorisation before expiry,
    /// the deadline is cleared and a later authorisation must serve a complete
    /// new grace period. Partial transfers and every other transfer kind are
    /// deliberately outside this operation.
    pub fn retire_complete_object_shards(
        &mut self,
        authorised: &BTreeSet<Uuid>,
        now_unix_ms: u64,
        grace_ms: u64,
    ) -> Result<usize> {
        if grace_ms == 0 {
            bail!("complete shard retirement grace must be non-zero");
        }
        let transfer_ids = self.metadata.keys().copied().collect::<Vec<_>>();
        let mut retire = Vec::new();
        let mut metadata_changed = false;
        for transfer_id in transfer_ids {
            let Some(metadata) = self.metadata.get(&transfer_id).cloned() else {
                continue;
            };
            let complete = self.complete_path(transfer_id);
            let partial = self.partial_path(transfer_id);
            let authorised_complete_shard = authorised.contains(&transfer_id)
                && metadata.kind == TransferKind::ObjectShard
                && !partial.exists();
            if !authorised_complete_shard {
                if metadata.retirement_not_before_unix_ms.is_some() {
                    let mut updated = metadata;
                    updated.retirement_not_before_unix_ms = None;
                    self.replace_metadata(&updated)?;
                    self.metadata.insert(transfer_id, updated);
                    metadata_changed = true;
                }
                continue;
            }
            match metadata.retirement_not_before_unix_ms {
                // The payload is normally still present here. It may already
                // be absent if the process stopped after unlinking `.complete`
                // but before unlinking its metadata; the expired durable
                // deadline makes finishing that interrupted retirement safe.
                Some(deadline) if now_unix_ms >= deadline => retire.push(transfer_id),
                Some(_) => {}
                None if complete.exists() => {
                    let mut updated = metadata;
                    updated.retirement_not_before_unix_ms = Some(
                        now_unix_ms
                            .checked_add(grace_ms)
                            .context("complete shard retirement deadline overflow")?,
                    );
                    self.replace_metadata(&updated)?;
                    self.metadata.insert(transfer_id, updated);
                    metadata_changed = true;
                }
                None => {}
            }
        }

        let mut removed = 0;
        for transfer_id in retire {
            let complete = self.complete_path(transfer_id);
            if complete.exists() {
                fs::remove_file(&complete)?;
            }
            let metadata_path = self.metadata_path(transfer_id);
            if metadata_path.exists() {
                fs::remove_file(metadata_path)?;
            }
            self.metadata.remove(&transfer_id);
            removed += 1;
        }
        if metadata_changed || removed != 0 {
            sync_directory(&self.directory)?;
        }
        Ok(removed)
    }

    /// Reclaims provisional object shards only after cluster GC proves the
    /// preparing transaction's snapshot is no longer reachable. Grace expiry
    /// and absence from every live manifest/job are additional requirements;
    /// neither can authorise deletion without the consensus-approved
    /// watermark proof.
    pub fn retire_orphan_provisional_object_shards(
        &mut self,
        gc_watermark: u64,
        now_unix_ms: u64,
        grace_ms: u64,
        protected_transfers: &BTreeSet<Uuid>,
    ) -> Result<usize> {
        if grace_ms == 0 {
            bail!("provisional shard retirement grace must be non-zero");
        }
        let candidates = self
            .metadata
            .iter()
            .filter_map(|(transfer_id, metadata)| {
                let grace_expired = metadata
                    .prepared_at_unix_ms
                    .checked_add(grace_ms)
                    .is_some_and(|deadline| now_unix_ms >= deadline);
                (metadata.kind == TransferKind::ObjectShard
                    && metadata.provisional
                    && !metadata.transaction_id.trim().is_empty()
                    && metadata.prepared_snapshot_version < gc_watermark
                    && grace_expired
                    && !protected_transfers.contains(transfer_id))
                .then_some(*transfer_id)
            })
            .collect::<Vec<_>>();
        let mut removed = 0;
        for transfer_id in candidates {
            for path in [
                self.partial_path(transfer_id),
                self.complete_path(transfer_id),
                self.metadata_path(transfer_id),
            ] {
                if path.exists() {
                    fs::remove_file(path)?;
                }
            }
            self.metadata.remove(&transfer_id);
            removed += 1;
        }
        if removed != 0 {
            sync_directory(&self.directory)?;
        }
        Ok(removed)
    }

    pub fn receive(
        &mut self,
        session: &mut ConnectionSession,
        frame: &ReplicationFrame,
    ) -> Result<ReplicationAck> {
        session.validate_sequence(frame.session_id, frame.sequence)?;
        session.validate_retransmission(frame)?;
        if frame.payload_checksum != ReplicationFrame::checksum(&frame.payload) {
            bail!("replication frame payload checksum mismatch");
        }
        if frame.cluster_id != session.cluster_id {
            bail!("replication frame belongs to a different cluster");
        }
        if frame.offset.saturating_add(frame.payload.len() as u64) > frame.total_length {
            bail!("replication frame exceeds declared transfer length");
        }

        let expected = TransferMetadata {
            transfer_id: frame.transfer_id,
            cluster_id: frame.cluster_id.clone(),
            partition: frame.partition.clone(),
            kind: frame.kind,
            total_length: frame.total_length,
            final_hash: frame.final_hash,
            transaction_id: frame.transaction_id.clone(),
            prepared_snapshot_version: frame.prepared_snapshot_version,
            prepared_at_unix_ms: frame.prepared_at_unix_ms,
            provisional: frame.provisional,
            retirement_not_before_unix_ms: None,
        };
        if frame.kind == TransferKind::ObjectShard
            && frame.provisional
            && (frame.transaction_id.trim().is_empty() || frame.prepared_at_unix_ms == 0)
        {
            bail!("object shard transfer requires durable provisional transaction metadata");
        }
        self.ensure_metadata(&expected)?;
        let complete_path = self.complete_path(frame.transfer_id);
        if complete_path.exists() {
            let persisted_through = complete_path.metadata()?.len();
            if persisted_through != frame.total_length {
                bail!("completed transfer length differs from immutable metadata");
            }
            session.record_frame(frame);
            #[cfg(test)]
            crate::mvcc_fault_injection::hit(
                crate::mvcc_fault_injection::FaultPoint::BeforeCompleteAck,
            )?;
            let ack = ReplicationAck {
                session_id: session.id(),
                acknowledged_sequence: frame.sequence,
                transfer_id: frame.transfer_id,
                persisted_through,
                completed_hash: Some(frame.final_hash),
                status: AckStatus::Complete,
            };
            #[cfg(test)]
            crate::mvcc_fault_injection::hit(
                crate::mvcc_fault_injection::FaultPoint::AfterCompleteAck,
            )?;
            return Ok(ack);
        }

        let path = self.partial_path(frame.transfer_id);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let current_len = file.metadata()?.len();
        if frame.offset > current_len {
            bail!(
                "replication transfer has a gap: received offset {}, persisted through {}",
                frame.offset,
                current_len
            );
        }
        if frame.offset < current_len {
            self.verify_duplicate(&mut file, frame)?;
        } else {
            file.seek(SeekFrom::End(0))?;
            file.write_all(&frame.payload)?;
        }
        #[cfg(any(test, debug_assertions))]
        if frame.kind == TransferKind::ObjectShard && frame.finish {
            crate::mvcc_fault_injection::hit(crate::mvcc_fault_injection::FaultPoint::ShardWrite)?;
        }
        file.sync_data()?;
        let persisted_through = file.metadata()?.len();
        session.record_frame(frame);

        let mut ack = ReplicationAck {
            session_id: session.id(),
            acknowledged_sequence: frame.sequence,
            transfer_id: frame.transfer_id,
            persisted_through,
            completed_hash: None,
            status: AckStatus::Persisted,
        };
        if frame.finish {
            if persisted_through != frame.total_length {
                bail!("finished transfer length does not match declared length");
            }
            file.seek(SeekFrom::Start(0))?;
            let mut hasher = blake3::Hasher::new();
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
            let blake3_hash = *hasher.finalize().as_bytes();
            if !final_hash_matches(
                frame.kind,
                frame.total_length,
                &path,
                frame.final_hash,
                blake3_hash,
            )? {
                bail!("completed replication transfer hash mismatch");
            }
            drop(file);
            fs::rename(&path, self.complete_path(frame.transfer_id))?;
            sync_directory(&self.directory)?;
            ack.completed_hash = Some(frame.final_hash);
            #[cfg(test)]
            crate::mvcc_fault_injection::hit(
                crate::mvcc_fault_injection::FaultPoint::BeforeCompleteAck,
            )?;
            ack.status = AckStatus::Complete;
            #[cfg(test)]
            crate::mvcc_fault_injection::hit(
                crate::mvcc_fault_injection::FaultPoint::AfterCompleteAck,
            )?;
        }
        Ok(ack)
    }

    fn ensure_metadata(&mut self, expected: &TransferMetadata) -> Result<()> {
        if let Some(existing) = self.metadata.get(&expected.transfer_id) {
            if !existing.has_same_immutable_identity(expected) {
                bail!("transfer ID was reused with different immutable metadata");
            }
            return Ok(());
        }
        let path = self.metadata_path(expected.transfer_id);
        let temporary = path.with_extension("meta.tmp");
        let bytes = serde_json::to_vec(expected)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        sync_directory(&self.directory)?;
        self.metadata.insert(expected.transfer_id, expected.clone());
        Ok(())
    }

    fn replace_metadata(&self, metadata: &TransferMetadata) -> Result<()> {
        let path = self.metadata_path(metadata.transfer_id);
        let temporary = path.with_extension("meta.retirement.tmp");
        let bytes = serde_json::to_vec(metadata)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        Ok(())
    }

    fn verify_duplicate(&self, file: &mut File, frame: &ReplicationFrame) -> Result<()> {
        let end = frame.offset.saturating_add(frame.payload.len() as u64);
        if end > file.metadata()?.len() {
            bail!("replication retransmission overlaps the durable watermark");
        }
        file.seek(SeekFrom::Start(frame.offset))?;
        let mut existing = vec![0_u8; frame.payload.len()];
        file.read_exact(&mut existing)?;
        if existing != frame.payload {
            bail!("replication retransmission differs from persisted bytes");
        }
        Ok(())
    }

    fn partial_path(&self, transfer_id: Uuid) -> PathBuf {
        self.directory.join(format!("{transfer_id}.part"))
    }

    fn complete_path(&self, transfer_id: Uuid) -> PathBuf {
        self.directory.join(format!("{transfer_id}.complete"))
    }

    fn metadata_path(&self, transfer_id: Uuid) -> PathBuf {
        self.directory.join(format!("{transfer_id}.meta"))
    }
}

fn final_hash_matches(
    kind: TransferKind,
    total_length: u64,
    path: &Path,
    expected: [u8; 32],
    blake3_hash: [u8; 32],
) -> Result<bool> {
    if expected == blake3_hash {
        return Ok(true);
    }
    let bytes = fs::read(path)?;
    let raw_sha256: [u8; 32] = Sha256::digest(&bytes).into();
    if expected == raw_sha256 {
        return Ok(true);
    }
    if kind == TransferKind::TransactionBundle {
        let mut hash = Sha256::new();
        hash.update(b"anvil.mvcc.transaction-bundle.v1");
        hash.update(total_length.to_be_bytes());
        hash.update(&bytes);
        return Ok(expected == <[u8; 32]>::from(hash.finalize()));
    }
    Ok(false)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(
        session: &ConnectionSession,
        transfer_id: Uuid,
        sequence: u64,
        offset: u64,
        payload: &[u8],
        whole: &[u8],
        finish: bool,
    ) -> ReplicationFrame {
        ReplicationFrame {
            session_id: session.id(),
            cluster_id: "cluster-a".into(),
            sequence,
            partition: "p0".into(),
            transfer_id,
            kind: TransferKind::ObjectShard,
            offset,
            payload: payload.to_vec(),
            payload_checksum: ReplicationFrame::checksum(payload),
            total_length: whole.len() as u64,
            final_hash: *blake3::hash(whole).as_bytes(),
            finish,
            transaction_id: "tx".into(),
            prepared_snapshot_version: 1,
            prepared_at_unix_ms: 1,
            provisional: true,
        }
    }

    #[test]
    fn reconnect_resumes_from_durable_watermark_and_completes() {
        let directory = tempfile::tempdir().unwrap();
        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        let peer = AuthenticatedPeer::new("node-b", 3).unwrap();
        let mut first_session = ConnectionSession::establish("cluster-a", peer.clone()).unwrap();
        let transfer_id = Uuid::new_v4();
        let whole = b"persistent-stream";

        let first = frame(
            &first_session,
            transfer_id,
            1,
            0,
            &whole[..10],
            whole,
            false,
        );
        let ack = receiver.receive(&mut first_session, &first).unwrap();
        assert_eq!(ack.status, AckStatus::Persisted);
        assert_eq!(ack.persisted_through, 10);

        drop(receiver);
        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        assert_eq!(receiver.persisted_watermark(transfer_id).unwrap(), Some(10));
        let mut resumed_session = ConnectionSession::establish("cluster-a", peer).unwrap();
        let resumed = frame(
            &resumed_session,
            transfer_id,
            1,
            10,
            &whole[10..],
            whole,
            true,
        );
        let ack = receiver.receive(&mut resumed_session, &resumed).unwrap();
        assert_eq!(ack.status, AckStatus::Complete);
        assert_eq!(ack.completed_hash, Some(*blake3::hash(whole).as_bytes()));
    }

    #[test]
    fn provisional_orphans_require_watermark_grace_and_no_live_reference() {
        let directory = tempfile::tempdir().unwrap();
        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        let peer = AuthenticatedPeer::new("node-b", 3).unwrap();
        let mut session = ConnectionSession::establish("cluster-a", peer).unwrap();
        let whole = b"ab";
        let orphan = Uuid::new_v4();
        let protected = Uuid::new_v4();
        let at_watermark = Uuid::new_v4();
        let inside_grace = Uuid::new_v4();
        for (sequence, transfer_id, snapshot, prepared_at) in [
            (1, orphan, 2, 10),
            (2, protected, 2, 10),
            (3, at_watermark, 5, 10),
            (4, inside_grace, 2, 95),
        ] {
            let mut provisional = frame(&session, transfer_id, sequence, 0, b"a", whole, false);
            provisional.prepared_snapshot_version = snapshot;
            provisional.prepared_at_unix_ms = prepared_at;
            receiver.receive(&mut session, &provisional).unwrap();
        }
        let protected_set = BTreeSet::from([protected]);
        assert_eq!(
            receiver
                .retire_orphan_provisional_object_shards(5, 100, 10, &protected_set)
                .unwrap(),
            1
        );
        assert!(receiver.watermark(orphan).unwrap().is_none());
        assert!(receiver.watermark(protected).unwrap().is_some());
        assert!(receiver.watermark(at_watermark).unwrap().is_some());
        assert!(receiver.watermark(inside_grace).unwrap().is_some());
        assert_eq!(
            receiver
                .retire_orphan_provisional_object_shards(5, 100, 10, &protected_set)
                .unwrap(),
            0
        );
    }

    #[test]
    fn retransmission_is_deduplicated_but_different_bytes_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        let peer = AuthenticatedPeer::new("node-b", 1).unwrap();
        let mut session = ConnectionSession::establish("cluster-a", peer.clone()).unwrap();
        let transfer_id = Uuid::new_v4();
        let whole = b"abcdef";
        let first = frame(&session, transfer_id, 1, 0, b"abc", whole, false);
        receiver.receive(&mut session, &first).unwrap();
        let duplicate = frame(&session, transfer_id, 1, 0, b"abc", whole, false);
        receiver.receive(&mut session, &duplicate).unwrap();
        let reused_sequence = frame(&session, transfer_id, 1, 0, b"abd", whole, false);
        assert!(receiver.receive(&mut session, &reused_sequence).is_err());

        let mut retry_session = ConnectionSession::establish("cluster-a", peer).unwrap();
        let retry = frame(&retry_session, transfer_id, 1, 0, b"abc", whole, false);
        receiver.receive(&mut retry_session, &retry).unwrap();
        let mut corrupt = frame(&retry_session, transfer_id, 2, 0, b"abd", whole, false);
        corrupt.payload_checksum = ReplicationFrame::checksum(&corrupt.payload);
        assert!(receiver.receive(&mut retry_session, &corrupt).is_err());
        assert_eq!(receiver.persisted_watermark(transfer_id).unwrap(), Some(3));
    }

    #[test]
    fn rejects_wrong_session_and_corrupt_frame() {
        let directory = tempfile::tempdir().unwrap();
        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        let mut session =
            ConnectionSession::establish("cluster-a", AuthenticatedPeer::new("node-b", 1).unwrap())
                .unwrap();
        let transfer_id = Uuid::new_v4();
        let mut item = frame(&session, transfer_id, 1, 0, b"abc", b"abc", true);
        item.session_id = Uuid::new_v4();
        assert!(receiver.receive(&mut session, &item).is_err());

        item.session_id = session.id();
        item.sequence = 2;
        item.payload_checksum = [0; 32];
        assert!(receiver.receive(&mut session, &item).is_err());
    }

    #[test]
    fn rejects_wrong_final_hash_without_advancing_completion_watermark() {
        let directory = tempfile::tempdir().unwrap();
        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        let peer = AuthenticatedPeer::new("node-b", 1).unwrap();
        let mut session = ConnectionSession::establish("cluster-a", peer.clone()).unwrap();
        let transfer_id = Uuid::new_v4();
        let whole = b"abcdef";
        let first = frame(&session, transfer_id, 1, 0, b"abc", whole, false);
        receiver.receive(&mut session, &first).unwrap();

        let mut corrupt_final = frame(&session, transfer_id, 2, 3, b"def", whole, true);
        corrupt_final.final_hash = [7; 32];
        assert!(receiver.receive(&mut session, &corrupt_final).is_err());
        let watermark = receiver.watermark(transfer_id).unwrap().unwrap();
        assert_eq!(watermark.persisted_through, 3);
        assert!(!watermark.complete);
        assert_eq!(watermark.completed_hash, None);

        let mut resumed = ConnectionSession::establish("cluster-a", peer).unwrap();
        let correct_final = frame(&resumed, transfer_id, 1, 3, b"def", whole, true);
        let ack = receiver.receive(&mut resumed, &correct_final).unwrap();
        assert_eq!(ack.status, AckStatus::Complete);
        assert_eq!(ack.persisted_through, whole.len() as u64);
        assert_eq!(ack.completed_hash, Some(*blake3::hash(whole).as_bytes()));
    }

    #[test]
    fn lost_complete_ack_is_recovered_without_rewriting_transfer() {
        let directory = tempfile::tempdir().unwrap();
        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        let peer = AuthenticatedPeer::new("node-b", 1).unwrap();
        let mut session = ConnectionSession::establish("cluster-a", peer.clone()).unwrap();
        let transfer_id = Uuid::new_v4();
        let first = frame(&session, transfer_id, 1, 0, b"shard", b"shard", true);
        receiver.receive(&mut session, &first).unwrap();

        let mut resumed = ConnectionSession::establish("cluster-a", peer).unwrap();
        let retry = frame(&resumed, transfer_id, 1, 0, b"shard", b"shard", true);
        let ack = receiver.receive(&mut resumed, &retry).unwrap();
        assert_eq!(ack.status, AckStatus::Complete);
        assert_eq!(ack.persisted_through, 5);
    }

    #[test]
    fn authorised_shard_retirement_waits_for_durable_grace_and_preserves_inflight_transfers() {
        let directory = tempfile::tempdir().unwrap();
        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        let peer = AuthenticatedPeer::new("node-b", 1).unwrap();
        let mut session = ConnectionSession::establish("cluster-a", peer).unwrap();
        let retired = Uuid::new_v4();
        let inflight = Uuid::new_v4();
        let bundle = Uuid::new_v4();
        let complete = frame(&session, retired, 1, 0, b"retired", b"retired", true);
        receiver.receive(&mut session, &complete).unwrap();
        let partial = frame(&session, inflight, 2, 0, b"in", b"inflight", false);
        receiver.receive(&mut session, &partial).unwrap();
        let mut non_shard = frame(&session, bundle, 3, 0, b"bundle", b"bundle", true);
        non_shard.kind = TransferKind::TransactionBundle;
        receiver.receive(&mut session, &non_shard).unwrap();

        let authorised = BTreeSet::from([retired, inflight, bundle]);
        assert_eq!(
            receiver
                .retire_complete_object_shards(&authorised, 100, 10)
                .unwrap(),
            0
        );
        assert!(receiver.watermark(retired).unwrap().unwrap().complete);
        drop(receiver);

        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        assert_eq!(
            receiver
                .retire_complete_object_shards(&authorised, 109, 10)
                .unwrap(),
            0
        );
        // Replaying a completed frame after the mutable deadline was persisted
        // must still match the transfer's immutable identity.
        let peer = AuthenticatedPeer::new("node-b", 1).unwrap();
        let mut resumed = ConnectionSession::establish("cluster-a", peer).unwrap();
        let replay = frame(&resumed, retired, 1, 0, b"retired", b"retired", true);
        assert_eq!(
            receiver.receive(&mut resumed, &replay).unwrap().status,
            AckStatus::Complete
        );
        assert_eq!(
            receiver
                .retire_complete_object_shards(&authorised, 110, 10)
                .unwrap(),
            1
        );
        assert!(receiver.watermark(retired).unwrap().is_none());
        assert_eq!(
            receiver
                .watermark(inflight)
                .unwrap()
                .unwrap()
                .persisted_through,
            2
        );
        assert!(receiver.watermark(bundle).unwrap().unwrap().complete);
        assert_eq!(
            receiver
                .retire_complete_object_shards(&authorised, 120, 10)
                .unwrap(),
            0
        );
    }

    #[test]
    fn withdrawing_retirement_authorisation_restarts_the_full_grace_window() {
        let directory = tempfile::tempdir().unwrap();
        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        let peer = AuthenticatedPeer::new("node-b", 1).unwrap();
        let mut session = ConnectionSession::establish("cluster-a", peer).unwrap();
        let transfer_id = Uuid::new_v4();
        let complete = frame(&session, transfer_id, 1, 0, b"rollback", b"rollback", true);
        receiver.receive(&mut session, &complete).unwrap();

        let authorised = BTreeSet::from([transfer_id]);
        assert_eq!(
            receiver
                .retire_complete_object_shards(&authorised, 100, 10)
                .unwrap(),
            0
        );
        assert_eq!(
            receiver
                .retire_complete_object_shards(&BTreeSet::new(), 105, 10)
                .unwrap(),
            0
        );
        assert_eq!(
            receiver
                .retire_complete_object_shards(&authorised, 200, 10)
                .unwrap(),
            0
        );
        assert_eq!(
            receiver
                .retire_complete_object_shards(&authorised, 209, 10)
                .unwrap(),
            0
        );
        assert!(receiver.watermark(transfer_id).unwrap().is_some());
        assert_eq!(
            receiver
                .retire_complete_object_shards(&authorised, 210, 10)
                .unwrap(),
            1
        );
        assert!(receiver.watermark(transfer_id).unwrap().is_none());
    }

    #[test]
    fn restart_finishes_retirement_interrupted_between_payload_and_metadata_unlink() {
        let directory = tempfile::tempdir().unwrap();
        let transfer_id = Uuid::new_v4();
        let authorised = BTreeSet::from([transfer_id]);
        {
            let mut receiver = TransferReceiver::open(directory.path()).unwrap();
            let peer = AuthenticatedPeer::new("node-b", 1).unwrap();
            let mut session = ConnectionSession::establish("cluster-a", peer).unwrap();
            let complete = frame(
                &session,
                transfer_id,
                1,
                0,
                b"interrupted",
                b"interrupted",
                true,
            );
            receiver.receive(&mut session, &complete).unwrap();
            assert_eq!(
                receiver
                    .retire_complete_object_shards(&authorised, 100, 10)
                    .unwrap(),
                0
            );
        }

        fs::remove_file(directory.path().join(format!("{transfer_id}.complete"))).unwrap();
        let mut receiver = TransferReceiver::open(directory.path()).unwrap();
        assert_eq!(
            receiver
                .retire_complete_object_shards(&authorised, 110, 10)
                .unwrap(),
            1
        );
        assert!(receiver.watermark(transfer_id).unwrap().is_none());
        assert!(
            !directory
                .path()
                .join(format!("{transfer_id}.meta"))
                .exists()
        );
    }
}
