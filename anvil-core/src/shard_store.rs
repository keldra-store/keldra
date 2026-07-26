//! Append-only storage for provisional and committed erasure shards.

use anyhow::{Context, Result, bail};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

use crate::{
    mvcc_shard_repair::ShardPlacementOverlay,
    mvcc_store::LocalMvccStore,
    mvcc_transaction::NodeIncarnation,
    object_shard_manifest::{PhysicalObjectShardManifest, PhysicalShardPlacement},
    replication::{AckStatus, ReplicationAck},
};

const MAGIC: &[u8; 8] = b"ANVSHD01";
const VERSION: u16 = 2;
const HEADER: usize = 109;
const TRAILER: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ShardKind {
    Data = 1,
    Parity = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardRecord {
    pub transaction_id: Uuid,
    pub object_identity: Uuid,
    pub encoding_generation: u64,
    pub prepared_at_unix_ms: u64,
    pub stripe_ordinal: u64,
    pub shard_ordinal: u16,
    pub shard_kind: ShardKind,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ShardIdentity {
    pub transaction_id: Uuid,
    pub object_identity: Uuid,
    pub encoding_generation: u64,
    pub stripe_ordinal: u64,
    pub shard_ordinal: u16,
}

#[derive(Clone, Debug)]
pub struct ShardRetirementEvidence {
    pub overlay: ShardPlacementOverlay,
    pub replacement_complete_acks: Vec<PlacementCompleteAck>,
    pub supported_replicas: BTreeSet<NodeIncarnation>,
    pub overlay_applied_by: BTreeSet<NodeIncarnation>,
    pub retire_not_before_unix_ms: u64,
}

#[derive(Clone, Debug)]
pub struct PlacementCompleteAck {
    pub node: NodeIncarnation,
    pub ack: ReplicationAck,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardRetainPlan {
    pub retained: BTreeSet<ShardIdentity>,
}

impl From<&ShardRecord> for ShardIdentity {
    fn from(record: &ShardRecord) -> Self {
        Self {
            transaction_id: record.transaction_id,
            object_identity: record.object_identity,
            encoding_generation: record.encoding_generation,
            stripe_ordinal: record.stripe_ordinal,
            shard_ordinal: record.shard_ordinal,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardLocation {
    pub segment_id: u64,
    pub payload_offset: u64,
    pub payload_length: u64,
    pub payload_hash: [u8; 32],
}

pub struct ShardSegment {
    id: u64,
    path: PathBuf,
    file: File,
}

impl ShardSegment {
    pub fn open(directory: impl AsRef<Path>, id: u64) -> Result<Self> {
        fs::create_dir_all(directory.as_ref())?;
        let path = directory.as_ref().join(format!("{id:020}.shards"));
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        recover_tail(&mut file)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self { id, path, file })
    }

    /// Returns only after the complete shard and its framing are fsynced.
    pub fn append(&mut self, record: &ShardRecord) -> Result<ShardLocation> {
        #[cfg(test)]
        crate::mvcc_fault_injection::hit(crate::mvcc_fault_injection::FaultPoint::ShardWrite)?;
        let location = append_record(&mut self.file, self.id, record)?;
        self.file.sync_data()?;
        tracing::debug!(
            operation = "shard.fsync",
            segment_id = self.id,
            stripe_ordinal = record.stripe_ordinal,
            shard_ordinal = record.shard_ordinal,
            payload_bytes = record.payload.len(),
            "durably persisted shard"
        );
        Ok(location)
    }

    pub fn read(&mut self, location: &ShardLocation) -> Result<Vec<u8>> {
        if location.segment_id != self.id {
            bail!("shard belongs to another segment");
        }
        self.file.seek(SeekFrom::Start(location.payload_offset))?;
        let mut payload = vec![0; location.payload_length as usize];
        self.file.read_exact(&mut payload)?;
        if *blake3::hash(&payload).as_bytes() != location.payload_hash {
            bail!("shard hash mismatch");
        }
        Ok(payload)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Rewrites this segment retaining exactly the shards authorised by a
    /// cluster-wide shard GC plan.
    ///
    /// The plan must already have proved manifest unreachability, replacement
    /// durability/application, rollback-window expiry, and absence of
    /// reader/repair/rebalance pins. This local append-only store intentionally
    /// cannot infer any of those distributed facts.
    pub fn compact_authorised(&mut self, retained: &BTreeSet<ShardIdentity>) -> Result<usize> {
        let started_at = std::time::Instant::now();
        let bytes_before = self.file.metadata()?.len();
        let records = read_records(&mut self.file)?;
        let removed = records
            .iter()
            .filter(|record| !retained.contains(&ShardIdentity::from(*record)))
            .count();
        if removed == 0 {
            return Ok(0);
        }
        let temporary_path = self.path.with_extension("shards.compacting");
        let mut replacement = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&temporary_path)?;
        for record in records {
            if retained.contains(&ShardIdentity::from(&record)) {
                append_record(&mut replacement, self.id, &record)?;
            }
        }
        replacement.sync_all()?;
        fs::rename(&temporary_path, &self.path)?;
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.file.seek(SeekFrom::End(0))?;
        let reclaimed_bytes = bytes_before.saturating_sub(self.file.metadata()?.len());
        crate::perf::record_shard_gc(reclaimed_bytes, started_at.elapsed());
        tracing::info!(
            operation = "gc.shard",
            segment_id = self.id,
            removed_shards = removed,
            reclaimed_bytes,
            "completed shard garbage collection"
        );
        Ok(removed)
    }

    pub fn retain_plan(
        &mut self,
        live_manifests: &[PhysicalObjectShardManifest],
        retirement_evidence: &[ShardRetirementEvidence],
        unfinished_work: &BTreeSet<ShardIdentity>,
        manifest_scan_complete: bool,
        now_unix_ms: u64,
        provisional_grace_ms: u64,
    ) -> Result<ShardRetainPlan> {
        for manifest in live_manifests {
            manifest.validate()?;
        }
        for evidence in retirement_evidence {
            if evidence.overlay.schema != ShardPlacementOverlay::SCHEMA
                || evidence.overlay.cluster_id != evidence.overlay.replacement_manifest.cluster_id
                || evidence.overlay.retired_after_commit.is_empty()
            {
                bail!("invalid or incomplete shard retirement evidence");
            }
            evidence.overlay.replacement_manifest.validate()?;
        }
        let records = read_records(&mut self.file)?;
        let mut retained = BTreeSet::new();
        for record in records {
            let identity = ShardIdentity::from(&record);
            let payload_hash = *blake3::hash(&record.payload).as_bytes();
            let live = live_manifests.iter().any(|manifest| {
                manifest.object_identity == record.object_identity
                    && manifest.encoding_generation == record.encoding_generation
                    && manifest
                        .placements
                        .iter()
                        .any(|placement| placement_matches(&record, payload_hash, placement))
            });
            let safely_retired = retirement_evidence.iter().any(|evidence| {
                retirement_proves_removal(&record, payload_hash, evidence, now_unix_ms)
            });
            let mentioned_as_retired = retirement_evidence.iter().any(|evidence| {
                evidence
                    .overlay
                    .retired_after_commit
                    .iter()
                    .any(|placement| placement_matches(&record, payload_hash, placement))
            });
            let provisional_expired = record
                .prepared_at_unix_ms
                .checked_add(provisional_grace_ms)
                .is_some_and(|deadline| now_unix_ms >= deadline);
            let removable = !unfinished_work.contains(&identity)
                && !live
                && ((manifest_scan_complete && provisional_expired && !mentioned_as_retired)
                    || safely_retired);
            if !removable {
                retained.insert(identity);
            }
        }
        Ok(ShardRetainPlan { retained })
    }

    pub fn compact_from_evidence(
        &mut self,
        local_mvcc: &LocalMvccStore,
        required_gc_watermark: u64,
        live_manifests: &[PhysicalObjectShardManifest],
        retirement_evidence: &[ShardRetirementEvidence],
        unfinished_work: &BTreeSet<ShardIdentity>,
        manifest_scan_complete: bool,
        now_unix_ms: u64,
        provisional_grace_ms: u64,
    ) -> Result<usize> {
        if local_mvcc.gc_watermark()? < required_gc_watermark {
            bail!("shard GC cannot run before the consensus watermark is applied locally");
        }
        let plan = self.retain_plan(
            live_manifests,
            retirement_evidence,
            unfinished_work,
            manifest_scan_complete,
            now_unix_ms,
            provisional_grace_ms,
        )?;
        self.compact_authorised(&plan.retained)
    }
}

fn placement_matches(
    record: &ShardRecord,
    payload_hash: [u8; 32],
    placement: &PhysicalShardPlacement,
) -> bool {
    placement.stripe_ordinal == record.stripe_ordinal
        && placement.shard_ordinal == record.shard_ordinal
        && placement.payload_length == record.payload.len() as u64
        && placement.payload_hash == payload_hash
}

fn retirement_proves_removal(
    record: &ShardRecord,
    payload_hash: [u8; 32],
    evidence: &ShardRetirementEvidence,
    now_unix_ms: u64,
) -> bool {
    if now_unix_ms < evidence.retire_not_before_unix_ms
        || !evidence
            .supported_replicas
            .is_subset(&evidence.overlay_applied_by)
        || evidence.overlay.replacement_manifest.object_identity != record.object_identity
        || evidence.overlay.replacement_manifest.encoding_generation != record.encoding_generation
        || !evidence
            .overlay
            .retired_after_commit
            .iter()
            .any(|placement| placement_matches(record, payload_hash, placement))
    {
        return false;
    }
    evidence
        .overlay
        .replacement_manifest
        .placements
        .iter()
        .all(|placement| {
            evidence.replacement_complete_acks.iter().any(|ack| {
                ack.node.node_id == placement.node_id
                    && ack.node.incarnation == placement.node_incarnation
                    && ack.ack.transfer_id == placement.transfer_id
                    && ack.ack.status == AckStatus::Complete
                    && ack.ack.completed_hash == Some(placement.payload_hash)
                    && ack.ack.persisted_through == placement.payload_length
            })
        })
}

fn append_record(file: &mut File, segment_id: u64, record: &ShardRecord) -> Result<ShardLocation> {
    let hash = *blake3::hash(&record.payload).as_bytes();
    let offset = file.seek(SeekFrom::End(0))?;
    let mut bytes = Vec::with_capacity(HEADER + record.payload.len() + TRAILER);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(record.transaction_id.as_bytes());
    bytes.extend_from_slice(record.object_identity.as_bytes());
    bytes.extend_from_slice(&record.encoding_generation.to_be_bytes());
    bytes.extend_from_slice(&record.prepared_at_unix_ms.to_be_bytes());
    bytes.extend_from_slice(&record.stripe_ordinal.to_be_bytes());
    bytes.extend_from_slice(&record.shard_ordinal.to_be_bytes());
    bytes.push(record.shard_kind as u8);
    bytes.extend_from_slice(&(record.payload.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&hash);
    debug_assert_eq!(bytes.len(), HEADER);
    bytes.extend_from_slice(&record.payload);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    file.write_all(&bytes)?;
    Ok(ShardLocation {
        segment_id,
        payload_offset: offset + HEADER as u64,
        payload_length: record.payload.len() as u64,
        payload_hash: hash,
    })
}

fn read_records(file: &mut File) -> Result<Vec<ShardRecord>> {
    recover_tail(file)?;
    let file_len = file.metadata()?.len();
    let mut offset = 0;
    let mut records = Vec::new();
    while offset < file_len {
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0; HEADER];
        file.read_exact(&mut header)?;
        let payload_length = u64::from_be_bytes(header[69..77].try_into().unwrap());
        let mut payload = vec![0; payload_length as usize];
        file.read_exact(&mut payload)?;
        file.seek(SeekFrom::Current(TRAILER as i64))?;
        records.push(ShardRecord {
            transaction_id: Uuid::from_slice(&header[10..26])?,
            object_identity: Uuid::from_slice(&header[26..42])?,
            encoding_generation: u64::from_be_bytes(header[42..50].try_into().unwrap()),
            prepared_at_unix_ms: u64::from_be_bytes(header[50..58].try_into().unwrap()),
            stripe_ordinal: u64::from_be_bytes(header[58..66].try_into().unwrap()),
            shard_ordinal: u16::from_be_bytes(header[66..68].try_into().unwrap()),
            shard_kind: match header[68] {
                1 => ShardKind::Data,
                2 => ShardKind::Parity,
                _ => bail!("invalid shard kind"),
            },
            payload,
        });
        offset = offset
            .checked_add(HEADER as u64 + payload_length + TRAILER as u64)
            .context("shard offset overflow")?;
    }
    Ok(records)
}

fn recover_tail(file: &mut File) -> Result<()> {
    let file_len = file.metadata()?.len();
    let mut offset = 0;
    while offset < file_len {
        let remaining = file_len - offset;
        if remaining < (HEADER + TRAILER) as u64 {
            return truncate(file, offset);
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0; HEADER];
        file.read_exact(&mut header)?;
        if &header[..8] != MAGIC {
            bail!("invalid shard magic at {offset}");
        }
        if u16::from_be_bytes(header[8..10].try_into().unwrap()) != VERSION {
            bail!("unsupported shard format");
        }
        if !matches!(header[68], 1 | 2) {
            bail!("invalid shard kind");
        }
        let len = u64::from_be_bytes(header[69..77].try_into().unwrap());
        let record_len = (HEADER as u64)
            .checked_add(len)
            .and_then(|v| v.checked_add(TRAILER as u64))
            .context("shard length overflow")?;
        if record_len > remaining {
            return truncate(file, offset);
        }
        let mut body = vec![0; HEADER + len as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut body)?;
        if *blake3::hash(&body[HEADER..]).as_bytes() != header[77..109] {
            bail!("shard payload checksum mismatch at {offset}");
        }
        let mut checksum = [0; 32];
        file.read_exact(&mut checksum)?;
        if *blake3::hash(&body).as_bytes() != checksum {
            bail!("shard record checksum mismatch at {offset}");
        }
        offset += record_len;
    }
    Ok(())
}

fn truncate(file: &mut File, offset: u64) -> Result<()> {
    file.set_len(offset)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(payload: &[u8]) -> ShardRecord {
        ShardRecord {
            transaction_id: Uuid::new_v4(),
            object_identity: Uuid::new_v4(),
            encoding_generation: 1,
            prepared_at_unix_ms: 1,
            stripe_ordinal: 3,
            shard_ordinal: 2,
            shard_kind: ShardKind::Parity,
            payload: payload.into(),
        }
    }

    #[test]
    fn append_reopens_and_verifies() {
        let dir = tempfile::tempdir().unwrap();
        let location = ShardSegment::open(dir.path(), 1)
            .unwrap()
            .append(&record(b"shard"))
            .unwrap();
        assert_eq!(
            ShardSegment::open(dir.path(), 1)
                .unwrap()
                .read(&location)
                .unwrap(),
            b"shard"
        );
    }

    #[test]
    fn incomplete_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment = ShardSegment::open(dir.path(), 1).unwrap();
        segment.append(&record(b"good")).unwrap();
        let good = segment.path().metadata().unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(segment.path())
            .unwrap()
            .write_all(b"ANV")
            .unwrap();
        drop(segment);
        assert_eq!(
            ShardSegment::open(dir.path(), 1)
                .unwrap()
                .path()
                .metadata()
                .unwrap()
                .len(),
            good
        );
    }

    #[test]
    fn corrupt_complete_shard_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment = ShardSegment::open(dir.path(), 1).unwrap();
        segment.append(&record(b"good")).unwrap();
        let mut file = OpenOptions::new().write(true).open(segment.path()).unwrap();
        file.seek(SeekFrom::Start(HEADER as u64)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
        drop(segment);
        assert!(ShardSegment::open(dir.path(), 1).is_err());
    }

    #[test]
    fn authorised_compaction_keeps_pinned_provisional_or_committed_shards() {
        let dir = tempfile::tempdir().unwrap();
        let keep = record(b"visible or pinned");
        let remove = record(b"expired provisional or safely retired");
        let mut segment = ShardSegment::open(dir.path(), 1).unwrap();
        segment.append(&keep).unwrap();
        segment.append(&remove).unwrap();

        let retained = [ShardIdentity::from(&keep)].into_iter().collect();
        assert_eq!(segment.compact_authorised(&retained).unwrap(), 1);
        drop(segment);

        let mut reopened = ShardSegment::open(dir.path(), 1).unwrap();
        let records = read_records(&mut reopened.file).unwrap();
        assert_eq!(records, [keep]);
    }

    #[test]
    fn provisional_gc_requires_complete_manifest_scan_and_expired_grace() {
        let dir = tempfile::tempdir().unwrap();
        let expired = record(b"expired provisional");
        let mut segment = ShardSegment::open(dir.path(), 1).unwrap();
        segment.append(&expired).unwrap();

        let incomplete = segment
            .retain_plan(&[], &[], &BTreeSet::new(), false, 100, 10)
            .unwrap();
        assert!(incomplete.retained.contains(&ShardIdentity::from(&expired)));
        let still_recent = segment
            .retain_plan(&[], &[], &BTreeSet::new(), true, 5, 10)
            .unwrap();
        assert!(
            still_recent
                .retained
                .contains(&ShardIdentity::from(&expired))
        );
        let mvcc_dir = tempfile::tempdir().unwrap();
        let local_mvcc = LocalMvccStore::open(mvcc_dir.path()).unwrap();
        assert_eq!(
            segment
                .compact_from_evidence(&local_mvcc, 0, &[], &[], &BTreeSet::new(), true, 100, 10,)
                .unwrap(),
            1
        );
    }

    #[test]
    fn unfinished_work_pin_overrides_expired_provisional_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let pinned = record(b"repair input");
        let identity = ShardIdentity::from(&pinned);
        let mut segment = ShardSegment::open(dir.path(), 1).unwrap();
        segment.append(&pinned).unwrap();

        let plan = segment
            .retain_plan(
                &[],
                &[],
                &[identity.clone()].into_iter().collect(),
                true,
                100,
                10,
            )
            .unwrap();
        assert!(plan.retained.contains(&identity));
    }

    #[test]
    fn retired_shard_requires_complete_replacement_replica_and_grace_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let mut retired = record(b"retired");
        retired.stripe_ordinal = 0;
        retired.shard_ordinal = 0;
        let hash = *blake3::hash(&retired.payload).as_bytes();
        let retired_placement = PhysicalShardPlacement {
            stripe_ordinal: retired.stripe_ordinal,
            shard_ordinal: retired.shard_ordinal,
            payload_length: retired.payload.len() as u64,
            payload_hash: hash,
            transfer_id: Uuid::new_v4(),
            node_id: "old".into(),
            node_incarnation: 1,
            failure_domain: "zone-a".into(),
        };
        let replacement_hash = [9; 32];
        let replacement = PhysicalShardPlacement {
            payload_hash: replacement_hash,
            transfer_id: Uuid::new_v4(),
            node_id: "new".into(),
            node_incarnation: 1,
            failure_domain: "zone-b".into(),
            ..retired_placement.clone()
        };
        let manifest = PhysicalObjectShardManifest {
            schema_version: crate::object_shard_manifest::OBJECT_SHARD_MANIFEST_SCHEMA,
            cluster_id: "cluster".into(),
            object_identity: retired.object_identity,
            object_hash: format!("sha256:{}", "a".repeat(64)),
            object_length: retired.payload.len() as u64,
            encoding_generation: retired.encoding_generation,
            data_shards: 1,
            parity_shards: 1,
            shard_bytes: retired.payload.len() as u64,
            stripe_count: 1,
            placements: vec![replacement.clone()],
        };
        let node = NodeIncarnation {
            node_id: "replica".into(),
            incarnation: 1,
        };
        let mut evidence = ShardRetirementEvidence {
            overlay: ShardPlacementOverlay {
                schema: ShardPlacementOverlay::SCHEMA.into(),
                cluster_id: "cluster".into(),
                target_logical_identity: "object".into(),
                source_manifest_hash: format!("sha256:{}", "b".repeat(64)),
                replacement_manifest: manifest,
                retired_after_commit: vec![retired_placement],
            },
            replacement_complete_acks: Vec::new(),
            supported_replicas: [node.clone()].into_iter().collect(),
            overlay_applied_by: BTreeSet::new(),
            retire_not_before_unix_ms: 50,
        };
        let mut segment = ShardSegment::open(dir.path(), 1).unwrap();
        segment.append(&retired).unwrap();

        let blocked = segment
            .retain_plan(&[], &[evidence.clone()], &BTreeSet::new(), true, 100, 10)
            .unwrap();
        assert!(blocked.retained.contains(&ShardIdentity::from(&retired)));

        evidence
            .replacement_complete_acks
            .push(PlacementCompleteAck {
                node: NodeIncarnation {
                    node_id: replacement.node_id.clone(),
                    incarnation: replacement.node_incarnation,
                },
                ack: ReplicationAck {
                    session_id: Uuid::new_v4(),
                    acknowledged_sequence: 1,
                    transfer_id: replacement.transfer_id,
                    persisted_through: replacement.payload_length,
                    completed_hash: Some(replacement_hash),
                    status: AckStatus::Complete,
                },
            });
        evidence.overlay_applied_by.insert(node);
        let authorised = segment
            .retain_plan(&[], &[evidence], &BTreeSet::new(), true, 100, 10)
            .unwrap();
        assert!(!authorised.retained.contains(&ShardIdentity::from(&retired)));
    }
}
