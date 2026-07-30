//! Local RocksDB storage for committed MVCC product rows.
//!
//! Certification orders transactions; this store atomically installs one
//! certified bundle and advances the node's locally applied version.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow, bail};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch,
    WriteOptions,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::bucket_locator_finalization_job::{
    BucketLocatorFinalizationJob, BucketLocatorFinalizationRecord, BucketLocatorFinalizationState,
};
use crate::git_source_postcommit_job::{
    GitSourcePostCommitJob, GitSourcePostCommitRecord, GitSourcePostCommitState,
};
use crate::hf_ingestion_postcommit_job::{
    HfIngestionPostCommitJob, HfIngestionPostCommitRecord, HfIngestionPostCommitState,
};
use crate::index_finalization_job::{
    IndexFinalizationJob, IndexFinalizationRecord, IndexFinalizationState,
};
use crate::mvcc_local_durability_upgrade::{
    LocalDurabilityUpgradeJob, LocalDurabilityUpgradeRecord, LocalDurabilityUpgradeState,
};
use crate::mvcc_shard_repair::{ShardRepairJob, ShardRepairRecord, ShardRepairState};
use crate::mvcc_transaction::{
    CommitVersion, IdempotencyResult, LogicalKey, NodeIncarnation, TransactionBundle,
    WriteOperation,
};
use crate::object_link_finalization_job::{
    ObjectLinkFinalizationJob, ObjectLinkFinalizationRecord, ObjectLinkFinalizationState,
};
use crate::object_materialisation::ObjectMaterialisationState;
use crate::object_materialisation::{ObjectMaterialisationJob, ObjectMaterialisationRecord};
use crate::personaldb_postcommit_job::{
    PersonalDbPostCommitJob, PersonalDbPostCommitRecord, PersonalDbPostCommitState,
};

pub const MVCC_COLUMN_FAMILIES: [&str; 6] = [
    "mvcc_versions",
    "mvcc_heads",
    "mvcc_applied",
    "mvcc_meta",
    "cf_materialisation",
    "cf_outbox",
];
const CF_VERSIONS: &str = MVCC_COLUMN_FAMILIES[0];
const CF_HEADS: &str = MVCC_COLUMN_FAMILIES[1];
const CF_APPLIED: &str = MVCC_COLUMN_FAMILIES[2];
const CF_META: &str = MVCC_COLUMN_FAMILIES[3];
const CF_MATERIALISATION: &str = MVCC_COLUMN_FAMILIES[4];
const CF_OUTBOX: &str = MVCC_COLUMN_FAMILIES[5];
const APPLIED_VERSION_KEY: &[u8] = b"applied_version";
const GC_WATERMARK_KEY: &[u8] = b"gc_watermark";
const DECISION_WATERMARK_KEY: &[u8] = b"decision_watermark";
const INSTALLED_CHECKPOINT_KEY: &[u8] = b"installed_checkpoint";
const LOCAL_DURABILITY_VIOLATION_PREFIX: &[u8] = b"local-durability-violation/";
const VALUE: u8 = 1;
const TOMBSTONE: u8 = 0;
pub const MVCC_CHECKPOINT_FORMAT_VERSION: u16 = 1;
const MVCC_CHECKPOINT_MAGIC: &[u8] = b"ANVIL-MVCC-CHECKPOINT";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleRow {
    pub commit_version: CommitVersion,
    pub value: Vec<u8>,
}

/// The newest committed row version visible at one snapshot.
///
/// Value-facing reads intentionally flatten both [`Self::Unwritten`] and
/// [`Self::Tombstone`] to `None`. Transaction certification must retain the
/// distinction: a tombstone is an observed committed write whose version must
/// participate in conflict detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PointSnapshot {
    Unwritten,
    Tombstone { commit_version: CommitVersion },
    Value(VisibleRow),
}

impl PointSnapshot {
    pub fn observed_version(&self) -> Option<CommitVersion> {
        match self {
            Self::Unwritten => None,
            Self::Tombstone { commit_version } => Some(*commit_version),
            Self::Value(row) => Some(row.commit_version),
        }
    }

    pub fn visible(&self) -> Option<&VisibleRow> {
        match self {
            Self::Value(row) => Some(row),
            Self::Unwritten | Self::Tombstone { .. } => None,
        }
    }

    pub fn into_visible(self) -> Option<VisibleRow> {
        match self {
            Self::Value(row) => Some(row),
            Self::Unwritten | Self::Tombstone { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedIdempotencyResult {
    pub transaction_id: String,
    pub commit_version: CommitVersion,
    pub result: IdempotencyResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    Replayed,
}

/// A cluster-scoped, point-in-time copy of the ordinary local MVCC state.
///
/// Raft snapshots deliberately exclude product rows and durable work bodies.
/// A clean-disk node therefore installs one of these checkpoints outside Raft,
/// then resumes ordered bundle application after `decision_watermark`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvccCheckpoint {
    pub format_version: u16,
    pub cluster_id: String,
    pub decision_watermark: CommitVersion,
    pub applied_version: CommitVersion,
    pub gc_watermark: CommitVersion,
    pub column_families: Vec<MvccCheckpointColumnFamily>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvccCheckpointColumnFamily {
    pub name: String,
    /// Keys are cluster-scope-relative and strictly lexicographically sorted.
    pub entries: Vec<MvccCheckpointEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvccCheckpointEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MvccCheckpointInstallOutcome {
    Installed,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InstalledMvccCheckpoint {
    format_version: u16,
    cluster_id: String,
    checkpoint_id: [u8; 32],
    decision_watermark: CommitVersion,
}

impl MvccCheckpoint {
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MVCC_CHECKPOINT_MAGIC);
        encoded.extend_from_slice(&self.format_version.to_be_bytes());
        encode_checkpoint_bytes_u32(&mut encoded, self.cluster_id.as_bytes(), "cluster ID")?;
        encoded.extend_from_slice(&self.decision_watermark.to_be_bytes());
        encoded.extend_from_slice(&self.applied_version.to_be_bytes());
        encoded.extend_from_slice(&self.gc_watermark.to_be_bytes());
        encoded.extend_from_slice(
            &u16::try_from(self.column_families.len())
                .context("MVCC checkpoint has too many column families")?
                .to_be_bytes(),
        );
        for column in &self.column_families {
            encode_checkpoint_bytes_u16(
                &mut encoded,
                column.name.as_bytes(),
                "column-family name",
            )?;
            encoded.extend_from_slice(
                &u64::try_from(column.entries.len())
                    .context("MVCC checkpoint has too many entries")?
                    .to_be_bytes(),
            );
            for entry in &column.entries {
                encode_checkpoint_bytes_u32(&mut encoded, &entry.key, "entry key")?;
                encoded.extend_from_slice(
                    &u64::try_from(entry.value.len())
                        .context("MVCC checkpoint entry value is too large")?
                        .to_be_bytes(),
                );
                encoded.extend_from_slice(&entry.value);
            }
        }
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = MvccCheckpointDecoder::new(bytes);
        if decoder.take(MVCC_CHECKPOINT_MAGIC.len())? != MVCC_CHECKPOINT_MAGIC {
            bail!("MVCC checkpoint magic is invalid");
        }
        let format_version = decoder.u16()?;
        let cluster_id = String::from_utf8(decoder.bytes_u32("cluster ID")?.to_vec())
            .context("MVCC checkpoint cluster ID is not UTF-8")?;
        let decision_watermark = decoder.u64()?;
        let applied_version = decoder.u64()?;
        let gc_watermark = decoder.u64()?;
        let column_count = usize::from(decoder.u16()?);
        if column_count != MVCC_COLUMN_FAMILIES.len() {
            bail!("MVCC checkpoint does not contain the complete column-family set");
        }
        let mut column_families = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let name = String::from_utf8(decoder.bytes_u16("column-family name")?.to_vec())
                .context("MVCC checkpoint column-family name is not UTF-8")?;
            let entry_count = decoder.collection_len("column-family entries", 13)?;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let key = decoder.bytes_u32("entry key")?.to_vec();
                let value_len = decoder.usize_u64("entry value")?;
                let value = decoder.take(value_len)?.to_vec();
                entries.push(MvccCheckpointEntry { key, value });
            }
            column_families.push(MvccCheckpointColumnFamily { name, entries });
        }
        if !decoder.is_finished() {
            bail!("MVCC checkpoint has trailing bytes");
        }
        let checkpoint = Self {
            format_version,
            cluster_id,
            decision_watermark,
            applied_version,
            gc_watermark,
            column_families,
        };
        checkpoint.validate()?;
        if checkpoint.encode()?.as_slice() != bytes {
            bail!("MVCC checkpoint encoding is not canonical");
        }
        Ok(checkpoint)
    }

    pub fn validate(&self) -> Result<()> {
        if self.format_version != MVCC_CHECKPOINT_FORMAT_VERSION {
            bail!(
                "unsupported MVCC checkpoint format version {}",
                self.format_version
            );
        }
        if self.cluster_id.trim().is_empty() {
            bail!("MVCC checkpoint cluster ID is required");
        }
        if self.applied_version > self.decision_watermark {
            bail!("MVCC checkpoint applied version exceeds its decision watermark");
        }
        if self.gc_watermark > self.decision_watermark {
            bail!("MVCC checkpoint GC watermark exceeds its decision watermark");
        }
        if self.column_families.len() != MVCC_COLUMN_FAMILIES.len() {
            bail!("MVCC checkpoint does not contain the complete column-family set");
        }
        for (column, expected_name) in self.column_families.iter().zip(MVCC_COLUMN_FAMILIES) {
            if column.name != expected_name {
                bail!(
                    "MVCC checkpoint column-family order mismatch: expected {expected_name}, found {}",
                    column.name
                );
            }
            if column.entries.iter().any(|entry| entry.key.is_empty()) {
                bail!("MVCC checkpoint contains an empty scope-relative key");
            }
            if column
                .entries
                .windows(2)
                .any(|pair| pair[0].key >= pair[1].key)
            {
                bail!(
                    "MVCC checkpoint column family {} is not strictly sorted and unique",
                    column.name
                );
            }
        }

        let versions = &self.column_families[0];
        for entry in &versions.entries {
            let (_, version) = decode_versioned_key(&entry.key)?;
            if version > self.applied_version {
                bail!("MVCC checkpoint contains a row above its applied version");
            }
            decode_point_snapshot(version, &entry.value)
                .context("validate MVCC checkpoint row encoding")?;
        }

        let heads = &self.column_families[1];
        for entry in &heads.entries {
            let logical_key = decode_logical_key(&entry.key)?;
            let head_version = decode_u64(&entry.value, "MVCC checkpoint head version")?;
            if head_version > self.applied_version {
                bail!("MVCC checkpoint contains a head above its applied version");
            }
            let versioned_key = encode_versioned_key(&logical_key, head_version)?;
            if versions
                .entries
                .binary_search_by(|candidate| candidate.key.cmp(&versioned_key))
                .is_err()
            {
                bail!("MVCC checkpoint head does not reference a retained row version");
            }
        }

        let applied = &self.column_families[2];
        for entry in &applied.entries {
            let version = decode_u64(&entry.key, "MVCC checkpoint applied-bundle version")?;
            if version > self.applied_version {
                bail!("MVCC checkpoint contains bundle evidence above its applied version");
            }
            if entry.value.is_empty() {
                bail!("MVCC checkpoint contains empty applied-bundle evidence");
            }
        }

        let meta = &self.column_families[3];
        if checkpoint_meta_version(meta, APPLIED_VERSION_KEY)? != self.applied_version
            || checkpoint_meta_version(meta, DECISION_WATERMARK_KEY)? != self.decision_watermark
            || checkpoint_meta_version(meta, GC_WATERMARK_KEY)? != self.gc_watermark
        {
            bail!("MVCC checkpoint watermarks do not match its metadata rows");
        }
        if checkpoint_entry(meta, INSTALLED_CHECKPOINT_KEY).is_some() {
            bail!("MVCC checkpoint must not contain a donor-local install marker");
        }
        Ok(())
    }

    /// Deterministic identity used to make checkpoint installation retryable.
    pub fn identity(&self) -> Result<[u8; 32]> {
        self.validate()?;
        let mut hash = Sha256::new();
        hash.update(b"anvil.mvcc.local-checkpoint.v1");
        hash.update(self.format_version.to_be_bytes());
        hash_checkpoint_component(&mut hash, self.cluster_id.as_bytes());
        hash.update(self.decision_watermark.to_be_bytes());
        hash.update(self.applied_version.to_be_bytes());
        hash.update(self.gc_watermark.to_be_bytes());
        hash.update((self.column_families.len() as u64).to_be_bytes());
        for column in &self.column_families {
            hash_checkpoint_component(&mut hash, column.name.as_bytes());
            hash.update((column.entries.len() as u64).to_be_bytes());
            for entry in &column.entries {
                hash_checkpoint_component(&mut hash, &entry.key);
                hash_checkpoint_component(&mut hash, &entry.value);
            }
        }
        let digest = hash.finalize();
        let mut identity = [0; 32];
        identity.copy_from_slice(&digest);
        Ok(identity)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxState {
    Pending,
    Running,
    Delivered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxRecord {
    pub event_id: String,
    pub transaction_id: String,
    pub commit_version: CommitVersion,
    pub ordinal: u32,
    pub payload: Vec<u8>,
    pub state: OutboxState,
    pub attempts: u32,
    #[serde(default)]
    pub created_unix_ms: u64,
    #[serde(default)]
    pub next_attempt_unix_ms: u64,
    #[serde(default)]
    pub last_error: Option<String>,
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDurabilityViolationRecord {
    pub commit_version: CommitVersion,
    pub bundle_hash: [u8; 32],
    pub lost_holder_node_id: u64,
    pub lost_holder_incarnation: u64,
    pub detected_at_log_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UnfinishedWorkPins {
    pub materialisation_snapshots: BTreeSet<CommitVersion>,
    pub repair_snapshots: BTreeSet<CommitVersion>,
    pub transaction_ids: BTreeSet<String>,
}

impl UnfinishedWorkPins {
    pub fn all(&self) -> BTreeSet<CommitVersion> {
        self.materialisation_snapshots
            .iter()
            .chain(self.repair_snapshots.iter())
            .copied()
            .collect()
    }
}

#[derive(Clone)]
pub struct MvccStore {
    db: Arc<DB>,
    cluster_id: String,
    scope: Vec<u8>,
    decision_transition: Arc<Mutex<()>>,
    materialisation_transition: Arc<Mutex<()>>,
    outbox_transition: Arc<Mutex<()>>,
}

pub type LocalMvccStore = MvccStore;

include!("mvcc_store/store_access.rs");
include!("mvcc_store/background_work.rs");
include!("mvcc_store/garbage_collection.rs");

fn current_unix_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn placement_is_local(
    placement: &crate::object_shard_manifest::PhysicalShardPlacement,
    local_node: &NodeIncarnation,
) -> bool {
    placement.node_id == local_node.node_id && placement.node_incarnation == local_node.incarnation
}

fn encode_logical_key(key: &LogicalKey) -> Result<Vec<u8>> {
    let length = u32::try_from(key.application_key.len())
        .context("MVCC application key exceeds u32 length")?;
    let mut encoded = Vec::with_capacity(6 + key.application_key.len());
    encoded.extend_from_slice(&key.table_id.to_be_bytes());
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(&key.application_key);
    Ok(encoded)
}

fn decode_logical_key(encoded: &[u8]) -> Result<LogicalKey> {
    if encoded.len() < 6 {
        bail!("invalid MVCC logical key");
    }
    let application_len = u32::from_be_bytes(encoded[2..6].try_into()?) as usize;
    if encoded.len() != 6usize.saturating_add(application_len) {
        bail!("invalid MVCC logical key length");
    }
    Ok(LogicalKey {
        table_id: u16::from_be_bytes(encoded[..2].try_into()?),
        application_key: encoded[6..].to_vec(),
    })
}

fn outbox_event_key(commit_version: CommitVersion, ordinal: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + 8 + 4);
    key.extend_from_slice(b"event/");
    key.extend_from_slice(&commit_version.to_be_bytes());
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

fn outbox_event_id(transaction_id: &str, ordinal: u32, payload: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.mvcc.outbox-event.v1");
    hasher.update(&(transaction_id.len() as u64).to_be_bytes());
    hasher.update(transaction_id.as_bytes());
    hasher.update(&ordinal.to_be_bytes());
    hasher.update(&(payload.len() as u64).to_be_bytes());
    hasher.update(payload);
    hasher.finalize().to_hex().to_string()
}

fn encode_versioned_key(key: &LogicalKey, version: CommitVersion) -> Result<Vec<u8>> {
    let mut encoded = encode_logical_key(key)?;
    encoded.extend_from_slice(&(!version).to_be_bytes());
    Ok(encoded)
}

fn decode_versioned_key(encoded: &[u8]) -> Result<(Vec<u8>, CommitVersion)> {
    if encoded.len() < 14 {
        bail!("invalid MVCC versioned key");
    }
    let logical_len = 6usize
        .checked_add(u32::from_be_bytes(encoded[2..6].try_into()?) as usize)
        .ok_or_else(|| anyhow!("invalid MVCC logical key length"))?;
    if encoded.len() != logical_len + 8 {
        bail!("invalid MVCC versioned key length");
    }
    let inverted = u64::from_be_bytes(encoded[logical_len..].try_into()?);
    Ok((encoded[..logical_len].to_vec(), !inverted))
}

fn encode_value(value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(value.len() + 1);
    encoded.push(VALUE);
    encoded.extend_from_slice(value);
    encoded
}

fn decode_point_snapshot(version: CommitVersion, encoded: &[u8]) -> Result<PointSnapshot> {
    match encoded.split_first() {
        Some((&TOMBSTONE, [])) => Ok(PointSnapshot::Tombstone {
            commit_version: version,
        }),
        Some((&VALUE, value)) => Ok(PointSnapshot::Value(VisibleRow {
            commit_version: version,
            value: value.to_vec(),
        })),
        _ => bail!("invalid MVCC row encoding"),
    }
}

fn decode_u64(bytes: &[u8], field: &str) -> Result<u64> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| anyhow!("invalid {field}"))
}

fn encode_checkpoint_bytes_u16(encoded: &mut Vec<u8>, bytes: &[u8], field: &str) -> Result<()> {
    let length = u16::try_from(bytes.len())
        .with_context(|| format!("MVCC checkpoint {field} exceeds u16 length"))?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

fn encode_checkpoint_bytes_u32(encoded: &mut Vec<u8>, bytes: &[u8], field: &str) -> Result<()> {
    let length = u32::try_from(bytes.len())
        .with_context(|| format!("MVCC checkpoint {field} exceeds u32 length"))?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

struct MvccCheckpointDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> MvccCheckpointDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| anyhow!("MVCC checkpoint length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| anyhow!("MVCC checkpoint is truncated"))?;
        self.offset = end;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(
            self.take(std::mem::size_of::<u16>())?
                .try_into()
                .expect("fixed-size slice"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(std::mem::size_of::<u32>())?
                .try_into()
                .expect("fixed-size slice"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(std::mem::size_of::<u64>())?
                .try_into()
                .expect("fixed-size slice"),
        ))
    }

    fn bytes_u16(&mut self, field: &str) -> Result<&'a [u8]> {
        let length = usize::from(self.u16()?);
        self.take(length)
            .with_context(|| format!("decode MVCC checkpoint {field}"))
    }

    fn bytes_u32(&mut self, field: &str) -> Result<&'a [u8]> {
        let length = usize::try_from(self.u32()?)
            .with_context(|| format!("MVCC checkpoint {field} length exceeds usize"))?;
        self.take(length)
            .with_context(|| format!("decode MVCC checkpoint {field}"))
    }

    fn usize_u64(&mut self, field: &str) -> Result<usize> {
        usize::try_from(self.u64()?)
            .with_context(|| format!("MVCC checkpoint {field} length exceeds usize"))
    }

    fn collection_len(&mut self, field: &str, minimum_item_bytes: usize) -> Result<usize> {
        let length = self.usize_u64(field)?;
        if minimum_item_bytes == 0 {
            bail!("MVCC checkpoint {field} has an invalid item-width bound");
        }
        let remaining = self.bytes.len().saturating_sub(self.offset);
        if length > remaining / minimum_item_bytes {
            bail!("MVCC checkpoint {field} count exceeds the remaining input");
        }
        Ok(length)
    }

    fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn checkpoint_entry<'a>(
    column: &'a MvccCheckpointColumnFamily,
    key: &[u8],
) -> Option<&'a MvccCheckpointEntry> {
    column
        .entries
        .binary_search_by(|entry| entry.key.as_slice().cmp(key))
        .ok()
        .map(|index| &column.entries[index])
}

fn checkpoint_meta_version(meta: &MvccCheckpointColumnFamily, key: &[u8]) -> Result<CommitVersion> {
    checkpoint_entry(meta, key)
        .map(|entry| decode_u64(&entry.value, "MVCC checkpoint metadata version"))
        .transpose()
        .map(|value| value.unwrap_or(0))
}

fn hash_checkpoint_component(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

fn durable_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

#[cfg(test)]
#[path = "mvcc_store/tests.rs"]
mod tests;
