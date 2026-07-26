//! Local RocksDB storage for committed MVCC product rows.
//!
//! Certification orders transactions; this store atomically installs one
//! certified bundle and advances the node's locally applied version.

use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, anyhow, bail};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch,
    WriteOptions,
};
use serde::{Deserialize, Serialize};

use crate::mvcc_shard_repair::{ShardRepairJob, ShardRepairRecord, ShardRepairState};
use crate::mvcc_transaction::{CommitVersion, LogicalKey, TransactionBundle, WriteOperation};
use crate::object_materialisation::ObjectMaterialisationState;
use crate::object_materialisation::{ObjectMaterialisationJob, ObjectMaterialisationRecord};

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
const VALUE: u8 = 1;
const TOMBSTONE: u8 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleRow {
    pub commit_version: CommitVersion,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    Replayed,
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
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UnfinishedWorkPins {
    pub outbox_versions: BTreeSet<CommitVersion>,
    pub materialisation_snapshots: BTreeSet<CommitVersion>,
    pub repair_snapshots: BTreeSet<CommitVersion>,
    pub transaction_ids: BTreeSet<String>,
}

impl UnfinishedWorkPins {
    pub fn all(&self) -> BTreeSet<CommitVersion> {
        self.outbox_versions
            .iter()
            .chain(self.materialisation_snapshots.iter())
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
    materialisation_transition: Arc<Mutex<()>>,
    outbox_transition: Arc<Mutex<()>>,
}

pub type LocalMvccStore = MvccStore;

impl MvccStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let descriptors = MVCC_COLUMN_FAMILIES
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
        let db = DB::open_cf_descriptors(&options, path.as_ref(), descriptors)
            .with_context(|| format!("open MVCC RocksDB at {}", path.as_ref().display()))?;
        Self::from_db(Arc::new(db), "cluster")
    }

    pub fn from_db(db: Arc<DB>, cluster_id: &str) -> Result<Self> {
        if cluster_id.is_empty() {
            bail!("MVCC store cluster ID is required");
        }
        for name in MVCC_COLUMN_FAMILIES {
            if db.cf_handle(name).is_none() {
                bail!("missing MVCC RocksDB column family {name}");
            }
        }
        let mut scope = Vec::with_capacity(4 + cluster_id.len());
        scope.extend_from_slice(&(cluster_id.len() as u32).to_be_bytes());
        scope.extend_from_slice(cluster_id.as_bytes());
        Ok(Self {
            db,
            cluster_id: cluster_id.to_string(),
            scope,
            materialisation_transition: Arc::new(Mutex::new(())),
            outbox_transition: Arc::new(Mutex::new(())),
        })
    }

    /// Atomically applies a certified bundle and advances the applied version.
    ///
    /// Application must follow certification order. Replaying the same bundle
    /// at the same version is a no-op; using that version for different content
    /// is rejected.
    pub fn apply_certified_bundle(
        &self,
        commit_version: CommitVersion,
        bundle: &TransactionBundle,
    ) -> Result<ApplyOutcome> {
        self.apply_certified_bundle_at_decision(commit_version, bundle, None)
    }

    pub fn apply_certified_bundle_and_advance(
        &self,
        commit_version: CommitVersion,
        bundle: &TransactionBundle,
        decision_position: CommitVersion,
    ) -> Result<ApplyOutcome> {
        self.apply_certified_bundle_at_decision(commit_version, bundle, Some(decision_position))
    }

    fn apply_certified_bundle_at_decision(
        &self,
        commit_version: CommitVersion,
        bundle: &TransactionBundle,
        decision_position: Option<CommitVersion>,
    ) -> Result<ApplyOutcome> {
        if bundle.cluster_id != self.cluster_id {
            bail!("transaction bundle belongs to another cluster");
        }
        let identity = bundle.identity()?.hash;
        let applied_key = self.key(&commit_version.to_be_bytes());
        let applied_cf = self.cf(CF_APPLIED)?;
        if let Some(existing) = self.db.get_cf(applied_cf, &applied_key)? {
            if existing.as_slice() == identity.as_bytes() {
                if let Some(position) = decision_position {
                    self.advance_decision_watermark(position)?;
                }
                return Ok(ApplyOutcome::Replayed);
            }
            bail!("commit version {commit_version} was already applied with another bundle");
        }

        let applied_version = self.applied_version()?;
        if commit_version <= applied_version {
            bail!(
                "cannot apply unseen version {commit_version} below applied version {applied_version}"
            );
        }

        let versions_cf = self.cf(CF_VERSIONS)?;
        let heads_cf = self.cf(CF_HEADS)?;
        let meta_cf = self.cf(CF_META)?;
        let materialisation_cf = self.cf(CF_MATERIALISATION)?;
        let outbox_cf = self.cf(CF_OUTBOX)?;
        let mut batch = WriteBatch::default();
        for write in &bundle.writes {
            let key = write.key();
            let logical_key = self.key(&encode_logical_key(key)?);
            let versioned_key = self.key(&encode_versioned_key(key, commit_version)?);
            let row = match write {
                WriteOperation::Put { value, .. } => encode_value(value),
                WriteOperation::Delete { .. } => vec![TOMBSTONE],
            };
            batch.put_cf(versions_cf, versioned_key, row);
            batch.put_cf(heads_cf, logical_key, commit_version.to_be_bytes());
        }
        for encoded_job in &bundle.materialisation_jobs {
            if serde_json::from_slice::<serde_json::Value>(encoded_job)?
                .get("schema")
                .and_then(serde_json::Value::as_str)
                == Some(ShardRepairJob::SCHEMA)
            {
                let job = ShardRepairJob::decode(encoded_job)?;
                if job.cluster_id != self.cluster_id || job.transaction_id != bundle.transaction_id
                {
                    bail!("shard repair job belongs to another transaction or cluster");
                }
                let key = self.key(format!("shard-repair/{}", job.job_id()?).as_bytes());
                let record = serde_json::to_vec(&ShardRepairRecord::pending(job))?;
                if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                    && existing.as_slice() != record.as_slice()
                {
                    bail!("shard repair job identity collision");
                }
                batch.put_cf(materialisation_cf, key, record);
                continue;
            }
            let job = ObjectMaterialisationJob::decode(encoded_job)?;
            if job.cluster_id != self.cluster_id || job.transaction_id != bundle.transaction_id {
                bail!("materialisation job belongs to another transaction or cluster");
            }
            let key = self.key(format!("object-job/{}", job.job_id()?).as_bytes());
            let record = serde_json::to_vec(&ObjectMaterialisationRecord::pending(job))?;
            if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                && existing.as_slice() != record.as_slice()
            {
                bail!("materialisation job identity collision");
            }
            batch.put_cf(materialisation_cf, key, record);
        }
        for (ordinal, payload) in bundle.outbox_events.iter().enumerate() {
            let ordinal = u32::try_from(ordinal).context("too many outbox events in bundle")?;
            let record = OutboxRecord {
                event_id: outbox_event_id(&bundle.transaction_id, ordinal, payload),
                transaction_id: bundle.transaction_id.clone(),
                commit_version,
                ordinal,
                payload: payload.clone(),
                state: OutboxState::Pending,
                attempts: 0,
                lease_owner: None,
                lease_expires_unix_ms: None,
            };
            batch.put_cf(
                outbox_cf,
                self.key(&outbox_event_key(commit_version, ordinal)),
                serde_json::to_vec(&record)?,
            );
        }
        batch.put_cf(applied_cf, applied_key, identity.as_bytes());
        batch.put_cf(
            meta_cf,
            self.key(APPLIED_VERSION_KEY),
            commit_version.to_be_bytes(),
        );
        if let Some(position) = decision_position {
            batch.put_cf(
                meta_cf,
                self.key(DECISION_WATERMARK_KEY),
                position.to_be_bytes(),
            );
        }
        #[cfg(test)]
        crate::mvcc_fault_injection::hit(crate::mvcc_fault_injection::FaultPoint::MvccBatchWrite)?;
        self.db.write_opt(batch, &durable_write_options())?;
        Ok(ApplyOutcome::Applied)
    }

    pub fn outbox_records_after(
        &self,
        commit_version: CommitVersion,
        limit: usize,
    ) -> Result<Vec<OutboxRecord>> {
        if limit == 0 {
            bail!("outbox page limit must be non-zero");
        }
        let cf = self.cf(CF_OUTBOX)?;
        let seek = self.key(&outbox_event_key(commit_version.saturating_add(1), 0));
        let prefix = self.key(b"event/");
        let mut records = Vec::new();
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            records.push(serde_json::from_slice(&value)?);
            if records.len() == limit {
                break;
            }
        }
        Ok(records)
    }

    pub fn claim_outbox(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<OutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("outbox worker and lease must be non-empty");
        }
        let _transition = self.outbox_transition.lock().unwrap();
        let cf = self.cf(CF_OUTBOX)?;
        let prefix = self.key(b"event/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let mut record: OutboxRecord = serde_json::from_slice(&value)?;
            let claimable = record.state == OutboxState::Pending
                || (record.state == OutboxState::Running
                    && record
                        .lease_expires_unix_ms
                        .is_some_and(|deadline| deadline <= now_unix_ms));
            if !claimable {
                continue;
            }
            record.state = OutboxState::Running;
            record.attempts = record.attempts.saturating_add(1);
            record.lease_owner = Some(worker_id.to_string());
            record.lease_expires_unix_ms = Some(
                now_unix_ms
                    .checked_add(lease_ms)
                    .context("outbox lease expiry overflow")?,
            );
            self.db.put_cf_opt(
                cf,
                key,
                serde_json::to_vec(&record)?,
                &durable_write_options(),
            )?;
            return Ok(Some(record));
        }
        Ok(None)
    }

    pub fn complete_outbox(&self, record: &OutboxRecord, worker_id: &str) -> Result<()> {
        let _transition = self.outbox_transition.lock().unwrap();
        let cf = self.cf(CF_OUTBOX)?;
        let key = self.key(&outbox_event_key(record.commit_version, record.ordinal));
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("outbox event not found")?;
        let mut current: OutboxRecord = serde_json::from_slice(&bytes)?;
        if current.event_id != record.event_id {
            bail!("outbox event identity mismatch");
        }
        if current.state == OutboxState::Delivered {
            return Ok(());
        }
        if current.state != OutboxState::Running
            || current.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("outbox event is not leased by this worker");
        }
        current.state = OutboxState::Delivered;
        current.lease_owner = None;
        current.lease_expires_unix_ms = None;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&current)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    pub fn read_at(
        &self,
        key: &LogicalKey,
        snapshot_version: CommitVersion,
    ) -> Result<Option<VisibleRow>> {
        let gc_watermark = self.gc_watermark()?;
        if snapshot_version < gc_watermark {
            bail!("snapshot {snapshot_version} is below local GC watermark {gc_watermark}");
        }
        if snapshot_version > self.readable_version()? {
            bail!(
                "snapshot {snapshot_version} is above local readable version {}",
                self.readable_version()?
            );
        }
        let prefix = self.key(&encode_logical_key(key)?);
        let seek = self.key(&encode_versioned_key(key, snapshot_version)?);
        let versions_cf = self.cf(CF_VERSIONS)?;
        let mut rows = self
            .db
            .iterator_cf(versions_cf, IteratorMode::From(&seek, Direction::Forward));
        let Some(row) = rows.next() else {
            return Ok(None);
        };
        let (encoded_key, encoded_value) = row?;
        if !encoded_key.starts_with(&prefix) {
            return Ok(None);
        }
        let version = decode_versioned_key(self.unscoped(&encoded_key)?)?.1;
        decode_visible_row(version, &encoded_value)
    }

    pub fn read_latest(&self, key: &LogicalKey) -> Result<Option<VisibleRow>> {
        let heads_cf = self.cf(CF_HEADS)?;
        let Some(head) = self
            .db
            .get_cf(heads_cf, self.key(&encode_logical_key(key)?))?
        else {
            return Ok(None);
        };
        let version = decode_u64(&head, "MVCC head")?;
        self.read_at(key, version)
    }

    pub fn scan_table_prefix_at(
        &self,
        table_id: u16,
        application_prefix: &[u8],
        snapshot_version: CommitVersion,
    ) -> Result<Vec<(LogicalKey, VisibleRow)>> {
        let gc_watermark = self.gc_watermark()?;
        if snapshot_version < gc_watermark {
            bail!("snapshot {snapshot_version} is below local GC watermark {gc_watermark}");
        }
        let readable_version = self.readable_version()?;
        if snapshot_version > readable_version {
            bail!("snapshot {snapshot_version} is above local readable version {readable_version}");
        }

        let heads_cf = self.cf(CF_HEADS)?;
        let mut visible = Vec::new();
        for row in self.db.iterator_cf(
            heads_cf,
            IteratorMode::From(&self.scope, Direction::Forward),
        ) {
            let (encoded_key, _) = row?;
            if !encoded_key.starts_with(&self.scope) {
                break;
            }
            let key = decode_logical_key(self.unscoped(&encoded_key)?)?;
            if key.table_id != table_id || !key.application_key.starts_with(application_prefix) {
                continue;
            }
            if let Some(row) = self.read_at(&key, snapshot_version)? {
                visible.push((key, row));
            }
        }
        Ok(visible)
    }

    pub fn applied_version(&self) -> Result<CommitVersion> {
        self.read_meta_version(APPLIED_VERSION_KEY)
    }

    pub fn readable_version(&self) -> Result<CommitVersion> {
        Ok(self.applied_version()?.max(self.decision_watermark()?))
    }

    pub fn gc_watermark(&self) -> Result<CommitVersion> {
        self.read_meta_version(GC_WATERMARK_KEY)
    }

    pub fn decision_watermark(&self) -> Result<CommitVersion> {
        self.read_meta_version(DECISION_WATERMARK_KEY)
    }

    pub fn advance_decision_watermark(&self, position: CommitVersion) -> Result<()> {
        let current = self.decision_watermark()?;
        if position < current {
            bail!("MVCC decision watermark cannot move backwards");
        }
        self.db.put_cf_opt(
            self.cf(CF_META)?,
            self.key(DECISION_WATERMARK_KEY),
            position.to_be_bytes(),
            &durable_write_options(),
        )?;
        Ok(())
    }

    pub fn claim_object_materialisation(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<(String, ObjectMaterialisationRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("materialisation worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"object-job/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let mut record: ObjectMaterialisationRecord = serde_json::from_slice(&value)?;
            if !record.claimable(now_unix_ms) {
                continue;
            }
            record.state = ObjectMaterialisationState::Running;
            record.attempts = record.attempts.saturating_add(1);
            record.lease_owner = Some(worker_id.to_string());
            record.lease_expires_unix_ms = Some(
                now_unix_ms
                    .checked_add(lease_ms)
                    .context("materialisation lease expiry overflow")?,
            );
            self.db.put_cf_opt(
                cf,
                &key,
                serde_json::to_vec(&record)?,
                &durable_write_options(),
            )?;
            let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
                .strip_prefix("object-job/")
                .context("invalid materialisation job key")?
                .to_string();
            return Ok(Some((id, record)));
        }
        Ok(None)
    }

    pub fn retry_object_materialisation(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_object_materialisation(job_id, worker_id, |record| {
            record.state = ObjectMaterialisationState::Pending;
            record.next_attempt_unix_ms = next_attempt_unix_ms;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = Some(error.to_string());
            Ok(())
        })
    }

    pub fn complete_object_materialisation(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.transition_object_materialisation(job_id, worker_id, |record| {
            record.state = ObjectMaterialisationState::Complete;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = None;
            Ok(())
        })
    }

    pub fn claim_shard_repair(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<(String, ShardRepairRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("shard repair worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"shard-repair/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let mut record: ShardRepairRecord = serde_json::from_slice(&value)?;
            if !record.claimable(now_unix_ms) {
                continue;
            }
            record.state = ShardRepairState::Running;
            record.attempts = record.attempts.saturating_add(1);
            record.lease_owner = Some(worker_id.to_string());
            record.lease_expires_unix_ms = Some(
                now_unix_ms
                    .checked_add(lease_ms)
                    .context("shard repair lease expiry overflow")?,
            );
            self.db.put_cf_opt(
                cf,
                &key,
                serde_json::to_vec(&record)?,
                &durable_write_options(),
            )?;
            let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
                .strip_prefix("shard-repair/")
                .context("invalid shard repair key")?
                .to_string();
            return Ok(Some((id, record)));
        }
        Ok(None)
    }

    pub fn retry_shard_repair(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_shard_repair(job_id, worker_id, |record| {
            record.state = ShardRepairState::Pending;
            record.next_attempt_unix_ms = next_attempt_unix_ms;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = Some(error.to_string());
            Ok(())
        })
    }

    pub fn complete_shard_repair(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.transition_shard_repair(job_id, worker_id, |record| {
            record.state = ShardRepairState::Complete;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = None;
            Ok(())
        })
    }

    pub fn has_incomplete_object_materialisations(&self) -> Result<bool> {
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"object-job/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: ObjectMaterialisationRecord = serde_json::from_slice(&value)?;
            if record.state != ObjectMaterialisationState::Complete {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn object_materialisation_record(
        &self,
        job_id: &str,
    ) -> Result<Option<ObjectMaterialisationRecord>> {
        if job_id.trim().is_empty() {
            bail!("materialisation job ID must be non-empty");
        }
        self.db
            .get_cf(
                self.cf(CF_MATERIALISATION)?,
                self.key(format!("object-job/{job_id}").as_bytes()),
            )?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }

    /// Reports durable unfinished work which must constrain the candidate GC
    /// watermark before `AdvanceGcWatermark` is proposed to consensus.
    pub fn unfinished_work_pins(&self) -> Result<UnfinishedWorkPins> {
        let mut pins = UnfinishedWorkPins::default();
        let outbox_cf = self.cf(CF_OUTBOX)?;
        let outbox_prefix = self.key(b"event/");
        for row in self.db.iterator_cf(
            outbox_cf,
            IteratorMode::From(&outbox_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&outbox_prefix) {
                break;
            }
            let record: OutboxRecord = serde_json::from_slice(&value)?;
            if record.state != OutboxState::Delivered {
                pins.outbox_versions.insert(record.commit_version);
                pins.transaction_ids.insert(record.transaction_id);
            }
        }

        let materialisation_cf = self.cf(CF_MATERIALISATION)?;
        let object_prefix = self.key(b"object-job/");
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&object_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&object_prefix) {
                break;
            }
            let record: ObjectMaterialisationRecord = serde_json::from_slice(&value)?;
            if record.state != ObjectMaterialisationState::Complete {
                pins.materialisation_snapshots
                    .insert(record.job.originating_snapshot_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }

        let repair_prefix = self.key(b"shard-repair/");
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&repair_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&repair_prefix) {
                break;
            }
            let record: ShardRepairRecord = serde_json::from_slice(&value)?;
            if record.state != ShardRepairState::Complete {
                pins.repair_snapshots
                    .insert(record.job.originating_snapshot_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }
        Ok(pins)
    }

    fn transition_object_materialisation(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut ObjectMaterialisationRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("object-job/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("materialisation job not found")?;
        let mut record: ObjectMaterialisationRecord = serde_json::from_slice(&bytes)?;
        if record.state != ObjectMaterialisationState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("materialisation job is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn transition_shard_repair(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut ShardRepairRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("shard-repair/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("shard repair not found")?;
        let mut record: ShardRepairRecord = serde_json::from_slice(&bytes)?;
        if record.state != ShardRepairState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("shard repair is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    /// Removes obsolete history below a consensus-approved watermark.
    ///
    /// The newest version at or below the watermark is retained as the
    /// visibility anchor, including tombstones. All newer versions remain.
    /// Delivered outbox events and completed jobs are removed only when their
    /// commit/snapshot coordinate is strictly below the watermark. Pending or
    /// leased work is a hard pin and causes collection to fail.
    pub fn garbage_collect(&self, safe_watermark: CommitVersion) -> Result<usize> {
        let started_at = std::time::Instant::now();
        let _materialisation_transition = self.materialisation_transition.lock().unwrap();
        let _outbox_transition = self.outbox_transition.lock().unwrap();
        let current = self.gc_watermark()?;
        if safe_watermark < current {
            bail!("GC watermark cannot move backwards");
        }
        if safe_watermark > self.readable_version()? {
            bail!("GC watermark cannot exceed the readable version");
        }
        if let Some(oldest_pin) = self.unfinished_work_pins()?.all().into_iter().next()
            && oldest_pin < safe_watermark
        {
            bail!("GC watermark {safe_watermark} exceeds unfinished work pin {oldest_pin}");
        }

        let versions_cf = self.cf(CF_VERSIONS)?;
        let applied_cf = self.cf(CF_APPLIED)?;
        let materialisation_cf = self.cf(CF_MATERIALISATION)?;
        let outbox_cf = self.cf(CF_OUTBOX)?;
        let meta_cf = self.cf(CF_META)?;
        let mut batch = WriteBatch::default();
        let mut deleted = 0;
        let mut deleted_bytes = 0_u64;
        let mut current_key: Option<Vec<u8>> = None;
        let mut retained_anchor = false;

        for row in self.db.iterator_cf(
            versions_cf,
            IteratorMode::From(&self.scope, Direction::Forward),
        ) {
            let (encoded_key, value) = row?;
            if !encoded_key.starts_with(&self.scope) {
                break;
            }
            let (logical_key, version) = decode_versioned_key(self.unscoped(&encoded_key)?)?;
            if current_key.as_deref() != Some(logical_key.as_slice()) {
                current_key = Some(logical_key);
                retained_anchor = false;
            }
            if version <= safe_watermark && !retained_anchor {
                retained_anchor = true;
            } else if version < safe_watermark && retained_anchor {
                batch.delete_cf(versions_cf, &encoded_key);
                deleted += 1;
                deleted_bytes = deleted_bytes
                    .saturating_add((encoded_key.len() as u64).saturating_add(value.len() as u64));
            }
        }
        for row in self.db.iterator_cf(
            applied_cf,
            IteratorMode::From(&self.scope, Direction::Forward),
        ) {
            let (encoded_key, value) = row?;
            if !encoded_key.starts_with(&self.scope) {
                break;
            }
            let version = decode_u64(self.unscoped(&encoded_key)?, "applied bundle version")?;
            if version < safe_watermark {
                batch.delete_cf(applied_cf, &encoded_key);
                deleted += 1;
                deleted_bytes = deleted_bytes
                    .saturating_add((encoded_key.len() as u64).saturating_add(value.len() as u64));
            }
        }
        let outbox_prefix = self.key(b"event/");
        for row in self.db.iterator_cf(
            outbox_cf,
            IteratorMode::From(&outbox_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&outbox_prefix) {
                break;
            }
            let record: OutboxRecord = serde_json::from_slice(&value)?;
            if record.state == OutboxState::Delivered && record.commit_version < safe_watermark {
                batch.delete_cf(outbox_cf, &key);
                deleted += 1;
                deleted_bytes = deleted_bytes
                    .saturating_add((key.len() as u64).saturating_add(value.len() as u64));
            }
        }
        self.collect_completed_jobs(
            materialisation_cf,
            b"object-job/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        self.collect_completed_jobs(
            materialisation_cf,
            b"shard-repair/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        batch.put_cf(
            meta_cf,
            self.key(GC_WATERMARK_KEY),
            safe_watermark.to_be_bytes(),
        );
        self.db.write_opt(batch, &durable_write_options())?;
        crate::perf::record_mvcc_gc(safe_watermark, deleted_bytes, started_at.elapsed());
        tracing::info!(
            operation = "gc.mvcc",
            watermark = safe_watermark,
            deleted_records = deleted,
            reclaimed_bytes = deleted_bytes,
            "completed MVCC garbage collection"
        );
        Ok(deleted)
    }

    fn collect_completed_jobs(
        &self,
        cf: &ColumnFamily,
        suffix: &[u8],
        safe_watermark: CommitVersion,
        batch: &mut WriteBatch,
        deleted: &mut usize,
        deleted_bytes: &mut u64,
    ) -> Result<()> {
        let prefix = self.key(suffix);
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let completed_below_watermark = if suffix == b"object-job/" {
                let record: ObjectMaterialisationRecord = serde_json::from_slice(&value)?;
                record.state == ObjectMaterialisationState::Complete
                    && record.job.originating_snapshot_version < safe_watermark
            } else {
                let record: ShardRepairRecord = serde_json::from_slice(&value)?;
                record.state == ShardRepairState::Complete
                    && record.job.originating_snapshot_version < safe_watermark
            };
            if completed_below_watermark {
                batch.delete_cf(cf, &key);
                *deleted += 1;
                *deleted_bytes = deleted_bytes
                    .saturating_add((key.len() as u64).saturating_add(value.len() as u64));
            }
        }
        Ok(())
    }

    fn read_meta_version(&self, key: &[u8]) -> Result<CommitVersion> {
        self.db
            .get_cf(self.cf(CF_META)?, self.key(key))?
            .map(|bytes| decode_u64(&bytes, "MVCC metadata version"))
            .transpose()
            .map(|value| value.unwrap_or(0))
    }

    fn cf(&self, name: &str) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| anyhow!("missing MVCC RocksDB column family {name}"))
    }

    fn key(&self, suffix: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.scope.len() + suffix.len());
        key.extend_from_slice(&self.scope);
        key.extend_from_slice(suffix);
        key
    }

    fn unscoped<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
        key.strip_prefix(self.scope.as_slice())
            .ok_or_else(|| anyhow!("MVCC key belongs to another cluster"))
    }
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

fn decode_visible_row(version: CommitVersion, encoded: &[u8]) -> Result<Option<VisibleRow>> {
    match encoded.split_first() {
        Some((&TOMBSTONE, [])) => Ok(None),
        Some((&VALUE, value)) => Ok(Some(VisibleRow {
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

fn durable_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::mvcc_transaction::{HierarchicalRangeStampScheme, TransactionBundleBuilder};

    fn key(table_id: u16, application_key: &[u8]) -> LogicalKey {
        LogicalKey {
            table_id,
            application_key: application_key.to_vec(),
        }
    }

    fn bundle(
        transaction_id: &str,
        writes: impl FnOnce(&mut TransactionBundleBuilder),
    ) -> TransactionBundle {
        let mut builder = TransactionBundleBuilder::new(
            "cluster",
            transaction_id,
            0,
            "principal",
            HierarchicalRangeStampScheme::new(),
        );
        writes(&mut builder);
        builder.build().unwrap()
    }

    #[test]
    fn snapshot_reads_select_the_newest_visible_immutable_version() {
        let temp = tempdir().unwrap();
        let store = MvccStore::open(temp.path()).unwrap();
        let row = key(7, b"account");
        store
            .apply_certified_bundle(
                2,
                &bundle("v2", |b| {
                    b.put(row.clone(), b"old".to_vec());
                }),
            )
            .unwrap();
        store
            .apply_certified_bundle(
                5,
                &bundle("v5", |b| {
                    b.put(row.clone(), b"new".to_vec());
                }),
            )
            .unwrap();

        assert_eq!(store.read_at(&row, 2).unwrap().unwrap().value, b"old");
        assert_eq!(store.read_at(&row, 4).unwrap().unwrap().value, b"old");
        assert_eq!(store.read_latest(&row).unwrap().unwrap().value, b"new");
    }

    #[test]
    fn outbox_events_install_atomically_and_claim_durably() {
        let temp = tempdir().unwrap();
        let row = key(7, b"account");
        let store = MvccStore::open(temp.path()).unwrap();
        store
            .apply_certified_bundle(
                3,
                &bundle("with-outbox", |builder| {
                    builder.put(row.clone(), b"visible".to_vec());
                    builder.add_outbox_event(b"notify-account".to_vec());
                }),
            )
            .unwrap();

        assert_eq!(store.read_latest(&row).unwrap().unwrap().value, b"visible");
        let records = store.outbox_records_after(0, 10).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].commit_version, 3);
        assert_eq!(records[0].payload, b"notify-account");
        assert_eq!(records[0].state, OutboxState::Pending);

        let first = store.claim_outbox("worker-a", 10, 5).unwrap().unwrap();
        assert_eq!(first.state, OutboxState::Running);
        assert_eq!(first.attempts, 1);
        assert!(store.claim_outbox("worker-b", 14, 5).unwrap().is_none());
        let reclaimed = store.claim_outbox("worker-b", 15, 5).unwrap().unwrap();
        assert_eq!(reclaimed.event_id, first.event_id);
        assert_eq!(reclaimed.attempts, 2);
        store.complete_outbox(&reclaimed, "worker-b").unwrap();
        store.complete_outbox(&reclaimed, "worker-b").unwrap();
        assert!(store.claim_outbox("worker-a", 100, 5).unwrap().is_none());
        assert_eq!(
            store.outbox_records_after(0, 10).unwrap()[0].state,
            OutboxState::Delivered
        );

        drop(store);
        let reopened = MvccStore::open(temp.path()).unwrap();
        let persisted = reopened.outbox_records_after(0, 10).unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].state, OutboxState::Delivered);
        assert_eq!(persisted[0].event_id, first.event_id);
    }

    #[test]
    fn table_prefix_scan_is_filtered_and_snapshot_consistent() {
        let temp = tempdir().unwrap();
        let store = MvccStore::open(temp.path()).unwrap();
        store
            .apply_certified_bundle(
                1,
                &bundle("initial", |builder| {
                    builder.put(key(7, b"part/a"), b"a1".to_vec());
                    builder.put(key(7, b"part/b"), b"b1".to_vec());
                    builder.put(key(7, b"other/c"), b"c1".to_vec());
                    builder.put(key(8, b"part/d"), b"d1".to_vec());
                }),
            )
            .unwrap();
        store
            .apply_certified_bundle(
                2,
                &bundle("update", |builder| {
                    builder.put(key(7, b"part/a"), b"a2".to_vec());
                    builder.delete(key(7, b"part/b"));
                }),
            )
            .unwrap();

        let at_one = store.scan_table_prefix_at(7, b"part/", 1).unwrap();
        assert_eq!(at_one.len(), 2);
        assert_eq!(at_one[0].1.value, b"a1");
        assert_eq!(at_one[1].1.value, b"b1");

        let at_two = store.scan_table_prefix_at(7, b"part/", 2).unwrap();
        assert_eq!(at_two.len(), 1);
        assert_eq!(at_two[0].0.application_key, b"part/a");
        assert_eq!(at_two[0].1.value, b"a2");
    }

    #[test]
    fn materialisation_leases_retry_and_recover_after_expiry() {
        let temp = tempdir().unwrap();
        let store = MvccStore::open(temp.path()).unwrap();
        let job = ObjectMaterialisationJob {
            schema: ObjectMaterialisationJob::SCHEMA.into(),
            cluster_id: "cluster".into(),
            transaction_id: "jobs".into(),
            tenant_id: 1,
            bucket_id: 2,
            bucket_name: "bucket".into(),
            object_key: "object".into(),
            object_version_id: "version".into(),
            target_logical_identity: "tenant/1/bucket/2/object/object/version/version".into(),
            representation: serde_json::json!({"schema": "local"}),
            content_hash: "sha256:payload".into(),
            payload_length: 3,
            frozen_object: serde_json::json!({
                "version_id": "version",
                "content_hash": "sha256:payload",
                "size": 3,
            }),
            source_manifest_hash:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
            content_type: Some("application/json".into()),
            user_metadata: serde_json::json!({}),
            index_policy_snapshot: serde_json::json!({}),
            originating_snapshot_version: 0,
            frozen_index_definitions: Vec::new(),
            authz_revision: 1,
            boundary_schema: None,
            boundary_schema_generation: 0,
            boundary_schema_hash: None,
            requested_operations: crate::object_materialisation::ObjectMaterialisationOperations {
                extract_boundaries: true,
                maintain_indexes: true,
            },
            requested_at_unix_ms: 1,
        };
        let id = job.job_id().unwrap();
        store
            .apply_certified_bundle(
                1,
                &bundle("jobs", |builder| {
                    builder.add_materialisation_job(job.canonical_bytes().unwrap());
                }),
            )
            .unwrap();

        let (_, first) = store
            .claim_object_materialisation("worker-a", 10, 10)
            .unwrap()
            .unwrap();
        assert_eq!(first.attempts, 1);
        assert!(
            store
                .claim_object_materialisation("worker-b", 19, 10)
                .unwrap()
                .is_none()
        );
        let (_, recovered) = store
            .claim_object_materialisation("worker-b", 20, 10)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.attempts, 2);
        store
            .retry_object_materialisation(&id, "worker-b", 40, "transient")
            .unwrap();
        assert!(
            store
                .claim_object_materialisation("worker-a", 39, 10)
                .unwrap()
                .is_none()
        );
        store
            .claim_object_materialisation("worker-a", 40, 10)
            .unwrap()
            .unwrap();
        store
            .complete_object_materialisation(&id, "worker-a")
            .unwrap();
        assert!(
            store
                .claim_object_materialisation("worker-b", 100, 10)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_a_bundle_from_another_cluster_before_writing() {
        let temp = tempdir().unwrap();
        let store = MvccStore::open(temp.path()).unwrap();
        let mut builder = TransactionBundleBuilder::new(
            "foreign",
            "tx",
            0,
            "principal",
            HierarchicalRangeStampScheme::new(),
        );
        builder.put(key(1, b"key"), b"value".to_vec());

        let error = store
            .apply_certified_bundle(1, &builder.build().unwrap())
            .unwrap_err();
        assert!(error.to_string().contains("another cluster"));
        assert_eq!(store.applied_version().unwrap(), 0);
    }

    #[test]
    fn tombstones_hide_only_snapshots_at_and_after_the_delete() {
        let temp = tempdir().unwrap();
        let store = MvccStore::open(temp.path()).unwrap();
        let row = key(1, b"k");
        store
            .apply_certified_bundle(
                3,
                &bundle("put", |b| {
                    b.put(row.clone(), b"value".to_vec());
                }),
            )
            .unwrap();
        store
            .apply_certified_bundle(
                8,
                &bundle("delete", |b| {
                    b.delete(row.clone());
                }),
            )
            .unwrap();

        assert!(store.read_at(&row, 7).unwrap().is_some());
        assert_eq!(store.read_at(&row, 8).unwrap(), None);
        assert_eq!(store.read_latest(&row).unwrap(), None);
    }

    #[test]
    fn non_data_decisions_advance_the_readable_snapshot_watermark() {
        let temp = tempdir().unwrap();
        let store = MvccStore::open(temp.path()).unwrap();
        let row = key(1, b"missing");

        store.advance_decision_watermark(1).unwrap();

        assert_eq!(store.applied_version().unwrap(), 0);
        assert_eq!(store.decision_watermark().unwrap(), 1);
        assert_eq!(store.readable_version().unwrap(), 1);
        assert_eq!(store.read_at(&row, 1).unwrap(), None);
        assert!(
            store
                .read_at(&row, 2)
                .unwrap_err()
                .to_string()
                .contains("snapshot 2 is above local readable version 1")
        );
    }

    #[test]
    fn applying_a_bundle_is_idempotent_but_version_reuse_is_rejected() {
        let temp = tempdir().unwrap();
        let store = MvccStore::open(temp.path()).unwrap();
        let first = bundle("first", |b| {
            b.put(key(1, b"a"), b"a".to_vec());
        });
        assert_eq!(
            store.apply_certified_bundle(4, &first).unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(
            store.apply_certified_bundle(4, &first).unwrap(),
            ApplyOutcome::Replayed
        );
        let other = bundle("other", |b| {
            b.put(key(1, b"a"), b"b".to_vec());
        });
        assert!(store.apply_certified_bundle(4, &other).is_err());
        assert_eq!(store.applied_version().unwrap(), 4);
    }

    #[test]
    fn gc_keeps_the_visibility_anchor_and_newer_history() {
        let temp = tempdir().unwrap();
        let store = MvccStore::open(temp.path()).unwrap();
        let row = key(2, b"row");
        for (version, value) in [(2, b"two".as_slice()), (5, b"five"), (9, b"nine")] {
            store
                .apply_certified_bundle(
                    version,
                    &bundle(&format!("v{version}"), |b| {
                        b.put(row.clone(), value.to_vec());
                    }),
                )
                .unwrap();
        }

        assert_eq!(store.garbage_collect(6).unwrap(), 3);
        assert_eq!(store.gc_watermark().unwrap(), 6);
        assert_eq!(store.read_at(&row, 6).unwrap().unwrap().value, b"five");
        assert_eq!(store.read_latest(&row).unwrap().unwrap().value, b"nine");
        assert!(store.garbage_collect(5).is_err());
    }

    #[test]
    fn gc_preserves_tombstone_anchor_at_the_watermark() {
        let temp = tempdir().unwrap();
        let store = MvccStore::open(temp.path()).unwrap();
        let row = key(2, b"deleted");
        store
            .apply_certified_bundle(
                2,
                &bundle("put-before-delete", |builder| {
                    builder.put(row.clone(), b"value".to_vec());
                }),
            )
            .unwrap();
        store
            .apply_certified_bundle(
                5,
                &bundle("delete-anchor", |builder| {
                    builder.delete(row.clone());
                }),
            )
            .unwrap();
        store
            .apply_certified_bundle(8, &bundle("later-unrelated", |_| {}))
            .unwrap();

        store.garbage_collect(6).unwrap();
        assert_eq!(store.read_at(&row, 6).unwrap(), None);
        assert_eq!(store.read_latest(&row).unwrap(), None);
    }

    #[test]
    fn unfinished_outbox_work_pins_gc_and_delivered_history_is_reclaimed() {
        let temp = tempdir().unwrap();
        let store = MvccStore::open(temp.path()).unwrap();
        store
            .apply_certified_bundle(
                2,
                &bundle("outbox", |builder| {
                    builder.add_outbox_event(b"event".to_vec());
                }),
            )
            .unwrap();
        store
            .apply_certified_bundle(5, &bundle("advance", |_| {}))
            .unwrap();

        assert_eq!(
            store.unfinished_work_pins().unwrap().outbox_versions,
            [2_u64].into_iter().collect()
        );
        assert!(store.garbage_collect(5).is_err());

        let record = store.claim_outbox("worker", 10, 10).unwrap().unwrap();
        store.complete_outbox(&record, "worker").unwrap();
        assert!(store.unfinished_work_pins().unwrap().all().is_empty());
        store.garbage_collect(5).unwrap();
        assert!(store.outbox_records_after(0, 10).unwrap().is_empty());
    }

    #[test]
    fn one_batch_updates_multiple_tables_and_survives_reopen() {
        let temp = tempdir().unwrap();
        let a = key(1, b"same");
        let b = key(9, b"same");
        {
            let store = MvccStore::open(temp.path()).unwrap();
            let transaction = bundle("cross-table", |builder| {
                builder.put(a.clone(), b"a".to_vec());
                builder.put(b.clone(), b"b".to_vec());
            });
            store.apply_certified_bundle(11, &transaction).unwrap();
        }
        let reopened = MvccStore::open(temp.path()).unwrap();
        assert_eq!(reopened.applied_version().unwrap(), 11);
        assert_eq!(reopened.read_latest(&a).unwrap().unwrap().value, b"a");
        assert_eq!(reopened.read_latest(&b).unwrap().unwrap().value, b"b");
    }
}
