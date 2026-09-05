use std::collections::BTreeMap;
use std::fmt;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::object_alias_registry::{applied_key as alias_applied_key, decode_registry};
use super::receipt_codec::decode_stored_receipt;
use super::{
    CF_DEFINITION_STATE, CF_HEADS, CF_METADATA, CF_OBJECT_ALIAS_REGISTRIES, CF_RECEIPTS,
    CF_VERSIONS, RECEIPT_RECORD_PREFIX, STORAGE_KEY_FORMAT_VERSION, StoredReceipt,
    VERSION_HIGH_WATERMARK_KEY, now_unix_millis, receipt_key, version_blob_reference,
};
use crate::key::{BucketId, BucketIdentity, TenantId};
use crate::{
    DefinitionKind, DefinitionLocator, Head, MAX_CONTENT_TYPE_BYTES, MUTATION_STAMP_FORMAT,
    MutationError, ObjectAliasRegistry, ObjectAliasRegistryTransition, ObjectKey, ObjectMutation,
    Store, Version, VersionId,
};

pub const MAX_OBJECT_RECORD_EXPORT_RECORDS: u32 = 1_000;
pub const MAX_OBJECT_RECORD_EXPORT_BYTES: u64 = 64 * 1024 * 1024;
const OBJECT_CURSOR_FORMAT: u8 = 1;
const MAX_OBJECT_CURSOR_KEY_BYTES: usize = 16 * 1024;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectRecordCursor(String);

impl ObjectRecordCursor {
    pub fn from_token(token: impl Into<String>) -> Result<Self, ObjectSnapshotError> {
        let cursor = Self(token.into());
        cursor.decode()?;
        Ok(cursor)
    }

    pub fn as_token(&self) -> &str {
        &self.0
    }

    fn from_position(position: &ObjectCursorPosition) -> Self {
        let mut bytes = Vec::with_capacity(2 + position.key.len());
        bytes.push(OBJECT_CURSOR_FORMAT);
        bytes.push(position.domain as u8);
        bytes.extend_from_slice(&position.key);
        Self(URL_SAFE_NO_PAD.encode(bytes))
    }

    fn decode(&self) -> Result<ObjectCursorPosition, ObjectSnapshotError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(&self.0)
            .map_err(|_| ObjectSnapshotError::InvalidCursor)?;
        if bytes.first() != Some(&OBJECT_CURSOR_FORMAT)
            || bytes.len() <= 2
            || bytes.len() > MAX_OBJECT_CURSOR_KEY_BYTES + 2
        {
            return Err(ObjectSnapshotError::InvalidCursor);
        }
        let domain =
            ObjectExportDomain::from_byte(bytes[1]).ok_or(ObjectSnapshotError::InvalidCursor)?;
        let key = bytes[2..].to_vec();
        if !domain.matches(&key) {
            return Err(ObjectSnapshotError::InvalidCursor);
        }
        Ok(ObjectCursorPosition { domain, key })
    }
}

impl fmt::Debug for ObjectRecordCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectRecordCursor")
            .field("token", &"[OPAQUE]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPathSnapshot {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
    pub head: Head,
    pub versions: Vec<Version>,
    pub journal_pending_versions: Vec<VersionId>,
    pub journal_released_versions: Vec<VersionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_locator: Option<DefinitionLocator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias_registry: Option<ObjectAliasRegistry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias_registry_transition: Option<ObjectAliasRegistryTransition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum ObjectRecordExport {
    ExactPath(ObjectPathSnapshot),
    Receipt(ObjectMutation),
}

impl ObjectRecordExport {
    /// Canonical transfer order used by the bounded ADD-handoff merge.
    ///
    /// The leading byte preserves the export-domain order; the remainder is
    /// the same stable storage identity used by the corresponding iterator.
    /// It deliberately exposes no stored value or mutable display name.
    pub fn handoff_order_key(&self) -> Result<Vec<u8>, ObjectSnapshotError> {
        self.validate()?;
        let (domain, key) = match self {
            Self::ExactPath(record) => (
                ObjectExportDomain::ExactPath,
                stable_identity(record.tenant_id, record.bucket_id).head_key(&record.exact_path),
            ),
            Self::Receipt(mutation) => (
                ObjectExportDomain::Receipt,
                receipt_key(
                    stable_identity(mutation.tenant_id, mutation.bucket_id),
                    &mutation.command_id,
                ),
            ),
        };
        let mut ordered = Vec::with_capacity(key.len() + 1);
        ordered.push(domain as u8);
        ordered.extend_from_slice(&key);
        Ok(ordered)
    }

    pub fn tenant_id(&self) -> u64 {
        match self {
            Self::ExactPath(record) => record.tenant_id,
            Self::Receipt(mutation) => mutation.tenant_id,
        }
    }

    pub fn bucket_id(&self) -> u64 {
        match self {
            Self::ExactPath(record) => record.bucket_id,
            Self::Receipt(mutation) => mutation.bucket_id,
        }
    }

    pub fn exact_path(&self) -> &str {
        match self {
            Self::ExactPath(record) => &record.exact_path,
            Self::Receipt(mutation) => &mutation.exact_path,
        }
    }

    pub fn validate(&self) -> Result<(), ObjectSnapshotError> {
        match self {
            Self::ExactPath(record) => record.validate(),
            Self::Receipt(mutation) => mutation
                .validate()
                .map_err(|error| invalid_snapshot(error.to_string())),
        }
    }
}

impl ObjectPathSnapshot {
    pub fn validate(&self) -> Result<(), ObjectSnapshotError> {
        require_nonzero(self.tenant_id, "tenant ID")?;
        require_nonzero(self.bucket_id, "bucket ID")?;
        validate_exact_path(&self.exact_path)?;
        if self.head.version.0 == 0 || self.versions.is_empty() {
            return Err(invalid_snapshot(
                "head version and retained descriptor set must be non-empty",
            ));
        }
        if let Some(stamp) = self.head.mutation_stamp {
            if stamp.format != MUTATION_STAMP_FORMAT
                || stamp.predecessor_version == Some(self.head.version)
                || stamp
                    .predecessor_version
                    .is_some_and(|predecessor| predecessor >= self.head.version)
                || stamp.program_commit_cursor == Some(0)
                || stamp.serving_fence_term == 0
                || stamp.source_id.node_id == 0
                || stamp.source_id.source_epoch == [0; 32]
                || stamp.source_journal_position == 0
            {
                return Err(invalid_snapshot("head mutation stamp is malformed"));
            }
        }
        let mut previous = None;
        let mut current = None;
        for version in &self.versions {
            crate::model::validate_version_descriptor(version)
                .map_err(|error| invalid_snapshot(error.to_string()))?;
            if version.id.0 == 0
                || previous.is_some_and(|previous| previous >= version.id)
                || version.id > self.head.version
                || version
                    .content_type
                    .as_ref()
                    .is_some_and(|content_type| content_type.len() > MAX_CONTENT_TYPE_BYTES)
            {
                return Err(invalid_snapshot(
                    "version descriptors must be valid, sorted, and unique",
                ));
            }
            version_blob_reference(version).map_err(|error| invalid_snapshot(error.to_string()))?;
            if version.id == self.head.version {
                current = Some(version);
            }
            previous = Some(version.id);
        }
        if !self
            .journal_pending_versions
            .windows(2)
            .all(|pair| pair[0] < pair[1])
            || self.journal_pending_versions.iter().any(|id| {
                self.versions
                    .binary_search_by_key(id, |version| version.id)
                    .is_err()
            })
            || !self
                .journal_released_versions
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self.journal_released_versions.iter().any(|id| {
                self.versions
                    .binary_search_by_key(id, |version| version.id)
                    .is_err()
                    || self.journal_pending_versions.binary_search(id).is_ok()
            })
        {
            return Err(invalid_snapshot(
                "journal-only descriptor identities are not an ordered subset",
            ));
        }
        let current = current.ok_or_else(|| {
            invalid_snapshot("head references a missing retained version descriptor")
        })?;
        if current.deleted != self.head.deleted {
            return Err(invalid_snapshot(
                "head and current version descriptor disagree",
            ));
        }
        if let Some(locator) = self.definition_locator.as_ref() {
            locator
                .validate()
                .map_err(|error| invalid_snapshot(error.to_string()))?;
            if self.head.deleted
                || locator.tenant_id != self.tenant_id
                || locator.bucket_id != self.bucket_id
                || locator.path != self.exact_path
                || locator.object_version != self.head.version
            {
                return Err(invalid_snapshot(
                    "definition locator does not match the live snapshot head",
                ));
            }
        }
        if let Some(registry) = self.alias_registry.as_ref() {
            registry
                .validate(&self.exact_path)
                .map_err(|error| invalid_snapshot(error.to_string()))?;
            if current.deleted || current.protected_link_descriptor {
                return Err(invalid_snapshot(
                    "alias registry cannot name a deleted or protected canonical target",
                ));
            }
        }
        if let Some(transition) = self.alias_registry_transition.as_ref() {
            transition
                .validate()
                .map_err(|error| invalid_snapshot(error.to_string()))?;
            if transition.replacement_hash
                != self
                    .alias_registry
                    .as_ref()
                    .map(ObjectAliasRegistry::canonical_hash)
                    .transpose()
                    .map_err(|error| invalid_snapshot(error.to_string()))?
                || self.alias_registry.as_ref().is_some_and(|registry| {
                    registry.program_commit_cursor != Some(transition.commit_cursor)
                })
            {
                return Err(invalid_snapshot(
                    "alias registry transition does not match the sidecar",
                ));
            }
        } else if self.alias_registry.is_some() {
            return Err(invalid_snapshot(
                "alias registry is missing its idempotent transition marker",
            ));
        }
        Ok(())
    }
}

fn snapshot_retention(
    snapshot: &ObjectPathSnapshot,
    version: VersionId,
) -> super::StoredVersionRetention {
    if snapshot
        .journal_pending_versions
        .binary_search(&version)
        .is_ok()
    {
        super::StoredVersionRetention::JournalPending
    } else if snapshot
        .journal_released_versions
        .binary_search(&version)
        .is_ok()
    {
        super::StoredVersionRetention::JournalReleased
    } else {
        super::StoredVersionRetention::UserRetained
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRecordExportPage {
    pub records: Vec<ObjectRecordExport>,
    pub next_cursor: Option<ObjectRecordCursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectSnapshotApplied {
    pub version: Option<VersionId>,
    pub replayed: bool,
    pub retained: bool,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ObjectSnapshotError {
    #[error("object-state export cursor is invalid")]
    InvalidCursor,
    #[error("object-state export limits are invalid: {0}")]
    InvalidExportLimit(String),
    #[error("one object-state record requires {required_bytes} bytes, exceeding the page limit")]
    ExportRecordTooLarge { required_bytes: u64 },
    #[error("invalid object-state snapshot: {0}")]
    InvalidRecord(String),
    #[error("object-state snapshot conflicts with an existing local value")]
    SnapshotConflict,
    #[error("object-state repair observation is no longer current")]
    RepairPreconditionFailed,
    #[error("object-state snapshot storage failed: {0}")]
    Storage(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
enum ObjectExportDomain {
    ExactPath = 0,
    Receipt = 1,
}

impl ObjectExportDomain {
    fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::ExactPath),
            1 => Some(Self::Receipt),
            _ => None,
        }
    }

    fn matches(self, key: &[u8]) -> bool {
        match self {
            Self::ExactPath => {
                key.len() > BucketIdentity::ENCODED_BYTES
                    && key.first() == Some(&STORAGE_KEY_FORMAT_VERSION)
            }
            Self::Receipt => {
                key.len() == 34 && key[..2] == [STORAGE_KEY_FORMAT_VERSION, RECEIPT_RECORD_PREFIX]
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ObjectCursorPosition {
    domain: ObjectExportDomain,
    key: Vec<u8>,
}

impl Store {
    /// Reads one exact-path record by stable storage identity. Callers may
    /// compare independently read replicas and choose a reconciled candidate;
    /// reconciliation policy deliberately remains outside the store.
    pub fn export_object_path_record(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
    ) -> Result<Option<ObjectPathSnapshot>, ObjectSnapshotError> {
        require_nonzero(tenant_id, "tenant ID")?;
        require_nonzero(bucket_id, "bucket ID")?;
        validate_exact_path(exact_path)?;
        let head_key = stable_identity(tenant_id, bucket_id).head_key(exact_path);
        self.current_path_snapshot(&head_key)
    }

    /// Reads a bounded ordered set of complete exact-path records from one
    /// RocksDB snapshot. This is the batched peer reconciliation boundary used
    /// before a distributed mutation group is evaluated.
    pub fn export_object_path_records(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        exact_paths: &[String],
    ) -> Result<Vec<Option<ObjectPathSnapshot>>, ObjectSnapshotError> {
        require_nonzero(tenant_id, "tenant ID")?;
        require_nonzero(bucket_id, "bucket ID")?;
        if exact_paths.is_empty() || exact_paths.len() > MAX_OBJECT_RECORD_EXPORT_RECORDS as usize {
            return Err(ObjectSnapshotError::InvalidExportLimit(format!(
                "object snapshot batch records must be 1..={MAX_OBJECT_RECORD_EXPORT_RECORDS}"
            )));
        }
        let identity = stable_identity(tenant_id, bucket_id);
        let mut head_keys = Vec::with_capacity(exact_paths.len());
        for exact_path in exact_paths {
            validate_exact_path(exact_path)?;
            head_keys.push(identity.head_key(exact_path));
        }
        let snapshot = self.db.snapshot();
        head_keys
            .iter()
            .map(|head_key| self.path_snapshot_at(&snapshot, head_key))
            .collect()
    }

    /// Enumerates local authoritative exact-path state and retained typed
    /// receipts. Cluster placement filtering belongs to the join coordinator.
    pub fn export_object_records(
        &self,
        cursor: Option<&ObjectRecordCursor>,
        max_records: u32,
        max_bytes: u64,
    ) -> Result<ObjectRecordExportPage, ObjectSnapshotError> {
        validate_limits(max_records, max_bytes)?;
        let after = cursor.map(ObjectRecordCursor::decode).transpose()?;
        let snapshot = self.db.snapshot();
        let now = now_unix_millis().map_err(object_storage)?;
        let mut records = Vec::with_capacity(max_records as usize);
        let mut encoded_bytes = 0_u64;
        let mut last_position = None;

        if !after
            .as_ref()
            .is_some_and(|position| position.domain > ObjectExportDomain::ExactPath)
        {
            let prefix = vec![STORAGE_KEY_FORMAT_VERSION];
            let start = after
                .as_ref()
                .filter(|position| position.domain == ObjectExportDomain::ExactPath)
                .map_or_else(|| prefix.clone(), |position| position.key.clone());
            for item in snapshot.iterator_cf(
                self.cf(CF_HEADS).map_err(object_storage)?,
                IteratorMode::From(&start, Direction::Forward),
            ) {
                let (key, encoded_head) = item.map_err(object_storage)?;
                if !key.starts_with(&prefix) {
                    break;
                }
                if !ObjectExportDomain::ExactPath.matches(&key) {
                    return Err(object_storage("object head key is malformed"));
                }
                if after.as_ref().is_some_and(|position| {
                    position.domain == ObjectExportDomain::ExactPath
                        && key.as_ref() <= position.key.as_slice()
                }) {
                    continue;
                }
                let locator = definition_locator_for_head_key(
                    &snapshot,
                    self.cf(CF_DEFINITION_STATE).map_err(object_storage)?,
                    &key,
                )?;
                let (alias_registry, alias_registry_transition) = alias_snapshot_for_head_key(
                    &snapshot,
                    self.cf(CF_OBJECT_ALIAS_REGISTRIES)
                        .map_err(object_storage)?,
                    &key,
                )?;
                let record = ObjectRecordExport::ExactPath(decode_path_snapshot(
                    key.as_ref(),
                    encoded_head.as_ref(),
                    snapshot.iterator_cf(
                        self.cf(CF_VERSIONS).map_err(object_storage)?,
                        IteratorMode::From(&version_prefix_for_head(&key), Direction::Forward),
                    ),
                    locator,
                    alias_registry,
                    alias_registry_transition,
                )?);
                if !append_export_record(
                    &mut records,
                    &mut encoded_bytes,
                    max_records,
                    max_bytes,
                    &record,
                )? {
                    return Ok(export_page(records, last_position, true));
                }
                last_position = Some(ObjectCursorPosition {
                    domain: ObjectExportDomain::ExactPath,
                    key: key.to_vec(),
                });
            }
        }

        let receipt_prefix = [STORAGE_KEY_FORMAT_VERSION, RECEIPT_RECORD_PREFIX];
        let start = after
            .as_ref()
            .filter(|position| position.domain == ObjectExportDomain::Receipt)
            .map_or_else(|| receipt_prefix.to_vec(), |position| position.key.clone());
        for item in snapshot.iterator_cf(
            self.cf(CF_RECEIPTS).map_err(object_storage)?,
            IteratorMode::From(&start, Direction::Forward),
        ) {
            let (key, encoded) = item.map_err(object_storage)?;
            if !key.starts_with(&receipt_prefix) {
                break;
            }
            if !ObjectExportDomain::Receipt.matches(&key) {
                return Err(object_storage("object receipt key is malformed"));
            }
            if after.as_ref().is_some_and(|position| {
                position.domain == ObjectExportDomain::Receipt
                    && key.as_ref() <= position.key.as_slice()
            }) {
                continue;
            }
            let stored = decode_stored_receipt(&encoded).map_err(object_storage)?;
            if stored.expires_at_unix_millis <= now {
                continue;
            }
            let Some(mutation) = stored.object_mutation.clone() else {
                // Released 0.5.0 receipts did not carry enough typed identity
                // to transfer safely. Their local retry window remains local.
                continue;
            };
            validate_stored_receipt(&stored, &mutation, &key)?;
            let record = ObjectRecordExport::Receipt(mutation);
            if !append_export_record(
                &mut records,
                &mut encoded_bytes,
                max_records,
                max_bytes,
                &record,
            )? {
                return Ok(export_page(records, last_position, true));
            }
            last_position = Some(ObjectCursorPosition {
                domain: ObjectExportDomain::Receipt,
                key: key.to_vec(),
            });
        }

        Ok(ObjectRecordExportPage {
            records,
            next_cursor: None,
        })
    }

    /// Installs one quorum-reconciled join/handoff record. This is a snapshot
    /// bootstrap boundary, not an ordinary mutation path and it emits no local
    /// source-journal entry or content-reference effect.
    pub async fn install_quorum_reconciled_object_record(
        &self,
        record: &ObjectRecordExport,
    ) -> Result<ObjectSnapshotApplied, ObjectSnapshotError> {
        record.validate()?;
        let _guard = self.lock_commit("object_snapshot").await;
        match record {
            ObjectRecordExport::ExactPath(record) => self.install_path_snapshot(record),
            ObjectRecordExport::Receipt(mutation) => self.install_receipt_snapshot(mutation),
        }
    }

    /// Replaces one exact-path replica with a state already selected by an
    /// external read quorum. This is the read-repair boundary: it accepts a
    /// missing state as well as an older, newer, or divergent observed state
    /// and installs the complete selected head and retained-version set in one
    /// synchronous RocksDB batch. The observed state is an exact compare
    /// condition so a delayed repair cannot roll back a concurrent commit.
    ///
    /// Quorum selection and serving-fence validation deliberately remain in
    /// the cluster layer. Repair emits no source-journal entry and applies no
    /// content-reference effect.
    pub async fn repair_object_path_snapshot(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
        expected: Option<&ObjectPathSnapshot>,
        selected: Option<&ObjectPathSnapshot>,
    ) -> Result<ObjectSnapshotApplied, ObjectSnapshotError> {
        require_nonzero(tenant_id, "tenant ID")?;
        require_nonzero(bucket_id, "bucket ID")?;
        validate_exact_path(exact_path)?;
        for snapshot in [expected, selected].into_iter().flatten() {
            validate_snapshot_request(snapshot, tenant_id, bucket_id, exact_path)?;
        }

        let _guard = self.lock_commit("object_snapshot").await;
        let identity = stable_identity(tenant_id, bucket_id);
        let head_key = identity.head_key(exact_path);
        let current = self.current_path_snapshot(&head_key)?;
        if current.as_ref() != expected {
            return Err(ObjectSnapshotError::RepairPreconditionFailed);
        }
        if current.as_ref() == selected {
            return Ok(ObjectSnapshotApplied {
                version: selected.map(|snapshot| snapshot.head.version),
                replayed: true,
                retained: selected.is_some(),
            });
        }

        let versions = self.cf(CF_VERSIONS).map_err(object_storage)?;
        let version_prefix = version_prefix_for_head(&head_key);
        let mut batch = WriteBatch::default();
        batch.delete_cf(self.cf(CF_HEADS).map_err(object_storage)?, &head_key);
        for item in self.db.iterator_cf(
            versions,
            IteratorMode::From(&version_prefix, Direction::Forward),
        ) {
            let (key, _) = item.map_err(object_storage)?;
            if !key.starts_with(&version_prefix) {
                break;
            }
            batch.delete_cf(versions, key);
        }
        self.stage_snapshot_locator(
            &mut batch,
            tenant_id,
            bucket_id,
            exact_path,
            selected.and_then(|snapshot| snapshot.definition_locator.as_ref()),
        )?;
        stage_alias_snapshot(self, &mut batch, identity, exact_path, selected)?;

        if let Some(selected) = selected {
            for version in &selected.versions {
                let retention = snapshot_retention(selected, version.id);
                batch.put_cf(
                    versions,
                    exact_version_key(&head_key, version.id),
                    serde_json::to_vec(&super::StoredVersion::new(version.clone(), retention))
                        .map_err(object_storage)?,
                );
            }
            batch.put_cf(
                self.cf(CF_HEADS).map_err(object_storage)?,
                &head_key,
                serde_json::to_vec(&selected.head).map_err(object_storage)?,
            );
            self.stage_object_high_watermark(&mut batch, selected.head.version)?;
        }

        self.write_object_snapshot_batch(batch)?;
        if let Some(selected) = selected {
            self.clock.observe(selected.head.version);
        }
        Ok(ObjectSnapshotApplied {
            version: selected.map(|snapshot| snapshot.head.version),
            replayed: false,
            retained: selected.is_some(),
        })
    }

    fn install_path_snapshot(
        &self,
        record: &ObjectPathSnapshot,
    ) -> Result<ObjectSnapshotApplied, ObjectSnapshotError> {
        let identity = stable_identity(record.tenant_id, record.bucket_id);
        let head_key = identity.head_key(&record.exact_path);
        match self.current_path_snapshot(&head_key)? {
            Some(existing) if existing == *record => {
                return Ok(ObjectSnapshotApplied {
                    version: Some(record.head.version),
                    replayed: true,
                    retained: true,
                });
            }
            Some(_) => return Err(ObjectSnapshotError::SnapshotConflict),
            None => {}
        }
        let version_prefix = version_prefix_for_head(&head_key);
        if self
            .db
            .iterator_cf(
                self.cf(CF_VERSIONS).map_err(object_storage)?,
                IteratorMode::From(&version_prefix, Direction::Forward),
            )
            .next()
            .transpose()
            .map_err(object_storage)?
            .is_some_and(|(key, _)| key.starts_with(&version_prefix))
        {
            return Err(ObjectSnapshotError::SnapshotConflict);
        }

        let mut batch = WriteBatch::default();
        for version in &record.versions {
            let retention = snapshot_retention(record, version.id);
            batch.put_cf(
                self.cf(CF_VERSIONS).map_err(object_storage)?,
                exact_version_key(&head_key, version.id),
                serde_json::to_vec(&super::StoredVersion::new(version.clone(), retention))
                    .map_err(object_storage)?,
            );
        }
        batch.put_cf(
            self.cf(CF_HEADS).map_err(object_storage)?,
            &head_key,
            serde_json::to_vec(&record.head).map_err(object_storage)?,
        );
        self.stage_snapshot_locator(
            &mut batch,
            record.tenant_id,
            record.bucket_id,
            &record.exact_path,
            record.definition_locator.as_ref(),
        )?;
        stage_alias_snapshot(self, &mut batch, identity, &record.exact_path, Some(record))?;
        self.stage_object_high_watermark(&mut batch, record.head.version)?;
        self.write_object_snapshot_batch(batch)?;
        self.clock.observe(record.head.version);
        Ok(ObjectSnapshotApplied {
            version: Some(record.head.version),
            replayed: false,
            retained: true,
        })
    }

    fn install_receipt_snapshot(
        &self,
        mutation: &ObjectMutation,
    ) -> Result<ObjectSnapshotApplied, ObjectSnapshotError> {
        let now = now_unix_millis().map_err(object_storage)?;
        if mutation.receipt_expires_at_unix_millis <= now {
            return Ok(ObjectSnapshotApplied {
                version: Some(mutation.version.id),
                replayed: false,
                retained: false,
            });
        }
        let identity = stable_identity(mutation.tenant_id, mutation.bucket_id);
        let primary_key = receipt_key(identity, &mutation.command_id);
        let stored = stored_receipt(mutation);
        if let Some(existing) = self
            .read_stored_receipt(&primary_key)
            .map_err(object_storage)?
        {
            if existing.expires_at_unix_millis <= now {
                // The bounded retry guarantee has ended. The batch below
                // prunes this stale value before installing the live record.
            } else if existing == stored {
                return Ok(ObjectSnapshotApplied {
                    version: Some(mutation.version.id),
                    replayed: true,
                    retained: true,
                });
            } else {
                return Err(ObjectSnapshotError::SnapshotConflict);
            }
        }

        let mut batch = WriteBatch::default();
        let mut status = self.mutation_receipt_status().map_err(object_storage)?;
        let initial_status = status;
        self.stage_expired_mutation_receipts(&mut batch, now, &mut status)
            .map_err(object_storage)?;
        self.stage_stored_mutation_receipt(
            &mut batch,
            primary_key,
            stored,
            &mut status,
            &mut BTreeMap::new(),
        )
        .map_err(object_storage)?;
        if status != initial_status {
            self.stage_mutation_receipt_status(&mut batch, status)
                .map_err(object_storage)?;
        }
        self.stage_object_high_watermark(&mut batch, mutation.version.id)?;
        self.write_object_snapshot_batch(batch)?;
        self.clock.observe(mutation.version.id);
        Ok(ObjectSnapshotApplied {
            version: Some(mutation.version.id),
            replayed: false,
            retained: true,
        })
    }

    fn current_path_snapshot(
        &self,
        head_key: &[u8],
    ) -> Result<Option<ObjectPathSnapshot>, ObjectSnapshotError> {
        let snapshot = self.db.snapshot();
        self.path_snapshot_at(&snapshot, head_key)
    }

    fn path_snapshot_at(
        &self,
        snapshot: &rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>,
        head_key: &[u8],
    ) -> Result<Option<ObjectPathSnapshot>, ObjectSnapshotError> {
        let Some(encoded_head) = snapshot
            .get_cf(self.cf(CF_HEADS).map_err(object_storage)?, head_key)
            .map_err(object_storage)?
        else {
            return Ok(None);
        };
        let locator = definition_locator_for_head_key(
            snapshot,
            self.cf(CF_DEFINITION_STATE).map_err(object_storage)?,
            head_key,
        )?;
        let (alias_registry, alias_registry_transition) = alias_snapshot_for_head_key(
            snapshot,
            self.cf(CF_OBJECT_ALIAS_REGISTRIES)
                .map_err(object_storage)?,
            head_key,
        )?;
        decode_path_snapshot(
            head_key,
            &encoded_head,
            snapshot.iterator_cf(
                self.cf(CF_VERSIONS).map_err(object_storage)?,
                IteratorMode::From(&version_prefix_for_head(head_key), Direction::Forward),
            ),
            locator,
            alias_registry,
            alias_registry_transition,
        )
        .map(Some)
    }

    fn stage_object_high_watermark(
        &self,
        batch: &mut WriteBatch,
        incoming: VersionId,
    ) -> Result<(), ObjectSnapshotError> {
        let high_watermark = self
            .read_json::<VersionId>(CF_METADATA, VERSION_HIGH_WATERMARK_KEY)
            .map_err(object_storage)?
            .map_or(incoming, |current| current.max(incoming));
        batch.put_cf(
            self.cf(CF_METADATA).map_err(object_storage)?,
            VERSION_HIGH_WATERMARK_KEY,
            serde_json::to_vec(&high_watermark).map_err(object_storage)?,
        );
        Ok(())
    }

    fn stage_snapshot_locator(
        &self,
        batch: &mut WriteBatch,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
        locator: Option<&DefinitionLocator>,
    ) -> Result<(), ObjectSnapshotError> {
        let definition_state = self.cf(CF_DEFINITION_STATE).map_err(object_storage)?;
        for kind in DefinitionKind::ALL {
            let key = super::definition_state::locator_key(kind, tenant_id, bucket_id, exact_path)
                .map_err(object_storage)?;
            batch.delete_cf(definition_state, key);
        }
        if let Some(locator) = locator {
            let key = super::definition_state::locator_key(
                locator.kind,
                locator.tenant_id,
                locator.bucket_id,
                &locator.path,
            )
            .map_err(object_storage)?;
            batch.put_cf(
                definition_state,
                key,
                super::definition_state::encode_locator(locator),
            );
        }
        Ok(())
    }

    fn write_object_snapshot_batch(&self, batch: WriteBatch) -> Result<(), ObjectSnapshotError> {
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(object_storage)
    }
}

pub(super) fn validate_limits(max_records: u32, max_bytes: u64) -> Result<(), ObjectSnapshotError> {
    if max_records == 0
        || max_records > MAX_OBJECT_RECORD_EXPORT_RECORDS
        || max_bytes == 0
        || max_bytes > MAX_OBJECT_RECORD_EXPORT_BYTES
    {
        return Err(ObjectSnapshotError::InvalidExportLimit(format!(
            "records must be 1..={MAX_OBJECT_RECORD_EXPORT_RECORDS} and bytes must be 1..={MAX_OBJECT_RECORD_EXPORT_BYTES}"
        )));
    }
    Ok(())
}

fn append_export_record(
    records: &mut Vec<ObjectRecordExport>,
    encoded_bytes: &mut u64,
    max_records: u32,
    max_bytes: u64,
    record: &ObjectRecordExport,
) -> Result<bool, ObjectSnapshotError> {
    record.validate()?;
    let record_bytes = u64::try_from(serde_json::to_vec(record).map_err(object_storage)?.len())
        .map_err(|_| object_storage("object-state record size overflow"))?;
    if record_bytes > MAX_OBJECT_RECORD_EXPORT_BYTES
        || (records.is_empty() && record_bytes > max_bytes)
    {
        return Err(ObjectSnapshotError::ExportRecordTooLarge {
            required_bytes: record_bytes,
        });
    }
    if records.len() == max_records as usize
        || encoded_bytes.saturating_add(record_bytes) > max_bytes
    {
        return Ok(false);
    }
    *encoded_bytes += record_bytes;
    records.push(record.clone());
    Ok(true)
}

fn export_page(
    records: Vec<ObjectRecordExport>,
    last_position: Option<ObjectCursorPosition>,
    truncated: bool,
) -> ObjectRecordExportPage {
    ObjectRecordExportPage {
        records,
        next_cursor: if truncated {
            last_position
                .as_ref()
                .map(ObjectRecordCursor::from_position)
        } else {
            None
        },
    }
}

fn decode_path_snapshot<I>(
    encoded_head_key: &[u8],
    encoded_head: &[u8],
    versions: I,
    definition_locator: Option<DefinitionLocator>,
    alias_registry: Option<ObjectAliasRegistry>,
    alias_registry_transition: Option<ObjectAliasRegistryTransition>,
) -> Result<ObjectPathSnapshot, ObjectSnapshotError>
where
    I: IntoIterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>,
{
    if !ObjectExportDomain::ExactPath.matches(encoded_head_key) {
        return Err(object_storage("object head key is malformed"));
    }
    let tenant_id = read_u64(&encoded_head_key[1..9])?;
    let bucket_id = read_u64(&encoded_head_key[9..17])?;
    let exact_path = std::str::from_utf8(&encoded_head_key[17..])
        .map_err(object_storage)?
        .to_owned();
    let head: Head = serde_json::from_slice(encoded_head).map_err(object_storage)?;
    let version_prefix = version_prefix_for_head(encoded_head_key);
    let mut retained = Vec::new();
    let mut journal_pending_versions = Vec::new();
    let mut journal_released_versions = Vec::new();
    for item in versions {
        let (key, encoded) = item.map_err(object_storage)?;
        if !key.starts_with(&version_prefix) {
            break;
        }
        if key.len() != version_prefix.len() + 8 {
            return Err(object_storage("retained version key is malformed"));
        }
        let key_version = VersionId(read_u64(&key[version_prefix.len()..])?);
        let stored = super::StoredVersion::decode(&encoded).map_err(object_storage)?;
        let version = stored.version;
        if version.id != key_version {
            return Err(object_storage(
                "retained version key and descriptor disagree",
            ));
        }
        if stored.retention == super::StoredVersionRetention::JournalPending {
            journal_pending_versions.push(version.id);
        } else if stored.retention == super::StoredVersionRetention::JournalReleased {
            journal_released_versions.push(version.id);
        }
        retained.push(version);
    }
    let record = ObjectPathSnapshot {
        tenant_id,
        bucket_id,
        exact_path,
        head,
        versions: retained,
        journal_pending_versions,
        journal_released_versions,
        definition_locator,
        alias_registry,
        alias_registry_transition,
    };
    record.validate()?;
    Ok(record)
}

fn alias_snapshot_for_head_key(
    snapshot: &rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>,
    aliases_cf: &rocksdb::ColumnFamily,
    encoded_head_key: &[u8],
) -> Result<
    (
        Option<ObjectAliasRegistry>,
        Option<ObjectAliasRegistryTransition>,
    ),
    ObjectSnapshotError,
> {
    if encoded_head_key.len() <= BucketIdentity::ENCODED_BYTES {
        return Err(object_storage("object head key is malformed"));
    }
    let identity = BucketIdentity::decode(&encoded_head_key[..BucketIdentity::ENCODED_BYTES])
        .map_err(object_storage)?;
    let canonical_path = std::str::from_utf8(&encoded_head_key[BucketIdentity::ENCODED_BYTES..])
        .map_err(object_storage)?;
    let registry = snapshot
        .get_cf(aliases_cf, encoded_head_key)
        .map_err(object_storage)?
        .map(|encoded| decode_registry(&encoded).map_err(object_storage))
        .transpose()?;
    if let Some(registry) = registry.as_ref() {
        registry.validate(canonical_path).map_err(object_storage)?;
    }
    let transition = snapshot
        .get_cf(aliases_cf, alias_applied_key(identity, canonical_path))
        .map_err(object_storage)?
        .map(|encoded| serde_json::from_slice(&encoded).map_err(object_storage))
        .transpose()?;
    Ok((registry, transition))
}

fn stage_alias_snapshot(
    store: &Store,
    batch: &mut WriteBatch,
    identity: BucketIdentity,
    canonical_path: &str,
    selected: Option<&ObjectPathSnapshot>,
) -> Result<(), ObjectSnapshotError> {
    let aliases_cf = store
        .cf(CF_OBJECT_ALIAS_REGISTRIES)
        .map_err(object_storage)?;
    let sidecar_key = identity.head_key(canonical_path);
    let transition_key = alias_applied_key(identity, canonical_path);
    batch.delete_cf(aliases_cf, &sidecar_key);
    batch.delete_cf(aliases_cf, &transition_key);
    if let Some(selected) = selected {
        if let Some(registry) = selected.alias_registry.as_ref() {
            batch.put_cf(
                aliases_cf,
                &sidecar_key,
                registry.canonical_bytes().map_err(object_storage)?,
            );
        }
        if let Some(transition) = selected.alias_registry_transition.as_ref() {
            batch.put_cf(
                aliases_cf,
                transition_key,
                serde_json::to_vec(transition).map_err(object_storage)?,
            );
        }
    }
    Ok(())
}

fn definition_locator_for_head_key(
    snapshot: &rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>,
    definition_state: &rocksdb::ColumnFamily,
    encoded_head_key: &[u8],
) -> Result<Option<DefinitionLocator>, ObjectSnapshotError> {
    if encoded_head_key.len() <= BucketIdentity::ENCODED_BYTES {
        return Err(object_storage("object head key is malformed"));
    }
    let tenant_id = read_u64(&encoded_head_key[1..9])?;
    let bucket_id = read_u64(&encoded_head_key[9..17])?;
    let path = std::str::from_utf8(&encoded_head_key[17..]).map_err(object_storage)?;
    let mut selected = None;
    for kind in DefinitionKind::ALL {
        let key = super::definition_state::locator_key(kind, tenant_id, bucket_id, path)
            .map_err(object_storage)?;
        let Some(value) = snapshot
            .get_cf(definition_state, &key)
            .map_err(object_storage)?
        else {
            continue;
        };
        let locator =
            super::definition_state::decode_locator(&key, &value).map_err(object_storage)?;
        if selected.replace(locator).is_some() {
            return Err(invalid_snapshot(
                "one definition path has multiple typed locators",
            ));
        }
    }
    Ok(selected)
}

fn validate_stored_receipt(
    stored: &StoredReceipt,
    mutation: &ObjectMutation,
    encoded_key: &[u8],
) -> Result<(), ObjectSnapshotError> {
    mutation
        .validate()
        .map_err(|error| invalid_snapshot(error.to_string()))?;
    if stored.fingerprint != mutation.input_fingerprint
        || stored.version != mutation.version.id
        || stored.deleted != mutation.version.deleted
        || stored.expires_at_unix_millis != mutation.receipt_expires_at_unix_millis
        || stored.object_mutation.as_ref() != Some(mutation)
        || stored.definition_transition != mutation.definition_transition
        || receipt_key(
            stable_identity(mutation.tenant_id, mutation.bucket_id),
            &mutation.command_id,
        ) != encoded_key
    {
        return Err(invalid_snapshot(
            "retained receipt disagrees with its typed mutation or key",
        ));
    }
    Ok(())
}

fn stored_receipt(mutation: &ObjectMutation) -> StoredReceipt {
    StoredReceipt {
        fingerprint: mutation.input_fingerprint,
        version: mutation.version.id,
        deleted: mutation.version.deleted,
        expires_at_unix_millis: mutation.receipt_expires_at_unix_millis,
        object_mutation: Some(mutation.clone()),
        definition_transition: mutation.definition_transition.clone(),
    }
}

fn stable_identity(tenant_id: u64, bucket_id: u64) -> BucketIdentity {
    BucketIdentity {
        tenant_id: TenantId(tenant_id),
        bucket_id: BucketId(bucket_id),
    }
}

fn version_prefix_for_head(head_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(head_key.len() + 1);
    key.extend_from_slice(head_key);
    key.push(0);
    key
}

fn exact_version_key(head_key: &[u8], version: VersionId) -> Vec<u8> {
    let mut key = version_prefix_for_head(head_key);
    key.extend_from_slice(&version.0.to_be_bytes());
    key
}

fn read_u64(bytes: &[u8]) -> Result<u64, ObjectSnapshotError> {
    let encoded: [u8; 8] = bytes
        .try_into()
        .map_err(|_| object_storage("stable ID is malformed"))?;
    let value = u64::from_be_bytes(encoded);
    require_nonzero(value, "stable ID")?;
    Ok(value)
}

fn validate_exact_path(path: &str) -> Result<(), ObjectSnapshotError> {
    ObjectKey::new("t", "b", path)
        .map(|_| ())
        .map_err(|error| invalid_snapshot(error.to_string()))
}

fn validate_snapshot_request(
    snapshot: &ObjectPathSnapshot,
    tenant_id: u64,
    bucket_id: u64,
    exact_path: &str,
) -> Result<(), ObjectSnapshotError> {
    snapshot.validate()?;
    if snapshot.tenant_id != tenant_id
        || snapshot.bucket_id != bucket_id
        || snapshot.exact_path != exact_path
    {
        return Err(invalid_snapshot(
            "snapshot does not match the requested exact path",
        ));
    }
    Ok(())
}

fn require_nonzero(value: u64, label: &str) -> Result<(), ObjectSnapshotError> {
    if value == 0 {
        Err(invalid_snapshot(format!("{label} must be non-zero")))
    } else {
        Ok(())
    }
}

fn invalid_snapshot(error: impl fmt::Display) -> ObjectSnapshotError {
    ObjectSnapshotError::InvalidRecord(error.to_string())
}

fn object_storage(error: impl fmt::Display) -> ObjectSnapshotError {
    ObjectSnapshotError::Storage(error.to_string())
}

impl From<MutationError> for ObjectSnapshotError {
    fn from(error: MutationError) -> Self {
        object_storage(error)
    }
}

#[cfg(test)]
mod tests;
