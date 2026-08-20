use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};

use super::{CF_DEFINITION_STATE, Store};
use crate::key::STORAGE_KEY_FORMAT_VERSION;
use crate::{
    DeletedDefinitionCleanup, IndexGenerationRetentionDue, IndexRetentionDueError, VersionId,
};

const DUE_DOMAIN: u8 = b'D';
const IDENTITY_DOMAIN: u8 = b'd';
const VALUE_FORMAT: u8 = 1;
const GENERATION_KIND: u8 = 1;
const DELETED_DEFINITION_KIND: u8 = 2;
const DUE_KEY_BYTES: usize = 1 + 1 + 1 + 8 + 8 + 8 + 8;
const IDENTITY_KEY_BYTES: usize = 1 + 1 + 8 + 8 + 8;
const IDENTITY_VALUE_BYTES: usize = 1 + 1 + 8;
const DUE_VALUE_FIXED_BYTES: usize = 1 + 8 + 8;

impl Store {
    /// Idempotently installs the newest generation-retention schedule for one
    /// index. One identity locator and one due-ordered record are committed in
    /// the same local RocksDB batch; replacing a schedule never scans.
    pub fn schedule_index_generation_retention(
        &self,
        due: &IndexGenerationRetentionDue,
    ) -> Result<bool, IndexRetentionDueError> {
        due.validate()?;
        let record = StoredDue::Generation(due.clone());
        self.schedule_index_retention_due(&record)
    }

    pub fn oldest_index_generation_retention_due(
        &self,
    ) -> Result<Option<IndexGenerationRetentionDue>, IndexRetentionDueError> {
        let _guard = self.due_lock()?;
        self.oldest_due(StoredDueKind::Generation)?
            .map(StoredDue::into_generation)
            .transpose()
    }

    pub fn index_generation_retention_due_matches(
        &self,
        expected: &IndexGenerationRetentionDue,
    ) -> Result<bool, IndexRetentionDueError> {
        expected.validate()?;
        let _guard = self.due_lock()?;
        self.due_matches(&StoredDue::Generation(expected.clone()))
    }

    pub fn replace_index_generation_retention_due(
        &self,
        expected: &IndexGenerationRetentionDue,
        replacement: &IndexGenerationRetentionDue,
    ) -> Result<bool, IndexRetentionDueError> {
        expected.validate()?;
        replacement.validate()?;
        let expected = StoredDue::Generation(expected.clone());
        let replacement = StoredDue::Generation(replacement.clone());
        require_same_work(&expected, &replacement)?;
        self.replace_index_retention_due(&expected, &replacement)
    }

    pub fn complete_index_generation_retention_due(
        &self,
        expected: &IndexGenerationRetentionDue,
    ) -> Result<bool, IndexRetentionDueError> {
        expected.validate()?;
        self.complete_index_retention_due(&StoredDue::Generation(expected.clone()))
    }

    /// Removes generation-retention work when this node loses the assignment.
    /// A definition-deletion schedule for the same identity is intentionally
    /// left intact because it is a separate durable cleanup handoff.
    pub fn cancel_index_generation_retention(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<bool, IndexRetentionDueError> {
        if tenant_id == 0 || bucket_id == 0 || index_id == 0 {
            return Err(IndexRetentionDueError::Malformed(
                "stable IDs must be non-zero".into(),
            ));
        }
        let identity = DueIdentity {
            tenant_id,
            bucket_id,
            index_id,
        };
        let _guard = self.due_lock()?;
        let Some(existing @ StoredDue::Generation(_)) = self.due_for_identity(identity)? else {
            return Ok(false);
        };
        let cf = self.due_cf()?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(cf, due_key(&existing));
        batch.delete_cf(cf, identity_key(identity));
        self.write_due_batch(batch)?;
        Ok(true)
    }

    /// Schedules deleted-definition cleanup outside an assignment batch.
    /// Assignment delivery should instead call the staging hook below so the
    /// deletion and its cleanup evidence share one durable commit.
    pub fn schedule_deleted_definition_cleanup(
        &self,
        cleanup: &DeletedDefinitionCleanup,
    ) -> Result<bool, IndexRetentionDueError> {
        cleanup.validate()?;
        let record = StoredDue::DeletedDefinition(cleanup.clone());
        self.schedule_index_retention_due(&record)
    }

    pub fn oldest_deleted_definition_cleanup(
        &self,
    ) -> Result<Option<DeletedDefinitionCleanup>, IndexRetentionDueError> {
        let _guard = self.due_lock()?;
        self.oldest_due(StoredDueKind::DeletedDefinition)?
            .map(StoredDue::into_deleted_definition)
            .transpose()
    }

    pub fn deleted_definition_cleanup_matches(
        &self,
        expected: &DeletedDefinitionCleanup,
    ) -> Result<bool, IndexRetentionDueError> {
        expected.validate()?;
        let _guard = self.due_lock()?;
        self.due_matches(&StoredDue::DeletedDefinition(expected.clone()))
    }

    pub fn replace_deleted_definition_cleanup(
        &self,
        expected: &DeletedDefinitionCleanup,
        replacement: &DeletedDefinitionCleanup,
    ) -> Result<bool, IndexRetentionDueError> {
        expected.validate()?;
        replacement.validate()?;
        let expected = StoredDue::DeletedDefinition(expected.clone());
        let replacement = StoredDue::DeletedDefinition(replacement.clone());
        require_same_work(&expected, &replacement)?;
        self.replace_index_retention_due(&expected, &replacement)
    }

    pub fn complete_deleted_definition_cleanup(
        &self,
        expected: &DeletedDefinitionCleanup,
    ) -> Result<bool, IndexRetentionDueError> {
        expected.validate()?;
        self.complete_index_retention_due(&StoredDue::DeletedDefinition(expected.clone()))
    }

    /// Stages cleanup evidence into a caller-owned definition-state batch.
    /// The caller must hold `definition_state_lock`; this method deliberately
    /// performs no write of its own so deletion delivery and its checkpoint
    /// cannot become durable without the cleanup schedule.
    pub(crate) fn stage_deleted_definition_cleanup(
        &self,
        batch: &mut WriteBatch,
        cleanup: &DeletedDefinitionCleanup,
    ) -> Result<(), IndexRetentionDueError> {
        cleanup.validate()?;
        self.stage_newer_due(batch, &StoredDue::DeletedDefinition(cleanup.clone()))?;
        Ok(())
    }

    fn schedule_index_retention_due(
        &self,
        incoming: &StoredDue,
    ) -> Result<bool, IndexRetentionDueError> {
        let _guard = self.due_lock()?;
        let mut batch = WriteBatch::default();
        let changed = self.stage_newer_due(&mut batch, incoming)?;
        if changed {
            self.write_due_batch(batch)?;
        }
        Ok(changed)
    }

    fn stage_newer_due(
        &self,
        batch: &mut WriteBatch,
        incoming: &StoredDue,
    ) -> Result<bool, IndexRetentionDueError> {
        let existing = self.due_for_identity(incoming.identity())?;
        if existing
            .as_ref()
            .is_some_and(|current| !incoming.is_newer_than(current))
        {
            return Ok(false);
        }
        let cf = self.due_cf()?;
        if let Some(existing) = existing {
            batch.delete_cf(cf, due_key(&existing));
        }
        batch.put_cf(cf, due_key(incoming), encode_due_value(incoming));
        batch.put_cf(
            cf,
            identity_key(incoming.identity()),
            encode_identity_value(incoming),
        );
        Ok(true)
    }

    fn oldest_due(&self, kind: StoredDueKind) -> Result<Option<StoredDue>, IndexRetentionDueError> {
        let prefix = due_prefix(kind);
        let mut records = self.db.iterator_cf(
            self.due_cf()?,
            IteratorMode::From(&prefix, Direction::Forward),
        );
        let Some(item) = records.next() else {
            return Ok(None);
        };
        let (key, value) = item.map_err(due_storage)?;
        if !key.starts_with(&prefix) {
            return Ok(None);
        }
        decode_due(&key, &value).map(Some)
    }

    fn due_matches(&self, expected: &StoredDue) -> Result<bool, IndexRetentionDueError> {
        let key = due_key(expected);
        let stored = self
            .db
            .get_cf(self.due_cf()?, &key)
            .map_err(due_storage)?
            .map(|value| decode_due(&key, &value))
            .transpose()?;
        if stored.as_ref() != Some(expected) {
            return Ok(false);
        }
        Ok(self.due_for_identity(expected.identity())?.as_ref() == Some(expected))
    }

    fn replace_index_retention_due(
        &self,
        expected: &StoredDue,
        replacement: &StoredDue,
    ) -> Result<bool, IndexRetentionDueError> {
        let _guard = self.due_lock()?;
        if !self.due_matches(expected)? {
            return Ok(false);
        }
        let cf = self.due_cf()?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(cf, due_key(expected));
        batch.put_cf(cf, due_key(replacement), encode_due_value(replacement));
        batch.put_cf(
            cf,
            identity_key(replacement.identity()),
            encode_identity_value(replacement),
        );
        self.write_due_batch(batch)?;
        Ok(true)
    }

    fn complete_index_retention_due(
        &self,
        expected: &StoredDue,
    ) -> Result<bool, IndexRetentionDueError> {
        let _guard = self.due_lock()?;
        if !self.due_matches(expected)? {
            return Ok(false);
        }
        let cf = self.due_cf()?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(cf, due_key(expected));
        batch.delete_cf(cf, identity_key(expected.identity()));
        self.write_due_batch(batch)?;
        Ok(true)
    }

    fn due_for_identity(
        &self,
        identity: DueIdentity,
    ) -> Result<Option<StoredDue>, IndexRetentionDueError> {
        let locator_key = identity_key(identity);
        let Some(locator) = self
            .db
            .get_cf(self.due_cf()?, locator_key)
            .map_err(due_storage)?
        else {
            return Ok(None);
        };
        let (kind, due_at) = decode_identity_value(&locator)?;
        let key = due_key_parts(kind, due_at, identity);
        let value = self
            .db
            .get_cf(self.due_cf()?, &key)
            .map_err(due_storage)?
            .ok_or_else(|| {
                IndexRetentionDueError::Storage(
                    "index-retention identity locator points to an absent due record".into(),
                )
            })?;
        decode_due(&key, &value).map(Some)
    }

    fn due_lock(&self) -> Result<std::sync::MutexGuard<'_, ()>, IndexRetentionDueError> {
        self.definition_state_lock.lock().map_err(|_| {
            IndexRetentionDueError::Storage("definition-state lock is poisoned".into())
        })
    }

    fn due_cf(&self) -> Result<&rocksdb::ColumnFamily, IndexRetentionDueError> {
        self.cf(CF_DEFINITION_STATE).map_err(due_storage)
    }

    fn write_due_batch(&self, batch: WriteBatch) -> Result<(), IndexRetentionDueError> {
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(due_storage)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StoredDueKind {
    Generation,
    DeletedDefinition,
}

impl StoredDueKind {
    fn byte(self) -> u8 {
        match self {
            Self::Generation => GENERATION_KIND,
            Self::DeletedDefinition => DELETED_DEFINITION_KIND,
        }
    }

    fn from_byte(byte: u8) -> Result<Self, IndexRetentionDueError> {
        match byte {
            GENERATION_KIND => Ok(Self::Generation),
            DELETED_DEFINITION_KIND => Ok(Self::DeletedDefinition),
            _ => Err(IndexRetentionDueError::Malformed(
                "due-record kind is unknown".into(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DueIdentity {
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StoredDue {
    Generation(IndexGenerationRetentionDue),
    DeletedDefinition(DeletedDefinitionCleanup),
}

impl StoredDue {
    fn kind(&self) -> StoredDueKind {
        match self {
            Self::Generation(_) => StoredDueKind::Generation,
            Self::DeletedDefinition(_) => StoredDueKind::DeletedDefinition,
        }
    }

    fn identity(&self) -> DueIdentity {
        match self {
            Self::Generation(record) => DueIdentity {
                tenant_id: record.tenant_id,
                bucket_id: record.bucket_id,
                index_id: record.index_id,
            },
            Self::DeletedDefinition(record) => DueIdentity {
                tenant_id: record.tenant_id,
                bucket_id: record.bucket_id,
                index_id: record.index_id,
            },
        }
    }

    fn definition_path(&self) -> &str {
        match self {
            Self::Generation(record) => &record.definition_path,
            Self::DeletedDefinition(record) => &record.definition_path,
        }
    }

    fn definition_version(&self) -> VersionId {
        match self {
            Self::Generation(record) => record.definition_object_version,
            Self::DeletedDefinition(record) => record.definition_object_version,
        }
    }

    fn generation(&self) -> u64 {
        match self {
            Self::Generation(record) => record.generation,
            Self::DeletedDefinition(_) => 0,
        }
    }

    fn due_at(&self) -> u64 {
        match self {
            Self::Generation(record) => record.due_at_unix_millis,
            Self::DeletedDefinition(record) => record.due_at_unix_millis,
        }
    }

    fn is_newer_than(&self, current: &Self) -> bool {
        let incoming = (
            self.definition_version().0,
            u8::from(matches!(self, Self::DeletedDefinition(_))),
            self.generation(),
        );
        let existing = (
            current.definition_version().0,
            u8::from(matches!(current, Self::DeletedDefinition(_))),
            current.generation(),
        );
        incoming > existing
    }

    fn into_generation(self) -> Result<IndexGenerationRetentionDue, IndexRetentionDueError> {
        match self {
            Self::Generation(record) => Ok(record),
            Self::DeletedDefinition(_) => Err(IndexRetentionDueError::Malformed(
                "generation range contains deleted-definition cleanup".into(),
            )),
        }
    }

    fn into_deleted_definition(self) -> Result<DeletedDefinitionCleanup, IndexRetentionDueError> {
        match self {
            Self::DeletedDefinition(record) => Ok(record),
            Self::Generation(_) => Err(IndexRetentionDueError::Malformed(
                "deleted-definition range contains generation retention".into(),
            )),
        }
    }
}

fn require_same_work(left: &StoredDue, right: &StoredDue) -> Result<(), IndexRetentionDueError> {
    if left.kind() != right.kind()
        || left.identity() != right.identity()
        || left.definition_path() != right.definition_path()
        || left.definition_version() != right.definition_version()
        || left.generation() != right.generation()
    {
        return Err(IndexRetentionDueError::Malformed(
            "replacement must identify the same exact retention work".into(),
        ));
    }
    Ok(())
}

fn due_prefix(kind: StoredDueKind) -> [u8; 3] {
    [STORAGE_KEY_FORMAT_VERSION, DUE_DOMAIN, kind.byte()]
}

fn due_key(record: &StoredDue) -> [u8; DUE_KEY_BYTES] {
    due_key_parts(record.kind(), record.due_at(), record.identity())
}

fn due_key_parts(kind: StoredDueKind, due_at: u64, identity: DueIdentity) -> [u8; DUE_KEY_BYTES] {
    let mut key = [0; DUE_KEY_BYTES];
    key[..3].copy_from_slice(&due_prefix(kind));
    key[3..11].copy_from_slice(&due_at.to_be_bytes());
    key[11..19].copy_from_slice(&identity.tenant_id.to_be_bytes());
    key[19..27].copy_from_slice(&identity.bucket_id.to_be_bytes());
    key[27..35].copy_from_slice(&identity.index_id.to_be_bytes());
    key
}

fn identity_key(identity: DueIdentity) -> [u8; IDENTITY_KEY_BYTES] {
    let mut key = [0; IDENTITY_KEY_BYTES];
    key[..2].copy_from_slice(&[STORAGE_KEY_FORMAT_VERSION, IDENTITY_DOMAIN]);
    key[2..10].copy_from_slice(&identity.tenant_id.to_be_bytes());
    key[10..18].copy_from_slice(&identity.bucket_id.to_be_bytes());
    key[18..26].copy_from_slice(&identity.index_id.to_be_bytes());
    key
}

fn encode_identity_value(record: &StoredDue) -> [u8; IDENTITY_VALUE_BYTES] {
    let mut value = [0; IDENTITY_VALUE_BYTES];
    value[0] = VALUE_FORMAT;
    value[1] = record.kind().byte();
    value[2..10].copy_from_slice(&record.due_at().to_be_bytes());
    value
}

fn decode_identity_value(value: &[u8]) -> Result<(StoredDueKind, u64), IndexRetentionDueError> {
    let value: &[u8; IDENTITY_VALUE_BYTES] = value.try_into().map_err(|_| {
        IndexRetentionDueError::Malformed("identity locator has the wrong length".into())
    })?;
    if value[0] != VALUE_FORMAT {
        return Err(IndexRetentionDueError::Malformed(
            "identity locator format is unsupported".into(),
        ));
    }
    Ok((
        StoredDueKind::from_byte(value[1])?,
        read_u64(&value[2..10])?,
    ))
}

fn encode_due_value(record: &StoredDue) -> Vec<u8> {
    let mut value = Vec::with_capacity(DUE_VALUE_FIXED_BYTES + record.definition_path().len());
    value.push(VALUE_FORMAT);
    value.extend_from_slice(&record.definition_version().0.to_be_bytes());
    value.extend_from_slice(&record.generation().to_be_bytes());
    value.extend_from_slice(record.definition_path().as_bytes());
    value
}

fn decode_due(key: &[u8], value: &[u8]) -> Result<StoredDue, IndexRetentionDueError> {
    let key: &[u8; DUE_KEY_BYTES] = key.try_into().map_err(|_| {
        IndexRetentionDueError::Malformed("due-record key has the wrong length".into())
    })?;
    if key[0] != STORAGE_KEY_FORMAT_VERSION || key[1] != DUE_DOMAIN {
        return Err(IndexRetentionDueError::Malformed(
            "due-record key domain is invalid".into(),
        ));
    }
    if value.len() <= DUE_VALUE_FIXED_BYTES || value[0] != VALUE_FORMAT {
        return Err(IndexRetentionDueError::Malformed(
            "due-record value is malformed".into(),
        ));
    }
    let kind = StoredDueKind::from_byte(key[2])?;
    let due_at_unix_millis = read_u64(&key[3..11])?;
    let tenant_id = read_u64(&key[11..19])?;
    let bucket_id = read_u64(&key[19..27])?;
    let index_id = read_u64(&key[27..35])?;
    let definition_object_version = VersionId(read_u64(&value[1..9])?);
    let generation = read_u64(&value[9..17])?;
    let definition_path = std::str::from_utf8(&value[17..])
        .map_err(|_| IndexRetentionDueError::Malformed("definition path is not UTF-8".into()))?
        .to_owned();
    let record = match kind {
        StoredDueKind::Generation => StoredDue::Generation(IndexGenerationRetentionDue {
            tenant_id,
            bucket_id,
            index_id,
            definition_path,
            definition_object_version,
            generation,
            due_at_unix_millis,
        }),
        StoredDueKind::DeletedDefinition => {
            if generation != 0 {
                return Err(IndexRetentionDueError::Malformed(
                    "deleted-definition cleanup has a generation".into(),
                ));
            }
            StoredDue::DeletedDefinition(DeletedDefinitionCleanup {
                tenant_id,
                bucket_id,
                index_id,
                definition_path,
                definition_object_version,
                due_at_unix_millis,
            })
        }
    };
    match &record {
        StoredDue::Generation(record) => record.validate()?,
        StoredDue::DeletedDefinition(record) => record.validate()?,
    }
    Ok(record)
}

fn read_u64(bytes: &[u8]) -> Result<u64, IndexRetentionDueError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| IndexRetentionDueError::Malformed("integer is truncated".into()))
}

fn due_storage(error: impl std::fmt::Display) -> IndexRetentionDueError {
    IndexRetentionDueError::Storage(error.to_string())
}

#[cfg(test)]
#[path = "index_retention_due/tests.rs"]
mod tests;
