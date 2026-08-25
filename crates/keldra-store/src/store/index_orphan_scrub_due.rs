use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};

use super::{CF_DEFINITION_STATE, Store};
use crate::key::STORAGE_KEY_FORMAT_VERSION;
use crate::{
    IndexOrphanScrubDue, IndexOrphanScrubDueError, MAX_INDEX_ORPHAN_CURSOR_BYTES, VersionId,
};

const DUE_DOMAIN: u8 = b'O';
const IDENTITY_DOMAIN: u8 = b'o';
const VALUE_FORMAT: u8 = 1;
const DUE_KEY_BYTES: usize = 2 + 8 + 8 + 8 + 8;
const IDENTITY_KEY_BYTES: usize = 2 + 8 + 8 + 8;
const IDENTITY_VALUE_BYTES: usize = 1 + 8;

#[derive(Clone, Copy, Eq, PartialEq)]
struct Identity {
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
}

impl Store {
    /// Install one independent scrub schedule. Repeated commit publication for
    /// the same definition never moves its due time or persisted scan cursor.
    pub fn schedule_index_orphan_scrub_if_absent(
        &self,
        due: &IndexOrphanScrubDue,
    ) -> Result<bool, IndexOrphanScrubDueError> {
        due.validate()?;
        let _guard = self.orphan_due_lock()?;
        let identity = identity(due);
        if let Some(existing) = self.orphan_due_for_identity(identity)? {
            if existing.definition_path == due.definition_path
                && existing.definition_object_version == due.definition_object_version
            {
                return Ok(false);
            }
            self.write_orphan_due(Some(&existing), due)?;
            return Ok(true);
        }
        self.write_orphan_due(None, due)?;
        Ok(true)
    }

    pub fn oldest_index_orphan_scrub_due(
        &self,
    ) -> Result<Option<IndexOrphanScrubDue>, IndexOrphanScrubDueError> {
        let _guard = self.orphan_due_lock()?;
        let prefix = [STORAGE_KEY_FORMAT_VERSION, DUE_DOMAIN];
        let mut records = self.db.iterator_cf(
            self.orphan_due_cf()?,
            IteratorMode::From(&prefix, Direction::Forward),
        );
        let Some(item) = records.next() else {
            return Ok(None);
        };
        let (key, value) = item.map_err(storage)?;
        if !key.starts_with(&prefix) {
            return Ok(None);
        }
        decode_due(&key, &value).map(Some)
    }

    pub fn index_orphan_scrub_due_matches(
        &self,
        expected: &IndexOrphanScrubDue,
    ) -> Result<bool, IndexOrphanScrubDueError> {
        expected.validate()?;
        let _guard = self.orphan_due_lock()?;
        self.orphan_due_matches_locked(expected)
    }

    pub fn replace_index_orphan_scrub_due(
        &self,
        expected: &IndexOrphanScrubDue,
        replacement: &IndexOrphanScrubDue,
    ) -> Result<bool, IndexOrphanScrubDueError> {
        expected.validate()?;
        replacement.validate()?;
        if identity(expected) != identity(replacement)
            || expected.definition_path != replacement.definition_path
            || expected.definition_object_version != replacement.definition_object_version
        {
            return Err(IndexOrphanScrubDueError::Malformed(
                "orphan scrub replacement must preserve definition identity".into(),
            ));
        }
        let _guard = self.orphan_due_lock()?;
        if !self.orphan_due_matches_locked(expected)? {
            return Ok(false);
        }
        self.write_orphan_due(Some(expected), replacement)?;
        Ok(true)
    }

    pub fn cancel_index_orphan_scrub(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<bool, IndexOrphanScrubDueError> {
        let identity = Identity {
            tenant_id,
            bucket_id,
            index_id,
        };
        if tenant_id == 0 || bucket_id == 0 || index_id == 0 {
            return Err(IndexOrphanScrubDueError::Malformed(
                "stable IDs must be non-zero".into(),
            ));
        }
        let _guard = self.orphan_due_lock()?;
        let Some(existing) = self.orphan_due_for_identity(identity)? else {
            return Ok(false);
        };
        let cf = self.orphan_due_cf()?;
        let mut batch = WriteBatch::default();
        batch.delete_cf(cf, due_key(&existing));
        batch.delete_cf(cf, identity_key(identity));
        self.write_orphan_batch(batch)?;
        Ok(true)
    }

    fn orphan_due_matches_locked(
        &self,
        expected: &IndexOrphanScrubDue,
    ) -> Result<bool, IndexOrphanScrubDueError> {
        Ok(self.orphan_due_for_identity(identity(expected))?.as_ref() == Some(expected))
    }

    fn orphan_due_for_identity(
        &self,
        identity: Identity,
    ) -> Result<Option<IndexOrphanScrubDue>, IndexOrphanScrubDueError> {
        let Some(locator) = self
            .db
            .get_cf(self.orphan_due_cf()?, identity_key(identity))
            .map_err(storage)?
        else {
            return Ok(None);
        };
        let due_at = decode_identity_value(&locator)?;
        let key = due_key_parts(due_at, identity);
        let value = self
            .db
            .get_cf(self.orphan_due_cf()?, key)
            .map_err(storage)?
            .ok_or_else(|| {
                IndexOrphanScrubDueError::Storage(
                    "orphan scrub identity points to an absent due record".into(),
                )
            })?;
        decode_due(&key, &value).map(Some)
    }

    fn write_orphan_due(
        &self,
        existing: Option<&IndexOrphanScrubDue>,
        replacement: &IndexOrphanScrubDue,
    ) -> Result<(), IndexOrphanScrubDueError> {
        let cf = self.orphan_due_cf()?;
        let mut batch = WriteBatch::default();
        if let Some(existing) = existing {
            batch.delete_cf(cf, due_key(existing));
        }
        batch.put_cf(cf, due_key(replacement), encode_due_value(replacement)?);
        batch.put_cf(
            cf,
            identity_key(identity(replacement)),
            encode_identity_value(replacement),
        );
        self.write_orphan_batch(batch)
    }

    fn orphan_due_lock(&self) -> Result<std::sync::MutexGuard<'_, ()>, IndexOrphanScrubDueError> {
        self.definition_state_lock.lock().map_err(|_| {
            IndexOrphanScrubDueError::Storage("definition-state lock is poisoned".into())
        })
    }

    fn orphan_due_cf(&self) -> Result<&rocksdb::ColumnFamily, IndexOrphanScrubDueError> {
        self.cf(CF_DEFINITION_STATE).map_err(storage)
    }

    fn write_orphan_batch(&self, batch: WriteBatch) -> Result<(), IndexOrphanScrubDueError> {
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage)
    }
}

fn identity(due: &IndexOrphanScrubDue) -> Identity {
    Identity {
        tenant_id: due.tenant_id,
        bucket_id: due.bucket_id,
        index_id: due.index_id,
    }
}

fn due_key(due: &IndexOrphanScrubDue) -> [u8; DUE_KEY_BYTES] {
    due_key_parts(due.due_at_unix_millis, identity(due))
}

fn due_key_parts(due_at: u64, identity: Identity) -> [u8; DUE_KEY_BYTES] {
    let mut key = [0; DUE_KEY_BYTES];
    key[..2].copy_from_slice(&[STORAGE_KEY_FORMAT_VERSION, DUE_DOMAIN]);
    key[2..10].copy_from_slice(&due_at.to_be_bytes());
    key[10..18].copy_from_slice(&identity.tenant_id.to_be_bytes());
    key[18..26].copy_from_slice(&identity.bucket_id.to_be_bytes());
    key[26..34].copy_from_slice(&identity.index_id.to_be_bytes());
    key
}

fn identity_key(identity: Identity) -> [u8; IDENTITY_KEY_BYTES] {
    let mut key = [0; IDENTITY_KEY_BYTES];
    key[..2].copy_from_slice(&[STORAGE_KEY_FORMAT_VERSION, IDENTITY_DOMAIN]);
    key[2..10].copy_from_slice(&identity.tenant_id.to_be_bytes());
    key[10..18].copy_from_slice(&identity.bucket_id.to_be_bytes());
    key[18..26].copy_from_slice(&identity.index_id.to_be_bytes());
    key
}

fn encode_identity_value(due: &IndexOrphanScrubDue) -> [u8; IDENTITY_VALUE_BYTES] {
    let mut value = [0; IDENTITY_VALUE_BYTES];
    value[0] = VALUE_FORMAT;
    value[1..9].copy_from_slice(&due.due_at_unix_millis.to_be_bytes());
    value
}

fn decode_identity_value(value: &[u8]) -> Result<u64, IndexOrphanScrubDueError> {
    if value.len() != IDENTITY_VALUE_BYTES || value[0] != VALUE_FORMAT {
        return Err(IndexOrphanScrubDueError::Malformed(
            "orphan scrub identity value is unsupported".into(),
        ));
    }
    read_u64(&value[1..9])
}

fn encode_due_value(due: &IndexOrphanScrubDue) -> Result<Vec<u8>, IndexOrphanScrubDueError> {
    let path = due.definition_path.as_bytes();
    let path_len = u32::try_from(path.len())
        .map_err(|_| IndexOrphanScrubDueError::Malformed("definition path is too long".into()))?;
    let cursor = due.scan_cursor.as_deref().map(str::as_bytes);
    let cursor_len = cursor
        .map(|cursor| u32::try_from(cursor.len()))
        .transpose()
        .map_err(|_| IndexOrphanScrubDueError::Malformed("scan cursor is too long".into()))?
        .unwrap_or(u32::MAX);
    let mut value = Vec::with_capacity(41 + path.len() + cursor.map_or(0, |value| value.len()));
    value.push(VALUE_FORMAT);
    value.extend_from_slice(&due.definition_object_version.0.to_be_bytes());
    value.extend_from_slice(&due.scan_placement_term.to_be_bytes());
    value.extend_from_slice(&due.scan_placement_index.to_be_bytes());
    value.extend_from_slice(&due.scan_node_id.to_be_bytes());
    value.extend_from_slice(&path_len.to_be_bytes());
    value.extend_from_slice(&cursor_len.to_be_bytes());
    value.extend_from_slice(path);
    if let Some(cursor) = cursor {
        value.extend_from_slice(cursor);
    }
    Ok(value)
}

fn decode_due(key: &[u8], value: &[u8]) -> Result<IndexOrphanScrubDue, IndexOrphanScrubDueError> {
    if key.len() != DUE_KEY_BYTES
        || key[0] != STORAGE_KEY_FORMAT_VERSION
        || key[1] != DUE_DOMAIN
        || value.len() < 41
        || value[0] != VALUE_FORMAT
    {
        return Err(IndexOrphanScrubDueError::Malformed(
            "orphan scrub due record is unsupported".into(),
        ));
    }
    let path_len = read_u32(&value[33..37])? as usize;
    let cursor_len = read_u32(&value[37..41])?;
    let cursor_bytes = if cursor_len == u32::MAX {
        0
    } else {
        cursor_len as usize
    };
    if cursor_bytes > MAX_INDEX_ORPHAN_CURSOR_BYTES || value.len() != 41 + path_len + cursor_bytes {
        return Err(IndexOrphanScrubDueError::Malformed(
            "orphan scrub due variable fields are malformed".into(),
        ));
    }
    let definition_path = std::str::from_utf8(&value[41..41 + path_len])
        .map_err(|_| IndexOrphanScrubDueError::Malformed("definition path is not UTF-8".into()))?
        .to_owned();
    let scan_cursor = (cursor_len != u32::MAX)
        .then(|| {
            std::str::from_utf8(&value[41 + path_len..])
                .map(str::to_owned)
                .map_err(|_| IndexOrphanScrubDueError::Malformed("scan cursor is not UTF-8".into()))
        })
        .transpose()?;
    let due = IndexOrphanScrubDue {
        tenant_id: read_u64(&key[10..18])?,
        bucket_id: read_u64(&key[18..26])?,
        index_id: read_u64(&key[26..34])?,
        definition_path,
        definition_object_version: VersionId(read_u64(&value[1..9])?),
        due_at_unix_millis: read_u64(&key[2..10])?,
        scan_placement_term: read_u64(&value[9..17])?,
        scan_placement_index: read_u64(&value[17..25])?,
        scan_node_id: read_u64(&value[25..33])?,
        scan_cursor,
    };
    due.validate()?;
    Ok(due)
}

fn read_u32(bytes: &[u8]) -> Result<u32, IndexOrphanScrubDueError> {
    bytes
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| IndexOrphanScrubDueError::Malformed("integer is truncated".into()))
}

fn read_u64(bytes: &[u8]) -> Result<u64, IndexOrphanScrubDueError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| IndexOrphanScrubDueError::Malformed("integer is truncated".into()))
}

fn storage(error: impl std::fmt::Display) -> IndexOrphanScrubDueError {
    IndexOrphanScrubDueError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoreOptions;

    fn due(definition_version: u64, due_at: u64) -> IndexOrphanScrubDue {
        IndexOrphanScrubDue {
            tenant_id: 1,
            bucket_id: 2,
            index_id: 3,
            definition_path: "_keldra/indexes/search".into(),
            definition_object_version: VersionId(definition_version),
            due_at_unix_millis: due_at,
            scan_placement_term: 0,
            scan_placement_index: 0,
            scan_node_id: 0,
            scan_cursor: None,
        }
    }

    #[tokio::test]
    async fn repeated_publication_does_not_reset_scrub_due_or_cursor() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let first = due(4, 100);
        assert!(store.schedule_index_orphan_scrub_if_absent(&first).unwrap());
        let mut checkpoint = first.clone();
        checkpoint.due_at_unix_millis = 101;
        checkpoint.scan_placement_term = 5;
        checkpoint.scan_placement_index = 6;
        checkpoint.scan_node_id = 7;
        checkpoint.scan_cursor = Some("cursor".into());
        assert!(
            store
                .replace_index_orphan_scrub_due(&first, &checkpoint)
                .unwrap()
        );

        assert!(
            !store
                .schedule_index_orphan_scrub_if_absent(&due(4, 999))
                .unwrap()
        );
        assert_eq!(
            store.oldest_index_orphan_scrub_due().unwrap(),
            Some(checkpoint)
        );
    }

    #[tokio::test]
    async fn scrub_checkpoint_survives_restart() {
        let temporary = tempfile::tempdir().unwrap();
        let options = StoreOptions::new(temporary.path(), 1);
        let expected = due(4, 100);
        {
            let store = Store::open(options.clone()).await.unwrap();
            store
                .schedule_index_orphan_scrub_if_absent(&expected)
                .unwrap();
        }
        let reopened = Store::open(options).await.unwrap();
        assert_eq!(
            reopened.oldest_index_orphan_scrub_due().unwrap(),
            Some(expected)
        );
    }
}
