use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::blob_gc::{
    BlobGcBudget, BlobGcCursor, BlobGcPhase, BlobGcTick, FilesystemGcChild, FilesystemGcChildKind,
    FilesystemGcCursor,
};

use super::journal_capacity::SourceJournalAdmission;
use super::*;

static NEXT_GC_QUARANTINE_ID: AtomicU64 = AtomicU64::new(1);

impl Store {
    pub async fn stage_blob(&self, bytes: &[u8]) -> Result<BlobRef, MutationError> {
        self.stage_blob_with_admission(bytes, SourceJournalAdmission::Bounded)
            .await
    }

    pub(super) async fn stage_blob_with_admission(
        &self,
        bytes: &[u8],
        admission: SourceJournalAdmission,
    ) -> Result<BlobRef, MutationError> {
        if bytes.len() <= SMALL_BLOB_MAX_BYTES {
            let reference = blob_reference_for_bytes(bytes);
            self.persist_small_blob_seal_with_admission(
                &reference,
                bytes,
                now_unix_millis()?,
                admission,
            )
            .await?;
            return Ok(reference);
        }
        let mut upload = self.begin_blob_upload().await?;
        upload.write(bytes).await.map_err(storage_error)?;
        self.seal_blob_upload_with_admission(upload, admission)
            .await
    }

    pub fn lock_manager(&self) -> LocalLockManager {
        self.program_locks.clone()
    }

    pub async fn begin_blob_upload(&self) -> Result<crate::BlobUpload, MutationError> {
        self.blobs.begin_upload().await.map_err(storage_error)
    }

    /// Seals one physical upload and records its single awaiting-publication
    /// reservation before returning it to the caller.
    pub async fn seal_blob_upload(
        &self,
        upload: crate::BlobUpload,
    ) -> Result<BlobRef, MutationError> {
        self.seal_blob_upload_with_admission(upload, SourceJournalAdmission::Bounded)
            .await
    }

    pub(super) async fn seal_blob_upload_with_admission(
        &self,
        upload: crate::BlobUpload,
        admission: SourceJournalAdmission,
    ) -> Result<BlobRef, MutationError> {
        // Hashing, fsync, rename and parent-directory fsync are byte-plane IO,
        // so complete them before taking the short metadata commit fence.
        let reference = upload.finish().await.map_err(storage_error)?;
        let now = now_unix_millis()?;
        if is_small_blob(&reference) {
            let bytes = self.blobs.get(&reference).await.map_err(storage_error)?;
            self.persist_small_blob_seal_with_admission(&reference, &bytes, now, admission)
                .await?;
            // A crash before this cleanup leaves only a normal untracked copy,
            // which the existing age-gated orphan scan removes.
            self.blobs.remove(&reference).map_err(storage_error)?;
        } else {
            self.reserve_sealed_blob_with_admission_wait(&reference, now, admission)
                .await?;
        }
        Ok(reference)
    }

    pub(crate) async fn read_blob_bytes(
        &self,
        reference: &BlobRef,
    ) -> Result<Vec<u8>, MutationError> {
        if is_small_blob(reference) {
            let bytes = self
                .db
                .get_cf(self.cf(CF_SMALL_BLOBS)?, blob_reference_key(reference))
                .map_err(storage_error)?
                .ok_or(MutationError::BlobNotFound)?
                .to_vec();
            validate_small_blob(reference, &bytes)?;
            Ok(bytes)
        } else {
            self.blobs.get(reference).await.map_err(storage_error)
        }
    }

    pub(super) async fn contains_blob(&self, reference: &BlobRef) -> Result<bool, MutationError> {
        if is_small_blob(reference) {
            let Some(bytes) = self
                .db
                .get_cf(self.cf(CF_SMALL_BLOBS)?, blob_reference_key(reference))
                .map_err(storage_error)?
            else {
                return Ok(false);
            };
            validate_small_blob(reference, &bytes)?;
            Ok(true)
        } else {
            self.blobs.contains(reference).await.map_err(storage_error)
        }
    }

    /// Returns the authoritative lifecycle state for one sealed blob.
    pub fn blob_reference_state(
        &self,
        reference: &BlobRef,
    ) -> Result<Option<BlobReferenceState>, MutationError> {
        self.read_blob_reference_state(&blob_reference_key(reference))
    }

    /// Performs one bounded local garbage-collection step after the caller has
    /// proved reference delivery is current. Discovery never holds the commit
    /// fence. A candidate is exact-read again under that short fence before
    /// its canonical lifecycle record is removed.
    pub async fn collect_blob_garbage_tick(
        &self,
        cursor: &mut BlobGcCursor,
        budget: BlobGcBudget,
    ) -> Result<BlobGcTick, MutationError> {
        self.collect_blob_garbage_tick_at(cursor, budget, now_unix_millis()?)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn collect_blob_garbage_at(
        &self,
        now_unix_millis: u64,
    ) -> Result<u64, MutationError> {
        let mut cursor = BlobGcCursor::default();
        let budget = BlobGcBudget::new(u32::MAX, u64::MAX, std::time::Duration::from_secs(60))
            .expect("test blob GC budget is valid");
        let mut removed = 0_u64;
        loop {
            let tick = self
                .collect_blob_garbage_tick_at(&mut cursor, budget, now_unix_millis)
                .await?;
            removed = removed.saturating_add(tick.removed);
            if tick.cycle_complete {
                return Ok(removed);
            }
        }
    }

    pub(super) async fn collect_blob_garbage_tick_at(
        &self,
        cursor: &mut BlobGcCursor,
        budget: BlobGcBudget,
        now_unix_millis: u64,
    ) -> Result<BlobGcTick, MutationError> {
        validate_blob_gc_budget(budget)?;
        let started = Instant::now();
        let mut tick = BlobGcTick::default();
        while tick.inspected_records < budget.max_records
            && tick.inspected_bytes < budget.max_bytes
            && started.elapsed() < budget.max_duration
        {
            match &mut cursor.phase {
                BlobGcPhase::References | BlobGcPhase::ReferencesAfter(_) => {
                    let after = match &cursor.phase {
                        BlobGcPhase::References => None,
                        BlobGcPhase::ReferencesAfter(key) => Some(key.as_slice()),
                        BlobGcPhase::Filesystem(_) => unreachable!(),
                    };
                    let Some((key, state, encoded_bytes)) = self.next_blob_gc_reference(after)?
                    else {
                        cursor.phase = BlobGcPhase::Filesystem(FilesystemGcCursor::default());
                        continue;
                    };
                    if tick.inspected_records != 0
                        && tick.inspected_bytes.saturating_add(encoded_bytes) > budget.max_bytes
                    {
                        break;
                    }
                    let removed = if blob_reference_is_garbage(
                        state,
                        now_unix_millis,
                        self.awaiting_publish_ttl_millis,
                    ) {
                        self.remove_blob_gc_reference_if_unchanged(&key, state, now_unix_millis)
                            .await?
                    } else {
                        false
                    };
                    cursor.phase = BlobGcPhase::ReferencesAfter(key);
                    tick.inspected_records += 1;
                    tick.inspected_bytes = tick.inspected_bytes.saturating_add(encoded_bytes);
                    tick.removed = tick.removed.saturating_add(u64::from(removed));
                }
                BlobGcPhase::Filesystem(filesystem) => {
                    let Some(record) = self.next_filesystem_gc_record(filesystem)? else {
                        cursor.phase = BlobGcPhase::References;
                        tick.cycle_complete = true;
                        break;
                    };
                    let encoded_bytes = filesystem_record_bytes(&record);
                    if tick.inspected_records != 0
                        && tick.inspected_bytes.saturating_add(encoded_bytes) > budget.max_bytes
                    {
                        filesystem.replay = Some(record);
                        break;
                    }
                    let removed = self
                        .remove_filesystem_gc_record(record, now_unix_millis)
                        .await?;
                    tick.inspected_records += 1;
                    tick.inspected_bytes = tick.inspected_bytes.saturating_add(encoded_bytes);
                    tick.removed = tick.removed.saturating_add(u64::from(removed));
                }
            }
        }
        Ok(tick)
    }

    fn next_blob_gc_reference(
        &self,
        after: Option<&[u8]>,
    ) -> Result<Option<(Vec<u8>, BlobReferenceState, u64)>, MutationError> {
        let references = self.cf(CF_BLOB_REFERENCES)?;
        let mode = after.map_or(IteratorMode::Start, |after| {
            IteratorMode::From(after, Direction::Forward)
        });
        for entry in self.db.iterator_cf(references, mode) {
            let (key, encoded) = entry.map_err(storage_error)?;
            if after.is_some_and(|after| key.as_ref() <= after) {
                continue;
            }
            let state = decode_blob_reference_state(&encoded)?;
            let encoded_bytes = (key.len() + encoded.len()) as u64;
            return Ok(Some((key.to_vec(), state, encoded_bytes)));
        }
        Ok(None)
    }

    pub(super) async fn remove_blob_gc_reference_if_unchanged(
        &self,
        key: &[u8],
        expected: BlobReferenceState,
        now_unix_millis: u64,
    ) -> Result<bool, MutationError> {
        enum PhysicalRemoval {
            None,
            Blob(BlobRef),
            Shard(ShardIdentity),
        }

        let quarantined = {
            let _commit_guard = self.commit_lock.lock().await;
            let Some(current) = self.read_blob_reference_state(key)? else {
                return Ok(false);
            };
            if current != expected
                || !blob_reference_is_garbage(
                    current,
                    now_unix_millis,
                    self.awaiting_publish_ttl_millis,
                )
            {
                return Ok(false);
            }
            let mut batch = WriteBatch::default();
            let physical = if key.len() == 32 + size_of::<u64>() {
                let reference = blob_reference_from_key(key)?;
                if is_small_blob(&reference) {
                    batch.delete_cf(self.cf(CF_SMALL_BLOBS)?, key);
                    PhysicalRemoval::None
                } else {
                    PhysicalRemoval::Blob(reference)
                }
            } else {
                PhysicalRemoval::Shard(ShardIdentity::decode(key).map_err(storage_error)?)
            };
            batch.delete_cf(self.cf(CF_BLOB_REFERENCES)?, key);
            self.stage_local_changes(
                &mut batch,
                &[PendingLocalChange::ContentLifecycleChanged {
                    blob_identity: key.to_vec(),
                    revision: now_unix_millis,
                    reference_deltas: Vec::new(),
                }],
                LocalReferenceEffects::NoReferenceEffects,
            )?;
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
            self.notify_local_invalidations();
            match physical {
                PhysicalRemoval::None => None,
                PhysicalRemoval::Blob(reference) => {
                    self.quarantine_gc_file(&self.blobs.path(&reference.hash))?
                }
                PhysicalRemoval::Shard(identity) => {
                    self.quarantine_gc_file(&shard_file_path(self.blobs.root(), &identity))?
                }
            }
        };
        if let Some(path) = quarantined {
            remove_file_and_sync_parent(&path)?;
        }
        Ok(true)
    }

    fn next_filesystem_gc_record(
        &self,
        cursor: &mut FilesystemGcCursor,
    ) -> Result<Option<crate::blob_gc::FilesystemGcRecord>, MutationError> {
        use crate::blob_gc::FilesystemGcRecord;

        if let Some(record) = cursor.replay.take() {
            return Ok(Some(record));
        }
        loop {
            if let Some(child) = cursor.child.as_mut() {
                let Some(entry) = child.entries.next() else {
                    cursor.child = None;
                    continue;
                };
                let entry = entry.map_err(storage_error)?;
                if !entry.file_type().map_err(storage_error)?.is_file() {
                    return Err(MutationError::Storage(
                        "blob maintenance directory contains a non-file entry".into(),
                    ));
                }
                let path = entry.path();
                let encoded_bytes = path_encoded_bytes(&path);
                match &child.kind {
                    FilesystemGcChildKind::Staging => {
                        return Ok(Some(FilesystemGcRecord::Staged {
                            modified_at: modified_unix_millis(&entry)?,
                            path,
                            encoded_bytes,
                        }));
                    }
                    FilesystemGcChildKind::Quarantine => {
                        return Ok(Some(FilesystemGcRecord::Quarantined {
                            path,
                            encoded_bytes,
                        }));
                    }
                    FilesystemGcChildKind::HashPrefix(prefix) => {
                        let name = entry.file_name();
                        let name = name.to_str().ok_or_else(|| {
                            MutationError::Storage("blob file name is not valid UTF-8".into())
                        })?;
                        let modified_at = modified_unix_millis(&entry)?;
                        if name.len() == 64 {
                            return Ok(Some(FilesystemGcRecord::Blob {
                                reference: blob_reference_from_file(&entry, prefix)?,
                                path,
                                modified_at,
                                encoded_bytes,
                            }));
                        }
                        return Ok(Some(FilesystemGcRecord::Shard {
                            identity: ShardIdentity::decode_file_name(prefix, name)
                                .map_err(storage_error)?,
                            path,
                            modified_at,
                            encoded_bytes,
                        }));
                    }
                }
            }

            if cursor.root.is_none() {
                cursor.root = Some(std::fs::read_dir(self.blobs.root()).map_err(storage_error)?);
            }
            let root = cursor
                .root
                .as_mut()
                .expect("filesystem GC root was initialized");
            let Some(entry) = root.next() else {
                return Ok(None);
            };
            let entry = entry.map_err(storage_error)?;
            if !entry.file_type().map_err(storage_error)?.is_dir() {
                return Err(MutationError::Storage(
                    "blob root contains an unexpected non-directory entry".into(),
                ));
            }
            let name = entry.file_name();
            let kind = if name.as_os_str() == std::ffi::OsStr::new(".staging") {
                FilesystemGcChildKind::Staging
            } else if name.as_os_str() == std::ffi::OsStr::new(".gc") {
                FilesystemGcChildKind::Quarantine
            } else {
                let prefix = name.to_str().ok_or_else(|| {
                    MutationError::Storage("blob shard directory name is not UTF-8".into())
                })?;
                if prefix.len() != 2 || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(MutationError::Storage(
                        "blob shard directory name is malformed".into(),
                    ));
                }
                FilesystemGcChildKind::HashPrefix(prefix.to_owned())
            };
            let path = entry.path();
            cursor.child = Some(FilesystemGcChild {
                entries: std::fs::read_dir(&path).map_err(storage_error)?,
                kind: kind.clone(),
            });
            return Ok(Some(FilesystemGcRecord::Directory {
                encoded_bytes: path_encoded_bytes(&path),
                path,
                kind,
            }));
        }
    }

    async fn remove_filesystem_gc_record(
        &self,
        record: crate::blob_gc::FilesystemGcRecord,
        now_unix_millis: u64,
    ) -> Result<bool, MutationError> {
        use crate::blob_gc::FilesystemGcRecord;

        let candidate = match record {
            FilesystemGcRecord::Directory { .. } => return Ok(false),
            FilesystemGcRecord::Quarantined { path, .. } => {
                remove_file_and_sync_parent(&path)?;
                return Ok(true);
            }
            FilesystemGcRecord::Staged {
                path, modified_at, ..
            } => {
                if now_unix_millis.saturating_sub(modified_at) < self.awaiting_publish_ttl_millis {
                    return Ok(false);
                }
                let Some(path) = self.quarantine_gc_file(&path)? else {
                    return Ok(false);
                };
                path
            }
            FilesystemGcRecord::Blob {
                path,
                reference,
                modified_at,
                ..
            } => {
                if now_unix_millis.saturating_sub(modified_at) < self.awaiting_publish_ttl_millis {
                    return Ok(false);
                }
                let _commit_guard = self.commit_lock.lock().await;
                if !is_small_blob(&reference)
                    && self
                        .read_blob_reference_state(&blob_reference_key(&reference))?
                        .is_some()
                {
                    return Ok(false);
                }
                let Some(path) = self.quarantine_gc_file(&path)? else {
                    return Ok(false);
                };
                path
            }
            FilesystemGcRecord::Shard {
                path,
                identity,
                modified_at,
                ..
            } => {
                if now_unix_millis.saturating_sub(modified_at) < self.awaiting_publish_ttl_millis {
                    return Ok(false);
                }
                let _commit_guard = self.commit_lock.lock().await;
                if self
                    .read_blob_reference_state(&identity.encode())?
                    .is_some()
                {
                    return Ok(false);
                }
                let Some(path) = self.quarantine_gc_file(&path)? else {
                    return Ok(false);
                };
                path
            }
        };
        remove_file_and_sync_parent(&candidate)?;
        Ok(true)
    }

    fn quarantine_gc_file(&self, source: &Path) -> Result<Option<PathBuf>, MutationError> {
        if !source.exists() {
            return Ok(None);
        }
        let quarantine = self.blobs.root().join(".gc");
        std::fs::create_dir_all(&quarantine).map_err(storage_error)?;
        let destination = quarantine.join(format!(
            "gc-{}-{}",
            std::process::id(),
            NEXT_GC_QUARANTINE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::rename(source, &destination) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(storage_error(error)),
        }
        sync_directory(source.parent().ok_or_else(|| {
            MutationError::Storage("garbage-collected blob has no parent".into())
        })?)?;
        sync_directory(&quarantine)?;
        Ok(Some(destination))
    }

    fn prepare_sealed_blob_reservation(
        &self,
        reference: &BlobRef,
        now_unix_millis: u64,
    ) -> Result<Option<BlobReferenceState>, MutationError> {
        let key = blob_reference_key(reference);
        self.prepare_sealed_artifact_reservation(&key, now_unix_millis)
    }

    pub(super) fn prepare_sealed_artifact_reservation(
        &self,
        key: &[u8],
        now_unix_millis: u64,
    ) -> Result<Option<BlobReferenceState>, MutationError> {
        let current = self.read_blob_reference_state(key)?;
        if let Some(current) = current {
            validate_blob_reference_state(current)?;
        }
        let next = match current {
            None => BlobReferenceState {
                ref_count: 1,
                flags: AWAITING_PUBLISH,
                created_at: now_unix_millis,
                updated_at: now_unix_millis,
            },
            Some(mut current) if current.ref_count == 0 => {
                current.ref_count = 1;
                current.flags = AWAITING_PUBLISH;
                current.updated_at = current.updated_at.max(now_unix_millis);
                current
            }
            Some(mut current) => {
                if current.flags & AWAITING_PUBLISH == 0 {
                    current.updated_at = current.updated_at.max(now_unix_millis);
                    return Ok(Some(current));
                }
                if current.ref_count != 1 {
                    return Err(MutationError::Storage(
                        "awaiting-publish blob must have exactly one reservation".into(),
                    ));
                }
                current.updated_at = current.updated_at.max(now_unix_millis);
                current
            }
        };
        Ok(Some(next))
    }

    pub(super) fn reserve_sealed_artifact(
        &self,
        key: &[u8],
        now_unix_millis: u64,
    ) -> Result<BlobReferenceState, MutationError> {
        self.reserve_sealed_artifact_with_admission(
            key,
            now_unix_millis,
            SourceJournalAdmission::Bounded,
        )
    }

    fn reserve_sealed_artifact_with_admission(
        &self,
        key: &[u8],
        now_unix_millis: u64,
        admission: SourceJournalAdmission,
    ) -> Result<BlobReferenceState, MutationError> {
        let next = self
            .prepare_sealed_artifact_reservation(key, now_unix_millis)?
            .ok_or_else(|| MutationError::Storage("sealed lifecycle state is missing".into()))?;
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.cf(CF_BLOB_REFERENCES)?,
            key,
            encode_blob_reference_state(next),
        );
        self.stage_local_changes_with_admission(
            &mut batch,
            &[PendingLocalChange::ContentLifecycleChanged {
                blob_identity: key.to_vec(),
                revision: next.updated_at,
                reference_deltas: Vec::new(),
            }],
            LocalReferenceEffects::NoReferenceEffects,
            admission,
        )?;
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        self.notify_local_invalidations();
        Ok(next)
    }

    pub(super) fn reserve_sealed_blob(
        &self,
        reference: &BlobRef,
        now_unix_millis: u64,
    ) -> Result<(), MutationError> {
        self.reserve_sealed_blob_with_admission(
            reference,
            now_unix_millis,
            SourceJournalAdmission::Bounded,
        )
    }

    fn reserve_sealed_blob_with_admission(
        &self,
        reference: &BlobRef,
        now_unix_millis: u64,
        admission: SourceJournalAdmission,
    ) -> Result<(), MutationError> {
        let key = blob_reference_key(reference);
        self.reserve_sealed_artifact_with_admission(&key, now_unix_millis, admission)
            .map(|_| ())
    }

    async fn reserve_sealed_blob_with_admission_wait(
        &self,
        reference: &BlobRef,
        now_unix_millis: u64,
        admission: SourceJournalAdmission,
    ) -> Result<(), MutationError> {
        loop {
            let guard = self.commit_lock.lock().await;
            // GC may have removed a stale deduplication target while finish was
            // outside the fence. Never recreate lifecycle state without bytes.
            if !self
                .blobs
                .contains(reference)
                .await
                .map_err(storage_error)?
            {
                return Err(MutationError::BlobNotFound);
            }
            let result =
                self.reserve_sealed_blob_with_admission(reference, now_unix_millis, admission);
            drop(guard);
            match result {
                Err(MutationError::SourceJournalCapacity)
                    if admission == SourceJournalAdmission::Bounded =>
                {
                    self.wait_for_mutation_capacity().await;
                }
                result => return result,
            }
        }
    }

    async fn persist_small_blob_seal_with_admission(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        now_unix_millis: u64,
        admission: SourceJournalAdmission,
    ) -> Result<(), MutationError> {
        loop {
            let guard = self.commit_lock.lock().await;
            let result = self.persist_small_blob_seal(reference, bytes, now_unix_millis, admission);
            drop(guard);
            match result {
                Err(MutationError::SourceJournalCapacity)
                    if admission == SourceJournalAdmission::Bounded =>
                {
                    self.wait_for_mutation_capacity().await;
                }
                result => return result,
            }
        }
    }

    fn persist_small_blob_seal(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        now_unix_millis: u64,
        admission: SourceJournalAdmission,
    ) -> Result<(), MutationError> {
        validate_small_blob(reference, bytes)?;
        let pending = BTreeSet::new();
        let value = self.prepare_small_blob_value(reference, bytes, &pending)?;
        let state = self
            .prepare_sealed_blob_reservation(reference, now_unix_millis)?
            .ok_or_else(|| MutationError::Storage("small blob reservation is missing".into()))?;
        let key = blob_reference_key(reference);
        let mut batch = WriteBatch::default();
        if let Some((small_key, small_bytes)) = value {
            batch.put_cf(self.cf(CF_SMALL_BLOBS)?, &small_key, small_bytes);
        }
        batch.put_cf(
            self.cf(CF_BLOB_REFERENCES)?,
            &key,
            encode_blob_reference_state(state),
        );
        self.stage_local_changes_with_admission(
            &mut batch,
            &[PendingLocalChange::ContentLifecycleChanged {
                blob_identity: key,
                revision: state.updated_at,
                reference_deltas: Vec::new(),
            }],
            LocalReferenceEffects::NoReferenceEffects,
            admission,
        )?;
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        self.notify_local_invalidations();
        Ok(())
    }

    pub(crate) fn prepare_blob_reference_publication(
        &self,
        reference: &BlobRef,
        pending: &PendingBlobReferences,
        now_unix_millis: u64,
    ) -> Result<(Vec<u8>, BlobReferenceState), MutationError> {
        let key = blob_reference_key(reference);
        let state = match pending.get(&key).copied() {
            Some(state) => state,
            None => self
                .read_blob_reference_state(&key)?
                .ok_or(MutationError::BlobNotFound)?,
        };
        advance_blob_reference_publication(state, now_unix_millis).map(|state| (key, state))
    }

    /// Publishes bytes materialised by an inline put. Small bytes join the
    /// final RocksDB batch; large bytes are already durable in the byte plane.
    /// Neither needs a separate awaiting-publication lifecycle write.
    pub(super) fn prepare_materialized_blob_publication(
        &self,
        reference: &BlobRef,
        pending: &PendingBlobReferences,
        now_unix_millis: u64,
    ) -> Result<(Vec<u8>, BlobReferenceState), MutationError> {
        let key = blob_reference_key(reference);
        let state = match pending.get(&key).copied() {
            Some(state) => advance_blob_reference_publication(state, now_unix_millis)?,
            None => match self.read_blob_reference_state(&key)? {
                Some(state) => advance_blob_reference_publication(state, now_unix_millis)?,
                None => BlobReferenceState {
                    ref_count: 1,
                    flags: 0,
                    created_at: now_unix_millis,
                    updated_at: now_unix_millis,
                },
            },
        };
        Ok((key, state))
    }

    pub(super) fn prepare_small_blob_value(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        pending: &BTreeSet<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MutationError> {
        validate_small_blob(reference, bytes)?;
        self.prepare_hashed_small_blob_value(reference, bytes, pending)
    }

    /// Prepares bytes whose reference was computed from this exact immutable
    /// slice during the current put preparation. Existing stored bytes remain
    /// independently verified before content-address reuse.
    pub(super) fn prepare_hashed_small_blob_value(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        pending: &BTreeSet<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MutationError> {
        let key = blob_reference_key(reference);
        if pending.contains(&key) {
            return Ok(None);
        }
        let existing = self
            .db
            .get_cf(self.cf(CF_SMALL_BLOBS)?, &key)
            .map_err(storage_error)?;
        match existing {
            Some(existing) => {
                validate_small_blob(reference, &existing)?;
                if existing.as_slice() != bytes {
                    return Err(MutationError::Storage(
                        "small blob content-address collision".into(),
                    ));
                }
                Ok(None)
            }
            None => Ok(Some((key, bytes.to_vec()))),
        }
    }

    /// Stages the lifecycle half of a future immutable-version retirement.
    /// The caller must put this update in the same RocksDB batch that removes
    /// the corresponding version descriptor.
    #[allow(dead_code)]
    pub(crate) fn prepare_blob_reference_retirement(
        &self,
        reference: &BlobRef,
        pending: &PendingBlobReferences,
        now_unix_millis: u64,
    ) -> Result<(Vec<u8>, BlobReferenceState), MutationError> {
        let key = blob_reference_key(reference);
        let mut state = match pending.get(&key).copied() {
            Some(state) => state,
            None => self.read_blob_reference_state(&key)?.ok_or_else(|| {
                MutationError::Storage(
                    "retired version references missing blob lifecycle metadata".into(),
                )
            })?,
        };
        validate_blob_reference_state(state)?;
        if state.flags & AWAITING_PUBLISH != 0 || state.ref_count == 0 {
            return Err(MutationError::Storage(
                "retired version has no published blob reference".into(),
            ));
        }
        state.ref_count -= 1;
        state.updated_at = state.updated_at.max(now_unix_millis);
        Ok((key, state))
    }

    /// Releases the one generic awaiting-publication reservation created when
    /// a prepared-program bundle was sealed. If these bytes were already
    /// published, sealing only refreshed their inactivity timestamp and there
    /// is no temporary reference to remove.
    pub(crate) fn prepare_awaiting_blob_release(
        &self,
        reference: &BlobRef,
        pending: &PendingBlobReferences,
        now_unix_millis: u64,
    ) -> Result<Option<(Vec<u8>, BlobReferenceState)>, MutationError> {
        let key = blob_reference_key(reference);
        let mut state = match pending.get(&key).copied() {
            Some(state) => state,
            None => self.read_blob_reference_state(&key)?.ok_or_else(|| {
                MutationError::Storage(
                    "prepared bundle references missing blob lifecycle metadata".into(),
                )
            })?,
        };
        validate_blob_reference_state(state)?;
        if state.flags & AWAITING_PUBLISH == 0 {
            return Ok(None);
        }
        if state.ref_count != 1 {
            return Err(MutationError::Storage(
                "awaiting-publish blob must have exactly one reservation".into(),
            ));
        }
        state.ref_count = 0;
        state.flags = 0;
        state.updated_at = state.updated_at.max(now_unix_millis);
        Ok(Some((key, state)))
    }

    pub(crate) fn stage_blob_reference_update(
        &self,
        batch: &mut WriteBatch,
        pending: &mut PendingBlobReferences,
        key: Vec<u8>,
        state: BlobReferenceState,
    ) -> Result<(), MutationError> {
        batch.put_cf(
            self.cf(CF_BLOB_REFERENCES)?,
            &key,
            encode_blob_reference_state(state),
        );
        pending.insert(key, state);
        Ok(())
    }

    pub(super) fn read_blob_reference_state(
        &self,
        key: &[u8],
    ) -> Result<Option<BlobReferenceState>, MutationError> {
        self.db
            .get_cf(self.cf(CF_BLOB_REFERENCES)?, key)
            .map_err(storage_error)?
            .map(|encoded| decode_blob_reference_state(&encoded))
            .transpose()
    }

    pub async fn open_blob(&self, reference: &BlobRef) -> Result<BlobReader, MutationError> {
        if is_small_blob(reference) {
            BlobReader::from_bytes(reference, self.read_blob_bytes(reference).await?)
                .map_err(storage_error)
        } else {
            self.blobs
                .open_verified(reference)
                .await
                .map_err(storage_error)
        }
    }
}

fn validate_blob_gc_budget(budget: BlobGcBudget) -> Result<(), MutationError> {
    if budget.max_records == 0 || budget.max_bytes == 0 || budget.max_duration.is_zero() {
        return Err(MutationError::Storage(
            "blob GC record, byte, and time budgets must be positive".into(),
        ));
    }
    Ok(())
}

fn filesystem_record_bytes(record: &crate::blob_gc::FilesystemGcRecord) -> u64 {
    use crate::blob_gc::FilesystemGcRecord;
    match record {
        FilesystemGcRecord::Directory { encoded_bytes, .. }
        | FilesystemGcRecord::Staged { encoded_bytes, .. }
        | FilesystemGcRecord::Quarantined { encoded_bytes, .. }
        | FilesystemGcRecord::Blob { encoded_bytes, .. }
        | FilesystemGcRecord::Shard { encoded_bytes, .. } => *encoded_bytes,
    }
}

fn path_encoded_bytes(path: &Path) -> u64 {
    path.as_os_str().as_encoded_bytes().len() as u64 + 32
}

fn modified_unix_millis(entry: &std::fs::DirEntry) -> Result<u64, MutationError> {
    entry
        .metadata()
        .map_err(storage_error)?
        .modified()
        .map_err(storage_error)?
        .duration_since(UNIX_EPOCH)
        .map_err(storage_error)?
        .as_millis()
        .try_into()
        .map_err(|_| MutationError::Storage("blob modification time exceeds u64".into()))
}

fn shard_file_path(root: &Path, identity: &ShardIdentity) -> PathBuf {
    let hash = hex::encode(identity.blob().hash);
    root.join(&hash[..2]).join(hex::encode(identity.encode()))
}

fn sync_directory(path: &Path) -> Result<(), MutationError> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(storage_error)
}
