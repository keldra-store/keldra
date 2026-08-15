use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::blob::blob_reference_from_staging_name;
use crate::blob_gc::{
    BlobGcBudget, BlobGcCursor, BlobGcPhase, BlobGcTick, FilesystemGcCursor, FilesystemGcDirectory,
};
use crate::key::STORAGE_KEY_FORMAT_VERSION;

use super::journal_capacity::SourceJournalAdmission;
use super::*;

static NEXT_GC_QUARANTINE_ID: AtomicU64 = AtomicU64::new(1);
const BLOB_GC_DUE_DOMAIN: u8 = b'B';
const BLOB_GC_COMPLETE_KIND: u8 = 0;
const BLOB_GC_SHARD_KIND: u8 = 1;
const BLOB_REFERENCE_IDENTITY_BYTES: usize = 32 + size_of::<u64>();
const SHARD_REFERENCE_IDENTITY_BYTES: usize = 2 + 32 + size_of::<u64>() + size_of::<u16>();
const BLOB_GC_DUE_PREFIX_BYTES: usize = 2;
const BLOB_GC_DUE_FIXED_BYTES: usize = BLOB_GC_DUE_PREFIX_BYTES + size_of::<u64>() + 1;

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
        // Hash and fsync under `.staging`, then make the lifecycle reservation
        // durable before exposing the canonical content path.
        let staged = upload.finish_staged().await.map_err(storage_error)?;
        let reference = staged.reference().clone();
        let now = now_unix_millis()?;
        if is_small_blob(&reference) {
            let bytes = tokio::fs::read(staged.path())
                .await
                .map_err(storage_error)?;
            self.persist_small_blob_seal_with_admission(&reference, &bytes, now, admission)
                .await?;
            remove_file_and_sync_parent(staged.path())?;
        } else {
            self.reserve_sealed_blob_with_admission_wait(&reference, now, admission)
                .await?;
            self.blobs
                .publish_staged(staged)
                .await
                .map_err(storage_error)?;
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
                BlobGcPhase::Due | BlobGcPhase::DueAfter(_) => {
                    let after = match &cursor.phase {
                        BlobGcPhase::Due => None,
                        BlobGcPhase::DueAfter(key) => Some(key.as_slice()),
                        BlobGcPhase::Filesystem(_) => unreachable!(),
                    };
                    let cutoff = now_unix_millis.saturating_sub(self.awaiting_publish_ttl_millis);
                    let Some(due) = self.next_blob_gc_due(after, cutoff)? else {
                        cursor.phase = BlobGcPhase::Filesystem(FilesystemGcCursor::default());
                        continue;
                    };
                    if tick.inspected_records != 0
                        && tick.inspected_bytes.saturating_add(due.encoded_bytes) > budget.max_bytes
                    {
                        break;
                    }
                    let removed = self.collect_due_artifact(&due, now_unix_millis).await?;
                    cursor.phase = BlobGcPhase::DueAfter(due.due_key);
                    tick.inspected_records += 1;
                    tick.inspected_bytes = tick.inspected_bytes.saturating_add(due.encoded_bytes);
                    tick.removed = tick.removed.saturating_add(u64::from(removed));
                }
                BlobGcPhase::Filesystem(filesystem) => {
                    let Some(record) = self.next_filesystem_gc_record(filesystem)? else {
                        cursor.phase = BlobGcPhase::Due;
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

    fn next_blob_gc_due(
        &self,
        after: Option<&[u8]>,
        cutoff_updated_at: u64,
    ) -> Result<Option<BlobGcDueRecord>, MutationError> {
        let prefix = blob_gc_due_prefix();
        let start = after.unwrap_or(&prefix);
        for entry in self.db.iterator_cf(
            self.cf(CF_BLOB_GC_DUE)?,
            IteratorMode::From(start, Direction::Forward),
        ) {
            let (key, value) = entry.map_err(storage_error)?;
            if after.is_some_and(|after| key.as_ref() <= after) {
                continue;
            }
            if !key.starts_with(&prefix) {
                return Ok(None);
            }
            let (updated_at, identity) = decode_blob_gc_due_key(&key)?;
            if updated_at > cutoff_updated_at {
                return Ok(None);
            }
            return Ok(Some(BlobGcDueRecord {
                due_key: key.to_vec(),
                identity,
                updated_at,
                encoded_bytes: (key.len() + value.len()) as u64,
            }));
        }
        Ok(None)
    }

    async fn collect_due_artifact(
        &self,
        due: &BlobGcDueRecord,
        now_unix_millis: u64,
    ) -> Result<bool, MutationError> {
        let quarantined = {
            let _commit_guard = self.commit_lock.lock().await;
            let Some(current) = self.read_blob_reference_state(&due.identity)? else {
                self.delete_stale_blob_gc_due(&due.due_key)?;
                return Ok(false);
            };
            if current.updated_at != due.updated_at || !blob_reference_needs_due(current) {
                self.delete_stale_blob_gc_due(&due.due_key)?;
                return Ok(false);
            }
            if !blob_reference_is_garbage(
                current,
                now_unix_millis,
                self.awaiting_publish_ttl_millis,
            ) {
                return Ok(false);
            }

            let physical = if due.identity.len() == BLOB_REFERENCE_IDENTITY_BYTES {
                let reference = blob_reference_from_key(&due.identity)?;
                if is_small_blob(&reference) {
                    None
                } else {
                    self.quarantine_gc_artifact(GcArtifact::Blob(reference))
                        .await?
                }
            } else {
                let identity = ShardIdentity::decode(&due.identity).map_err(storage_error)?;
                self.quarantine_gc_artifact(GcArtifact::Shard(identity))
                    .await?
            };
            let mut batch = WriteBatch::default();
            if due.identity.len() == BLOB_REFERENCE_IDENTITY_BYTES {
                let reference = blob_reference_from_key(&due.identity)?;
                if is_small_blob(&reference) {
                    batch.delete_cf(self.cf(CF_SMALL_BLOBS)?, &due.identity);
                }
            }
            self.stage_blob_reference_delete(&mut batch, &due.identity, current)?;
            self.stage_local_changes(
                &mut batch,
                &[PendingLocalChange::ContentLifecycleChanged {
                    blob_identity: due.identity.clone(),
                    revision: now_unix_millis,
                    reference_deltas: Vec::new(),
                }],
                LocalReferenceEffects::NoReferenceEffects,
            )?;
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
            self.notify_local_invalidations();
            physical
        };
        if let Some(path) = quarantined {
            remove_file_and_sync_parent(&path)?;
        }
        Ok(true)
    }

    fn delete_stale_blob_gc_due(&self, due_key: &[u8]) -> Result<(), MutationError> {
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .delete_cf_opt(self.cf(CF_BLOB_GC_DUE)?, due_key, &options)
            .map_err(storage_error)
    }

    #[cfg(test)]
    pub(super) async fn remove_blob_gc_reference_if_unchanged(
        &self,
        key: &[u8],
        expected: BlobReferenceState,
        now_unix_millis: u64,
    ) -> Result<bool, MutationError> {
        let due_key = blob_gc_due_key(key, expected)?
            .ok_or_else(|| MutationError::Storage("blob GC state is not due-indexed".into()))?;
        let due = BlobGcDueRecord {
            encoded_bytes: due_key.len() as u64,
            due_key,
            identity: key.to_vec(),
            updated_at: expected.updated_at,
        };
        let Some(current) = self.read_blob_reference_state(key)? else {
            return Ok(false);
        };
        if current != expected {
            return Ok(false);
        }
        self.collect_due_artifact(&due, now_unix_millis).await
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
            if cursor.entries.is_none() {
                let name = match cursor.directory {
                    FilesystemGcDirectory::Staging => ".staging",
                    FilesystemGcDirectory::Quarantine => ".gc",
                    FilesystemGcDirectory::Complete => return Ok(None),
                };
                cursor.entries = match std::fs::read_dir(self.blobs.root().join(name)) {
                    Ok(entries) => Some(entries),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        cursor.directory = next_filesystem_gc_directory(cursor.directory);
                        continue;
                    }
                    Err(error) => return Err(storage_error(error)),
                };
            }
            let Some(entry) = cursor
                .entries
                .as_mut()
                .expect("maintenance directory was opened")
                .next()
            else {
                cursor.entries = None;
                cursor.directory = next_filesystem_gc_directory(cursor.directory);
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
            return Ok(Some(match cursor.directory {
                FilesystemGcDirectory::Staging => FilesystemGcRecord::Staged {
                    modified_at: modified_unix_millis(&entry)?,
                    path,
                    encoded_bytes,
                },
                FilesystemGcDirectory::Quarantine => FilesystemGcRecord::Quarantined {
                    path,
                    encoded_bytes,
                },
                FilesystemGcDirectory::Complete => unreachable!(),
            }));
        }
    }

    async fn remove_filesystem_gc_record(
        &self,
        record: crate::blob_gc::FilesystemGcRecord,
        now_unix_millis: u64,
    ) -> Result<bool, MutationError> {
        use crate::blob_gc::FilesystemGcRecord;

        match record {
            FilesystemGcRecord::Quarantined { path, .. } => {
                return self.reconcile_quarantined_file(&path).await;
            }
            FilesystemGcRecord::Staged {
                path, modified_at, ..
            } => {
                self.reconcile_staged_file(&path, modified_at, now_unix_millis)
                    .await
            }
        }
    }

    async fn quarantine_gc_artifact(
        &self,
        artifact: GcArtifact,
    ) -> Result<Option<PathBuf>, MutationError> {
        let source = artifact.canonical_path(self.blobs.root());
        if !source.exists() {
            return Ok(None);
        }
        let quarantine = self.blobs.root().join(".gc");
        crate::blob::create_directory_all_durable(&quarantine)
            .await
            .map_err(storage_error)?;
        let destination = quarantine.join(gc_quarantine_name(
            &artifact,
            std::process::id(),
            self.blobs.upload_boot_nonce(),
            NEXT_GC_QUARANTINE_ID.fetch_add(1, Ordering::Relaxed),
        ));
        let _directory_guard = self.blobs.directory_lock.lock().await;
        match std::fs::rename(&source, &destination) {
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

    #[cfg(test)]
    pub(crate) async fn quarantine_blob_for_test(
        &self,
        reference: &BlobRef,
    ) -> Result<Option<PathBuf>, MutationError> {
        self.quarantine_gc_artifact(GcArtifact::Blob(reference.clone()))
            .await
    }

    pub(super) async fn quarantine_shard_for_removal(
        &self,
        identity: &ShardIdentity,
    ) -> Result<Option<PathBuf>, MutationError> {
        self.quarantine_gc_artifact(GcArtifact::Shard(identity.clone()))
            .await
    }

    pub(super) fn remove_quarantined_artifact(&self, path: &Path) -> Result<(), MutationError> {
        remove_file_and_sync_parent(path)
    }

    async fn reconcile_staged_file(
        &self,
        path: &Path,
        modified_at: u64,
        now_unix_millis: u64,
    ) -> Result<bool, MutationError> {
        if let Some(artifact) = staged_artifact_from_path(path)? {
            let _commit_guard = self.commit_lock.lock().await;
            if self
                .read_blob_reference_state(&artifact.lifecycle_key())?
                .is_some()
            {
                match &artifact {
                    GcArtifact::Blob(reference) if is_small_blob(reference) => {
                        let present = self
                            .db
                            .get_cf(self.cf(CF_SMALL_BLOBS)?, blob_reference_key(reference))
                            .map_err(storage_error)?
                            .is_some();
                        if !present {
                            return Err(MutationError::Storage(
                                "small-blob lifecycle exists without inline bytes".into(),
                            ));
                        }
                        remove_file_and_sync_parent(path)?;
                    }
                    GcArtifact::Blob(reference) => {
                        self.blobs
                            .publish_identified_staging(path, reference)
                            .await
                            .map_err(storage_error)?;
                    }
                    GcArtifact::Shard(identity) => {
                        self.publish_identified_staged_shard(identity, path)
                            .await
                            .map_err(storage_error)?;
                    }
                }
                return Ok(true);
            }
        }
        if now_unix_millis.saturating_sub(modified_at) < self.awaiting_publish_ttl_millis {
            return Ok(false);
        }
        remove_file_and_sync_parent(path)?;
        Ok(true)
    }

    async fn reconcile_quarantined_file(&self, path: &Path) -> Result<bool, MutationError> {
        let Some(artifact) = quarantined_artifact_from_path(path)? else {
            remove_file_and_sync_parent(path)?;
            return Ok(true);
        };
        let _commit_guard = self.commit_lock.lock().await;
        if self
            .read_blob_reference_state(&artifact.lifecycle_key())?
            .is_none()
        {
            remove_file_and_sync_parent(path)?;
            return Ok(true);
        }
        match artifact {
            GcArtifact::Blob(reference) => {
                self.blobs
                    .publish_identified_staging(path, &reference)
                    .await
                    .map_err(storage_error)?;
            }
            GcArtifact::Shard(identity) => {
                self.publish_identified_staged_shard(&identity, path)
                    .await
                    .map_err(storage_error)?;
            }
        }
        Ok(true)
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
        let mut pending = PendingBlobReferences::new();
        self.stage_blob_reference_update(&mut batch, &mut pending, key.to_vec(), next)?;
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
        let mut references = PendingBlobReferences::new();
        self.stage_blob_reference_update(&mut batch, &mut references, key.clone(), state)?;
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
        self.prepare_blob_reference_publication_cached(reference, pending, None, now_unix_millis)
    }

    pub(super) fn prepare_blob_reference_publication_cached(
        &self,
        reference: &BlobRef,
        pending: &PendingBlobReferences,
        prefetched: Option<Result<Option<BlobReferenceState>, MutationError>>,
        now_unix_millis: u64,
    ) -> Result<(Vec<u8>, BlobReferenceState), MutationError> {
        let key = blob_reference_key(reference);
        let state = match pending.get(&key).copied() {
            Some(state) => state,
            None => match prefetched {
                Some(cached) => cached?,
                None => self.read_blob_reference_state(&key)?,
            }
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
        prefetched: Option<Result<Option<BlobReferenceState>, MutationError>>,
        now_unix_millis: u64,
    ) -> Result<(Vec<u8>, BlobReferenceState), MutationError> {
        let key = blob_reference_key(reference);
        let state = match pending.get(&key).copied() {
            Some(state) => advance_blob_reference_publication(state, now_unix_millis)?,
            None => match match prefetched {
                Some(cached) => cached?,
                None => self.read_blob_reference_state(&key)?,
            } {
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
        self.prepare_hashed_small_blob_value_cached(reference, bytes, pending, None)
    }

    pub(super) fn prepare_hashed_small_blob_value_cached(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        pending: &BTreeSet<Vec<u8>>,
        prefetched: Option<Result<Option<Vec<u8>>, MutationError>>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MutationError> {
        let key = blob_reference_key(reference);
        if pending.contains(&key) {
            return Ok(None);
        }
        let existing = match prefetched {
            Some(cached) => cached?,
            None => self
                .db
                .get_cf(self.cf(CF_SMALL_BLOBS)?, &key)
                .map_err(storage_error)?
                .map(|encoded| encoded.to_vec()),
        };
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
        self.prepare_blob_reference_retirement_cached(reference, pending, None, now_unix_millis)
    }

    pub(super) fn prepare_blob_reference_retirement_cached(
        &self,
        reference: &BlobRef,
        pending: &PendingBlobReferences,
        prefetched: Option<Result<Option<BlobReferenceState>, MutationError>>,
        now_unix_millis: u64,
    ) -> Result<(Vec<u8>, BlobReferenceState), MutationError> {
        let key = blob_reference_key(reference);
        let mut state = match pending.get(&key).copied() {
            Some(state) => state,
            None => match prefetched {
                Some(cached) => cached?,
                None => self.read_blob_reference_state(&key)?,
            }
            .ok_or_else(|| {
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
        self.stage_blob_reference_update_cached(batch, pending, key, state, None)
    }

    pub(super) fn stage_blob_reference_update_cached(
        &self,
        batch: &mut WriteBatch,
        pending: &mut PendingBlobReferences,
        key: Vec<u8>,
        state: BlobReferenceState,
        prefetched: Option<Result<Option<BlobReferenceState>, MutationError>>,
    ) -> Result<(), MutationError> {
        let previous = match pending.get(&key).copied() {
            Some(state) => Some(state),
            None => match prefetched {
                Some(cached) => cached?,
                None => self.read_blob_reference_state(&key)?,
            },
        };
        if let Some(previous) = previous
            && let Some(old_due) = blob_gc_due_key(&key, previous)?
        {
            batch.delete_cf(self.cf(CF_BLOB_GC_DUE)?, old_due);
        }
        batch.put_cf(
            self.cf(CF_BLOB_REFERENCES)?,
            &key,
            encode_blob_reference_state(state),
        );
        if let Some(new_due) = blob_gc_due_key(&key, state)? {
            batch.put_cf(self.cf(CF_BLOB_GC_DUE)?, new_due, []);
        }
        pending.insert(key, state);
        Ok(())
    }

    pub(super) fn stage_blob_reference_delete(
        &self,
        batch: &mut WriteBatch,
        key: &[u8],
        state: BlobReferenceState,
    ) -> Result<(), MutationError> {
        if let Some(due) = blob_gc_due_key(key, state)? {
            batch.delete_cf(self.cf(CF_BLOB_GC_DUE)?, due);
        }
        batch.delete_cf(self.cf(CF_BLOB_REFERENCES)?, key);
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
        FilesystemGcRecord::Staged { encoded_bytes, .. }
        | FilesystemGcRecord::Quarantined { encoded_bytes, .. } => *encoded_bytes,
    }
}

fn next_filesystem_gc_directory(directory: FilesystemGcDirectory) -> FilesystemGcDirectory {
    match directory {
        FilesystemGcDirectory::Staging => FilesystemGcDirectory::Quarantine,
        FilesystemGcDirectory::Quarantine | FilesystemGcDirectory::Complete => {
            FilesystemGcDirectory::Complete
        }
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum GcArtifact {
    Blob(BlobRef),
    Shard(ShardIdentity),
}

impl GcArtifact {
    fn lifecycle_key(&self) -> Vec<u8> {
        match self {
            Self::Blob(reference) => blob_reference_key(reference),
            Self::Shard(identity) => identity.encode().to_vec(),
        }
    }

    fn canonical_path(&self, root: &Path) -> PathBuf {
        match self {
            Self::Blob(reference) => {
                let encoded = hex::encode(reference.hash);
                root.join(&encoded[..2]).join(encoded)
            }
            Self::Shard(identity) => shard_file_path(root, identity),
        }
    }
}

struct BlobGcDueRecord {
    due_key: Vec<u8>,
    identity: Vec<u8>,
    updated_at: u64,
    encoded_bytes: u64,
}

fn blob_reference_needs_due(state: BlobReferenceState) -> bool {
    state.ref_count == 0 || state.flags & AWAITING_PUBLISH != 0
}

fn blob_gc_due_prefix() -> [u8; BLOB_GC_DUE_PREFIX_BYTES] {
    [STORAGE_KEY_FORMAT_VERSION, BLOB_GC_DUE_DOMAIN]
}

pub(super) fn blob_gc_due_key(
    identity: &[u8],
    state: BlobReferenceState,
) -> Result<Option<Vec<u8>>, MutationError> {
    validate_blob_reference_state(state)?;
    if !blob_reference_needs_due(state) {
        return Ok(None);
    }
    let kind = match identity.len() {
        BLOB_REFERENCE_IDENTITY_BYTES => {
            blob_reference_from_key(identity)?;
            BLOB_GC_COMPLETE_KIND
        }
        SHARD_REFERENCE_IDENTITY_BYTES => {
            ShardIdentity::decode(identity).map_err(storage_error)?;
            BLOB_GC_SHARD_KIND
        }
        _ => {
            return Err(MutationError::Storage(
                "blob lifecycle identity is malformed".into(),
            ));
        }
    };
    let mut key = Vec::with_capacity(BLOB_GC_DUE_FIXED_BYTES + identity.len());
    key.extend_from_slice(&blob_gc_due_prefix());
    key.extend_from_slice(&state.updated_at.to_be_bytes());
    key.push(kind);
    key.extend_from_slice(identity);
    Ok(Some(key))
}

fn decode_blob_gc_due_key(encoded: &[u8]) -> Result<(u64, Vec<u8>), MutationError> {
    if encoded.len() < BLOB_GC_DUE_FIXED_BYTES || !encoded.starts_with(&blob_gc_due_prefix()) {
        return Err(MutationError::Storage(
            "blob GC due key is malformed".into(),
        ));
    }
    let updated_at = u64::from_be_bytes(
        encoded[BLOB_GC_DUE_PREFIX_BYTES..BLOB_GC_DUE_PREFIX_BYTES + size_of::<u64>()]
            .try_into()
            .expect("blob GC timestamp width was checked"),
    );
    let kind = encoded[BLOB_GC_DUE_PREFIX_BYTES + size_of::<u64>()];
    let identity = &encoded[BLOB_GC_DUE_FIXED_BYTES..];
    match kind {
        BLOB_GC_COMPLETE_KIND if identity.len() == BLOB_REFERENCE_IDENTITY_BYTES => {
            blob_reference_from_key(identity)?;
        }
        BLOB_GC_SHARD_KIND if identity.len() == SHARD_REFERENCE_IDENTITY_BYTES => {
            ShardIdentity::decode(identity).map_err(storage_error)?;
        }
        _ => {
            return Err(MutationError::Storage(
                "blob GC due identity is malformed".into(),
            ));
        }
    }
    Ok((updated_at, identity.to_vec()))
}

fn staged_artifact_from_path(path: &Path) -> Result<Option<GcArtifact>, MutationError> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(None);
    };
    if let Some(reference) = blob_reference_from_staging_name(name) {
        return Ok(Some(GcArtifact::Blob(reference)));
    }
    shard_identity_from_staging_name(name)
        .map(|identity| identity.map(GcArtifact::Shard))
        .map_err(storage_error)
}

fn shard_identity_from_staging_name(name: &str) -> Result<Option<ShardIdentity>, ShardStoreError> {
    let Some(body) = name
        .strip_prefix("shard-")
        .and_then(|name| name.strip_suffix(".tmp"))
    else {
        return Ok(None);
    };
    let fields = body.split('-').collect::<Vec<_>>();
    let identity = match fields.as_slice() {
        [process_id, upload_id, identity]
            if process_id.parse::<u32>().is_ok() && upload_id.parse::<u64>().is_ok() =>
        {
            *identity
        }
        [process_id, nonce, upload_id, identity]
            if process_id.parse::<u32>().is_ok()
                && nonce.len() == 32
                && upload_id.parse::<u64>().is_ok() =>
        {
            *identity
        }
        _ => return Ok(None),
    };
    if identity.len() != SHARD_REFERENCE_IDENTITY_BYTES * 2 {
        return Ok(None);
    }
    let mut encoded = [0_u8; SHARD_REFERENCE_IDENTITY_BYTES];
    if hex::decode_to_slice(identity, &mut encoded).is_err() {
        return Ok(None);
    }
    ShardIdentity::decode(&encoded).map(Some)
}

fn gc_quarantine_name(
    artifact: &GcArtifact,
    process_id: u32,
    boot_nonce: &[u8],
    sequence: u64,
) -> String {
    let (kind, identity) = match artifact {
        GcArtifact::Blob(reference) => ("blob", blob_reference_key(reference)),
        GcArtifact::Shard(identity) => ("shard", identity.encode().to_vec()),
    };
    format!(
        "gc-{kind}-{process_id}-{}-{sequence}-{}.tmp",
        hex::encode(boot_nonce),
        hex::encode(identity),
    )
}

fn quarantined_artifact_from_path(path: &Path) -> Result<Option<GcArtifact>, MutationError> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(None);
    };
    let Some(body) = name
        .strip_prefix("gc-")
        .and_then(|name| name.strip_suffix(".tmp"))
    else {
        return Ok(None);
    };
    let fields = body.split('-').collect::<Vec<_>>();
    let [kind, process_id, nonce, sequence, identity] = fields.as_slice() else {
        return Ok(None);
    };
    if process_id.parse::<u32>().is_err() || nonce.len() != 32 || sequence.parse::<u64>().is_err() {
        return Ok(None);
    }
    let expected = match *kind {
        "blob" => BLOB_REFERENCE_IDENTITY_BYTES,
        "shard" => SHARD_REFERENCE_IDENTITY_BYTES,
        _ => return Ok(None),
    };
    if identity.len() != expected * 2 {
        return Ok(None);
    }
    let mut encoded = vec![0_u8; expected];
    if hex::decode_to_slice(identity, &mut encoded).is_err() {
        return Ok(None);
    }
    match *kind {
        "blob" => blob_reference_from_key(&encoded).map(GcArtifact::Blob),
        "shard" => ShardIdentity::decode(&encoded)
            .map(GcArtifact::Shard)
            .map_err(storage_error),
        _ => unreachable!(),
    }
    .map(Some)
}

fn sync_directory(path: &Path) -> Result<(), MutationError> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(storage_error)
}
