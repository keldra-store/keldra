use std::time::Instant;

use crate::blob_gc::{BlobGcBudget, BlobGcCursor, BlobGcPhase, BlobGcTick};
use crate::key::STORAGE_KEY_FORMAT_VERSION;

use super::journal_capacity::SourceJournalAdmission;
use super::mutation_prefetch::MutationReadCache;
use super::payload_artifacts::{ArtifactKind, ArtifactManifest, RocksArtifactReader};
use super::*;

const BLOB_GC_DUE_DOMAIN: u8 = b'B';
const BLOB_GC_COMPLETE_KIND: u8 = 0;
const BLOB_GC_SHARD_KIND: u8 = 1;
const BLOB_GC_UPLOAD_KIND: u8 = 2;
const BLOB_REFERENCE_IDENTITY_BYTES: usize = 32 + size_of::<u64>();
const SHARD_REFERENCE_IDENTITY_BYTES: usize = 2 + 32 + size_of::<u64>() + size_of::<u16>();
const UPLOAD_IDENTITY_BYTES: usize = 32;
const BLOB_GC_DUE_PREFIX_BYTES: usize = 2;
const BLOB_GC_DUE_FIXED_BYTES: usize = BLOB_GC_DUE_PREFIX_BYTES + size_of::<u64>() + 1;

impl Store {
    #[cfg(test)]
    pub(crate) fn has_pending_upload_install(
        &self,
        upload_id: &[u8; 32],
    ) -> Result<bool, MutationError> {
        Ok(self.read_artifact_install_state(upload_id)?.is_some())
    }

    pub async fn stage_blob(&self, bytes: &[u8]) -> Result<BlobRef, MutationError> {
        self.stage_blob_with_admission(bytes, SourceJournalAdmission::Bounded)
            .await
    }

    pub(super) async fn stage_blob_with_admission(
        &self,
        bytes: &[u8],
        admission: SourceJournalAdmission,
    ) -> Result<BlobRef, MutationError> {
        if bytes.len() <= PAYLOAD_ARTIFACT_CHUNK_BYTES {
            let reference = blob_reference_for_bytes(bytes);
            self.persist_inline_payload_seal_with_admission(
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

    pub(super) async fn stage_derived_progress_inline_blob_batch(
        &self,
        blobs: &[Vec<u8>],
    ) -> Result<Vec<BlobRef>, MutationError> {
        if blobs.len() > MAX_DERIVED_PROGRESS_INLINE_BATCH_ITEMS {
            return Err(MutationError::Storage(format!(
                "derived-progress inline blob batch exceeds the {MAX_DERIVED_PROGRESS_INLINE_BATCH_ITEMS}-item bound"
            )));
        }
        if blobs.is_empty() {
            return Ok(Vec::new());
        }

        let mut logical_bytes = 0_u64;
        let mut references = Vec::with_capacity(blobs.len());
        for bytes in blobs {
            if bytes.len() > PAYLOAD_ARTIFACT_CHUNK_BYTES {
                return Err(MutationError::Storage(
                    "derived-progress inline blob batch contains a chunked payload".into(),
                ));
            }
            logical_bytes = logical_bytes
                .checked_add(u64::try_from(bytes.len()).map_err(storage_error)?)
                .ok_or_else(|| {
                    MutationError::Storage(
                        "derived-progress inline blob batch byte count overflow".into(),
                    )
                })?;
            if logical_bytes > MAX_DERIVED_PROGRESS_INLINE_BATCH_BYTES {
                return Err(MutationError::Storage(format!(
                    "derived-progress inline blob batch exceeds the {MAX_DERIVED_PROGRESS_INLINE_BATCH_BYTES}-byte bound"
                )));
            }
            let reference = blob_reference_for_bytes(bytes);
            validate_complete_artifact(&reference, bytes)?;
            references.push(reference);
        }

        let now = now_unix_millis()?;
        let _guard = self.lock_commit("blob_reference").await;
        self.persist_derived_progress_inline_blob_batch(blobs, &references, now)?;
        Ok(references)
    }

    fn persist_derived_progress_inline_blob_batch(
        &self,
        blobs: &[Vec<u8>],
        references: &[BlobRef],
        now_unix_millis: u64,
    ) -> Result<(), MutationError> {
        let mut batch = WriteBatch::default();
        let mut pending_inline_payloads = BTreeSet::new();
        let mut pending_blob_references = PendingBlobReferences::new();
        let mut changes = Vec::with_capacity(blobs.len());

        for (bytes, reference) in blobs.iter().zip(references) {
            if let Some((artifact_key, artifact_bytes)) =
                self.prepare_inline_payload_value(reference, bytes, &pending_inline_payloads)?
            {
                self.stage_inline_complete_artifact(&mut batch, reference, &artifact_bytes)?;
                pending_inline_payloads.insert(artifact_key);
            }
            let state = self
                .prepare_sealed_blob_reservation(reference, now_unix_millis)?
                .ok_or_else(|| {
                    MutationError::Storage("inline payload reservation is missing".into())
                })?;
            let key = blob_reference_key(reference);
            self.stage_blob_reference_update(
                &mut batch,
                &mut pending_blob_references,
                key.clone(),
                state,
            )?;
            changes.push(PendingLocalChange::ContentLifecycleChanged {
                blob_identity: key,
                revision: state.updated_at,
                reference_deltas: Vec::new(),
                accounting_transition: None,
            });
        }
        self.stage_local_changes_with_admission(
            &mut batch,
            &changes,
            LocalReferenceEffects::NoReferenceEffects,
            SourceJournalAdmission::DerivedProgress,
        )?;
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        self.notify_local_invalidations();
        Ok(())
    }

    pub fn lock_manager(&self) -> LocalLockManager {
        self.program_locks.clone()
    }

    pub async fn begin_blob_upload(&self) -> Result<crate::BlobUpload, MutationError> {
        self.blobs.begin_upload(self.clone()).map_err(storage_error)
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
        let staged = upload.finish_staged().await.map_err(storage_error)?;
        self.seal_staged_blob_with_admission(staged, admission)
            .await
    }

    pub(super) async fn seal_staged_blob_with_admission(
        &self,
        staged: crate::blob::StagedBlob,
        admission: SourceJournalAdmission,
    ) -> Result<BlobRef, MutationError> {
        let reference = staged.reference().clone();
        let now = now_unix_millis()?;
        if staged.persisted_chunks() == 0 {
            self.persist_inline_payload_seal_with_admission(
                &reference,
                staged.final_chunk(),
                now,
                admission,
            )
            .await?;
        } else {
            if !staged.final_chunk().is_empty() {
                self.persist_pending_upload_chunk(
                    staged.upload_id(),
                    staged.persisted_chunks(),
                    staged.final_chunk().to_vec(),
                )
                .await?;
            }
            self.finish_pending_upload(&staged, now, admission).await?;
        }
        Ok(reference)
    }

    pub(crate) async fn persist_pending_upload_chunk(
        &self,
        upload_id: [u8; 32],
        ordinal: u32,
        bytes: Vec<u8>,
    ) -> Result<(), MutationError> {
        if bytes.len() > PAYLOAD_ARTIFACT_CHUNK_BYTES {
            return Err(MutationError::Storage(
                "pending upload chunk exceeds the payload chunk bound".into(),
            ));
        }
        let now = now_unix_millis()?;
        let _guard = self.lock_commit("payload_upload").await;
        let manifest = ArtifactManifest::upload(upload_id);
        let (start, previous_updated_at) = if let Some((current, next, updated_at)) =
            self.read_artifact_install_state(&upload_id)?
        {
            if current != manifest {
                return Err(MutationError::Storage(
                    "pending upload identity has a conflicting installation".into(),
                ));
            }
            (next, updated_at)
        } else {
            let mut batch = WriteBatch::default();
            let start =
                self.begin_artifact_install(&mut batch, &upload_id, manifest.clone(), now)?;
            batch.put_cf(
                self.cf(CF_BLOB_GC_DUE)?,
                upload_gc_due_key(&upload_id, now),
                [],
            );
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
            (start, now)
        };
        if start != ordinal {
            return Err(MutationError::Storage(
                "pending upload chunk ordinal is not contiguous".into(),
            ));
        }
        let previous_due = upload_gc_due_key(&upload_id, previous_updated_at);
        let replacement_due = upload_gc_due_key(&upload_id, now);
        self.advance_artifact_install(
            &upload_id,
            &manifest,
            ordinal,
            &bytes,
            now,
            Some((&previous_due, &replacement_due)),
        )
    }

    async fn finish_pending_upload(
        &self,
        staged: &crate::blob::StagedBlob,
        now: u64,
        admission: SourceJournalAdmission,
    ) -> Result<(), MutationError> {
        let reference = staged.reference();
        let manifest = ArtifactManifest::uploaded_complete(reference, staged.upload_id())?;
        let mut verification = BlobReader::from_rocksdb(
            reference,
            RocksArtifactReader::new(self.db.clone(), manifest.clone()),
        );
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            match verification.read(&mut buffer).await {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) => return Err(storage_error(error)),
            }
        }

        loop {
            let observed_existing = self.read_complete_manifest(reference)?;
            if let Some(existing) = &observed_existing {
                let mut reader = BlobReader::from_rocksdb(
                    reference,
                    RocksArtifactReader::new(self.db.clone(), existing.clone()),
                );
                loop {
                    match reader.read(&mut buffer).await {
                        Ok(0) => break,
                        Ok(_) => {}
                        Err(error) => return Err(storage_error(error)),
                    }
                }
            }
            let guard = self.lock_commit("payload_upload").await;
            let result = (|| {
                let Some((upload_manifest, _, upload_updated_at)) =
                    self.read_artifact_install_state(&staged.upload_id())?
                else {
                    return Err(MutationError::Storage(
                        "pending upload disappeared before finalization".into(),
                    ));
                };
                let current_existing = self.read_complete_manifest(reference)?;
                if current_existing != observed_existing {
                    return Ok(None);
                }
                let current_lifecycle =
                    self.read_blob_reference_state(&blob_reference_key(reference))?;
                match (current_existing.is_some(), current_lifecycle.is_some()) {
                    (true, false) => {
                        return Err(MutationError::Storage(
                            "sealed payload manifest has no lifecycle authority".into(),
                        ));
                    }
                    (false, true) => {
                        return Err(MutationError::Storage(
                            "sealed payload lifecycle has no published manifest".into(),
                        ));
                    }
                    _ => {}
                }
                let state = self
                    .prepare_sealed_blob_reservation(reference, now)?
                    .ok_or_else(|| {
                        MutationError::Storage("sealed upload lifecycle is missing".into())
                    })?;
                let mut batch = WriteBatch::default();
                if let Some(existing) = current_existing {
                    if existing.storage_id == manifest.storage_id {
                        return Err(MutationError::Storage(
                            "pending upload storage identity collides with a sealed artifact"
                                .into(),
                        ));
                    }
                    self.stage_artifact_delete(&mut batch, &staged.upload_id(), &upload_manifest)?;
                } else {
                    self.stage_uploaded_complete_manifest(
                        &mut batch,
                        &staged.upload_id(),
                        reference,
                        &manifest,
                    )?;
                }
                batch.delete_cf(
                    self.cf(CF_BLOB_GC_DUE)?,
                    upload_gc_due_key(&staged.upload_id(), upload_updated_at),
                );
                let mut pending = PendingBlobReferences::new();
                let identity = blob_reference_key(reference);
                self.stage_blob_reference_update(
                    &mut batch,
                    &mut pending,
                    identity.clone(),
                    state,
                )?;
                self.stage_local_changes_with_admission(
                    &mut batch,
                    &[PendingLocalChange::ContentLifecycleChanged {
                        blob_identity: identity,
                        revision: state.updated_at,
                        reference_deltas: Vec::new(),
                        accounting_transition: None,
                    }],
                    LocalReferenceEffects::NoReferenceEffects,
                    admission,
                )?;
                let mut options = WriteOptions::default();
                options.set_sync(self.sync_writes);
                self.db.write_opt(batch, &options).map_err(storage_error)?;
                self.notify_local_invalidations();
                Ok(Some(()))
            })();
            drop(guard);
            match result {
                Ok(None) => continue,
                Err(MutationError::SourceJournalCapacity)
                    if admission == SourceJournalAdmission::Bounded =>
                {
                    self.wait_for_mutation_capacity().await;
                }
                Ok(Some(())) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn read_blob_bytes(
        &self,
        reference: &BlobRef,
    ) -> Result<Vec<u8>, MutationError> {
        let mut reader = self.open_blob(reference).await?;
        let capacity = usize::try_from(reference.length)
            .map_err(|_| MutationError::Storage("blob length does not fit in memory".into()))?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut buffer = vec![0_u8; PAYLOAD_ARTIFACT_CHUNK_BYTES.min(64 * 1024)];
        loop {
            let read = reader.read(&mut buffer).await.map_err(storage_error)?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        Ok(bytes)
    }

    pub(crate) async fn read_retained_blob_bytes(
        &self,
        reference: &BlobRef,
    ) -> Result<Vec<u8>, MutationError> {
        let mut reader = self.open_retained_blob(reference).await?;
        let capacity = usize::try_from(reference.length)
            .map_err(|_| MutationError::Storage("blob length does not fit in memory".into()))?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut buffer = vec![0_u8; PAYLOAD_ARTIFACT_CHUNK_BYTES.min(64 * 1024)];
        loop {
            let read = reader.read(&mut buffer).await.map_err(storage_error)?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        Ok(bytes)
    }

    pub(super) async fn contains_blob(&self, reference: &BlobRef) -> Result<bool, MutationError> {
        let Some(state) = self.blob_reference_state(reference)? else {
            return Ok(false);
        };
        validate_blob_reference_state(state)?;
        Ok(state.ref_count != 0 && self.read_complete_manifest(reference)?.is_some())
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
                    };
                    let cutoff = now_unix_millis.saturating_sub(self.awaiting_publish_ttl_millis);
                    let Some(due) = self.next_blob_gc_due(after, cutoff)? else {
                        cursor.phase = BlobGcPhase::Due;
                        tick.cycle_complete = true;
                        break;
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
        if due.identity.len() == UPLOAD_IDENTITY_BYTES {
            return self.collect_pending_upload_due(due).await;
        }
        {
            let _commit_guard = self.lock_commit("blob_reference").await;
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

            let manifest = if due.identity.len() == BLOB_REFERENCE_IDENTITY_BYTES {
                let reference = blob_reference_from_key(&due.identity)?;
                self.read_complete_manifest(&reference)?
            } else {
                let identity = ShardIdentity::decode(&due.identity).map_err(storage_error)?;
                self.read_shard_manifest(&identity)?
            }
            .or(self.read_artifact_install_manifest(&due.identity)?)
            .ok_or_else(|| {
                MutationError::Storage(
                    "payload lifecycle has neither a sealed manifest nor an installation record"
                        .into(),
                )
            })?;
            let mut batch = WriteBatch::default();
            self.stage_artifact_delete(&mut batch, &due.identity, &manifest)?;
            self.stage_blob_reference_delete(&mut batch, &due.identity, current)?;
            self.stage_local_changes(
                &mut batch,
                &[PendingLocalChange::ContentLifecycleChanged {
                    blob_identity: due.identity.clone(),
                    revision: now_unix_millis,
                    reference_deltas: Vec::new(),
                    accounting_transition: None,
                }],
                LocalReferenceEffects::NoReferenceEffects,
            )?;
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
            self.notify_local_invalidations();
        }
        Ok(true)
    }

    async fn collect_pending_upload_due(
        &self,
        due: &BlobGcDueRecord,
    ) -> Result<bool, MutationError> {
        let _commit_guard = self.lock_commit("payload_upload_gc").await;
        let Some((manifest, _, updated_at)) = self.read_artifact_install_state(&due.identity)?
        else {
            self.delete_stale_blob_gc_due(&due.due_key)?;
            return Ok(false);
        };
        if manifest.kind != ArtifactKind::Upload
            || manifest.storage_id.as_slice() != due.identity.as_slice()
        {
            return Err(MutationError::Storage(
                "pending upload GC authority is malformed".into(),
            ));
        }
        if updated_at != due.updated_at {
            let mut batch = WriteBatch::default();
            batch.delete_cf(self.cf(CF_BLOB_GC_DUE)?, &due.due_key);
            batch.put_cf(
                self.cf(CF_BLOB_GC_DUE)?,
                upload_gc_due_key(&manifest.storage_id, updated_at),
                [],
            );
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
            return Ok(false);
        }
        let mut batch = WriteBatch::default();
        self.stage_artifact_delete(&mut batch, &due.identity, &manifest)?;
        batch.delete_cf(self.cf(CF_BLOB_GC_DUE)?, &due.due_key);
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
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

    pub(super) async fn begin_sealed_artifact_install_with_admission_wait(
        &self,
        identity: &[u8],
        manifest: ArtifactManifest,
        now_unix_millis: u64,
        admission: SourceJournalAdmission,
    ) -> Result<u32, MutationError> {
        loop {
            let guard = self.lock_commit("payload_install").await;
            let result = (|| {
                let state = self
                    .prepare_sealed_artifact_reservation(identity, now_unix_millis)?
                    .ok_or_else(|| {
                        MutationError::Storage("sealed lifecycle state is missing".into())
                    })?;
                let mut batch = WriteBatch::default();
                let start = self.begin_artifact_install(
                    &mut batch,
                    identity,
                    manifest.clone(),
                    now_unix_millis,
                )?;
                let mut pending = PendingBlobReferences::new();
                self.stage_blob_reference_update(
                    &mut batch,
                    &mut pending,
                    identity.to_vec(),
                    state,
                )?;
                self.stage_local_changes_with_admission(
                    &mut batch,
                    &[PendingLocalChange::ContentLifecycleChanged {
                        blob_identity: identity.to_vec(),
                        revision: state.updated_at,
                        reference_deltas: Vec::new(),
                        accounting_transition: None,
                    }],
                    LocalReferenceEffects::NoReferenceEffects,
                    admission,
                )?;
                let mut options = WriteOptions::default();
                options.set_sync(self.sync_writes);
                self.db.write_opt(batch, &options).map_err(storage_error)?;
                self.notify_local_invalidations();
                Ok(start)
            })();
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
                accounting_transition: None,
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

    #[cfg(test)]
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

    #[cfg(test)]
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

    pub(super) async fn reserve_sealed_artifact_with_admission_wait(
        &self,
        identity: &[u8],
        now_unix_millis: u64,
        admission: SourceJournalAdmission,
    ) -> Result<(), MutationError> {
        loop {
            let guard = self.lock_commit("blob_reference").await;
            let result = self
                .reserve_sealed_artifact_with_admission(identity, now_unix_millis, admission)
                .map(|_| ());
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

    async fn persist_inline_payload_seal_with_admission(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        now_unix_millis: u64,
        admission: SourceJournalAdmission,
    ) -> Result<(), MutationError> {
        loop {
            let guard = self.lock_commit("blob_reference").await;
            let result =
                self.persist_inline_payload_seal(reference, bytes, now_unix_millis, admission);
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

    fn persist_inline_payload_seal(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        now_unix_millis: u64,
        admission: SourceJournalAdmission,
    ) -> Result<(), MutationError> {
        validate_complete_artifact(reference, bytes)?;
        let pending = BTreeSet::new();
        let value = self.prepare_inline_payload_value(reference, bytes, &pending)?;
        let state = self
            .prepare_sealed_blob_reservation(reference, now_unix_millis)?
            .ok_or_else(|| {
                MutationError::Storage("inline payload reservation is missing".into())
            })?;
        let key = blob_reference_key(reference);
        let mut batch = WriteBatch::default();
        if let Some((_artifact_key, artifact_bytes)) = value {
            self.stage_inline_complete_artifact(&mut batch, reference, &artifact_bytes)?;
        }
        let mut references = PendingBlobReferences::new();
        self.stage_blob_reference_update(&mut batch, &mut references, key.clone(), state)?;
        self.stage_local_changes_with_admission(
            &mut batch,
            &[PendingLocalChange::ContentLifecycleChanged {
                blob_identity: key,
                revision: state.updated_at,
                reference_deltas: Vec::new(),
                accounting_transition: None,
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

    /// Materializes inline bytes and, when their reference effect must remain
    /// ordered behind a journal prefix, retains them with a reservation that
    /// the eventual positive delta publishes without double-counting.
    pub(super) fn prepare_coordinated_inline_payload(
        &self,
        operation: &PreparedOperation,
        materialize: bool,
        reserve_for_deferred_delta: bool,
        pending_inline_payloads: &BTreeSet<Vec<u8>>,
        pending_blob_references: &PendingBlobReferences,
        read_cache: &MutationReadCache,
        now_unix_millis: u64,
    ) -> Result<
        (
            Option<(Vec<u8>, Vec<u8>)>,
            Option<(Vec<u8>, BlobReferenceState)>,
        ),
        MutationError,
    > {
        if !materialize {
            return Ok((None, None));
        }
        let PreparedOperation::Put { payload, .. } = operation else {
            return Ok((None, None));
        };
        let inline_payload = match payload.inline_bytes() {
            Some(bytes) => self.prepare_hashed_inline_payload_value_cached(
                payload.reference(),
                bytes,
                pending_inline_payloads,
                read_cache.inline_payload(payload.reference()),
            )?,
            None => None,
        };
        if !reserve_for_deferred_delta {
            return Ok((inline_payload, None));
        }
        let key = blob_reference_key(payload.reference());
        let reservation = if pending_blob_references.contains_key(&key) {
            None
        } else {
            self.prepare_sealed_artifact_reservation(&key, now_unix_millis)?
                .map(|state| (key, state))
        };
        Ok((inline_payload, reservation))
    }

    pub(super) fn prepare_inline_payload_value(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        pending: &BTreeSet<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MutationError> {
        validate_complete_artifact(reference, bytes)?;
        self.prepare_hashed_inline_payload_value(reference, bytes, pending)
    }

    /// Prepares bytes whose reference was computed from this exact immutable
    /// slice during the current put preparation. Existing stored bytes remain
    /// independently verified before content-address reuse.
    pub(super) fn prepare_hashed_inline_payload_value(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        pending: &BTreeSet<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MutationError> {
        self.prepare_hashed_inline_payload_value_cached(reference, bytes, pending, None)
    }

    pub(super) fn prepare_hashed_inline_payload_value_cached(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        pending: &BTreeSet<Vec<u8>>,
        prefetched: Option<Result<Option<Vec<u8>>, MutationError>>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, MutationError> {
        let key = complete_artifact_key(reference);
        if pending.contains(&key) {
            return Ok(None);
        }
        let existing = match prefetched {
            Some(cached) => cached?,
            None => self
                .db
                .get_cf(self.cf(CF_PAYLOAD_ARTIFACTS)?, &key)
                .map_err(storage_error)?
                .map(|encoded| encoded.to_vec()),
        };
        match existing {
            Some(existing) => {
                validate_complete_artifact(reference, &existing)?;
                self.read_complete_manifest(reference)?.ok_or_else(|| {
                    MutationError::Storage(
                        "complete payload bytes exist without their manifest".into(),
                    )
                })?;
                let state = self.blob_reference_state(reference)?.ok_or_else(|| {
                    MutationError::Storage(
                        "complete payload bytes exist without lifecycle authority".into(),
                    )
                })?;
                validate_blob_reference_state(state)?;
                if existing.as_slice() != bytes {
                    return Err(MutationError::Storage(
                        "inline payload content-address collision".into(),
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
        let state = self
            .blob_reference_state(reference)?
            .filter(|state| state.ref_count != 0)
            .ok_or(MutationError::BlobNotFound)?;
        validate_blob_reference_state(state)?;
        self.open_retained_blob(reference).await
    }

    pub(crate) async fn open_retained_blob(
        &self,
        reference: &BlobRef,
    ) -> Result<BlobReader, MutationError> {
        let state = self
            .blob_reference_state(reference)?
            .ok_or(MutationError::BlobNotFound)?;
        validate_blob_reference_state(state)?;
        let manifest = self
            .read_complete_manifest(reference)?
            .ok_or(MutationError::BlobNotFound)?;
        Ok(BlobReader::from_rocksdb(
            reference,
            RocksArtifactReader::new(self.db.clone(), manifest),
        ))
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

pub(super) fn upload_gc_due_key(upload_id: &[u8; 32], updated_at: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(BLOB_GC_DUE_FIXED_BYTES + upload_id.len());
    key.extend_from_slice(&blob_gc_due_prefix());
    key.extend_from_slice(&updated_at.to_be_bytes());
    key.push(BLOB_GC_UPLOAD_KIND);
    key.extend_from_slice(upload_id);
    key
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
        BLOB_GC_UPLOAD_KIND if identity.len() == UPLOAD_IDENTITY_BYTES => {}
        _ => {
            return Err(MutationError::Storage(
                "blob GC due identity is malformed".into(),
            ));
        }
    }
    Ok((updated_at, identity.to_vec()))
}
