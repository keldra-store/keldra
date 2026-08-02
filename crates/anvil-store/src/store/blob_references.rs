use super::*;

impl Store {
    pub async fn stage_blob(&self, bytes: &[u8]) -> Result<BlobRef, MutationError> {
        if bytes.len() <= SMALL_BLOB_MAX_BYTES {
            let reference = blob_reference_for_bytes(bytes);
            let _commit_guard = self.commit_lock.lock().await;
            self.persist_small_blob_seal(&reference, bytes, now_unix_millis()?)?;
            return Ok(reference);
        }
        let mut upload = self.begin_blob_upload().await?;
        upload.write(bytes).await.map_err(storage_error)?;
        self.seal_blob_upload(upload).await
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
        // Hashing, fsync, rename and parent-directory fsync are byte-plane IO,
        // so complete them before taking the short metadata commit fence.
        let reference = upload.finish().await.map_err(storage_error)?;
        let now = now_unix_millis()?;
        if is_small_blob(&reference) {
            let bytes = self.blobs.get(&reference).await.map_err(storage_error)?;
            {
                let _commit_guard = self.commit_lock.lock().await;
                self.persist_small_blob_seal(&reference, &bytes, now)?;
            }
            // A crash before this cleanup leaves only a normal untracked copy,
            // which the existing age-gated orphan scan removes.
            self.blobs.remove(&reference).map_err(storage_error)?;
        } else {
            let _commit_guard = self.commit_lock.lock().await;
            // GC may have removed a stale deduplication target while finish was
            // outside the fence. Never recreate lifecycle state without bytes.
            if !self
                .blobs
                .contains(&reference)
                .await
                .map_err(storage_error)?
            {
                return Err(MutationError::BlobNotFound);
            }
            self.reserve_sealed_blob(&reference, now)?;
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

    /// Removes every unreferenced blob and every awaiting blob whose inactivity
    /// has reached the configured TTL. The full metadata column family is
    /// streamed without retaining a second in-memory index.
    pub async fn collect_blob_garbage(&self) -> Result<u64, MutationError> {
        let _commit_guard = self.commit_lock.lock().await;
        self.collect_blob_garbage_at(now_unix_millis()?)
    }

    pub(crate) fn collect_blob_garbage_at(
        &self,
        now_unix_millis: u64,
    ) -> Result<u64, MutationError> {
        let references = self.cf(CF_BLOB_REFERENCES)?;
        let mut removed = 0_u64;
        for entry in self.db.iterator_cf(references, IteratorMode::Start) {
            let (key, encoded) = entry.map_err(storage_error)?;
            let reference = blob_reference_from_key(&key)?;
            let state = decode_blob_reference_state(&encoded)?;
            if !blob_reference_is_garbage(state, now_unix_millis, self.awaiting_publish_ttl_millis)
            {
                continue;
            }

            let mut batch = WriteBatch::default();
            if is_small_blob(&reference) {
                batch.delete_cf(self.cf(CF_SMALL_BLOBS)?, &key);
            } else {
                self.blobs.remove(&reference).map_err(storage_error)?;
            }
            batch.delete_cf(references, &key);
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
            removed = removed
                .checked_add(1)
                .ok_or_else(|| MutationError::Storage("blob GC count is exhausted".into()))?;
        }
        removed
            .checked_add(self.collect_untracked_blob_files_at(now_unix_millis)?)
            .ok_or_else(|| MutationError::Storage("blob GC count is exhausted".into()))
    }

    fn collect_untracked_blob_files_at(&self, now_unix_millis: u64) -> Result<u64, MutationError> {
        let mut removed = 0_u64;
        for entry in std::fs::read_dir(self.blobs.root()).map_err(storage_error)? {
            let entry = entry.map_err(storage_error)?;
            let file_type = entry.file_type().map_err(storage_error)?;
            let name = entry.file_name();
            if name.as_os_str() == std::ffi::OsStr::new(".staging") {
                if !file_type.is_dir() {
                    return Err(MutationError::Storage(
                        "blob staging path is not a directory".into(),
                    ));
                }
                for staged in std::fs::read_dir(entry.path()).map_err(storage_error)? {
                    let staged = staged.map_err(storage_error)?;
                    if !staged.file_type().map_err(storage_error)?.is_file() {
                        return Err(MutationError::Storage(
                            "blob staging directory contains a non-file entry".into(),
                        ));
                    }
                    let modified = staged
                        .metadata()
                        .map_err(storage_error)?
                        .modified()
                        .map_err(storage_error)?
                        .duration_since(UNIX_EPOCH)
                        .map_err(storage_error)?
                        .as_millis() as u64;
                    if now_unix_millis.saturating_sub(modified) < self.awaiting_publish_ttl_millis {
                        continue;
                    }
                    remove_file_and_sync_parent(&staged.path())?;
                    removed = removed.checked_add(1).ok_or_else(|| {
                        MutationError::Storage("blob GC count is exhausted".into())
                    })?;
                }
                continue;
            }
            if !file_type.is_dir() {
                return Err(MutationError::Storage(
                    "blob root contains an unexpected non-directory entry".into(),
                ));
            }
            let shard = name.to_str().ok_or_else(|| {
                MutationError::Storage("blob shard directory name is not UTF-8".into())
            })?;
            if shard.len() != 2 || !shard.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(MutationError::Storage(
                    "blob shard directory name is malformed".into(),
                ));
            }
            for file in std::fs::read_dir(entry.path()).map_err(storage_error)? {
                let file = file.map_err(storage_error)?;
                if !file.file_type().map_err(storage_error)?.is_file() {
                    return Err(MutationError::Storage(
                        "blob shard directory contains a non-file entry".into(),
                    ));
                }
                let reference = blob_reference_from_file(&file, shard)?;
                let modified = file
                    .metadata()
                    .map_err(storage_error)?
                    .modified()
                    .map_err(storage_error)?
                    .duration_since(UNIX_EPOCH)
                    .map_err(storage_error)?
                    .as_millis() as u64;
                if now_unix_millis.saturating_sub(modified) < self.awaiting_publish_ttl_millis {
                    continue;
                }
                if !is_small_blob(&reference) && self.blob_reference_state(&reference)?.is_some() {
                    continue;
                }
                self.blobs.remove(&reference).map_err(storage_error)?;
                removed = removed
                    .checked_add(1)
                    .ok_or_else(|| MutationError::Storage("blob GC count is exhausted".into()))?;
            }
        }
        Ok(removed)
    }

    fn prepare_sealed_blob_reservation(
        &self,
        reference: &BlobRef,
        now_unix_millis: u64,
    ) -> Result<Option<BlobReferenceState>, MutationError> {
        let key = blob_reference_key(reference);
        let current = self.read_blob_reference_state(&key)?;
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

    pub(super) fn reserve_sealed_blob(
        &self,
        reference: &BlobRef,
        now_unix_millis: u64,
    ) -> Result<(), MutationError> {
        let Some(next) = self.prepare_sealed_blob_reservation(reference, now_unix_millis)? else {
            return Ok(());
        };
        let key = blob_reference_key(reference);
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(
                self.cf(CF_BLOB_REFERENCES)?,
                key,
                encode_blob_reference_state(next),
                &options,
            )
            .map_err(storage_error)
    }

    fn persist_small_blob_seal(
        &self,
        reference: &BlobRef,
        bytes: &[u8],
        now_unix_millis: u64,
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
            key,
            encode_blob_reference_state(state),
        );
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)
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

    fn read_blob_reference_state(
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
