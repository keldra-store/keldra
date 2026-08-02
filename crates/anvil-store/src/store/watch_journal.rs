use super::*;

impl Store {
    fn materialize_local_change(
        &self,
        stored: StoredLocalChange,
    ) -> Result<LocalChange, MutationError> {
        match stored {
            StoredLocalChange::Current(change) => Ok(change),
            StoredLocalChange::V050(invalidation) => {
                let tenant_id = self
                    .tenant_id_by_name(invalidation.key.tenant())?
                    .ok_or_else(|| {
                        MutationError::Storage(format!(
                            "0.5.0 local invalidation tenant `{}` has no stable identity mapping",
                            invalidation.key.tenant()
                        ))
                    })?;
                let bucket_id = self
                    .bucket_id_by_name(tenant_id, invalidation.key.bucket())?
                    .ok_or_else(|| {
                        MutationError::Storage(format!(
                            "0.5.0 local invalidation bucket `{}/{}` has no stable identity mapping",
                            invalidation.key.tenant(),
                            invalidation.key.bucket()
                        ))
                    })?;
                Ok(LocalChange::object_head(
                    invalidation.offset,
                    tenant_id.0,
                    bucket_id.0,
                    invalidation.key.path().to_owned(),
                    invalidation.minimum_path_version,
                    invalidation.state_hint == InvalidationStateHint::Deleted,
                ))
            }
        }
    }

    fn decode_local_change_record(&self, encoded: &[u8]) -> Result<LocalChange, MutationError> {
        let stored = decode_local_change(encoded).map_err(storage_error)?;
        self.materialize_local_change(stored)
    }

    fn watch_scope_identity(&self, scope: &WatchScope) -> Result<BucketIdentity, WatchError> {
        let tenant_id = self
            .tenant_id_by_name(scope.tenant())
            .map_err(|error| WatchError::Storage(error.to_string()))?
            .ok_or_else(|| WatchError::Storage("watch tenant identity is missing".into()))?;
        let bucket_id = self
            .bucket_id_by_name(tenant_id, scope.bucket())
            .map_err(|error| WatchError::Storage(error.to_string()))?
            .ok_or_else(|| WatchError::Storage("watch bucket identity is missing".into()))?;
        Ok(BucketIdentity {
            tenant_id,
            bucket_id,
        })
    }

    pub fn local_invalidation_offset(&self) -> Result<u64, MutationError> {
        let Some(encoded) = self
            .db
            .get_cf(self.cf(CF_METADATA)?, LOCAL_INVALIDATION_OFFSET_KEY)
            .map_err(storage_error)?
        else {
            return Ok(0);
        };
        decode_offset(&encoded)
    }

    pub fn local_watch_status(&self) -> Result<WatchJournalStatus, WatchError> {
        let metadata = self
            .cf(CF_METADATA)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let snapshot = self.db.snapshot();
        let read_counter = |key: &[u8]| {
            let encoded = snapshot
                .get_cf(metadata, key)
                .map_err(|error| WatchError::Storage(error.to_string()))?
                .ok_or_else(|| WatchError::Storage("local watch metadata is missing".into()))?;
            decode_watch_u64(&encoded)
        };
        let tail = read_counter(LOCAL_INVALIDATION_OFFSET_KEY)?;
        let retention_floor = read_counter(LOCAL_INVALIDATION_FLOOR_KEY)?;
        let retained_entries = read_counter(LOCAL_INVALIDATION_COUNT_KEY)?;
        let retained_bytes = read_counter(LOCAL_INVALIDATION_BYTES_KEY)?;
        if retention_floor > tail || retained_entries != tail - retention_floor {
            return Err(WatchError::Storage(
                "local invalidation retention metadata is inconsistent".into(),
            ));
        }
        Ok(WatchJournalStatus {
            source_id: SourceId {
                node_id: self.node_id,
                source_epoch: self.watch_source_epoch,
            },
            tail,
            retention_floor,
            retained_entries,
            retained_bytes,
        })
    }

    pub fn start_watch(
        &self,
        scope: &WatchScope,
        start: WatchStart,
    ) -> Result<WatchCursor, WatchError> {
        let status = self.local_watch_status()?;
        let cursor = match start {
            WatchStart::Now => WatchCursor::new(status.tail),
            WatchStart::RetainedBeginning => WatchCursor::new(status.retention_floor),
            WatchStart::Resume(token) => decode_resume_token(
                &token,
                scope,
                self.watch_source_epoch,
                &self.watch_token_key,
                self.watch_retention,
            )?,
        };
        if cursor.offset() < status.retention_floor || cursor.offset() > status.tail {
            return Err(WatchError::ResumeExpired);
        }
        Ok(cursor)
    }

    pub fn watch_checkpoint(
        &self,
        scope: &WatchScope,
        cursor: WatchCursor,
    ) -> Result<Vec<u8>, WatchError> {
        let status = self.local_watch_status()?;
        if cursor.offset() < status.retention_floor || cursor.offset() > status.tail {
            return Err(WatchError::ResumeExpired);
        }
        encode_resume_token(
            scope,
            cursor,
            self.watch_source_epoch,
            &self.watch_token_key,
            self.watch_retention,
        )
    }

    /// Scans a bounded number of retained source records, filtering only
    /// after each record has been represented in the returned checkpoint.
    /// This allows unrelated paths to advance a prefix-specific cursor without
    /// silently stepping over a matching invalidation.
    pub async fn scan_watch_page(
        &self,
        scope: &WatchScope,
        cursor: WatchCursor,
        limit: usize,
    ) -> Result<WatchPage, WatchError> {
        let _commit_guard = self.commit_lock.lock().await;
        let scope_identity = self.watch_scope_identity(scope)?;
        let status = self.local_watch_status()?;
        if cursor.offset() < status.retention_floor || cursor.offset() > status.tail {
            return Err(WatchError::ResumeExpired);
        }
        let limit = limit.min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        if limit == 0 || cursor.offset() == status.tail {
            return Ok(WatchPage {
                invalidations: Vec::new(),
                checkpoint: cursor,
            });
        }
        let through = cursor
            .offset()
            .saturating_add(limit as u64)
            .min(status.tail);
        let mut invalidations = Vec::new();
        let first_offset = cursor.offset() + 1;
        let first_key = invalidation_key(first_offset);
        let iterator = self.db.iterator_cf(
            self.cf(CF_LOCAL_INVALIDATIONS)
                .map_err(|error| WatchError::Storage(error.to_string()))?,
            IteratorMode::From(&first_key, Direction::Forward),
        );
        let expected_records = usize::try_from(through - first_offset + 1)
            .expect("watch page is bounded by a usize limit");
        let mut records_seen = 0_usize;
        for entry in iterator.take(limit) {
            let (key, encoded) = entry.map_err(|error| WatchError::Storage(error.to_string()))?;
            let offset = offset_from_key(&key)
                .ok_or_else(|| WatchError::Storage("local invalidation key is malformed".into()))?;
            let expected = first_offset + records_seen as u64;
            if offset != expected || offset > through {
                return Err(WatchError::Storage(format!(
                    "retained local invalidation offset {expected} is missing"
                )));
            }
            let stored = decode_local_change(&encoded)
                .map_err(|error| WatchError::Storage(error.to_string()))?;
            if stored.offset() != offset {
                return Err(WatchError::Storage(
                    "local change key does not match its stored offset".into(),
                ));
            }
            let change = self
                .materialize_local_change(stored)
                .map_err(|error| WatchError::Storage(error.to_string()))?;
            if let Some(head) = change.into_object_head()
                && head.tenant_id == scope_identity.tenant_id.0
                && head.bucket_id == scope_identity.bucket_id.0
            {
                let key = ObjectKey::new(scope.tenant(), scope.bucket(), head.exact_path)
                    .map_err(|error| WatchError::Storage(error.to_string()))?;
                if scope.contains(&key) && !contains_reserved_anvil_segment(key.path()) {
                    invalidations.push(LocalInvalidation {
                        offset: head.offset,
                        key,
                        minimum_path_version: head.path_version,
                        state_hint: match head.kind {
                            ObjectHeadChangeKind::Put => InvalidationStateHint::Present,
                            ObjectHeadChangeKind::Delete => InvalidationStateHint::Deleted,
                        },
                    });
                }
            }
            records_seen += 1;
        }
        if records_seen != expected_records {
            let missing = first_offset + records_seen as u64;
            return Err(WatchError::Storage(format!(
                "retained local invalidation offset {missing} is missing"
            )));
        }
        Ok(WatchPage {
            invalidations,
            checkpoint: WatchCursor::new(through),
        })
    }

    /// Waits until a scan after `cursor` may return a record or an expiry.
    /// Registering the notification before rereading the tail avoids a lost
    /// wake-up between the caller's empty scan and this wait.
    pub async fn wait_for_watch_change(&self, cursor: WatchCursor) -> Result<(), WatchError> {
        let mut notifications = self.watch_notify.subscribe();
        loop {
            let status = self.local_watch_status()?;
            if cursor.offset() < status.retention_floor || cursor.offset() > status.tail {
                return Err(WatchError::ResumeExpired);
            }
            if cursor.offset() < status.tail {
                return Ok(());
            }
            notifications
                .changed()
                .await
                .map_err(|_| WatchError::Storage("local watch notifier closed".into()))?;
        }
    }

    /// Reads one exact source-local change offset.
    pub fn read_local_change(&self, offset: u64) -> Result<Option<LocalChange>, MutationError> {
        if offset == 0 {
            return Ok(None);
        }
        let Some(encoded) = self
            .db
            .get_cf(self.cf(CF_LOCAL_INVALIDATIONS)?, invalidation_key(offset))
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let change = self.decode_local_change_record(&encoded)?;
        if change.offset() != offset {
            return Err(MutationError::Storage(
                "local change key does not match its stored offset".into(),
            ));
        }
        Ok(Some(change))
    }

    /// Scans source-local changes after one offset in ascending local order.
    /// The result is capped independently of the requested limit.
    pub fn scan_local_changes(
        &self,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LocalChange>, MutationError> {
        let status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        if after_offset < status.retention_floor {
            return Err(MutationError::Storage(format!(
                "local change cursor {after_offset} is below retention floor {}",
                status.retention_floor
            )));
        }
        if after_offset > status.tail {
            return Err(MutationError::Storage(format!(
                "local change cursor {after_offset} is beyond journal tail {}",
                status.tail
            )));
        }
        let limit = limit.min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        if limit == 0 || after_offset == status.tail {
            return Ok(Vec::new());
        }
        let first_offset = after_offset + 1;
        let through = after_offset.saturating_add(limit as u64).min(status.tail);
        let expected_records = usize::try_from(through - after_offset)
            .expect("local change scan is bounded by a usize limit");
        let first_key = invalidation_key(first_offset);
        let iterator = self.db.iterator_cf(
            self.cf(CF_LOCAL_INVALIDATIONS)?,
            IteratorMode::From(&first_key, Direction::Forward),
        );
        let mut changes = Vec::with_capacity(expected_records);
        for entry in iterator.take(expected_records) {
            let (key, encoded) = entry.map_err(storage_error)?;
            let stored_offset = offset_from_key(&key)
                .ok_or_else(|| MutationError::Storage("local change key is malformed".into()))?;
            let expected_offset = first_offset + changes.len() as u64;
            if stored_offset != expected_offset {
                return Err(MutationError::Storage(format!(
                    "local change offset {expected_offset} is missing"
                )));
            }
            let change = self.decode_local_change_record(&encoded)?;
            if change.offset() != stored_offset {
                return Err(MutationError::Storage(
                    "local change key does not match its stored offset".into(),
                ));
            }
            changes.push(change);
        }
        if changes.len() != expected_records {
            let missing = first_offset + changes.len() as u64;
            return Err(MutationError::Storage(format!(
                "local change offset {missing} is missing"
            )));
        }
        Ok(changes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legacy_invalidation() -> LocalInvalidation {
        LocalInvalidation::new(
            7,
            ObjectKey::new("tenant", "bucket", "documents/one").unwrap(),
            VersionId(41),
            false,
        )
    }

    #[tokio::test]
    async fn v050_names_are_resolved_through_existing_stable_identity_mappings() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let identity = store
            .install_test_bucket_identity("tenant", "bucket")
            .unwrap();
        let change = store
            .materialize_local_change(StoredLocalChange::V050(legacy_invalidation()))
            .unwrap()
            .into_object_head()
            .unwrap();

        assert_eq!(change.tenant_id, identity.tenant_id.0);
        assert_eq!(change.bucket_id, identity.bucket_id.0);
        assert_eq!(change.exact_path, "documents/one");
        assert_eq!(change.path_version, VersionId(41));
        assert_eq!(change.kind, ObjectHeadChangeKind::Put);
    }

    #[tokio::test]
    async fn v050_names_without_identity_mappings_fail_explicitly() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let error = store
            .materialize_local_change(StoredLocalChange::V050(legacy_invalidation()))
            .unwrap_err();
        assert!(error.to_string().contains("has no stable identity mapping"));
    }

    #[tokio::test]
    async fn local_change_scan_rejects_a_missing_middle_record() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        for index in 1..=3 {
            store
                .put(PutRequest {
                    key: ObjectKey::new("tenant", "bucket", format!("object-{index}")).unwrap(),
                    bytes: vec![index],
                    content_type: None,
                    mode: PutMode::PutIfAbsent,
                    command_id: Some(format!("put-{index}")),
                    durability: Durability::Local,
                })
                .await
                .unwrap();
        }
        store
            .db
            .delete_cf(
                store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
                invalidation_key(2),
            )
            .unwrap();

        let error = store.scan_local_changes(0, 10).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("local change offset 2 is missing")
        );
    }
}
