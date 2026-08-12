use super::*;

const MUTATION_CAPACITY_RECHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

impl Store {
    /// Advances the highest contiguous source offset known durable at every
    /// consumer that currently constrains reference delivery. The value is
    /// monotonic for one running process and is reconstructed from destination
    /// cursors after restart rather than becoming another durable side plane.
    pub async fn advance_source_journal_reference_safe_through(
        &self,
        offset: u64,
    ) -> Result<(), MutationError> {
        let _commit_guard = self.commit_lock.lock().await;
        let status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let current = self
            .source_journal_reference_safe_through
            .load(std::sync::atomic::Ordering::Acquire);
        if offset < current {
            return Ok(());
        }
        if offset > status.tail {
            return Err(MutationError::Storage(format!(
                "source journal safe-through cursor {offset} is beyond tail {}",
                status.tail
            )));
        }
        self.source_journal_reference_safe_through
            .store(offset, std::sync::atomic::Ordering::Release);
        self.enforce_local_watch_retention()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        self.mutation_capacity_notify.notify_waiters();
        self.notify_local_invalidations();
        Ok(())
    }

    /// Advances the highest contiguous source offset whose metadata and
    /// atomic-program visibility are settled. This cut never controls journal
    /// pruning; reference-delivery safety has its own independent boundary.
    pub async fn advance_source_journal_settled_through(
        &self,
        offset: u64,
    ) -> Result<(), MutationError> {
        let _commit_guard = self.commit_lock.lock().await;
        let status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let current = status.settled_through;
        if offset < current {
            return Err(MutationError::Storage(format!(
                "source journal settled cursor regressed from {current} to {offset}"
            )));
        }
        if offset > status.tail {
            return Err(MutationError::Storage(format!(
                "source journal settled cursor {offset} is beyond tail {}",
                status.tail
            )));
        }
        if offset == current {
            return Ok(());
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(
                self.cf(CF_METADATA)?,
                LOCAL_INVALIDATION_SETTLED_KEY,
                offset.to_be_bytes(),
                &options,
            )
            .map_err(storage_error)?;
        self.mutation_capacity_notify.notify_waiters();
        self.notify_local_invalidations();
        Ok(())
    }

    /// Durably settles one quorum-proven source position when it is the next
    /// contiguous event. An already-settled position is an idempotent no-op;
    /// an out-of-order position is left for the recovery worker.
    pub async fn settle_source_journal_position_if_contiguous(
        &self,
        source: SourceId,
        offset: u64,
    ) -> Result<bool, MutationError> {
        self.settle_source_journal_positions_if_contiguous(source, &[offset])
            .await
            .map(|settled| settled.is_some())
    }

    /// Settle the longest quorum-proven contiguous prefix with one durable
    /// metadata write. Callers may supply already-settled or out-of-order
    /// positions; neither can advance across a missing proof.
    pub async fn settle_source_journal_positions_if_contiguous(
        &self,
        source: SourceId,
        offsets: &[u64],
    ) -> Result<Option<u64>, MutationError> {
        if offsets.is_empty() {
            return Ok(None);
        }
        let _commit_guard = self.commit_lock.lock().await;
        let status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        if source != status.source_id {
            return Err(MutationError::Storage(format!(
                "source journal identity {source:?} does not match local source {:?}",
                status.source_id
            )));
        }
        if offsets.iter().any(|offset| *offset > status.tail) {
            return Err(MutationError::Storage(format!(
                "source journal settled cursor is beyond tail {}",
                status.tail
            )));
        }
        let proven = offsets.iter().copied().collect::<BTreeSet<_>>();
        let mut through = status.settled_through;
        loop {
            let next = through.checked_add(1).ok_or_else(|| {
                MutationError::Storage("source journal settled cursor overflowed".into())
            })?;
            if !proven.contains(&next) {
                break;
            }
            through = next;
        }
        if through == status.settled_through {
            return Ok(None);
        }

        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(
                self.cf(CF_METADATA)?,
                LOCAL_INVALIDATION_SETTLED_KEY,
                through.to_be_bytes(),
                &options,
            )
            .map_err(storage_error)?;
        self.mutation_capacity_notify.notify_waiters();
        self.notify_local_invalidations();
        Ok(Some(through))
    }

    /// Reconstructs the volatile reference-safe cut after a one-node mutation.
    /// Its durable visibility cut was already staged in the same RocksDB batch
    /// as the journal entry; this helper performs no second durable write.
    pub(crate) fn settle_inline_source_changes(&self) -> Result<(), MutationError> {
        let status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let reference_safe = self
            .source_journal_reference_safe_through
            .load(std::sync::atomic::Ordering::Acquire);
        if reference_safe > status.tail {
            return Err(MutationError::Storage(format!(
                "source journal reference-safe cursor {reference_safe} is beyond tail {}",
                status.tail,
            )));
        }
        self.source_journal_reference_safe_through
            .store(status.tail, std::sync::atomic::Ordering::Release);
        self.mutation_capacity_notify.notify_waiters();
        Ok(())
    }

    /// Waits without holding a storage lock until journal or receipt capacity
    /// may have changed. Receipt expiry has no dedicated background task, so a
    /// bounded timer ensures a blocked writer eventually retries and prunes it.
    pub async fn wait_for_mutation_capacity(&self) {
        match self.prune_expired_receipts_for_capacity().await {
            Ok(true) => return,
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(%error, "bounded mutation-capacity maintenance will retry");
            }
        }
        let notified = self.mutation_capacity_notify.notified();
        tokio::select! {
            () = notified => {}
            () = tokio::time::sleep(MUTATION_CAPACITY_RECHECK_INTERVAL) => {}
        }
    }

    /// Waits without holding the commit lock until the proof-backed source cut
    /// reaches the physical tail, then acquires the lock and rechecks. The
    /// returned guard prevents a new head/journal batch from entering before a
    /// snapshot worker captures its RocksDB snapshot.
    pub(crate) async fn lock_settled_source_snapshot(
        &self,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, WatchError> {
        loop {
            let mut notifications = self.watch_notify.subscribe();
            let status = self.local_watch_status()?;
            if status.settled_through != status.tail {
                notifications.changed().await.map_err(|_| {
                    WatchError::Storage("local source settlement notifier closed".into())
                })?;
                continue;
            }
            let guard = self.commit_lock.clone().lock_owned().await;
            let status = self.local_watch_status()?;
            if status.settled_through == status.tail {
                return Ok(guard);
            }
            drop(guard);
        }
    }

    pub(crate) fn decode_local_change_record(
        &self,
        encoded: &[u8],
    ) -> Result<LocalChange, MutationError> {
        decode_local_change(encoded).map_err(storage_error)
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
        let snapshot = self.db.snapshot();
        self.local_watch_status_at(&snapshot)
    }

    pub(crate) fn local_watch_status_at(
        &self,
        snapshot: &rocksdb::SnapshotWithThreadMode<'_, rocksdb::DB>,
    ) -> Result<WatchJournalStatus, WatchError> {
        let metadata = self
            .cf(CF_METADATA)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let read_counter = |key: &[u8]| {
            let encoded = snapshot
                .get_cf(metadata, key)
                .map_err(|error| WatchError::Storage(error.to_string()))?
                .ok_or_else(|| WatchError::Storage("local watch metadata is missing".into()))?;
            decode_watch_u64(&encoded)
        };
        let tail = read_counter(LOCAL_INVALIDATION_OFFSET_KEY)?;
        let settled_through = read_counter(LOCAL_INVALIDATION_SETTLED_KEY)?;
        let retention_floor = read_counter(LOCAL_INVALIDATION_FLOOR_KEY)?;
        let retained_entries = read_counter(LOCAL_INVALIDATION_COUNT_KEY)?;
        let retained_bytes = read_counter(LOCAL_INVALIDATION_BYTES_KEY)?;
        if retention_floor > settled_through
            || settled_through > tail
            || retained_entries != tail - retention_floor
        {
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
            settled_through,
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
            WatchStart::Now => WatchCursor::new(status.settled_through),
            WatchStart::RetainedBeginning => WatchCursor::new(status.retention_floor),
            WatchStart::Resume(token) => decode_resume_token(
                &token,
                scope,
                self.watch_source_epoch,
                &self.watch_token_key,
                self.watch_retention,
            )?,
        };
        if cursor.offset() < status.retention_floor || cursor.offset() > status.settled_through {
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
        if cursor.offset() < status.retention_floor || cursor.offset() > status.settled_through {
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
        if cursor.offset() < status.retention_floor || cursor.offset() > status.settled_through {
            return Err(WatchError::ResumeExpired);
        }
        let limit = limit.min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        if limit == 0 || cursor.offset() == status.settled_through {
            return Ok(WatchPage {
                invalidations: Vec::new(),
                checkpoint: cursor,
            });
        }
        let through = cursor
            .offset()
            .saturating_add(limit as u64)
            .min(status.settled_through);
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
            let change = decode_local_change(&encoded)
                .map_err(|error| WatchError::Storage(error.to_string()))?;
            if change.offset() != offset {
                return Err(WatchError::Storage(
                    "local change key does not match its stored offset".into(),
                ));
            }
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
            if cursor.offset() < status.retention_floor || cursor.offset() > status.settled_through
            {
                return Err(WatchError::ResumeExpired);
            }
            if cursor.offset() < status.settled_through {
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
        Ok(self
            .scan_local_changes_bounded(after_offset, limit, u64::MAX)?
            .changes)
    }

    /// Scans source-local changes without retaining a page larger than the
    /// caller's encoded-byte allowance.
    ///
    /// A nonempty prefix is returned when the following record would exceed
    /// `max_bytes`. If the first unread record alone exceeds the bound, its
    /// offset and required bytes are reported without returning or advancing
    /// past that record.
    pub fn scan_local_changes_bounded(
        &self,
        after_offset: u64,
        limit: usize,
        max_bytes: u64,
    ) -> Result<LocalChangePage, MutationError> {
        if max_bytes == 0 {
            return Err(MutationError::Storage(
                "local change scan byte limit must be positive".into(),
            ));
        }
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
            return Ok(LocalChangePage {
                source_id: status.source_id,
                changes: Vec::new(),
                encoded_bytes: 0,
                oversize: None,
            });
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
        let mut encoded_bytes = 0_u64;
        let mut stopped_at_byte_limit = false;
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
            let change_bytes = encoded_change_len(&change)?;
            let projected = encoded_bytes.checked_add(change_bytes).ok_or_else(|| {
                MutationError::Storage("local change page length overflow".into())
            })?;
            if projected > max_bytes && changes.is_empty() {
                return Ok(LocalChangePage {
                    source_id: status.source_id,
                    changes,
                    encoded_bytes: 0,
                    oversize: Some(OversizeLocalChange {
                        offset: stored_offset,
                        encoded_bytes: change_bytes,
                    }),
                });
            }
            if projected > max_bytes {
                stopped_at_byte_limit = true;
                break;
            }
            encoded_bytes = projected;
            changes.push(change);
        }
        if !stopped_at_byte_limit && changes.len() != expected_records {
            let missing = first_offset + changes.len() as u64;
            return Err(MutationError::Storage(format!(
                "local change offset {missing} is missing"
            )));
        }
        Ok(LocalChangePage {
            source_id: status.source_id,
            changes,
            encoded_bytes,
            oversize: None,
        })
    }
}

pub(crate) fn encoded_change_len(change: &LocalChange) -> Result<u64, MutationError> {
    let mut counter = ChangeByteCounter(0);
    serde_json::to_writer(&mut counter, change)
        .map_err(|error| MutationError::Storage(format!("encode local change: {error}")))?;
    Ok(counter.0)
}

struct ChangeByteCounter(u64);

impl std::io::Write for ChangeByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("local change length overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_rejects_a_durable_settled_cursor_beyond_the_raw_tail() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        store
            .db
            .put_cf(
                store.cf(CF_METADATA).unwrap(),
                LOCAL_INVALIDATION_SETTLED_KEY,
                1_u64.to_be_bytes(),
            )
            .unwrap();
        drop(store);

        let error = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("settled cursor 1"));
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

    #[tokio::test]
    async fn bounded_local_change_scan_stops_before_the_next_record() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        for index in 1..=2 {
            store
                .put(PutRequest {
                    key: ObjectKey::new("tenant", "bucket", format!("object-{index}")).unwrap(),
                    bytes: vec![index],
                    content_type: None,
                    mode: PutMode::PutIfAbsent,
                    command_id: Some(format!("bounded-put-{index}")),
                    durability: Durability::Local,
                })
                .await
                .unwrap();
        }
        let first = store.read_local_change(1).unwrap().unwrap();
        let first_bytes = encoded_change_len(&first).unwrap();

        let page = store.scan_local_changes_bounded(0, 2, first_bytes).unwrap();
        assert_eq!(page.changes, vec![first]);
        assert_eq!(page.encoded_bytes, first_bytes);
        assert_eq!(page.oversize, None);

        let second = store.scan_local_changes_bounded(1, 2, first_bytes).unwrap();
        assert_eq!(second.changes.len(), 1);
        assert_eq!(second.changes[0].offset(), 2);
    }

    #[tokio::test]
    async fn bounded_local_change_scan_reports_one_oversize_record_without_returning_it() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        store
            .put(PutRequest {
                key: ObjectKey::new("tenant", "bucket", "oversize").unwrap(),
                bytes: vec![1],
                content_type: None,
                mode: PutMode::PutIfAbsent,
                command_id: Some("oversize-put".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        let first = store.read_local_change(1).unwrap().unwrap();
        let first_bytes = encoded_change_len(&first).unwrap();

        let page = store
            .scan_local_changes_bounded(0, 1, first_bytes - 1)
            .unwrap();
        assert!(page.changes.is_empty());
        assert_eq!(page.encoded_bytes, 0);
        assert_eq!(
            page.oversize,
            Some(OversizeLocalChange {
                offset: 1,
                encoded_bytes: first_bytes,
            })
        );
    }

    #[tokio::test]
    async fn object_changes_retain_exact_reference_effects() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        for (command, bytes) in [("first", b"first".as_slice()), ("second", b"second")] {
            store
                .put(PutRequest {
                    key: ObjectKey::new("tenant", "bucket", "object").unwrap(),
                    bytes: bytes.to_vec(),
                    content_type: None,
                    mode: PutMode::Put,
                    command_id: Some(command.into()),
                    durability: Durability::Local,
                })
                .await
                .unwrap();
        }

        let changes = store.scan_local_changes(0, 10).unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(
            changes[0].reference_deltas(),
            &[ReferenceDelta {
                blob: blob_reference_for_bytes(b"first"),
                change: 1,
            }]
        );
        assert_eq!(
            changes[1].reference_deltas(),
            &[
                ReferenceDelta {
                    blob: blob_reference_for_bytes(b"first"),
                    change: -1,
                },
                ReferenceDelta {
                    blob: blob_reference_for_bytes(b"second"),
                    change: 1,
                },
            ]
        );
    }
}
