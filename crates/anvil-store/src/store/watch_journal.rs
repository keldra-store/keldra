use super::*;

impl Store {
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
            source_epoch: self.watch_source_epoch,
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
            let invalidation = serde_json::from_slice::<LocalInvalidation>(&encoded)
                .map_err(|error| WatchError::Storage(error.to_string()))?;
            if invalidation.offset != offset {
                return Err(WatchError::Storage(
                    "local invalidation key does not match its stored offset".into(),
                ));
            }
            if scope.contains(&invalidation.key) {
                invalidations.push(invalidation);
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

    /// Reads one exact source-local invalidation offset.
    pub fn read_local_invalidation(
        &self,
        offset: u64,
    ) -> Result<Option<LocalInvalidation>, MutationError> {
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
        let invalidation =
            serde_json::from_slice::<LocalInvalidation>(&encoded).map_err(storage_error)?;
        if invalidation.offset != offset {
            return Err(MutationError::Storage(
                "local invalidation key does not match its stored offset".into(),
            ));
        }
        Ok(Some(invalidation))
    }

    /// Scans source-local invalidations after one offset in ascending local
    /// order. The result is capped independently of the requested limit.
    pub fn scan_local_invalidations(
        &self,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LocalInvalidation>, MutationError> {
        let limit = limit.min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        let Some(first_offset) = after_offset.checked_add(1).filter(|_| limit > 0) else {
            return Ok(Vec::new());
        };
        let first_key = invalidation_key(first_offset);
        let iterator = self.db.iterator_cf(
            self.cf(CF_LOCAL_INVALIDATIONS)?,
            IteratorMode::From(&first_key, Direction::Forward),
        );
        let mut invalidations = Vec::with_capacity(limit);
        for entry in iterator.take(limit) {
            let (key, encoded) = entry.map_err(storage_error)?;
            let stored_offset = offset_from_key(&key).ok_or_else(|| {
                MutationError::Storage("local invalidation key is malformed".into())
            })?;
            let invalidation =
                serde_json::from_slice::<LocalInvalidation>(&encoded).map_err(storage_error)?;
            if invalidation.offset != stored_offset {
                return Err(MutationError::Storage(
                    "local invalidation key does not match its stored offset".into(),
                ));
            }
            invalidations.push(invalidation);
        }
        Ok(invalidations)
    }
}
