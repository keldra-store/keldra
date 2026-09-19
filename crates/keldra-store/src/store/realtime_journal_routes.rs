//! Durable sparse routing for explicitly requested real-time index work.
//!
//! Values are deliberately empty: the authoritative event remains in the
//! source journal. A marker and its event are written by the same RocksDB
//! batch, so recovery can always replay selected work by resolving the marker
//! back to the source event.

use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};

use super::journal_routes::{
    project_change_for_route, route_matches, try_visit_routes_for_change, validate_route,
};
use super::{CF_LOCAL_INVALIDATIONS, CF_REALTIME_JOURNAL_ROUTES, Store, storage_error};
use crate::journal_route::{JournalRoute, RoutedJournalError, RoutedLocalChangePage};
use crate::key::STORAGE_KEY_FORMAT_VERSION;
use crate::watch::{
    LocalChange, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, OversizeLocalChange, SourceId,
    encoded_change_len, invalidation_key,
};

const REALTIME_BUCKET_ROUTE_DOMAIN: u8 = b'R';
const REALTIME_BUCKET_ROUTE_KEY_BYTES: usize = 1 + 1 + 8 + 8 + 32 + 8;
const REALTIME_BUCKET_ROUTE_PREFIX_BYTES: usize = REALTIME_BUCKET_ROUTE_KEY_BYTES - 8;
const MAX_REALTIME_ROUTE_REMOVALS: usize = 1_024;
pub const MAX_PENDING_REALTIME_BUCKET_ROUTES: usize = 1_024;

pub(super) fn realtime_journal_route_logical_bytes(
    change: &LocalChange,
) -> Result<u64, crate::MutationError> {
    let mut bytes = 0_u64;
    try_visit_routes_for_change(change, |route| {
        if matches!(route, JournalRoute::Bucket { .. }) {
            bytes = bytes.saturating_add(
                (REALTIME_BUCKET_ROUTE_PREFIX_BYTES + REALTIME_BUCKET_ROUTE_KEY_BYTES) as u64,
            );
        }
        Ok::<(), crate::MutationError>(())
    })?;
    Ok(bytes)
}

impl Store {
    pub(super) fn rebuild_realtime_journal_routes(&self) -> Result<u64, crate::MutationError> {
        let status = self
            .local_watch_status()
            .map_err(|error| crate::MutationError::Storage(error.to_string()))?;
        let journal = self.cf(CF_LOCAL_INVALIDATIONS)?;
        let start = invalidation_key(status.retention_floor.saturating_add(1));
        let mut batch = WriteBatch::default();
        let mut staged = 0_usize;
        let mut rebuilt = 0_u64;
        let route_cf = self.cf(CF_REALTIME_JOURNAL_ROUTES)?;
        for item in self.db.iterator_cf(route_cf, IteratorMode::Start) {
            let (key, _) = item.map_err(storage_error)?;
            if key.len() == REALTIME_BUCKET_ROUTE_PREFIX_BYTES
                || key.len() == REALTIME_BUCKET_ROUTE_KEY_BYTES
            {
                let (_, epoch) =
                    realtime_route_from_prefix(&key[..REALTIME_BUCKET_ROUTE_PREFIX_BYTES])
                        .map_err(realtime_mutation_error)?;
                if epoch != status.source_id.source_epoch {
                    batch.delete_cf(route_cf, key);
                    staged += 1;
                    if staged == MAX_REALTIME_ROUTE_REMOVALS {
                        let mut options = WriteOptions::default();
                        options.set_sync(self.sync_writes);
                        self.db
                            .write_opt(std::mem::take(&mut batch), &options)
                            .map_err(storage_error)?;
                        staged = 0;
                    }
                }
            }
        }
        for item in self
            .db
            .iterator_cf(journal, IteratorMode::From(&start, Direction::Forward))
        {
            let (key, encoded) = item.map_err(storage_error)?;
            // Reference proofs share this column family but use a longer,
            // namespaced key. Only fixed-width source-journal offsets contain
            // local-change envelopes.
            let Ok(offset_bytes) = <[u8; std::mem::size_of::<u64>()]>::try_from(key.as_ref())
            else {
                continue;
            };
            if u64::from_be_bytes(offset_bytes) > status.tail {
                break;
            }
            let decoded = self.decode_local_change_record_with_length(&encoded)?;
            if decoded.indexing_intent == crate::IndexingIntent::Realtime {
                self.stage_realtime_journal_routes(
                    &mut batch,
                    status.source_id.source_epoch,
                    &decoded.change,
                )?;
                staged += 1;
                rebuilt = rebuilt.saturating_add(1);
            }
            if staged >= MAX_REALTIME_ROUTE_REMOVALS {
                let mut options = WriteOptions::default();
                options.set_sync(self.sync_writes);
                self.db
                    .write_opt(std::mem::take(&mut batch), &options)
                    .map_err(storage_error)?;
                staged = 0;
            }
        }
        if staged != 0 {
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
        }
        Ok(rebuilt)
    }

    pub(super) fn realtime_journal_routes_pending(
        &self,
        source_epoch: [u8; 32],
        change: &LocalChange,
    ) -> Result<bool, crate::MutationError> {
        let cf = self.cf(CF_REALTIME_JOURNAL_ROUTES)?;
        let mut pending = false;
        try_visit_routes_for_change(change, |route| {
            let JournalRoute::Bucket { .. } = route else {
                return Ok::<(), crate::MutationError>(());
            };
            let key = realtime_route_key(route, source_epoch, change.offset())
                .map_err(realtime_mutation_error)?;
            pending |= self.db.get_cf(cf, key).map_err(storage_error)?.is_some();
            Ok(())
        })?;
        Ok(pending)
    }

    pub(super) fn stage_realtime_journal_routes(
        &self,
        batch: &mut WriteBatch,
        source_epoch: [u8; 32],
        change: &LocalChange,
    ) -> Result<(), crate::MutationError> {
        let cf = self.cf(CF_REALTIME_JOURNAL_ROUTES)?;
        try_visit_routes_for_change(change, |route| {
            // Search projections are bucket scoped. Definition-only routes do
            // not name a physical index partition and remain on their normal
            // projection path.
            let JournalRoute::Bucket { .. } = route else {
                return Ok(());
            };
            let prefix =
                realtime_route_prefix(route, source_epoch).map_err(realtime_mutation_error)?;
            // The empty prefix key is a disposable route-discovery marker.
            // It lets consumers find selected buckets even when no physical
            // index family is currently active for that bucket.
            batch.put_cf(cf, &prefix, []);
            batch.put_cf(
                cf,
                realtime_route_key_from_prefix(prefix, change.offset())
                    .map_err(realtime_mutation_error)?,
                [],
            );
            Ok(())
        })
    }

    pub(super) fn stage_realtime_replay_event(
        &self,
        batch: &mut WriteBatch,
        source_id: SourceId,
        expected: &LocalChange,
    ) -> Result<(), crate::MutationError> {
        let journal = self.cf(CF_LOCAL_INVALIDATIONS)?;
        let key = invalidation_key(expected.offset());
        let encoded = self
            .db
            .get_cf(journal, &key)
            .map_err(storage_error)?
            .ok_or_else(|| {
                crate::MutationError::Storage(
                    "real-time replay source event is no longer retained".into(),
                )
            })?;
        let decoded = self.decode_local_change_record_with_length(&encoded)?;
        if !realtime_replay_identity_matches(&decoded.change, expected) {
            return Err(crate::MutationError::Storage(
                "real-time replay source event no longer matches its retained receipt".into(),
            ));
        }
        if decoded.indexing_intent != crate::IndexingIntent::Realtime {
            return Err(crate::MutationError::Storage(
                "real-time replay receipt disagrees with authoritative source intent".into(),
            ));
        }
        self.stage_realtime_journal_routes(batch, source_id.source_epoch, expected)
    }

    /// Replays selected real-time work from its authoritative source events.
    ///
    /// `through_offset` advances across sparse gaps just like the ordinary
    /// routed journal. Markers are retained until explicit base-coverage
    /// acknowledgement or ordinary source-journal retention removes them.
    pub fn scan_realtime_routed_local_changes(
        &self,
        route: JournalRoute,
        source_id: SourceId,
        after_offset: u64,
        target_offset: u64,
        limit: usize,
        max_bytes: u64,
    ) -> Result<RoutedLocalChangePage, RoutedJournalError> {
        if limit == 0 || limit > MAX_LOCAL_INVALIDATION_SCAN_RECORDS || max_bytes == 0 {
            return Err(RoutedJournalError::InvalidLimits);
        }
        validate_realtime_route(route)?;
        let snapshot = self.db.snapshot();
        let status = self
            .local_watch_status_at(&snapshot)
            .map_err(realtime_storage)?;
        validate_source_range(status, source_id, after_offset, target_offset)?;
        if target_offset > status.settled_through {
            return Err(RoutedJournalError::TargetFuture {
                target: target_offset,
                tail: status.settled_through,
            });
        }
        if after_offset == target_offset {
            return Ok(RoutedLocalChangePage {
                source_id,
                changes: Vec::new(),
                encoded_bytes: 0,
                through_offset: after_offset,
                oversize: None,
            });
        }

        let prefix = realtime_route_prefix(route, source_id.source_epoch)?;
        let start = realtime_route_key(
            route,
            source_id.source_epoch,
            after_offset.saturating_add(1),
        )?;
        let marker_cf = self
            .cf(CF_REALTIME_JOURNAL_ROUTES)
            .map_err(realtime_storage)?;
        let journal_cf = self.cf(CF_LOCAL_INVALIDATIONS).map_err(realtime_storage)?;
        let mut iterator =
            snapshot.iterator_cf(marker_cf, IteratorMode::From(&start, Direction::Forward));
        let mut offsets = Vec::with_capacity(limit);
        let mut complete_to_target = false;
        loop {
            let Some(item) = iterator.next() else {
                complete_to_target = true;
                break;
            };
            let (key, value) = item.map_err(realtime_storage)?;
            if !key.starts_with(&prefix) {
                complete_to_target = true;
                break;
            }
            if !value.is_empty() {
                return Err(RoutedJournalError::Storage(
                    "real-time journal route value must be empty".into(),
                ));
            }
            let offset = realtime_route_offset(&key)?;
            if offset <= after_offset {
                continue;
            }
            if offset > target_offset {
                complete_to_target = true;
                break;
            }
            if offsets.len() == limit {
                break;
            }
            offsets.push(offset);
        }
        drop(iterator);

        let records = snapshot.multi_get_cf(
            offsets
                .iter()
                .copied()
                .map(|offset| (journal_cf, invalidation_key(offset))),
        );
        if records.len() != offsets.len() {
            return Err(RoutedJournalError::Storage(
                "real-time journal multi-get returned the wrong result count".into(),
            ));
        }

        let marker_count = offsets.len();
        let mut changes = Vec::with_capacity(marker_count);
        let mut encoded_bytes = 0_u64;
        let mut through_offset = after_offset;
        let mut stopped_at_byte_limit = false;
        for (offset, encoded) in offsets.into_iter().zip(records) {
            let encoded = encoded
                .map_err(realtime_storage)?
                .ok_or(RoutedJournalError::MissingPrimary { offset })?;
            let decoded = self
                .decode_local_change_record_with_length(&encoded)
                .map_err(realtime_storage)?;
            if decoded.change.offset() != offset || !route_matches(route, &decoded.change) {
                return Err(RoutedJournalError::RouteMismatch { offset });
            }
            let route_projected = matches!(
                decoded.change,
                LocalChange::AtomicBatchPublished(_) | LocalChange::ContentLifecycleBatchChanged(_)
            );
            let peer_encoded_bytes = decoded.peer_encoded_bytes;
            let change = project_change_for_route(route, decoded.change)?;
            let change_bytes = if route_projected {
                encoded_change_len(&change).map_err(realtime_storage)?
            } else {
                peer_encoded_bytes
            };
            let projected = encoded_bytes.checked_add(change_bytes).ok_or_else(|| {
                RoutedJournalError::Storage("real-time journal page length overflow".into())
            })?;
            if projected > max_bytes {
                if changes.is_empty() {
                    return Ok(RoutedLocalChangePage {
                        source_id,
                        changes,
                        encoded_bytes: 0,
                        through_offset: after_offset,
                        oversize: Some(OversizeLocalChange {
                            offset,
                            encoded_bytes: change_bytes,
                        }),
                    });
                }
                stopped_at_byte_limit = true;
                break;
            }
            encoded_bytes = projected;
            through_offset = offset;
            changes.push(change);
        }
        if complete_to_target && !stopped_at_byte_limit && changes.len() == marker_count {
            through_offset = target_offset;
        }
        Ok(RoutedLocalChangePage {
            source_id,
            changes,
            encoded_bytes,
            through_offset,
            oversize: None,
        })
    }

    /// Removes a bounded page of real-time markers proven absorbed by the base
    /// projection. The caller supplies the base projection's durable source
    /// coverage; no marker is removed automatically on overlay publication.
    pub fn acknowledge_realtime_base_coverage(
        &self,
        route: JournalRoute,
        source_id: SourceId,
        covered_through: u64,
    ) -> Result<usize, RoutedJournalError> {
        validate_realtime_route(route)?;
        let status = self.local_watch_status().map_err(realtime_storage)?;
        validate_source_range(status, source_id, status.retention_floor, covered_through)?;
        if covered_through > status.settled_through {
            return Err(RoutedJournalError::TargetFuture {
                target: covered_through,
                tail: status.settled_through,
            });
        }
        let prefix = realtime_route_prefix(route, source_id.source_epoch)?;
        let first = realtime_route_key(
            route,
            source_id.source_epoch,
            status.retention_floor.saturating_add(1),
        )?;
        let cf = self
            .cf(CF_REALTIME_JOURNAL_ROUTES)
            .map_err(realtime_storage)?;
        let mut batch = WriteBatch::default();
        let mut removed = 0_usize;
        for item in self
            .db
            .iterator_cf(cf, IteratorMode::From(&first, Direction::Forward))
        {
            let (key, value) = item.map_err(realtime_storage)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if !value.is_empty() {
                return Err(RoutedJournalError::Storage(
                    "real-time journal route value must be empty".into(),
                ));
            }
            if realtime_route_offset(&key)? > covered_through {
                break;
            }
            batch.delete_cf(cf, key);
            removed += 1;
            if removed == MAX_REALTIME_ROUTE_REMOVALS {
                break;
            }
        }
        if removed != 0 {
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(|error| {
                let error = storage_error(error);
                RoutedJournalError::Storage(error.to_string())
            })?;
            self.mutation_capacity_notify.notify_waiters();
            self.watch_notify.send_replace(());
        }
        Ok(removed)
    }

    /// Enumerates distinct bucket routes touched by real-time work in the
    /// current local source epoch. A route can currently have no pending
    /// offset markers; callers scan it to distinguish empty from pending.
    /// `after_route` is an exclusive route cursor.
    pub fn realtime_bucket_routes(
        &self,
        source_id: SourceId,
        after_route: Option<JournalRoute>,
        limit: usize,
    ) -> Result<Vec<JournalRoute>, RoutedJournalError> {
        if limit == 0 || limit > MAX_PENDING_REALTIME_BUCKET_ROUTES {
            return Err(RoutedJournalError::InvalidLimits);
        }
        if let Some(route) = after_route {
            validate_realtime_route(route)?;
        }
        let status = self.local_watch_status().map_err(realtime_storage)?;
        validate_source_range(
            status,
            source_id,
            status.retention_floor,
            status.retention_floor,
        )?;
        let cf = self
            .cf(CF_REALTIME_JOURNAL_ROUTES)
            .map_err(realtime_storage)?;
        let start = after_route.map_or_else(
            || {
                Ok::<Vec<u8>, RoutedJournalError>(vec![
                    STORAGE_KEY_FORMAT_VERSION,
                    REALTIME_BUCKET_ROUTE_DOMAIN,
                ])
            },
            |route| realtime_route_prefix(route, source_id.source_epoch),
        )?;
        let after = after_route.map(route_order_key);
        let mut routes = Vec::with_capacity(limit);
        for item in self
            .db
            .iterator_cf(cf, IteratorMode::From(&start, Direction::Forward))
        {
            let (key, value) = item.map_err(realtime_storage)?;
            if !key.starts_with(&[STORAGE_KEY_FORMAT_VERSION, REALTIME_BUCKET_ROUTE_DOMAIN]) {
                break;
            }
            if !value.is_empty() {
                return Err(RoutedJournalError::Storage(
                    "real-time journal route value must be empty".into(),
                ));
            }
            if key.len() != REALTIME_BUCKET_ROUTE_PREFIX_BYTES {
                continue;
            }
            let (route, epoch) = realtime_route_from_prefix(&key)?;
            if epoch != source_id.source_epoch
                || after.is_some_and(|cursor| route_order_key(route) <= cursor)
            {
                continue;
            }
            routes.push(route);
            if routes.len() == limit {
                break;
            }
        }
        Ok(routes)
    }

    /// Wakes when local journal state changes. Consumers must re-read pending
    /// routes after every wake; notifications are deliberately coalesced.
    pub fn subscribe_realtime_journal_changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.realtime_journal_notify.subscribe()
    }

    pub(super) fn stage_realtime_journal_route_removal(
        &self,
        batch: &mut WriteBatch,
        source_epoch: [u8; 32],
        change: &LocalChange,
    ) -> Result<(), crate::MutationError> {
        let cf = self.cf(CF_REALTIME_JOURNAL_ROUTES)?;
        try_visit_routes_for_change(change, |route| {
            let JournalRoute::Bucket { .. } = route else {
                return Ok(());
            };
            batch.delete_cf(
                cf,
                realtime_route_key(route, source_epoch, change.offset())
                    .map_err(realtime_mutation_error)?,
            );
            Ok(())
        })
    }
}

fn validate_source_range(
    status: crate::WatchJournalStatus,
    source_id: SourceId,
    after_offset: u64,
    target_offset: u64,
) -> Result<(), RoutedJournalError> {
    if source_id.node_id != status.source_id.node_id {
        return Err(RoutedJournalError::SourceNodeMismatch);
    }
    if source_id.source_epoch != status.source_id.source_epoch {
        return Err(RoutedJournalError::SourceEpochMismatch);
    }
    if after_offset < status.retention_floor {
        return Err(RoutedJournalError::CursorExpired {
            cursor: after_offset,
            retention_floor: status.retention_floor,
        });
    }
    if after_offset > status.tail {
        return Err(RoutedJournalError::CursorFuture {
            cursor: after_offset,
            tail: status.tail,
        });
    }
    if target_offset < after_offset {
        return Err(RoutedJournalError::TargetBeforeCursor {
            cursor: after_offset,
            target: target_offset,
        });
    }
    if target_offset > status.tail {
        return Err(RoutedJournalError::TargetFuture {
            target: target_offset,
            tail: status.tail,
        });
    }
    Ok(())
}

fn realtime_replay_identity_matches(retained: &LocalChange, expected: &LocalChange) -> bool {
    match (retained, expected) {
        (LocalChange::ObjectHead(retained), LocalChange::ObjectHead(expected)) => {
            retained.offset == expected.offset
                && retained.tenant_id == expected.tenant_id
                && retained.bucket_id == expected.bucket_id
                && retained.exact_path == expected.exact_path
                && retained.canonical_path == expected.canonical_path
                && retained.path_version == expected.path_version
                && retained.kind == expected.kind
                && retained.program_commit_cursor == expected.program_commit_cursor
        }
        (
            LocalChange::RetainedVersionDeleted(retained),
            LocalChange::RetainedVersionDeleted(expected),
        ) => {
            retained.offset == expected.offset
                && retained.tenant_id == expected.tenant_id
                && retained.bucket_id == expected.bucket_id
                && retained.exact_path == expected.exact_path
                && retained.canonical_path == expected.canonical_path
                && retained.deleted_version == expected.deleted_version
                && retained.resulting_head_version == expected.resulting_head_version
        }
        // Atomic publication, aggregate, and content-lifecycle records do not
        // have replay-only accounting/reference fields. Preserve their exact
        // equality contract rather than weakening their unit identity.
        _ => retained == expected,
    }
}

fn validate_realtime_route(route: JournalRoute) -> Result<(), RoutedJournalError> {
    validate_route(route)?;
    if !matches!(route, JournalRoute::Bucket { .. }) {
        return Err(RoutedJournalError::Storage(
            "real-time index routes must name a bucket".into(),
        ));
    }
    Ok(())
}

fn realtime_route_prefix(
    route: JournalRoute,
    source_epoch: [u8; 32],
) -> Result<Vec<u8>, RoutedJournalError> {
    validate_realtime_route(route)?;
    if source_epoch == [0; 32] {
        return Err(RoutedJournalError::Storage(
            "real-time journal source epoch must be non-zero".into(),
        ));
    }
    let JournalRoute::Bucket {
        tenant_id,
        bucket_id,
    } = route
    else {
        unreachable!("validated bucket route")
    };
    let mut key = Vec::with_capacity(REALTIME_BUCKET_ROUTE_KEY_BYTES - 8);
    key.extend_from_slice(&[STORAGE_KEY_FORMAT_VERSION, REALTIME_BUCKET_ROUTE_DOMAIN]);
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(&source_epoch);
    Ok(key)
}

fn realtime_route_key(
    route: JournalRoute,
    source_epoch: [u8; 32],
    offset: u64,
) -> Result<Vec<u8>, RoutedJournalError> {
    if offset == 0 {
        return Err(RoutedJournalError::Storage(
            "real-time journal offset must be non-zero".into(),
        ));
    }
    realtime_route_key_from_prefix(realtime_route_prefix(route, source_epoch)?, offset)
}

fn realtime_route_key_from_prefix(
    mut key: Vec<u8>,
    offset: u64,
) -> Result<Vec<u8>, RoutedJournalError> {
    key.extend_from_slice(&offset.to_be_bytes());
    Ok(key)
}

fn realtime_route_from_prefix(key: &[u8]) -> Result<(JournalRoute, [u8; 32]), RoutedJournalError> {
    if key.len() != REALTIME_BUCKET_ROUTE_PREFIX_BYTES
        || key[..2] != [STORAGE_KEY_FORMAT_VERSION, REALTIME_BUCKET_ROUTE_DOMAIN]
    {
        return Err(RoutedJournalError::Storage(
            "real-time journal route prefix is malformed".into(),
        ));
    }
    let tenant_id = u64::from_be_bytes(key[2..10].try_into().expect("fixed slice"));
    let bucket_id = u64::from_be_bytes(key[10..18].try_into().expect("fixed slice"));
    let source_epoch = key[18..50].try_into().expect("fixed slice");
    Ok((
        JournalRoute::Bucket {
            tenant_id,
            bucket_id,
        },
        source_epoch,
    ))
}

fn route_order_key(route: JournalRoute) -> (u64, u64) {
    match route {
        JournalRoute::Bucket {
            tenant_id,
            bucket_id,
        } => (tenant_id, bucket_id),
        JournalRoute::Definition(_) => unreachable!("validated bucket route"),
    }
}

fn realtime_route_offset(key: &[u8]) -> Result<u64, RoutedJournalError> {
    if key.len() != REALTIME_BUCKET_ROUTE_KEY_BYTES {
        return Err(RoutedJournalError::Storage(
            "real-time journal route key is malformed".into(),
        ));
    }
    Ok(u64::from_be_bytes(
        key[key.len() - 8..].try_into().expect("fixed slice"),
    ))
}

fn realtime_storage(error: impl std::fmt::Display) -> RoutedJournalError {
    RoutedJournalError::Storage(error.to_string())
}

fn realtime_mutation_error(error: impl std::fmt::Display) -> crate::MutationError {
    crate::MutationError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BatchOperation, Durability, IndexingIntent, ObjectKey, ObjectMutationContext,
        ObjectMutationGovernance, PlacementLogId, PutMode, PutRequest, StoreOptions,
    };

    fn put(path: &str, command_id: &str) -> BatchOperation {
        put_in("bucket", path, command_id)
    }

    fn put_in(bucket: &str, path: &str, command_id: &str) -> BatchOperation {
        BatchOperation::Put(PutRequest {
            key: ObjectKey::new("tenant", bucket, path).unwrap(),
            bytes: format!("payload-{path}").into_bytes(),
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::Put,
            command_id: Some(command_id.into()),
            durability: Durability::Local,
        })
    }

    #[tokio::test]
    async fn pending_route_enumeration_is_distinct_and_exclusively_paginated() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        let (tenant_id, first_bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let (_, second_bucket_id) = store.resolve_bucket_ids("tenant", "bucket-two").unwrap();
        let governance = |bucket: &str, bucket_id| ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", bucket).unwrap(),
            policy: store.bucket_policy("tenant", bucket).unwrap(),
        };
        store
            .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                vec![
                    (
                        put("one", "one"),
                        governance("bucket", first_bucket_id),
                        None,
                        IndexingIntent::Realtime,
                    ),
                    (
                        put("two", "two"),
                        governance("bucket", first_bucket_id),
                        None,
                        IndexingIntent::Realtime,
                    ),
                    (
                        put_in("bucket-two", "three", "three"),
                        governance("bucket-two", second_bucket_id),
                        None,
                        IndexingIntent::Realtime,
                    ),
                ],
                context(),
            )
            .await
            .unwrap();
        let source_id = store.local_watch_status().unwrap().source_id;
        let first = store.realtime_bucket_routes(source_id, None, 1).unwrap();
        assert_eq!(first.len(), 1);
        let second = store
            .realtime_bucket_routes(source_id, Some(first[0]), 1)
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_ne!(first[0], second[0]);
        assert!(
            store
                .realtime_bucket_routes(source_id, Some(second[0]), 1)
                .unwrap()
                .is_empty()
        );
    }

    fn context() -> ObjectMutationContext {
        ObjectMutationContext {
            active_placement_log_id: PlacementLogId { term: 3, index: 9 },
            serving_fence_term: 4,
        }
    }

    #[tokio::test]
    async fn only_requested_mutations_enter_the_durable_realtime_route() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        let wake = store.subscribe_realtime_journal_changes();
        let outcomes = store
            .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                vec![
                    (
                        put("ordinary", "ordinary-command"),
                        governance.clone(),
                        None,
                        IndexingIntent::Standard,
                    ),
                    (
                        put("urgent", "urgent-command"),
                        governance,
                        None,
                        IndexingIntent::Realtime,
                    ),
                ],
                context(),
            )
            .await
            .unwrap()
            .outcomes;
        assert!(wake.has_changed().unwrap());
        let ordinary = outcomes[0].as_ref().unwrap();
        assert!(ordinary.receipt.realtime_visibility.is_none());
        let urgent = outcomes[1].as_ref().unwrap();
        let evidence = urgent.receipt.realtime_visibility.as_ref().unwrap();
        assert_eq!(evidence.tenant_id, tenant_id);
        assert_eq!(evidence.bucket_id, bucket_id);
        assert_eq!(evidence.exact_path, "urgent");
        assert_eq!(
            evidence.active_placement_log_id,
            context().active_placement_log_id
        );

        let status = store.local_watch_status().unwrap();
        assert_eq!(
            store
                .realtime_bucket_routes(status.source_id, None, 10)
                .unwrap(),
            vec![JournalRoute::Bucket {
                tenant_id,
                bucket_id,
            }]
        );
        let page = store
            .scan_realtime_routed_local_changes(
                JournalRoute::Bucket {
                    tenant_id,
                    bucket_id,
                },
                status.source_id,
                status.retention_floor,
                status.tail,
                MAX_LOCAL_INVALIDATION_SCAN_RECORDS,
                1024 * 1024,
            )
            .unwrap();
        assert_eq!(page.changes.len(), 1);
        assert_eq!(page.changes[0].offset(), evidence.source_journal_position);
        assert_eq!(page.through_offset, status.tail);

        let marker_cf = store.cf(CF_REALTIME_JOURNAL_ROUTES).unwrap();
        let marker_keys = store
            .db
            .iterator_cf(marker_cf, IteratorMode::Start)
            .map(|item| item.unwrap().0.to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            marker_keys.len(),
            2,
            "one discovery key and one offset marker"
        );
        for key in marker_keys {
            store.db.delete_cf(marker_cf, key).unwrap();
        }
        let stale_epoch = [0xA5; 32];
        let route = JournalRoute::Bucket {
            tenant_id,
            bucket_id,
        };
        store
            .db
            .put_cf(
                marker_cf,
                realtime_route_prefix(route, stale_epoch).unwrap(),
                [],
            )
            .unwrap();
        store
            .db
            .put_cf(
                marker_cf,
                realtime_route_key(route, stale_epoch, 1).unwrap(),
                [],
            )
            .unwrap();

        drop(store);
        let reopened = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        let replay_governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: reopened.bucket_versioning("tenant", "bucket").unwrap(),
            policy: reopened.bucket_policy("tenant", "bucket").unwrap(),
        };
        let retry = reopened
            .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                vec![(
                    put("urgent", "urgent-command"),
                    replay_governance,
                    None,
                    IndexingIntent::Realtime,
                )],
                context(),
            )
            .await
            .unwrap();
        let retry_receipt = retry.outcomes[0].as_ref().unwrap().receipt.clone();
        assert!(retry_receipt.replayed);
        assert_eq!(retry_receipt.realtime_visibility.as_ref(), Some(evidence));
        let reopened_status = reopened.local_watch_status().unwrap();
        assert!(
            reopened
                .db
                .iterator_cf(
                    reopened.cf(CF_REALTIME_JOURNAL_ROUTES).unwrap(),
                    IteratorMode::Start,
                )
                .all(|item| {
                    let (key, _) = item.unwrap();
                    !key.windows(stale_epoch.len())
                        .any(|window| window == stale_epoch)
                })
        );
        let replayed = reopened
            .scan_realtime_routed_local_changes(
                JournalRoute::Bucket {
                    tenant_id,
                    bucket_id,
                },
                reopened_status.source_id,
                reopened_status.retention_floor,
                reopened_status.tail,
                MAX_LOCAL_INVALIDATION_SCAN_RECORDS,
                1024 * 1024,
            )
            .unwrap();
        assert_eq!(replayed.changes.len(), 1);
        assert_eq!(
            replayed.changes[0].offset(),
            evidence.source_journal_position
        );
    }

    #[tokio::test]
    async fn realtime_notification_ignores_ordinary_journal_commits() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        let wake = store.subscribe_realtime_journal_changes();
        store
            .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                vec![(
                    put("ordinary-wake", "ordinary-wake"),
                    governance.clone(),
                    None,
                    IndexingIntent::Standard,
                )],
                context(),
            )
            .await
            .unwrap();
        assert!(!wake.has_changed().unwrap());
        store
            .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                vec![(
                    put("realtime-wake", "realtime-wake"),
                    governance,
                    None,
                    IndexingIntent::Realtime,
                )],
                context(),
            )
            .await
            .unwrap();
        assert!(wake.has_changed().unwrap());
    }

    #[tokio::test]
    async fn base_coverage_acknowledgement_removes_only_absorbed_markers() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        store
            .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                vec![
                    (
                        put("one", "one-command"),
                        governance.clone(),
                        None,
                        IndexingIntent::Realtime,
                    ),
                    (
                        put("two", "two-command"),
                        governance,
                        None,
                        IndexingIntent::Realtime,
                    ),
                ],
                context(),
            )
            .await
            .unwrap();
        let status = store.local_watch_status().unwrap();
        let route = JournalRoute::Bucket {
            tenant_id,
            bucket_id,
        };
        assert_eq!(
            store
                .acknowledge_realtime_base_coverage(route, status.source_id, status.tail)
                .unwrap(),
            2
        );
        assert!(
            store
                .realtime_bucket_routes(status.source_id, None, 10)
                .unwrap()
                == vec![route]
        );
        let page = store
            .scan_realtime_routed_local_changes(
                route,
                status.source_id,
                status.retention_floor,
                status.tail,
                MAX_LOCAL_INVALIDATION_SCAN_RECORDS,
                1024 * 1024,
            )
            .unwrap();
        assert!(page.changes.is_empty());
        assert_eq!(page.through_offset, status.tail);
    }

    #[tokio::test]
    async fn concurrent_ack_and_commit_cannot_lose_route_discovery() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        store
            .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                vec![(
                    put("before", "before"),
                    governance.clone(),
                    None,
                    IndexingIntent::Realtime,
                )],
                context(),
            )
            .await
            .unwrap();
        let status = store.local_watch_status().unwrap();
        let route = JournalRoute::Bucket {
            tenant_id,
            bucket_id,
        };
        let guard = store.lock_commit("realtime-ack-race-test").await;
        let mutation_store = store.clone();
        let mutation = tokio::spawn(async move {
            mutation_store
                .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                    vec![(
                        put("after", "after"),
                        governance,
                        None,
                        IndexingIntent::Realtime,
                    )],
                    context(),
                )
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;
        assert!(!mutation.is_finished());
        assert_eq!(
            store
                .acknowledge_realtime_base_coverage(route, status.source_id, status.tail)
                .unwrap(),
            1,
            "ack does not acquire the global mutation commit fence"
        );
        assert_eq!(
            store
                .realtime_bucket_routes(status.source_id, None, 10)
                .unwrap(),
            vec![route],
            "persistent registration survives an empty offset set"
        );
        drop(guard);
        mutation.await.unwrap();
        assert_eq!(
            store
                .realtime_bucket_routes(status.source_id, None, 10)
                .unwrap(),
            vec![route]
        );
        let final_status = store.local_watch_status().unwrap();
        let page = store
            .scan_realtime_routed_local_changes(
                route,
                status.source_id,
                status.tail,
                final_status.tail,
                10,
                1024 * 1024,
            )
            .unwrap();
        assert_eq!(page.changes.len(), 1);
    }

    #[tokio::test]
    async fn pending_realtime_marker_pins_its_authoritative_source_event() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(directory.path(), 1)
                .with_watch_retention(crate::WatchRetention::new(1, 1024 * 1024).unwrap()),
        )
        .await
        .unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        store
            .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                vec![(
                    put("pinned", "pinned-command"),
                    governance,
                    None,
                    IndexingIntent::Realtime,
                )],
                context(),
            )
            .await
            .unwrap();
        let status = store.local_watch_status().unwrap();
        store
            .advance_source_journal_reference_safe_through(status.tail)
            .await
            .unwrap();
        assert!(!store.prune_source_journal_for_test().await.unwrap());
        assert_eq!(store.local_watch_status().unwrap().retention_floor, 0);
        assert!(
            store
                .db
                .get_cf(
                    store.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
                    invalidation_key(status.tail),
                )
                .unwrap()
                .is_some()
        );

        let route = JournalRoute::Bucket {
            tenant_id,
            bucket_id,
        };
        assert_eq!(
            store
                .acknowledge_realtime_base_coverage(route, status.source_id, status.tail)
                .unwrap(),
            1
        );
        assert!(store.prune_source_journal_for_test().await.unwrap());
        assert_eq!(
            store.local_watch_status().unwrap().retention_floor,
            status.tail
        );
    }

    #[tokio::test]
    async fn indexing_intent_is_part_of_command_idempotency() {
        async fn run(first: IndexingIntent, second: IndexingIntent) {
            let directory = tempfile::tempdir().unwrap();
            let store = Store::open(StoreOptions::new(directory.path(), 1))
                .await
                .unwrap();
            let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
            let governance = ObjectMutationGovernance {
                tenant_id,
                bucket_id,
                versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
                policy: store.bucket_policy("tenant", "bucket").unwrap(),
            };
            let first_outcome = store
                .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                    vec![(put("same", "same-command"), governance.clone(), None, first)],
                    context(),
                )
                .await
                .unwrap();
            assert!(first_outcome.outcomes[0].is_ok());
            let second_outcome = store
                .coordinate_single_node_mutation_batch_with_settlement_and_indexing(
                    vec![(put("same", "same-command"), governance, None, second)],
                    context(),
                )
                .await
                .unwrap();
            assert!(matches!(
                second_outcome.outcomes[0],
                Err(crate::MutationError::IdempotencyConflict)
            ));
        }

        run(IndexingIntent::Standard, IndexingIntent::Realtime).await;
        run(IndexingIntent::Realtime, IndexingIntent::Standard).await;
    }
}
