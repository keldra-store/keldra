use rocksdb::{Direction, IteratorMode, WriteBatch};

use super::{CF_JOURNAL_ROUTES, CF_LOCAL_INVALIDATIONS, Store};
#[cfg(test)]
use crate::definition_state::DefinitionKind;
use crate::journal_route::{JournalRoute, RoutedJournalError, RoutedLocalChangePage};
use crate::key::STORAGE_KEY_FORMAT_VERSION;
use crate::watch::{
    LocalChange, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, OversizeLocalChange, SourceId,
    invalidation_key,
};

const DEFINITION_ROUTE_DOMAIN: u8 = b'D';
const BUCKET_ROUTE_DOMAIN: u8 = b'B';
const DEFINITION_ROUTE_KEY_BYTES: usize = 1 + 1 + 1 + 32 + 8;
const BUCKET_ROUTE_KEY_BYTES: usize = 1 + 1 + 8 + 8 + 32 + 8;

impl Store {
    pub fn scan_routed_local_changes(
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
        validate_route(route)?;
        // Status, route keys, and authoritative journal records share one
        // RocksDB view. Concurrent retention therefore cannot turn a removed
        // route into false proof that an interval contained no matching event.
        let snapshot = self.db.snapshot();
        let status = self
            .local_watch_status_at(&snapshot)
            .map_err(route_storage)?;
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
        if after_offset == target_offset {
            return Ok(RoutedLocalChangePage {
                source_id,
                changes: Vec::new(),
                encoded_bytes: 0,
                through_offset: after_offset,
                oversize: None,
            });
        }

        let prefix = route_prefix(route, source_id.source_epoch);
        let start = route_key(
            route,
            source_id.source_epoch,
            after_offset.saturating_add(1),
        )?;
        let route_cf = self.cf(CF_JOURNAL_ROUTES).map_err(route_storage)?;
        let journal_cf = self.cf(CF_LOCAL_INVALIDATIONS).map_err(route_storage)?;
        let mut changes = Vec::with_capacity(limit);
        let mut encoded_bytes = 0_u64;
        let mut through_offset = after_offset;

        let mut iterator =
            snapshot.iterator_cf(route_cf, IteratorMode::From(&start, Direction::Forward));
        let mut complete_to_target = false;
        loop {
            let Some(item) = iterator.next() else {
                complete_to_target = true;
                break;
            };
            let (key, value) = item.map_err(route_storage)?;
            if !key.starts_with(&prefix) {
                complete_to_target = true;
                break;
            }
            if !value.is_empty() {
                return Err(RoutedJournalError::Storage(
                    "routed journal value must be empty".into(),
                ));
            }
            let offset = route_offset(route, &key)?;
            if offset <= after_offset {
                continue;
            }
            if offset > status.tail {
                return Err(RoutedJournalError::RouteMismatch { offset });
            }
            if offset > target_offset {
                complete_to_target = true;
                break;
            }
            if changes.len() == limit {
                break;
            }
            let encoded = snapshot
                .get_cf(journal_cf, invalidation_key(offset))
                .map_err(route_storage)?
                .ok_or(RoutedJournalError::MissingPrimary { offset })?;
            let change = self
                .decode_local_change_record(&encoded)
                .map_err(route_storage)?;
            if change.offset() != offset || !route_matches(route, &change) {
                return Err(RoutedJournalError::RouteMismatch { offset });
            }
            // Private peers serialize each bare LocalChange, not the
            // RocksDB-only versioned journal envelope or its key.
            let change_bytes =
                super::watch_journal::encoded_change_len(&change).map_err(route_storage)?;
            let projected = encoded_bytes.checked_add(change_bytes).ok_or_else(|| {
                RoutedJournalError::Storage("routed journal page length overflow".into())
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
                break;
            }
            encoded_bytes = projected;
            through_offset = offset;
            changes.push(change);
        }

        // If iteration found no next matching route, every source position
        // through the captured tail is proven irrelevant to this route.
        if complete_to_target {
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

    pub(crate) fn stage_journal_routes(
        &self,
        batch: &mut WriteBatch,
        source_epoch: [u8; 32],
        change: &LocalChange,
    ) -> Result<(), crate::MutationError> {
        let routes = routes_for_change(change);
        let cf = self.cf(CF_JOURNAL_ROUTES)?;
        for route in routes {
            batch.put_cf(
                cf,
                route_key(route, source_epoch, change.offset()).map_err(route_mutation_error)?,
                [],
            );
        }
        Ok(())
    }

    pub(crate) fn stage_journal_route_removal(
        &self,
        batch: &mut WriteBatch,
        source_epoch: [u8; 32],
        change: &LocalChange,
    ) -> Result<(), crate::MutationError> {
        let cf = self.cf(CF_JOURNAL_ROUTES)?;
        for route in routes_for_change(change) {
            batch.delete_cf(
                cf,
                route_key(route, source_epoch, change.offset()).map_err(route_mutation_error)?,
            );
        }
        Ok(())
    }
}

fn routes_for_change(change: &LocalChange) -> Vec<JournalRoute> {
    match change {
        LocalChange::ObjectHead(change) => {
            let mut routes = vec![JournalRoute::Bucket {
                tenant_id: change.tenant_id,
                bucket_id: change.bucket_id,
            }];
            if let Some(transition) = change.definition_transition.as_ref() {
                routes.push(JournalRoute::Definition(transition.kind));
            }
            routes
        }
        LocalChange::RetainedVersionDeleted(change) => vec![JournalRoute::Bucket {
            tenant_id: change.tenant_id,
            bucket_id: change.bucket_id,
        }],
        LocalChange::AggregateChanged(_) | LocalChange::ContentLifecycleChanged(_) => Vec::new(),
    }
}

pub(crate) fn journal_route_logical_bytes(change: &LocalChange) -> u64 {
    routes_for_change(change)
        .into_iter()
        .map(|route| match route {
            JournalRoute::Definition(_) => DEFINITION_ROUTE_KEY_BYTES as u64,
            JournalRoute::Bucket { .. } => BUCKET_ROUTE_KEY_BYTES as u64,
        })
        .sum()
}

fn route_matches(route: JournalRoute, change: &LocalChange) -> bool {
    match (route, change) {
        (
            JournalRoute::Bucket {
                tenant_id,
                bucket_id,
            },
            LocalChange::ObjectHead(change),
        ) => change.tenant_id == tenant_id && change.bucket_id == bucket_id,
        (
            JournalRoute::Bucket {
                tenant_id,
                bucket_id,
            },
            LocalChange::RetainedVersionDeleted(change),
        ) => change.tenant_id == tenant_id && change.bucket_id == bucket_id,
        (JournalRoute::Definition(kind), LocalChange::ObjectHead(change)) => change
            .definition_transition
            .as_ref()
            .is_some_and(|transition| transition.kind == kind),
        _ => false,
    }
}

fn validate_route(route: JournalRoute) -> Result<(), RoutedJournalError> {
    if let JournalRoute::Bucket {
        tenant_id,
        bucket_id,
    } = route
        && (tenant_id == 0 || bucket_id == 0)
    {
        return Err(RoutedJournalError::Storage(
            "routed journal bucket IDs must be non-zero".into(),
        ));
    }
    Ok(())
}

fn route_prefix(route: JournalRoute, source_epoch: [u8; 32]) -> Vec<u8> {
    match route {
        JournalRoute::Definition(kind) => {
            let mut key = Vec::with_capacity(DEFINITION_ROUTE_KEY_BYTES - 8);
            key.extend_from_slice(&[
                STORAGE_KEY_FORMAT_VERSION,
                DEFINITION_ROUTE_DOMAIN,
                kind as u8,
            ]);
            key.extend_from_slice(&source_epoch);
            key
        }
        JournalRoute::Bucket {
            tenant_id,
            bucket_id,
        } => {
            let mut key = Vec::with_capacity(BUCKET_ROUTE_KEY_BYTES - 8);
            key.extend_from_slice(&[STORAGE_KEY_FORMAT_VERSION, BUCKET_ROUTE_DOMAIN]);
            key.extend_from_slice(&tenant_id.to_be_bytes());
            key.extend_from_slice(&bucket_id.to_be_bytes());
            key.extend_from_slice(&source_epoch);
            key
        }
    }
}

pub(crate) fn route_key(
    route: JournalRoute,
    source_epoch: [u8; 32],
    offset: u64,
) -> Result<Vec<u8>, RoutedJournalError> {
    validate_route(route)?;
    if source_epoch == [0; 32] || offset == 0 {
        return Err(RoutedJournalError::Storage(
            "routed journal source epoch and offset must be non-zero".into(),
        ));
    }
    let mut key = route_prefix(route, source_epoch);
    key.extend_from_slice(&offset.to_be_bytes());
    Ok(key)
}

fn route_offset(route: JournalRoute, key: &[u8]) -> Result<u64, RoutedJournalError> {
    let expected = match route {
        JournalRoute::Definition(_) => DEFINITION_ROUTE_KEY_BYTES,
        JournalRoute::Bucket { .. } => BUCKET_ROUTE_KEY_BYTES,
    };
    if key.len() != expected {
        return Err(RoutedJournalError::Storage(
            "routed journal key is malformed".into(),
        ));
    }
    Ok(u64::from_be_bytes(
        key[key.len() - 8..].try_into().expect("fixed slice"),
    ))
}

fn route_storage(error: impl std::fmt::Display) -> RoutedJournalError {
    RoutedJournalError::Storage(error.to_string())
}

fn route_mutation_error(error: impl std::fmt::Display) -> crate::MutationError {
    crate::MutationError::Storage(error.to_string())
}

#[cfg(test)]
mod tests;
