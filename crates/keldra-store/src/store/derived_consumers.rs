use std::collections::BTreeSet;

use rocksdb::{WriteBatch, WriteOptions};

use super::{CF_METADATA, Store};
use crate::key::STORAGE_KEY_FORMAT_VERSION;
use crate::{
    DerivedConsumerCheckpoint, DerivedConsumerError, DerivedConsumerKind, DerivedConsumerStatus,
    MAX_DERIVED_CONSUMER_NODES, PlacementLogId, SourceId, SourceJournalRuntimeMetrics,
    WatchJournalStatus,
};

const MEMBERSHIP_DOMAIN: u8 = b'M';
const CHECKPOINT_DOMAIN: u8 = b'C';
const VALUE_FORMAT: u8 = 1;
const MEMBERSHIP_KEY: [u8; 3] = [STORAGE_KEY_FORMAT_VERSION, b'J', MEMBERSHIP_DOMAIN];
const CHECKPOINT_KEY_BYTES: usize = 1 + 1 + 1 + 8 + 8 + 1 + 2;
const CHECKPOINT_VALUE_BYTES: usize = 1 + 2 + 32 + 8;
const MEMBERSHIP_FIXED_BYTES: usize = 1 + 2 + 32 + 8 + 8 + 2;

#[derive(Clone, Debug, Eq, PartialEq)]
struct DerivedMembership {
    source_id: SourceId,
    fence: PlacementLogId,
    active_nodes: Vec<u16>,
}

impl Store {
    /// Installs the exact ACTIVE consumer set for the current membership fence.
    /// A newer fence starts every aggregate consumer at the already-retained
    /// floor, so the previous set can never release more history after cutover.
    pub async fn ensure_derived_consumer_membership(
        &self,
        fence: PlacementLogId,
        active_nodes: &[u16],
    ) -> Result<(), DerivedConsumerError> {
        validate_fence(fence)?;
        validate_active_nodes(active_nodes)?;
        let _commit_guard = self.commit_lock.lock().await;
        let source = self.local_status()?;
        let mut batch = WriteBatch::default();
        let changed = self.stage_membership(&mut batch, source, fence, active_nodes)?;
        if changed {
            self.write_derived_batch(batch)?;
            self.mutation_capacity_notify.notify_waiters();
            self.notify_local_invalidations();
        }
        Ok(())
    }

    /// Durably advances one ACTIVE node's aggregate checkpoint. The caller
    /// supplies the Raft-derived ACTIVE set; storage neither trusts nor derives
    /// membership from the checkpoint itself.
    pub async fn apply_derived_consumer_checkpoint(
        &self,
        checkpoint: DerivedConsumerCheckpoint,
        active_nodes: &[u16],
    ) -> Result<DerivedConsumerStatus, DerivedConsumerError> {
        checkpoint.validate()?;
        validate_active_nodes(active_nodes)?;
        if active_nodes
            .binary_search(&checkpoint.consumer_node_id)
            .is_err()
        {
            return Err(DerivedConsumerError::InactiveConsumer);
        }

        let _commit_guard = self.commit_lock.lock().await;
        let source = self.local_status()?;
        if checkpoint.source_id != source.source_id {
            return Err(DerivedConsumerError::SourceMismatch);
        }
        let minimum_next = source
            .retention_floor
            .checked_add(1)
            .ok_or_else(|| malformed("source retention floor is exhausted"))?;
        let maximum_next = source
            .settled_through
            .checked_add(1)
            .ok_or_else(|| malformed("source settled cursor is exhausted"))?;
        if checkpoint.next_offset < minimum_next {
            return Err(DerivedConsumerError::CheckpointExpired);
        }
        if checkpoint.next_offset > maximum_next {
            return Err(DerivedConsumerError::CheckpointFuture);
        }

        let mut batch = WriteBatch::default();
        let membership_changed =
            self.stage_membership(&mut batch, source, checkpoint.observed_fence, active_nodes)?;
        let key = checkpoint_key(
            checkpoint.observed_fence,
            checkpoint.consumer_kind,
            checkpoint.consumer_node_id,
        );
        let existing = if membership_changed {
            DerivedConsumerCheckpoint {
                next_offset: minimum_next,
                ..checkpoint
            }
        } else {
            let encoded = self
                .db
                .get_cf(self.metadata_cf()?, key)
                .map_err(storage)?
                .ok_or_else(|| malformed("ACTIVE derived checkpoint is missing"))?;
            decode_checkpoint_value(
                checkpoint.consumer_kind,
                checkpoint.consumer_node_id,
                checkpoint.observed_fence,
                &encoded,
            )?
        };
        if existing.source_id != checkpoint.source_id {
            return Err(DerivedConsumerError::SourceMismatch);
        }
        if checkpoint.next_offset < existing.next_offset {
            return Err(DerivedConsumerError::CheckpointRegression);
        }
        if checkpoint.next_offset > existing.next_offset {
            batch.put_cf(
                self.metadata_cf()?,
                key,
                encode_checkpoint_value(checkpoint),
            );
        }
        if !batch.is_empty() {
            self.write_derived_batch(batch)?;
        }
        let status = self
            .derived_consumer_status()?
            .ok_or_else(|| malformed("derived membership disappeared after checkpoint apply"))?;
        self.enforce_local_watch_retention()
            .map_err(|error| DerivedConsumerError::Storage(error.to_string()))?;
        self.mutation_capacity_notify.notify_waiters();
        self.notify_local_invalidations();
        Ok(status)
    }

    pub fn derived_consumer_status(
        &self,
    ) -> Result<Option<DerivedConsumerStatus>, DerivedConsumerError> {
        let Some(membership) = self.read_membership()? else {
            return Ok(None);
        };
        let mut minima = [u64::MAX; 2];
        for kind in DerivedConsumerKind::ALL {
            for node in &membership.active_nodes {
                let key = checkpoint_key(membership.fence, kind, *node);
                let encoded = self
                    .db
                    .get_cf(self.metadata_cf()?, key)
                    .map_err(storage)?
                    .ok_or_else(|| malformed("ACTIVE derived checkpoint is missing"))?;
                let checkpoint = decode_checkpoint_value(kind, *node, membership.fence, &encoded)?;
                if checkpoint.source_id != membership.source_id {
                    return Err(DerivedConsumerError::SourceMismatch);
                }
                let through = checkpoint
                    .next_offset
                    .checked_sub(1)
                    .ok_or_else(|| malformed("derived checkpoint offset is zero"))?;
                minima[kind_index(kind)] = minima[kind_index(kind)].min(through);
            }
        }
        Ok(Some(DerivedConsumerStatus {
            source_id: membership.source_id,
            observed_fence: membership.fence,
            active_consumer_nodes: membership.active_nodes,
            index_safe_through: minima[kind_index(DerivedConsumerKind::Index)],
            accounting_safe_through: minima[kind_index(DerivedConsumerKind::Accounting)],
        }))
    }

    pub fn derived_consumer_checkpoint(
        &self,
        kind: DerivedConsumerKind,
        consumer_node_id: u16,
    ) -> Result<Option<DerivedConsumerCheckpoint>, DerivedConsumerError> {
        let Some(membership) = self.read_membership()? else {
            return Ok(None);
        };
        if membership
            .active_nodes
            .binary_search(&consumer_node_id)
            .is_err()
        {
            return Ok(None);
        }
        let key = checkpoint_key(membership.fence, kind, consumer_node_id);
        self.db
            .get_cf(self.metadata_cf()?, key)
            .map_err(storage)?
            .map(|encoded| {
                decode_checkpoint_value(kind, consumer_node_id, membership.fence, &encoded)
            })
            .transpose()
    }

    pub fn source_journal_runtime_metrics(
        &self,
    ) -> Result<SourceJournalRuntimeMetrics, DerivedConsumerError> {
        let status = self.local_status()?;
        let reference_safe_through = self
            .source_journal_reference_safe_through
            .load(std::sync::atomic::Ordering::Acquire)
            .min(status.settled_through);
        let (index_safe_through, accounting_safe_through) =
            self.derived_consumer_safe_through(status)?;
        Ok(SourceJournalRuntimeMetrics {
            tail: status.tail,
            settled_through: status.settled_through,
            retention_floor: status.retention_floor,
            reference_safe_through,
            index_safe_through,
            accounting_safe_through,
            retained_entries: status.retained_entries,
            retained_bytes: status.retained_bytes,
            max_entries: self.watch_retention.max_entries,
            max_bytes: self.watch_retention.max_bytes,
            progress_debt_peak_entries: self
                .source_journal_progress_debt_peak_entries
                .load(std::sync::atomic::Ordering::Relaxed)
                .max(
                    status
                        .retained_entries
                        .saturating_sub(self.watch_retention.max_entries),
                ),
            progress_debt_peak_bytes: self
                .source_journal_progress_debt_peak_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
                .max(
                    status
                        .retained_bytes
                        .saturating_sub(self.watch_retention.max_bytes),
                ),
        })
    }

    pub(crate) fn derived_consumer_safe_through(
        &self,
        source: WatchJournalStatus,
    ) -> Result<(u64, u64), DerivedConsumerError> {
        let Some(status) = self.derived_consumer_status()? else {
            // Embedded storage users have no cluster-derived consumers. The
            // server installs a membership fence before it admits cluster work.
            return Ok((source.settled_through, source.settled_through));
        };
        if status.source_id != source.source_id {
            return Err(DerivedConsumerError::SourceMismatch);
        }
        if status.index_safe_through > source.settled_through
            || status.accounting_safe_through > source.settled_through
        {
            return Err(DerivedConsumerError::CheckpointFuture);
        }
        Ok((status.index_safe_through, status.accounting_safe_through))
    }

    fn stage_membership(
        &self,
        batch: &mut WriteBatch,
        source: WatchJournalStatus,
        fence: PlacementLogId,
        active_nodes: &[u16],
    ) -> Result<bool, DerivedConsumerError> {
        let existing = self.read_membership()?;
        if let Some(existing) = &existing {
            if existing.source_id != source.source_id {
                return Err(DerivedConsumerError::SourceMismatch);
            }
            match fence_order(existing.fence).cmp(&fence_order(fence)) {
                std::cmp::Ordering::Greater => {
                    return Err(DerivedConsumerError::FenceRegression);
                }
                std::cmp::Ordering::Equal => {
                    if existing.active_nodes != active_nodes {
                        return Err(DerivedConsumerError::MembershipMismatch);
                    }
                    return Ok(false);
                }
                std::cmp::Ordering::Less => {
                    for kind in DerivedConsumerKind::ALL {
                        for node in &existing.active_nodes {
                            batch.delete_cf(
                                self.metadata_cf()?,
                                checkpoint_key(existing.fence, kind, *node),
                            );
                        }
                    }
                }
            }
        }

        let initial_next = source
            .retention_floor
            .checked_add(1)
            .ok_or_else(|| malformed("source retention floor is exhausted"))?;
        let membership = DerivedMembership {
            source_id: source.source_id,
            fence,
            active_nodes: active_nodes.to_vec(),
        };
        batch.put_cf(
            self.metadata_cf()?,
            MEMBERSHIP_KEY,
            encode_membership(&membership)?,
        );
        for kind in DerivedConsumerKind::ALL {
            for node in active_nodes {
                let checkpoint = DerivedConsumerCheckpoint {
                    consumer_kind: kind,
                    source_id: source.source_id,
                    consumer_node_id: *node,
                    next_offset: initial_next,
                    observed_fence: fence,
                };
                batch.put_cf(
                    self.metadata_cf()?,
                    checkpoint_key(fence, kind, *node),
                    encode_checkpoint_value(checkpoint),
                );
            }
        }
        Ok(true)
    }

    fn read_membership(&self) -> Result<Option<DerivedMembership>, DerivedConsumerError> {
        self.db
            .get_cf(self.metadata_cf()?, MEMBERSHIP_KEY)
            .map_err(storage)?
            .map(|encoded| decode_membership(&encoded))
            .transpose()
    }

    fn metadata_cf(&self) -> Result<&rocksdb::ColumnFamily, DerivedConsumerError> {
        self.cf(CF_METADATA)
            .map_err(|error| DerivedConsumerError::Storage(error.to_string()))
    }

    fn local_status(&self) -> Result<WatchJournalStatus, DerivedConsumerError> {
        self.local_watch_status()
            .map_err(|error| DerivedConsumerError::Storage(error.to_string()))
    }

    fn write_derived_batch(&self, batch: WriteBatch) -> Result<(), DerivedConsumerError> {
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage)
    }
}

fn validate_active_nodes(nodes: &[u16]) -> Result<(), DerivedConsumerError> {
    if nodes.is_empty() || nodes.len() > MAX_DERIVED_CONSUMER_NODES {
        return Err(malformed("derived ACTIVE node set is empty or too large"));
    }
    let unique = nodes.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != nodes.len()
        || unique.contains(&0)
        || !nodes.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err(malformed(
            "derived ACTIVE node set must be non-zero, unique, and ordered",
        ));
    }
    Ok(())
}

fn validate_fence(fence: PlacementLogId) -> Result<(), DerivedConsumerError> {
    if fence.term == 0 || fence.index == 0 {
        Err(malformed("derived membership fence is invalid"))
    } else {
        Ok(())
    }
}

fn encode_membership(value: &DerivedMembership) -> Result<Vec<u8>, DerivedConsumerError> {
    validate_fence(value.fence)?;
    validate_active_nodes(&value.active_nodes)?;
    if value.source_id.node_id == 0 || value.source_id.source_epoch == [0; 32] {
        return Err(malformed("derived membership source is invalid"));
    }
    let count = u16::try_from(value.active_nodes.len())
        .map_err(|_| malformed("derived ACTIVE node count is too large"))?;
    let mut encoded = Vec::with_capacity(MEMBERSHIP_FIXED_BYTES + value.active_nodes.len() * 2);
    encoded.push(VALUE_FORMAT);
    encoded.extend_from_slice(&value.source_id.node_id.to_be_bytes());
    encoded.extend_from_slice(&value.source_id.source_epoch);
    encoded.extend_from_slice(&value.fence.term.to_be_bytes());
    encoded.extend_from_slice(&value.fence.index.to_be_bytes());
    encoded.extend_from_slice(&count.to_be_bytes());
    for node in &value.active_nodes {
        encoded.extend_from_slice(&node.to_be_bytes());
    }
    Ok(encoded)
}

fn decode_membership(encoded: &[u8]) -> Result<DerivedMembership, DerivedConsumerError> {
    if encoded.len() < MEMBERSHIP_FIXED_BYTES || encoded[0] != VALUE_FORMAT {
        return Err(malformed("derived membership encoding is invalid"));
    }
    let count = usize::from(u16::from_be_bytes(
        encoded[51..53].try_into().expect("fixed membership count"),
    ));
    if encoded.len() != MEMBERSHIP_FIXED_BYTES + count * 2 {
        return Err(malformed("derived membership length is invalid"));
    }
    let mut active_nodes = Vec::with_capacity(count);
    for chunk in encoded[MEMBERSHIP_FIXED_BYTES..].chunks_exact(2) {
        active_nodes.push(u16::from_be_bytes(chunk.try_into().expect("two-byte node")));
    }
    validate_active_nodes(&active_nodes)?;
    let value = DerivedMembership {
        source_id: SourceId {
            node_id: u16::from_be_bytes(encoded[1..3].try_into().expect("fixed source node")),
            source_epoch: encoded[3..35].try_into().expect("fixed source epoch"),
        },
        fence: PlacementLogId {
            term: u64::from_be_bytes(encoded[35..43].try_into().expect("fixed fence term")),
            index: u64::from_be_bytes(encoded[43..51].try_into().expect("fixed fence index")),
        },
        active_nodes,
    };
    validate_fence(value.fence)?;
    if value.source_id.node_id == 0 || value.source_id.source_epoch == [0; 32] {
        return Err(malformed("derived membership source is invalid"));
    }
    Ok(value)
}

fn checkpoint_key(
    fence: PlacementLogId,
    kind: DerivedConsumerKind,
    node: u16,
) -> [u8; CHECKPOINT_KEY_BYTES] {
    let mut key = [0; CHECKPOINT_KEY_BYTES];
    key[..3].copy_from_slice(&[STORAGE_KEY_FORMAT_VERSION, b'J', CHECKPOINT_DOMAIN]);
    key[3..11].copy_from_slice(&fence.term.to_be_bytes());
    key[11..19].copy_from_slice(&fence.index.to_be_bytes());
    key[19] = kind as u8;
    key[20..22].copy_from_slice(&node.to_be_bytes());
    key
}

fn encode_checkpoint_value(checkpoint: DerivedConsumerCheckpoint) -> [u8; CHECKPOINT_VALUE_BYTES] {
    let mut value = [0; CHECKPOINT_VALUE_BYTES];
    value[0] = VALUE_FORMAT;
    value[1..3].copy_from_slice(&checkpoint.source_id.node_id.to_be_bytes());
    value[3..35].copy_from_slice(&checkpoint.source_id.source_epoch);
    value[35..43].copy_from_slice(&checkpoint.next_offset.to_be_bytes());
    value
}

fn decode_checkpoint_value(
    kind: DerivedConsumerKind,
    node: u16,
    fence: PlacementLogId,
    encoded: &[u8],
) -> Result<DerivedConsumerCheckpoint, DerivedConsumerError> {
    let encoded: &[u8; CHECKPOINT_VALUE_BYTES] = encoded
        .try_into()
        .map_err(|_| malformed("derived checkpoint length is invalid"))?;
    if encoded[0] != VALUE_FORMAT {
        return Err(malformed("derived checkpoint format is unsupported"));
    }
    let checkpoint = DerivedConsumerCheckpoint {
        consumer_kind: kind,
        source_id: SourceId {
            node_id: u16::from_be_bytes(encoded[1..3].try_into().expect("fixed source node")),
            source_epoch: encoded[3..35].try_into().expect("fixed source epoch"),
        },
        consumer_node_id: node,
        next_offset: u64::from_be_bytes(encoded[35..43].try_into().expect("fixed next offset")),
        observed_fence: fence,
    };
    checkpoint.validate()?;
    Ok(checkpoint)
}

const fn kind_index(kind: DerivedConsumerKind) -> usize {
    match kind {
        DerivedConsumerKind::Index => 0,
        DerivedConsumerKind::Accounting => 1,
    }
}

const fn fence_order(fence: PlacementLogId) -> (u64, u64) {
    (fence.term, fence.index)
}

fn malformed(message: impl Into<String>) -> DerivedConsumerError {
    DerivedConsumerError::Malformed(message.into())
}

fn storage(error: rocksdb::Error) -> DerivedConsumerError {
    DerivedConsumerError::Storage(error.to_string())
}

#[cfg(test)]
mod tests;
