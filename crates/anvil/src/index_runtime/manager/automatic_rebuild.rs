//! Deterministic incremental-versus-bulk rebuild selection.

use std::num::NonZeroU64;

use anvil_index::IndexKind;

use crate::index_config::IndexRuntimeConfig;
use crate::index_runtime::events::{
    IndexBarrier, IndexEventError, IndexEventJournal, IndexRoutedLag,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RebuildSelection {
    Incremental(IndexRoutedLag),
    ScopedSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RebuildTrigger {
    Entries,
    Bytes,
    EntriesAndBytes,
    HistoryUnavailable,
}

impl RebuildTrigger {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Entries => "lag_entries",
            Self::Bytes => "lag_bytes",
            Self::EntriesAndBytes => "lag_entries_and_bytes",
            Self::HistoryUnavailable => "history_unavailable",
        }
    }
}

pub(super) async fn select(
    index_id: u64,
    tenant_id: u64,
    bucket_id: u64,
    kind: IndexKind,
    journal: &IndexEventJournal,
    published: &IndexBarrier,
    target: &IndexBarrier,
    config: IndexRuntimeConfig,
) -> Result<RebuildSelection, IndexEventError> {
    let entry_threshold = NonZeroU64::new(config.auto_rebuild_lag_entries())
        .expect("validated automatic rebuild entry threshold is non-zero");
    let byte_threshold = NonZeroU64::new(config.auto_rebuild_lag_bytes())
        .expect("validated automatic rebuild byte threshold is non-zero");
    if !barriers_can_advance(published, target) {
        emit_trigger(
            index_id,
            tenant_id,
            bucket_id,
            kind,
            RebuildTrigger::HistoryUnavailable,
        );
        return Ok(RebuildSelection::ScopedSnapshot);
    }

    let lag = match journal
        .measure_routed_lag(
            tenant_id,
            bucket_id,
            published,
            target,
            entry_threshold,
            byte_threshold,
        )
        .await
    {
        Ok(lag) => lag,
        Err(error) if history_is_unavailable(&error) => {
            emit_trigger(
                index_id,
                tenant_id,
                bucket_id,
                kind,
                RebuildTrigger::HistoryUnavailable,
            );
            return Ok(RebuildSelection::ScopedSnapshot);
        }
        Err(error) => return Err(error),
    };
    emit_lag(
        index_id,
        tenant_id,
        bucket_id,
        kind,
        &lag,
        target,
        entry_threshold,
        byte_threshold,
    );
    let Some(trigger) = classify_lag(&lag, entry_threshold, byte_threshold) else {
        return Ok(RebuildSelection::Incremental(lag));
    };
    emit_trigger(index_id, tenant_id, bucket_id, kind, trigger);
    Ok(RebuildSelection::ScopedSnapshot)
}

/// Extend one below-threshold measurement from its exact observed cursor to a
/// newer source barrier. This keeps catch-up threshold checks linear in new
/// journal traffic rather than rescanning the complete unpublished suffix at
/// every bounded work quantum.
#[allow(clippy::too_many_arguments)]
pub(super) async fn recheck(
    index_id: u64,
    tenant_id: u64,
    bucket_id: u64,
    kind: IndexKind,
    journal: &IndexEventJournal,
    observed: &IndexRoutedLag,
    target: &IndexBarrier,
    config: IndexRuntimeConfig,
) -> Result<RebuildSelection, IndexEventError> {
    let entry_threshold = NonZeroU64::new(config.auto_rebuild_lag_entries())
        .expect("validated automatic rebuild entry threshold is non-zero");
    let byte_threshold = NonZeroU64::new(config.auto_rebuild_lag_bytes())
        .expect("validated automatic rebuild byte threshold is non-zero");
    if let Some(trigger) = classify_lag(observed, entry_threshold, byte_threshold) {
        emit_trigger(index_id, tenant_id, bucket_id, kind, trigger);
        return Ok(RebuildSelection::ScopedSnapshot);
    }
    if !barriers_can_advance(&observed.through, target) {
        emit_trigger(
            index_id,
            tenant_id,
            bucket_id,
            kind,
            RebuildTrigger::HistoryUnavailable,
        );
        return Ok(RebuildSelection::ScopedSnapshot);
    }
    if observed.through == *target {
        return Ok(RebuildSelection::Incremental(observed.clone()));
    }

    let remaining_entries = NonZeroU64::new(entry_threshold.get() - observed.entries)
        .expect("below-threshold entry lag leaves non-zero measurement capacity");
    let remaining_bytes = NonZeroU64::new(byte_threshold.get() - observed.encoded_bytes)
        .expect("below-threshold byte lag leaves non-zero measurement capacity");
    let suffix = match journal
        .measure_routed_lag(
            tenant_id,
            bucket_id,
            &observed.through,
            target,
            remaining_entries,
            remaining_bytes,
        )
        .await
    {
        Ok(lag) => lag,
        Err(error) if history_is_unavailable(&error) => {
            emit_trigger(
                index_id,
                tenant_id,
                bucket_id,
                kind,
                RebuildTrigger::HistoryUnavailable,
            );
            return Ok(RebuildSelection::ScopedSnapshot);
        }
        Err(error) => return Err(error),
    };
    let lag = extend_lag(observed, suffix)?;
    emit_lag(
        index_id,
        tenant_id,
        bucket_id,
        kind,
        &lag,
        target,
        entry_threshold,
        byte_threshold,
    );
    let Some(trigger) = classify_lag(&lag, entry_threshold, byte_threshold) else {
        return Ok(RebuildSelection::Incremental(lag));
    };
    emit_trigger(index_id, tenant_id, bucket_id, kind, trigger);
    Ok(RebuildSelection::ScopedSnapshot)
}

fn extend_lag(
    observed: &IndexRoutedLag,
    suffix: IndexRoutedLag,
) -> Result<IndexRoutedLag, IndexEventError> {
    Ok(IndexRoutedLag {
        entries: observed
            .entries
            .checked_add(suffix.entries)
            .ok_or(IndexEventError::PageLengthOverflow)?,
        encoded_bytes: observed
            .encoded_bytes
            .checked_add(suffix.encoded_bytes)
            .ok_or(IndexEventError::PageLengthOverflow)?,
        through: suffix.through,
    })
}

pub(super) fn barriers_can_advance(from: &IndexBarrier, target: &IndexBarrier) -> bool {
    from.fence == target.fence
        && from.sources.len() == target.sources.len()
        && from.sources.iter().all(|(node, cursor)| {
            target.sources.get(node).is_some_and(|latest| {
                latest.source == cursor.source && latest.next_offset >= cursor.next_offset
            })
        })
}

fn classify_lag(
    lag: &IndexRoutedLag,
    entry_threshold: NonZeroU64,
    byte_threshold: NonZeroU64,
) -> Option<RebuildTrigger> {
    match (
        lag.entry_threshold_reached(entry_threshold),
        lag.byte_threshold_reached(byte_threshold),
    ) {
        (false, false) => None,
        (true, false) => Some(RebuildTrigger::Entries),
        (false, true) => Some(RebuildTrigger::Bytes),
        (true, true) => Some(RebuildTrigger::EntriesAndBytes),
    }
}

fn history_is_unavailable(error: &IndexEventError) -> bool {
    matches!(
        error,
        IndexEventError::CheckpointMismatch(_)
            | IndexEventError::SourceEpochChanged(_)
            | IndexEventError::SourceHistoryGap(_)
            | IndexEventError::IncompleteSources
    )
}

#[allow(clippy::too_many_arguments)]
fn emit_lag(
    index_id: u64,
    tenant_id: u64,
    bucket_id: u64,
    kind: IndexKind,
    lag: &IndexRoutedLag,
    target: &IndexBarrier,
    entry_threshold: NonZeroU64,
    byte_threshold: NonZeroU64,
) {
    tracing::info!(
        index.id = index_id,
        tenant.id = tenant_id,
        bucket.id = bucket_id,
        index.kind = ?kind,
        lag.entries = lag.entries,
        lag.bytes = lag.encoded_bytes,
        lag.entry_threshold = entry_threshold.get(),
        lag.byte_threshold = byte_threshold.get(),
        lag.measurement_complete = lag.through == *target,
        histogram.anvil_index_routed_lag_entries = lag.entries,
        histogram.anvil_index_routed_lag_bytes = lag.encoded_bytes,
        gauge.anvil_index_auto_rebuild_lag_entry_threshold = entry_threshold.get(),
        gauge.anvil_index_auto_rebuild_lag_byte_threshold = byte_threshold.get(),
        "index routed source lag measured"
    );
}

fn emit_trigger(
    index_id: u64,
    tenant_id: u64,
    bucket_id: u64,
    kind: IndexKind,
    trigger: RebuildTrigger,
) {
    let reason = trigger.as_str();
    match trigger {
        RebuildTrigger::HistoryUnavailable => tracing::info!(
            index.id = index_id,
            tenant.id = tenant_id,
            bucket.id = bucket_id,
            index.kind = ?kind,
            reason,
            monotonic_counter.anvil_index_rebuild_triggers_total = 1_u64,
            "index scoped rebuild selected because incremental history is unavailable"
        ),
        RebuildTrigger::Entries | RebuildTrigger::Bytes | RebuildTrigger::EntriesAndBytes => {
            tracing::info!(
                index.id = index_id,
                tenant.id = tenant_id,
                bucket.id = bucket_id,
                index.kind = ?kind,
                reason,
                monotonic_counter.anvil_index_rebuild_triggers_total = 1_u64,
                monotonic_counter.anvil_index_auto_rebuild_threshold_crossings_total = 1_u64,
                "index scoped rebuild selected at deterministic lag threshold"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anvil_consensus::NodeId;
    use anvil_store::{PlacementLogId, SourceId};

    use super::*;
    use crate::index_runtime::events::{AtomicProgramWatermark, IndexSourceCursor};

    fn barrier(offset: u64) -> IndexBarrier {
        IndexBarrier {
            fence: PlacementLogId { term: 2, index: 3 },
            atomic: AtomicProgramWatermark::new(None, None, 0),
            sources: BTreeMap::from([(
                NodeId(1),
                IndexSourceCursor {
                    source: SourceId {
                        node_id: 1,
                        source_epoch: [7; 32],
                    },
                    next_offset: offset,
                },
            )]),
        }
    }

    #[test]
    fn barrier_compatibility_requires_the_same_fence_sources_and_epoch() {
        assert!(barriers_can_advance(&barrier(4), &barrier(7)));
        assert!(!barriers_can_advance(&barrier(8), &barrier(7)));

        let mut changed_fence = barrier(7);
        changed_fence.fence.index += 1;
        assert!(!barriers_can_advance(&barrier(4), &changed_fence));

        let mut changed_epoch = barrier(7);
        changed_epoch
            .sources
            .get_mut(&NodeId(1))
            .unwrap()
            .source
            .source_epoch = [8; 32];
        assert!(!barriers_can_advance(&barrier(4), &changed_epoch));
    }

    #[test]
    fn either_threshold_selects_a_rebuild_deterministically() {
        let entries = NonZeroU64::new(5).unwrap();
        let bytes = NonZeroU64::new(10).unwrap();
        let lag = |entry_count, encoded_bytes| IndexRoutedLag {
            entries: entry_count,
            encoded_bytes,
            through: barrier(1),
        };

        assert_eq!(classify_lag(&lag(4, 9), entries, bytes), None);
        assert_eq!(
            classify_lag(&lag(5, 9), entries, bytes),
            Some(RebuildTrigger::Entries)
        );
        assert_eq!(
            classify_lag(&lag(4, 10), entries, bytes),
            Some(RebuildTrigger::Bytes)
        );
        assert_eq!(
            classify_lag(&lag(5, 10), entries, bytes),
            Some(RebuildTrigger::EntriesAndBytes)
        );
    }

    #[test]
    fn recheck_accumulates_only_the_new_suffix() {
        let observed = IndexRoutedLag {
            entries: 4,
            encoded_bytes: 8,
            through: barrier(10),
        };
        let suffix = IndexRoutedLag {
            entries: 1,
            encoded_bytes: 2,
            through: barrier(11),
        };

        let combined = extend_lag(&observed, suffix).unwrap();

        assert_eq!(combined.entries, 5);
        assert_eq!(combined.encoded_bytes, 10);
        assert_eq!(combined.through, barrier(11));
        assert_eq!(
            classify_lag(
                &combined,
                NonZeroU64::new(5).unwrap(),
                NonZeroU64::new(10).unwrap(),
            ),
            Some(RebuildTrigger::EntriesAndBytes)
        );
    }

    #[test]
    fn below_threshold_recheck_advances_its_exact_resume_cursor() {
        let observed = IndexRoutedLag {
            entries: 2,
            encoded_bytes: 3,
            through: barrier(10),
        };
        let suffix = IndexRoutedLag {
            entries: 1,
            encoded_bytes: 2,
            through: barrier(11),
        };

        let combined = extend_lag(&observed, suffix).unwrap();

        assert_eq!(combined.entries, 3);
        assert_eq!(combined.encoded_bytes, 5);
        assert_eq!(combined.through, barrier(11));
        assert_eq!(
            classify_lag(
                &combined,
                NonZeroU64::new(5).unwrap(),
                NonZeroU64::new(10).unwrap(),
            ),
            None
        );
    }

    #[test]
    fn retained_history_failures_select_a_scoped_snapshot() {
        for error in [
            IndexEventError::CheckpointMismatch(NodeId(1)),
            IndexEventError::SourceEpochChanged(NodeId(1)),
            IndexEventError::SourceHistoryGap(NodeId(1)),
            IndexEventError::IncompleteSources,
        ] {
            assert!(history_is_unavailable(&error));
        }
        assert!(!history_is_unavailable(&IndexEventError::BarrierChanged));
    }
}
