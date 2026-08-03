//! All-source journal barriers for one index-builder node.
//!
//! Every object coordinator owns one source-local ordered journal. The index
//! builder reads the local source directly and every other ACTIVE source over
//! mandatory mTLS. A generation may publish only after consuming one complete
//! vector barrier. Cross-source order is deliberately neither invented nor
//! required: builders reread current heads and compare exact path versions.

use std::collections::BTreeMap;
use std::sync::Arc;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    LocalChange, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, PlacementLogId, SourceId, Store,
    WatchJournalStatus,
};
use thiserror::Error;

use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;

mod router;

pub(crate) use router::{
    IndexEventCatchUp, IndexEventRouter, IndexEventRouterError, IndexEventRouterRetention,
    IndexEventRouterTask,
};

/// The globally replicated atomic-program publication watermark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AtomicProgramWatermark {
    last_commit_cursor: Option<u64>,
    finalized_through: Option<u64>,
    unfinalized_commits: u32,
}

impl AtomicProgramWatermark {
    pub(crate) const fn new(
        last_commit_cursor: Option<u64>,
        finalized_through: Option<u64>,
        unfinalized_commits: u32,
    ) -> Self {
        Self {
            last_commit_cursor,
            finalized_through,
            unfinalized_commits,
        }
    }

    pub(crate) fn is_clear(self) -> bool {
        self.unfinalized_commits == 0 && self.last_commit_cursor == self.finalized_through
    }

    pub(crate) const fn finalized_through(self) -> Option<u64> {
        self.finalized_through
    }

    /// A current head created by a later atomic commit must be deferred until
    /// a later all-source barrier. Ordinary heads carry no program cursor.
    pub(crate) fn permits(self, program_commit_cursor: Option<u64>) -> bool {
        match program_commit_cursor {
            None => true,
            Some(cursor) => self
                .finalized_through
                .is_some_and(|finalized| cursor != 0 && cursor <= finalized),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexSource {
    pub node: NodeId,
    pub address: String,
}

/// One applied ACTIVE membership and atomic-program state snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexEventPlacement {
    pub fence: PlacementLogId,
    pub sources: Vec<IndexSource>,
    pub atomic: AtomicProgramWatermark,
}

pub(crate) trait IndexEventAuthority: Send + Sync + 'static {
    fn current(&self) -> Result<IndexEventPlacement, String>;
}

#[derive(Clone)]
pub(crate) struct DecisionIndexEventAuthority {
    decisions: DecisionRaft,
}

impl DecisionIndexEventAuthority {
    pub(crate) fn new(decisions: DecisionRaft) -> Self {
        Self { decisions }
    }
}

impl IndexEventAuthority for DecisionIndexEventAuthority {
    fn current(&self) -> Result<IndexEventPlacement, String> {
        let state = self.decisions.state().map_err(|error| error.to_string())?;
        let placement =
            ClusterPlacement::from_applied(&state).map_err(|error| error.to_string())?;
        let sources = placement
            .active_node_ids()
            .into_iter()
            .map(|node| {
                let address = placement
                    .address(node)
                    .ok_or_else(|| format!("ACTIVE index source {} has no peer address", node.0))?;
                Ok(IndexSource {
                    node,
                    address: address.0.clone(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(IndexEventPlacement {
            fence: placement.fence(),
            sources,
            atomic: AtomicProgramWatermark::new(
                state.last_commit_cursor(),
                state.finalized_through(),
                state.unfinalized_commit_len(),
            ),
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IndexSourcePage {
    pub source_id: SourceId,
    pub changes: Vec<LocalChange>,
}

#[tonic::async_trait]
pub(crate) trait IndexEventSources: Send + Sync + 'static {
    async fn status(&self, source: &IndexSource) -> Result<WatchJournalStatus, IndexEventError>;

    async fn read_page(
        &self,
        source: &IndexSource,
        expected_source: SourceId,
        after_offset: u64,
        limit: usize,
    ) -> Result<IndexSourcePage, IndexEventError>;
}

#[derive(Clone)]
pub(crate) struct ClusterIndexEventSources {
    local_node: NodeId,
    store: Store,
    peers: DataPeerTransport,
}

impl ClusterIndexEventSources {
    pub(crate) fn new(local_node: NodeId, store: Store, peers: DataPeerTransport) -> Self {
        Self {
            local_node,
            store,
            peers,
        }
    }
}

#[tonic::async_trait]
impl IndexEventSources for ClusterIndexEventSources {
    async fn status(&self, source: &IndexSource) -> Result<WatchJournalStatus, IndexEventError> {
        if source.node == self.local_node {
            let store = self.store.clone();
            return tokio::task::spawn_blocking(move || store.local_watch_status())
                .await
                .map_err(|error| IndexEventError::Source {
                    node: source.node,
                    message: error.to_string(),
                })?
                .map_err(|error| IndexEventError::Source {
                    node: source.node,
                    message: error.to_string(),
                });
        }
        self.peers
            .source_journal_status(source.node, &source.address)
            .await
            .map_err(|error| IndexEventError::Source {
                node: source.node,
                message: error.to_string(),
            })
    }

    async fn read_page(
        &self,
        source: &IndexSource,
        expected_source: SourceId,
        after_offset: u64,
        limit: usize,
    ) -> Result<IndexSourcePage, IndexEventError> {
        let changes = if source.node == self.local_node {
            let store = self.store.clone();
            tokio::task::spawn_blocking(move || store.scan_local_changes(after_offset, limit))
                .await
                .map_err(|error| IndexEventError::Source {
                    node: source.node,
                    message: error.to_string(),
                })?
                .map_err(|error| IndexEventError::Source {
                    node: source.node,
                    message: error.to_string(),
                })?
        } else {
            self.peers
                .read_source_journal(source.node, &source.address, after_offset, limit)
                .await
                .map_err(|error| IndexEventError::Source {
                    node: source.node,
                    message: error.to_string(),
                })?
        };
        Ok(IndexSourcePage {
            source_id: expected_source,
            changes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexSourceCursor {
    pub source: SourceId,
    /// First source offset not included in the checkpoint.
    pub next_offset: u64,
}

/// Complete cluster-wide generation boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexBarrier {
    pub fence: PlacementLogId,
    pub atomic: AtomicProgramWatermark,
    pub sources: BTreeMap<NodeId, IndexSourceCursor>,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexJournalChange {
    pub node: NodeId,
    pub source: SourceId,
    pub change: LocalChange,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexJournalBatch {
    pub changes: Vec<IndexJournalChange>,
    pub through: IndexBarrier,
}

/// Stateless all-source collector. A node-level router uses one instance to
/// read each source once and fan the returned invalidations to local builders.
pub(crate) struct IndexEventJournal {
    authority: Arc<dyn IndexEventAuthority>,
    sources: Arc<dyn IndexEventSources>,
    page_size: usize,
}

impl IndexEventJournal {
    pub(crate) fn new(
        authority: Arc<dyn IndexEventAuthority>,
        sources: Arc<dyn IndexEventSources>,
    ) -> Self {
        Self {
            authority,
            sources,
            page_size: MAX_LOCAL_INVALIDATION_SCAN_RECORDS,
        }
    }

    #[cfg(test)]
    fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size.clamp(1, MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        self
    }

    /// Capture every ACTIVE source tail under one unchanged, fully finalized
    /// atomic-program watermark. A concurrent atomic commit makes the caller
    /// retry rather than creating a partially atomic boundary.
    pub(crate) async fn capture_barrier(&self) -> Result<IndexBarrier, IndexEventError> {
        let before = self
            .authority
            .current()
            .map_err(IndexEventError::Placement)?;
        if !before.atomic.is_clear() {
            return Err(IndexEventError::AtomicProgramInProgress);
        }

        let mut tasks = tokio::task::JoinSet::new();
        for source in before.sources.iter().cloned() {
            let sources = self.sources.clone();
            tasks.spawn(async move {
                let status = sources.status(&source).await;
                (source, status)
            });
        }
        let mut cursors = BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (source, result) =
                joined.map_err(|error| IndexEventError::Task(error.to_string()))?;
            let status = result?;
            validate_status(source.node, &status)?;
            let next_offset = status
                .tail
                .checked_add(1)
                .ok_or(IndexEventError::OffsetOverflow(source.node))?;
            cursors.insert(
                source.node,
                IndexSourceCursor {
                    source: status.source_id,
                    next_offset,
                },
            );
        }

        let after = self
            .authority
            .current()
            .map_err(IndexEventError::Placement)?;
        if before != after || !after.atomic.is_clear() {
            return Err(IndexEventError::BarrierChanged);
        }
        if cursors.len() != before.sources.len() {
            return Err(IndexEventError::IncompleteSources);
        }
        Ok(IndexBarrier {
            fence: before.fence,
            atomic: before.atomic,
            sources: cursors,
        })
    }

    /// Drain every source from `from` through exactly `target`. Results are
    /// returned only if every source succeeds and retains the same epoch.
    pub(crate) async fn drain(
        &self,
        from: &IndexBarrier,
        target: IndexBarrier,
    ) -> Result<IndexJournalBatch, IndexEventError> {
        let placement = self
            .authority
            .current()
            .map_err(IndexEventError::Placement)?;
        require_compatible(from, &target, &placement)?;

        let mut tasks = tokio::task::JoinSet::new();
        for source in placement.sources.iter().cloned() {
            let start = from.sources[&source.node];
            let through = target.sources[&source.node];
            let sources = self.sources.clone();
            let page_size = self.page_size;
            tasks.spawn(
                async move { drain_source(sources, source, start, through, page_size).await },
            );
        }

        let mut changes = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            changes.extend(joined.map_err(|error| IndexEventError::Task(error.to_string()))??);
        }
        if self
            .authority
            .current()
            .map_err(IndexEventError::Placement)?
            != placement
        {
            return Err(IndexEventError::BarrierChanged);
        }
        Ok(IndexJournalBatch {
            changes,
            through: target,
        })
    }
}

async fn drain_source(
    sources: Arc<dyn IndexEventSources>,
    source: IndexSource,
    start: IndexSourceCursor,
    through: IndexSourceCursor,
    page_size: usize,
) -> Result<Vec<IndexJournalChange>, IndexEventError> {
    if start.source != through.source || start.next_offset > through.next_offset {
        return Err(IndexEventError::CheckpointMismatch(source.node));
    }
    let mut next = start.next_offset;
    let mut returned = Vec::new();
    while next < through.next_offset {
        let remaining = usize::try_from(through.next_offset - next).unwrap_or(usize::MAX);
        let after = next
            .checked_sub(1)
            .ok_or(IndexEventError::CheckpointMismatch(source.node))?;
        let page = sources
            .read_page(&source, start.source, after, page_size.min(remaining))
            .await?;
        if page.source_id != start.source || page.changes.is_empty() {
            return Err(IndexEventError::SourceEpochChanged(source.node));
        }
        for change in page.changes {
            if change.offset() != next || next >= through.next_offset {
                return Err(IndexEventError::NonContiguousSource(source.node));
            }
            returned.push(IndexJournalChange {
                node: source.node,
                source: start.source,
                change,
            });
            next = next
                .checked_add(1)
                .ok_or(IndexEventError::OffsetOverflow(source.node))?;
        }
    }
    let status = sources.status(&source).await?;
    validate_status(source.node, &status)?;
    if status.source_id != start.source || status.tail.saturating_add(1) < through.next_offset {
        return Err(IndexEventError::SourceEpochChanged(source.node));
    }
    Ok(returned)
}

fn require_compatible(
    from: &IndexBarrier,
    target: &IndexBarrier,
    placement: &IndexEventPlacement,
) -> Result<(), IndexEventError> {
    if from.fence != target.fence || target.fence != placement.fence {
        return Err(IndexEventError::CheckpointMismatch(NodeId(0)));
    }
    if !target.atomic.is_clear() || target.atomic != placement.atomic {
        return Err(IndexEventError::BarrierChanged);
    }
    let expected = placement
        .sources
        .iter()
        .map(|source| source.node)
        .collect::<Vec<_>>();
    if from.sources.keys().copied().collect::<Vec<_>>() != expected
        || target.sources.keys().copied().collect::<Vec<_>>() != expected
    {
        return Err(IndexEventError::IncompleteSources);
    }
    Ok(())
}

fn validate_status(node: NodeId, status: &WatchJournalStatus) -> Result<(), IndexEventError> {
    if u64::from(status.source_id.node_id) != node.0
        || status.retention_floor > status.tail
        || status.retained_entries != status.tail - status.retention_floor
    {
        return Err(IndexEventError::InvalidSourceStatus(node));
    }
    Ok(())
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum IndexEventError {
    #[error("index placement is unavailable: {0}")]
    Placement(String),
    #[error("an atomic program is awaiting finalization")]
    AtomicProgramInProgress,
    #[error("membership or atomic-program state changed while collecting an index barrier")]
    BarrierChanged,
    #[error("not every ACTIVE source participated in the index barrier")]
    IncompleteSources,
    #[error("index source {node:?} failed: {message}")]
    Source { node: NodeId, message: String },
    #[error("index source task failed: {0}")]
    Task(String),
    #[error("index source {0:?} returned invalid journal status")]
    InvalidSourceStatus(NodeId),
    #[error("index checkpoint is incompatible at source {0:?}")]
    CheckpointMismatch(NodeId),
    #[error("index source {0:?} changed epoch while being drained")]
    SourceEpochChanged(NodeId),
    #[error("index source {0:?} returned non-contiguous offsets")]
    NonContiguousSource(NodeId),
    #[error("index source {0:?} exhausted its offset space")]
    OffsetOverflow(NodeId),
}

#[cfg(test)]
#[path = "events/tests.rs"]
mod tests;
