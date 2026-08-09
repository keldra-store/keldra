//! Pull-based all-source journal barriers for one index builder.
//!
//! Builders read a bounded page only after obtaining their per-kind memory
//! permit. There is no node-local fan-out inbox: a complete source vector is
//! authoritative only when a manifest CAS publishes every prepared run.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    LocalChange, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, OversizeLocalChange, PlacementLogId,
    SourceId, Store, WatchJournalStatus,
};
use thiserror::Error;

use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;

/// The private peer journal codec already rejects pages larger than this.
/// Applying the same bound to local pages keeps both paths equivalent.
pub(crate) const MAX_INDEX_EVENT_PAGE_BYTES: u64 = 16 * 1024 * 1024;

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
    pub encoded_bytes: u64,
    pub oversize: Option<OversizeLocalChange>,
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
        max_bytes: u64,
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
                .map_err(|error| source_error(source.node, error))?
                .map_err(|error| source_error(source.node, error));
        }
        self.peers
            .source_journal_status(source.node, &source.address)
            .await
            .map_err(|error| source_error(source.node, error))
    }

    async fn read_page(
        &self,
        source: &IndexSource,
        expected_source: SourceId,
        after_offset: u64,
        limit: usize,
        max_bytes: u64,
    ) -> Result<IndexSourcePage, IndexEventError> {
        let page = if source.node == self.local_node {
            let store = self.store.clone();
            tokio::task::spawn_blocking(move || {
                store.scan_local_changes_bounded(after_offset, limit, max_bytes)
            })
            .await
            .map_err(|error| source_error(source.node, error))?
            .map_err(|error| source_error(source.node, error))?
        } else {
            self.peers
                .read_source_journal(
                    source.node,
                    &source.address,
                    expected_source,
                    after_offset,
                    limit,
                    max_bytes,
                )
                .await
                .map_err(|error| source_error(source.node, error))?
        };
        Ok(IndexSourcePage {
            source_id: page.source_id,
            changes: page.changes,
            encoded_bytes: page.encoded_bytes,
            oversize: page.oversize,
        })
    }
}

fn source_error(node: NodeId, error: impl std::fmt::Display) -> IndexEventError {
    IndexEventError::Source {
        node,
        message: error.to_string(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexSourceCursor {
    pub source: SourceId,
    /// First source-local journal offset not represented by the checkpoint.
    pub next_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexBarrier {
    pub fence: PlacementLogId,
    pub atomic: AtomicProgramWatermark,
    pub sources: BTreeMap<NodeId, IndexSourceCursor>,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexJournalChange {
    pub node: NodeId,
    pub change: LocalChange,
}

/// One bounded source-local page and the exact cursor after that page.
#[derive(Clone, Debug)]
pub(crate) struct IndexJournalPage {
    pub changes: Vec<IndexJournalChange>,
    pub through: IndexBarrier,
    pub encoded_bytes: u64,
}

pub(crate) struct IndexEventJournal {
    authority: Arc<dyn IndexEventAuthority>,
    sources: Arc<dyn IndexEventSources>,
    page_size: usize,
    observed: std::sync::RwLock<Option<IndexBarrier>>,
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
            observed: std::sync::RwLock::new(None),
        }
    }

    #[cfg(test)]
    fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size.clamp(1, MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        self
    }

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
            cursors.insert(
                source.node,
                IndexSourceCursor {
                    source: status.source_id,
                    next_offset: status
                        .tail
                        .checked_add(1)
                        .ok_or(IndexEventError::OffsetOverflow(source.node))?,
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
        let barrier = IndexBarrier {
            fence: before.fence,
            atomic: before.atomic,
            sources: cursors,
        };
        *self
            .observed
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier.clone());
        Ok(barrier)
    }

    pub(crate) fn last_observed_barrier(&self) -> Option<IndexBarrier> {
        self.observed
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Revalidate the authority named by a completed candidate immediately
    /// before its current-pointer CAS. Later ordinary writes may make the
    /// candidate stale, but cannot invalidate it; a membership/source epoch or
    /// atomic-program watermark change does invalidate the candidate.
    pub(crate) async fn validate_publication_barrier(
        &self,
        candidate: &IndexBarrier,
    ) -> Result<(), IndexEventError> {
        let observed = self.capture_barrier().await?;
        if observed.fence != candidate.fence || observed.atomic != candidate.atomic {
            return Err(IndexEventError::BarrierChanged);
        }
        if observed.sources.len() != candidate.sources.len()
            || candidate.sources.iter().any(|(node, cursor)| {
                observed.sources.get(node).is_none_or(|latest| {
                    latest.source != cursor.source || latest.next_offset < cursor.next_offset
                })
            })
        {
            return Err(IndexEventError::IncompleteSources);
        }
        Ok(())
    }

    /// Bind source-local RocksDB snapshot tails to the current membership and
    /// complete atomic-program watermark.
    ///
    /// The snapshot transport already proves each `(source, tail)` pair came
    /// from one held RocksDB snapshot. This final authority check proves the
    /// complete set belongs to the same ACTIVE membership fence before a
    /// rebuild consumes any frames.
    pub(crate) fn barrier_from_snapshot_tails(
        &self,
        fence: PlacementLogId,
        tails: &[(NodeId, SourceId, u64)],
    ) -> Result<IndexBarrier, IndexEventError> {
        let placement = self
            .authority
            .current()
            .map_err(IndexEventError::Placement)?;
        if placement.fence != fence || !placement.atomic.is_clear() {
            return Err(IndexEventError::BarrierChanged);
        }
        let expected = placement
            .sources
            .iter()
            .map(|source| source.node)
            .collect::<Vec<_>>();
        let mut cursors = BTreeMap::new();
        for &(node, source, tail) in tails {
            if u64::from(source.node_id) != node.0 || cursors.contains_key(&node) {
                return Err(IndexEventError::IncompleteSources);
            }
            cursors.insert(
                node,
                IndexSourceCursor {
                    source,
                    next_offset: tail
                        .checked_add(1)
                        .ok_or(IndexEventError::OffsetOverflow(node))?,
                },
            );
        }
        if cursors.keys().copied().collect::<Vec<_>>() != expected {
            return Err(IndexEventError::IncompleteSources);
        }
        let barrier = IndexBarrier {
            fence,
            atomic: placement.atomic,
            sources: cursors,
        };
        *self
            .observed
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(barrier.clone());
        Ok(barrier)
    }

    /// Pull at most one byte-bounded page from the first source still behind.
    ///
    /// The caller advances to `page.through` only after durably preparing the
    /// corresponding mutation run. Returning `None` proves the complete target
    /// vector under the same placement and atomic watermark.
    pub(crate) async fn next_page(
        &self,
        from: &IndexBarrier,
        target: &IndexBarrier,
        max_bytes: u64,
    ) -> Result<Option<IndexJournalPage>, IndexEventError> {
        if max_bytes == 0 {
            return Err(IndexEventError::ZeroPageByteLimit);
        }
        let placement = self
            .authority
            .current()
            .map_err(IndexEventError::Placement)?;
        require_compatible(from, target, &placement)?;
        let Some(source) = placement.sources.iter().find(|source| {
            from.sources[&source.node].next_offset < target.sources[&source.node].next_offset
        }) else {
            if from != target {
                return Err(IndexEventError::IncompleteSources);
            }
            return Ok(None);
        };

        let start = from.sources[&source.node];
        let through = target.sources[&source.node];
        let status_before = self.sources.status(source).await?;
        validate_status(source.node, &status_before)?;
        if status_before.source_id != start.source
            || start.next_offset.saturating_sub(1) < status_before.retention_floor
        {
            return Err(IndexEventError::SourceEpochChanged(source.node));
        }
        let remaining = usize::try_from(through.next_offset - start.next_offset)
            .unwrap_or(usize::MAX)
            .min(self.page_size);
        let after = start
            .next_offset
            .checked_sub(1)
            .ok_or(IndexEventError::CheckpointMismatch(source.node))?;
        let page = self
            .sources
            .read_page(source, start.source, after, remaining, max_bytes)
            .await?;
        if page.source_id != start.source {
            return Err(IndexEventError::SourceEpochChanged(source.node));
        }
        if let Some(oversize) = page.oversize {
            if !page.changes.is_empty()
                || page.encoded_bytes != 0
                || oversize.offset != start.next_offset
            {
                return Err(IndexEventError::NonContiguousSource(source.node));
            }
            return Err(IndexEventError::PageBytesExceeded {
                bytes: oversize.encoded_bytes,
                limit: max_bytes,
            });
        }
        if page.changes.is_empty() {
            return Err(IndexEventError::SourceEpochChanged(source.node));
        }

        let mut next = start.next_offset;
        let mut encoded_bytes = 0_u64;
        let mut changes = Vec::with_capacity(page.changes.len());
        for change in page.changes {
            if change.offset() != next || next >= through.next_offset {
                return Err(IndexEventError::NonContiguousSource(source.node));
            }
            let bytes = encoded_len(&change)?;
            let projected = encoded_bytes
                .checked_add(bytes)
                .ok_or(IndexEventError::PageLengthOverflow)?;
            if projected > max_bytes && changes.is_empty() {
                return Err(IndexEventError::PageBytesExceeded {
                    bytes: projected,
                    limit: max_bytes,
                });
            }
            if projected > max_bytes {
                break;
            }
            encoded_bytes = projected;
            changes.push(IndexJournalChange {
                node: source.node,
                change,
            });
            next = next
                .checked_add(1)
                .ok_or(IndexEventError::OffsetOverflow(source.node))?;
        }
        if encoded_bytes != page.encoded_bytes {
            return Err(IndexEventError::PageLengthMismatch {
                measured: encoded_bytes,
                reported: page.encoded_bytes,
            });
        }

        let status_after = self.sources.status(source).await?;
        validate_status(source.node, &status_after)?;
        if status_after.source_id != start.source
            || status_after.tail.saturating_add(1) < through.next_offset
        {
            return Err(IndexEventError::SourceEpochChanged(source.node));
        }
        if self
            .authority
            .current()
            .map_err(IndexEventError::Placement)?
            != placement
        {
            return Err(IndexEventError::BarrierChanged);
        }
        let mut advanced = from.clone();
        advanced.sources.get_mut(&source.node).unwrap().next_offset = next;
        if advanced
            .sources
            .iter()
            .all(|(node, cursor)| cursor.next_offset == target.sources[node].next_offset)
        {
            // The complete source vector, not an individual page, crosses an
            // atomic-program watermark. This is the publication boundary that
            // prevents a partially indexed atomic batch.
            advanced.atomic = target.atomic;
        }
        Ok(Some(IndexJournalPage {
            changes,
            through: advanced,
            encoded_bytes,
        }))
    }
}

fn encoded_len(change: &LocalChange) -> Result<u64, IndexEventError> {
    let mut counter = ByteCounter(0);
    serde_json::to_writer(&mut counter, change)
        .map_err(|error| IndexEventError::Encode(error.to_string()))?;
    Ok(counter.0)
}

struct ByteCounter(u64);

impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("index event byte count overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
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
        || from.sources.iter().any(|(node, cursor)| {
            cursor.source != target.sources[node].source
                || cursor.next_offset > target.sources[node].next_offset
        })
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
    #[error("membership or atomic-program state changed while collecting index events")]
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
    #[error("index source {0:?} changed epoch or lost retained history")]
    SourceEpochChanged(NodeId),
    #[error("index source {0:?} returned non-contiguous offsets")]
    NonContiguousSource(NodeId),
    #[error("index source {0:?} exhausted its offset space")]
    OffsetOverflow(NodeId),
    #[error("index event page byte limit must be positive")]
    ZeroPageByteLimit,
    #[error("index event page requires {bytes} bytes but is capped at {limit} bytes")]
    PageBytesExceeded { bytes: u64, limit: u64 },
    #[error("index event page length overflow")]
    PageLengthOverflow,
    #[error("index event page measured {measured} bytes but its source reported {reported}")]
    PageLengthMismatch { measured: u64, reported: u64 },
    #[error("measure index event: {0}")]
    Encode(String),
}

#[cfg(test)]
#[path = "events/tests.rs"]
mod tests;
