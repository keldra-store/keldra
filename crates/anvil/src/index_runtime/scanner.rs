//! Fenced, pull-based cluster scans for cold discovery and rebuilds.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{ObjectRecordCursor, PlacementLogId, RetainedObjectSnapshot, SourceId};
use tonic::Status;

use crate::cluster_peer::{
    ClusterPeerTransport, IndexCurrentHead, IndexHeadScanPage, IndexHeadScanScope,
    IndexSourceSnapshot, IndexSourceSnapshotHead, RetainedSourceSnapshot,
};
use crate::cluster_placement::ClusterPlacement;

#[derive(Clone)]
pub(crate) struct ClusterIndexScanner {
    decisions: DecisionRaft,
    peers: ClusterPeerTransport,
    evidence: Arc<HeadScanEvidence>,
}

#[derive(Default)]
struct HeadScanEvidence {
    counts: Mutex<HeadScanCounts>,
}

#[derive(Clone, Copy, Default)]
struct HeadScanCounts {
    total: u64,
    scoped: u64,
}

impl ClusterIndexScanner {
    pub(crate) fn new(decisions: DecisionRaft, peers: ClusterPeerTransport) -> Self {
        Self {
            decisions,
            peers,
            evidence: Arc::new(HeadScanEvidence::default()),
        }
    }

    pub(crate) fn scan_evidence(&self) -> (u64, u64) {
        let counts = *self
            .evidence
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (counts.scoped, counts.total.saturating_sub(counts.scoped))
    }

    fn record_scoped_scan(&self) {
        let mut counts = self
            .evidence
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        counts.total = counts.total.saturating_add(1);
        counts.scoped = counts.scoped.saturating_add(1);
    }

    /// Begin a scan without fetching a page. The caller can therefore obtain
    /// its memory permit before every `next_page` call.
    pub(crate) fn begin(&self, scope: IndexHeadScanScope) -> Result<ClusterIndexScan, Status> {
        self.record_scoped_scan();
        let placement = self.placement()?;
        let fence = placement.fence();
        let nodes = placement
            .active_node_ids()
            .into_iter()
            .map(|node| {
                let address = placement
                    .address(node)
                    .ok_or_else(|| Status::unavailable("ACTIVE index scan source has no address"))?
                    .0
                    .clone();
                Ok(ScanNode { node, address })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(ClusterIndexScan {
            scanner: self.clone(),
            scope,
            fence,
            nodes,
            node_index: 0,
            cursor: None,
            finished: false,
        })
    }

    /// Open one snapshot-bound current-head stream for every ACTIVE source.
    ///
    /// Every stream is opened before the first frame is consumed so its source
    /// epoch and captured journal tail form one rebuild boundary. Frames are
    /// then pulled from one source at a time; the runtime never buffers a page
    /// for every node while waiting for its construction-memory permit.
    pub(crate) async fn begin_source_snapshot(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: String,
        max_frame_bytes: u64,
    ) -> Result<ClusterIndexSourceSnapshot, Status> {
        self.record_scoped_scan();
        let placement = self.placement()?;
        let fence = placement.fence();
        let sources = placement
            .active_node_ids()
            .into_iter()
            .map(|node| {
                let address = placement
                    .address(node)
                    .ok_or_else(|| {
                        Status::unavailable("ACTIVE index snapshot source has no address")
                    })?
                    .0
                    .clone();
                Ok((node, address))
            })
            .collect::<Result<Vec<_>, Status>>()?;

        let mut tasks = tokio::task::JoinSet::new();
        for (node, address) in sources {
            let peers = self.peers.clone();
            let path_prefix = path_prefix.clone();
            tasks.spawn(async move {
                let snapshot = peers
                    .scan_index_source_snapshot(
                        node,
                        &address,
                        tenant_id,
                        bucket_id,
                        path_prefix,
                        max_frame_bytes,
                    )
                    .await;
                (node, snapshot)
            });
        }

        let mut opened = BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (node, snapshot) = joined.map_err(|error| {
                Status::internal(format!("index snapshot task failed: {error}"))
            })?;
            let snapshot = snapshot?;
            if snapshot.placement_fence() != fence || u64::from(snapshot.source().node_id) != node.0
            {
                return Err(Status::data_loss(
                    "index source snapshot identity or placement fence is inconsistent",
                ));
            }
            if opened.insert(node, snapshot).is_some() {
                return Err(Status::data_loss(
                    "index source snapshot returned a duplicate ACTIVE source",
                ));
            }
        }
        if opened.len() != placement.active_node_ids().len() {
            return Err(Status::unavailable(
                "not every ACTIVE source opened an index source snapshot",
            ));
        }
        self.require_fence(fence)?;

        let checkpoints = opened
            .iter()
            .map(|(&node, snapshot)| IndexSnapshotSourceCheckpoint {
                node,
                source: snapshot.source(),
                captured_tail: snapshot.captured_tail(),
            })
            .collect();
        Ok(ClusterIndexSourceSnapshot {
            scanner: self.clone(),
            fence,
            checkpoints,
            snapshots: opened.into_values().collect(),
            source_index: 0,
            finished: false,
        })
    }

    /// Open one retained `(path, version)` snapshot stream for every ACTIVE
    /// source. Frames remain credit-driven and are consumed sequentially so a
    /// bucket with deep version history never materializes all descriptors.
    pub(crate) async fn begin_retained_source_snapshot(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: String,
        max_frame_bytes: u64,
    ) -> Result<ClusterRetainedSourceSnapshot, Status> {
        self.record_scoped_scan();
        let placement = self.placement()?;
        let fence = placement.fence();
        let sources = placement
            .active_node_ids()
            .into_iter()
            .map(|node| {
                let address = placement
                    .address(node)
                    .ok_or_else(|| {
                        Status::unavailable("ACTIVE retained snapshot source has no address")
                    })?
                    .0
                    .clone();
                Ok((node, address))
            })
            .collect::<Result<Vec<_>, Status>>()?;

        let mut tasks = tokio::task::JoinSet::new();
        for (node, address) in sources {
            let peers = self.peers.clone();
            let path_prefix = path_prefix.clone();
            tasks.spawn(async move {
                let snapshot = peers
                    .scan_retained_source_snapshot(
                        node,
                        &address,
                        tenant_id,
                        bucket_id,
                        path_prefix,
                        max_frame_bytes,
                    )
                    .await;
                (node, snapshot)
            });
        }

        let mut opened = BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (node, snapshot) = joined.map_err(|error| {
                Status::internal(format!("retained snapshot task failed: {error}"))
            })?;
            let snapshot = snapshot?;
            if snapshot.placement_fence() != fence || u64::from(snapshot.source().node_id) != node.0
            {
                return Err(Status::data_loss(
                    "retained snapshot identity or placement fence is inconsistent",
                ));
            }
            if opened.insert(node, snapshot).is_some() {
                return Err(Status::data_loss(
                    "retained snapshot returned a duplicate ACTIVE source",
                ));
            }
        }
        if opened.len() != placement.active_node_ids().len() {
            return Err(Status::unavailable(
                "not every ACTIVE source opened a retained snapshot",
            ));
        }
        self.require_fence(fence)?;
        let checkpoints = opened
            .iter()
            .map(|(&node, snapshot)| IndexSnapshotSourceCheckpoint {
                node,
                source: snapshot.source(),
                captured_tail: snapshot.captured_tail(),
            })
            .collect();
        Ok(ClusterRetainedSourceSnapshot {
            scanner: self.clone(),
            fence,
            checkpoints,
            snapshots: opened.into_values().collect(),
            source_index: 0,
            finished: false,
        })
    }

    fn placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    fn require_fence(&self, expected: anvil_store::PlacementLogId) -> Result<(), Status> {
        if self.placement()?.fence() != expected {
            return Err(Status::unavailable(
                "cluster placement changed during index scan",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexSnapshotSourceCheckpoint {
    pub(crate) node: NodeId,
    pub(crate) source: SourceId,
    pub(crate) captured_tail: u64,
}

/// Pull cursor over a complete set of source-local snapshot streams.
pub(crate) struct ClusterIndexSourceSnapshot {
    scanner: ClusterIndexScanner,
    fence: PlacementLogId,
    checkpoints: Vec<IndexSnapshotSourceCheckpoint>,
    snapshots: Vec<IndexSourceSnapshot>,
    source_index: usize,
    finished: bool,
}

pub(crate) struct ClusterRetainedSourceSnapshot {
    scanner: ClusterIndexScanner,
    fence: PlacementLogId,
    checkpoints: Vec<IndexSnapshotSourceCheckpoint>,
    snapshots: Vec<RetainedSourceSnapshot>,
    source_index: usize,
    finished: bool,
}

impl ClusterRetainedSourceSnapshot {
    pub(crate) fn placement_fence(&self) -> PlacementLogId {
        self.fence
    }

    pub(crate) fn checkpoints(&self) -> &[IndexSnapshotSourceCheckpoint] {
        &self.checkpoints
    }

    pub(crate) async fn next_frame(
        &mut self,
    ) -> Result<Option<Vec<RetainedObjectSnapshot>>, Status> {
        if self.finished {
            return Ok(None);
        }
        loop {
            let Some(snapshot) = self.snapshots.get_mut(self.source_index) else {
                self.scanner.require_fence(self.fence)?;
                self.finished = true;
                return Ok(None);
            };
            match snapshot.next_frame().await? {
                Some(frame) => {
                    self.scanner.require_fence(self.fence)?;
                    return Ok(Some(frame));
                }
                None => self.source_index += 1,
            }
        }
    }
}

impl ClusterIndexSourceSnapshot {
    pub(crate) fn placement_fence(&self) -> PlacementLogId {
        self.fence
    }

    pub(crate) fn checkpoints(&self) -> &[IndexSnapshotSourceCheckpoint] {
        &self.checkpoints
    }

    pub(crate) async fn next_frame(
        &mut self,
    ) -> Result<Option<Vec<IndexSourceSnapshotHead>>, Status> {
        if self.finished {
            return Ok(None);
        }
        loop {
            let Some(snapshot) = self.snapshots.get_mut(self.source_index) else {
                self.scanner.require_fence(self.fence)?;
                self.finished = true;
                return Ok(None);
            };
            match snapshot.next_frame().await? {
                Some(frame) => {
                    self.scanner.require_fence(self.fence)?;
                    return Ok(Some(frame));
                }
                None => self.source_index += 1,
            }
        }
    }
}

struct ScanNode {
    node: NodeId,
    address: String,
}

/// Sequential page cursor over all ACTIVE sources.
///
/// Sequential fetch is intentional: it prevents one page per node being held
/// while the builder is waiting for its aggregate memory budget.
pub(crate) struct ClusterIndexScan {
    scanner: ClusterIndexScanner,
    scope: IndexHeadScanScope,
    fence: anvil_store::PlacementLogId,
    nodes: Vec<ScanNode>,
    node_index: usize,
    cursor: Option<ObjectRecordCursor>,
    finished: bool,
}

impl ClusterIndexScan {
    pub(crate) async fn next_page(&mut self) -> Result<Option<Vec<IndexCurrentHead>>, Status> {
        if self.finished {
            return Ok(None);
        }
        loop {
            let Some(source) = self.nodes.get(self.node_index) else {
                self.scanner.require_fence(self.fence)?;
                self.finished = true;
                return Ok(None);
            };
            let IndexHeadScanPage {
                heads,
                next_cursor,
                placement_fence,
                ..
            } = self
                .scanner
                .peers
                .scan_index_heads(
                    source.node,
                    &source.address,
                    self.scope.clone(),
                    self.cursor.as_ref(),
                )
                .await?;
            if placement_fence != self.fence {
                return Err(Status::unavailable(
                    "index scan source used another placement fence",
                ));
            }
            match next_cursor {
                Some(next) if self.cursor.as_ref().is_some_and(|current| current == &next) => {
                    return Err(Status::data_loss(
                        "index scan source returned a non-advancing cursor",
                    ));
                }
                Some(next) => self.cursor = Some(next),
                None => {
                    self.node_index += 1;
                    self.cursor = None;
                }
            }
            self.scanner.require_fence(self.fence)?;
            if !heads.is_empty() {
                return Ok(Some(heads));
            }
        }
    }
}
