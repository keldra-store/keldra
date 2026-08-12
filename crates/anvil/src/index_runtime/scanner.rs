//! Fenced, pull-based cluster scans for cold discovery and rebuilds.

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{ObjectRecordCursor, PlacementLogId, RetainedObjectSnapshot, SourceId};
use std::collections::{BTreeMap, VecDeque};
use tonic::Status;

use crate::cluster_peer::{
    ClusterPeerTransport, IndexCurrentHead, IndexHeadScanPage, IndexHeadScanScope,
    IndexSourceSnapshot, IndexSourceSnapshotHead, RetainedSourceSnapshot,
};
use crate::cluster_placement::ClusterPlacement;
use crate::startup_scan_evidence::{StartupScanEvidence, StartupScanExtent, StartupScanKind};

#[derive(Clone)]
pub(crate) struct ClusterIndexScanner {
    decisions: DecisionRaft,
    peers: ClusterPeerTransport,
    startup_scan_evidence: StartupScanEvidence,
}

impl ClusterIndexScanner {
    pub(crate) fn new(
        decisions: DecisionRaft,
        peers: ClusterPeerTransport,
        startup_scan_evidence: StartupScanEvidence,
    ) -> Self {
        Self {
            decisions,
            peers,
            startup_scan_evidence,
        }
    }

    /// Begin a scan without fetching a page. The caller can therefore obtain
    /// its memory permit before every `next_page` call.
    pub(crate) fn begin(&self, scope: IndexHeadScanScope) -> Result<ClusterIndexScan, Status> {
        self.startup_scan_evidence
            .record(StartupScanKind::IndexArtifacts, StartupScanExtent::Scoped);
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
    /// epoch and captured journal tail form one rebuild boundary. The caller's
    /// frame budget is divided across sources and their sorted streams are
    /// merged into one globally canonical path stream.
    pub(crate) async fn begin_source_snapshot(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path_prefix: String,
        max_frame_bytes: u64,
    ) -> Result<ClusterIndexSourceSnapshot, Status> {
        self.startup_scan_evidence
            .record(StartupScanKind::ObjectHeads, StartupScanExtent::Scoped);
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
        if sources.is_empty() {
            return Err(Status::unavailable(
                "index snapshot requires at least one ACTIVE source",
            ));
        }
        let source_count = sources.len() as u64;
        let source_frame_bytes = max_frame_bytes / source_count;
        if source_frame_bytes < 16 * 1024 {
            return Err(Status::resource_exhausted(
                "configured index source quantum cannot fund one bounded frame per ACTIVE source",
            ));
        }

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
                        source_frame_bytes,
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
        let snapshots = opened.into_values().collect::<Vec<_>>();
        let buffers = (0..snapshots.len()).map(|_| VecDeque::new()).collect();
        let ended = vec![false; snapshots.len()];
        Ok(ClusterIndexSourceSnapshot {
            scanner: self.clone(),
            fence,
            checkpoints,
            snapshots,
            buffers,
            ended,
            max_frame_bytes,
            previous_path: None,
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
        self.startup_scan_evidence
            .record(StartupScanKind::ObjectHeads, StartupScanExtent::Scoped);
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
    buffers: Vec<VecDeque<IndexSourceSnapshotHead>>,
    ended: Vec<bool>,
    max_frame_bytes: u64,
    previous_path: Option<String>,
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
        for source in 0..self.snapshots.len() {
            self.fill_source(source).await?;
        }

        let mut output = Vec::new();
        let mut encoded_bytes = 0_u64;
        loop {
            match take_canonical_head(
                &mut self.buffers,
                &mut self.previous_path,
                encoded_bytes,
                self.max_frame_bytes,
            )? {
                CanonicalHeadStep::Empty => {
                    self.scanner.require_fence(self.fence)?;
                    self.finished = true;
                    return if output.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(output))
                    };
                }
                CanonicalHeadStep::FrameFull => {
                    self.scanner.require_fence(self.fence)?;
                    return Ok(Some(output));
                }
                CanonicalHeadStep::Head {
                    source,
                    head,
                    encoded_bytes: head_bytes,
                } => {
                    encoded_bytes = encoded_bytes.checked_add(head_bytes).ok_or_else(|| {
                        Status::resource_exhausted("index snapshot frame overflow")
                    })?;
                    output.push(head);
                    self.fill_source(source).await?;
                }
            }
        }
    }

    async fn fill_source(&mut self, source: usize) -> Result<(), Status> {
        if !self.buffers[source].is_empty() || self.ended[source] {
            return Ok(());
        }
        match self.snapshots[source].next_frame().await? {
            Some(frame) => self.buffers[source].extend(frame),
            None => self.ended[source] = true,
        }
        Ok(())
    }
}

enum CanonicalHeadStep {
    Empty,
    FrameFull,
    Head {
        source: usize,
        head: IndexSourceSnapshotHead,
        encoded_bytes: u64,
    },
}

fn take_canonical_head(
    buffers: &mut [VecDeque<IndexSourceSnapshotHead>],
    previous_path: &mut Option<String>,
    frame_bytes: u64,
    max_frame_bytes: u64,
) -> Result<CanonicalHeadStep, Status> {
    let Some(source) = buffers
        .iter()
        .enumerate()
        .filter_map(|(source, frame)| frame.front().map(|head| (source, head)))
        .min_by(|(_, left), (_, right)| left.exact_path.cmp(&right.exact_path))
        .map(|(source, _)| source)
    else {
        return Ok(CanonicalHeadStep::Empty);
    };
    let head = buffers[source]
        .front()
        .expect("the selected source has one buffered head");
    let encoded_bytes = u64::try_from(
        serde_json::to_vec(head)
            .map_err(|error| Status::internal(format!("measure index snapshot head: {error}")))?
            .len(),
    )
    .map_err(|_| Status::resource_exhausted("index snapshot head exceeds platform"))?;
    if frame_bytes != 0 && frame_bytes.saturating_add(encoded_bytes) > max_frame_bytes {
        return Ok(CanonicalHeadStep::FrameFull);
    }
    if encoded_bytes > max_frame_bytes {
        return Err(Status::resource_exhausted(
            "one index snapshot head exceeds the configured source quantum",
        ));
    }
    let head = buffers[source]
        .pop_front()
        .expect("the selected source head remains buffered");
    if previous_path
        .as_ref()
        .is_some_and(|previous| previous.as_str() >= head.exact_path.as_str())
    {
        return Err(Status::data_loss(
            "index snapshot sources are not globally canonical or contain duplicate paths",
        ));
    }
    *previous_path = Some(head.exact_path.clone());
    Ok(CanonicalHeadStep::Head {
        source,
        head,
        encoded_bytes,
    })
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

#[cfg(test)]
mod tests {
    use anvil_store::{BlobRef, Head, Version, VersionId};

    use super::*;

    fn head(path: &str, version: u64) -> IndexSourceSnapshotHead {
        IndexSourceSnapshotHead {
            tenant_id: 1,
            bucket_id: 2,
            exact_path: path.into(),
            head: Head {
                version: VersionId(version),
                deleted: false,
                mutation_stamp: None,
            },
            version: Version {
                id: VersionId(version),
                blob: Some(BlobRef {
                    hash: [version as u8; 32],
                    length: 10,
                }),
                content_type: Some("application/json".into()),
                deleted: false,
                committed_at_unix_millis: version,
            },
        }
    }

    #[test]
    fn multi_source_merge_is_globally_ordered_and_frame_bounded() {
        let mut sources = vec![
            VecDeque::from([head("a", 1), head("d", 4)]),
            VecDeque::from([head("b", 2), head("e", 5)]),
            VecDeque::from([head("c", 3), head("f", 6)]),
        ];
        let one = serde_json::to_vec(&head("a", 1)).unwrap().len() as u64;
        let limit = one * 2;
        let mut previous = None;
        let mut frame_bytes = 0;
        let mut maximum_frame = 0;
        let mut paths = Vec::new();
        loop {
            match take_canonical_head(&mut sources, &mut previous, frame_bytes, limit).unwrap() {
                CanonicalHeadStep::Head {
                    head,
                    encoded_bytes,
                    ..
                } => {
                    frame_bytes += encoded_bytes;
                    maximum_frame = maximum_frame.max(frame_bytes);
                    paths.push(head.exact_path);
                }
                CanonicalHeadStep::FrameFull => frame_bytes = 0,
                CanonicalHeadStep::Empty => break,
            }
        }
        assert_eq!(paths, ["a", "b", "c", "d", "e", "f"]);
        assert!(maximum_frame <= limit);
    }

    #[test]
    fn multi_source_merge_rejects_duplicate_paths() {
        let mut sources = vec![
            VecDeque::from([head("same", 1)]),
            VecDeque::from([head("same", 2)]),
        ];
        let mut previous = None;
        assert!(matches!(
            take_canonical_head(&mut sources, &mut previous, 0, u64::MAX).unwrap(),
            CanonicalHeadStep::Head { .. }
        ));
        assert!(take_canonical_head(&mut sources, &mut previous, 0, u64::MAX).is_err());
    }
}
