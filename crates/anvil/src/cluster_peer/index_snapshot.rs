//! Credit-driven, snapshot-bound source scans for index rebuilds.

use std::pin::Pin;

use anvil_consensus::{DecisionRaft, NodeId, PeerSpkiSha256};
use anvil_store::{CurrentObjectSnapshot, Head, PlacementLogId, SourceId, Version};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use super::index_artifacts::{contains_reserved_segment, valid_source_prefix};
use super::storage::object_coordinator;
use super::{
    CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, MAX_INDEX_SOURCE_SNAPSHOT_TIME, decode_json,
    encode_json, require_response_schema, wire,
};
use crate::cluster_placement::ClusterPlacement;
use crate::index_service::path_matches_prefix;

const INDEX_SOURCE_FRAME_MAX_RECORDS: u32 = 128;
pub(super) const INDEX_SOURCE_FRAME_MAX_BYTES: u64 = 8 * 1024 * 1024;

pub(super) type IndexSourceSnapshotRpcStream = Pin<
    Box<dyn tokio_stream::Stream<Item = Result<wire::IndexSourceSnapshotResponse, Status>> + Send>,
>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexSourceSnapshotHead {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
    pub head: Head,
    pub version: Version,
}

impl From<CurrentObjectSnapshot> for IndexSourceSnapshotHead {
    fn from(value: CurrentObjectSnapshot) -> Self {
        Self {
            tenant_id: value.tenant_id,
            bucket_id: value.bucket_id,
            exact_path: value.exact_path,
            head: value.head,
            version: value.version,
        }
    }
}

/// Client side of one ephemeral peer snapshot. Sending one pull command is the
/// only operation that permits the server to materialize one data frame.
pub(crate) struct IndexSourceSnapshot {
    source: SourceId,
    captured_tail: u64,
    placement_fence: PlacementLogId,
    target: NodeId,
    decisions: DecisionRaft,
    requests: mpsc::Sender<wire::IndexSourceSnapshotRequest>,
    stream: tonic::Streaming<wire::IndexSourceSnapshotResponse>,
    next_sequence: u64,
    deadline: tokio::time::Instant,
    ended: bool,
}

impl IndexSourceSnapshot {
    pub(crate) fn source(&self) -> SourceId {
        self.source
    }

    pub(crate) fn captured_tail(&self) -> u64 {
        self.captured_tail
    }

    pub(crate) fn placement_fence(&self) -> PlacementLogId {
        self.placement_fence
    }

    /// Pull exactly one bounded frame. Callers must hold the full per-kind
    /// construction permit for the complete duration of this method.
    pub(crate) async fn next_frame(
        &mut self,
    ) -> Result<Option<Vec<IndexSourceSnapshotHead>>, Status> {
        if self.ended {
            return Ok(None);
        }
        require_client_fence(&self.decisions, self.placement_fence, self.target)?;
        let pull = wire::IndexSourceSnapshotRequest {
            command: Some(wire::index_source_snapshot_request::Command::Pull(
                wire::IndexSourceSnapshotPull {
                    sequence: self.next_sequence,
                },
            )),
        };
        tokio::time::timeout_at(self.deadline, self.requests.send(pull))
            .await
            .map_err(|_| Status::deadline_exceeded("index snapshot pull deadline exceeded"))?
            .map_err(|_| Status::unavailable("index snapshot request stream closed"))?;
        let response = tokio::time::timeout_at(self.deadline, self.stream.message())
            .await
            .map_err(|_| Status::deadline_exceeded("index source snapshot deadline exceeded"))??
            .ok_or_else(|| {
                Status::data_loss("index source snapshot ended without a terminal frame")
            })?;
        let frame = match response.event {
            Some(wire::index_source_snapshot_response::Event::Frame(frame)) => frame,
            Some(wire::index_source_snapshot_response::Event::Begun(_)) | None => {
                return Err(Status::data_loss(
                    "index source snapshot returned an unexpected event",
                ));
            }
        };
        validate_frame_identity(
            &frame,
            self.target,
            self.source,
            self.captured_tail,
            self.placement_fence,
            self.next_sequence,
        )?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| Status::data_loss("index source snapshot sequence overflow"))?;
        if frame.end {
            if !frame.heads_json.is_empty() {
                return Err(Status::data_loss(
                    "terminal index source snapshot frame contains heads",
                ));
            }
            self.ended = true;
            return Ok(None);
        }
        if frame.heads_json.is_empty() {
            return Err(Status::data_loss(
                "non-terminal index source snapshot frame is empty",
            ));
        }
        let heads = frame
            .heads_json
            .iter()
            .map(|encoded| decode_json::<IndexSourceSnapshotHead>(encoded))
            .collect::<Result<Vec<_>, _>>()?;
        for head in &heads {
            validate_source_head(head)?;
        }
        require_client_fence(&self.decisions, self.placement_fence, self.target)?;
        Ok(Some(heads))
    }
}

impl ClusterPeerService {
    pub(super) async fn scan_index_source_snapshot_call(
        &self,
        request: Request<tonic::Streaming<wire::IndexSourceSnapshotRequest>>,
    ) -> Result<Response<IndexSourceSnapshotRpcStream>, Status> {
        let started = tokio::time::Instant::now();
        let pin = request
            .extensions()
            .get::<PeerSpkiSha256>()
            .copied()
            .ok_or_else(|| Status::unauthenticated("peer mTLS identity is missing"))?;
        let mut inbound = request.into_inner();
        let first = tokio::time::timeout(MAX_INDEX_SOURCE_SNAPSHOT_TIME, inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("index snapshot begin deadline exceeded"))??
            .ok_or_else(|| Status::invalid_argument("index snapshot begin command is required"))?;
        let begin = match first.command {
            Some(wire::index_source_snapshot_request::Command::Begin(begin)) => begin,
            Some(wire::index_source_snapshot_request::Command::Pull(_)) | None => {
                return Err(Status::invalid_argument(
                    "the first index snapshot command must be begin",
                ));
            }
        };
        let admitted = self.admit_pin_with_timeout_limit(
            pin,
            begin
                .peer
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("peer context is required"))?,
            0,
            MAX_INDEX_SOURCE_SNAPSHOT_TIME,
        )?;
        if begin.tenant_id == 0 || begin.bucket_id == 0 || !valid_source_prefix(&begin.path_prefix)
        {
            return Err(Status::invalid_argument(
                "index source snapshot stable IDs or path prefix are invalid",
            ));
        }
        require_snapshot_frame_bound(begin.max_frame_bytes)?;
        let tenant_id = begin.tenant_id;
        let bucket_id = begin.bucket_id;
        let path_prefix = begin.path_prefix;
        let max_frame_bytes = begin.max_frame_bytes;
        let fence = admitted.placement.fence();
        let placement = admitted.placement;
        let local_node = self.local_node;
        let include_prefix = path_prefix.clone();
        let include = move |snapshot: &CurrentObjectSnapshot| {
            !snapshot.head.deleted
                && path_matches_prefix(&snapshot.exact_path, &include_prefix)
                && !contains_reserved_segment(&snapshot.exact_path)
                && object_coordinator(
                    &placement,
                    snapshot.tenant_id,
                    snapshot.bucket_id,
                    &snapshot.exact_path,
                ) == Some(local_node)
        };
        let deadline = started + admitted.timeout;
        let mut scan = tokio::time::timeout_at(
            deadline,
            self.store.start_current_head_snapshot_scan(
                tenant_id,
                bucket_id,
                &path_prefix,
                INDEX_SOURCE_FRAME_MAX_RECORDS,
                max_frame_bytes,
                include,
            ),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("index source snapshot capture deadline exceeded"))?
        .map_err(|error| Status::internal(error.to_string()))?;
        self.require_unchanged(fence)?;
        let source = scan.source();
        let captured_tail = scan.captured_tail();
        let service = self.clone();

        let output = async_stream::try_stream! {
            yield begun_response(source, captured_tail, fence);
            let mut sequence = 0_u64;
            loop {
                let request = match tokio::time::timeout_at(deadline, inbound.message()).await {
                    Ok(Ok(Some(request))) => request,
                    Ok(Ok(None)) => break,
                    Ok(Err(error)) => Err(error)?,
                    Err(_) => Err(Status::deadline_exceeded(
                        "index source snapshot deadline exceeded",
                    ))?,
                };
                let pull = match request.command {
                    Some(wire::index_source_snapshot_request::Command::Pull(pull))
                        if pull.sequence == sequence => pull,
                    Some(wire::index_source_snapshot_request::Command::Pull(_)) => {
                        Err(Status::data_loss(
                            "index source snapshot pull sequence is not contiguous",
                        ))?
                    }
                    Some(wire::index_source_snapshot_request::Command::Begin(_)) | None => {
                        Err(Status::invalid_argument(
                            "index source snapshot accepts exactly one begin command",
                        ))?
                    }
                };
                let _ = pull;
                service.require_unchanged(fence)?;
                let next = tokio::time::timeout_at(deadline, scan.next_frame())
                    .await
                    .map_err(|_| Status::deadline_exceeded(
                        "index source snapshot deadline exceeded",
                    ))?
                    .map_err(|error| Status::internal(error.to_string()))?;
                service.require_unchanged(fence)?;
                let (heads_json, end) = match next {
                    Some(frame) => (
                        frame
                            .heads
                            .into_iter()
                            .map(IndexSourceSnapshotHead::from)
                            .map(|head| encode_json(&head))
                            .collect::<Result<Vec<_>, _>>()?,
                        false,
                    ),
                    None => (Vec::new(), true),
                };
                yield frame_response(snapshot_frame(
                    source,
                    captured_tail,
                    fence,
                    sequence,
                    heads_json,
                    end,
                ));
                sequence = sequence.checked_add(1).ok_or_else(|| {
                    Status::internal("index source snapshot sequence overflow")
                })?;
                if end {
                    break;
                }
            }
        };
        Ok(Response::new(Box::pin(output)))
    }
}

pub(super) fn require_snapshot_frame_bound(max_frame_bytes: u64) -> Result<(), Status> {
    if max_frame_bytes == 0 || max_frame_bytes > INDEX_SOURCE_FRAME_MAX_BYTES {
        return Err(Status::invalid_argument(
            "index source snapshot frame bound is invalid",
        ));
    }
    Ok(())
}

fn begun_response(
    source: SourceId,
    captured_tail: u64,
    fence: PlacementLogId,
) -> wire::IndexSourceSnapshotResponse {
    wire::IndexSourceSnapshotResponse {
        event: Some(wire::index_source_snapshot_response::Event::Begun(
            wire::IndexSourceSnapshotBegun {
                schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                source_node_id: u64::from(source.node_id),
                source_epoch: source.source_epoch.to_vec(),
                captured_source_tail: captured_tail,
                placement_term: fence.term,
                placement_index: fence.index,
            },
        )),
    }
}

fn frame_response(frame: wire::IndexSourceSnapshotFrame) -> wire::IndexSourceSnapshotResponse {
    wire::IndexSourceSnapshotResponse {
        event: Some(wire::index_source_snapshot_response::Event::Frame(frame)),
    }
}

fn snapshot_frame(
    source: SourceId,
    captured_tail: u64,
    fence: PlacementLogId,
    sequence: u64,
    heads_json: Vec<Vec<u8>>,
    end: bool,
) -> wire::IndexSourceSnapshotFrame {
    wire::IndexSourceSnapshotFrame {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        source_node_id: u64::from(source.node_id),
        source_epoch: source.source_epoch.to_vec(),
        captured_source_tail: captured_tail,
        placement_term: fence.term,
        placement_index: fence.index,
        sequence,
        heads_json,
        end,
    }
}

fn validate_frame_identity(
    frame: &wire::IndexSourceSnapshotFrame,
    target: NodeId,
    source: SourceId,
    captured_tail: u64,
    fence: PlacementLogId,
    expected_sequence: u64,
) -> Result<(), Status> {
    require_response_schema(frame.schema_version)?;
    if frame.source_node_id != target.0
        || frame.source_node_id != u64::from(source.node_id)
        || frame.source_epoch != source.source_epoch
        || frame.captured_source_tail != captured_tail
        || frame.placement_term != fence.term
        || frame.placement_index != fence.index
        || frame.sequence != expected_sequence
    {
        return Err(Status::data_loss(
            "index source snapshot identity, checkpoint, fence, or sequence changed",
        ));
    }
    Ok(())
}

fn validate_source_head(head: &IndexSourceSnapshotHead) -> Result<(), Status> {
    CurrentObjectSnapshot {
        tenant_id: head.tenant_id,
        bucket_id: head.bucket_id,
        exact_path: head.exact_path.clone(),
        head: head.head.clone(),
        version: head.version.clone(),
    }
    .validate()
    .map_err(|error| Status::data_loss(error.to_string()))
}

fn require_client_fence(
    decisions: &DecisionRaft,
    expected: PlacementLogId,
    target: NodeId,
) -> Result<(), Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
    let placement = ClusterPlacement::from_applied(&state)
        .map_err(|error| Status::unavailable(error.to_string()))?;
    if placement.fence() != expected || !placement.active_node_ids().contains(&target) {
        return Err(Status::unavailable(
            "cluster placement changed during index source snapshot",
        ));
    }
    Ok(())
}

pub(super) fn open_client_snapshot(
    target: NodeId,
    decisions: DecisionRaft,
    fence: PlacementLogId,
    requests: mpsc::Sender<wire::IndexSourceSnapshotRequest>,
    stream: tonic::Streaming<wire::IndexSourceSnapshotResponse>,
    begun: wire::IndexSourceSnapshotBegun,
    deadline: tokio::time::Instant,
) -> Result<IndexSourceSnapshot, Status> {
    require_response_schema(begun.schema_version)?;
    if begun.source_node_id != target.0
        || begun.placement_term != fence.term
        || begun.placement_index != fence.index
    {
        return Err(Status::data_loss(
            "index source snapshot acknowledgement has the wrong source or fence",
        ));
    }
    let source_epoch: [u8; 32] = begun
        .source_epoch
        .as_slice()
        .try_into()
        .map_err(|_| Status::data_loss("index source epoch has the wrong length"))?;
    if source_epoch == [0; 32] {
        return Err(Status::data_loss("index source epoch is all zero"));
    }
    let source_node = u16::try_from(begun.source_node_id)
        .map_err(|_| Status::data_loss("index source node exceeds u16"))?;
    let source = SourceId {
        node_id: source_node,
        source_epoch,
    };
    require_client_fence(&decisions, fence, target)?;
    Ok(IndexSourceSnapshot {
        source,
        captured_tail: begun.captured_source_tail,
        placement_fence: fence,
        target,
        decisions,
        requests,
        stream,
        next_sequence: 0,
        deadline,
        ended: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changing_frame_checkpoint_or_fence_is_rejected() {
        let source = SourceId {
            node_id: 2,
            source_epoch: [3; 32],
        };
        let fence = PlacementLogId { term: 4, index: 5 };
        let mut frame = snapshot_frame(source, 8, fence, 0, Vec::new(), true);
        validate_frame_identity(&frame, NodeId(2), source, 8, fence, 0).unwrap();
        frame.captured_source_tail = 9;
        assert!(validate_frame_identity(&frame, NodeId(2), source, 8, fence, 0).is_err());
        frame.captured_source_tail = 8;
        frame.placement_index = 6;
        assert!(validate_frame_identity(&frame, NodeId(2), source, 8, fence, 0).is_err());
    }

    #[test]
    fn request_scoped_frame_bound_accepts_smaller_caps_and_rejects_invalid_ones() {
        assert!(require_snapshot_frame_bound(1024 * 1024).is_ok());
        assert!(require_snapshot_frame_bound(0).is_err());
        assert!(require_snapshot_frame_bound(INDEX_SOURCE_FRAME_MAX_BYTES + 1).is_err());
    }

    #[test]
    fn begun_ack_contains_no_data_frame() {
        let response = begun_response(
            SourceId {
                node_id: 2,
                source_epoch: [3; 32],
            },
            8,
            PlacementLogId { term: 4, index: 5 },
        );
        assert!(matches!(
            response.event,
            Some(wire::index_source_snapshot_response::Event::Begun(_))
        ));
    }
}
