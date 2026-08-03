//! Restart-safe typed ADD handoff.
//!
//! Progress is intentionally recomputed from the durable Raft transition and
//! typed store state. There is no per-key progress registry in Raft or RocksDB.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use anvil_consensus::{
    ClusterId, DecisionRaft, MembershipTransition, MembershipTransitionKind, NodeDescriptor,
    NodeId, NodeState, SERVING_LEASE_CUTOVER_WAIT, ServingLeaseIssuer,
};
use anvil_store::{
    ErasureProfile, LocalChange, MAX_LOCAL_INVALIDATION_SCAN_RECORDS, PlacementLogId, SourceId,
    Store, WatchJournalStatus,
};
use tonic::Status;

use super::{JoinActivationGate, JoinActivationPermit};
use crate::data_peer::DataPeerTransport;
use crate::payload_read::AnonymousPayloadReadSpools;
use crate::placement::{PlacementKind, PlacementNode, rank_nodes};
use crate::programs::LateBoundProgramQuiescence;

mod merge;
mod payload;
mod records;

#[derive(Clone)]
pub(crate) struct TypedAddHandoff {
    local_node: NodeId,
    decisions: DecisionRaft,
    store: Store,
    peers: DataPeerTransport,
    leases: ServingLeaseIssuer,
    programs: LateBoundProgramQuiescence,
    profile: ErasureProfile,
}

#[derive(Clone, Debug)]
pub(super) struct HandoffEndpoint {
    pub(super) node_id: NodeId,
    pub(super) address: String,
}

#[derive(Clone, Debug)]
pub(super) struct HandoffTopology {
    cluster_id: ClusterId,
    fence: PlacementLogId,
    active: Vec<HandoffEndpoint>,
    old_nodes: Vec<PlacementNode>,
    new_nodes: Vec<PlacementNode>,
    joining: HandoffEndpoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SourceTail {
    source_id: SourceId,
    tail: u64,
    retention_floor: u64,
}

impl TypedAddHandoff {
    pub(crate) fn new(
        local_node: NodeId,
        decisions: DecisionRaft,
        store: Store,
        peers: DataPeerTransport,
        leases: ServingLeaseIssuer,
        programs: LateBoundProgramQuiescence,
        profile: ErasureProfile,
    ) -> Self {
        Self {
            local_node,
            decisions,
            store,
            peers,
            leases,
            programs,
            profile,
        }
    }

    async fn require_current(
        &self,
        descriptor: &NodeDescriptor,
        transition: &MembershipTransition,
    ) -> Result<(), Status> {
        self.decisions
            .confirm_leadership()
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let state = self
            .decisions
            .state()
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if state.cluster_control().transition() != Some(transition)
            || state.cluster_control().nodes().get(&descriptor.node_id) != Some(descriptor)
            || descriptor.state != NodeState::Joining
            || transition.kind != MembershipTransitionKind::Add
            || transition.node_id != descriptor.node_id
        {
            return Err(Status::unavailable(
                "ADD handoff no longer matches the durable transition",
            ));
        }
        if self.decisions.current_leader() != Some(self.local_node.0) {
            return Err(Status::unavailable(
                "ADD handoff node is no longer the Raft leader",
            ));
        }
        Ok(())
    }

    fn topology(
        &self,
        descriptor: &NodeDescriptor,
        transition: &MembershipTransition,
    ) -> Result<HandoffTopology, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if state.cluster_control().transition() != Some(transition) {
            return Err(Status::unavailable("ADD transition changed during handoff"));
        }
        let cluster_id = state
            .cluster_id()
            .ok_or_else(|| Status::failed_precondition("cluster identity is unavailable"))?;
        let placement = state
            .cluster_control()
            .active_placement_log_id()
            .ok_or_else(|| Status::failed_precondition("active placement fence is unavailable"))?;
        let mut active = Vec::new();
        let mut old_nodes = Vec::new();
        for committed in state.cluster_control().nodes().values() {
            if committed.state != NodeState::Active {
                continue;
            }
            let weight = NonZeroU32::new(committed.storage_weight_millionths).ok_or_else(|| {
                Status::data_loss(format!(
                    "ACTIVE node {} has zero weight",
                    committed.node_id.0
                ))
            })?;
            active.push(HandoffEndpoint {
                node_id: committed.node_id,
                address: committed.peer_address.0.clone(),
            });
            old_nodes.push(PlacementNode::new(committed.node_id, weight));
        }
        if active.is_empty() {
            return Err(Status::failed_precondition(
                "ADD handoff has no current ACTIVE source",
            ));
        }
        let joining_weight = NonZeroU32::new(descriptor.storage_weight_millionths)
            .ok_or_else(|| Status::data_loss("JOINING node has zero weight"))?;
        let joining = HandoffEndpoint {
            node_id: descriptor.node_id,
            address: descriptor.peer_address.0.clone(),
        };
        let mut new_nodes = old_nodes.clone();
        new_nodes.push(PlacementNode::new(descriptor.node_id, joining_weight));
        Ok(HandoffTopology {
            cluster_id,
            fence: PlacementLogId {
                term: placement.leader_id.term,
                index: placement.index,
            },
            active,
            old_nodes,
            new_nodes,
            joining,
        })
    }

    async fn journal_tails(
        &self,
        topology: &HandoffTopology,
        peers: &DataPeerTransport,
    ) -> Result<BTreeMap<NodeId, SourceTail>, Status> {
        let mut tails = BTreeMap::new();
        for endpoint in &topology.active {
            let status = peers
                .handoff_source_journal_status(endpoint.node_id, &endpoint.address)
                .await?;
            if u64::from(status.source_id.node_id) != endpoint.node_id.0
                || tails
                    .insert(endpoint.node_id, source_tail(status))
                    .is_some()
            {
                return Err(Status::data_loss(
                    "source journal identity disagrees with ACTIVE membership",
                ));
            }
        }
        Ok(tails)
    }

    async fn changes_between(
        &self,
        topology: &HandoffTopology,
        peers: &DataPeerTransport,
        started: &BTreeMap<NodeId, SourceTail>,
        finished: &BTreeMap<NodeId, SourceTail>,
    ) -> Result<Vec<LocalChange>, Status> {
        let mut changes = Vec::new();
        for endpoint in &topology.active {
            let before = started
                .get(&endpoint.node_id)
                .ok_or_else(|| Status::data_loss("initial source journal is missing"))?;
            let after = finished
                .get(&endpoint.node_id)
                .ok_or_else(|| Status::data_loss("final source journal is missing"))?;
            if before.source_id != after.source_id
                || before.tail > after.tail
                || after.retention_floor > before.tail
            {
                return Err(Status::failed_precondition(
                    "source journal handoff tail expired or changed incarnation",
                ));
            }
            let mut cursor = before.tail;
            while cursor < after.tail {
                let page = peers
                    .read_handoff_source_journal(
                        endpoint.node_id,
                        &endpoint.address,
                        cursor,
                        MAX_LOCAL_INVALIDATION_SCAN_RECORDS,
                    )
                    .await?;
                if page.is_empty() {
                    return Err(Status::data_loss(
                        "source journal ended before its advertised tail",
                    ));
                }
                for change in page {
                    let expected = cursor
                        .checked_add(1)
                        .ok_or_else(|| Status::data_loss("source journal offset overflow"))?;
                    if change.offset() != expected || change.offset() > after.tail {
                        return Err(Status::data_loss(
                            "source journal handoff page is not contiguous",
                        ));
                    }
                    cursor = change.offset();
                    changes.push(change);
                }
            }
        }
        Ok(changes)
    }

    async fn require_reference_cursors(
        &self,
        topology: &HandoffTopology,
        peers: &DataPeerTransport,
        tails: &BTreeMap<NodeId, SourceTail>,
    ) -> Result<(), Status> {
        for destination in &topology.active {
            for tail in tails.values() {
                let cursor = peers
                    .handoff_reference_cursor(
                        destination.node_id,
                        &destination.address,
                        tail.source_id,
                    )
                    .await?;
                if cursor != tail.tail {
                    return Err(Status::unavailable(format!(
                        "node {} reference cursor is {cursor}, expected {} before handoff",
                        destination.node_id.0, tail.tail
                    )));
                }
            }
        }
        Ok(())
    }

    async fn advance_joiner_reference_cursors(
        &self,
        topology: &HandoffTopology,
        peers: &DataPeerTransport,
        tails: &BTreeMap<NodeId, SourceTail>,
    ) -> Result<(), Status> {
        for tail in tails.values() {
            let current = peers
                .handoff_reference_cursor(
                    topology.joining.node_id,
                    &topology.joining.address,
                    tail.source_id,
                )
                .await?;
            if current > tail.tail {
                return Err(Status::data_loss(
                    "JOINING reference cursor is ahead of the source tail",
                ));
            }
            let applied = peers
                .advance_handoff_reference_cursor(
                    topology.joining.node_id,
                    &topology.joining.address,
                    tail.source_id,
                    tail.tail,
                )
                .await?;
            if applied.through != tail.tail {
                return Err(Status::data_loss(
                    "JOINING reference cursor did not reach the final source tail",
                ));
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl JoinActivationGate for TypedAddHandoff {
    async fn ensure_handoff_complete(
        &self,
        descriptor: &NodeDescriptor,
        transition: &MembershipTransition,
    ) -> Result<JoinActivationPermit, Status> {
        self.require_current(descriptor, transition).await?;
        let topology = self.topology(descriptor, transition)?;
        let peers = self
            .peers
            .for_handoff(descriptor.node_id, transition.started_log_index);
        let started = self.journal_tails(&topology, &peers).await?;

        // Pre-copy the largest typed metadata while normal traffic continues.
        records::transfer_all(&topology, &peers).await?;
        let precopy_tail = self.journal_tails(&topology, &peers).await?;
        let changes = self
            .changes_between(&topology, &peers, &started, &precopy_tail)
            .await?;
        records::replay_object_paths(&topology, &peers, &changes).await?;
        self.require_current(descriptor, transition).await?;

        // Linearize the pause after every in-flight old grant has completed,
        // then allow the full two-second authority plus scheduler margin to
        // expire before taking the final stable snapshot.
        let program_quiescence = self.programs.quiesce_for_membership().await?;
        let lease_pause = self.leases.pause_grants().await;
        tokio::time::sleep(SERVING_LEASE_CUTOVER_WAIT).await;
        self.require_current(descriptor, transition).await?;

        let final_tail = self.journal_tails(&topology, &peers).await?;
        self.require_reference_cursors(&topology, &peers, &final_tail)
            .await?;
        records::transfer_all(&topology, &peers).await?;
        let final_changes = self
            .changes_between(&topology, &peers, &started, &final_tail)
            .await?;
        records::replay_object_paths(&topology, &peers, &final_changes).await?;
        payload::transfer_all(
            &topology,
            &peers,
            self.profile,
            Arc::new(AnonymousPayloadReadSpools::new(
                self.store.payload_spool_directory(),
            )),
        )
        .await?;
        self.advance_joiner_reference_cursors(&topology, &peers, &final_tail)
            .await?;

        if self.journal_tails(&topology, &peers).await? != final_tail {
            return Err(Status::unavailable(
                "source journal changed during the final paused handoff",
            ));
        }
        self.require_current(descriptor, transition).await?;
        Ok(JoinActivationPermit::after_handoff(
            lease_pause,
            program_quiescence,
        ))
    }
}

impl HandoffTopology {
    pub(super) fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    pub(super) fn fence(&self) -> PlacementLogId {
        self.fence
    }

    pub(super) fn active(&self) -> &[HandoffEndpoint] {
        &self.active
    }

    pub(super) fn joining(&self) -> &HandoffEndpoint {
        &self.joining
    }

    pub(super) fn discovery_endpoints(&self) -> impl Iterator<Item = &HandoffEndpoint> {
        self.active.iter().chain(std::iter::once(&self.joining))
    }

    pub(super) fn old_replicas(&self, kind: PlacementKind, key: &[u8]) -> Vec<NodeId> {
        ranked(
            kind,
            self.cluster_id,
            key,
            &self.old_nodes,
            self.old_nodes.len().min(3),
        )
    }

    pub(super) fn new_replicas(&self, kind: PlacementKind, key: &[u8]) -> Vec<NodeId> {
        ranked(
            kind,
            self.cluster_id,
            key,
            &self.new_nodes,
            self.new_nodes.len().min(3),
        )
    }

    pub(super) fn old_nodes(&self) -> &[PlacementNode] {
        &self.old_nodes
    }

    pub(super) fn new_nodes(&self) -> &[PlacementNode] {
        &self.new_nodes
    }

    pub(super) fn address(&self, node: NodeId) -> Option<&str> {
        self.discovery_endpoints()
            .find(|endpoint| endpoint.node_id == node)
            .map(|endpoint| endpoint.address.as_str())
    }
}

fn ranked(
    kind: PlacementKind,
    cluster_id: ClusterId,
    key: &[u8],
    nodes: &[PlacementNode],
    count: usize,
) -> Vec<NodeId> {
    rank_nodes(kind, cluster_id, key, nodes)
        .into_iter()
        .take(count)
        .map(PlacementNode::node_id)
        .collect()
}

fn source_tail(status: WatchJournalStatus) -> SourceTail {
    SourceTail {
        source_id: status.source_id,
        tail: status.tail,
        retention_floor: status.retention_floor,
    }
}
