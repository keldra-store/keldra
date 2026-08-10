//! Ordered, crash-safe delivery of logical content-reference effects.
//!
//! Each coordinator's existing source journal is the only authority. This
//! module expands its logical blob deltas into current physical placement,
//! advances every ACTIVE destination through contiguous prefixes, and lets the
//! store compact only through the minimum durable destination cursor.

pub(crate) mod cleanup;
mod runtime;

pub(crate) use runtime::{ReferenceRuntime, ReferenceRuntimeHandle};

use std::collections::BTreeMap;
use std::sync::Arc;

use anvil_consensus::{ClusterId, NodeId};
use anvil_store::{
    BlobRef, DestinationReferenceArtifact, DestinationReferenceDelta, ErasureProfile, LocalChange,
    MAX_LOCAL_INVALIDATION_SCAN_RECORDS, PlacementLogId, ReferenceDeltaApplied,
    ReferenceDeltaBatch, ReferenceProof, ShardIdentity, SourceId, Store,
};
use thiserror::Error;

use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::payload_placement::{PayloadPlacement, select_payload_placement};
use crate::placement::{PlacementKind, PlacementNode};

/// Result of authoritative metadata-quorum reconciliation for one local event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReferenceCommitDisposition {
    /// The event selected the committed head, or is proven to be its ancestor.
    CommittedOrAncestor,
    /// A legacy one-node event applied its reference effects in the same local
    /// RocksDB batch as the journal append. Advancing the destination cursor
    /// must not replay those effects.
    AlreadyAppliedLocally,
}

/// Exact-path quorum proof. Missing lineage must return an error; delivery may
/// never infer ancestry from Snowflake ordering or retained descriptors alone.
#[tonic::async_trait]
pub(crate) trait ReferenceCommitAuthority: Send + Sync {
    async fn classify(
        &self,
        source: SourceId,
        change: &LocalChange,
    ) -> Result<ReferenceCommitDisposition, String>;
}

/// Exact typed request made to one current object-metadata replica.
///
/// The path and fence let the eventual peer endpoint reject a stale or
/// misrouted read. The response remains the stored proof (or exact absence),
/// rather than accepting a caller-provided expected value as evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReferenceProofRead {
    pub placement_fence: PlacementLogId,
    pub source: SourceId,
    pub offset: u64,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
}

#[tonic::async_trait]
pub(crate) trait ReferenceProofPeers: Send + Sync {
    async fn read_reference_proof(
        &self,
        node: NodeId,
        address: &str,
        request: ReferenceProofRead,
    ) -> Result<Option<ReferenceProof>, String>;
}

pub(crate) trait ReferencePlacementAuthority: Send + Sync {
    fn current(&self) -> Result<ReferencePlacement, String>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReferencePlacement {
    cluster_id: ClusterId,
    fence: anvil_store::PlacementLogId,
    nodes: Vec<PlacementNode>,
    addresses: BTreeMap<NodeId, String>,
}

impl ReferencePlacement {
    fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    fn fence(&self) -> anvil_store::PlacementLogId {
        self.fence
    }

    fn placement_nodes(&self) -> &[PlacementNode] {
        &self.nodes
    }

    fn active_node_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|node| node.node_id()).collect()
    }

    fn address(&self, node: NodeId) -> Option<&str> {
        self.addresses.get(&node).map(String::as_str)
    }
}

impl crate::payload_distribution::PayloadPlacementView for ReferencePlacement {
    fn cluster_id(&self) -> ClusterId {
        self.cluster_id()
    }

    fn fence(&self) -> anvil_store::PlacementLogId {
        self.fence()
    }

    fn placement_nodes(&self) -> &[PlacementNode] {
        self.placement_nodes()
    }

    fn active_node_ids(&self) -> Vec<NodeId> {
        self.active_node_ids()
    }

    fn address(&self, node: NodeId) -> Option<&str> {
        self.address(node)
    }
}

/// Proves one source-journal event from its current fixed metadata quorum.
///
/// The source's own proof supplies the exact value being reconciled. Current
/// object placement then determines the complete replica group to read. This
/// deliberately makes no version or lineage inference. Exact equality can
/// prove commitment; exact absence is only an observation and cannot prove
/// that a different candidate committed.
pub(crate) struct QuorumReferenceCommitAuthority {
    source: Store,
    placement: Arc<dyn ReferencePlacementAuthority>,
    peers: Arc<dyn ReferenceProofPeers>,
}

impl QuorumReferenceCommitAuthority {
    pub(crate) fn new(
        source: Store,
        placement: Arc<dyn ReferencePlacementAuthority>,
        peers: Arc<dyn ReferenceProofPeers>,
    ) -> Self {
        Self {
            source,
            placement,
            peers,
        }
    }
}

#[tonic::async_trait]
impl ReferenceCommitAuthority for QuorumReferenceCommitAuthority {
    async fn classify(
        &self,
        source: SourceId,
        change: &LocalChange,
    ) -> Result<ReferenceCommitDisposition, String> {
        let started = self
            .placement
            .current()
            .map_err(|error| format!("current placement is unavailable: {error}"))?;
        let expected = self
            .source
            .read_reference_proof(source, change.offset())
            .map_err(|error| format!("source reference proof is unavailable: {error}"))?;
        let Some(expected) = expected else {
            let local_source = self
                .source
                .local_watch_status()
                .map_err(|error| format!("local source identity is unavailable: {error}"))?
                .source_id;
            let only_active_source = started.active_node_ids().as_slice()
                == [NodeId(u64::from(source.node_id))]
                && local_source == source;
            if only_active_source && matches!(change, LocalChange::ObjectHead(_)) {
                return Ok(ReferenceCommitDisposition::AlreadyAppliedLocally);
            }
            return Err("source reference proof is missing".to_owned());
        };
        if expected.source_id != source
            || expected.offset() != change.offset()
            || expected.change != *change
        {
            return Err("source reference proof does not match its journal event".into());
        }

        let path = reference_object_path(change)?;
        let placement_key = object_placement_key(path.tenant_id, path.bucket_id, path.exact_path);
        let group = MutableRecordReplicaGroup::select(
            PlacementKind::Object,
            started.cluster_id(),
            &placement_key,
            started.placement_nodes(),
        )
        .ok_or_else(|| "current placement has no object metadata replica group".to_owned())?;
        let request = ReferenceProofRead {
            placement_fence: started.fence(),
            source,
            offset: change.offset(),
            tenant_id: path.tenant_id,
            bucket_id: path.bucket_id,
            exact_path: path.exact_path.to_owned(),
        };
        let local_node = NodeId(u64::from(source.node_id));
        let mut exact = 0_usize;
        let mut absent = 0_usize;
        let mut unavailable = Vec::new();
        let mut conflict = None;

        for replica in group.replicas() {
            let observed = if *replica == local_node {
                self.source
                    .read_reference_proof(source, change.offset())
                    .map_err(|error| error.to_string())
            } else if let Some(address) = started.address(*replica) {
                self.peers
                    .read_reference_proof(*replica, address, request.clone())
                    .await
            } else {
                Err("current replica has no peer address".into())
            };
            match observed {
                Ok(Some(proof)) if proof == expected => exact += 1,
                Ok(Some(_)) => {
                    conflict = Some(*replica);
                }
                Ok(None) => absent += 1,
                Err(error) => unavailable.push(format!("node {}: {error}", replica.0)),
            }
        }

        let current = self
            .placement
            .current()
            .map_err(|error| format!("current placement recheck failed: {error}"))?;
        if current != started {
            return Err("object placement changed during reference-proof read".into());
        }
        if let Some(node) = conflict {
            return Err(format!(
                "node {} returned a conflicting proof for the same source position",
                node.0
            ));
        }

        let quorum = group.required_acknowledgements();
        if exact >= quorum {
            return Ok(ReferenceCommitDisposition::CommittedOrAncestor);
        }
        if exact + absent < quorum {
            return Err(format!(
                "reference-proof read reached {} of {quorum} required replicas ({})",
                exact + absent,
                if unavailable.is_empty() {
                    "no successful observations".to_owned()
                } else {
                    unavailable.join(", ")
                }
            ));
        }
        Err(format!(
            "reference-proof quorum is unresolved between {exact} exact and {absent} absent replicas"
        ))
    }
}

struct ReferenceObjectPath<'a> {
    tenant_id: u64,
    bucket_id: u64,
    exact_path: &'a str,
}

fn reference_object_path(change: &LocalChange) -> Result<ReferenceObjectPath<'_>, String> {
    match change {
        LocalChange::ObjectHead(change) => Ok(ReferenceObjectPath {
            tenant_id: change.tenant_id,
            bucket_id: change.bucket_id,
            exact_path: &change.exact_path,
        }),
        LocalChange::RetainedVersionDeleted(change) => Ok(ReferenceObjectPath {
            tenant_id: change.tenant_id,
            bucket_id: change.bucket_id,
            exact_path: &change.exact_path,
        }),
        _ => Err("source change has no supported exact object placement key".into()),
    }
}

fn object_placement_key(tenant_id: u64, bucket_id: u64, path: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + path.len());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(path.as_bytes());
    key
}

#[tonic::async_trait]
pub(crate) trait ReferenceDestinations: Send + Sync {
    async fn cursor(&self, node: NodeId, address: &str, source: SourceId) -> Result<u64, String>;

    async fn apply(
        &self,
        node: NodeId,
        address: &str,
        batch: ReferenceDeltaBatch,
    ) -> Result<ReferenceDeltaApplied, String>;
}

/// Installs every current final artifact before a positive effect is sent.
#[tonic::async_trait]
pub(crate) trait PositiveReferencePreparation: Send + Sync {
    async fn prepare(&self, placement: &ReferencePlacement, blob: &BlobRef) -> Result<(), String>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReferenceDeliveryProgress {
    pub source_id: SourceId,
    pub safe_through: u64,
    pub tail: u64,
}

pub(crate) struct ReferenceDelivery {
    source: Store,
    placement: Arc<dyn ReferencePlacementAuthority>,
    commits: Arc<dyn ReferenceCommitAuthority>,
    destinations: Arc<dyn ReferenceDestinations>,
    payloads: Arc<dyn PositiveReferencePreparation>,
    profile: ErasureProfile,
    page_size: usize,
}

impl ReferenceDelivery {
    pub(crate) fn new(
        source: Store,
        placement: Arc<dyn ReferencePlacementAuthority>,
        commits: Arc<dyn ReferenceCommitAuthority>,
        destinations: Arc<dyn ReferenceDestinations>,
        payloads: Arc<dyn PositiveReferencePreparation>,
        profile: ErasureProfile,
    ) -> Self {
        Self {
            source,
            placement,
            commits,
            destinations,
            payloads,
            profile,
            page_size: MAX_LOCAL_INVALIDATION_SCAN_RECORDS,
        }
    }

    #[cfg(test)]
    fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size.clamp(1, MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        self
    }

    /// Delivers at most one bounded page from the slowest current destination.
    pub(crate) async fn deliver_once(
        &self,
    ) -> Result<ReferenceDeliveryProgress, ReferenceDeliveryError> {
        let placement = self
            .placement
            .current()
            .map_err(ReferenceDeliveryError::Placement)?;
        let initial_status = self
            .source
            .local_watch_status()
            .map_err(|error| ReferenceDeliveryError::Source(error.to_string()))?;
        let active = active_destinations(&placement)?;
        if !active
            .iter()
            .any(|destination| destination.node.0 == u64::from(initial_status.source_id.node_id))
        {
            return Err(ReferenceDeliveryError::SourceNotActive {
                source_id: initial_status.source_id,
            });
        }

        let mut cursors = BTreeMap::new();
        for destination in &active {
            let cursor = self
                .destinations
                .cursor(
                    destination.node,
                    &destination.address,
                    initial_status.source_id,
                )
                .await
                .map_err(|message| ReferenceDeliveryError::Destination {
                    node: destination.node,
                    message,
                })?;
            cursors.insert(destination.node, cursor);
        }

        // Cursor reads may await peer RPCs while this source appends another
        // event. Validate the observations against a status captured after
        // those awaits, not against the stale status from before them.
        let status = self
            .source
            .local_watch_status()
            .map_err(|error| ReferenceDeliveryError::Source(error.to_string()))?;
        if status.source_id != initial_status.source_id {
            return Err(ReferenceDeliveryError::Source(
                "source journal identity changed during reference cursor reads".into(),
            ));
        }
        for (node, cursor) in &cursors {
            validate_cursor(*node, *cursor, status.retention_floor, status.tail)?;
        }
        let slowest = cursors.values().copied().min().unwrap_or(status.tail);
        if slowest == status.tail {
            let final_tail = self
                .finish_compaction(&placement, status.source_id, &cursors)
                .await?;
            return Ok(ReferenceDeliveryProgress {
                source_id: status.source_id,
                safe_through: status.tail,
                tail: final_tail,
            });
        }

        let changes = self
            .source
            .scan_local_changes(slowest, self.page_size)
            .map_err(|error| ReferenceDeliveryError::Source(error.to_string()))?
            .into_iter()
            .take_while(|change| change.offset() <= status.tail);
        let mut routed = Vec::with_capacity(self.page_size);
        let mut prepared = Vec::new();
        let mut blocked = None;
        for change in changes {
            if change.reference_deltas().is_empty() {
                routed.push((change.offset(), BTreeMap::new()));
                continue;
            }
            let disposition = match self.commits.classify(status.source_id, &change).await {
                Ok(disposition) => disposition,
                Err(message) => {
                    blocked = Some(ReferenceDeliveryError::CommitProof {
                        offset: change.offset(),
                        message,
                    });
                    break;
                }
            };
            if disposition == ReferenceCommitDisposition::AlreadyAppliedLocally {
                routed.push((change.offset(), BTreeMap::new()));
                continue;
            }
            for delta in change
                .reference_deltas()
                .iter()
                .filter(|delta| delta.change > 0)
            {
                if !prepared.contains(&delta.blob)
                    && let Err(message) = self.payloads.prepare(&placement, &delta.blob).await
                {
                    blocked = Some(ReferenceDeliveryError::PayloadPreparation {
                        offset: change.offset(),
                        message,
                    });
                    break;
                }
                if !prepared.contains(&delta.blob) {
                    prepared.push(delta.blob.clone());
                }
            }
            if blocked.is_some() {
                break;
            }
            routed.push((
                change.offset(),
                route_effects(&placement, self.profile, change.reference_deltas()),
            ));
        }

        let through = routed.last().map_or(slowest, |(offset, _)| *offset);
        if through > slowest {
            let mut first_failure = None;
            for destination in &active {
                let after = cursors[&destination.node];
                if after >= through {
                    continue;
                }
                let deltas = routed
                    .iter()
                    .filter(|(offset, _)| *offset > after)
                    .flat_map(|(_, by_node)| {
                        by_node
                            .get(&destination.node)
                            .into_iter()
                            .flatten()
                            .cloned()
                    })
                    .collect();
                let batch = ReferenceDeltaBatch {
                    source: status.source_id,
                    after,
                    through,
                    deltas,
                };
                match self
                    .destinations
                    .apply(destination.node, &destination.address, batch)
                    .await
                {
                    Ok(applied) if applied.through >= through => {
                        cursors.insert(destination.node, applied.through);
                    }
                    Ok(applied) => {
                        first_failure.get_or_insert_with(|| ReferenceDeliveryError::Destination {
                            node: destination.node,
                            message: format!(
                                "destination advanced only through {}, expected {through}",
                                applied.through
                            ),
                        });
                    }
                    Err(message) => {
                        first_failure.get_or_insert(ReferenceDeliveryError::Destination {
                            node: destination.node,
                            message,
                        });
                    }
                }
            }
            self.finish_compaction(&placement, status.source_id, &cursors)
                .await?;
            if let Some(error) = first_failure {
                return Err(error);
            }
        }
        if let Some(error) = blocked {
            return Err(error);
        }
        let final_tail = self
            .source
            .local_watch_status()
            .map_err(|error| ReferenceDeliveryError::Source(error.to_string()))?;
        if final_tail.source_id != status.source_id {
            return Err(ReferenceDeliveryError::Source(
                "source journal identity changed during reference delivery".into(),
            ));
        }
        Ok(ReferenceDeliveryProgress {
            source_id: status.source_id,
            safe_through: cursors.values().copied().min().unwrap_or(through),
            tail: final_tail.tail,
        })
    }

    async fn finish_compaction(
        &self,
        started: &ReferencePlacement,
        source: SourceId,
        cursors: &BTreeMap<NodeId, u64>,
    ) -> Result<u64, ReferenceDeliveryError> {
        let current = self
            .placement
            .current()
            .map_err(ReferenceDeliveryError::Placement)?;
        if current.fence() != started.fence()
            || current.active_node_ids() != started.active_node_ids()
        {
            return Err(ReferenceDeliveryError::PlacementChanged);
        }
        let status = self
            .source
            .local_watch_status()
            .map_err(|error| ReferenceDeliveryError::Source(error.to_string()))?;
        if status.source_id != source {
            return Err(ReferenceDeliveryError::Source(
                "source journal identity changed before reference compaction".into(),
            ));
        }
        let safe = cursors.values().copied().min().unwrap_or(status.tail);
        if safe > status.tail {
            return Err(ReferenceDeliveryError::CursorAhead {
                node: NodeId(u64::from(source.node_id)),
                cursor: safe,
                tail: status.tail,
            });
        }
        self.source
            .advance_source_journal_safe_through(safe)
            .await
            .map_err(|error| ReferenceDeliveryError::Source(error.to_string()))?;
        Ok(status.tail)
    }
}

#[derive(Clone, Debug)]
struct ActiveDestination {
    node: NodeId,
    address: String,
}

fn active_destinations(
    placement: &ReferencePlacement,
) -> Result<Vec<ActiveDestination>, ReferenceDeliveryError> {
    placement
        .active_node_ids()
        .into_iter()
        .map(|node| {
            placement
                .address(node)
                .map(|address| ActiveDestination {
                    node,
                    address: address.to_owned(),
                })
                .ok_or(ReferenceDeliveryError::MissingAddress { node })
        })
        .collect()
}

fn validate_cursor(
    node: NodeId,
    cursor: u64,
    floor: u64,
    tail: u64,
) -> Result<(), ReferenceDeliveryError> {
    if cursor < floor {
        return Err(ReferenceDeliveryError::JournalGap {
            node,
            cursor,
            floor,
        });
    }
    if cursor > tail {
        return Err(ReferenceDeliveryError::CursorAhead { node, cursor, tail });
    }
    Ok(())
}

fn route_effects(
    placement: &ReferencePlacement,
    profile: ErasureProfile,
    deltas: &[anvil_store::ReferenceDelta],
) -> BTreeMap<NodeId, Vec<DestinationReferenceDelta>> {
    let mut routed = BTreeMap::<NodeId, Vec<DestinationReferenceDelta>>::new();
    for delta in deltas {
        match select_payload_placement(
            placement.cluster_id(),
            &delta.blob,
            profile,
            placement.placement_nodes(),
        ) {
            PayloadPlacement::Small(selected) => {
                for owner in selected.owners() {
                    routed
                        .entry(*owner)
                        .or_default()
                        .push(DestinationReferenceDelta {
                            artifact: DestinationReferenceArtifact::CompleteBlob(
                                delta.blob.clone(),
                            ),
                            change: delta.change,
                        });
                }
            }
            PayloadPlacement::LargeComplete(selected) => {
                for owner in selected.owners() {
                    routed
                        .entry(*owner)
                        .or_default()
                        .push(DestinationReferenceDelta {
                            artifact: DestinationReferenceArtifact::CompleteBlob(
                                delta.blob.clone(),
                            ),
                            change: delta.change,
                        });
                }
            }
            PayloadPlacement::Large(selected) => {
                for shard in selected.shards() {
                    routed
                        .entry(shard.owner())
                        .or_default()
                        .push(DestinationReferenceDelta {
                            artifact: DestinationReferenceArtifact::Shard(ShardIdentity::new(
                                delta.blob.clone(),
                                shard.ordinal(),
                            )),
                            change: delta.change,
                        });
                }
            }
        }
    }
    routed
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum ReferenceDeliveryError {
    #[error("current placement is unavailable: {0}")]
    Placement(String),
    #[error("source journal is unavailable: {0}")]
    Source(String),
    #[error("source journal {source_id:?} does not belong to a current ACTIVE node")]
    SourceNotActive { source_id: SourceId },
    #[error("ACTIVE destination {node:?} has no peer address")]
    MissingAddress { node: NodeId },
    #[error("destination {node:?} failed reference delivery: {message}")]
    Destination { node: NodeId, message: String },
    #[error("destination {node:?} cursor {cursor} is below retained source floor {floor}")]
    JournalGap {
        node: NodeId,
        cursor: u64,
        floor: u64,
    },
    #[error("destination {node:?} cursor {cursor} is beyond source tail {tail}")]
    CursorAhead {
        node: NodeId,
        cursor: u64,
        tail: u64,
    },
    #[error("metadata quorum cannot prove source event {offset}: {message}")]
    CommitProof { offset: u64, message: String },
    #[error("payload for positive source event {offset} is not ready: {message}")]
    PayloadPreparation { offset: u64, message: String },
    #[error("ACTIVE placement changed during reference delivery")]
    PlacementChanged,
}

#[cfg(test)]
mod tests;
