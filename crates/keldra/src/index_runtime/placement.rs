use keldra_consensus::NodeId;
use keldra_store::SourceId;
use thiserror::Error;

use crate::cluster_placement::ClusterPlacement;
use crate::placement::PlacementKind;

const QUERY_REPLICA_LIMIT: usize = 3;
const PROJECTION_PARTITION_ID: u64 = 1;
const SOURCE_PRODUCER_DOMAIN: &[u8] = b"keldra/v6/source-producer/v1\0";

/// Stable identity used for index placement. Mutable names never participate.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct IndexIdentity {
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
}

impl IndexIdentity {
    pub(crate) fn new(
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<Self, IndexPlacementError> {
        if tenant_id == 0 || bucket_id == 0 || index_id == 0 {
            return Err(IndexPlacementError::ZeroIdentity);
        }
        Ok(Self {
            tenant_id,
            bucket_id,
            index_id,
        })
    }

    /// Placement authority for all physical projections over one source
    /// bucket. Logical and physical index IDs deliberately do not participate:
    /// one assigned source-partition writer must see every shareable recipe.
    pub(crate) fn projection_partition(
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Self, IndexPlacementError> {
        Self::new(tenant_id, bucket_id, PROJECTION_PARTITION_ID)
    }

    fn placement_key(self) -> [u8; 24] {
        let mut key = [0_u8; 24];
        key[..8].copy_from_slice(&self.tenant_id.to_be_bytes());
        key[8..16].copy_from_slice(&self.bucket_id.to_be_bytes());
        key[16..].copy_from_slice(&self.index_id.to_be_bytes());
        key
    }

    pub(crate) const fn tenant_id(self) -> u64 {
        self.tenant_id
    }

    pub(crate) const fn bucket_id(self) -> u64 {
        self.bucket_id
    }

    pub(crate) const fn index_id(self) -> u64 {
        self.index_id
    }
}

/// Derived ownership for one index under one applied membership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexPlacement {
    builder: NodeId,
    query_replicas: Vec<NodeId>,
    fence: keldra_store::PlacementLogId,
}

impl IndexPlacement {
    pub(crate) fn derive(
        identity: IndexIdentity,
        placement: &ClusterPlacement,
    ) -> Result<Self, IndexPlacementError> {
        let ranked = placement.rank(PlacementKind::FutureIndex, &identity.placement_key());
        Self::from_ranked(ranked, placement.fence())
    }

    pub(super) fn from_ranked(
        ranked: Vec<NodeId>,
        fence: keldra_store::PlacementLogId,
    ) -> Result<Self, IndexPlacementError> {
        let builder = ranked
            .first()
            .copied()
            .ok_or(IndexPlacementError::NoActiveNode)?;
        Ok(Self {
            builder,
            query_replicas: ranked.into_iter().take(QUERY_REPLICA_LIMIT).collect(),
            fence,
        })
    }

    pub(crate) const fn builder(&self) -> NodeId {
        self.builder
    }

    pub(crate) fn query_replicas(&self) -> &[NodeId] {
        &self.query_replicas
    }

    pub(crate) const fn fence(&self) -> keldra_store::PlacementLogId {
        self.fence
    }

    pub(crate) fn rank_of(&self, node: NodeId) -> Option<u8> {
        self.query_replicas
            .iter()
            .position(|candidate| *candidate == node)
            .and_then(|rank| u8::try_from(rank).ok())
    }
}

/// Deterministic v6 producer authority for one immutable source incarnation.
/// While the originating source remains ACTIVE it produces locally. Once it
/// leaves placement, capacity-weighted HRW elects one successor from a key
/// that deliberately excludes logical definitions and physical families, so
/// every shared family follows the same source handoff.
pub(crate) fn source_projection_producer(
    tenant_id: u64,
    bucket_id: u64,
    source: SourceId,
    placement: &ClusterPlacement,
) -> Result<NodeId, IndexPlacementError> {
    let source_node = NodeId(u64::from(source.node_id));
    let active = placement.active_node_ids();
    let ranked = placement.rank(
        PlacementKind::FutureIndex,
        &source_producer_key(tenant_id, bucket_id, source),
    );
    select_source_producer(source_node, &active, ranked)
}

fn select_source_producer(
    source_node: NodeId,
    active: &[NodeId],
    ranked_successors: Vec<NodeId>,
) -> Result<NodeId, IndexPlacementError> {
    if active.contains(&source_node) {
        return Ok(source_node);
    }
    ranked_successors
        .into_iter()
        .next()
        .ok_or(IndexPlacementError::NoActiveNode)
}

fn source_producer_key(tenant_id: u64, bucket_id: u64, source: SourceId) -> Vec<u8> {
    let mut key = Vec::with_capacity(SOURCE_PRODUCER_DOMAIN.len() + 8 + 8 + 2 + 32);
    key.extend_from_slice(SOURCE_PRODUCER_DOMAIN);
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(&source.node_id.to_be_bytes());
    key.extend_from_slice(&source.source_epoch);
    key
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum IndexPlacementError {
    #[error("index stable identities must be non-zero")]
    ZeroIdentity,
    #[error("index placement contains no ACTIVE node")]
    NoActiveNode,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Placement construction is already exhaustively tested by
    // cluster_placement. This test freezes only the index-specific identity
    // and top-three projection.
    #[test]
    fn one_builder_and_at_most_three_query_replicas_are_derived() {
        let assignment = IndexPlacement::from_ranked(
            vec![NodeId(4), NodeId(2), NodeId(3), NodeId(1)],
            keldra_store::PlacementLogId { term: 2, index: 9 },
        )
        .unwrap();

        assert_eq!(assignment.query_replicas().len(), 3);
        assert_eq!(assignment.builder(), assignment.query_replicas()[0]);
        assert_eq!(assignment.fence().term, 2);
        assert_eq!(assignment.fence().index, 9);
    }

    #[test]
    fn mutable_or_missing_identifiers_cannot_enter_placement() {
        assert_eq!(
            IndexIdentity::new(0, 1, 1),
            Err(IndexPlacementError::ZeroIdentity)
        );
    }

    #[test]
    fn source_producer_key_is_stable_and_domain_separated() {
        let source = SourceId {
            node_id: 7,
            source_epoch: [3; 32],
        };
        let first = source_producer_key(4, 5, source);
        assert_eq!(first, source_producer_key(4, 5, source));
        assert_ne!(first, source_producer_key(4, 6, source));
        assert_ne!(
            first,
            IndexIdentity::projection_partition(4, 5)
                .unwrap()
                .placement_key()
                .to_vec()
        );
    }

    #[test]
    fn active_source_is_local_regardless_of_active_membership_iteration_order() {
        let source = NodeId(7);
        assert_eq!(
            select_source_producer(source, &[NodeId(2), source, NodeId(9)], vec![NodeId(2)]),
            Ok(source)
        );
        assert_eq!(
            select_source_producer(source, &[NodeId(9), NodeId(2), source], vec![NodeId(9)]),
            Ok(source)
        );
    }

    #[test]
    fn removed_source_hands_off_to_the_ranked_successor() {
        let source = NodeId(7);
        assert_eq!(
            select_source_producer(source, &[NodeId(2), NodeId(9)], vec![NodeId(9), NodeId(2)]),
            Ok(NodeId(9))
        );
        assert_eq!(
            select_source_producer(source, &[NodeId(2)], Vec::new()),
            Err(IndexPlacementError::NoActiveNode)
        );
    }
}
