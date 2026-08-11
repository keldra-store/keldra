use anvil_consensus::NodeId;
use thiserror::Error;

use crate::cluster_placement::ClusterPlacement;
use crate::placement::PlacementKind;

const QUERY_REPLICA_LIMIT: usize = 3;

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
    fence: anvil_store::PlacementLogId,
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
        fence: anvil_store::PlacementLogId,
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

    pub(crate) const fn fence(&self) -> anvil_store::PlacementLogId {
        self.fence
    }

    pub(crate) fn rank_of(&self, node: NodeId) -> Option<u8> {
        self.query_replicas
            .iter()
            .position(|candidate| *candidate == node)
            .and_then(|rank| u8::try_from(rank).ok())
    }
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
            anvil_store::PlacementLogId { term: 2, index: 9 },
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
}
