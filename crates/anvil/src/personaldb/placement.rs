use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::PlacementLogId;
use personaldb_core::{DatabaseId, PlacementEpoch, ServerId};
use personaldb_server::{PrimaryAssignment, PrimaryResolver, ServerError};

use crate::cluster_placement::ClusterPlacement;
use crate::placement::PlacementKind;

use super::scope::{PersonalDbGroupScope, PersonalDbStorageId};

const POLICY_SOURCE: &str = "anvil_weighted_hrw_from_applied_membership";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PersonalDbPrimary {
    pub(crate) node_id: NodeId,
    pub(crate) peer_address: String,
    pub(crate) fence: PlacementLogId,
    pub(crate) assignment: PrimaryAssignment,
}

#[derive(Clone)]
pub(crate) struct HrwPrimaryResolver {
    decisions: DecisionRaft,
}

impl HrwPrimaryResolver {
    pub(crate) fn new(decisions: DecisionRaft) -> Self {
        Self { decisions }
    }

    pub(crate) fn current(
        &self,
        scope: &PersonalDbGroupScope,
    ) -> Result<PersonalDbPrimary, ServerError> {
        let database_id = &scope.database_id;
        let state = self
            .decisions
            .state()
            .map_err(|_| placement_unavailable(database_id))?;
        let placement = ClusterPlacement::from_applied(&state)
            .map_err(|_| placement_unavailable(database_id))?;
        let node_id = placement
            .rank(PlacementKind::FuturePersonalDb, &scope.placement_key())
            .into_iter()
            .next()
            .ok_or_else(|| placement_unavailable(database_id))?;
        let fence = placement.fence();
        let peer_address = placement
            .address(node_id)
            .ok_or_else(|| placement_unavailable(database_id))?
            .0
            .clone();
        Ok(PersonalDbPrimary {
            node_id,
            peer_address,
            fence,
            assignment: PrimaryAssignment {
                database_id: database_id.clone(),
                primary_server_id: server_id(node_id, scope.storage),
                // Peer addresses belong to the mTLS network. Public requests
                // are proxied internally instead of leaking that endpoint.
                primary_server_endpoint: None,
                placement_epoch: PlacementEpoch(fence.index),
                expires_at_unix_millis: None,
                policy_source: POLICY_SOURCE.to_string(),
            },
        })
    }

    pub(crate) fn scoped(&self, storage: PersonalDbStorageId) -> ScopedHrwPrimaryResolver {
        ScopedHrwPrimaryResolver {
            resolver: self.clone(),
            storage,
        }
    }
}

/// Adapter required by PersonalDB's canonical resolver trait. One adapter is
/// installed on the upstream server for a stable tenant/bucket storage scope,
/// so every database ID is ranked with that scope without changing the ID.
#[derive(Clone)]
pub(crate) struct ScopedHrwPrimaryResolver {
    resolver: HrwPrimaryResolver,
    storage: PersonalDbStorageId,
}

impl ScopedHrwPrimaryResolver {
    pub(crate) fn current(
        &self,
        database_id: DatabaseId,
    ) -> Result<PersonalDbPrimary, ServerError> {
        self.resolver.current(&self.storage.group(database_id))
    }

    pub(crate) fn server_id(&self, node_id: NodeId) -> ServerId {
        server_id(node_id, self.storage)
    }
}

#[tonic::async_trait]
impl PrimaryResolver for ScopedHrwPrimaryResolver {
    async fn primary_for(&self, database_id: DatabaseId) -> Result<PrimaryAssignment, ServerError> {
        Ok(self.current(database_id)?.assignment)
    }
}

fn server_id(node_id: NodeId, storage: PersonalDbStorageId) -> ServerId {
    ServerId::new(format!(
        "anvil-node-{}-personaldb-{}-{}",
        node_id.0, storage.tenant_id, storage.bucket_id
    ))
}

fn placement_unavailable(database_id: &DatabaseId) -> ServerError {
    ServerError::PlacementUnknown {
        database_id: database_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_external_database_id_has_a_distinct_hrw_identity_per_storage_scope() {
        let database_id = DatabaseId::new("shared-id");
        let first = PersonalDbStorageId::new(1, 2).group(database_id.clone());
        let another_tenant = PersonalDbStorageId::new(3, 2).group(database_id.clone());
        let another_bucket = PersonalDbStorageId::new(1, 4).group(database_id);

        assert_ne!(first.placement_key(), another_tenant.placement_key());
        assert_ne!(first.placement_key(), another_bucket.placement_key());
        assert_eq!(&first.placement_key()[1..9], &1_u64.to_be_bytes());
        assert_eq!(&first.placement_key()[9..17], &2_u64.to_be_bytes());
        assert_ne!(
            server_id(NodeId(7), first.storage),
            server_id(NodeId(7), another_tenant.storage)
        );
    }
}
