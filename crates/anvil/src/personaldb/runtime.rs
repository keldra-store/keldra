use std::sync::Arc;

use anvil_consensus::NodeId;
use personaldb_core::{
    CommittedEntry, DatabaseId, LeaderLease, PlacementRecord, ProposedLogEntry, ReplicaId, VoterAck,
};
use personaldb_server::{
    AuthContext, JsonTransportCodec, PersonalDbServer, ServerError, SessionContext,
    TransportDelivery, TransportKind, TransportLimits, WireFrame, WriteProposalEnvelope,
};

use crate::serving_fence::ServingAuthority;

use super::group_locks::GroupLocks;
use super::object_store::AnvilPersonalDbObjectStore;
use super::placement::{PersonalDbPrimary, ScopedHrwPrimaryResolver};

/// Serializes one PersonalDB group on its HRW-selected primary while carrying
/// the same applied membership fence through the complete witness operation.
/// It stores no ownership decision and no PersonalDB payload bytes itself.
#[derive(Clone)]
pub(crate) struct PersonalDbRuntime {
    local_node: NodeId,
    resolver: ScopedHrwPrimaryResolver,
    serving: ServingAuthority,
    server: PersonalDbServer,
    object_store: Arc<AnvilPersonalDbObjectStore>,
    group_locks: Arc<GroupLocks>,
}

impl PersonalDbRuntime {
    pub(crate) fn new(
        local_node: NodeId,
        resolver: ScopedHrwPrimaryResolver,
        serving: ServingAuthority,
        server: PersonalDbServer,
        object_store: Arc<AnvilPersonalDbObjectStore>,
    ) -> Result<Self, ServerError> {
        if server.server_id() != &resolver.server_id(local_node) {
            return Err(ServerError::InvalidTraitOutput {
                provider: "anvil_personaldb_runtime",
                message: "server identity does not match the local Anvil node".to_string(),
            });
        }
        Ok(Self {
            local_node,
            resolver,
            serving,
            server,
            object_store,
            group_locks: Arc::new(GroupLocks::default()),
        })
    }

    pub(crate) async fn exchange(
        &self,
        session: SessionContext,
        frame: WireFrame,
        codec: &JsonTransportCodec,
        limits: &TransportLimits,
    ) -> Result<Vec<TransportDelivery>, ServerError> {
        let Some(database_id) = frame.database_group.clone() else {
            return self
                .server
                .handle_wire_frame(session, frame, codec, limits)
                .await;
        };
        let _guard = self.group_locks.acquire(&database_id.0).await;
        let primary = self.prepare_authority(&database_id).await?;
        let deliveries = self
            .server
            .handle_wire_frame(session, frame, codec, limits)
            .await?;
        self.require_same_primary(&database_id, &primary)?;
        Ok(deliveries)
    }

    pub(crate) async fn grant_leader_lease(
        &self,
        database_id: &DatabaseId,
        leader_replica: ReplicaId,
        duration: std::time::Duration,
    ) -> Result<LeaderLease, ServerError> {
        let _guard = self.group_locks.acquire(&database_id.0).await;
        let primary = self.prepare_authority(database_id).await?;
        let lease = self
            .server
            .grant_leader_lease(database_id, leader_replica, duration)?;
        self.require_same_primary(database_id, &primary)?;
        self.persist_active_authority(database_id, &lease).await?;
        self.require_same_primary(database_id, &primary)?;
        Ok(lease)
    }

    pub(crate) async fn renew_leader_lease(
        &self,
        database_id: &DatabaseId,
        current: &LeaderLease,
        duration: std::time::Duration,
    ) -> Result<LeaderLease, ServerError> {
        let _guard = self.group_locks.acquire(&database_id.0).await;
        let primary = self.prepare_authority(database_id).await?;
        let lease = self
            .server
            .renew_leader_lease(database_id, current, duration)?;
        self.require_same_primary(database_id, &primary)?;
        self.persist_active_authority(database_id, &lease).await?;
        self.require_same_primary(database_id, &primary)?;
        Ok(lease)
    }

    pub(crate) async fn witness_commit(
        &self,
        auth: AuthContext,
        proposed: ProposedLogEntry,
        voter_acks: Vec<VoterAck>,
    ) -> Result<CommittedEntry, ServerError> {
        let database_id = proposed.database_id.clone();
        let _guard = self.group_locks.acquire(&database_id.0).await;
        let primary = self.prepare_authority(&database_id).await?;
        let committed = self
            .server
            .submit_write_proposal(
                auth,
                WriteProposalEnvelope {
                    proposal: proposed,
                    voter_acks,
                },
            )
            .await?
            .committed;
        self.require_same_primary(&database_id, &primary)?;
        Ok(committed)
    }

    async fn prepare_authority(
        &self,
        database_id: &DatabaseId,
    ) -> Result<PersonalDbPrimary, ServerError> {
        let primary = self.prepare_primary(database_id)?;
        self.server.hydrate_committed_log(database_id).await?;
        if let Some(authority) = self.object_store.load_authority(database_id).await? {
            self.server
                .install_recovered_authority(authority.membership, authority.leader_lease)?;
        }
        Ok(primary)
    }

    async fn persist_active_authority(
        &self,
        database_id: &DatabaseId,
        leader_lease: &LeaderLease,
    ) -> Result<(), ServerError> {
        let membership =
            self.server
                .membership(database_id)
                .ok_or_else(|| ServerError::InvalidTraitOutput {
                    provider: "anvil_personaldb_runtime",
                    message: "new leader lease has no active database-group membership".into(),
                })?;
        self.object_store
            .persist_authority(&membership, leader_lease)
            .await?;
        Ok(())
    }

    fn prepare_primary(&self, database_id: &DatabaseId) -> Result<PersonalDbPrimary, ServerError> {
        let primary = self.resolver.current(database_id.clone())?;
        if primary.node_id != self.local_node {
            return Err(ServerError::NotPrimary {
                database_id: database_id.clone(),
                known_placement_epoch: primary.assignment.placement_epoch,
                known_primary_server_id: primary.assignment.primary_server_id.clone(),
            });
        }
        let context = self
            .serving
            .mutation_context()
            .map_err(|status| unavailable(status.message()))?;
        if context.active_placement_log_id != primary.fence {
            return Err(unavailable(
                "PersonalDB placement changed while acquiring the serving fence",
            ));
        }

        let previous = self
            .server
            .placement(database_id)
            .map(|placement| placement.primary_server_id)
            .filter(|owner| owner != &primary.assignment.primary_server_id);
        self.server
            .install_placement(PlacementRecord::active_static(
                database_id.clone(),
                primary.assignment.placement_epoch,
                primary.assignment.primary_server_id.clone(),
                previous,
            ));
        Ok(primary)
    }

    fn require_same_primary(
        &self,
        database_id: &DatabaseId,
        expected: &PersonalDbPrimary,
    ) -> Result<(), ServerError> {
        let context = self
            .serving
            .mutation_context()
            .map_err(|status| unavailable(status.message()))?;
        let current = self.resolver.current(database_id.clone())?;
        if context.active_placement_log_id != expected.fence
            || current.fence != expected.fence
            || current.node_id != expected.node_id
        {
            return Err(unavailable(
                "PersonalDB placement changed before the operation completed; outcome is unknown",
            ));
        }
        Ok(())
    }
}

fn unavailable(message: impl Into<String>) -> ServerError {
    ServerError::TransportUnavailable {
        transport: TransportKind::InternalRpc,
        message: message.into(),
    }
}
