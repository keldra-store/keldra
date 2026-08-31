//! Exact ordinary-definition admission for assigned index owners.

use keldra_consensus::{DecisionRaft, NodeId};
use keldra_store::{
    DefinitionAssignment, DefinitionAssignmentMutation, DefinitionConsumerKind, DefinitionKind,
    PlacementLogId, Store,
};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::index_service::{StoredIndexDefinition, definition_path};

use super::super::catalog::{CatalogDefinition, IndexCatalog};
use super::super::placement::{IndexIdentity, IndexPlacement};
use crate::cluster_placement::ClusterPlacement;

pub(super) async fn refresh_index_assignment(
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    reader: &ClusterObjectReader,
    catalog: &IndexCatalog,
    identity: (u64, u64, u64),
) -> Result<(), Status> {
    catalog.remove(identity.0, identity.1, identity.2)?;
    let assignment = {
        let store = store.clone();
        tokio::task::spawn_blocking(move || {
            store.definition_assignment(DefinitionKind::Index, identity.0, identity.1, identity.2)
        })
        .await
        .map_err(join_status)?
        .map_err(internal_status)?
    };
    let Some(assignment) = assignment else {
        return Ok(());
    };
    if assignment.rank != 0 {
        return Ok(());
    }
    match load_index_assignment(local_node, decisions, reader, &assignment).await? {
        Some(definition) => catalog.upsert(definition)?,
        None => remove_stale_assignment(store, &assignment).await?,
    }
    Ok(())
}

pub(super) async fn remove_stale_assignment(
    store: &Store,
    assignment: &DefinitionAssignment,
) -> Result<(), Status> {
    let assignment = assignment.clone();
    let store = store.clone();
    tokio::task::spawn_blocking(move || store.remove_definition_assignment_if_matches(&assignment))
        .await
        .map_err(join_status)?
        .map(|_| ())
        .map_err(internal_status)
}

pub(crate) async fn load_index_assignment(
    local_node: NodeId,
    decisions: &DecisionRaft,
    reader: &ClusterObjectReader,
    assignment: &DefinitionAssignment,
) -> Result<Option<CatalogDefinition>, Status> {
    assignment
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if assignment.kind != DefinitionKind::Index {
        return Ok(None);
    }
    let placement = current_placement(decisions)?;
    let owners = assignment_placement(
        assignment.tenant_id,
        assignment.bucket_id,
        assignment.definition_id,
        &placement,
    )?;
    if owners.rank_of(local_node) != Some(assignment.rank)
        || assignment.observed_fence != placement.fence()
    {
        return Ok(None);
    }
    let Some(opened) = super::load_assigned_definition_object(reader, assignment).await? else {
        return Ok(None);
    };
    let stored = StoredIndexDefinition::decode(&opened.bytes)?;
    if stored.index_id != assignment.definition_id
        || definition_path(&stored.name)? != assignment.definition_path
    {
        return Err(Status::data_loss(
            "assigned index identity disagrees with the ordinary definition",
        ));
    }
    require_placement(decisions, placement.fence())?;
    Ok(Some(CatalogDefinition::new(
        assignment.tenant_id,
        assignment.bucket_id,
        opened.object_version.0,
        stored,
    )?))
}

pub(super) fn mutation_identity(mutation: &DefinitionAssignmentMutation) -> (u64, u64, u64) {
    match mutation {
        DefinitionAssignmentMutation::Upsert(value) => {
            (value.tenant_id, value.bucket_id, value.definition_id)
        }
        DefinitionAssignmentMutation::Delete(value) => {
            (value.tenant_id, value.bucket_id, value.definition_id)
        }
        DefinitionAssignmentMutation::Remove {
            tenant_id,
            bucket_id,
            definition_id,
            ..
        } => (*tenant_id, *bucket_id, *definition_id),
    }
}

pub(super) fn assignment_placement(
    tenant_id: u64,
    bucket_id: u64,
    _definition_id: u64,
    placement: &ClusterPlacement,
) -> Result<IndexPlacement, Status> {
    let identity = IndexIdentity::projection_partition(tenant_id, bucket_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    IndexPlacement::derive(identity, placement)
        .map_err(|error| Status::unavailable(error.to_string()))
}

pub(super) fn consumer_kind(kind: DefinitionKind) -> DefinitionConsumerKind {
    match kind {
        DefinitionKind::Index => DefinitionConsumerKind::IndexAssignments,
        DefinitionKind::Accounting => DefinitionConsumerKind::AccountingAssignments,
    }
}

pub(super) fn delivery_consumer_kind(kind: DefinitionKind) -> DefinitionConsumerKind {
    match kind {
        DefinitionKind::Index => DefinitionConsumerKind::V6IndexCatalog,
        DefinitionKind::Accounting => DefinitionConsumerKind::AccountingDelivery,
    }
}

pub(crate) fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
    ClusterPlacement::from_applied(&state).map_err(|error| Status::unavailable(error.to_string()))
}

pub(super) fn require_placement(
    decisions: &DecisionRaft,
    expected: PlacementLogId,
) -> Result<(), Status> {
    if current_placement(decisions)?.fence() == expected {
        Ok(())
    } else {
        Err(Status::unavailable(
            "cluster placement changed during definition coordination",
        ))
    }
}

pub(super) fn fence_after(left: PlacementLogId, right: PlacementLogId) -> bool {
    (left.term, left.index) > (right.term, right.index)
}

pub(super) fn join_status(error: tokio::task::JoinError) -> Status {
    Status::internal(format!("definition coordination task failed: {error}"))
}

pub(super) fn internal_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}
