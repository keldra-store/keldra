//! Bounded parallel dispatch for independent bulk-write coordinator groups.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use keldra_api::v1::bulk_outcome::Outcome;
use keldra_api::v1::{BulkOperation, BulkOutcome, BulkWriteRequest};
use keldra_consensus::NodeId;
use keldra_store::{BatchOperation, DefinitionMutationIntent, ObjectKey};
use tonic::Status;

use super::{
    MAX_CONTENT_TYPE_BYTES, api_mutation_failure, api_receipt, api_request_failure, durability,
    validate_command_id,
};
use crate::authorization::ObjectPermission;
use crate::cluster_peer::ClusterPeerTransport;
use crate::object_distribution::ObjectDistribution;

/// Validates a bulk item without cloning its payload so a locally-coordinated
/// item can move the original bytes directly into the storage batch.
pub(super) fn validate_operation(
    operation: &BulkOperation,
    max_blob_bytes: u64,
) -> Result<(ObjectKey, ObjectPermission), Status> {
    use keldra_api::v1::bulk_operation::Operation;

    let (address, command_id, durability_value, content_type_value, payload_bytes, permission) =
        match operation.operation.as_ref() {
            Some(Operation::Put(request))
            | Some(Operation::PutIfAbsent(request))
            | Some(Operation::PutImmutable(request)) => (
                request.address.as_ref(),
                request.command_id.as_str(),
                request.durability,
                Some(request.content_type.as_str()),
                request.bytes.len() as u64,
                ObjectPermission::Put,
            ),
            Some(Operation::PutIfVersion(request)) => (
                request.address.as_ref(),
                request.command_id.as_str(),
                request.durability,
                Some(request.content_type.as_str()),
                request.bytes.len() as u64,
                ObjectPermission::Put,
            ),
            Some(Operation::Delete(request)) => (
                request.address.as_ref(),
                request.command_id.as_str(),
                request.durability,
                None,
                0,
                ObjectPermission::Delete,
            ),
            Some(Operation::DeleteIfVersion(request)) => (
                request.address.as_ref(),
                request.command_id.as_str(),
                request.durability,
                None,
                0,
                ObjectPermission::Delete,
            ),
            None => return Err(Status::invalid_argument("bulk operation is required")),
        };
    let address = address.ok_or_else(|| Status::invalid_argument("object address is required"))?;
    let key = ObjectKey::new(&address.tenant, &address.bucket, &address.path)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    validate_command_id(command_id)?;
    durability(durability_value)?;
    if content_type_value.is_some_and(|value| value.len() > MAX_CONTENT_TYPE_BYTES) {
        return Err(Status::invalid_argument(format!(
            "content_type exceeds {MAX_CONTENT_TYPE_BYTES} UTF-8 bytes"
        )));
    }
    if content_type_value.is_some_and(|value| value == keldra_store::OBJECT_LINK_CONTENT_TYPE) {
        return Err(Status::invalid_argument(
            "the object-link descriptor content type is reserved for Keldra",
        ));
    }
    if payload_bytes > max_blob_bytes {
        return Err(Status::resource_exhausted(
            "bulk put item exceeds the object-size limit",
        ));
    }
    Ok((key, permission))
}

pub(super) fn operation_inbound_bytes(operation: &BulkOperation) -> u64 {
    use keldra_api::v1::bulk_operation::Operation;
    match operation.operation.as_ref() {
        Some(Operation::Put(request))
        | Some(Operation::PutIfAbsent(request))
        | Some(Operation::PutImmutable(request)) => request.bytes.len() as u64,
        Some(Operation::PutIfVersion(request)) => request.bytes.len() as u64,
        Some(Operation::Delete(_)) | Some(Operation::DeleteIfVersion(_)) | None => 0,
    }
}

pub(super) fn requests_replicated_durability(operation: &BulkOperation) -> bool {
    use keldra_api::v1::bulk_operation::Operation;
    let durability = match operation.operation.as_ref() {
        Some(Operation::Put(request))
        | Some(Operation::PutIfAbsent(request))
        | Some(Operation::PutImmutable(request)) => request.durability,
        Some(Operation::PutIfVersion(request)) => request.durability,
        Some(Operation::Delete(request)) => request.durability,
        Some(Operation::DeleteIfVersion(request)) => request.durability,
        None => return false,
    };
    durability == keldra_api::v1::Durability::Replicated as i32
}

pub(super) fn record_phase_metrics(
    validation: Duration,
    authorization: Duration,
    identity_resolution: Duration,
    routing: Duration,
    dispatch: Duration,
) {
    tracing::info!(
        histogram.keldra_bulk_validation_duration_seconds = validation.as_secs_f64(),
        histogram.keldra_bulk_authorization_duration_seconds = authorization.as_secs_f64(),
        histogram.keldra_bulk_identity_resolution_duration_seconds =
            identity_resolution.as_secs_f64(),
        histogram.keldra_bulk_routing_duration_seconds = routing.as_secs_f64(),
        histogram.keldra_bulk_dispatch_duration_seconds = dispatch.as_secs_f64(),
        "bulk write phases completed"
    );
}

pub(super) fn record_dispatch_interruption(
    error: &Status,
    operation_count: usize,
    encoded_bytes: u64,
    dispatch_duration: Duration,
) {
    tracing::info!(
        bulk.phase = "coordinator_dispatch",
        grpc.code = ?error.code(),
        operation_count,
        encoded_bytes,
        histogram.keldra_bulk_interrupted_phase_duration_seconds =
            dispatch_duration.as_secs_f64(),
        "bulk write ended before coordinator dispatch completed"
    );
}

pub(super) async fn execute_coordinator_groups(
    distribution: ObjectDistribution,
    peers: ClusterPeerTransport,
    local_indices: Vec<usize>,
    local_operations: Vec<(BatchOperation, Option<DefinitionMutationIntent>)>,
    remote: BTreeMap<
        Vec<u64>,
        (
            NodeId,
            String,
            Vec<(usize, BulkOperation, Option<DefinitionMutationIntent>)>,
        ),
    >,
    bearer: String,
    internal: bool,
    started: Instant,
    route_budget: Duration,
) -> Result<Vec<BulkOutcome>, Status> {
    let mut tasks = tokio::task::JoinSet::new();
    if !local_operations.is_empty() {
        tasks.spawn(async move {
            let outcomes = distribution
                .mutate_many_with_definition_intents(local_operations)
                .await
                .into_iter()
                .enumerate()
                .map(|(index, result)| BulkOutcome {
                    index: local_indices[index] as u32,
                    outcome: Some(match result {
                        Ok(receipt) => Outcome::Receipt(api_receipt(receipt)),
                        Err(error) => Outcome::Failure(api_mutation_failure(error)),
                    }),
                })
                .collect();
            Ok::<_, Status>(outcomes)
        });
    }

    for (_, (target, address, operations)) in remote {
        let peers = peers.clone();
        let bearer = bearer.clone();
        let original_indices = operations
            .iter()
            .map(|(index, _, _)| *index)
            .collect::<Vec<_>>();
        let definition_intents = operations
            .iter()
            .enumerate()
            .filter_map(|(routed_index, (_, _, intent))| {
                intent.map(|intent| (routed_index, intent))
            })
            .collect();
        let request = BulkWriteRequest {
            operations: operations
                .into_iter()
                .map(|(_, operation, _)| operation)
                .collect(),
        };
        let remaining = route_budget
            .checked_sub(started.elapsed())
            .ok_or_else(|| Status::deadline_exceeded("bulk write routing deadline exceeded"))?;
        tasks.spawn(async move {
            let routed = if internal {
                peers
                    .route_internal_bulk_write(
                        target,
                        &address,
                        &bearer,
                        request,
                        definition_intents,
                        remaining,
                    )
                    .await
            } else {
                peers
                    .route_bulk_write(target, &address, &bearer, request, remaining)
                    .await
            };
            match routed {
                Ok(response) => remap_remote_outcomes(response.outcomes, &original_indices),
                Err(error) => Ok(original_indices
                    .into_iter()
                    .map(|index| BulkOutcome {
                        index: index as u32,
                        outcome: Some(Outcome::Failure(api_request_failure(error.clone()))),
                    })
                    .collect()),
            }
        });
    }

    let mut outcomes = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        outcomes.extend(joined.map_err(|error| {
            Status::internal(format!("bulk coordinator task failed: {error}"))
        })??);
    }
    Ok(outcomes)
}

fn remap_remote_outcomes(
    routed: Vec<BulkOutcome>,
    original_indices: &[usize],
) -> Result<Vec<BulkOutcome>, Status> {
    if routed.len() != original_indices.len() {
        return Err(Status::data_loss(
            "routed bulk response has an unexpected outcome count",
        ));
    }
    let mut seen = vec![false; original_indices.len()];
    let mut outcomes = Vec::with_capacity(routed.len());
    for mut outcome in routed {
        let routed_index = outcome.index as usize;
        if routed_index >= original_indices.len() || seen[routed_index] {
            return Err(Status::data_loss(
                "routed bulk response contains an invalid outcome index",
            ));
        }
        seen[routed_index] = true;
        outcome.index = original_indices[routed_index] as u32;
        outcomes.push(outcome);
    }
    Ok(outcomes)
}

#[cfg(test)]
mod tests {
    use keldra_api::v1::bulk_operation::Operation;
    use keldra_api::v1::{BulkPutRequest, Durability, MutationReceipt};

    use super::*;

    fn receipt(index: u32) -> BulkOutcome {
        BulkOutcome {
            index,
            outcome: Some(Outcome::Receipt(MutationReceipt::default())),
        }
    }

    #[test]
    fn remote_results_are_mapped_back_to_original_request_positions() {
        let mapped = remap_remote_outcomes(vec![receipt(1), receipt(0)], &[4, 9]).unwrap();
        assert_eq!(mapped[0].index, 9);
        assert_eq!(mapped[1].index, 4);
    }

    #[test]
    fn malformed_remote_indexes_fail_the_whole_protocol_exchange() {
        let duplicate = remap_remote_outcomes(vec![receipt(0), receipt(0)], &[4, 9]).unwrap_err();
        assert_eq!(duplicate.code(), tonic::Code::DataLoss);
        let missing = remap_remote_outcomes(vec![receipt(0)], &[4, 9]).unwrap_err();
        assert_eq!(missing.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn replicated_bulk_items_are_detected_without_rejecting_malformed_items() {
        let replicated = BulkOperation {
            operation: Some(Operation::Put(BulkPutRequest {
                durability: Durability::Replicated as i32,
                ..Default::default()
            })),
        };
        let local = BulkOperation {
            operation: Some(Operation::Put(BulkPutRequest {
                durability: Durability::Local as i32,
                ..Default::default()
            })),
        };
        assert!(requests_replicated_durability(&replicated));
        assert!(!requests_replicated_durability(&local));
        assert!(!requests_replicated_durability(&BulkOperation::default()));
    }
}
