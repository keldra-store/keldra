//! Bounded parallel dispatch for independent bulk-write coordinator groups.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anvil_api::v1::bulk_outcome::Outcome;
use anvil_api::v1::{BulkOperation, BulkOutcome, BulkWriteRequest};
use anvil_consensus::NodeId;
use anvil_store::{BatchOperation, DefinitionMutationIntent};
use tonic::Status;

use super::{api_receipt, api_request_failure};
use crate::cluster_peer::ClusterPeerTransport;
use crate::object_distribution::ObjectDistribution;

pub(super) async fn execute_coordinator_groups(
    distribution: ObjectDistribution,
    peers: ClusterPeerTransport,
    local_indices: Vec<usize>,
    local_operations: Vec<(BatchOperation, Option<DefinitionMutationIntent>)>,
    remote: BTreeMap<
        NodeId,
        (
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
                        Err(error) => Outcome::Failure(api_request_failure(error)),
                    }),
                })
                .collect();
            Ok::<_, Status>(outcomes)
        });
    }

    for (target, (address, operations)) in remote {
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
    use anvil_api::v1::MutationReceipt;

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
}
