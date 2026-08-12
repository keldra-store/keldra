use anvil_consensus::PeerRpcKind;
use anvil_store::{
    DerivedConsumerCheckpoint, DerivedConsumerError, DerivedConsumerKind, PlacementLogId, SourceId,
};
use tonic::{Request, Response, Status};

use super::{DATA_PEER_SCHEMA_VERSION, DataPeerService, wire};

#[cfg(test)]
macro_rules! denied_test_call {
    ($client:ident, $peer:ident, $require_denied:ident) => {
        $require_denied!(
            $client.apply_derived_consumer_checkpoint(
                wire::ApplyDerivedConsumerCheckpointRequest {
                    peer: Some($peer.clone()),
                    consumer_kind: wire::PrivateDerivedConsumerKind::Index as i32,
                    source_node_id: 1,
                    source_epoch: vec![1; 32],
                    consumer_node_id: 2,
                    next_offset: 1,
                    observed_fence_term: 1,
                    observed_fence_index: 1,
                }
            ),
            "ApplyDerivedConsumerCheckpoint"
        );
    };
}

#[cfg(test)]
pub(super) use denied_test_call;

pub(super) async fn apply(
    service: &DataPeerService,
    mut request: Request<wire::ApplyDerivedConsumerCheckpointRequest>,
) -> Result<Response<wire::DerivedConsumerCheckpointApplied>, Status> {
    let peer = request.get_ref().peer.clone();
    let authenticated = service.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
    let metadata = request.metadata().clone();
    let value = request.into_inner();
    let consumer_kind = decode_kind(value.consumer_kind)?;
    let source_id =
        SourceId {
            node_id: u16::try_from(value.source_node_id)
                .map_err(|_| Status::invalid_argument("derived source node ID is invalid"))?,
            source_epoch: value.source_epoch.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("derived source epoch must contain 32 bytes")
            })?,
        };
    let consumer_node_id = u16::try_from(value.consumer_node_id)
        .map_err(|_| Status::invalid_argument("derived consumer node ID is invalid"))?;
    let checkpoint = DerivedConsumerCheckpoint {
        consumer_kind,
        source_id,
        consumer_node_id,
        next_offset: value.next_offset,
        observed_fence: PlacementLogId {
            term: value.observed_fence_term,
            index: value.observed_fence_index,
        },
    };
    checkpoint
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let admission = service.mutation_admission.derived_consumer_checkpoint(
        authenticated,
        source_id.node_id,
        consumer_node_id,
        checkpoint.observed_fence,
    )?;
    let store = service.store.clone();
    let active_nodes = admission.active_nodes;
    let status = service
        .bounded(&metadata, async move {
            store
                .apply_derived_consumer_checkpoint(checkpoint, &active_nodes)
                .await
                .map_err(map_error)
        })
        .await?;
    service.mutation_admission.require_fence(admission.fence)?;
    Ok(Response::new(wire::DerivedConsumerCheckpointApplied {
        schema_version: DATA_PEER_SCHEMA_VERSION,
        consumer_kind: value.consumer_kind,
        source_node_id: value.source_node_id,
        consumer_node_id: value.consumer_node_id,
        next_offset: value.next_offset,
        observed_fence_term: value.observed_fence_term,
        observed_fence_index: value.observed_fence_index,
        index_safe_through: status.index_safe_through,
        accounting_safe_through: status.accounting_safe_through,
    }))
}

fn decode_kind(value: i32) -> Result<DerivedConsumerKind, Status> {
    match wire::PrivateDerivedConsumerKind::try_from(value) {
        Ok(wire::PrivateDerivedConsumerKind::Index) => Ok(DerivedConsumerKind::Index),
        Ok(wire::PrivateDerivedConsumerKind::Accounting) => Ok(DerivedConsumerKind::Accounting),
        _ => Err(Status::invalid_argument("derived consumer kind is invalid")),
    }
}

fn map_error(error: DerivedConsumerError) -> Status {
    match error {
        DerivedConsumerError::Malformed(_) => Status::invalid_argument(error.to_string()),
        DerivedConsumerError::CheckpointExpired | DerivedConsumerError::CheckpointFuture => {
            Status::out_of_range(error.to_string())
        }
        DerivedConsumerError::SourceMismatch
        | DerivedConsumerError::FenceRegression
        | DerivedConsumerError::MembershipMismatch
        | DerivedConsumerError::InactiveConsumer
        | DerivedConsumerError::CheckpointRegression => {
            Status::failed_precondition(error.to_string())
        }
        DerivedConsumerError::Storage(_) => Status::internal(error.to_string()),
    }
}
