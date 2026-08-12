use anvil_store::{DerivedConsumerCheckpoint, DerivedConsumerKind};

use super::*;

impl DataPeerTransport {
    pub(crate) async fn apply_derived_consumer_checkpoint(
        &self,
        target: NodeId,
        address: &str,
        checkpoint: DerivedConsumerCheckpoint,
    ) -> Result<(), Status> {
        checkpoint
            .validate()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if u64::from(checkpoint.consumer_node_id) != self.source_node_id.0 {
            return Err(Status::invalid_argument(
                "derived checkpoint consumer is not this transport node",
            ));
        }
        if u64::from(checkpoint.source_id.node_id) != target.0 {
            return Err(Status::invalid_argument(
                "derived checkpoint target is not its source node",
            ));
        }
        let wire_kind = match checkpoint.consumer_kind {
            DerivedConsumerKind::Index => wire::PrivateDerivedConsumerKind::Index,
            DerivedConsumerKind::Accounting => wire::PrivateDerivedConsumerKind::Accounting,
        };
        let response = self
            .client(target, address)?
            .apply_derived_consumer_checkpoint(wire::ApplyDerivedConsumerCheckpointRequest {
                peer: Some(self.context()),
                consumer_kind: wire_kind as i32,
                source_node_id: u64::from(checkpoint.source_id.node_id),
                source_epoch: checkpoint.source_id.source_epoch.to_vec(),
                consumer_node_id: u64::from(checkpoint.consumer_node_id),
                next_offset: checkpoint.next_offset,
                observed_fence_term: checkpoint.observed_fence.term,
                observed_fence_index: checkpoint.observed_fence.index,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        let kind_safe_through = match checkpoint.consumer_kind {
            DerivedConsumerKind::Index => response.index_safe_through,
            DerivedConsumerKind::Accounting => response.accounting_safe_through,
        };
        if response.consumer_kind != wire_kind as i32
            || response.source_node_id != u64::from(checkpoint.source_id.node_id)
            || response.consumer_node_id != u64::from(checkpoint.consumer_node_id)
            || response.next_offset != checkpoint.next_offset
            || response.observed_fence_term != checkpoint.observed_fence.term
            || response.observed_fence_index != checkpoint.observed_fence.index
            || kind_safe_through >= checkpoint.next_offset
        {
            return Err(Status::data_loss(
                "derived checkpoint response identity or safe cursor is invalid",
            ));
        }
        Ok(())
    }
}
