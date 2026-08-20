use super::*;

pub(super) fn map_mutation_error(error: MutationError) -> Status {
    match error {
        MutationError::BlobNotFound => Status::not_found(error.to_string()),
        MutationError::InvalidObjectMutation(_) | MutationError::InvalidCommandId => {
            Status::invalid_argument(error.to_string())
        }
        MutationError::Storage(_) => Status::internal(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

pub(super) fn map_shard_error(error: ShardStoreError) -> Status {
    match error {
        ShardStoreError::NotFound => Status::not_found(error.to_string()),
        ShardStoreError::MalformedIdentity => Status::invalid_argument(error.to_string()),
        ShardStoreError::Storage(_) => Status::internal(error.to_string()),
        _ => Status::failed_precondition(error.to_string()),
    }
}

pub(super) fn map_payload_error(error: PayloadStoreError) -> Status {
    match error {
        PayloadStoreError::NotSmall | PayloadStoreError::NotLarge => {
            Status::invalid_argument(error.to_string())
        }
        PayloadStoreError::CompleteCopyMissing => Status::not_found(error.to_string()),
        PayloadStoreError::CompleteCopyCorrupt => Status::data_loss(error.to_string()),
        PayloadStoreError::Mutation(error) => map_mutation_error(error),
        PayloadStoreError::Shard(error) => map_shard_error(error),
        PayloadStoreError::Erasure(error) => Status::failed_precondition(error.to_string()),
        PayloadStoreError::Storage(_) => Status::internal(error.to_string()),
    }
}
