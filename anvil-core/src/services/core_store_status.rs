use crate::core_store::{CoreStoreAvailabilityError, core_store_availability_error};
use crate::error_codes::AnvilErrorCode;
use tonic::Status;

pub(crate) fn availability_status(error: &anyhow::Error) -> Option<Status> {
    let availability = core_store_availability_error(error)?;
    let code = match availability {
        CoreStoreAvailabilityError::MeshTopologyUnavailable { .. }
        | CoreStoreAvailabilityError::QuorumUnavailable { .. } => {
            AnvilErrorCode::CoreMetaQuorumUnavailable
        }
        CoreStoreAvailabilityError::PeerUnavailable { .. }
        | CoreStoreAvailabilityError::ShardQuorumUnavailable { .. } => {
            AnvilErrorCode::ObjectShardQuorumUnavailable
        }
    };
    Some(Status::unavailable(format!("{code}: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinguishes_coremeta_and_object_shard_unavailability() {
        let coremeta: anyhow::Error = CoreStoreAvailabilityError::QuorumUnavailable {
            operation: "root_prepare",
            required: 2,
            received: 1,
            details: "peer unavailable".to_string(),
        }
        .into();
        let coremeta = availability_status(&coremeta).expect("CoreMeta availability status");
        assert_eq!(coremeta.code(), tonic::Code::Unavailable);
        assert!(
            coremeta
                .message()
                .contains(AnvilErrorCode::CoreMetaQuorumUnavailable.as_str())
        );

        let shards: anyhow::Error = CoreStoreAvailabilityError::ShardQuorumUnavailable {
            operation: "object_read",
            required: 4,
            received: 3,
            details: "peer unavailable".to_string(),
        }
        .into();
        let shards = availability_status(&shards).expect("shard availability status");
        assert_eq!(shards.code(), tonic::Code::Unavailable);
        assert!(
            shards
                .message()
                .contains(AnvilErrorCode::ObjectShardQuorumUnavailable.as_str())
        );
    }
}
