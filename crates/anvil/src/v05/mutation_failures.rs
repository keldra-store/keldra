use anvil_api::v1::{MutationFailure, MutationFailureCode};
use anvil_store::MutationError;
use tonic::Status;

pub(super) fn api_request_failure(error: Status) -> MutationFailure {
    let code = match error.code() {
        tonic::Code::ResourceExhausted => MutationFailureCode::ResourceLimit,
        tonic::Code::Unavailable => MutationFailureCode::DurabilityUnavailable,
        _ => MutationFailureCode::Invalid,
    };
    MutationFailure {
        code: code as i32,
        message: error.message().to_owned(),
        current_version: None,
    }
}

/// Preserves the public outcome of a mutation after coordinator execution has
/// converted the store error to a gRPC status.
pub(super) fn api_mutation_failure(error: Status) -> MutationFailure {
    let code = match error.code() {
        tonic::Code::FailedPrecondition => MutationFailureCode::ConditionFailed,
        tonic::Code::AlreadyExists => MutationFailureCode::IdempotencyInputMismatch,
        _ => return api_request_failure(error),
    };
    MutationFailure {
        code: code as i32,
        message: error.message().to_owned(),
        current_version: None,
    }
}

pub(super) fn api_failure(error: MutationError) -> MutationFailure {
    let (code, current_version) = match &error {
        MutationError::PreconditionFailed { current } => (
            MutationFailureCode::ConditionFailed,
            current.map(|value| value.0),
        ),
        MutationError::Immutable => (MutationFailureCode::Immutable, None),
        MutationError::ImmutablePolicyRequired => {
            (MutationFailureCode::ImmutablePolicyRequired, None)
        }
        MutationError::ProgramConcurrencyViolation => {
            (MutationFailureCode::ProgramConcurrencyViolation, None)
        }
        MutationError::CurrentTombstoneCannotBeDeleted => {
            (MutationFailureCode::ConditionFailed, None)
        }
        MutationError::ObjectVersioningNotEnabled => (MutationFailureCode::ConditionFailed, None),
        MutationError::IdempotencyConflict => (MutationFailureCode::IdempotencyInputMismatch, None),
        MutationError::InvalidCommandId
        | MutationError::InvalidPolicy(_)
        | MutationError::InvalidObjectMutation(_)
        | MutationError::BlobNotFound => (MutationFailureCode::Invalid, None),
        MutationError::DurabilityUnavailable => (MutationFailureCode::DurabilityUnavailable, None),
        MutationError::ReceiptCapacity | MutationError::SourceJournalCapacity => {
            (MutationFailureCode::ResourceLimit, None)
        }
        MutationError::ReceiptTooLarge { .. }
        | MutationError::SourceJournalRecordTooLarge { .. } => {
            (MutationFailureCode::ResourceLimit, None)
        }
        MutationError::ObjectMutationLineageGap { .. }
        | MutationError::ObjectMutationSibling { .. }
        | MutationError::ObjectMutationConflict
        | MutationError::Storage(_) => (MutationFailureCode::Internal, None),
    };
    MutationFailure {
        code: code as i32,
        message: error.to_string(),
        current_version,
    }
}
