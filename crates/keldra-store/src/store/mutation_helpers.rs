//! Shared pure helpers for mutation admission and evaluation.

use super::*;
use crate::{DefinitionMutationIntent, DefinitionStateError, DefinitionTransition};

pub(super) fn is_mutation_capacity(error: &MutationError) -> bool {
    mutation_capacity_kind(error).is_some()
}

pub(super) fn mutation_capacity_kind(error: &MutationError) -> Option<&'static str> {
    match error {
        MutationError::ReceiptCapacity => Some("receipt"),
        MutationError::SourceJournalCapacity => Some("source_journal"),
        _ => None,
    }
}

pub(super) fn fail_unresolved_prepared(
    results: &mut BTreeMap<usize, Result<MutationReceipt, MutationError>>,
    prepared: &[(usize, PreparedOperation)],
    error: MutationError,
) {
    for (index, _) in prepared {
        let result = results.entry(*index).or_insert_with(|| Err(error.clone()));
        if result.is_ok() || result.as_ref().is_err_and(is_mutation_capacity) {
            *result = Err(error.clone());
        }
    }
}

pub(super) fn live_version_length(version: &Version) -> Option<u64> {
    (!version.deleted)
        .then(|| version.blob.as_ref().map(|blob| blob.length))
        .flatten()
}

pub(super) fn definition_receipt_matches_intent(
    stored: Option<&DefinitionTransition>,
    intent: Option<DefinitionMutationIntent>,
    operation: &PreparedOperation,
) -> bool {
    match (stored, intent) {
        (None, None) => true,
        (Some(stored), Some(intent)) => {
            stored.kind == intent.kind
                && stored.definition_id == intent.definition_id
                && stored.tenant_id == operation.identity().tenant_id.0
                && stored.bucket_id == operation.identity().bucket_id.0
                && stored.path == operation.key().path()
        }
        (Some(_), None) | (None, Some(_)) => false,
    }
}

pub(super) fn definition_mutation_error(error: DefinitionStateError) -> MutationError {
    MutationError::InvalidObjectMutation(error.to_string())
}

pub(super) fn exact_version_key(
    identity: BucketIdentity,
    exact_path: &str,
    version: VersionId,
) -> Vec<u8> {
    let mut encoded = identity.head_key(exact_path);
    encoded.push(0);
    encoded.extend_from_slice(&version.0.to_be_bytes());
    encoded
}
