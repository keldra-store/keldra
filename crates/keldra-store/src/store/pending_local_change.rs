use super::PendingLocalChange;
use crate::LocalChange;

impl PendingLocalChange {
    pub(super) fn at_offset(&self, offset: u64) -> LocalChange {
        match self {
            Self::ObjectHead {
                identity,
                exact_path,
                path_version,
                deleted,
                program_commit_cursor,
                reference_deltas,
                accounting_transition,
                definition_transition,
            } => LocalChange::object_head_with_program_cursor(
                offset,
                identity.tenant_id.0,
                identity.bucket_id.0,
                exact_path.clone(),
                *path_version,
                *deleted,
                *program_commit_cursor,
                reference_deltas.clone(),
                *accounting_transition,
                definition_transition.clone(),
            ),
            Self::AliasObjectHead {
                identity,
                exact_path,
                canonical_path,
                path_version,
                deleted,
                program_commit_cursor,
            } => LocalChange::alias_object_head_with_program_cursor(
                offset,
                identity.tenant_id.0,
                identity.bucket_id.0,
                exact_path.clone(),
                canonical_path.clone(),
                *path_version,
                *deleted,
                *program_commit_cursor,
            ),
            Self::RetainedVersionDeleted {
                identity,
                exact_path,
                deleted_version,
                resulting_head_version,
                reference_deltas,
                accounting_transition,
            } => LocalChange::retained_version_deleted(
                offset,
                identity.tenant_id.0,
                identity.bucket_id.0,
                exact_path.clone(),
                *deleted_version,
                *resulting_head_version,
                reference_deltas.clone(),
                *accounting_transition,
            ),
            Self::AggregateChanged {
                aggregate_kind,
                aggregate_key,
                revision,
            } => LocalChange::aggregate_changed(
                offset,
                *aggregate_kind,
                aggregate_key.clone(),
                *revision,
            ),
            Self::ContentLifecycleChanged {
                blob_identity,
                revision,
                reference_deltas,
                accounting_transition,
            } => LocalChange::content_lifecycle_changed(
                offset,
                blob_identity.clone(),
                *revision,
                reference_deltas.clone(),
                accounting_transition.clone(),
            ),
            Self::AtomicBatchPublished {
                cursor,
                bundle_hash,
                affected_routes,
                mutations,
            } => LocalChange::atomic_batch_published(
                offset,
                *cursor,
                *bundle_hash,
                affected_routes.clone(),
                mutations.clone(),
            ),
        }
    }
}
