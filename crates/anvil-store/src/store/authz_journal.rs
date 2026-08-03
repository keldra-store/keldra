use rocksdb::WriteBatch;

use super::*;
use crate::{
    AggregateKind, AuthzRealmMutationContext, AuthzStoreError, CoordinatedAuthzRealmMutation,
    CoordinatedAuthzRealmResult, PlacementLogId, TupleBatchRequest,
};

impl Store {
    /// Coordinates a Zanzibar tuple mutation and appends its compact aggregate
    /// invalidation in the same durable RocksDB batch.
    pub async fn coordinate_journaled_authz_tuple_mutation(
        &self,
        stable_tenant_id: u64,
        request: TupleBatchRequest,
        active_placement_log_id: PlacementLogId,
        serving_fence_term: u64,
    ) -> Result<CoordinatedAuthzRealmMutation, AuthzStoreError> {
        if stable_tenant_id == 0 {
            return Err(AuthzStoreError::InvalidInput(
                "stable authorization tenant ID must be non-zero".into(),
            ));
        }
        let command_id = request.operation_id.clone().ok_or_else(|| {
            AuthzStoreError::InvalidInput(
                "journaled authorization mutation requires an operation ID".into(),
            )
        })?;
        let scope_key = request.scope.handoff_order_key()?;

        let _commit_guard = self.commit_lock.lock().await;
        let source = self
            .local_watch_status()
            .map_err(|error| AuthzStoreError::Storage(error.to_string()))?;
        let source_journal_position = source
            .tail
            .checked_add(1)
            .ok_or_else(|| AuthzStoreError::Storage("source journal offset is exhausted".into()))?;
        let repository = self.authz();
        let _authz_guard = repository.lock_writes()?;
        let mut batch = WriteBatch::default();
        let coordinated = repository.stage_coordinated_tuple_mutation(
            request,
            AuthzRealmMutationContext {
                command_id,
                active_placement_log_id,
                serving_fence_term,
                source_id: source.source_id,
                source_journal_position,
            },
            &mut batch,
        )?;
        let (revision, replayed) = match &coordinated.result {
            CoordinatedAuthzRealmResult::Tuples(receipt) => {
                (receipt.authz_revision.0, receipt.replayed)
            }
            CoordinatedAuthzRealmResult::Bound(_) => {
                return Err(AuthzStoreError::Storage(
                    "tuple coordination returned a schema binding".into(),
                ));
            }
        };
        if replayed {
            return Ok(coordinated);
        }
        let mutation = coordinated.mutation.as_ref().ok_or_else(|| {
            AuthzStoreError::Storage("new tuple mutation omitted its typed result".into())
        })?;
        if mutation.stamp.source_journal_position != source_journal_position
            || mutation.stamp.source_id != source.source_id
        {
            return Err(AuthzStoreError::Storage(
                "authorization mutation and source journal position disagree".into(),
            ));
        }
        let mut aggregate_key = Vec::with_capacity(8 + scope_key.len());
        aggregate_key.extend_from_slice(&stable_tenant_id.to_be_bytes());
        aggregate_key.extend_from_slice(&scope_key);
        self.stage_local_changes(
            &mut batch,
            &[PendingLocalChange::AggregateChanged {
                aggregate_kind: AggregateKind::ZanzibarRealm,
                aggregate_key,
                revision,
            }],
        )
        .map_err(|error| AuthzStoreError::Storage(error.to_string()))?;
        repository.write(batch)?;
        self.notify_local_invalidations();
        Ok(coordinated)
    }
}
