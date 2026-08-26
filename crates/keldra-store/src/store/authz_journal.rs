use rocksdb::WriteBatch;

use super::*;
use crate::{
    AggregateKind, AuthzRealmMutationContext, AuthzStoreError, BindSchemaRequest,
    CoordinatedAuthzRealmMutation, CoordinatedAuthzRealmResult, CoordinatedAuthzSchemaPublication,
    PlacementLogId, PublishSchemaRequest, TupleBatchRequest,
};

impl Store {
    pub async fn coordinate_journaled_authz_schema_publication(
        &self,
        stable_tenant_id: u64,
        request: PublishSchemaRequest,
        active_placement_log_id: PlacementLogId,
        serving_fence_term: u64,
    ) -> Result<CoordinatedAuthzSchemaPublication, AuthzStoreError> {
        require_stable_tenant(stable_tenant_id)?;
        let command_id = command_id(
            "publish",
            &(
                request.storage_tenant.as_str(),
                request.schema_id.as_str(),
                &request.schema,
                request.expected_revision,
            ),
        )?;
        let aggregate_key = aggregate_key(
            stable_tenant_id,
            &(
                "schema",
                request.storage_tenant.as_str(),
                request.schema_id.as_str(),
            ),
        )?;
        let _commit_guard = self.lock_commit("authorization_journal").await;
        let source = self.authz_source()?;
        let position = next_position(source.tail)?;
        let repository = self.authz();
        let _authz_guard = repository.lock_writes()?;
        let mut batch = WriteBatch::default();
        let coordinated = repository.stage_coordinated_schema_publication(
            request,
            authz_context(
                command_id,
                active_placement_log_id,
                serving_fence_term,
                source.source_id,
                position,
            ),
            &mut batch,
        )?;
        if coordinated.result.replayed {
            return Ok(coordinated);
        }
        require_publication_position(&coordinated, source.source_id, position)?;
        self.stage_authz_change(
            &mut batch,
            aggregate_key,
            coordinated.result.authz_revision.0,
        )?;
        repository.write(batch)?;
        self.notify_local_invalidations();
        Ok(coordinated)
    }

    pub async fn coordinate_journaled_authz_schema_binding(
        &self,
        stable_tenant_id: u64,
        request: BindSchemaRequest,
        active_placement_log_id: PlacementLogId,
        serving_fence_term: u64,
    ) -> Result<CoordinatedAuthzRealmMutation, AuthzStoreError> {
        require_stable_tenant(stable_tenant_id)?;
        let command_id = command_id(
            "bind",
            &(
                &request.scope,
                &request.schema_ref,
                request.expected_generation,
                request.expected_revision,
            ),
        )?;
        let scope_key = request.scope.handoff_order_key()?;
        let mut aggregate_key = Vec::with_capacity(8 + scope_key.len());
        aggregate_key.extend_from_slice(&stable_tenant_id.to_be_bytes());
        aggregate_key.extend_from_slice(&scope_key);
        let _commit_guard = self.lock_commit("authorization_journal").await;
        let source = self.authz_source()?;
        let position = next_position(source.tail)?;
        let repository = self.authz();
        let _authz_guard = repository.lock_writes()?;
        let mut batch = WriteBatch::default();
        let coordinated = repository.stage_coordinated_bind_schema(
            request,
            authz_context(
                command_id,
                active_placement_log_id,
                serving_fence_term,
                source.source_id,
                position,
            ),
            &mut batch,
        )?;
        let (revision, replayed) = match &coordinated.result {
            CoordinatedAuthzRealmResult::Bound(bound) => {
                (bound.binding.authz_revision.0, bound.replayed)
            }
            CoordinatedAuthzRealmResult::Tuples(_) => {
                return Err(AuthzStoreError::Storage(
                    "schema binding returned a tuple result".into(),
                ));
            }
        };
        if replayed {
            return Ok(coordinated);
        }
        require_realm_position(&coordinated, source.source_id, position)?;
        self.stage_authz_change(&mut batch, aggregate_key, revision)?;
        repository.write(batch)?;
        self.notify_local_invalidations();
        Ok(coordinated)
    }

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

        let _commit_guard = self.lock_commit("authorization_journal").await;
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
            LocalReferenceEffects::NoReferenceEffects,
        )
        .map_err(authz_mutation_error)?;
        repository.write(batch)?;
        self.notify_local_invalidations();
        Ok(coordinated)
    }

    fn authz_source(&self) -> Result<crate::WatchJournalStatus, AuthzStoreError> {
        self.local_watch_status()
            .map_err(|error| AuthzStoreError::Storage(error.to_string()))
    }

    fn stage_authz_change(
        &self,
        batch: &mut WriteBatch,
        aggregate_key: Vec<u8>,
        revision: u64,
    ) -> Result<(), AuthzStoreError> {
        self.stage_local_changes(
            batch,
            &[PendingLocalChange::AggregateChanged {
                aggregate_kind: AggregateKind::ZanzibarRealm,
                aggregate_key,
                revision,
            }],
            LocalReferenceEffects::NoReferenceEffects,
        )
        .map_err(authz_mutation_error)
    }
}

fn authz_mutation_error(error: MutationError) -> AuthzStoreError {
    match error {
        MutationError::SourceJournalCapacity => AuthzStoreError::SourceJournalCapacity,
        error => AuthzStoreError::Storage(error.to_string()),
    }
}

fn require_stable_tenant(stable_tenant_id: u64) -> Result<(), AuthzStoreError> {
    if stable_tenant_id == 0 {
        Err(AuthzStoreError::InvalidInput(
            "stable authorization tenant ID must be non-zero".into(),
        ))
    } else {
        Ok(())
    }
}

fn next_position(tail: u64) -> Result<u64, AuthzStoreError> {
    tail.checked_add(1)
        .ok_or_else(|| AuthzStoreError::Storage("source journal offset is exhausted".into()))
}

fn authz_context(
    command_id: String,
    active_placement_log_id: PlacementLogId,
    serving_fence_term: u64,
    source_id: crate::SourceId,
    source_journal_position: u64,
) -> AuthzRealmMutationContext {
    AuthzRealmMutationContext {
        command_id,
        active_placement_log_id,
        serving_fence_term,
        source_id,
        source_journal_position,
    }
}

fn command_id<T: serde::Serialize>(domain: &str, value: &T) -> Result<String, AuthzStoreError> {
    let encoded =
        serde_json::to_vec(value).map_err(|error| AuthzStoreError::Storage(error.to_string()))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra.authz-journal-command.v1");
    hasher.update(domain.as_bytes());
    hasher.update(&encoded);
    Ok(format!("{domain}-{}", hasher.finalize().to_hex()))
}

fn aggregate_key<T: serde::Serialize>(
    stable_tenant_id: u64,
    value: &T,
) -> Result<Vec<u8>, AuthzStoreError> {
    let encoded =
        serde_json::to_vec(value).map_err(|error| AuthzStoreError::Storage(error.to_string()))?;
    let mut key = Vec::with_capacity(8 + encoded.len());
    key.extend_from_slice(&stable_tenant_id.to_be_bytes());
    key.extend_from_slice(&encoded);
    Ok(key)
}

fn require_publication_position(
    coordinated: &CoordinatedAuthzSchemaPublication,
    source: crate::SourceId,
    position: u64,
) -> Result<(), AuthzStoreError> {
    let mutation = coordinated.mutation.as_ref().ok_or_else(|| {
        AuthzStoreError::Storage("new schema publication omitted its typed result".into())
    })?;
    if mutation.stamp.source_id != source || mutation.stamp.source_journal_position != position {
        return Err(AuthzStoreError::Storage(
            "schema publication and source journal position disagree".into(),
        ));
    }
    Ok(())
}

fn require_realm_position(
    coordinated: &CoordinatedAuthzRealmMutation,
    source: crate::SourceId,
    position: u64,
) -> Result<(), AuthzStoreError> {
    let mutation = coordinated.mutation.as_ref().ok_or_else(|| {
        AuthzStoreError::Storage("new realm mutation omitted its typed result".into())
    })?;
    if mutation.stamp.source_id != source || mutation.stamp.source_journal_position != position {
        return Err(AuthzStoreError::Storage(
            "realm mutation and source journal position disagree".into(),
        ));
    }
    Ok(())
}
