//! Typed complete-realm replication for Zanzibar state.
//!
//! This module deliberately exposes mutations, not RocksDB keys. A peer can
//! apply one coordinator result or reject it; it cannot read or write an
//! authorization column family through this API.

use std::collections::BTreeSet;

use anvil_authz::{Authorization, AuthorizationLimits, Schema, Tuple};
use rocksdb::{WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};

use super::{
    AuthzRepository, AuthzRevision, AuthzScope, AuthzStoreError, BindSchemaRequest, BoundRealm,
    RealmBinding, STORED_TUPLE_RECEIPT_FORMAT, StorageTenantId, StoredSchema, StoredTuple,
    StoredTupleReceipt, TupleBatchReceipt, TupleBatchRequest, TupleMutation, TupleMutationKind,
    binding_key, canonical_schema, current_unix_millis, encode_json, receipt_key,
    receipt_record_bytes, schema_digest_key, schema_revision_key, storage_error, tuple_fingerprint,
    tuple_key, validate_binding, validate_principal, validate_stored_schema,
    validate_stored_tuple_receipt_shape,
};
use crate::store::{CF_AUTHZ_BINDINGS, CF_AUTHZ_RECEIPTS, CF_AUTHZ_SCHEMAS, CF_AUTHZ_TUPLES};
use crate::{PlacementLogId, SourceId};

pub const AUTHZ_REALM_MUTATION_FORMAT: u16 = 1;
pub const AUTHZ_REALM_MUTATION_STAMP_FORMAT: u16 = 1;

/// Consensus-derived values attached to a mutation by the current realm
/// coordinator. Source position assignment and journal append remain the
/// coordinator runtime's responsibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzRealmMutationContext {
    pub command_id: String,
    pub active_placement_log_id: PlacementLogId,
    pub serving_fence_term: u64,
    pub source_id: SourceId,
    pub source_journal_position: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzRealmMutationStamp {
    pub format: u16,
    pub predecessor_revision: Option<AuthzRevision>,
    pub mutation_fingerprint: [u8; 32],
    pub active_placement_log_id: PlacementLogId,
    pub serving_fence_term: u64,
    pub source_id: SourceId,
    pub source_journal_position: u64,
}

/// The immutable schema bytes needed for a bound realm to be a complete local
/// authorization graph. This does not decide the separate schema-catalogue
/// placement boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzRealmSchema {
    pub schema_ref: super::SchemaRef,
    pub schema: Schema,
    pub published_at_revision: AuthzRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "change", rename_all = "snake_case")]
pub enum AuthzRealmChange {
    BindSchema {
        schema: AuthzRealmSchema,
        binding: RealmBinding,
    },
    MutateTuples {
        mutations: Vec<TupleMutation>,
        binding: RealmBinding,
        receipt: TupleBatchReceipt,
        receipt_created_at_unix_millis: u64,
    },
}

impl AuthzRealmChange {
    fn revision(&self) -> AuthzRevision {
        match self {
            Self::BindSchema { binding, .. } => binding.authz_revision,
            Self::MutateTuples { receipt, .. } => receipt.authz_revision,
        }
    }

    fn binding(&self) -> &RealmBinding {
        match self {
            Self::BindSchema { binding, .. } | Self::MutateTuples { binding, .. } => binding,
        }
    }
}

/// One bounded typed result for the complete `(storage_tenant, realm)`
/// aggregate. Raw column-family operations and Raft payloads are absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzRealmMutation {
    pub format: u16,
    pub scope: AuthzScope,
    pub command_id: String,
    pub input_fingerprint: [u8; 32],
    pub change: AuthzRealmChange,
    pub stamp: AuthzRealmMutationStamp,
}

impl AuthzRealmMutation {
    pub fn revision(&self) -> AuthzRevision {
        self.change.revision()
    }

    pub fn computed_fingerprint(&self) -> [u8; 32] {
        let material = (
            self.format,
            &self.scope,
            &self.command_id,
            self.input_fingerprint,
            &self.change,
            self.stamp.format,
            self.stamp.predecessor_revision,
            self.stamp.active_placement_log_id,
            self.stamp.serving_fence_term,
            self.stamp.source_id,
            self.stamp.source_journal_position,
        );
        let encoded = serde_json::to_vec(&material)
            .expect("typed authorization mutation fingerprint material serializes");
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"anvil.authz-realm-mutation.v1");
        hasher.update(&encoded);
        *hasher.finalize().as_bytes()
    }

    pub(crate) fn set_computed_fingerprint(&mut self) {
        self.stamp.mutation_fingerprint = self.computed_fingerprint();
    }

    pub fn validate(&self) -> Result<(), AuthzStoreError> {
        self.validate_with_limits(
            super::AuthzStoreLimits::default().max_mutations_per_batch,
            AuthorizationLimits::default(),
        )
    }

    fn validate_with_limits(
        &self,
        max_mutations: usize,
        evaluator_limits: AuthorizationLimits,
    ) -> Result<(), AuthzStoreError> {
        if self.format != AUTHZ_REALM_MUTATION_FORMAT
            || self.stamp.format != AUTHZ_REALM_MUTATION_STAMP_FORMAT
        {
            return Err(invalid_mutation("unsupported mutation or stamp format"));
        }
        self.scope.validate()?;
        super::validate_component(
            &self.command_id,
            "command id",
            super::MAX_OPERATION_ID_BYTES,
        )?;
        if self.stamp.predecessor_revision == Some(AuthzRevision::ZERO)
            || self.stamp.serving_fence_term == 0
            || self.stamp.source_id.node_id == 0
            || self.stamp.source_id.source_epoch == [0; 32]
            || self.stamp.source_journal_position == 0
        {
            return Err(invalid_mutation(
                "predecessor, serving fence, or source identity is invalid",
            ));
        }
        let binding = self.change.binding();
        validate_binding(binding, &self.scope)?;
        let revision = self.revision();
        if revision == AuthzRevision::ZERO
            || self
                .stamp
                .predecessor_revision
                .is_some_and(|predecessor| predecessor >= revision)
        {
            return Err(invalid_mutation(
                "result revision must follow its predecessor",
            ));
        }

        match &self.change {
            AuthzRealmChange::BindSchema { schema, binding } => {
                let canonical = canonical_schema(schema.schema.clone(), evaluator_limits)?;
                let stored = StoredSchema {
                    schema_ref: schema.schema_ref.clone(),
                    schema: canonical,
                    published_at_revision: schema.published_at_revision,
                };
                validate_stored_schema(&stored, &schema.schema_ref, evaluator_limits)?;
                if binding.schema_ref != schema.schema_ref
                    || schema.published_at_revision == AuthzRevision::ZERO
                    || schema.published_at_revision > binding.authz_revision
                {
                    return Err(invalid_mutation(
                        "bound schema and realm result are inconsistent",
                    ));
                }
            }
            AuthzRealmChange::MutateTuples {
                mutations,
                binding,
                receipt,
                receipt_created_at_unix_millis,
            } => {
                if mutations.is_empty() || mutations.len() > max_mutations {
                    return Err(invalid_mutation("tuple mutation count is out of bounds"));
                }
                let mut canonical = mutations.clone();
                canonical.sort();
                let unique_tuples = mutations
                    .iter()
                    .map(|mutation| &mutation.tuple)
                    .collect::<BTreeSet<_>>();
                if canonical != *mutations || unique_tuples.len() != mutations.len() {
                    return Err(invalid_mutation(
                        "tuple mutations must be sorted and unique by tuple",
                    ));
                }
                validate_principal(&receipt.principal)?;
                if receipt.scope != self.scope
                    || receipt.replayed
                    || binding.authz_revision > receipt.authz_revision
                    || receipt.binding_generation != binding.generation
                    || receipt.mutation_count != mutations.len()
                    || *receipt_created_at_unix_millis == 0
                    || receipt.replay_guarantee_expires_at_unix_millis
                        <= *receipt_created_at_unix_millis
                {
                    return Err(invalid_mutation(
                        "tuple receipt and realm result are inconsistent",
                    ));
                }
            }
        }
        if self.stamp.mutation_fingerprint != self.computed_fingerprint() {
            return Err(invalid_mutation(
                "mutation fingerprint does not match its typed result",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatedAuthzRealmResult {
    Bound(BoundRealm),
    Tuples(TupleBatchReceipt),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedAuthzRealmMutation {
    pub result: CoordinatedAuthzRealmResult,
    /// `None` only for a released local receipt or semantic binding replay
    /// that predates typed realm mutation storage.
    pub mutation: Option<AuthzRealmMutation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaAuthzRealmMutationApplied {
    pub revision: AuthzRevision,
    pub replayed: bool,
}

/// Stored binding envelope. Flattening preserves the released binding JSON
/// shape: old values decode with no stamp, and old `RealmBinding` readers
/// ignore the additional field on 0.5.1 values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct StoredRealmBinding {
    #[serde(flatten)]
    pub binding: RealmBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_stamp: Option<AuthzRealmMutationStamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_revision: Option<AuthzRevision>,
}

impl AuthzRepository {
    pub fn coordinate_bind_schema_mutation(
        &self,
        request: BindSchemaRequest,
        context: AuthzRealmMutationContext,
    ) -> Result<CoordinatedAuthzRealmMutation, AuthzStoreError> {
        self.validate_context(&context)?;
        request.scope.validate()?;
        let input_fingerprint = bind_input_fingerprint(&request)?;
        let _guard = self.lock_writes()?;
        let predecessor = self.read_stored_binding(&request.scope)?;
        let mut batch = WriteBatch::default();
        let bound = self.prepare_binding(&request, false, &mut batch)?;
        if bound.replayed {
            return Ok(CoordinatedAuthzRealmMutation {
                result: CoordinatedAuthzRealmResult::Bound(bound),
                mutation: None,
            });
        }
        let stored_schema =
            self.require_schema(&request.scope.storage_tenant, &request.schema_ref)?;
        let predecessor_revision = predecessor
            .as_ref()
            .map(|stored| self.stored_realm_revision(&request.scope, stored))
            .transpose()?;
        let mut mutation = AuthzRealmMutation {
            format: AUTHZ_REALM_MUTATION_FORMAT,
            scope: request.scope,
            command_id: context.command_id.clone(),
            input_fingerprint,
            change: AuthzRealmChange::BindSchema {
                schema: AuthzRealmSchema {
                    schema_ref: stored_schema.schema_ref,
                    schema: stored_schema.schema,
                    published_at_revision: stored_schema.published_at_revision,
                },
                binding: bound.binding.clone(),
            },
            stamp: stamp(predecessor_revision, context),
        };
        mutation.set_computed_fingerprint();
        mutation
            .validate_with_limits(self.limits.max_mutations_per_batch, self.limits.evaluator)?;
        self.stage_stamped_binding(&mut batch, &mutation)?;
        self.write(batch)?;
        Ok(CoordinatedAuthzRealmMutation {
            result: CoordinatedAuthzRealmResult::Bound(bound),
            mutation: Some(mutation),
        })
    }

    pub fn coordinate_tuple_mutation(
        &self,
        request: TupleBatchRequest,
        context: AuthzRealmMutationContext,
    ) -> Result<CoordinatedAuthzRealmMutation, AuthzStoreError> {
        let _guard = self.lock_writes()?;
        let mut batch = WriteBatch::default();
        let coordinated = self.stage_coordinated_tuple_mutation(request, context, &mut batch)?;
        if !batch.is_empty() {
            self.write(batch)?;
        }
        Ok(coordinated)
    }

    /// Stages one coordinator-owned tuple mutation into a caller-provided
    /// RocksDB batch. The caller must hold this repository's write lock. This
    /// is the storage boundary used to append the source-journal invalidation
    /// atomically with the authoritative Zanzibar mutation.
    pub(crate) fn stage_coordinated_tuple_mutation(
        &self,
        request: TupleBatchRequest,
        context: AuthzRealmMutationContext,
        batch: &mut WriteBatch,
    ) -> Result<CoordinatedAuthzRealmMutation, AuthzStoreError> {
        self.validate_context(&context)?;
        if request.operation_id.as_deref() != Some(context.command_id.as_str()) {
            return Err(invalid_mutation(
                "tuple operation id must equal the mutation command id",
            ));
        }
        let canonical_mutations = self.validate_mutation_request(&request)?;
        let input_fingerprint = tuple_fingerprint(&request, &canonical_mutations)?;
        let predecessor = self.read_stored_binding(&request.scope)?.ok_or_else(|| {
            AuthzStoreError::MissingBinding(
                request.scope.storage_tenant.clone(),
                request.scope.realm.clone(),
            )
        })?;
        let predecessor_revision = self.stored_realm_revision(&request.scope, &predecessor)?;
        let receipt = self.prepare_tuple_batch(&request, batch)?;
        if receipt.replayed {
            let mutation = self
                .stored_tuple_receipt(&request, &context.command_id)?
                .and_then(|stored| stored.realm_mutation);
            return Ok(CoordinatedAuthzRealmMutation {
                result: CoordinatedAuthzRealmResult::Tuples(receipt),
                mutation,
            });
        }

        let resulting_tuples =
            apply_tuple_changes(self.read_tuples(&request.scope)?, &canonical_mutations);
        let stored_schema = self.require_schema(
            &request.scope.storage_tenant,
            &predecessor.binding.schema_ref,
        )?;
        Authorization::new(
            request.scope.realm.clone(),
            stored_schema.schema,
            resulting_tuples.iter().cloned(),
            self.limits.evaluator,
        )?;
        let mut binding = predecessor.binding.clone();
        binding.tuple_count = resulting_tuples.len();
        let created_at = receipt
            .replay_guarantee_expires_at_unix_millis
            .checked_sub(self.limits.receipt_retention_millis)
            .ok_or_else(|| invalid_mutation("tuple receipt creation time is invalid"))?;
        let mut mutation = AuthzRealmMutation {
            format: AUTHZ_REALM_MUTATION_FORMAT,
            scope: request.scope.clone(),
            command_id: context.command_id.clone(),
            input_fingerprint,
            change: AuthzRealmChange::MutateTuples {
                mutations: canonical_mutations,
                binding,
                receipt: receipt.clone(),
                receipt_created_at_unix_millis: created_at,
            },
            stamp: stamp(Some(predecessor_revision), context),
        };
        mutation.set_computed_fingerprint();
        mutation
            .validate_with_limits(self.limits.max_mutations_per_batch, self.limits.evaluator)?;
        self.stage_stamped_binding(batch, &mutation)?;
        self.stage_replicated_tuple_receipt(batch, &mutation, created_at)?;
        Ok(CoordinatedAuthzRealmMutation {
            result: CoordinatedAuthzRealmResult::Tuples(receipt),
            mutation: Some(mutation),
        })
    }

    /// Applies one coordinator-produced realm result atomically. This method
    /// never appends to the source journal; only the coordinator owns that
    /// source position.
    pub fn apply_authz_realm_mutation_replica(
        &self,
        mutation: &AuthzRealmMutation,
    ) -> Result<ReplicaAuthzRealmMutationApplied, AuthzStoreError> {
        mutation
            .validate_with_limits(self.limits.max_mutations_per_batch, self.limits.evaluator)?;
        let _guard = self.lock_writes()?;
        let now = current_unix_millis()?;
        if self.is_retained_tuple_replay(mutation, now)? {
            return Ok(ReplicaAuthzRealmMutationApplied {
                revision: mutation.revision(),
                replayed: true,
            });
        }
        let current = self.read_stored_binding(&mutation.scope)?;
        if current.as_ref().and_then(|stored| stored.mutation_stamp) == Some(mutation.stamp) {
            self.verify_applied_mutation(mutation, now)?;
            return Ok(ReplicaAuthzRealmMutationApplied {
                revision: mutation.revision(),
                replayed: true,
            });
        }
        let current_revision = current
            .as_ref()
            .map(|stored| self.stored_realm_revision(&mutation.scope, stored))
            .transpose()?;
        validate_predecessor(current.as_ref(), current_revision, mutation)?;

        let mut batch = WriteBatch::default();
        match &mutation.change {
            AuthzRealmChange::BindSchema { schema, binding } => {
                let tuples = self.read_tuples(&mutation.scope)?;
                if tuples.len() != binding.tuple_count {
                    return Err(AuthzStoreError::RealmMutationConflict);
                }
                Authorization::new(
                    mutation.scope.realm.clone(),
                    schema.schema.clone(),
                    tuples,
                    self.limits.evaluator,
                )?;
                self.stage_realm_schema(&mut batch, &mutation.scope.storage_tenant, schema)?;
            }
            AuthzRealmChange::MutateTuples {
                mutations,
                binding,
                receipt_created_at_unix_millis,
                ..
            } => {
                let current = current
                    .as_ref()
                    .ok_or(AuthzStoreError::RealmMutationConflict)?;
                if current.binding.schema_ref != binding.schema_ref
                    || current.binding.generation != binding.generation
                    || current.binding.authz_revision != binding.authz_revision
                {
                    return Err(AuthzStoreError::RealmMutationConflict);
                }
                let schema = self
                    .require_schema(&mutation.scope.storage_tenant, &current.binding.schema_ref)?;
                let tuples = apply_tuple_changes(self.read_tuples(&mutation.scope)?, mutations);
                if tuples.len() != binding.tuple_count {
                    return Err(AuthzStoreError::RealmMutationConflict);
                }
                Authorization::new(
                    mutation.scope.realm.clone(),
                    schema.schema,
                    tuples,
                    self.limits.evaluator,
                )?;
                for change in mutations {
                    let key = tuple_key(&mutation.scope, &change.tuple)?;
                    match change.kind {
                        TupleMutationKind::Add => batch.put_cf(
                            self.cf(CF_AUTHZ_TUPLES)?,
                            key,
                            encode_json(&StoredTuple {
                                tuple: change.tuple.clone(),
                            })?,
                        ),
                        TupleMutationKind::Remove => {
                            batch.delete_cf(self.cf(CF_AUTHZ_TUPLES)?, key)
                        }
                    }
                }
                self.stage_replicated_tuple_receipt(
                    &mut batch,
                    mutation,
                    *receipt_created_at_unix_millis,
                )?;
            }
        }
        self.stage_stamped_binding(&mut batch, mutation)?;
        let local_revision = self.tenant_revision(&mutation.scope.storage_tenant)?;
        if mutation.revision() > local_revision {
            self.stage_tenant_revision(
                &mut batch,
                &mutation.scope.storage_tenant,
                mutation.revision(),
            )?;
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        Ok(ReplicaAuthzRealmMutationApplied {
            revision: mutation.revision(),
            replayed: false,
        })
    }

    fn validate_context(&self, context: &AuthzRealmMutationContext) -> Result<(), AuthzStoreError> {
        super::validate_component(
            &context.command_id,
            "command id",
            self.limits.max_operation_id_bytes,
        )?;
        if context.serving_fence_term == 0
            || context.source_id.node_id == 0
            || context.source_id.source_epoch == [0; 32]
            || context.source_journal_position == 0
        {
            return Err(invalid_mutation(
                "serving fence and source identity must be non-zero",
            ));
        }
        Ok(())
    }

    fn read_stored_binding(
        &self,
        scope: &AuthzScope,
    ) -> Result<Option<StoredRealmBinding>, AuthzStoreError> {
        let stored: Option<StoredRealmBinding> =
            self.read_json(CF_AUTHZ_BINDINGS, &binding_key(scope))?;
        if let Some(stored) = stored.as_ref() {
            validate_binding(&stored.binding, scope)?;
        }
        Ok(stored)
    }

    fn stored_realm_revision(
        &self,
        scope: &AuthzScope,
        stored: &StoredRealmBinding,
    ) -> Result<AuthzRevision, AuthzStoreError> {
        match (stored.aggregate_revision, stored.mutation_stamp) {
            (Some(revision), Some(_)) if revision != AuthzRevision::ZERO => Ok(revision),
            (None, None) => self.tenant_revision(&scope.storage_tenant),
            _ => Err(AuthzStoreError::Storage(
                "persisted authorization realm lineage is inconsistent".into(),
            )),
        }
    }

    fn stage_stamped_binding(
        &self,
        batch: &mut WriteBatch,
        mutation: &AuthzRealmMutation,
    ) -> Result<(), AuthzStoreError> {
        batch.put_cf(
            self.cf(CF_AUTHZ_BINDINGS)?,
            binding_key(&mutation.scope),
            encode_json(&StoredRealmBinding {
                binding: mutation.change.binding().clone(),
                mutation_stamp: Some(mutation.stamp),
                aggregate_revision: Some(mutation.revision()),
            })?,
        );
        Ok(())
    }

    fn stage_realm_schema(
        &self,
        batch: &mut WriteBatch,
        tenant: &StorageTenantId,
        schema: &AuthzRealmSchema,
    ) -> Result<(), AuthzStoreError> {
        let stored = StoredSchema {
            schema_ref: schema.schema_ref.clone(),
            schema: schema.schema.clone(),
            published_at_revision: schema.published_at_revision,
        };
        let revision_key = schema_revision_key(tenant, &stored.schema_ref);
        let revision_exists =
            match self.read_json::<StoredSchema>(CF_AUTHZ_SCHEMAS, &revision_key)? {
                Some(existing) if existing != stored => {
                    return Err(AuthzStoreError::RealmMutationConflict);
                }
                Some(_) => true,
                None => false,
            };
        let digest_key = schema_digest_key(
            tenant,
            &stored.schema_ref.schema_id,
            stored.schema_ref.schema_digest,
        );
        if let Some(existing) = self.read_json::<super::SchemaRef>(CF_AUTHZ_SCHEMAS, &digest_key)?
            && existing != stored.schema_ref
        {
            return Err(AuthzStoreError::RealmMutationConflict);
        }
        if !revision_exists {
            batch.put_cf(
                self.cf(CF_AUTHZ_SCHEMAS)?,
                revision_key,
                encode_json(&stored)?,
            );
        }
        batch.put_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            digest_key,
            encode_json(&stored.schema_ref)?,
        );
        Ok(())
    }

    fn stage_replicated_tuple_receipt(
        &self,
        batch: &mut WriteBatch,
        mutation: &AuthzRealmMutation,
        created_at_unix_millis: u64,
    ) -> Result<(), AuthzStoreError> {
        let AuthzRealmChange::MutateTuples { receipt, .. } = &mutation.change else {
            return Err(AuthzStoreError::RealmMutationConflict);
        };
        let key = receipt_key(
            &mutation.scope.storage_tenant,
            &receipt.principal,
            &mutation.command_id,
        )?;
        let stored = StoredTupleReceipt {
            format: STORED_TUPLE_RECEIPT_FORMAT,
            operation_id: mutation.command_id.clone(),
            created_at_unix_millis,
            expires_at_unix_millis: receipt.replay_guarantee_expires_at_unix_millis,
            fingerprint: mutation.input_fingerprint,
            receipt: receipt.clone(),
            realm_mutation: Some(mutation.clone()),
        };
        let now = current_unix_millis()?;
        if stored.expires_at_unix_millis <= now {
            return Ok(());
        }
        let encoded = encode_json(&stored)?;
        let inventory = self.tuple_receipt_inventory(now)?;
        let entries = inventory
            .retained_entries
            .checked_add(1)
            .ok_or(AuthzStoreError::ReceiptCapacity)?;
        let bytes = inventory
            .retained_bytes
            .checked_add(receipt_record_bytes(&key, &encoded)?)
            .ok_or(AuthzStoreError::ReceiptCapacity)?;
        if entries > self.limits.max_receipt_entries || bytes > self.limits.max_receipt_bytes {
            return Err(AuthzStoreError::ReceiptCapacity);
        }
        for expired in inventory.expired_keys {
            batch.delete_cf(self.cf(CF_AUTHZ_RECEIPTS)?, expired);
        }
        batch.put_cf(self.cf(CF_AUTHZ_RECEIPTS)?, key, encoded);
        Ok(())
    }

    fn stored_tuple_receipt(
        &self,
        request: &TupleBatchRequest,
        command_id: &str,
    ) -> Result<Option<StoredTupleReceipt>, AuthzStoreError> {
        self.read_json(
            CF_AUTHZ_RECEIPTS,
            &receipt_key(
                &request.scope.storage_tenant,
                &request.principal,
                command_id,
            )?,
        )
    }

    fn is_retained_tuple_replay(
        &self,
        mutation: &AuthzRealmMutation,
        now: u64,
    ) -> Result<bool, AuthzStoreError> {
        let AuthzRealmChange::MutateTuples { receipt, .. } = &mutation.change else {
            return Ok(false);
        };
        let key = receipt_key(
            &mutation.scope.storage_tenant,
            &receipt.principal,
            &mutation.command_id,
        )?;
        let Some(stored) = self.read_json::<StoredTupleReceipt>(CF_AUTHZ_RECEIPTS, &key)? else {
            return Ok(false);
        };
        validate_stored_tuple_receipt_shape(&stored)?;
        if stored.expires_at_unix_millis <= now {
            return Ok(false);
        }
        if stored.fingerprint != mutation.input_fingerprint
            || stored.receipt != *receipt
            || stored.realm_mutation.as_ref() != Some(mutation)
        {
            return Err(AuthzStoreError::RealmMutationConflict);
        }
        Ok(true)
    }

    fn verify_applied_mutation(
        &self,
        mutation: &AuthzRealmMutation,
        now: u64,
    ) -> Result<(), AuthzStoreError> {
        let stored = self
            .read_stored_binding(&mutation.scope)?
            .ok_or(AuthzStoreError::RealmMutationConflict)?;
        if stored.binding != *mutation.change.binding()
            || stored.mutation_stamp != Some(mutation.stamp)
        {
            return Err(AuthzStoreError::RealmMutationConflict);
        }
        let tuples = self.read_tuples(&mutation.scope)?;
        if tuples.len() != stored.binding.tuple_count {
            return Err(AuthzStoreError::RealmMutationConflict);
        }
        match &mutation.change {
            AuthzRealmChange::BindSchema { schema, .. } => {
                let found =
                    self.require_schema(&mutation.scope.storage_tenant, &schema.schema_ref)?;
                if found.schema != schema.schema
                    || found.published_at_revision != schema.published_at_revision
                {
                    return Err(AuthzStoreError::RealmMutationConflict);
                }
            }
            AuthzRealmChange::MutateTuples { receipt, .. }
                if receipt.replay_guarantee_expires_at_unix_millis > now =>
            {
                if !self.is_retained_tuple_replay(mutation, now)? {
                    return Err(AuthzStoreError::RealmMutationConflict);
                }
            }
            AuthzRealmChange::MutateTuples { .. } => {}
        }
        Ok(())
    }
}

fn stamp(
    predecessor_revision: Option<AuthzRevision>,
    context: AuthzRealmMutationContext,
) -> AuthzRealmMutationStamp {
    AuthzRealmMutationStamp {
        format: AUTHZ_REALM_MUTATION_STAMP_FORMAT,
        predecessor_revision,
        mutation_fingerprint: [0; 32],
        active_placement_log_id: context.active_placement_log_id,
        serving_fence_term: context.serving_fence_term,
        source_id: context.source_id,
        source_journal_position: context.source_journal_position,
    }
}

fn bind_input_fingerprint(request: &BindSchemaRequest) -> Result<[u8; 32], AuthzStoreError> {
    let encoded = serde_json::to_vec(&(
        "bind_schema",
        &request.scope,
        &request.schema_ref,
        request.expected_generation,
        request.expected_revision,
    ))
    .map_err(storage_error)?;
    Ok(*blake3::hash(&encoded).as_bytes())
}

fn apply_tuple_changes(mut tuples: Vec<Tuple>, changes: &[TupleMutation]) -> Vec<Tuple> {
    let mut tuples = tuples.drain(..).collect::<BTreeSet<_>>();
    for change in changes {
        match change.kind {
            TupleMutationKind::Add => {
                tuples.insert(change.tuple.clone());
            }
            TupleMutationKind::Remove => {
                tuples.remove(&change.tuple);
            }
        }
    }
    tuples.into_iter().collect()
}

fn validate_predecessor(
    current: Option<&StoredRealmBinding>,
    current_revision: Option<AuthzRevision>,
    mutation: &AuthzRealmMutation,
) -> Result<(), AuthzStoreError> {
    let predecessor = mutation.stamp.predecessor_revision;
    if current
        .and_then(|stored| stored.mutation_stamp)
        .is_some_and(|stamp| stamp.predecessor_revision == predecessor)
    {
        return Err(AuthzStoreError::RealmMutationSibling { predecessor });
    }
    match (current, current_revision, predecessor) {
        (None, None, None) => Ok(()),
        (Some(_), Some(current), Some(expected)) if current == expected => Ok(()),
        (None, None, Some(_)) => Err(AuthzStoreError::RealmMutationLineageGap {
            current: None,
            predecessor,
        }),
        (Some(_), Some(current), None) => Err(AuthzStoreError::RealmMutationStale {
            current,
            incoming: mutation.revision(),
        }),
        (Some(_), Some(current), Some(expected)) if current < expected => {
            Err(AuthzStoreError::RealmMutationLineageGap {
                current: Some(current),
                predecessor,
            })
        }
        (Some(_), Some(current), Some(_)) => Err(AuthzStoreError::RealmMutationStale {
            current,
            incoming: mutation.revision(),
        }),
        _ => Err(AuthzStoreError::RealmMutationConflict),
    }
}

fn invalid_mutation(message: impl Into<String>) -> AuthzStoreError {
    AuthzStoreError::InvalidRealmMutation(message.into())
}

#[cfg(test)]
mod tests;
