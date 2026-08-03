//! Tenant-wide authorization schema catalogue replication.
//!
//! Every revision, including an unbound revision, is part of this one logical
//! catalogue. The catalogue and all realms for the tenant use the same replica
//! group; this module only supplies the typed storage boundary.

use std::collections::{BTreeMap, BTreeSet};

use anvil_authz::Schema;
use rocksdb::{Direction, IteratorMode, WriteBatch};
use serde::{Deserialize, Serialize};

use super::{
    AuthzRealmMutationContext, AuthzRepository, AuthzRevision, AuthzStoreError,
    PublishSchemaRequest, PublishedSchema, SchemaId, SchemaRef, StorageTenantId, StoredSchema,
    decode_json, encode_json, next_revision, push_component, schema_digest_key, schema_latest_key,
    schema_revision_key, storage_error, tenant_revision_key, validate_component,
    validate_stored_schema,
};
use crate::store::{CF_AUTHZ_SCHEMAS, CF_AUTHZ_TENANTS};

pub const AUTHZ_SCHEMA_CATALOGUE_FORMAT: u16 = 1;
pub const AUTHZ_SCHEMA_PUBLICATION_FORMAT: u16 = 1;
pub const AUTHZ_SCHEMA_PUBLICATION_STAMP_FORMAT: u16 = 1;
const CATALOGUE_HASH_DOMAIN: &[u8] = b"anvil.authz-schema-catalogue.v1\0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzSchemaRevision {
    pub schema_ref: SchemaRef,
    pub schema: Schema,
    pub published_at_revision: AuthzRevision,
}

impl From<StoredSchema> for AuthzSchemaRevision {
    fn from(stored: StoredSchema) -> Self {
        Self {
            schema_ref: stored.schema_ref,
            schema: stored.schema,
            published_at_revision: stored.published_at_revision,
        }
    }
}

impl From<&AuthzSchemaRevision> for StoredSchema {
    fn from(revision: &AuthzSchemaRevision) -> Self {
        Self {
            schema_ref: revision.schema_ref.clone(),
            schema: revision.schema.clone(),
            published_at_revision: revision.published_at_revision,
        }
    }
}

/// Complete schema state for one stable tenant identity. `authz_revision` is
/// the shared tenant Zanzibar revision, so it also advances when a realm in
/// this replica group changes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzSchemaCatalogue {
    pub format: u16,
    pub storage_tenant: StorageTenantId,
    pub authz_revision: AuthzRevision,
    pub schemas: Vec<AuthzSchemaRevision>,
}

impl AuthzSchemaCatalogue {
    pub fn validate(&self) -> Result<(), AuthzStoreError> {
        self.validate_with_limits(super::AuthzStoreLimits::default().evaluator)
    }

    fn validate_with_limits(
        &self,
        limits: anvil_authz::AuthorizationLimits,
    ) -> Result<(), AuthzStoreError> {
        if self.format != AUTHZ_SCHEMA_CATALOGUE_FORMAT
            || self.authz_revision == AuthzRevision::ZERO
            || self.schemas.is_empty()
        {
            return Err(invalid_publication(
                "catalogue format, revision, or schema set is invalid",
            ));
        }
        self.storage_tenant.validate()?;

        let mut previous: Option<(&SchemaId, u64)> = None;
        let mut publication_revisions = BTreeSet::new();
        for revision in &self.schemas {
            let stored = StoredSchema::from(revision);
            validate_stored_schema(&stored, &revision.schema_ref, limits)?;
            if revision.published_at_revision == AuthzRevision::ZERO
                || revision.published_at_revision > self.authz_revision
                || !publication_revisions.insert(revision.published_at_revision)
            {
                return Err(invalid_publication(
                    "schema publication revisions are invalid",
                ));
            }
            match previous {
                None if revision.schema_ref.schema_revision == 1 => {}
                Some((schema_id, previous_revision))
                    if schema_id == &revision.schema_ref.schema_id
                        && revision.schema_ref.schema_revision == previous_revision + 1 => {}
                Some((schema_id, _))
                    if schema_id < &revision.schema_ref.schema_id
                        && revision.schema_ref.schema_revision == 1 => {}
                _ => {
                    return Err(invalid_publication(
                        "schema catalogue must be sorted with contiguous revisions",
                    ));
                }
            }
            previous = Some((
                &revision.schema_ref.schema_id,
                revision.schema_ref.schema_revision,
            ));
        }
        Ok(())
    }

    pub fn candidate(&self) -> Result<AuthzSchemaCatalogueCandidate, AuthzStoreError> {
        self.validate()?;
        let encoded = encode_json(self)?;
        let encoded_bytes = u64::try_from(encoded.len())
            .map_err(|_| AuthzStoreError::Storage("schema catalogue size overflow".into()))?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(CATALOGUE_HASH_DOMAIN);
        hasher.update(&encoded);
        Ok(AuthzSchemaCatalogueCandidate {
            storage_tenant: self.storage_tenant.clone(),
            authz_revision: self.authz_revision,
            encoded_bytes,
            content_hash: *hasher.finalize().as_bytes(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzSchemaCatalogueCandidate {
    pub storage_tenant: StorageTenantId,
    pub authz_revision: AuthzRevision,
    pub encoded_bytes: u64,
    pub content_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzSchemaPublicationStamp {
    pub format: u16,
    pub predecessor_revision: Option<AuthzRevision>,
    pub mutation_fingerprint: [u8; 32],
    pub active_placement_log_id: crate::PlacementLogId,
    pub serving_fence_term: u64,
    pub source_id: crate::SourceId,
    pub source_journal_position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzSchemaPublicationMutation {
    pub format: u16,
    pub storage_tenant: StorageTenantId,
    pub command_id: String,
    pub schema: AuthzSchemaRevision,
    pub stamp: AuthzSchemaPublicationStamp,
}

impl AuthzSchemaPublicationMutation {
    pub fn revision(&self) -> AuthzRevision {
        self.schema.published_at_revision
    }

    fn computed_fingerprint(&self) -> [u8; 32] {
        let material = (
            self.format,
            &self.storage_tenant,
            &self.command_id,
            &self.schema,
            self.stamp.format,
            self.stamp.predecessor_revision,
            self.stamp.active_placement_log_id,
            self.stamp.serving_fence_term,
            self.stamp.source_id,
            self.stamp.source_journal_position,
        );
        let encoded = serde_json::to_vec(&material)
            .expect("typed schema publication fingerprint material serializes");
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"anvil.authz-schema-publication.v1");
        hasher.update(&encoded);
        *hasher.finalize().as_bytes()
    }

    fn set_computed_fingerprint(&mut self) {
        self.stamp.mutation_fingerprint = self.computed_fingerprint();
    }

    pub fn validate(&self) -> Result<(), AuthzStoreError> {
        self.validate_with_limits(Default::default())
    }

    fn validate_with_limits(
        &self,
        limits: anvil_authz::AuthorizationLimits,
    ) -> Result<(), AuthzStoreError> {
        if self.format != AUTHZ_SCHEMA_PUBLICATION_FORMAT
            || self.stamp.format != AUTHZ_SCHEMA_PUBLICATION_STAMP_FORMAT
        {
            return Err(invalid_publication("unsupported publication format"));
        }
        self.storage_tenant.validate()?;
        validate_component(
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
            return Err(invalid_publication(
                "predecessor, serving fence, or source identity is invalid",
            ));
        }
        let expected = match self.stamp.predecessor_revision {
            Some(predecessor) => next_revision(predecessor)?,
            None => AuthzRevision(1),
        };
        if self.revision() != expected {
            return Err(invalid_publication(
                "publication revision must immediately follow its predecessor",
            ));
        }
        let stored = StoredSchema::from(&self.schema);
        validate_stored_schema(&stored, &self.schema.schema_ref, limits)?;
        if self.stamp.mutation_fingerprint != self.computed_fingerprint() {
            return Err(invalid_publication(
                "publication fingerprint does not match its typed result",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatedAuthzSchemaPublication {
    pub result: PublishedSchema,
    pub mutation: Option<AuthzSchemaPublicationMutation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaAuthzSchemaPublicationApplied {
    pub revision: AuthzRevision,
    pub replayed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthzSchemaCatalogueApplied {
    pub revision: AuthzRevision,
    pub schema_count: usize,
}

impl AuthzRepository {
    pub fn coordinate_schema_publication(
        &self,
        request: PublishSchemaRequest,
        context: AuthzRealmMutationContext,
    ) -> Result<CoordinatedAuthzSchemaPublication, AuthzStoreError> {
        validate_publication_context(&context, self.limits.max_operation_id_bytes)?;
        let _guard = self.lock_writes()?;
        let mut batch = WriteBatch::default();
        let storage_tenant = request.storage_tenant.clone();
        let (result, stored) = self.prepare_schema_publication(request, &mut batch)?;
        let Some(stored) = stored else {
            return Ok(CoordinatedAuthzSchemaPublication {
                result,
                mutation: None,
            });
        };
        let predecessor_revision = result
            .authz_revision
            .0
            .checked_sub(1)
            .filter(|revision| *revision != 0)
            .map(AuthzRevision);
        let mut mutation = AuthzSchemaPublicationMutation {
            format: AUTHZ_SCHEMA_PUBLICATION_FORMAT,
            storage_tenant,
            command_id: context.command_id,
            schema: stored.into(),
            stamp: AuthzSchemaPublicationStamp {
                format: AUTHZ_SCHEMA_PUBLICATION_STAMP_FORMAT,
                predecessor_revision,
                mutation_fingerprint: [0; 32],
                active_placement_log_id: context.active_placement_log_id,
                serving_fence_term: context.serving_fence_term,
                source_id: context.source_id,
                source_journal_position: context.source_journal_position,
            },
        };
        mutation.set_computed_fingerprint();
        mutation.validate_with_limits(self.limits.evaluator)?;
        self.write(batch)?;
        Ok(CoordinatedAuthzSchemaPublication {
            result,
            mutation: Some(mutation),
        })
    }

    pub fn apply_schema_publication_replica(
        &self,
        mutation: &AuthzSchemaPublicationMutation,
    ) -> Result<ReplicaAuthzSchemaPublicationApplied, AuthzStoreError> {
        mutation.validate_with_limits(self.limits.evaluator)?;
        let _guard = self.lock_writes()?;
        let revision_key =
            schema_revision_key(&mutation.storage_tenant, &mutation.schema.schema_ref);
        if let Some(existing) = self.read_json::<StoredSchema>(CF_AUTHZ_SCHEMAS, &revision_key)? {
            let expected = StoredSchema::from(&mutation.schema);
            if existing != expected
                || self.tenant_revision(&mutation.storage_tenant)? < mutation.revision()
                || self.read_json::<SchemaRef>(
                    CF_AUTHZ_SCHEMAS,
                    &schema_digest_key(
                        &mutation.storage_tenant,
                        &mutation.schema.schema_ref.schema_id,
                        mutation.schema.schema_ref.schema_digest,
                    ),
                )? != Some(mutation.schema.schema_ref.clone())
            {
                return Err(AuthzStoreError::RealmMutationConflict);
            }
            return Ok(ReplicaAuthzSchemaPublicationApplied {
                revision: mutation.revision(),
                replayed: true,
            });
        }

        let current = self.tenant_revision(&mutation.storage_tenant)?;
        if Some(current).filter(|revision| *revision != AuthzRevision::ZERO)
            != mutation.stamp.predecessor_revision
        {
            return Err(if current >= mutation.revision() {
                AuthzStoreError::RealmMutationStale {
                    current,
                    incoming: mutation.revision(),
                }
            } else {
                AuthzStoreError::RealmMutationLineageGap {
                    current: (current != AuthzRevision::ZERO).then_some(current),
                    predecessor: mutation.stamp.predecessor_revision,
                }
            });
        }
        let latest_key = schema_latest_key(
            &mutation.storage_tenant,
            &mutation.schema.schema_ref.schema_id,
        );
        let latest = self
            .read_json::<u64>(CF_AUTHZ_SCHEMAS, &latest_key)?
            .unwrap_or(0);
        if latest.checked_add(1) != Some(mutation.schema.schema_ref.schema_revision) {
            return Err(AuthzStoreError::RealmMutationConflict);
        }
        let digest_key = schema_digest_key(
            &mutation.storage_tenant,
            &mutation.schema.schema_ref.schema_id,
            mutation.schema.schema_ref.schema_digest,
        );
        if self
            .read_json::<SchemaRef>(CF_AUTHZ_SCHEMAS, &digest_key)?
            .is_some()
        {
            return Err(AuthzStoreError::RealmMutationConflict);
        }
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            revision_key,
            encode_json(&StoredSchema::from(&mutation.schema))?,
        );
        batch.put_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            latest_key,
            encode_json(&mutation.schema.schema_ref.schema_revision)?,
        );
        batch.put_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            digest_key,
            encode_json(&mutation.schema.schema_ref)?,
        );
        self.stage_tenant_revision(&mut batch, &mutation.storage_tenant, mutation.revision())?;
        self.write(batch)?;
        Ok(ReplicaAuthzSchemaPublicationApplied {
            revision: mutation.revision(),
            replayed: false,
        })
    }

    pub fn export_authz_schema_catalogue(
        &self,
        tenant: &StorageTenantId,
    ) -> Result<Option<AuthzSchemaCatalogue>, AuthzStoreError> {
        tenant.validate()?;
        let authz_revision = self.tenant_revision(tenant)?;
        let mut schemas = Vec::<AuthzSchemaRevision>::new();
        let prefix = schema_tenant_prefix(b'S', tenant);
        for item in self.db.iterator_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            IteratorMode::From(&prefix, Direction::Forward),
        ) {
            let (key, value) = item.map_err(storage_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let stored = decode_json::<StoredSchema>(&value)?;
            if schema_revision_key(tenant, &stored.schema_ref).as_slice() != key.as_ref() {
                return Err(AuthzStoreError::Storage(
                    "persisted schema revision key is inconsistent".into(),
                ));
            }
            schemas.push(stored.into());
        }
        schemas.sort_by(|left, right| {
            left.schema_ref
                .schema_id
                .cmp(&right.schema_ref.schema_id)
                .then_with(|| {
                    left.schema_ref
                        .schema_revision
                        .cmp(&right.schema_ref.schema_revision)
                })
        });
        if authz_revision == AuthzRevision::ZERO && schemas.is_empty() {
            return Ok(None);
        }
        let catalogue = AuthzSchemaCatalogue {
            format: AUTHZ_SCHEMA_CATALOGUE_FORMAT,
            storage_tenant: tenant.clone(),
            authz_revision,
            schemas,
        };
        catalogue.validate_with_limits(self.limits.evaluator)?;
        Ok(Some(catalogue))
    }

    /// Replaces only this tenant's schema catalogue and shared revision after
    /// an exact quorum has selected `catalogue`. Realm data is untouched.
    pub fn install_quorum_reconciled_authz_schema_catalogue(
        &self,
        tenant: &StorageTenantId,
        catalogue: Option<&AuthzSchemaCatalogue>,
    ) -> Result<AuthzSchemaCatalogueApplied, AuthzStoreError> {
        tenant.validate()?;
        if let Some(catalogue) = catalogue {
            catalogue.validate_with_limits(self.limits.evaluator)?;
            if &catalogue.storage_tenant != tenant {
                return Err(AuthzStoreError::RealmMutationConflict);
            }
        }
        let _guard = self.lock_writes()?;
        let mut batch = WriteBatch::default();
        for tag in [b'S', b'L', b'D'] {
            let prefix = schema_tenant_prefix(tag, tenant);
            for item in self.db.iterator_cf(
                self.cf(CF_AUTHZ_SCHEMAS)?,
                IteratorMode::From(&prefix, Direction::Forward),
            ) {
                let (key, _) = item.map_err(storage_error)?;
                if !key.starts_with(&prefix) {
                    break;
                }
                batch.delete_cf(self.cf(CF_AUTHZ_SCHEMAS)?, key);
            }
        }
        batch.delete_cf(self.cf(CF_AUTHZ_TENANTS)?, tenant_revision_key(tenant));

        let Some(catalogue) = catalogue else {
            self.write(batch)?;
            return Ok(AuthzSchemaCatalogueApplied {
                revision: AuthzRevision::ZERO,
                schema_count: 0,
            });
        };
        let mut latest = BTreeMap::<SchemaId, u64>::new();
        for revision in &catalogue.schemas {
            let stored = StoredSchema::from(revision);
            batch.put_cf(
                self.cf(CF_AUTHZ_SCHEMAS)?,
                schema_revision_key(tenant, &revision.schema_ref),
                encode_json(&stored)?,
            );
            batch.put_cf(
                self.cf(CF_AUTHZ_SCHEMAS)?,
                schema_digest_key(
                    tenant,
                    &revision.schema_ref.schema_id,
                    revision.schema_ref.schema_digest,
                ),
                encode_json(&revision.schema_ref)?,
            );
            latest.insert(
                revision.schema_ref.schema_id.clone(),
                revision.schema_ref.schema_revision,
            );
        }
        for (schema_id, revision) in latest {
            batch.put_cf(
                self.cf(CF_AUTHZ_SCHEMAS)?,
                schema_latest_key(tenant, &schema_id),
                encode_json(&revision)?,
            );
        }
        self.stage_tenant_revision(&mut batch, tenant, catalogue.authz_revision)?;
        self.write(batch)?;
        Ok(AuthzSchemaCatalogueApplied {
            revision: catalogue.authz_revision,
            schema_count: catalogue.schemas.len(),
        })
    }
}

fn schema_tenant_prefix(tag: u8, tenant: &StorageTenantId) -> Vec<u8> {
    let mut prefix = vec![tag];
    push_component(&mut prefix, tenant.as_str().as_bytes());
    prefix
}

fn validate_publication_context(
    context: &AuthzRealmMutationContext,
    max_command_bytes: usize,
) -> Result<(), AuthzStoreError> {
    validate_component(&context.command_id, "command id", max_command_bytes)?;
    if context.serving_fence_term == 0
        || context.source_id.node_id == 0
        || context.source_id.source_epoch == [0; 32]
        || context.source_journal_position == 0
    {
        return Err(invalid_publication(
            "serving fence and source identity must be non-zero",
        ));
    }
    Ok(())
}

fn invalid_publication(message: impl Into<String>) -> AuthzStoreError {
    AuthzStoreError::InvalidRealmMutation(format!("schema publication: {}", message.into()))
}

#[cfg(test)]
mod tests;
