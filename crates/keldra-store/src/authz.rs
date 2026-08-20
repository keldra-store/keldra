use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use keldra_authz::{
    Authorization, AuthorizationCheck, AuthorizationLimits, ExactPath, NamespaceDefinition,
    ObjectId, ObjectRef, RealmId, RelationKind, Schema, Tuple,
};
use rocksdb::{DB, Direction, IteratorMode, WriteBatch, WriteOptions};
use serde::{Deserialize, Deserializer, Serialize, de, de::DeserializeOwned};
use thiserror::Error;

use crate::Store;
use crate::store::{
    CF_AUTHZ_BINDINGS, CF_AUTHZ_RECEIPTS, CF_AUTHZ_SCHEMAS, CF_AUTHZ_TENANTS, CF_AUTHZ_TUPLES,
};

pub const SYSTEM_STORAGE_TENANT_ID: &str = "_keldra";
pub const DEFAULT_AUTHZ_RECEIPT_RETENTION_SECONDS: u64 = 24 * 60 * 60;
pub const DEFAULT_AUTHZ_RECEIPT_MAX_ENTRIES: usize = 4_096;
pub const DEFAULT_AUTHZ_RECEIPT_MAX_BYTES: u64 = 16 * 1024 * 1024;
const MAX_EXTERNAL_STORAGE_TENANT_BYTES: usize = 63;
const MAX_ID_BYTES: usize = 256;
const MAX_OPERATION_ID_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct StorageTenantId(String);

impl StorageTenantId {
    pub fn parse(value: impl Into<String>) -> Result<Self, AuthzStoreError> {
        let value = value.into();
        validate_storage_tenant(&value)?;
        Ok(Self(value))
    }

    pub fn system() -> Self {
        Self(SYSTEM_STORAGE_TENANT_ID.to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_system(&self) -> bool {
        self.0 == SYSTEM_STORAGE_TENANT_ID
    }

    fn validate(&self) -> Result<(), AuthzStoreError> {
        validate_storage_tenant(self.as_str())
    }
}

impl<'de> Deserialize<'de> for StorageTenantId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl fmt::Display for StorageTenantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct AuthzScope {
    pub storage_tenant: StorageTenantId,
    pub realm: RealmId,
}

impl AuthzScope {
    pub fn new(storage_tenant: StorageTenantId, realm: RealmId) -> Result<Self, AuthzStoreError> {
        if storage_tenant.is_system() != realm.is_system() {
            return Err(AuthzStoreError::InvalidInput(
                "the protected system storage tenant and system realm must be used together".into(),
            ));
        }
        Ok(Self {
            storage_tenant,
            realm,
        })
    }

    pub fn system() -> Self {
        Self {
            storage_tenant: StorageTenantId::system(),
            realm: RealmId::system(),
        }
    }

    /// Canonical realm-binding order used by bounded cluster handoff.
    pub fn handoff_order_key(&self) -> Result<Vec<u8>, AuthzStoreError> {
        self.validate()?;
        Ok(binding_key(self))
    }

    fn validate(&self) -> Result<(), AuthzStoreError> {
        self.storage_tenant.validate()?;
        RealmId::parse(self.realm.as_str())?;
        if self.storage_tenant.is_system() != self.realm.is_system() {
            return Err(AuthzStoreError::InvalidInput(
                "the protected system storage tenant and system realm must be used together".into(),
            ));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for AuthzScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct EncodedScope {
            storage_tenant: StorageTenantId,
            realm: RealmId,
        }

        let encoded = EncodedScope::deserialize(deserializer)?;
        Self::new(encoded.storage_tenant, encoded.realm).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SchemaId(String);

impl SchemaId {
    pub fn parse(value: impl Into<String>) -> Result<Self, AuthzStoreError> {
        let value = value.into();
        validate_safe_component(&value, "schema id", MAX_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), AuthzStoreError> {
        validate_safe_component(self.as_str(), "schema id", MAX_ID_BYTES)
    }
}

impl<'de> Deserialize<'de> for SchemaId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct AuthzRevision(pub u64);

impl AuthzRevision {
    pub const ZERO: Self = Self(0);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaDigest(pub [u8; 32]);

impl fmt::Display for SchemaDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRef {
    pub schema_id: SchemaId,
    pub schema_revision: u64,
    pub schema_digest: SchemaDigest,
}

impl SchemaRef {
    fn validate(&self) -> Result<(), AuthzStoreError> {
        self.schema_id.validate()?;
        if self.schema_revision == 0 {
            return Err(AuthzStoreError::InvalidInput(
                "schema revision must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishSchemaRequest {
    pub storage_tenant: StorageTenantId,
    pub schema_id: SchemaId,
    pub schema: Schema,
    pub expected_revision: Option<AuthzRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedSchema {
    pub schema_ref: SchemaRef,
    pub authz_revision: AuthzRevision,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmBinding {
    pub scope: AuthzScope,
    pub schema_ref: SchemaRef,
    pub generation: u64,
    pub authz_revision: AuthzRevision,
    /// Authoritative active tuple count, updated in the same write batch as
    /// tuple mutations. This keeps mutation cost proportional to the batch
    /// instead of rescanning the complete realm on every write.
    pub tuple_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindSchemaRequest {
    pub scope: AuthzScope,
    pub schema_ref: SchemaRef,
    pub expected_generation: Option<u64>,
    pub expected_revision: Option<AuthzRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundRealm {
    pub binding: RealmBinding,
    pub replayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TupleMutationKind {
    Add,
    Remove,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TupleMutation {
    pub kind: TupleMutationKind,
    pub tuple: Tuple,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TupleBatchRequest {
    pub scope: AuthzScope,
    /// Authenticated caller fixed by the trusted server boundary. It scopes
    /// operation receipts; it is not a client-selected authorization subject.
    pub principal: ObjectRef,
    pub expected_revision: Option<AuthzRevision>,
    pub expected_binding_generation: u64,
    pub operation_id: Option<String>,
    pub mutations: Vec<TupleMutation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TupleBatchReceipt {
    pub scope: AuthzScope,
    pub principal: ObjectRef,
    pub authz_revision: AuthzRevision,
    pub binding_generation: u64,
    pub mutation_count: usize,
    pub replayed: bool,
    /// Zero only for internal mutations that did not request idempotency.
    pub replay_guarantee_expires_at_unix_millis: u64,
}

/// Internal composition used by first realm binding. Callers provide only the
/// new owner and the protected-realm CAS values; the repository constructs the
/// exact parent and owner tuples. This makes the helper incapable of carrying
/// unrelated system-realm mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtectedRealmOwnership {
    pub principal: ObjectRef,
    pub expected_revision: AuthzRevision,
    pub expected_binding_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicRealmBinding {
    pub realm: BoundRealm,
    pub system_grant: TupleBatchReceipt,
}

/// Current-state consistency. Anvil 0.5 retains exactly the current
/// authorization revision, so `Exact(current)` works and an older exact
/// revision has a stable expired result rather than silently moving forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthzConsistency {
    Latest,
    AtLeast(AuthzRevision),
    Exact(AuthzRevision),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealmSnapshot {
    pub scope: AuthzScope,
    pub revision: AuthzRevision,
    pub binding: RealmBinding,
    pub schema: Schema,
    pub tuples: Vec<Tuple>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzBatchCheck {
    pub revision: AuthzRevision,
    pub allowed: Vec<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthzStoreLimits {
    pub max_mutations_per_batch: usize,
    pub max_checks_per_batch: usize,
    pub max_operation_id_bytes: usize,
    pub receipt_retention_millis: u64,
    pub max_receipt_entries: usize,
    pub max_receipt_bytes: u64,
    pub evaluator: AuthorizationLimits,
}

impl Default for AuthzStoreLimits {
    fn default() -> Self {
        Self {
            max_mutations_per_batch: 1_000,
            max_checks_per_batch: 1_000,
            max_operation_id_bytes: MAX_OPERATION_ID_BYTES,
            receipt_retention_millis: DEFAULT_AUTHZ_RECEIPT_RETENTION_SECONDS * 1_000,
            max_receipt_entries: DEFAULT_AUTHZ_RECEIPT_MAX_ENTRIES,
            max_receipt_bytes: DEFAULT_AUTHZ_RECEIPT_MAX_BYTES,
            evaluator: AuthorizationLimits::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum AuthzStoreError {
    #[error("invalid authorization input: {0}")]
    InvalidInput(String),
    #[error("authorization scope {0}/{1} has no schema binding")]
    MissingBinding(StorageTenantId, RealmId),
    #[error("authorization schema {0}@{1} was not found")]
    SchemaNotFound(SchemaId, u64),
    #[error("authorization revision conflict: expected {expected:?}, current {current:?}")]
    RevisionConflict {
        expected: AuthzRevision,
        current: AuthzRevision,
    },
    #[error("binding generation conflict: expected {expected:?}, current {current:?}")]
    BindingGenerationConflict {
        expected: Option<u64>,
        current: Option<u64>,
    },
    #[error("requested revision {required:?} is ahead of current revision {current:?}")]
    RevisionNotAvailable {
        required: AuthzRevision,
        current: AuthzRevision,
    },
    #[error("AUTHZ_REVISION_EXPIRED: requested {requested:?}, current {current:?}")]
    RevisionExpired {
        requested: AuthzRevision,
        current: AuthzRevision,
    },
    #[error("authorization tuple receipt capacity is exhausted by unexpired guarantees")]
    ReceiptCapacity,
    #[error("source journal capacity is exhausted")]
    SourceJournalCapacity,
    #[error("operation id is already bound to different tuple input")]
    OperationMismatch,
    #[error("invalid replicated authorization realm mutation: {0}")]
    InvalidRealmMutation(String),
    #[error(
        "replicated authorization realm mutation has a lineage gap: local revision {current:?}, incoming predecessor {predecessor:?}"
    )]
    RealmMutationLineageGap {
        current: Option<AuthzRevision>,
        predecessor: Option<AuthzRevision>,
    },
    #[error(
        "replicated authorization realm mutation is stale: local revision {current:?}, incoming revision {incoming:?}"
    )]
    RealmMutationStale {
        current: AuthzRevision,
        incoming: AuthzRevision,
    },
    #[error(
        "replicated authorization realm mutations are contradictory siblings of {predecessor:?}"
    )]
    RealmMutationSibling { predecessor: Option<AuthzRevision> },
    #[error("replicated authorization realm mutation conflicts with durable state")]
    RealmMutationConflict,
    #[error("authorization validation failed: {0}")]
    Authorization(#[from] keldra_authz::AuthorizationError),
    #[error("authorization storage failed: {0}")]
    Storage(String),
}

#[derive(Clone)]
pub struct AuthzRepository {
    db: Arc<DB>,
    write_lock: Arc<Mutex<()>>,
    sync_writes: bool,
    limits: AuthzStoreLimits,
}

impl fmt::Debug for AuthzRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthzRepository")
            .finish_non_exhaustive()
    }
}

impl Store {
    pub fn authz(&self) -> AuthzRepository {
        AuthzRepository {
            db: self.db.clone(),
            write_lock: self.authz_write_lock.clone(),
            sync_writes: self.sync_writes,
            limits: AuthzStoreLimits::default(),
        }
    }

    pub fn authz_with_limits(&self, limits: AuthzStoreLimits) -> AuthzRepository {
        AuthzRepository {
            db: self.db.clone(),
            write_lock: self.authz_write_lock.clone(),
            sync_writes: self.sync_writes,
            limits,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredSchema {
    pub(crate) schema_ref: SchemaRef,
    pub(crate) schema: Schema,
    pub(crate) published_at_revision: AuthzRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredTuple {
    tuple: Tuple,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredTupleReceipt {
    format: u16,
    operation_id: String,
    created_at_unix_millis: u64,
    expires_at_unix_millis: u64,
    fingerprint: [u8; 32],
    receipt: TupleBatchReceipt,
    /// Present only for coordinator-produced 0.5.1 realm mutations. Released
    /// 0.5.0 receipts decode with this absent and remain valid local receipts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    realm_mutation: Option<AuthzRealmMutation>,
}

const STORED_TUPLE_RECEIPT_FORMAT: u16 = 1;

#[derive(Default)]
struct TupleReceiptInventory {
    retained_entries: usize,
    retained_bytes: u64,
    expired_keys: Vec<Vec<u8>>,
}

impl AuthzRepository {
    pub fn limits(&self) -> AuthzStoreLimits {
        self.limits
    }

    pub fn tenant_revision(
        &self,
        tenant: &StorageTenantId,
    ) -> Result<AuthzRevision, AuthzStoreError> {
        tenant.validate()?;
        match self.read_json(CF_AUTHZ_TENANTS, &tenant_revision_key(tenant))? {
            None => Ok(AuthzRevision::ZERO),
            Some(AuthzRevision(0)) => Err(AuthzStoreError::Storage(
                "persisted authorization revision must be nonzero".into(),
            )),
            Some(revision) => Ok(revision),
        }
    }

    pub fn get_binding(&self, scope: &AuthzScope) -> Result<Option<RealmBinding>, AuthzStoreError> {
        scope.validate()?;
        let binding = self.read_json::<RealmBinding>(CF_AUTHZ_BINDINGS, &binding_key(scope))?;
        if let Some(binding) = binding.as_ref() {
            validate_binding(binding, scope)?;
        }
        Ok(binding)
    }

    pub fn get_schema(
        &self,
        tenant: &StorageTenantId,
        schema_ref: &SchemaRef,
    ) -> Result<Option<Schema>, AuthzStoreError> {
        tenant.validate()?;
        schema_ref.validate()?;
        let Some(stored) = self.read_json::<StoredSchema>(
            CF_AUTHZ_SCHEMAS,
            &schema_revision_key(tenant, schema_ref),
        )?
        else {
            return Ok(None);
        };
        validate_stored_schema(&stored, schema_ref, self.limits.evaluator)?;
        Ok(Some(stored.schema))
    }

    pub fn publish_schema(
        &self,
        request: PublishSchemaRequest,
    ) -> Result<PublishedSchema, AuthzStoreError> {
        let _guard = self.lock_writes()?;
        let mut batch = WriteBatch::default();
        let (published, _) = self.prepare_schema_publication(request, &mut batch)?;
        if !published.replayed {
            self.write(batch)?;
        }
        Ok(published)
    }

    pub(crate) fn prepare_schema_publication(
        &self,
        request: PublishSchemaRequest,
        batch: &mut WriteBatch,
    ) -> Result<(PublishedSchema, Option<StoredSchema>), AuthzStoreError> {
        request.storage_tenant.validate()?;
        request.schema_id.validate()?;
        let canonical = canonical_schema(request.schema, self.limits.evaluator)?;
        let canonical_bytes = serde_json::to_vec(&canonical).map_err(storage_error)?;
        let digest = SchemaDigest(*blake3::hash(&canonical_bytes).as_bytes());

        if let Some(existing_ref) = self.read_json::<SchemaRef>(
            CF_AUTHZ_SCHEMAS,
            &schema_digest_key(&request.storage_tenant, &request.schema_id, digest),
        )? {
            let stored = self.require_schema(&request.storage_tenant, &existing_ref)?;
            if stored.schema != canonical {
                return Err(AuthzStoreError::Storage(
                    "authorization schema digest collision".into(),
                ));
            }
            return Ok((
                PublishedSchema {
                    schema_ref: existing_ref,
                    authz_revision: stored.published_at_revision,
                    replayed: true,
                },
                None,
            ));
        }

        let current = self.tenant_revision(&request.storage_tenant)?;
        require_revision(request.expected_revision, current)?;
        let latest = self.read_json::<u64>(
            CF_AUTHZ_SCHEMAS,
            &schema_latest_key(&request.storage_tenant, &request.schema_id),
        )?;
        if latest == Some(0) {
            return Err(AuthzStoreError::Storage(
                "persisted latest schema revision must be nonzero".into(),
            ));
        }
        let latest = latest.unwrap_or(0);
        let schema_revision = latest
            .checked_add(1)
            .ok_or_else(|| AuthzStoreError::Storage("schema revision overflow".into()))?;
        let authz_revision = next_revision(current)?;
        let schema_ref = SchemaRef {
            schema_id: request.schema_id,
            schema_revision,
            schema_digest: digest,
        };
        let stored = StoredSchema {
            schema_ref: schema_ref.clone(),
            schema: canonical,
            published_at_revision: authz_revision,
        };

        batch.put_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            schema_revision_key(&request.storage_tenant, &schema_ref),
            encode_json(&stored)?,
        );
        batch.put_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            schema_latest_key(&request.storage_tenant, &schema_ref.schema_id),
            encode_json(&schema_revision)?,
        );
        batch.put_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            schema_digest_key(
                &request.storage_tenant,
                &schema_ref.schema_id,
                schema_ref.schema_digest,
            ),
            encode_json(&schema_ref)?,
        );
        self.stage_tenant_revision(batch, &request.storage_tenant, authz_revision)?;
        Ok((
            PublishedSchema {
                schema_ref,
                authz_revision,
                replayed: false,
            },
            Some(stored),
        ))
    }

    pub fn bind_schema(&self, request: BindSchemaRequest) -> Result<BoundRealm, AuthzStoreError> {
        let _guard = self.lock_writes()?;
        let mut batch = WriteBatch::default();
        let bound = self.prepare_binding(&request, false, &mut batch)?;
        if !bound.replayed {
            self.write(batch)?;
        }
        Ok(bound)
    }

    fn prepare_binding(
        &self,
        request: &BindSchemaRequest,
        require_absent: bool,
        batch: &mut WriteBatch,
    ) -> Result<BoundRealm, AuthzStoreError> {
        request.scope.validate()?;
        request.schema_ref.validate()?;
        let schema = self.require_schema(&request.scope.storage_tenant, &request.schema_ref)?;
        if schema.schema_ref.schema_digest != request.schema_ref.schema_digest {
            return Err(AuthzStoreError::SchemaNotFound(
                request.schema_ref.schema_id.clone(),
                request.schema_ref.schema_revision,
            ));
        }
        let binding_key = binding_key(&request.scope);
        let existing = self.get_binding(&request.scope)?;
        if require_absent && existing.is_some() {
            return Err(AuthzStoreError::BindingGenerationConflict {
                expected: request.expected_generation,
                current: existing.as_ref().map(|binding| binding.generation),
            });
        }
        if let Some(binding) = existing.as_ref()
            && binding.schema_ref == request.schema_ref
        {
            return Ok(BoundRealm {
                binding: binding.clone(),
                replayed: true,
            });
        }
        let expected_matches = match &existing {
            None => request.expected_generation.unwrap_or(0) == 0,
            Some(binding) => request.expected_generation == Some(binding.generation),
        };
        if !expected_matches {
            return Err(AuthzStoreError::BindingGenerationConflict {
                expected: request.expected_generation,
                current: existing.as_ref().map(|binding| binding.generation),
            });
        }

        let current = self.tenant_revision(&request.scope.storage_tenant)?;
        require_revision(request.expected_revision, current)?;
        let current_tuples = self.read_tuples(&request.scope)?;
        if existing
            .as_ref()
            .is_some_and(|binding| binding.tuple_count != current_tuples.len())
        {
            return Err(AuthzStoreError::Storage(
                "persisted authorization tuple count is inconsistent".into(),
            ));
        }
        let tuple_count = current_tuples.len();
        Authorization::new(
            request.scope.realm.clone(),
            schema.schema,
            current_tuples,
            self.limits.evaluator,
        )?;
        let generation = match existing.as_ref() {
            None => 1,
            Some(binding) => binding
                .generation
                .checked_add(1)
                .ok_or_else(|| AuthzStoreError::Storage("binding generation overflow".into()))?,
        };
        let authz_revision = next_revision(current)?;
        let binding = RealmBinding {
            scope: request.scope.clone(),
            schema_ref: request.schema_ref.clone(),
            generation,
            authz_revision,
            tuple_count,
        };
        batch.put_cf(
            self.cf(CF_AUTHZ_BINDINGS)?,
            binding_key,
            encode_json(&binding)?,
        );
        self.stage_tenant_revision(batch, &request.scope.storage_tenant, authz_revision)?;
        Ok(BoundRealm {
            binding,
            replayed: false,
        })
    }

    pub fn mutate_tuples(
        &self,
        request: TupleBatchRequest,
    ) -> Result<TupleBatchReceipt, AuthzStoreError> {
        let _guard = self.lock_writes()?;
        let mut batch = WriteBatch::default();
        let receipt = self.prepare_tuple_batch(&request, &mut batch)?;
        if !receipt.replayed {
            self.write(batch)?;
        }
        Ok(receipt)
    }

    /// Creates a custom realm and grants its protected system-realm ownership
    /// in one durable metadata write. Both sides use the ordinary binding and
    /// tuple validators and advance their own storage tenant revision once.
    pub fn bind_schema_with_protected_owner(
        &self,
        binding: BindSchemaRequest,
        protected: ProtectedRealmOwnership,
    ) -> Result<AtomicRealmBinding, AuthzStoreError> {
        if binding.scope.realm.is_system() {
            return Err(AuthzStoreError::InvalidInput(
                "atomic first binding requires one custom realm".into(),
            ));
        }
        if binding.expected_generation.unwrap_or(0) != 0 {
            return Err(AuthzStoreError::InvalidInput(
                "first binding expected generation must be absent or zero".into(),
            ));
        }

        let protected_request = protected_realm_owner_request(&binding.scope, protected)?;
        let _guard = self.lock_writes()?;
        let mut batch = WriteBatch::default();
        let realm = self.prepare_binding(&binding, true, &mut batch)?;
        if realm.binding.generation != 1 {
            return Err(AuthzStoreError::BindingGenerationConflict {
                expected: binding.expected_generation,
                current: Some(realm.binding.generation),
            });
        }
        let system_grant = self.prepare_tuple_batch(&protected_request, &mut batch)?;
        if !realm.replayed || !system_grant.replayed {
            self.write(batch)?;
        }
        Ok(AtomicRealmBinding {
            realm,
            system_grant,
        })
    }

    pub(crate) fn prepare_tuple_batch(
        &self,
        request: &TupleBatchRequest,
        batch: &mut WriteBatch,
    ) -> Result<TupleBatchReceipt, AuthzStoreError> {
        let canonical_mutations = self.validate_mutation_request(request)?;
        let fingerprint = tuple_fingerprint(request, &canonical_mutations)?;
        if request.operation_id.is_some()
            && (self.limits.receipt_retention_millis == 0
                || self.limits.max_receipt_entries == 0
                || self.limits.max_receipt_bytes == 0)
        {
            return Err(AuthzStoreError::InvalidInput(
                "authorization tuple receipt limits must all be non-zero".into(),
            ));
        }
        let now = current_unix_millis()?;

        if let Some(operation_id) = request.operation_id.as_deref()
            && let Some(stored) = self.read_json::<StoredTupleReceipt>(
                CF_AUTHZ_RECEIPTS,
                &receipt_key(
                    &request.scope.storage_tenant,
                    &request.principal,
                    operation_id,
                )?,
            )?
        {
            validate_stored_tuple_receipt(&stored, request)?;
            if stored.expires_at_unix_millis > now {
                if stored.fingerprint != fingerprint {
                    return Err(AuthzStoreError::OperationMismatch);
                }
                if stored.receipt.scope != request.scope
                    || stored.receipt.mutation_count != canonical_mutations.len()
                {
                    return Err(AuthzStoreError::Storage(
                        "persisted authorization tuple receipt is inconsistent".into(),
                    ));
                }
                let mut receipt = stored.receipt;
                receipt.replayed = true;
                return Ok(receipt);
            }
            batch.delete_cf(
                self.cf(CF_AUTHZ_RECEIPTS)?,
                receipt_key(
                    &request.scope.storage_tenant,
                    &request.principal,
                    operation_id,
                )?,
            );
        }

        let binding = self.get_binding(&request.scope)?.ok_or_else(|| {
            AuthzStoreError::MissingBinding(
                request.scope.storage_tenant.clone(),
                request.scope.realm.clone(),
            )
        })?;
        if binding.generation != request.expected_binding_generation {
            return Err(AuthzStoreError::BindingGenerationConflict {
                expected: Some(request.expected_binding_generation),
                current: Some(binding.generation),
            });
        }
        if binding.tuple_count > self.limits.evaluator.max_tuples {
            return Err(AuthzStoreError::Storage(
                "persisted authorization tuple count exceeds the configured limit".into(),
            ));
        }
        let current = self.tenant_revision(&request.scope.storage_tenant)?;
        require_revision(request.expected_revision, current)?;
        let stored_schema =
            self.require_schema(&request.scope.storage_tenant, &binding.schema_ref)?;
        Authorization::new(
            request.scope.realm.clone(),
            stored_schema.schema.clone(),
            canonical_mutations
                .iter()
                .map(|mutation| mutation.tuple.clone()),
            self.limits.evaluator,
        )?;
        let mut tuple_count = binding.tuple_count;
        let mut changed = false;
        for mutation in &canonical_mutations {
            let key = tuple_key(&request.scope, &mutation.tuple)?;
            let existing = self.read_json::<StoredTuple>(CF_AUTHZ_TUPLES, &key)?;
            if let Some(existing) = existing.as_ref()
                && existing.tuple != mutation.tuple
            {
                return Err(AuthzStoreError::Storage(
                    "authorization tuple digest collision".into(),
                ));
            }
            match mutation.kind {
                TupleMutationKind::Add => {
                    if existing.is_none() {
                        tuple_count = tuple_count.checked_add(1).ok_or_else(|| {
                            AuthzStoreError::Storage("authorization tuple count overflow".into())
                        })?;
                        changed = true;
                        batch.put_cf(
                            self.cf(CF_AUTHZ_TUPLES)?,
                            key,
                            encode_json(&StoredTuple {
                                tuple: mutation.tuple.clone(),
                            })?,
                        );
                    }
                }
                TupleMutationKind::Remove => {
                    if existing.is_some() {
                        tuple_count = tuple_count.checked_sub(1).ok_or_else(|| {
                            AuthzStoreError::Storage("authorization tuple count underflow".into())
                        })?;
                        changed = true;
                        batch.delete_cf(self.cf(CF_AUTHZ_TUPLES)?, key);
                    }
                }
            }
        }
        if tuple_count > self.limits.evaluator.max_tuples {
            return Err(AuthzStoreError::InvalidInput(format!(
                "realm would contain {tuple_count} tuples, exceeding {}",
                self.limits.evaluator.max_tuples
            )));
        }
        if !changed && request.operation_id.is_none() {
            return Ok(TupleBatchReceipt {
                scope: request.scope.clone(),
                principal: request.principal.clone(),
                authz_revision: current,
                binding_generation: binding.generation,
                mutation_count: canonical_mutations.len(),
                replayed: true,
                replay_guarantee_expires_at_unix_millis: 0,
            });
        }

        let authz_revision = next_revision(current)?;
        let replay_guarantee_expires_at_unix_millis = if request.operation_id.is_some() {
            now.checked_add(self.limits.receipt_retention_millis)
                .ok_or_else(|| {
                    AuthzStoreError::Storage("authorization tuple receipt expiry overflow".into())
                })?
        } else {
            0
        };
        let receipt = TupleBatchReceipt {
            scope: request.scope.clone(),
            principal: request.principal.clone(),
            authz_revision,
            binding_generation: binding.generation,
            mutation_count: canonical_mutations.len(),
            replayed: false,
            replay_guarantee_expires_at_unix_millis,
        };
        let mut updated_binding = binding;
        updated_binding.tuple_count = tuple_count;
        batch.put_cf(
            self.cf(CF_AUTHZ_BINDINGS)?,
            binding_key(&request.scope),
            encode_json(&updated_binding)?,
        );
        self.stage_tenant_revision(batch, &request.scope.storage_tenant, authz_revision)?;
        if let Some(operation_id) = request.operation_id.as_deref() {
            let key = receipt_key(
                &request.scope.storage_tenant,
                &request.principal,
                operation_id,
            )?;
            let stored = StoredTupleReceipt {
                format: STORED_TUPLE_RECEIPT_FORMAT,
                operation_id: operation_id.to_owned(),
                created_at_unix_millis: now,
                expires_at_unix_millis: replay_guarantee_expires_at_unix_millis,
                fingerprint,
                receipt: receipt.clone(),
                realm_mutation: None,
            };
            let encoded = encode_json(&stored)?;
            let encoded_bytes = receipt_record_bytes(&key, &encoded)?;
            let inventory = self.tuple_receipt_inventory(now)?;
            let next_entries = inventory
                .retained_entries
                .checked_add(1)
                .ok_or(AuthzStoreError::ReceiptCapacity)?;
            let next_bytes = inventory
                .retained_bytes
                .checked_add(encoded_bytes)
                .ok_or(AuthzStoreError::ReceiptCapacity)?;
            if next_entries > self.limits.max_receipt_entries
                || next_bytes > self.limits.max_receipt_bytes
            {
                return Err(AuthzStoreError::ReceiptCapacity);
            }
            for expired_key in inventory.expired_keys {
                batch.delete_cf(self.cf(CF_AUTHZ_RECEIPTS)?, expired_key);
            }
            batch.put_cf(self.cf(CF_AUTHZ_RECEIPTS)?, key, encoded);
        }
        Ok(receipt)
    }

    /// Restore the original tenant-revision CAS for a retained semantic retry.
    ///
    /// Some trusted protocol adapters reconstruct a request after the original
    /// mutation advanced the tenant revision. The operation ID and canonical
    /// tuple input are stable, but copying the now-current revision would make
    /// the ordinary receipt fingerprint look different. This helper proves the
    /// reconstructed tuple input against the retained receipt before restoring
    /// the exact predecessor used by the original request. The normal mutation
    /// path still owns replay, journal recovery, and replica durability.
    pub fn restore_retained_tuple_replay_precondition(
        &self,
        mut request: TupleBatchRequest,
    ) -> Result<TupleBatchRequest, AuthzStoreError> {
        let canonical_mutations = self.validate_mutation_request(&request)?;
        let operation_id = request.operation_id.as_deref().ok_or_else(|| {
            AuthzStoreError::InvalidInput("a retained tuple replay requires an operation id".into())
        })?;
        let key = receipt_key(
            &request.scope.storage_tenant,
            &request.principal,
            operation_id,
        )?;
        let Some(stored) = self.read_json::<StoredTupleReceipt>(CF_AUTHZ_RECEIPTS, &key)? else {
            return Ok(request);
        };
        validate_stored_tuple_receipt(&stored, &request)?;
        if stored.expires_at_unix_millis <= current_unix_millis()? {
            return Ok(request);
        }

        let predecessor = stored
            .receipt
            .authz_revision
            .0
            .checked_sub(1)
            .map(AuthzRevision)
            .ok_or_else(|| {
                AuthzStoreError::Storage(
                    "persisted authorization tuple receipt has no predecessor revision".into(),
                )
            })?;
        request.expected_revision = Some(predecessor);
        if stored.fingerprint != tuple_fingerprint(&request, &canonical_mutations)? {
            return Err(AuthzStoreError::OperationMismatch);
        }
        Ok(request)
    }

    fn tuple_receipt_inventory(
        &self,
        now_unix_millis: u64,
    ) -> Result<TupleReceiptInventory, AuthzStoreError> {
        if self.limits.receipt_retention_millis == 0
            || self.limits.max_receipt_entries == 0
            || self.limits.max_receipt_bytes == 0
        {
            return Err(AuthzStoreError::InvalidInput(
                "authorization tuple receipt limits must all be non-zero".into(),
            ));
        }
        let mut inventory = TupleReceiptInventory::default();
        for item in self
            .db
            .iterator_cf(self.cf(CF_AUTHZ_RECEIPTS)?, IteratorMode::Start)
        {
            let (key, encoded) = item.map_err(storage_error)?;
            let stored = decode_json::<StoredTupleReceipt>(&encoded)?;
            validate_stored_tuple_receipt_shape(&stored)?;
            let expected_key = receipt_key(
                &stored.receipt.scope.storage_tenant,
                &stored.receipt.principal,
                &stored.operation_id,
            )?;
            if key.as_ref() != expected_key.as_slice() {
                return Err(AuthzStoreError::Storage(
                    "persisted authorization tuple receipt key is inconsistent".into(),
                ));
            }
            if stored.expires_at_unix_millis <= now_unix_millis {
                inventory.expired_keys.push(key.to_vec());
                continue;
            }
            inventory.retained_entries = inventory
                .retained_entries
                .checked_add(1)
                .ok_or(AuthzStoreError::ReceiptCapacity)?;
            inventory.retained_bytes = inventory
                .retained_bytes
                .checked_add(receipt_record_bytes(&key, &encoded)?)
                .ok_or(AuthzStoreError::ReceiptCapacity)?;
        }
        Ok(inventory)
    }

    fn validate_mutation_request(
        &self,
        request: &TupleBatchRequest,
    ) -> Result<Vec<TupleMutation>, AuthzStoreError> {
        request.scope.validate()?;
        validate_principal(&request.principal)?;
        if request.mutations.is_empty() {
            return Err(AuthzStoreError::InvalidInput(
                "tuple mutation batch must not be empty".into(),
            ));
        }
        if request.mutations.len() > self.limits.max_mutations_per_batch {
            return Err(AuthzStoreError::InvalidInput(format!(
                "tuple mutation batch has {} entries, exceeding {}",
                request.mutations.len(),
                self.limits.max_mutations_per_batch
            )));
        }
        if let Some(operation_id) = request.operation_id.as_deref() {
            validate_component(
                operation_id,
                "operation id",
                self.limits.max_operation_id_bytes,
            )?;
        }
        let mut mutations = request.mutations.clone();
        mutations.sort();
        let mut seen = HashSet::with_capacity(mutations.len());
        for mutation in &mutations {
            let encoded = serde_json::to_vec(&mutation.tuple).map_err(storage_error)?;
            if !seen.insert(encoded) {
                return Err(AuthzStoreError::InvalidInput(
                    "a tuple may appear only once in a mutation batch".into(),
                ));
            }
        }
        Ok(mutations)
    }

    pub fn realm_snapshot(
        &self,
        scope: &AuthzScope,
        consistency: AuthzConsistency,
    ) -> Result<RealmSnapshot, AuthzStoreError> {
        scope.validate()?;
        let snapshot = self.db.snapshot();
        let stored_revision = snapshot
            .get_cf(
                self.cf(CF_AUTHZ_TENANTS)?,
                tenant_revision_key(&scope.storage_tenant),
            )
            .map_err(storage_error)?
            .map(|bytes| decode_json::<AuthzRevision>(&bytes))
            .transpose()?;
        let revision = match stored_revision {
            None => AuthzRevision::ZERO,
            Some(AuthzRevision(0)) => {
                return Err(AuthzStoreError::Storage(
                    "persisted authorization revision must be nonzero".into(),
                ));
            }
            Some(revision) => revision,
        };
        match consistency {
            AuthzConsistency::Latest => {}
            AuthzConsistency::AtLeast(required) if revision < required => {
                return Err(AuthzStoreError::RevisionNotAvailable {
                    required,
                    current: revision,
                });
            }
            AuthzConsistency::Exact(requested) if requested < revision => {
                return Err(AuthzStoreError::RevisionExpired {
                    requested,
                    current: revision,
                });
            }
            AuthzConsistency::Exact(required) if required > revision => {
                return Err(AuthzStoreError::RevisionNotAvailable {
                    required,
                    current: revision,
                });
            }
            AuthzConsistency::AtLeast(_) | AuthzConsistency::Exact(_) => {}
        }
        let binding = snapshot
            .get_cf(self.cf(CF_AUTHZ_BINDINGS)?, binding_key(scope))
            .map_err(storage_error)?
            .map(|bytes| decode_json::<RealmBinding>(&bytes))
            .transpose()?
            .ok_or_else(|| {
                AuthzStoreError::MissingBinding(scope.storage_tenant.clone(), scope.realm.clone())
            })?;
        validate_binding(&binding, scope)?;
        if binding.authz_revision > revision {
            return Err(AuthzStoreError::Storage(
                "persisted realm binding is ahead of the tenant authorization revision".into(),
            ));
        }
        let stored_schema = snapshot
            .get_cf(
                self.cf(CF_AUTHZ_SCHEMAS)?,
                schema_revision_key(&scope.storage_tenant, &binding.schema_ref),
            )
            .map_err(storage_error)?
            .map(|bytes| decode_json::<StoredSchema>(&bytes))
            .transpose()?
            .ok_or_else(|| {
                AuthzStoreError::SchemaNotFound(
                    binding.schema_ref.schema_id.clone(),
                    binding.schema_ref.schema_revision,
                )
            })?;
        if stored_schema.schema_ref != binding.schema_ref {
            return Err(AuthzStoreError::Storage(
                "realm binding schema reference does not match stored schema".into(),
            ));
        }
        validate_stored_schema(&stored_schema, &binding.schema_ref, self.limits.evaluator)?;
        let prefix = tuple_prefix(scope);
        let mut tuples = Vec::new();
        for item in snapshot.iterator_cf(
            self.cf(CF_AUTHZ_TUPLES)?,
            IteratorMode::From(&prefix, Direction::Forward),
        ) {
            let (key, value) = item.map_err(storage_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let tuple = decode_json::<StoredTuple>(&value)?.tuple;
            if tuple_key(scope, &tuple)?.as_slice() != key.as_ref() {
                return Err(AuthzStoreError::Storage(
                    "persisted authorization tuple key is inconsistent".into(),
                ));
            }
            tuples.push(tuple);
        }
        tuples.sort();
        if tuples.len() != binding.tuple_count {
            return Err(AuthzStoreError::Storage(
                "persisted authorization tuple count is inconsistent".into(),
            ));
        }
        Ok(RealmSnapshot {
            scope: scope.clone(),
            revision,
            binding,
            schema: stored_schema.schema,
            tuples,
        })
    }

    pub fn check(
        &self,
        scope: &AuthzScope,
        consistency: AuthzConsistency,
        check: &AuthorizationCheck,
    ) -> Result<(bool, AuthzRevision), AuthzStoreError> {
        let result = self.batch_check(scope, consistency, std::slice::from_ref(check))?;
        Ok((result.allowed[0], result.revision))
    }

    pub fn batch_check(
        &self,
        scope: &AuthzScope,
        consistency: AuthzConsistency,
        checks: &[AuthorizationCheck],
    ) -> Result<AuthzBatchCheck, AuthzStoreError> {
        if checks.len() > self.limits.max_checks_per_batch {
            return Err(AuthzStoreError::InvalidInput(format!(
                "authorization check batch has {} entries, exceeding {}",
                checks.len(),
                self.limits.max_checks_per_batch
            )));
        }
        let snapshot = self.realm_snapshot(scope, consistency)?;
        let authorization = Authorization::new(
            scope.realm.clone(),
            snapshot.schema,
            snapshot.tuples,
            self.limits.evaluator,
        )?;
        let allowed = checks
            .iter()
            .map(|check| authorization.check(check))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(AuthzBatchCheck {
            revision: snapshot.revision,
            allowed,
        })
    }

    fn require_schema(
        &self,
        tenant: &StorageTenantId,
        schema_ref: &SchemaRef,
    ) -> Result<StoredSchema, AuthzStoreError> {
        tenant.validate()?;
        schema_ref.validate()?;
        let stored = self
            .read_json::<StoredSchema>(CF_AUTHZ_SCHEMAS, &schema_revision_key(tenant, schema_ref))?
            .ok_or_else(|| {
                AuthzStoreError::SchemaNotFound(
                    schema_ref.schema_id.clone(),
                    schema_ref.schema_revision,
                )
            })?;
        validate_stored_schema(&stored, schema_ref, self.limits.evaluator)?;
        Ok(stored)
    }

    fn read_tuples(&self, scope: &AuthzScope) -> Result<Vec<Tuple>, AuthzStoreError> {
        let prefix = tuple_prefix(scope);
        let mut tuples = Vec::new();
        for item in self.db.iterator_cf(
            self.cf(CF_AUTHZ_TUPLES)?,
            IteratorMode::From(&prefix, Direction::Forward),
        ) {
            let (key, value) = item.map_err(storage_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let tuple = decode_json::<StoredTuple>(&value)?.tuple;
            if tuple_key(scope, &tuple)?.as_slice() != key.as_ref() {
                return Err(AuthzStoreError::Storage(
                    "persisted authorization tuple key is inconsistent".into(),
                ));
            }
            tuples.push(tuple);
        }
        tuples.sort();
        Ok(tuples)
    }

    fn stage_tenant_revision(
        &self,
        batch: &mut WriteBatch,
        tenant: &StorageTenantId,
        revision: AuthzRevision,
    ) -> Result<(), AuthzStoreError> {
        batch.put_cf(
            self.cf(CF_AUTHZ_TENANTS)?,
            tenant_revision_key(tenant),
            encode_json(&revision)?,
        );
        Ok(())
    }

    /// Stages the first protected system realm without publishing any part of
    /// it. The bootstrap repository commits this batch together with the
    /// credential and completion marker.
    pub(crate) fn stage_initial_system_realm(
        &self,
        batch: &mut WriteBatch,
        schema_id: SchemaId,
        schema: Schema,
        bootstrap_application: ObjectRef,
    ) -> Result<(), AuthzStoreError> {
        schema_id.validate()?;
        validate_principal(&bootstrap_application)?;
        let tenant = StorageTenantId::system();
        let scope = AuthzScope::system();
        let canonical = canonical_schema(schema, self.limits.evaluator)?;
        let canonical_bytes = serde_json::to_vec(&canonical).map_err(storage_error)?;
        let schema_ref = SchemaRef {
            schema_id,
            schema_revision: 1,
            schema_digest: SchemaDigest(*blake3::hash(&canonical_bytes).as_bytes()),
        };
        let schema_key = schema_revision_key(&tenant, &schema_ref);
        let latest_key = schema_latest_key(&tenant, &schema_ref.schema_id);
        let digest_key =
            schema_digest_key(&tenant, &schema_ref.schema_id, schema_ref.schema_digest);
        let binding_key = binding_key(&scope);
        if self.tenant_revision(&tenant)? != AuthzRevision::ZERO
            || self
                .read_json::<StoredSchema>(CF_AUTHZ_SCHEMAS, &schema_key)?
                .is_some()
            || self
                .read_json::<u64>(CF_AUTHZ_SCHEMAS, &latest_key)?
                .is_some()
            || self
                .read_json::<SchemaRef>(CF_AUTHZ_SCHEMAS, &digest_key)?
                .is_some()
            || self
                .read_json::<RealmBinding>(CF_AUTHZ_BINDINGS, &binding_key)?
                .is_some()
        {
            return Err(AuthzStoreError::InvalidInput(
                "protected system authorization state is already initialized".into(),
            ));
        }

        let bootstrap_tuple = Tuple::new(
            ObjectRef::opaque("system", SYSTEM_STORAGE_TENANT_ID)?,
            "bootstrap_admin",
            bootstrap_application,
        );
        let tuple_key = tuple_key(&scope, &bootstrap_tuple)?;
        if self
            .read_json::<StoredTuple>(CF_AUTHZ_TUPLES, &tuple_key)?
            .is_some()
        {
            return Err(AuthzStoreError::InvalidInput(
                "protected system authorization state is already initialized".into(),
            ));
        }
        Authorization::new(
            scope.realm.clone(),
            canonical.clone(),
            [bootstrap_tuple.clone()],
            self.limits.evaluator,
        )?;

        let stored_schema = StoredSchema {
            schema_ref: schema_ref.clone(),
            schema: canonical,
            published_at_revision: AuthzRevision(1),
        };
        let binding = RealmBinding {
            scope,
            schema_ref: schema_ref.clone(),
            generation: 1,
            authz_revision: AuthzRevision(2),
            tuple_count: 1,
        };
        batch.put_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            schema_key,
            encode_json(&stored_schema)?,
        );
        batch.put_cf(self.cf(CF_AUTHZ_SCHEMAS)?, latest_key, encode_json(&1_u64)?);
        batch.put_cf(
            self.cf(CF_AUTHZ_SCHEMAS)?,
            digest_key,
            encode_json(&schema_ref)?,
        );
        batch.put_cf(
            self.cf(CF_AUTHZ_BINDINGS)?,
            binding_key,
            encode_json(&binding)?,
        );
        batch.put_cf(
            self.cf(CF_AUTHZ_TUPLES)?,
            tuple_key,
            encode_json(&StoredTuple {
                tuple: bootstrap_tuple,
            })?,
        );
        self.stage_tenant_revision(batch, &tenant, AuthzRevision(3))
    }

    fn read_json<T: DeserializeOwned>(
        &self,
        cf: &'static str,
        key: &[u8],
    ) -> Result<Option<T>, AuthzStoreError> {
        self.db
            .get_cf(self.cf(cf)?, key)
            .map_err(storage_error)?
            .map(|bytes| decode_json(&bytes))
            .transpose()
    }

    fn cf(&self, name: &'static str) -> Result<&rocksdb::ColumnFamily, AuthzStoreError> {
        self.db.cf_handle(name).ok_or_else(|| {
            AuthzStoreError::Storage(format!("missing authorization column family {name}"))
        })
    }

    pub(crate) fn lock_writes(&self) -> Result<std::sync::MutexGuard<'_, ()>, AuthzStoreError> {
        self.write_lock
            .lock()
            .map_err(|_| AuthzStoreError::Storage("authorization write lock poisoned".into()))
    }

    pub(crate) fn write(&self, batch: WriteBatch) -> Result<(), AuthzStoreError> {
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)
    }
}

fn protected_realm_owner_request(
    custom_scope: &AuthzScope,
    protected: ProtectedRealmOwnership,
) -> Result<TupleBatchRequest, AuthzStoreError> {
    let realm_resource = ObjectRef::opaque(
        "authz_realm",
        format!(
            "{}/{}",
            custom_scope.storage_tenant.as_str(),
            custom_scope.realm.as_str()
        ),
    )?;
    let parent_tenant = ObjectRef::opaque(
        "storage_tenant",
        custom_scope.storage_tenant.as_str().to_owned(),
    )?;
    let owner = protected.principal.clone();
    Ok(TupleBatchRequest {
        scope: AuthzScope::system(),
        principal: protected.principal,
        expected_revision: Some(protected.expected_revision),
        expected_binding_generation: protected.expected_binding_generation,
        operation_id: None,
        mutations: vec![
            TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: Tuple::new(realm_resource.clone(), "parent_tenant", parent_tenant),
            },
            TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: Tuple::new(realm_resource, "owner", owner),
            },
        ],
    })
}

fn canonical_schema(
    mut schema: Schema,
    limits: AuthorizationLimits,
) -> Result<Schema, AuthzStoreError> {
    schema
        .namespaces
        .sort_by(|left, right| left.name.cmp(&right.name));
    for NamespaceDefinition { relations, .. } in &mut schema.namespaces {
        relations.sort_by(|left, right| left.name.cmp(&right.name));
        for relation in relations {
            match &mut relation.kind {
                RelationKind::Direct { allowed_subjects } => allowed_subjects.sort(),
                RelationKind::Permission { rules } => rules.sort(),
            }
        }
    }
    schema.validate(limits)?;
    Ok(schema)
}

fn validate_component(value: &str, label: &str, maximum: usize) -> Result<(), AuthzStoreError> {
    if value.is_empty() {
        return Err(AuthzStoreError::InvalidInput(format!(
            "{label} must not be empty"
        )));
    }
    if value.len() > maximum {
        return Err(AuthzStoreError::InvalidInput(format!(
            "{label} exceeds {maximum} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(AuthzStoreError::InvalidInput(format!(
            "{label} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_safe_component(
    value: &str,
    label: &str,
    maximum: usize,
) -> Result<(), AuthzStoreError> {
    validate_component(value, label, maximum)?;
    if matches!(value, "." | "..")
        || value
            .chars()
            .any(|character| matches!(character, '/' | ':' | '#'))
    {
        return Err(AuthzStoreError::InvalidInput(format!(
            "{label} must be one canonical component"
        )));
    }
    Ok(())
}

fn validate_storage_tenant(value: &str) -> Result<(), AuthzStoreError> {
    if value == SYSTEM_STORAGE_TENANT_ID {
        return Ok(());
    }
    let bytes = value.as_bytes();
    let is_ascii_alphanumeric = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if bytes.is_empty()
        || bytes.len() > MAX_EXTERNAL_STORAGE_TENANT_BYTES
        || !bytes.first().is_some_and(is_ascii_alphanumeric)
        || !bytes.last().is_some_and(is_ascii_alphanumeric)
        || bytes
            .iter()
            .any(|byte| !is_ascii_alphanumeric(byte) && *byte != b'-')
    {
        return Err(AuthzStoreError::InvalidInput(format!(
            "storage tenant must be a lowercase ASCII DNS label of at most {MAX_EXTERNAL_STORAGE_TENANT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_principal(principal: &ObjectRef) -> Result<(), AuthzStoreError> {
    let validated = match &principal.id {
        ObjectId::Opaque(id) => ObjectRef::opaque(&principal.namespace, id)?,
        ObjectId::ExactPath(path) => ObjectRef::exact_path(
            &principal.namespace,
            ExactPath::new(&path.tenant, &path.bucket, &path.path)?,
        )?,
    };
    if validated != *principal {
        return Err(AuthzStoreError::InvalidInput(
            "principal is not canonical".into(),
        ));
    }
    Ok(())
}

fn validate_binding(
    binding: &RealmBinding,
    expected_scope: &AuthzScope,
) -> Result<(), AuthzStoreError> {
    binding.scope.validate()?;
    binding.schema_ref.validate()?;
    if binding.scope != *expected_scope || binding.generation == 0 || binding.authz_revision.0 == 0
    {
        return Err(AuthzStoreError::Storage(
            "persisted authorization binding is not canonical".into(),
        ));
    }
    Ok(())
}

fn validate_tuple_receipt(
    receipt: &TupleBatchReceipt,
    request: &TupleBatchRequest,
) -> Result<(), AuthzStoreError> {
    receipt.scope.validate()?;
    validate_principal(&receipt.principal)?;
    if receipt.scope.storage_tenant != request.scope.storage_tenant
        || receipt.principal != request.principal
        || receipt.authz_revision.0 == 0
        || receipt.binding_generation == 0
        || receipt.replay_guarantee_expires_at_unix_millis == 0
    {
        return Err(AuthzStoreError::Storage(
            "persisted authorization tuple receipt is inconsistent".into(),
        ));
    }
    Ok(())
}

fn validate_stored_tuple_receipt(
    stored: &StoredTupleReceipt,
    request: &TupleBatchRequest,
) -> Result<(), AuthzStoreError> {
    validate_stored_tuple_receipt_shape(stored)?;
    if request.operation_id.as_deref() != Some(stored.operation_id.as_str()) {
        return Err(AuthzStoreError::Storage(
            "persisted authorization tuple receipt operation id is inconsistent".into(),
        ));
    }
    validate_tuple_receipt(&stored.receipt, request)
}

fn validate_stored_tuple_receipt_shape(stored: &StoredTupleReceipt) -> Result<(), AuthzStoreError> {
    if stored.format != STORED_TUPLE_RECEIPT_FORMAT
        || validate_component(&stored.operation_id, "operation id", MAX_OPERATION_ID_BYTES).is_err()
        || stored.created_at_unix_millis == 0
        || stored.expires_at_unix_millis <= stored.created_at_unix_millis
        || stored.receipt.replay_guarantee_expires_at_unix_millis != stored.expires_at_unix_millis
        || stored.receipt.replayed
        || stored.receipt.authz_revision == AuthzRevision::ZERO
        || stored.receipt.binding_generation == 0
        || stored.receipt.mutation_count == 0
    {
        return Err(AuthzStoreError::Storage(
            "persisted authorization tuple receipt is inconsistent".into(),
        ));
    }
    stored.receipt.scope.validate()?;
    validate_principal(&stored.receipt.principal)
}

pub(crate) fn validate_stored_schema(
    stored: &StoredSchema,
    expected: &SchemaRef,
    limits: AuthorizationLimits,
) -> Result<(), AuthzStoreError> {
    stored.schema_ref.validate()?;
    if stored.schema_ref != *expected || stored.published_at_revision.0 == 0 {
        return Err(AuthzStoreError::Storage(
            "persisted authorization schema reference is inconsistent".into(),
        ));
    }
    let canonical = canonical_schema(stored.schema.clone(), limits)?;
    if canonical != stored.schema {
        return Err(AuthzStoreError::Storage(
            "persisted authorization schema is not canonical".into(),
        ));
    }
    let bytes = serde_json::to_vec(&canonical).map_err(storage_error)?;
    let digest = SchemaDigest(*blake3::hash(&bytes).as_bytes());
    if digest != stored.schema_ref.schema_digest {
        return Err(AuthzStoreError::Storage(
            "persisted authorization schema digest is inconsistent".into(),
        ));
    }
    Ok(())
}

fn require_revision(
    expected: Option<AuthzRevision>,
    current: AuthzRevision,
) -> Result<(), AuthzStoreError> {
    if let Some(expected) = expected
        && expected != current
    {
        return Err(AuthzStoreError::RevisionConflict { expected, current });
    }
    Ok(())
}

fn next_revision(current: AuthzRevision) -> Result<AuthzRevision, AuthzStoreError> {
    current
        .0
        .checked_add(1)
        .map(AuthzRevision)
        .ok_or_else(|| AuthzStoreError::Storage("authorization revision overflow".into()))
}

fn current_unix_millis() -> Result<u64, AuthzStoreError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AuthzStoreError::Storage("system clock predates the Unix epoch".into()))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| AuthzStoreError::Storage("Unix time exceeds u64 milliseconds".into()))
}

fn receipt_record_bytes(key: &[u8], value: &[u8]) -> Result<u64, AuthzStoreError> {
    let bytes = key
        .len()
        .checked_add(value.len())
        .ok_or(AuthzStoreError::ReceiptCapacity)?;
    u64::try_from(bytes).map_err(|_| AuthzStoreError::ReceiptCapacity)
}

fn tuple_fingerprint(
    request: &TupleBatchRequest,
    canonical_mutations: &[TupleMutation],
) -> Result<[u8; 32], AuthzStoreError> {
    let bytes = serde_json::to_vec(&(
        &request.scope,
        &request.principal,
        request.expected_revision,
        canonical_mutations,
    ))
    .map_err(storage_error)?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, AuthzStoreError> {
    serde_json::to_vec(value).map_err(storage_error)
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, AuthzStoreError> {
    serde_json::from_slice(bytes).map_err(storage_error)
}

fn storage_error(error: impl fmt::Display) -> AuthzStoreError {
    AuthzStoreError::Storage(error.to_string())
}

fn push_component(key: &mut Vec<u8>, component: &[u8]) {
    let length = u32::try_from(component.len()).expect("bounded authorization key component");
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(component);
}

fn tenant_revision_key(tenant: &StorageTenantId) -> Vec<u8> {
    let mut key = vec![b'H'];
    push_component(&mut key, tenant.as_str().as_bytes());
    key
}

fn schema_latest_key(tenant: &StorageTenantId, schema_id: &SchemaId) -> Vec<u8> {
    let mut key = vec![b'L'];
    push_component(&mut key, tenant.as_str().as_bytes());
    push_component(&mut key, schema_id.as_str().as_bytes());
    key
}

pub(crate) fn schema_revision_key(tenant: &StorageTenantId, schema_ref: &SchemaRef) -> Vec<u8> {
    let mut key = vec![b'S'];
    push_component(&mut key, tenant.as_str().as_bytes());
    push_component(&mut key, schema_ref.schema_id.as_str().as_bytes());
    key.extend_from_slice(&schema_ref.schema_revision.to_be_bytes());
    key
}

fn schema_digest_key(
    tenant: &StorageTenantId,
    schema_id: &SchemaId,
    digest: SchemaDigest,
) -> Vec<u8> {
    let mut key = vec![b'D'];
    push_component(&mut key, tenant.as_str().as_bytes());
    push_component(&mut key, schema_id.as_str().as_bytes());
    key.extend_from_slice(&digest.0);
    key
}

fn binding_key(scope: &AuthzScope) -> Vec<u8> {
    let mut key = vec![b'B'];
    push_component(&mut key, scope.storage_tenant.as_str().as_bytes());
    push_component(&mut key, scope.realm.as_str().as_bytes());
    key
}

fn tuple_prefix(scope: &AuthzScope) -> Vec<u8> {
    let mut key = vec![b'T'];
    push_component(&mut key, scope.storage_tenant.as_str().as_bytes());
    push_component(&mut key, scope.realm.as_str().as_bytes());
    key
}

fn tuple_key(scope: &AuthzScope, tuple: &Tuple) -> Result<Vec<u8>, AuthzStoreError> {
    let mut key = tuple_prefix(scope);
    let encoded = serde_json::to_vec(tuple).map_err(storage_error)?;
    key.extend_from_slice(blake3::hash(&encoded).as_bytes());
    Ok(key)
}

fn receipt_key(
    tenant: &StorageTenantId,
    principal: &ObjectRef,
    operation_id: &str,
) -> Result<Vec<u8>, AuthzStoreError> {
    let mut key = vec![b'R'];
    push_component(&mut key, tenant.as_str().as_bytes());
    let principal = serde_json::to_vec(principal).map_err(storage_error)?;
    push_component(&mut key, blake3::hash(&principal).as_bytes());
    push_component(&mut key, operation_id.as_bytes());
    Ok(key)
}

#[cfg(test)]
mod tests;

mod catalogue;
pub use catalogue::*;

mod replication;
pub use replication::*;

mod snapshot;
pub use snapshot::*;
