use std::fmt;

use keldra_authz::{AuthorizationLimits, ObjectRef, Schema};
use rocksdb::{WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::authz::{StoredSchema, schema_revision_key, validate_stored_schema};
use crate::bootstrap::{
    APPLICATION_FORMAT_VERSION, CREDENTIAL_FORMAT_VERSION, MAX_CLIENT_SECRET_BYTES,
    PROVISIONING_FORMAT_VERSION, StoredApplication, StoredApplicationCredential, StoredBucket,
    StoredCredentialVerifier, StoredTenant, application_key, application_ref, bucket_record_key,
    credential_from_stored, credential_key, credential_matches, tenant_record_key,
    validate_client_id, validate_stored_application, validate_stored_bucket,
    validate_stored_credential_verifier, validate_stored_tenant,
};
use crate::key::{
    BucketId, BucketIdentity, STORAGE_KEY_FORMAT_VERSION, TENANT_NAME_TYPE, TenantId,
    bucket_name_key, decode_identity_value, tenant_name_key,
};
use crate::store::{
    CF_AUTHZ_SCHEMAS, CF_BUCKET_OPTIONS, CF_CREDENTIALS, CF_METADATA, CF_NAMES, CF_POLICIES,
    LocalReferenceEffects, PendingLocalChange, VERSION_HIGH_WATERMARK_KEY,
    decode_object_versioning, encode_object_versioning,
};
use crate::{
    AggregateKind, AuthzRevision, AuthzStoreLimits, BucketPolicy, MutationError, ObjectKey,
    ObjectVersioning, PlacementLogId, SchemaRef, StorageTenantId, Store, VersionId,
};

pub const LOGICAL_RECORD_FORMAT: u16 = 1;
const BASELINE_HASH_DOMAIN: &str = "keldra.logical-record-baseline.v1";
const MUTATION_HASH_DOMAIN: &str = "keldra.logical-record-mutation.v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BaselineHash(pub [u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum LogicalRecordPredecessor {
    Absent,
    BaselineHash(BaselineHash),
    VersionId(VersionId),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalRecordId {
    TenantNameClaim {
        storage_tenant: StorageTenantId,
    },
    BucketNameClaim {
        tenant_id: u64,
        bucket: String,
    },
    TenantRecord {
        tenant_id: u64,
    },
    BucketRecord {
        tenant_id: u64,
        bucket_id: u64,
    },
    BucketOptions {
        tenant_id: u64,
        bucket_id: u64,
    },
    BucketPolicy {
        tenant_id: u64,
        bucket_id: u64,
    },
    Application {
        app_id: String,
    },
    Credential {
        client_id: String,
    },
    TenantSchema {
        storage_tenant: StorageTenantId,
        schema_ref: SchemaRef,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalTenantRecord {
    pub tenant_id: u64,
    pub storage_tenant: StorageTenantId,
    pub owner_app_id: String,
    pub owner_client_id: String,
    pub authorization_revision: AuthzRevision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalBucketRecord {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub storage_tenant: StorageTenantId,
    pub bucket: String,
    pub owner: ObjectRef,
    pub authorization_revision: AuthzRevision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalApplicationRecord {
    pub app_id: String,
    pub client_id: String,
    pub storage_tenant: StorageTenantId,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalCredentialRecord {
    app_id: String,
    client_id: String,
    storage_tenant: StorageTenantId,
    active: bool,
    verifier: StoredCredentialVerifier,
    sigv4_secret: Option<crate::CredentialSecretEnvelope>,
}

impl LogicalCredentialRecord {
    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn storage_tenant(&self) -> &StorageTenantId {
        &self.storage_tenant
    }

    pub fn active(&self) -> bool {
        self.active
    }

    pub fn sigv4_secret(&self) -> Option<&crate::CredentialSecretEnvelope> {
        self.sigv4_secret.as_ref()
    }

    pub fn install_sigv4_secret(&mut self, envelope: crate::CredentialSecretEnvelope) {
        self.sigv4_secret = Some(envelope);
    }

    /// Verify a secret against one quorum-selected credential without relying
    /// on an unrelated local application replica.
    pub fn verify_secret(
        &self,
        secret: &str,
    ) -> Result<Option<crate::ApplicationCredential>, crate::CredentialRepositoryError> {
        self.validate()?;
        if secret.len() > MAX_CLIENT_SECRET_BYTES
            || !self.active
            || !credential_matches(&self.verifier, secret.as_bytes())?
        {
            return Ok(None);
        }
        credential_from_stored(&self.clone().into()).map(Some)
    }

    fn validate(&self) -> Result<(), crate::CredentialRepositoryError> {
        validate_stored_credential_verifier(&self.verifier)?;
        if let Some(envelope) = self.sigv4_secret.as_ref() {
            envelope
                .validate()
                .map_err(|message| crate::CredentialRepositoryError::Storage(message.into()))?;
        }
        credential_from_stored(&self.clone().into()).map(|_| ())
    }
}

impl fmt::Debug for LogicalCredentialRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalCredentialRecord")
            .field("app_id", &self.app_id)
            .field("client_id", &self.client_id)
            .field("storage_tenant", &self.storage_tenant)
            .field("active", &self.active)
            .field("verifier", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalTenantSchema {
    pub storage_tenant: StorageTenantId,
    pub schema_ref: SchemaRef,
    pub schema: Schema,
    pub published_at_revision: AuthzRevision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogicalRecordValue {
    TenantNameClaim {
        storage_tenant: StorageTenantId,
        tenant_id: u64,
    },
    BucketNameClaim {
        tenant_id: u64,
        bucket: String,
        bucket_id: u64,
    },
    TenantRecord(LogicalTenantRecord),
    BucketRecord(LogicalBucketRecord),
    BucketOptions {
        tenant_id: u64,
        bucket_id: u64,
        versioning: ObjectVersioning,
    },
    BucketPolicy {
        tenant_id: u64,
        bucket_id: u64,
        policy: BucketPolicy,
    },
    Application(LogicalApplicationRecord),
    Credential(LogicalCredentialRecord),
    TenantSchema(LogicalTenantSchema),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogicalRecordMutationContext {
    pub record_version: VersionId,
    pub active_placement_log_id: PlacementLogId,
    pub serving_fence_term: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalRecordMutation {
    pub format: u16,
    pub record_version: VersionId,
    pub predecessor: LogicalRecordPredecessor,
    pub mutation_fingerprint: [u8; 32],
    pub active_placement_log_id: PlacementLogId,
    pub serving_fence_term: u64,
    pub typed_value: LogicalRecordValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalRecordCandidate {
    Baseline {
        typed_value: LogicalRecordValue,
        baseline_hash: BaselineHash,
    },
    Versioned(LogicalRecordMutation),
}

impl LogicalRecordCandidate {
    pub fn typed_value(&self) -> &LogicalRecordValue {
        match self {
            Self::Baseline { typed_value, .. } => typed_value,
            Self::Versioned(mutation) => &mutation.typed_value,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogicalRecordApplied {
    pub record_version: VersionId,
    pub replayed: bool,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum LogicalRecordError {
    #[error("invalid logical record: {0}")]
    Invalid(String),
    #[error("logical record is immutable after its first committed value")]
    Immutable,
    #[error("logical record mutation has a lineage gap")]
    LineageGap,
    #[error("logical record mutations are contradictory siblings")]
    Sibling,
    #[error("logical record mutation is stale")]
    Stale,
    #[error("logical record mutation or stored envelope failed integrity validation")]
    Tampered,
    #[error("logical record export cursor is invalid")]
    InvalidCursor,
    #[error("logical record export limits are invalid: {0}")]
    InvalidExportLimit(String),
    #[error("one logical record requires {required_bytes} bytes, exceeding the page limit")]
    ExportRecordTooLarge { required_bytes: u64 },
    #[error("logical record snapshot conflicts with an existing local value")]
    SnapshotConflict,
    #[error("source journal capacity is exhausted")]
    SourceJournalCapacity,
    #[error("logical record storage failed: {0}")]
    Storage(String),
}

#[derive(Serialize)]
struct MutationFingerprintInput<'a> {
    format: u16,
    record_version: VersionId,
    predecessor: LogicalRecordPredecessor,
    active_placement_log_id: PlacementLogId,
    serving_fence_term: u64,
    typed_value: &'a LogicalRecordValue,
}

impl LogicalRecordMutation {
    pub fn computed_fingerprint(&self) -> Result<[u8; 32], LogicalRecordError> {
        let input = MutationFingerprintInput {
            format: self.format,
            record_version: self.record_version,
            predecessor: self.predecessor,
            active_placement_log_id: self.active_placement_log_id,
            serving_fence_term: self.serving_fence_term,
            typed_value: &self.typed_value,
        };
        let bytes = canonical_bytes(&input)?;
        Ok(blake3::derive_key(MUTATION_HASH_DOMAIN, &bytes))
    }

    pub fn validate(&self) -> Result<(), LogicalRecordError> {
        if self.format != LOGICAL_RECORD_FORMAT {
            return Err(LogicalRecordError::Invalid(format!(
                "unsupported envelope format {}",
                self.format
            )));
        }
        if self.record_version.0 == 0
            || self.active_placement_log_id.term == 0
            || self.active_placement_log_id.index == 0
            || self.serving_fence_term == 0
        {
            return Err(LogicalRecordError::Invalid(
                "record version, active placement log ID, and serving-fence term must be non-zero"
                    .into(),
            ));
        }
        if let LogicalRecordPredecessor::VersionId(predecessor) = self.predecessor
            && (predecessor.0 == 0 || predecessor >= self.record_version)
        {
            return Err(LogicalRecordError::Invalid(
                "record version must follow its predecessor".into(),
            ));
        }
        self.typed_value.validate()?;
        if self.mutation_fingerprint != self.computed_fingerprint()? {
            return Err(LogicalRecordError::Tampered);
        }
        Ok(())
    }
}

impl LogicalRecordValue {
    pub fn id(&self) -> LogicalRecordId {
        match self {
            Self::TenantNameClaim { storage_tenant, .. } => LogicalRecordId::TenantNameClaim {
                storage_tenant: storage_tenant.clone(),
            },
            Self::BucketNameClaim {
                tenant_id, bucket, ..
            } => LogicalRecordId::BucketNameClaim {
                tenant_id: *tenant_id,
                bucket: bucket.clone(),
            },
            Self::TenantRecord(record) => LogicalRecordId::TenantRecord {
                tenant_id: record.tenant_id,
            },
            Self::BucketRecord(record) => LogicalRecordId::BucketRecord {
                tenant_id: record.tenant_id,
                bucket_id: record.bucket_id,
            },
            Self::BucketOptions {
                tenant_id,
                bucket_id,
                ..
            } => LogicalRecordId::BucketOptions {
                tenant_id: *tenant_id,
                bucket_id: *bucket_id,
            },
            Self::BucketPolicy {
                tenant_id,
                bucket_id,
                ..
            } => LogicalRecordId::BucketPolicy {
                tenant_id: *tenant_id,
                bucket_id: *bucket_id,
            },
            Self::Application(record) => LogicalRecordId::Application {
                app_id: record.app_id.clone(),
            },
            Self::Credential(record) => LogicalRecordId::Credential {
                client_id: record.client_id.clone(),
            },
            Self::TenantSchema(record) => LogicalRecordId::TenantSchema {
                storage_tenant: record.storage_tenant.clone(),
                schema_ref: record.schema_ref.clone(),
            },
        }
    }

    pub fn validate(&self) -> Result<(), LogicalRecordError> {
        self.id().validate()?;
        match self {
            Self::TenantNameClaim { tenant_id, .. } if *tenant_id == 0 => {
                Err(invalid("tenant ID must be non-zero"))
            }
            Self::BucketNameClaim { bucket_id, .. } if *bucket_id == 0 => {
                Err(invalid("bucket ID must be non-zero"))
            }
            Self::TenantRecord(record) => validate_tenant_record(record),
            Self::BucketRecord(record) => validate_bucket_record(record),
            Self::BucketPolicy { policy, .. } => policy
                .validate()
                .map_err(|error| invalid(error.to_string())),
            Self::Application(record) => validate_application_record(record),
            Self::Credential(record) => validate_credential_record(record),
            Self::TenantSchema(record) => validate_schema_record(record),
            _ => Ok(()),
        }
    }

    fn is_write_once(&self) -> bool {
        matches!(
            self,
            Self::TenantNameClaim { .. } | Self::BucketNameClaim { .. } | Self::TenantSchema(_)
        )
    }
}

impl LogicalRecordId {
    fn validate(&self) -> Result<(), LogicalRecordError> {
        match self {
            Self::TenantNameClaim { .. } => Ok(()),
            Self::BucketNameClaim { tenant_id, bucket } => {
                require_nonzero(*tenant_id, "tenant ID")?;
                validate_bucket_name(bucket)
            }
            Self::TenantRecord { tenant_id } => require_nonzero(*tenant_id, "tenant ID"),
            Self::BucketRecord {
                tenant_id,
                bucket_id,
            }
            | Self::BucketOptions {
                tenant_id,
                bucket_id,
            }
            | Self::BucketPolicy {
                tenant_id,
                bucket_id,
            } => {
                require_nonzero(*tenant_id, "tenant ID")?;
                require_nonzero(*bucket_id, "bucket ID")
            }
            Self::Application { app_id } => application_ref(app_id)
                .map(|_| ())
                .map_err(|error| invalid(error.to_string())),
            Self::Credential { client_id } => {
                validate_client_id(client_id).map_err(|error| invalid(error.to_string()))
            }
            Self::TenantSchema { schema_ref, .. } => {
                if schema_ref.schema_revision == 0 {
                    return Err(invalid("schema revision must be non-zero"));
                }
                Ok(())
            }
        }
    }

    fn location(&self) -> Result<RecordLocation, LogicalRecordError> {
        self.validate()?;
        Ok(match self {
            Self::TenantNameClaim { storage_tenant } => RecordLocation {
                cf: CF_NAMES,
                key: tenant_name_key(storage_tenant.as_str()),
            },
            Self::BucketNameClaim { tenant_id, bucket } => RecordLocation {
                cf: CF_NAMES,
                key: bucket_name_key(TenantId(*tenant_id), bucket),
            },
            Self::TenantRecord { tenant_id } => RecordLocation {
                cf: CF_METADATA,
                key: tenant_record_key(TenantId(*tenant_id)),
            },
            Self::BucketRecord {
                tenant_id,
                bucket_id,
            } => RecordLocation {
                cf: CF_METADATA,
                key: bucket_record_key(identity(*tenant_id, *bucket_id)),
            },
            Self::BucketOptions {
                tenant_id,
                bucket_id,
            } => RecordLocation {
                cf: CF_BUCKET_OPTIONS,
                key: identity(*tenant_id, *bucket_id).encode().to_vec(),
            },
            Self::BucketPolicy {
                tenant_id,
                bucket_id,
            } => RecordLocation {
                cf: CF_POLICIES,
                key: identity(*tenant_id, *bucket_id).encode().to_vec(),
            },
            Self::Application { app_id } => RecordLocation {
                cf: CF_CREDENTIALS,
                key: application_key(app_id),
            },
            Self::Credential { client_id } => RecordLocation {
                cf: CF_CREDENTIALS,
                key: credential_key(client_id),
            },
            Self::TenantSchema {
                storage_tenant,
                schema_ref,
            } => RecordLocation {
                cf: CF_AUTHZ_SCHEMAS,
                key: schema_revision_key(storage_tenant, schema_ref),
            },
        })
    }
}

struct RecordLocation {
    cf: &'static str,
    key: Vec<u8>,
}

impl Store {
    /// Allocate the next node-scoped Snowflake version after a logical-record
    /// coordinator has reconciled its current replica quorum.
    pub fn allocate_logical_record_version(&self) -> Result<VersionId, LogicalRecordError> {
        self.clock.next().map_err(storage)
    }

    pub fn logical_record_candidate(
        &self,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordCandidate>, LogicalRecordError> {
        let location = id.location()?;
        let Some(bytes) = self
            .db
            .get_cf(self.logical_record_cf(location.cf)?, &location.key)
            .map_err(storage)?
        else {
            return Ok(None);
        };
        decode_candidate(id, &bytes).map(Some)
    }

    /// Constructs the complete mutation peers need without changing storage.
    pub fn construct_logical_record_mutation(
        &self,
        typed_value: LogicalRecordValue,
        context: LogicalRecordMutationContext,
    ) -> Result<LogicalRecordMutation, LogicalRecordError> {
        typed_value.validate()?;
        let id = typed_value.id();
        let predecessor = match self.logical_record_candidate(&id)? {
            None => LogicalRecordPredecessor::Absent,
            Some(LogicalRecordCandidate::Baseline {
                typed_value: existing,
                baseline_hash,
            }) => {
                if typed_value.is_write_once() && existing != typed_value {
                    return Err(LogicalRecordError::Immutable);
                }
                LogicalRecordPredecessor::BaselineHash(baseline_hash)
            }
            Some(LogicalRecordCandidate::Versioned(current)) => {
                if typed_value.is_write_once() {
                    return Err(LogicalRecordError::Immutable);
                }
                if context.record_version <= current.record_version {
                    return Err(LogicalRecordError::Stale);
                }
                LogicalRecordPredecessor::VersionId(current.record_version)
            }
        };
        let mut mutation = LogicalRecordMutation {
            format: LOGICAL_RECORD_FORMAT,
            record_version: context.record_version,
            predecessor,
            mutation_fingerprint: [0; 32],
            active_placement_log_id: context.active_placement_log_id,
            serving_fence_term: context.serving_fence_term,
            typed_value,
        };
        mutation.mutation_fingerprint = mutation.computed_fingerprint()?;
        mutation.validate()?;
        Ok(mutation)
    }

    pub fn apply_logical_record_mutation_replica(
        &self,
        mutation: &LogicalRecordMutation,
    ) -> Result<LogicalRecordApplied, LogicalRecordError> {
        self.apply_logical_record_mutation(mutation, false)
    }

    /// Applies a normal distributed logical-record mutation and appends its
    /// compact source invalidation in the same synchronous RocksDB batch.
    pub async fn apply_logical_record_mutation_journaled(
        &self,
        mutation: &LogicalRecordMutation,
    ) -> Result<LogicalRecordApplied, LogicalRecordError> {
        let _commit_guard = self.commit_lock.lock().await;
        let applied = self.apply_logical_record_mutation(mutation, true)?;
        if !applied.replayed {
            self.notify_local_invalidations();
        }
        Ok(applied)
    }

    /// Local coordinator commits use exactly the replica apply path.
    pub fn commit_logical_record_mutation(
        &self,
        mutation: &LogicalRecordMutation,
    ) -> Result<LogicalRecordApplied, LogicalRecordError> {
        self.apply_logical_record_mutation(mutation, false)
    }

    fn apply_logical_record_mutation(
        &self,
        mutation: &LogicalRecordMutation,
        journal: bool,
    ) -> Result<LogicalRecordApplied, LogicalRecordError> {
        mutation.validate()?;
        let id = mutation.typed_value.id();
        let location = id.location()?;
        let _guard = self
            .authz_write_lock
            .lock()
            .map_err(|_| storage("logical-record write lock is poisoned"))?;
        let current = self.logical_record_candidate(&id)?;
        if let Some(LogicalRecordCandidate::Versioned(existing)) = current.as_ref()
            && existing == mutation
        {
            return Ok(LogicalRecordApplied {
                record_version: mutation.record_version,
                replayed: true,
            });
        }
        if mutation.typed_value.is_write_once() {
            match current.as_ref() {
                Some(LogicalRecordCandidate::Baseline { typed_value, .. })
                    if typed_value == &mutation.typed_value => {}
                Some(_) => return Err(LogicalRecordError::Immutable),
                None => {}
            }
        }
        match current.as_ref() {
            None if mutation.predecessor != LogicalRecordPredecessor::Absent => {
                return Err(LogicalRecordError::LineageGap);
            }
            Some(LogicalRecordCandidate::Baseline { baseline_hash, .. })
                if mutation.predecessor
                    != LogicalRecordPredecessor::BaselineHash(*baseline_hash) =>
            {
                return Err(LogicalRecordError::LineageGap);
            }
            Some(LogicalRecordCandidate::Versioned(existing))
                if mutation.predecessor == existing.predecessor =>
            {
                return Err(LogicalRecordError::Sibling);
            }
            Some(LogicalRecordCandidate::Versioned(existing))
                if mutation.predecessor
                    != LogicalRecordPredecessor::VersionId(existing.record_version) =>
            {
                return Err(LogicalRecordError::LineageGap);
            }
            _ => {}
        }

        let change = if journal {
            Some(PendingLocalChange::AggregateChanged {
                aggregate_kind: AggregateKind::LogicalRecord,
                aggregate_key: canonical_bytes(&id)?,
                revision: mutation.record_version.0,
            })
        } else {
            None
        };
        self.write_logical_record_with_change(
            &location,
            canonical_bytes(mutation)?,
            Some(mutation.record_version),
            change.as_ref(),
        )?;
        Ok(LogicalRecordApplied {
            record_version: mutation.record_version,
            replayed: false,
        })
    }

    fn logical_record_cf(
        &self,
        name: &'static str,
    ) -> Result<&rocksdb::ColumnFamily, LogicalRecordError> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| storage(format!("missing column family {name}")))
    }

    fn write_logical_record(
        &self,
        location: &RecordLocation,
        encoded: Vec<u8>,
        record_version: Option<VersionId>,
    ) -> Result<(), LogicalRecordError> {
        self.write_logical_record_with_change(location, encoded, record_version, None)
    }

    fn write_logical_record_with_change(
        &self,
        location: &RecordLocation,
        encoded: Vec<u8>,
        record_version: Option<VersionId>,
        change: Option<&PendingLocalChange>,
    ) -> Result<(), LogicalRecordError> {
        let mut batch = WriteBatch::default();
        batch.put_cf(self.logical_record_cf(location.cf)?, &location.key, encoded);
        if let Some(record_version) = record_version {
            let high_watermark = self
                .db
                .get_cf(
                    self.logical_record_cf(CF_METADATA)?,
                    VERSION_HIGH_WATERMARK_KEY,
                )
                .map_err(storage)?
                .map(|bytes| serde_json::from_slice::<VersionId>(&bytes).map_err(storage))
                .transpose()?
                .map_or(record_version, |current| current.max(record_version));
            batch.put_cf(
                self.logical_record_cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                canonical_bytes(&high_watermark)?,
            );
        }
        if let Some(change) = change {
            self.stage_local_changes(
                &mut batch,
                std::slice::from_ref(change),
                LocalReferenceEffects::NoReferenceEffects,
            )
            .map_err(logical_record_mutation_error)?;
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage)?;
        if let Some(record_version) = record_version {
            self.clock.observe(record_version);
        }
        Ok(())
    }
}

fn logical_record_mutation_error(error: MutationError) -> LogicalRecordError {
    match error {
        MutationError::SourceJournalCapacity => LogicalRecordError::SourceJournalCapacity,
        error => storage(error),
    }
}

fn decode_candidate(
    id: &LogicalRecordId,
    bytes: &[u8],
) -> Result<LogicalRecordCandidate, LogicalRecordError> {
    if looks_like_envelope(bytes) {
        let mutation: LogicalRecordMutation =
            serde_json::from_slice(bytes).map_err(|_| LogicalRecordError::Tampered)?;
        mutation.validate()?;
        if mutation.typed_value.id() != *id {
            return Err(LogicalRecordError::Tampered);
        }
        return Ok(LogicalRecordCandidate::Versioned(mutation));
    }
    let typed_value = decode_baseline(id, bytes)?;
    let baseline_hash = computed_baseline_hash(&typed_value)?;
    Ok(LogicalRecordCandidate::Baseline {
        typed_value,
        baseline_hash,
    })
}

pub(crate) fn decode_current_value(
    id: &LogicalRecordId,
    bytes: &[u8],
) -> Result<LogicalRecordValue, LogicalRecordError> {
    match decode_candidate(id, bytes)? {
        LogicalRecordCandidate::Baseline { typed_value, .. } => Ok(typed_value),
        LogicalRecordCandidate::Versioned(mutation) => Ok(mutation.typed_value),
    }
}

fn looks_like_envelope(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| value.as_object().cloned())
        .is_some_and(|object| {
            object.contains_key("record_version")
                || object.contains_key("predecessor")
                || object.contains_key("mutation_fingerprint")
                || object.contains_key("typed_value")
        })
}

fn decode_baseline(
    id: &LogicalRecordId,
    bytes: &[u8],
) -> Result<LogicalRecordValue, LogicalRecordError> {
    let value = match id {
        LogicalRecordId::TenantNameClaim { storage_tenant } => {
            LogicalRecordValue::TenantNameClaim {
                storage_tenant: storage_tenant.clone(),
                tenant_id: decode_id(bytes)?,
            }
        }
        LogicalRecordId::BucketNameClaim { tenant_id, bucket } => {
            LogicalRecordValue::BucketNameClaim {
                tenant_id: *tenant_id,
                bucket: bucket.clone(),
                bucket_id: decode_id(bytes)?,
            }
        }
        LogicalRecordId::TenantRecord { tenant_id } => {
            let stored: StoredTenant = decode_json(bytes)?;
            validate_stored_tenant(&stored, TenantId(*tenant_id), &stored.storage_tenant)
                .map_err(|error| storage(error.to_string()))?;
            LogicalRecordValue::TenantRecord(stored.into())
        }
        LogicalRecordId::BucketRecord {
            tenant_id,
            bucket_id,
        } => {
            let stored: StoredBucket = decode_json(bytes)?;
            validate_stored_bucket(
                &stored,
                identity(*tenant_id, *bucket_id),
                &stored.storage_tenant,
                &stored.bucket,
            )
            .map_err(|error| storage(error.to_string()))?;
            LogicalRecordValue::BucketRecord(stored.into())
        }
        LogicalRecordId::BucketOptions {
            tenant_id,
            bucket_id,
        } => LogicalRecordValue::BucketOptions {
            tenant_id: *tenant_id,
            bucket_id: *bucket_id,
            versioning: decode_object_versioning(bytes)
                .map_err(|error| storage(error.to_string()))?,
        },
        LogicalRecordId::BucketPolicy {
            tenant_id,
            bucket_id,
        } => LogicalRecordValue::BucketPolicy {
            tenant_id: *tenant_id,
            bucket_id: *bucket_id,
            policy: decode_json(bytes)?,
        },
        LogicalRecordId::Application { app_id } => {
            let stored: StoredApplication = decode_json(bytes)?;
            validate_stored_application(&stored, &stored.storage_tenant, app_id)
                .map_err(|error| storage(error.to_string()))?;
            LogicalRecordValue::Application(stored.into())
        }
        LogicalRecordId::Credential { client_id } => {
            let stored: StoredApplicationCredential = decode_json(bytes)?;
            credential_from_stored(&stored).map_err(|error| storage(error.to_string()))?;
            if stored.client_id != *client_id {
                return Err(storage(
                    "persisted credential identity does not match its key",
                ));
            }
            LogicalRecordValue::Credential(stored.into())
        }
        LogicalRecordId::TenantSchema {
            storage_tenant,
            schema_ref,
        } => {
            let stored: StoredSchema = decode_json(bytes)?;
            validate_stored_schema(&stored, schema_ref, AuthorizationLimits::default())
                .map_err(|error| storage(error.to_string()))?;
            LogicalRecordValue::TenantSchema(LogicalTenantSchema {
                storage_tenant: storage_tenant.clone(),
                schema_ref: stored.schema_ref,
                schema: stored.schema,
                published_at_revision: stored.published_at_revision,
            })
        }
    };
    value.validate()?;
    if value.id() != *id {
        return Err(storage(
            "persisted logical record identity does not match its key",
        ));
    }
    Ok(value)
}

fn validate_tenant_record(record: &LogicalTenantRecord) -> Result<(), LogicalRecordError> {
    let stored: StoredTenant = record.clone().into();
    validate_stored_tenant(&stored, TenantId(record.tenant_id), &record.storage_tenant)
        .map_err(|error| invalid(error.to_string()))
}

fn validate_bucket_record(record: &LogicalBucketRecord) -> Result<(), LogicalRecordError> {
    let stored: StoredBucket = record.clone().into();
    validate_stored_bucket(
        &stored,
        identity(record.tenant_id, record.bucket_id),
        &record.storage_tenant,
        &record.bucket,
    )
    .map_err(|error| invalid(error.to_string()))
}

fn validate_application_record(
    record: &LogicalApplicationRecord,
) -> Result<(), LogicalRecordError> {
    let stored: StoredApplication = record.clone().into();
    validate_stored_application(&stored, &record.storage_tenant, &record.app_id)
        .map_err(|error| invalid(error.to_string()))
}

fn validate_credential_record(record: &LogicalCredentialRecord) -> Result<(), LogicalRecordError> {
    validate_stored_credential_verifier(&record.verifier)
        .map_err(|error| invalid(error.to_string()))?;
    credential_from_stored(&record.clone().into())
        .map(|_| ())
        .map_err(|error| invalid(error.to_string()))
}

fn validate_schema_record(record: &LogicalTenantSchema) -> Result<(), LogicalRecordError> {
    let stored = StoredSchema {
        schema_ref: record.schema_ref.clone(),
        schema: record.schema.clone(),
        published_at_revision: record.published_at_revision,
    };
    validate_stored_schema(
        &stored,
        &record.schema_ref,
        AuthzStoreLimits::default().evaluator,
    )
    .map_err(|error| invalid(error.to_string()))
}

fn validate_bucket_name(bucket: &str) -> Result<(), LogicalRecordError> {
    ObjectKey::new("t", bucket, "_keldra/identity-check")
        .map(|_| ())
        .map_err(|error| invalid(error.to_string()))
}

fn require_nonzero(value: u64, name: &str) -> Result<(), LogicalRecordError> {
    if value == 0 {
        Err(invalid(format!("{name} must be non-zero")))
    } else {
        Ok(())
    }
}

fn identity(tenant_id: u64, bucket_id: u64) -> BucketIdentity {
    BucketIdentity {
        tenant_id: TenantId(tenant_id),
        bucket_id: BucketId(bucket_id),
    }
}

fn decode_id(bytes: &[u8]) -> Result<u64, LogicalRecordError> {
    let id = decode_identity_value(bytes).map_err(|error| storage(error.to_string()))?;
    require_nonzero(id, "stable identity")?;
    Ok(id)
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, LogicalRecordError> {
    serde_json::to_vec(value).map_err(storage)
}

fn computed_baseline_hash(
    typed_value: &LogicalRecordValue,
) -> Result<BaselineHash, LogicalRecordError> {
    Ok(BaselineHash(blake3::derive_key(
        BASELINE_HASH_DOMAIN,
        &canonical_bytes(typed_value)?,
    )))
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, LogicalRecordError> {
    serde_json::from_slice(bytes).map_err(storage)
}

fn invalid(message: impl Into<String>) -> LogicalRecordError {
    LogicalRecordError::Invalid(message.into())
}

fn storage(error: impl fmt::Display) -> LogicalRecordError {
    LogicalRecordError::Storage(error.to_string())
}

impl From<StoredTenant> for LogicalTenantRecord {
    fn from(stored: StoredTenant) -> Self {
        Self {
            tenant_id: stored.tenant_id.0,
            storage_tenant: stored.storage_tenant,
            owner_app_id: stored.owner_app_id,
            owner_client_id: stored.owner_client_id,
            authorization_revision: stored.authorization_revision,
        }
    }
}

impl From<LogicalTenantRecord> for StoredTenant {
    fn from(record: LogicalTenantRecord) -> Self {
        Self {
            format_version: PROVISIONING_FORMAT_VERSION,
            tenant_id: TenantId(record.tenant_id),
            storage_tenant: record.storage_tenant,
            owner_app_id: record.owner_app_id,
            owner_client_id: record.owner_client_id,
            authorization_revision: record.authorization_revision,
        }
    }
}

impl From<StoredBucket> for LogicalBucketRecord {
    fn from(stored: StoredBucket) -> Self {
        Self {
            tenant_id: stored.tenant_id.0,
            bucket_id: stored.bucket_id.0,
            storage_tenant: stored.storage_tenant,
            bucket: stored.bucket,
            owner: stored.owner,
            authorization_revision: stored.authorization_revision,
        }
    }
}

impl From<LogicalBucketRecord> for StoredBucket {
    fn from(record: LogicalBucketRecord) -> Self {
        Self {
            format_version: PROVISIONING_FORMAT_VERSION,
            tenant_id: TenantId(record.tenant_id),
            bucket_id: BucketId(record.bucket_id),
            storage_tenant: record.storage_tenant,
            bucket: record.bucket,
            owner: record.owner,
            authorization_revision: record.authorization_revision,
        }
    }
}

impl From<StoredApplication> for LogicalApplicationRecord {
    fn from(stored: StoredApplication) -> Self {
        Self {
            app_id: stored.app_id,
            client_id: stored.client_id,
            storage_tenant: stored.storage_tenant,
        }
    }
}

impl From<LogicalApplicationRecord> for StoredApplication {
    fn from(record: LogicalApplicationRecord) -> Self {
        Self {
            format_version: APPLICATION_FORMAT_VERSION,
            app_id: record.app_id,
            client_id: record.client_id,
            storage_tenant: record.storage_tenant,
        }
    }
}

impl From<StoredApplicationCredential> for LogicalCredentialRecord {
    fn from(stored: StoredApplicationCredential) -> Self {
        Self {
            app_id: stored.app_id,
            client_id: stored.client_id,
            storage_tenant: stored.storage_tenant,
            active: stored.active,
            verifier: stored.verifier,
            sigv4_secret: stored.sigv4_secret,
        }
    }
}

impl From<LogicalCredentialRecord> for StoredApplicationCredential {
    fn from(record: LogicalCredentialRecord) -> Self {
        Self {
            format_version: CREDENTIAL_FORMAT_VERSION,
            app_id: record.app_id,
            client_id: record.client_id,
            storage_tenant: record.storage_tenant,
            active: record.active,
            verifier: record.verifier,
            sigv4_secret: record.sigv4_secret,
        }
    }
}

impl From<MutationError> for LogicalRecordError {
    fn from(error: MutationError) -> Self {
        storage(error)
    }
}

mod export;
pub use export::{
    LogicalRecordCursor, LogicalRecordExport, LogicalRecordExportPage,
    LogicalRecordSnapshotApplied, MAX_LOGICAL_RECORD_EXPORT_BYTES,
    MAX_LOGICAL_RECORD_EXPORT_RECORDS,
};

#[cfg(test)]
mod tests;
