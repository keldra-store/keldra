use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use keldra_atomic_program::{
    AtomicProgramEngine, AtomicWriteBundle, CommandReceipt, EngineError, ExecutionLease,
    ExpandedProgramPath, HeadPrecondition, InvocationContext, ObjectPath, ObservedHead,
    ProgramDefinition, ProgramInvocation, ProgramSnapshot, StateReader, StoredValue,
    VersionedDocument,
};
use rocksdb::{WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::key::{BucketId, BucketIdentity, TenantId};
use crate::model::{MUTATION_STAMP_FORMAT, MutationStamp};
use crate::store::{
    CF_HEADS, CF_METADATA, CF_VERSIONS, LocalReferenceEffects, PendingBlobReferences,
    PendingLocalChange, StoredVersion, StoredVersionRetention, VERSION_HIGH_WATERMARK_KEY,
    is_program_definition_path, now_unix_millis, version_blob_reference, version_key,
};
use crate::{
    AccountingHeadTransition, BlobRef, Head, MutationError, ObjectKey, ObjectMutationContext,
    ReferenceDelta, Store, Version, VersionId,
};

mod alias_resolution;
mod builtin;
mod distributed;
mod publication;
mod reservations;
mod validation;

use alias_resolution::{stored_alias_delete_binding, stored_alias_registry_transitions};
use validation::{
    conservative_atomic_source_journal_changes, live_version_length, prepared_alias_publications,
    publishes_physical_write, validate_atomic_delivery_bound, validate_builtin_plan,
    validate_builtin_record, validate_observed_head,
};

pub use distributed::{
    CoordinatedProgramPathFinalization, ProgramAliasRegistryMutation, ProgramAliasRegistryStage,
    ProgramPathMutation, ProgramPathStage, ReplicaProgramPathApplied,
    alias_registry_stages_from_prepared, path_stage_from_prepared,
};
pub use publication::SealedAtomicBatchPublication;
pub use reservations::{
    BuiltInAliasObservation, BuiltInAliasRegistryAccess, BuiltInObjectTransactionPlan,
    BuiltInReadProof, BuiltInTransactionAssertion, BuiltInVersionWrite, BuiltInWritePayload,
    ExistingReferenceWrite, PROGRAM_PARTICIPANT_MANIFEST_FORMAT, PROGRAM_PATH_RESERVATION_FORMAT,
    ProgramAliasBinding, ProgramAliasRegistryCondition, ProgramBundleAuthority,
    ProgramGovernanceParticipant, ProgramGovernanceReservation, ProgramObjectParticipant,
    ProgramParticipantIntent, ProgramParticipantManifest, ProgramPathCondition,
    ProgramPathReservation, ProgramReservation, ProgramReservationState,
    StoredProgramAliasRegistryTransition,
};

const PREPARED_BUNDLE_FORMAT: u16 = 5;
const LEGACY_PREPARED_BUNDLE_FORMAT: u16 = 4;
const DURABILITY_EVIDENCE_FORMAT: u16 = 1;
const APPLIED_PROGRAM_COMMIT_KEY: &[u8] = b"applied_program_commit";
const ATOMIC_BATCH_PUBLISHED_KEY: &[u8] = b"atomic_batch_published";
const LOCAL_DURABILITY_CLASS: &str = "local";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProgramHash(pub [u8; 32]);

impl std::fmt::Display for ProgramHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PreparedBundleHash(pub [u8; 32]);

impl std::fmt::Display for PreparedBundleHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

/// Ordinary content-addressed reference to the one prepared bundle blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PreparedBundleRef {
    pub hash: [u8; 32],
    pub length: u64,
}

impl From<BlobRef> for PreparedBundleRef {
    fn from(reference: BlobRef) -> Self {
        Self {
            hash: reference.hash,
            length: reference.length,
        }
    }
}

impl From<PreparedBundleRef> for BlobRef {
    fn from(reference: PreparedBundleRef) -> Self {
        Self {
            hash: reference.hash,
            length: reference.length,
        }
    }
}

/// Content identity of the configured remote durability class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProgramDurabilityClassHash(pub [u8; 32]);

impl ProgramDurabilityClassHash {
    pub fn for_class(class: &str) -> Self {
        Self(tagged_hash(b"keldra.durability-class.v1", class.as_bytes()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum ProgramDurabilityScope {
    /// The ordinary blob plane persisted the complete preparation locally.
    ExecutorLocal { node_id: u16, synced: bool },
    /// An injected provider attests that the complete manifest is recoverable
    /// under its named remote durability class. This crate deliberately does
    /// not define that class's participants or replication rule.
    ConfiguredRemote { class: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramDurabilityEvidence {
    pub format: u16,
    pub bundle: PreparedBundleRef,
    pub scope: ProgramDurabilityScope,
    /// Reserved for a later replicated byte-plane acknowledgement. It is
    /// empty for 0.5.0 LOCAL and is never stored in a bespoke side plane.
    pub provider_receipt: Vec<u8>,
}

impl ProgramDurabilityEvidence {
    pub fn hash(&self) -> Result<ProgramDurabilityEvidenceHash, ProgramStoreError> {
        let encoded = serde_json::to_vec(self).map_err(program_storage_error)?;
        Ok(ProgramDurabilityEvidenceHash(tagged_hash(
            b"keldra.program-durability-evidence.v1",
            &encoded,
        )))
    }

    pub fn is_remote_recoverable(&self) -> bool {
        matches!(self.scope, ProgramDurabilityScope::ConfiguredRemote { .. })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProgramDurabilityEvidenceHash(pub [u8; 32]);

impl std::fmt::Display for ProgramDurabilityEvidenceHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VerifiedProgramDefinition {
    pub hash: ProgramHash,
    pub definition: ProgramDefinition,
}

pub struct StoreProgramEngine {
    program_hash: ProgramHash,
    inner: AtomicProgramEngine<Store>,
    policy_gate: Arc<tokio::sync::RwLock<()>>,
}

#[derive(Debug)]
pub struct ProgramExecutionLease {
    program_hash: ProgramHash,
    inner: ExecutionLease,
    _policy_guard: tokio::sync::OwnedRwLockReadGuard<()>,
}

impl StoreProgramEngine {
    pub fn program_hash(&self) -> ProgramHash {
        self.program_hash
    }

    pub fn definition(&self) -> &ProgramDefinition {
        self.inner.definition()
    }

    pub fn expanded_paths(
        &self,
        context: &InvocationContext,
        invocation: &ProgramInvocation,
    ) -> Result<Vec<ExpandedProgramPath>, EngineError> {
        self.inner.expanded_paths(context, invocation)
    }

    pub async fn prepare(
        &self,
        context: &InvocationContext,
        invocation: &ProgramInvocation,
    ) -> Result<ProgramExecutionLease, EngineError> {
        let policy_guard = self.policy_gate.clone().read_owned().await;
        Ok(ProgramExecutionLease {
            program_hash: self.program_hash,
            inner: self.inner.prepare(context, invocation).await?,
            _policy_guard: policy_guard,
        })
    }

    pub async fn prepare_canonicalized(
        &self,
        context: &InvocationContext,
        invocation: &ProgramInvocation,
        canonical_paths: &BTreeMap<ObjectPath, ObjectPath>,
    ) -> Result<ProgramExecutionLease, EngineError> {
        let policy_guard = self.policy_gate.clone().read_owned().await;
        Ok(ProgramExecutionLease {
            program_hash: self.program_hash,
            inner: self
                .inner
                .prepare_canonicalized(context, invocation, canonical_paths)
                .await?,
            _policy_guard: policy_guard,
        })
    }
}

impl ProgramExecutionLease {
    pub fn bundle(&self) -> &AtomicWriteBundle {
        self.inner.bundle()
    }

    pub fn release(self) -> Box<AtomicWriteBundle> {
        self.inner.release()
    }
}

/// Compact preparation descriptor. Output bodies and the complete bundle are
/// ordinary blob-plane objects; only their compact identities enter Raft.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedProgramBundle {
    pub hash: PreparedBundleHash,
    pub source_bundle_hash: PreparedBundleHash,
    pub program_hash: ProgramHash,
    pub authority: ProgramBundleAuthority,
    pub participant_manifest_hash: [u8; 32],
    pub bundle: PreparedBundleRef,
    pub durability_evidence_hash: ProgramDurabilityEvidenceHash,
    pub durability: ProgramDurabilityEvidence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedProgramVersion {
    pub version: VersionId,
    pub deleted: bool,
}

impl PreparedProgramBundle {
    /// Returns evidence suitable for a cluster-safe `CommitBatch`. The
    /// 0.5.0 executor-local byte plane always fails this check.
    pub fn remote_durability_evidence_hash(
        &self,
    ) -> Result<ProgramDurabilityEvidenceHash, ProgramStoreError> {
        if self.durability.is_remote_recoverable() {
            Ok(self.durability_evidence_hash)
        } else {
            Err(ProgramStoreError::ExecutorLocalDurability)
        }
    }
}

/// The only local finalization marker. It deliberately contains no program
/// output, object path, or command receipt: the ordinary prepared bundle and
/// Raft's bounded committed-invocation entry are authoritative for replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AppliedProgramCommit {
    pub commit_cursor: u64,
    pub bundle_ref: PreparedBundleRef,
    pub bundle_hash: PreparedBundleHash,
    pub program_hash: ProgramHash,
    pub authority: ProgramBundleAuthority,
    pub participant_manifest_hash: [u8; 32],
    pub durability_class: ProgramDurabilityClassHash,
    pub durability_evidence_hash: ProgramDurabilityEvidenceHash,
}

#[derive(Deserialize)]
struct AppliedProgramCommitWire {
    commit_cursor: u64,
    bundle_ref: PreparedBundleRef,
    bundle_hash: PreparedBundleHash,
    program_hash: ProgramHash,
    #[serde(default)]
    authority: Option<ProgramBundleAuthority>,
    #[serde(default)]
    participant_manifest_hash: Option<[u8; 32]>,
    durability_class: ProgramDurabilityClassHash,
    durability_evidence_hash: ProgramDurabilityEvidenceHash,
}

impl<'de> Deserialize<'de> for AppliedProgramCommit {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = AppliedProgramCommitWire::deserialize(deserializer)?;
        Ok(Self {
            commit_cursor: wire.commit_cursor,
            bundle_ref: wire.bundle_ref,
            bundle_hash: wire.bundle_hash,
            program_hash: wire.program_hash,
            authority: wire
                .authority
                .unwrap_or(ProgramBundleAuthority::LegacyProgramOnly {
                    program_path_hash: [0; 32],
                    program_hash: wire.program_hash.0,
                }),
            participant_manifest_hash: wire.participant_manifest_hash.unwrap_or([0; 32]),
            durability_class: wire.durability_class,
            durability_evidence_hash: wire.durability_evidence_hash,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommittedProgramResult {
    pub receipt: CommandReceipt,
    pub published_versions: BTreeMap<ObjectPath, PublishedProgramVersion>,
    pub asserted_versions: BTreeMap<ObjectPath, Version>,
    pub alias_targets: BTreeMap<ObjectPath, ObjectPath>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgramCommit {
    pub previous_commit_cursor: Option<u64>,
    pub commit_cursor: u64,
    pub begin_cursor: u64,
    pub bundle_ref: PreparedBundleRef,
    pub bundle_hash: PreparedBundleHash,
    pub program_hash: ProgramHash,
    pub authority: ProgramBundleAuthority,
    pub participant_manifest_hash: [u8; 32],
    pub durability_class: ProgramDurabilityClassHash,
    pub durability_evidence_hash: ProgramDurabilityEvidenceHash,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProgramStoreError {
    #[error("invalid program definition: {0}")]
    InvalidDefinition(String),
    #[error("loaded program bytes do not match the expected immutable object hash")]
    ProgramHashMismatch,
    #[error("prepared bundle {0} was not found")]
    PreparedBundleNotFound(PreparedBundleHash),
    #[error("prepared bundle does not belong to this execution lease")]
    PreparedBundleMismatch,
    #[error("invalid prepared bundle: {0}")]
    InvalidBundle(String),
    #[error("atomic write cannot replace or delete create-once path {path:?}")]
    Immutable { path: ObjectPath },
    #[error("program precondition failed for {path:?}; current version is {current:?}")]
    PreconditionFailed {
        path: ObjectPath,
        current: Option<VersionId>,
    },
    #[error(
        "program commit {requested} expected predecessor {expected:?}, but local predecessor is {applied:?}"
    )]
    OutOfOrderCommit {
        applied: Option<u64>,
        expected: Option<u64>,
        requested: u64,
    },
    #[error("commit cursor {cursor} is already bound to a different prepared bundle")]
    CommitCorruption { cursor: u64 },
    #[error("durability evidence does not bind the committed bundle")]
    DurabilityEvidenceMismatch,
    #[error("durability evidence class does not match the committed durability class")]
    DurabilityClassMismatch,
    #[error(
        "prepared ordinary blobs are durable only on the executor and cannot back a cluster-safe commit"
    )]
    ExecutorLocalDurability,
    #[error("source journal capacity is exhausted before required consumers are durable")]
    SourceJournalCapacity,
    #[error(
        "one atomic source-journal transition requires {entries} records and {bytes} bytes, exceeding the configured bounds of {maximum_entries} records and {maximum_bytes} bytes"
    )]
    SourceJournalTransitionTooLarge {
        entries: u64,
        bytes: u64,
        maximum_entries: u64,
        maximum_bytes: u64,
    },
    #[error("storage error: {0}")]
    Storage(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredPreparedBundle {
    format: u16,
    source_bundle_hash: PreparedBundleHash,
    program_hash: ProgramHash,
    authority: ProgramBundleAuthority,
    participant_manifest: ProgramParticipantManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    builtin_plan: Option<BuiltInObjectTransactionPlan>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    alias_bindings: Vec<ProgramAliasBinding>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    alias_registry_transitions: Vec<StoredProgramAliasRegistryTransition>,
    preconditions: Vec<HeadPrecondition>,
    writes: Vec<PreparedVersionWrite>,
    receipt: CommandReceipt,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct LegacyStoredPreparedBundleV4 {
    format: u16,
    source_bundle_hash: PreparedBundleHash,
    program_hash: ProgramHash,
    preconditions: Vec<HeadPrecondition>,
    writes: Vec<PreparedVersionWrite>,
    receipt: CommandReceipt,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PreparedVersionWrite {
    path: ObjectPath,
    expected: ObservedHead,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    previous_version: Option<Version>,
    version: Version,
}

pub type PreparedProgramRecord = StoredPreparedBundle;
pub type PreparedProgramWrite = PreparedVersionWrite;

#[derive(Debug)]
struct LoadedPreparedBundle {
    bundle: PreparedBundleRef,
    record: StoredPreparedBundle,
    evidence: ProgramDurabilityEvidence,
}

impl ProgramHash {
    pub fn for_definition_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }
}

impl VerifiedProgramDefinition {
    /// Verifies bytes already loaded from the immutable ordinary object at the
    /// program path. This type proves parsing and content-hash verification; it
    /// does not persist the definition anywhere.
    pub fn from_bytes(bytes: &[u8], expected_hash: ProgramHash) -> Result<Self, ProgramStoreError> {
        if ProgramHash::for_definition_bytes(bytes) != expected_hash {
            return Err(ProgramStoreError::ProgramHashMismatch);
        }
        let definition =
            serde_json::from_slice::<ProgramDefinition>(bytes).map_err(program_storage_error)?;
        definition
            .validate()
            .map_err(|error| ProgramStoreError::InvalidDefinition(error.to_string()))?;
        Ok(Self {
            hash: expected_hash,
            definition,
        })
    }
}

impl Store {
    /// Constructs an evaluator that necessarily shares this store's path-lock
    /// table and reads its committed snapshot.
    pub fn program_engine(
        &self,
        program: &VerifiedProgramDefinition,
    ) -> Result<StoreProgramEngine, EngineError> {
        let inner = AtomicProgramEngine::with_lock_manager(
            program.definition.clone(),
            self.clone(),
            self.lock_manager(),
        )?;
        Ok(StoreProgramEngine {
            program_hash: program.hash,
            inner,
            policy_gate: self.policy_gate.clone(),
        })
    }
}

impl StateReader for Store {
    async fn read_snapshot(
        &self,
        document_paths: &[ObjectPath],
    ) -> Result<ProgramSnapshot, String> {
        let selected = {
            let snapshot = self.db.snapshot();
            let mut selected = Vec::with_capacity(document_paths.len());
            let mut identity_cache =
                BTreeMap::<(String, String), Result<BucketIdentity, String>>::new();
            for path in document_paths {
                let key = object_key(path).map_err(|error| error.to_string())?;
                let cache_key = (path.tenant.clone(), path.bucket.clone());
                let identity = identity_cache
                    .entry(cache_key)
                    .or_insert_with(|| {
                        self.resolve_bucket_identity(&path.tenant, &path.bucket)
                            .map_err(|error| error.to_string())
                    })
                    .clone()?;
                let head = snapshot
                    .get_cf(
                        self.cf(CF_HEADS).map_err(|error| error.to_string())?,
                        identity.head_key(key.path()),
                    )
                    .map_err(|error| error.to_string())?
                    .map(|encoded| serde_json::from_slice::<Head>(&encoded))
                    .transpose()
                    .map_err(|error| error.to_string())?;
                let version = match head {
                    Some(head) => Some(
                        snapshot
                            .get_cf(
                                self.cf(CF_VERSIONS).map_err(|error| error.to_string())?,
                                version_key(identity, &key, head.version),
                            )
                            .map_err(|error| error.to_string())?
                            .ok_or_else(|| "head references a missing version".to_owned())
                            .and_then(|encoded| {
                                StoredVersion::decode(&encoded)
                                    .map(|stored| stored.version)
                                    .map_err(|error| error.to_string())
                            })?,
                    ),
                    None => None,
                };
                selected.push((path.clone(), version));
            }

            selected
        };

        let mut documents = BTreeMap::new();
        for (path, version) in selected {
            let Some(version) = version else {
                continue;
            };
            if version.deleted {
                if version.blob.is_some() {
                    return Err("tombstone version unexpectedly has a payload".into());
                }
                documents.insert(
                    path,
                    VersionedDocument {
                        version: version.id.0.to_string(),
                        value: None,
                        content_type: None,
                    },
                );
                continue;
            }
            let bytes = match &version.blob {
                Some(blob) => self
                    .read_blob_bytes(blob)
                    .await
                    .map_err(|error| error.to_string())?,
                None => return Err("live version has an invalid payload shape".into()),
            };
            let content_type = version
                .content_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".into());
            let value = if is_json_content_type(&content_type) {
                StoredValue::Json(
                    serde_json::from_slice(&bytes).map_err(|error| error.to_string())?,
                )
            } else {
                StoredValue::Opaque(bytes)
            };
            documents.insert(
                path,
                VersionedDocument {
                    version: version.id.0.to_string(),
                    value: Some(value),
                    content_type: Some(content_type),
                },
            );
        }
        Ok(ProgramSnapshot { documents })
    }
}

impl Store {
    fn raw_get(
        &self,
        column_family: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ProgramStoreError> {
        self.db
            .get_cf(self.program_cf(column_family)?, key)
            .map(|value| value.map(|value| value.to_vec()))
            .map_err(program_storage_error)
    }

    fn read_program_json<T: for<'de> Deserialize<'de>>(
        &self,
        column_family: &'static str,
        key: &[u8],
    ) -> Result<Option<T>, ProgramStoreError> {
        self.raw_get(column_family, key)?
            .map(|encoded| serde_json::from_slice(&encoded).map_err(program_storage_error))
            .transpose()
    }

    fn program_cf(&self, name: &'static str) -> Result<&rocksdb::ColumnFamily, ProgramStoreError> {
        self.cf(name).map_err(program_mutation_error)
    }

    fn write_program_batch(&self, batch: WriteBatch) -> Result<(), ProgramStoreError> {
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .write_opt(batch, &options)
            .map_err(program_storage_error)
    }
}

fn validate_source_bundle(source: &AtomicWriteBundle) -> Result<(), ProgramStoreError> {
    let mut preconditions = BTreeMap::new();
    for precondition in &source.head_preconditions {
        validate_observed_head(&precondition.expected)?;
        if preconditions
            .insert(precondition.path.clone(), precondition.expected.clone())
            .is_some()
        {
            return Err(ProgramStoreError::InvalidBundle(
                "duplicate head precondition".into(),
            ));
        }
    }
    let mut writes = BTreeSet::new();
    for write in &source.writes {
        if !writes.insert(write.path.clone()) {
            return Err(ProgramStoreError::InvalidBundle(
                "duplicate versioned write".into(),
            ));
        }
        if preconditions.get(&write.path) != Some(&write.expected) {
            return Err(ProgramStoreError::InvalidBundle(
                "write does not match its exact head precondition".into(),
            ));
        }
        match (&write.value, &write.content_type) {
            (Some(StoredValue::Json(_)), Some(content_type))
                if is_json_content_type(content_type) => {}
            (Some(StoredValue::Opaque(_)), Some(_)) | (None, None) => {}
            _ => {
                return Err(ProgramStoreError::InvalidBundle(
                    "write value/content type pairing is invalid".into(),
                ));
            }
        }
    }
    if source.outputs != source.receipt.outputs {
        return Err(ProgramStoreError::InvalidBundle(
            "bundle outputs do not match command receipt".into(),
        ));
    }
    Ok(())
}

fn validate_prepared_record(record: &StoredPreparedBundle) -> Result<(), ProgramStoreError> {
    let legacy = record.format == LEGACY_PREPARED_BUNDLE_FORMAT
        && matches!(
            record.authority,
            ProgramBundleAuthority::LegacyProgramOnly { .. }
        );
    if record.format != PREPARED_BUNDLE_FORMAT && !legacy {
        return Err(ProgramStoreError::InvalidBundle(
            "unsupported prepared record format".into(),
        ));
    }
    record
        .authority
        .validate(legacy)
        .map_err(|message| ProgramStoreError::InvalidBundle(message.into()))?;
    validate_builtin_record(record)?;
    if !legacy {
        record
            .participant_manifest
            .validate()
            .map_err(ProgramStoreError::InvalidBundle)?;
    }
    let manifest_heads = record
        .participant_manifest
        .objects
        .iter()
        .filter_map(|participant| {
            participant
                .condition
                .observed_head()
                .map(|head| (participant.path.clone(), head))
        })
        .collect::<BTreeMap<_, _>>();
    let mut preconditions = BTreeMap::new();
    for precondition in &record.preconditions {
        validate_observed_head(&precondition.expected)?;
        if preconditions
            .insert(precondition.path.clone(), precondition.expected.clone())
            .is_some()
        {
            return Err(ProgramStoreError::InvalidBundle(
                "duplicate prepared precondition".into(),
            ));
        }
    }
    if !legacy && manifest_heads != preconditions {
        return Err(ProgramStoreError::InvalidBundle(
            "prepared participant manifest does not bind every head precondition".into(),
        ));
    }
    let mut write_paths = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    for write in &record.writes {
        let participant = record
            .participant_manifest
            .objects
            .iter()
            .find(|participant| participant.path == write.path);
        if !legacy && participant.is_none() {
            return Err(ProgramStoreError::InvalidBundle(
                "prepared write has no participant intent".into(),
            ));
        }
        if preconditions.get(&write.path) != Some(&write.expected)
            || !write_paths.insert(write.path.clone())
            || !version_ids.insert(write.version.id)
            || participant.is_some_and(|participant| !participant.intent.put)
            || participant
                .is_some_and(|participant| participant.intent.delete != write.version.deleted)
        {
            return Err(ProgramStoreError::InvalidBundle(
                "prepared write has no unique matching precondition or version".into(),
            ));
        }
        if let Some(previous) = write.previous_version.as_ref() {
            if !matches!(
                &write.expected,
                ObservedHead::Version { version }
                    if version.parse::<u64>().ok() == Some(previous.id.0)
                        && previous.id < write.version.id
            ) {
                return Err(ProgramStoreError::InvalidBundle(
                    "prepared write predecessor does not match its observed head".into(),
                ));
            }
        }
        let valid_tombstone = write.version.deleted
            && write.version.blob.is_none()
            && write.version.content_type.is_none();
        let valid_live = !write.version.deleted
            && write.version.blob.is_some()
            && (write.version.content_type.is_some()
                || matches!(
                    record.authority,
                    ProgramBundleAuthority::BuiltInObjectTransaction { .. }
                ));
        if !valid_tombstone && !valid_live {
            return Err(ProgramStoreError::InvalidBundle(
                "prepared version has an invalid payload or tombstone shape".into(),
            ));
        }
        let protected_link_descriptor = matches!(
            record.authority,
            ProgramBundleAuthority::BuiltInObjectTransaction {
                kind: 2,
                contract_version: 1
            }
        ) && !write.version.deleted
            && write.version.content_type.as_deref() == Some(crate::OBJECT_LINK_CONTENT_TYPE);
        if write.version.protected_link_descriptor != protected_link_descriptor {
            return Err(ProgramStoreError::InvalidBundle(
                "prepared version has unauthorized protected-link provenance".into(),
            ));
        }
    }
    if !legacy
        && record
            .participant_manifest
            .objects
            .iter()
            .any(|participant| participant.intent.put && !write_paths.contains(&participant.path))
    {
        return Err(ProgramStoreError::InvalidBundle(
            "participant put intent has no exact prepared write".into(),
        ));
    }
    validate_atomic_delivery_bound(record)?;
    Ok(())
}

fn head_matches(
    expected: &ObservedHead,
    current: Option<&Head>,
) -> Result<bool, ProgramStoreError> {
    match expected {
        ObservedHead::NeverExisted => Ok(current.is_none()),
        ObservedHead::Version { version } => {
            let expected = version.parse::<u64>().map_err(|_| {
                ProgramStoreError::InvalidBundle(format!("invalid store version `{version}`"))
            })?;
            Ok(current.is_some_and(|head| head.version == VersionId(expected)))
        }
    }
}

fn encode_stored_value(value: &StoredValue) -> Result<Vec<u8>, ProgramStoreError> {
    match value {
        StoredValue::Json(value) => serde_json::to_vec(value).map_err(program_storage_error),
        StoredValue::Opaque(value) => Ok(value.clone()),
    }
}

fn is_json_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}

fn object_key(path: &ObjectPath) -> Result<ObjectKey, ProgramStoreError> {
    ObjectKey::new(&path.tenant, &path.bucket, &path.path)
        .map_err(|error| ProgramStoreError::InvalidBundle(error.to_string()))
}

fn committed_result(record: &StoredPreparedBundle) -> CommittedProgramResult {
    let published_versions = record
        .writes
        .iter()
        .map(|write| {
            (
                write.path.clone(),
                PublishedProgramVersion {
                    version: write.version.id,
                    deleted: write.version.deleted,
                },
            )
        })
        .collect();
    CommittedProgramResult {
        receipt: record.receipt.clone(),
        published_versions,
        asserted_versions: record.asserted_versions(),
        alias_targets: record.alias_targets(),
    }
}

fn validate_durability_evidence(
    evidence: &ProgramDurabilityEvidence,
) -> Result<(), ProgramStoreError> {
    if evidence.format != DURABILITY_EVIDENCE_FORMAT
        || evidence.bundle.hash == [0; 32]
        || evidence.bundle.length == 0
        || matches!(
            &evidence.scope,
            ProgramDurabilityScope::ConfiguredRemote { class } if class.trim().is_empty()
        )
    {
        return Err(ProgramStoreError::DurabilityEvidenceMismatch);
    }
    Ok(())
}

fn verify_loaded_commit(
    loaded: &LoadedPreparedBundle,
    commit: &ProgramCommit,
) -> Result<(), ProgramStoreError> {
    if loaded.bundle != commit.bundle_ref
        || PreparedBundleHash(loaded.bundle.hash) != commit.bundle_hash
        || loaded.record.program_hash != commit.program_hash
        || loaded.record.authority != commit.authority
        || loaded
            .record
            .participant_manifest_hash(commit.bundle_hash)?
            != commit.participant_manifest_hash
        || loaded.evidence.hash()? != commit.durability_evidence_hash
    {
        return Err(ProgramStoreError::PreparedBundleMismatch);
    }

    verify_commit_durability(&loaded.evidence.scope, commit.durability_class)
}

fn verify_prepared_commit(
    prepared: &PreparedProgramBundle,
    commit: &ProgramCommit,
) -> Result<(), ProgramStoreError> {
    if prepared.bundle != commit.bundle_ref
        || prepared.hash != commit.bundle_hash
        || prepared.program_hash != commit.program_hash
        || prepared.authority != commit.authority
        || prepared.participant_manifest_hash != commit.participant_manifest_hash
        || prepared.durability_evidence_hash != commit.durability_evidence_hash
        || prepared.durability.hash()? != commit.durability_evidence_hash
    {
        return Err(ProgramStoreError::PreparedBundleMismatch);
    }

    verify_commit_durability(&prepared.durability.scope, commit.durability_class)
}

fn verify_commit_durability(
    scope: &ProgramDurabilityScope,
    committed_class: ProgramDurabilityClassHash,
) -> Result<(), ProgramStoreError> {
    let expected_class = match scope {
        ProgramDurabilityScope::ExecutorLocal { synced: true, .. } => {
            ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS)
        }
        ProgramDurabilityScope::ExecutorLocal { synced: false, .. } => {
            return Err(ProgramStoreError::ExecutorLocalDurability);
        }
        ProgramDurabilityScope::ConfiguredRemote { class } => {
            ProgramDurabilityClassHash::for_class(class)
        }
    };
    if expected_class != committed_class {
        return Err(ProgramStoreError::DurabilityClassMismatch);
    }
    Ok(())
}

fn tagged_hash(tag: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(tag);
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn program_storage_error(error: impl std::fmt::Display) -> ProgramStoreError {
    ProgramStoreError::Storage(error.to_string())
}

fn program_mutation_error(error: MutationError) -> ProgramStoreError {
    match error {
        MutationError::SourceJournalCapacity => ProgramStoreError::SourceJournalCapacity,
        MutationError::SourceJournalTransitionTooLarge {
            entries,
            bytes,
            maximum_entries,
            maximum_bytes,
        } => ProgramStoreError::SourceJournalTransitionTooLarge {
            entries,
            bytes,
            maximum_entries,
            maximum_bytes,
        },
        other => ProgramStoreError::Storage(other.to_string()),
    }
}

impl Store {
    /// Finalizes a committed prepared bundle while the evaluator's exact-path
    /// locks are still held. `ProgramCommit::previous_commit_cursor` is the
    /// predecessor among atomic-program commits, not `commit_cursor - 1` in
    /// the Raft log.
    pub async fn apply_program_bundle(
        &self,
        lease: ProgramExecutionLease,
        prepared: &PreparedProgramBundle,
        commit: ProgramCommit,
        mutation_context: ObjectMutationContext,
    ) -> Result<CommittedProgramResult, ProgramStoreError> {
        let result = async {
            let source = lease.bundle();
            let source_encoded = serde_json::to_vec(source).map_err(program_storage_error)?;
            let source_hash = PreparedBundleHash(tagged_hash(
                b"keldra.atomic-source-bundle.v1",
                &source_encoded,
            ));
            if source_hash != prepared.source_bundle_hash {
                return Err(ProgramStoreError::PreparedBundleMismatch);
            }
            verify_prepared_commit(prepared, &commit)?;
            loop {
                let commit_guard = self.lock_commit("atomic_program").await;
                let loaded = self
                    .load_prepared_bundle(
                        commit.bundle_ref,
                        commit.bundle_hash,
                        commit.durability_evidence_hash,
                    )
                    .await?;
                if source_hash != loaded.record.source_bundle_hash
                    || prepared.bundle != loaded.bundle
                    || prepared.durability != loaded.evidence
                {
                    return Err(ProgramStoreError::PreparedBundleMismatch);
                }
                verify_loaded_commit(&loaded, &commit)?;
                let attempt = if let Some(existing) = self.applied_program_commit()?
                    && existing.commit_cursor == commit.commit_cursor
                {
                    self.match_applied_commit(&existing, &commit)?;
                    Ok(committed_result(&loaded.record))
                } else {
                    self.apply_prepared_record(&loaded, &commit, mutation_context)
                };
                drop(commit_guard);
                match attempt {
                    Err(ProgramStoreError::SourceJournalCapacity) => {
                        self.wait_for_mutation_capacity().await;
                    }
                    outcome => break outcome,
                }
            }
        }
        .await;
        drop(lease);
        result
    }

    /// Recovery path for a committed bundle in the ordinary blob plane.
    pub async fn recover_program_bundle(
        &self,
        commit: ProgramCommit,
        mutation_context: ObjectMutationContext,
    ) -> Result<CommittedProgramResult, ProgramStoreError> {
        let _policy_guard = self.policy_gate.read().await;
        if let Some(existing) = self.applied_program_commit()?
            && existing.commit_cursor == commit.commit_cursor
        {
            self.match_applied_commit(&existing, &commit)?;
            return self.committed_program_result(commit).await;
        }
        let loaded = self
            .load_prepared_bundle(
                commit.bundle_ref,
                commit.bundle_hash,
                commit.durability_evidence_hash,
            )
            .await?;
        verify_loaded_commit(&loaded, &commit)?;
        let paths = loaded
            .record
            .preconditions
            .iter()
            .map(|precondition| precondition.path.clone())
            .collect::<Vec<_>>();
        let _guard = self.program_locks.acquire(&paths).await;
        loop {
            let commit_guard = self.lock_commit("atomic_program").await;
            // Re-read and verify under the commit fence so GC cannot retire an
            // awaiting output or bundle between verification and publication.
            let loaded = self
                .load_prepared_bundle(
                    commit.bundle_ref,
                    commit.bundle_hash,
                    commit.durability_evidence_hash,
                )
                .await?;
            let attempt = if let Some(existing) = self.applied_program_commit()?
                && existing.commit_cursor == commit.commit_cursor
            {
                self.match_applied_commit(&existing, &commit)?;
                Ok(committed_result(&loaded.record))
            } else {
                self.apply_prepared_record(&loaded, &commit, mutation_context)
            };
            drop(commit_guard);
            match attempt {
                Err(ProgramStoreError::SourceJournalCapacity) => {
                    self.wait_for_mutation_capacity().await;
                }
                outcome => break outcome,
            }
        }
    }

    /// Reconstructs the exact public response from the ordinary prepared
    /// bundle named by an authoritative committed-invocation entry in Raft.
    /// No second local receipt or result copy is consulted.
    pub async fn committed_program_result(
        &self,
        commit: ProgramCommit,
    ) -> Result<CommittedProgramResult, ProgramStoreError> {
        let loaded = self
            .load_prepared_bundle(
                commit.bundle_ref,
                commit.bundle_hash,
                commit.durability_evidence_hash,
            )
            .await?;
        verify_loaded_commit(&loaded, &commit)?;
        Ok(committed_result(&loaded.record))
    }

    pub fn applied_program_commit(
        &self,
    ) -> Result<Option<AppliedProgramCommit>, ProgramStoreError> {
        self.read_program_json(CF_METADATA, APPLIED_PROGRAM_COMMIT_KEY)
    }

    pub fn applied_program_commit_cursor(&self) -> Result<Option<u64>, ProgramStoreError> {
        Ok(self
            .applied_program_commit()?
            .map(|applied| applied.commit_cursor))
    }

    fn version_high_watermark(&self) -> Result<Option<VersionId>, ProgramStoreError> {
        self.read_program_json(CF_METADATA, VERSION_HIGH_WATERMARK_KEY)
    }

    fn match_applied_commit(
        &self,
        existing: &AppliedProgramCommit,
        requested: &ProgramCommit,
    ) -> Result<(), ProgramStoreError> {
        let legacy_authority_matches = matches!(
            (existing.authority, requested.authority),
            (
                ProgramBundleAuthority::LegacyProgramOnly {
                    program_path_hash: existing_path,
                    program_hash: existing_hash,
                },
                ProgramBundleAuthority::LegacyProgramOnly {
                    program_hash: requested_hash,
                    ..
                }
            ) if existing_path == [0; 32] && existing_hash == requested_hash
        ) && existing.participant_manifest_hash == [0; 32];
        if existing.commit_cursor == requested.commit_cursor
            && existing.bundle_ref == requested.bundle_ref
            && existing.bundle_hash == requested.bundle_hash
            && existing.program_hash == requested.program_hash
            && ((existing.authority == requested.authority
                && existing.participant_manifest_hash == requested.participant_manifest_hash)
                || legacy_authority_matches)
            && existing.durability_class == requested.durability_class
            && existing.durability_evidence_hash == requested.durability_evidence_hash
        {
            Ok(())
        } else {
            Err(ProgramStoreError::CommitCorruption {
                cursor: requested.commit_cursor,
            })
        }
    }

    async fn load_prepared_bundle(
        &self,
        bundle: PreparedBundleRef,
        hash: PreparedBundleHash,
        evidence_hash: ProgramDurabilityEvidenceHash,
    ) -> Result<LoadedPreparedBundle, ProgramStoreError> {
        if bundle.hash != hash.0 || bundle.length == 0 {
            return Err(ProgramStoreError::PreparedBundleMismatch);
        }
        let evidence = self.local_program_durability_evidence(bundle);
        validate_durability_evidence(&evidence)?;
        if evidence.hash()? != evidence_hash {
            return Err(ProgramStoreError::DurabilityEvidenceMismatch);
        }

        let bundle_reference = BlobRef::from(bundle);
        let bundle_bytes = self
            .read_blob_bytes(&bundle_reference)
            .await
            .map_err(|error| match error {
                MutationError::BlobNotFound => ProgramStoreError::PreparedBundleNotFound(hash),
                other => program_mutation_error(other),
            })?;
        let record = serde_json::from_slice::<StoredPreparedBundle>(&bundle_bytes)
            .map_err(program_storage_error)?;
        validate_prepared_record(&record)?;
        for write in &record.writes {
            if let Some(blob) = &write.version.blob {
                // `open_blob` verifies the ordinary byte plane without
                // copying output payloads into the bundle.
                self.open_blob(blob).await.map_err(program_mutation_error)?;
            }
        }

        Ok(LoadedPreparedBundle {
            bundle,
            record,
            evidence,
        })
    }

    fn local_program_durability_evidence(
        &self,
        bundle: PreparedBundleRef,
    ) -> ProgramDurabilityEvidence {
        ProgramDurabilityEvidence {
            format: DURABILITY_EVIDENCE_FORMAT,
            bundle,
            scope: ProgramDurabilityScope::ExecutorLocal {
                node_id: self.node_id,
                synced: self.sync_writes,
            },
            provider_receipt: Vec::new(),
        }
    }

    fn apply_prepared_record(
        &self,
        loaded: &LoadedPreparedBundle,
        commit: &ProgramCommit,
        mutation_context: ObjectMutationContext,
    ) -> Result<CommittedProgramResult, ProgramStoreError> {
        let record = &loaded.record;
        validate_prepared_record(record)?;
        verify_loaded_commit(loaded, commit)?;

        if commit.commit_cursor == 0
            || (commit.begin_cursor == 0
                && !matches!(
                    record.authority,
                    ProgramBundleAuthority::LegacyProgramOnly { .. }
                ))
            || commit
                .previous_commit_cursor
                .is_some_and(|previous| previous >= commit.commit_cursor)
            || mutation_context.active_placement_log_id.term == 0
            || mutation_context.active_placement_log_id.index == 0
            || mutation_context.serving_fence_term == 0
        {
            return Err(ProgramStoreError::InvalidBundle(
                "commit cursors and mutation authority must be non-zero and ordered".into(),
            ));
        }
        let applied_commit = self.applied_program_commit()?;
        if let Some(existing) = &applied_commit
            && existing.commit_cursor == commit.commit_cursor
        {
            self.match_applied_commit(existing, commit)?;
            return Ok(committed_result(record));
        }
        let local_predecessor = applied_commit.as_ref().map(|applied| applied.commit_cursor);
        if local_predecessor != commit.previous_commit_cursor {
            return Err(ProgramStoreError::OutOfOrderCommit {
                applied: local_predecessor,
                expected: commit.previous_commit_cursor,
                requested: commit.commit_cursor,
            });
        }
        if commit.program_hash != record.program_hash {
            return Err(ProgramStoreError::PreparedBundleMismatch);
        }

        if !matches!(
            record.authority,
            ProgramBundleAuthority::LegacyProgramOnly { .. }
        ) {
            for participant in &record.participant_manifest.objects {
                self.require_committed_program_reservation_locked(
                    BucketIdentity {
                        tenant_id: TenantId(participant.tenant_id),
                        bucket_id: BucketId(participant.bucket_id),
                    },
                    &participant.path.path,
                    commit.begin_cursor,
                    commit.commit_cursor,
                    mutation_context.serving_fence_term,
                    mutation_context.active_placement_log_id,
                )
                .map_err(program_mutation_error)?;
            }
            for participant in &record.participant_manifest.governance {
                self.require_committed_governance_reservation_locked(
                    BucketIdentity {
                        tenant_id: TenantId(participant.tenant_id),
                        bucket_id: BucketId(participant.bucket_id),
                    },
                    commit.begin_cursor,
                    commit.commit_cursor,
                    mutation_context.serving_fence_term,
                    mutation_context.active_placement_log_id,
                )
                .map_err(program_mutation_error)?;
            }
        }

        let mut identities = BTreeMap::<(String, String), BucketIdentity>::new();
        for path in record
            .preconditions
            .iter()
            .map(|precondition| &precondition.path)
            .chain(record.writes.iter().map(|write| &write.path))
        {
            let cache_key = (path.tenant.clone(), path.bucket.clone());
            if !identities.contains_key(&cache_key) {
                let identity = self
                    .resolve_bucket_identity(&path.tenant, &path.bucket)
                    .map_err(program_mutation_error)?;
                identities.insert(cache_key, identity);
            }
        }
        let mut current_heads = BTreeMap::new();
        let mut current_versions = BTreeMap::new();
        for precondition in &record.preconditions {
            let key = object_key(&precondition.path)?;
            let identity = *identities
                .get(&(
                    precondition.path.tenant.clone(),
                    precondition.path.bucket.clone(),
                ))
                .ok_or_else(|| {
                    ProgramStoreError::Storage("precondition has no stable bucket identity".into())
                })?;
            let current = self
                .head_by_storage_key(&identity.head_key(key.path()))
                .map_err(program_mutation_error)?;
            if !head_matches(&precondition.expected, current.as_ref())? {
                return Err(ProgramStoreError::PreconditionFailed {
                    path: precondition.path.clone(),
                    current: current.map(|head| head.version),
                });
            }
            let current_version = current
                .as_ref()
                .map(|head| {
                    self.version_metadata_by_identity(identity, &key, head.version)
                        .map_err(program_mutation_error)?
                        .ok_or_else(|| {
                            ProgramStoreError::Storage(
                                "head references a missing version descriptor".into(),
                            )
                        })
                })
                .transpose()?;
            if current_version
                .as_ref()
                .zip(current.as_ref())
                .is_some_and(|(version, head)| {
                    version.id != head.version || version.deleted != head.deleted
                })
            {
                return Err(ProgramStoreError::Storage(
                    "head and current version descriptor disagree".into(),
                ));
            }
            current_heads.insert(precondition.path.clone(), current);
            current_versions.insert(precondition.path.clone(), current_version);
        }

        for write in &record.writes {
            let identity = *identities
                .get(&(write.path.tenant.clone(), write.path.bucket.clone()))
                .ok_or_else(|| {
                    ProgramStoreError::Storage("write has no stable bucket identity".into())
                })?;
            let policy = self
                .bucket_policy_by_key(&identity.encode())
                .map_err(program_mutation_error)?
                .unwrap_or_default();
            if is_program_definition_path(&write.path.path) {
                return Err(ProgramStoreError::Immutable {
                    path: write.path.clone(),
                });
            }
            if policy.is_immutable(&write.path.path)
                && (write.version.deleted
                    || current_heads
                        .get(&write.path)
                        .is_some_and(|head| head.is_some()))
            {
                return Err(ProgramStoreError::Immutable {
                    path: write.path.clone(),
                });
            }
        }
        let result = committed_result(record);
        let applied = AppliedProgramCommit {
            commit_cursor: commit.commit_cursor,
            bundle_ref: commit.bundle_ref,
            bundle_hash: commit.bundle_hash,
            program_hash: record.program_hash,
            authority: record.authority,
            participant_manifest_hash: record.participant_manifest_hash(commit.bundle_hash)?,
            durability_class: commit.durability_class,
            durability_evidence_hash: commit.durability_evidence_hash,
        };

        let journal_status = self
            .local_watch_status()
            .map_err(|error| ProgramStoreError::Storage(error.to_string()))?;
        let mut batch = WriteBatch::default();
        let mut changes = Vec::with_capacity(record.writes.len());
        let mut pending_blob_references = PendingBlobReferences::new();
        let publication_at_unix_millis = now_unix_millis().map_err(program_mutation_error)?;
        for (write_index, write) in record.writes.iter().enumerate() {
            let mut reference_deltas = Vec::new();
            let key = object_key(&write.path)?;
            let identity = *identities
                .get(&(write.path.tenant.clone(), write.path.bucket.clone()))
                .ok_or_else(|| {
                    ProgramStoreError::Storage("write has no stable bucket identity".into())
                })?;
            let retention = self
                .version_retention_for_bucket(identity)
                .map_err(program_mutation_error)?;
            let version_bytes =
                serde_json::to_vec(&StoredVersion::new(write.version.clone(), retention))
                    .map_err(program_storage_error)?;
            let old_version = current_versions
                .get(&write.path)
                .ok_or_else(|| {
                    ProgramStoreError::Storage("write has no current-version observation".into())
                })?
                .as_ref();
            let mut released_predecessor_blob = None;
            if let Some(old_version) = old_version {
                let old_key = version_key(identity, &key, old_version.id);
                if let Some(mut stored) = self
                    .stored_version_by_key(&old_key)
                    .map_err(program_mutation_error)?
                {
                    match stored.retention {
                        StoredVersionRetention::JournalPending
                            if retention == StoredVersionRetention::UserRetained =>
                        {
                            stored.retention = StoredVersionRetention::UserRetained;
                            batch.put_cf(
                                self.program_cf(CF_VERSIONS)?,
                                old_key,
                                serde_json::to_vec(&stored).map_err(program_storage_error)?,
                            );
                        }
                        StoredVersionRetention::JournalReleased
                            if retention == StoredVersionRetention::UserRetained =>
                        {
                            stored.retention = StoredVersionRetention::UserRetained;
                            batch.put_cf(
                                self.program_cf(CF_VERSIONS)?,
                                old_key,
                                serde_json::to_vec(&stored).map_err(program_storage_error)?,
                            );
                        }
                        StoredVersionRetention::JournalReleased => {
                            released_predecessor_blob = stored.version.blob;
                            batch.delete_cf(self.program_cf(CF_VERSIONS)?, old_key);
                        }
                        StoredVersionRetention::JournalPending
                        | StoredVersionRetention::UserRetained => {}
                    }
                }
            }
            let accounting_transition = AccountingHeadTransition::new(
                old_version.and_then(live_version_length),
                live_version_length(&write.version),
            );
            let encoded_version_key = version_key(identity, &key, write.version.id);
            let released_same_as_new = released_predecessor_blob
                .as_ref()
                .zip(write.version.blob.as_ref())
                .is_some_and(|(old, new)| old == new);
            let existing = self.raw_get(CF_VERSIONS, &encoded_version_key)?;
            if let Some(existing) = &existing {
                if existing.as_slice() != version_bytes.as_slice() {
                    return Err(ProgramStoreError::CommitCorruption {
                        cursor: commit.commit_cursor,
                    });
                }
            } else {
                let new_blob =
                    version_blob_reference(&write.version).map_err(program_mutation_error)?;
                if !released_same_as_new && let Some(reference) = new_blob.as_ref() {
                    let (reference_key, state) = self
                        .prepare_blob_reference_publication(
                            reference,
                            &pending_blob_references,
                            publication_at_unix_millis,
                        )
                        .map_err(program_mutation_error)?;
                    self.stage_blob_reference_update(
                        &mut batch,
                        &mut pending_blob_references,
                        reference_key,
                        state,
                    )
                    .map_err(program_mutation_error)?;
                    reference_deltas.push(ReferenceDelta {
                        blob: reference.clone(),
                        change: 1,
                    });
                }
            }
            if !released_same_as_new && let Some(reference) = released_predecessor_blob.as_ref() {
                let (reference_key, state) = self
                    .prepare_blob_reference_retirement(
                        reference,
                        &pending_blob_references,
                        publication_at_unix_millis,
                    )
                    .map_err(program_mutation_error)?;
                self.stage_blob_reference_update(
                    &mut batch,
                    &mut pending_blob_references,
                    reference_key,
                    state,
                )
                .map_err(program_mutation_error)?;
                reference_deltas.push(ReferenceDelta {
                    blob: reference.clone(),
                    change: -1,
                });
            }
            batch.put_cf(
                self.program_cf(CF_VERSIONS)?,
                encoded_version_key,
                version_bytes,
            );
            batch.put_cf(
                self.program_cf(CF_HEADS)?,
                identity.head_key(key.path()),
                serde_json::to_vec(&Head {
                    version: write.version.id,
                    deleted: write.version.deleted,
                    mutation_stamp: Some(MutationStamp {
                        format: MUTATION_STAMP_FORMAT,
                        predecessor_version: old_version.map(|version| version.id),
                        program_commit_cursor: Some(commit.commit_cursor),
                        mutation_fingerprint: commit.bundle_hash.0,
                        active_placement_log_id: mutation_context.active_placement_log_id,
                        serving_fence_term: mutation_context.serving_fence_term,
                        source_id: journal_status.source_id,
                        source_journal_position: journal_status
                            .tail
                            .checked_add(u64::try_from(write_index).map_err(|_| {
                                ProgramStoreError::Storage(
                                    "atomic source-journal position is exhausted".into(),
                                )
                            })?)
                            .and_then(|offset| offset.checked_add(1))
                            .ok_or_else(|| {
                                ProgramStoreError::Storage(
                                    "atomic source-journal position is exhausted".into(),
                                )
                            })?,
                    }),
                })
                .map_err(program_storage_error)?,
            );
            changes.push(PendingLocalChange::ObjectHead {
                identity,
                exact_path: key.path().to_owned(),
                path_version: write.version.id,
                deleted: write.version.deleted,
                program_commit_cursor: Some(commit.commit_cursor),
                reference_deltas,
                accounting_transition: Some(accounting_transition),
                definition_transition: None,
            });
        }
        for (target, expected, replacement_aliases) in record.alias_registry_writes()? {
            let identity = BucketIdentity {
                tenant_id: TenantId(target.tenant_id),
                bucket_id: BucketId(target.bucket_id),
            };
            if !replacement_aliases.is_empty() {
                let resulting_target = record
                    .writes
                    .iter()
                    .find(|write| write.path == target.path)
                    .map(|write| &write.version)
                    .or_else(|| current_versions.get(&target.path).and_then(Option::as_ref))
                    .ok_or_else(|| {
                        ProgramStoreError::InvalidBundle(
                            "nonempty alias registry has no canonical target version".into(),
                        )
                    })?;
                if resulting_target.deleted || resulting_target.protected_link_descriptor {
                    return Err(ProgramStoreError::InvalidBundle(
                        "nonempty alias registry requires a live ordinary canonical target".into(),
                    ));
                }
            }
            self.stage_alias_registry_transition_locked(
                &mut batch,
                identity,
                &target.path.path,
                expected,
                replacement_aliases,
                commit.commit_cursor,
            )
            .map_err(program_mutation_error)?;
        }
        let mut mutations = record
            .writes
            .iter()
            .enumerate()
            .filter(|(_, write)| publishes_physical_write(record, &write.path))
            .map(|(write_index, write)| {
                let identity = identities[&(write.path.tenant.clone(), write.path.bucket.clone())];
                let source_journal_position = journal_status
                    .tail
                    .checked_add(u64::try_from(write_index).unwrap_or(u64::MAX))
                    .and_then(|offset| offset.checked_add(1))
                    .expect("validated atomic source-journal bound");
                crate::AtomicBatchMutation {
                    tenant_id: identity.tenant_id.0,
                    bucket_id: identity.bucket_id.0,
                    exact_path: write.path.path.clone(),
                    canonical_path: None,
                    path_version: write.version.id,
                    deleted: write.version.deleted,
                    source_id: journal_status.source_id,
                    source_journal_position,
                }
            })
            .collect::<Vec<_>>();
        let aliases = prepared_alias_publications(record)?;
        for (alias_index, alias) in aliases.iter().enumerate() {
            let source_journal_position = journal_status
                .tail
                .checked_add(u64::try_from(record.writes.len()).unwrap_or(u64::MAX))
                .and_then(|offset| {
                    offset.checked_add(u64::try_from(alias_index).unwrap_or(u64::MAX))
                })
                .and_then(|offset| offset.checked_add(1))
                .ok_or_else(|| {
                    ProgramStoreError::Storage("alias source-journal position is exhausted".into())
                })?;
            changes.push(PendingLocalChange::AliasObjectHead {
                identity: alias.identity,
                exact_path: alias.requested_path.clone(),
                canonical_path: alias.canonical_path.clone(),
                path_version: alias.canonical_version,
                deleted: alias.deleted,
                program_commit_cursor: Some(commit.commit_cursor),
            });
            mutations.push(crate::AtomicBatchMutation {
                tenant_id: alias.identity.tenant_id.0,
                bucket_id: alias.identity.bucket_id.0,
                exact_path: alias.requested_path.clone(),
                canonical_path: Some(alias.canonical_path.clone()),
                path_version: alias.canonical_version,
                deleted: alias.deleted,
                source_id: journal_status.source_id,
                source_journal_position,
            });
        }
        mutations.sort_unstable();
        let mut affected_routes = mutations
            .iter()
            .map(|mutation| crate::AtomicBatchRoute {
                tenant_id: mutation.tenant_id,
                bucket_id: mutation.bucket_id,
            })
            .collect::<Vec<_>>();
        affected_routes.sort_unstable();
        affected_routes.dedup();
        if !mutations.is_empty() {
            changes.push(PendingLocalChange::AtomicBatchPublished {
                cursor: commit.commit_cursor,
                bundle_hash: commit.bundle_hash,
                affected_routes,
                mutations,
            });
        }
        let bundle_reference = BlobRef::from(loaded.bundle);
        if let Some((reference_key, state)) = self
            .prepare_awaiting_blob_release(
                &bundle_reference,
                &pending_blob_references,
                publication_at_unix_millis,
            )
            .map_err(program_mutation_error)?
        {
            self.stage_blob_reference_update(
                &mut batch,
                &mut pending_blob_references,
                reference_key,
                state,
            )
            .map_err(program_mutation_error)?;
        }
        self.stage_local_changes(&mut batch, &changes, LocalReferenceEffects::AppliedInline)
            .map_err(program_mutation_error)?;
        let allocated_high = record.writes.iter().map(|write| write.version.id).max();
        if let Some(allocated) = allocated_high {
            let persisted = self.version_high_watermark()?.unwrap_or(VersionId(0));
            let high = allocated.max(persisted);
            batch.put_cf(
                self.program_cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&high).map_err(program_storage_error)?,
            );
        }
        batch.put_cf(
            self.program_cf(CF_METADATA)?,
            APPLIED_PROGRAM_COMMIT_KEY,
            serde_json::to_vec(&applied).map_err(program_storage_error)?,
        );
        self.write_program_batch(batch)?;
        if !changes.is_empty() {
            self.settle_inline_source_changes()
                .map_err(program_mutation_error)?;
            self.notify_local_invalidations();
        }
        if let Some(allocated) = allocated_high {
            self.clock.observe(allocated);
        }
        Ok(result)
    }
}

impl Store {
    /// Seals every output and one complete descriptor bundle through the
    /// ordinary blob plane. Nothing becomes visible until Raft commits the
    /// returned compact bundle identity.
    pub async fn prepare_program_bundle(
        &self,
        lease: &ProgramExecutionLease,
    ) -> Result<PreparedProgramBundle, ProgramStoreError> {
        let source = lease.bundle();
        self.validate_program_policies(source)?;
        self.prepare_program_bundle_source(lease.program_hash, source, None, &[])
            .await
    }

    pub async fn prepare_program_bundle_with_aliases(
        &self,
        lease: &ProgramExecutionLease,
        alias_bindings: &[ProgramAliasBinding],
    ) -> Result<PreparedProgramBundle, ProgramStoreError> {
        let source = lease.bundle();
        self.validate_program_policies(source)?;
        self.prepare_program_bundle_source(lease.program_hash, source, None, alias_bindings)
            .await
    }

    /// Distributed preparation uses the executor's shared exact-path lock
    /// manager and an authoritative cluster reader rather than this node's
    /// local reader.
    pub async fn prepare_distributed_program_bundle(
        &self,
        program_hash: ProgramHash,
        source: &AtomicWriteBundle,
        previous_versions: &BTreeMap<ObjectPath, Version>,
    ) -> Result<PreparedProgramBundle, ProgramStoreError> {
        self.prepare_program_bundle_source(program_hash, source, Some(previous_versions), &[])
            .await
    }

    pub async fn prepare_distributed_program_bundle_with_aliases(
        &self,
        program_hash: ProgramHash,
        source: &AtomicWriteBundle,
        previous_versions: &BTreeMap<ObjectPath, Version>,
        alias_bindings: &[ProgramAliasBinding],
    ) -> Result<PreparedProgramBundle, ProgramStoreError> {
        self.prepare_program_bundle_source(
            program_hash,
            source,
            Some(previous_versions),
            alias_bindings,
        )
        .await
    }

    async fn prepare_program_bundle_source(
        &self,
        program_hash: ProgramHash,
        source: &AtomicWriteBundle,
        previous_versions: Option<&BTreeMap<ObjectPath, Version>>,
        alias_bindings: &[ProgramAliasBinding],
    ) -> Result<PreparedProgramBundle, ProgramStoreError> {
        validate_source_bundle(source)?;
        let alias_registry_transitions = stored_alias_registry_transitions(source, alias_bindings)?;
        // This is deliberately before payload staging and, critically, before
        // the prepared bundle can be proposed to consensus. The synthetic
        // records use worst-case route identities, reference deltas and
        // accounting widths, so every actual one-node atomic publication is
        // no larger than the transition proven here.
        let journal_changes = conservative_atomic_source_journal_changes(source, alias_bindings)?;
        self.preflight_source_journal_transition(&journal_changes)
            .map_err(program_mutation_error)?;
        let source_encoded = serde_json::to_vec(source).map_err(program_storage_error)?;
        let source_bundle_hash = PreparedBundleHash(tagged_hash(
            b"keldra.atomic-source-bundle.v1",
            &source_encoded,
        ));

        let committed_at_unix_millis = now_unix_millis().map_err(program_mutation_error)?;
        let mut writes = Vec::with_capacity(source.writes.len());
        let mut allocated_versions = Vec::with_capacity(source.writes.len());
        for write in &source.writes {
            let alias_delete = stored_alias_delete_binding(write, alias_bindings);
            let (blob, deleted) = match &write.value {
                Some(value) => {
                    let bytes = encode_stored_value(value)?;
                    (
                        Some(
                            self.stage_blob(&bytes)
                                .await
                                .map_err(program_mutation_error)?,
                        ),
                        false,
                    )
                }
                None => (None, true),
            };
            let version_id = self.clock.next().map_err(program_storage_error)?;
            allocated_versions.push(version_id);
            let descriptor = PreparedVersionWrite {
                path: alias_delete.map_or_else(
                    || write.path.clone(),
                    |binding| binding.requested_path.clone(),
                ),
                expected: alias_delete.map_or_else(
                    || write.expected.clone(),
                    |binding| ObservedHead::Version {
                        version: binding
                            .descriptor_version
                            .as_ref()
                            .expect("validated alias delete has a descriptor")
                            .id
                            .0
                            .to_string(),
                    },
                ),
                previous_version: alias_delete
                    .and_then(|binding| binding.descriptor_version.clone())
                    .or_else(|| {
                        previous_versions.and_then(|versions| versions.get(&write.path).cloned())
                    }),
                version: Version {
                    id: version_id,
                    blob,
                    content_type: write.content_type.clone(),
                    deleted,
                    committed_at_unix_millis,
                    protected_link_descriptor: false,
                },
            };
            writes.push(descriptor);
        }

        let record = StoredPreparedBundle {
            format: PREPARED_BUNDLE_FORMAT,
            source_bundle_hash,
            program_hash,
            authority: ProgramBundleAuthority::StoredProgram {
                program_path_hash: source.receipt.program_path_hash,
                program_hash: program_hash.0,
            },
            participant_manifest: self.program_participant_manifest(
                source,
                alias_bindings,
                &alias_registry_transitions,
            )?,
            builtin_plan: None,
            alias_bindings: alias_bindings.to_vec(),
            alias_registry_transitions,
            preconditions: prepared_preconditions(source, alias_bindings),
            writes,
            receipt: source.receipt.clone(),
        };
        validate_prepared_record(&record)?;
        let bundle_bytes = serde_json::to_vec(&record).map_err(program_storage_error)?;
        let bundle_ref = PreparedBundleRef::from(
            self.stage_blob(&bundle_bytes)
                .await
                .map_err(program_mutation_error)?,
        );
        let hash = PreparedBundleHash(bundle_ref.hash);

        if let Some(allocated) = allocated_versions.into_iter().max() {
            let _commit_guard = self.lock_commit("atomic_program").await;
            let persisted = self.version_high_watermark()?.unwrap_or(VersionId(0));
            let high = allocated.max(persisted);
            let mut batch = WriteBatch::default();
            batch.put_cf(
                self.program_cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&high).map_err(program_storage_error)?,
            );
            self.write_program_batch(batch)?;
        }

        let durability = self.local_program_durability_evidence(bundle_ref);
        validate_durability_evidence(&durability)?;
        let durability_evidence_hash = durability.hash()?;

        Ok(PreparedProgramBundle {
            hash,
            source_bundle_hash,
            program_hash,
            authority: record.authority,
            participant_manifest_hash: record
                .participant_manifest
                .hash()
                .map_err(ProgramStoreError::InvalidBundle)?,
            bundle: bundle_ref,
            durability_evidence_hash,
            durability,
        })
    }

    pub async fn prepared_program_bundle(
        &self,
        bundle: PreparedBundleRef,
        hash: PreparedBundleHash,
        durability_evidence_hash: ProgramDurabilityEvidenceHash,
    ) -> Result<Option<PreparedProgramBundle>, ProgramStoreError> {
        match self
            .load_prepared_bundle(bundle, hash, durability_evidence_hash)
            .await
        {
            Ok(loaded) => Ok(Some(PreparedProgramBundle {
                hash,
                source_bundle_hash: loaded.record.source_bundle_hash,
                program_hash: loaded.record.program_hash,
                authority: loaded.record.authority,
                participant_manifest_hash: loaded.record.participant_manifest_hash(hash)?,
                bundle: loaded.bundle,
                durability_evidence_hash,
                durability: loaded.evidence,
            })),
            Err(ProgramStoreError::PreparedBundleNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn prepared_program_record(
        &self,
        prepared: &PreparedProgramBundle,
    ) -> Result<PreparedProgramRecord, ProgramStoreError> {
        let reference = BlobRef::from(prepared.bundle);
        let bytes = self
            .read_blob_bytes(&reference)
            .await
            .map_err(program_mutation_error)?;
        PreparedProgramRecord::decode_distributed(
            &bytes,
            prepared.bundle,
            prepared.hash,
            prepared.program_hash,
        )
    }

    fn validate_program_policies(
        &self,
        source: &AtomicWriteBundle,
    ) -> Result<(), ProgramStoreError> {
        if let Some(write) = source
            .writes
            .iter()
            .find(|write| is_program_definition_path(&write.path.path))
        {
            return Err(ProgramStoreError::Immutable {
                path: write.path.clone(),
            });
        }
        Ok(())
    }
}

fn prepared_preconditions(
    source: &AtomicWriteBundle,
    alias_bindings: &[ProgramAliasBinding],
) -> Vec<HeadPrecondition> {
    let mut preconditions = source.head_preconditions.clone();
    preconditions.extend(alias_bindings.iter().filter_map(|binding| {
        binding
            .descriptor_version
            .as_ref()
            .map(|version| HeadPrecondition {
                path: binding.requested_path.clone(),
                expected: ObservedHead::Version {
                    version: version.id.0.to_string(),
                },
            })
    }));
    preconditions.sort_by(|left, right| left.path.cmp(&right.path));
    preconditions
}

#[cfg(test)]
mod tests;
