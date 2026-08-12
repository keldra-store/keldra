use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anvil_atomic_program::{
    AtomicProgramEngine, AtomicWriteBundle, CommandReceipt, EngineError, ExecutionLease,
    ExpandedProgramPath, HeadPrecondition, InvocationContext, ObjectPath, ObservedHead,
    ProgramDefinition, ProgramInvocation, ProgramSnapshot, StateReader, StoredValue,
    VersionedDocument,
};
use rocksdb::{WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::key::BucketIdentity;
use crate::store::{
    CF_HEADS, CF_METADATA, CF_VERSIONS, LocalReferenceEffects, PendingBlobReferences,
    PendingLocalChange, VERSION_HIGH_WATERMARK_KEY, is_program_definition_path, now_unix_millis,
    version_blob_reference, version_key,
};
use crate::{
    AccountingHeadTransition, BlobRef, Head, MutationError, ObjectKey, ObjectVersioning,
    ReferenceDelta, Store, Version, VersionId,
};

fn live_version_length(version: &Version) -> Option<u64> {
    (!version.deleted)
        .then(|| version.blob.as_ref().map(|blob| blob.length))
        .flatten()
}

mod distributed;

pub use distributed::{
    CoordinatedProgramPathFinalization, ProgramPathMutation, ProgramPathStage,
    ReplicaProgramPathApplied, path_stage_from_prepared,
};

const PREPARED_BUNDLE_FORMAT: u16 = 4;
const DURABILITY_EVIDENCE_FORMAT: u16 = 1;
const APPLIED_PROGRAM_COMMIT_KEY: &[u8] = b"applied_program_commit";
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
        Self(tagged_hash(b"anvil.durability-class.v1", class.as_bytes()))
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
            b"anvil.program-durability-evidence.v1",
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
    store: Store,
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
        let dependencies = self.inner.expanded_paths(context, invocation)?;
        self.validate_dependency_policies(&dependencies)?;
        Ok(ProgramExecutionLease {
            program_hash: self.program_hash,
            inner: self.inner.prepare(context, invocation).await?,
            _policy_guard: policy_guard,
        })
    }

    fn validate_dependency_policies(
        &self,
        dependencies: &[ExpandedProgramPath],
    ) -> Result<(), EngineError> {
        for dependency in dependencies {
            let policy = self
                .store
                .bucket_policy(&dependency.path.tenant, &dependency.path.bucket)
                .map_err(|error| EngineError::Read(error.to_string()))?;
            let path = dependency.path.path.as_str();
            if !policy.is_program_only(path) {
                return Err(EngineError::ProgramConcurrency {
                    path: dependency.path.clone(),
                    reason: "dependency must use PROGRAM_ONLY policy".into(),
                });
            }
        }
        Ok(())
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedProgramCommit {
    pub commit_cursor: u64,
    pub bundle_ref: PreparedBundleRef,
    pub bundle_hash: PreparedBundleHash,
    pub program_hash: ProgramHash,
    pub durability_class: ProgramDurabilityClassHash,
    pub durability_evidence_hash: ProgramDurabilityEvidenceHash,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommittedProgramResult {
    pub receipt: CommandReceipt,
    pub published_versions: BTreeMap<ObjectPath, PublishedProgramVersion>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgramCommit {
    pub previous_commit_cursor: Option<u64>,
    pub commit_cursor: u64,
    pub bundle_ref: PreparedBundleRef,
    pub bundle_hash: PreparedBundleHash,
    pub program_hash: ProgramHash,
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
    #[error("atomic path {path:?} is not covered by a program-only bucket policy")]
    ProgramPolicy { path: ObjectPath },
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
    #[error("storage error: {0}")]
    Storage(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredPreparedBundle {
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
            store: self.clone(),
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
                                serde_json::from_slice::<Version>(&encoded)
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
    if record.format != PREPARED_BUNDLE_FORMAT {
        return Err(ProgramStoreError::InvalidBundle(
            "unsupported prepared record format".into(),
        ));
    }
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
    let mut write_paths = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    for write in &record.writes {
        if preconditions.get(&write.path) != Some(&write.expected)
            || !write_paths.insert(write.path.clone())
            || !version_ids.insert(write.version.id)
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
            && write.version.content_type.is_some();
        if !valid_tombstone && !valid_live {
            return Err(ProgramStoreError::InvalidBundle(
                "prepared version has an invalid payload or tombstone shape".into(),
            ));
        }
    }
    Ok(())
}

fn validate_observed_head(head: &ObservedHead) -> Result<(), ProgramStoreError> {
    if let ObservedHead::Version { version } = head {
        version.parse::<u64>().map_err(|_| {
            ProgramStoreError::InvalidBundle(format!("invalid store version `{version}`"))
        })?;
    }
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
    ) -> Result<CommittedProgramResult, ProgramStoreError> {
        let result = async {
            let source = lease.bundle();
            let source_encoded = serde_json::to_vec(source).map_err(program_storage_error)?;
            let source_hash = PreparedBundleHash(tagged_hash(
                b"anvil.atomic-source-bundle.v1",
                &source_encoded,
            ));
            if source_hash != prepared.source_bundle_hash {
                return Err(ProgramStoreError::PreparedBundleMismatch);
            }
            verify_prepared_commit(prepared, &commit)?;
            loop {
                let commit_guard = self.commit_lock.lock().await;
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
                    self.apply_prepared_record(&loaded, &commit)
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
            let commit_guard = self.commit_lock.lock().await;
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
                self.apply_prepared_record(&loaded, &commit)
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
        if existing.commit_cursor == requested.commit_cursor
            && existing.bundle_ref == requested.bundle_ref
            && existing.bundle_hash == requested.bundle_hash
            && existing.program_hash == requested.program_hash
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
    ) -> Result<CommittedProgramResult, ProgramStoreError> {
        let record = &loaded.record;
        validate_prepared_record(record)?;
        verify_loaded_commit(loaded, commit)?;

        if commit.commit_cursor == 0
            || commit
                .previous_commit_cursor
                .is_some_and(|previous| previous >= commit.commit_cursor)
        {
            return Err(ProgramStoreError::InvalidBundle(
                "commit cursors must be non-zero and strictly increasing".into(),
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

        let mut versioning_by_path = BTreeMap::new();
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
            if !policy.is_program_only(&write.path.path) {
                return Err(ProgramStoreError::ProgramPolicy {
                    path: write.path.clone(),
                });
            }
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
            versioning_by_path.insert(
                write.path.clone(),
                self.bucket_versioning_by_key(&identity.encode())
                    .map_err(program_mutation_error)?,
            );
        }
        let result = committed_result(record);
        let applied = AppliedProgramCommit {
            commit_cursor: commit.commit_cursor,
            bundle_ref: commit.bundle_ref,
            bundle_hash: commit.bundle_hash,
            program_hash: record.program_hash,
            durability_class: commit.durability_class,
            durability_evidence_hash: commit.durability_evidence_hash,
        };

        let mut batch = WriteBatch::default();
        let mut changes = Vec::with_capacity(record.writes.len());
        let mut pending_blob_references = PendingBlobReferences::new();
        let publication_at_unix_millis = now_unix_millis().map_err(program_mutation_error)?;
        for write in &record.writes {
            let mut reference_deltas = Vec::new();
            let key = object_key(&write.path)?;
            let identity = *identities
                .get(&(write.path.tenant.clone(), write.path.bucket.clone()))
                .ok_or_else(|| {
                    ProgramStoreError::Storage("write has no stable bucket identity".into())
                })?;
            let version_bytes =
                serde_json::to_vec(&write.version).map_err(program_storage_error)?;
            let old_version = current_versions
                .get(&write.path)
                .ok_or_else(|| {
                    ProgramStoreError::Storage("write has no current-version observation".into())
                })?
                .as_ref();
            let accounting_transition = AccountingHeadTransition::new(
                old_version.and_then(live_version_length),
                live_version_length(&write.version),
            );
            let encoded_version_key = version_key(identity, &key, write.version.id);
            let existing = self.raw_get(CF_VERSIONS, &encoded_version_key)?;
            if let Some(existing) = &existing {
                if existing.as_slice() != version_bytes.as_slice() {
                    return Err(ProgramStoreError::CommitCorruption {
                        cursor: commit.commit_cursor,
                    });
                }
            } else {
                let versioning = *versioning_by_path.get(&write.path).ok_or_else(|| {
                    ProgramStoreError::Storage("write has no bucket versioning decision".into())
                })?;
                let old_blob = old_version
                    .map(version_blob_reference)
                    .transpose()
                    .map_err(program_mutation_error)?
                    .flatten();
                let new_blob =
                    version_blob_reference(&write.version).map_err(program_mutation_error)?;
                let references_changed = old_blob.as_ref() != new_blob.as_ref();
                if versioning == ObjectVersioning::Unversioned && references_changed {
                    if let Some(reference) = old_blob.as_ref() {
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
                }
                if let Some(reference) = new_blob.as_ref()
                    && (versioning == ObjectVersioning::Enabled || references_changed)
                {
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
                if versioning == ObjectVersioning::Unversioned
                    && let Some(previous) = old_version
                {
                    batch.delete_cf(
                        self.program_cf(CF_VERSIONS)?,
                        version_key(identity, &key, previous.id),
                    );
                }
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
                    mutation_stamp: None,
                })
                .map_err(program_storage_error)?,
            );
            changes.push(PendingLocalChange::ObjectHead {
                identity,
                exact_path: key.path().to_owned(),
                path_version: write.version.id,
                deleted: write.version.deleted,
                reference_deltas,
                accounting_transition: Some(accounting_transition),
                definition_transition: None,
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
        self.prepare_program_bundle_source(lease.program_hash, source, None)
            .await
    }

    /// Distributed preparation uses the executor's shared exact-path lock
    /// manager and an authoritative cluster reader rather than this node's
    /// local reader. Path authorities validate `PROGRAM_ONLY` again while
    /// staging, before any Raft visibility decision.
    pub async fn prepare_distributed_program_bundle(
        &self,
        program_hash: ProgramHash,
        source: &AtomicWriteBundle,
        previous_versions: &BTreeMap<ObjectPath, Version>,
    ) -> Result<PreparedProgramBundle, ProgramStoreError> {
        self.prepare_program_bundle_source(program_hash, source, Some(previous_versions))
            .await
    }

    async fn prepare_program_bundle_source(
        &self,
        program_hash: ProgramHash,
        source: &AtomicWriteBundle,
        previous_versions: Option<&BTreeMap<ObjectPath, Version>>,
    ) -> Result<PreparedProgramBundle, ProgramStoreError> {
        validate_source_bundle(source)?;
        let source_encoded = serde_json::to_vec(source).map_err(program_storage_error)?;
        let source_bundle_hash = PreparedBundleHash(tagged_hash(
            b"anvil.atomic-source-bundle.v1",
            &source_encoded,
        ));

        let committed_at_unix_millis = now_unix_millis().map_err(program_mutation_error)?;
        let mut writes = Vec::with_capacity(source.writes.len());
        let mut allocated_versions = Vec::with_capacity(source.writes.len());
        for write in &source.writes {
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
                path: write.path.clone(),
                expected: write.expected.clone(),
                previous_version: previous_versions
                    .and_then(|versions| versions.get(&write.path).cloned()),
                version: Version {
                    id: version_id,
                    blob,
                    content_type: write.content_type.clone(),
                    deleted,
                    committed_at_unix_millis,
                },
            };
            writes.push(descriptor);
        }

        let record = StoredPreparedBundle {
            format: PREPARED_BUNDLE_FORMAT,
            source_bundle_hash,
            program_hash,
            preconditions: source.head_preconditions.clone(),
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
            let _commit_guard = self.commit_lock.lock().await;
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
        for path in source.writes.iter().map(|write| &write.path) {
            let policy = self
                .bucket_policy(&path.tenant, &path.bucket)
                .map_err(program_mutation_error)?;
            if !policy.is_program_only(&path.path) {
                return Err(ProgramStoreError::ProgramPolicy { path: path.clone() });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
