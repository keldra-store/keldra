use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anvil_atomic_program::{
    AtomicProgramEngine, AtomicWriteBundle, CommandReceipt, EmissionPayload, EngineError,
    ExecutionLease, ExpandedProgramPath, HeadPrecondition, InvocationContext, ObjectPath,
    ObservedHead, ProgramDefinition, ProgramInvocation, ProgramSnapshot, StateReader, StoredValue,
    VersionedDocument,
};
use rocksdb::{DB, IteratorMode, WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::store::{
    CF_HEADS, CF_METADATA, CF_OUTBOX, CF_PROGRAM_ARTIFACTS, CF_PROGRAM_COMMITS, CF_VERSIONS,
    VERSION_HIGH_WATERMARK_KEY, is_program_definition_path, now_unix_millis, version_key,
};
use crate::{BlobRef, Head, InlinePayload, MutationError, ObjectKey, Store, Version, VersionId};

const PREPARED_BUNDLE_FORMAT: u16 = 2;
const DURABILITY_EVIDENCE_FORMAT: u16 = 1;
const APPLIED_PROGRAM_COMMIT_KEY: &[u8] = b"applied_program_commit";
const COMMIT_CURSOR_PREFIX: &[u8] = b"cursor/";
const COMMIT_BUNDLE_PREFIX: &[u8] = b"bundle/";
const ARTIFACT_PREFIX: u8 = b'a';
const EVIDENCE_PREFIX: u8 = b'e';
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

/// Opaque fixed-size reference to the root prepared-bundle artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PreparedBundleRef(pub [u8; 32]);

/// Content identity of the configured remote durability class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProgramDurabilityClassHash(pub [u8; 32]);

impl ProgramDurabilityClassHash {
    pub fn for_class(class: &str) -> Self {
        Self(tagged_hash(b"anvil.durability-class.v1", class.as_bytes()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparedArtifactKind {
    Payload,
    VersionDescriptor,
    Receipt,
    Bundle,
}

impl PreparedArtifactKind {
    fn tag(self) -> &'static [u8] {
        match self {
            Self::Payload => b"anvil.prepared-payload.v1",
            Self::VersionDescriptor => b"anvil.prepared-version-descriptor.v1",
            Self::Receipt => b"anvil.prepared-receipt.v1",
            Self::Bundle => b"anvil.prepared-bundle.v2",
        }
    }

    fn key_tag(self) -> u8 {
        match self {
            Self::Payload => 1,
            Self::VersionDescriptor => 2,
            Self::Receipt => 3,
            Self::Bundle => 4,
        }
    }
}

/// One immutable, content-addressed preparation artifact.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PreparedArtifactRef {
    pub kind: PreparedArtifactKind,
    pub hash: [u8; 32],
    pub length: u64,
}

impl PreparedArtifactRef {
    fn for_bytes(kind: PreparedArtifactKind, bytes: &[u8]) -> Self {
        let hash = match kind {
            PreparedArtifactKind::Payload => *blake3::hash(bytes).as_bytes(),
            _ => tagged_hash(kind.tag(), bytes),
        };
        Self {
            kind,
            hash,
            length: bytes.len() as u64,
        }
    }

    fn verify(&self, bytes: &[u8]) -> bool {
        self.length == bytes.len() as u64 && self.hash == Self::for_bytes(self.kind, bytes).hash
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifact {
    pub reference: PreparedArtifactRef,
    pub bytes: Vec<u8>,
}

impl PreparedArtifact {
    fn new(kind: PreparedArtifactKind, bytes: Vec<u8>) -> Self {
        Self {
            reference: PreparedArtifactRef::for_bytes(kind, &bytes),
            bytes,
        }
    }
}

/// The complete artifact set passed to the configured durability provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedArtifactBatch {
    pub bundle: PreparedArtifactRef,
    pub manifest_hash: [u8; 32],
    pub artifacts: Vec<PreparedArtifact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum ProgramDurabilityScope {
    /// The built-in repository persisted only on the executor. Consensus must
    /// not treat this as surviving executor loss.
    ExecutorLocal {
        node_id: u16,
        /// Whether the local RocksDB write was requested with WAL sync.
        synced: bool,
    },
    /// An injected provider attests that the complete manifest is recoverable
    /// under its named remote durability class. This crate deliberately does
    /// not define that class's participants or replication rule.
    ConfiguredRemote { class: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgramDurabilityEvidence {
    pub format: u16,
    pub bundle: PreparedArtifactRef,
    pub manifest_hash: [u8; 32],
    pub scope: ProgramDurabilityScope,
    /// Opaque provider receipt. It is content-addressed outside Raft; Raft
    /// carries only [`ProgramDurabilityEvidenceHash`].
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

pub type PreparedArtifactFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Storage and durability boundary for prepared atomic-program artifacts.
///
/// `persist` must not return `ConfiguredRemote` until every artifact in the
/// manifest can be fetched after executor loss under that provider's named
/// durability class. It must also persist the returned evidence so another
/// node can resolve it by hash. The participant/quorum rules are intentionally
/// outside this crate.
pub trait PreparedArtifactRepository: std::fmt::Debug + Send + Sync {
    fn persist(
        &self,
        batch: PreparedArtifactBatch,
    ) -> PreparedArtifactFuture<'_, ProgramDurabilityEvidence>;

    fn load(&self, reference: PreparedArtifactRef) -> PreparedArtifactFuture<'_, Option<Vec<u8>>>;

    fn load_evidence(
        &self,
        hash: ProgramDurabilityEvidenceHash,
    ) -> PreparedArtifactFuture<'_, Option<ProgramDurabilityEvidence>>;
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
                return Err(EngineError::InvalidInvocation(format!(
                    "atomic-program dependency {:?} must use PROGRAM_ONLY policy",
                    dependency.path
                )));
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

/// Compact preparation descriptor. Object bodies, version descriptors, the
/// receipt, the complete bundle, and full durability evidence live in the
/// configured artifact repository rather than Raft.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedProgramBundle {
    pub hash: PreparedBundleHash,
    pub source_bundle_hash: PreparedBundleHash,
    pub program_hash: ProgramHash,
    pub bundle: PreparedArtifactRef,
    pub manifest_hash: [u8; 32],
    pub durability_evidence_hash: ProgramDurabilityEvidenceHash,
    pub durability: ProgramDurabilityEvidence,
}

impl PreparedProgramBundle {
    /// Returns evidence suitable for a cluster-safe `CommitBatch`. The
    /// built-in executor-local repository always fails this check.
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AppliedProgramReceipt {
    pub receipt: CommandReceipt,
    pub bundle_ref: PreparedBundleRef,
    pub bundle_hash: PreparedBundleHash,
    pub program_hash: ProgramHash,
    pub durability_class: ProgramDurabilityClassHash,
    pub durability_evidence_hash: ProgramDurabilityEvidenceHash,
    #[serde(with = "object_path_version_map")]
    pub published_versions: BTreeMap<ObjectPath, VersionId>,
    pub commit_cursor: u64,
}

mod object_path_version_map {
    use std::collections::BTreeMap;

    use anvil_atomic_program::ObjectPath;
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

    use crate::VersionId;

    pub fn serialize<S: Serializer>(
        values: &BTreeMap<ObjectPath, VersionId>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        values.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<ObjectPath, VersionId>, D::Error> {
        let values = Vec::<(ObjectPath, VersionId)>::deserialize(deserializer)?;
        let mut result = BTreeMap::new();
        for (path, version) in values {
            if result.insert(path, version).is_some() {
                return Err(D::Error::custom("duplicate published program path"));
            }
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedProgramCommit {
    pub commit_cursor: u64,
    pub bundle_ref: PreparedBundleRef,
    pub bundle_hash: PreparedBundleHash,
    pub durability_class: ProgramDurabilityClassHash,
    pub durability_evidence_hash: ProgramDurabilityEvidenceHash,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEntry {
    pub effect_id: String,
    pub route_id: String,
    pub payload: OutboxPayload,
    pub content_type: String,
    pub bundle_hash: PreparedBundleHash,
    pub commit_cursor: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum OutboxPayload {
    Inline(InlinePayload),
    Blob(BlobRef),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProgramStoreError {
    #[error("invalid program definition: {0}")]
    InvalidDefinition(String),
    #[error("loaded program bytes do not match the expected immutable object hash")]
    ProgramHashMismatch,
    #[error("content hash collision or corrupt hashed record")]
    HashCollision,
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
    #[error("prepared artifact is missing: {0:?}")]
    ArtifactNotFound(PreparedArtifactRef),
    #[error("prepared artifact failed content verification: {0:?}")]
    ArtifactCorruption(PreparedArtifactRef),
    #[error("durability evidence {0} was not found")]
    DurabilityEvidenceNotFound(ProgramDurabilityEvidenceHash),
    #[error("durability evidence does not bind the committed bundle and manifest")]
    DurabilityEvidenceMismatch,
    #[error("durability evidence class does not match the committed durability class")]
    DurabilityClassMismatch,
    #[error(
        "prepared artifacts are durable only on the executor and cannot back a cluster-safe commit"
    )]
    ExecutorLocalDurability,
    #[error("storage error: {0}")]
    Storage(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StoredPreparedBundle {
    format: u16,
    source_bundle_hash: PreparedBundleHash,
    program_hash: ProgramHash,
    preconditions: Vec<HeadPrecondition>,
    writes: Vec<PreparedArtifactRef>,
    receipt: PreparedArtifactRef,
    emissions: Vec<PreparedEmission>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PreparedVersionWrite {
    path: ObjectPath,
    expected: ObservedHead,
    version: Version,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PreparedEmission {
    effect_id: String,
    route_id: String,
    payload: PreparedArtifactRef,
    content_type: String,
}

#[derive(Debug)]
struct LoadedPreparedBundle {
    bundle: PreparedArtifactRef,
    record: StoredPreparedBundle,
    writes: Vec<PreparedVersionWrite>,
    write_payloads: Vec<Option<Vec<u8>>>,
    receipt: CommandReceipt,
    emission_payloads: Vec<Vec<u8>>,
    evidence: ProgramDurabilityEvidence,
}

#[derive(Debug)]
pub(crate) struct LocalPreparedArtifactRepository {
    db: Arc<DB>,
    node_id: u16,
    sync_writes: bool,
    write_lock: tokio::sync::Mutex<()>,
}

impl LocalPreparedArtifactRepository {
    pub(crate) fn new(db: Arc<DB>, node_id: u16, sync_writes: bool) -> Self {
        Self {
            db,
            node_id,
            sync_writes,
            write_lock: tokio::sync::Mutex::new(()),
        }
    }

    fn cf(&self) -> Result<&rocksdb::ColumnFamily, String> {
        self.db
            .cf_handle(CF_PROGRAM_ARTIFACTS)
            .ok_or_else(|| format!("missing column family {CF_PROGRAM_ARTIFACTS}"))
    }
}

impl PreparedArtifactRepository for LocalPreparedArtifactRepository {
    fn persist(
        &self,
        batch: PreparedArtifactBatch,
    ) -> PreparedArtifactFuture<'_, ProgramDurabilityEvidence> {
        Box::pin(async move {
            validate_artifact_batch(&batch).map_err(|error| error.to_string())?;
            let evidence = ProgramDurabilityEvidence {
                format: DURABILITY_EVIDENCE_FORMAT,
                bundle: batch.bundle.clone(),
                manifest_hash: batch.manifest_hash,
                scope: ProgramDurabilityScope::ExecutorLocal {
                    node_id: self.node_id,
                    synced: self.sync_writes,
                },
                provider_receipt: Vec::new(),
            };
            let evidence_hash = evidence.hash().map_err(|error| error.to_string())?;
            let evidence_bytes =
                serde_json::to_vec(&evidence).map_err(|error| error.to_string())?;

            let _guard = self.write_lock.lock().await;
            let cf = self.cf()?;
            let mut writes = WriteBatch::default();
            for artifact in &batch.artifacts {
                let key = artifact_key(&artifact.reference);
                match self
                    .db
                    .get_cf(cf, &key)
                    .map_err(|error| error.to_string())?
                {
                    Some(existing) if existing.as_slice() != artifact.bytes.as_slice() => {
                        return Err("content-addressed prepared artifact collision".into());
                    }
                    Some(_) => {}
                    None => writes.put_cf(cf, key, &artifact.bytes),
                }
            }

            let evidence_key = evidence_key(evidence_hash);
            match self
                .db
                .get_cf(cf, &evidence_key)
                .map_err(|error| error.to_string())?
            {
                Some(existing) if existing.as_slice() != evidence_bytes.as_slice() => {
                    return Err("content-addressed durability evidence collision".into());
                }
                Some(_) => {}
                None => writes.put_cf(cf, evidence_key, evidence_bytes),
            }

            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db
                .write_opt(writes, &options)
                .map_err(|error| error.to_string())?;
            Ok(evidence)
        })
    }

    fn load(&self, reference: PreparedArtifactRef) -> PreparedArtifactFuture<'_, Option<Vec<u8>>> {
        Box::pin(async move {
            let value = self
                .db
                .get_cf(self.cf()?, artifact_key(&reference))
                .map_err(|error| error.to_string())?
                .map(|value| value.to_vec());
            if value
                .as_deref()
                .is_some_and(|bytes| !reference.verify(bytes))
            {
                return Err("prepared artifact failed content verification".into());
            }
            Ok(value)
        })
    }

    fn load_evidence(
        &self,
        hash: ProgramDurabilityEvidenceHash,
    ) -> PreparedArtifactFuture<'_, Option<ProgramDurabilityEvidence>> {
        Box::pin(async move {
            let Some(encoded) = self
                .db
                .get_cf(self.cf()?, evidence_key(hash))
                .map_err(|error| error.to_string())?
            else {
                return Ok(None);
            };
            let evidence = serde_json::from_slice::<ProgramDurabilityEvidence>(&encoded)
                .map_err(|error| error.to_string())?;
            if evidence.format != DURABILITY_EVIDENCE_FORMAT
                || evidence.hash().map_err(|error| error.to_string())? != hash
            {
                return Err("durability evidence failed content verification".into());
            }
            Ok(Some(evidence))
        })
    }
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anvil_atomic_program::{
        Cardinality, DEFINITION_SCHEMA_VERSION, DocumentAccess, DocumentRef, DocumentSpec,
        DocumentValueRef, DocumentView, EmissionDefinition, ExpectedHead, InputValue, IntegerType,
        InvocationContext, JsonPointerRef, Operation, PathBinding, PathTemplate, PayloadSource,
        ProgramCaps, ProgramInvocation, ReturnDefinition, ValueSource,
    };
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::{BucketPolicy, INLINE_PAYLOAD_MAX_BYTES, Precondition, PutRequest, StoreOptions};

    #[derive(Debug, Default)]
    struct MemoryRemoteRepository {
        artifacts: Mutex<BTreeMap<PreparedArtifactRef, Vec<u8>>>,
        evidence: Mutex<BTreeMap<ProgramDurabilityEvidenceHash, ProgramDurabilityEvidence>>,
    }

    impl PreparedArtifactRepository for MemoryRemoteRepository {
        fn persist(
            &self,
            batch: PreparedArtifactBatch,
        ) -> PreparedArtifactFuture<'_, ProgramDurabilityEvidence> {
            Box::pin(async move {
                validate_artifact_batch(&batch).map_err(|error| error.to_string())?;
                let evidence = ProgramDurabilityEvidence {
                    format: DURABILITY_EVIDENCE_FORMAT,
                    bundle: batch.bundle,
                    manifest_hash: batch.manifest_hash,
                    scope: ProgramDurabilityScope::ConfiguredRemote {
                        class: "test-remote".into(),
                    },
                    provider_receipt: b"opaque-test-attestation".to_vec(),
                };
                let hash = evidence.hash().map_err(|error| error.to_string())?;
                let mut artifacts = self.artifacts.lock().unwrap();
                for artifact in batch.artifacts {
                    if let Some(existing) =
                        artifacts.insert(artifact.reference.clone(), artifact.bytes.clone())
                        && existing != artifact.bytes
                    {
                        return Err("test repository artifact collision".into());
                    }
                }
                drop(artifacts);
                self.evidence.lock().unwrap().insert(hash, evidence.clone());
                Ok(evidence)
            })
        }

        fn load(
            &self,
            reference: PreparedArtifactRef,
        ) -> PreparedArtifactFuture<'_, Option<Vec<u8>>> {
            Box::pin(async move { Ok(self.artifacts.lock().unwrap().get(&reference).cloned()) })
        }

        fn load_evidence(
            &self,
            hash: ProgramDurabilityEvidenceHash,
        ) -> PreparedArtifactFuture<'_, Option<ProgramDurabilityEvidence>> {
            Box::pin(async move { Ok(self.evidence.lock().unwrap().get(&hash).cloned()) })
        }
    }

    fn counter_path() -> ObjectPath {
        ObjectPath::new("tenant", "bucket", "managed/counter").unwrap()
    }

    fn definition() -> ProgramDefinition {
        let counter = DocumentRef::one("counter");
        ProgramDefinition {
            schema_version: DEFINITION_SCHEMA_VERSION,
            documents: vec![DocumentSpec {
                name: "counter".into(),
                path: PathTemplate::new("{tenant}", "bucket", "managed/counter"),
                cardinality: Cardinality::One,
                access: DocumentAccess::ReadWrite,
                allow_initial_json: true,
            }],
            assertions: Vec::new(),
            operations: vec![Operation::CheckedIntegerAdd {
                target: JsonPointerRef::new(counter.clone(), "/value"),
                delta: InputValue::Input {
                    name: "delta".into(),
                },
                numeric_type: IntegerType::I64 {
                    min: Some(0),
                    max: None,
                },
            }],
            returns: vec![ReturnDefinition {
                name: "value".into(),
                value: DocumentValueRef {
                    value: JsonPointerRef::new(counter.clone(), "/value"),
                    view: DocumentView::Current,
                },
            }],
            emissions: vec![EmissionDefinition {
                route_id: "counter.changed".into(),
                payload: PayloadSource::Json {
                    value: ValueSource::Document {
                        source: DocumentValueRef {
                            value: JsonPointerRef::new(counter, ""),
                            view: DocumentView::Current,
                        },
                    },
                },
                content_type: "application/json".into(),
            }],
            caps: ProgramCaps {
                max_paths: 1,
                max_writes: 1,
                max_operations: 2,
                max_emissions: 1,
                max_input_bytes: INLINE_PAYLOAD_MAX_BYTES,
                max_document_bytes: INLINE_PAYLOAD_MAX_BYTES,
                max_emitted_bytes: INLINE_PAYLOAD_MAX_BYTES,
            },
        }
    }

    fn invocation(command: &str, expected_head: ExpectedHead) -> ProgramInvocation {
        ProgramInvocation {
            program_path_hash: [0x11; 32],
            command_id: command.into(),
            input_fingerprint: hex::encode(blake3::hash(command.as_bytes()).as_bytes()),
            arguments: Default::default(),
            inputs: [("delta".into(), json!(1))].into_iter().collect(),
            blobs: Default::default(),
            bindings: [(
                "counter".into(),
                vec![PathBinding {
                    path: counter_path(),
                    template_values: Default::default(),
                    expected_head,
                    initial_json: Some(json!({"value": 0})),
                }],
            )]
            .into_iter()
            .collect(),
        }
    }

    fn verified_definition() -> VerifiedProgramDefinition {
        let bytes = serde_json::to_vec(&definition()).unwrap();
        VerifiedProgramDefinition::from_bytes(&bytes, ProgramHash::for_definition_bytes(&bytes))
            .unwrap()
    }

    async fn configured_store() -> (TempDir, Store, VerifiedProgramDefinition) {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap()
            .with_prepared_artifact_repository(Arc::new(MemoryRemoteRepository::default()));
        configure_policy(&store).await;
        (temporary, store, verified_definition())
    }

    async fn configure_policy(store: &Store) {
        store
            .set_bucket_policy(
                "tenant",
                "bucket",
                BucketPolicy {
                    program_only_prefixes: vec!["managed".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    fn reserved_program_path_attempt(
        command_id: &str,
        expected_head: ExpectedHead,
        operation: Operation,
    ) -> (VerifiedProgramDefinition, ProgramInvocation) {
        let target = ObjectPath::new("tenant", "bucket", "_anvil/programs/victim@1").unwrap();
        let mut definition = definition();
        definition.documents[0].path =
            PathTemplate::new("{tenant}", "bucket", "_anvil/programs/victim@1");
        definition.documents[0].allow_initial_json = false;
        definition.operations = vec![operation];
        definition.returns.clear();
        definition.emissions.clear();
        let bytes = serde_json::to_vec(&definition).unwrap();
        let verified = VerifiedProgramDefinition::from_bytes(
            &bytes,
            ProgramHash::for_definition_bytes(&bytes),
        )
        .unwrap();

        let mut invocation = invocation(command_id, expected_head);
        let binding = &mut invocation.bindings.get_mut("counter").unwrap()[0];
        binding.path = target;
        binding.initial_json = None;
        (verified, invocation)
    }

    async fn snapshot(store: &Store) -> ProgramSnapshot {
        StateReader::read_snapshot(store, &[counter_path()])
            .await
            .unwrap()
    }

    fn commit(
        prepared: &PreparedProgramBundle,
        previous_commit_cursor: Option<u64>,
        commit_cursor: u64,
    ) -> ProgramCommit {
        let ProgramDurabilityScope::ConfiguredRemote { class } = &prepared.durability.scope else {
            panic!("test commit requires configured remote durability evidence");
        };
        ProgramCommit {
            previous_commit_cursor,
            commit_cursor,
            bundle_ref: PreparedBundleRef(prepared.bundle.hash),
            bundle_hash: prepared.hash,
            program_hash: prepared.program_hash,
            durability_class: ProgramDurabilityClassHash::for_class(class),
            durability_evidence_hash: prepared.durability_evidence_hash,
        }
    }

    #[tokio::test]
    async fn built_in_repository_only_attests_executor_local_durability() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        configure_policy(&store).await;
        let engine = store.program_engine(&verified_definition()).unwrap();
        let lease = engine
            .prepare(
                &InvocationContext::new("tenant").unwrap(),
                &invocation("local", ExpectedHead::Absent),
            )
            .await
            .unwrap();
        let prepared = store.prepare_program_bundle(&lease).await.unwrap();

        assert!(matches!(
            &prepared.durability.scope,
            ProgramDurabilityScope::ExecutorLocal {
                node_id: 1,
                synced: true
            }
        ));
        assert_eq!(
            prepared.remote_durability_evidence_hash().unwrap_err(),
            ProgramStoreError::ExecutorLocalDurability
        );

        let wrong_class = ProgramCommit {
            previous_commit_cursor: None,
            commit_cursor: 1,
            bundle_ref: PreparedBundleRef(prepared.bundle.hash),
            bundle_hash: prepared.hash,
            program_hash: prepared.program_hash,
            durability_class: ProgramDurabilityClassHash::for_class("replicated"),
            durability_evidence_hash: prepared.durability_evidence_hash,
        };
        assert_eq!(
            verify_prepared_commit(&prepared, &wrong_class).unwrap_err(),
            ProgramStoreError::DurabilityClassMismatch
        );

        let local_commit = ProgramCommit {
            durability_class: ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS),
            ..wrong_class
        };
        let applied = store
            .apply_program_bundle(lease, &prepared, local_commit)
            .await
            .unwrap();
        assert_eq!(store.applied_program_receipt(1).unwrap(), Some(applied));
    }

    #[test]
    fn unsynced_executor_local_evidence_cannot_satisfy_local_commit() {
        assert_eq!(
            verify_commit_durability(
                &ProgramDurabilityScope::ExecutorLocal {
                    node_id: 1,
                    synced: false,
                },
                ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS),
            )
            .unwrap_err(),
            ProgramStoreError::ExecutorLocalDurability
        );
    }

    #[tokio::test]
    async fn apply_is_all_old_or_all_new_and_records_result_by_commit_cursor() {
        let (_temporary, store, verified) = configured_store().await;
        let engine = store.program_engine(&verified).unwrap();
        let context = InvocationContext::new("tenant").unwrap();
        let first_invocation = invocation("command-1", ExpectedHead::Absent);
        let lease = engine.prepare(&context, &first_invocation).await.unwrap();
        let prepared = store.prepare_program_bundle(&lease).await.unwrap();

        let before = snapshot(&store).await;
        assert!(before.documents.is_empty());
        let first_commit = commit(&prepared, None, 1);
        let first = store
            .apply_program_bundle(lease, &prepared, first_commit)
            .await
            .unwrap();
        let after = snapshot(&store).await;
        assert_eq!(
            after.documents[&counter_path()].value,
            Some(StoredValue::Json(json!({"value": 1})))
        );
        assert_eq!(
            store.applied_program_receipt(1).unwrap(),
            Some(first.clone())
        );
        assert!(matches!(
            store.pending_outbox(10).unwrap()[0].payload,
            OutboxPayload::Blob(_)
        ));

        let current_version = first.published_versions[&counter_path()];
        let second_invocation = invocation(
            "command-2",
            ExpectedHead::Version {
                version: current_version.0.to_string(),
            },
        );
        let second_lease = engine.prepare(&context, &second_invocation).await.unwrap();
        let second_prepared = store.prepare_program_bundle(&second_lease).await.unwrap();
        let second_commit = commit(&second_prepared, Some(1), 2);
        let second = store
            .apply_program_bundle(second_lease, &second_prepared, second_commit)
            .await
            .unwrap();
        assert!(second.published_versions[&counter_path()] > current_version);
        assert_eq!(
            snapshot(&store).await.documents[&counter_path()].value,
            Some(StoredValue::Json(json!({"value": 2})))
        );
        assert_eq!(store.applied_program_commit_cursor().unwrap(), Some(2));
    }

    #[tokio::test]
    async fn atomic_program_cannot_replace_or_delete_a_program_definition() {
        let (_temporary, store, _) = configured_store().await;
        let target = ObjectPath::new("tenant", "bucket", "_anvil/programs/victim@1").unwrap();
        let existing = store
            .put(PutRequest {
                key: object_key(&target).unwrap(),
                bytes: serde_json::to_vec(&json!({"value": 1})).unwrap(),
                content_type: Some("application/json".into()),
                precondition: Precondition::Absent,
                command_id: Some("install-victim".into()),
                durability_class: "test-default".into(),
            })
            .await
            .unwrap();
        store
            .set_bucket_policy(
                "tenant",
                "bucket",
                BucketPolicy {
                    program_only_prefixes: vec!["_anvil/programs".into(), "managed".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let attempts = [
            reserved_program_path_attempt(
                "replace-definition",
                ExpectedHead::Version {
                    version: existing.version.0.to_string(),
                },
                Operation::SetValue {
                    target: JsonPointerRef::new(DocumentRef::one("counter"), ""),
                    value: ValueSource::Literal {
                        value: json!({"value": 2}),
                    },
                },
            ),
            reserved_program_path_attempt(
                "delete-definition",
                ExpectedHead::Version {
                    version: existing.version.0.to_string(),
                },
                Operation::RemoveValue {
                    target: JsonPointerRef::new(DocumentRef::one("counter"), ""),
                },
            ),
        ];

        for (program, invocation) in attempts {
            let engine = store.program_engine(&program).unwrap();
            let lease = engine
                .prepare(&InvocationContext::new("tenant").unwrap(), &invocation)
                .await
                .unwrap();
            assert_eq!(
                store.prepare_program_bundle(&lease).await.unwrap_err(),
                ProgramStoreError::Immutable {
                    path: target.clone(),
                }
            );
        }
        assert_eq!(
            store
                .head(&object_key(&target).unwrap())
                .unwrap()
                .unwrap()
                .version,
            existing.version
        );
    }

    #[tokio::test]
    async fn exact_head_is_rechecked_before_atomic_apply() {
        let (_temporary, store, verified) = configured_store().await;
        let engine = store.program_engine(&verified).unwrap();
        let context = InvocationContext::new("tenant").unwrap();
        let lease = engine
            .prepare(&context, &invocation("stale", ExpectedHead::Absent))
            .await
            .unwrap();
        let prepared = store.prepare_program_bundle(&lease).await.unwrap();

        let rogue_id = store.clock.next().unwrap();
        let rogue = Version {
            id: rogue_id,
            blob: None,
            inline: Some(InlinePayload::new(
                serde_json::to_vec(&json!({"value": 99})).unwrap(),
            )),
            content_type: Some("application/json".into()),
            deleted: false,
            committed_at_unix_millis: now_unix_millis().unwrap(),
        };
        let key = object_key(&counter_path()).unwrap();
        let mut batch = WriteBatch::default();
        batch.put_cf(
            store.program_cf(CF_VERSIONS).unwrap(),
            version_key(&key, rogue_id),
            serde_json::to_vec(&rogue).unwrap(),
        );
        batch.put_cf(
            store.program_cf(CF_HEADS).unwrap(),
            key.encode(),
            serde_json::to_vec(&Head {
                version: rogue_id,
                deleted: false,
            })
            .unwrap(),
        );
        store.write_program_batch(batch).unwrap();

        let stale_commit = commit(&prepared, None, 1);
        assert_eq!(
            store
                .apply_program_bundle(lease, &prepared, stale_commit)
                .await
                .unwrap_err(),
            ProgramStoreError::PreconditionFailed {
                path: counter_path(),
                current: Some(rogue_id),
            }
        );
        assert!(store.applied_program_receipt(1).unwrap().is_none());
    }

    #[tokio::test]
    async fn prepared_artifacts_survive_reopen_for_recovery() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = Arc::new(MemoryRemoteRepository::default());
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap()
            .with_prepared_artifact_repository(repository.clone());
        configure_policy(&store).await;
        let verified = verified_definition();
        let engine = store.program_engine(&verified).unwrap();
        let context = InvocationContext::new("tenant").unwrap();
        let lease = engine
            .prepare(&context, &invocation("recover", ExpectedHead::Absent))
            .await
            .unwrap();
        let prepared = store.prepare_program_bundle(&lease).await.unwrap();
        drop(lease.release());
        drop(engine);
        drop(store);

        let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap()
            .with_prepared_artifact_repository(repository);
        assert_eq!(
            reopened
                .prepared_program_bundle(prepared.hash, prepared.durability_evidence_hash)
                .await
                .unwrap(),
            Some(prepared.clone())
        );
        let applied = reopened
            .recover_program_bundle(commit(&prepared, None, 1))
            .await
            .unwrap();
        assert_eq!(applied.receipt.command_id, "recover");
        assert_eq!(
            reopened.applied_program_receipt(1).unwrap(),
            Some(applied.clone())
        );
        assert_eq!(reopened.pending_outbox(10).unwrap().len(), 1);
        assert_eq!(reopened.applied_program_commit_cursor().unwrap(), Some(1));
    }

    #[tokio::test]
    async fn recovery_rejects_committed_bundle_reference_and_durability_class_mismatches() {
        let (_temporary, store, program) = configured_store().await;
        let engine = store.program_engine(&program).unwrap();
        let lease = engine
            .prepare(
                &InvocationContext::new("tenant").unwrap(),
                &invocation("identity-mismatch", ExpectedHead::Absent),
            )
            .await
            .unwrap();
        let prepared = store.prepare_program_bundle(&lease).await.unwrap();
        drop(lease.release());

        let mut wrong_reference = commit(&prepared, None, 1);
        wrong_reference.bundle_ref = PreparedBundleRef([0x41; 32]);
        assert_eq!(
            store
                .recover_program_bundle(wrong_reference)
                .await
                .unwrap_err(),
            ProgramStoreError::PreparedBundleMismatch
        );

        let mut wrong_class = commit(&prepared, None, 1);
        wrong_class.durability_class = ProgramDurabilityClassHash::for_class("other-remote");
        assert_eq!(
            store.recover_program_bundle(wrong_class).await.unwrap_err(),
            ProgramStoreError::DurabilityClassMismatch
        );
        assert_eq!(store.applied_program_commit_cursor().unwrap(), None);
        assert!(snapshot(&store).await.documents.is_empty());
    }

    #[tokio::test]
    async fn finalization_rejects_a_committed_durability_class_mismatch_before_publication() {
        let (_temporary, store, program) = configured_store().await;
        let engine = store.program_engine(&program).unwrap();
        let lease = engine
            .prepare(
                &InvocationContext::new("tenant").unwrap(),
                &invocation("class-mismatch", ExpectedHead::Absent),
            )
            .await
            .unwrap();
        let prepared = store.prepare_program_bundle(&lease).await.unwrap();
        let mut wrong_class = commit(&prepared, None, 1);
        wrong_class.durability_class = ProgramDurabilityClassHash::for_class("other-remote");

        assert_eq!(
            store
                .apply_program_bundle(lease, &prepared, wrong_class)
                .await
                .unwrap_err(),
            ProgramStoreError::DurabilityClassMismatch
        );
        assert_eq!(store.applied_program_commit_cursor().unwrap(), None);
        assert!(snapshot(&store).await.documents.is_empty());
    }

    #[tokio::test]
    async fn finalization_is_idempotent_and_rejects_cursor_corruption() {
        let (_temporary, store, program) = configured_store().await;
        let engine = store.program_engine(&program).unwrap();
        let lease = engine
            .prepare(
                &InvocationContext::new("tenant").unwrap(),
                &invocation("idempotent", ExpectedHead::Absent),
            )
            .await
            .unwrap();
        let prepared = store.prepare_program_bundle(&lease).await.unwrap();
        let first_commit = commit(&prepared, None, 10);
        let first = store
            .apply_program_bundle(lease, &prepared, first_commit)
            .await
            .unwrap();
        let replay = store
            .recover_program_bundle(commit(&prepared, None, 10))
            .await
            .unwrap();
        assert_eq!(replay, first);

        let mut corrupt = commit(&prepared, None, 10);
        corrupt.program_hash = ProgramHash([9; 32]);
        assert_eq!(
            store.recover_program_bundle(corrupt).await.unwrap_err(),
            ProgramStoreError::CommitCorruption { cursor: 10 }
        );

        let mut corrupt = commit(&prepared, None, 10);
        corrupt.bundle_ref = PreparedBundleRef([8; 32]);
        assert_eq!(
            store.recover_program_bundle(corrupt).await.unwrap_err(),
            ProgramStoreError::CommitCorruption { cursor: 10 }
        );

        let mut corrupt = commit(&prepared, None, 10);
        corrupt.durability_class = ProgramDurabilityClassHash([7; 32]);
        assert_eq!(
            store.recover_program_bundle(corrupt).await.unwrap_err(),
            ProgramStoreError::CommitCorruption { cursor: 10 }
        );
        assert_eq!(store.applied_program_commit_cursor().unwrap(), Some(10));
    }

    #[tokio::test]
    async fn predecessor_cursor_prevents_out_of_order_publication() {
        let (_temporary, store, program) = configured_store().await;
        let engine = store.program_engine(&program).unwrap();
        let lease = engine
            .prepare(
                &InvocationContext::new("tenant").unwrap(),
                &invocation("future", ExpectedHead::Absent),
            )
            .await
            .unwrap();
        let prepared = store.prepare_program_bundle(&lease).await.unwrap();
        drop(lease.release());

        assert_eq!(
            store
                .recover_program_bundle(commit(&prepared, Some(20), 30))
                .await
                .unwrap_err(),
            ProgramStoreError::OutOfOrderCommit {
                applied: None,
                expected: Some(20),
                requested: 30,
            }
        );
        assert!(snapshot(&store).await.documents.is_empty());
        assert_eq!(store.applied_program_commit_cursor().unwrap(), None);
    }

    #[test]
    fn program_definition_must_match_loaded_immutable_bytes() {
        let bytes = serde_json::to_vec(&definition()).unwrap();
        assert_eq!(
            VerifiedProgramDefinition::from_bytes(&bytes, ProgramHash([7; 32])).unwrap_err(),
            ProgramStoreError::ProgramHashMismatch
        );
    }

    #[tokio::test]
    async fn injected_repository_can_attest_remote_recoverability() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = Arc::new(MemoryRemoteRepository::default());
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap()
            .with_prepared_artifact_repository(repository);
        configure_policy(&store).await;
        let program = verified_definition();
        let engine = store.program_engine(&program).unwrap();
        let lease = engine
            .prepare(
                &InvocationContext::new("tenant").unwrap(),
                &invocation("remote", ExpectedHead::Absent),
            )
            .await
            .unwrap();
        let prepared = store.prepare_program_bundle(&lease).await.unwrap();

        assert!(prepared.durability.is_remote_recoverable());
        assert_eq!(
            prepared.remote_durability_evidence_hash().unwrap(),
            prepared.durability_evidence_hash
        );
        assert!(matches!(
            prepared.durability.scope,
            ProgramDurabilityScope::ConfiguredRemote { ref class }
                if class == "test-remote"
        ));
    }

    #[tokio::test]
    async fn mutable_read_only_program_dependency_is_rejected_before_execution() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap()
            .with_prepared_artifact_repository(Arc::new(MemoryRemoteRepository::default()));
        let config_path = ObjectPath::new("tenant", "bucket", "configuration/current").unwrap();
        let config = store
            .put(PutRequest {
                key: object_key(&config_path).unwrap(),
                bytes: serde_json::to_vec(&json!({"enabled": true})).unwrap(),
                content_type: Some("application/json".into()),
                precondition: Precondition::Absent,
                command_id: Some("install-mutable-configuration".into()),
                durability_class: "test-default".into(),
            })
            .await
            .unwrap();
        configure_policy(&store).await;

        let mut definition = definition();
        definition.documents.push(DocumentSpec {
            name: "configuration".into(),
            path: PathTemplate::new("{tenant}", "bucket", "configuration/current"),
            cardinality: Cardinality::One,
            access: DocumentAccess::ReadOnly,
            allow_initial_json: false,
        });
        definition.caps.max_paths = 3;
        let bytes = serde_json::to_vec(&definition).unwrap();
        let verified = VerifiedProgramDefinition::from_bytes(
            &bytes,
            ProgramHash::for_definition_bytes(&bytes),
        )
        .unwrap();

        let mut invocation = invocation("read-mutable-configuration", ExpectedHead::Absent);
        invocation.bindings.insert(
            "configuration".into(),
            vec![PathBinding {
                path: config_path.clone(),
                template_values: Default::default(),
                expected_head: ExpectedHead::Version {
                    version: config.version.0.to_string(),
                },
                initial_json: None,
            }],
        );

        let error = store
            .program_engine(&verified)
            .unwrap()
            .prepare(&InvocationContext::new("tenant").unwrap(), &invocation)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            EngineError::InvalidInvocation(message)
                if message.contains("configuration/current") && message.contains("PROGRAM_ONLY")
        ));
        assert_eq!(
            store.head(&object_key(&counter_path()).unwrap()).unwrap(),
            None
        );
        assert_eq!(
            store.head(&object_key(&config_path).unwrap()).unwrap(),
            Some(Head {
                version: config.version,
                deleted: false,
            })
        );
    }

    #[tokio::test]
    async fn immutable_read_only_program_dependency_still_requires_program_only_policy() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap()
            .with_prepared_artifact_repository(Arc::new(MemoryRemoteRepository::default()));
        let config_path = ObjectPath::new("tenant", "bucket", "configuration/current").unwrap();
        let config = store
            .put(PutRequest {
                key: object_key(&config_path).unwrap(),
                bytes: serde_json::to_vec(&json!({"enabled": true})).unwrap(),
                content_type: Some("application/json".into()),
                precondition: Precondition::Absent,
                command_id: Some("install-configuration".into()),
                durability_class: "test-default".into(),
            })
            .await
            .unwrap();
        store
            .set_bucket_policy(
                "tenant",
                "bucket",
                BucketPolicy {
                    create_once_prefixes: vec!["configuration".into()],
                    program_only_prefixes: vec!["managed".into()],
                },
            )
            .await
            .unwrap();

        let mut definition = definition();
        definition.documents.push(DocumentSpec {
            name: "configuration".into(),
            path: PathTemplate::new("{tenant}", "bucket", "configuration/current"),
            cardinality: Cardinality::One,
            access: DocumentAccess::ReadOnly,
            allow_initial_json: false,
        });
        definition.caps.max_paths = 3;
        let bytes = serde_json::to_vec(&definition).unwrap();
        let verified = VerifiedProgramDefinition::from_bytes(
            &bytes,
            ProgramHash::for_definition_bytes(&bytes),
        )
        .unwrap();

        let mut invocation = invocation("read-configuration", ExpectedHead::Absent);
        invocation.bindings.insert(
            "configuration".into(),
            vec![PathBinding {
                path: config_path.clone(),
                template_values: Default::default(),
                expected_head: ExpectedHead::Version {
                    version: config.version.0.to_string(),
                },
                initial_json: None,
            }],
        );

        let error = store
            .program_engine(&verified)
            .unwrap()
            .prepare(&InvocationContext::new("tenant").unwrap(), &invocation)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            EngineError::InvalidInvocation(message)
                if message.contains("configuration/current") && message.contains("PROGRAM_ONLY")
        ));
        assert_eq!(
            store.head(&object_key(&counter_path()).unwrap()).unwrap(),
            None
        );
        assert_eq!(
            store.head(&object_key(&config_path).unwrap()).unwrap(),
            Some(Head {
                version: config.version,
                deleted: false,
            })
        );
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
            for path in document_paths {
                let key = object_key(path).map_err(|error| error.to_string())?;
                let head = snapshot
                    .get_cf(
                        self.cf(CF_HEADS).map_err(|error| error.to_string())?,
                        key.encode(),
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
                                version_key(&key, head.version),
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
                if version.blob.is_some() || version.inline.is_some() {
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
            let bytes = match (&version.inline, &version.blob) {
                (Some(inline), None) if inline.is_valid() => inline.bytes.clone(),
                (None, Some(blob)) => self
                    .blobs
                    .get(blob)
                    .await
                    .map_err(|error| error.to_string())?,
                _ => return Err("live version has an invalid payload shape".into()),
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
    let mut effects = BTreeSet::new();
    for emission in &source.emissions {
        if !effects.insert(&emission.effect_id) {
            return Err(ProgramStoreError::InvalidBundle(
                "duplicate emission effect id".into(),
            ));
        }
    }
    Ok(())
}

fn validate_prepared_record(record: &StoredPreparedBundle) -> Result<(), ProgramStoreError> {
    if record.format != PREPARED_BUNDLE_FORMAT {
        return Err(ProgramStoreError::InvalidBundle(
            "unsupported prepared record format".into(),
        ));
    }
    let mut preconditions = BTreeSet::new();
    for precondition in &record.preconditions {
        validate_observed_head(&precondition.expected)?;
        if !preconditions.insert(precondition.path.clone()) {
            return Err(ProgramStoreError::InvalidBundle(
                "duplicate prepared precondition".into(),
            ));
        }
    }
    if record
        .writes
        .iter()
        .any(|reference| reference.kind != PreparedArtifactKind::VersionDescriptor)
        || record.writes.iter().collect::<BTreeSet<_>>().len() != record.writes.len()
    {
        return Err(ProgramStoreError::InvalidBundle(
            "prepared writes must be unique version-descriptor references".into(),
        ));
    }
    if record.receipt.kind != PreparedArtifactKind::Receipt {
        return Err(ProgramStoreError::InvalidBundle(
            "prepared receipt reference has the wrong artifact kind".into(),
        ));
    }
    let mut effects = BTreeSet::new();
    if record.emissions.iter().any(|emission| {
        !effects.insert(&emission.effect_id)
            || emission.payload.kind != PreparedArtifactKind::Payload
    }) {
        return Err(ProgramStoreError::InvalidBundle(
            "duplicate prepared effect id".into(),
        ));
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

fn encode_emission(payload: &EmissionPayload) -> Result<Vec<u8>, ProgramStoreError> {
    match payload {
        EmissionPayload::Json(value) => serde_json::to_vec(value).map_err(program_storage_error),
        EmissionPayload::Opaque(value) => Ok(value.clone()),
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

fn artifact_key(reference: &PreparedArtifactRef) -> Vec<u8> {
    let mut key = Vec::with_capacity(34);
    key.push(ARTIFACT_PREFIX);
    key.push(reference.kind.key_tag());
    key.extend_from_slice(&reference.hash);
    key
}

fn evidence_key(hash: ProgramDurabilityEvidenceHash) -> Vec<u8> {
    let mut key = Vec::with_capacity(33);
    key.push(EVIDENCE_PREFIX);
    key.extend_from_slice(&hash.0);
    key
}

fn commit_cursor_key(cursor: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(COMMIT_CURSOR_PREFIX.len() + 8);
    key.extend_from_slice(COMMIT_CURSOR_PREFIX);
    key.extend_from_slice(&cursor.to_be_bytes());
    key
}

fn commit_bundle_key(hash: PreparedBundleHash) -> Vec<u8> {
    let mut key = Vec::with_capacity(COMMIT_BUNDLE_PREFIX.len() + 32);
    key.extend_from_slice(COMMIT_BUNDLE_PREFIX);
    key.extend_from_slice(&hash.0);
    key
}

fn artifact_manifest_hash(
    references: &[PreparedArtifactRef],
) -> Result<[u8; 32], ProgramStoreError> {
    let mut references = references.to_vec();
    references.sort();
    let encoded = serde_json::to_vec(&references).map_err(program_storage_error)?;
    Ok(tagged_hash(
        b"anvil.prepared-artifact-manifest.v1",
        &encoded,
    ))
}

fn validate_artifact_batch(batch: &PreparedArtifactBatch) -> Result<(), ProgramStoreError> {
    if batch.bundle.kind != PreparedArtifactKind::Bundle {
        return Err(ProgramStoreError::InvalidBundle(
            "prepared root is not a bundle artifact".into(),
        ));
    }
    let mut identities = BTreeMap::new();
    let mut references = Vec::with_capacity(batch.artifacts.len());
    for artifact in &batch.artifacts {
        if !artifact.reference.verify(&artifact.bytes) {
            return Err(ProgramStoreError::ArtifactCorruption(
                artifact.reference.clone(),
            ));
        }
        let identity = (artifact.reference.kind, artifact.reference.hash);
        if identities
            .insert(identity, artifact.reference.length)
            .is_some()
        {
            return Err(ProgramStoreError::InvalidBundle(
                "prepared artifact manifest contains a duplicate artifact".into(),
            ));
        }
        references.push(artifact.reference.clone());
    }
    if !references.contains(&batch.bundle) {
        return Err(ProgramStoreError::InvalidBundle(
            "prepared artifact manifest omits its root bundle".into(),
        ));
    }
    if artifact_manifest_hash(&references)? != batch.manifest_hash {
        return Err(ProgramStoreError::InvalidBundle(
            "prepared artifact manifest hash does not match its contents".into(),
        ));
    }
    Ok(())
}

fn insert_artifact(
    artifacts: &mut BTreeMap<PreparedArtifactRef, PreparedArtifact>,
    artifact: PreparedArtifact,
) -> Result<(), ProgramStoreError> {
    if let Some(existing) = artifacts.get(&artifact.reference) {
        if existing.bytes != artifact.bytes {
            return Err(ProgramStoreError::HashCollision);
        }
        return Ok(());
    }
    artifacts.insert(artifact.reference.clone(), artifact);
    Ok(())
}

fn validate_durability_evidence(
    evidence: &ProgramDurabilityEvidence,
) -> Result<(), ProgramStoreError> {
    if evidence.format != DURABILITY_EVIDENCE_FORMAT
        || evidence.bundle.kind != PreparedArtifactKind::Bundle
        || evidence.manifest_hash == [0; 32]
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
    if PreparedBundleRef(loaded.bundle.hash) != commit.bundle_ref
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
    if PreparedBundleRef(prepared.bundle.hash) != commit.bundle_ref
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

fn validate_loaded_record(
    record: &StoredPreparedBundle,
    writes: &[PreparedVersionWrite],
    _receipt: &CommandReceipt,
) -> Result<(), ProgramStoreError> {
    validate_prepared_record(record)?;
    if writes.len() != record.writes.len() {
        return Err(ProgramStoreError::InvalidBundle(
            "prepared bundle did not resolve every version descriptor".into(),
        ));
    }
    let preconditions = record
        .preconditions
        .iter()
        .map(|precondition| (precondition.path.clone(), precondition.expected.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut write_paths = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    for write in writes {
        if preconditions.get(&write.path) != Some(&write.expected)
            || !write_paths.insert(write.path.clone())
            || !version_ids.insert(write.version.id)
        {
            return Err(ProgramStoreError::InvalidBundle(
                "prepared write has no unique matching precondition or version".into(),
            ));
        }
        let valid_tombstone = write.version.deleted
            && write.version.blob.is_none()
            && write.version.inline.is_none()
            && write.version.content_type.is_none();
        let valid_live = !write.version.deleted
            && write.version.blob.is_some()
            && write.version.inline.is_none()
            && write.version.content_type.is_some();
        if !valid_tombstone && !valid_live {
            return Err(ProgramStoreError::InvalidBundle(
                "prepared version has an invalid payload or tombstone shape".into(),
            ));
        }
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
    ProgramStoreError::Storage(error.to_string())
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
    ) -> Result<AppliedProgramReceipt, ProgramStoreError> {
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
            if let Some(existing) = self.applied_program_at(commit.commit_cursor)? {
                return self.match_applied_commit(existing, &commit);
            }
            let loaded = self
                .load_prepared_bundle(commit.bundle_hash, commit.durability_evidence_hash)
                .await?;
            if source_hash != loaded.record.source_bundle_hash
                || prepared.bundle != loaded.bundle
                || prepared.manifest_hash != loaded.evidence.manifest_hash
                || prepared.durability != loaded.evidence
            {
                return Err(ProgramStoreError::PreparedBundleMismatch);
            }
            verify_loaded_commit(&loaded, &commit)?;
            self.materialize_prepared_payloads(&loaded).await?;
            let _commit_guard = self.commit_lock.lock().await;
            self.apply_prepared_record(&loaded, &commit)
        }
        .await;
        drop(lease);
        result
    }

    /// Recovery path for a committed bundle. The repository must resolve both
    /// hashes without relying on executor-local state if the commit claimed a
    /// remote durability class.
    pub async fn recover_program_bundle(
        &self,
        commit: ProgramCommit,
    ) -> Result<AppliedProgramReceipt, ProgramStoreError> {
        let _policy_guard = self.policy_gate.read().await;
        if let Some(existing) = self.applied_program_at(commit.commit_cursor)? {
            return self.match_applied_commit(existing, &commit);
        }
        let loaded = self
            .load_prepared_bundle(commit.bundle_hash, commit.durability_evidence_hash)
            .await?;
        verify_loaded_commit(&loaded, &commit)?;
        self.materialize_prepared_payloads(&loaded).await?;
        let paths = loaded
            .record
            .preconditions
            .iter()
            .map(|precondition| precondition.path.clone())
            .collect::<Vec<_>>();
        let _guard = self.program_locks.acquire(&paths).await;
        let _commit_guard = self.commit_lock.lock().await;
        self.apply_prepared_record(&loaded, &commit)
    }

    pub fn applied_program_receipt(
        &self,
        commit_cursor: u64,
    ) -> Result<Option<AppliedProgramReceipt>, ProgramStoreError> {
        self.applied_program_at(commit_cursor)
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

    pub fn outbox_entry(&self, effect_id: &str) -> Result<Option<OutboxEntry>, ProgramStoreError> {
        self.read_program_json(CF_OUTBOX, effect_id.as_bytes())
    }

    pub fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxEntry>, ProgramStoreError> {
        self.db
            .iterator_cf(self.program_cf(CF_OUTBOX)?, IteratorMode::Start)
            .take(limit)
            .map(|item| {
                let (_, encoded) = item.map_err(program_storage_error)?;
                serde_json::from_slice(&encoded).map_err(program_storage_error)
            })
            .collect()
    }

    pub async fn outbox_payload(&self, entry: &OutboxEntry) -> Result<Vec<u8>, ProgramStoreError> {
        match &entry.payload {
            OutboxPayload::Inline(payload) if payload.is_valid() => Ok(payload.bytes.clone()),
            OutboxPayload::Inline(_) => Err(ProgramStoreError::Storage(
                "inline outbox payload failed hash or length verification".into(),
            )),
            OutboxPayload::Blob(blob) => self.blobs.get(blob).await.map_err(program_storage_error),
        }
    }

    fn applied_program_at(
        &self,
        cursor: u64,
    ) -> Result<Option<AppliedProgramReceipt>, ProgramStoreError> {
        self.read_program_json(CF_PROGRAM_COMMITS, &commit_cursor_key(cursor))
    }

    fn match_applied_commit(
        &self,
        existing: AppliedProgramReceipt,
        requested: &ProgramCommit,
    ) -> Result<AppliedProgramReceipt, ProgramStoreError> {
        if let Some(tip) = self.applied_program_commit()?
            && (existing.commit_cursor > tip.commit_cursor
                || (existing.commit_cursor == tip.commit_cursor
                    && (existing.bundle_ref != tip.bundle_ref
                        || existing.bundle_hash != tip.bundle_hash
                        || existing.durability_class != tip.durability_class
                        || existing.durability_evidence_hash != tip.durability_evidence_hash)))
        {
            return Err(ProgramStoreError::CommitCorruption {
                cursor: requested.commit_cursor,
            });
        }
        if existing.commit_cursor == requested.commit_cursor
            && existing.bundle_ref == requested.bundle_ref
            && existing.bundle_hash == requested.bundle_hash
            && existing.program_hash == requested.program_hash
            && existing.durability_class == requested.durability_class
            && existing.durability_evidence_hash == requested.durability_evidence_hash
        {
            Ok(existing)
        } else {
            Err(ProgramStoreError::CommitCorruption {
                cursor: requested.commit_cursor,
            })
        }
    }

    async fn load_artifact(
        &self,
        reference: &PreparedArtifactRef,
    ) -> Result<Vec<u8>, ProgramStoreError> {
        let bytes = self
            .program_artifacts
            .load(reference.clone())
            .await
            .map_err(program_storage_error)?
            .ok_or_else(|| ProgramStoreError::ArtifactNotFound(reference.clone()))?;
        if !reference.verify(&bytes) {
            return Err(ProgramStoreError::ArtifactCorruption(reference.clone()));
        }
        Ok(bytes)
    }

    async fn load_prepared_bundle(
        &self,
        hash: PreparedBundleHash,
        evidence_hash: ProgramDurabilityEvidenceHash,
    ) -> Result<LoadedPreparedBundle, ProgramStoreError> {
        let evidence = self
            .program_artifacts
            .load_evidence(evidence_hash)
            .await
            .map_err(program_storage_error)?
            .ok_or(ProgramStoreError::DurabilityEvidenceNotFound(evidence_hash))?;
        validate_durability_evidence(&evidence)?;
        if evidence.hash()? != evidence_hash
            || evidence.bundle.kind != PreparedArtifactKind::Bundle
            || evidence.bundle.hash != hash.0
        {
            return Err(ProgramStoreError::DurabilityEvidenceMismatch);
        }

        let bundle_bytes = self.load_artifact(&evidence.bundle).await?;
        let record = serde_json::from_slice::<StoredPreparedBundle>(&bundle_bytes)
            .map_err(program_storage_error)?;
        validate_prepared_record(&record)?;

        let mut references = vec![evidence.bundle.clone()];
        let mut writes = Vec::with_capacity(record.writes.len());
        let mut write_payloads = Vec::with_capacity(record.writes.len());
        for reference in &record.writes {
            let bytes = self.load_artifact(reference).await?;
            let write = serde_json::from_slice::<PreparedVersionWrite>(&bytes)
                .map_err(program_storage_error)?;
            if let Some(blob) = &write.version.blob {
                let payload_ref = PreparedArtifactRef {
                    kind: PreparedArtifactKind::Payload,
                    hash: blob.hash,
                    length: blob.length,
                };
                write_payloads.push(Some(self.load_artifact(&payload_ref).await?));
                references.push(payload_ref);
            } else {
                write_payloads.push(None);
            }
            references.push(reference.clone());
            writes.push(write);
        }

        let receipt_bytes = self.load_artifact(&record.receipt).await?;
        let receipt = serde_json::from_slice::<CommandReceipt>(&receipt_bytes)
            .map_err(program_storage_error)?;
        references.push(record.receipt.clone());

        let mut emission_payloads = Vec::with_capacity(record.emissions.len());
        for emission in &record.emissions {
            emission_payloads.push(self.load_artifact(&emission.payload).await?);
            references.push(emission.payload.clone());
        }
        references.sort();
        references.dedup();
        if artifact_manifest_hash(&references)? != evidence.manifest_hash {
            return Err(ProgramStoreError::DurabilityEvidenceMismatch);
        }
        validate_loaded_record(&record, &writes, &receipt)?;

        Ok(LoadedPreparedBundle {
            bundle: evidence.bundle.clone(),
            record,
            writes,
            write_payloads,
            receipt,
            emission_payloads,
            evidence,
        })
    }

    async fn materialize_prepared_payloads(
        &self,
        loaded: &LoadedPreparedBundle,
    ) -> Result<(), ProgramStoreError> {
        for (write, payload) in loaded.writes.iter().zip(&loaded.write_payloads) {
            let (Some(expected), Some(bytes)) = (&write.version.blob, payload) else {
                continue;
            };
            let reference = PreparedArtifactRef {
                kind: PreparedArtifactKind::Payload,
                hash: expected.hash,
                length: expected.length,
            };
            let actual = self.blobs.put(bytes).await.map_err(program_storage_error)?;
            if &actual != expected {
                return Err(ProgramStoreError::ArtifactCorruption(reference));
            }
        }
        for (emission, bytes) in loaded
            .record
            .emissions
            .iter()
            .zip(&loaded.emission_payloads)
        {
            let actual = self.blobs.put(bytes).await.map_err(program_storage_error)?;
            if actual.hash != emission.payload.hash || actual.length != emission.payload.length {
                return Err(ProgramStoreError::ArtifactCorruption(
                    emission.payload.clone(),
                ));
            }
        }
        Ok(())
    }

    fn apply_prepared_record(
        &self,
        loaded: &LoadedPreparedBundle,
        commit: &ProgramCommit,
    ) -> Result<AppliedProgramReceipt, ProgramStoreError> {
        let record = &loaded.record;
        let hash = commit.bundle_hash;
        validate_prepared_record(record)?;
        validate_loaded_record(record, &loaded.writes, &loaded.receipt)?;
        verify_loaded_commit(loaded, commit)?;

        if let Some(existing) = self.applied_program_at(commit.commit_cursor)? {
            return self.match_applied_commit(existing, commit);
        }
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
        if let Some(tip) = &applied_commit {
            let indexed = self.applied_program_at(tip.commit_cursor)?.ok_or(
                ProgramStoreError::CommitCorruption {
                    cursor: tip.commit_cursor,
                },
            )?;
            if indexed.bundle_ref != tip.bundle_ref
                || indexed.bundle_hash != tip.bundle_hash
                || indexed.durability_class != tip.durability_class
                || indexed.durability_evidence_hash != tip.durability_evidence_hash
            {
                return Err(ProgramStoreError::CommitCorruption {
                    cursor: tip.commit_cursor,
                });
            }
        }
        let local_predecessor = applied_commit.as_ref().map(|applied| applied.commit_cursor);
        if local_predecessor != commit.previous_commit_cursor {
            return Err(ProgramStoreError::OutOfOrderCommit {
                applied: local_predecessor,
                expected: commit.previous_commit_cursor,
                requested: commit.commit_cursor,
            });
        }
        if let Some(existing_cursor) =
            self.read_program_json::<u64>(CF_PROGRAM_COMMITS, &commit_bundle_key(hash))?
            && existing_cursor != commit.commit_cursor
        {
            return Err(ProgramStoreError::CommitCorruption {
                cursor: commit.commit_cursor,
            });
        }

        if commit.program_hash != record.program_hash {
            return Err(ProgramStoreError::PreparedBundleMismatch);
        }

        let mut current_heads = BTreeMap::new();
        for precondition in &record.preconditions {
            let key = object_key(&precondition.path)?;
            let current = self.head(&key).map_err(program_mutation_error)?;
            if !head_matches(&precondition.expected, current.as_ref())? {
                return Err(ProgramStoreError::PreconditionFailed {
                    path: precondition.path.clone(),
                    current: current.map(|head| head.version),
                });
            }
            current_heads.insert(precondition.path.clone(), current);
        }

        for write in &loaded.writes {
            let policy = self
                .bucket_policy(&write.path.tenant, &write.path.bucket)
                .map_err(program_mutation_error)?;
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
            if policy.is_create_once(&write.path.path)
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
        for (emission, payload) in record.emissions.iter().zip(&loaded.emission_payloads) {
            if let Some(existing) = self.outbox_entry(&emission.effect_id)? {
                let expected = OutboxEntry {
                    effect_id: emission.effect_id.clone(),
                    route_id: emission.route_id.clone(),
                    payload: OutboxPayload::Blob(BlobRef {
                        hash: emission.payload.hash,
                        length: emission.payload.length,
                    }),
                    content_type: emission.content_type.clone(),
                    bundle_hash: hash,
                    commit_cursor: commit.commit_cursor,
                };
                if existing != expected {
                    return Err(ProgramStoreError::CommitCorruption {
                        cursor: commit.commit_cursor,
                    });
                }
            }
            if payload.len() as u64 != emission.payload.length {
                return Err(ProgramStoreError::ArtifactCorruption(
                    emission.payload.clone(),
                ));
            }
        }

        let published_versions = loaded
            .writes
            .iter()
            .map(|write| (write.path.clone(), write.version.id))
            .collect::<BTreeMap<_, _>>();
        let applied = AppliedProgramReceipt {
            receipt: loaded.receipt.clone(),
            bundle_ref: commit.bundle_ref,
            bundle_hash: hash,
            program_hash: record.program_hash,
            durability_class: commit.durability_class,
            durability_evidence_hash: commit.durability_evidence_hash,
            published_versions,
            commit_cursor: commit.commit_cursor,
        };

        let mut batch = WriteBatch::default();
        for write in &loaded.writes {
            let key = object_key(&write.path)?;
            let version_bytes =
                serde_json::to_vec(&write.version).map_err(program_storage_error)?;
            let version_key = version_key(&key, write.version.id);
            if let Some(existing) = self.raw_get(CF_VERSIONS, &version_key)?
                && existing.as_slice() != version_bytes.as_slice()
            {
                return Err(ProgramStoreError::CommitCorruption {
                    cursor: commit.commit_cursor,
                });
            }
            batch.put_cf(self.program_cf(CF_VERSIONS)?, version_key, version_bytes);
            batch.put_cf(
                self.program_cf(CF_HEADS)?,
                key.encode(),
                serde_json::to_vec(&Head {
                    version: write.version.id,
                    deleted: write.version.deleted,
                })
                .map_err(program_storage_error)?,
            );
        }
        for emission in &record.emissions {
            let entry = OutboxEntry {
                effect_id: emission.effect_id.clone(),
                route_id: emission.route_id.clone(),
                payload: OutboxPayload::Blob(BlobRef {
                    hash: emission.payload.hash,
                    length: emission.payload.length,
                }),
                content_type: emission.content_type.clone(),
                bundle_hash: hash,
                commit_cursor: commit.commit_cursor,
            };
            batch.put_cf(
                self.program_cf(CF_OUTBOX)?,
                emission.effect_id.as_bytes(),
                serde_json::to_vec(&entry).map_err(program_storage_error)?,
            );
        }
        let allocated_high = loaded.writes.iter().map(|write| write.version.id).max();
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
            self.program_cf(CF_PROGRAM_COMMITS)?,
            commit_cursor_key(commit.commit_cursor),
            serde_json::to_vec(&applied).map_err(program_storage_error)?,
        );
        batch.put_cf(
            self.program_cf(CF_PROGRAM_COMMITS)?,
            commit_bundle_key(hash),
            serde_json::to_vec(&commit.commit_cursor).map_err(program_storage_error)?,
        );
        batch.put_cf(
            self.program_cf(CF_METADATA)?,
            APPLIED_PROGRAM_COMMIT_KEY,
            serde_json::to_vec(&AppliedProgramCommit {
                commit_cursor: commit.commit_cursor,
                bundle_ref: commit.bundle_ref,
                bundle_hash: hash,
                durability_class: commit.durability_class,
                durability_evidence_hash: commit.durability_evidence_hash,
            })
            .map_err(program_storage_error)?,
        );
        self.write_program_batch(batch)?;
        if let Some(allocated) = allocated_high {
            self.clock.observe(allocated);
        }
        Ok(applied)
    }
}

impl Store {
    /// Materialises interpreter output into immutable content-addressed
    /// payload, version-descriptor, receipt, and root-bundle artifacts. The
    /// returned evidence says whether that artifact set is merely
    /// executor-local or remotely recoverable under an injected provider.
    pub async fn prepare_program_bundle(
        &self,
        lease: &ProgramExecutionLease,
    ) -> Result<PreparedProgramBundle, ProgramStoreError> {
        let source = lease.bundle();
        validate_source_bundle(source)?;
        self.validate_program_policies(source)?;
        let source_encoded = serde_json::to_vec(source).map_err(program_storage_error)?;
        let source_bundle_hash = PreparedBundleHash(tagged_hash(
            b"anvil.atomic-source-bundle.v1",
            &source_encoded,
        ));

        let committed_at_unix_millis = now_unix_millis().map_err(program_mutation_error)?;
        let mut artifacts = BTreeMap::<PreparedArtifactRef, PreparedArtifact>::new();
        let mut writes = Vec::with_capacity(source.writes.len());
        let mut allocated_versions = Vec::with_capacity(source.writes.len());
        for write in &source.writes {
            let (blob, deleted) = match &write.value {
                Some(value) => {
                    let bytes = encode_stored_value(value)?;
                    let artifact = PreparedArtifact::new(PreparedArtifactKind::Payload, bytes);
                    let blob = BlobRef {
                        hash: artifact.reference.hash,
                        length: artifact.reference.length,
                    };
                    insert_artifact(&mut artifacts, artifact)?;
                    (Some(blob), false)
                }
                None => (None, true),
            };
            let version_id = self.clock.next().map_err(program_storage_error)?;
            allocated_versions.push(version_id);
            let descriptor = PreparedVersionWrite {
                path: write.path.clone(),
                expected: write.expected.clone(),
                version: Version {
                    id: version_id,
                    blob,
                    inline: None,
                    content_type: write.content_type.clone(),
                    deleted,
                    committed_at_unix_millis,
                },
            };
            let artifact = PreparedArtifact::new(
                PreparedArtifactKind::VersionDescriptor,
                serde_json::to_vec(&descriptor).map_err(program_storage_error)?,
            );
            writes.push(artifact.reference.clone());
            insert_artifact(&mut artifacts, artifact)?;
        }

        let mut emissions = Vec::with_capacity(source.emissions.len());
        for emission in &source.emissions {
            let bytes = encode_emission(&emission.payload)?;
            let payload = PreparedArtifact::new(PreparedArtifactKind::Payload, bytes);
            emissions.push(PreparedEmission {
                effect_id: emission.effect_id.clone(),
                route_id: emission.route_id.clone(),
                payload: payload.reference.clone(),
                content_type: emission.content_type.clone(),
            });
            insert_artifact(&mut artifacts, payload)?;
        }

        let receipt = PreparedArtifact::new(
            PreparedArtifactKind::Receipt,
            serde_json::to_vec(&source.receipt).map_err(program_storage_error)?,
        );
        let receipt_ref = receipt.reference.clone();
        insert_artifact(&mut artifacts, receipt)?;

        let record = StoredPreparedBundle {
            format: PREPARED_BUNDLE_FORMAT,
            source_bundle_hash,
            program_hash: lease.program_hash,
            preconditions: source.head_preconditions.clone(),
            writes,
            receipt: receipt_ref,
            emissions,
        };
        validate_prepared_record(&record)?;
        let bundle = PreparedArtifact::new(
            PreparedArtifactKind::Bundle,
            serde_json::to_vec(&record).map_err(program_storage_error)?,
        );
        let bundle_ref = bundle.reference.clone();
        let hash = PreparedBundleHash(bundle_ref.hash);
        insert_artifact(&mut artifacts, bundle)?;
        let artifacts = artifacts.into_values().collect::<Vec<_>>();
        let references = artifacts
            .iter()
            .map(|artifact| artifact.reference.clone())
            .collect::<Vec<_>>();
        let manifest_hash = artifact_manifest_hash(&references)?;
        let artifact_batch = PreparedArtifactBatch {
            bundle: bundle_ref.clone(),
            manifest_hash,
            artifacts,
        };
        validate_artifact_batch(&artifact_batch)?;

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

        let durability = self
            .program_artifacts
            .persist(artifact_batch)
            .await
            .map_err(program_storage_error)?;
        validate_durability_evidence(&durability)?;
        if durability.bundle != bundle_ref || durability.manifest_hash != manifest_hash {
            return Err(ProgramStoreError::DurabilityEvidenceMismatch);
        }
        let durability_evidence_hash = durability.hash()?;
        let recovered_evidence = self
            .program_artifacts
            .load_evidence(durability_evidence_hash)
            .await
            .map_err(program_storage_error)?
            .ok_or(ProgramStoreError::DurabilityEvidenceNotFound(
                durability_evidence_hash,
            ))?;
        if recovered_evidence != durability {
            return Err(ProgramStoreError::DurabilityEvidenceMismatch);
        }

        Ok(PreparedProgramBundle {
            hash,
            source_bundle_hash,
            program_hash: lease.program_hash,
            bundle: bundle_ref,
            manifest_hash,
            durability_evidence_hash,
            durability,
        })
    }

    pub async fn prepared_program_bundle(
        &self,
        hash: PreparedBundleHash,
        durability_evidence_hash: ProgramDurabilityEvidenceHash,
    ) -> Result<Option<PreparedProgramBundle>, ProgramStoreError> {
        match self
            .load_prepared_bundle(hash, durability_evidence_hash)
            .await
        {
            Ok(loaded) => Ok(Some(PreparedProgramBundle {
                hash,
                source_bundle_hash: loaded.record.source_bundle_hash,
                program_hash: loaded.record.program_hash,
                bundle: loaded.bundle,
                manifest_hash: loaded.evidence.manifest_hash,
                durability_evidence_hash,
                durability: loaded.evidence,
            })),
            Err(ProgramStoreError::ArtifactNotFound(_))
            | Err(ProgramStoreError::DurabilityEvidenceNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
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
