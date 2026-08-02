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
    CF_HEADS, CF_METADATA, CF_VERSIONS, PendingBlobReferences, VERSION_HIGH_WATERMARK_KEY,
    is_program_definition_path, now_unix_millis, version_blob_reference, version_key,
};
use crate::{BlobRef, Head, MutationError, ObjectKey, ObjectVersioning, Store, Version, VersionId};

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
    #[error("storage error: {0}")]
    Storage(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StoredPreparedBundle {
    format: u16,
    source_bundle_hash: PreparedBundleHash,
    program_hash: ProgramHash,
    preconditions: Vec<HeadPrecondition>,
    writes: Vec<PreparedVersionWrite>,
    receipt: CommandReceipt,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PreparedVersionWrite {
    path: ObjectPath,
    expected: ObservedHead,
    version: Version,
}

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

#[cfg(test)]
mod tests {
    use anvil_atomic_program::{
        Cardinality, DEFINITION_SCHEMA_VERSION, DocumentAccess, DocumentRef, DocumentSpec,
        DocumentValueRef, DocumentView, ExpectedHead, InputValue, IntegerType, InvocationContext,
        JsonPointerRef, Operation, PathBinding, PathTemplate, ProgramCaps, ProgramInvocation,
        ReturnDefinition, ValueSource,
    };
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::{BucketPolicy, Durability, PutMode, PutRequest, StoreOptions};

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
            caps: ProgramCaps {
                max_paths: 1,
                max_writes: 1,
                max_operations: 2,
                max_input_bytes: 64 * 1024,
                max_document_bytes: 64 * 1024,
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
            .unwrap();
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
        ProgramCommit {
            previous_commit_cursor,
            commit_cursor,
            bundle_ref: prepared.bundle,
            bundle_hash: prepared.hash,
            program_hash: prepared.program_hash,
            durability_class: ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS),
            durability_evidence_hash: prepared.durability_evidence_hash,
        }
    }

    #[tokio::test]
    async fn ordinary_blob_plane_attests_executor_local_durability() {
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
            bundle_ref: prepared.bundle,
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
        let before_apply = store.db.latest_sequence_number();
        let applied = store
            .apply_program_bundle(lease, &prepared, local_commit.clone())
            .await
            .unwrap();
        let apply_batches = store
            .db
            .get_updates_since(before_apply)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(apply_batches.len(), 1);
        assert_eq!(
            store.applied_program_commit().unwrap(),
            Some(AppliedProgramCommit {
                commit_cursor: 1,
                bundle_ref: prepared.bundle,
                bundle_hash: prepared.hash,
                program_hash: prepared.program_hash,
                durability_class: local_commit.durability_class,
                durability_evidence_hash: prepared.durability_evidence_hash,
            })
        );
        let marker = store
            .raw_get(CF_METADATA, APPLIED_PROGRAM_COMMIT_KEY)
            .unwrap()
            .unwrap();
        let marker = serde_json::from_slice::<serde_json::Value>(&marker).unwrap();
        let marker_fields = marker
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            marker_fields,
            BTreeSet::from([
                "bundle_hash",
                "bundle_ref",
                "commit_cursor",
                "durability_class",
                "durability_evidence_hash",
                "program_hash",
            ])
        );
        let applied_key = object_key(&counter_path()).unwrap();
        let applied_head = store.head(&applied_key).unwrap().unwrap();
        let applied_version = store
            .version_metadata(&applied_key, applied_head.version)
            .unwrap()
            .unwrap();
        let applied_blob = applied_version.blob.unwrap();
        let applied_blob_state = store.blob_reference_state(&applied_blob).unwrap().unwrap();
        assert_eq!(applied_blob_state.ref_count, 1);
        assert_eq!(applied_blob_state.flags, 0);
        let bundle_blob = BlobRef::from(prepared.bundle);
        let released_bundle = store.blob_reference_state(&bundle_blob).unwrap().unwrap();
        assert_eq!(released_bundle.ref_count, 0);
        assert_eq!(released_bundle.flags, 0);
        assert!(
            !store
                .read_blob_bytes(&bundle_blob)
                .await
                .unwrap()
                .is_empty()
        );
        let invalidations = store.scan_local_invalidations(0, 10).unwrap();
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].key, object_key(&counter_path()).unwrap());
        assert_eq!(
            invalidations[0].minimum_path_version,
            store.head(&invalidations[0].key).unwrap().unwrap().version
        );

        // Recovery of an already-finalized commit must not append a duplicate
        // invalidation. The compact commit marker, head and journal move together in
        // the one local RocksDB WriteBatch above.
        let replayed = store
            .recover_program_bundle(ProgramCommit {
                previous_commit_cursor: None,
                commit_cursor: 1,
                bundle_ref: prepared.bundle,
                bundle_hash: prepared.hash,
                program_hash: prepared.program_hash,
                durability_class: ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS),
                durability_evidence_hash: prepared.durability_evidence_hash,
            })
            .await
            .unwrap();
        assert_eq!(replayed, applied);
        assert_eq!(store.local_invalidation_offset().unwrap(), 1);
        assert_eq!(
            store.blob_reference_state(&applied_blob).unwrap().unwrap(),
            applied_blob_state
        );

        let replay_grace_millis = crate::DEFAULT_AWAITING_PUBLISH_TTL_SECONDS * 1_000;
        assert_eq!(
            store
                .collect_blob_garbage_at(released_bundle.updated_at + replay_grace_millis - 1)
                .unwrap(),
            0
        );
        assert!(store.read_blob_bytes(&bundle_blob).await.is_ok());
        assert_eq!(
            store
                .collect_blob_garbage_at(released_bundle.updated_at + replay_grace_millis)
                .unwrap(),
            1
        );
        assert_eq!(
            store.read_blob_bytes(&bundle_blob).await.unwrap_err(),
            MutationError::BlobNotFound
        );
    }

    #[test]
    fn replay_uses_no_local_receipt_column_family() {
        assert!(
            !crate::store::COLUMN_FAMILIES
                .iter()
                .any(|name| name.contains("program") || name.contains("replay"))
        );
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
    async fn apply_is_all_old_or_all_new_and_records_only_the_compact_cursor() {
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
        assert_eq!(store.applied_program_commit_cursor().unwrap(), Some(1));
        let current_version = first.published_versions[&counter_path()];
        let second_invocation = invocation(
            "command-2",
            ExpectedHead::Version {
                version: current_version.version.0.to_string(),
            },
        );
        let second_lease = engine.prepare(&context, &second_invocation).await.unwrap();
        let second_prepared = store.prepare_program_bundle(&second_lease).await.unwrap();
        let second_commit = commit(&second_prepared, Some(1), 2);
        let second = store
            .apply_program_bundle(second_lease, &second_prepared, second_commit)
            .await
            .unwrap();
        assert!(second.published_versions[&counter_path()].version > current_version.version);
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
                mode: PutMode::PutImmutable,
                command_id: Some("install-victim".into()),
                durability: Durability::Local,
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
            blob: Some(BlobRef {
                hash: [0x99; 32],
                length: 1,
            }),
            content_type: Some("application/json".into()),
            deleted: false,
            committed_at_unix_millis: now_unix_millis().unwrap(),
        };
        let key = object_key(&counter_path()).unwrap();
        let identity = store
            .resolve_bucket_identity(key.tenant(), key.bucket())
            .unwrap();
        let mut batch = WriteBatch::default();
        batch.put_cf(
            store.program_cf(CF_VERSIONS).unwrap(),
            version_key(identity, &key, rogue_id),
            serde_json::to_vec(&rogue).unwrap(),
        );
        batch.put_cf(
            store.program_cf(CF_HEADS).unwrap(),
            identity.head_key(key.path()),
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
        assert!(store.applied_program_commit().unwrap().is_none());
    }

    #[tokio::test]
    async fn ordinary_prepared_blobs_survive_reopen_for_recovery() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
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
            .unwrap();
        assert_eq!(
            reopened
                .prepared_program_bundle(
                    prepared.bundle,
                    prepared.hash,
                    prepared.durability_evidence_hash,
                )
                .await
                .unwrap(),
            Some(prepared.clone())
        );
        let applied = reopened
            .recover_program_bundle(commit(&prepared, None, 1))
            .await
            .unwrap();
        assert_eq!(applied.receipt.command_id, "recover");
        assert_eq!(reopened.applied_program_commit_cursor().unwrap(), Some(1));
        let replayed = reopened
            .committed_program_result(commit(&prepared, None, 1))
            .await
            .unwrap();
        assert_eq!(replayed, applied);
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
        wrong_reference.bundle_ref = PreparedBundleRef {
            hash: [0x41; 32],
            length: prepared.bundle.length,
        };
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
        corrupt.bundle_ref = PreparedBundleRef {
            hash: [8; 32],
            length: prepared.bundle.length,
        };
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
    async fn mutable_read_only_program_dependency_is_rejected_before_execution() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let config_path = ObjectPath::new("tenant", "bucket", "configuration/current").unwrap();
        let config = store
            .put(PutRequest {
                key: object_key(&config_path).unwrap(),
                bytes: serde_json::to_vec(&json!({"enabled": true})).unwrap(),
                content_type: Some("application/json".into()),
                mode: PutMode::PutIfAbsent,
                command_id: Some("install-mutable-configuration".into()),
                durability: Durability::Local,
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
            EngineError::ProgramConcurrency { path, reason }
                if path.path == "configuration/current" && reason.contains("PROGRAM_ONLY")
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
            .unwrap();
        let config_path = ObjectPath::new("tenant", "bucket", "configuration/current").unwrap();
        let config = store
            .put(PutRequest {
                key: object_key(&config_path).unwrap(),
                bytes: serde_json::to_vec(&json!({"enabled": true})).unwrap(),
                content_type: Some("application/json".into()),
                mode: PutMode::PutIfAbsent,
                command_id: Some("install-configuration".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        store
            .set_bucket_policy(
                "tenant",
                "bucket",
                BucketPolicy {
                    immutable_prefixes: vec!["configuration".into()],
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
            EngineError::ProgramConcurrency { path, reason }
                if path.path == "configuration/current" && reason.contains("PROGRAM_ONLY")
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
            let _commit_guard = self.commit_lock.lock().await;
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
            if let Some(existing) = self.applied_program_commit()?
                && existing.commit_cursor == commit.commit_cursor
            {
                self.match_applied_commit(&existing, &commit)?;
                return Ok(committed_result(&loaded.record));
            }
            self.apply_prepared_record(&loaded, &commit)
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
        let _commit_guard = self.commit_lock.lock().await;
        // Re-read and verify under the commit fence so GC cannot retire an
        // awaiting output or bundle between verification and publication.
        let loaded = self
            .load_prepared_bundle(
                commit.bundle_ref,
                commit.bundle_hash,
                commit.durability_evidence_hash,
            )
            .await?;
        if let Some(existing) = self.applied_program_commit()?
            && existing.commit_cursor == commit.commit_cursor
        {
            self.match_applied_commit(&existing, &commit)?;
            return Ok(committed_result(&loaded.record));
        }
        self.apply_prepared_record(&loaded, &commit)
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
        let mut invalidations = Vec::with_capacity(record.writes.len());
        let mut pending_blob_references = PendingBlobReferences::new();
        let publication_at_unix_millis = now_unix_millis().map_err(program_mutation_error)?;
        for write in &record.writes {
            let key = object_key(&write.path)?;
            let identity = *identities
                .get(&(write.path.tenant.clone(), write.path.bucket.clone()))
                .ok_or_else(|| {
                    ProgramStoreError::Storage("write has no stable bucket identity".into())
                })?;
            let version_bytes =
                serde_json::to_vec(&write.version).map_err(program_storage_error)?;
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
                let old_version = current_versions
                    .get(&write.path)
                    .ok_or_else(|| {
                        ProgramStoreError::Storage(
                            "write has no current-version observation".into(),
                        )
                    })?
                    .as_ref();
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
                })
                .map_err(program_storage_error)?,
            );
            invalidations.push((key, write.version.id, write.version.deleted));
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
        self.stage_local_invalidations(&mut batch, &invalidations)
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
        if !invalidations.is_empty() {
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
        validate_source_bundle(source)?;
        self.validate_program_policies(source)?;
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
            program_hash: lease.program_hash,
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
            program_hash: lease.program_hash,
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
