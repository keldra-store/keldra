use serde::{Deserialize, Serialize};
use thiserror::Error;

use keldra_atomic_program::MAX_OBJECT_PATH_BYTES;

use crate::{
    BlobRef, DefinitionOperation, DefinitionTransition, ObjectKey, ReferenceDelta, SourceId,
};

/// A bucket policy is deliberately small enough to validate on every write.
pub const MAX_BUCKET_POLICY_PREFIXES: usize = 64;
/// Combined UTF-8 bytes across both prefix lists in one bucket policy.
pub const MAX_BUCKET_POLICY_PREFIX_BYTES: usize = 8 * 1024;
/// Payloads at or below this size use the RocksDB-backed small-byte plane.
pub const SMALL_BLOB_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VersionId(pub u64);

pub const MUTATION_STAMP_FORMAT: u16 = 1;
pub const OBJECT_MUTATION_FORMAT: u16 = 1;
pub const RETAINED_VERSION_DELETE_FORMAT: u16 = 1;
pub const MAX_OBJECT_MUTATION_REFERENCE_DELTAS: usize = 2;
pub const MAX_CONTENT_TYPE_BYTES: usize = 512;

/// Consensus-neutral identity of the Raft entry that activated one placement
/// view. It is the complete OpenRaft LogId shape without coupling the store to
/// OpenRaft or the consensus crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementLogId {
    pub term: u64,
    pub index: u64,
}

/// Bounded lineage attached to every distributed 0.5.1 object-head candidate.
/// A missing stamp is reserved for an authoritative 0.5.0 committed baseline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationStamp {
    pub format: u16,
    pub predecessor_version: Option<VersionId>,
    /// Raft cursor for an explicitly atomic-program mutation. Ordinary object
    /// mutations leave this absent. Readers use it only as a visibility fence;
    /// it is not a second commit record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program_commit_cursor: Option<u64>,
    pub mutation_fingerprint: [u8; 32],
    pub active_placement_log_id: PlacementLogId,
    pub serving_fence_term: u64,
    pub source_id: SourceId,
    pub source_journal_position: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub version: VersionId,
    pub deleted: bool,
    /// Released 0.5.0 heads omit this field and remain committed baselines.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_stamp: Option<MutationStamp>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub id: VersionId,
    pub blob: Option<BlobRef>,
    pub content_type: Option<String>,
    pub deleted: bool,
    pub committed_at_unix_millis: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectVersioning {
    #[default]
    Unversioned,
    Enabled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    pub key: ObjectKey,
    pub version: Version,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precondition {
    #[default]
    Any,
    Absent,
    Version(VersionId),
}

/// The four explicit public put operations. Keeping intent distinct from the
/// derived head precondition is necessary because PutIfAbsent and PutImmutable
/// both compare against absence but have different path-policy admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PutMode {
    Put,
    PutIfAbsent,
    PutIfVersion(VersionId),
    PutImmutable,
}

impl PutMode {
    pub fn precondition(self) -> Precondition {
        match self {
            Self::Put => Precondition::Any,
            Self::PutIfAbsent | Self::PutImmutable => Precondition::Absent,
            Self::PutIfVersion(version) => Precondition::Version(version),
        }
    }
}

/// Per-request durability for ordinary object mutations. Local is deliberately
/// the default fast path. Replicated is retained in the stable API but cannot
/// be satisfied by the single-node 0.5.0 store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    #[default]
    Local,
    Replicated,
}

#[derive(Clone, Debug)]
pub struct PutRequest {
    pub key: ObjectKey,
    pub bytes: Vec<u8>,
    pub content_type: Option<String>,
    pub mode: PutMode,
    pub command_id: Option<String>,
    pub durability: Durability,
}

#[derive(Clone, Debug)]
pub struct PublishRequest {
    pub key: ObjectKey,
    pub blob: BlobRef,
    pub content_type: Option<String>,
    pub mode: PutMode,
    pub command_id: Option<String>,
    pub durability: Durability,
}

#[derive(Clone, Debug)]
pub struct DeleteRequest {
    pub key: ObjectKey,
    pub precondition: Precondition,
    pub command_id: Option<String>,
    pub durability: Durability,
}

#[derive(Clone, Debug)]
pub enum BatchOperation {
    Put(PutRequest),
    Publish(PublishRequest),
    Delete(DeleteRequest),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchOutcome {
    pub index: usize,
    pub result: Result<MutationReceipt, MutationError>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationReceipt {
    pub command_id: Option<String>,
    pub fingerprint: [u8; 32],
    pub version: VersionId,
    pub deleted: bool,
    pub replayed: bool,
    /// Zero only for internal callers that deliberately omitted a command ID.
    pub replay_guarantee_expires_at_unix_millis: u64,
}

/// Consensus-derived values needed to construct one distributed object
/// mutation. The source identity and position are assigned by the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectMutationContext {
    pub active_placement_log_id: PlacementLogId,
    pub serving_fence_term: u64,
}

/// Authoritative bucket-scoped inputs resolved from their complete logical
/// replica group before an exact-path coordinator evaluates a mutation.
/// These values are typed policy, not a cache or a second persistence plane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMutationGovernance {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub versioning: ObjectVersioning,
    pub policy: BucketPolicy,
}

impl ObjectMutationGovernance {
    pub fn validate(&self) -> Result<(), MutationError> {
        if self.tenant_id == 0 || self.bucket_id == 0 {
            return Err(MutationError::InvalidPolicy(
                "stable tenant and bucket IDs must be non-zero".into(),
            ));
        }
        self.policy.validate()
    }
}

/// One exact, bounded object mutation replicated between metadata owners.
/// Payload bytes and raw RocksDB operations are deliberately absent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMutation {
    pub format: u16,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
    pub command_id: String,
    pub input_fingerprint: [u8; 32],
    pub version: Version,
    pub retire_predecessor: bool,
    pub receipt_expires_at_unix_millis: u64,
    pub stamp: MutationStamp,
    pub reference_deltas: Vec<ReferenceDelta>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accounting_transition: Option<crate::AccountingHeadTransition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_transition: Option<DefinitionTransition>,
}

impl ObjectMutation {
    pub(crate) fn set_computed_fingerprint(&mut self) {
        self.stamp.mutation_fingerprint = self.computed_fingerprint();
    }

    pub fn computed_fingerprint(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"keldra.object-mutation.v1");
        hash_u16(&mut hasher, self.format);
        hash_u64(&mut hasher, self.tenant_id);
        hash_u64(&mut hasher, self.bucket_id);
        hash_bytes(&mut hasher, self.exact_path.as_bytes());
        hash_bytes(&mut hasher, self.command_id.as_bytes());
        hasher.update(&self.input_fingerprint);
        hash_version(&mut hasher, &self.version);
        hasher.update(&[u8::from(self.retire_predecessor)]);
        hash_u64(&mut hasher, self.receipt_expires_at_unix_millis);
        hash_u16(&mut hasher, self.stamp.format);
        hash_optional_version(&mut hasher, self.stamp.predecessor_version);
        hash_optional_u64(&mut hasher, self.stamp.program_commit_cursor);
        hash_u64(&mut hasher, self.stamp.active_placement_log_id.term);
        hash_u64(&mut hasher, self.stamp.active_placement_log_id.index);
        hash_u64(&mut hasher, self.stamp.serving_fence_term);
        hash_u16(&mut hasher, self.stamp.source_id.node_id);
        hasher.update(&self.stamp.source_id.source_epoch);
        hash_u64(&mut hasher, self.stamp.source_journal_position);
        hash_u64(&mut hasher, self.reference_deltas.len() as u64);
        for delta in &self.reference_deltas {
            hash_blob(&mut hasher, &delta.blob);
            hasher.update(&delta.change.to_be_bytes());
        }
        if let Some(transition) = self.accounting_transition {
            hasher.update(b"keldra.accounting-head-transition.v1");
            hash_optional_u64(&mut hasher, transition.previous_live_length);
            hash_optional_u64(&mut hasher, transition.current_live_length);
        }
        match self.definition_transition.as_ref() {
            Some(transition) => {
                hasher.update(&[1]);
                hasher.update(&[transition.kind as u8]);
                hash_u64(&mut hasher, transition.tenant_id);
                hash_u64(&mut hasher, transition.bucket_id);
                hash_u64(&mut hasher, transition.definition_id);
                hash_bytes(&mut hasher, transition.path.as_bytes());
                hash_u64(&mut hasher, transition.object_version.0);
                hasher.update(&[transition.operation as u8]);
            }
            None => {
                hasher.update(&[0]);
            }
        }
        *hasher.finalize().as_bytes()
    }

    pub fn validate(&self) -> Result<(), MutationError> {
        if self.format != OBJECT_MUTATION_FORMAT {
            return Err(MutationError::InvalidObjectMutation(format!(
                "unsupported object mutation format {}",
                self.format
            )));
        }
        if self.stamp.format != MUTATION_STAMP_FORMAT {
            return Err(MutationError::InvalidObjectMutation(format!(
                "unsupported mutation stamp format {}",
                self.stamp.format
            )));
        }
        if self.tenant_id == 0 || self.bucket_id == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "stable tenant and bucket IDs must be non-zero".into(),
            ));
        }
        validate_exact_path(&self.exact_path)?;
        if self.command_id.is_empty()
            || self.command_id.len() > 256
            || self.command_id.contains('\0')
        {
            return Err(MutationError::InvalidObjectMutation(
                "command ID must contain 1 to 256 bytes and no NUL".into(),
            ));
        }
        if self.version.id.0 == 0
            || self.version.deleted != self.version.blob.is_none()
            || self
                .version
                .content_type
                .as_ref()
                .is_some_and(|value| value.len() > MAX_CONTENT_TYPE_BYTES)
            || self.version.deleted && self.version.content_type.is_some()
        {
            return Err(MutationError::InvalidObjectMutation(
                "new version descriptor is malformed".into(),
            ));
        }
        if self.stamp.predecessor_version == Some(self.version.id)
            || self
                .stamp
                .predecessor_version
                .is_some_and(|predecessor| predecessor >= self.version.id)
        {
            return Err(MutationError::InvalidObjectMutation(
                "new version must follow its predecessor".into(),
            ));
        }
        if self.retire_predecessor && self.stamp.predecessor_version.is_none() {
            return Err(MutationError::InvalidObjectMutation(
                "predecessor retirement does not match mutation lineage".into(),
            ));
        }
        if self.stamp.program_commit_cursor.is_some() {
            return Err(MutationError::InvalidObjectMutation(
                "ordinary object mutation carries an atomic-program commit cursor".into(),
            ));
        }
        if self.receipt_expires_at_unix_millis <= self.version.committed_at_unix_millis {
            return Err(MutationError::InvalidObjectMutation(
                "mutation receipt does not outlive the committed version".into(),
            ));
        }
        if self.stamp.serving_fence_term == 0
            || self.stamp.source_journal_position == 0
            || self.stamp.source_id.node_id == 0
            || self.stamp.source_id.source_epoch == [0; 32]
        {
            return Err(MutationError::InvalidObjectMutation(
                "mutation serving fence or source identity is invalid".into(),
            ));
        }
        if self.reference_deltas.len() > MAX_OBJECT_MUTATION_REFERENCE_DELTAS
            || self
                .reference_deltas
                .iter()
                .any(|delta| !matches!(delta.change, -1 | 1))
        {
            return Err(MutationError::InvalidObjectMutation(
                "object mutation reference deltas are malformed".into(),
            ));
        }
        for (index, delta) in self.reference_deltas.iter().enumerate() {
            if self.reference_deltas[..index]
                .iter()
                .any(|earlier| earlier.blob == delta.blob)
            {
                return Err(MutationError::InvalidObjectMutation(
                    "object mutation repeats one reference delta".into(),
                ));
            }
        }
        if let Some(transition) = self.accounting_transition {
            transition
                .validate()
                .map_err(|error| MutationError::InvalidObjectMutation(error.into()))?;
            let current_live_length = self.version.blob.as_ref().map(|blob| blob.length);
            if transition.current_live_length != current_live_length
                || (self.stamp.predecessor_version.is_none()
                    && transition.previous_live_length.is_some())
            {
                return Err(MutationError::InvalidObjectMutation(
                    "accounting head-transition does not match mutation lineage".into(),
                ));
            }
        }
        if let Some(transition) = self.definition_transition.as_ref() {
            transition
                .validate()
                .map_err(|error| MutationError::InvalidObjectMutation(error.to_string()))?;
            let expected_operation = if self.version.deleted {
                DefinitionOperation::Delete
            } else {
                DefinitionOperation::Upsert
            };
            if transition.tenant_id != self.tenant_id
                || transition.bucket_id != self.bucket_id
                || transition.path != self.exact_path
                || transition.object_version != self.version.id
                || transition.operation != expected_operation
            {
                return Err(MutationError::InvalidObjectMutation(
                    "definition transition does not match its enclosing object mutation".into(),
                ));
            }
        }
        if self.stamp.mutation_fingerprint != self.computed_fingerprint() {
            return Err(MutationError::InvalidObjectMutation(
                "mutation fingerprint does not match its typed result".into(),
            ));
        }
        Ok(())
    }

    pub fn receipt(&self, replayed: bool) -> MutationReceipt {
        MutationReceipt {
            command_id: Some(self.command_id.clone()),
            fingerprint: self.input_fingerprint,
            version: self.version.id,
            deleted: self.version.deleted,
            replayed,
            replay_guarantee_expires_at_unix_millis: self.receipt_expires_at_unix_millis,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatedObjectMutation {
    pub receipt: MutationReceipt,
    /// None only when evaluation was an idempotent or immutable semantic replay.
    pub mutation: Option<ObjectMutation>,
}

/// One coordinator-selected deletion from a versioned path's immutable
/// descriptor set. The typed expected head and target descriptor are the
/// complete compare condition; replicas never infer lineage from arrival
/// order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedVersionDeleteMutation {
    pub format: u16,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
    pub expected_head: Head,
    pub target: Version,
    pub replacement_tombstone: Option<Version>,
    pub stamp: MutationStamp,
    pub reference_deltas: Vec<ReferenceDelta>,
}

impl RetainedVersionDeleteMutation {
    pub(crate) fn set_computed_fingerprint(&mut self) {
        self.stamp.mutation_fingerprint = self.computed_fingerprint();
    }

    pub fn computed_fingerprint(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"keldra.retained-version-delete.v1");
        hash_u16(&mut hasher, self.format);
        hash_u64(&mut hasher, self.tenant_id);
        hash_u64(&mut hasher, self.bucket_id);
        hash_bytes(&mut hasher, self.exact_path.as_bytes());
        hash_head(&mut hasher, &self.expected_head);
        hash_version(&mut hasher, &self.target);
        match self.replacement_tombstone.as_ref() {
            Some(version) => {
                hasher.update(&[1]);
                hash_version(&mut hasher, version);
            }
            None => {
                hasher.update(&[0]);
            }
        }
        hash_u16(&mut hasher, self.stamp.format);
        hash_optional_version(&mut hasher, self.stamp.predecessor_version);
        hash_optional_u64(&mut hasher, self.stamp.program_commit_cursor);
        hash_u64(&mut hasher, self.stamp.active_placement_log_id.term);
        hash_u64(&mut hasher, self.stamp.active_placement_log_id.index);
        hash_u64(&mut hasher, self.stamp.serving_fence_term);
        hash_u16(&mut hasher, self.stamp.source_id.node_id);
        hasher.update(&self.stamp.source_id.source_epoch);
        hash_u64(&mut hasher, self.stamp.source_journal_position);
        hash_u64(&mut hasher, self.reference_deltas.len() as u64);
        for delta in &self.reference_deltas {
            hash_blob(&mut hasher, &delta.blob);
            hasher.update(&delta.change.to_be_bytes());
        }
        *hasher.finalize().as_bytes()
    }

    pub fn validate(&self) -> Result<(), MutationError> {
        if self.format != RETAINED_VERSION_DELETE_FORMAT {
            return Err(MutationError::InvalidObjectMutation(format!(
                "unsupported retained-version deletion format {}",
                self.format
            )));
        }
        if self.tenant_id == 0 || self.bucket_id == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "stable tenant and bucket IDs must be non-zero".into(),
            ));
        }
        ObjectKey::new("typed", "delete-version", &self.exact_path)
            .map_err(|error| MutationError::InvalidObjectMutation(error.to_string()))?;
        validate_version_descriptor(&self.target)?;
        if self.expected_head.version.0 == 0
            || self.stamp.format != MUTATION_STAMP_FORMAT
            || self.stamp.predecessor_version != Some(self.expected_head.version)
            || self.stamp.program_commit_cursor.is_some()
            || self.stamp.serving_fence_term == 0
            || self.stamp.source_id.node_id == 0
            || self.stamp.source_id.source_epoch == [0; 32]
            || self.stamp.source_journal_position == 0
        {
            return Err(MutationError::InvalidObjectMutation(
                "retained-version deletion lineage or source is malformed".into(),
            ));
        }
        match self.replacement_tombstone.as_ref() {
            Some(replacement) => {
                validate_version_descriptor(replacement)?;
                if self.expected_head.version != self.target.id
                    || self.expected_head.deleted != self.target.deleted
                    || self.target.deleted
                    || !replacement.deleted
                    || replacement.id <= self.expected_head.version
                {
                    return Err(MutationError::InvalidObjectMutation(
                        "current-version deletion replacement is malformed".into(),
                    ));
                }
            }
            None if self.expected_head.version == self.target.id => {
                return Err(MutationError::InvalidObjectMutation(
                    "current-version deletion requires a fresh tombstone".into(),
                ));
            }
            None => {}
        }
        let expected_delta = self.target.blob.as_ref().map(|blob| ReferenceDelta {
            blob: blob.clone(),
            change: -1,
        });
        if self.reference_deltas.as_slice() != expected_delta.as_slice() {
            return Err(MutationError::InvalidObjectMutation(
                "retained-version deletion reference effect is malformed".into(),
            ));
        }
        if self.stamp.mutation_fingerprint != self.computed_fingerprint() {
            return Err(MutationError::InvalidObjectMutation(
                "retained-version deletion fingerprint does not match".into(),
            ));
        }
        Ok(())
    }

    pub fn outcome(&self) -> DeleteRetainedVersionOutcome {
        self.replacement_tombstone.as_ref().map_or(
            DeleteRetainedVersionOutcome::DeletedNonCurrent,
            |replacement| DeleteRetainedVersionOutcome::ReplacedCurrentWithTombstone {
                version: replacement.id,
            },
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatedRetainedVersionDelete {
    pub outcome: DeleteRetainedVersionOutcome,
    pub mutation: Option<RetainedVersionDeleteMutation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaRetainedVersionDeleteApplied {
    pub outcome: DeleteRetainedVersionOutcome,
    pub replayed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaObjectMutationApplied {
    pub version: VersionId,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeleteRetainedVersionOutcome {
    NotFound,
    DeletedNonCurrent,
    ReplacedCurrentWithTombstone { version: VersionId },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketPolicy {
    /// Canonical prefixes whose matching paths can be created exactly once.
    #[serde(default)]
    pub immutable_prefixes: Vec<String>,
    /// Canonical prefixes writable only through an invoked atomic program.
    #[serde(default)]
    pub program_only_prefixes: Vec<String>,
}

impl BucketPolicy {
    pub fn validate(&self) -> Result<(), MutationError> {
        let prefix_count = self
            .immutable_prefixes
            .len()
            .saturating_add(self.program_only_prefixes.len());
        if prefix_count > MAX_BUCKET_POLICY_PREFIXES {
            return Err(MutationError::InvalidPolicy(format!(
                "a bucket policy may contain at most {MAX_BUCKET_POLICY_PREFIXES} prefixes in total"
            )));
        }
        let prefixes = self
            .immutable_prefixes
            .iter()
            .chain(&self.program_only_prefixes);
        let encoded_bytes =
            prefixes.fold(0_usize, |total, prefix| total.saturating_add(prefix.len()));
        if encoded_bytes > MAX_BUCKET_POLICY_PREFIX_BYTES {
            return Err(MutationError::InvalidPolicy(format!(
                "bucket policy prefixes may contain at most {MAX_BUCKET_POLICY_PREFIX_BYTES} UTF-8 bytes in total"
            )));
        }
        validate_prefixes("immutable", &self.immutable_prefixes)?;
        validate_prefixes("program-only", &self.program_only_prefixes)?;
        for immutable in &self.immutable_prefixes {
            for program_only in &self.program_only_prefixes {
                if prefix_matches(immutable, program_only)
                    || prefix_matches(program_only, immutable)
                {
                    return Err(MutationError::InvalidPolicy(format!(
                        "immutable prefix `{immutable}` and program-only prefix `{program_only}` must not overlap"
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn is_immutable(&self, path: &str) -> bool {
        matches_prefix(&self.immutable_prefixes, path)
    }

    pub fn is_program_only(&self, path: &str) -> bool {
        matches_prefix(&self.program_only_prefixes, path)
    }
}

fn validate_prefixes(kind: &str, prefixes: &[String]) -> Result<(), MutationError> {
    let mut previous: Option<&str> = None;
    for prefix in prefixes {
        if prefix.is_empty()
            || prefix.starts_with('/')
            || prefix.ends_with('/')
            || prefix.contains("//")
        {
            return Err(MutationError::InvalidPolicy(format!(
                "{kind} prefixes must be canonical and non-empty"
            )));
        }
        if let Some(previous) = previous
            && prefix.as_str() <= previous
        {
            return Err(MutationError::InvalidPolicy(format!(
                "{kind} prefixes must be sorted and unique"
            )));
        }
        previous = Some(prefix);
    }
    Ok(())
}

fn matches_prefix(prefixes: &[String], path: &str) -> bool {
    prefixes.iter().any(|prefix| prefix_matches(prefix, path))
}

fn prefix_matches(prefix: &str, path: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum MutationError {
    #[error("precondition failed; current version is {current:?}")]
    PreconditionFailed { current: Option<VersionId> },
    #[error("path belongs to an immutable namespace")]
    Immutable,
    #[error("PutImmutable requires an immutable path policy")]
    ImmutablePolicyRequired,
    #[error("ordinary mutation cannot write a PROGRAM_ONLY path")]
    ProgramConcurrencyViolation,
    #[error("command id was reused with different input")]
    IdempotencyConflict,
    #[error("invalid command id")]
    InvalidCommandId,
    #[error("blob is not present on this node")]
    BlobNotFound,
    #[error("requested durability is unavailable")]
    DurabilityUnavailable,
    #[error("mutation receipt capacity is exhausted by unexpired guarantees")]
    ReceiptCapacity,
    #[error(
        "one mutation receipt requires {bytes} bytes, exceeding the configured {maximum} byte bound"
    )]
    ReceiptTooLarge { bytes: u64, maximum: u64 },
    #[error("source journal capacity is exhausted before required consumers are durable")]
    SourceJournalCapacity,
    #[error(
        "one source-journal transition requires {bytes} bytes, exceeding the configured {maximum} byte bound"
    )]
    SourceJournalRecordTooLarge { bytes: u64, maximum: u64 },
    #[error("invalid replicated object mutation: {0}")]
    InvalidObjectMutation(String),
    #[error(
        "replicated object mutation has a lineage gap: local head {current:?}, incoming predecessor {predecessor:?}"
    )]
    ObjectMutationLineageGap {
        current: Option<VersionId>,
        predecessor: Option<VersionId>,
    },
    #[error("replicated object mutations are contradictory siblings of {predecessor:?}")]
    ObjectMutationSibling { predecessor: Option<VersionId> },
    #[error("replicated object mutation conflicts with its durable receipt or version")]
    ObjectMutationConflict,
    #[error("object versioning is not enabled for this bucket")]
    ObjectVersioningNotEnabled,
    #[error("the current tombstone is the path's CAS/ABA fence and cannot be deleted")]
    CurrentTombstoneCannotBeDeleted,
    #[error("invalid bucket policy: {0}")]
    InvalidPolicy(String),
    #[error("storage error: {0}")]
    Storage(String),
}

fn validate_exact_path(path: &str) -> Result<(), MutationError> {
    if path.is_empty()
        || path.len() > MAX_OBJECT_PATH_BYTES
        || path.contains('\0')
        || path.starts_with('/')
        || path.ends_with('/')
        || path
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
        || path.chars().any(char::is_control)
    {
        return Err(MutationError::InvalidObjectMutation(
            "exact object path is not canonical".into(),
        ));
    }
    Ok(())
}

fn hash_version(hasher: &mut blake3::Hasher, version: &Version) {
    hash_u64(hasher, version.id.0);
    match &version.blob {
        Some(blob) => {
            hasher.update(&[1]);
            hash_blob(hasher, blob);
        }
        None => {
            hasher.update(&[0]);
        }
    }
    match &version.content_type {
        Some(value) => {
            hasher.update(&[1]);
            hash_bytes(hasher, value.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(&[u8::from(version.deleted)]);
    hash_u64(hasher, version.committed_at_unix_millis);
}

fn hash_head(hasher: &mut blake3::Hasher, head: &Head) {
    hash_u64(hasher, head.version.0);
    hasher.update(&[u8::from(head.deleted)]);
    match head.mutation_stamp {
        Some(stamp) => {
            hasher.update(&[1]);
            hash_u16(hasher, stamp.format);
            hash_optional_version(hasher, stamp.predecessor_version);
            hash_optional_u64(hasher, stamp.program_commit_cursor);
            hasher.update(&stamp.mutation_fingerprint);
            hash_u64(hasher, stamp.active_placement_log_id.term);
            hash_u64(hasher, stamp.active_placement_log_id.index);
            hash_u64(hasher, stamp.serving_fence_term);
            hash_u16(hasher, stamp.source_id.node_id);
            hasher.update(&stamp.source_id.source_epoch);
            hash_u64(hasher, stamp.source_journal_position);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn validate_version_descriptor(version: &Version) -> Result<(), MutationError> {
    if version.id.0 == 0
        || version.deleted != version.blob.is_none()
        || version
            .content_type
            .as_ref()
            .is_some_and(|value| value.len() > MAX_CONTENT_TYPE_BYTES)
        || version.deleted && version.content_type.is_some()
    {
        return Err(MutationError::InvalidObjectMutation(
            "version descriptor is malformed".into(),
        ));
    }
    Ok(())
}

fn hash_blob(hasher: &mut blake3::Hasher, blob: &BlobRef) {
    hasher.update(&blob.hash);
    hash_u64(hasher, blob.length);
}

fn hash_optional_version(hasher: &mut blake3::Hasher, version: Option<VersionId>) {
    match version {
        Some(version) => {
            hasher.update(&[1]);
            hash_u64(hasher, version.0);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_optional_u64(hasher: &mut blake3::Hasher, value: Option<u64>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hash_u64(hasher, value);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_bytes(hasher: &mut blake3::Hasher, value: &[u8]) {
    hash_u64(hasher, value.len() as u64);
    hasher.update(value);
}

fn hash_u16(hasher: &mut blake3::Hasher, value: u16) {
    hasher.update(&value.to_be_bytes());
}

fn hash_u64(hasher: &mut blake3::Hasher, value: u64) {
    hasher.update(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numbered_prefixes(label: &str, count: usize) -> Vec<String> {
        (0..count)
            .map(|index| format!("{label}-{index:03}"))
            .collect()
    }

    #[test]
    fn bucket_policy_prefix_count_bound_applies_to_both_lists_together() {
        let boundary = BucketPolicy {
            immutable_prefixes: numbered_prefixes("immutable", 32),
            program_only_prefixes: numbered_prefixes("program", 32),
        };
        assert_eq!(
            boundary.immutable_prefixes.len() + boundary.program_only_prefixes.len(),
            MAX_BUCKET_POLICY_PREFIXES
        );
        boundary.validate().unwrap();

        let over_limit = BucketPolicy {
            immutable_prefixes: numbered_prefixes("immutable", 32),
            program_only_prefixes: numbered_prefixes("program", 33),
        };
        assert!(matches!(
            over_limit.validate(),
            Err(MutationError::InvalidPolicy(message))
                if message.contains("at most 64 prefixes")
        ));
    }

    #[test]
    fn bucket_policy_byte_bound_counts_utf8_bytes_across_both_lists() {
        let half = MAX_BUCKET_POLICY_PREFIX_BYTES / 2;
        let boundary = BucketPolicy {
            immutable_prefixes: vec!["a".repeat(half)],
            program_only_prefixes: vec!["é".repeat(half / "é".len())],
        };
        let boundary_bytes = boundary
            .immutable_prefixes
            .iter()
            .chain(&boundary.program_only_prefixes)
            .map(String::len)
            .sum::<usize>();
        assert_eq!(boundary_bytes, MAX_BUCKET_POLICY_PREFIX_BYTES);
        boundary.validate().unwrap();

        let over_limit = BucketPolicy {
            immutable_prefixes: boundary.immutable_prefixes,
            program_only_prefixes: vec![format!("{}x", boundary.program_only_prefixes[0])],
        };
        assert!(matches!(
            over_limit.validate(),
            Err(MutationError::InvalidPolicy(message))
                if message.contains("UTF-8 bytes")
        ));
    }

    #[test]
    fn bucket_policy_rejects_overlap_between_immutable_and_program_only_prefixes() {
        for (immutable, program_only) in [
            ("shared", "shared"),
            ("shared", "shared/child"),
            ("shared/child", "shared"),
        ] {
            let policy = BucketPolicy {
                immutable_prefixes: vec![immutable.into()],
                program_only_prefixes: vec![program_only.into()],
            };
            assert!(matches!(
                policy.validate(),
                Err(MutationError::InvalidPolicy(message))
                    if message.contains("must not overlap")
            ));
        }

        BucketPolicy {
            immutable_prefixes: vec!["shared/child".into()],
            program_only_prefixes: vec!["shared/childish".into()],
        }
        .validate()
        .unwrap();
    }
}
