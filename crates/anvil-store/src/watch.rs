use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq as _;
use thiserror::Error;

use crate::{DefinitionTransition, ObjectKey, ReferenceDelta, VersionId};

mod codec;

#[cfg(test)]
pub(crate) use codec::encoded_change_len;
pub(crate) use codec::{
    DecodedLocalChange, decode_local_change, decode_local_change_with_length, encode_local_change,
};

/// Release defaults for the one source-local 0.5.0 invalidation journal.
pub const DEFAULT_WATCH_MAX_ENTRIES: u64 = 1_000_000;
pub const DEFAULT_WATCH_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// Maximum number of source-local invalidations returned by one storage scan.
pub const MAX_LOCAL_INVALIDATION_SCAN_RECORDS: usize = 1_024;

pub(crate) const LOCAL_INVALIDATION_OFFSET_KEY: &[u8] = b"local_invalidation_offset";
pub(crate) const LOCAL_INVALIDATION_SETTLED_KEY: &[u8] = b"local_invalidation_settled_through";
pub(crate) const LOCAL_INVALIDATION_FLOOR_KEY: &[u8] = b"local_invalidation_floor";
pub(crate) const LOCAL_INVALIDATION_COUNT_KEY: &[u8] = b"local_invalidation_count";
pub(crate) const LOCAL_INVALIDATION_BYTES_KEY: &[u8] = b"local_invalidation_bytes";
pub(crate) const LOCAL_INVALIDATION_EPOCH_KEY: &[u8] = b"local_invalidation_epoch";
pub(crate) const LOCAL_INVALIDATION_TOKEN_KEY: &[u8] = b"local_invalidation_token_key";

const WATCH_TOKEN_FORMAT: u16 = 1;
const WATCH_TOKEN_MAX_ENCODED_BYTES: usize = 16 * 1024;
const REFERENCE_PROOF_FORMAT: u16 = 2;
const REFERENCE_PROOF_NAMESPACE: u8 = 0xff;
pub(crate) const REFERENCE_PROOF_KEY_BYTES: usize =
    1 + 1 + size_of::<u16>() + size_of::<[u8; 32]>() + size_of::<u64>();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatchRetention {
    pub max_entries: u64,
    pub max_bytes: u64,
}

impl WatchRetention {
    pub fn new(max_entries: u64, max_bytes: u64) -> Result<Self, WatchError> {
        if max_entries == 0 {
            return Err(WatchError::InvalidConfiguration(
                "watch retention max_entries must be non-zero".into(),
            ));
        }
        if max_bytes == 0 {
            return Err(WatchError::InvalidConfiguration(
                "watch retention max_bytes must be non-zero".into(),
            ));
        }
        Ok(Self {
            max_entries,
            max_bytes,
        })
    }
}

impl Default for WatchRetention {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_WATCH_MAX_ENTRIES,
            max_bytes: DEFAULT_WATCH_MAX_BYTES,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchScope {
    tenant: String,
    bucket: String,
    prefix: String,
}

impl WatchScope {
    pub fn new(
        tenant: impl Into<String>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, WatchError> {
        let tenant = tenant.into();
        let bucket = bucket.into();
        let prefix = prefix.into();
        // An empty prefix intentionally selects the whole bucket. A non-empty
        // prefix has the same canonical path shape and bound as an object key.
        let validation_path = if prefix.is_empty() {
            "_anvil/watch-scope"
        } else {
            &prefix
        };
        ObjectKey::new(&tenant, &bucket, validation_path)
            .map_err(|error| WatchError::InvalidScope(error.to_string()))?;
        Ok(Self {
            tenant,
            bucket,
            prefix,
        })
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub(crate) fn contains(&self, key: &ObjectKey) -> bool {
        key.tenant() == self.tenant
            && key.bucket() == self.bucket
            && (self.prefix.is_empty()
                || key.path() == self.prefix
                || key
                    .path()
                    .strip_prefix(&self.prefix)
                    .is_some_and(|rest| rest.starts_with('/')))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchStart {
    Now,
    RetainedBeginning,
    Resume(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatchCursor(u64);

impl WatchCursor {
    pub fn offset(self) -> u64 {
        self.0
    }

    pub(crate) fn new(offset: u64) -> Self {
        Self(offset)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchPage {
    pub invalidations: Vec<LocalInvalidation>,
    pub checkpoint: WatchCursor,
}

/// Stable identity for one node-local source journal incarnation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceId {
    pub node_id: u16,
    pub source_epoch: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatchJournalStatus {
    pub source_id: SourceId,
    /// Highest offset ever allocated, including entries already pruned.
    pub tail: u64,
    /// Highest contiguous offset whose object metadata is known to have
    /// reached its required authority. Derived consumers must never read
    /// beyond this boundary; retention and handoff continue to use `tail`.
    pub settled_through: u64,
    /// Lowest valid resume cursor. Entries through this offset were pruned.
    pub retention_floor: u64,
    pub retained_entries: u64,
    /// Logical encoded key-plus-value bytes retained in the journal. RocksDB
    /// implementation overhead is deliberately not part of the API bound.
    pub retained_bytes: u64,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum WatchError {
    #[error("invalid watch configuration: {0}")]
    InvalidConfiguration(String),
    #[error("invalid watch scope: {0}")]
    InvalidScope(String),
    #[error("invalid watch resume token")]
    InvalidResumeToken,
    #[error("RESUME_EXPIRED")]
    ResumeExpired,
    #[error("watch storage error: {0}")]
    Storage(String),
}

/// A hint about the exact path state selected by a head mutation. Consumers
/// must still reread the path; the hint is not a payload or event log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidationStateHint {
    Present,
    Deleted,
}

/// One bounded source-local invalidation. It carries no payload bytes and no
/// global sequence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalInvalidation {
    pub offset: u64,
    pub key: ObjectKey,
    pub minimum_path_version: VersionId,
    pub state_hint: InvalidationStateHint,
}

impl LocalInvalidation {
    #[cfg(test)]
    pub(crate) fn new(offset: u64, key: ObjectKey, version: VersionId, deleted: bool) -> Self {
        Self {
            offset,
            key,
            minimum_path_version: version,
            state_hint: if deleted {
                InvalidationStateHint::Deleted
            } else {
                InvalidationStateHint::Present
            },
        }
    }
}

/// The current state selected by one exact-path head mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectHeadChangeKind {
    Put,
    Delete,
}

/// Compact evidence needed by aggregate consumers to advance exact current-
/// head totals without retaining one entry per object path. Released journal
/// records omit this field; consumers must rescan and rebase when they meet
/// such an entry rather than guessing its transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountingHeadTransition {
    format: u8,
    pub previous_live_length: Option<u64>,
    pub current_live_length: Option<u64>,
}

impl AccountingHeadTransition {
    pub const FORMAT: u8 = 1;

    pub fn new(previous_live_length: Option<u64>, current_live_length: Option<u64>) -> Self {
        Self {
            format: Self::FORMAT,
            previous_live_length,
            current_live_length,
        }
    }

    pub fn validate(self) -> Result<(), &'static str> {
        if self.format == Self::FORMAT {
            Ok(())
        } else {
            Err("unsupported accounting head-transition format")
        }
    }
}

/// Stable-ID form of one source-local object-head change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectHeadChange {
    pub offset: u64,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
    pub path_version: VersionId,
    pub kind: ObjectHeadChangeKind,
    /// Exact logical content-reference effects selected by this mutation.
    /// Public watches ignore these; peer replication consumes them from the
    /// same ordered source journal.
    #[serde(default)]
    pub reference_deltas: Vec<ReferenceDelta>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accounting_transition: Option<AccountingHeadTransition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_transition: Option<DefinitionTransition>,
}

/// Deletion of one retained immutable descriptor that did not necessarily
/// move the current object head.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedVersionDeletedChange {
    pub offset: u64,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub exact_path: String,
    pub deleted_version: VersionId,
    pub resulting_head_version: Option<VersionId>,
    #[serde(default)]
    pub reference_deltas: Vec<ReferenceDelta>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accounting_transition: Option<AccountingHeadTransition>,
}

/// Typed mutable aggregate whose current state must be fetched by internal
/// catch-up consumers. Public object watches deliberately filter this out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateKind {
    ZanzibarRealm,
    LogicalRecord,
}

/// Compact invalidation for a non-object mutable aggregate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateChanged {
    pub offset: u64,
    pub aggregate_kind: AggregateKind,
    pub aggregate_key: Vec<u8>,
    pub revision: u64,
}

/// Change to one content lifecycle record. Internal handoff consumers fetch
/// the current typed state by `blob_identity`; public watches filter it out.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentLifecycleChanged {
    pub offset: u64,
    pub blob_identity: Vec<u8>,
    pub revision: u64,
    #[serde(default)]
    pub reference_deltas: Vec<ReferenceDelta>,
}

/// One typed record in a source-local change journal.
///
/// The enum is deliberately non-exhaustive so later typed changes can share
/// the same ordered source journal without changing public Watch delivery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum LocalChange {
    ObjectHead(ObjectHeadChange),
    RetainedVersionDeleted(RetainedVersionDeletedChange),
    AggregateChanged(AggregateChanged),
    ContentLifecycleChanged(ContentLifecycleChanged),
}

/// One source-local journal page admitted under an explicit encoded-byte cap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalChangePage {
    pub source_id: SourceId,
    pub changes: Vec<LocalChange>,
    pub encoded_bytes: u64,
    /// Present only when the first unread record cannot fit by itself. The
    /// record is not returned and the caller's cursor must not advance.
    pub oversize: Option<OversizeLocalChange>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OversizeLocalChange {
    pub offset: u64,
    pub encoded_bytes: u64,
}

/// Exact object-mutation evidence copied to every complete metadata replica.
///
/// This is not a second event stream or a commit marker. It retains the
/// bounded source-journal change and exact typed metadata mutation needed to
/// complete an interrupted ordinary mutation quorum. Atomic-program evidence
/// remains exact but completion stays with the nominated executor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceProof {
    format: u16,
    pub source_id: SourceId,
    pub mutation_fingerprint: [u8; 32],
    pub change: LocalChange,
    pub mutation: ReferenceProofMutation,
}

/// Replayable typed metadata operation retained under the existing proof key.
/// It contains descriptors and lineage only; payload bytes remain in the
/// ordinary inline or erasure-coded byte plane.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReferenceProofMutation {
    Object(crate::ObjectMutation),
    RetainedVersionDelete(crate::RetainedVersionDeleteMutation),
    ProgramPath(crate::ProgramPathMutation),
}

impl ReferenceProof {
    pub(crate) fn new(
        source_id: SourceId,
        mutation_fingerprint: [u8; 32],
        change: LocalChange,
        mutation: ReferenceProofMutation,
    ) -> Self {
        Self {
            format: REFERENCE_PROOF_FORMAT,
            source_id,
            mutation_fingerprint,
            change,
            mutation,
        }
    }

    pub fn offset(&self) -> u64 {
        self.change.offset()
    }

    /// Canonical proof identity in source-and-offset iterator order.
    pub fn handoff_order_key(&self) -> Vec<u8> {
        reference_proof_key(self.source_id, self.offset()).to_vec()
    }
}

impl LocalChange {
    pub(crate) fn object_head(
        offset: u64,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: String,
        path_version: VersionId,
        deleted: bool,
        reference_deltas: Vec<ReferenceDelta>,
        accounting_transition: Option<AccountingHeadTransition>,
        definition_transition: Option<DefinitionTransition>,
    ) -> Self {
        Self::ObjectHead(ObjectHeadChange {
            offset,
            tenant_id,
            bucket_id,
            exact_path,
            path_version,
            kind: if deleted {
                ObjectHeadChangeKind::Delete
            } else {
                ObjectHeadChangeKind::Put
            },
            reference_deltas,
            accounting_transition,
            definition_transition,
        })
    }

    pub(crate) fn retained_version_deleted(
        offset: u64,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: String,
        deleted_version: VersionId,
        resulting_head_version: Option<VersionId>,
        reference_deltas: Vec<ReferenceDelta>,
        accounting_transition: Option<AccountingHeadTransition>,
    ) -> Self {
        Self::RetainedVersionDeleted(RetainedVersionDeletedChange {
            offset,
            tenant_id,
            bucket_id,
            exact_path,
            deleted_version,
            resulting_head_version,
            reference_deltas,
            accounting_transition,
        })
    }

    pub(crate) fn aggregate_changed(
        offset: u64,
        aggregate_kind: AggregateKind,
        aggregate_key: Vec<u8>,
        revision: u64,
    ) -> Self {
        Self::AggregateChanged(AggregateChanged {
            offset,
            aggregate_kind,
            aggregate_key,
            revision,
        })
    }

    pub(crate) fn content_lifecycle_changed(
        offset: u64,
        blob_identity: Vec<u8>,
        revision: u64,
        reference_deltas: Vec<ReferenceDelta>,
    ) -> Self {
        Self::ContentLifecycleChanged(ContentLifecycleChanged {
            offset,
            blob_identity,
            revision,
            reference_deltas,
        })
    }

    pub fn offset(&self) -> u64 {
        match self {
            Self::ObjectHead(change) => change.offset,
            Self::RetainedVersionDeleted(change) => change.offset,
            Self::AggregateChanged(change) => change.offset,
            Self::ContentLifecycleChanged(change) => change.offset,
        }
    }

    pub fn reference_deltas(&self) -> &[ReferenceDelta] {
        match self {
            Self::ObjectHead(change) => &change.reference_deltas,
            Self::RetainedVersionDeleted(change) => &change.reference_deltas,
            Self::AggregateChanged(_) => &[],
            Self::ContentLifecycleChanged(change) => &change.reference_deltas,
        }
    }

    /// Selects the subset exposed by public WatchPrefix.
    ///
    /// The wildcard is intentional: newly added source-change variants are
    /// filtered while the caller still advances its source cursor.
    #[allow(unreachable_patterns)]
    pub fn into_object_head(self) -> Option<ObjectHeadChange> {
        match self {
            Self::ObjectHead(change) => Some(change),
            Self::RetainedVersionDeleted(_) => None,
            Self::AggregateChanged(_) => None,
            Self::ContentLifecycleChanged(_) => None,
            _ => None,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum ReferenceProofCodecError {
    #[error("reference proof is malformed: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("unsupported reference proof format {0}")]
    UnsupportedFormat(u16),
}

pub(crate) fn encode_reference_proof(
    proof: &ReferenceProof,
) -> Result<Vec<u8>, ReferenceProofCodecError> {
    serde_json::to_vec(proof).map_err(Into::into)
}

pub(crate) fn decode_reference_proof(
    encoded: &[u8],
) -> Result<ReferenceProof, ReferenceProofCodecError> {
    let proof = serde_json::from_slice::<ReferenceProof>(encoded)?;
    if proof.format != REFERENCE_PROOF_FORMAT {
        return Err(ReferenceProofCodecError::UnsupportedFormat(proof.format));
    }
    Ok(proof)
}

#[derive(Debug, Serialize, Deserialize)]
struct WatchTokenClaims {
    format: u16,
    source_epoch: [u8; 32],
    tenant: String,
    bucket: String,
    prefix: String,
    retention_max_entries: u64,
    retention_max_bytes: u64,
    offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SignedWatchToken {
    claims: WatchTokenClaims,
    mac: [u8; 32],
}

pub(crate) fn encode_resume_token(
    scope: &WatchScope,
    cursor: WatchCursor,
    source_epoch: [u8; 32],
    token_key: &[u8; 32],
    retention: WatchRetention,
) -> Result<Vec<u8>, WatchError> {
    let claims = WatchTokenClaims {
        format: WATCH_TOKEN_FORMAT,
        source_epoch,
        tenant: scope.tenant.clone(),
        bucket: scope.bucket.clone(),
        prefix: scope.prefix.clone(),
        retention_max_entries: retention.max_entries,
        retention_max_bytes: retention.max_bytes,
        offset: cursor.0,
    };
    let claims_bytes = serde_json::to_vec(&claims).map_err(watch_storage_error)?;
    let token = SignedWatchToken {
        mac: *blake3::keyed_hash(token_key, &claims_bytes).as_bytes(),
        claims,
    };
    let encoded = serde_json::to_vec(&token).map_err(watch_storage_error)?;
    Ok(URL_SAFE_NO_PAD.encode(encoded).into_bytes())
}

pub(crate) fn decode_resume_token(
    encoded: &[u8],
    scope: &WatchScope,
    source_epoch: [u8; 32],
    token_key: &[u8; 32],
    retention: WatchRetention,
) -> Result<WatchCursor, WatchError> {
    if encoded.is_empty() || encoded.len() > WATCH_TOKEN_MAX_ENCODED_BYTES {
        return Err(WatchError::InvalidResumeToken);
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| WatchError::InvalidResumeToken)?;
    let token = serde_json::from_slice::<SignedWatchToken>(&decoded)
        .map_err(|_| WatchError::InvalidResumeToken)?;
    let claims_bytes = serde_json::to_vec(&token.claims).map_err(watch_storage_error)?;
    let expected = blake3::keyed_hash(token_key, &claims_bytes);
    if !bool::from(expected.as_bytes().ct_eq(&token.mac)) {
        return Err(WatchError::InvalidResumeToken);
    }
    if token.claims.format != WATCH_TOKEN_FORMAT
        || token.claims.tenant != scope.tenant
        || token.claims.bucket != scope.bucket
        || token.claims.prefix != scope.prefix
    {
        return Err(WatchError::InvalidResumeToken);
    }
    if token.claims.source_epoch != source_epoch
        || token.claims.retention_max_entries != retention.max_entries
        || token.claims.retention_max_bytes != retention.max_bytes
    {
        return Err(WatchError::ResumeExpired);
    }
    Ok(WatchCursor(token.claims.offset))
}

pub(crate) fn invalidation_key(offset: u64) -> [u8; size_of::<u64>()] {
    offset.to_be_bytes()
}

/// Fixed-width proof key in the existing local-invalidation column family:
/// storage format, proof namespace, source node, source epoch, source offset.
pub(crate) fn reference_proof_key(
    source: SourceId,
    offset: u64,
) -> [u8; REFERENCE_PROOF_KEY_BYTES] {
    let mut key = [0_u8; REFERENCE_PROOF_KEY_BYTES];
    key[0] = crate::key::STORAGE_KEY_FORMAT_VERSION;
    key[1] = REFERENCE_PROOF_NAMESPACE;
    key[2..4].copy_from_slice(&source.node_id.to_be_bytes());
    key[4..36].copy_from_slice(&source.source_epoch);
    key[36..44].copy_from_slice(&offset.to_be_bytes());
    key
}

pub(crate) fn invalidation_record_bytes(encoded_value_bytes: usize) -> u64 {
    (size_of::<u64>() as u64).saturating_add(encoded_value_bytes as u64)
}

pub(crate) fn offset_from_key(key: &[u8]) -> Option<u64> {
    let bytes: [u8; size_of::<u64>()] = key.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

fn watch_storage_error(error: impl std::fmt::Display) -> WatchError {
    WatchError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> WatchScope {
        WatchScope::new("tenant", "bucket", "documents").unwrap()
    }

    #[test]
    fn tokens_are_integrity_protected_and_bound_to_the_exact_scope() {
        let key = [7_u8; 32];
        let epoch = [8_u8; 32];
        let retention = WatchRetention::new(10, 1_000).unwrap();
        let token =
            encode_resume_token(&scope(), WatchCursor::new(42), epoch, &key, retention).unwrap();
        assert_eq!(
            decode_resume_token(&token, &scope(), epoch, &key, retention).unwrap(),
            WatchCursor::new(42)
        );

        for different in [
            WatchScope::new("other", "bucket", "documents").unwrap(),
            WatchScope::new("tenant", "other", "documents").unwrap(),
            WatchScope::new("tenant", "bucket", "other").unwrap(),
        ] {
            assert_eq!(
                decode_resume_token(&token, &different, epoch, &key, retention).unwrap_err(),
                WatchError::InvalidResumeToken
            );
        }

        let mut tampered = token.clone();
        let last = tampered.last_mut().unwrap();
        *last = if *last == b'A' { b'B' } else { b'A' };
        assert_eq!(
            decode_resume_token(&tampered, &scope(), epoch, &key, retention).unwrap_err(),
            WatchError::InvalidResumeToken
        );
    }

    #[test]
    fn epoch_or_retention_window_change_expires_an_authentic_token() {
        let key = [7_u8; 32];
        let epoch = [8_u8; 32];
        let retention = WatchRetention::new(10, 1_000).unwrap();
        let token =
            encode_resume_token(&scope(), WatchCursor::new(42), epoch, &key, retention).unwrap();
        assert_eq!(
            decode_resume_token(&token, &scope(), [9; 32], &key, retention).unwrap_err(),
            WatchError::ResumeExpired
        );
        assert_eq!(
            decode_resume_token(
                &token,
                &scope(),
                epoch,
                &key,
                WatchRetention::new(11, 1_000).unwrap(),
            )
            .unwrap_err(),
            WatchError::ResumeExpired
        );
    }

    #[test]
    fn prefix_matching_uses_path_segment_boundaries() {
        let scope = scope();
        assert!(scope.contains(&ObjectKey::new("tenant", "bucket", "documents").unwrap()));
        assert!(scope.contains(&ObjectKey::new("tenant", "bucket", "documents/one").unwrap()));
        assert!(!scope.contains(&ObjectKey::new("tenant", "bucket", "documents-old").unwrap()));
    }

    #[test]
    fn retention_limits_are_hard_nonzero_configuration() {
        assert!(matches!(
            WatchRetention::new(0, 1).unwrap_err(),
            WatchError::InvalidConfiguration(_)
        ));
        assert!(matches!(
            WatchRetention::new(1, 0).unwrap_err(),
            WatchError::InvalidConfiguration(_)
        ));
    }

    #[test]
    fn current_local_changes_have_an_explicit_binary_format_and_type_tag() {
        let expected = LocalChange::object_head(
            7,
            11,
            12,
            "documents/one".into(),
            VersionId(41),
            false,
            Vec::new(),
            None,
            None,
        );
        let encoded = encode_local_change(&expected).unwrap();
        assert_eq!(&encoded[..4], b"ANVJ");
        assert_eq!(u16::from_be_bytes(encoded[4..6].try_into().unwrap()), 2);
        assert_eq!(encoded[6], 1);
        assert_eq!(decode_local_change(&encoded).unwrap(), expected);
    }

    #[test]
    fn unknown_local_change_formats_fail_closed() {
        let change = LocalChange::object_head(
            9,
            11,
            12,
            "documents/three".into(),
            VersionId(43),
            false,
            Vec::new(),
            None,
            None,
        );
        let mut encoded = encode_local_change(&change).unwrap();
        encoded[5] = 3;
        assert!(matches!(
            decode_local_change(&encoded),
            Err(codec::LocalChangeCodecError::UnsupportedFormat(3))
        ));
    }
}
