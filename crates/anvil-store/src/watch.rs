use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq as _;
use thiserror::Error;

use crate::{ObjectKey, ReferenceDelta, VersionId};

/// Release defaults for the one source-local 0.5.0 invalidation journal.
pub const DEFAULT_WATCH_MAX_ENTRIES: u64 = 1_000_000;
pub const DEFAULT_WATCH_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// Maximum number of source-local invalidations returned by one storage scan.
pub const MAX_LOCAL_INVALIDATION_SCAN_RECORDS: usize = 1_024;

pub(crate) const LOCAL_INVALIDATION_OFFSET_KEY: &[u8] = b"local_invalidation_offset";
pub(crate) const LOCAL_INVALIDATION_FLOOR_KEY: &[u8] = b"local_invalidation_floor";
pub(crate) const LOCAL_INVALIDATION_COUNT_KEY: &[u8] = b"local_invalidation_count";
pub(crate) const LOCAL_INVALIDATION_BYTES_KEY: &[u8] = b"local_invalidation_bytes";
pub(crate) const LOCAL_INVALIDATION_EPOCH_KEY: &[u8] = b"local_invalidation_epoch";
pub(crate) const LOCAL_INVALIDATION_TOKEN_KEY: &[u8] = b"local_invalidation_token_key";

const WATCH_TOKEN_FORMAT: u16 = 1;
const WATCH_TOKEN_MAX_ENCODED_BYTES: usize = 16 * 1024;
const LOCAL_CHANGE_FORMAT: u16 = 1;

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
}

/// One typed record in a source-local change journal.
///
/// Only object-head changes exist in 0.5.1's first storage slice. The enum is
/// deliberately non-exhaustive so later typed changes can share the same
/// ordered source journal without changing public Watch delivery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum LocalChange {
    ObjectHead(ObjectHeadChange),
    RetainedVersionDeleted(RetainedVersionDeletedChange),
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
    ) -> Self {
        Self::RetainedVersionDeleted(RetainedVersionDeletedChange {
            offset,
            tenant_id,
            bucket_id,
            exact_path,
            deleted_version,
            resulting_head_version,
            reference_deltas,
        })
    }

    pub fn offset(&self) -> u64 {
        match self {
            Self::ObjectHead(change) => change.offset,
            Self::RetainedVersionDeleted(change) => change.offset,
        }
    }

    pub fn reference_deltas(&self) -> &[ReferenceDelta] {
        match self {
            Self::ObjectHead(change) => &change.reference_deltas,
            Self::RetainedVersionDeleted(change) => &change.reference_deltas,
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
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct LocalChangeEnvelope {
    format: u16,
    change: LocalChange,
}

#[derive(Serialize)]
struct LocalChangeEnvelopeRef<'a> {
    format: u16,
    change: &'a LocalChange,
}

#[derive(Debug, Error)]
pub(crate) enum LocalChangeCodecError {
    #[error("local change record is malformed: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("unsupported local change format {0}")]
    UnsupportedFormat(u16),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StoredLocalChange {
    Current(LocalChange),
    V050(LocalInvalidation),
}

impl StoredLocalChange {
    pub(crate) fn offset(&self) -> u64 {
        match self {
            Self::Current(change) => change.offset(),
            Self::V050(invalidation) => invalidation.offset,
        }
    }
}

pub(crate) fn encode_local_change(change: &LocalChange) -> Result<Vec<u8>, LocalChangeCodecError> {
    serde_json::to_vec(&LocalChangeEnvelopeRef {
        format: LOCAL_CHANGE_FORMAT,
        change,
    })
    .map_err(Into::into)
}

pub(crate) fn decode_local_change(
    encoded: &[u8],
) -> Result<StoredLocalChange, LocalChangeCodecError> {
    let value = serde_json::from_slice::<serde_json::Value>(encoded)?;
    if value.get("format").is_some() {
        let envelope = serde_json::from_value::<LocalChangeEnvelope>(value)?;
        if envelope.format != LOCAL_CHANGE_FORMAT {
            return Err(LocalChangeCodecError::UnsupportedFormat(envelope.format));
        }
        return Ok(StoredLocalChange::Current(envelope.change));
    }

    // Anvil 0.5.0 stored the object-head invalidation directly as JSON. There
    // are only two possible records (present or deleted), both represented by
    // this exact released type.
    serde_json::from_value::<LocalInvalidation>(value)
        .map(StoredLocalChange::V050)
        .map_err(Into::into)
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
    fn current_local_changes_have_an_explicit_format_and_type_tag() {
        let expected = LocalChange::object_head(
            7,
            11,
            12,
            "documents/one".into(),
            VersionId(41),
            false,
            Vec::new(),
        );
        let encoded = encode_local_change(&expected).unwrap();
        let value = serde_json::from_slice::<serde_json::Value>(&encoded).unwrap();
        assert_eq!(value["format"], LOCAL_CHANGE_FORMAT);
        assert_eq!(value["change"]["kind"], "object_head");
        assert_eq!(
            decode_local_change(&encoded).unwrap(),
            StoredLocalChange::Current(expected)
        );
    }

    #[test]
    fn every_released_v050_invalidation_shape_decodes_as_an_object_head_change() {
        let fixtures = [
            (
                br#"{"offset":7,"key":{"tenant":"tenant","bucket":"bucket","path":"documents/one"},"minimum_path_version":41,"state_hint":"present"}"#
                    .as_slice(),
                InvalidationStateHint::Present,
            ),
            (
                br#"{"offset":8,"key":{"tenant":"tenant","bucket":"bucket","path":"documents/two"},"minimum_path_version":42,"state_hint":"deleted"}"#
                    .as_slice(),
                InvalidationStateHint::Deleted,
            ),
        ];

        for (encoded, expected_hint) in fixtures {
            let StoredLocalChange::V050(invalidation) = decode_local_change(encoded).unwrap()
            else {
                panic!("0.5.0 record decoded as a current envelope")
            };
            assert_eq!(invalidation.key.tenant(), "tenant");
            assert_eq!(invalidation.key.bucket(), "bucket");
            assert_eq!(invalidation.state_hint, expected_hint);
        }
    }

    #[test]
    fn unknown_local_change_formats_fail_instead_of_falling_back_to_v050() {
        let change = LocalChange::object_head(
            9,
            11,
            12,
            "documents/three".into(),
            VersionId(43),
            false,
            Vec::new(),
        );
        let encoded = encode_local_change(&change).unwrap();
        let mut value = serde_json::from_slice::<serde_json::Value>(&encoded).unwrap();
        value["format"] = serde_json::json!(LOCAL_CHANGE_FORMAT + 1);
        let encoded = serde_json::to_vec(&value).unwrap();
        assert!(matches!(
            decode_local_change(&encoded),
            Err(LocalChangeCodecError::UnsupportedFormat(2))
        ));
    }
}
