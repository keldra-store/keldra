use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq as _;
use thiserror::Error;

use crate::{ObjectKey, VersionId};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatchJournalStatus {
    pub source_epoch: [u8; 32],
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
}
