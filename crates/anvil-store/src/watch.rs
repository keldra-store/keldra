use serde::{Deserialize, Serialize};

use crate::{ObjectKey, VersionId};

/// Maximum number of source-local invalidations returned by one internal scan.
///
/// This bounds one storage call without defining a public watch protocol,
/// retention window, source epoch, or topology contract.
pub const MAX_LOCAL_INVALIDATION_SCAN_RECORDS: usize = 1_024;

/// A hint about the exact path state selected by an ordinary head mutation.
/// Consumers must still reread the path; the hint is not a payload or event log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidationStateHint {
    Present,
    Deleted,
}

/// One bounded source-local invalidation.
///
/// Object key components are bounded by [`ObjectKey`] validation. The record
/// deliberately carries no payload bytes, source identity, epoch, or global
/// sequence.
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

pub(crate) fn invalidation_key(offset: u64) -> [u8; size_of::<u64>()] {
    offset.to_be_bytes()
}

pub(crate) fn offset_from_key(key: &[u8]) -> Option<u64> {
    let bytes: [u8; size_of::<u64>()] = key.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}
