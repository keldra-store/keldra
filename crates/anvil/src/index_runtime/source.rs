//! Format-neutral ordinary-object input for index projection.

use anvil_index::v4::ObjectIdentity;

#[derive(Clone, Debug)]
pub(crate) struct IndexBuildObject {
    pub path: String,
    pub version: u64,
    pub content_type: Option<String>,
    pub content_hash: [u8; 32],
    pub content_length: u64,
    pub committed_at_unix_millis: u64,
}

impl IndexBuildObject {
    pub(crate) fn identity(&self) -> ObjectIdentity {
        ObjectIdentity {
            path: self.path.clone(),
            version: self.version,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum IndexSourceMutation {
    Upsert(IndexBuildObject),
    Remove(ObjectIdentity),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct IndexBuildDiagnostics {
    pub accepted_objects: u64,
    pub skipped_objects: u64,
}

impl IndexBuildDiagnostics {
    pub(crate) fn add(&mut self, other: Self) {
        self.accepted_objects = self.accepted_objects.saturating_add(other.accepted_objects);
        self.skipped_objects = self.skipped_objects.saturating_add(other.skipped_objects);
    }
}
