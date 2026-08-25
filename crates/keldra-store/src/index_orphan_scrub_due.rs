use thiserror::Error;

use crate::VersionId;

pub const MAX_INDEX_ORPHAN_CURSOR_BYTES: usize = 16 * 1024;

/// Restart-safe progress for one low-frequency, per-definition orphan scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexOrphanScrubDue {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub index_id: u64,
    pub definition_path: String,
    pub definition_object_version: VersionId,
    pub due_at_unix_millis: u64,
    /// Placement fence of the persisted cursor. Zero means start a new scan.
    pub scan_placement_term: u64,
    pub scan_placement_index: u64,
    /// Source node currently being scanned. Zero means the first ACTIVE node.
    pub scan_node_id: u64,
    pub scan_cursor: Option<String>,
}

impl IndexOrphanScrubDue {
    pub fn validate(&self) -> Result<(), IndexOrphanScrubDueError> {
        if self.tenant_id == 0
            || self.bucket_id == 0
            || self.index_id == 0
            || self.definition_object_version.0 == 0
        {
            return Err(IndexOrphanScrubDueError::Malformed(
                "stable IDs and definition version must be non-zero".into(),
            ));
        }
        crate::ObjectKey::new("system", "definitions", &self.definition_path)
            .map_err(|error| IndexOrphanScrubDueError::Malformed(error.to_string()))?;
        let cursor_state_is_clear = self.scan_placement_term == 0
            && self.scan_placement_index == 0
            && self.scan_node_id == 0
            && self.scan_cursor.is_none();
        let cursor_state_is_set = self.scan_placement_term != 0
            && self.scan_placement_index != 0
            && self.scan_node_id != 0;
        if !cursor_state_is_clear && !cursor_state_is_set {
            return Err(IndexOrphanScrubDueError::Malformed(
                "orphan scan cursor has an incomplete placement or node identity".into(),
            ));
        }
        if self
            .scan_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_INDEX_ORPHAN_CURSOR_BYTES)
        {
            return Err(IndexOrphanScrubDueError::Malformed(
                "orphan scan cursor exceeds its bound".into(),
            ));
        }
        Ok(())
    }

    pub fn scan_is_new(&self) -> bool {
        self.scan_node_id == 0
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum IndexOrphanScrubDueError {
    #[error("index-orphan scrub due record is malformed: {0}")]
    Malformed(String),
    #[error("index-orphan scrub due storage failed: {0}")]
    Storage(String),
}
