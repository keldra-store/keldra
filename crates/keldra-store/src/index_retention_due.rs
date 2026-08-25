use thiserror::Error;

use crate::VersionId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexCommitRetentionDue {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub index_id: u64,
    pub definition_path: String,
    pub definition_object_version: VersionId,
    pub commit_revision: u64,
    pub due_at_unix_millis: u64,
}

impl IndexCommitRetentionDue {
    pub fn validate(&self) -> Result<(), IndexRetentionDueError> {
        validate_common(
            self.tenant_id,
            self.bucket_id,
            self.index_id,
            &self.definition_path,
            self.definition_object_version,
        )?;
        if self.commit_revision == 0 {
            return Err(IndexRetentionDueError::Malformed(
                "index commit revision must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

/// Durable handoff from definition lifecycle into scoped index cleanup.
///
/// This is evidence that cleanup is due, not evidence that deletion is safe.
/// A cleanup worker must exact-read the ordinary definition tombstone/version
/// before deleting any index artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeletedDefinitionCleanup {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub index_id: u64,
    pub definition_path: String,
    pub definition_object_version: VersionId,
    pub due_at_unix_millis: u64,
}

impl DeletedDefinitionCleanup {
    pub fn validate(&self) -> Result<(), IndexRetentionDueError> {
        validate_common(
            self.tenant_id,
            self.bucket_id,
            self.index_id,
            &self.definition_path,
            self.definition_object_version,
        )
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum IndexRetentionDueError {
    #[error("index-retention due record is malformed: {0}")]
    Malformed(String),
    #[error("index-retention due storage failed: {0}")]
    Storage(String),
}

fn validate_common(
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
    definition_path: &str,
    definition_object_version: VersionId,
) -> Result<(), IndexRetentionDueError> {
    if tenant_id == 0 || bucket_id == 0 || index_id == 0 {
        return Err(IndexRetentionDueError::Malformed(
            "stable IDs must be non-zero".into(),
        ));
    }
    if definition_object_version.0 == 0 {
        return Err(IndexRetentionDueError::Malformed(
            "definition object version must be non-zero".into(),
        ));
    }
    crate::ObjectKey::new("system", "definitions", definition_path)
        .map(|_| ())
        .map_err(|error| IndexRetentionDueError::Malformed(error.to_string()))
}
