use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{PlacementLogId, SourceId, VersionId};

pub const MAX_DEFINITION_STATE_SCAN_RECORDS: u32 = 1_024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionKind {
    Index = 1,
    Accounting = 2,
}

impl DefinitionKind {
    pub(crate) const ALL: [Self; 2] = [Self::Index, Self::Accounting];

    pub(crate) fn from_byte(value: u8) -> Result<Self, DefinitionStateError> {
        match value {
            1 => Ok(Self::Index),
            2 => Ok(Self::Accounting),
            _ => Err(DefinitionStateError::Malformed(
                "definition kind is unknown".into(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionOperation {
    Upsert = 1,
    Delete = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DefinitionMutationIntent {
    pub kind: DefinitionKind,
    pub definition_id: u64,
}

impl DefinitionMutationIntent {
    pub fn new(kind: DefinitionKind, definition_id: u64) -> Result<Self, DefinitionStateError> {
        if definition_id == 0 {
            return Err(DefinitionStateError::Malformed(
                "definition ID must be non-zero".into(),
            ));
        }
        Ok(Self {
            kind,
            definition_id,
        })
    }

    pub fn validate(self) -> Result<(), DefinitionStateError> {
        if self.definition_id == 0 {
            Err(DefinitionStateError::Malformed(
                "definition ID must be non-zero".into(),
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionTransition {
    pub kind: DefinitionKind,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub definition_id: u64,
    pub path: String,
    pub object_version: VersionId,
    pub operation: DefinitionOperation,
}

impl DefinitionTransition {
    pub fn validate(&self) -> Result<(), DefinitionStateError> {
        validate_identity(self.tenant_id, self.bucket_id, self.definition_id)?;
        validate_path(&self.path)?;
        if self.object_version.0 == 0 {
            return Err(DefinitionStateError::Malformed(
                "definition object version must be non-zero".into(),
            ));
        }
        Ok(())
    }

    pub fn locator(&self) -> Option<DefinitionLocator> {
        (self.operation == DefinitionOperation::Upsert).then(|| DefinitionLocator {
            kind: self.kind,
            tenant_id: self.tenant_id,
            bucket_id: self.bucket_id,
            definition_id: self.definition_id,
            path: self.path.clone(),
            object_version: self.object_version,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionLocator {
    pub kind: DefinitionKind,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub definition_id: u64,
    pub path: String,
    pub object_version: VersionId,
}

impl DefinitionLocator {
    pub fn validate(&self) -> Result<(), DefinitionStateError> {
        validate_identity(self.tenant_id, self.bucket_id, self.definition_id)?;
        validate_path(&self.path)?;
        if self.object_version.0 == 0 {
            return Err(DefinitionStateError::Malformed(
                "definition object version must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefinitionLocatorCursor(pub(crate) Vec<u8>);

impl DefinitionLocatorCursor {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, DefinitionStateError> {
        crate::store::definition_state::validate_locator_cursor(&bytes)?;
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefinitionLocatorPage {
    pub locators: Vec<DefinitionLocator>,
    pub next_cursor: Option<DefinitionLocatorCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionAssignment {
    pub kind: DefinitionKind,
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub definition_id: u64,
    pub definition_path: String,
    pub object_version: VersionId,
    pub observed_fence: PlacementLogId,
    pub rank: u8,
}

impl DefinitionAssignment {
    pub fn validate(&self) -> Result<(), DefinitionStateError> {
        validate_identity(self.tenant_id, self.bucket_id, self.definition_id)?;
        validate_path(&self.definition_path)?;
        if self.object_version.0 == 0 {
            return Err(DefinitionStateError::Malformed(
                "assignment object version must be non-zero".into(),
            ));
        }
        validate_fence(self.observed_fence)?;
        if self.rank > 2 {
            return Err(DefinitionStateError::Malformed(
                "definition assignment rank must be 0, 1, or 2".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", content = "record", rename_all = "snake_case")]
pub enum DefinitionAssignmentMutation {
    Upsert(DefinitionAssignment),
    Remove {
        kind: DefinitionKind,
        tenant_id: u64,
        bucket_id: u64,
        definition_id: u64,
        object_version: VersionId,
        observed_fence: PlacementLogId,
    },
}

impl DefinitionAssignmentMutation {
    pub fn kind(&self) -> DefinitionKind {
        match self {
            Self::Upsert(assignment) => assignment.kind,
            Self::Remove { kind, .. } => *kind,
        }
    }

    pub fn object_version(&self) -> VersionId {
        match self {
            Self::Upsert(assignment) => assignment.object_version,
            Self::Remove { object_version, .. } => *object_version,
        }
    }

    pub fn observed_fence(&self) -> PlacementLogId {
        match self {
            Self::Upsert(assignment) => assignment.observed_fence,
            Self::Remove { observed_fence, .. } => *observed_fence,
        }
    }

    pub(crate) fn identity(&self) -> (DefinitionKind, u64, u64, u64) {
        match self {
            Self::Upsert(assignment) => (
                assignment.kind,
                assignment.tenant_id,
                assignment.bucket_id,
                assignment.definition_id,
            ),
            Self::Remove {
                kind,
                tenant_id,
                bucket_id,
                definition_id,
                ..
            } => (*kind, *tenant_id, *bucket_id, *definition_id),
        }
    }

    pub fn validate(&self) -> Result<(), DefinitionStateError> {
        match self {
            Self::Upsert(assignment) => assignment.validate(),
            Self::Remove {
                tenant_id,
                bucket_id,
                definition_id,
                object_version,
                observed_fence,
                ..
            } => {
                validate_identity(*tenant_id, *bucket_id, *definition_id)?;
                if object_version.0 == 0 {
                    return Err(DefinitionStateError::Malformed(
                        "assignment removal version must be non-zero".into(),
                    ));
                }
                validate_fence(*observed_fence)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefinitionAssignmentCursor(pub(crate) Vec<u8>);

impl DefinitionAssignmentCursor {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, DefinitionStateError> {
        crate::store::definition_state::validate_assignment_cursor(&bytes)?;
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefinitionAssignmentPage {
    pub assignments: Vec<DefinitionAssignment>,
    pub next_cursor: Option<DefinitionAssignmentCursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum DefinitionConsumerKind {
    /// Destination-local cursor paired atomically with assigned-state changes.
    IndexAssignments = 1,
    /// Destination-local cursor paired atomically with assigned-state changes.
    AccountingAssignments = 2,
    /// Source-local cursor advanced only after index assignment delivery.
    IndexDelivery = 3,
    /// Source-local cursor advanced only after accounting assignment delivery.
    AccountingDelivery = 4,
}

impl DefinitionConsumerKind {
    pub fn definition_kind(self) -> DefinitionKind {
        match self {
            Self::IndexAssignments | Self::IndexDelivery => DefinitionKind::Index,
            Self::AccountingAssignments | Self::AccountingDelivery => DefinitionKind::Accounting,
        }
    }

    pub(crate) fn from_byte(value: u8) -> Result<Self, DefinitionStateError> {
        match value {
            1 => Ok(Self::IndexAssignments),
            2 => Ok(Self::AccountingAssignments),
            3 => Ok(Self::IndexDelivery),
            4 => Ok(Self::AccountingDelivery),
            _ => Err(DefinitionStateError::Malformed(
                "definition consumer kind is unknown".into(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefinitionCheckpoint {
    pub consumer_kind: DefinitionConsumerKind,
    pub source_id: SourceId,
    pub next_offset: u64,
    pub observed_fence: PlacementLogId,
}

impl DefinitionCheckpoint {
    pub fn validate(self) -> Result<(), DefinitionStateError> {
        if self.source_id.node_id == 0 || self.source_id.source_epoch == [0; 32] {
            return Err(DefinitionStateError::Malformed(
                "definition checkpoint source is invalid".into(),
            ));
        }
        validate_fence(self.observed_fence)
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DefinitionStateError {
    #[error("definition-state record is malformed: {0}")]
    Malformed(String),
    #[error("definition-state cursor is invalid")]
    InvalidCursor,
    #[error("definition-state scan limit must be 1..=1024")]
    InvalidScanLimit,
    #[error("definition-state checkpoint would regress")]
    CheckpointRegression,
    #[error("definition-state membership reconciliation fence would regress")]
    ReconciliationFenceRegression,
    #[error("definition-state assignment conflicts with the checkpoint")]
    AssignmentCheckpointMismatch,
    #[error("definition-state storage failed: {0}")]
    Storage(String),
}

fn validate_identity(
    tenant_id: u64,
    bucket_id: u64,
    definition_id: u64,
) -> Result<(), DefinitionStateError> {
    if tenant_id == 0 || bucket_id == 0 || definition_id == 0 {
        return Err(DefinitionStateError::Malformed(
            "definition stable IDs must be non-zero".into(),
        ));
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<(), DefinitionStateError> {
    crate::ObjectKey::new("typed", "definitions", path)
        .map(|_| ())
        .map_err(|error| DefinitionStateError::Malformed(error.to_string()))
}

pub(crate) fn validate_fence(fence: PlacementLogId) -> Result<(), DefinitionStateError> {
    if fence.term == 0 || fence.index == 0 {
        return Err(DefinitionStateError::Malformed(
            "definition membership fence must be non-zero".into(),
        ));
    }
    Ok(())
}
