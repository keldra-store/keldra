//! Disposable scheduling state for an assigned-definition recovery walk.

use anvil_store::DefinitionAssignmentCursor;

use super::ASSIGNMENT_RETRY_INTERVAL;

pub(super) struct AssignmentInventoryRecovery {
    pub(super) cursor: Option<DefinitionAssignmentCursor>,
    pub(super) due: tokio::time::Instant,
}

impl AssignmentInventoryRecovery {
    pub(super) fn startup() -> Self {
        Self::immediate()
    }

    pub(super) fn immediate() -> Self {
        Self {
            cursor: None,
            due: tokio::time::Instant::now(),
        }
    }

    pub(super) fn retry() -> Self {
        Self::retry_from(None)
    }

    pub(super) fn retry_from(cursor: Option<DefinitionAssignmentCursor>) -> Self {
        Self {
            cursor,
            due: tokio::time::Instant::now() + ASSIGNMENT_RETRY_INTERVAL,
        }
    }

    pub(super) fn after_page(cursor: Option<DefinitionAssignmentCursor>) -> Option<Self> {
        cursor.map(|cursor| Self {
            cursor: Some(cursor),
            due: tokio::time::Instant::now(),
        })
    }
}
