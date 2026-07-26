use crate::persistence::ObjectCreateOptions;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Default)]
pub struct ObjectWriteOptions {
    pub content_type: Option<String>,
    pub user_metadata: Option<JsonValue>,
    pub transaction_id: Option<String>,
    pub transaction_principal: Option<String>,
    pub storage_class_id: Option<String>,
    pub visibility: ObjectWriteVisibility,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMaintenanceVisibility {
    Deferred,
    Enqueued,
    CaughtUp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchVisibility {
    Deferred,
    Published,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthzMaterializationVisibility {
    InheritedOk,
    Materialized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryExtractionVisibility {
    HintsOnly,
    PayloadNow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexPolicySnapshotVisibility {
    Cached,
    Exact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthzRevisionVisibility {
    CurrentKnown,
    FenceExact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectWriteVisibility {
    pub indexes: IndexMaintenanceVisibility,
    pub watches: WatchVisibility,
    pub authz_materialization: AuthzMaterializationVisibility,
    pub boundary_extraction: BoundaryExtractionVisibility,
    pub index_policy_snapshot: IndexPolicySnapshotVisibility,
    pub authz_revision: AuthzRevisionVisibility,
}

impl Default for ObjectWriteVisibility {
    fn default() -> Self {
        Self {
            indexes: IndexMaintenanceVisibility::Deferred,
            watches: WatchVisibility::Deferred,
            authz_materialization: AuthzMaterializationVisibility::InheritedOk,
            boundary_extraction: BoundaryExtractionVisibility::HintsOnly,
            index_policy_snapshot: IndexPolicySnapshotVisibility::Cached,
            authz_revision: AuthzRevisionVisibility::CurrentKnown,
        }
    }
}

impl ObjectWriteVisibility {
    pub fn strict() -> Self {
        Self {
            indexes: IndexMaintenanceVisibility::Enqueued,
            watches: WatchVisibility::Published,
            authz_materialization: AuthzMaterializationVisibility::Materialized,
            boundary_extraction: BoundaryExtractionVisibility::PayloadNow,
            index_policy_snapshot: IndexPolicySnapshotVisibility::Exact,
            authz_revision: AuthzRevisionVisibility::FenceExact,
        }
    }

    pub(crate) fn persistence_options(self) -> ObjectCreateOptions {
        ObjectCreateOptions {
            exact_index_policy_snapshot: matches!(
                self.index_policy_snapshot,
                IndexPolicySnapshotVisibility::Exact
            ),
            exact_authz_revision: matches!(
                self.authz_revision,
                AuthzRevisionVisibility::FenceExact
            ),
            enqueue_index_maintenance: matches!(
                self.indexes,
                IndexMaintenanceVisibility::Enqueued | IndexMaintenanceVisibility::CaughtUp
            ),
            enqueue_metadata_compaction: matches!(
                self.indexes,
                IndexMaintenanceVisibility::Enqueued | IndexMaintenanceVisibility::CaughtUp
            ),
            journal_mutation: crate::metadata_journal::ObjectJournalMutation::Put,
        }
    }

    pub fn requires_watch_visible(self) -> bool {
        matches!(self.watches, WatchVisibility::Published)
    }

    pub(crate) fn requires_payload_boundary_extraction(self) -> bool {
        matches!(
            self.boundary_extraction,
            BoundaryExtractionVisibility::PayloadNow
        )
    }

    pub(crate) fn requires_authz_materialization(self) -> bool {
        matches!(
            self.authz_materialization,
            AuthzMaterializationVisibility::Materialized
        )
    }

    pub(crate) fn defers_write_maintenance(self) -> bool {
        matches!(self.indexes, IndexMaintenanceVisibility::Deferred)
    }
}
