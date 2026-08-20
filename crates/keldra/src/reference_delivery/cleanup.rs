//! Derived cleanup for metadata reference proofs below source retention floors.
//!
//! A source's durable retention floor already proves that every ACTIVE
//! destination advanced through that source prefix. This module deliberately
//! persists no second watermark, acknowledgement, or cleanup cursor.

use std::collections::BTreeMap;
use std::sync::Arc;

use keldra_consensus::NodeId;
use keldra_store::{
    MAX_REFERENCE_PROOF_PRUNE_BYTES, MAX_REFERENCE_PROOF_PRUNE_RECORDS, PlacementLogId,
    ReferenceProofPruneError, Store, WatchJournalStatus,
};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActiveReferenceSource {
    pub(crate) node: NodeId,
    pub(crate) address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReferenceProofCleanupView {
    pub(crate) placement_fence: PlacementLogId,
    pub(crate) active_sources: Vec<ActiveReferenceSource>,
    pub(crate) transition_in_progress: bool,
    pub(crate) reference_reconstruction_safe: bool,
}

pub(crate) trait ReferenceProofCleanupPlacement: Send + Sync {
    fn current(&self) -> Result<ReferenceProofCleanupView, String>;
}

#[tonic::async_trait]
pub(crate) trait ReferenceProofSourceStatuses: Send + Sync {
    async fn status(&self, node: NodeId, address: &str) -> Result<WatchJournalStatus, String>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReferenceProofCleanupPause {
    MembershipTransition,
    ReferenceReconstruction,
    MissingSource(NodeId),
    PlacementChanged,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReferenceProofCleanupRun {
    pub(crate) deleted_records: u64,
    pub(crate) deleted_bytes: u64,
    pub(crate) complete: bool,
    pub(crate) pause: Option<ReferenceProofCleanupPause>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum ReferenceProofCleanupError {
    #[error("reference-proof cleanup placement is unavailable: {0}")]
    Placement(String),
    #[error("reference-proof cleanup placement is invalid: {0}")]
    InvalidPlacement(String),
    #[error("reference-proof cleanup source status is invalid: {0}")]
    InvalidSourceStatus(String),
    #[error(transparent)]
    Prune(#[from] ReferenceProofPruneError),
}

pub(crate) struct ReferenceProofCleanup {
    store: Store,
    placement: Arc<dyn ReferenceProofCleanupPlacement>,
    statuses: Arc<dyn ReferenceProofSourceStatuses>,
    max_records: u32,
    max_bytes: u64,
}

impl ReferenceProofCleanup {
    pub(crate) fn new(
        store: Store,
        placement: Arc<dyn ReferenceProofCleanupPlacement>,
        statuses: Arc<dyn ReferenceProofSourceStatuses>,
    ) -> Self {
        Self {
            store,
            placement,
            statuses,
            max_records: MAX_REFERENCE_PROOF_PRUNE_RECORDS,
            max_bytes: MAX_REFERENCE_PROOF_PRUNE_BYTES,
        }
    }

    #[cfg(test)]
    fn with_limits(mut self, max_records: u32, max_bytes: u64) -> Self {
        self.max_records = max_records;
        self.max_bytes = max_bytes;
        self
    }

    /// Captures every ACTIVE source status before deleting anything, then
    /// rechecks the exact placement and safety state before each bounded local
    /// prune. A missing source therefore pauses the complete cycle rather than
    /// producing a partial status capture.
    pub(crate) async fn run_once(
        &self,
    ) -> Result<ReferenceProofCleanupRun, ReferenceProofCleanupError> {
        let started = self
            .placement
            .current()
            .map_err(ReferenceProofCleanupError::Placement)?;
        if let Some(pause) = unsafe_pause(&started) {
            return Ok(paused(pause, ReferenceProofCleanupRun::default()));
        }
        validate_view(&started)?;

        let mut captured = BTreeMap::new();
        for source in &started.active_sources {
            let status = match self.statuses.status(source.node, &source.address).await {
                Ok(status) => status,
                Err(_) => {
                    return Ok(paused(
                        ReferenceProofCleanupPause::MissingSource(source.node),
                        ReferenceProofCleanupRun::default(),
                    ));
                }
            };
            validate_status(source.node, status)?;
            captured.insert(source.node, status);
        }

        let mut run = ReferenceProofCleanupRun {
            complete: true,
            ..ReferenceProofCleanupRun::default()
        };
        for source in &started.active_sources {
            let current = self
                .placement
                .current()
                .map_err(ReferenceProofCleanupError::Placement)?;
            if let Some(pause) = unsafe_pause(&current) {
                return Ok(paused(pause, run));
            }
            validate_view(&current)?;
            if current != started {
                return Ok(paused(ReferenceProofCleanupPause::PlacementChanged, run));
            }

            let status = captured
                .get(&source.node)
                .expect("every validated ACTIVE source has one captured status");
            if status.retention_floor == 0 {
                continue;
            }
            let page = self
                .store
                .prune_reference_proofs(
                    status.source_id,
                    status.retention_floor,
                    self.max_records,
                    self.max_bytes,
                )
                .await?;
            run.deleted_records = run
                .deleted_records
                .checked_add(u64::from(page.deleted_records))
                .ok_or_else(|| {
                    ReferenceProofCleanupError::InvalidSourceStatus(
                        "reference-proof cleanup record count overflow".into(),
                    )
                })?;
            run.deleted_bytes = run
                .deleted_bytes
                .checked_add(page.deleted_bytes)
                .ok_or_else(|| {
                    ReferenceProofCleanupError::InvalidSourceStatus(
                        "reference-proof cleanup byte count overflow".into(),
                    )
                })?;
            run.complete &= page.complete;
        }
        Ok(run)
    }
}

fn unsafe_pause(view: &ReferenceProofCleanupView) -> Option<ReferenceProofCleanupPause> {
    if view.transition_in_progress {
        return Some(ReferenceProofCleanupPause::MembershipTransition);
    }
    if !view.reference_reconstruction_safe {
        return Some(ReferenceProofCleanupPause::ReferenceReconstruction);
    }
    None
}

fn validate_view(view: &ReferenceProofCleanupView) -> Result<(), ReferenceProofCleanupError> {
    if view.placement_fence.term == 0 || view.placement_fence.index == 0 {
        return Err(ReferenceProofCleanupError::InvalidPlacement(
            "placement fence must be nonzero".into(),
        ));
    }
    if view.active_sources.is_empty() {
        return Err(ReferenceProofCleanupError::InvalidPlacement(
            "ACTIVE source set is empty".into(),
        ));
    }
    let mut previous = None;
    for source in &view.active_sources {
        if source.node.0 == 0 || source.node.0 > u64::from(u16::MAX) || source.address.is_empty() {
            return Err(ReferenceProofCleanupError::InvalidPlacement(
                "ACTIVE source identity or address is invalid".into(),
            ));
        }
        if previous.is_some_and(|previous| previous >= source.node) {
            return Err(ReferenceProofCleanupError::InvalidPlacement(
                "ACTIVE sources must be unique and ordered by node ID".into(),
            ));
        }
        previous = Some(source.node);
    }
    Ok(())
}

fn validate_status(
    expected_node: NodeId,
    status: WatchJournalStatus,
) -> Result<(), ReferenceProofCleanupError> {
    if u64::from(status.source_id.node_id) != expected_node.0
        || status.source_id.source_epoch == [0; 32]
        || status.retention_floor > status.settled_through
        || status.settled_through > status.tail
        || status.retained_entries != status.tail - status.retention_floor
    {
        return Err(ReferenceProofCleanupError::InvalidSourceStatus(format!(
            "node {} returned inconsistent source-journal coordinates",
            expected_node.0
        )));
    }
    Ok(())
}

fn paused(
    pause: ReferenceProofCleanupPause,
    mut run: ReferenceProofCleanupRun,
) -> ReferenceProofCleanupRun {
    run.complete = false;
    run.pause = Some(pause);
    run
}

#[cfg(test)]
#[path = "cleanup/tests.rs"]
mod tests;
