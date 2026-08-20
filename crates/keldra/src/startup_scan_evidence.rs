//! Measured evidence that startup did not inventory unbounded data planes.
//!
//! The counters cover the server-owned object-head, index-artifact, blob, and
//! cache scan entrypoints that can run while the public service is assembled.
//! Those entrypoints classify work as scoped or global; only global work
//! contributes to the release gate.

use std::sync::{Arc, Mutex};

#[derive(Clone, Copy)]
pub(crate) enum StartupScanKind {
    ObjectHeads,
    IndexArtifacts,
    Blobs,
    Cache,
}

#[derive(Clone, Copy)]
pub(crate) enum StartupScanExtent {
    Scoped,
    Global,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct StartupScanCounts {
    pub(crate) global_object_head_scans_total: u64,
    pub(crate) global_index_artifact_scans_total: u64,
    pub(crate) global_blob_scans_total: u64,
    pub(crate) global_cache_scans_total: u64,
}

#[derive(Default)]
struct StartupScanState {
    active: bool,
    counts: StartupScanCounts,
}

#[derive(Clone)]
pub(crate) struct StartupScanEvidence {
    state: Arc<Mutex<StartupScanState>>,
}

impl StartupScanEvidence {
    pub(crate) fn begin() -> Self {
        Self {
            state: Arc::new(Mutex::new(StartupScanState {
                active: true,
                counts: StartupScanCounts::default(),
            })),
        }
    }

    pub(crate) fn record(&self, kind: StartupScanKind, extent: StartupScanExtent) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.active || matches!(extent, StartupScanExtent::Scoped) {
            return;
        }
        let counter = match kind {
            StartupScanKind::ObjectHeads => &mut state.counts.global_object_head_scans_total,
            StartupScanKind::IndexArtifacts => &mut state.counts.global_index_artifact_scans_total,
            StartupScanKind::Blobs => &mut state.counts.global_blob_scans_total,
            StartupScanKind::Cache => &mut state.counts.global_cache_scans_total,
        };
        *counter = counter.saturating_add(1);
    }

    /// Close the measured startup interval after the public socket is bound but
    /// before its accept task is spawned. Later bounded maintenance is
    /// intentionally outside this evidence window.
    pub(crate) fn finish(&self) -> StartupScanCounts {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = false;
        state.counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_global_scans_during_the_startup_window_are_counted() {
        let evidence = StartupScanEvidence::begin();
        for kind in [
            StartupScanKind::ObjectHeads,
            StartupScanKind::IndexArtifacts,
            StartupScanKind::Blobs,
            StartupScanKind::Cache,
        ] {
            evidence.record(kind, StartupScanExtent::Scoped);
            evidence.record(kind, StartupScanExtent::Global);
        }

        assert_eq!(
            evidence.finish(),
            StartupScanCounts {
                global_object_head_scans_total: 1,
                global_index_artifact_scans_total: 1,
                global_blob_scans_total: 1,
                global_cache_scans_total: 1,
            }
        );

        evidence.record(StartupScanKind::ObjectHeads, StartupScanExtent::Global);
        assert_eq!(evidence.finish().global_object_head_scans_total, 1);
    }
}
