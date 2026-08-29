use std::time::Instant;

#[derive(Clone, Copy)]
pub(super) enum BulkStorePhase {
    PayloadPreparation,
    OrdinaryPathLock,
    CommitLock,
    Evaluation,
    Persistence,
    SourceSettlement,
    CapacityWait,
}

impl BulkStorePhase {
    const fn name(self) -> &'static str {
        match self {
            Self::PayloadPreparation => "payload_preparation",
            Self::OrdinaryPathLock => "ordinary_path_lock",
            Self::CommitLock => "commit_lock",
            Self::Evaluation => "operation_evaluation",
            Self::Persistence => "rocksdb_persistence",
            Self::SourceSettlement => "source_journal_settlement",
            Self::CapacityWait => "capacity_wait",
        }
    }
}

pub(super) struct BulkStorePhaseTracker {
    started: Instant,
    phase_started: Instant,
    phase: BulkStorePhase,
    operation_count: usize,
    completed: bool,
}

impl BulkStorePhaseTracker {
    pub(super) fn start(operation_count: usize) -> Self {
        let now = Instant::now();
        Self {
            started: now,
            phase_started: now,
            phase: BulkStorePhase::PayloadPreparation,
            operation_count,
            completed: false,
        }
    }

    pub(super) fn enter(&mut self, phase: BulkStorePhase) {
        self.phase = phase;
        self.phase_started = Instant::now();
    }

    pub(super) fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for BulkStorePhaseTracker {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        tracing::info!(
            bulk.phase = self.phase.name(),
            operation_count = self.operation_count,
            histogram.keldra_store_bulk_interrupted_phase_duration_seconds =
                self.phase_started.elapsed().as_secs_f64(),
            histogram.keldra_store_bulk_interrupted_duration_seconds =
                self.started.elapsed().as_secs_f64(),
            "object storage bulk execution ended before phase completion"
        );
    }
}
