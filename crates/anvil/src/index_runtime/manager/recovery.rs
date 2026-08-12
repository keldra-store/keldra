use tonic::{Code, Status};

use super::{BUILDER_RETRY_INTERVAL, BuilderDisposition, BuilderJob, BuilderPhase, BuilderStep};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BuilderFailurePhase {
    Inspect,
    Rebuild,
    CatchUp,
    Publish,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BuilderFailureRecovery {
    Preserve,
    Reinspect,
    ScopedRebuild,
    FailClosed,
}

pub(super) fn failure_recovery(
    phase: BuilderFailurePhase,
    error: &Status,
) -> BuilderFailureRecovery {
    match error.code() {
        Code::FailedPrecondition
            if matches!(
                phase,
                BuilderFailurePhase::Rebuild | BuilderFailurePhase::CatchUp
            ) =>
        {
            BuilderFailureRecovery::ScopedRebuild
        }
        Code::FailedPrecondition => BuilderFailureRecovery::Reinspect,
        Code::Aborted if phase != BuilderFailurePhase::Rebuild => BuilderFailureRecovery::Reinspect,
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::Unknown => {
            if phase == BuilderFailurePhase::Rebuild {
                // Snapshot streams are ephemeral and cannot resume after a
                // terminal transport error. Restart only this definition's
                // scoped baseline; never broaden the scan.
                BuilderFailureRecovery::ScopedRebuild
            } else {
                BuilderFailureRecovery::Preserve
            }
        }
        Code::Aborted => BuilderFailureRecovery::ScopedRebuild,
        Code::InvalidArgument
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::ResourceExhausted
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::Internal
        | Code::DataLoss
        | Code::Unauthenticated
        | Code::Ok => BuilderFailureRecovery::FailClosed,
    }
}

pub(super) fn recover_builder_failure(
    mut job: BuilderJob,
    phase: BuilderFailurePhase,
    retry_phase: Option<BuilderPhase>,
    error: Status,
) -> BuilderStep {
    let recovery = failure_recovery(phase, &error);
    tracing::warn!(
        index.id = job.definition.stored.index_id,
        index.kind = ?job.kind,
        ?phase,
        ?recovery,
        %error,
        "bounded index build quantum failed; prior generation remains current"
    );
    tracing::info!(
        index.kind = ?job.kind,
        builder.phase = ?phase,
        monotonic_counter.anvil_index_builder_failures_total = 1_u64,
        "index builder failure observed"
    );
    tracing::info!(
        index.kind = ?job.kind,
        recovery.action = ?recovery,
        monotonic_counter.anvil_index_builder_recoveries_total = 1_u64,
        "index builder recovery selected"
    );
    let disposition = match recovery {
        BuilderFailureRecovery::Preserve => {
            emit_retry(job.kind, recovery);
            job.phase = retry_phase.expect("preservable builder phase has retry state");
            BuilderDisposition::Retry(BUILDER_RETRY_INTERVAL)
        }
        BuilderFailureRecovery::Reinspect => {
            emit_retry(job.kind, recovery);
            job.phase = BuilderPhase::Inspect;
            BuilderDisposition::Retry(BUILDER_RETRY_INTERVAL)
        }
        BuilderFailureRecovery::ScopedRebuild => {
            emit_retry(job.kind, recovery);
            job.phase = BuilderPhase::Inspect;
            job.force_snapshot_rebuild = true;
            BuilderDisposition::Retry(BUILDER_RETRY_INTERVAL)
        }
        BuilderFailureRecovery::FailClosed => {
            tracing::info!(
                index.kind = ?job.kind,
                monotonic_counter.anvil_index_builder_failed_closed_total = 1_u64,
                "index builder failed closed"
            );
            job.phase = BuilderPhase::Inspect;
            BuilderDisposition::Failed
        }
    };
    BuilderStep {
        job,
        disposition,
        retention_current: None,
    }
}

fn emit_retry(kind: anvil_index::IndexKind, recovery: BuilderFailureRecovery) {
    tracing::info!(
        index.kind = ?kind,
        recovery.action = ?recovery,
        monotonic_counter.anvil_index_builder_retries_total = 1_u64,
        "index builder retry scheduled"
    );
}
