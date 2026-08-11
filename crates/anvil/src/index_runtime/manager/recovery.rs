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
    let disposition = match recovery {
        BuilderFailureRecovery::Preserve => {
            job.phase = retry_phase.expect("preservable builder phase has retry state");
            BuilderDisposition::Retry(BUILDER_RETRY_INTERVAL)
        }
        BuilderFailureRecovery::Reinspect => {
            job.phase = BuilderPhase::Inspect;
            job.observed = None;
            BuilderDisposition::Retry(BUILDER_RETRY_INTERVAL)
        }
        BuilderFailureRecovery::ScopedRebuild => {
            job.phase = BuilderPhase::Inspect;
            job.observed = None;
            job.force_snapshot_rebuild = true;
            BuilderDisposition::Retry(BUILDER_RETRY_INTERVAL)
        }
        BuilderFailureRecovery::FailClosed => {
            job.phase = BuilderPhase::Inspect;
            job.observed = None;
            BuilderDisposition::Failed
        }
    };
    BuilderStep {
        job,
        disposition,
        retention_current: None,
    }
}
