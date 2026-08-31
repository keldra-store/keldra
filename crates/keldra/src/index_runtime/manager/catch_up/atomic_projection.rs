use super::*;

pub(crate) struct AtomicProjectionWork {
    pub(super) cursor: u64,
    pub(super) bundle_hash: keldra_store::PreparedBundleHash,
    pub(super) paths: Vec<ExactSourcePath>,
    pub(super) next_path: usize,
    pub(super) staged: CandidateCommit,
    pub(super) builder: NativeSegmentBuild,
    pub(super) plan: SegmentMemoryPlan,
    pub(super) source_payload_bytes: u64,
    pub(super) projection_target: Option<keldra_index::v5::ProjectionBarrier>,
    pub(super) projection_staged:
        Option<crate::index_runtime::projection_family_writer::StagedProjectionAdvance>,
    pub(super) phase: AtomicProjectionPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AtomicProjectionPhase {
    Project,
    Flush,
    Compact,
    Done,
}

pub(super) fn transient_atomic_projection_error(error: &Status) -> bool {
    matches!(
        error.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    )
}
