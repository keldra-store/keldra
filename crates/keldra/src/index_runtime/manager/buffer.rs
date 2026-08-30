//! Active and frozen segment-buffer state.

use super::*;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ProjectionExecution {
    pub(super) queue_seconds: f64,
    pub(super) cpu_seconds: f64,
}

pub(super) struct NativeSegmentBuild {
    pub(super) writer: NativeSegmentWriter,
    pub(super) plan: SegmentMemoryPlan,
    pub(super) maximum_operations: u64,
    pub(super) started: Option<BufferAge>,
    pub(super) runnable: BuilderRunnableClock,
    pub(super) source_paths: BTreeMap<String, u64>,
    pub(super) frozen: Option<FrozenSegmentTask>,
    pub(super) publication_lane: SegmentPublicationLane,
    pub(super) maximum_segments: usize,
}

#[derive(Clone, Copy)]
pub(super) enum SegmentPublicationLane {
    Incremental,
    Maintenance,
}

impl SegmentPublicationLane {
    pub(super) const fn cohort_class(self) -> PublicationCohortClass {
        match self {
            Self::Incremental => PublicationCohortClass::Incremental,
            Self::Maintenance => PublicationCohortClass::Maintenance,
        }
    }
}

pub(super) struct FrozenSegmentTask {
    pub(super) task: AbortOnDropTask<Result<FrozenSegment, Status>>,
    pub(super) source_paths: BTreeMap<String, u64>,
    pub(super) resident_charge: u64,
}

pub(super) struct FrozenSegment {
    pub(super) built: BuiltSegment,
    pub(super) resident_bytes: u64,
    pub(super) seal_workspace_bytes: u64,
}

impl NativeSegmentBuild {
    pub(super) fn new(
        job: &BuilderJob,
        plan: SegmentMemoryPlan,
        publication_lane: SegmentPublicationLane,
        dependencies: &IndexBuilderDependencies,
    ) -> Result<Self, Status> {
        Self::open(
            &job.definition,
            plan,
            publication_lane,
            job.runnable.clone(),
            dependencies,
        )
    }

    pub(super) fn open(
        definition: &CatalogDefinition,
        plan: SegmentMemoryPlan,
        publication_lane: SegmentPublicationLane,
        runnable: BuilderRunnableClock,
        dependencies: &IndexBuilderDependencies,
    ) -> Result<Self, Status> {
        Self::open_with_segment_limit(
            definition,
            plan,
            publication_lane,
            MAX_SEGMENTS_PER_COMMIT,
            runnable,
            dependencies,
        )
    }

    pub(super) fn open_with_segment_limit(
        definition: &CatalogDefinition,
        plan: SegmentMemoryPlan,
        publication_lane: SegmentPublicationLane,
        maximum_segments: usize,
        runnable: BuilderRunnableClock,
        dependencies: &IndexBuilderDependencies,
    ) -> Result<Self, Status> {
        let segment_id = dependencies
            .store
            .allocate_snowflake_id()
            .map_err(|error| Status::internal(format!("allocate index segment ID: {error}")))?;
        let identity = SegmentIdentity::new(
            definition.physical_index_id(),
            definition.physical_definition_version(),
            definition.schema_fingerprint,
            segment_id,
        )
        .map_err(index_status)?;
        let limits = BuildLimits::with_resident_limits(
            plan.total_bytes,
            plan.max_resident_bytes,
            FIXED_INDEX_SEAL_WORKSPACE_BYTES,
        )
        .map_err(index_status)?;
        let writer = NativeSegmentWriter::new(identity, definition.schema.clone(), limits)
            .map_err(index_status)?;
        Ok(Self {
            writer,
            plan,
            maximum_operations: dependencies
                .config
                .segment_flush_max_operations(runtime_kind(definition.schema.kind)),
            started: None,
            runnable,
            source_paths: BTreeMap::new(),
            frozen: None,
            publication_lane,
            maximum_segments,
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.writer.source_count() == 0
    }

    pub(super) fn frozen_resident_charge(&self) -> u64 {
        self.frozen
            .as_ref()
            .map_or(0, |frozen| frozen.resident_charge)
    }
}
