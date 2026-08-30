//! One bounded format-v5 writer per physical projection family.
//!
//! Logical definitions never enter this writer. A caller supplies one exact
//! family recipe union, one complete ordered source unit, and the barrier that
//! unit represents. Rebuild frames may stage immutable generations without
//! exposing them; only `finish_rebuild` installs the final `current` pointer.

use std::sync::Arc;

use keldra_index::v4::ObjectIdentity;
use keldra_index::v5::{
    ProjectionBarrier, ProjectionGeneration, ProjectionMutationBuffer, decode_projection_generation,
};
use keldra_store::VersionId;
use tonic::Status;

use super::events::IndexBarrier;
use super::projection_mapper::{ProjectionFamilyPlan, SharedProjectionMapper};
use super::publication::DerivedArtifactAdmission;
use super::publisher::{
    IndexCommitPublisher, PublishedProjectionArtifacts, PublishedProjectionGeneration,
};
use super::source::IndexSourceMutation;

const WRITER_STRIPES: usize = 64;
const MINIMUM_FAMILY_WORKSPACE_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub(crate) struct SharedProjectionFamilyWriter {
    mapper: SharedProjectionMapper,
    publisher: IndexCommitPublisher,
    stripes: Arc<[tokio::sync::Mutex<()>; WRITER_STRIPES]>,
}

pub(crate) struct StagedProjectionFamily {
    pub(crate) plan_fingerprint: [u8; 32],
    pub(crate) generation: ProjectionGeneration,
    pub(crate) generation_hash: [u8; 32],
    published: PublishedProjectionArtifacts,
}

impl SharedProjectionFamilyWriter {
    pub(crate) fn new(mapper: SharedProjectionMapper, publisher: IndexCommitPublisher) -> Self {
        Self {
            mapper,
            publisher,
            stripes: Arc::new(std::array::from_fn(|_| tokio::sync::Mutex::new(()))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn advance_visible(
        &self,
        plan: &ProjectionFamilyPlan,
        sources: Vec<IndexSourceMutation>,
        barrier: ProjectionBarrier,
        maximum_workspace_bytes: usize,
        admission: DerivedArtifactAdmission,
    ) -> Result<PublishedProjectionGeneration, Status> {
        let _family = self.stripes[stripe(plan.identity.family_id)].lock().await;
        let previous = self
            .publisher
            .load_projection_generation(
                &plan.storage_tenant,
                &plan.bucket,
                plan.identity.tenant_id,
                plan.identity.bucket_id,
                plan.identity.family_id,
            )
            .await?;
        if previous
            .as_ref()
            .is_some_and(|loaded| loaded.generation.barrier.covers(&barrier))
        {
            return Err(Status::already_exists(
                "projection family already covers the requested source barrier",
            ));
        }
        let deltas = self
            .project_sources(plan, previous.as_ref(), sources, maximum_workspace_bytes)
            .await?;
        self.publisher
            .advance_projection_generation(
                &plan.storage_tenant,
                &plan.bucket,
                plan.identity.tenant_id,
                plan.identity.bucket_id,
                plan.identity.family_id,
                previous.as_ref(),
                barrier,
                deltas,
                admission,
            )
            .await
    }

    /// Stage one bounded frame of a newly captured baseline. `current` remains
    /// untouched, so a crash or retry can expose only the preceding complete
    /// generation. Snapshot paths are globally unique, hence an initial
    /// rebuild has no predecessor state to load for a frame.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_rebuild_frame(
        &self,
        plan: &ProjectionFamilyPlan,
        previous: Option<&StagedProjectionFamily>,
        sources: Vec<IndexSourceMutation>,
        barrier: ProjectionBarrier,
        maximum_workspace_bytes: usize,
        admission: DerivedArtifactAdmission,
    ) -> Result<StagedProjectionFamily, Status> {
        let _family = self.stripes[stripe(plan.identity.family_id)].lock().await;
        if previous.is_some_and(|previous| previous.plan_fingerprint != plan.schema_fingerprint) {
            return Err(Status::aborted(
                "projection family recipes changed during its baseline rebuild",
            ));
        }
        let deltas = self
            .project_rebuild_sources(plan, sources, maximum_workspace_bytes)
            .await?;
        let prepared = self
            .publisher
            .prepare_projection_advance(
                &plan.storage_tenant,
                &plan.bucket,
                plan.identity.tenant_id,
                plan.identity.bucket_id,
                plan.identity.family_id,
                previous.map(|previous| (&previous.generation, previous.generation_hash)),
                barrier,
                deltas,
            )
            .await?;
        let generation_hash = prepared.generation.hash;
        let generation = decode_projection_generation(
            &prepared.generation.bytes,
            &prepared.generation.component_directory,
        )
        .map_err(index_status)?;
        let published = self
            .publisher
            .publish_projection_artifacts(
                &plan.storage_tenant,
                &plan.bucket,
                plan.identity.tenant_id,
                plan.identity.bucket_id,
                plan.identity.family_id,
                prepared,
                admission,
            )
            .await?;
        Ok(StagedProjectionFamily {
            plan_fingerprint: plan.schema_fingerprint,
            generation,
            generation_hash,
            published,
        })
    }

    pub(crate) async fn finish_rebuild(
        &self,
        plan: &ProjectionFamilyPlan,
        expected_current: Option<VersionId>,
        staged: StagedProjectionFamily,
        admission: DerivedArtifactAdmission,
    ) -> Result<PublishedProjectionGeneration, Status> {
        let _family = self.stripes[stripe(plan.identity.family_id)].lock().await;
        if staged.plan_fingerprint != plan.schema_fingerprint
            || staged.generation.family_id != plan.identity.family_id
        {
            return Err(Status::aborted(
                "projection family recipes changed before baseline installation",
            ));
        }
        self.publisher
            .install_projection_current(
                &plan.storage_tenant,
                &plan.bucket,
                plan.identity.tenant_id,
                plan.identity.bucket_id,
                expected_current,
                staged.published,
                admission,
            )
            .await
    }

    async fn project_sources(
        &self,
        plan: &ProjectionFamilyPlan,
        previous: Option<&super::publisher::LoadedProjectionGeneration>,
        sources: Vec<IndexSourceMutation>,
        maximum_workspace_bytes: usize,
    ) -> Result<Vec<keldra_index::v5::SealedComponentDelta>, Status> {
        let mut buffer = family_buffer(maximum_workspace_bytes)?;
        for source in sources {
            let (path, version) = source_identity(&source);
            let prior = match previous {
                Some(previous) => {
                    let states = self
                        .publisher
                        .load_projection_source_states(
                            &plan.storage_tenant,
                            &plan.bucket,
                            plan.identity.tenant_id,
                            plan.identity.bucket_id,
                            previous,
                            plan.schema
                                .recipe_fingerprints()
                                .map_err(index_status)?
                                .membership,
                            &path,
                        )
                        .await?;
                    require_state_bound(&states, maximum_workspace_bytes / 4)?;
                    states
                }
                None => Vec::new(),
            };
            let (current, _) = self
                .mapper
                .project_family(plan.identity, source, maximum_workspace_bytes / 2)
                .await?;
            buffer
                .apply_source_states(
                    plan.schema
                        .recipe_fingerprints()
                        .map_err(index_status)?
                        .membership,
                    &path,
                    version,
                    current,
                    prior,
                )
                .map_err(index_status)?;
        }
        buffer.seal().map_err(index_status)
    }

    async fn project_rebuild_sources(
        &self,
        plan: &ProjectionFamilyPlan,
        sources: Vec<IndexSourceMutation>,
        maximum_workspace_bytes: usize,
    ) -> Result<Vec<keldra_index::v5::SealedComponentDelta>, Status> {
        let mut buffer = family_buffer(maximum_workspace_bytes)?;
        let source_scope = plan
            .schema
            .recipe_fingerprints()
            .map_err(index_status)?
            .membership;
        for source in sources {
            let (path, version) = source_identity(&source);
            let (current, _) = self
                .mapper
                .project_family(plan.identity, source, maximum_workspace_bytes / 2)
                .await?;
            buffer
                .apply_source_states(source_scope, &path, version, current, Vec::new())
                .map_err(index_status)?;
        }
        buffer.seal().map_err(index_status)
    }
}

pub(crate) fn projection_barrier(barrier: &IndexBarrier) -> Result<ProjectionBarrier, Status> {
    ProjectionBarrier::new(
        barrier
            .sources
            .iter()
            .map(|(node, cursor)| (node.0, cursor.source.source_epoch, cursor.next_offset))
            .collect(),
        barrier.atomic.finalized_through(),
    )
    .map_err(index_status)
}

fn family_buffer(maximum_workspace_bytes: usize) -> Result<ProjectionMutationBuffer, Status> {
    if maximum_workspace_bytes < MINIMUM_FAMILY_WORKSPACE_BYTES {
        return Err(Status::resource_exhausted(
            "projection family has no minimum bounded workspace",
        ));
    }
    ProjectionMutationBuffer::new(maximum_workspace_bytes / 4).map_err(index_status)
}

fn source_identity(source: &IndexSourceMutation) -> (String, u64) {
    match source {
        IndexSourceMutation::Upsert(object) => (object.path.clone(), object.version),
        IndexSourceMutation::Remove(ObjectIdentity { path, version }) => (path.clone(), *version),
    }
}

fn require_state_bound(
    states: &[keldra_index::v5::ProjectedDocumentState],
    maximum_bytes: usize,
) -> Result<(), Status> {
    let resident = states.iter().try_fold(0_usize, |total, state| {
        total
            .checked_add(state.resident_bytes().map_err(index_status)?)
            .ok_or_else(|| Status::resource_exhausted("prior projected state bytes overflow"))
    })?;
    if resident > maximum_bytes {
        return Err(Status::resource_exhausted(
            "one prior projected source state exceeds its bounded workspace",
        ));
    }
    Ok(())
}

fn stripe(family_id: [u8; 32]) -> usize {
    usize::from(family_id[0]) % WRITER_STRIPES
}

fn index_status(error: keldra_index::IndexError) -> Status {
    match error {
        keldra_index::IndexError::ResourceLimit { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        keldra_index::IndexError::Io(_) => Status::unavailable(error.to_string()),
        _ => Status::data_loss(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use keldra_consensus::NodeId;
    use keldra_store::{PlacementLogId, SourceId};

    use super::*;
    use crate::index_runtime::events::{AtomicProgramWatermark, IndexSourceCursor};

    #[test]
    fn runtime_barrier_preserves_source_epochs_and_atomic_visibility() {
        let barrier = IndexBarrier {
            fence: PlacementLogId { term: 3, index: 7 },
            atomic: AtomicProgramWatermark::new(Some(11), Some(11), 0),
            sources: BTreeMap::from([(
                NodeId(4),
                IndexSourceCursor {
                    source: SourceId {
                        node_id: 4,
                        source_epoch: [9; 32],
                    },
                    next_offset: 18,
                },
            )]),
        };
        let encoded = projection_barrier(&barrier).unwrap();
        assert_eq!(encoded.source_offsets, vec![(4, [9; 32], 18)]);
        assert_eq!(encoded.atomic_through, Some(11));
    }

    #[test]
    fn another_source_epoch_never_covers_the_previous_history() {
        let first = ProjectionBarrier::new(vec![(1, [1; 32], 8)], None).unwrap();
        let next_epoch = ProjectionBarrier::new(vec![(1, [2; 32], 9)], None).unwrap();
        assert!(!next_epoch.covers(&first));
    }
}
