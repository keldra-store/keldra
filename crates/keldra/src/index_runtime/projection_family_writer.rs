//! One bounded format-v5 writer per physical projection family.
//!
//! Logical definitions never enter this writer. A caller supplies one exact
//! family recipe union, one complete ordered source unit, and the barrier that
//! unit represents. Rebuild frames may stage immutable generations without
//! exposing them; only `finish_rebuild` installs the final `current` pointer.

use std::sync::Arc;

use keldra_index::v4::ObjectIdentity;
use keldra_index::v5::{
    ProjectionBarrier, ProjectionGeneration, ProjectionMutationBuffer,
    decode_projection_generation, inherit_projection_preserving_versions, query_cache_mutations,
};
use keldra_store::VersionId;
use tonic::Status;

use super::events::IndexBarrier;
use super::projection_mapper::{ProjectionFamilyPlan, SharedProjectionMapper};
use super::publication::DerivedArtifactAdmission;
use super::publisher::{
    IndexCommitPublisher, PublishedProjectionArtifacts, PublishedProjectionGeneration,
};
use super::source::IndexBuildDiagnostics;
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

pub(crate) struct StagedProjectionAdvance {
    pub(crate) plan_fingerprint: [u8; 32],
    pub(crate) generation: ProjectionGeneration,
    pub(crate) generation_hash: [u8; 32],
    expected_current: Option<VersionId>,
    published: PublishedProjectionArtifacts,
    pub(crate) cache_mutations: Vec<keldra_index::v4::build::MergeMutation>,
    pub(crate) diagnostics: IndexBuildDiagnostics,
}

struct ProjectedFamilyFrame {
    deltas: Vec<keldra_index::v5::SealedComponentDelta>,
    cache_mutations: Vec<keldra_index::v4::build::MergeMutation>,
    diagnostics: IndexBuildDiagnostics,
}

pub(crate) struct PublishedProjectionFrame {
    pub(crate) cache_mutations: Vec<keldra_index::v4::build::MergeMutation>,
    pub(crate) diagnostics: IndexBuildDiagnostics,
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
    ) -> Result<Option<PublishedProjectionGeneration>, Status> {
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
            return Ok(None);
        }
        let projected = self
            .project_sources(
                plan,
                previous.as_ref().map(|loaded| &loaded.generation),
                sources,
                maximum_workspace_bytes,
            )
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
                projected.deltas,
                admission,
            )
            .await
            .map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_incremental_frame(
        &self,
        plan: &ProjectionFamilyPlan,
        previous: Option<&StagedProjectionAdvance>,
        sources: Vec<IndexSourceMutation>,
        barrier: ProjectionBarrier,
        maximum_workspace_bytes: usize,
        admission: DerivedArtifactAdmission,
    ) -> Result<Option<StagedProjectionAdvance>, Status> {
        let _family = self.stripes[stripe(plan.identity.family_id)].lock().await;
        if previous.is_some_and(|previous| previous.plan_fingerprint != plan.schema_fingerprint) {
            return Err(Status::aborted(
                "projection family recipes changed during incremental staging",
            ));
        }
        let loaded = if previous.is_none() {
            self.publisher
                .load_projection_generation(
                    &plan.storage_tenant,
                    &plan.bucket,
                    plan.identity.tenant_id,
                    plan.identity.bucket_id,
                    plan.identity.family_id,
                )
                .await?
        } else {
            None
        };
        let predecessor = previous
            .map(|staged| (&staged.generation, staged.generation_hash))
            .or_else(|| {
                loaded
                    .as_ref()
                    .map(|loaded| (&loaded.generation, loaded.current.generation_hash))
            });
        // A loaded current covering the target means this journal unit already
        // completed. A caller-supplied staged predecessor is one earlier frame
        // of the same bounded unit and may intentionally carry the final
        // barrier while further source chunks append to its immutable roots.
        if predecessor.is_some_and(|(generation, _)| {
            projection_unit_already_completed(previous.is_some(), generation, &barrier)
        }) {
            return Ok(None);
        }
        let projected = self
            .project_sources(
                plan,
                predecessor.map(|(generation, _)| generation),
                sources,
                maximum_workspace_bytes,
            )
            .await?;
        let prepared = self
            .publisher
            .prepare_projection_advance(
                &plan.storage_tenant,
                &plan.bucket,
                plan.identity.tenant_id,
                plan.identity.bucket_id,
                plan.identity.family_id,
                predecessor,
                barrier,
                projected.deltas,
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
        Ok(Some(StagedProjectionAdvance {
            plan_fingerprint: plan.schema_fingerprint,
            generation,
            generation_hash,
            expected_current: previous
                .map(|staged| staged.expected_current)
                .unwrap_or_else(|| loaded.map(|loaded| loaded.current_object_version)),
            published,
            cache_mutations: projected.cache_mutations,
            diagnostics: projected.diagnostics,
        }))
    }

    pub(crate) async fn finish_incremental_frames(
        &self,
        plan: &ProjectionFamilyPlan,
        staged: StagedProjectionAdvance,
        admission: DerivedArtifactAdmission,
    ) -> Result<PublishedProjectionGeneration, Status> {
        let _family = self.stripes[stripe(plan.identity.family_id)].lock().await;
        if staged.plan_fingerprint != plan.schema_fingerprint
            || staged.generation.family_id != plan.identity.family_id
        {
            return Err(Status::aborted(
                "projection family recipes changed before incremental installation",
            ));
        }
        self.publisher
            .install_projection_current(
                &plan.storage_tenant,
                &plan.bucket,
                plan.identity.tenant_id,
                plan.identity.bucket_id,
                staged.expected_current,
                staged.published,
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
        let projected = self
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
                projected.deltas,
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

    /// Durably append one bounded baseline frame while the logical definition
    /// remains unbound. Equal source barriers are legal here because each
    /// snapshot frame contains a disjoint stable source range. Installing the
    /// partial family current makes rebuild restart bounded; it does not make
    /// the definition query-visible before its native cache root is complete.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn advance_rebuild_visible_frame(
        &self,
        plan: &ProjectionFamilyPlan,
        sources: Vec<IndexSourceMutation>,
        barrier: ProjectionBarrier,
        maximum_workspace_bytes: usize,
        admission: DerivedArtifactAdmission,
    ) -> Result<PublishedProjectionFrame, Status> {
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
        let projected = self
            .project_rebuild_sources(plan, sources, maximum_workspace_bytes)
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
                projected.deltas,
                admission,
            )
            .await?;
        Ok(PublishedProjectionFrame {
            cache_mutations: projected.cache_mutations,
            diagnostics: projected.diagnostics,
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
        previous: Option<&ProjectionGeneration>,
        sources: Vec<IndexSourceMutation>,
        maximum_workspace_bytes: usize,
    ) -> Result<ProjectedFamilyFrame, Status> {
        let mut buffer = family_buffer(maximum_workspace_bytes)?;
        let mut cache_mutations = Vec::new();
        let mut diagnostics = IndexBuildDiagnostics::default();
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
            let (mut current, source_diagnostics) = self
                .mapper
                .project_family(plan.identity, source, maximum_workspace_bytes / 2)
                .await?;
            inherit_projection_preserving_versions(&mut current, &prior).map_err(index_status)?;
            cache_mutations.extend(
                query_cache_mutations(&plan.schema, version, &current, &prior)
                    .map_err(index_status)?,
            );
            diagnostics.add(source_diagnostics);
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
        cache_mutations.sort_by(|left, right| {
            mutation_identity(left)
                .path
                .cmp(&mutation_identity(right).path)
        });
        if cache_mutations
            .windows(2)
            .any(|pair| mutation_identity(&pair[0]).path == mutation_identity(&pair[1]).path)
        {
            return Err(Status::data_loss(
                "projection frame produced one stable cache key twice",
            ));
        }
        Ok(ProjectedFamilyFrame {
            deltas: buffer.seal().map_err(index_status)?,
            cache_mutations,
            diagnostics,
        })
    }

    async fn project_rebuild_sources(
        &self,
        plan: &ProjectionFamilyPlan,
        sources: Vec<IndexSourceMutation>,
        maximum_workspace_bytes: usize,
    ) -> Result<ProjectedFamilyFrame, Status> {
        let mut buffer = family_buffer(maximum_workspace_bytes)?;
        let mut cache_mutations = Vec::new();
        let mut diagnostics = IndexBuildDiagnostics::default();
        let source_scope = plan
            .schema
            .recipe_fingerprints()
            .map_err(index_status)?
            .membership;
        for source in sources {
            let (path, version) = source_identity(&source);
            let (current, source_diagnostics) = self
                .mapper
                .project_family(plan.identity, source, maximum_workspace_bytes / 2)
                .await?;
            cache_mutations.extend(
                query_cache_mutations(&plan.schema, version, &current, &[])
                    .map_err(index_status)?,
            );
            diagnostics.add(source_diagnostics);
            buffer
                .apply_source_states(source_scope, &path, version, current, Vec::new())
                .map_err(index_status)?;
        }
        cache_mutations.sort_by(|left, right| {
            mutation_identity(left)
                .path
                .cmp(&mutation_identity(right).path)
        });
        if cache_mutations
            .windows(2)
            .any(|pair| mutation_identity(&pair[0]).path == mutation_identity(&pair[1]).path)
        {
            return Err(Status::data_loss(
                "rebuild projection frame produced one stable cache key twice",
            ));
        }
        Ok(ProjectedFamilyFrame {
            deltas: buffer.seal().map_err(index_status)?,
            cache_mutations,
            diagnostics,
        })
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

fn mutation_identity(mutation: &keldra_index::v4::build::MergeMutation) -> &ObjectIdentity {
    match mutation {
        keldra_index::v4::build::MergeMutation::Upsert(source) => &source.source_identity,
        keldra_index::v4::build::MergeMutation::Delete(identity) => identity,
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

fn projection_unit_already_completed(
    has_staged_predecessor: bool,
    predecessor: &ProjectionGeneration,
    target: &ProjectionBarrier,
) -> bool {
    !has_staged_predecessor && predecessor.barrier.covers(target)
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

    #[test]
    fn a_staged_frame_at_the_final_barrier_does_not_skip_later_frames() {
        let barrier = ProjectionBarrier::new(vec![(1, [1; 32], 8)], None).unwrap();
        let generation = ProjectionGeneration {
            family_id: [3; 32],
            revision: 1,
            barrier: barrier.clone(),
            roots: Vec::new(),
            previous_generation_hash: None,
        };
        assert!(projection_unit_already_completed(
            false,
            &generation,
            &barrier
        ));
        assert!(!projection_unit_already_completed(
            true,
            &generation,
            &barrier
        ));
    }
}
