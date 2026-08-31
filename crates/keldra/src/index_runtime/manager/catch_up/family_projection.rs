//! Format-v5 family projection and disposable native-cache assembly.

use super::*;
use crate::index_runtime::projection_family_writer::StagedProjectionAdvance;

#[allow(clippy::too_many_arguments)]
pub(super) async fn project_typed_json_family_unit(
    definition: &CatalogDefinition,
    plan: SegmentMemoryPlan,
    paths: &[ExactSourcePath],
    target: IndexBarrier,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
    soft_flush_allowed: bool,
    source_payload_bytes: &mut u64,
) -> Result<(), Status> {
    let target = super::super::super::projection_family_writer::projection_barrier(&target)?;
    let mut staged = None;
    if paths.is_empty() {
        stage_typed_json_family_sources(
            definition,
            plan,
            Vec::new(),
            target.clone(),
            &mut staged,
            builder,
            candidate,
            dependencies,
            soft_flush_allowed,
        )
        .await?;
    } else {
        for paths in paths.chunks(MAX_OBJECT_RECORD_EXPORT_RECORDS as usize) {
            let phase_started = Instant::now();
            let sources = load_exact_sources(definition, paths, dependencies).await?;
            timing::complete(definition, "exact_source_read", paths.len(), phase_started);
            *source_payload_bytes =
                add_source_payload_bytes(*source_payload_bytes, &definition.schema, &sources)?;
            let phase_started = Instant::now();
            stage_typed_json_family_sources(
                definition,
                plan,
                sources,
                target.clone(),
                &mut staged,
                builder,
                candidate,
                dependencies,
                soft_flush_allowed,
            )
            .await?;
            timing::complete(
                definition,
                "family_projection_and_cache_apply",
                paths.len(),
                phase_started,
            );
        }
    }
    if let Some(staged) = staged {
        finish_typed_json_family(definition, staged, dependencies).await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn stage_typed_json_family_sources(
    definition: &CatalogDefinition,
    plan: SegmentMemoryPlan,
    sources: Vec<IndexSourceMutation>,
    target: keldra_index::v5::ProjectionBarrier,
    staged: &mut Option<StagedProjectionAdvance>,
    builder: &mut NativeSegmentBuild,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
    soft_flush_allowed: bool,
) -> Result<(), Status> {
    let family_plan = dependencies
        .projection_mapper
        .family_plan(definition.projection_family_identity())?
        .ok_or_else(|| Status::failed_precondition("projection family is not registered"))?;
    let next = dependencies
        .projection_family_writer
        .stage_incremental_frame(
            &family_plan,
            staged.as_ref(),
            sources,
            target,
            plan.max_source_projection_bytes,
            DerivedArtifactAdmission::PublicationProgress,
        )
        .await?;
    let Some(mut next) = next else {
        return Ok(());
    };
    candidate.diagnostics.add(next.diagnostics);
    let cache_mutations = std::mem::take(&mut next.cache_mutations);
    if !cache_mutations.is_empty() {
        apply_incremental_mutations(
            definition,
            IndexKind::TypedJson,
            plan,
            builder,
            cache_mutations,
            candidate,
            dependencies,
            soft_flush_allowed,
        )
        .await?;
    }
    *staged = Some(next);
    Ok(())
}

pub(super) async fn finish_typed_json_family(
    definition: &CatalogDefinition,
    staged: StagedProjectionAdvance,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let family_plan = dependencies
        .projection_mapper
        .family_plan(definition.projection_family_identity())?
        .ok_or_else(|| Status::failed_precondition("projection family is not registered"))?;
    dependencies
        .projection_family_writer
        .finish_incremental_frames(
            &family_plan,
            staged,
            DerivedArtifactAdmission::PublicationProgress,
        )
        .await?;
    Ok(())
}
