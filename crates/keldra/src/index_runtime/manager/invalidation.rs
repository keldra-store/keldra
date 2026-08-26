//! Commit-local live-mask invalidation accumulation and materialization.

use super::candidate::PendingLiveMaskInvalidation;
use super::*;

pub(super) async fn materialize_pending_live_masks(
    definition: &CatalogDefinition,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
) -> Result<u64, Status> {
    if !candidate.has_pending_live_mask_invalidations() {
        return Ok(0);
    }
    let mut invalidations = candidate.take_live_mask_invalidations();
    let result = materialize(definition, candidate, &invalidations, dependencies).await;
    match result {
        Ok(rewrites) => {
            invalidations.clear();
            candidate.restore_live_mask_invalidations(invalidations);
            tracing::debug!(
                index.id = definition.stored.index_id,
                live_mask.rewrites = rewrites,
                monotonic_counter.keldra_index_live_mask_rewrites_total = rewrites,
                "coalesced candidate live-mask invalidations materialized"
            );
            Ok(rewrites)
        }
        Err(error) => {
            candidate.restore_live_mask_invalidations(invalidations);
            Err(error)
        }
    }
}

async fn materialize(
    definition: &CatalogDefinition,
    candidate: &mut CandidateCommit,
    invalidations: &[PendingLiveMaskInvalidation],
    dependencies: &IndexBuilderDependencies,
) -> Result<u64, Status> {
    let directory = ManifestArtifactDirectory::new(
        dependencies.cache.clone(),
        dependencies.reader.clone(),
        definition.stored.tenant.clone(),
        definition.stored.bucket.clone(),
        definition.tenant_id,
        definition.bucket_id,
        definition.stored.index_id,
    )
    .map_err(index_status)?;
    let routing_codec = definition
        .schema
        .codec_version(keldra_index::v4::ComponentKind::ROUTING_NODE)
        .map_err(index_status)?;
    let mut sink = dependencies.publisher.component_sink(
        &definition.stored,
        definition.tenant_id,
        definition.bucket_id,
        DerivedArtifactAdmission::PublicationProgress,
        PublicationCohortClass::Incremental,
    );
    let mut replacements = Vec::with_capacity(invalidations.len());
    for pending in invalidations {
        let identity = pending.identity;
        let position = candidate
            .segments
            .binary_search_by_key(&identity.segment_id, |segment| segment.identity.segment_id)
            .map_err(|_| Status::data_loss("live-mask invalidation names a missing segment"))?;
        if candidate.segments[position].identity != identity {
            return Err(Status::data_loss(
                "live-mask invalidation segment identity changed before materialization",
            ));
        }
        let replacement = rewrite_segment_live_mask(
            &directory,
            &mut sink,
            &candidate.segments[position],
            routing_codec,
            &pending.ranges,
        )
        .await
        .map_err(index_status)?;
        replacements.push((position, replacement));
    }
    let rewrites = replacements.len() as u64;
    for (position, replacement) in replacements {
        candidate.segments[position] = replacement;
    }
    Ok(rewrites)
}
