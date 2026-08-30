//! Commit-local live-mask invalidation accumulation and materialization.

use super::candidate::PendingLiveMaskInvalidation;
use super::*;

struct PreparedReplacement {
    position: usize,
    identity: SegmentIdentity,
    rewrite: PreparedLiveMaskRewrite,
}

struct MaterializedLiveMasks {
    rewrites: u64,
    logical_artifacts: u64,
    logical_bytes: u64,
    cohort_calls: u64,
    encode_duration: Duration,
    cohort_duration: Duration,
}

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
        Ok(materialized) => {
            invalidations.clear();
            candidate.restore_live_mask_invalidations(invalidations);
            tracing::info!(
                index.id = definition.physical_index_id(),
                live_mask.rewrites = materialized.rewrites,
                live_mask.logical_artifacts = materialized.logical_artifacts,
                live_mask.logical_bytes = materialized.logical_bytes,
                live_mask.cohort_calls = materialized.cohort_calls,
                live_mask.encode_duration_seconds = materialized.encode_duration.as_secs_f64(),
                live_mask.cohort_duration_seconds = materialized.cohort_duration.as_secs_f64(),
                monotonic_counter.keldra_index_live_mask_rewrites_total = materialized.rewrites,
                "coalesced candidate live-mask invalidations materialized"
            );
            Ok(materialized.rewrites)
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
) -> Result<MaterializedLiveMasks, Status> {
    let directory = ManifestArtifactDirectory::new(
        dependencies.cache.clone(),
        dependencies.reader.clone(),
        definition.stored.tenant.clone(),
        definition.stored.bucket.clone(),
        definition.tenant_id,
        definition.bucket_id,
        definition.physical_index_id(),
    )
    .map_err(index_status)?;
    let routing_codec = definition
        .schema
        .codec_version(keldra_index::v4::ComponentKind::ROUTING_NODE)
        .map_err(index_status)?;
    let physical_definition = definition.physical_stored();
    let mut sink = dependencies.publisher.component_sink(
        &physical_definition,
        definition.tenant_id,
        definition.bucket_id,
        DerivedArtifactAdmission::PublicationProgress,
        PublicationCohortClass::Incremental,
    );
    let encode_started = Instant::now();
    let mut replacements = Vec::with_capacity(invalidations.len());
    let mut pack_publications = Vec::with_capacity(invalidations.len());
    let mut retained_metadata_bytes = 0usize;
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
        let rewrite = prepare_segment_live_mask(
            &directory,
            &mut sink,
            &candidate.segments[position],
            routing_codec,
            &pending.ranges,
        )
        .await
        .map_err(index_status)?;
        if rewrite.identity() != identity || rewrite.is_unchanged() {
            return Err(Status::data_loss(
                "live-mask invalidation produced an invalid prepared rewrite",
            ));
        }
        let publication = sink
            .prepare_segment_publication(identity)
            .await
            .map_err(index_status)?;
        let prepared_bytes = rewrite
            .resident_bytes()
            .and_then(|bytes| {
                bytes
                    .checked_add(publication.resident_bytes()?)
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<usize>()))
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<SegmentIdentity>()))
                    .ok_or(IndexError::OffsetOverflow)
            })
            .map_err(index_status)?;
        retained_metadata_bytes = admit_prepared_metadata(retained_metadata_bytes, prepared_bytes)?;
        replacements.push(PreparedReplacement {
            position,
            identity,
            rewrite,
        });
        pack_publications.push(publication);
    }
    let encode_duration = encode_started.elapsed();
    let published = dependencies
        .publisher
        .publish_incremental_prepared_packs(pack_publications)
        .await
        .map_err(index_status)?;
    if published.references.len() != replacements.len() {
        return Err(Status::data_loss(
            "live-mask publication result count differs from prepared rewrites",
        ));
    }
    let logical_artifacts = published.logical_artifacts;
    let logical_bytes = published.logical_bytes;
    let cohort_calls = published.cohort_calls;
    let cohort_duration = published.cohort_duration;
    let references = published.references;
    let mut finished = Vec::with_capacity(replacements.len());
    for (prepared, packs) in replacements.into_iter().zip(references) {
        finished.push((
            prepared.position,
            prepared.identity,
            prepared.rewrite.finish(packs).map_err(index_status)?,
        ));
    }
    apply_replacements(candidate, finished)?;
    Ok(MaterializedLiveMasks {
        rewrites: invalidations.len() as u64,
        logical_artifacts,
        logical_bytes,
        cohort_calls,
        encode_duration,
        cohort_duration,
    })
}

fn admit_prepared_metadata(retained: usize, measured: usize) -> Result<usize, Status> {
    // The coordinator simultaneously retains the original prepared requests,
    // a flattened clone, one scheduler-owned submitted chunk, and its
    // outcomes/ranges/ordinal maps. Four complete measured copies are a
    // conservative bound for those transient allocations.
    let charged = measured
        .checked_mul(4)
        .and_then(|bytes| retained.checked_add(bytes))
        .ok_or_else(|| Status::resource_exhausted("live-mask preparation size overflow"))?;
    if charged > LIVE_MASK_PREPARED_METADATA_BYTES {
        return Err(Status::resource_exhausted(
            "live-mask prepared metadata exhausted its charged builder workspace",
        ));
    }
    Ok(charged)
}

fn apply_replacements(
    candidate: &mut CandidateCommit,
    replacements: Vec<(usize, SegmentIdentity, SegmentDescriptor)>,
) -> Result<(), Status> {
    for (position, identity, replacement) in &replacements {
        if replacement.identity != *identity
            || candidate
                .segments
                .get(*position)
                .is_none_or(|segment| segment.identity != *identity)
        {
            return Err(Status::data_loss(
                "live-mask invalidation segment identity changed before descriptor swap",
            ));
        }
    }
    for (position, _, replacement) in replacements {
        candidate.segments[position] = replacement;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(identity: SegmentIdentity, live: u32) -> SegmentDescriptor {
        SegmentDescriptor {
            identity,
            document_count: 32,
            live_document_count: live,
            packs: Vec::new(),
            components: Vec::new(),
            encoded_bytes: 0,
            logical_bytes: 0,
        }
    }

    #[test]
    fn descriptor_swap_is_all_or_none_after_identity_revalidation() {
        let first = SegmentIdentity::new(3, 1, [1; 32], 1).unwrap();
        let second = SegmentIdentity::new(3, 2, [2; 32], 1).unwrap();
        let changed = SegmentIdentity::new(3, 2, [3; 32], 2).unwrap();
        let mut candidate = CandidateCommit::rebuild();
        candidate.segments = vec![segment(first, 32), segment(second, 32)];

        let result = apply_replacements(
            &mut candidate,
            vec![
                (0, first, segment(first, 30)),
                (1, changed, segment(changed, 28)),
            ],
        );
        assert!(result.is_err());
        assert_eq!(candidate.segments[0].live_document_count, 32);
        assert_eq!(candidate.segments[1].live_document_count, 32);
    }

    #[test]
    fn descriptor_swap_installs_every_exact_identity_together() {
        let first = SegmentIdentity::new(3, 1, [1; 32], 1).unwrap();
        let second = SegmentIdentity::new(3, 2, [2; 32], 1).unwrap();
        let mut candidate = CandidateCommit::rebuild();
        candidate.segments = vec![segment(first, 32), segment(second, 32)];
        apply_replacements(
            &mut candidate,
            vec![
                (0, first, segment(first, 30)),
                (1, second, segment(second, 28)),
            ],
        )
        .unwrap();
        assert_eq!(candidate.segments[0].live_document_count, 30);
        assert_eq!(candidate.segments[1].live_document_count, 28);
    }

    #[test]
    fn prepared_metadata_admission_holds_the_four_copy_peak_at_its_limit() {
        let measured = LIVE_MASK_PREPARED_METADATA_BYTES / 4;
        assert_eq!(
            admit_prepared_metadata(0, measured).unwrap(),
            LIVE_MASK_PREPARED_METADATA_BYTES
        );
        assert!(admit_prepared_metadata(0, measured + 1).is_err());
        assert!(admit_prepared_metadata(4, measured).is_err());
    }
}
