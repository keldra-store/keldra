//! Bounded compaction of immutable path-locator deltas.

use std::time::Instant;

use keldra_index::v4::{
    ComponentKind, LOCATOR_COMPACTION_FAN_IN, SegmentIdentity, compact_locator_roots,
};
use tracing::Instrument;

use super::*;

pub(super) async fn compact_oldest_prefix(
    definition: &CatalogDefinition,
    kind: IndexKind,
    selection: debt::LocatorDebtSelection,
    admission: DerivedArtifactAdmission,
    candidate: &mut CandidateCommit,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    if !(2..=LOCATOR_COMPACTION_FAN_IN).contains(&selection.input_roots)
        || selection.input_roots > candidate.locator_roots.len()
    {
        return Err(Status::data_loss(
            "invalid format-v4 locator compaction selection",
        ));
    }
    let covered = candidate.locator_roots[..selection.input_roots].to_vec();
    if covered
        .windows(2)
        .any(|pair| pair[0].sequence >= pair[1].sequence)
    {
        return Err(Status::data_loss(
            "format-v4 locator roots are not strictly ordered",
        ));
    }
    let replacement_sequence = covered
        .last()
        .expect("locator compaction has at least two roots")
        .sequence;
    if candidate
        .locator_roots
        .get(selection.input_roots)
        .is_some_and(|root| root.sequence <= replacement_sequence)
    {
        return Err(Status::data_loss(
            "format-v4 locator prefix is not followed by a newer root",
        ));
    }
    let identity = SegmentIdentity::new(
        definition.stored.index_id,
        definition.object_version,
        definition.schema_fingerprint,
        dependencies
            .store
            .allocate_snowflake_id()
            .map_err(|error| Status::internal(format!("allocate compacted locator ID: {error}")))?,
    )
    .map_err(index_status)?;
    let roots = covered
        .iter()
        .map(|root| candidate.locator_stream_root(root))
        .collect::<Result<Vec<_>, _>>()?;
    let input_encoded_bytes = covered.iter().fold(0_u64, |total, root| {
        total.saturating_add(root.encoded_bytes)
    });
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
    let started = Instant::now();
    let mut sink = dependencies.publisher.component_sink(
        &definition.stored,
        definition.tenant_id,
        definition.bucket_id,
        admission,
    );
    let span = tracing::info_span!(
        "keldra.index.locator_compaction",
        index.id = definition.stored.index_id,
        tenant.id = definition.tenant_id,
        bucket.id = definition.bucket_id,
        index.kind = ?kind,
        compaction.input_roots = selection.input_roots as u64,
        compaction.input_encoded_bytes = input_encoded_bytes,
        compaction.elapsed_seconds = tracing::field::Empty,
        compaction.outcome = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    );
    sink.begin_segment(identity, &[]).map_err(index_status)?;
    let result = async {
        let published = compact_locator_roots(
            &directory,
            &mut sink,
            &roots,
            identity,
            definition
                .schema
                .codec_version(ComponentKind::PATH_LOCATOR)?,
            definition
                .schema
                .codec_version(ComponentKind::ROUTING_NODE)?,
        )
        .await?;
        let packs = sink.finalize_segment(identity).await?;
        Ok::<_, IndexError>((published, packs))
    }
    .instrument(span.clone())
    .await
    .map_err(index_status);
    let elapsed = started.elapsed().as_secs_f64();
    span.record("compaction.elapsed_seconds", elapsed);
    span.record(
        "compaction.outcome",
        if result.is_ok() {
            "completed"
        } else {
            "failed"
        },
    );
    span.record(
        "otel.status_code",
        if result.is_ok() { "ok" } else { "error" },
    );
    let (published, packs) = result?;

    candidate.locator_roots.drain(..selection.input_roots);
    candidate.locator_roots.insert(
        0,
        LocatorRoot {
            sequence: replacement_sequence,
            identity,
            artifact: published.root,
            pack_ownership: LocatorPackOwnership::Standalone(packs),
            encoded_bytes: published.encoded_bytes,
            logical_bytes: published.logical_bytes,
        },
    );
    tracing::info!(
        index.kind = ?kind,
        compaction.input_roots = selection.input_roots as u64,
        compaction.input_encoded_bytes = input_encoded_bytes,
        compaction.output_roots = 1_u64,
        compaction.output_encoded_bytes = published.encoded_bytes,
        histogram.keldra_index_locator_compaction_duration_seconds = elapsed,
        monotonic_counter.keldra_index_locator_compaction_input_bytes_total = input_encoded_bytes,
        monotonic_counter.keldra_index_locator_compaction_output_bytes_total = published.encoded_bytes,
        monotonic_counter.keldra_index_locator_compactions_total = 1_u64,
        gauge.keldra_index_locator_roots = candidate.locator_roots.len() as u64,
        "format-v4 path-locator prefix compacted"
    );
    Ok(())
}
