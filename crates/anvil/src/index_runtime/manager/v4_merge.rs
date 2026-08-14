//! Manager boundary for storage-neutral format-v4 segment merging.

use std::collections::BTreeSet;
use std::time::Instant;

use anvil_index::compaction::{CompactionParallelism, CompactionProgress};
use anvil_index::v4::build::{BuildLimits, merge_segments};

use super::super::cpu::IndexCompactionExecutor;
use super::super::telemetry::{
    CompactionInputTotals, CompactionTelemetry, IndexTelemetryIdentity,
    await_with_compaction_heartbeats,
};
use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn compact_selected_segments(
    definition: &CatalogDefinition,
    kind: IndexKind,
    selection: DebtSelection,
    leased_bytes: u64,
    admission: DerivedArtifactAdmission,
    candidate: &mut CandidateGeneration,
    dependencies: &IndexBuilderDependencies,
) -> Result<(), Status> {
    let started = Instant::now();
    let mut selected = candidate
        .segments
        .iter()
        .filter(|segment| debt::segment_size_tier(segment.encoded_bytes) == selection.tier)
        .cloned()
        .collect::<Vec<_>>();
    selected.sort_by_key(|segment| segment.identity.segment_id);
    selected.truncate(selection.input_segments);
    if selected.len() != selection.input_segments || selected.len() < 2 {
        return Err(Status::internal(
            "overfull format-v4 segment tier has too few inputs",
        ));
    }

    let input = CompactionInputTotals::from_segments(&selected).map_err(index_status)?;
    let selected_ids = selected
        .iter()
        .map(|segment| segment.identity.segment_id)
        .collect::<BTreeSet<_>>();
    if selected
        .iter()
        .all(|segment| segment.live_document_count == 0)
    {
        candidate
            .segments
            .retain(|segment| !selected_ids.contains(&segment.identity.segment_id));
        emit_repayment(
            kind,
            selection.tier,
            input,
            0,
            0,
            0,
            started.elapsed().as_secs_f64(),
        );
        return Ok(());
    }

    let output_identity = SegmentIdentity::new(
        definition.stored.index_id,
        definition.object_version,
        definition.schema_fingerprint,
        dependencies
            .store
            .allocate_snowflake_id()
            .map_err(|error| Status::internal(format!("allocate merged segment ID: {error}")))?,
    )
    .map_err(index_status)?;
    let resident_bytes = usize::try_from(leased_bytes)
        .map_err(|_| Status::resource_exhausted("index compaction budget exceeds platform"))?;
    let merge_resident_bytes = resident_bytes
        .checked_sub(FIXED_INDEX_SEAL_WORKSPACE_BYTES)
        .ok_or_else(|| {
            Status::resource_exhausted("index compaction budget cannot fit fixed workspace")
        })?;
    let limits = BuildLimits::with_resident_limits(
        resident_bytes,
        merge_resident_bytes,
        FIXED_INDEX_SEAL_WORKSPACE_BYTES,
    )
    .map_err(index_status)?;
    let configured_lanes = usize::try_from(dependencies.config.compaction_max_lanes(kind))
        .map_err(|_| Status::resource_exhausted("index compaction lane limit exceeds platform"))?;
    let parallelism = CompactionParallelism::for_budget(
        configured_lanes,
        dependencies.cpu.workers(),
        leased_bytes,
    )
    .map_err(index_status)?;
    let progress = CompactionProgress::default();
    let telemetry = CompactionTelemetry::start(
        IndexTelemetryIdentity {
            index_id: definition.stored.index_id,
            tenant_id: definition.tenant_id,
            bucket_id: definition.bucket_id,
            kind,
        },
        selection.tier,
        selection.tier.saturating_add(1),
        input,
        parallelism,
        leased_bytes,
        progress.clone(),
    )
    .map_err(index_status)?;
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
    let mut sink = dependencies.publisher.observed_component_sink(
        &definition.stored,
        definition.tenant_id,
        definition.bucket_id,
        admission,
        progress.clone(),
    );
    let scratch = dependencies.cache.merge_scratch();
    let result = await_with_compaction_heartbeats(
        &telemetry,
        merge_segments(
            &directory,
            &definition.schema,
            &selected,
            output_identity,
            limits,
            &mut sink,
            &scratch,
            IndexCompactionExecutor::new(dependencies.cpu.clone()),
            parallelism,
            progress,
        ),
    )
    .await;
    let built = match result {
        Ok(built) => {
            telemetry.complete();
            built
        }
        Err(error) => {
            telemetry.failed();
            return Err(index_status(error));
        }
    };

    let sequence = candidate.allocate_sequence()?;
    let output_tier = debt::segment_size_tier(built.descriptor.encoded_bytes);
    let output_documents = u64::from(built.descriptor.document_count);
    let output_bytes = built.descriptor.encoded_bytes;
    let descriptor_identity = built.descriptor.identity;
    candidate
        .segments
        .retain(|segment| !selected_ids.contains(&segment.identity.segment_id));
    candidate.segments.push(built.descriptor);
    candidate
        .segments
        .sort_by_key(|segment| segment.identity.segment_id);
    candidate.locator_roots.push(LocatorRoot {
        sequence,
        identity: descriptor_identity,
        artifact: built.locator.root,
        encoded_bytes: built.locator.encoded_bytes,
        logical_bytes: built.locator.logical_bytes,
    });
    candidate.locator_roots.sort_by_key(|root| root.sequence);
    emit_repayment(
        kind,
        selection.tier,
        input,
        1,
        output_documents,
        output_bytes,
        started.elapsed().as_secs_f64(),
    );
    tracing::info!(
        index.kind = ?kind,
        index.tier = output_tier,
        gauge.anvil_index_compaction_output_tier = u64::from(output_tier),
        "format-v4 merged segment placed in its derived size tier"
    );
    Ok(())
}

fn emit_repayment(
    kind: IndexKind,
    input_tier: u8,
    input: CompactionInputTotals,
    output_segments: u64,
    output_documents: u64,
    output_bytes: u64,
    elapsed_seconds: f64,
) {
    let segments_repaid = input.segments.saturating_sub(output_segments);
    let repayment_rate = input.bytes as f64 / elapsed_seconds.max(0.001);
    tracing::info!(
        index.kind = ?kind,
        index.tier = input_tier,
        monotonic_counter.anvil_index_compactions_total = 1_u64,
        monotonic_counter.anvil_index_compaction_debt_segments_repaid_total = segments_repaid,
        monotonic_counter.anvil_index_compaction_debt_bytes_processed_total = input.bytes,
        gauge.anvil_index_compaction_debt_repayment_bytes_per_second = repayment_rate,
        histogram.anvil_index_compaction_input_segments = input.segments,
        histogram.anvil_index_compaction_input_documents = input.documents,
        histogram.anvil_index_compaction_output_segments = output_segments,
        histogram.anvil_index_compaction_output_documents = output_documents,
        histogram.anvil_index_compaction_output_bytes = output_bytes,
        histogram.anvil_index_compaction_debt_repayment_duration_seconds = elapsed_seconds,
        "format-v4 index segment debt repaid"
    );
}
