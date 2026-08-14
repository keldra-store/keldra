//! Storage-neutral fixed-memory format-v4 segment merging.
//!
//! Authoritative input is limited to checked immutable index artifacts. Local
//! scratch holds only restart-disposable remaps, permutations, and bounded
//! external-sort runs; no scratch location is ever published.

mod doc_components;
mod io;
mod locator;
mod order;
mod parallel;
mod point_streams;
mod term_streams;

#[cfg(test)]
mod tests;

use crate::IndexError;
use crate::compaction::{CompactionExecutor, CompactionParallelism, CompactionProgress};

use super::super::{
    ArtifactDirectoryRead, ComponentKind, ComponentStatistics, FieldComponents, FieldId,
    FieldStatistics, INDEX_COMPONENT_BYTES, INDEX_DECODE_BYTES, IdentityBlock,
    LIVE_MASK_BLOCK_DOCS, LiveMaskBlock, PhysicalOrderBounds, Schema, SegmentComponent,
    SegmentComponentReader, SegmentDescriptor, SegmentIdentity, SegmentStatistics,
};
use super::{
    BuildLimits, BuiltSegment, ComponentBatchSink, MergeScratchFile, MergeScratchSpace,
    PublishedStream, StreamingComponentPublisher,
};
use doc_components::FieldCounts;
use io::CompactionDirectory;
use locator::{RelocationRecord, RelocationSorter, publish_locator};
use order::{OrderedInput, compare_current};
use parallel::build_parallel_components;

pub const MAXIMUM_SEGMENT_MERGE_INPUTS: usize = 4;

type SegmentStream = (ComponentKind, Option<FieldId>, PublishedStream);

/// Merge immutable v4 segments without materializing their source objects or
/// their complete projected corpus.
///
/// The caller owns `scratch`; all its files are disposable after this future
/// resolves. The returned segment and locator refer only to components placed
/// through `sink`.
#[allow(clippy::too_many_arguments)]
pub async fn merge_segments<D, S, W, E>(
    directory: &D,
    schema: &Schema,
    input_descriptors: &[SegmentDescriptor],
    output_identity: SegmentIdentity,
    limits: BuildLimits,
    sink: &mut S,
    scratch: &W,
    executor: E,
    parallelism: CompactionParallelism,
    progress: CompactionProgress,
) -> Result<BuiltSegment, IndexError>
where
    D: ArtifactDirectoryRead + Clone + 'static,
    S: ComponentBatchSink + Clone + 'static,
    W: MergeScratchSpace,
    E: CompactionExecutor,
{
    let inputs = validate_merge(schema, input_descriptors, output_identity, limits)?;
    let directory = CompactionDirectory::new(directory.clone(), executor.clone());
    let minimum_field_lengths =
        std::sync::Arc::new(load_minimum_field_lengths(&directory, schema, &inputs).await?);
    let mut remaps = Vec::with_capacity(inputs.len());
    for input in &inputs {
        let remap = scratch.create_file().await?;
        remap
            .resize_zeroed(u64::from(input.document_count) * 4)
            .await?;
        remaps.push(remap);
    }
    let permutation = scratch.create_file().await?;
    let mut relocations =
        RelocationSorter::new(scratch, limits.total_resident_bytes() / 8, executor.clone())?;
    let (identity_stream, document_count, physical_order_bounds) = build_identity_and_remaps(
        &directory,
        schema,
        &inputs,
        output_identity,
        sink,
        &remaps,
        &permutation,
        &mut relocations,
    )
    .await?;
    let relocation_run = relocations.finish().await?;
    let (locator, source_count) =
        publish_locator(sink, schema, output_identity, relocation_run).await?;
    let live_mask = publish_live_mask(sink, schema, output_identity, document_count).await?;

    let owned_inputs = std::sync::Arc::new(
        inputs
            .iter()
            .map(|input| (*input).clone())
            .collect::<Vec<_>>(),
    );
    let (mut built_doc_streams, built_terms) = build_parallel_components(
        directory.clone(),
        schema.clone(),
        owned_inputs,
        output_identity,
        permutation.clone(),
        std::sync::Arc::new(remaps),
        minimum_field_lengths,
        sink.clone(),
        scratch.clone(),
        document_count,
        executor,
        parallelism,
        progress,
        limits.total_resident_bytes() / 8,
    )
    .await?;
    let mut term_streams = Vec::new();
    for (field_id, built) in built_terms {
        let field = &schema.fields[field_id.get() as usize];
        let counts = &mut built_doc_streams.counts[field_id.get() as usize];
        counts.unique_terms = built.counts.unique_terms;
        counts.total_term_frequency = built.counts.total_term_frequency;
        if !field.components.contains(FieldComponents::DOC_VALUES)
            && !field.components.contains(FieldComponents::POINTS)
        {
            counts.null_documents = built.counts.null_documents;
            counts.value_count = built.counts.value_count;
            counts.boolean_values = built.counts.boolean_values;
            counts.string_values = built.counts.string_values;
        }
        if !field.components.contains(FieldComponents::DOC_VALUES)
            && !field.components.contains(FieldComponents::POINTS)
            && !field.components.contains(FieldComponents::NORMS)
            && !field.components.contains(FieldComponents::VECTOR)
        {
            counts.present_documents = built.counts.present_documents;
        }
        term_streams.extend(built.streams);
    }
    let shape = schema.segment_shape()?;
    let mut streams = planned_vec(shape.component_count)?;
    streams.push((ComponentKind::IDENTITY_TABLE, None, identity_stream));
    streams.push((ComponentKind::LIVE_MASK, None, live_mask));
    streams.append(&mut built_doc_streams.streams);
    streams.append(&mut term_streams);
    let mut component_statistics = planned_vec(shape.component_statistics_count)?;
    for (role, field_id, stream) in &streams {
        if let Some(statistics) = stream.statistics(*role, *field_id)? {
            component_statistics.push(statistics);
        }
    }
    component_statistics.sort_by_key(|statistics| (statistics.role, statistics.field_id));
    let statistics = publish_statistics(
        sink,
        schema,
        output_identity,
        source_count,
        document_count,
        physical_order_bounds,
        &built_doc_streams.counts,
        component_statistics,
    )
    .await?;
    streams.push((ComponentKind::SCORING_STATISTICS, None, statistics));
    assemble_segment(
        output_identity,
        document_count,
        source_count,
        locator,
        streams,
    )
}

async fn load_minimum_field_lengths<D: ArtifactDirectoryRead>(
    directory: &D,
    schema: &Schema,
    inputs: &[&SegmentDescriptor],
) -> Result<Vec<Option<u32>>, IndexError> {
    let mut minimums = vec![None::<u32>; schema.fields.len()];
    for input in inputs {
        let statistics = SegmentComponentReader::new(directory, input)?
            .statistics()
            .await?;
        if statistics.document_count != u64::from(input.document_count)
            || statistics.fields.len() != schema.fields.len()
        {
            return Err(IndexError::InvalidFormat(
                "merge input statistics differ from its segment or schema",
            ));
        }
        for (field, observed) in schema.fields.iter().zip(&statistics.fields) {
            if observed.field_id != field.id {
                return Err(IndexError::InvalidFormat(
                    "merge input statistics field order",
                ));
            }
            if field.components.contains(FieldComponents::NORMS)
                && let Some(value) = observed.minimum_field_length
            {
                let minimum = &mut minimums[field.id.get() as usize];
                *minimum = Some(minimum.map_or(value, |current| current.min(value)));
            }
        }
    }
    for field in &schema.fields {
        if field.components.contains(FieldComponents::TERMS)
            && field.components.contains(FieldComponents::NORMS)
            && minimums[field.id.get() as usize].is_none()
        {
            minimums[field.id.get() as usize] = Some(0);
        }
    }
    Ok(minimums)
}

#[allow(clippy::too_many_arguments)]
async fn build_identity_and_remaps<'a, D, S, W, E>(
    directory: &'a D,
    schema: &Schema,
    inputs: &[&'a SegmentDescriptor],
    identity: SegmentIdentity,
    sink: &mut S,
    remaps: &[W::File],
    permutation: &W::File,
    relocations: &mut RelocationSorter<'_, W, E>,
) -> Result<(PublishedStream, u32, Option<PhysicalOrderBounds>), IndexError>
where
    D: ArtifactDirectoryRead,
    S: ComponentBatchSink,
    W: MergeScratchSpace,
    E: CompactionExecutor,
{
    let mut cursors = inputs
        .iter()
        .map(|input| OrderedInput::new(directory, input, schema))
        .collect::<Result<Vec<_>, _>>()?;
    for cursor in &mut cursors {
        cursor.advance().await?;
    }
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::IDENTITY_TABLE,
        schema.codec_version(ComponentKind::IDENTITY_TABLE)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    let mut buffered = Vec::new();
    let mut buffered_bytes = 10usize;
    let mut emitted = 0u32;
    let mut minimum_order_key = None::<Vec<u8>>;
    let mut maximum_order_key = None::<Vec<u8>>;
    while let Some(selected) = cursors
        .iter()
        .enumerate()
        .filter_map(|(index, cursor)| cursor.current().map(|document| (index, document)))
        .min_by(|(left_index, left), (right_index, right)| {
            compare_current(*left_index, left, *right_index, right)
        })
        .map(|(index, _)| index)
    {
        let document = cursors[selected]
            .current()
            .expect("selected live document")
            .clone();
        if !schema.physical_order.is_empty() {
            if minimum_order_key.is_none() {
                minimum_order_key = Some(document.order_key().to_vec());
            }
            maximum_order_key = Some(document.order_key().to_vec());
        }
        let row_bytes = identity_resident_bytes(&document.identity)?;
        if !buffered.is_empty()
            && buffered_bytes.saturating_add(row_bytes) > INDEX_COMPONENT_BYTES / 2
        {
            emitted =
                emit_identity_block(directory, &mut publisher, emitted, &mut buffered).await?;
            buffered_bytes = 10;
        }
        let new_doc_id = emitted
            .checked_add(u32::try_from(buffered.len()).map_err(|_| IndexError::OffsetOverflow)?)
            .ok_or(IndexError::OffsetOverflow)?;
        let encoded_remap = new_doc_id.checked_add(1).ok_or(IndexError::ResourceLimit {
            needed: u32::MAX as usize + 1,
            limit: u32::MAX as usize,
        })?;
        remaps[selected]
            .write_all_at(
                u64::from(document.old_doc_id) * 4,
                encoded_remap.to_le_bytes().to_vec(),
            )
            .await?;
        let mut permutation_record = Vec::with_capacity(8);
        permutation_record.extend_from_slice(
            &u32::try_from(selected)
                .map_err(|_| IndexError::OffsetOverflow)?
                .to_le_bytes(),
        );
        permutation_record.extend_from_slice(&document.old_doc_id.to_le_bytes());
        permutation.append(permutation_record).await?;
        relocations
            .push(RelocationRecord {
                path: document.identity.source.path.clone(),
                object_version: document.identity.source.version,
                input_ordinal: u32::try_from(selected).map_err(|_| IndexError::OffsetOverflow)?,
                new_doc_id,
            })
            .await?;
        buffered_bytes = buffered_bytes
            .checked_add(row_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        buffered.push(document.identity);
        cursors[selected].advance().await?;
    }
    emitted = emit_identity_block(directory, &mut publisher, emitted, &mut buffered).await?;
    let expected = inputs.iter().try_fold(0u64, |sum, input| {
        sum.checked_add(u64::from(input.live_document_count))
            .ok_or(IndexError::OffsetOverflow)
    })?;
    if emitted == 0 || u64::from(emitted) != expected {
        return Err(IndexError::InvalidFormat(
            "merged live document count differs from input descriptors",
        ));
    }
    let physical_order_bounds =
        minimum_order_key
            .zip(maximum_order_key)
            .map(|(minimum_key, maximum_key)| PhysicalOrderBounds {
                minimum_key,
                maximum_key,
            });
    Ok((publisher.finish().await?, emitted, physical_order_bounds))
}

async fn emit_identity_block<D: ArtifactDirectoryRead, S: ComponentBatchSink>(
    directory: &D,
    publisher: &mut StreamingComponentPublisher<'_, S>,
    first: u32,
    identities: &mut Vec<super::super::DocumentIdentity>,
) -> Result<u32, IndexError> {
    if identities.is_empty() {
        return Ok(first);
    }
    let count = u32::try_from(identities.len()).map_err(|_| IndexError::OffsetOverflow)?;
    let identities = std::mem::take(identities);
    let payload = directory
        .run_query_cpu(move || {
            IdentityBlock::new(super::super::DocId::new(first), identities)?.encode_payload()
        })
        .await?;
    let last = first
        .checked_add(count - 1)
        .ok_or(IndexError::OffsetOverflow)?;
    publisher
        .push_payload(
            first.to_be_bytes().to_vec(),
            last.to_be_bytes().to_vec(),
            u64::from(count),
            payload,
        )
        .await?;
    first.checked_add(count).ok_or(IndexError::OffsetOverflow)
}

fn identity_resident_bytes(identity: &super::super::DocumentIdentity) -> Result<usize, IndexError> {
    17usize
        .checked_add(identity.source.path.len())
        .and_then(|bytes| {
            bytes.checked_add(
                identity
                    .result
                    .as_ref()
                    .map_or(0, |result| 12 + result.path.len()),
            )
        })
        .ok_or(IndexError::OffsetOverflow)
}

async fn publish_live_mask<S: ComponentBatchSink>(
    sink: &mut S,
    schema: &Schema,
    identity: SegmentIdentity,
    document_count: u32,
) -> Result<PublishedStream, IndexError> {
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::LIVE_MASK,
        schema.codec_version(ComponentKind::LIVE_MASK)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    let mut first = 0u32;
    while first < document_count {
        let count = LIVE_MASK_BLOCK_DOCS.min(document_count - first);
        let block = LiveMaskBlock::all_live(super::super::DocId::new(first), count)?;
        let last = first
            .checked_add(count - 1)
            .ok_or(IndexError::OffsetOverflow)?;
        publisher
            .push_payload(
                first.to_be_bytes().to_vec(),
                last.to_be_bytes().to_vec(),
                u64::from(count),
                block.encode_payload()?,
            )
            .await?;
        first = first.checked_add(count).ok_or(IndexError::OffsetOverflow)?;
    }
    publisher.finish().await
}

#[allow(clippy::too_many_arguments)]
async fn publish_statistics<S: ComponentBatchSink>(
    sink: &mut S,
    schema: &Schema,
    identity: SegmentIdentity,
    source_count: u64,
    document_count: u32,
    physical_order_bounds: Option<PhysicalOrderBounds>,
    counts: &[FieldCounts],
    components: Vec<ComponentStatistics>,
) -> Result<PublishedStream, IndexError> {
    let mut fields = planned_vec(schema.fields.len())?;
    fields.extend(
        schema
            .fields
            .iter()
            .zip(counts)
            .map(|(field, counts)| FieldStatistics {
                field_id: field.id,
                present_documents: counts.present_documents,
                null_documents: counts.null_documents,
                value_count: counts.value_count,
                unique_terms: counts.unique_terms,
                total_term_frequency: counts.total_term_frequency,
                total_field_length: counts.total_field_length,
                minimum_field_length: counts.minimum_field_length,
                maximum_field_length: counts.maximum_field_length,
                vector_count: counts.vector_count,
                vector_dimensions: counts.vector_dimensions,
                multi_valued_documents: counts.multi_valued_documents,
                boolean_values: counts.boolean_values,
                number_values: counts.number_values,
                unsigned_values: counts.unsigned_values,
                string_values: counts.string_values,
            }),
    );
    let unique_terms = counts.iter().try_fold(0u64, |sum, counts| {
        sum.checked_add(counts.unique_terms)
            .ok_or(IndexError::OffsetOverflow)
    })?;
    let statistics = SegmentStatistics::new(
        source_count,
        u64::from(document_count),
        unique_terms,
        physical_order_bounds,
        fields,
        components,
    )?;
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::SCORING_STATISTICS,
        schema.codec_version(ComponentKind::SCORING_STATISTICS)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    publisher
        .push_payload(
            b"statistics".to_vec(),
            b"statistics".to_vec(),
            1,
            statistics.encode_payload()?,
        )
        .await?;
    publisher.finish().await
}

fn assemble_segment(
    identity: SegmentIdentity,
    document_count: u32,
    source_count: u64,
    locator: PublishedStream,
    streams: Vec<SegmentStream>,
) -> Result<BuiltSegment, IndexError> {
    let mut components = planned_vec(streams.len())?;
    let (mut encoded_bytes, mut logical_bytes) = (0u64, 0u64);
    for (role, field_id, stream) in streams {
        encoded_bytes = encoded_bytes
            .checked_add(stream.encoded_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        logical_bytes = logical_bytes
            .checked_add(stream.logical_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        components.push(SegmentComponent {
            role,
            field_id,
            ordinal: 0,
            artifact: stream.root,
        });
    }
    components.sort_by_key(|component| (component.role, component.field_id, component.ordinal));
    Ok(BuiltSegment {
        descriptor: SegmentDescriptor::new(
            identity,
            document_count,
            document_count,
            components,
            encoded_bytes,
            logical_bytes,
        )?,
        locator,
        source_count,
    })
}

fn validate_merge<'a>(
    schema: &Schema,
    inputs: &'a [SegmentDescriptor],
    output: SegmentIdentity,
    limits: BuildLimits,
) -> Result<Vec<&'a SegmentDescriptor>, IndexError> {
    schema.validate()?;
    output.validate()?;
    limits.validate()?;
    if inputs.is_empty()
        || inputs.len() > MAXIMUM_SEGMENT_MERGE_INPUTS
        || schema.fingerprint()? != output.schema_fingerprint
    {
        return Err(IndexError::InvalidDefinition(
            "segment merge requires one to four inputs and an output matching the schema".into(),
        ));
    }
    let mut ordered = inputs.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|input| input.identity.segment_id);
    let mut previous = None;
    let mut live = 0u64;
    for input in &ordered {
        input.validate()?;
        if input.identity.index_id != output.index_id
            || input.identity.definition_version != output.definition_version
            || input.identity.schema_fingerprint != output.schema_fingerprint
            || input.identity.segment_id == output.segment_id
            || previous == Some(input.identity.segment_id)
        {
            return Err(IndexError::InvalidDefinition(
                "merge segment identities do not share one exact schema lineage".into(),
            ));
        }
        validate_component_shape(schema, input)?;
        live = live
            .checked_add(u64::from(input.live_document_count))
            .ok_or(IndexError::OffsetOverflow)?;
        previous = Some(input.identity.segment_id);
    }
    if live == 0 || live > u64::from(u32::MAX) {
        return Err(IndexError::ResourceLimit {
            needed: usize::try_from(live).unwrap_or(usize::MAX),
            limit: u32::MAX as usize,
        });
    }
    let order_blocks = ordered
        .len()
        .checked_mul(schema.physical_order.len().saturating_add(2))
        .ok_or(IndexError::OffsetOverflow)?;
    let term_blocks = if schema
        .fields
        .iter()
        .any(|field| field.components.contains(FieldComponents::TERMS))
    {
        ordered
            .len()
            .checked_mul(3)
            .ok_or(IndexError::OffsetOverflow)?
    } else {
        ordered.len()
    };
    let resident_blocks = order_blocks.max(term_blocks).saturating_add(4);
    let codec_workspace = resident_blocks
        .checked_mul(INDEX_DECODE_BYTES)
        .ok_or(IndexError::OffsetOverflow)?;
    let needed = codec_workspace
        .checked_add(merge_schema_workspace_bytes(schema)?)
        .ok_or(IndexError::OffsetOverflow)?;
    if needed > limits.total_resident_bytes() {
        return Err(IndexError::ResourceLimit {
            needed,
            limit: limits.total_resident_bytes(),
        });
    }
    Ok(ordered)
}

fn merge_schema_workspace_bytes(schema: &Schema) -> Result<usize, IndexError> {
    let shape = schema.segment_shape()?;
    let streams = shape
        .component_count
        .checked_mul(std::mem::size_of::<SegmentStream>())
        .ok_or(IndexError::OffsetOverflow)?;
    let fields_and_statistics = shape
        .field_count
        .checked_mul(std::mem::size_of::<FieldStatistics>())
        .and_then(|bytes| {
            bytes.checked_add(
                shape
                    .component_statistics_count
                    .checked_mul(std::mem::size_of::<ComponentStatistics>())?,
            )
        })
        .ok_or(IndexError::OffsetOverflow)?;
    let descriptors = shape
        .component_count
        .checked_mul(std::mem::size_of::<SegmentComponent>())
        .ok_or(IndexError::OffsetOverflow)?;
    streams
        .checked_add(fields_and_statistics.max(descriptors))
        .ok_or(IndexError::OffsetOverflow)
}

fn planned_vec<T>(capacity: usize) -> Result<Vec<T>, IndexError> {
    let values = Vec::with_capacity(capacity);
    if values.capacity() != capacity {
        return Err(IndexError::ResourceLimit {
            needed: values.capacity().saturating_mul(std::mem::size_of::<T>()),
            limit: capacity.saturating_mul(std::mem::size_of::<T>()),
        });
    }
    Ok(values)
}

fn validate_component_shape(
    schema: &Schema,
    segment: &SegmentDescriptor,
) -> Result<(), IndexError> {
    for field in &schema.fields {
        let has = |kind| {
            segment
                .components
                .binary_search_by_key(&(kind, Some(field.id), 0), |component| {
                    (component.role, component.field_id, component.ordinal)
                })
                .is_ok()
        };
        let terms = has(ComponentKind::TERM_DICTIONARY);
        let postings = has(ComponentKind::POSTINGS);
        let positions = has(ComponentKind::POSITIONS);
        if terms != postings
            || field.components.contains(FieldComponents::POSITIONS) != positions
            || field.components.contains(FieldComponents::POINTS) != has(ComponentKind::POINTS)
            || field.components.contains(FieldComponents::DOC_VALUES)
                != has(ComponentKind::DOC_VALUES)
            || field.components.contains(FieldComponents::NORMS) != has(ComponentKind::NORMS)
            || field.components.contains(FieldComponents::VECTOR) != has(ComponentKind::VECTORS)
        {
            return Err(IndexError::InvalidFormat(
                "merge input component presence differs from its schema",
            ));
        }
    }
    Ok(())
}
