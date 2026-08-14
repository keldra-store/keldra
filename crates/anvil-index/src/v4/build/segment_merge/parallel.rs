use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::IndexError;
use crate::compaction::{
    CompactionExecutor, CompactionParallelism, CompactionProgress, CompactionTaskHandle,
};

use super::super::super::{
    ArtifactDirectoryRead, ComponentKind, FieldComponents, FieldId, Schema, SegmentDescriptor,
    SegmentIdentity,
};
use super::super::sink::{PublishedStream, combine_published_streams};
use super::super::{ComponentBatchSink, MergeScratchSpace};
use super::doc_components::{BuiltDocStreams, DocRange, FieldCounts, build_doc_streams};
use super::term_streams::{BuiltTermStreams, TermRange, build_term_streams, plan_term_ranges};

enum RangeJob {
    Documents(DocRange),
    Terms(FieldId, TermRange),
}

enum RangeResult {
    Documents(BuiltDocStreams),
    Terms(FieldId, BuiltTermStreams),
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build_parallel_components<D, S, W, E>(
    directory: D,
    schema: Schema,
    inputs: Arc<Vec<SegmentDescriptor>>,
    identity: SegmentIdentity,
    permutation: W::File,
    remaps: Arc<Vec<W::File>>,
    minimum_field_lengths: Arc<Vec<Option<u32>>>,
    sink: S,
    scratch: W,
    document_count: u32,
    executor: E,
    parallelism: CompactionParallelism,
    progress: CompactionProgress,
) -> Result<(BuiltDocStreams, Vec<(FieldId, BuiltTermStreams)>), IndexError>
where
    D: ArtifactDirectoryRead + Clone + 'static,
    S: ComponentBatchSink + Clone + 'static,
    W: MergeScratchSpace,
    E: CompactionExecutor,
{
    let input_refs = inputs.iter().collect::<Vec<_>>();
    let needs_doc_aligned = schema.fields.iter().any(|field| {
        field.components.contains(FieldComponents::FAST_COLUMN)
            || field.components.contains(FieldComponents::STORED)
            || field.components.contains(FieldComponents::NORMS)
            || field.components.contains(FieldComponents::VECTOR)
    });
    let requested = parallelism.max_lanes();
    let mut jobs = Vec::new();
    if needs_doc_aligned {
        jobs.extend(
            split_doc_ranges(document_count, requested)
                .into_iter()
                .map(RangeJob::Documents),
        );
    }
    for field in schema
        .fields
        .iter()
        .filter(|field| field.components.contains(FieldComponents::TERMS))
    {
        jobs.extend(
            plan_term_ranges(&directory, &input_refs, field.id, requested)
                .await?
                .into_iter()
                .map(|range| RangeJob::Terms(field.id, range)),
        );
    }
    if jobs.is_empty() {
        return Ok((empty_doc_streams(&schema), Vec::new()));
    }

    let lanes = requested.min(jobs.len()).max(1);
    progress.add_ranges(jobs.len())?;
    progress.record_range_limit(jobs.len())?;
    progress.record_effective_lanes(lanes)?;
    let mut results = Vec::with_capacity(jobs.len());
    for group in jobs.chunks(lanes) {
        let mut active = Vec::with_capacity(group.len());
        for job in group {
            let job = match job {
                RangeJob::Documents(range) => RangeJob::Documents(*range),
                RangeJob::Terms(field_id, range) => RangeJob::Terms(*field_id, range.clone()),
            };
            let directory = directory.clone();
            let schema = schema.clone();
            let inputs = inputs.clone();
            let permutation = permutation.clone();
            let remaps = remaps.clone();
            let minimum_field_lengths = minimum_field_lengths.clone();
            let lane_sink = sink.clone();
            let scratch = scratch.clone();
            let progress = progress.clone();
            let slot = Arc::new(Mutex::new(None));
            let output = slot.clone();
            let handle = executor.spawn_io(Box::pin(async move {
                let active_range = progress.start_range();
                let input_refs = inputs.iter().collect::<Vec<_>>();
                let result = match job {
                    RangeJob::Documents(range) => {
                        let mut lane_sink = lane_sink;
                        build_doc_streams(
                            &directory,
                            &mut lane_sink,
                            &schema,
                            &input_refs,
                            identity,
                            &permutation,
                            range,
                        )
                        .await
                        .map(RangeResult::Documents)
                    }
                    RangeJob::Terms(field_id, range) => build_term_streams(
                        &directory,
                        lane_sink,
                        &scratch,
                        &schema,
                        &input_refs,
                        &remaps,
                        identity,
                        field_id,
                        minimum_field_lengths[field_id.get() as usize],
                        range,
                    )
                    .await
                    .map(|built| RangeResult::Terms(field_id, built)),
                };
                if result.is_ok() {
                    active_range.complete();
                }
                *output
                    .lock()
                    .map_err(|_| IndexError::Io("range result lock poisoned".into()))? =
                    Some(result);
                Ok(())
            }));
            active.push((handle, slot));
        }
        for index in 0..active.len() {
            if let Err(error) = (&mut active[index].0).await {
                for (handle, _) in &active[index + 1..] {
                    handle.abort();
                }
                return Err(error);
            }
        }
        for (_, slot) in active {
            results.push(
                slot.lock()
                    .map_err(|_| IndexError::Io("range result lock poisoned".into()))?
                    .take()
                    .ok_or(IndexError::InvalidFormat(
                        "range lane completed without a result",
                    ))??,
            );
        }
    }
    assemble_range_results(sink, &schema, identity, results).await
}

fn split_doc_ranges(total: u32, requested: usize) -> Vec<DocRange> {
    let count = requested.min(total as usize).max(1);
    (0..count)
        .map(|ordinal| DocRange {
            first: ((u64::from(total) * ordinal as u64) / count as u64) as u32,
            end: ((u64::from(total) * (ordinal + 1) as u64) / count as u64) as u32,
            total,
        })
        .collect()
}

async fn assemble_range_results<S: ComponentBatchSink>(
    mut sink: S,
    schema: &Schema,
    identity: SegmentIdentity,
    results: Vec<RangeResult>,
) -> Result<(BuiltDocStreams, Vec<(FieldId, BuiltTermStreams)>), IndexError> {
    let mut doc_counts = vec![FieldCounts::default(); schema.fields.len()];
    let mut doc_streams = BTreeMap::<(ComponentKind, Option<FieldId>), Vec<PublishedStream>>::new();
    let mut term_counts = BTreeMap::<FieldId, FieldCounts>::new();
    let mut term_streams =
        BTreeMap::<(ComponentKind, Option<FieldId>), Vec<PublishedStream>>::new();
    for result in results {
        match result {
            RangeResult::Documents(built) => {
                add_counts(&mut doc_counts, &built.counts)?;
                for (kind, field, stream) in built.streams {
                    doc_streams.entry((kind, field)).or_default().push(stream);
                }
            }
            RangeResult::Terms(field_id, built) => {
                add_one_count(term_counts.entry(field_id).or_default(), &built.counts)?;
                for (kind, field, stream) in built.streams {
                    term_streams.entry((kind, field)).or_default().push(stream);
                }
            }
        }
    }
    let routing_codec = schema.codec_version(ComponentKind::ROUTING_NODE)?;
    let mut assembled_docs = Vec::with_capacity(doc_streams.len());
    for ((kind, field), streams) in doc_streams {
        assembled_docs.push((
            kind,
            field,
            combine_published_streams(&mut sink, identity, kind, routing_codec, streams).await?,
        ));
    }
    let mut assembled_terms = Vec::new();
    for (field_id, counts) in term_counts {
        let mut streams = Vec::new();
        for kind in [
            ComponentKind::POSTINGS,
            ComponentKind::POSITIONS,
            ComponentKind::TERM_DICTIONARY,
        ] {
            if let Some(parts) = term_streams.remove(&(kind, Some(field_id))) {
                streams.push((
                    kind,
                    Some(field_id),
                    combine_published_streams(&mut sink, identity, kind, routing_codec, parts)
                        .await?,
                ));
            }
        }
        assembled_terms.push((field_id, BuiltTermStreams { streams, counts }));
    }
    if !term_streams.is_empty() {
        return Err(IndexError::InvalidFormat("unclaimed term range stream"));
    }
    Ok((
        BuiltDocStreams {
            streams: assembled_docs,
            counts: doc_counts,
        },
        assembled_terms,
    ))
}

fn empty_doc_streams(schema: &Schema) -> BuiltDocStreams {
    BuiltDocStreams {
        streams: Vec::new(),
        counts: vec![FieldCounts::default(); schema.fields.len()],
    }
}

fn add_counts(target: &mut [FieldCounts], source: &[FieldCounts]) -> Result<(), IndexError> {
    if target.len() != source.len() {
        return Err(IndexError::InvalidFormat("range field statistics width"));
    }
    for (target, source) in target.iter_mut().zip(source) {
        add_one_count(target, source)?;
    }
    Ok(())
}

fn add_one_count(target: &mut FieldCounts, source: &FieldCounts) -> Result<(), IndexError> {
    target.present_documents = target
        .present_documents
        .checked_add(source.present_documents)
        .ok_or(IndexError::OffsetOverflow)?;
    target.null_documents = target
        .null_documents
        .checked_add(source.null_documents)
        .ok_or(IndexError::OffsetOverflow)?;
    target.value_count = target
        .value_count
        .checked_add(source.value_count)
        .ok_or(IndexError::OffsetOverflow)?;
    target.unique_terms = target
        .unique_terms
        .checked_add(source.unique_terms)
        .ok_or(IndexError::OffsetOverflow)?;
    target.total_term_frequency = target
        .total_term_frequency
        .checked_add(source.total_term_frequency)
        .ok_or(IndexError::OffsetOverflow)?;
    target.total_field_length = target
        .total_field_length
        .checked_add(source.total_field_length)
        .ok_or(IndexError::OffsetOverflow)?;
    target.vector_count = target
        .vector_count
        .checked_add(source.vector_count)
        .ok_or(IndexError::OffsetOverflow)?;
    target.multi_valued_documents = target
        .multi_valued_documents
        .checked_add(source.multi_valued_documents)
        .ok_or(IndexError::OffsetOverflow)?;
    target.boolean_values = target
        .boolean_values
        .checked_add(source.boolean_values)
        .ok_or(IndexError::OffsetOverflow)?;
    target.number_values = target
        .number_values
        .checked_add(source.number_values)
        .ok_or(IndexError::OffsetOverflow)?;
    target.unsigned_values = target
        .unsigned_values
        .checked_add(source.unsigned_values)
        .ok_or(IndexError::OffsetOverflow)?;
    target.string_values = target
        .string_values
        .checked_add(source.string_values)
        .ok_or(IndexError::OffsetOverflow)?;
    target.minimum_field_length = match (target.minimum_field_length, source.minimum_field_length) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (None, value) | (value, None) => value,
    };
    target.maximum_field_length = match (target.maximum_field_length, source.maximum_field_length) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (None, value) | (value, None) => value,
    };
    target.vector_dimensions = match (target.vector_dimensions, source.vector_dimensions) {
        (Some(left), Some(right)) if left != right => {
            return Err(IndexError::InvalidFormat(
                "range vector dimensions are inconsistent",
            ));
        }
        (Some(value), _) | (_, Some(value)) => Some(value),
        (None, None) => None,
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_ranges_are_dense_and_disjoint() {
        let ranges = split_doc_ranges(11, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges.first().unwrap().first, 0);
        assert_eq!(ranges.last().unwrap().end, 11);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].first);
            assert!(pair[0].count() > 0);
        }
    }
}
