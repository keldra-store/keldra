use crate::IndexError;

use super::SegmentAssembly;
use super::layout::{DocumentRef, TermRef, term};
use super::statistics::StatisticsAccumulator;
use crate::v4::build::{ComponentBatchSink, ProjectedSource, StreamingComponentPublisher};
use crate::v4::{
    COMPONENT_HEADER_BYTES, ComponentKind, DocId, FieldComponents, INDEX_COMPONENT_BYTES,
    PositionEntry, PositionsBlock, PostingBlock, PostingImpact, PostingReference, Schema,
    SegmentIdentity, TERM_DICTIONARY_TARGET_BYTES, TermDictionary, TermEntry,
    component_ordinal_key,
};

const MAX_PAYLOAD_BYTES: usize = INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;
const MAX_POSTING_DOCS: usize = 16 * 1024;

#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_terms<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    references: &[TermRef],
    statistics: &mut StatisticsAccumulator,
    assembly: &mut SegmentAssembly,
) -> Result<(), IndexError> {
    let mut field_start = 0usize;
    while field_start < references.len() {
        let field_id = term(sources, documents, references[field_start]).field_id;
        let field_end = references[field_start..]
            .partition_point(|reference| term(sources, documents, *reference).field_id == field_id)
            + field_start;
        let minimum_field_length = schema.fields[field_id.get() as usize]
            .components
            .contains(FieldComponents::NORMS)
            .then(|| {
                statistics
                    .minimum_field_length(field_id.get() as usize)
                    .unwrap_or(0)
            });
        publish_field_postings(
            sink,
            identity,
            schema,
            routing_codec,
            sources,
            documents,
            &references[field_start..field_end],
            field_id,
            minimum_field_length,
            assembly,
        )
        .await?;
        publish_field_dictionary(
            sink,
            identity,
            schema,
            routing_codec,
            sources,
            documents,
            &references[field_start..field_end],
            field_id,
            statistics,
            assembly,
        )
        .await?;
        field_start = field_end;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn publish_field_postings<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    references: &[TermRef],
    field_id: crate::v4::FieldId,
    minimum_field_length: Option<u32>,
    assembly: &mut SegmentAssembly,
) -> Result<(), IndexError> {
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::POSTINGS,
        schema.codec_version(ComponentKind::POSTINGS)?,
        routing_codec,
    )?;
    let mut ordinal = 0u32;
    let mut group_start = 0usize;
    let mut has_positions = false;
    while group_start < references.len() {
        let group_end = term_group_end(sources, documents, references, group_start);
        let mut block_start = group_start;
        while block_start < group_end {
            let block_end =
                posting_block_end(sources, documents, references, block_start, group_end)?;
            let block_refs = &references[block_start..block_end];
            let doc_ids = block_refs
                .iter()
                .map(|value| DocId::new(value.doc_id))
                .collect();
            let frequencies = block_refs
                .iter()
                .map(|value| term(sources, documents, *value).frequency)
                .collect::<Vec<_>>();
            let maximum_frequency = frequencies
                .iter()
                .copied()
                .max()
                .expect("nonempty posting block");
            let block = PostingBlock::with_frequencies(
                doc_ids,
                Some(frequencies),
                minimum_field_length.map(|minimum_field_length| PostingImpact {
                    maximum_frequency,
                    minimum_field_length,
                }),
            )?;
            let key = component_ordinal_key(ordinal).to_vec();
            let payload = block.encode_payload()?;
            drop(block);
            publisher
                .push_payload(
                    key.clone(),
                    key,
                    u64::try_from(block_refs.len()).map_err(|_| IndexError::OffsetOverflow)?,
                    payload,
                )
                .await?;
            has_positions |= block_refs
                .iter()
                .any(|reference| !term(sources, documents, *reference).positions.is_empty());
            ordinal = ordinal.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
            block_start = block_end;
        }
        group_start = group_end;
    }
    assembly.add(
        ComponentKind::POSTINGS,
        Some(field_id),
        publisher.finish().await?,
    )?;
    if has_positions {
        publish_field_positions(
            sink,
            identity,
            schema,
            routing_codec,
            sources,
            documents,
            references,
            field_id,
            assembly,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn publish_field_positions<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    references: &[TermRef],
    field_id: crate::v4::FieldId,
    assembly: &mut SegmentAssembly,
) -> Result<(), IndexError> {
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::POSITIONS,
        schema.codec_version(ComponentKind::POSITIONS)?,
        routing_codec,
    )?;
    let mut ordinal = 0u32;
    let mut group_start = 0usize;
    while group_start < references.len() {
        let group_end = term_group_end(sources, documents, references, group_start);
        let mut block_start = group_start;
        while block_start < group_end {
            let block_end =
                posting_block_end(sources, documents, references, block_start, group_end)?;
            let entries = references[block_start..block_end]
                .iter()
                .filter_map(|reference| {
                    let value = term(sources, documents, *reference);
                    (!value.positions.is_empty()).then(|| PositionEntry {
                        doc_id: DocId::new(reference.doc_id),
                        positions: value.positions.clone(),
                    })
                })
                .collect::<Vec<_>>();
            if !entries.is_empty() {
                let block = PositionsBlock::new(entries)?;
                let key = component_ordinal_key(ordinal).to_vec();
                let element_count =
                    u64::try_from(block.entries().len()).map_err(|_| IndexError::OffsetOverflow)?;
                let payload = block.encode_payload()?;
                drop(block);
                publisher
                    .push_payload(key.clone(), key, element_count, payload)
                    .await?;
            }
            ordinal = ordinal.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
            block_start = block_end;
        }
        group_start = group_end;
    }
    assembly.add(
        ComponentKind::POSITIONS,
        Some(field_id),
        publisher.finish().await?,
    )
}

#[allow(clippy::too_many_arguments)]
async fn publish_field_dictionary<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    references: &[TermRef],
    field_id: crate::v4::FieldId,
    statistics: &mut StatisticsAccumulator,
    assembly: &mut SegmentAssembly,
) -> Result<(), IndexError> {
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::TERM_DICTIONARY,
        schema.codec_version(ComponentKind::TERM_DICTIONARY)?,
        routing_codec,
    )?;
    let mut pending = Vec::<TermEntry>::new();
    let mut pending_bytes = 6usize;
    let mut posting_ordinal = 0u32;
    let mut group_start = 0usize;
    while group_start < references.len() {
        let group_end = term_group_end(sources, documents, references, group_start);
        let first_component_ordinal = posting_ordinal;
        let mut block_start = group_start;
        while block_start < group_end {
            block_start =
                posting_block_end(sources, documents, references, block_start, group_end)?;
            posting_ordinal = posting_ordinal
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        let value = term(sources, documents, references[group_start]);
        let term_key = value.canonical_key()?;
        let row_bytes = 4usize
            .checked_add(term_key.len())
            .and_then(|bytes| bytes.checked_add(8 + 8 + 4 + 4))
            .ok_or(IndexError::OffsetOverflow)?;
        if !pending.is_empty()
            && pending_bytes.saturating_add(row_bytes) > TERM_DICTIONARY_TARGET_BYTES
        {
            publish_dictionary_block(&mut publisher, std::mem::take(&mut pending)).await?;
            pending_bytes = 6;
        }
        if pending_bytes.saturating_add(row_bytes) > MAX_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: pending_bytes.saturating_add(row_bytes) + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        let total_term_frequency =
            references[group_start..group_end]
                .iter()
                .try_fold(0u64, |sum, reference| {
                    sum.checked_add(u64::from(term(sources, documents, *reference).frequency))
                        .ok_or(IndexError::OffsetOverflow)
                })?;
        pending.push(TermEntry {
            term: term_key,
            postings: PostingReference {
                document_frequency: u64::try_from(group_end - group_start)
                    .map_err(|_| IndexError::OffsetOverflow)?,
                total_term_frequency,
                first_component_ordinal,
                component_count: posting_ordinal
                    .checked_sub(first_component_ordinal)
                    .ok_or(IndexError::OffsetOverflow)?,
            },
        });
        pending_bytes += row_bytes;
        if value.term_type != crate::v4::TERM_TYPE_FIELD_PRESENCE {
            statistics.observe_unique_term(field_id.get() as usize)?;
        }
        group_start = group_end;
    }
    if !pending.is_empty() {
        publish_dictionary_block(&mut publisher, pending).await?;
    }
    assembly.add(
        ComponentKind::TERM_DICTIONARY,
        Some(field_id),
        publisher.finish().await?,
    )
}

async fn publish_dictionary_block<S: ComponentBatchSink>(
    publisher: &mut StreamingComponentPublisher<'_, S>,
    entries: Vec<TermEntry>,
) -> Result<(), IndexError> {
    let dictionary = TermDictionary::new(entries)?;
    let minimum_key = dictionary
        .entries()
        .first()
        .expect("nonempty dictionary")
        .term
        .clone();
    let maximum_key = dictionary
        .entries()
        .last()
        .expect("nonempty dictionary")
        .term
        .clone();
    let element_count =
        u64::try_from(dictionary.entries().len()).map_err(|_| IndexError::OffsetOverflow)?;
    let payload = dictionary.encode_payload()?;
    drop(dictionary);
    publisher
        .push_payload(minimum_key, maximum_key, element_count, payload)
        .await
}

fn term_group_end(
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    references: &[TermRef],
    start: usize,
) -> usize {
    let first = term(sources, documents, references[start]);
    references[start..].partition_point(|reference| {
        let value = term(sources, documents, *reference);
        value.field_id == first.field_id
            && value.term_type == first.term_type
            && value.term == first.term
    }) + start
}

fn posting_block_end(
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    references: &[TermRef],
    start: usize,
    group_end: usize,
) -> Result<usize, IndexError> {
    let maximum = group_end.min(start.saturating_add(MAX_POSTING_DOCS));
    let mut end = start;
    let mut position_bytes = 6usize;
    while end < maximum {
        let value = term(sources, documents, references[end]);
        let row = if value.positions.is_empty() {
            0
        } else {
            8usize
                .checked_add(
                    value
                        .positions
                        .len()
                        .checked_mul(std::mem::size_of::<u32>())
                        .ok_or(IndexError::OffsetOverflow)?,
                )
                .ok_or(IndexError::OffsetOverflow)?
        };
        if end > start && position_bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
            break;
        }
        if position_bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: position_bytes.saturating_add(row) + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        position_bytes += row;
        end += 1;
    }
    Ok(end)
}
