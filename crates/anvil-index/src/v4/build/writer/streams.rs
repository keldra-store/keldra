use crate::IndexError;

use super::layout::{DocumentRef, PointRef, SourceDocRef, point, record, source};
use super::{SegmentAssembly, push_payload};
use crate::v4::build::{
    ComponentBatchSink, ProjectedSource, PublishedStream, StreamingComponentPublisher,
};
use crate::v4::locator::PathLocatorBlockBuilder;
use crate::v4::{
    COMPONENT_HEADER_BYTES, Cardinality, ComponentKind, DocId, DocIdRange, DocumentIdentity,
    DocValueBlock, DocValueCell, FieldComponents, INDEX_COMPONENT_BYTES, IdentityBlock,
    IndexSemantics, LIVE_MASK_BLOCK_DOCS, LiveMaskBlock, LocatorEntry, LocatorValue, NormBlock,
    PathLocatorBlock, PointBlock, PointEntry, PointValue, ScalarValue, Schema, SegmentIdentity,
    VectorBlock, point_entry_key,
};

const MAX_PAYLOAD_BYTES: usize = INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;
const MAX_DOCS_PER_BLOCK: usize = 4096;

pub(super) async fn publish_identities<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    assembly: &mut SegmentAssembly,
) -> Result<(), IndexError> {
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::IDENTITY_TABLE,
        schema.codec_version(ComponentKind::IDENTITY_TABLE)?,
        routing_codec,
    )?;
    let mut start = 0usize;
    while start < documents.len() {
        let mut end = start;
        let mut encoded = 10usize;
        while end < documents.len() {
            let reference = documents[end];
            let source = source(sources, reference);
            let record = record(sources, reference);
            let row = 4usize
                .checked_add(source.source_identity.path.len())
                .and_then(|bytes| bytes.checked_add(8 + 4 + 1))
                .and_then(|bytes| {
                    bytes.checked_add(record.result_identity.as_ref().map_or(0, |result| {
                        4usize.saturating_add(result.path.len()).saturating_add(8)
                    }))
                })
                .ok_or(IndexError::OffsetOverflow)?;
            if end > start && encoded.saturating_add(row) > MAX_PAYLOAD_BYTES {
                break;
            }
            if encoded.saturating_add(row) > MAX_PAYLOAD_BYTES {
                return Err(IndexError::ResourceLimit {
                    needed: encoded.saturating_add(row) + COMPONENT_HEADER_BYTES,
                    limit: INDEX_COMPONENT_BYTES,
                });
            }
            encoded += row;
            end += 1;
        }
        let entries = documents[start..end]
            .iter()
            .copied()
            .map(|reference| {
                let source = source(sources, reference);
                let record = record(sources, reference);
                DocumentIdentity {
                    source: source.source_identity.clone(),
                    source_record: reference.source_record,
                    result: record.result_identity.clone(),
                }
            })
            .collect();
        let block = IdentityBlock::new(
            DocId::new(u32::try_from(start).map_err(|_| IndexError::OffsetOverflow)?),
            entries,
        )?;
        let count = u32::try_from(end - start).map_err(|_| IndexError::OffsetOverflow)?;
        let payload = block.encode_payload()?;
        drop(block);
        push_payload(
            &mut publisher,
            u32::try_from(start).map_err(|_| IndexError::OffsetOverflow)?,
            count,
            payload,
        )
        .await?;
        start = end;
    }
    assembly.add(
        ComponentKind::IDENTITY_TABLE,
        None,
        publisher.finish().await?,
    )
}

pub(super) async fn publish_live_mask<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    document_count: u32,
    assembly: &mut SegmentAssembly,
) -> Result<(), IndexError> {
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::LIVE_MASK,
        schema.codec_version(ComponentKind::LIVE_MASK)?,
        routing_codec,
    )?;
    let mut first = 0u32;
    while first < document_count {
        let count = LIVE_MASK_BLOCK_DOCS.min(document_count - first);
        let block = LiveMaskBlock::all_live(DocId::new(first), count)?;
        let payload = block.encode_payload()?;
        drop(block);
        push_payload(&mut publisher, first, count, payload).await?;
        first = first.checked_add(count).ok_or(IndexError::OffsetOverflow)?;
    }
    assembly.add(ComponentKind::LIVE_MASK, None, publisher.finish().await?)
}

pub(super) async fn publish_locator<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    sources: &[ProjectedSource],
    references: &[SourceDocRef],
) -> Result<PublishedStream, IndexError> {
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::PATH_LOCATOR,
        schema.codec_version(ComponentKind::PATH_LOCATOR)?,
        routing_codec,
    )?;
    let mut builder = PathLocatorBlockBuilder::default();
    let mut start = 0usize;
    while start < references.len() {
        let source_ordinal = references[start].source_ordinal;
        let source = &sources[source_ordinal as usize];
        let mut end = start;
        let mut ranges = Vec::new();
        while end < references.len() && references[end].source_ordinal == source_ordinal {
            let doc_id = references[end].doc_id;
            match ranges.last_mut() {
                Some(DocIdRange {
                    first_doc_id,
                    count,
                    ..
                }) if first_doc_id.get().checked_add(*count) == Some(doc_id) => {
                    *count = count.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
                }
                _ => {
                    let encoded = 4usize
                        .checked_add(source.source_identity.path.len())
                        .and_then(|bytes| bytes.checked_add(1 + 8 + 4))
                        .and_then(|bytes| bytes.checked_add((ranges.len() + 1) * 16))
                        .ok_or(IndexError::OffsetOverflow)?;
                    if encoded.saturating_add(6) > MAX_PAYLOAD_BYTES {
                        return Err(IndexError::ResourceLimit {
                            needed: encoded + COMPONENT_HEADER_BYTES,
                            limit: INDEX_COMPONENT_BYTES,
                        });
                    }
                    ranges.push(DocIdRange {
                        segment_id: identity.segment_id,
                        first_doc_id: DocId::new(doc_id),
                        count: 1,
                    });
                }
            }
            end += 1;
        }
        let entry = LocatorEntry {
            path: source.source_identity.path.clone(),
            value: LocatorValue::Live {
                object_version: source.source_identity.version,
                ranges,
            },
        };
        if let Some(entry) = builder.push(entry)? {
            publish_locator_block(&mut publisher, builder.finish()?.expect("nonempty locator"))
                .await?;
            if builder.push(entry)?.is_some() {
                return Err(IndexError::InvalidFormat(
                    "locator entry exceeds an empty block",
                ));
            }
        }
        start = end;
    }
    if let Some(block) = builder.finish()? {
        publish_locator_block(&mut publisher, block).await?;
    }
    publisher.finish().await
}

async fn publish_locator_block<S: ComponentBatchSink>(
    publisher: &mut StreamingComponentPublisher<'_, S>,
    block: PathLocatorBlock,
) -> Result<(), IndexError> {
    let minimum_key = block
        .entries()
        .first()
        .expect("nonempty locator")
        .path
        .as_bytes()
        .to_vec();
    let maximum_key = block
        .entries()
        .last()
        .expect("nonempty locator")
        .path
        .as_bytes()
        .to_vec();
    let element_count =
        u64::try_from(block.entries().len()).map_err(|_| IndexError::OffsetOverflow)?;
    let payload = block.encode_payload()?;
    drop(block);
    publisher
        .push_payload(minimum_key, maximum_key, element_count, payload)
        .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_doc_values<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    assembly: &mut SegmentAssembly,
) -> Result<(), IndexError> {
    for field in schema
        .fields
        .iter()
        .filter(|field| field.components.contains(FieldComponents::DOC_VALUES))
    {
        let mut publisher = StreamingComponentPublisher::new(
            sink,
            identity,
            ComponentKind::DOC_VALUES,
            schema.codec_version(ComponentKind::DOC_VALUES)?,
            routing_codec,
        )?;
        let multi_valued = field.cardinality == Cardinality::Multi;
        let mut start = 0usize;
        while start < documents.len() {
            let end = bounded_end(start, documents.len(), |index| {
                let cell = doc_value_cell(sources, documents[index], field.id);
                doc_value_cell_upper_bound(cell)
            })?;
            let cells = documents[start..end]
                .iter()
                .map(|document| {
                    doc_value_cell(sources, *document, field.id)
                        .cloned()
                        .unwrap_or_else(DocValueCell::missing)
                })
                .collect();
            let block = DocValueBlock::new(
                field.id,
                DocId::new(u32::try_from(start).map_err(|_| IndexError::OffsetOverflow)?),
                multi_valued,
                cells,
            )?;
            let count = u32::try_from(end - start).map_err(|_| IndexError::OffsetOverflow)?;
            let payload = block.encode_payload()?;
            drop(block);
            push_payload(
                &mut publisher,
                u32::try_from(start).map_err(|_| IndexError::OffsetOverflow)?,
                count,
                payload,
            )
            .await?;
            start = end;
        }
        assembly.add(
            ComponentKind::DOC_VALUES,
            Some(field.id),
            publisher.finish().await?,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_points<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    references: &[PointRef],
    assembly: &mut SegmentAssembly,
) -> Result<(), IndexError> {
    let mut field_start = 0usize;
    while field_start < references.len() {
        let field_id = point(sources, documents, references[field_start]).0;
        let field_end = references[field_start..]
            .partition_point(|reference| point(sources, documents, *reference).0 == field_id)
            + field_start;
        let mut publisher = StreamingComponentPublisher::new(
            sink,
            identity,
            ComponentKind::POINTS,
            schema.codec_version(ComponentKind::POINTS)?,
            routing_codec,
        )?;
        let mut start = field_start;
        while start < field_end {
            let mut end = start;
            let mut bytes = 16usize;
            while end < field_end && end - start < MAX_DOCS_PER_BLOCK {
                let row = 32usize;
                if end > start && bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
                    break;
                }
                bytes = bytes.saturating_add(row);
                end += 1;
            }
            let entries = references[start..end]
                .iter()
                .map(|reference| {
                    let (_, value) = point(sources, documents, *reference);
                    PointEntry {
                        value,
                        doc_id: DocId::new(reference.doc_id),
                    }
                })
                .collect();
            let block = PointBlock::new(field_id, entries)?;
            let minimum = point_entry_key(
                field_id,
                &block.minimum_entry().value,
                block.minimum_entry().doc_id,
            )?;
            let maximum = point_entry_key(
                field_id,
                &block.maximum_entry().value,
                block.maximum_entry().doc_id,
            )?;
            let element_count = block.entries().len() as u64;
            let payload = block.encode_payload()?;
            publisher
                .push_payload(minimum, maximum, element_count, payload)
                .await?;
            start = end;
        }
        assembly.add(
            ComponentKind::POINTS,
            Some(field_id),
            publisher.finish().await?,
        )?;
        field_start = field_end;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_vectors<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    assembly: &mut SegmentAssembly,
) -> Result<(), IndexError> {
    let dimensions = match &schema.semantics {
        IndexSemantics::Vector { dimensions, .. } | IndexSemantics::Hybrid { dimensions, .. } => {
            Some(*dimensions)
        }
        _ => None,
    };
    for field in schema
        .fields
        .iter()
        .filter(|field| field.components.contains(FieldComponents::VECTOR))
    {
        let dimensions = dimensions.ok_or_else(|| {
            IndexError::InvalidDefinition("vector field requires vector semantics".into())
        })?;
        let mut publisher = StreamingComponentPublisher::new(
            sink,
            identity,
            ComponentKind::VECTORS,
            schema.codec_version(ComponentKind::VECTORS)?,
            routing_codec,
        )?;
        let mut start = 0usize;
        while start < documents.len() {
            let end = bounded_end(start, documents.len(), |index| {
                Ok(16usize.saturating_add(
                    vector_values(sources, documents[index], field.id)
                        .map_or(0, |values| values.len().saturating_mul(4)),
                ))
            })?;
            let values = documents[start..end]
                .iter()
                .map(|document| vector_values(sources, *document, field.id).map(<[f32]>::to_vec))
                .collect();
            let block = VectorBlock::new(
                field.id,
                DocId::new(u32::try_from(start).map_err(|_| IndexError::OffsetOverflow)?),
                dimensions,
                values,
            )?;
            let count = u32::try_from(end - start).map_err(|_| IndexError::OffsetOverflow)?;
            let payload = block.encode_payload()?;
            drop(block);
            push_payload(
                &mut publisher,
                u32::try_from(start).map_err(|_| IndexError::OffsetOverflow)?,
                count,
                payload,
            )
            .await?;
            start = end;
        }
        assembly.add(
            ComponentKind::VECTORS,
            Some(field.id),
            publisher.finish().await?,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_norms<S: ComponentBatchSink>(
    sink: &mut S,
    identity: SegmentIdentity,
    schema: &Schema,
    routing_codec: u16,
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    assembly: &mut SegmentAssembly,
) -> Result<(), IndexError> {
    for field in schema
        .fields
        .iter()
        .filter(|field| field.components.contains(FieldComponents::NORMS))
    {
        let mut publisher = StreamingComponentPublisher::new(
            sink,
            identity,
            ComponentKind::NORMS,
            schema.codec_version(ComponentKind::NORMS)?,
            routing_codec,
        )?;
        let mut start = 0usize;
        while start < documents.len() {
            let end = documents.len().min(start + MAX_DOCS_PER_BLOCK);
            let values = documents[start..end]
                .iter()
                .map(|document| field_length(sources, *document, field.id))
                .collect();
            let block = NormBlock::new(
                field.id,
                DocId::new(u32::try_from(start).map_err(|_| IndexError::OffsetOverflow)?),
                values,
            )?;
            let count = u32::try_from(end - start).map_err(|_| IndexError::OffsetOverflow)?;
            let payload = block.encode_payload()?;
            drop(block);
            push_payload(
                &mut publisher,
                u32::try_from(start).map_err(|_| IndexError::OffsetOverflow)?,
                count,
                payload,
            )
            .await?;
            start = end;
        }
        assembly.add(
            ComponentKind::NORMS,
            Some(field.id),
            publisher.finish().await?,
        )?;
    }
    Ok(())
}

fn bounded_end<F>(start: usize, total: usize, mut row_bytes: F) -> Result<usize, IndexError>
where
    F: FnMut(usize) -> Result<usize, IndexError>,
{
    let maximum = total.min(start.saturating_add(MAX_DOCS_PER_BLOCK));
    let mut end = start;
    let mut bytes = 128usize;
    while end < maximum {
        let row = row_bytes(end)?;
        if end > start && bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
            break;
        }
        if bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: bytes.saturating_add(row) + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        bytes += row;
        end += 1;
    }
    Ok(end)
}

fn doc_value_cell_upper_bound(cell: Option<&DocValueCell>) -> Result<usize, IndexError> {
    let Some(cell) = cell else {
        return Ok(16);
    };
    cell.values.iter().try_fold(16usize, |bytes, value| {
        bytes
            .checked_add(match value {
                ScalarValue::Null => 1,
                ScalarValue::Boolean(_) => 6,
                ScalarValue::Signed(_) | ScalarValue::Number(_) | ScalarValue::Unsigned(_) => 27,
                ScalarValue::String(value) => {
                    3usize.saturating_mul(5usize.saturating_add(value.len()))
                }
            })
            .ok_or(IndexError::OffsetOverflow)
    })
}

fn doc_value_cell(
    sources: &[ProjectedSource],
    document: DocumentRef,
    field_id: crate::v4::FieldId,
) -> Option<&DocValueCell> {
    let values = &record(sources, document).doc_values;
    values
        .binary_search_by_key(&field_id, |column| column.field_id)
        .ok()
        .map(|index| &values[index].cell)
}

fn vector_values(
    sources: &[ProjectedSource],
    document: DocumentRef,
    field_id: crate::v4::FieldId,
) -> Option<&[f32]> {
    let vectors = &record(sources, document).vectors;
    vectors
        .binary_search_by_key(&field_id, |vector| vector.field_id)
        .ok()
        .map(|index| vectors[index].values.as_slice())
}

fn field_length(
    sources: &[ProjectedSource],
    document: DocumentRef,
    field_id: crate::v4::FieldId,
) -> Option<u32> {
    let lengths = &record(sources, document).field_lengths;
    lengths
        .binary_search_by_key(&field_id, |value| value.0)
        .ok()
        .map(|index| lengths[index].1)
}
