use crate::IndexError;

use super::super::super::{
    ArtifactDirectoryRead, Cardinality, ComponentKind, DocId, FastColumnBlock, FastColumnCell,
    FieldComponents, FieldId, INDEX_COMPONENT_BYTES, IndexSemantics, NormBlock, ScalarValue,
    Schema, SegmentDescriptor, StoredFieldsBlock, VectorBlock,
};
use super::super::ComponentBatchSink;
use super::super::scratch::MergeScratchFile;
use super::super::sink::{PublishedStream, StreamingComponentPublisher};
use super::io::{FixedScratchReader, RoutedBlockStream, optional_stream, required_stream};

#[derive(Clone, Debug, Default)]
pub(super) struct FieldCounts {
    pub(super) present_documents: u64,
    pub(super) null_documents: u64,
    pub(super) value_count: u64,
    pub(super) unique_terms: u64,
    pub(super) total_term_frequency: u64,
    pub(super) total_field_length: u64,
    pub(super) minimum_field_length: Option<u32>,
    pub(super) maximum_field_length: Option<u32>,
    pub(super) vector_count: u64,
    pub(super) vector_dimensions: Option<u32>,
    pub(super) multi_valued_documents: u64,
    pub(super) boolean_values: u64,
    pub(super) number_values: u64,
    pub(super) unsigned_values: u64,
    pub(super) string_values: u64,
}

pub(super) struct BuiltDocStreams {
    pub(super) streams: Vec<(ComponentKind, Option<FieldId>, PublishedStream)>,
    pub(super) counts: Vec<FieldCounts>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DocRange {
    pub(super) first: u32,
    pub(super) end: u32,
    pub(super) total: u32,
}

impl DocRange {
    #[cfg(test)]
    pub(super) fn count(self) -> u32 {
        self.end - self.first
    }

    fn is_last(self) -> bool {
        self.end == self.total
    }
}

pub(super) async fn build_doc_streams<D, S, F>(
    directory: &D,
    sink: &mut S,
    schema: &Schema,
    inputs: &[&SegmentDescriptor],
    output_identity: super::super::super::SegmentIdentity,
    permutation: &F,
    range: DocRange,
) -> Result<BuiltDocStreams, IndexError>
where
    D: ArtifactDirectoryRead,
    S: ComponentBatchSink,
    F: MergeScratchFile,
{
    let mut streams = Vec::new();
    let mut counts = vec![FieldCounts::default(); schema.fields.len()];
    for field in &schema.fields {
        if field.components.contains(FieldComponents::FAST_COLUMN) {
            let (stream, field_counts) = build_columns(
                directory,
                sink,
                schema,
                inputs,
                output_identity,
                permutation,
                range,
                field.id,
                field.cardinality == Cardinality::Multi,
            )
            .await?;
            streams.push((ComponentKind::FAST_COLUMN, Some(field.id), stream));
            counts[field.id.get() as usize].present_documents = field_counts.present_documents;
            counts[field.id.get() as usize].null_documents = field_counts.null_documents;
            counts[field.id.get() as usize].value_count = field_counts.value_count;
            counts[field.id.get() as usize].multi_valued_documents =
                field_counts.multi_valued_documents;
            counts[field.id.get() as usize].boolean_values = field_counts.boolean_values;
            counts[field.id.get() as usize].number_values = field_counts.number_values;
            counts[field.id.get() as usize].unsigned_values = field_counts.unsigned_values;
            counts[field.id.get() as usize].string_values = field_counts.string_values;
        }
    }
    if schema
        .fields
        .iter()
        .any(|field| field.components.contains(FieldComponents::STORED))
    {
        streams.push((
            ComponentKind::STORED_FIELDS,
            None,
            build_stored(
                directory,
                sink,
                schema,
                inputs,
                output_identity,
                permutation,
                range,
            )
            .await?,
        ));
    }
    for field in &schema.fields {
        if field.components.contains(FieldComponents::NORMS) {
            let (stream, present, total, minimum, maximum) = build_norms(
                directory,
                sink,
                schema,
                inputs,
                output_identity,
                permutation,
                range,
                field.id,
            )
            .await?;
            streams.push((ComponentKind::NORMS, Some(field.id), stream));
            let counts = &mut counts[field.id.get() as usize];
            if !field.components.contains(FieldComponents::FAST_COLUMN) {
                counts.present_documents = present;
            }
            counts.total_field_length = total;
            counts.minimum_field_length = minimum;
            counts.maximum_field_length = maximum;
        }
        if field.components.contains(FieldComponents::VECTOR) {
            let dimensions = dimensions(schema)?;
            let (stream, present) = build_vectors(
                directory,
                sink,
                schema,
                inputs,
                output_identity,
                permutation,
                range,
                field.id,
                dimensions,
            )
            .await?;
            streams.push((ComponentKind::VECTORS, Some(field.id), stream));
            let counts = &mut counts[field.id.get() as usize];
            if !field.components.contains(FieldComponents::FAST_COLUMN)
                && !field.components.contains(FieldComponents::NORMS)
            {
                counts.present_documents = present;
            }
            counts.vector_count = present;
            counts.vector_dimensions = Some(dimensions);
        }
    }
    Ok(BuiltDocStreams { streams, counts })
}

#[allow(clippy::too_many_arguments)]
async fn build_columns<D, S, F>(
    directory: &D,
    sink: &mut S,
    schema: &Schema,
    inputs: &[&SegmentDescriptor],
    identity: super::super::super::SegmentIdentity,
    permutation: &F,
    range: DocRange,
    field_id: FieldId,
    multi_valued: bool,
) -> Result<(PublishedStream, FieldCounts), IndexError>
where
    D: ArtifactDirectoryRead,
    S: ComponentBatchSink,
    F: MergeScratchFile,
{
    let mut cursors = inputs
        .iter()
        .map(|input| ColumnCursor::new(directory, input, field_id, multi_valued))
        .collect::<Result<Vec<_>, _>>()?;
    let mut permutation = FixedScratchReader::new_range(permutation, 8, range.first, range.end);
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::FAST_COLUMN,
        schema.codec_version(ComponentKind::FAST_COLUMN)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    let mut emitted = range.first;
    let mut values = Vec::new();
    let mut resident = 0usize;
    let mut counts = FieldCounts::default();
    while let Some(record) = permutation.next().await? {
        let (input, old) = decode_permutation(record, cursors.len())?;
        let cell = cursors[input].get(old).await?;
        counts.present_documents += u64::from(cell.present);
        counts.null_documents += u64::from(cell.null);
        counts.value_count = counts
            .value_count
            .checked_add(cell.values.len() as u64)
            .ok_or(IndexError::OffsetOverflow)?;
        counts.multi_valued_documents = counts
            .multi_valued_documents
            .checked_add(u64::from(
                cell.values.len().saturating_add(usize::from(cell.null)) > 1,
            ))
            .ok_or(IndexError::OffsetOverflow)?;
        for value in &cell.values {
            let count = match value {
                ScalarValue::Null => {
                    return Err(IndexError::InvalidFormat(
                        "fast-column value contains inline null",
                    ));
                }
                ScalarValue::Boolean(_) => &mut counts.boolean_values,
                ScalarValue::Number(_) => &mut counts.number_values,
                ScalarValue::Unsigned(_) => &mut counts.unsigned_values,
                ScalarValue::String(_) => &mut counts.string_values,
            };
            *count = count.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        }
        let bytes = cell_resident_bytes(&cell)?;
        if !values.is_empty() && resident.saturating_add(bytes) > INDEX_COMPONENT_BYTES / 2 {
            emitted = publish_column_values(
                directory,
                &mut publisher,
                field_id,
                multi_valued,
                emitted,
                &mut values,
            )
            .await?;
            resident = 0;
        }
        resident = resident
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        values.push(cell);
    }
    emitted = publish_column_values(
        directory,
        &mut publisher,
        field_id,
        multi_valued,
        emitted,
        &mut values,
    )
    .await?;
    if emitted != range.end {
        return Err(IndexError::InvalidFormat("column permutation count"));
    }
    if range.is_last() {
        for (cursor, input) in cursors.iter_mut().zip(inputs) {
            cursor.finish(input.document_count).await?;
        }
    }
    Ok((publisher.finish().await?, counts))
}

async fn publish_column_values<D: ArtifactDirectoryRead, S: ComponentBatchSink>(
    directory: &D,
    publisher: &mut StreamingComponentPublisher<'_, S>,
    field_id: FieldId,
    multi_valued: bool,
    first: u32,
    values: &mut Vec<FastColumnCell>,
) -> Result<u32, IndexError> {
    if values.is_empty() {
        return Ok(first);
    }
    let count = u32::try_from(values.len()).map_err(|_| IndexError::OffsetOverflow)?;
    let values = std::mem::take(values);
    let payload = directory
        .run_query_cpu(move || {
            FastColumnBlock::new(field_id, DocId::new(first), multi_valued, values)?
                .encode_payload()
        })
        .await?;
    push_doc_block(publisher, first, count, payload).await?;
    first.checked_add(count).ok_or(IndexError::OffsetOverflow)
}

// The remaining doc-aligned builders deliberately use the same bounded
// permutation/cursor pattern. They never retain a corpus-sized value array.

#[allow(clippy::too_many_arguments)]
async fn build_stored<D, S, F>(
    directory: &D,
    sink: &mut S,
    schema: &Schema,
    inputs: &[&SegmentDescriptor],
    identity: super::super::super::SegmentIdentity,
    permutation: &F,
    range: DocRange,
) -> Result<PublishedStream, IndexError>
where
    D: ArtifactDirectoryRead,
    S: ComponentBatchSink,
    F: MergeScratchFile,
{
    let mut cursors = inputs
        .iter()
        .map(|input| StoredCursor::new(directory, input))
        .collect::<Result<Vec<_>, _>>()?;
    let mut permutation = FixedScratchReader::new_range(permutation, 8, range.first, range.end);
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::STORED_FIELDS,
        schema.codec_version(ComponentKind::STORED_FIELDS)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    let mut values = Vec::new();
    let mut resident = 0usize;
    let mut emitted = range.first;
    while let Some(record) = permutation.next().await? {
        let (input, old) = decode_permutation(record, cursors.len())?;
        let value = cursors[input].get(old).await?;
        let bytes = value
            .as_ref()
            .map_or(1, |value| value.len().saturating_add(1));
        if !values.is_empty() && resident.saturating_add(bytes) > INDEX_COMPONENT_BYTES / 2 {
            emitted = emit_stored(directory, &mut publisher, emitted, &mut values).await?;
            resident = 0;
        }
        resident = resident
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        values.push(value);
    }
    emitted = emit_stored(directory, &mut publisher, emitted, &mut values).await?;
    if emitted != range.end {
        return Err(IndexError::InvalidFormat("stored permutation count"));
    }
    if range.is_last() {
        for (cursor, input) in cursors.iter_mut().zip(inputs) {
            cursor.finish(input.document_count).await?;
        }
    }
    publisher.finish().await
}

async fn emit_stored<D: ArtifactDirectoryRead, S: ComponentBatchSink>(
    directory: &D,
    publisher: &mut StreamingComponentPublisher<'_, S>,
    first: u32,
    values: &mut Vec<Option<Vec<u8>>>,
) -> Result<u32, IndexError> {
    let count = u32::try_from(values.len()).map_err(|_| IndexError::OffsetOverflow)?;
    if count == 0 {
        return Ok(first);
    }
    let values = std::mem::take(values);
    let payload = directory
        .run_query_cpu(move || StoredFieldsBlock::new(DocId::new(first), values)?.encode_payload())
        .await?;
    push_doc_block(publisher, first, count, payload).await?;
    first.checked_add(count).ok_or(IndexError::OffsetOverflow)
}

#[allow(clippy::too_many_arguments)]
async fn build_norms<D, S, F>(
    directory: &D,
    sink: &mut S,
    schema: &Schema,
    inputs: &[&SegmentDescriptor],
    identity: super::super::super::SegmentIdentity,
    permutation: &F,
    range: DocRange,
    field_id: FieldId,
) -> Result<(PublishedStream, u64, u64, Option<u32>, Option<u32>), IndexError>
where
    D: ArtifactDirectoryRead,
    S: ComponentBatchSink,
    F: MergeScratchFile,
{
    let mut cursors = inputs
        .iter()
        .map(|input| NormCursor::new(directory, input, field_id, 0))
        .collect::<Result<Vec<_>, _>>()?;
    let mut permutation = FixedScratchReader::new_range(permutation, 8, range.first, range.end);
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::NORMS,
        schema.codec_version(ComponentKind::NORMS)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    let mut values = Vec::with_capacity(4096);
    let mut emitted = range.first;
    let (mut present, mut total) = (0u64, 0u64);
    let (mut minimum, mut maximum) = (None::<u32>, None::<u32>);
    while let Some(record) = permutation.next().await? {
        let (input, old) = decode_permutation(record, cursors.len())?;
        let value = cursors[input].get(old).await?;
        if let Some(value) = value {
            present += 1;
            total = total
                .checked_add(u64::from(value))
                .ok_or(IndexError::OffsetOverflow)?;
            minimum = Some(minimum.map_or(value, |current| current.min(value)));
            maximum = Some(maximum.map_or(value, |current| current.max(value)));
        }
        values.push(value);
        if values.len() == 4096 {
            emitted = emit_norms(directory, &mut publisher, field_id, emitted, &mut values).await?;
        }
    }
    emitted = emit_norms(directory, &mut publisher, field_id, emitted, &mut values).await?;
    if emitted != range.end {
        return Err(IndexError::InvalidFormat("norm permutation count"));
    }
    if range.is_last() {
        for (cursor, input) in cursors.iter_mut().zip(inputs) {
            cursor.finish(input.document_count).await?;
        }
    }
    Ok((publisher.finish().await?, present, total, minimum, maximum))
}

async fn emit_norms<D: ArtifactDirectoryRead, S: ComponentBatchSink>(
    directory: &D,
    publisher: &mut StreamingComponentPublisher<'_, S>,
    field_id: FieldId,
    first: u32,
    values: &mut Vec<Option<u32>>,
) -> Result<u32, IndexError> {
    let count = u32::try_from(values.len()).map_err(|_| IndexError::OffsetOverflow)?;
    if count == 0 {
        return Ok(first);
    }
    let values = std::mem::take(values);
    let payload = directory
        .run_query_cpu(move || {
            NormBlock::new(field_id, DocId::new(first), values)?.encode_payload()
        })
        .await?;
    push_doc_block(publisher, first, count, payload).await?;
    first.checked_add(count).ok_or(IndexError::OffsetOverflow)
}

#[allow(clippy::too_many_arguments)]
async fn build_vectors<D, S, F>(
    directory: &D,
    sink: &mut S,
    schema: &Schema,
    inputs: &[&SegmentDescriptor],
    identity: super::super::super::SegmentIdentity,
    permutation: &F,
    range: DocRange,
    field_id: FieldId,
    dimensions: u32,
) -> Result<(PublishedStream, u64), IndexError>
where
    D: ArtifactDirectoryRead,
    S: ComponentBatchSink,
    F: MergeScratchFile,
{
    let mut cursors = inputs
        .iter()
        .map(|input| VectorCursor::new(directory, input, field_id, dimensions))
        .collect::<Result<Vec<_>, _>>()?;
    let mut permutation = FixedScratchReader::new_range(permutation, 8, range.first, range.end);
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::VECTORS,
        schema.codec_version(ComponentKind::VECTORS)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    let mut values = Vec::new();
    let mut resident = 0usize;
    let mut emitted = range.first;
    let mut present = 0u64;
    while let Some(record) = permutation.next().await? {
        let (input, old) = decode_permutation(record, cursors.len())?;
        let value = cursors[input].get(old).await?;
        present += u64::from(value.is_some());
        let bytes = value
            .as_ref()
            .map_or(1, |value| value.len().saturating_mul(4));
        if !values.is_empty() && resident.saturating_add(bytes) > INDEX_COMPONENT_BYTES / 2 {
            emitted = emit_vectors(
                directory,
                &mut publisher,
                field_id,
                dimensions,
                emitted,
                &mut values,
            )
            .await?;
            resident = 0;
        }
        resident = resident
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        values.push(value);
    }
    emitted = emit_vectors(
        directory,
        &mut publisher,
        field_id,
        dimensions,
        emitted,
        &mut values,
    )
    .await?;
    if emitted != range.end {
        return Err(IndexError::InvalidFormat("vector permutation count"));
    }
    if range.is_last() {
        for (cursor, input) in cursors.iter_mut().zip(inputs) {
            cursor.finish(input.document_count).await?;
        }
    }
    Ok((publisher.finish().await?, present))
}

async fn emit_vectors<D: ArtifactDirectoryRead, S: ComponentBatchSink>(
    directory: &D,
    publisher: &mut StreamingComponentPublisher<'_, S>,
    field_id: FieldId,
    dimensions: u32,
    first: u32,
    values: &mut Vec<Option<Vec<f32>>>,
) -> Result<u32, IndexError> {
    let count = u32::try_from(values.len()).map_err(|_| IndexError::OffsetOverflow)?;
    if count == 0 {
        return Ok(first);
    }
    let values = std::mem::take(values);
    let payload = directory
        .run_query_cpu(move || {
            VectorBlock::new(field_id, DocId::new(first), dimensions, values)?.encode_payload()
        })
        .await?;
    push_doc_block(publisher, first, count, payload).await?;
    first.checked_add(count).ok_or(IndexError::OffsetOverflow)
}

async fn push_doc_block<S: ComponentBatchSink>(
    publisher: &mut StreamingComponentPublisher<'_, S>,
    first: u32,
    count: u32,
    payload: Vec<u8>,
) -> Result<(), IndexError> {
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
        .await
}

fn decode_permutation(bytes: &[u8], input_count: usize) -> Result<(usize, u32), IndexError> {
    let input = u32::from_le_bytes(
        bytes
            .get(..4)
            .ok_or(IndexError::InvalidFormat("permutation input"))?
            .try_into()
            .map_err(|_| IndexError::InvalidFormat("permutation input width"))?,
    ) as usize;
    let old = u32::from_le_bytes(
        bytes
            .get(4..8)
            .ok_or(IndexError::InvalidFormat("permutation DocId"))?
            .try_into()
            .map_err(|_| IndexError::InvalidFormat("permutation DocId width"))?,
    );
    if input >= input_count {
        return Err(IndexError::InvalidFormat("permutation input ordinal"));
    }
    Ok((input, old))
}

fn cell_resident_bytes(cell: &FastColumnCell) -> Result<usize, IndexError> {
    cell.values
        .iter()
        .try_fold(std::mem::size_of::<FastColumnCell>(), |bytes, value| {
            bytes
                .checked_add(std::mem::size_of::<ScalarValue>())
                .and_then(|bytes| {
                    bytes.checked_add(match value {
                        ScalarValue::String(value) => value.len(),
                        _ => 0,
                    })
                })
                .ok_or(IndexError::OffsetOverflow)
        })
}

fn dimensions(schema: &Schema) -> Result<u32, IndexError> {
    match schema.semantics {
        IndexSemantics::Vector { dimensions, .. } | IndexSemantics::Hybrid { dimensions, .. } => {
            Ok(dimensions)
        }
        _ => Err(IndexError::InvalidFormat(
            "vector component has non-vector semantics",
        )),
    }
}

// Dense input cursors retain one checked component block and advance only in
// old DocId order for their input segment.

struct ColumnCursor<'a, D> {
    stream: RoutedBlockStream<'a, D>,
    field_id: FieldId,
    multi_valued: bool,
    block: Option<FastColumnBlock>,
    next_first: u32,
    last_requested: Option<u32>,
}

impl<'a, D: ArtifactDirectoryRead> ColumnCursor<'a, D> {
    fn new(
        directory: &'a D,
        input: &'a SegmentDescriptor,
        field_id: FieldId,
        multi_valued: bool,
    ) -> Result<Self, IndexError> {
        Ok(Self {
            stream: required_stream(
                directory,
                input,
                ComponentKind::FAST_COLUMN,
                Some(field_id),
                None,
            )?,
            field_id,
            multi_valued,
            block: None,
            next_first: 0,
            last_requested: None,
        })
    }

    async fn get(&mut self, doc: u32) -> Result<FastColumnCell, IndexError> {
        if self.last_requested.is_some_and(|previous| previous >= doc) {
            return Err(IndexError::InvalidFormat("non-monotonic column remap"));
        }
        self.last_requested = Some(doc);
        loop {
            if let Some(block) = &self.block
                && let Some(cell) = block.get(DocId::new(doc))
            {
                return Ok(cell.clone());
            }
            self.load().await?;
        }
    }

    async fn load(&mut self) -> Result<(), IndexError> {
        let (_, block) = self
            .stream
            .next(FastColumnBlock::decode_payload)
            .await?
            .ok_or(IndexError::InvalidFormat("fast-column stream ended early"))?;
        if block.field_id != self.field_id
            || block.multi_valued != self.multi_valued
            || block.first_doc_id.get() != self.next_first
        {
            return Err(IndexError::InvalidFormat("fast-column input coverage"));
        }
        self.next_first = self
            .next_first
            .checked_add(block.cells().len() as u32)
            .ok_or(IndexError::OffsetOverflow)?;
        self.block = Some(block);
        Ok(())
    }

    async fn finish(&mut self, total: u32) -> Result<(), IndexError> {
        while self.next_first < total {
            self.load().await?;
        }
        if self.next_first != total
            || self
                .stream
                .next(FastColumnBlock::decode_payload)
                .await?
                .is_some()
        {
            return Err(IndexError::InvalidFormat("fast-column input tail"));
        }
        Ok(())
    }
}

struct StoredCursor<'a, D> {
    stream: Option<RoutedBlockStream<'a, D>>,
    block: Option<StoredFieldsBlock>,
    next_first: u32,
    last_requested: Option<u32>,
}

impl<'a, D: ArtifactDirectoryRead> StoredCursor<'a, D> {
    fn new(directory: &'a D, input: &'a SegmentDescriptor) -> Result<Self, IndexError> {
        Ok(Self {
            stream: optional_stream(directory, input, ComponentKind::STORED_FIELDS, None, None)?,
            block: None,
            next_first: 0,
            last_requested: None,
        })
    }

    async fn get(&mut self, doc: u32) -> Result<Option<Vec<u8>>, IndexError> {
        if self.stream.is_none() {
            return Ok(None);
        }
        if self.last_requested.is_some_and(|previous| previous >= doc) {
            return Err(IndexError::InvalidFormat("non-monotonic stored remap"));
        }
        self.last_requested = Some(doc);
        loop {
            if let Some(block) = &self.block {
                let end = block
                    .first_doc_id
                    .get()
                    .checked_add(block.document_count() as u32)
                    .ok_or(IndexError::OffsetOverflow)?;
                if doc < end {
                    return Ok(block.get(DocId::new(doc)).map(<[u8]>::to_vec));
                }
            }
            self.load().await?;
        }
    }

    async fn load(&mut self) -> Result<(), IndexError> {
        let (_, block) = self
            .stream
            .as_mut()
            .expect("present stored stream")
            .next(StoredFieldsBlock::decode_payload)
            .await?
            .ok_or(IndexError::InvalidFormat("stored stream ended early"))?;
        if block.first_doc_id.get() != self.next_first {
            return Err(IndexError::InvalidFormat("stored input coverage"));
        }
        self.next_first = self
            .next_first
            .checked_add(block.document_count() as u32)
            .ok_or(IndexError::OffsetOverflow)?;
        self.block = Some(block);
        Ok(())
    }

    async fn finish(&mut self, total: u32) -> Result<(), IndexError> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(());
        };
        while self.next_first < total {
            let (_, block) = stream
                .next(StoredFieldsBlock::decode_payload)
                .await?
                .ok_or(IndexError::InvalidFormat("stored stream ended early"))?;
            if block.first_doc_id.get() != self.next_first {
                return Err(IndexError::InvalidFormat("stored input coverage"));
            }
            self.next_first += block.document_count() as u32;
        }
        if self.next_first != total
            || stream
                .next(StoredFieldsBlock::decode_payload)
                .await?
                .is_some()
        {
            return Err(IndexError::InvalidFormat("stored input tail"));
        }
        Ok(())
    }
}

macro_rules! dense_optional_cursor {
    ($name:ident, $block:ty, $kind:expr, $decode:path, $value:ty, $get:expr, $validate:expr) => {
        struct $name<'a, D> {
            stream: RoutedBlockStream<'a, D>,
            field_id: FieldId,
            block: Option<$block>,
            next_first: u32,
            last_requested: Option<u32>,
            validate_extra: u32,
        }

        impl<'a, D: ArtifactDirectoryRead> $name<'a, D> {
            fn new(
                directory: &'a D,
                input: &'a SegmentDescriptor,
                field_id: FieldId,
                validate_extra: u32,
            ) -> Result<Self, IndexError> {
                Ok(Self {
                    stream: required_stream(directory, input, $kind, Some(field_id), None)?,
                    field_id,
                    block: None,
                    next_first: 0,
                    last_requested: None,
                    validate_extra,
                })
            }

            async fn get(&mut self, doc: u32) -> Result<Option<$value>, IndexError> {
                if self.last_requested.is_some_and(|previous| previous >= doc) {
                    return Err(IndexError::InvalidFormat("non-monotonic dense remap"));
                }
                self.last_requested = Some(doc);
                loop {
                    if let Some(block) = &self.block {
                        let first = block.first_doc_id.get();
                        let count = block.values().len() as u32;
                        if (first..first + count).contains(&doc) {
                            return Ok(($get)(block, DocId::new(doc)));
                        }
                    }
                    self.load().await?;
                }
            }

            async fn load(&mut self) -> Result<(), IndexError> {
                let (_, block) = self
                    .stream
                    .next($decode)
                    .await?
                    .ok_or(IndexError::InvalidFormat("dense stream ended early"))?;
                if block.field_id != self.field_id
                    || block.first_doc_id.get() != self.next_first
                    || !($validate)(&block, self.validate_extra)
                {
                    return Err(IndexError::InvalidFormat("dense input coverage"));
                }
                self.next_first = self
                    .next_first
                    .checked_add(block.values().len() as u32)
                    .ok_or(IndexError::OffsetOverflow)?;
                self.block = Some(block);
                Ok(())
            }

            async fn finish(&mut self, total: u32) -> Result<(), IndexError> {
                while self.next_first < total {
                    self.load().await?;
                }
                if self.next_first != total || self.stream.next($decode).await?.is_some() {
                    return Err(IndexError::InvalidFormat("dense input tail"));
                }
                Ok(())
            }
        }
    };
}

dense_optional_cursor!(
    NormCursor,
    NormBlock,
    ComponentKind::NORMS,
    NormBlock::decode_payload,
    u32,
    |block: &NormBlock, doc| block.get(doc),
    |_block: &NormBlock, _extra| true
);

dense_optional_cursor!(
    VectorCursor,
    VectorBlock,
    ComponentKind::VECTORS,
    VectorBlock::decode_payload,
    Vec<f32>,
    |block: &VectorBlock, doc| block.get(doc).map(<[f32]>::to_vec),
    |block: &VectorBlock, dimensions| block.dimensions == dimensions
);
