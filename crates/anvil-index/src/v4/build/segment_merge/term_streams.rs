use crate::IndexError;

use super::super::super::{
    ArtifactDirectoryRead, ComponentKind, DocId, FieldComponents, FieldId, FieldType,
    INDEX_COMPONENT_BYTES, PositionEntry, PositionsBlock, PostingBlock, PostingImpact,
    PostingReference, Schema, SegmentDescriptor, SegmentIdentity, TERM_DICTIONARY_TARGET_BYTES,
    TERM_TYPE_BOOLEAN, TERM_TYPE_FIELD_PRESENCE, TERM_TYPE_HASHED_KEYWORD, TERM_TYPE_NULL,
    TERM_TYPE_STRING, TermDictionary, TermEntry, component_ordinal_key,
    decode_component_ordinal_key,
};
use super::super::ComponentBatchSink;
use super::super::scratch::{MergeScratchFile, MergeScratchSpace};
use super::super::sink::{PublishedStream, StreamingComponentPublisher};
use super::doc_components::FieldCounts;
use super::io::{RemapReader, RoutedBlockStream, optional_stream, required_stream};

pub(super) struct BuiltTermStreams {
    pub(super) streams: Vec<(ComponentKind, Option<FieldId>, PublishedStream)>,
    pub(super) counts: FieldCounts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TermRange {
    minimum: Option<Vec<u8>>,
    maximum_exclusive: Option<Vec<u8>>,
    first_posting_ordinal: u32,
    posting_ordinal_limit: u64,
}

pub(super) async fn plan_term_ranges<D: ArtifactDirectoryRead>(
    directory: &D,
    inputs: &[&SegmentDescriptor],
    field_id: FieldId,
    requested: usize,
) -> Result<Vec<TermRange>, IndexError> {
    let requested = requested.max(1);
    let mut leaf_count = 0usize;
    for input in inputs {
        let Some(mut stream) = optional_stream(
            directory,
            input,
            ComponentKind::TERM_DICTIONARY,
            Some(field_id),
            None,
        )?
        else {
            continue;
        };
        while stream.next_leaf().await?.is_some() {
            leaf_count = leaf_count
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
        }
    }
    if leaf_count == 0 {
        return Ok(Vec::new());
    }
    let range_count = requested.min(leaf_count);
    let targets = (1..range_count)
        .map(|ordinal| {
            leaf_count
                .saturating_mul(ordinal)
                .checked_div(range_count)
                .unwrap_or(0)
                .saturating_add(1)
        })
        .collect::<Vec<_>>();
    let mut streams = inputs
        .iter()
        .map(|input| {
            optional_stream(
                directory,
                input,
                ComponentKind::TERM_DICTIONARY,
                Some(field_id),
                None,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut current = Vec::with_capacity(streams.len());
    for stream in &mut streams {
        current.push(match stream {
            Some(stream) => stream.next_leaf().await?,
            None => None,
        });
    }
    let mut boundaries = Vec::with_capacity(targets.len());
    let mut seen = 0usize;
    let mut target = 0usize;
    while let Some(selected) = current
        .iter()
        .enumerate()
        .filter_map(|(index, leaf)| leaf.as_ref().map(|leaf| (index, &leaf.minimum_key)))
        .min_by(|(left_index, left), (right_index, right)| {
            left.cmp(right).then_with(|| left_index.cmp(right_index))
        })
        .map(|(index, _)| index)
    {
        seen = seen.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        while target < targets.len() && seen >= targets[target] {
            let boundary = current[selected]
                .as_ref()
                .expect("selected routed leaf")
                .minimum_key
                .clone();
            if boundaries.last() != Some(&boundary) {
                boundaries.push(boundary);
            }
            target += 1;
        }
        current[selected] = streams[selected]
            .as_mut()
            .expect("selected term stream")
            .next_leaf()
            .await?;
    }
    let actual_count = boundaries.len() + 1;
    let ordinal_space = u64::from(u32::MAX) + 1;
    let ordinal_span = ordinal_space.div_ceil(actual_count as u64);
    let mut output = Vec::with_capacity(actual_count);
    for ordinal in 0..actual_count {
        let base = (ordinal as u64)
            .checked_mul(ordinal_span)
            .ok_or(IndexError::OffsetOverflow)?;
        let limit = ((ordinal + 1) as u64)
            .checked_mul(ordinal_span)
            .ok_or(IndexError::OffsetOverflow)?
            .min(ordinal_space);
        output.push(TermRange {
            minimum: ordinal
                .checked_sub(1)
                .and_then(|index| boundaries.get(index).cloned()),
            maximum_exclusive: boundaries.get(ordinal).cloned(),
            first_posting_ordinal: u32::try_from(base).map_err(|_| IndexError::OffsetOverflow)?,
            posting_ordinal_limit: limit,
        });
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build_term_streams<D, S, W>(
    directory: &D,
    mut sink: S,
    workspace: &W,
    schema: &Schema,
    inputs: &[&SegmentDescriptor],
    remaps: &[W::File],
    identity: SegmentIdentity,
    field_id: FieldId,
    minimum_field_length: Option<u32>,
    range: TermRange,
) -> Result<BuiltTermStreams, IndexError>
where
    D: ArtifactDirectoryRead,
    S: ComponentBatchSink,
    W: MergeScratchSpace,
{
    let postings_file = workspace.create_file().await?;
    let positions_file = workspace.create_file().await?;
    let dictionaries_file = workspace.create_file().await?;
    let mut dictionaries = inputs
        .iter()
        .map(|input| DictionaryCursor::new(directory, input, field_id, &range))
        .collect::<Result<Vec<_>, _>>()?;
    for dictionary in &mut dictionaries {
        dictionary.advance().await?;
    }
    let mut posting_ordinal = range.first_posting_ordinal;
    let mut dictionary_buffer = Vec::new();
    let mut dictionary_bytes = 0usize;
    let mut unique_terms = 0u64;
    let mut term_present_documents = 0u64;
    let mut term_fallback_present_documents = 0u64;
    let mut term_presence_marker = false;
    let mut total_term_frequency = 0u64;
    let mut null_documents = 0u64;
    let mut value_count = 0u64;
    let mut boolean_values = 0u64;
    let mut string_values = 0u64;
    let field = &schema.fields[field_id.get() as usize];
    let terms_are_scalar_authority = !field.components.contains(FieldComponents::DOC_VALUES)
        && !field.components.contains(FieldComponents::POINTS)
        && matches!(field.field_type, FieldType::Boolean | FieldType::Keyword);
    while let Some(term) = dictionaries
        .iter()
        .filter_map(|cursor| cursor.current.as_ref().map(|entry| entry.term.as_slice()))
        .min()
        .map(<[u8]>::to_vec)
    {
        validate_term_field(&term, field_id)?;
        let selected = dictionaries
            .iter()
            .enumerate()
            .filter(|(_, cursor)| {
                cursor
                    .current
                    .as_ref()
                    .is_some_and(|entry| entry.term == term)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let first_ordinal = posting_ordinal;
        let mut occurrence_cursors = Vec::with_capacity(selected.len());
        for input in &selected {
            occurrence_cursors.push(
                PostingOccurrenceCursor::new(
                    directory,
                    inputs[*input],
                    field_id,
                    dictionaries[*input].current.as_ref().unwrap().postings,
                    remaps[*input].clone(),
                )
                .await?,
            );
        }
        for cursor in &mut occurrence_cursors {
            cursor.advance().await?;
        }
        let mut output = Vec::<Occurrence>::new();
        let mut output_bytes = 0usize;
        let mut live_frequency = 0u64;
        let mut live_total_term_frequency = 0u64;
        let mut last_doc = None;
        while let Some(selected_cursor) = occurrence_cursors
            .iter()
            .enumerate()
            .filter_map(|(index, cursor)| {
                cursor.current.as_ref().map(|value| (index, value.doc_id))
            })
            .min_by_key(|(_, doc)| *doc)
            .map(|(index, _)| index)
        {
            let occurrence = occurrence_cursors[selected_cursor]
                .current
                .take()
                .expect("selected posting occurrence");
            if last_doc.is_some_and(|previous| previous >= occurrence.doc_id) {
                return Err(IndexError::InvalidFormat(
                    "translated postings are not unique and ordered",
                ));
            }
            last_doc = Some(occurrence.doc_id);
            let bytes = occurrence.resident_bytes()?;
            if !output.is_empty() && output_bytes.saturating_add(bytes) > INDEX_COMPONENT_BYTES / 2
            {
                ensure_ordinal_capacity(posting_ordinal, range.posting_ordinal_limit)?;
                write_posting_leaf(
                    directory,
                    &postings_file,
                    &positions_file,
                    posting_ordinal,
                    minimum_field_length,
                    &mut output,
                )
                .await?;
                posting_ordinal = posting_ordinal
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                output_bytes = 0;
            }
            output_bytes = output_bytes
                .checked_add(bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            output.push(occurrence);
            live_frequency = live_frequency
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
            live_total_term_frequency = live_total_term_frequency
                .checked_add(u64::from(
                    output.last().expect("just pushed occurrence").frequency,
                ))
                .ok_or(IndexError::OffsetOverflow)?;
            occurrence_cursors[selected_cursor].advance().await?;
        }
        if !output.is_empty() {
            ensure_ordinal_capacity(posting_ordinal, range.posting_ordinal_limit)?;
            write_posting_leaf(
                directory,
                &postings_file,
                &positions_file,
                posting_ordinal,
                minimum_field_length,
                &mut output,
            )
            .await?;
            posting_ordinal = posting_ordinal
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        if live_frequency != 0 {
            let term_type = *term
                .get(4)
                .ok_or(IndexError::InvalidFormat("term key lacks a type tag"))?;
            if term_type == TERM_TYPE_FIELD_PRESENCE {
                term_presence_marker = true;
                term_present_documents = term_present_documents
                    .checked_add(live_frequency)
                    .ok_or(IndexError::OffsetOverflow)?;
            } else {
                unique_terms = unique_terms
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                term_fallback_present_documents = term_fallback_present_documents
                    .checked_add(live_frequency)
                    .ok_or(IndexError::OffsetOverflow)?;
                total_term_frequency = total_term_frequency
                    .checked_add(live_total_term_frequency)
                    .ok_or(IndexError::OffsetOverflow)?;
                if terms_are_scalar_authority {
                    match term_type {
                        TERM_TYPE_NULL => {
                            null_documents = null_documents
                                .checked_add(live_frequency)
                                .ok_or(IndexError::OffsetOverflow)?;
                        }
                        TERM_TYPE_BOOLEAN => {
                            value_count = value_count
                                .checked_add(live_total_term_frequency)
                                .ok_or(IndexError::OffsetOverflow)?;
                            boolean_values = boolean_values
                                .checked_add(live_total_term_frequency)
                                .ok_or(IndexError::OffsetOverflow)?;
                        }
                        TERM_TYPE_STRING | TERM_TYPE_HASHED_KEYWORD => {
                            value_count = value_count
                                .checked_add(live_total_term_frequency)
                                .ok_or(IndexError::OffsetOverflow)?;
                            string_values = string_values
                                .checked_add(live_total_term_frequency)
                                .ok_or(IndexError::OffsetOverflow)?;
                        }
                        _ => {}
                    }
                }
            }
            let entry = TermEntry {
                term,
                postings: PostingReference {
                    document_frequency: live_frequency,
                    total_term_frequency: live_total_term_frequency,
                    first_component_ordinal: first_ordinal,
                    component_count: posting_ordinal
                        .checked_sub(first_ordinal)
                        .ok_or(IndexError::OffsetOverflow)?,
                },
            };
            let bytes = entry.term.len().saturating_add(28);
            if !dictionary_buffer.is_empty()
                && dictionary_bytes.saturating_add(bytes) > TERM_DICTIONARY_TARGET_BYTES
            {
                write_dictionary_leaf(directory, &dictionaries_file, &mut dictionary_buffer)
                    .await?;
                dictionary_bytes = 0;
            }
            dictionary_bytes = dictionary_bytes
                .checked_add(bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            dictionary_buffer.push(entry);
        }
        for input in selected {
            dictionaries[input].advance().await?;
        }
    }
    if !dictionary_buffer.is_empty() {
        write_dictionary_leaf(directory, &dictionaries_file, &mut dictionary_buffer).await?;
    }
    if posting_ordinal == range.first_posting_ordinal {
        return Ok(BuiltTermStreams {
            streams: Vec::new(),
            counts: FieldCounts::default(),
        });
    }
    let mut streams = Vec::new();
    streams.push((
        ComponentKind::POSTINGS,
        Some(field_id),
        publish_ordinal_file(
            &mut sink,
            schema,
            identity,
            ComponentKind::POSTINGS,
            postings_file,
        )
        .await?
        .ok_or(IndexError::InvalidFormat("nonempty postings scratch"))?,
    ));
    if let Some(positions) = publish_ordinal_file(
        &mut sink,
        schema,
        identity,
        ComponentKind::POSITIONS,
        positions_file,
    )
    .await?
    {
        streams.push((ComponentKind::POSITIONS, Some(field_id), positions));
    }
    streams.push((
        ComponentKind::TERM_DICTIONARY,
        Some(field_id),
        publish_dictionary_file(&mut sink, schema, identity, dictionaries_file)
            .await?
            .ok_or(IndexError::InvalidFormat("nonempty dictionary scratch"))?,
    ));
    Ok(BuiltTermStreams {
        streams,
        counts: FieldCounts {
            present_documents: term_present_documents,
            null_documents,
            value_count,
            unique_terms,
            total_term_frequency,
            boolean_values,
            string_values,
            term_presence_marker,
            term_fallback_present_documents,
            ..FieldCounts::default()
        },
    })
}

fn ensure_ordinal_capacity(next: u32, limit: u64) -> Result<(), IndexError> {
    if u64::from(next) >= limit {
        return Err(IndexError::ResourceLimit {
            needed: usize::try_from(u64::from(next) + 1).unwrap_or(usize::MAX),
            limit: usize::try_from(limit).unwrap_or(usize::MAX),
        });
    }
    Ok(())
}

#[derive(Clone)]
struct Occurrence {
    doc_id: DocId,
    frequency: u32,
    positions: Vec<u32>,
}

impl Occurrence {
    fn resident_bytes(&self) -> Result<usize, IndexError> {
        std::mem::size_of::<Self>()
            .checked_add(self.positions.len().saturating_mul(4))
            .ok_or(IndexError::OffsetOverflow)
    }
}

async fn write_posting_leaf<D: ArtifactDirectoryRead, F: MergeScratchFile>(
    directory: &D,
    postings: &F,
    positions: &F,
    ordinal: u32,
    minimum_field_length: Option<u32>,
    occurrences: &mut Vec<Occurrence>,
) -> Result<(), IndexError> {
    let values = std::mem::take(occurrences);
    let (posting_count, posting_payload, position_count, position_payload) = directory
        .run_query_cpu(move || {
            let posting_count = values.len() as u64;
            let posting = PostingBlock::with_frequencies(
                values.iter().map(|value| value.doc_id).collect(),
                Some(values.iter().map(|value| value.frequency).collect()),
                minimum_field_length.map(|minimum_field_length| PostingImpact {
                    maximum_frequency: values
                        .iter()
                        .map(|value| value.frequency)
                        .max()
                        .expect("nonempty posting leaf"),
                    minimum_field_length,
                }),
            )?;
            let entries = values
                .into_iter()
                .filter(|value| !value.positions.is_empty())
                .map(|value| PositionEntry {
                    doc_id: value.doc_id,
                    positions: value.positions,
                })
                .collect::<Vec<_>>();
            let position_count = entries.len() as u64;
            let position_payload = if entries.is_empty() {
                None
            } else {
                Some(PositionsBlock::new(entries)?.encode_payload()?)
            };
            Ok((
                posting_count,
                posting.encode_payload()?,
                position_count,
                position_payload,
            ))
        })
        .await?;
    append_ordinal_leaf(postings, ordinal, posting_count, posting_payload).await?;
    if let Some(payload) = position_payload {
        append_ordinal_leaf(positions, ordinal, position_count, payload).await?;
    }
    Ok(())
}

async fn append_ordinal_leaf<F: MergeScratchFile>(
    file: &F,
    ordinal: u32,
    count: u64,
    payload: Vec<u8>,
) -> Result<(), IndexError> {
    let mut bytes = Vec::with_capacity(16 + payload.len());
    bytes.extend_from_slice(&ordinal.to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| IndexError::OffsetOverflow)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&payload);
    file.append(bytes).await?;
    Ok(())
}

async fn write_dictionary_leaf<D: ArtifactDirectoryRead, F: MergeScratchFile>(
    directory: &D,
    file: &F,
    entries: &mut Vec<TermEntry>,
) -> Result<(), IndexError> {
    let entries = std::mem::take(entries);
    let (first, last, count, payload) = directory
        .run_query_cpu(move || {
            let dictionary = TermDictionary::new(entries)?;
            Ok((
                dictionary.entries().first().unwrap().term.clone(),
                dictionary.entries().last().unwrap().term.clone(),
                dictionary.entries().len() as u64,
                dictionary.encode_payload()?,
            ))
        })
        .await?;
    let mut bytes = Vec::with_capacity(16 + first.len() + last.len() + payload.len());
    for key in [&first, &last] {
        bytes.extend_from_slice(
            &u32::try_from(key.len())
                .map_err(|_| IndexError::OffsetOverflow)?
                .to_le_bytes(),
        );
        bytes.extend_from_slice(key);
    }
    bytes.extend_from_slice(&count.to_le_bytes());
    bytes.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| IndexError::OffsetOverflow)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&payload);
    file.append(bytes).await?;
    Ok(())
}

async fn publish_ordinal_file<S: ComponentBatchSink, F: MergeScratchFile>(
    sink: &mut S,
    schema: &Schema,
    identity: SegmentIdentity,
    kind: ComponentKind,
    file: F,
) -> Result<Option<PublishedStream>, IndexError> {
    let length = file.len().await?;
    if length == 0 {
        return Ok(None);
    }
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        kind,
        schema.codec_version(kind)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    let mut offset = 0u64;
    while offset < length {
        let header = file.read_exact_at(offset, 16).await?;
        let ordinal = read_u32(&header, 0)?;
        let count = read_u64(&header, 4)?;
        let payload_len = read_u32(&header, 12)? as usize;
        let payload = file.read_exact_at(offset + 16, payload_len).await?;
        let key = component_ordinal_key(ordinal).to_vec();
        publisher
            .push_payload(key.clone(), key, count, payload)
            .await?;
        offset = offset
            .checked_add(16 + payload_len as u64)
            .ok_or(IndexError::OffsetOverflow)?;
    }
    if offset != length {
        return Err(IndexError::InvalidFormat("ordinal scratch tail"));
    }
    publisher.finish().await.map(Some)
}

async fn publish_dictionary_file<S: ComponentBatchSink, F: MergeScratchFile>(
    sink: &mut S,
    schema: &Schema,
    identity: SegmentIdentity,
    file: F,
) -> Result<Option<PublishedStream>, IndexError> {
    let length = file.len().await?;
    if length == 0 {
        return Ok(None);
    }
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::TERM_DICTIONARY,
        schema.codec_version(ComponentKind::TERM_DICTIONARY)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    let mut offset = 0u64;
    while offset < length {
        let (first, next) = read_scratch_bytes(&file, offset).await?;
        let (last, next) = read_scratch_bytes(&file, next).await?;
        let header = file.read_exact_at(next, 12).await?;
        let count = read_u64(&header, 0)?;
        let payload_len = read_u32(&header, 8)? as usize;
        let payload = file.read_exact_at(next + 12, payload_len).await?;
        publisher.push_payload(first, last, count, payload).await?;
        offset = next
            .checked_add(12 + payload_len as u64)
            .ok_or(IndexError::OffsetOverflow)?;
    }
    if offset != length {
        return Err(IndexError::InvalidFormat("dictionary scratch tail"));
    }
    publisher.finish().await.map(Some)
}

async fn read_scratch_bytes<F: MergeScratchFile>(
    file: &F,
    offset: u64,
) -> Result<(Vec<u8>, u64), IndexError> {
    let prefix = file.read_exact_at(offset, 4).await?;
    let length = read_u32(&prefix, 0)? as usize;
    let value = file.read_exact_at(offset + 4, length).await?;
    Ok((value, offset + 4 + length as u64))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, IndexError> {
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or(IndexError::InvalidFormat("scratch u32"))?
            .try_into()
            .map_err(|_| IndexError::InvalidFormat("scratch u32 width"))?,
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, IndexError> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or(IndexError::InvalidFormat("scratch u64"))?
            .try_into()
            .map_err(|_| IndexError::InvalidFormat("scratch u64 width"))?,
    ))
}

struct DictionaryCursor<'a, D> {
    stream: Option<RoutedBlockStream<'a, D>>,
    entries: std::vec::IntoIter<TermEntry>,
    current: Option<TermEntry>,
    previous: Option<Vec<u8>>,
    minimum: Option<Vec<u8>>,
    maximum_exclusive: Option<Vec<u8>>,
}

impl<'a, D: ArtifactDirectoryRead> DictionaryCursor<'a, D> {
    fn new(
        directory: &'a D,
        input: &'a SegmentDescriptor,
        field_id: FieldId,
        range: &TermRange,
    ) -> Result<Self, IndexError> {
        let routed_range = match (&range.minimum, &range.maximum_exclusive) {
            (None, None) => None,
            (minimum, maximum) => Some((
                minimum
                    .clone()
                    .unwrap_or_else(|| field_id.get().to_be_bytes().to_vec()),
                maximum.clone().unwrap_or_else(|| {
                    let mut value = field_id.get().to_be_bytes().to_vec();
                    value.extend_from_slice(&[u8::MAX; 2]);
                    value
                }),
            )),
        };
        Ok(Self {
            stream: optional_stream(
                directory,
                input,
                ComponentKind::TERM_DICTIONARY,
                Some(field_id),
                routed_range,
            )?,
            entries: Vec::new().into_iter(),
            current: None,
            previous: None,
            minimum: range.minimum.clone(),
            maximum_exclusive: range.maximum_exclusive.clone(),
        })
    }

    async fn advance(&mut self) -> Result<(), IndexError> {
        loop {
            if let Some(entry) = self.entries.next() {
                if self
                    .minimum
                    .as_ref()
                    .is_some_and(|minimum| entry.term < *minimum)
                {
                    continue;
                }
                if self
                    .maximum_exclusive
                    .as_ref()
                    .is_some_and(|maximum| entry.term >= *maximum)
                {
                    self.current = None;
                    self.stream = None;
                    return Ok(());
                }
                if self
                    .previous
                    .as_ref()
                    .is_some_and(|value| value >= &entry.term)
                {
                    return Err(IndexError::InvalidFormat(
                        "term dictionaries are not globally ordered",
                    ));
                }
                self.previous = Some(entry.term.clone());
                self.current = Some(entry);
                return Ok(());
            }
            let Some(stream) = self.stream.as_mut() else {
                self.current = None;
                return Ok(());
            };
            let Some((_, block)) = stream.next(TermDictionary::decode_payload).await? else {
                self.current = None;
                return Ok(());
            };
            self.entries = block.entries().to_vec().into_iter();
        }
    }
}

struct PostingOccurrenceCursor<'a, D, F> {
    postings: RoutedBlockStream<'a, D>,
    positions: Option<RoutedBlockStream<'a, D>>,
    pending_position: Option<(u32, PositionsBlock)>,
    posting: Option<PostingBlock>,
    posting_positions: Vec<PositionEntry>,
    posting_index: usize,
    next_ordinal: u32,
    end_ordinal: u32,
    expected_frequency: u64,
    observed_frequency: u64,
    expected_total_term_frequency: u64,
    observed_total_term_frequency: u64,
    remap: RemapReader<F>,
    current: Option<Occurrence>,
}

impl<'a, D: ArtifactDirectoryRead, F: MergeScratchFile> PostingOccurrenceCursor<'a, D, F> {
    async fn new(
        directory: &'a D,
        input: &'a SegmentDescriptor,
        field_id: FieldId,
        reference: PostingReference,
        remap_file: F,
    ) -> Result<Self, IndexError> {
        let end = reference
            .first_component_ordinal
            .checked_add(reference.component_count)
            .ok_or(IndexError::OffsetOverflow)?;
        if reference.component_count == 0 {
            return Err(IndexError::InvalidFormat("posting reference is empty"));
        }
        let range = Some((
            component_ordinal_key(reference.first_component_ordinal).to_vec(),
            component_ordinal_key(end - 1).to_vec(),
        ));
        let postings = required_stream(
            directory,
            input,
            ComponentKind::POSTINGS,
            Some(field_id),
            range.clone(),
        )?;
        let mut positions = optional_stream(
            directory,
            input,
            ComponentKind::POSITIONS,
            Some(field_id),
            range,
        )?;
        let pending_position = match positions.as_mut() {
            Some(stream) => stream
                .next(PositionsBlock::decode_payload)
                .await?
                .map(|(leaf, block)| Ok((exact_ordinal(&leaf)?, block)))
                .transpose()?,
            None => None,
        };
        Ok(Self {
            postings,
            positions,
            pending_position,
            posting: None,
            posting_positions: Vec::new(),
            posting_index: 0,
            next_ordinal: reference.first_component_ordinal,
            end_ordinal: end,
            expected_frequency: reference.document_frequency,
            observed_frequency: 0,
            expected_total_term_frequency: reference.total_term_frequency,
            observed_total_term_frequency: 0,
            remap: RemapReader::new(remap_file, input.document_count),
            current: None,
        })
    }

    async fn advance(&mut self) -> Result<(), IndexError> {
        self.current = None;
        loop {
            if self
                .posting
                .as_ref()
                .is_none_or(|posting| self.posting_index == posting.doc_ids().len())
            {
                if self.next_ordinal == self.end_ordinal {
                    if self.observed_frequency != self.expected_frequency
                        || self.observed_total_term_frequency != self.expected_total_term_frequency
                        || self.pending_position.is_some()
                    {
                        return Err(IndexError::InvalidFormat("posting reference summary"));
                    }
                    return Ok(());
                }
                self.load_posting().await?;
            }
            let posting = self.posting.as_ref().unwrap();
            let old = posting.doc_ids()[self.posting_index].get();
            let frequency = posting
                .frequencies()
                .map_or(1, |values| values[self.posting_index]);
            let positions = self
                .posting_positions
                .binary_search_by_key(&DocId::new(old), |entry| entry.doc_id)
                .ok()
                .map(|index| self.posting_positions[index].positions.clone())
                .unwrap_or_default();
            self.posting_index += 1;
            self.observed_frequency = self
                .observed_frequency
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
            self.observed_total_term_frequency = self
                .observed_total_term_frequency
                .checked_add(u64::from(frequency))
                .ok_or(IndexError::OffsetOverflow)?;
            if let Some(new) = self.remap.get(old).await? {
                self.current = Some(Occurrence {
                    doc_id: DocId::new(new),
                    frequency,
                    positions,
                });
                return Ok(());
            }
        }
    }

    async fn load_posting(&mut self) -> Result<(), IndexError> {
        let (leaf, posting) = self
            .postings
            .next(PostingBlock::decode_payload)
            .await?
            .ok_or(IndexError::InvalidFormat("posting stream ended early"))?;
        let ordinal = exact_ordinal(&leaf)?;
        if ordinal != self.next_ordinal {
            return Err(IndexError::InvalidFormat("posting ordinal coverage"));
        }
        self.posting_positions = match self.pending_position.take() {
            Some((position_ordinal, block)) if position_ordinal == ordinal => {
                self.pending_position = match self.positions.as_mut() {
                    Some(stream) => stream
                        .next(PositionsBlock::decode_payload)
                        .await?
                        .map(|(leaf, block)| Ok((exact_ordinal(&leaf)?, block)))
                        .transpose()?,
                    None => None,
                };
                block.entries().to_vec()
            }
            Some(value) if value.0 > ordinal => {
                self.pending_position = Some(value);
                Vec::new()
            }
            Some(_) => return Err(IndexError::InvalidFormat("position ordinal order")),
            None => Vec::new(),
        };
        if self
            .posting_positions
            .iter()
            .any(|entry| posting.doc_ids().binary_search(&entry.doc_id).is_err())
        {
            return Err(IndexError::InvalidFormat("position without posting"));
        }
        self.posting = Some(posting);
        self.posting_index = 0;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(())
    }
}

fn exact_ordinal(leaf: &super::super::super::StreamLeaf) -> Result<u32, IndexError> {
    if leaf.minimum_key != leaf.maximum_key {
        return Err(IndexError::InvalidFormat("ordinal leaf range"));
    }
    decode_component_ordinal_key(&leaf.minimum_key)
}

fn validate_term_field(term: &[u8], field_id: FieldId) -> Result<(), IndexError> {
    let field = term
        .get(..4)
        .ok_or(IndexError::InvalidFormat("canonical term field"))?;
    if u32::from_be_bytes(
        field
            .try_into()
            .map_err(|_| IndexError::InvalidFormat("canonical term field width"))?,
    ) != field_id.get()
        || term.get(4).copied().unwrap_or(0) == 0
        || term.len() <= 5
    {
        return Err(IndexError::InvalidFormat("canonical term key"));
    }
    Ok(())
}
