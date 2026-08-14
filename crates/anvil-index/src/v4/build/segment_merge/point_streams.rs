use crate::IndexError;
use crate::compaction::CompactionExecutor;

use super::super::super::{
    ArtifactDirectoryRead, ComponentKind, DocId, FieldId, FieldType, PointBlock, PointEntry,
    PointValue, ScalarValue, Schema, SegmentDescriptor, SegmentIdentity, point_entry_key,
};
use super::super::sink::{PublishedStream, StreamingComponentPublisher};
use super::super::{ComponentBatchSink, MergeScratchFile, MergeScratchSpace};
use super::doc_components::FieldCounts;
use super::io::{RemapReader, required_stream};

const SORT_FAN_IN: usize = 16;
const RECORD_BYTES: usize = 13;
const POINTS_PER_BLOCK: usize = 4096;

pub(super) struct BuiltPointStream {
    pub(super) stream: PublishedStream,
    pub(super) counts: FieldCounts,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn build_point_stream<D, S, W, E>(
    directory: &D,
    mut sink: S,
    workspace: &W,
    schema: &Schema,
    inputs: &[&SegmentDescriptor],
    remaps: &[W::File],
    identity: SegmentIdentity,
    field_id: FieldId,
    sort_bytes: usize,
    executor: E,
) -> Result<BuiltPointStream, IndexError>
where
    D: ArtifactDirectoryRead,
    S: ComponentBatchSink,
    W: MergeScratchSpace,
    E: CompactionExecutor,
{
    if inputs.len() != remaps.len() {
        return Err(IndexError::InvalidFormat("point remap input width"));
    }
    let field_type = schema
        .fields
        .get(field_id.get() as usize)
        .filter(|field| field.id == field_id)
        .map(|field| field.field_type)
        .ok_or(IndexError::InvalidFormat("point field is outside schema"))?;
    let mut sorter = PointSorter::new(workspace, sort_bytes, executor)?;
    for (input, remap) in inputs.iter().zip(remaps) {
        let mut stream = required_stream(
            directory,
            input,
            ComponentKind::POINTS,
            Some(field_id),
            None,
        )?;
        let mut remap = RemapReader::new(remap.clone(), input.document_count);
        while let Some((leaf, block)) = stream.next(PointBlock::decode_payload).await? {
            if block.field_id != field_id
                || point_entry_key(
                    field_id,
                    &block.minimum_entry().value,
                    block.minimum_entry().doc_id,
                )? != leaf.minimum_key
                || point_entry_key(
                    field_id,
                    &block.maximum_entry().value,
                    block.maximum_entry().doc_id,
                )? != leaf.maximum_key
                || block.entries().len() as u64 != leaf.element_count
            {
                return Err(IndexError::InvalidFormat("point stream routing evidence"));
            }
            for entry in block.entries() {
                if !point_matches_type(&entry.value, field_type) {
                    return Err(IndexError::InvalidFormat(
                        "point value differs from field type",
                    ));
                }
                if let Some(new_doc_id) = remap.get(entry.doc_id.get()).await? {
                    sorter
                        .push(PointEntry {
                            value: entry.value.clone(),
                            doc_id: DocId::new(new_doc_id),
                        })
                        .await?;
                }
            }
        }
    }
    let run = sorter.finish().await?;
    publish_points(&mut sink, schema, identity, field_id, run).await
}

fn point_matches_type(value: &PointValue, field_type: FieldType) -> bool {
    matches!(
        (value, field_type),
        (PointValue::Presence | PointValue::Null, _)
            | (
                PointValue::Value(ScalarValue::Signed(_)),
                FieldType::SignedInteger
            )
            | (
                PointValue::Value(ScalarValue::Unsigned(_)),
                FieldType::UnsignedInteger
            )
            | (PointValue::Value(ScalarValue::Number(_)), FieldType::Float)
    )
}

async fn publish_points<S, F>(
    sink: &mut S,
    schema: &Schema,
    identity: SegmentIdentity,
    field_id: FieldId,
    run: F,
) -> Result<BuiltPointStream, IndexError>
where
    S: ComponentBatchSink,
    F: MergeScratchFile,
{
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::POINTS,
        schema.codec_version(ComponentKind::POINTS)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    let mut cursor = PointCursor::new(run).await?;
    let mut entries = Vec::with_capacity(POINTS_PER_BLOCK);
    let mut counts = FieldCounts::default();
    while let Some(entry) = cursor.next().await? {
        match &entry.value {
            PointValue::Presence => {
                counts.present_documents = counts
                    .present_documents
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            PointValue::Null => {
                counts.null_documents = counts
                    .null_documents
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            PointValue::Value(ScalarValue::Signed(_) | ScalarValue::Number(_)) => {
                counts.number_values = counts
                    .number_values
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                counts.value_count = counts
                    .value_count
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            PointValue::Value(ScalarValue::Unsigned(_)) => {
                counts.unsigned_values = counts
                    .unsigned_values
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                counts.value_count = counts
                    .value_count
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            PointValue::Value(_) => {
                return Err(IndexError::InvalidFormat(
                    "non-numeric point scratch record",
                ));
            }
        }
        entries.push(entry);
        if entries.len() == POINTS_PER_BLOCK {
            emit_block(&mut publisher, field_id, &mut entries).await?;
        }
    }
    emit_block(&mut publisher, field_id, &mut entries).await?;
    Ok(BuiltPointStream {
        stream: publisher.finish().await?,
        counts,
    })
}

async fn emit_block<S: ComponentBatchSink>(
    publisher: &mut StreamingComponentPublisher<'_, S>,
    field_id: FieldId,
    entries: &mut Vec<PointEntry>,
) -> Result<(), IndexError> {
    if entries.is_empty() {
        return Ok(());
    }
    let block = PointBlock::new(field_id, std::mem::take(entries))?;
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
    let count = block.entries().len() as u64;
    publisher
        .push_payload(minimum, maximum, count, block.encode_payload()?)
        .await
}

struct PointSorter<'a, W: MergeScratchSpace, E: CompactionExecutor> {
    workspace: &'a W,
    executor: E,
    chunk_limit: usize,
    chunk: Vec<PointEntry>,
    levels: Vec<Vec<W::File>>,
}

impl<'a, W: MergeScratchSpace, E: CompactionExecutor> PointSorter<'a, W, E> {
    fn new(workspace: &'a W, chunk_limit: usize, executor: E) -> Result<Self, IndexError> {
        let minimum = RECORD_BYTES * 1024;
        if chunk_limit < minimum {
            return Err(IndexError::ResourceLimit {
                needed: minimum,
                limit: chunk_limit,
            });
        }
        Ok(Self {
            workspace,
            executor,
            chunk_limit,
            chunk: Vec::new(),
            levels: Vec::new(),
        })
    }

    async fn push(&mut self, entry: PointEntry) -> Result<(), IndexError> {
        if self
            .chunk
            .len()
            .saturating_mul(std::mem::size_of::<PointEntry>())
            >= self.chunk_limit
        {
            self.flush().await?;
        }
        self.chunk.push(entry);
        Ok(())
    }

    async fn finish(mut self) -> Result<W::File, IndexError> {
        self.flush().await?;
        loop {
            let total = self.levels.iter().map(Vec::len).sum::<usize>();
            if total == 0 {
                return Err(IndexError::InvalidFormat("merged point stream is empty"));
            }
            if total == 1 {
                return Ok(self
                    .levels
                    .iter_mut()
                    .find_map(Vec::pop)
                    .expect("one point run"));
            }
            let mut inputs = Vec::with_capacity(SORT_FAN_IN);
            for level in &mut self.levels {
                while inputs.len() < SORT_FAN_IN {
                    let Some(file) = level.pop() else { break };
                    inputs.push(file);
                }
                if inputs.len() == SORT_FAN_IN {
                    break;
                }
            }
            let merged = merge_runs(self.workspace, inputs).await?;
            self.insert_run(self.levels.len(), merged).await?;
        }
    }

    async fn flush(&mut self) -> Result<(), IndexError> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::take(&mut self.chunk);
        let bytes = self.executor.run_cpu(move || encode_sorted(chunk)).await?;
        let file = self.workspace.create_file().await?;
        file.append(bytes).await?;
        self.insert_run(0, file).await
    }

    async fn insert_run(&mut self, mut level: usize, mut file: W::File) -> Result<(), IndexError> {
        loop {
            if self.levels.len() <= level {
                self.levels.resize_with(level + 1, Vec::new);
            }
            self.levels[level].push(file);
            if self.levels[level].len() <= SORT_FAN_IN {
                return Ok(());
            }
            let inputs = self.levels[level].drain(..SORT_FAN_IN).collect();
            file = merge_runs(self.workspace, inputs).await?;
            level += 1;
        }
    }
}

fn encode_sorted(mut entries: Vec<PointEntry>) -> Result<Vec<u8>, IndexError> {
    entries.sort_unstable_by(compare_entry);
    let mut output = Vec::with_capacity(entries.len().saturating_mul(RECORD_BYTES));
    for entry in entries {
        encode_entry(&entry, &mut output)?;
    }
    Ok(output)
}

async fn merge_runs<W: MergeScratchSpace>(
    workspace: &W,
    inputs: Vec<W::File>,
) -> Result<W::File, IndexError> {
    if inputs.is_empty() || inputs.len() > SORT_FAN_IN {
        return Err(IndexError::InvalidFormat("point scratch fan-in"));
    }
    let mut cursors = Vec::with_capacity(inputs.len());
    for input in inputs {
        let mut cursor = PointCursor::new(input).await?;
        cursor.advance().await?;
        cursors.push(cursor);
    }
    let output = workspace.create_file().await?;
    let mut buffer = Vec::with_capacity(64 * 1024);
    while let Some(selected) = cursors
        .iter()
        .enumerate()
        .filter_map(|(index, cursor)| cursor.current.as_ref().map(|entry| (index, entry)))
        .min_by(|(_, left), (_, right)| compare_entry(left, right))
        .map(|(index, _)| index)
    {
        encode_entry(
            cursors[selected].current.as_ref().expect("selected point"),
            &mut buffer,
        )?;
        cursors[selected].advance().await?;
        if buffer.len() >= 64 * 1024 {
            output.append(std::mem::take(&mut buffer)).await?;
        }
    }
    if !buffer.is_empty() {
        output.append(buffer).await?;
    }
    Ok(output)
}

fn compare_entry(left: &PointEntry, right: &PointEntry) -> std::cmp::Ordering {
    left.value
        .cmp(&right.value)
        .then_with(|| left.doc_id.cmp(&right.doc_id))
}

fn encode_entry(entry: &PointEntry, output: &mut Vec<u8>) -> Result<(), IndexError> {
    let (tag, bits) = match entry.value {
        PointValue::Presence => (0, 0),
        PointValue::Null => (1, 0),
        PointValue::Value(ScalarValue::Signed(value)) => (2, value as u64),
        PointValue::Value(ScalarValue::Unsigned(value)) => (3, value),
        PointValue::Value(ScalarValue::Number(bits)) => (4, bits),
        PointValue::Value(_) => {
            return Err(IndexError::InvalidFormat(
                "non-numeric point scratch record",
            ));
        }
    };
    output.push(tag);
    output.extend_from_slice(&bits.to_le_bytes());
    output.extend_from_slice(&entry.doc_id.get().to_le_bytes());
    Ok(())
}

fn decode_entry(bytes: &[u8]) -> Result<PointEntry, IndexError> {
    if bytes.len() != RECORD_BYTES {
        return Err(IndexError::InvalidFormat("point scratch record width"));
    }
    let bits = u64::from_le_bytes(
        bytes[1..9]
            .try_into()
            .map_err(|_| IndexError::InvalidFormat("point scratch value"))?,
    );
    let value = match bytes[0] {
        0 if bits == 0 => PointValue::Presence,
        1 if bits == 0 => PointValue::Null,
        2 => PointValue::Value(ScalarValue::Signed(bits as i64)),
        3 => PointValue::Value(ScalarValue::Unsigned(bits)),
        4 => PointValue::Value(
            ScalarValue::number(f64::from_bits(bits))
                .map_err(|_| IndexError::InvalidFormat("point scratch number"))?,
        ),
        _ => return Err(IndexError::InvalidFormat("point scratch kind")),
    };
    Ok(PointEntry {
        value,
        doc_id: DocId::new(u32::from_le_bytes(
            bytes[9..13]
                .try_into()
                .map_err(|_| IndexError::InvalidFormat("point scratch DocId"))?,
        )),
    })
}

struct PointCursor<F> {
    file: F,
    length: u64,
    offset: u64,
    current: Option<PointEntry>,
}

impl<F: MergeScratchFile> PointCursor<F> {
    async fn new(file: F) -> Result<Self, IndexError> {
        let length = file.len().await?;
        if length == 0 || length % RECORD_BYTES as u64 != 0 {
            return Err(IndexError::InvalidFormat("point scratch length"));
        }
        Ok(Self {
            file,
            length,
            offset: 0,
            current: None,
        })
    }

    async fn next(&mut self) -> Result<Option<PointEntry>, IndexError> {
        self.advance().await?;
        Ok(self.current.take())
    }

    async fn advance(&mut self) -> Result<(), IndexError> {
        if self.offset == self.length {
            self.current = None;
            return Ok(());
        }
        let bytes = self.file.read_exact_at(self.offset, RECORD_BYTES).await?;
        self.offset = self
            .offset
            .checked_add(RECORD_BYTES as u64)
            .ok_or(IndexError::OffsetOverflow)?;
        self.current = Some(decode_entry(&bytes)?);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_point_records_round_trip_in_value_order() {
        let entries = vec![
            PointEntry {
                value: PointValue::Value(ScalarValue::Signed(-9)),
                doc_id: DocId::new(7),
            },
            PointEntry {
                value: PointValue::Presence,
                doc_id: DocId::new(2),
            },
        ];
        let encoded = encode_sorted(entries).unwrap();
        assert_eq!(
            decode_entry(&encoded[..RECORD_BYTES]).unwrap().value,
            PointValue::Presence
        );
        assert_eq!(
            decode_entry(&encoded[RECORD_BYTES..]).unwrap().value,
            PointValue::Value(ScalarValue::Signed(-9))
        );
    }
}
