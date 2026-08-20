use crate::IndexError;
use crate::compaction::CompactionExecutor;

use super::super::super::locator::PathLocatorBlockBuilder;
use super::super::super::{
    ComponentKind, DocId, DocIdRange, LocatorEntry, LocatorValue, Schema, SegmentIdentity,
};
use super::super::ComponentBatchSink;
use super::super::scratch::{MergeScratchFile, MergeScratchSpace};
use super::super::sink::{PublishedStream, StreamingComponentPublisher};

const SORT_FAN_IN: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RelocationRecord {
    pub(super) path: String,
    pub(super) object_version: u64,
    pub(super) input_ordinal: u32,
    pub(super) new_doc_id: u32,
}

impl RelocationRecord {
    fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.path.len())
    }

    fn encode(&self, output: &mut Vec<u8>) -> Result<(), IndexError> {
        let length = u32::try_from(self.path.len()).map_err(|_| IndexError::OffsetOverflow)?;
        output.extend_from_slice(&length.to_le_bytes());
        output.extend_from_slice(self.path.as_bytes());
        output.extend_from_slice(&self.object_version.to_le_bytes());
        output.extend_from_slice(&self.input_ordinal.to_le_bytes());
        output.extend_from_slice(&self.new_doc_id.to_le_bytes());
        Ok(())
    }
}

pub(super) struct RelocationSorter<'a, W: MergeScratchSpace, E: CompactionExecutor> {
    workspace: &'a W,
    executor: E,
    chunk_limit: usize,
    chunk_bytes: usize,
    chunk: Vec<RelocationRecord>,
    levels: Vec<Vec<W::File>>,
}

impl<'a, W: MergeScratchSpace, E: CompactionExecutor> RelocationSorter<'a, W, E> {
    pub(super) fn new(
        workspace: &'a W,
        chunk_limit: usize,
        executor: E,
    ) -> Result<Self, IndexError> {
        if chunk_limit < 16 * 1024 {
            return Err(IndexError::ResourceLimit {
                needed: 16 * 1024,
                limit: chunk_limit,
            });
        }
        Ok(Self {
            workspace,
            executor,
            chunk_limit,
            chunk_bytes: 0,
            chunk: Vec::new(),
            levels: Vec::new(),
        })
    }

    pub(super) async fn push(&mut self, record: RelocationRecord) -> Result<(), IndexError> {
        let bytes = record.resident_bytes();
        if bytes > self.chunk_limit {
            return Err(IndexError::ResourceLimit {
                needed: bytes,
                limit: self.chunk_limit,
            });
        }
        if !self.chunk.is_empty()
            && self
                .chunk_bytes
                .checked_add(bytes)
                .ok_or(IndexError::OffsetOverflow)?
                > self.chunk_limit
        {
            self.flush_chunk().await?;
        }
        self.chunk_bytes = self
            .chunk_bytes
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        self.chunk.push(record);
        Ok(())
    }

    pub(super) async fn finish(mut self) -> Result<W::File, IndexError> {
        self.flush_chunk().await?;
        loop {
            let total = self.levels.iter().map(Vec::len).sum::<usize>();
            if total == 0 {
                return Err(IndexError::InvalidDefinition(
                    "cannot merge segments with no live documents".into(),
                ));
            }
            if total == 1 {
                return Ok(self
                    .levels
                    .iter_mut()
                    .find_map(Vec::pop)
                    .expect("one relocation run"));
            }
            let mut files = Vec::with_capacity(SORT_FAN_IN);
            for runs in &mut self.levels {
                while files.len() < SORT_FAN_IN {
                    let Some(file) = runs.pop() else {
                        break;
                    };
                    files.push(file);
                }
                if files.len() == SORT_FAN_IN {
                    break;
                }
            }
            let merged = merge_runs(self.workspace, files).await?;
            self.insert_run(self.levels.len(), merged).await?;
        }
    }

    async fn flush_chunk(&mut self) -> Result<(), IndexError> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        let chunk_bytes = self.chunk_bytes;
        let chunk = std::mem::take(&mut self.chunk);
        let bytes = self
            .executor
            .run_cpu(move || {
                let mut chunk = chunk;
                chunk.sort_by(compare_record);
                let mut bytes = Vec::with_capacity(chunk_bytes);
                for record in chunk {
                    record.encode(&mut bytes)?;
                }
                Ok(bytes)
            })
            .await?;
        self.chunk_bytes = 0;
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

fn compare_record(left: &RelocationRecord, right: &RelocationRecord) -> std::cmp::Ordering {
    left.path
        .cmp(&right.path)
        .then_with(|| left.input_ordinal.cmp(&right.input_ordinal))
        .then_with(|| left.new_doc_id.cmp(&right.new_doc_id))
}

async fn merge_runs<W: MergeScratchSpace>(
    workspace: &W,
    files: Vec<W::File>,
) -> Result<W::File, IndexError> {
    if files.is_empty() || files.len() > SORT_FAN_IN {
        return Err(IndexError::InvalidFormat("relocation scratch fan-in"));
    }
    let mut cursors = Vec::with_capacity(files.len());
    for file in files {
        let mut cursor = RelocationCursor::new(file).await?;
        cursor.advance().await?;
        cursors.push(cursor);
    }
    let output = workspace.create_file().await?;
    let mut buffer = Vec::with_capacity(64 * 1024);
    while let Some(selected) = cursors
        .iter()
        .enumerate()
        .filter_map(|(index, cursor)| cursor.current.as_ref().map(|value| (index, value)))
        .min_by(|(_, left), (_, right)| compare_record(left, right))
        .map(|(index, _)| index)
    {
        cursors[selected]
            .current
            .as_ref()
            .expect("selected relocation")
            .encode(&mut buffer)?;
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

pub(super) async fn publish_locator<S: ComponentBatchSink, F: MergeScratchFile>(
    sink: &mut S,
    schema: &Schema,
    identity: SegmentIdentity,
    file: F,
) -> Result<(PublishedStream, u64), IndexError> {
    let mut cursor = RelocationCursor::new(file).await?;
    cursor.advance().await?;
    let mut publisher = StreamingComponentPublisher::new(
        sink,
        identity,
        ComponentKind::PATH_LOCATOR,
        schema.codec_version(ComponentKind::PATH_LOCATOR)?,
        schema.codec_version(ComponentKind::ROUTING_NODE)?,
    )?;
    let mut block = PathLocatorBlockBuilder::default();
    let mut sources = 0u64;
    while let Some(first) = cursor.current.clone() {
        let mut ranges = Vec::<DocIdRange>::new();
        let path = first.path.clone();
        let object_version = first.object_version;
        let input_ordinal = first.input_ordinal;
        while let Some(record) = cursor.current.as_ref().filter(|value| value.path == path) {
            if record.object_version != object_version || record.input_ordinal != input_ordinal {
                return Err(IndexError::InvalidFormat(
                    "one live source path spans merge segments or versions",
                ));
            }
            match ranges.last_mut() {
                Some(range)
                    if range.first_doc_id.get().checked_add(range.count)
                        == Some(record.new_doc_id) =>
                {
                    range.count = range
                        .count
                        .checked_add(1)
                        .ok_or(IndexError::OffsetOverflow)?;
                }
                _ => ranges.push(DocIdRange {
                    segment_id: identity.segment_id,
                    first_doc_id: DocId::new(record.new_doc_id),
                    count: 1,
                }),
            }
            cursor.advance().await?;
        }
        sources = sources.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        let entry = LocatorEntry {
            path,
            value: LocatorValue::Live {
                object_version,
                ranges,
            },
        };
        if let Some(retry) = block.push(entry)? {
            publish_locator_block(&mut publisher, block.finish()?.unwrap()).await?;
            debug_assert!(block.push(retry)?.is_none());
        }
    }
    if let Some(block) = block.finish()? {
        publish_locator_block(&mut publisher, block).await?;
    }
    Ok((publisher.finish().await?, sources))
}

async fn publish_locator_block<S: ComponentBatchSink>(
    publisher: &mut StreamingComponentPublisher<'_, S>,
    block: super::super::super::PathLocatorBlock,
) -> Result<(), IndexError> {
    let first = block.entries().first().expect("nonempty locator");
    let last = block.entries().last().expect("nonempty locator");
    publisher
        .push_payload(
            first.path.as_bytes().to_vec(),
            last.path.as_bytes().to_vec(),
            block.entries().len() as u64,
            block.encode_payload()?,
        )
        .await
}

struct RelocationCursor<F> {
    file: F,
    offset: u64,
    length: u64,
    current: Option<RelocationRecord>,
}

impl<F: MergeScratchFile> RelocationCursor<F> {
    async fn new(file: F) -> Result<Self, IndexError> {
        let length = file.len().await?;
        Ok(Self {
            file,
            offset: 0,
            length,
            current: None,
        })
    }

    async fn advance(&mut self) -> Result<(), IndexError> {
        if self.offset == self.length {
            self.current = None;
            return Ok(());
        }
        let prefix = self.file.read_exact_at(self.offset, 4).await?;
        let path_length = u32::from_le_bytes(
            prefix
                .as_slice()
                .try_into()
                .map_err(|_| IndexError::InvalidFormat("relocation length"))?,
        ) as usize;
        if path_length == 0 || path_length > super::super::super::INDEX_ROUTING_KEY_BYTES {
            return Err(IndexError::InvalidFormat("relocation path length"));
        }
        let body_length = path_length
            .checked_add(16)
            .ok_or(IndexError::OffsetOverflow)?;
        let body = self
            .file
            .read_exact_at(self.offset + 4, body_length)
            .await?;
        let path = String::from_utf8(
            body.get(..path_length)
                .ok_or(IndexError::InvalidFormat("relocation path"))?
                .to_vec(),
        )
        .map_err(|_| IndexError::InvalidFormat("relocation path UTF-8"))?;
        let mut offset = path_length;
        let object_version = take_u64(&body, &mut offset)?;
        let input_ordinal = take_u32(&body, &mut offset)?;
        let new_doc_id = take_u32(&body, &mut offset)?;
        if object_version == 0 || path.contains('\0') {
            return Err(IndexError::InvalidFormat("relocation identity"));
        }
        self.offset = self
            .offset
            .checked_add(4 + body_length as u64)
            .ok_or(IndexError::OffsetOverflow)?;
        if self.offset > self.length {
            return Err(IndexError::InvalidFormat("relocation scratch tail"));
        }
        self.current = Some(RelocationRecord {
            path,
            object_version,
            input_ordinal,
            new_doc_id,
        });
        Ok(())
    }
}

fn take_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, IndexError> {
    let end = offset.checked_add(4).ok_or(IndexError::OffsetOverflow)?;
    let value = u32::from_le_bytes(
        bytes
            .get(*offset..end)
            .ok_or(IndexError::InvalidFormat("relocation u32"))?
            .try_into()
            .map_err(|_| IndexError::InvalidFormat("relocation u32 width"))?,
    );
    *offset = end;
    Ok(value)
}

fn take_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, IndexError> {
    let end = offset.checked_add(8).ok_or(IndexError::OffsetOverflow)?;
    let value = u64::from_le_bytes(
        bytes
            .get(*offset..end)
            .ok_or(IndexError::InvalidFormat("relocation u64"))?
            .try_into()
            .map_err(|_| IndexError::InvalidFormat("relocation u64 width"))?,
    );
    *offset = end;
    Ok(value)
}
