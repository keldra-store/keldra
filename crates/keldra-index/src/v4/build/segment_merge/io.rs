use std::future::Future;

use crate::IndexError;
use crate::compaction::CompactionExecutor;

use super::super::super::{
    ArtifactDirectoryRead, ArtifactPackReference, ComponentKind, ComponentStream, FieldId,
    SegmentComponent, SegmentDescriptor, SegmentIdentity, StreamLeaf, read_artifact_component,
};
use super::super::scratch::MergeScratchFile;

/// Routes finite component decode/encode chunks through the one executor
/// supplied by the Anvil process while leaving all artifact I/O asynchronous.
#[derive(Clone)]
pub(super) struct CompactionDirectory<D, E> {
    directory: D,
    executor: E,
}

impl<D, E> CompactionDirectory<D, E> {
    pub(super) fn new(directory: D, executor: E) -> Self {
        Self {
            directory,
            executor,
        }
    }
}

impl<D, E> ArtifactDirectoryRead for CompactionDirectory<D, E>
where
    D: ArtifactDirectoryRead,
    E: CompactionExecutor,
{
    type File = D::File;

    fn open_artifact(
        &self,
        pack: &ArtifactPackReference,
    ) -> impl Future<Output = Result<Self::File, IndexError>> + Send {
        self.directory.open_artifact(pack)
    }

    fn run_query_cpu<T, F>(&self, work: F) -> impl Future<Output = Result<T, IndexError>> + Send
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, IndexError> + Send + 'static,
    {
        self.executor.run_cpu(work)
    }
}

pub(super) struct RoutedBlockStream<'a, D> {
    directory: &'a D,
    identity: SegmentIdentity,
    packs: &'a [ArtifactPackReference],
    kind: ComponentKind,
    stream: ComponentStream<'a, D>,
}

impl<'a, D: ArtifactDirectoryRead> RoutedBlockStream<'a, D> {
    pub(super) async fn next<T: Send + 'static>(
        &mut self,
        decode: fn(&[u8]) -> Result<T, IndexError>,
    ) -> Result<Option<(StreamLeaf, T)>, IndexError> {
        let Some(leaf) = self.stream.next_leaf().await? else {
            return Ok(None);
        };
        let component = read_artifact_component(
            self.directory,
            self.identity,
            self.packs,
            &leaf.descriptor,
            self.kind,
        )
        .await?;
        let payload = component.payload;
        let decoded = self
            .directory
            .run_query_cpu(move || decode(&payload))
            .await?;
        Ok(Some((leaf, decoded)))
    }

    pub(super) async fn next_leaf(&mut self) -> Result<Option<StreamLeaf>, IndexError> {
        self.stream.next_leaf().await
    }
}

pub(super) fn required_stream<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    kind: ComponentKind,
    field_id: Option<FieldId>,
    range: Option<(Vec<u8>, Vec<u8>)>,
) -> Result<RoutedBlockStream<'a, D>, IndexError> {
    optional_stream(directory, segment, kind, field_id, range)?.ok_or(IndexError::InvalidFormat(
        "format-v4 merge input lacks a required component stream",
    ))
}

pub(super) fn optional_stream<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    kind: ComponentKind,
    field_id: Option<FieldId>,
    range: Option<(Vec<u8>, Vec<u8>)>,
) -> Result<Option<RoutedBlockStream<'a, D>>, IndexError> {
    let Some(component) = component(segment, kind, field_id) else {
        return Ok(None);
    };
    let (minimum, maximum) = range.map_or((None, None), |(minimum, maximum)| {
        (Some(minimum), Some(maximum))
    });
    Ok(Some(RoutedBlockStream {
        directory,
        identity: segment.identity,
        packs: &segment.packs,
        kind,
        stream: ComponentStream::new(
            directory,
            segment.identity,
            &segment.packs,
            kind,
            component.artifact.clone(),
            minimum,
            maximum,
        )?,
    }))
}

pub(super) fn component(
    segment: &SegmentDescriptor,
    kind: ComponentKind,
    field_id: Option<FieldId>,
) -> Option<&SegmentComponent> {
    segment
        .components
        .binary_search_by_key(&(kind, field_id, 0), |component| {
            (component.role, component.field_id, component.ordinal)
        })
        .ok()
        .map(|index| &segment.components[index])
}

pub(super) struct FixedScratchReader<'a, F> {
    file: &'a F,
    record_bytes: usize,
    records: u32,
    next_record: u32,
    buffer_first: u32,
    buffer: Vec<u8>,
}

impl<'a, F: MergeScratchFile> FixedScratchReader<'a, F> {
    pub(super) fn new_range(
        file: &'a F,
        record_bytes: usize,
        first_record: u32,
        end_record: u32,
    ) -> Self {
        Self {
            file,
            record_bytes,
            records: end_record,
            next_record: first_record,
            buffer_first: first_record,
            buffer: Vec::new(),
        }
    }

    pub(super) async fn next(&mut self) -> Result<Option<&[u8]>, IndexError> {
        if self.next_record == self.records {
            return Ok(None);
        }
        let buffered_records = self.buffer.len() / self.record_bytes;
        if self.next_record < self.buffer_first
            || self.next_record >= self.buffer_first.saturating_add(buffered_records as u32)
        {
            self.buffer_first = self.next_record;
            let remaining = (self.records - self.next_record) as usize;
            let count = remaining.min((64 * 1024 / self.record_bytes).max(1));
            let offset = u64::from(self.next_record)
                .checked_mul(self.record_bytes as u64)
                .ok_or(IndexError::OffsetOverflow)?;
            self.buffer = self
                .file
                .read_exact_at(offset, count.saturating_mul(self.record_bytes))
                .await?;
        }
        let local = (self.next_record - self.buffer_first) as usize;
        self.next_record = self
            .next_record
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        let start = local.saturating_mul(self.record_bytes);
        Ok(Some(
            self.buffer
                .get(start..start + self.record_bytes)
                .ok_or(IndexError::InvalidFormat("scratch fixed record range"))?,
        ))
    }
}

pub(super) struct RemapReader<F> {
    file: F,
    document_count: u32,
    page_first: u32,
    page: Vec<u8>,
}

impl<F: MergeScratchFile> RemapReader<F> {
    pub(super) fn new(file: F, document_count: u32) -> Self {
        Self {
            file,
            document_count,
            page_first: u32::MAX,
            page: Vec::new(),
        }
    }

    pub(super) async fn get(&mut self, old_doc_id: u32) -> Result<Option<u32>, IndexError> {
        if old_doc_id >= self.document_count {
            return Err(IndexError::InvalidFormat(
                "posting DocId exceeds its remap domain",
            ));
        }
        const PAGE_RECORDS: u32 = 16 * 1024;
        let page_first = old_doc_id / PAGE_RECORDS * PAGE_RECORDS;
        if self.page_first != page_first {
            let count = PAGE_RECORDS.min(self.document_count - page_first);
            self.page = self
                .file
                .read_exact_at(u64::from(page_first) * 4, count as usize * 4)
                .await?;
            self.page_first = page_first;
        }
        let offset = (old_doc_id - self.page_first) as usize * 4;
        let encoded = u32::from_le_bytes(
            self.page
                .get(offset..offset + 4)
                .ok_or(IndexError::InvalidFormat("scratch remap range"))?
                .try_into()
                .map_err(|_| IndexError::InvalidFormat("scratch remap width"))?,
        );
        Ok(encoded.checked_sub(1))
    }
}
