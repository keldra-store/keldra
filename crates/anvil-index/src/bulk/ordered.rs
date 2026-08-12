use crate::bulk::{
    BULK_OUTPUT_LEVEL, BULK_RANGE_ROOT_HEIGHT, BULK_TARGET_BLOCK_BYTES, BulkStatistics,
    range_local_ordinal, range_ordinal_base,
};
use crate::ordered::PathDocument;
use crate::run::{ComponentRangeAssembler, seal_run_root};
use crate::segment::{DocumentState, PATH_CHANGES_TAG, PathChange, PathComponentWriter};
use crate::{IndexBlockSink, IndexError, IndexKind, IndexMutation, SealedRun};

struct PathRange {
    ordinal_base: u64,
    live: u64,
    writer: PathComponentWriter,
}

/// Streams one path base run without retaining the corpus or emitting L0 runs.
pub struct PathBulkBuilder<S> {
    sink: S,
    paths: ComponentRangeAssembler,
    range: Option<PathRange>,
    next_range_id: u64,
    statistics: BulkStatistics,
    last_path: Option<String>,
}

impl<S: IndexBlockSink> PathBulkBuilder<S> {
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            paths: ComponentRangeAssembler::new(
                IndexKind::Path,
                PATH_CHANGES_TAG,
                BULK_RANGE_ROOT_HEIGHT,
            ),
            range: None,
            next_range_id: 0,
            statistics: BulkStatistics::default(),
            last_path: None,
        }
    }

    pub async fn push(&mut self, mutation: IndexMutation<PathDocument>) -> Result<(), IndexError> {
        let document = match &mutation {
            IndexMutation::Upsert(value) => &value.document,
            IndexMutation::Remove(value) => value,
        };
        document.validate()?;
        if self
            .last_path
            .as_ref()
            .is_some_and(|previous| previous.as_str() >= document.path.as_str())
        {
            return Err(IndexError::UnsortedRecords);
        }
        self.last_path = Some(document.path.clone());
        if self.range.is_none() {
            self.range = Some(PathRange {
                ordinal_base: range_ordinal_base(self.next_range_id)?,
                live: 0,
                writer: PathComponentWriter::new(
                    IndexKind::Path,
                    BULK_OUTPUT_LEVEL,
                    BULK_TARGET_BLOCK_BYTES,
                ),
            });
        }
        let range = self.range.as_mut().expect("bulk path range was opened");
        let (change, live) = match mutation {
            IndexMutation::Upsert(value) => {
                let ordinal = range_local_ordinal(range.ordinal_base, range.live)?;
                range.live = range
                    .live
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                (
                    PathChange {
                        document: value.document,
                        state: DocumentState::Live,
                        document_ordinal: Some(ordinal),
                    },
                    true,
                )
            }
            IndexMutation::Remove(document) => (
                PathChange {
                    document,
                    state: DocumentState::Removed,
                    document_ordinal: None,
                },
                false,
            ),
        };
        self.statistics.record(change.document.version, live)?;
        range.writer.push(change, &mut self.sink).await
    }

    /// Seal one deterministic source-work range before yielding to another
    /// definition. The returned builder retains only bounded routing state.
    pub async fn finish_range(&mut self) -> Result<(), IndexError> {
        let Some(range) = self.range.take() else {
            return Ok(());
        };
        let tree = range.writer.finish(&mut self.sink).await?;
        self.paths.push(tree, &mut self.sink).await?;
        self.next_range_id = self
            .next_range_id
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<(Option<SealedRun>, S), IndexError> {
        self.finish_range().await?;
        if self.statistics.is_empty() {
            return Ok((None, self.sink));
        }
        let paths = self
            .paths
            .finish(&mut self.sink)
            .await?
            .ok_or(IndexError::InvalidFormat("missing bulk path component"))?;
        let run = seal_run_root(
            IndexKind::Path,
            BULK_OUTPUT_LEVEL,
            self.statistics.finish()?,
            [paths],
        )?;
        Ok((Some(run), self.sink))
    }
}
