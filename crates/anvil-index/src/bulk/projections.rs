use crate::bulk::{
    BULK_OUTPUT_LEVEL, BULK_RANGE_ROOT_HEIGHT, BULK_TARGET_BLOCK_BYTES, BulkBuildOptions,
    BulkStatistics, range_local_ordinal, range_ordinal_base,
};
use crate::compaction::{CompactionExecutor, CompactionProgress};
use crate::projections::{
    GitPayload, GitSourceDocument, OrdinalComponentWriter, OrdinalRow, ProjectionPayload,
    RECORDS_TAG, TensorDocument, TensorPayload, git_encoded_bytes, preflight_projection_row,
    tensor_encoded_bytes, validate_git_records, validate_tensor_records,
};
use crate::routed_sort::{RoutedExternalSorter, rewrite_routed_component_tree_parallel};
use crate::run::{ComponentRangeAssembler, seal_run_root};
use crate::segment::{
    DOCUMENTS_TAG, DocumentComponentWriter, DocumentRecord, DocumentState, PATH_CHANGES_TAG,
    PathChange, PathComponentWriter,
};
use crate::{
    DocumentRef, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind, IndexMutation,
    SealedRun,
};

struct ProjectionRange<P> {
    ordinal_base: u64,
    live: u64,
    paths: PathComponentWriter,
    documents: DocumentComponentWriter,
    records: OrdinalComponentWriter<P>,
}

struct ProjectionBulkCore<S, E, P> {
    kind: IndexKind,
    output: S,
    paths: ComponentRangeAssembler,
    documents: ComponentRangeAssembler,
    records: ComponentRangeAssembler,
    keys: Vec<(u8, RoutedExternalSorter<S, E>)>,
    executor: E,
    progress: CompactionProgress,
    max_rewrite_lanes: usize,
    range: Option<ProjectionRange<P>>,
    next_range_id: u64,
    statistics: BulkStatistics,
    last_path: Option<String>,
}

impl<S, E, P> ProjectionBulkCore<S, E, P>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
    P: ProjectionPayload,
{
    fn new(
        kind: IndexKind,
        output: S,
        executor: E,
        options: BulkBuildOptions,
    ) -> Result<Self, IndexError> {
        let mut keys = Vec::with_capacity(P::key_tags().len());
        let sorter_count = P::key_tags().len().max(1);
        let sorter_chunk_bytes = options.max_sort_chunk_bytes / sorter_count;
        if sorter_chunk_bytes == 0 {
            return Err(IndexError::InvalidDefinition(
                "projection bulk sort budget cannot fund every key family".into(),
            ));
        }
        let progress = CompactionProgress::default();
        for &tag in P::key_tags() {
            keys.push((
                tag,
                RoutedExternalSorter::new(
                    kind,
                    tag,
                    BULK_OUTPUT_LEVEL,
                    BULK_TARGET_BLOCK_BYTES,
                    sorter_chunk_bytes,
                    output.fork_scratch()?,
                    executor.clone(),
                    progress.clone(),
                )?,
            ));
        }
        Ok(Self {
            kind,
            output,
            paths: ComponentRangeAssembler::new(kind, PATH_CHANGES_TAG, BULK_RANGE_ROOT_HEIGHT),
            documents: ComponentRangeAssembler::new(kind, DOCUMENTS_TAG, BULK_RANGE_ROOT_HEIGHT),
            records: ComponentRangeAssembler::new(kind, RECORDS_TAG, BULK_RANGE_ROOT_HEIGHT),
            keys,
            executor,
            progress,
            max_rewrite_lanes: options.max_rewrite_lanes,
            range: None,
            next_range_id: 0,
            statistics: BulkStatistics::default(),
            last_path: None,
        })
    }

    async fn push(&mut self, mutation: IndexMutation<(DocumentRef, P)>) -> Result<(), IndexError> {
        let document = match &mutation {
            IndexMutation::Upsert((document, _)) | IndexMutation::Remove(document) => document,
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
        self.ensure_range()?;
        let range = self
            .range
            .as_mut()
            .expect("projection bulk range was opened");
        match mutation {
            IndexMutation::Upsert((document, payload)) => {
                let ordinal = range_local_ordinal(range.ordinal_base, range.live)?;
                let key_rows = payload.key_rows(ordinal)?;
                range.live = range
                    .live
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                self.statistics.record(document.version, true)?;
                range
                    .paths
                    .push(
                        PathChange {
                            document: document.clone(),
                            state: DocumentState::Live,
                            document_ordinal: Some(ordinal),
                        },
                        &mut self.output,
                    )
                    .await?;
                range
                    .documents
                    .push(DocumentRecord { ordinal, document }, &mut self.output)
                    .await?;
                range
                    .records
                    .push(OrdinalRow { ordinal, payload }, &mut self.output)
                    .await?;
                for (tag, row) in key_rows {
                    self.keys
                        .iter_mut()
                        .find(|(candidate, _)| *candidate == tag)
                        .ok_or(IndexError::InvalidFormat("unknown projection key tag"))?
                        .1
                        .push(row)
                        .await?;
                }
            }
            IndexMutation::Remove(document) => {
                self.statistics.record(document.version, false)?;
                range
                    .paths
                    .push(
                        PathChange {
                            document,
                            state: DocumentState::Removed,
                            document_ordinal: None,
                        },
                        &mut self.output,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    fn ensure_range(&mut self) -> Result<(), IndexError> {
        if self.range.is_some() {
            return Ok(());
        }
        let ordinal_base = range_ordinal_base(self.next_range_id)?;
        self.range = Some(ProjectionRange {
            ordinal_base,
            live: 0,
            paths: PathComponentWriter::new(self.kind, BULK_OUTPUT_LEVEL, BULK_TARGET_BLOCK_BYTES),
            documents: DocumentComponentWriter::with_ordinal_base(
                self.kind,
                BULK_OUTPUT_LEVEL,
                BULK_TARGET_BLOCK_BYTES,
                ordinal_base,
            ),
            records: OrdinalComponentWriter::new(
                self.kind,
                BULK_OUTPUT_LEVEL,
                BULK_TARGET_BLOCK_BYTES,
            ),
        });
        Ok(())
    }

    async fn finish_range(&mut self) -> Result<(), IndexError> {
        let Some(range) = self.range.take() else {
            return Ok(());
        };
        self.paths
            .push(
                range.paths.finish(&mut self.output).await?,
                &mut self.output,
            )
            .await?;
        if range.live > 0 {
            self.documents
                .push(
                    range.documents.finish(&mut self.output).await?,
                    &mut self.output,
                )
                .await?;
            self.records
                .push(
                    range.records.finish(&mut self.output).await?,
                    &mut self.output,
                )
                .await?;
        }
        for (_, sorter) in &mut self.keys {
            sorter.checkpoint().await?;
        }
        self.next_range_id = self
            .next_range_id
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(())
    }

    async fn finish(mut self) -> Result<(Option<SealedRun>, S), IndexError> {
        self.finish_range().await?;
        if self.statistics.is_empty() {
            return Ok((None, self.output));
        }
        let statistics = self.statistics.finish()?;
        let mut components = vec![
            self.paths
                .finish(&mut self.output)
                .await?
                .ok_or(IndexError::InvalidFormat("missing bulk projection paths"))?,
        ];
        if statistics.live_document_count > 0 {
            components.push(self.documents.finish(&mut self.output).await?.ok_or(
                IndexError::InvalidFormat("missing bulk projection documents"),
            )?);
            components.push(
                self.records
                    .finish(&mut self.output)
                    .await?
                    .ok_or(IndexError::InvalidFormat("missing bulk projection records"))?,
            );
            for (tag, sorter) in self.keys {
                let Some(scratch_tree) = sorter.finish().await? else {
                    continue;
                };
                let directory = self.output.clone();
                components.push(
                    rewrite_routed_component_tree_parallel(
                        self.kind,
                        tag,
                        BULK_OUTPUT_LEVEL,
                        BULK_TARGET_BLOCK_BYTES,
                        scratch_tree,
                        directory,
                        &mut self.output,
                        self.max_rewrite_lanes,
                        self.executor.clone(),
                        self.progress.clone(),
                    )
                    .await?,
                );
            }
        }
        Ok((
            Some(seal_run_root(
                self.kind,
                BULK_OUTPUT_LEVEL,
                statistics,
                components,
            )?),
            self.output,
        ))
    }
}

pub struct GitSourceBulkBuilder<S, E>(ProjectionBulkCore<S, E, GitPayload>);

impl<S, E> GitSourceBulkBuilder<S, E>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    pub fn new(output: S, executor: E, options: BulkBuildOptions) -> Result<Self, IndexError> {
        Ok(Self(ProjectionBulkCore::new(
            IndexKind::GitSource,
            output,
            executor,
            options,
        )?))
    }

    pub async fn push(
        &mut self,
        mutation: IndexMutation<GitSourceDocument>,
    ) -> Result<(), IndexError> {
        self.0
            .push(match mutation {
                IndexMutation::Upsert(document) => {
                    validate_git_records(&document.records)?;
                    preflight_projection_row(git_encoded_bytes(&document.records))?;
                    IndexMutation::Upsert((document.document, GitPayload(document.records)))
                }
                IndexMutation::Remove(document) => IndexMutation::Remove(document),
            })
            .await
    }

    pub async fn finish_range(&mut self) -> Result<(), IndexError> {
        self.0.finish_range().await
    }

    pub fn progress(&self) -> CompactionProgress {
        self.0.progress.clone()
    }

    pub async fn finish(self) -> Result<(Option<SealedRun>, S), IndexError> {
        self.0.finish().await
    }
}

pub struct TensorBulkBuilder<S, E>(ProjectionBulkCore<S, E, TensorPayload>);

impl<S, E> TensorBulkBuilder<S, E>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    pub fn new(output: S, executor: E, options: BulkBuildOptions) -> Result<Self, IndexError> {
        Ok(Self(ProjectionBulkCore::new(
            IndexKind::Tensor,
            output,
            executor,
            options,
        )?))
    }

    pub async fn push(
        &mut self,
        mutation: IndexMutation<TensorDocument>,
    ) -> Result<(), IndexError> {
        self.0
            .push(match mutation {
                IndexMutation::Upsert(document) => {
                    validate_tensor_records(&document.records)?;
                    preflight_projection_row(tensor_encoded_bytes(&document.records))?;
                    IndexMutation::Upsert((document.document, TensorPayload(document.records)))
                }
                IndexMutation::Remove(document) => IndexMutation::Remove(document),
            })
            .await
    }

    pub async fn finish_range(&mut self) -> Result<(), IndexError> {
        self.0.finish_range().await
    }

    pub fn progress(&self) -> CompactionProgress {
        self.0.progress.clone()
    }

    pub async fn finish(self) -> Result<(Option<SealedRun>, S), IndexError> {
        self.0.finish().await
    }
}
