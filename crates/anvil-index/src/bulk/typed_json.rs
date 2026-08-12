use crate::bulk::{
    BULK_OUTPUT_LEVEL, BULK_RANGE_ROOT_HEIGHT, BULK_TARGET_BLOCK_BYTES, BulkBuildOptions,
    BulkStatistics, range_local_ordinal, range_ordinal_base,
};
use crate::compaction::{
    CompactionExecutor, CompactionProgress, KeyRange, LaneResultProducer, collect_ordered_lanes,
    deterministic_suffix_key_range_plan,
};
use crate::routed::RoutedCursor;
use crate::routed_sort::RoutedExternalSorter;
use crate::run::{
    ComponentRangeAssembler, ComponentTree, assemble_component_ranges, discard_component_tree,
    seal_run_root,
};
use crate::segment::{
    DOCUMENTS_TAG, DocumentComponentWriter, DocumentRecord, DocumentState, PATH_CHANGES_TAG,
    PathChange, PathComponentWriter,
};
use crate::typed_json::{
    KEYS_TAG, MetadataDocument, PostingComponentWriter, ROWS_TAG, TypedComponentWriter,
    TypedJsonDefinition, TypedJsonDocument, TypedPayload, TypedRow, preflight_typed_row,
    validate_selected_fields,
};
use crate::{IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind, IndexMutation, SealedRun};

struct TypedRange {
    ordinal_base: u64,
    live: u64,
    paths: PathComponentWriter,
    documents: DocumentComponentWriter,
    typed: TypedComponentWriter,
}

struct TypedBulkCore<S, E> {
    kind: IndexKind,
    output: S,
    paths: ComponentRangeAssembler,
    documents: ComponentRangeAssembler,
    typed: ComponentRangeAssembler,
    keys: Option<RoutedExternalSorter<S, E>>,
    executor: E,
    progress: CompactionProgress,
    max_rewrite_lanes: usize,
    range: Option<TypedRange>,
    next_range_id: u64,
    statistics: BulkStatistics,
    last_path: Option<String>,
}

impl<S, E> TypedBulkCore<S, E>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    fn new(
        kind: IndexKind,
        output: S,
        executor: E,
        options: BulkBuildOptions,
    ) -> Result<Self, IndexError> {
        let scratch = output.fork_scratch()?;
        let progress = CompactionProgress::default();
        let keys = RoutedExternalSorter::new(
            kind,
            KEYS_TAG,
            BULK_OUTPUT_LEVEL,
            BULK_TARGET_BLOCK_BYTES,
            options.max_sort_chunk_bytes,
            scratch,
            executor.clone(),
            progress.clone(),
        )?;
        Ok(Self {
            kind,
            output,
            paths: ComponentRangeAssembler::new(kind, PATH_CHANGES_TAG, BULK_RANGE_ROOT_HEIGHT),
            documents: ComponentRangeAssembler::new(kind, DOCUMENTS_TAG, BULK_RANGE_ROOT_HEIGHT),
            typed: ComponentRangeAssembler::new(kind, ROWS_TAG, BULK_RANGE_ROOT_HEIGHT),
            keys: Some(keys),
            executor,
            progress,
            max_rewrite_lanes: options.max_rewrite_lanes,
            range: None,
            next_range_id: 0,
            statistics: BulkStatistics::default(),
            last_path: None,
        })
    }

    async fn push(
        &mut self,
        mutation: IndexMutation<(crate::DocumentRef, TypedPayload)>,
    ) -> Result<(), IndexError> {
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
        if self.range.is_none() {
            let ordinal_base = range_ordinal_base(self.next_range_id)?;
            self.range = Some(TypedRange {
                ordinal_base,
                live: 0,
                paths: PathComponentWriter::new(
                    self.kind,
                    BULK_OUTPUT_LEVEL,
                    BULK_TARGET_BLOCK_BYTES,
                ),
                documents: DocumentComponentWriter::with_ordinal_base(
                    self.kind,
                    BULK_OUTPUT_LEVEL,
                    BULK_TARGET_BLOCK_BYTES,
                    ordinal_base,
                ),
                typed: TypedComponentWriter::new(
                    self.kind,
                    BULK_OUTPUT_LEVEL,
                    BULK_TARGET_BLOCK_BYTES,
                ),
            });
        }
        let range = self.range.as_mut().expect("typed bulk range was opened");
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
                    .typed
                    .push(TypedRow { ordinal, payload }, &mut self.output)
                    .await?;
                for row in key_rows {
                    self.keys
                        .as_mut()
                        .expect("typed key sorter is present")
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

    async fn finish_range(&mut self) -> Result<(), IndexError> {
        let Some(range) = self.range.take() else {
            return Ok(());
        };
        let paths = range.paths.finish(&mut self.output).await?;
        self.paths.push(paths, &mut self.output).await?;
        if range.live > 0 {
            let documents = range.documents.finish(&mut self.output).await?;
            self.documents.push(documents, &mut self.output).await?;
            let typed = range.typed.finish(&mut self.output).await?;
            self.typed.push(typed, &mut self.output).await?;
        }
        self.keys
            .as_mut()
            .expect("typed key sorter is present")
            .checkpoint()
            .await?;
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
        let path_tree = self
            .paths
            .finish(&mut self.output)
            .await?
            .ok_or(IndexError::InvalidFormat("missing bulk typed paths"))?;
        let statistics = self.statistics.finish()?;
        let mut components = vec![path_tree];
        if statistics.live_document_count > 0 {
            components.push(
                self.documents
                    .finish(&mut self.output)
                    .await?
                    .ok_or(IndexError::InvalidFormat("missing bulk typed documents"))?,
            );
            components.push(
                self.typed
                    .finish(&mut self.output)
                    .await?
                    .ok_or(IndexError::InvalidFormat("missing bulk typed rows"))?,
            );
            if let Some(scratch_tree) = self
                .keys
                .take()
                .expect("typed key sorter is present")
                .finish()
                .await?
            {
                components.push(
                    rewrite_postings_parallel(
                        self.kind,
                        scratch_tree,
                        self.output.clone(),
                        &mut self.output,
                        self.max_rewrite_lanes,
                        self.executor,
                        self.progress,
                    )
                    .await?,
                );
            }
        }
        let run = seal_run_root(self.kind, BULK_OUTPUT_LEVEL, statistics, components)?;
        Ok((Some(run), self.output))
    }
}

async fn rewrite_postings_parallel<D, S, E>(
    kind: IndexKind,
    tree: ComponentTree,
    directory: D,
    sink: &mut S,
    max_lanes: usize,
    executor: E,
    progress: CompactionProgress,
) -> Result<ComponentTree, IndexError>
where
    D: IndexDirectoryRead + Clone + Send + Sync + 'static,
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    let plan = deterministic_suffix_key_range_plan(
        [tree.root.clone()],
        std::mem::size_of::<u64>() + std::mem::size_of::<u32>(),
        max_lanes,
    )?;
    progress.record_range_limit(plan.range_limit)?;
    let mut producers = Vec::<LaneResultProducer<Option<ComponentTree>>>::new();
    for range in plan.ranges {
        let root = tree.root.clone();
        let directory = directory.clone();
        let lane_sink = sink.fork()?;
        let lane_executor = executor.clone();
        let lane_progress = progress.clone();
        producers.push(Box::new(move || {
            Box::pin(rewrite_posting_range(
                kind,
                root,
                range,
                directory,
                lane_sink,
                lane_executor,
                lane_progress,
            ))
        }));
    }
    let trees = collect_ordered_lanes(&executor, producers, &progress)
        .await?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let output = assemble_component_ranges(kind, KEYS_TAG, &trees, sink).await?;
    discard_component_tree(&directory, &tree, sink).await?;
    Ok(output)
}

async fn rewrite_posting_range<D, S, E>(
    kind: IndexKind,
    root: crate::BlockDescriptor,
    range: KeyRange,
    directory: D,
    mut sink: S,
    executor: E,
    progress: CompactionProgress,
) -> Result<Option<ComponentTree>, IndexError>
where
    D: IndexDirectoryRead,
    S: IndexBlockSink,
    E: CompactionExecutor,
{
    let mut cursor = RoutedCursor::in_range(&directory, root, range);
    let mut writer = PostingComponentWriter::new(kind, BULK_TARGET_BLOCK_BYTES);
    let mut wrote = false;
    while let Some(row) = cursor.next_parallel(&executor, &progress).await? {
        writer.push(row, &mut sink).await?;
        wrote = true;
    }
    if wrote {
        writer.finish(&mut sink).await
    } else {
        Ok(None)
    }
}

pub struct TypedJsonBulkBuilder<S, E> {
    definition: TypedJsonDefinition,
    core: TypedBulkCore<S, E>,
}

impl<S, E> TypedJsonBulkBuilder<S, E>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    pub fn new(
        definition: TypedJsonDefinition,
        sink: S,
        executor: E,
        options: BulkBuildOptions,
    ) -> Result<Self, IndexError> {
        definition.validate()?;
        Ok(Self {
            definition,
            core: TypedBulkCore::new(IndexKind::TypedJson, sink, executor, options)?,
        })
    }

    pub async fn push(
        &mut self,
        mutation: IndexMutation<TypedJsonDocument>,
    ) -> Result<(), IndexError> {
        let mutation = match mutation {
            IndexMutation::Upsert(document) => {
                validate_selected_fields(&self.definition, &document.fields)?;
                preflight_typed_row(&document.fields)?;
                IndexMutation::Upsert((document.document, TypedPayload::canonical(document.fields)))
            }
            IndexMutation::Remove(document) => IndexMutation::Remove(document),
        };
        self.core.push(mutation).await
    }

    pub async fn finish_range(&mut self) -> Result<(), IndexError> {
        self.core.finish_range().await
    }

    pub fn progress(&self) -> CompactionProgress {
        self.core.progress.clone()
    }

    pub async fn finish(self) -> Result<(Option<SealedRun>, S), IndexError> {
        self.core.finish().await
    }
}

pub struct MetadataBulkBuilder<S, E> {
    definition: TypedJsonDefinition,
    core: TypedBulkCore<S, E>,
}

impl<S, E> MetadataBulkBuilder<S, E>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    pub fn new(
        definition: TypedJsonDefinition,
        sink: S,
        executor: E,
        options: BulkBuildOptions,
    ) -> Result<Self, IndexError> {
        definition.validate()?;
        Ok(Self {
            definition,
            core: TypedBulkCore::new(IndexKind::MetadataFilter, sink, executor, options)?,
        })
    }

    pub async fn push(
        &mut self,
        mutation: IndexMutation<MetadataDocument>,
    ) -> Result<(), IndexError> {
        let mutation = match mutation {
            IndexMutation::Upsert(document) => {
                validate_selected_fields(&self.definition, &document.fields)?;
                preflight_typed_row(&document.fields)?;
                IndexMutation::Upsert((document.document, TypedPayload::canonical(document.fields)))
            }
            IndexMutation::Remove(document) => IndexMutation::Remove(document),
        };
        self.core.push(mutation).await
    }

    pub async fn finish_range(&mut self) -> Result<(), IndexError> {
        self.core.finish_range().await
    }

    pub fn progress(&self) -> CompactionProgress {
        self.core.progress.clone()
    }

    pub async fn finish(self) -> Result<(Option<SealedRun>, S), IndexError> {
        self.core.finish().await
    }
}
