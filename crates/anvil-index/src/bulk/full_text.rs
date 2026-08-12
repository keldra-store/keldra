use std::collections::BTreeMap;

use crate::bulk::{
    BULK_OUTPUT_LEVEL, BULK_RANGE_ROOT_HEIGHT, BULK_TARGET_BLOCK_BYTES, BulkBuildOptions,
    BulkStatistics, range_local_ordinal, range_ordinal_base,
};
use crate::compaction::{CompactionExecutor, CompactionProgress};
use crate::full_text::text_sort::{
    TextExternalSorter, TextSortOrder, rewrite_text_component_tree_parallel,
};
use crate::full_text::{
    FULL_TEXT_POSTINGS_TAG, FullTextDocument, TextPostingRow, tokenize_iter, validate_fields,
};
use crate::hybrid::{HYBRID_TEXT_TAG, HYBRID_VECTOR_TAG, HybridDefinition, HybridDocument};
use crate::run::{ComponentRangeAssembler, seal_run_root};
use crate::segment::{
    DOCUMENTS_TAG, DocumentComponentWriter, DocumentRecord, DocumentState, PATH_CHANGES_TAG,
    PathChange, PathComponentWriter,
};
use crate::vector::{VectorComponentWriter, VectorRow, validate_vector};
use crate::{
    DocumentRef, IndexBlockSink, IndexDirectoryRead, IndexError, IndexKind, IndexMutation,
    SealedRun,
};

struct TextRange {
    ordinal_base: u64,
    live: u64,
    paths: PathComponentWriter,
    documents: DocumentComponentWriter,
    vectors: Option<VectorComponentWriter>,
}

struct TextBulkCore<S, E> {
    kind: IndexKind,
    text_tag: u8,
    vector: Option<(u8, usize)>,
    output: S,
    paths: ComponentRangeAssembler,
    documents: ComponentRangeAssembler,
    vectors: Option<ComponentRangeAssembler>,
    text: Option<TextExternalSorter<S, E>>,
    executor: E,
    progress: CompactionProgress,
    max_rewrite_lanes: usize,
    range: Option<TextRange>,
    next_range_id: u64,
    statistics: BulkStatistics,
    last_path: Option<String>,
}

impl<S, E> TextBulkCore<S, E>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    fn new(
        kind: IndexKind,
        text_tag: u8,
        vector: Option<(u8, usize)>,
        output: S,
        executor: E,
        options: BulkBuildOptions,
    ) -> Result<Self, IndexError> {
        let scratch = output.fork_scratch()?;
        let progress = CompactionProgress::default();
        let text = TextExternalSorter::new(
            kind,
            text_tag,
            BULK_OUTPUT_LEVEL,
            BULK_TARGET_BLOCK_BYTES,
            options.max_sort_chunk_bytes,
            TextSortOrder::FinalPosting,
            scratch,
            executor.clone(),
            progress.clone(),
        )?;
        Ok(Self {
            kind,
            text_tag,
            vector,
            output,
            paths: ComponentRangeAssembler::new(kind, PATH_CHANGES_TAG, BULK_RANGE_ROOT_HEIGHT),
            documents: ComponentRangeAssembler::new(kind, DOCUMENTS_TAG, BULK_RANGE_ROOT_HEIGHT),
            vectors: vector
                .map(|(tag, _)| ComponentRangeAssembler::new(kind, tag, BULK_RANGE_ROOT_HEIGHT)),
            text: Some(text),
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
        mutation: IndexMutation<(DocumentRef, BTreeMap<String, String>, Option<Vec<f32>>)>,
    ) -> Result<(), IndexError> {
        let document = match &mutation {
            IndexMutation::Upsert((document, _, _)) | IndexMutation::Remove(document) => document,
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
        let range = self.range.as_mut().expect("text bulk range was opened");
        match mutation {
            IndexMutation::Upsert((document, fields, vector)) => {
                validate_fields(&fields)?;
                match (self.vector, &vector) {
                    (Some((_, dimension)), Some(values)) => validate_vector(values, dimension)?,
                    (None, None) => {}
                    _ => {
                        return Err(IndexError::InvalidDefinition(
                            "hybrid bulk source must contain exactly one vector".into(),
                        ));
                    }
                }
                let ordinal = range_local_ordinal(range.ordinal_base, range.live)?;
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
                if let (Some(writer), Some(values)) = (&mut range.vectors, vector) {
                    writer
                        .push(VectorRow { ordinal, values }, &mut self.output)
                        .await?;
                }
                self.push_text_rows(ordinal, fields).await?;
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
        self.range = Some(TextRange {
            ordinal_base,
            live: 0,
            paths: PathComponentWriter::new(self.kind, BULK_OUTPUT_LEVEL, BULK_TARGET_BLOCK_BYTES),
            documents: DocumentComponentWriter::with_ordinal_base(
                self.kind,
                BULK_OUTPUT_LEVEL,
                BULK_TARGET_BLOCK_BYTES,
                ordinal_base,
            ),
            vectors: self.vector.map(|(tag, dimension)| {
                VectorComponentWriter::new(
                    self.kind,
                    tag,
                    BULK_OUTPUT_LEVEL,
                    dimension,
                    BULK_TARGET_BLOCK_BYTES,
                )
            }),
        });
        Ok(())
    }

    async fn push_text_rows(
        &mut self,
        ordinal: u64,
        fields: BTreeMap<String, String>,
    ) -> Result<(), IndexError> {
        for (field, text) in fields {
            let field_length = u32::try_from(tokenize_iter(&text).count()).unwrap_or(u32::MAX);
            for (term, position) in tokenize_iter(&text) {
                self.text
                    .as_mut()
                    .expect("text sorter is present")
                    .push(TextPostingRow {
                        term,
                        ordinal,
                        field: field.clone(),
                        field_length,
                        part: position,
                        positions: vec![position],
                    })
                    .await?;
            }
        }
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
            if let (Some(assembler), Some(writer)) = (&mut self.vectors, range.vectors) {
                assembler
                    .push(writer.finish(&mut self.output).await?, &mut self.output)
                    .await?;
            }
        }
        self.text
            .as_mut()
            .expect("text sorter is present")
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
        let statistics = self.statistics.finish()?;
        let mut components = vec![
            self.paths
                .finish(&mut self.output)
                .await?
                .ok_or(IndexError::InvalidFormat("missing bulk text paths"))?,
        ];
        if statistics.live_document_count > 0 {
            components.push(
                self.documents
                    .finish(&mut self.output)
                    .await?
                    .ok_or(IndexError::InvalidFormat("missing bulk text documents"))?,
            );
            if let Some(assembler) = self.vectors {
                components.push(
                    assembler
                        .finish(&mut self.output)
                        .await?
                        .ok_or(IndexError::InvalidFormat("missing bulk hybrid vectors"))?,
                );
            }
            if let Some(scratch_tree) = self
                .text
                .take()
                .expect("text sorter is present")
                .finish()
                .await?
            {
                let directory = self.output.clone();
                components.push(
                    rewrite_text_component_tree_parallel(
                        self.kind,
                        self.text_tag,
                        BULK_OUTPUT_LEVEL,
                        BULK_TARGET_BLOCK_BYTES,
                        scratch_tree,
                        directory,
                        &mut self.output,
                        self.max_rewrite_lanes,
                        self.executor,
                        self.progress,
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

pub struct FullTextBulkBuilder<S, E>(TextBulkCore<S, E>);

impl<S, E> FullTextBulkBuilder<S, E>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    pub fn new(output: S, executor: E, options: BulkBuildOptions) -> Result<Self, IndexError> {
        Ok(Self(TextBulkCore::new(
            IndexKind::FullText,
            FULL_TEXT_POSTINGS_TAG,
            None,
            output,
            executor,
            options,
        )?))
    }

    pub async fn push(
        &mut self,
        mutation: IndexMutation<FullTextDocument>,
    ) -> Result<(), IndexError> {
        self.0
            .push(match mutation {
                IndexMutation::Upsert(document) => {
                    IndexMutation::Upsert((document.document, document.fields, None))
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

pub struct HybridBulkBuilder<S, E>(TextBulkCore<S, E>);

impl<S, E> HybridBulkBuilder<S, E>
where
    S: IndexBlockSink + IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    pub fn new(
        definition: HybridDefinition,
        output: S,
        executor: E,
        options: BulkBuildOptions,
    ) -> Result<Self, IndexError> {
        definition.validate()?;
        let dimension = definition.vector.dimension;
        Ok(Self(TextBulkCore::new(
            IndexKind::Hybrid,
            HYBRID_TEXT_TAG,
            Some((HYBRID_VECTOR_TAG, dimension)),
            output,
            executor,
            options,
        )?))
    }

    pub async fn push(
        &mut self,
        mutation: IndexMutation<HybridDocument>,
    ) -> Result<(), IndexError> {
        self.0
            .push(match mutation {
                IndexMutation::Upsert(document) => IndexMutation::Upsert((
                    document.document,
                    document.text_fields,
                    Some(document.vector),
                )),
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
