use crate::bulk::{
    BULK_OUTPUT_LEVEL, BULK_RANGE_ROOT_HEIGHT, BULK_TARGET_BLOCK_BYTES, BulkStatistics,
    range_local_ordinal, range_ordinal_base,
};
use crate::run::{ComponentRangeAssembler, seal_run_root};
use crate::segment::{
    DOCUMENTS_TAG, DocumentComponentWriter, DocumentRecord, DocumentState, PATH_CHANGES_TAG,
    PathChange, PathComponentWriter,
};
use crate::vector::{
    VECTORS_TAG, VectorComponentWriter, VectorDefinition, VectorDocument, VectorRow,
    validate_vector,
};
use crate::{IndexBlockSink, IndexError, IndexKind, IndexMutation, SealedRun};

struct VectorRange {
    ordinal_base: u64,
    live: u64,
    paths: PathComponentWriter,
    documents: DocumentComponentWriter,
    vectors: VectorComponentWriter,
}

pub struct VectorBulkBuilder<S> {
    definition: VectorDefinition,
    output: S,
    paths: ComponentRangeAssembler,
    documents: ComponentRangeAssembler,
    vectors: ComponentRangeAssembler,
    range: Option<VectorRange>,
    next_range_id: u64,
    statistics: BulkStatistics,
    last_path: Option<String>,
}

impl<S: IndexBlockSink> VectorBulkBuilder<S> {
    pub fn new(definition: VectorDefinition, output: S) -> Result<Self, IndexError> {
        definition.validate()?;
        Ok(Self {
            definition,
            output,
            paths: ComponentRangeAssembler::new(
                IndexKind::Vector,
                PATH_CHANGES_TAG,
                BULK_RANGE_ROOT_HEIGHT,
            ),
            documents: ComponentRangeAssembler::new(
                IndexKind::Vector,
                DOCUMENTS_TAG,
                BULK_RANGE_ROOT_HEIGHT,
            ),
            vectors: ComponentRangeAssembler::new(
                IndexKind::Vector,
                VECTORS_TAG,
                BULK_RANGE_ROOT_HEIGHT,
            ),
            range: None,
            next_range_id: 0,
            statistics: BulkStatistics::default(),
            last_path: None,
        })
    }

    pub async fn push(
        &mut self,
        mutation: IndexMutation<VectorDocument>,
    ) -> Result<(), IndexError> {
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
            let ordinal_base = range_ordinal_base(self.next_range_id)?;
            self.range = Some(VectorRange {
                ordinal_base,
                live: 0,
                paths: PathComponentWriter::new(
                    IndexKind::Vector,
                    BULK_OUTPUT_LEVEL,
                    BULK_TARGET_BLOCK_BYTES,
                ),
                documents: DocumentComponentWriter::with_ordinal_base(
                    IndexKind::Vector,
                    BULK_OUTPUT_LEVEL,
                    BULK_TARGET_BLOCK_BYTES,
                    ordinal_base,
                ),
                vectors: VectorComponentWriter::new(
                    IndexKind::Vector,
                    VECTORS_TAG,
                    BULK_OUTPUT_LEVEL,
                    self.definition.dimension,
                    BULK_TARGET_BLOCK_BYTES,
                ),
            });
        }
        let range = self.range.as_mut().expect("vector bulk range was opened");
        match mutation {
            IndexMutation::Upsert(value) => {
                validate_vector(&value.values, self.definition.dimension)?;
                let ordinal = range_local_ordinal(range.ordinal_base, range.live)?;
                range.live = range
                    .live
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                self.statistics.record(value.document.version, true)?;
                range
                    .paths
                    .push(
                        PathChange {
                            document: value.document.clone(),
                            state: DocumentState::Live,
                            document_ordinal: Some(ordinal),
                        },
                        &mut self.output,
                    )
                    .await?;
                range
                    .documents
                    .push(
                        DocumentRecord {
                            ordinal,
                            document: value.document,
                        },
                        &mut self.output,
                    )
                    .await?;
                range
                    .vectors
                    .push(
                        VectorRow {
                            ordinal,
                            values: value.values,
                        },
                        &mut self.output,
                    )
                    .await?;
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

    pub async fn finish_range(&mut self) -> Result<(), IndexError> {
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
            self.vectors
                .push(
                    range.vectors.finish(&mut self.output).await?,
                    &mut self.output,
                )
                .await?;
        }
        self.next_range_id = self
            .next_range_id
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<(Option<SealedRun>, S), IndexError> {
        self.finish_range().await?;
        if self.statistics.is_empty() {
            return Ok((None, self.output));
        }
        let statistics = self.statistics.finish()?;
        let mut components = vec![
            self.paths
                .finish(&mut self.output)
                .await?
                .ok_or(IndexError::InvalidFormat("missing bulk vector paths"))?,
        ];
        if statistics.live_document_count > 0 {
            components.push(
                self.documents
                    .finish(&mut self.output)
                    .await?
                    .ok_or(IndexError::InvalidFormat("missing bulk vector documents"))?,
            );
            components.push(
                self.vectors
                    .finish(&mut self.output)
                    .await?
                    .ok_or(IndexError::InvalidFormat("missing bulk vectors"))?,
            );
        }
        let run = seal_run_root(IndexKind::Vector, BULK_OUTPUT_LEVEL, statistics, components)?;
        Ok((Some(run), self.output))
    }
}
