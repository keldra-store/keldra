//! Conversion from ordinary object state into bounded format-3 engine work.

use std::io::Read;

use anvil_api::v1::index_specification::Specification;
use anvil_api::v1::{IndexSpecification, VectorMetric as ApiVectorMetric};
use anvil_index::bulk::{
    BulkBuildOptions, FullTextBulkBuilder, GitSourceBulkBuilder, HybridBulkBuilder,
    MetadataBulkBuilder, PathBulkBuilder, TensorBulkBuilder, TypedJsonBulkBuilder,
    VectorBulkBuilder,
};
use anvil_index::compaction::{CompactionExecutor, CompactionParallelism, CompactionProgress};
use anvil_index::full_text::{FullTextDocument, FullTextEngine, FullTextSegmentBuilder};
use anvil_index::hybrid::{HybridDefinition, HybridDocument, HybridEngine, HybridSegmentBuilder};
use anvil_index::ordered::{PathDocument, PathEngine, PathSegmentBuilder};
use anvil_index::projections::{
    GitSourceDocument, GitSourceEngine, GitSourceRecord, GitSourceSegmentBuilder, TensorDocument,
    TensorProjectionEngine, TensorRecord, TensorSegmentBuilder,
};
use anvil_index::typed_json::{
    MetadataDocument, MetadataFilterEngine, MetadataSegmentBuilder, ScalarValue,
    SelectedScalarFields, TypedField, TypedJsonDefinition, TypedJsonDocument, TypedJsonEngine,
    TypedJsonSegmentBuilder,
};
use anvil_index::vector::{
    VectorDefinition, VectorDocument, VectorEngine, VectorMetric, VectorSegmentBuilder,
};
use anvil_index::{
    DocumentRef, IndexBlockSink, IndexError, IndexKind, IndexMutation, SealedRun,
    SegmentMemoryPlan, SegmentPush,
};
use serde::Deserialize;

use super::directory::ManifestIndexDirectory;
use super::json_projection::{
    ProjectedJson, ProjectionSelection, project_json, projection_floor_bytes,
};
use super::publisher::IndexBlockStagingSink;

const PROJECTION_FIXED_BYTES: usize = 256;
const RECORD_PROJECTION_EXPANSION: u64 = 16;

#[derive(Clone, Debug)]
pub(crate) struct IndexBuildObject {
    pub path: String,
    pub version: u64,
    pub content_type: Option<String>,
    pub content_hash: [u8; 32],
    pub content_length: u64,
    pub committed_at_unix_millis: u64,
}

impl IndexBuildObject {
    pub(crate) fn document(&self) -> DocumentRef {
        DocumentRef {
            path: self.path.clone(),
            version: self.version,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum IndexSourceMutation {
    Upsert(IndexBuildObject),
    Remove(DocumentRef),
}

impl IndexSourceMutation {
    fn document(&self) -> DocumentRef {
        match self {
            Self::Upsert(object) => object.document(),
            Self::Remove(document) => document.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct IndexBuildDiagnostics {
    pub accepted_objects: u64,
    pub skipped_objects: u64,
}

impl IndexBuildDiagnostics {
    pub(crate) fn add(&mut self, other: Self) {
        self.accepted_objects = self.accepted_objects.saturating_add(other.accepted_objects);
        self.skipped_objects = self.skipped_objects.saturating_add(other.skipped_objects);
    }
}

#[derive(Debug)]
pub(crate) enum EngineMutation {
    Path(IndexMutation<PathDocument>),
    Metadata(IndexMutation<MetadataDocument>),
    TypedJson(IndexMutation<TypedJsonDocument>),
    FullText(IndexMutation<FullTextDocument>),
    Vector(IndexMutation<VectorDocument>),
    Hybrid(IndexMutation<HybridDocument>),
    GitSource(IndexMutation<GitSourceDocument>),
    Tensor(IndexMutation<TensorDocument>),
}

impl EngineMutation {
    pub(crate) fn estimated_bytes(&self) -> usize {
        match self {
            Self::Path(value) => PathSegmentBuilder::estimate_mutation(value),
            Self::Metadata(value) => MetadataSegmentBuilder::estimate_mutation(value),
            Self::TypedJson(value) => TypedJsonSegmentBuilder::estimate_mutation(value),
            Self::FullText(value) => FullTextSegmentBuilder::estimate_mutation(value),
            Self::Vector(value) => VectorSegmentBuilder::estimate_mutation(value),
            Self::Hybrid(value) => HybridSegmentBuilder::estimate_mutation(value),
            Self::GitSource(value) => GitSourceSegmentBuilder::estimate_mutation(value),
            Self::Tensor(value) => TensorSegmentBuilder::estimate_mutation(value),
        }
    }
}

pub(crate) enum EngineSegmentPush {
    Accepted,
    Full(EngineMutation),
}

pub(crate) enum EngineSegmentBuilder {
    Path(PathSegmentBuilder),
    Metadata(MetadataSegmentBuilder),
    TypedJson(TypedJsonSegmentBuilder),
    FullText(FullTextSegmentBuilder),
    Vector(VectorSegmentBuilder),
    Hybrid(HybridSegmentBuilder),
    GitSource(GitSourceSegmentBuilder),
    Tensor(TensorSegmentBuilder),
}

impl EngineSegmentBuilder {
    pub(crate) fn new(
        specification: &IndexSpecification,
        plan: SegmentMemoryPlan,
    ) -> Result<Self, IndexError> {
        let options = plan.options()?;
        match specification.specification.as_ref() {
            Some(Specification::Path(_)) => Ok(Self::Path(PathSegmentBuilder::new(options)?)),
            Some(Specification::MetadataFilter(value)) => Ok(Self::Metadata(
                MetadataSegmentBuilder::new(metadata_definition(&value.fields), options)?,
            )),
            Some(Specification::TypedJson(value)) => Ok(Self::TypedJson(
                TypedJsonSegmentBuilder::new(typed_definition(&value.fields), options)?,
            )),
            Some(Specification::FullText(_)) => {
                Ok(Self::FullText(FullTextSegmentBuilder::new(options)?))
            }
            Some(Specification::Vector(value)) => Ok(Self::Vector(VectorSegmentBuilder::new(
                vector_definition(value)?,
                options,
            )?)),
            Some(Specification::Hybrid(value)) => Ok(Self::Hybrid(HybridSegmentBuilder::new(
                hybrid_definition(value)?,
                options,
            )?)),
            Some(Specification::GitSource(_)) => {
                Ok(Self::GitSource(GitSourceSegmentBuilder::new(options)?))
            }
            Some(Specification::Tensor(_)) => Ok(Self::Tensor(TensorSegmentBuilder::new(options)?)),
            None => Err(missing_specification()),
        }
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        match self {
            Self::Path(value) => value.resident_bytes(),
            Self::Metadata(value) => value.resident_bytes(),
            Self::TypedJson(value) => value.resident_bytes(),
            Self::FullText(value) => value.resident_bytes(),
            Self::Vector(value) => value.resident_bytes(),
            Self::Hybrid(value) => value.resident_bytes(),
            Self::GitSource(value) => value.resident_bytes(),
            Self::Tensor(value) => value.resident_bytes(),
        }
    }

    pub(crate) fn seal_workspace_bytes(&self) -> Result<usize, IndexError> {
        match self {
            Self::Path(value) => value.seal_workspace_bytes(),
            Self::Metadata(value) => value.seal_workspace_bytes(),
            Self::TypedJson(value) => value.seal_workspace_bytes(),
            Self::FullText(value) => value.seal_workspace_bytes(),
            Self::Vector(value) => value.seal_workspace_bytes(),
            Self::Hybrid(value) => value.seal_workspace_bytes(),
            Self::GitSource(value) => value.seal_workspace_bytes(),
            Self::Tensor(value) => value.seal_workspace_bytes(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Path(value) => value.is_empty(),
            Self::Metadata(value) => value.is_empty(),
            Self::TypedJson(value) => value.is_empty(),
            Self::FullText(value) => value.is_empty(),
            Self::Vector(value) => value.is_empty(),
            Self::Hybrid(value) => value.is_empty(),
            Self::GitSource(value) => value.is_empty(),
            Self::Tensor(value) => value.is_empty(),
        }
    }

    pub(crate) fn try_push(
        &mut self,
        mutation: EngineMutation,
    ) -> Result<EngineSegmentPush, IndexError> {
        macro_rules! push {
            ($builder:expr, $mutation:expr, $wrap:path) => {
                match $builder.try_push($mutation)? {
                    SegmentPush::Accepted => EngineSegmentPush::Accepted,
                    SegmentPush::Full(value) => EngineSegmentPush::Full($wrap(value)),
                }
            };
        }
        let value = match (self, mutation) {
            (Self::Path(builder), EngineMutation::Path(value)) => {
                push!(builder, value, EngineMutation::Path)
            }
            (Self::Metadata(builder), EngineMutation::Metadata(value)) => {
                push!(builder, value, EngineMutation::Metadata)
            }
            (Self::TypedJson(builder), EngineMutation::TypedJson(value)) => {
                push!(builder, value, EngineMutation::TypedJson)
            }
            (Self::FullText(builder), EngineMutation::FullText(value)) => {
                push!(builder, value, EngineMutation::FullText)
            }
            (Self::Vector(builder), EngineMutation::Vector(value)) => {
                push!(builder, value, EngineMutation::Vector)
            }
            (Self::Hybrid(builder), EngineMutation::Hybrid(value)) => {
                push!(builder, value, EngineMutation::Hybrid)
            }
            (Self::GitSource(builder), EngineMutation::GitSource(value)) => {
                push!(builder, value, EngineMutation::GitSource)
            }
            (Self::Tensor(builder), EngineMutation::Tensor(value)) => {
                push!(builder, value, EngineMutation::Tensor)
            }
            _ => {
                return Err(IndexError::InvalidDefinition(
                    "index mutation kind differs from its builder".into(),
                ));
            }
        };
        Ok(value)
    }

    pub(crate) async fn seal<S: IndexBlockSink>(
        self,
        sink: &mut S,
    ) -> Result<Option<SealedRun>, IndexError> {
        match self {
            Self::Path(value) => value.seal(sink).await,
            Self::Metadata(value) => value.seal(sink).await,
            Self::TypedJson(value) => value.seal(sink).await,
            Self::FullText(value) => value.seal(sink).await,
            Self::Vector(value) => value.seal(sink).await,
            Self::Hybrid(value) => value.seal(sink).await,
            Self::GitSource(value) => value.seal(sink).await,
            Self::Tensor(value) => value.seal(sink).await,
        }
    }
}

pub(crate) enum EngineBulkBuilder<S, E> {
    Path(PathBulkBuilder<S>),
    Metadata(MetadataBulkBuilder<S, E>),
    TypedJson(TypedJsonBulkBuilder<S, E>),
    FullText(FullTextBulkBuilder<S, E>),
    Vector(VectorBulkBuilder<S>),
    Hybrid(HybridBulkBuilder<S, E>),
    GitSource(GitSourceBulkBuilder<S, E>),
    Tensor(TensorBulkBuilder<S, E>),
}

impl<S, E> EngineBulkBuilder<S, E>
where
    S: IndexBlockSink + anvil_index::IndexDirectoryRead + Clone + 'static,
    E: CompactionExecutor,
{
    pub(crate) fn new(
        specification: &IndexSpecification,
        sink: S,
        executor: E,
        options: BulkBuildOptions,
    ) -> Result<Self, IndexError> {
        match specification.specification.as_ref() {
            Some(Specification::Path(_)) => Ok(Self::Path(PathBulkBuilder::new(sink))),
            Some(Specification::MetadataFilter(value)) => {
                Ok(Self::Metadata(MetadataBulkBuilder::new(
                    metadata_definition(&value.fields),
                    sink,
                    executor,
                    options,
                )?))
            }
            Some(Specification::TypedJson(value)) => {
                Ok(Self::TypedJson(TypedJsonBulkBuilder::new(
                    typed_definition(&value.fields),
                    sink,
                    executor,
                    options,
                )?))
            }
            Some(Specification::FullText(_)) => Ok(Self::FullText(FullTextBulkBuilder::new(
                sink, executor, options,
            )?)),
            Some(Specification::Vector(value)) => Ok(Self::Vector(VectorBulkBuilder::new(
                vector_definition(value)?,
                sink,
            )?)),
            Some(Specification::Hybrid(value)) => Ok(Self::Hybrid(HybridBulkBuilder::new(
                hybrid_definition(value)?,
                sink,
                executor,
                options,
            )?)),
            Some(Specification::GitSource(_)) => Ok(Self::GitSource(GitSourceBulkBuilder::new(
                sink, executor, options,
            )?)),
            Some(Specification::Tensor(_)) => Ok(Self::Tensor(TensorBulkBuilder::new(
                sink, executor, options,
            )?)),
            None => Err(missing_specification()),
        }
    }

    pub(crate) async fn push(&mut self, mutation: EngineMutation) -> Result<(), IndexError> {
        match (self, mutation) {
            (Self::Path(builder), EngineMutation::Path(value)) => builder.push(value).await,
            (Self::Metadata(builder), EngineMutation::Metadata(value)) => builder.push(value).await,
            (Self::TypedJson(builder), EngineMutation::TypedJson(value)) => {
                builder.push(value).await
            }
            (Self::FullText(builder), EngineMutation::FullText(value)) => builder.push(value).await,
            (Self::Vector(builder), EngineMutation::Vector(value)) => builder.push(value).await,
            (Self::Hybrid(builder), EngineMutation::Hybrid(value)) => builder.push(value).await,
            (Self::GitSource(builder), EngineMutation::GitSource(value)) => {
                builder.push(value).await
            }
            (Self::Tensor(builder), EngineMutation::Tensor(value)) => builder.push(value).await,
            _ => Err(IndexError::InvalidDefinition(
                "index mutation kind differs from its bulk builder".into(),
            )),
        }
    }

    pub(crate) async fn finish_range(&mut self) -> Result<(), IndexError> {
        match self {
            Self::Path(value) => value.finish_range().await,
            Self::Metadata(value) => value.finish_range().await,
            Self::TypedJson(value) => value.finish_range().await,
            Self::FullText(value) => value.finish_range().await,
            Self::Vector(value) => value.finish_range().await,
            Self::Hybrid(value) => value.finish_range().await,
            Self::GitSource(value) => value.finish_range().await,
            Self::Tensor(value) => value.finish_range().await,
        }
    }

    pub(crate) fn external_sort_progress(&self) -> Option<CompactionProgress> {
        match self {
            Self::Metadata(value) => Some(value.progress()),
            Self::TypedJson(value) => Some(value.progress()),
            Self::FullText(value) => Some(value.progress()),
            Self::Hybrid(value) => Some(value.progress()),
            Self::GitSource(value) => Some(value.progress()),
            Self::Tensor(value) => Some(value.progress()),
            Self::Path(_) | Self::Vector(_) => None,
        }
    }

    pub(crate) async fn finish(self) -> Result<(Option<SealedRun>, S), IndexError> {
        match self {
            Self::Path(value) => value.finish().await,
            Self::Metadata(value) => value.finish().await,
            Self::TypedJson(value) => value.finish().await,
            Self::FullText(value) => value.finish().await,
            Self::Vector(value) => value.finish().await,
            Self::Hybrid(value) => value.finish().await,
            Self::GitSource(value) => value.finish().await,
            Self::Tensor(value) => value.finish().await,
        }
    }
}

pub(crate) fn kind_for_specification(
    specification: &IndexSpecification,
) -> Result<IndexKind, IndexError> {
    match specification.specification.as_ref() {
        Some(Specification::Path(_)) => Ok(IndexKind::Path),
        Some(Specification::MetadataFilter(_)) => Ok(IndexKind::MetadataFilter),
        Some(Specification::TypedJson(_)) => Ok(IndexKind::TypedJson),
        Some(Specification::FullText(_)) => Ok(IndexKind::FullText),
        Some(Specification::Vector(_)) => Ok(IndexKind::Vector),
        Some(Specification::Hybrid(_)) => Ok(IndexKind::Hybrid),
        Some(Specification::GitSource(_)) => Ok(IndexKind::GitSource),
        Some(Specification::Tensor(_)) => Ok(IndexKind::Tensor),
        None => Err(missing_specification()),
    }
}

/// Minimum selected-state reservation required before any payload bytes are
/// pulled. The manager holds the complete per-kind permit around this work.
/// Selective JSON does not charge unrelated payload bytes; whole-record Git and
/// tensor projections conservatively charge an expansion of the complete body.
pub(crate) fn projection_admission_bytes(
    specification: &IndexSpecification,
    source: &IndexSourceMutation,
) -> Result<u64, IndexError> {
    let base = source_base_bytes(source)?;
    let extra = match specification.specification.as_ref() {
        Some(Specification::Path(_)) => 0,
        Some(Specification::MetadataFilter(value)) => value
            .fields
            .iter()
            .try_fold(0usize, |total, field| checked_add(total, field.len() + 24))?,
        Some(Specification::TypedJson(value)) => {
            projection_floor_bytes(&ProjectionSelection::Scalars(named_pointers(&value.fields)))?
        }
        Some(Specification::FullText(value)) => projection_floor_bytes(
            &ProjectionSelection::Strings(full_text_pointers(&value.fields)),
        )?,
        Some(Specification::Vector(value)) => {
            projection_floor_bytes(&ProjectionSelection::Vector {
                pointer: value.json_pointer.clone(),
                dimensions: value.dimensions as usize,
                normalize: value.normalize,
            })?
        }
        Some(Specification::Hybrid(value)) => {
            let text = value.full_text.as_ref().ok_or_else(|| {
                IndexError::InvalidDefinition("hybrid full-text spec is required".into())
            })?;
            let vector = value.vector.as_ref().ok_or_else(|| {
                IndexError::InvalidDefinition("hybrid vector spec is required".into())
            })?;
            projection_floor_bytes(&ProjectionSelection::Hybrid {
                strings: full_text_pointers(&text.fields),
                vector_pointer: vector.json_pointer.clone(),
                dimensions: vector.dimensions as usize,
                normalize: vector.normalize,
            })?
        }
        Some(Specification::GitSource(value)) => {
            whole_record_projection_bytes(source, value.repository_id.len())?
        }
        Some(Specification::Tensor(value)) => {
            whole_record_projection_bytes(source, value.model_id.len())?
        }
        None => return Err(missing_specification()),
    };
    let needed = checked_add(base, extra)?;
    u64::try_from(needed).map_err(|_| IndexError::OffsetOverflow)
}

pub(crate) fn project_mutation(
    specification: &IndexSpecification,
    source: IndexSourceMutation,
    payload: Option<&mut dyn Read>,
    max_projection_bytes: usize,
) -> Result<(EngineMutation, IndexBuildDiagnostics), IndexError> {
    let source_base = source_base_bytes(&source)?;
    let needed = usize::try_from(projection_admission_bytes(specification, &source)?)
        .map_err(|_| IndexError::OffsetOverflow)?;
    if needed > max_projection_bytes {
        return Err(IndexError::ResourceLimit {
            needed,
            limit: max_projection_bytes,
        });
    }
    let selected_limit =
        max_projection_bytes
            .checked_sub(source_base)
            .ok_or(IndexError::ResourceLimit {
                needed: source_base,
                limit: max_projection_bytes,
            })?;
    let document = source.document();
    if matches!(source, IndexSourceMutation::Remove(_)) {
        return bounded_projection(
            remove_for(specification, document)?,
            accepted(),
            max_projection_bytes,
        );
    }
    let IndexSourceMutation::Upsert(object) = source else {
        unreachable!("remove returned above")
    };

    let projected = match specification.specification.as_ref() {
        Some(Specification::Path(_)) => (
            EngineMutation::Path(IndexMutation::Upsert(PathDocument {
                document: object.document(),
            })),
            accepted(),
        ),
        Some(Specification::MetadataFilter(value)) => (
            EngineMutation::Metadata(IndexMutation::Upsert(MetadataDocument {
                document: object.document(),
                fields: object_metadata(&object, &value.fields),
            })),
            accepted(),
        ),
        Some(Specification::TypedJson(value)) => match project_selected_json(
            payload,
            ProjectionSelection::Scalars(named_pointers(&value.fields)),
            selected_limit,
        )? {
            Some(ProjectedJson::Scalars(fields)) if !fields.is_empty() => (
                EngineMutation::TypedJson(IndexMutation::Upsert(TypedJsonDocument {
                    document: object.document(),
                    fields,
                })),
                accepted(),
            ),
            Some(ProjectedJson::Scalars(_)) | None => (
                EngineMutation::TypedJson(IndexMutation::Remove(object.document())),
                skipped(),
            ),
            Some(_) => return Err(projection_kind_mismatch()),
        },
        Some(Specification::FullText(value)) => match project_selected_json(
            payload,
            ProjectionSelection::Strings(full_text_pointers(&value.fields)),
            selected_limit,
        )? {
            Some(ProjectedJson::Strings(fields)) if !fields.is_empty() => (
                EngineMutation::FullText(IndexMutation::Upsert(FullTextDocument {
                    document: object.document(),
                    fields,
                })),
                accepted(),
            ),
            Some(ProjectedJson::Strings(_)) | None => (
                EngineMutation::FullText(IndexMutation::Remove(object.document())),
                skipped(),
            ),
            Some(_) => return Err(projection_kind_mismatch()),
        },
        Some(Specification::Vector(value)) => match project_selected_json(
            payload,
            ProjectionSelection::Vector {
                pointer: value.json_pointer.clone(),
                dimensions: value.dimensions as usize,
                normalize: value.normalize,
            },
            selected_limit,
        )? {
            Some(ProjectedJson::Vector(values)) => (
                EngineMutation::Vector(IndexMutation::Upsert(VectorDocument {
                    document: object.document(),
                    values,
                })),
                accepted(),
            ),
            None => (
                EngineMutation::Vector(IndexMutation::Remove(object.document())),
                skipped(),
            ),
            Some(_) => return Err(projection_kind_mismatch()),
        },
        Some(Specification::Hybrid(value)) => {
            let text = value.full_text.as_ref().ok_or_else(|| {
                IndexError::InvalidDefinition("hybrid full-text spec is required".into())
            })?;
            let vector = value.vector.as_ref().ok_or_else(|| {
                IndexError::InvalidDefinition("hybrid vector spec is required".into())
            })?;
            match project_selected_json(
                payload,
                ProjectionSelection::Hybrid {
                    strings: full_text_pointers(&text.fields),
                    vector_pointer: vector.json_pointer.clone(),
                    dimensions: vector.dimensions as usize,
                    normalize: vector.normalize,
                },
                selected_limit,
            )? {
                Some(ProjectedJson::Hybrid { strings, vector }) if !strings.is_empty() => (
                    EngineMutation::Hybrid(IndexMutation::Upsert(HybridDocument {
                        document: object.document(),
                        text_fields: strings,
                        vector,
                    })),
                    accepted(),
                ),
                Some(ProjectedJson::Hybrid { .. }) | None => (
                    EngineMutation::Hybrid(IndexMutation::Remove(object.document())),
                    skipped(),
                ),
                Some(_) => return Err(projection_kind_mismatch()),
            }
        }
        Some(Specification::GitSource(value)) => {
            let records = parse_records::<GitSourceRecord>(payload)?
                .unwrap_or_default()
                .into_iter()
                .filter(|record| record.repository_id == value.repository_id)
                .collect::<Vec<_>>();
            if records.is_empty() {
                (
                    EngineMutation::GitSource(IndexMutation::Remove(object.document())),
                    skipped(),
                )
            } else {
                (
                    EngineMutation::GitSource(IndexMutation::Upsert(GitSourceDocument {
                        document: object.document(),
                        records,
                    })),
                    accepted(),
                )
            }
        }
        Some(Specification::Tensor(value)) => {
            let records = parse_records::<TensorRecord>(payload)?
                .unwrap_or_default()
                .into_iter()
                .filter(|record| {
                    record.model_id == value.model_id
                        && !record.tensor_name.is_empty()
                        && !record.source_path.is_empty()
                        && record.source_version > 0
                })
                .collect::<Vec<_>>();
            if records.is_empty() {
                (
                    EngineMutation::Tensor(IndexMutation::Remove(object.document())),
                    skipped(),
                )
            } else {
                (
                    EngineMutation::Tensor(IndexMutation::Upsert(TensorDocument {
                        document: object.document(),
                        records,
                    })),
                    accepted(),
                )
            }
        }
        None => return Err(missing_specification()),
    };
    bounded_projection(projected.0, projected.1, max_projection_bytes)
}

pub(crate) async fn merge_runs(
    specification: &IndexSpecification,
    runs: &[ManifestIndexDirectory],
    output_level: u8,
    sink: &mut IndexBlockStagingSink,
) -> Result<SealedRun, IndexError> {
    match specification.specification.as_ref() {
        Some(Specification::Path(_)) => PathEngine::merge_runs(runs, output_level, sink).await,
        Some(Specification::MetadataFilter(_)) => {
            MetadataFilterEngine::merge_runs(runs, output_level, sink).await
        }
        Some(Specification::TypedJson(_)) => {
            TypedJsonEngine::merge_runs(runs, output_level, sink).await
        }
        Some(Specification::FullText(_)) => {
            FullTextEngine::merge_runs(runs, output_level, sink).await
        }
        Some(Specification::Vector(value)) => {
            VectorEngine::merge_runs(runs, &vector_definition(value)?, output_level, sink).await
        }
        Some(Specification::Hybrid(value)) => {
            HybridEngine::merge_runs(runs, &hybrid_definition(value)?, output_level, sink).await
        }
        Some(Specification::GitSource(_)) => {
            GitSourceEngine::merge_runs(runs, output_level, sink).await
        }
        Some(Specification::Tensor(_)) => {
            TensorProjectionEngine::merge_runs(runs, output_level, sink).await
        }
        None => Err(missing_specification()),
    }
}

pub(crate) async fn merge_runs_parallel<E: CompactionExecutor>(
    specification: &IndexSpecification,
    runs: &[ManifestIndexDirectory],
    output_level: u8,
    sink: &mut IndexBlockStagingSink,
    parallelism: CompactionParallelism,
    progress: CompactionProgress,
    executor: E,
) -> Result<SealedRun, IndexError> {
    match specification.specification.as_ref() {
        Some(Specification::Path(_)) => {
            PathEngine::merge_runs_parallel(
                runs,
                output_level,
                sink,
                parallelism,
                progress,
                executor,
            )
            .await
        }
        Some(Specification::MetadataFilter(_)) => {
            MetadataFilterEngine::merge_runs_parallel(
                runs,
                output_level,
                sink,
                parallelism,
                progress,
                executor,
            )
            .await
        }
        Some(Specification::TypedJson(_)) => {
            TypedJsonEngine::merge_runs_parallel(
                runs,
                output_level,
                sink,
                parallelism,
                progress,
                executor,
            )
            .await
        }
        Some(Specification::FullText(_)) => {
            FullTextEngine::merge_runs_parallel(
                runs,
                output_level,
                sink,
                parallelism,
                progress,
                executor,
            )
            .await
        }
        Some(Specification::Vector(value)) => {
            VectorEngine::merge_runs_parallel(
                runs,
                &vector_definition(value)?,
                output_level,
                sink,
                parallelism,
                progress,
                executor,
            )
            .await
        }
        Some(Specification::Hybrid(value)) => {
            HybridEngine::merge_runs_parallel(
                runs,
                &hybrid_definition(value)?,
                output_level,
                sink,
                parallelism,
                progress,
                executor,
            )
            .await
        }
        Some(Specification::GitSource(_)) => {
            GitSourceEngine::merge_runs_parallel(
                runs,
                output_level,
                sink,
                parallelism,
                progress,
                executor,
            )
            .await
        }
        Some(Specification::Tensor(_)) => {
            TensorProjectionEngine::merge_runs_parallel(
                runs,
                output_level,
                sink,
                parallelism,
                progress,
                executor,
            )
            .await
        }
        None => Err(missing_specification()),
    }
}

pub(crate) fn typed_definition(fields: &[anvil_api::v1::IndexField]) -> TypedJsonDefinition {
    TypedJsonDefinition {
        fields: fields
            .iter()
            .map(|field| TypedField {
                name: field.name.clone(),
                json_pointer: field.json_pointer.clone(),
            })
            .collect(),
    }
}

pub(crate) fn metadata_definition(fields: &[String]) -> TypedJsonDefinition {
    TypedJsonDefinition {
        fields: fields
            .iter()
            .map(|field| TypedField {
                name: field.clone(),
                json_pointer: format!("/{field}"),
            })
            .collect(),
    }
}

pub(crate) fn vector_definition(
    specification: &anvil_api::v1::VectorIndexSpec,
) -> Result<VectorDefinition, IndexError> {
    let metric = match ApiVectorMetric::try_from(specification.metric)
        .map_err(|_| IndexError::InvalidDefinition("unknown vector metric".into()))?
    {
        ApiVectorMetric::Cosine => VectorMetric::Cosine,
        ApiVectorMetric::Dot => VectorMetric::DotProduct,
        ApiVectorMetric::Euclidean => VectorMetric::Euclidean,
    };
    Ok(VectorDefinition {
        dimension: specification.dimensions as usize,
        metric,
    })
}

pub(crate) fn hybrid_definition(
    specification: &anvil_api::v1::HybridIndexSpec,
) -> Result<HybridDefinition, IndexError> {
    let vector = specification
        .vector
        .as_ref()
        .ok_or_else(|| IndexError::InvalidDefinition("hybrid vector spec is required".into()))?;
    Ok(HybridDefinition {
        vector: vector_definition(vector)?,
        text_weight: effective_weight(specification.full_text_weight),
        vector_weight: effective_weight(specification.vector_weight),
    })
}

fn remove_for(
    specification: &IndexSpecification,
    document: DocumentRef,
) -> Result<EngineMutation, IndexError> {
    match specification.specification.as_ref() {
        Some(Specification::Path(_)) => Ok(EngineMutation::Path(IndexMutation::Remove(document))),
        Some(Specification::MetadataFilter(_)) => {
            Ok(EngineMutation::Metadata(IndexMutation::Remove(document)))
        }
        Some(Specification::TypedJson(_)) => {
            Ok(EngineMutation::TypedJson(IndexMutation::Remove(document)))
        }
        Some(Specification::FullText(_)) => {
            Ok(EngineMutation::FullText(IndexMutation::Remove(document)))
        }
        Some(Specification::Vector(_)) => {
            Ok(EngineMutation::Vector(IndexMutation::Remove(document)))
        }
        Some(Specification::Hybrid(_)) => {
            Ok(EngineMutation::Hybrid(IndexMutation::Remove(document)))
        }
        Some(Specification::GitSource(_)) => {
            Ok(EngineMutation::GitSource(IndexMutation::Remove(document)))
        }
        Some(Specification::Tensor(_)) => {
            Ok(EngineMutation::Tensor(IndexMutation::Remove(document)))
        }
        None => Err(missing_specification()),
    }
}

fn bounded_projection(
    mutation: EngineMutation,
    diagnostics: IndexBuildDiagnostics,
    limit: usize,
) -> Result<(EngineMutation, IndexBuildDiagnostics), IndexError> {
    let needed = mutation
        .estimated_bytes()
        .saturating_add(PROJECTION_FIXED_BYTES);
    if needed > limit {
        return Err(IndexError::ResourceLimit { needed, limit });
    }
    Ok((mutation, diagnostics))
}

fn project_selected_json(
    payload: Option<&mut dyn Read>,
    selection: ProjectionSelection,
    max_projection_bytes: usize,
) -> Result<Option<ProjectedJson>, IndexError> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    project_json(payload, &selection, max_projection_bytes)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

fn parse_records<T: serde::de::DeserializeOwned>(
    payload: Option<&mut dyn Read>,
) -> Result<Option<Vec<T>>, IndexError> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    match serde_json::from_reader::<_, OneOrMany<T>>(payload) {
        Ok(OneOrMany::One(record)) => Ok(Some(vec![record])),
        Ok(OneOrMany::Many(records)) => Ok(Some(records)),
        Err(error) if error.is_syntax() || error.is_data() || error.is_eof() => Ok(None),
        Err(error) => Err(IndexError::Io(error.to_string())),
    }
}

fn named_pointers(fields: &[anvil_api::v1::IndexField]) -> Vec<(String, String)> {
    fields
        .iter()
        .map(|field| (field.name.clone(), field.json_pointer.clone()))
        .collect()
}

fn full_text_pointers(fields: &[anvil_api::v1::FullTextField]) -> Vec<(String, String)> {
    fields
        .iter()
        .map(|field| (field.name.clone(), field.json_pointer.clone()))
        .collect()
}

fn object_metadata(object: &IndexBuildObject, fields: &[String]) -> SelectedScalarFields {
    fields
        .iter()
        .map(|field| {
            let value = match field.as_str() {
                "path" => ScalarValue::String(object.path.clone()),
                "version" => ScalarValue::Unsigned(object.version),
                "content_type" => object
                    .content_type
                    .clone()
                    .map_or(ScalarValue::Null, ScalarValue::String),
                "content_length" => ScalarValue::Unsigned(object.content_length),
                "content_hash" => ScalarValue::String(hex::encode(object.content_hash)),
                "committed_at_unix_millis" => {
                    ScalarValue::Unsigned(object.committed_at_unix_millis)
                }
                _ => ScalarValue::Null,
            };
            (field.clone(), vec![value])
        })
        .collect()
}

fn source_base_bytes(source: &IndexSourceMutation) -> Result<usize, IndexError> {
    let bytes = match source {
        IndexSourceMutation::Upsert(object) => object
            .path
            .len()
            .checked_add(object.content_type.as_ref().map_or(0, String::len))
            .and_then(|value| value.checked_add(PROJECTION_FIXED_BYTES + 32)),
        IndexSourceMutation::Remove(document) => {
            document.path.len().checked_add(PROJECTION_FIXED_BYTES)
        }
    };
    bytes.ok_or(IndexError::OffsetOverflow)
}

fn whole_record_projection_bytes(
    source: &IndexSourceMutation,
    fixed_value_bytes: usize,
) -> Result<usize, IndexError> {
    let content_length = match source {
        IndexSourceMutation::Upsert(object) => object.content_length,
        IndexSourceMutation::Remove(_) => 0,
    };
    let expanded = content_length
        .checked_mul(RECORD_PROJECTION_EXPANSION)
        .ok_or(IndexError::OffsetOverflow)?;
    usize::try_from(expanded)
        .map_err(|_| IndexError::OffsetOverflow)?
        .checked_add(fixed_value_bytes)
        .ok_or(IndexError::OffsetOverflow)
}

fn checked_add(left: usize, right: usize) -> Result<usize, IndexError> {
    left.checked_add(right).ok_or(IndexError::OffsetOverflow)
}

fn accepted() -> IndexBuildDiagnostics {
    IndexBuildDiagnostics {
        accepted_objects: 1,
        skipped_objects: 0,
    }
}

fn skipped() -> IndexBuildDiagnostics {
    IndexBuildDiagnostics {
        accepted_objects: 0,
        skipped_objects: 1,
    }
}

fn effective_weight(weight: f32) -> f32 {
    if weight == 0.0 { 1.0 } else { weight }
}

fn missing_specification() -> IndexError {
    IndexError::InvalidDefinition("index specification is required".into())
}

fn projection_kind_mismatch() -> IndexError {
    IndexError::InvalidDefinition("JSON projection differs from the index specification".into())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use anvil_api::v1::{
        FullTextField, GitSourceIndexSpec, IndexField, MetadataFilterIndexSpec, TypedJsonIndexSpec,
        VectorIndexSpec,
    };
    use anvil_index::MIN_INDEX_KIND_MEMORY_BYTES;

    use super::*;

    fn object(path: &str, length: u64) -> IndexBuildObject {
        IndexBuildObject {
            path: path.into(),
            version: 4,
            content_type: Some("application/json".into()),
            content_hash: [7; 32],
            content_length: length,
            committed_at_unix_millis: 7,
        }
    }

    fn typed_spec(name: &str) -> IndexSpecification {
        IndexSpecification {
            specification: Some(Specification::TypedJson(TypedJsonIndexSpec {
                fields: vec![IndexField {
                    name: name.into(),
                    json_pointer: "/state".into(),
                }],
            })),
        }
    }

    #[test]
    fn malformed_payload_becomes_a_newer_tombstone_not_stale_results() {
        let specification = typed_spec("state");
        let mut payload = Cursor::new(b"not-json");
        let (mutation, diagnostics) = project_mutation(
            &specification,
            IndexSourceMutation::Upsert(object("bad", 8)),
            Some(&mut payload),
            64 * 1024,
        )
        .unwrap();
        assert!(matches!(
            mutation,
            EngineMutation::TypedJson(IndexMutation::Remove(DocumentRef { version: 4, .. }))
        ));
        assert_eq!(diagnostics.skipped_objects, 1);
    }

    #[test]
    fn one_kind_builder_never_accepts_another_kind_mutation() {
        let specification = IndexSpecification {
            specification: Some(Specification::Path(anvil_api::v1::PathIndexSpec {})),
        };
        let plan = SegmentMemoryPlan::new(MIN_INDEX_KIND_MEMORY_BYTES).unwrap();
        let mut builder = EngineSegmentBuilder::new(&specification, plan).unwrap();
        let result = builder.try_push(EngineMutation::Vector(IndexMutation::Remove(DocumentRef {
            path: "a".into(),
            version: 1,
        })));
        assert!(matches!(result, Err(IndexError::InvalidDefinition(_))));
    }

    #[test]
    fn metadata_projection_uses_typed_scalar_fields() {
        let specification = IndexSpecification {
            specification: Some(Specification::MetadataFilter(MetadataFilterIndexSpec {
                fields: vec!["path".into(), "content_length".into()],
            })),
        };
        let (mutation, diagnostics) = project_mutation(
            &specification,
            IndexSourceMutation::Upsert(object("records/7", 91)),
            None,
            64 * 1024,
        )
        .unwrap();
        let EngineMutation::Metadata(IndexMutation::Upsert(document)) = mutation else {
            panic!("expected metadata upsert")
        };
        assert_eq!(
            document.fields["path"],
            vec![ScalarValue::String("records/7".into())]
        );
        assert_eq!(
            document.fields["content_length"],
            vec![ScalarValue::Unsigned(91)]
        );
        assert_eq!(diagnostics.accepted_objects, 1);
    }

    #[test]
    fn metadata_projection_preserves_u64_values_above_json_float_precision() {
        let specification = IndexSpecification {
            specification: Some(Specification::MetadataFilter(MetadataFilterIndexSpec {
                fields: vec![
                    "version".into(),
                    "content_length".into(),
                    "committed_at_unix_millis".into(),
                ],
            })),
        };
        let mut object = object("records/exact", (1_u64 << 53) + 1);
        object.version = (1_u64 << 53) + 3;
        object.committed_at_unix_millis = u64::MAX;

        let (mutation, _) = project_mutation(
            &specification,
            IndexSourceMutation::Upsert(object),
            None,
            64 * 1024,
        )
        .unwrap();
        let EngineMutation::Metadata(IndexMutation::Upsert(document)) = mutation else {
            panic!("expected metadata upsert")
        };
        assert_eq!(
            document.fields["version"],
            [ScalarValue::Unsigned((1_u64 << 53) + 3)]
        );
        assert_eq!(
            document.fields["content_length"],
            [ScalarValue::Unsigned((1_u64 << 53) + 1)]
        );
        assert_eq!(
            document.fields["committed_at_unix_millis"],
            [ScalarValue::Unsigned(u64::MAX)]
        );
    }

    #[test]
    fn admission_counts_definitions_vectors_and_whole_record_expansion() {
        let source = IndexSourceMutation::Upsert(object("records/7", 100));
        assert!(
            projection_admission_bytes(&typed_spec(&"x".repeat(1000)), &source).unwrap()
                > projection_admission_bytes(&typed_spec("x"), &source).unwrap()
        );

        let vector = |dimensions| IndexSpecification {
            specification: Some(Specification::Vector(VectorIndexSpec {
                json_pointer: "/vector".into(),
                dimensions,
                metric: ApiVectorMetric::Cosine as i32,
                normalize: false,
            })),
        };
        assert!(
            projection_admission_bytes(&vector(1024), &source).unwrap()
                > projection_admission_bytes(&vector(2), &source).unwrap()
        );

        let git = IndexSpecification {
            specification: Some(Specification::GitSource(GitSourceIndexSpec {
                repository_id: "repository".into(),
            })),
        };
        assert!(
            projection_admission_bytes(&git, &source).unwrap() >= 100 * RECORD_PROJECTION_EXPANSION
        );
    }

    #[test]
    fn full_text_projection_floor_counts_long_field_names() {
        let source = IndexSourceMutation::Upsert(object("records/7", 10));
        let specification = |name: String| IndexSpecification {
            specification: Some(Specification::FullText(anvil_api::v1::FullTextIndexSpec {
                fields: vec![FullTextField {
                    name,
                    json_pointer: "/body".into(),
                }],
            })),
        };
        assert!(
            projection_admission_bytes(&specification("x".repeat(1000)), &source).unwrap()
                > projection_admission_bytes(&specification("x".into()), &source).unwrap()
        );
    }
}
