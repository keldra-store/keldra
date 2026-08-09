//! Storage-neutral execution of one immutable index generation.

use anvil_api::v1::index_query::Query;
use anvil_api::v1::index_specification::Specification;
use anvil_api::v1::{
    IndexOrderDirection, IndexPredicate, IndexPredicateOperator, IndexQuery, IndexSpecification,
    VectorMetric as ApiVectorMetric,
};
use anvil_index::full_text::{FullTextEngine, FullTextQuery, FullTextQueryCursor};
use anvil_index::hybrid::{HybridDefinition, HybridEngine, HybridQuery, HybridQueryCursor};
use anvil_index::ordered::{PathEngine, PathQuery};
use anvil_index::projections::{GitSourceEngine, TensorProjectionEngine};
use anvil_index::typed_json::{
    MetadataFilterEngine, ScalarValue, TypedField, TypedJsonDefinition, TypedJsonEngine,
    TypedOrder, TypedPredicate, TypedQuery, TypedQueryCursor,
};
use anvil_index::vector::{VectorDefinition, VectorEngine, VectorMetric, VectorQueryCursor};
use anvil_index::{IndexDirectoryRead, IndexError};
use serde::{Deserialize, Serialize};

const INDEX_QUERY_POSITION_FORMAT: u8 = 2;

/// Engine-native continuation state carried inside the signed public page
/// token. The outer token binds these bytes to the immutable generation,
/// definition, query, and authorization revision.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexQueryPosition {
    format: u8,
    cursor: Option<IndexQueryCursor>,
}

impl Default for IndexQueryPosition {
    fn default() -> Self {
        Self {
            format: INDEX_QUERY_POSITION_FORMAT,
            cursor: None,
        }
    }
}

impl IndexQueryPosition {
    fn after(cursor: IndexQueryCursor) -> Self {
        Self {
            format: INDEX_QUERY_POSITION_FORMAT,
            cursor: Some(cursor),
        }
    }

    fn validate(&self) -> Result<(), IndexError> {
        if self.format == INDEX_QUERY_POSITION_FORMAT {
            Ok(())
        } else {
            Err(IndexError::InvalidQuery(
                "index page position uses an unsupported format".into(),
            ))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "engine",
    content = "position",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum IndexQueryCursor {
    Path { after_path: String },
    Git { after_path: String },
    Typed(TypedQueryCursor),
    FullText(FullTextQueryCursor),
    Vector(VectorQueryCursor),
    Hybrid(HybridQueryCursor),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EngineQueryHit {
    pub object_path: Option<String>,
    pub object_version: u64,
    pub score: Option<f32>,
    pub fields_json: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EngineQueryPage {
    pub hits: Vec<EngineQueryHit>,
    pub next: Option<IndexQueryPosition>,
}

pub(crate) async fn execute_query<D: IndexDirectoryRead>(
    segments: &[D],
    specification: &IndexSpecification,
    query: &IndexQuery,
    page_size: usize,
    position: IndexQueryPosition,
) -> Result<EngineQueryPage, IndexError> {
    position.validate()?;
    if page_size == 0 {
        return Ok(EngineQueryPage {
            hits: Vec::new(),
            next: None,
        });
    }
    match (specification.specification.as_ref(), query.query.as_ref()) {
        (Some(Specification::Path(_)), Some(Query::Path(query))) => {
            query_path(segments, query, page_size, position).await
        }
        (
            Some(Specification::MetadataFilter(specification)),
            Some(Query::MetadataFilter(query)),
        ) => {
            let definition = metadata_definition(&specification.fields);
            let query = typed_query(&query.predicates, &[], page_limit(page_size)?)?;
            let hits = MetadataFilterEngine::query_after(
                segments,
                &definition,
                &query,
                typed_cursor(&position)?,
            )
            .await?;
            typed_page(hits, &query.order, page_size)
        }
        (Some(Specification::TypedJson(specification)), Some(Query::TypedJson(query))) => {
            let definition = typed_definition(&specification.fields);
            let query = typed_query(&query.predicates, &query.order, page_limit(page_size)?)?;
            let hits = TypedJsonEngine::query_after(
                segments,
                &definition,
                &query,
                typed_cursor(&position)?,
            )
            .await?;
            typed_page(hits, &query.order, page_size)
        }
        (Some(Specification::FullText(_)), Some(Query::FullText(query))) => {
            let hits = FullTextEngine::query_after(
                segments,
                FullTextQuery {
                    text: &query.text,
                    fields: &[],
                    phrase: query.phrase,
                    match_all_terms: false,
                    limit: page_limit(page_size)?,
                },
                full_text_cursor(&position)?,
            )
            .await?;
            full_text_page(hits, page_size)
        }
        (Some(Specification::Vector(specification)), Some(Query::Vector(query))) => {
            let definition = vector_definition(specification)?;
            let values = normalize_query_vector(
                query.values.clone(),
                definition.dimension,
                specification.normalize,
            )?;
            let hits = VectorEngine::query_after(
                segments,
                &definition,
                &values,
                page_limit(page_size)?,
                vector_cursor(&position)?,
            )
            .await?;
            vector_page(hits, page_size)
        }
        (Some(Specification::Hybrid(specification)), Some(Query::Hybrid(query))) => {
            let vector = specification.vector.as_ref().ok_or_else(|| {
                IndexError::InvalidDefinition("hybrid vector spec missing".into())
            })?;
            let definition = HybridDefinition {
                vector: vector_definition(vector)?,
                text_weight: effective_weight(specification.full_text_weight),
                vector_weight: effective_weight(specification.vector_weight),
            };
            let values = if query.vector.is_empty() {
                Vec::new()
            } else {
                normalize_query_vector(
                    query.vector.clone(),
                    definition.vector.dimension,
                    vector.normalize,
                )?
            };
            let hits = HybridEngine::query_after(
                segments,
                &definition,
                HybridQuery {
                    text: &query.text,
                    vector: &values,
                    fields: &[],
                    phrase: false,
                    limit: page_limit(page_size)?,
                },
                hybrid_cursor(&position)?,
            )
            .await?;
            hybrid_page(hits, page_size)
        }
        (Some(Specification::GitSource(specification)), Some(Query::GitSource(query))) => {
            query_git(
                segments,
                &specification.repository_id,
                query,
                page_size,
                position,
            )
            .await
        }
        (Some(Specification::Tensor(specification)), Some(Query::Tensor(query))) => {
            query_tensor(
                segments,
                &specification.model_id,
                &query.tensor_name,
                position,
            )
            .await
        }
        (Some(_), Some(_)) => Err(IndexError::InvalidQuery(
            "query kind does not match index kind".into(),
        )),
        (_, None) => Err(IndexError::InvalidQuery("index query is required".into())),
        (None, _) => Err(IndexError::InvalidDefinition(
            "index specification is required".into(),
        )),
    }
}

fn full_text_page(
    hits: Vec<anvil_index::full_text::FullTextHit>,
    page_size: usize,
) -> Result<EngineQueryPage, IndexError> {
    let has_more = hits.len() > page_size;
    let selected = hits.into_iter().take(page_size).collect::<Vec<_>>();
    let next = has_more.then(|| {
        IndexQueryPosition::after(IndexQueryCursor::FullText(FullTextQueryCursor::from_hit(
            selected.last().expect("a nonempty page has a last hit"),
        )))
    });
    let hits = selected
        .into_iter()
        .map(|hit| {
            Ok(EngineQueryHit {
                object_path: Some(hit.document.path),
                object_version: hit.document.version,
                score: Some(hit.score),
                fields_json: serde_json::to_vec(&serde_json::json!({
                    "matched_terms": hit.matched_terms,
                }))
                .map_err(|error| IndexError::Encode(error.to_string()))?,
            })
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    Ok(EngineQueryPage { hits, next })
}

fn vector_page(
    hits: Vec<anvil_index::vector::VectorHit>,
    page_size: usize,
) -> Result<EngineQueryPage, IndexError> {
    let has_more = hits.len() > page_size;
    let selected = hits.into_iter().take(page_size).collect::<Vec<_>>();
    let next = has_more.then(|| {
        IndexQueryPosition::after(IndexQueryCursor::Vector(VectorQueryCursor::from_hit(
            selected.last().expect("a nonempty page has a last hit"),
        )))
    });
    let hits = selected
        .into_iter()
        .map(|hit| EngineQueryHit {
            object_path: Some(hit.document.path),
            object_version: hit.document.version,
            score: Some(hit.score),
            fields_json: Vec::new(),
        })
        .collect();
    Ok(EngineQueryPage { hits, next })
}

fn hybrid_page(
    hits: Vec<anvil_index::hybrid::HybridHit>,
    page_size: usize,
) -> Result<EngineQueryPage, IndexError> {
    let has_more = hits.len() > page_size;
    let selected = hits.into_iter().take(page_size).collect::<Vec<_>>();
    let next = has_more.then(|| {
        IndexQueryPosition::after(IndexQueryCursor::Hybrid(HybridQueryCursor::from_hit(
            selected.last().expect("a nonempty page has a last hit"),
        )))
    });
    let hits = selected
        .into_iter()
        .map(|hit| {
            Ok(EngineQueryHit {
                object_path: Some(hit.document.path),
                object_version: hit.document.version,
                score: Some(hit.score),
                fields_json: serde_json::to_vec(&serde_json::json!({
                    "text_score": hit.text_score,
                    "vector_score": hit.vector_score,
                }))
                .map_err(|error| IndexError::Encode(error.to_string()))?,
            })
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    Ok(EngineQueryPage { hits, next })
}

async fn query_path<D: IndexDirectoryRead>(
    segments: &[D],
    query: &anvil_api::v1::PathIndexQuery,
    page_size: usize,
    position: IndexQueryPosition,
) -> Result<EngineQueryPage, IndexError> {
    let after = match position.cursor.as_ref() {
        None => query.start_after.as_deref(),
        Some(IndexQueryCursor::Path { after_path }) => Some(after_path.as_str()),
        Some(_) => return Err(cursor_kind_mismatch()),
    };
    let documents = PathEngine::query(
        segments,
        PathQuery {
            prefix: &query.prefix,
            after_path: after,
            limit: page_limit(page_size)?,
        },
    )
    .await?;
    let has_more = documents.len() > page_size;
    let hits = documents
        .into_iter()
        .take(page_size)
        .map(|document| EngineQueryHit {
            object_path: Some(document.path),
            object_version: document.version,
            score: None,
            fields_json: Vec::new(),
        })
        .collect::<Vec<_>>();
    let next = has_more.then(|| {
        IndexQueryPosition::after(IndexQueryCursor::Path {
            after_path: hits
                .last()
                .and_then(|hit| hit.object_path.clone())
                .expect("a nonempty page has a last path"),
        })
    });
    Ok(EngineQueryPage { hits, next })
}

async fn query_git<D: IndexDirectoryRead>(
    segments: &[D],
    repository_id: &str,
    query: &anvil_api::v1::GitSourceIndexQuery,
    page_size: usize,
    position: IndexQueryPosition,
) -> Result<EngineQueryPage, IndexError> {
    let after_path = match position.cursor.as_ref() {
        None => None,
        Some(IndexQueryCursor::Git { after_path }) if query.prefix => Some(after_path.as_str()),
        Some(_) => return Err(cursor_kind_mismatch()),
    };
    let records = if query.prefix {
        GitSourceEngine::list_tree(
            segments,
            repository_id,
            &query.commit_id,
            &query.tree_path,
            after_path,
            page_limit(page_size)?,
        )
        .await?
    } else {
        GitSourceEngine::get_by_path(segments, repository_id, &query.commit_id, &query.tree_path)
            .await?
            .into_iter()
            .collect()
    };
    let has_more = records.len() > page_size;
    let selected = records.into_iter().take(page_size).collect::<Vec<_>>();
    let next = has_more.then(|| {
        IndexQueryPosition::after(IndexQueryCursor::Git {
            after_path: selected
                .last()
                .expect("a nonempty page has a last record")
                .tree_path
                .clone(),
        })
    });
    let hits = selected
        .into_iter()
        .map(|record| {
            let fields_json = serde_json::to_vec(&record)
                .map_err(|error| IndexError::Encode(error.to_string()))?;
            Ok(EngineQueryHit {
                object_path: Some(record.pack_path),
                object_version: record.pack_version,
                score: None,
                fields_json,
            })
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    Ok(EngineQueryPage { hits, next })
}

async fn query_tensor<D: IndexDirectoryRead>(
    segments: &[D],
    model_id: &str,
    tensor_name: &str,
    position: IndexQueryPosition,
) -> Result<EngineQueryPage, IndexError> {
    if position.cursor.is_some() {
        return Err(cursor_kind_mismatch());
    }
    let hits = TensorProjectionEngine::get(segments, model_id, tensor_name)
        .await?
        .into_iter()
        .map(|record| {
            let object_path = record.source_path.clone();
            let object_version = record.source_version;
            let fields_json = serde_json::to_vec(&record)
                .map_err(|error| IndexError::Encode(error.to_string()))?;
            Ok(EngineQueryHit {
                object_path: Some(object_path),
                object_version,
                score: None,
                fields_json,
            })
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    Ok(EngineQueryPage { hits, next: None })
}

fn typed_page(
    hits: Vec<anvil_index::typed_json::TypedHit>,
    order: &[TypedOrder],
    page_size: usize,
) -> Result<EngineQueryPage, IndexError> {
    let has_more = hits.len() > page_size;
    let selected = hits.into_iter().take(page_size).collect::<Vec<_>>();
    let next = has_more.then(|| {
        IndexQueryPosition::after(IndexQueryCursor::Typed(TypedQueryCursor::from_hit(
            selected.last().expect("a nonempty page has a last hit"),
            order,
        )))
    });
    let hits = selected
        .into_iter()
        .map(|hit| {
            let fields_json = serde_json::to_vec(&hit.fields)
                .map_err(|error| IndexError::Encode(error.to_string()))?;
            Ok(EngineQueryHit {
                object_path: Some(hit.document.path),
                object_version: hit.document.version,
                score: None,
                fields_json,
            })
        })
        .collect::<Result<Vec<_>, IndexError>>()?;
    Ok(EngineQueryPage { hits, next })
}

fn typed_cursor(position: &IndexQueryPosition) -> Result<Option<&TypedQueryCursor>, IndexError> {
    match position.cursor.as_ref() {
        None => Ok(None),
        Some(IndexQueryCursor::Typed(cursor)) => Ok(Some(cursor)),
        Some(_) => Err(cursor_kind_mismatch()),
    }
}

fn full_text_cursor(
    position: &IndexQueryPosition,
) -> Result<Option<&FullTextQueryCursor>, IndexError> {
    match position.cursor.as_ref() {
        None => Ok(None),
        Some(IndexQueryCursor::FullText(cursor)) => Ok(Some(cursor)),
        Some(_) => Err(cursor_kind_mismatch()),
    }
}

fn vector_cursor(position: &IndexQueryPosition) -> Result<Option<&VectorQueryCursor>, IndexError> {
    match position.cursor.as_ref() {
        None => Ok(None),
        Some(IndexQueryCursor::Vector(cursor)) => Ok(Some(cursor)),
        Some(_) => Err(cursor_kind_mismatch()),
    }
}

fn hybrid_cursor(position: &IndexQueryPosition) -> Result<Option<&HybridQueryCursor>, IndexError> {
    match position.cursor.as_ref() {
        None => Ok(None),
        Some(IndexQueryCursor::Hybrid(cursor)) => Ok(Some(cursor)),
        Some(_) => Err(cursor_kind_mismatch()),
    }
}

fn cursor_kind_mismatch() -> IndexError {
    IndexError::InvalidQuery("index page position does not match the query kind".into())
}

fn typed_query(
    predicates: &[IndexPredicate],
    order: &[anvil_api::v1::IndexOrder],
    limit: usize,
) -> Result<TypedQuery, IndexError> {
    Ok(TypedQuery {
        predicates: predicates
            .iter()
            .map(typed_predicate)
            .collect::<Result<_, _>>()?,
        order: order
            .iter()
            .map(|order| {
                let direction = IndexOrderDirection::try_from(order.direction)
                    .map_err(|_| IndexError::InvalidQuery("unknown order direction".into()))?;
                Ok(TypedOrder {
                    field: order.field.clone(),
                    descending: direction == IndexOrderDirection::Descending,
                })
            })
            .collect::<Result<_, IndexError>>()?,
        limit,
    })
}

fn typed_predicate(predicate: &IndexPredicate) -> Result<TypedPredicate, IndexError> {
    let operator = IndexPredicateOperator::try_from(predicate.operator)
        .map_err(|_| IndexError::InvalidQuery("unknown predicate operator".into()))?;
    let values = predicate
        .values_json
        .iter()
        .map(|encoded| {
            let value: serde_json::Value = serde_json::from_slice(encoded)
                .map_err(|_| IndexError::InvalidQuery("predicate value is invalid JSON".into()))?;
            ScalarValue::from_json(&value).ok_or_else(|| {
                IndexError::InvalidQuery("predicate value must be a JSON scalar".into())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let one = || {
        if values.len() == 1 {
            Ok(values[0].clone())
        } else {
            Err(IndexError::InvalidQuery(
                "predicate operator requires exactly one value".into(),
            ))
        }
    };
    Ok(match operator {
        IndexPredicateOperator::Equal => TypedPredicate::Equal {
            field: predicate.field.clone(),
            value: one()?,
        },
        IndexPredicateOperator::In if !values.is_empty() => TypedPredicate::In {
            field: predicate.field.clone(),
            values,
        },
        IndexPredicateOperator::Prefix => TypedPredicate::Prefix {
            field: predicate.field.clone(),
            prefix: match one()? {
                ScalarValue::String(value) => value,
                _ => {
                    return Err(IndexError::InvalidQuery(
                        "prefix predicate requires a JSON string".into(),
                    ));
                }
            },
        },
        IndexPredicateOperator::LessThan => TypedPredicate::LessThan {
            field: predicate.field.clone(),
            value: one()?,
        },
        IndexPredicateOperator::LessThanOrEqual => TypedPredicate::LessThanOrEqual {
            field: predicate.field.clone(),
            value: one()?,
        },
        IndexPredicateOperator::GreaterThan => TypedPredicate::GreaterThan {
            field: predicate.field.clone(),
            value: one()?,
        },
        IndexPredicateOperator::GreaterThanOrEqual => TypedPredicate::GreaterThanOrEqual {
            field: predicate.field.clone(),
            value: one()?,
        },
        IndexPredicateOperator::Exists if values.is_empty() => TypedPredicate::Exists {
            field: predicate.field.clone(),
        },
        _ => {
            return Err(IndexError::InvalidQuery(
                "predicate value count does not match its operator".into(),
            ));
        }
    })
}

fn typed_definition(fields: &[anvil_api::v1::IndexField]) -> TypedJsonDefinition {
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

fn metadata_definition(fields: &[String]) -> TypedJsonDefinition {
    TypedJsonDefinition {
        fields: fields
            .iter()
            .map(|name| TypedField {
                name: name.clone(),
                json_pointer: format!("/{}", name.replace('~', "~0").replace('/', "~1")),
            })
            .collect(),
    }
}

fn vector_definition(
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

fn normalize_query_vector(
    mut values: Vec<f32>,
    dimension: usize,
    normalize: bool,
) -> Result<Vec<f32>, IndexError> {
    if values.len() != dimension || values.iter().any(|value| !value.is_finite()) {
        return Err(IndexError::InvalidQuery(
            "query vector has the wrong dimension or a non-finite value".into(),
        ));
    }
    if normalize {
        let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
        if norm == 0.0 || !norm.is_finite() {
            return Err(IndexError::InvalidQuery(
                "query vector cannot be normalized".into(),
            ));
        }
        for value in &mut values {
            *value /= norm;
        }
    }
    Ok(values)
}

fn page_limit(page_size: usize) -> Result<usize, IndexError> {
    page_size.checked_add(1).ok_or(IndexError::OffsetOverflow)
}

fn effective_weight(value: f32) -> f32 {
    if value == 0.0 { 1.0 } else { value }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use anvil_api::v1::{
        PathIndexQuery, PathIndexSpec, TensorIndexQuery, TensorIndexSpec, VectorIndexQuery,
        VectorIndexSpec, index_query, index_specification,
    };
    use anvil_index::ordered::{PathDocument, PathSegmentBuilder};
    use anvil_index::projections::{TensorDocument, TensorRecord, TensorSegmentBuilder};
    use anvil_index::vector::{VectorDocument, VectorSegmentBuilder};
    use anvil_index::{
        BlockDescriptor, DocumentRef, GeneratedBlock, IndexBlockSink, IndexMutation,
        SegmentBuildOptions,
    };

    use super::*;

    #[derive(Clone)]
    struct MemoryFile(Arc<[u8]>);

    impl anvil_index::IndexFileRead for MemoryFile {
        type Slice = Arc<[u8]>;

        async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
            let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
            if start >= self.0.len() || max_length == 0 {
                return Ok(Arc::from([]));
            }
            let end = start.saturating_add(max_length).min(self.0.len());
            Ok(Arc::from(&self.0[start..end]))
        }
    }

    #[derive(Clone)]
    struct MemoryDirectory {
        root: MemoryFile,
        blocks: Arc<BTreeMap<[u8; 32], MemoryFile>>,
    }

    impl MemoryDirectory {
        fn from_sealed_run(sink: MemoryBlockSink, root: GeneratedBlock) -> Self {
            let (_, root) = root.into_parts();
            Self {
                root: MemoryFile(root.into()),
                blocks: Arc::new(
                    sink.blocks
                        .into_iter()
                        .map(|(hash, bytes)| (hash, MemoryFile(bytes.into())))
                        .collect(),
                ),
            }
        }
    }

    impl IndexDirectoryRead for MemoryDirectory {
        type File = MemoryFile;

        async fn open_root(&self) -> Result<Self::File, IndexError> {
            Ok(self.root.clone())
        }

        async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
            self.blocks
                .get(&descriptor.hash)
                .cloned()
                .ok_or_else(|| IndexError::FileNotFound(descriptor.logical_name()))
        }
    }

    #[derive(Default)]
    struct MemoryBlockSink {
        blocks: BTreeMap<[u8; 32], Vec<u8>>,
    }

    impl IndexBlockSink for MemoryBlockSink {
        async fn emit(&mut self, block: GeneratedBlock) -> Result<(), IndexError> {
            let (descriptor, bytes) = block.into_parts();
            if let Some(existing) = self.blocks.get(&descriptor.hash) {
                if existing == &bytes {
                    return Ok(());
                }
                return Err(IndexError::Integrity);
            }
            self.blocks.insert(descriptor.hash, bytes);
            Ok(())
        }
    }

    #[tokio::test]
    async fn path_pages_resume_by_exact_last_path() {
        let mut builder = PathSegmentBuilder::new(SegmentBuildOptions::new(4096).unwrap()).unwrap();
        for (path, version) in [("a", 1), ("b", 2)] {
            assert!(matches!(
                builder
                    .try_push(IndexMutation::Upsert(PathDocument {
                        document: DocumentRef {
                            path: path.into(),
                            version,
                        },
                    }))
                    .unwrap(),
                anvil_index::SegmentPush::Accepted
            ));
        }
        let mut sink = MemoryBlockSink::default();
        let run = builder.seal(&mut sink).await.unwrap().unwrap();
        let directory = MemoryDirectory::from_sealed_run(sink, run.into_root());
        let specification = IndexSpecification {
            specification: Some(index_specification::Specification::Path(PathIndexSpec {})),
        };
        let query = IndexQuery {
            query: Some(index_query::Query::Path(PathIndexQuery {
                prefix: String::new(),
                start_after: None,
            })),
        };
        let first = execute_query(
            std::slice::from_ref(&directory),
            &specification,
            &query,
            1,
            IndexQueryPosition::default(),
        )
        .await
        .unwrap();
        assert_eq!(first.hits[0].object_path.as_deref(), Some("a"));
        let encoded = serde_json::to_vec(first.next.as_ref().unwrap()).unwrap();
        let encoded_value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(encoded_value["format"], INDEX_QUERY_POSITION_FORMAT);
        assert!(encoded_value.get("offset").is_none());
        let position = serde_json::from_slice(&encoded).unwrap();
        let second = execute_query(
            std::slice::from_ref(&directory),
            &specification,
            &query,
            1,
            position,
        )
        .await
        .unwrap();
        assert_eq!(second.hits[0].object_path.as_deref(), Some("b"));
    }

    #[test]
    fn format_two_position_rejects_removed_offset_shape() {
        assert!(
            serde_json::from_slice::<IndexQueryPosition>(br#"{"offset":1,"after_path":"a"}"#)
                .is_err()
        );
        let wrong_format = IndexQueryPosition {
            format: 1,
            cursor: None,
        };
        assert!(wrong_format.validate().is_err());
    }

    #[tokio::test]
    async fn vector_pages_resume_by_engine_score_and_document_cursor() {
        let definition = VectorDefinition {
            dimension: 2,
            metric: VectorMetric::DotProduct,
        };
        let mut builder =
            VectorSegmentBuilder::new(definition, SegmentBuildOptions::new(4096).unwrap()).unwrap();
        for (path, values) in [
            ("a", vec![1.0, 0.0]),
            ("b", vec![0.8, 0.2]),
            ("c", vec![0.0, 1.0]),
        ] {
            assert!(matches!(
                builder
                    .try_push(IndexMutation::Upsert(VectorDocument {
                        document: DocumentRef {
                            path: path.into(),
                            version: 1,
                        },
                        values,
                    }))
                    .unwrap(),
                anvil_index::SegmentPush::Accepted
            ));
        }
        let mut sink = MemoryBlockSink::default();
        let run = builder.seal(&mut sink).await.unwrap().unwrap();
        let directory = MemoryDirectory::from_sealed_run(sink, run.into_root());
        let specification = IndexSpecification {
            specification: Some(index_specification::Specification::Vector(
                VectorIndexSpec {
                    json_pointer: "/embedding".into(),
                    dimensions: 2,
                    metric: ApiVectorMetric::Dot as i32,
                    normalize: false,
                },
            )),
        };
        let query = IndexQuery {
            query: Some(index_query::Query::Vector(VectorIndexQuery {
                values: vec![1.0, 0.0],
            })),
        };

        let first = execute_query(
            std::slice::from_ref(&directory),
            &specification,
            &query,
            1,
            IndexQueryPosition::default(),
        )
        .await
        .unwrap();
        assert_eq!(first.hits[0].object_path.as_deref(), Some("a"));
        let second = execute_query(
            std::slice::from_ref(&directory),
            &specification,
            &query,
            1,
            first.next.unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(second.hits[0].object_path.as_deref(), Some("b"));
        let third = execute_query(
            std::slice::from_ref(&directory),
            &specification,
            &query,
            1,
            second.next.unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(third.hits[0].object_path.as_deref(), Some("c"));
        assert!(third.next.is_none());
    }

    #[tokio::test]
    async fn tensor_query_returns_the_referenced_ordinary_object() {
        let mut builder =
            TensorSegmentBuilder::new(SegmentBuildOptions::new(4096).unwrap()).unwrap();
        assert!(matches!(
            builder
                .try_push(IndexMutation::Upsert(TensorDocument {
                    document: DocumentRef {
                        path: "tensor-manifest.json".into(),
                        version: 1,
                    },
                    records: vec![TensorRecord {
                        model_id: "model-a".into(),
                        tensor_name: "encoder.weight".into(),
                        source_path: "weights/model-a.bin".into(),
                        source_version: 9,
                        offset: 128,
                        length: 256,
                        dtype: "f32".into(),
                        shape: vec![8, 8],
                    }],
                }))
                .unwrap(),
            anvil_index::SegmentPush::Accepted
        ));
        let mut sink = MemoryBlockSink::default();
        let run = builder.seal(&mut sink).await.unwrap().unwrap();
        let directory = MemoryDirectory::from_sealed_run(sink, run.into_root());
        let specification = IndexSpecification {
            specification: Some(index_specification::Specification::Tensor(
                TensorIndexSpec {
                    model_id: "model-a".into(),
                },
            )),
        };
        let query = IndexQuery {
            query: Some(index_query::Query::Tensor(TensorIndexQuery {
                tensor_name: "encoder.weight".into(),
            })),
        };
        let page = execute_query(
            std::slice::from_ref(&directory),
            &specification,
            &query,
            10,
            IndexQueryPosition::default(),
        )
        .await
        .unwrap();
        assert_eq!(page.hits.len(), 1);
        assert_eq!(
            page.hits[0].object_path.as_deref(),
            Some("weights/model-a.bin")
        );
        assert_eq!(page.hits[0].object_version, 9);
    }
}
