//! Storage-neutral execution of one immutable index generation.

use anvil_api::v1::index_query::Query;
use anvil_api::v1::index_specification::Specification;
use anvil_api::v1::{
    IndexOrderDirection, IndexPredicate, IndexPredicateOperator, IndexQuery, IndexSpecification,
    VectorMetric as ApiVectorMetric,
};
use anvil_index::full_text::{FullTextEngine, FullTextQuery};
use anvil_index::hybrid::{HybridDefinition, HybridEngine, HybridQuery};
use anvil_index::ordered::{PathEngine, PathQuery};
use anvil_index::projections::{GitSourceEngine, TensorProjectionEngine};
use anvil_index::typed_json::{
    MetadataFilterEngine, ScalarValue, TypedField, TypedJsonDefinition, TypedJsonEngine,
    TypedOrder, TypedPredicate, TypedQuery,
};
use anvil_index::vector::{VectorDefinition, VectorEngine, VectorMetric};
use anvil_index::{IndexDirectoryRead, IndexError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexQueryPosition {
    /// Number of globally ordered hits already returned for score/order scans.
    pub offset: u64,
    /// Exact last path for engines with a native ordered seek.
    pub after_path: Option<String>,
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
    directory: &D,
    specification: &IndexSpecification,
    query: &IndexQuery,
    page_size: usize,
    position: IndexQueryPosition,
) -> Result<EngineQueryPage, IndexError> {
    if page_size == 0 {
        return Ok(EngineQueryPage {
            hits: Vec::new(),
            next: None,
        });
    }
    match (specification.specification.as_ref(), query.query.as_ref()) {
        (Some(Specification::Path(_)), Some(Query::Path(query))) => {
            query_path(directory, query, page_size, position).await
        }
        (
            Some(Specification::MetadataFilter(specification)),
            Some(Query::MetadataFilter(query)),
        ) => {
            let definition = metadata_definition(&specification.fields);
            let query = typed_query(
                &query.predicates,
                &[],
                scan_limit(page_size, position.offset)?,
            )?;
            let hits = MetadataFilterEngine::query(directory, &definition, &query).await?;
            typed_page(hits, page_size, position)
        }
        (Some(Specification::TypedJson(specification)), Some(Query::TypedJson(query))) => {
            let definition = typed_definition(&specification.fields);
            let query = typed_query(
                &query.predicates,
                &query.order,
                scan_limit(page_size, position.offset)?,
            )?;
            let hits = TypedJsonEngine::query(directory, &definition, &query).await?;
            typed_page(hits, page_size, position)
        }
        (Some(Specification::FullText(_)), Some(Query::FullText(query))) => {
            let hits = FullTextEngine::query(
                directory,
                FullTextQuery {
                    text: &query.text,
                    fields: &[],
                    phrase: query.phrase,
                    match_all_terms: false,
                    limit: scan_limit(page_size, position.offset)?,
                },
            )
            .await?
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
            offset_page(hits, page_size, position)
        }
        (Some(Specification::Vector(specification)), Some(Query::Vector(query))) => {
            let definition = vector_definition(specification)?;
            let values = normalize_query_vector(
                query.values.clone(),
                definition.dimension,
                specification.normalize,
            )?;
            let hits = VectorEngine::query(
                directory,
                &definition,
                &values,
                scan_limit(page_size, position.offset)?,
            )
            .await?
            .into_iter()
            .map(|hit| EngineQueryHit {
                object_path: Some(hit.document.path),
                object_version: hit.document.version,
                score: Some(hit.score),
                fields_json: Vec::new(),
            })
            .collect();
            offset_page(hits, page_size, position)
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
            let hits = HybridEngine::query(
                directory,
                &definition,
                HybridQuery {
                    text: &query.text,
                    vector: &values,
                    fields: &[],
                    phrase: false,
                    limit: scan_limit(page_size, position.offset)?,
                },
            )
            .await?
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
            offset_page(hits, page_size, position)
        }
        (Some(Specification::GitSource(specification)), Some(Query::GitSource(query))) => {
            query_git(
                directory,
                &specification.repository_id,
                query,
                page_size,
                position,
            )
            .await
        }
        (Some(Specification::Tensor(specification)), Some(Query::Tensor(query))) => {
            query_tensor(
                directory,
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

async fn query_path<D: IndexDirectoryRead>(
    directory: &D,
    query: &anvil_api::v1::PathIndexQuery,
    page_size: usize,
    position: IndexQueryPosition,
) -> Result<EngineQueryPage, IndexError> {
    let after = position
        .after_path
        .as_deref()
        .or(query.start_after.as_deref());
    let documents = PathEngine::query(
        directory,
        PathQuery {
            prefix: &query.prefix,
            after_path: after,
            limit: page_size.saturating_add(1),
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
    let next = has_more.then(|| IndexQueryPosition {
        offset: position.offset.saturating_add(hits.len() as u64),
        after_path: hits.last().and_then(|hit| hit.object_path.clone()),
    });
    Ok(EngineQueryPage { hits, next })
}

async fn query_git<D: IndexDirectoryRead>(
    directory: &D,
    repository_id: &str,
    query: &anvil_api::v1::GitSourceIndexQuery,
    page_size: usize,
    position: IndexQueryPosition,
) -> Result<EngineQueryPage, IndexError> {
    let records = if query.prefix {
        GitSourceEngine::list_tree(
            directory,
            repository_id,
            &query.commit_id,
            &query.tree_path,
            position.after_path.as_deref(),
            page_size.saturating_add(1),
        )
        .await?
    } else {
        GitSourceEngine::get_by_path(directory, repository_id, &query.commit_id, &query.tree_path)
            .await?
            .into_iter()
            .collect()
    };
    let has_more = records.len() > page_size;
    let hits = records
        .into_iter()
        .take(page_size)
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
    let next = has_more.then(|| IndexQueryPosition {
        offset: position.offset.saturating_add(hits.len() as u64),
        after_path: hits
            .last()
            .and_then(|hit| serde_json::from_slice::<serde_json::Value>(&hit.fields_json).ok())
            .and_then(|record| record.get("tree_path")?.as_str().map(str::to_owned)),
    });
    Ok(EngineQueryPage { hits, next })
}

async fn query_tensor<D: IndexDirectoryRead>(
    directory: &D,
    model_id: &str,
    tensor_name: &str,
    position: IndexQueryPosition,
) -> Result<EngineQueryPage, IndexError> {
    if position.offset > 0 {
        return Ok(EngineQueryPage {
            hits: Vec::new(),
            next: None,
        });
    }
    let hits = TensorProjectionEngine::get(directory, model_id, tensor_name)
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
    page_size: usize,
    position: IndexQueryPosition,
) -> Result<EngineQueryPage, IndexError> {
    let hits = hits
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
    offset_page(hits, page_size, position)
}

fn offset_page(
    hits: Vec<EngineQueryHit>,
    page_size: usize,
    position: IndexQueryPosition,
) -> Result<EngineQueryPage, IndexError> {
    let offset = usize::try_from(position.offset).map_err(|_| IndexError::OffsetOverflow)?;
    let has_more = hits.len() > offset.saturating_add(page_size);
    let selected = hits
        .into_iter()
        .skip(offset)
        .take(page_size)
        .collect::<Vec<_>>();
    let next = has_more.then(|| IndexQueryPosition {
        offset: position.offset.saturating_add(selected.len() as u64),
        after_path: None,
    });
    Ok(EngineQueryPage {
        hits: selected,
        next,
    })
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

fn scan_limit(page_size: usize, offset: u64) -> Result<usize, IndexError> {
    usize::try_from(offset)
        .map_err(|_| IndexError::OffsetOverflow)?
        .checked_add(page_size)
        .and_then(|limit| limit.checked_add(1))
        .ok_or(IndexError::OffsetOverflow)
}

fn effective_weight(value: f32) -> f32 {
    if value == 0.0 { 1.0 } else { value }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use anvil_api::v1::{
        PathIndexQuery, PathIndexSpec, TensorIndexQuery, TensorIndexSpec, index_query,
        index_specification,
    };
    use anvil_index::ordered::PathDocument;
    use anvil_index::projections::TensorRecord;

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
    struct MemoryDirectory(Arc<BTreeMap<String, MemoryFile>>);

    impl MemoryDirectory {
        fn new(files: impl IntoIterator<Item = (String, Vec<u8>)>) -> Self {
            Self(Arc::new(
                files
                    .into_iter()
                    .map(|(name, bytes)| (name, MemoryFile(bytes.into())))
                    .collect(),
            ))
        }
    }

    impl IndexDirectoryRead for MemoryDirectory {
        type File = MemoryFile;

        async fn open_file(&self, name: &str) -> Result<Self::File, IndexError> {
            self.0
                .get(name)
                .cloned()
                .ok_or_else(|| IndexError::FileNotFound(name.into()))
        }
    }

    #[tokio::test]
    async fn path_pages_resume_by_exact_last_path() {
        let artifacts = PathEngine::build([
            PathDocument {
                path: "a".into(),
                version: 1,
            },
            PathDocument {
                path: "b".into(),
                version: 2,
            },
        ])
        .unwrap();
        let directory =
            MemoryDirectory::new(artifacts.into_files().map(|file| (file.name, file.bytes)));
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
            &directory,
            &specification,
            &query,
            1,
            IndexQueryPosition::default(),
        )
        .await
        .unwrap();
        assert_eq!(first.hits[0].object_path.as_deref(), Some("a"));
        let second = execute_query(&directory, &specification, &query, 1, first.next.unwrap())
            .await
            .unwrap();
        assert_eq!(second.hits[0].object_path.as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn tensor_query_returns_the_referenced_ordinary_object() {
        let artifacts = TensorProjectionEngine::build([TensorRecord {
            model_id: "model-a".into(),
            tensor_name: "encoder.weight".into(),
            source_path: "weights/model-a.bin".into(),
            source_version: 9,
            offset: 128,
            length: 256,
            dtype: "f32".into(),
            shape: vec![8, 8],
        }])
        .unwrap();
        let directory =
            MemoryDirectory::new(artifacts.into_files().map(|file| (file.name, file.bytes)));
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
            &directory,
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
