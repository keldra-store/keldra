use std::future::Future;
use std::pin::Pin;

use crate::IndexError;

use super::super::{
    Analyzer, ArtifactDirectoryRead, ComponentKind, ComponentStream, FIELD_PRESENCE_TERM,
    FieldComponents, FieldId, IndexKind, IndexSemantics, NativeQuery,
    NativeQueryStatisticsRecorder, PostingReference, Predicate, RangeBound, ScalarValue, Schema,
    SegmentDescriptor, TERM_TYPE_FIELD_PRESENCE, TERM_TYPE_STRING, TERM_TYPE_TEXT, TermDictionary,
    analyze_unicode_alphanumeric_lowercase, canonical_term_key, read_artifact_component,
    scalar_term, text_term,
};
use super::posting::{DocCursor, PostingStream, TermBounds, TermRangeStream, component_root};

pub(super) struct SegmentPlan<'a, D> {
    pub cursor: DocCursor<'a, D>,
    pub exact_filter: Option<&'a Predicate>,
    pub text_terms: Vec<TextTermPlan>,
    pub phrase_fields: Vec<PhraseFieldPlan>,
}

#[derive(Clone)]
pub(super) struct TextTermPlan {
    pub field_id: FieldId,
    pub token_ordinal: u32,
    pub postings: PostingReference,
}

#[derive(Clone)]
pub(super) struct PhraseFieldPlan {
    pub field_id: FieldId,
    pub terms: Vec<PostingReference>,
}

#[derive(Clone)]
struct ResolvedTerm {
    postings: PostingReference,
}

pub(super) async fn plan_segment<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    schema: &'a Schema,
    query: &'a NativeQuery,
    maximum_expanded_terms: usize,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<SegmentPlan<'a, D>, IndexError> {
    if maximum_expanded_terms == 0 {
        return Err(IndexError::InvalidDefinition(
            "query term expansion limit must be non-zero".into(),
        ));
    }
    let (cursor, exact_filter, text_terms, phrase_fields) = match query {
        NativeQuery::Path { prefix, .. } => {
            require_kind(schema, IndexKind::Path)?;
            let field = field(schema, FieldId::new(0), FieldComponents::TERMS)?;
            (
                prefix_cursor(
                    directory,
                    segment,
                    field.id,
                    TERM_TYPE_STRING,
                    string_term_prefix(prefix),
                    statistics,
                )?,
                None,
                Vec::new(),
                Vec::new(),
            )
        }
        NativeQuery::Filter { predicate, .. } => {
            if !matches!(
                schema.kind,
                IndexKind::MetadataFilter | IndexKind::TypedJson
            ) {
                return Err(IndexError::InvalidQuery(
                    "filter query requires a metadata or Typed JSON index".into(),
                ));
            }
            if let Some(predicate) = predicate {
                predicate.validate()?;
                let cursor = plan_predicate(
                    directory,
                    segment,
                    schema,
                    predicate,
                    maximum_expanded_terms,
                    statistics.clone(),
                )
                .await?;
                (cursor, Some(predicate), Vec::new(), Vec::new())
            } else {
                (
                    DocCursor::all(segment.document_count),
                    None,
                    Vec::new(),
                    Vec::new(),
                )
            }
        }
        NativeQuery::FullText { text, phrase } => {
            require_kind(schema, IndexKind::FullText)?;
            let analyzer = match &schema.semantics {
                IndexSemantics::FullText { analyzer, .. } => *analyzer,
                _ => unreachable!("kind validated"),
            };
            plan_text(
                directory,
                segment,
                schema,
                text,
                *phrase,
                analyzer,
                maximum_expanded_terms,
                statistics,
            )
            .await?
        }
        NativeQuery::Vector { values } => {
            require_kind(schema, IndexKind::Vector)?;
            validate_vector(schema, values)?;
            (
                DocCursor::all(segment.document_count),
                None,
                Vec::new(),
                Vec::new(),
            )
        }
        NativeQuery::Hybrid { text, vector } => {
            require_kind(schema, IndexKind::Hybrid)?;
            if !vector.is_empty() {
                validate_vector(schema, vector)?;
            }
            let analyzer = match &schema.semantics {
                IndexSemantics::Hybrid { analyzer, .. } => *analyzer,
                _ => unreachable!("kind validated"),
            };
            if text.trim().is_empty() {
                (
                    DocCursor::all(segment.document_count),
                    None,
                    Vec::new(),
                    Vec::new(),
                )
            } else {
                plan_text(
                    directory,
                    segment,
                    schema,
                    text,
                    false,
                    analyzer,
                    maximum_expanded_terms,
                    statistics,
                )
                .await?
            }
        }
        NativeQuery::GitSource {
            repository_id,
            commit_id,
            tree_path,
            prefix,
        } => {
            require_kind(schema, IndexKind::GitSource)?;
            let repository = exact_scalar_cursor(
                directory,
                segment,
                field(schema, FieldId::new(0), FieldComponents::TERMS)?.id,
                &ScalarValue::String(repository_id.clone()),
                statistics,
            )
            .await?;
            let commit = exact_scalar_cursor(
                directory,
                segment,
                field(schema, FieldId::new(1), FieldComponents::TERMS)?.id,
                &ScalarValue::String(commit_id.clone()),
                statistics,
            )
            .await?;
            let path_field = field(schema, FieldId::new(2), FieldComponents::TERMS)?.id;
            let path = if *prefix {
                prefix_cursor(
                    directory,
                    segment,
                    path_field,
                    TERM_TYPE_STRING,
                    string_term_prefix(tree_path),
                    statistics,
                )?
            } else {
                exact_scalar_cursor(
                    directory,
                    segment,
                    path_field,
                    &ScalarValue::String(tree_path.clone()),
                    statistics,
                )
                .await?
            };
            (
                DocCursor::and(vec![repository, commit, path], statistics.clone()),
                None,
                Vec::new(),
                Vec::new(),
            )
        }
        NativeQuery::Tensor {
            model_id,
            tensor_name,
        } => {
            require_kind(schema, IndexKind::Tensor)?;
            let model = exact_scalar_cursor(
                directory,
                segment,
                field(schema, FieldId::new(0), FieldComponents::TERMS)?.id,
                &ScalarValue::String(model_id.clone()),
                statistics,
            )
            .await?;
            let name = exact_scalar_cursor(
                directory,
                segment,
                field(schema, FieldId::new(1), FieldComponents::TERMS)?.id,
                &ScalarValue::String(tensor_name.clone()),
                statistics,
            )
            .await?;
            (
                DocCursor::and(vec![model, name], statistics.clone()),
                None,
                Vec::new(),
                Vec::new(),
            )
        }
    };
    Ok(SegmentPlan {
        cursor,
        exact_filter,
        text_terms,
        phrase_fields,
    })
}

fn plan_predicate<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    schema: &'a Schema,
    predicate: &'a Predicate,
    maximum_expanded_terms: usize,
    statistics: NativeQueryStatisticsRecorder,
) -> Pin<Box<dyn Future<Output = Result<DocCursor<'a, D>, IndexError>> + Send + 'a>> {
    Box::pin(async move {
        Ok(match predicate {
            Predicate::Equal {
                field_id, value, ..
            } => exact_scalar_cursor(directory, segment, *field_id, value, &statistics).await?,
            Predicate::In {
                field_id, values, ..
            } => {
                field(schema, *field_id, FieldComponents::TERMS)?;
                if values.len() > maximum_expanded_terms {
                    return Err(IndexError::ResourceLimit {
                        needed: values.len(),
                        limit: maximum_expanded_terms,
                    });
                }
                let mut children = Vec::with_capacity(values.len());
                for value in values {
                    children.push(
                        exact_scalar_cursor(directory, segment, *field_id, value, &statistics)
                            .await?,
                    );
                }
                DocCursor::or(children, statistics.clone())
            }
            Predicate::Prefix {
                field_id, prefix, ..
            } => {
                field(schema, *field_id, FieldComponents::TERMS)?;
                prefix_cursor(
                    directory,
                    segment,
                    *field_id,
                    TERM_TYPE_STRING,
                    string_term_prefix(prefix),
                    &statistics,
                )?
            }
            Predicate::Range {
                field_id,
                lower,
                upper,
                ..
            } => {
                field(schema, *field_id, FieldComponents::TERMS)?;
                range_cursor(
                    directory,
                    segment,
                    *field_id,
                    lower.as_ref(),
                    upper.as_ref(),
                    &statistics,
                )?
            }
            Predicate::Exists { field_id, .. } => {
                field(
                    schema,
                    *field_id,
                    FieldComponents::TERMS.union(FieldComponents::FAST_COLUMN),
                )?;
                exact_term_cursor(
                    directory,
                    segment,
                    *field_id,
                    TERM_TYPE_FIELD_PRESENCE,
                    FIELD_PRESENCE_TERM,
                    &statistics,
                )
                .await?
            }
            Predicate::And(children) => {
                let mut planned = Vec::with_capacity(children.len());
                for child in children {
                    planned.push(
                        plan_predicate(
                            directory,
                            segment,
                            schema,
                            child,
                            maximum_expanded_terms,
                            statistics.clone(),
                        )
                        .await?,
                    );
                }
                DocCursor::and(planned, statistics.clone())
            }
            Predicate::Or(children) => {
                if children.len() > maximum_expanded_terms {
                    return Err(IndexError::ResourceLimit {
                        needed: children.len(),
                        limit: maximum_expanded_terms,
                    });
                }
                let mut planned = Vec::with_capacity(children.len());
                for child in children {
                    planned.push(
                        plan_predicate(
                            directory,
                            segment,
                            schema,
                            child,
                            maximum_expanded_terms,
                            statistics.clone(),
                        )
                        .await?,
                    );
                }
                DocCursor::or(planned, statistics.clone())
            }
            Predicate::Not(child) => DocCursor::not(
                DocCursor::all(segment.document_count),
                plan_predicate(
                    directory,
                    segment,
                    schema,
                    child,
                    maximum_expanded_terms,
                    statistics,
                )
                .await?,
            ),
        })
    })
}

#[allow(clippy::too_many_arguments)]
async fn plan_text<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    schema: &'a Schema,
    text: &str,
    phrase: bool,
    analyzer: Analyzer,
    maximum_expanded_terms: usize,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<
    (
        DocCursor<'a, D>,
        Option<&'a Predicate>,
        Vec<TextTermPlan>,
        Vec<PhraseFieldPlan>,
    ),
    IndexError,
> {
    let tokens = analyze(analyzer, text, maximum_expanded_terms)?;
    if tokens.is_empty() {
        return Err(IndexError::InvalidQuery(
            "full-text query contains no indexable terms".into(),
        ));
    }
    let text_fields = schema
        .fields
        .iter()
        .filter(|field| field.components.contains(FieldComponents::POSITIONS))
        .collect::<Vec<_>>();
    if text_fields.is_empty() {
        return Err(IndexError::InvalidDefinition(
            "full-text schema has no positional field".into(),
        ));
    }
    if text_fields.len() > maximum_expanded_terms {
        return Err(IndexError::ResourceLimit {
            needed: text_fields.len(),
            limit: maximum_expanded_terms,
        });
    }
    let mut token_cursors = Vec::with_capacity(tokens.len());
    let mut text_terms = Vec::new();
    let mut per_field = vec![Vec::new(); text_fields.len()];
    for (token_ordinal, token) in tokens.iter().enumerate() {
        let (_, term) = text_term(token)?;
        let mut fields = Vec::new();
        for (field_index, field) in text_fields.iter().enumerate() {
            if let Some(resolved) = resolve_exact(
                directory,
                segment,
                field.id,
                TERM_TYPE_TEXT,
                &term,
                statistics,
            )
            .await?
            {
                fields.push(DocCursor::Posting(PostingStream::new(
                    directory,
                    segment,
                    field.id,
                    resolved.postings,
                    statistics.clone(),
                )?));
                text_terms.push(TextTermPlan {
                    field_id: field.id,
                    token_ordinal: u32::try_from(token_ordinal)
                        .map_err(|_| IndexError::OffsetOverflow)?,
                    postings: resolved.postings,
                });
                per_field[field_index].push(Some(resolved.postings));
            } else {
                per_field[field_index].push(None);
            }
        }
        token_cursors.push(DocCursor::or(fields, statistics.clone()));
    }
    let phrase_fields = if phrase {
        text_fields
            .iter()
            .zip(per_field)
            .filter_map(|(field, terms)| {
                terms
                    .into_iter()
                    .collect::<Option<Vec<_>>>()
                    .map(|terms| PhraseFieldPlan {
                        field_id: field.id,
                        terms,
                    })
            })
            .collect()
    } else {
        Vec::new()
    };
    if phrase && phrase_fields.is_empty() {
        return Ok((DocCursor::Empty, None, text_terms, Vec::new()));
    }
    Ok((
        DocCursor::and(token_cursors, statistics.clone()),
        None,
        text_terms,
        phrase_fields,
    ))
}

async fn exact_scalar_cursor<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    field_id: FieldId,
    value: &ScalarValue,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<DocCursor<'a, D>, IndexError> {
    let (term_type, term) = scalar_term(value)
        .map_err(|_| IndexError::InvalidQuery("predicate scalar is invalid".into()))?;
    exact_term_cursor(directory, segment, field_id, term_type, &term, statistics).await
}

async fn exact_term_cursor<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    field_id: FieldId,
    term_type: u8,
    term: &[u8],
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<DocCursor<'a, D>, IndexError> {
    Ok(
        match resolve_exact(directory, segment, field_id, term_type, term, statistics).await? {
            Some(resolved) => DocCursor::Posting(PostingStream::new(
                directory,
                segment,
                field_id,
                resolved.postings,
                statistics.clone(),
            )?),
            None => DocCursor::Empty,
        },
    )
}

async fn resolve_exact<D: ArtifactDirectoryRead>(
    directory: &D,
    segment: &SegmentDescriptor,
    field_id: FieldId,
    term_type: u8,
    term: &[u8],
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<Option<ResolvedTerm>, IndexError> {
    let key = canonical_term_key(field_id, term_type, term)?;
    Ok(resolve_terms(
        directory,
        segment,
        field_id,
        key.clone(),
        key.clone(),
        1,
        |entry| entry == key.as_slice(),
        statistics,
    )
    .await?
    .pop())
}

fn prefix_cursor<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    field_id: FieldId,
    term_type: u8,
    term_prefix: Vec<u8>,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<DocCursor<'a, D>, IndexError> {
    let prefix = canonical_term_key(field_id, term_type, &term_prefix)?;
    let maximum = prefix_successor(&prefix).ok_or(IndexError::InvalidQuery(
        "canonical term prefix has no finite successor".into(),
    ))?;
    term_range_cursor(
        directory,
        segment,
        field_id,
        TermBounds::new(prefix, true, maximum, false)?,
        statistics,
    )
}

fn range_cursor<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    field_id: FieldId,
    lower: Option<&RangeBound>,
    upper: Option<&RangeBound>,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<DocCursor<'a, D>, IndexError> {
    let reference = lower
        .map(|bound| &bound.value)
        .or_else(|| upper.map(|bound| &bound.value))
        .ok_or_else(|| IndexError::InvalidQuery("range has no bound".into()))?;
    let (term_type, _) = scalar_term(reference)?;
    if lower
        .is_some_and(|bound| scalar_term(&bound.value).ok().map(|value| value.0) != Some(term_type))
        || upper.is_some_and(|bound| {
            scalar_term(&bound.value).ok().map(|value| value.0) != Some(term_type)
        })
    {
        return Err(IndexError::InvalidQuery(
            "range bounds must have one exact scalar type".into(),
        ));
    }
    let mut type_prefix = field_id.get().to_be_bytes().to_vec();
    type_prefix.push(term_type);
    let lower_key = lower
        .map(|bound| {
            scalar_term(&bound.value)
                .and_then(|(_, term)| canonical_term_key(field_id, term_type, &term))
        })
        .transpose()?;
    let upper_key = upper
        .map(|bound| {
            scalar_term(&bound.value)
                .and_then(|(_, term)| canonical_term_key(field_id, term_type, &term))
        })
        .transpose()?;
    let scan_min = lower_key.clone().unwrap_or_else(|| type_prefix.clone());
    let scan_max = upper_key
        .clone()
        .or_else(|| prefix_successor(&type_prefix))
        .ok_or(IndexError::InvalidQuery(
            "canonical scalar type has no finite successor".into(),
        ))?;
    term_range_cursor(
        directory,
        segment,
        field_id,
        TermBounds::new(
            scan_min,
            lower.is_none_or(|bound| bound.inclusive),
            scan_max,
            upper.is_some_and(|bound| bound.inclusive),
        )?,
        statistics,
    )
}

fn term_range_cursor<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    field_id: FieldId,
    bounds: TermBounds,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<DocCursor<'a, D>, IndexError> {
    let Some(root) =
        optional_component_root(segment, ComponentKind::TERM_DICTIONARY, Some(field_id))
    else {
        return Ok(DocCursor::Empty);
    };
    Ok(DocCursor::TermRange(TermRangeStream::new(
        directory,
        segment,
        field_id,
        root,
        bounds,
        statistics.clone(),
    )?))
}

#[allow(clippy::too_many_arguments)]
async fn resolve_terms<D, F>(
    directory: &D,
    segment: &SegmentDescriptor,
    field_id: FieldId,
    minimum: Vec<u8>,
    maximum: Vec<u8>,
    limit: usize,
    mut matches: F,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<Vec<ResolvedTerm>, IndexError>
where
    D: ArtifactDirectoryRead,
    F: FnMut(&[u8]) -> bool,
{
    statistics.term_seek();
    let Some(root) =
        optional_component_root(segment, ComponentKind::TERM_DICTIONARY, Some(field_id))
    else {
        return Ok(Vec::new());
    };
    let mut stream = ComponentStream::new(
        directory,
        segment.identity,
        ComponentKind::TERM_DICTIONARY,
        root,
        Some(minimum),
        Some(maximum),
    )?;
    let mut output = Vec::new();
    while let Some(leaf) = stream.next_leaf().await? {
        let loaded = read_artifact_component(
            directory,
            segment.identity,
            &leaf.descriptor,
            ComponentKind::TERM_DICTIONARY,
        )
        .await?;
        let dictionary = directory
            .run_query_cpu(move || TermDictionary::decode_payload(&loaded.payload))
            .await?;
        for entry in dictionary
            .entries()
            .iter()
            .filter(|entry| matches(&entry.term))
        {
            statistics.enumerated_terms(1);
            if output.len() == limit {
                return Err(IndexError::ResourceLimit {
                    needed: output.len().saturating_add(1),
                    limit,
                });
            }
            output.push(ResolvedTerm {
                postings: entry.postings,
            });
        }
    }
    Ok(output)
}

fn optional_component_root(
    segment: &SegmentDescriptor,
    kind: ComponentKind,
    field_id: Option<FieldId>,
) -> Option<super::super::ArtifactDescriptor> {
    component_root(segment, kind, field_id).ok()
}

fn field(
    schema: &Schema,
    field_id: FieldId,
    component: FieldComponents,
) -> Result<&super::super::FieldSchema, IndexError> {
    schema
        .fields
        .get(field_id.get() as usize)
        .filter(|field| field.id == field_id && field.components.contains(component))
        .ok_or_else(|| IndexError::InvalidQuery("query field lacks its required component".into()))
}

fn require_kind(schema: &Schema, expected: IndexKind) -> Result<(), IndexError> {
    if schema.kind != expected {
        return Err(IndexError::InvalidQuery(
            "query kind does not match its schema".into(),
        ));
    }
    Ok(())
}

fn string_term_prefix(value: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(value.len().saturating_add(1));
    bytes.push(0);
    bytes.extend_from_slice(value.as_bytes());
    bytes
}

fn prefix_successor(value: &[u8]) -> Option<Vec<u8>> {
    let mut successor = value.to_vec();
    for index in (0..successor.len()).rev() {
        if successor[index] != u8::MAX {
            successor[index] += 1;
            successor.truncate(index + 1);
            return Some(successor);
        }
    }
    None
}

fn analyze(analyzer: Analyzer, text: &str, limit: usize) -> Result<Vec<String>, IndexError> {
    let values: Vec<String> = match analyzer {
        Analyzer::Keyword => (!text.is_empty())
            .then(|| text.to_owned())
            .into_iter()
            .collect(),
        Analyzer::UnicodeAlphanumericLowercase => {
            return analyze_unicode_alphanumeric_lowercase(text, limit)
                .map(|tokens| tokens.into_iter().map(|(token, _position)| token).collect());
        }
    };
    if values.len() > limit {
        return Err(IndexError::ResourceLimit {
            needed: values.len(),
            limit,
        });
    }
    Ok(values)
}

fn validate_vector(schema: &Schema, values: &[f32]) -> Result<(), IndexError> {
    let dimensions = match &schema.semantics {
        IndexSemantics::Vector { dimensions, .. } | IndexSemantics::Hybrid { dimensions, .. } => {
            *dimensions
        }
        _ => {
            return Err(IndexError::InvalidQuery(
                "schema has no vector semantics".into(),
            ));
        }
    };
    if values.len() != dimensions as usize || values.iter().any(|value| !value.is_finite()) {
        return Err(IndexError::InvalidQuery(
            "query vector differs from declared dimensions or is not finite".into(),
        ));
    }
    Ok(())
}
