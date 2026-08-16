use std::future::Future;
use std::pin::Pin;

use crate::IndexError;

use super::super::{
    Analyzer, ArtifactDirectoryRead, ComponentKind, ComponentStream, FIELD_PRESENCE_TERM,
    FieldCapabilities, FieldComponents, FieldId, FieldType, IndexKind, IndexSemantics, NativeQuery,
    NativeQueryStatisticsRecorder, PostingReference, Predicate, RangeBound, ScalarValue, Schema,
    SegmentDescriptor, TERM_TYPE_FIELD_PRESENCE, TERM_TYPE_STRING, TERM_TYPE_TEXT, TermDictionary,
    analyze_unicode_alphanumeric_lowercase, canonical_term_key, read_artifact_component,
    scalar_term, text_term,
};
use super::posting::{
    DocCursor, DocValuePresenceStream, PointBounds, PointRangeStream, PostingStream, TermBounds,
    TermRangeStream, component_root,
};

pub(super) struct SegmentPlan<'a, D> {
    pub cursor: DocCursor<'a, D>,
    pub text_terms: Vec<TextTermPlan>,
}

#[derive(Clone)]
pub(super) struct TextTermPlan {
    pub field_id: FieldId,
    pub token_ordinal: u32,
    pub postings: PostingReference,
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
    let (cursor, text_terms) = match query {
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
                (cursor, Vec::new())
            } else {
                (DocCursor::all(segment.document_count), Vec::new())
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
            (DocCursor::all(segment.document_count), Vec::new())
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
                (DocCursor::all(segment.document_count), Vec::new())
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
                Vec::new(),
            )
        }
    };
    Ok(SegmentPlan { cursor, text_terms })
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
            } => {
                exact_value_cursor(directory, segment, schema, *field_id, value, &statistics)
                    .await?
            }
            Predicate::In {
                field_id, values, ..
            } => {
                if values.len() > maximum_expanded_terms {
                    return Err(IndexError::ResourceLimit {
                        needed: values.len(),
                        limit: maximum_expanded_terms,
                    });
                }
                let mut children = Vec::with_capacity(values.len());
                for value in values {
                    children.push(
                        exact_value_cursor(
                            directory,
                            segment,
                            schema,
                            *field_id,
                            value,
                            &statistics,
                        )
                        .await?,
                    );
                }
                DocCursor::or(children, statistics.clone())
            }
            Predicate::Prefix {
                field_id, prefix, ..
            } => {
                let field = field(schema, *field_id, FieldComponents::TERMS)?;
                if !field.capabilities.contains(FieldCapabilities::PREFIX) {
                    return Err(IndexError::InvalidQuery(
                        "prefix predicate requires PREFIX".into(),
                    ));
                }
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
                let field_schema = schema
                    .fields
                    .get(field_id.get() as usize)
                    .ok_or_else(|| IndexError::InvalidQuery("unknown range field".into()))?;
                if field_schema.components.contains(FieldComponents::POINTS) {
                    DocCursor::PointRange(PointRangeStream::new(
                        directory,
                        segment,
                        *field_id,
                        PointBounds::new(lower.clone(), upper.clone())?,
                        statistics.clone(),
                    )?)
                } else {
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
            }
            Predicate::Exists { field_id, .. } => {
                let field = schema
                    .fields
                    .get(field_id.get() as usize)
                    .ok_or_else(|| IndexError::InvalidQuery("unknown EXISTS field".into()))?;
                if field.components.contains(FieldComponents::TERMS) {
                    exact_term_cursor(
                        directory,
                        segment,
                        *field_id,
                        TERM_TYPE_FIELD_PRESENCE,
                        FIELD_PRESENCE_TERM,
                        &statistics,
                    )
                    .await?
                } else if field.components.contains(FieldComponents::POINTS) {
                    DocCursor::PointRange(PointRangeStream::new(
                        directory,
                        segment,
                        *field_id,
                        PointBounds::presence(),
                        statistics.clone(),
                    )?)
                } else if field.components.contains(FieldComponents::DOC_VALUES) {
                    DocCursor::DocValuePresence(DocValuePresenceStream::new(
                        directory,
                        segment,
                        *field_id,
                        statistics.clone(),
                    )?)
                } else {
                    return Err(IndexError::InvalidQuery(
                        "EXISTS field has no presence-capable component".into(),
                    ));
                }
            }
            Predicate::FullText { field_id, text, .. }
            | Predicate::Phrase { field_id, text, .. } => {
                plan_text_field(
                    directory,
                    segment,
                    schema,
                    *field_id,
                    text,
                    matches!(predicate, Predicate::Phrase { .. }),
                    maximum_expanded_terms,
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
) -> Result<(DocCursor<'a, D>, Vec<TextTermPlan>), IndexError> {
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
    if phrase {
        let mut fields = Vec::new();
        for (field, terms) in text_fields.iter().zip(per_field) {
            if let Some(terms) = terms.into_iter().collect::<Option<Vec<_>>>() {
                fields.push(DocCursor::phrase(
                    directory,
                    segment,
                    field.id,
                    terms,
                    statistics.clone(),
                )?);
            }
        }
        return Ok((DocCursor::or(fields, statistics.clone()), text_terms));
    }
    Ok((
        DocCursor::and(token_cursors, statistics.clone()),
        text_terms,
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

async fn exact_value_cursor<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    schema: &Schema,
    field_id: FieldId,
    value: &ScalarValue,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<DocCursor<'a, D>, IndexError> {
    let field = schema
        .fields
        .get(field_id.get() as usize)
        .ok_or_else(|| IndexError::InvalidQuery("unknown exact field".into()))?;
    if field.components.contains(FieldComponents::POINTS) {
        if value == &ScalarValue::Null {
            return Ok(DocCursor::PointRange(PointRangeStream::new(
                directory,
                segment,
                field_id,
                PointBounds::null(),
                statistics.clone(),
            )?));
        }
        let bound = RangeBound {
            value: value.clone(),
            inclusive: true,
        };
        return Ok(DocCursor::PointRange(PointRangeStream::new(
            directory,
            segment,
            field_id,
            PointBounds::new(Some(bound.clone()), Some(bound))?,
            statistics.clone(),
        )?));
    }
    if !field.components.contains(FieldComponents::TERMS) {
        return Err(IndexError::InvalidQuery(
            "EXACT field has no exact access component".into(),
        ));
    }
    exact_scalar_cursor(directory, segment, field_id, value, statistics).await
}

#[allow(clippy::too_many_arguments)]
async fn plan_text_field<'a, D: ArtifactDirectoryRead>(
    directory: &'a D,
    segment: &'a SegmentDescriptor,
    schema: &Schema,
    field_id: FieldId,
    text: &str,
    phrase: bool,
    maximum_expanded_terms: usize,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<DocCursor<'a, D>, IndexError> {
    let field = schema
        .fields
        .get(field_id.get() as usize)
        .ok_or_else(|| IndexError::InvalidQuery("unknown full-text field".into()))?;
    if field.field_type != FieldType::Text
        || !field.capabilities.contains(FieldCapabilities::FULL_TEXT)
    {
        return Err(IndexError::InvalidQuery(
            "full-text predicate requires FULL_TEXT".into(),
        ));
    }
    let analyzer = field
        .analyzer
        .ok_or_else(|| IndexError::InvalidDefinition("full-text field lacks an analyzer".into()))?;
    let tokens = analyze(analyzer, text, maximum_expanded_terms)?;
    if tokens.is_empty() {
        return Err(IndexError::InvalidQuery(
            "full-text predicate contains no indexable terms".into(),
        ));
    }
    let mut cursors = Vec::with_capacity(tokens.len());
    let mut references = Vec::with_capacity(tokens.len());
    for token in &tokens {
        let (_, term) = text_term(token)?;
        let Some(resolved) = resolve_exact(
            directory,
            segment,
            field_id,
            TERM_TYPE_TEXT,
            &term,
            statistics,
        )
        .await?
        else {
            return Ok(DocCursor::Empty);
        };
        cursors.push(DocCursor::Posting(PostingStream::new(
            directory,
            segment,
            field_id,
            resolved.postings,
            statistics.clone(),
        )?));
        references.push(resolved.postings);
    }
    if phrase {
        return DocCursor::phrase(directory, segment, field_id, references, statistics.clone());
    }
    Ok(DocCursor::and(cursors, statistics.clone()))
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
        &segment.packs,
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
            &segment.packs,
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
