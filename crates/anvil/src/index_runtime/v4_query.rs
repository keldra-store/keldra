//! Checked translation from the public protobuf query into the native v4 plan input.

use anvil_api::v1::index_query::Query;
use anvil_api::v1::index_specification::Specification;
use anvil_api::v1::{
    IndexAggregateOperation, IndexAggregateRequest, IndexFacetRequest, IndexOrder,
    IndexOrderDirection, IndexPredicate, IndexPredicateOperator, IndexQuery, IndexSpecification,
};
use anvil_index::IndexError;
use anvil_index::v4::{
    AggregateOperation, AggregateRequest, FacetRequest, FieldCapabilities, FieldId, FieldSchema,
    FieldType, NativeQuery, OrderDirection, OrderField, Predicate, PredicateId, RangeBound,
    ScalarValue, Schema,
};

const MAX_COMPUTATIONS: usize = 32;
const MAX_FACET_BUCKETS: u32 = 1_000;

pub(crate) struct CompiledQuery {
    pub(crate) query: NativeQuery,
    pub(crate) facets: Vec<FacetRequest>,
    pub(crate) aggregates: Vec<AggregateRequest>,
}

/// Compile one public query without retaining protobuf enum values or field
/// names in the execution engine. Repeated public predicates are an exact AND.
pub(crate) fn compile_query(
    schema: &Schema,
    specification: &IndexSpecification,
    query: &IndexQuery,
) -> Result<CompiledQuery, IndexError> {
    schema.validate()?;
    let (query, facets, aggregates) =
        match (specification.specification.as_ref(), query.query.as_ref()) {
            (Some(Specification::Path(_)), Some(Query::Path(query))) => (
                NativeQuery::Path {
                    prefix: query.prefix.clone(),
                    start_after: query.start_after.clone(),
                },
                Vec::new(),
                Vec::new(),
            ),
            (Some(Specification::MetadataFilter(_)), Some(Query::MetadataFilter(query))) => (
                NativeQuery::Filter {
                    predicate: compile_predicates(schema, &query.predicates)?,
                    order: Vec::new(),
                },
                Vec::new(),
                Vec::new(),
            ),
            (Some(Specification::TypedJson(_)), Some(Query::TypedJson(query))) => {
                if query.facets.len().saturating_add(query.aggregates.len()) > MAX_COMPUTATIONS {
                    return Err(IndexError::InvalidQuery(
                        "a query supports at most 32 facet and aggregate computations".into(),
                    ));
                }
                (
                    NativeQuery::Filter {
                        predicate: compile_predicates(schema, &query.predicates)?,
                        order: compile_order(schema, &query.order)?,
                    },
                    compile_facets(schema, &query.facets)?,
                    compile_aggregates(schema, &query.aggregates)?,
                )
            }
            (Some(Specification::FullText(_)), Some(Query::FullText(query))) => {
                if query.text.trim().is_empty() {
                    return Err(IndexError::InvalidQuery(
                        "full-text query must not be empty".into(),
                    ));
                }
                (
                    NativeQuery::FullText {
                        text: query.text.clone(),
                        phrase: query.phrase,
                    },
                    Vec::new(),
                    Vec::new(),
                )
            }
            (Some(Specification::Vector(_)), Some(Query::Vector(query))) => (
                NativeQuery::Vector {
                    values: query.values.clone(),
                },
                Vec::new(),
                Vec::new(),
            ),
            (Some(Specification::Hybrid(_)), Some(Query::Hybrid(query))) => {
                if query.text.trim().is_empty() && query.vector.is_empty() {
                    return Err(IndexError::InvalidQuery(
                        "hybrid query requires text or a vector".into(),
                    ));
                }
                (
                    NativeQuery::Hybrid {
                        text: query.text.clone(),
                        vector: query.vector.clone(),
                    },
                    Vec::new(),
                    Vec::new(),
                )
            }
            (Some(Specification::GitSource(specification)), Some(Query::GitSource(query))) => {
                if query.commit_id.is_empty() || query.tree_path.contains('\0') {
                    return Err(IndexError::InvalidQuery(
                        "Git query requires a commit and a canonical tree path".into(),
                    ));
                }
                (
                    NativeQuery::GitSource {
                        repository_id: specification.repository_id.clone(),
                        commit_id: query.commit_id.clone(),
                        tree_path: query.tree_path.clone(),
                        prefix: query.prefix,
                    },
                    Vec::new(),
                    Vec::new(),
                )
            }
            (Some(Specification::Tensor(specification)), Some(Query::Tensor(query))) => {
                if query.tensor_name.is_empty() {
                    return Err(IndexError::InvalidQuery(
                        "tensor query requires a tensor name".into(),
                    ));
                }
                (
                    NativeQuery::Tensor {
                        model_id: specification.model_id.clone(),
                        tensor_name: query.tensor_name.clone(),
                    },
                    Vec::new(),
                    Vec::new(),
                )
            }
            (Some(_), Some(_)) => {
                return Err(IndexError::InvalidQuery(
                    "query kind does not match index kind".into(),
                ));
            }
            (None, _) => {
                return Err(IndexError::InvalidDefinition(
                    "index specification is required".into(),
                ));
            }
            (_, None) => {
                return Err(IndexError::InvalidQuery("index query is required".into()));
            }
        };
    Ok(CompiledQuery {
        query,
        facets,
        aggregates,
    })
}

fn compile_facets(
    schema: &Schema,
    requests: &[IndexFacetRequest],
) -> Result<Vec<FacetRequest>, IndexError> {
    requests
        .iter()
        .map(|request| {
            if !(1..=MAX_FACET_BUCKETS).contains(&request.limit) {
                return Err(IndexError::InvalidQuery(
                    "facet limit must be in 1..=1000".into(),
                ));
            }
            let field = field(schema, &request.field)?;
            if !field.capabilities.contains(FieldCapabilities::FACET) {
                return Err(IndexError::InvalidQuery(format!(
                    "field {:?} does not declare FACET",
                    field.name
                )));
            }
            Ok(FacetRequest {
                field_id: field.id,
                limit: request.limit,
            })
        })
        .collect()
}

fn compile_aggregates(
    schema: &Schema,
    requests: &[IndexAggregateRequest],
) -> Result<Vec<AggregateRequest>, IndexError> {
    requests
        .iter()
        .map(|request| {
            let field = field(schema, &request.field)?;
            if !field.capabilities.contains(FieldCapabilities::AGGREGATE) {
                return Err(IndexError::InvalidQuery(format!(
                    "field {:?} does not declare AGGREGATE",
                    field.name
                )));
            }
            let operation = match IndexAggregateOperation::try_from(request.operation)
                .map_err(|_| IndexError::InvalidQuery("unknown aggregate operation".into()))?
            {
                IndexAggregateOperation::Count => AggregateOperation::Count,
                IndexAggregateOperation::Minimum => AggregateOperation::Minimum,
                IndexAggregateOperation::Maximum => AggregateOperation::Maximum,
                IndexAggregateOperation::Sum => AggregateOperation::Sum,
                IndexAggregateOperation::Average => AggregateOperation::Average,
            };
            Ok(AggregateRequest {
                field_id: field.id,
                operation,
            })
        })
        .collect()
}

fn compile_predicates(
    schema: &Schema,
    predicates: &[IndexPredicate],
) -> Result<Option<Predicate>, IndexError> {
    let mut compiled = Vec::with_capacity(predicates.len());
    for (ordinal, predicate) in predicates.iter().enumerate() {
        let id = PredicateId::new(
            u32::try_from(ordinal)
                .map_err(|_| IndexError::InvalidQuery("too many predicates".into()))?,
        );
        compiled.push(compile_predicate(schema, predicate, id)?);
    }
    Ok(match compiled.len() {
        0 => None,
        1 => compiled.pop(),
        _ => Some(Predicate::And(compiled)),
    })
}

fn compile_predicate(
    schema: &Schema,
    predicate: &IndexPredicate,
    id: PredicateId,
) -> Result<Predicate, IndexError> {
    let field = field(schema, &predicate.field)?;
    let field_id = field.id;
    let operator = IndexPredicateOperator::try_from(predicate.operator)
        .map_err(|_| IndexError::InvalidQuery("unknown predicate operator".into()))?;
    let values = predicate
        .values_json
        .iter()
        .map(|encoded| {
            if matches!(
                operator,
                IndexPredicateOperator::FullText | IndexPredicateOperator::Phrase
            ) {
                decode_text(encoded, field)
            } else {
                decode_scalar(encoded, field)
            }
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
    let required = match operator {
        IndexPredicateOperator::Equal | IndexPredicateOperator::In => FieldCapabilities::EXACT,
        IndexPredicateOperator::Prefix => FieldCapabilities::PREFIX,
        IndexPredicateOperator::LessThan
        | IndexPredicateOperator::LessThanOrEqual
        | IndexPredicateOperator::GreaterThan
        | IndexPredicateOperator::GreaterThanOrEqual => FieldCapabilities::RANGE,
        IndexPredicateOperator::FullText | IndexPredicateOperator::Phrase => {
            FieldCapabilities::FULL_TEXT
        }
        IndexPredicateOperator::Exists => field.capabilities,
        IndexPredicateOperator::Unspecified => {
            return Err(IndexError::InvalidQuery(
                "predicate operator is unspecified".into(),
            ));
        }
    };
    if !field.capabilities.contains(required) {
        return Err(IndexError::InvalidQuery(format!(
            "field {:?} does not declare the capability required by its predicate",
            field.name
        )));
    }
    Ok(match operator {
        IndexPredicateOperator::Equal => Predicate::Equal {
            id,
            field_id,
            value: one()?,
        },
        IndexPredicateOperator::In if !values.is_empty() => Predicate::In {
            id,
            field_id,
            values,
        },
        IndexPredicateOperator::Prefix => Predicate::Prefix {
            id,
            field_id,
            prefix: match one()? {
                ScalarValue::String(value) => value,
                _ => {
                    return Err(IndexError::InvalidQuery(
                        "prefix predicate requires a JSON string".into(),
                    ));
                }
            },
        },
        IndexPredicateOperator::LessThan => Predicate::Range {
            id,
            field_id,
            lower: None,
            upper: Some(RangeBound {
                value: one()?,
                inclusive: false,
            }),
        },
        IndexPredicateOperator::LessThanOrEqual => Predicate::Range {
            id,
            field_id,
            lower: None,
            upper: Some(RangeBound {
                value: one()?,
                inclusive: true,
            }),
        },
        IndexPredicateOperator::GreaterThan => Predicate::Range {
            id,
            field_id,
            lower: Some(RangeBound {
                value: one()?,
                inclusive: false,
            }),
            upper: None,
        },
        IndexPredicateOperator::GreaterThanOrEqual => Predicate::Range {
            id,
            field_id,
            lower: Some(RangeBound {
                value: one()?,
                inclusive: true,
            }),
            upper: None,
        },
        IndexPredicateOperator::Exists if values.is_empty() => Predicate::Exists { id, field_id },
        IndexPredicateOperator::FullText => Predicate::FullText {
            id,
            field_id,
            text: text_value(one()?)?,
        },
        IndexPredicateOperator::Phrase => Predicate::Phrase {
            id,
            field_id,
            text: text_value(one()?)?,
        },
        _ => {
            return Err(IndexError::InvalidQuery(
                "predicate value count does not match its operator".into(),
            ));
        }
    })
}

fn compile_order(schema: &Schema, order: &[IndexOrder]) -> Result<Vec<OrderField>, IndexError> {
    order
        .iter()
        .map(|order| {
            let direction = match IndexOrderDirection::try_from(order.direction)
                .map_err(|_| IndexError::InvalidQuery("unknown order direction".into()))?
            {
                IndexOrderDirection::Ascending => OrderDirection::Ascending,
                IndexOrderDirection::Descending => OrderDirection::Descending,
            };
            let field = field(schema, &order.field)?;
            if !field.capabilities.contains(FieldCapabilities::ORDER) {
                return Err(IndexError::InvalidQuery(format!(
                    "field {:?} does not declare ORDER",
                    field.name
                )));
            }
            Ok(OrderField {
                field_id: field.id,
                direction,
            })
        })
        .collect()
}

fn field_id(schema: &Schema, name: &str) -> Result<FieldId, IndexError> {
    Ok(field(schema, name)?.id)
}

fn field<'a>(schema: &'a Schema, name: &str) -> Result<&'a FieldSchema, IndexError> {
    schema
        .fields
        .iter()
        .find(|field| field.name == name)
        .ok_or_else(|| IndexError::InvalidQuery(format!("query names unknown field {name:?}")))
}

fn decode_scalar(encoded: &[u8], field: &FieldSchema) -> Result<ScalarValue, IndexError> {
    let value: serde_json::Value = serde_json::from_slice(encoded)
        .map_err(|_| IndexError::InvalidQuery("predicate value is invalid JSON".into()))?;
    if value.is_null() && field.allow_null {
        return Ok(ScalarValue::Null);
    }
    let invalid = || {
        IndexError::InvalidQuery(format!(
            "predicate value does not match field {:?}'s declared type",
            field.name
        ))
    };
    Ok(match field.field_type {
        FieldType::Boolean => ScalarValue::Boolean(value.as_bool().ok_or_else(invalid)?),
        FieldType::SignedInteger => ScalarValue::Signed(value.as_i64().ok_or_else(invalid)?),
        FieldType::UnsignedInteger => ScalarValue::Unsigned(value.as_u64().ok_or_else(invalid)?),
        FieldType::Float => {
            ScalarValue::number(value.as_f64().ok_or_else(invalid)?).map_err(|_| invalid())?
        }
        FieldType::Keyword => ScalarValue::String(value.as_str().ok_or_else(invalid)?.to_owned()),
        FieldType::Text | FieldType::Vector => return Err(invalid()),
    })
}

fn decode_text(encoded: &[u8], field: &FieldSchema) -> Result<ScalarValue, IndexError> {
    if field.field_type != FieldType::Text {
        return Err(IndexError::InvalidQuery(format!(
            "full-text predicate requires text field {:?}",
            field.name
        )));
    }
    let value: serde_json::Value = serde_json::from_slice(encoded)
        .map_err(|_| IndexError::InvalidQuery("predicate value is invalid JSON".into()))?;
    let text = value.as_str().ok_or_else(|| {
        IndexError::InvalidQuery("full-text predicate requires one JSON string".into())
    })?;
    if text.trim().is_empty() {
        return Err(IndexError::InvalidQuery(
            "full-text predicate must not be empty".into(),
        ));
    }
    Ok(ScalarValue::String(text.to_owned()))
}

fn text_value(value: ScalarValue) -> Result<String, IndexError> {
    match value {
        ScalarValue::String(value) => Ok(value),
        _ => Err(IndexError::InvalidQuery(
            "full-text predicate requires one JSON string".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use anvil_api::v1::index_query::Query;
    use anvil_api::v1::index_specification::Specification;
    use anvil_api::v1::{
        IndexField, IndexFieldCapability, IndexFieldCardinality, IndexPredicate, KeywordIndexField,
        PathIndexQuery, PathIndexSpec, TextIndexField, TypedJsonIndexQuery, TypedJsonIndexSpec,
        index_field,
    };

    use super::*;
    use crate::index_runtime::v4_schema::compile_schema;

    fn typed() -> (IndexSpecification, Schema) {
        let specification = IndexSpecification {
            specification: Some(Specification::TypedJson(TypedJsonIndexSpec {
                fields: vec![IndexField {
                    name: "state".into(),
                    json_pointer: "/state".into(),
                    cardinality: IndexFieldCardinality::Single as i32,
                    capabilities: vec![IndexFieldCapability::Exact as i32],
                    field_type: Some(index_field::FieldType::Keyword(KeywordIndexField {})),
                }],
                physical_order: Vec::new(),
            })),
        };
        let schema = compile_schema("", Some("application/json"), &specification).unwrap();
        (specification, schema)
    }

    fn typed_text() -> (IndexSpecification, Schema) {
        let specification = IndexSpecification {
            specification: Some(Specification::TypedJson(TypedJsonIndexSpec {
                fields: vec![IndexField {
                    name: "summary".into(),
                    json_pointer: "/summary".into(),
                    cardinality: IndexFieldCardinality::Single as i32,
                    capabilities: vec![IndexFieldCapability::FullText as i32],
                    field_type: Some(index_field::FieldType::Text(TextIndexField::default())),
                }],
                physical_order: Vec::new(),
            })),
        };
        let schema = compile_schema("", Some("application/json"), &specification).unwrap();
        (specification, schema)
    }

    fn typed_predicate_query(
        field: &str,
        operator: IndexPredicateOperator,
        values_json: Vec<Vec<u8>>,
    ) -> IndexQuery {
        IndexQuery {
            query: Some(Query::TypedJson(TypedJsonIndexQuery {
                predicates: vec![IndexPredicate {
                    field: field.into(),
                    operator: operator as i32,
                    values_json,
                }],
                order: Vec::new(),
                facets: Vec::new(),
                aggregates: Vec::new(),
            })),
        }
    }

    #[test]
    fn repeated_public_predicates_compile_to_exact_and() {
        let (specification, schema) = typed();
        let query = IndexQuery {
            query: Some(Query::TypedJson(TypedJsonIndexQuery {
                predicates: vec![
                    IndexPredicate {
                        field: "state".into(),
                        operator: IndexPredicateOperator::Equal as i32,
                        values_json: vec![br#""active""#.to_vec()],
                    },
                    IndexPredicate {
                        field: "state".into(),
                        operator: IndexPredicateOperator::Exists as i32,
                        values_json: Vec::new(),
                    },
                ],
                order: Vec::new(),
                facets: Vec::new(),
                aggregates: Vec::new(),
            })),
        };
        let CompiledQuery {
            query:
                NativeQuery::Filter {
                    predicate: Some(Predicate::And(predicates)),
                    ..
                },
            ..
        } = compile_query(&schema, &specification, &query).unwrap()
        else {
            panic!("expected predicate conjunction")
        };
        assert_eq!(predicates.len(), 2);
    }

    #[test]
    fn mismatched_query_kind_is_rejected() {
        let (specification, schema) = typed();
        let query = IndexQuery {
            query: Some(Query::Path(PathIndexQuery {
                prefix: String::new(),
                start_after: None,
            })),
        };
        assert!(compile_query(&schema, &specification, &query).is_err());

        let path_specification = IndexSpecification {
            specification: Some(Specification::Path(PathIndexSpec {})),
        };
        let path_schema = compile_schema("", None, &path_specification).unwrap();
        assert!(compile_query(&path_schema, &path_specification, &query).is_ok());
    }

    #[test]
    fn query_values_follow_the_declared_field_type() {
        let (_, keyword_schema) = typed();
        assert_eq!(
            decode_scalar(br#""active""#, &keyword_schema.fields[0]).unwrap(),
            ScalarValue::String("active".into())
        );
        assert!(decode_scalar(b"4", &keyword_schema.fields[0]).is_err());
    }

    #[test]
    fn predicate_requires_its_declared_capability() {
        let (specification, schema) = typed();
        let query = IndexQuery {
            query: Some(Query::TypedJson(TypedJsonIndexQuery {
                predicates: vec![IndexPredicate {
                    field: "state".into(),
                    operator: IndexPredicateOperator::Prefix as i32,
                    values_json: vec![br#""act""#.to_vec()],
                }],
                order: Vec::new(),
                facets: Vec::new(),
                aggregates: Vec::new(),
            })),
        };
        assert!(compile_query(&schema, &specification, &query).is_err());
    }

    #[test]
    fn fielded_full_text_and_phrase_compile_for_text_fields() {
        let (specification, schema) = typed_text();
        for (operator, phrase) in [
            (IndexPredicateOperator::FullText, false),
            (IndexPredicateOperator::Phrase, true),
        ] {
            let compiled = compile_query(
                &schema,
                &specification,
                &typed_predicate_query("summary", operator, vec![br#""memory safety""#.to_vec()]),
            )
            .unwrap();
            let NativeQuery::Filter {
                predicate: Some(predicate),
                ..
            } = compiled.query
            else {
                panic!("expected a fielded text predicate")
            };
            assert_eq!(
                matches!(predicate, Predicate::Phrase { .. }),
                phrase,
                "operator {operator:?}"
            );
        }
    }

    #[test]
    fn fielded_text_rejects_wrong_field_type_json_type_and_value_count() {
        let (keyword_specification, keyword_schema) = typed();
        assert!(
            compile_query(
                &keyword_schema,
                &keyword_specification,
                &typed_predicate_query(
                    "state",
                    IndexPredicateOperator::FullText,
                    vec![br#""active""#.to_vec()],
                ),
            )
            .is_err()
        );

        let (text_specification, text_schema) = typed_text();
        for values in [
            vec![b"7".to_vec()],
            Vec::new(),
            vec![br#""one""#.to_vec(), br#""two""#.to_vec()],
        ] {
            assert!(
                compile_query(
                    &text_schema,
                    &text_specification,
                    &typed_predicate_query("summary", IndexPredicateOperator::Phrase, values),
                )
                .is_err()
            );
        }
    }

    #[test]
    fn fielded_text_requires_the_full_text_capability() {
        let (_, mut schema) = typed_text();
        schema.fields[0].capabilities = FieldCapabilities::empty();
        let predicate = IndexPredicate {
            field: "summary".into(),
            operator: IndexPredicateOperator::FullText as i32,
            values_json: vec![br#""memory safety""#.to_vec()],
        };

        assert!(compile_predicate(&schema, &predicate, PredicateId::new(0)).is_err());
    }
}
