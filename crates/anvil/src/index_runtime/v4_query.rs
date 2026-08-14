//! Checked translation from the public protobuf query into the native v4 plan input.

use anvil_api::v1::index_query::Query;
use anvil_api::v1::index_specification::Specification;
use anvil_api::v1::{
    IndexOrder, IndexOrderDirection, IndexPredicate, IndexPredicateOperator, IndexQuery,
    IndexSpecification,
};
use anvil_index::IndexError;
use anvil_index::v4::{
    FieldId, NativeQuery, OrderDirection, OrderField, Predicate, PredicateId, RangeBound,
    ScalarValue, Schema,
};

/// Compile one public query without retaining protobuf enum values or field
/// names in the execution engine. Repeated public predicates are an exact AND.
pub(crate) fn compile_query(
    schema: &Schema,
    specification: &IndexSpecification,
    query: &IndexQuery,
) -> Result<NativeQuery, IndexError> {
    schema.validate()?;
    match (specification.specification.as_ref(), query.query.as_ref()) {
        (Some(Specification::Path(_)), Some(Query::Path(query))) => Ok(NativeQuery::Path {
            prefix: query.prefix.clone(),
            start_after: query.start_after.clone(),
        }),
        (Some(Specification::MetadataFilter(_)), Some(Query::MetadataFilter(query))) => {
            Ok(NativeQuery::Filter {
                predicate: compile_predicates(schema, &query.predicates, true)?,
                order: Vec::new(),
            })
        }
        (Some(Specification::TypedJson(_)), Some(Query::TypedJson(query))) => {
            Ok(NativeQuery::Filter {
                predicate: compile_predicates(schema, &query.predicates, false)?,
                order: compile_order(schema, &query.order)?,
            })
        }
        (Some(Specification::FullText(_)), Some(Query::FullText(query))) => {
            if query.text.trim().is_empty() {
                return Err(IndexError::InvalidQuery(
                    "full-text query must not be empty".into(),
                ));
            }
            Ok(NativeQuery::FullText {
                text: query.text.clone(),
                phrase: query.phrase,
            })
        }
        (Some(Specification::Vector(_)), Some(Query::Vector(query))) => Ok(NativeQuery::Vector {
            values: query.values.clone(),
        }),
        (Some(Specification::Hybrid(_)), Some(Query::Hybrid(query))) => {
            if query.text.trim().is_empty() && query.vector.is_empty() {
                return Err(IndexError::InvalidQuery(
                    "hybrid query requires text or a vector".into(),
                ));
            }
            Ok(NativeQuery::Hybrid {
                text: query.text.clone(),
                vector: query.vector.clone(),
            })
        }
        (Some(Specification::GitSource(specification)), Some(Query::GitSource(query))) => {
            if query.commit_id.is_empty() || query.tree_path.contains('\0') {
                return Err(IndexError::InvalidQuery(
                    "Git query requires a commit and a canonical tree path".into(),
                ));
            }
            Ok(NativeQuery::GitSource {
                repository_id: specification.repository_id.clone(),
                commit_id: query.commit_id.clone(),
                tree_path: query.tree_path.clone(),
                prefix: query.prefix,
            })
        }
        (Some(Specification::Tensor(specification)), Some(Query::Tensor(query))) => {
            if query.tensor_name.is_empty() {
                return Err(IndexError::InvalidQuery(
                    "tensor query requires a tensor name".into(),
                ));
            }
            Ok(NativeQuery::Tensor {
                model_id: specification.model_id.clone(),
                tensor_name: query.tensor_name.clone(),
            })
        }
        (Some(_), Some(_)) => Err(IndexError::InvalidQuery(
            "query kind does not match index kind".into(),
        )),
        (None, _) => Err(IndexError::InvalidDefinition(
            "index specification is required".into(),
        )),
        (_, None) => Err(IndexError::InvalidQuery("index query is required".into())),
    }
}

fn compile_predicates(
    schema: &Schema,
    predicates: &[IndexPredicate],
    metadata: bool,
) -> Result<Option<Predicate>, IndexError> {
    let mut compiled = Vec::with_capacity(predicates.len());
    for (ordinal, predicate) in predicates.iter().enumerate() {
        let id = PredicateId::new(
            u32::try_from(ordinal)
                .map_err(|_| IndexError::InvalidQuery("too many predicates".into()))?,
        );
        compiled.push(compile_predicate(schema, predicate, id, metadata)?);
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
    metadata: bool,
) -> Result<Predicate, IndexError> {
    let field_id = field_id(schema, &predicate.field)?;
    let operator = IndexPredicateOperator::try_from(predicate.operator)
        .map_err(|_| IndexError::InvalidQuery("unknown predicate operator".into()))?;
    let values = predicate
        .values_json
        .iter()
        .map(|encoded| decode_scalar(encoded, metadata, &predicate.field))
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
            Ok(OrderField {
                field_id: field_id(schema, &order.field)?,
                direction,
            })
        })
        .collect()
}

fn field_id(schema: &Schema, name: &str) -> Result<FieldId, IndexError> {
    schema
        .fields
        .iter()
        .find(|field| field.name == name)
        .map(|field| field.id)
        .ok_or_else(|| IndexError::InvalidQuery(format!("query names unknown field {name:?}")))
}

fn decode_scalar(encoded: &[u8], metadata: bool, field: &str) -> Result<ScalarValue, IndexError> {
    let value: serde_json::Value = serde_json::from_slice(encoded)
        .map_err(|_| IndexError::InvalidQuery("predicate value is invalid JSON".into()))?;
    if encoded.trim_ascii() == b"-0" {
        return Ok(ScalarValue::Unsigned(0));
    }
    if metadata
        && matches!(
            field,
            "version" | "content_length" | "committed_at_unix_millis"
        )
    {
        return value.as_u64().map(ScalarValue::Unsigned).ok_or_else(|| {
            IndexError::InvalidQuery(
                "unsigned metadata predicate must be a JSON unsigned integer".into(),
            )
        });
    }
    match value {
        serde_json::Value::Null => Ok(ScalarValue::Null),
        serde_json::Value::Bool(value) => Ok(ScalarValue::Boolean(value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                Ok(ScalarValue::Unsigned(value))
            } else {
                ScalarValue::number(value.as_f64().ok_or_else(|| {
                    IndexError::InvalidQuery("predicate number is not finite".into())
                })?)
                .map_err(|_| IndexError::InvalidQuery("predicate number is not finite".into()))
            }
        }
        serde_json::Value::String(value) => Ok(ScalarValue::String(value)),
        _ => Err(IndexError::InvalidQuery(
            "predicate value must be a JSON scalar".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use anvil_api::v1::index_query::Query;
    use anvil_api::v1::index_specification::Specification;
    use anvil_api::v1::{
        IndexField, IndexPredicate, PathIndexQuery, PathIndexSpec, TypedJsonIndexQuery,
        TypedJsonIndexSpec,
    };

    use super::*;
    use crate::index_runtime::v4_schema::compile_schema;

    fn typed() -> (IndexSpecification, Schema) {
        let specification = IndexSpecification {
            specification: Some(Specification::TypedJson(TypedJsonIndexSpec {
                fields: vec![IndexField {
                    name: "state".into(),
                    json_pointer: "/state".into(),
                    multi_valued: false,
                }],
                physical_order: Vec::new(),
            })),
        };
        let schema = compile_schema("", Some("application/json"), &specification).unwrap();
        (specification, schema)
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
            })),
        };
        let NativeQuery::Filter {
            predicate: Some(Predicate::And(predicates)),
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
    fn scalar_numbers_match_projection_types_and_canonicalize_lexical_negative_zero() {
        assert_eq!(
            decode_scalar(b"18446744073709551615", false, "value").unwrap(),
            ScalarValue::Unsigned(u64::MAX)
        );
        assert_eq!(
            decode_scalar(b"0", false, "value").unwrap(),
            ScalarValue::Unsigned(0)
        );
        assert_eq!(
            decode_scalar(b" \n -0\t", false, "value").unwrap(),
            ScalarValue::Unsigned(0)
        );
        assert_eq!(
            decode_scalar(b"-2", false, "value").unwrap(),
            ScalarValue::number(-2.0).unwrap()
        );
        assert_eq!(
            decode_scalar(b"2.0", false, "value").unwrap(),
            ScalarValue::number(2.0).unwrap()
        );
        assert_eq!(
            decode_scalar(b"2e0", false, "value").unwrap(),
            ScalarValue::number(2.0).unwrap()
        );
    }
}
