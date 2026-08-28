//! Checked translation from the public protobuf query into the native v4 plan input.

use keldra_api::v1::index_predicate_expression::Expression;
use keldra_api::v1::index_query::Query;
use keldra_api::v1::index_specification::Specification;
use keldra_api::v1::{
    IndexAggregateOperation, IndexAggregateRequest, IndexFacetRequest, IndexOrder,
    IndexOrderDirection, IndexPredicate, IndexPredicateExpression, IndexPredicateOperator,
    IndexQuery, IndexSpecification,
};
use keldra_index::IndexError;
use keldra_index::v4::{
    AggregateOperation, AggregateRequest, FacetRequest, FieldCapabilities, FieldSchema, FieldType,
    NativeQuery, OrderDirection, OrderField, Predicate, PredicateId, RangeBound, ScalarValue,
    Schema,
};

use super::date::parse_millis;

const MAX_COMPUTATIONS: usize = 32;
const MAX_FACET_BUCKETS: u32 = 1_000;
const MAX_PREDICATE_DEPTH: u32 = 32;
const MAX_PREDICATE_NODES: u32 = 256;

pub(crate) struct CompiledQuery {
    pub(crate) query: NativeQuery,
    pub(crate) facets: Vec<FacetRequest>,
    pub(crate) aggregates: Vec<AggregateRequest>,
}

/// Compile one public query without retaining protobuf enum values or field
/// names in the execution engine.
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
                    predicate: compile_predicate_expression(schema, query.predicate.as_ref())?,
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
                        predicate: compile_predicate_expression(schema, query.predicate.as_ref())?,
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

fn compile_predicate_expression(
    schema: &Schema,
    expression: Option<&IndexPredicateExpression>,
) -> Result<Option<Predicate>, IndexError> {
    expression
        .map(|expression| {
            let mut compiler = PredicateExpressionCompiler {
                schema,
                nodes: 0,
                leaves: 0,
            };
            compiler.compile(expression, 1)
        })
        .transpose()
}

struct PredicateExpressionCompiler<'a> {
    schema: &'a Schema,
    nodes: u32,
    leaves: u32,
}

impl PredicateExpressionCompiler<'_> {
    fn compile(
        &mut self,
        expression: &IndexPredicateExpression,
        depth: u32,
    ) -> Result<Predicate, IndexError> {
        if depth > MAX_PREDICATE_DEPTH {
            return Err(IndexError::InvalidQuery(format!(
                "predicate expression exceeds the maximum depth of {MAX_PREDICATE_DEPTH}"
            )));
        }
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        if self.nodes > MAX_PREDICATE_NODES {
            return Err(IndexError::InvalidQuery(format!(
                "predicate expression exceeds the maximum node count of {MAX_PREDICATE_NODES}"
            )));
        }

        match expression.expression.as_ref() {
            Some(Expression::Predicate(predicate)) => {
                let id = PredicateId::new(self.leaves);
                self.leaves = self
                    .leaves
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                compile_predicate(self.schema, predicate, id)
            }
            Some(Expression::Conjunction(conjunction)) => {
                if conjunction.expressions.is_empty() {
                    return Err(IndexError::InvalidQuery(
                        "predicate conjunction requires at least one child".into(),
                    ));
                }
                Ok(Predicate::And(
                    conjunction
                        .expressions
                        .iter()
                        .map(|child| self.compile(child, depth + 1))
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            }
            Some(Expression::Disjunction(disjunction)) => {
                if disjunction.expressions.is_empty() {
                    return Err(IndexError::InvalidQuery(
                        "predicate disjunction requires at least one child".into(),
                    ));
                }
                Ok(Predicate::Or(
                    disjunction
                        .expressions
                        .iter()
                        .map(|child| self.compile(child, depth + 1))
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            }
            Some(Expression::Negation(negation)) => {
                let child = negation.expression.as_deref().ok_or_else(|| {
                    IndexError::InvalidQuery("predicate negation requires one child".into())
                })?;
                Ok(Predicate::Not(Box::new(self.compile(child, depth + 1)?)))
            }
            None => Err(IndexError::InvalidQuery(
                "predicate expression variant is required".into(),
            )),
        }
    }
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

fn field<'a>(schema: &'a Schema, name: &str) -> Result<&'a FieldSchema, IndexError> {
    schema
        .fields
        .iter()
        .find(|field| field.name == name)
        .ok_or_else(|| IndexError::InvalidQuery(format!("query names unknown field {name:?}")))
}

fn decode_scalar(encoded: &[u8], field: &FieldSchema) -> Result<ScalarValue, IndexError> {
    let value = decode_canonical_json_scalar(encoded)?;
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
        FieldType::Float => match value {
            serde_json::Value::Number(number) if number.is_i64() => {
                ScalarValue::exact_number_from_i64(number.as_i64().ok_or_else(invalid)?)
                    .ok_or_else(invalid)?
            }
            serde_json::Value::Number(number) if number.is_u64() => {
                ScalarValue::exact_number_from_u64(number.as_u64().ok_or_else(invalid)?)
                    .ok_or_else(invalid)?
            }
            _ => ScalarValue::number(value.as_f64().ok_or_else(invalid)?).map_err(|_| invalid())?,
        },
        FieldType::Date => ScalarValue::Signed(
            parse_millis(
                value.as_str().ok_or_else(invalid)?,
                field.date_format.as_ref().ok_or_else(|| {
                    IndexError::InvalidDefinition("Date field has no format".into())
                })?,
            )
            .map_err(|_| invalid())?,
        ),
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
    let value = decode_canonical_json_scalar(encoded)?;
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

fn decode_canonical_json_scalar(encoded: &[u8]) -> Result<serde_json::Value, IndexError> {
    let value: serde_json::Value = serde_json::from_slice(encoded)
        .map_err(|_| IndexError::InvalidQuery("predicate value is invalid JSON".into()))?;
    if matches!(
        value,
        serde_json::Value::Array(_) | serde_json::Value::Object(_)
    ) || serde_json::to_vec(&value)
        .map_err(|_| IndexError::InvalidQuery("predicate value is invalid JSON".into()))?
        != encoded
    {
        return Err(IndexError::InvalidQuery(
            "predicate value must be one canonical JSON scalar".into(),
        ));
    }
    Ok(value)
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
    use keldra_api::v1::index_predicate_expression::Expression;
    use keldra_api::v1::index_query::Query;
    use keldra_api::v1::index_specification::Specification;
    use keldra_api::v1::{
        IndexField, IndexFieldCapability, IndexFieldCardinality, IndexPredicate,
        IndexPredicateConjunction, IndexPredicateDisjunction, IndexPredicateExpression,
        IndexPredicateNegation, KeywordIndexField, PathIndexQuery, PathIndexSpec, TextIndexField,
        TypedJsonIndexQuery, TypedJsonIndexSpec, index_field,
    };
    use keldra_index::v4::DateFormat;

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
                predicate: Some(leaf(IndexPredicate {
                    field: field.into(),
                    operator: operator as i32,
                    values_json,
                })),
                order: Vec::new(),
                facets: Vec::new(),
                aggregates: Vec::new(),
            })),
        }
    }

    fn leaf(predicate: IndexPredicate) -> IndexPredicateExpression {
        IndexPredicateExpression {
            expression: Some(Expression::Predicate(predicate)),
        }
    }

    fn exists(field: &str) -> IndexPredicateExpression {
        leaf(IndexPredicate {
            field: field.into(),
            operator: IndexPredicateOperator::Exists as i32,
            values_json: Vec::new(),
        })
    }

    fn all(expressions: Vec<IndexPredicateExpression>) -> IndexPredicateExpression {
        IndexPredicateExpression {
            expression: Some(Expression::Conjunction(IndexPredicateConjunction {
                expressions,
            })),
        }
    }

    fn any(expressions: Vec<IndexPredicateExpression>) -> IndexPredicateExpression {
        IndexPredicateExpression {
            expression: Some(Expression::Disjunction(IndexPredicateDisjunction {
                expressions,
            })),
        }
    }

    fn not(expression: IndexPredicateExpression) -> IndexPredicateExpression {
        IndexPredicateExpression {
            expression: Some(Expression::Negation(Box::new(IndexPredicateNegation {
                expression: Some(Box::new(expression)),
            }))),
        }
    }

    #[test]
    fn public_boolean_expression_compiles_with_stable_preorder_leaf_ids() {
        let (specification, schema) = typed();
        let query = IndexQuery {
            query: Some(Query::TypedJson(TypedJsonIndexQuery {
                predicate: Some(all(vec![
                    leaf(IndexPredicate {
                        field: "state".into(),
                        operator: IndexPredicateOperator::Equal as i32,
                        values_json: vec![br#""active""#.to_vec()],
                    }),
                    any(vec![exists("state"), not(exists("state"))]),
                ])),
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
        let Predicate::Equal { id: first, .. } = &predicates[0] else {
            panic!("expected equality leaf")
        };
        let Predicate::Or(alternatives) = &predicates[1] else {
            panic!("expected disjunction")
        };
        let Predicate::Exists { id: second, .. } = &alternatives[0] else {
            panic!("expected existence leaf")
        };
        let Predicate::Not(negated) = &alternatives[1] else {
            panic!("expected negation")
        };
        let Predicate::Exists { id: third, .. } = negated.as_ref() else {
            panic!("expected negated existence leaf")
        };
        assert_eq!([first.get(), second.get(), third.get()], [0, 1, 2]);
    }

    #[test]
    fn absent_predicate_matches_all_but_empty_boolean_nodes_are_rejected() {
        let (specification, schema) = typed();
        let match_all = IndexQuery {
            query: Some(Query::TypedJson(TypedJsonIndexQuery {
                predicate: None,
                order: Vec::new(),
                facets: Vec::new(),
                aggregates: Vec::new(),
            })),
        };
        let compiled = compile_query(&schema, &specification, &match_all).unwrap();
        assert!(matches!(
            compiled.query,
            NativeQuery::Filter {
                predicate: None,
                ..
            }
        ));

        for invalid in [
            IndexPredicateExpression { expression: None },
            all(Vec::new()),
            any(Vec::new()),
            IndexPredicateExpression {
                expression: Some(Expression::Negation(Box::new(IndexPredicateNegation {
                    expression: None,
                }))),
            },
        ] {
            assert!(compile_predicate_expression(&schema, Some(&invalid)).is_err());
        }
    }

    #[test]
    fn public_boolean_expression_depth_and_node_count_are_bounded() {
        let (_, schema) = typed();
        let mut too_deep = exists("state");
        for _ in 0..MAX_PREDICATE_DEPTH {
            too_deep = not(too_deep);
        }
        assert!(compile_predicate_expression(&schema, Some(&too_deep)).is_err());

        let too_wide = all((0..MAX_PREDICATE_NODES).map(|_| exists("state")).collect());
        assert!(compile_predicate_expression(&schema, Some(&too_wide)).is_err());
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
        assert!(decode_scalar(br#""\u0061ctive""#, &keyword_schema.fields[0]).is_err());
        assert!(decode_scalar(b" \"active\"", &keyword_schema.fields[0]).is_err());
    }

    #[test]
    fn date_query_literals_use_the_fields_format() {
        let (_, mut schema) = typed();
        let field = &mut schema.fields[0];
        field.field_type = FieldType::Date;
        field.date_format = Some(DateFormat::Strftime("%d/%m/%Y".into()));
        assert_eq!(
            decode_scalar(br#""02/01/1970""#, field).unwrap(),
            ScalarValue::Signed(86_400_000)
        );
        assert!(decode_scalar(br#""1970-01-02""#, field).is_err());
        assert!(decode_scalar(b"86400000", field).is_err());
    }

    #[test]
    fn float_query_values_reject_lossy_integer_conversion() {
        let (_, mut schema) = typed();
        schema.fields[0].field_type = FieldType::Float;
        let field = &schema.fields[0];

        assert!(decode_scalar(b"9007199254740992", field).is_ok());
        assert!(decode_scalar(b"9007199254740993", field).is_err());
        assert!(decode_scalar(b"18446744073709551615", field).is_err());
        assert!(decode_scalar(b"1.5", field).is_ok());
    }

    #[test]
    fn predicate_requires_its_declared_capability() {
        let (specification, schema) = typed();
        let query = IndexQuery {
            query: Some(Query::TypedJson(TypedJsonIndexQuery {
                predicate: Some(leaf(IndexPredicate {
                    field: "state".into(),
                    operator: IndexPredicateOperator::Prefix as i32,
                    values_json: vec![br#""act""#.to_vec()],
                })),
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
