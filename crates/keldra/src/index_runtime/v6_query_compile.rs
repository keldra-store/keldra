//! Checked public-protobuf translation for the storage-neutral v6 executor.

use keldra_api::v1::index_predicate_expression::Expression;
use keldra_api::v1::index_query::Query;
use keldra_api::v1::{
    IndexAggregateOperation, IndexPredicate, IndexPredicateExpression, IndexPredicateOperator,
    IndexQuery,
};
use keldra_index::IndexError;
use keldra_index::typed_json::{
    AggregateOperation, AggregateRequest, FacetRequest, FieldCapabilities, FieldId, FieldSchema,
    FieldType, OrderDirection, OrderField, Predicate, PredicateId, RangeBound, ScalarValue,
    TypedJsonSchema,
};

use super::date::parse_millis;

const MAX_COMPUTATIONS: usize = 32;
const MAX_FACET_BUCKETS: u32 = 1_000;
const MAX_PREDICATE_DEPTH: u32 = 32;
const MAX_PREDICATE_NODES: u32 = 256;

pub(crate) struct CompiledV6Query {
    pub(crate) predicate: Option<Predicate>,
    pub(crate) order: Vec<OrderField>,
    pub(crate) facets: Vec<FacetRequest>,
    pub(crate) aggregates: Vec<AggregateRequest>,
}

pub(crate) fn compile_v6_query(
    schema: &TypedJsonSchema,
    query: &IndexQuery,
) -> Result<CompiledV6Query, IndexError> {
    schema.validate()?;
    let Some(Query::TypedJson(query)) = query.query.as_ref() else {
        return Err(IndexError::InvalidQuery(
            "query kind does not match the TypedJson index".into(),
        ));
    };
    if query.facets.len().saturating_add(query.aggregates.len()) > MAX_COMPUTATIONS {
        return Err(IndexError::InvalidQuery(
            "a query supports at most 32 facet and aggregate computations".into(),
        ));
    }
    Ok(CompiledV6Query {
        predicate: compile_expression(schema, query.predicate.as_ref())?,
        order: query
            .order
            .iter()
            .map(|order| {
                let field = field(schema, &order.field)?;
                if !field.capabilities.contains(FieldCapabilities::ORDER) {
                    return Err(IndexError::InvalidQuery(format!(
                        "field {:?} does not declare ORDER",
                        field.name
                    )));
                }
                let direction = match keldra_api::v1::IndexOrderDirection::try_from(order.direction)
                    .map_err(|_| IndexError::InvalidQuery("unknown order direction".into()))?
                {
                    keldra_api::v1::IndexOrderDirection::Ascending => OrderDirection::Ascending,
                    keldra_api::v1::IndexOrderDirection::Descending => OrderDirection::Descending,
                };
                Ok(OrderField {
                    field_id: field.id,
                    direction,
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        facets: query
            .facets
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
            .collect::<Result<Vec<_>, _>>()?,
        aggregates: query
            .aggregates
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
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn compile_expression(
    schema: &TypedJsonSchema,
    expression: Option<&IndexPredicateExpression>,
) -> Result<Option<Predicate>, IndexError> {
    expression
        .map(|expression| {
            PredicateCompiler {
                schema,
                nodes: 0,
                leaves: 0,
            }
            .compile(expression, 1)
        })
        .transpose()
}

struct PredicateCompiler<'a> {
    schema: &'a TypedJsonSchema,
    nodes: u32,
    leaves: u32,
}

impl PredicateCompiler<'_> {
    fn compile(
        &mut self,
        expression: &IndexPredicateExpression,
        depth: u32,
    ) -> Result<Predicate, IndexError> {
        if depth > MAX_PREDICATE_DEPTH {
            return Err(IndexError::InvalidQuery(
                "predicate expression exceeds the maximum depth of 32".into(),
            ));
        }
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        if self.nodes > MAX_PREDICATE_NODES {
            return Err(IndexError::InvalidQuery(
                "predicate expression exceeds the maximum node count of 256".into(),
            ));
        }
        match expression.expression.as_ref() {
            Some(Expression::Predicate(predicate)) => {
                let id = PredicateId::new(self.leaves);
                self.leaves = self
                    .leaves
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                compile_leaf(self.schema, predicate, id)
            }
            Some(Expression::Conjunction(value)) => {
                if value.expressions.is_empty() {
                    return Err(IndexError::InvalidQuery(
                        "Boolean predicate requires a child".into(),
                    ));
                }
                let children = value
                    .expressions
                    .iter()
                    .map(|child| self.compile(child, depth + 1))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Predicate::And(children))
            }
            Some(Expression::Disjunction(value)) => {
                if value.expressions.is_empty() {
                    return Err(IndexError::InvalidQuery(
                        "Boolean predicate requires a child".into(),
                    ));
                }
                Ok(Predicate::Or(
                    value
                        .expressions
                        .iter()
                        .map(|child| self.compile(child, depth + 1))
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            }
            Some(Expression::Negation(value)) => Ok(Predicate::Not(Box::new(self.compile(
                value.expression.as_deref().ok_or_else(|| {
                    IndexError::InvalidQuery("predicate negation requires one child".into())
                })?,
                depth + 1,
            )?))),
            None => Err(IndexError::InvalidQuery(
                "predicate expression variant is required".into(),
            )),
        }
    }
}

fn compile_leaf(
    schema: &TypedJsonSchema,
    predicate: &IndexPredicate,
    id: PredicateId,
) -> Result<Predicate, IndexError> {
    let field = field(schema, &predicate.field)?;
    let operator = IndexPredicateOperator::try_from(predicate.operator)
        .map_err(|_| IndexError::InvalidQuery("unknown predicate operator".into()))?;
    let values = predicate
        .values_json
        .iter()
        .map(|bytes| decode_scalar(bytes, field, operator))
        .collect::<Result<Vec<_>, _>>()?;
    let one = || {
        (values.len() == 1)
            .then(|| values[0].clone())
            .ok_or_else(|| {
                IndexError::InvalidQuery("predicate operator requires exactly one value".into())
            })
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
            "field {:?} does not declare the predicate capability",
            field.name
        )));
    }
    let field_id = field.id;
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
            prefix: string_value(one()?, "prefix")?,
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
            text: string_value(one()?, "full-text")?,
        },
        IndexPredicateOperator::Phrase => Predicate::Phrase {
            id,
            field_id,
            text: string_value(one()?, "phrase")?,
        },
        _ => {
            return Err(IndexError::InvalidQuery(
                "predicate value count does not match its operator".into(),
            ));
        }
    })
}

fn field<'a>(schema: &'a TypedJsonSchema, name: &str) -> Result<&'a FieldSchema, IndexError> {
    schema
        .fields
        .iter()
        .find(|field| field.name == name)
        .ok_or_else(|| IndexError::InvalidQuery(format!("query names unknown field {name:?}")))
}

fn decode_scalar(
    encoded: &[u8],
    field: &FieldSchema,
    operator: IndexPredicateOperator,
) -> Result<ScalarValue, IndexError> {
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
    let invalid = || {
        IndexError::InvalidQuery(format!(
            "predicate value does not match field {:?}'s declared type",
            field.name
        ))
    };
    if matches!(
        operator,
        IndexPredicateOperator::FullText | IndexPredicateOperator::Phrase
    ) {
        if field.field_type != FieldType::Text {
            return Err(invalid());
        }
        return Ok(ScalarValue::String(
            value.as_str().ok_or_else(invalid)?.to_owned(),
        ));
    }
    if value.is_null() && field.allow_null {
        return Ok(ScalarValue::Null);
    }
    Ok(match field.field_type {
        FieldType::Boolean => ScalarValue::Boolean(value.as_bool().ok_or_else(invalid)?),
        FieldType::SignedInteger => ScalarValue::Signed(value.as_i64().ok_or_else(invalid)?),
        FieldType::UnsignedInteger => ScalarValue::Unsigned(value.as_u64().ok_or_else(invalid)?),
        FieldType::Float => match &value {
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
                &field.effective_date_format().ok_or_else(invalid)?,
            )
            .map_err(|_| invalid())?,
        ),
        FieldType::Keyword => ScalarValue::String(value.as_str().ok_or_else(invalid)?.to_owned()),
        FieldType::Text => return Err(invalid()),
    })
}

fn string_value(value: ScalarValue, operation: &str) -> Result<String, IndexError> {
    match value {
        ScalarValue::String(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(IndexError::InvalidQuery(format!(
            "{operation} predicate requires a non-empty JSON string"
        ))),
    }
}
