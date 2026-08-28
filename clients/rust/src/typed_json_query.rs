//! Bounded, typed construction of public index predicate expressions.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use keldra_api::v1::{IndexPredicate, IndexPredicateExpression, IndexPredicateOperator};

const MAX_PREDICATE_DEPTH: u32 = 32;
const MAX_PREDICATE_NODES: u32 = 256;

/// One canonical JSON scalar accepted by a Typed JSON or metadata predicate.
#[derive(Clone, Debug, PartialEq)]
pub enum PredicateScalar {
    Null,
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(String),
}

impl From<bool> for PredicateScalar {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i64> for PredicateScalar {
    fn from(value: i64) -> Self {
        Self::Signed(value)
    }
}

impl From<u64> for PredicateScalar {
    fn from(value: u64) -> Self {
        Self::Unsigned(value)
    }
}

impl From<f64> for PredicateScalar {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<String> for PredicateScalar {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for PredicateScalar {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl PredicateScalar {
    fn encode(self) -> Result<Vec<u8>, PredicateBuildError> {
        let value = match self {
            Self::Null => serde_json::Value::Null,
            Self::Boolean(value) => serde_json::Value::Bool(value),
            Self::Signed(value) => serde_json::Value::Number(value.into()),
            Self::Unsigned(value) => serde_json::Value::Number(value.into()),
            Self::Float(value) => serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .ok_or(PredicateBuildError::NonFiniteFloat)?,
            Self::String(value) => serde_json::Value::String(value),
        };
        serde_json::to_vec(&value).map_err(|_| PredicateBuildError::ScalarEncoding)
    }
}

/// A predicate rejected locally before a request is sent to Keldra.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PredicateBuildError {
    InvalidField,
    EmptyMembership,
    EmptyText,
    EmptyConjunction,
    EmptyDisjunction,
    NonFiniteFloat,
    ScalarEncoding,
    ExpressionTooDeep,
    TooManyExpressionNodes,
}

impl Display for PredicateBuildError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidField => {
                formatter.write_str("predicate field must be non-empty and contain no NUL")
            }
            Self::EmptyMembership => formatter.write_str("IN requires at least one value"),
            Self::EmptyText => {
                formatter.write_str("full-text and phrase predicates require non-empty text")
            }
            Self::EmptyConjunction => {
                formatter.write_str("a predicate conjunction requires at least one child")
            }
            Self::EmptyDisjunction => {
                formatter.write_str("a predicate disjunction requires at least one child")
            }
            Self::NonFiniteFloat => formatter.write_str("predicate float must be finite"),
            Self::ScalarEncoding => {
                formatter.write_str("predicate scalar could not be encoded as canonical JSON")
            }
            Self::ExpressionTooDeep => write!(
                formatter,
                "predicate expression exceeds the maximum depth of {MAX_PREDICATE_DEPTH}"
            ),
            Self::TooManyExpressionNodes => write!(
                formatter,
                "predicate expression exceeds the maximum node count of {MAX_PREDICATE_NODES}"
            ),
        }
    }
}

impl Error for PredicateBuildError {}

/// One valid, bounded public Boolean predicate expression.
#[derive(Clone, Debug, PartialEq)]
pub struct PredicateExpression {
    expression: IndexPredicateExpression,
    depth: u32,
    nodes: u32,
}

impl PredicateExpression {
    pub fn equal(
        field: impl Into<String>,
        value: impl Into<PredicateScalar>,
    ) -> Result<Self, PredicateBuildError> {
        Self::scalar(field, IndexPredicateOperator::Equal, value.into())
    }

    pub fn in_values(
        field: impl Into<String>,
        values: impl IntoIterator<Item = impl Into<PredicateScalar>>,
    ) -> Result<Self, PredicateBuildError> {
        let field = valid_field(field.into())?;
        let values_json = values
            .into_iter()
            .map(|value| value.into().encode())
            .collect::<Result<Vec<_>, _>>()?;
        if values_json.is_empty() {
            return Err(PredicateBuildError::EmptyMembership);
        }
        Ok(Self::leaf(IndexPredicate {
            field,
            operator: IndexPredicateOperator::In as i32,
            values_json,
        }))
    }

    pub fn prefix(
        field: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, PredicateBuildError> {
        Self::scalar(
            field,
            IndexPredicateOperator::Prefix,
            PredicateScalar::String(prefix.into()),
        )
    }

    pub fn less_than(
        field: impl Into<String>,
        value: impl Into<PredicateScalar>,
    ) -> Result<Self, PredicateBuildError> {
        Self::scalar(field, IndexPredicateOperator::LessThan, value.into())
    }

    pub fn less_than_or_equal(
        field: impl Into<String>,
        value: impl Into<PredicateScalar>,
    ) -> Result<Self, PredicateBuildError> {
        Self::scalar(field, IndexPredicateOperator::LessThanOrEqual, value.into())
    }

    pub fn greater_than(
        field: impl Into<String>,
        value: impl Into<PredicateScalar>,
    ) -> Result<Self, PredicateBuildError> {
        Self::scalar(field, IndexPredicateOperator::GreaterThan, value.into())
    }

    pub fn greater_than_or_equal(
        field: impl Into<String>,
        value: impl Into<PredicateScalar>,
    ) -> Result<Self, PredicateBuildError> {
        Self::scalar(
            field,
            IndexPredicateOperator::GreaterThanOrEqual,
            value.into(),
        )
    }

    pub fn exists(field: impl Into<String>) -> Result<Self, PredicateBuildError> {
        Ok(Self::leaf(IndexPredicate {
            field: valid_field(field.into())?,
            operator: IndexPredicateOperator::Exists as i32,
            values_json: Vec::new(),
        }))
    }

    pub fn full_text(
        field: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<Self, PredicateBuildError> {
        Self::text(field, IndexPredicateOperator::FullText, text.into())
    }

    pub fn phrase(
        field: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<Self, PredicateBuildError> {
        Self::text(field, IndexPredicateOperator::Phrase, text.into())
    }

    pub fn all(expressions: impl IntoIterator<Item = Self>) -> Result<Self, PredicateBuildError> {
        let expressions = expressions.into_iter().collect::<Vec<_>>();
        Self::combine(expressions, true)
    }

    pub fn any(expressions: impl IntoIterator<Item = Self>) -> Result<Self, PredicateBuildError> {
        let expressions = expressions.into_iter().collect::<Vec<_>>();
        Self::combine(expressions, false)
    }

    pub fn negated(self) -> Result<Self, PredicateBuildError> {
        let depth = self
            .depth
            .checked_add(1)
            .ok_or(PredicateBuildError::ExpressionTooDeep)?;
        let nodes = self
            .nodes
            .checked_add(1)
            .ok_or(PredicateBuildError::TooManyExpressionNodes)?;
        validate_bounds(depth, nodes)?;
        Ok(Self {
            expression: self.expression.negated(),
            depth,
            nodes,
        })
    }

    pub fn into_proto(self) -> IndexPredicateExpression {
        self.expression
    }

    fn scalar(
        field: impl Into<String>,
        operator: IndexPredicateOperator,
        value: PredicateScalar,
    ) -> Result<Self, PredicateBuildError> {
        Ok(Self::leaf(IndexPredicate {
            field: valid_field(field.into())?,
            operator: operator as i32,
            values_json: vec![value.encode()?],
        }))
    }

    fn text(
        field: impl Into<String>,
        operator: IndexPredicateOperator,
        text: String,
    ) -> Result<Self, PredicateBuildError> {
        if text.trim().is_empty() {
            return Err(PredicateBuildError::EmptyText);
        }
        Self::scalar(field, operator, PredicateScalar::String(text))
    }

    fn leaf(predicate: IndexPredicate) -> Self {
        Self {
            expression: IndexPredicateExpression::leaf(predicate),
            depth: 1,
            nodes: 1,
        }
    }

    fn combine(expressions: Vec<Self>, conjunction: bool) -> Result<Self, PredicateBuildError> {
        if expressions.is_empty() {
            return Err(if conjunction {
                PredicateBuildError::EmptyConjunction
            } else {
                PredicateBuildError::EmptyDisjunction
            });
        }
        let depth = expressions
            .iter()
            .map(|expression| expression.depth)
            .max()
            .expect("non-empty expression list")
            .checked_add(1)
            .ok_or(PredicateBuildError::ExpressionTooDeep)?;
        let nodes = expressions.iter().try_fold(1_u32, |nodes, expression| {
            nodes
                .checked_add(expression.nodes)
                .ok_or(PredicateBuildError::TooManyExpressionNodes)
        })?;
        validate_bounds(depth, nodes)?;
        let expressions = expressions
            .into_iter()
            .map(Self::into_proto)
            .collect::<Vec<_>>();
        let expression = if conjunction {
            IndexPredicateExpression::all(expressions)
                .expect("client rejected an empty conjunction")
        } else {
            IndexPredicateExpression::any(expressions)
                .expect("client rejected an empty disjunction")
        };
        Ok(Self {
            expression,
            depth,
            nodes,
        })
    }
}

impl From<PredicateExpression> for IndexPredicateExpression {
    fn from(expression: PredicateExpression) -> Self {
        expression.into_proto()
    }
}

fn valid_field(field: String) -> Result<String, PredicateBuildError> {
    if field.is_empty() || field.contains('\0') {
        Err(PredicateBuildError::InvalidField)
    } else {
        Ok(field)
    }
}

fn validate_bounds(depth: u32, nodes: u32) -> Result<(), PredicateBuildError> {
    if depth > MAX_PREDICATE_DEPTH {
        return Err(PredicateBuildError::ExpressionTooDeep);
    }
    if nodes > MAX_PREDICATE_NODES {
        return Err(PredicateBuildError::TooManyExpressionNodes);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use keldra_api::v1::index_predicate_expression::Expression;

    use super::*;

    #[test]
    fn typed_scalars_and_boolean_shape_have_canonical_wire_values() {
        let expression = PredicateExpression::all([
            PredicateExpression::any([
                PredicateExpression::equal("status", "pending").unwrap(),
                PredicateExpression::equal("status", "retryable").unwrap(),
            ])
            .unwrap(),
            PredicateExpression::less_than_or_equal("due_at", 42_u64).unwrap(),
            PredicateExpression::exists("owner")
                .unwrap()
                .negated()
                .unwrap(),
        ])
        .unwrap()
        .into_proto();

        let Some(Expression::Conjunction(all)) = expression.expression else {
            panic!("expected conjunction")
        };
        assert_eq!(all.expressions.len(), 3);
        let Some(Expression::Predicate(due_at)) = &all.expressions[1].expression else {
            panic!("expected range leaf")
        };
        assert_eq!(due_at.values_json, [b"42"]);
        assert!(matches!(
            all.expressions[2].expression,
            Some(Expression::Negation(_))
        ));
    }

    #[test]
    fn invalid_values_and_expression_bounds_fail_locally() {
        assert_eq!(
            PredicateExpression::in_values("status", Vec::<&str>::new()).unwrap_err(),
            PredicateBuildError::EmptyMembership
        );
        assert_eq!(
            PredicateExpression::equal("score", f64::NAN).unwrap_err(),
            PredicateBuildError::NonFiniteFloat
        );
        assert_eq!(
            PredicateExpression::all(Vec::new()).unwrap_err(),
            PredicateBuildError::EmptyConjunction
        );

        let mut expression = PredicateExpression::exists("owner").unwrap();
        for _ in 1..MAX_PREDICATE_DEPTH {
            expression = expression.negated().unwrap();
        }
        assert_eq!(
            expression.negated().unwrap_err(),
            PredicateBuildError::ExpressionTooDeep
        );

        let leaves = (0..MAX_PREDICATE_NODES)
            .map(|_| PredicateExpression::exists("owner").unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            PredicateExpression::all(leaves).unwrap_err(),
            PredicateBuildError::TooManyExpressionNodes
        );
    }
}
