use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::IndexError;
use crate::typed_json::{
    AggregateOperation, AggregateRequest, AggregateResult, Cardinality, FacetBucket, FacetRequest,
    FacetResult, FieldCapabilities, FieldId, FieldSchema, FieldType, OrderDirection, OrderField,
    Predicate, ScalarValue,
};

use super::{
    Budget, ProjectionPartitionIdentity, QueryBlockCredits, QueryCandidate, QueryFieldBinding,
    QueryRunChild, QueryRunPage, RecipeIdentity, StableDocumentKey, TypedJsonQueryRequest,
};

pub(super) fn page_summary(
    hash: [u8; 32],
    page: &QueryRunPage,
    encoded_bytes: usize,
) -> Result<QueryRunChild, IndexError> {
    let encoded_bytes = u64::try_from(encoded_bytes).map_err(|_| IndexError::OffsetOverflow)?;
    match page {
        QueryRunPage::Leaf(runs) => {
            let first = runs.first().ok_or(IndexError::Integrity)?;
            let last = runs.last().ok_or(IndexError::Integrity)?;
            Ok(QueryRunChild {
                hash,
                encoded_bytes,
                run_count: runs.len() as u64,
                first_sequence: first.sequence,
                last_sequence: last.sequence,
                source_start_offset: first.source_start_offset,
                next_offset: last.next_offset,
                through_atomic_position: last.through_atomic_position,
            })
        }
        QueryRunPage::Branch(children) => {
            let first = children.first().ok_or(IndexError::Integrity)?;
            let last = children.last().ok_or(IndexError::Integrity)?;
            Ok(QueryRunChild {
                hash,
                encoded_bytes,
                run_count: children.iter().try_fold(0u64, |sum, child| {
                    sum.checked_add(child.run_count)
                        .ok_or(IndexError::OffsetOverflow)
                })?,
                first_sequence: first.first_sequence,
                last_sequence: last.last_sequence,
                source_start_offset: first.source_start_offset,
                next_offset: last.next_offset,
                through_atomic_position: last.through_atomic_position,
            })
        }
    }
}

pub(super) fn predicate_requires_universe(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Not(_) => true,
        Predicate::And(children) | Predicate::Or(children) => {
            children.iter().any(predicate_requires_universe)
        }
        _ => false,
    }
}

pub(super) fn count_predicate_nodes(
    predicate: &Predicate,
    count: &mut usize,
) -> Result<(), IndexError> {
    *count = count.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                count_predicate_nodes(child, count)?;
            }
        }
        Predicate::Not(child) => count_predicate_nodes(child, count)?,
        _ => {}
    }
    Ok(())
}

pub(super) fn reduce_streaming<'a>(
    operation: AggregateOperation,
    values: impl Iterator<Item = &'a ScalarValue>,
) -> Result<(Option<ScalarValue>, u64), IndexError> {
    let mut count = 0u64;
    let mut value = None;
    let mut average_sum = 0.0;
    for next in values {
        count = count.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        match operation {
            AggregateOperation::Count => {}
            AggregateOperation::Minimum => {
                if value.as_ref().is_none_or(|current| next < current) {
                    value = Some(next.clone());
                }
            }
            AggregateOperation::Maximum => {
                if value.as_ref().is_none_or(|current| next > current) {
                    value = Some(next.clone());
                }
            }
            AggregateOperation::Sum => {
                value = Some(match value.take() {
                    None => next.clone(),
                    Some(current) => sum_pair(current, next)?,
                });
            }
            AggregateOperation::Average => average_sum += scalar_number(next)?,
        }
    }
    let value = match operation {
        AggregateOperation::Count => Some(ScalarValue::Unsigned(count)),
        AggregateOperation::Average if count != 0 => {
            Some(ScalarValue::number(average_sum / count as f64)?)
        }
        AggregateOperation::Average => None,
        _ => value,
    };
    Ok((value, count))
}

fn sum_pair(current: ScalarValue, next: &ScalarValue) -> Result<ScalarValue, IndexError> {
    match (current, next) {
        (ScalarValue::Signed(left), ScalarValue::Signed(right)) => left
            .checked_add(*right)
            .map(ScalarValue::Signed)
            .ok_or(IndexError::OffsetOverflow),
        (ScalarValue::Unsigned(left), ScalarValue::Unsigned(right)) => left
            .checked_add(*right)
            .map(ScalarValue::Unsigned)
            .ok_or(IndexError::OffsetOverflow),
        (current @ ScalarValue::Number(_), ScalarValue::Number(_)) => {
            ScalarValue::number(scalar_number(&current)? + scalar_number(next)?)
        }
        _ => Err(IndexError::InvalidQuery(
            "aggregate scalar types differ".into(),
        )),
    }
}

pub(super) fn leaf_field(predicate: &Predicate) -> Option<FieldId> {
    match predicate {
        Predicate::Equal { field_id, .. }
        | Predicate::In { field_id, .. }
        | Predicate::Prefix { field_id, .. }
        | Predicate::Range { field_id, .. }
        | Predicate::Exists { field_id, .. }
        | Predicate::FullText { field_id, .. }
        | Predicate::Phrase { field_id, .. } => Some(*field_id),
        _ => None,
    }
}

pub(super) fn validate_leaf_capability(
    field: &FieldSchema,
    predicate: &Predicate,
) -> Result<(), IndexError> {
    let required = match predicate {
        Predicate::Equal { .. } | Predicate::In { .. } | Predicate::Exists { .. } => {
            FieldCapabilities::EXACT
        }
        Predicate::Prefix { .. } => FieldCapabilities::PREFIX,
        Predicate::Range { .. } => FieldCapabilities::RANGE,
        Predicate::FullText { .. } | Predicate::Phrase { .. } => FieldCapabilities::FULL_TEXT,
        _ => return Err(IndexError::InvalidQuery("expected leaf predicate".into())),
    };
    if !field.capabilities.contains(required) {
        return Err(IndexError::InvalidQuery(
            "field lacks query capability".into(),
        ));
    }
    let values = match predicate {
        Predicate::Equal { value, .. } => std::slice::from_ref(value),
        Predicate::In { values, .. } => values.as_slice(),
        Predicate::Range { lower, upper, .. } => {
            for bound in lower.iter().chain(upper.iter()) {
                validate_scalar(field.field_type, &bound.value)?;
                if matches!(bound.value, ScalarValue::Null) {
                    return Err(IndexError::InvalidQuery(
                        "range does not accept null".into(),
                    ));
                }
            }
            return Ok(());
        }
        _ => return Ok(()),
    };
    for value in values {
        if matches!(value, ScalarValue::Null) && !field.allow_null {
            return Err(IndexError::InvalidQuery("field does not admit null".into()));
        }
        validate_scalar(field.field_type, value)?;
    }
    Ok(())
}

pub(super) fn requested_value_recipes(
    request: &TypedJsonQueryRequest,
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
) -> Result<BTreeSet<RecipeIdentity>, IndexError> {
    request
        .order
        .iter()
        .map(|field| field.field_id)
        .chain(request.facets.iter().map(|field| field.field_id))
        .chain(request.aggregates.iter().map(|field| field.field_id))
        .map(|id| {
            contracts
                .get(&id)
                .map(|binding| binding.recipe)
                .ok_or_else(|| IndexError::InvalidQuery("query field is not bound".into()))
        })
        .collect()
}

pub(super) fn order_candidates(
    candidates: &mut [QueryCandidate],
    result_limit: usize,
    order: &[OrderField],
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    values: &BTreeMap<
        (
            ProjectionPartitionIdentity,
            StableDocumentKey,
            RecipeIdentity,
        ),
        Option<Vec<ScalarValue>>,
    >,
) -> Result<(), IndexError> {
    for field in order {
        let contract = contracts
            .get(&field.field_id)
            .ok_or_else(|| IndexError::InvalidQuery("order field is not bound".into()))?;
        if contract.field.cardinality != Cardinality::Single
            || !contract
                .field
                .capabilities
                .contains(FieldCapabilities::ORDER)
        {
            return Err(IndexError::InvalidQuery("field cannot order".into()));
        }
    }
    if order.iter().any(|field| {
        let recipe = contracts[&field.field_id].recipe;
        candidates.iter().any(|candidate| {
            values
                .get(&(candidate.partition, candidate.document, recipe))
                .and_then(Option::as_ref)
                .is_some_and(|values| values.len() != 1)
        })
    }) {
        return Err(IndexError::Integrity);
    }
    let compare = |left: &QueryCandidate, right: &QueryCandidate| {
        for field in order {
            let recipe = contracts[&field.field_id].recipe;
            let left_value = values.get(&(left.partition, left.document, recipe));
            let right_value = values.get(&(right.partition, right.document, recipe));
            let comparison = compare_order_value(left_value, right_value);
            let comparison = match field.direction {
                OrderDirection::Ascending => comparison,
                OrderDirection::Descending => comparison.reverse(),
            };
            if comparison != Ordering::Equal {
                return comparison;
            }
        }
        left.document.cmp(&right.document)
    };
    if candidates.len() > result_limit {
        candidates.select_nth_unstable_by(result_limit, compare);
        candidates[..result_limit].sort_by(compare);
    } else {
        candidates.sort_by(compare);
    }
    Ok(())
}

fn compare_order_value(
    left: Option<&Option<Vec<ScalarValue>>>,
    right: Option<&Option<Vec<ScalarValue>>>,
) -> Ordering {
    fn one(value: Option<&Option<Vec<ScalarValue>>>) -> (u8, Option<&ScalarValue>) {
        match value {
            None | Some(None) => (0, None),
            Some(Some(values)) => (1, values.first()),
        }
    }
    one(left).cmp(&one(right))
}

pub(super) fn facet_candidates(
    candidates: &[QueryCandidate],
    requests: &[FacetRequest],
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    values: &BTreeMap<
        (
            ProjectionPartitionIdentity,
            StableDocumentKey,
            RecipeIdentity,
        ),
        Option<Vec<ScalarValue>>,
    >,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Vec<FacetResult>, IndexError> {
    budget.reserve_heap(
        credits,
        requests
            .len()
            .checked_mul(std::mem::size_of::<FacetResult>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut results = Vec::with_capacity(requests.len());
    for request in requests {
        let binding = contracts
            .get(&request.field_id)
            .ok_or_else(|| IndexError::InvalidQuery("facet field is not bound".into()))?;
        if !binding
            .field
            .capabilities
            .contains(FieldCapabilities::FACET)
            || request.limit == 0
        {
            return Err(IndexError::InvalidQuery(
                "field cannot facet or facet limit is zero".into(),
            ));
        }
        let mut counts = BTreeMap::<ScalarValue, u64>::new();
        for candidate in candidates {
            if let Some(Some(document_values)) =
                values.get(&(candidate.partition, candidate.document, binding.recipe))
            {
                let mut previous = None;
                for value in document_values {
                    if previous == Some(value) {
                        continue;
                    }
                    previous = Some(value);
                    if !counts.contains_key(value) {
                        budget.reserve_heap(
                            credits,
                            resident_scalar_bytes(value)
                                .checked_add(std::mem::size_of::<u64>())
                                .ok_or(IndexError::OffsetOverflow)?,
                        )?;
                    }
                    *counts.entry(value.clone()).or_default() += 1;
                }
            }
        }
        budget.reserve_heap(
            credits,
            counts
                .len()
                .checked_mul(std::mem::size_of::<FacetBucket>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut buckets = counts
            .into_iter()
            .map(|(value, count)| FacetBucket { value, count })
            .collect::<Vec<_>>();
        buckets.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.value.cmp(&right.value))
        });
        buckets.truncate(request.limit as usize);
        results.push(FacetResult {
            field_id: request.field_id,
            buckets,
        });
    }
    Ok(results)
}

pub(super) fn aggregate_candidates(
    candidates: &[QueryCandidate],
    requests: &[AggregateRequest],
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    values: &BTreeMap<
        (
            ProjectionPartitionIdentity,
            StableDocumentKey,
            RecipeIdentity,
        ),
        Option<Vec<ScalarValue>>,
    >,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<Vec<AggregateResult>, IndexError> {
    budget.reserve_heap(
        credits,
        requests
            .len()
            .checked_mul(std::mem::size_of::<AggregateResult>())
            .ok_or(IndexError::OffsetOverflow)?,
    )?;
    let mut results = Vec::with_capacity(requests.len());
    for request in requests {
        let binding = contracts
            .get(&request.field_id)
            .ok_or_else(|| IndexError::InvalidQuery("aggregate field is not bound".into()))?;
        if !binding
            .field
            .capabilities
            .contains(FieldCapabilities::AGGREGATE)
        {
            return Err(IndexError::InvalidQuery("field cannot aggregate".into()));
        }
        let selected = candidates
            .iter()
            .flat_map(|candidate| {
                values
                    .get(&(candidate.partition, candidate.document, binding.recipe))
                    .and_then(Option::as_ref)
            })
            .flatten()
            .filter(|value| !matches!(value, ScalarValue::Null));
        let (value, contributing_count) = reduce_streaming(request.operation, selected)?;
        if let Some(value) = &value {
            budget.reserve_heap(credits, resident_scalar_bytes(value))?;
        }
        results.push(AggregateResult {
            field_id: request.field_id,
            operation: request.operation,
            value,
            contributing_count,
        });
    }
    Ok(results)
}

pub(super) fn resident_scalar_bytes(value: &ScalarValue) -> usize {
    std::mem::size_of::<ScalarValue>()
        + match value {
            ScalarValue::String(value) => value.len(),
            _ => 0,
        }
}

pub(super) fn scalar_number(value: &ScalarValue) -> Result<f64, IndexError> {
    match value {
        ScalarValue::Signed(value) => Ok(*value as f64),
        ScalarValue::Unsigned(value) => Ok(*value as f64),
        ScalarValue::Number(_) => Ok(value.as_number().expect("number")),
        _ => Err(IndexError::InvalidQuery(
            "average requires numeric values".into(),
        )),
    }
}

pub(super) fn resource<T>(needed: usize, limit: usize) -> Result<T, IndexError> {
    Err(IndexError::ResourceLimit { needed, limit })
}

pub(super) fn validate_scalar(
    field_type: FieldType,
    value: &ScalarValue,
) -> Result<(), IndexError> {
    let valid = matches!(
        (field_type, value),
        (_, ScalarValue::Null)
            | (FieldType::Boolean, ScalarValue::Boolean(_))
            | (
                FieldType::SignedInteger | FieldType::Date,
                ScalarValue::Signed(_)
            )
            | (FieldType::UnsignedInteger, ScalarValue::Unsigned(_))
            | (FieldType::Float, ScalarValue::Number(_))
            | (FieldType::Keyword | FieldType::Text, ScalarValue::String(_))
    );
    if valid {
        Ok(())
    } else {
        Err(IndexError::InvalidQuery(
            "query scalar type does not match its field".into(),
        ))
    }
}
