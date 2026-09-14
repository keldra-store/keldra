use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use crate::IndexError;
use crate::typed_json::{
    AggregateOperation, AggregateRequest, AggregateResult, Cardinality, FacetBucket, FacetRequest,
    FacetResult, FieldCapabilities, FieldId, FieldSchema, FieldType, OrderDirection, OrderField,
    Predicate, ScalarValue,
};

use super::{
    AuthorizedQueryCandidate, Budget, ProjectionPartitionIdentity, QueryBlockCredits,
    QueryFieldBinding, QueryRunChild, QueryRunPage, RecipeIdentity, StableDocumentKey,
    TypedJsonQueryRequest, resident_authorized_candidate_bytes,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct QueryCandidate {
    pub partition: ProjectionPartitionIdentity,
    pub document: StableDocumentKey,
    pub material_source_version: u64,
}

/// Exclusive continuation boundary for an explicitly ordered query.
/// Values are in request order; `None` is missing, while `Some(Null)` is null.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplicitQuerySearchAfter {
    pub values: Vec<Option<ScalarValue>>,
    pub document: StableDocumentKey,
}

/// Supplies the exact public scalar bytes used to break equal facet counts.
pub trait QueryPublicValueEncoder: Send + Sync {
    fn encode_public_value(
        &self,
        field: &FieldSchema,
        value: &ScalarValue,
    ) -> Result<Vec<u8>, IndexError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ScalarSortKeyValueEncoder;

impl QueryPublicValueEncoder for ScalarSortKeyValueEncoder {
    fn encode_public_value(
        &self,
        _field: &FieldSchema,
        value: &ScalarValue,
    ) -> Result<Vec<u8>, IndexError> {
        crate::typed_json::encode_scalar_sort_key(value)
    }
}

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

pub(super) fn validate_order(
    order: &[OrderField],
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
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
    Ok(())
}

pub(super) struct PartitionValueColumns {
    recipes: Vec<RecipeIdentity>,
    columns: Vec<Vec<Option<Option<Vec<ScalarValue>>>>>,
    resident_bytes: usize,
}

impl PartitionValueColumns {
    pub(super) fn new(recipes: Vec<RecipeIdentity>, resident_bytes: usize) -> Self {
        Self {
            columns: Vec::with_capacity(recipes.len()),
            recipes,
            resident_bytes,
        }
    }

    pub(super) fn push(
        &mut self,
        values: Vec<Option<Option<Vec<ScalarValue>>>>,
        resident_bytes: usize,
    ) -> Result<(), IndexError> {
        if self
            .columns
            .first()
            .is_some_and(|column| column.len() != values.len())
        {
            return Err(IndexError::Integrity);
        }
        self.resident_bytes = self
            .resident_bytes
            .checked_add(resident_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        self.columns.push(values);
        Ok(())
    }

    fn values(&self, recipe: RecipeIdentity, row: usize) -> Option<&[ScalarValue]> {
        let column = self.recipes.binary_search(&recipe).ok()?;
        self.columns.get(column)?.get(row)?.as_ref()?.as_deref()
    }

    pub(super) fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }
}

struct FacetAccumulator {
    field_id: FieldId,
    recipe: RecipeIdentity,
    limit: usize,
    counts: BTreeMap<ScalarValue, (u64, Vec<u8>)>,
}

struct AggregateAccumulator {
    field_id: FieldId,
    recipe: RecipeIdentity,
    operation: AggregateOperation,
    count: u64,
    value: Option<ScalarValue>,
    average_sum: f64,
}

pub(super) struct QueryValueReducers {
    facets: Vec<FacetAccumulator>,
    aggregates: Vec<AggregateAccumulator>,
}

impl QueryValueReducers {
    pub(super) fn new(
        requests: &[FacetRequest],
        aggregates: &[AggregateRequest],
        contracts: &BTreeMap<FieldId, QueryFieldBinding>,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<Self, IndexError> {
        budget.reserve_heap(
            credits,
            requests
                .len()
                .checked_mul(std::mem::size_of::<FacetAccumulator>())
                .and_then(|bytes| {
                    aggregates
                        .len()
                        .checked_mul(std::mem::size_of::<AggregateAccumulator>())
                        .and_then(|aggregate_bytes| bytes.checked_add(aggregate_bytes))
                })
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut facets = Vec::with_capacity(requests.len());
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
            facets.push(FacetAccumulator {
                field_id: request.field_id,
                recipe: binding.recipe,
                limit: request.limit as usize,
                counts: BTreeMap::new(),
            });
        }
        let mut aggregate_states = Vec::with_capacity(aggregates.len());
        for request in aggregates {
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
            aggregate_states.push(AggregateAccumulator {
                field_id: request.field_id,
                recipe: binding.recipe,
                operation: request.operation,
                count: 0,
                value: None,
                average_sum: 0.0,
            });
        }
        Ok(Self {
            facets,
            aggregates: aggregate_states,
        })
    }

    pub(super) fn observe<E: QueryPublicValueEncoder>(
        &mut self,
        row: usize,
        columns: &PartitionValueColumns,
        contracts: &BTreeMap<FieldId, QueryFieldBinding>,
        encoder: &E,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<(), IndexError> {
        for facet in &mut self.facets {
            let Some(values) = columns.values(facet.recipe, row) else {
                continue;
            };
            let field = &contracts[&facet.field_id].field;
            let mut previous = None;
            for value in values {
                if previous == Some(value) {
                    continue;
                }
                previous = Some(value);
                if let Some((count, _)) = facet.counts.get_mut(value) {
                    *count = count.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
                } else {
                    let public_key = encoder.encode_public_value(field, value)?;
                    budget.reserve_heap(
                        credits,
                        resident_scalar_bytes(value)
                            .checked_add(std::mem::size_of::<u64>())
                            .and_then(|bytes| bytes.checked_add(public_key.len()))
                            .ok_or(IndexError::OffsetOverflow)?,
                    )?;
                    facet.counts.insert(value.clone(), (1, public_key));
                }
            }
        }
        for aggregate in &mut self.aggregates {
            let Some(values) = columns.values(aggregate.recipe, row) else {
                continue;
            };
            for value in values
                .iter()
                .filter(|value| !matches!(value, ScalarValue::Null))
            {
                aggregate.count = aggregate
                    .count
                    .checked_add(1)
                    .ok_or(IndexError::OffsetOverflow)?;
                match aggregate.operation {
                    AggregateOperation::Count => {}
                    AggregateOperation::Minimum => {
                        if aggregate
                            .value
                            .as_ref()
                            .is_none_or(|current| value < current)
                        {
                            replace_aggregate_value(&mut aggregate.value, value, credits, budget)?;
                        }
                    }
                    AggregateOperation::Maximum => {
                        if aggregate
                            .value
                            .as_ref()
                            .is_none_or(|current| value > current)
                        {
                            replace_aggregate_value(&mut aggregate.value, value, credits, budget)?;
                        }
                    }
                    AggregateOperation::Sum => {
                        aggregate.value = Some(match aggregate.value.take() {
                            None => value.clone(),
                            Some(current) => sum_pair(current, value)?,
                        });
                    }
                    AggregateOperation::Average => {
                        aggregate.average_sum += scalar_number(value)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(
        self,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<(Vec<FacetResult>, Vec<AggregateResult>), IndexError> {
        let accumulator_bytes = self
            .facets
            .len()
            .checked_mul(std::mem::size_of::<FacetAccumulator>())
            .and_then(|bytes| {
                self.aggregates
                    .len()
                    .checked_mul(std::mem::size_of::<AggregateAccumulator>())
                    .and_then(|aggregate_bytes| bytes.checked_add(aggregate_bytes))
            })
            .ok_or(IndexError::OffsetOverflow)?;
        budget.reserve_heap(
            credits,
            self.facets
                .len()
                .checked_mul(std::mem::size_of::<FacetResult>())
                .and_then(|bytes| {
                    self.aggregates
                        .len()
                        .checked_mul(std::mem::size_of::<AggregateResult>())
                        .and_then(|aggregate_bytes| bytes.checked_add(aggregate_bytes))
                })
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut facets = Vec::with_capacity(self.facets.len());
        for facet in self.facets {
            let count_resident_bytes =
                facet
                    .counts
                    .iter()
                    .try_fold(0usize, |bytes, (value, (_, public_key))| {
                        bytes
                            .checked_add(resident_scalar_bytes(value))
                            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<u64>()))
                            .and_then(|bytes| bytes.checked_add(public_key.len()))
                            .ok_or(IndexError::OffsetOverflow)
                    })?;
            let scratch_bytes = facet
                .counts
                .len()
                .checked_mul(std::mem::size_of::<(FacetBucket, Vec<u8>)>())
                .ok_or(IndexError::OffsetOverflow)?;
            budget.reserve_heap(credits, scratch_bytes)?;
            let mut buckets = facet
                .counts
                .into_iter()
                .map(|(value, (count, public_key))| (FacetBucket { value, count }, public_key))
                .collect::<Vec<_>>();
            buckets.sort_unstable_by(|(left, left_key), (right, right_key)| {
                right
                    .count
                    .cmp(&left.count)
                    .then_with(|| left_key.cmp(right_key))
            });
            buckets.truncate(facet.limit);
            let retained_string_bytes = buckets.iter().try_fold(0usize, |bytes, (bucket, _)| {
                bytes
                    .checked_add(match &bucket.value {
                        ScalarValue::String(value) => value.len(),
                        _ => 0,
                    })
                    .ok_or(IndexError::OffsetOverflow)
            })?;
            budget.reserve_heap(
                credits,
                buckets
                    .len()
                    .checked_mul(std::mem::size_of::<FacetBucket>())
                    .and_then(|bytes| bytes.checked_add(retained_string_bytes))
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
            facets.push(FacetResult {
                field_id: facet.field_id,
                buckets: buckets.into_iter().map(|(bucket, _)| bucket).collect(),
            });
            budget.release_heap(
                credits,
                count_resident_bytes
                    .checked_add(scratch_bytes)
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
        }
        let mut aggregates = Vec::with_capacity(self.aggregates.len());
        for aggregate in self.aggregates {
            let value = match aggregate.operation {
                AggregateOperation::Count => Some(ScalarValue::Unsigned(aggregate.count)),
                AggregateOperation::Average if aggregate.count != 0 => Some(ScalarValue::number(
                    aggregate.average_sum / aggregate.count as f64,
                )?),
                AggregateOperation::Average => None,
                _ => aggregate.value,
            };
            aggregates.push(AggregateResult {
                field_id: aggregate.field_id,
                operation: aggregate.operation,
                value,
                contributing_count: aggregate.count,
            });
        }
        budget.release_heap(credits, accumulator_bytes)?;
        Ok((facets, aggregates))
    }
}

fn replace_aggregate_value(
    current: &mut Option<ScalarValue>,
    next: &ScalarValue,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<(), IndexError> {
    let next_bytes = match next {
        ScalarValue::String(value) => value.len(),
        _ => 0,
    };
    let current_bytes = match current.as_ref() {
        Some(ScalarValue::String(value)) => value.len(),
        _ => 0,
    };
    budget.reserve_heap(credits, next_bytes)?;
    *current = Some(next.clone());
    budget.release_heap(credits, current_bytes)
}

#[derive(Eq, PartialEq)]
struct RankedCandidate {
    sort_key: Vec<u8>,
    values: Vec<Option<ScalarValue>>,
    candidate: AuthorizedQueryCandidate,
}

impl Ord for RankedCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sort_key.cmp(&other.sort_key).then_with(|| {
            self.candidate
                .candidate
                .document
                .cmp(&other.candidate.candidate.document)
        })
    }
}

impl PartialOrd for RankedCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(super) struct BoundedCandidateCollector {
    heap: BinaryHeap<RankedCandidate>,
    limit: usize,
    order: Vec<(RecipeIdentity, OrderDirection)>,
    search_after: Option<(Vec<u8>, super::StableDocumentKey)>,
    heap_cell_bytes: usize,
    metadata_bytes: usize,
    qualifying_count: usize,
}

impl BoundedCandidateCollector {
    pub(super) fn new(
        limit: usize,
        order: &[OrderField],
        contracts: &BTreeMap<FieldId, QueryFieldBinding>,
        search_after: Option<&ExplicitQuerySearchAfter>,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<Self, IndexError> {
        validate_order(order, contracts)?;
        if let Some(cursor) = search_after {
            if cursor.values.len() != order.len() {
                return Err(IndexError::InvalidQuery(
                    "ordered search-after value count differs from order".into(),
                ));
            }
            for (value, ordered) in cursor.values.iter().zip(order) {
                let field = &contracts[&ordered.field_id].field;
                match value {
                    None if !field.allow_missing => {
                        return Err(IndexError::InvalidQuery(
                            "ordered search-after is missing a required field".into(),
                        ));
                    }
                    Some(ScalarValue::Null) if !field.allow_null => {
                        return Err(IndexError::InvalidQuery(
                            "ordered search-after contains a disallowed null".into(),
                        ));
                    }
                    Some(value) => validate_scalar(field.field_type, value)?,
                    None => {}
                }
            }
        }
        let plan = order
            .iter()
            .map(|field| (contracts[&field.field_id].recipe, field.direction))
            .collect::<Vec<_>>();
        let search_after = search_after
            .map(|cursor| {
                Ok((
                    encode_order_key(cursor.values.iter().map(Option::as_ref), &plan)?,
                    cursor.document,
                ))
            })
            .transpose()?;
        let metadata_bytes = plan
            .len()
            .checked_mul(std::mem::size_of::<(RecipeIdentity, OrderDirection)>())
            .and_then(|bytes| {
                bytes.checked_add(search_after.as_ref().map_or(0, |(key, _)| key.len()))
            })
            .ok_or(IndexError::OffsetOverflow)?;
        let heap_cell_bytes = limit
            .checked_mul(std::mem::size_of::<RankedCandidate>())
            .ok_or(IndexError::OffsetOverflow)?;
        budget.reserve_heap(
            credits,
            heap_cell_bytes
                .checked_add(metadata_bytes)
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        Ok(Self {
            heap: BinaryHeap::with_capacity(limit),
            limit,
            order: plan,
            search_after,
            heap_cell_bytes,
            metadata_bytes,
            qualifying_count: 0,
        })
    }

    pub(super) fn observe(
        &mut self,
        candidate: QueryCandidate,
        authorized: AuthorizedQueryCandidate,
        row: usize,
        columns: &PartitionValueColumns,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<(), IndexError> {
        let mut values = Vec::with_capacity(self.order.len());
        for (recipe, _) in &self.order {
            let value = columns.values(*recipe, row);
            if value.is_some_and(|values| values.len() != 1) {
                return Err(IndexError::Integrity);
            }
            values.push(value.and_then(|values| values.first()).cloned());
        }
        let sort_key = encode_order_key(values.iter().map(Option::as_ref), &self.order)?;
        if self.search_after.as_ref().is_some_and(|(key, document)| {
            (sort_key.as_slice(), candidate.document) <= (key.as_slice(), *document)
        }) {
            budget.release_heap(credits, resident_authorized_candidate_bytes(&authorized)?)?;
            return Ok(());
        }
        self.qualifying_count = self
            .qualifying_count
            .checked_add(1)
            .ok_or(IndexError::OffsetOverflow)?;
        if self.heap.len() == self.limit {
            let Some(worst) = self.heap.peek() else {
                return Ok(());
            };
            if (sort_key.as_slice(), candidate.document)
                >= (
                    worst.sort_key.as_slice(),
                    worst.candidate.candidate.document,
                )
            {
                budget.release_heap(credits, resident_authorized_candidate_bytes(&authorized)?)?;
                return Ok(());
            }
            budget.reserve_heap(credits, resident_rank_bytes(&sort_key, &values)?)?;
            let removed = self.heap.pop().ok_or(IndexError::Integrity)?;
            self.heap.push(RankedCandidate {
                sort_key,
                values,
                candidate: authorized,
            });
            budget.release_heap(
                credits,
                resident_rank_bytes(&removed.sort_key, &removed.values)?,
            )?;
            budget.release_heap(
                credits,
                resident_authorized_candidate_bytes(&removed.candidate)?,
            )?;
        } else {
            budget.reserve_heap(credits, resident_rank_bytes(&sort_key, &values)?)?;
            self.heap.push(RankedCandidate {
                sort_key,
                values,
                candidate: authorized,
            });
        }
        Ok(())
    }

    pub(super) fn finish(
        self,
        credits: &mut QueryBlockCredits,
        budget: &mut Budget,
    ) -> Result<
        (
            Vec<AuthorizedQueryCandidate>,
            Option<ExplicitQuerySearchAfter>,
        ),
        IndexError,
    > {
        let mut ranked = self.heap.into_sorted_vec();
        let retained_cursor_bytes = if self.qualifying_count > self.limit {
            ranked
                .last()
                .map(|item| resident_cursor_values(&item.values))
                .transpose()?
                .unwrap_or(0)
        } else {
            0
        };
        let dynamic_bytes = ranked.iter().try_fold(0usize, |bytes, item| {
            bytes
                .checked_add(resident_rank_bytes(&item.sort_key, &item.values)?)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        let next_search_after = (self.qualifying_count > self.limit)
            .then(|| {
                ranked.last_mut().map(|item| ExplicitQuerySearchAfter {
                    values: std::mem::take(&mut item.values),
                    document: item.candidate.candidate.document,
                })
            })
            .flatten();
        let candidates = ranked.into_iter().map(|item| item.candidate).collect();
        budget.release_heap(
            credits,
            self.heap_cell_bytes
                .checked_add(dynamic_bytes)
                .and_then(|bytes| bytes.checked_add(self.metadata_bytes))
                .and_then(|bytes| bytes.checked_sub(retained_cursor_bytes))
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        Ok((candidates, next_search_after))
    }
}

fn resident_cursor_values(values: &[Option<ScalarValue>]) -> Result<usize, IndexError> {
    let slots = values
        .len()
        .checked_mul(std::mem::size_of::<Option<ScalarValue>>())
        .ok_or(IndexError::OffsetOverflow)?;
    values.iter().try_fold(slots, |bytes, value| {
        bytes
            .checked_add(match value {
                Some(ScalarValue::String(value)) => value.len(),
                _ => 0,
            })
            .ok_or(IndexError::OffsetOverflow)
    })
}

fn resident_rank_bytes(
    sort_key: &[u8],
    values: &[Option<ScalarValue>],
) -> Result<usize, IndexError> {
    sort_key
        .len()
        .checked_add(resident_cursor_values(values)?)
        .ok_or(IndexError::OffsetOverflow)
}

fn encode_order_key<'a>(
    values: impl IntoIterator<Item = Option<&'a ScalarValue>>,
    order: &[(RecipeIdentity, OrderDirection)],
) -> Result<Vec<u8>, IndexError> {
    let mut output = Vec::new();
    for (value, (_, direction)) in values.into_iter().zip(order) {
        let mut atom = match value {
            None => vec![0],
            Some(value) => {
                let mut atom = vec![1];
                atom.extend_from_slice(&crate::typed_json::encode_scalar_sort_key(value)?);
                atom
            }
        };
        if *direction == OrderDirection::Descending {
            for byte in &mut atom {
                *byte = !*byte;
            }
        }
        output.extend_from_slice(&atom);
    }
    Ok(output)
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
