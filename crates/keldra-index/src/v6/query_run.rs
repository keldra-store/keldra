//! Semantic oracle for the eventual format-v6 Typed JSON query mini-runs.
//!
//! This deliberately test-only model validates updates, tombstones and Boolean
//! semantics. It is not a production representation: production code must use
//! byte-bounded encoded blocks, lazy caller-loaded cursors, precharged memory,
//! and streaming compaction rather than these whole-run collections.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use crate::IndexError;
use crate::typed_json::{
    AggregateOperation, AggregateResult, FacetBucket, FacetResult, FieldCapabilities, FieldSchema,
    FieldType, OrderDirection, Predicate, RangeBound, ScalarValue, TypedJsonFieldState,
    analyze_typed_json_text, matches_typed_json_leaf,
};

use super::{ProjectionPartitionIdentity, RecipeIdentity, StableDocumentKey};

const MAX_QUERY_RUN_MUTATIONS: usize = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryDocumentGate {
    pub material_source_version: u64,
    pub live: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryFieldContract {
    pub recipe: RecipeIdentity,
    pub field: FieldSchema,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryFieldMutation {
    pub recipe: RecipeIdentity,
    /// `None` is an exact field tombstone for this stable document key.
    pub state: Option<TypedJsonFieldState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryDocumentMutation {
    pub stable_key: StableDocumentKey,
    pub gate: QueryDocumentGate,
    pub fields: Vec<QueryFieldMutation>,
}

/// One immutable query-ready mini-run at an exact source/atomic cut.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionQueryRun {
    pub partition: ProjectionPartitionIdentity,
    pub physical_catalog_generation: [u8; 32],
    pub sequence: u64,
    pub source_start_offset: u64,
    pub next_offset: u64,
    pub through_atomic_position: u64,
    gates: BTreeMap<StableDocumentKey, QueryDocumentGate>,
    fields: BTreeMap<RecipeIdentity, QueryFieldRun>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueryFieldRun {
    contract: FieldSchema,
    /// Newest value emitted by this run. A missing map key means this run did
    /// not modify that document; `None` is an explicit tombstone.
    states: BTreeMap<StableDocumentKey, Option<TypedJsonFieldState>>,
    exact: BTreeMap<ScalarValue, BTreeSet<StableDocumentKey>>,
    points: BTreeMap<ScalarValue, BTreeSet<StableDocumentKey>>,
    text: BTreeMap<String, BTreeMap<StableDocumentKey, Vec<u32>>>,
}

impl ProjectionQueryRun {
    pub fn build(
        partition: ProjectionPartitionIdentity,
        physical_catalog_generation: [u8; 32],
        sequence: u64,
        source_start_offset: u64,
        next_offset: u64,
        through_atomic_position: u64,
        contracts: Vec<QueryFieldContract>,
        mutations: Vec<QueryDocumentMutation>,
    ) -> Result<Self, IndexError> {
        partition.validate()?;
        if physical_catalog_generation == [0; 32]
            || sequence == 0
            || source_start_offset >= next_offset
            || mutations.is_empty()
            || mutations.len() > MAX_QUERY_RUN_MUTATIONS
        {
            return Err(IndexError::InvalidDefinition(
                "projection query run identity or mutation bound is invalid".into(),
            ));
        }
        let mut fields = BTreeMap::new();
        for contract in contracts {
            contract.field.validate()?;
            if fields
                .insert(
                    contract.recipe,
                    QueryFieldRun {
                        contract: contract.field,
                        states: BTreeMap::new(),
                        exact: BTreeMap::new(),
                        points: BTreeMap::new(),
                        text: BTreeMap::new(),
                    },
                )
                .is_some()
            {
                return Err(IndexError::InvalidDefinition(
                    "projection query run has duplicate recipe contracts".into(),
                ));
            }
        }
        let mut gates = BTreeMap::new();
        for mutation in mutations {
            if mutation.gate.material_source_version == 0
                || gates.insert(mutation.stable_key, mutation.gate).is_some()
            {
                return Err(IndexError::InvalidDefinition(
                    "projection query run has invalid document gates".into(),
                ));
            }
            let mut seen = BTreeSet::new();
            for field_mutation in mutation.fields {
                if !seen.insert(field_mutation.recipe) {
                    return Err(IndexError::InvalidDefinition(
                        "projection query run has duplicate field mutations".into(),
                    ));
                }
                let field = fields.get_mut(&field_mutation.recipe).ok_or_else(|| {
                    IndexError::InvalidDefinition("query run field recipe is unknown".into())
                })?;
                if let Some(state) = &field_mutation.state {
                    validate_state(&field.contract, state)?;
                    index_state(field, mutation.stable_key, state)?;
                }
                field
                    .states
                    .insert(mutation.stable_key, field_mutation.state);
            }
        }
        Ok(Self {
            partition,
            physical_catalog_generation,
            sequence,
            source_start_offset,
            next_offset,
            through_atomic_position,
            gates,
            fields,
        })
    }

    pub fn field_contract(&self, recipe: RecipeIdentity) -> Option<&FieldSchema> {
        self.fields.get(&recipe).map(|field| &field.contract)
    }

    pub fn seek_leaf(
        &self,
        recipe: RecipeIdentity,
        predicate: &Predicate,
    ) -> Result<BTreeSet<StableDocumentKey>, IndexError> {
        self.fields
            .get(&recipe)
            .ok_or_else(|| IndexError::InvalidQuery("query recipe is unavailable".into()))?
            .seek(predicate)
    }

    pub fn gate(&self, key: StableDocumentKey) -> Option<QueryDocumentGate> {
        self.gates.get(&key).copied()
    }

    fn state(
        &self,
        recipe: RecipeIdentity,
        key: StableDocumentKey,
    ) -> Option<&Option<TypedJsonFieldState>> {
        self.fields.get(&recipe)?.states.get(&key)
    }

    fn all_gate_keys(&self) -> impl Iterator<Item = StableDocumentKey> + '_ {
        self.gates.keys().copied()
    }
}

impl QueryFieldRun {
    fn seek(&self, predicate: &Predicate) -> Result<BTreeSet<StableDocumentKey>, IndexError> {
        match predicate {
            Predicate::Equal { value, .. } => Ok(self.exact.get(value).cloned().unwrap_or_default()),
            Predicate::In { values, .. } => Ok(values.iter().flat_map(|value| self.exact.get(value).into_iter().flatten().copied()).collect()),
            Predicate::Prefix { prefix, .. } => Ok(self.exact.range(ScalarValue::String(prefix.clone())..).take_while(|(value, _)| matches!(value, ScalarValue::String(value) if value.starts_with(prefix))).flat_map(|(_, keys)| keys.iter().copied()).collect()),
            Predicate::Range { lower, upper, .. } => Ok(self.points.range((range_start(lower), range_end(upper))).flat_map(|(_, keys)| keys.iter().copied()).collect()),
            Predicate::Exists { .. } => Ok(self.states.iter().filter_map(|(key, state)| state.as_ref().filter(|state| state.present).map(|_| *key)).collect()),
            Predicate::FullText { text, .. } | Predicate::Phrase { text, .. } => {
                let terms = analyze_typed_json_text(text);
                let mut terms = terms.into_iter();
                let Some(first) = terms.next() else { return Ok(BTreeSet::new()); };
                let mut candidates: BTreeSet<StableDocumentKey> = self.text.get(&first).map(|entries| entries.keys().copied().collect()).unwrap_or_default();
                for term in terms {
                    let keys = self.text.get(&term).map(|entries| entries.keys().copied().collect()).unwrap_or_default();
                    candidates = candidates.intersection(&keys).copied().collect();
                }
                Ok(candidates)
            }
            Predicate::And(_) | Predicate::Or(_) | Predicate::Not(_) => Err(IndexError::InvalidQuery("query run seek requires a leaf".into())),
        }
    }
}

/// A common-cut, newest-first vector of partition query runs.
pub struct ProjectionQueryView<'a> {
    runs: Vec<&'a ProjectionQueryRun>,
}

impl<'a> ProjectionQueryView<'a> {
    pub fn new(runs: Vec<&'a ProjectionQueryRun>) -> Result<Self, IndexError> {
        let Some(first) = runs.first() else {
            return Err(IndexError::InvalidDefinition(
                "query view has no runs".into(),
            ));
        };
        if runs.iter().any(|run| {
            run.partition != first.partition
                || run.physical_catalog_generation != first.physical_catalog_generation
        }) || runs
            .windows(2)
            .any(|pair| pair[0].sequence <= pair[1].sequence)
        {
            return Err(IndexError::InvalidDefinition(
                "query view is not one newest-first common-cut lineage".into(),
            ));
        }
        Ok(Self { runs })
    }

    pub fn execute(
        &self,
        bindings: &BTreeMap<crate::typed_json::FieldId, RecipeIdentity>,
        contracts: &BTreeMap<RecipeIdentity, FieldSchema>,
        predicate: &Predicate,
    ) -> Result<BTreeSet<StableDocumentKey>, IndexError> {
        predicate.validate()?;
        self.execute_inner(bindings, contracts, predicate)
    }

    fn execute_inner(
        &self,
        bindings: &BTreeMap<crate::typed_json::FieldId, RecipeIdentity>,
        contracts: &BTreeMap<RecipeIdentity, FieldSchema>,
        predicate: &Predicate,
    ) -> Result<BTreeSet<StableDocumentKey>, IndexError> {
        match predicate {
            Predicate::And(children) => {
                let mut children = children.iter();
                let Some(first) = children.next() else {
                    return Err(IndexError::InvalidQuery("AND is empty".into()));
                };
                let mut result = self.execute_inner(bindings, contracts, first)?;
                for child in children {
                    result = result
                        .intersection(&self.execute_inner(bindings, contracts, child)?)
                        .copied()
                        .collect();
                }
                Ok(result)
            }
            Predicate::Or(children) => {
                children
                    .iter()
                    .try_fold(BTreeSet::new(), |mut result, child| {
                        result.extend(self.execute_inner(bindings, contracts, child)?);
                        Ok(result)
                    })
            }
            Predicate::Not(child) => Ok(self
                .live_keys()
                .difference(&self.execute_inner(bindings, contracts, child)?)
                .copied()
                .collect()),
            leaf => self.execute_leaf(bindings, contracts, leaf),
        }
    }

    fn execute_leaf(
        &self,
        bindings: &BTreeMap<crate::typed_json::FieldId, RecipeIdentity>,
        contracts: &BTreeMap<RecipeIdentity, FieldSchema>,
        predicate: &Predicate,
    ) -> Result<BTreeSet<StableDocumentKey>, IndexError> {
        let field_id = leaf_field_id(predicate).expect("leaf");
        let recipe = *bindings
            .get(&field_id)
            .ok_or_else(|| IndexError::InvalidQuery("query binding is missing".into()))?;
        let field = contracts
            .get(&recipe)
            .ok_or_else(|| IndexError::InvalidQuery("query contract is missing".into()))?;
        let candidates = self
            .runs
            .iter()
            .try_fold(BTreeSet::new(), |mut output, run| {
                output.extend(run.seek_leaf(recipe, predicate)?);
                Ok::<_, IndexError>(output)
            })?;
        let mut accepted = BTreeSet::new();
        for key in candidates {
            if !self.gate(key).is_some_and(|gate| gate.live) {
                continue;
            }
            if let Some(state) = self.state(recipe, key) {
                if matches_typed_json_leaf(field, state, predicate)? {
                    accepted.insert(key);
                }
            }
        }
        Ok(accepted)
    }

    pub fn state(
        &self,
        recipe: RecipeIdentity,
        key: StableDocumentKey,
    ) -> Option<&TypedJsonFieldState> {
        self.runs
            .iter()
            .find_map(|run| run.state(recipe, key))
            .and_then(Option::as_ref)
    }

    pub fn gate(&self, key: StableDocumentKey) -> Option<QueryDocumentGate> {
        self.runs.iter().find_map(|run| run.gate(key))
    }

    pub fn live_keys(&self) -> BTreeSet<StableDocumentKey> {
        self.runs
            .iter()
            .flat_map(|run| run.all_gate_keys())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|key| self.gate(*key).is_some_and(|gate| gate.live))
            .collect()
    }

    pub fn facet(
        &self,
        recipe: RecipeIdentity,
        candidates: &BTreeSet<StableDocumentKey>,
        limit: u32,
    ) -> Result<FacetResult, IndexError> {
        let field = self.contract(recipe)?;
        let mut buckets = BTreeMap::<ScalarValue, u64>::new();
        for key in candidates {
            if let Some(state) = self.state(recipe, *key) {
                for value in state_values(field, state)? {
                    *buckets.entry(value).or_default() += 1;
                }
            }
        }
        let mut buckets = buckets
            .into_iter()
            .map(|(value, count)| FacetBucket { value, count })
            .collect::<Vec<_>>();
        buckets.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.value.cmp(&right.value))
        });
        buckets.truncate(limit as usize);
        Ok(FacetResult {
            field_id: field.id,
            buckets,
        })
    }

    pub fn aggregate(
        &self,
        recipe: RecipeIdentity,
        candidates: &BTreeSet<StableDocumentKey>,
        operation: AggregateOperation,
    ) -> Result<AggregateResult, IndexError> {
        let field = self.contract(recipe)?;
        if !field.capabilities.contains(FieldCapabilities::AGGREGATE) {
            return Err(IndexError::InvalidQuery("field cannot aggregate".into()));
        }
        let values = candidates
            .iter()
            .filter_map(|key| self.state(recipe, *key))
            .flat_map(|state| state.values.clone())
            .collect::<Vec<_>>();
        let value = reduce(operation, &values)?;
        Ok(AggregateResult {
            field_id: field.id,
            operation,
            value,
            contributing_count: values.len() as u64,
        })
    }

    fn contract(&self, recipe: RecipeIdentity) -> Result<&FieldSchema, IndexError> {
        self.runs
            .iter()
            .find_map(|run| run.field_contract(recipe))
            .ok_or_else(|| IndexError::InvalidQuery("query recipe is unavailable".into()))
    }

    /// Order already-selected Boolean candidates without revisiting source
    /// bytes. Each requested recipe must name a single-valued ORDER field.
    pub fn order(
        &self,
        candidates: &BTreeSet<StableDocumentKey>,
        fields: &[(RecipeIdentity, OrderDirection)],
    ) -> Result<Vec<StableDocumentKey>, IndexError> {
        for (recipe, _) in fields {
            let field = self.contract(*recipe)?;
            if field.cardinality != crate::typed_json::Cardinality::Single
                || !field.capabilities.contains(FieldCapabilities::ORDER)
            {
                return Err(IndexError::InvalidQuery("field cannot order".into()));
            }
        }
        let mut ordered = candidates.iter().copied().collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            for (recipe, direction) in fields {
                let comparison =
                    compare_order_state(self.state(*recipe, *left), self.state(*recipe, *right));
                let comparison = match direction {
                    OrderDirection::Ascending => comparison,
                    OrderDirection::Descending => comparison.reverse(),
                };
                if comparison != std::cmp::Ordering::Equal {
                    return comparison;
                }
            }
            left.cmp(right)
        });
        Ok(ordered)
    }
}

/// Compact a newest-first run vector into one whole query run. It resolves
/// gates and field states before rebuilding postings, so stale L0 candidates
/// cannot survive the output run.
pub fn compact_projection_query_runs(
    partition: ProjectionPartitionIdentity,
    physical_catalog_generation: [u8; 32],
    sequence: u64,
    source_start_offset: u64,
    next_offset: u64,
    through_atomic_position: u64,
    contracts: Vec<QueryFieldContract>,
    newest_first: &[ProjectionQueryRun],
) -> Result<ProjectionQueryRun, IndexError> {
    let view = ProjectionQueryView::new(newest_first.iter().collect())?;
    let keys = view.live_keys();
    let recipes = contracts
        .iter()
        .map(|contract| contract.recipe)
        .collect::<Vec<_>>();
    let mutations = keys
        .into_iter()
        .map(|stable_key| QueryDocumentMutation {
            stable_key,
            gate: view.gate(stable_key).expect("live key has a gate"),
            fields: recipes
                .iter()
                .filter_map(|recipe| {
                    view.state(*recipe, stable_key)
                        .cloned()
                        .map(|state| QueryFieldMutation {
                            recipe: *recipe,
                            state: Some(state),
                        })
                })
                .collect(),
        })
        .collect();
    ProjectionQueryRun::build(
        partition,
        physical_catalog_generation,
        sequence,
        source_start_offset,
        next_offset,
        through_atomic_position,
        contracts,
        mutations,
    )
}

fn index_state(
    field: &mut QueryFieldRun,
    key: StableDocumentKey,
    state: &TypedJsonFieldState,
) -> Result<(), IndexError> {
    if !state.present {
        return Ok(());
    }
    if state.null {
        field
            .exact
            .entry(ScalarValue::Null)
            .or_default()
            .insert(key);
    }
    for value in &state.values {
        if field
            .contract
            .capabilities
            .contains(crate::typed_json::FieldCapabilities::EXACT)
            || field
                .contract
                .capabilities
                .contains(crate::typed_json::FieldCapabilities::PREFIX)
        {
            field.exact.entry(value.clone()).or_default().insert(key);
        }
        if field
            .contract
            .capabilities
            .contains(crate::typed_json::FieldCapabilities::RANGE)
        {
            field.points.entry(value.clone()).or_default().insert(key);
        }
        if field.contract.field_type == FieldType::Text {
            if let ScalarValue::String(text) = value {
                for (position, term) in analyze_typed_json_text(text).into_iter().enumerate() {
                    field
                        .text
                        .entry(term)
                        .or_default()
                        .entry(key)
                        .or_default()
                        .push(u32::try_from(position).map_err(|_| IndexError::OffsetOverflow)?);
                }
            }
        }
    }
    Ok(())
}

fn validate_state(field: &FieldSchema, state: &TypedJsonFieldState) -> Result<(), IndexError> {
    crate::typed_json::encode_typed_json_field_state(field, state).map(|_| ())
}

fn leaf_field_id(predicate: &Predicate) -> Option<crate::typed_json::FieldId> {
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

fn range_start(bound: &Option<RangeBound>) -> Bound<ScalarValue> {
    match bound {
        None => Bound::Unbounded,
        Some(bound) if bound.inclusive => Bound::Included(bound.value.clone()),
        Some(bound) => Bound::Excluded(bound.value.clone()),
    }
}
fn range_end(bound: &Option<RangeBound>) -> Bound<ScalarValue> {
    match bound {
        None => Bound::Unbounded,
        Some(bound) if bound.inclusive => Bound::Included(bound.value.clone()),
        Some(bound) => Bound::Excluded(bound.value.clone()),
    }
}

fn state_values(
    field: &FieldSchema,
    state: &TypedJsonFieldState,
) -> Result<Vec<ScalarValue>, IndexError> {
    if !field
        .capabilities
        .contains(crate::typed_json::FieldCapabilities::FACET)
    {
        return Err(IndexError::InvalidQuery("field cannot facet".into()));
    }
    let mut values = state.values.clone();
    if state.null {
        values.push(ScalarValue::Null);
    }
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

fn compare_order_state(
    left: Option<&TypedJsonFieldState>,
    right: Option<&TypedJsonFieldState>,
) -> std::cmp::Ordering {
    fn value(state: Option<&TypedJsonFieldState>) -> (u8, Option<&ScalarValue>) {
        match state {
            None | Some(TypedJsonFieldState { present: false, .. }) => (0, None),
            Some(TypedJsonFieldState { null: true, .. }) => (1, None),
            Some(state) => (2, state.values.first()),
        }
    }
    let left = value(left);
    let right = value(right);
    left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
}

fn reduce(
    operation: AggregateOperation,
    values: &[ScalarValue],
) -> Result<Option<ScalarValue>, IndexError> {
    if operation == AggregateOperation::Count {
        return Ok(Some(ScalarValue::Unsigned(values.len() as u64)));
    }
    let Some(first) = values.first() else {
        return Ok(None);
    };
    match operation {
        AggregateOperation::Minimum => Ok(values.iter().min().cloned()),
        AggregateOperation::Maximum => Ok(values.iter().max().cloned()),
        AggregateOperation::Sum => match first {
            ScalarValue::Signed(_) => values
                .iter()
                .try_fold(0_i64, |sum, value| match value {
                    ScalarValue::Signed(value) => {
                        sum.checked_add(*value).ok_or(IndexError::OffsetOverflow)
                    }
                    _ => Err(IndexError::InvalidQuery(
                        "aggregate scalar types differ".into(),
                    )),
                })
                .map(ScalarValue::Signed)
                .map(Some),
            ScalarValue::Unsigned(_) => values
                .iter()
                .try_fold(0_u64, |sum, value| match value {
                    ScalarValue::Unsigned(value) => {
                        sum.checked_add(*value).ok_or(IndexError::OffsetOverflow)
                    }
                    _ => Err(IndexError::InvalidQuery(
                        "aggregate scalar types differ".into(),
                    )),
                })
                .map(ScalarValue::Unsigned)
                .map(Some),
            ScalarValue::Number(_) => {
                ScalarValue::number(values.iter().try_fold(0.0_f64, |sum, value| {
                    value.as_number().map(|number| sum + number).ok_or_else(|| {
                        IndexError::InvalidQuery("aggregate scalar types differ".into())
                    })
                })?)
                .map(Some)
            }
            _ => Err(IndexError::InvalidQuery(
                "sum requires numeric values".into(),
            )),
        },
        AggregateOperation::Average => ScalarValue::number(
            values.iter().try_fold(0.0_f64, |sum, value| match value {
                ScalarValue::Signed(value) => Ok(sum + *value as f64),
                ScalarValue::Unsigned(value) => Ok(sum + *value as f64),
                ScalarValue::Number(_) => Ok(sum + value.as_number().expect("number")),
                _ => Err(IndexError::InvalidQuery(
                    "average requires numeric values".into(),
                )),
            })? / values.len() as f64,
        )
        .map(Some),
        AggregateOperation::Count => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_json::{
        Cardinality, Collation, FieldCapabilities, FieldId, OrderDirection, PredicateId,
    };

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap()
    }

    fn keyword(id: u32) -> FieldSchema {
        FieldSchema {
            id: FieldId::new(id),
            name: "status".into(),
            source_selector: "/status".into(),
            field_type: FieldType::Keyword,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::EXACT
                .union(FieldCapabilities::PREFIX)
                .union(FieldCapabilities::FACET)
                .union(FieldCapabilities::ORDER),
            analyzer: None,
            date_format: None,
        }
    }

    fn number(id: u32) -> FieldSchema {
        FieldSchema {
            id: FieldId::new(id),
            name: "score".into(),
            source_selector: "/score".into(),
            field_type: FieldType::SignedInteger,
            cardinality: Cardinality::Single,
            allow_missing: true,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            capabilities: FieldCapabilities::EXACT
                .union(FieldCapabilities::RANGE)
                .union(FieldCapabilities::FACET)
                .union(FieldCapabilities::AGGREGATE)
                .union(FieldCapabilities::ORDER),
            analyzer: None,
            date_format: None,
        }
    }

    fn state(value: ScalarValue) -> TypedJsonFieldState {
        TypedJsonFieldState::from_selected(&number(1), Some(vec![value])).unwrap()
    }

    #[test]
    fn seek_boolean_order_facet_aggregate_updates_deletes_and_compaction() {
        let status_recipe = RecipeIdentity::new([7; 32]).unwrap();
        let score_recipe = RecipeIdentity::new([8; 32]).unwrap();
        let contracts = vec![
            QueryFieldContract {
                recipe: status_recipe,
                field: keyword(0),
            },
            QueryFieldContract {
                recipe: score_recipe,
                field: number(1),
            },
        ];
        let one = StableDocumentKey::from_bytes([1; 32]).unwrap();
        let two = StableDocumentKey::from_bytes([2; 32]).unwrap();
        let run_one = ProjectionQueryRun::build(
            partition(),
            [9; 32],
            1,
            0,
            1,
            1,
            contracts.clone(),
            vec![
                QueryDocumentMutation {
                    stable_key: one,
                    gate: QueryDocumentGate {
                        material_source_version: 1,
                        live: true,
                    },
                    fields: vec![
                        QueryFieldMutation {
                            recipe: status_recipe,
                            state: Some(
                                TypedJsonFieldState::from_selected(
                                    &keyword(0),
                                    Some(vec![ScalarValue::String("active".into())]),
                                )
                                .unwrap(),
                            ),
                        },
                        QueryFieldMutation {
                            recipe: score_recipe,
                            state: Some(state(ScalarValue::Signed(10))),
                        },
                    ],
                },
                QueryDocumentMutation {
                    stable_key: two,
                    gate: QueryDocumentGate {
                        material_source_version: 1,
                        live: true,
                    },
                    fields: vec![
                        QueryFieldMutation {
                            recipe: status_recipe,
                            state: Some(
                                TypedJsonFieldState::from_selected(
                                    &keyword(0),
                                    Some(vec![ScalarValue::String("active".into())]),
                                )
                                .unwrap(),
                            ),
                        },
                        QueryFieldMutation {
                            recipe: score_recipe,
                            state: Some(state(ScalarValue::Signed(30))),
                        },
                    ],
                },
            ],
        )
        .unwrap();
        let run_two = ProjectionQueryRun::build(
            partition(),
            [9; 32],
            2,
            1,
            2,
            2,
            contracts.clone(),
            vec![QueryDocumentMutation {
                stable_key: one,
                gate: QueryDocumentGate {
                    material_source_version: 2,
                    live: true,
                },
                fields: vec![QueryFieldMutation {
                    recipe: score_recipe,
                    state: Some(state(ScalarValue::Signed(20))),
                }],
            }],
        )
        .unwrap();
        let view = ProjectionQueryView::new(vec![&run_two, &run_one]).unwrap();
        let bindings = BTreeMap::from([
            (FieldId::new(0), status_recipe),
            (FieldId::new(1), score_recipe),
        ]);
        let contracts_by_recipe = contracts
            .iter()
            .map(|contract| (contract.recipe, contract.field.clone()))
            .collect();
        let predicate = Predicate::And(vec![
            Predicate::Equal {
                id: PredicateId::new(1),
                field_id: FieldId::new(0),
                value: ScalarValue::String("active".into()),
            },
            Predicate::Range {
                id: PredicateId::new(2),
                field_id: FieldId::new(1),
                lower: Some(RangeBound {
                    value: ScalarValue::Signed(15),
                    inclusive: false,
                }),
                upper: Some(RangeBound {
                    value: ScalarValue::Signed(25),
                    inclusive: true,
                }),
            },
        ]);
        let candidates = view
            .execute(&bindings, &contracts_by_recipe, &predicate)
            .unwrap();
        assert_eq!(candidates, BTreeSet::from([one]));
        assert_eq!(
            view.order(
                &BTreeSet::from([one, two]),
                &[(score_recipe, OrderDirection::Ascending)]
            )
            .unwrap(),
            vec![one, two]
        );
        assert_eq!(
            view.facet(score_recipe, &candidates, 10).unwrap().buckets,
            vec![FacetBucket {
                value: ScalarValue::Signed(20),
                count: 1
            }]
        );
        assert_eq!(
            view.aggregate(score_recipe, &candidates, AggregateOperation::Sum)
                .unwrap()
                .value,
            Some(ScalarValue::Signed(20))
        );
        let compacted = compact_projection_query_runs(
            partition(),
            [9; 32],
            3,
            0,
            3,
            3,
            contracts.clone(),
            &[run_two.clone(), run_one.clone()],
        )
        .unwrap();
        let compacted_view = ProjectionQueryView::new(vec![&compacted]).unwrap();
        assert_eq!(
            compacted_view
                .execute(&bindings, &contracts_by_recipe, &predicate)
                .unwrap(),
            candidates
        );
        let delete = ProjectionQueryRun::build(
            partition(),
            [9; 32],
            4,
            3,
            4,
            4,
            contracts,
            vec![QueryDocumentMutation {
                stable_key: one,
                gate: QueryDocumentGate {
                    material_source_version: 3,
                    live: false,
                },
                fields: vec![
                    QueryFieldMutation {
                        recipe: status_recipe,
                        state: None,
                    },
                    QueryFieldMutation {
                        recipe: score_recipe,
                        state: None,
                    },
                ],
            }],
        )
        .unwrap();
        let deleted_view = ProjectionQueryView::new(vec![&delete, &run_two, &run_one]).unwrap();
        assert!(
            deleted_view
                .execute(&bindings, &contracts_by_recipe, &predicate)
                .unwrap()
                .is_empty()
        );
    }
}
