use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use crate::IndexError;

use super::super::{
    ArtifactDirectoryRead, CandidateGate, CandidateReference, DocId, FieldId,
    INDEX_GENERATION_SEGMENTS, NativeQuery, NativeQueryCursor, NativeQueryExecutionTier,
    NativeQueryHit, NativeQueryPage, NativeQueryRequest, NativeQueryStatisticsRecorder,
    ObjectIdentity, OrderDirection, OrderField, ScalarValue, SegmentComponentReader, SortValue,
};
use super::plan::{SegmentPlan, plan_segment};
use super::posting::DocCursor;
use super::query_semantics::{
    physical_order, query_directions, scoring_query_vector, text_scoring_active,
};
use super::score::{GlobalTextStatistics, SegmentScorer};
use super::values::SegmentValues;

mod impact;
mod compute;
mod result;

pub const MAXIMUM_CANDIDATE_GATE_BATCH: usize = 256;

#[derive(Debug)]
pub enum NativeQueryExecutionError<E> {
    Index(IndexError),
    Gate(E),
}

impl<E> From<IndexError> for NativeQueryExecutionError<E> {
    fn from(error: IndexError) -> Self {
        Self::Index(error)
    }
}

impl<E: fmt::Display> fmt::Display for NativeQueryExecutionError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Index(error) => error.fmt(formatter),
            Self::Gate(error) => error.fmt(formatter),
        }
    }
}

impl<E: Error + 'static> Error for NativeQueryExecutionError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Index(error) => Some(error),
            Self::Gate(error) => Some(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeQueryLimits {
    pub maximum_segments: usize,
    pub maximum_expanded_terms: usize,
    pub maximum_result_limit: usize,
    pub candidate_gate_batch: usize,
    /// Maximum retained bytes for one native result page and its owned values.
    pub maximum_page_bytes: usize,
}

impl Default for NativeQueryLimits {
    fn default() -> Self {
        Self {
            maximum_segments: INDEX_GENERATION_SEGMENTS,
            maximum_expanded_terms: 64,
            maximum_result_limit: 100_000,
            candidate_gate_batch: MAXIMUM_CANDIDATE_GATE_BATCH,
            maximum_page_bytes: super::memory::DEFAULT_MAXIMUM_PAGE_BYTES,
        }
    }
}

impl NativeQueryLimits {
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.maximum_segments == 0
            || self.maximum_segments > INDEX_GENERATION_SEGMENTS
            || self.maximum_expanded_terms == 0
            || self.maximum_result_limit == 0
            || self.candidate_gate_batch == 0
            || self.candidate_gate_batch > MAXIMUM_CANDIDATE_GATE_BATCH
            || self.maximum_page_bytes == 0
        {
            return Err(IndexError::InvalidDefinition(
                "native query limits are zero or exceed a fixed format bound".into(),
            ));
        }
        Ok(())
    }
}

pub struct NativeQueryExecutor<'a, D, G> {
    directory: &'a D,
    gate: &'a G,
    limits: NativeQueryLimits,
}

impl<'a, D, G> NativeQueryExecutor<'a, D, G>
where
    D: ArtifactDirectoryRead,
    G: CandidateGate,
{
    pub fn new(
        directory: &'a D,
        gate: &'a G,
        limits: NativeQueryLimits,
    ) -> Result<Self, IndexError> {
        limits.validate()?;
        Ok(Self {
            directory,
            gate,
            limits,
        })
    }

    pub fn working_memory_bytes(&self, request: &NativeQueryRequest) -> Result<usize, IndexError> {
        super::memory::estimate_working_memory(request, self.limits)
    }

    pub async fn execute<'query>(
        &'query self,
        request: &'query NativeQueryRequest,
    ) -> Result<NativeQueryPage, NativeQueryExecutionError<G::Error>> {
        self.execute_observed(request, NativeQueryStatisticsRecorder::new())
            .await
    }

    /// Execute with a process-local recorder that remains observable if the
    /// calling future is cancelled before a page is returned.
    #[doc(hidden)]
    pub async fn execute_observed<'query>(
        &'query self,
        request: &'query NativeQueryRequest,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Result<NativeQueryPage, NativeQueryExecutionError<G::Error>> {
        request.validate()?;
        if request.segments.len() > self.limits.maximum_segments
            || request.limit as usize > self.limits.maximum_result_limit
        {
            return Err(IndexError::ResourceLimit {
                needed: request.segments.len().max(request.limit as usize),
                limit: self
                    .limits
                    .maximum_segments
                    .max(self.limits.maximum_result_limit),
            }
            .into());
        }
        self.working_memory_bytes(request)?;
        let scoring_query_vector = scoring_query_vector(&request.schema, &request.query)?;
        let text_scoring = text_scoring_active(&request.query);
        let physical = physical_order(request).is_some();
        statistics.record_tier(if physical {
            NativeQueryExecutionTier::Physical
        } else {
            NativeQueryExecutionTier::TopK
        });

        let mut global = GlobalTextStatistics::default();
        let mut executions = Vec::with_capacity(request.segments.len());
        for (segment_index, segment) in request.segments.iter().enumerate() {
            let plan = plan_segment(
                self.directory,
                segment,
                &request.schema,
                &request.query,
                self.limits.maximum_expanded_terms,
                &statistics,
            )
            .await?;
            if text_scoring {
                global.add_segment(
                    &SegmentComponentReader::new(self.directory, segment)?
                        .statistics()
                        .await?,
                    &plan.text_terms,
                )?;
            }
            executions.push(SegmentExecution::new(
                self.directory,
                segment_index,
                segment,
                plan,
                statistics.clone(),
            )?);
        }

        let selected = if physical {
            self.execute_physical(request, &mut executions, &statistics)
                .await?
        } else {
            self.execute_top_k(
                request,
                &global,
                scoring_query_vector.as_ref(),
                &mut executions,
                &statistics,
            )
            .await?
        };
        let (facet_results, aggregate_results) = self
            .compute(request, &statistics)
            .await?;
        result::materialize(
            request,
            executions.as_mut_slice(),
            selected,
            facet_results,
            aggregate_results,
            self.limits.maximum_page_bytes,
            &statistics,
        )
        .await
        .map_err(NativeQueryExecutionError::Index)
    }

    async fn compute<'query>(
        &'query self,
        request: &'query NativeQueryRequest,
        statistics: &NativeQueryStatisticsRecorder,
    ) -> Result<
        (
            Vec<super::super::FacetResult>,
            Vec<super::super::AggregateResult>,
        ),
        NativeQueryExecutionError<G::Error>,
    > {
        if request.facets.is_empty() && request.aggregates.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut state = compute::ComputationState::new(request, self.limits.maximum_page_bytes)?;
        for (segment_index, segment) in request.segments.iter().enumerate() {
            let plan = plan_segment(
                self.directory,
                segment,
                &request.schema,
                &request.query,
                self.limits.maximum_expanded_terms,
                statistics,
            )
            .await?;
            let mut execution = SegmentExecution::new(
                self.directory,
                segment_index,
                segment,
                plan,
                statistics.clone(),
            )?;
            let mut pending = Vec::with_capacity(self.limits.candidate_gate_batch);
            loop {
                while pending.len() < self.limits.candidate_gate_batch {
                    let Some(candidate) = execution.next_unranked().await? else {
                        break;
                    };
                    pending.push(candidate);
                }
                if pending.is_empty() {
                    break;
                }
                let references = pending
                    .iter()
                    .map(|candidate| CandidateReference {
                        source: candidate.identity.source.clone(),
                        result: candidate.identity.result_or_source().clone(),
                    })
                    .collect::<Vec<_>>();
                let evidence = self
                    .gate
                    .evaluate(&references)
                    .await
                    .map_err(NativeQueryExecutionError::Gate)?;
                result::validate_gate(request, references.len(), &evidence, statistics)?;
                for (candidate, visible) in pending.drain(..).zip(evidence.visible) {
                    if visible {
                        state.observe(&mut execution.values, candidate.doc_id).await?;
                    }
                }
            }
            execution.release_decoded()?;
        }
        state.finish().map_err(NativeQueryExecutionError::Index)
    }

    async fn execute_top_k<'query>(
        &'query self,
        request: &NativeQueryRequest,
        global: &GlobalTextStatistics,
        scoring_query_vector: Option<&Arc<[f32]>>,
        executions: &mut [SegmentExecution<'query, D>],
        statistics: &NativeQueryStatisticsRecorder,
    ) -> Result<Vec<Selected>, NativeQueryExecutionError<G::Error>> {
        let directions = Arc::<[OrderDirection]>::from(query_directions(request));
        let mut heap = BinaryHeap::new();
        for execution in executions.iter_mut() {
            let mut pending = Vec::with_capacity(self.limits.candidate_gate_batch);
            while let Some(candidate) = execution
                .next_competitive(request, global, impact::threshold(request, &heap))
                .await?
            {
                statistics.top_k_inspected();
                pending.push(candidate);
                if pending.len() == self.limits.candidate_gate_batch {
                    self.rank_batch(
                        request,
                        global,
                        scoring_query_vector,
                        execution,
                        &directions,
                        &mut pending,
                        &mut heap,
                        statistics,
                    )
                    .await?;
                }
            }
            if !pending.is_empty() {
                self.rank_batch(
                    request,
                    global,
                    scoring_query_vector,
                    execution,
                    &directions,
                    &mut pending,
                    &mut heap,
                    statistics,
                )
                .await?;
            }
            execution.release_decoded()?;
        }
        let mut values = heap.into_vec();
        values.sort_by(compare_selected);
        Ok(values)
    }

    #[allow(clippy::too_many_arguments)]
    async fn rank_batch<'query>(
        &'query self,
        request: &NativeQueryRequest,
        global: &GlobalTextStatistics,
        scoring_query_vector: Option<&Arc<[f32]>>,
        execution: &mut SegmentExecution<'query, D>,
        directions: &Arc<[OrderDirection]>,
        pending: &mut Vec<Unranked>,
        heap: &mut BinaryHeap<Selected>,
        statistics: &NativeQueryStatisticsRecorder,
    ) -> Result<(), NativeQueryExecutionError<G::Error>> {
        let references = pending
            .iter()
            .map(|candidate| CandidateReference {
                source: candidate.identity.source.clone(),
                result: candidate.identity.result_or_source().clone(),
            })
            .collect::<Vec<_>>();
        let evidence = self
            .gate
            .evaluate(&references)
            .await
            .map_err(NativeQueryExecutionError::Gate)?;
        result::validate_gate(request, references.len(), &evidence, statistics)?;
        for (candidate, visible) in pending.drain(..).zip(evidence.visible) {
            if !visible {
                continue;
            }
            let score = execution
                .scorer
                .score(
                    &request.schema,
                    &request.query,
                    scoring_query_vector,
                    global,
                    &mut execution.values,
                    candidate.doc_id,
                )
                .await?;
            if matches!(
                &request.query,
                NativeQuery::Vector { .. } | NativeQuery::Hybrid { .. }
            ) && score.is_none()
            {
                continue;
            }
            let sort_values = rank_values(
                request,
                &mut execution.values,
                candidate.doc_id,
                candidate.identity.result_or_source(),
                score,
            )
            .await?;
            let result = candidate.identity.result_or_source().clone();
            let source_record = candidate.identity.source_record;
            let selected = Selected {
                segment_index: execution.segment_index,
                doc_id: candidate.doc_id,
                source: candidate.identity.source,
                source_record,
                result,
                score,
                sort_values,
                directions: directions.clone(),
            };
            if request
                .after
                .as_ref()
                .is_some_and(|cursor| compare_to_cursor(&selected, cursor) != Ordering::Greater)
            {
                continue;
            }
            heap.push(selected);
            if heap.len() > request.limit as usize {
                heap.pop();
            }
        }
        Ok(())
    }

    async fn execute_physical<'query>(
        &'query self,
        request: &NativeQueryRequest,
        executions: &mut [SegmentExecution<'query, D>],
        statistics: &NativeQueryStatisticsRecorder,
    ) -> Result<Vec<Selected>, NativeQueryExecutionError<G::Error>> {
        let order = physical_order(request).expect("checked physical plan");
        let directions = Arc::<[OrderDirection]>::from(query_directions(request));
        let after = physical_after(request, &directions)?;
        let mut heads = Vec::with_capacity(executions.len());
        for execution in executions.iter_mut() {
            if let Some(after) = after.as_ref() {
                execution
                    .seek_after(request, order, after, &directions)
                    .await?;
            }
            heads.push(
                execution
                    .next_physical(request, order, directions.clone())
                    .await?,
            );
            // Only the advancing segment below retains decoded components.
            execution.release_decoded()?;
        }
        let mut retained_segment: Option<usize> = None;
        let mut selected = Vec::with_capacity(request.limit as usize);
        let mut refill_required = false;
        while selected.len() < request.limit as usize {
            let batch_target = self
                .limits
                .candidate_gate_batch
                .min((request.limit as usize).saturating_sub(selected.len()));
            let mut pending = Vec::with_capacity(batch_target);
            while pending.len() < batch_target {
                let Some(index) = minimum_head(&heads) else {
                    break;
                };
                pending.push(heads[index].take().unwrap());
                if retained_segment != Some(index) {
                    if let Some(previous) = retained_segment.take() {
                        executions[previous].release_decoded()?;
                    }
                    retained_segment = Some(index);
                }
                heads[index] = executions[index]
                    .next_physical(request, order, directions.clone())
                    .await?;
                if heads[index].is_none() {
                    executions[index].release_decoded()?;
                    retained_segment = None;
                }
            }
            if pending.is_empty() {
                break;
            }
            if refill_required {
                statistics.candidate_gate_refill();
            }
            let references = pending
                .iter()
                .map(|candidate| CandidateReference {
                    source: candidate.source.clone(),
                    result: candidate.result.clone(),
                })
                .collect::<Vec<_>>();
            let evidence = self
                .gate
                .evaluate(&references)
                .await
                .map_err(NativeQueryExecutionError::Gate)?;
            result::validate_gate(request, references.len(), &evidence, statistics)?;
            let rejected = evidence.denied.saturating_add(evidence.stale);
            for (candidate, visible) in pending.into_iter().zip(evidence.visible) {
                if visible {
                    selected.push(candidate);
                    if selected.len() == request.limit as usize {
                        break;
                    }
                }
            }
            refill_required = rejected != 0 && selected.len() < request.limit as usize;
        }
        if selected.len() == request.limit as usize {
            statistics.physical_early_termination();
        }
        if let Some(index) = retained_segment {
            executions[index].release_decoded()?;
        }
        Ok(selected)
    }
}

struct SegmentExecution<'a, D> {
    segment_index: usize,
    segment: &'a super::super::SegmentDescriptor,
    cursor: DocCursor<'a, D>,
    exact_filter: Option<&'a super::super::Predicate>,
    scorer: SegmentScorer<'a, D>,
    values: SegmentValues<'a, D>,
    prefetched: Option<DocId>,
    statistics: NativeQueryStatisticsRecorder,
}

impl<'a, D: ArtifactDirectoryRead> SegmentExecution<'a, D> {
    fn new(
        directory: &'a D,
        segment_index: usize,
        segment: &'a super::super::SegmentDescriptor,
        plan: SegmentPlan<'a, D>,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Result<Self, IndexError> {
        Ok(Self {
            segment_index,
            segment,
            cursor: plan.cursor,
            exact_filter: plan.exact_filter,
            scorer: SegmentScorer::new(
                directory,
                segment,
                plan.text_terms,
                plan.phrase_fields,
                &statistics,
            )?,
            values: SegmentValues::new(directory, segment, statistics.clone())?,
            prefetched: None,
            statistics,
        })
    }

    async fn next_unranked(&mut self) -> Result<Option<Unranked>, IndexError> {
        loop {
            let doc_id = if let Some(doc_id) = self.prefetched.take() {
                Some(doc_id)
            } else {
                self.cursor.next().await?
            };
            let Some(doc_id) = doc_id else {
                return Ok(None);
            };
            self.statistics.candidate_doc_id();
            if !self.values.is_live(doc_id).await? {
                self.statistics.live_mask_reject();
                continue;
            }
            if let Some(predicate) = self.exact_filter {
                self.statistics.two_phase_verification();
                if !self.values.predicate(predicate, doc_id).await? {
                    continue;
                }
            }
            if !self.scorer.phrase_matches(doc_id).await? {
                continue;
            }
            return Ok(Some(Unranked {
                doc_id,
                identity: self.values.identity(doc_id).await?,
            }));
        }
    }

    async fn seek_after(
        &mut self,
        request: &NativeQueryRequest,
        order: &[OrderField],
        after: &NativeQueryCursor,
        directions: &[OrderDirection],
    ) -> Result<(), IndexError> {
        let mut lower = 0u32;
        let mut upper = self.segment.document_count;
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let doc_id = DocId::new(middle);
            let identity = self.values.identity(doc_id).await?;
            let sort_values = physical_values(
                request,
                &mut self.values,
                doc_id,
                identity.result_or_source(),
                order,
            )
            .await?;
            if compare_parts(
                &sort_values,
                identity.result_or_source(),
                &identity.source,
                identity.source_record,
                &after.sort_values,
                &after.result,
                &after.source,
                after.source_record,
                directions,
            ) != Ordering::Greater
            {
                lower = middle.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
            } else {
                upper = middle;
            }
        }
        // Advancing to `document_count` deliberately exhausts the cursor;
        // leaving it untouched would restart a page whose cursor sorts after
        // the final document in this segment.
        self.statistics.cursor_seek(u64::from(lower));
        self.prefetched = self.cursor.advance(DocId::new(lower)).await?;
        Ok(())
    }

    async fn next_physical(
        &mut self,
        request: &NativeQueryRequest,
        order: &[OrderField],
        directions: Arc<[OrderDirection]>,
    ) -> Result<Option<Selected>, IndexError> {
        let Some(candidate) = self.next_unranked().await? else {
            return Ok(None);
        };
        let result = candidate.identity.result_or_source().clone();
        let source_record = candidate.identity.source_record;
        let sort_values =
            physical_values(request, &mut self.values, candidate.doc_id, &result, order).await?;
        let selected = Selected {
            segment_index: self.segment_index,
            doc_id: candidate.doc_id,
            source: candidate.identity.source,
            source_record,
            result,
            score: None,
            sort_values,
            directions,
        };
        Ok(Some(selected))
    }

    fn release_decoded(&mut self) -> Result<(), IndexError> {
        self.cursor.release_decoded()?;
        self.scorer.release_decoded()?;
        self.values.release_decoded();
        Ok(())
    }
}

struct Unranked {
    doc_id: DocId,
    identity: super::super::DocumentIdentity,
}

struct Selected {
    segment_index: usize,
    doc_id: DocId,
    source: ObjectIdentity,
    source_record: u32,
    result: ObjectIdentity,
    score: Option<f32>,
    sort_values: Vec<SortValue>,
    directions: Arc<[OrderDirection]>,
}

impl PartialEq for Selected {
    fn eq(&self, other: &Self) -> bool {
        compare_selected(self, other) == Ordering::Equal
    }
}

impl Eq for Selected {}

impl PartialOrd for Selected {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Selected {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_selected(self, other)
    }
}

fn compare_selected(left: &Selected, right: &Selected) -> Ordering {
    compare_parts(
        &left.sort_values,
        &left.result,
        &left.source,
        left.source_record,
        &right.sort_values,
        &right.result,
        &right.source,
        right.source_record,
        &left.directions,
    )
}

fn compare_to_cursor(candidate: &Selected, cursor: &NativeQueryCursor) -> Ordering {
    compare_parts(
        &candidate.sort_values,
        &candidate.result,
        &candidate.source,
        candidate.source_record,
        &cursor.sort_values,
        &cursor.result,
        &cursor.source,
        cursor.source_record,
        &candidate.directions,
    )
}

#[allow(clippy::too_many_arguments)]
fn compare_parts(
    left_values: &[SortValue],
    left_result: &ObjectIdentity,
    left_source: &ObjectIdentity,
    left_source_record: u32,
    right_values: &[SortValue],
    right_result: &ObjectIdentity,
    right_source: &ObjectIdentity,
    right_source_record: u32,
    directions: &[OrderDirection],
) -> Ordering {
    for ((left, right), direction) in left_values.iter().zip(right_values).zip(directions) {
        let order = compare_sort_value(left, right, *direction);
        if order != Ordering::Equal {
            return order;
        }
    }
    left_result
        .path
        .as_bytes()
        .cmp(right_result.path.as_bytes())
        .then_with(|| left_result.version.cmp(&right_result.version))
        .then_with(|| {
            left_source
                .path
                .as_bytes()
                .cmp(right_source.path.as_bytes())
        })
        .then_with(|| left_source.version.cmp(&right_source.version))
        .then_with(|| left_source_record.cmp(&right_source_record))
}

fn compare_sort_value(left: &SortValue, right: &SortValue, direction: OrderDirection) -> Ordering {
    let ascending = match (left, right) {
        (SortValue::Missing, SortValue::Missing) => Ordering::Equal,
        (SortValue::Missing, _) => Ordering::Greater,
        (_, SortValue::Missing) => Ordering::Less,
        (SortValue::Value(left), SortValue::Value(right)) => left.cmp(right),
    };
    if direction == OrderDirection::Descending {
        ascending.reverse()
    } else {
        ascending
    }
}

fn minimum_head(heads: &[Option<Selected>]) -> Option<usize> {
    heads
        .iter()
        .enumerate()
        .filter_map(|(index, value)| value.as_ref().map(|value| (index, value)))
        .min_by(|left, right| compare_selected(left.1, right.1))
        .map(|(index, _)| index)
}

async fn rank_values<D: ArtifactDirectoryRead>(
    request: &NativeQueryRequest,
    values: &mut SegmentValues<'_, D>,
    doc_id: DocId,
    result: &ObjectIdentity,
    score: Option<f32>,
) -> Result<Vec<SortValue>, IndexError> {
    match &request.query {
        NativeQuery::Filter { order, .. } => order_values(values, doc_id, order).await,
        NativeQuery::FullText { .. } | NativeQuery::Vector { .. } | NativeQuery::Hybrid { .. } => {
            Ok(vec![SortValue::Value(ScalarValue::number(f64::from(
                score.ok_or(IndexError::InvalidFormat("ranked query has no score"))?,
            ))?)])
        }
        NativeQuery::Path { .. } => Ok(vec![SortValue::Value(ScalarValue::String(
            result.path.clone(),
        ))]),
        NativeQuery::GitSource { .. } => {
            Ok(vec![values.sort_value(FieldId::new(2), doc_id).await?])
        }
        NativeQuery::Tensor { .. } => Ok(vec![values.sort_value(FieldId::new(1), doc_id).await?]),
    }
}

async fn physical_values<D: ArtifactDirectoryRead>(
    request: &NativeQueryRequest,
    values: &mut SegmentValues<'_, D>,
    doc_id: DocId,
    result: &ObjectIdentity,
    order: &[OrderField],
) -> Result<Vec<SortValue>, IndexError> {
    match request.query {
        NativeQuery::Path { .. } => Ok(vec![SortValue::Value(ScalarValue::String(
            result.path.clone(),
        ))]),
        _ => order_values(values, doc_id, order).await,
    }
}

async fn order_values<D: ArtifactDirectoryRead>(
    values: &mut SegmentValues<'_, D>,
    doc_id: DocId,
    order: &[OrderField],
) -> Result<Vec<SortValue>, IndexError> {
    let mut output = Vec::with_capacity(order.len());
    for field in order {
        output.push(values.sort_value(field.field_id, doc_id).await?);
    }
    Ok(output)
}

fn physical_after(
    request: &NativeQueryRequest,
    directions: &[OrderDirection],
) -> Result<Option<NativeQueryCursor>, IndexError> {
    let mut after = request.after.clone();
    if let NativeQuery::Path {
        start_after: Some(path),
        ..
    } = &request.query
    {
        let path_cursor = NativeQueryCursor {
            sort_values: vec![SortValue::Value(ScalarValue::String(path.clone()))],
            result: ObjectIdentity {
                path: path.clone(),
                version: u64::MAX,
            },
            source: ObjectIdentity {
                path: path.clone(),
                version: u64::MAX,
            },
            source_record: u32::MAX,
        };
        if after.as_ref().is_none_or(|current| {
            compare_parts(
                &current.sort_values,
                &current.result,
                &current.source,
                current.source_record,
                &path_cursor.sort_values,
                &path_cursor.result,
                &path_cursor.source,
                path_cursor.source_record,
                directions,
            ) == Ordering::Less
        }) {
            after = Some(path_cursor);
        }
    }
    Ok(after)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::v4::{
        Analyzer, ArtifactDescriptor, Cardinality, Collation, ComponentKind, ComponentVersion,
        FastColumnCell, FieldComponents, FieldSchema, IndexKind, IndexSemantics, PredicateId,
        ScalarDomain, Schema, SegmentIdentity, VectorMetric, VectorNormalization,
        encode_physical_order_key, scalar_term, text_term,
    };
    use crate::{IndexFileRead, v4::build::*};

    mod correctness;

    #[derive(Clone)]
    struct MemoryFile {
        bytes: Arc<[u8]>,
        reads: Arc<AtomicUsize>,
    }

    impl IndexFileRead for MemoryFile {
        type Slice = Arc<[u8]>;

        async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);
            let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
            if start >= self.bytes.len() {
                return Ok(Arc::from([]));
            }
            let end = start.saturating_add(max_length).min(self.bytes.len());
            Ok(Arc::from(&self.bytes[start..end]))
        }
    }

    struct MemoryArtifacts {
        objects: BTreeMap<String, PublishedObject>,
        reads: Arc<AtomicUsize>,
    }

    impl MemoryArtifacts {
        fn from_sink(sink: &ExactMemorySink) -> Self {
            Self {
                objects: sink.objects().clone(),
                reads: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn reads(&self) -> usize {
            self.reads.load(AtomicOrdering::Relaxed)
        }
    }

    impl ArtifactDirectoryRead for MemoryArtifacts {
        type File = MemoryFile;

        async fn open_artifact(
            &self,
            descriptor: &ArtifactDescriptor,
        ) -> Result<Self::File, IndexError> {
            let object = self
                .objects
                .get(&descriptor.path)
                .ok_or_else(|| IndexError::FileNotFound(descriptor.path.clone()))?;
            if object.object_version != descriptor.object_version {
                return Err(IndexError::Integrity);
            }
            let start =
                usize::try_from(descriptor.offset).map_err(|_| IndexError::OffsetOverflow)?;
            let length = usize::try_from(descriptor.encoded_length)
                .map_err(|_| IndexError::OffsetOverflow)?;
            let end = start
                .checked_add(length)
                .ok_or(IndexError::OffsetOverflow)?;
            Ok(MemoryFile {
                bytes: Arc::from(object.bytes.get(start..end).ok_or(IndexError::Integrity)?),
                reads: self.reads.clone(),
            })
        }
    }

    struct TestGate {
        revision: u64,
        denied: BTreeSet<String>,
        batches: Mutex<Vec<usize>>,
    }

    impl CandidateGate for TestGate {
        type Error = IndexError;

        fn evaluate(
            &self,
            candidates: &[CandidateReference],
        ) -> impl std::future::Future<
            Output = Result<super::super::super::CandidateGateEvidence, Self::Error>,
        > + Send {
            self.batches.lock().unwrap().push(candidates.len());
            let visible = candidates
                .iter()
                .map(|candidate| !self.denied.contains(&candidate.result.path))
                .collect::<Vec<_>>();
            let denied = u64::try_from(visible.iter().filter(|visible| !**visible).count())
                .map_err(|_| IndexError::OffsetOverflow);
            std::future::ready(
                denied.map(|denied| super::super::super::CandidateGateEvidence {
                    visible,
                    authorization_revision: self.revision,
                    denied,
                    stale: 0,
                }),
            )
        }
    }

    fn version(kind: ComponentKind) -> ComponentVersion {
        ComponentVersion {
            component_kind: kind,
            codec_version: if kind == ComponentKind::IDENTITY_TABLE {
                2
            } else if kind == ComponentKind::STORED_FIELDS {
                crate::v4::STORED_FIELDS_COMPONENT_CODEC_VERSION
            } else {
                1
            },
        }
    }

    fn schema() -> super::super::super::Schema {
        super::super::super::Schema {
            kind: IndexKind::TypedJson,
            path_prefix: String::new(),
            content_type_scope: Some("application/json".into()),
            fields: vec![
                FieldSchema {
                    id: FieldId::new(0),
                    name: "state".into(),
                    source_selector: "/state".into(),
                    domain: ScalarDomain::STRING,
                    cardinality: Cardinality::Single,
                    allow_missing: false,
                    allow_null: false,
                    collation: Collation::BinaryUtf8,
                    components: FieldComponents::TERMS
                        .union(FieldComponents::FAST_COLUMN)
                        .union(FieldComponents::STORED),
                },
                FieldSchema {
                    id: FieldId::new(1),
                    name: "priority".into(),
                    source_selector: "/priority".into(),
                    domain: ScalarDomain::UNSIGNED,
                    cardinality: Cardinality::Single,
                    allow_missing: false,
                    allow_null: false,
                    collation: Collation::BinaryUtf8,
                    components: FieldComponents::TERMS
                        .union(FieldComponents::FAST_COLUMN)
                        .union(FieldComponents::STORED),
                },
                FieldSchema {
                    id: FieldId::new(2),
                    name: "category".into(),
                    source_selector: "/category".into(),
                    domain: ScalarDomain::STRING,
                    cardinality: Cardinality::Single,
                    allow_missing: false,
                    allow_null: false,
                    collation: Collation::BinaryUtf8,
                    components: FieldComponents::TERMS
                        .union(FieldComponents::FAST_COLUMN)
                        .union(FieldComponents::STORED),
                },
                FieldSchema {
                    id: FieldId::new(3),
                    name: "sequence".into(),
                    source_selector: "/sequence".into(),
                    domain: ScalarDomain::UNSIGNED,
                    cardinality: Cardinality::Single,
                    allow_missing: false,
                    allow_null: false,
                    collation: Collation::BinaryUtf8,
                    components: FieldComponents::TERMS
                        .union(FieldComponents::FAST_COLUMN)
                        .union(FieldComponents::STORED),
                },
            ],
            semantics: IndexSemantics::TypedJson,
            physical_order: vec![
                OrderField {
                    field_id: FieldId::new(1),
                    direction: OrderDirection::Descending,
                },
                OrderField {
                    field_id: FieldId::new(2),
                    direction: OrderDirection::Ascending,
                },
                OrderField {
                    field_id: FieldId::new(3),
                    direction: OrderDirection::Ascending,
                },
                OrderField {
                    field_id: FieldId::new(0),
                    direction: OrderDirection::Ascending,
                },
            ],
            component_versions: [
                ComponentKind::ROUTING_NODE,
                ComponentKind::IDENTITY_TABLE,
                ComponentKind::LIVE_MASK,
                ComponentKind::PATH_LOCATOR,
                ComponentKind::TERM_DICTIONARY,
                ComponentKind::POSTINGS,
                ComponentKind::FAST_COLUMN,
                ComponentKind::STORED_FIELDS,
                ComponentKind::SCORING_STATISTICS,
            ]
            .into_iter()
            .map(version)
            .collect(),
        }
    }

    fn source(path: &str, state: &str, priority: u64) -> ProjectedSource {
        let state_value = ScalarValue::String(state.into());
        let priority_value = ScalarValue::Unsigned(priority);
        let category_value = ScalarValue::String("advisory".into());
        let sequence_value = ScalarValue::Unsigned(priority);
        let (state_type, state_term) = scalar_term(&state_value).unwrap();
        let (priority_type, priority_term) = scalar_term(&priority_value).unwrap();
        let (category_type, category_term) = scalar_term(&category_value).unwrap();
        let (sequence_type, sequence_term) = scalar_term(&sequence_value).unwrap();
        ProjectedSource {
            source_identity: ObjectIdentity {
                path: path.into(),
                version: 1,
            },
            records: vec![ProjectedRecord {
                result_identity: None,
                order_key: encode_physical_order_key(&[
                    (
                        SortValue::Value(priority_value.clone()),
                        OrderDirection::Descending,
                    ),
                    (
                        SortValue::Value(category_value.clone()),
                        OrderDirection::Ascending,
                    ),
                    (
                        SortValue::Value(sequence_value.clone()),
                        OrderDirection::Ascending,
                    ),
                    (
                        SortValue::Value(state_value.clone()),
                        OrderDirection::Ascending,
                    ),
                ])
                .unwrap(),
                terms: vec![
                    ProjectedTerm {
                        field_id: FieldId::new(0),
                        term_type: state_type,
                        term: state_term,
                        frequency: 1,
                        positions: Vec::new(),
                    },
                    ProjectedTerm {
                        field_id: FieldId::new(1),
                        term_type: priority_type,
                        term: priority_term,
                        frequency: 1,
                        positions: Vec::new(),
                    },
                    ProjectedTerm {
                        field_id: FieldId::new(2),
                        term_type: category_type,
                        term: category_term,
                        frequency: 1,
                        positions: Vec::new(),
                    },
                    ProjectedTerm {
                        field_id: FieldId::new(3),
                        term_type: sequence_type,
                        term: sequence_term,
                        frequency: 1,
                        positions: Vec::new(),
                    },
                ],
                columns: vec![
                    ProjectedColumn {
                        field_id: FieldId::new(0),
                        multi_valued: false,
                        cell: FastColumnCell::value(state_value),
                    },
                    ProjectedColumn {
                        field_id: FieldId::new(1),
                        multi_valued: false,
                        cell: FastColumnCell::value(priority_value),
                    },
                    ProjectedColumn {
                        field_id: FieldId::new(2),
                        multi_valued: false,
                        cell: FastColumnCell::value(category_value),
                    },
                    ProjectedColumn {
                        field_id: FieldId::new(3),
                        multi_valued: false,
                        cell: FastColumnCell::value(sequence_value),
                    },
                ],
                stored_fields: Some(format!("{{\"path\":\"{path}\"}}").into_bytes()),
                vectors: Vec::new(),
                field_lengths: Vec::new(),
            }],
        }
    }

    fn field(id: u32, name: &str, components: FieldComponents) -> FieldSchema {
        FieldSchema {
            id: FieldId::new(id),
            name: name.into(),
            source_selector: format!("/{name}"),
            domain: ScalarDomain::STRING,
            cardinality: Cardinality::Single,
            allow_missing: false,
            allow_null: false,
            collation: Collation::BinaryUtf8,
            components,
        }
    }

    fn versions(fields: &[FieldSchema]) -> Vec<ComponentVersion> {
        let mut kinds = BTreeSet::from([
            ComponentKind::ROUTING_NODE,
            ComponentKind::IDENTITY_TABLE,
            ComponentKind::LIVE_MASK,
            ComponentKind::PATH_LOCATOR,
            ComponentKind::SCORING_STATISTICS,
        ]);
        for field in fields {
            for (component, kind) in [
                (FieldComponents::TERMS, ComponentKind::TERM_DICTIONARY),
                (FieldComponents::TERMS, ComponentKind::POSTINGS),
                (FieldComponents::FAST_COLUMN, ComponentKind::FAST_COLUMN),
                (FieldComponents::STORED, ComponentKind::STORED_FIELDS),
                (FieldComponents::POSITIONS, ComponentKind::POSITIONS),
                (FieldComponents::NORMS, ComponentKind::NORMS),
                (FieldComponents::VECTOR, ComponentKind::VECTORS),
            ] {
                if field.components.contains(component) {
                    kinds.insert(kind);
                }
            }
        }
        kinds.into_iter().map(version).collect()
    }

    fn scalar_projected_term(field_id: u32, value: &str) -> ProjectedTerm {
        let (term_type, term) = scalar_term(&ScalarValue::String(value.into())).unwrap();
        ProjectedTerm {
            field_id: FieldId::new(field_id),
            term_type,
            term,
            frequency: 1,
            positions: Vec::new(),
        }
    }

    fn one_record_source(
        source_path: &str,
        result_path: Option<&str>,
        terms: Vec<ProjectedTerm>,
        columns: Vec<ProjectedColumn>,
        vectors: Vec<ProjectedVector>,
        field_lengths: Vec<(FieldId, u32)>,
    ) -> ProjectedSource {
        ProjectedSource {
            source_identity: ObjectIdentity {
                path: source_path.into(),
                version: 1,
            },
            records: vec![ProjectedRecord {
                result_identity: result_path.map(|path| ObjectIdentity {
                    path: path.into(),
                    version: 1,
                }),
                order_key: Vec::new(),
                terms,
                columns,
                stored_fields: Some(br#"{"stored":true}"#.to_vec()),
                vectors,
                field_lengths,
            }],
        }
    }

    async fn assert_kind_executes(
        index_id: u64,
        mut schema: Schema,
        source: ProjectedSource,
        query: NativeQuery,
        expected_path: &str,
    ) {
        schema.component_versions = versions(&schema.fields);
        let identity = SegmentIdentity::new(index_id, 1, schema.fingerprint().unwrap(), 1).unwrap();
        let mut writer = NativeSegmentWriter::new(
            identity,
            schema.clone(),
            BuildLimits::new(64 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(writer.push_source(source).unwrap(), SourcePush::Accepted);
        let mut sink = ExactMemorySink::new();
        let built = writer.seal(&mut sink).await.unwrap();
        let directory = MemoryArtifacts::from_sink(&sink);
        let gate = TestGate {
            revision: 1,
            denied: BTreeSet::new(),
            batches: Mutex::new(Vec::new()),
        };
        let executor =
            NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default()).unwrap();
        let mut request = NativeQueryRequest {
            schema,
            segments: vec![built.descriptor],
            query,
            after: None,
            limit: 1,
            authorization_revision: 1,
        };
        let first = executor.execute(&request).await.unwrap();
        assert_eq!(first.hits.len(), 1);
        assert_eq!(first.hits[0].result.path, expected_path);
        assert_eq!(first.statistics.returned_hits, 1);
        assert_eq!(first.statistics.candidate_gate_checked, 1);
        if first.statistics.tier == NativeQueryExecutionTier::TopK {
            assert_eq!(first.statistics.top_k_inspected, 1);
        }
        assert!(gate.batches.lock().unwrap().iter().all(|size| *size <= 256));
        request.after = first.next;
        assert!(executor.execute(&request).await.unwrap().hits.is_empty());
    }

    async fn execute_many(
        index_id: u64,
        mut schema: Schema,
        sources: Vec<ProjectedSource>,
        query: NativeQuery,
        expected_hits: usize,
    ) -> NativeQueryPage {
        schema.component_versions = versions(&schema.fields);
        let identity = SegmentIdentity::new(index_id, 1, schema.fingerprint().unwrap(), 1).unwrap();
        let mut writer = NativeSegmentWriter::new(
            identity,
            schema.clone(),
            BuildLimits::new(64 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let expected = sources.len();
        for source in sources {
            assert_eq!(writer.push_source(source).unwrap(), SourcePush::Accepted);
        }
        let mut sink = ExactMemorySink::new();
        let built = writer.seal(&mut sink).await.unwrap();
        let directory = MemoryArtifacts::from_sink(&sink);
        let gate = TestGate {
            revision: 1,
            denied: BTreeSet::new(),
            batches: Mutex::new(Vec::new()),
        };
        let executor = NativeQueryExecutor::new(
            &directory,
            &gate,
            NativeQueryLimits {
                maximum_expanded_terms: 64,
                ..NativeQueryLimits::default()
            },
        )
        .unwrap();
        let page = executor
            .execute(&NativeQueryRequest {
                schema,
                segments: vec![built.descriptor],
                query,
                after: None,
                limit: u32::try_from(expected).unwrap(),
                authorization_revision: 1,
            })
            .await
            .unwrap();
        assert_eq!(page.hits.len(), expected_hits);
        assert_eq!(
            page.hits
                .iter()
                .map(|hit| hit.result.path.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            expected_hits
        );
        page
    }

    #[tokio::test]
    async fn physical_order_seeks_refills_and_never_exceeds_gate_batch() {
        let schema = schema();
        let identity = SegmentIdentity::new(7, 1, schema.fingerprint().unwrap(), 9).unwrap();
        let mut writer = NativeSegmentWriter::new(
            identity,
            schema.clone(),
            BuildLimits::new(64 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        for value in [
            source("a", "active", 1),
            source("b", "inactive", 10),
            source("c", "active", 5),
            source("d", "active", 3),
        ] {
            assert_eq!(writer.push_source(value).unwrap(), SourcePush::Accepted);
        }
        let mut sink = ExactMemorySink::new();
        let built = writer.seal(&mut sink).await.unwrap();
        let directory = MemoryArtifacts::from_sink(&sink);
        let gate = TestGate {
            revision: 17,
            denied: BTreeSet::from(["d".into()]),
            batches: Mutex::new(Vec::new()),
        };
        let request = NativeQueryRequest {
            schema: schema.clone(),
            segments: vec![built.descriptor],
            query: NativeQuery::Filter {
                predicate: Some(super::super::super::Predicate::Equal {
                    id: PredicateId::new(1),
                    field_id: FieldId::new(0),
                    value: ScalarValue::String("active".into()),
                }),
                order: schema.physical_order.clone(),
            },
            after: None,
            limit: 2,
            authorization_revision: 17,
        };
        assert!(request.working_memory_bytes().unwrap() > 0);
        let executor = NativeQueryExecutor::new(
            &directory,
            &gate,
            NativeQueryLimits {
                candidate_gate_batch: 2,
                ..NativeQueryLimits::default()
            },
        )
        .unwrap();
        let page = executor.execute(&request).await.unwrap();
        assert_eq!(
            page.hits
                .iter()
                .map(|hit| hit.result.path.as_str())
                .collect::<Vec<_>>(),
            ["c", "a"]
        );
        assert!(gate.batches.lock().unwrap().iter().all(|size| *size <= 2));
        assert_eq!(page.next, page.hits.last().map(|hit| hit.cursor.clone()));
        assert_eq!(page.statistics.tier, NativeQueryExecutionTier::Physical);
        assert!(page.statistics.term_seeks > 0);
        assert!(page.statistics.enumerated_terms > 0);
        assert!(page.statistics.posting_blocks_decoded > 0);
        assert!(page.statistics.candidate_doc_ids > 0);
        assert!(page.statistics.fast_column_blocks_decoded > 0);
        assert!(page.statistics.stored_field_blocks_decoded > 0);
        assert_eq!(page.statistics.candidate_gate_denied, 1);
        assert_eq!(page.statistics.candidate_gate_refills, 1);
        assert_eq!(page.statistics.returned_hits, 2);
        assert_eq!(page.statistics.physical_early_terminations, 1);

        let mut next_request = request.clone();
        next_request.after = page.next;
        let next = executor.execute(&next_request).await.unwrap();
        assert_eq!(next.statistics.cursor_seeks, 1);
        assert!(next.statistics.cursor_skipped_doc_ids > 0);
        assert!(next.statistics.posting_advance_calls > 0);
    }

    #[tokio::test]
    async fn physical_order_reuses_the_advancing_segments_decoded_blocks() {
        let schema = schema();
        let identity = SegmentIdentity::new(8, 1, schema.fingerprint().unwrap(), 1).unwrap();
        let mut writer = NativeSegmentWriter::new(
            identity,
            schema.clone(),
            BuildLimits::new(64 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        for priority in 0..64 {
            assert_eq!(
                writer
                    .push_source(source(&format!("record-{priority:03}"), "active", priority))
                    .unwrap(),
                SourcePush::Accepted
            );
        }
        let mut sink = ExactMemorySink::new();
        let built = writer.seal(&mut sink).await.unwrap();
        let directory = MemoryArtifacts::from_sink(&sink);
        let gate = TestGate {
            revision: 1,
            denied: BTreeSet::new(),
            batches: Mutex::new(Vec::new()),
        };
        let request = NativeQueryRequest {
            schema: schema.clone(),
            segments: vec![built.descriptor],
            query: NativeQuery::Filter {
                predicate: Some(super::super::super::Predicate::Equal {
                    id: PredicateId::new(1),
                    field_id: FieldId::new(0),
                    value: ScalarValue::String("active".into()),
                }),
                order: schema.physical_order.clone(),
            },
            after: None,
            limit: 64,
            authorization_revision: 1,
        };
        let executor =
            NativeQueryExecutor::new(&directory, &gate, NativeQueryLimits::default()).unwrap();
        let page = executor.execute(&request).await.unwrap();
        assert_eq!(page.hits.len(), 64);
        // One initial head, one retained advancing working set, and late stored
        // materialization may each open the finite component set. The read
        // count must not grow once per candidate.
        assert!(directory.reads() < 128, "reads={}", directory.reads());
    }

    #[tokio::test]
    async fn retained_page_bytes_stop_at_a_hit_boundary_with_continuation() {
        let schema = schema();
        let identity = SegmentIdentity::new(9, 1, schema.fingerprint().unwrap(), 1).unwrap();
        let mut writer = NativeSegmentWriter::new(
            identity,
            schema.clone(),
            BuildLimits::new(64 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        for priority in 0..6 {
            let mut projected = source(&format!("large-{priority}"), "active", priority);
            projected.records[0].stored_fields = Some(vec![b'x'; 180 * 1024]);
            assert_eq!(writer.push_source(projected).unwrap(), SourcePush::Accepted);
        }
        let mut sink = ExactMemorySink::new();
        let built = writer.seal(&mut sink).await.unwrap();
        let directory = MemoryArtifacts::from_sink(&sink);
        let gate = TestGate {
            revision: 1,
            denied: BTreeSet::new(),
            batches: Mutex::new(Vec::new()),
        };
        let page_limit = 400 * 1024;
        let executor = NativeQueryExecutor::new(
            &directory,
            &gate,
            NativeQueryLimits {
                maximum_page_bytes: page_limit,
                ..NativeQueryLimits::default()
            },
        )
        .unwrap();
        let mut request = NativeQueryRequest {
            schema: schema.clone(),
            segments: vec![built.descriptor],
            query: NativeQuery::Filter {
                predicate: Some(super::super::super::Predicate::Equal {
                    id: PredicateId::new(1),
                    field_id: FieldId::new(0),
                    value: ScalarValue::String("active".into()),
                }),
                order: schema.physical_order.clone(),
            },
            after: None,
            limit: 6,
            authorization_revision: 1,
        };
        let mut paths = Vec::new();
        loop {
            let page = executor.execute(&request).await.unwrap();
            assert!(!page.hits.is_empty());
            assert!(page.hits.len() < request.limit as usize);
            assert!(super::super::memory::retained_page_bytes(&page).unwrap() <= page_limit);
            paths.extend(page.hits.iter().map(|hit| hit.result.path.clone()));
            let Some(next) = page.next else {
                break;
            };
            request.after = Some(next);
        }
        assert_eq!(
            paths,
            [
                "large-5", "large-4", "large-3", "large-2", "large-1", "large-0"
            ]
        );
    }

    #[tokio::test]
    async fn every_native_index_kind_executes_with_cursor_and_gate() {
        let terms_stored = FieldComponents::TERMS.union(FieldComponents::STORED);
        let path_schema = Schema {
            kind: IndexKind::Path,
            path_prefix: String::new(),
            content_type_scope: None,
            fields: vec![field(0, "path", terms_stored)],
            semantics: IndexSemantics::Path,
            physical_order: Vec::new(),
            component_versions: Vec::new(),
        };
        assert_kind_executes(
            10,
            path_schema,
            one_record_source(
                "docs/a",
                None,
                vec![scalar_projected_term(0, "docs/a")],
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            NativeQuery::Path {
                prefix: "docs/".into(),
                start_after: None,
            },
            "docs/a",
        )
        .await;

        for (index_id, kind, semantics) in [
            (
                11,
                IndexKind::MetadataFilter,
                IndexSemantics::MetadataFilter,
            ),
            (12, IndexKind::TypedJson, IndexSemantics::TypedJson),
        ] {
            let fields = vec![field(
                0,
                "state",
                FieldComponents::TERMS
                    .union(FieldComponents::FAST_COLUMN)
                    .union(FieldComponents::STORED),
            )];
            assert_kind_executes(
                index_id,
                Schema {
                    kind,
                    path_prefix: String::new(),
                    content_type_scope: Some("application/json".into()),
                    fields,
                    semantics,
                    physical_order: Vec::new(),
                    component_versions: Vec::new(),
                },
                one_record_source(
                    "records/a",
                    None,
                    vec![scalar_projected_term(0, "active")],
                    vec![ProjectedColumn {
                        field_id: FieldId::new(0),
                        multi_valued: false,
                        cell: FastColumnCell::value(ScalarValue::String("active".into())),
                    }],
                    Vec::new(),
                    Vec::new(),
                ),
                NativeQuery::Filter {
                    predicate: Some(super::super::super::Predicate::Equal {
                        id: PredicateId::new(1),
                        field_id: FieldId::new(0),
                        value: ScalarValue::String("active".into()),
                    }),
                    order: Vec::new(),
                },
                "records/a",
            )
            .await;
        }

        let text_components = FieldComponents::TERMS
            .union(FieldComponents::POSITIONS)
            .union(FieldComponents::NORMS)
            .union(FieldComponents::STORED);
        let (text_type, text_bytes) = text_term("hello").unwrap();
        let text_term_value = ProjectedTerm {
            field_id: FieldId::new(0),
            term_type: text_type,
            term: text_bytes,
            frequency: 1,
            positions: vec![0],
        };
        assert_kind_executes(
            13,
            Schema {
                kind: IndexKind::FullText,
                path_prefix: String::new(),
                content_type_scope: None,
                fields: vec![field(0, "body", text_components)],
                semantics: IndexSemantics::FullText {
                    analyzer: Analyzer::UnicodeAlphanumericLowercase,
                    bm25_k1: 1.2,
                    bm25_b: 0.75,
                },
                physical_order: Vec::new(),
                component_versions: Vec::new(),
            },
            one_record_source(
                "text/a",
                None,
                vec![text_term_value.clone()],
                Vec::new(),
                Vec::new(),
                vec![(FieldId::new(0), 1)],
            ),
            NativeQuery::FullText {
                text: "hello".into(),
                phrase: true,
            },
            "text/a",
        )
        .await;

        let vector_field = field(
            0,
            "embedding",
            FieldComponents::VECTOR.union(FieldComponents::STORED),
        );
        assert_kind_executes(
            14,
            Schema {
                kind: IndexKind::Vector,
                path_prefix: String::new(),
                content_type_scope: None,
                fields: vec![vector_field],
                semantics: IndexSemantics::Vector {
                    dimensions: 2,
                    metric: VectorMetric::Cosine,
                    normalization: VectorNormalization::None,
                },
                physical_order: Vec::new(),
                component_versions: Vec::new(),
            },
            one_record_source(
                "vectors/a",
                None,
                Vec::new(),
                Vec::new(),
                vec![ProjectedVector {
                    field_id: FieldId::new(0),
                    values: vec![1.0, 0.0],
                }],
                Vec::new(),
            ),
            NativeQuery::Vector {
                values: vec![1.0, 0.0],
            },
            "vectors/a",
        )
        .await;

        assert_kind_executes(
            15,
            Schema {
                kind: IndexKind::Hybrid,
                path_prefix: String::new(),
                content_type_scope: None,
                fields: vec![
                    field(0, "body", text_components),
                    field(1, "embedding", FieldComponents::VECTOR),
                ],
                semantics: IndexSemantics::Hybrid {
                    analyzer: Analyzer::UnicodeAlphanumericLowercase,
                    bm25_k1: 1.2,
                    bm25_b: 0.75,
                    dimensions: 2,
                    metric: VectorMetric::Cosine,
                    normalization: VectorNormalization::None,
                    lexical_weight: 0.5,
                    vector_weight: 0.5,
                },
                physical_order: Vec::new(),
                component_versions: Vec::new(),
            },
            one_record_source(
                "hybrid/a",
                None,
                vec![text_term_value],
                Vec::new(),
                vec![ProjectedVector {
                    field_id: FieldId::new(1),
                    values: vec![1.0, 0.0],
                }],
                vec![(FieldId::new(0), 1)],
            ),
            NativeQuery::Hybrid {
                text: "hello".into(),
                vector: vec![1.0, 0.0],
            },
            "hybrid/a",
        )
        .await;

        let keyed_fields = |third: bool| {
            let mut fields = vec![
                field(0, "scope", terms_stored),
                field(1, "name", terms_stored),
            ];
            if third {
                fields.push(field(
                    2,
                    "path",
                    FieldComponents::TERMS
                        .union(FieldComponents::FAST_COLUMN)
                        .union(FieldComponents::STORED),
                ));
            } else {
                fields[1].components = fields[1].components.union(FieldComponents::FAST_COLUMN);
            }
            fields
        };
        assert_kind_executes(
            16,
            Schema {
                kind: IndexKind::GitSource,
                path_prefix: String::new(),
                content_type_scope: None,
                fields: keyed_fields(true),
                semantics: IndexSemantics::GitSource {
                    repository_scope: String::new(),
                },
                physical_order: Vec::new(),
                component_versions: Vec::new(),
            },
            one_record_source(
                "git/source",
                Some("git/result"),
                vec![
                    scalar_projected_term(0, "repo"),
                    scalar_projected_term(1, "commit"),
                    scalar_projected_term(2, "src/lib.rs"),
                ],
                vec![ProjectedColumn {
                    field_id: FieldId::new(2),
                    multi_valued: false,
                    cell: FastColumnCell::value(ScalarValue::String("src/lib.rs".into())),
                }],
                Vec::new(),
                Vec::new(),
            ),
            NativeQuery::GitSource {
                repository_id: "repo".into(),
                commit_id: "commit".into(),
                tree_path: "src/".into(),
                prefix: true,
            },
            "git/result",
        )
        .await;

        assert_kind_executes(
            17,
            Schema {
                kind: IndexKind::Tensor,
                path_prefix: String::new(),
                content_type_scope: None,
                fields: keyed_fields(false),
                semantics: IndexSemantics::Tensor {
                    model_scope: String::new(),
                },
                physical_order: Vec::new(),
                component_versions: Vec::new(),
            },
            one_record_source(
                "tensor/source",
                Some("tensor/result"),
                vec![
                    scalar_projected_term(0, "model"),
                    scalar_projected_term(1, "weights"),
                ],
                vec![ProjectedColumn {
                    field_id: FieldId::new(1),
                    multi_valued: false,
                    cell: FastColumnCell::value(ScalarValue::String("weights".into())),
                }],
                Vec::new(),
                Vec::new(),
            ),
            NativeQuery::Tensor {
                model_id: "model".into(),
                tensor_name: "weights".into(),
            },
            "tensor/result",
        )
        .await;
    }

    #[tokio::test]
    async fn dictionary_ranges_stream_more_terms_than_the_request_term_limit() {
        // Exceeds both the request-term limit (64) and the cursor's retained
        // document batch (256), proving exact continuation across a rescan.
        const RECORDS: usize = 300;

        let typed_sources = (0..RECORDS)
            .map(|ordinal| {
                source(
                    &format!("records/{ordinal:03}"),
                    &format!("group-{ordinal:03}"),
                    ordinal as u64,
                )
            })
            .collect::<Vec<_>>();
        execute_many(
            20,
            schema(),
            typed_sources.clone(),
            NativeQuery::Filter {
                predicate: Some(super::super::super::Predicate::Prefix {
                    id: PredicateId::new(1),
                    field_id: FieldId::new(0),
                    prefix: "group-".into(),
                }),
                order: Vec::new(),
            },
            RECORDS,
        )
        .await;
        execute_many(
            21,
            schema(),
            typed_sources,
            NativeQuery::Filter {
                predicate: Some(super::super::super::Predicate::Range {
                    id: PredicateId::new(1),
                    field_id: FieldId::new(1),
                    lower: Some(super::super::super::RangeBound {
                        value: ScalarValue::Unsigned(0),
                        inclusive: false,
                    }),
                    upper: Some(super::super::super::RangeBound {
                        value: ScalarValue::Unsigned((RECORDS - 1) as u64),
                        inclusive: false,
                    }),
                }),
                order: Vec::new(),
            },
            RECORDS - 2,
        )
        .await;

        let terms_stored = FieldComponents::TERMS.union(FieldComponents::STORED);
        let path_sources = (0..RECORDS)
            .map(|ordinal| {
                let path = format!("docs/{ordinal:03}");
                one_record_source(
                    &path,
                    None,
                    vec![scalar_projected_term(0, &path)],
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
            })
            .collect();
        execute_many(
            22,
            Schema {
                kind: IndexKind::Path,
                path_prefix: String::new(),
                content_type_scope: None,
                fields: vec![field(0, "path", terms_stored)],
                semantics: IndexSemantics::Path,
                physical_order: Vec::new(),
                component_versions: Vec::new(),
            },
            path_sources,
            NativeQuery::Path {
                prefix: "docs/".into(),
                start_after: None,
            },
            RECORDS,
        )
        .await;

        let git_sources = (0..RECORDS)
            .map(|ordinal| {
                let source_path = format!("git/source/{ordinal:03}");
                let result_path = format!("git/result/{ordinal:03}");
                let tree_path = format!("src/{ordinal:03}.rs");
                one_record_source(
                    &source_path,
                    Some(&result_path),
                    vec![
                        scalar_projected_term(0, "repo"),
                        scalar_projected_term(1, "commit"),
                        scalar_projected_term(2, &tree_path),
                    ],
                    vec![ProjectedColumn {
                        field_id: FieldId::new(2),
                        multi_valued: false,
                        cell: FastColumnCell::value(ScalarValue::String(tree_path)),
                    }],
                    Vec::new(),
                    Vec::new(),
                )
            })
            .collect();
        execute_many(
            23,
            Schema {
                kind: IndexKind::GitSource,
                path_prefix: String::new(),
                content_type_scope: None,
                fields: vec![
                    field(0, "repository", terms_stored),
                    field(1, "commit", terms_stored),
                    field(
                        2,
                        "tree_path",
                        terms_stored.union(FieldComponents::FAST_COLUMN),
                    ),
                ],
                semantics: IndexSemantics::GitSource {
                    repository_scope: String::new(),
                },
                physical_order: Vec::new(),
                component_versions: Vec::new(),
            },
            git_sources,
            NativeQuery::GitSource {
                repository_id: "repo".into(),
                commit_id: "commit".into(),
                tree_path: "src/".into(),
                prefix: true,
            },
            RECORDS,
        )
        .await;
    }

    #[tokio::test]
    async fn sixty_four_segment_four_field_merge_reserves_less_than_512_mib() {
        let schema = schema();
        let identity = SegmentIdentity::new(7, 1, schema.fingerprint().unwrap(), 1).unwrap();
        let mut writer = NativeSegmentWriter::new(
            identity,
            schema.clone(),
            BuildLimits::new(64 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(
            writer.push_source(source("a", "active", 1)).unwrap(),
            SourcePush::Accepted
        );
        let mut sink = ExactMemorySink::new();
        let built = writer.seal(&mut sink).await.unwrap();
        let segments = (1..=64)
            .map(|segment_id| {
                let mut descriptor = built.descriptor.clone();
                descriptor.identity = SegmentIdentity::new(
                    identity.index_id,
                    identity.definition_version,
                    identity.schema_fingerprint,
                    segment_id,
                )
                .unwrap();
                descriptor
            })
            .collect();
        let request = NativeQueryRequest {
            schema: schema.clone(),
            segments,
            query: NativeQuery::Filter {
                predicate: Some(super::super::super::Predicate::Equal {
                    id: PredicateId::new(1),
                    field_id: FieldId::new(0),
                    value: ScalarValue::String("active".into()),
                }),
                order: schema.physical_order.clone(),
            },
            after: None,
            limit: 100,
            authorization_revision: 1,
        };
        assert!(request.working_memory_bytes().unwrap() < 512 * 1024 * 1024);
    }
}
