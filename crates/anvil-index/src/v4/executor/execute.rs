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
    ObjectIdentity, OrderDirection, OrderField, SegmentComponentReader, SortValue,
};
use super::plan::{SegmentPlan, plan_segment};
use super::posting::DocCursor;
use super::query_semantics::{
    physical_order, query_directions, scoring_query_vector, text_scoring_active,
};
use super::score::{GlobalTextStatistics, SegmentScorer};
use super::values::SegmentValues;

mod compute;
mod impact;
mod result;
mod sorting;

use sorting::{
    compare_parts, compare_selected, compare_to_cursor, minimum_head, physical_after,
    physical_values, rank_values,
};

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
        let (facet_results, aggregate_results) = self.compute(request, &statistics).await?;
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
                        state
                            .observe(&mut execution.values, candidate.doc_id, statistics)
                            .await?;
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
        }
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
                heads[index] = executions[index]
                    .next_physical(request, order, directions.clone())
                    .await?;
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
        Ok(selected)
    }
}

struct SegmentExecution<'a, D> {
    segment_index: usize,
    segment: &'a super::super::SegmentDescriptor,
    cursor: DocCursor<'a, D>,
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
            scorer: SegmentScorer::new(directory, segment, plan.text_terms, &statistics)?,
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

#[cfg(test)]
#[path = "execute/tests_v4.rs"]
mod tests;

#[cfg(test)]
#[path = "execute/tests_ranked.rs"]
mod tests_ranked;
