use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use crate::IndexError;
use futures_util::future::join_all;

use super::super::{
    ArtifactDirectoryRead, CandidateGate, CandidateReference, DocId, FieldId,
    INDEX_COMMIT_SEGMENTS, NativeQuery, NativeQueryCursor, NativeQueryExecutionTier,
    NativeQueryHit, NativeQueryPage, NativeQueryPhase, NativeQueryRequest,
    NativeQueryStatisticsRecorder, ObjectIdentity, OrderDirection, OrderField,
    SegmentComponentReader, SegmentStatistics, SortValue,
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
    compare_parts, compare_selected, compare_to_cursor, physical_after, physical_values,
    rank_values,
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
            maximum_segments: INDEX_COMMIT_SEGMENTS,
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
            || self.maximum_segments > INDEX_COMMIT_SEGMENTS
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
        super::memory::estimate_working_memory(
            request,
            self.limits,
            self.directory.query_parallelism(),
        )
    }

    pub fn memory_estimate(
        &self,
        request: &NativeQueryRequest,
    ) -> Result<super::memory::NativeQueryMemoryEstimate, IndexError> {
        super::memory::estimate_working_memory_range(
            request,
            self.limits,
            self.directory.query_parallelism(),
        )
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
        self.execute_observed_with_resident_segments(
            request,
            statistics,
            request.segments.len().max(1),
        )
        .await
    }

    /// Execute under a previously admitted decoded-state residency. The limit
    /// bounds concurrent segment planning and physical-order retained state;
    /// non-physical execution lanes are already part of the mandatory grant.
    #[doc(hidden)]
    pub async fn execute_observed_with_resident_segments<'query>(
        &'query self,
        request: &'query NativeQueryRequest,
        statistics: NativeQueryStatisticsRecorder,
        resident_segment_limit: usize,
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
        let memory = self.memory_estimate(request)?;
        let scoring_query_vector = scoring_query_vector(&request.schema, &request.query)?;
        let text_scoring = text_scoring_active(&request.query);
        let physical = physical_order(request).is_some();
        statistics.record_tier(if physical {
            NativeQueryExecutionTier::Physical
        } else {
            NativeQueryExecutionTier::TopK
        });

        let plan_phase = statistics.phase_timer(NativeQueryPhase::Plan);
        let mut global = GlobalTextStatistics::default();
        let mut executions = Vec::with_capacity(request.segments.len());
        let planning_lanes = self.planning_lanes(request, resident_segment_limit);
        let mut start = 0usize;
        while start < request.segments.len() {
            let end = start
                .saturating_add(planning_lanes)
                .min(request.segments.len());
            for (segment_index, plan, segment_statistics) in self
                .plan_segment_chunk(request, start..end, text_scoring, &statistics)
                .await?
            {
                let segment = &request.segments[segment_index];
                if let Some(segment_statistics) = segment_statistics {
                    global.add_segment(&segment_statistics, &plan.text_terms)?;
                }
                executions.push(SegmentExecution::new(
                    self.directory,
                    segment_index,
                    segment,
                    plan,
                    statistics.clone(),
                )?);
            }
            start = end;
        }
        drop(plan_phase);

        let selected = if physical {
            self.execute_physical(
                request,
                &mut executions,
                resident_segment_limit.max(1),
                memory.resident_segment_bytes(),
                &statistics,
            )
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
            .compute(request, resident_segment_limit, &statistics)
            .await?;
        let materialization_phase =
            statistics.phase_timer(NativeQueryPhase::ResponseMaterialization);
        let page = result::materialize(
            request,
            executions.as_mut_slice(),
            selected,
            facet_results,
            aggregate_results,
            self.limits.maximum_page_bytes,
            &statistics,
        )
        .await
        .map_err(NativeQueryExecutionError::Index);
        drop(materialization_phase);
        page
    }

    async fn compute<'query>(
        &'query self,
        request: &'query NativeQueryRequest,
        resident_segment_limit: usize,
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
        let planning_lanes = self.planning_lanes(request, resident_segment_limit);
        let mut start = 0usize;
        while start < request.segments.len() {
            let end = start
                .saturating_add(planning_lanes)
                .min(request.segments.len());
            let plan_phase = statistics.phase_timer(NativeQueryPhase::Plan);
            let planned = self
                .plan_segment_chunk(request, start..end, false, statistics)
                .await?;
            drop(plan_phase);
            for (segment_index, plan, _) in planned {
                let mut execution = SegmentExecution::new(
                    self.directory,
                    segment_index,
                    &request.segments[segment_index],
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
                    for (candidate, resolved) in pending.drain(..).zip(evidence.resolved) {
                        if resolved.is_some() {
                            state
                                .observe(&mut execution.values, candidate.doc_id, statistics)
                                .await?;
                        }
                    }
                }
                execution.release_decoded()?;
            }
            start = end;
        }
        state.finish().map_err(NativeQueryExecutionError::Index)
    }

    fn planning_lanes(&self, request: &NativeQueryRequest, resident_segment_limit: usize) -> usize {
        self.directory
            .query_parallelism()
            .max(1)
            .min(resident_segment_limit.max(1))
            .min(request.segments.len().max(1))
    }

    async fn plan_segment_chunk<'query>(
        &'query self,
        request: &'query NativeQueryRequest,
        indices: std::ops::Range<usize>,
        read_text_statistics: bool,
        statistics: &NativeQueryStatisticsRecorder,
    ) -> Result<Vec<(usize, SegmentPlan<'query, D>, Option<SegmentStatistics>)>, IndexError> {
        let mut work = Vec::with_capacity(indices.len());
        for segment_index in indices {
            let segment = &request.segments[segment_index];
            work.push(async move {
                let plan = plan_segment(
                    self.directory,
                    segment,
                    &request.schema,
                    &request.query,
                    self.limits.maximum_expanded_terms,
                    statistics,
                )
                .await?;
                let segment_statistics = if read_text_statistics {
                    Some(
                        SegmentComponentReader::new(self.directory, segment)?
                            .statistics()
                            .await?,
                    )
                } else {
                    None
                };
                Ok::<_, IndexError>((segment_index, plan, segment_statistics))
            });
        }
        let mut planned = Vec::with_capacity(work.len());
        // `join_all` preserves the segment-index order established above. Fold
        // errors and global statistics only after every lane completes so the
        // externally visible plan remains deterministic.
        for result in join_all(work).await {
            planned.push(result?);
        }
        Ok(planned)
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
        let requested_lanes = self
            .directory
            .query_parallelism()
            .max(1)
            .min(executions.len().max(1));
        // Full-text block-max pruning depends on the best score found in
        // earlier segments. Keep that proven global threshold until a shared
        // competitive-score implementation can parallelize it without
        // multiplying candidate verification.
        let lanes = if matches!(&request.query, NativeQuery::FullText { .. }) {
            1
        } else {
            requested_lanes
        };
        if lanes == 1 {
            let mut heap = BinaryHeap::new();
            for execution in executions.iter_mut() {
                heap = self
                    .execute_segment_top_k(
                        request,
                        global,
                        scoring_query_vector,
                        execution,
                        &directions,
                        heap,
                        statistics,
                    )
                    .await?;
            }
            let mut values = heap.into_vec();
            values.sort_by(compare_selected);
            return Ok(values);
        }
        // A global top K is necessarily contained in the union of every
        // segment's local top K. This keeps parallel lanes independent and
        // publication/order semantics identical to the serial merge.
        let mut heap = BinaryHeap::new();
        for chunk in executions.chunks_mut(lanes) {
            let mut work = Vec::with_capacity(chunk.len());
            for execution in chunk {
                work.push(self.execute_segment_top_k(
                    request,
                    global,
                    scoring_query_vector,
                    execution,
                    &directions,
                    BinaryHeap::new(),
                    statistics,
                ));
            }
            for selected in join_all(work).await {
                for selected in selected? {
                    heap.push(selected);
                    if heap.len() > request.limit as usize {
                        heap.pop();
                    }
                }
            }
        }
        let mut values = heap.into_vec();
        values.sort_by(compare_selected);
        Ok(values)
    }

    async fn execute_segment_top_k<'query>(
        &'query self,
        request: &NativeQueryRequest,
        global: &GlobalTextStatistics,
        scoring_query_vector: Option<&Arc<[f32]>>,
        execution: &mut SegmentExecution<'query, D>,
        directions: &Arc<[OrderDirection]>,
        mut heap: BinaryHeap<Selected>,
        statistics: &NativeQueryStatisticsRecorder,
    ) -> Result<BinaryHeap<Selected>, NativeQueryExecutionError<G::Error>> {
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
                    directions,
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
                directions,
                &mut pending,
                &mut heap,
                statistics,
            )
            .await?;
        }
        execution.release_decoded()?;
        Ok(heap)
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
        for (candidate, resolved) in pending.drain(..).zip(evidence.resolved) {
            let Some(resolved) = resolved else { continue };
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
                order_source: candidate.identity.source,
                order_result: result,
                source: resolved.source,
                source_record,
                result: resolved.result,
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
        resident_segment_limit: usize,
        resident_segment_bytes: usize,
        statistics: &NativeQueryStatisticsRecorder,
    ) -> Result<Vec<Selected>, NativeQueryExecutionError<G::Error>> {
        let order = physical_order(request).expect("checked physical plan");
        let directions = Arc::<[OrderDirection]>::from(query_directions(request));
        let after = physical_after(request, &directions)?;
        let resident_segment_limit = resident_segment_limit.max(1).min(executions.len().max(1));
        let lanes = self
            .directory
            .query_parallelism()
            .max(1)
            .min(resident_segment_limit);
        statistics.resident_segment_slots(resident_segment_limit);
        let mut residency = PhysicalResidency::new(
            resident_segment_limit,
            executions.len(),
            resident_segment_bytes,
            statistics.clone(),
        );
        let mut indexed_heads = Vec::with_capacity(executions.len());
        let mut start = 0usize;
        while start < executions.len() {
            let end = start.saturating_add(lanes).min(executions.len());
            residency.activate_batch(start..end, executions)?;
            let chunk = &mut executions[start..end];
            let mut work = Vec::with_capacity(chunk.len());
            for execution in chunk {
                work.push(self.initialize_physical_segment(
                    request,
                    order,
                    after.as_ref(),
                    directions.clone(),
                    execution,
                ));
            }
            for head in join_all(work).await {
                indexed_heads.push(head?);
            }
            start = end;
        }
        let mut heads = BinaryHeap::with_capacity(indexed_heads.len());
        for (_, head) in indexed_heads {
            if let Some(head) = head {
                heads.push(PhysicalHead(head));
            }
        }
        let mut selected = Vec::with_capacity(request.limit as usize);
        let mut refill_required = false;
        while selected.len() < request.limit as usize {
            let batch_target = self
                .limits
                .candidate_gate_batch
                .min((request.limit as usize).saturating_sub(selected.len()));
            let mut pending = Vec::with_capacity(batch_target);
            let merge_phase = statistics.phase_timer(NativeQueryPhase::PhysicalMergeAdvance);
            while pending.len() < batch_target {
                let Some(PhysicalHead(head)) = heads.pop() else {
                    break;
                };
                let index = head.segment_index;
                pending.push(head);
                residency.activate(index, executions)?;
                if let Some(next) = executions[index]
                    .next_physical(request, order, directions.clone())
                    .await?
                {
                    debug_assert_eq!(next.segment_index, index);
                    heads.push(PhysicalHead(next));
                }
            }
            drop(merge_phase);
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
            for (mut candidate, resolved) in pending.into_iter().zip(evidence.resolved) {
                if let Some(resolved) = resolved {
                    candidate.source = resolved.source;
                    candidate.result = resolved.result;
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
        residency.release_all(executions)?;
        Ok(selected)
    }

    async fn initialize_physical_segment<'query>(
        &'query self,
        request: &NativeQueryRequest,
        order: &[OrderField],
        after: Option<&NativeQueryCursor>,
        directions: Arc<[OrderDirection]>,
        execution: &mut SegmentExecution<'query, D>,
    ) -> Result<(usize, Option<Selected>), NativeQueryExecutionError<G::Error>> {
        if let Some(after) = after {
            let seek_phase = execution
                .statistics
                .phase_timer(NativeQueryPhase::ContinuationSeek);
            execution
                .seek_after(request, order, after, &directions)
                .await?;
            drop(seek_phase);
        }
        let head_phase = execution
            .statistics
            .phase_timer(NativeQueryPhase::HeadInitialization);
        let head = execution.next_physical(request, order, directions).await?;
        drop(head_phase);
        Ok((execution.segment_index, head))
    }
}

/// Query-local LRU for disposable decoded segment state. Activating a segment
/// evicts before the next read, so execution never transiently exceeds the
/// admitted resident-segment count.
struct PhysicalResidency {
    capacity: usize,
    resident: VecDeque<usize>,
    present: Vec<bool>,
    previously_loaded: Vec<bool>,
    charged_bytes: u64,
    statistics: NativeQueryStatisticsRecorder,
}

impl PhysicalResidency {
    fn new(
        capacity: usize,
        segments: usize,
        charged_bytes: usize,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Self {
        Self {
            capacity: capacity.max(1),
            resident: VecDeque::with_capacity(capacity.min(segments)),
            present: vec![false; segments],
            previously_loaded: vec![false; segments],
            charged_bytes: u64::try_from(charged_bytes).unwrap_or(u64::MAX),
            statistics,
        }
    }

    fn activate_batch<D: ArtifactDirectoryRead>(
        &mut self,
        indices: std::ops::Range<usize>,
        executions: &mut [SegmentExecution<'_, D>],
    ) -> Result<(), IndexError> {
        for index in indices {
            self.activate(index, executions)?;
        }
        Ok(())
    }

    fn activate<D: ArtifactDirectoryRead>(
        &mut self,
        index: usize,
        executions: &mut [SegmentExecution<'_, D>],
    ) -> Result<(), IndexError> {
        if self.present.get(index).copied().unwrap_or(false) {
            if let Some(position) = self.resident.iter().position(|resident| *resident == index) {
                self.resident.remove(position);
            }
            self.resident.push_back(index);
            return Ok(());
        }
        while self.resident.len() >= self.capacity {
            let evicted = self
                .resident
                .pop_front()
                .expect("resident capacity checked nonzero");
            executions[evicted].release_decoded()?;
            self.present[evicted] = false;
            self.statistics.decoded_state_released(self.charged_bytes);
            self.statistics.decoded_state_evicted();
        }
        self.resident.push_back(index);
        self.present[index] = true;
        if self.previously_loaded[index] {
            self.statistics.decoded_state_reloaded();
        }
        self.previously_loaded[index] = true;
        self.statistics.decoded_state_retained(self.charged_bytes);
        Ok(())
    }

    fn release_all<D: ArtifactDirectoryRead>(
        &mut self,
        executions: &mut [SegmentExecution<'_, D>],
    ) -> Result<(), IndexError> {
        while let Some(index) = self.resident.pop_front() {
            executions[index].release_decoded()?;
            self.present[index] = false;
            self.statistics.decoded_state_released(self.charged_bytes);
        }
        Ok(())
    }
}

impl Drop for PhysicalResidency {
    fn drop(&mut self) {
        for present in &mut self.present {
            if *present {
                *present = false;
                self.statistics.decoded_state_released(self.charged_bytes);
            }
        }
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
            order_source: candidate.identity.source.clone(),
            order_result: result.clone(),
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
    order_source: ObjectIdentity,
    order_result: ObjectIdentity,
    source: ObjectIdentity,
    source_record: u32,
    result: ObjectIdentity,
    score: Option<f32>,
    sort_values: Vec<SortValue>,
    directions: Arc<[OrderDirection]>,
}

/// Reverses `Selected`'s natural ordering for the standard max-heap while
/// retaining a deterministic lower-segment tie-break for indistinguishable
/// heads. The physical merge stores exactly one head per live segment and pops
/// the globally smallest candidate in `O(log segments)`.
struct PhysicalHead(Selected);

impl PartialEq for PhysicalHead {
    fn eq(&self, other: &Self) -> bool {
        compare_selected(&self.0, &other.0) == Ordering::Equal
            && self.0.segment_index == other.0.segment_index
    }
}

impl Eq for PhysicalHead {}

impl PartialOrd for PhysicalHead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PhysicalHead {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_selected(&other.0, &self.0)
            .then_with(|| other.0.segment_index.cmp(&self.0.segment_index))
    }
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
