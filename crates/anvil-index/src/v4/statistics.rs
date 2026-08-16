use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum NativeQueryExecutionTier {
    #[default]
    Unplanned = 0,
    Physical = 1,
    TopK = 2,
}

impl NativeQueryExecutionTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unplanned => "unplanned",
            Self::Physical => "physical",
            Self::TopK => "top_k",
        }
    }
}

/// Bounded, low-cardinality execution phases used by process-local query
/// observability. These values are not persisted or exposed in the query
/// protocol.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeQueryPhase {
    Plan,
    ContinuationSeek,
    HeadInitialization,
    PhysicalMergeAdvance,
    CandidateVisibility,
    ResponseMaterialization,
}

impl NativeQueryPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::ContinuationSeek => "continuation_seek",
            Self::HeadInitialization => "head_initialization",
            Self::PhysicalMergeAdvance => "physical_merge_advance",
            Self::CandidateVisibility => "candidate_visibility",
            Self::ResponseMaterialization => "response_materialization",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NativeQueryStatistics {
    pub tier: NativeQueryExecutionTier,
    pub planner_conjunctions: u64,
    pub planner_reordered_conjunctions: u64,
    pub planner_costed_children: u64,
    pub planner_child_cost_total: u64,
    pub planner_lead_cost_min: u64,
    pub planner_lead_cost_max: u64,
    pub term_seeks: u64,
    pub enumerated_terms: u64,
    pub posting_blocks_sought: u64,
    pub posting_blocks_decoded: u64,
    pub posting_blocks_skipped: u64,
    pub posting_bytes_read: u64,
    pub posting_advance_calls: u64,
    pub conjunction_advances: u64,
    pub union_heap_pushes: u64,
    pub union_heap_pops: u64,
    pub two_phase_verifications: u64,
    pub candidate_doc_ids: u64,
    pub live_mask_blocks_decoded: u64,
    pub live_mask_rejects: u64,
    pub point_blocks_decoded: u64,
    pub doc_value_blocks_decoded: u64,
    pub facet_documents_processed: u64,
    pub facet_values_processed: u64,
    pub aggregate_documents_processed: u64,
    pub aggregate_values_processed: u64,
    pub cursor_seeks: u64,
    pub cursor_skipped_doc_ids: u64,
    pub physical_early_terminations: u64,
    pub top_k_inspected: u64,
    pub candidate_gate_checked: u64,
    pub candidate_gate_batches: u64,
    pub candidate_gate_denied: u64,
    pub candidate_gate_stale: u64,
    pub candidate_gate_refills: u64,
    pub returned_hits: u64,
    /// Working memory the executor would use when its preferred decoded state
    /// residency can be admitted.
    pub query_memory_desired_bytes: u64,
    /// Conservatively charged working-memory bytes actually admitted.
    pub query_memory_granted_bytes: u64,
    /// Maximum number of segment decoded states the admitted memory can retain.
    pub resident_segment_slots: u64,
    pub resident_segments_current: u64,
    pub resident_segments_peak: u64,
    /// Conservative charged bytes, rather than sampled allocator residency.
    pub retained_decoded_charged_bytes: u64,
    pub retained_decoded_charged_peak_bytes: u64,
    pub decoded_state_evictions: u64,
    pub decoded_state_reloads: u64,
    pub plan_duration_nanos: u64,
    pub continuation_seek_duration_nanos: u64,
    pub head_initialization_duration_nanos: u64,
    pub physical_merge_advance_duration_nanos: u64,
    /// Combined Zanzibar authorization and exact-current validation time. The
    /// current visibility boundary deliberately exposes one operation.
    pub candidate_visibility_duration_nanos: u64,
    pub response_materialization_duration_nanos: u64,
}

#[derive(Default)]
struct NativeQueryStatisticsInner {
    tier: AtomicU8,
    planner_conjunctions: AtomicU64,
    planner_reordered_conjunctions: AtomicU64,
    planner_costed_children: AtomicU64,
    planner_child_cost_total: AtomicU64,
    planner_lead_cost_min: AtomicU64,
    planner_lead_cost_max: AtomicU64,
    term_seeks: AtomicU64,
    enumerated_terms: AtomicU64,
    posting_blocks_sought: AtomicU64,
    posting_blocks_decoded: AtomicU64,
    posting_blocks_skipped: AtomicU64,
    posting_bytes_read: AtomicU64,
    posting_advance_calls: AtomicU64,
    conjunction_advances: AtomicU64,
    union_heap_pushes: AtomicU64,
    union_heap_pops: AtomicU64,
    two_phase_verifications: AtomicU64,
    candidate_doc_ids: AtomicU64,
    live_mask_blocks_decoded: AtomicU64,
    live_mask_rejects: AtomicU64,
    point_blocks_decoded: AtomicU64,
    doc_value_blocks_decoded: AtomicU64,
    facet_documents_processed: AtomicU64,
    facet_values_processed: AtomicU64,
    aggregate_documents_processed: AtomicU64,
    aggregate_values_processed: AtomicU64,
    cursor_seeks: AtomicU64,
    cursor_skipped_doc_ids: AtomicU64,
    physical_early_terminations: AtomicU64,
    top_k_inspected: AtomicU64,
    candidate_gate_checked: AtomicU64,
    candidate_gate_batches: AtomicU64,
    candidate_gate_denied: AtomicU64,
    candidate_gate_stale: AtomicU64,
    candidate_gate_refills: AtomicU64,
    returned_hits: AtomicU64,
    query_memory_desired_bytes: AtomicU64,
    query_memory_granted_bytes: AtomicU64,
    resident_segment_slots: AtomicU64,
    resident_segments_current: AtomicU64,
    resident_segments_peak: AtomicU64,
    retained_decoded_charged_bytes: AtomicU64,
    retained_decoded_charged_peak_bytes: AtomicU64,
    decoded_state_evictions: AtomicU64,
    decoded_state_reloads: AtomicU64,
    plan_duration_nanos: AtomicU64,
    continuation_seek_duration_nanos: AtomicU64,
    head_initialization_duration_nanos: AtomicU64,
    physical_merge_advance_duration_nanos: AtomicU64,
    candidate_visibility_duration_nanos: AtomicU64,
    response_materialization_duration_nanos: AtomicU64,
}

/// Process-local progress recorder shared with the query cancellation guard.
/// It is intentionally absent from all public and persistent protocols.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct NativeQueryStatisticsRecorder {
    inner: Arc<NativeQueryStatisticsInner>,
}

/// Cancellation-safe timer for one native-query phase.
pub(crate) struct NativeQueryPhaseGuard {
    statistics: NativeQueryStatisticsRecorder,
    phase: NativeQueryPhase,
    started: std::time::Instant,
}

impl NativeQueryStatisticsRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> NativeQueryStatistics {
        let planner_conjunctions = load(&self.inner.planner_conjunctions);
        NativeQueryStatistics {
            tier: match self.inner.tier.load(Ordering::Relaxed) {
                1 => NativeQueryExecutionTier::Physical,
                2 => NativeQueryExecutionTier::TopK,
                _ => NativeQueryExecutionTier::Unplanned,
            },
            planner_conjunctions,
            planner_reordered_conjunctions: load(&self.inner.planner_reordered_conjunctions),
            planner_costed_children: load(&self.inner.planner_costed_children),
            planner_child_cost_total: load(&self.inner.planner_child_cost_total),
            planner_lead_cost_min: if planner_conjunctions == 0 {
                0
            } else {
                load(&self.inner.planner_lead_cost_min)
            },
            planner_lead_cost_max: load(&self.inner.planner_lead_cost_max),
            term_seeks: load(&self.inner.term_seeks),
            enumerated_terms: load(&self.inner.enumerated_terms),
            posting_blocks_sought: load(&self.inner.posting_blocks_sought),
            posting_blocks_decoded: load(&self.inner.posting_blocks_decoded),
            posting_blocks_skipped: load(&self.inner.posting_blocks_skipped),
            posting_bytes_read: load(&self.inner.posting_bytes_read),
            posting_advance_calls: load(&self.inner.posting_advance_calls),
            conjunction_advances: load(&self.inner.conjunction_advances),
            union_heap_pushes: load(&self.inner.union_heap_pushes),
            union_heap_pops: load(&self.inner.union_heap_pops),
            two_phase_verifications: load(&self.inner.two_phase_verifications),
            candidate_doc_ids: load(&self.inner.candidate_doc_ids),
            live_mask_blocks_decoded: load(&self.inner.live_mask_blocks_decoded),
            live_mask_rejects: load(&self.inner.live_mask_rejects),
            point_blocks_decoded: load(&self.inner.point_blocks_decoded),
            doc_value_blocks_decoded: load(&self.inner.doc_value_blocks_decoded),
            facet_documents_processed: load(&self.inner.facet_documents_processed),
            facet_values_processed: load(&self.inner.facet_values_processed),
            aggregate_documents_processed: load(&self.inner.aggregate_documents_processed),
            aggregate_values_processed: load(&self.inner.aggregate_values_processed),
            cursor_seeks: load(&self.inner.cursor_seeks),
            cursor_skipped_doc_ids: load(&self.inner.cursor_skipped_doc_ids),
            physical_early_terminations: load(&self.inner.physical_early_terminations),
            top_k_inspected: load(&self.inner.top_k_inspected),
            candidate_gate_checked: load(&self.inner.candidate_gate_checked),
            candidate_gate_batches: load(&self.inner.candidate_gate_batches),
            candidate_gate_denied: load(&self.inner.candidate_gate_denied),
            candidate_gate_stale: load(&self.inner.candidate_gate_stale),
            candidate_gate_refills: load(&self.inner.candidate_gate_refills),
            returned_hits: load(&self.inner.returned_hits),
            query_memory_desired_bytes: load(&self.inner.query_memory_desired_bytes),
            query_memory_granted_bytes: load(&self.inner.query_memory_granted_bytes),
            resident_segment_slots: load(&self.inner.resident_segment_slots),
            resident_segments_current: load(&self.inner.resident_segments_current),
            resident_segments_peak: load(&self.inner.resident_segments_peak),
            retained_decoded_charged_bytes: load(&self.inner.retained_decoded_charged_bytes),
            retained_decoded_charged_peak_bytes: load(
                &self.inner.retained_decoded_charged_peak_bytes,
            ),
            decoded_state_evictions: load(&self.inner.decoded_state_evictions),
            decoded_state_reloads: load(&self.inner.decoded_state_reloads),
            plan_duration_nanos: load(&self.inner.plan_duration_nanos),
            continuation_seek_duration_nanos: load(&self.inner.continuation_seek_duration_nanos),
            head_initialization_duration_nanos: load(
                &self.inner.head_initialization_duration_nanos,
            ),
            physical_merge_advance_duration_nanos: load(
                &self.inner.physical_merge_advance_duration_nanos,
            ),
            candidate_visibility_duration_nanos: load(
                &self.inner.candidate_visibility_duration_nanos,
            ),
            response_materialization_duration_nanos: load(
                &self.inner.response_materialization_duration_nanos,
            ),
        }
    }

    pub(crate) fn record_tier(&self, tier: NativeQueryExecutionTier) {
        self.inner.tier.store(tier as u8, Ordering::Relaxed);
    }

    pub(crate) fn planned_conjunction(&self, original: &[u64], chosen: &[u64]) {
        debug_assert!(!chosen.is_empty());
        debug_assert_eq!(original.len(), chosen.len());
        let previous_conjunctions = self
            .inner
            .planner_conjunctions
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(1))
            })
            .unwrap_or(u64::MAX);
        add(
            &self.inner.planner_reordered_conjunctions,
            u64::from(original != chosen),
        );
        add(
            &self.inner.planner_costed_children,
            u64::try_from(chosen.len()).unwrap_or(u64::MAX),
        );
        add(
            &self.inner.planner_child_cost_total,
            chosen
                .iter()
                .fold(0_u64, |total, cost| total.saturating_add(*cost)),
        );
        let lead = chosen[0];
        if previous_conjunctions == 0 {
            self.inner
                .planner_lead_cost_min
                .store(lead, Ordering::Relaxed);
        } else {
            let _ = self
                .inner
                .planner_lead_cost_min
                .fetch_min(lead, Ordering::Relaxed);
        }
        let _ = self
            .inner
            .planner_lead_cost_max
            .fetch_max(lead, Ordering::Relaxed);
    }

    pub(crate) fn term_seek(&self) {
        add(&self.inner.term_seeks, 1);
    }

    pub(crate) fn enumerated_terms(&self, count: u64) {
        add(&self.inner.enumerated_terms, count);
    }

    pub(crate) fn posting_block_sought(&self, bytes: u64) {
        add(&self.inner.posting_blocks_sought, 1);
        add(&self.inner.posting_bytes_read, bytes);
    }

    pub(crate) fn posting_block_decoded(&self) {
        add(&self.inner.posting_blocks_decoded, 1);
    }

    pub(crate) fn posting_block_skipped(&self) {
        add(&self.inner.posting_blocks_skipped, 1);
    }

    pub(crate) fn posting_advance(&self) {
        add(&self.inner.posting_advance_calls, 1);
    }

    pub(crate) fn conjunction_advance(&self) {
        add(&self.inner.conjunction_advances, 1);
    }

    pub(crate) fn union_heap_push(&self) {
        add(&self.inner.union_heap_pushes, 1);
    }

    pub(crate) fn union_heap_pop(&self) {
        add(&self.inner.union_heap_pops, 1);
    }

    pub(crate) fn two_phase_verification(&self) {
        add(&self.inner.two_phase_verifications, 1);
    }

    pub(crate) fn candidate_doc_id(&self) {
        add(&self.inner.candidate_doc_ids, 1);
    }

    pub(crate) fn live_mask_reject(&self) {
        add(&self.inner.live_mask_rejects, 1);
    }

    pub(crate) fn live_mask_blocks_decoded(&self, count: u64) {
        add(&self.inner.live_mask_blocks_decoded, count);
    }

    pub(crate) fn point_blocks_decoded(&self, count: u64) {
        add(&self.inner.point_blocks_decoded, count);
    }

    pub(crate) fn doc_value_blocks_decoded(&self, count: u64) {
        add(&self.inner.doc_value_blocks_decoded, count);
    }

    pub(crate) fn facet_processed(&self, values: u64) {
        add(&self.inner.facet_documents_processed, 1);
        add(&self.inner.facet_values_processed, values);
    }

    pub(crate) fn aggregate_processed(&self, values: u64) {
        add(&self.inner.aggregate_documents_processed, 1);
        add(&self.inner.aggregate_values_processed, values);
    }

    pub(crate) fn cursor_seek(&self, skipped_doc_ids: u64) {
        add(&self.inner.cursor_seeks, 1);
        add(&self.inner.cursor_skipped_doc_ids, skipped_doc_ids);
    }

    pub(crate) fn physical_early_termination(&self) {
        add(&self.inner.physical_early_terminations, 1);
    }

    pub(crate) fn top_k_inspected(&self) {
        add(&self.inner.top_k_inspected, 1);
    }

    pub(crate) fn candidate_gate_checked(&self, count: u64) {
        add(&self.inner.candidate_gate_checked, count);
    }

    pub(crate) fn candidate_gate_batch(&self) {
        add(&self.inner.candidate_gate_batches, 1);
    }

    pub(crate) fn candidate_gate_denied(&self, count: u64) {
        add(&self.inner.candidate_gate_denied, count);
    }

    pub(crate) fn candidate_gate_stale(&self, count: u64) {
        add(&self.inner.candidate_gate_stale, count);
    }

    pub(crate) fn candidate_gate_refill(&self) {
        add(&self.inner.candidate_gate_refills, 1);
    }

    pub(crate) fn returned_hit(&self) {
        add(&self.inner.returned_hits, 1);
    }

    /// Record the preferred and admitted query-memory envelopes. Values are
    /// maxima so a later elastic admission may safely increase either value.
    #[doc(hidden)]
    pub fn query_memory(&self, desired_bytes: u64, granted_bytes: u64) {
        let _ = self
            .inner
            .query_memory_desired_bytes
            .fetch_max(desired_bytes, Ordering::Relaxed);
        let _ = self
            .inner
            .query_memory_granted_bytes
            .fetch_max(granted_bytes, Ordering::Relaxed);
    }

    /// Set the admitted number of segment-residency slots.
    #[doc(hidden)]
    pub fn resident_segment_slots(&self, slots: usize) {
        self.inner
            .resident_segment_slots
            .store(u64::try_from(slots).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// Account one segment's conservatively charged decoded state becoming
    /// resident. This is an accounting signal, not allocator RSS sampling.
    #[doc(hidden)]
    pub fn decoded_state_retained(&self, charged_bytes: u64) {
        let current = saturating_increment(&self.inner.resident_segments_current, 1);
        let bytes = saturating_increment(&self.inner.retained_decoded_charged_bytes, charged_bytes);
        let _ = self
            .inner
            .resident_segments_peak
            .fetch_max(current, Ordering::Relaxed);
        let _ = self
            .inner
            .retained_decoded_charged_peak_bytes
            .fetch_max(bytes, Ordering::Relaxed);
    }

    /// Account one segment's charged decoded state being released.
    #[doc(hidden)]
    pub fn decoded_state_released(&self, charged_bytes: u64) {
        saturating_decrement(&self.inner.resident_segments_current, 1);
        saturating_decrement(&self.inner.retained_decoded_charged_bytes, charged_bytes);
    }

    #[doc(hidden)]
    pub fn decoded_state_evicted(&self) {
        add(&self.inner.decoded_state_evictions, 1);
    }

    #[doc(hidden)]
    pub fn decoded_state_reloaded(&self) {
        add(&self.inner.decoded_state_reloads, 1);
    }

    /// Accumulate elapsed time for an execution phase. Concurrent segment
    /// planning may therefore report aggregate lane time rather than wall time;
    /// callers that time the coordinator should record one wall-time sample.
    #[doc(hidden)]
    pub fn phase_elapsed(&self, phase: NativeQueryPhase, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let target = match phase {
            NativeQueryPhase::Plan => &self.inner.plan_duration_nanos,
            NativeQueryPhase::ContinuationSeek => &self.inner.continuation_seek_duration_nanos,
            NativeQueryPhase::HeadInitialization => &self.inner.head_initialization_duration_nanos,
            NativeQueryPhase::PhysicalMergeAdvance => {
                &self.inner.physical_merge_advance_duration_nanos
            }
            NativeQueryPhase::CandidateVisibility => {
                &self.inner.candidate_visibility_duration_nanos
            }
            NativeQueryPhase::ResponseMaterialization => {
                &self.inner.response_materialization_duration_nanos
            }
        };
        add(target, nanos);
    }

    /// Start a timer which records even when the surrounding future fails or
    /// is cancelled.
    pub(crate) fn phase_timer(&self, phase: NativeQueryPhase) -> NativeQueryPhaseGuard {
        NativeQueryPhaseGuard {
            statistics: self.clone(),
            phase,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for NativeQueryPhaseGuard {
    fn drop(&mut self) {
        self.statistics
            .phase_elapsed(self.phase, self.started.elapsed());
    }
}

fn load(value: &AtomicU64) -> u64 {
    value.load(Ordering::Relaxed)
}

fn add(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(amount))
    });
}

fn saturating_increment(value: &AtomicU64, amount: u64) -> u64 {
    value
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(amount))
        })
        .unwrap_or(u64::MAX)
        .saturating_add(amount)
}

fn saturating_decrement(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_bounded_query_progress() {
        let recorder = NativeQueryStatisticsRecorder::new();
        let clone = recorder.clone();
        recorder.record_tier(NativeQueryExecutionTier::TopK);
        clone.enumerated_terms(3);
        recorder.planned_conjunction(&[9, 1], &[1, 9]);
        clone.conjunction_advance();
        recorder.union_heap_push();
        clone.union_heap_pop();
        recorder.candidate_gate_batch();
        recorder.candidate_gate_checked(4);
        clone.returned_hit();
        recorder.query_memory(900, 1_024);
        recorder.resident_segment_slots(3);
        recorder.decoded_state_retained(200);
        clone.decoded_state_retained(300);
        recorder.decoded_state_evicted();
        clone.decoded_state_reloaded();
        recorder.decoded_state_released(200);
        recorder.phase_elapsed(NativeQueryPhase::Plan, Duration::from_millis(2));
        clone.phase_elapsed(NativeQueryPhase::Plan, Duration::from_millis(3));

        let snapshot = recorder.snapshot();
        assert_eq!(snapshot.tier, NativeQueryExecutionTier::TopK);
        assert_eq!(snapshot.enumerated_terms, 3);
        assert_eq!(snapshot.planner_conjunctions, 1);
        assert_eq!(snapshot.planner_reordered_conjunctions, 1);
        assert_eq!(snapshot.planner_costed_children, 2);
        assert_eq!(snapshot.planner_child_cost_total, 10);
        assert_eq!(snapshot.planner_lead_cost_min, 1);
        assert_eq!(snapshot.planner_lead_cost_max, 1);
        assert_eq!(snapshot.conjunction_advances, 1);
        assert_eq!(snapshot.union_heap_pushes, 1);
        assert_eq!(snapshot.union_heap_pops, 1);
        assert_eq!(snapshot.candidate_gate_batches, 1);
        assert_eq!(snapshot.candidate_gate_checked, 4);
        assert_eq!(snapshot.returned_hits, 1);
        assert_eq!(snapshot.query_memory_desired_bytes, 900);
        assert_eq!(snapshot.query_memory_granted_bytes, 1_024);
        assert_eq!(snapshot.resident_segment_slots, 3);
        assert_eq!(snapshot.resident_segments_current, 1);
        assert_eq!(snapshot.resident_segments_peak, 2);
        assert_eq!(snapshot.retained_decoded_charged_bytes, 300);
        assert_eq!(snapshot.retained_decoded_charged_peak_bytes, 500);
        assert_eq!(snapshot.decoded_state_evictions, 1);
        assert_eq!(snapshot.decoded_state_reloads, 1);
        assert_eq!(snapshot.plan_duration_nanos, 5_000_000);
    }
}
