//! Retained in-process evidence for format-v1 partition progress and retries.

use std::time::{Duration, Instant};

use keldra_index::v1::ProjectionPartitionIdentity;
use keldra_store::SourceId;
use tonic::Status;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProducerStage {
    Opening,
    Backfill,
    JournalScan,
    Preparing,
    Sealing,
    Compacting,
    Publishing,
    CaughtUp,
    Backoff,
    Halted,
}

impl ProducerStage {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Opening => "opening",
            Self::Backfill => "backfill",
            Self::JournalScan => "journal_scan",
            Self::Preparing => "preparing",
            Self::Sealing => "sealing",
            Self::Compacting => "compacting",
            Self::Publishing => "publishing",
            Self::CaughtUp => "caught_up",
            Self::Backoff => "backoff",
            Self::Halted => "halted",
        }
    }
}

pub(super) struct PartitionEvidence {
    pub(super) source: SourceId,
    pub(super) family_id: [u8; 32],
    pub(super) physical_generation: [u8; 32],
    pub(super) published_next: u64,
    pub(super) processed_next: u64,
    pub(super) last_progress_at: Instant,
    pub(super) lag_started_at: Option<Instant>,
    pub(super) stage: ProducerStage,
    pub(super) identical_retries: u32,
    pub(super) last_error: Option<(tonic::Code, String)>,
    pub(super) last_error_message: Option<String>,
    pub(super) halted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct IntegrityHaltTransition {
    pub(super) failed_stage: ProducerStage,
    pub(super) newly_halted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PartitionLagObservation {
    pub(super) entries: u64,
    pub(super) no_progress_milliseconds: u64,
    pub(super) stalled: bool,
}

impl PartitionEvidence {
    pub(super) fn opened(
        source: SourceId,
        family_id: [u8; 32],
        physical_generation: [u8; 32],
        published_next: u64,
    ) -> Self {
        Self {
            source,
            family_id,
            physical_generation,
            published_next,
            processed_next: published_next,
            last_progress_at: Instant::now(),
            lag_started_at: None,
            stage: ProducerStage::Opening,
            identical_retries: 0,
            last_error: None,
            last_error_message: None,
            halted: false,
        }
    }

    pub(super) fn observe_progress(
        &mut self,
        published_next: u64,
        processed_next: u64,
        stage: ProducerStage,
    ) {
        let advanced = published_next > self.published_next || processed_next > self.processed_next;
        if published_next > self.published_next {
            self.published_next = published_next;
        }
        if processed_next > self.processed_next {
            self.processed_next = processed_next;
        }
        if advanced {
            self.last_progress_at = Instant::now();
        }
        self.stage = stage;
    }

    pub(super) fn observe_success(
        &mut self,
        published_next: u64,
        processed_next: u64,
        stage: ProducerStage,
    ) {
        self.observe_progress(published_next, processed_next, stage);
        self.identical_retries = 0;
        self.last_error = None;
        self.last_error_message = None;
    }

    pub(super) fn reopened(
        &mut self,
        source: SourceId,
        family_id: [u8; 32],
        physical_generation: [u8; 32],
        published_next: u64,
    ) {
        if self.source != source
            || self.family_id != family_id
            || self.physical_generation != physical_generation
            || self.published_next != published_next
        {
            self.source = source;
            self.family_id = family_id;
            self.physical_generation = physical_generation;
            self.published_next = published_next;
            self.processed_next = published_next;
            self.last_progress_at = Instant::now();
            self.lag_started_at = None;
            self.identical_retries = 0;
            self.last_error = None;
            self.last_error_message = None;
            self.halted = false;
        }
        // Reopening discarded all speculative processing, even when Current
        // did not change. Do not report the failed writer's discarded frontier
        // as progress owned by its replacement.
        self.processed_next = published_next;
        self.stage = ProducerStage::Opening;
    }

    pub(super) fn record_retry(&mut self, failed_stage: ProducerStage, error: &Status) {
        let reason = if error.code() == tonic::Code::ResourceExhausted {
            format!("{}:resource_exhausted", failed_stage.label())
        } else {
            error.message().to_owned()
        };
        let signature = (error.code(), reason);
        self.identical_retries = if self.last_error.as_ref() == Some(&signature) {
            self.identical_retries.saturating_add(1)
        } else {
            1
        };
        self.last_error = Some(signature);
        self.last_error_message = Some(error.message().to_owned());
        self.stage = ProducerStage::Backoff;
    }

    pub(super) fn halt(&mut self, error: &Status) {
        self.last_error = Some((error.code(), error.message().to_owned()));
        self.last_error_message = Some(error.message().to_owned());
        self.halted = true;
        self.stage = ProducerStage::Halted;
    }

    pub(super) fn observe_lag(
        &mut self,
        source_next: u64,
        processing_is_behind: bool,
        has_unpublished_projection_work: bool,
        stall_after: Duration,
        now: Instant,
    ) -> PartitionLagObservation {
        let entries = source_next.saturating_sub(self.published_next);
        if entries == 0 {
            self.lag_started_at = None;
            self.last_progress_at = now;
        } else if self.lag_started_at.is_none() {
            self.lag_started_at = Some(now);
            self.last_progress_at = now;
        }
        let no_progress_milliseconds = now
            .saturating_duration_since(self.last_progress_at)
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let stalled = entries > 0
            && (self.halted
                || ((processing_is_behind || has_unpublished_projection_work)
                    && now.saturating_duration_since(self.last_progress_at) >= stall_after));
        PartitionLagObservation {
            entries,
            no_progress_milliseconds,
            stalled,
        }
    }
}

pub(super) fn contain_integrity_failure(
    writer_halted: &mut bool,
    writer_stage: &mut ProducerStage,
    evidence: Option<&mut PartitionEvidence>,
    error: &Status,
) -> IntegrityHaltTransition {
    let transition = IntegrityHaltTransition {
        failed_stage: *writer_stage,
        newly_halted: !*writer_halted,
    };
    *writer_halted = true;
    *writer_stage = ProducerStage::Halted;
    if let Some(evidence) = evidence {
        evidence.halt(error);
    }
    transition
}

pub(super) type PartitionEvidenceMap =
    std::collections::BTreeMap<ProjectionPartitionIdentity, PartitionEvidence>;

#[cfg(test)]
mod tests {
    use super::*;

    fn new_evidence() -> PartitionEvidence {
        PartitionEvidence::opened(
            SourceId {
                node_id: 1,
                source_epoch: [2; 32],
            },
            [3; 32],
            [4; 32],
            7,
        )
    }

    #[test]
    fn retryable_failures_accumulate_evidence_without_halting() {
        let mut evidence = new_evidence();
        for retry in 1..=240 {
            evidence.record_retry(
                ProducerStage::Preparing,
                &Status::resource_exhausted(format!("needs {} bytes", 128 + retry)),
            );
        }
        assert_eq!(evidence.identical_retries, 240);
        assert_eq!(evidence.stage, ProducerStage::Backoff);
        assert!(!evidence.halted);

        let mut evidence = new_evidence();
        evidence.record_retry(ProducerStage::JournalScan, &Status::unavailable("first"));
        evidence.record_retry(ProducerStage::JournalScan, &Status::unavailable("second"));
        assert_eq!(evidence.identical_retries, 1);
        assert!(!evidence.halted);
    }

    #[test]
    fn durable_progress_resets_retry_evidence() {
        let mut evidence = new_evidence();
        evidence.record_retry(ProducerStage::JournalScan, &Status::unavailable("retry"));
        evidence.observe_success(8, 8, ProducerStage::CaughtUp);

        assert_eq!(evidence.published_next, 8);
        assert_eq!(evidence.identical_retries, 0);
        assert!(evidence.last_error.is_none());
    }

    #[test]
    fn catalog_rebuild_can_reset_the_published_cut_without_false_lag() {
        let mut evidence = new_evidence();
        let source = evidence.source;
        let family_id = evidence.family_id;
        evidence.halt(&Status::data_loss("old generation failed"));
        evidence.reopened(source, family_id, [5; 32], 0);

        assert_eq!(evidence.published_next, 0);
        assert_eq!(evidence.processed_next, 0);
        assert_eq!(evidence.stage, ProducerStage::Opening);
        assert!(!evidence.halted);
        assert_eq!(evidence.identical_retries, 0);
    }

    #[test]
    fn filtered_scan_progress_does_not_claim_publication_progress() {
        let mut evidence = new_evidence();
        evidence.record_retry(ProducerStage::JournalScan, &Status::unavailable("retry"));
        evidence.observe_success(7, 12, ProducerStage::CaughtUp);

        assert_eq!(evidence.published_next, 7);
        assert_eq!(evidence.processed_next, 12);
        assert_eq!(evidence.identical_retries, 0);
        assert!(evidence.last_error.is_none());
    }

    #[test]
    fn retry_reopen_discards_speculative_progress_at_unchanged_current() {
        let mut evidence = new_evidence();
        let cut = evidence.published_next;
        evidence.observe_progress(cut, cut + 100, ProducerStage::Preparing);
        evidence.record_retry(ProducerStage::Preparing, &Status::unavailable("retry"));
        let retries = evidence.identical_retries;
        let last_progress = evidence.last_progress_at;
        evidence.reopened(
            evidence.source,
            evidence.family_id,
            evidence.physical_generation,
            cut,
        );

        assert_eq!(evidence.published_next, cut);
        assert_eq!(evidence.processed_next, cut);
        assert_eq!(evidence.stage, ProducerStage::Opening);
        assert_eq!(evidence.identical_retries, retries);
        assert_eq!(evidence.last_progress_at, last_progress);
        assert!(evidence.last_error.is_some());
    }
}
