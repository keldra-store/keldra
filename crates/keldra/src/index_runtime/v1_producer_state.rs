//! Retained in-process evidence for format-v1 partition progress and retries.

use std::time::Instant;

use keldra_index::v1::ProjectionPartitionIdentity;
use keldra_store::SourceId;
use tonic::Status;

pub(super) const MAX_IDENTICAL_RETRIES: u32 = 120;

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
    pub(super) published_next: u64,
    pub(super) last_progress_at: Instant,
    pub(super) lag_started_at: Option<Instant>,
    pub(super) stage: ProducerStage,
    pub(super) identical_retries: u32,
    pub(super) last_error: Option<(tonic::Code, String)>,
    pub(super) last_error_message: Option<String>,
    pub(super) halted: bool,
}

impl PartitionEvidence {
    pub(super) fn opened(source: SourceId, family_id: [u8; 32], published_next: u64) -> Self {
        Self {
            source,
            family_id,
            published_next,
            last_progress_at: Instant::now(),
            lag_started_at: None,
            stage: ProducerStage::Opening,
            identical_retries: 0,
            last_error: None,
            last_error_message: None,
            halted: false,
        }
    }

    pub(super) fn observe_progress(&mut self, published_next: u64, stage: ProducerStage) {
        if published_next > self.published_next {
            self.published_next = published_next;
            self.last_progress_at = Instant::now();
            self.identical_retries = 0;
            self.last_error = None;
            self.last_error_message = None;
        }
        self.stage = stage;
    }

    pub(super) fn reopened(&mut self, source: SourceId, family_id: [u8; 32], published_next: u64) {
        if self.source != source
            || self.family_id != family_id
            || self.published_next != published_next
        {
            self.source = source;
            self.family_id = family_id;
            self.published_next = published_next;
            self.last_progress_at = Instant::now();
            self.lag_started_at = None;
        }
        self.stage = ProducerStage::Opening;
    }

    pub(super) fn record_retry(&mut self, failed_stage: ProducerStage, error: &Status) -> bool {
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
        self.halted = self.identical_retries >= MAX_IDENTICAL_RETRIES;
        self.stage = if self.halted {
            ProducerStage::Halted
        } else {
            ProducerStage::Backoff
        };
        self.halted
    }

    pub(super) fn halt(&mut self, error: &Status) {
        self.last_error = Some((error.code(), error.message().to_owned()));
        self.last_error_message = Some(error.message().to_owned());
        self.halted = true;
        self.stage = ProducerStage::Halted;
    }
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
            7,
        )
    }

    #[test]
    fn only_identical_retries_accumulate_toward_halt() {
        let mut evidence = new_evidence();
        for retry in 1..MAX_IDENTICAL_RETRIES {
            assert!(!evidence.record_retry(
                ProducerStage::Preparing,
                &Status::resource_exhausted(format!("needs {} bytes", 128 + retry))
            ));
        }
        assert!(evidence.record_retry(
            ProducerStage::Preparing,
            &Status::resource_exhausted("needs 249 bytes")
        ));
        assert_eq!(evidence.stage, ProducerStage::Halted);

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
        evidence.observe_progress(8, ProducerStage::CaughtUp);

        assert_eq!(evidence.published_next, 8);
        assert_eq!(evidence.identical_retries, 0);
        assert!(evidence.last_error.is_none());
    }

    #[test]
    fn catalog_rebuild_can_reset_the_published_cut_without_false_lag() {
        let mut evidence = new_evidence();
        let source = evidence.source;
        let family_id = evidence.family_id;
        evidence.reopened(source, family_id, 0);

        assert_eq!(evidence.published_next, 0);
        assert_eq!(evidence.stage, ProducerStage::Opening);
    }
}
