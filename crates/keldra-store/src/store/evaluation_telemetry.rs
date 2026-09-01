//! Diagnostic-only aggregate timing for single-node mutation evaluation.

use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub(super) enum EvaluationSubphase {
    CurrentGovernance,
    Planning,
    MutationConstructionValidation,
    DurableEncoding,
    InlinePayloadReceiptStage,
    BlobLifecycle,
    ObjectState,
    Coordinator,
    ProofBookkeeping,
    ProofConstruction,
    ProofMultiGetLookup,
    ProofValidateEncodeStage,
}

#[derive(Clone, Copy, Default)]
pub(super) struct EvaluationSubphaseMetrics {
    enabled: bool,
    pub(super) current_precondition_governance: Duration,
    pub(super) mutation_planning: Duration,
    pub(super) mutation_construction_validation: Duration,
    pub(super) mutation_construction_validation_operations: u64,
    pub(super) inline_payload_receipt_stage: Duration,
    pub(super) inline_payload_receipt_stage_operations: u64,
    pub(super) durable_record_encoding: Duration,
    pub(super) blob_lifecycle_stage: Duration,
    pub(super) object_state_stage: Duration,
    pub(super) coordinator_bookkeeping: Duration,
    pub(super) proof_bookkeeping: Duration,
    pub(super) proof_construction: Duration,
    pub(super) proof_construction_proofs: u64,
    pub(super) proof_multi_get_lookup: Duration,
    pub(super) proof_multi_get_lookup_proofs: u64,
    pub(super) proof_validate_encode_stage: Duration,
    pub(super) proof_validate_encode_stage_proofs: u64,
}

impl EvaluationSubphaseMetrics {
    pub(super) fn single_node_group() -> Self {
        Self {
            enabled: tracing::enabled!(
                target: "keldra_store::single_node_group_commit_phases",
                tracing::Level::INFO
            ),
            ..Self::default()
        }
    }

    pub(super) fn measure<T>(
        &mut self,
        subphase: EvaluationSubphase,
        operation: impl FnOnce() -> T,
    ) -> T {
        if !self.enabled {
            return operation();
        }
        let started = Instant::now();
        let result = operation();
        self.record_elapsed(subphase, started.elapsed());
        result
    }

    pub(super) fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }

    pub(super) fn record_since(&mut self, subphase: EvaluationSubphase, started: Option<Instant>) {
        let Some(started) = started else { return };
        self.record_elapsed(subphase, started.elapsed());
    }

    fn record_elapsed(&mut self, subphase: EvaluationSubphase, elapsed: Duration) {
        match subphase {
            EvaluationSubphase::CurrentGovernance => {
                self.current_precondition_governance += elapsed;
            }
            EvaluationSubphase::Planning => self.mutation_planning += elapsed,
            EvaluationSubphase::MutationConstructionValidation => {
                self.mutation_construction_validation += elapsed;
            }
            EvaluationSubphase::DurableEncoding => {
                self.durable_record_encoding += elapsed;
            }
            EvaluationSubphase::InlinePayloadReceiptStage => {
                self.inline_payload_receipt_stage += elapsed;
            }
            EvaluationSubphase::BlobLifecycle => self.blob_lifecycle_stage += elapsed,
            EvaluationSubphase::ObjectState => self.object_state_stage += elapsed,
            EvaluationSubphase::Coordinator => {
                self.coordinator_bookkeeping += elapsed;
            }
            EvaluationSubphase::ProofBookkeeping => self.proof_bookkeeping += elapsed,
            EvaluationSubphase::ProofConstruction => self.proof_construction += elapsed,
            EvaluationSubphase::ProofMultiGetLookup => self.proof_multi_get_lookup += elapsed,
            EvaluationSubphase::ProofValidateEncodeStage => {
                self.proof_validate_encode_stage += elapsed;
            }
        }
    }

    pub(super) fn count_mutation_construction_validation(&mut self) {
        if self.enabled {
            self.mutation_construction_validation_operations = self
                .mutation_construction_validation_operations
                .saturating_add(1);
        }
    }

    pub(super) fn count_inline_payload_receipt_stage(&mut self) {
        if self.enabled {
            self.inline_payload_receipt_stage_operations = self
                .inline_payload_receipt_stage_operations
                .saturating_add(1);
        }
    }

    pub(super) fn count_proofs(&mut self, proofs: usize) {
        if !self.enabled {
            return;
        }
        let proofs = u64::try_from(proofs).unwrap_or(u64::MAX);
        self.proof_construction_proofs = self.proof_construction_proofs.saturating_add(proofs);
        self.proof_multi_get_lookup_proofs =
            self.proof_multi_get_lookup_proofs.saturating_add(proofs);
        self.proof_validate_encode_stage_proofs = self
            .proof_validate_encode_stage_proofs
            .saturating_add(proofs);
    }

    pub(super) fn categorized(&self) -> Duration {
        self.current_precondition_governance
            .saturating_add(self.mutation_planning)
            .saturating_add(self.mutation_construction_validation)
            .saturating_add(self.durable_record_encoding)
            .saturating_add(self.inline_payload_receipt_stage)
            .saturating_add(self.blob_lifecycle_stage)
            .saturating_add(self.object_state_stage)
            .saturating_add(self.coordinator_bookkeeping)
            .saturating_add(self.proof_bookkeeping)
            .saturating_add(self.proof_construction)
            .saturating_add(self.proof_multi_get_lookup)
            .saturating_add(self.proof_validate_encode_stage)
    }
}
