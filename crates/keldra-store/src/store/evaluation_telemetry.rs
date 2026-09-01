//! Diagnostic-only aggregate timing for single-node mutation evaluation.

use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub(super) enum EvaluationSubphase {
    MutationConstructionValidation,
    InlinePayloadReceiptStage,
    ProofConstruction,
    ProofMultiGetLookup,
    ProofValidateEncodeStage,
}

#[derive(Clone, Copy, Default)]
pub(super) struct EvaluationSubphaseMetrics {
    enabled: bool,
    pub(super) mutation_construction_validation: Duration,
    pub(super) mutation_construction_validation_operations: u64,
    pub(super) inline_payload_receipt_stage: Duration,
    pub(super) inline_payload_receipt_stage_operations: u64,
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
        let elapsed = started.elapsed();
        match subphase {
            EvaluationSubphase::MutationConstructionValidation => {
                self.mutation_construction_validation += elapsed;
            }
            EvaluationSubphase::InlinePayloadReceiptStage => {
                self.inline_payload_receipt_stage += elapsed;
            }
            EvaluationSubphase::ProofConstruction => self.proof_construction += elapsed,
            EvaluationSubphase::ProofMultiGetLookup => self.proof_multi_get_lookup += elapsed,
            EvaluationSubphase::ProofValidateEncodeStage => {
                self.proof_validate_encode_stage += elapsed;
            }
        }
        result
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
        self.mutation_construction_validation
            .saturating_add(self.inline_payload_receipt_stage)
            .saturating_add(self.proof_construction)
            .saturating_add(self.proof_multi_get_lookup)
            .saturating_add(self.proof_validate_encode_stage)
    }
}
