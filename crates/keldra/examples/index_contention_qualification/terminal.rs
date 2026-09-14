use super::{MutationReport, QueryPhaseReport, config};
use serde::Serialize;
use std::sync::Mutex;

pub(super) struct TerminalEvidence {
    state: Mutex<PartialQualificationEvidence>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct PartialQualificationEvidence {
    stage: &'static str,
    baseline: Option<QueryPhaseReport>,
    concurrent: Option<QueryPhaseReport>,
    mutations: Option<MutationReport>,
}

#[derive(Debug, Serialize)]
pub(super) struct TerminalFailureReport {
    schema: &'static str,
    started_unix_milliseconds: u128,
    completed_unix_milliseconds: u128,
    result: &'static str,
    configuration: config::PublicConfig,
    failure: TerminalFailure,
    partial_evidence: PartialQualificationEvidence,
}

#[derive(Debug, Serialize)]
struct TerminalFailure {
    stage: &'static str,
    error: String,
}

impl TerminalEvidence {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(PartialQualificationEvidence {
                stage: "configuration",
                ..PartialQualificationEvidence::default()
            }),
        }
    }

    pub(super) fn stage(&self, stage: &'static str) {
        self.with_state(|state| state.stage = stage);
    }

    pub(super) fn baseline(&self, report: &QueryPhaseReport) {
        self.with_state(|state| state.baseline = Some(report.clone()));
    }

    pub(super) fn concurrent(&self, report: &QueryPhaseReport) {
        self.with_state(|state| state.concurrent = Some(report.clone()));
    }

    pub(super) fn mutations(&self, report: &MutationReport) {
        self.with_state(|state| state.mutations = Some(report.clone()));
    }

    pub(super) fn report(
        &self,
        started_unix_milliseconds: u128,
        completed_unix_milliseconds: u128,
        configuration: config::PublicConfig,
        error: String,
    ) -> TerminalFailureReport {
        let partial_evidence = self.snapshot();
        TerminalFailureReport {
            schema: "keldra.index-contention-terminal-failure.v3",
            started_unix_milliseconds,
            completed_unix_milliseconds,
            result: "fail",
            configuration,
            failure: TerminalFailure {
                stage: partial_evidence.stage,
                error,
            },
            partial_evidence,
        }
    }

    fn snapshot(&self) -> PartialQualificationEvidence {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn with_state(&self, update: impl FnOnce(&mut PartialQualificationEvidence)) {
        update(
            &mut self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_report_keeps_completed_phase_evidence_and_exact_stage() {
        let evidence = TerminalEvidence::new();
        evidence.stage("concurrent");
        evidence.concurrent(&QueryPhaseReport {
            scheduled_queries: 41,
            ..QueryPhaseReport::default()
        });
        evidence.stage("authority_snapshot_load");
        evidence.mutations(&MutationReport {
            visibility_probes_failed: 7,
            visibility_probe_observation_deadlines: 7,
            ..MutationReport::default()
        });

        let report = evidence.snapshot();
        assert_eq!(report.stage, "authority_snapshot_load");
        assert_eq!(report.concurrent.as_ref().unwrap().scheduled_queries, 41);
        assert_eq!(
            report
                .mutations
                .as_ref()
                .unwrap()
                .visibility_probe_observation_deadlines,
            7
        );
    }
}
