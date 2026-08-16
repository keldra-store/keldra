use super::*;
use crate::{DefinitionTransition, ObjectMutation, ObjectMutationContext};

#[derive(Clone, Copy)]
pub(super) struct DistributedEvaluationContext {
    pub(super) mutation: ObjectMutationContext,
    pub(super) source_id: SourceId,
    pub(super) source_journal_position: u64,
}

pub(super) struct EvaluatedOperation {
    pub(super) receipt: MutationReceipt,
    pub(super) mutation: Option<ObjectMutation>,
    pub(super) reference_deltas: Vec<ReferenceDelta>,
    pub(super) accounting_transition: Option<AccountingHeadTransition>,
    pub(super) definition_transition: Option<DefinitionTransition>,
}
