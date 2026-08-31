use super::*;
use crate::{DefinitionTransition, ObjectAliasSnapshot, ObjectMutation, ObjectMutationContext};

#[derive(Clone, Copy)]
pub(super) struct DistributedEvaluationContext {
    pub(super) mutation: ObjectMutationContext,
    pub(super) source_id: SourceId,
    pub(super) source_journal_position: u64,
    pub(super) reference_effects: LocalReferenceEffects,
}

pub(super) struct EvaluatedOperation {
    pub(super) receipt: MutationReceipt,
    pub(super) mutation: Option<ObjectMutation>,
    pub(super) reference_deltas: Vec<ReferenceDelta>,
    pub(super) accounting_transition: Option<AccountingHeadTransition>,
    pub(super) definition_transition: Option<DefinitionTransition>,
    pub(super) alias_snapshot: Option<ObjectAliasSnapshot>,
}

impl EvaluatedOperation {
    pub(super) fn pending_head_changes(
        &self,
        identity: crate::key::BucketIdentity,
        canonical_path: &str,
    ) -> Vec<PendingLocalChange> {
        let mut changes = Vec::with_capacity(
            1 + self
                .alias_snapshot
                .as_ref()
                .map_or(0, |snapshot| snapshot.registry.aliases.len()),
        );
        changes.push(PendingLocalChange::ObjectHead {
            identity,
            exact_path: canonical_path.to_owned(),
            path_version: self.receipt.version,
            deleted: self.receipt.deleted,
            program_commit_cursor: None,
            reference_deltas: self.reference_deltas.clone(),
            accounting_transition: self.accounting_transition,
            definition_transition: self.definition_transition.clone(),
        });
        if let Some(snapshot) = self.alias_snapshot.as_ref() {
            changes.extend(snapshot.registry.aliases.iter().map(|alias| {
                PendingLocalChange::AliasObjectHead {
                    identity,
                    exact_path: alias.clone(),
                    canonical_path: canonical_path.to_owned(),
                    path_version: self.receipt.version,
                    deleted: self.receipt.deleted,
                    program_commit_cursor: None,
                }
            }));
        }
        changes
    }
}
