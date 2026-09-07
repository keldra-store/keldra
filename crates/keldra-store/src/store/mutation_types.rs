use super::*;
use crate::{DefinitionTransition, ObjectAliasSnapshot, ObjectMutation, ObjectMutationContext};

#[derive(Clone, Copy)]
pub(super) struct DistributedEvaluationContext {
    pub(super) mutation: ObjectMutationContext,
    pub(super) source_id: SourceId,
    pub(super) source_journal_position: u64,
    pub(super) reference_effects: LocalReferenceEffects,
    pub(super) materialize_inline_payload: bool,
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
            // A target with logical aliases changes every visible name in one
            // RocksDB commit, but those journal wakes may straddle bounded
            // consumer pages. Force the accounting projection to rebuild from
            // the committed target+registry snapshot instead of publishing a
            // canonical-only intermediate total.
            accounting_transition: if self.alias_snapshot.is_some() {
                None
            } else {
                self.accounting_transition
            },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{BucketId, TenantId};
    use crate::{OBJECT_ALIAS_REGISTRY_FORMAT, ObjectAliasRegistry, Version};

    #[test]
    fn alias_expansion_omits_the_canonical_incremental_accounting_transition() {
        let transition = AccountingHeadTransition::new(Some(7), Some(9), 7);
        let operation = EvaluatedOperation {
            receipt: MutationReceipt {
                command_id: Some("alias-accounting".into()),
                fingerprint: [1; 32],
                version: VersionId(9),
                deleted: false,
                replayed: false,
                replay_guarantee_expires_at_unix_millis: 1,
            },
            mutation: None,
            reference_deltas: Vec::new(),
            accounting_transition: Some(transition),
            definition_transition: None,
            alias_snapshot: Some(ObjectAliasSnapshot {
                registry: ObjectAliasRegistry {
                    format: OBJECT_ALIAS_REGISTRY_FORMAT,
                    revision: 1,
                    aliases: vec!["aliases/a".into()],
                    program_commit_cursor: Some(1),
                },
                canonical_version: Version {
                    id: VersionId(7),
                    blob: None,
                    content_type: None,
                    deleted: false,
                    committed_at_unix_millis: 1,
                    protected_link_descriptor: false,
                },
            }),
        };

        let changes = operation.pending_head_changes(
            crate::key::BucketIdentity {
                tenant_id: TenantId(11),
                bucket_id: BucketId(12),
            },
            "canonical",
        );
        assert_eq!(changes.len(), 2);
        let PendingLocalChange::ObjectHead {
            accounting_transition,
            ..
        } = &changes[0]
        else {
            panic!("first alias-expanded event must be the canonical head");
        };
        assert_eq!(*accounting_transition, None);
        assert!(matches!(
            &changes[1],
            PendingLocalChange::AliasObjectHead {
                exact_path,
                canonical_path,
                ..
            } if exact_path == "aliases/a" && canonical_path == "canonical"
        ));
    }
}
