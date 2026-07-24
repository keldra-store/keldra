use super::*;

const MAX_TERMINAL_REASON_CHARS: usize = 1_024;

pub(super) fn append_publication_guard_plan_hash(
    bytes: &mut Vec<u8>,
    guard: Option<&super::super::local_tx_rows::CorePublicationGuardSummary>,
) {
    append_hash_part(bytes, b"anvil.core.root_publication_guard.v1");
    match guard {
        Some(guard) => {
            append_hash_part(bytes, guard.context_hash.as_bytes());
            append_hash_part(
                bytes,
                &guard.transaction_expires_at_unix_nanos.to_le_bytes(),
            );
            append_hash_part(bytes, &guard.visible_update_count.to_le_bytes());
            append_hash_part(bytes, &guard.precondition_count.to_le_bytes());
        }
        None => append_hash_part(bytes, b"none"),
    }
}

impl RootPublicationIntent {
    fn encoded_rows(&self) -> Vec<&CoreMetaEncodedOwnedRow> {
        self.roots
            .iter()
            .flat_map(|root| root.rows.iter())
            .chain(self.local_rows.iter())
            .collect()
    }

    pub(super) fn transaction_deadline_elapsed(&self) -> Result<bool> {
        let Some(guard) = self.guard.as_ref() else {
            return Ok(false);
        };
        Ok(guard.transaction_expires_at_unix_nanos != 0
            && current_unix_nanos_u64()? >= guard.transaction_expires_at_unix_nanos)
    }

    fn publication_generations(&self) -> Result<BTreeMap<String, u64>> {
        let mut generations = BTreeMap::new();
        for root in &self.roots {
            let root_key_hash = root.publication.descriptor.root_key_hash();
            if generations
                .insert(root_key_hash.clone(), root.publication.post_root_generation)
                .is_some()
            {
                bail!("CoreMeta publication intent repeats root {root_key_hash}");
            }
        }
        Ok(generations)
    }
}

impl CoreStore {
    pub(in crate::core_store::local) async fn ensure_publication_intent_active(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<()> {
        intent.ensure_pending()?;
        if !intent.transaction_deadline_elapsed()? {
            return Ok(());
        }
        let (_guards, _) = self.acquire_publication_intent_locks(intent).await?;
        let current = self
            .read_root_publication_intent(&intent.transaction_id)?
            .ok_or_else(|| anyhow!("CoreMeta publication intent disappeared before expiry"))?;
        if !publication_intent_retry_matches(&current, intent)?
            || current.state != intent.state
            || current.terminal_reason != intent.terminal_reason
        {
            bail!("CoreMeta publication intent changed during expiry handling");
        }
        if !current.transaction_deadline_elapsed()? {
            bail!("CoreMeta publication intent deadline changed during expiry handling");
        }
        self.ensure_publication_intent_active_locked(&current)
    }

    pub(super) fn ensure_publication_intent_active_locked(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<()> {
        intent.ensure_pending()?;
        if !intent.transaction_deadline_elapsed()? {
            return Ok(());
        }
        self.mark_root_publication_intent_terminal(intent, "TransactionExpired")?;
        Err(publication_terminal_error("TransactionExpired"))
    }

    pub(super) async fn acquire_publication_intent_locks(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<(
        Vec<CoreStoreLock>,
        Option<super::super::local_tx_rows::CorePublicationGuardContext>,
    )> {
        let rows = intent.encoded_rows();
        let context = super::super::local_tx_rows::hydrate_publication_guard_context(
            self,
            &intent.transaction_id,
            &rows,
        )
        .await?;
        let mut observed = BTreeSet::new();
        for _ in 0..CORE_PROCESS_LOCK_RETRY_ATTEMPTS {
            let lock_keys = self.publication_intent_lock_keys(intent, context.as_ref())?;
            let guards = self.acquire_sorted_lock_keys(&lock_keys).await?;
            let stable = self.publication_intent_lock_keys(intent, context.as_ref())?;
            if stable.is_subset(&lock_keys) {
                return Ok((guards, context));
            }
            observed = stable;
        }
        bail!(
            "CoreMeta publication guard locks changed too often while acquiring: {:?}",
            observed
        )
    }

    fn publication_intent_lock_keys(
        &self,
        intent: &RootPublicationIntent,
        context: Option<&super::super::local_tx_rows::CorePublicationGuardContext>,
    ) -> Result<BTreeSet<(String, String)>> {
        let mut keys = BTreeSet::new();
        keys.insert(("transaction".to_string(), intent.transaction_id.clone()));
        for root in &intent.roots {
            keys.insert((
                "root-publication".to_string(),
                root.publication.descriptor.root_key_hash(),
            ));
        }
        for row in intent.encoded_rows() {
            let cf = canonical_coremeta_cf_name(&row.cf)?;
            let table_id = core_meta_record_table_id(&row.core_meta_key)?;
            let tuple_key = core_meta_record_tuple_key(&row.core_meta_key)?;
            Self::insert_coremeta_row_lock(&mut keys, cf, table_id, tuple_key);
            if !row.root_key_hash.is_empty() {
                keys.insert(("root-publication".to_string(), row.root_key_hash.clone()));
            }
        }
        if let Some(context) = context {
            for update in &context.transaction.visible_updates {
                if let CoreTransactionUpdate::StreamAppend { stream_id, .. } = update {
                    keys.insert(("stream".to_string(), stream_id.clone()));
                }
            }
            for persisted in &context.preconditions {
                if persisted.revalidate_at_publication {
                    self.insert_publication_precondition_lock_keys(
                        &mut keys,
                        &persisted.precondition,
                    )?;
                }
            }
        }
        Ok(keys)
    }

    fn insert_publication_precondition_lock_keys(
        &self,
        keys: &mut BTreeSet<(String, String)>,
        precondition: &CoreMutationPrecondition,
    ) -> Result<()> {
        match precondition {
            CoreMutationPrecondition::Fence { fence_name, .. } => {
                keys.insert(("fence".to_string(), fence_name.clone()));
                let tuple_key = super::super::local_stream_control::core_fence_row_key(fence_name)?;
                self.insert_publication_observed_row_locks(
                    keys,
                    CF_LEASES_FENCES,
                    TABLE_CORE_FENCE_ROW,
                    &tuple_key,
                )?;
            }
            CoreMutationPrecondition::CoreMetaRow {
                cf,
                table_id,
                tuple_key,
                ..
            }
            | CoreMutationPrecondition::CoreMetaLease {
                cf,
                table_id,
                tuple_key,
                ..
            } => {
                let cf = canonical_coremeta_cf_name(cf)?;
                self.insert_publication_observed_row_locks(keys, cf, *table_id, tuple_key)?;
            }
            CoreMutationPrecondition::StreamHead { stream_id, .. } => {
                keys.insert(("stream".to_string(), stream_id.clone()));
                self.insert_publication_observed_row_locks(
                    keys,
                    CF_STREAM_HEADS,
                    TABLE_STREAM_HEAD_ROW,
                    &stream_head_key(stream_id),
                )?;
            }
        }
        Ok(())
    }

    fn insert_publication_observed_row_locks(
        &self,
        keys: &mut BTreeSet<(String, String)>,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
    ) -> Result<()> {
        Self::insert_coremeta_row_lock(keys, cf, table_id, tuple_key);
        if let Some(payload) = self.read_coremeta_row(cf, table_id, tuple_key)? {
            let common = core_meta_row_common_from_payload(&payload)?;
            if !common.root_key_hash.is_empty() {
                keys.insert(("root-publication".to_string(), common.root_key_hash));
            }
        }
        Ok(())
    }

    pub(super) async fn validate_publication_guards_at_linearization(
        &self,
        intent: &RootPublicationIntent,
        context: Option<&super::super::local_tx_rows::CorePublicationGuardContext>,
    ) -> Result<()> {
        intent.ensure_pending()?;
        let Some(context) = context else {
            if intent.guard.is_some() {
                bail!("CoreMeta publication guard context is missing");
            }
            return Ok(());
        };
        if intent.guard.as_ref() != Some(&context.summary) {
            bail!("CoreMeta publication guard summary changed before linearization");
        }
        if self.publication_transaction_source_changed(context).await? {
            return self.terminal_publication_guard_failure(
                intent,
                "TransactionConflict: staged transaction changed after publication prepare",
            );
        }
        if let Err(error) = self
            .validate_staged_transaction_preconditions_unlocked(
                &context.preconditions,
                &context.transaction.committed_by_principal,
                &context.transaction,
            )
            .await
        {
            return self.terminal_publication_guard_failure(
                intent,
                &format!("PublicationPreconditionFailed: {error:#}"),
            );
        }
        let mutation_validation = if context.transaction.purpose == "implicit_mutation_batch" {
            match intent.publication_generations() {
                Ok(publication_generations) => {
                    self.validate_implicit_transaction_publication_unlocked(
                        &context.transaction,
                        &publication_generations,
                    )
                    .await
                }
                Err(error) => Err(error),
            }
        } else {
            self.validate_explicit_transaction_commit_unlocked(&context.transaction)
                .await
        };
        if let Err(error) = mutation_validation {
            return self.terminal_publication_guard_failure(
                intent,
                &format!("PublicationMutationConflict: {error:#}"),
            );
        }
        self.validate_publication_temporal_deadlines(intent, context)
    }

    async fn publication_transaction_source_changed(
        &self,
        context: &super::super::local_tx_rows::CorePublicationGuardContext,
    ) -> Result<bool> {
        let expected = &context.transaction;
        let current = self
            .read_transaction_unlocked(&expected.transaction_id)
            .await?;
        if expected.purpose == "implicit_mutation_batch" {
            return Ok(current.is_some_and(|current| {
                !publication_transaction_is_completed_retry(&current, expected)
            }));
        }
        let Some(mut current) = current else {
            return Ok(true);
        };
        let mut current_preconditions = self
            .read_transaction_preconditions_unlocked(&expected.transaction_id)
            .await?;
        let committed_root_generation = expected
            .committed_root_generation
            .ok_or_else(|| anyhow!("CoreMeta publication guard transaction is not root-bound"))?;
        if self
            .bind_explicit_transaction_to_generation(&mut current, committed_root_generation)
            .is_err()
            || self
                .bind_explicit_transaction_preconditions(&current, &mut current_preconditions)
                .is_err()
        {
            return Ok(true);
        }
        Ok(!publication_transaction_staging_matches(
            &current,
            expected,
            &current_preconditions,
            &context.preconditions,
        ))
    }

    fn validate_publication_temporal_deadlines(
        &self,
        intent: &RootPublicationIntent,
        context: &super::super::local_tx_rows::CorePublicationGuardContext,
    ) -> Result<()> {
        let now = current_unix_nanos_u64()?;
        if context.transaction.expires_at_unix_nanos != 0
            && now >= context.transaction.expires_at_unix_nanos
        {
            return self.terminal_publication_guard_failure(intent, "TransactionExpired");
        }
        for persisted in &context.preconditions {
            if !persisted.revalidate_at_publication {
                continue;
            }
            if let CoreMutationPrecondition::CoreMetaLease {
                expires_at_unix_nanos,
                ..
            } = &persisted.precondition
                && now >= *expires_at_unix_nanos
            {
                return self.terminal_publication_guard_failure(
                    intent,
                    "PublicationPreconditionFailed: CoreMeta lease expired before linearization",
                );
            }
        }
        Ok(())
    }

    pub(super) fn terminal_publication_guard_failure<T>(
        &self,
        intent: &RootPublicationIntent,
        reason: &str,
    ) -> Result<T> {
        self.mark_root_publication_intent_terminal(intent, reason)?;
        Err(publication_terminal_error(reason))
    }

    pub(in crate::core_store::local) fn mark_root_publication_intent_terminal(
        &self,
        intent: &RootPublicationIntent,
        reason: &str,
    ) -> Result<()> {
        if intent.state == RootPublicationIntentState::Terminal {
            return intent.ensure_pending();
        }
        if reason.trim().is_empty() {
            bail!("CoreMeta publication terminal reason must not be empty");
        }
        if !self.validate_persisted_root_publication_intent_summary(intent)? {
            bail!("CoreMeta publication intent disappeared before terminalization");
        }
        let mut terminal = intent.clone();
        terminal.state = RootPublicationIntentState::Terminal;
        terminal.terminal_reason = Some(reason.chars().take(MAX_TERMINAL_REASON_CHARS).collect());
        let header = intent_header_proto(&terminal)?;
        let tuple_key = intent_header_key(&terminal.transaction_id)?;
        let payload = encode_deterministic_proto(&header);
        self.meta.write_local_committed_batch(&[CoreMetaBatchOp {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_ROOT_PUBLICATION_INTENT_ROW,
            tuple_key: &tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        }])
    }
}

fn publication_transaction_staging_matches(
    current: &CoreTransaction,
    expected: &CoreTransaction,
    current_preconditions: &[super::super::local_tx_rows::CoreTransactionPreconditionRow],
    expected_preconditions: &[super::super::local_tx_rows::CoreTransactionPreconditionRow],
) -> bool {
    let exact_published_staging = current == expected;
    let open_staging = current.transaction_id == expected.transaction_id
        && current.scope_partition == expected.scope_partition
        && current.state == CoreTransactionState::Open
        && current.preconditions_hash == expected.preconditions_hash
        && current.operations_hash == expected.operations_hash
        && current.writer_families == expected.writer_families
        && current.visible_updates == expected.visible_updates
        && current.finalisation_error.is_none()
        && current.committed_by_principal == expected.committed_by_principal
        && current.created_at_unix_nanos == expected.created_at_unix_nanos
        && current.expires_at_unix_nanos == expected.expires_at_unix_nanos
        && current.root_anchor_key == expected.root_anchor_key
        && current.root_key_hash == expected.root_key_hash
        && current.committed_root_generation.is_none()
        && current.purpose == expected.purpose
        && current.failure_evidence.is_none()
        && current.outcome == "open";
    (open_staging || exact_published_staging) && current_preconditions == expected_preconditions
}

fn publication_transaction_is_completed_retry(
    current: &CoreTransaction,
    expected: &CoreTransaction,
) -> bool {
    current.transaction_id == expected.transaction_id
        && current.state == expected.state
        && current.preconditions_hash == expected.preconditions_hash
        && current.operations_hash == expected.operations_hash
        && current.visible_updates == expected.visible_updates
        && current.committed_by_principal == expected.committed_by_principal
        && current.root_anchor_key == expected.root_anchor_key
        && current.root_key_hash == expected.root_key_hash
        && current.committed_root_generation == expected.committed_root_generation
}
