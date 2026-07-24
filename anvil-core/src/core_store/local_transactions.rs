use super::local_stream_control::control_record_proto::decode_stream_head_record;
use super::local_tx_rows::CoreTransactionPreconditionRow;
use super::*;
use crate::formats::writer::WriterFamily;

struct CoreStoreMutationLocks {
    _root_plan_guards: Vec<CoreStoreLock>,
    mutable_guards: Vec<CoreStoreLock>,
}

impl CoreStoreMutationLocks {
    fn release_mutable_guards(&mut self) {
        self.mutable_guards.clear();
    }
}

fn insert_coremeta_root_lock_from_payload(
    lock_keys: &mut BTreeSet<(String, String)>,
    payload: &[u8],
) -> Result<()> {
    let common = core_meta_row_common_from_payload(payload)?;
    if !common.root_key_hash.is_empty() {
        lock_keys.insert(("coremeta-root".to_string(), common.root_key_hash));
    }
    Ok(())
}

impl CoreStore {
    pub(super) async fn acquire_sorted_lock_keys(
        &self,
        lock_keys: &BTreeSet<(String, String)>,
    ) -> Result<Vec<CoreStoreLock>> {
        let mut guards = Vec::with_capacity(lock_keys.len());
        for (kind, id) in lock_keys {
            guards.push(self.acquire_named_lock(kind, id).await?);
        }
        Ok(guards)
    }

    async fn acquire_mutation_lock_keys(
        &self,
        lock_keys: &BTreeSet<(String, String)>,
        publication_root_hashes: &BTreeSet<String>,
    ) -> Result<CoreStoreMutationLocks> {
        let mut locks = CoreStoreMutationLocks {
            _root_plan_guards: Vec::new(),
            mutable_guards: Vec::new(),
        };
        for (kind, id) in lock_keys {
            let guard = self.acquire_named_lock(kind, id).await?;
            if kind == "coremeta-root" && publication_root_hashes.contains(id) {
                locks._root_plan_guards.push(guard);
            } else {
                locks.mutable_guards.push(guard);
            }
        }
        Ok(locks)
    }

    async fn acquire_batch_locks(
        &self,
        batch: &CoreMutationBatch,
    ) -> Result<CoreStoreMutationLocks> {
        let publication_root_hashes = batch
            .root_publications
            .iter()
            .map(|publication| root_key_hash(&publication.root_anchor_key))
            .collect::<BTreeSet<_>>();
        let mut acquired_keys = BTreeSet::new();
        for _ in 0..CORE_PROCESS_LOCK_RETRY_ATTEMPTS {
            let lock_keys = self.batch_lock_keys(batch)?;
            let guards = self
                .acquire_mutation_lock_keys(&lock_keys, &publication_root_hashes)
                .await?;

            // Deletions discover their root from the current row. Recompute while
            // row locks are held so a concurrent writer cannot make us miss a
            // root lock; if the required set grew, reacquire everything in the
            // global sorted order to avoid deadlocks.
            let stable_lock_keys = self.batch_lock_keys(batch)?;
            if stable_lock_keys.is_subset(&lock_keys) {
                return Ok(guards);
            }
            acquired_keys = stable_lock_keys;
        }

        bail!(
            "CoreStore mutation batch locks changed too often while acquiring: {:?}",
            acquired_keys
        )
    }

    pub(super) fn insert_precondition_lock_keys(
        &self,
        lock_keys: &mut BTreeSet<(String, String)>,
        precondition: &CoreMutationPrecondition,
    ) -> Result<()> {
        match precondition {
            CoreMutationPrecondition::Fence { fence_name, .. } => {
                lock_keys.insert(("fence".to_string(), fence_name.clone()));
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
                Self::insert_coremeta_row_lock(lock_keys, cf, *table_id, tuple_key);
                if let Some(payload) = self.read_coremeta_row(cf, *table_id, tuple_key)? {
                    insert_coremeta_root_lock_from_payload(lock_keys, &payload)?;
                }
            }
            CoreMutationPrecondition::StreamHead { stream_id, .. } => {
                lock_keys.insert(("stream".to_string(), stream_id.clone()));
            }
        }
        Ok(())
    }

    pub(super) fn explicit_transaction_lock_keys(
        &self,
        transaction: &CoreTransaction,
        preconditions: &[CoreTransactionPreconditionRow],
    ) -> Result<BTreeSet<(String, String)>> {
        validate_transaction_root_scope(transaction)?;
        let mut lock_keys = BTreeSet::new();
        lock_keys.insert((
            "transaction".to_string(),
            transaction.transaction_id.clone(),
        ));
        lock_keys.insert((
            "coremeta-root".to_string(),
            transaction.root_key_hash.clone(),
        ));
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::StreamAppend { stream_id, .. } => {
                    lock_keys.insert(("stream".to_string(), stream_id.clone()));
                }
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    Self::insert_coremeta_row_lock(&mut lock_keys, cf, *table_id, tuple_key);
                    insert_coremeta_root_lock_from_payload(&mut lock_keys, payload)?;
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    Self::insert_coremeta_row_lock(&mut lock_keys, cf, *table_id, tuple_key);
                }
            }
        }
        for precondition in preconditions {
            self.insert_precondition_lock_keys(&mut lock_keys, &precondition.precondition)?;
        }
        Ok(lock_keys)
    }

    async fn acquire_current_explicit_transaction_locks(
        &self,
        transaction_id: &str,
    ) -> Result<(
        CoreStoreMutationLocks,
        CoreTransaction,
        Vec<CoreTransactionPreconditionRow>,
    )> {
        let mut acquired_keys = BTreeSet::new();
        for _ in 0..CORE_PROCESS_LOCK_RETRY_ATTEMPTS {
            let transaction = self
                .read_transaction_unlocked(transaction_id)
                .await?
                .ok_or_else(|| anyhow!("TransactionNotFound"))?;
            let preconditions = self
                .read_transaction_preconditions_unlocked(transaction_id)
                .await?;
            let lock_keys = self.explicit_transaction_lock_keys(&transaction, &preconditions)?;
            let publication_root_hashes = BTreeSet::from([transaction.root_key_hash.clone()]);
            let guards = self
                .acquire_mutation_lock_keys(&lock_keys, &publication_root_hashes)
                .await?;
            let current = self
                .read_transaction_unlocked(transaction_id)
                .await?
                .ok_or_else(|| anyhow!("TransactionNotFound"))?;
            let current_preconditions = self
                .read_transaction_preconditions_unlocked(transaction_id)
                .await?;
            let stable_lock_keys =
                self.explicit_transaction_lock_keys(&current, &current_preconditions)?;
            if stable_lock_keys.is_subset(&lock_keys) {
                return Ok((guards, current, current_preconditions));
            }
            acquired_keys = stable_lock_keys;
        }
        bail!(
            "CoreStore explicit transaction locks changed too often while acquiring: {:?}",
            acquired_keys
        )
    }

    fn batch_lock_keys(&self, batch: &CoreMutationBatch) -> Result<BTreeSet<(String, String)>> {
        let mut lock_keys = BTreeSet::new();
        lock_keys.insert(("transaction".to_string(), batch.transaction_id.clone()));
        for publication in &batch.root_publications {
            lock_keys.insert((
                "coremeta-root".to_string(),
                root_key_hash(&publication.root_anchor_key),
            ));
        }
        if let Some(header) = self.read_transaction_header_row_unlocked(&batch.transaction_id)? {
            lock_keys.insert((
                "coremeta-root".to_string(),
                header.transaction.root_key_hash,
            ));
        }
        for precondition in &batch.preconditions {
            self.insert_precondition_lock_keys(&mut lock_keys, precondition)?;
        }
        for operation in &batch.operations {
            match operation {
                CoreMutationOperation::StreamAppend { stream_id, .. } => {
                    lock_keys.insert(("stream".to_string(), stream_id.clone()));
                }
                CoreMutationOperation::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    Self::insert_coremeta_row_lock(&mut lock_keys, cf, *table_id, tuple_key);
                    insert_coremeta_root_lock_from_payload(&mut lock_keys, payload)?;
                }
                CoreMutationOperation::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    Self::insert_coremeta_row_lock(&mut lock_keys, cf, *table_id, tuple_key);
                    if let Some(payload) = self.read_coremeta_row(cf, *table_id, tuple_key)? {
                        insert_coremeta_root_lock_from_payload(&mut lock_keys, &payload)?;
                    }
                }
            }
        }
        Ok(lock_keys)
    }

    pub(super) fn insert_coremeta_row_lock(
        lock_keys: &mut BTreeSet<(String, String)>,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
    ) {
        lock_keys.insert((
            "coremeta-row".to_string(),
            format!("{cf}:{table_id}:{}", sha256_hex(tuple_key)),
        ));
    }

    pub async fn list_stream_ids_page(
        &self,
        prefix: &str,
        after_stream_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        if after_stream_id.is_some_and(|stream_id| !stream_id.starts_with(prefix)) {
            bail!("CoreStore stream page cursor is outside the requested prefix");
        }
        let mut ids = Vec::with_capacity(limit);
        let tuple_prefix = stream_head_prefix(prefix);
        let after_key = after_stream_id.map(stream_head_key);
        for item in self.scan_coremeta_prefix_page(
            CF_STREAM_HEADS,
            TABLE_STREAM_HEAD_ROW,
            &tuple_prefix,
            after_key.as_deref(),
            limit,
        )? {
            let head = decode_stream_head_record(&item.payload)?;
            if head.schema != "anvil.core.stream_head.v1" {
                bail!("CoreStore stream head metadata row has invalid schema");
            }
            if head.stream_id.starts_with(prefix) && head.record_count > 0 {
                ids.push(head.stream_id);
            }
        }
        Ok(ids)
    }

    pub async fn commit_mutation_batch(
        &self,
        mut batch: CoreMutationBatch,
    ) -> Result<CoreMutationBatchReceipt> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "commit_mutation_batch")],
        );
        let total_start = std::time::Instant::now();
        let timing_name = batch.transaction_id.clone();
        validate_logical_id(&batch.transaction_id, "transaction id")?;
        validate_logical_id(&batch.scope_partition, "transaction scope partition")?;
        validate_logical_id(&batch.committed_by_principal, "transaction principal")?;
        if batch.operations.is_empty() {
            bail!("CoreStore mutation batch must include at least one operation");
        }
        Self::complete_implicit_stream_root_publications(&mut batch)?;
        validate_batch_partitions(&batch)?;
        let requested_preconditions_hash = core_mutation_preconditions_hash(&batch.preconditions)?;
        let requested_operations_hash =
            core_mutation_logical_operations_hash(&batch.operations, &batch.root_publications)?;

        let step_start = std::time::Instant::now();
        let operation_guards = self.acquire_batch_locks(&batch).await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch acquire_batch_locks tx={timing_name}"),
            step_start.elapsed(),
        );
        self.validate_mutation_root_publications_unlocked(&batch, false)?;
        let step_start = std::time::Instant::now();
        if let Some(transaction) = self
            .read_transaction_unlocked(&batch.transaction_id)
            .await?
        {
            if matches!(
                transaction.state,
                CoreTransactionState::Committed | CoreTransactionState::FinalisationFailed
            ) {
                validate_implicit_mutation_replay(
                    &transaction,
                    &batch,
                    &requested_preconditions_hash,
                    &requested_operations_hash,
                )?;
                if transaction.state == CoreTransactionState::FinalisationFailed {
                    return Ok(receipt_from_transaction(&transaction));
                }
                let generation = transaction.committed_root_generation.ok_or_else(|| {
                    anyhow!("CoreStore terminal mutation transaction has no coordinator generation")
                })?;
                if self.root_generation_is_published(
                    &transaction.root_key_hash,
                    generation,
                    &transaction.transaction_id,
                )? {
                    return Ok(receipt_from_transaction(&transaction));
                }
                bail!(
                    "CoreStore admitted mutation coordinator publication is incomplete; durable repair is required"
                );
            }
            bail!(
                "CoreStore transaction {} already exists with non-implicit state {}",
                batch.transaction_id,
                core_transaction_state_name(transaction.state)
            );
        }
        self.bind_mutation_batch_root_generations_unlocked(&mut batch)
            .await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch read_transaction tx={timing_name}"),
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        self.validate_mutation_preconditions_unlocked(
            &batch.preconditions,
            &batch.committed_by_principal,
            None,
        )
        .await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch validate_preconditions tx={timing_name}"),
            step_start.elapsed(),
        );
        let batch_payload = encode_core_mutation_batch(&batch)?;
        // Run admission and finalisation in an owned task. Once this task is
        // spawned, cancelling an RPC cannot strand a durable pending mutation.
        let store = self.clone();
        let finalisation_timing_name = timing_name.clone();
        let finalisation: tokio::task::JoinHandle<Result<CoreMutationBatchReceipt>> = tokio::spawn(
            async move {
                let mut operation_guards = operation_guards;
                let pending_mutation_payload =
                    if batch_payload.len() <= CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES {
                        CorePendingMutationPayload::Inline(&batch_payload)
                    } else {
                        CorePendingMutationPayload::Landed(&batch_payload)
                    };
                let step_start = std::time::Instant::now();
                let admission = store
                    .admit_core_mutation(
                        "mutation.batch",
                        WriterFamily::CoreControl.as_str(),
                        CorePendingMutationTarget::MutationBatch {
                            transaction_id: batch.transaction_id.clone(),
                            scope_partition: batch.scope_partition.clone(),
                            operation_count: batch.operations.len() as u64,
                        },
                        batch.transaction_id.clone(),
                        Some(batch.transaction_id.clone()),
                        pending_mutation_payload,
                        Vec::new(),
                    )
                    .await?;
                crate::emit_test_timing(
                    format!(
                        "core_store.commit_mutation_batch admission tx={finalisation_timing_name}"
                    ),
                    step_start.elapsed(),
                );
                // Mutable row, stream, and fence guards are revalidated at the
                // publication linearization point. Keep only the canonical root
                // planning guards so a concurrent writer cannot reserve the same
                // successor generation before this publication is durable.
                operation_guards.release_mutable_guards();
                let first_attempt = store
                    .finalise_admitted_mutation_batch(&batch, &admission, &finalisation_timing_name)
                    .await;
                drop(operation_guards);
                match first_attempt {
                    Ok(receipt) => Ok(receipt),
                    Err(first_error) => {
                        let retryable_conflict = is_retryable_mutation_conflict(&first_error);
                        tracing::error!(
                            transaction_id = %batch.transaction_id,
                            error = %first_error,
                            "CoreStore admitted mutation finalisation failed; recovering in-process"
                        );
                        let recovery = store.recover_admitted_mutation_batch(batch, &admission);
                        let receipt = recovery.await.with_context(|| {
                            format!(
                                "recover admitted CoreStore mutation after finalisation error: {first_error:#}"
                            )
                        })?;
                        store
                            .mark_pending_mutation_finalised_unlocked(
                                &admission,
                                core_transaction_state_name(receipt.state),
                            )
                            .await?;
                        if retryable_conflict {
                            Err(first_error)
                        } else {
                            Ok(receipt)
                        }
                    }
                }
            },
        );
        let receipt = finalisation
            .await
            .context("join admitted CoreStore mutation finalisation task")??;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch total tx={timing_name}"),
            total_start.elapsed(),
        );
        self.notify_stream_updates(&receipt.visible_updates);
        Ok(receipt)
    }

    async fn finalise_admitted_mutation_batch(
        &self,
        batch: &CoreMutationBatch,
        admission: &CorePendingMutationRecord,
        timing_name: &str,
    ) -> Result<CoreMutationBatchReceipt> {
        self.finalise_admitted_mutation_batch_with_error(batch, admission, timing_name, None, true)
            .await
    }

    async fn finalise_admitted_mutation_batch_with_error(
        &self,
        batch: &CoreMutationBatch,
        admission: &CorePendingMutationRecord,
        timing_name: &str,
        initial_error: Option<String>,
        revalidate_preconditions: bool,
    ) -> Result<CoreMutationBatchReceipt> {
        let step_start = std::time::Instant::now();
        let mut prepared_coremeta_ops = Vec::new();
        let (visible_updates, finalisation_error) = match initial_error {
            Some(error) => (Vec::new(), Some(error)),
            None => match self.prepare_mutation_batch_operations_unlocked(batch).await {
                Ok((ops, updates)) => {
                    prepared_coremeta_ops = ops;
                    (updates, None)
                }
                Err(error) => (Vec::new(), Some(format!("{error:#}"))),
            },
        };
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch operations tx={timing_name}"),
            step_start.elapsed(),
        );

        let transaction_state = if finalisation_error.is_some() {
            CoreTransactionState::FinalisationFailed
        } else {
            CoreTransactionState::Committed
        };
        let transaction_visible_updates = if finalisation_error.is_some() {
            Vec::new()
        } else {
            visible_updates.clone()
        };
        let writer_families = batch
            .root_publications
            .iter()
            .find(|publication| publication.transaction_coordinator)
            .ok_or_else(|| anyhow!("CoreStore mutation batch has no coordinator publication"))?
            .writer_families
            .clone();
        let mut transaction = CoreTransaction {
            schema: CORE_TRANSACTION_SCHEMA.to_string(),
            transaction_id: batch.transaction_id.clone(),
            scope_partition: batch.scope_partition.clone(),
            state: transaction_state,
            preconditions_hash: core_mutation_preconditions_hash(&batch.preconditions)?,
            operations_hash: core_mutation_logical_operations_hash(
                &batch.operations,
                &batch.root_publications,
            )?,
            writer_families,
            visible_updates: transaction_visible_updates.clone(),
            finalisation_error: finalisation_error.clone(),
            committed_at: now_rfc3339(),
            committed_by_principal: batch.committed_by_principal.clone(),
            created_at_unix_nanos: current_unix_nanos_u64()?,
            expires_at_unix_nanos: 0,
            root_anchor_key: batch.scope_partition.clone(),
            root_key_hash: root_key_hash(&batch.scope_partition),
            committed_root_generation: None,
            purpose: "implicit_mutation_batch".to_string(),
            failure_evidence: finalisation_error.clone(),
            outcome: core_transaction_state_name(transaction_state).to_string(),
        };
        if transaction_state == CoreTransactionState::Committed {
            transaction.committed_root_generation = Some(
                self.infer_implicit_transaction_coordinator_generation_unlocked(&transaction)
                    .await?,
            );
        }
        let step_start = std::time::Instant::now();
        let transaction_ops = self
            .transaction_rows_as_coremeta_ops_unlocked(
                &transaction,
                &batch.preconditions,
                revalidate_preconditions && finalisation_error.is_none(),
            )
            .await?;
        prepared_coremeta_ops.extend(transaction_ops);
        self.mark_pending_mutation_finalised_with_result_and_ops_unlocked(
            admission,
            core_transaction_state_name(transaction_state),
            None,
            prepared_coremeta_ops,
        )
        .await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch write_transaction tx={timing_name}"),
            step_start.elapsed(),
        );

        Ok(CoreMutationBatchReceipt {
            transaction_id: batch.transaction_id.clone(),
            scope_partition: batch.scope_partition.clone(),
            state: transaction_state,
            visible_updates: transaction_visible_updates,
            finalisation_error,
        })
    }

    pub(super) async fn recover_admitted_mutation_batch(
        &self,
        batch: CoreMutationBatch,
        admission: &CorePendingMutationRecord,
    ) -> Result<CoreMutationBatchReceipt> {
        let operation_guards = self.acquire_batch_locks(&batch).await?;
        self.recover_admitted_mutation_batch_prelocked(batch, admission, operation_guards)
            .await
    }

    async fn recover_admitted_mutation_batch_prelocked(
        &self,
        batch: CoreMutationBatch,
        admission: &CorePendingMutationRecord,
        mut operation_guards: CoreStoreMutationLocks,
    ) -> Result<CoreMutationBatchReceipt> {
        validate_logical_id(&batch.transaction_id, "transaction id")?;
        validate_logical_id(&batch.scope_partition, "transaction scope partition")?;
        validate_logical_id(&batch.committed_by_principal, "transaction principal")?;
        if batch.operations.is_empty() {
            bail!("CoreStore mutation batch must include at least one operation");
        }
        if let Some(intent) = self.read_root_publication_intent(&batch.transaction_id)?
            && let Err(error) = intent.ensure_pending()
        {
            // The durable publication intent is the terminal authority for an
            // admitted mutation. Resolve it before validating roots against a
            // newer winning generation; that validation cannot make a
            // superseded publication viable again.
            drop(operation_guards);
            return self
                .finalise_terminal_admitted_mutation_batch(&batch, admission, error)
                .await;
        }
        validate_batch_partitions(&batch)?;
        self.validate_admitted_mutation_root_publications(&batch, false)?;
        validate_admitted_batch_root_bindings(&batch)?;
        let requested_preconditions_hash = core_mutation_preconditions_hash(&batch.preconditions)?;
        let requested_operations_hash =
            core_mutation_logical_operations_hash(&batch.operations, &batch.root_publications)?;

        if let Some(transaction) = self
            .read_transaction_unlocked(&batch.transaction_id)
            .await?
        {
            if matches!(
                transaction.state,
                CoreTransactionState::Committed | CoreTransactionState::FinalisationFailed
            ) {
                validate_implicit_mutation_replay(
                    &transaction,
                    &batch,
                    &requested_preconditions_hash,
                    &requested_operations_hash,
                )?;
                if transaction.state == CoreTransactionState::FinalisationFailed {
                    return Ok(receipt_from_transaction(&transaction));
                }
                let generation = transaction.committed_root_generation.ok_or_else(|| {
                    anyhow!(
                        "CoreStore terminal recovered transaction has no coordinator generation"
                    )
                })?;
                if self.root_generation_is_published(
                    &transaction.root_key_hash,
                    generation,
                    &transaction.transaction_id,
                )? {
                    return Ok(receipt_from_transaction(&transaction));
                }
                bail!("CoreStore recovered transaction coordinator publication is incomplete");
            }
            bail!(
                "CoreStore transaction {} already exists with non-implicit state {}",
                batch.transaction_id,
                core_transaction_state_name(transaction.state)
            );
        }
        let has_published_effect = self
            .mutation_batch_has_published_effect_unlocked(&batch)
            .await?;
        let precondition_error = if has_published_effect {
            None
        } else {
            self.validate_mutation_preconditions_unlocked(
                &batch.preconditions,
                &batch.committed_by_principal,
                None,
            )
            .await
            .err()
            .map(|error| format!("{error:#}"))
        };
        let revalidate_preconditions = !has_published_effect && precondition_error.is_none();
        operation_guards.release_mutable_guards();
        let result = self
            .finalise_admitted_mutation_batch_with_error(
                &batch,
                admission,
                "recovery",
                precondition_error,
                revalidate_preconditions,
            )
            .await;
        drop(operation_guards);
        match result {
            Err(error)
                if super::local_root_publication_recovery::publication_terminal_reason(&error)
                    .is_some() =>
            {
                self.finalise_terminal_admitted_mutation_batch(&batch, admission, error)
                    .await
            }
            result => result,
        }
    }

    async fn finalise_terminal_admitted_mutation_batch(
        &self,
        batch: &CoreMutationBatch,
        admission: &CorePendingMutationRecord,
        error: anyhow::Error,
    ) -> Result<CoreMutationBatchReceipt> {
        self.finalise_admitted_mutation_batch_with_error(
            batch,
            admission,
            "terminal-recovery",
            Some(format!("{error:#}")),
            false,
        )
        .await
    }

    async fn mutation_batch_has_published_effect_unlocked(
        &self,
        batch: &CoreMutationBatch,
    ) -> Result<bool> {
        for publication in &batch.root_publications {
            let Some(anchor) = self
                .read_latest_root_anchor(&publication.root_anchor_key)
                .await?
            else {
                continue;
            };
            if anchor.mutation_first.as_deref() == Some(batch.transaction_id.as_str())
                && anchor.mutation_last.as_deref() == Some(batch.transaction_id.as_str())
            {
                return Ok(true);
            }
        }

        for operation in &batch.operations {
            let CoreMutationOperation::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } = operation
            else {
                continue;
            };
            let cf = canonical_coremeta_cf_name(cf)?;
            if self
                .committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?
                .as_deref()
                == Some(payload)
            {
                let common = core_meta_row_common_from_payload(payload)?;
                if common.root_key_hash.is_empty() || common.transaction_id == batch.transaction_id
                {
                    return Ok(true);
                }
            }
        }

        let mut stream_positions = BTreeMap::<String, (u64, String)>::new();
        for operation in &batch.operations {
            let CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                record_kind,
                payload,
                idempotency_key,
            } = operation
            else {
                continue;
            };
            if !stream_positions.contains_key(stream_id) {
                let Some(position) =
                    super::local_mutation_preparation::stream_precondition(batch, stream_id)?
                else {
                    continue;
                };
                stream_positions.insert(stream_id.clone(), position);
            }
            let (previous_sequence, previous_hash) = stream_positions
                .get(stream_id)
                .cloned()
                .ok_or_else(|| anyhow!("CoreStore recovery stream position is missing"))?;
            let sequence = previous_sequence
                .checked_add(1)
                .ok_or_else(|| anyhow!("CoreStore stream sequence overflow"))?;
            let Some(record) = self
                .read_stream_record_from_meta(stream_id, sequence)
                .await?
            else {
                continue;
            };
            if super::local_mutation_preparation::validate_existing_stream_operation(
                batch,
                partition_id,
                stream_id,
                record_kind,
                payload,
                idempotency_key.as_deref(),
                sequence,
                &previous_hash,
                &record,
            )
            .is_ok()
            {
                return Ok(true);
            }
            stream_positions.remove(stream_id);
        }
        Ok(false)
    }

    async fn infer_implicit_transaction_coordinator_generation_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<u64> {
        let mut bound_generation = None;
        for update in &transaction.visible_updates {
            if let CoreTransactionUpdate::CoreMetaPut { payload, .. } = update {
                let common = core_meta_row_common_from_payload(payload)?;
                if common.root_key_hash == transaction.root_key_hash {
                    merge_implicit_coordinator_generation(
                        &mut bound_generation,
                        common.root_generation,
                    )?;
                }
            }
        }
        self.implicit_root_generation_unlocked(
            &transaction.transaction_id,
            &transaction.root_anchor_key,
            bound_generation,
        )
        .await
    }

    pub(super) async fn implicit_root_generation_unlocked(
        &self,
        transaction_id: &str,
        root_anchor_key: &str,
        bound_generation: Option<u64>,
    ) -> Result<u64> {
        let latest = self.read_latest_root_anchor(root_anchor_key).await?;
        let expected_generation = match latest {
            Some(anchor)
                if anchor.mutation_first.as_deref() == Some(transaction_id)
                    && anchor.mutation_last.as_deref() == Some(transaction_id) =>
            {
                anchor.root_generation
            }
            Some(anchor) => anchor
                .root_generation
                .checked_add(1)
                .ok_or_else(|| anyhow!("CoreMeta root generation overflow"))?,
            None => 1,
        };
        if bound_generation.is_some_and(|bound| bound != expected_generation) {
            bail!(
                "CoreMeta mutation bound root generation does not match current publication state"
            );
        }
        Ok(bound_generation.unwrap_or(expected_generation))
    }

    pub(super) async fn rooted_delete_generation_unlocked(
        &self,
        batch: &CoreMutationBatch,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
    ) -> Result<Option<u64>> {
        let cf = canonical_coremeta_cf_name(cf)?;
        let Some(payload) = self.committed_coremeta_payload_unlocked(cf, table_id, tuple_key)?
        else {
            return Ok(None);
        };
        let common = core_meta_row_common_from_payload(&payload)?;
        if common.root_key_hash.is_empty() {
            return Ok(None);
        }
        let publication = batch
            .root_publications
            .iter()
            .find(|publication| root_key_hash(&publication.root_anchor_key) == common.root_key_hash)
            .ok_or_else(|| {
                anyhow!(
                    "CoreMeta rooted delete does not declare canonical root {}",
                    common.root_key_hash
                )
            })?;
        let mut bound_generation = None;
        for operation in &batch.operations {
            let CoreMutationOperation::CoreMetaPut { payload, .. } = operation else {
                continue;
            };
            let put_common = core_meta_row_common_from_payload(payload)?;
            if put_common.root_key_hash == common.root_key_hash {
                merge_implicit_coordinator_generation(
                    &mut bound_generation,
                    put_common.root_generation,
                )?;
            }
        }
        self.implicit_root_generation_unlocked(
            &batch.transaction_id,
            &publication.root_anchor_key,
            bound_generation,
        )
        .await
        .map(Some)
    }

    async fn prepare_mutation_batch_operations_unlocked(
        &self,
        batch: &CoreMutationBatch,
    ) -> Result<(
        Vec<super::local_tx_rows::OwnedCoreMetaBatchOp>,
        Vec<CoreTransactionUpdate>,
    )> {
        super::local_mutation_preparation::prepare_mutation_batch_operations(self, batch).await
    }

    pub async fn read_transaction(&self, transaction_id: &str) -> Result<Option<CoreTransaction>> {
        validate_logical_id(transaction_id, "transaction id")?;
        self.read_transaction_unlocked(transaction_id).await
    }

    pub async fn read_explicit_transaction_for_principal(
        &self,
        transaction_id: &str,
        principal: &str,
    ) -> Result<CoreTransaction> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        let transaction = self
            .read_transaction_unlocked(transaction_id)
            .await?
            .ok_or_else(|| anyhow!("TransactionNotFound"))?;
        if transaction.committed_by_principal != principal {
            bail!("TransactionPrincipalMismatch");
        }
        if transaction.state == CoreTransactionState::Open
            && transaction.expires_at_unix_nanos != 0
            && current_unix_nanos_u64()? >= transaction.expires_at_unix_nanos
        {
            let (_guards, current, _preconditions) = self
                .acquire_current_explicit_transaction_locks(transaction_id)
                .await?;
            if current.committed_by_principal != principal {
                bail!("TransactionPrincipalMismatch");
            }
            if current.state != CoreTransactionState::Open
                || current.expires_at_unix_nanos == 0
                || current_unix_nanos_u64()? < current.expires_at_unix_nanos
            {
                return Ok(current);
            }
            let expired = transaction_with_state(
                current,
                CoreTransactionState::Expired,
                Some("TransactionExpired".to_string()),
            )?;
            self.write_transaction_unlocked(&expired).await?;
            return Ok(expired);
        }
        Ok(transaction)
    }

    #[cfg(test)]
    pub(crate) async fn expire_explicit_transaction_for_tests(
        &self,
        transaction_id: &str,
        principal: &str,
    ) -> Result<()> {
        let (_guards, current, _preconditions) = self
            .acquire_current_explicit_transaction_locks(transaction_id)
            .await?;
        if current.committed_by_principal != principal {
            bail!("TransactionPrincipalMismatch");
        }
        if current.state != CoreTransactionState::Open {
            bail!("TransactionNotOpen");
        }
        let expired = transaction_with_state(
            current,
            CoreTransactionState::Expired,
            Some("TransactionExpired".to_string()),
        )?;
        self.write_transaction_unlocked(&expired).await
    }

    pub fn root_key_hash_for_anchor(root_anchor_key: &str) -> String {
        root_key_hash(root_anchor_key)
    }

    pub(crate) async fn infer_explicit_transaction_commit_root_generation(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<u64> {
        let (_guards, current, _preconditions) = self
            .acquire_current_explicit_transaction_locks(&transaction.transaction_id)
            .await?;
        self.infer_explicit_transaction_commit_root_generation_unlocked(&current)
            .await
    }

    pub async fn begin_explicit_transaction(
        &self,
        input: CoreBeginTransaction,
    ) -> Result<CoreTransaction> {
        validate_logical_id(&input.idempotency_key, "transaction idempotency key")?;
        validate_logical_id(&input.root_anchor_key, "transaction root anchor key")?;
        validate_hash(&input.root_key_hash, "transaction root key hash")?;
        validate_logical_id(&input.scope_partition, "transaction scope partition")?;
        validate_logical_id(&input.principal, "transaction principal")?;
        validate_transaction_scope_fields(
            &input.root_anchor_key,
            &input.root_key_hash,
            &input.scope_partition,
        )?;
        let ttl_ms = input.ttl_ms.clamp(1, 3_600_000);
        let created_at_unix_nanos = current_unix_nanos_u64()?;
        let expires_at_unix_nanos =
            created_at_unix_nanos.saturating_add(ttl_ms.saturating_mul(1_000_000));
        let transaction_id = explicit_transaction_id(
            &input.principal,
            &input.root_key_hash,
            &input.idempotency_key,
        );
        let transaction = CoreTransaction {
            schema: CORE_TRANSACTION_SCHEMA.to_string(),
            transaction_id,
            scope_partition: input.scope_partition,
            state: CoreTransactionState::Open,
            preconditions_hash: input.preconditions_hash,
            operations_hash: ZERO_HASH.to_string(),
            writer_families: vec![WriterFamily::CoreControl.as_str().to_string()],
            visible_updates: Vec::new(),
            finalisation_error: None,
            committed_at: String::new(),
            committed_by_principal: input.principal,
            created_at_unix_nanos,
            expires_at_unix_nanos,
            root_anchor_key: input.root_anchor_key,
            root_key_hash: input.root_key_hash,
            committed_root_generation: None,
            purpose: input.purpose,
            failure_evidence: None,
            outcome: "open".to_string(),
        };

        let lock_keys = self.explicit_transaction_lock_keys(&transaction, &[])?;
        let _guards = self.acquire_sorted_lock_keys(&lock_keys).await?;
        if let Some(existing) = self
            .read_transaction_unlocked(&transaction.transaction_id)
            .await?
        {
            if existing.root_anchor_key == transaction.root_anchor_key
                && existing.root_key_hash == transaction.root_key_hash
                && existing.scope_partition == transaction.scope_partition
                && existing.preconditions_hash == transaction.preconditions_hash
                && existing.committed_by_principal == transaction.committed_by_principal
            {
                return Ok(existing);
            }
            if !is_allowed_transaction_transition(&existing, &transaction) {
                bail!(
                    "CoreStore transaction {} idempotency conflict",
                    transaction.transaction_id
                );
            }
        }
        self.write_transaction_unlocked(&transaction).await?;
        Ok(transaction)
    }

    pub async fn stage_explicit_transaction_batch(
        &self,
        mut batch: CoreMutationBatch,
    ) -> Result<CoreMutationBatchReceipt> {
        validate_logical_id(&batch.transaction_id, "transaction id")?;
        validate_logical_id(&batch.scope_partition, "transaction scope partition")?;
        validate_logical_id(&batch.committed_by_principal, "transaction principal")?;
        if batch.operations.is_empty() {
            bail!("CoreStore explicit transaction stage must include at least one operation");
        }
        let _operation_guards = self.acquire_batch_locks(&batch).await?;
        let mut transaction = self
            .read_transaction_unlocked(&batch.transaction_id)
            .await?
            .ok_or_else(|| anyhow!("TransactionNotFound"))?;
        match transaction.state {
            CoreTransactionState::Open => {}
            CoreTransactionState::Expired => bail!("TransactionExpired"),
            CoreTransactionState::RolledBack | CoreTransactionState::Aborted => {
                bail!("TransactionRolledBack")
            }
            CoreTransactionState::Committed => bail!("TransactionAlreadyCommitted"),
            _ => bail!("TransactionNotOpen"),
        }
        if transaction.committed_by_principal != batch.committed_by_principal {
            bail!("TransactionPrincipalMismatch");
        }
        validate_transaction_root_scope(&transaction)?;
        if transaction.scope_partition != batch.scope_partition {
            bail!("TransactionScopeMismatch");
        }
        if transaction.expires_at_unix_nanos != 0
            && current_unix_nanos_u64()? >= transaction.expires_at_unix_nanos
        {
            let expired = transaction_with_state(
                transaction,
                CoreTransactionState::Expired,
                Some("TransactionExpired".to_string()),
            )?;
            self.write_transaction_unlocked(&expired).await?;
            self.notify_stream_updates(&expired.visible_updates);
            bail!("TransactionExpired");
        }
        validate_explicit_transaction_scope(&batch, &transaction)?;
        validate_batch_partitions(&batch)?;
        self.validate_mutation_root_publications_unlocked(&batch, true)?;
        let generation_bindings = BTreeMap::from([(transaction.root_key_hash.clone(), 0)]);
        self.bind_mutation_batch_to_generations(&mut batch, &generation_bindings)?;
        transaction.writer_families.extend(
            batch
                .root_publications
                .iter()
                .flat_map(|publication| publication.writer_families.iter().cloned()),
        );
        transaction.writer_families.sort();
        transaction.writer_families.dedup();
        self.validate_mutation_preconditions_unlocked(
            &batch.preconditions,
            &batch.committed_by_principal,
            Some(&batch.transaction_id),
        )
        .await?;

        let mut staged_updates = Vec::with_capacity(batch.operations.len());
        let mut stream_overlay = transaction.visible_updates.clone();
        let mut coremeta_batch_overlay = BTreeMap::<(String, u16, Vec<u8>), Option<Vec<u8>>>::new();
        for operation in &batch.operations {
            let update = match operation {
                CoreMutationOperation::StreamAppend {
                    partition_id,
                    stream_id,
                    record_kind,
                    payload,
                    idempotency_key,
                } => {
                    let update = self
                        .prepare_staged_stream_append_update_unlocked(
                            &transaction,
                            &stream_overlay,
                            partition_id,
                            stream_id,
                            record_kind,
                            payload,
                            idempotency_key.as_deref(),
                        )
                        .await?;
                    stream_overlay.push(update.clone());
                    update
                }
                CoreMutationOperation::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => {
                    ensure_coremeta_payload_in_transaction_scope(payload, &transaction)?;
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let previous_payload = match coremeta_batch_overlay.get(&key) {
                        Some(payload) => payload.clone(),
                        None => self.coremeta_payload_visible_to_transaction_unlocked(
                            cf,
                            *table_id,
                            tuple_key,
                            &transaction,
                        )?,
                    };
                    let previous_payload_hash = previous_payload
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(*table_id, payload));
                    let payload_hash = core_meta_payload_digest(*table_id, payload);
                    coremeta_batch_overlay.insert(key, Some(payload.clone()));
                    CoreTransactionUpdate::CoreMetaPut {
                        cf: cf.to_string(),
                        table_id: *table_id,
                        tuple_key: tuple_key.clone(),
                        previous_payload_hash,
                        payload: payload.clone(),
                        payload_hash,
                    }
                }
                CoreMutationOperation::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let previous_payload = match coremeta_batch_overlay.get(&key) {
                        Some(payload) => payload.clone(),
                        None => self.coremeta_payload_visible_to_transaction_unlocked(
                            cf,
                            *table_id,
                            tuple_key,
                            &transaction,
                        )?,
                    };
                    if let Some(payload) = previous_payload.as_ref() {
                        ensure_coremeta_payload_in_transaction_scope(payload, &transaction)?;
                    }
                    let previous_payload_hash = previous_payload
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(*table_id, payload));
                    coremeta_batch_overlay.insert(key, None);
                    CoreTransactionUpdate::CoreMetaDelete {
                        cf: cf.to_string(),
                        table_id: *table_id,
                        tuple_key: tuple_key.clone(),
                        previous_payload_hash,
                    }
                }
            };
            staged_updates.push(update);
        }

        for update in &staged_updates {
            if !transaction.visible_updates.contains(update) {
                transaction.visible_updates.push(update.clone());
            }
        }
        let staged_batch_hash =
            core_mutation_operations_hash(&batch.operations, &batch.root_publications)?;
        transaction.operations_hash = descriptor_hash(&[
            "anvil.explicit_transaction.operations.v1",
            &transaction.operations_hash,
            &staged_batch_hash,
        ]);
        transaction.outcome = "open".to_string();
        self.write_pending_transaction_with_staged_rows_unlocked(
            &transaction,
            &batch.preconditions,
        )
        .await?;
        Ok(CoreMutationBatchReceipt {
            transaction_id: batch.transaction_id,
            scope_partition: batch.scope_partition,
            state: CoreTransactionState::Open,
            visible_updates: staged_updates,
            finalisation_error: None,
        })
    }

    async fn prepare_staged_stream_append_update_unlocked(
        &self,
        transaction: &CoreTransaction,
        staged_updates: &[CoreTransactionUpdate],
        partition_id: &str,
        stream_id: &str,
        record_kind: &str,
        payload: &[u8],
        idempotency_key: Option<&str>,
    ) -> Result<CoreTransactionUpdate> {
        let idempotency_key_hash =
            idempotency_key.map(|key| format!("sha256:{}", sha256_hex(key.as_bytes())));
        if let Some(idempotency_key_hash) = idempotency_key_hash.as_deref()
            && let Some(existing) = staged_updates.iter().find(|update| {
                matches!(
                    update,
                    CoreTransactionUpdate::StreamAppend {
                        stream_id: existing_stream_id,
                        idempotency_key_hash: Some(existing_hash),
                        ..
                    } if existing_stream_id == stream_id && existing_hash == idempotency_key_hash
                )
            })
        {
            let CoreTransactionUpdate::StreamAppend {
                partition_id: existing_partition_id,
                record_kind: existing_record_kind,
                payload: existing_payload,
                ..
            } = existing
            else {
                unreachable!("stream update was selected above")
            };
            if existing_partition_id != partition_id
                || existing_record_kind != record_kind
                || existing_payload != payload
            {
                bail!("CoreStore stream idempotency conflict for staged transaction");
            }
            return Ok(existing.clone());
        }

        let staged_head = staged_updates.iter().rev().find_map(|update| match update {
            CoreTransactionUpdate::StreamAppend {
                stream_id: existing_stream_id,
                visible_sequence,
                prepared_record_hash,
                ..
            } if existing_stream_id == stream_id => {
                Some((*visible_sequence, prepared_record_hash.clone()))
            }
            _ => None,
        });
        let (last_sequence, previous_event_hash) = match staged_head {
            Some(head) => head,
            None => self
                .read_stream_head_from_meta(stream_id)?
                .map(|head| (head.last_sequence, head.last_event_hash))
                .unwrap_or_else(|| (0, ZERO_HASH.to_string())),
        };
        let record = build_stream_record_after_head(
            AppendStreamRecord {
                stream_id: stream_id.to_string(),
                partition_id: partition_id.to_string(),
                record_kind: record_kind.to_string(),
                payload: payload.to_vec(),
                content_type: None,
                user_metadata_json: "{}".to_string(),
                fence: None,
                transaction_id: Some(transaction.transaction_id.clone()),
                idempotency_key: None,
            },
            idempotency_key_hash,
            transaction.committed_by_principal.clone(),
            last_sequence,
            previous_event_hash,
            now_rfc3339(),
        )?;
        Ok(CoreTransactionUpdate::StreamAppend {
            partition_id: record.partition_id,
            stream_id: record.stream_id,
            record_kind: record.record_kind,
            payload: record.payload,
            idempotency_key_hash: record.idempotency_key_hash,
            visible_sequence: record.sequence,
            previous_event_hash: record.previous_event_hash,
            prepared_record_hash: record.event_hash,
            created_at: record.created_at,
        })
    }

    pub async fn stage_coremeta_put_in_transaction(
        &self,
        transaction_id: &str,
        principal: &str,
        cf: &str,
        table_id: u16,
        tuple_key: Vec<u8>,
        payload: Vec<u8>,
        expected_payload_hash: Option<String>,
        require_absent: bool,
        require_present: bool,
    ) -> Result<CoreMutationBatchReceipt> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        validate_coremeta_operation_payload(cf, table_id, &tuple_key, &payload)?;
        let transaction = self
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        validate_transaction_root_scope(&transaction)?;
        let scope_partition = transaction.scope_partition.clone();
        let root_publication = CoreMutationRootPublication::new(
            scope_partition.clone(),
            WriterFamily::CoreControl.as_str(),
        )
        .coordinator();
        self.stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction_id.to_string(),
            scope_partition,
            committed_by_principal: principal.to_string(),
            root_publications: vec![root_publication],
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id,
                tuple_key: tuple_key.clone(),
                expected_payload_hash,
                require_absent,
                require_present,
            }],
            operations: vec![CoreMutationOperation::CoreMetaPut {
                partition_id: transaction.root_anchor_key,
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id,
                tuple_key,
                payload,
            }],
        })
        .await
    }

    pub(crate) async fn stage_mutation_additions_in_transaction(
        &self,
        transaction_id: &str,
        principal: &str,
        additions: CoreMutationBatchAdditions,
    ) -> Result<CoreMutationBatchReceipt> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        if additions.operations.is_empty() {
            bail!("CoreStore transaction additions must include an operation");
        }
        if !additions.root_publications.is_empty() {
            bail!("explicit transaction additions cannot introduce publication roots");
        }
        let transaction = self
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        validate_transaction_root_scope(&transaction)?;
        let scope_partition = transaction.scope_partition.clone();
        self.stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction_id.to_string(),
            scope_partition: scope_partition.clone(),
            committed_by_principal: principal.to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    scope_partition,
                    WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
            ],
            preconditions: additions.preconditions,
            operations: additions.operations,
        })
        .await
    }

    pub async fn stage_coremeta_delete_in_transaction(
        &self,
        transaction_id: &str,
        principal: &str,
        cf: &str,
        table_id: u16,
        tuple_key: Vec<u8>,
        expected_payload_hash: Option<String>,
        require_present: bool,
    ) -> Result<CoreMutationBatchReceipt> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        validate_coremeta_operation_key(cf, table_id, &tuple_key)?;
        let transaction = self
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        validate_transaction_root_scope(&transaction)?;
        let scope_partition = transaction.scope_partition.clone();
        let root_publication = CoreMutationRootPublication::new(
            scope_partition.clone(),
            WriterFamily::CoreControl.as_str(),
        )
        .coordinator();
        self.stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction_id.to_string(),
            scope_partition,
            committed_by_principal: principal.to_string(),
            root_publications: vec![root_publication],
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id,
                tuple_key: tuple_key.clone(),
                expected_payload_hash,
                require_absent: false,
                require_present,
            }],
            operations: vec![CoreMutationOperation::CoreMetaDelete {
                partition_id: transaction.root_anchor_key,
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id,
                tuple_key,
            }],
        })
        .await
    }

    pub async fn commit_explicit_transaction(
        &self,
        transaction_id: &str,
        principal: &str,
    ) -> Result<CoreTransaction> {
        let total_started_at = std::time::Instant::now();
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        let step_started_at = std::time::Instant::now();
        let (mut guards, transaction, preconditions) = self
            .acquire_current_explicit_transaction_locks(transaction_id)
            .await?;
        crate::emit_test_timing(
            format!(
                "core_store.commit_explicit_transaction acquire_scoped_locks tx={transaction_id}"
            ),
            step_started_at.elapsed(),
        );
        if transaction.committed_by_principal != principal {
            bail!("TransactionPrincipalMismatch");
        }
        match transaction.state {
            CoreTransactionState::Committed => return Ok(transaction),
            CoreTransactionState::Open => {}
            CoreTransactionState::RolledBack | CoreTransactionState::Aborted => {
                bail!("TransactionRolledBack");
            }
            CoreTransactionState::Expired => bail!("TransactionExpired"),
            _ => bail!("TransactionNotCommittable"),
        }
        if transaction.expires_at_unix_nanos != 0
            && current_unix_nanos_u64()? >= transaction.expires_at_unix_nanos
        {
            let expired = transaction_with_state(
                transaction,
                CoreTransactionState::Expired,
                Some("TransactionExpired".to_string()),
            )?;
            self.write_transaction_unlocked(&expired).await?;
            bail!("TransactionExpired");
        }
        validate_transaction_root_scope(&transaction)?;
        let step_started_at = std::time::Instant::now();
        let committed_root_generation = self
            .infer_explicit_transaction_commit_root_generation_unlocked(&transaction)
            .await?;
        let mut publication_transaction = transaction.clone();
        self.bind_explicit_transaction_to_generation(
            &mut publication_transaction,
            committed_root_generation,
        )?;
        self.validate_staged_transaction_preconditions_unlocked(
            &preconditions,
            principal,
            &transaction,
        )
        .await?;
        self.validate_explicit_transaction_commit_unlocked(&publication_transaction)
            .await?;
        crate::emit_test_timing(
            format!("core_store.commit_explicit_transaction validate_commit tx={transaction_id}"),
            step_started_at.elapsed(),
        );
        let step_started_at = std::time::Instant::now();
        let mut committed = transaction_with_state(
            publication_transaction,
            CoreTransactionState::Committed,
            None,
        )?;
        committed.committed_root_generation = Some(committed_root_generation);
        // The publication intent revalidates mutable guards at final
        // linearization. Retain the root planning guard until its successor
        // generation has been durably published.
        guards.release_mutable_guards();
        let committed_transaction = self
            .commit_explicit_transaction_rows_and_coremeta_updates_unlocked(&committed)
            .await?;
        drop(guards);
        crate::emit_test_timing(
            format!("core_store.commit_explicit_transaction commit_rows tx={transaction_id}"),
            step_started_at.elapsed(),
        );
        crate::emit_test_timing(
            format!("core_store.commit_explicit_transaction total tx={transaction_id}"),
            total_started_at.elapsed(),
        );
        self.notify_stream_updates(&committed_transaction.visible_updates);
        Ok(committed_transaction)
    }

    pub async fn rollback_explicit_transaction(
        &self,
        transaction_id: &str,
        principal: &str,
        reason: &str,
    ) -> Result<CoreTransaction> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        let (_guards, transaction, _preconditions) = self
            .acquire_current_explicit_transaction_locks(transaction_id)
            .await?;
        if transaction.committed_by_principal != principal {
            bail!("TransactionPrincipalMismatch");
        }
        match transaction.state {
            CoreTransactionState::RolledBack => return Ok(transaction),
            CoreTransactionState::Committed => bail!("TransactionAlreadyCommitted"),
            _ => {}
        }
        let rolled_back = transaction_with_state(
            transaction,
            CoreTransactionState::RolledBack,
            Some(if reason.trim().is_empty() {
                "RollbackRequested".to_string()
            } else {
                format!("RollbackRequested: {reason}")
            }),
        )?;
        self.write_transaction_unlocked(&rolled_back).await?;
        self.notify_stream_updates(&rolled_back.visible_updates);
        Ok(rolled_back)
    }

    pub async fn commit_transaction(&self, transaction: CoreTransaction) -> Result<()> {
        if transaction.state != CoreTransactionState::Committed {
            bail!("CoreStore only persists committed transactions through commit_transaction");
        }
        validate_transaction_root_scope(&transaction)?;
        validate_logical_id(&transaction.transaction_id, "transaction id")?;
        let lock_keys = self.explicit_transaction_lock_keys(&transaction, &[])?;
        let _guards = self.acquire_sorted_lock_keys(&lock_keys).await?;
        self.write_transaction_unlocked(&transaction).await?;
        self.notify_stream_updates(&transaction.visible_updates);
        Ok(())
    }

    fn notify_stream_updates(&self, updates: &[CoreTransactionUpdate]) {
        for update in updates {
            if let CoreTransactionUpdate::StreamAppend { stream_id, .. } = update {
                self.storage.notify_stream(stream_id);
            }
        }
    }

    pub(super) async fn write_transaction_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<()> {
        if let Some(existing) = self
            .read_transaction_unlocked(&transaction.transaction_id)
            .await?
        {
            if existing.state == transaction.state
                && existing.preconditions_hash == transaction.preconditions_hash
                && existing.operations_hash == transaction.operations_hash
                && existing.visible_updates == transaction.visible_updates
                && existing.finalisation_error == transaction.finalisation_error
                && existing.committed_by_principal == transaction.committed_by_principal
                && existing.root_anchor_key == transaction.root_anchor_key
                && existing.root_key_hash == transaction.root_key_hash
                && existing.scope_partition == transaction.scope_partition
                && existing.failure_evidence == transaction.failure_evidence
                && existing.outcome == transaction.outcome
            {
                return Ok(());
            }
            if !is_allowed_transaction_transition(&existing, transaction) {
                bail!(
                    "CoreStore transaction {} idempotency conflict",
                    transaction.transaction_id
                );
            }
        }
        if transaction.state == CoreTransactionState::Committed {
            self.commit_explicit_transaction_rows_and_coremeta_updates_unlocked(transaction)
                .await?;
        } else {
            self.write_pending_transaction_with_staged_rows_unlocked(transaction, &[])
                .await?;
        }
        Ok(())
    }

    pub(super) async fn read_transaction_unlocked(
        &self,
        transaction_id: &str,
    ) -> Result<Option<CoreTransaction>> {
        self.read_transaction_from_rows_unlocked(transaction_id)
            .await
    }
}

fn validate_implicit_mutation_replay(
    transaction: &CoreTransaction,
    batch: &CoreMutationBatch,
    requested_preconditions_hash: &str,
    requested_operations_hash: &str,
) -> Result<()> {
    let coordinator = batch
        .root_publications
        .iter()
        .find(|publication| publication.transaction_coordinator)
        .ok_or_else(|| anyhow!("CoreStore mutation batch has no coordinator publication"))?;
    let expected_root_key_hash = root_key_hash(&batch.scope_partition);
    let exact_replay = transaction.purpose == "implicit_mutation_batch"
        && transaction.scope_partition == batch.scope_partition
        && transaction.committed_by_principal == batch.committed_by_principal
        && transaction.preconditions_hash == requested_preconditions_hash
        && transaction.operations_hash == requested_operations_hash
        && transaction.writer_families == coordinator.writer_families
        && transaction.root_anchor_key == batch.scope_partition
        && transaction.root_key_hash == expected_root_key_hash;
    if !exact_replay {
        bail!(
            "CoreStore transaction {} idempotency conflict",
            batch.transaction_id
        );
    }
    Ok(())
}

fn merge_implicit_coordinator_generation(current: &mut Option<u64>, candidate: u64) -> Result<()> {
    if candidate == 0 {
        bail!("CoreMeta coordinator root generation must be nonzero");
    }
    if current.is_some_and(|current| current != candidate) {
        bail!("CoreMeta mutation batch assigns multiple coordinator root generations");
    }
    *current = Some(candidate);
    Ok(())
}

fn validate_admitted_batch_root_bindings(batch: &CoreMutationBatch) -> Result<()> {
    let declared_roots = batch
        .root_publications
        .iter()
        .map(|publication| root_key_hash(&publication.root_anchor_key))
        .collect::<BTreeSet<_>>();
    for operation in &batch.operations {
        let CoreMutationOperation::CoreMetaPut { payload, .. } = operation else {
            continue;
        };
        let common = core_meta_row_common_from_payload(payload)?;
        if common.root_key_hash.is_empty() {
            continue;
        }
        if common.root_generation == 0 {
            bail!("CoreStore admitted mutation has an unbound root generation");
        }
        if common.transaction_id != batch.transaction_id {
            bail!("CoreStore admitted mutation has a mismatched transaction binding");
        }
        if !declared_roots.contains(&common.root_key_hash) {
            bail!(
                "CoreStore admitted mutation payload references undeclared root {}",
                common.root_key_hash
            );
        }
    }
    Ok(())
}

pub(super) fn transaction_lists_stream_record(
    transaction: &CoreTransaction,
    record: &StreamRecord,
) -> Result<bool> {
    Ok(transaction_lists_stream_record_identity(
        transaction,
        &record.stream_id,
        record.sequence,
        &record.event_hash,
    ))
}

pub(super) fn transaction_lists_stream_record_identity(
    transaction: &CoreTransaction,
    stream_id: &str,
    sequence: u64,
    event_hash: &str,
) -> bool {
    transaction.visible_updates.iter().any(|visible_update| {
        matches!(
            visible_update,
            CoreTransactionUpdate::StreamAppend {
                stream_id: update_stream_id,
                visible_sequence,
                prepared_record_hash,
                ..
            } if update_stream_id == stream_id
                && *visible_sequence == sequence
                && prepared_record_hash == event_hash
        )
    })
}

fn ensure_coremeta_payload_in_transaction_scope(
    payload: &[u8],
    transaction: &CoreTransaction,
) -> Result<()> {
    let common = core_meta_row_common_from_payload(payload)?;
    if !common.root_key_hash.is_empty() && common.root_key_hash != transaction.root_key_hash {
        bail!("TransactionScopeMismatch");
    }
    Ok(())
}

fn validate_explicit_transaction_scope(
    batch: &CoreMutationBatch,
    transaction: &CoreTransaction,
) -> Result<()> {
    if batch
        .root_publications
        .iter()
        .any(|publication| publication.root_anchor_key != transaction.scope_partition)
    {
        bail!("TransactionScopeMismatch");
    }
    for operation in &batch.operations {
        let partition_id = match operation {
            CoreMutationOperation::StreamAppend { partition_id, .. }
            | CoreMutationOperation::CoreMetaPut { partition_id, .. }
            | CoreMutationOperation::CoreMetaDelete { partition_id, .. } => partition_id,
        };
        if partition_id != &transaction.scope_partition {
            bail!("TransactionScopeMismatch");
        }
        match operation {
            CoreMutationOperation::StreamAppend { .. } => {}
            CoreMutationOperation::CoreMetaPut { payload, .. } => {
                let common = core_meta_row_common_from_payload(payload)?;
                if common.root_key_hash != transaction.root_key_hash
                    || common.visibility_state_enum() != CoreMetaVisibilityState::Committed
                {
                    bail!("TransactionScopeMismatch");
                }
            }
            CoreMutationOperation::CoreMetaDelete { .. } => {}
        }
    }
    Ok(())
}

fn is_allowed_transaction_transition(existing: &CoreTransaction, next: &CoreTransaction) -> bool {
    if existing.transaction_id != next.transaction_id
        || existing.scope_partition != next.scope_partition
        || existing.preconditions_hash != next.preconditions_hash
        || existing.committed_by_principal != next.committed_by_principal
        || existing.created_at_unix_nanos != next.created_at_unix_nanos
        || existing.expires_at_unix_nanos != next.expires_at_unix_nanos
        || existing.root_anchor_key != next.root_anchor_key
        || existing.root_key_hash != next.root_key_hash
    {
        return false;
    }

    match existing.state {
        CoreTransactionState::Open => match next.state {
            CoreTransactionState::Open => {
                has_prefix(&next.visible_updates, &existing.visible_updates)
            }
            CoreTransactionState::Prepared
            | CoreTransactionState::Committed
            | CoreTransactionState::FinalisationFailed
            | CoreTransactionState::Aborted
            | CoreTransactionState::RolledBack
            | CoreTransactionState::Expired
            | CoreTransactionState::Failed => true,
        },
        CoreTransactionState::Prepared => matches!(
            next.state,
            CoreTransactionState::Committed
                | CoreTransactionState::FinalisationFailed
                | CoreTransactionState::Aborted
                | CoreTransactionState::RolledBack
                | CoreTransactionState::Expired
                | CoreTransactionState::Failed
        ),
        CoreTransactionState::Committed
        | CoreTransactionState::FinalisationFailed
        | CoreTransactionState::Aborted
        | CoreTransactionState::RolledBack
        | CoreTransactionState::Expired
        | CoreTransactionState::Failed => false,
    }
}

pub(super) fn validate_core_meta_row_precondition(
    current: Option<&[u8]>,
    cf: &str,
    table_id: u16,
    tuple_key: &[u8],
    expected_payload_hash: Option<&str>,
    require_absent: bool,
    require_present: bool,
) -> Result<()> {
    if require_absent && current.is_some() {
        return Err(CoreStoreCommitError::CoreMetaRowPreconditionFailed {
            cf: cf.to_string(),
            table_id,
            tuple_key_hex: hex::encode(tuple_key),
            reason: "row must be absent".to_string(),
        }
        .into());
    }
    if require_present && current.is_none() {
        return Err(CoreStoreCommitError::CoreMetaRowPreconditionFailed {
            cf: cf.to_string(),
            table_id,
            tuple_key_hex: hex::encode(tuple_key),
            reason: "row must be present".to_string(),
        }
        .into());
    }
    if let (Some(expected), Some(payload)) = (expected_payload_hash, current) {
        let actual = core_meta_payload_digest(table_id, payload);
        if actual != expected {
            return Err(CoreStoreCommitError::CoreMetaRowPreconditionFailed {
                cf: cf.to_string(),
                table_id,
                tuple_key_hex: hex::encode(tuple_key),
                reason: format!("payload hash mismatch: expected {expected}, got {actual}"),
            }
            .into());
        }
    }
    Ok(())
}

fn has_prefix<T: PartialEq>(value: &[T], prefix: &[T]) -> bool {
    value.len() >= prefix.len() && &value[..prefix.len()] == prefix
}
