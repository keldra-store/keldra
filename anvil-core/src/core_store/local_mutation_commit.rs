use super::local_stream_control::control_record_proto::decode_stream_head_record;
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

    fn batch_lock_keys(&self, batch: &CoreMutationBatch) -> Result<BTreeSet<(String, String)>> {
        let mut lock_keys = BTreeSet::new();
        lock_keys.insert(("transaction".to_string(), batch.transaction_id.clone()));
        for publication in &batch.root_publications {
            lock_keys.insert((
                "coremeta-root".to_string(),
                root_key_hash(&publication.root_anchor_key),
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
        let step_start = std::time::Instant::now();
        let operation_guards = self.acquire_batch_locks(&batch).await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch acquire_batch_locks tx={timing_name}"),
            step_start.elapsed(),
        );
        self.validate_mutation_root_publications_unlocked(&batch, false)?;
        let step_start = std::time::Instant::now();
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
                let admission = match store
                    .admit_core_mutation_outcome(
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
                    .await?
                {
                    CoreAdmissionOutcome::Pending(admission) => admission,
                    CoreAdmissionOutcome::Finalised(finalisation) => {
                        return mutation_batch_receipt_from_finalisation(finalisation);
                    }
                };
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
        self.notify_committed_stream_appends(&receipt.stream_appends);
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
        _revalidate_preconditions: bool,
    ) -> Result<CoreMutationBatchReceipt> {
        let step_start = std::time::Instant::now();
        let mut prepared_coremeta_ops = Vec::new();
        let mut stream_appends = Vec::new();
        let finalisation_error = match initial_error {
            Some(error) => Some(error),
            None => match self.prepare_mutation_batch_operations_unlocked(batch).await {
                Ok(prepared) => {
                    prepared_coremeta_ops = prepared.owned_ops;
                    stream_appends = prepared.stream_appends;
                    None
                }
                Err(error) => Some(format!("{error:#}")),
            },
        };
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch operations tx={timing_name}"),
            step_start.elapsed(),
        );

        let outcome = if finalisation_error.is_some() {
            CoreMutationBatchOutcome::FinalisationFailed
        } else {
            CoreMutationBatchOutcome::Committed
        };
        let receipt = CoreMutationBatchReceipt {
            transaction_id: batch.transaction_id.clone(),
            scope_partition: batch.scope_partition.clone(),
            outcome,
            stream_appends: stream_appends.clone(),
            finalisation_error: finalisation_error.clone(),
        };
        let step_start = std::time::Instant::now();
        self.mark_pending_mutation_finalised_with_result_and_ops_unlocked(
            admission,
            match outcome {
                CoreMutationBatchOutcome::Committed => "committed",
                CoreMutationBatchOutcome::FinalisationFailed => "finalisation_failed",
            },
            Some(CorePendingMutationFinalisationResult::MutationBatchReceipt(
                receipt.clone(),
            )),
            prepared_coremeta_ops,
        )
        .await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch write_transaction tx={timing_name}"),
            step_start.elapsed(),
        );

        Ok(receipt)
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
        let has_published_effect = self
            .mutation_batch_has_published_effect_unlocked(&batch)
            .await?;
        let precondition_error = if has_published_effect {
            None
        } else {
            self.validate_mutation_preconditions_unlocked(
                &batch.preconditions,
                &batch.committed_by_principal,
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
    ) -> Result<super::local_mutation_preparation::PreparedMutationBatch> {
        super::local_mutation_preparation::prepare_mutation_batch_operations(self, batch).await
    }
}

impl CoreStore {
    fn notify_committed_stream_appends(&self, appends: &[CoreCommittedStreamAppend]) {
        for append in appends {
            self.storage.notify_stream(&append.stream_id);
        }
    }
}

fn mutation_batch_receipt_from_finalisation(
    finalisation: CorePendingMutationFinalisationRecord,
) -> Result<CoreMutationBatchReceipt> {
    match finalisation.result {
        Some(CorePendingMutationFinalisationResult::MutationBatchReceipt(receipt)) => Ok(receipt),
        _ => bail!(
            "CoreStore finalised mutation batch {} has no compact receipt",
            finalisation.mutation_id
        ),
    }
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
