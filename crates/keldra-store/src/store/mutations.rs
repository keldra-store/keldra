use super::evaluation_telemetry::{EvaluationSubphase, EvaluationSubphaseMetrics};
use super::journal_capacity::SourceJournalAdmission;
use super::mutation_helpers::{
    definition_mutation_error, definition_receipt_matches_intent, exact_version_key,
    head_accounting_transition, is_mutation_capacity, mutation_capacity_kind,
    validate_accounting_transition, version_retention,
};
use super::mutation_prefetch::MutationReadCache;
use super::mutation_types::{
    DistributedEvaluationContext, EvaluatedOperation, trusted_derived_put_if_absent_replay,
};
use super::receipt_codec::{decode_stored_receipt, encode_stored_receipt};
use super::*;
use crate::model::{
    CoordinatedObjectMutation, MUTATION_STAMP_FORMAT, MutationStamp, OBJECT_MUTATION_FORMAT,
    ObjectMutation, ObjectMutationContext, ObjectMutationGovernance, ReplicaObjectMutationApplied,
};
use crate::{DefinitionMutationIntent, DefinitionOperation, DefinitionTransition};

const MAX_EXPIRED_RECEIPTS_PRUNED_PER_PASS: usize = 1_024;
const MAX_EXPIRED_RECEIPT_BYTES_PRUNED_PER_PASS: u64 = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StagedLocalChanges {
    pub(super) previous_tail: u64,
    pub(super) status: WatchJournalStatus,
    pub(super) visibility_settlement_staged: bool,
}

impl Store {
    /// One-node WriteBatch request with coordinator-reconciled bucket options.
    pub async fn mutate_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, MutationError> {
        governance.validate()?;
        self.bulk_write_inner(
            vec![operation],
            Some(governance),
            None,
            false,
            SourceJournalAdmission::Bounded,
        )
        .await
        .pop()
        .expect("one operation has one outcome")
        .result
    }

    pub async fn mutate_with_governance_and_backpressure(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
    ) -> Result<MutationReceipt, MutationError> {
        governance.validate()?;
        self.bulk_write_inner(
            vec![operation],
            Some(governance),
            None,
            true,
            SourceJournalAdmission::Bounded,
        )
        .await
        .pop()
        .expect("one operation has one outcome")
        .result
    }

    /// Trusted definition mutation converted to a transition in the same batch.
    pub async fn mutate_definition_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        intent: DefinitionMutationIntent,
    ) -> Result<MutationReceipt, MutationError> {
        governance.validate()?;
        intent.validate().map_err(definition_mutation_error)?;
        self.bulk_write_inner(
            vec![operation],
            Some(governance),
            Some(intent),
            false,
            SourceJournalAdmission::Bounded,
        )
        .await
        .pop()
        .expect("one operation has one outcome")
        .result
    }

    pub async fn mutate_definition_with_governance_and_backpressure(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        intent: DefinitionMutationIntent,
    ) -> Result<MutationReceipt, MutationError> {
        governance.validate()?;
        intent.validate().map_err(definition_mutation_error)?;
        self.bulk_write_inner(
            vec![operation],
            Some(governance),
            Some(intent),
            true,
            SourceJournalAdmission::Bounded,
        )
        .await
        .pop()
        .expect("one operation has one outcome")
        .result
    }

    pub async fn bulk_write(&self, operations: Vec<BatchOperation>) -> Vec<BatchOutcome> {
        self.bulk_write_inner(
            operations,
            None,
            None,
            false,
            SourceJournalAdmission::Bounded,
        )
        .await
    }

    pub async fn bulk_write_with_backpressure(
        &self,
        operations: Vec<BatchOperation>,
    ) -> Vec<BatchOutcome> {
        self.bulk_write_inner(
            operations,
            None,
            None,
            true,
            SourceJournalAdmission::Bounded,
        )
        .await
    }

    pub(super) async fn bulk_write_inner(
        &self,
        operations: Vec<BatchOperation>,
        governance: Option<ObjectMutationGovernance>,
        definition_intent: Option<DefinitionMutationIntent>,
        backpressure: bool,
        source_journal_admission: SourceJournalAdmission,
    ) -> Vec<BatchOutcome> {
        let governance_supplied = governance.is_some();
        if definition_intent.is_some() && operations.len() != 1 {
            return operations
                .into_iter()
                .enumerate()
                .map(|(index, _)| BatchOutcome {
                    index,
                    result: Err(MutationError::InvalidObjectMutation(
                        "one typed definition intent must describe exactly one operation".into(),
                    )),
                })
                .collect();
        }
        let mut pending = Vec::with_capacity(operations.len());
        let mut completed = BTreeMap::<usize, Result<MutationReceipt, MutationError>>::new();
        let mut governance_cache =
            BTreeMap::<(String, String), Result<ObjectMutationGovernance, MutationError>>::new();
        for (index, operation) in operations.into_iter().enumerate() {
            let logical_key = match &operation {
                BatchOperation::Put(request) => &request.key,
                BatchOperation::Publish(request) => &request.key,
                BatchOperation::Clone(request) => &request.destination,
                BatchOperation::Delete(request) => &request.key,
            };
            let resolved = governance.clone().map_or_else(
                || {
                    let cache_key = (
                        logical_key.tenant().to_owned(),
                        logical_key.bucket().to_owned(),
                    );
                    governance_cache
                        .entry(cache_key)
                        .or_insert_with(|| {
                            let identity = self.resolve_bucket_identity(
                                logical_key.tenant(),
                                logical_key.bucket(),
                            )?;
                            Ok(ObjectMutationGovernance {
                                tenant_id: identity.tenant_id.0,
                                bucket_id: identity.bucket_id.0,
                                versioning: self.bucket_versioning_by_key(&identity.encode())?,
                                policy: self
                                    .bucket_policy_by_key(&identity.encode())?
                                    .unwrap_or_default(),
                            })
                        })
                        .clone()
                },
                Ok,
            );
            match resolved {
                Ok(resolved) => pending.push((index, operation, resolved, definition_intent)),
                Err(error) => {
                    completed.insert(index, Err(error));
                }
            }
        }

        while !pending.is_empty() {
            let lane_operations = pending
                .iter()
                .map(|(_, operation, governance, intent)| {
                    (operation.clone(), governance.clone(), *intent)
                })
                .collect();
            match self
                .commit_direct_local_mutation_batch(
                    lane_operations,
                    source_journal_admission,
                    governance_supplied,
                )
                .await
            {
                Ok((outcomes, _receipt_capacity_at)) => {
                    if outcomes.len() != pending.len() {
                        let error = MutationError::Storage(
                            "local lane batch outcome count is inconsistent".into(),
                        );
                        for (index, _, _, _) in pending.drain(..) {
                            completed.insert(index, Err(error.clone()));
                        }
                        break;
                    }
                    let mut retry = Vec::new();
                    let mut retry_capacity = None;
                    let mut contradictory_capacity = false;
                    for ((index, operation, governance, intent), outcome) in
                        pending.drain(..).zip(outcomes)
                    {
                        if backpressure
                            && outcome
                                .as_ref()
                                .is_err_and(|error| is_mutation_capacity(error))
                        {
                            let capacity = outcome
                                .as_ref()
                                .err()
                                .and_then(mutation_capacity_kind)
                                .expect("capacity outcome was matched");
                            if retry_capacity.is_some_and(|existing| existing != capacity) {
                                contradictory_capacity = true;
                            } else {
                                retry_capacity = Some(capacity);
                            }
                            retry.push((index, operation, governance, intent));
                        } else {
                            completed.insert(index, outcome);
                        }
                    }
                    if retry.is_empty() {
                        break;
                    }
                    if contradictory_capacity {
                        let error = MutationError::Storage(
                            "one local lane batch returned contradictory capacity authorities"
                                .into(),
                        );
                        for (index, _, _, _) in retry.drain(..) {
                            completed.insert(index, Err(error.clone()));
                        }
                        break;
                    }
                    pending = retry;
                    self.wait_for_capacity_with_metrics(
                        retry_capacity.expect("a capacity retry names its authority"),
                    )
                    .await;
                }
                Err(error) if backpressure && is_mutation_capacity(&error) => {
                    let capacity =
                        mutation_capacity_kind(&error).expect("capacity error was matched");
                    self.wait_for_capacity_with_metrics(capacity).await;
                }
                Err(error) => {
                    for (index, _, _, _) in pending.drain(..) {
                        completed.insert(index, Err(error.clone()));
                    }
                    break;
                }
            }
        }
        completed
            .into_iter()
            .map(|(index, result)| BatchOutcome { index, result })
            .collect()
    }

    async fn wait_for_capacity_with_metrics(&self, capacity: &'static str) {
        let wait = super::MutationBackpressureWait::start(capacity);
        self.wait_for_mutation_capacity().await;
        wait.complete();
    }
    /// Applies one exact-path mutation; routing and acknowledgement remain outside storage.
    pub async fn coordinate_object_mutation(
        &self,
        operation: BatchOperation,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        let _policy_guard = self.policy_gate.read().await;
        let logical_key = match &operation {
            BatchOperation::Put(request) => &request.key,
            BatchOperation::Publish(request) => &request.key,
            BatchOperation::Clone(request) => &request.destination,
            BatchOperation::Delete(request) => &request.key,
        };
        let identity = self.resolve_bucket_identity(logical_key.tenant(), logical_key.bucket())?;
        let governance = ObjectMutationGovernance {
            tenant_id: identity.tenant_id.0,
            bucket_id: identity.bucket_id.0,
            versioning: self.bucket_versioning_by_key(&identity.encode())?,
            policy: self
                .bucket_policy_by_key(&identity.encode())?
                .unwrap_or_default(),
        };
        self.coordinate_object_mutation_with_governance(operation, governance, context)
            .await
    }
    pub async fn coordinate_object_mutation_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if matches!(&operation, BatchOperation::Clone(_)) {
            return Err(MutationError::InvalidObjectMutation(
                "distributed clone requires an exact retained-version atomic precondition".into(),
            ));
        }
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        let mut outcomes = self
            .coordinate_distributed_mutation_batch_with_admission(
                vec![(operation, governance, None)],
                context,
                SourceJournalAdmission::Bounded,
            )
            .await?;
        outcomes.pop().ok_or_else(|| {
            MutationError::Storage(
                "distributed mutation batch omitted its singleton outcome".into(),
            )
        })?
    }
    pub async fn coordinate_definition_object_mutation_with_governance(
        &self,
        operation: BatchOperation,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
        intent: DefinitionMutationIntent,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if matches!(&operation, BatchOperation::Clone(_)) {
            return Err(MutationError::InvalidObjectMutation(
                "clone is not a definition mutation".into(),
            ));
        }
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        intent.validate().map_err(definition_mutation_error)?;
        let mut outcomes = self
            .coordinate_distributed_mutation_batch_with_admission(
                vec![(operation, governance, Some(intent))],
                context,
                SourceJournalAdmission::Bounded,
            )
            .await?;
        outcomes.pop().ok_or_else(|| {
            MutationError::Storage("definition mutation batch omitted its singleton outcome".into())
        })?
    }
    pub async fn coordinate_distributed_definition_publish_with_governance(
        &self,
        request: PublishRequest,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
        intent: DefinitionMutationIntent,
    ) -> Result<CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        intent.validate().map_err(definition_mutation_error)?;
        let mut outcomes = self
            .coordinate_distributed_mutation_batch_with_admission(
                vec![(BatchOperation::Publish(request), governance, Some(intent))],
                context,
                SourceJournalAdmission::Bounded,
            )
            .await?;
        outcomes.pop().ok_or_else(|| {
            MutationError::Storage("definition publish batch omitted its singleton outcome".into())
        })?
    }
    /// Applies a coordinator result; reference owners consume the source journal.
    pub async fn apply_object_mutation_replica(
        &self,
        mutation: &ObjectMutation,
    ) -> Result<ReplicaObjectMutationApplied, MutationError> {
        let mut applied = self
            .apply_object_mutation_replica_batch(std::slice::from_ref(mutation))
            .await?;
        applied.pop().ok_or_else(|| {
            MutationError::Storage("replica mutation batch omitted its singleton outcome".into())
        })
    }
    pub(crate) fn stage_local_changes(
        &self,
        batch: &mut WriteBatch,
        changes: &[PendingLocalChange],
        reference_effects: LocalReferenceEffects,
    ) -> Result<(), MutationError> {
        self.stage_local_changes_with_admission(
            batch,
            changes,
            reference_effects,
            SourceJournalAdmission::Bounded,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn stage_local_changes_from_status(
        &self,
        batch: &mut WriteBatch,
        changes: &[PendingLocalChange],
        reference_effects: LocalReferenceEffects,
        admission: SourceJournalAdmission,
        mut status: WatchJournalStatus,
        cursor: u64,
        stage_visibility_settlement: bool,
        stage_status: bool,
    ) -> Result<StagedLocalChanges, MutationError> {
        let journal = self.cf(CF_LOCAL_INVALIDATIONS)?;
        let metadata = self.cf(CF_METADATA)?;
        let retained_entries_before = status.retained_entries;
        let retained_bytes_before = status.retained_bytes;
        if admission == SourceJournalAdmission::Bounded
            && (status.retained_entries > self.watch_retention.max_entries
                || status.retained_bytes > self.watch_retention.max_bytes)
        {
            return Err(MutationError::SourceJournalCapacity);
        }
        let old_tail = status.tail;
        if cursor > old_tail {
            return Err(MutationError::Storage(format!(
                "local reference cursor {cursor} is ahead of source-journal tail {old_tail}"
            )));
        }
        let local_reference_cursor = match reference_effects {
            LocalReferenceEffects::AppliedInline => {
                if cursor != old_tail {
                    // Deferred publication can advance the tail before ordered
                    // reference delivery. Retry against a fresh tail.
                    tracing::debug!(
                        reference_cursor = cursor,
                        source_journal_tail = old_tail,
                        "inline reference effects are waiting for source-journal catch-up"
                    );
                    return Err(MutationError::SourceJournalCapacity);
                }
                Some(status.source_id)
            }
            LocalReferenceEffects::AppliedInlineLane => None,
            LocalReferenceEffects::NoReferenceEffects => {
                if changes
                    .iter()
                    .any(PendingLocalChange::has_reference_effects)
                {
                    return Err(MutationError::Storage(
                        "source-journal append declared no reference effects but carried a reference delta"
                            .into(),
                    ));
                }
                (cursor == old_tail).then_some(status.source_id)
            }
            LocalReferenceEffects::Deferred => None,
        };
        let mut appended = VecDeque::new();
        for pending in changes {
            status.tail = status.tail.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation offset is exhausted".into())
            })?;
            let change = pending.at_offset(status.tail);
            let encoded = encode_local_change(&change).map_err(storage_error)?;
            let logical_bytes = invalidation_record_bytes(encoded.len())
                .saturating_add(super::journal_routes::journal_route_logical_bytes(&change));
            if admission == SourceJournalAdmission::Bounded
                && logical_bytes > self.watch_retention.max_bytes
            {
                return Err(MutationError::SourceJournalRecordTooLarge {
                    bytes: logical_bytes,
                    maximum: self.watch_retention.max_bytes,
                });
            }
            self.stage_journal_routes_with_admission(
                batch,
                status.source_id.source_epoch,
                admission,
                &change,
            )?;
            status.retained_entries = status.retained_entries.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation entry count is exhausted".into())
            })?;
            status.retained_bytes = status
                .retained_bytes
                .checked_add(logical_bytes)
                .ok_or_else(|| {
                    MutationError::Storage("local invalidation byte count is exhausted".into())
                })?;
            appended.push_back((status.tail, encoded));
        }
        if admission == SourceJournalAdmission::Bounded {
            let appended_entries = status
                .retained_entries
                .saturating_sub(retained_entries_before);
            let appended_bytes = status.retained_bytes.saturating_sub(retained_bytes_before);
            if appended_entries > self.watch_retention.max_entries
                || appended_bytes > self.watch_retention.max_bytes
            {
                return Err(MutationError::SourceJournalTransitionTooLarge {
                    entries: appended_entries,
                    bytes: appended_bytes,
                    maximum_entries: self.watch_retention.max_entries,
                    maximum_bytes: self.watch_retention.max_bytes,
                });
            }
            if status.retained_entries > self.watch_retention.max_entries
                || status.retained_bytes > self.watch_retention.max_bytes
            {
                return Err(MutationError::SourceJournalCapacity);
            }
        }
        for (offset, encoded) in appended {
            batch.put_cf(journal, invalidation_key(offset), encoded);
        }
        let visibility_settlement_staged =
            stage_visibility_settlement && status.settled_through == old_tail;
        if visibility_settlement_staged {
            status.settled_through = status.tail;
        }
        if stage_status {
            batch.put_cf(
                metadata,
                LOCAL_INVALIDATION_STATUS_KEY,
                encode_watch_journal_status(status),
            );
        }
        if let Some(source) = local_reference_cursor {
            self.stage_reference_delta_cursor(batch, source, status.tail)?;
        }
        Ok(StagedLocalChanges {
            previous_tail: old_tail,
            status,
            visibility_settlement_staged,
        })
    }
    pub(crate) fn notify_local_invalidations(&self) {
        self.observe_source_journal_progress_debt();
        self.watch_notify.send_replace(());
    }

    pub(super) fn notify_local_invalidations_from_status(&self, status: WatchJournalStatus) {
        self.observe_source_journal_progress_debt_from_status(status);
        self.watch_notify.send_replace(());
    }

    pub(super) async fn prepare(
        &self,
        operation: BatchOperation,
        identity: BucketIdentity,
        distributed_coordination: bool,
    ) -> Result<PreparedOperation, MutationError> {
        match operation {
            BatchOperation::Put(mut request) => {
                validate_command_id(request.command_id.as_deref())?;
                if !distributed_coordination {
                    require_local_durability(request.durability)?;
                }
                let bytes = std::mem::take(&mut request.bytes);
                let payload = if distributed_coordination {
                    PreparedPayload::Sealed(self.stage_blob(&bytes).await?)
                } else if bytes.len() <= PAYLOAD_ARTIFACT_CHUNK_BYTES {
                    let reference = blob_reference_for_bytes(&bytes);
                    PreparedPayload::Inline { reference, bytes }
                } else {
                    PreparedPayload::Installed(self.stage_blob(&bytes).await?)
                };
                let fingerprint = put_fingerprint(
                    &identity.head_key(request.key.path()),
                    request.mode,
                    request.content_type.as_deref(),
                    request.durability,
                    payload.reference(),
                );
                Ok(PreparedOperation::Put {
                    request,
                    identity,
                    payload,
                    fingerprint,
                })
            }
            BatchOperation::Publish(request) => {
                validate_command_id(request.command_id.as_deref())?;
                if !distributed_coordination {
                    require_local_durability(request.durability)?;
                }
                if !self.contains_blob(&request.blob).await? {
                    return Err(MutationError::BlobNotFound);
                }
                let fingerprint = publish_fingerprint(&request, identity);
                Ok(PreparedOperation::Publish {
                    request,
                    identity,
                    fingerprint,
                })
            }
            BatchOperation::Clone(request) => {
                validate_clone_request(&request)?;
                if !distributed_coordination {
                    require_local_durability(request.durability)?;
                    if !self.contains_blob(&request.blob).await? {
                        return Err(MutationError::BlobNotFound);
                    }
                }
                let fingerprint = clone_fingerprint(&request, identity);
                Ok(PreparedOperation::Clone {
                    request,
                    identity,
                    fingerprint,
                })
            }
            BatchOperation::Delete(request) => {
                validate_command_id(request.command_id.as_deref())?;
                if !distributed_coordination {
                    require_local_durability(request.durability)?;
                }
                let fingerprint = delete_fingerprint(&request, identity);
                Ok(PreparedOperation::Delete {
                    request,
                    identity,
                    fingerprint,
                })
            }
        }
    }

    pub(super) fn mutation_receipt_status(&self) -> Result<MutationReceiptStatus, MutationError> {
        let metadata = self.cf(CF_METADATA)?;
        let encoded = self
            .db
            .get_cf(metadata, MUTATION_RECEIPT_STATUS_KEY)
            .map_err(storage_error)?
            .ok_or_else(|| MutationError::Storage("mutation receipt metadata is missing".into()))?;
        decode_mutation_receipt_status(&encoded)
    }

    pub(super) fn stage_expired_mutation_receipts(
        &self,
        batch: &mut WriteBatch,
        now_unix_millis: u64,
        status: &mut MutationReceiptStatus,
    ) -> Result<BTreeSet<Vec<u8>>, MutationError> {
        if status.entries == 0 {
            return Ok(BTreeSet::new());
        }
        let receipts = self.cf(CF_RECEIPTS)?;
        let mut pruned = BTreeSet::new();
        let mut pruned_bytes = 0_u64;
        let iterator = self.db.iterator_cf(
            receipts,
            IteratorMode::From(
                &[STORAGE_KEY_FORMAT_VERSION, RECEIPT_EXPIRY_PREFIX],
                Direction::Forward,
            ),
        );
        for entry in iterator {
            let (index_key, _) = entry.map_err(storage_error)?;
            let Some((expires_at, primary_key)) = parse_receipt_expiry_key(&index_key)? else {
                break;
            };
            if expires_at > now_unix_millis {
                break;
            }
            if pruned.contains(&primary_key) {
                return Err(MutationError::Storage(
                    "mutation receipt has duplicate expiry indexes".into(),
                ));
            }
            let encoded = self
                .db
                .get_cf(receipts, &primary_key)
                .map_err(storage_error)?
                .ok_or_else(|| {
                    MutationError::Storage(
                        "mutation receipt expiry index references a missing receipt".into(),
                    )
                })?;
            let receipt = decode_stored_receipt(&encoded)?;
            if receipt.expires_at_unix_millis != expires_at {
                return Err(MutationError::Storage(
                    "mutation receipt expiry index disagrees with its receipt".into(),
                ));
            }
            let logical_bytes =
                mutation_receipt_logical_bytes(primary_key.len(), encoded.len(), index_key.len());
            if pruned.len() >= MAX_EXPIRED_RECEIPTS_PRUNED_PER_PASS
                || (!pruned.is_empty()
                    && pruned_bytes.saturating_add(logical_bytes)
                        > MAX_EXPIRED_RECEIPT_BYTES_PRUNED_PER_PASS)
            {
                break;
            }
            status.entries = status.entries.checked_sub(1).ok_or_else(|| {
                MutationError::Storage("mutation receipt count is inconsistent".into())
            })?;
            status.bytes = status.bytes.checked_sub(logical_bytes).ok_or_else(|| {
                MutationError::Storage("mutation receipt byte accounting is inconsistent".into())
            })?;
            batch.delete_cf(receipts, &primary_key);
            batch.delete_cf(receipts, &index_key);
            pruned.insert(primary_key);
            pruned_bytes = pruned_bytes.saturating_add(logical_bytes);
        }
        Ok(pruned)
    }

    /// Persists receipt expiry separately so a rejected WriteBatch stays atomic.
    pub(super) async fn prune_expired_receipts_for_capacity(&self) -> Result<bool, MutationError> {
        let _commit_guard = self.lock_commit("receipt_pruning").await;
        let now = now_unix_millis()?;
        let mut status = self.mutation_receipt_status()?;
        let initial = status;
        let mut batch = WriteBatch::default();
        let pruned = self.stage_expired_mutation_receipts(&mut batch, now, &mut status)?;
        if pruned.is_empty() {
            return Ok(false);
        }
        if status != initial {
            self.stage_mutation_receipt_status(&mut batch, status)?;
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        self.mutation_capacity_notify.notify_waiters();
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn stage_mutation_receipt(
        &self,
        batch: &mut WriteBatch,
        primary_key: Option<Vec<u8>>,
        fingerprint: [u8; 32],
        version: VersionId,
        deleted: bool,
        object_mutation: Option<ObjectMutation>,
        definition_transition: Option<DefinitionTransition>,
        now_unix_millis: u64,
        status: &mut MutationReceiptStatus,
        pending_receipts: &mut BTreeMap<Vec<u8>, StoredReceipt>,
    ) -> Result<u64, MutationError> {
        let Some(primary_key) = primary_key else {
            return Ok(0);
        };
        let expires_at_unix_millis = now_unix_millis
            .checked_add(self.mutation_receipt_retention.retention_millis())
            .ok_or_else(|| MutationError::Storage("mutation receipt expiry overflow".into()))?;
        let stored = StoredReceipt {
            fingerprint,
            version,
            deleted,
            expires_at_unix_millis,
            object_mutation,
            definition_transition,
        };
        self.stage_stored_mutation_receipt(batch, primary_key, stored, status, pending_receipts)?;
        Ok(expires_at_unix_millis)
    }

    pub(super) fn stage_stored_mutation_receipt(
        &self,
        batch: &mut WriteBatch,
        primary_key: Vec<u8>,
        stored: StoredReceipt,
        status: &mut MutationReceiptStatus,
        pending_receipts: &mut BTreeMap<Vec<u8>, StoredReceipt>,
    ) -> Result<(), MutationError> {
        let encoded = encode_stored_receipt(&stored)?;
        let expiry_key = receipt_expiry_key(stored.expires_at_unix_millis, &primary_key)?;
        let logical_bytes =
            mutation_receipt_logical_bytes(primary_key.len(), encoded.len(), expiry_key.len());
        if logical_bytes > self.mutation_receipt_retention.max_bytes {
            return Err(MutationError::ReceiptTooLarge {
                bytes: logical_bytes,
                maximum: self.mutation_receipt_retention.max_bytes,
            });
        }
        let next_entries = status
            .entries
            .checked_add(1)
            .ok_or_else(|| MutationError::Storage("mutation receipt count is exhausted".into()))?;
        let next_bytes = status.bytes.checked_add(logical_bytes).ok_or_else(|| {
            MutationError::Storage("mutation receipt byte accounting is exhausted".into())
        })?;
        if next_entries > self.mutation_receipt_retention.max_entries
            || next_bytes > self.mutation_receipt_retention.max_bytes
        {
            return Err(MutationError::ReceiptCapacity);
        }
        batch.put_cf(self.cf(CF_RECEIPTS)?, &primary_key, encoded);
        batch.put_cf(self.cf(CF_RECEIPTS)?, expiry_key, []);
        pending_receipts.insert(primary_key, stored);
        status.entries = next_entries;
        status.bytes = next_bytes;
        Ok(())
    }

    pub(super) fn stage_mutation_receipt_status(
        &self,
        batch: &mut WriteBatch,
        status: MutationReceiptStatus,
    ) -> Result<(), MutationError> {
        let metadata = self.cf(CF_METADATA)?;
        batch.put_cf(
            metadata,
            MUTATION_RECEIPT_STATUS_KEY,
            encode_mutation_receipt_status(status),
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn evaluate_operation(
        &self,
        operation: &PreparedOperation,
        batch: &mut WriteBatch,
        pending_heads: &mut BTreeMap<Vec<u8>, Head>,
        pending_versions: &mut BTreeMap<Vec<u8>, Version>,
        pending_receipts: &mut BTreeMap<Vec<u8>, StoredReceipt>,
        pending_blob_references: &mut PendingBlobReferences,
        pending_inline_payloads: &mut BTreeSet<Vec<u8>>,
        read_cache: &MutationReadCache,
        policy_cache: &mut BTreeMap<Vec<u8>, Result<BucketPolicy, MutationError>>,
        versioning_cache: &mut BTreeMap<Vec<u8>, Result<ObjectVersioning, MutationError>>,
        pruned_receipts: &BTreeSet<Vec<u8>>,
        receipt_status: &mut MutationReceiptStatus,
        now_unix_millis: u64,
        distributed: Option<DistributedEvaluationContext>,
        definition_intent: Option<DefinitionMutationIntent>,
        evaluation_subphases: &mut EvaluationSubphaseMetrics,
    ) -> Result<EvaluatedOperation, MutationError> {
        let mut timing = evaluation_subphases.start();
        let key = operation.key();
        let encoded_key = operation.encoded_head_key();
        let retain_command_receipt =
            distributed.is_none_or(|context| context.retain_command_receipt);
        let receipt_key = retain_command_receipt
            .then(|| operation.command_id())
            .flatten()
            .map(|command_id| receipt_key(operation.identity(), command_id));
        if let Some(receipt_key) = receipt_key.as_ref() {
            let existing = match pending_receipts.get(receipt_key) {
                Some(receipt) => Some(receipt.clone()),
                None if pruned_receipts.contains(receipt_key) => None,
                None => match read_cache.receipt(receipt_key) {
                    Some(cached) => cached?,
                    None => self.read_stored_receipt(receipt_key)?,
                },
            };
            if let Some(existing) = existing {
                if existing.expires_at_unix_millis <= now_unix_millis {
                    // Lane writers never prune from an optimistic authority
                    // snapshot: two independent lanes could otherwise debit
                    // the same expired row. Surface bounded capacity so the
                    // existing exclusive maintenance pass prunes once, then
                    // the idempotent caller retries against fresh authority.
                    return Err(MutationError::ReceiptCapacity);
                }
                if existing.fingerprint != operation.fingerprint() {
                    return Err(MutationError::IdempotencyConflict);
                }
                if !definition_receipt_matches_intent(
                    existing.definition_transition.as_ref(),
                    definition_intent,
                    operation,
                ) {
                    return Err(MutationError::IdempotencyConflict);
                }
                let alias_snapshot = existing
                    .object_mutation
                    .as_ref()
                    .and_then(|mutation| mutation.alias_snapshot.clone());
                evaluation_subphases.record_since(EvaluationSubphase::CurrentGovernance, timing);
                return Ok(EvaluatedOperation {
                    receipt: MutationReceipt {
                        command_id: operation.command_id().map(str::to_owned),
                        fingerprint: existing.fingerprint,
                        version: existing.version,
                        deleted: existing.deleted,
                        replayed: true,
                        replay_guarantee_expires_at_unix_millis: existing.expires_at_unix_millis,
                    },
                    mutation: existing.object_mutation,
                    reference_deltas: Vec::new(),
                    accounting_transition: None,
                    definition_transition: existing.definition_transition,
                    alias_snapshot,
                });
            }
        }

        if let PreparedOperation::Clone { request, .. } = operation {
            let source = self.user_retained_version(
                operation.identity(),
                &request.source,
                request.source_version,
            )?;
            if !source.as_ref().is_some_and(|source| {
                !source.deleted
                    && source.id == request.source_version
                    && source.blob.as_ref() == Some(&request.blob)
                    && source.content_type == request.content_type
            }) {
                return Err(MutationError::InvalidObjectMutation(
                    "clone source exact version is no longer live or no longer matches its content identity"
                        .into(),
                ));
            }
        }

        let current = match pending_heads.get(&encoded_key) {
            Some(head) => Some(head.clone()),
            None => match read_cache.head(&encoded_key) {
                Some(cached) => cached?,
                None => self.head_by_storage_key(&encoded_key)?,
            },
        };
        let current_stored_version = match current.as_ref() {
            Some(head) if !pending_versions.contains_key(&encoded_key) => Some(
                match read_cache.stored_version(&encoded_key) {
                    Some(cached) => cached?,
                    None => self.stored_version_by_key(&version_key(
                        operation.identity(),
                        key,
                        head.version,
                    ))?,
                }
                .ok_or_else(|| {
                    MutationError::Storage("head references a missing version".into())
                })?,
            ),
            Some(_) | None => None,
        };
        let current_version = match current.as_ref() {
            Some(_) => Some(
                pending_versions
                    .get(&encoded_key)
                    .cloned()
                    .or_else(|| {
                        current_stored_version
                            .as_ref()
                            .map(|stored| stored.version.clone())
                    })
                    .ok_or_else(|| {
                        MutationError::Storage("head references a missing version".into())
                    })?,
            ),
            None => None,
        };
        if current_version
            .as_ref()
            .zip(current.as_ref())
            .is_some_and(|(version, head)| {
                version.id != head.version || version.deleted != head.deleted
            })
        {
            return Err(MutationError::Storage(
                "head and current version descriptor disagree".into(),
            ));
        }
        if current_version
            .as_ref()
            .is_some_and(|version| version.protected_link_descriptor)
        {
            return Err(MutationError::InvalidObjectMutation(
                "protected alias descriptors must be mutated through sealed link authority".into(),
            ));
        }
        let alias_registry = match read_cache.alias_registry(&encoded_key, key.path()) {
            Some(cached) => cached?,
            None => self.alias_registry_locked(operation.identity(), key.path())?,
        };
        let alias_snapshot = match alias_registry {
            Some(registry) => {
                if matches!(operation, PreparedOperation::Delete { .. }) {
                    return Err(MutationError::ObjectHasInboundAliases);
                }
                let canonical_version = current_version
                    .clone()
                    .filter(|version| !version.deleted)
                    .ok_or_else(|| {
                        MutationError::Storage(
                            "alias registry exists without a live canonical target".into(),
                        )
                    })?;
                Some(crate::ObjectAliasSnapshot {
                    registry,
                    canonical_version,
                })
            }
            None => None,
        };
        let encoded_bucket = operation.identity().encode().to_vec();
        let policy = policy_cache
            .entry(encoded_bucket.clone())
            .or_insert_with(|| {
                self.bucket_policy_by_key(&encoded_bucket)
                    .map(Option::unwrap_or_default)
            })
            .as_ref()
            .map_err(Clone::clone)?;
        let versioning = *versioning_cache
            .entry(encoded_bucket)
            .or_insert_with(|| self.bucket_versioning_by_key(&operation.identity().encode()))
            .as_ref()
            .map_err(Clone::clone)?;
        let program_definition = is_program_definition_path(key.path());
        if policy.is_program_only(key.path()) && !program_definition {
            return Err(MutationError::ProgramConcurrencyViolation);
        }
        let immutable_path = policy.is_immutable(key.path()) || program_definition;
        match operation.put_mode() {
            Some(PutMode::PutImmutable) if !immutable_path => {
                return Err(MutationError::ImmutablePolicyRequired);
            }
            Some(PutMode::PutImmutable) => {}
            Some(_) | None if immutable_path => {
                return Err(MutationError::Immutable);
            }
            Some(_) | None => {}
        }
        if matches!(operation.put_mode(), Some(PutMode::PutImmutable))
            && let Some(current) = current.as_ref()
        {
            let existing = current_version.as_ref().ok_or_else(|| {
                MutationError::Storage("head references a missing version".into())
            })?;
            let requested_payload = match operation {
                PreparedOperation::Put { payload, .. } => payload.reference().clone(),
                PreparedOperation::Publish { request, .. } => request.blob.clone(),
                PreparedOperation::Clone { request, .. } => request.blob.clone(),
                PreparedOperation::Delete { .. } => unreachable!(),
            };
            let requested_content_type = match operation {
                PreparedOperation::Put { request, .. } => request.content_type.as_ref(),
                PreparedOperation::Publish { request, .. } => request.content_type.as_ref(),
                PreparedOperation::Clone { request, .. } => request.content_type.as_ref(),
                PreparedOperation::Delete { .. } => unreachable!(),
            };
            if !current.deleted
                && version_blob_reference(existing)?.as_ref() == Some(&requested_payload)
                && existing.content_type.as_ref() == requested_content_type
            {
                let fingerprint = operation.fingerprint();
                let definition_transition = definition_intent.map(|intent| DefinitionTransition {
                    kind: intent.kind,
                    tenant_id: operation.identity().tenant_id.0,
                    bucket_id: operation.identity().bucket_id.0,
                    definition_id: intent.definition_id,
                    path: key.path().to_owned(),
                    object_version: current.version,
                    operation: DefinitionOperation::Upsert,
                });
                let expires_at = self.stage_mutation_receipt(
                    batch,
                    receipt_key,
                    fingerprint,
                    current.version,
                    false,
                    None,
                    definition_transition.clone(),
                    now_unix_millis,
                    receipt_status,
                    pending_receipts,
                )?;
                if let Some(transition) = definition_transition.as_ref() {
                    self.stage_definition_transition(batch, transition)
                        .map_err(definition_mutation_error)?;
                }
                evaluation_subphases.record_since(EvaluationSubphase::CurrentGovernance, timing);
                return Ok(EvaluatedOperation {
                    receipt: MutationReceipt {
                        command_id: operation.command_id().map(str::to_owned),
                        fingerprint,
                        version: current.version,
                        deleted: false,
                        replayed: true,
                        replay_guarantee_expires_at_unix_millis: expires_at,
                    },
                    mutation: None,
                    reference_deltas: Vec::new(),
                    accounting_transition: None,
                    definition_transition,
                    alias_snapshot: None,
                });
            }
            return Err(MutationError::Immutable);
        }
        if !retain_command_receipt
            && let (Some(current), Some(existing)) = (current.as_ref(), current_version.as_ref())
            && let Some(replay) =
                trusted_derived_put_if_absent_replay(operation, current, existing)?
        {
            evaluation_subphases.record_since(EvaluationSubphase::CurrentGovernance, timing);
            return Ok(replay);
        }
        check_precondition(operation.precondition(), current.as_ref())?;

        evaluation_subphases.record_since(EvaluationSubphase::CurrentGovernance, timing);
        timing = evaluation_subphases.start();
        let id = self.clock.next().map_err(storage_error)?;
        let deleted = matches!(operation, PreparedOperation::Delete { .. });
        let new_blob = match operation {
            PreparedOperation::Put { payload, .. } => Some(payload.reference().clone()),
            PreparedOperation::Publish { request, .. } => Some(request.blob.clone()),
            PreparedOperation::Clone { request, .. } => Some(request.blob.clone()),
            PreparedOperation::Delete { .. } => None,
        };
        if let PreparedOperation::Put { payload, .. } = operation
            && payload.inline_bytes().is_none()
            && !self.contains_blob(payload.reference()).await?
        {
            return Err(MutationError::BlobNotFound);
        }
        let version = Version {
            id,
            blob: new_blob.clone(),
            content_type: match operation {
                PreparedOperation::Put { request, .. } => request.content_type.clone(),
                PreparedOperation::Publish { request, .. } => request.content_type.clone(),
                PreparedOperation::Clone { request, .. } => request.content_type.clone(),
                PreparedOperation::Delete { .. } => None,
            },
            deleted,
            committed_at_unix_millis: now_unix_millis,
            protected_link_descriptor: false,
        };
        let retention = version_retention(versioning);
        let accounting_transition =
            head_accounting_transition(current_version.as_ref(), &version, retention);
        let definition_transition = definition_intent.map(|intent| DefinitionTransition {
            kind: intent.kind,
            tenant_id: operation.identity().tenant_id.0,
            bucket_id: operation.identity().bucket_id.0,
            definition_id: intent.definition_id,
            path: key.path().to_owned(),
            object_version: id,
            operation: if deleted {
                DefinitionOperation::Delete
            } else {
                DefinitionOperation::Upsert
            },
        });
        if let Some(transition) = definition_transition.as_ref() {
            transition.validate().map_err(definition_mutation_error)?;
        }
        let fingerprint = operation.fingerprint();
        let apply_content_lifecycle = distributed.is_none_or(|distributed| {
            matches!(
                distributed.reference_effects,
                LocalReferenceEffects::AppliedInline | LocalReferenceEffects::AppliedInlineLane
            )
        });
        let released_predecessor = (versioning == ObjectVersioning::Unversioned)
            .then_some(current_stored_version.as_ref())
            .flatten()
            .filter(|stored| stored.retention == StoredVersionRetention::JournalReleased);
        let mut reference_deltas = Vec::with_capacity(2);
        if let Some(reference) = new_blob.as_ref() {
            reference_deltas.push(ReferenceDelta {
                blob: reference.clone(),
                change: 1,
            });
        }
        if let Some(reference) = released_predecessor
            .as_ref()
            .and_then(|stored| stored.version.blob.clone())
        {
            reference_deltas.push(ReferenceDelta {
                blob: reference,
                change: -1,
            });
        }
        if reference_deltas.len() == 2 && reference_deltas[0].blob == reference_deltas[1].blob {
            reference_deltas.clear();
        }
        let released_same_as_new = released_predecessor
            .as_ref()
            .and_then(|stored| stored.version.blob.as_ref())
            .zip(new_blob.as_ref())
            .is_some_and(|(old, new)| old == new);
        let receipt_expires_at_unix_millis = if distributed.is_some() || receipt_key.is_some() {
            now_unix_millis
                .checked_add(self.mutation_receipt_retention.retention_millis())
                .ok_or_else(|| MutationError::Storage("mutation receipt expiry overflow".into()))?
        } else {
            0
        };
        evaluation_subphases.record_since(EvaluationSubphase::Planning, timing);
        evaluation_subphases.count_mutation_construction_validation();
        let object_mutation = evaluation_subphases.measure(
            EvaluationSubphase::MutationConstructionValidation,
            || {
                distributed
                    .map(|distributed| {
                        let command_id = operation
                            .command_id()
                            .ok_or(MutationError::InvalidCommandId)?;
                        let mut mutation = ObjectMutation {
                            format: OBJECT_MUTATION_FORMAT,
                            tenant_id: operation.identity().tenant_id.0,
                            bucket_id: operation.identity().bucket_id.0,
                            versioning,
                            exact_path: key.path().to_owned(),
                            command_id: command_id.to_owned(),
                            input_fingerprint: fingerprint,
                            version: version.clone(),
                            receipt_expires_at_unix_millis,
                            stamp: MutationStamp {
                                format: MUTATION_STAMP_FORMAT,
                                predecessor_version: current.as_ref().map(|head| head.version),
                                program_commit_cursor: None,
                                mutation_fingerprint: [0; 32],
                                active_placement_log_id: distributed
                                    .mutation
                                    .active_placement_log_id,
                                serving_fence_term: distributed.mutation.serving_fence_term,
                                source_id: distributed.source_id,
                                source_journal_position: distributed.source_journal_position,
                            },
                            reference_deltas: reference_deltas.clone(),
                            accounting_transition: Some(accounting_transition),
                            definition_transition: definition_transition.clone(),
                            alias_snapshot: alias_snapshot.clone(),
                        };
                        mutation.set_computed_fingerprint();
                        mutation.validate()?;
                        Ok(mutation)
                    })
                    .transpose()
            },
        )?;
        timing = evaluation_subphases.start();
        let head = Head {
            version: id,
            deleted,
            mutation_stamp: object_mutation.as_ref().map(|mutation| mutation.stamp),
        };
        let encoded_version = StoredVersion::new(version.clone(), retention).encode()?;
        let encoded_head = encode_head(&head)?;
        let versions = self.cf(CF_VERSIONS)?;
        let heads = self.cf(CF_HEADS)?;
        let encoded_version_key = version_key(operation.identity(), key, id);
        let mut blob_reference_updates = Vec::with_capacity(2);
        let materialize_inline =
            distributed.is_some_and(|distributed| distributed.materialize_inline_payload);
        evaluation_subphases.record_since(EvaluationSubphase::DurableEncoding, timing);
        evaluation_subphases.count_inline_payload_receipt_stage();
        let (inline_payload_key, reservation) =
            evaluation_subphases.measure(EvaluationSubphase::InlinePayloadReceiptStage, || {
                self.prepare_coordinated_inline_payload(
                    operation,
                    apply_content_lifecycle || materialize_inline,
                    materialize_inline && !apply_content_lifecycle,
                    pending_inline_payloads,
                    pending_blob_references,
                    read_cache,
                    now_unix_millis,
                )
            })?;
        timing = evaluation_subphases.start();
        blob_reference_updates.extend(reservation);
        if apply_content_lifecycle
            && !released_same_as_new
            && let Some(reference) = new_blob.as_ref()
        {
            let update = match operation {
                PreparedOperation::Put { .. } => self.prepare_materialized_blob_publication(
                    reference,
                    pending_blob_references,
                    read_cache.blob_reference(reference),
                    now_unix_millis,
                )?,
                PreparedOperation::Publish { .. } => self
                    .prepare_blob_reference_publication_cached(
                        reference,
                        pending_blob_references,
                        read_cache.blob_reference(reference),
                        now_unix_millis,
                    )?,
                PreparedOperation::Clone { .. } => self.prepare_blob_reference_publication_cached(
                    reference,
                    pending_blob_references,
                    read_cache.blob_reference(reference),
                    now_unix_millis,
                )?,
                PreparedOperation::Delete { .. } => unreachable!(),
            };
            blob_reference_updates.push(update);
        }
        evaluation_subphases.record_since(EvaluationSubphase::BlobLifecycle, timing);
        let expires_at =
            evaluation_subphases.measure(EvaluationSubphase::InlinePayloadReceiptStage, || {
                self.stage_mutation_receipt(
                    batch,
                    receipt_key,
                    fingerprint,
                    id,
                    deleted,
                    object_mutation.clone(),
                    definition_transition.clone(),
                    now_unix_millis,
                    receipt_status,
                    pending_receipts,
                )
            })?;
        if let Some(key) = inline_payload_key {
            let (reference, bytes) = match operation {
                PreparedOperation::Put { payload, .. } => (
                    payload.reference(),
                    payload
                        .inline_bytes()
                        .expect("only an inline put materializes inline payload bytes"),
                ),
                _ => unreachable!("only a put materializes inline payload bytes"),
            };
            evaluation_subphases.measure(EvaluationSubphase::InlinePayloadReceiptStage, || {
                self.stage_inline_complete_artifact(batch, reference, bytes)
            })?;
            timing = evaluation_subphases.start();
            pending_inline_payloads.insert(key);
            evaluation_subphases.record_since(EvaluationSubphase::BlobLifecycle, timing);
        }
        timing = evaluation_subphases.start();
        for (key, state) in blob_reference_updates {
            let prefetched = read_cache.blob_reference_by_key(&key);
            self.stage_blob_reference_update_cached(
                batch,
                pending_blob_references,
                key,
                state,
                prefetched,
            )?;
        }
        if apply_content_lifecycle
            && !released_same_as_new
            && let Some(reference) = released_predecessor
                .as_ref()
                .and_then(|stored| stored.version.blob.as_ref())
        {
            let (key, state) = self.prepare_blob_reference_retirement_cached(
                reference,
                pending_blob_references,
                read_cache.blob_reference(reference),
                now_unix_millis,
            )?;
            self.stage_blob_reference_update(batch, pending_blob_references, key, state)?;
        }
        if let Some(previous) = current_version.as_ref() {
            let previous_key = version_key(operation.identity(), key, previous.id);
            if let Some(stored) = current_stored_version.as_ref() {
                match stored.retention {
                    StoredVersionRetention::JournalPending
                        if versioning == ObjectVersioning::Enabled =>
                    {
                        batch.put_cf(
                            versions,
                            previous_key,
                            StoredVersion::new(
                                stored.version.clone(),
                                StoredVersionRetention::UserRetained,
                            )
                            .encode()?,
                        );
                    }
                    StoredVersionRetention::JournalReleased
                        if versioning == ObjectVersioning::Enabled =>
                    {
                        batch.put_cf(
                            versions,
                            previous_key,
                            StoredVersion::new(
                                stored.version.clone(),
                                StoredVersionRetention::UserRetained,
                            )
                            .encode()?,
                        );
                    }
                    StoredVersionRetention::JournalReleased => {
                        batch.delete_cf(versions, previous_key);
                    }
                    StoredVersionRetention::JournalPending
                    | StoredVersionRetention::UserRetained => {}
                }
            } else if pending_versions
                .get(&encoded_key)
                .is_none_or(|pending| pending.id != previous.id)
            {
                return Err(MutationError::Storage(
                    "current predecessor descriptor is missing".into(),
                ));
            }
        }
        evaluation_subphases.record_since(EvaluationSubphase::BlobLifecycle, timing);
        timing = evaluation_subphases.start();
        batch.put_cf(versions, encoded_version_key, encoded_version);
        batch.put_cf(heads, &encoded_key, encoded_head);
        if let Some(transition) = definition_transition.as_ref() {
            self.stage_definition_transition(batch, transition)
                .map_err(definition_mutation_error)?;
        }
        pending_heads.insert(encoded_key.clone(), head);
        pending_versions.insert(encoded_key, version);
        let evaluated = EvaluatedOperation {
            receipt: MutationReceipt {
                command_id: operation.command_id().map(str::to_owned),
                fingerprint,
                version: id,
                deleted,
                replayed: false,
                replay_guarantee_expires_at_unix_millis: expires_at,
            },
            mutation: object_mutation,
            reference_deltas,
            accounting_transition: Some(accounting_transition),
            definition_transition,
            alias_snapshot,
        };
        evaluation_subphases.record_since(EvaluationSubphase::ObjectState, timing);
        Ok(evaluated)
    }
}
