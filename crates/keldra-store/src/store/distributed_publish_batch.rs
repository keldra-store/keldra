//! One physical coordinator batch for independently receipted distributed publishes.

use super::journal_capacity::SourceJournalAdmission;
use super::mutation_prefetch::MutationReadCache;
use super::mutation_types::DistributedEvaluationContext;
use super::*;
use crate::model::{CoordinatedObjectMutation, ObjectMutationContext, ObjectMutationGovernance};
use crate::{BatchOperation, DefinitionMutationIntent};

struct PreparedDistributedMutation {
    index: usize,
    operation: PreparedOperation,
    definition_intent: Option<DefinitionMutationIntent>,
}

impl Store {
    /// Evaluate independently receipted operations for one metadata replica
    /// group in request order and commit their successful coordinator state
    /// with one physical RocksDB batch.
    pub async fn coordinate_distributed_mutation_batch(
        &self,
        operations: Vec<(
            BatchOperation,
            ObjectMutationGovernance,
            Option<DefinitionMutationIntent>,
        )>,
        context: ObjectMutationContext,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        let total = operations.len();
        let mut prepared = Vec::with_capacity(total);
        let mut early = BTreeMap::new();
        let mut bucket_governance = BTreeMap::<Vec<u8>, ObjectMutationGovernance>::new();
        for (index, (operation, governance, definition_intent)) in
            operations.into_iter().enumerate()
        {
            let key = match &operation {
                BatchOperation::Put(request) => &request.key,
                BatchOperation::Publish(request) => &request.key,
                BatchOperation::Clone(request) => &request.destination,
                BatchOperation::Delete(request) => &request.key,
            };
            let identity = BucketIdentity {
                tenant_id: TenantId(governance.tenant_id),
                bucket_id: BucketId(governance.bucket_id),
            };
            let validation = governance.validate().and_then(|()| {
                definition_intent
                    .map(DefinitionMutationIntent::validate)
                    .transpose()
                    .map_err(|error| MutationError::InvalidObjectMutation(error.to_string()))?;
                if let Some(existing) = bucket_governance.get(&identity.encode().to_vec())
                    && existing != &governance
                {
                    return Err(MutationError::InvalidPolicy(
                        "one distributed batch supplied contradictory bucket governance".into(),
                    ));
                }
                if key.tenant().is_empty() || key.bucket().is_empty() {
                    return Err(MutationError::InvalidObjectMutation(
                        "distributed batch object identity is empty".into(),
                    ));
                }
                Ok(())
            });
            if let Err(error) = validation {
                early.insert(index, error);
                continue;
            }
            bucket_governance.insert(identity.encode().to_vec(), governance.clone());
            match self.prepare(operation, identity, true).await {
                Ok(operation) => prepared.push(PreparedDistributedMutation {
                    index,
                    operation,
                    definition_intent,
                }),
                Err(error) => {
                    early.insert(index, error);
                }
            }
        }

        let _policy_guard = self.policy_gate.read().await;
        let _path_guards = self
            .ordinary_locks
            .acquire(
                &prepared
                    .iter()
                    .map(|item| object_path(item.operation.key()))
                    .collect::<Vec<_>>(),
            )
            .await;
        let _commit_guard = self.lock_commit("distributed_publish").await;
        let mut reserved = BTreeMap::new();
        for item in &prepared {
            if let Err(error) = self.require_unreserved_object_locked(
                item.operation.identity(),
                item.operation.key().path(),
                None,
            ) {
                reserved.insert(item.index, error);
            }
        }
        if !reserved.is_empty() {
            prepared.retain(|item| !reserved.contains_key(&item.index));
            early.extend(reserved);
        }
        let source = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let mut next_source_position = source.tail.checked_add(1).ok_or_else(|| {
            MutationError::Storage("local invalidation offset is exhausted".into())
        })?;
        let now = now_unix_millis()?;
        let mut batch = WriteBatch::default();
        let mut receipt_status = self.mutation_receipt_status()?;
        let initial_receipt_status = receipt_status;
        let pruned_receipts =
            self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status)?;
        let read_cache = MutationReadCache::load(
            self,
            &prepared
                .iter()
                .map(|item| &item.operation)
                .collect::<Vec<_>>(),
        )?;
        let mut pending_heads = BTreeMap::new();
        let mut pending_versions = BTreeMap::new();
        let mut pending_receipts = BTreeMap::new();
        let mut pending_blob_references = PendingBlobReferences::new();
        let mut pending_small_blobs = BTreeSet::new();
        let mut policy_cache = bucket_governance
            .iter()
            .map(|(identity, governance)| (identity.clone(), Ok(governance.policy.clone())))
            .collect();
        let mut versioning_cache = bucket_governance
            .iter()
            .map(|(identity, governance)| (identity.clone(), Ok(governance.versioning)))
            .collect();
        let mut pending_changes = Vec::new();
        let mut high_watermark = None;
        let mut evaluated = BTreeMap::new();
        let mut receipt_capacity_exhausted = false;

        for item in &prepared {
            let outcome = self
                .evaluate_operation(
                    &item.operation,
                    &mut batch,
                    &mut pending_heads,
                    &mut pending_versions,
                    &mut pending_receipts,
                    &mut pending_blob_references,
                    &mut pending_small_blobs,
                    &read_cache,
                    &mut policy_cache,
                    &mut versioning_cache,
                    &pruned_receipts,
                    &mut receipt_status,
                    now,
                    Some(DistributedEvaluationContext {
                        mutation: context,
                        source_id: source.source_id,
                        source_journal_position: next_source_position,
                    }),
                    item.definition_intent,
                )
                .await;
            if outcome
                .as_ref()
                .is_err_and(|error| matches!(error, MutationError::ReceiptCapacity))
            {
                receipt_capacity_exhausted = true;
                break;
            }
            if let Ok(value) = &outcome {
                if !value.receipt.replayed {
                    let mutation = value.mutation.as_ref().ok_or_else(|| {
                        MutationError::Storage(
                            "distributed batch mutation result is missing".into(),
                        )
                    })?;
                    if mutation.stamp.source_journal_position != next_source_position {
                        return Err(MutationError::Storage(
                            "distributed batch source position changed during evaluation".into(),
                        ));
                    }
                    next_source_position = next_source_position
                        .checked_add(
                            1 + mutation
                                .alias_snapshot
                                .as_ref()
                                .map_or(0, |snapshot| snapshot.registry.aliases.len() as u64),
                        )
                        .ok_or_else(|| {
                            MutationError::Storage("local invalidation offset is exhausted".into())
                        })?;
                    high_watermark = Some(
                        high_watermark.map_or(value.receipt.version, |current: VersionId| {
                            current.max(value.receipt.version)
                        }),
                    );
                    pending_changes.extend(value.pending_head_changes(
                        item.operation.identity(),
                        item.operation.key().path(),
                    ));
                }
                if let Some(mutation) = value.mutation.as_ref() {
                    self.stage_object_mutation_reference_proof(&mut batch, mutation)?;
                }
            }
            evaluated.insert(
                item.index,
                outcome.map(|value| CoordinatedObjectMutation {
                    receipt: value.receipt,
                    mutation: value.mutation,
                }),
            );
        }

        if receipt_status != initial_receipt_status {
            self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
        }
        self.stage_local_changes(
            &mut batch,
            &pending_changes,
            LocalReferenceEffects::Deferred,
        )?;
        if let Some(high_watermark) = high_watermark {
            batch.put_cf(
                self.cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&high_watermark).map_err(storage_error)?,
            );
        }
        if !batch.is_empty() {
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
        }
        if !pruned_receipts.is_empty() {
            self.mutation_capacity_notify.notify_waiters();
        }
        if !pending_changes.is_empty() {
            self.notify_local_invalidations();
        }
        if receipt_capacity_exhausted {
            return Err(MutationError::ReceiptCapacity);
        }

        let mut outcomes = Vec::with_capacity(total);
        for index in 0..total {
            outcomes.push(match evaluated.remove(&index) {
                Some(outcome) => outcome,
                None => Err(early.remove(&index).ok_or_else(|| {
                    MutationError::Storage("distributed batch outcome index is inconsistent".into())
                })?),
            });
        }
        Ok(outcomes)
    }

    /// Evaluate independent, already payload-verified publishes in request order
    /// and commit every successful metadata mutation with one RocksDB batch.
    ///
    /// The cluster layer remains responsible for exact replica-group routing,
    /// payload evidence, replica acknowledgement and per-item durability. A
    /// A capacity error commits the successful request prefix and bounded
    /// expiry pruning before it is returned. The caller's ordinary idempotent
    /// retry then replays that prefix and continues without rebuilding bytes.
    pub async fn coordinate_distributed_publish_batch_with_governance(
        &self,
        requests: Vec<PublishRequest>,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        self.coordinate_distributed_publish_batch_with_admission(
            requests,
            governance,
            context,
            SourceJournalAdmission::Bounded,
        )
        .await
    }

    /// Trusted grouped derived publication. The cluster layer validates that
    /// every request is an immutable artifact needed to publish progress.
    #[doc(hidden)]
    pub async fn coordinate_derived_progress_publish_batch_with_governance(
        &self,
        requests: Vec<PublishRequest>,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        self.coordinate_distributed_publish_batch_with_admission(
            requests,
            governance,
            context,
            SourceJournalAdmission::DerivedProgress,
        )
        .await
    }

    async fn coordinate_distributed_publish_batch_with_admission(
        &self,
        requests: Vec<PublishRequest>,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
        source_journal_admission: SourceJournalAdmission,
    ) -> Result<Vec<Result<CoordinatedObjectMutation, MutationError>>, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let identity = BucketIdentity {
            tenant_id: TenantId(governance.tenant_id),
            bucket_id: BucketId(governance.bucket_id),
        };
        let mut prepared = Vec::with_capacity(requests.len());
        let mut early = BTreeMap::new();
        for (index, request) in requests.into_iter().enumerate() {
            match self.prepare_verified_distributed_publish(request, identity) {
                Ok(operation) => prepared.push((index, operation)),
                Err(error) => {
                    early.insert(index, error);
                }
            }
        }

        let _policy_guard = self.policy_gate.read().await;
        let _path_guards = self
            .ordinary_locks
            .acquire(
                &prepared
                    .iter()
                    .map(|(_, operation)| object_path(operation.key()))
                    .collect::<Vec<_>>(),
            )
            .await;
        let _commit_guard = self.lock_commit("distributed_publish").await;
        let source = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let mut next_source_position = source.tail.checked_add(1).ok_or_else(|| {
            MutationError::Storage("local invalidation offset is exhausted".into())
        })?;
        let now = now_unix_millis()?;
        let mut batch = WriteBatch::default();
        let mut receipt_status = self.mutation_receipt_status()?;
        let initial_receipt_status = receipt_status;
        let pruned_receipts =
            self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status)?;
        let read_cache = MutationReadCache::load(
            self,
            &prepared
                .iter()
                .map(|(_, operation)| operation)
                .collect::<Vec<_>>(),
        )?;
        let mut pending_heads = BTreeMap::new();
        let mut pending_versions = BTreeMap::new();
        let mut pending_receipts = BTreeMap::new();
        let mut pending_blob_references = PendingBlobReferences::new();
        let mut pending_small_blobs = BTreeSet::new();
        let encoded_bucket = identity.encode().to_vec();
        let mut policy_cache =
            BTreeMap::from([(encoded_bucket.clone(), Ok(governance.policy.clone()))]);
        let mut versioning_cache = BTreeMap::from([(encoded_bucket, Ok(governance.versioning))]);
        let mut pending_changes = Vec::new();
        let mut high_watermark = None;
        let mut evaluated = BTreeMap::new();
        let mut receipt_capacity_exhausted = false;

        for (index, operation) in &prepared {
            let outcome = self
                .evaluate_operation(
                    operation,
                    &mut batch,
                    &mut pending_heads,
                    &mut pending_versions,
                    &mut pending_receipts,
                    &mut pending_blob_references,
                    &mut pending_small_blobs,
                    &read_cache,
                    &mut policy_cache,
                    &mut versioning_cache,
                    &pruned_receipts,
                    &mut receipt_status,
                    now,
                    Some(DistributedEvaluationContext {
                        mutation: context,
                        source_id: source.source_id,
                        source_journal_position: next_source_position,
                    }),
                    None,
                )
                .await;
            if outcome
                .as_ref()
                .is_err_and(|error| matches!(error, MutationError::ReceiptCapacity))
            {
                // Receipt creation is the first physical staging step for a
                // new mutation. Capacity therefore leaves none of this
                // failing item in `batch`; stop before evaluating any suffix.
                receipt_capacity_exhausted = true;
                break;
            }
            if let Ok(value) = &outcome {
                if !value.receipt.replayed {
                    let mutation = value.mutation.as_ref().ok_or_else(|| {
                        MutationError::Storage(
                            "distributed batch mutation result is missing".into(),
                        )
                    })?;
                    if mutation.stamp.source_journal_position != next_source_position {
                        return Err(MutationError::Storage(
                            "distributed batch source position changed during evaluation".into(),
                        ));
                    }
                    next_source_position = next_source_position
                        .checked_add(
                            1 + mutation
                                .alias_snapshot
                                .as_ref()
                                .map_or(0, |snapshot| snapshot.registry.aliases.len() as u64),
                        )
                        .ok_or_else(|| {
                            MutationError::Storage("local invalidation offset is exhausted".into())
                        })?;
                    high_watermark = Some(
                        high_watermark.map_or(value.receipt.version, |current: VersionId| {
                            current.max(value.receipt.version)
                        }),
                    );
                    pending_changes
                        .extend(value.pending_head_changes(identity, operation.key().path()));
                }
                if let Some(mutation) = value.mutation.as_ref() {
                    self.stage_object_mutation_reference_proof(&mut batch, mutation)?;
                }
            }
            evaluated.insert(
                *index,
                outcome.map(|value| CoordinatedObjectMutation {
                    receipt: value.receipt,
                    mutation: value.mutation,
                }),
            );
        }

        if receipt_status != initial_receipt_status {
            self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
        }
        self.stage_local_changes_with_admission(
            &mut batch,
            &pending_changes,
            LocalReferenceEffects::Deferred,
            source_journal_admission,
        )?;
        if let Some(high_watermark) = high_watermark {
            batch.put_cf(
                self.cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&high_watermark).map_err(storage_error)?,
            );
        }
        if !batch.is_empty() {
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)?;
        }
        if !pruned_receipts.is_empty() {
            self.mutation_capacity_notify.notify_waiters();
        }
        if !pending_changes.is_empty() {
            self.notify_local_invalidations();
        }
        if receipt_capacity_exhausted {
            return Err(MutationError::ReceiptCapacity);
        }

        let mut outcomes = Vec::with_capacity(prepared.len() + early.len());
        for index in 0..prepared.len() + early.len() {
            outcomes.push(match evaluated.remove(&index) {
                Some(outcome) => outcome,
                None => Err(early.remove(&index).ok_or_else(|| {
                    MutationError::Storage("distributed batch outcome index is inconsistent".into())
                })?),
            });
        }
        Ok(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PlacementLogId;

    fn request(path: &str, command: &str, blob: BlobRef) -> PublishRequest {
        PublishRequest {
            key: ObjectKey::new("tenant", "bucket", path).unwrap(),
            blob,
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some(command.into()),
            durability: Durability::Local,
        }
    }

    #[tokio::test]
    async fn multiple_distributed_publishes_use_one_physical_metadata_batch() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let first = store.stage_blob(b"first pack").await.unwrap();
        let second = store.stage_blob(b"second pack").await.unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        let before = store.db.latest_sequence_number();
        let outcomes = store
            .coordinate_distributed_publish_batch_with_governance(
                vec![
                    request("packs/0", "pack-0", first),
                    request("packs/1", "pack-1", second),
                ],
                governance,
                ObjectMutationContext {
                    active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                    serving_fence_term: 1,
                },
            )
            .await
            .unwrap();

        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(std::result::Result::is_ok));
        assert!(
            store
                .head(&ObjectKey::new("tenant", "bucket", "packs/0").unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .head(&ObjectKey::new("tenant", "bucket", "packs/1").unwrap())
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .db
                .get_updates_since(before)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn receipt_capacity_commits_only_the_successful_prefix_before_retry() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_mutation_receipt_retention(
                MutationReceiptRetention::new(60, 1, 1024 * 1024).unwrap(),
            ),
        )
        .await
        .unwrap();
        let first = store.stage_blob(b"first pack").await.unwrap();
        let second = store.stage_blob(b"second pack").await.unwrap();
        let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        let before = store.db.latest_sequence_number();

        let error = store
            .coordinate_distributed_publish_batch_with_governance(
                vec![
                    request("packs/0", "pack-0", first),
                    request("packs/1", "pack-1", second),
                ],
                governance,
                ObjectMutationContext {
                    active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                    serving_fence_term: 1,
                },
            )
            .await
            .unwrap_err();

        assert_eq!(error, MutationError::ReceiptCapacity);
        assert!(
            store
                .head(&ObjectKey::new("tenant", "bucket", "packs/0").unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .head(&ObjectKey::new("tenant", "bucket", "packs/1").unwrap())
                .unwrap()
                .is_none()
        );
        assert_eq!(store.mutation_receipt_status().unwrap().entries, 1);
        assert_eq!(
            store
                .db
                .get_updates_since(before)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            1
        );
    }
}
