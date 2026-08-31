//! Hard source-journal admission and trusted derived-publication progress debt.

use super::*;
use crate::BlobUpload;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SourceJournalAdmission {
    Bounded,
    DerivedProgress,
}

impl Store {
    pub(super) fn observe_source_journal_progress_debt(&self) {
        let Ok(status) = self.local_watch_status() else {
            return;
        };
        self.source_journal_progress_debt_peak_entries.fetch_max(
            status
                .retained_entries
                .saturating_sub(self.watch_retention.max_entries),
            std::sync::atomic::Ordering::Relaxed,
        );
        self.source_journal_progress_debt_peak_bytes.fetch_max(
            status
                .retained_bytes
                .saturating_sub(self.watch_retention.max_bytes),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Persist a standalone proof-backed prune pass while a writer is waiting.
    /// This deliberately cannot share the rejected mutation's RocksDB batch:
    /// publication debt must be fully repaid before an ordinary append begins.
    pub(super) async fn prune_source_journal_for_capacity(&self) -> Result<bool, MutationError> {
        let _commit_guard = self.lock_commit("journal_capacity").await;
        let before = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        self.enforce_local_watch_retention_inner(true)
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let after = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let pruned = after.retention_floor > before.retention_floor;
        if pruned {
            self.mutation_capacity_notify.notify_waiters();
            self.notify_local_invalidations();
        }
        Ok(pruned)
    }

    /// Trusted derived-artifact staging. This is hidden from the supported
    /// storage API and is entered only by Keldra's validated index/accounting
    /// publication boundary.
    #[doc(hidden)]
    pub async fn stage_derived_progress_blob(
        &self,
        bytes: &[u8],
    ) -> Result<BlobRef, MutationError> {
        self.stage_blob_with_admission(bytes, SourceJournalAdmission::DerivedProgress)
            .await
    }

    /// Streaming counterpart to [`Store::stage_derived_progress_blob`].
    #[doc(hidden)]
    pub async fn seal_derived_progress_blob_upload(
        &self,
        upload: BlobUpload,
    ) -> Result<BlobRef, MutationError> {
        self.seal_blob_upload_with_admission(upload, SourceJournalAdmission::DerivedProgress)
            .await
    }

    /// Publish trusted derived state even when the configured source-journal
    /// boundary is full. The ordinary object and source-journal batches remain
    /// indivisible; only their admission class differs.
    #[doc(hidden)]
    pub async fn mutate_derived_progress_with_governance_and_backpressure(
        &self,
        request: PublishRequest,
        governance: crate::model::ObjectMutationGovernance,
    ) -> Result<MutationReceipt, MutationError> {
        governance.validate()?;
        self.bulk_write_inner(
            vec![BatchOperation::Publish(request)],
            Some(governance),
            None,
            true,
            SourceJournalAdmission::DerivedProgress,
        )
        .await
        .pop()
        .expect("one operation has one outcome")
        .result
    }

    /// Grouped counterpart used only after the index publication boundary has
    /// authenticated and validated every immutable artifact.
    #[doc(hidden)]
    pub async fn bulk_write_derived_progress_with_backpressure(
        &self,
        requests: Vec<PublishRequest>,
    ) -> Vec<BatchOutcome> {
        self.bulk_write_inner(
            requests.into_iter().map(BatchOperation::Publish).collect(),
            None,
            None,
            true,
            SourceJournalAdmission::DerivedProgress,
        )
        .await
    }

    /// Distributed counterpart used only by the authenticated artifact path.
    #[doc(hidden)]
    pub async fn coordinate_derived_progress_publish_with_governance(
        &self,
        request: PublishRequest,
        governance: crate::model::ObjectMutationGovernance,
        context: crate::model::ObjectMutationContext,
    ) -> Result<crate::model::CoordinatedObjectMutation, MutationError> {
        if context.serving_fence_term == 0 {
            return Err(MutationError::InvalidObjectMutation(
                "serving-fence term must be non-zero".into(),
            ));
        }
        governance.validate()?;
        let identity = BucketIdentity {
            tenant_id: TenantId(governance.tenant_id),
            bucket_id: BucketId(governance.bucket_id),
        };
        let prepared = self.prepare_verified_distributed_publish(request, identity)?;
        self.coordinate_prepared_object_mutation(
            prepared,
            context,
            governance,
            None,
            SourceJournalAdmission::DerivedProgress,
        )
        .await
    }

    pub(super) fn enforce_local_watch_retention(&self) -> Result<(), WatchError> {
        self.enforce_local_watch_retention_inner(false)
    }

    fn enforce_local_watch_retention_inner(&self, force_headroom: bool) -> Result<(), WatchError> {
        let journal = self
            .cf(CF_LOCAL_INVALIDATIONS)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let metadata = self
            .cf(CF_METADATA)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let mut status = self.local_watch_status()?;
        if !force_headroom
            && status.retained_entries <= self.watch_retention.max_entries
            && status.retained_bytes <= self.watch_retention.max_bytes
        {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        let reference_safe_through = self
            .source_journal_reference_safe_through
            .load(std::sync::atomic::Ordering::Acquire)
            .min(status.settled_through);
        let (index_safe_through, accounting_safe_through) = self
            .derived_consumer_safe_through(status)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let safe_through = reference_safe_through
            .min(index_safe_through)
            .min(accounting_safe_through);
        let mut retired_version_keys = BTreeSet::new();
        let mut delayed_releases = Vec::new();
        let mut pruned_records = 0_u64;
        while (status.retained_entries > self.watch_retention.max_entries
            || status.retained_bytes > self.watch_retention.max_bytes
            || (force_headroom && pruned_records == 0))
            && status.retention_floor < safe_through
        {
            let offset = status.retention_floor.checked_add(1).ok_or_else(|| {
                WatchError::Storage("local invalidation retention floor is exhausted".into())
            })?;
            let encoded = self
                .db
                .get_cf(journal, invalidation_key(offset))
                .map_err(|error| WatchError::Storage(error.to_string()))?
                .ok_or_else(|| {
                    WatchError::Storage(format!(
                        "retained local invalidation offset {offset} is missing"
                    ))
                })?;
            let pruned_change = self
                .decode_local_change_record(&encoded)
                .map_err(|error| WatchError::Storage(error.to_string()))?;
            if pruned_change.offset() != offset {
                return Err(WatchError::Storage(
                    "local change key does not match its stored offset".into(),
                ));
            }
            self.stage_journal_route_removal(
                &mut batch,
                status.source_id.source_epoch,
                &pruned_change,
            )
            .map_err(|error| WatchError::Storage(error.to_string()))?;
            if let Some(release) = self
                .stage_pruned_change_version_retirement(
                    &mut batch,
                    &mut retired_version_keys,
                    &pruned_change,
                )
                .map_err(|error| WatchError::Storage(error.to_string()))?
            {
                delayed_releases.push(release);
            }
            batch.delete_cf(journal, invalidation_key(offset));
            status.retention_floor = offset;
            pruned_records += 1;
            status.retained_entries -= 1;
            status.retained_bytes = status
                .retained_bytes
                .checked_sub(invalidation_record_bytes(encoded.len()).saturating_add(
                    super::journal_routes::journal_route_logical_bytes(&pruned_change),
                ))
                .ok_or_else(|| {
                    WatchError::Storage("local invalidation byte accounting is inconsistent".into())
                })?;
        }
        for release in delayed_releases {
            let PendingLocalChange::ContentLifecycleChanged {
                blob_identity,
                revision,
                reference_deltas,
                accounting_transition,
            } = release
            else {
                unreachable!("source-version retirement emits one lifecycle release")
            };
            status.tail = status.tail.checked_add(1).ok_or_else(|| {
                WatchError::Storage("local invalidation offset is exhausted".into())
            })?;
            let release = LocalChange::content_lifecycle_changed(
                status.tail,
                blob_identity,
                revision,
                reference_deltas,
                accounting_transition,
            );
            let encoded = encode_local_change(&release)
                .map_err(|error| WatchError::Storage(error.to_string()))?;
            let logical_bytes = invalidation_record_bytes(encoded.len())
                .saturating_add(super::journal_routes::journal_route_logical_bytes(&release));
            self.stage_journal_routes(&mut batch, status.source_id.source_epoch, &release)
                .map_err(|error| WatchError::Storage(error.to_string()))?;
            batch.put_cf(journal, invalidation_key(status.tail), encoded);
            status.retained_entries = status.retained_entries.checked_add(1).ok_or_else(|| {
                WatchError::Storage("local invalidation entry count is exhausted".into())
            })?;
            status.retained_bytes = status
                .retained_bytes
                .checked_add(logical_bytes)
                .ok_or_else(|| {
                    WatchError::Storage("local invalidation byte count is exhausted".into())
                })?;
        }
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_OFFSET_KEY,
            status.tail.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_FLOOR_KEY,
            status.retention_floor.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_COUNT_KEY,
            status.retained_entries.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            LOCAL_INVALIDATION_BYTES_KEY,
            status.retained_bytes.to_be_bytes(),
        );
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .write_opt(batch, &options)
            .map_err(|error| WatchError::Storage(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ObjectMutationGovernance;
    use crate::{
        DerivedConsumerCheckpoint, DerivedConsumerKind, Durability, ObjectKey, PlacementLogId,
        PutMode, PutRequest, StoreOptions, WatchRetention,
    };

    fn fence() -> PlacementLogId {
        PlacementLogId { term: 1, index: 1 }
    }

    fn put(path: &str, command: &str) -> BatchOperation {
        BatchOperation::Put(PutRequest {
            key: ObjectKey::new("tenant", "bucket", path).unwrap(),
            bytes: vec![1],
            content_type: None,
            mode: PutMode::PutIfAbsent,
            command_id: Some(command.into()),
            durability: Durability::Local,
        })
    }

    async fn checkpoint_all(store: &Store, through: u64) {
        let source = store.local_watch_status().unwrap().source_id;
        for consumer_kind in DerivedConsumerKind::ALL {
            store
                .apply_derived_consumer_checkpoint(
                    DerivedConsumerCheckpoint {
                        consumer_kind,
                        source_id: source,
                        consumer_node_id: 1,
                        next_offset: through + 1,
                        observed_fence: fence(),
                    },
                    &[1],
                )
                .await
                .unwrap();
        }
    }

    async fn publish_progress(store: &Store, command: &str) {
        let blob = store
            .stage_derived_progress_blob(b"published progress")
            .await
            .unwrap();
        let identity = store.resolve_bucket_identity("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id: identity.tenant_id.0,
            bucket_id: identity.bucket_id.0,
            versioning: store.bucket_versioning("tenant", "bucket").unwrap(),
            policy: store.bucket_policy("tenant", "bucket").unwrap(),
        };
        store
            .mutate_derived_progress_with_governance_and_backpressure(
                PublishRequest {
                    key: ObjectKey::new("tenant", "bucket", "_keldra/index-projections/v6/0000000000000000000000000000000000000000000000000000000000000001/partitions/7/0202020202020202020202020202020202020202020202020202020202020202/3/4/current")
                        .unwrap(),
                    blob,
                    content_type: Some("application/vnd.keldra.index-artifact".into()),
                    mode: PutMode::PutIfAbsent,
                    command_id: Some(command.into()),
                    durability: Durability::Local,
                },
                governance,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn derived_publication_can_repay_a_full_journal_and_wake_a_source_writer() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1)
                .with_watch_retention(WatchRetention::new(1, 1024 * 1024).unwrap()),
        )
        .await
        .unwrap();
        store
            .ensure_derived_consumer_membership(fence(), &[1])
            .await
            .unwrap();
        assert!(
            store.bulk_write(vec![put("source", "source")]).await[0]
                .result
                .is_ok()
        );

        // A user-selected reserved-looking path receives ordinary bounded
        // admission. No path string grants the trusted progress capability.
        let forged = store
            .bulk_write(vec![put("_keldra/index-projections/v6/0000000000000000000000000000000000000000000000000000000000000001/partitions/7/0202020202020202020202020202020202020202020202020202020202020202/3/4/current", "forged-progress")])
            .await;
        assert_eq!(forged[0].result, Err(MutationError::SourceJournalCapacity));

        publish_progress(&store, "trusted-progress").await;
        let debt = store.source_journal_runtime_metrics().unwrap();
        assert!(debt.progress_debt_entries() >= 1);
        assert!(
            store
            .head(&ObjectKey::new("tenant", "bucket", "_keldra/index-projections/v6/0000000000000000000000000000000000000000000000000000000000000001/partitions/7/0202020202020202020202020202020202020202020202020202020202020202/3/4/current").unwrap())
                .unwrap()
                .is_some()
        );

        let waiting = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .bulk_write_with_backpressure(vec![put("after-progress", "after-progress")])
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());

        let tail = store.local_watch_status().unwrap().tail;
        store
            .advance_source_journal_reference_safe_through(tail)
            .await
            .unwrap();
        checkpoint_all(&store, tail).await;
        let outcomes = tokio::time::timeout(std::time::Duration::from_secs(2), waiting)
            .await
            .expect("published progress should wake the blocked source writer")
            .unwrap();
        assert!(outcomes[0].result.is_ok());
        let repaid = store.source_journal_runtime_metrics().unwrap();
        assert_eq!(repaid.progress_debt_entries(), 0);
        assert_eq!(repaid.progress_debt_bytes(), 0);
    }

    #[tokio::test]
    async fn bounded_append_cannot_refill_debt_during_the_same_prune_batch() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1)
                .with_watch_retention(WatchRetention::new(1, 1024 * 1024).unwrap()),
        )
        .await
        .unwrap();

        publish_progress(&store, "trusted-progress").await;
        let debt = store.source_journal_runtime_metrics().unwrap();
        assert!(debt.progress_debt_entries() >= 1);
        // With no installed cluster-derived membership, this publication is
        // already proof-safe. It still must be pruned in its own durable pass
        // before an ordinary append can begin.
        assert_eq!(debt.prune_safe_through(), debt.tail);
        let tail_with_debt = debt.tail;

        let bounded = store
            .bulk_write(vec![put("source", "bounded-source")])
            .await;
        assert_eq!(bounded[0].result, Err(MutationError::SourceJournalCapacity));
        assert_eq!(store.local_watch_status().unwrap().tail, tail_with_debt);

        assert!(store.prune_source_journal_for_capacity().await.unwrap());
        assert_eq!(
            store
                .source_journal_runtime_metrics()
                .unwrap()
                .progress_debt_entries(),
            0
        );
        // The first pass repays publication debt without admitting an
        // ordinary append in the same batch. A second forced pass creates
        // ordinary headroom, just as the production retry loop does when its
        // first retry still observes a full (but no longer indebted) journal.
        assert!(store.prune_source_journal_for_capacity().await.unwrap());
        assert!(
            store.bulk_write(vec![put("source", "after-prune")]).await[0]
                .result
                .is_ok()
        );
    }
}
