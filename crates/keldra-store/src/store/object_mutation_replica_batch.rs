//! Ordered replica application for one exact metadata replica group.

use super::*;
use crate::{ObjectMutation, ReplicaObjectMutationApplied};

impl Store {
    /// Apply a coordinator-produced group with one synchronous RocksDB write.
    ///
    /// The caller has already recomputed the placement fence and exact replica
    /// group. This storage boundary preserves mutation order so multiple
    /// entries for one path see the successful predecessor staged earlier in
    /// this same physical batch.
    pub async fn apply_object_mutation_replica_batch(
        &self,
        mutations: &[ObjectMutation],
    ) -> Result<Vec<ReplicaObjectMutationApplied>, MutationError> {
        if mutations.is_empty() {
            return Err(MutationError::InvalidObjectMutation(
                "replica mutation batch must not be empty".into(),
            ));
        }
        for mutation in mutations {
            mutation.validate()?;
        }

        let _commit_guard = self.commit_lock.lock().await;
        let now = now_unix_millis()?;
        let mut batch = WriteBatch::default();
        let mut receipt_status = self.mutation_receipt_status()?;
        let initial_receipt_status = receipt_status;
        let pruned = self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status)?;
        let mut pending_heads = BTreeMap::<Vec<u8>, Head>::new();
        let mut pending_versions = BTreeMap::<Vec<u8>, Version>::new();
        let mut deleted_versions = BTreeSet::<Vec<u8>>::new();
        let mut pending_receipts = BTreeMap::<Vec<u8>, StoredReceipt>::new();
        let mut high_watermark =
            self.read_json::<VersionId>(CF_METADATA, VERSION_HIGH_WATERMARK_KEY)?;
        let mut outcomes = Vec::with_capacity(mutations.len());

        for mutation in mutations {
            let identity = BucketIdentity {
                tenant_id: TenantId(mutation.tenant_id),
                bucket_id: BucketId(mutation.bucket_id),
            };
            let encoded_head_key = identity.head_key(&mutation.exact_path);
            let encoded_version_key =
                replica_version_key(identity, &mutation.exact_path, mutation.version.id);
            let primary_receipt_key = receipt_key(identity, &mutation.command_id);
            let retained_receipt = match pending_receipts.get(&primary_receipt_key) {
                Some(receipt) => Some(receipt.clone()),
                None if pruned.contains(&primary_receipt_key) => None,
                None => self.read_json::<StoredReceipt>(CF_RECEIPTS, &primary_receipt_key)?,
            }
            .filter(|receipt| receipt.expires_at_unix_millis > now);
            let retained_identical_receipt = if let Some(existing) = retained_receipt.as_ref() {
                if existing.fingerprint != mutation.input_fingerprint
                    || existing.version != mutation.version.id
                    || existing.deleted != mutation.version.deleted
                    || existing.expires_at_unix_millis != mutation.receipt_expires_at_unix_millis
                    || existing.object_mutation.as_ref() != Some(mutation)
                {
                    return Err(MutationError::ObjectMutationConflict);
                }
                true
            } else {
                false
            };

            let proof_staged = self.stage_object_mutation_reference_proof(&mut batch, mutation)?;
            let current = match pending_heads.get(&encoded_head_key) {
                Some(head) => Some(head.clone()),
                None => self.head_by_storage_key(&encoded_head_key)?,
            };
            let locator_applied = mutation
                .definition_transition
                .as_ref()
                .map(|transition| self.definition_transition_is_applied(transition))
                .transpose()?
                .unwrap_or(true);
            let mut already_applied = false;
            match current.as_ref() {
                None if mutation.stamp.predecessor_version.is_some() => {
                    return Err(MutationError::ObjectMutationLineageGap {
                        current: None,
                        predecessor: mutation.stamp.predecessor_version,
                    });
                }
                None => {}
                Some(head) if head.version == mutation.version.id => {
                    let descriptor = read_replica_version(
                        self,
                        &pending_versions,
                        &deleted_versions,
                        &encoded_version_key,
                    )?
                    .ok_or_else(|| {
                        MutationError::Storage(
                            "replicated head references a missing version descriptor".into(),
                        )
                    })?;
                    if head.deleted == mutation.version.deleted
                        && head.mutation_stamp == Some(mutation.stamp)
                        && descriptor == mutation.version
                    {
                        if retained_identical_receipt && !proof_staged && locator_applied {
                            outcomes.push(ReplicaObjectMutationApplied {
                                version: mutation.version.id,
                                replayed: true,
                            });
                            continue;
                        }
                        already_applied = true;
                    } else if head.mutation_stamp.is_some_and(|stamp| {
                        stamp.predecessor_version == mutation.stamp.predecessor_version
                    }) {
                        return Err(MutationError::ObjectMutationSibling {
                            predecessor: mutation.stamp.predecessor_version,
                        });
                    } else {
                        return Err(MutationError::ObjectMutationConflict);
                    }
                }
                Some(head) if Some(head.version) == mutation.stamp.predecessor_version => {}
                Some(head)
                    if retained_identical_receipt
                        && head.mutation_stamp.is_some_and(|stamp| {
                            stamp.predecessor_version == Some(mutation.version.id)
                        }) =>
                {
                    if !proof_staged {
                        outcomes.push(ReplicaObjectMutationApplied {
                            version: mutation.version.id,
                            replayed: true,
                        });
                        continue;
                    }
                    already_applied = true;
                }
                Some(head)
                    if head.mutation_stamp.is_some_and(|stamp| {
                        stamp.predecessor_version == mutation.stamp.predecessor_version
                    }) =>
                {
                    return Err(MutationError::ObjectMutationSibling {
                        predecessor: mutation.stamp.predecessor_version,
                    });
                }
                Some(head) => {
                    return Err(MutationError::ObjectMutationLineageGap {
                        current: Some(head.version),
                        predecessor: mutation.stamp.predecessor_version,
                    });
                }
            }

            if let Some(existing) = read_replica_version(
                self,
                &pending_versions,
                &deleted_versions,
                &encoded_version_key,
            )? && existing != mutation.version
            {
                return Err(MutationError::ObjectMutationConflict);
            }
            if !retained_identical_receipt
                && retained_receipt.is_some()
                && !pruned.contains(&primary_receipt_key)
            {
                return Err(MutationError::ObjectMutationConflict);
            }

            if !already_applied {
                if mutation.retire_predecessor {
                    let predecessor = mutation.stamp.predecessor_version.ok_or_else(|| {
                        MutationError::InvalidObjectMutation(
                            "retired predecessor is missing from mutation lineage".into(),
                        )
                    })?;
                    let predecessor_key =
                        replica_version_key(identity, &mutation.exact_path, predecessor);
                    batch.delete_cf(self.cf(CF_VERSIONS)?, &predecessor_key);
                    pending_versions.remove(&predecessor_key);
                    deleted_versions.insert(predecessor_key);
                }
                batch.put_cf(
                    self.cf(CF_VERSIONS)?,
                    &encoded_version_key,
                    serde_json::to_vec(&mutation.version).map_err(storage_error)?,
                );
                let head = Head {
                    version: mutation.version.id,
                    deleted: mutation.version.deleted,
                    mutation_stamp: Some(mutation.stamp),
                };
                batch.put_cf(
                    self.cf(CF_HEADS)?,
                    &encoded_head_key,
                    serde_json::to_vec(&head).map_err(storage_error)?,
                );
                deleted_versions.remove(&encoded_version_key);
                pending_versions.insert(encoded_version_key.clone(), mutation.version.clone());
                pending_heads.insert(encoded_head_key.clone(), head);
            }
            if !retained_identical_receipt && mutation.receipt_expires_at_unix_millis > now {
                self.stage_stored_mutation_receipt(
                    &mut batch,
                    primary_receipt_key,
                    StoredReceipt {
                        fingerprint: mutation.input_fingerprint,
                        version: mutation.version.id,
                        deleted: mutation.version.deleted,
                        expires_at_unix_millis: mutation.receipt_expires_at_unix_millis,
                        object_mutation: Some(mutation.clone()),
                        definition_transition: mutation.definition_transition.clone(),
                    },
                    &mut receipt_status,
                    &mut pending_receipts,
                )?;
            }
            high_watermark = Some(high_watermark.map_or(mutation.version.id, |current| {
                current.max(mutation.version.id)
            }));
            let mutation_is_current = !already_applied
                || current
                    .as_ref()
                    .is_some_and(|head| head.version == mutation.version.id);
            if mutation_is_current && let Some(transition) = mutation.definition_transition.as_ref()
            {
                self.stage_definition_transition(&mut batch, transition)
                    .map_err(|error| MutationError::InvalidObjectMutation(error.to_string()))?;
            }
            outcomes.push(ReplicaObjectMutationApplied {
                version: mutation.version.id,
                replayed: already_applied,
            });
        }

        if receipt_status != initial_receipt_status {
            self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
        }
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
        if !pruned.is_empty() {
            self.mutation_capacity_notify.notify_waiters();
        }
        for mutation in mutations {
            self.clock.observe(mutation.version.id);
        }
        Ok(outcomes)
    }
}

fn replica_version_key(identity: BucketIdentity, exact_path: &str, version: VersionId) -> Vec<u8> {
    let mut encoded = identity.head_key(exact_path);
    encoded.push(0);
    encoded.extend_from_slice(&version.0.to_be_bytes());
    encoded
}

fn read_replica_version(
    store: &Store,
    pending: &BTreeMap<Vec<u8>, Version>,
    deleted: &BTreeSet<Vec<u8>>,
    key: &[u8],
) -> Result<Option<Version>, MutationError> {
    if deleted.contains(key) {
        return Ok(None);
    }
    match pending.get(key) {
        Some(version) => Ok(Some(version.clone())),
        None => store.read_json(CF_VERSIONS, key),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BatchOperation, Durability, ObjectMutationContext, ObjectMutationGovernance,
        PlacementLogId, PutMode, PutRequest, StoreOptions,
    };

    fn put(path: &str, command: &str) -> BatchOperation {
        BatchOperation::Put(PutRequest {
            key: ObjectKey::new("tenant", "bucket", path).unwrap(),
            bytes: format!("payload-{command}").into_bytes(),
            content_type: Some("application/octet-stream".into()),
            mode: PutMode::Put,
            command_id: Some(command.into()),
            durability: Durability::Local,
        })
    }

    async fn mutations(operations: Vec<BatchOperation>) -> Vec<ObjectMutation> {
        let temporary = tempfile::tempdir().unwrap();
        let source = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = source.resolve_bucket_ids("tenant", "bucket").unwrap();
        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: source.bucket_versioning("tenant", "bucket").unwrap(),
            policy: source.bucket_policy("tenant", "bucket").unwrap(),
        };
        let context = ObjectMutationContext {
            active_placement_log_id: PlacementLogId { term: 1, index: 1 },
            serving_fence_term: 1,
        };
        let mut result = Vec::new();
        for operation in operations {
            let coordinated = source
                .coordinate_object_mutation_with_governance(operation, governance.clone(), context)
                .await
                .unwrap();
            result.push(coordinated.mutation.unwrap());
        }
        result
    }

    #[tokio::test]
    async fn batch_matches_unary_and_replays_receipts() {
        let mutations = mutations(vec![put("a", "first"), put("b", "second")]).await;
        let unary_dir = tempfile::tempdir().unwrap();
        let unary = Store::open(StoreOptions::new(unary_dir.path(), 2))
            .await
            .unwrap();
        for mutation in &mutations {
            unary.apply_object_mutation_replica(mutation).await.unwrap();
        }
        let batch_dir = tempfile::tempdir().unwrap();
        let batch = Store::open(StoreOptions::new(batch_dir.path(), 2))
            .await
            .unwrap();
        let before = batch.db.latest_sequence_number();
        let applied = batch
            .apply_object_mutation_replica_batch(&mutations)
            .await
            .unwrap();
        assert!(applied.iter().all(|outcome| !outcome.replayed));
        assert_eq!(
            batch
                .db
                .get_updates_since(before)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            1
        );
        for mutation in &mutations {
            assert_eq!(
                batch
                    .head_by_storage_key(
                        &BucketIdentity {
                            tenant_id: TenantId(mutation.tenant_id),
                            bucket_id: BucketId(mutation.bucket_id),
                        }
                        .head_key(&mutation.exact_path),
                    )
                    .unwrap(),
                unary
                    .head_by_storage_key(
                        &BucketIdentity {
                            tenant_id: TenantId(mutation.tenant_id),
                            bucket_id: BucketId(mutation.bucket_id),
                        }
                        .head_key(&mutation.exact_path),
                    )
                    .unwrap()
            );
        }
        let replay = batch
            .apply_object_mutation_replica_batch(&mutations)
            .await
            .unwrap();
        assert!(replay.iter().all(|outcome| outcome.replayed));
    }

    #[tokio::test]
    async fn repeated_path_observes_the_prior_staged_head() {
        let first = mutations(vec![put("same", "first")]).await.pop().unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        let source = Store::open(StoreOptions::new(source_dir.path(), 1))
            .await
            .unwrap();
        source.apply_object_mutation_replica(&first).await.unwrap();
        let second = ObjectMutation {
            command_id: "second".into(),
            version: Version {
                id: VersionId(first.version.id.0 + 1),
                blob: None,
                content_type: None,
                deleted: true,
                committed_at_unix_millis: first.version.committed_at_unix_millis + 1,
            },
            retire_predecessor: true,
            stamp: crate::MutationStamp {
                predecessor_version: Some(first.version.id),
                source_journal_position: first.stamp.source_journal_position + 1,
                ..first.stamp
            },
            reference_deltas: first
                .version
                .blob
                .clone()
                .into_iter()
                .map(|blob| crate::ReferenceDelta { blob, change: -1 })
                .collect(),
            accounting_transition: Some(crate::AccountingHeadTransition::new(
                first.version.blob.as_ref().map(|blob| blob.length),
                None,
            )),
            ..first.clone()
        };
        let mut second = second;
        second.input_fingerprint = [7; 32];
        second.receipt_expires_at_unix_millis = first.receipt_expires_at_unix_millis;
        second.set_computed_fingerprint();

        let replica_dir = tempfile::tempdir().unwrap();
        let replica = Store::open(StoreOptions::new(replica_dir.path(), 2))
            .await
            .unwrap();
        let outcomes = replica
            .apply_object_mutation_replica_batch(&[first.clone(), second.clone()])
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 2);
        assert_eq!(
            replica
                .head_by_storage_key(
                    &BucketIdentity {
                        tenant_id: TenantId(second.tenant_id),
                        bucket_id: BucketId(second.bucket_id),
                    }
                    .head_key(&second.exact_path),
                )
                .unwrap()
                .unwrap()
                .version,
            second.version.id
        );
    }

    #[tokio::test]
    async fn lineage_gap_aborts_the_replica_batch_before_commit() {
        let mut mutations = mutations(vec![put("accepted", "first"), put("gap", "second")]).await;
        let incoming_version = mutations[1].version.id.0;
        assert!(incoming_version > 1);
        mutations[1].stamp.predecessor_version = Some(VersionId(incoming_version - 1));
        mutations[1].set_computed_fingerprint();
        let replica_dir = tempfile::tempdir().unwrap();
        let replica = Store::open(StoreOptions::new(replica_dir.path(), 2))
            .await
            .unwrap();

        assert!(matches!(
            replica
                .apply_object_mutation_replica_batch(&mutations)
                .await
                .unwrap_err(),
            MutationError::ObjectMutationLineageGap { .. }
        ));
        let identity = BucketIdentity {
            tenant_id: TenantId(mutations[0].tenant_id),
            bucket_id: BucketId(mutations[0].bucket_id),
        };
        assert!(
            replica
                .head_by_storage_key(&identity.head_key(&mutations[0].exact_path))
                .unwrap()
                .is_none()
        );
    }
}
