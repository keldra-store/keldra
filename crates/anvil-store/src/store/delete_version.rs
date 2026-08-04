use super::*;
use crate::{
    CoordinatedRetainedVersionDelete, MUTATION_STAMP_FORMAT, MutationStamp, ObjectMutationContext,
    ObjectMutationGovernance, RETAINED_VERSION_DELETE_FORMAT, ReplicaRetainedVersionDeleteApplied,
    RetainedVersionDeleteMutation,
};

impl Store {
    /// Select and commit one retained-version deletion on the exact-path
    /// coordinator. Metadata and the ordered reference effect share one sync
    /// batch; content owners consume that effect from the ordinary journal.
    pub async fn coordinate_retained_version_delete(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedRetainedVersionDelete, MutationError> {
        governance.validate()?;
        if governance.versioning != ObjectVersioning::Enabled {
            return Err(MutationError::ObjectVersioningNotEnabled);
        }
        if governance.policy.is_program_only(key.path()) && !is_program_definition_path(key.path())
        {
            return Err(MutationError::ProgramConcurrencyViolation);
        }
        if governance.policy.is_immutable(key.path()) || is_program_definition_path(key.path()) {
            return Err(MutationError::Immutable);
        }
        let identity = BucketIdentity {
            tenant_id: TenantId(governance.tenant_id),
            bucket_id: BucketId(governance.bucket_id),
        };
        let _path_guard = self.ordinary_locks.acquire(&[object_path(key)]).await;
        let _commit_guard = self.commit_lock.lock().await;
        let head_key = identity.head_key(key.path());
        let Some(expected_head) = self.head_by_storage_key(&head_key)? else {
            return Ok(not_found());
        };
        let Some(target) = self.version_metadata_by_identity(identity, key, version_id)? else {
            if expected_head.version == version_id {
                return Err(MutationError::Storage(
                    "head references a missing retained version".into(),
                ));
            }
            return Ok(not_found());
        };
        if target.id != version_id || target.deleted != target.blob.is_none() {
            return Err(MutationError::Storage(
                "retained version descriptor is malformed".into(),
            ));
        }
        if expected_head.version == version_id && target.deleted {
            return Err(MutationError::CurrentTombstoneCannotBeDeleted);
        }

        let source = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let source_journal_position = source.tail.checked_add(1).ok_or_else(|| {
            MutationError::Storage("local invalidation offset is exhausted".into())
        })?;
        let now = now_unix_millis()?;
        let replacement_tombstone = if expected_head.version == version_id {
            Some(Version {
                id: self.clock.next().map_err(storage_error)?,
                blob: None,
                content_type: None,
                deleted: true,
                committed_at_unix_millis: now,
            })
        } else {
            None
        };
        let reference_deltas = target
            .blob
            .as_ref()
            .map(|blob| ReferenceDelta {
                blob: blob.clone(),
                change: -1,
            })
            .into_iter()
            .collect();
        let predecessor_version = expected_head.version;
        let mut mutation = RetainedVersionDeleteMutation {
            format: RETAINED_VERSION_DELETE_FORMAT,
            tenant_id: governance.tenant_id,
            bucket_id: governance.bucket_id,
            exact_path: key.path().to_owned(),
            expected_head,
            target,
            replacement_tombstone,
            stamp: MutationStamp {
                format: MUTATION_STAMP_FORMAT,
                predecessor_version: Some(predecessor_version),
                program_commit_cursor: None,
                mutation_fingerprint: [0; 32],
                active_placement_log_id: context.active_placement_log_id,
                serving_fence_term: context.serving_fence_term,
                source_id: source.source_id,
                source_journal_position,
            },
            reference_deltas,
        };
        mutation.set_computed_fingerprint();
        mutation.validate()?;

        let mut batch = WriteBatch::default();
        self.stage_retained_version_delete(&mut batch, &mutation)?;
        self.stage_local_changes(
            &mut batch,
            &[PendingLocalChange::RetainedVersionDeleted {
                identity,
                exact_path: key.path().to_owned(),
                deleted_version: version_id,
                resulting_head_version: mutation
                    .replacement_tombstone
                    .as_ref()
                    .map(|version| version.id),
                reference_deltas: mutation.reference_deltas.clone(),
                accounting_transition: Some(if mutation.replacement_tombstone.is_some() {
                    AccountingHeadTransition::new(
                        mutation.target.blob.as_ref().map(|blob| blob.length),
                        None,
                    )
                } else {
                    AccountingHeadTransition::new(None, None)
                }),
            }],
        )?;
        self.stage_retained_version_delete_reference_proof(&mut batch, &mutation)?;
        self.write_retained_version_delete(batch)?;
        if let Some(replacement) = mutation.replacement_tombstone.as_ref() {
            self.clock.observe(replacement.id);
        }
        self.notify_local_invalidations();
        Ok(CoordinatedRetainedVersionDelete {
            outcome: mutation.outcome(),
            mutation: Some(mutation),
        })
    }

    /// Apply one coordinator-produced retained-version deletion to a complete
    /// metadata replica. Reference counts are changed only by ordered journal
    /// delivery, never while installing metadata.
    pub async fn apply_retained_version_delete_replica(
        &self,
        mutation: &RetainedVersionDeleteMutation,
    ) -> Result<ReplicaRetainedVersionDeleteApplied, MutationError> {
        mutation.validate()?;
        let identity = BucketIdentity {
            tenant_id: TenantId(mutation.tenant_id),
            bucket_id: BucketId(mutation.bucket_id),
        };
        let key = ObjectKey::new("typed", "delete-version", &mutation.exact_path)
            .map_err(|error| MutationError::InvalidObjectMutation(error.to_string()))?;
        let _commit_guard = self.commit_lock.lock().await;
        let head_key = identity.head_key(&mutation.exact_path);
        let current = self.head_by_storage_key(&head_key)?;
        let replacement_head = mutation
            .replacement_tombstone
            .as_ref()
            .map(|replacement| Head {
                version: replacement.id,
                deleted: true,
                mutation_stamp: Some(mutation.stamp),
            });
        let target = self.version_metadata_by_identity(identity, &key, mutation.target.id)?;
        let replayed = if current.as_ref() == replacement_head.as_ref() {
            let replacement = mutation
                .replacement_tombstone
                .as_ref()
                .ok_or(MutationError::ObjectMutationConflict)?;
            if target.is_some()
                || self.version_metadata_by_identity(identity, &key, replacement.id)?
                    != Some(replacement.clone())
            {
                return Err(MutationError::ObjectMutationConflict);
            }
            true
        } else {
            if current.as_ref() != Some(&mutation.expected_head) {
                return Err(MutationError::ObjectMutationLineageGap {
                    current: current.map(|head| head.version),
                    predecessor: Some(mutation.expected_head.version),
                });
            }
            match target {
                Some(target) if target == mutation.target => false,
                None if mutation.replacement_tombstone.is_none() => true,
                _ => return Err(MutationError::ObjectMutationConflict),
            }
        };

        let mut batch = WriteBatch::default();
        if !replayed {
            self.stage_retained_version_delete(&mut batch, mutation)?;
        }
        self.stage_retained_version_delete_reference_proof(&mut batch, mutation)?;
        if !batch.is_empty() {
            self.write_retained_version_delete(batch)?;
        }
        if let Some(replacement) = mutation.replacement_tombstone.as_ref() {
            self.clock.observe(replacement.id);
        }
        Ok(ReplicaRetainedVersionDeleteApplied {
            outcome: mutation.outcome(),
            replayed,
        })
    }

    fn stage_retained_version_delete(
        &self,
        batch: &mut WriteBatch,
        mutation: &RetainedVersionDeleteMutation,
    ) -> Result<(), MutationError> {
        let identity = BucketIdentity {
            tenant_id: TenantId(mutation.tenant_id),
            bucket_id: BucketId(mutation.bucket_id),
        };
        let key = ObjectKey::new("typed", "delete-version", &mutation.exact_path)
            .map_err(|error| MutationError::InvalidObjectMutation(error.to_string()))?;
        batch.delete_cf(
            self.cf(CF_VERSIONS)?,
            version_key(identity, &key, mutation.target.id),
        );
        if let Some(replacement) = mutation.replacement_tombstone.as_ref() {
            batch.put_cf(
                self.cf(CF_VERSIONS)?,
                version_key(identity, &key, replacement.id),
                serde_json::to_vec(replacement).map_err(storage_error)?,
            );
            batch.put_cf(
                self.cf(CF_HEADS)?,
                identity.head_key(&mutation.exact_path),
                serde_json::to_vec(&Head {
                    version: replacement.id,
                    deleted: true,
                    mutation_stamp: Some(mutation.stamp),
                })
                .map_err(storage_error)?,
            );
            let high_watermark = self
                .read_json::<VersionId>(CF_METADATA, VERSION_HIGH_WATERMARK_KEY)?
                .map_or(replacement.id, |current| current.max(replacement.id));
            batch.put_cf(
                self.cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&high_watermark).map_err(storage_error)?,
            );
        }
        Ok(())
    }

    fn write_retained_version_delete(&self, batch: WriteBatch) -> Result<(), MutationError> {
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)
    }
}

fn not_found() -> CoordinatedRetainedVersionDelete {
    CoordinatedRetainedVersionDelete {
        outcome: DeleteRetainedVersionOutcome::NotFound,
        mutation: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PlacementLogId;

    async fn open(node: u16) -> (tempfile::TempDir, Store) {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), node))
            .await
            .unwrap();
        (temporary, store)
    }

    fn key() -> ObjectKey {
        ObjectKey::new("tenant", "bucket", "ledger/entry").unwrap()
    }

    fn put(bytes: &[u8], mode: PutMode, command: &str) -> PutRequest {
        PutRequest {
            key: key(),
            bytes: bytes.to_vec(),
            content_type: None,
            mode,
            command_id: Some(command.into()),
            durability: Durability::Local,
        }
    }

    #[tokio::test]
    async fn replicated_delete_is_fenced_idempotent_and_proves_one_ordered_delta() {
        let (_source_dir, source) = open(1).await;
        let (_replica_dir, replica) = open(2).await;
        source
            .enable_bucket_versioning("tenant", "bucket")
            .await
            .unwrap();
        let first = source
            .put(put(b"first", PutMode::PutIfAbsent, "first"))
            .await
            .unwrap();
        source
            .put(put(
                b"second",
                PutMode::PutIfVersion(first.version),
                "second",
            ))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = source.resolve_bucket_ids("tenant", "bucket").unwrap();
        let before = source
            .export_object_path_record(tenant_id, bucket_id, key().path())
            .unwrap()
            .unwrap();
        replica
            .install_quorum_reconciled_object_record(&ObjectRecordExport::ExactPath(before))
            .await
            .unwrap();

        let governance = ObjectMutationGovernance {
            tenant_id,
            bucket_id,
            versioning: ObjectVersioning::Enabled,
            policy: BucketPolicy::default(),
        };
        let context = ObjectMutationContext {
            active_placement_log_id: PlacementLogId { term: 4, index: 9 },
            serving_fence_term: 4,
        };
        let coordinated = source
            .coordinate_retained_version_delete(&key(), first.version, governance.clone(), context)
            .await
            .unwrap();
        assert_eq!(
            coordinated.outcome,
            DeleteRetainedVersionOutcome::DeletedNonCurrent
        );
        let mutation = coordinated.mutation.unwrap();
        let proof = source
            .read_reference_proof(
                mutation.stamp.source_id,
                mutation.stamp.source_journal_position,
            )
            .unwrap()
            .unwrap();
        assert_eq!(proof.change.reference_deltas(), mutation.reference_deltas);

        let applied = replica
            .apply_retained_version_delete_replica(&mutation)
            .await
            .unwrap();
        assert!(!applied.replayed);
        let replay = replica
            .apply_retained_version_delete_replica(&mutation)
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(
            replica
                .read_reference_proof(
                    mutation.stamp.source_id,
                    mutation.stamp.source_journal_position,
                )
                .unwrap(),
            Some(proof)
        );
        assert_eq!(
            source
                .export_object_path_record(tenant_id, bucket_id, key().path())
                .unwrap(),
            replica
                .export_object_path_record(tenant_id, bucket_id, key().path())
                .unwrap()
        );

        let current = source.head(&key()).unwrap().unwrap().version;
        let coordinated = source
            .coordinate_retained_version_delete(&key(), current, governance, context)
            .await
            .unwrap();
        let outcome = coordinated.outcome.clone();
        let DeleteRetainedVersionOutcome::ReplacedCurrentWithTombstone { version } = outcome else {
            panic!("current deletion must install a fresh tombstone")
        };
        assert!(version > current);
        let mutation = coordinated.mutation.unwrap();
        let applied = replica
            .apply_retained_version_delete_replica(&mutation)
            .await
            .unwrap();
        assert_eq!(applied.outcome, coordinated.outcome);
        assert_eq!(
            source
                .export_object_path_record(tenant_id, bucket_id, key().path())
                .unwrap(),
            replica
                .export_object_path_record(tenant_id, bucket_id, key().path())
                .unwrap()
        );
    }
}
