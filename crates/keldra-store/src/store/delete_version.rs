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
        self.coordinate_retained_version_delete_inner(
            key,
            version_id,
            governance,
            context,
            LocalReferenceEffects::Deferred,
        )
        .await
    }

    /// Select and commit a retained-version deletion whose reference effect is
    /// applied on this same node. The cluster layer must select this explicit
    /// entrypoint only for the one-node topology.
    pub async fn coordinate_local_retained_version_delete(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
    ) -> Result<CoordinatedRetainedVersionDelete, MutationError> {
        self.coordinate_retained_version_delete_inner(
            key,
            version_id,
            governance,
            context,
            LocalReferenceEffects::AppliedInline,
        )
        .await
    }

    async fn coordinate_retained_version_delete_inner(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
        governance: ObjectMutationGovernance,
        context: ObjectMutationContext,
        reference_effects: LocalReferenceEffects,
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
        let _commit_guard = self.lock_commit("retained_version_delete").await;
        self.require_unreserved_object_locked(identity, key.path(), None)?;
        let head_key = identity.head_key(key.path());
        let Some(expected_head) = self.head_by_storage_key(&head_key)? else {
            return Ok(not_found());
        };
        let Some(target) = self.user_retained_version(identity, key, version_id)? else {
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
        if target.protected_link_descriptor {
            return Err(MutationError::InvalidObjectMutation(
                "ordinary retained-version deletion names a protected link descriptor".into(),
            ));
        }
        if expected_head.version == version_id && target.deleted {
            return Err(MutationError::CurrentTombstoneCannotBeDeleted);
        }
        let alias_registry = self.alias_registry_locked(identity, key.path())?;
        if expected_head.version == version_id && alias_registry.is_some() {
            return Err(MutationError::ObjectHasInboundAliases);
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
                protected_link_descriptor: false,
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
            alias_paths: alias_registry
                .map(|registry| registry.aliases)
                .unwrap_or_default(),
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
        if reference_effects == LocalReferenceEffects::AppliedInline
            && let Some(reference) = mutation.target.blob.as_ref()
        {
            let mut pending_references = PendingBlobReferences::new();
            let (reference_key, state) =
                self.prepare_blob_reference_retirement(reference, &pending_references, now)?;
            self.stage_blob_reference_update(
                &mut batch,
                &mut pending_references,
                reference_key,
                state,
            )?;
        }
        self.stage_retained_version_delete(&mut batch, &mutation)?;
        let mut changes = Vec::with_capacity(mutation.alias_paths.len().saturating_add(1));
        changes.push(PendingLocalChange::RetainedVersionDeleted {
            identity,
            exact_path: key.path().to_owned(),
            canonical_path: None,
            deleted_version: version_id,
            resulting_head_version: mutation
                .replacement_tombstone
                .as_ref()
                .map(|version| version.id),
            reference_deltas: mutation.reference_deltas.clone(),
            accounting_transition: mutation.alias_paths.is_empty().then(|| {
                AccountingHeadTransition::new(
                    mutation
                        .replacement_tombstone
                        .as_ref()
                        .and(mutation.target.blob.as_ref().map(|blob| blob.length)),
                    None,
                    mutation.target.blob.as_ref().map_or(0, |blob| blob.length),
                )
            }),
        });
        changes.extend(mutation.alias_paths.iter().cloned().map(|exact_path| {
            PendingLocalChange::RetainedVersionDeleted {
                identity,
                exact_path,
                canonical_path: Some(mutation.exact_path.clone()),
                deleted_version: version_id,
                resulting_head_version: mutation
                    .replacement_tombstone
                    .as_ref()
                    .map(|version| version.id),
                reference_deltas: Vec::new(),
                accounting_transition: None,
            }
        }));
        self.stage_local_changes(&mut batch, &changes, reference_effects)?;
        if reference_effects == LocalReferenceEffects::Deferred {
            self.stage_retained_version_delete_reference_proof(&mut batch, &mutation)?;
        }
        self.write_retained_version_delete(batch)?;
        if let Some(replacement) = mutation.replacement_tombstone.as_ref() {
            self.clock.observe(replacement.id);
        }
        if reference_effects == LocalReferenceEffects::AppliedInline {
            self.settle_inline_source_changes()?;
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
        let _commit_guard = self.lock_commit("retained_version_delete").await;
        self.require_unreserved_object_locked(identity, &mutation.exact_path, None)?;
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
            if mutation.replacement_tombstone.is_some()
                && self
                    .alias_registry_locked(identity, &mutation.exact_path)?
                    .is_some()
            {
                return Err(MutationError::ObjectHasInboundAliases);
            }
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
                serde_json::to_vec(&StoredVersion::new(
                    replacement.clone(),
                    StoredVersionRetention::UserRetained,
                ))
                .map_err(storage_error)?,
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
    use crate::{OBJECT_ALIAS_REGISTRY_FORMAT, ObjectAliasRegistry, PlacementLogId};

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

    fn replace_alias_registry_for_test(
        store: &Store,
        identity: BucketIdentity,
        expected: Option<&ObjectAliasRegistry>,
        aliases: &[String],
        commit_cursor: u64,
    ) -> Option<ObjectAliasRegistry> {
        let mut batch = WriteBatch::default();
        let (_, replacement) = store
            .stage_alias_registry_transition_locked(
                &mut batch,
                identity,
                key().path(),
                expected,
                aliases,
                commit_cursor,
            )
            .unwrap();
        store.db.write(batch).unwrap();
        replacement
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

    #[tokio::test]
    async fn noncurrent_delete_commutes_with_later_alias_registry_changes_on_replicas() {
        let (_source_dir, source) = open(1).await;
        let (_removed_dir, removed_replica) = open(2).await;
        let (_added_dir, added_replica) = open(3).await;
        source
            .enable_bucket_versioning("tenant", "bucket")
            .await
            .unwrap();
        let first = source
            .put(put(b"first", PutMode::PutIfAbsent, "alias-first"))
            .await
            .unwrap();
        source
            .put(put(
                b"second",
                PutMode::PutIfVersion(first.version),
                "alias-second",
            ))
            .await
            .unwrap();
        let (tenant_id, bucket_id) = source.resolve_bucket_ids("tenant", "bucket").unwrap();
        let identity = BucketIdentity {
            tenant_id: TenantId(tenant_id),
            bucket_id: BucketId(bucket_id),
        };
        let initial_aliases = vec!["aliases/a".to_owned()];
        let initial = replace_alias_registry_for_test(&source, identity, None, &initial_aliases, 1)
            .expect("initial alias registry");
        let record = source
            .export_object_path_record(tenant_id, bucket_id, key().path())
            .unwrap()
            .expect("source path snapshot");
        for replica in [&removed_replica, &added_replica] {
            replica
                .install_quorum_reconciled_object_record(&ObjectRecordExport::ExactPath(
                    record.clone(),
                ))
                .await
                .unwrap();
        }

        let mutation = source
            .coordinate_retained_version_delete(
                &key(),
                first.version,
                ObjectMutationGovernance {
                    tenant_id,
                    bucket_id,
                    versioning: ObjectVersioning::Enabled,
                    policy: BucketPolicy::default(),
                },
                ObjectMutationContext {
                    active_placement_log_id: PlacementLogId { term: 4, index: 9 },
                    serving_fence_term: 4,
                },
            )
            .await
            .unwrap()
            .mutation
            .expect("new retained delete");
        assert_eq!(mutation.alias_paths, initial_aliases);

        assert!(
            replace_alias_registry_for_test(&removed_replica, identity, Some(&initial), &[], 2)
                .is_none()
        );
        let expanded_aliases = vec!["aliases/a".to_owned(), "aliases/z".to_owned()];
        let expanded = replace_alias_registry_for_test(
            &added_replica,
            identity,
            Some(&initial),
            &expanded_aliases,
            2,
        )
        .expect("expanded alias registry");

        for replica in [&removed_replica, &added_replica] {
            let applied = replica
                .apply_retained_version_delete_replica(&mutation)
                .await
                .unwrap();
            assert!(!applied.replayed);
            assert!(
                replica
                    .version_metadata_by_identity(identity, &key(), first.version)
                    .unwrap()
                    .is_none()
            );
        }
        assert!(
            removed_replica
                .object_alias_registry(tenant_id, bucket_id, key().path())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            added_replica
                .object_alias_registry(tenant_id, bucket_id, key().path())
                .unwrap(),
            Some(expanded)
        );
    }

    #[tokio::test]
    async fn local_delete_applies_its_reference_effect_and_cursor_atomically() {
        let (_source_dir, source) = open(1).await;
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
        let first_version = source
            .version_metadata(&key(), first.version)
            .unwrap()
            .unwrap();
        let first_blob = first_version.blob.unwrap();
        assert_eq!(
            source
                .blob_reference_state(&first_blob)
                .unwrap()
                .unwrap()
                .ref_count,
            1
        );
        let (tenant_id, bucket_id) = source.resolve_bucket_ids("tenant", "bucket").unwrap();
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
        let before = source.db.latest_sequence_number();

        let coordinated = source
            .coordinate_local_retained_version_delete(&key(), first.version, governance, context)
            .await
            .unwrap();

        let mutation = coordinated.mutation.unwrap();
        assert_eq!(
            source
                .blob_reference_state(&first_blob)
                .unwrap()
                .unwrap()
                .ref_count,
            0
        );
        assert!(
            source
                .read_reference_proof(
                    mutation.stamp.source_id,
                    mutation.stamp.source_journal_position,
                )
                .unwrap()
                .is_none()
        );
        let journal = source.local_watch_status().unwrap();
        assert_eq!(
            source.reference_delta_cursor(journal.source_id).unwrap(),
            journal.tail
        );
        let batches = source
            .db
            .get_updates_since(before)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[tokio::test]
    async fn retained_delete_seals_sorted_aliases_and_wakes_every_logical_name() {
        let (_source_dir, source) = open(1).await;
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
        let identity = BucketIdentity {
            tenant_id: TenantId(tenant_id),
            bucket_id: BucketId(bucket_id),
        };
        let registry = ObjectAliasRegistry {
            format: OBJECT_ALIAS_REGISTRY_FORMAT,
            revision: 1,
            aliases: vec!["aliases/a".into(), "aliases/z".into()],
            program_commit_cursor: Some(1),
        };
        source
            .db
            .put_cf(
                source.cf(CF_OBJECT_ALIAS_REGISTRIES).unwrap(),
                identity.head_key(key().path()),
                registry.canonical_bytes().unwrap(),
            )
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
            .coordinate_retained_version_delete(&key(), first.version, governance, context)
            .await
            .unwrap();
        let mutation = coordinated.mutation.unwrap();
        assert_eq!(mutation.alias_paths, registry.aliases);
        let proof = source
            .read_reference_proof(
                mutation.stamp.source_id,
                mutation.stamp.source_journal_position,
            )
            .unwrap()
            .unwrap();
        let LocalChange::RetainedVersionDeleted(proof_change) = proof.change else {
            panic!("retained deletion proof changed kind")
        };
        assert_eq!(proof_change.exact_path, key().path());
        assert!(proof_change.accounting_transition.is_none());
        assert_eq!(proof_change.reference_deltas, mutation.reference_deltas);

        let changes = source
            .scan_local_changes(0, 20)
            .unwrap()
            .into_iter()
            .filter_map(|change| match change {
                LocalChange::RetainedVersionDeleted(change) => Some(change),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(changes.len(), 3);
        assert_eq!(
            changes
                .iter()
                .map(|change| change.offset)
                .collect::<Vec<_>>(),
            [
                mutation.stamp.source_journal_position,
                mutation.stamp.source_journal_position + 1,
                mutation.stamp.source_journal_position + 2,
            ]
        );
        assert!(changes[0].canonical_path.is_none());
        assert!(
            changes[1..]
                .iter()
                .all(|change| change.canonical_path.as_deref() == Some("ledger/entry"))
        );
        assert_eq!(
            changes
                .iter()
                .map(|change| change.exact_path.as_str())
                .collect::<Vec<_>>(),
            [key().path(), "aliases/a", "aliases/z"]
        );
        assert!(
            changes
                .iter()
                .all(|change| change.accounting_transition.is_none())
        );
        assert_eq!(changes[0].reference_deltas, mutation.reference_deltas);
        assert!(
            changes[1..]
                .iter()
                .all(|change| change.reference_deltas.is_empty())
        );
    }
}
