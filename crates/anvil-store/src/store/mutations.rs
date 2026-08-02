use super::*;

impl Store {
    pub async fn put(&self, request: PutRequest) -> Result<MutationReceipt, MutationError> {
        self.bulk_write(vec![BatchOperation::Put(request)])
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    pub async fn publish(&self, request: PublishRequest) -> Result<MutationReceipt, MutationError> {
        self.bulk_write(vec![BatchOperation::Publish(request)])
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    pub async fn delete(&self, request: DeleteRequest) -> Result<MutationReceipt, MutationError> {
        self.bulk_write(vec![BatchOperation::Delete(request)])
            .await
            .pop()
            .expect("one operation has one outcome")
            .result
    }

    /// Evaluates independent operations in request order and persists all
    /// successful outcomes with one physical RocksDB write. A failed
    /// precondition is an item result, not a reason to retry the whole bulk.
    pub async fn bulk_write(&self, operations: Vec<BatchOperation>) -> Vec<BatchOutcome> {
        let _policy_guard = self.policy_gate.read().await;
        let mut prepared = Vec::with_capacity(operations.len());
        let mut early = BTreeMap::new();
        let mut identity_cache =
            BTreeMap::<(String, String), Result<BucketIdentity, MutationError>>::new();
        for (index, operation) in operations.into_iter().enumerate() {
            let logical_key = match &operation {
                BatchOperation::Put(request) => &request.key,
                BatchOperation::Publish(request) => &request.key,
                BatchOperation::Delete(request) => &request.key,
            };
            let cache_key = (
                logical_key.tenant().to_owned(),
                logical_key.bucket().to_owned(),
            );
            let identity = identity_cache
                .entry(cache_key)
                .or_insert_with(|| {
                    self.resolve_bucket_identity(logical_key.tenant(), logical_key.bucket())
                })
                .clone();
            let result = match identity {
                Ok(identity) => self.prepare(operation, identity).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(operation) => prepared.push((index, operation)),
                Err(error) => {
                    early.insert(index, error);
                }
            }
        }

        let _guards = self
            .ordinary_locks
            .acquire(
                &prepared
                    .iter()
                    .map(|(_, operation)| object_path(operation.key()))
                    .collect::<Vec<_>>(),
            )
            .await;
        let _commit_guard = self.commit_lock.lock().await;
        let mut batch = WriteBatch::default();
        let now = match now_unix_millis() {
            Ok(now) => now,
            Err(error) => {
                return fail_prepared_operations(early, prepared, error);
            }
        };
        let mut receipt_status = match self.mutation_receipt_status() {
            Ok(status) => status,
            Err(error) => {
                return fail_prepared_operations(early, prepared, error);
            }
        };
        let initial_receipt_status = receipt_status;
        let pruned_receipts =
            match self.stage_expired_mutation_receipts(&mut batch, now, &mut receipt_status) {
                Ok(pruned) => pruned,
                Err(error) => {
                    return fail_prepared_operations(early, prepared, error);
                }
            };
        let mut pending_heads = BTreeMap::<Vec<u8>, Head>::new();
        let mut pending_versions = BTreeMap::<Vec<u8>, Version>::new();
        let mut pending_receipts = BTreeMap::<Vec<u8>, StoredReceipt>::new();
        let mut pending_blob_references = PendingBlobReferences::new();
        let mut pending_small_blobs = BTreeSet::<Vec<u8>>::new();
        let mut policy_cache = BTreeMap::<Vec<u8>, Result<BucketPolicy, MutationError>>::new();
        let mut versioning_cache =
            BTreeMap::<Vec<u8>, Result<ObjectVersioning, MutationError>>::new();
        let mut results = BTreeMap::<usize, Result<MutationReceipt, MutationError>>::new();
        let mut batch_high_watermark = None;
        let mut pending_invalidations = Vec::new();
        for (index, operation) in prepared {
            let outcome = self
                .evaluate_operation(
                    &operation,
                    &mut batch,
                    &mut pending_heads,
                    &mut pending_versions,
                    &mut pending_receipts,
                    &mut pending_blob_references,
                    &mut pending_small_blobs,
                    &mut policy_cache,
                    &mut versioning_cache,
                    &pruned_receipts,
                    &mut receipt_status,
                    now,
                )
                .await;
            if let Ok(receipt) = &outcome
                && !receipt.replayed
            {
                batch_high_watermark = Some(
                    batch_high_watermark.map_or(receipt.version, |current: VersionId| {
                        current.max(receipt.version)
                    }),
                );
                pending_invalidations.push((
                    operation.identity(),
                    operation.key().path().to_owned(),
                    receipt.version,
                    receipt.deleted,
                ));
            }
            results.insert(index, outcome);
        }

        let persistence = (|| {
            if receipt_status != initial_receipt_status {
                self.stage_mutation_receipt_status(&mut batch, receipt_status)?;
            }
            self.stage_local_invalidations(&mut batch, &pending_invalidations)?;
            if let Some(high_watermark) = batch_high_watermark {
                batch.put_cf(
                    self.cf(CF_METADATA)?,
                    VERSION_HIGH_WATERMARK_KEY,
                    serde_json::to_vec(&high_watermark).map_err(storage_error)?,
                );
            }
            if batch.is_empty() {
                return Ok(());
            }
            let mut options = WriteOptions::default();
            options.set_sync(self.sync_writes);
            self.db.write_opt(batch, &options).map_err(storage_error)
        })();
        match persistence {
            Ok(()) => {
                if !pending_invalidations.is_empty() {
                    self.notify_local_invalidations();
                }
            }
            Err(error) => {
                let message = error.to_string();
                for result in results.values_mut() {
                    if result.is_ok() {
                        *result = Err(MutationError::Storage(message.clone()));
                    }
                }
            }
        }
        results.extend(early.into_iter().map(|(index, error)| (index, Err(error))));
        results
            .into_iter()
            .map(|(index, result)| BatchOutcome { index, result })
            .collect()
    }

    pub(crate) fn stage_local_invalidations(
        &self,
        batch: &mut WriteBatch,
        changes: &[(BucketIdentity, String, VersionId, bool)],
    ) -> Result<(), MutationError> {
        if changes.is_empty() {
            return Ok(());
        }

        let journal = self.cf(CF_LOCAL_INVALIDATIONS)?;
        let metadata = self.cf(CF_METADATA)?;
        let mut status = self
            .local_watch_status()
            .map_err(|error| MutationError::Storage(error.to_string()))?;
        let old_tail = status.tail;
        let first_old_key = invalidation_key(status.retention_floor.saturating_add(1));
        let mut old_entries = self.db.iterator_cf(
            journal,
            IteratorMode::From(&first_old_key, Direction::Forward),
        );
        let mut appended = VecDeque::new();
        for (identity, exact_path, version, deleted) in changes {
            status.tail = status.tail.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation offset is exhausted".into())
            })?;
            let change = LocalChange::object_head(
                status.tail,
                identity.tenant_id.0,
                identity.bucket_id.0,
                exact_path.clone(),
                *version,
                *deleted,
            );
            let encoded = encode_local_change(&change).map_err(storage_error)?;
            status.retained_entries = status.retained_entries.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation entry count is exhausted".into())
            })?;
            status.retained_bytes = status
                .retained_bytes
                .checked_add(invalidation_record_bytes(encoded.len()))
                .ok_or_else(|| {
                    MutationError::Storage("local invalidation byte count is exhausted".into())
                })?;
            appended.push_back((status.tail, encoded));
        }

        while status.retained_entries > self.watch_retention.max_entries
            || status.retained_bytes > self.watch_retention.max_bytes
        {
            let pruned = status.retention_floor.checked_add(1).ok_or_else(|| {
                MutationError::Storage("local invalidation retention floor is exhausted".into())
            })?;
            let encoded = if pruned <= old_tail {
                let (stored_key, encoded) = old_entries
                    .next()
                    .ok_or_else(|| {
                        MutationError::Storage(format!(
                            "retained local invalidation offset {pruned} is missing"
                        ))
                    })?
                    .map_err(storage_error)?;
                if offset_from_key(&stored_key) != Some(pruned) {
                    return Err(MutationError::Storage(format!(
                        "retained local invalidation offset {pruned} is missing"
                    )));
                }
                encoded.to_vec()
            } else {
                let (offset, encoded) = appended.pop_front().ok_or_else(|| {
                    MutationError::Storage(
                        "local invalidation retention accounting is inconsistent".into(),
                    )
                })?;
                if offset != pruned {
                    return Err(MutationError::Storage(
                        "local invalidation retention offsets are inconsistent".into(),
                    ));
                }
                encoded
            };
            batch.delete_cf(journal, invalidation_key(pruned));
            status.retention_floor = pruned;
            status.retained_entries -= 1;
            status.retained_bytes = status
                .retained_bytes
                .checked_sub(invalidation_record_bytes(encoded.len()))
                .ok_or_else(|| {
                    MutationError::Storage(
                        "local invalidation byte accounting is inconsistent".into(),
                    )
                })?;
        }
        for (offset, encoded) in appended {
            batch.put_cf(journal, invalidation_key(offset), encoded);
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
        Ok(())
    }

    pub(crate) fn notify_local_invalidations(&self) {
        self.watch_notify.send_replace(());
    }

    pub(super) fn enforce_local_watch_retention(&self) -> Result<(), WatchError> {
        let journal = self
            .cf(CF_LOCAL_INVALIDATIONS)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let metadata = self
            .cf(CF_METADATA)
            .map_err(|error| WatchError::Storage(error.to_string()))?;
        let mut status = self.local_watch_status()?;
        if status.retained_entries <= self.watch_retention.max_entries
            && status.retained_bytes <= self.watch_retention.max_bytes
        {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        while status.retained_entries > self.watch_retention.max_entries
            || status.retained_bytes > self.watch_retention.max_bytes
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
            batch.delete_cf(journal, invalidation_key(offset));
            status.retention_floor = offset;
            status.retained_entries -= 1;
            status.retained_bytes = status
                .retained_bytes
                .checked_sub(invalidation_record_bytes(encoded.len()))
                .ok_or_else(|| {
                    WatchError::Storage("local invalidation byte accounting is inconsistent".into())
                })?;
        }
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

    pub(super) async fn prepare(
        &self,
        operation: BatchOperation,
        identity: BucketIdentity,
    ) -> Result<PreparedOperation, MutationError> {
        match operation {
            BatchOperation::Put(mut request) => {
                validate_command_id(request.command_id.as_deref())?;
                require_local_durability(request.durability)?;
                let bytes = std::mem::take(&mut request.bytes);
                let payload = if bytes.len() <= SMALL_BLOB_MAX_BYTES {
                    let reference = blob_reference_for_bytes(&bytes);
                    PreparedPayload::Small { reference, bytes }
                } else {
                    PreparedPayload::Large(self.blobs.put(&bytes).await.map_err(storage_error)?)
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
                require_local_durability(request.durability)?;
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
            BatchOperation::Delete(request) => {
                validate_command_id(request.command_id.as_deref())?;
                require_local_durability(request.durability)?;
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
        let read = |key: &[u8]| {
            self.db
                .get_cf(metadata, key)
                .map_err(storage_error)?
                .ok_or_else(|| {
                    MutationError::Storage("mutation receipt metadata is missing".into())
                })
                .and_then(|encoded| decode_offset(&encoded))
        };
        Ok(MutationReceiptStatus {
            entries: read(MUTATION_RECEIPT_COUNT_KEY)?,
            bytes: read(MUTATION_RECEIPT_BYTES_KEY)?,
        })
    }

    fn stage_expired_mutation_receipts(
        &self,
        batch: &mut WriteBatch,
        now_unix_millis: u64,
        status: &mut MutationReceiptStatus,
    ) -> Result<BTreeSet<Vec<u8>>, MutationError> {
        let receipts = self.cf(CF_RECEIPTS)?;
        let mut pruned = BTreeSet::new();
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
            let receipt =
                serde_json::from_slice::<StoredReceipt>(&encoded).map_err(storage_error)?;
            if receipt.expires_at_unix_millis != expires_at {
                return Err(MutationError::Storage(
                    "mutation receipt expiry index disagrees with its receipt".into(),
                ));
            }
            let logical_bytes =
                mutation_receipt_logical_bytes(primary_key.len(), encoded.len(), index_key.len());
            status.entries = status.entries.checked_sub(1).ok_or_else(|| {
                MutationError::Storage("mutation receipt count is inconsistent".into())
            })?;
            status.bytes = status.bytes.checked_sub(logical_bytes).ok_or_else(|| {
                MutationError::Storage("mutation receipt byte accounting is inconsistent".into())
            })?;
            batch.delete_cf(receipts, &primary_key);
            batch.delete_cf(receipts, &index_key);
            pruned.insert(primary_key);
        }
        Ok(pruned)
    }

    #[allow(clippy::too_many_arguments)]
    fn stage_mutation_receipt(
        &self,
        batch: &mut WriteBatch,
        primary_key: Option<Vec<u8>>,
        fingerprint: [u8; 32],
        version: VersionId,
        deleted: bool,
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
        };
        let encoded = serde_json::to_vec(&stored).map_err(storage_error)?;
        let expiry_key = receipt_expiry_key(expires_at_unix_millis, &primary_key)?;
        let logical_bytes =
            mutation_receipt_logical_bytes(primary_key.len(), encoded.len(), expiry_key.len());
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
        Ok(expires_at_unix_millis)
    }

    fn stage_mutation_receipt_status(
        &self,
        batch: &mut WriteBatch,
        status: MutationReceiptStatus,
    ) -> Result<(), MutationError> {
        let metadata = self.cf(CF_METADATA)?;
        batch.put_cf(
            metadata,
            MUTATION_RECEIPT_COUNT_KEY,
            status.entries.to_be_bytes(),
        );
        batch.put_cf(
            metadata,
            MUTATION_RECEIPT_BYTES_KEY,
            status.bytes.to_be_bytes(),
        );
        Ok(())
    }

    async fn evaluate_operation(
        &self,
        operation: &PreparedOperation,
        batch: &mut WriteBatch,
        pending_heads: &mut BTreeMap<Vec<u8>, Head>,
        pending_versions: &mut BTreeMap<Vec<u8>, Version>,
        pending_receipts: &mut BTreeMap<Vec<u8>, StoredReceipt>,
        pending_blob_references: &mut PendingBlobReferences,
        pending_small_blobs: &mut BTreeSet<Vec<u8>>,
        policy_cache: &mut BTreeMap<Vec<u8>, Result<BucketPolicy, MutationError>>,
        versioning_cache: &mut BTreeMap<Vec<u8>, Result<ObjectVersioning, MutationError>>,
        pruned_receipts: &BTreeSet<Vec<u8>>,
        receipt_status: &mut MutationReceiptStatus,
        now_unix_millis: u64,
    ) -> Result<MutationReceipt, MutationError> {
        let key = operation.key();
        let encoded_key = operation.encoded_head_key();
        let receipt_key = operation
            .command_id()
            .map(|command_id| receipt_key(operation.identity(), command_id));
        if let Some(receipt_key) = receipt_key.as_ref() {
            let existing = match pending_receipts.get(receipt_key) {
                Some(receipt) => Some(receipt.clone()),
                None if pruned_receipts.contains(receipt_key) => None,
                None => self.read_json(CF_RECEIPTS, receipt_key)?,
            };
            if let Some(existing) = existing {
                if existing.expires_at_unix_millis <= now_unix_millis {
                    return Err(MutationError::Storage(
                        "expired mutation receipt escaped pruning".into(),
                    ));
                }
                if existing.fingerprint != operation.fingerprint() {
                    return Err(MutationError::IdempotencyConflict);
                }
                return Ok(MutationReceipt {
                    command_id: operation.command_id().map(str::to_owned),
                    fingerprint: existing.fingerprint,
                    version: existing.version,
                    deleted: existing.deleted,
                    replayed: true,
                    replay_guarantee_expires_at_unix_millis: existing.expires_at_unix_millis,
                });
            }
        }

        let current = match pending_heads.get(&encoded_key) {
            Some(head) => Some(head.clone()),
            None => self.head_by_storage_key(&encoded_key)?,
        };
        let current_version = match current.as_ref() {
            Some(head) => match pending_versions.get(&encoded_key) {
                Some(version) => Some(version.clone()),
                None => Some(
                    self.version_metadata_by_identity(operation.identity(), key, head.version)?
                        .ok_or_else(|| {
                            MutationError::Storage("head references a missing version".into())
                        })?,
                ),
            },
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
            Some(PutMode::PutImmutable) => {
                // Handled below: publish once or return an identical-content
                // semantic replay without advancing the path version.
            }
            Some(_) | None if immutable_path => {
                return Err(MutationError::Immutable);
            }
            Some(_) | None => {}
        }
        if matches!(operation.put_mode(), Some(PutMode::PutImmutable)) {
            if let Some(current) = current.as_ref() {
                let existing = current_version.as_ref().ok_or_else(|| {
                    MutationError::Storage("head references a missing version".into())
                })?;
                let requested_payload = match operation {
                    PreparedOperation::Put { payload, .. } => payload.reference().clone(),
                    PreparedOperation::Publish { request, .. } => request.blob.clone(),
                    PreparedOperation::Delete { .. } => unreachable!(),
                };
                let requested_content_type = match operation {
                    PreparedOperation::Put { request, .. } => request.content_type.as_ref(),
                    PreparedOperation::Publish { request, .. } => request.content_type.as_ref(),
                    PreparedOperation::Delete { .. } => unreachable!(),
                };
                if !current.deleted
                    && version_blob_reference(existing)?.as_ref() == Some(&requested_payload)
                    && existing.content_type.as_ref() == requested_content_type
                {
                    let fingerprint = operation.fingerprint();
                    let expires_at = self.stage_mutation_receipt(
                        batch,
                        receipt_key,
                        fingerprint,
                        current.version,
                        false,
                        now_unix_millis,
                        receipt_status,
                        pending_receipts,
                    )?;
                    return Ok(MutationReceipt {
                        command_id: operation.command_id().map(str::to_owned),
                        fingerprint,
                        version: current.version,
                        deleted: false,
                        replayed: true,
                        replay_guarantee_expires_at_unix_millis: expires_at,
                    });
                }
                return Err(MutationError::Immutable);
            }
        }
        check_precondition(operation.precondition(), current.as_ref())?;

        let id = self.clock.next().map_err(storage_error)?;
        let deleted = matches!(operation, PreparedOperation::Delete { .. });
        let new_blob = match operation {
            PreparedOperation::Put { payload, .. } => Some(payload.reference().clone()),
            PreparedOperation::Publish { request, .. } => Some(request.blob.clone()),
            PreparedOperation::Delete { .. } => None,
        };
        if let PreparedOperation::Put { payload, .. } = operation
            && payload.small_bytes().is_none()
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
                PreparedOperation::Delete { .. } => None,
            },
            deleted,
            committed_at_unix_millis: now_unix_millis,
        };
        let head = Head {
            version: id,
            deleted,
        };
        let encoded_version = serde_json::to_vec(&version).map_err(storage_error)?;
        let encoded_head = serde_json::to_vec(&head).map_err(storage_error)?;
        let versions = self.cf(CF_VERSIONS)?;
        let heads = self.cf(CF_HEADS)?;
        let encoded_version_key = version_key(operation.identity(), key, id);
        let fingerprint = operation.fingerprint();
        let old_blob = current_version
            .as_ref()
            .map(version_blob_reference)
            .transpose()?
            .flatten();
        let mut blob_reference_updates = Vec::with_capacity(2);
        let references_changed = old_blob.as_ref() != new_blob.as_ref();
        if versioning == ObjectVersioning::Unversioned && references_changed {
            if let Some(reference) = old_blob.as_ref() {
                blob_reference_updates.push(self.prepare_blob_reference_retirement(
                    reference,
                    pending_blob_references,
                    now_unix_millis,
                )?);
            }
        }
        let small_blob_value = match operation {
            PreparedOperation::Put { payload, .. } => match payload.small_bytes() {
                Some(bytes) => {
                    self.prepare_small_blob_value(payload.reference(), bytes, pending_small_blobs)?
                }
                None => None,
            },
            PreparedOperation::Publish { .. } | PreparedOperation::Delete { .. } => None,
        };
        if let Some(reference) = new_blob.as_ref()
            && (versioning == ObjectVersioning::Enabled || references_changed)
        {
            let update = match operation {
                PreparedOperation::Put { .. } => self.prepare_materialized_blob_publication(
                    reference,
                    pending_blob_references,
                    now_unix_millis,
                )?,
                PreparedOperation::Publish { .. } => self.prepare_blob_reference_publication(
                    reference,
                    pending_blob_references,
                    now_unix_millis,
                )?,
                PreparedOperation::Delete { .. } => unreachable!(),
            };
            blob_reference_updates.push(update);
        }
        let expires_at = self.stage_mutation_receipt(
            batch,
            receipt_key,
            fingerprint,
            id,
            deleted,
            now_unix_millis,
            receipt_status,
            pending_receipts,
        )?;
        if let Some((key, bytes)) = small_blob_value {
            batch.put_cf(self.cf(CF_SMALL_BLOBS)?, &key, bytes);
            pending_small_blobs.insert(key);
        }
        for (key, state) in blob_reference_updates {
            self.stage_blob_reference_update(batch, pending_blob_references, key, state)?;
        }
        if versioning == ObjectVersioning::Unversioned
            && let Some(previous) = current_version.as_ref()
        {
            batch.delete_cf(
                versions,
                version_key(operation.identity(), key, previous.id),
            );
        }
        batch.put_cf(versions, encoded_version_key, encoded_version);
        batch.put_cf(heads, &encoded_key, encoded_head);
        pending_heads.insert(encoded_key.clone(), head);
        pending_versions.insert(encoded_key, version);
        Ok(MutationReceipt {
            command_id: operation.command_id().map(str::to_owned),
            fingerprint,
            version: id,
            deleted,
            replayed: false,
            replay_guarantee_expires_at_unix_millis: expires_at,
        })
    }
}
