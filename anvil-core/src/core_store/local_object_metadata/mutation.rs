use super::super::local_stream_control::control_record_proto::encode_boundary_value_row;
use super::super::local_tx_rows::OwnedCoreMetaBatchOp;
use super::*;
use std::collections::{BTreeMap, BTreeSet};

const VERSION_SCAN_PAGE_ROWS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectMetadataProjectionMutation {
    Upsert,
    DeleteVersion,
}

pub(crate) struct ObjectMetadataMutationGuard {
    _guard: super::super::CoreStoreLock,
}

#[derive(Debug)]
pub(crate) struct PreparedObjectMetadataProjection {
    pub(crate) root_generation: u64,
    pub(crate) operations: Vec<CoreMutationOperation>,
}

#[derive(Debug, Clone)]
pub(crate) struct ObjectMetadataPreconditionSnapshot {
    pub(crate) object: Option<Object>,
    pub(crate) precondition: CoreMutationPrecondition,
}

impl CoreStore {
    pub(crate) fn object_metadata_precondition_snapshot(
        &self,
        bucket: &Bucket,
        object_key: &str,
        explicit_transaction: Option<&CoreTransaction>,
    ) -> Result<ObjectMetadataPreconditionSnapshot> {
        let tuple_key = object_current_key(bucket, object_key);
        let payload = self.object_metadata_payload_visible_to_transaction(
            CF_OBJECT_HEADS,
            TABLE_OBJECT_HEAD_ROW,
            &tuple_key,
            explicit_transaction,
        )?;
        let decoded = payload
            .as_deref()
            .map(decode_object_metadata_row)
            .transpose()?;
        if let Some(object) = decoded.as_ref() {
            validate_object_scope(bucket, object)?;
            if object.key != object_key {
                bail!("CoreStore object metadata current row key mismatch");
            }
        }
        let object = decoded.filter(|object| object.deleted_at.is_none());
        let expected_payload_hash = payload
            .as_ref()
            .map(|payload| core_meta_payload_digest(TABLE_OBJECT_HEAD_ROW, payload));
        Ok(ObjectMetadataPreconditionSnapshot {
            object,
            precondition: CoreMutationPrecondition::CoreMetaRow {
                cf: CF_OBJECT_HEADS.to_string(),
                table_id: TABLE_OBJECT_HEAD_ROW,
                tuple_key,
                require_absent: expected_payload_hash.is_none(),
                require_present: expected_payload_hash.is_some(),
                expected_payload_hash,
            },
        })
    }

    pub(crate) async fn acquire_object_metadata_mutation_lock(
        &self,
        bucket: &Bucket,
    ) -> Result<ObjectMetadataMutationGuard> {
        Ok(ObjectMetadataMutationGuard {
            _guard: self
                .acquire_named_lock(
                    "object-metadata-bucket",
                    &object_metadata_bucket_lock_id(bucket),
                )
                .await?,
        })
    }

    pub(crate) async fn prepare_object_metadata_projection(
        &self,
        bucket: &Bucket,
        object: &Object,
        mutation: ObjectMetadataProjectionMutation,
        scope_partition: &str,
        transaction_id: &str,
        explicit_transaction: Option<&CoreTransaction>,
    ) -> Result<PreparedObjectMetadataProjection> {
        validate_object_scope(bucket, object)?;
        if scope_partition != object_metadata_root_anchor_key(bucket.tenant_id, bucket.id) {
            bail!("object metadata mutation scope does not match its CoreMeta root");
        }
        let root_generation = match explicit_transaction {
            Some(transaction) => {
                if transaction.transaction_id != transaction_id
                    || transaction.root_anchor_key != scope_partition
                    || transaction.root_key_hash
                        != object_metadata_root_key_hash(bucket.tenant_id, bucket.id)
                {
                    bail!("object metadata explicit transaction scope mismatch");
                }
                self.infer_explicit_transaction_commit_root_generation(transaction)
                    .await?
            }
            None => {
                self.implicit_root_generation_unlocked(transaction_id, scope_partition, None)
                    .await?
            }
        };
        let operations = match mutation {
            ObjectMetadataProjectionMutation::Upsert => {
                self.prepare_object_metadata_upsert_operations(
                    bucket,
                    object,
                    scope_partition,
                    transaction_id,
                    root_generation,
                    explicit_transaction,
                )
                .await?
            }
            ObjectMetadataProjectionMutation::DeleteVersion => {
                self.prepare_object_metadata_delete_version_operations(
                    bucket,
                    object,
                    scope_partition,
                    transaction_id,
                    root_generation,
                    explicit_transaction,
                )
                .await?
            }
        };
        Ok(PreparedObjectMetadataProjection {
            root_generation,
            operations,
        })
    }

    pub(crate) async fn materialize_object_metadata_ancillary_projections(
        &self,
        bucket: &Bucket,
        object: &Object,
        mutation: ObjectMetadataProjectionMutation,
    ) -> Result<()> {
        let transaction_id = object.mutation_id.to_string();
        let projection = [(bucket, object, mutation)];
        self.materialize_object_metadata_ancillary_projection_batch(&transaction_id, &projection)
            .await
    }

    pub(crate) async fn materialize_object_metadata_ancillary_projection_batch(
        &self,
        transaction_id: &str,
        projections: &[(&Bucket, &Object, ObjectMetadataProjectionMutation)],
    ) -> Result<()> {
        let mut prepared = PreparedAncillaryProjectionBatch::default();
        for (bucket, object, mutation) in projections {
            validate_object_scope(bucket, object)?;
            let (payload_ops, payload_publications) = match mutation {
                ObjectMetadataProjectionMutation::Upsert => {
                    self.payload_reference_put_ops_for_object(bucket, object, transaction_id)
                        .await?
                }
                ObjectMetadataProjectionMutation::DeleteVersion => {
                    self.payload_reference_delete_ops_for_object(bucket, object, transaction_id)
                        .await?
                }
            };
            prepared.extend(payload_ops, payload_publications)?;

            if *mutation == ObjectMetadataProjectionMutation::Upsert {
                let (boundary_ops, boundary_publications) = self
                    .prepare_object_boundary_value_projections(bucket, object)
                    .await?;
                prepared.extend(boundary_ops, boundary_publications)?;
            }
        }

        let (owned_ops, publications) = prepared.finish()?;
        if owned_ops.is_empty() {
            return Ok(());
        }
        let ops = borrow_owned_coremeta_batch_ops(&owned_ops);
        self.commit_coremeta_root_groups(transaction_id, &ops, &publications)
            .await?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn put_object_metadata(&self, bucket: &Bucket, object: &Object) -> Result<()> {
        let _guard = self.acquire_object_metadata_mutation_lock(bucket).await?;
        let transaction_id = format!("object-metadata-projection:{}", object.mutation_id);
        let scope_partition = object_metadata_root_anchor_key(bucket.tenant_id, bucket.id);
        let prepared = self
            .prepare_object_metadata_projection(
                bucket,
                object,
                ObjectMetadataProjectionMutation::Upsert,
                &scope_partition,
                &transaction_id,
                None,
            )
            .await?;
        let receipt = self
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id,
                root_publications: vec![object_metadata_root_publication(&scope_partition)],
                scope_partition,
                committed_by_principal: format!(
                    "object-metadata-projection:{}:{}",
                    bucket.tenant_id, bucket.id
                ),
                preconditions: Vec::new(),
                operations: prepared.operations,
            })
            .await?;
        require_committed_projection_receipt(&receipt)?;
        self.materialize_object_metadata_ancillary_projections(
            bucket,
            object,
            ObjectMetadataProjectionMutation::Upsert,
        )
        .await
    }

    #[cfg(test)]
    pub async fn delete_object_version_metadata(
        &self,
        bucket: &Bucket,
        object: &Object,
    ) -> Result<()> {
        let _guard = self.acquire_object_metadata_mutation_lock(bucket).await?;
        let transaction_id = format!("object-metadata-projection:{}", object.mutation_id);
        let scope_partition = object_metadata_root_anchor_key(bucket.tenant_id, bucket.id);
        let prepared = self
            .prepare_object_metadata_projection(
                bucket,
                object,
                ObjectMetadataProjectionMutation::DeleteVersion,
                &scope_partition,
                &transaction_id,
                None,
            )
            .await?;
        let receipt = self
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id,
                root_publications: vec![object_metadata_root_publication(&scope_partition)],
                scope_partition,
                committed_by_principal: format!(
                    "object-metadata-projection:{}:{}",
                    bucket.tenant_id, bucket.id
                ),
                preconditions: Vec::new(),
                operations: prepared.operations,
            })
            .await?;
        require_committed_projection_receipt(&receipt)?;
        self.materialize_object_metadata_ancillary_projections(
            bucket,
            object,
            ObjectMetadataProjectionMutation::DeleteVersion,
        )
        .await
    }

    pub async fn next_object_metadata_id(&self, bucket: &Bucket) -> Result<i64> {
        self.next_object_metadata_id_in_transaction(bucket, None)
            .await
    }

    pub(crate) async fn next_object_metadata_id_in_transaction(
        &self,
        bucket: &Bucket,
        transaction_id: Option<&str>,
    ) -> Result<i64> {
        let transaction = match transaction_id {
            Some(transaction_id) => Some(
                self.read_transaction(transaction_id)
                    .await?
                    .ok_or_else(|| anyhow!("TransactionNotFound"))?,
            ),
            None => None,
        };
        let counter_key = object_id_counter_key(bucket);
        let projected_max_id = match self.object_metadata_payload_visible_to_transaction(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &counter_key,
            transaction.as_ref(),
        )? {
            Some(bytes) => decode_object_metadata_counter_for_bucket(&bytes, bucket)?.max_id,
            None => 0,
        };
        let metadata_stream_id = format!(
            "object_metadata:tenant:{}:bucket:{}",
            bucket.tenant_id, bucket.id
        );
        let stream_sequence = match transaction.as_ref() {
            Some(transaction) => {
                self.stream_head_visible_to_transaction_unlocked(
                    &metadata_stream_id,
                    Some(transaction),
                )?
                .0
            }
            None => self.stream_head_sequence(&metadata_stream_id).await?,
        };
        let stream_max_id = i64::try_from(stream_sequence)
            .context("object metadata stream sequence exceeds i64")?;
        projected_max_id
            .max(stream_max_id)
            .checked_add(1)
            .ok_or_else(|| anyhow!("object id overflow"))
    }

    async fn prepare_object_metadata_upsert_operations(
        &self,
        bucket: &Bucket,
        object: &Object,
        scope_partition: &str,
        transaction_id: &str,
        root_generation: u64,
        explicit_transaction: Option<&CoreTransaction>,
    ) -> Result<Vec<CoreMutationOperation>> {
        let payload = encode_object_metadata_row_at_generation_for_transaction(
            object,
            root_generation,
            transaction_id,
        )?;
        let counter_payload = explicit_transaction
            .is_none()
            .then(|| {
                self.object_id_counter_payload_at_generation(
                    bucket,
                    object.id,
                    root_generation,
                    transaction_id,
                    explicit_transaction,
                )
            })
            .transpose()?;
        let mut operations = vec![
            put_operation(
                scope_partition,
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                object_current_key(bucket, &object.key),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_key(bucket, &object.key, object.version_id),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_id_key(bucket, object.version_id),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                object_key_catalog_key(bucket, object),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_page_key_for_object(bucket, object, root_generation),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_catalog_key(bucket, object, root_generation),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                object_current_history_key(bucket, &object.key, root_generation, object.version_id),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_history_key(bucket, &object.key, object.version_id, root_generation),
                payload.clone(),
            ),
        ];
        if let Some(counter_payload) = counter_payload {
            operations.push(put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_id_counter_key(bucket),
                counter_payload,
            ));
        }
        let current_page_key = object_current_page_key_for_object(bucket, object);
        if object.deleted_at.is_some() {
            operations.push(delete_operation(
                scope_partition,
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                current_page_key,
            ));
        } else {
            operations.push(put_operation(
                scope_partition,
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                current_page_key,
                payload,
            ));
        }
        Ok(operations)
    }

    async fn prepare_object_metadata_delete_version_operations(
        &self,
        bucket: &Bucket,
        deletion: &Object,
        scope_partition: &str,
        transaction_id: &str,
        root_generation: u64,
        explicit_transaction: Option<&CoreTransaction>,
    ) -> Result<Vec<CoreMutationOperation>> {
        if deletion.deleted_at.is_none() {
            bail!("object version deletion projection requires deleted_at");
        }
        let object_key = deletion.key.as_str();
        let version_id = deletion.version_id;
        let current_key = object_current_key(bucket, object_key);
        let version_key = object_version_key(bucket, object_key, version_id);
        let current = self
            .object_metadata_payload_visible_to_transaction(
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                &current_key,
                explicit_transaction,
            )?
            .map(|payload| decode_object_metadata_row(&payload))
            .transpose()?;
        let original = self
            .object_metadata_payload_visible_to_transaction(
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                &version_key,
                explicit_transaction,
            )?
            .map(|payload| decode_object_metadata_row_with_common(&payload))
            .transpose()?
            .ok_or_else(|| anyhow!("CoreStore object version metadata row missing"))?;
        validate_object_scope(bucket, &original.object)?;
        if original.object.key != object_key || original.object.version_id != version_id {
            bail!("CoreStore object version deletion row key mismatch");
        }
        let deleted_is_current = current
            .as_ref()
            .is_some_and(|object| object.key == object_key && object.version_id == version_id);
        let replacement = if deleted_is_current {
            self.latest_object_version_for_key_after_delete_in_transaction(
                bucket,
                object_key,
                version_id,
                explicit_transaction,
            )?
        } else {
            None
        };
        let mut tombstone = deletion.clone();
        tombstone.record_hash = format!("sha256:{}", sha256_hex(tombstone.mutation_id.as_bytes()));
        let tombstone_payload =
            encode_object_metadata_row_at_generation_with_delete_marker_for_transaction(
                &tombstone,
                root_generation,
                false,
                transaction_id,
            )?;
        let replacement_payload = replacement
            .as_ref()
            .map(|object| {
                encode_object_metadata_row_at_generation_for_transaction(
                    object,
                    root_generation,
                    transaction_id,
                )
            })
            .transpose()?;
        let counter_payload = explicit_transaction
            .is_none()
            .then(|| {
                self.object_id_counter_payload_at_generation(
                    bucket,
                    deletion.id,
                    root_generation,
                    transaction_id,
                    explicit_transaction,
                )
            })
            .transpose()?;
        let mut operations = vec![
            delete_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                version_key,
            ),
            delete_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_id_key(bucket, version_id),
            ),
            delete_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_page_key_for_object(
                    bucket,
                    &original.object,
                    original.root_generation,
                ),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_history_key(bucket, object_key, version_id, root_generation),
                tombstone_payload.clone(),
            ),
        ];
        if let Some(counter_payload) = counter_payload {
            operations.push(put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_id_counter_key(bucket),
                counter_payload,
            ));
        }
        if deleted_is_current {
            match (replacement.as_ref(), replacement_payload.as_ref()) {
                (Some(replacement), Some(replacement_payload)) => {
                    operations.extend([
                        put_operation(
                            scope_partition,
                            CF_OBJECT_HEADS,
                            TABLE_OBJECT_HEAD_ROW,
                            current_key,
                            replacement_payload.clone(),
                        ),
                        if replacement.deleted_at.is_some() {
                            delete_operation(
                                scope_partition,
                                CF_OBJECT_HEADS,
                                TABLE_OBJECT_HEAD_ROW,
                                object_current_page_key_for_object(bucket, replacement),
                            )
                        } else {
                            put_operation(
                                scope_partition,
                                CF_OBJECT_HEADS,
                                TABLE_OBJECT_HEAD_ROW,
                                object_current_page_key_for_object(bucket, replacement),
                                replacement_payload.clone(),
                            )
                        },
                        put_operation(
                            scope_partition,
                            CF_OBJECT_HEADS,
                            TABLE_OBJECT_HEAD_ROW,
                            object_current_history_key(
                                bucket,
                                object_key,
                                root_generation,
                                replacement.version_id,
                            ),
                            replacement_payload.clone(),
                        ),
                    ]);
                }
                (None, None) => {
                    operations.extend([
                        delete_operation(
                            scope_partition,
                            CF_OBJECT_HEADS,
                            TABLE_OBJECT_HEAD_ROW,
                            current_key,
                        ),
                        delete_operation(
                            scope_partition,
                            CF_OBJECT_HEADS,
                            TABLE_OBJECT_HEAD_ROW,
                            object_current_page_key_for_object(bucket, &original.object),
                        ),
                        put_operation(
                            scope_partition,
                            CF_OBJECT_HEADS,
                            TABLE_OBJECT_HEAD_ROW,
                            object_current_history_key(
                                bucket,
                                object_key,
                                root_generation,
                                version_id,
                            ),
                            tombstone_payload,
                        ),
                    ]);
                }
                _ => unreachable!("replacement object and payload are created together"),
            }
        }
        Ok(operations)
    }

    fn object_id_counter_payload_at_generation(
        &self,
        bucket: &Bucket,
        candidate_id: i64,
        root_generation: u64,
        transaction_id: &str,
        explicit_transaction: Option<&CoreTransaction>,
    ) -> Result<Vec<u8>> {
        if candidate_id <= 0 {
            bail!("object metadata id must be positive");
        }
        let counter_key = object_id_counter_key(bucket);
        let current_max = match self.object_metadata_payload_visible_to_transaction(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &counter_key,
            explicit_transaction,
        )? {
            Some(bytes) => decode_object_metadata_counter_for_bucket(&bytes, bucket)?.max_id,
            None => i64::try_from(root_generation.saturating_sub(1))
                .context("object metadata root generation exceeds i64")?,
        };
        let effective_max = candidate_id.max(current_max);
        encode_object_metadata_counter_at_generation(
            bucket,
            effective_max,
            root_generation,
            transaction_id,
        )
    }

    pub(super) async fn current_object_metadata_root_generation(
        &self,
        bucket: &Bucket,
    ) -> Result<u64> {
        let counter_key = object_id_counter_key(bucket);
        let Some(payload) = self.read_coremeta_row(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &counter_key,
        )?
        else {
            return Ok(0);
        };
        let counter = decode_object_metadata_counter_for_bucket(&payload, bucket)?;
        Ok(counter
            .common
            .expect("counter decoder requires CoreMeta common")
            .root_generation)
    }

    fn object_metadata_payload_visible_to_transaction(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        transaction: Option<&CoreTransaction>,
    ) -> Result<Option<Vec<u8>>> {
        match transaction {
            Some(transaction) => self.coremeta_payload_visible_to_transaction_unlocked(
                cf,
                table_id,
                tuple_key,
                transaction,
            ),
            None => self.read_coremeta_row(canonical_coremeta_cf_name(cf)?, table_id, tuple_key),
        }
    }

    fn latest_object_version_for_key_after_delete_in_transaction(
        &self,
        bucket: &Bucket,
        object_key: &str,
        deleted_version_id: uuid::Uuid,
        transaction: Option<&CoreTransaction>,
    ) -> Result<Option<Object>> {
        let Some(transaction) = transaction else {
            return self.latest_object_version_for_key_after_delete(
                bucket,
                object_key,
                deleted_version_id,
            );
        };
        let prefix = object_version_page_prefix(bucket, object_key);
        let mut overlay = BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } if cf == CF_OBJECT_VERSIONS
                    && *table_id == TABLE_OBJECT_VERSION_META_ROW
                    && tuple_key.starts_with(&prefix) =>
                {
                    overlay.insert(tuple_key.clone(), Some(payload.clone()));
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } if cf == CF_OBJECT_VERSIONS
                    && *table_id == TABLE_OBJECT_VERSION_META_ROW
                    && tuple_key.starts_with(&prefix) =>
                {
                    overlay.insert(tuple_key.clone(), None);
                }
                _ => {}
            }
        }
        let mut best = overlay
            .iter()
            .filter_map(|(key, payload)| payload.as_ref().map(|payload| (key.clone(), payload)))
            .map(|(key, payload)| Ok((key, decode_object_metadata_row_with_common(payload)?)))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|(_, decoded)| decoded.object.version_id != deleted_version_id)
            .min_by(|left, right| left.0.cmp(&right.0));
        let mut after = None;
        loop {
            let rows = self.scan_coremeta_prefix_page(
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                &prefix,
                after.as_deref(),
                VERSION_SCAN_PAGE_ROWS,
            )?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let tuple_key = core_meta_record_tuple_key(&row.key)?.to_vec();
                let payload = match overlay.get(&tuple_key) {
                    Some(Some(payload)) => payload,
                    Some(None) => continue,
                    None => &row.payload,
                };
                let decoded = decode_object_metadata_row_with_common(payload)?;
                if decoded.object.version_id == deleted_version_id {
                    continue;
                }
                if best.as_ref().is_none_or(|(key, _)| tuple_key < *key) {
                    best = Some((tuple_key, decoded));
                }
                break;
            }
            if best.is_some() || rows.len() < VERSION_SCAN_PAGE_ROWS {
                break;
            }
            after = rows
                .last()
                .map(|row| core_meta_record_tuple_key(&row.key).map(ToOwned::to_owned))
                .transpose()?;
        }
        let Some((_, decoded)) = best else {
            return Ok(None);
        };
        validate_object_scope(bucket, &decoded.object)?;
        Ok(Some(decoded.object))
    }

    async fn prepare_object_boundary_value_projections(
        &self,
        bucket: &Bucket,
        object: &Object,
    ) -> Result<(Vec<OwnedCoreMetaBatchOp>, Vec<CoreMetaRootPublication>)> {
        let Some(data_target) = object_data_target_from_shard_map(object.shard_map.as_ref())?
        else {
            return Ok((Vec::new(), Vec::new()));
        };
        let boundary_values = match &data_target {
            ObjectDataTarget::LogicalFile { locator, .. } => {
                let manifest = self.read_logical_file_manifest(locator).await?;
                manifest_boundary_values(&manifest)
            }
            ObjectDataTarget::ObjectRef { object_ref, .. } => {
                self.read_object_manifest(object_ref).await?.boundary_values
            }
        };
        if boundary_values.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let bucket_key = boundary_schema_bucket_key(bucket.tenant_id, &bucket.name);
        let object_ref = data_target.target_string();
        validate_logical_id(&bucket_key, "boundary value bucket")?;
        validate_logical_id(object_ref, "boundary value object ref")?;

        let mut operations = Vec::with_capacity(boundary_values.len());
        for value in &boundary_values {
            validate_logical_id(&value.name, "boundary value dimension")?;
            let tuple_key = ancillary_boundary_value_key(&bucket_key, value, object_ref)?;
            let payload = encode_boundary_value_row(&bucket_key, object_ref, "object", value)?;
            operations.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_BOUNDARY,
                table_id: TABLE_BOUNDARY_VALUE_ROW,
                tuple_key,
                payload,
                common: None,
            });
        }
        let publications = vec![CoreMetaRootPublication::new(
            ancillary_boundary_root_anchor_key(&bucket_key),
            WriterFamily::TypedMetadata,
        )];
        Ok((operations, publications))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AncillaryOperationKey {
    cf: &'static str,
    table_id: u16,
    tuple_key: Vec<u8>,
}

#[derive(Default)]
struct PreparedAncillaryProjectionBatch {
    operations: Vec<OwnedCoreMetaBatchOp>,
    operation_positions: BTreeMap<AncillaryOperationKey, (usize, String)>,
    publications: BTreeMap<String, CoreMetaRootPublication>,
}

impl PreparedAncillaryProjectionBatch {
    fn extend(
        &mut self,
        operations: Vec<OwnedCoreMetaBatchOp>,
        publications: Vec<CoreMetaRootPublication>,
    ) -> Result<()> {
        for publication in publications {
            self.merge_publication(publication)?;
        }
        for operation in operations {
            self.push_operation(operation)?;
        }
        Ok(())
    }

    fn merge_publication(&mut self, mut publication: CoreMetaRootPublication) -> Result<()> {
        publication.transaction_coordinator = false;
        let root_key_hash = publication.root_key_hash();
        let Some(existing) = self.publications.get_mut(&root_key_hash) else {
            publication.writer_families.sort();
            publication.writer_families.dedup();
            self.publications.insert(root_key_hash, publication);
            return Ok(());
        };
        if existing.root_anchor_key != publication.root_anchor_key {
            bail!("ancillary projection publication root hash collision");
        }
        existing.writer_families.extend(publication.writer_families);
        existing.writer_families.sort();
        existing.writer_families.dedup();
        for manifest in publication.logical_manifests {
            if !existing.logical_manifests.contains(&manifest) {
                existing.logical_manifests.push(manifest);
            }
        }
        existing
            .idempotency_key_hashes
            .extend(publication.idempotency_key_hashes);
        existing.idempotency_key_hashes.sort();
        existing.idempotency_key_hashes.dedup();
        Ok(())
    }

    fn push_operation(&mut self, operation: OwnedCoreMetaBatchOp) -> Result<()> {
        let (operation_key, root_key_hash) = ancillary_operation_identity(&operation)?;
        if root_key_hash.is_empty() {
            bail!("ancillary projection operation is not bound to a publication root");
        }
        if let Some((position, existing_root_key_hash)) =
            self.operation_positions.get(&operation_key)
        {
            if existing_root_key_hash != &root_key_hash {
                bail!("ancillary projection physical row spans multiple publication roots");
            }
            // Projections follow committed stream order, so the final effect
            // for a repeated physical row is the transaction-visible result.
            self.operations[*position] = operation;
            return Ok(());
        }
        let position = self.operations.len();
        self.operations.push(operation);
        self.operation_positions
            .insert(operation_key, (position, root_key_hash));
        Ok(())
    }

    fn finish(mut self) -> Result<(Vec<OwnedCoreMetaBatchOp>, Vec<CoreMetaRootPublication>)> {
        if self.operations.is_empty() {
            if !self.publications.is_empty() {
                bail!("ancillary projection declares publication roots without operations");
            }
            return Ok((Vec::new(), Vec::new()));
        }

        let operation_roots = self
            .operation_positions
            .values()
            .map(|(_, root_key_hash)| root_key_hash.clone())
            .collect::<BTreeSet<_>>();
        let publication_roots = self.publications.keys().cloned().collect::<BTreeSet<_>>();
        if operation_roots != publication_roots {
            bail!("ancillary projection operations and publication roots do not match");
        }

        // CoreMeta rebinds every row to its root's next generation. A single
        // coordinator makes those root publications one atomic commit group.
        let coordinator = self
            .publications
            .values_mut()
            .next()
            .ok_or_else(|| anyhow!("ancillary projection has no coordinator root"))?;
        coordinator
            .writer_families
            .push(WriterFamily::CoreControl.as_str().to_string());
        coordinator.writer_families.sort();
        coordinator.writer_families.dedup();
        coordinator.transaction_coordinator = true;

        Ok((self.operations, self.publications.into_values().collect()))
    }
}

fn ancillary_operation_identity(
    operation: &OwnedCoreMetaBatchOp,
) -> Result<(AncillaryOperationKey, String)> {
    let (cf, table_id, tuple_key, common) = match operation {
        OwnedCoreMetaBatchOp::Put {
            cf,
            table_id,
            tuple_key,
            payload,
            common,
        } => (
            *cf,
            *table_id,
            tuple_key,
            match common {
                Some(common) => common.clone(),
                None => core_meta_row_common_from_payload(payload)?,
            },
        ),
        OwnedCoreMetaBatchOp::Delete {
            cf,
            table_id,
            tuple_key,
            common,
        } => (
            *cf,
            *table_id,
            tuple_key,
            common
                .clone()
                .ok_or_else(|| anyhow!("ancillary projection delete is missing row common"))?,
        ),
    };
    Ok((
        AncillaryOperationKey {
            cf,
            table_id,
            tuple_key: tuple_key.clone(),
        },
        common.root_key_hash,
    ))
}

fn ancillary_boundary_root_anchor_key(bucket: &str) -> String {
    format!("boundary/{bucket}")
}

fn ancillary_boundary_value_key(
    bucket: &str,
    value: &CoreBoundaryValue,
    object_ref: &str,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(bucket),
        CoreMetaTuplePart::Utf8(&value.name),
        CoreMetaTuplePart::Utf8(&value.value),
        CoreMetaTuplePart::Utf8(object_ref),
        CoreMetaTuplePart::Utf8("object"),
    ])
}

fn put_operation(
    partition_id: &str,
    cf: &str,
    table_id: u16,
    tuple_key: Vec<u8>,
    payload: Vec<u8>,
) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: cf.to_string(),
        table_id,
        tuple_key,
        payload,
    }
}

fn delete_operation(
    partition_id: &str,
    cf: &str,
    table_id: u16,
    tuple_key: Vec<u8>,
) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaDelete {
        partition_id: partition_id.to_string(),
        cf: cf.to_string(),
        table_id,
        tuple_key,
    }
}

#[cfg(test)]
fn require_committed_projection_receipt(receipt: &CoreMutationBatchReceipt) -> Result<()> {
    if receipt.state != CoreTransactionState::Committed {
        bail!(
            "object metadata projection mutation did not commit: {}",
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation error")
        );
    }
    Ok(())
}

#[cfg(test)]
fn object_metadata_root_publication(root_anchor_key: &str) -> CoreMutationRootPublication {
    CoreMutationRootPublication {
        root_anchor_key: root_anchor_key.to_string(),
        writer_families: vec![
            WriterFamily::CoreControl.as_str().to_string(),
            WriterFamily::ObjectBlob.as_str().to_string(),
        ],
        transaction_coordinator: true,
    }
}
