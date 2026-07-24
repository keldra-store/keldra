use super::*;

#[derive(Debug, Clone, Copy)]
pub struct ObjectCreateOptions {
    pub exact_index_policy_snapshot: bool,
    pub exact_authz_revision: bool,
    pub enqueue_index_maintenance: bool,
    pub enqueue_metadata_compaction: bool,
    pub(crate) journal_mutation: metadata_journal::ObjectJournalMutation,
}

pub(crate) struct ObjectBatchCreateInput {
    pub key: String,
    pub content_hash: String,
    pub size: i64,
    pub etag: String,
    pub content_type: Option<String>,
    pub user_meta: Option<JsonValue>,
    pub shard_map: JsonValue,
    pub storage_class: String,
}

pub(crate) struct PreparedObjectBatchCreate {
    pub bucket: Bucket,
    pub objects: Vec<Object>,
}

impl ObjectCreateOptions {
    pub fn deferred() -> Self {
        Self {
            exact_index_policy_snapshot: false,
            exact_authz_revision: false,
            enqueue_index_maintenance: false,
            enqueue_metadata_compaction: false,
            journal_mutation: metadata_journal::ObjectJournalMutation::Put,
        }
    }

    pub fn strict() -> Self {
        Self {
            exact_index_policy_snapshot: true,
            exact_authz_revision: true,
            enqueue_index_maintenance: true,
            enqueue_metadata_compaction: true,
            journal_mutation: metadata_journal::ObjectJournalMutation::Put,
        }
    }

    pub(crate) fn copy() -> Self {
        Self {
            journal_mutation: metadata_journal::ObjectJournalMutation::Copy,
            ..Self::strict()
        }
    }
}

impl Default for ObjectCreateOptions {
    fn default() -> Self {
        Self::deferred()
    }
}

fn deferred_index_policy_snapshot_hash(tenant_id: i64, bucket_id: i64) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.deferred_index_policy_snapshot.v1");
    hasher.update(&tenant_id.to_le_bytes());
    hasher.update(&bucket_id.to_le_bytes());
    hasher.finalize().to_hex().to_string()
}

impl Persistence {
    pub(crate) async fn create_objects_with_storage_class_in_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        inputs: Vec<ObjectBatchCreateInput>,
        transaction_id: &str,
        transaction_principal: &str,
        options: ObjectCreateOptions,
        additions: CoreMutationBatchAdditions,
    ) -> Result<Vec<Object>> {
        let prepared = self
            .prepare_objects_with_storage_class_in_transaction(
                tenant_id,
                bucket_id,
                inputs,
                transaction_id,
                options,
            )
            .await?;
        self.stage_prepared_objects_in_transaction(
            prepared,
            transaction_id,
            transaction_principal,
            additions,
        )
        .await
    }

    pub(crate) async fn prepare_objects_with_storage_class_in_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        inputs: Vec<ObjectBatchCreateInput>,
        transaction_id: &str,
        options: ObjectCreateOptions,
    ) -> Result<PreparedObjectBatchCreate> {
        self.prepare_objects_with_storage_class_inner(
            tenant_id,
            bucket_id,
            inputs,
            Some(transaction_id),
            options,
        )
        .await
    }

    pub(crate) async fn prepare_objects_with_storage_class(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        inputs: Vec<ObjectBatchCreateInput>,
        options: ObjectCreateOptions,
    ) -> Result<PreparedObjectBatchCreate> {
        self.prepare_objects_with_storage_class_inner(tenant_id, bucket_id, inputs, None, options)
            .await
    }

    async fn prepare_objects_with_storage_class_inner(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        inputs: Vec<ObjectBatchCreateInput>,
        transaction_id: Option<&str>,
        options: ObjectCreateOptions,
    ) -> Result<PreparedObjectBatchCreate> {
        if inputs.is_empty() {
            return Err(anyhow!("object batch must not be empty"));
        }
        let bucket = bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        if bucket.tenant_id != tenant_id {
            return Err(anyhow!("bucket does not belong to tenant"));
        }
        let index_policy_snapshot = if options.exact_index_policy_snapshot {
            self.active_index_policy_snapshot_hash(tenant_id, bucket_id)
                .await?
        } else {
            deferred_index_policy_snapshot_hash(tenant_id, bucket_id)
        };
        let authz_revision = if options.exact_authz_revision {
            self.latest_authz_revision(tenant_id).await?
        } else {
            0
        };
        let core_store = CoreStore::new(self.storage.clone()).await?;
        let first_object_id = core_store
            .next_object_metadata_id_in_transaction(&bucket, transaction_id)
            .await?;
        let mut objects = Vec::with_capacity(inputs.len());
        for (index, input) in inputs.into_iter().enumerate() {
            let offset = i64::try_from(index).map_err(|_| anyhow!("object batch exceeds i64"))?;
            let id = first_object_id
                .checked_add(offset)
                .ok_or_else(|| anyhow!("object id overflow"))?;
            let version_id = uuid::Uuid::new_v4();
            let mutation_id = uuid::Uuid::new_v4();
            let user_metadata_hash = user_metadata_hash(input.user_meta.as_ref());
            let record_hash = object_version_record_hash(ObjectVersionRecordHashInput {
                tenant_id,
                bucket_id,
                key: &input.key,
                version_id,
                mutation_id,
                content_hash: &input.content_hash,
                size: input.size,
                etag: &input.etag,
                content_type: input.content_type.as_deref(),
                storage_class: Some(&input.storage_class),
                user_metadata_hash: &user_metadata_hash,
                index_policy_snapshot: &index_policy_snapshot,
                authz_revision,
                delete_marker: false,
            });
            objects.push(Object {
                id,
                tenant_id,
                bucket_id,
                key: input.key,
                kind: object_links::ObjectEntryKind::Blob,
                content_hash: input.content_hash,
                size: input.size,
                etag: input.etag,
                content_type: input.content_type,
                version_id,
                mutation_id,
                index_policy_snapshot: index_policy_snapshot.clone(),
                user_metadata_hash,
                authz_revision,
                record_hash,
                created_at: Utc::now(),
                deleted_at: None,
                storage_class: Some(input.storage_class),
                user_meta: input.user_meta,
                shard_map: Some(input.shard_map),
                checksum: None,
                link: None,
            });
        }
        Ok(PreparedObjectBatchCreate { bucket, objects })
    }

    pub(crate) async fn commit_prepared_objects(
        &self,
        prepared: PreparedObjectBatchCreate,
        transaction_id: &str,
        additions: CoreMutationBatchAdditions,
    ) -> Result<Vec<Object>> {
        let permit = self
            .object_metadata_write_permit(prepared.bucket.tenant_id, prepared.bucket.id)
            .await?;
        metadata_journal::commit_object_put_mutations_with_permit(
            &self.storage,
            &prepared.bucket,
            &prepared.objects,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            additions,
        )
        .await?;
        Ok(prepared.objects)
    }

    pub(crate) async fn stage_prepared_objects_in_transaction(
        &self,
        prepared: PreparedObjectBatchCreate,
        transaction_id: &str,
        transaction_principal: &str,
        additions: CoreMutationBatchAdditions,
    ) -> Result<Vec<Object>> {
        let permit = self
            .object_metadata_write_permit(prepared.bucket.tenant_id, prepared.bucket.id)
            .await?;
        metadata_journal::append_object_put_mutations_with_permit_in_transaction(
            &self.storage,
            &prepared.bucket,
            &prepared.objects,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            transaction_principal,
            additions,
        )
        .await?;
        Ok(prepared.objects)
    }

    pub async fn create_object(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        content_hash: &str,
        size: i64,
        etag: &str,
        content_type: Option<&str>,
        user_meta: Option<JsonValue>,
        shard_map: Option<JsonValue>,
        payload: Option<Vec<u8>>,
        transaction_id: Option<&str>,
    ) -> Result<Object> {
        self.create_object_with_storage_class(
            tenant_id,
            bucket_id,
            key,
            content_hash,
            size,
            etag,
            content_type,
            user_meta,
            shard_map,
            payload,
            transaction_id,
            None,
            None,
        )
        .await
    }

    pub async fn create_object_with_storage_class(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        content_hash: &str,
        size: i64,
        etag: &str,
        content_type: Option<&str>,
        user_meta: Option<JsonValue>,
        shard_map: Option<JsonValue>,
        payload: Option<Vec<u8>>,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
        storage_class: Option<String>,
    ) -> Result<Object> {
        self.create_object_with_storage_class_with_options(
            tenant_id,
            bucket_id,
            key,
            content_hash,
            size,
            etag,
            content_type,
            user_meta,
            shard_map,
            payload,
            transaction_id,
            transaction_principal,
            storage_class,
            ObjectCreateOptions::strict(),
        )
        .await
    }

    pub async fn create_object_with_storage_class_with_options(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        content_hash: &str,
        size: i64,
        etag: &str,
        content_type: Option<&str>,
        user_meta: Option<JsonValue>,
        shard_map: Option<JsonValue>,
        payload: Option<Vec<u8>>,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
        storage_class: Option<String>,
        options: ObjectCreateOptions,
    ) -> Result<Object> {
        let total_start = std::time::Instant::now();
        let step_start = std::time::Instant::now();
        let bucket = bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        if bucket.tenant_id != tenant_id {
            return Err(anyhow!("bucket does not belong to tenant"));
        }
        crate::emit_test_timing(
            "persistence.create_object read_bucket",
            step_start.elapsed(),
        );
        let version_id = uuid::Uuid::new_v4();
        let mutation_id = uuid::Uuid::new_v4();
        let step_start = std::time::Instant::now();
        let shard_map = match (shard_map, payload) {
            (Some(shard_map), _) => Some(shard_map),
            (None, Some(_)) => {
                bail!(
                    "object payload bytes must be written by the object service through CoreStore logical-file staging before metadata creation"
                );
            }
            (None, None) => None,
        };
        crate::emit_test_timing("persistence.create_object shard_map", step_start.elapsed());
        let step_start = std::time::Instant::now();
        let index_policy_snapshot = if options.exact_index_policy_snapshot {
            self.active_index_policy_snapshot_hash(tenant_id, bucket_id)
                .await?
        } else {
            deferred_index_policy_snapshot_hash(tenant_id, bucket_id)
        };
        crate::emit_test_timing(
            "persistence.create_object active_index_policy_snapshot_hash",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        let user_metadata_hash = user_metadata_hash(user_meta.as_ref());
        let authz_revision = if options.exact_authz_revision {
            self.latest_authz_revision(tenant_id).await?
        } else {
            0
        };
        crate::emit_test_timing(
            "persistence.create_object latest_authz_revision",
            step_start.elapsed(),
        );
        let record_hash = object_version_record_hash(ObjectVersionRecordHashInput {
            tenant_id,
            bucket_id,
            key,
            version_id,
            mutation_id,
            content_hash,
            size,
            etag,
            content_type,
            storage_class: storage_class.as_deref(),
            user_metadata_hash: &user_metadata_hash,
            index_policy_snapshot: &index_policy_snapshot,
            authz_revision,
            delete_marker: false,
        });
        let step_start = std::time::Instant::now();
        let object = Object {
            id: metadata_journal::next_object_id(
                &self.storage,
                &bucket,
                &self.partition_owner_signing_key,
            )
            .await?,
            tenant_id,
            bucket_id,
            key: key.to_string(),
            kind: object_links::ObjectEntryKind::Blob,
            content_hash: content_hash.to_string(),
            size,
            etag: etag.to_string(),
            content_type: content_type.map(ToOwned::to_owned),
            version_id,
            mutation_id,
            index_policy_snapshot,
            user_metadata_hash,
            authz_revision,
            record_hash,
            created_at: Utc::now(),
            deleted_at: None,
            storage_class,
            user_meta,
            shard_map,
            checksum: None,
            link: None,
        };
        crate::emit_test_timing(
            "persistence.create_object next_object_id",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        let permit =
            Box::pin(self.object_metadata_write_permit(bucket.tenant_id, bucket.id)).await?;
        crate::emit_test_timing(
            "persistence.create_object object_metadata_write_permit",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        if let Some(transaction_id) = transaction_id {
            Box::pin(
                metadata_journal::append_object_mutation_with_permit_in_transaction(
                    &self.storage,
                    &bucket,
                    &object,
                    options.journal_mutation,
                    &permit,
                    &self.partition_owner_signing_key,
                    Some(transaction_id),
                    transaction_principal,
                ),
            )
            .await?;
        } else {
            Box::pin(metadata_journal::append_object_mutation_with_permit(
                &self.storage,
                &bucket,
                &object,
                options.journal_mutation,
                &permit,
                &self.partition_owner_signing_key,
            ))
            .await?;
        }
        crate::emit_test_timing(
            "persistence.create_object append_object_mutation",
            step_start.elapsed(),
        );
        if transaction_id.is_none() {
            if options.enqueue_index_maintenance {
                let step_start = std::time::Instant::now();
                self.enqueue_index_builds_for_object_keys(&bucket, [object.key.as_str()])
                    .await?;
                crate::emit_test_timing(
                    "persistence.create_object enqueue_index_builds_for_object_keys",
                    step_start.elapsed(),
                );
            }
            if options.enqueue_metadata_compaction {
                let step_start = std::time::Instant::now();
                self.enqueue_object_metadata_compaction_if_due(&bucket)
                    .await?;
                crate::emit_test_timing(
                    "persistence.create_object enqueue_object_metadata_compaction_if_due",
                    step_start.elapsed(),
                );
            }
        }
        crate::emit_test_timing("persistence.create_object total", total_start.elapsed());
        Ok(object)
    }

    pub async fn put_object_link(
        &self,
        request: object_links::PutObjectLinkRequest,
    ) -> std::result::Result<object_links::ObjectLinkMutation, object_links::ObjectLinkError> {
        self.put_object_link_with_options(request, ObjectCreateOptions::deferred())
            .await
    }

    pub async fn put_object_link_with_options(
        &self,
        request: object_links::PutObjectLinkRequest,
        options: ObjectCreateOptions,
    ) -> std::result::Result<object_links::ObjectLinkMutation, object_links::ObjectLinkError> {
        if !crate::validation::is_valid_object_key(&request.link_key) {
            return Err(object_links::ObjectLinkError::InvalidLinkKey);
        }
        if !crate::validation::is_valid_object_key(&request.target_key) {
            return Err(object_links::ObjectLinkError::InvalidTargetKey);
        }

        let bucket = bucket_journal::read_current_bucket_by_id(&self.storage, request.bucket_id)
            .await?
            .ok_or(object_links::ObjectLinkError::BucketNotFound)?;
        if bucket.tenant_id != request.tenant_id {
            return Err(object_links::ObjectLinkError::BucketTenantMismatch);
        }

        let current = metadata_journal::read_current_object(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            &request.link_key,
        )
        .await?;
        if request.create_only && current.is_some() {
            return Err(object_links::ObjectLinkError::AlreadyExists);
        }
        let existing_generation = match current.as_ref() {
            Some(object) if object.kind != object_links::ObjectEntryKind::Link => {
                return Err(object_links::ObjectLinkError::ExistingObjectIsNotLink);
            }
            Some(object) => object_links::link_generation(object).unwrap_or(0),
            None => 0,
        };

        if !request.create_only {
            let expected = request
                .expected_generation
                .ok_or(object_links::ObjectLinkError::MissingExpectedGeneration)?;
            if expected != existing_generation {
                return Err(object_links::ObjectLinkError::GenerationConflict {
                    expected,
                    actual: existing_generation,
                });
            }
        } else if let Some(expected) = request.expected_generation
            && expected != 0
        {
            return Err(object_links::ObjectLinkError::GenerationConflict {
                expected,
                actual: existing_generation,
            });
        }

        if !request.allow_dangling {
            let target = match request.target_version {
                Some(version_id) => {
                    metadata_journal::read_object_version(
                        &self.storage,
                        &bucket,
                        &self.partition_owner_signing_key,
                        &request.target_key,
                        version_id,
                    )
                    .await?
                }
                None => {
                    metadata_journal::read_current_object(
                        &self.storage,
                        &bucket,
                        &self.partition_owner_signing_key,
                        &request.target_key,
                    )
                    .await?
                }
            }
            .ok_or(object_links::ObjectLinkError::DanglingObjectLink)?;
            if target.deleted_at.is_some() {
                return Err(object_links::ObjectLinkError::DanglingObjectLink);
            }
            if target.kind != object_links::ObjectEntryKind::Blob {
                return Err(object_links::ObjectLinkError::TargetNotBlob);
            }
        }

        let now = Utc::now();
        let generation = existing_generation.checked_add(1).ok_or_else(|| {
            object_links::ObjectLinkError::Internal("link generation overflow".to_string())
        })?;
        let link_created_at = current
            .as_ref()
            .and_then(|object| object.link.as_ref())
            .map(|link| link.created_at)
            .unwrap_or(now);
        let descriptor = object_links::ObjectLinkDescriptor {
            schema: "anvil.object_link.v1".to_string(),
            tenant_id: request.tenant_id.to_string(),
            bucket_name: bucket.name.clone(),
            link_key: request.link_key.clone(),
            target_key: request.target_key.clone(),
            target_version: request.target_version.map(|version| version.to_string()),
            resolution: request.resolution,
            created_at: link_created_at,
            updated_at: now,
            created_by: request.created_by.clone(),
            generation,
        };
        let content_hash = object_links::link_metadata_hash(&descriptor);
        let etag = object_links::link_metadata_etag(&descriptor);
        let version_id = uuid::Uuid::new_v4();
        let mutation_id = uuid::Uuid::new_v4();
        let index_policy_snapshot = if options.exact_index_policy_snapshot {
            self.active_index_policy_snapshot_hash(request.tenant_id, bucket.id)
                .await?
        } else {
            deferred_index_policy_snapshot_hash(request.tenant_id, bucket.id)
        };
        let user_meta = Some(serde_json::json!({
            "schema": "anvil.object_link.v1",
            "idempotency_key": request.idempotency_key.clone(),
        }));
        let user_metadata_hash = user_metadata_hash(user_meta.as_ref());
        let authz_revision = if options.exact_authz_revision {
            self.latest_authz_revision(request.tenant_id).await?
        } else {
            0
        };
        let record_hash = object_version_record_hash(ObjectVersionRecordHashInput {
            tenant_id: request.tenant_id,
            bucket_id: bucket.id,
            key: &request.link_key,
            version_id,
            mutation_id,
            content_hash: &content_hash,
            size: 0,
            etag: &etag,
            content_type: Some(object_links::LINK_METADATA_CONTENT_TYPE),
            storage_class: None,
            user_metadata_hash: &user_metadata_hash,
            index_policy_snapshot: &index_policy_snapshot,
            authz_revision,
            delete_marker: false,
        });
        let link = object_links::ObjectLinkTarget {
            target_key: request.target_key,
            target_version: request.target_version,
            resolution: request.resolution,
            generation,
            created_at: link_created_at,
            created_by: request.created_by,
        };
        let object = Object {
            id: metadata_journal::next_object_id(
                &self.storage,
                &bucket,
                &self.partition_owner_signing_key,
            )
            .await?,
            tenant_id: request.tenant_id,
            bucket_id: bucket.id,
            key: request.link_key,
            kind: object_links::ObjectEntryKind::Link,
            content_hash,
            size: 0,
            etag,
            content_type: Some(object_links::LINK_METADATA_CONTENT_TYPE.to_string()),
            version_id,
            mutation_id,
            index_policy_snapshot,
            user_metadata_hash,
            authz_revision,
            record_hash,
            created_at: now,
            deleted_at: None,
            storage_class: None,
            user_meta,
            shard_map: None,
            checksum: None,
            link: Some(link),
        };
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        if let Some(transaction_id) = request.transaction_id.as_deref() {
            metadata_journal::append_object_mutation_with_permit_in_transaction(
                &self.storage,
                &bucket,
                &object,
                metadata_journal::ObjectJournalMutation::Put,
                &permit,
                &self.partition_owner_signing_key,
                Some(transaction_id),
                request.transaction_principal.as_deref(),
            )
            .await?;
        } else {
            metadata_journal::append_object_mutation_with_permit(
                &self.storage,
                &bucket,
                &object,
                metadata_journal::ObjectJournalMutation::Put,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await?;
            if options.enqueue_index_maintenance {
                self.enqueue_index_builds_for_object_keys(&bucket, [object.key.as_str()])
                    .await?;
            }
            if options.enqueue_metadata_compaction {
                self.enqueue_object_metadata_compaction_if_due(&bucket)
                    .await?;
            }
        }
        Ok(object_links::ObjectLinkMutation {
            link: object,
            descriptor,
        })
    }

    pub async fn get_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        metadata_journal::read_current_object(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            key,
        )
        .await
    }

    pub async fn get_object_link(
        &self,
        bucket_id: i64,
        key: &str,
    ) -> std::result::Result<
        Option<object_links::ObjectLinkDescriptor>,
        object_links::ObjectLinkError,
    > {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        let Some(object) = metadata_journal::read_current_object(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            key,
        )
        .await?
        else {
            return Ok(None);
        };
        if object.kind != object_links::ObjectEntryKind::Link {
            return Ok(None);
        }
        Ok(object_links::link_descriptor(&bucket.name, &object))
    }

    pub async fn list_object_links(
        &self,
        bucket_id: i64,
        prefix: Option<&str>,
    ) -> std::result::Result<Vec<object_links::ObjectLinkDescriptor>, object_links::ObjectLinkError>
    {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Err(object_links::ObjectLinkError::BucketNotFound);
        };
        let mut links = metadata_journal::read_current_directory_objects(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
        )
        .await?
        .into_iter()
        .filter(|object| object.kind == object_links::ObjectEntryKind::Link)
        .filter_map(|object| object_links::link_descriptor(&bucket.name, &object))
        .filter(|descriptor| {
            prefix
                .map(|prefix| descriptor.link_key.starts_with(prefix))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
        links.sort_by(|left, right| left.link_key.cmp(&right.link_key));
        Ok(links)
    }

    pub async fn delete_object_link(
        &self,
        request: object_links::DeleteObjectLinkRequest,
    ) -> std::result::Result<object_links::DeleteObjectLinkResult, object_links::ObjectLinkError>
    {
        self.delete_object_link_with_options(request, ObjectCreateOptions::deferred())
            .await
    }

    pub async fn delete_object_link_with_options(
        &self,
        request: object_links::DeleteObjectLinkRequest,
        options: ObjectCreateOptions,
    ) -> std::result::Result<object_links::DeleteObjectLinkResult, object_links::ObjectLinkError>
    {
        if !crate::validation::is_valid_object_key(&request.link_key) {
            return Err(object_links::ObjectLinkError::InvalidLinkKey);
        }

        let bucket = bucket_journal::read_current_bucket_by_id(&self.storage, request.bucket_id)
            .await?
            .ok_or(object_links::ObjectLinkError::BucketNotFound)?;
        if bucket.tenant_id != request.tenant_id {
            return Err(object_links::ObjectLinkError::BucketTenantMismatch);
        }

        let current = metadata_journal::read_current_object(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            &request.link_key,
        )
        .await?
        .ok_or(object_links::ObjectLinkError::NotFound)?;
        if current.kind != object_links::ObjectEntryKind::Link {
            return Err(object_links::ObjectLinkError::ExistingObjectIsNotLink);
        }
        let current_link = current
            .link
            .as_ref()
            .ok_or_else(|| object_links::ObjectLinkError::Internal("link target missing".into()))?;
        if current_link.generation != request.expected_generation {
            return Err(object_links::ObjectLinkError::GenerationConflict {
                expected: request.expected_generation,
                actual: current_link.generation,
            });
        }

        let new_generation = current_link.generation.checked_add(1).ok_or_else(|| {
            object_links::ObjectLinkError::Internal("link generation overflow".to_string())
        })?;
        let now = Utc::now();
        let version_id = uuid::Uuid::new_v4();
        let mutation_id = uuid::Uuid::new_v4();
        let content_hash = String::new();
        let etag = String::new();
        let index_policy_snapshot = if options.exact_index_policy_snapshot {
            self.active_index_policy_snapshot_hash(request.tenant_id, bucket.id)
                .await?
        } else {
            deferred_index_policy_snapshot_hash(request.tenant_id, bucket.id)
        };
        let user_meta = Some(serde_json::json!({
            "schema": "anvil.object_link_delete.v1",
            "idempotency_key": request.idempotency_key.clone(),
        }));
        let user_metadata_hash = user_metadata_hash(user_meta.as_ref());
        let authz_revision = if options.exact_authz_revision {
            self.latest_authz_revision(request.tenant_id).await?
        } else {
            0
        };
        let record_hash = object_version_record_hash(ObjectVersionRecordHashInput {
            tenant_id: request.tenant_id,
            bucket_id: bucket.id,
            key: &request.link_key,
            version_id,
            mutation_id,
            content_hash: &content_hash,
            size: 0,
            etag: &etag,
            content_type: Some(object_links::LINK_METADATA_CONTENT_TYPE),
            storage_class: None,
            user_metadata_hash: &user_metadata_hash,
            index_policy_snapshot: &index_policy_snapshot,
            authz_revision,
            delete_marker: true,
        });
        let object = Object {
            id: metadata_journal::next_object_id(
                &self.storage,
                &bucket,
                &self.partition_owner_signing_key,
            )
            .await?,
            tenant_id: request.tenant_id,
            bucket_id: bucket.id,
            key: request.link_key.clone(),
            kind: object_links::ObjectEntryKind::Link,
            content_hash,
            size: 0,
            etag,
            content_type: Some(object_links::LINK_METADATA_CONTENT_TYPE.to_string()),
            version_id,
            mutation_id,
            index_policy_snapshot,
            user_metadata_hash,
            authz_revision,
            record_hash,
            created_at: now,
            deleted_at: Some(now),
            storage_class: None,
            user_meta,
            shard_map: None,
            checksum: None,
            link: Some(object_links::ObjectLinkTarget {
                target_key: current_link.target_key.clone(),
                target_version: current_link.target_version,
                resolution: current_link.resolution,
                generation: new_generation,
                created_at: current_link.created_at,
                created_by: current_link.created_by.clone(),
            }),
        };
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        if let Some(transaction_id) = request.transaction_id.as_deref() {
            metadata_journal::append_object_mutation_with_permit_in_transaction(
                &self.storage,
                &bucket,
                &object,
                metadata_journal::ObjectJournalMutation::DeleteMarker,
                &permit,
                &self.partition_owner_signing_key,
                Some(transaction_id),
                request.transaction_principal.as_deref(),
            )
            .await?;
        } else {
            metadata_journal::append_object_mutation_with_permit(
                &self.storage,
                &bucket,
                &object,
                metadata_journal::ObjectJournalMutation::DeleteMarker,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await?;
            if options.enqueue_index_maintenance {
                self.enqueue_index_builds_for_object_keys(&bucket, [object.key.as_str()])
                    .await?;
            }
            if options.enqueue_metadata_compaction {
                self.enqueue_object_metadata_compaction_if_due(&bucket)
                    .await?;
            }
        }
        Ok(object_links::DeleteObjectLinkResult {
            link_key: request.link_key,
            generation: new_generation,
        })
    }

    pub async fn resolve_object_link_target(
        &self,
        bucket_id: i64,
        link_key: &str,
    ) -> std::result::Result<Object, object_links::ObjectLinkError> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Err(object_links::ObjectLinkError::BucketNotFound);
        };
        let mut current_key = link_key.to_string();
        let mut current_version = None;
        let mut seen = HashSet::new();
        for _ in 0..object_links::MAX_LINK_RESOLUTION_DEPTH {
            let object = match current_version {
                Some(version_id) => {
                    metadata_journal::read_object_version(
                        &self.storage,
                        &bucket,
                        &self.partition_owner_signing_key,
                        &current_key,
                        version_id,
                    )
                    .await?
                }
                None => {
                    metadata_journal::read_current_object(
                        &self.storage,
                        &bucket,
                        &self.partition_owner_signing_key,
                        &current_key,
                    )
                    .await?
                }
            }
            .ok_or(object_links::ObjectLinkError::DanglingObjectLink)?;
            if object.deleted_at.is_some() {
                return Err(object_links::ObjectLinkError::DanglingObjectLink);
            }
            if object.kind == object_links::ObjectEntryKind::Blob {
                return Ok(object);
            }
            let Some(link) = object.link.as_ref() else {
                return Err(object_links::ObjectLinkError::TargetNotBlob);
            };
            let seen_key = format!("{}:{}", object.key, object.version_id);
            if !seen.insert(seen_key) {
                return Err(object_links::ObjectLinkError::LinkLoop);
            }
            current_key = link.target_key.clone();
            current_version = link.target_version;
        }
        Err(object_links::ObjectLinkError::LinkDepthExceeded)
    }

    pub async fn get_object_version(
        &self,
        bucket_id: i64,
        key: &str,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        metadata_journal::read_object_version(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            key,
            version_id,
        )
        .await
    }

    pub async fn get_object_version_by_id(
        &self,
        bucket_id: i64,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        metadata_journal::read_object_version_by_id(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            version_id,
        )
        .await
    }

    pub async fn list_current_directory_objects(&self, bucket: &Bucket) -> Result<Vec<Object>> {
        metadata_journal::read_current_directory_objects(
            &self.storage,
            bucket,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_objects(
        &self,
        bucket_id: i64,
        prefix: &str,
        start_after: &str,
        limit: i32,
        delimiter: &str,
    ) -> Result<(Vec<Object>, Vec<String>)> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok((Vec::new(), Vec::new()));
        };
        let listing = metadata_journal::list_current_objects(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            prefix,
            start_after,
            limit,
            delimiter,
        )
        .await?;
        Ok((listing.objects, listing.common_prefixes))
    }

    pub async fn soft_delete_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        self.soft_delete_object_in_transaction(bucket_id, key, None, None)
            .await
    }

    pub async fn soft_delete_object_in_transaction(
        &self,
        bucket_id: i64,
        key: &str,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<Option<Object>> {
        self.soft_delete_object_in_transaction_with_options(
            bucket_id,
            key,
            transaction_id,
            transaction_principal,
            ObjectCreateOptions::deferred(),
        )
        .await
    }

    pub async fn soft_delete_object_in_transaction_with_options(
        &self,
        bucket_id: i64,
        key: &str,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
        options: ObjectCreateOptions,
    ) -> Result<Option<Object>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        let Some(base) = metadata_journal::read_current_object(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            key,
        )
        .await?
        else {
            return Ok(None);
        };
        let now = Utc::now();
        let object = Object {
            id: metadata_journal::next_object_id(
                &self.storage,
                &bucket,
                &self.partition_owner_signing_key,
            )
            .await?,
            mutation_id: uuid::Uuid::new_v4(),
            version_id: uuid::Uuid::new_v4(),
            content_hash: String::new(),
            size: 0,
            etag: String::new(),
            created_at: now,
            deleted_at: Some(now),
            ..base
        };
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        if let Some(transaction_id) = transaction_id {
            metadata_journal::append_object_mutation_with_permit_in_transaction(
                &self.storage,
                &bucket,
                &object,
                metadata_journal::ObjectJournalMutation::DeleteMarker,
                &permit,
                &self.partition_owner_signing_key,
                Some(transaction_id),
                transaction_principal,
            )
            .await?;
        } else {
            metadata_journal::append_object_mutation_with_permit(
                &self.storage,
                &bucket,
                &object,
                metadata_journal::ObjectJournalMutation::DeleteMarker,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await?;
            if options.enqueue_index_maintenance {
                self.enqueue_index_builds_for_object_keys(&bucket, [object.key.as_str()])
                    .await?;
            }
            if options.enqueue_metadata_compaction {
                self.enqueue_object_metadata_compaction_if_due(&bucket)
                    .await?;
            }
        }
        Ok(Some(object))
    }

    pub async fn delete_object_version(
        &self,
        bucket_id: i64,
        key: &str,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        self.delete_object_version_in_transaction(bucket_id, key, version_id, None, None)
            .await
    }

    pub async fn delete_object_version_in_transaction(
        &self,
        bucket_id: i64,
        key: &str,
        version_id: uuid::Uuid,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<Option<Object>> {
        self.delete_object_version_in_transaction_with_options(
            bucket_id,
            key,
            version_id,
            transaction_id,
            transaction_principal,
            ObjectCreateOptions::deferred(),
        )
        .await
    }

    pub async fn delete_object_version_in_transaction_with_options(
        &self,
        bucket_id: i64,
        key: &str,
        version_id: uuid::Uuid,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
        options: ObjectCreateOptions,
    ) -> Result<Option<Object>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        let Some(mut object) = metadata_journal::read_object_version(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            key,
            version_id,
        )
        .await?
        else {
            return Ok(None);
        };
        object.id = metadata_journal::next_object_id(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
        )
        .await?;
        object.mutation_id = uuid::Uuid::new_v4();
        object.deleted_at = Some(Utc::now());
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        if let Some(transaction_id) = transaction_id {
            metadata_journal::append_object_mutation_with_permit_in_transaction(
                &self.storage,
                &bucket,
                &object,
                metadata_journal::ObjectJournalMutation::DeleteVersion,
                &permit,
                &self.partition_owner_signing_key,
                Some(transaction_id),
                transaction_principal,
            )
            .await?;
        } else {
            metadata_journal::append_object_mutation_with_permit(
                &self.storage,
                &bucket,
                &object,
                metadata_journal::ObjectJournalMutation::DeleteVersion,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await?;
            if options.enqueue_index_maintenance {
                self.enqueue_index_builds_for_object_keys(&bucket, [object.key.as_str()])
                    .await?;
            }
            if options.enqueue_metadata_compaction {
                self.enqueue_object_metadata_compaction_if_due(&bucket)
                    .await?;
            }
        }
        Ok(Some(object))
    }

    pub async fn list_object_versions(
        &self,
        bucket_id: i64,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<ObjectVersionsPage> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(ObjectVersionsPage {
                versions: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
            });
        };
        metadata_journal::read_object_versions(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            prefix,
            key_marker,
            version_id_marker,
            limit,
        )
        .await
    }

    pub async fn compact_object_metadata(
        &self,
        bucket_id: i64,
    ) -> Result<Option<metadata_journal::SealedObjectMetadataSegments>> {
        let Some(bucket) = self.pending_object_metadata_compaction(bucket_id).await? else {
            return Ok(None);
        };
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        metadata_journal::seal_object_journal_segments_with_permit(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
        .map(Some)
    }

    pub(crate) async fn compact_object_metadata_for_task(
        &self,
        bucket_id: i64,
        task_guard: &crate::task_execution_guard::TaskExecutionGuard,
    ) -> Result<Option<metadata_journal::SealedObjectMetadataSegments>> {
        let Some(bucket) = self.pending_object_metadata_compaction(bucket_id).await? else {
            return Ok(None);
        };
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        metadata_journal::seal_object_journal_segments_with_task_guard(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            &permit,
            &self.partition_owner_signing_key,
            task_guard,
        )
        .await
        .map(Some)
    }

    async fn pending_object_metadata_compaction(&self, bucket_id: i64) -> Result<Option<Bucket>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        let active_stats = metadata_journal::active_object_journal_stats(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
        )
        .await?;
        Ok(
            (active_stats.last_sequence > active_stats.compacted_through_sequence)
                .then_some(bucket),
        )
    }

    pub(super) async fn enqueue_object_metadata_compaction_if_due(
        &self,
        bucket: &Bucket,
    ) -> Result<()> {
        let stats = metadata_journal::active_object_journal_stats(
            &self.storage,
            bucket,
            &self.partition_owner_signing_key,
        )
        .await?;
        let frame_due = self.object_metadata_compaction_frame_threshold > 0
            && stats.uncompacted_frame_count >= self.object_metadata_compaction_frame_threshold;
        let bytes_due = self.object_metadata_compaction_bytes_threshold > 0
            && stats.uncompacted_encoded_bytes >= self.object_metadata_compaction_bytes_threshold;
        if !frame_due && !bytes_due {
            return Ok(());
        }

        self.enqueue_task_if_absent(
            crate::tasks::TaskType::ObjectMetadataCompaction,
            serde_json::json!({ "bucket_id": bucket.id }),
            50,
        )
        .await?;
        Ok(())
    }

    pub async fn enqueue_object_write_maintenance_for_keys_if_due(
        &self,
        bucket: &Bucket,
        object_keys: &[String],
        enqueue_indexes: bool,
        enqueue_compaction: bool,
    ) -> Result<usize> {
        let mut scheduled = 0usize;
        if enqueue_indexes {
            scheduled = scheduled.saturating_add(
                self.enqueue_index_builds_for_object_keys(
                    bucket,
                    object_keys.iter().map(String::as_str),
                )
                .await?,
            );
        }
        if enqueue_compaction {
            self.enqueue_object_metadata_compaction_if_due(bucket)
                .await?;
        }
        Ok(scheduled)
    }
}
