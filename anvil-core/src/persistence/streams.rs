use super::*;

impl Persistence {
    pub async fn create_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
    ) -> Result<MultipartUploadMutation> {
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::create_multipart_upload_with_permit(
            &self.storage,
            tenant_id,
            bucket_id,
            key,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn create_multipart_upload_in_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<MultipartUploadMutation> {
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::create_multipart_upload_with_permit_in_transaction(
            &self.storage,
            tenant_id,
            bucket_id,
            key,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    pub async fn get_active_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        upload_id: uuid::Uuid,
    ) -> Result<Option<MultipartUpload>> {
        multipart_journal::get_active_multipart_upload(
            &self.storage,
            tenant_id,
            bucket_id,
            key,
            upload_id,
        )
        .await
    }

    pub async fn get_active_multipart_upload_in_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        upload_id: uuid::Uuid,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<Option<MultipartUpload>> {
        multipart_journal::get_active_multipart_upload_in_transaction(
            &self.storage,
            tenant_id,
            bucket_id,
            key,
            upload_id,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    pub async fn upsert_multipart_part(
        &self,
        upload_row_id: i64,
        part_number: i32,
        object_ref: CoreObjectRef,
        size: i64,
        etag: &str,
    ) -> Result<MultipartUploadPartMutation> {
        let (tenant_id, bucket_id) =
            multipart_journal::find_multipart_upload_partition(&self.storage, upload_row_id)
                .await?
                .ok_or_else(|| anyhow!("multipart upload not found"))?;
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::upsert_multipart_part_with_permit(
            &self.storage,
            upload_row_id,
            part_number,
            object_ref,
            size,
            etag,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn upsert_multipart_part_in_transaction(
        &self,
        upload_row_id: i64,
        part_number: i32,
        object_ref: CoreObjectRef,
        size: i64,
        etag: &str,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<MultipartUploadPartMutation> {
        let (tenant_id, bucket_id) =
            multipart_journal::find_multipart_upload_partition_in_transaction(
                &self.storage,
                upload_row_id,
                transaction_id,
                transaction_principal,
            )
            .await?
            .ok_or_else(|| anyhow!("multipart upload not found"))?;
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::upsert_multipart_part_with_permit_in_transaction(
            &self.storage,
            upload_row_id,
            part_number,
            object_ref,
            size,
            etag,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    pub async fn list_multipart_parts(
        &self,
        upload_row_id: i64,
    ) -> Result<Vec<MultipartUploadPart>> {
        multipart_journal::list_multipart_parts(&self.storage, upload_row_id).await
    }

    pub async fn list_multipart_parts_in_transaction(
        &self,
        upload_row_id: i64,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<Vec<MultipartUploadPart>> {
        multipart_journal::list_multipart_parts_in_transaction(
            &self.storage,
            upload_row_id,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    pub async fn list_multipart_parts_page(
        &self,
        upload_row_id: i64,
        part_number_marker: i32,
        limit: i32,
    ) -> Result<MultipartPartsPage> {
        multipart_journal::list_multipart_parts_page(
            &self.storage,
            upload_row_id,
            part_number_marker,
            limit,
        )
        .await
    }

    pub async fn list_active_multipart_uploads(
        &self,
        bucket_id: i64,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<MultipartUploadsPage> {
        multipart_journal::list_active_multipart_uploads(
            &self.storage,
            bucket_id,
            prefix,
            key_marker,
            upload_id_marker,
            limit,
        )
        .await
    }

    pub async fn complete_multipart_upload(
        &self,
        upload_row_id: i64,
    ) -> Result<MultipartCompletionMutation> {
        let Some((tenant_id, bucket_id)) =
            multipart_journal::find_multipart_upload_partition(&self.storage, upload_row_id)
                .await?
        else {
            return Ok(MultipartCompletionMutation {
                completed: false,
                receipt: None,
            });
        };
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::complete_multipart_upload_with_permit(
            &self.storage,
            upload_row_id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn complete_multipart_upload_in_transaction(
        &self,
        upload_row_id: i64,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<MultipartCompletionMutation> {
        let Some((tenant_id, bucket_id)) =
            multipart_journal::find_multipart_upload_partition_in_transaction(
                &self.storage,
                upload_row_id,
                transaction_id,
                transaction_principal,
            )
            .await?
        else {
            return Ok(MultipartCompletionMutation {
                completed: false,
                receipt: None,
            });
        };
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::complete_multipart_upload_with_permit_in_transaction(
            &self.storage,
            upload_row_id,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    pub async fn abort_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        upload_id: uuid::Uuid,
    ) -> Result<MultipartAbortMutation> {
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::abort_multipart_upload_with_permit(
            &self.storage,
            tenant_id,
            bucket_id,
            key,
            upload_id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn abort_multipart_upload_in_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        upload_id: uuid::Uuid,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<MultipartAbortMutation> {
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::abort_multipart_upload_with_permit_in_transaction(
            &self.storage,
            tenant_id,
            bucket_id,
            key,
            upload_id,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    pub async fn latest_object_watch_cursor(&self, tenant_id: i64, bucket_id: i64) -> Result<i64> {
        i64::try_from(
            watch_log::latest_object_watch_stream_cursor(&self.storage, tenant_id, bucket_id)
                .await?,
        )
        .map_err(|_| anyhow!("object watch cursor exceeds i64"))
    }

    pub async fn list_object_watch_event_page(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        prefix: &str,
        after_cursor: i64,
        limit: usize,
    ) -> Result<watch_log::ObjectWatchEventPage> {
        watch_log::list_object_watch_event_page(
            &self.storage,
            tenant_id,
            bucket_id,
            prefix,
            after_cursor,
            limit,
        )
        .await
    }

    pub async fn create_append_stream(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        stream_key: &str,
    ) -> Result<AppendStreamMutation> {
        let permit = self
            .append_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        append_journal::create_append_stream_with_permit(
            &self.storage,
            tenant_id,
            bucket_id,
            bucket_name,
            stream_key,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn create_append_stream_in_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        stream_key: &str,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<AppendStreamMutation> {
        let permit = self
            .append_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        append_journal::create_append_stream_with_permit_in_transaction(
            &self.storage,
            tenant_id,
            bucket_id,
            bucket_name,
            stream_key,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    pub async fn get_active_append_stream(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        stream_key: &str,
        stream_id: uuid::Uuid,
    ) -> Result<Option<AppendStream>> {
        append_journal::get_active_append_stream(
            &self.storage,
            tenant_id,
            bucket_id,
            stream_key,
            stream_id,
        )
        .await
    }

    pub async fn get_active_append_stream_in_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        stream_key: &str,
        stream_id: uuid::Uuid,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<Option<AppendStream>> {
        append_journal::get_active_append_stream_in_transaction(
            &self.storage,
            tenant_id,
            bucket_id,
            stream_key,
            stream_id,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    pub async fn append_stream_record(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        stream: &AppendStream,
        payload_object_ref: CoreObjectRef,
        payload_size: i64,
        content_type: Option<String>,
        user_meta: Option<JsonValue>,
        authenticated_principal: &str,
    ) -> Result<AppendStreamRecordMutation> {
        let permit = self
            .append_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        append_journal::append_stream_record_with_permit_in_partition(
            &self.storage,
            tenant_id,
            bucket_id,
            stream,
            payload_object_ref,
            payload_size,
            content_type,
            user_meta,
            authenticated_principal,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn append_stream_record_in_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        stream: &AppendStream,
        payload_object_ref: CoreObjectRef,
        payload_size: i64,
        content_type: Option<String>,
        user_meta: Option<JsonValue>,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<AppendStreamRecordMutation> {
        let permit = self
            .append_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        append_journal::append_stream_record_with_permit_in_partition_transaction(
            &self.storage,
            tenant_id,
            bucket_id,
            stream,
            payload_object_ref,
            payload_size,
            content_type,
            user_meta,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    pub async fn list_append_stream_records(
        &self,
        stream: &AppendStream,
        after_sequence: u64,
        limit: usize,
    ) -> Result<append_journal::AppendStreamRecordPage> {
        append_journal::list_append_stream_records_page(
            &self.storage,
            stream,
            after_sequence,
            limit,
        )
        .await
    }

    pub async fn append_stream_has_records(
        &self,
        stream: &AppendStream,
        transaction: Option<(&str, &str)>,
    ) -> Result<bool> {
        append_journal::append_stream_has_records(&self.storage, stream, transaction).await
    }

    pub async fn list_append_streams_page(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        after_stream_id: Option<&str>,
        limit: usize,
    ) -> Result<append_journal::AppendStreamPage> {
        append_journal::list_append_streams_page(
            &self.storage,
            tenant_id,
            bucket_id,
            after_stream_id,
            limit,
        )
        .await
    }

    pub async fn seal_append_stream(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        stream: &AppendStream,
        segment_hash: &str,
    ) -> Result<SealAppendStreamMutation> {
        let permit = self
            .append_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        append_journal::seal_append_stream_with_permit_in_partition(
            &self.storage,
            tenant_id,
            bucket_id,
            stream,
            segment_hash,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn seal_append_stream_in_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        stream: &AppendStream,
        segment_hash: &str,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<SealAppendStreamMutation> {
        let permit = self
            .append_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        append_journal::seal_append_stream_with_permit_in_partition_transaction(
            &self.storage,
            tenant_id,
            bucket_id,
            stream,
            segment_hash,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    pub async fn compare_and_swap_manifest(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        _bucket_name: &str,
        object_key: &str,
        expected_revision: i64,
        manifest: JsonValue,
        manifest_hash: &str,
    ) -> Result<Option<ManifestCasResult>> {
        let permit = self.manifest_cas_write_permit(tenant_id, bucket_id).await?;
        manifest_journal::compare_and_swap_manifest_with_permit(
            &self.storage,
            tenant_id,
            bucket_id,
            object_key,
            expected_revision,
            manifest,
            manifest_hash,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn compare_and_swap_manifest_in_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        _bucket_name: &str,
        object_key: &str,
        expected_revision: i64,
        manifest: JsonValue,
        manifest_hash: &str,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<Option<ManifestCasResult>> {
        let permit = self.manifest_cas_write_permit(tenant_id, bucket_id).await?;
        manifest_journal::compare_and_swap_manifest_with_permit_in_transaction(
            &self.storage,
            tenant_id,
            bucket_id,
            object_key,
            expected_revision,
            manifest,
            manifest_hash,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            transaction_principal,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write_authz_tuple(
        &self,
        tenant_id: i64,
        namespace: &str,
        object_id: &str,
        relation: &str,
        subject_kind: &str,
        subject_id: &str,
        caveat_hash: &str,
        operation: &str,
        written_by: &str,
        reason: &str,
    ) -> Result<AuthzTupleRecord> {
        let permit = self.authz_write_permit(tenant_id).await?;
        let record = authz_journal::write_authz_tuple_with_permit(
            &self.storage,
            authz_journal::AuthzTupleWrite {
                tenant_id,
                namespace,
                object_id,
                relation,
                subject_kind,
                subject_id,
                caveat_hash,
                operation,
                written_by,
                reason,
            },
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        self.enqueue_authz_materialization_after_write(tenant_id, record.revision)
            .await;
        Ok(record)
    }

    pub(crate) async fn materialize_authz_through_revision(
        &self,
        tenant_id: i64,
        revision: i64,
    ) -> Result<authz_journal::AuthzMaterializationOutcome> {
        let target_revision = u64::try_from(revision)
            .map_err(|_| anyhow!("authorization revision must be nonnegative"))?;
        let source_fence_token =
            authz_journal::latest_authz_journal_fence_token(&self.storage, tenant_id).await?;
        authz_journal::materialize_authz_derived_state_through_revision(
            &self.storage,
            tenant_id,
            target_revision,
            source_fence_token,
        )
        .await
    }

    pub async fn write_authz_tuple_batch(
        &self,
        tenant_id: i64,
        mutations: Vec<AuthzTupleBatchMutation>,
        written_by: &str,
    ) -> Result<Vec<AuthzTupleRecord>> {
        let permit = self.authz_write_permit(tenant_id).await?;
        let writes = mutations
            .iter()
            .map(|mutation| authz_journal::AuthzTupleWrite {
                tenant_id,
                namespace: mutation.namespace.as_str(),
                object_id: mutation.object_id.as_str(),
                relation: mutation.relation.as_str(),
                subject_kind: mutation.subject_kind.as_str(),
                subject_id: mutation.subject_id.as_str(),
                caveat_hash: mutation.caveat_hash.as_str(),
                operation: mutation.operation.as_str(),
                written_by,
                reason: mutation.reason.as_str(),
            })
            .collect();
        let records = authz_journal::write_authz_tuple_batch_with_permit(
            &self.storage,
            writes,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        if let Some(revision) = records.iter().map(|record| record.revision).max() {
            self.enqueue_authz_materialization_after_write(tenant_id, revision)
                .await;
        }
        Ok(records)
    }

    pub async fn replay_authz_tuple_batch(
        &self,
        tenant_id: i64,
        mutations: &[AuthzTupleBatchMutation],
        written_by: &str,
        options: &AuthzTupleBatchWriteOptions,
    ) -> Result<Option<AuthzTupleBatchWriteOutcome>> {
        let writes = authz_tuple_batch_writes(tenant_id, mutations, written_by);
        authz_journal::replay_authz_tuple_batch(&self.storage, &writes, options).await
    }

    pub async fn write_authz_tuple_batch_conditionally(
        &self,
        tenant_id: i64,
        mutations: Vec<AuthzTupleBatchMutation>,
        written_by: &str,
        options: &AuthzTupleBatchWriteOptions,
    ) -> Result<AuthzTupleBatchWriteOutcome> {
        let permit = self.authz_write_permit(tenant_id).await?;
        let writes = authz_tuple_batch_writes(tenant_id, &mutations, written_by);
        let outcome = authz_journal::write_authz_tuple_batch_conditionally_with_permit(
            &self.storage,
            writes,
            options,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        if !outcome.replayed
            && let Some(revision) = outcome.records.iter().map(|record| record.revision).max()
        {
            self.enqueue_authz_materialization_after_write(tenant_id, revision)
                .await;
        }
        Ok(outcome)
    }

    async fn enqueue_authz_materialization_after_write(&self, tenant_id: i64, revision: i64) {
        let Ok(target_revision) = u64::try_from(revision) else {
            tracing::warn!(
                tenant_id,
                revision,
                "skipping authz materialization enqueue for negative revision"
            );
            return;
        };
        if let Err(error) = self
            .enqueue_authz_materialization(tenant_id, target_revision)
            .await
        {
            tracing::warn!(
                tenant_id,
                target_revision,
                %error,
                "failed to enqueue authz materialization task after tuple write"
            );
        }
    }

    pub async fn check_authz_tuple(
        &self,
        tenant_id: i64,
        namespace: &str,
        object_id: &str,
        relation: &str,
        subject_kind: &str,
        subject_id: &str,
        caveat_hash: &str,
    ) -> Result<Option<AuthzTupleRecord>> {
        authz_journal::check_authz_tuple(
            &self.storage,
            tenant_id,
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
        )
        .await
    }

    pub async fn check_authz_tuple_at_revision(
        &self,
        tenant_id: i64,
        namespace: &str,
        object_id: &str,
        relation: &str,
        subject_kind: &str,
        subject_id: &str,
        caveat_hash: &str,
        revision: i64,
    ) -> Result<Option<AuthzTupleRecord>> {
        authz_journal::check_authz_tuple_at_revision(
            &self.storage,
            tenant_id,
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
            revision,
        )
        .await
    }

    pub async fn list_authz_tuple_log(
        &self,
        tenant_id: i64,
        after_revision: i64,
        namespace: &str,
        limit: i32,
    ) -> Result<Vec<AuthzTupleRecord>> {
        authz_journal::list_authz_tuple_log(
            &self.storage,
            tenant_id,
            after_revision,
            namespace,
            if limit == 0 {
                1000
            } else {
                limit.max(1) as usize
            },
        )
        .await
    }
}

fn authz_tuple_batch_writes<'a>(
    tenant_id: i64,
    mutations: &'a [AuthzTupleBatchMutation],
    written_by: &'a str,
) -> Vec<authz_journal::AuthzTupleWrite<'a>> {
    mutations
        .iter()
        .map(|mutation| authz_journal::AuthzTupleWrite {
            tenant_id,
            namespace: mutation.namespace.as_str(),
            object_id: mutation.object_id.as_str(),
            relation: mutation.relation.as_str(),
            subject_kind: mutation.subject_kind.as_str(),
            subject_id: mutation.subject_id.as_str(),
            caveat_hash: mutation.caveat_hash.as_str(),
            operation: mutation.operation.as_str(),
            written_by,
            reason: mutation.reason.as_str(),
        })
        .collect()
}
