use crate::anvil_api::bucket_service_server::BucketService;
use crate::anvil_api::*;
use crate::bucket_journal::BucketJournalMutation;
use crate::{
    AppState, auth, bucket_journal, mesh_lifecycle,
    permissions::AnvilAction,
    services::watch_envelope::{self, WatchEnvelopeParts},
    validation,
};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

fn bucket_transaction_id(options: Option<&WriteOptions>) -> Result<Option<&str>, Status> {
    crate::services::saga_reserved::write_options_transaction_id(options)
}

#[tonic::async_trait]
impl BucketService for AppState {
    type WatchBucketMetadataStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchBucketMetadataResponse, Status>> + Send>,
    >;

    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        tracing::debug!("[service] ENTERING create_bucket");
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;

        let req = request.get_ref();
        let transaction_id = bucket_transaction_id(req.options.as_ref())?;
        let bucket = if let Some(transaction_id) = transaction_id {
            self.create_bucket_in_transaction(claims, req, transaction_id)
                .await?
        } else {
            let bucket = self
                .bucket_manager
                .create_bucket(claims, &req.bucket_name, &req.region)
                .await?;
            self.publish_bucket_metadata_event(claims.tenant_id, &bucket, "create", false)
                .await?;
            bucket
        };

        tracing::debug!("[service] EXITING create_bucket");
        Ok(Response::new(CreateBucketResponse {
            bucket_id: bucket.id,
        }))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();
        let transaction_id = bucket_transaction_id(req.options.as_ref())?;
        if let Some(transaction_id) = transaction_id {
            self.delete_bucket_in_transaction(claims, req, transaction_id)
                .await?;
        } else {
            let bucket = self
                .bucket_manager
                .delete_bucket(claims, &req.bucket_name)
                .await?;
            self.publish_bucket_metadata_event(claims.tenant_id, &bucket, "delete", true)
                .await?;
        }

        Ok(Response::new(DeleteBucketResponse {}))
    }

    async fn list_buckets(
        &self,
        request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        tracing::debug!("[service] ENTERING list_buckets");
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();

        self.bucket_manager.authorize_bucket_list(&claims).await?;
        let page_size = crate::services::collection_cursor::page_size(req.page.as_ref())?;
        let revision =
            bucket_journal::current_bucket_collection_revision(&self.storage, claims.tenant_id)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
        let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
        let binding = crate::services::collection_cursor::CollectionCursorBinding {
            service_method: "anvil.BucketService/ListBuckets",
            filters: &[],
            principal_scope: &principal_scope,
            page_size,
            revision: &revision,
            sort: "bucket_name.asc",
        };
        let position = crate::services::collection_cursor::decode_page_token(
            req.page.as_ref(),
            &binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        let after_tuple_key =
            crate::services::collection_cursor::decode_binary_position(position.as_deref())?;
        let bucket_page = bucket_journal::page_current_buckets(
            &self.storage,
            claims.tenant_id,
            &revision,
            after_tuple_key.as_deref(),
            page_size,
        )
        .await
        .map_err(|error| Status::aborted(error.to_string()))?;
        let next_page_token = bucket_page
            .next_tuple_key
            .as_deref()
            .map(crate::services::collection_cursor::encode_binary_position)
            .transpose()?
            .map(|position| {
                crate::services::collection_cursor::encode_next_page_token(
                    &position,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })
            .transpose()?
            .unwrap_or_default();
        let response_buckets = bucket_page
            .buckets
            .into_iter()
            .map(|bucket| crate::anvil_api::Bucket {
                name: bucket.name,
                creation_date: bucket.created_at.to_string(),
                region: bucket.region,
                is_public_read: bucket.is_public_read,
                deleted: false,
                bucket_id: bucket.id,
            })
            .collect::<Vec<_>>();

        tracing::debug!(
            "[service] EXITING list_buckets, found {} buckets",
            response_buckets.len()
        );
        Ok(Response::new(ListBucketsResponse {
            buckets: response_buckets,
            page: Some(PageResponse { next_page_token }),
        }))
    }

    async fn get_bucket_policy(
        &self,
        request: Request<GetBucketPolicyRequest>,
    ) -> Result<Response<GetBucketPolicyResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        let policy = self
            .bucket_manager
            .get_bucket_policy(claims, &req.bucket_name)
            .await?;

        Ok(Response::new(GetBucketPolicyResponse {
            policy_json: policy.to_string(),
        }))
    }

    async fn put_bucket_policy(
        &self,
        request: Request<PutBucketPolicyRequest>,
    ) -> Result<Response<PutBucketPolicyResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();
        let transaction_id = bucket_transaction_id(req.options.as_ref())?;

        // Bucket policy is projected into Anvil's native public-read flag; all
        // object-level enforcement still flows through the normal authorisation path.
        let policy: serde_json::Value = serde_json::from_str(&req.policy_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid policy JSON: {}", e)))?;
        let is_public_read = policy["is_public_read"].as_bool().unwrap_or(false);

        if let Some(transaction_id) = transaction_id {
            self.put_bucket_policy_in_transaction(claims, req, is_public_read, transaction_id)
                .await?;
        } else {
            let bucket = self
                .bucket_manager
                .set_bucket_public_access(claims, &req.bucket_name, is_public_read)
                .await?;
            self.publish_bucket_metadata_event(claims.tenant_id, &bucket, "policy_update", false)
                .await?;
        }

        Ok(Response::new(PutBucketPolicyResponse {}))
    }

    async fn watch_bucket_metadata(
        &self,
        request: Request<WatchBucketMetadataRequest>,
    ) -> Result<Response<Self::WatchBucketMetadataStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if !req.bucket_name.is_empty() && !validation::is_valid_bucket_name(&req.bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        let resource = if req.bucket_name.is_empty() {
            "*"
        } else {
            req.bucket_name.as_str()
        };
        crate::access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::BucketWatch,
            resource,
        )
        .await?;
        let after_cursor = i64::try_from(req.after_cursor)
            .map_err(|_| Status::invalid_argument("after_cursor exceeds supported range"))?;
        let stream_id = bucket_journal::tenant_bucket_metadata_stream_id(claims.tenant_id);
        let mut live = self.storage.subscribe_stream(&stream_id);
        let storage = self.storage.clone();
        let tenant_id = claims.tenant_id;
        let bucket_name = req.bucket_name;

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page = match bucket_journal::list_bucket_metadata_event_page(
                        &storage,
                        tenant_id,
                        &bucket_name,
                        last_cursor,
                        256,
                    )
                    .await
                    {
                        Ok(page) => page,
                        Err(error) => {
                            let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                            return;
                        }
                    };
                    let previous_cursor = last_cursor;
                    for event in page.events {
                        if tx
                            .send(bucket_metadata_event_response(&event))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    last_cursor = page.next_cursor;
                    if !page.has_more || last_cursor == previous_cursor {
                        break;
                    }
                }

                match live.recv().await {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchBucketMetadataStream
        ))
    }
}

impl AppState {
    async fn create_bucket_in_transaction(
        &self,
        claims: &auth::Claims,
        req: &CreateBucketRequest,
        transaction_id: &str,
    ) -> Result<crate::persistence::Bucket, Status> {
        if !validation::is_valid_bucket_name(&req.bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        crate::access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::BucketCreate,
            &req.bucket_name,
        )
        .await?;
        mesh_lifecycle::ensure_new_writable_placement(
            &self.storage,
            &req.region,
            &self.config.cell_id,
            &self.config.node_id,
        )
        .await
        .map_err(|err| Status::failed_precondition(err.to_string()))?;
        if bucket_journal::read_current_bucket(&self.storage, claims.tenant_id, &req.bucket_name)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .is_some()
        {
            return Err(Status::already_exists(
                "A bucket with that name already exists.",
            ));
        }
        let bucket = crate::persistence::Bucket {
            id: bucket_journal::next_bucket_id(&self.storage)
                .await
                .map_err(|err| Status::internal(err.to_string()))?,
            tenant_id: claims.tenant_id,
            name: req.bucket_name.clone(),
            region: req.region.clone(),
            created_at: chrono::Utc::now(),
            is_public_read: false,
        };
        self.stage_bucket_metadata_transaction(
            claims,
            &bucket,
            BucketJournalMutation::Create,
            transaction_id,
        )
        .await?;
        Ok(bucket)
    }

    async fn delete_bucket_in_transaction(
        &self,
        claims: &auth::Claims,
        req: &DeleteBucketRequest,
        transaction_id: &str,
    ) -> Result<crate::persistence::Bucket, Status> {
        crate::access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::BucketDelete,
            &req.bucket_name,
        )
        .await?;
        let bucket =
            bucket_journal::read_current_bucket(&self.storage, claims.tenant_id, &req.bucket_name)
                .await
                .map_err(|err| Status::internal(err.to_string()))?
                .ok_or_else(|| Status::not_found("Bucket not found"))?;
        if self
            .persistence
            .bucket_has_retained_objects_or_uploads(bucket.id)
            .await
            .map_err(|err| Status::internal(err.to_string()))?
        {
            return Err(Status::failed_precondition("Bucket not empty"));
        }
        self.stage_bucket_metadata_transaction(
            claims,
            &bucket,
            BucketJournalMutation::Delete,
            transaction_id,
        )
        .await?;
        Ok(bucket)
    }

    async fn put_bucket_policy_in_transaction(
        &self,
        claims: &auth::Claims,
        req: &PutBucketPolicyRequest,
        is_public_read: bool,
        transaction_id: &str,
    ) -> Result<crate::persistence::Bucket, Status> {
        crate::access_control::require_action(
            &self.storage,
            &self.persistence,
            claims,
            AnvilAction::BucketWrite,
            &req.bucket_name,
        )
        .await?;
        let mut bucket =
            bucket_journal::read_current_bucket(&self.storage, claims.tenant_id, &req.bucket_name)
                .await
                .map_err(|err| Status::internal(err.to_string()))?
                .ok_or_else(|| Status::not_found("Bucket not found"))?;
        bucket.is_public_read = is_public_read;
        self.stage_bucket_metadata_transaction(
            claims,
            &bucket,
            BucketJournalMutation::Update,
            transaction_id,
        )
        .await?;
        Ok(bucket)
    }

    async fn stage_bucket_metadata_transaction(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        mutation: BucketJournalMutation,
        transaction_id: &str,
    ) -> Result<(), Status> {
        let principal = crate::object_manager::transaction_principal_from_claims(claims);
        bucket_journal::stage_bucket_mutation_in_transaction(
            &self.storage,
            bucket,
            mutation,
            transaction_id,
            &principal,
        )
        .await
        .map_err(bucket_core_store_status)
    }

    async fn publish_bucket_metadata_event(
        &self,
        tenant_id: i64,
        bucket: &crate::persistence::Bucket,
        event_type: &str,
        deleted: bool,
    ) -> Result<crate::persistence::BucketMetadataEvent, Status> {
        let event =
            bucket_journal::latest_bucket_metadata_event(&self.storage, tenant_id, &bucket.name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::internal("Bucket metadata journal event not found"))?;
        if event.event_type != event_type {
            tracing::debug!(
                expected = event_type,
                actual = event.event_type,
                deleted,
                "bucket metadata journal event type differs from service hint"
            );
        }
        Ok(event)
    }
}

fn bucket_metadata_event_response(
    event: &crate::persistence::BucketMetadataEvent,
) -> Result<WatchBucketMetadataResponse, Status> {
    let cursor = u64::try_from(event.id).map_err(|_| Status::internal("Invalid watch cursor"))?;
    let emitted_at = event.created_at.to_string();
    let payload_hash = watch_envelope::payload_hash(&event.bucket_metadata);
    Ok(WatchBucketMetadataResponse {
        cursor,
        event_type: event.event_type.clone(),
        bucket: Some(bucket_from_metadata(&event.bucket_metadata)?),
        emitted_at: emitted_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "bucket_metadata",
            partition_family: "bucket_metadata",
            partition_id: event.bucket_name.clone(),
            cursor: event.id as u128,
            mutation_id: event.mutation_id.to_string(),
            record_kind: "bucket_metadata".to_string(),
            object_ref: event.bucket_name.clone(),
            authz_revision: 0,
            index_generation: 0,
            personaldb_log_index: 0,
            payload_hash,
            emitted_at,
        })),
    })
}

fn bucket_from_metadata(value: &JsonValue) -> Result<Bucket, Status> {
    Ok(Bucket {
        bucket_id: value
            .get("bucket_id")
            .and_then(JsonValue::as_i64)
            .ok_or_else(|| Status::internal("Malformed bucket metadata event"))?,
        name: json_string_field(value, "name")?,
        creation_date: json_string_field(value, "creation_date")?,
        region: json_string_field(value, "region")?,
        is_public_read: value
            .get("is_public_read")
            .and_then(JsonValue::as_bool)
            .ok_or_else(|| Status::internal("Malformed bucket metadata event"))?,
        deleted: value
            .get("deleted")
            .and_then(JsonValue::as_bool)
            .ok_or_else(|| Status::internal("Malformed bucket metadata event"))?,
    })
}

fn json_string_field(value: &JsonValue, name: &str) -> Result<String, Status> {
    value
        .get(name)
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| Status::internal("Malformed bucket metadata event"))
}

fn bucket_core_store_status(error: anyhow::Error) -> Status {
    if let Some(status) = crate::services::core_store_status::availability_status(&error) {
        return status;
    }
    let message = error.to_string();
    if message.contains("TransactionNotFound") {
        Status::not_found("TransactionNotFound")
    } else if message.contains("TransactionPrincipalMismatch") {
        Status::permission_denied("TransactionPrincipalMismatch")
    } else if message.contains("TransactionScopeMismatch") {
        Status::failed_precondition("TransactionScopeMismatch")
    } else if message.contains("TransactionExpired")
        || message.contains("TransactionRolledBack")
        || message.contains("TransactionAlreadyCommitted")
        || message.contains("TransactionNotOpen")
        || message.contains("TransactionNotCommittable")
    {
        Status::failed_precondition(message)
    } else if message.contains("TransactionConflict") {
        Status::aborted("TransactionConflict")
    } else if message.contains("idempotency conflict") {
        Status::already_exists("TransactionConflict")
    } else if message.contains("must not be empty")
        || message.contains("must be a sha256 hash")
        || message.contains("root key hash mismatch")
        || message.contains("contains an invalid component")
    {
        Status::invalid_argument(message)
    } else {
        Status::internal(message)
    }
}
