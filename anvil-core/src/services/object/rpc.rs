use super::*;
use crate::object_manager;

pub(super) fn native_transaction_id(
    context: Option<&NativeMutationContext>,
) -> Result<Option<&str>, Status> {
    crate::services::saga_reserved::native_context_transaction_id(context)
}

fn native_route_tenant_id(metadata: &tonic::metadata::MetadataMap) -> Result<Option<i64>, Status> {
    let Some(raw) = metadata.get("x-anvil-tenant-id") else {
        return Ok(None);
    };
    let value = raw
        .to_str()
        .map_err(|_| Status::invalid_argument("Invalid x-anvil-tenant-id route metadata"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(
            "Empty x-anvil-tenant-id route metadata",
        ));
    }
    trimmed
        .parse::<i64>()
        .map(Some)
        .map_err(|_| Status::invalid_argument("Invalid x-anvil-tenant-id route metadata"))
}

fn object_storage_class(object: &crate::persistence::Object) -> String {
    object.storage_class.clone().unwrap_or_default()
}

pub(super) fn write_state_for_transaction(transaction_id: Option<&str>) -> i32 {
    if transaction_id.is_some() {
        WriteState::Staged as i32
    } else {
        WriteState::Finalised as i32
    }
}

pub(super) fn object_write_visibility(
    context: Option<&NativeMutationContext>,
) -> Result<ObjectWriteVisibility, Status> {
    let Some(options) = context.and_then(|context| context.write_visibility.as_ref()) else {
        return Ok(ObjectWriteVisibility::default());
    };
    Ok(ObjectWriteVisibility {
        indexes: match options.indexes {
            0 => IndexMaintenanceVisibility::Deferred,
            1 => IndexMaintenanceVisibility::Enqueued,
            2 => IndexMaintenanceVisibility::CaughtUp,
            _ => return Err(Status::invalid_argument("Invalid index maintenance mode")),
        },
        watches: match options.watches {
            0 => WatchVisibility::Deferred,
            1 => WatchVisibility::Published,
            _ => return Err(Status::invalid_argument("Invalid watch visibility mode")),
        },
        authz_materialization: match options.authz_materialization {
            0 => AuthzMaterializationVisibility::InheritedOk,
            1 => AuthzMaterializationVisibility::Materialized,
            _ => {
                return Err(Status::invalid_argument(
                    "Invalid authz materialization mode",
                ));
            }
        },
        boundary_extraction: match options.boundary_extraction {
            0 => BoundaryExtractionVisibility::HintsOnly,
            1 => BoundaryExtractionVisibility::PayloadNow,
            _ => return Err(Status::invalid_argument("Invalid boundary extraction mode")),
        },
        index_policy_snapshot: match options.index_policy_snapshot {
            0 => IndexPolicySnapshotVisibility::Cached,
            1 => IndexPolicySnapshotVisibility::Exact,
            _ => {
                return Err(Status::invalid_argument(
                    "Invalid index policy snapshot mode",
                ));
            }
        },
        authz_revision: match options.authz_revision {
            0 => AuthzRevisionVisibility::CurrentKnown,
            1 => AuthzRevisionVisibility::FenceExact,
            _ => return Err(Status::invalid_argument("Invalid authz revision mode")),
        },
    })
}

#[tonic::async_trait]
impl ObjectService for AppState {
    type GetObjectStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<GetObjectResponse, Status>> + Send>,
    >;
    type WatchPrefixStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchPrefixResponse, Status>> + Send>,
    >;
    type TailAppendStreamStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<TailAppendStreamResponse, Status>> + Send>,
    >;

    async fn put_object(
        &self,
        request: Request<tonic::Streaming<PutObjectRequest>>,
    ) -> Result<Response<PutObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;

        let mut stream = request.into_inner();

        let metadata = match stream.next().await {
            Some(Ok(chunk)) => match chunk.data {
                Some(put_object_request::Data::Metadata(metadata)) => metadata,
                _ => return Err(Status::invalid_argument("First chunk must be metadata")),
            },
            _ => return Err(Status::invalid_argument("Empty stream")),
        };
        if let Some(target) = native_put_route_target(self, &claims, &metadata).await? {
            let response = proxy_native_put(self, &target, &claims, metadata, stream).await?;
            return Ok(Response::new(response));
        }

        let data_stream = stream.map(native_put_data_chunk);
        let response = execute_native_put(self, claims, metadata, data_stream).await?;
        Ok(Response::new(response))
    }

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let route_tenant_id = native_route_tenant_id(request.metadata())?;
        let claims = request.extensions().get::<auth::Claims>().cloned();
        let req = request.into_inner();
        let consistency = object_read_consistency(req.consistency.as_ref())?;

        if let Some(stream) =
            proxy_get_object_if_needed(self, claims.as_ref(), route_tenant_id, &req, consistency)
                .await?
        {
            return Ok(Response::new(stream));
        }

        let result = self
            .object_manager
            .get_object_with_link_mode_for_tenant(
                claims,
                route_tenant_id,
                req.bucket_name,
                req.object_key,
                parse_optional_version_id(req.version_id.as_deref())?,
                req.range.map(|range| crate::core_store::CoreByteRange {
                    start: range.start,
                    end_exclusive: range.end_exclusive,
                }),
                crate::object_manager::ObjectLinkReadMode::Follow,
                consistency,
            )
            .await?;
        let object = result.object;
        let mut data_stream = result.stream;
        let mut logical_offset = result.range_start;

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let info = ObjectInfo {
                content_type: object.content_type.clone().unwrap_or_default(),
                content_length: object.size,
                version_id: object.version_id.to_string(),
                user_metadata_json: json_object_string(object.user_meta.as_ref()),
                storage_class: object_storage_class(&object),
            };
            if tx
                .send(Ok(GetObjectResponse {
                    data: Some(get_object_response::Data::Metadata(info)),
                    logical_offset: 0,
                    trace_id: String::new(),
                }))
                .await
                .is_err()
            {
                return; // Client disconnected
            }

            while let Some(chunk_result) = data_stream.next().await {
                let chunk = match chunk_result {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        let _ = tx.send(Err(error)).await;
                        break;
                    }
                };
                if tx
                    .send(Ok(GetObjectResponse {
                        data: Some(get_object_response::Data::Chunk(chunk.to_vec())),
                        logical_offset,
                        trace_id: String::new(),
                    }))
                    .await
                    .is_err()
                {
                    break; // Client disconnected
                }
                logical_offset = logical_offset.saturating_add(chunk.len() as u64);
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::GetObjectStream
        ))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();
        validate_native_mutation_context(
            self,
            claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        let transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
        let write_visibility = object_write_visibility(req.mutation_context.as_ref())?;
        let target =
            NativeIdempotencyTarget::new("DeleteObject", &req.bucket_name, &req.object_key)
                .with_parameters(serde_json::json!({
                    "version_id": req.version_id.as_deref().unwrap_or("")
                }));
        let (attempt, replay) = begin_native_mutation::<DeleteObjectResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims,
            AnvilAction::ObjectDelete,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectDelete,
        )
        .await?;

        let transaction_principal = transaction_id
            .map(|_| crate::object_manager::transaction_principal_from_claims(claims));
        let deleted =
            if let Some(version_id) = parse_optional_version_id(req.version_id.as_deref())? {
                self.object_manager
                    .delete_object_version(
                        claims,
                        &req.bucket_name,
                        &req.object_key,
                        version_id,
                        transaction_id,
                        transaction_principal.as_deref(),
                        write_visibility,
                    )
                    .await?
            } else {
                self.object_manager
                    .delete_object(
                        claims,
                        &req.bucket_name,
                        &req.object_key,
                        transaction_id,
                        transaction_principal.as_deref(),
                        write_visibility,
                    )
                    .await?
            };
        let watch_cursor = if transaction_id.is_some() || !write_visibility.requires_watch_visible()
        {
            0
        } else {
            object_watch_cursor(self, &deleted).await?
        };

        let response = DeleteObjectResponse {
            version_id: deleted.version_id.to_string(),
            mutation_id: deleted.mutation_id.to_string(),
            payload_hash: deleted.content_hash,
            record_hash: deleted.record_hash,
            authz_revision: u64::try_from(deleted.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
            index_policy_snapshot: deleted.index_policy_snapshot,
            watch_cursor,
            delete_marker: deleted.deleted_at.is_some(),
            write_state: write_state_for_transaction(transaction_id),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<HeadObjectResponse>, Status> {
        let request_id = request
            .extensions()
            .get::<crate::middleware::AnvilRequestId>()
            .cloned();
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();
        let consistency = object_read_consistency(req.consistency.as_ref())?;

        if let Some(response) =
            proxy_head_object_if_needed(self, claims, request_id.as_ref(), req, consistency).await?
        {
            return Ok(Response::new(response));
        }

        let object = self
            .object_manager
            .head_object_with_consistency(
                Some(claims.clone()),
                &req.bucket_name,
                &req.object_key,
                parse_optional_version_id(req.version_id.as_deref())?,
                consistency,
            )
            .await?;

        let storage_class = object_storage_class(&object);
        Ok(Response::new(HeadObjectResponse {
            etag: object.etag,
            size: object.size,
            last_modified: object.created_at.to_string(),
            version_id: object.version_id.to_string(),
            mutation_id: object.mutation_id.to_string(),
            record_hash: object.record_hash,
            authz_revision: u64::try_from(object.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
            index_policy_snapshot: object.index_policy_snapshot,
            content_type: object.content_type.unwrap_or_default(),
            user_metadata_json: json_object_string(object.user_meta.as_ref()),
            storage_class,
        }))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();
        let consistency_proto = effective_read_consistency(req.consistency.as_ref());
        let consistency = object_read_consistency(Some(&consistency_proto))?;
        let limit = if req.max_keys <= 0 {
            1000
        } else {
            req.max_keys.min(1000)
        } as u32;
        let token_binding = ObjectPageTokenBinding::for_objects(
            claims,
            &req.bucket_name,
            &req.prefix,
            &req.delimiter,
            limit,
            &consistency_proto,
        );
        let token = ObjectPageToken::decode(
            &req.page_token,
            &token_binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        if token.is_some() && !req.start_after.is_empty() {
            return Err(Status::invalid_argument("PageTokenScopeMismatch"));
        }
        let effective_start_after = token
            .as_ref()
            .map(|token| token.last_key.as_str())
            .unwrap_or(req.start_after.as_str());
        let source_limit = i32::try_from(limit.saturating_add(1))
            .map_err(|_| Status::internal("Object listing limit exceeds i32"))?;

        let (mut objects, common_prefixes) = self
            .object_manager
            .list_objects_for_tenant(
                Some(claims.clone()),
                None,
                &req.bucket_name,
                &req.prefix,
                effective_start_after,
                source_limit,
                &req.delimiter,
                consistency,
            )
            .await?;

        let next_page_token = if objects.len() > limit as usize {
            let last_key = objects
                .get(limit.saturating_sub(1) as usize)
                .map(|object| object.key.clone())
                .unwrap_or_default();
            objects.truncate(limit as usize);
            ObjectPageToken::for_object_key(&token_binding, last_key)
                .encode(self.config.jwt_secret.as_bytes())?
        } else {
            String::new()
        };

        let response_objects = objects
            .into_iter()
            .map(|o| {
                let storage_class = object_storage_class(&o);
                crate::anvil_api::ObjectSummary {
                    key: o.key,
                    size: o.size,
                    last_modified: o.created_at.to_string(),
                    etag: o.etag,
                    content_type: o.content_type.unwrap_or_default(),
                    user_metadata_json: json_object_string(o.user_meta.as_ref()),
                    storage_class,
                }
            })
            .collect();

        Ok(Response::new(ListObjectsResponse {
            objects: response_objects,
            common_prefixes,
            next_page_token,
        }))
    }

    async fn list_object_versions(
        &self,
        request: Request<ListObjectVersionsRequest>,
    ) -> Result<Response<ListObjectVersionsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();
        let consistency_proto = effective_read_consistency(req.consistency.as_ref());
        let consistency = object_read_consistency(Some(&consistency_proto))?;
        let limit = if req.max_keys <= 0 {
            1000
        } else {
            req.max_keys.min(1000)
        } as u32;
        let token_binding = ObjectPageTokenBinding::for_versions(
            claims,
            &req.bucket_name,
            &req.prefix,
            limit,
            &consistency_proto,
        );
        let token = ObjectPageToken::decode(
            &req.page_token,
            &token_binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        if token.is_some() && (!req.key_marker.is_empty() || !req.version_id_marker.is_empty()) {
            return Err(Status::invalid_argument("PageTokenScopeMismatch"));
        }
        let effective_key_marker = token
            .as_ref()
            .map(|token| token.last_key.as_str())
            .unwrap_or(req.key_marker.as_str());
        let effective_version_marker = token
            .as_ref()
            .map(|token| token.last_version_id.as_str())
            .unwrap_or(req.version_id_marker.as_str());
        let source_limit = i32::try_from(limit)
            .map_err(|_| Status::internal("Object version listing limit exceeds i32"))?;

        let versions = self
            .object_manager
            .list_object_versions_for_tenant(
                Some(claims.clone()),
                None,
                &req.bucket_name,
                &req.prefix,
                effective_key_marker,
                effective_version_marker,
                source_limit,
                consistency,
            )
            .await?;
        let next_key_marker = versions.next_key_marker.clone().unwrap_or_default();
        let next_version_id_marker = versions
            .next_version_id_marker
            .map(|marker| marker.to_string())
            .unwrap_or_default();
        let next_page_token = if versions.is_truncated {
            ObjectPageToken::for_version_marker(
                &token_binding,
                next_key_marker.clone(),
                next_version_id_marker.clone(),
            )
            .encode(self.config.jwt_secret.as_bytes())?
        } else {
            String::new()
        };
        let response_versions = versions
            .versions
            .into_iter()
            .map(|version| {
                let object = version.object;
                let storage_class = object_storage_class(&object);
                crate::anvil_api::ObjectVersionSummary {
                    key: object.key,
                    version_id: object.version_id.to_string(),
                    size: object.size,
                    last_modified: object.created_at.to_string(),
                    etag: object.etag,
                    is_delete_marker: version.is_delete_marker,
                    is_latest: version.is_latest,
                    content_type: object.content_type.unwrap_or_default(),
                    user_metadata_json: json_object_string(object.user_meta.as_ref()),
                    storage_class,
                }
            })
            .collect();

        Ok(Response::new(ListObjectVersionsResponse {
            versions: response_versions,
            is_truncated: versions.is_truncated,
            next_key_marker,
            next_version_id_marker,
            next_page_token,
        }))
    }

    async fn copy_object(
        &self,
        request: Request<CopyObjectRequest>,
    ) -> Result<Response<CopyObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_native_mutation_context(
            self,
            &claims,
            &req.destination_bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        let transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
        let target = NativeIdempotencyTarget::new(
            "CopyObject",
            &req.destination_bucket_name,
            &req.destination_object_key,
        )
        .with_parameters(serde_json::json!({
            "source_bucket_name": req.source_bucket_name.clone(),
            "source_object_key": req.source_object_key.clone(),
            "source_version_id": req.source_version_id.as_deref().unwrap_or("")
        }));
        let (attempt, replay) = begin_native_mutation::<CopyObjectResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.destination_bucket_name,
            &req.destination_object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;

        let object = self
            .object_manager
            .copy_object(
                claims,
                &req.source_bucket_name,
                &req.source_object_key,
                parse_optional_version_id(req.source_version_id.as_deref())?,
                &req.destination_bucket_name,
                &req.destination_object_key,
                transaction_id,
            )
            .await?;
        let watch_cursor = if transaction_id.is_some() {
            0
        } else {
            object_watch_cursor(self, &object).await?
        };
        let authz_revision = object_authz_revision(&object)?;

        let response = CopyObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
            write_state: write_state_for_transaction(transaction_id),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn compose_object(
        &self,
        request: Request<ComposeObjectRequest>,
    ) -> Result<Response<ComposeObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_native_mutation_context(
            self,
            &claims,
            &req.destination_bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        let transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
        let target_sources = req
            .sources
            .iter()
            .map(|source| {
                serde_json::json!({
                    "bucket_name": source.bucket_name.clone(),
                    "object_key": source.object_key.clone(),
                    "version_id": source.version_id.as_deref().unwrap_or("")
                })
            })
            .collect::<Vec<_>>();
        let target = NativeIdempotencyTarget::new(
            "ComposeObject",
            &req.destination_bucket_name,
            &req.destination_object_key,
        )
        .with_parameters(serde_json::json!({ "sources": target_sources }));
        let (attempt, replay) = begin_native_mutation::<ComposeObjectResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.destination_bucket_name,
            &req.destination_object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;

        let mut sources = Vec::with_capacity(req.sources.len());
        for source in req.sources {
            sources.push(crate::object_manager::ComposeSource {
                bucket_name: source.bucket_name,
                object_key: source.object_key,
                version_id: parse_optional_version_id(source.version_id.as_deref())?,
            });
        }

        let object = self
            .object_manager
            .compose_object(
                claims,
                sources,
                &req.destination_bucket_name,
                &req.destination_object_key,
                transaction_id,
            )
            .await?;
        let watch_cursor = if transaction_id.is_some() {
            0
        } else {
            object_watch_cursor(self, &object).await?
        };
        let authz_revision = object_authz_revision(&object)?;

        let response = ComposeObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
            write_state: write_state_for_transaction(transaction_id),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn patch_json_object(
        &self,
        request: Request<PatchJsonObjectRequest>,
    ) -> Result<Response<PatchJsonObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        let transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
        let target = NativeIdempotencyTarget::new(
            "PatchJsonObject",
            &req.bucket_name,
            &req.object_key,
        )
        .with_parameters(serde_json::json!({
            "base_version_id": req.base_version_id.as_deref().unwrap_or(""),
            "merge_patch_hash": blake3::hash(req.merge_patch_json.as_bytes()).to_hex().to_string()
        }));
        let (attempt, replay) = begin_native_mutation::<PatchJsonObjectResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        enforce_write_precondition(self, &claims, req.precondition.as_ref()).await?;

        let object = self
            .object_manager
            .patch_json_object(
                claims,
                &req.bucket_name,
                &req.object_key,
                parse_optional_version_id(req.base_version_id.as_deref())?,
                &req.merge_patch_json,
                transaction_id,
            )
            .await?;
        let watch_cursor = if transaction_id.is_some() {
            0
        } else {
            object_watch_cursor(self, &object).await?
        };
        let authz_revision = object_authz_revision(&object)?;

        let response = PatchJsonObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
            write_state: write_state_for_transaction(transaction_id),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn compare_and_swap_manifest(
        &self,
        request: Request<CompareAndSwapManifestRequest>,
    ) -> Result<Response<CompareAndSwapManifestResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        let transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
        let target = NativeIdempotencyTarget::new(
            "CompareAndSwapManifest",
            &req.bucket_name,
            &req.manifest_key,
        )
        .with_parameters(serde_json::json!({
            "expected_revision": req.expected_revision,
            "manifest_hash": blake3::hash(req.manifest_json.as_bytes()).to_hex().to_string()
        }));
        let (attempt, replay) = begin_native_mutation::<CompareAndSwapManifestResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.manifest_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        enforce_write_precondition(self, &claims, req.precondition.as_ref()).await?;
        let transaction_principal =
            transaction_id.map(|_| object_manager::transaction_principal_from_claims(&claims));
        let result = self
            .object_manager
            .compare_and_swap_manifest(
                &claims,
                &req.bucket_name,
                &req.manifest_key,
                req.expected_revision,
                &req.manifest_json,
                transaction_id,
                transaction_principal.as_deref(),
            )
            .await?;
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        let response = CompareAndSwapManifestResponse {
            revision: result.revision,
            manifest_hash: result.manifest_hash.clone(),
            version_id: result.revision.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.manifest_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: if transaction_id.is_some() {
                0
            } else {
                result.receipt.watch_cursor
            },
            write_state: write_state_for_transaction(transaction_id),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn watch_prefix(
        &self,
        request: Request<WatchPrefixRequest>,
    ) -> Result<Response<Self::WatchPrefixStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let tenant_id = claims.tenant_id;
        let prefix = req.prefix.clone();
        let after_cursor = i64::try_from(req.after_cursor)
            .map_err(|_| Status::invalid_argument("after_cursor exceeds supported range"))?;
        let bucket_id = self
            .object_manager
            .resolve_prefix_watch_scope(claims, &req.bucket_name, &req.prefix)
            .await?;
        let stream_id = watch_log::object_watch_stream_id(tenant_id, bucket_id);
        let mut live = self.storage.subscribe_stream(&stream_id);
        let storage = self.storage.clone();

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page = match watch_log::list_object_watch_event_page(
                        &storage,
                        tenant_id,
                        bucket_id,
                        &prefix,
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
                        if let Some(response) = watch_event_response(&event)
                            && tx.send(Ok(response)).await.is_err()
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
            Box::pin(ReceiverStream::new(rx)) as Self::WatchPrefixStream
        ))
    }

    async fn create_append_stream(
        &self,
        request: Request<CreateAppendStreamRequest>,
    ) -> Result<Response<CreateAppendStreamResponse>, Status> {
        create_append_stream_rpc(self, request).await
    }

    async fn append_stream_record(
        &self,
        request: Request<AppendStreamRecordRequest>,
    ) -> Result<Response<AppendStreamRecordResponse>, Status> {
        append_stream_record_rpc(self, request).await
    }

    async fn read_append_stream(
        &self,
        request: Request<ReadAppendStreamRequest>,
    ) -> Result<Response<ReadAppendStreamResponse>, Status> {
        read_append_stream_rpc(self, request).await
    }

    async fn tail_append_stream(
        &self,
        request: Request<TailAppendStreamRequest>,
    ) -> Result<Response<Self::TailAppendStreamStream>, Status> {
        tail_append_stream_rpc(self, request).await
    }

    async fn seal_append_stream_segment(
        &self,
        request: Request<SealAppendStreamSegmentRequest>,
    ) -> Result<Response<SealAppendStreamSegmentResponse>, Status> {
        seal_append_stream_segment_rpc(self, request).await
    }

    async fn put_boundary_schema(
        &self,
        request: Request<PutBoundarySchemaRequest>,
    ) -> Result<Response<BoundarySchemaResponse>, Status> {
        put_boundary_schema_rpc(self, request).await
    }

    async fn get_boundary_schema(
        &self,
        request: Request<GetBoundarySchemaRequest>,
    ) -> Result<Response<BoundarySchemaResponse>, Status> {
        get_boundary_schema_rpc(self, request).await
    }

    async fn start_boundary_migration(
        &self,
        request: Request<StartBoundaryMigrationRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        start_boundary_migration_rpc(self, request).await
    }

    async fn get_boundary_migration(
        &self,
        request: Request<GetBoundaryMigrationRequest>,
    ) -> Result<Response<BoundaryMigrationStatus>, Status> {
        get_boundary_migration_rpc(self, request).await
    }

    async fn mutation_batch(
        &self,
        request: Request<MutationBatchRequest>,
    ) -> Result<Response<MutationBatchResponse>, Status> {
        execute_mutation_batch(self, request).await
    }

    async fn initiate_multipart_upload(
        &self,
        request: Request<InitiateMultipartRequest>,
    ) -> Result<Response<InitiateMultipartResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        let transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
        let transaction_principal = transaction_id
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
        let target = NativeIdempotencyTarget::new(
            "InitiateMultipartUpload",
            &req.bucket_name,
            &req.object_key,
        );
        let (attempt, replay) = begin_native_mutation::<InitiateMultipartResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;

        let result = self
            .object_manager
            .initiate_multipart_upload(
                &claims,
                &req.bucket_name,
                &req.object_key,
                transaction_id,
                transaction_principal.as_deref(),
            )
            .await?;
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        let response = InitiateMultipartResponse {
            upload_id: result.upload_id.to_string(),
            version_id: result.upload_id.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.receipt.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: if transaction_id.is_some() {
                0
            } else {
                result.receipt.watch_cursor
            },
            write_state: write_state_for_transaction(transaction_id),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn upload_part(
        &self,
        request: Request<tonic::Streaming<UploadPartRequest>>,
    ) -> Result<Response<UploadPartResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;

        let mut stream = request.into_inner();
        let metadata = match stream.next().await {
            Some(Ok(chunk)) => match chunk.data {
                Some(upload_part_request::Data::Metadata(metadata)) => metadata,
                _ => return Err(Status::invalid_argument("First chunk must be metadata")),
            },
            Some(Err(status)) => return Err(status),
            None => return Err(Status::invalid_argument("Empty stream")),
        };
        validate_native_mutation_context(
            self,
            &claims,
            &metadata.bucket_name,
            metadata.mutation_context.as_ref(),
        )
        .await?;
        let transaction_id = native_transaction_id(metadata.mutation_context.as_ref())?;
        let transaction_principal = transaction_id
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
        let target =
            NativeIdempotencyTarget::new("UploadPart", &metadata.bucket_name, &metadata.object_key)
                .with_parameters(serde_json::json!({
                    "upload_id": metadata.upload_id.clone(),
                    "part_number": metadata.part_number
                }));
        let (attempt, replay) = begin_native_mutation::<UploadPartResponse>(
            self,
            metadata.mutation_context.as_ref(),
            &target,
            &claims,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &metadata.bucket_name,
            &metadata.object_key,
            metadata.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;

        let part_version_id = metadata.part_number.to_string();
        let upload_id = uuid::Uuid::parse_str(&metadata.upload_id)
            .map_err(|_| Status::invalid_argument("Invalid upload_id"))?;
        let data_stream = stream.map(|chunk_result| match chunk_result {
            Ok(chunk) => match chunk.data {
                Some(upload_part_request::Data::Chunk(bytes)) => Ok(bytes),
                _ => Ok(vec![]),
            },
            Err(e) => Err(e),
        });

        let result = self
            .object_manager
            .upload_part(
                &claims,
                &metadata.bucket_name,
                &metadata.object_key,
                upload_id,
                metadata.part_number,
                data_stream,
                transaction_id,
                transaction_principal.as_deref(),
            )
            .await?;
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        let response = UploadPartResponse {
            etag: result.etag,
            version_id: part_version_id,
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: if transaction_id.is_some() {
                0
            } else {
                result.receipt.watch_cursor
            },
            write_state: write_state_for_transaction(transaction_id),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn complete_multipart_upload(
        &self,
        request: Request<CompleteMultipartRequest>,
    ) -> Result<Response<CompleteMultipartResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        let transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
        let transaction_principal = transaction_id
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
        let target_parts = req
            .parts
            .iter()
            .map(|part| serde_json::json!({"part_number": part.part_number, "etag": part.etag.clone()}))
            .collect::<Vec<_>>();
        let target = NativeIdempotencyTarget::new(
            "CompleteMultipartUpload",
            &req.bucket_name,
            &req.object_key,
        )
        .with_parameters(serde_json::json!({
            "upload_id": req.upload_id.clone(),
            "parts": target_parts
        }));
        let (attempt, replay) = begin_native_mutation::<CompleteMultipartResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        let upload_id = uuid::Uuid::parse_str(&req.upload_id)
            .map_err(|_| Status::invalid_argument("Invalid upload_id"))?;
        let parts = req
            .parts
            .into_iter()
            .map(|part| crate::object_manager::CompleteMultipartPart {
                part_number: part.part_number,
                etag: part.etag,
            })
            .collect();

        let object = self
            .object_manager
            .complete_multipart_upload(
                &claims,
                &req.bucket_name,
                &req.object_key,
                upload_id,
                parts,
                transaction_id,
                transaction_principal.as_deref(),
            )
            .await?;
        let watch_cursor = if transaction_id.is_some() {
            0
        } else {
            object_watch_cursor(self, &object).await?
        };
        let authz_revision = object_authz_revision(&object)?;

        let response = CompleteMultipartResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
            write_state: write_state_for_transaction(transaction_id),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn abort_multipart_upload(
        &self,
        request: Request<AbortMultipartRequest>,
    ) -> Result<Response<AbortMultipartResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        let transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
        let transaction_principal = transaction_id
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
        let target =
            NativeIdempotencyTarget::new("AbortMultipartUpload", &req.bucket_name, &req.object_key)
                .with_parameters(serde_json::json!({ "upload_id": req.upload_id.clone() }));
        let (attempt, replay) = begin_native_mutation::<AbortMultipartResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        let upload_id = uuid::Uuid::parse_str(&req.upload_id)
            .map_err(|_| Status::invalid_argument("Invalid upload_id"))?;

        let result = self
            .object_manager
            .abort_multipart_upload(
                &claims,
                &req.bucket_name,
                &req.object_key,
                upload_id,
                transaction_id,
                transaction_principal.as_deref(),
            )
            .await?;
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        let response = AbortMultipartResponse {
            version_id: result.upload_id.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.receipt.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: if transaction_id.is_some() {
                0
            } else {
                result.receipt.watch_cursor
            },
            write_state: write_state_for_transaction(transaction_id),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn create_object_link(
        &self,
        request: Request<CreateObjectLinkRequest>,
    ) -> Result<Response<ObjectLinkResponse>, Status> {
        link_rpc::create_object_link(self, request).await
    }

    async fn update_object_link(
        &self,
        request: Request<UpdateObjectLinkRequest>,
    ) -> Result<Response<ObjectLinkResponse>, Status> {
        link_rpc::update_object_link(self, request).await
    }

    async fn delete_object_link(
        &self,
        request: Request<DeleteObjectLinkRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        link_rpc::delete_object_link(self, request).await
    }

    async fn read_object_link(
        &self,
        request: Request<ReadObjectLinkRequest>,
    ) -> Result<Response<ObjectLinkResponse>, Status> {
        link_rpc::read_object_link(self, request).await
    }

    async fn list_object_links(
        &self,
        request: Request<ListObjectLinksRequest>,
    ) -> Result<Response<ListObjectLinksResponse>, Status> {
        link_rpc::list_object_links(self, request).await
    }

    async fn create_host_alias(
        &self,
        request: Request<CreateHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        link_rpc::create_host_alias(self, request).await
    }

    async fn verify_host_alias(
        &self,
        request: Request<VerifyHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        link_rpc::verify_host_alias(self, request).await
    }

    async fn delete_host_alias(
        &self,
        request: Request<DeleteHostAliasRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        link_rpc::delete_host_alias(self, request).await
    }

    async fn read_host_alias(
        &self,
        request: Request<ReadHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        link_rpc::read_host_alias(self, request).await
    }

    async fn list_host_aliases(
        &self,
        request: Request<ListHostAliasesRequest>,
    ) -> Result<Response<ListHostAliasesResponse>, Status> {
        link_rpc::list_host_aliases(self, request).await
    }
}
