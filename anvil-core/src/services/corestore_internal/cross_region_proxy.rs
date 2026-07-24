use super::*;
use crate::anvil_api::cross_region_proxy_internal_server::CrossRegionProxyInternal;

#[tonic::async_trait]
impl CrossRegionProxyInternal for AppState {
    type ProxyObjectReadStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<ObjectChunk, Status>> + Send>>;
    type ProxyShardRangeStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<ShardChunk, Status>> + Send>>;

    async fn proxy_native(
        &self,
        request: Request<ProxyNativeRequest>,
    ) -> Result<Response<ProxyNativeResponse>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        ensure_len_at_most(
            req.request_body.len(),
            MAX_PROXY_NATIVE_BODY_BYTES,
            "native proxy request_body",
        )?;
        ensure_bounded_identity(&req.method, "native proxy method")?;
        ensure_local_proxy_target(&self.config.region, &req.target_region_id)?;
        match req.method.as_str() {
            "anvil.internal.ping" => Ok(Response::new(ProxyNativeResponse {
                status_code: 200,
                response_body: format!(
                    "{{\"region\":\"{}\",\"node_id\":\"{}\"}}",
                    self.config.region, self.config.node_id
                )
                .into_bytes(),
                error_code: String::new(),
            })),
            "anvil.internal.root.read_by_hash" => {
                let body = core_store::decode_deterministic_proto::<ReadRootRequest>(
                    &req.request_body,
                    "proxied ReadRootRequest",
                )
                .map_err(internal_status)?;
                if let Some(generation) = body.exact_generation {
                    if body.committed_cache {
                        let anchor = self
                            .core_store
                            .read_committed_root_anchor_generation(&body.root_key_hash, generation)
                            .await
                            .map_err(internal_status)?
                            .ok_or_else(|| {
                                Status::not_found("committed root-cache generation not found")
                            })?;
                        let root_anchor_record = core_store::encode_root_anchor_record(&anchor)
                            .map_err(internal_status)?;
                        return Ok(Response::new(ProxyNativeResponse {
                            status_code: 200,
                            response_body: core_store::encode_deterministic_proto(
                                &RootAnchorRead {
                                    root_key_hash: anchor.root_key_hash,
                                    generation: anchor.root_generation,
                                    root_anchor_hash: format!(
                                        "sha256:{}",
                                        core_store::sha256_hex(&root_anchor_record)
                                    ),
                                    root_anchor_record,
                                    shard_index: 0,
                                    register_cohort_node_ids: Vec::new(),
                                    register_cohort_hash: String::new(),
                                    placement_epoch: 0,
                                },
                            ),
                            error_code: String::new(),
                        }));
                    }
                    let shard = self
                        .core_store
                        .read_exact_root_register_shard(&body.root_key_hash, generation)
                        .await
                        .map_err(internal_status)?
                        .ok_or_else(|| {
                            Status::not_found("physical root-register shard not found")
                        })?;
                    return Ok(Response::new(ProxyNativeResponse {
                        status_code: 200,
                        response_body: core_store::encode_deterministic_proto(&RootAnchorRead {
                            root_key_hash: shard.root_key_hash,
                            generation: shard.root_generation,
                            root_anchor_record: shard.root_anchor_record,
                            root_anchor_hash: shard.root_anchor_hash,
                            shard_index: u32::from(shard.shard_index),
                            register_cohort_node_ids: shard.register_cohort_nodes,
                            register_cohort_hash: shard.register_cohort_hash,
                            placement_epoch: shard.placement_epoch,
                        }),
                        error_code: String::new(),
                    }));
                }
                if body.committed_cache {
                    return Err(Status::invalid_argument(
                        "committed-cache root reads require an exact generation",
                    ));
                }
                let read = self
                    .core_store
                    .read_internal_root_anchor_by_hash(&body.root_key_hash, body.min_generation)
                    .await
                    .map_err(internal_status)?;
                Ok(Response::new(ProxyNativeResponse {
                    status_code: 200,
                    response_body: core_store::encode_deterministic_proto(&RootAnchorRead {
                        root_key_hash: read.root_key_hash,
                        generation: read.generation,
                        root_anchor_record: read.root_anchor_record,
                        root_anchor_hash: read.root_anchor_hash,
                        shard_index: 0,
                        register_cohort_node_ids: Vec::new(),
                        register_cohort_hash: String::new(),
                        placement_epoch: 0,
                    }),
                    error_code: String::new(),
                }))
            }
            "anvil.internal.coremeta.inventory" => {
                let body = core_store::decode_deterministic_proto::<CoreMetaInventoryRequest>(
                    &req.request_body,
                    "proxied CoreMetaInventoryRequest",
                )
                .map_err(internal_status)?;
                let max_entries = bounded_coremeta_history_page(
                    body.max_entries,
                    body.max_bytes,
                    "proxied CoreMeta inventory",
                )?;
                let inventory = self
                    .core_store
                    .coremeta_generation_inventory(
                        &body.root_key_hash,
                        body.after.as_ref(),
                        body.through_generation,
                        max_entries,
                        body.max_bytes,
                    )
                    .map_err(internal_status)?;
                Ok(Response::new(ProxyNativeResponse {
                    status_code: 200,
                    response_body: core_store::encode_deterministic_proto(&inventory),
                    error_code: String::new(),
                }))
            }
            other => Err(Status::invalid_argument(format!(
                "native proxy method is not admitted by this Anvil build: {other}"
            ))),
        }
    }

    async fn proxy_object_read(
        &self,
        request: Request<ProxyObjectReadRequest>,
    ) -> Result<Response<Self::ProxyObjectReadStream>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        ensure_local_proxy_target(&self.config.region, &req.target_region_id)?;
        ensure_len_at_most(
            req.authz_context.len(),
            MAX_PROXY_AUTHZ_CONTEXT_BYTES,
            "proxy authz_context",
        )?;
        let range = if req.range_start > 0 || req.range_end_exclusive > 0 {
            if req.range_start > req.range_end_exclusive {
                return Err(Status::invalid_argument(
                    "proxy object range start exceeds end",
                ));
            }
            Some(core_store::CoreByteRange {
                start: req.range_start,
                end_exclusive: req.range_end_exclusive,
            })
        } else {
            None
        };
        let version_id = if req.version_id.trim().is_empty() {
            None
        } else {
            Some(
                uuid::Uuid::parse_str(&req.version_id)
                    .map_err(|_| Status::invalid_argument("version_id must be a UUID"))?,
            )
        };
        let original_claims =
            crate::services::internal_proxy::decode_proxy_authz_context_bytes(&req.authz_context)?;
        if original_claims.tenant_id != req.tenant_id {
            return Err(Status::permission_denied(
                "proxy tenant does not match authz context",
            ));
        }
        if !req.principal_id.is_empty() && original_claims.sub != req.principal_id {
            return Err(Status::permission_denied(
                "proxy principal does not match authz context",
            ));
        }
        let result = self
            .object_manager
            .get_object_with_link_mode_for_tenant(
                Some(original_claims),
                Some(req.tenant_id),
                req.bucket_name.clone(),
                req.object_key.clone(),
                version_id,
                range,
                crate::object_manager::ObjectLinkReadMode::Follow,
                crate::object_manager::ObjectReadConsistency::Latest,
            )
            .await?;
        let mut stream = result.stream;
        let start_offset = result.range_start;
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut offset = start_offset;
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(data) => {
                        let len = data.len() as u64;
                        if tx
                            .send(Ok(ObjectChunk {
                                offset,
                                data,
                                eof: false,
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        offset = offset.saturating_add(len);
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                }
            }
            let _ = tx
                .send(Ok(ObjectChunk {
                    offset,
                    data: Vec::new(),
                    eof: true,
                }))
                .await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn proxy_shard_range(
        &self,
        request: Request<ProxyShardRangeRequest>,
    ) -> Result<Response<Self::ProxyShardRangeStream>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        ensure_local_proxy_target(&self.config.region, &req.target_region_id)?;
        validate_shard_read_scope(
            &req.block_id,
            req.shard_index,
            &req.erasure_profile_id,
            &req.shard_hash,
            &req.boundary_summary_hash,
        )?;
        let range = bounded_shard_range(req.range_start, req.range_end_exclusive)?;
        let bytes = self
            .core_store
            .read_internal_shard_range(CoreInternalGetShard {
                block_id: req.block_id.clone(),
                shard_index: u16::try_from(req.shard_index)
                    .map_err(|_| Status::invalid_argument("shard_index exceeds u16"))?,
                erasure_profile_id: req.erasure_profile_id,
                placement_epoch: req.placement_epoch,
                shard_hash: req.shard_hash,
                boundary_summary_hash: if req.boundary_summary_hash.is_empty() {
                    None
                } else {
                    Some(req.boundary_summary_hash)
                },
                range,
            })
            .await
            .map_err(internal_status)?;
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(ShardChunk {
                    block_id: req.block_id,
                    shard_index: req.shard_index,
                    offset: req.range_start,
                    data: bytes,
                    eof: true,
                }))
                .await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn ensure_local_proxy_target(local_region: &str, target_region: &str) -> Result<(), Status> {
    if !target_region.is_empty() && target_region != local_region {
        return Err(Status::unavailable(format!(
            "target region {target_region} is not served by this node"
        )));
    }
    Ok(())
}
