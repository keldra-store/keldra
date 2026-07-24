use super::*;
use crate::anvil_api::internal_proxy_service_client::InternalProxyServiceClient;
use crate::core_store::{CoreMetaPeerTarget, CoreMetaWriteRoute};
use crate::middleware::AnvilRequestId;
use crate::object_manager::ObjectReadConsistency;
use crate::services::internal_proxy::encode_proxy_authz_context;
use std::pin::Pin;
use tonic::metadata::MetadataValue;

pub(super) type PeerGetObjectStream =
    Pin<Box<dyn futures_core::Stream<Item = Result<GetObjectResponse, Status>> + Send>>;

pub(super) async fn native_put_route_target(
    state: &AppState,
    claims: &auth::Claims,
    metadata: &ObjectMetadata,
) -> Result<Option<CoreMetaPeerTarget>, Status> {
    let Some(bucket) = bucket_journal::read_current_bucket(
        &state.storage,
        claims.tenant_id,
        &metadata.bucket_name,
    )
    .await
    .map_err(|error| Status::internal(error.to_string()))?
    else {
        return Ok(None);
    };
    let partition_id = hex::encode(crate::metadata_journal::object_metadata_partition_id(
        bucket.tenant_id,
        bucket.id,
    ));
    let root_key_hash =
        crate::partition_fence::partition_owner_root_key_hash("object_metadata", &partition_id);
    match state
        .core_store
        .coremeta_write_route(&root_key_hash)
        .await
        .map_err(|error| Status::unavailable(format!("failed to resolve write route: {error}")))?
    {
        CoreMetaWriteRoute::Local => Ok(None),
        CoreMetaWriteRoute::Remote(target) => Ok(Some(target)),
    }
}

pub(super) async fn proxy_native_put(
    state: &AppState,
    target: &CoreMetaPeerTarget,
    claims: &auth::Claims,
    metadata: ObjectMetadata,
    mut stream: tonic::Streaming<PutObjectRequest>,
) -> Result<PutObjectResponse, Status> {
    let token = state.config.corestore_internal_bearer_token.trim();
    if token.is_empty() {
        return Err(Status::unavailable(
            "peer routing requires an internal node bearer token",
        ));
    }
    let request_id = metadata
        .mutation_context
        .as_ref()
        .map(|context| context.request_id.trim())
        .filter(|request_id| !request_id.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let idempotency_key = metadata
        .mutation_context
        .as_ref()
        .map(|context| context.idempotency_key.clone())
        .unwrap_or_default();
    let header = ProxyRequestHeader {
        request_id,
        idempotency_key,
        principal_id: claims.sub.clone(),
        tenant_id: claims.tenant_id.to_string(),
        bucket_name: metadata.bucket_name.clone(),
        object_key: metadata.object_key.clone(),
        method: "NATIVE_PUT".to_string(),
        canonical_host: state.config.public_api_addr.clone(),
        canonical_path: "/anvil.ObjectService/PutObject".to_string(),
        bucket_locator_generation: 0,
        headers: Vec::new(),
        authz_context: encode_proxy_authz_context(claims)?,
        native_object_metadata: Some(metadata),
    };
    let (tx, rx) = mpsc::channel(4);
    tx.send(ProxyRequestChunk {
        part: Some(proxy_request_chunk::Part::Header(header)),
    })
    .await
    .map_err(|_| Status::unavailable("peer proxy request stream closed"))?;
    tokio::spawn(async move {
        while let Some(chunk) = stream.next().await {
            let part = match chunk {
                Ok(PutObjectRequest {
                    data: Some(put_object_request::Data::Chunk(bytes)),
                }) => proxy_request_chunk::Part::Body(bytes),
                Ok(_) => proxy_request_chunk::Part::Failure(ProxyStreamFailure {
                    grpc_code: tonic::Code::InvalidArgument as i32,
                    message: "PutObject metadata may appear only in the first chunk".to_string(),
                }),
                Err(status) => proxy_request_chunk::Part::Failure(ProxyStreamFailure {
                    grpc_code: status.code() as i32,
                    message: status.message().to_string(),
                }),
            };
            let failed = matches!(part, proxy_request_chunk::Part::Failure(_));
            if tx
                .send(ProxyRequestChunk { part: Some(part) })
                .await
                .is_err()
                || failed
            {
                break;
            }
        }
    });

    let channel = state
        .core_store
        .internal_grpc_channel(&target.public_api_addr, "object.native_put_peer_proxy")
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut client = InternalProxyServiceClient::new(channel);
    let mut request = Request::new(ReceiverStream::new(rx));
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|_| Status::internal("failed to encode internal node bearer token"))?,
    );
    let mut response_stream = client.proxy_object(request).await?.into_inner();
    let first = response_stream
        .next()
        .await
        .ok_or_else(|| Status::unavailable("peer proxy returned no response"))??;
    let Some(proxy_response_chunk::Part::Header(header)) = first.part else {
        return Err(Status::internal(
            "peer proxy response did not start with a header",
        ));
    };
    if !(200..300).contains(&header.status) {
        return Err(proxy_status(header.status));
    }
    header
        .native_put_response
        .ok_or_else(|| Status::internal("peer proxy omitted native PutObject response"))
}

pub(super) async fn proxy_get_object_if_needed(
    state: &AppState,
    claims: Option<&auth::Claims>,
    route_tenant_id: Option<i64>,
    request: &GetObjectRequest,
    consistency: ObjectReadConsistency,
) -> Result<Option<PeerGetObjectStream>, Status> {
    let Some(claims) = claims else {
        return Ok(None);
    };
    let tenant_id = routed_tenant_id(claims, route_tenant_id)?;
    let Some(targets) = peer_targets_if_needed(state, tenant_id).await? else {
        return Ok(None);
    };

    let mut headers = version_header(request.version_id.as_deref());
    if let Some(range) = request.range.as_ref() {
        headers.push(proxy_header("x-anvil-range-start", range.start.to_string()));
        headers.push(proxy_header(
            "x-anvil-range-end-exclusive",
            range.end_exclusive.to_string(),
        ));
    }
    add_consistency_headers(&mut headers, consistency);
    let (response_header, mut response_stream) = open_peer_proxy(
        state,
        &targets,
        claims,
        tenant_id,
        &request.bucket_name,
        &request.object_key,
        "GET",
        headers,
    )
    .await?;
    let info = object_info_from_proxy_headers(&response_header.headers)?;
    let mut logical_offset =
        optional_u64_header(&response_header.headers, "x-anvil-range-start")?.unwrap_or(0);
    let (tx, rx) = mpsc::channel(4);
    tokio::spawn(async move {
        if tx
            .send(Ok(GetObjectResponse {
                data: Some(get_object_response::Data::Metadata(info)),
                logical_offset: 0,
                trace_id: String::new(),
            }))
            .await
            .is_err()
        {
            return;
        }
        while let Some(chunk) = response_stream.next().await {
            match chunk {
                Ok(ProxyResponseChunk {
                    part: Some(proxy_response_chunk::Part::Body(bytes)),
                }) => {
                    let chunk_len = bytes.len() as u64;
                    if tx
                        .send(Ok(GetObjectResponse {
                            data: Some(get_object_response::Data::Chunk(bytes)),
                            logical_offset,
                            trace_id: String::new(),
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    logical_offset = logical_offset.saturating_add(chunk_len);
                }
                Ok(ProxyResponseChunk {
                    part: Some(proxy_response_chunk::Part::Header(_)),
                }) => {
                    let _ = tx
                        .send(Err(Status::internal(
                            "peer proxy returned more than one response header",
                        )))
                        .await;
                    return;
                }
                Ok(ProxyResponseChunk { part: None }) => {}
                Err(status) => {
                    let _ = tx.send(Err(status)).await;
                    return;
                }
            }
        }
    });
    Ok(Some(Box::pin(ReceiverStream::new(rx))))
}

pub(super) async fn proxy_head_object_if_needed(
    state: &AppState,
    claims: &auth::Claims,
    request_id: Option<&AnvilRequestId>,
    request: &HeadObjectRequest,
    consistency: ObjectReadConsistency,
) -> Result<Option<HeadObjectResponse>, Status> {
    let Some(targets) = peer_targets_if_needed(state, claims.tenant_id).await? else {
        return Ok(None);
    };
    let mut headers = version_header(request.version_id.as_deref());
    add_consistency_headers(&mut headers, consistency);
    let (header, _stream) = open_peer_proxy_with_request_id(
        state,
        &targets,
        claims,
        claims.tenant_id,
        &request.bucket_name,
        &request.object_key,
        "HEAD",
        headers,
        request_id,
    )
    .await?;
    Ok(Some(head_response_from_proxy_headers(&header.headers)?))
}

async fn peer_targets_if_needed(
    state: &AppState,
    tenant_id: i64,
) -> Result<Option<Vec<CoreMetaPeerTarget>>, Status> {
    let root_key_hash = bucket_journal::tenant_bucket_root_key_hash(tenant_id);
    let route = state
        .core_store
        .coremeta_peer_route(&root_key_hash)
        .await
        .map_err(|error| {
            Status::unavailable(format!("failed to resolve metadata route: {error}"))
        })?;
    if route.local_replica {
        return Ok(None);
    }
    if route.remote_targets.is_empty() {
        return Err(Status::unavailable(
            "metadata route has no reachable replica target",
        ));
    }
    Ok(Some(route.remote_targets))
}

fn routed_tenant_id(claims: &auth::Claims, route_tenant_id: Option<i64>) -> Result<i64, Status> {
    if route_tenant_id.is_some_and(|tenant_id| tenant_id != claims.tenant_id) {
        return Err(Status::permission_denied(
            "Credentials are not valid for routed tenant",
        ));
    }
    Ok(route_tenant_id.unwrap_or(claims.tenant_id))
}

#[allow(clippy::too_many_arguments)]
async fn open_peer_proxy(
    state: &AppState,
    targets: &[CoreMetaPeerTarget],
    claims: &auth::Claims,
    tenant_id: i64,
    bucket_name: &str,
    object_key: &str,
    method: &str,
    headers: Vec<ProxyHeader>,
) -> Result<(ProxyResponseHeader, tonic::Streaming<ProxyResponseChunk>), Status> {
    open_peer_proxy_with_request_id(
        state,
        targets,
        claims,
        tenant_id,
        bucket_name,
        object_key,
        method,
        headers,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn open_peer_proxy_with_request_id(
    state: &AppState,
    targets: &[CoreMetaPeerTarget],
    claims: &auth::Claims,
    tenant_id: i64,
    bucket_name: &str,
    object_key: &str,
    method: &str,
    headers: Vec<ProxyHeader>,
    request_id: Option<&AnvilRequestId>,
) -> Result<(ProxyResponseHeader, tonic::Streaming<ProxyResponseChunk>), Status> {
    let token = state.config.corestore_internal_bearer_token.trim();
    if token.is_empty() {
        return Err(Status::unavailable(
            "peer routing requires an internal node bearer token",
        ));
    }
    let request_id = request_id
        .map(|request_id| request_id.0.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let header = ProxyRequestHeader {
        request_id,
        idempotency_key: String::new(),
        principal_id: claims.sub.clone(),
        tenant_id: tenant_id.to_string(),
        bucket_name: bucket_name.to_string(),
        object_key: object_key.to_string(),
        method: method.to_string(),
        canonical_host: state.config.public_api_addr.clone(),
        canonical_path: format!("/anvil.ObjectService/{method}"),
        bucket_locator_generation: 0,
        headers,
        authz_context: encode_proxy_authz_context(claims)?,
        native_object_metadata: None,
    };

    let mut failures = Vec::new();
    let mut last_status = None;
    for target in targets {
        match open_peer_proxy_target(state, target, token, header.clone()).await {
            Ok(result) => return Ok(result),
            Err(status) if peer_proxy_retryable(&status) => {
                failures.push(format!("{}: {status}", target.node_id));
                last_status = Some(status);
            }
            Err(status) => return Err(status),
        }
    }
    let status = last_status.unwrap_or_else(|| Status::unavailable("peer proxy has no target"));
    Err(Status::new(
        status.code(),
        format!(
            "peer proxy exhausted metadata replicas: {}",
            failures.join("; ")
        ),
    ))
}

async fn open_peer_proxy_target(
    state: &AppState,
    target: &CoreMetaPeerTarget,
    token: &str,
    header: ProxyRequestHeader,
) -> Result<(ProxyResponseHeader, tonic::Streaming<ProxyResponseChunk>), Status> {
    let channel = state
        .core_store
        .internal_grpc_channel(&target.public_api_addr, "object.peer_proxy")
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut client = InternalProxyServiceClient::new(channel);
    let request_stream = tokio_stream::iter([ProxyRequestChunk {
        part: Some(proxy_request_chunk::Part::Header(header)),
    }]);
    let mut request = Request::new(request_stream);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|_| Status::internal("failed to encode internal node bearer token"))?,
    );
    let mut stream = client.proxy_object(request).await?.into_inner();
    let first = stream
        .next()
        .await
        .ok_or_else(|| Status::unavailable("peer proxy returned no response"))??;
    let Some(proxy_response_chunk::Part::Header(header)) = first.part else {
        return Err(Status::internal(
            "peer proxy response did not start with a header",
        ));
    };
    if !(200..300).contains(&header.status) {
        return Err(proxy_status(header.status));
    }
    Ok((header, stream))
}

fn peer_proxy_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::NotFound
            | tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Unknown
    )
}

fn proxy_status(status: u32) -> Status {
    match status {
        400 => Status::invalid_argument("peer proxy rejected the request"),
        401 => Status::unauthenticated("peer proxy authentication failed"),
        403 => Status::permission_denied("peer proxy authorisation failed"),
        404 => Status::not_found("peer proxy object was not found"),
        409 | 412 => Status::failed_precondition("peer proxy precondition failed"),
        503 => Status::unavailable("peer proxy is unavailable"),
        _ => Status::internal(format!("peer proxy returned HTTP status {status}")),
    }
}

fn version_header(version_id: Option<&str>) -> Vec<ProxyHeader> {
    version_id
        .filter(|version_id| !version_id.is_empty())
        .map(|version_id| vec![proxy_header("x-anvil-version-id", version_id)])
        .unwrap_or_default()
}

fn add_consistency_headers(headers: &mut Vec<ProxyHeader>, consistency: ObjectReadConsistency) {
    match consistency {
        ObjectReadConsistency::Latest => {}
        ObjectReadConsistency::AtRootGeneration(generation) => headers.push(proxy_header(
            "x-anvil-consistency-root-generation",
            generation.to_string(),
        )),
        ObjectReadConsistency::AtAuthzRevision(revision) => headers.push(proxy_header(
            "x-anvil-consistency-authz-revision",
            revision.to_string(),
        )),
    }
}

fn proxy_header(name: &str, value: impl AsRef<[u8]>) -> ProxyHeader {
    ProxyHeader {
        name: name.to_ascii_lowercase(),
        value: value.as_ref().to_vec(),
    }
}

fn object_info_from_proxy_headers(headers: &[ProxyHeader]) -> Result<ObjectInfo, Status> {
    Ok(ObjectInfo {
        content_type: optional_header(headers, "content-type").unwrap_or_default(),
        content_length: required_i64_header(headers, "content-length")?,
        version_id: required_header(headers, "x-anvil-version-id")?,
        user_metadata_json: proxy_user_metadata_json(headers),
        storage_class: optional_header(headers, "x-anvil-storage-class").unwrap_or_default(),
    })
}

fn head_response_from_proxy_headers(headers: &[ProxyHeader]) -> Result<HeadObjectResponse, Status> {
    Ok(HeadObjectResponse {
        etag: required_header(headers, "etag")?,
        size: required_i64_header(headers, "content-length")?,
        last_modified: required_header(headers, "x-anvil-created-at")?,
        version_id: required_header(headers, "x-anvil-version-id")?,
        mutation_id: required_header(headers, "x-anvil-mutation-id")?,
        record_hash: required_header(headers, "x-anvil-record-hash")?,
        authz_revision: required_u64_header(headers, "x-anvil-authz-revision")?,
        index_policy_snapshot: optional_header(headers, "x-anvil-index-policy-snapshot")
            .unwrap_or_default(),
        content_type: optional_header(headers, "content-type").unwrap_or_default(),
        user_metadata_json: proxy_user_metadata_json(headers),
        storage_class: optional_header(headers, "x-anvil-storage-class").unwrap_or_default(),
    })
}

fn proxy_user_metadata_json(headers: &[ProxyHeader]) -> String {
    let values = headers
        .iter()
        .filter_map(|header| {
            let key = header.name.strip_prefix("x-amz-meta-")?;
            let value = std::str::from_utf8(&header.value).ok()?;
            Some((
                key.to_string(),
                serde_json::Value::String(value.to_string()),
            ))
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::Value::Object(values).to_string()
}

fn required_u64_header(headers: &[ProxyHeader], name: &str) -> Result<u64, Status> {
    required_header(headers, name)?
        .parse()
        .map_err(|_| Status::internal(format!("peer proxy returned invalid {name}")))
}

fn required_i64_header(headers: &[ProxyHeader], name: &str) -> Result<i64, Status> {
    required_header(headers, name)?
        .parse()
        .map_err(|_| Status::internal(format!("peer proxy returned invalid {name}")))
}

fn optional_u64_header(headers: &[ProxyHeader], name: &str) -> Result<Option<u64>, Status> {
    optional_header(headers, name)
        .map(|value| {
            value
                .parse()
                .map_err(|_| Status::internal(format!("peer proxy returned invalid {name}")))
        })
        .transpose()
}

fn required_header(headers: &[ProxyHeader], name: &str) -> Result<String, Status> {
    optional_header(headers, name)
        .ok_or_else(|| Status::internal(format!("peer proxy response is missing {name}")))
}

fn optional_header(headers: &[ProxyHeader], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .and_then(|header| std::str::from_utf8(&header.value).ok())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_headers_preserve_native_object_metadata() {
        let headers = vec![
            proxy_header("etag", "sha256:etag"),
            proxy_header("content-length", "42"),
            proxy_header("x-anvil-created-at", "2026-07-20T00:00:00Z"),
            proxy_header("x-anvil-version-id", "version"),
            proxy_header("x-anvil-mutation-id", "mutation"),
            proxy_header("x-anvil-record-hash", "record"),
            proxy_header("x-anvil-authz-revision", "7"),
            proxy_header("x-anvil-index-policy-snapshot", "policy"),
            proxy_header("x-anvil-storage-class", "default"),
            proxy_header("content-type", "application/json"),
            proxy_header("x-amz-meta-owner", "alice"),
        ];

        let response = head_response_from_proxy_headers(&headers).unwrap();
        assert_eq!(response.size, 42);
        assert_eq!(response.authz_revision, 7);
        assert_eq!(response.storage_class, "default");
        assert_eq!(response.user_metadata_json, r#"{"owner":"alice"}"#);
    }

    #[test]
    fn routed_tenant_must_match_authenticated_claims() {
        let claims = auth::Claims {
            sub: "app".to_string(),
            exp: usize::MAX,
            tenant_id: 42,
            jti: None,
        };
        assert_eq!(routed_tenant_id(&claims, None).unwrap(), 42);
        assert_eq!(routed_tenant_id(&claims, Some(42)).unwrap(), 42);
        assert_eq!(
            routed_tenant_id(&claims, Some(7)).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
    }
}
