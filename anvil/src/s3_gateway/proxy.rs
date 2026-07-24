use super::*;

pub(super) fn s3_redirect(region: &str) -> Response {
    let request_id = new_s3_request_id();
    let escaped_region = xml_escape(region);
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>PermanentRedirect</Code>\n  <Message>The bucket is in this region: {escaped_region}. Please use this region to retry the request.</Message>\n  <BucketRegion>{escaped_region}</BucketRegion>\n  <RequestId>{request_id}</RequestId>\n</Error>\n"
    );
    Response::builder()
        .status(axum::http::StatusCode::MOVED_PERMANENTLY)
        .header("Content-Type", "application/xml")
        .header("x-amz-request-id", request_id)
        .header("x-amz-bucket-region", region)
        .body(Body::from(body))
        .unwrap()
}

pub(super) async fn select_remote_bucket_proxy_target(
    state: &AppState,
    region: &str,
) -> anyhow::Result<Option<String>> {
    let mut nodes = state
        .persistence
        .list_node_descriptors(Some(region), None)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(nodes.into_iter().find_map(|node| {
        let can_proxy = node.state == LifecycleState::Active
            && node
                .capabilities
                .iter()
                .any(|capability| *capability == NodeCapability::Object)
            && !node.public_api_addr.trim().is_empty();
        can_proxy.then(|| normalize_proxy_endpoint(&node.public_api_addr))
    }))
}

pub(super) fn normalize_proxy_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

pub(super) fn s3_remote_bucket_response(
    policy: CrossRegionRoutingPolicy,
    region: &str,
    proxy_available: bool,
) -> Response {
    match core_routing::remote_bucket_routing_action(policy, proxy_available) {
        core_routing::RemoteBucketRoutingAction::Redirect => s3_redirect(region),
        core_routing::RemoteBucketRoutingAction::Proxy => add_bucket_region_header(
            s3_error(
                "InternalError",
                "Cross-region proxying was selected for an operation that cannot be proxied",
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
            region,
        ),
        core_routing::RemoteBucketRoutingAction::RejectLocalOnly => add_bucket_region_header(
            s3_error(
                "InvalidRequest",
                &format!(
                    "Bucket is in region {region}; cross-region routing is disabled by local_only policy"
                ),
                axum::http::StatusCode::BAD_REQUEST,
            ),
            region,
        ),
        core_routing::RemoteBucketRoutingAction::ProxyUnavailable => add_bucket_region_header(
            s3_error(
                "ServiceUnavailable",
                &format!(
                    "Bucket is in region {region}; cross-region proxying is required by policy but no eligible proxy target is available"
                ),
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
            ),
            region,
        ),
    }
}

pub(super) async fn s3_object_proxy_response_if_needed(
    state: &AppState,
    checked_route: &CheckedS3Route,
    claims: Option<&Claims>,
    bucket: &str,
    key: &str,
    method: &str,
    headers: &HeaderMap,
    uri: &Uri,
    version_id: Option<uuid::Uuid>,
    body: Option<Body>,
) -> Option<Response> {
    let claims = match claims {
        Some(claims) => claims,
        None => {
            let tenant_id = checked_route.tenant_id?;
            let locator = state
                .persistence
                .get_mesh_bucket_locator(tenant_id, bucket)
                .await
                .ok()
                .flatten()?;
            if locator.status == BucketLocatorStatus::Deleted
                || locator.home_region.as_str() == state.region.as_str()
            {
                return None;
            }
            return Some(s3_remote_bucket_response(
                state.config.cross_region_routing_policy,
                locator.home_region.as_str(),
                false,
            ));
        }
    };

    let target =
        match s3_object_proxy_target_if_needed(state, checked_route, claims, bucket).await? {
            Ok(target) => target,
            Err(response) => return Some(response),
        };
    Some(
        proxy_s3_object_request(
            state, target, claims, bucket, key, method, headers, uri, version_id, body,
        )
        .await,
    )
}

pub(super) async fn s3_object_proxy_target_if_needed(
    state: &AppState,
    checked_route: &CheckedS3Route,
    claims: &Claims,
    bucket: &str,
) -> Option<Result<RemoteBucketProxyTarget, Response>> {
    match checked_route.remote_bucket.clone() {
        Some(target) => Some(Ok(target)),
        None => match state
            .persistence
            .get_mesh_bucket_locator(claims.tenant_id, bucket)
            .await
        {
            Ok(Some(locator))
                if locator.status != BucketLocatorStatus::Deleted
                    && locator.home_region.as_str() != state.region.as_str() =>
            {
                let proxy_endpoint =
                    match select_remote_bucket_proxy_target(state, locator.home_region.as_str())
                        .await
                    {
                        Ok(endpoint) => endpoint,
                        Err(error) => {
                            return Some(Err(s3_error(
                                "InternalError",
                                &format!("Failed to resolve remote proxy target: {error}"),
                                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            )));
                        }
                    };
                match core_routing::remote_bucket_routing_action(
                    state.config.cross_region_routing_policy,
                    proxy_endpoint.is_some(),
                ) {
                    core_routing::RemoteBucketRoutingAction::Proxy => {
                        Some(Ok(RemoteBucketProxyTarget {
                            region: locator.home_region.as_str().to_string(),
                            bucket_locator_generation: locator.generation,
                            endpoint: proxy_endpoint.expect("proxy target checked above"),
                        }))
                    }
                    _ => {
                        return Some(Err(s3_remote_bucket_response(
                            state.config.cross_region_routing_policy,
                            locator.home_region.as_str(),
                            proxy_endpoint.is_some(),
                        )));
                    }
                }
            }
            Ok(_) => None,
            Err(error) => {
                return Some(Err(s3_error(
                    "InternalError",
                    &format!("Failed to resolve bucket route: {error}"),
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                )));
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn proxy_s3_object_request(
    state: &AppState,
    target: RemoteBucketProxyTarget,
    claims: &Claims,
    bucket: &str,
    key: &str,
    method: &str,
    headers: &HeaderMap,
    uri: &Uri,
    version_id: Option<uuid::Uuid>,
    body: Option<Body>,
) -> Response {
    let request_id = new_s3_request_id();
    let mut proxy_headers = http_headers_to_proxy_headers(headers);
    if let Some(version_id) = version_id {
        proxy_headers.push(proxy_header("x-anvil-version-id", version_id.to_string()));
    }
    let authz_context =
        match anvil_core::services::internal_proxy::encode_proxy_authz_context(claims) {
            Ok(authz_context) => authz_context,
            Err(error) => {
                return s3_error(
                    "InternalError",
                    &format!("Failed to encode proxy authorisation context: {error}"),
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        };
    let host = headers
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let header = ProxyRequestHeader {
        request_id: request_id.clone(),
        idempotency_key: headers
            .get("x-anvil-idempotency-key")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string(),
        principal_id: claims.sub.clone(),
        tenant_id: claims.tenant_id.to_string(),
        bucket_name: bucket.to_string(),
        object_key: key.to_string(),
        method: method.to_ascii_uppercase(),
        canonical_host: host,
        canonical_path: uri
            .path_and_query()
            .map(|path| path.as_str())
            .unwrap_or_else(|| uri.path())
            .to_string(),
        bucket_locator_generation: target.bucket_locator_generation,
        headers: proxy_headers,
        authz_context,
        native_object_metadata: None,
    };

    let token = state
        .config
        .corestore_internal_bearer_token
        .trim()
        .to_string();
    if token.is_empty() {
        return s3_error(
            "ServiceUnavailable",
            "Cross-region proxying is not configured with an internal node bearer token",
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        );
    }

    let mut client = match InternalProxyServiceClient::connect(target.endpoint.clone()).await {
        Ok(client) => client,
        Err(error) => {
            return add_bucket_region_header(
                s3_error(
                    "ServiceUnavailable",
                    &format!(
                        "Failed to connect to cross-region proxy in {}: {error}",
                        target.region
                    ),
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                ),
                &target.region,
            );
        }
    };

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        if tx
            .send(ProxyRequestChunk {
                part: Some(proxy_request_chunk::Part::Header(header)),
            })
            .await
            .is_err()
        {
            return;
        }
        if let Some(body) = body {
            let mut stream = body.into_data_stream();
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        if tx
                            .send(ProxyRequestChunk {
                                part: Some(proxy_request_chunk::Part::Body(bytes.to_vec())),
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "failed to read S3 proxy request body");
                        break;
                    }
                }
            }
        }
    });

    let mut request = tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    request.metadata_mut().insert(
        "authorization",
        match format!("Bearer {token}").parse() {
            Ok(value) => value,
            Err(_) => {
                return s3_error(
                    "InternalError",
                    "Failed to encode internal proxy token",
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        },
    );

    let response = match client.proxy_object(request).await {
        Ok(response) => response,
        Err(status) => return s3_proxy_status_to_response(status, &target.region),
    };
    let mut stream = response.into_inner();
    let first = match stream.next().await {
        Some(Ok(chunk)) => chunk,
        Some(Err(status)) => return s3_proxy_status_to_response(status, &target.region),
        None => {
            return add_bucket_region_header(
                s3_error(
                    "ServiceUnavailable",
                    "Cross-region proxy returned no response",
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                ),
                &target.region,
            );
        }
    };
    let response_header = match first.part {
        Some(proxy_response_chunk::Part::Header(header)) => header,
        _ => {
            return s3_error(
                "InternalError",
                "Cross-region proxy response did not start with a header",
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };
    s3_proxy_response_to_http(method, &target.region, headers, response_header, stream)
}

pub(super) fn http_headers_to_proxy_headers(headers: &HeaderMap) -> Vec<ProxyHeader> {
    headers
        .iter()
        .filter(|(name, _)| *name != http::header::AUTHORIZATION)
        .flat_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|_| proxy_header(name.as_str(), value.as_bytes()))
        })
        .collect()
}

pub(super) fn proxy_header(name: &str, value: impl AsRef<[u8]>) -> ProxyHeader {
    ProxyHeader {
        name: name.to_ascii_lowercase(),
        value: value.as_ref().to_vec(),
    }
}

pub(super) fn s3_proxy_status_to_response(status: tonic::Status, region: &str) -> Response {
    let status_code = match status.code() {
        tonic::Code::InvalidArgument => axum::http::StatusCode::BAD_REQUEST,
        tonic::Code::Unauthenticated => axum::http::StatusCode::UNAUTHORIZED,
        tonic::Code::PermissionDenied => axum::http::StatusCode::FORBIDDEN,
        tonic::Code::FailedPrecondition => axum::http::StatusCode::PRECONDITION_FAILED,
        tonic::Code::NotFound => axum::http::StatusCode::NOT_FOUND,
        tonic::Code::Unavailable => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    };
    add_bucket_region_header(
        s3_error("CrossRegionProxyError", status.message(), status_code),
        region,
    )
}

pub(super) fn s3_proxy_response_to_http(
    method: &str,
    region: &str,
    request_headers: &HeaderMap,
    header: ProxyResponseHeader,
    stream: tonic::Streaming<anvil_core::anvil_api::ProxyResponseChunk>,
) -> Response {
    let mut status = axum::http::StatusCode::from_u16(header.status as u16)
        .unwrap_or(axum::http::StatusCode::BAD_GATEWAY);
    let proxy_headers = header.headers;
    let content_length = proxy_header_value(&proxy_headers, "content-length")
        .and_then(|value| value.parse::<u64>().ok());
    let etag = proxy_header_value(&proxy_headers, "etag");
    let last_modified = proxy_header_value(&proxy_headers, "x-anvil-created-at")
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&chrono::Utc));

    if status.is_success()
        && (method.eq_ignore_ascii_case("GET") || method.eq_ignore_ascii_case("HEAD"))
        && let (Some(etag), Some(last_modified)) = (etag.as_deref(), last_modified)
        && let Some(response) = evaluate_object_preconditions(request_headers, etag, last_modified)
    {
        return response;
    }

    let range = if status == axum::http::StatusCode::OK && method.eq_ignore_ascii_case("GET") {
        match parse_http_range(request_headers, content_length) {
            Ok(Some(requested_range)) => {
                let Some(content_length) = content_length else {
                    return invalid_range_response(0);
                };
                match requested_range.resolve(content_length) {
                    Ok(range) => {
                        status = axum::http::StatusCode::PARTIAL_CONTENT;
                        Some(range)
                    }
                    Err(response) => return response,
                }
            }
            Ok(None) => None,
            Err(response) => return response,
        }
    } else {
        None
    };

    let mut builder = Response::builder().status(status);
    for proxy_header in proxy_headers {
        builder = add_proxy_response_header(builder, proxy_header);
    }
    builder = builder.header("x-amz-bucket-region", region);
    if let Some(last_modified) = last_modified {
        builder = builder.header(
            "Last-Modified",
            httpdate::fmt_http_date(object_last_modified_time(last_modified)),
        );
    }
    if let Some(range) = range {
        builder = builder.header("Content-Length", range.len()).header(
            "Content-Range",
            format!(
                "bytes {}-{}/{}",
                range.start,
                range.end,
                content_length.unwrap_or_default()
            ),
        );
    } else if let Some(content_length) = content_length {
        builder = builder.header("Content-Length", content_length);
    }
    if method.eq_ignore_ascii_case("HEAD") || status == axum::http::StatusCode::NO_CONTENT {
        return builder.body(Body::empty()).unwrap();
    }

    let body_stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, tonic::Status>> + Send + 'static>> =
        Box::pin(stream.filter_map(|chunk| async move {
            match chunk {
                Ok(chunk) => match chunk.part {
                    Some(proxy_response_chunk::Part::Body(bytes)) => Some(Ok(bytes)),
                    Some(proxy_response_chunk::Part::Header(_)) | None => None,
                },
                Err(status) => Some(Err(status)),
            }
        }));
    let body_stream = match range {
        Some(range) => slice_stream_by_range(body_stream, range),
        None => body_stream,
    };
    let body_stream = body_stream.map(|chunk| match chunk {
        Ok(bytes) => Ok(Bytes::from(bytes)),
        Err(status) => Err(axum::Error::new(status)),
    });
    builder.body(Body::from_stream(body_stream)).unwrap()
}

pub(super) fn proxy_header_value(headers: &[ProxyHeader], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .and_then(|header| std::str::from_utf8(&header.value).ok())
        .map(ToOwned::to_owned)
}

pub(super) fn add_proxy_response_header(
    mut builder: http::response::Builder,
    header: ProxyHeader,
) -> http::response::Builder {
    let name = match header.name.as_str() {
        "x-anvil-version-id" => "x-amz-version-id",
        "content-length" | "x-anvil-created-at" => return builder,
        other => other,
    };
    if let (Ok(name), Ok(value)) = (
        http::header::HeaderName::from_bytes(name.as_bytes()),
        http::HeaderValue::from_bytes(&header.value),
    ) {
        builder = builder.header(name, value);
    }
    builder
}

pub(super) fn add_bucket_region_header(mut response: Response, region: &str) -> Response {
    if let Ok(value) = http::HeaderValue::from_str(region) {
        response.headers_mut().insert("x-amz-bucket-region", value);
    }
    response
}

pub(super) fn remote_bucket_region_from_status(status: &tonic::Status) -> Option<String> {
    status
        .metadata()
        .get("x-anvil-bucket-region")
        .and_then(|value| value.to_str().ok())
        .filter(|region| !region.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            status
                .message()
                .strip_prefix("Bucket is in region ")
                .and_then(|rest| {
                    rest.split(';')
                        .next()
                        .map(str::trim)
                        .filter(|region| !region.is_empty())
                        .map(ToOwned::to_owned)
                })
        })
}

pub(super) fn s3_remote_bucket_response_from_status(
    status: &tonic::Status,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Option<Response> {
    remote_bucket_region_from_status(status)
        .map(|region| s3_remote_bucket_response(cross_region_policy, &region, false))
}

pub(super) fn s3_unavailable_status_to_response(
    status: &tonic::Status,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Response {
    s3_remote_bucket_response_from_status(status, cross_region_policy).unwrap_or_else(|| {
        s3_error(
            "ServiceUnavailable",
            status.message(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        )
    })
}

pub(super) async fn s3_remote_bucket_response_for_authorized_claims(
    state: &AppState,
    claims: &Claims,
    bucket: &str,
    action: AnvilAction,
) -> Result<Option<Response>, Response> {
    if let Err(status) = anvil_core::access_control::require_action(
        &state.storage,
        &state.persistence,
        claims,
        action,
        bucket,
    )
    .await
    {
        if status.code() == tonic::Code::PermissionDenied {
            return Err(s3_error(
                "AccessDenied",
                "Permission denied",
                axum::http::StatusCode::FORBIDDEN,
            ));
        }
        if status.code() == tonic::Code::NotFound {
            if let Ok(Some(locator)) = state
                .persistence
                .get_mesh_bucket_locator(claims.tenant_id, bucket)
                .await
                && locator.status != BucketLocatorStatus::Deleted
                && locator.home_region.as_str() != state.region.as_str()
            {
                return Err(s3_remote_bucket_response(
                    state.config.cross_region_routing_policy,
                    locator.home_region.as_str(),
                    false,
                ));
            }
            return Err(s3_error(
                "NoSuchBucket",
                "The specified bucket does not exist",
                axum::http::StatusCode::NOT_FOUND,
            ));
        }
        return Err(s3_remote_bucket_response_from_status(
            &status,
            state.config.cross_region_routing_policy,
        )
        .unwrap_or_else(|| {
            s3_error(
                "ServiceUnavailable",
                status.message(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
            )
        }));
    }

    match state
        .persistence
        .get_mesh_bucket_locator(claims.tenant_id, bucket)
        .await
    {
        Ok(Some(locator))
            if locator.status != BucketLocatorStatus::Deleted
                && locator.home_region.as_str() != state.region.as_str() =>
        {
            Ok(Some(s3_remote_bucket_response(
                state.config.cross_region_routing_policy,
                locator.home_region.as_str(),
                false,
            )))
        }
        Ok(_) => Ok(None),
        Err(error) => Err(s3_error(
            "InternalError",
            &format!("Failed to resolve bucket route: {error}"),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}
