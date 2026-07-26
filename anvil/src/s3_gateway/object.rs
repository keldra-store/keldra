use super::*;

pub(super) fn s3_user_metadata(headers: &axum::http::HeaderMap) -> Option<serde_json::Value> {
    let mut values = serde_json::Map::new();
    for (name, value) in headers {
        let Some(metadata_key) = name.as_str().strip_prefix("x-amz-meta-") else {
            continue;
        };
        let Ok(metadata_value) = value.to_str() else {
            continue;
        };
        values.insert(
            metadata_key.to_string(),
            serde_json::Value::String(metadata_value.to_string()),
        );
    }
    if values.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(values))
    }
}

pub(super) fn add_s3_user_metadata_headers(
    mut builder: axum::http::response::Builder,
    user_meta: Option<&serde_json::Value>,
) -> axum::http::response::Builder {
    let Some(serde_json::Value::Object(values)) = user_meta else {
        return builder;
    };
    for (key, value) in values {
        if let Some(value) = value.as_str() {
            builder = builder.header(format!("x-amz-meta-{key}"), value);
        }
    }
    builder
}

pub(super) async fn get_object(
    State(state): State<AppState>,
    Path((mut bucket, mut key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        return Box::pin(list_objects(State(state), Path(bucket), Query(q), req)).await;
    }
    (bucket, key) = s3_routed_bucket_key(&req, bucket, key);

    let checked_route = match s3_checked_route(
        &state,
        s3_host_route(&req),
        req.extensions().get::<Claims>().cloned(),
    )
    .await
    {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route.claims.clone();
    if let Some(upload_id) = q.get("uploadId") {
        let claims = match claims {
            Some(claims) => claims,
            None => {
                return s3_error(
                    "AccessDenied",
                    "Missing credentials",
                    axum::http::StatusCode::FORBIDDEN,
                );
            }
        };
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        return list_multipart_parts_response(state, claims, bucket, key, upload_id, &q).await;
    }

    let version_id = match parse_s3_version_id(&q) {
        Ok(version_id) => version_id,
        Err(response) => return response,
    };
    if is_link_metadata_request(req.headers()) {
        return get_object_link_metadata_response(
            state,
            claims,
            checked_route.tenant_id,
            &bucket,
            &key,
            version_id,
        )
        .await;
    }
    if let Some(response) = s3_object_proxy_response_if_needed(
        &state,
        &checked_route,
        checked_route.claims.as_ref(),
        &bucket,
        &key,
        "GET",
        req.headers(),
        req.uri(),
        version_id,
        None,
    )
    .await
    {
        return response;
    }
    let requested_range = match parse_http_range(req.headers(), None) {
        Ok(range) => range,
        Err(response) => return response,
    };

    let response_bucket = bucket.clone();
    let response_key = key.clone();
    match state
        .object_manager
        .get_object_with_link_mode_for_tenant(
            claims,
            checked_route.tenant_id,
            bucket,
            key,
            version_id,
            None,
            ObjectLinkReadMode::Follow,
            ObjectReadConsistency::Latest,
        )
        .await
    {
        Ok(result) => {
            let anvil_core::object_manager::ObjectReadResult {
                object,
                stream,
                followed_link,
                range_start: _,
            } = result;
            if let Some(response) =
                evaluate_object_preconditions(req.headers(), &object.etag, object.created_at)
            {
                return response;
            }
            let range = match requested_range {
                Some(range_header) => match range_header.resolve(object.size as u64) {
                    Ok(range) => Some(range),
                    Err(response) => return response,
                },
                None => None,
            };
            let (status, content_length, body_stream) = match range {
                Some(range) => (
                    axum::http::StatusCode::PARTIAL_CONTENT,
                    range.len() as i64,
                    slice_stream_by_range(stream, range),
                ),
                None => (axum::http::StatusCode::OK, object.size, stream),
            };
            let mut builder = Response::builder()
                .status(status)
                .header("Content-Type", object.content_type.unwrap_or_default())
                .header("Content-Length", content_length)
                .header("ETag", object.etag)
                .header("Accept-Ranges", "bytes")
                .header("x-amz-version-id", object.version_id.to_string());
            builder = add_followed_link_headers(builder, followed_link.as_ref());
            builder = add_s3_user_metadata_headers(builder, object.user_meta.as_ref());
            if let Some(range) = range {
                builder = builder.header(
                    "Content-Range",
                    format!("bytes {}-{}/{}", range.start, range.end, object.size),
                );
            }
            builder
                .body(Body::from_stream(body_stream.map(move |r| {
                    r.map_err(|e| {
                        tracing::warn!(
                            bucket = %response_bucket,
                            key = %response_key,
                            error = %e,
                            "S3 object body stream failed"
                        );
                        axum::Error::new(e)
                    })
                })))
                .unwrap()
        }
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if let Some(response) = s3_remote_bucket_response_from_status(
                    &status,
                    state.config.cross_region_routing_policy,
                ) {
                    return response;
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => {
                if req.extensions().get::<Claims>().is_none() {
                    s3_error(
                        "AccessDenied",
                        status.message(),
                        axum::http::StatusCode::FORBIDDEN,
                    )
                } else {
                    s3_error(
                        "NoSuchKey",
                        status.message(),
                        axum::http::StatusCode::NOT_FOUND,
                    )
                }
            }
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::Unavailable => {
                s3_unavailable_status_to_response(&status, state.config.cross_region_routing_policy)
            }
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

pub(super) async fn get_object_link_metadata_response(
    state: AppState,
    claims: Option<Claims>,
    route_tenant_id: Option<i64>,
    bucket: &str,
    key: &str,
    version_id: Option<uuid::Uuid>,
) -> Response {
    match state
        .object_manager
        .read_object_link_for_tenant(
            claims.clone(),
            route_tenant_id,
            bucket,
            key,
            version_id,
            ObjectReadConsistency::Latest,
        )
        .await
    {
        Ok(descriptor) => {
            let body = match serde_json::to_vec(&descriptor) {
                Ok(body) => body,
                Err(error) => {
                    return s3_error(
                        "InternalError",
                        &error.to_string(),
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }
            };
            let builder = Response::builder()
                .status(axum::http::StatusCode::OK)
                .header("Content-Type", object_links::LINK_METADATA_CONTENT_TYPE)
                .header("Content-Length", body.len())
                .header("ETag", object_links::link_metadata_etag(&descriptor));
            add_link_descriptor_headers(builder, &descriptor)
                .body(Body::from(body))
                .unwrap()
        }
        Err(status) => link_status_to_response(
            status,
            claims.is_some(),
            state.config.cross_region_routing_policy,
        ),
    }
}

pub(super) async fn head_object_link_metadata_response(
    state: AppState,
    claims: Option<Claims>,
    route_tenant_id: Option<i64>,
    bucket: &str,
    key: &str,
    version_id: Option<uuid::Uuid>,
) -> Response {
    match state
        .object_manager
        .read_object_link_for_tenant(
            claims.clone(),
            route_tenant_id,
            bucket,
            key,
            version_id,
            ObjectReadConsistency::Latest,
        )
        .await
    {
        Ok(descriptor) => {
            let content_length = serde_json::to_vec(&descriptor)
                .map(|body| body.len())
                .unwrap_or(0);
            let builder = Response::builder()
                .status(axum::http::StatusCode::OK)
                .header("Content-Type", object_links::LINK_METADATA_CONTENT_TYPE)
                .header("Content-Length", content_length)
                .header("ETag", object_links::link_metadata_etag(&descriptor));
            add_link_descriptor_headers(builder, &descriptor)
                .body(Body::empty())
                .unwrap()
        }
        Err(status) => link_status_to_response(
            status,
            claims.is_some(),
            state.config.cross_region_routing_policy,
        ),
    }
}

pub(super) fn is_link_metadata_request(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get("x-anvil-link-mode")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("metadata"))
}

pub(super) fn add_followed_link_headers(
    builder: axum::http::response::Builder,
    followed_link: Option<&object_links::FollowedObjectLink>,
) -> axum::http::response::Builder {
    let Some(followed_link) = followed_link else {
        return builder;
    };
    add_link_descriptor_headers(builder, &followed_link.descriptor)
}

pub(super) fn add_link_descriptor_headers(
    builder: axum::http::response::Builder,
    descriptor: &object_links::ObjectLinkDescriptor,
) -> axum::http::response::Builder {
    builder
        .header("x-anvil-object-kind", "link")
        .header("x-anvil-link-key", descriptor.link_key.clone())
        .header("x-anvil-link-generation", descriptor.generation.to_string())
        .header(
            "x-anvil-link-target-version",
            descriptor.target_version.clone().unwrap_or_default(),
        )
}

pub(super) fn link_status_to_response(
    status: tonic::Status,
    has_claims: bool,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Response {
    if let Some(response) = s3_remote_bucket_response_from_status(&status, cross_region_policy) {
        return response;
    }

    match status.code() {
        tonic::Code::NotFound => {
            if has_claims {
                s3_error(
                    "NoSuchKey",
                    status.message(),
                    axum::http::StatusCode::NOT_FOUND,
                )
            } else {
                s3_error(
                    "AccessDenied",
                    status.message(),
                    axum::http::StatusCode::FORBIDDEN,
                )
            }
        }
        tonic::Code::PermissionDenied => s3_error(
            "AccessDenied",
            status.message(),
            axum::http::StatusCode::FORBIDDEN,
        ),
        tonic::Code::FailedPrecondition => s3_error(
            status.message(),
            status.message(),
            axum::http::StatusCode::PRECONDITION_FAILED,
        ),
        tonic::Code::InvalidArgument => s3_error(
            "InvalidArgument",
            status.message(),
            axum::http::StatusCode::BAD_REQUEST,
        ),
        tonic::Code::Unavailable => s3_unavailable_status_to_response(&status, cross_region_policy),
        _ => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

pub(super) async fn put_object(
    State(state): State<AppState>,
    Path((mut bucket, mut key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        return Box::pin(create_bucket(State(state), Path(bucket), Query(q), req)).await;
    }
    (bucket, key) = s3_routed_bucket_key(&req, bucket, key);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let checked_route = match s3_checked_route(&state, s3_host_route(&req), Some(claims)).await {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route
        .claims
        .clone()
        .expect("authenticated put object path supplied claims");
    let copy_source = match req.headers().get("x-amz-copy-source") {
        Some(value) => match value.to_str() {
            Ok(value) => Some(value.to_owned()),
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid x-amz-copy-source",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        },
        None => None,
    };

    if let Some(copy_source) = copy_source {
        return copy_object(state, claims, bucket, key, copy_source, req.headers()).await;
    }

    if let Some(upload_id) = q.get("uploadId") {
        let part_number = match q
            .get("partNumber")
            .and_then(|value| value.parse::<i32>().ok())
        {
            Some(part_number) => part_number,
            None => {
                return s3_error(
                    "InvalidArgument",
                    "Missing or invalid partNumber",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        let body_stream = req.into_body().into_data_stream().map(|r| {
            r.map(|chunk| chunk.to_vec())
                .map_err(|e| tonic::Status::internal(e.to_string()))
        });
        return upload_part(
            state,
            claims,
            bucket,
            key,
            upload_id,
            part_number,
            body_stream,
        )
        .await;
    }

    if let Some(proxy_target) =
        s3_object_proxy_target_if_needed(&state, &checked_route, &claims, &bucket).await
    {
        return match proxy_target {
            Ok(proxy_target) => {
                let headers = req.headers().clone();
                let uri = req.uri().clone();
                let body = req.into_body();
                proxy_s3_object_request(
                    &state,
                    proxy_target,
                    &claims,
                    &bucket,
                    &key,
                    "PUT",
                    &headers,
                    &uri,
                    None,
                    Some(body),
                )
                .await
            }
            Err(response) => response,
        };
    }

    if request_has_write_etag_preconditions(req.headers()) {
        let current = match state
            .object_manager
            .current_object_for_write_precondition(&claims, &bucket, &key)
            .await
        {
            Ok(current) => current,
            Err(status) => {
                return s3_status_to_response_for_auth(
                    status,
                    true,
                    "NoSuchBucket",
                    state.config.cross_region_routing_policy,
                );
            }
        };
        if let Some(response) = evaluate_write_etag_preconditions(
            req.headers(),
            current.as_ref().map(|object| object.etag.as_str()),
        ) {
            return response;
        }
    }

    let (transaction, transaction_principal) =
        match begin_s3_write_transaction(&state, &claims, "put", &bucket, &key).await {
            Ok(binding) => binding,
            Err(status) => {
                return s3_status_to_response_for_auth(
                    status,
                    true,
                    "ServiceUnavailable",
                    state.config.cross_region_routing_policy,
                );
            }
        };
    let options = ObjectWriteOptions {
        content_type: req
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string),
        user_metadata: s3_user_metadata(req.headers()),
        transaction_id: Some(transaction.transaction_id.clone()),
        transaction_principal: Some(transaction_principal.clone()),
        storage_class_id: None,
        ..Default::default()
    };
    let body_stream = req.into_body().into_data_stream().map(|r| {
        r.map(|chunk| chunk.to_vec())
            .map_err(|e| tonic::Status::internal(e.to_string()))
    });

    let put_result = state
        .object_manager
        .put_object(&claims, &bucket, &key, body_stream, options)
        .await;
    match put_result {
        Ok(object) => {
            if let Err(status) =
                commit_s3_write_transaction(&state, &transaction, &transaction_principal).await
            {
                return s3_status_to_response_for_auth(
                    status,
                    true,
                    "ServiceUnavailable",
                    state.config.cross_region_routing_policy,
                );
            }
            Response::builder()
                .status(200)
                .header("ETag", object.etag)
                .header("x-amz-version-id", object.version_id.to_string())
                .body(Body::empty())
                .unwrap()
        }
        Err(status) => {
            let _ = state.mvcc.open_transactions.rollback(
                &transaction.transaction_id,
                &transaction_principal,
                s3_now_unix_ms().unwrap_or_default(),
            );
            match status.code() {
                tonic::Code::FailedPrecondition => {
                    if let Some(response) = s3_remote_bucket_response_from_status(
                        &status,
                        state.config.cross_region_routing_policy,
                    ) {
                        return response;
                    }
                    s3_error(
                        "PreconditionFailed",
                        status.message(),
                        axum::http::StatusCode::PRECONDITION_FAILED,
                    )
                }
                tonic::Code::NotFound => s3_error(
                    "NoSuchBucket",
                    status.message(),
                    axum::http::StatusCode::NOT_FOUND,
                ),
                tonic::Code::PermissionDenied => s3_error(
                    "AccessDenied",
                    status.message(),
                    axum::http::StatusCode::FORBIDDEN,
                ),
                tonic::Code::Unavailable => s3_unavailable_status_to_response(
                    &status,
                    state.config.cross_region_routing_policy,
                ),
                _ => s3_error(
                    "InternalError",
                    status.message(),
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                ),
            }
        }
    }
}

pub(super) async fn post_object(
    State(state): State<AppState>,
    Path((mut bucket, mut key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        return Box::pin(post_bucket(State(state), Path(bucket), Query(q), req)).await;
    }
    (bucket, key) = s3_routed_bucket_key(&req, bucket, key);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let checked_route = match s3_checked_route(&state, s3_host_route(&req), Some(claims)).await {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route
        .claims
        .expect("authenticated post object path supplied claims");

    if q.contains_key("uploads") {
        return initiate_multipart_upload(state, claims, bucket, key).await;
    }

    if let Some(upload_id) = q.get("uploadId") {
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
            .await
            .unwrap_or_default();
        return complete_multipart_upload(state, claims, bucket, key, upload_id, bytes).await;
    }

    s3_error(
        "InvalidArgument",
        "Unsupported POST object operation",
        axum::http::StatusCode::BAD_REQUEST,
    )
}

pub(super) async fn copy_object(
    state: AppState,
    claims: Claims,
    destination_bucket: String,
    destination_key: String,
    copy_source: String,
    headers: &axum::http::HeaderMap,
) -> Response {
    let (source_bucket, source_key, source_version_id) = match parse_copy_source(&copy_source) {
        Ok(source) => source,
        Err(response) => return response,
    };

    let source_object = match state
        .object_manager
        .head_object(
            Some(claims.clone()),
            &source_bucket,
            &source_key,
            source_version_id,
        )
        .await
    {
        Ok(source) => source,
        Err(status) => {
            return copy_status_to_response(
                status,
                "NoSuchKey",
                state.config.cross_region_routing_policy,
            );
        }
    };

    if let Some(response) =
        evaluate_copy_source_preconditions(headers, &source_object.etag, source_object.created_at)
    {
        return response;
    }

    match state
        .object_manager
        .copy_object(
            claims,
            &source_bucket,
            &source_key,
            source_version_id,
            &destination_bucket,
            &destination_key,
            None,
        )
        .await
    {
        Ok(object) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult>\n  <LastModified>{}</LastModified>\n  <ETag>\"{}\"</ETag>\n</CopyObjectResult>\n",
                object.created_at.to_rfc3339(),
                object.etag
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .header("ETag", object.etag)
                .header("x-amz-version-id", object.version_id.to_string())
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => copy_status_to_response(
            status,
            "NoSuchBucket",
            state.config.cross_region_routing_policy,
        ),
    }
}

pub(super) fn parse_copy_source(
    value: &str,
) -> Result<(String, String, Option<uuid::Uuid>), Response> {
    let value = value.trim_start_matches('/');
    let (path, query) = value.split_once('?').unwrap_or((value, ""));
    let Some((bucket, key)) = path.split_once('/') else {
        return Err(s3_error(
            "InvalidArgument",
            "Invalid x-amz-copy-source",
            axum::http::StatusCode::BAD_REQUEST,
        ));
    };
    let bucket = percent_decode_path_component(bucket);
    let key = percent_decode_path_component(key);
    if bucket.is_empty() || key.is_empty() {
        return Err(s3_error(
            "InvalidArgument",
            "Invalid x-amz-copy-source",
            axum::http::StatusCode::BAD_REQUEST,
        ));
    }
    let version_id = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find_map(|(name, value)| {
            (name == "versionId" || name == "version-id")
                .then(|| percent_decode_query_component(value))
        })
        .filter(|value| !value.is_empty())
        .map(|value| {
            uuid::Uuid::parse_str(&value).map_err(|_| {
                s3_error(
                    "InvalidArgument",
                    "Invalid source versionId",
                    axum::http::StatusCode::BAD_REQUEST,
                )
            })
        })
        .transpose()?;

    Ok((bucket, key, version_id))
}

pub(super) fn copy_status_to_response(
    status: tonic::Status,
    not_found_code: &str,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Response {
    if let Some(response) = s3_remote_bucket_response_from_status(&status, cross_region_policy) {
        return response;
    }

    match status.code() {
        tonic::Code::FailedPrecondition => {
            if let Some(response) =
                s3_remote_bucket_response_from_status(&status, cross_region_policy)
            {
                return response;
            }
            s3_error(
                "PreconditionFailed",
                status.message(),
                axum::http::StatusCode::PRECONDITION_FAILED,
            )
        }
        tonic::Code::NotFound => s3_error(
            not_found_code,
            status.message(),
            axum::http::StatusCode::NOT_FOUND,
        ),
        tonic::Code::PermissionDenied => s3_error(
            "AccessDenied",
            status.message(),
            axum::http::StatusCode::FORBIDDEN,
        ),
        tonic::Code::InvalidArgument => s3_error(
            "InvalidArgument",
            status.message(),
            axum::http::StatusCode::BAD_REQUEST,
        ),
        _ => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

pub(super) async fn delete_object(
    State(state): State<AppState>,
    Path((mut bucket, mut key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        return Box::pin(delete_bucket(State(state), Path(bucket), req)).await;
    }
    (bucket, key) = s3_routed_bucket_key(&req, bucket, key);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let checked_route = match s3_checked_route(&state, s3_host_route(&req), Some(claims)).await {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route
        .claims
        .clone()
        .expect("authenticated delete object path supplied claims");

    if let Some(upload_id) = q.get("uploadId") {
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        return abort_multipart_upload(state, claims, bucket, key, upload_id).await;
    }

    let version_id = match parse_s3_version_id(&q) {
        Ok(version_id) => version_id,
        Err(response) => return response,
    };

    if let Some(response) = s3_object_proxy_response_if_needed(
        &state,
        &checked_route,
        Some(&claims),
        &bucket,
        &key,
        "DELETE",
        req.headers(),
        req.uri(),
        version_id,
        None,
    )
    .await
    {
        return response;
    }

    if let Some(version_id) = version_id {
        return match state
            .object_manager
            .delete_object_version(
                &claims,
                &bucket,
                &key,
                version_id,
                None,
                None,
                ObjectWriteVisibility::default(),
            )
            .await
        {
            Ok(deleted) => {
                let mut builder = Response::builder()
                    .status(axum::http::StatusCode::NO_CONTENT)
                    .header("x-amz-version-id", deleted.version_id.to_string());
                if deleted.deleted_at.is_some() {
                    builder = builder.header("x-amz-delete-marker", "true");
                }
                builder.body(Body::empty()).unwrap()
            }
            Err(status) => {
                s3_delete_status_to_response(status, state.config.cross_region_routing_policy)
            }
        };
    }

    match state
        .object_manager
        .delete_object(
            &claims,
            &bucket,
            &key,
            None,
            None,
            ObjectWriteVisibility::default(),
        )
        .await
    {
        Ok(delete_marker) => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .header("x-amz-delete-marker", "true")
            .header("x-amz-version-id", delete_marker.version_id.to_string())
            .body(Body::empty())
            .unwrap(),
        Err(status) => {
            s3_delete_status_to_response(status, state.config.cross_region_routing_policy)
        }
    }
}

pub(super) fn s3_delete_status_to_response(
    status: tonic::Status,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Response {
    if let Some(response) = s3_remote_bucket_response_from_status(&status, cross_region_policy) {
        return response;
    }

    match status.code() {
        tonic::Code::FailedPrecondition => {
            if let Some(response) =
                s3_remote_bucket_response_from_status(&status, cross_region_policy)
            {
                return response;
            }
            s3_error(
                "PreconditionFailed",
                status.message(),
                axum::http::StatusCode::PRECONDITION_FAILED,
            )
        }
        tonic::Code::NotFound => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap(),
        tonic::Code::PermissionDenied => s3_error(
            "AccessDenied",
            status.message(),
            axum::http::StatusCode::FORBIDDEN,
        ),
        tonic::Code::InvalidArgument => s3_error(
            "InvalidArgument",
            status.message(),
            axum::http::StatusCode::BAD_REQUEST,
        ),
        _ => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

pub(super) async fn head_object(
    State(state): State<AppState>,
    Path((mut bucket, mut key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        return Box::pin(head_bucket(State(state), Path(bucket), req)).await;
    }
    (bucket, key) = s3_routed_bucket_key(&req, bucket, key);

    let checked_route = match s3_checked_route(
        &state,
        s3_host_route(&req),
        req.extensions().get::<Claims>().cloned(),
    )
    .await
    {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route.claims.clone();
    let version_id = match parse_s3_version_id(&q) {
        Ok(version_id) => version_id,
        Err(response) => return response,
    };
    if is_link_metadata_request(req.headers()) {
        return head_object_link_metadata_response(
            state,
            claims,
            checked_route.tenant_id,
            &bucket,
            &key,
            version_id,
        )
        .await;
    }

    if let Some(response) = s3_object_proxy_response_if_needed(
        &state,
        &checked_route,
        checked_route.claims.as_ref(),
        &bucket,
        &key,
        "HEAD",
        req.headers(),
        req.uri(),
        version_id,
        None,
    )
    .await
    {
        return response;
    }

    match state
        .object_manager
        .head_object_with_link_mode_for_tenant(
            claims,
            checked_route.tenant_id,
            &bucket,
            &key,
            version_id,
            ObjectLinkReadMode::Follow,
            ObjectReadConsistency::Latest,
        )
        .await
    {
        Ok(result) => {
            let anvil_core::object_manager::ObjectHeadResult {
                object,
                followed_link,
            } = result;
            if let Some(response) =
                evaluate_object_preconditions(req.headers(), &object.etag, object.created_at)
            {
                return response;
            }
            let builder = Response::builder()
                .status(200)
                .header(
                    "Content-Type",
                    object.content_type.clone().unwrap_or_default(),
                )
                .header("Content-Length", object.size)
                .header("ETag", object.etag)
                .header("Accept-Ranges", "bytes")
                .header("x-amz-version-id", object.version_id.to_string());
            let builder = add_followed_link_headers(builder, followed_link.as_ref());
            add_s3_user_metadata_headers(builder, object.user_meta.as_ref())
                .body(Body::empty())
                .unwrap()
        }
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if let Some(response) = s3_remote_bucket_response_from_status(
                    &status,
                    state.config.cross_region_routing_policy,
                ) {
                    return response;
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => {
                if req.extensions().get::<Claims>().is_none() {
                    s3_error(
                        "AccessDenied",
                        status.message(),
                        axum::http::StatusCode::FORBIDDEN,
                    )
                } else {
                    s3_error(
                        "NoSuchKey",
                        status.message(),
                        axum::http::StatusCode::NOT_FOUND,
                    )
                }
            }
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::Unavailable => {
                s3_unavailable_status_to_response(&status, state.config.cross_region_routing_policy)
            }
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}
