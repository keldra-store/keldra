use super::*;

#[derive(Deserialize)]
pub(super) struct CreateBucketConfiguration {
    #[serde(rename = "LocationConstraint")]
    location_constraint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct BucketVersioningConfigurationXml {
    #[serde(rename = "Status")]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DeleteObjectsXml {
    #[serde(rename = "Object", default)]
    objects: Vec<DeleteObjectsXmlObject>,
    #[serde(rename = "Quiet")]
    quiet: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DeleteObjectsXmlObject {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: Option<String>,
}

pub(super) async fn list_buckets(State(state): State<AppState>, req: Request) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        let q = s3_query_map(req.uri());
        return Box::pin(list_objects(State(state), Path(bucket), Query(q), req)).await;
    }

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
        .expect("authenticated delete bucket path supplied claims");
    let query = s3_query_map(req.uri());
    if query.contains_key("prefix") || query.contains_key("bucket-region") {
        return s3_error(
            "NotImplemented",
            "ListBuckets prefix and bucket-region filters are not implemented",
            axum::http::StatusCode::NOT_IMPLEMENTED,
        );
    }
    let max_buckets = match query.get("max-buckets") {
        Some(value) => match value.parse::<u32>() {
            Ok(value) if (1..=1000).contains(&value) => value,
            _ => {
                return s3_error(
                    "InvalidArgument",
                    "max-buckets must be between 1 and 1000",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        },
        None => 1000,
    };
    let continuation_token = query.get("continuation-token").cloned().unwrap_or_default();

    let result = async {
        state.bucket_manager.authorize_bucket_list(&claims).await?;
        let page_request = anvil_core::anvil_api::PageRequest {
            page_size: max_buckets,
            page_token: continuation_token.clone(),
        };
        let revision =
            bucket_journal::current_bucket_collection_revision(&state.storage, claims.tenant_id)
                .await
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
        let binding = anvil_core::services::collection_cursor::CollectionCursorBinding {
            service_method: "anvil.S3/ListBuckets",
            filters: &[],
            principal_scope: &principal_scope,
            page_size: max_buckets as usize,
            revision: &revision,
            sort: "bucket_name.asc",
        };
        let position = anvil_core::services::collection_cursor::decode_page_token(
            Some(&page_request),
            &binding,
            state.config.jwt_secret.as_bytes(),
        )?;
        let after_tuple_key =
            anvil_core::services::collection_cursor::decode_binary_position(position.as_deref())?;
        let page = bucket_journal::page_current_buckets(
            &state.storage,
            claims.tenant_id,
            &revision,
            after_tuple_key.as_deref(),
            max_buckets as usize,
        )
        .await
        .map_err(|error| tonic::Status::aborted(error.to_string()))?;
        let next_page_token = page
            .next_tuple_key
            .as_deref()
            .map(anvil_core::services::collection_cursor::encode_binary_position)
            .transpose()?
            .map(|position| {
                anvil_core::services::collection_cursor::encode_next_page_token(
                    &position,
                    &binding,
                    state.config.jwt_secret.as_bytes(),
                )
            })
            .transpose()?
            .unwrap_or_default();
        Ok::<_, tonic::Status>((page.buckets, next_page_token))
    }
    .await;

    match result {
        Ok((buckets, next_page_token)) => {
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
            );
            xml.push_str("  <Owner>\n");
            xml.push_str(&format!("    <ID>{}</ID>\n", claims.tenant_id));
            // S3 requires DisplayName; Anvil's tenant identifier is the stable owner label.
            xml.push_str(&format!(
                "    <DisplayName>{}</DisplayName>\n",
                claims.tenant_id
            ));
            xml.push_str("  </Owner>\n");
            xml.push_str("  <Buckets>\n");
            for b in buckets {
                xml.push_str("    <Bucket>\n");
                xml.push_str(&format!("      <Name>{}</Name>\n", xml_escape(&b.name)));
                xml.push_str(&format!(
                    "      <CreationDate>{}</CreationDate>\n",
                    b.created_at.to_rfc3339()
                ));
                xml.push_str("    </Bucket>\n");
            }
            xml.push_str("  </Buckets>\n");
            if !next_page_token.is_empty() {
                xml.push_str(&format!(
                    "  <ContinuationToken>{}</ContinuationToken>\n",
                    xml_escape(&next_page_token)
                ));
            }
            xml.push_str("</ListAllMyBucketsResult>\n");

            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => match status.code() {
            tonic::Code::InvalidArgument | tonic::Code::Aborted => s3_error(
                "InvalidArgument",
                status.message(),
                axum::http::StatusCode::BAD_REQUEST,
            ),
            tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

pub(super) async fn create_bucket(
    State(state): State<AppState>,
    Path(mut bucket): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    // The S3 `CreateBucket` operation can contain an XML body with the location
    // constraint. The gateway consumes it here because region routing is derived
    // from the Anvil mesh route rather than the S3 XML body.
    if let Some((bucket, key)) = s3_routed_object(&req) {
        return Box::pin(put_object(State(state), Path((bucket, key)), Query(q), req)).await;
    }
    bucket = s3_routed_bucket(&req, bucket);

    // Claims may be absent for anonymous; handler will enforce bucket public access
    let claims = req.extensions().get::<Claims>().cloned();
    let claims = match claims {
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
        .expect("authenticated create bucket path supplied claims");

    if q.contains_key("versioning") {
        return put_bucket_versioning_response(state, claims, &bucket, req).await;
    }

    let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    let region = if !bytes.is_empty() {
        if let Ok(config) = quick_xml::de::from_reader::<_, CreateBucketConfiguration>(&bytes[..]) {
            config.location_constraint.unwrap_or(state.region.clone())
        } else {
            state.region.clone()
        }
    } else {
        state.region.clone()
    };

    match state
        .bucket_manager
        .create_bucket(&claims, &bucket, &region)
        .await
    {
        Ok(_) => (axum::http::StatusCode::OK, "").into_response(),
        Err(status) => match status.code() {
            tonic::Code::AlreadyExists => s3_error(
                "BucketAlreadyExists",
                status.message(),
                axum::http::StatusCode::CONFLICT,
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
        },
    }
}

pub(super) async fn get_bucket_versioning_response(
    state: AppState,
    claims: Claims,
    bucket: &str,
) -> Response {
    match s3_remote_bucket_response_for_authorized_claims(
        &state,
        &claims,
        bucket,
        AnvilAction::BucketRead,
    )
    .await
    {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    match bucket_journal::read_current_bucket(&state.storage, claims.tenant_id, bucket).await {
        Ok(Some(_)) => {
            let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Status>Enabled</Status>\n</VersioningConfiguration>\n";
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Ok(None) => s3_error(
            "NoSuchBucket",
            "The specified bucket does not exist",
            axum::http::StatusCode::NOT_FOUND,
        ),
        Err(e) => s3_error(
            "InternalError",
            &e.to_string(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

pub(super) async fn put_bucket_versioning_response(
    state: AppState,
    claims: Claims,
    bucket: &str,
    req: Request,
) -> Response {
    match s3_remote_bucket_response_for_authorized_claims(
        &state,
        &claims,
        bucket,
        AnvilAction::BucketWrite,
    )
    .await
    {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    match bucket_journal::read_current_bucket(&state.storage, claims.tenant_id, bucket).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return s3_error(
                "NoSuchBucket",
                "The specified bucket does not exist",
                axum::http::StatusCode::NOT_FOUND,
            );
        }
        Err(e) => {
            return s3_error(
                "InternalError",
                &e.to_string(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    }

    let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    if !bytes.is_empty() {
        match quick_xml::de::from_reader::<_, BucketVersioningConfigurationXml>(&bytes[..]) {
            Ok(config) => {
                if config
                    .status
                    .as_deref()
                    .is_some_and(|status| status != "Enabled")
                {
                    return s3_error(
                        "NotImplemented",
                        "Bucket versioning can only be enabled",
                        axum::http::StatusCode::NOT_IMPLEMENTED,
                    );
                }
            }
            Err(e) => {
                return s3_error(
                    "MalformedXML",
                    &format!("Invalid versioning configuration: {e}"),
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        }
    }

    (axum::http::StatusCode::OK, "").into_response()
}

pub(super) async fn delete_bucket(
    State(state): State<AppState>,
    Path(mut bucket): Path<String>,
    req: Request,
) -> Response {
    if let Some((bucket, key)) = s3_routed_object(&req) {
        let q = s3_query_map(req.uri());
        return Box::pin(delete_object(
            State(state),
            Path((bucket, key)),
            Query(q),
            req,
        ))
        .await;
    }
    bucket = s3_routed_bucket(&req, bucket);

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

    match s3_remote_bucket_response_for_authorized_claims(
        &state,
        &claims,
        &bucket,
        AnvilAction::BucketDelete,
    )
    .await
    {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    match state.bucket_manager.delete_bucket(&claims, &bucket).await {
        Ok(_) => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap(),
        Err(status) => match status.code() {
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::NotFound => s3_error(
                "NoSuchBucket",
                status.message(),
                axum::http::StatusCode::NOT_FOUND,
            ),
            tonic::Code::InvalidArgument => s3_error(
                "InvalidArgument",
                status.message(),
                axum::http::StatusCode::BAD_REQUEST,
            ),
            tonic::Code::FailedPrecondition => s3_error(
                "BucketNotEmpty",
                status.message(),
                axum::http::StatusCode::CONFLICT,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

pub(super) async fn head_bucket(
    State(state): State<AppState>,
    Path(mut bucket_name): Path<String>,
    req: Request,
) -> Response {
    if let Some((bucket, key)) = s3_routed_object(&req) {
        let q = s3_query_map(req.uri());
        return Box::pin(head_object(
            State(state),
            Path((bucket, key)),
            Query(q),
            req,
        ))
        .await;
    }
    bucket_name = s3_routed_bucket(&req, bucket_name);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials for HEAD request",
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
        .expect("authenticated head bucket path supplied claims");

    match s3_remote_bucket_response_for_authorized_claims(
        &state,
        &claims,
        &bucket_name,
        AnvilAction::BucketRead,
    )
    .await
    {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    match bucket_journal::read_current_bucket(&state.storage, claims.tenant_id, &bucket_name).await
    {
        Ok(Some(bucket)) => {
            if bucket.region != state.region {
                return s3_remote_bucket_response(
                    state.config.cross_region_routing_policy,
                    &bucket.region,
                    false,
                );
            }
            (axum::http::StatusCode::OK, "").into_response()
        }
        Ok(None) => s3_error(
            "NoSuchBucket",
            "The specified bucket does not exist",
            axum::http::StatusCode::NOT_FOUND,
        ),
        Err(e) => s3_error(
            "InternalError",
            &e.to_string(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

pub(super) async fn list_objects(
    State(state): State<AppState>,
    bucket: Path<String>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some((bucket, key)) = s3_routed_object(&req) {
        return Box::pin(get_object(State(state), Path((bucket, key)), Query(q), req)).await;
    }
    let Path(bucket) = bucket;
    let bucket = s3_routed_bucket(&req, bucket);
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

    if q.contains_key("versions") {
        let request_is_authenticated = req.extensions().get::<Claims>().is_some();
        return list_object_versions_response(
            state,
            claims,
            checked_route.tenant_id,
            &bucket,
            &q,
            request_is_authenticated,
        )
        .await;
    }

    if q.contains_key("uploads") {
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
        return list_multipart_uploads_response(state, claims, &bucket, &q).await;
    }

    if q.contains_key("versioning") {
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
        return get_bucket_versioning_response(state, claims, &bucket).await;
    }

    if q.contains_key("location") {
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
        return get_bucket_location_response(state, claims, &bucket).await;
    }

    let is_list_v2 = q
        .get("list-type")
        .or_else(|| q.get("listType"))
        .is_some_and(|value| value == "2");
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let continuation_token = q
        .get("continuation-token")
        .or_else(|| q.get("continuationToken"))
        .cloned();
    let marker = q.get("marker").cloned().unwrap_or_default();
    let start_after = if is_list_v2 {
        continuation_token.clone().unwrap_or_else(|| {
            q.get("start-after")
                .or_else(|| q.get("startAfter"))
                .cloned()
                .unwrap_or_default()
        })
    } else {
        marker.clone()
    };
    let delimiter = q.get("delimiter").cloned().unwrap_or_default();
    let max_keys: i32 = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let fetch_limit = max_keys.saturating_add(1);

    match state
        .object_manager
        .list_objects_for_tenant(
            claims,
            checked_route.tenant_id,
            &bucket,
            &prefix,
            &start_after,
            fetch_limit,
            &delimiter,
            ObjectReadConsistency::Latest,
        )
        .await
    {
        Ok((objects, common_prefixes)) => {
            let requested_max_keys = if max_keys <= 0 {
                1000
            } else {
                max_keys as usize
            };
            let (entries, is_truncated, next_marker) =
                paginate_list_bucket_entries(objects, common_prefixes, requested_max_keys);
            let key_count = entries.len() as i32;
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
",
            );
            xml.push_str(&format!("  <Name>{}</Name>\n", &*bucket));
            xml.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            if is_list_v2 {
                if let Some(token) = continuation_token {
                    xml.push_str(&format!(
                        "  <ContinuationToken>{}</ContinuationToken>\n",
                        xml_escape(&token)
                    ));
                }
                xml.push_str(&format!("  <KeyCount>{}</KeyCount>\n", key_count));
            } else {
                xml.push_str(&format!("  <Marker>{}</Marker>\n", xml_escape(&marker)));
            }
            if !delimiter.is_empty() {
                xml.push_str(&format!(
                    "  <Delimiter>{}</Delimiter>\n",
                    xml_escape(&delimiter)
                ));
            }
            xml.push_str(&format!("  <MaxKeys>{}</MaxKeys>\n", max_keys));
            xml.push_str(&format!(
                "  <IsTruncated>{}</IsTruncated>\n",
                if is_truncated { "true" } else { "false" }
            ));
            if let Some(token) = next_marker {
                if is_list_v2 {
                    xml.push_str(&format!(
                        "  <NextContinuationToken>{}</NextContinuationToken>\n",
                        xml_escape(&token)
                    ));
                } else {
                    xml.push_str(&format!(
                        "  <NextMarker>{}</NextMarker>\n",
                        xml_escape(&token)
                    ));
                }
            }
            for entry in entries {
                append_list_bucket_entry_xml(&mut xml, entry);
            }
            xml.push_str("</ListBucketResult>\n");

            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
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
                        "NoSuchBucket",
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

pub(super) async fn post_bucket(
    State(state): State<AppState>,
    Path(mut bucket): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some((bucket, key)) = s3_routed_object(&req) {
        return Box::pin(post_object(
            State(state),
            Path((bucket, key)),
            Query(q),
            req,
        ))
        .await;
    }
    bucket = s3_routed_bucket(&req, bucket);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(claims) => claims,
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
        .expect("authenticated post bucket path supplied claims");

    if q.contains_key("delete") {
        let bytes = match axum::body::to_bytes(req.into_body(), 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return s3_error(
                    "InvalidRequest",
                    &format!("Failed to read DeleteObjects body: {error}"),
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        return delete_objects(state, claims, bucket, bytes).await;
    }

    s3_error(
        "InvalidArgument",
        "Unsupported bucket POST operation",
        axum::http::StatusCode::BAD_REQUEST,
    )
}

pub(super) async fn delete_objects(
    state: AppState,
    claims: Claims,
    bucket: String,
    body: axum::body::Bytes,
) -> Response {
    let request = match quick_xml::de::from_reader::<_, DeleteObjectsXml>(&body[..]) {
        Ok(request) => request,
        Err(error) => {
            return s3_error(
                "MalformedXML",
                &format!("Invalid DeleteObjects body: {error}"),
                axum::http::StatusCode::BAD_REQUEST,
            );
        }
    };

    let quiet = request.quiet.unwrap_or(false);
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for object in request.objects {
        let key = object.key;
        let requested_version_id = object.version_id;

        if validation::is_reserved_internal_key(&key) {
            errors.push(DeleteObjectError {
                key,
                version_id: requested_version_id,
                code: "UnauthorizedReservedNamespace".to_string(),
                message: "UnauthorizedReservedNamespace".to_string(),
            });
            continue;
        }

        let version_id = match requested_version_id.as_deref() {
            Some("") | None => None,
            Some(version_id) => match uuid::Uuid::parse_str(version_id) {
                Ok(version_id) => Some(version_id),
                Err(_) => {
                    errors.push(DeleteObjectError {
                        key,
                        version_id: requested_version_id,
                        code: "InvalidArgument".to_string(),
                        message: "Invalid versionId".to_string(),
                    });
                    continue;
                }
            },
        };

        let delete_result = if let Some(version_id) = version_id {
            state
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
        } else {
            state
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
        };

        match delete_result {
            Ok(delete_marker) => {
                if !quiet {
                    deleted.push(DeletedObject {
                        key,
                        version_id: requested_version_id,
                        delete_marker: Some(delete_marker.deleted_at.is_some()),
                        delete_marker_version_id: delete_marker
                            .deleted_at
                            .is_some()
                            .then(|| delete_marker.version_id.to_string()),
                    });
                }
            }
            Err(status) if status.code() == tonic::Code::NotFound => {
                if !quiet {
                    deleted.push(DeletedObject {
                        key,
                        version_id: None,
                        delete_marker: None,
                        delete_marker_version_id: None,
                    });
                }
            }
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                if let Some(response) = s3_remote_bucket_response_from_status(
                    &status,
                    state.config.cross_region_routing_policy,
                ) {
                    return response;
                }
                errors.push(DeleteObjectError::from_status(
                    key,
                    requested_version_id,
                    status,
                ));
            }
            Err(status) => {
                if let Some(response) = s3_remote_bucket_response_from_status(
                    &status,
                    state.config.cross_region_routing_policy,
                ) {
                    return response;
                }
                errors.push(DeleteObjectError::from_status(
                    key,
                    requested_version_id,
                    status,
                ));
            }
        }
    }

    delete_objects_result_response(deleted, errors)
}

#[derive(Debug)]
pub(super) struct DeletedObject {
    key: String,
    version_id: Option<String>,
    delete_marker: Option<bool>,
    delete_marker_version_id: Option<String>,
}

#[derive(Debug)]
pub(super) struct DeleteObjectError {
    key: String,
    version_id: Option<String>,
    code: String,
    message: String,
}

impl DeleteObjectError {
    fn from_status(key: String, version_id: Option<String>, status: tonic::Status) -> Self {
        let code = match status.code() {
            tonic::Code::PermissionDenied => "AccessDenied",
            tonic::Code::InvalidArgument => "InvalidArgument",
            tonic::Code::NotFound => "NoSuchKey",
            tonic::Code::Unimplemented => "NotImplemented",
            _ => "InternalError",
        };
        Self {
            key,
            version_id,
            code: code.to_string(),
            message: status.message().to_string(),
        }
    }
}

pub(super) fn delete_objects_result_response(
    deleted: Vec<DeletedObject>,
    errors: Vec<DeleteObjectError>,
) -> Response {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );

    for object in deleted {
        xml.push_str("  <Deleted>\n");
        xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&object.key)));
        if let Some(version_id) = object.version_id {
            xml.push_str(&format!(
                "    <VersionId>{}</VersionId>\n",
                xml_escape(&version_id)
            ));
        }
        if let Some(delete_marker) = object.delete_marker {
            xml.push_str(&format!(
                "    <DeleteMarker>{}</DeleteMarker>\n",
                if delete_marker { "true" } else { "false" }
            ));
        }
        if let Some(version_id) = object.delete_marker_version_id {
            xml.push_str(&format!(
                "    <DeleteMarkerVersionId>{}</DeleteMarkerVersionId>\n",
                xml_escape(&version_id)
            ));
        }
        xml.push_str("  </Deleted>\n");
    }

    for error in errors {
        xml.push_str("  <Error>\n");
        xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&error.key)));
        if let Some(version_id) = error.version_id {
            xml.push_str(&format!(
                "    <VersionId>{}</VersionId>\n",
                xml_escape(&version_id)
            ));
        }
        xml.push_str(&format!("    <Code>{}</Code>\n", xml_escape(&error.code)));
        xml.push_str(&format!(
            "    <Message>{}</Message>\n",
            xml_escape(&error.message)
        ));
        xml.push_str("  </Error>\n");
    }

    xml.push_str("</DeleteResult>\n");
    Response::builder()
        .status(200)
        .header("Content-Type", "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

pub(super) async fn get_bucket_location_response(
    state: AppState,
    claims: Claims,
    bucket: &str,
) -> Response {
    match s3_remote_bucket_response_for_authorized_claims(
        &state,
        &claims,
        bucket,
        AnvilAction::BucketRead,
    )
    .await
    {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    match bucket_journal::read_current_bucket(&state.storage, claims.tenant_id, bucket).await {
        Ok(Some(bucket)) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>\n",
                xml_escape(&bucket.region)
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .header("x-amz-bucket-region", bucket.region)
                .body(Body::from(xml))
                .unwrap()
        }
        Ok(None) => s3_error(
            "NoSuchBucket",
            "The specified bucket does not exist",
            axum::http::StatusCode::NOT_FOUND,
        ),
        Err(e) => s3_error(
            "InternalError",
            &e.to_string(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

pub(super) async fn list_multipart_uploads_response(
    state: AppState,
    claims: Claims,
    bucket: &str,
    q: &HashMap<String, String>,
) -> Response {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let key_marker = q
        .get("key-marker")
        .or_else(|| q.get("keyMarker"))
        .cloned()
        .unwrap_or_default();
    let upload_id_marker = match q
        .get("upload-id-marker")
        .or_else(|| q.get("uploadIdMarker"))
    {
        Some(value) => match uuid::Uuid::parse_str(value) {
            Ok(upload_id) => Some(upload_id),
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid upload-id-marker",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        },
        None => None,
    };
    let max_uploads: i32 = q
        .get("max-uploads")
        .or_else(|| q.get("maxUploads"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match state
        .object_manager
        .list_multipart_uploads(
            &claims,
            bucket,
            &prefix,
            &key_marker,
            upload_id_marker,
            max_uploads,
        )
        .await
    {
        Ok(page) => {
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <KeyMarker>{}</KeyMarker>\n  <UploadIdMarker>{}</UploadIdMarker>\n",
                xml_escape(bucket),
                xml_escape(&key_marker),
                upload_id_marker
                    .map(|marker| marker.to_string())
                    .as_deref()
                    .map(xml_escape)
                    .unwrap_or_default()
            );
            if let Some(next_key_marker) = page.next_key_marker.as_deref() {
                xml.push_str(&format!(
                    "  <NextKeyMarker>{}</NextKeyMarker>\n",
                    xml_escape(next_key_marker)
                ));
            }
            if let Some(next_upload_id_marker) = page.next_upload_id_marker {
                xml.push_str(&format!(
                    "  <NextUploadIdMarker>{next_upload_id_marker}</NextUploadIdMarker>\n"
                ));
            }
            xml.push_str(&format!(
                "  <Delimiter></Delimiter>\n  <Prefix>{}</Prefix>\n  <MaxUploads>{}</MaxUploads>\n  <IsTruncated>{}</IsTruncated>\n",
                xml_escape(&prefix),
                max_uploads,
                if page.is_truncated { "true" } else { "false" }
            ));
            for upload in page.uploads {
                xml.push_str("  <Upload>\n");
                xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&upload.key)));
                xml.push_str(&format!("    <UploadId>{}</UploadId>\n", upload.upload_id));
                xml.push_str(&format!(
                    "    <Initiated>{}</Initiated>\n",
                    upload.created_at.to_rfc3339()
                ));
                xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
                xml.push_str("  </Upload>\n");
            }
            xml.push_str("</ListMultipartUploadsResult>\n");
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchBucket",
            state.config.cross_region_routing_policy,
        ),
    }
}

pub(super) async fn list_object_versions_response(
    state: AppState,
    claims: Option<Claims>,
    route_tenant_id: Option<i64>,
    bucket: &str,
    q: &HashMap<String, String>,
    request_is_authenticated: bool,
) -> Response {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let key_marker = q
        .get("key-marker")
        .or_else(|| q.get("keyMarker"))
        .cloned()
        .unwrap_or_default();
    let version_id_marker = q
        .get("version-id-marker")
        .or_else(|| q.get("versionIdMarker"))
        .cloned()
        .unwrap_or_default();
    let max_keys: i32 = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match state
        .object_manager
        .list_object_versions_for_tenant(
            claims,
            route_tenant_id,
            bucket,
            &prefix,
            &key_marker,
            &version_id_marker,
            max_keys,
            ObjectReadConsistency::Latest,
        )
        .await
    {
        Ok(page) => {
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
            );
            xml.push_str(&format!("  <Name>{}</Name>\n", xml_escape(bucket)));
            xml.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            xml.push_str(&format!(
                "  <KeyMarker>{}</KeyMarker>\n",
                xml_escape(&key_marker)
            ));
            xml.push_str(&format!(
                "  <VersionIdMarker>{}</VersionIdMarker>\n",
                xml_escape(&version_id_marker)
            ));
            xml.push_str(&format!("  <MaxKeys>{}</MaxKeys>\n", max_keys));
            if let Some(next_key_marker) = page.next_key_marker.as_deref() {
                xml.push_str(&format!(
                    "  <NextKeyMarker>{}</NextKeyMarker>\n",
                    xml_escape(next_key_marker)
                ));
            }
            if let Some(next_version_id_marker) = page.next_version_id_marker {
                xml.push_str(&format!(
                    "  <NextVersionIdMarker>{}</NextVersionIdMarker>\n",
                    next_version_id_marker
                ));
            }
            xml.push_str(&format!(
                "  <IsTruncated>{}</IsTruncated>\n",
                if page.is_truncated { "true" } else { "false" }
            ));
            for version in page.versions {
                let object = version.object;
                let tag = if version.is_delete_marker {
                    "DeleteMarker"
                } else {
                    "Version"
                };
                xml.push_str(&format!("  <{}>\n", tag));
                xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&object.key)));
                xml.push_str(&format!(
                    "    <VersionId>{}</VersionId>\n",
                    object.version_id
                ));
                xml.push_str(&format!(
                    "    <IsLatest>{}</IsLatest>\n",
                    if version.is_latest { "true" } else { "false" }
                ));
                xml.push_str(&format!(
                    "    <LastModified>{}</LastModified>\n",
                    object.created_at.to_rfc3339()
                ));
                if !version.is_delete_marker {
                    xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", object.etag));
                    xml.push_str(&format!("    <Size>{}</Size>\n", object.size));
                    xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
                }
                xml.push_str(&format!("  </{}>\n", tag));
            }
            xml.push_str("</ListVersionsResult>\n");

            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(
            status,
            request_is_authenticated,
            "NoSuchBucket",
            state.config.cross_region_routing_policy,
        ),
    }
}

pub(super) enum ListBucketEntry {
    Object(Object),
    Prefix(String),
}

pub(super) fn paginate_list_bucket_entries(
    objects: Vec<Object>,
    common_prefixes: Vec<String>,
    requested_max_keys: usize,
) -> (Vec<ListBucketEntry>, bool, Option<String>) {
    let mut entries = objects
        .into_iter()
        .map(ListBucketEntry::Object)
        .chain(common_prefixes.into_iter().map(ListBucketEntry::Prefix))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.marker()
            .cmp(right.marker())
            .then_with(|| left.kind_order().cmp(&right.kind_order()))
    });

    let is_truncated = entries.len() > requested_max_keys;
    if is_truncated {
        entries.truncate(requested_max_keys);
    }
    let next_continuation_token = is_truncated
        .then(|| entries.last().map(|entry| entry.marker().to_string()))
        .flatten();

    (entries, is_truncated, next_continuation_token)
}

impl ListBucketEntry {
    fn marker(&self) -> &str {
        match self {
            Self::Object(object) => &object.key,
            Self::Prefix(prefix) => prefix,
        }
    }

    fn kind_order(&self) -> u8 {
        match self {
            Self::Object(_) => 0,
            Self::Prefix(_) => 1,
        }
    }
}

pub(super) fn append_list_bucket_entry_xml(xml: &mut String, entry: ListBucketEntry) {
    match entry {
        ListBucketEntry::Object(object) => {
            xml.push_str("  <Contents>\n");
            xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&object.key)));
            xml.push_str(&format!(
                "    <LastModified>{}</LastModified>\n",
                object.created_at.to_rfc3339()
            ));
            xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", object.etag));
            xml.push_str(&format!("    <Size>{}</Size>\n", object.size));
            xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
            xml.push_str("  </Contents>\n");
        }
        ListBucketEntry::Prefix(prefix) => {
            xml.push_str("  <CommonPrefixes>\n");
            xml.push_str(&format!("    <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            xml.push_str("  </CommonPrefixes>\n");
        }
    }
}

#[cfg(test)]
mod list_bucket_pagination_tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn list_bucket_pagination_uses_last_returned_entry_as_continuation_token() {
        let (entries, is_truncated, token) = paginate_list_bucket_entries(
            vec![object("page/a.txt"), object("page/b.txt")],
            Vec::new(),
            1,
        );

        assert!(is_truncated);
        assert_eq!(token.as_deref(), Some("page/a.txt"));
        assert_eq!(
            entries
                .iter()
                .map(ListBucketEntry::marker)
                .collect::<Vec<_>>(),
            vec!["page/a.txt"]
        );
    }

    #[test]
    fn list_bucket_pagination_merges_objects_and_common_prefixes_before_truncating() {
        let (entries, is_truncated, token) = paginate_list_bucket_entries(
            vec![object("root/b.txt")],
            vec!["root/a/".to_string(), "root/c/".to_string()],
            2,
        );

        assert!(is_truncated);
        assert_eq!(token.as_deref(), Some("root/b.txt"));
        assert_eq!(
            entries
                .iter()
                .map(ListBucketEntry::marker)
                .collect::<Vec<_>>(),
            vec!["root/a/", "root/b.txt"]
        );
    }

    fn object(key: &str) -> Object {
        Object {
            id: 0,
            tenant_id: 0,
            bucket_id: 0,
            key: key.to_string(),
            kind: object_links::ObjectEntryKind::Blob,
            content_hash: String::new(),
            size: 0,
            etag: String::new(),
            content_type: None,
            version_id: uuid::Uuid::nil(),
            mutation_id: uuid::Uuid::nil(),
            index_policy_snapshot: String::new(),
            user_metadata_hash: String::new(),
            authz_revision: 0,
            record_hash: String::new(),
            created_at: Utc::now(),
            deleted_at: None,
            storage_class: None,
            user_meta: None,
            shard_map: None,
            checksum: None,
            link: None,
        }
    }
}

pub(super) async fn readiness_check(State(state): State<AppState>) -> Response {
    let coremeta = state.core_store.coremeta_recovery_snapshot();
    if coremeta.ready {
        (axum::http::StatusCode::OK, "READY").into_response()
    } else {
        let body = serde_json::json!({
            "status": "not_ready",
            "coremeta": {
                "ready": coremeta.ready,
                "distributed_required": coremeta.distributed_required,
                "in_progress": coremeta.in_progress,
                "reachable_peers": coremeta.reachable_peers,
                "known_roots": coremeta.known_roots,
                "lagging_roots": coremeta.lagging_roots,
                "root_directory_complete": coremeta.root_directory_complete,
                "canonical_settlement_complete": coremeta.canonical_settlement_complete,
                "physical_register_quorum_complete": coremeta.physical_register_quorum_complete,
                "completed_rounds": coremeta.completed_rounds,
                "last_error": coremeta.last_error,
            }
        });
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::response::Json(body),
        )
            .into_response()
    }
}
