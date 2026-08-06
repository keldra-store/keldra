use std::collections::HashMap;
use std::sync::Arc;

use anvil_api::v1::object_chunk::Value as ChunkValue;
use anvil_api::v1::object_head::State as HeadState;
use anvil_api::v1::{CreateBucketRequest, ObjectHead, ObjectVersioning};
use anvil_store::ObjectKey;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{self, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use bytes::Bytes;
use tokio_stream::StreamExt as _;

use crate::authentication::{JwtManager, RequestRateLimits};
use crate::distributed_control_plane::DistributedControlPlane;
use crate::serving_fence::ServingAuthority;
use crate::v05::{GatewayIdentity, GatewayObjectAdapter, GatewayPutMode};

mod auth;
mod aws_chunked;

const MAX_PAGE_SIZE: u32 = 1_000;

#[derive(Clone)]
pub(crate) struct S3State {
    pub(crate) objects: GatewayObjectAdapter,
    pub(crate) control: Arc<DistributedControlPlane>,
    pub(crate) tokens: JwtManager,
    pub(crate) rate_limits: RequestRateLimits,
    pub(crate) serving: ServingAuthority,
    pub(crate) mutation_admission: crate::mutation_admission::MutationAdmission,
}

pub(crate) fn router(state: S3State) -> Router {
    Router::new()
        .route("/ready", get(ready))
        .route("/", any(root))
        .route("/{bucket}", any(bucket_route))
        .route("/{bucket}/", any(bucket_route))
        .route("/{bucket}/{*path}", any(object_route))
        .with_state(state)
}

async fn ready() -> &'static str {
    "ready\n"
}

async fn root() -> Response {
    S3Error::not_implemented("ListBuckets is not part of the 0.5.3 S3 surface").into_response()
}

async fn bucket_route(
    State(state): State<S3State>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    mut request: Request,
) -> Response {
    let identity = match request_identity(&state, &mut request).await {
        Ok(identity) => identity,
        Err(error) => return error.into_response(),
    };
    if identity.caller().is_none() {
        return S3Error::access_denied("Unsigned object reads use /{tenant}/{bucket}/{key}")
            .into_response();
    }
    match *request.method() {
        http::Method::PUT => create_bucket(&state, &identity, bucket).await,
        http::Method::HEAD => head_bucket(&state, &identity, bucket).await,
        http::Method::GET if is_list_v2(&query) => {
            let tenant = identity
                .caller()
                .expect("authenticated above")
                .storage_tenant()
                .as_str()
                .to_owned();
            list_objects(&state, &identity, tenant, bucket, query).await
        }
        _ => S3Error::not_implemented("This bucket operation is not implemented").into_response(),
    }
}

async fn object_route(
    State(state): State<S3State>,
    Path((first, remainder)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    mut request: Request,
) -> Response {
    let method = request.method().clone();
    let identity = match request_identity(&state, &mut request).await {
        Ok(identity) => identity,
        Err(error) => return error.into_response(),
    };
    if identity.caller().is_none() && is_list_v2(&query) && !remainder.contains('/') {
        return list_objects(&state, &identity, first, remainder, query).await;
    }
    let (tenant, bucket, path) = match object_address(&identity, first, remainder) {
        Ok(address) => address,
        Err(error) => return error.into_response(),
    };
    let key = match ObjectKey::new(tenant, bucket, path) {
        Ok(key) => key,
        Err(error) => return S3Error::invalid_request(&error.to_string()).into_response(),
    };
    match method {
        http::Method::GET => get_object(&state, &identity, key, request.headers()).await,
        http::Method::HEAD => head_object(&state, &identity, key, request.headers()).await,
        http::Method::PUT => put_object(&state, &identity, key, request).await,
        http::Method::DELETE => delete_object(&state, &identity, key).await,
        _ => S3Error::not_implemented("This object operation is not implemented").into_response(),
    }
}

async fn request_identity(
    state: &S3State,
    request: &mut Request,
) -> Result<GatewayIdentity, S3Error> {
    Ok(auth::authenticate(state, request)
        .await?
        .unwrap_or(GatewayIdentity::Anonymous))
}

fn object_address(
    identity: &GatewayIdentity,
    first: String,
    remainder: String,
) -> Result<(String, String, String), S3Error> {
    if let Some(caller) = identity.caller() {
        return Ok((
            caller.storage_tenant().as_str().to_owned(),
            first,
            remainder,
        ));
    }
    let (bucket, path) = remainder.split_once('/').ok_or_else(|| {
        S3Error::invalid_request("Unsigned reads require /{tenant}/{bucket}/{key}")
    })?;
    if bucket.is_empty() || path.is_empty() {
        return Err(S3Error::invalid_request(
            "Unsigned reads require a non-empty tenant, bucket and key",
        ));
    }
    Ok((first, bucket.to_owned(), path.to_owned()))
}

async fn create_bucket(state: &S3State, identity: &GatewayIdentity, bucket: String) -> Response {
    let _permit = match state.mutation_admission.enter() {
        Ok(permit) => permit,
        Err(error) => return status_error(error, "ServiceUnavailable").into_response(),
    };
    let Some(caller) = identity.caller().cloned() else {
        return S3Error::access_denied("Bucket creation requires credentials").into_response();
    };
    let bearer = match identity.original_bearer() {
        Ok(bearer) => bearer,
        Err(error) => return status_error(error, "NoSuchBucket").into_response(),
    };
    match state
        .control
        .create_bucket(
            caller,
            bearer,
            CreateBucketRequest {
                bucket: bucket.clone(),
                versioning: ObjectVersioning::Unversioned as i32,
            },
        )
        .await
    {
        Ok(_) => Response::builder()
            .status(StatusCode::OK)
            .header(http::header::LOCATION, format!("/{bucket}"))
            .body(Body::empty())
            .expect("static S3 response"),
        Err(error) => status_error(error, "BucketAlreadyExists").into_response(),
    }
}

async fn head_bucket(state: &S3State, identity: &GatewayIdentity, bucket: String) -> Response {
    let tenant = identity
        .caller()
        .expect("head bucket is authenticated")
        .storage_tenant()
        .as_str();
    match state
        .objects
        .list(identity, tenant, &bucket, String::new(), None, 1)
        .await
    {
        Ok(_) => StatusCode::OK.into_response(),
        Err(error) => status_error(error, "NoSuchBucket").into_response(),
    }
}

async fn list_objects(
    state: &S3State,
    identity: &GatewayIdentity,
    tenant: String,
    bucket: String,
    query: HashMap<String, String>,
) -> Response {
    if query
        .get("delimiter")
        .is_some_and(|value| !value.is_empty())
    {
        return S3Error::not_implemented("ListObjectsV2 delimiter grouping is not implemented")
            .into_response();
    }
    let max_keys = query
        .get("max-keys")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1_000)
        .min(MAX_PAGE_SIZE);
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let start_after = query
        .get("continuation-token")
        .or_else(|| query.get("start-after"))
        .filter(|value| !value.is_empty())
        .cloned();
    let page = match state
        .objects
        .list(
            identity,
            &tenant,
            &bucket,
            prefix.clone(),
            start_after,
            max_keys,
        )
        .await
    {
        Ok(page) => page,
        Err(error) => return status_error(error, "NoSuchBucket").into_response(),
    };
    let next = page.has_more.then(|| page.paths.last().cloned()).flatten();
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>");
    element(&mut xml, "Name", &bucket);
    element(&mut xml, "Prefix", &prefix);
    element(&mut xml, "KeyCount", &page.paths.len().to_string());
    element(&mut xml, "MaxKeys", &max_keys.to_string());
    element(
        &mut xml,
        "IsTruncated",
        if page.has_more { "true" } else { "false" },
    );
    if let Some(next) = next {
        element(&mut xml, "NextContinuationToken", &next);
    }
    for path in page.paths {
        xml.push_str("<Contents>");
        element(&mut xml, "Key", &path);
        xml.push_str("</Contents>");
    }
    xml.push_str("</ListBucketResult>");
    xml_response(StatusCode::OK, xml)
}

async fn head_object(
    state: &S3State,
    identity: &GatewayIdentity,
    key: ObjectKey,
    headers: &HeaderMap,
) -> Response {
    match state.objects.head(identity, &key).await {
        Ok(head) => head_response(&head, headers, true),
        Err(error) => status_error(error, "NoSuchKey").into_response(),
    }
}

async fn get_object(
    state: &S3State,
    identity: &GatewayIdentity,
    key: ObjectKey,
    headers: &HeaderMap,
) -> Response {
    let mut stream = match state.objects.get(identity, &key).await {
        Ok(stream) => stream,
        Err(error) => return status_error(error, "NoSuchKey").into_response(),
    };
    let head = match stream.next().await {
        Some(Ok(chunk)) => match chunk.value {
            Some(ChunkValue::Head(head)) => head,
            _ => {
                return S3Error::internal("Object stream did not start with its head")
                    .into_response();
            }
        },
        Some(Err(error)) => return status_error(error, "NoSuchKey").into_response(),
        None => return S3Error::internal("Object stream ended before its head").into_response(),
    };
    let Some(HeadState::Present(present)) = head.state.as_ref() else {
        return S3Error::no_such_key().into_response();
    };
    if let Some(response) = precondition_response(headers, &etag(&present.content_hash)) {
        return response;
    }
    let body_stream = stream.map(|item| match item {
        Ok(chunk) => match chunk.value {
            Some(ChunkValue::Bytes(bytes)) => Ok::<Bytes, std::io::Error>(Bytes::from(bytes)),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "object stream contains a second head",
            )),
        },
        Err(error) => Err(std::io::Error::other(error.to_string())),
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_LENGTH, present.content_length)
        .header(http::header::CONTENT_TYPE, &present.content_type)
        .header(http::header::ETAG, etag(&present.content_hash))
        .header(http::header::ACCEPT_RANGES, "bytes")
        .body(Body::from_stream(body_stream))
        .expect("validated object response headers")
}

fn head_response(head: &ObjectHead, headers: &HeaderMap, empty_body: bool) -> Response {
    let Some(HeadState::Present(present)) = head.state.as_ref() else {
        return S3Error::no_such_key().into_response();
    };
    let etag = etag(&present.content_hash);
    if let Some(response) = precondition_response(headers, &etag) {
        return response;
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_LENGTH, present.content_length)
        .header(http::header::CONTENT_TYPE, &present.content_type)
        .header(http::header::ETAG, etag)
        .header(http::header::ACCEPT_RANGES, "bytes")
        .body(if empty_body {
            Body::empty()
        } else {
            Body::from("")
        })
        .expect("validated object head headers")
}

async fn put_object(
    state: &S3State,
    identity: &GatewayIdentity,
    key: ObjectKey,
    mut request: Request,
) -> Response {
    if identity.caller().is_none() {
        return S3Error::access_denied("Object writes require credentials").into_response();
    }
    let _permit = match state.mutation_admission.enter() {
        Ok(permit) => permit,
        Err(error) => return status_error(error, "ServiceUnavailable").into_response(),
    };
    let mode = match put_mode(state, identity, &key, request.headers()).await {
        Ok(mode) => mode,
        Err(error) => return error.into_response(),
    };
    let expected_sha256 = match aws_chunked::prepare_body(&mut request) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let content_type = request
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let command_id = format!("s3-put-{}", uuid::Uuid::new_v4());
    match state
        .objects
        .put(
            identity,
            &key,
            content_type,
            command_id,
            mode,
            expected_sha256,
            request.into_body(),
        )
        .await
    {
        Ok(result) => {
            let head = match state.objects.head(identity, &key).await {
                Ok(head) => head,
                Err(error) => return status_error(error, "NoSuchKey").into_response(),
            };
            let Some(HeadState::Present(present)) = head.state else {
                return S3Error::internal("Published object has no live head").into_response();
            };
            tracing::info!(
                monotonic_counter.anvil_gateway_ingress_bytes_total = result.content_length,
                gateway = "s3",
                tenant = key.tenant(),
                bucket = key.bucket(),
                "S3 object upload completed"
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(http::header::ETAG, etag(&present.content_hash))
                .header("x-amz-version-id", result.receipt.version)
                .body(Body::empty())
                .expect("validated put response")
        }
        Err(error) => status_error(error, "NoSuchKey").into_response(),
    }
}

async fn put_mode(
    state: &S3State,
    identity: &GatewayIdentity,
    key: &ObjectKey,
    headers: &HeaderMap,
) -> Result<GatewayPutMode, S3Error> {
    if headers
        .get(http::header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() == "*")
    {
        return Ok(GatewayPutMode::IfAbsent);
    }
    if let Some(expected) = headers
        .get(http::header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
    {
        let head = state
            .objects
            .head(identity, key)
            .await
            .map_err(|error| status_error(error, "NoSuchKey"))?;
        let Some(HeadState::Present(present)) = head.state else {
            return Err(S3Error::precondition_failed());
        };
        if !etag_matches(expected, &etag(&present.content_hash)) {
            return Err(S3Error::precondition_failed());
        }
        return Ok(GatewayPutMode::IfVersion(present.version));
    }
    Ok(GatewayPutMode::Put)
}

async fn delete_object(state: &S3State, identity: &GatewayIdentity, key: ObjectKey) -> Response {
    if identity.caller().is_none() {
        return S3Error::access_denied("Object deletion requires credentials").into_response();
    }
    let _permit = match state.mutation_admission.enter() {
        Ok(permit) => permit,
        Err(error) => return status_error(error, "ServiceUnavailable").into_response(),
    };
    match state
        .objects
        .delete(
            identity,
            &key,
            format!("s3-delete-{}", uuid::Uuid::new_v4()),
        )
        .await
    {
        Ok(receipt) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("x-amz-version-id", receipt.version)
            .body(Body::empty())
            .expect("static delete response"),
        Err(error) => status_error(error, "NoSuchKey").into_response(),
    }
}

fn is_list_v2(query: &HashMap<String, String>) -> bool {
    query.get("list-type").is_some_and(|value| value == "2")
}

fn precondition_response(headers: &HeaderMap, current_etag: &str) -> Option<Response> {
    if headers
        .get(http::header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !etag_matches(value, current_etag))
    {
        return Some(S3Error::precondition_failed().into_response());
    }
    if headers
        .get(http::header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| etag_matches(value, current_etag))
    {
        return Some(
            Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header(http::header::ETAG, current_etag)
                .body(Body::empty())
                .expect("static not-modified response"),
        );
    }
    None
}

fn etag(hash: &[u8]) -> String {
    format!("\"{}\"", hex::encode(hash))
}

fn etag_matches(condition: &str, current: &str) -> bool {
    condition
        .split(',')
        .map(str::trim)
        .any(|value| value == "*" || value.trim_start_matches("W/") == current)
}

fn status_error(status: tonic::Status, not_found: &'static str) -> S3Error {
    match status.code() {
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => {
            S3Error::access_denied(status.message())
        }
        tonic::Code::NotFound => S3Error::new(not_found, status.message(), StatusCode::NOT_FOUND),
        tonic::Code::AlreadyExists => S3Error::new(
            "BucketAlreadyOwnedByYou",
            status.message(),
            StatusCode::CONFLICT,
        ),
        tonic::Code::InvalidArgument => S3Error::invalid_request(status.message()),
        tonic::Code::FailedPrecondition | tonic::Code::Aborted => S3Error::precondition_failed(),
        tonic::Code::ResourceExhausted => S3Error::new(
            "EntityTooLarge",
            status.message(),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => S3Error::new(
            "ServiceUnavailable",
            status.message(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        _ => S3Error::internal(status.message()),
    }
}

#[derive(Debug)]
pub(crate) struct S3Error {
    code: &'static str,
    message: String,
    status: StatusCode,
}

impl S3Error {
    fn new(code: &'static str, message: impl Into<String>, status: StatusCode) -> Self {
        Self {
            code,
            message: message.into(),
            status,
        }
    }

    fn access_denied(message: impl Into<String>) -> Self {
        Self::new("AccessDenied", message, StatusCode::FORBIDDEN)
    }

    fn invalid_access_key() -> Self {
        Self::new(
            "InvalidAccessKeyId",
            "The access key ID does not exist",
            StatusCode::FORBIDDEN,
        )
    }

    fn signature_mismatch() -> Self {
        Self::new(
            "SignatureDoesNotMatch",
            "The request signature does not match",
            StatusCode::FORBIDDEN,
        )
    }

    fn invalid_request(message: impl Into<String>) -> Self {
        Self::new("InvalidRequest", message, StatusCode::BAD_REQUEST)
    }

    fn not_implemented(message: impl Into<String>) -> Self {
        Self::new("NotImplemented", message, StatusCode::NOT_IMPLEMENTED)
    }

    fn no_such_key() -> Self {
        Self::new(
            "NoSuchKey",
            "The specified key does not exist",
            StatusCode::NOT_FOUND,
        )
    }

    fn precondition_failed() -> Self {
        Self::new(
            "PreconditionFailed",
            "At least one precondition did not hold",
            StatusCode::PRECONDITION_FAILED,
        )
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new("InternalError", message, StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let request_id = uuid::Uuid::new_v4().simple().to_string();
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{}</Code><Message>{}</Message><RequestId>{}</RequestId></Error>",
            self.code,
            xml_escape(&self.message),
            request_id
        );
        Response::builder()
            .status(self.status)
            .header(http::header::CONTENT_TYPE, "application/xml")
            .header("x-amz-request-id", request_id)
            .body(Body::from(body))
            .expect("static S3 error response")
    }
}

fn xml_response(status: StatusCode, body: String) -> Response {
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .expect("static XML response")
}

fn element(output: &mut String, name: &str, value: &str) {
    output.push('<');
    output.push_str(name);
    output.push('>');
    output.push_str(&xml_escape(value));
    output.push_str("</");
    output.push_str(name);
    output.push('>');
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_and_authenticated_paths_have_unambiguous_tenants() {
        let public = object_address(
            &GatewayIdentity::Anonymous,
            "acme".into(),
            "assets/site/logo.svg".into(),
        )
        .unwrap();
        assert_eq!(
            public,
            ("acme".into(), "assets".into(), "site/logo.svg".into())
        );
    }

    #[test]
    fn xml_values_are_escaped() {
        assert_eq!(xml_escape("a<&\"'"), "a&lt;&amp;&quot;&apos;");
    }
}
