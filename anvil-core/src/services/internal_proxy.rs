use crate::anvil_api::internal_proxy_service_server::InternalProxyService;
use crate::anvil_api::*;
use crate::object_manager::{
    ObjectLinkReadMode, ObjectReadConsistency, ObjectWriteOptions, ObjectWriteVisibility,
};
use crate::{AppState, auth, system_realm};
use futures_util::StreamExt;
use http::HeaderValue;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl InternalProxyService for AppState {
    type ProxyObjectStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<ProxyResponseChunk, Status>> + Send>,
    >;

    async fn proxy_object(
        &self,
        request: Request<tonic::Streaming<ProxyRequestChunk>>,
    ) -> Result<Response<Self::ProxyObjectStream>, Status> {
        let internal_claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?
            .clone();

        let mut stream = request.into_inner();
        let header = match stream.next().await {
            Some(Ok(chunk)) => match chunk.part {
                Some(proxy_request_chunk::Part::Header(header)) => header,
                Some(proxy_request_chunk::Part::Body(_)) => {
                    return Err(Status::invalid_argument(
                        "first proxy request chunk must be a header",
                    ));
                }
                Some(proxy_request_chunk::Part::Failure(failure)) => {
                    return Err(proxy_stream_failure_status(failure));
                }
                None => return Err(Status::invalid_argument("empty proxy request chunk")),
            },
            Some(Err(status)) => return Err(status),
            None => return Err(Status::invalid_argument("empty proxy request stream")),
        };

        validate_internal_proxy_claims(self, &internal_claims, &header).await?;
        let original_claims = decode_proxy_authz_context(&header)?;
        validate_proxy_principal_matches_header(&original_claims, &header)?;

        match header.method.to_ascii_uppercase().as_str() {
            "GET" => proxy_get_or_head(self, header, original_claims, false).await,
            "HEAD" => proxy_get_or_head(self, header, original_claims, true).await,
            "PUT" => proxy_put(self, header, original_claims, stream).await,
            "NATIVE_PUT" => proxy_native_put(self, header, original_claims, stream).await,
            "DELETE" => proxy_delete(self, header, original_claims).await,
            method => Err(Status::invalid_argument(format!(
                "unsupported proxy method {method}"
            ))),
        }
    }
}

async fn proxy_get_or_head(
    state: &AppState,
    header: ProxyRequestHeader,
    original_claims: auth::Claims,
    head_only: bool,
) -> Result<Response<<AppState as InternalProxyService>::ProxyObjectStream>, Status> {
    let tenant_id = parse_proxy_tenant_id(&header)?;
    let version_id = parse_proxy_version_id(&header)?;
    let consistency = parse_proxy_read_consistency(&header.headers)?;
    let result = if head_only {
        let object = state
            .object_manager
            .head_object_with_link_mode_for_tenant(
                Some(original_claims),
                Some(tenant_id),
                &header.bucket_name,
                &header.object_key,
                version_id,
                ObjectLinkReadMode::Follow,
                consistency,
            )
            .await?;
        ProxyReadEither::Head(object.object)
    } else {
        let range = parse_proxy_read_range(&header.headers)?;
        ProxyReadEither::Get(
            state
                .object_manager
                .get_object_with_link_mode_for_tenant(
                    Some(original_claims),
                    Some(tenant_id),
                    header.bucket_name.clone(),
                    header.object_key.clone(),
                    version_id,
                    range,
                    ObjectLinkReadMode::Follow,
                    consistency,
                )
                .await?,
        )
    };

    let (tx, rx) = mpsc::channel(4);
    tokio::spawn(async move {
        match result {
            ProxyReadEither::Head(object) => {
                let _ = tx
                    .send(Ok(proxy_header_chunk(ProxyResponseHeader {
                        request_id: header.request_id,
                        status: 200,
                        headers: object_response_headers(&object),
                        trailers: Vec::new(),
                        committed: false,
                        native_put_response: None,
                    })))
                    .await;
            }
            ProxyReadEither::Get(result) => {
                let object = result.object;
                let mut headers = object_response_headers(&object);
                headers.push(proxy_header(
                    "x-anvil-range-start",
                    result.range_start.to_string().as_bytes(),
                ));
                if tx
                    .send(Ok(proxy_header_chunk(ProxyResponseHeader {
                        request_id: header.request_id,
                        status: 200,
                        headers,
                        trailers: Vec::new(),
                        committed: false,
                        native_put_response: None,
                    })))
                    .await
                    .is_err()
                {
                    return;
                }
                let mut data_stream = result.stream;
                while let Some(chunk) = data_stream.next().await {
                    match chunk {
                        Ok(bytes) => {
                            if tx
                                .send(Ok(ProxyResponseChunk {
                                    part: Some(proxy_response_chunk::Part::Body(bytes)),
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(status) => {
                            let _ = tx.send(Err(status)).await;
                            break;
                        }
                    }
                }
            }
        }
    });
    Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
}

async fn proxy_put(
    state: &AppState,
    header: ProxyRequestHeader,
    original_claims: auth::Claims,
    stream: tonic::Streaming<ProxyRequestChunk>,
) -> Result<Response<<AppState as InternalProxyService>::ProxyObjectStream>, Status> {
    let tenant_id = parse_proxy_tenant_id(&header)?;
    if tenant_id != original_claims.tenant_id {
        return Err(Status::permission_denied(
            "proxy tenant does not match original principal",
        ));
    }
    if proxy_request_has_write_etag_preconditions(&header.headers) {
        let current = state
            .object_manager
            .current_object_for_write_precondition(
                &original_claims,
                &header.bucket_name,
                &header.object_key,
            )
            .await?;
        evaluate_proxy_write_etag_preconditions(
            &header.headers,
            current.as_ref().map(|object| object.etag.as_str()),
        )?;
    }
    let content_type = proxy_header_string(&header.headers, "content-type");
    let data_stream = stream.map(|chunk_result| match chunk_result {
        Ok(chunk) => match chunk.part {
            Some(proxy_request_chunk::Part::Body(bytes)) => Ok(bytes),
            Some(proxy_request_chunk::Part::Failure(failure)) => {
                Err(proxy_stream_failure_status(failure))
            }
            Some(proxy_request_chunk::Part::Header(_)) => Err(Status::invalid_argument(
                "proxy request may contain only one header chunk",
            )),
            None => Ok(Vec::new()),
        },
        Err(status) => Err(status),
    });

    let object = state
        .object_manager
        .put_object(
            &original_claims,
            &header.bucket_name,
            &header.object_key,
            data_stream,
            ObjectWriteOptions {
                content_type,
                user_metadata: proxy_user_metadata(&header.headers),
                transaction_id: None,
                transaction_principal: None,
                storage_class_id: None,
                ..Default::default()
            },
        )
        .await?;

    unary_proxy_response(
        header.request_id,
        200,
        true,
        object_response_headers(&object),
        None,
    )
}

async fn proxy_native_put(
    state: &AppState,
    header: ProxyRequestHeader,
    original_claims: auth::Claims,
    stream: tonic::Streaming<ProxyRequestChunk>,
) -> Result<Response<<AppState as InternalProxyService>::ProxyObjectStream>, Status> {
    let tenant_id = parse_proxy_tenant_id(&header)?;
    if tenant_id != original_claims.tenant_id {
        return Err(Status::permission_denied(
            "proxy tenant does not match original principal",
        ));
    }
    let metadata = header
        .native_object_metadata
        .clone()
        .ok_or_else(|| Status::invalid_argument("native proxy metadata is required"))?;
    if metadata.bucket_name != header.bucket_name || metadata.object_key != header.object_key {
        return Err(Status::permission_denied(
            "native proxy metadata does not match its routing header",
        ));
    }
    if metadata
        .mutation_context
        .as_ref()
        .is_some_and(|context| context.idempotency_key != header.idempotency_key)
    {
        return Err(Status::permission_denied(
            "native proxy idempotency identity does not match its routing header",
        ));
    }
    // A routed write is already detached from its original client connection.
    // Give the durable mutation its own task boundary as well: this keeps the
    // large authorization/storage future chain off the proxy RPC stack and
    // lets an admitted idempotent mutation finish if the forwarding peer loses
    // its response connection.
    let state = state.clone();
    let response = tokio::spawn(async move {
        crate::services::object::execute_native_put(
            &state,
            original_claims,
            metadata,
            stream.map(native_proxy_data_chunk),
        )
        .await
    })
    .await
    .map_err(|error| Status::internal(format!("native proxy task failed: {error}")))??;
    unary_proxy_response(header.request_id, 200, true, Vec::new(), Some(response))
}

async fn proxy_delete(
    state: &AppState,
    header: ProxyRequestHeader,
    original_claims: auth::Claims,
) -> Result<Response<<AppState as InternalProxyService>::ProxyObjectStream>, Status> {
    let tenant_id = parse_proxy_tenant_id(&header)?;
    if tenant_id != original_claims.tenant_id {
        return Err(Status::permission_denied(
            "proxy tenant does not match original principal",
        ));
    }
    let deleted = if let Some(version_id) = parse_proxy_version_id(&header)? {
        state
            .object_manager
            .delete_object_version(
                &original_claims,
                &header.bucket_name,
                &header.object_key,
                version_id,
                None,
                None,
                ObjectWriteVisibility::default(),
            )
            .await?
    } else {
        state
            .object_manager
            .delete_object(
                &original_claims,
                &header.bucket_name,
                &header.object_key,
                None,
                None,
                ObjectWriteVisibility::default(),
            )
            .await?
    };

    unary_proxy_response(
        header.request_id,
        204,
        true,
        object_response_headers(&deleted),
        None,
    )
}

fn unary_proxy_response(
    request_id: String,
    status: u32,
    committed: bool,
    headers: Vec<ProxyHeader>,
    native_put_response: Option<PutObjectResponse>,
) -> Result<Response<<AppState as InternalProxyService>::ProxyObjectStream>, Status> {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let _ = tx
            .send(Ok(proxy_header_chunk(ProxyResponseHeader {
                request_id,
                status,
                headers,
                trailers: Vec::new(),
                committed,
                native_put_response,
            })))
            .await;
    });
    Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
}

fn native_proxy_data_chunk(
    chunk_result: Result<ProxyRequestChunk, Status>,
) -> Result<Vec<u8>, Status> {
    match chunk_result?.part {
        Some(proxy_request_chunk::Part::Body(bytes)) => Ok(bytes),
        Some(proxy_request_chunk::Part::Failure(failure)) => {
            Err(proxy_stream_failure_status(failure))
        }
        Some(proxy_request_chunk::Part::Header(_)) => Err(Status::invalid_argument(
            "proxy request may contain only one header chunk",
        )),
        None => Err(Status::invalid_argument("empty proxy request chunk")),
    }
}

fn proxy_stream_failure_status(failure: ProxyStreamFailure) -> Status {
    Status::new(grpc_code_from_i32(failure.grpc_code), failure.message)
}

fn grpc_code_from_i32(code: i32) -> tonic::Code {
    match code {
        0 => tonic::Code::Ok,
        1 => tonic::Code::Cancelled,
        2 => tonic::Code::Unknown,
        3 => tonic::Code::InvalidArgument,
        4 => tonic::Code::DeadlineExceeded,
        5 => tonic::Code::NotFound,
        6 => tonic::Code::AlreadyExists,
        7 => tonic::Code::PermissionDenied,
        8 => tonic::Code::ResourceExhausted,
        9 => tonic::Code::FailedPrecondition,
        10 => tonic::Code::Aborted,
        11 => tonic::Code::OutOfRange,
        12 => tonic::Code::Unimplemented,
        13 => tonic::Code::Internal,
        14 => tonic::Code::Unavailable,
        15 => tonic::Code::DataLoss,
        16 => tonic::Code::Unauthenticated,
        _ => tonic::Code::Unknown,
    }
}

fn proxy_header_chunk(header: ProxyResponseHeader) -> ProxyResponseChunk {
    ProxyResponseChunk {
        part: Some(proxy_response_chunk::Part::Header(header)),
    }
}

fn object_response_headers(object: &crate::persistence::Object) -> Vec<ProxyHeader> {
    let mut headers = vec![
        proxy_header("etag", object.etag.as_bytes()),
        proxy_header(
            "x-anvil-version-id",
            object.version_id.to_string().as_bytes(),
        ),
        proxy_header("x-anvil-record-hash", object.record_hash.as_bytes()),
        proxy_header(
            "x-anvil-mutation-id",
            object.mutation_id.to_string().as_bytes(),
        ),
        proxy_header("content-length", object.size.to_string().as_bytes()),
        proxy_header(
            "x-anvil-created-at",
            object.created_at.to_rfc3339().as_bytes(),
        ),
        proxy_header(
            "x-anvil-authz-revision",
            object.authz_revision.to_string().as_bytes(),
        ),
        proxy_header(
            "x-anvil-index-policy-snapshot",
            object.index_policy_snapshot.as_bytes(),
        ),
    ];
    if let Some(storage_class) = object.storage_class.as_deref() {
        headers.push(proxy_header(
            "x-anvil-storage-class",
            storage_class.as_bytes(),
        ));
    }
    if let Some(content_type) = object.content_type.as_deref() {
        headers.push(proxy_header("content-type", content_type.as_bytes()));
    }
    if let Some(serde_json::Value::Object(values)) = object.user_meta.as_ref() {
        for (key, value) in values {
            if let Some(value) = value.as_str() {
                headers.push(proxy_header(&format!("x-amz-meta-{key}"), value.as_bytes()));
            }
        }
    }
    headers
}

fn proxy_header(name: &str, value: &[u8]) -> ProxyHeader {
    ProxyHeader {
        name: name.to_ascii_lowercase(),
        value: value.to_vec(),
    }
}

async fn validate_internal_proxy_claims(
    state: &AppState,
    claims: &auth::Claims,
    _header: &ProxyRequestHeader,
) -> Result<(), Status> {
    system_realm::check_admin_relation(
        &state.storage,
        &state.config.mesh_id,
        claims,
        system_realm::SystemAdminRelation::ManageNodes,
    )
    .await
    .map_err(|error| Status::internal(error.to_string()))?
    .then_some(())
    .ok_or_else(|| Status::permission_denied("system realm manage_nodes relation required"))
}

fn decode_proxy_authz_context(header: &ProxyRequestHeader) -> Result<auth::Claims, Status> {
    decode_proxy_authz_context_bytes(&header.authz_context)
}

pub(crate) fn decode_proxy_authz_context_bytes(bytes: &[u8]) -> Result<auth::Claims, Status> {
    if bytes.is_empty() {
        return Err(Status::invalid_argument("proxy authz_context is required"));
    }
    let proto = crate::core_store::decode_deterministic_proto::<ProxyAuthzContextProto>(
        bytes,
        "proxy authz context",
    )
    .map_err(|error| Status::invalid_argument(format!("invalid proxy authz_context: {error}")))?;
    proxy_authz_context_from_proto(proto)
}

pub fn encode_proxy_authz_context(claims: &auth::Claims) -> Result<Vec<u8>, Status> {
    let proto = ProxyAuthzContextProto {
        version: PROXY_AUTHZ_CONTEXT_VERSION,
        sub: claims.sub.clone(),
        exp: u64::try_from(claims.exp)
            .map_err(|_| Status::invalid_argument("proxy authz_context exp is invalid"))?,
        tenant_id: claims.tenant_id,
        jti: claims.jti.clone(),
    };
    Ok(crate::core_store::encode_deterministic_proto(&proto))
}

const PROXY_AUTHZ_CONTEXT_VERSION: u32 = 1;

#[derive(Clone, PartialEq, ::prost::Message)]
struct ProxyAuthzContextProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    sub: String,
    #[prost(uint64, tag = "3")]
    exp: u64,
    #[prost(int64, tag = "5")]
    tenant_id: i64,
    #[prost(string, optional, tag = "6")]
    jti: Option<String>,
}

fn proxy_authz_context_from_proto(proto: ProxyAuthzContextProto) -> Result<auth::Claims, Status> {
    if proto.version != PROXY_AUTHZ_CONTEXT_VERSION {
        return Err(Status::invalid_argument(
            "invalid proxy authz_context version",
        ));
    }
    Ok(auth::Claims {
        sub: proto.sub,
        exp: usize::try_from(proto.exp)
            .map_err(|_| Status::invalid_argument("invalid proxy authz_context exp"))?,
        tenant_id: proto.tenant_id,
        jti: proto.jti,
    })
}

fn validate_proxy_principal_matches_header(
    claims: &auth::Claims,
    header: &ProxyRequestHeader,
) -> Result<(), Status> {
    if claims.sub != header.principal_id {
        return Err(Status::permission_denied(
            "proxy principal does not match authz context",
        ));
    }
    if claims.tenant_id != parse_proxy_tenant_id(header)? {
        return Err(Status::permission_denied(
            "proxy tenant does not match authz context",
        ));
    }
    Ok(())
}

fn parse_proxy_tenant_id(header: &ProxyRequestHeader) -> Result<i64, Status> {
    header
        .tenant_id
        .parse::<i64>()
        .map_err(|_| Status::invalid_argument("proxy tenant_id must be an integer"))
}

fn parse_proxy_version_id(header: &ProxyRequestHeader) -> Result<Option<uuid::Uuid>, Status> {
    let Some(value) = proxy_header_string(&header.headers, "x-anvil-version-id") else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    uuid::Uuid::parse_str(&value)
        .map(Some)
        .map_err(|_| Status::invalid_argument("invalid x-anvil-version-id"))
}

fn parse_proxy_read_range(
    headers: &[ProxyHeader],
) -> Result<Option<crate::core_store::CoreByteRange>, Status> {
    let start = proxy_header_u64(headers, "x-anvil-range-start")?;
    let end_exclusive = proxy_header_u64(headers, "x-anvil-range-end-exclusive")?;
    match (start, end_exclusive) {
        (None, None) => Ok(None),
        (Some(start), Some(end_exclusive)) if start <= end_exclusive => {
            Ok(Some(crate::core_store::CoreByteRange {
                start,
                end_exclusive,
            }))
        }
        (Some(_), Some(_)) => Err(Status::invalid_argument(
            "proxy range start exceeds end_exclusive",
        )),
        _ => Err(Status::invalid_argument(
            "proxy range requires start and end_exclusive",
        )),
    }
}

fn parse_proxy_read_consistency(headers: &[ProxyHeader]) -> Result<ObjectReadConsistency, Status> {
    let root_generation = proxy_header_u64(headers, "x-anvil-consistency-root-generation")?;
    let authz_revision = proxy_header_i64(headers, "x-anvil-consistency-authz-revision")?;
    match (root_generation, authz_revision) {
        (None, None) => Ok(ObjectReadConsistency::Latest),
        (Some(generation), None) => Ok(ObjectReadConsistency::AtRootGeneration(generation)),
        (None, Some(revision)) => Ok(ObjectReadConsistency::AtAuthzRevision(revision)),
        (Some(_), Some(_)) => Err(Status::invalid_argument(
            "proxy read consistency modes are mutually exclusive",
        )),
    }
}

fn proxy_header_u64(headers: &[ProxyHeader], name: &str) -> Result<Option<u64>, Status> {
    proxy_header_string(headers, name)
        .map(|value| {
            value
                .parse()
                .map_err(|_| Status::invalid_argument(format!("invalid proxy header {name}")))
        })
        .transpose()
}

fn proxy_header_i64(headers: &[ProxyHeader], name: &str) -> Result<Option<i64>, Status> {
    proxy_header_string(headers, name)
        .map(|value| {
            value
                .parse()
                .map_err(|_| Status::invalid_argument(format!("invalid proxy header {name}")))
        })
        .transpose()
}

fn proxy_header_string(headers: &[ProxyHeader], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .and_then(|header| std::str::from_utf8(&header.value).ok())
        .map(ToOwned::to_owned)
}

fn proxy_user_metadata(headers: &[ProxyHeader]) -> Option<serde_json::Value> {
    let mut values = serde_json::Map::new();
    for header in headers {
        let Some(metadata_key) = header.name.strip_prefix("x-amz-meta-") else {
            continue;
        };
        let Ok(metadata_value) = std::str::from_utf8(&header.value) else {
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

fn proxy_request_has_write_etag_preconditions(headers: &[ProxyHeader]) -> bool {
    proxy_header_string(headers, "if-match").is_some()
        || proxy_header_string(headers, "if-none-match").is_some()
}

fn evaluate_proxy_write_etag_preconditions(
    headers: &[ProxyHeader],
    current_etag: Option<&str>,
) -> Result<(), Status> {
    if let Some(value) = proxy_header_string(headers, "if-match")
        && !current_etag.is_some_and(|etag| proxy_etag_condition_matches(&value, etag))
    {
        return Err(proxy_precondition_failed_status());
    }
    if let Some(value) = proxy_header_string(headers, "if-none-match")
        && current_etag.is_some_and(|etag| proxy_etag_condition_matches(&value, etag))
    {
        return Err(proxy_precondition_failed_status());
    }
    Ok(())
}

fn proxy_etag_condition_matches(header_value: &str, current_etag: &str) -> bool {
    header_value
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || normalize_proxy_etag(candidate) == current_etag)
}

fn normalize_proxy_etag(value: &str) -> &str {
    value
        .strip_prefix("W/")
        .unwrap_or(value)
        .trim()
        .trim_matches('"')
}

fn proxy_precondition_failed_status() -> Status {
    Status::failed_precondition("At least one precondition did not hold")
}

#[allow(dead_code)]
fn metadata_value_to_proxy_header(name: &str, value: &HeaderValue) -> Option<ProxyHeader> {
    Some(proxy_header(name, value.as_bytes()))
}

enum ProxyReadEither {
    Head(crate::persistence::Object),
    Get(crate::object_manager::ObjectReadResult),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    fn claims(sub: &str, tenant_id: i64) -> auth::Claims {
        auth::Claims {
            sub: sub.to_string(),
            exp: usize::MAX,
            tenant_id,
            jti: None,
        }
    }

    fn header() -> ProxyRequestHeader {
        ProxyRequestHeader {
            request_id: "req-1".to_string(),
            idempotency_key: "idem-1".to_string(),
            principal_id: "app-a".to_string(),
            tenant_id: "42".to_string(),
            bucket_name: "bucket-a".to_string(),
            object_key: "path/file.txt".to_string(),
            method: "GET".to_string(),
            canonical_host: "bucket-a.tenant.eu-west-1.anvil-storage.test".to_string(),
            canonical_path: "/path/file.txt".to_string(),
            bucket_locator_generation: 7,
            headers: Vec::new(),
            authz_context: encode_proxy_authz_context(&claims("app-a", 42)).unwrap(),
            native_object_metadata: None,
        }
    }

    #[test]
    fn proxy_authz_context_encodes_principal_identity_only() {
        let decoded = decode_proxy_authz_context(&header()).unwrap();
        assert_eq!(decoded.sub, "app-a");
        assert_eq!(decoded.tenant_id, 42);
    }

    #[test]
    fn proxy_authz_context_must_match_header_principal_and_tenant() {
        let header = header();
        let decoded = decode_proxy_authz_context(&header).unwrap();
        validate_proxy_principal_matches_header(&decoded, &header).unwrap();

        let mut bad_principal = header.clone();
        bad_principal.principal_id = "other".to_string();
        assert_eq!(
            validate_proxy_principal_matches_header(&decoded, &bad_principal)
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );

        let mut bad_tenant = header;
        bad_tenant.tenant_id = "43".to_string();
        assert_eq!(
            validate_proxy_principal_matches_header(&decoded, &bad_tenant)
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }

    #[test]
    fn proxy_read_options_preserve_range_and_consistency() {
        let headers = vec![
            proxy_header("x-anvil-range-start", b"10"),
            proxy_header("x-anvil-range-end-exclusive", b"20"),
            proxy_header("x-anvil-consistency-root-generation", b"7"),
        ];
        assert_eq!(
            parse_proxy_read_range(&headers).unwrap(),
            Some(crate::core_store::CoreByteRange {
                start: 10,
                end_exclusive: 20,
            })
        );
        assert_eq!(
            parse_proxy_read_consistency(&headers).unwrap(),
            ObjectReadConsistency::AtRootGeneration(7)
        );
    }

    #[test]
    fn proxy_read_rejects_ambiguous_consistency() {
        let headers = vec![
            proxy_header("x-anvil-consistency-root-generation", b"7"),
            proxy_header("x-anvil-consistency-authz-revision", b"9"),
        ];
        assert_eq!(
            parse_proxy_read_consistency(&headers).unwrap_err().code(),
            Code::InvalidArgument
        );
    }
}
