use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{self, HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use base64::Engine as _;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use keldra_api::v1::ExchangeClientCredentialsRequest;
use keldra_store::{ObjectKey, StorageTenantId};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt as _;

use crate::authentication::{JwtManager, RequestRateLimits};
use crate::authorization::ObjectPermission;
use crate::distributed_control_plane::DistributedControlPlane;
use crate::serving_fence::ServingAuthority;
use crate::v05::{GatewayIdentity, GatewayObjectAdapter};

const OCI_PLUGIN_ID: &str = "oci@1";
const OCI_BINDING_PATH: &str = "_keldra/plugins/oci@1";
const MAX_BINDING_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug)]
pub struct PluginGatewayConfig {
    pub public_base_domain: Option<String>,
    pub public_scheme: String,
    pub endpoints: HashMap<String, Uri>,
}

impl Default for PluginGatewayConfig {
    fn default() -> Self {
        Self {
            public_base_domain: None,
            public_scheme: "https".into(),
            endpoints: HashMap::new(),
        }
    }
}

impl PluginGatewayConfig {
    pub fn new(
        public_base_domain: Option<String>,
        public_scheme: Option<String>,
        endpoints: impl IntoIterator<Item = String>,
    ) -> anyhow::Result<Self> {
        let public_base_domain = public_base_domain
            .map(|value| value.trim().trim_end_matches('.').to_ascii_lowercase())
            .filter(|value| !value.is_empty());
        if let Some(domain) = public_base_domain.as_deref() {
            anyhow::ensure!(
                valid_dns_name(domain),
                "public base domain is not a valid DNS name"
            );
        }
        let public_scheme = public_scheme.unwrap_or_else(|| "https".into());
        anyhow::ensure!(
            matches!(public_scheme.as_str(), "http" | "https"),
            "public scheme must be http or https"
        );
        let mut parsed = HashMap::new();
        for entry in endpoints {
            let (id, endpoint) = entry.split_once('=').ok_or_else(|| {
                anyhow::anyhow!("HTTP plugin must use name@version=http://host:port")
            })?;
            anyhow::ensure!(valid_plugin_id(id), "HTTP plugin ID is invalid");
            let endpoint: Uri = endpoint.parse().context("parse HTTP plugin endpoint")?;
            anyhow::ensure!(
                endpoint.scheme_str() == Some("http")
                    && endpoint.authority().is_some()
                    && endpoint.query().is_none()
                    && matches!(endpoint.path(), "" | "/"),
                "HTTP plugin endpoint must be an HTTP origin without a path or query"
            );
            anyhow::ensure!(
                parsed.insert(id.to_owned(), endpoint).is_none(),
                "duplicate HTTP plugin ID"
            );
        }
        anyhow::ensure!(
            parsed.is_empty() == public_base_domain.is_none(),
            "public base domain and HTTP plugins must be configured together"
        );
        Ok(Self {
            public_base_domain,
            public_scheme,
            endpoints: parsed,
        })
    }

    fn enabled(&self) -> bool {
        self.public_base_domain.is_some() && self.endpoints.contains_key(OCI_PLUGIN_ID)
    }
}

use anyhow::Context as _;

#[derive(Clone)]
pub(crate) struct PluginGatewayState {
    pub(crate) objects: GatewayObjectAdapter,
    pub(crate) control: Arc<DistributedControlPlane>,
    pub(crate) tokens: JwtManager,
    pub(crate) rate_limits: RequestRateLimits,
    pub(crate) serving: ServingAuthority,
    pub(crate) config: PluginGatewayConfig,
    pub(crate) basic_tokens: Arc<Mutex<HashMap<[u8; 32], String>>>,
    client: Client<HttpConnector, Body>,
}

impl PluginGatewayState {
    pub(crate) fn new(
        objects: GatewayObjectAdapter,
        control: Arc<DistributedControlPlane>,
        tokens: JwtManager,
        rate_limits: RequestRateLimits,
        serving: ServingAuthority,
        config: PluginGatewayConfig,
    ) -> Self {
        Self {
            objects,
            control,
            tokens,
            rate_limits,
            serving,
            config,
            basic_tokens: Arc::new(Mutex::new(HashMap::new())),
            client: Client::builder(TokioExecutor::new()).build_http(),
        }
    }
}

pub(crate) fn router(state: PluginGatewayState) -> Router {
    Router::new()
        .route("/v2/token", get(token))
        .route("/v2", any(proxy))
        .route("/v2/", any(proxy))
        .route("/v2/{*path}", any(proxy))
        .with_state(state)
}

#[derive(Clone, Debug)]
struct TenantBucket {
    tenant: String,
    bucket: String,
    public_host: String,
    public_scheme: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginBinding {
    format: u8,
    plugin: String,
    protocol_version: u16,
    root: String,
    service_app_id: String,
}

#[derive(Serialize)]
struct TokenResponse<'a> {
    token: &'a str,
    access_token: &'a str,
    expires_in: u64,
}

async fn token(State(state): State<PluginGatewayState>, request: Request) -> Response {
    let target = match target_from_headers(&state.config, request.headers()) {
        Ok(target) => target,
        Err(error) => return error.into_response(),
    };
    let identity = match authenticate(&state, request.headers(), false).await {
        Ok(identity @ GatewayIdentity::Authenticated { .. }) => identity,
        Ok(GatewayIdentity::Anonymous) => return challenge(&target, None),
        Err(error) => return error.into_response(),
    };
    if let Err(error) = require_identity_tenant(&identity, &target) {
        return error.into_response();
    }
    let GatewayIdentity::Authenticated { bearer, .. } = identity else {
        unreachable!()
    };
    let response = TokenResponse {
        token: &bearer,
        access_token: &bearer,
        expires_in: 60 * 60,
    };
    (StatusCode::OK, axum::Json(response)).into_response()
}

async fn proxy(State(state): State<PluginGatewayState>, mut request: Request) -> Response {
    if !state.config.enabled() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let target = match target_from_headers(&state.config, request.headers()) {
        Ok(target) => target,
        Err(error) => return error.into_response(),
    };
    let repository = match repository_from_path(request.uri().path()) {
        Ok(Some(repository)) => repository,
        Ok(None) if matches!(request.uri().path(), "/v2" | "/v2/") => {
            let identity = match authenticate(&state, request.headers(), true).await {
                Ok(identity) => identity,
                Err(error) => return error.into_response(),
            };
            if let Err(error) = require_identity_tenant(&identity, &target) {
                return error.into_response();
            }
            if identity.caller().is_none() {
                return challenge(&target, None);
            }
            return StatusCode::OK.into_response();
        }
        Ok(None) => return PluginError::not_found("OCI repository is missing").into_response(),
        Err(error) => return error.into_response(),
    };
    let identity = match authenticate(&state, request.headers(), true).await {
        Ok(identity) => identity,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = require_identity_tenant(&identity, &target) {
        return error.into_response();
    }
    let permission = permission_for(request.method(), request.uri().path());
    if identity.caller().is_none() && permission != ObjectPermission::Get {
        return challenge(&target, Some(&repository));
    }
    let repository_key = match ObjectKey::new(
        target.tenant.clone(),
        target.bucket.clone(),
        format!("container-registry/repositories/{repository}"),
    ) {
        Ok(key) => key,
        Err(error) => return PluginError::bad_request(&error.to_string()).into_response(),
    };
    if let Err(status) = state
        .objects
        .require(&identity, &repository_key, permission)
        .await
    {
        if identity.caller().is_none() {
            return challenge(&target, Some(&repository));
        }
        return PluginError::from_status(status).into_response();
    }
    let binding = match binding(&state, &identity, &target).await {
        Ok(binding) => binding,
        Err(error) => return error.into_response(),
    };
    let tenant = match StorageTenantId::parse(&target.tenant) {
        Ok(tenant) => tenant,
        Err(error) => return PluginError::bad_request(&error.to_string()).into_response(),
    };
    if let Err(status) = state
        .control
        .require_application(&tenant, &binding.service_app_id)
        .await
    {
        return PluginError::from_status(status).into_response();
    }
    let plugin_token = match state.tokens.mint_plugin_token(
        tenant,
        binding.service_app_id.clone(),
        target.bucket.clone(),
        binding.root.clone(),
    ) {
        Ok(token) => token,
        Err(_) => {
            return PluginError::internal("could not mint plugin object token").into_response();
        }
    };
    if let Err(error) = prepare_forward_request(
        &state,
        &target,
        &repository,
        &binding,
        &plugin_token,
        &mut request,
    ) {
        return error.into_response();
    }
    match state.client.request(request).await {
        Ok(response) => {
            let (parts, body) = response.into_parts();
            Response::from_parts(parts, Body::new(body))
        }
        Err(error) => {
            PluginError::unavailable(&format!("OCI plugin request failed: {error}")).into_response()
        }
    }
}

async fn binding(
    state: &PluginGatewayState,
    identity: &GatewayIdentity,
    target: &TenantBucket,
) -> Result<PluginBinding, PluginError> {
    let key = ObjectKey::new(&target.tenant, &target.bucket, OCI_BINDING_PATH)
        .map_err(|error| PluginError::bad_request(&error.to_string()))?;
    let mut stream = state
        .objects
        .plugin_binding_get(identity, &key)
        .await
        .map_err(PluginError::from_status)?;
    let mut bytes = Vec::new();
    let mut saw_head = false;
    while let Some(chunk) = stream.next().await {
        match chunk.map_err(PluginError::from_status)?.value {
            Some(keldra_api::v1::object_chunk::Value::Head(_)) if !saw_head => saw_head = true,
            Some(keldra_api::v1::object_chunk::Value::Bytes(next)) if saw_head => {
                if bytes.len().saturating_add(next.len()) > MAX_BINDING_BYTES {
                    return Err(PluginError::bad_request("plugin binding exceeds 16 KiB"));
                }
                bytes.extend_from_slice(&next);
            }
            _ => {
                return Err(PluginError::internal(
                    "plugin binding object stream is malformed",
                ));
            }
        }
    }
    let binding: PluginBinding = serde_json::from_slice(&bytes)
        .map_err(|_| PluginError::bad_request("plugin binding is not canonical JSON"))?;
    if binding.format != 1
        || binding.plugin != "oci"
        || binding.protocol_version != 1
        || binding.root != "container-registry"
        || binding.service_app_id.is_empty()
    {
        return Err(PluginError::bad_request("plugin binding is invalid"));
    }
    Ok(binding)
}

fn prepare_forward_request(
    state: &PluginGatewayState,
    target: &TenantBucket,
    repository: &str,
    binding: &PluginBinding,
    plugin_token: &str,
    request: &mut Request,
) -> Result<(), PluginError> {
    let endpoint = state
        .config
        .endpoints
        .get(OCI_PLUGIN_ID)
        .ok_or_else(|| PluginError::unavailable("OCI plugin is not installed"))?;
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/v2/");
    *request.uri_mut() = format!(
        "{}://{}{}",
        endpoint.scheme_str().unwrap_or("http"),
        endpoint.authority().expect("validated endpoint"),
        path_and_query
    )
    .parse()
    .map_err(|_| PluginError::internal("could not construct plugin URI"))?;
    request.headers_mut().remove(http::header::HOST);
    request.headers_mut().remove(http::header::AUTHORIZATION);
    for name in [
        http::header::CONNECTION,
        http::header::PROXY_AUTHENTICATE,
        http::header::PROXY_AUTHORIZATION,
        http::header::TE,
        http::header::TRAILER,
        http::header::TRANSFER_ENCODING,
        http::header::UPGRADE,
    ] {
        request.headers_mut().remove(name);
    }
    request.headers_mut().insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {plugin_token}"))
            .map_err(|_| PluginError::internal("plugin bearer is malformed"))?,
    );
    insert_header(
        request.headers_mut(),
        "x-keldra-storage-tenant",
        &target.tenant,
    )?;
    insert_header(request.headers_mut(), "x-keldra-bucket", &target.bucket)?;
    insert_header(request.headers_mut(), "x-keldra-plugin-root", &binding.root)?;
    insert_header(request.headers_mut(), "x-keldra-repository", repository)?;
    insert_header(
        request.headers_mut(),
        "x-forwarded-host",
        &target.public_host,
    )?;
    Ok(())
}

fn insert_header(
    headers: &mut HeaderMap,
    name: &'static str,
    value: &str,
) -> Result<(), PluginError> {
    headers.insert(
        name,
        HeaderValue::from_str(value)
            .map_err(|_| PluginError::bad_request("routing metadata is malformed"))?,
    );
    Ok(())
}

async fn authenticate(
    state: &PluginGatewayState,
    headers: &HeaderMap,
    anonymous: bool,
) -> Result<GatewayIdentity, PluginError> {
    state
        .serving
        .require(tonic::Request::new(()))
        .map_err(PluginError::from_status)?;
    state
        .rate_limits
        .check_gateway_global()
        .map_err(PluginError::from_status)?;
    let mut values = headers.get_all(http::header::AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        return if anonymous {
            Ok(GatewayIdentity::Anonymous)
        } else {
            Err(PluginError::unauthorized("credentials are required"))
        };
    };
    if values.next().is_some() {
        return Err(PluginError::unauthorized(
            "multiple Authorization headers are not accepted",
        ));
    }
    let value = value
        .to_str()
        .map_err(|_| PluginError::unauthorized("Authorization is malformed"))?;
    let (caller, bearer) = if let Some(token) = value
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty())
    {
        (
            state
                .tokens
                .verify(token)
                .map_err(|_| PluginError::unauthorized("Bearer token is invalid or expired"))?,
            token.to_owned(),
        )
    } else if let Some(encoded) = value.strip_prefix("Basic ") {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| PluginError::unauthorized("Basic credentials are malformed"))?;
        let decoded = String::from_utf8(decoded)
            .map_err(|_| PluginError::unauthorized("Basic credentials are malformed"))?;
        let (client_id, client_secret) = decoded
            .split_once(':')
            .filter(|(id, secret)| !id.is_empty() && !secret.is_empty())
            .ok_or_else(|| PluginError::unauthorized("Basic credentials are malformed"))?;
        let cache_key = blake3::derive_key(
            "keldra.plugin/basic-credential-cache/v1",
            decoded.as_bytes(),
        );
        let cached = state
            .basic_tokens
            .lock()
            .map_err(|_| PluginError::internal("credential cache is unavailable"))?
            .get(&cache_key)
            .cloned();
        let bearer = if let Some(token) = cached.filter(|token| state.tokens.verify(token).is_ok())
        {
            token
        } else {
            state
                .rate_limits
                .check_credential_exchange(client_id)
                .map_err(PluginError::from_status)?;
            let access = state
                .control
                .exchange_client_credentials(ExchangeClientCredentialsRequest {
                    client_id: client_id.to_owned(),
                    client_secret: client_secret.to_owned(),
                })
                .await
                .map_err(|_| PluginError::unauthorized("client credentials are invalid"))?;
            let mut cache = state
                .basic_tokens
                .lock()
                .map_err(|_| PluginError::internal("credential cache is unavailable"))?;
            if cache.len() >= 1_024 {
                cache.clear();
            }
            cache.insert(cache_key, access.access_token.clone());
            access.access_token
        };
        let caller = state
            .tokens
            .verify(&bearer)
            .map_err(|_| PluginError::internal("credential exchange returned an invalid token"))?;
        (caller, bearer)
    } else {
        return Err(PluginError::unauthorized(
            "Authorization must use Basic or Bearer",
        ));
    };
    state
        .rate_limits
        .check_gateway_identity(&caller)
        .map_err(PluginError::from_status)?;
    Ok(GatewayIdentity::Authenticated { caller, bearer })
}

fn target_from_headers(
    config: &PluginGatewayConfig,
    headers: &HeaderMap,
) -> Result<TenantBucket, PluginError> {
    let base = config
        .public_base_domain
        .as_deref()
        .ok_or_else(|| PluginError::not_found("HTTP plugins are disabled"))?;
    let mut hosts = headers.get_all(http::header::HOST).iter();
    let host = hosts
        .next()
        .ok_or_else(|| PluginError::bad_request("Host is required"))?;
    if hosts.next().is_some() {
        return Err(PluginError::bad_request(
            "multiple Host headers are not accepted",
        ));
    }
    let host = host
        .to_str()
        .map_err(|_| PluginError::bad_request("Host is malformed"))?;
    let authority: http::uri::Authority = host
        .parse()
        .map_err(|_| PluginError::bad_request("Host is malformed"))?;
    let name = authority.host().trim_end_matches('.').to_ascii_lowercase();
    let suffix = format!(".{base}");
    let labels = name
        .strip_suffix(&suffix)
        .ok_or_else(|| PluginError::not_found("Host is outside the configured Keldra domain"))?;
    let (bucket, tenant) = labels
        .split_once('.')
        .filter(|(bucket, tenant)| {
            valid_dns_label(bucket) && valid_dns_label(tenant) && !tenant.contains('.')
        })
        .ok_or_else(|| PluginError::not_found("Host does not name one tenant bucket"))?;
    Ok(TenantBucket {
        tenant: tenant.to_owned(),
        bucket: bucket.to_owned(),
        public_host: host.to_owned(),
        public_scheme: config.public_scheme.clone(),
    })
}

fn require_identity_tenant(
    identity: &GatewayIdentity,
    target: &TenantBucket,
) -> Result<(), PluginError> {
    if identity
        .caller()
        .is_none_or(|caller| caller.storage_tenant().as_str() == target.tenant)
    {
        Ok(())
    } else {
        Err(PluginError::forbidden(
            "credential does not belong to this tenant",
        ))
    }
}

fn repository_from_path(path: &str) -> Result<Option<String>, PluginError> {
    let Some(rest) = path.strip_prefix("/v2/") else {
        return Ok(None);
    };
    let segments = rest.split('/').collect::<Vec<_>>();
    let marker = segments
        .iter()
        .position(|segment| matches!(*segment, "blobs" | "manifests" | "tags" | "referrers"));
    let Some(marker) = marker else {
        return Ok(None);
    };
    if marker == 0
        || segments[..marker]
            .iter()
            .any(|segment| !canonical_segment(segment))
    {
        return Err(PluginError::bad_request("OCI repository path is malformed"));
    }
    Ok(Some(segments[..marker].join("/")))
}

fn permission_for(method: &http::Method, path: &str) -> ObjectPermission {
    if method == http::Method::GET || method == http::Method::HEAD {
        ObjectPermission::Get
    } else if method == http::Method::DELETE {
        ObjectPermission::Delete
    } else if path.contains("/blobs/uploads/")
        || path.ends_with("/blobs/uploads/")
        || path.contains("/manifests/")
    {
        ObjectPermission::Put
    } else {
        ObjectPermission::Put
    }
}

fn challenge(target: &TenantBucket, repository: Option<&str>) -> Response {
    let scope = repository.map_or_else(
        || "registry:catalog:*".to_owned(),
        |repository| format!("repository:{repository}:pull,push"),
    );
    let value = format!(
        "Bearer realm=\"{}://{}/v2/token\",service=\"{}\",scope=\"{}\"",
        target.public_scheme, target.public_host, target.public_host, scope
    );
    let mut response = PluginError::unauthorized("authentication is required").into_response();
    if let Ok(value) = HeaderValue::from_str(&value) {
        response
            .headers_mut()
            .insert(http::header::WWW_AUTHENTICATE, value);
    }
    response
}

fn valid_plugin_id(value: &str) -> bool {
    value
        .split_once('@')
        .is_some_and(|(name, version)| canonical_segment(name) && canonical_segment(version))
}

fn canonical_segment(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value != "_keldra"
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_dns_name(value: &str) -> bool {
    value.len() <= 253 && value.split('.').all(valid_dns_label)
}
fn valid_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

#[derive(Debug, Serialize)]
struct OciErrorBody {
    errors: Vec<OciErrorDetail>,
}
#[derive(Debug, Serialize)]
struct OciErrorDetail {
    code: &'static str,
    message: String,
}

#[derive(Debug)]
struct PluginError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl PluginError {
    fn bad_request(message: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "DENIED",
            message: message.into(),
        }
    }
    fn unauthorized(message: &str) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "UNAUTHORIZED",
            message: message.into(),
        }
    }
    fn forbidden(message: &str) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "DENIED",
            message: message.into(),
        }
    }
    fn not_found(message: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NAME_UNKNOWN",
            message: message.into(),
        }
    }
    fn unavailable(message: &str) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "UNAVAILABLE",
            message: message.into(),
        }
    }
    fn internal(message: &str) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "UNKNOWN",
            message: message.into(),
        }
    }
    fn from_status(status: tonic::Status) -> Self {
        let http = match status.code() {
            tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
            tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
            tonic::Code::NotFound => StatusCode::NOT_FOUND,
            tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
            tonic::Code::AlreadyExists | tonic::Code::Aborted => StatusCode::CONFLICT,
            tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status: http,
            code: if http == StatusCode::UNAUTHORIZED {
                "UNAUTHORIZED"
            } else {
                "DENIED"
            },
            message: status.message().into(),
        }
    }
}

impl IntoResponse for PluginError {
    fn into_response(self) -> Response {
        (
            self.status,
            axum::Json(OciErrorBody {
                errors: vec![OciErrorDetail {
                    code: self.code,
                    message: self.message,
                }],
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_selects_exact_bucket_and_tenant() {
        let config = PluginGatewayConfig::new(
            Some("keldra.test".into()),
            Some("https".into()),
            ["oci@1=http://127.0.0.1:9000".into()],
        )
        .unwrap();
        let headers = HeaderMap::from_iter([(
            http::header::HOST,
            HeaderValue::from_static("images.acme.keldra.test:443"),
        )]);
        let target = target_from_headers(&config, &headers).unwrap();
        assert_eq!(
            (target.bucket.as_str(), target.tenant.as_str()),
            ("images", "acme")
        );
    }

    #[test]
    fn repository_parser_rejects_traversal() {
        assert_eq!(
            repository_from_path("/v2/team/app/manifests/latest")
                .unwrap()
                .as_deref(),
            Some("team/app")
        );
        assert!(repository_from_path("/v2/team/../blobs/sha256:x").is_err());
    }

    #[test]
    fn authenticated_identity_cannot_cross_tenants() {
        let identity = GatewayIdentity::Authenticated {
            caller: crate::authentication::Caller::from_authenticated_application(
                StorageTenantId::parse("other").unwrap(),
                "app",
            )
            .unwrap(),
            bearer: "unused".into(),
        };
        let target = TenantBucket {
            tenant: "acme".into(),
            bucket: "images".into(),
            public_host: "images.acme.keldra.test".into(),
            public_scheme: "https".into(),
        };
        assert!(require_identity_tenant(&identity, &target).is_err());
    }
}
