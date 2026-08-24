use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use axum::Router;
use axum::extract::{Path, Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use keldra_store::ObjectKey;
use tokio_stream::StreamExt as _;

use crate::authentication::{JwtManager, RequestRateLimits};
use crate::distributed_control_plane::DistributedControlPlane;
use crate::serving_fence::ServingAuthority;
use crate::v05::GatewayObjectAdapter;

mod auth;
mod backend;
pub(crate) mod cache;
mod model;
mod repository;
mod storage;

#[derive(Clone)]
pub(crate) struct GitGatewayState {
    pub(crate) objects: GatewayObjectAdapter,
    pub(crate) control: Arc<DistributedControlPlane>,
    pub(crate) tokens: JwtManager,
    pub(crate) rate_limits: RequestRateLimits,
    pub(crate) serving: ServingAuthority,
    pub(crate) mutation_admission: crate::mutation_admission::MutationAdmission,
    pub(crate) cache_root: PathBuf,
    pub(crate) repository_locks: Arc<cache::RepositoryLocks>,
    pub(crate) basic_tokens: Arc<std::sync::Mutex<HashMap<[u8; 32], String>>>,
}

pub(crate) fn router(state: GitGatewayState) -> Router {
    Router::new()
        .route("/git/{*path}", any(handle))
        .with_state(state)
}

async fn handle(
    State(state): State<GitGatewayState>,
    Path(path): Path<String>,
    mut request: Request,
) -> Response {
    let target = match Target::parse(&path, request.method(), request.uri().query()) {
        Ok(target) => target,
        Err(error) => return error.into_response(),
    };
    let identity = match auth::authenticate(&state, request.headers()).await {
        Ok(identity) => identity,
        Err(error) => return error.into_response(),
    };
    if let Some(caller) = identity.caller()
        && caller.storage_tenant().as_str() != target.tenant
    {
        return GitError::forbidden("credential does not belong to this tenant").into_response();
    }
    if identity.caller().is_none() && !target.operation.is_pull() {
        return GitError::unauthorized("Git push requires credentials").into_response();
    }
    let key = match target.authorization_key() {
        Ok(key) => key,
        Err(error) => return error.into_response(),
    };
    if target.operation.is_push()
        && let Err(error) = state.objects.git_require_write(&identity, &key).await
    {
        return GitError::from_status(error).into_response();
    }
    if target.operation.is_pull()
        && let Err(error) = state.objects.git_require_read(&identity, &key).await
    {
        if identity.caller().is_none() && error.code() == tonic::Code::PermissionDenied {
            return GitError::unauthorized("credentials are required for this repository")
                .into_response();
        }
        return GitError::from_status(error).into_response();
    }
    let _mutation_permit = if target.operation.mutates_repository() {
        match state.mutation_admission.enter() {
            Ok(permit) => Some(permit),
            Err(error) => return GitError::from_status(error).into_response(),
        }
    } else {
        None
    };

    let location = match storage::RepositoryLocation::resolve(
        &state,
        &identity,
        &target,
        target.operation.is_push(),
    )
    .await
    {
        Ok(Some(location)) => location,
        Ok(None) => return GitError::not_found().into_response(),
        Err(error) => return error.into_response(),
    };
    let storage = storage::GitStorage::new(&state, &identity, location);
    let materialized = match repository::materialize(
        &state,
        &storage,
        target.operation.mutates_repository(),
    )
    .await
    {
        Ok(materialized) => materialized,
        Err(error) if identity.caller().is_none() && error.status == StatusCode::FORBIDDEN => {
            return GitError::unauthorized("credentials are required for this repository")
                .into_response();
        }
        Err(error) => return error.into_response(),
    };
    let method = request.method().as_str().to_owned();
    let content_type = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_length = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let body = std::mem::replace(request.body_mut(), axum::body::Body::empty());
    if target.operation.mutates_repository() {
        repository::begin_push(&materialized).await;
    }
    let executed = match backend::execute(
        &target,
        &identity,
        &materialized.repository,
        &method,
        content_type.as_deref(),
        content_length,
        body,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => return error.into_response(),
    };
    let response = executed.response;
    if target.operation.mutates_repository() && response.status().is_success() {
        match repository::publish(&storage, &materialized).await {
            Ok(true) => repository::spawn_compaction(state.clone(), storage.clone()),
            Ok(false) => {}
            Err(error) => return error.into_response(),
        }
    }
    let keepalive = target.operation.streams_response().then_some(materialized);
    let ingress = state.objects.clone();
    let ingress_key = key.clone();
    let egress = state.objects.clone();
    let egress_key = key;
    meter_successful_response(
        response,
        executed.inbound_bytes,
        executed.request_complete,
        keepalive,
        target.operation.metric_name(),
        move |bytes| ingress.record_gateway_ingress(&ingress_key, bytes),
        move |bytes| egress.record_gateway_egress(&egress_key, bytes),
    )
}

fn meter_successful_response<Ingress, Egress>(
    response: Response,
    inbound_bytes: Arc<AtomicU64>,
    request_complete: Arc<AtomicBool>,
    keepalive: Option<repository::MaterializedRepository>,
    operation: &'static str,
    record_ingress: Ingress,
    record_egress: Egress,
) -> Response
where
    Ingress: FnOnce(u64) + Send + 'static,
    Egress: FnOnce(u64) + Send + 'static,
{
    if !response.status().is_success() {
        return response;
    }
    let (parts, body) = response.into_parts();
    let stream = async_stream::stream! {
        // The repository read guard pins native pack/index files until the Git
        // subprocess response is fully consumed. It is deliberately not used
        // as authority; `materialize` already bound it to an exact `current`.
        let _keepalive = keepalive;
        let mut body = body.into_data_stream();
        let mut completed_bytes = 0_u64;
        while let Some(next) = body.next().await {
            match next {
                Ok(bytes) => {
                    completed_bytes = completed_bytes.saturating_add(bytes.len() as u64);
                    yield Ok::<_, axum::Error>(bytes);
                }
                Err(error) => {
                    yield Err(error);
                    return;
                }
            }
        }
        if !request_complete.load(Ordering::Acquire) {
            return;
        }
        record_ingress(inbound_bytes.load(Ordering::Relaxed));
        record_egress(completed_bytes);
        tracing::info!(
            monotonic_counter.keldra_git_requests_completed_total = 1_u64,
            monotonic_counter.keldra_git_ingress_bytes_total = inbound_bytes.load(Ordering::Relaxed),
            monotonic_counter.keldra_git_egress_bytes_total = completed_bytes,
            operation,
            "Git smart HTTP request completed"
        );
    };
    Response::from_parts(parts, axum::body::Body::from_stream(stream))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    AdvertisePull,
    Pull,
    AdvertisePush,
    Push,
}

impl Operation {
    const fn is_pull(self) -> bool {
        matches!(self, Self::AdvertisePull | Self::Pull)
    }

    const fn is_push(self) -> bool {
        matches!(self, Self::AdvertisePush | Self::Push)
    }

    const fn streams_response(self) -> bool {
        !matches!(self, Self::Push)
    }

    const fn mutates_repository(self) -> bool {
        matches!(self, Self::Push)
    }

    const fn metric_name(self) -> &'static str {
        match self {
            Self::AdvertisePull => "advertise_pull",
            Self::Pull => "pull",
            Self::AdvertisePush => "advertise_push",
            Self::Push => "push",
        }
    }
}

#[derive(Clone, Debug)]
struct Target {
    tenant: String,
    bucket: String,
    repository: String,
    path_info: String,
    query: String,
    operation: Operation,
}

impl Target {
    fn parse(path: &str, method: &Method, query: Option<&str>) -> Result<Self, GitError> {
        let segments = path.split('/').collect::<Vec<_>>();
        if segments.len() < 4 || segments.iter().any(|segment| segment.is_empty()) {
            return Err(GitError::not_found());
        }
        let repository_end = segments
            .iter()
            .enumerate()
            .skip(2)
            .find_map(|(index, segment)| segment.ends_with(".git").then_some(index))
            .ok_or_else(GitError::not_found)?;
        let repository = segments[2..=repository_end].join("/");
        let repository = repository
            .strip_suffix(".git")
            .filter(|value| !value.is_empty())
            .ok_or_else(GitError::not_found)?
            .to_owned();
        if repository
            .split('/')
            .any(|segment| matches!(segment, "." | "..") || segment.contains('\\'))
        {
            return Err(GitError::not_found());
        }
        let service_path = segments[repository_end + 1..].join("/");
        let query = query.unwrap_or_default().to_owned();
        let operation = match (method, service_path.as_str(), query.as_str()) {
            (&Method::GET, "info/refs", "service=git-upload-pack") => Operation::AdvertisePull,
            (&Method::POST, "git-upload-pack", "") => Operation::Pull,
            (&Method::GET, "info/refs", "service=git-receive-pack") => Operation::AdvertisePush,
            (&Method::POST, "git-receive-pack", "") => Operation::Push,
            _ => return Err(GitError::not_found()),
        };
        Ok(Self {
            tenant: segments[0].to_owned(),
            bucket: segments[1].to_owned(),
            repository,
            path_info: format!("/repo.git/{service_path}"),
            query,
            operation,
        })
    }

    fn authorization_key(&self) -> Result<ObjectKey, GitError> {
        storage::name_key(self)
    }
}

#[derive(Debug)]
struct GitError {
    status: StatusCode,
    message: String,
}

impl GitError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, message)
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, message)
    }

    fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "Git repository route was not found")
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, message)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }

    fn from_status(status: tonic::Status) -> Self {
        let code = match status.code() {
            tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
            tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
            tonic::Code::NotFound => StatusCode::NOT_FOUND,
            tonic::Code::Aborted | tonic::Code::FailedPrecondition => StatusCode::CONFLICT,
            tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self::new(code, status.message())
    }
}

impl IntoResponse for GitError {
    fn into_response(self) -> Response {
        let mut response = (self.status, self.message).into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Basic realm=\"Keldra Git\""),
            );
        }
        response
    }
}

#[cfg(test)]
mod traffic_tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use bytes::Bytes;
    use http_body_util::BodyExt as _;

    use super::*;

    #[tokio::test]
    async fn successful_git_transfer_records_exact_public_body_bytes_once() {
        let inbound = Arc::new(Mutex::new(Vec::new()));
        let outbound = Arc::new(Mutex::new(Vec::new()));
        let response = Response::builder()
            .status(StatusCode::OK)
            .body(Body::from("git response"))
            .unwrap();
        let metered = meter_successful_response(
            response,
            Arc::new(AtomicU64::new(17)),
            Arc::new(AtomicBool::new(true)),
            None,
            "test",
            {
                let inbound = inbound.clone();
                move |bytes| inbound.lock().unwrap().push(bytes)
            },
            {
                let outbound = outbound.clone();
                move |bytes| outbound.lock().unwrap().push(bytes)
            },
        );

        assert_eq!(*inbound.lock().unwrap(), [17]);
        assert!(outbound.lock().unwrap().is_empty());
        let body = metered.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"git response");
        assert_eq!(*outbound.lock().unwrap(), [12]);
    }

    #[tokio::test]
    async fn failed_or_unfinished_git_transfers_do_not_record_egress() {
        let failed = Arc::new(Mutex::new(Vec::new()));
        let response = meter_successful_response(
            Response::builder()
                .status(StatusCode::CONFLICT)
                .body(Body::from("failed"))
                .unwrap(),
            Arc::new(AtomicU64::new(9)),
            Arc::new(AtomicBool::new(true)),
            None,
            "test",
            {
                let failed = failed.clone();
                move |bytes| failed.lock().unwrap().push(("in", bytes))
            },
            {
                let failed = failed.clone();
                move |bytes| failed.lock().unwrap().push(("out", bytes))
            },
        );
        let _ = response.into_body().collect().await.unwrap();
        assert!(failed.lock().unwrap().is_empty());

        let incomplete_egress = Arc::new(Mutex::new(Vec::new()));
        let response = meter_successful_response(
            Response::new(Body::from("not consumed")),
            Arc::new(AtomicU64::new(11)),
            Arc::new(AtomicBool::new(true)),
            None,
            "test",
            |_| {},
            {
                let incomplete_egress = incomplete_egress.clone();
                move |bytes| incomplete_egress.lock().unwrap().push(bytes)
            },
        );
        drop(response);
        assert!(incomplete_egress.lock().unwrap().is_empty());

        let errored_egress = Arc::new(Mutex::new(Vec::new()));
        let body = Body::from_stream(tokio_stream::iter([
            Ok::<_, io::Error>(Bytes::from_static(b"partial")),
            Err(io::Error::other("broken response")),
        ]));
        let response = meter_successful_response(
            Response::new(body),
            Arc::new(AtomicU64::new(7)),
            Arc::new(AtomicBool::new(true)),
            None,
            "test",
            |_| {},
            {
                let errored_egress = errored_egress.clone();
                move |bytes| errored_egress.lock().unwrap().push(bytes)
            },
        );
        assert!(response.into_body().collect().await.is_err());
        assert!(errored_egress.lock().unwrap().is_empty());
    }
}
