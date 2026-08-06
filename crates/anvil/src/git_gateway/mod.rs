use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anvil_store::ObjectKey;
use axum::Router;
use axum::extract::{Path, Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;

use crate::authentication::{JwtManager, RequestRateLimits};
use crate::distributed_control_plane::DistributedControlPlane;
use crate::serving_fence::ServingAuthority;
use crate::v05::GatewayObjectAdapter;

mod auth;
mod backend;
mod repository;

#[derive(Clone)]
pub(crate) struct GitGatewayState {
    pub(crate) objects: GatewayObjectAdapter,
    pub(crate) control: Arc<DistributedControlPlane>,
    pub(crate) tokens: JwtManager,
    pub(crate) rate_limits: RequestRateLimits,
    pub(crate) serving: ServingAuthority,
    pub(crate) mutation_admission: crate::mutation_admission::MutationAdmission,
    pub(crate) cache_root: PathBuf,
    pub(crate) max_request_bytes: u64,
    pub(crate) lock: Arc<tokio::sync::Mutex<()>>,
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
    let key = match target.bundle_key() {
        Ok(key) => key,
        Err(error) => return error.into_response(),
    };
    if target.operation.is_push()
        && let Err(error) = state.objects.git_require_write(&identity, &key).await
    {
        return GitError::from_status(error).into_response();
    }
    let _mutation_permit = if target.operation.is_push() {
        match state.mutation_admission.enter() {
            Ok(permit) => Some(permit),
            Err(error) => return GitError::from_status(error).into_response(),
        }
    } else {
        None
    };

    // The 0.5.3 cache is disposable and one process-wide lock keeps a CGI
    // request plus its bundle CAS coherent without inventing a lock service.
    let _guard = state.lock.lock().await;
    let materialized = match repository::materialize(&state, &identity, &target, &key).await {
        Ok(materialized) => materialized,
        Err(error) if identity.caller().is_none() && error.status == StatusCode::FORBIDDEN => {
            return GitError::unauthorized("credentials are required for this repository")
                .into_response();
        }
        Err(error) => return error.into_response(),
    };
    let before = if target.operation.is_push() {
        match repository::refs(&materialized.repository).await {
            Ok(refs) => Some(refs),
            Err(error) => return error.into_response(),
        }
    } else {
        None
    };
    let method = request.method().as_str().to_owned();
    let content_type = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = match backend::bounded_body(request.body_mut(), state.max_request_bytes).await {
        Ok(body) => body,
        Err(error) => return error.into_response(),
    };
    let response = match backend::execute(
        &target,
        &identity,
        &materialized.repository,
        &method,
        content_type.as_deref(),
        body,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => return error.into_response(),
    };
    if target.operation.is_push() && response.status().is_success() {
        let after = match repository::refs(&materialized.repository).await {
            Ok(refs) => refs,
            Err(error) => return error.into_response(),
        };
        if before.as_deref() != Some(after.as_str())
            && let Err(error) =
                repository::publish(&state, &identity, &target, &key, &materialized).await
        {
            return error.into_response();
        }
    }
    response
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

    fn bundle_key(&self) -> Result<ObjectKey, GitError> {
        let repository_id = blake3::hash(self.repository.as_bytes()).to_hex();
        ObjectKey::new(
            self.tenant.clone(),
            self.bucket.clone(),
            format!("_anvil/git/{repository_id}/repository.bundle"),
        )
        .map_err(|error| GitError::bad_request(error.to_string()))
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
                axum::http::HeaderValue::from_static("Basic realm=\"Anvil Git\""),
            );
        }
        response
    }
}
