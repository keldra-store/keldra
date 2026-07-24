use crate::AppState;
use axum::Router;
use axum::extract::{Json, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};

const CONTROL_DIR_ENV: &str = "ANVIL_TEST_ROOT_PUBLICATION_CONTROL_DIR";
const CONTROL_TOKEN_ENV: &str = "ANVIL_TEST_ROOT_PUBLICATION_CONTROL_TOKEN";
const CONTROL_TOKEN_HEADER: &str = "x-anvil-test-control-token";

#[derive(Clone)]
struct TestControlState {
    app: AppState,
    token: String,
}

#[derive(Deserialize)]
struct TransactionControlRequest {
    transaction_id: String,
}

#[derive(Serialize)]
struct RootPublicationStatus {
    transaction_id: String,
    armed: bool,
    pause_reached: bool,
    intent_present: bool,
    recovery_ready: bool,
}

pub(super) fn extend_admin_router(router: Router, app: AppState) -> Router {
    let Some(token) = test_control_token() else {
        return router;
    };
    if std::env::var_os(CONTROL_DIR_ENV).is_none_or(|value| value.is_empty()) {
        return router;
    }
    let control = Router::new()
        .route(
            "/__anvil_test/root-publication/arm-after-q2",
            post(arm_after_q2),
        )
        .route(
            "/__anvil_test/root-publication/status",
            get(root_publication_status),
        )
        .with_state(TestControlState { app, token });
    router.merge(control)
}

async fn arm_after_q2(
    State(state): State<TestControlState>,
    headers: HeaderMap,
    Json(request): Json<TransactionControlRequest>,
) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match state
        .app
        .core_store
        .arm_external_root_publication_pause_after_q2(&request.transaction_id)
        .await
    {
        Ok(()) => publication_status_response(&state.app, request.transaction_id).await,
        Err(error) => (
            StatusCode::BAD_REQUEST,
            format!("arm root-publication test pause: {error:#}"),
        )
            .into_response(),
    }
}

async fn root_publication_status(
    State(state): State<TestControlState>,
    headers: HeaderMap,
    Query(request): Query<TransactionControlRequest>,
) -> Response {
    if !authorized(&headers, &state.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    publication_status_response(&state.app, request.transaction_id).await
}

async fn publication_status_response(app: &AppState, transaction_id: String) -> Response {
    match app
        .core_store
        .external_root_publication_test_status(&transaction_id)
        .await
    {
        Ok((armed, pause_reached, intent_present, recovery_ready)) => Json(RootPublicationStatus {
            transaction_id,
            armed,
            pause_reached,
            intent_present,
            recovery_ready,
        })
        .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("inspect root-publication test state: {error:#}"),
        )
            .into_response(),
    }
}

fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(CONTROL_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected)
}

fn test_control_token() -> Option<String> {
    std::env::var(CONTROL_TOKEN_ENV)
        .ok()
        .filter(|value| !value.is_empty())
}
