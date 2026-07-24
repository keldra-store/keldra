#![recursion_limit = "512"]

use anyhow::Result;
use axum::ServiceExt;
use axum::serve::ListenerExt;
use once_cell::sync::OnceCell;
use std::time::Instant;
use tonic::service;
use tower::ServiceExt as TowerServiceExt;
use tracing::{error, info};

// Re-export the core types for the binary and services to use.
pub use anvil_core::*;

// Modules that remain in the main anvil crate
pub mod s3_gateway;

pub mod s3_auth;
mod startup_readiness;

pub async fn run(
    listener: tokio::net::TcpListener,
    admin_listener: tokio::net::TcpListener,
    config: anvil_core::config::Config,
) -> Result<()> {
    config.validate_admin_listener_bind()?;
    let personaldb_protocol_keyring =
        anvil_core::personaldb_signing::PersonalDbProtocolKeyring::disabled();
    let state = AppState::new(config, personaldb_protocol_keyring).await?;

    // Then start the node
    start_node_with_admin_listener(listener, Some(admin_listener), state).await
}

pub async fn start_node(listener: tokio::net::TcpListener, state: AppState) -> Result<()> {
    start_node_with_admin_listener(listener, None, state).await
}

pub async fn start_node_with_admin_listener(
    listener: tokio::net::TcpListener,
    admin_listener: Option<tokio::net::TcpListener>,
    state: AppState,
) -> Result<()> {
    // Distributed nodes must fail closed before any background mutation can
    // race the canonical topology/bootstrap import.
    let distributed_recovery_required = state.config.requires_distributed_coremeta_recovery();
    let startup_recovery_deferred = state.core_store.startup_recovery_deferred();
    let public_readiness = startup_readiness::PublicReadiness::new(
        state.core_store.clone(),
        !startup_recovery_deferred,
    );
    let _coremeta_recovery_task = state
        .core_store
        .start_coremeta_distributed_recovery(distributed_recovery_required);
    if startup_recovery_deferred {
        let startup_state = state.clone();
        let startup_readiness = public_readiness.clone();
        tokio::spawn(async move {
            loop {
                startup_state
                    .core_store
                    .wait_for_coremeta_recovery_ready()
                    .await;
                match startup_state.ensure_system_realm_bootstrapped().await {
                    Ok(()) => {
                        startup_readiness.mark_system_realm_ready();
                        break;
                    }
                    Err(error) => {
                        error!(%error, "deferred system realm bootstrap failed");
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    }
                }
            }
        });
    }

    if state.config.run_background_worker {
        let worker_state = state.clone();
        let worker_readiness = public_readiness.clone();
        tokio::spawn(async move {
            // Recovery and system-realm bootstrap establish the canonical
            // metadata view. Background maintenance must not race either one.
            worker_readiness.wait_until_ready().await;
            // Queue scanning must share the worker capability and cancellation scope.
            let shard_recovery_store = worker_state.core_store.clone();
            let shard_recovery = async move {
                shard_recovery_store.run_distributed_shard_recovery().await;
                std::future::pending::<()>().await;
            };
            let worker = anvil_core::worker::run(
                worker_state.persistence.clone(),
                worker_state.core_store.clone(),
                worker_state.jwt_manager.clone(),
                worker_state.object_manager.clone(),
                worker_state.secret_keyring.clone(),
                worker_state.config.background_worker_concurrency,
            );
            tokio::select! {
                result = worker => {
                    if let Err(error) = result {
                        error!("Worker process failed: {}", error);
                    }
                }
                _ = shard_recovery => unreachable!("shard recovery supervisor completed"),
            }
        });
    }

    // --- Services ---
    let state_clone = state.clone();
    let auth_interceptor =
        anvil_core::services::AuthInterceptorFn::new(move |req: tonic::Request<()>| {
            middleware::auth_interceptor(req, &state_clone)
        });

    let mut grpc_router =
        anvil_core::services::create_grpc_router(state.clone(), auth_interceptor.clone());

    if let Some(ext) = ENTERPRISE_EXTENDER.get() {
        grpc_router = ext(grpc_router, state.clone(), auth_interceptor.clone());
    }

    let grpc_axum = anvil_core::services::create_axum_router(grpc_router);
    let admin_auth_state = state.clone();
    let admin_auth_interceptor =
        anvil_core::services::AuthInterceptorFn::new(move |req: tonic::Request<()>| {
            middleware::admin_auth_interceptor(req, &admin_auth_state)
        });
    let admin_axum = admin_listener.as_ref().map(|_| {
        anvil_core::services::create_admin_axum_router(
            state.clone(),
            admin_auth_interceptor.clone(),
        )
    });
    let s3_app = s3_gateway::app(state.clone());

    let app = tower::service_fn(move |req: axum::extract::Request| {
        let grpc_router = grpc_axum.clone();
        let s3_router = s3_app.clone();
        let public_readiness = public_readiness.clone();

        async move {
            let started_at = Instant::now();
            let method = req.method().to_string();
            let path = req.uri().path().to_string();
            let content_type = req
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();

            let plane = if content_type.starts_with("application/grpc") {
                "public-grpc"
            } else {
                "s3"
            };
            if !public_readiness.public_api_ready()
                && !startup_readiness::may_bypass_public_readiness(
                    &path,
                    public_readiness.coremeta_ready(),
                )
            {
                return Ok(startup_readiness::unavailable_response(
                    content_type.starts_with("application/grpc"),
                ));
            }
            let mux_request_id = uuid::Uuid::new_v4().simple().to_string();
            let context = vec![
                ("mux_request_id".to_string(), mux_request_id.clone()),
                ("plane".to_string(), plane.to_string()),
                ("method".to_string(), method.clone()),
                ("path".to_string(), path.clone()),
            ];
            let response = anvil_core::perf::with_context(context, async move {
                if content_type.starts_with("application/grpc") {
                    grpc_router.oneshot(req).await
                } else {
                    tracing::info!(
                        "[gRPC Mux] Routing to S3 gateway for content-type: {}",
                        content_type
                    );
                    s3_router.oneshot(req).await
                }
            })
            .await;
            let status = response
                .as_ref()
                .map(|response| response.status().as_u16().to_string())
                .unwrap_or_else(|_| "service_error".to_string());
            anvil_core::perf::record_duration(
                "anvil_request_mux",
                &[
                    ("mux_request_id", mux_request_id.as_str()),
                    ("plane", plane),
                    ("method", method.as_str()),
                    ("path", path.as_str()),
                    ("status", status.as_str()),
                ],
                started_at.elapsed(),
            );
            response
        }
    });

    let addr = listener.local_addr()?;
    info!("Anvil server (gRPC & S3) listening on {}", addr);
    let admin_addr = admin_listener
        .as_ref()
        .map(tokio::net::TcpListener::local_addr)
        .transpose()?;
    if let Some(admin_addr) = admin_addr {
        info!("Anvil admin gRPC listener available on {}", admin_addr);
    }

    let server_task = tokio::spawn(async move {
        let listener = listener.tap_io(|stream| {
            if let Err(error) = stream.set_nodelay(true) {
                tracing::warn!(%error, "failed to enable TCP_NODELAY on public connection");
            }
        });
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
    });
    let admin_server_task = admin_listener
        .zip(admin_axum)
        .map(|(admin_listener, admin_app)| {
            tokio::spawn(async move {
                let admin_listener = admin_listener.tap_io(|stream| {
                    if let Err(error) = stream.set_nodelay(true) {
                        tracing::warn!(%error, "failed to enable TCP_NODELAY on admin connection");
                    }
                });
                axum::serve(admin_listener, admin_app.into_make_service()).await
            })
        });

    // Run the public and optional admin gRPC servers concurrently.
    if let Some(admin_server_task) = admin_server_task {
        let (server_result, admin_result) = tokio::join!(server_task, admin_server_task);
        server_result??;
        admin_result??;
    } else {
        server_task.await??;
    }

    Ok(())
}

static ENTERPRISE_EXTENDER: OnceCell<
    fn(
        service::Routes,
        anvil_core::AppState,
        anvil_core::services::AuthInterceptorFn,
    ) -> service::Routes,
> = OnceCell::new();

pub fn register_enterprise_extender(
    f: fn(
        service::Routes,
        anvil_core::AppState,
        anvil_core::services::AuthInterceptorFn,
    ) -> service::Routes,
) {
    let _ = ENTERPRISE_EXTENDER.set(f);
}
