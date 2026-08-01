pub mod authentication;
mod authorization;
mod authz_api;
mod authz_service;
mod programs;
mod v05;

use std::net::SocketAddr;
use std::path::PathBuf;

use anvil_api::v1::authz_service_server::AuthzServiceServer;
use anvil_api::v1::object_service_server::ObjectServiceServer;
use anvil_store::{Store, StoreOptions};
use anyhow::{Context, Result, bail};
use tonic::transport::Server;
use tonic::{Request, Status};

pub use v05::ObjectServiceImpl;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub node_id: u16,
    pub max_atomic_commit_entries: u32,
    pub max_atomic_commit_bytes: u64,
    pub api_token: String,
    pub insecure_no_auth: bool,
    pub max_blob_bytes: u64,
}

pub async fn serve(config: ServerConfig) -> Result<()> {
    if config.api_token.is_empty() && !config.insecure_no_auth {
        bail!("ANVIL_API_TOKEN is required unless --insecure-no-auth is explicit");
    }
    let store = Store::open(StoreOptions::new(&config.data_dir, config.node_id))
        .await
        .with_context(|| format!("open Anvil data at {}", config.data_dir.display()))?;
    let authz_repository = store.authz();
    let bootstrap_repository = authz_repository.clone();
    tokio::task::spawn_blocking(move || authorization::ensure_system_realm(&bootstrap_repository))
        .await
        .context("join protected authorization bootstrap")?
        .context("install protected authorization realm")?;
    let programs = programs::ProgramCoordinator::open(
        store.clone(),
        &config.data_dir,
        u64::from(config.node_id),
        config.max_atomic_commit_entries,
        config.max_atomic_commit_bytes,
    )
    .await?;
    let object_service = ObjectServiceImpl::new(store, programs.clone(), config.max_blob_bytes);
    let authz_service = authz_service::AuthzServiceImpl::new(authz_repository);
    let required_token = config.api_token;
    let insecure = config.insecure_no_auth;
    let object_service = ObjectServiceServer::new(object_service)
        .max_decoding_message_size(72 * 1024 * 1024)
        .max_encoding_message_size(72 * 1024 * 1024);
    let authenticate = move |request: Request<()>| {
        if insecure {
            return Ok(request);
        }
        let authorized = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|token| constant_time_equal(token.as_bytes(), required_token.as_bytes()));
        if authorized {
            Ok(request)
        } else {
            Err(Status::unauthenticated("a valid bearer token is required"))
        }
    };
    let object_service =
        tonic::service::interceptor::InterceptedService::new(object_service, authenticate.clone());
    let authz_service = tonic::service::interceptor::InterceptedService::new(
        AuthzServiceServer::new(authz_service),
        authenticate,
    );

    tracing::info!(address = %config.listen, "Anvil 0.5 server listening");
    let server_result = Server::builder()
        .add_service(object_service)
        .add_service(authz_service)
        .serve_with_shutdown(config.listen, async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(%error, "failed to install shutdown signal");
            }
        })
        .await;
    let shutdown_result = programs.shutdown().await;
    server_result?;
    shutdown_result
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_comparison_checks_every_byte() {
        assert!(constant_time_equal(b"secret", b"secret"));
        assert!(!constant_time_equal(b"secret", b"secrex"));
        assert!(!constant_time_equal(b"secret", b"short"));
    }
}
