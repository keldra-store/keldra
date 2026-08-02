mod administration_service;
pub mod authentication;
mod authorization;
mod authz_api;
mod authz_service;
mod bootstrap;
mod cluster_startup;
mod credential_service;
mod mutable_record_quorum;
mod mutable_record_replica_group;
mod node_identity;
pub mod observability;
mod payload_placement;
mod peer_runtime;
mod placement;
mod programs;
mod serving_fence;
mod v05;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anvil_api::v1::administration_service_server::AdministrationServiceServer;
use anvil_api::v1::authz_service_server::AuthzServiceServer;
use anvil_api::v1::credential_service_server::CredentialServiceServer;
use anvil_api::v1::object_service_server::ObjectServiceServer;
use anvil_consensus::{ATOMIC_REPLAY_RETENTION_MILLIS, NodeId};
use anvil_store::{ErasureProfile, MutationReceiptRetention, Store, StoreOptions, WatchRetention};
use anyhow::{Context, Result};
use tonic::transport::Server;

use authentication::{JwtManager, RateLimitConfig, RequestRateLimits};

pub use v05::ObjectServiceImpl;

const MAX_GRPC_MESSAGE_BYTES: usize = 72 * 1024 * 1024;
const BLOB_GC_INTERVAL: Duration = Duration::from_secs(60 * 60);
const DECISION_LEADER_TIMEOUT: Duration = Duration::from_secs(10);
// A maximum 1,000-item authorization batch can contain two maximum-size exact
// paths per tuple plus identifiers and protobuf framing.
const MIN_AUTHZ_BATCH_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const _: () = assert!(MAX_GRPC_MESSAGE_BYTES >= MIN_AUTHZ_BATCH_MESSAGE_BYTES);

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub peer_listen: SocketAddr,
    pub peer_advertise: Option<String>,
    pub data_dir: PathBuf,
    pub run_system_bootstrap: bool,
    pub system_bootstrap_credential_output: Option<PathBuf>,
    pub node_id: u16,
    pub max_atomic_commit_entries: u32,
    pub max_atomic_commit_bytes: u64,
    pub atomic_program_timeout: Duration,
    pub token_manager: JwtManager,
    pub rate_limits: RateLimitConfig,
    pub max_blob_bytes: u64,
    pub erasure_profile: ErasureProfile,
    pub awaiting_publish_ttl_seconds: u64,
    pub mutation_receipt_retention_seconds: u64,
    pub max_mutation_receipt_entries: u64,
    pub max_mutation_receipt_bytes: u64,
    pub watch_max_entries: u64,
    pub watch_max_bytes: u64,
}

pub async fn serve(config: ServerConfig) -> Result<()> {
    anyhow::ensure!(
        !config.atomic_program_timeout.is_zero()
            && tokio::time::Instant::now()
                .checked_add(config.atomic_program_timeout)
                .is_some(),
        "atomic program timeout must be greater than zero and fit the server clock"
    );
    validate_atomic_replay_gc(config.awaiting_publish_ttl_seconds)?;
    let watch_retention = WatchRetention::new(config.watch_max_entries, config.watch_max_bytes)
        .context("validate watch retention")?;
    let mutation_receipt_retention = MutationReceiptRetention::new(
        config.mutation_receipt_retention_seconds,
        config.max_mutation_receipt_entries,
        config.max_mutation_receipt_bytes,
    )
    .context("validate mutation receipt retention")?;
    let peer_address =
        peer_runtime::peer_address(config.peer_listen, config.peer_advertise.as_deref())?;
    let store = Store::open(
        StoreOptions::new(&config.data_dir, config.node_id)
            .with_watch_retention(watch_retention)
            .with_mutation_receipt_retention(mutation_receipt_retention)
            .with_awaiting_publish_ttl_seconds(config.awaiting_publish_ttl_seconds),
    )
    .await
    .with_context(|| format!("open Anvil data at {}", config.data_dir.display()))?;
    let local_node = NodeId(u64::from(config.node_id));
    let (decisions, peer_runtime) = peer_runtime::open(peer_runtime::OpenPeerConfig {
        data_dir: &config.data_dir,
        node_id: local_node,
        peer_address,
        run_system_bootstrap: config.run_system_bootstrap,
        max_commit_entries: config.max_atomic_commit_entries,
        max_commit_bytes: config.max_atomic_commit_bytes,
        leader_timeout: DECISION_LEADER_TIMEOUT,
    })
    .await?;
    // The private listener must be accepting before an existing multi-node
    // group can elect a leader after a coordinated restart.
    let mut peer_server = peer_runtime
        .start(config.peer_listen, decisions.clone())
        .await?;
    decisions
        .wait_for_leader(DECISION_LEADER_TIMEOUT)
        .await
        .context("elect decision leader")?;
    let cluster_id = cluster_startup::ensure_genesis_identity(&decisions).await?;
    tracing::info!(cluster.id = %hex::encode(cluster_id.0), "cluster identity is ready");
    cluster_startup::ensure_erasure_code_profile(&decisions, config.erasure_profile).await?;
    tracing::info!(
        erasure.data_shards = config.erasure_profile.data_shards(),
        erasure.parity_shards = config.erasure_profile.parity_shards(),
        erasure.stripe_unit_bytes = config.erasure_profile.stripe_unit(),
        "cluster erasure-code profile is ready"
    );
    let programs =
        programs::ProgramCoordinator::start(store.clone(), decisions.clone(), local_node).await?;
    cluster_startup::reconcile_system_bootstrap(
        &store,
        &decisions,
        local_node,
        &config.data_dir,
        config.run_system_bootstrap,
        config.system_bootstrap_credential_output.as_deref(),
    )
    .await?;
    let authz_repository = store.authz();
    // A committed bundle may have spent longer than the inactivity grace on
    // disk while this process was down. Recovery must pin/finalize every Raft
    // decision before startup GC considers ordinary awaiting blobs.
    collect_blob_garbage(&store, "startup").await;
    let object_service = ObjectServiceImpl::new(
        store.clone(),
        programs.clone(),
        config.token_manager.clone(),
        config.max_blob_bytes,
        config.atomic_program_timeout,
    );
    let authz_service = authz_service::AuthzServiceImpl::new(authz_repository);
    let administration_service =
        administration_service::AdministrationServiceImpl::new(store.clone());
    let request_rate_limits = RequestRateLimits::new(config.rate_limits);
    let credential_service = credential_service::CredentialServiceImpl::new(
        store.clone(),
        config.token_manager.clone(),
        request_rate_limits.clone(),
    );
    let object_service = ObjectServiceServer::new(object_service)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
    let authz_service = AuthzServiceServer::new(authz_service)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
    let tokens = config.token_manager;
    let authenticate =
        move |request: tonic::Request<()>| request_rate_limits.authenticate(&tokens, request);
    let object_service =
        tonic::service::interceptor::InterceptedService::new(object_service, authenticate.clone());
    let authz_service =
        tonic::service::interceptor::InterceptedService::new(authz_service, authenticate.clone());
    let administration_service = tonic::service::interceptor::InterceptedService::new(
        AdministrationServiceServer::new(administration_service),
        authenticate,
    );

    let blob_gc_task = spawn_blob_gc(store);
    tracing::info!(address = %config.listen, "Anvil 0.5 server listening");
    let (stop_public, public_stopped) = tokio::sync::oneshot::channel();
    let mut public_server = Box::pin(
        Server::builder()
            .add_service(object_service)
            .add_service(authz_service)
            .add_service(administration_service)
            // Deliberately not intercepted: this one service exchanges durable
            // long-lived credentials for the bearer token used everywhere else.
            .add_service(CredentialServiceServer::new(credential_service))
            .serve_with_shutdown(config.listen, async move {
                let _ = public_stopped.await;
            }),
    );
    enum FirstStop {
        Signal(std::io::Result<()>),
        Public(Result<(), tonic::transport::Error>),
        Peer(Result<Result<()>, tokio::task::JoinError>),
    }
    let first_stop = tokio::select! {
        signal = tokio::signal::ctrl_c() => FirstStop::Signal(signal),
        public = &mut public_server => FirstStop::Public(public),
        peer = peer_server.task_mut() => FirstStop::Peer(peer),
    };
    let server_result = match first_stop {
        FirstStop::Signal(signal) => {
            let _ = stop_public.send(());
            let public = public_server.await.context("serve public listener");
            let peer = peer_server.shutdown().await;
            signal.context("wait for shutdown signal")?;
            public?;
            peer
        }
        FirstStop::Public(public) => {
            let peer = peer_server.shutdown().await;
            public.context("serve public listener")?;
            peer
        }
        FirstStop::Peer(peer) => {
            peer_server.record_completed();
            let _ = stop_public.send(());
            let public = public_server.await.context("serve public listener");
            let peer = peer
                .context("join private peer server task")?
                .context("serve private peer listener");
            peer?;
            public
        }
    };
    blob_gc_task.abort();
    if let Err(error) = blob_gc_task.await
        && !error.is_cancelled()
    {
        tracing::error!(%error, "blob garbage-collection task stopped unexpectedly");
    }
    let shutdown_result = decisions
        .shutdown()
        .await
        .context("shut down decision Raft");
    server_result?;
    shutdown_result
}

fn validate_atomic_replay_gc(awaiting_publish_ttl_seconds: u64) -> Result<()> {
    let blob_gc_inactivity_millis = awaiting_publish_ttl_seconds
        .checked_mul(1_000)
        .context("awaiting-publish blob TTL exceeds u64 milliseconds")?;
    anyhow::ensure!(
        blob_gc_inactivity_millis >= ATOMIC_REPLAY_RETENTION_MILLIS,
        "awaiting-publish blob TTL must be at least the fixed 24-hour atomic replay window"
    );
    Ok(())
}

fn spawn_blob_gc(store: Store) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let first_run = tokio::time::Instant::now() + BLOB_GC_INTERVAL;
        let mut interval = tokio::time::interval_at(first_run, BLOB_GC_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            collect_blob_garbage(&store, "scheduled").await;
        }
    })
}

async fn collect_blob_garbage(store: &Store, trigger: &'static str) {
    match store.collect_blob_garbage().await {
        Ok(removed) => {
            tracing::info!(
                monotonic_counter.anvil_blob_gc_runs_total = 1_u64,
                monotonic_counter.anvil_blob_gc_removed_total = removed,
                trigger,
                removed,
                "blob garbage-collection pass completed"
            );
        }
        Err(error) => {
            tracing::error!(
                monotonic_counter.anvil_blob_gc_failures_total = 1_u64,
                trigger,
                %error,
                "blob garbage-collection pass failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_blob_gc_cannot_expire_before_atomic_replay() {
        let replay_seconds = ATOMIC_REPLAY_RETENTION_MILLIS / 1_000;
        assert!(validate_atomic_replay_gc(replay_seconds).is_ok());
        assert!(validate_atomic_replay_gc(replay_seconds - 1).is_err());
    }
}
