mod accounting;
mod administration_service;
pub mod authentication;
mod authoritative_system;
mod authorization;
mod authz_api;
mod authz_distribution;
mod authz_service;
mod bootstrap;
mod bucket_governance;
mod cluster_list_watch;
mod cluster_object_read;
mod cluster_peer;
mod cluster_placement;
mod cluster_startup;
mod credential_service;
mod data_peer;
mod distributed_control_plane;
mod distributed_list;
mod distributed_watch;
mod git_gateway;
mod http_gateway;
mod index_config;
mod index_runtime;
mod index_service;
mod join_bundle;
mod join_peer;
mod logical_name_resolution;
mod logical_record_distribution;
mod mutable_record_quorum;
mod mutable_record_replica_group;
mod mutation_admission;
mod node_identity;
mod object_distribution;
mod object_path_access;
pub mod observability;
mod payload_distribution;
mod payload_gc;
mod payload_placement;
mod payload_read;
mod payload_read_transport;
mod peer_runtime;
mod personaldb;
mod placement;
mod programs;
#[allow(
    dead_code,
    reason = "transport-neutral delivery awaits the approved quorum-proof adapter"
)]
mod reference_delivery;
mod s3;
mod serving_fence;
mod v05;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anvil_api::v1::accounting_service_server::AccountingServiceServer;
use anvil_api::v1::administration_service_server::AdministrationServiceServer;
use anvil_api::v1::authz_service_server::AuthzServiceServer;
use anvil_api::v1::credential_service_server::CredentialServiceServer;
use anvil_api::v1::index_service_server::IndexServiceServer;
use anvil_api::v1::object_service_server::ObjectServiceServer;
use anvil_api::v1::personal_db_service_server::PersonalDbServiceServer;
use anvil_consensus::{ATOMIC_REPLAY_RETENTION_MILLIS, NodeId};
use anvil_store::{ErasureProfile, MutationReceiptRetention, Store, StoreOptions, WatchRetention};
use anyhow::{Context, Result};

use authentication::{JwtManager, RateLimitConfig, RequestRateLimits};
use mutation_admission::{AdmissionSurface, MutationAdmissionService};

pub use index_config::{IndexRuntimeConfig, IndexRuntimeConfigError};
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
    pub join_bundle: Option<PathBuf>,
    pub data_dir: PathBuf,
    pub run_system_bootstrap: bool,
    pub system_bootstrap_credential_output: Option<PathBuf>,
    pub node_id: u16,
    pub max_atomic_commit_entries: u32,
    pub max_atomic_commit_bytes: u64,
    pub atomic_program_timeout: Duration,
    pub token_manager: JwtManager,
    pub rate_limits: RateLimitConfig,
    pub index_runtime: IndexRuntimeConfig,
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
        join_bundle: config.join_bundle.as_deref(),
        run_system_bootstrap: config.run_system_bootstrap,
        max_commit_entries: config.max_atomic_commit_entries,
        max_commit_bytes: config.max_atomic_commit_bytes,
        leader_timeout: DECISION_LEADER_TIMEOUT,
    })
    .await?;
    let serving_transport = peer_runtime.serving_transport();
    let data_transport = peer_runtime.data_transport();
    let cluster_transport =
        cluster_peer::ClusterPeerTransport::new(data_transport.clone(), decisions.clone());
    let routed_public_handlers = peer_runtime.routed_public_handlers();
    let routed_authz_handlers = peer_runtime.routed_authz_handlers();
    let routed_index_query_handlers = peer_runtime.routed_index_query_handlers();
    let routed_accounting_handlers = peer_runtime.routed_accounting_handlers();
    let routed_personaldb_handlers = peer_runtime.routed_personaldb_handlers();
    let list_authorizer_binding = peer_runtime.list_authorizer();
    let fresh_authorization_binding = peer_runtime.fresh_authorization();
    let distributed_control_binding = peer_runtime.distributed_control();
    let name_resolution_binding = peer_runtime.name_resolution();
    let program_quiescence_binding = peer_runtime.program_quiescence();
    let index_artifacts_binding = peer_runtime.index_artifacts();
    let pending_join = peer_runtime.join_transport();
    let mutation_admission = peer_runtime.mutation_admission();
    // The private listener must be accepting before an existing multi-node
    // group can elect a leader after a coordinated restart.
    let mut peer_server = peer_runtime
        .start(
            config.peer_listen,
            decisions.clone(),
            store.clone(),
            config.erasure_profile,
            config.atomic_program_timeout,
            config.max_blob_bytes,
        )
        .await?;
    if let Some(pending_join) = pending_join.as_ref() {
        peer_runtime::complete_pending_join(
            pending_join,
            &decisions,
            &config.data_dir,
            config.erasure_profile,
            DECISION_LEADER_TIMEOUT,
        )
        .await
        .context("join existing cluster through typed ownership handoff")?;
    }
    decisions
        .wait_for_leader(DECISION_LEADER_TIMEOUT)
        .await
        .context("elect decision leader")?;
    let cluster_id = cluster_startup::ensure_genesis_identity(&decisions).await?;
    tracing::info!(cluster.id = %hex::encode(cluster_id.0), "cluster identity is ready");
    cluster_startup::ensure_jwt_signing_key_fingerprint(
        &decisions,
        config.token_manager.signing_key_fingerprint(),
    )
    .await?;
    cluster_startup::ensure_erasure_code_profile(&decisions, config.erasure_profile).await?;
    tracing::info!(
        erasure.data_shards = config.erasure_profile.data_shards(),
        erasure.parity_shards = config.erasure_profile.parity_shards(),
        erasure.stripe_unit_bytes = config.erasure_profile.stripe_unit(),
        "cluster erasure-code profile is ready"
    );
    let serving_fence = serving_fence::ServingFenceRuntime::start(
        decisions.clone(),
        serving_transport,
        DECISION_LEADER_TIMEOUT,
    )
    .await
    .context("establish initial serving fence after cutover")?;
    let (reference_runtime, reference_runtime_handle) = reference_delivery::ReferenceRuntime::start(
        local_node,
        store.clone(),
        decisions.clone(),
        serving_fence.authority(),
        data_transport.clone(),
        cluster_transport.clone(),
        config.erasure_profile,
        mutation_admission.clone(),
    );
    // Ordered reference effects and their cursor must be recovered before any
    // startup component can create another journal entry. In particular,
    // program recovery must not race a cursor left behind by a 0.5.3 node.
    enum ReferenceStartup {
        Ready,
        Signal(std::io::Result<()>),
        Peer(Result<Result<()>, tokio::task::JoinError>),
    }
    let reference_startup = tokio::select! {
        _ = reference_runtime_handle.wait_until_startup_ready() => ReferenceStartup::Ready,
        signal = tokio::signal::ctrl_c() => ReferenceStartup::Signal(signal),
        peer = peer_server.task_mut() => ReferenceStartup::Peer(peer),
    };
    match reference_startup {
        ReferenceStartup::Ready => {}
        ReferenceStartup::Signal(signal) => {
            reference_runtime.shutdown().await;
            let peer = peer_server.shutdown().await;
            serving_fence.shutdown().await;
            let raft = decisions
                .shutdown()
                .await
                .context("shut down decision Raft during startup");
            signal.context("wait for shutdown signal during startup")?;
            peer?;
            raft?;
            return Ok(());
        }
        ReferenceStartup::Peer(peer) => {
            peer_server.record_completed();
            reference_runtime.shutdown().await;
            serving_fence.shutdown().await;
            let raft = decisions
                .shutdown()
                .await
                .context("shut down decision Raft after startup peer failure");
            peer.context("join private peer server task during startup")?
                .context("serve private peer listener during startup")?;
            raft?;
            return Ok(());
        }
    }
    let payload_read_transport = payload_read_transport::StorePayloadReadTransport::new(
        local_node,
        store.clone(),
        data_transport.clone(),
        config.erasure_profile,
    )
    .context("initialize payload-read transport")?;
    let object_distribution = object_distribution::ObjectDistribution::new(
        local_node,
        store.clone(),
        decisions.clone(),
        serving_fence.authority(),
        data_transport.clone(),
        config.erasure_profile,
        reference_runtime_handle.clone(),
        config.atomic_program_timeout,
        mutation_admission.clone(),
    );
    let object_reader = cluster_object_read::ClusterObjectReader::new(
        object_distribution.clone(),
        config.erasure_profile,
        std::sync::Arc::new(payload_read_transport),
        &config.data_dir,
    )
    .context("initialize cluster object reader")?;
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
    let logical_records = logical_record_distribution::LogicalRecordDistribution::new(
        local_node,
        store.clone(),
        decisions.clone(),
        serving_fence.authority(),
        Arc::new(cluster_transport.clone()),
        mutation_admission.clone(),
    );
    let name_resolver = logical_name_resolution::LogicalNameResolver::new(
        logical_records.clone(),
        cluster_transport.clone(),
    );
    name_resolution_binding
        .install(Arc::new(name_resolver.clone()))
        .map_err(|_| anyhow::anyhow!("logical name resolver was installed more than once"))?;
    programs
        .install_distributed(
            object_reader.clone(),
            object_distribution.clone(),
            cluster_transport.clone(),
            name_resolver.clone(),
        )
        .await
        .context("initialize distributed atomic programs")?;
    program_quiescence_binding
        .install(programs.clone())
        .map_err(|_| anyhow::anyhow!("atomic program quiescence was installed more than once"))?;
    let zanzibar = Arc::new(authz_distribution::ZanzibarDistribution::new(
        local_node,
        authz_repository.clone(),
        decisions.clone(),
        serving_fence.authority(),
        Arc::new(cluster_transport.clone()),
        mutation_admission.clone(),
    ));
    fresh_authorization_binding
        .install(zanzibar.clone())
        .map_err(|_| anyhow::anyhow!("fresh Zanzibar handler was installed more than once"))?;
    let distributed_control = Arc::new(distributed_control_plane::DistributedControlPlane::new(
        local_node,
        store.clone(),
        decisions.clone(),
        serving_fence.authority(),
        logical_records.clone(),
        zanzibar.clone(),
        cluster_transport.clone(),
        config.token_manager.clone(),
    ));
    distributed_control_binding
        .install(distributed_control.clone())
        .map_err(|_| anyhow::anyhow!("distributed control plane was installed more than once"))?;
    let authoritative_system = authoritative_system::AuthoritativeSystemAuthorization::new(
        local_node,
        decisions.clone(),
        zanzibar.clone(),
        cluster_transport.clone(),
        name_resolver.clone(),
    );
    let bucket_governance = bucket_governance::BucketGovernance::new(
        logical_records,
        cluster_transport.clone(),
        name_resolver.clone(),
    );
    let index_artifact_coordinator = index_runtime::publication::IndexArtifactCoordinator::new(
        object_distribution.clone(),
        bucket_governance.clone(),
    );
    index_artifacts_binding
        .install(Arc::new(index_artifact_coordinator))
        .map_err(|_| anyhow::anyhow!("index artifact coordinator was installed more than once"))?;
    let list_authorizer: Arc<dyn distributed_list::AuthoritativeListAuthorizer> =
        Arc::new(distributed_list::CoordinatedListAuthorizer::new(
            config.token_manager.clone(),
            Arc::new(cluster_transport.clone()),
        ));
    list_authorizer_binding
        .install(list_authorizer.clone())
        .map_err(|_| anyhow::anyhow!("list authorizer was installed more than once"))?;
    let object_lister = distributed_list::DistributedObjectLister::new(
        local_node,
        store.clone(),
        decisions.clone(),
        Arc::new(cluster_transport.clone()),
        list_authorizer.clone(),
    );
    let watch_sources = Arc::new(cluster_list_watch::ClusterWatchSourcesAdapter::new(
        local_node,
        store.clone(),
        decisions.clone(),
        cluster_transport.clone(),
        list_authorizer,
    ));
    let distributed_watch = Arc::new(distributed_watch::DistributedWatch::new(
        Arc::new(cluster_list_watch::DecisionWatchPlacement::new(
            decisions.clone(),
        )),
        watch_sources,
        Arc::new(config.token_manager.clone()),
    ));
    // No public request is accepted until ordered reference delivery proves
    // every current source tail locally applied. Recheck immediately before
    // the destructive scan in case placement changed after readiness.
    collect_blob_garbage_if_safe(&store, &reference_runtime_handle, "startup").await;
    let index_runtime = index_runtime::runtime::start(
        local_node,
        decisions.clone(),
        store.clone(),
        data_transport.clone(),
        cluster_transport.clone(),
        object_distribution.clone(),
        bucket_governance.clone(),
        object_reader.clone(),
        &config.data_dir,
        config.index_runtime,
    )
    .await
    .context("initialize distributed index runtime")?;
    let index_authorization: Arc<dyn index_service::IndexAuthorization> =
        Arc::new(authoritative_system.clone());
    routed_index_query_handlers
        .install(Arc::new(cluster_peer::AuthorizedIndexQueryHandler::new(
            local_node,
            config.token_manager.clone(),
            name_resolver.clone(),
            index_authorization.clone(),
            index_runtime.local_queries.clone(),
        )))
        .map_err(|_| anyhow::anyhow!("routed index query handler was installed more than once"))?;
    let mut accounting_runtime = accounting::runtime::start(
        local_node,
        decisions.clone(),
        store.clone(),
        object_reader.clone(),
        index_runtime.scanner.clone(),
        index_runtime.event_journal.clone(),
        index_runtime.artifact_router.clone(),
    )
    .await
    .context("initialize distributed accounting runtime")?;
    let accounting_service = accounting::AccountingServiceImpl::new(
        local_node,
        decisions.clone(),
        config.token_manager.clone(),
        name_resolver.clone(),
        authoritative_system.clone(),
        cluster_transport.clone(),
        object_reader.clone(),
        accounting_runtime.publisher.clone(),
        accounting_runtime.catalog.clone(),
        config.atomic_program_timeout,
    );
    routed_accounting_handlers
        .install(accounting_service.routed_handler())
        .map_err(|_| anyhow::anyhow!("routed accounting handler was installed more than once"))?;
    accounting_runtime.start_traffic(
        local_node,
        decisions.clone(),
        store.clone(),
        cluster_transport.clone(),
        accounting_service.clone(),
    );
    let object_service = ObjectServiceImpl::new(
        store.clone(),
        programs.clone(),
        object_distribution,
        object_reader,
        cluster_transport.clone(),
        object_lister.clone(),
        distributed_watch,
        name_resolver.clone(),
        authoritative_system.clone(),
        bucket_governance,
        config.token_manager.clone(),
        accounting_runtime.traffic.clone(),
        config.max_blob_bytes,
        config.atomic_program_timeout,
    );
    let index_service = index_service::IndexServiceImpl::new(
        object_service.clone(),
        name_resolver.clone(),
        index_service::IndexServiceDependencies {
            definitions: index_runtime.definitions.clone(),
            queries: index_runtime.queries.clone(),
            authorization: index_authorization,
            page_tokens: Arc::new(config.token_manager.clone()),
        },
        config.atomic_program_timeout,
    );
    let personaldb_service = personaldb::PersonalDbServiceImpl::new(
        local_node,
        decisions.clone(),
        config.token_manager.clone(),
        name_resolver.clone(),
        authoritative_system.clone(),
        zanzibar.clone(),
        store.clone(),
        distributed_control.clone(),
        cluster_transport.clone(),
        personaldb::PersonalDbObjects::new(object_service.clone()),
        object_lister.clone(),
        config.atomic_program_timeout,
    )
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    routed_personaldb_handlers
        .install(personaldb_service.routed_handler())
        .map_err(|_| anyhow::anyhow!("routed PersonalDB handler was installed more than once"))?;
    let request_rate_limits = RequestRateLimits::new(config.rate_limits);
    let gateway_objects = v05::GatewayObjectAdapter::new(object_service.clone());
    let s3_state = s3::S3State {
        objects: gateway_objects.clone(),
        control: distributed_control.clone(),
        tokens: config.token_manager.clone(),
        rate_limits: request_rate_limits.clone(),
        serving: serving_fence.authority(),
        mutation_admission: mutation_admission.clone(),
    };
    let git_state = git_gateway::GitGatewayState {
        objects: gateway_objects,
        control: distributed_control.clone(),
        tokens: config.token_manager.clone(),
        rate_limits: request_rate_limits.clone(),
        serving: serving_fence.authority(),
        mutation_admission: mutation_admission.clone(),
        cache_root: config.data_dir.join("gateway-cache/git"),
        max_request_bytes: config.max_blob_bytes,
        lock: Arc::new(tokio::sync::Mutex::new(())),
        basic_tokens: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
    };
    routed_public_handlers
        .install(object_service.routed_public_handler())
        .map_err(|_| anyhow::anyhow!("routed public handler was installed more than once"))?;
    let distributed_authz = authz_service::DistributedAuthzService::new(
        local_node,
        store.clone(),
        decisions.clone(),
        zanzibar,
        cluster_transport.clone(),
        name_resolver,
        authoritative_system,
        config.token_manager.clone(),
    );
    let authz_service =
        authz_service::AuthzServiceImpl::new(authz_repository).with_distributed(distributed_authz);
    routed_authz_handlers
        .install(authz_service.routed_authz_handler())
        .map_err(|_| {
            anyhow::anyhow!("routed authorization handler was installed more than once")
        })?;
    let administration_service = administration_service::AdministrationServiceImpl::new(
        store.clone(),
        decisions.clone(),
        config.data_dir.clone(),
    )
    .with_distributed(distributed_control.clone());
    let credential_service = credential_service::CredentialServiceImpl::new(
        store.clone(),
        config.token_manager.clone(),
        request_rate_limits.clone(),
    )
    .with_distributed(distributed_control.clone());
    let object_service = ObjectServiceServer::new(object_service)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
    let index_service = IndexServiceServer::new(index_service)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
    let personaldb_service = PersonalDbServiceServer::new(personaldb_service)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
    let accounting_service = AccountingServiceServer::new(accounting_service)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
    let authz_service = AuthzServiceServer::new(authz_service)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
    let tokens = config.token_manager;
    let object_tokens = tokens.clone();
    let object_rate_limits = request_rate_limits.clone();
    let object_authority = serving_fence.authority();
    let authenticate_object = move |request: tonic::Request<()>| {
        let request = object_authority.require(request)?;
        object_rate_limits.authenticate_object(&object_tokens, request)
    };
    let index_tokens = tokens.clone();
    let index_rate_limits = request_rate_limits.clone();
    let index_authority = serving_fence.authority();
    let authenticate_index = move |request: tonic::Request<()>| {
        let request = index_authority.require(request)?;
        index_rate_limits.authenticate_index(&index_tokens, request)
    };
    let authenticated_authority = serving_fence.authority();
    let authenticate = move |request: tonic::Request<()>| {
        let request = authenticated_authority.require(request)?;
        request_rate_limits.authenticate(&tokens, request)
    };
    let object_service =
        tonic::service::interceptor::InterceptedService::new(object_service, authenticate_object);
    let index_service =
        tonic::service::interceptor::InterceptedService::new(index_service, authenticate_index);
    let personaldb_service = tonic::service::interceptor::InterceptedService::new(
        personaldb_service,
        authenticate.clone(),
    );
    let accounting_service = tonic::service::interceptor::InterceptedService::new(
        accounting_service,
        authenticate.clone(),
    );
    let authz_service =
        tonic::service::interceptor::InterceptedService::new(authz_service, authenticate.clone());
    let administration_service = tonic::service::interceptor::InterceptedService::new(
        AdministrationServiceServer::new(administration_service),
        authenticate,
    );
    let credential_authority = serving_fence.authority();
    let credential_service = tonic::service::interceptor::InterceptedService::new(
        CredentialServiceServer::new(credential_service),
        move |request| credential_authority.require(request),
    );

    let object_service = MutationAdmissionService::new(
        object_service,
        mutation_admission.clone(),
        AdmissionSurface::Public,
    );
    let index_service = MutationAdmissionService::new(
        index_service,
        mutation_admission.clone(),
        AdmissionSurface::Public,
    );
    let personaldb_service = MutationAdmissionService::new(
        personaldb_service,
        mutation_admission.clone(),
        AdmissionSurface::Public,
    );
    let accounting_service = MutationAdmissionService::new(
        accounting_service,
        mutation_admission.clone(),
        AdmissionSurface::Public,
    );
    let authz_service = MutationAdmissionService::new(
        authz_service,
        mutation_admission.clone(),
        AdmissionSurface::Public,
    );
    let administration_service = MutationAdmissionService::new(
        administration_service,
        mutation_admission.clone(),
        AdmissionSurface::Public,
    );
    let credential_service = MutationAdmissionService::new(
        credential_service,
        mutation_admission,
        AdmissionSurface::Public,
    );

    let grpc_router = tonic::service::Routes::new(object_service)
        .add_service(index_service)
        .add_service(personaldb_service)
        .add_service(accounting_service)
        .add_service(authz_service)
        .add_service(administration_service)
        // Deliberately not bearer-authenticated: this service exchanges
        // durable long-lived credentials for that bearer token. It still
        // requires the node-wide serving fence.
        .add_service(credential_service)
        .into_axum_router();
    let gateway_router = s3::router(s3_state).merge(git_gateway::router(git_state));
    let mut public_server =
        http_gateway::PublicServer::start(config.listen, grpc_router, gateway_router)
            .await
            .context("start public gRPC, S3, and Git listener")?;
    let payload_gc = payload_gc::PayloadGarbageCollector::new(
        local_node,
        store.clone(),
        decisions.clone(),
        data_transport,
        reference_runtime_handle.clone(),
        config.erasure_profile,
    );
    let blob_gc_task = spawn_blob_gc(store, reference_runtime_handle, payload_gc);
    enum FirstStop {
        Signal(std::io::Result<()>),
        Public(Result<Result<()>, tokio::task::JoinError>),
        Peer(Result<Result<()>, tokio::task::JoinError>),
    }
    let first_stop = tokio::select! {
        signal = tokio::signal::ctrl_c() => FirstStop::Signal(signal),
        public = public_server.task_mut() => FirstStop::Public(public),
        peer = peer_server.task_mut() => FirstStop::Peer(peer),
    };
    let server_result = match first_stop {
        FirstStop::Signal(signal) => {
            let public = public_server.shutdown().await;
            let peer = peer_server.shutdown().await;
            signal.context("wait for shutdown signal")?;
            public?;
            peer
        }
        FirstStop::Public(public) => {
            public_server.record_completed();
            let peer = peer_server.shutdown().await;
            public
                .context("join public server task")?
                .context("serve public listener")?;
            peer
        }
        FirstStop::Peer(peer) => {
            peer_server.record_completed();
            let public = public_server.shutdown().await;
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
    reference_runtime.shutdown().await;
    serving_fence.shutdown().await;
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

fn spawn_blob_gc(
    store: Store,
    references: reference_delivery::ReferenceRuntimeHandle,
    payloads: payload_gc::PayloadGarbageCollector,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let first_run = tokio::time::Instant::now() + BLOB_GC_INTERVAL;
        let mut interval = tokio::time::interval_at(first_run, BLOB_GC_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            match payloads.run_once().await {
                Ok(retired) if retired > 0 => {
                    tracing::info!(
                        retired,
                        "former payload artifacts entered the ordinary GC grace window"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, "former payload-artifact retirement paused");
                }
            }
            collect_blob_garbage_if_safe(&store, &references, "scheduled").await;
        }
    })
}

async fn collect_blob_garbage_if_safe(
    store: &Store,
    references: &reference_delivery::ReferenceRuntimeHandle,
    trigger: &'static str,
) {
    if !references.gc_safe().await {
        tracing::warn!(
            monotonic_counter.anvil_blob_gc_paused_total = 1_u64,
            trigger,
            "blob garbage collection paused until every ACTIVE source tail is current"
        );
        return;
    }
    collect_blob_garbage(store, trigger).await;
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
