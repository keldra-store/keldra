pub mod admin;
pub(crate) mod admin_cursor;
pub mod audit;
pub mod auth;
pub(crate) mod authz_status;
pub mod bucket;
pub mod collection_cursor;
pub mod coordination;
pub(crate) mod core_store_status;
pub mod corestore_internal;
pub mod git_source;
pub mod huggingface;
pub mod index;
pub mod internal_proxy;
pub mod mesh_control;
pub mod object;
pub mod personaldb;
pub mod registry;
pub mod repair;
#[cfg(feature = "root-publication-test-control")]
mod root_publication_test_control;
pub mod saga;
pub(crate) mod saga_reserved;
pub mod stream;
pub mod transaction;
pub(crate) mod watch_envelope;

use crate::anvil_api::{
    admin_service_server::AdminServiceServer,
    anti_entropy_internal_server::AntiEntropyInternalServer,
    audit_service_server::AuditServiceServer, auth_service_server::AuthServiceServer,
    block_store_internal_server::BlockStoreInternalServer,
    bucket_service_server::BucketServiceServer,
    coordination_service_server::CoordinationServiceServer,
    core_meta_replication_internal_server::CoreMetaReplicationInternalServer,
    cross_region_proxy_internal_server::CrossRegionProxyInternalServer,
    git_source_service_server::GitSourceServiceServer,
    hf_ingestion_service_server::HfIngestionServiceServer,
    hugging_face_key_service_server::HuggingFaceKeyServiceServer,
    index_service_server::IndexServiceServer,
    internal_proxy_service_server::InternalProxyServiceServer,
    mesh_control_service_server::MeshControlServiceServer,
    object_service_server::ObjectServiceServer,
    personal_db_service_server::PersonalDbServiceServer,
    registry_service_server::RegistryServiceServer, repair_service_server::RepairServiceServer,
    root_register_internal_server::RootRegisterInternalServer,
    saga_service_server::SagaServiceServer, stream_service_server::StreamServiceServer,
    transaction_service_server::TransactionServiceServer,
};
use crate::{AppState, middleware};
use tonic::service::Routes;
use tonic::{Request, Status};

#[derive(Clone)]
pub struct AuthInterceptorFn {
    f: std::sync::Arc<dyn Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync>,
}

impl AuthInterceptorFn {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static,
    {
        Self {
            f: std::sync::Arc::new(f),
        }
    }

    pub fn call(&self, req: Request<()>) -> Result<Request<()>, Status> {
        (self.f)(req)
    }
}

pub fn create_grpc_router(state: AppState, auth_interceptor: AuthInterceptorFn) -> Routes {
    // Adapt our handle to a closure Interceptor Tonic accepts
    let auth_closure = {
        let f = auth_interceptor.clone();
        move |req| f.call(req)
    };
    tonic::service::Routes::new(AuthServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(ObjectServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(BucketServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(CoordinationServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(IndexServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(GitSourceServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(PersonalDbServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(RegistryServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(StreamServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(RepairServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(TransactionServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(SagaServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(AuditServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(InternalProxyServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(BlockStoreInternalServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(CoreMetaReplicationInternalServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(RootRegisterInternalServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(AntiEntropyInternalServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(CrossRegionProxyInternalServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(HuggingFaceKeyServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(HfIngestionServiceServer::with_interceptor(
        state.clone(),
        auth_closure,
    ))
}

pub fn create_admin_grpc_router(state: AppState, auth_interceptor: AuthInterceptorFn) -> Routes {
    let auth_closure = {
        let f = auth_interceptor.clone();
        move |req| f.call(req)
    };
    tonic::service::Routes::new(AdminServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(MeshControlServiceServer::with_interceptor(
        state,
        auth_closure,
    ))
}

pub fn create_admin_axum_router(
    state: AppState,
    auth_interceptor: AuthInterceptorFn,
) -> axum::Router {
    let grpc_router = create_admin_grpc_router(state.clone(), auth_interceptor);
    let router = create_axum_router(grpc_router);
    #[cfg(feature = "root-publication-test-control")]
    {
        root_publication_test_control::extend_admin_router(router, state)
    }
    #[cfg(not(feature = "root-publication-test-control"))]
    {
        router
    }
}

pub fn create_axum_router(grpc_router: Routes) -> axum::Router {
    grpc_router
        .into_axum_router()
        .route_layer(axum::middleware::from_fn(middleware::request_id_mw))
        .route_layer(axum::middleware::from_fn(middleware::save_uri_mw))
}
