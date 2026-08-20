//! Destination binding for one-hop public authorization calls.

use std::sync::{Arc, OnceLock};

use keldra_api::v1 as api;
use tonic::{Request, Response, Status};

use super::{ClusterPeerService, RoutedCall, wire};

#[tonic::async_trait]
pub(crate) trait RoutedAuthzHandler: Send + Sync + 'static {
    async fn put_schema(
        &self,
        call: RoutedCall<api::PutSchemaRequest>,
    ) -> Result<api::PutSchemaResponse, Status>;
    async fn bind_schema(
        &self,
        call: RoutedCall<api::BindSchemaRequest>,
    ) -> Result<api::BindSchemaResponse, Status>;
    async fn get_binding(
        &self,
        call: RoutedCall<api::GetBindingRequest>,
    ) -> Result<api::GetBindingResponse, Status>;
    async fn get_schema(
        &self,
        call: RoutedCall<api::GetSchemaRequest>,
    ) -> Result<api::GetSchemaResponse, Status>;
    async fn mutate_tuples(
        &self,
        call: RoutedCall<api::MutateTuplesRequest>,
    ) -> Result<api::MutateTuplesResponse, Status>;
    async fn read_tuples(
        &self,
        call: RoutedCall<api::ReadTuplesRequest>,
    ) -> Result<api::ReadTuplesResponse, Status>;
    async fn check_permission(
        &self,
        call: RoutedCall<api::CheckPermissionRequest>,
    ) -> Result<api::CheckPermissionResponse, Status>;
    async fn check_permissions(
        &self,
        call: RoutedCall<api::CheckPermissionsRequest>,
    ) -> Result<api::CheckPermissionsResponse, Status>;
}

#[derive(Clone, Default)]
pub(crate) struct RoutedAuthzHandlers {
    inner: Arc<OnceLock<Arc<dyn RoutedAuthzHandler>>>,
}

impl RoutedAuthzHandlers {
    pub(crate) fn install(
        &self,
        handler: Arc<dyn RoutedAuthzHandler>,
    ) -> Result<(), Arc<dyn RoutedAuthzHandler>> {
        self.inner.set(handler)
    }

    fn get(&self) -> Result<Arc<dyn RoutedAuthzHandler>, Status> {
        self.inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("routed authorization handler is not ready"))
    }
}

macro_rules! routed_authz_call {
    ($name:ident, $wire:ty, $response:ty, $method:ident, $label:literal) => {
        pub(super) async fn $name(
            &self,
            request: Request<$wire>,
        ) -> Result<Response<$response>, Status> {
            let (call, timeout) = self.routed_call(
                &request,
                request.get_ref().peer.as_ref(),
                request.get_ref().request.clone().ok_or_else(|| {
                    Status::invalid_argument(concat!($label, " request is required"))
                })?,
            )?;
            let fence = call.placement_fence();
            let response = tokio::time::timeout(timeout, self.routed_authz.get()?.$method(call))
                .await
                .map_err(|_| {
                    Status::deadline_exceeded(concat!("routed ", $label, " deadline exceeded"))
                })??;
            self.require_unchanged(fence)?;
            Ok(Response::new(response))
        }
    };
}

impl ClusterPeerService {
    routed_authz_call!(
        route_authz_put_schema_call,
        wire::RouteAuthzPutSchemaRequest,
        api::PutSchemaResponse,
        put_schema,
        "PutSchema"
    );
    routed_authz_call!(
        route_authz_bind_schema_call,
        wire::RouteAuthzBindSchemaRequest,
        api::BindSchemaResponse,
        bind_schema,
        "BindSchema"
    );
    routed_authz_call!(
        route_authz_get_binding_call,
        wire::RouteAuthzGetBindingRequest,
        api::GetBindingResponse,
        get_binding,
        "GetBinding"
    );
    routed_authz_call!(
        route_authz_get_schema_call,
        wire::RouteAuthzGetSchemaRequest,
        api::GetSchemaResponse,
        get_schema,
        "GetSchema"
    );
    routed_authz_call!(
        route_authz_mutate_tuples_call,
        wire::RouteAuthzMutateTuplesRequest,
        api::MutateTuplesResponse,
        mutate_tuples,
        "MutateTuples"
    );
    routed_authz_call!(
        route_authz_read_tuples_call,
        wire::RouteAuthzReadTuplesRequest,
        api::ReadTuplesResponse,
        read_tuples,
        "ReadTuples"
    );
    routed_authz_call!(
        route_authz_check_permission_call,
        wire::RouteAuthzCheckPermissionRequest,
        api::CheckPermissionResponse,
        check_permission,
        "CheckPermission"
    );
    routed_authz_call!(
        route_authz_check_permissions_call,
        wire::RouteAuthzCheckPermissionsRequest,
        api::CheckPermissionsResponse,
        check_permissions,
        "CheckPermissions"
    );
}
