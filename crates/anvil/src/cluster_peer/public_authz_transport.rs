//! One-hop typed public authorization routing.

use std::time::Duration;

use anvil_api::v1 as api;
use anvil_consensus::NodeId;
use tonic::Request;
use tonic::Status;
use tonic::metadata::MetadataValue;

use super::{ClusterPeerTransport, MAX_CLUSTER_OPERATION_TIME, wire};

impl ClusterPeerTransport {
    pub(crate) async fn route_authz_put_schema(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::PutSchemaRequest,
        remaining: Duration,
    ) -> Result<api::PutSchemaResponse, Status> {
        let request = self.authz_request(
            wire::RouteAuthzPutSchemaRequest {
                peer: Some(self.context(self.placement()?.fence(), 1, remaining)?),
                request: Some(value),
            },
            bearer,
            remaining,
        )?;
        Ok(self
            .client(target, address)?
            .route_authz_put_schema(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_authz_bind_schema(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::BindSchemaRequest,
        remaining: Duration,
    ) -> Result<api::BindSchemaResponse, Status> {
        let request = self.authz_request(
            wire::RouteAuthzBindSchemaRequest {
                peer: Some(self.context(self.placement()?.fence(), 1, remaining)?),
                request: Some(value),
            },
            bearer,
            remaining,
        )?;
        Ok(self
            .client(target, address)?
            .route_authz_bind_schema(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_authz_get_binding(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::GetBindingRequest,
        remaining: Duration,
    ) -> Result<api::GetBindingResponse, Status> {
        let request = self.authz_request(
            wire::RouteAuthzGetBindingRequest {
                peer: Some(self.context(self.placement()?.fence(), 1, remaining)?),
                request: Some(value),
            },
            bearer,
            remaining,
        )?;
        Ok(self
            .client(target, address)?
            .route_authz_get_binding(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_authz_get_schema(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::GetSchemaRequest,
        remaining: Duration,
    ) -> Result<api::GetSchemaResponse, Status> {
        let request = self.authz_request(
            wire::RouteAuthzGetSchemaRequest {
                peer: Some(self.context(self.placement()?.fence(), 1, remaining)?),
                request: Some(value),
            },
            bearer,
            remaining,
        )?;
        Ok(self
            .client(target, address)?
            .route_authz_get_schema(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_authz_mutate_tuples(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::MutateTuplesRequest,
        remaining: Duration,
    ) -> Result<api::MutateTuplesResponse, Status> {
        let request = self.authz_request(
            wire::RouteAuthzMutateTuplesRequest {
                peer: Some(self.context(self.placement()?.fence(), 1, remaining)?),
                request: Some(value),
            },
            bearer,
            remaining,
        )?;
        Ok(self
            .client(target, address)?
            .route_authz_mutate_tuples(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_authz_read_tuples(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::ReadTuplesRequest,
        remaining: Duration,
    ) -> Result<api::ReadTuplesResponse, Status> {
        let request = self.authz_request(
            wire::RouteAuthzReadTuplesRequest {
                peer: Some(self.context(self.placement()?.fence(), 1, remaining)?),
                request: Some(value),
            },
            bearer,
            remaining,
        )?;
        Ok(self
            .client(target, address)?
            .route_authz_read_tuples(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_authz_check_permission(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::CheckPermissionRequest,
        remaining: Duration,
    ) -> Result<api::CheckPermissionResponse, Status> {
        let request = self.authz_request(
            wire::RouteAuthzCheckPermissionRequest {
                peer: Some(self.context(self.placement()?.fence(), 1, remaining)?),
                request: Some(value),
            },
            bearer,
            remaining,
        )?;
        Ok(self
            .client(target, address)?
            .route_authz_check_permission(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_authz_check_permissions(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::CheckPermissionsRequest,
        remaining: Duration,
    ) -> Result<api::CheckPermissionsResponse, Status> {
        let request = self.authz_request(
            wire::RouteAuthzCheckPermissionsRequest {
                peer: Some(self.context(self.placement()?.fence(), 1, remaining)?),
                request: Some(value),
            },
            bearer,
            remaining,
        )?;
        Ok(self
            .client(target, address)?
            .route_authz_check_permissions(request)
            .await?
            .into_inner())
    }

    fn authz_request<T>(
        &self,
        value: T,
        bearer: &str,
        remaining: Duration,
    ) -> Result<Request<T>, Status> {
        let mut request = Request::new(value);
        let bearer = MetadataValue::try_from(format!("Bearer {bearer}")).map_err(|_| {
            Status::invalid_argument("bearer token cannot be represented as metadata")
        })?;
        request.metadata_mut().insert("authorization", bearer);
        request.set_timeout(remaining.min(MAX_CLUSTER_OPERATION_TIME));
        Ok(request)
    }
}
