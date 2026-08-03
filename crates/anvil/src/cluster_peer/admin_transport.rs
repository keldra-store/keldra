use std::time::Duration;

use anvil_api::v1 as api;
use anvil_consensus::NodeId;
use tonic::{Request, Status};

use super::transport::add_bearer_and_timeout;
use super::{ClusterPeerTransport, wire};

impl ClusterPeerTransport {
    pub(crate) async fn route_admin_create_application(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::CreateApplicationRequest,
        remaining: Duration,
    ) -> Result<api::ApplicationCredential, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteAdminCreateApplicationRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_admin_create_application(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_admin_rotate_credential(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::RotateApplicationCredentialRequest,
        remaining: Duration,
    ) -> Result<api::ApplicationCredential, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteAdminRotateCredentialRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_admin_rotate_credential(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_admin_disable_credential(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::DisableApplicationCredentialRequest,
        remaining: Duration,
    ) -> Result<api::ApplicationCredentialState, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteAdminDisableCredentialRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_admin_disable_credential(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_admin_set_bucket_versioning(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::SetBucketVersioningRequest,
        remaining: Duration,
    ) -> Result<api::SetBucketVersioningResponse, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteAdminSetBucketVersioningRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_admin_set_bucket_versioning(request)
            .await?
            .into_inner())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn route_admin_change_application_role(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: api::ApplicationRoleRequest,
        granted: bool,
        remaining: Duration,
    ) -> Result<api::ApplicationRoleResponse, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteAdminChangeApplicationRoleRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
            granted,
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_admin_change_application_role(request)
            .await?
            .into_inner())
    }
}
