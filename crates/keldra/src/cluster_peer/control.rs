use std::sync::{Arc, OnceLock};

use keldra_store::{LogicalRecordId, LogicalRecordValue, PlacementLogId, TupleBatchRequest};
use tonic::{Request, Response, Status};

use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, decode_json, encode_json, wire};
use crate::distributed_control_plane::DistributedControlPlane;
use crate::distributed_list::OriginalBearer;

/// Fail-closed binding installed only after join and serving-fence recovery.
#[derive(Clone, Default)]
pub(crate) struct LateBoundDistributedControl {
    inner: Arc<OnceLock<Arc<DistributedControlPlane>>>,
}

impl LateBoundDistributedControl {
    pub(crate) fn install(
        &self,
        control: Arc<DistributedControlPlane>,
    ) -> Result<(), Arc<DistributedControlPlane>> {
        self.inner.set(control)
    }

    fn get(&self) -> Result<Arc<DistributedControlPlane>, Status> {
        self.inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("distributed control plane is not ready"))
    }
}

impl ClusterPeerService {
    pub(super) async fn route_provision_tenant_call(
        &self,
        request: Request<wire::RouteProvisionTenantRequest>,
    ) -> Result<Response<keldra_api::v1::ProvisionTenantResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 1)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("ProvisionTenant request is required"))?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .execute_routed_provision_tenant(bearer.signed_token(), value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed ProvisionTenant deadline exceeded"))??;
        self.require_unchanged_control(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_create_bucket_call(
        &self,
        request: Request<wire::RouteCreateBucketRequest>,
    ) -> Result<Response<keldra_api::v1::CreateBucketResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 1)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("CreateBucket request is required"))?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .execute_routed_create_bucket(bearer.signed_token(), value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed CreateBucket deadline exceeded"))??;
        self.require_unchanged_control(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_credential_exchange_call(
        &self,
        request: Request<wire::RouteCredentialExchangeRequest>,
    ) -> Result<Response<keldra_api::v1::AccessToken>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 1)?;
        let value =
            request.get_ref().request.clone().ok_or_else(|| {
                Status::invalid_argument("credential exchange request is required")
            })?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .execute_routed_credential_exchange(value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed credential deadline exceeded"))??;
        self.require_unchanged_control(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_admin_create_application_call(
        &self,
        request: Request<wire::RouteAdminCreateApplicationRequest>,
    ) -> Result<Response<keldra_api::v1::ApplicationCredential>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 1)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("CreateApplication request is required"))?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .execute_routed_create_application(bearer.signed_token(), value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed CreateApplication deadline exceeded"))??;
        self.require_unchanged_control(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_admin_rotate_credential_call(
        &self,
        request: Request<wire::RouteAdminRotateCredentialRequest>,
    ) -> Result<Response<keldra_api::v1::ApplicationCredential>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 1)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value =
            request.get_ref().request.clone().ok_or_else(|| {
                Status::invalid_argument("credential rotation request is required")
            })?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .execute_routed_rotate_credential(bearer.signed_token(), value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed credential rotation deadline exceeded"))??;
        self.require_unchanged_control(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_admin_recover_credential_call(
        &self,
        request: Request<wire::RouteAdminRecoverCredentialRequest>,
    ) -> Result<Response<keldra_api::v1::ApplicationCredential>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 1)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value =
            request.get_ref().request.clone().ok_or_else(|| {
                Status::invalid_argument("credential recovery request is required")
            })?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .execute_routed_recover_credential(bearer.signed_token(), value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed credential recovery deadline exceeded"))??;
        self.require_unchanged_control(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_admin_disable_credential_call(
        &self,
        request: Request<wire::RouteAdminDisableCredentialRequest>,
    ) -> Result<Response<keldra_api::v1::ApplicationCredentialState>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 1)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value =
            request.get_ref().request.clone().ok_or_else(|| {
                Status::invalid_argument("credential disable request is required")
            })?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .execute_routed_disable_credential(bearer.signed_token(), value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed credential disable deadline exceeded"))??;
        self.require_unchanged_control(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_admin_set_bucket_versioning_call(
        &self,
        request: Request<wire::RouteAdminSetBucketVersioningRequest>,
    ) -> Result<Response<keldra_api::v1::SetBucketVersioningResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 1)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value =
            request.get_ref().request.clone().ok_or_else(|| {
                Status::invalid_argument("SetBucketVersioning request is required")
            })?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .execute_routed_set_bucket_versioning(bearer.signed_token(), value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed SetBucketVersioning deadline exceeded"))??;
        self.require_unchanged_control(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_admin_set_bucket_public_read_call(
        &self,
        request: Request<wire::RouteAdminSetBucketPublicReadRequest>,
    ) -> Result<Response<keldra_api::v1::SetBucketPublicReadResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 1)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value =
            request.get_ref().request.clone().ok_or_else(|| {
                Status::invalid_argument("SetBucketPublicRead request is required")
            })?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .execute_routed_set_bucket_public_read(bearer.signed_token(), value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed SetBucketPublicRead deadline exceeded"))??;
        self.require_unchanged_control(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_admin_change_application_role_call(
        &self,
        request: Request<wire::RouteAdminChangeApplicationRoleRequest>,
    ) -> Result<Response<keldra_api::v1::ApplicationRoleResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 1)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("application role request is required"))?;
        let granted = request.get_ref().granted;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .execute_routed_change_application_role(bearer.signed_token(), value, granted),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed application role deadline exceeded"))??;
        self.require_unchanged_control(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn coordinate_logical_record_call(
        &self,
        request: Request<wire::CoordinateLogicalRecordRequest>,
    ) -> Result<Response<wire::CoordinateLogicalRecordResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        self.require_executor_source(admitted.authenticated.node_id)?;
        let value: LogicalRecordValue = decode_json(&request.get_ref().typed_value_json)?;
        tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .coordinate_logical_record(value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("logical coordination deadline exceeded"))??;
        self.require_unchanged_control(admitted.placement.fence())?;
        Ok(Response::new(wire::CoordinateLogicalRecordResponse {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        }))
    }

    pub(super) async fn read_coordinated_logical_record_call(
        &self,
        request: Request<wire::CoordinatedLogicalReadRequest>,
    ) -> Result<Response<wire::CoordinatedLogicalReadResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let id: LogicalRecordId = decode_json(&request.get_ref().id_json)?;
        let value = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control.get()?.read_logical_record(id),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("logical read deadline exceeded"))??;
        self.require_unchanged_control(admitted.placement.fence())?;
        Ok(Response::new(wire::CoordinatedLogicalReadResponse {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            present: value.is_some(),
            typed_value_json: value
                .as_ref()
                .map(encode_json)
                .transpose()?
                .unwrap_or_default(),
        }))
    }

    pub(super) async fn coordinate_system_grant_call(
        &self,
        request: Request<wire::CoordinateSystemGrantRequest>,
    ) -> Result<Response<wire::CoordinateSystemGrantResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        self.require_executor_source(admitted.authenticated.node_id)?;
        let value: TupleBatchRequest = decode_json(&request.get_ref().tuple_batch_json)?;
        let receipt = tokio::time::timeout(
            admitted.timeout,
            self.distributed_control
                .get()?
                .coordinate_system_grant(value),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("system grant deadline exceeded"))??;
        self.require_unchanged_control(admitted.placement.fence())?;
        Ok(Response::new(wire::CoordinateSystemGrantResponse {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            receipt_json: encode_json(&receipt)?,
        }))
    }

    fn require_executor_source(&self, source: keldra_consensus::NodeId) -> Result<(), Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("atomic executor state is unavailable"))?;
        if state
            .executor()
            .is_some_and(|nomination| nomination.executor == source)
        {
            Ok(())
        } else {
            Err(Status::permission_denied(
                "administration coordination source is not the nominated executor",
            ))
        }
    }

    fn require_unchanged_control(&self, expected: PlacementLogId) -> Result<(), Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        let placement = crate::cluster_placement::ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if placement.fence() == expected {
            Ok(())
        } else {
            Err(Status::unavailable(
                "active placement changed during control-plane operation",
            ))
        }
    }
}
