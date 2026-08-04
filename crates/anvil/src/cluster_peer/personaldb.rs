use std::sync::Arc;
use std::time::Duration;

use anvil_api::v1::{
    AppendPersonalDbEntryRequest, ChangePersonalDbGroupRoleRequest, CreatePersonalDbGroupRequest,
    MaterializePersonalDbProjectionRequest, PersonalDbCommit, PersonalDbGroup,
    PersonalDbGroupRoleChange, PersonalDbMaterialization, PersonalDbSnapshot,
    RegisterPersonalDbSnapshotRequest,
};
use anvil_consensus::NodeId;
use tonic::{Request, Response, Status};

use super::transport::add_bearer_and_timeout;
use super::{ClusterPeerService, ClusterPeerTransport, wire};
use crate::distributed_list::OriginalBearer;
use crate::personaldb::{
    ApplyPersonalDbRoleCall, RoutedPersonalDbCall, RoutedPersonalDbRequest,
    RoutedPersonalDbResponse,
};

impl ClusterPeerService {
    pub(super) async fn route_create_personaldb_group_call(
        &self,
        request: Request<wire::RouteCreatePersonalDbGroupRequest>,
    ) -> Result<Response<PersonalDbGroup>, Status> {
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("CreateGroup request is required"))?;
        let response = self
            .execute_personaldb_route(
                &request,
                request.get_ref().peer.as_ref(),
                RoutedPersonalDbRequest::Create(value),
            )
            .await?;
        match response {
            RoutedPersonalDbResponse::Group(value) => Ok(Response::new(value)),
            _ => Err(Status::internal(
                "PersonalDB route returned the wrong response type",
            )),
        }
    }

    pub(super) async fn route_change_personaldb_group_role_call(
        &self,
        request: Request<wire::RouteChangePersonalDbGroupRoleRequest>,
    ) -> Result<Response<PersonalDbGroupRoleChange>, Status> {
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("group role request is required"))?;
        let routed = RoutedPersonalDbRequest::ChangeRole {
            request: value,
            granted: request.get_ref().granted,
        };
        let response = self
            .execute_personaldb_route(&request, request.get_ref().peer.as_ref(), routed)
            .await?;
        match response {
            RoutedPersonalDbResponse::RoleChange(value) => Ok(Response::new(value)),
            _ => Err(Status::internal(
                "PersonalDB route returned the wrong response type",
            )),
        }
    }

    pub(super) async fn route_append_personaldb_entry_call(
        &self,
        request: Request<wire::RouteAppendPersonalDbEntryRequest>,
    ) -> Result<Response<PersonalDbCommit>, Status> {
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("AppendEntry request is required"))?;
        let response = self
            .execute_personaldb_route(
                &request,
                request.get_ref().peer.as_ref(),
                RoutedPersonalDbRequest::Append(value),
            )
            .await?;
        match response {
            RoutedPersonalDbResponse::Commit(value) => Ok(Response::new(value)),
            _ => Err(Status::internal(
                "PersonalDB route returned the wrong response type",
            )),
        }
    }

    pub(super) async fn route_materialize_personaldb_projection_call(
        &self,
        request: Request<wire::RouteMaterializePersonalDbProjectionRequest>,
    ) -> Result<Response<PersonalDbMaterialization>, Status> {
        let value =
            request.get_ref().request.clone().ok_or_else(|| {
                Status::invalid_argument("MaterializeProjection request is required")
            })?;
        let response = self
            .execute_personaldb_route(
                &request,
                request.get_ref().peer.as_ref(),
                RoutedPersonalDbRequest::Materialize(value),
            )
            .await?;
        match response {
            RoutedPersonalDbResponse::Materialization(value) => Ok(Response::new(value)),
            _ => Err(Status::internal(
                "PersonalDB route returned the wrong response type",
            )),
        }
    }

    pub(super) async fn route_register_personaldb_snapshot_call(
        &self,
        request: Request<wire::RouteRegisterPersonalDbSnapshotRequest>,
    ) -> Result<Response<PersonalDbSnapshot>, Status> {
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("RegisterSnapshot request is required"))?;
        let response = self
            .execute_personaldb_route(
                &request,
                request.get_ref().peer.as_ref(),
                RoutedPersonalDbRequest::RegisterSnapshot(value),
            )
            .await?;
        match response {
            RoutedPersonalDbResponse::Snapshot(value) => Ok(Response::new(value)),
            _ => Err(Status::internal(
                "PersonalDB route returned the wrong response type",
            )),
        }
    }

    pub(super) async fn apply_personaldb_role_call(
        &self,
        request: Request<wire::ApplyPersonalDbRoleRequest>,
    ) -> Result<Response<PersonalDbGroupRoleChange>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("group role request is required"))?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.routed_personaldb
                .get()?
                .apply_role(ApplyPersonalDbRoleCall {
                    bearer: Arc::from(bearer.signed_token()),
                    source_node: admitted.authenticated.node_id,
                    placement_fence: fence,
                    tenant_id: request.get_ref().tenant_id,
                    bucket_id: request.get_ref().bucket_id,
                    request: value,
                    granted: request.get_ref().granted,
                    creator_owner: request.get_ref().creator_owner,
                }),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded("PersonalDB role application deadline exceeded")
        })??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    async fn execute_personaldb_route<T>(
        &self,
        request: &Request<T>,
        context: Option<&wire::PeerContext>,
        value: RoutedPersonalDbRequest,
    ) -> Result<RoutedPersonalDbResponse, Status> {
        let admitted = self.admit(request, context, 1)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let fence = admitted.placement.fence();
        let response = tokio::time::timeout(
            admitted.timeout,
            self.routed_personaldb
                .get()?
                .execute(RoutedPersonalDbCall::new(
                    Arc::from(bearer.signed_token()),
                    fence,
                    value,
                )),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed PersonalDB deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(response)
    }
}

impl ClusterPeerTransport {
    pub(crate) async fn route_create_personaldb_group(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: CreatePersonalDbGroupRequest,
        remaining: Duration,
    ) -> Result<PersonalDbGroup, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteCreatePersonalDbGroupRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_create_personal_db_group(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_change_personaldb_group_role(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: ChangePersonalDbGroupRoleRequest,
        granted: bool,
        remaining: Duration,
    ) -> Result<PersonalDbGroupRoleChange, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteChangePersonalDbGroupRoleRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
            granted,
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_change_personal_db_group_role(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_append_personaldb_entry(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: AppendPersonalDbEntryRequest,
        remaining: Duration,
    ) -> Result<PersonalDbCommit, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteAppendPersonalDbEntryRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_append_personal_db_entry(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_materialize_personaldb_projection(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: MaterializePersonalDbProjectionRequest,
        remaining: Duration,
    ) -> Result<PersonalDbMaterialization, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteMaterializePersonalDbProjectionRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_materialize_personal_db_projection(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn route_register_personaldb_snapshot(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        value: RegisterPersonalDbSnapshotRequest,
        remaining: Duration,
    ) -> Result<PersonalDbSnapshot, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::RouteRegisterPersonalDbSnapshotRequest {
            peer: Some(self.context(fence, 1, remaining)?),
            request: Some(value),
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .route_register_personal_db_snapshot(request)
            .await?
            .into_inner())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn apply_personaldb_role(
        &self,
        target: NodeId,
        address: &str,
        bearer: &str,
        tenant_id: u64,
        bucket_id: u64,
        value: ChangePersonalDbGroupRoleRequest,
        granted: bool,
        creator_owner: bool,
        remaining: Duration,
    ) -> Result<PersonalDbGroupRoleChange, Status> {
        let fence = self.placement()?.fence();
        let mut request = Request::new(wire::ApplyPersonalDbRoleRequest {
            peer: Some(self.context(fence, 0, remaining)?),
            tenant_id,
            bucket_id,
            request: Some(value),
            granted,
            creator_owner,
        });
        add_bearer_and_timeout(&mut request, bearer, remaining)?;
        Ok(self
            .client(target, address)?
            .apply_personal_db_role(request)
            .await?
            .into_inner())
    }
}
