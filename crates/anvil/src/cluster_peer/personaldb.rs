use std::sync::{Arc, OnceLock};

use anvil_api::v1::{
    PersonalDbExchangeRequest, PersonalDbExchangeResponse, PersonalDbGrantLeaderLeaseRequest,
    PersonalDbGrantLeaderLeaseResponse, PersonalDbRenewLeaderLeaseRequest,
    PersonalDbRenewLeaderLeaseResponse, PersonalDbWitnessCommitRequest,
    PersonalDbWitnessCommitResponse,
};
use tonic::{Request, Response, Status};

use super::routing::RoutedCall;
use super::{ClusterPeerService, wire};

#[tonic::async_trait]
pub(crate) trait RoutedPersonalDbHandler: Send + Sync + 'static {
    async fn exchange(
        &self,
        call: RoutedCall<PersonalDbExchangeRequest>,
    ) -> Result<PersonalDbExchangeResponse, Status>;

    async fn grant_leader_lease(
        &self,
        call: RoutedCall<PersonalDbGrantLeaderLeaseRequest>,
    ) -> Result<PersonalDbGrantLeaderLeaseResponse, Status>;

    async fn renew_leader_lease(
        &self,
        call: RoutedCall<PersonalDbRenewLeaderLeaseRequest>,
    ) -> Result<PersonalDbRenewLeaderLeaseResponse, Status>;

    async fn witness_commit(
        &self,
        call: RoutedCall<PersonalDbWitnessCommitRequest>,
    ) -> Result<PersonalDbWitnessCommitResponse, Status>;
}

#[derive(Clone, Default)]
pub(crate) struct RoutedPersonalDbHandlers {
    inner: Arc<OnceLock<Arc<dyn RoutedPersonalDbHandler>>>,
}

impl RoutedPersonalDbHandlers {
    pub(crate) fn install(
        &self,
        handler: Arc<dyn RoutedPersonalDbHandler>,
    ) -> Result<(), Arc<dyn RoutedPersonalDbHandler>> {
        self.inner.set(handler)
    }

    fn get(&self) -> Result<Arc<dyn RoutedPersonalDbHandler>, Status> {
        self.inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("routed PersonalDB handler is not ready"))
    }
}

impl ClusterPeerService {
    pub(super) async fn route_personaldb_exchange_call(
        &self,
        request: Request<wire::RoutePersonalDbExchangeRequest>,
    ) -> Result<Response<PersonalDbExchangeResponse>, Status> {
        let value =
            request.get_ref().request.clone().ok_or_else(|| {
                Status::invalid_argument("PersonalDB exchange request is required")
            })?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let fence = call.placement_fence();
        let response = tokio::time::timeout(timeout, self.routed_personaldb.get()?.exchange(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed PersonalDB deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_personaldb_grant_leader_lease_call(
        &self,
        request: Request<wire::RoutePersonalDbGrantLeaderLeaseRequest>,
    ) -> Result<Response<PersonalDbGrantLeaderLeaseResponse>, Status> {
        let value = request.get_ref().request.clone().ok_or_else(|| {
            Status::invalid_argument("PersonalDB grant-leader-lease request is required")
        })?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let fence = call.placement_fence();
        let response = tokio::time::timeout(
            timeout,
            self.routed_personaldb.get()?.grant_leader_lease(call),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed PersonalDB deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_personaldb_renew_leader_lease_call(
        &self,
        request: Request<wire::RoutePersonalDbRenewLeaderLeaseRequest>,
    ) -> Result<Response<PersonalDbRenewLeaderLeaseResponse>, Status> {
        let value = request.get_ref().request.clone().ok_or_else(|| {
            Status::invalid_argument("PersonalDB renew-leader-lease request is required")
        })?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let fence = call.placement_fence();
        let response = tokio::time::timeout(
            timeout,
            self.routed_personaldb.get()?.renew_leader_lease(call),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("routed PersonalDB deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_personaldb_witness_commit_call(
        &self,
        request: Request<wire::RoutePersonalDbWitnessCommitRequest>,
    ) -> Result<Response<PersonalDbWitnessCommitResponse>, Status> {
        let value = request.get_ref().request.clone().ok_or_else(|| {
            Status::invalid_argument("PersonalDB witness-commit request is required")
        })?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let fence = call.placement_fence();
        let response =
            tokio::time::timeout(timeout, self.routed_personaldb.get()?.witness_commit(call))
                .await
                .map_err(|_| Status::deadline_exceeded("routed PersonalDB deadline exceeded"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }
}
