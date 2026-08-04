use std::sync::{Arc, OnceLock};

use anvil_api::v1::{
    AccountingDefinition, AccountingSnapshot, DisableAccountingRequest, DisableAccountingResponse,
    EnableAccountingRequest, GetAccountingRequest,
};
use anvil_consensus::NodeId;
use tonic::{Request, Response, Status};

use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, RoutedCall, wire};
use crate::cluster_placement::ClusterPlacement;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccountingTrafficFlush {
    pub(crate) accounting_id: u64,
    pub(crate) source_node: NodeId,
    pub(crate) accepted_inbound_bytes: u64,
    pub(crate) served_outbound_bytes: u64,
    pub(crate) flush_id: String,
}

#[tonic::async_trait]
pub(crate) trait RoutedAccountingHandler: Send + Sync + 'static {
    async fn enable(
        &self,
        call: RoutedCall<EnableAccountingRequest>,
    ) -> Result<AccountingDefinition, Status>;

    async fn disable(
        &self,
        call: RoutedCall<DisableAccountingRequest>,
    ) -> Result<DisableAccountingResponse, Status>;

    async fn get(
        &self,
        call: RoutedCall<GetAccountingRequest>,
    ) -> Result<AccountingSnapshot, Status>;

    async fn flush(
        &self,
        source: NodeId,
        placement: ClusterPlacement,
        value: AccountingTrafficFlush,
    ) -> Result<bool, Status>;
}

#[derive(Clone, Default)]
pub(crate) struct RoutedAccountingHandlers {
    inner: Arc<OnceLock<Arc<dyn RoutedAccountingHandler>>>,
}

impl RoutedAccountingHandlers {
    pub(crate) fn install(
        &self,
        handler: Arc<dyn RoutedAccountingHandler>,
    ) -> Result<(), Arc<dyn RoutedAccountingHandler>> {
        self.inner.set(handler)
    }

    fn get(&self) -> Result<Arc<dyn RoutedAccountingHandler>, Status> {
        self.inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("routed accounting handler is not ready"))
    }
}

impl ClusterPeerService {
    pub(super) async fn route_enable_accounting_call(
        &self,
        request: Request<wire::RouteEnableAccountingRequest>,
    ) -> Result<Response<AccountingDefinition>, Status> {
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("EnableAccounting request is required"))?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let fence = call.placement_fence();
        let response = tokio::time::timeout(timeout, self.routed_accounting.get()?.enable(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed accounting enable timed out"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_disable_accounting_call(
        &self,
        request: Request<wire::RouteDisableAccountingRequest>,
    ) -> Result<Response<DisableAccountingResponse>, Status> {
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("DisableAccounting request is required"))?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let fence = call.placement_fence();
        let response = tokio::time::timeout(timeout, self.routed_accounting.get()?.disable(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed accounting disable timed out"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn route_get_accounting_call(
        &self,
        request: Request<wire::RouteGetAccountingRequest>,
    ) -> Result<Response<AccountingSnapshot>, Status> {
        let value = request
            .get_ref()
            .request
            .clone()
            .ok_or_else(|| Status::invalid_argument("GetAccounting request is required"))?;
        let (call, timeout) = self.routed_call(&request, request.get_ref().peer.as_ref(), value)?;
        let fence = call.placement_fence();
        let response = tokio::time::timeout(timeout, self.routed_accounting.get()?.get(call))
            .await
            .map_err(|_| Status::deadline_exceeded("routed accounting query timed out"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(response))
    }

    pub(super) async fn flush_accounting_traffic_call(
        &self,
        request: Request<wire::FlushAccountingTrafficRequest>,
    ) -> Result<Response<wire::FlushAccountingTrafficResponse>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let raw = request.get_ref();
        if raw.accounting_id == 0
            || raw.source_node_id == 0
            || raw.flush_id.is_empty()
            || raw.flush_id.len() > 256
        {
            return Err(Status::invalid_argument(
                "accounting traffic flush identity is invalid",
            ));
        }
        if raw.source_node_id != admitted.authenticated.node_id.0 {
            return Err(Status::permission_denied(
                "accounting traffic source differs from the authenticated peer",
            ));
        }
        let fence = admitted.placement.fence();
        let value = AccountingTrafficFlush {
            accounting_id: raw.accounting_id,
            source_node: admitted.authenticated.node_id,
            accepted_inbound_bytes: raw.accepted_inbound_bytes,
            served_outbound_bytes: raw.served_outbound_bytes,
            flush_id: raw.flush_id.clone(),
        };
        let replayed = tokio::time::timeout(
            admitted.timeout,
            self.routed_accounting.get()?.flush(
                admitted.authenticated.node_id,
                admitted.placement,
                value,
            ),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("accounting traffic flush timed out"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(wire::FlushAccountingTrafficResponse {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
            replayed,
        }))
    }
}
