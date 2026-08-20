use std::sync::{Arc, OnceLock};

use keldra_api::v1::{
    AccountingDefinition, AccountingSnapshot, DisableAccountingRequest, DisableAccountingResponse,
    EnableAccountingRequest, GetAccountingRequest,
};
use keldra_consensus::NodeId;
use keldra_store::ObjectKey;
use prost::Message;
use tonic::{Request, Response, Status};

use super::{CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, RoutedCall, wire};
use crate::cluster_placement::ClusterPlacement;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccountingTrafficFlush {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) accounting_id: u64,
    pub(crate) source_node: NodeId,
    pub(crate) accepted_inbound_bytes: u64,
    pub(crate) served_outbound_bytes: u64,
    pub(crate) flush_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccountingTrafficEntry {
    pub(crate) exact_path: String,
    pub(crate) accepted_inbound_bytes: u64,
    pub(crate) served_outbound_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccountingTrafficBatch {
    pub(crate) source_node: NodeId,
    pub(crate) source_epoch: [u8; 32],
    pub(crate) sequence: u64,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) entries: Vec<AccountingTrafficEntry>,
}

pub(crate) const MAX_ACCOUNTING_TRAFFIC_ENTRIES: usize = 256;
pub(crate) const MAX_ACCOUNTING_TRAFFIC_LOGICAL_BYTES: u64 = 512 * 1024;
// A valid PeerContext is at most 71 encoded bytes. Each repeated entry needs at
// most 12 protobuf framing/varint bytes beyond its logical path + two u64s.
// Keep small explicit cushions so the wire envelope remains mechanically tied
// to the shared logical-entry limit rather than becoming a second batch limit.
const MAX_ACCOUNTING_TRAFFIC_CONTEXT_AND_FIXED_BYTES: usize = 256;
const MAX_ACCOUNTING_TRAFFIC_ENTRY_WIRE_OVERHEAD: usize = 16;
const MAX_ACCOUNTING_TRAFFIC_WIRE_BYTES: usize = MAX_ACCOUNTING_TRAFFIC_LOGICAL_BYTES as usize
    + MAX_ACCOUNTING_TRAFFIC_CONTEXT_AND_FIXED_BYTES
    + MAX_ACCOUNTING_TRAFFIC_ENTRIES * MAX_ACCOUNTING_TRAFFIC_ENTRY_WIRE_OVERHEAD;

pub(crate) fn accounting_traffic_entry_logical_bytes(path: &str) -> u64 {
    (path.len() as u64).saturating_add(16)
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

    async fn match_traffic(
        &self,
        source: NodeId,
        placement: ClusterPlacement,
        value: AccountingTrafficBatch,
    ) -> Result<(), Status>;

    async fn invalidate_matcher_bucket(
        &self,
        source: NodeId,
        placement: ClusterPlacement,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<(), Status>;

    async fn clear_matcher_cache(
        &self,
        source: NodeId,
        placement: ClusterPlacement,
    ) -> Result<(), Status>;
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
        if raw.tenant_id == 0
            || raw.bucket_id == 0
            || raw.accounting_id == 0
            || raw.source_node_id == 0
            || raw.flush_id.is_empty()
            || raw.flush_id.len() > 256
        {
            return Err(Status::invalid_argument(
                "accounting traffic flush identity is invalid",
            ));
        }
        let fence = admitted.placement.fence();
        let value = AccountingTrafficFlush {
            tenant_id: raw.tenant_id,
            bucket_id: raw.bucket_id,
            accounting_id: raw.accounting_id,
            source_node: NodeId(raw.source_node_id),
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

    pub(super) async fn match_accounting_traffic_call(
        &self,
        request: Request<wire::MatchAccountingTrafficRequest>,
    ) -> Result<Response<wire::MatchAccountingTrafficResponse>, Status> {
        require_accounting_traffic_size(request.get_ref())?;
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let raw = request.get_ref();
        if raw.source_node_id != admitted.authenticated.node_id.0 {
            return Err(Status::permission_denied(
                "accounting traffic source differs from the authenticated peer",
            ));
        }
        if raw.source_node_id == 0
            || raw.source_epoch.len() != 32
            || raw.source_epoch.iter().all(|byte| *byte == 0)
            || raw.sequence == 0
            || raw.tenant_id == 0
            || raw.bucket_id == 0
            || raw.entries.is_empty()
            || raw.entries.len() > MAX_ACCOUNTING_TRAFFIC_ENTRIES
        {
            return Err(Status::invalid_argument(
                "accounting traffic batch identity or entry count is invalid",
            ));
        }
        let source_epoch: [u8; 32] = raw
            .source_epoch
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("accounting source epoch is invalid"))?;
        let entries = raw
            .entries
            .iter()
            .map(|entry| {
                ObjectKey::new("typed", "accounting", &entry.exact_path)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
                if entry.accepted_inbound_bytes == 0 && entry.served_outbound_bytes == 0 {
                    return Err(Status::invalid_argument(
                        "accounting traffic entry has no bytes",
                    ));
                }
                Ok(AccountingTrafficEntry {
                    exact_path: entry.exact_path.clone(),
                    accepted_inbound_bytes: entry.accepted_inbound_bytes,
                    served_outbound_bytes: entry.served_outbound_bytes,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let fence = admitted.placement.fence();
        let source = admitted.authenticated.node_id;
        tokio::time::timeout(
            admitted.timeout,
            self.routed_accounting.get()?.match_traffic(
                source,
                admitted.placement,
                AccountingTrafficBatch {
                    source_node: source,
                    source_epoch,
                    sequence: raw.sequence,
                    tenant_id: raw.tenant_id,
                    bucket_id: raw.bucket_id,
                    entries,
                },
            ),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("accounting traffic match timed out"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(wire::MatchAccountingTrafficResponse {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        }))
    }

    pub(super) async fn invalidate_accounting_matcher_bucket_call(
        &self,
        request: Request<wire::InvalidateAccountingMatcherBucketRequest>,
    ) -> Result<Response<wire::AccountingMatcherCacheInvalidated>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let raw = request.get_ref();
        if raw.tenant_id == 0 || raw.bucket_id == 0 {
            return Err(Status::invalid_argument(
                "accounting matcher invalidation identity is invalid",
            ));
        }
        let fence = admitted.placement.fence();
        tokio::time::timeout(
            admitted.timeout,
            self.routed_accounting.get()?.invalidate_matcher_bucket(
                admitted.authenticated.node_id,
                admitted.placement,
                raw.tenant_id,
                raw.bucket_id,
            ),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("accounting matcher invalidation timed out"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(wire::AccountingMatcherCacheInvalidated {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        }))
    }

    pub(super) async fn clear_accounting_matcher_cache_call(
        &self,
        request: Request<wire::ClearAccountingMatcherCacheRequest>,
    ) -> Result<Response<wire::AccountingMatcherCacheInvalidated>, Status> {
        let admitted = self.admit(&request, request.get_ref().peer.as_ref(), 0)?;
        let fence = admitted.placement.fence();
        tokio::time::timeout(
            admitted.timeout,
            self.routed_accounting
                .get()?
                .clear_matcher_cache(admitted.authenticated.node_id, admitted.placement),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("accounting matcher cache clear timed out"))??;
        self.require_unchanged(fence)?;
        Ok(Response::new(wire::AccountingMatcherCacheInvalidated {
            schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        }))
    }
}

fn require_accounting_traffic_size(
    value: &wire::MatchAccountingTrafficRequest,
) -> Result<(), Status> {
    if value.encoded_len() > MAX_ACCOUNTING_TRAFFIC_WIRE_BYTES {
        return Err(Status::resource_exhausted(
            "accounting traffic batch exceeds the private peer wire limit",
        ));
    }
    let logical_bytes = value.entries.iter().try_fold(0_u64, |total, entry| {
        total
            .checked_add(accounting_traffic_entry_logical_bytes(&entry.exact_path))
            .ok_or_else(|| Status::resource_exhausted("accounting traffic batch size overflow"))
    })?;
    if logical_bytes > MAX_ACCOUNTING_TRAFFIC_LOGICAL_BYTES {
        return Err(Status::resource_exhausted(
            "accounting traffic batch exceeds the logical entry limit",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn near_limit_request(path_bytes: usize) -> wire::MatchAccountingTrafficRequest {
        wire::MatchAccountingTrafficRequest {
            peer: Some(wire::PeerContext {
                schema_version: CLUSTER_PEER_SCHEMA_VERSION,
                cluster_id: [9_u8; 16].to_vec(),
                source_node_id: u64::MAX,
                placement_term: u64::MAX,
                placement_index: u64::MAX,
                hop_count: u32::MAX,
                remaining_deadline_millis: u32::MAX,
            }),
            source_node_id: u64::MAX,
            source_epoch: [7_u8; 32].to_vec(),
            sequence: u64::MAX,
            tenant_id: u64::MAX,
            bucket_id: u64::MAX,
            entries: (0..128)
                .map(|_| wire::AccountingTrafficEntry {
                    exact_path: "p".repeat(path_bytes),
                    accepted_inbound_bytes: u64::MAX,
                    served_outbound_bytes: u64::MAX,
                })
                .collect(),
        }
    }

    #[test]
    fn near_limit_remote_request_fits_the_derived_wire_envelope() {
        // 128 * (4,080 path bytes + two logical u64s) is exactly 512 KiB.
        let request = near_limit_request(4_080);
        assert_eq!(
            request
                .entries
                .iter()
                .map(|entry| accounting_traffic_entry_logical_bytes(&entry.exact_path))
                .sum::<u64>(),
            MAX_ACCOUNTING_TRAFFIC_LOGICAL_BYTES
        );
        assert!(request.encoded_len() > MAX_ACCOUNTING_TRAFFIC_LOGICAL_BYTES as usize);
        assert!(request.encoded_len() <= MAX_ACCOUNTING_TRAFFIC_WIRE_BYTES);
        require_accounting_traffic_size(&request).unwrap();
    }

    #[test]
    fn remote_request_over_the_shared_logical_limit_is_rejected() {
        let error = require_accounting_traffic_size(&near_limit_request(4_081)).unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(error.message().contains("logical entry limit"));
    }
}
