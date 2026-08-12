//! One-hop routing to a single weighted-HRW query replica.

use std::sync::Arc;

use anvil_consensus::{DecisionRaft, NodeId};
use prost::Message;
use tonic::Status;

use crate::cluster_peer::{
    ClusterPeerTransport, LocalIndexQueryExecutor, LocalIndexQueryRequest, RoutedIndexQueryRequest,
};
use crate::cluster_placement::ClusterPlacement;
use crate::index_service::{ExecuteIndexQuery, ExecutedIndexQuery, IndexQueryExecutor};

use super::placement::{IndexIdentity, IndexPlacement};

#[derive(Clone)]
pub(crate) struct DistributedIndexQueryExecutor {
    local_node: NodeId,
    decisions: DecisionRaft,
    peers: ClusterPeerTransport,
    local: Arc<dyn LocalIndexQueryExecutor>,
}

impl DistributedIndexQueryExecutor {
    pub(crate) fn new(
        local_node: NodeId,
        decisions: DecisionRaft,
        peers: ClusterPeerTransport,
        local: Arc<dyn LocalIndexQueryExecutor>,
    ) -> Self {
        Self {
            local_node,
            decisions,
            peers,
            local,
        }
    }

    fn placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    async fn execute_on(
        &self,
        target: NodeId,
        placement: &ClusterPlacement,
        bearer: &str,
        request: RoutedIndexQueryRequest,
        remaining: std::time::Duration,
    ) -> Result<ExecutedIndexQuery, Status> {
        if target == self.local_node {
            self.local
                .execute_local(LocalIndexQueryRequest {
                    storage_tenant: request.storage_tenant,
                    tenant_id: request.tenant_id,
                    bucket_id: request.bucket_id,
                    definition: request.definition,
                    query: request.query,
                    limit: request.limit,
                    resume: request.resume,
                })
                .await
        } else {
            let address = placement
                .address(target)
                .ok_or_else(|| Status::unavailable("index query replica has no peer address"))?;
            self.peers
                .route_index_query(target, &address.0, bearer, request, remaining)
                .await
        }
    }
}

#[tonic::async_trait]
impl IndexQueryExecutor for DistributedIndexQueryExecutor {
    async fn execute(&self, request: ExecuteIndexQuery) -> Result<ExecutedIndexQuery, Status> {
        let placement = self.placement()?;
        let identity = IndexIdentity::new(
            request.tenant_id,
            request.bucket_id,
            request.definition.index_id,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let assignment = IndexPlacement::derive(identity, &placement)
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let replicas = assignment.query_replicas();
        let target = replicas[replica_offset(&request, replicas.len())];
        let bearer = request.context.routed_bearer().to_owned();
        let routed = RoutedIndexQueryRequest {
            storage_tenant: request
                .context
                .caller()
                .storage_tenant()
                .as_str()
                .to_owned(),
            tenant_id: request.tenant_id,
            bucket_id: request.bucket_id,
            definition: request.definition.clone(),
            query: request.query.clone(),
            limit: request.limit,
            resume: request.resume.clone(),
        };
        let result = self
            .execute_on(
                target,
                &placement,
                &bearer,
                routed,
                request.context.remaining()?,
            )
            .await?;
        if self.placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "index query placement changed during execution",
            ));
        }
        Ok(result)
    }
}

fn replica_offset(request: &ExecuteIndexQuery, replica_count: usize) -> usize {
    debug_assert!(replica_count > 0);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.index/query-replica/v1");
    hasher.update(&request.definition.index_id.to_be_bytes());
    hasher.update(request.context.routed_bearer().as_bytes());
    hasher.update(&request.query.encode_to_vec());
    let mut value = [0_u8; 8];
    value.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    (u64::from_be_bytes(value) % replica_count as u64) as usize
}
