//! One-hop routing with deterministic failover across weighted-HRW query replicas.

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
        local_visibility: Arc<dyn crate::index_service::IndexCandidateVisibility>,
        local_authorization_revision: u64,
    ) -> Result<ExecutedIndexQuery, Status> {
        if target == self.local_node {
            let result = self
                .local
                .execute_local(LocalIndexQueryRequest {
                    storage_tenant: request.storage_tenant,
                    tenant_id: request.tenant_id,
                    bucket_id: request.bucket_id,
                    definition: request.definition,
                    query: request.query,
                    limit: request.limit,
                    resume: request.resume,
                    candidate_visibility: local_visibility,
                    authorization_revision: local_authorization_revision,
                })
                .await?;
            require_local_authorization_revision(&result, local_authorization_revision)?;
            Ok(result)
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
        let first_replica = replica_offset(&request, replicas.len());
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
        let placement_ref = &placement;
        let bearer_ref = bearer.as_str();
        let context = &request.context;
        let candidate_visibility = request.candidate_visibility.clone();
        let authorization_revision = request.authorization_revision;
        let result = execute_with_owner_failover(replicas, first_replica, |target| {
            let routed = routed.clone();
            let candidate_visibility = candidate_visibility.clone();
            let remaining = context.remaining();
            async move {
                self.execute_on(
                    target,
                    placement_ref,
                    bearer_ref,
                    routed,
                    remaining?,
                    candidate_visibility,
                    authorization_revision,
                )
                .await
            }
        })
        .await?;
        if self.placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "index query placement changed during execution",
            ));
        }
        Ok(result)
    }
}

async fn execute_with_owner_failover<T, F, Fut>(
    replicas: &[NodeId],
    first_replica: usize,
    mut execute: F,
) -> Result<T, Status>
where
    F: FnMut(NodeId) -> Fut,
    Fut: std::future::Future<Output = Result<T, Status>>,
{
    let mut last_unavailable = None;
    for target in replica_attempt_order(replicas, first_replica) {
        match execute(target).await {
            Ok(result) => return Ok(result),
            Err(error) if retryable_owner_failure(&error) => {
                tracing::debug!(
                    target.node_id = target.0,
                    grpc.status_code = ?error.code(),
                    "index query owner unavailable; trying the next weighted-HRW owner"
                );
                last_unavailable = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_unavailable
        .unwrap_or_else(|| Status::unavailable("index query has no available weighted-HRW owner")))
}

fn replica_attempt_order(
    replicas: &[NodeId],
    first_replica: usize,
) -> impl Iterator<Item = NodeId> + '_ {
    let count = replicas.len();
    (0..count).map(move |offset| replicas[(first_replica + offset) % count])
}

fn retryable_owner_failure(error: &Status) -> bool {
    // Peer transport, peer admission under a changed placement fence, and the
    // post-execution fence check all report UNAVAILABLE. Do not retry semantic,
    // authorization, integrity, resource, or query-deadline failures.
    error.code() == tonic::Code::Unavailable
}

fn require_local_authorization_revision(
    result: &ExecutedIndexQuery,
    required: u64,
) -> Result<(), Status> {
    if required == 0 || result.freshness.authorization_revision == 0 {
        return Err(Status::data_loss(
            "local index result has no Zanzibar authorization revision",
        ));
    }
    if result.freshness.authorization_revision != required {
        return Err(Status::failed_precondition(
            "authorization revision changed during local index execution",
        ));
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn owner_attempt_order_starts_at_selected_replica_and_wraps_once() {
        let replicas = [NodeId(10), NodeId(20), NodeId(30)];
        assert_eq!(
            replica_attempt_order(&replicas, 1).collect::<Vec<_>>(),
            [NodeId(20), NodeId(30), NodeId(10)]
        );
    }

    #[tokio::test]
    async fn unavailable_owner_falls_through_to_the_next_owner() {
        let replicas = [NodeId(10), NodeId(20), NodeId(30)];
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&attempts);
        let selected = execute_with_owner_failover(&replicas, 1, move |target| {
            let recorded = Arc::clone(&recorded);
            async move {
                recorded.lock().unwrap().push(target);
                if target == NodeId(20) {
                    Err(Status::unavailable("owner is offline"))
                } else {
                    Ok(target)
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(selected, NodeId(30));
        assert_eq!(*attempts.lock().unwrap(), [NodeId(20), NodeId(30)]);
    }

    #[tokio::test]
    async fn semantic_failure_is_not_retried_on_another_owner() {
        let replicas = [NodeId(10), NodeId(20), NodeId(30)];
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&attempts);
        let error = execute_with_owner_failover(&replicas, 2, move |target| {
            let recorded = Arc::clone(&recorded);
            async move {
                recorded.lock().unwrap().push(target);
                Err::<NodeId, _>(Status::permission_denied("not authorized"))
            }
        })
        .await
        .unwrap_err();

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert_eq!(*attempts.lock().unwrap(), [NodeId(30)]);
    }

    #[test]
    fn retry_classification_excludes_deadlines_integrity_and_semantic_errors() {
        assert!(retryable_owner_failure(&Status::unavailable("transport")));
        for error in [
            Status::deadline_exceeded("query deadline"),
            Status::failed_precondition("generation changed"),
            Status::permission_denied("not authorized"),
            Status::data_loss("corrupt artifact"),
            Status::resource_exhausted("query budget"),
        ] {
            assert!(!retryable_owner_failure(&error), "{:?}", error.code());
        }
    }
}
