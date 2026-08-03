//! Fenced cluster scans used for cold index discovery and initial builds.

use std::collections::BTreeMap;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::ObjectRecordCursor;
use tonic::Status;

use crate::cluster_peer::{
    ClusterPeerTransport, IndexCurrentHead, IndexHeadScanPage, IndexHeadScanScope,
};
use crate::cluster_placement::ClusterPlacement;

#[derive(Clone)]
pub(crate) struct ClusterIndexScanner {
    decisions: DecisionRaft,
    peers: ClusterPeerTransport,
}

impl ClusterIndexScanner {
    pub(crate) fn new(decisions: DecisionRaft, peers: ClusterPeerTransport) -> Self {
        Self { decisions, peers }
    }

    pub(crate) async fn scan(
        &self,
        scope: IndexHeadScanScope,
    ) -> Result<Vec<IndexCurrentHead>, Status> {
        let placement = self.placement()?;
        let fence = placement.fence();
        let mut tasks = tokio::task::JoinSet::new();
        for node in placement.active_node_ids() {
            let address = placement
                .address(node)
                .ok_or_else(|| Status::unavailable("ACTIVE index scan source has no address"))?
                .0
                .clone();
            let peers = self.peers.clone();
            let scope = scope.clone();
            tasks.spawn(async move { scan_source(peers, node, address, scope).await });
        }

        let mut selected = BTreeMap::<(u64, u64, String), IndexCurrentHead>::new();
        while let Some(joined) = tasks.join_next().await {
            for head in joined
                .map_err(|error| Status::internal(format!("index scan task failed: {error}")))??
            {
                let key = (head.tenant_id, head.bucket_id, head.exact_path.clone());
                match selected.get(&key) {
                    Some(existing)
                        if existing.head == head.head && existing.version == head.version => {}
                    Some(existing) if existing.head.version >= head.head.version => {}
                    _ => {
                        selected.insert(key, head);
                    }
                }
            }
        }
        if self.placement()?.fence() != fence {
            return Err(Status::unavailable(
                "cluster placement changed during index scan",
            ));
        }
        Ok(selected.into_values().collect())
    }

    fn placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

async fn scan_source(
    peers: ClusterPeerTransport,
    node: NodeId,
    address: String,
    scope: IndexHeadScanScope,
) -> Result<Vec<IndexCurrentHead>, Status> {
    let mut cursor: Option<ObjectRecordCursor> = None;
    let mut heads = Vec::new();
    loop {
        let IndexHeadScanPage {
            heads: page,
            next_cursor,
            ..
        } = peers
            .scan_index_heads(node, &address, scope.clone(), cursor.as_ref())
            .await?;
        heads.extend(page);
        match next_cursor {
            Some(next) if cursor.as_ref().is_some_and(|current| current == &next) => {
                return Err(Status::data_loss(
                    "index scan source returned a non-advancing cursor",
                ));
            }
            Some(next) => cursor = Some(next),
            None => return Ok(heads),
        }
    }
}
