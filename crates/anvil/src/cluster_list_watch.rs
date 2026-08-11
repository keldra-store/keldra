//! Production adapters for cluster-wide object listing and prefix watches.
//!
//! The distributed cores own merge/checkpoint semantics. This module only
//! projects applied Raft membership and avoids a peer round-trip when the
//! required watch source is the ingress node itself.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{JournalRoute, PlacementLogId, RoutedJournalError, SourceId, Store};
use tonic::{Code, Status};

use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::distributed_list::{AuthoritativeListAuthorizer, LocalListQuery, OriginalBearer};
use crate::distributed_watch::{
    ClusterWatchSources, DistributedWatchScope, WatchPlacement, WatchPlacementAuthority,
    WatchSourceError, WatchSourcePage, WatchSourceQuery, WatchSourceStatus, filter_public_changes,
};

#[derive(Clone)]
pub(crate) struct DecisionWatchPlacement {
    decisions: DecisionRaft,
}

impl DecisionWatchPlacement {
    pub(crate) fn new(decisions: DecisionRaft) -> Self {
        Self { decisions }
    }
}

impl WatchPlacementAuthority for DecisionWatchPlacement {
    fn current(&self) -> Result<WatchPlacement, String> {
        let state = self
            .decisions
            .state()
            .map_err(|error| format!("read applied cluster membership: {error}"))?;
        let placement =
            ClusterPlacement::from_applied(&state).map_err(|error| error.to_string())?;
        let sources = placement
            .active_node_ids()
            .into_iter()
            .map(|node| {
                placement
                    .address(node)
                    .map(|address| (node, address.0.clone()))
                    .ok_or_else(|| format!("ACTIVE watch source {} has no peer address", node.0))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        WatchPlacement::new(placement.cluster_id(), placement.fence(), sources)
            .map_err(|error| error.to_string())
    }
}

/// Local-source fast path plus the mandatory-mTLS transport for every other
/// ACTIVE source. Both paths evaluate the original bearer at the source.
#[derive(Clone)]
pub(crate) struct ClusterWatchSourcesAdapter {
    local_node: NodeId,
    store: Store,
    decisions: DecisionRaft,
    remote: ClusterPeerTransport,
    authorization: Arc<dyn AuthoritativeListAuthorizer>,
}

impl ClusterWatchSourcesAdapter {
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        remote: ClusterPeerTransport,
        authorization: Arc<dyn AuthoritativeListAuthorizer>,
    ) -> Self {
        Self {
            local_node,
            store,
            decisions,
            remote,
            authorization,
        }
    }

    fn require_local_placement(
        &self,
        target: NodeId,
        expected: PlacementLogId,
    ) -> Result<(), WatchSourceError> {
        if target != self.local_node {
            return Err(WatchSourceError::Unavailable(
                "local watch source received another node target".into(),
            ));
        }
        let state = self
            .decisions
            .state()
            .map_err(|_| WatchSourceError::Unavailable("membership is unavailable".into()))?;
        let placement = ClusterPlacement::from_applied(&state)
            .map_err(|error| WatchSourceError::Unavailable(error.to_string()))?;
        if placement.fence() != expected || !placement.active_node_ids().contains(&self.local_node)
        {
            return Err(WatchSourceError::Unavailable(
                "watch source placement changed".into(),
            ));
        }
        Ok(())
    }

    async fn authorize(
        &self,
        bearer: &OriginalBearer,
        scope: &DistributedWatchScope,
        fence: PlacementLogId,
    ) -> Result<(), WatchSourceError> {
        let query = watch_authorization_query(fence, scope).map_err(status_to_watch_error)?;
        self.authorization
            .authorize(bearer, &query)
            .await
            .map_err(status_to_watch_error)
    }
}

#[tonic::async_trait]
impl ClusterWatchSources for ClusterWatchSourcesAdapter {
    async fn status(
        &self,
        target: NodeId,
        address: &str,
        membership_revision: PlacementLogId,
        bearer: OriginalBearer,
        scope: DistributedWatchScope,
    ) -> Result<WatchSourceStatus, WatchSourceError> {
        if target != self.local_node {
            return self
                .remote
                .status(target, address, membership_revision, bearer, scope)
                .await;
        }
        self.require_local_placement(target, membership_revision)?;
        self.authorize(&bearer, &scope, membership_revision).await?;
        loop {
            let store = self.store.clone();
            let status = tokio::task::spawn_blocking(move || store.local_watch_status())
                .await
                .map_err(|error| WatchSourceError::Unavailable(error.to_string()))?
                .map_err(|error| WatchSourceError::Unavailable(error.to_string()))?;
            require_source_identity(self.local_node, status.source_id)?;
            if crate::programs::atomic_tail_is_clear(&self.decisions)
                .map_err(status_to_watch_error)?
            {
                return Ok(WatchSourceStatus {
                    source_node: self.local_node,
                    membership_revision,
                    status,
                });
            }
            crate::programs::wait_for_atomic_tail(&self.decisions, Duration::from_secs(30))
                .await
                .map_err(status_to_watch_error)?;
        }
    }

    async fn read_page(
        &self,
        target: NodeId,
        address: &str,
        bearer: OriginalBearer,
        query: WatchSourceQuery,
    ) -> Result<WatchSourcePage, WatchSourceError> {
        if target != self.local_node {
            return self.remote.read_page(target, address, bearer, query).await;
        }
        self.require_local_placement(target, query.membership_revision)?;
        self.authorize(&bearer, &query.scope, query.membership_revision)
            .await?;
        loop {
            let store = self.store.clone();
            let expected_source = query.expected_source;
            let next_offset = query.next_offset;
            let max_records = query.max_records;
            let tenant_id = query.scope.tenant_id();
            let bucket_id = query.scope.bucket_id();
            let (status, changes, returned_next) = tokio::task::spawn_blocking(move || {
                let status = store.local_watch_status()?;
                if status.source_id != expected_source {
                    return Err(anvil_store::WatchError::ResumeExpired);
                }
                let floor_next = status.retention_floor.checked_add(1).ok_or_else(|| {
                    anvil_store::WatchError::Storage("watch retention floor cannot advance".into())
                })?;
                let settled_next = status.settled_through.checked_add(1).ok_or_else(|| {
                    anvil_store::WatchError::Storage("watch settled boundary cannot advance".into())
                })?;
                if next_offset < floor_next || next_offset > settled_next {
                    return Err(anvil_store::WatchError::ResumeExpired);
                }
                let page = store
                    .scan_routed_local_changes(
                        JournalRoute::Bucket {
                            tenant_id,
                            bucket_id,
                        },
                        expected_source,
                        next_offset - 1,
                        status.settled_through,
                        max_records,
                        crate::distributed_watch::MAX_WATCH_SOURCE_PAGE_BYTES,
                    )
                    .map_err(routed_watch_error)?;
                if let Some(oversize) = page.oversize {
                    return Err(anvil_store::WatchError::Storage(format!(
                        "watch source event at offset {} needs {} bytes",
                        oversize.offset, oversize.encoded_bytes
                    )));
                }
                let returned_next = page.through_offset.checked_add(1).ok_or_else(|| {
                    anvil_store::WatchError::Storage("watch cursor cannot advance".into())
                })?;
                Ok((status, page.changes, returned_next))
            })
            .await
            .map_err(|error| WatchSourceError::Unavailable(error.to_string()))?
            .map_err(|error| match error {
                anvil_store::WatchError::ResumeExpired => WatchSourceError::ResumeExpired,
                other => WatchSourceError::Unavailable(other.to_string()),
            })?;
            require_source_identity(self.local_node, status.source_id)?;
            if crate::programs::atomic_tail_is_clear(&self.decisions)
                .map_err(status_to_watch_error)?
            {
                return Ok(WatchSourcePage {
                    source_node: self.local_node,
                    membership_revision: query.membership_revision,
                    status,
                    next_offset: returned_next,
                    object_heads: filter_public_changes(&query.scope, changes),
                });
            }
            crate::programs::wait_for_atomic_tail(&self.decisions, Duration::from_secs(30))
                .await
                .map_err(status_to_watch_error)?;
        }
    }
}

fn routed_watch_error(error: RoutedJournalError) -> anvil_store::WatchError {
    match error {
        RoutedJournalError::CursorExpired { .. }
        | RoutedJournalError::SourceEpochMismatch
        | RoutedJournalError::SourceNodeMismatch => anvil_store::WatchError::ResumeExpired,
        other => anvil_store::WatchError::Storage(other.to_string()),
    }
}

fn watch_authorization_query(
    fence: PlacementLogId,
    scope: &DistributedWatchScope,
) -> Result<LocalListQuery, Status> {
    LocalListQuery::new(
        fence,
        scope.tenant(),
        scope.bucket(),
        scope.tenant_id(),
        scope.bucket_id(),
        scope.prefix(),
        None,
        1,
    )
}

fn require_source_identity(local_node: NodeId, source: SourceId) -> Result<(), WatchSourceError> {
    if u64::from(source.node_id) == local_node.0 {
        Ok(())
    } else {
        Err(WatchSourceError::Unavailable(
            "local watch journal belongs to another node".into(),
        ))
    }
}

fn status_to_watch_error(status: Status) -> WatchSourceError {
    if status.code() == Code::OutOfRange && status.message() == "RESUME_EXPIRED" {
        WatchSourceError::ResumeExpired
    } else {
        WatchSourceError::Unavailable(status.to_string())
    }
}
