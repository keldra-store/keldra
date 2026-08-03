//! Production adapters and bounded workers for ordered reference delivery.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::{
    BlobRef, Durability, ErasureProfile, PayloadArtifactState, ReferenceDeltaApplied,
    ReferenceDeltaBatch, SourceId, Store, WatchJournalStatus,
};
use tonic::Code;

use super::cleanup::{
    ActiveReferenceSource, ReferenceProofCleanup, ReferenceProofCleanupPlacement,
    ReferenceProofCleanupView, ReferenceProofSourceStatuses,
};
use super::{
    PositiveReferencePreparation, QuorumReferenceCommitAuthority, ReferenceDelivery,
    ReferenceDestinations, ReferencePlacement, ReferencePlacementAuthority,
};
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::{DATA_PEER_FRAME_BYTES, DATA_PEER_SCHEMA_VERSION, DataPeerTransport};
use crate::payload_distribution::PayloadDistribution;
use crate::serving_fence::ServingAuthority;

const DELIVERY_IDLE_INTERVAL: Duration = Duration::from_millis(100);
const DELIVERY_ERROR_BACKOFF: Duration = Duration::from_millis(500);
const PROOF_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone)]
struct DecisionReferencePlacement {
    decisions: DecisionRaft,
    reference_safe: Arc<AtomicBool>,
}

impl DecisionReferencePlacement {
    fn placement(&self) -> Result<ReferencePlacement, String> {
        let state = self.decisions.state().map_err(|error| error.to_string())?;
        let placement =
            ClusterPlacement::from_applied(&state).map_err(|error| error.to_string())?;
        let addresses = placement
            .active_node_ids()
            .into_iter()
            .map(|node| {
                let address = placement
                    .address(node)
                    .ok_or_else(|| format!("ACTIVE node {} has no peer address", node.0))?;
                Ok((node, address.0.clone()))
            })
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        Ok(ReferencePlacement {
            cluster_id: placement.cluster_id(),
            fence: placement.fence(),
            nodes: placement.placement_nodes().to_vec(),
            addresses,
        })
    }
}

impl ReferencePlacementAuthority for DecisionReferencePlacement {
    fn current(&self) -> Result<ReferencePlacement, String> {
        self.placement()
    }
}

impl ReferenceProofCleanupPlacement for DecisionReferencePlacement {
    fn current(&self) -> Result<ReferenceProofCleanupView, String> {
        let state = self.decisions.state().map_err(|error| error.to_string())?;
        let transition_in_progress = state.cluster_control().transition().is_some();
        let placement =
            ClusterPlacement::from_applied(&state).map_err(|error| error.to_string())?;
        let active_sources = placement
            .active_node_ids()
            .into_iter()
            .map(|node| {
                let address = placement
                    .address(node)
                    .ok_or_else(|| format!("ACTIVE node {} has no peer address", node.0))?;
                Ok(ActiveReferenceSource {
                    node,
                    address: address.0.clone(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(ReferenceProofCleanupView {
            placement_fence: placement.fence(),
            active_sources,
            transition_in_progress,
            reference_reconstruction_safe: self.reference_safe.load(Ordering::Acquire),
        })
    }
}

#[derive(Clone)]
struct StoreReferenceDestinations {
    local_node: NodeId,
    store: Store,
    peers: DataPeerTransport,
}

#[tonic::async_trait]
impl ReferenceDestinations for StoreReferenceDestinations {
    async fn cursor(&self, node: NodeId, address: &str, source: SourceId) -> Result<u64, String> {
        if node == self.local_node {
            return self
                .store
                .reference_delta_cursor(source)
                .map_err(|error| error.to_string());
        }
        self.peers
            .reference_delta_status(node, address, source)
            .await
            .map_err(|error| error.to_string())
    }

    async fn apply(
        &self,
        node: NodeId,
        address: &str,
        batch: ReferenceDeltaBatch,
    ) -> Result<ReferenceDeltaApplied, String> {
        if node == self.local_node {
            return self
                .store
                .apply_reference_deltas(batch)
                .await
                .map_err(|error| error.to_string());
        }
        self.peers
            .apply_reference_deltas(node, address, &batch)
            .await
            .map_err(|error| error.to_string())
    }
}

#[tonic::async_trait]
impl ReferenceProofSourceStatuses for StoreReferenceDestinations {
    async fn status(&self, node: NodeId, address: &str) -> Result<WatchJournalStatus, String> {
        if node == self.local_node {
            return self
                .store
                .local_watch_status()
                .map_err(|error| error.to_string());
        }
        self.peers
            .source_journal_status(node, address)
            .await
            .map_err(|error| error.to_string())
    }
}

struct StorePositiveReferencePreparation {
    local_node: NodeId,
    store: Store,
    peers: DataPeerTransport,
    distribution: PayloadDistribution,
}

impl StorePositiveReferencePreparation {
    async fn ensure_complete_source(
        &self,
        placement: &ReferencePlacement,
        reference: &BlobRef,
    ) -> Result<(), String> {
        if self
            .store
            .complete_copy_state(reference)
            .await
            .map_err(|error| error.to_string())?
            == PayloadArtifactState::Valid
        {
            return Ok(());
        }

        for node in placement.active_node_ids() {
            if node == self.local_node {
                continue;
            }
            let address = placement
                .address(node)
                .ok_or_else(|| format!("ACTIVE node {} has no peer address", node.0))?;
            let fetched = if reference.length <= anvil_store::SMALL_BLOB_MAX_BYTES as u64 {
                self.fetch_small(node, address, reference).await
            } else {
                self.fetch_large(node, address, reference).await
            };
            match fetched {
                Ok(()) => return Ok(()),
                Err(SourceFetchError::Absent) => continue,
                Err(SourceFetchError::Failed(error)) => {
                    tracing::warn!(source.node = node.0, %error, "reference source probe failed");
                }
            }
        }
        Err("no ACTIVE node holds a valid complete source for positive reference delivery".into())
    }

    async fn fetch_small(
        &self,
        node: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<(), SourceFetchError> {
        let bytes = self
            .peers
            .get_small_content(node, address, reference)
            .await
            .map_err(source_fetch_status)?;
        self.store
            .seal_small_copy(reference, &bytes)
            .await
            .map_err(|error| SourceFetchError::Failed(error.to_string()))?;
        Ok(())
    }

    async fn fetch_large(
        &self,
        node: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<(), SourceFetchError> {
        let mut stream = self
            .peers
            .get_complete_source(node, address, reference)
            .await
            .map_err(source_fetch_status)?;
        let mut upload = self
            .store
            .begin_blob_upload()
            .await
            .map_err(|error| SourceFetchError::Failed(error.to_string()))?;
        let mut offset = 0_u64;
        while let Some(frame) = stream.message().await.map_err(source_fetch_status)? {
            if frame.schema_version != DATA_PEER_SCHEMA_VERSION
                || frame.offset != offset
                || frame.content.len() > DATA_PEER_FRAME_BYTES
                || (frame.content.is_empty() && !frame.end)
            {
                return Err(SourceFetchError::Failed(
                    "complete-source stream is malformed".into(),
                ));
            }
            let next_offset = offset
                .checked_add(frame.content.len() as u64)
                .ok_or_else(|| SourceFetchError::Failed("source length overflow".into()))?;
            if next_offset > reference.length {
                return Err(SourceFetchError::Failed(
                    "complete-source stream exceeded the advertised length".into(),
                ));
            }
            upload
                .write(&frame.content)
                .await
                .map_err(|error| SourceFetchError::Failed(error.to_string()))?;
            offset = next_offset;
            if frame.end {
                if offset != reference.length {
                    return Err(SourceFetchError::Failed(
                        "complete-source stream ended at another length".into(),
                    ));
                }
                self.store
                    .seal_complete_source_upload(reference, upload)
                    .await
                    .map_err(|error| SourceFetchError::Failed(error.to_string()))?;
                return Ok(());
            }
        }
        Err(SourceFetchError::Failed(
            "complete-source stream ended without an end frame".into(),
        ))
    }
}

#[tonic::async_trait]
impl PositiveReferencePreparation for StorePositiveReferencePreparation {
    async fn prepare(&self, placement: &ReferencePlacement, blob: &BlobRef) -> Result<(), String> {
        self.ensure_complete_source(placement, blob).await?;
        self.distribution
            .prepare_on_upload_source(placement, blob, Durability::Replicated)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

enum SourceFetchError {
    Absent,
    Failed(String),
}

fn source_fetch_status(status: tonic::Status) -> SourceFetchError {
    if status.code() == Code::NotFound {
        SourceFetchError::Absent
    } else {
        SourceFetchError::Failed(status.to_string())
    }
}

/// Owns the background tasks and the exact distributed-GC safety check.
pub(crate) struct ReferenceRuntime {
    stop: tokio::sync::watch::Sender<bool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

#[derive(Clone)]
pub(crate) struct ReferenceRuntimeHandle {
    local_node: NodeId,
    store: Store,
    decisions: DecisionRaft,
    serving: ServingAuthority,
    destinations: StoreReferenceDestinations,
    reference_safe: Arc<AtomicBool>,
    profile: ErasureProfile,
}

impl ReferenceRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        serving: ServingAuthority,
        data_peers: DataPeerTransport,
        cluster_peers: ClusterPeerTransport,
        profile: ErasureProfile,
    ) -> (Self, ReferenceRuntimeHandle) {
        let reference_safe = Arc::new(AtomicBool::new(false));
        let placement = Arc::new(DecisionReferencePlacement {
            decisions: decisions.clone(),
            reference_safe: reference_safe.clone(),
        });
        let destinations = StoreReferenceDestinations {
            local_node,
            store: store.clone(),
            peers: data_peers.clone(),
        };
        let payloads = Arc::new(StorePositiveReferencePreparation {
            local_node,
            store: store.clone(),
            peers: data_peers.clone(),
            distribution: PayloadDistribution::new(
                local_node,
                store.clone(),
                Arc::new(data_peers.clone()),
                profile,
            ),
        });
        let commits = Arc::new(QuorumReferenceCommitAuthority::new(
            store.clone(),
            placement.clone(),
            Arc::new(cluster_peers),
        ));
        let delivery = ReferenceDelivery::new(
            store.clone(),
            placement.clone(),
            commits,
            Arc::new(destinations.clone()),
            payloads,
            profile,
        );
        let cleanup =
            ReferenceProofCleanup::new(store.clone(), placement, Arc::new(destinations.clone()));
        let (stop, stop_signal) = tokio::sync::watch::channel(false);
        let delivery_safety = reference_safe.clone();
        let mut delivery_stop = stop_signal.clone();
        let delivery_task = tokio::spawn(async move {
            loop {
                let result = tokio::select! {
                    changed = delivery_stop.changed() => {
                        if changed.is_err() || *delivery_stop.borrow() {
                            break;
                        }
                        continue;
                    }
                    result = delivery.deliver_once() => result,
                };
                let delay = match result {
                    Ok(progress) => {
                        delivery_safety.store(true, Ordering::Release);
                        if progress.safe_through == progress.tail {
                            DELIVERY_IDLE_INTERVAL
                        } else {
                            Duration::ZERO
                        }
                    }
                    Err(error) => {
                        // A source or destination outage invalidates the last
                        // reconstruction proof until a complete pass succeeds.
                        delivery_safety.store(false, Ordering::Release);
                        tracing::warn!(%error, "ordered reference delivery paused");
                        DELIVERY_ERROR_BACKOFF
                    }
                };
                if wait_for_stop(&mut delivery_stop, delay).await {
                    break;
                }
            }
            delivery_safety.store(false, Ordering::Release);
        });
        let mut cleanup_stop = stop_signal;
        let cleanup_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(PROOF_CLEANUP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = cleanup_stop.changed() => {
                        if changed.is_err() || *cleanup_stop.borrow() {
                            break;
                        }
                        continue;
                    }
                    _ = interval.tick() => {}
                }
                let result = tokio::select! {
                    changed = cleanup_stop.changed() => {
                        if changed.is_err() || *cleanup_stop.borrow() {
                            break;
                        }
                        continue;
                    }
                    result = cleanup.run_once() => result,
                };
                if let Err(error) = result {
                    tracing::warn!(%error, "reference-proof cleanup paused");
                }
            }
        });
        let handle = ReferenceRuntimeHandle {
            local_node,
            store,
            decisions,
            serving,
            destinations,
            reference_safe,
            profile,
        };
        (
            Self {
                stop,
                tasks: vec![delivery_task, cleanup_task],
            },
            handle,
        )
    }

    pub(crate) async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        for task in self.tasks.drain(..) {
            if let Err(error) = task.await
                && !error.is_cancelled()
            {
                tracing::error!(%error, "reference runtime task stopped unexpectedly");
            }
        }
    }
}

impl Drop for ReferenceRuntime {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl ReferenceRuntimeHandle {
    /// Waits without a data-size timeout for exact reference safety, or for a
    /// freshly fenced placement that is structurally too small to converge.
    /// `true` permits GC; `false` permits only degraded LOCAL service. Dropping
    /// this future cancels the wait without changing durable state.
    pub(crate) async fn wait_until_startup_ready(&self) -> bool {
        loop {
            if self.gc_safe().await {
                return true;
            }
            if self.can_serve_under_redundant() {
                return false;
            }
            tokio::time::sleep(READINESS_POLL_INTERVAL).await;
        }
    }

    /// A cluster smaller than the fixed erasure geometry cannot create every
    /// distinct final shard. LOCAL requests are still allowed to rely on the
    /// coordinator's complete source, but GC remains disabled until enough
    /// nodes join and ordered reference delivery catches up.
    fn can_serve_under_redundant(&self) -> bool {
        if !self.serving.has_valid_lease() {
            return false;
        }
        let Ok(state) = self.decisions.state() else {
            return false;
        };
        if state.cluster_control().transition().is_some() {
            return false;
        }
        let Ok(placement) = ClusterPlacement::from_applied(&state) else {
            return false;
        };
        let active = placement.active_node_ids();
        active.contains(&self.local_node)
            && structurally_under_redundant(active.len(), self.profile)
    }

    /// GC is safe only after this destination has consumed the complete tail
    /// from every current source under one unchanged, freshly leased view.
    pub(crate) async fn gc_safe(&self) -> bool {
        if !self.reference_safe.load(Ordering::Acquire) || !self.serving.has_valid_lease() {
            return false;
        }
        let Ok(started) = self.decisions.state() else {
            return false;
        };
        if started.cluster_control().transition().is_some() {
            return false;
        }
        let Ok(placement) = ClusterPlacement::from_applied(&started) else {
            return false;
        };
        let active_nodes = placement.active_node_ids();
        if !active_nodes.contains(&self.local_node) {
            return false;
        }
        for node in active_nodes {
            let Some(address) = placement.address(node) else {
                return false;
            };
            let Ok(status) = self.destinations.status(node, &address.0).await else {
                return false;
            };
            let Ok(cursor) = self.store.reference_delta_cursor(status.source_id) else {
                return false;
            };
            if !source_is_fully_applied(node, status, cursor) {
                return false;
            }
        }
        let Ok(current) = self.decisions.state() else {
            return false;
        };
        current.cluster_control().active_placement_log_id()
            == started.cluster_control().active_placement_log_id()
            && current.cluster_control().transition().is_none()
            && self.reference_safe.load(Ordering::Acquire)
            && self.serving.has_valid_lease()
    }
}

async fn wait_for_stop(stop: &mut tokio::sync::watch::Receiver<bool>, delay: Duration) -> bool {
    if *stop.borrow() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        changed = stop.changed() => changed.is_err() || *stop.borrow(),
    }
}

fn source_is_fully_applied(expected_node: NodeId, status: WatchJournalStatus, cursor: u64) -> bool {
    u64::from(status.source_id.node_id) == expected_node.0
        && status.source_id.source_epoch != [0; 32]
        && status.retention_floor <= status.tail
        && status.retained_entries == status.tail - status.retention_floor
        && cursor == status.tail
}

fn structurally_under_redundant(active_nodes: usize, profile: ErasureProfile) -> bool {
    active_nodes < usize::from(profile.total_shards())
}

#[cfg(test)]
#[path = "runtime/tests.rs"]
mod tests;
