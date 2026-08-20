//! Placement-aware retirement of former payload artifacts.
//!
//! Placement remains derived from the committed ACTIVE membership. This
//! worker keeps no inventory: it scans the ordinary lifecycle column family,
//! proves the current selected artifacts healthy, and turns a former local
//! artifact into an ordinary age-gated GC candidate.

use keldra_consensus::{DecisionRaft, NodeId};
use keldra_store::{
    ErasureCodec, ErasureProfile, PayloadArtifactCursor, PayloadArtifactIdentity,
    PayloadArtifactState, PlacementLogId, ShardIdentity, Store,
};
use thiserror::Error;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;
use crate::payload_distribution::PayloadArtifactPeers;
use crate::payload_placement::{PayloadPlacement, select_payload_placement};
use crate::reference_delivery::ReferenceRuntimeHandle;

const DEFAULT_MAX_ARTIFACTS_PER_TICK: u32 = 128;
const DEFAULT_MAX_ARTIFACT_METADATA_BYTES_PER_TICK: u64 = 1024 * 1024;
const DEFAULT_MAX_ARTIFACT_TIME_PER_TICK: Duration = Duration::from_secs(30);
const MAX_ARTIFACTS_PER_TICK: u32 = 1_000;
const MAX_ARTIFACT_METADATA_RECORD_BYTES: u64 = 128;

#[derive(Clone, Copy, Debug)]
pub(crate) struct PayloadGcBudget {
    pub(crate) max_records: u32,
    pub(crate) max_bytes: u64,
    pub(crate) max_time: Duration,
}

impl PayloadGcBudget {
    pub(crate) fn new(
        max_records: u32,
        max_bytes: u64,
        max_time: Duration,
    ) -> Result<Self, PayloadGcError> {
        if max_records == 0
            || max_records > MAX_ARTIFACTS_PER_TICK
            || max_bytes < MAX_ARTIFACT_METADATA_RECORD_BYTES
            || max_time.is_zero()
        {
            return Err(PayloadGcError::InvalidBudget);
        }
        Ok(Self {
            max_records,
            max_bytes,
            max_time,
        })
    }
}

impl Default for PayloadGcBudget {
    fn default() -> Self {
        Self {
            max_records: DEFAULT_MAX_ARTIFACTS_PER_TICK,
            max_bytes: DEFAULT_MAX_ARTIFACT_METADATA_BYTES_PER_TICK,
            max_time: DEFAULT_MAX_ARTIFACT_TIME_PER_TICK,
        }
    }
}

#[derive(Clone)]
pub(crate) struct PayloadGarbageCollector {
    local_node: NodeId,
    store: Store,
    decisions: DecisionRaft,
    peers: DataPeerTransport,
    references: ReferenceRuntimeHandle,
    profile: ErasureProfile,
    budget: PayloadGcBudget,
    progress: Arc<Mutex<PayloadGcProgress>>,
    run_lock: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Default)]
struct PayloadGcProgress {
    fence: Option<PlacementLogId>,
    cursor: Option<PayloadArtifactCursor>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PayloadGcTick {
    pub(crate) retired: u64,
    pub(crate) cycle_complete: bool,
}

impl PayloadGarbageCollector {
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        decisions: DecisionRaft,
        peers: DataPeerTransport,
        references: ReferenceRuntimeHandle,
        profile: ErasureProfile,
    ) -> Self {
        Self {
            local_node,
            store,
            decisions,
            peers,
            references,
            profile,
            budget: PayloadGcBudget::default(),
            progress: Arc::new(Mutex::new(PayloadGcProgress::default())),
            run_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(crate) fn with_budget(mut self, budget: PayloadGcBudget) -> Self {
        self.budget = budget;
        self
    }

    /// Retire former local copies only under one exact, reference-safe ACTIVE
    /// placement. Physical deletion remains the ordinary Store GC's job after
    /// its configured inactivity grace.
    pub(crate) async fn run_once(&self) -> Result<PayloadGcTick, PayloadGcError> {
        let _run = self.run_lock.lock().await;
        if !self.references.gc_safe().await {
            return Ok(PayloadGcTick {
                retired: 0,
                cycle_complete: true,
            });
        }
        let placement = current_stable_placement(&self.decisions, self.local_node)?;
        let fence = placement.fence();
        let cursor = {
            let mut progress = self
                .progress
                .lock()
                .map_err(|_| PayloadGcError::ProgressPoisoned)?;
            if progress.fence != Some(fence) {
                progress.fence = Some(fence);
                progress.cursor = None;
            }
            progress.cursor.clone()
        };
        let started = Instant::now();
        let mut inspected = 0_u32;
        let mut inspected_bytes = 0_u64;
        let mut retired = 0_u64;
        let page = self
            .store
            .export_payload_artifact_snapshots(cursor.as_ref(), self.budget.max_records)
            .map_err(|error| PayloadGcError::Store(error.to_string()))?;
        let page_complete = page.next_cursor.is_none();
        let page_records = page.artifacts.len();
        for artifact in page.artifacts {
            let record_bytes = artifact.identity.handoff_order_key().len() as u64
                + std::mem::size_of_val(&artifact.lifecycle) as u64;
            if inspected != 0
                && (inspected >= self.budget.max_records
                    || inspected_bytes.saturating_add(record_bytes) > self.budget.max_bytes
                    || started.elapsed() >= self.budget.max_time)
            {
                break;
            }
            let remaining = self.budget.max_time.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            if artifact.lifecycle.ref_count != 0 || artifact.lifecycle.flags != 0 {
                let desired = select_payload_placement(
                    placement.cluster_id(),
                    artifact.identity.blob(),
                    self.profile,
                    placement.placement_nodes(),
                );
                if !artifact_is_selected(self.local_node, &artifact.identity, &desired) {
                    let healthy = match tokio::time::timeout(
                        remaining,
                        self.placement_is_healthy(&placement, &desired, artifact.identity.blob()),
                    )
                    .await
                    {
                        Ok(Ok(healthy)) => healthy,
                        Ok(Err(error)) => {
                            tracing::debug!(
                                %error,
                                monotonic_counter.keldra_payload_gc_probe_skipped_total = 1_u64,
                                "payload retirement candidate skipped after a failed health probe"
                            );
                            false
                        }
                        Err(_) => {
                            tracing::debug!(
                                monotonic_counter.keldra_payload_gc_probe_timeouts_total = 1_u64,
                                "payload retirement candidate skipped after exhausting the tick"
                            );
                            false
                        }
                    };
                    if healthy {
                        // The health probes can take time. Re-prove reference
                        // safety before entering the short Store commit fence.
                        if !self.references.gc_safe().await {
                            return Ok(PayloadGcTick {
                                retired,
                                cycle_complete: false,
                            });
                        }
                        let decisions = self.decisions.clone();
                        let local_node = self.local_node;
                        if self
                            .store
                            .retire_payload_artifact_if_unchanged(&artifact, move || {
                                placement_fence_is_current(&decisions, local_node, fence)
                            })
                            .await
                            .map_err(|error| PayloadGcError::Store(error.to_string()))?
                        {
                            retired = retired
                                .checked_add(1)
                                .ok_or(PayloadGcError::RetirementCountExhausted)?;
                        }
                    }
                }
            }
            inspected += 1;
            inspected_bytes = inspected_bytes.saturating_add(record_bytes);
            let cursor = PayloadArtifactCursor::from_key(artifact.identity.handoff_order_key())
                .map_err(|error| PayloadGcError::Store(error.to_string()))?;
            self.progress
                .lock()
                .map_err(|_| PayloadGcError::ProgressPoisoned)?
                .cursor = Some(cursor);
        }
        let cycle_complete = page_complete && inspected as usize == page_records;
        if cycle_complete {
            self.progress
                .lock()
                .map_err(|_| PayloadGcError::ProgressPoisoned)?
                .cursor = None;
        }
        tracing::debug!(
            gauge.keldra_payload_gc_tick_records = inspected as u64,
            gauge.keldra_payload_gc_tick_bytes = inspected_bytes,
            monotonic_counter.keldra_payload_gc_tick_retired_total = retired,
            "bounded payload retirement tick completed"
        );
        Ok(PayloadGcTick {
            retired,
            cycle_complete,
        })
    }

    async fn placement_is_healthy(
        &self,
        placement: &ClusterPlacement,
        desired: &PayloadPlacement,
        blob: &keldra_store::BlobRef,
    ) -> Result<bool, PayloadGcError> {
        match desired {
            PayloadPlacement::Small(selected) => {
                for owner in selected.owners() {
                    if !self
                        .complete_is_healthy(placement, *owner, blob, true)
                        .await?
                    {
                        return Ok(false);
                    }
                }
            }
            PayloadPlacement::LargeComplete(selected) => {
                for owner in selected.owners() {
                    if !self
                        .complete_is_healthy(placement, *owner, blob, false)
                        .await?
                    {
                        return Ok(false);
                    }
                }
            }
            PayloadPlacement::Large(selected) => {
                let codec = ErasureCodec::new(self.profile)
                    .map_err(|error| PayloadGcError::Placement(error.to_string()))?;
                for shard in selected.shards() {
                    let identity = ShardIdentity::new(blob.clone(), shard.ordinal());
                    let healthy = if shard.owner() == self.local_node {
                        self.store.get_shard(&codec, &identity).is_ok()
                    } else {
                        let address = placement
                            .address(shard.owner())
                            .ok_or(PayloadGcError::MissingAddress(shard.owner()))?;
                        PayloadArtifactPeers::shard_exists(
                            &self.peers,
                            shard.owner(),
                            &address.0,
                            &identity,
                        )
                        .await
                        .map_err(|error| PayloadGcError::Peer(error.to_string()))?
                    };
                    if !healthy {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    async fn complete_is_healthy(
        &self,
        placement: &ClusterPlacement,
        owner: NodeId,
        blob: &keldra_store::BlobRef,
        small: bool,
    ) -> Result<bool, PayloadGcError> {
        if owner == self.local_node {
            return self
                .store
                .complete_copy_state(blob)
                .await
                .map(|state| state == PayloadArtifactState::Valid)
                .map_err(|error| PayloadGcError::Store(error.to_string()));
        }
        let address = placement
            .address(owner)
            .ok_or(PayloadGcError::MissingAddress(owner))?;
        let result = if small {
            PayloadArtifactPeers::small_exists(&self.peers, owner, &address.0, blob).await
        } else {
            PayloadArtifactPeers::complete_exists(&self.peers, owner, &address.0, blob).await
        };
        result.map_err(|error| PayloadGcError::Peer(error.to_string()))
    }
}

fn artifact_is_selected(
    local_node: NodeId,
    artifact: &PayloadArtifactIdentity,
    desired: &PayloadPlacement,
) -> bool {
    match (artifact, desired) {
        (PayloadArtifactIdentity::Complete(_), PayloadPlacement::Small(selected)) => {
            selected.owners().contains(&local_node)
        }
        (PayloadArtifactIdentity::Complete(_), PayloadPlacement::LargeComplete(selected)) => {
            selected.owners().contains(&local_node)
        }
        (PayloadArtifactIdentity::Shard(identity), PayloadPlacement::Large(selected)) => selected
            .shards()
            .iter()
            .any(|shard| shard.owner() == local_node && shard.ordinal() == identity.ordinal()),
        _ => false,
    }
}

fn current_stable_placement(
    decisions: &DecisionRaft,
    local_node: NodeId,
) -> Result<ClusterPlacement, PayloadGcError> {
    let state = decisions
        .state()
        .map_err(|error| PayloadGcError::Placement(error.to_string()))?;
    if state.cluster_control().transition().is_some() {
        return Err(PayloadGcError::TransitionInProgress);
    }
    let placement = ClusterPlacement::from_applied(&state)
        .map_err(|error| PayloadGcError::Placement(error.to_string()))?;
    if !placement.active_node_ids().contains(&local_node) {
        return Err(PayloadGcError::LocalNodeInactive(local_node));
    }
    Ok(placement)
}

fn placement_fence_is_current(
    decisions: &DecisionRaft,
    local_node: NodeId,
    expected: PlacementLogId,
) -> bool {
    current_stable_placement(decisions, local_node)
        .is_ok_and(|placement| placement.fence() == expected)
}

#[derive(Debug, Error)]
pub(crate) enum PayloadGcError {
    #[error("payload GC placement is unavailable: {0}")]
    Placement(String),
    #[error("payload GC is paused during a membership transition")]
    TransitionInProgress,
    #[error("local node {0:?} is not ACTIVE in the current placement")]
    LocalNodeInactive(NodeId),
    #[error("ACTIVE payload owner {0:?} has no peer address")]
    MissingAddress(NodeId),
    #[error("payload replacement health probe failed: {0}")]
    Peer(String),
    #[error("payload lifecycle storage failed: {0}")]
    Store(String),
    #[error("payload GC retirement count is exhausted")]
    RetirementCountExhausted,
    #[error("payload GC progress lock is poisoned")]
    ProgressPoisoned,
    #[error("payload GC record, byte, or time budget is outside its supported bound")]
    InvalidBudget,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use keldra_consensus::ClusterId;
    use keldra_store::{BlobRef, SMALL_BLOB_MAX_BYTES};

    use super::*;
    use crate::placement::PlacementNode;

    fn node(node_id: u64) -> PlacementNode {
        PlacementNode::new(NodeId(node_id), NonZeroU32::new(1_000_000).unwrap())
    }

    #[test]
    fn selection_classifies_displaced_small_and_former_large_complete_copies() {
        let cluster = ClusterId(*b"payload-gc-test!");
        let profile = ErasureProfile::default();
        let nodes = [node(1), node(2), node(3)];
        let small = BlobRef {
            hash: [3; 32],
            length: 7,
        };
        let large = BlobRef {
            hash: [9; 32],
            length: SMALL_BLOB_MAX_BYTES as u64 + 1,
        };
        let small_placement = select_payload_placement(cluster, &small, profile, &nodes);
        let large_placement = select_payload_placement(cluster, &large, profile, &nodes);
        let PayloadPlacement::Small(selected_small) = &small_placement else {
            panic!("expected small placement")
        };
        let displaced = nodes
            .iter()
            .map(|node| node.node_id())
            .find(|node_id| !selected_small.owners().contains(node_id))
            .expect("three nodes contain one node outside a two-copy placement");
        assert!(!artifact_is_selected(
            displaced,
            &PayloadArtifactIdentity::Complete(small.clone()),
            &small_placement,
        ));
        assert!(artifact_is_selected(
            selected_small.owners()[0],
            &PayloadArtifactIdentity::Complete(small),
            &small_placement,
        ));
        assert!(!artifact_is_selected(
            NodeId(1),
            &PayloadArtifactIdentity::Complete(large.clone()),
            &large_placement,
        ));
        let PayloadPlacement::Large(selected_large) = &large_placement else {
            panic!("expected erasure placement")
        };
        let shard = selected_large.shards()[0];
        assert!(artifact_is_selected(
            shard.owner(),
            &PayloadArtifactIdentity::Shard(ShardIdentity::new(large, shard.ordinal())),
            &large_placement,
        ));
    }

    #[test]
    fn zero_maintenance_budgets_are_rejected() {
        assert!(matches!(
            PayloadGcBudget::new(
                0,
                MAX_ARTIFACT_METADATA_RECORD_BYTES,
                Duration::from_millis(1),
            ),
            Err(PayloadGcError::InvalidBudget)
        ));
        assert!(matches!(
            PayloadGcBudget::new(1, 0, Duration::from_millis(1)),
            Err(PayloadGcError::InvalidBudget)
        ));
        assert!(
            PayloadGcBudget::new(
                1,
                MAX_ARTIFACT_METADATA_RECORD_BYTES,
                Duration::from_millis(1),
            )
            .is_ok()
        );
    }
}
