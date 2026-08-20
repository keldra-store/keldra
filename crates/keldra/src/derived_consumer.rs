//! Aggregate index/accounting retention checkpoints.
//!
//! Per-definition barriers remain in ordinary generation or rollup objects.
//! This module computes one conservative cursor per source and consumer kind;
//! it never persists a per-definition checkpoint catalogue.

use keldra_consensus::{DecisionRaft, NodeId};
use keldra_store::{
    DefinitionCheckpoint, DerivedConsumerCheckpoint, DerivedConsumerKind, Store, WatchJournalStatus,
};
use tonic::Status;
use tracing::Instrument;

use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;
#[path = "derived_consumer/assigned.rs"]
mod assigned;
#[path = "derived_consumer/tracker.rs"]
mod tracker;
pub(crate) use tracker::{
    DerivedBarrierEvidence, DerivedDefinitionIdentity, SparseDerivedInventory,
    SparseDerivedTracker, retention_kind,
};
#[path = "derived_consumer/runtime.rs"]
mod runtime;
pub(crate) use runtime::{
    DerivedConsumerRuntimeTask, DerivedEvidenceResolver, DerivedProgressReporter,
};

#[derive(Clone)]
pub(crate) struct DerivedCheckpointPublisher {
    local_node: NodeId,
    decisions: DecisionRaft,
    store: Store,
    peers: DataPeerTransport,
}

pub(crate) struct DerivedConsumerFenceTask {
    task: tokio::task::JoinHandle<()>,
}

impl DerivedConsumerFenceTask {
    pub(crate) fn start(publisher: DerivedCheckpointPublisher) -> Self {
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut installed_fence = None;
            let mut last_failure_log = None;
            loop {
                interval.tick().await;
                let current_fence = match publisher.placement() {
                    Ok(placement) => placement.fence(),
                    Err(error) => {
                        log_fence_failure(&mut last_failure_log, &error);
                        continue;
                    }
                };
                if installed_fence == Some(current_fence) {
                    continue;
                }
                if let Err(error) = publisher.fence_local_source().await {
                    log_fence_failure(&mut last_failure_log, &error);
                    continue;
                }
                installed_fence = Some(current_fence);
                last_failure_log = None;
            }
        });
        Self { task }
    }
}

const FENCE_FAILURE_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

fn log_fence_failure(last: &mut Option<std::time::Instant>, error: &Status) {
    let now = std::time::Instant::now();
    if last.is_none_or(|last| now.duration_since(last) >= FENCE_FAILURE_LOG_INTERVAL) {
        tracing::warn!(%error, "derived-consumer membership fence will retry");
        *last = Some(now);
    }
}

impl Drop for DerivedConsumerFenceTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl DerivedCheckpointPublisher {
    pub(crate) fn new(
        local_node: NodeId,
        decisions: DecisionRaft,
        store: Store,
        peers: DataPeerTransport,
    ) -> Self {
        Self {
            local_node,
            decisions,
            store,
            peers,
        }
    }

    pub(crate) fn local_checkpoint(
        &self,
        kind: DerivedConsumerKind,
        source_node_id: u16,
    ) -> Result<Option<DefinitionCheckpoint>, Status> {
        self.store
            .definition_checkpoint(retention_kind(kind), source_node_id)
            .map_err(|error| Status::internal(error.to_string()))
    }

    pub(crate) async fn publish_tracker(
        &self,
        tracker: &mut SparseDerivedTracker,
    ) -> Result<(), Status> {
        let checkpoints = tracker
            .checkpoints()
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        for checkpoint in checkpoints {
            self.publish(checkpoint).await?;
            tracker
                .acknowledge(checkpoint)
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
        }
        Ok(())
    }

    pub(crate) async fn publish(
        &self,
        checkpoint: DerivedConsumerCheckpoint,
    ) -> Result<(), Status> {
        let started = std::time::Instant::now();
        let kind = consumer_kind(checkpoint.consumer_kind);
        let span = tracing::info_span!(
            "keldra.derived_consumer.checkpoint",
            consumer.kind = kind,
            consumer.node_id = checkpoint.consumer_node_id,
            source.node_id = checkpoint.source_id.node_id,
            source.next_offset = checkpoint.next_offset,
            membership.term = checkpoint.observed_fence.term,
            membership.index = checkpoint.observed_fence.index,
            checkpoint.route = tracing::field::Empty,
            checkpoint.outcome = tracing::field::Empty,
            checkpoint.elapsed_seconds = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        span.in_scope(|| {
            tracing::info!(
                consumer.kind = kind,
                counter.keldra_derived_checkpoint_publish_active = 1_i64,
                monotonic_counter.keldra_derived_checkpoint_publish_attempts_total = 1_u64,
                "aggregate checkpoint publication started"
            );
        });
        let result = self
            .publish_inner(checkpoint)
            .instrument(span.clone())
            .await;
        let elapsed_seconds = started.elapsed().as_secs_f64();
        let (route, outcome) = match &result {
            Ok(route) => (*route, "completed"),
            Err(_) => ("unknown", "failed"),
        };
        span.record("checkpoint.route", route);
        span.record("checkpoint.outcome", outcome);
        span.record("checkpoint.elapsed_seconds", elapsed_seconds);
        span.record(
            "otel.status_code",
            if result.is_err() { "error" } else { "ok" },
        );
        span.in_scope(|| {
            tracing::info!(
                consumer.kind = kind,
                counter.keldra_derived_checkpoint_publish_active = -1_i64,
                "aggregate checkpoint publication released"
            );
            tracing::info!(
                consumer.kind = kind,
                checkpoint.route = route,
                checkpoint.outcome = outcome,
                monotonic_counter.keldra_derived_checkpoint_publish_failures_total =
                    u64::from(result.is_err()),
                histogram.keldra_derived_checkpoint_publish_duration_seconds = elapsed_seconds,
                "aggregate checkpoint publication finished"
            );
        });
        result.map(|_| ())
    }

    async fn publish_inner(
        &self,
        checkpoint: DerivedConsumerCheckpoint,
    ) -> Result<&'static str, Status> {
        if u64::from(checkpoint.consumer_node_id) != self.local_node.0 {
            return Err(Status::invalid_argument(
                "aggregate checkpoint belongs to another consumer node",
            ));
        }
        let placement = self.placement()?;
        if placement.fence() != checkpoint.observed_fence {
            return Err(Status::unavailable(
                "aggregate checkpoint carries a stale membership fence",
            ));
        }
        let active_nodes = active_source_nodes(&placement)?;
        if active_nodes
            .binary_search(&checkpoint.consumer_node_id)
            .is_err()
        {
            return Err(Status::failed_precondition(
                "aggregate checkpoint consumer is not ACTIVE",
            ));
        }
        let source_node = NodeId(u64::from(checkpoint.source_id.node_id));
        let address = placement.address(source_node).ok_or_else(|| {
            Status::failed_precondition("aggregate checkpoint source is not ACTIVE")
        })?;

        let local = DefinitionCheckpoint {
            consumer_kind: retention_kind(checkpoint.consumer_kind),
            source_id: checkpoint.source_id,
            next_offset: checkpoint.next_offset,
            observed_fence: checkpoint.observed_fence,
        };
        self.store
            .apply_definition_assignment_page(&[], &local)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if source_node == self.local_node {
            self.store
                .apply_derived_consumer_checkpoint(checkpoint, &active_nodes)
                .await
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            self.require_checkpoint_fence(checkpoint.observed_fence)?;
            Ok("local")
        } else {
            self.peers
                .apply_derived_consumer_checkpoint(source_node, &address.0, checkpoint)
                .await?;
            self.require_checkpoint_fence(checkpoint.observed_fence)?;
            Ok("remote")
        }
    }

    fn require_checkpoint_fence(
        &self,
        observed_fence: keldra_store::PlacementLogId,
    ) -> Result<(), Status> {
        if self.placement()?.fence() != observed_fence {
            return Err(Status::unavailable(
                "membership changed while publishing an aggregate checkpoint",
            ));
        }
        Ok(())
    }

    pub(crate) async fn fence_local_source(&self) -> Result<(), Status> {
        let started = std::time::Instant::now();
        let span = tracing::info_span!(
            "keldra.derived_consumer.membership_fence",
            source.node_id = self.local_node.0,
            membership.term = tracing::field::Empty,
            membership.index = tracing::field::Empty,
            membership.active_nodes = tracing::field::Empty,
            fence.outcome = tracing::field::Empty,
            fence.elapsed_seconds = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let result = self
            .fence_local_source_inner(&span)
            .instrument(span.clone())
            .await;
        let elapsed_seconds = started.elapsed().as_secs_f64();
        span.record(
            "fence.outcome",
            if result.is_err() {
                "failed"
            } else {
                "completed"
            },
        );
        span.record("fence.elapsed_seconds", elapsed_seconds);
        span.record(
            "otel.status_code",
            if result.is_err() { "error" } else { "ok" },
        );
        span.in_scope(|| {
            tracing::info!(
                fence.outcome = if result.is_err() {
                    "failed"
                } else {
                    "completed"
                },
                monotonic_counter.keldra_derived_fence_attempts_total = 1_u64,
                monotonic_counter.keldra_derived_fence_failures_total = u64::from(result.is_err()),
                histogram.keldra_derived_fence_duration_seconds = elapsed_seconds,
                "derived-consumer membership fence finished"
            );
        });
        result
    }

    async fn fence_local_source_inner(&self, span: &tracing::Span) -> Result<(), Status> {
        let placement = self.placement()?;
        let active_nodes = active_source_nodes(&placement)?;
        span.record("membership.term", placement.fence().term);
        span.record("membership.index", placement.fence().index);
        span.record("membership.active_nodes", active_nodes.len());
        self.store
            .ensure_derived_consumer_membership(placement.fence(), &active_nodes)
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if self.placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "membership changed while fencing derived consumers",
            ));
        }
        Ok(())
    }

    pub(crate) async fn source_statuses(&self) -> Result<Vec<WatchJournalStatus>, Status> {
        let placement = self.placement()?;
        let mut statuses = Vec::with_capacity(placement.active_node_ids().len());
        for node in placement.active_node_ids() {
            let status = if node == self.local_node {
                let store = self.store.clone();
                tokio::task::spawn_blocking(move || store.local_watch_status())
                    .await
                    .map_err(|error| Status::internal(format!("source status task: {error}")))?
                    .map_err(|error| Status::failed_precondition(error.to_string()))?
            } else {
                let address = placement
                    .address(node)
                    .ok_or_else(|| Status::unavailable("ACTIVE source has no peer address"))?;
                self.peers.source_journal_status(node, &address.0).await?
            };
            if u64::from(status.source_id.node_id) != node.0 {
                return Err(Status::data_loss("source status belongs to another node"));
            }
            statuses.push(status);
        }
        if self.placement()?.fence() != placement.fence() {
            return Err(Status::unavailable(
                "membership changed while reading source status",
            ));
        }
        Ok(statuses)
    }

    pub(crate) fn placement(&self) -> Result<ClusterPlacement, Status> {
        let state = self
            .decisions
            .state()
            .map_err(|error| Status::unavailable(error.to_string()))?;
        ClusterPlacement::from_applied(&state)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

const fn consumer_kind(kind: DerivedConsumerKind) -> &'static str {
    match kind {
        DerivedConsumerKind::Index => "index",
        DerivedConsumerKind::Accounting => "accounting",
    }
}

fn active_source_nodes(placement: &ClusterPlacement) -> Result<Vec<u16>, Status> {
    let mut nodes = placement
        .active_node_ids()
        .into_iter()
        .map(|node| {
            u16::try_from(node.0)
                .map_err(|_| Status::data_loss("ACTIVE node exceeds source-journal identity range"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    nodes.sort_unstable();
    Ok(nodes)
}
