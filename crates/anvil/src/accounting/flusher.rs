//! Bounded delivery of process-local public traffic to each bucket matcher.

use std::time::Duration;

use anvil_consensus::{DecisionRaft, NodeId};

use crate::cluster_peer::{
    AccountingTrafficBatch, AccountingTrafficEntry, ClusterPeerTransport, RoutedAccountingHandler,
};
use crate::cluster_placement::ClusterPlacement;

use super::{AccountingServiceImpl, AccountingTraffic, matcher_node};

const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct AccountingTrafficTask {
    task: tokio::task::JoinHandle<()>,
}

impl AccountingTrafficTask {
    pub(crate) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        peers: ClusterPeerTransport,
        traffic: AccountingTraffic,
        service: AccountingServiceImpl,
    ) -> Self {
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(FLUSH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                for pending in traffic.pending() {
                    let placement = match placement(&decisions) {
                        Ok(value) => value,
                        Err(error) => {
                            emit_retry(&pending, &error);
                            break;
                        }
                    };
                    let matcher =
                        match matcher_node(&placement, pending.tenant_id, pending.bucket_id) {
                            Ok(value) => value,
                            Err(error) => {
                                emit_retry(&pending, &error);
                                break;
                            }
                        };
                    let value = AccountingTrafficBatch {
                        source_node: pending.id.source_node,
                        source_epoch: pending.id.source_epoch,
                        sequence: pending.id.sequence,
                        tenant_id: pending.tenant_id,
                        bucket_id: pending.bucket_id,
                        entries: pending
                            .entries
                            .iter()
                            .map(|entry| AccountingTrafficEntry {
                                exact_path: entry.path.clone(),
                                accepted_inbound_bytes: entry.accepted_inbound_bytes,
                                served_outbound_bytes: entry.served_outbound_bytes,
                            })
                            .collect(),
                    };
                    let result = if matcher == local_node {
                        service
                            .match_traffic(local_node, placement.clone(), value)
                            .await
                    } else if let Some(address) = placement.address(matcher) {
                        peers
                            .match_accounting_traffic(matcher, &address.0, &value)
                            .await
                    } else {
                        Err(tonic::Status::unavailable(
                            "accounting matcher has no peer address",
                        ))
                    };
                    match result {
                        Ok(()) => traffic.acknowledge(&pending.id),
                        Err(error) => {
                            emit_retry(&pending, &error);
                            break;
                        }
                    }
                }
            }
        });
        Self { task }
    }
}

impl Drop for AccountingTrafficTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, tonic::Status> {
    let state = decisions
        .state()
        .map_err(|_| tonic::Status::unavailable("applied cluster membership is unavailable"))?;
    ClusterPlacement::from_applied(&state)
        .map_err(|error| tonic::Status::unavailable(error.to_string()))
}

fn emit_retry(batch: &super::traffic::TrafficBatch, error: &tonic::Status) {
    tracing::warn!(
        tenant.id = batch.tenant_id,
        bucket.id = batch.bucket_id,
        monotonic_counter.anvil_accounting_matcher_retries_total = 1_u64,
        %error,
        "accounting traffic batch will retry"
    );
}
