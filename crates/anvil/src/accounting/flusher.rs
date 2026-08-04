//! Periodic persistence of node-local public traffic counters as ordinary objects.

use std::time::Duration;

use anvil_consensus::{DecisionRaft, NodeId};
use anvil_store::Store;

use crate::cluster_peer::{AccountingTrafficFlush, ClusterPeerTransport, RoutedAccountingHandler};
use crate::cluster_placement::ClusterPlacement;
use crate::index_runtime::placement::{IndexIdentity, IndexPlacement};

use super::{AccountingCatalog, AccountingServiceImpl, AccountingTraffic};

const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct AccountingTrafficTask {
    task: tokio::task::JoinHandle<()>,
}

impl AccountingTrafficTask {
    pub(crate) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        store: Store,
        peers: ClusterPeerTransport,
        catalog: AccountingCatalog,
        traffic: AccountingTraffic,
        service: AccountingServiceImpl,
    ) -> Self {
        let task = tokio::spawn(async move {
            let source = match store.local_watch_status() {
                Ok(status) => status.source_id,
                Err(error) => {
                    tracing::error!(%error, "accounting traffic source identity is unavailable");
                    return;
                }
            };
            let mut sequence = 0_u64;
            let mut interval = tokio::time::interval(FLUSH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                for (accounting_id, delta) in traffic.pending() {
                    let Some(definition) = catalog.get(accounting_id).ok().flatten() else {
                        traffic.acknowledge(accounting_id, delta);
                        continue;
                    };
                    let placement = match placement(&decisions) {
                        Ok(value) => value,
                        Err(error) => {
                            tracing::warn!(%error, "accounting traffic placement is unavailable");
                            continue;
                        }
                    };
                    let identity = match IndexIdentity::new(
                        definition.tenant_id,
                        definition.bucket_id,
                        accounting_id,
                    ) {
                        Ok(value) => value,
                        Err(error) => {
                            tracing::warn!(%error, "accounting traffic identity is invalid");
                            continue;
                        }
                    };
                    let assignment = match IndexPlacement::derive(identity, &placement) {
                        Ok(value) => value,
                        Err(error) => {
                            tracing::warn!(%error, "accounting traffic worker is unavailable");
                            continue;
                        }
                    };
                    sequence = sequence.wrapping_add(1).max(1);
                    let value = AccountingTrafficFlush {
                        accounting_id,
                        source_node: local_node,
                        accepted_inbound_bytes: delta.accepted_inbound_bytes,
                        served_outbound_bytes: delta.served_outbound_bytes,
                        flush_id: format!(
                            "accounting-traffic-{}-{}-{}",
                            local_node.0,
                            hex::encode(&source.source_epoch[..8]),
                            sequence
                        ),
                    };
                    let result = if assignment.builder() == local_node {
                        service.flush(local_node, placement.clone(), value).await
                    } else if let Some(address) = placement.address(assignment.builder()) {
                        peers
                            .flush_accounting_traffic(assignment.builder(), &address.0, &value)
                            .await
                    } else {
                        Err(tonic::Status::unavailable(
                            "accounting traffic worker has no peer address",
                        ))
                    };
                    match result {
                        Ok(_) => traffic.acknowledge(accounting_id, delta),
                        Err(error) => tracing::warn!(
                            accounting.id = accounting_id,
                            %error,
                            "accounting traffic flush will retry"
                        ),
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
