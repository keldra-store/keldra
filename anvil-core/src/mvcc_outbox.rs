//! Continuous delivery of committed MVCC outbox rows.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::{
    core_store::{AppendStreamRecord, CoreStore, StreamAppendReceipt},
    mvcc_store::{LocalMvccStore, OutboxRecord},
    mvcc_transaction::NodeIncarnation,
    observability::{
        MVCC_OUTBOX_BACKLOG, MVCC_OUTBOX_FAILURES, MVCC_OUTBOX_OLDEST_AGE_MS, Observability,
    },
};
use anvil_mvcc_consensus::{Consensus, NodeId, OpenRaftConsensus};

const OUTBOX_SCHEMA: &str = "anvil.mvcc.outbox.stream.v1";

/// A transaction-owned event destined for Anvil's durable internal stream
/// path. `partition_id` is the compact Raft assignment; `stream_partition` is
/// the downstream CoreStore partition identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamOutboxEvent {
    pub schema: String,
    pub partition_id: u64,
    pub stream_id: String,
    pub stream_partition: String,
    pub record_kind: String,
    pub payload: Vec<u8>,
    pub content_type: Option<String>,
    pub user_metadata_json: String,
}

impl StreamOutboxEvent {
    pub fn new(
        partition_id: u64,
        stream_id: impl Into<String>,
        stream_partition: impl Into<String>,
        record_kind: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<Self> {
        let event = Self {
            schema: OUTBOX_SCHEMA.into(),
            partition_id,
            stream_id: stream_id.into(),
            stream_partition: stream_partition.into(),
            record_kind: record_kind.into(),
            payload,
            content_type: None,
            user_metadata_json: "{}".into(),
        };
        event.validate()?;
        Ok(event)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn decode(payload: &[u8]) -> Result<Self> {
        let event: Self = serde_json::from_slice(payload).context("decode stream outbox event")?;
        event.validate()?;
        Ok(event)
    }

    fn validate(&self) -> Result<()> {
        if self.schema != OUTBOX_SCHEMA
            || self.partition_id == 0
            || self.stream_id.trim().is_empty()
            || self.stream_partition.trim().is_empty()
            || self.record_kind.trim().is_empty()
        {
            bail!("invalid MVCC stream outbox event");
        }
        Ok(())
    }
}

/// Stable compact-control partition identity for one downstream stream
/// partition. Operators assign this value through Raft before transactions may
/// target it.
pub fn stream_partition_id(stream_partition: &str) -> Result<u64> {
    if stream_partition.trim().is_empty() {
        bail!("stream partition is required");
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.mvcc.outbox-stream-partition.v1");
    hasher.update(&(stream_partition.len() as u64).to_be_bytes());
    hasher.update(stream_partition.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    let id = u64::from_be_bytes(bytes);
    if id == 0 { Ok(1) } else { Ok(id) }
}

#[derive(Clone)]
pub struct DurableStreamOutboxConsumer {
    core_store: CoreStore,
}

impl DurableStreamOutboxConsumer {
    pub fn new(core_store: CoreStore) -> Self {
        Self { core_store }
    }

    async fn dispatch(
        &self,
        record: &OutboxRecord,
        event: StreamOutboxEvent,
    ) -> Result<StreamAppendReceipt> {
        // The deterministic event ID is the downstream idempotency key. A
        // crash after the append's durable receipt but before outbox completion
        // therefore replays the same append rather than creating a duplicate.
        self.core_store
            .append_stream(AppendStreamRecord {
                stream_id: event.stream_id,
                partition_id: event.stream_partition,
                record_kind: event.record_kind,
                payload: event.payload,
                content_type: event.content_type,
                user_metadata_json: event.user_metadata_json,
                fence: None,
                transaction_id: None,
                idempotency_key: Some(record.event_id.clone()),
            })
            .await
            .context("durably append internal outbox event")
    }
}

pub struct MvccOutboxRunner {
    store: LocalMvccStore,
    consensus: Arc<OpenRaftConsensus>,
    local_node: anvil_mvcc_consensus::NodeIncarnation,
    consumer: DurableStreamOutboxConsumer,
    observability: Observability,
    worker_id: String,
    lease_ms: u64,
}

impl MvccOutboxRunner {
    pub fn new(
        store: LocalMvccStore,
        consensus: Arc<OpenRaftConsensus>,
        local_node_id: NodeId,
        local_incarnation: NodeIncarnation,
        core_store: CoreStore,
        observability: Observability,
    ) -> Result<Self> {
        if local_node_id.0 == 0 {
            bail!("outbox runner requires a non-zero compact control node ID");
        }
        Ok(Self {
            store,
            consensus,
            local_node: anvil_mvcc_consensus::NodeIncarnation {
                node_id: local_node_id,
                incarnation: local_incarnation.incarnation,
            },
            consumer: DurableStreamOutboxConsumer::new(core_store),
            observability,
            worker_id: format!(
                "mvcc-outbox/{}/{}",
                local_incarnation.node_id, local_incarnation.incarnation
            ),
            lease_ms: 30_000,
        })
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            if let Err(error) = self.run_once().await {
                tracing::warn!(error = %error, "MVCC outbox delivery pass failed");
            }
            tokio::select! {
                _ = shutdown.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }

    pub async fn run_once(&self) -> Result<bool> {
        let now = unix_ms();
        self.publish_metrics(now)?;
        let snapshot = self.linearized_control_snapshot().await?;
        let record =
            self.store
                .claim_outbox_where(&self.worker_id, now, self.lease_ms, |record| {
                    StreamOutboxEvent::decode(&record.payload)
                        .ok()
                        .and_then(|event| {
                            snapshot
                                .partitions
                                .iter()
                                .find(|(partition, _)| *partition == event.partition_id)
                        })
                        .is_some_and(|(_, assignment)| assignment.owner == self.local_node)
                })?;
        let Some(record) = record else {
            return Ok(false);
        };
        let event = StreamOutboxEvent::decode(&record.payload)?;
        let guard = Self::assignment_guard(&snapshot, &self.local_node, event.partition_id)
            .context("outbox assignment changed after claim")?;
        let lease_owner = guard.lease_owner(&self.worker_id);
        let record = self
            .store
            .rebind_outbox_lease(&record, &self.worker_id, &lease_owner)?;
        let dispatch = tokio::time::timeout(
            Duration::from_millis(self.lease_ms.saturating_sub(1_000)),
            self.consumer.dispatch(&record, event.clone()),
        )
        .await;
        let receipt = match dispatch {
            Ok(Ok(receipt)) => receipt,
            Ok(Err(error)) => return self.retry(&record, &lease_owner, now, error),
            Err(_) => {
                return self.retry(
                    &record,
                    &lease_owner,
                    now,
                    anyhow!("outbox dispatch lease timeout"),
                );
            }
        };
        if receipt.stream_id != event.stream_id {
            return self.retry(
                &record,
                &lease_owner,
                now,
                anyhow!("downstream durable ACK named another stream"),
            );
        }

        // Assignment may change while CoreStore durably appends. In that case
        // leave the lease to expire. The new owner will replay the
        // deterministic downstream idempotency key and receive the same ACK.
        if !self.still_assigned(&guard).await? {
            return Ok(false);
        }
        let completed_at = unix_ms();
        self.store
            .complete_outbox_at(&record, &lease_owner, completed_at)?;
        self.publish_metrics(completed_at)?;
        Ok(true)
    }

    fn assignment_guard(
        snapshot: &anvil_mvcc_consensus::AppliedControlSnapshot,
        local_node: &anvil_mvcc_consensus::NodeIncarnation,
        partition_id: u64,
    ) -> Option<crate::mvcc_worker_authority::AssignmentGuard> {
        snapshot
            .partitions
            .iter()
            .find(|(id, assignment)| *id == partition_id && assignment.owner == *local_node)
            .map(
                |(_, assignment)| crate::mvcc_worker_authority::AssignmentGuard {
                    partition_id,
                    assignment_epoch: assignment.epoch,
                    topology_epoch: snapshot.topology_epoch,
                    owner: NodeIncarnation {
                        node_id: local_node.node_id.0.to_string(),
                        incarnation: local_node.incarnation,
                    },
                },
            )
    }

    async fn still_assigned(
        &self,
        guard: &crate::mvcc_worker_authority::AssignmentGuard,
    ) -> Result<bool> {
        let snapshot = self.linearized_control_snapshot().await?;
        Ok(snapshot.topology_epoch == guard.topology_epoch
            && snapshot.partitions.iter().any(|(id, assignment)| {
                *id == guard.partition_id
                    && assignment.epoch == guard.assignment_epoch
                    && assignment.owner == self.local_node
            }))
    }

    async fn linearized_control_snapshot(
        &self,
    ) -> Result<anvil_mvcc_consensus::AppliedControlSnapshot> {
        let target = self.consensus.linearized_read_barrier().await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.consensus.observed_commit_version() >= target {
                    return self.consensus.applied_control_snapshot();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("local control state did not reach the linearized outbox assignment")?
    }

    fn retry(
        &self,
        record: &OutboxRecord,
        lease_owner: &str,
        now: u64,
        error: anyhow::Error,
    ) -> Result<bool> {
        let exponent = record.attempts.saturating_sub(1).min(10);
        let delay_ms = 100u64.saturating_mul(1u64 << exponent);
        self.store.retry_outbox(
            record,
            lease_owner,
            now.saturating_add(delay_ms),
            &error.to_string(),
        )?;
        self.observability
            .increment_counter(MVCC_OUTBOX_FAILURES, &[("cluster", "local")]);
        Ok(false)
    }

    fn publish_metrics(&self, now: u64) -> Result<()> {
        let (backlog, oldest_age_ms, failures) = self.store.outbox_backlog(now)?;
        let labels = [("cluster", "local")];
        self.observability.set_gauge(
            MVCC_OUTBOX_BACKLOG,
            &labels,
            i64::try_from(backlog).unwrap_or(i64::MAX),
        );
        self.observability.set_gauge(
            MVCC_OUTBOX_OLDEST_AGE_MS,
            &labels,
            i64::try_from(oldest_age_ms).unwrap_or(i64::MAX),
        );
        self.observability.set_gauge(
            MVCC_OUTBOX_FAILURES,
            &labels,
            i64::try_from(failures).unwrap_or(i64::MAX),
        );
        Ok(())
    }
}

fn unix_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_event_encoding_is_strict_and_round_trips() {
        let event =
            StreamOutboxEvent::new(7, "events", "partition-7", "account.changed", vec![1]).unwrap();
        assert_eq!(
            StreamOutboxEvent::decode(&event.encode().unwrap()).unwrap(),
            event
        );
        assert!(StreamOutboxEvent::decode(b"event").is_err());
    }
}
