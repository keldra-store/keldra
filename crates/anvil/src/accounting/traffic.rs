use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anvil_consensus::NodeId;
use anvil_store::SourceId;

use crate::cluster_peer::{
    MAX_ACCOUNTING_TRAFFIC_ENTRIES, MAX_ACCOUNTING_TRAFFIC_LOGICAL_BYTES,
    accounting_traffic_entry_logical_bytes,
};

const DEFAULT_MAX_PENDING_BATCHES: usize = 4_096;
const DEFAULT_MAX_PENDING_ENTRIES: usize = 65_536;
const DEFAULT_MAX_PENDING_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_FLUSH_BATCHES: usize = 64;

/// Startup-only bounds for disposable bandwidth observations.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AccountingTrafficConfig {
    pub(crate) max_pending_batches: usize,
    pub(crate) max_pending_entries: usize,
    pub(crate) max_pending_bytes: u64,
    pub(crate) max_batch_entries: usize,
    pub(crate) max_batch_bytes: u64,
    pub(crate) flush_batches: usize,
}

impl AccountingTrafficConfig {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        max_pending_batches: usize,
        max_pending_entries: usize,
        max_pending_bytes: u64,
        max_batch_entries: usize,
        max_batch_bytes: u64,
        flush_batches: usize,
    ) -> Option<Self> {
        (max_pending_batches != 0
            && max_pending_entries != 0
            && max_pending_bytes != 0
            && max_batch_entries != 0
            && max_batch_entries <= MAX_ACCOUNTING_TRAFFIC_ENTRIES
            && max_batch_bytes != 0
            && max_batch_bytes <= MAX_ACCOUNTING_TRAFFIC_LOGICAL_BYTES
            && flush_batches != 0)
            .then_some(Self {
                max_pending_batches,
                max_pending_entries,
                max_pending_bytes,
                max_batch_entries,
                max_batch_bytes,
                flush_batches,
            })
    }
}

impl Default for AccountingTrafficConfig {
    fn default() -> Self {
        Self {
            max_pending_batches: DEFAULT_MAX_PENDING_BATCHES,
            max_pending_entries: DEFAULT_MAX_PENDING_ENTRIES,
            max_pending_bytes: DEFAULT_MAX_PENDING_BYTES,
            max_batch_entries: MAX_ACCOUNTING_TRAFFIC_ENTRIES,
            max_batch_bytes: MAX_ACCOUNTING_TRAFFIC_LOGICAL_BYTES,
            flush_batches: DEFAULT_FLUSH_BATCHES,
        }
    }
}

/// Bounded, process-local ingress queue. Losing it may lose a small amount of
/// bandwidth telemetry, but never affects exact object-count or stored-byte
/// accounting.
#[derive(Clone)]
pub(crate) struct AccountingTraffic {
    source: SourceId,
    config: AccountingTrafficConfig,
    pending: Arc<Mutex<TrafficQueue>>,
    dropped: Arc<TrafficDropState>,
}

impl AccountingTraffic {
    pub(crate) fn new(source: SourceId, config: AccountingTrafficConfig) -> Self {
        // Journal source epochs survive a normal restart, whereas this queue is
        // deliberately process-local. Give each process incarnation its own
        // batch epoch so sequence numbers restarted at one cannot replay an
        // earlier process's acknowledged traffic command IDs.
        let mut hasher = blake3::Hasher::new_derive_key("anvil 0.7 accounting traffic batch epoch");
        hasher.update(&source.source_epoch);
        hasher.update(uuid::Uuid::new_v4().as_bytes());
        let source = SourceId {
            source_epoch: *hasher.finalize().as_bytes(),
            ..source
        };
        let traffic = Self {
            source,
            config,
            pending: Arc::new(Mutex::new(TrafficQueue::default())),
            dropped: Arc::new(TrafficDropState::default()),
        };
        traffic.emit_drop_state(0, 0, None);
        traffic
    }

    pub(crate) fn record_inbound(&self, tenant_id: u64, bucket_id: u64, path: &str, bytes: u64) {
        self.record(tenant_id, bucket_id, path, bytes, 0);
    }

    pub(crate) fn record_outbound(&self, tenant_id: u64, bucket_id: u64, path: &str, bytes: u64) {
        self.record(tenant_id, bucket_id, path, 0, bytes);
    }

    /// Records one request's accepted inbound bytes while acquiring the
    /// disposable traffic queue once. Object durability never depends on this
    /// best-effort accounting path.
    pub(crate) fn record_inbound_batch<'a>(
        &self,
        entries: impl IntoIterator<Item = (u64, u64, &'a str, u64)>,
    ) {
        self.record_batch(
            entries
                .into_iter()
                .map(|(tenant_id, bucket_id, path, bytes)| (tenant_id, bucket_id, path, bytes, 0)),
        );
    }

    pub(crate) fn record_resolution_drop(&self, inbound: u64, outbound: u64) {
        self.emit_dropped(inbound, outbound, "stable bucket identity was unavailable");
    }

    fn record(&self, tenant_id: u64, bucket_id: u64, path: &str, inbound: u64, outbound: u64) {
        self.record_batch(std::iter::once((
            tenant_id, bucket_id, path, inbound, outbound,
        )));
    }

    fn record_batch<'a>(&self, entries: impl IntoIterator<Item = (u64, u64, &'a str, u64, u64)>) {
        let Ok(mut pending) = self.pending.lock() else {
            let (inbound, outbound) = entries.into_iter().fold(
                (0_u64, 0_u64),
                |(inbound_total, outbound_total), (_, _, _, inbound, outbound)| {
                    (
                        inbound_total.saturating_add(inbound),
                        outbound_total.saturating_add(outbound),
                    )
                },
            );
            self.emit_dropped(inbound, outbound, "traffic queue lock is poisoned");
            return;
        };
        let mut accepted = false;
        for (tenant_id, bucket_id, path, inbound, outbound) in entries {
            if inbound == 0 && outbound == 0 {
                continue;
            }
            if tenant_id == 0 || bucket_id == 0 || path.is_empty() {
                self.emit_dropped(inbound, outbound, "traffic observation identity is invalid");
                continue;
            }
            let entry = TrafficEntry {
                path: path.to_owned(),
                accepted_inbound_bytes: inbound,
                served_outbound_bytes: outbound,
            };
            let encoded_bytes = entry.encoded_bytes();
            if encoded_bytes > self.config.max_batch_bytes {
                self.emit_dropped(
                    inbound,
                    outbound,
                    "traffic observation exceeds the batch byte bound",
                );
                continue;
            }
            if pending.total_entries >= self.config.max_pending_entries
                || pending.total_bytes.saturating_add(encoded_bytes) > self.config.max_pending_bytes
            {
                self.emit_dropped(inbound, outbound, "traffic queue capacity is exhausted");
                continue;
            }
            let bucket = BucketIdentity {
                tenant_id,
                bucket_id,
            };
            let must_rotate = pending.open.get(&bucket).is_some_and(|batch| {
                batch.entries.len() >= self.config.max_batch_entries
                    || batch.encoded_bytes.saturating_add(encoded_bytes)
                        > self.config.max_batch_bytes
            });
            if must_rotate && !pending.seal(bucket, self.config.max_pending_batches) {
                self.emit_dropped(
                    inbound,
                    outbound,
                    "traffic batch queue capacity is exhausted",
                );
                continue;
            }
            if !pending.open.contains_key(&bucket) {
                if pending.ready.len().saturating_add(pending.open.len())
                    >= self.config.max_pending_batches
                {
                    self.emit_dropped(
                        inbound,
                        outbound,
                        "traffic batch queue capacity is exhausted",
                    );
                    continue;
                }
                pending.next_sequence = pending.next_sequence.wrapping_add(1).max(1);
                let sequence = pending.next_sequence;
                pending
                    .open
                    .insert(bucket, TrafficBatch::new(self.source, bucket, sequence));
            }
            let batch = pending.open.get_mut(&bucket).expect("open batch exists");
            batch.encoded_bytes = batch.encoded_bytes.saturating_add(encoded_bytes);
            batch.entries.push(entry);
            pending.total_entries += 1;
            pending.total_bytes = pending.total_bytes.saturating_add(encoded_bytes);
            accepted = true;
        }
        if accepted {
            emit_pending(&pending);
        }
    }

    pub(crate) fn pending(&self) -> Vec<TrafficBatch> {
        let Ok(mut pending) = self.pending.lock() else {
            tracing::error!("usage accounting traffic queue lock is poisoned");
            return Vec::new();
        };
        pending.seal_all(self.config.max_pending_batches);
        emit_pending(&pending);
        pending
            .ready
            .iter()
            .take(self.config.flush_batches)
            .cloned()
            .collect()
    }

    pub(crate) fn acknowledge(&self, id: &TrafficBatchId) {
        let Ok(mut pending) = self.pending.lock() else {
            tracing::error!("usage accounting traffic queue lock is poisoned");
            return;
        };
        if pending.ready.front().is_none_or(|batch| &batch.id != id) {
            return;
        }
        let batch = pending.ready.pop_front().expect("front batch was checked");
        pending.total_entries = pending.total_entries.saturating_sub(batch.entries.len());
        pending.total_bytes = pending.total_bytes.saturating_sub(batch.encoded_bytes);
        emit_pending(&pending);
    }

    fn emit_dropped(&self, inbound: u64, outbound: u64, reason: &'static str) {
        self.emit_drop_state(1, inbound.saturating_add(outbound), Some(reason));
    }

    fn emit_drop_state(&self, batches: u64, bytes: u64, reason: Option<&'static str>) {
        let (dropped_batches_total, dropped_bytes_total) = {
            let mut totals = self
                .dropped
                .totals
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            totals.batches = totals.batches.saturating_add(batches);
            totals.bytes = totals.bytes.saturating_add(bytes);
            (totals.batches, totals.bytes)
        };
        match reason {
            Some(reason) => tracing::warn!(
                node_id = self.source.node_id,
                dropped_batches_total,
                dropped_bytes_total,
                monotonic_counter.anvil_accounting_traffic_dropped_batches_total = batches,
                monotonic_counter.anvil_accounting_traffic_dropped_bytes_total = bytes,
                reason,
                "anvil_accounting_traffic_drop_state"
            ),
            None => tracing::info!(
                node_id = self.source.node_id,
                dropped_batches_total,
                dropped_bytes_total,
                "anvil_accounting_traffic_drop_state"
            ),
        }
    }

    #[cfg(test)]
    fn dropped_totals(&self) -> (u64, u64) {
        let totals = self
            .dropped
            .totals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (totals.batches, totals.bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BucketIdentity {
    tenant_id: u64,
    bucket_id: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct TrafficBatch {
    pub(crate) id: TrafficBatchId,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) entries: Vec<TrafficEntry>,
    encoded_bytes: u64,
    created: Instant,
}

impl TrafficBatch {
    fn new(source: SourceId, bucket: BucketIdentity, sequence: u64) -> Self {
        Self {
            id: TrafficBatchId {
                source_node: NodeId(u64::from(source.node_id)),
                source_epoch: source.source_epoch,
                sequence,
            },
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.bucket_id,
            entries: Vec::new(),
            encoded_bytes: 0,
            created: Instant::now(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct TrafficBatchId {
    pub(crate) source_node: NodeId,
    pub(crate) source_epoch: [u8; 32],
    pub(crate) sequence: u64,
}

impl TrafficBatchId {
    pub(crate) fn stable_string(self) -> String {
        format!(
            "accounting-traffic-{}-{}-{}",
            self.source_node.0,
            hex::encode(&self.source_epoch[..8]),
            self.sequence
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TrafficEntry {
    pub(crate) path: String,
    pub(crate) accepted_inbound_bytes: u64,
    pub(crate) served_outbound_bytes: u64,
}

impl TrafficEntry {
    fn encoded_bytes(&self) -> u64 {
        accounting_traffic_entry_logical_bytes(&self.path)
    }
}

#[derive(Default)]
struct TrafficQueue {
    open: BTreeMap<BucketIdentity, TrafficBatch>,
    ready: VecDeque<TrafficBatch>,
    next_sequence: u64,
    total_entries: usize,
    total_bytes: u64,
}

#[derive(Default)]
struct TrafficDropState {
    totals: Mutex<TrafficDropTotals>,
}

#[derive(Default)]
struct TrafficDropTotals {
    batches: u64,
    bytes: u64,
}

impl TrafficQueue {
    fn seal(&mut self, bucket: BucketIdentity, max_batches: usize) -> bool {
        if self.ready.len().saturating_add(self.open.len()) > max_batches {
            return false;
        }
        let Some(batch) = self.open.remove(&bucket) else {
            return true;
        };
        let position = self
            .ready
            .iter()
            .position(|ready| ready.id.sequence > batch.id.sequence)
            .unwrap_or(self.ready.len());
        self.ready.insert(position, batch);
        true
    }

    fn seal_all(&mut self, max_batches: usize) {
        let buckets = self.open.keys().copied().collect::<Vec<_>>();
        for bucket in buckets {
            if !self.seal(bucket, max_batches) {
                break;
            }
        }
    }
}

fn emit_pending(queue: &TrafficQueue) {
    let oldest = queue
        .ready
        .front()
        .map_or(Duration::ZERO, |batch| batch.created.elapsed());
    tracing::debug!(
        gauge.anvil_accounting_traffic_pending_batches =
            queue.ready.len().saturating_add(queue.open.len()) as u64,
        gauge.anvil_accounting_traffic_pending_bytes = queue.total_bytes,
        gauge.anvil_accounting_traffic_oldest_pending_millis = oldest.as_millis() as u64,
        "best-effort bandwidth accounting queue state"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meter(config: AccountingTrafficConfig) -> AccountingTraffic {
        AccountingTraffic::new(
            SourceId {
                node_id: 7,
                source_epoch: [3; 32],
            },
            config,
        )
    }

    #[test]
    fn one_bucket_is_batched_and_retry_identity_is_stable() {
        let meter = meter(AccountingTrafficConfig::default());
        meter.record_inbound(11, 12, "users/7/a", 12);
        meter.record_outbound(11, 12, "users/7/a", 5);

        let first = meter.pending();
        let retried = meter.pending();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].entries.len(), 2);
        assert_eq!(first[0].id, retried[0].id);
        assert_eq!(first[0].id.source_node, NodeId(7));

        meter.acknowledge(&first[0].id);
        assert!(meter.pending().is_empty());
    }

    #[test]
    fn request_batch_records_all_inbound_observations() {
        let meter = meter(AccountingTrafficConfig::default());
        meter.record_inbound_batch([
            (11, 12, "users/7/a", 12),
            (11, 12, "users/7/b", 8),
            (11, 13, "users/8/a", 5),
        ]);

        let batches = meter.pending();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches
                .iter()
                .flat_map(|batch| batch.entries.iter())
                .map(|entry| entry.accepted_inbound_bytes)
                .sum::<u64>(),
            25
        );
    }

    #[test]
    fn batches_never_mix_stable_bucket_identities() {
        let meter = meter(AccountingTrafficConfig::default());
        meter.record_inbound(11, 12, "a", 1);
        meter.record_inbound(11, 13, "a", 1);
        let batches = meter.pending();
        assert_eq!(batches.len(), 2);
        assert_ne!(batches[0].bucket_id, batches[1].bucket_id);
    }

    #[test]
    fn sealed_batches_are_delivered_and_acknowledged_in_source_sequence_order() {
        let meter = meter(AccountingTrafficConfig::default());
        // BTreeMap bucket order is the reverse of creation/source sequence.
        meter.record_inbound(11, 13, "first", 1);
        meter.record_inbound(11, 12, "second", 1);
        let batches = meter.pending();
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.id.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        meter.acknowledge(&batches[1].id);
        assert_eq!(meter.pending().len(), 2);
        meter.acknowledge(&batches[0].id);
        assert_eq!(meter.pending()[0].id.sequence, 2);
    }

    #[test]
    fn queue_capacity_drops_without_displacing_acknowledged_order() {
        let config = AccountingTrafficConfig::new(1, 1, 64, 1, 64, 1).unwrap();
        let meter = meter(config);
        meter.record_inbound(11, 12, "a", 5);
        meter.record_inbound(11, 12, "b", 7);
        let batches = meter.pending();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].entries[0].path, "a");
        assert_eq!(meter.dropped_totals(), (1, 7));
    }

    #[test]
    fn process_local_queues_do_not_reuse_batch_identities_after_restart() {
        let first = meter(AccountingTrafficConfig::default());
        let second = meter(AccountingTrafficConfig::default());
        first.record_inbound(11, 12, "a", 1);
        second.record_inbound(11, 12, "a", 1);
        assert_ne!(first.pending()[0].id, second.pending()[0].id);
    }
}
