//! Post-listener bounded blob lifecycle maintenance.
//!
//! This is the single runtime owner for ordinary due-ordered artifact GC and
//! former-placement retirement. Payload discovery is entirely RocksDB-backed;
//! starting this task never inventories payload files.

use keldra_store::{BlobGcBudget, BlobGcCursor, Store};

use crate::payload_gc::PayloadGarbageCollector;
use crate::reference_delivery::ReferenceRuntimeHandle;
use crate::startup_scan_evidence::{StartupScanEvidence, StartupScanExtent, StartupScanKind};
use crate::{
    BLOB_GC_BYTES_PER_TICK, BLOB_GC_CONTINUATION_INTERVAL, BLOB_GC_INTERVAL,
    BLOB_GC_RECORDS_PER_TICK, BLOB_GC_TIME_PER_TICK,
};

pub(crate) struct BlobMaintenanceTask {
    task: tokio::task::JoinHandle<()>,
}

impl BlobMaintenanceTask {
    /// Start after public and peer listeners are accepting work.
    pub(crate) fn start(
        store: Store,
        references: ReferenceRuntimeHandle,
        payloads: PayloadGarbageCollector,
        startup_scan_evidence: StartupScanEvidence,
    ) -> Self {
        Self {
            task: tokio::spawn(async move {
                tokio::join!(
                    run_blob_gc(store, references, startup_scan_evidence.clone()),
                    run_payload_retirement(payloads, startup_scan_evidence)
                );
            }),
        }
    }

    pub(crate) async fn shutdown(self) {
        self.task.abort();
        if let Err(error) = self.task.await
            && !error.is_cancelled()
        {
            tracing::error!(%error, "blob maintenance task stopped unexpectedly");
        }
    }
}

async fn run_blob_gc(
    store: Store,
    references: ReferenceRuntimeHandle,
    startup_scan_evidence: StartupScanEvidence,
) {
    let mut cursor = BlobGcCursor::default();
    let budget = BlobGcBudget::new(
        BLOB_GC_RECORDS_PER_TICK,
        BLOB_GC_BYTES_PER_TICK,
        BLOB_GC_TIME_PER_TICK,
    )
    .expect("fixed blob GC budget is valid");
    let mut delay = BLOB_GC_INTERVAL;
    loop {
        tokio::time::sleep(delay).await;
        // This evidence is scoped to the due CF. It is never a payload-file
        // inventory.
        startup_scan_evidence.record(StartupScanKind::Blobs, StartupScanExtent::Scoped);
        let outcome = collect_if_reference_safe(&store, &references, &mut cursor, budget).await;
        delay = if outcome == Some(false) {
            BLOB_GC_CONTINUATION_INTERVAL
        } else {
            BLOB_GC_INTERVAL
        };
    }
}

async fn collect_if_reference_safe(
    store: &Store,
    references: &ReferenceRuntimeHandle,
    cursor: &mut BlobGcCursor,
    budget: BlobGcBudget,
) -> Option<bool> {
    if !references.gc_safe().await {
        tracing::warn!(
            monotonic_counter.keldra_blob_gc_paused_total = 1_u64,
            trigger = "scheduled",
            "blob garbage collection paused until every ACTIVE source tail is current"
        );
        return None;
    }
    match store.collect_blob_garbage_tick(cursor, budget).await {
        Ok(tick) => {
            tracing::debug!(
                monotonic_counter.keldra_blob_gc_runs_total = 1_u64,
                monotonic_counter.keldra_blob_gc_removed_total = tick.removed,
                gauge.keldra_blob_gc_tick_records = tick.inspected_records as u64,
                gauge.keldra_blob_gc_tick_bytes = tick.inspected_bytes,
                cycle_complete = tick.cycle_complete,
                trigger = "scheduled",
                removed = tick.removed,
                "bounded blob garbage-collection tick completed"
            );
            Some(tick.cycle_complete)
        }
        Err(error) => {
            tracing::error!(
                monotonic_counter.keldra_blob_gc_failures_total = 1_u64,
                trigger = "scheduled",
                %error,
                "blob garbage-collection pass failed"
            );
            None
        }
    }
}

async fn run_payload_retirement(
    payloads: PayloadGarbageCollector,
    startup_scan_evidence: StartupScanEvidence,
) {
    let mut delay = BLOB_GC_INTERVAL;
    loop {
        tokio::time::sleep(delay).await;
        startup_scan_evidence.record(StartupScanKind::Blobs, StartupScanExtent::Scoped);
        match payloads.run_once().await {
            Ok(tick) => {
                if tick.retired > 0 {
                    tracing::info!(
                        retired = tick.retired,
                        "former payload artifacts entered the ordinary GC grace window"
                    );
                }
                delay = if tick.cycle_complete {
                    BLOB_GC_INTERVAL
                } else {
                    BLOB_GC_CONTINUATION_INTERVAL
                };
            }
            Err(error) => {
                tracing::warn!(%error, "former payload-artifact retirement paused");
                delay = BLOB_GC_INTERVAL;
            }
        }
    }
}
