use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::catalog::AccountingCatalog;

const MAX_PENDING_ACCOUNTING_DEFINITIONS: usize = 65_536;

/// Small injectable traffic hook. Object and gateway code sees only this
/// counter surface; it does not depend on accounting persistence or routing.
#[derive(Clone)]
pub(crate) struct AccountingTraffic {
    catalog: AccountingCatalog,
    pending: Arc<Mutex<BTreeMap<u64, TrafficDelta>>>,
}

impl AccountingTraffic {
    pub(crate) fn new(catalog: AccountingCatalog) -> Self {
        Self {
            catalog,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(crate) fn record_inbound(
        &self,
        storage_tenant: &str,
        bucket: &str,
        path: &str,
        bytes: u64,
    ) {
        self.record(storage_tenant, bucket, path, bytes, 0);
    }

    pub(crate) fn record_outbound(
        &self,
        storage_tenant: &str,
        bucket: &str,
        path: &str,
        bytes: u64,
    ) {
        self.record(storage_tenant, bucket, path, 0, bytes);
    }

    fn record(&self, storage_tenant: &str, bucket: &str, path: &str, inbound: u64, outbound: u64) {
        if inbound == 0 && outbound == 0 {
            return;
        }
        let definitions = match self.catalog.matching_names(storage_tenant, bucket, path) {
            Ok(definitions) => definitions,
            Err(error) => {
                tracing::warn!(%error, "usage accounting catalog lookup failed");
                return;
            }
        };
        let Ok(mut pending) = self.pending.lock() else {
            tracing::error!("usage accounting traffic counter lock is poisoned");
            return;
        };
        for definition in definitions {
            if !pending.contains_key(&definition.stored.accounting_id)
                && pending.len() >= MAX_PENDING_ACCOUNTING_DEFINITIONS
            {
                tracing::warn!(
                    monotonic_counter.anvil_accounting_traffic_dropped_total = 1_u64,
                    "usage accounting traffic counter capacity is exhausted"
                );
                continue;
            }
            let delta = pending.entry(definition.stored.accounting_id).or_default();
            delta.accepted_inbound_bytes = delta.accepted_inbound_bytes.saturating_add(inbound);
            delta.served_outbound_bytes = delta.served_outbound_bytes.saturating_add(outbound);
        }
    }

    pub(crate) fn pending(&self) -> Vec<(u64, TrafficDelta)> {
        match self.pending.lock() {
            Ok(pending) => pending.iter().map(|(id, delta)| (*id, *delta)).collect(),
            Err(_) => {
                tracing::error!("usage accounting traffic counter lock is poisoned");
                Vec::new()
            }
        }
    }

    pub(crate) fn acknowledge(&self, accounting_id: u64, flushed: TrafficDelta) {
        let Ok(mut pending) = self.pending.lock() else {
            tracing::error!("usage accounting traffic counter lock is poisoned");
            return;
        };
        let Some(current) = pending.get_mut(&accounting_id) else {
            return;
        };
        current.accepted_inbound_bytes = current
            .accepted_inbound_bytes
            .saturating_sub(flushed.accepted_inbound_bytes);
        current.served_outbound_bytes = current
            .served_outbound_bytes
            .saturating_sub(flushed.served_outbound_bytes);
        if *current == TrafficDelta::default() {
            pending.remove(&accounting_id);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TrafficDelta {
    pub(crate) accepted_inbound_bytes: u64,
    pub(crate) served_outbound_bytes: u64,
}

#[cfg(test)]
mod tests {
    use anvil_store::VersionId;

    use super::*;
    use crate::accounting::{LoadedAccountingDefinition, StoredAccountingDefinition};

    #[test]
    fn overlapping_bucket_and_prefix_definitions_both_receive_traffic() {
        let catalog = AccountingCatalog::default();
        catalog
            .replace(vec![
                definition("", 1),
                definition("users/7", 2),
                definition("users/8", 3),
            ])
            .unwrap();
        let meter = AccountingTraffic::new(catalog);
        meter.record_inbound("tenant", "bucket", "users/7/a", 12);
        meter.record_outbound("tenant", "bucket", "users/7/a", 5);
        let pending = meter.pending();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|(_, value)| {
            value.accepted_inbound_bytes == 12 && value.served_outbound_bytes == 5
        }));
    }

    fn definition(prefix: &str, version: u64) -> LoadedAccountingDefinition {
        LoadedAccountingDefinition {
            tenant_id: 7,
            bucket_id: 9,
            version: VersionId(version),
            stored: StoredAccountingDefinition::create(
                "tenant".into(),
                "bucket".into(),
                prefix.into(),
                7,
                9,
            )
            .unwrap(),
        }
    }
}
