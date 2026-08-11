use anvil_store::ObjectKey;

use super::ObjectServiceImpl;

impl ObjectServiceImpl {
    pub(crate) fn record_gateway_ingress(&self, key: &ObjectKey, bytes: u64) {
        self.record_accounting_traffic(key.tenant(), key.bucket(), key.path(), bytes, 0);
    }

    pub(crate) fn record_gateway_egress(&self, key: &ObjectKey, bytes: u64) {
        self.record_accounting_traffic(key.tenant(), key.bucket(), key.path(), 0, bytes);
    }

    pub(crate) fn record_gateway_ingress_stable(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        bytes: u64,
    ) {
        self.accounting_traffic
            .record_inbound(tenant_id, bucket_id, path, bytes);
    }

    pub(crate) fn record_gateway_egress_stable(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        bytes: u64,
    ) {
        self.accounting_traffic
            .record_outbound(tenant_id, bucket_id, path, bytes);
    }

    pub(super) fn record_accounting_inbound(&self, key: &ObjectKey, bytes: u64) {
        self.record_accounting_traffic(key.tenant(), key.bucket(), key.path(), bytes, 0);
    }

    pub(super) fn record_accounting_outbound(&self, key: &ObjectKey, bytes: u64) {
        self.record_accounting_traffic(key.tenant(), key.bucket(), key.path(), 0, bytes);
    }

    pub(super) fn record_accounting_traffic(
        &self,
        tenant: &str,
        bucket: &str,
        path: &str,
        inbound: u64,
        outbound: u64,
    ) {
        match self.store.resolve_bucket_ids(tenant, bucket) {
            Ok((tenant_id, bucket_id)) => {
                if inbound != 0 {
                    self.accounting_traffic
                        .record_inbound(tenant_id, bucket_id, path, inbound);
                }
                if outbound != 0 {
                    self.accounting_traffic
                        .record_outbound(tenant_id, bucket_id, path, outbound);
                }
            }
            Err(_) => self
                .accounting_traffic
                .record_resolution_drop(inbound, outbound),
        }
    }
}
