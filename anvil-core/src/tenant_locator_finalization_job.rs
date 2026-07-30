use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::{mvcc_product::ProductMutation, mvcc_transaction::LogicalKey, persistence::Tenant};

pub const TABLE_TENANT_LOCATOR_FINALIZATION: u16 = 0x6201;
pub const TENANT_LOCATOR_FINALIZATION_PREFIX: &[u8] = b"anvil.tenant-locator-finalization.v1/";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantLocatorFinalizationJob {
    pub cluster_id: String,
    pub transaction_id: String,
    pub tenant: Tenant,
    pub idempotency_key: String,
    pub home_region: String,
}

impl TenantLocatorFinalizationJob {
    pub fn validate(&self) -> Result<()> {
        if self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self.tenant.id <= 0
            || self.tenant.name.trim().is_empty()
            || self.idempotency_key.trim().is_empty()
            || self.home_region.trim().is_empty()
        {
            bail!("invalid tenant locator finalization job");
        }
        Ok(())
    }

    pub fn logical_key(&self) -> Result<LogicalKey> {
        self.validate()?;
        let mut application_key = TENANT_LOCATOR_FINALIZATION_PREFIX.to_vec();
        application_key.extend_from_slice(self.transaction_id.as_bytes());
        Ok(LogicalKey {
            table_id: TABLE_TENANT_LOCATOR_FINALIZATION,
            application_key,
        })
    }

    pub fn assignment_logical_identity(&self) -> String {
        format!("tenant:{}", self.tenant.id)
    }

    pub fn mutation(&self) -> Result<ProductMutation> {
        Ok(ProductMutation {
            key: self.logical_key()?,
            value: Some(serde_json::to_vec(self)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job() -> TenantLocatorFinalizationJob {
        TenantLocatorFinalizationJob {
            cluster_id: "cluster".into(),
            transaction_id: "transaction".into(),
            tenant: Tenant {
                id: 42,
                name: "tenant".into(),
            },
            idempotency_key: "idempotency".into(),
            home_region: "region".into(),
        }
    }

    #[test]
    fn assignment_identity_is_stable_per_tenant() {
        assert_eq!(job().assignment_logical_identity(), "tenant:42");
    }
}
