use anyhow::{Result, anyhow};

use crate::{
    mvcc_product::ProductMutation,
    mvcc_transaction::{DurabilityLevel, TransactionRuntime},
    tenant_locator_finalization_job::{
        TABLE_TENANT_LOCATOR_FINALIZATION, TENANT_LOCATOR_FINALIZATION_PREFIX,
        TenantLocatorFinalizationJob,
    },
};

use super::Persistence;

impl Persistence {
    pub(crate) async fn run_tenant_locator_finalization_once(&self) -> Result<bool> {
        let mvcc = self.mvcc()?;
        let snapshot = mvcc.runtime.applied_version()?;
        let Some((key, row)) = mvcc
            .runtime
            .scan_table_prefix_at(
                TABLE_TENANT_LOCATOR_FINALIZATION,
                TENANT_LOCATOR_FINALIZATION_PREFIX,
                snapshot,
            )?
            .into_iter()
            .next()
        else {
            return Ok(false);
        };
        let job: TenantLocatorFinalizationJob = serde_json::from_slice(&row.value)?;
        job.validate()?;
        let guard = mvcc
            .claim_assignment("control-plane", mvcc.cluster_id())?
            .ok_or_else(|| anyhow!("tenant locator finalization is not assigned to this node"))?;
        self.write_mesh_tenant_locators(&job.tenant, &job.idempotency_key, &job.home_region)
            .await?;
        mvcc.validate_assignment(&guard)?;
        mvcc.autocommit_product_mutations(
            &format!("tenant-locator-finalization/{}", self.owner_node_id()),
            &format!(
                "tenant-locator-finalization-complete:{}",
                job.transaction_id
            ),
            vec![ProductMutation { key, value: None }],
            DurabilityLevel::Quorum,
            u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
        )
        .await?;
        Ok(true)
    }
}
