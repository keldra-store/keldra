use super::*;
use crate::bucket_locator_finalization_job::{
    BucketLocatorFinalizationJob, BucketLocatorFinalizationOperation,
};

impl Persistence {
    pub(crate) async fn run_bucket_locator_finalization_once(&self) -> Result<bool> {
        let worker_id = format!("bucket-locator-finalization/{}", self.owner_node_id());
        let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
        let Some((job_id, record)) = self
            .mvcc()?
            .runtime
            .local_store()
            .claim_bucket_locator_finalization_authorized(&worker_id, now, 30_000, |record| {
                self.mvcc()
                    .ok()?
                    .claim_assignment(
                        "bucket-locator-finalization",
                        &record.job.target_logical_identity(),
                    )
                    .ok()
                    .flatten()
                    .map(|guard| guard.lease_owner(&worker_id))
            })?
        else {
            return Ok(false);
        };
        let guard = self
            .mvcc()?
            .claim_assignment(
                "bucket-locator-finalization",
                &record.job.target_logical_identity(),
            )?
            .ok_or_else(|| anyhow!("bucket locator finalization assignment changed after claim"))?;
        let lease_owner = guard.lease_owner(&worker_id);
        let result = self
            .execute_bucket_locator_finalization(&record.job, record.commit_version)
            .await;
        match result {
            Ok(()) => {
                self.mvcc()?.validate_assignment(&guard)?;
                self.mvcc()?
                    .runtime
                    .local_store()
                    .complete_bucket_locator_finalization(&job_id, &lease_owner)?;
                Ok(true)
            }
            Err(error) => {
                let shift = record.attempts.saturating_sub(1).min(10);
                let delay = 250_u64.saturating_mul(1_u64 << shift);
                self.mvcc()?
                    .runtime
                    .local_store()
                    .retry_bucket_locator_finalization(
                        &job_id,
                        &lease_owner,
                        now.saturating_add(delay),
                        &error.to_string(),
                    )?;
                Err(error)
            }
        }
    }

    async fn execute_bucket_locator_finalization(
        &self,
        job: &BucketLocatorFinalizationJob,
        commit_version: u64,
    ) -> Result<()> {
        crate::mvcc_fault_injection::hit(
            crate::mvcc_fault_injection::FaultPoint::BucketLocatorFinalizationBeforeEffects,
        )?;
        let visible = crate::bucket_journal::read_current_bucket_at_mvcc_snapshot(
            self.mvcc()?,
            job.frozen_bucket.tenant_id,
            &job.frozen_bucket.name,
            commit_version,
        )?;
        match job.operation {
            BucketLocatorFinalizationOperation::Publish => {
                let Some(visible) = visible else {
                    // A create followed by a delete in the same transaction
                    // has no committed locator source. Its later delete job is
                    // still replayed in operation-sequence order.
                    return Ok(());
                };
                ensure_frozen_bucket_matches(&job.frozen_bucket, &visible)?;
                self.write_mesh_bucket_locator(&job.frozen_bucket).await?;
            }
            BucketLocatorFinalizationOperation::Delete => {
                if visible.is_some() {
                    bail!("committed bucket delete did not tombstone its source row");
                }
                self.mark_mesh_bucket_locator_deleted(&job.frozen_bucket)
                    .await?;
            }
        }
        crate::mvcc_fault_injection::hit(
            crate::mvcc_fault_injection::FaultPoint::BucketLocatorFinalizationAfterEffects,
        )?;
        Ok(())
    }
}

fn ensure_frozen_bucket_matches(expected: &Bucket, actual: &Bucket) -> Result<()> {
    if expected.id != actual.id
        || expected.tenant_id != actual.tenant_id
        || expected.name != actual.name
        || expected.region != actual.region
        || expected.created_at != actual.created_at
    {
        bail!("committed bucket locator source row diverges from immutable job");
    }
    Ok(())
}
