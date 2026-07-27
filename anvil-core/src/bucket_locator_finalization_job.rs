use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::persistence::Bucket;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketLocatorFinalizationOperation {
    Publish,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketLocatorFinalizationJob {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    pub operation_sequence: u64,
    pub operation: BucketLocatorFinalizationOperation,
    pub frozen_bucket: Bucket,
}

impl BucketLocatorFinalizationJob {
    pub const SCHEMA: &'static str = "anvil.mvcc.bucket-locator-finalization-job.v1";

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let job: Self = serde_json::from_slice(bytes)?;
        if job.encode()? != bytes {
            bail!("bucket locator finalization job is not canonically encoded");
        }
        Ok(job)
    }

    pub fn job_id(&self) -> Result<String> {
        Ok(hex::encode(blake3::hash(&self.encode()?).as_bytes()))
    }

    pub fn target_logical_identity(&self) -> String {
        format!(
            "tenant/{}/bucket/{}/locator",
            self.frozen_bucket.tenant_id, self.frozen_bucket.id
        )
    }

    fn validate(&self) -> Result<()> {
        if self.schema != Self::SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self.operation_sequence == 0
            || self.frozen_bucket.id <= 0
            || self.frozen_bucket.tenant_id < 0
            || self.frozen_bucket.name.trim().is_empty()
            || self.frozen_bucket.region.trim().is_empty()
        {
            bail!("invalid bucket locator finalization job");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BucketLocatorFinalizationState {
    Pending,
    Running,
    Complete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketLocatorFinalizationRecord {
    pub job: BucketLocatorFinalizationJob,
    pub commit_version: u64,
    pub state: BucketLocatorFinalizationState,
    pub attempts: u32,
    pub next_attempt_unix_ms: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl BucketLocatorFinalizationRecord {
    pub fn pending(job: BucketLocatorFinalizationJob, commit_version: u64) -> Self {
        Self {
            job,
            commit_version,
            state: BucketLocatorFinalizationState::Pending,
            attempts: 0,
            next_attempt_unix_ms: 0,
            lease_owner: None,
            lease_expires_unix_ms: None,
            last_error: None,
        }
    }

    pub fn claimable(&self, now_unix_ms: u64) -> bool {
        match self.state {
            BucketLocatorFinalizationState::Pending => {
                self.next_attempt_unix_ms <= now_unix_ms
            }
            BucketLocatorFinalizationState::Running => self
                .lease_expires_unix_ms
                .is_some_and(|expiry| expiry <= now_unix_ms),
            BucketLocatorFinalizationState::Complete => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    fn job(operation: BucketLocatorFinalizationOperation) -> BucketLocatorFinalizationJob {
        BucketLocatorFinalizationJob {
            schema: BucketLocatorFinalizationJob::SCHEMA.to_string(),
            cluster_id: "cluster-a".to_string(),
            transaction_id: "transaction-a".to_string(),
            operation_sequence: 1,
            operation,
            frozen_bucket: Bucket {
                id: 7,
                tenant_id: 3,
                name: "artifacts".to_string(),
                region: "eu-west-1".to_string(),
                created_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                is_public_read: false,
            },
        }
    }

    #[test]
    fn canonical_job_identity_covers_operation_and_frozen_bucket() {
        let publish = job(BucketLocatorFinalizationOperation::Publish);
        let delete = job(BucketLocatorFinalizationOperation::Delete);

        let decoded =
            BucketLocatorFinalizationJob::decode(&publish.encode().unwrap()).unwrap();
        assert_eq!(decoded.encode().unwrap(), publish.encode().unwrap());
        assert_ne!(publish.job_id().unwrap(), delete.job_id().unwrap());
        assert_eq!(
            publish.target_logical_identity(),
            "tenant/3/bucket/7/locator"
        );
    }

    #[test]
    fn completed_jobs_are_not_claimable() {
        let mut record = BucketLocatorFinalizationRecord::pending(
            job(BucketLocatorFinalizationOperation::Publish),
            11,
        );
        assert!(record.claimable(0));
        record.state = BucketLocatorFinalizationState::Complete;
        assert!(!record.claimable(u64::MAX));
    }
}
