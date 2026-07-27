use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexFinalizationJob {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub index_name: String,
    pub index_id: i64,
    pub index_version: i64,
    pub event_type: String,
    pub creator_principal: String,
    pub frozen_definition: Value,
}

impl IndexFinalizationJob {
    pub const SCHEMA: &'static str = "anvil.mvcc.index-finalization-job.v1";

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let job: Self = serde_json::from_slice(bytes)?;
        if job.encode()? != bytes {
            bail!("index finalization job is not canonically encoded");
        }
        Ok(job)
    }

    pub fn job_id(&self) -> Result<String> {
        Ok(hex::encode(blake3::hash(&self.encode()?).as_bytes()))
    }

    pub fn target_logical_identity(&self) -> String {
        format!(
            "tenant/{}/bucket/{}/index/{}/version/{}",
            self.tenant_id, self.bucket_id, self.index_id, self.index_version
        )
    }

    fn validate(&self) -> Result<()> {
        if self.schema != Self::SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self.tenant_id < 0
            || self.bucket_id <= 0
            || self.bucket_name.trim().is_empty()
            || self.index_name.trim().is_empty()
            || self.index_id <= 0
            || self.index_version <= 0
            || self.event_type.trim().is_empty()
            || self.creator_principal.trim().is_empty()
            || !self.frozen_definition.is_object()
        {
            bail!("invalid index finalization job");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexFinalizationState {
    Pending,
    Running,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexFinalizationRecord {
    pub job: IndexFinalizationJob,
    pub commit_version: u64,
    pub state: IndexFinalizationState,
    pub attempts: u32,
    pub next_attempt_unix_ms: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl IndexFinalizationRecord {
    pub fn pending(job: IndexFinalizationJob, commit_version: u64) -> Self {
        Self {
            job,
            commit_version,
            state: IndexFinalizationState::Pending,
            attempts: 0,
            next_attempt_unix_ms: 0,
            lease_owner: None,
            lease_expires_unix_ms: None,
            last_error: None,
        }
    }

    pub fn claimable(&self, now_unix_ms: u64) -> bool {
        match self.state {
            IndexFinalizationState::Pending => self.next_attempt_unix_ms <= now_unix_ms,
            IndexFinalizationState::Running => self
                .lease_expires_unix_ms
                .is_some_and(|expiry| expiry <= now_unix_ms),
            IndexFinalizationState::Complete => false,
        }
    }
}
