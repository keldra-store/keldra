use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HfIngestionPostCommitJob {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    pub ingestion_id: i64,
    pub tenant_id: i64,
    pub priority: i32,
}

impl HfIngestionPostCommitJob {
    pub const SCHEMA: &'static str = "anvil.mvcc.hf-ingestion-postcommit-job.v1";

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let job: Self = serde_json::from_slice(bytes)?;
        if job.encode()? != bytes {
            bail!("Hugging Face ingestion postcommit job is not canonically encoded");
        }
        Ok(job)
    }

    pub fn job_id(&self) -> Result<String> {
        Ok(hex::encode(blake3::hash(&self.encode()?).as_bytes()))
    }

    pub fn target_logical_identity(&self) -> String {
        format!(
            "tenant/{}/hf-ingestion/{}",
            self.tenant_id, self.ingestion_id
        )
    }

    fn validate(&self) -> Result<()> {
        if self.schema != Self::SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self.ingestion_id <= 0
            || self.tenant_id < 0
        {
            bail!("invalid Hugging Face ingestion postcommit job");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HfIngestionPostCommitState {
    Pending,
    Running,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HfIngestionPostCommitRecord {
    pub job: HfIngestionPostCommitJob,
    pub commit_version: u64,
    pub state: HfIngestionPostCommitState,
    pub attempts: u32,
    pub next_attempt_unix_ms: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl HfIngestionPostCommitRecord {
    pub fn pending(job: HfIngestionPostCommitJob, commit_version: u64) -> Self {
        Self {
            job,
            commit_version,
            state: HfIngestionPostCommitState::Pending,
            attempts: 0,
            next_attempt_unix_ms: 0,
            lease_owner: None,
            lease_expires_unix_ms: None,
            last_error: None,
        }
    }

    pub fn claimable(&self, now_unix_ms: u64) -> bool {
        match self.state {
            HfIngestionPostCommitState::Pending => self.next_attempt_unix_ms <= now_unix_ms,
            HfIngestionPostCommitState::Running => self
                .lease_expires_unix_ms
                .is_some_and(|expiry| expiry <= now_unix_ms),
            HfIngestionPostCommitState::Complete => false,
        }
    }
}
