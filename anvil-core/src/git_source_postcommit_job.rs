use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitSourcePostCommitJob {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    pub tenant_id: i64,
    pub repository_id: String,
    pub bucket_name: String,
    pub object_key: String,
    pub pack_object_version_id: String,
    pub pack_mutation_id: String,
    pub source_hash: String,
    pub generation: u64,
    pub record_count: u64,
    pub index_path: String,
    pub authz_revision: u64,
    pub emitted_at: String,
}

impl GitSourcePostCommitJob {
    pub const SCHEMA: &'static str = "anvil.mvcc.git-source-postcommit-job.v1";

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let job: Self = serde_json::from_slice(bytes)?;
        if job.encode()? != bytes {
            bail!("GitSource postcommit job is not canonically encoded");
        }
        Ok(job)
    }

    pub fn job_id(&self) -> Result<String> {
        Ok(hex::encode(blake3::hash(&self.encode()?).as_bytes()))
    }

    pub fn target_logical_identity(&self) -> String {
        format!(
            "tenant/{}/git-source/{}/generation/{}",
            self.tenant_id, self.repository_id, self.generation
        )
    }

    fn validate(&self) -> Result<()> {
        if self.schema != Self::SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self.tenant_id <= 0
            || self.repository_id.trim().is_empty()
            || self.bucket_name.trim().is_empty()
            || self.object_key.trim().is_empty()
            || self.generation == 0
            || self.index_path.trim().is_empty()
            || self.emitted_at.trim().is_empty()
        {
            bail!("invalid GitSource postcommit job");
        }
        uuid::Uuid::parse_str(&self.pack_object_version_id)
            .map_err(|_| anyhow::anyhow!("GitSource pack version ID is invalid"))?;
        uuid::Uuid::parse_str(&self.pack_mutation_id)
            .map_err(|_| anyhow::anyhow!("GitSource pack mutation ID is invalid"))?;
        if self.source_hash.len() != 64
            || !self
                .source_hash
                .as_bytes()
                .iter()
                .all(u8::is_ascii_hexdigit)
        {
            bail!("GitSource pack source hash must be hex32");
        }
        let expected = crate::git_source_index::git_source_index_ref_name(
            self.tenant_id,
            &self.repository_id,
            self.generation,
            &self.source_hash,
        )?;
        if self.index_path != expected {
            bail!("GitSource postcommit index ref is not canonical");
        }
        chrono::DateTime::parse_from_rfc3339(&self.emitted_at)
            .map_err(|_| anyhow::anyhow!("GitSource emitted_at is invalid"))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitSourcePostCommitState {
    Pending,
    Running,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitSourcePostCommitRecord {
    pub job: GitSourcePostCommitJob,
    pub commit_version: u64,
    pub state: GitSourcePostCommitState,
    pub attempts: u32,
    pub next_attempt_unix_ms: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl GitSourcePostCommitRecord {
    pub fn pending(job: GitSourcePostCommitJob, commit_version: u64) -> Self {
        Self {
            job,
            commit_version,
            state: GitSourcePostCommitState::Pending,
            attempts: 0,
            next_attempt_unix_ms: 0,
            lease_owner: None,
            lease_expires_unix_ms: None,
            last_error: None,
        }
    }

    pub fn claimable(&self, now_unix_ms: u64) -> bool {
        match self.state {
            GitSourcePostCommitState::Pending => self.next_attempt_unix_ms <= now_unix_ms,
            GitSourcePostCommitState::Running => self
                .lease_expires_unix_ms
                .is_some_and(|expiry| expiry <= now_unix_ms),
            GitSourcePostCommitState::Complete => false,
        }
    }
}
