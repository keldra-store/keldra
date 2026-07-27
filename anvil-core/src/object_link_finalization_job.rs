use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectLinkFinalizationOperation {
    Put,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectLinkFinalizationConsequences {
    pub maintain_indexes: bool,
    pub compact_metadata: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectLinkFinalizationJob {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub link_key: String,
    pub generation: u64,
    pub operation: ObjectLinkFinalizationOperation,
    pub target_key: Option<String>,
    pub target_version_id: Option<String>,
    pub mutation_id: String,
    pub consequences: ObjectLinkFinalizationConsequences,
}

impl ObjectLinkFinalizationJob {
    pub const SCHEMA: &'static str = "anvil.mvcc.object-link-finalization-job.v1";

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let job: Self = serde_json::from_slice(bytes)?;
        if job.encode()? != bytes {
            bail!("object-link finalization job is not canonically encoded");
        }
        Ok(job)
    }

    pub fn job_id(&self) -> Result<String> {
        Ok(hex::encode(blake3::hash(&self.encode()?).as_bytes()))
    }

    pub fn target_logical_identity(&self) -> String {
        format!(
            "tenant/{}/bucket/{}/object-link/{}/generation/{}",
            self.tenant_id, self.bucket_id, self.link_key, self.generation
        )
    }

    fn validate(&self) -> Result<()> {
        if self.schema != Self::SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self.tenant_id < 0
            || self.bucket_id <= 0
            || self.bucket_name.trim().is_empty()
            || self.link_key.trim().is_empty()
            || self.generation == 0
        {
            bail!("invalid object-link finalization job");
        }
        uuid::Uuid::parse_str(&self.mutation_id)
            .map_err(|_| anyhow::anyhow!("object-link mutation ID is invalid"))?;
        if let Some(version) = &self.target_version_id {
            uuid::Uuid::parse_str(version)
                .map_err(|_| anyhow::anyhow!("object-link target version ID is invalid"))?;
        }
        match self.operation {
            ObjectLinkFinalizationOperation::Put if self.target_key.as_deref().is_none_or(str::is_empty) => {
                bail!("object-link put finalization requires a target key");
            }
            ObjectLinkFinalizationOperation::Delete
                if self.target_key.is_some() || self.target_version_id.is_some() =>
            {
                bail!("object-link delete finalization cannot carry a target");
            }
            _ => {}
        }
        if !self.consequences.maintain_indexes && !self.consequences.compact_metadata {
            bail!("object-link finalization must request at least one consequence");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectLinkFinalizationState {
    Pending,
    Running,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectLinkFinalizationRecord {
    pub job: ObjectLinkFinalizationJob,
    pub commit_version: u64,
    pub state: ObjectLinkFinalizationState,
    pub attempts: u32,
    pub next_attempt_unix_ms: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl ObjectLinkFinalizationRecord {
    pub fn pending(job: ObjectLinkFinalizationJob, commit_version: u64) -> Self {
        Self {
            job,
            commit_version,
            state: ObjectLinkFinalizationState::Pending,
            attempts: 0,
            next_attempt_unix_ms: 0,
            lease_owner: None,
            lease_expires_unix_ms: None,
            last_error: None,
        }
    }

    pub fn claimable(&self, now_unix_ms: u64) -> bool {
        match self.state {
            ObjectLinkFinalizationState::Pending => self.next_attempt_unix_ms <= now_unix_ms,
            ObjectLinkFinalizationState::Running => self
                .lease_expires_unix_ms
                .is_some_and(|expiry| expiry <= now_unix_ms),
            ObjectLinkFinalizationState::Complete => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job() -> ObjectLinkFinalizationJob {
        ObjectLinkFinalizationJob {
            schema: ObjectLinkFinalizationJob::SCHEMA.into(),
            cluster_id: "cluster".into(),
            transaction_id: "transaction".into(),
            tenant_id: 1,
            bucket_id: 2,
            bucket_name: "bucket".into(),
            link_key: "links/latest".into(),
            generation: 3,
            operation: ObjectLinkFinalizationOperation::Put,
            target_key: Some("objects/version".into()),
            target_version_id: Some(uuid::Uuid::from_u128(1).to_string()),
            mutation_id: uuid::Uuid::from_u128(2).to_string(),
            consequences: ObjectLinkFinalizationConsequences {
                maintain_indexes: true,
                compact_metadata: true,
            },
        }
    }

    #[test]
    fn encoding_is_canonical_and_identity_is_stable() {
        let job = job();
        let encoded = job.encode().unwrap();
        assert_eq!(ObjectLinkFinalizationJob::decode(&encoded).unwrap(), job);
        assert_eq!(
            job.target_logical_identity(),
            "tenant/1/bucket/2/object-link/links/latest/generation/3"
        );
        assert_eq!(job.job_id().unwrap(), job.job_id().unwrap());
    }

    #[test]
    fn operation_and_consequence_invariants_are_enforced() {
        let mut invalid = job();
        invalid.target_key = None;
        assert!(invalid.encode().is_err());

        let mut invalid = job();
        invalid.operation = ObjectLinkFinalizationOperation::Delete;
        assert!(invalid.encode().is_err());

        let mut invalid = job();
        invalid.consequences.maintain_indexes = false;
        invalid.consequences.compact_metadata = false;
        assert!(invalid.encode().is_err());
    }
}
