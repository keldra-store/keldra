use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMaterialisationJob {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub object_key: String,
    pub object_version_id: String,
    pub target_logical_identity: String,
    pub representation: Value,
    pub payload_length: u64,
    pub content_type: Option<String>,
    pub user_metadata: Value,
    pub index_policy_snapshot: Value,
    pub authz_revision: i64,
    pub boundary_schema: Option<Value>,
    pub boundary_schema_generation: u64,
    pub boundary_schema_hash: Option<String>,
    pub requested_operations: ObjectMaterialisationOperations,
    pub requested_at_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMaterialisationOperations {
    pub extract_boundaries: bool,
    pub maintain_indexes: bool,
}

impl ObjectMaterialisationJob {
    pub const SCHEMA: &'static str = "anvil.mvcc.object-materialisation-job.v1";

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    pub fn job_id(&self) -> Result<String> {
        Ok(hex::encode(Sha256::digest(self.canonical_bytes()?)))
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema != Self::SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self.bucket_name.trim().is_empty()
            || self.object_key.is_empty()
            || self.object_version_id.trim().is_empty()
            || self.target_logical_identity.trim().is_empty()
            || self.requested_at_unix_ms == 0
            || !self.representation.is_object()
            || !self.user_metadata.is_object()
            || !self.index_policy_snapshot.is_object()
            || self
                .boundary_schema
                .as_ref()
                .is_some_and(|schema| !schema.is_object())
            || (self.boundary_schema.is_some()
                != self
                    .boundary_schema_hash
                    .as_ref()
                    .is_some_and(|hash| !hash.is_empty()))
            || (!self.requested_operations.extract_boundaries
                && !self.requested_operations.maintain_indexes)
        {
            bail!("invalid object materialisation job");
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let job: Self = serde_json::from_slice(bytes)?;
        job.validate()?;
        if job.canonical_bytes()? != bytes {
            bail!("object materialisation job is not canonically encoded");
        }
        Ok(job)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMaterialisationRecord {
    pub job: ObjectMaterialisationJob,
    pub state: ObjectMaterialisationState,
    pub attempts: u32,
    pub next_attempt_unix_ms: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectMaterialisationState {
    Pending,
    Running,
    Complete,
}

impl ObjectMaterialisationRecord {
    pub fn pending(job: ObjectMaterialisationJob) -> Self {
        Self {
            job,
            state: ObjectMaterialisationState::Pending,
            attempts: 0,
            next_attempt_unix_ms: 0,
            lease_owner: None,
            lease_expires_unix_ms: None,
            last_error: None,
        }
    }

    pub fn claimable(&self, now_unix_ms: u64) -> bool {
        (self.state == ObjectMaterialisationState::Pending
            && self.next_attempt_unix_ms <= now_unix_ms)
            || (self.state == ObjectMaterialisationState::Running
                && self
                    .lease_expires_unix_ms
                    .is_some_and(|expiry| expiry <= now_unix_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_canonical_and_target_scoped() {
        let job = ObjectMaterialisationJob {
            schema: ObjectMaterialisationJob::SCHEMA.into(),
            cluster_id: "cluster".into(),
            transaction_id: "tx".into(),
            tenant_id: 1,
            bucket_id: 2,
            bucket_name: "bucket".into(),
            object_key: "key".into(),
            object_version_id: "version".into(),
            target_logical_identity: "tenant/1/bucket/2/object/key/version/version".into(),
            representation: serde_json::json!({"schema": "local"}),
            payload_length: 3,
            content_type: Some("application/json".into()),
            user_metadata: serde_json::json!({}),
            index_policy_snapshot: serde_json::json!({}),
            authz_revision: 1,
            boundary_schema: None,
            boundary_schema_generation: 0,
            boundary_schema_hash: None,
            requested_operations: ObjectMaterialisationOperations {
                extract_boundaries: true,
                maintain_indexes: true,
            },
            requested_at_unix_ms: 1,
        };
        assert_eq!(job.job_id().unwrap(), job.job_id().unwrap());
        assert_eq!(
            ObjectMaterialisationJob::decode(&job.canonical_bytes().unwrap()).unwrap(),
            job
        );
    }
}
