use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    core_store::{CF_MATERIALISATION, TABLE_OBJECT_MATERIALISATION_ROW},
    mvcc_product::coremeta_logical_key,
    mvcc_transaction::LogicalKey,
};

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
    pub content_hash: String,
    pub payload_length: u64,
    pub frozen_object: Value,
    pub source_manifest_hash: String,
    pub content_type: Option<String>,
    pub user_metadata: Value,
    pub index_policy_snapshot: Value,
    pub originating_snapshot_version: u64,
    pub frozen_index_definitions: Vec<FrozenIndexDefinition>,
    pub authz_revision: i64,
    pub boundary_schema: Option<Value>,
    pub boundary_schema_generation: u64,
    pub boundary_schema_hash: Option<String>,
    pub requested_operations: ObjectMaterialisationOperations,
    pub requested_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenIndexDefinition {
    pub id: i64,
    pub version: i64,
    pub name: String,
    pub kind: String,
    pub selector: Value,
    pub extractor: Value,
    pub authorization_mode: String,
    pub build_policy: Value,
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

    /// Keeps all derived publications for a bucket on one compact-Raft work
    /// owner. Index ownership is bucket-scoped in practice: hashing each
    /// object version independently would move consecutive builds between
    /// nodes while the prior node still holds the index publication fence.
    pub fn assignment_logical_identity(&self) -> String {
        format!(
            "tenant/{}/bucket/{}/object-materialisation",
            self.tenant_id, self.bucket_id
        )
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
            || self.content_hash.trim().is_empty()
            || !self.frozen_object.is_object()
            || !is_sha256_hash(&self.source_manifest_hash)
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
        let expected_target = format!(
            "tenant/{}/bucket/{}/object/{}/version/{}",
            self.tenant_id, self.bucket_id, self.object_key, self.object_version_id
        );
        let frozen_version = self.frozen_object.get("version_id").and_then(Value::as_str);
        let frozen_content_hash = self
            .frozen_object
            .get("content_hash")
            .and_then(Value::as_str);
        let frozen_length = self.frozen_object.get("size").and_then(Value::as_i64);
        if self.target_logical_identity != expected_target
            || frozen_version != Some(self.object_version_id.as_str())
            || frozen_content_hash != Some(self.content_hash.as_str())
            || frozen_length.and_then(|length| u64::try_from(length).ok())
                != Some(self.payload_length)
        {
            bail!("frozen object does not match materialisation target");
        }
        let mut definitions = self.frozen_index_definitions.clone();
        definitions.sort_by_key(|definition| (definition.id, definition.version));
        if definitions != self.frozen_index_definitions
            || definitions.windows(2).any(|pair| pair[0].id == pair[1].id)
        {
            bail!("frozen index definitions must be sorted and unique");
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

fn is_sha256_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMaterialisationResult {
    pub schema: String,
    pub cluster_id: String,
    pub target_logical_identity: String,
    pub job_id: String,
    pub state: ObjectMaterialisationState,
    pub boundary_schema_hash: Option<String>,
    pub derived_boundaries: Value,
    pub index_marker: Value,
    pub updated_at_unix_ms: u64,
}

impl ObjectMaterialisationResult {
    pub const SCHEMA: &'static str = "anvil.mvcc.object-materialisation-result.v1";

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        if self.schema != Self::SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.target_logical_identity.trim().is_empty()
            || self.job_id.trim().is_empty()
            || !self.derived_boundaries.is_array()
            || !self.index_marker.is_object()
            || self.updated_at_unix_ms == 0
        {
            bail!("invalid object materialisation result");
        }
        Ok(serde_json::to_vec(self)?)
    }

    pub fn result_key(&self) -> Result<LogicalKey> {
        materialisation_result_key(&self.target_logical_identity, &self.job_id)
    }

    pub fn status_key(&self) -> Result<LogicalKey> {
        materialisation_status_key(&self.target_logical_identity)
    }
}

pub fn materialisation_result_key(target: &str, job_id: &str) -> Result<LogicalKey> {
    materialisation_key(b"result", target, Some(job_id))
}

pub fn materialisation_status_key(target: &str) -> Result<LogicalKey> {
    materialisation_key(b"status", target, None)
}

fn materialisation_key(kind: &[u8], target: &str, job_id: Option<&str>) -> Result<LogicalKey> {
    if target.is_empty() || job_id.is_some_and(str::is_empty) {
        bail!("materialisation key identity is required");
    }
    let mut tuple = Vec::new();
    tuple.extend_from_slice(b"object-materialisation/");
    tuple.extend_from_slice(kind);
    tuple.push(b'/');
    push_key_part(&mut tuple, target)?;
    if let Some(job_id) = job_id {
        push_key_part(&mut tuple, job_id)?;
    }
    coremeta_logical_key(CF_MATERIALISATION, TABLE_OBJECT_MATERIALISATION_ROW, &tuple)
}

fn push_key_part(output: &mut Vec<u8>, value: &str) -> Result<()> {
    let length = u32::try_from(value.len())?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
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
            content_hash: "sha256:payload".into(),
            payload_length: 3,
            frozen_object: serde_json::json!({
                "version_id": "version",
                "content_hash": "sha256:payload",
                "size": 3,
            }),
            source_manifest_hash:
                "0000000000000000000000000000000000000000000000000000000000000000".into(),
            content_type: Some("application/json".into()),
            user_metadata: serde_json::json!({}),
            index_policy_snapshot: serde_json::json!({}),
            originating_snapshot_version: 1,
            frozen_index_definitions: Vec::new(),
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
        let mut next_object = job.clone();
        next_object.object_key = "other".into();
        next_object.object_version_id = "other-version".into();
        next_object.target_logical_identity =
            "tenant/1/bucket/2/object/other/version/other-version".into();
        assert_eq!(
            job.assignment_logical_identity(),
            next_object.assignment_logical_identity()
        );
    }

    #[test]
    fn result_and_status_keys_are_distinct_and_target_scoped() {
        let status = materialisation_status_key("object/version").unwrap();
        let result = materialisation_result_key("object/version", "job").unwrap();
        assert_ne!(status, result);
        assert_ne!(
            result,
            materialisation_result_key("object/other", "job").unwrap()
        );
        assert_eq!(status.table_id, TABLE_OBJECT_MATERIALISATION_ROW);
    }
}
