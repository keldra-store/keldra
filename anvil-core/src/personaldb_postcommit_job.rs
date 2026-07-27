use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersonalDbPostCommitJob {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    pub tenant_id: i64,
    pub database_id: String,
    pub principal: String,
    pub log_index: u64,
    pub log_hash: String,
    pub authz_revision: u64,
    pub schema_sql: String,
    pub changeset_bytes: Vec<u8>,
    pub envelope: Value,
    pub committed_head_hash: String,
    /// Definitions whose target mutations were committed in the originating
    /// transaction and must not be applied a second time during fanout.
    pub excluded_projection_ids: Vec<String>,
}

impl PersonalDbPostCommitJob {
    pub const SCHEMA: &'static str = "anvil.mvcc.personaldb-postcommit-job.v2";

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let job: Self = serde_json::from_slice(bytes)?;
        if job.encode()? != bytes {
            bail!("PersonalDB postcommit job is not canonically encoded");
        }
        Ok(job)
    }

    pub fn job_id(&self) -> Result<String> {
        Ok(hex::encode(blake3::hash(&self.encode()?).as_bytes()))
    }

    pub fn target_logical_identity(&self) -> String {
        format!(
            "tenant/{}/personaldb/{}",
            self.tenant_id, self.database_id
        )
    }

    fn validate(&self) -> Result<()> {
        if self.schema != Self::SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self.tenant_id < 0
            || self.database_id.trim().is_empty()
            || self.principal.trim().is_empty()
            || self.log_index == 0
            || self.log_hash.trim().is_empty()
            || self.schema_sql.trim().is_empty()
            || !self.envelope.is_object()
            || self.committed_head_hash.trim().is_empty()
            || self
                .excluded_projection_ids
                .iter()
                .any(|projection_id| projection_id.trim().is_empty())
        {
            bail!("invalid PersonalDB postcommit job");
        }
        let mut canonical = self.excluded_projection_ids.clone();
        canonical.sort();
        canonical.dedup();
        if canonical != self.excluded_projection_ids {
            bail!("PersonalDB postcommit projection exclusions are not canonical");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersonalDbPostCommitState {
    Pending,
    Running,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersonalDbPostCommitRecord {
    pub job: PersonalDbPostCommitJob,
    pub commit_version: u64,
    pub state: PersonalDbPostCommitState,
    pub attempts: u32,
    pub next_attempt_unix_ms: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_unix_ms: Option<u64>,
    pub last_error: Option<String>,
}

impl PersonalDbPostCommitRecord {
    pub fn pending(job: PersonalDbPostCommitJob, commit_version: u64) -> Self {
        Self {
            job,
            commit_version,
            state: PersonalDbPostCommitState::Pending,
            attempts: 0,
            next_attempt_unix_ms: 0,
            lease_owner: None,
            lease_expires_unix_ms: None,
            last_error: None,
        }
    }

    pub fn claimable(&self, now: u64) -> bool {
        match self.state {
            PersonalDbPostCommitState::Pending => self.next_attempt_unix_ms <= now,
            PersonalDbPostCommitState::Running => {
                self.lease_expires_unix_ms.is_some_and(|expiry| expiry <= now)
            }
            PersonalDbPostCommitState::Complete => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(log_index: u64) -> PersonalDbPostCommitJob {
        PersonalDbPostCommitJob {
            schema: PersonalDbPostCommitJob::SCHEMA.into(),
            cluster_id: "cluster-a".into(),
            transaction_id: format!("transaction-{log_index}"),
            tenant_id: 1,
            database_id: "db-a".into(),
            principal: "app-a".into(),
            log_index,
            log_hash: hex::encode([log_index as u8; 32]),
            authz_revision: 7,
            schema_sql: "CREATE TABLE rows(id INTEGER PRIMARY KEY);".into(),
            changeset_bytes: vec![1, 2, 3],
            envelope: serde_json::json!({"format_version": 1}),
            committed_head_hash: hex::encode([9; 32]),
            excluded_projection_ids: Vec::new(),
        }
    }

    #[test]
    fn canonical_job_round_trips_and_assignment_serializes_database_commits() {
        let first = job(1);
        let second = job(2);
        assert_eq!(
            PersonalDbPostCommitJob::decode(&first.encode().unwrap()).unwrap(),
            first
        );
        assert_eq!(
            first.target_logical_identity(),
            second.target_logical_identity()
        );
        assert_ne!(first.job_id().unwrap(), second.job_id().unwrap());
    }

    #[test]
    fn projection_exclusions_must_be_sorted_and_unique() {
        let mut value = job(1);
        value.excluded_projection_ids = vec!["db-b/projection".into(), "db-a/projection".into()];
        assert!(value.encode().is_err());
        value.excluded_projection_ids.sort();
        assert!(value.encode().is_ok());
        value.excluded_projection_ids.push("db-b/projection".into());
        assert!(value.encode().is_err());
    }
}
