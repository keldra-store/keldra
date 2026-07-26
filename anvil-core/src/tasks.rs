#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskType {
    DeleteObject,
    DeleteBucket,
    ObjectMetadataCompaction,
    IndexBuild,
    RebalanceShard,
    RepairRun,
    HFIngestion,
    AuthzMaterialization,
}

impl TaskType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeleteObject => "DELETE_OBJECT",
            Self::DeleteBucket => "DELETE_BUCKET",
            Self::ObjectMetadataCompaction => "OBJECT_METADATA_COMPACTION",
            Self::IndexBuild => "INDEX_BUILD",
            Self::RebalanceShard => "REBALANCE_SHARD",
            Self::RepairRun => "REPAIR_RUN",
            Self::HFIngestion => "HF_INGESTION",
            Self::AuthzMaterialization => "AUTHZ_MATERIALIZATION",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairRunBackend {
    Index,
    DirectoryIndex,
    AuthzDerivedIndex,
    PersonalDbLogChain,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepairRunTaskPayload {
    pub backend: RepairRunBackend,
    pub tenant_id: i64,
    pub bucket_name: Option<String>,
    pub index_name: Option<String>,
    pub derived_index_id: Option<String>,
    pub database_id: Option<String>,
    pub rebuild: bool,
    pub audit_event_id: String,
    pub request_id: String,
}

impl RepairRunTaskPayload {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.tenant_id <= 0 {
            return Err(anyhow::anyhow!("RepairRun tenant_id must be positive"));
        }
        require_repair_identity(&self.audit_event_id, "audit_event_id")?;
        require_repair_identity(&self.request_id, "request_id")?;
        let present = |value: &Option<String>| {
            value.as_deref().is_some_and(|value| {
                !value.trim().is_empty() && !value.chars().any(char::is_control)
            })
        };
        let valid = match self.backend {
            RepairRunBackend::Index => {
                present(&self.bucket_name)
                    && present(&self.index_name)
                    && self.derived_index_id.is_none()
                    && self.database_id.is_none()
            }
            RepairRunBackend::DirectoryIndex => {
                present(&self.bucket_name)
                    && self.index_name.is_none()
                    && self.derived_index_id.is_none()
                    && self.database_id.is_none()
            }
            RepairRunBackend::AuthzDerivedIndex => {
                self.bucket_name.is_none()
                    && self.index_name.is_none()
                    && present(&self.derived_index_id)
                    && self.database_id.is_none()
            }
            RepairRunBackend::PersonalDbLogChain => {
                self.bucket_name.is_none()
                    && self.index_name.is_none()
                    && self.derived_index_id.is_none()
                    && present(&self.database_id)
            }
        };
        if !valid {
            return Err(anyhow::anyhow!(
                "RepairRun payload fields do not match its backend"
            ));
        }
        Ok(())
    }

    pub fn scope_kind(&self) -> &'static str {
        match self.backend {
            RepairRunBackend::Index => "index",
            RepairRunBackend::DirectoryIndex => "bucket",
            RepairRunBackend::AuthzDerivedIndex => "authz_derived_index",
            RepairRunBackend::PersonalDbLogChain => "personaldb",
        }
    }
}

fn require_repair_identity(value: &str, field: &'static str) -> anyhow::Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(anyhow::anyhow!(
            "RepairRun {field} must be a nonempty stable identity"
        ));
    }
    Ok(())
}

/// Durable canonical manifest identity; workers re-probe effective placements at execution time.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebalanceShardTaskPayload {
    pub object_hash: String,
    pub logical_size: u64,
    pub manifest_ref: String,
    pub block_id: String,
    pub manifest_root_key_hash: String,
    pub manifest_root_generation: u64,
    pub manifest_transaction_id: String,
    pub manifest_payload_digest: String,
}

impl RebalanceShardTaskPayload {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.object_digest()?;
        require_stable_identity_component(&self.manifest_ref, "manifest_ref")?;
        require_stable_identity_component(&self.block_id, "block_id")?;
        require_canonical_digest(
            &self.manifest_root_key_hash,
            "manifest_root_key_hash",
            "sha256",
        )?;
        if self.manifest_root_generation == 0 {
            return Err(anyhow::anyhow!(
                "rebalance shard manifest_root_generation must be nonzero"
            ));
        }
        require_stable_identity_component(
            &self.manifest_transaction_id,
            "manifest_transaction_id",
        )?;
        self.manifest_payload_digest_hex()?;
        Ok(())
    }

    pub fn object_digest(&self) -> anyhow::Result<&str> {
        let digest = self
            .object_hash
            .strip_prefix("sha256:")
            .ok_or_else(|| anyhow::anyhow!("rebalance shard object_hash must use sha256:"))?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(anyhow::anyhow!(
                "rebalance shard object_hash must contain a canonical lowercase SHA-256 digest"
            ));
        }
        Ok(digest)
    }

    pub fn manifest_payload_digest_hex(&self) -> anyhow::Result<&str> {
        require_canonical_digest(
            &self.manifest_payload_digest,
            "manifest_payload_digest",
            "blake3",
        )?;
        Ok(self
            .manifest_payload_digest
            .strip_prefix("blake3:")
            .expect("validated BLAKE3 digest has its canonical prefix"))
    }

    pub(crate) fn immutable_identity_bytes(&self) -> Vec<u8> {
        let mut identity = b"anvil.rebalance_shard_task.v2".to_vec();
        append_identity_component(&mut identity, self.object_hash.as_bytes());
        identity.extend_from_slice(&self.logical_size.to_be_bytes());
        append_identity_component(&mut identity, self.manifest_ref.as_bytes());
        append_identity_component(&mut identity, self.block_id.as_bytes());
        append_identity_component(&mut identity, self.manifest_root_key_hash.as_bytes());
        identity.extend_from_slice(&self.manifest_root_generation.to_be_bytes());
        append_identity_component(&mut identity, self.manifest_transaction_id.as_bytes());
        append_identity_component(&mut identity, self.manifest_payload_digest.as_bytes());
        identity
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RepairedShardPlacement {
    pub(crate) shard_index: u16,
    pub(crate) replaced_node_id: String,
    pub(crate) replacement_node_id: String,
    pub(crate) placement_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShardRepairRetryReason {
    NoEligibleReplacementTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct UnresolvedShardPlacement {
    pub(crate) shard_index: u16,
    pub(crate) expected_node_id: String,
    pub(crate) reason: ShardRepairRetryReason,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum RebalanceShardTaskOutcome {
    VerifiedHealthy,
    Repaired {
        repaired: Vec<RepairedShardPlacement>,
    },
    Retryable {
        repaired: Vec<RepairedShardPlacement>,
        unresolved: Vec<UnresolvedShardPlacement>,
    },
}

impl RebalanceShardTaskOutcome {
    pub(crate) fn repaired(repaired: Vec<RepairedShardPlacement>) -> anyhow::Result<Self> {
        validate_repair_outcome_placements(&repaired, &[])?;
        if repaired.is_empty() {
            return Err(anyhow::anyhow!(
                "rebalance shard repaired outcome must name at least one placement"
            ));
        }
        Ok(Self::Repaired { repaired })
    }

    pub(crate) fn retryable(
        repaired: Vec<RepairedShardPlacement>,
        unresolved: Vec<UnresolvedShardPlacement>,
    ) -> anyhow::Result<Self> {
        validate_repair_outcome_placements(&repaired, &unresolved)?;
        if unresolved.is_empty() {
            return Err(anyhow::anyhow!(
                "rebalance shard retryable outcome must name an unresolved placement"
            ));
        }
        Ok(Self::Retryable {
            repaired,
            unresolved,
        })
    }

    pub(crate) fn overlays_published(&self) -> bool {
        match self {
            Self::VerifiedHealthy => false,
            Self::Repaired { repaired } | Self::Retryable { repaired, .. } => !repaired.is_empty(),
        }
    }

    pub(crate) fn requires_retry(&self) -> bool {
        matches!(self, Self::Retryable { .. })
    }

    pub(crate) fn unresolved_placements(&self) -> &[UnresolvedShardPlacement] {
        match self {
            Self::Retryable { unresolved, .. } => unresolved,
            Self::VerifiedHealthy | Self::Repaired { .. } => &[],
        }
    }
}

fn validate_repair_outcome_placements(
    repaired: &[RepairedShardPlacement],
    unresolved: &[UnresolvedShardPlacement],
) -> anyhow::Result<()> {
    let mut indices = std::collections::BTreeSet::new();
    for index in repaired
        .iter()
        .map(|placement| placement.shard_index)
        .chain(unresolved.iter().map(|placement| placement.shard_index))
    {
        if !indices.insert(index) {
            return Err(anyhow::anyhow!(
                "rebalance shard outcome accounts for shard {index} more than once"
            ));
        }
    }
    Ok(())
}

fn require_stable_identity_component(value: &str, field: &'static str) -> anyhow::Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(anyhow::anyhow!(
            "rebalance shard {field} must be a nonempty stable identity"
        ));
    }
    Ok(())
}

fn require_canonical_digest(
    value: &str,
    field: &'static str,
    expected_algorithm: &'static str,
) -> anyhow::Result<()> {
    let Some((algorithm, digest)) = value.split_once(':') else {
        return Err(anyhow::anyhow!(
            "rebalance shard {field} must use {expected_algorithm}:hex encoding"
        ));
    };
    if algorithm != expected_algorithm
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(anyhow::anyhow!(
            "rebalance shard {field} must contain a canonical lowercase {expected_algorithm} digest"
        ));
    }
    Ok(())
}

fn append_identity_component(identity: &mut Vec<u8>, value: &[u8]) {
    identity.extend_from_slice(&(value.len() as u64).to_be_bytes());
    identity.extend_from_slice(value);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HFIngestionState {
    Queued,
    Running,
    Completed,
    Failed,
    Canceled,
}

impl HFIngestionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HFIngestionItemState {
    Queued,
    Downloading,
    Stored,
    Failed,
    Skipped,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebalance_shard_payload_serde_and_identity_are_stable() {
        let digest = "ab".repeat(32);
        let payload = RebalanceShardTaskPayload {
            object_hash: format!("sha256:{digest}"),
            logical_size: 4_096,
            manifest_ref: format!("core-manifest-sha256:{digest}:profile:ec-4-2"),
            block_id: "object-block-a".to_string(),
            manifest_root_key_hash: format!("sha256:{}", "cd".repeat(32)),
            manifest_root_generation: 7,
            manifest_transaction_id: "manifest-mutation-a".to_string(),
            manifest_payload_digest: format!("blake3:{}", "ef".repeat(32)),
        };

        payload.validate().unwrap();
        assert_eq!(payload.object_digest().unwrap(), digest);
        assert_eq!(
            payload.manifest_payload_digest_hex().unwrap(),
            "ef".repeat(32)
        );
        let encoded = serde_json::to_string(&payload).unwrap();
        assert_eq!(
            encoded,
            format!(
                "{{\"object_hash\":\"sha256:{digest}\",\"logical_size\":4096,\"manifest_ref\":\"core-manifest-sha256:{digest}:profile:ec-4-2\",\"block_id\":\"object-block-a\",\"manifest_root_key_hash\":\"sha256:{}\",\"manifest_root_generation\":7,\"manifest_transaction_id\":\"manifest-mutation-a\",\"manifest_payload_digest\":\"blake3:{}\"}}",
                "cd".repeat(32),
                "ef".repeat(32),
            )
        );
        assert_eq!(
            serde_json::from_str::<RebalanceShardTaskPayload>(&encoded).unwrap(),
            payload
        );

        let mut transient = serde_json::to_value(&payload).unwrap();
        transient["suspected_missing_indices"] = serde_json::json!([1, 4]);
        assert!(serde_json::from_value::<RebalanceShardTaskPayload>(transient).is_err());

        let identity = payload.immutable_identity_bytes();
        assert_eq!(identity, payload.immutable_identity_bytes());
        let mut changed = payload.clone();
        changed.logical_size += 1;
        assert_ne!(identity, changed.immutable_identity_bytes());

        changed = payload.clone();
        changed.manifest_root_generation += 1;
        assert_ne!(identity, changed.immutable_identity_bytes());

        changed = payload.clone();
        changed.manifest_payload_digest = format!("blake3:{}", "01".repeat(32));
        assert_ne!(identity, changed.immutable_identity_bytes());

        changed = payload;
        changed.manifest_root_generation = 0;
        assert!(changed.validate().is_err());
    }

    #[test]
    fn rebalance_shard_outcome_keeps_unresolved_placements_retryable() {
        let repaired = RepairedShardPlacement {
            shard_index: 1,
            replaced_node_id: "node-a".to_string(),
            replacement_node_id: "node-b".to_string(),
            placement_generation: 2,
        };
        let unresolved = UnresolvedShardPlacement {
            shard_index: 4,
            expected_node_id: "node-e".to_string(),
            reason: ShardRepairRetryReason::NoEligibleReplacementTarget,
        };
        let outcome =
            RebalanceShardTaskOutcome::retryable(vec![repaired.clone()], vec![unresolved.clone()])
                .unwrap();

        assert!(outcome.overlays_published());
        assert!(outcome.requires_retry());
        assert_eq!(outcome.unresolved_placements(), &[unresolved]);
        assert!(RebalanceShardTaskOutcome::repaired(vec![repaired]).is_ok());
        assert!(RebalanceShardTaskOutcome::retryable(Vec::new(), Vec::new()).is_err());
    }

    #[test]
    fn repair_run_payload_accepts_exact_backend_fields_only() {
        let payload = RepairRunTaskPayload {
            backend: RepairRunBackend::Index,
            tenant_id: 7,
            bucket_name: Some("documents".to_string()),
            index_name: Some("search".to_string()),
            derived_index_id: None,
            database_id: None,
            rebuild: true,
            audit_event_id: "audit:admin:req-1".to_string(),
            request_id: "req-1".to_string(),
        };
        payload.validate().unwrap();
        assert_eq!(payload.scope_kind(), "index");

        let mut invalid = payload.clone();
        invalid.database_id = Some("db-1".to_string());
        assert!(invalid.validate().is_err());

        let encoded = serde_json::to_value(&payload).unwrap();
        assert_eq!(
            serde_json::from_value::<RepairRunTaskPayload>(encoded).unwrap(),
            payload
        );
    }
}
