use std::time::{SystemTime, UNIX_EPOCH};

use anvil_api::v1::{
    AccountingDefinition as ApiDefinition, AccountingFreshness as ApiFreshness,
    AccountingSnapshot as ApiSnapshot, AccountingSourceCheckpoint as ApiSourceCheckpoint,
};
use anvil_store::{LocalChange, ObjectKey, PlacementLogId, SourceId, VersionId};
use prost_types::Timestamp;
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::index_runtime::events::IndexBarrier;

const DEFINITION_FORMAT: u16 = 1;
const ROLLUP_FORMAT: u16 = 2;
const OUTBOUND_SOURCE_FORMAT: u16 = 2;
const MAX_TRAFFIC_FLUSH_ID_BYTES: usize = 256;
const DEFINITION_PREFIX: &str = "_anvil/accounting/definitions/";
const ACCOUNTING_ROOT: &str = "_anvil/accounting/";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredAccountingDefinition {
    format: u16,
    pub(crate) storage_tenant: String,
    pub(crate) bucket: String,
    pub(crate) path_prefix: String,
    pub(crate) accounting_id: u64,
}

impl StoredAccountingDefinition {
    pub(crate) fn create(
        storage_tenant: String,
        bucket: String,
        path_prefix: String,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Self, Status> {
        validate_prefix(&path_prefix)?;
        if storage_tenant.is_empty() || bucket.is_empty() || tenant_id == 0 || bucket_id == 0 {
            return Err(Status::invalid_argument(
                "accounting tenant, bucket, and stable IDs must be non-empty",
            ));
        }
        Ok(Self {
            format: DEFINITION_FORMAT,
            accounting_id: derive_accounting_id(tenant_id, bucket_id, &path_prefix),
            storage_tenant,
            bucket,
            path_prefix,
        })
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, Status> {
        serde_json::to_vec(self)
            .map_err(|error| Status::internal(format!("encode accounting definition: {error}")))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Status> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|error| Status::data_loss(format!("decode accounting definition: {error}")))?;
        if value.format != DEFINITION_FORMAT || value.accounting_id == 0 {
            return Err(Status::data_loss(
                "accounting definition has an unsupported format or zero identity",
            ));
        }
        validate_prefix(&value.path_prefix)
            .map_err(|error| Status::data_loss(error.message().to_owned()))?;
        Ok(value)
    }

    pub(crate) fn to_api(&self, version: VersionId) -> Result<ApiDefinition, Status> {
        if version.0 == 0 {
            return Err(Status::data_loss(
                "accounting definition has a zero object version",
            ));
        }
        Ok(ApiDefinition {
            storage_tenant: self.storage_tenant.clone(),
            bucket: self.bucket.clone(),
            path_prefix: self.path_prefix.clone(),
            accounting_id: self.accounting_id,
            version: version.0,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredAccountingRollup {
    format: u16,
    pub(crate) accounting_id: u64,
    pub(crate) definition_version: u64,
    pub(crate) logical_stored_bytes: u64,
    pub(crate) object_count: u64,
    pub(crate) accepted_inbound_bytes: u64,
    pub(crate) served_outbound_bytes: u64,
    pub(crate) refreshed_at_unix_millis: u64,
    pub(crate) complete: bool,
    pub(crate) placement_fence: PlacementLogId,
    pub(crate) atomic_finalized_through: Option<u64>,
    pub(crate) sources: Vec<StoredSourceCheckpoint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) traffic_sources: Vec<StoredTrafficCheckpoint>,
}

impl StoredAccountingRollup {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        accounting_id: u64,
        definition_version: u64,
        logical_stored_bytes: u64,
        object_count: u64,
        accepted_inbound_bytes: u64,
        served_outbound_bytes: u64,
        complete: bool,
        barrier: &IndexBarrier,
        traffic_sources: Vec<StoredTrafficCheckpoint>,
    ) -> Result<Self, Status> {
        if accounting_id == 0 || definition_version == 0 {
            return Err(Status::invalid_argument(
                "accounting rollup identities must be non-zero",
            ));
        }
        Ok(Self {
            format: ROLLUP_FORMAT,
            accounting_id,
            definition_version,
            logical_stored_bytes,
            object_count,
            accepted_inbound_bytes,
            served_outbound_bytes,
            refreshed_at_unix_millis: unix_millis(SystemTime::now())?,
            complete,
            placement_fence: barrier.fence,
            atomic_finalized_through: barrier.atomic.finalized_through(),
            sources: barrier
                .sources
                .iter()
                .map(|(node, cursor)| StoredSourceCheckpoint {
                    node_id: node.0,
                    source: cursor.source,
                    through_offset: cursor.next_offset.saturating_sub(1),
                })
                .collect(),
            traffic_sources,
        })
    }

    pub(crate) fn barrier(&self) -> Result<IndexBarrier, Status> {
        let mut sources = std::collections::BTreeMap::new();
        for source in &self.sources {
            let node = anvil_consensus::NodeId(source.node_id);
            let next_offset = source.through_offset.checked_add(1).ok_or_else(|| {
                Status::data_loss("accounting source checkpoint offset is exhausted")
            })?;
            if sources
                .insert(
                    node,
                    crate::index_runtime::events::IndexSourceCursor {
                        source: source.source,
                        next_offset,
                    },
                )
                .is_some()
            {
                return Err(Status::data_loss(
                    "accounting rollup repeats a source checkpoint",
                ));
            }
        }
        Ok(IndexBarrier {
            fence: self.placement_fence,
            atomic: crate::index_runtime::events::AtomicProgramWatermark::new(
                self.atomic_finalized_through,
                self.atomic_finalized_through,
                0,
            ),
            sources,
        })
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, Status> {
        serde_json::to_vec(self)
            .map_err(|error| Status::internal(format!("encode accounting rollup: {error}")))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Status> {
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|error| Status::data_loss(format!("decode accounting rollup: {error}")))?;
        if value.format != ROLLUP_FORMAT
            || value.accounting_id == 0
            || value.definition_version == 0
        {
            return Err(Status::data_loss(
                "accounting rollup has an unsupported format or zero identity",
            ));
        }
        let mut nodes = value
            .sources
            .iter()
            .map(|source| source.node_id)
            .collect::<Vec<_>>();
        let original = nodes.clone();
        nodes.sort_unstable();
        nodes.dedup();
        if nodes != original || value.sources.iter().any(|source| source.node_id == 0) {
            return Err(Status::data_loss(
                "accounting rollup source checkpoints are not strictly ordered",
            ));
        }
        let traffic_nodes = value
            .traffic_sources
            .iter()
            .map(|source| source.node_id)
            .collect::<Vec<_>>();
        if traffic_nodes.iter().any(|node| *node == 0)
            || traffic_nodes.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Status::data_loss(
                "accounting traffic checkpoints are not strictly ordered",
            ));
        }
        Ok(value)
    }

    pub(crate) fn to_api(
        &self,
        definition: &StoredAccountingDefinition,
        definition_version: VersionId,
    ) -> Result<ApiSnapshot, Status> {
        if self.accounting_id != definition.accounting_id
            || self.definition_version != definition_version.0
        {
            return Err(Status::failed_precondition(
                "accounting rollup belongs to another definition generation",
            ));
        }
        let refreshed_at = millis_timestamp(self.refreshed_at_unix_millis)?;
        Ok(ApiSnapshot {
            definition: Some(definition.to_api(definition_version)?),
            logical_stored_bytes: self.logical_stored_bytes,
            object_count: self.object_count,
            accepted_inbound_bytes: self.accepted_inbound_bytes,
            served_outbound_bytes: self.served_outbound_bytes,
            freshness: Some(ApiFreshness {
                refreshed_at: Some(refreshed_at),
                sources: self
                    .sources
                    .iter()
                    .map(|source| ApiSourceCheckpoint {
                        node_id: source.node_id,
                        source_epoch: source.source.source_epoch.to_vec(),
                        through_offset: source.through_offset,
                    })
                    .collect(),
                complete: self.complete,
            }),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredSourceCheckpoint {
    pub(crate) node_id: u64,
    pub(crate) source: SourceId,
    pub(crate) through_offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredTrafficCheckpoint {
    pub(crate) node_id: u64,
    pub(crate) accepted_inbound_bytes: u64,
    pub(crate) served_outbound_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredTrafficSource {
    format: u16,
    pub(crate) accounting_id: u64,
    pub(crate) definition_version: u64,
    pub(crate) node_id: u64,
    pub(crate) accepted_inbound_bytes: u64,
    pub(crate) served_outbound_bytes: u64,
    pub(crate) last_flush_id: String,
    pub(crate) updated_at_unix_millis: u64,
}

impl StoredTrafficSource {
    pub(crate) fn new(
        accounting_id: u64,
        definition_version: u64,
        node_id: u64,
        accepted_inbound_bytes: u64,
        served_outbound_bytes: u64,
        last_flush_id: String,
    ) -> Result<Self, Status> {
        if accounting_id == 0 || definition_version == 0 || node_id == 0 {
            return Err(Status::invalid_argument(
                "accounting outbound source identities must be non-zero",
            ));
        }
        validate_traffic_flush_id(&last_flush_id).map_err(Status::invalid_argument)?;
        Ok(Self {
            format: OUTBOUND_SOURCE_FORMAT,
            accounting_id,
            definition_version,
            node_id,
            accepted_inbound_bytes,
            served_outbound_bytes,
            last_flush_id,
            updated_at_unix_millis: unix_millis(SystemTime::now())?,
        })
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, Status> {
        serde_json::to_vec(self).map_err(|error| {
            Status::internal(format!("encode accounting outbound source: {error}"))
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Status> {
        let value: Self = serde_json::from_slice(bytes).map_err(|error| {
            Status::data_loss(format!("decode accounting outbound source: {error}"))
        })?;
        if value.format != OUTBOUND_SOURCE_FORMAT
            || value.accounting_id == 0
            || value.definition_version == 0
            || value.node_id == 0
            || validate_traffic_flush_id(&value.last_flush_id).is_err()
        {
            return Err(Status::data_loss(
                "accounting outbound source has an invalid identity or format",
            ));
        }
        Ok(value)
    }
}

fn validate_traffic_flush_id(value: &str) -> Result<(), &'static str> {
    if value.is_empty() || value.len() > MAX_TRAFFIC_FLUSH_ID_BYTES || value.contains('\0') {
        Err("accounting traffic flush ID must contain 1 to 256 bytes and no NUL")
    } else {
        Ok(())
    }
}

pub(crate) fn derive_accounting_id(tenant_id: u64, bucket_id: u64, prefix: &str) -> u64 {
    let mut hasher = blake3::Hasher::new_derive_key("anvil.accounting/definition-id/v1");
    hasher.update(&tenant_id.to_be_bytes());
    hasher.update(&bucket_id.to_be_bytes());
    hasher.update(&(prefix.len() as u64).to_be_bytes());
    hasher.update(prefix.as_bytes());
    let mut word = [0_u8; 8];
    word.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    u64::from_be_bytes(word).max(1)
}

pub(crate) fn definition_path(accounting_id: u64) -> Result<String, Status> {
    nonzero_path(accounting_id, |id| format!("{DEFINITION_PREFIX}{id}"))
}

pub(crate) fn current_path(accounting_id: u64) -> Result<String, Status> {
    nonzero_path(accounting_id, |id| format!("{ACCOUNTING_ROOT}{id}/current"))
}

pub(crate) fn outbound_source_path(accounting_id: u64, node_id: u64) -> Result<String, Status> {
    if node_id == 0 {
        return Err(Status::invalid_argument(
            "accounting source node ID must be non-zero",
        ));
    }
    nonzero_path(accounting_id, |id| {
        format!("{ACCOUNTING_ROOT}{id}/sources/{node_id}")
    })
}

pub(crate) fn definition_id_from_path(path: &str) -> Option<u64> {
    canonical_id(path.strip_prefix(DEFINITION_PREFIX)?)
}

pub(crate) fn is_artifact_path(path: &str, expected_id: u64) -> bool {
    definition_path(expected_id).ok().as_deref() == Some(path)
        || current_path(expected_id).ok().as_deref() == Some(path)
        || path
            .strip_prefix(&format!("{ACCOUNTING_ROOT}{expected_id}/sources/"))
            .and_then(canonical_id)
            .is_some()
}

pub(crate) fn includes_path(prefix: &str, path: &str) -> bool {
    (prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/')))
        && !is_accounting_path(path)
}

pub(crate) fn is_accounting_path(path: &str) -> bool {
    path == "_anvil/accounting" || path.starts_with("_anvil/accounting/")
}

/// Select object changes which can alter an accounting rollup. Ordinary source
/// objects and per-node traffic sources are inputs; definitions and published
/// rollups are not, so publishing a rollup cannot recursively wake itself.
pub(crate) fn is_accounting_source_change(change: &LocalChange) -> bool {
    let path = match change {
        LocalChange::ObjectHead(change) => &change.exact_path,
        LocalChange::RetainedVersionDeleted(change) => &change.exact_path,
        _ => return false,
    };
    !is_accounting_path(path) || is_outbound_source_path(path)
}

fn is_outbound_source_path(path: &str) -> bool {
    let Some(remainder) = path.strip_prefix(ACCOUNTING_ROOT) else {
        return false;
    };
    let mut segments = remainder.split('/');
    canonical_id(segments.next().unwrap_or_default()).is_some()
        && segments.next() == Some("sources")
        && segments.next().and_then(canonical_id).is_some()
        && segments.next().is_none()
}

pub(crate) fn validate_prefix(prefix: &str) -> Result<(), Status> {
    if prefix.is_empty() {
        return Ok(());
    }
    ObjectKey::new("accounting-validation", "accounting-validation", prefix)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if prefix.split('/').any(|segment| segment == "_anvil") {
        return Err(Status::invalid_argument(
            "accounting path prefix cannot select a reserved _anvil namespace",
        ));
    }
    Ok(())
}

fn canonical_id(value: &str) -> Option<u64> {
    let parsed = value.parse::<u64>().ok()?;
    (parsed != 0 && parsed.to_string() == value).then_some(parsed)
}

fn nonzero_path(accounting_id: u64, build: impl FnOnce(u64) -> String) -> Result<String, Status> {
    if accounting_id == 0 {
        return Err(Status::invalid_argument(
            "accounting identity must be non-zero",
        ));
    }
    Ok(build(accounting_id))
}

fn unix_millis(value: SystemTime) -> Result<u64, Status> {
    value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock is before the Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| Status::internal("system clock exceeds accounting timestamp range"))
}

fn millis_timestamp(value: u64) -> Result<Timestamp, Status> {
    let seconds = value / 1_000;
    Ok(Timestamp {
        seconds: seconds
            .try_into()
            .map_err(|_| Status::data_loss("accounting timestamp exceeds protobuf range"))?,
        nanos: ((value % 1_000) * 1_000_000) as i32,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LoadedAccountingDefinition {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) version: VersionId,
    pub(crate) stored: StoredAccountingDefinition,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_matching_is_segment_aware_and_excludes_self_accounting() {
        assert!(includes_path("tenants/7", "tenants/7"));
        assert!(includes_path("tenants/7", "tenants/7/a"));
        assert!(!includes_path("tenants/7", "tenants/70/a"));
        assert!(!includes_path("", "_anvil/accounting/7/current"));
        assert!(includes_path("", "_anvil/indices/7/current"));
    }

    #[test]
    fn stable_identity_uses_ids_and_prefix_not_mutable_names() {
        let first = StoredAccountingDefinition::create(
            "old-name".into(),
            "old-bucket".into(),
            "tenant/7".into(),
            11,
            12,
        )
        .unwrap();
        let renamed = StoredAccountingDefinition::create(
            "new-name".into(),
            "new-bucket".into(),
            "tenant/7".into(),
            11,
            12,
        )
        .unwrap();
        assert_eq!(first.accounting_id, renamed.accounting_id);
    }

    #[test]
    fn only_exact_reserved_shapes_are_artifacts() {
        assert!(is_artifact_path("_anvil/accounting/7/current", 7));
        assert!(is_artifact_path("_anvil/accounting/definitions/7", 7));
        assert!(is_artifact_path("_anvil/accounting/7/sources/2", 7));
        assert!(!is_artifact_path("_anvil/accounting/7/sources/02", 7));
        assert!(!is_artifact_path("_anvil/accounting/8/current", 7));
    }

    #[test]
    fn traffic_source_persists_and_validates_its_last_flush_identity() {
        let source = StoredTrafficSource::new(7, 3, 2, 10, 20, "traffic-2-9".into()).unwrap();
        assert_eq!(
            StoredTrafficSource::decode(&source.encode().unwrap()).unwrap(),
            source
        );
        assert!(StoredTrafficSource::new(7, 3, 2, 10, 20, String::new()).is_err());
        assert!(StoredTrafficSource::new(7, 3, 2, 10, 20, "bad\0id".into()).is_err());
    }
}
