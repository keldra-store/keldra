use crate::core_store::{
    CF_MESH, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreMutationRootPublication, CoreStore, CoreTransaction, CoreTransactionUpdate,
    TABLE_MESH_NODE_ROW, TABLE_MESH_PARTITION_ROW, core_meta_payload_digest,
    core_meta_record_tuple_key, core_meta_tuple_key,
};
use crate::formats::writer::WriterFamily;
use crate::mesh_control_stream::{
    ControlMutationHeaderInput, ControlRecordDigest, ControlStreamFrame, ControlStreamSequence,
    read_control_checkpoint,
};
use crate::mesh_directory::{self, BucketLocatorDescriptor, BucketLocatorStatus};
use crate::partition_fence::{self, PartitionWritePermit};
use crate::routing::{self, HostAliasDescriptor, HostAliasState, RoutingConfig};
use crate::storage::Storage;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

mod bootstrap;
mod cell;
mod host_alias;
mod node;
mod portable_snapshot;
mod record_proto;
mod region;
mod topology_activation;
mod topology_mutation;
pub use bootstrap::{BootstrapMeshLifecycleProjection, install_bootstrap_lifecycle_projection};
pub use cell::*;
pub use host_alias::{
    create_host_alias, create_host_alias_in_transaction, list_host_aliases, transition_host_alias,
    transition_host_alias_in_transaction,
};
pub use node::*;
pub(crate) use portable_snapshot::validate_portable_lifecycle_topology_snapshot;
pub use region::*;
pub(crate) use topology_activation::is_synthetic_control_node_id;
pub use topology_activation::{
    CANONICAL_METADATA_QUORUM_PROFILE, CANONICAL_TOPOLOGY_ACTIVATION_SCHEMA,
    CanonicalTopologyActivation,
};

pub const REGION_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.region.v1";
pub const CELL_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.cell.v1";
pub const NODE_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.node.v1";
pub const LIFECYCLE_TOPOLOGY_HEAD_SCHEMA: &str = "anvil.mesh.lifecycle_topology_head.v1";
pub const ACTIVATION_CHECKPOINT_SCHEMA: &str = "anvil.mesh.activation_checkpoint.v1";
pub const BUCKET_DRAIN_EXCEPTION_SCHEMA: &str = "anvil.mesh.bucket_drain_exception.v1";
pub const REGION_DESCRIPTOR_STREAM_FAMILY: &str = "region_descriptor";
pub const CELL_DESCRIPTOR_STREAM_FAMILY: &str = "cell_descriptor";
pub const NODE_DESCRIPTOR_STREAM_FAMILY: &str = "node_descriptor";
const CONTROL_MUTATION_SCHEMA: &str = "anvil.mesh.control_mutation.v1";
const MESH_LIFECYCLE_PROJECTION_PARTITION_ID: &str = "mesh-lifecycle-projection";
pub(crate) const LIFECYCLE_TOPOLOGY_ROOT_ANCHOR_KEY: &str = "mesh/lifecycle/topology";
const LIFECYCLE_PROJECTION_PAGE_SIZE: usize = 256;
const MAX_LIFECYCLE_PROJECTION_ROWS_PER_TABLE: usize = 8_192;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshLifecycleCommittedResource {
    Region {
        region: String,
    },
    Cell {
        region: String,
        cell_id: String,
    },
    Node {
        region: String,
        cell_id: String,
        node_id: String,
    },
}

#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{resource_kind} {resource_id} already exists")]
    AlreadyExists {
        resource_kind: &'static str,
        resource_id: String,
    },
    #[error("{resource_kind} {resource_id} not found")]
    NotFound {
        resource_kind: &'static str,
        resource_id: String,
    },
    #[error(
        "{resource_kind} {resource_id} generation conflict: expected {expected}, current {current}"
    )]
    GenerationConflict {
        resource_kind: &'static str,
        resource_id: String,
        expected: u64,
        current: u64,
    },
    #[error("lifecycle transition denied for {resource_kind} {resource_id}: {from:?} -> {to:?}")]
    LifecycleTransitionDenied {
        resource_kind: &'static str,
        resource_id: String,
        from: LifecycleState,
        to: LifecycleState,
    },
    #[error(
        "ActivationCheckpointNotReached: control stream {stream_family}/{partition} has not reached sequence {sequence} with digest {expected_digest}: {reason}"
    )]
    ActivationCheckpointNotReached {
        stream_family: String,
        partition: String,
        sequence: u64,
        expected_digest: String,
        reason: String,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type LifecycleResult<T> = Result<T, LifecycleError>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Joining,
    Active,
    ReadOnly,
    Draining,
    Drained,
    DrainedWithExceptions,
    Offline,
    Removed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeCapability {
    Object,
    Index,
    PersonalDb,
    Metadata,
    Gateway,
    Admin,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum BucketDrainDisposition {
    BlockUntilEmpty,
    RemainProxyOnly,
    ReadOnlyUntilRemoved,
    DeleteAfterRetention,
}

impl BucketDrainDisposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BlockUntilEmpty => "block_until_empty",
            Self::RemainProxyOnly => "remain_proxy_only",
            Self::ReadOnlyUntilRemoved => "read_only_until_removed",
            Self::DeleteAfterRetention => "delete_after_retention",
        }
    }

    pub fn allows_drained_exception(self) -> bool {
        matches!(self, Self::RemainProxyOnly | Self::ReadOnlyUntilRemoved)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeDrainDescriptor {
    pub started_at: String,
    pub graceful_timeout_ms: u64,
    pub force_after_timeout: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeDescriptor {
    pub schema: String,
    pub mesh_id: String,
    pub node_id: String,
    pub region: String,
    pub cell_id: String,
    pub receipt_signing_public_key: Vec<u8>,
    pub public_api_addr: String,
    pub capabilities: Vec<NodeCapability>,
    pub capacity_json_hash: String,
    pub state: LifecycleState,
    pub drain: Option<NodeDrainDescriptor>,
    pub last_heartbeat_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegionDescriptor {
    pub schema: String,
    pub mesh_id: String,
    pub region: String,
    pub state: LifecycleState,
    pub public_base_url: String,
    pub virtual_host_suffix: String,
    pub placement_weight: u32,
    pub default_cell: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CellDescriptor {
    pub schema: String,
    pub mesh_id: String,
    pub region: String,
    pub cell_id: String,
    pub state: LifecycleState,
    pub placement_weight: u32,
    pub failure_domain: String,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRegionDescriptor {
    pub mesh_id: String,
    pub region: String,
    pub public_base_url: String,
    pub virtual_host_suffix: String,
    pub placement_weight: u32,
    pub default_cell: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterCellDescriptor {
    pub mesh_id: String,
    pub region: String,
    pub cell_id: String,
    pub placement_weight: u32,
    pub failure_domain: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterNodeDescriptor {
    pub mesh_id: String,
    pub node_id: String,
    pub region: String,
    pub cell_id: String,
    pub receipt_signing_public_key: Vec<u8>,
    pub public_api_addr: String,
    pub capabilities: Vec<NodeCapability>,
    pub capacity_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateHostAliasDescriptor {
    pub hostname: String,
    pub tenant_id: String,
    pub bucket_name: String,
    pub region: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActivationCheckpoint {
    pub schema: String,
    pub mesh_id: String,
    pub region: String,
    pub created_at: String,
    #[serde(default)]
    pub required_streams: Vec<ActivationCheckpointStream>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActivationCheckpointStream {
    pub stream_family: String,
    pub partition: String,
    pub sequence: ControlStreamSequence,
    pub digest: ControlRecordDigest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketDrainExceptionDescriptor {
    pub schema: String,
    pub tenant_id: String,
    pub bucket_name: String,
    pub region: String,
    pub disposition: BucketDrainDisposition,
    pub reason: String,
    pub expires_at: Option<String>,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDrainExceptionInput {
    pub tenant_id: String,
    pub bucket_name: String,
    pub region: String,
    pub disposition: BucketDrainDisposition,
    pub reason: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct LifecycleControlWriteAuthority<'a> {
    pub permit: &'a PartitionWritePermit,
    pub signing_key: &'a [u8],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecycleTopologyHead {
    pub schema: String,
    pub mesh_id: String,
    pub topology_hash: String,
    pub generation: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MeshLifecycleState {
    pub regions: BTreeMap<String, RegionDescriptor>,
    pub cells: BTreeMap<String, CellDescriptor>,
    pub nodes: BTreeMap<String, NodeDescriptor>,
    #[serde(default)]
    pub host_aliases: BTreeMap<String, HostAliasDescriptor>,
    #[serde(default)]
    pub bucket_drain_exceptions: BTreeMap<String, BucketDrainExceptionDescriptor>,
    #[serde(default)]
    pub canonical_topology_activation: Option<CanonicalTopologyActivation>,
    #[serde(default)]
    pub topology_head: Option<LifecycleTopologyHead>,
}

pub async fn read_state(storage: &Storage) -> LifecycleResult<MeshLifecycleState> {
    let mut state = read_lifecycle_state_projection(storage).await?;
    overlay_lifecycle_control_streams(storage, &mut state).await?;
    Ok(state)
}

pub async fn read_state_with_core_store(
    storage: &Storage,
    store: &CoreStore,
) -> LifecycleResult<MeshLifecycleState> {
    let mut state = read_lifecycle_state_projection_with_core_store(store)?;
    overlay_lifecycle_control_streams_with_store(storage, store, &mut state).await?;
    Ok(state)
}

async fn read_state_for_transaction(
    storage: &Storage,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<MeshLifecycleState> {
    let mut state = read_state(storage).await?;
    let store = CoreStore::new(storage.clone()).await?;
    let transaction = store
        .read_explicit_transaction_for_principal(transaction_id, principal)
        .await?;
    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::CoreMetaPut {
            cf,
            table_id,
            payload,
            ..
        } = update
        else {
            continue;
        };
        if cf == CF_MESH && is_lifecycle_projection_table(*table_id) {
            apply_lifecycle_projection_row(&mut state, *table_id, payload)?;
        }
    }
    Ok(state)
}

async fn lifecycle_transaction_timestamp(
    storage: &Storage,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<String> {
    let store = CoreStore::new(storage.clone()).await?;
    let transaction = store
        .read_explicit_transaction_for_principal(transaction_id, principal)
        .await?;
    let seconds =
        i64::try_from(transaction.created_at_unix_nanos / 1_000_000_000).map_err(|_| {
            LifecycleError::InvalidArgument(
                "lifecycle transaction timestamp exceeds the supported range".to_string(),
            )
        })?;
    let nanos = (transaction.created_at_unix_nanos % 1_000_000_000) as u32;
    let timestamp = DateTime::<Utc>::from_timestamp(seconds, nanos).ok_or_else(|| {
        LifecycleError::InvalidArgument(
            "lifecycle transaction timestamp exceeds the supported range".to_string(),
        )
    })?;
    Ok(timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
}

async fn write_state(storage: &Storage, state: &MeshLifecycleState) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let existing_state = read_lifecycle_state_projection_with_core_store(&store)?;
    let existing_activation = existing_state.canonical_topology_activation.as_ref();
    ensure_canonical_topology_activation_is_preserved(
        existing_activation,
        state.canonical_topology_activation.as_ref(),
    )?;
    ensure_topology_head_is_preserved(
        existing_state.topology_head.as_ref(),
        state.topology_head.as_ref(),
    )?;
    let rows = encode_lifecycle_projection_rows(state)?;
    let mut desired_keys = BTreeSet::new();
    let mut data_roots = BTreeSet::new();
    let mut preconditions = Vec::with_capacity(rows.len());
    let mut operations = Vec::with_capacity(rows.len());
    for row in rows {
        let table_id = lifecycle_projection_table_id(row.kind)?;
        let tuple_key = lifecycle_projection_row_key(row.kind, &row.record_key)?;
        desired_keys.insert((table_id, tuple_key.clone()));
        let current = store.read_coremeta_row(CF_MESH, table_id, &tuple_key)?;
        if current.as_deref() == Some(row.payload.as_slice()) {
            continue;
        }
        data_roots.insert(lifecycle_projection_root_anchor_key(
            row.kind,
            &row.record_key,
        ));
        preconditions.push(CoreMutationPrecondition::CoreMetaRow {
            cf: CF_MESH.to_string(),
            table_id,
            tuple_key: tuple_key.clone(),
            expected_payload_hash: current
                .as_ref()
                .map(|payload| core_meta_payload_digest(table_id, payload)),
            require_absent: current.is_none(),
            require_present: current.is_some(),
        });
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: MESH_LIFECYCLE_PROJECTION_PARTITION_ID.to_string(),
            cf: CF_MESH.to_string(),
            table_id,
            tuple_key,
            payload: row.payload,
        });
    }
    let prefix = lifecycle_projection_row_prefix()?;
    for table_id in [TABLE_MESH_PARTITION_ROW, TABLE_MESH_NODE_ROW] {
        for row in scan_lifecycle_projection_rows(&store, table_id, &prefix)? {
            let projection = record_proto::decode_lifecycle_projection_row(&row.payload)?;
            let (kind, record_key) = lifecycle_projection_descriptor_key(&projection)?;
            let tuple_key = lifecycle_projection_row_key(kind, &record_key)?;
            if desired_keys.contains(&(table_id, tuple_key.clone())) {
                continue;
            }
            data_roots.insert(lifecycle_projection_root_anchor_key(kind, &record_key));
            preconditions.push(CoreMutationPrecondition::CoreMetaRow {
                cf: CF_MESH.to_string(),
                table_id,
                tuple_key: tuple_key.clone(),
                expected_payload_hash: Some(core_meta_payload_digest(table_id, &row.payload)),
                require_absent: false,
                require_present: true,
            });
            operations.push(CoreMutationOperation::CoreMetaDelete {
                partition_id: MESH_LIFECYCLE_PROJECTION_PARTITION_ID.to_string(),
                cf: CF_MESH.to_string(),
                table_id,
                tuple_key,
            });
        }
    }
    if operations.is_empty() {
        return Ok(());
    }
    let root_publications =
        lifecycle_projection_root_publications(MESH_LIFECYCLE_PROJECTION_PARTITION_ID, data_roots);
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("mesh-lifecycle-projection:{}", uuid::Uuid::new_v4()),
            scope_partition: MESH_LIFECYCLE_PROJECTION_PARTITION_ID.to_string(),
            committed_by_principal: "mesh-lifecycle".to_string(),
            root_publications,
            preconditions,
            operations,
        })
        .await?;
    Ok(())
}

async fn read_lifecycle_state_projection(storage: &Storage) -> LifecycleResult<MeshLifecycleState> {
    let store = CoreStore::new(storage.clone()).await?;
    read_lifecycle_state_projection_with_core_store(&store)
}

fn read_lifecycle_state_projection_with_core_store(
    store: &CoreStore,
) -> LifecycleResult<MeshLifecycleState> {
    let mut state = MeshLifecycleState::default();
    for table_id in [TABLE_MESH_PARTITION_ROW, TABLE_MESH_NODE_ROW] {
        let prefix = lifecycle_projection_row_prefix()?;
        for row in scan_lifecycle_projection_rows(store, table_id, &prefix)? {
            apply_lifecycle_projection_row(&mut state, table_id, &row.payload)?;
        }
    }
    Ok(state)
}

pub(crate) fn canonical_topology_activation_with_core_store(
    store: &CoreStore,
) -> LifecycleResult<Option<CanonicalTopologyActivation>> {
    Ok(read_lifecycle_state_projection_with_core_store(store)?.canonical_topology_activation)
}

fn ensure_canonical_topology_activation_is_preserved(
    existing: Option<&CanonicalTopologyActivation>,
    candidate: Option<&CanonicalTopologyActivation>,
) -> LifecycleResult<()> {
    if let Some(existing) = existing
        && candidate != Some(existing)
    {
        return Err(LifecycleError::InvalidArgument(
            "canonical topology activation evidence cannot be replaced or removed".to_string(),
        ));
    }
    Ok(())
}

fn ensure_topology_head_is_preserved(
    existing: Option<&LifecycleTopologyHead>,
    candidate: Option<&LifecycleTopologyHead>,
) -> LifecycleResult<()> {
    if existing != candidate {
        return Err(LifecycleError::InvalidArgument(
            "generic lifecycle projection writes cannot replace, remove, or advance the topology head"
                .to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
async fn delete_lifecycle_state_projection(storage: &Storage) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    if canonical_topology_activation_with_core_store(&store)?.is_some() {
        return Err(LifecycleError::InvalidArgument(
            "lifecycle test cleanup cannot remove canonical topology activation evidence"
                .to_string(),
        ));
    }
    let prefix = lifecycle_projection_row_prefix()?;
    let mut data_roots = BTreeSet::new();
    let mut preconditions = Vec::new();
    let mut operations = Vec::new();
    for table_id in [TABLE_MESH_PARTITION_ROW, TABLE_MESH_NODE_ROW] {
        for row in scan_lifecycle_projection_rows(&store, table_id, &prefix)? {
            let projection = record_proto::decode_lifecycle_projection_row(&row.payload)?;
            let (kind, record_key) = lifecycle_projection_descriptor_key(&projection)?;
            let tuple_key = lifecycle_projection_row_key(kind, &record_key)?;
            data_roots.insert(lifecycle_projection_root_anchor_key(kind, &record_key));
            preconditions.push(CoreMutationPrecondition::CoreMetaRow {
                cf: CF_MESH.to_string(),
                table_id,
                tuple_key: tuple_key.clone(),
                expected_payload_hash: Some(core_meta_payload_digest(table_id, &row.payload)),
                require_absent: false,
                require_present: true,
            });
            operations.push(CoreMutationOperation::CoreMetaDelete {
                partition_id: MESH_LIFECYCLE_PROJECTION_PARTITION_ID.to_string(),
                cf: CF_MESH.to_string(),
                table_id,
                tuple_key,
            });
        }
    }
    if operations.is_empty() {
        return Ok(());
    }
    let root_publications =
        lifecycle_projection_root_publications(MESH_LIFECYCLE_PROJECTION_PARTITION_ID, data_roots);
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("mesh-lifecycle-projection-delete:{}", uuid::Uuid::new_v4()),
            scope_partition: MESH_LIFECYCLE_PROJECTION_PARTITION_ID.to_string(),
            committed_by_principal: "mesh-lifecycle-test".to_string(),
            root_publications,
            preconditions,
            operations,
        })
        .await?;
    Ok(())
}

fn scan_lifecycle_projection_rows(
    store: &CoreStore,
    table_id: u16,
    prefix: &[u8],
) -> LifecycleResult<Vec<crate::core_store::CoreMetaRecord>> {
    let mut rows = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let page = store.scan_coremeta_prefix_page(
            CF_MESH,
            table_id,
            prefix,
            cursor.as_deref(),
            LIFECYCLE_PROJECTION_PAGE_SIZE,
        )?;
        if rows.len().saturating_add(page.len()) > MAX_LIFECYCLE_PROJECTION_ROWS_PER_TABLE {
            return Err(LifecycleError::InvalidArgument(format!(
                "mesh lifecycle projection table {table_id:#06x} exceeds the {} row safety limit",
                MAX_LIFECYCLE_PROJECTION_ROWS_PER_TABLE
            )));
        }
        if page.is_empty() {
            break;
        }
        let next_cursor = core_meta_record_tuple_key(
            &page
                .last()
                .ok_or_else(|| {
                    LifecycleError::InvalidArgument(
                        "mesh lifecycle page lost its final row".to_string(),
                    )
                })?
                .key,
        )?
        .to_vec();
        if cursor
            .as_ref()
            .is_some_and(|current| current.as_slice() >= next_cursor.as_slice())
        {
            return Err(LifecycleError::InvalidArgument(
                "mesh lifecycle projection cursor did not advance".to_string(),
            ));
        }
        let page_is_full = page.len() == LIFECYCLE_PROJECTION_PAGE_SIZE;
        rows.extend(page);
        if !page_is_full {
            break;
        }
        cursor = Some(next_cursor);
    }
    Ok(rows)
}

fn encode_lifecycle_projection_rows(
    state: &MeshLifecycleState,
) -> LifecycleResult<Vec<record_proto::EncodedLifecycleProjectionRow>> {
    let mut rows = Vec::new();
    for descriptor in state.regions.values() {
        rows.push(record_proto::encode_region_projection_row(descriptor)?);
    }
    for descriptor in state.cells.values() {
        rows.push(record_proto::encode_cell_projection_row(descriptor)?);
    }
    for descriptor in state.nodes.values() {
        rows.push(record_proto::encode_node_projection_row(descriptor)?);
    }
    for descriptor in state.host_aliases.values() {
        rows.push(record_proto::encode_host_alias_projection_row(descriptor)?);
    }
    for descriptor in state.bucket_drain_exceptions.values() {
        rows.push(record_proto::encode_bucket_drain_exception_projection_row(
            descriptor,
        )?);
    }
    if let Some(activation) = state.canonical_topology_activation.as_ref() {
        rows.push(record_proto::encode_topology_activation_projection_row(
            activation,
        )?);
    }
    if let Some(head) = state.topology_head.as_ref() {
        rows.push(record_proto::encode_topology_head_projection_row(head)?);
    }
    Ok(rows)
}

fn lifecycle_projection_table_id(kind: &str) -> LifecycleResult<u16> {
    match kind {
        record_proto::LIFECYCLE_PROJECTION_NODE_KIND => Ok(TABLE_MESH_NODE_ROW),
        record_proto::LIFECYCLE_PROJECTION_REGION_KIND
        | record_proto::LIFECYCLE_PROJECTION_CELL_KIND
        | record_proto::LIFECYCLE_PROJECTION_HOST_ALIAS_KIND
        | record_proto::LIFECYCLE_PROJECTION_BUCKET_DRAIN_EXCEPTION_KIND
        | record_proto::LIFECYCLE_PROJECTION_TOPOLOGY_ACTIVATION_KIND
        | record_proto::LIFECYCLE_PROJECTION_TOPOLOGY_HEAD_KIND => Ok(TABLE_MESH_PARTITION_ROW),
        _ => Err(LifecycleError::InvalidArgument(format!(
            "unknown mesh lifecycle projection kind {kind}"
        ))),
    }
}

fn is_lifecycle_projection_table(table_id: u16) -> bool {
    matches!(table_id, TABLE_MESH_PARTITION_ROW | TABLE_MESH_NODE_ROW)
}

fn lifecycle_projection_row_prefix() -> LifecycleResult<Vec<u8>> {
    Ok(core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(
        record_proto::LIFECYCLE_PROJECTION_ROW_PREFIX,
    )])?)
}

fn lifecycle_projection_row_key(kind: &str, record_key: &str) -> LifecycleResult<Vec<u8>> {
    Ok(core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(record_proto::LIFECYCLE_PROJECTION_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(kind),
        CoreMetaTuplePart::Utf8(record_key),
    ])?)
}

fn lifecycle_projection_root_anchor_key(kind: &str, record_key: &str) -> String {
    record_proto::projection_root_anchor_key(kind, record_key)
}

fn lifecycle_projection_root_publications(
    coordinator_root: &str,
    mut data_roots: BTreeSet<String>,
) -> Vec<CoreMutationRootPublication> {
    let coordinator_is_data_root = data_roots.remove(coordinator_root);
    let coordinator = if coordinator_is_data_root {
        CoreMutationRootPublication {
            root_anchor_key: coordinator_root.to_string(),
            writer_families: vec![
                WriterFamily::CoreControl.as_str().to_string(),
                WriterFamily::MeshControl.as_str().to_string(),
            ],
            transaction_coordinator: true,
        }
    } else {
        CoreMutationRootPublication::new(coordinator_root, WriterFamily::CoreControl.as_str())
            .coordinator()
    };
    std::iter::once(coordinator)
        .chain(data_roots.into_iter().map(|root_anchor_key| {
            CoreMutationRootPublication::new(root_anchor_key, WriterFamily::MeshControl.as_str())
        }))
        .collect()
}

async fn stage_lifecycle_projection_row_in_transaction(
    storage: &Storage,
    row: record_proto::EncodedLifecycleProjectionRow,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<()> {
    let table_id = lifecycle_projection_table_id(row.kind)?;
    let tuple_key = lifecycle_projection_row_key(row.kind, &row.record_key)?;
    let store = CoreStore::new(storage.clone()).await?;
    let current = transaction_visible_lifecycle_payload(
        &store,
        transaction_id,
        principal,
        table_id,
        &tuple_key,
    )
    .await?;
    store
        .stage_coremeta_put_in_transaction(
            transaction_id,
            principal,
            CF_MESH,
            table_id,
            tuple_key,
            row.payload,
            current
                .as_ref()
                .map(|payload| core_meta_payload_digest(table_id, payload)),
            current.is_none(),
            current.is_some(),
        )
        .await?;
    Ok(())
}

async fn transaction_visible_lifecycle_payload(
    store: &CoreStore,
    transaction_id: &str,
    principal: &str,
    table_id: u16,
    tuple_key: &[u8],
) -> LifecycleResult<Option<Vec<u8>>> {
    let transaction = store
        .read_explicit_transaction_for_principal(transaction_id, principal)
        .await?;
    for update in transaction.visible_updates.iter().rev() {
        match update {
            CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id: update_table_id,
                tuple_key: update_tuple_key,
                payload,
                ..
            } if cf == CF_MESH && *update_table_id == table_id && update_tuple_key == tuple_key => {
                return Ok(Some(payload.clone()));
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id: update_table_id,
                tuple_key: update_tuple_key,
                ..
            } if cf == CF_MESH && *update_table_id == table_id && update_tuple_key == tuple_key => {
                return Ok(None);
            }
            _ => {}
        }
    }
    Ok(store.read_coremeta_row(CF_MESH, table_id, tuple_key)?)
}

fn apply_lifecycle_projection_row(
    state: &mut MeshLifecycleState,
    table_id: u16,
    payload: &[u8],
) -> LifecycleResult<()> {
    let projection = record_proto::decode_lifecycle_projection_row(payload)?;
    match projection {
        record_proto::LifecycleProjectionDescriptor::Region(descriptor) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_REGION_KIND,
            )?;
            state.regions.insert(descriptor.region.clone(), descriptor);
        }
        record_proto::LifecycleProjectionDescriptor::Cell(descriptor) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_CELL_KIND,
            )?;
            let key = cell_key(&descriptor.region, &descriptor.cell_id)?;
            state.cells.insert(key, descriptor);
        }
        record_proto::LifecycleProjectionDescriptor::Node(descriptor) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_NODE_KIND,
            )?;
            state.nodes.insert(descriptor.node_id.clone(), descriptor);
        }
        record_proto::LifecycleProjectionDescriptor::HostAlias(descriptor) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_HOST_ALIAS_KIND,
            )?;
            state
                .host_aliases
                .insert(descriptor.hostname.clone(), descriptor);
        }
        record_proto::LifecycleProjectionDescriptor::BucketDrainException(descriptor) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_BUCKET_DRAIN_EXCEPTION_KIND,
            )?;
            let key = bucket_drain_exception_key(
                &descriptor.region,
                &descriptor.tenant_id,
                &descriptor.bucket_name,
            );
            state.bucket_drain_exceptions.insert(key, descriptor);
        }
        record_proto::LifecycleProjectionDescriptor::TopologyActivation(activation) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_TOPOLOGY_ACTIVATION_KIND,
            )?;
            if let Some(existing) = state.canonical_topology_activation.as_ref()
                && existing != &activation
            {
                return Err(LifecycleError::InvalidArgument(
                    "canonical topology activation evidence is immutable".to_string(),
                ));
            }
            state.canonical_topology_activation = Some(activation);
        }
        record_proto::LifecycleProjectionDescriptor::TopologyHead(head) => {
            if let Some(existing) = state.topology_head.as_ref()
                && existing.generation >= head.generation
                && existing != &head
            {
                return Err(LifecycleError::InvalidArgument(
                    "lifecycle topology head generation did not advance monotonically".to_string(),
                ));
            }
            state.topology_head = Some(head);
        }
    }
    Ok(())
}

pub fn committed_topology_resources_from_transaction(
    transaction: &CoreTransaction,
) -> LifecycleResult<Vec<MeshLifecycleCommittedResource>> {
    let mut regions = BTreeSet::new();
    let mut cells = BTreeSet::new();
    let mut nodes = BTreeSet::new();

    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::CoreMetaPut {
            cf,
            table_id,
            payload,
            ..
        } = update
        else {
            continue;
        };
        if cf != CF_MESH || !is_lifecycle_projection_table(*table_id) {
            continue;
        }

        let projection = record_proto::decode_lifecycle_projection_row(payload)?;
        match projection {
            record_proto::LifecycleProjectionDescriptor::Region(descriptor) => {
                ensure_lifecycle_projection_table(
                    *table_id,
                    record_proto::LIFECYCLE_PROJECTION_REGION_KIND,
                )?;
                regions.insert(descriptor.region);
            }
            record_proto::LifecycleProjectionDescriptor::Cell(descriptor) => {
                ensure_lifecycle_projection_table(
                    *table_id,
                    record_proto::LIFECYCLE_PROJECTION_CELL_KIND,
                )?;
                cells.insert((descriptor.region, descriptor.cell_id));
            }
            record_proto::LifecycleProjectionDescriptor::Node(descriptor) => {
                ensure_lifecycle_projection_table(
                    *table_id,
                    record_proto::LIFECYCLE_PROJECTION_NODE_KIND,
                )?;
                nodes.insert((descriptor.region, descriptor.cell_id, descriptor.node_id));
            }
            record_proto::LifecycleProjectionDescriptor::HostAlias(_)
            | record_proto::LifecycleProjectionDescriptor::BucketDrainException(_)
            | record_proto::LifecycleProjectionDescriptor::TopologyActivation(_)
            | record_proto::LifecycleProjectionDescriptor::TopologyHead(_) => {}
        }
    }

    let mut out = Vec::with_capacity(regions.len() + cells.len() + nodes.len());
    out.extend(
        regions
            .into_iter()
            .map(|region| MeshLifecycleCommittedResource::Region { region }),
    );
    out.extend(
        cells
            .into_iter()
            .map(|(region, cell_id)| MeshLifecycleCommittedResource::Cell { region, cell_id }),
    );
    out.extend(nodes.into_iter().map(|(region, cell_id, node_id)| {
        MeshLifecycleCommittedResource::Node {
            region,
            cell_id,
            node_id,
        }
    }));
    Ok(out)
}

fn ensure_lifecycle_projection_table(table_id: u16, kind: &str) -> LifecycleResult<()> {
    if lifecycle_projection_table_id(kind)? != table_id {
        return Err(LifecycleError::InvalidArgument(format!(
            "mesh lifecycle projection kind {kind} is stored in the wrong CoreMeta table"
        )));
    }
    Ok(())
}

fn lifecycle_projection_descriptor_key(
    projection: &record_proto::LifecycleProjectionDescriptor,
) -> LifecycleResult<(&'static str, String)> {
    match projection {
        record_proto::LifecycleProjectionDescriptor::Region(descriptor) => Ok((
            record_proto::LIFECYCLE_PROJECTION_REGION_KIND,
            descriptor.region.clone(),
        )),
        record_proto::LifecycleProjectionDescriptor::Cell(descriptor) => Ok((
            record_proto::LIFECYCLE_PROJECTION_CELL_KIND,
            cell_key(&descriptor.region, &descriptor.cell_id)?,
        )),
        record_proto::LifecycleProjectionDescriptor::Node(descriptor) => Ok((
            record_proto::LIFECYCLE_PROJECTION_NODE_KIND,
            node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?,
        )),
        record_proto::LifecycleProjectionDescriptor::HostAlias(descriptor) => Ok((
            record_proto::LIFECYCLE_PROJECTION_HOST_ALIAS_KIND,
            descriptor.hostname.clone(),
        )),
        record_proto::LifecycleProjectionDescriptor::BucketDrainException(descriptor) => Ok((
            record_proto::LIFECYCLE_PROJECTION_BUCKET_DRAIN_EXCEPTION_KIND,
            bucket_drain_exception_key(
                &descriptor.region,
                &descriptor.tenant_id,
                &descriptor.bucket_name,
            ),
        )),
        record_proto::LifecycleProjectionDescriptor::TopologyActivation(activation) => Ok((
            record_proto::LIFECYCLE_PROJECTION_TOPOLOGY_ACTIVATION_KIND,
            activation.mesh_id.clone(),
        )),
        record_proto::LifecycleProjectionDescriptor::TopologyHead(head) => Ok((
            record_proto::LIFECYCLE_PROJECTION_TOPOLOGY_HEAD_KIND,
            head.mesh_id.clone(),
        )),
    }
}

async fn overlay_lifecycle_control_streams(
    storage: &Storage,
    state: &mut MeshLifecycleState,
) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    overlay_lifecycle_control_streams_with_store(storage, &store, state).await
}

async fn overlay_lifecycle_control_streams_with_store(
    storage: &Storage,
    _store: &CoreStore,
    state: &mut MeshLifecycleState,
) -> LifecycleResult<()> {
    for stream_family in lifecycle_control_stream_families() {
        let mut partition_cursor = None;
        loop {
            let partition_page = crate::mesh_control_stream::list_control_stream_partitions_page(
                storage,
                stream_family,
                partition_cursor.as_deref(),
                256,
            )
            .await
            .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;
            for partition in partition_page.partitions {
                let mut record_cursor = None;
                loop {
                    let page = crate::mesh_control_stream::list_current_control_stream_records_page(
                        storage,
                        stream_family,
                        &partition,
                        record_cursor.as_deref(),
                        512,
                    )
                    .await
                    .map_err(|err| {
                        LifecycleError::InvalidArgument(format!(
                            "could not read current lifecycle control records {stream_family}/{partition}: {err}"
                        ))
                    })?;
                    for current in page.records {
                        apply_lifecycle_control_frame(
                            state,
                            stream_family,
                            &partition,
                            &current.frame.header_proto,
                            &current.frame.payload_proto,
                        )?;
                    }
                    let Some(next) = page.next_stream_id else {
                        break;
                    };
                    record_cursor = Some(next);
                }
            }
            let Some(next) = partition_page.next_stream_id else {
                break;
            };
            partition_cursor = Some(next);
        }
    }
    Ok(())
}

fn apply_lifecycle_control_frame(
    state: &mut MeshLifecycleState,
    expected_stream_family: &str,
    expected_partition: &str,
    header_proto: &[u8],
    payload_proto: &[u8],
) -> LifecycleResult<()> {
    let header = crate::mesh_control_stream::decode_control_mutation_header(header_proto)
        .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;
    let stream_family = header.stream_family.as_str();
    let partition = header.partition.as_str();
    if stream_family != expected_stream_family || partition != expected_partition {
        return Err(LifecycleError::InvalidArgument(format!(
            "lifecycle control frame scope {stream_family}/{partition} does not match path {expected_stream_family}/{expected_partition}"
        )));
    }
    let operation = header.operation.as_str();
    let record_key = if header.record_key.trim().is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "lifecycle control frame missing record_key".to_string(),
        ));
    } else {
        header.record_key.as_str()
    };
    if matches!(operation, "delete" | "tombstone") {
        remove_lifecycle_projection(state, stream_family, record_key)?;
        return Ok(());
    }
    match record_proto::decode_lifecycle_control_payload(stream_family, payload_proto)? {
        record_proto::LifecycleControlDescriptor::Region(descriptor) => {
            if descriptor.region != record_key {
                return Err(LifecycleError::InvalidArgument(format!(
                    "region descriptor key mismatch: expected {record_key}, got {}",
                    descriptor.region
                )));
            }
            state.regions.insert(descriptor.region.clone(), descriptor);
        }
        record_proto::LifecycleControlDescriptor::Cell(descriptor) => {
            let key = cell_record_key(&descriptor.region, &descriptor.cell_id)?;
            if key != record_key {
                return Err(LifecycleError::InvalidArgument(format!(
                    "cell descriptor key mismatch: expected {record_key}, got {key}"
                )));
            }
            state.cells.insert(key, descriptor);
        }
        record_proto::LifecycleControlDescriptor::Node(descriptor) => {
            let key =
                node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?;
            if key != record_key {
                return Err(LifecycleError::InvalidArgument(format!(
                    "node descriptor key mismatch: expected {record_key}, got {key}"
                )));
            }
            state.nodes.insert(descriptor.node_id.clone(), descriptor);
        }
    }
    Ok(())
}

fn remove_lifecycle_projection(
    state: &mut MeshLifecycleState,
    stream_family: &str,
    record_key: &str,
) -> LifecycleResult<()> {
    match stream_family {
        REGION_DESCRIPTOR_STREAM_FAMILY => {
            state.regions.remove(record_key);
        }
        CELL_DESCRIPTOR_STREAM_FAMILY => {
            state.cells.remove(record_key);
        }
        NODE_DESCRIPTOR_STREAM_FAMILY => {
            let (_, _, node_id) = parse_node_record_key(record_key)?;
            state.nodes.remove(node_id);
        }
        _ => {
            return Err(LifecycleError::InvalidArgument(format!(
                "unknown lifecycle control stream family {stream_family}"
            )));
        }
    }
    Ok(())
}

pub async fn upsert_bucket_drain_exception(
    storage: &Storage,
    input: BucketDrainExceptionInput,
) -> LifecycleResult<BucketDrainExceptionDescriptor> {
    require_identifier(&input.tenant_id, "bucket drain exception tenant id")?;
    require_identifier(&input.bucket_name, "bucket drain exception bucket name")?;
    require_identifier(&input.region, "bucket drain exception region")?;
    require_nonempty(&input.reason, "bucket drain exception reason")?;
    if !input.disposition.allows_drained_exception() {
        return Err(LifecycleError::InvalidArgument(format!(
            "bucket drain exception disposition must be remain_proxy_only or read_only_until_removed, got {}",
            input.disposition.as_str()
        )));
    }
    if let Some(expires_at) = &input.expires_at {
        require_nonempty(expires_at, "bucket drain exception expires_at")?;
    }

    let mut state = read_state(storage).await?;
    let key = bucket_drain_exception_key(&input.region, &input.tenant_id, &input.bucket_name);
    let generation = state
        .bucket_drain_exceptions
        .get(&key)
        .map_or(1, |existing| existing.generation.saturating_add(1));
    let descriptor = BucketDrainExceptionDescriptor {
        schema: BUCKET_DRAIN_EXCEPTION_SCHEMA.to_string(),
        tenant_id: input.tenant_id,
        bucket_name: input.bucket_name,
        region: input.region,
        disposition: input.disposition,
        reason: input.reason,
        expires_at: input.expires_at,
        generation,
    };
    state
        .bucket_drain_exceptions
        .insert(key, descriptor.clone());
    write_state(storage, &state).await?;
    Ok(descriptor)
}

pub async fn list_bucket_drain_exceptions(
    storage: &Storage,
    region_filter: Option<&str>,
) -> LifecycleResult<Vec<BucketDrainExceptionDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "bucket drain exception region")?;
    }
    Ok(read_state(storage)
        .await?
        .bucket_drain_exceptions
        .into_values()
        .filter(|exception| {
            region_filter.is_none_or(|region| region.is_empty() || exception.region == region)
        })
        .collect())
}

mod helpers;
pub use helpers::*;

pub(crate) fn control_payload_operator_json(
    stream_family: &str,
    record_key: &str,
    payload_proto: &[u8],
) -> LifecycleResult<Vec<u8>> {
    record_proto::control_payload_operator_json(stream_family, record_key, payload_proto)
}

#[cfg(test)]
mod tests;
