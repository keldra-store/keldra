use super::{
    OwnershipFenceRecord, OwnershipResource, PartitionOwnerState, decode_ownership_fence_record,
    decode_partition_owner_record, encode_ownership_fence_record, encode_partition_owner_record,
    ownership_resource_hash, require_nonempty, validate_hex32, validate_ownership_resource,
};
use crate::{
    core_store::{
        CF_LEASES_FENCES, CoreMetaRecord, CoreMetaStore, CoreMetaTuplePart, CoreMutationBatch,
        CoreMutationOperation, CoreMutationPrecondition, CoreMutationRootPublication, CoreStore,
        TABLE_OWNERSHIP_FENCE_ROW, TABLE_PARTITION_OWNER_ROW, core_meta_payload_digest,
        core_meta_record_tuple_key, core_meta_tuple_key, is_retryable_mutation_conflict,
    },
    formats::writer::WriterFamily,
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use serde::{Deserialize, Serialize};

const PARTITION_OWNER_ROW_PREFIX: &str = "partition_owner";
const PARTITION_OWNER_BY_NODE_PREFIX: &str = "partition_owner_by_node";
const OWNERSHIP_FENCE_ROW_PREFIX: &str = "ownership_fence";
const OWNERSHIP_FENCE_BY_NODE_PREFIX: &str = "ownership_fence_by_node";

/// Maximum number of ordered CoreMeta rows inspected by one fence page request.
pub const MAX_PARTITION_FENCE_PAGE_SIZE: usize = 256;

/// Exclusive continuation position for an ordered partition-owner page.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct PartitionOwnerPageCursor(String);

impl PartitionOwnerPageCursor {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A bounded page of partition owners in CoreMeta tuple-key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionOwnerPage {
    pub owners: Vec<PartitionOwnerState>,
    pub next_cursor: Option<PartitionOwnerPageCursor>,
}

/// Exclusive continuation position for an ordered ownership-fence page.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct OwnershipFencePageCursor(String);

impl OwnershipFencePageCursor {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A bounded page of ownership fences in CoreMeta tuple-key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipFencePage {
    pub fences: Vec<OwnershipFenceRecord>,
    pub next_cursor: Option<OwnershipFencePageCursor>,
}

/// Lists at most `limit` partition-owner rows after `cursor`.
pub async fn list_partition_owners_page(
    storage: &Storage,
    cursor: Option<&PartitionOwnerPageCursor>,
    limit: usize,
    signing_key: &[u8],
) -> Result<PartitionOwnerPage> {
    let store = CoreStore::new(storage.clone()).await?;
    partition_owner_page(
        &store,
        partition_owner_row_prefix()?,
        cursor,
        limit,
        signing_key,
        |owner| partition_owner_row_key(&owner.partition_family, &owner.partition_id),
    )
}

/// Lists one bounded page from the node-owned partition projection.
pub async fn list_partition_owners_for_node_page(
    storage: &Storage,
    owner_node_id: &str,
    cursor: Option<&PartitionOwnerPageCursor>,
    limit: usize,
    signing_key: &[u8],
) -> Result<PartitionOwnerPage> {
    require_nonempty(owner_node_id, "owner node id")?;
    let store = CoreStore::new(storage.clone()).await?;
    partition_owner_page(
        &store,
        partition_owner_by_node_prefix(owner_node_id)?,
        cursor,
        limit,
        signing_key,
        partition_owner_by_node_key,
    )
}

/// Lists at most `limit` ownership-fence rows after `cursor`.
pub async fn list_ownership_fences_page(
    storage: &Storage,
    cursor: Option<&OwnershipFencePageCursor>,
    limit: usize,
    signing_key: &[u8],
) -> Result<OwnershipFencePage> {
    let store = CoreStore::new(storage.clone()).await?;
    ownership_fence_page(
        &store,
        ownership_fence_row_prefix()?,
        cursor,
        limit,
        signing_key,
        |record| ownership_fence_row_key(record.owner.tenant_id, &record.resource),
        |_| true,
    )
}

/// Lists one bounded source page from the node-owned fence projection.
pub async fn list_active_ownership_fences_for_node_page(
    storage: &Storage,
    owner_node_id: &str,
    now_nanos: i64,
    cursor: Option<&OwnershipFencePageCursor>,
    limit: usize,
    signing_key: &[u8],
) -> Result<OwnershipFencePage> {
    require_nonempty(owner_node_id, "owner node id")?;
    if now_nanos < 0 {
        return Err(anyhow!("ownership fence timestamp must be nonnegative"));
    }
    let store = CoreStore::new(storage.clone()).await?;
    ownership_fence_page(
        &store,
        ownership_fence_by_node_prefix(owner_node_id)?,
        cursor,
        limit,
        signing_key,
        ownership_fence_by_node_key,
        |record| record.is_active_unexpired(now_nanos),
    )
}

pub(super) async fn read_ownership_fence_state(
    storage: &Storage,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
) -> Result<Option<(Vec<u8>, OwnershipFenceRecord)>> {
    let row_key = ownership_fence_row_key(tenant_id, resource)?;
    let store = CoreStore::new(storage.clone()).await?;
    record_point_read();
    let Some(bytes) = read_committed_fence_row(
        storage,
        &store,
        TABLE_OWNERSHIP_FENCE_ROW,
        &row_key,
        "ownership fence",
    )?
    else {
        return Ok(None);
    };
    let record = decode_ownership_fence_record(&bytes)?;
    record.verify(signing_key)?;
    if record.owner.tenant_id != tenant_id || record.resource != *resource {
        return Err(anyhow!("ownership fence row scope mismatch"));
    }
    Ok(Some((bytes, record)))
}

pub(super) async fn write_ownership_fence_state(
    storage: &Storage,
    record: &OwnershipFenceRecord,
    expected_ref: Option<&Vec<u8>>,
) -> Result<()> {
    let row_key = ownership_fence_row_key(record.owner.tenant_id, &record.resource)?;
    let transaction_id = ownership_fence_transaction_id(record)?;
    let payload = encode_ownership_fence_record(record)?;
    let previous = expected_ref
        .map(|payload| decode_ownership_fence_record(payload))
        .transpose()?;
    let old_projection_key = previous
        .as_ref()
        .filter(|record| record.owner.principal_kind == "node")
        .map(ownership_fence_by_node_key)
        .transpose()?;
    let new_projection_key = (record.owner.principal_kind == "node")
        .then(|| ownership_fence_by_node_key(record))
        .transpose()?;
    let scope_partition = ownership_resource_hash(record.owner.tenant_id, &record.resource)?;
    let new_root_anchor_key = ownership_fence_root_anchor_key(record)?;
    let old_root_anchor_key = previous
        .as_ref()
        .map(ownership_fence_root_anchor_key)
        .transpose()?;
    commit_projected_point_put(
        storage,
        TABLE_OWNERSHIP_FENCE_ROW,
        row_key,
        expected_ref.map(Vec::as_slice),
        payload,
        old_projection_key,
        new_projection_key,
        scope_partition,
        new_root_anchor_key,
        old_root_anchor_key,
        transaction_id,
    )
    .await
}

pub(super) async fn read_partition_owner_state(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    signing_key: &[u8],
) -> Result<Option<(Vec<u8>, PartitionOwnerState)>> {
    let row_key = partition_owner_row_key(partition_family, partition_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    record_point_read();
    let Some(bytes) = read_committed_fence_row(
        storage,
        &store,
        TABLE_PARTITION_OWNER_ROW,
        &row_key,
        "partition owner",
    )?
    else {
        return Ok(None);
    };
    let owner = decode_partition_owner_record(&bytes)?;
    owner.verify(signing_key)?;
    if owner.partition_family != partition_family || owner.partition_id != partition_id {
        return Err(anyhow!("partition owner row scope mismatch"));
    }
    Ok(Some((bytes, owner)))
}

pub(super) async fn write_partition_owner_state(
    storage: &Storage,
    owner: &PartitionOwnerState,
    expected_ref: Option<&Vec<u8>>,
) -> Result<()> {
    let row_key = partition_owner_row_key(&owner.partition_family, &owner.partition_id)?;
    let transaction_id = partition_owner_transaction_id(owner);
    let payload = encode_partition_owner_record(owner)?;
    let previous = expected_ref
        .map(|payload| decode_partition_owner_record(payload))
        .transpose()?;
    let old_projection_key = previous
        .as_ref()
        .map(partition_owner_by_node_key)
        .transpose()?;
    let new_root_anchor_key = partition_owner_root_anchor_key(owner);
    let old_root_anchor_key = previous.as_ref().map(partition_owner_root_anchor_key);
    commit_projected_point_put(
        storage,
        TABLE_PARTITION_OWNER_ROW,
        row_key,
        expected_ref.map(Vec::as_slice),
        payload,
        old_projection_key,
        Some(partition_owner_by_node_key(owner)?),
        owner.partition_id.clone(),
        new_root_anchor_key,
        old_root_anchor_key,
        transaction_id,
    )
    .await
}

pub(super) fn is_partition_fence_cas_conflict(error: &anyhow::Error) -> bool {
    is_retryable_mutation_conflict(error)
}

fn read_committed_fence_row(
    storage: &Storage,
    store: &CoreStore,
    table_id: u16,
    row_key: &[u8],
    label: &str,
) -> Result<Option<Vec<u8>>> {
    if let Some(payload) = store.read_coremeta_row(CF_LEASES_FENCES, table_id, row_key)? {
        return Ok(Some(payload));
    }
    if CoreMetaStore::open(storage.core_store_meta_path())?
        .get(CF_LEASES_FENCES, table_id, row_key)?
        .is_some()
    {
        bail!("{label} row exists but is not committed-visible");
    }
    Ok(None)
}

pub(super) fn ownership_fence_row_key(
    tenant_id: i64,
    resource: &OwnershipResource,
) -> Result<Vec<u8>> {
    validate_ownership_resource(resource)?;
    if tenant_id < 0 {
        return Err(anyhow!("ownership fence tenant id must be nonnegative"));
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(OWNERSHIP_FENCE_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(&format!("tenant:{tenant_id}")),
        CoreMetaTuplePart::Utf8(resource.resource_kind.as_str()),
        CoreMetaTuplePart::Hash(&format!(
            "blake3:{}",
            ownership_resource_hash(tenant_id, resource)?
        )),
    ])
}

pub(super) fn partition_owner_row_key(
    partition_family: &str,
    partition_id: &str,
) -> Result<Vec<u8>> {
    require_nonempty(partition_family, "partition family")?;
    if partition_family.contains('\0')
        || partition_family.contains("..")
        || partition_family.contains(':')
        || partition_family.chars().any(char::is_control)
    {
        return Err(anyhow!("partition family contains an invalid component"));
    }
    validate_hex32(partition_id, "partition id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(PARTITION_OWNER_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(partition_family),
        CoreMetaTuplePart::Hash(&format!("blake3:{partition_id}")),
    ])
}

async fn commit_projected_point_put(
    storage: &Storage,
    table_id: u16,
    tuple_key: Vec<u8>,
    expected_payload: Option<&[u8]>,
    payload: Vec<u8>,
    old_projection_key: Option<Vec<u8>>,
    new_projection_key: Option<Vec<u8>>,
    scope_partition: String,
    new_root_anchor_key: String,
    old_root_anchor_key: Option<String>,
    transaction_id: String,
) -> Result<()> {
    let store = CoreStore::new(storage.clone()).await?;
    wait_at_point_cas_barrier().await;
    let projection_changed = old_projection_key != new_projection_key;
    let mut preconditions = vec![CoreMutationPrecondition::CoreMetaRow {
        cf: CF_LEASES_FENCES.to_string(),
        table_id,
        tuple_key: tuple_key.clone(),
        expected_payload_hash: expected_payload
            .map(|payload| core_meta_payload_digest(table_id, payload)),
        require_absent: expected_payload.is_none(),
        require_present: expected_payload.is_some(),
    }];
    let include_old_root = projection_changed && old_projection_key.is_some();
    if projection_changed {
        if let Some(old_key) = old_projection_key.as_ref() {
            preconditions.push(CoreMutationPrecondition::CoreMetaRow {
                cf: CF_LEASES_FENCES.to_string(),
                table_id,
                tuple_key: old_key.clone(),
                expected_payload_hash: expected_payload
                    .map(|payload| core_meta_payload_digest(table_id, payload)),
                require_absent: false,
                require_present: true,
            });
        }
        if let Some(new_key) = new_projection_key.as_ref() {
            preconditions.push(CoreMutationPrecondition::CoreMetaRow {
                cf: CF_LEASES_FENCES.to_string(),
                table_id,
                tuple_key: new_key.clone(),
                expected_payload_hash: None,
                require_absent: true,
                require_present: false,
            });
        }
    }
    let mut operations = Vec::with_capacity(3);
    if projection_changed && let Some(old_key) = old_projection_key {
        operations.push(CoreMutationOperation::CoreMetaDelete {
            partition_id: scope_partition.clone(),
            cf: CF_LEASES_FENCES.to_string(),
            table_id,
            tuple_key: old_key,
        });
    }
    operations.push(CoreMutationOperation::CoreMetaPut {
        partition_id: scope_partition.clone(),
        cf: CF_LEASES_FENCES.to_string(),
        table_id,
        tuple_key,
        payload: payload.clone(),
    });
    if let Some(new_key) = new_projection_key {
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: scope_partition.clone(),
            cf: CF_LEASES_FENCES.to_string(),
            table_id,
            tuple_key: new_key,
            payload,
        });
    }
    let mut root_anchor_keys = vec![new_root_anchor_key];
    if include_old_root && let Some(old_root_anchor_key) = old_root_anchor_key {
        root_anchor_keys.push(old_root_anchor_key);
    }
    let receipt = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: scope_partition.clone(),
            committed_by_principal: "partition-fence".to_string(),
            root_publications: control_root_publications(&scope_partition, root_anchor_keys),
            preconditions,
            operations,
        })
        .await?;
    if receipt.state != crate::core_store::CoreTransactionState::Committed {
        bail!(
            "partition fence CoreStore mutation failed: {}",
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation failure")
        );
    }
    Ok(())
}

pub(super) fn partition_owner_transaction_id(owner: &PartitionOwnerState) -> String {
    let identity = format!(
        "{}\0{}\0{}\0{}\0{}",
        owner.partition_family,
        owner.partition_id,
        owner.owner_node_id,
        owner.fence_token,
        owner.generation
    );
    format!(
        "partition-owner-cas:{}",
        hex::encode(crate::formats::hash32(identity.as_bytes()))
    )
}

pub(super) fn ownership_fence_transaction_id(record: &OwnershipFenceRecord) -> Result<String> {
    let mut identity = vec![
        record.format_version.to_string(),
        record.owner.tenant_id.to_string(),
        record.resource.resource_kind.as_str().to_string(),
        ownership_resource_hash(record.owner.tenant_id, &record.resource)?,
        record.owner.principal_kind.clone(),
        record.owner.principal_id.clone(),
        record.owner.actor_instance_id.clone(),
        record.owner.display_name.clone(),
        record.owner.region.clone(),
        record.owner.cell.clone(),
        record.fence.to_string(),
        record.state.as_str().to_string(),
        record.lease_expires_at_nanos.to_string(),
        record.last_heartbeat_at_nanos.to_string(),
        record.generation.to_string(),
        record.last_operation.clone().unwrap_or_default(),
        record.last_idempotency_key.clone().unwrap_or_default(),
    ];
    if let Some(actor) = &record.last_actor {
        identity.extend([
            actor.tenant_id.to_string(),
            actor.principal_kind.clone(),
            actor.principal_id.clone(),
            actor.actor_instance_id.clone(),
            actor.display_name.clone(),
            actor.region.clone(),
            actor.cell.clone(),
        ]);
    }
    Ok(format!(
        "ownership-fence-cas:{}",
        hex::encode(crate::formats::hash32(identity.join("\0").as_bytes()))
    ))
}

fn control_root_publications(
    coordinator_root: &str,
    mut data_roots: Vec<String>,
) -> Vec<CoreMutationRootPublication> {
    let coordinator_is_data_root = data_roots
        .iter()
        .any(|root| root.as_str() == coordinator_root);
    data_roots.push(coordinator_root.to_string());
    data_roots.sort();
    data_roots.dedup();
    data_roots
        .into_iter()
        .map(|root| {
            let is_coordinator = root.as_str() == coordinator_root;
            if is_coordinator {
                if coordinator_is_data_root {
                    CoreMutationRootPublication {
                        root_anchor_key: root,
                        writer_families: vec![
                            WriterFamily::CoreControl.as_str().to_string(),
                            WriterFamily::MeshControl.as_str().to_string(),
                        ],
                        transaction_coordinator: true,
                    }
                } else {
                    CoreMutationRootPublication::new(root, WriterFamily::CoreControl.as_str())
                        .coordinator()
                }
            } else {
                CoreMutationRootPublication::new(root, WriterFamily::MeshControl.as_str())
            }
        })
        .collect()
}

fn partition_owner_root_anchor_key(owner: &PartitionOwnerState) -> String {
    format!(
        "partition-owner/{}/{}",
        owner.partition_family, owner.partition_id
    )
}

fn ownership_fence_root_anchor_key(record: &OwnershipFenceRecord) -> Result<String> {
    match record.format_version {
        1 => Ok(format!(
            "ownership-fence/{}/{}",
            record.resource.resource_kind.as_str(),
            record.resource.resource_id
        )),
        2 => Ok(format!(
            "ownership-fence/v2/tenant:{}/{}/{}",
            record.owner.tenant_id,
            record.resource.resource_kind.as_str(),
            ownership_resource_hash(record.owner.tenant_id, &record.resource)?
        )),
        _ => Err(anyhow!("unsupported ownership fence version")),
    }
}

fn partition_owner_page(
    store: &CoreStore,
    prefix: Vec<u8>,
    cursor: Option<&PartitionOwnerPageCursor>,
    limit: usize,
    signing_key: &[u8],
    expected_key: impl Fn(&PartitionOwnerState) -> Result<Vec<u8>>,
) -> Result<PartitionOwnerPage> {
    let (rows, has_more) = scan_page(
        store,
        TABLE_PARTITION_OWNER_ROW,
        &prefix,
        cursor.map(PartitionOwnerPageCursor::as_str),
        limit,
    )?;
    let next_cursor = if has_more {
        Some(PartitionOwnerPageCursor(cursor_for_last_row(&rows)?))
    } else {
        None
    };
    let mut owners = Vec::new();
    for row in rows {
        let owner = decode_partition_owner_record(&row.payload)?;
        owner.verify(signing_key)?;
        let tuple_key = core_meta_record_tuple_key(&row.key)?;
        let expected_key = expected_key(&owner)?;
        if tuple_key != expected_key.as_slice() {
            bail!("partition owner page row key does not match its payload");
        }
        owners.push(owner);
    }
    Ok(PartitionOwnerPage {
        owners,
        next_cursor,
    })
}

fn ownership_fence_page(
    store: &CoreStore,
    prefix: Vec<u8>,
    cursor: Option<&OwnershipFencePageCursor>,
    limit: usize,
    signing_key: &[u8],
    expected_key: impl Fn(&OwnershipFenceRecord) -> Result<Vec<u8>>,
    include: impl Fn(&OwnershipFenceRecord) -> bool,
) -> Result<OwnershipFencePage> {
    let (rows, has_more) = scan_page(
        store,
        TABLE_OWNERSHIP_FENCE_ROW,
        &prefix,
        cursor.map(OwnershipFencePageCursor::as_str),
        limit,
    )?;
    let next_cursor = if has_more {
        Some(OwnershipFencePageCursor(cursor_for_last_row(&rows)?))
    } else {
        None
    };
    let mut fences = Vec::new();
    for row in rows {
        let record = decode_ownership_fence_record(&row.payload)?;
        record.verify(signing_key)?;
        let tuple_key = core_meta_record_tuple_key(&row.key)?;
        let expected_key = expected_key(&record)?;
        if tuple_key != expected_key.as_slice() {
            bail!("ownership fence page row key does not match its payload");
        }
        if include(&record) {
            fences.push(record);
        }
    }
    Ok(OwnershipFencePage {
        fences,
        next_cursor,
    })
}

fn scan_page(
    store: &CoreStore,
    table_id: u16,
    prefix: &[u8],
    cursor: Option<&str>,
    limit: usize,
) -> Result<(Vec<CoreMetaRecord>, bool)> {
    validate_page_limit(limit)?;
    let after = cursor
        .map(|cursor| decode_cursor(cursor, prefix))
        .transpose()?;
    let mut rows = store.scan_coremeta_prefix_page(
        CF_LEASES_FENCES,
        table_id,
        prefix,
        after.as_deref(),
        limit + 1,
    )?;
    let has_more = rows.len() > limit;
    if has_more {
        rows.pop();
    }
    Ok((rows, has_more))
}

fn validate_page_limit(limit: usize) -> Result<()> {
    if !(1..=MAX_PARTITION_FENCE_PAGE_SIZE).contains(&limit) {
        bail!("partition fence page limit must be between 1 and {MAX_PARTITION_FENCE_PAGE_SIZE}");
    }
    Ok(())
}

fn decode_cursor(cursor: &str, prefix: &[u8]) -> Result<Vec<u8>> {
    let tuple_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .context("partition fence page cursor is not valid base64url")?;
    if tuple_key.len() <= prefix.len() || !tuple_key.starts_with(prefix) {
        bail!("partition fence page cursor is outside the requested key prefix");
    }
    Ok(tuple_key)
}

fn cursor_for_last_row(rows: &[CoreMetaRecord]) -> Result<String> {
    let row = rows
        .last()
        .ok_or_else(|| anyhow!("partition fence continuation page has no final row"))?;
    let tuple_key = core_meta_record_tuple_key(&row.key)?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(tuple_key))
}

fn partition_owner_row_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(PARTITION_OWNER_ROW_PREFIX)])
}

fn partition_owner_by_node_prefix(owner_node_id: &str) -> Result<Vec<u8>> {
    require_nonempty(owner_node_id, "owner node id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(PARTITION_OWNER_BY_NODE_PREFIX),
        CoreMetaTuplePart::Utf8(owner_node_id),
    ])
}

fn partition_owner_by_node_key(owner: &PartitionOwnerState) -> Result<Vec<u8>> {
    validate_hex32(&owner.partition_id, "partition id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(PARTITION_OWNER_BY_NODE_PREFIX),
        CoreMetaTuplePart::Utf8(&owner.owner_node_id),
        CoreMetaTuplePart::Utf8(&owner.partition_family),
        CoreMetaTuplePart::Hash(&format!("blake3:{}", owner.partition_id)),
    ])
}

fn ownership_fence_row_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(OWNERSHIP_FENCE_ROW_PREFIX)])
}

fn ownership_fence_by_node_prefix(owner_node_id: &str) -> Result<Vec<u8>> {
    require_nonempty(owner_node_id, "owner node id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(OWNERSHIP_FENCE_BY_NODE_PREFIX),
        CoreMetaTuplePart::Utf8(owner_node_id),
    ])
}

fn ownership_fence_by_node_key(record: &OwnershipFenceRecord) -> Result<Vec<u8>> {
    if record.owner.principal_kind != "node" {
        bail!("ownership fence node projection requires a node principal");
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(OWNERSHIP_FENCE_BY_NODE_PREFIX),
        CoreMetaTuplePart::Utf8(&record.owner.actor_instance_id),
        CoreMetaTuplePart::I64(record.owner.tenant_id),
        CoreMetaTuplePart::Utf8(record.resource.resource_kind.as_str()),
        CoreMetaTuplePart::Hash(&format!(
            "blake3:{}",
            ownership_resource_hash(record.owner.tenant_id, &record.resource)?
        )),
    ])
}

#[cfg(test)]
struct PointCasTestGate {
    barrier: std::sync::Arc<tokio::sync::Barrier>,
    entered: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
tokio::task_local! {
    static POINT_CAS_TEST_GATE: std::sync::Arc<PointCasTestGate>;
}

#[cfg(test)]
tokio::task_local! {
    static POINT_READ_COUNT: std::cell::Cell<usize>;
}

#[cfg(test)]
pub(super) async fn with_point_cas_barrier<F>(
    barrier: std::sync::Arc<tokio::sync::Barrier>,
    future: F,
) -> F::Output
where
    F: std::future::Future,
{
    let gate = std::sync::Arc::new(PointCasTestGate {
        barrier,
        entered: std::sync::atomic::AtomicBool::new(false),
    });
    POINT_CAS_TEST_GATE.scope(gate, future).await
}

#[cfg(test)]
pub(super) async fn count_point_reads<F>(future: F) -> (F::Output, usize)
where
    F: std::future::Future,
{
    POINT_READ_COUNT
        .scope(std::cell::Cell::new(0), async move {
            let output = future.await;
            let count = POINT_READ_COUNT.with(|count| count.get());
            (output, count)
        })
        .await
}

#[cfg(test)]
async fn wait_at_point_cas_barrier() {
    use std::sync::atomic::Ordering;

    let gate = POINT_CAS_TEST_GATE.try_with(|gate| gate.clone()).ok();
    if let Some(gate) = gate
        && !gate.entered.swap(true, Ordering::SeqCst)
    {
        gate.barrier.wait().await;
    }
}

#[cfg(not(test))]
async fn wait_at_point_cas_barrier() {}

fn record_point_read() {
    #[cfg(test)]
    let _ = POINT_READ_COUNT.try_with(|count| count.set(count.get().saturating_add(1)));
}
