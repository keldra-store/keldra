use super::{
    HfBody, HfIngestionItemProto, HfIngestionProto, HfKeyProto, ensure_deterministic_proto,
    hf_ingestion_from_proto, hf_ingestion_item_from_proto, hf_ingestion_item_to_proto,
    hf_ingestion_to_proto, hf_key_from_proto, hf_key_to_proto,
};
use crate::core_store::{
    CF_OBSERVABILITY, CoreMetaTuplePart, TABLE_OBSERVABILITY_CURSOR_ROW, core_meta_tuple_key,
};
use crate::mvcc_product::ProductMutation;
use crate::mvcc_transaction::{LogicalKey, PredicateKind};
use crate::persistence::{HfIngestion, HfIngestionItem, HfKey};
use anyhow::{Result, anyhow};
use prost::Message;

const HF_KEY_PROJECTION_SCHEMA: &str = "anvil.hf.key_projection.v2";
const HF_INGESTION_PROJECTION_SCHEMA: &str = "anvil.hf.ingestion_projection.v2";
const HF_ITEM_PROJECTION_SCHEMA: &str = "anvil.hf.item_projection.v2";
const HF_INGESTION_STATUS_PROJECTION_SCHEMA: &str = "anvil.hf.ingestion_status_projection.v2";
const HF_TARGET_ITEM_PROJECTION_SCHEMA: &str = "anvil.hf.target_item_projection.v2";
const HF_PROJECTION_PAGE_MAX: usize = 1000;

#[derive(Debug, Clone)]
pub(crate) struct HfKeyPage {
    pub keys: Vec<HfKey>,
    pub next_cursor: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HfStoredItem {
    pub path: String,
    pub size: Option<i64>,
    pub etag: Option<String>,
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone)]
pub struct HfStoredItemPage {
    pub items: Vec<HfStoredItem>,
    pub next_cursor: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HfIngestionStatus {
    pub state: crate::tasks::HFIngestionState,
    pub queued: i64,
    pub downloading: i64,
    pub stored: i64,
    pub failed: i64,
    pub error: Option<String>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
struct HfIngestionStatusProjection {
    ingestion: HfIngestion,
    queued: i64,
    downloading: i64,
    stored: i64,
    failed: i64,
}

#[derive(Clone, PartialEq, Message)]
struct HfKeyProjectionProto {
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    key: Option<HfKeyProto>,
}

#[derive(Clone, PartialEq, Message)]
struct HfIngestionProjectionProto {
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    ingestion: Option<HfIngestionProto>,
}

#[derive(Clone, PartialEq, Message)]
struct HfItemProjectionProto {
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    item: Option<HfIngestionItemProto>,
}

#[derive(Clone, PartialEq, Message)]
struct HfIngestionStatusProjectionProto {
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    ingestion: Option<HfIngestionProto>,
    #[prost(int64, tag = "4")]
    queued: i64,
    #[prost(int64, tag = "5")]
    downloading: i64,
    #[prost(int64, tag = "6")]
    stored: i64,
    #[prost(int64, tag = "7")]
    failed: i64,
}

#[derive(Clone, PartialEq, Message)]
struct HfTargetItemProjectionProto {
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(string, tag = "4")]
    bucket: String,
    #[prost(string, tag = "5")]
    prefix: String,
    #[prost(message, optional, tag = "6")]
    item: Option<HfIngestionItemProto>,
}

pub(super) fn get_key_by_name(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    tenant_id: i64,
    name: &str,
) -> Result<Option<HfKey>> {
    let key = read_key(mvcc, snapshot, &key_name_key(tenant_id, name)?)?;
    if key
        .as_ref()
        .is_some_and(|key| key.tenant_id != tenant_id || key.name != name)
    {
        return Err(anyhow!("hf key-name projection scope mismatch"));
    }
    Ok(key)
}

pub(super) fn get_key_by_id(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    id: i64,
) -> Result<Option<HfKey>> {
    let key = read_key(mvcc, snapshot, &key_id_key(id)?)?;
    if key.as_ref().is_some_and(|key| key.id != id) {
        return Err(anyhow!("hf key-id projection scope mismatch"));
    }
    Ok(key)
}

pub(super) fn list_tenant_keys(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    tenant_id: i64,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfKeyPage> {
    let page = read_key_page(
        mvcc,
        snapshot,
        &key_name_prefix(tenant_id)?,
        after_cursor,
        limit,
    )?;
    if page.keys.iter().any(|key| key.tenant_id != tenant_id) {
        return Err(anyhow!("hf tenant key projection scope mismatch"));
    }
    Ok(page)
}

pub(super) fn list_all_keys(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfKeyPage> {
    read_key_page(mvcc, snapshot, &key_id_prefix()?, after_cursor, limit)
}

pub(super) fn get_ingestion(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    id: i64,
) -> Result<Option<HfIngestion>> {
    let Some(payload) = read_payload_at(mvcc, snapshot, &ingestion_key(id)?)? else {
        return Ok(None);
    };
    let row = HfIngestionProjectionProto::decode(payload.as_slice())?;
    ensure_deterministic_proto(&row, &payload, "hf ingestion projection")?;
    if row.schema != HF_INGESTION_PROJECTION_SCHEMA {
        return Err(anyhow!("hf ingestion projection schema mismatch"));
    }
    let ingestion = hf_ingestion_from_proto(
        row.ingestion
            .ok_or_else(|| anyhow!("hf ingestion projection is missing ingestion"))?,
    )?;
    if ingestion.id != id {
        return Err(anyhow!("hf ingestion projection scope mismatch"));
    }
    Ok(Some(ingestion))
}

pub(super) fn get_item(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    id: i64,
) -> Result<Option<HfIngestionItem>> {
    let item = read_item(mvcc, snapshot, &item_id_key(id)?)?;
    if item.as_ref().is_some_and(|item| item.id != id) {
        return Err(anyhow!("hf item-id projection scope mismatch"));
    }
    Ok(item)
}

pub(super) fn get_item_by_path(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    ingestion_id: i64,
    path: &str,
) -> Result<Option<HfIngestionItem>> {
    let item = read_item(mvcc, snapshot, &item_path_key(ingestion_id, path)?)?;
    if item
        .as_ref()
        .is_some_and(|item| item.ingestion_id != ingestion_id || item.path != path)
    {
        return Err(anyhow!("hf item-path projection scope mismatch"));
    }
    Ok(item)
}

pub(super) fn list_stored_items_for_ingestion(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    ingestion_id: i64,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfStoredItemPage> {
    read_stored_item_page(
        mvcc,
        snapshot,
        &stored_item_ingestion_prefix(ingestion_id)?,
        after_cursor,
        limit,
        |payload| {
            let item = read_item_payload(payload)?;
            if item.ingestion_id != ingestion_id {
                return Err(anyhow!(
                    "hf stored ingestion-item projection scope mismatch"
                ));
            }
            stored_item(item)
        },
    )
}

pub(super) fn list_stored_items_for_target(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    tenant_id: i64,
    bucket: &str,
    prefix: &str,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfStoredItemPage> {
    read_stored_item_page(
        mvcc,
        snapshot,
        &stored_item_target_prefix(tenant_id, bucket, prefix)?,
        after_cursor,
        limit,
        |payload| read_target_item_payload(payload, tenant_id, bucket, prefix),
    )
}

pub(super) fn get_ingestion_status(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    ingestion_id: i64,
) -> Result<Option<HfIngestionStatus>> {
    Ok(
        read_status_projection_at(mvcc, snapshot, ingestion_id)?.map(|projection| {
            HfIngestionStatus {
                state: projection.ingestion.state,
                queued: projection.queued,
                downloading: projection.downloading,
                stored: projection.stored,
                failed: projection.failed,
                error: projection.ingestion.error,
                started_at: projection.ingestion.started_at,
                finished_at: projection.ingestion.finished_at,
                created_at: projection.ingestion.created_at,
            }
        }),
    )
}

pub(super) struct HfProjectionPlan {
    pub mutations: Vec<ProductMutation>,
    pub predicates: Vec<(LogicalKey, PredicateKind)>,
}

pub(super) fn projection_plan(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    body: &HfBody,
) -> Result<HfProjectionPlan> {
    let mut desired = std::collections::BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
    match body {
        HfBody::KeyUpsert { key, .. } => {
            let payload = encode_key_projection(key)?;
            for tuple_key in [key_id_key(key.id)?, key_name_key(key.tenant_id, &key.name)?] {
                if let Some(existing) =
                    read_key_transaction(mvcc, transaction_id, principal, &tuple_key)?
                    && existing.id != key.id
                {
                    return Err(anyhow!("hugging face key identity is already in use"));
                }
                desired.insert(tuple_key, Some(payload.clone()));
            }
        }
        HfBody::KeyDelete {
            tenant_id,
            key_id,
            key_name,
            ..
        } => {
            desired.insert(key_id_key(*key_id)?, None);
            desired.insert(key_name_key(*tenant_id, key_name)?, None);
        }
        HfBody::IngestionUpsert { ingestion, .. } => {
            let existing_status =
                read_status_projection_transaction(mvcc, transaction_id, principal, ingestion.id)?;
            if existing_status.as_ref().is_some_and(|status| {
                status.ingestion.tenant_id != ingestion.tenant_id
                    || status.ingestion.target_bucket != ingestion.target_bucket
                    || status.ingestion.target_prefix != ingestion.target_prefix
            }) {
                return Err(anyhow!(
                    "hf ingestion target cannot change during an upsert"
                ));
            }
            let mut status = existing_status.unwrap_or(HfIngestionStatusProjection {
                ingestion: ingestion.clone(),
                queued: 0,
                downloading: 0,
                stored: 0,
                failed: 0,
            });
            status.ingestion = ingestion.clone();
            desired.insert(
                ingestion_key(ingestion.id)?,
                Some(encode_ingestion_projection(ingestion)?),
            );
            desired.insert(
                ingestion_status_key(ingestion.id)?,
                Some(encode_status_projection(&status)?),
            );
        }
        HfBody::ItemUpsert { item, .. } => {
            let previous =
                read_item_transaction(mvcc, transaction_id, principal, &item_id_key(item.id)?)?;
            if previous.as_ref().is_some_and(|previous| {
                previous.ingestion_id != item.ingestion_id || previous.path != item.path
            }) {
                return Err(anyhow!("hf item identity cannot change during an upsert"));
            }
            let mut status = read_status_projection_transaction(
                mvcc,
                transaction_id,
                principal,
                item.ingestion_id,
            )?
            .ok_or_else(|| anyhow!("hf item ingestion status projection is missing"))?;
            apply_item_transition(
                &mut status,
                previous.as_ref().map(|previous| previous.state),
                item.state,
            )?;
            let payload = encode_item_projection(item)?;
            desired.insert(item_id_key(item.id)?, Some(payload.clone()));
            desired.insert(
                item_path_key(item.ingestion_id, &item.path)?,
                Some(payload.clone()),
            );
            desired.insert(
                ingestion_status_key(item.ingestion_id)?,
                Some(encode_status_projection(&status)?),
            );
            let was_stored = previous.as_ref().is_some_and(|previous| {
                previous.state == crate::tasks::HFIngestionItemState::Stored
            });
            let is_stored = item.state == crate::tasks::HFIngestionItemState::Stored;
            let ingestion_item_key = stored_item_ingestion_key(item.ingestion_id, item.id)?;
            let target_item_key = stored_item_target_key(&status.ingestion, item.id)?;
            match (was_stored, is_stored) {
                (_, true) => {
                    desired.insert(ingestion_item_key, Some(payload));
                    desired.insert(
                        target_item_key,
                        Some(encode_target_item_projection(&status.ingestion, item)?),
                    );
                }
                (true, false) => {
                    desired.insert(ingestion_item_key, None);
                    desired.insert(target_item_key, None);
                }
                (false, false) => {}
            }
        }
    }

    let mut mutations = Vec::with_capacity(desired.len());
    let mut predicates = Vec::with_capacity(desired.len());
    for (tuple_key, value) in desired {
        let key = logical_key(&tuple_key)?;
        let observed = mvcc.read_transaction_value(transaction_id, principal, &key)?;
        predicates.push((
            key.clone(),
            observed
                .as_ref()
                .map(|payload| PredicateKind::ValueHash(*blake3::hash(payload).as_bytes()))
                .unwrap_or(PredicateKind::Absent),
        ));
        mutations.push(match value {
            Some(payload) => ProductMutation::put(key, payload),
            None => ProductMutation::delete(key),
        });
    }
    Ok(HfProjectionPlan {
        mutations,
        predicates,
    })
}

fn read_key(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    tuple_key: &[u8],
) -> Result<Option<HfKey>> {
    let Some(payload) = read_payload_at(mvcc, snapshot, tuple_key)? else {
        return Ok(None);
    };
    let row = HfKeyProjectionProto::decode(payload.as_slice())?;
    ensure_deterministic_proto(&row, &payload, "hf key projection")?;
    if row.schema != HF_KEY_PROJECTION_SCHEMA {
        return Err(anyhow!("hf key projection schema mismatch"));
    }
    Ok(Some(hf_key_from_proto(row.key.ok_or_else(|| {
        anyhow!("hf key projection is missing key")
    })?)?))
}

fn read_key_page(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    prefix: &[u8],
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfKeyPage> {
    if !(1..=HF_PROJECTION_PAGE_MAX).contains(&limit) {
        return Err(anyhow!(
            "hf key page size must be between 1 and {HF_PROJECTION_PAGE_MAX}"
        ));
    }
    let mut rows = scan_prefix_at(mvcc, snapshot, prefix, after_cursor, limit + 1)?;
    let has_more = rows.len() > limit;
    if has_more {
        rows.truncate(limit);
    }
    let next_cursor = if has_more {
        Some(
            rows.last()
                .ok_or_else(|| anyhow!("hf key continuation has no row"))?
                .0
                .clone(),
        )
    } else {
        None
    };
    let keys = rows
        .into_iter()
        .map(|(_, payload)| read_key_payload(&payload))
        .collect::<Result<Vec<_>>>()?;
    Ok(HfKeyPage { keys, next_cursor })
}

fn read_key_payload(payload: &[u8]) -> Result<HfKey> {
    let row = HfKeyProjectionProto::decode(payload)?;
    ensure_deterministic_proto(&row, payload, "hf key projection")?;
    if row.schema != HF_KEY_PROJECTION_SCHEMA {
        return Err(anyhow!("hf key projection schema mismatch"));
    }
    hf_key_from_proto(
        row.key
            .ok_or_else(|| anyhow!("hf key projection is missing key"))?,
    )
}

fn read_item(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    tuple_key: &[u8],
) -> Result<Option<HfIngestionItem>> {
    let Some(payload) = read_payload_at(mvcc, snapshot, tuple_key)? else {
        return Ok(None);
    };
    Ok(Some(read_item_payload(&payload)?))
}

fn read_item_payload(payload: &[u8]) -> Result<HfIngestionItem> {
    let row = HfItemProjectionProto::decode(payload)?;
    ensure_deterministic_proto(&row, payload, "hf item projection")?;
    if row.schema != HF_ITEM_PROJECTION_SCHEMA {
        return Err(anyhow!("hf item projection schema mismatch"));
    }
    hf_ingestion_item_from_proto(
        row.item
            .ok_or_else(|| anyhow!("hf item projection is missing item"))?,
    )
}

fn read_stored_item_page(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    prefix: &[u8],
    after_cursor: Option<&[u8]>,
    limit: usize,
    decode: impl Fn(&[u8]) -> Result<HfStoredItem>,
) -> Result<HfStoredItemPage> {
    validate_page_limit(limit, "hf stored item")?;
    let mut rows = scan_prefix_at(mvcc, snapshot, prefix, after_cursor, limit + 1)?;
    let has_more = rows.len() > limit;
    if has_more {
        rows.truncate(limit);
    }
    let next_cursor = if has_more {
        Some(
            rows.last()
                .ok_or_else(|| anyhow!("hf stored item continuation has no row"))?
                .0
                .clone(),
        )
    } else {
        None
    };
    let items = rows
        .into_iter()
        .map(|(_, payload)| decode(&payload))
        .collect::<Result<Vec<_>>>()?;
    Ok(HfStoredItemPage { items, next_cursor })
}

fn read_target_item_payload(
    payload: &[u8],
    tenant_id: i64,
    bucket: &str,
    prefix: &str,
) -> Result<HfStoredItem> {
    let row = HfTargetItemProjectionProto::decode(payload)?;
    ensure_deterministic_proto(&row, payload, "hf target item projection")?;
    if row.schema != HF_TARGET_ITEM_PROJECTION_SCHEMA
        || row.tenant_id != tenant_id
        || row.bucket != bucket
        || row.prefix != prefix
    {
        return Err(anyhow!("hf target item projection scope mismatch"));
    }
    let item = hf_ingestion_item_from_proto(
        row.item
            .ok_or_else(|| anyhow!("hf target item projection is missing item"))?,
    )?;
    stored_item(item)
}

fn read_status_projection_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    ingestion_id: i64,
) -> Result<Option<HfIngestionStatusProjection>> {
    let Some(payload) = read_payload_at(mvcc, snapshot, &ingestion_status_key(ingestion_id)?)?
    else {
        return Ok(None);
    };
    decode_status_projection(&payload, ingestion_id).map(Some)
}

fn decode_status_projection(
    payload: &[u8],
    ingestion_id: i64,
) -> Result<HfIngestionStatusProjection> {
    let row = HfIngestionStatusProjectionProto::decode(payload)?;
    ensure_deterministic_proto(&row, payload, "hf ingestion status projection")?;
    if row.schema != HF_INGESTION_STATUS_PROJECTION_SCHEMA {
        return Err(anyhow!("hf ingestion status projection schema mismatch"));
    }
    if [row.queued, row.downloading, row.stored, row.failed]
        .into_iter()
        .any(|count| count < 0)
    {
        return Err(anyhow!(
            "hf ingestion status projection has a negative count"
        ));
    }
    let ingestion = hf_ingestion_from_proto(
        row.ingestion
            .ok_or_else(|| anyhow!("hf ingestion status projection is missing ingestion"))?,
    )?;
    if ingestion.id != ingestion_id {
        return Err(anyhow!("hf ingestion status projection scope mismatch"));
    }
    Ok(HfIngestionStatusProjection {
        ingestion,
        queued: row.queued,
        downloading: row.downloading,
        stored: row.stored,
        failed: row.failed,
    })
}

fn stored_item(item: HfIngestionItem) -> Result<HfStoredItem> {
    if item.state != crate::tasks::HFIngestionItemState::Stored {
        return Err(anyhow!(
            "hf stored item projection contains a non-stored item"
        ));
    }
    Ok(HfStoredItem {
        path: item.path,
        size: item.size,
        etag: item.etag,
        finished_at: item.finished_at,
    })
}

fn validate_page_limit(limit: usize, label: &str) -> Result<()> {
    if !(1..=HF_PROJECTION_PAGE_MAX).contains(&limit) {
        return Err(anyhow!(
            "{label} page size must be between 1 and {HF_PROJECTION_PAGE_MAX}"
        ));
    }
    Ok(())
}

fn apply_item_transition(
    status: &mut HfIngestionStatusProjection,
    previous: Option<crate::tasks::HFIngestionItemState>,
    next: crate::tasks::HFIngestionItemState,
) -> Result<()> {
    if previous == Some(next) {
        return Ok(());
    }
    if let Some(previous) = previous {
        adjust_item_count(status, previous, -1)?;
    }
    adjust_item_count(status, next, 1)
}

fn adjust_item_count(
    status: &mut HfIngestionStatusProjection,
    state: crate::tasks::HFIngestionItemState,
    delta: i64,
) -> Result<()> {
    let count = match state {
        crate::tasks::HFIngestionItemState::Queued => &mut status.queued,
        crate::tasks::HFIngestionItemState::Downloading => &mut status.downloading,
        crate::tasks::HFIngestionItemState::Stored => &mut status.stored,
        crate::tasks::HFIngestionItemState::Failed => &mut status.failed,
        crate::tasks::HFIngestionItemState::Skipped => return Ok(()),
    };
    *count = count
        .checked_add(delta)
        .ok_or_else(|| anyhow!("hf ingestion status count overflow"))?;
    if *count < 0 {
        return Err(anyhow!("hf ingestion status count underflow"));
    }
    Ok(())
}

fn encode_key_projection(key: &HfKey) -> Result<Vec<u8>> {
    encode_proto(&HfKeyProjectionProto {
        schema: HF_KEY_PROJECTION_SCHEMA.to_string(),
        key: Some(hf_key_to_proto(key)),
    })
}

fn encode_ingestion_projection(ingestion: &HfIngestion) -> Result<Vec<u8>> {
    encode_proto(&HfIngestionProjectionProto {
        schema: HF_INGESTION_PROJECTION_SCHEMA.to_string(),
        ingestion: Some(hf_ingestion_to_proto(ingestion)),
    })
}

fn encode_item_projection(item: &HfIngestionItem) -> Result<Vec<u8>> {
    encode_proto(&HfItemProjectionProto {
        schema: HF_ITEM_PROJECTION_SCHEMA.to_string(),
        item: Some(hf_ingestion_item_to_proto(item)),
    })
}

fn encode_status_projection(status: &HfIngestionStatusProjection) -> Result<Vec<u8>> {
    encode_proto(&HfIngestionStatusProjectionProto {
        schema: HF_INGESTION_STATUS_PROJECTION_SCHEMA.to_string(),
        ingestion: Some(hf_ingestion_to_proto(&status.ingestion)),
        queued: status.queued,
        downloading: status.downloading,
        stored: status.stored,
        failed: status.failed,
    })
}

fn encode_target_item_projection(
    ingestion: &HfIngestion,
    item: &HfIngestionItem,
) -> Result<Vec<u8>> {
    encode_proto(&HfTargetItemProjectionProto {
        schema: HF_TARGET_ITEM_PROJECTION_SCHEMA.to_string(),
        tenant_id: ingestion.tenant_id,
        bucket: ingestion.target_bucket.clone(),
        prefix: ingestion.target_prefix.clone(),
        item: Some(hf_ingestion_item_to_proto(item)),
    })
}

fn encode_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn logical_key(tuple_key: &[u8]) -> Result<LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_OBSERVABILITY,
        TABLE_OBSERVABILITY_CURSOR_ROW,
        tuple_key,
    )
}

fn read_payload_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    tuple_key: &[u8],
) -> Result<Option<Vec<u8>>> {
    Ok(mvcc
        .runtime
        .read_at(&logical_key(tuple_key)?, snapshot)?
        .map(|row| row.value))
}

fn read_payload_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tuple_key: &[u8],
) -> Result<Option<Vec<u8>>> {
    mvcc.read_transaction_value(transaction_id, principal, &logical_key(tuple_key)?)
}

fn read_key_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tuple_key: &[u8],
) -> Result<Option<HfKey>> {
    read_payload_transaction(mvcc, transaction_id, principal, tuple_key)?
        .as_deref()
        .map(read_key_payload)
        .transpose()
}

fn read_item_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tuple_key: &[u8],
) -> Result<Option<HfIngestionItem>> {
    read_payload_transaction(mvcc, transaction_id, principal, tuple_key)?
        .as_deref()
        .map(read_item_payload)
        .transpose()
}

fn read_status_projection_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    ingestion_id: i64,
) -> Result<Option<HfIngestionStatusProjection>> {
    let Some(payload) = read_payload_transaction(
        mvcc,
        transaction_id,
        principal,
        &ingestion_status_key(ingestion_id)?,
    )?
    else {
        return Ok(None);
    };
    decode_status_projection(&payload, ingestion_id).map(Some)
}

fn scan_prefix_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    tuple_prefix: &[u8],
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_OBSERVABILITY, tuple_prefix)?;
    let mut rows = mvcc
        .runtime
        .scan_table_prefix_at(
            TABLE_OBSERVABILITY_CURSOR_ROW,
            &application_prefix,
            snapshot,
        )?
        .into_iter()
        .map(|(key, row)| {
            Ok((
                crate::mvcc_product::coremeta_tuple_from_logical_key(&key, CF_OBSERVABILITY)?
                    .to_vec(),
                row.value,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    rows.retain(|(tuple_key, _)| after_cursor.is_none_or(|after| tuple_key.as_slice() > after));
    rows.truncate(limit);
    Ok(rows)
}

fn key_id_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("key-id"),
    ])
}

fn key_id_key(id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("key-id"),
        CoreMetaTuplePart::I64(id),
    ])
}

fn key_name_prefix(tenant_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("key-name"),
        CoreMetaTuplePart::I64(tenant_id),
    ])
}

fn key_name_key(tenant_id: i64, name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("key-name"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(name),
    ])
}

fn ingestion_key(id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("ingestion"),
        CoreMetaTuplePart::I64(id),
    ])
}

fn ingestion_status_key(id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("ingestion-status"),
        CoreMetaTuplePart::I64(id),
    ])
}

fn stored_item_target_prefix(tenant_id: i64, bucket: &str, prefix: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("stored-item-target"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(bucket),
        CoreMetaTuplePart::Utf8(prefix),
    ])
}

fn stored_item_target_key(ingestion: &HfIngestion, item_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("stored-item-target"),
        CoreMetaTuplePart::I64(ingestion.tenant_id),
        CoreMetaTuplePart::Utf8(&ingestion.target_bucket),
        CoreMetaTuplePart::Utf8(&ingestion.target_prefix),
        CoreMetaTuplePart::I64(item_id),
    ])
}

fn item_id_key(id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("item"),
        CoreMetaTuplePart::I64(id),
    ])
}

fn stored_item_ingestion_prefix(ingestion_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("stored-item-ingestion"),
        CoreMetaTuplePart::I64(ingestion_id),
    ])
}

fn stored_item_ingestion_key(ingestion_id: i64, item_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("stored-item-ingestion"),
        CoreMetaTuplePart::I64(ingestion_id),
        CoreMetaTuplePart::I64(item_id),
    ])
}

fn item_path_key(ingestion_id: i64, path: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf"),
        CoreMetaTuplePart::Utf8("item-path"),
        CoreMetaTuplePart::I64(ingestion_id),
        CoreMetaTuplePart::Utf8(path),
    ])
}
