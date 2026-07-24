use super::{
    HfBody, HfIngestionItemProto, HfIngestionProto, HfKeyProto, ensure_deterministic_proto,
    hf_ingestion_from_proto, hf_ingestion_item_from_proto, hf_ingestion_item_to_proto,
    hf_ingestion_to_proto, hf_key_from_proto, hf_key_to_proto,
};
use crate::core_store::{
    CF_OBSERVABILITY, CoreMetaTuplePart, CoreMutationOperation, CoreStore,
    TABLE_OBSERVABILITY_CURSOR_ROW, core_meta_committed_row_common, core_meta_record_tuple_key,
    core_meta_root_key_hash, core_meta_tuple_key,
};
use crate::persistence::{HfIngestion, HfIngestionItem, HfKey};
use anyhow::{Result, anyhow};
use prost::Message;

const HF_KEY_PROJECTION_SCHEMA: &str = "anvil.hf.key_projection.v1";
const HF_INGESTION_PROJECTION_SCHEMA: &str = "anvil.hf.ingestion_projection.v1";
const HF_ITEM_PROJECTION_SCHEMA: &str = "anvil.hf.item_projection.v1";
const HF_INGESTION_STATUS_PROJECTION_SCHEMA: &str = "anvil.hf.ingestion_status_projection.v1";
const HF_TARGET_ITEM_PROJECTION_SCHEMA: &str = "anvil.hf.target_item_projection.v1";
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
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    key: Option<HfKeyProto>,
}

#[derive(Clone, PartialEq, Message)]
struct HfIngestionProjectionProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    ingestion: Option<HfIngestionProto>,
}

#[derive(Clone, PartialEq, Message)]
struct HfItemProjectionProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    item: Option<HfIngestionItemProto>,
}

#[derive(Clone, PartialEq, Message)]
struct HfIngestionStatusProjectionProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
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
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
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
    store: &CoreStore,
    tenant_id: i64,
    name: &str,
) -> Result<Option<HfKey>> {
    let key = read_key(store, &key_name_key(tenant_id, name)?)?;
    if key
        .as_ref()
        .is_some_and(|key| key.tenant_id != tenant_id || key.name != name)
    {
        return Err(anyhow!("hf key-name projection scope mismatch"));
    }
    Ok(key)
}

pub(super) fn get_key_by_id(store: &CoreStore, id: i64) -> Result<Option<HfKey>> {
    let key = read_key(store, &key_id_key(id)?)?;
    if key.as_ref().is_some_and(|key| key.id != id) {
        return Err(anyhow!("hf key-id projection scope mismatch"));
    }
    Ok(key)
}

pub(super) fn list_tenant_keys(
    store: &CoreStore,
    tenant_id: i64,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfKeyPage> {
    let page = read_key_page(store, &key_name_prefix(tenant_id)?, after_cursor, limit)?;
    if page.keys.iter().any(|key| key.tenant_id != tenant_id) {
        return Err(anyhow!("hf tenant key projection scope mismatch"));
    }
    Ok(page)
}

pub(super) fn list_all_keys(
    store: &CoreStore,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfKeyPage> {
    read_key_page(store, &key_id_prefix()?, after_cursor, limit)
}

pub(super) fn get_ingestion(store: &CoreStore, id: i64) -> Result<Option<HfIngestion>> {
    let Some(payload) = read_payload(store, &ingestion_key(id)?)? else {
        return Ok(None);
    };
    let row = HfIngestionProjectionProto::decode(payload.as_slice())?;
    ensure_deterministic_proto(&row, &payload, "hf ingestion projection")?;
    if row.common.is_none() || row.schema != HF_INGESTION_PROJECTION_SCHEMA {
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

pub(super) fn get_item(store: &CoreStore, id: i64) -> Result<Option<HfIngestionItem>> {
    let item = read_item(store, &item_id_key(id)?)?;
    if item.as_ref().is_some_and(|item| item.id != id) {
        return Err(anyhow!("hf item-id projection scope mismatch"));
    }
    Ok(item)
}

pub(super) fn get_item_by_path(
    store: &CoreStore,
    ingestion_id: i64,
    path: &str,
) -> Result<Option<HfIngestionItem>> {
    let item = read_item(store, &item_path_key(ingestion_id, path)?)?;
    if item
        .as_ref()
        .is_some_and(|item| item.ingestion_id != ingestion_id || item.path != path)
    {
        return Err(anyhow!("hf item-path projection scope mismatch"));
    }
    Ok(item)
}

pub(super) fn list_stored_items_for_ingestion(
    store: &CoreStore,
    ingestion_id: i64,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfStoredItemPage> {
    read_stored_item_page(
        store,
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
    store: &CoreStore,
    tenant_id: i64,
    bucket: &str,
    prefix: &str,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfStoredItemPage> {
    read_stored_item_page(
        store,
        &stored_item_target_prefix(tenant_id, bucket, prefix)?,
        after_cursor,
        limit,
        |payload| read_target_item_payload(payload, tenant_id, bucket, prefix),
    )
}

pub(super) fn get_ingestion_status(
    store: &CoreStore,
    ingestion_id: i64,
) -> Result<Option<HfIngestionStatus>> {
    Ok(
        read_status_projection(store, ingestion_id)?.map(|projection| HfIngestionStatus {
            state: projection.ingestion.state,
            queued: projection.queued,
            downloading: projection.downloading,
            stored: projection.stored,
            failed: projection.failed,
            error: projection.ingestion.error,
            started_at: projection.ingestion.started_at,
            finished_at: projection.ingestion.finished_at,
            created_at: projection.ingestion.created_at,
        }),
    )
}

pub(super) fn projection_operations(
    store: &CoreStore,
    body: &HfBody,
    stream_id: &str,
    root_generation: u64,
    transaction_id: &str,
    partition_id: &str,
) -> Result<Vec<CoreMutationOperation>> {
    let root_hash = core_meta_root_key_hash(&projection_root_anchor_key(stream_id));
    match body {
        HfBody::KeyUpsert { key, .. } => {
            let payload = encode_key_projection(key, &root_hash, root_generation, transaction_id)?;
            Ok(vec![
                put(partition_id, key_id_key(key.id)?, payload.clone()),
                put(
                    partition_id,
                    key_name_key(key.tenant_id, &key.name)?,
                    payload,
                ),
            ])
        }
        HfBody::KeyDelete {
            tenant_id,
            key_id,
            key_name,
            ..
        } => Ok(vec![
            delete(partition_id, key_id_key(*key_id)?),
            delete(partition_id, key_name_key(*tenant_id, key_name)?),
        ]),
        HfBody::IngestionUpsert { ingestion, .. } => {
            let payload = encode_ingestion_projection(
                ingestion,
                &root_hash,
                root_generation,
                transaction_id,
            )?;
            let existing_status = read_status_projection(store, ingestion.id)?;
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
            let status_payload =
                encode_status_projection(&status, &root_hash, root_generation, transaction_id)?;
            Ok(vec![
                put(partition_id, ingestion_key(ingestion.id)?, payload),
                put(
                    partition_id,
                    ingestion_status_key(ingestion.id)?,
                    status_payload,
                ),
            ])
        }
        HfBody::ItemUpsert { item, .. } => {
            let previous = get_item(store, item.id)?;
            if previous.as_ref().is_some_and(|previous| {
                previous.ingestion_id != item.ingestion_id || previous.path != item.path
            }) {
                return Err(anyhow!("hf item identity cannot change during an upsert"));
            }
            let mut status = read_status_projection(store, item.ingestion_id)?
                .ok_or_else(|| anyhow!("hf item ingestion status projection is missing"))?;
            apply_item_transition(
                &mut status,
                previous.as_ref().map(|previous| previous.state),
                item.state,
            )?;
            let payload =
                encode_item_projection(item, &root_hash, root_generation, transaction_id)?;
            let status_payload =
                encode_status_projection(&status, &root_hash, root_generation, transaction_id)?;
            let mut operations = vec![
                put(partition_id, item_id_key(item.id)?, payload.clone()),
                put(
                    partition_id,
                    item_path_key(item.ingestion_id, &item.path)?,
                    payload,
                ),
                put(
                    partition_id,
                    ingestion_status_key(item.ingestion_id)?,
                    status_payload,
                ),
            ];
            let was_stored = previous.as_ref().is_some_and(|previous| {
                previous.state == crate::tasks::HFIngestionItemState::Stored
            });
            let is_stored = item.state == crate::tasks::HFIngestionItemState::Stored;
            let ingestion_item_key = stored_item_ingestion_key(item.ingestion_id, item.id)?;
            let target_item_key = stored_item_target_key(&status.ingestion, item.id)?;
            match (was_stored, is_stored) {
                (_, true) => {
                    let item_payload =
                        encode_item_projection(item, &root_hash, root_generation, transaction_id)?;
                    let target_payload = encode_target_item_projection(
                        &status.ingestion,
                        item,
                        &root_hash,
                        root_generation,
                        transaction_id,
                    )?;
                    operations.push(put(partition_id, ingestion_item_key, item_payload));
                    operations.push(put(partition_id, target_item_key, target_payload));
                }
                (true, false) => {
                    operations.push(delete(partition_id, ingestion_item_key));
                    operations.push(delete(partition_id, target_item_key));
                }
                (false, false) => {}
            }
            Ok(operations)
        }
    }
}

pub(super) fn projection_root_anchor_key(stream_id: &str) -> String {
    format!("stream/{stream_id}")
}

fn read_key(store: &CoreStore, tuple_key: &[u8]) -> Result<Option<HfKey>> {
    let Some(payload) = read_payload(store, tuple_key)? else {
        return Ok(None);
    };
    let row = HfKeyProjectionProto::decode(payload.as_slice())?;
    ensure_deterministic_proto(&row, &payload, "hf key projection")?;
    if row.common.is_none() || row.schema != HF_KEY_PROJECTION_SCHEMA {
        return Err(anyhow!("hf key projection schema mismatch"));
    }
    Ok(Some(hf_key_from_proto(row.key.ok_or_else(|| {
        anyhow!("hf key projection is missing key")
    })?)?))
}

fn read_key_page(
    store: &CoreStore,
    prefix: &[u8],
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfKeyPage> {
    if !(1..=HF_PROJECTION_PAGE_MAX).contains(&limit) {
        return Err(anyhow!(
            "hf key page size must be between 1 and {HF_PROJECTION_PAGE_MAX}"
        ));
    }
    let mut rows = store.scan_coremeta_prefix_page(
        CF_OBSERVABILITY,
        TABLE_OBSERVABILITY_CURSOR_ROW,
        prefix,
        after_cursor,
        limit + 1,
    )?;
    let has_more = rows.len() > limit;
    if has_more {
        rows.truncate(limit);
    }
    let next_cursor = if has_more {
        Some(
            core_meta_record_tuple_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("hf key continuation has no row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let keys = rows
        .into_iter()
        .map(|row| read_key_payload(&row.payload))
        .collect::<Result<Vec<_>>>()?;
    Ok(HfKeyPage { keys, next_cursor })
}

fn read_key_payload(payload: &[u8]) -> Result<HfKey> {
    let row = HfKeyProjectionProto::decode(payload)?;
    ensure_deterministic_proto(&row, payload, "hf key projection")?;
    if row.common.is_none() || row.schema != HF_KEY_PROJECTION_SCHEMA {
        return Err(anyhow!("hf key projection schema mismatch"));
    }
    hf_key_from_proto(
        row.key
            .ok_or_else(|| anyhow!("hf key projection is missing key"))?,
    )
}

fn read_item(store: &CoreStore, tuple_key: &[u8]) -> Result<Option<HfIngestionItem>> {
    let Some(payload) = read_payload(store, tuple_key)? else {
        return Ok(None);
    };
    Ok(Some(read_item_payload(&payload)?))
}

fn read_item_payload(payload: &[u8]) -> Result<HfIngestionItem> {
    let row = HfItemProjectionProto::decode(payload)?;
    ensure_deterministic_proto(&row, payload, "hf item projection")?;
    if row.common.is_none() || row.schema != HF_ITEM_PROJECTION_SCHEMA {
        return Err(anyhow!("hf item projection schema mismatch"));
    }
    hf_ingestion_item_from_proto(
        row.item
            .ok_or_else(|| anyhow!("hf item projection is missing item"))?,
    )
}

fn read_stored_item_page(
    store: &CoreStore,
    prefix: &[u8],
    after_cursor: Option<&[u8]>,
    limit: usize,
    decode: impl Fn(&[u8]) -> Result<HfStoredItem>,
) -> Result<HfStoredItemPage> {
    validate_page_limit(limit, "hf stored item")?;
    let mut rows = store.scan_coremeta_prefix_page(
        CF_OBSERVABILITY,
        TABLE_OBSERVABILITY_CURSOR_ROW,
        prefix,
        after_cursor,
        limit + 1,
    )?;
    let has_more = rows.len() > limit;
    if has_more {
        rows.truncate(limit);
    }
    let next_cursor = if has_more {
        Some(
            core_meta_record_tuple_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("hf stored item continuation has no row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let items = rows
        .into_iter()
        .map(|row| decode(&row.payload))
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
    if row.common.is_none()
        || row.schema != HF_TARGET_ITEM_PROJECTION_SCHEMA
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

fn read_status_projection(
    store: &CoreStore,
    ingestion_id: i64,
) -> Result<Option<HfIngestionStatusProjection>> {
    let Some(payload) = read_payload(store, &ingestion_status_key(ingestion_id)?)? else {
        return Ok(None);
    };
    let row = HfIngestionStatusProjectionProto::decode(payload.as_slice())?;
    ensure_deterministic_proto(&row, &payload, "hf ingestion status projection")?;
    if row.common.is_none() || row.schema != HF_INGESTION_STATUS_PROJECTION_SCHEMA {
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
    Ok(Some(HfIngestionStatusProjection {
        ingestion,
        queued: row.queued,
        downloading: row.downloading,
        stored: row.stored,
        failed: row.failed,
    }))
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

fn read_payload(store: &CoreStore, tuple_key: &[u8]) -> Result<Option<Vec<u8>>> {
    store.read_coremeta_row(CF_OBSERVABILITY, TABLE_OBSERVABILITY_CURSOR_ROW, tuple_key)
}

fn encode_key_projection(
    key: &HfKey,
    root_hash: &str,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_proto(&HfKeyProjectionProto {
        common: Some(common(root_hash, root_generation, transaction_id)),
        schema: HF_KEY_PROJECTION_SCHEMA.to_string(),
        key: Some(hf_key_to_proto(key)),
    })
}

fn encode_ingestion_projection(
    ingestion: &HfIngestion,
    root_hash: &str,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_proto(&HfIngestionProjectionProto {
        common: Some(common(root_hash, root_generation, transaction_id)),
        schema: HF_INGESTION_PROJECTION_SCHEMA.to_string(),
        ingestion: Some(hf_ingestion_to_proto(ingestion)),
    })
}

fn encode_item_projection(
    item: &HfIngestionItem,
    root_hash: &str,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_proto(&HfItemProjectionProto {
        common: Some(common(root_hash, root_generation, transaction_id)),
        schema: HF_ITEM_PROJECTION_SCHEMA.to_string(),
        item: Some(hf_ingestion_item_to_proto(item)),
    })
}

fn encode_status_projection(
    status: &HfIngestionStatusProjection,
    root_hash: &str,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_proto(&HfIngestionStatusProjectionProto {
        common: Some(common(root_hash, root_generation, transaction_id)),
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
    root_hash: &str,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_proto(&HfTargetItemProjectionProto {
        common: Some(common(root_hash, root_generation, transaction_id)),
        schema: HF_TARGET_ITEM_PROJECTION_SCHEMA.to_string(),
        tenant_id: ingestion.tenant_id,
        bucket: ingestion.target_bucket.clone(),
        prefix: ingestion.target_prefix.clone(),
        item: Some(hf_ingestion_item_to_proto(item)),
    })
}

fn common(
    root_hash: &str,
    root_generation: u64,
    transaction_id: &str,
) -> crate::core_store::CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        "system",
        root_hash,
        root_generation,
        transaction_id,
        root_generation,
    )
}

fn encode_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn put(partition_id: &str, tuple_key: Vec<u8>, payload: Vec<u8>) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: CF_OBSERVABILITY.to_string(),
        table_id: TABLE_OBSERVABILITY_CURSOR_ROW,
        tuple_key,
        payload,
    }
}

fn delete(partition_id: &str, tuple_key: Vec<u8>) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaDelete {
        partition_id: partition_id.to_string(),
        cf: CF_OBSERVABILITY.to_string(),
        table_id: TABLE_OBSERVABILITY_CURSOR_ROW,
        tuple_key,
    }
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
