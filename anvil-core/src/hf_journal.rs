use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreMutationRootPublication, CoreStore,
};
use crate::formats::{Hash32, hash32, writer::WriterFamily};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{HfIngestion, HfIngestionItem, HfIngestionJob, HfKey};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use prost::{Message, Oneof};

mod projection;
pub(crate) use projection::HfKeyPage;
pub use projection::{HfIngestionStatus, HfStoredItem, HfStoredItemPage};

#[cfg(test)]
mod bounded_read_tests;

const HF_METADATA_BODY_SCHEMA: &str = "anvil.core.hf_metadata.v3";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HfMutationKind {
    KeyUpsert,
    KeyDelete,
    IngestionUpsert,
    ItemUpsert,
}

impl HfMutationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::KeyUpsert => "key_upsert",
            Self::KeyDelete => "key_delete",
            Self::IngestionUpsert => "ingestion_upsert",
            Self::ItemUpsert => "item_upsert",
        }
    }
}

#[derive(Debug, Clone)]
enum HfBody {
    KeyUpsert {
        key: HfKey,
        emitted_at: DateTime<Utc>,
    },
    KeyDelete {
        tenant_id: i64,
        key_id: i64,
        key_name: String,
        emitted_at: DateTime<Utc>,
    },
    IngestionUpsert {
        ingestion: HfIngestion,
        emitted_at: DateTime<Utc>,
    },
    ItemUpsert {
        item: HfIngestionItem,
        emitted_at: DateTime<Utc>,
    },
}

#[derive(Clone, PartialEq, Message)]
struct HfJournalBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    emitted_at: String,
    #[prost(uint64, tag = "3")]
    fence_token: u64,
    #[prost(string, tag = "4")]
    mutation_id: String,
    #[prost(oneof = "hf_journal_body_proto::Event", tags = "10, 11, 12, 13")]
    event: Option<hf_journal_body_proto::Event>,
}

mod hf_journal_body_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Event {
        #[prost(message, tag = "10")]
        KeyUpsert(super::HfKeyProto),
        #[prost(message, tag = "11")]
        KeyDelete(super::HfKeyDeleteProto),
        #[prost(message, tag = "12")]
        IngestionUpsert(super::HfIngestionProto),
        #[prost(message, tag = "13")]
        ItemUpsert(super::HfIngestionItemProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct HfKeyProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(bytes, tag = "3")]
    token_encrypted: Vec<u8>,
    #[prost(string, optional, tag = "4")]
    note: Option<String>,
    #[prost(string, tag = "5")]
    created_at: String,
    #[prost(string, tag = "6")]
    updated_at: String,
    #[prost(int64, tag = "7")]
    tenant_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct HfKeyDeleteProto {
    #[prost(int64, tag = "1")]
    tenant_id: i64,
    #[prost(string, tag = "2")]
    key_name: String,
    #[prost(int64, tag = "3")]
    key_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct HfIngestionProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    key_id: i64,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    requester_app_id: i64,
    #[prost(string, tag = "5")]
    repo: String,
    #[prost(string, tag = "6")]
    revision: String,
    #[prost(string, tag = "7")]
    target_bucket: String,
    #[prost(string, tag = "8")]
    target_region: String,
    #[prost(string, tag = "9")]
    target_prefix: String,
    #[prost(string, repeated, tag = "10")]
    include_globs: Vec<String>,
    #[prost(string, repeated, tag = "11")]
    exclude_globs: Vec<String>,
    #[prost(enumeration = "HfIngestionStateProto", tag = "12")]
    state: i32,
    #[prost(string, optional, tag = "13")]
    error: Option<String>,
    #[prost(string, tag = "14")]
    created_at: String,
    #[prost(string, optional, tag = "15")]
    started_at: Option<String>,
    #[prost(string, optional, tag = "16")]
    finished_at: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct HfIngestionItemProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    ingestion_id: i64,
    #[prost(string, tag = "3")]
    path: String,
    #[prost(int64, optional, tag = "4")]
    size: Option<i64>,
    #[prost(string, optional, tag = "5")]
    etag: Option<String>,
    #[prost(enumeration = "HfIngestionItemStateProto", tag = "6")]
    state: i32,
    #[prost(string, optional, tag = "7")]
    error: Option<String>,
    #[prost(string, tag = "8")]
    created_at: String,
    #[prost(string, optional, tag = "9")]
    started_at: Option<String>,
    #[prost(string, optional, tag = "10")]
    finished_at: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum HfIngestionStateProto {
    Unspecified = 0,
    Queued = 1,
    Running = 2,
    Completed = 3,
    Failed = 4,
    Canceled = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum HfIngestionItemStateProto {
    Unspecified = 0,
    Queued = 1,
    Downloading = 2,
    Stored = 3,
    Failed = 4,
    Skipped = 5,
}

#[derive(Debug, Clone, Default)]
struct HfWriteGuard {
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
}

#[cfg(test)]
async fn create_key(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
) -> Result<()> {
    create_key_inner(
        storage,
        tenant_id,
        name,
        token_encrypted,
        note,
        HfWriteGuard::default(),
    )
    .await
}

pub(crate) async fn create_key_with_permit(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    create_key_inner(storage, tenant_id, name, token_encrypted, note, guard).await
}

async fn create_key_inner(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
    guard: HfWriteGuard,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    if projection::get_key_by_name(&core_store, tenant_id, name)?.is_some() {
        return Err(anyhow!("hugging face key already exists"));
    }
    let (stream_precondition, id) = next_hf_entity_id(storage).await?;
    let now = Utc::now();
    append_body(
        storage,
        HfMutationKind::KeyUpsert,
        Some(HfKey {
            id,
            tenant_id,
            name: name.to_string(),
            token_encrypted: token_encrypted.to_vec(),
            note: note.map(ToOwned::to_owned),
            created_at: now,
            updated_at: now,
        }),
        None,
        None,
        None,
        guard,
        Some(stream_precondition),
    )
    .await
}

#[cfg(test)]
async fn delete_key(storage: &Storage, tenant_id: i64, name: &str) -> Result<u64> {
    delete_key_inner(storage, tenant_id, name, HfWriteGuard::default()).await
}

pub(crate) async fn delete_key_with_permit(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    delete_key_inner(storage, tenant_id, name, guard).await
}

async fn delete_key_inner(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
    guard: HfWriteGuard,
) -> Result<u64> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let key = projection::get_key_by_name(&core_store, tenant_id, name)?;
    if let Some(key) = key {
        append_body(
            storage,
            HfMutationKind::KeyDelete,
            None,
            Some((tenant_id, key.id, name.to_string())),
            None,
            None,
            guard,
            None,
        )
        .await?;
        return Ok(1);
    }
    Ok(0)
}

pub async fn get_key_encrypted(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
) -> Result<Option<(i64, Vec<u8>)>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(projection::get_key_by_name(&core_store, tenant_id, name)?
        .map(|key| (key.id, key.token_encrypted)))
}

pub async fn get_key_encrypted_by_id(
    storage: &Storage,
    tenant_id: i64,
    id: i64,
) -> Result<Option<Vec<u8>>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(projection::get_key_by_id(&core_store, id)?
        .filter(|key| key.tenant_id == tenant_id)
        .map(|key| key.token_encrypted))
}

pub(crate) async fn list_encrypted_key_page(
    storage: &Storage,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfKeyPage> {
    let core_store = CoreStore::new(storage.clone()).await?;
    projection::list_all_keys(&core_store, after_cursor, limit)
}

pub(crate) async fn update_key_encrypted_with_permit(
    storage: &Storage,
    id: i64,
    token_encrypted: &[u8],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut key = projection::get_key_by_id(&core_store, id)?
        .ok_or_else(|| anyhow!("hugging face key not found"))?;
    key.token_encrypted = token_encrypted.to_vec();
    key.updated_at = Utc::now();
    append_body(
        storage,
        HfMutationKind::KeyUpsert,
        Some(key),
        None,
        None,
        None,
        guard,
        None,
    )
    .await
}

pub(crate) async fn list_key_page(
    storage: &Storage,
    tenant_id: i64,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfKeyPage> {
    let core_store = CoreStore::new(storage.clone()).await?;
    projection::list_tenant_keys(&core_store, tenant_id, after_cursor, limit)
}

pub(crate) async fn hf_collection_revision(storage: &Storage) -> Result<String> {
    Ok(CoreStore::new(storage.clone())
        .await?
        .stream_head_sequence(&hf_metadata_stream_id())
        .await?
        .to_string())
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn create_ingestion(
    storage: &Storage,
    key_id: i64,
    tenant_id: i64,
    requester_app_id: i64,
    repo: &str,
    revision: Option<&str>,
    target_bucket: &str,
    target_region: &str,
    target_prefix: Option<&str>,
    include_globs: &[String],
    exclude_globs: &[String],
) -> Result<i64> {
    create_ingestion_inner(
        storage,
        key_id,
        tenant_id,
        requester_app_id,
        repo,
        revision,
        target_bucket,
        target_region,
        target_prefix,
        include_globs,
        exclude_globs,
        HfWriteGuard::default(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_ingestion_with_permit(
    storage: &Storage,
    key_id: i64,
    tenant_id: i64,
    requester_app_id: i64,
    repo: &str,
    revision: Option<&str>,
    target_bucket: &str,
    target_region: &str,
    target_prefix: Option<&str>,
    include_globs: &[String],
    exclude_globs: &[String],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<i64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    create_ingestion_inner(
        storage,
        key_id,
        tenant_id,
        requester_app_id,
        repo,
        revision,
        target_bucket,
        target_region,
        target_prefix,
        include_globs,
        exclude_globs,
        guard,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_ingestion_inner(
    storage: &Storage,
    key_id: i64,
    tenant_id: i64,
    requester_app_id: i64,
    repo: &str,
    revision: Option<&str>,
    target_bucket: &str,
    target_region: &str,
    target_prefix: Option<&str>,
    include_globs: &[String],
    exclude_globs: &[String],
    guard: HfWriteGuard,
) -> Result<i64> {
    let (stream_precondition, id) = next_hf_entity_id(storage).await?;
    append_body(
        storage,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(HfIngestion {
            id,
            key_id,
            tenant_id,
            requester_app_id,
            repo: repo.to_string(),
            revision: revision.unwrap_or("main").to_string(),
            target_bucket: target_bucket.to_string(),
            target_region: target_region.to_string(),
            target_prefix: target_prefix.unwrap_or_default().to_string(),
            include_globs: include_globs.to_vec(),
            exclude_globs: exclude_globs.to_vec(),
            state: crate::tasks::HFIngestionState::Queued,
            error: None,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
        }),
        None,
        guard,
        Some(stream_precondition),
    )
    .await?;
    Ok(id)
}

pub async fn get_ingestion_job(storage: &Storage, id: i64) -> Result<Option<HfIngestionJob>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(
        projection::get_ingestion(&core_store, id)?.map(|job| HfIngestionJob {
            key_id: job.key_id,
            tenant_id: job.tenant_id,
            requester_app_id: job.requester_app_id,
            repo: job.repo,
            revision: job.revision,
            target_bucket: job.target_bucket,
            target_region: job.target_region,
            target_prefix: job.target_prefix,
            include_globs: job.include_globs,
            exclude_globs: job.exclude_globs,
        }),
    )
}

#[cfg(test)]
async fn update_ingestion_state(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
) -> Result<()> {
    update_ingestion_state_inner(storage, id, state_value, error, HfWriteGuard::default()).await
}

pub(crate) async fn update_ingestion_state_with_permit(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    update_ingestion_state_inner(storage, id, state_value, error, guard).await
}

async fn update_ingestion_state_inner(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
    guard: HfWriteGuard,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(mut job) = projection::get_ingestion(&core_store, id)? else {
        return Ok(());
    };
    job.state = state_value;
    job.error = error.map(ToOwned::to_owned);
    if state_value == crate::tasks::HFIngestionState::Running && job.started_at.is_none() {
        job.started_at = Some(Utc::now());
    }
    if matches!(
        state_value,
        crate::tasks::HFIngestionState::Completed
            | crate::tasks::HFIngestionState::Failed
            | crate::tasks::HFIngestionState::Canceled
    ) {
        job.finished_at = Some(Utc::now());
    }
    append_body(
        storage,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(job),
        None,
        guard,
        None,
    )
    .await
}

pub(crate) async fn cancel_ingestion_with_permit(
    storage: &Storage,
    id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    cancel_ingestion_inner(storage, id, guard).await
}

async fn cancel_ingestion_inner(storage: &Storage, id: i64, guard: HfWriteGuard) -> Result<u64> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(mut job) = projection::get_ingestion(&core_store, id)? else {
        return Ok(0);
    };
    if !matches!(
        job.state,
        crate::tasks::HFIngestionState::Queued | crate::tasks::HFIngestionState::Running
    ) {
        return Ok(0);
    }
    job.state = crate::tasks::HFIngestionState::Canceled;
    job.finished_at = Some(Utc::now());
    append_body(
        storage,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(job),
        None,
        guard,
        None,
    )
    .await?;
    Ok(1)
}

#[cfg(test)]
async fn add_item(
    storage: &Storage,
    ingestion_id: i64,
    path: &str,
    size: Option<i64>,
    etag: Option<&str>,
) -> Result<i64> {
    add_item_inner(
        storage,
        ingestion_id,
        path,
        size,
        etag,
        HfWriteGuard::default(),
    )
    .await
}

pub(crate) async fn add_item_with_permit(
    storage: &Storage,
    ingestion_id: i64,
    path: &str,
    size: Option<i64>,
    etag: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<i64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    add_item_inner(storage, ingestion_id, path, size, etag, guard).await
}

async fn add_item_inner(
    storage: &Storage,
    ingestion_id: i64,
    path: &str,
    size: Option<i64>,
    etag: Option<&str>,
    guard: HfWriteGuard,
) -> Result<i64> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let existing = projection::get_item_by_path(&core_store, ingestion_id, path)?;
    let (stream_precondition, id) = if let Some(item) = existing.as_ref() {
        (None, item.id)
    } else {
        let (precondition, id) = next_hf_entity_id(storage).await?;
        (Some(precondition), id)
    };
    let mut item = existing.unwrap_or_else(|| HfIngestionItem {
        id,
        ingestion_id,
        path: path.to_string(),
        size,
        etag: etag.map(ToOwned::to_owned),
        state: crate::tasks::HFIngestionItemState::Queued,
        error: None,
        created_at: Utc::now(),
        started_at: None,
        finished_at: None,
    });
    item.size = size;
    item.etag = etag.map(ToOwned::to_owned);
    let id = item.id;
    append_body(
        storage,
        HfMutationKind::ItemUpsert,
        None,
        None,
        None,
        Some(item),
        guard,
        stream_precondition,
    )
    .await?;
    Ok(id)
}

pub(crate) async fn update_item_state_with_permit(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionItemState,
    error: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    update_item_state_inner(storage, id, state_value, error, guard).await
}

async fn update_item_state_inner(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionItemState,
    error: Option<&str>,
    guard: HfWriteGuard,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(mut item) = projection::get_item(&core_store, id)? else {
        return Ok(());
    };
    item.state = state_value;
    item.error = error.map(ToOwned::to_owned);
    if state_value == crate::tasks::HFIngestionItemState::Downloading && item.started_at.is_none() {
        item.started_at = Some(Utc::now());
    }
    if matches!(
        state_value,
        crate::tasks::HFIngestionItemState::Stored
            | crate::tasks::HFIngestionItemState::Failed
            | crate::tasks::HFIngestionItemState::Skipped
    ) {
        item.finished_at = Some(Utc::now());
    }
    append_body(
        storage,
        HfMutationKind::ItemUpsert,
        None,
        None,
        None,
        Some(item),
        guard,
        None,
    )
    .await
}

#[cfg(test)]
async fn update_item_success(storage: &Storage, id: i64, size: i64, etag: &str) -> Result<()> {
    update_item_success_inner(storage, id, size, etag, HfWriteGuard::default()).await
}

pub(crate) async fn update_item_success_with_permit(
    storage: &Storage,
    id: i64,
    size: i64,
    etag: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    update_item_success_inner(storage, id, size, etag, guard).await
}

async fn update_item_success_inner(
    storage: &Storage,
    id: i64,
    size: i64,
    etag: &str,
    guard: HfWriteGuard,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(mut item) = projection::get_item(&core_store, id)? else {
        return Ok(());
    };
    item.state = crate::tasks::HFIngestionItemState::Stored;
    item.size = Some(size);
    item.etag = Some(etag.to_string());
    item.finished_at = Some(Utc::now());
    append_body(
        storage,
        HfMutationKind::ItemUpsert,
        None,
        None,
        None,
        Some(item),
        guard,
        None,
    )
    .await
}

pub async fn list_stored_ingestion_item_page(
    storage: &Storage,
    ingestion_id: i64,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfStoredItemPage> {
    let core_store = CoreStore::new(storage.clone()).await?;
    projection::list_stored_items_for_ingestion(&core_store, ingestion_id, after_cursor, limit)
}

pub async fn list_stored_target_item_page(
    storage: &Storage,
    tenant_id: i64,
    bucket: &str,
    prefix: &str,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfStoredItemPage> {
    let core_store = CoreStore::new(storage.clone()).await?;
    projection::list_stored_items_for_target(
        &core_store,
        tenant_id,
        bucket,
        prefix,
        after_cursor,
        limit,
    )
}

pub async fn get_ingestion_status(storage: &Storage, id: i64) -> Result<HfIngestionStatus> {
    let core_store = CoreStore::new(storage.clone()).await?;
    projection::get_ingestion_status(&core_store, id)?.ok_or_else(|| anyhow!("ingestion not found"))
}

async fn append_body(
    storage: &Storage,
    event: HfMutationKind,
    key: Option<HfKey>,
    key_delete: Option<(i64, i64, String)>,
    ingestion: Option<HfIngestion>,
    item: Option<HfIngestionItem>,
    guard: HfWriteGuard,
    stream_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mutation_id = uuid::Uuid::new_v4();
    let key_text = key
        .as_ref()
        .map(|key| format!("tenant/{}/key/{}", key.tenant_id, key.id))
        .or_else(|| {
            key_delete
                .as_ref()
                .map(|(tenant_id, _, name)| format!("tenant/{tenant_id}/key-name/{name}"))
        })
        .or_else(|| {
            ingestion
                .as_ref()
                .map(|job| format!("ingestion/{}", job.id))
        })
        .or_else(|| item.as_ref().map(|item| format!("item/{}", item.id)))
        .unwrap_or_else(|| event.as_str().to_string());
    let body = hf_body_from_parts(event, key, key_delete, ingestion, item, Utc::now())?;
    let payload = encode_hf_body(&body, guard.fence_token, mutation_id)?;
    let partition_id = hex::encode(hf_partition_id());
    let stream_id = hf_metadata_stream_id();
    let stream_precondition = match stream_precondition {
        Some(precondition) => precondition,
        None => core_store.stream_head_precondition(&stream_id).await?,
    };
    let root_generation = next_stream_generation(&stream_precondition)?;
    let transaction_id = format!("hf-metadata:{key_text}:{mutation_id}");
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id: stream_id.clone(),
        record_kind: "hf_metadata".to_string(),
        payload,
        idempotency_key: Some(transaction_id.clone()),
    }];
    operations.extend(projection::projection_operations(
        &core_store,
        &body,
        &stream_id,
        root_generation,
        &transaction_id,
        &partition_id,
    )?);
    let projection_root = projection::projection_root_anchor_key(&stream_id);
    let mut preconditions: Vec<_> = guard.partition_precondition.into_iter().collect();
    preconditions.push(stream_precondition);
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id.clone(),
            committed_by_principal: hf_partition_principal(),
            root_publications: vec![
                CoreMutationRootPublication::new(partition_id, WriterFamily::CoreControl.as_str())
                    .coordinator(),
                CoreMutationRootPublication::new(
                    projection_root,
                    WriterFamily::ObjectBlob.as_str(),
                ),
            ],
            preconditions,
            operations,
        })
        .await?;
    Ok(())
}

fn hf_body_from_parts(
    event: HfMutationKind,
    key: Option<HfKey>,
    key_delete: Option<(i64, i64, String)>,
    ingestion: Option<HfIngestion>,
    item: Option<HfIngestionItem>,
    emitted_at: DateTime<Utc>,
) -> Result<HfBody> {
    match event {
        HfMutationKind::KeyUpsert => Ok(HfBody::KeyUpsert {
            key: key.ok_or_else(|| anyhow!("hf key upsert body is missing key"))?,
            emitted_at,
        }),
        HfMutationKind::KeyDelete => {
            let (tenant_id, key_id, key_name) = key_delete
                .ok_or_else(|| anyhow!("hf key delete body is missing tenant and key name"))?;
            Ok(HfBody::KeyDelete {
                tenant_id,
                key_id,
                key_name,
                emitted_at,
            })
        }
        HfMutationKind::IngestionUpsert => Ok(HfBody::IngestionUpsert {
            ingestion: ingestion
                .ok_or_else(|| anyhow!("hf ingestion upsert body is missing ingestion"))?,
            emitted_at,
        }),
        HfMutationKind::ItemUpsert => Ok(HfBody::ItemUpsert {
            item: item.ok_or_else(|| anyhow!("hf item upsert body is missing item"))?,
            emitted_at,
        }),
    }
}

fn encode_hf_body(body: &HfBody, fence_token: u64, mutation_id: uuid::Uuid) -> Result<Vec<u8>> {
    encode_deterministic_proto(&hf_body_to_proto(body, fence_token, mutation_id)?)
}

fn decode_hf_body(bytes: &[u8]) -> Result<HfBody> {
    let proto = HfJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "hf metadata body")?;
    hf_body_from_proto(proto)
}

fn hf_body_to_proto(
    body: &HfBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<HfJournalBodyProto> {
    Ok(match body {
        HfBody::KeyUpsert { key, emitted_at } => HfJournalBodyProto {
            schema: HF_METADATA_BODY_SCHEMA.to_string(),
            emitted_at: emitted_at.to_rfc3339(),
            fence_token,
            mutation_id: mutation_id.to_string(),
            event: Some(hf_journal_body_proto::Event::KeyUpsert(hf_key_to_proto(
                key,
            ))),
        },
        HfBody::KeyDelete {
            tenant_id,
            key_id,
            key_name,
            emitted_at,
        } => HfJournalBodyProto {
            schema: HF_METADATA_BODY_SCHEMA.to_string(),
            emitted_at: emitted_at.to_rfc3339(),
            fence_token,
            mutation_id: mutation_id.to_string(),
            event: Some(hf_journal_body_proto::Event::KeyDelete(HfKeyDeleteProto {
                tenant_id: *tenant_id,
                key_name: key_name.clone(),
                key_id: *key_id,
            })),
        },
        HfBody::IngestionUpsert {
            ingestion,
            emitted_at,
        } => HfJournalBodyProto {
            schema: HF_METADATA_BODY_SCHEMA.to_string(),
            emitted_at: emitted_at.to_rfc3339(),
            fence_token,
            mutation_id: mutation_id.to_string(),
            event: Some(hf_journal_body_proto::Event::IngestionUpsert(
                hf_ingestion_to_proto(ingestion),
            )),
        },
        HfBody::ItemUpsert { item, emitted_at } => HfJournalBodyProto {
            schema: HF_METADATA_BODY_SCHEMA.to_string(),
            emitted_at: emitted_at.to_rfc3339(),
            fence_token,
            mutation_id: mutation_id.to_string(),
            event: Some(hf_journal_body_proto::Event::ItemUpsert(
                hf_ingestion_item_to_proto(item),
            )),
        },
    })
}

fn hf_body_from_proto(proto: HfJournalBodyProto) -> Result<HfBody> {
    if proto.schema != HF_METADATA_BODY_SCHEMA {
        return Err(anyhow!("hf metadata body has invalid schema"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("hf metadata body has invalid mutation id"))?;
    let emitted_at = parse_required_hf_time(&proto.emitted_at, "emitted_at")?;
    match proto
        .event
        .ok_or_else(|| anyhow!("hf metadata body is missing event"))?
    {
        hf_journal_body_proto::Event::KeyUpsert(key) => Ok(HfBody::KeyUpsert {
            key: hf_key_from_proto(key)?,
            emitted_at,
        }),
        hf_journal_body_proto::Event::KeyDelete(key) => Ok(HfBody::KeyDelete {
            tenant_id: key.tenant_id,
            key_id: key.key_id,
            key_name: key.key_name,
            emitted_at,
        }),
        hf_journal_body_proto::Event::IngestionUpsert(ingestion) => Ok(HfBody::IngestionUpsert {
            ingestion: hf_ingestion_from_proto(ingestion)?,
            emitted_at,
        }),
        hf_journal_body_proto::Event::ItemUpsert(item) => Ok(HfBody::ItemUpsert {
            item: hf_ingestion_item_from_proto(item)?,
            emitted_at,
        }),
    }
}

#[cfg(test)]
fn decode_hf_body_fence(bytes: &[u8]) -> Result<u64> {
    let proto = HfJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "hf metadata body")?;
    if proto.schema != HF_METADATA_BODY_SCHEMA {
        return Err(anyhow!("hf metadata body has invalid schema"));
    }
    Ok(proto.fence_token)
}

fn hf_key_to_proto(key: &HfKey) -> HfKeyProto {
    HfKeyProto {
        id: key.id,
        tenant_id: key.tenant_id,
        name: key.name.clone(),
        token_encrypted: key.token_encrypted.clone(),
        note: key.note.clone(),
        created_at: key.created_at.to_rfc3339(),
        updated_at: key.updated_at.to_rfc3339(),
    }
}

fn hf_key_from_proto(proto: HfKeyProto) -> Result<HfKey> {
    Ok(HfKey {
        id: proto.id,
        tenant_id: proto.tenant_id,
        name: proto.name,
        token_encrypted: proto.token_encrypted,
        note: proto.note,
        created_at: parse_required_hf_time(&proto.created_at, "key.created_at")?,
        updated_at: parse_required_hf_time(&proto.updated_at, "key.updated_at")?,
    })
}

fn hf_ingestion_to_proto(ingestion: &HfIngestion) -> HfIngestionProto {
    HfIngestionProto {
        id: ingestion.id,
        key_id: ingestion.key_id,
        tenant_id: ingestion.tenant_id,
        requester_app_id: ingestion.requester_app_id,
        repo: ingestion.repo.clone(),
        revision: ingestion.revision.clone(),
        target_bucket: ingestion.target_bucket.clone(),
        target_region: ingestion.target_region.clone(),
        target_prefix: ingestion.target_prefix.clone(),
        include_globs: ingestion.include_globs.clone(),
        exclude_globs: ingestion.exclude_globs.clone(),
        state: hf_ingestion_state_to_proto(ingestion.state) as i32,
        error: ingestion.error.clone(),
        created_at: ingestion.created_at.to_rfc3339(),
        started_at: ingestion.started_at.as_ref().map(DateTime::to_rfc3339),
        finished_at: ingestion.finished_at.as_ref().map(DateTime::to_rfc3339),
    }
}

fn hf_ingestion_from_proto(proto: HfIngestionProto) -> Result<HfIngestion> {
    Ok(HfIngestion {
        id: proto.id,
        key_id: proto.key_id,
        tenant_id: proto.tenant_id,
        requester_app_id: proto.requester_app_id,
        repo: proto.repo,
        revision: proto.revision,
        target_bucket: proto.target_bucket,
        target_region: proto.target_region,
        target_prefix: proto.target_prefix,
        include_globs: proto.include_globs,
        exclude_globs: proto.exclude_globs,
        state: hf_ingestion_state_from_proto(proto.state)?,
        error: proto.error,
        created_at: parse_required_hf_time(&proto.created_at, "ingestion.created_at")?,
        started_at: parse_optional_hf_time(proto.started_at, "ingestion.started_at")?,
        finished_at: parse_optional_hf_time(proto.finished_at, "ingestion.finished_at")?,
    })
}

fn hf_ingestion_item_to_proto(item: &HfIngestionItem) -> HfIngestionItemProto {
    HfIngestionItemProto {
        id: item.id,
        ingestion_id: item.ingestion_id,
        path: item.path.clone(),
        size: item.size,
        etag: item.etag.clone(),
        state: hf_ingestion_item_state_to_proto(item.state) as i32,
        error: item.error.clone(),
        created_at: item.created_at.to_rfc3339(),
        started_at: item.started_at.as_ref().map(DateTime::to_rfc3339),
        finished_at: item.finished_at.as_ref().map(DateTime::to_rfc3339),
    }
}

fn hf_ingestion_item_from_proto(proto: HfIngestionItemProto) -> Result<HfIngestionItem> {
    Ok(HfIngestionItem {
        id: proto.id,
        ingestion_id: proto.ingestion_id,
        path: proto.path,
        size: proto.size,
        etag: proto.etag,
        state: hf_ingestion_item_state_from_proto(proto.state)?,
        error: proto.error,
        created_at: parse_required_hf_time(&proto.created_at, "item.created_at")?,
        started_at: parse_optional_hf_time(proto.started_at, "item.started_at")?,
        finished_at: parse_optional_hf_time(proto.finished_at, "item.finished_at")?,
    })
}

fn hf_ingestion_state_to_proto(state: crate::tasks::HFIngestionState) -> HfIngestionStateProto {
    match state {
        crate::tasks::HFIngestionState::Queued => HfIngestionStateProto::Queued,
        crate::tasks::HFIngestionState::Running => HfIngestionStateProto::Running,
        crate::tasks::HFIngestionState::Completed => HfIngestionStateProto::Completed,
        crate::tasks::HFIngestionState::Failed => HfIngestionStateProto::Failed,
        crate::tasks::HFIngestionState::Canceled => HfIngestionStateProto::Canceled,
    }
}

fn hf_ingestion_state_from_proto(value: i32) -> Result<crate::tasks::HFIngestionState> {
    Ok(
        match HfIngestionStateProto::try_from(value)
            .map_err(|_| anyhow!("hf ingestion body has invalid state"))?
        {
            HfIngestionStateProto::Unspecified => {
                return Err(anyhow!("hf ingestion body has unspecified state"));
            }
            HfIngestionStateProto::Queued => crate::tasks::HFIngestionState::Queued,
            HfIngestionStateProto::Running => crate::tasks::HFIngestionState::Running,
            HfIngestionStateProto::Completed => crate::tasks::HFIngestionState::Completed,
            HfIngestionStateProto::Failed => crate::tasks::HFIngestionState::Failed,
            HfIngestionStateProto::Canceled => crate::tasks::HFIngestionState::Canceled,
        },
    )
}

fn hf_ingestion_item_state_to_proto(
    state: crate::tasks::HFIngestionItemState,
) -> HfIngestionItemStateProto {
    match state {
        crate::tasks::HFIngestionItemState::Queued => HfIngestionItemStateProto::Queued,
        crate::tasks::HFIngestionItemState::Downloading => HfIngestionItemStateProto::Downloading,
        crate::tasks::HFIngestionItemState::Stored => HfIngestionItemStateProto::Stored,
        crate::tasks::HFIngestionItemState::Failed => HfIngestionItemStateProto::Failed,
        crate::tasks::HFIngestionItemState::Skipped => HfIngestionItemStateProto::Skipped,
    }
}

fn hf_ingestion_item_state_from_proto(value: i32) -> Result<crate::tasks::HFIngestionItemState> {
    Ok(
        match HfIngestionItemStateProto::try_from(value)
            .map_err(|_| anyhow!("hf ingestion item body has invalid state"))?
        {
            HfIngestionItemStateProto::Unspecified => {
                return Err(anyhow!("hf ingestion item body has unspecified state"));
            }
            HfIngestionItemStateProto::Queued => crate::tasks::HFIngestionItemState::Queued,
            HfIngestionItemStateProto::Downloading => {
                crate::tasks::HFIngestionItemState::Downloading
            }
            HfIngestionItemStateProto::Stored => crate::tasks::HFIngestionItemState::Stored,
            HfIngestionItemStateProto::Failed => crate::tasks::HFIngestionItemState::Failed,
            HfIngestionItemStateProto::Skipped => crate::tasks::HFIngestionItemState::Skipped,
        },
    )
}

fn parse_required_hf_time(value: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|err| anyhow!("hf metadata body has invalid {field}: {err}"))
}

fn parse_optional_hf_time(value: Option<String>, field: &str) -> Result<Option<DateTime<Utc>>> {
    value
        .map(|time| parse_required_hf_time(&time, field))
        .transpose()
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    let encoded = encode_deterministic_proto(message)?;
    if encoded != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
}

async fn next_hf_entity_id(storage: &Storage) -> Result<(CoreMutationPrecondition, i64)> {
    let precondition = CoreStore::new(storage.clone())
        .await?
        .stream_head_precondition(&hf_metadata_stream_id())
        .await?;
    let generation = next_stream_generation(&precondition)?;
    Ok((
        precondition,
        i64::try_from(generation).map_err(|_| anyhow!("hf entity id exceeds i64"))?,
    ))
}

fn next_stream_generation(precondition: &CoreMutationPrecondition) -> Result<u64> {
    let CoreMutationPrecondition::StreamHead {
        expected_last_sequence,
        ..
    } = precondition
    else {
        return Err(anyhow!("hf stream precondition has wrong kind"));
    };
    expected_last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("hf stream sequence overflow"))
}

pub fn hf_partition_id() -> Hash32 {
    hash32(b"hf_metadata/global")
}

fn hf_metadata_stream_id() -> String {
    "hf_metadata:global".to_string()
}

fn hf_partition_principal() -> String {
    "partition-owner:hf_metadata:global".to_string()
}

#[cfg(test)]
pub(crate) async fn read_hf_frame_fences_for_test(storage: &Storage) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut after_sequence = 0;
    let mut fences = Vec::new();
    loop {
        let page = core_store
            .read_stream_page(crate::core_store::ReadStream {
                stream_id: hf_metadata_stream_id(),
                after_sequence,
                limit: 256,
            })
            .await?;
        for record in page.records {
            if record.record_kind == "hf_metadata" {
                fences.push(decode_hf_body_fence(&record.payload)?);
            }
        }
        if !page.has_more || page.next_sequence == after_sequence {
            break;
        }
        after_sequence = page.next_sequence;
    }
    Ok(fences)
}

async fn hf_write_guard(
    storage: &Storage,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<HfWriteGuard> {
    require_hf_permit(permit)?;
    Ok(HfWriteGuard {
        fence_token: permit.fence_token,
        partition_precondition: Some(
            partition_write_precondition(storage, permit, partition_owner_signing_key).await?,
        ),
    })
}

fn require_hf_permit(permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "hf_metadata"
        || permit.partition_id != hex::encode(hf_partition_id())
    {
        anyhow::bail!("hf metadata write permit targets a different partition");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery,
        force_expire_partition_owner_for_node, publish_partition_ready,
    };
    use tempfile::tempdir;

    const KEY: &[u8] = b"hf metadata partition owner key";

    #[tokio::test]
    async fn hf_journal_replays_keys_ingestions_and_items() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        create_key(&storage, 1, "primary", b"secret", Some("note"))
            .await
            .unwrap();
        let (key_id, secret) = get_key_encrypted(&storage, 1, "primary")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(secret, b"secret");
        let ingestion_id = create_ingestion(
            &storage,
            key_id,
            1,
            2,
            "owner/repo",
            None,
            "bucket",
            "region",
            Some("prefix"),
            &[],
            &[],
        )
        .await
        .unwrap();
        update_ingestion_state(
            &storage,
            ingestion_id,
            crate::tasks::HFIngestionState::Running,
            None,
        )
        .await
        .unwrap();
        let item_id = add_item(&storage, ingestion_id, "a.txt", None, None)
            .await
            .unwrap();
        update_item_success(&storage, item_id, 10, "etag")
            .await
            .unwrap();
        assert_eq!(
            list_stored_ingestion_item_page(&storage, ingestion_id, None, 10)
                .await
                .unwrap()
                .items
                .len(),
            1
        );
        let status = get_ingestion_status(&storage, ingestion_id).await.unwrap();
        assert_eq!(status.stored, 1);
        assert_eq!(delete_key(&storage, 1, "primary").await.unwrap(), 1);
        assert!(
            get_key_encrypted_by_id(&storage, 1, key_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn hf_keys_are_isolated_by_tenant() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        create_key(&storage, 11, "shared-name", b"tenant-11", None)
            .await
            .unwrap();
        create_key(&storage, 12, "shared-name", b"tenant-12", None)
            .await
            .unwrap();

        let (tenant_11_key_id, tenant_11_secret) = get_key_encrypted(&storage, 11, "shared-name")
            .await
            .unwrap()
            .unwrap();
        let (tenant_12_key_id, tenant_12_secret) = get_key_encrypted(&storage, 12, "shared-name")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(tenant_11_key_id, tenant_12_key_id);
        assert_eq!(tenant_11_secret, b"tenant-11");
        assert_eq!(tenant_12_secret, b"tenant-12");
        assert_eq!(
            list_key_page(&storage, 11, None, 10)
                .await
                .unwrap()
                .keys
                .len(),
            1
        );
        assert_eq!(
            list_key_page(&storage, 12, None, 10)
                .await
                .unwrap()
                .keys
                .len(),
            1
        );

        assert_eq!(delete_key(&storage, 11, "shared-name").await.unwrap(), 1);
        assert!(
            get_key_encrypted(&storage, 11, "shared-name")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            get_key_encrypted(&storage, 12, "shared-name")
                .await
                .unwrap()
                .unwrap()
                .1,
            b"tenant-12"
        );
    }

    #[tokio::test]
    async fn hf_key_pages_do_not_read_unrelated_tenant_history() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for index in 0..48 {
            create_key(
                &storage,
                99,
                &format!("unrelated-{index:03}"),
                b"secret",
                None,
            )
            .await
            .unwrap();
        }
        for index in 0..3 {
            create_key(&storage, 11, &format!("target-{index:03}"), b"secret", None)
                .await
                .unwrap();
        }

        let first = list_key_page(&storage, 11, None, 2).await.unwrap();
        assert_eq!(first.keys.len(), 2);
        let second = list_key_page(&storage, 11, first.next_cursor.as_deref(), 2)
            .await
            .unwrap();
        assert_eq!(second.keys.len(), 1);
        assert!(second.next_cursor.is_none());
        assert!(second.keys.iter().all(|key| key.tenant_id == 11));
    }

    #[tokio::test]
    async fn hf_metadata_frame_bodies_are_deterministic_protobuf() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        create_key(&storage, 1, "primary", b"secret", Some("note"))
            .await
            .unwrap();
        let (key_id, _) = get_key_encrypted(&storage, 1, "primary")
            .await
            .unwrap()
            .unwrap();
        let ingestion_id = create_ingestion(
            &storage,
            key_id,
            1,
            2,
            "owner/repo",
            None,
            "bucket",
            "region",
            Some("prefix"),
            &["*.safetensors".to_string()],
            &["tmp/*".to_string()],
        )
        .await
        .unwrap();
        let item_id = add_item(&storage, ingestion_id, "a.txt", Some(10), Some("etag"))
            .await
            .unwrap();
        update_item_success(&storage, item_id, 10, "etag")
            .await
            .unwrap();
        delete_key(&storage, 1, "primary").await.unwrap();

        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let records = core_store
            .read_stream(crate::core_store::ReadStream {
                stream_id: hf_metadata_stream_id(),
                after_sequence: 0,
                limit: 5,
            })
            .await
            .unwrap();
        assert_eq!(records.len(), 5);

        let mut saw_key_upsert = false;
        let mut saw_key_delete = false;
        let mut saw_ingestion = false;
        let mut saw_item = false;
        for record in records {
            assert_eq!(record.record_kind, "hf_metadata");
            assert_eq!(record.payload.first().copied(), Some(0x0a));

            let proto = HfJournalBodyProto::decode(record.payload.as_slice()).unwrap();
            assert_eq!(proto.schema, HF_METADATA_BODY_SCHEMA);

            let body = decode_hf_body(&record.payload).unwrap();
            let reencoded = encode_deterministic_proto(&proto).unwrap();
            assert_eq!(reencoded, record.payload);

            match body {
                HfBody::KeyUpsert { key, .. } => {
                    saw_key_upsert = true;
                    assert_eq!(key.id, key_id);
                    assert_eq!(key.tenant_id, 1);
                    assert_eq!(key.name, "primary");
                    assert_eq!(key.token_encrypted, b"secret");
                }
                HfBody::KeyDelete {
                    tenant_id,
                    key_name,
                    ..
                } => {
                    saw_key_delete = true;
                    assert_eq!(tenant_id, 1);
                    assert_eq!(key_name, "primary");
                }
                HfBody::IngestionUpsert { ingestion, .. } => {
                    saw_ingestion = true;
                    assert_eq!(ingestion.id, ingestion_id);
                    assert_eq!(ingestion.include_globs, vec!["*.safetensors".to_string()]);
                    assert_eq!(ingestion.exclude_globs, vec!["tmp/*".to_string()]);
                }
                HfBody::ItemUpsert { item, .. } => {
                    saw_item = true;
                    assert_eq!(item.ingestion_id, ingestion_id);
                    assert_eq!(item.path, "a.txt");
                }
            }
        }
        assert!(saw_key_upsert);
        assert!(saw_key_delete);
        assert!(saw_ingestion);
        assert!(saw_item);
    }

    #[test]
    fn hf_metadata_body_rejects_invalid_schema_and_unknown_fields() {
        let body = HfBody::KeyDelete {
            tenant_id: 1,
            key_id: 7,
            key_name: "primary".to_string(),
            emitted_at: fixed_hf_time(),
        };

        let mutation_id = uuid::Uuid::from_u128(0x1234_5678_1234_5678_1234_5678_1234_5678);
        let mut invalid_schema = hf_body_to_proto(&body, 0, mutation_id).unwrap();
        invalid_schema.schema = "anvil.core.hf_metadata.v0".to_string();
        let invalid_schema_bytes = encode_deterministic_proto(&invalid_schema).unwrap();
        let err = decode_hf_body(&invalid_schema_bytes).unwrap_err();
        assert!(
            err.to_string().contains("invalid schema"),
            "unexpected invalid schema error: {err}"
        );

        let mut unknown_field = encode_hf_body(&body, 0, mutation_id).unwrap();
        unknown_field.extend_from_slice(&[0xf8, 0x07, 0x01]);
        let err = decode_hf_body(&unknown_field).unwrap_err();
        assert!(
            err.to_string().contains("not deterministically encoded"),
            "unexpected unknown field error: {err}"
        );
    }

    #[tokio::test]
    pub(crate) async fn hf_journal_with_permit_writes_fenced_frames() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let permit = owner.write_permit().unwrap();

        create_key_with_permit(
            &storage,
            1,
            "primary",
            b"secret",
            Some("note"),
            &permit,
            KEY,
        )
        .await
        .unwrap();
        let (key_id, _) = get_key_encrypted(&storage, 1, "primary")
            .await
            .unwrap()
            .unwrap();
        let ingestion_id = create_ingestion_with_permit(
            &storage,
            key_id,
            1,
            2,
            "owner/repo",
            None,
            "bucket",
            "region",
            Some("prefix"),
            &[],
            &[],
            &permit,
            KEY,
        )
        .await
        .unwrap();
        update_ingestion_state_with_permit(
            &storage,
            ingestion_id,
            crate::tasks::HFIngestionState::Running,
            None,
            &permit,
            KEY,
        )
        .await
        .unwrap();
        let item_id =
            add_item_with_permit(&storage, ingestion_id, "a.txt", None, None, &permit, KEY)
                .await
                .unwrap();
        update_item_state_with_permit(
            &storage,
            item_id,
            crate::tasks::HFIngestionItemState::Downloading,
            None,
            &permit,
            KEY,
        )
        .await
        .unwrap();
        update_item_success_with_permit(&storage, item_id, 10, "etag", &permit, KEY)
            .await
            .unwrap();
        delete_key_with_permit(&storage, 1, "primary", &permit, KEY)
            .await
            .unwrap();

        let fences = read_hf_frame_fences_for_test(&storage).await.unwrap();
        assert_eq!(fences.len(), 7);
        assert!(fences.iter().all(|fence| *fence == permit.fence_token));
    }

    #[tokio::test]
    pub(crate) async fn hf_journal_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        force_expire_partition_owner_for_node(
            &storage,
            &owner.partition_family,
            &owner.partition_id,
            "node-a",
            250,
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_key_with_permit(
            &storage,
            1,
            "primary",
            b"secret",
            Some("note"),
            &stale_permit,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("write permit owner is not current")
        );
    }

    #[tokio::test]
    pub(crate) async fn hf_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        force_expire_partition_owner_for_node(
            &storage,
            &owner.partition_family,
            &owner.partition_id,
            "node-a",
            250,
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_key_inner(
            &storage,
            1,
            "primary",
            b"secret",
            Some("note"),
            HfWriteGuard {
                fence_token: stale_permit.fence_token,
                partition_precondition: Some(stale_precondition),
            },
        )
        .await
        .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("generation mismatch")
                || message.contains("target mismatch")
                || message.contains("precondition failed"),
            "unexpected stale precondition error: {message}"
        );

        create_key_with_permit(
            &storage,
            1,
            "primary",
            b"secret",
            Some("note"),
            &newer.write_permit().unwrap(),
            KEY,
        )
        .await
        .unwrap();
    }

    async fn ready_owner(
        storage: &Storage,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "hf_metadata".to_string();
        let id = hex::encode(hf_partition_id());
        let recovering = acquire_partition_recovery(
            storage,
            PartitionRecoveryAcquire {
                partition_family: family.clone(),
                partition_id: id.clone(),
                owner_node_id: owner_node_id.to_string(),
                recovered_through_sequence: 0,
                recovered_manifest_hash: hex::encode([0; 32]),
                now_nanos: 100,
            },
            KEY,
        )
        .await
        .unwrap();
        publish_partition_ready(
            storage,
            &family,
            &id,
            owner_node_id,
            recovering.fence_token,
            0,
            &hex::encode([1; 32]),
            200,
            KEY,
        )
        .await
        .unwrap()
    }

    fn fixed_hf_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z")
            .unwrap()
            .with_timezone(&Utc)
    }
}
