use crate::core_store::{
    CF_OBSERVABILITY, CoreMetaTuplePart, TABLE_OBSERVABILITY_CURSOR_ROW, core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::mvcc_bootstrap::MvccSubsystem;
use crate::mvcc_product::{ProductMutation, coremeta_logical_key};
use crate::mvcc_transaction::{DurabilityLevel, LogicalKey, PredicateKind, ReadConsistency};
use crate::partition_fence::PartitionWritePermit;
use crate::persistence::{HfIngestion, HfIngestionItem, HfIngestionJob, HfKey};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use prost::{Message, Oneof};
use std::time::Duration;

mod projection;
pub(crate) use projection::HfKeyPage;
pub use projection::{HfIngestionStatus, HfStoredItem, HfStoredItemPage};

const HF_METADATA_BODY_SCHEMA: &str = "anvil.core.hf_metadata.v3";
const HF_JOURNAL_HEAD_SCHEMA: &str = "anvil.core.hf_journal_head.v1";
const HF_JOURNAL_EVENT_SCHEMA: &str = "anvil.core.hf_journal_event.v1";

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

#[derive(Clone, PartialEq, Message)]
struct HfJournalHeadProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(uint64, tag = "2")]
    last_sequence: u64,
    #[prost(uint64, tag = "3")]
    next_entity_id: u64,
    #[prost(bytes, tag = "4")]
    last_event_hash: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct HfJournalEventProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(uint64, tag = "2")]
    sequence: u64,
    #[prost(bytes, tag = "3")]
    previous_event_hash: Vec<u8>,
    #[prost(bytes, tag = "4")]
    event_hash: Vec<u8>,
    #[prost(string, tag = "5")]
    mutation_id: String,
    #[prost(bytes, tag = "6")]
    body: Vec<u8>,
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
}

pub(crate) async fn create_key_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    create_key_inner(storage, mvcc, tenant_id, name, token_encrypted, note, guard).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn stage_create_key_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
    transaction_id: &str,
    principal: &str,
    now_unix_ms: u64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    let now = Utc::now();
    stage_body(
        mvcc,
        transaction_id,
        principal,
        HfMutationKind::KeyUpsert,
        Some(HfKey {
            id: 0,
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
        uuid::Uuid::new_v4(),
        now_unix_ms,
    )
    .await
    .map(|_| ())
}

async fn create_key_inner(
    _storage: &Storage,
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
    guard: HfWriteGuard,
) -> Result<()> {
    if projection::get_key_by_name(mvcc, mvcc.runtime.applied_version()?, tenant_id, name)?
        .is_some()
    {
        return Err(anyhow!("hugging face key already exists"));
    }
    let now = Utc::now();
    append_body(
        mvcc,
        HfMutationKind::KeyUpsert,
        Some(HfKey {
            id: 0,
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
    )
    .await
    .map(|_| ())
}

pub(crate) async fn delete_key_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    name: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    delete_key_inner(storage, mvcc, tenant_id, name, guard).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn stage_delete_key_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    name: &str,
    transaction_id: &str,
    principal: &str,
    now_unix_ms: u64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    let Some(key) =
        projection::get_key_by_name(mvcc, mvcc.runtime.applied_version()?, tenant_id, name)?
    else {
        return Ok(0);
    };
    stage_body(
        mvcc,
        transaction_id,
        principal,
        HfMutationKind::KeyDelete,
        None,
        Some((tenant_id, key.id, name.to_string())),
        None,
        None,
        guard,
        uuid::Uuid::new_v4(),
        now_unix_ms,
    )
    .await?;
    Ok(1)
}

async fn delete_key_inner(
    _storage: &Storage,
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    name: &str,
    guard: HfWriteGuard,
) -> Result<u64> {
    let key = projection::get_key_by_name(mvcc, mvcc.runtime.applied_version()?, tenant_id, name)?;
    if let Some(key) = key {
        append_body(
            mvcc,
            HfMutationKind::KeyDelete,
            None,
            Some((tenant_id, key.id, name.to_string())),
            None,
            None,
            guard,
        )
        .await?;
        return Ok(1);
    }
    Ok(0)
}

pub async fn get_key_encrypted(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    name: &str,
) -> Result<Option<(i64, Vec<u8>)>> {
    Ok(
        projection::get_key_by_name(mvcc, mvcc.runtime.applied_version()?, tenant_id, name)?
            .map(|key| (key.id, key.token_encrypted)),
    )
}

pub(crate) fn get_key_record(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    name: &str,
) -> Result<Option<HfKey>> {
    projection::get_key_by_name(mvcc, mvcc.runtime.applied_version()?, tenant_id, name)
}

pub async fn get_key_encrypted_by_id(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    id: i64,
) -> Result<Option<Vec<u8>>> {
    Ok(
        projection::get_key_by_id(mvcc, mvcc.runtime.applied_version()?, id)?
            .filter(|key| key.tenant_id == tenant_id)
            .map(|key| key.token_encrypted),
    )
}

pub(crate) async fn list_encrypted_key_page(
    mvcc: &MvccSubsystem,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfKeyPage> {
    projection::list_all_keys(mvcc, mvcc.runtime.applied_version()?, after_cursor, limit)
}

pub(crate) async fn update_key_encrypted_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    id: i64,
    token_encrypted: &[u8],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    let mut key = projection::get_key_by_id(mvcc, mvcc.runtime.applied_version()?, id)?
        .ok_or_else(|| anyhow!("hugging face key not found"))?;
    key.token_encrypted = token_encrypted.to_vec();
    key.updated_at = Utc::now();
    append_body(
        mvcc,
        HfMutationKind::KeyUpsert,
        Some(key),
        None,
        None,
        None,
        guard,
    )
    .await
    .map(|_| ())
}

pub(crate) async fn list_key_page(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfKeyPage> {
    projection::list_tenant_keys(
        mvcc,
        mvcc.runtime.applied_version()?,
        tenant_id,
        after_cursor,
        limit,
    )
}

pub(crate) async fn hf_collection_revision(mvcc: &MvccSubsystem) -> Result<String> {
    let snapshot = mvcc.runtime.applied_version()?;
    Ok(mvcc
        .runtime
        .read_at(&hf_head_logical_key()?, snapshot)?
        .as_ref()
        .map(|row| decode_hf_head(&row.value))
        .transpose()?
        .map(|head| head.last_sequence)
        .unwrap_or_default()
        .to_string())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_ingestion_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
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
        mvcc,
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
pub(crate) async fn stage_create_ingestion_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
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
    transaction_id: &str,
    principal: &str,
    now_unix_ms: u64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<i64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    stage_body(
        mvcc,
        transaction_id,
        principal,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(HfIngestion {
            id: 0,
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
        uuid::Uuid::new_v4(),
        now_unix_ms,
    )
    .await?
    .ok_or_else(|| anyhow!("hf ingestion creation did not allocate an id"))
}

#[allow(clippy::too_many_arguments)]
async fn create_ingestion_inner(
    _storage: &Storage,
    mvcc: &MvccSubsystem,
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
    let id = append_body(
        mvcc,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(HfIngestion {
            id: 0,
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
    )
    .await?
    .ok_or_else(|| anyhow!("hf ingestion creation did not allocate an id"))?;
    Ok(id)
}

pub async fn get_ingestion_job(mvcc: &MvccSubsystem, id: i64) -> Result<Option<HfIngestionJob>> {
    Ok(
        projection::get_ingestion(mvcc, mvcc.runtime.applied_version()?, id)?.map(|job| {
            HfIngestionJob {
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
            }
        }),
    )
}

pub(crate) async fn update_ingestion_state_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    update_ingestion_state_inner(storage, mvcc, id, state_value, error, guard).await
}

async fn update_ingestion_state_inner(
    _storage: &Storage,
    mvcc: &MvccSubsystem,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
    guard: HfWriteGuard,
) -> Result<()> {
    let Some(mut job) = projection::get_ingestion(mvcc, mvcc.runtime.applied_version()?, id)?
    else {
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
        mvcc,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(job),
        None,
        guard,
    )
    .await
    .map(|_| ())
}

pub(crate) async fn cancel_ingestion_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    cancel_ingestion_inner(storage, mvcc, id, guard).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn stage_cancel_ingestion_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    id: i64,
    transaction_id: &str,
    principal: &str,
    now_unix_ms: u64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    let Some(mut job) = projection::get_ingestion(mvcc, mvcc.runtime.applied_version()?, id)?
    else {
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
    stage_body(
        mvcc,
        transaction_id,
        principal,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(job),
        None,
        guard,
        uuid::Uuid::new_v4(),
        now_unix_ms,
    )
    .await?;
    Ok(1)
}

async fn cancel_ingestion_inner(
    _storage: &Storage,
    mvcc: &MvccSubsystem,
    id: i64,
    guard: HfWriteGuard,
) -> Result<u64> {
    let Some(mut job) = projection::get_ingestion(mvcc, mvcc.runtime.applied_version()?, id)?
    else {
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
        mvcc,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(job),
        None,
        guard,
    )
    .await?;
    Ok(1)
}

pub(crate) async fn add_item_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    ingestion_id: i64,
    path: &str,
    size: Option<i64>,
    etag: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<i64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    add_item_inner(storage, mvcc, ingestion_id, path, size, etag, guard).await
}

async fn add_item_inner(
    _storage: &Storage,
    mvcc: &MvccSubsystem,
    ingestion_id: i64,
    path: &str,
    size: Option<i64>,
    etag: Option<&str>,
    guard: HfWriteGuard,
) -> Result<i64> {
    let existing =
        projection::get_item_by_path(mvcc, mvcc.runtime.applied_version()?, ingestion_id, path)?;
    let id = existing.as_ref().map(|item| item.id).unwrap_or_default();
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
    let existing_id = item.id;
    let allocated_id = append_body(
        mvcc,
        HfMutationKind::ItemUpsert,
        None,
        None,
        None,
        Some(item),
        guard,
    )
    .await?;
    Ok(allocated_id.unwrap_or(existing_id))
}

pub(crate) async fn update_item_state_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    id: i64,
    state_value: crate::tasks::HFIngestionItemState,
    error: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    update_item_state_inner(storage, mvcc, id, state_value, error, guard).await
}

async fn update_item_state_inner(
    _storage: &Storage,
    mvcc: &MvccSubsystem,
    id: i64,
    state_value: crate::tasks::HFIngestionItemState,
    error: Option<&str>,
    guard: HfWriteGuard,
) -> Result<()> {
    let Some(mut item) = projection::get_item(mvcc, mvcc.runtime.applied_version()?, id)? else {
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
        mvcc,
        HfMutationKind::ItemUpsert,
        None,
        None,
        None,
        Some(item),
        guard,
    )
    .await
    .map(|_| ())
}

pub(crate) async fn update_item_success_with_permit(
    storage: &Storage,
    mvcc: &MvccSubsystem,
    id: i64,
    size: i64,
    etag: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    update_item_success_inner(storage, mvcc, id, size, etag, guard).await
}

async fn update_item_success_inner(
    _storage: &Storage,
    mvcc: &MvccSubsystem,
    id: i64,
    size: i64,
    etag: &str,
    guard: HfWriteGuard,
) -> Result<()> {
    let Some(mut item) = projection::get_item(mvcc, mvcc.runtime.applied_version()?, id)? else {
        return Ok(());
    };
    item.state = crate::tasks::HFIngestionItemState::Stored;
    item.size = Some(size);
    item.etag = Some(etag.to_string());
    item.finished_at = Some(Utc::now());
    append_body(
        mvcc,
        HfMutationKind::ItemUpsert,
        None,
        None,
        None,
        Some(item),
        guard,
    )
    .await
    .map(|_| ())
}

pub async fn list_stored_ingestion_item_page(
    mvcc: &MvccSubsystem,
    ingestion_id: i64,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfStoredItemPage> {
    projection::list_stored_items_for_ingestion(
        mvcc,
        mvcc.runtime.applied_version()?,
        ingestion_id,
        after_cursor,
        limit,
    )
}

pub async fn list_stored_target_item_page(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    bucket: &str,
    prefix: &str,
    after_cursor: Option<&[u8]>,
    limit: usize,
) -> Result<HfStoredItemPage> {
    projection::list_stored_items_for_target(
        mvcc,
        mvcc.runtime.applied_version()?,
        tenant_id,
        bucket,
        prefix,
        after_cursor,
        limit,
    )
}

pub async fn get_ingestion_status(mvcc: &MvccSubsystem, id: i64) -> Result<HfIngestionStatus> {
    projection::get_ingestion_status(mvcc, mvcc.runtime.applied_version()?, id)?
        .ok_or_else(|| anyhow!("ingestion not found"))
}

async fn append_body(
    mvcc: &MvccSubsystem,
    event: HfMutationKind,
    key: Option<HfKey>,
    key_delete: Option<(i64, i64, String)>,
    ingestion: Option<HfIngestion>,
    item: Option<HfIngestionItem>,
    guard: HfWriteGuard,
) -> Result<Option<i64>> {
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
    let principal = hf_partition_principal();
    let now_unix_ms = current_unix_ms();
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            &principal,
            &format!("hf-metadata:{key_text}:{mutation_id}"),
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            now_unix_ms,
        )
        .await?;
    let transaction_id = handle.transaction_id.as_str();
    let allocated_id = stage_body(
        mvcc,
        transaction_id,
        &principal,
        event,
        key,
        key_delete,
        ingestion,
        item,
        guard,
        mutation_id,
        now_unix_ms,
    )
    .await?;
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            transaction_id,
            &principal,
            current_unix_ms(),
        )
        .await?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(allocated_id),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
            Err(anyhow!("hf metadata transaction aborted: {reason:?}"))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn stage_body(
    mvcc: &MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    event: HfMutationKind,
    key: Option<HfKey>,
    key_delete: Option<(i64, i64, String)>,
    ingestion: Option<HfIngestion>,
    item: Option<HfIngestionItem>,
    guard: HfWriteGuard,
    mutation_id: uuid::Uuid,
    now_unix_ms: u64,
) -> Result<Option<i64>> {
    let assignment = mvcc
        .reconcile_work_assignment("hf-metadata", "global")
        .await?
        .ok_or_else(|| anyhow!("this node does not own the hf metadata assignment"))?;
    let head_key = hf_head_logical_key()?;
    let committed_head = mvcc.read_latest_value(&head_key)?;
    let observed_head = mvcc.read_transaction_value(transaction_id, principal, &head_key)?;
    let mut head = observed_head
        .as_deref()
        .map(decode_hf_head)
        .transpose()?
        .unwrap_or(HfJournalHeadProto {
            schema: HF_JOURNAL_HEAD_SCHEMA.to_string(),
            last_sequence: 0,
            next_entity_id: 1,
            last_event_hash: Vec::new(),
        });
    let allocated_id = assign_entity_id(event, &mut head, &key, &ingestion, &item)?;
    let key = key.map(|mut value| {
        if value.id == 0 {
            value.id = allocated_id.expect("create key allocates an id");
        }
        value
    });
    let ingestion = ingestion.map(|mut value| {
        if value.id == 0 {
            value.id = allocated_id.expect("create ingestion allocates an id");
        }
        value
    });
    let item = item.map(|mut value| {
        if value.id == 0 {
            value.id = allocated_id.expect("create item allocates an id");
        }
        value
    });
    let body = hf_body_from_parts(event, key, key_delete, ingestion, item, Utc::now())?;
    let payload = encode_hf_body(&body, guard.fence_token, mutation_id)?;
    let sequence = head
        .last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("hf journal sequence overflow"))?;
    let event_hash = hf_event_hash(
        sequence,
        &head.last_event_hash,
        mutation_id.as_bytes(),
        &payload,
    );
    let event_key = hf_event_logical_key(sequence)?;
    let event_payload = encode_deterministic_proto(&HfJournalEventProto {
        schema: HF_JOURNAL_EVENT_SCHEMA.to_string(),
        sequence,
        previous_event_hash: head.last_event_hash.clone(),
        event_hash: event_hash.to_vec(),
        mutation_id: mutation_id.to_string(),
        body: payload,
    })?;
    head.last_sequence = sequence;
    head.last_event_hash = event_hash.to_vec();
    let mut plan = projection::projection_plan(mvcc, transaction_id, principal, &body)?;
    plan.mutations.push(ProductMutation::put(
        head_key.clone(),
        encode_deterministic_proto(&head)?,
    ));
    plan.mutations
        .push(ProductMutation::put(event_key.clone(), event_payload));
    mvcc.stage_product_mutations(transaction_id, principal, plan.mutations, now_unix_ms)?;
    mvcc.stage_predicate(
        transaction_id,
        principal,
        head_key,
        value_predicate(committed_head.as_deref()),
        now_unix_ms,
    )?;
    mvcc.stage_predicate(
        transaction_id,
        principal,
        event_key,
        PredicateKind::Absent,
        now_unix_ms,
    )?;
    for (key, predicate) in plan.predicates {
        mvcc.stage_predicate(transaction_id, principal, key, predicate, now_unix_ms)?;
    }
    mvcc.stage_assignment_guard(transaction_id, principal, &assignment, now_unix_ms)?;
    Ok(allocated_id)
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

fn current_unix_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
}

fn hf_head_logical_key() -> Result<LogicalKey> {
    hf_logical_key(&core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf-journal"),
        CoreMetaTuplePart::Utf8("head"),
    ])?)
}

fn hf_event_logical_key(sequence: u64) -> Result<LogicalKey> {
    hf_logical_key(&core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("hf-journal"),
        CoreMetaTuplePart::Utf8("event"),
        CoreMetaTuplePart::U64(sequence),
    ])?)
}

fn hf_logical_key(tuple_key: &[u8]) -> Result<LogicalKey> {
    coremeta_logical_key(CF_OBSERVABILITY, TABLE_OBSERVABILITY_CURSOR_ROW, tuple_key)
}

fn decode_hf_head(payload: &[u8]) -> Result<HfJournalHeadProto> {
    let head = HfJournalHeadProto::decode(payload)?;
    ensure_deterministic_proto(&head, payload, "hf journal head")?;
    if head.schema != HF_JOURNAL_HEAD_SCHEMA || head.next_entity_id == 0 {
        return Err(anyhow!("hf journal head has invalid schema or entity id"));
    }
    if head.last_sequence == 0 && !head.last_event_hash.is_empty()
        || head.last_sequence != 0 && head.last_event_hash.len() != 32
    {
        return Err(anyhow!("hf journal head has invalid hash chain state"));
    }
    Ok(head)
}

fn value_predicate(value: Option<&[u8]>) -> PredicateKind {
    value
        .map(|payload| PredicateKind::ValueHash(*blake3::hash(payload).as_bytes()))
        .unwrap_or(PredicateKind::Absent)
}

fn assign_entity_id(
    event: HfMutationKind,
    head: &mut HfJournalHeadProto,
    key: &Option<HfKey>,
    ingestion: &Option<HfIngestion>,
    item: &Option<HfIngestionItem>,
) -> Result<Option<i64>> {
    let needs_id = match event {
        HfMutationKind::KeyUpsert => key.as_ref().is_some_and(|value| value.id == 0),
        HfMutationKind::IngestionUpsert => ingestion.as_ref().is_some_and(|value| value.id == 0),
        HfMutationKind::ItemUpsert => item.as_ref().is_some_and(|value| value.id == 0),
        HfMutationKind::KeyDelete => false,
    };
    if !needs_id {
        return Ok(None);
    }
    let id = i64::try_from(head.next_entity_id).map_err(|_| anyhow!("hf entity id exceeds i64"))?;
    head.next_entity_id = head
        .next_entity_id
        .checked_add(1)
        .ok_or_else(|| anyhow!("hf entity id overflow"))?;
    Ok(Some(id))
}

fn hf_event_hash(
    sequence: u64,
    previous_event_hash: &[u8],
    mutation_id: &[u8],
    payload: &[u8],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.hf.journal.event.v1\0");
    hasher.update(&sequence.to_be_bytes());
    hasher.update(previous_event_hash);
    hasher.update(mutation_id);
    hasher.update(payload);
    *hasher.finalize().as_bytes()
}

pub fn hf_partition_id() -> Hash32 {
    hash32(b"hf_metadata/global")
}

fn hf_partition_principal() -> String {
    "partition-owner:hf_metadata:global".to_string()
}

async fn hf_write_guard(
    _storage: &Storage,
    permit: &PartitionWritePermit,
    _partition_owner_signing_key: &[u8],
) -> Result<HfWriteGuard> {
    require_hf_permit(permit)?;
    Ok(HfWriteGuard {
        fence_token: permit.fence_token,
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
