use crate::core_store::{
    CF_MESH, CoreMetaTuplePart, CoreMutationOperation, TABLE_BUCKET_CURRENT_BY_ID_ROW,
    TABLE_BUCKET_CURRENT_BY_NAME_ROW, TABLE_BUCKET_EVENT_HEAD_ROW, TABLE_BUCKET_ID_ALLOCATOR_ROW,
    core_meta_root_key_hash, core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::persistence::{Bucket, BucketMetadataEvent};
use anyhow::{Context, Result, anyhow, bail};
use prost::Message;
use serde_json::{Value as JsonValue, json};
use std::time::Duration;

const BUCKET_CURRENT_ROW_SCHEMA: &str = "anvil.mvcc.bucket-current.v2";
const BUCKET_EVENT_HEAD_ROW_SCHEMA: &str = "anvil.mvcc.bucket-event-head.v2";
const BUCKET_ID_ALLOCATOR_ROW_SCHEMA: &str = "anvil.mvcc.bucket-id-allocator.v2";
const BUCKET_METADATA_BODY_SCHEMA: &str = "anvil.core.bucket_metadata.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketJournalMutation {
    Create,
    Update,
    Delete,
}

impl BucketJournalMutation {
    fn event_name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BucketJournalBody {
    event: String,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: String,
    region: String,
    is_public_read: bool,
    mutation_id: String,
    created_at: String,
    emitted_at: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct BucketJournalBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    event: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    bucket_name: String,
    #[prost(string, tag = "6")]
    region: String,
    #[prost(bool, tag = "7")]
    is_public_read: bool,
    #[prost(string, tag = "8")]
    mutation_id: String,
    #[prost(string, tag = "9")]
    created_at: String,
    #[prost(string, optional, tag = "10")]
    emitted_at: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct BucketCurrentRowProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(bool, tag = "2")]
    deleted: bool,
    #[prost(int64, tag = "3")]
    bucket_id: i64,
    #[prost(int64, tag = "4")]
    tenant_id: i64,
    #[prost(string, tag = "5")]
    bucket_name: String,
    #[prost(string, tag = "6")]
    region: String,
    #[prost(string, tag = "7")]
    created_at: String,
    #[prost(bool, tag = "8")]
    is_public_read: bool,
}

#[derive(Clone, PartialEq, Message)]
struct BucketIdAllocatorRowProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    max_allocated_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct BucketEventHeadRowProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    bucket_name: String,
    #[prost(uint64, tag = "4")]
    stream_sequence: u64,
    #[prost(bytes, tag = "5")]
    event_payload: Vec<u8>,
}

pub(crate) async fn stage_bucket_mutation_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<u64> {
    let (_, collection_revision) = build_bucket_mvcc_mutation_plan_with_transaction(
        mvcc,
        bucket,
        mutation,
        Some((transaction_id, transaction_principal)),
    )?
        .stage(
            mvcc,
            transaction_id,
            transaction_principal,
            u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
        )
        .await?;
    Ok(collection_revision)
}

pub fn next_bucket_id_mvcc(mvcc: &crate::mvcc_bootstrap::MvccSubsystem) -> Result<i64> {
    let key = bucket_mvcc_key(
        TABLE_BUCKET_ID_ALLOCATOR_ROW,
        &bucket_id_allocator_tuple_key()?,
    )?;
    mvcc.read_latest_value(&key)?
        .as_deref()
        .map(decode_bucket_id_allocator_payload)
        .transpose()?
        .unwrap_or_default()
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("bucket id overflow"))
}

pub(crate) fn next_bucket_id_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<i64> {
    let key = bucket_mvcc_key(
        TABLE_BUCKET_ID_ALLOCATOR_ROW,
        &bucket_id_allocator_tuple_key()?,
    )?;
    mvcc.read_transaction_value(transaction_id, transaction_principal, &key)?
        .as_deref()
        .map(decode_bucket_id_allocator_payload)
        .transpose()?
        .unwrap_or_default()
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("bucket id overflow"))
}

fn bucket_event_head_put(
    bucket: &Bucket,
    event_payload: &[u8],
    stream_sequence: u64,
    partition_id: &str,
) -> Result<CoreMutationOperation> {
    if stream_sequence == 0 || event_payload.is_empty() {
        return Err(anyhow!("bucket event head must reference a durable event"));
    }
    let payload = encode_deterministic_proto(&BucketEventHeadRowProto {
        schema: BUCKET_EVENT_HEAD_ROW_SCHEMA.to_string(),
        tenant_id: bucket.tenant_id,
        bucket_name: bucket.name.clone(),
        stream_sequence,
        event_payload: event_payload.to_vec(),
    })?;
    Ok(CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: CF_MESH.to_string(),
        table_id: TABLE_BUCKET_EVENT_HEAD_ROW,
        tuple_key: bucket_event_head_tuple_key(bucket.tenant_id, &bucket.name)?,
        payload,
    })
}

fn bucket_event_head_tuple_key(tenant_id: i64, bucket_name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(bucket_name),
    ])
}

fn bucket_event_prefix(tenant_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("bucket-event"),
        CoreMetaTuplePart::I64(tenant_id),
    ])
}

fn bucket_event_tuple_key(tenant_id: i64, sequence: u64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("bucket-event"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::U64(sequence),
    ])
}

fn bucket_event_sequence_from_tuple_key(tuple_key: &[u8]) -> Option<i64> {
    let suffix = tuple_key.get(tuple_key.len().checked_sub(9)?..)?;
    if suffix.first().copied()? != 0x02 {
        return None;
    }
    let sequence = u64::from_be_bytes(suffix.get(1..)?.try_into().ok()?);
    i64::try_from(sequence).ok()
}

pub async fn latest_bucket_metadata_event(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Option<BucketMetadataEvent>> {
    let tuple_key = bucket_event_head_tuple_key(tenant_id, bucket_name)?;
    let key = bucket_mvcc_key(TABLE_BUCKET_EVENT_HEAD_ROW, &tuple_key)?;
    let Some(payload) = mvcc.read_latest_value(&key)? else {
        return Ok(None);
    };
    let row = BucketEventHeadRowProto::decode(payload.as_slice())?;
    ensure_deterministic_proto(&row, &payload, "bucket event head row")?;
    if row.schema != BUCKET_EVENT_HEAD_ROW_SCHEMA
        || row.tenant_id != tenant_id
        || row.bucket_name != bucket_name
        || row.stream_sequence == 0
        || row.event_payload.is_empty()
    {
        return Err(anyhow!("bucket event head row scope mismatch"));
    }
    let body = decode_bucket_journal_body(&row.event_payload)?;
    if body.tenant_id != tenant_id || body.bucket_name != bucket_name {
        return Err(anyhow!("bucket event head payload scope mismatch"));
    }
    bucket_event_from_body(row.stream_sequence, body).map(Some)
}

#[derive(Debug, Clone)]
pub struct BucketMetadataEventPage {
    pub events: Vec<BucketMetadataEvent>,
    pub next_cursor: i64,
    pub has_more: bool,
}

pub async fn list_bucket_metadata_event_page(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_name: &str,
    after_cursor: i64,
    limit: usize,
) -> Result<BucketMetadataEventPage> {
    if after_cursor < 0 {
        return Err(anyhow!("bucket metadata watch cursor must be non-negative"));
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let prefix = bucket_event_prefix(tenant_id)?;
    let application_prefix = crate::mvcc_product::coremeta_application_prefix(CF_MESH, &prefix)?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_BUCKET_EVENT_HEAD_ROW,
        &application_prefix,
        snapshot,
    )?;
    rows.retain(|(key, _)| {
        crate::mvcc_product::coremeta_tuple_from_logical_key(key, CF_MESH)
            .ok()
            .and_then(bucket_event_sequence_from_tuple_key)
            .is_some_and(|sequence| sequence > after_cursor)
    });
    rows.truncate(limit.saturating_add(1));
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let mut next_cursor = after_cursor;
    let mut events = Vec::with_capacity(rows.len());
    for (key, row) in rows {
        let tuple_key = crate::mvcc_product::coremeta_tuple_from_logical_key(&key, CF_MESH)?;
        let sequence = bucket_event_sequence_from_tuple_key(tuple_key)
            .ok_or_else(|| anyhow!("bucket metadata event key is malformed"))?;
        next_cursor = sequence;
        let body = decode_bucket_journal_body(&row.value)
            .with_context(|| format!("decode bucket metadata event {sequence}"))?;
        if !bucket_name.is_empty() && body.bucket_name != bucket_name {
            continue;
        }
        events.push(bucket_event_from_body(u64::try_from(sequence)?, body)?);
    }
    Ok(BucketMetadataEventPage {
        events,
        next_cursor,
        has_more,
    })
}

#[derive(Debug, Clone)]
struct BucketCurrentRow {
    deleted: bool,
    bucket: Bucket,
}

#[derive(Debug, Clone)]
pub struct CurrentBucketPage {
    pub buckets: Vec<Bucket>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Debug)]
pub(crate) struct BucketMvccMutationPlan {
    pub mutations: Vec<crate::mvcc_product::ProductMutation>,
    pub predicates: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    pub outbox_events: Vec<crate::mvcc_outbox::StreamOutboxEvent>,
    pub allocated_bucket_id: i64,
    pub collection_revision: u64,
    assignment_identity: String,
}

impl BucketMvccMutationPlan {
    pub fn with_admin_audit(mut self, event: &crate::admin_audit::AdminAuditEvent) -> Result<Self> {
        let audit = crate::admin_audit::admin_audit_mvcc_plan(
            event,
            self.collection_revision,
            &event.audit_event_id,
        )?;
        self.mutations.extend(audit.mutations);
        self.outbox_events.extend(audit.outbox_events);
        Ok(self)
    }
    pub async fn stage(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<(i64, u64)> {
        mvcc.stage_product_mutations(transaction_id, principal, self.mutations, now_unix_ms)?;
        for event in self.outbox_events {
            mvcc.open_transactions
                .add_stream_event(transaction_id, event, now_unix_ms)?;
        }
        for (key, kind) in self.predicates {
            mvcc.stage_predicate(transaction_id, principal, key, kind, now_unix_ms)?;
        }
        stage_bucket_assignment_guard(
            mvcc,
            &self.assignment_identity,
            transaction_id,
            principal,
            now_unix_ms,
        )
        .await?;
        Ok((self.allocated_bucket_id, self.collection_revision))
    }

    pub async fn autocommit(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        principal: &str,
        idempotency_key: &str,
        _durability: crate::mvcc_transaction::DurabilityLevel,
        now_unix_ms: u64,
    ) -> Result<(i64, u64)> {
        let allocated_bucket_id = self.allocated_bucket_id;
        let collection_revision = self.collection_revision;
        let handle = mvcc
            .open_transactions
            .begin(
                mvcc.runtime.as_ref(),
                mvcc.cluster_id(),
                principal,
                idempotency_key,
                Duration::from_secs(30),
                crate::mvcc_transaction::DurabilityLevel::Quorum,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                now_unix_ms,
            )
            .await?;
        let status =
            mvcc.open_transactions
                .status(&handle.transaction_id, principal, now_unix_ms)?;
        if status.state == "open" {
            mvcc.stage_product_mutations(
                &handle.transaction_id,
                principal,
                self.mutations,
                now_unix_ms,
            )?;
            for (key, kind) in self.predicates {
                mvcc.stage_predicate(&handle.transaction_id, principal, key, kind, now_unix_ms)?;
            }
            for event in self.outbox_events {
                mvcc.open_transactions.add_stream_event(
                    &handle.transaction_id,
                    event,
                    now_unix_ms,
                )?;
            }
            stage_bucket_assignment_guard(
                mvcc,
                &self.assignment_identity,
                &handle.transaction_id,
                principal,
                now_unix_ms,
            )
            .await?;
        }
        let outcome = mvcc
            .open_transactions
            .commit(
                mvcc.runtime.as_ref(),
                &handle.transaction_id,
                principal,
                now_unix_ms,
            )
            .await?;
        if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
            outcome.certification
        {
            bail!("bucket MVCC transaction aborted: {reason:?}");
        }
        Ok((allocated_bucket_id, collection_revision))
    }
}

async fn stage_bucket_assignment_guard(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    assignment_identity: &str,
    transaction_id: &str,
    principal: &str,
    now_unix_ms: u64,
) -> Result<()> {
    let assignment = mvcc
        .reconcile_work_assignment("bucket-metadata", assignment_identity)
        .await?
        .ok_or_else(|| anyhow!("this node does not own the bucket metadata assignment"))?;
    mvcc.stage_assignment_guard(transaction_id, principal, &assignment, now_unix_ms)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct BucketCollectionRevisionValue {
    schema: String,
    tenant_id: i64,
    revision: u64,
}

const BUCKET_COLLECTION_REVISION_SCHEMA: &str = "anvil.mvcc.bucket.collection-revision.v1";

fn bucket_mvcc_key(table_id: u16, tuple_key: &[u8]) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(CF_MESH, table_id, tuple_key)
}

fn bucket_collection_revision_tuple_key(tenant_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("bucket-collection-revision"),
        CoreMetaTuplePart::I64(tenant_id),
    ])
}

pub fn read_current_bucket_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Option<Bucket>> {
    let key = bucket_mvcc_key(
        TABLE_BUCKET_CURRENT_BY_NAME_ROW,
        &tenant_bucket_name_current_tuple_key(tenant_id, bucket_name)?,
    )?;
    let Some(payload) = mvcc.read_latest_value(&key)? else {
        return Ok(None);
    };
    let current = decode_bucket_current_row(&payload)?;
    ensure_bucket_tenant_name_matches(&current.bucket, tenant_id, bucket_name)?;
    Ok(current.into_active_bucket())
}

pub(crate) fn read_current_bucket_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_name: &str,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<Bucket>> {
    let key = bucket_mvcc_key(
        TABLE_BUCKET_CURRENT_BY_NAME_ROW,
        &tenant_bucket_name_current_tuple_key(tenant_id, bucket_name)?,
    )?;
    let Some(payload) =
        mvcc.read_transaction_value(transaction_id, transaction_principal, &key)?
    else {
        return Ok(None);
    };
    let current = decode_bucket_current_row(&payload)?;
    ensure_bucket_tenant_name_matches(&current.bucket, tenant_id, bucket_name)?;
    Ok(current.into_active_bucket())
}

pub(crate) fn read_current_bucket_at_mvcc_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    bucket_name: &str,
    snapshot: u64,
) -> Result<Option<Bucket>> {
    let key = bucket_mvcc_key(
        TABLE_BUCKET_CURRENT_BY_NAME_ROW,
        &tenant_bucket_name_current_tuple_key(tenant_id, bucket_name)?,
    )?;
    let Some(row) = mvcc.runtime.read_at(&key, snapshot)? else {
        return Ok(None);
    };
    let current = decode_bucket_current_row(&row.value)?;
    ensure_bucket_tenant_name_matches(&current.bucket, tenant_id, bucket_name)?;
    Ok(current.into_active_bucket())
}

pub(crate) fn read_current_bucket_by_id_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket_id: i64,
) -> Result<Option<Bucket>> {
    let key = bucket_mvcc_key(
        TABLE_BUCKET_CURRENT_BY_ID_ROW,
        &global_bucket_id_current_tuple_key(bucket_id)?,
    )?;
    let Some(payload) = mvcc.read_latest_value(&key)? else {
        return Ok(None);
    };
    let current = decode_bucket_current_row(&payload)?;
    if current.bucket.id != bucket_id {
        bail!("bucket current id row scope mismatch");
    }
    Ok(current.into_active_bucket())
}

pub(crate) fn page_current_buckets_at_mvcc_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    snapshot: u64,
    after_application_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentBucketPage> {
    if !(1..=1_000).contains(&page_size) {
        bail!("bucket page size must be between 1 and 1000");
    }
    let tuple_prefix = tenant_bucket_name_current_tuple_prefix(tenant_id)?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_MESH, &tuple_prefix)?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_BUCKET_CURRENT_BY_NAME_ROW,
        &application_prefix,
        snapshot,
    )?;
    if let Some(after) = after_application_key {
        rows.retain(|(key, _)| key.application_key.as_slice() > after);
    }
    let has_more = rows.len() > page_size;
    rows.truncate(page_size);
    let next_tuple_key = has_more
        .then(|| rows.last().map(|(key, _)| key.application_key.clone()))
        .flatten();
    let mut buckets = Vec::with_capacity(rows.len());
    for (_, row) in rows {
        let current = decode_bucket_current_row(&row.value)?;
        ensure_bucket_scope_matches(BucketJournalScope::Tenant(tenant_id), &current.bucket)?;
        if current.deleted {
            bail!("tenant bucket current table contains a deleted row");
        }
        buckets.push(current.bucket);
    }
    Ok(CurrentBucketPage {
        buckets,
        next_tuple_key,
    })
}

pub(crate) fn read_bucket_collection_revision_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
) -> Result<u64> {
    let key = bucket_mvcc_key(
        TABLE_BUCKET_EVENT_HEAD_ROW,
        &bucket_collection_revision_tuple_key(tenant_id)?,
    )?;
    let Some(payload) = mvcc.read_latest_value(&key)? else {
        return Ok(0);
    };
    let value: BucketCollectionRevisionValue = serde_json::from_slice(&payload)?;
    if value.schema != BUCKET_COLLECTION_REVISION_SCHEMA || value.tenant_id != tenant_id {
        bail!("bucket collection revision row scope mismatch");
    }
    Ok(value.revision)
}

pub fn current_bucket_collection_revision_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
) -> Result<String> {
    Ok(read_bucket_collection_revision_mvcc(mvcc, tenant_id)?.to_string())
}

pub fn page_current_buckets_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    expected_revision: &str,
    after_application_key: Option<&[u8]>,
    page_size: usize,
) -> Result<CurrentBucketPage> {
    if current_bucket_collection_revision_mvcc(mvcc, tenant_id)? != expected_revision {
        bail!("bucket collection revision changed");
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let page = page_current_buckets_at_mvcc_snapshot(
        mvcc,
        tenant_id,
        snapshot,
        after_application_key,
        page_size,
    )?;
    if current_bucket_collection_revision_mvcc(mvcc, tenant_id)? != expected_revision {
        bail!("bucket collection revision changed");
    }
    Ok(page)
}

pub(crate) fn build_bucket_mvcc_mutation_plan(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
) -> Result<BucketMvccMutationPlan> {
    build_bucket_mvcc_mutation_plan_with_transaction(mvcc, bucket, mutation, None)
}

fn build_bucket_mvcc_mutation_plan_with_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    transaction: Option<(&str, &str)>,
) -> Result<BucketMvccMutationPlan> {
    use crate::mvcc_transaction::PredicateKind;

    let allocator_key = bucket_mvcc_key(
        TABLE_BUCKET_ID_ALLOCATOR_ROW,
        &bucket_id_allocator_tuple_key()?,
    )?;
    let allocator_payload = match transaction {
        Some((transaction_id, principal)) => {
            mvcc.read_transaction_value(transaction_id, principal, &allocator_key)?
        }
        None => mvcc.read_latest_value(&allocator_key)?,
    };
    let allocator_max = match allocator_payload.as_deref() {
        Some(payload) => decode_bucket_id_allocator_payload(payload)?,
        None => 0,
    };
    let allocated_bucket_id = if bucket.id > 0 {
        bucket.id
    } else {
        allocator_max
            .checked_add(1)
            .ok_or_else(|| anyhow!("bucket id overflow"))?
    };
    let collection_revision_key = bucket_mvcc_key(
        TABLE_BUCKET_EVENT_HEAD_ROW,
        &bucket_collection_revision_tuple_key(bucket.tenant_id)?,
    )?;
    let revision_payload = match transaction {
        Some((transaction_id, principal)) => {
            mvcc.read_transaction_value(transaction_id, principal, &collection_revision_key)?
        }
        None => mvcc.read_latest_value(&collection_revision_key)?,
    };
    let revision = match revision_payload.as_deref() {
        Some(payload) => {
            let value: BucketCollectionRevisionValue = serde_json::from_slice(payload)?;
            if value.schema != BUCKET_COLLECTION_REVISION_SCHEMA
                || value.tenant_id != bucket.tenant_id
            {
                bail!("bucket collection revision row scope mismatch");
            }
            value.revision
        }
        None => 0,
    };
    let collection_revision = revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("bucket collection revision overflow"))?;
    let mutation_id = uuid::Uuid::new_v4();
    let mutation_id_string = mutation_id.to_string();
    let partition_id = hex::encode(BucketJournalScope::Global.partition_id());
    let mut projected_bucket = bucket.clone();
    projected_bucket.id = allocated_bucket_id;
    let body = BucketJournalBody {
        event: mutation.event_name().to_string(),
        tenant_id: projected_bucket.tenant_id,
        bucket_id: projected_bucket.id,
        bucket_name: projected_bucket.name.clone(),
        region: projected_bucket.region.clone(),
        is_public_read: projected_bucket.is_public_read,
        mutation_id: mutation_id_string.clone(),
        created_at: projected_bucket.created_at.to_rfc3339(),
        emitted_at: Some(chrono::Utc::now().to_rfc3339()),
    };
    let event_payload = encode_bucket_journal_body(&body)?;
    let mut operations = Vec::new();
    operations.extend(bucket_current_coremeta_operations(
        BucketJournalScope::Tenant(projected_bucket.tenant_id),
        &projected_bucket,
        mutation,
        &partition_id,
    )?);
    operations.extend(bucket_current_coremeta_operations(
        BucketJournalScope::Global,
        &projected_bucket,
        mutation,
        &partition_id,
    )?);
    operations.push(bucket_event_head_put(
        &projected_bucket,
        &event_payload,
        collection_revision,
        &partition_id,
    )?);
    if mutation == BucketJournalMutation::Create && allocated_bucket_id > allocator_max {
        operations.push(bucket_id_allocator_put(allocated_bucket_id, &partition_id)?);
    }
    operations.push(CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.clone(),
        cf: CF_MESH.to_string(),
        table_id: TABLE_BUCKET_EVENT_HEAD_ROW,
        tuple_key: bucket_collection_revision_tuple_key(projected_bucket.tenant_id)?,
        payload: serde_json::to_vec(&BucketCollectionRevisionValue {
            schema: BUCKET_COLLECTION_REVISION_SCHEMA.to_string(),
            tenant_id: projected_bucket.tenant_id,
            revision: collection_revision,
        })?,
    });
    operations.push(CoreMutationOperation::CoreMetaPut {
        partition_id,
        cf: CF_MESH.to_string(),
        table_id: TABLE_BUCKET_EVENT_HEAD_ROW,
        tuple_key: bucket_event_tuple_key(projected_bucket.tenant_id, collection_revision)?,
        payload: event_payload,
    });

    let mut predicate_keys = std::collections::BTreeSet::new();
    let mut predicates = Vec::new();
    for operation in &operations {
        let (cf, table_id, tuple_key) = match operation {
            CoreMutationOperation::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                ..
            }
            | CoreMutationOperation::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } => (cf, *table_id, tuple_key),
            CoreMutationOperation::StreamAppend { .. } => {
                unreachable!("bucket MVCC plans never contain physical stream appends")
            }
        };
        let key = crate::mvcc_product::coremeta_logical_key(cf, table_id, tuple_key)?;
        if !predicate_keys.insert(key.clone()) {
            continue;
        }
        if let Some((transaction_id, principal)) = transaction
            && mvcc
                .open_transactions
                .staged_value(transaction_id, principal, &key)?
                .is_some()
        {
            // The first write to this key already captured its snapshot
            // observation and any stronger explicit predicate. A later bucket
            // operation in the same transaction must validate against that
            // transaction overlay without replacing the original observation.
            continue;
        }
        let kind = if mutation == BucketJournalMutation::Create
            && matches!(
                table_id,
                TABLE_BUCKET_CURRENT_BY_NAME_ROW | TABLE_BUCKET_CURRENT_BY_ID_ROW
            ) {
            PredicateKind::Absent
        } else {
            let visible = match transaction {
                Some((transaction_id, principal)) => {
                    mvcc.read_transaction_value(transaction_id, principal, &key)?
                }
                None => mvcc.read_latest_value(&key)?,
            };
            match visible {
                Some(payload) => PredicateKind::ValueHash(*blake3::hash(&payload).as_bytes()),
                None if mutation != BucketJournalMutation::Create
                    && matches!(
                        table_id,
                        TABLE_BUCKET_CURRENT_BY_NAME_ROW | TABLE_BUCKET_CURRENT_BY_ID_ROW
                    ) =>
                {
                    PredicateKind::Exists
                }
                None => PredicateKind::Absent,
            }
        };
        predicates.push((key, kind));
    }
    let plan = crate::mvcc_product::product_mutations_and_outbox_from_operations(operations)?;
    Ok(BucketMvccMutationPlan {
        mutations: plan.mutations,
        predicates,
        outbox_events: plan.outbox_events,
        allocated_bucket_id,
        collection_revision,
        assignment_identity: projected_bucket.tenant_id.to_string(),
    })
}

fn decode_bucket_id_allocator_payload(payload: &[u8]) -> Result<i64> {
    let row = BucketIdAllocatorRowProto::decode(payload)?;
    ensure_deterministic_proto(&row, payload, "bucket id allocator row")?;
    if row.schema != BUCKET_ID_ALLOCATOR_ROW_SCHEMA || row.max_allocated_id < 0 {
        bail!("bucket id allocator row is invalid");
    }
    Ok(row.max_allocated_id)
}

impl BucketCurrentRow {
    fn into_active_bucket(self) -> Option<Bucket> {
        if self.deleted {
            None
        } else {
            Some(self.bucket)
        }
    }
}

fn bucket_id_allocator_put(
    max_allocated_id: i64,
    partition_id: &str,
) -> Result<CoreMutationOperation> {
    if max_allocated_id <= 0 {
        return Err(anyhow!("bucket id allocator must be positive"));
    }
    let payload = encode_deterministic_proto(&BucketIdAllocatorRowProto {
        schema: BUCKET_ID_ALLOCATOR_ROW_SCHEMA.to_string(),
        max_allocated_id,
    })?;
    Ok(CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: CF_MESH.to_string(),
        table_id: TABLE_BUCKET_ID_ALLOCATOR_ROW,
        tuple_key: bucket_id_allocator_tuple_key()?,
        payload,
    })
}

fn bucket_id_allocator_tuple_key() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("bucket-id-allocator")])
}

fn bucket_current_coremeta_operations(
    scope: BucketJournalScope,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    operation_partition_id: &str,
) -> Result<Vec<CoreMutationOperation>> {
    let operations = match scope {
        BucketJournalScope::Tenant(tenant_id) if mutation == BucketJournalMutation::Delete => {
            vec![CoreMutationOperation::CoreMetaDelete {
                partition_id: operation_partition_id.to_string(),
                cf: CF_MESH.to_string(),
                table_id: TABLE_BUCKET_CURRENT_BY_NAME_ROW,
                tuple_key: tenant_bucket_name_current_tuple_key(tenant_id, &bucket.name)?,
            }]
        }
        BucketJournalScope::Tenant(tenant_id) => vec![CoreMutationOperation::CoreMetaPut {
            partition_id: operation_partition_id.to_string(),
            cf: CF_MESH.to_string(),
            table_id: TABLE_BUCKET_CURRENT_BY_NAME_ROW,
            tuple_key: tenant_bucket_name_current_tuple_key(tenant_id, &bucket.name)?,
            payload: encode_bucket_current_row(bucket, false)?,
        }],
        BucketJournalScope::Global => vec![CoreMutationOperation::CoreMetaPut {
            partition_id: operation_partition_id.to_string(),
            cf: CF_MESH.to_string(),
            table_id: TABLE_BUCKET_CURRENT_BY_ID_ROW,
            tuple_key: global_bucket_id_current_tuple_key(bucket.id)?,
            payload: encode_bucket_current_row(bucket, mutation == BucketJournalMutation::Delete)?,
        }],
    };
    Ok(operations)
}

fn encode_bucket_current_row(bucket: &Bucket, deleted: bool) -> Result<Vec<u8>> {
    let row = BucketCurrentRowProto {
        schema: BUCKET_CURRENT_ROW_SCHEMA.to_string(),
        deleted,
        bucket_id: bucket.id,
        tenant_id: bucket.tenant_id,
        bucket_name: bucket.name.clone(),
        region: bucket.region.clone(),
        created_at: bucket.created_at.to_rfc3339(),
        is_public_read: bucket.is_public_read,
    };
    encode_deterministic_proto(&row)
}

fn decode_bucket_current_row(bytes: &[u8]) -> Result<BucketCurrentRow> {
    let row = BucketCurrentRowProto::decode(bytes)?;
    ensure_deterministic_proto(&row, bytes, "bucket current row")?;
    if row.schema != BUCKET_CURRENT_ROW_SCHEMA {
        return Err(anyhow!("MVCC bucket current row has invalid schema"));
    }
    let bucket = Bucket {
        id: row.bucket_id,
        tenant_id: row.tenant_id,
        name: row.bucket_name,
        region: row.region,
        created_at: chrono::DateTime::parse_from_rfc3339(&row.created_at)?
            .with_timezone(&chrono::Utc),
        is_public_read: row.is_public_read,
    };
    Ok(BucketCurrentRow {
        deleted: row.deleted,
        bucket,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketJournalScope {
    Tenant(i64),
    Global,
}

impl BucketJournalScope {
    fn partition_id(self) -> Hash32 {
        match self {
            Self::Tenant(tenant_id) => tenant_bucket_partition_id(tenant_id),
            Self::Global => global_bucket_partition_id(),
        }
    }

    fn root_anchor_key(self) -> String {
        match self {
            Self::Tenant(tenant_id) => format!("bucket-current/tenant/{tenant_id}"),
            Self::Global => "bucket-current/global".to_string(),
        }
    }

    fn root_key_hash(self) -> String {
        core_meta_root_key_hash(&self.root_anchor_key())
    }
}

pub(crate) fn tenant_bucket_root_key_hash(tenant_id: i64) -> String {
    BucketJournalScope::Tenant(tenant_id).root_key_hash()
}

fn ensure_bucket_tenant_name_matches(
    bucket: &Bucket,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<()> {
    if bucket.tenant_id != tenant_id || bucket.name != bucket_name {
        return Err(anyhow!(
            "MVCC bucket current tenant/name row scope mismatch"
        ));
    }
    Ok(())
}

fn ensure_bucket_scope_matches(scope: BucketJournalScope, bucket: &Bucket) -> Result<()> {
    if let BucketJournalScope::Tenant(tenant_id) = scope {
        if bucket.tenant_id != tenant_id {
            return Err(anyhow!("MVCC bucket current list row scope mismatch"));
        }
    }
    Ok(())
}

pub fn tenant_bucket_partition_id(tenant_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket_metadata").as_bytes())
}

pub fn global_bucket_partition_id() -> Hash32 {
    hash32(b"bucket_metadata/global")
}

fn tenant_bucket_name_current_tuple_key(tenant_id: i64, bucket_name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(bucket_name),
    ])
}

fn tenant_bucket_name_current_tuple_prefix(tenant_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::I64(tenant_id)])
}

fn global_bucket_id_current_tuple_key(bucket_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::I64(bucket_id)])
}

fn bucket_event_from_body(sequence: u64, body: BucketJournalBody) -> Result<BucketMetadataEvent> {
    let id = i64::try_from(sequence).context("bucket metadata cursor exceeds i64")?;
    let bucket_created_at =
        chrono::DateTime::parse_from_rfc3339(&body.created_at)?.with_timezone(&chrono::Utc);
    let event_created_at = body
        .emitted_at
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()?
        .map(|value| value.with_timezone(&chrono::Utc))
        .unwrap_or(bucket_created_at);
    let deleted = body.event == "delete";
    Ok(BucketMetadataEvent {
        id,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        event_type: bucket_event_type(&body.event).to_string(),
        mutation_id: uuid::Uuid::parse_str(&body.mutation_id)?,
        bucket_metadata: bucket_metadata_json(&body, deleted),
        created_at: event_created_at,
    })
}

fn bucket_event_type(event: &str) -> &str {
    match event {
        "update" => "policy_update",
        other => other,
    }
}

fn bucket_metadata_json(body: &BucketJournalBody, deleted: bool) -> JsonValue {
    json!({
        "bucket_id": body.bucket_id,
        "name": body.bucket_name,
        "creation_date": body.created_at,
        "region": body.region,
        "is_public_read": body.is_public_read,
        "deleted": deleted,
    })
}

fn encode_bucket_journal_body(body: &BucketJournalBody) -> Result<Vec<u8>> {
    let proto = BucketJournalBodyProto {
        schema: BUCKET_METADATA_BODY_SCHEMA.to_string(),
        event: body.event.clone(),
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        region: body.region.clone(),
        is_public_read: body.is_public_read,
        mutation_id: body.mutation_id.clone(),
        created_at: body.created_at.clone(),
        emitted_at: body.emitted_at.clone(),
    };
    encode_deterministic_proto(&proto)
}

fn decode_bucket_journal_body(bytes: &[u8]) -> Result<BucketJournalBody> {
    let proto = BucketJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "bucket metadata body")?;
    if proto.schema != BUCKET_METADATA_BODY_SCHEMA {
        return Err(anyhow!("bucket metadata body has invalid schema"));
    }
    uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("bucket metadata body has invalid mutation id"))?;
    Ok(BucketJournalBody {
        event: proto.event,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        bucket_name: proto.bucket_name,
        region: proto.region,
        is_public_read: proto.is_public_read,
        mutation_id: proto.mutation_id,
        created_at: proto.created_at,
        emitted_at: proto.emitted_at,
    })
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    if encode_deterministic_proto(message)? != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
}
