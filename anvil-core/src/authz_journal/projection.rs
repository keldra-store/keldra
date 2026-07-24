use super::{
    AuthzTupleFilter, AuthzTupleRecordProto, authz_record_from_proto, authz_record_to_proto,
    ensure_deterministic_proto,
};
use crate::authz_coremeta_payload::{decode_authz_payload_row, encode_authz_payload_row};
use crate::authz_head;
use crate::authz_scope::{DEFAULT_AUTHZ_REALM_ID, split_realm_namespace};
use crate::core_store::{
    CF_AUTHZ, CoreMetaTuplePart, CoreMutationOperation, CoreStore,
    TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW, TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW,
    core_meta_committed_row_common, core_meta_record_tuple_key, core_meta_root_key_hash,
    core_meta_tuple_key,
};
use crate::persistence::AuthzTupleRecord;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use prost::Message;
use std::collections::BTreeMap;

const AUTHZ_TUPLE_CURRENT_ROW_SCHEMA: &str = "anvil.authz.tuple_current_row.v1";
const AUTHZ_TUPLE_CURRENT_PAYLOAD_KIND: &str = "authz_tuple_current";

#[derive(Clone, PartialEq, Message)]
struct AuthzTupleCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    record: Option<AuthzTupleRecordProto>,
}

/// A single public page may inspect at most this many ordered projection rows.
/// A sparse filter can therefore return a partial page with a continuation
/// instead of turning one request into an unbounded tenant scan.
pub(crate) const MAX_AUTHZ_PAGE_CANDIDATES: usize = 16_384;
const AUTHZ_PAGE_CANDIDATE_MULTIPLIER: usize = 16;
const AUTHZ_SOURCE_SCAN_CHUNK_ROWS: usize = 4_096;

/// Maximum number of tuples attached to one object relation that a foreground
/// Zanzibar traversal may expand. Exceeding this limit fails closed rather
/// than silently returning a partial authorization decision.
pub(super) const MAX_AUTHZ_RELATION_ROWS: usize = 1_024;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum AuthzProjectionPageError {
    #[error(
        "AuthzRevisionUnavailable: current authorization revision is {actual}, requested {expected}"
    )]
    RevisionMismatch { expected: i64, actual: i64 },
    #[error("authz projection page size must be between 1 and 1000")]
    InvalidPageSize,
    #[error("authz projection read failed: {0}")]
    Internal(String),
}

impl From<anyhow::Error> for AuthzProjectionPageError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(format!("{error:#}"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AuthzTupleProjectionPage {
    pub records: Vec<AuthzTupleRecord>,
    pub next_tuple_key: Option<Vec<u8>>,
    pub candidates_visited: usize,
}

#[derive(Debug, Clone)]
pub(super) struct AuthzObjectCandidatePage {
    pub object_ids: Vec<String>,
    pub next_object_id: Option<String>,
    pub candidates_visited: usize,
}

#[derive(Debug, Clone)]
pub(super) struct AuthzRelationRows {
    pub records: Vec<AuthzTupleRecord>,
    pub candidates_visited: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProjectionOrder {
    Object,
    Subject,
}

pub(super) async fn current_operations(
    storage: &Storage,
    records: &[AuthzTupleRecord],
    partition_id: &str,
    transaction_id: &str,
) -> Result<Vec<CoreMutationOperation>> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    // A batch may touch one tuple more than once. Only its final state belongs
    // in the active projections, while the journal retains every operation.
    let mut current_records = BTreeMap::new();
    for record in records {
        current_records.insert(object_row_key(record)?, record);
    }

    let mut operations = Vec::with_capacity(current_records.len() * 2);
    for (object_key, record) in current_records {
        let subject_key = subject_row_key(record)?;
        match record.operation.as_str() {
            "add" => {
                let payload = encode_current_payload(storage, record, transaction_id).await?;
                operations.push(current_put(
                    partition_id,
                    TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
                    object_key,
                    payload.clone(),
                ));
                operations.push(current_put(
                    partition_id,
                    TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW,
                    subject_key,
                    payload,
                ));
            }
            "remove" => {
                operations.push(current_delete(
                    partition_id,
                    TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
                    object_key,
                ));
                operations.push(current_delete(
                    partition_id,
                    TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW,
                    subject_key,
                ));
            }
            operation => return Err(anyhow!("unsupported authz tuple operation {operation}")),
        }
    }
    Ok(operations)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn read_current_record(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<Option<AuthzTupleRecord>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_current_record_with_core_store(
        storage,
        &core_store,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn read_current_record_with_core_store(
    storage: &Storage,
    core_store: &CoreStore,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<Option<AuthzTupleRecord>> {
    let (realm_id, local_namespace) = namespace_parts(namespace);
    let tuple_key = core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(&realm_id),
        CoreMetaTuplePart::Utf8(&local_namespace),
        CoreMetaTuplePart::Utf8(object_id),
        CoreMetaTuplePart::Utf8(relation),
        CoreMetaTuplePart::Utf8(subject_kind),
        CoreMetaTuplePart::Utf8(subject_id),
        CoreMetaTuplePart::Utf8(caveat_hash),
    ])?;
    let Some(payload) =
        core_store.read_coremeta_row(CF_AUTHZ, TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW, &tuple_key)?
    else {
        return Ok(None);
    };
    let record = decode_current_payload(storage, tenant_id, &payload).await?;
    validate_projection_row_key(ProjectionOrder::Object, &tuple_key, &record)?;
    Ok(Some(record))
}

pub(super) async fn read_current_relation_rows(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: Option<&str>,
) -> Result<AuthzRelationRows> {
    let (realm_id, local_namespace) = namespace_parts(namespace);
    let mut parts = vec![
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(&realm_id),
        CoreMetaTuplePart::Utf8(&local_namespace),
        CoreMetaTuplePart::Utf8(object_id),
        CoreMetaTuplePart::Utf8(relation),
    ];
    if let Some(subject_kind) = subject_kind {
        parts.push(CoreMetaTuplePart::Utf8(subject_kind));
    }
    let prefix = core_meta_tuple_key(&parts)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let rows = core_store.scan_coremeta_prefix_page(
        CF_AUTHZ,
        TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
        &prefix,
        None,
        MAX_AUTHZ_RELATION_ROWS + 1,
    )?;
    if rows.len() > MAX_AUTHZ_RELATION_ROWS {
        return Err(anyhow!(
            "AuthzGraphBreadthExceeded: relation contains more than {MAX_AUTHZ_RELATION_ROWS} tuples"
        ));
    }

    let candidates_visited = rows.len();
    let mut records = Vec::with_capacity(candidates_visited);
    for row in rows {
        let tuple_key = core_meta_record_tuple_key(&row.key)
            .context("decode authz relation projection tuple key")?;
        let record = decode_current_payload(storage, tenant_id, &row.payload).await?;
        validate_projection_row_key(ProjectionOrder::Object, tuple_key, &record)?;
        if record.namespace != namespace
            || record.object_id != object_id
            || record.relation != relation
            || subject_kind.is_some_and(|kind| record.subject_kind != kind)
        {
            return Err(anyhow!("authz relation projection scope mismatch"));
        }
        records.push(record);
    }
    Ok(AuthzRelationRows {
        records,
        candidates_visited,
    })
}

pub(super) async fn page_current_records(
    storage: &Storage,
    tenant_id: i64,
    filter: &AuthzTupleFilter,
    expected_revision: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> std::result::Result<AuthzTupleProjectionPage, AuthzProjectionPageError> {
    if !(1..=1000).contains(&page_size) {
        return Err(AuthzProjectionPageError::InvalidPageSize);
    }
    require_revision(storage, tenant_id, expected_revision).await?;

    let order = projection_order(filter);
    let (table_id, prefix) = match order {
        ProjectionOrder::Object => (
            TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
            object_filter_prefix(tenant_id, filter)?,
        ),
        ProjectionOrder::Subject => (
            TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW,
            subject_filter_prefix(tenant_id, filter)?,
        ),
    };
    let candidate_budget = candidate_budget(page_size);
    let core_store = CoreStore::new(storage.clone())
        .await
        .map_err(AuthzProjectionPageError::from)?;
    let mut matches = Vec::with_capacity(page_size.saturating_add(1));
    let mut scan_after = after_tuple_key.map(ToOwned::to_owned);
    let mut candidates_visited = 0;
    let mut source_exhausted = false;
    while candidates_visited < candidate_budget && matches.len() <= page_size {
        let chunk_limit = (candidate_budget - candidates_visited).min(AUTHZ_SOURCE_SCAN_CHUNK_ROWS);
        let rows = core_store
            .scan_coremeta_prefix_page(
                CF_AUTHZ,
                table_id,
                &prefix,
                scan_after.as_deref(),
                chunk_limit,
            )
            .map_err(AuthzProjectionPageError::from)?;
        if rows.is_empty() {
            source_exhausted = true;
            break;
        }
        let row_count = rows.len();
        for row in rows {
            candidates_visited += 1;
            let tuple_key = core_meta_record_tuple_key(&row.key)
                .context("decode authz projection tuple key")?
                .to_vec();
            scan_after = Some(tuple_key.clone());
            let record = decode_current_payload(storage, tenant_id, &row.payload)
                .await
                .map_err(AuthzProjectionPageError::from)?;
            validate_projection_row_key(order, &tuple_key, &record)
                .map_err(AuthzProjectionPageError::from)?;
            if matches_filter(&record, filter) {
                matches.push((tuple_key, record));
            }
            if matches.len() > page_size {
                break;
            }
        }
        if matches.len() > page_size {
            break;
        }
        if row_count < chunk_limit {
            source_exhausted = true;
            break;
        }
    }

    let has_more_matches = matches.len() > page_size;
    if has_more_matches {
        matches.truncate(page_size);
    }
    let next_tuple_key = if has_more_matches {
        matches.last().map(|(key, _)| key.clone())
    } else if candidates_visited == candidate_budget && !source_exhausted {
        scan_after
    } else {
        None
    };
    let records = matches.into_iter().map(|(_, record)| record).collect();

    require_revision(storage, tenant_id, expected_revision).await?;
    Ok(AuthzTupleProjectionPage {
        records,
        next_tuple_key,
        candidates_visited,
    })
}

pub(super) async fn page_current_object_candidates(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    expected_revision: i64,
    after_object_id: Option<&str>,
    page_size: usize,
) -> std::result::Result<AuthzObjectCandidatePage, AuthzProjectionPageError> {
    if !(1..=1000).contains(&page_size) {
        return Err(AuthzProjectionPageError::InvalidPageSize);
    }
    require_revision(storage, tenant_id, expected_revision).await?;

    let (realm_id, local_namespace) = namespace_parts(namespace);
    let prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(&realm_id),
        CoreMetaTuplePart::Utf8(&local_namespace),
    ])
    .map_err(AuthzProjectionPageError::from)?;
    let after_tuple_key = after_object_id
        .map(|object_id| {
            object_projection_upper_bound(tenant_id, &realm_id, &local_namespace, object_id)
        })
        .transpose()
        .map_err(AuthzProjectionPageError::from)?;
    let candidate_budget = page_size;
    let core_store = CoreStore::new(storage.clone())
        .await
        .map_err(AuthzProjectionPageError::from)?;
    let mut object_ids = Vec::new();
    let mut scan_after = after_tuple_key;
    let mut candidates_visited = 0;
    let mut source_exhausted = false;

    while candidates_visited < candidate_budget {
        let chunk_limit = (candidate_budget - candidates_visited).min(AUTHZ_SOURCE_SCAN_CHUNK_ROWS);
        let rows = core_store
            .scan_coremeta_prefix_page(
                CF_AUTHZ,
                TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
                &prefix,
                scan_after.as_deref(),
                chunk_limit,
            )
            .map_err(AuthzProjectionPageError::from)?;
        if rows.is_empty() {
            source_exhausted = true;
            break;
        }
        let row_count = rows.len();
        for row in rows {
            candidates_visited += 1;
            let tuple_key = core_meta_record_tuple_key(&row.key)
                .context("decode authz object candidate tuple key")?
                .to_vec();
            scan_after = Some(tuple_key.clone());
            let record = decode_current_payload(storage, tenant_id, &row.payload)
                .await
                .map_err(AuthzProjectionPageError::from)?;
            validate_projection_row_key(ProjectionOrder::Object, &tuple_key, &record)
                .map_err(AuthzProjectionPageError::from)?;
            if record.namespace != namespace {
                return Err(AuthzProjectionPageError::Internal(
                    "authz object candidate projection scope mismatch".to_string(),
                ));
            }
            if object_ids.last() != Some(&record.object_id) {
                object_ids.push(record.object_id);
            }
        }
        if row_count < chunk_limit {
            source_exhausted = true;
            break;
        }
    }

    let next_object_id = (!source_exhausted)
        .then(|| object_ids.last().cloned())
        .flatten();
    require_revision(storage, tenant_id, expected_revision).await?;
    Ok(AuthzObjectCandidatePage {
        object_ids,
        next_object_id,
        candidates_visited,
    })
}

async fn require_revision(
    storage: &Storage,
    tenant_id: i64,
    expected_revision: i64,
) -> std::result::Result<(), AuthzProjectionPageError> {
    let actual = i64::try_from(
        authz_head::read(storage, tenant_id)
            .await
            .map_err(AuthzProjectionPageError::from)?
            .head
            .committed_revision,
    )
    .map_err(|error| AuthzProjectionPageError::Internal(error.to_string()))?;
    if actual != expected_revision {
        return Err(AuthzProjectionPageError::RevisionMismatch {
            expected: expected_revision,
            actual,
        });
    }
    Ok(())
}

fn candidate_budget(page_size: usize) -> usize {
    page_size
        .saturating_mul(AUTHZ_PAGE_CANDIDATE_MULTIPLIER)
        .saturating_add(1)
        .clamp(page_size.saturating_add(1), MAX_AUTHZ_PAGE_CANDIDATES)
}

fn object_projection_upper_bound(
    tenant_id: i64,
    realm_id: &str,
    namespace: &str,
    object_id: &str,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(realm_id),
        CoreMetaTuplePart::Utf8(namespace),
        CoreMetaTuplePart::Utf8(object_id),
        // Every tuple row extends the object prefix with a UTF-8 relation
        // component (kind 0x01). Bool is kind 0x06, so this valid tuple key is
        // ordered after every row for the object and before the next object.
        CoreMetaTuplePart::Bool(true),
    ])
}

async fn encode_current_payload(
    storage: &Storage,
    record: &AuthzTupleRecord,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    let record_payload = encode_current_row(record, transaction_id)?;
    encode_authz_payload_row(
        storage,
        current_common(record, transaction_id),
        AUTHZ_TUPLE_CURRENT_PAYLOAD_KIND,
        &format!(
            "tenant/{}/authz/current/{}",
            record.tenant_id, record.record_hash
        ),
        record.revision.max(0) as u64,
        transaction_id,
        record_payload,
    )
    .await
}

async fn decode_current_payload(
    storage: &Storage,
    tenant_id: i64,
    payload: &[u8],
) -> Result<AuthzTupleRecord> {
    let record_payload = decode_authz_payload_row(
        storage,
        tenant_id,
        payload,
        AUTHZ_TUPLE_CURRENT_PAYLOAD_KIND,
    )
    .await?;
    let record = decode_current_row(&record_payload)?;
    if record.tenant_id != tenant_id {
        return Err(anyhow!("authz tuple current row tenant mismatch"));
    }
    if record.operation != "add" {
        return Err(anyhow!(
            "authz active current projection contains a removed tuple"
        ));
    }
    Ok(record)
}

fn encode_current_row(record: &AuthzTupleRecord, transaction_id: &str) -> Result<Vec<u8>> {
    super::encode_deterministic_proto(&AuthzTupleCurrentRowProto {
        common: Some(current_common(record, transaction_id)),
        schema: AUTHZ_TUPLE_CURRENT_ROW_SCHEMA.to_string(),
        record: Some(authz_record_to_proto(record)?),
    })
}

fn decode_current_row(bytes: &[u8]) -> Result<AuthzTupleRecord> {
    let row = AuthzTupleCurrentRowProto::decode(bytes)?;
    ensure_deterministic_proto(&row, bytes, "authz tuple current row")?;
    if row.schema != AUTHZ_TUPLE_CURRENT_ROW_SCHEMA {
        return Err(anyhow!("authz tuple current row schema mismatch"));
    }
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("authz tuple current row missing CoreMeta common"))?;
    let record = authz_record_from_proto(
        row.record
            .ok_or_else(|| anyhow!("authz tuple current row is missing record"))?,
    )?;
    let written_at_unix_nanos = record
        .written_at
        .timestamp_nanos_opt()
        .unwrap_or_default()
        .max(0) as u64;
    if record.revision <= 0
        || common.realm_id != format!("tenant/{}", record.tenant_id)
        || common.root_key_hash != core_meta_root_key_hash(&format!("authz/{}", record.tenant_id))
        || common.root_generation == 0
        || common.transaction_id.is_empty()
        || common.created_at_unix_nanos != written_at_unix_nanos
        || common.visibility_state_enum() != crate::core_store::CoreMetaVisibilityState::Committed
    {
        return Err(anyhow!("authz tuple current row scope metadata mismatch"));
    }
    Ok(record)
}

fn current_common(
    record: &AuthzTupleRecord,
    transaction_id: &str,
) -> crate::core_store::CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        format!("tenant/{}", record.tenant_id),
        core_meta_root_key_hash(&format!("authz/{}", record.tenant_id)),
        record.revision.max(0) as u64,
        transaction_id,
        record
            .written_at
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .max(0) as u64,
    )
}

fn current_put(
    partition_id: &str,
    table_id: u16,
    tuple_key: Vec<u8>,
    payload: Vec<u8>,
) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: CF_AUTHZ.to_string(),
        table_id,
        tuple_key,
        payload,
    }
}

fn current_delete(partition_id: &str, table_id: u16, tuple_key: Vec<u8>) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaDelete {
        partition_id: partition_id.to_string(),
        cf: CF_AUTHZ.to_string(),
        table_id,
        tuple_key,
    }
}

fn validate_projection_row_key(
    order: ProjectionOrder,
    tuple_key: &[u8],
    record: &AuthzTupleRecord,
) -> Result<()> {
    let expected = match order {
        ProjectionOrder::Object => object_row_key(record)?,
        ProjectionOrder::Subject => subject_row_key(record)?,
    };
    if tuple_key != expected {
        return Err(anyhow!(
            "authz active projection key does not match payload"
        ));
    }
    Ok(())
}

pub(super) fn object_row_key(record: &AuthzTupleRecord) -> Result<Vec<u8>> {
    let (realm_id, namespace) = namespace_parts(&record.namespace);
    core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(record.tenant_id),
        CoreMetaTuplePart::Utf8(&realm_id),
        CoreMetaTuplePart::Utf8(&namespace),
        CoreMetaTuplePart::Utf8(&record.object_id),
        CoreMetaTuplePart::Utf8(&record.relation),
        CoreMetaTuplePart::Utf8(&record.subject_kind),
        CoreMetaTuplePart::Utf8(&record.subject_id),
        CoreMetaTuplePart::Utf8(&record.caveat_hash),
    ])
}

pub(super) fn subject_row_key(record: &AuthzTupleRecord) -> Result<Vec<u8>> {
    let (realm_id, namespace) = namespace_parts(&record.namespace);
    core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(record.tenant_id),
        CoreMetaTuplePart::Utf8(&realm_id),
        CoreMetaTuplePart::Utf8(&record.subject_kind),
        CoreMetaTuplePart::Utf8(&record.subject_id),
        CoreMetaTuplePart::Utf8(&record.caveat_hash),
        CoreMetaTuplePart::Utf8(&namespace),
        CoreMetaTuplePart::Utf8(&record.object_id),
        CoreMetaTuplePart::Utf8(&record.relation),
    ])
}

fn object_filter_prefix(tenant_id: i64, filter: &AuthzTupleFilter) -> Result<Vec<u8>> {
    let mut parts = vec![CoreMetaTuplePart::I64(tenant_id)];
    let Some(realm_id) = filter_realm(filter) else {
        return core_meta_tuple_key(&parts);
    };
    parts.push(CoreMetaTuplePart::Utf8(&realm_id));

    let local_namespace = filter
        .namespace
        .as_deref()
        .map(namespace_parts)
        .map(|(_, ns)| ns);
    let Some(namespace) = local_namespace.as_deref() else {
        return core_meta_tuple_key(&parts);
    };
    parts.push(CoreMetaTuplePart::Utf8(namespace));
    push_contiguous(
        &mut parts,
        [
            filter.object_id.as_deref(),
            filter.relation.as_deref(),
            filter.subject_kind.as_deref(),
            filter.subject_id.as_deref(),
            filter.caveat_hash.as_deref(),
        ],
    );
    core_meta_tuple_key(&parts)
}

fn subject_filter_prefix(tenant_id: i64, filter: &AuthzTupleFilter) -> Result<Vec<u8>> {
    let mut parts = vec![CoreMetaTuplePart::I64(tenant_id)];
    let Some(realm_id) = filter_realm(filter) else {
        return core_meta_tuple_key(&parts);
    };
    parts.push(CoreMetaTuplePart::Utf8(&realm_id));
    push_contiguous(
        &mut parts,
        [
            filter.subject_kind.as_deref(),
            filter.subject_id.as_deref(),
            filter.caveat_hash.as_deref(),
        ],
    );
    if filter.subject_kind.is_none() || filter.subject_id.is_none() || filter.caveat_hash.is_none()
    {
        return core_meta_tuple_key(&parts);
    }
    let local_namespace = filter
        .namespace
        .as_deref()
        .map(namespace_parts)
        .map(|(_, namespace)| namespace);
    if let Some(namespace) = local_namespace.as_deref() {
        parts.push(CoreMetaTuplePart::Utf8(namespace));
        push_contiguous(
            &mut parts,
            [filter.object_id.as_deref(), filter.relation.as_deref()],
        );
    }
    core_meta_tuple_key(&parts)
}

fn push_contiguous<'a, const N: usize>(
    parts: &mut Vec<CoreMetaTuplePart<'a>>,
    values: [Option<&'a str>; N],
) {
    for value in values {
        let Some(value) = value else {
            break;
        };
        parts.push(CoreMetaTuplePart::Utf8(value));
    }
}

fn projection_order(filter: &AuthzTupleFilter) -> ProjectionOrder {
    if filter.subject_kind.is_some() {
        ProjectionOrder::Subject
    } else {
        ProjectionOrder::Object
    }
}

fn filter_realm(filter: &AuthzTupleFilter) -> Option<String> {
    filter.realm_id.clone().or_else(|| {
        filter
            .namespace
            .as_deref()
            .map(namespace_parts)
            .map(|(realm_id, _)| realm_id)
    })
}

fn matches_filter(record: &AuthzTupleRecord, filter: &AuthzTupleFilter) -> bool {
    let (realm_id, _) = namespace_parts(&record.namespace);
    filter
        .realm_id
        .as_ref()
        .is_none_or(|value| realm_id == *value)
        && filter
            .namespace
            .as_ref()
            .is_none_or(|value| record.namespace == *value)
        && filter
            .object_id
            .as_ref()
            .is_none_or(|value| record.object_id == *value)
        && filter
            .relation
            .as_ref()
            .is_none_or(|value| record.relation == *value)
        && filter
            .subject_kind
            .as_ref()
            .is_none_or(|value| record.subject_kind == *value)
        && filter
            .subject_id
            .as_ref()
            .is_none_or(|value| record.subject_id == *value)
        && filter
            .caveat_hash
            .as_ref()
            .is_none_or(|value| record.caveat_hash == *value)
}

fn namespace_parts(namespace: &str) -> (String, String) {
    split_realm_namespace(namespace)
        .map(|(realm_id, local_namespace)| (realm_id, local_namespace.to_string()))
        .unwrap_or_else(|| (DEFAULT_AUTHZ_REALM_ID.to_string(), namespace.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authz_journal::{AuthzRecordHashInput, authz_record_hash};
    use chrono::Utc;

    #[test]
    fn current_projection_accepts_independent_physical_root_generation() {
        let mut record = AuthzTupleRecord {
            revision: 3,
            revision_ordinal: 0,
            tenant_id: 42,
            namespace: "document".into(),
            object_id: "alpha".into(),
            relation: "viewer".into(),
            subject_kind: "user".into(),
            subject_id: "alice".into(),
            caveat_hash: String::new(),
            operation: "add".into(),
            written_by: "tester".into(),
            reason: "test".into(),
            mutation_id: uuid::Uuid::new_v4(),
            record_hash: String::new(),
            written_at: Utc::now(),
        };
        record.record_hash = authz_record_hash(AuthzRecordHashInput {
            revision: record.revision,
            revision_ordinal: record.revision_ordinal,
            tenant_id: record.tenant_id,
            namespace: &record.namespace,
            object_id: &record.object_id,
            relation: &record.relation,
            subject_kind: &record.subject_kind,
            subject_id: &record.subject_id,
            caveat_hash: &record.caveat_hash,
            operation: &record.operation,
            written_by: &record.written_by,
            reason: &record.reason,
        });
        let payload = encode_current_row(&record, "tx-physical-generation").unwrap();
        let mut common = crate::core_store::core_meta_row_common_from_payload(&payload).unwrap();
        common.root_generation = 91;
        let rebound = crate::core_store::replace_core_meta_row_common(&payload, &common).unwrap();

        let decoded = decode_current_row(&rebound).unwrap();
        assert_eq!(decoded.record_hash, record.record_hash);
        assert_ne!(common.root_generation, record.revision as u64);
    }
}
