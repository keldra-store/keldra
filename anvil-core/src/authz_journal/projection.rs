use super::{
    AuthzTupleFilter, AuthzTupleRecordProto, authz_record_from_proto, authz_record_to_proto,
    ensure_deterministic_proto,
};
use crate::authz_head;
use crate::authz_scope::{DEFAULT_AUTHZ_REALM_ID, split_realm_namespace};
use crate::core_store::{
    CF_AUTHZ, CoreMetaTuplePart, TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
    TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW, core_meta_tuple_key,
};
use crate::persistence::AuthzTupleRecord;
use anyhow::{Result, anyhow};
use prost::Message;
use std::collections::BTreeMap;

const AUTHZ_TUPLE_CURRENT_ROW_SCHEMA: &str = "anvil.authz.tuple_current_row.v2";
#[derive(Clone, PartialEq, Message)]
struct AuthzTupleCurrentRowProto {
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

pub(super) fn current_mutations(
    records: &[AuthzTupleRecord],
    transaction_id: &str,
) -> Result<Vec<crate::mvcc_product::ProductMutation>> {
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
                let payload = encode_current_payload(record, transaction_id)?;
                operations.push(current_put(
                    TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
                    object_key,
                    payload.clone(),
                ));
                operations.push(current_put(
                    TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW,
                    subject_key,
                    payload,
                ));
            }
            "remove" => {
                operations.push(current_delete(
                    TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
                    object_key,
                ));
                operations.push(current_delete(
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
pub(super) fn read_current_record(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<Option<AuthzTupleRecord>> {
    let snapshot = mvcc.runtime.applied_version()?;
    read_current_record_at_runtime(
        mvcc.runtime.as_ref(),
        snapshot,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn read_current_record_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<Option<AuthzTupleRecord>> {
    read_current_record_at_runtime(
        mvcc.runtime.as_ref(),
        snapshot,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn read_current_record_at_runtime(
    runtime: &crate::mvcc_bootstrap::ProductMvccRuntime,
    snapshot: u64,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<Option<AuthzTupleRecord>> {
    let tuple_key = object_tuple_key(
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
    )?;
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_AUTHZ,
        TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
        &tuple_key,
    )?;
    let Some(row) = runtime.read_at(&key, snapshot)? else {
        return Ok(None);
    };
    let record = decode_current_payload(tenant_id, &row.value)?;
    validate_projection_row_key(ProjectionOrder::Object, &tuple_key, &record)?;
    Ok(Some(record))
}

pub(super) fn read_current_relation_rows(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot: u64,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: Option<&str>,
) -> Result<AuthzRelationRows> {
    let prefix = object_relation_prefix(tenant_id, namespace, object_id, relation, subject_kind)?;
    let rows = scan_projection(
        mvcc,
        TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
        &prefix,
        snapshot,
    )?;
    if rows.len() > MAX_AUTHZ_RELATION_ROWS {
        return Err(anyhow!(
            "AuthzGraphBreadthExceeded: relation contains more than {MAX_AUTHZ_RELATION_ROWS} tuples"
        ));
    }
    let candidates_visited = rows.len();
    let mut records = Vec::with_capacity(candidates_visited);
    for (tuple_key, payload) in rows {
        let record = decode_current_payload(tenant_id, &payload)?;
        validate_projection_row_key(ProjectionOrder::Object, &tuple_key, &record)?;
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

pub(super) fn page_current_records(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    filter: &AuthzTupleFilter,
    expected_revision: i64,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> std::result::Result<AuthzTupleProjectionPage, AuthzProjectionPageError> {
    if !(1..=1000).contains(&page_size) {
        return Err(AuthzProjectionPageError::InvalidPageSize);
    }
    let snapshot = mvcc
        .runtime
        .applied_version()
        .map_err(AuthzProjectionPageError::from)?;
    require_revision_mvcc(mvcc, tenant_id, expected_revision, snapshot)?;
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
    let mut matches = Vec::with_capacity(page_size.saturating_add(1));
    let mut candidates_visited = 0;
    let mut continuation = None;
    for (tuple_key, payload) in scan_projection(mvcc, table_id, &prefix, snapshot)? {
        if after_tuple_key.is_some_and(|after| tuple_key.as_slice() <= after) {
            continue;
        }
        if candidates_visited == candidate_budget {
            continuation = matches
                .last()
                .map(|(key, _)| key.clone())
                .or_else(|| Some(tuple_key));
            break;
        }
        candidates_visited += 1;
        let record = decode_current_payload(tenant_id, &payload)?;
        validate_projection_row_key(order, &tuple_key, &record)?;
        if matches_filter(&record, filter) {
            matches.push((tuple_key, record));
            if matches.len() > page_size {
                continuation = matches.get(page_size - 1).map(|(key, _)| key.clone());
                matches.truncate(page_size);
                break;
            }
        }
    }
    require_revision_mvcc(mvcc, tenant_id, expected_revision, snapshot)?;
    Ok(AuthzTupleProjectionPage {
        records: matches.into_iter().map(|(_, record)| record).collect(),
        next_tuple_key: continuation,
        candidates_visited,
    })
}

pub(super) fn page_current_object_candidates(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    namespace: &str,
    expected_revision: i64,
    after_object_id: Option<&str>,
    page_size: usize,
) -> std::result::Result<AuthzObjectCandidatePage, AuthzProjectionPageError> {
    if !(1..=1000).contains(&page_size) {
        return Err(AuthzProjectionPageError::InvalidPageSize);
    }
    let snapshot = mvcc
        .runtime
        .applied_version()
        .map_err(AuthzProjectionPageError::from)?;
    require_revision_mvcc(mvcc, tenant_id, expected_revision, snapshot)?;
    let (realm_id, local_namespace) = namespace_parts(namespace);
    let prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(&realm_id),
        CoreMetaTuplePart::Utf8(&local_namespace),
    ])?;
    let mut object_ids = Vec::with_capacity(page_size);
    let mut candidates_visited = 0;
    let mut next_object_id = None;
    for (tuple_key, payload) in scan_projection(
        mvcc,
        TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW,
        &prefix,
        snapshot,
    )? {
        let record = decode_current_payload(tenant_id, &payload)?;
        validate_projection_row_key(ProjectionOrder::Object, &tuple_key, &record)?;
        if after_object_id.is_some_and(|after| record.object_id.as_str() <= after) {
            continue;
        }
        candidates_visited += 1;
        if object_ids.last() != Some(&record.object_id) {
            if object_ids.len() == page_size {
                next_object_id = object_ids.last().cloned();
                break;
            }
            object_ids.push(record.object_id);
        }
    }
    require_revision_mvcc(mvcc, tenant_id, expected_revision, snapshot)?;
    Ok(AuthzObjectCandidatePage {
        object_ids,
        next_object_id,
        candidates_visited,
    })
}

fn require_revision_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    expected_revision: i64,
    snapshot: u64,
) -> std::result::Result<(), AuthzProjectionPageError> {
    let actual = i64::try_from(authz_head::read_at_mvcc(mvcc, tenant_id, snapshot)?.tuple_revision)
        .map_err(|_| {
            AuthzProjectionPageError::Internal("authorization revision exceeds i64".to_string())
        })?;
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

fn encode_current_payload(record: &AuthzTupleRecord, transaction_id: &str) -> Result<Vec<u8>> {
    encode_current_row(record, transaction_id)
}

fn decode_current_payload(tenant_id: i64, payload: &[u8]) -> Result<AuthzTupleRecord> {
    let record = decode_current_row(payload)?;
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
    if transaction_id.is_empty() {
        return Err(anyhow!("authz tuple MVCC transaction ID is empty"));
    }
    super::encode_deterministic_proto(&AuthzTupleCurrentRowProto {
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
    let record = authz_record_from_proto(
        row.record
            .ok_or_else(|| anyhow!("authz tuple current row is missing record"))?,
    )?;
    if record.revision <= 0 {
        return Err(anyhow!("authz tuple current row revision is invalid"));
    }
    Ok(record)
}

fn current_put(
    table_id: u16,
    tuple_key: Vec<u8>,
    payload: Vec<u8>,
) -> crate::mvcc_product::ProductMutation {
    crate::mvcc_product::ProductMutation::put(
        crate::mvcc_product::coremeta_logical_key(CF_AUTHZ, table_id, &tuple_key)
            .expect("validated authorization tuple key"),
        payload,
    )
}

fn current_delete(table_id: u16, tuple_key: Vec<u8>) -> crate::mvcc_product::ProductMutation {
    crate::mvcc_product::ProductMutation::delete(
        crate::mvcc_product::coremeta_logical_key(CF_AUTHZ, table_id, &tuple_key)
            .expect("validated authorization tuple key"),
    )
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
    object_tuple_key(
        record.tenant_id,
        &record.namespace,
        &record.object_id,
        &record.relation,
        &record.subject_kind,
        &record.subject_id,
        &record.caveat_hash,
    )
}

#[allow(clippy::too_many_arguments)]
fn object_tuple_key(
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<Vec<u8>> {
    let (realm_id, namespace) = namespace_parts(namespace);
    core_meta_tuple_key(&[
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(&realm_id),
        CoreMetaTuplePart::Utf8(&namespace),
        CoreMetaTuplePart::Utf8(object_id),
        CoreMetaTuplePart::Utf8(relation),
        CoreMetaTuplePart::Utf8(subject_kind),
        CoreMetaTuplePart::Utf8(subject_id),
        CoreMetaTuplePart::Utf8(caveat_hash),
    ])
}

fn object_relation_prefix(
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: Option<&str>,
) -> Result<Vec<u8>> {
    let (realm_id, namespace) = namespace_parts(namespace);
    let mut parts = vec![
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(&realm_id),
        CoreMetaTuplePart::Utf8(&namespace),
        CoreMetaTuplePart::Utf8(object_id),
        CoreMetaTuplePart::Utf8(relation),
    ];
    if let Some(subject_kind) = subject_kind {
        parts.push(CoreMetaTuplePart::Utf8(subject_kind));
    }
    core_meta_tuple_key(&parts)
}

fn scan_projection(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    table_id: u16,
    tuple_prefix: &[u8],
    snapshot: u64,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_AUTHZ, tuple_prefix)?;
    mvcc.runtime
        .scan_table_prefix_at(table_id, &application_prefix, snapshot)?
        .into_iter()
        .map(|(key, row)| {
            Ok((
                crate::mvcc_product::coremeta_tuple_from_logical_key(&key, CF_AUTHZ)?.to_vec(),
                row.value,
            ))
        })
        .collect()
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
