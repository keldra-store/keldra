use crate::core_store::{
    CF_INDEX_ROWS, CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMetaVisibilityState,
    TABLE_INDEX_ROW, core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
    encode_deterministic_proto, sha256_hex,
};
use anyhow::{Result, anyhow, bail};
use prost::Message;

const INDEX_SEGMENT_ROW_SCHEMA: &str = "anvil.coremeta.index_segment_row.v1";
const INDEX_SEGMENT_AUTHZ_SCOPE_SCHEMA: &str = "anvil.index.segment_authz_scope.v1";
const INDEX_DEFINITION_CURRENT_ROW_SCHEMA: &str = "anvil.coremeta.index_definition_current.v1";
const INDEX_DEFINITION_STATE_ROW_SCHEMA: &str = "anvil.coremeta.index_definition_state.v1";
pub const INDEX_SEGMENT_COREMETA_PAGE_MAX: usize = 1000;

const DETERMINISTIC_TIME_BASE_NANOS: u64 = 946_684_800_000_000_000;
const DETERMINISTIC_TIME_WINDOW_NANOS: u64 = 3_155_760_000_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSegmentCoreMetaRecord {
    pub index_id: String,
    pub index_kind: String,
    pub writer_family: String,
    pub segment_ref: String,
    pub core_object_ref_target: String,
    pub segment_hash: String,
    pub segment_length: u64,
    pub generation: u64,
    pub source_kind: String,
    pub source_cursor: u64,
    pub authz_realm_id: String,
    pub authz_scope_hash: String,
    pub authz_revision: u64,
    pub row_count: u64,
    pub field_names: Vec<String>,
    pub created_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDefinitionCurrentCoreMetaRecord {
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub index_name: String,
    pub deleted: bool,
    pub cursor: i64,
    pub index_version: i64,
    pub event_payload: Vec<u8>,
    pub updated_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDefinitionStateCoreMetaRecord {
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub latest_cursor: i64,
    pub max_index_id: i64,
    pub updated_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSegmentCoreMetaPage {
    pub records: Vec<IndexSegmentCoreMetaRecord>,
    pub next_tuple_key: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct IndexSegmentCoreMetaRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    index_id: String,
    #[prost(string, tag = "4")]
    index_kind: String,
    #[prost(string, tag = "5")]
    writer_family: String,
    #[prost(string, tag = "6")]
    segment_ref: String,
    #[prost(string, tag = "7")]
    core_object_ref_target: String,
    #[prost(string, tag = "8")]
    segment_hash: String,
    #[prost(uint64, tag = "9")]
    segment_length: u64,
    #[prost(uint64, tag = "10")]
    generation: u64,
    #[prost(string, tag = "11")]
    source_kind: String,
    #[prost(uint64, tag = "12")]
    source_cursor: u64,
    #[prost(string, tag = "13")]
    authz_realm_id: String,
    #[prost(string, tag = "14")]
    authz_scope_hash: String,
    #[prost(uint64, tag = "15")]
    authz_revision: u64,
    #[prost(uint64, tag = "16")]
    row_count: u64,
    #[prost(string, repeated, tag = "17")]
    field_names: Vec<String>,
    #[prost(uint64, tag = "18")]
    created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDefinitionCurrentCoreMetaRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    index_name: String,
    #[prost(bool, tag = "6")]
    deleted: bool,
    #[prost(int64, tag = "7")]
    cursor: i64,
    #[prost(int64, tag = "8")]
    index_version: i64,
    #[prost(bytes, tag = "9")]
    event_payload: Vec<u8>,
    #[prost(uint64, tag = "10")]
    updated_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDefinitionStateCoreMetaRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(int64, tag = "5")]
    latest_cursor: i64,
    #[prost(int64, tag = "6")]
    max_index_id: i64,
    #[prost(uint64, tag = "7")]
    updated_at_unix_nanos: u64,
}

pub fn segment_authz_scope_hash(index_kind: &str, authorization_mode: &str) -> String {
    let payload = format!(
        "{INDEX_SEGMENT_AUTHZ_SCOPE_SCHEMA}\0index_kind={index_kind}\0authorization_mode={authorization_mode}\0namespace=object\0relation=reader"
    );
    format!("blake3:{}", blake3::hash(payload.as_bytes()).to_hex())
}

pub fn typed_segment_index_kind(source_kind: &str) -> &'static str {
    if source_kind == "object_metadata" {
        "metadata"
    } else {
        "typed_json"
    }
}

pub async fn write_index_segment_coremeta_record(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    record: &IndexSegmentCoreMetaRecord,
    additional_preconditions: &[(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )],
) -> Result<()> {
    validate_index_segment_record(record)?;
    let payload = encode_index_segment_record(record);
    let tuple_keys = [
        index_segment_tuple_key(record)?,
        index_segment_order_tuple_key(record)?,
        index_segment_generation_tuple_key(
            &record.index_id,
            &record.writer_family,
            record.generation,
        )?,
        index_segment_ref_tuple_key(&record.index_id, &record.segment_ref)?,
    ];
    let mut present = 0;
    for tuple_key in &tuple_keys {
        if let Some(existing) = mvcc.read_latest_value(&index_segment_logical_key(tuple_key)?)? {
            if decode_index_segment_record(&existing)? != *record {
                bail!("index segment locator already identifies different immutable bytes");
            }
            present += 1;
        }
    }
    if present == tuple_keys.len() {
        return Ok(());
    }
    if present != 0 {
        bail!("index segment locator projection is incomplete");
    }

    let mut preconditions = tuple_keys
        .iter()
        .map(|tuple_key| {
            Ok((
                index_segment_logical_key(tuple_key)?,
                crate::mvcc_transaction::PredicateKind::Absent,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    preconditions.extend_from_slice(additional_preconditions);
    let operations = tuple_keys
        .into_iter()
        .map(|tuple_key| {
            Ok(crate::mvcc_product::ProductMutation::put(
                index_segment_logical_key(&tuple_key)?,
                payload.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let logical_transaction_id = format!(
        "index-segment:{}:{}:{}",
        record.index_id, record.generation, record.segment_hash
    );
    mvcc.autocommit_product_mutations_with_predicates(
        &format!("index-builder:{}", record.index_id),
        &logical_transaction_id,
        operations,
        preconditions,
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| anyhow!("index segment timestamp predates Unix epoch"))?,
    )
    .await?;
    Ok(())
}

/// Produces a stable timestamp for task-derived index payloads.
///
/// Wall-clock time makes a replay produce different segment, proof, watch, and
/// diagnostic bytes. This maps immutable build inputs into a bounded RFC3339
/// range while retaining deterministic divergence detection.
pub(crate) fn deterministic_index_publication_nanos(
    index_scope: &str,
    publication_kind: &str,
    generation: u64,
    source_cursor: u128,
    content_digest: &str,
) -> i64 {
    let mut input = Vec::new();
    let generation = generation.to_string();
    let source_cursor = source_cursor.to_string();
    for part in [
        index_scope,
        publication_kind,
        &generation,
        &source_cursor,
        content_digest,
    ] {
        input.extend_from_slice(part.as_bytes());
        input.push(0);
    }
    let hash = blake3::hash(&input);
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&hash.as_bytes()[..8]);
    let nanos = DETERMINISTIC_TIME_BASE_NANOS
        + u64::from_be_bytes(prefix) % DETERMINISTIC_TIME_WINDOW_NANOS;
    i64::try_from(nanos).expect("bounded deterministic index timestamp fits i64")
}

pub(crate) fn deterministic_index_mutation_id(
    index_scope: &str,
    publication_kind: &str,
    generation: u64,
    source_cursor: u128,
    content_digest: &str,
) -> [u8; 16] {
    let mut input = Vec::new();
    let generation = generation.to_string();
    let source_cursor = source_cursor.to_string();
    for part in [
        index_scope,
        publication_kind,
        &generation,
        &source_cursor,
        content_digest,
    ] {
        input.extend_from_slice(part.as_bytes());
        input.push(0);
    }
    let hash = blake3::hash(&input);
    let mut mutation_id = [0_u8; 16];
    mutation_id.copy_from_slice(&hash.as_bytes()[..16]);
    mutation_id
}

pub async fn latest_index_segment_coremeta_record(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    index_id: &str,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    latest_index_segment_coremeta_record_matching(mvcc, index_id, None)
}

pub async fn latest_index_segment_coremeta_record_for_family(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    index_id: &str,
    writer_family: &str,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    latest_index_segment_coremeta_record_matching(mvcc, index_id, Some(writer_family))
}

pub async fn index_segment_coremeta_record_for_family_generation(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    index_id: &str,
    writer_family: &str,
    generation: u64,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    let snapshot = mvcc.runtime.applied_version()?;
    let record = read_index_segment_point_at(
        mvcc,
        &index_segment_generation_tuple_key(index_id, writer_family, generation)?,
        snapshot,
    )?;
    if record.as_ref().is_some_and(|record| {
        record.index_id != index_id
            || record.writer_family != writer_family
            || record.generation != generation
    }) {
        bail!("CoreMeta index segment generation point row scope mismatch");
    }
    Ok(record)
}

fn latest_index_segment_coremeta_record_matching(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    index_id: &str,
    writer_family: Option<&str>,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    let tuple_prefix = match writer_family {
        Some(writer_family) => index_segment_generation_tuple_prefix(index_id, writer_family)?,
        None => index_segment_order_tuple_prefix(index_id)?,
    };
    let snapshot = mvcc.runtime.applied_version()?;
    let prefix = crate::mvcc_product::coremeta_application_prefix(CF_INDEX_ROWS, &tuple_prefix)?;
    let record = mvcc
        .runtime
        .scan_table_prefix_at(TABLE_INDEX_ROW, &prefix, snapshot)?
        .into_iter()
        .next_back()
        .map(|(_, row)| decode_index_segment_record(&row.value))
        .transpose()?;
    if record.as_ref().is_some_and(|record| {
        record.index_id != index_id
            || writer_family.is_some_and(|family| record.writer_family != family)
    }) {
        bail!("CoreMeta latest index segment point row scope mismatch");
    }
    Ok(record)
}

pub async fn read_index_segment_coremeta_record_by_ref(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    index_id: &str,
    segment_ref: &str,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    let snapshot = mvcc.runtime.applied_version()?;
    let record = read_index_segment_point_at(
        mvcc,
        &index_segment_ref_tuple_key(index_id, segment_ref)?,
        snapshot,
    )?;
    if record
        .as_ref()
        .is_some_and(|record| record.index_id != index_id || record.segment_ref != segment_ref)
    {
        bail!("CoreMeta index segment ref point row scope mismatch");
    }
    Ok(record)
}

pub async fn page_index_segment_coremeta_records(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    index_id: &str,
    after_tuple_key: Option<&[u8]>,
    page_size: usize,
) -> Result<IndexSegmentCoreMetaPage> {
    if !(1..=INDEX_SEGMENT_COREMETA_PAGE_MAX).contains(&page_size) {
        bail!(
            "index segment CoreMeta page size must be between 1 and {INDEX_SEGMENT_COREMETA_PAGE_MAX}"
        );
    }
    let prefix = index_segment_tuple_prefix(index_id)?;
    if after_tuple_key
        .is_some_and(|cursor| cursor.len() <= prefix.len() || !cursor.starts_with(&prefix))
    {
        bail!("index segment CoreMeta cursor is outside the index prefix");
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_INDEX_ROWS, &prefix)?;
    let namespace = crate::mvcc_product::coremeta_application_prefix(CF_INDEX_ROWS, &[])?;
    let mut rows =
        mvcc.runtime
            .scan_table_prefix_at(TABLE_INDEX_ROW, &application_prefix, snapshot)?;
    if let Some(after) = after_tuple_key {
        rows.retain(|(key, _)| {
            key.application_key
                .strip_prefix(&namespace)
                .is_some_and(|tuple| tuple > after)
        });
    }
    rows.truncate(page_size + 1);
    let has_more = rows.len() > page_size;
    if has_more {
        rows.truncate(page_size);
    }
    let next_tuple_key = if has_more {
        Some(
            rows.last()
                .and_then(|(key, _)| key.application_key.strip_prefix(&namespace))
                .ok_or_else(|| anyhow!("index segment CoreMeta page is empty"))?
                .to_vec(),
        )
    } else {
        None
    };
    let records = rows
        .into_iter()
        .map(|(key, row)| {
            let record = decode_index_segment_record(&row.value)?;
            let expected_key = index_segment_tuple_key(&record)?;
            if record.index_id != index_id
                || key.application_key.strip_prefix(&namespace) != Some(expected_key.as_slice())
            {
                bail!("CoreMeta index segment history row scope mismatch");
            }
            Ok(record)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(IndexSegmentCoreMetaPage {
        records,
        next_tuple_key,
    })
}

fn read_index_segment_point_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tuple_key: &[u8],
    snapshot: u64,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    mvcc.runtime
        .read_at(&index_segment_logical_key(tuple_key)?, snapshot)?
        .map(|row| decode_index_segment_record(&row.value))
        .transpose()
}

fn index_segment_logical_key(tuple_key: &[u8]) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(CF_INDEX_ROWS, TABLE_INDEX_ROW, tuple_key)
}

fn encode_index_segment_record(record: &IndexSegmentCoreMetaRecord) -> Vec<u8> {
    encode_deterministic_proto(&IndexSegmentCoreMetaRecordProto {
        schema: INDEX_SEGMENT_ROW_SCHEMA.to_string(),
        common: Some(CoreMetaRowCommonProto {
            realm_id: record.authz_realm_id.clone(),
            root_key_hash: index_segment_root_key_hash(&record.index_id),
            root_generation: record.generation,
            transaction_id: format!("index-segment:{}:{}", record.index_id, record.generation),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: record.created_at_unix_nanos,
            payload_schema_version: 1,
        }),
        index_id: record.index_id.clone(),
        index_kind: record.index_kind.clone(),
        writer_family: record.writer_family.clone(),
        segment_ref: record.segment_ref.clone(),
        core_object_ref_target: record.core_object_ref_target.clone(),
        segment_hash: record.segment_hash.clone(),
        segment_length: record.segment_length,
        generation: record.generation,
        source_kind: record.source_kind.clone(),
        source_cursor: record.source_cursor,
        authz_realm_id: record.authz_realm_id.clone(),
        authz_scope_hash: record.authz_scope_hash.clone(),
        authz_revision: record.authz_revision,
        row_count: record.row_count,
        field_names: record.field_names.clone(),
        created_at_unix_nanos: record.created_at_unix_nanos,
    })
}

fn encode_index_definition_current_record(
    record: &IndexDefinitionCurrentCoreMetaRecord,
) -> Vec<u8> {
    encode_deterministic_proto(&IndexDefinitionCurrentCoreMetaRecordProto {
        schema: INDEX_DEFINITION_CURRENT_ROW_SCHEMA.to_string(),
        common: Some(CoreMetaRowCommonProto {
            realm_id: format!("tenant:{}", record.tenant_id),
            root_key_hash: index_definition_root_key_hash(record.tenant_id, record.bucket_id),
            root_generation: record.cursor as u64,
            transaction_id: format!(
                "index-definition-current:{}:{}:{}",
                record.tenant_id, record.bucket_id, record.cursor
            ),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: record.updated_at_unix_nanos,
            payload_schema_version: 1,
        }),
        tenant_id: record.tenant_id,
        bucket_id: record.bucket_id,
        index_name: record.index_name.clone(),
        deleted: record.deleted,
        cursor: record.cursor,
        index_version: record.index_version,
        event_payload: record.event_payload.clone(),
        updated_at_unix_nanos: record.updated_at_unix_nanos,
    })
}

fn decode_index_definition_current_record(
    bytes: &[u8],
) -> Result<IndexDefinitionCurrentCoreMetaRecord> {
    let proto = decode_deterministic_proto::<IndexDefinitionCurrentCoreMetaRecordProto>(
        bytes,
        "index definition current row",
    )?;
    if proto.schema != INDEX_DEFINITION_CURRENT_ROW_SCHEMA {
        bail!("CoreMeta index definition current row has invalid schema");
    }
    let record = IndexDefinitionCurrentCoreMetaRecord {
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        index_name: proto.index_name,
        deleted: proto.deleted,
        cursor: proto.cursor,
        index_version: proto.index_version,
        event_payload: proto.event_payload,
        updated_at_unix_nanos: proto.updated_at_unix_nanos,
    };
    validate_index_definition_current_record(&record)?;
    Ok(record)
}

fn encode_index_definition_state_record(record: &IndexDefinitionStateCoreMetaRecord) -> Vec<u8> {
    encode_deterministic_proto(&IndexDefinitionStateCoreMetaRecordProto {
        schema: INDEX_DEFINITION_STATE_ROW_SCHEMA.to_string(),
        common: Some(CoreMetaRowCommonProto {
            realm_id: format!("tenant:{}", record.tenant_id),
            root_key_hash: index_definition_root_key_hash(record.tenant_id, record.bucket_id),
            root_generation: record.latest_cursor as u64,
            transaction_id: format!(
                "index-definition-state:{}:{}:{}",
                record.tenant_id, record.bucket_id, record.latest_cursor
            ),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: record.updated_at_unix_nanos,
            payload_schema_version: 1,
        }),
        tenant_id: record.tenant_id,
        bucket_id: record.bucket_id,
        latest_cursor: record.latest_cursor,
        max_index_id: record.max_index_id,
        updated_at_unix_nanos: record.updated_at_unix_nanos,
    })
}

fn decode_index_definition_state_record(
    bytes: &[u8],
) -> Result<IndexDefinitionStateCoreMetaRecord> {
    let proto = decode_deterministic_proto::<IndexDefinitionStateCoreMetaRecordProto>(
        bytes,
        "index definition state row",
    )?;
    if proto.schema != INDEX_DEFINITION_STATE_ROW_SCHEMA {
        bail!("CoreMeta index definition state row has invalid schema");
    }
    let record = IndexDefinitionStateCoreMetaRecord {
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        latest_cursor: proto.latest_cursor,
        max_index_id: proto.max_index_id,
        updated_at_unix_nanos: proto.updated_at_unix_nanos,
    };
    validate_index_definition_state_record(&record)?;
    Ok(record)
}

fn decode_index_segment_record(bytes: &[u8]) -> Result<IndexSegmentCoreMetaRecord> {
    let proto =
        decode_deterministic_proto::<IndexSegmentCoreMetaRecordProto>(bytes, "index segment row")?;
    if proto.schema != INDEX_SEGMENT_ROW_SCHEMA {
        bail!("CoreMeta index segment row has invalid schema");
    }
    let record = IndexSegmentCoreMetaRecord {
        index_id: proto.index_id,
        index_kind: proto.index_kind,
        writer_family: proto.writer_family,
        segment_ref: proto.segment_ref,
        core_object_ref_target: proto.core_object_ref_target,
        segment_hash: proto.segment_hash,
        segment_length: proto.segment_length,
        generation: proto.generation,
        source_kind: proto.source_kind,
        source_cursor: proto.source_cursor,
        authz_realm_id: proto.authz_realm_id,
        authz_scope_hash: proto.authz_scope_hash,
        authz_revision: proto.authz_revision,
        row_count: proto.row_count,
        field_names: proto.field_names,
        created_at_unix_nanos: proto.created_at_unix_nanos,
    };
    validate_index_segment_record(&record)?;
    Ok(record)
}

fn validate_index_segment_record(record: &IndexSegmentCoreMetaRecord) -> Result<()> {
    require_nonempty(&record.index_id, "index_id")?;
    require_nonempty(&record.index_kind, "index_kind")?;
    require_nonempty(&record.writer_family, "writer_family")?;
    require_nonempty(&record.segment_ref, "segment_ref")?;
    if !record
        .core_object_ref_target
        .starts_with("core-object-ref:")
    {
        bail!("index segment CoreMeta row must carry a CoreStore object ref target");
    }
    validate_hex32(&record.segment_hash, "segment_hash")?;
    if record.segment_length == 0 {
        bail!("index segment length must be nonzero");
    }
    if record.generation == 0 {
        bail!("index segment generation must be nonzero");
    }
    require_nonempty(&record.source_kind, "source_kind")?;
    require_nonempty(&record.authz_realm_id, "authz_realm_id")?;
    validate_hash(&record.authz_scope_hash, "authz_scope_hash")?;
    Ok(())
}

fn validate_index_definition_current_record(
    record: &IndexDefinitionCurrentCoreMetaRecord,
) -> Result<()> {
    validate_index_definition_scope(record.tenant_id, record.bucket_id)?;
    require_nonempty(&record.index_name, "index_name")?;
    if record.index_name.chars().any(char::is_control) {
        bail!("index_name must not contain control characters");
    }
    if record.cursor <= 0 {
        bail!("index definition current cursor must be positive");
    }
    if record.index_version < 0 {
        bail!("index definition version must not be negative");
    }
    if record.event_payload.is_empty() {
        bail!("index definition current row must carry an event payload");
    }
    Ok(())
}

fn validate_index_definition_current_bucket_scope(
    record: &IndexDefinitionCurrentCoreMetaRecord,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<()> {
    if record.tenant_id != tenant_id || record.bucket_id != bucket_id {
        bail!("CoreMeta index definition current row bucket scope mismatch");
    }
    Ok(())
}

fn validate_index_definition_current_scope(
    record: &IndexDefinitionCurrentCoreMetaRecord,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<()> {
    validate_index_definition_current_bucket_scope(record, tenant_id, bucket_id)?;
    if record.index_name != index_name {
        bail!("CoreMeta index definition current row name scope mismatch");
    }
    Ok(())
}

fn validate_index_definition_state_record(
    record: &IndexDefinitionStateCoreMetaRecord,
) -> Result<()> {
    validate_index_definition_scope(record.tenant_id, record.bucket_id)?;
    if record.latest_cursor < 0 || record.max_index_id < 0 {
        bail!("index definition state counters must not be negative");
    }
    Ok(())
}

fn validate_index_definition_scope(tenant_id: i64, bucket_id: i64) -> Result<()> {
    if tenant_id < 0 || bucket_id < 0 {
        bail!("index definition CoreMeta scope ids must be nonnegative");
    }
    Ok(())
}

fn index_definition_root_anchor_key(tenant_id: i64, bucket_id: i64) -> String {
    format!("tenant/{tenant_id}/bucket/{bucket_id}/index_definition")
}

fn index_definition_root_key_hash(tenant_id: i64, bucket_id: i64) -> String {
    core_meta_root_key_hash(&index_definition_root_anchor_key(tenant_id, bucket_id))
}

fn index_segment_root_key_hash(index_id: &str) -> String {
    core_meta_root_key_hash(&format!("index/{index_id}/segments"))
}

fn index_definition_current_tuple_key(
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<Vec<u8>> {
    require_nonempty(index_name, "index_name")?;
    if index_name.chars().any(char::is_control) {
        bail!("index_name must not contain control characters");
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_definition_current"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::Utf8(index_name),
    ])
}

fn index_definition_state_tuple_key(tenant_id: i64, bucket_id: i64) -> Result<Vec<u8>> {
    validate_index_definition_scope(tenant_id, bucket_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_definition_state"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
    ])
}

fn index_segment_tuple_prefix(index_id: &str) -> Result<Vec<u8>> {
    require_nonempty(index_id, "index_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_segment"),
        CoreMetaTuplePart::Utf8(index_id),
    ])
}

fn index_segment_tuple_key(record: &IndexSegmentCoreMetaRecord) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_segment"),
        CoreMetaTuplePart::Utf8(&record.index_id),
        CoreMetaTuplePart::Utf8(&record.index_kind),
        CoreMetaTuplePart::U64(record.generation),
        CoreMetaTuplePart::Utf8(&record.segment_hash),
    ])
}

fn index_segment_order_tuple_prefix(index_id: &str) -> Result<Vec<u8>> {
    require_nonempty(index_id, "index_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_segment_order"),
        CoreMetaTuplePart::Utf8(index_id),
    ])
}

fn index_segment_order_tuple_key(record: &IndexSegmentCoreMetaRecord) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_segment_order"),
        CoreMetaTuplePart::Utf8(&record.index_id),
        CoreMetaTuplePart::U64(record.generation),
        CoreMetaTuplePart::U64(record.created_at_unix_nanos),
        CoreMetaTuplePart::Utf8(&record.writer_family),
        CoreMetaTuplePart::Hash(&record.segment_hash),
    ])
}

fn index_segment_generation_tuple_prefix(index_id: &str, writer_family: &str) -> Result<Vec<u8>> {
    require_nonempty(index_id, "index_id")?;
    require_nonempty(writer_family, "writer_family")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_segment_generation"),
        CoreMetaTuplePart::Utf8(index_id),
        CoreMetaTuplePart::Utf8(writer_family),
    ])
}

fn index_segment_generation_tuple_key(
    index_id: &str,
    writer_family: &str,
    generation: u64,
) -> Result<Vec<u8>> {
    require_nonempty(index_id, "index_id")?;
    require_nonempty(writer_family, "writer_family")?;
    if generation == 0 {
        bail!("index segment generation must be nonzero");
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_segment_generation"),
        CoreMetaTuplePart::Utf8(index_id),
        CoreMetaTuplePart::Utf8(writer_family),
        CoreMetaTuplePart::U64(generation),
    ])
}

fn index_segment_ref_tuple_key(index_id: &str, segment_ref: &str) -> Result<Vec<u8>> {
    require_nonempty(index_id, "index_id")?;
    require_nonempty(segment_ref, "segment_ref")?;
    let ref_hash = format!("sha256:{}", sha256_hex(segment_ref.as_bytes()));
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_segment_ref"),
        CoreMetaTuplePart::Utf8(index_id),
        CoreMetaTuplePart::Hash(&ref_hash),
    ])
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{field} must be hex32");
    }
    Ok(())
}

fn validate_hash(value: &str, field: &'static str) -> Result<()> {
    let Some((algorithm, hex)) = value.split_once(':') else {
        bail!("{field} must be algorithm:hex");
    };
    if algorithm.is_empty()
        || hex.is_empty()
        || !algorithm
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || !hex.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("{field} must be algorithm:hex");
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(())
}
